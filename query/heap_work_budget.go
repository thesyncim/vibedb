package query

import (
	"fmt"
	"math"
	"sync/atomic"
	"unsafe"

	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibejson"
)

const (
	defaultHeapMemoryBytes = int64(64 << 20)
	minimumHeapMemoryBytes = int64(64 << 10)
)

// heapWorkSource distinguishes the two in-memory address spaces. A Segment
// uses integer ordinals; a heap snapshot may additionally retain stable
// chunk/slot locations and live masks while it gathers survivors.
type heapWorkSource uint8

const (
	heapWorkSegment heapWorkSource = iota
	heapWorkSnapshot
)

// heapWorkBudget is the whole-execution admission account for the in-memory
// executors. Its fixed reservation is made before row-proportional slices are
// grown. Variable-width decoded text and distinct GROUP BY keys are then
// admitted at their exact sizes before their arenas grow.
//
// used is atomic because the filter phase may decode escaped strings on
// several workers. Everything else, including plannerPlanned/rowRows and
// groupRows, is touched only by the execution's calling goroutine before or
// after those workers.
type heapWorkBudget struct {
	limit           int64
	used            atomic.Int64
	active          bool
	plannerPlanned  bool
	rowsPlanned     bool
	rowRows         int
	rowBytes        int64
	groupRows       int
	groupKeyScratch int64
}

func normalizeHeapMemoryBytes(options ExecOptions) (int64, error) {
	bytes := options.MemoryBytes
	if bytes == 0 {
		bytes = defaultHeapMemoryBytes
	}
	if bytes < minimumHeapMemoryBytes {
		return 0, fmt.Errorf(
			"query: MemoryBytes must be at least %d bytes",
			minimumHeapMemoryBytes,
		)
	}
	return bytes, nil
}

func (b *heapWorkBudget) begin(limit int64) {
	b.limit = limit
	b.used.Store(0)
	b.active = true
	b.plannerPlanned = false
	b.rowsPlanned = false
	b.rowRows = 0
	b.rowBytes = 0
	b.groupRows = 0
	b.groupKeyScratch = 0
}

func (b *heapWorkBudget) disable() {
	b.limit = 0
	b.used.Store(0)
	b.active = false
	b.plannerPlanned = false
	b.rowsPlanned = false
	b.rowRows = 0
	b.rowBytes = 0
	b.groupRows = 0
	b.groupKeyScratch = 0
}

// admit reserves bytes before the storage it describes is grown.
func (b *heapWorkBudget) admit(resource string, bytes int64) error {
	if !b.active || bytes == 0 {
		return nil
	}
	if bytes < 0 {
		bytes = math.MaxInt64
	}
	for {
		used := b.used.Load()
		required := saturatedBytes(used, bytes)
		if required > b.limit {
			return &WorkBudgetError{
				Resource: resource,
				Bytes:    required,
				Limit:    b.limit,
			}
		}
		if b.used.CompareAndSwap(used, required) {
			return nil
		}
	}
}

// checkpoint/rollback bracket an adaptive join build before any worker exists.
// A primary-key semi-join may abandon all build-side storage and use exact
// probes instead; restoring the calling goroutine's mark lets that released
// logical workspace fund the driving scan. Retained capacities remain warm,
// just as capacities from previous executions are not charged until reused.
func (b *heapWorkBudget) checkpoint() int64 {
	if !b.active {
		return 0
	}
	return b.used.Load()
}

func (b *heapWorkBudget) rollback(mark int64) {
	if b.active {
		b.used.Store(mark)
	}
}

func (b *heapWorkBudget) remaining() int64 {
	if !b.active {
		return math.MaxInt64
	}
	used := b.used.Load()
	if used >= b.limit {
		return 0
	}
	return b.limit - used
}

// admitHighWater reserves only growth beyond a reusable buffer's largest
// logical footprint in this execution. Batch storage is emptied and refilled,
// so charging every refill would turn a memory bound into a scan-byte quota;
// charging the largest refill instead matches the retained capacity that can
// coexist with the rest of the execution.
func (b *heapWorkBudget) admitHighWater(
	resource string,
	required int64,
	reserved *int64,
) error {
	if !b.active || required <= *reserved {
		return nil
	}
	additional := required
	if required != math.MaxInt64 && *reserved >= 0 {
		additional = required - *reserved
	}
	if err := b.admit(resource, additional); err != nil {
		return err
	}
	*reserved = required
	return nil
}

// admitPlanner reserves candidate-planning storage before an index source is
// asked to populate it. It is deliberately separate from admitRows: an exact
// index can narrow a million-row universe to one candidate while retaining
// only O(rows/64) masks, and charging a million materialized rows before that
// mask exists would turn a useful bound into a denial of selective queries.
//
// It is idempotent because the exact-count fast path may inspect the same
// candidate plan before deciding that the generic executor must recheck it.
func (b *heapWorkBudget) admitPlanner(
	p *plan,
	rows int,
	source heapWorkSource,
	postings bool,
	maskCount int,
) error {
	if b.plannerPlanned {
		return nil
	}
	required := heapPlannerFixedBytes(p, rows, source, postings, maskCount)
	if err := b.admit("heap candidate-planner workspace", required); err != nil {
		return err
	}
	b.plannerPlanned = true
	return nil
}

// admitRows reserves fixed-width materialization storage after candidate
// planning has established how many rows will actually be scanned. It grows
// incrementally so a snapshot fast path may reserve a width and then decline
// without making the generic fallback pay for the same retained capacity
// twice.
func (b *heapWorkBudget) admitRows(
	p *plan,
	rows int,
	source heapWorkSource,
	workers int,
) error {
	if !b.active {
		return nil
	}
	required := heapRowFixedBytes(p, rows, source, workers)
	if b.rowsPlanned && required <= b.rowBytes {
		return nil
	}
	additional := required
	if required != math.MaxInt64 && b.rowBytes <= required {
		additional = required - b.rowBytes
	}
	if err := b.admit("heap row workspace", additional); err != nil {
		return err
	}
	b.rowsPlanned = true
	b.rowRows = rows
	b.rowBytes = required
	return nil
}

// admitGroups reserves the worst-case fixed storage for rows distinct groups.
// The reservation happens before the first group or interner entry is grown.
// Key bytes themselves are data-dependent and are admitted separately, before
// KeyInterner copies a previously unseen key.
func (b *heapWorkBudget) admitGroups(p *plan, rows int) error {
	if !b.active || rows <= b.groupRows {
		return nil
	}
	required := heapGroupFixedBytes(p, rows)
	prior := heapGroupFixedBytes(p, b.groupRows)
	additional := required
	if required != math.MaxInt64 && prior <= required {
		additional = required - prior
	}
	if err := b.admit("heap GROUP BY workspace", additional); err != nil {
		return err
	}
	b.groupRows = rows
	return nil
}

func (b *heapWorkBudget) admitDecodedText(bytes int) error {
	return b.admit(
		"heap decoded-text workspace",
		saturatedProduct(int64(bytes), 2),
	)
}

func (b *heapWorkBudget) admitGroupKey(bytes int) error {
	return b.admit(
		"heap GROUP BY key workspace",
		saturatedProduct(int64(bytes), 2),
	)
}

func (b *heapWorkBudget) admitGroupKeyScratch(bytes int64) error {
	return b.admitHighWater(
		"heap GROUP BY key scratch",
		saturatedProduct(bytes, 2),
		&b.groupKeyScratch,
	)
}

func (b *heapWorkBudget) admitJoinMember() error {
	fixed := saturatedProduct(int64(unsafe.Sizeof(scalar{})), 2)
	return b.admit("heap join membership workspace", fixed)
}

func (b *heapWorkBudget) admitJoinText(
	required int64,
	reserved *int64,
) error {
	return b.admitHighWater(
		"heap join membership text workspace",
		required,
		reserved,
	)
}

func (b *heapWorkBudget) admitContainmentTape(
	entries int64,
	reserved *int64,
) error {
	required := saturatedProduct(
		entries,
		int64(unsafe.Sizeof(vibejson.IndexEntry{})),
	)
	return b.admitHighWater(
		"JSON containment index workspace",
		required,
		reserved,
	)
}

func (b *heapWorkBudget) admitJoinNeedles(entries int, payloadBytes int64) error {
	perEntry := int64(unsafe.Sizeof(vibejson.Index{})) +
		int64(unsafe.Sizeof(vibejson.IndexEntry{})) +
		int64(unsafe.Sizeof(int(0)))
	fixed := saturatedProduct(
		saturatedProduct(int64(entries), perEntry),
		2,
	)
	payload := saturatedProduct(payloadBytes, 2)
	return b.admit(
		"heap join index-needle workspace",
		saturatedBytes(fixed, payload),
	)
}

func (b *heapWorkBudget) admitJoinCandidates(
	p *plan,
	rows int,
	source heapWorkSource,
	postings bool,
	maskCount int,
) error {
	required := heapPlannerFixedBytes(p, rows, source, postings, maskCount)
	if p.where == nil && source == heapWorkSnapshot {
		required = saturatedProduct(
			saturatedProduct(
				int64(maskCount),
				int64(unsafe.Sizeof(store.Mask{})),
			),
			2,
		)
	}
	return b.admit("heap join candidate workspace", required)
}

func (b *heapWorkBudget) admitJoinBatch(
	p *plan,
	rows int,
	source heapWorkSource,
	keys bool,
) error {
	required := heapRowFixedBytes(p, rows, source, 1)
	if keys {
		required = saturatedBytes(
			required,
			saturatedProduct(
				saturatedProduct(
					int64(rows),
					int64(unsafe.Sizeof(string(""))),
				),
				2,
			),
		)
	}
	return b.admit("heap join batch workspace", required)
}

func (b *heapWorkBudget) resetJoinBloom(filter *joinBloom, keys int) error {
	if err := b.admit(
		"join Bloom-filter workspace",
		saturatedProduct(
			int64(joinBloomBlocks(keys)),
			int64(unsafe.Sizeof(joinBloomBlock{})),
		),
	); err != nil {
		return err
	}
	filter.reset(keys)
	return nil
}

// heapRowFixedBytes covers the high-water capacities that scale with the
// number of rows actually scanned. Every append/resize-backed row slice is
// charged at twice its logical width, which covers this package's geometric
// growth policy (capacity is always less than twice the requested length).
func heapRowFixedBytes(
	p *plan,
	rows int,
	source heapWorkSource,
	workers int,
) int64 {
	if rows < 0 {
		return math.MaxInt64
	}

	const capacityFactor = int64(2)
	intBytes := int64(unsafe.Sizeof(int(0)))
	rawBytes := int64(unsafe.Sizeof(vibejson.RawValue{}))
	scalarBytes := int64(unsafe.Sizeof(scalar{}))
	locationBytes := int64(unsafe.Sizeof(store.Location{}))
	sliceBytes := int64(unsafe.Sizeof([]byte{}))

	// One raw and one classified scalar per value path; numeric paths retain a
	// scalar column and share one reusable raw gather. Four ordinal slices
	// cover the global selection, worker selections/identity, and compact late
	// gather without depending on which path the planner chooses.
	perRow := saturatedProduct(int64(len(p.valuePaths)), rawBytes+scalarBytes)
	perRow = saturatedBytes(
		perRow,
		saturatedProduct(int64(len(p.numPaths)), scalarBytes),
	)
	if len(p.numPaths) != 0 {
		perRow = saturatedBytes(perRow, rawBytes)
	}
	perRow = saturatedBytes(perRow, 4*intBytes)
	if source == heapWorkSnapshot {
		// At most two address universes coexist: candidate/worker locations and
		// the late-gather address list.
		perRow = saturatedBytes(perRow, 2*locationBytes)
	}

	required := saturatedProduct(
		saturatedProduct(int64(rows), perRow),
		capacityFactor,
	)

	// The outer slice headers and worker records are small but retained, so
	// include them rather than hiding a row-independent allowance.
	outerSlices := int64(2*len(p.valuePaths)+len(p.numPaths)+4) * sliceBytes
	required = saturatedBytes(required, outerSlices)
	if p.where != nil && rows != 0 {
		n := scanWorkerCount(rows, workers)
		required = saturatedBytes(
			required,
			saturatedProduct(int64(n), int64(unsafe.Sizeof(scanWorker{}))),
		)
	}

	return required
}

// heapPlannerFixedBytes covers the independent append buffer each candidate
// leaf and merge may retain. Snapshot masks scale with the live universe's
// word count, not with the number of candidate rows they ultimately name.
func heapPlannerFixedBytes(
	p *plan,
	rows int,
	source heapWorkSource,
	postings bool,
	maskCount int,
) int64 {
	if rows < 0 || maskCount < 0 {
		return math.MaxInt64
	}
	if p.where == nil {
		return 0
	}
	buffers := heapPredicateBufferUpperBound(p.where)
	const capacityFactor = int64(2)
	headers := saturatedProduct(
		saturatedProduct(
			buffers,
			int64(unsafe.Sizeof([]byte{})),
		),
		capacityFactor,
	)
	switch source {
	case heapWorkSegment:
		if !postings {
			return 0
		}
		candidate := saturatedProduct(
			int64(rows),
			int64(unsafe.Sizeof(int(0))),
		)
		return saturatedBytes(
			headers,
			saturatedProduct(
				saturatedProduct(buffers, candidate),
				capacityFactor,
			),
		)
	case heapWorkSnapshot:
		candidate := saturatedProduct(
			int64(maskCount),
			int64(unsafe.Sizeof(store.Mask{})),
		)
		return saturatedBytes(
			headers,
			saturatedProduct(
				saturatedProduct(buffers, candidate),
				capacityFactor,
			),
		)
	default:
		return math.MaxInt64
	}
}

// heapGroupFixedBytes covers the group table, order, per-group projection and
// accumulator slices, and either interner directory. resize starts a fresh
// nested slice with at least eight elements, so that real retained capacity is
// charged rather than merely its usually-one-element logical length.
func heapGroupFixedBytes(p *plan, rows int) int64 {
	if rows <= 0 {
		return 0
	}
	scalarBytes := int64(unsafe.Sizeof(scalar{}))
	accBytes := int64(unsafe.Sizeof(aggAcc{}))
	groupBytes := int64(unsafe.Sizeof(group{}))
	intBytes := int64(unsafe.Sizeof(int(0)))
	sliceBytes := int64(unsafe.Sizeof([]byte{}))

	scalarCap := int64(max(8, len(p.groupCols)))
	accCap := int64(max(8, len(p.columns)))
	perGroup := saturatedProduct(scalarCap, scalarBytes)
	perGroup = saturatedBytes(perGroup, saturatedProduct(accCap, accBytes))

	// groups and groupOrder grow geometrically. The remaining directory charge
	// covers KeyInterner's hashes, key/chunk slice headers and open-addressing
	// table, as well as the categorical fast path's hash/table pair.
	perGroup = saturatedBytes(perGroup, 2*groupBytes)
	perGroup = saturatedBytes(perGroup, 2*intBytes)
	perGroup = saturatedBytes(perGroup, 4*sliceBytes+32)
	return saturatedProduct(int64(rows), perGroup)
}

func heapPredicateBufferUpperBound(p *compiledPredicate) int64 {
	if p == nil {
		return 0
	}
	// Two buffers per node cover its leaf result and a parent merge. Membership
	// may perform one probe and one union per exact needle.
	total := int64(2)
	total = saturatedBytes(
		total,
		saturatedProduct(int64(len(p.needles)), 2),
	)
	for _, child := range p.kids {
		total = saturatedBytes(total, heapPredicateBufferUpperBound(child))
	}
	return total
}

func saturatedProduct(a, b int64) int64 {
	if a < 0 || b < 0 || (a != 0 && b > math.MaxInt64/a) {
		return math.MaxInt64
	}
	return a * b
}
