package query

import (
	"errors"
	"fmt"
	"math"
	"math/bits"
	"unsafe"

	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
	vibejson "github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/x/byteview"
)

// The durable join's inner side.
//
// A join has two halves and they meet the durable backend very differently.
//
// The driving half needs nothing new. runFileSnapshotBatched already
// materializes each batch of documents into a per-worker store.Segment and runs
// the ordinary columnar filter over it, so a predInBound leaf on the driving
// side is evaluated by exactly the code the heap backend uses, against exactly
// the same binding. All that had to be added there is pointing each worker's
// evaluator at the shared bindings — which is the same read-only/per-worker
// split selectSegmentParallel already performs, and which the durable executor
// gets for free because each of its workers already owns a Workspace.
//
// The inner half is where the work is. The heap binder reads its inner
// collection through gathered columnar extraction at stable row addresses; a
// durable snapshot has no such gather, because its rows live in evictable page
// frames and every read copies out. What it does have is an ordered scan that
// yields (key, value) per selected row. So the inner side here is rebuilt on
// that scan, and it deliberately funnels back into the heap path as early as it
// can: a batch of scanned documents is appended into a store.Segment and then
// filtered by the same plan, the same extraction, and the same selectRows the
// heap binder uses.
//
// That funnel is the reason the two backends agree. The alternative — a second
// implementation of inner filtering that reads the durable snapshot directly —
// would have been faster by one document copy per inner row and would have had
// to be kept semantically identical to the heap's by hand, forever, across
// every predicate kind. Reusing the Segment path makes agreement structural:
// the differential tests compare two spellings of the scan, not two evaluators.
//
// Ordering is preserved for the same reason. RangeMasksRawBuffer visits rows in
// ascending chunk and slot order, which is the order the heap binder's mask
// walk produces, so a batch's rows arrive in the same sequence and the
// collected set — and, for a fan-out clause, the build's entry order — matches
// the heap's element for element.

// joinFileBloomScanRatio is joinBloomScanRatio for a durable inner side: how
// many inner rows the binder will scan, per outer row, to build a semi-join
// reduction filter.
//
// It is separate from joinBloomScanRatio because the durable filter-building
// scan is serial while ordinary durable scans are parallel, and cached durable
// probes have a different cost shape from heap hash probes.
// BenchmarkDurableJoinFilterCrossover measures the actual forced-on/forced-off
// crossover rather than inferring it from the ordinary scan. Parallelizing the
// bind scan would require re-measuring this constant and re-deriving filePool's
// channel-balance proof for the new job shape. Exact results and reproduction
// commands live in docs/performance.md.
const joinFileBloomScanRatio = 2

// joinFileEffectiveRatio resolves the caller's setting for a durable inner
// side. Zero means the durable default measured above; every other value,
// including a negative one that declines the filter outright, means exactly
// what it means on the heap, so an application that has measured its own
// workload keeps full control.
func joinFileEffectiveRatio(ratio int) int {
	if ratio == 0 {
		return joinFileBloomScanRatio
	}
	return ratio
}

// errJoinInnerStop unwinds a durable inner scan that has collected everything
// it is going to. The scan callbacks report "stop" by returning an error, so a
// deliberate early stop needs a sentinel that the caller strips rather than
// reports. It never escapes this file.
var errJoinInnerStop = errors.New("query: join inner scan stopped")

// A fileJoinSide is one join clause's durable inner-side storage: the snapshot
// it reads, the index session its candidate probe reuses, and the batch
// buffers the scan fills.
//
// It lives on the joinBinding, so it follows the same retained-capacity rule as
// everything else there: the buffers survive the execution and are refilled by
// the next one, which is what keeps a warmed durable join allocation-free.
type fileJoinSide struct {
	// snapshot is the inner collection at the joined instant, and is what a
	// probe-mode binding resolves each outer key against. It is also the
	// discriminator: a binding whose inner side is durable has it non-nil, and
	// that is what routes matches to the durable probe.
	snapshot *durable.Snapshot
	// index is the persistent-index planning session for the inner side's own
	// candidate probe. It is per clause rather than shared with the driving
	// side because the two bind different snapshots and retain independent
	// metrics and scratch high-water marks.
	index durable.IndexSession

	// docs is the batch Segment the inner filter runs over, and data/ends the
	// document bytes accumulated for it. Appending into one buffer and
	// recording end offsets is the shape scanFileBatches already uses, for the
	// same reason: the scan's key and value slices borrow a page lease that is
	// released as soon as the callback returns.
	docs store.Segment
	data []byte
	ends []int
	// keys holds one key per batch row, needed only when the clause joins on
	// the inner collection's primary key. keyText owns their bytes for the same
	// borrowed-lease reason the documents are copied.
	keys    []string
	keyText []byte
	// overflow is the scan's caller-owned overflow storage, and masks the
	// candidate masks its bound scan walks.
	overflow []byte
	masks    []store.Mask
	// batchRows counts the rows accumulated but not yet drained.
	batchRows int
}

// fileJoinBatchBudget tracks the largest durable inner-side refill by storage
// class. A scalar high-water mark would be insufficient: a batch of long
// strings can maximize source arenas while a later punctuation-dense batch
// maximizes index tapes, and both retained capacities coexist after Reset.
type fileJoinBatchBudget struct {
	documentBytes int64
	entryUnits    int64
	keyBytes      int64
	rows          int64

	rawReserved   int64
	entryReserved int64
	keyReserved   int64
	rowReserved   int64
	scanReserved  int64
}

func (b *fileJoinBatchBudget) resetBatch() {
	b.documentBytes = 0
	b.entryUnits = 0
	b.keyBytes = 0
	b.rows = 0
}

// admitRow bounds every arena grown before a durable join batch can be
// released. The collector owns two geometrically grown source copies:
// fileJoinSide.data plus its scratch Segment. RequiredIndexEntries is
// allocation-free and gives the exact structural width before either copy
// grows; charging four times that logical tape covers its chunk growth, spill
// tape, and recycled generation without rejecting a long scalar string as
// though every source byte were a token.
func (b *fileJoinBatchBudget) admitRow(
	work *heapWorkBudget,
	p *plan,
	key, value []byte,
	needKeys bool,
) error {
	if work == nil {
		return nil
	}
	entries, err := vibejson.RequiredIndexEntries(value)
	if err != nil {
		return err
	}
	documentBytes := saturatedBytes(b.documentBytes, int64(len(value)))
	entryUnits := saturatedBytes(b.entryUnits, int64(entries))
	rows := saturatedBytes(b.rows, 1)

	// Every source arena can retain its old and new geometric generations
	// across a growth. The Segment starts with an 8 KiB source chunk.
	rawRequired := saturatedProduct(documentBytes, 4)
	if rawRequired < 8<<10 {
		rawRequired = 8 << 10
	}
	if err := work.admitHighWater(
		"durable batch document workspace",
		rawRequired,
		&b.rawReserved,
	); err != nil {
		return err
	}

	entryRequired := saturatedProduct(
		saturatedProduct(
			entryUnits,
			int64(unsafe.Sizeof(vibejson.IndexEntry{})),
		),
		4,
	)
	// store.Segment begins with 512 entries.
	minimumEntries := int64(512) * int64(unsafe.Sizeof(vibejson.IndexEntry{}))
	if entryRequired < minimumEntries {
		entryRequired = minimumEntries
	}
	if err := work.admitHighWater(
		"durable batch index workspace",
		entryRequired,
		&b.entryReserved,
	); err != nil {
		return err
	}

	perRow := int64(unsafe.Sizeof(int(0))) +
		int64(unsafe.Sizeof(vibejson.Index{}))
	if needKeys {
		perRow += int64(unsafe.Sizeof(string("")))
	}
	rowRequired := saturatedProduct(
		saturatedProduct(rows, perRow),
		2,
	)
	if err := work.admitHighWater(
		"durable batch row workspace",
		rowRequired,
		&b.rowReserved,
	); err != nil {
		return err
	}
	if err := work.admitHighWater(
		"durable batch filter workspace",
		durableBatchRowFixedBytes(p, int(rows)),
		&b.scanReserved,
	); err != nil {
		return err
	}

	keyBytes := b.keyBytes
	if needKeys {
		keyBytes = saturatedBytes(keyBytes, int64(len(key)))
		keyRequired := saturatedProduct(keyBytes, 2)
		if err := work.admitHighWater(
			"durable batch key workspace",
			keyRequired,
			&b.keyReserved,
		); err != nil {
			return err
		}
	}

	b.documentBytes = documentBytes
	b.entryUnits = entryUnits
	b.keyBytes = keyBytes
	b.rows = rows
	return nil
}

// durableBatchRowFixedBytes is the retained columnar workspace of one
// per-worker scratch Segment scan. It is intentionally narrower than
// heapRowFixedBytes: durable batches materialize every path in one phase and
// need only the selection ordinal, while the heap executor can simultaneously
// retain candidates, identity, and late-gather ordinals plus a scanWorker.
// Charging those absent heap lanes here made the minimum 64 KiB configuration
// reject batches whose real storage fit.
func durableBatchRowFixedBytes(p *plan, rows int) int64 {
	if rows < 0 {
		return math.MaxInt64
	}
	const capacityFactor = int64(2)
	rawBytes := int64(unsafe.Sizeof(vibejson.RawValue{}))
	scalarBytes := int64(unsafe.Sizeof(scalar{}))
	intBytes := int64(unsafe.Sizeof(int(0)))
	sliceBytes := int64(unsafe.Sizeof([]byte{}))

	perRow := saturatedProduct(
		int64(len(p.valuePaths)),
		rawBytes+scalarBytes,
	)
	perRow = saturatedBytes(
		perRow,
		saturatedProduct(int64(len(p.numPaths)), scalarBytes),
	)
	if len(p.numPaths) != 0 {
		perRow = saturatedBytes(perRow, rawBytes)
	}
	perRow = saturatedBytes(perRow, intBytes)
	required := saturatedProduct(
		saturatedProduct(int64(rows), perRow),
		capacityFactor,
	)

	// resize grows these three outer arrays geometrically with a minimum of
	// eight elements. The selected and numRaws slice headers are inline fields
	// in Workspace; only their row backing stores are charged above.
	outer := int64(0)
	if len(p.valuePaths) != 0 {
		outer = saturatedBytes(
			outer,
			saturatedProduct(
				int64(2*max(8, len(p.valuePaths))),
				sliceBytes,
			),
		)
	}
	if len(p.numPaths) != 0 {
		outer = saturatedBytes(
			outer,
			saturatedProduct(
				int64(max(8, len(p.numPaths))),
				int64(unsafe.Sizeof(numColumn{})),
			),
		)
	}
	return saturatedBytes(required, outer)
}

// reset returns the side to an unbound state while keeping its storage.
//
// The keys are cleared rather than truncated, the same discipline
// joinBinding.reset applies to every other borrowed view: a retained key string
// views keyText, and leaving stale ones reachable through the retained capacity
// would pin an arena the next execution is about to overwrite in place.
func (f *fileJoinSide) reset() {
	f.snapshot = nil
	f.index.Reset(nil)
	f.data = f.data[:0]
	f.ends = f.ends[:0]
	clear(f.keys)
	f.keys = f.keys[:0]
	f.keyText = f.keyText[:0]
	f.batchRows = 0
}

// bindFileJoins is bindJoins for a durable driving side: it resolves every
// clause against a durable catalog and fills w's bindings, so the driving
// scan's predInBound leaves are either a membership it can search or a probe it
// can call.
//
// outer is the driving snapshot, consulted only for its declared index catalog
// — whether the outer join path carries a ready exact index is what decides if
// building needles for a collected set can pay for itself, exactly as on the
// heap side.
func (p *plan) bindFileJoins(
	w *Workspace,
	outer *durable.Snapshot,
	catalog durable.DatabaseSnapshot,
	limit, ratio int,
	workBudget *heapWorkBudget,
	stats *ExecStats,
) error {
	if err := w.checkCanceled(); err != nil {
		return err
	}
	if len(p.joins) == 0 {
		w.eval.bindTo(nil)
		w.scanUsed = 0
		return nil
	}
	if limit <= 0 {
		limit = joinMembershipMax
	}
	for len(w.joins) < len(p.joins) {
		w.joins = append(w.joins, joinBinding{})
	}
	defer func() { w.eval.bindTo(w.joins[:len(p.joins)]) }()
	w.storeIndexes = outer.AppendIndexes(w.storeIndexes[:0])
	for i := range p.joins {
		if err := cancellationCheckpoint(w.cancel, i); err != nil {
			return err
		}
		j := &p.joins[i]
		b := &w.joins[j.slot]
		if err := j.bindFile(
			b, catalog, limit, int(outer.Len()), ratio, workBudget, w.cancel, stats,
		); err != nil {
			return err
		}
		if b.mode == joinBindSet {
			if err := b.buildNeedles(
				p.valuePaths[j.outerPath].indexPath(),
				w.storeIndexes,
				workBudget,
			); err != nil {
				if !errors.Is(err, ErrWorkBudget) {
					return err
				}
				b.needles = b.needles[:0]
			}
		}
	}
	return nil
}

// bindFile runs one clause's durable inner side and installs the strategy it
// measured. It is the durable twin of planJoin.bind and shares its tail, so the
// two backends cannot drift on which strategy a given cardinality selects.
func (j *planJoin) bindFile(
	b *joinBinding,
	catalog durable.DatabaseSnapshot,
	limit, outerRows, ratio int,
	workBudget *heapWorkBudget,
	cancel *CancelFlag,
	stats *ExecStats,
) error {
	b.reset()
	b.scan.cancel = cancel
	if err := b.scan.checkCanceled(); err != nil {
		return err
	}
	inner, ok := catalog.Collection(j.collection)
	if !ok {
		return fmt.Errorf(
			"query: join: collection %q is not in the database snapshot", j.collection)
	}
	if j.fanOut {
		return fmt.Errorf(
			"query: join: collection %q projects joined columns, which the durable "+
				"backend does not yet materialize; joins over a durable database are "+
				"currently semi-joins only", j.collection)
	}
	if inner == nil {
		// A nil snapshot with ok=true is durable's representation of a
		// cataloged collection that has never needed a backing file. Bind the
		// measured empty membership directly: there are no pages to plan or
		// scan, EXISTS matches nothing, and the totalized anti leaf retains
		// every outer row (including NULL and missing keys). An absent name was
		// rejected above and therefore cannot be confused with this state.
		b.outerRows = outerRows
		b.ratio = joinFileEffectiveRatio(ratio)
		b.plan = j.inner
		j.installStrategy(b, false, stats)
		return b.scan.checkCanceled()
	}

	stoppable := j.innerPath == joinPrimaryKey
	workMark := int64(0)
	if workBudget != nil {
		workMark = workBudget.checkpoint()
	}
	overflowed, err := j.collectFile(
		b, inner, limit, outerRows, ratio, stoppable, workBudget,
	)
	if err != nil {
		if !stoppable || !errors.Is(err, ErrWorkBudget) {
			return err
		}
		b.discardJoinAttempt()
		workBudget.rollback(workMark)
		overflowed = true
		b.filtering = false
		b.bloom.disable()
	}
	b.file.snapshot = inner
	b.plan = j.inner
	j.installStrategy(b, overflowed, stats)
	return b.scan.checkCanceled()
}

// runFileJoinedBatched executes a joined plan over a durable driving snapshot.
//
// It is a separate entry point from the direct dispatchers rather than a branch
// inside them because the ordering is load-bearing in two places. The inner
// sides must be bound before the driving side's candidate masks are computed,
// since a membership binding is what lets a predInBound leaf lower to an index
// probe at all; and the probe tallies can only be folded once every worker has
// finished, since they live on per-worker scratch precisely so the filter
// phase's tightest loop writes nothing shared.
func (p *plan) runFileJoinedBatched(
	e *Exec,
	snapshot *durable.Snapshot,
	catalog durable.DatabaseSnapshot,
	n normalizedFileOptions,
	stats ExecStats,
) error {
	if err := e.Workspace.checkCanceled(); err != nil {
		return err
	}
	// The durable driving scan has its own bounded batch/merge frontier. The
	// joined side is bound before that scan exists, so arm the configured
	// admission account for its data-dependent candidate plan, membership,
	// Bloom filter, and containment tapes. Its schema- and row-bounded scratch
	// batch uses the same value as an independent target, matching the durable
	// MemoryBytes contract rather than pretending fixed batches are resident
	// capacity in this account.
	e.Workspace.heapWorkBudget.begin(n.memoryBytes)
	if err := p.bindFileJoins(
		&e.Workspace, snapshot, catalog,
		e.Options.JoinMembershipMax, e.Options.JoinFilterScanRatio,
		&e.Workspace.heapWorkBudget, &stats,
	); err != nil {
		return err
	}
	// Inner data-dependent storage and the driving pipeline coexist. Spend only
	// the unreserved remainder on driving batches and merge frontiers instead
	// of giving the membership/planner and driving halves independent copies of
	// MemoryBytes.
	remaining := e.Workspace.heapWorkBudget.remaining()
	if remaining < 1 {
		return &WorkBudgetError{
			Resource: "durable joined driving workspace",
			Bytes:    n.memoryBytes + 1,
			Limit:    n.memoryBytes,
		}
	}
	n.memoryBytes = remaining
	n.mergeBytes = max(int64(1), remaining/2)
	batchShare := remaining / int64(n.workers) / 4
	if batchShare < 1 {
		return &WorkBudgetError{
			Resource: "durable joined batch workspace",
			Bytes:    int64(n.workers) * 4,
			Limit:    remaining,
		}
	}
	if n.batchBytes > batchShare {
		n.batchBytes = batchShare
	}
	result, stats, err := p.runFileSnapshotBatched(e, snapshot, nil, n, stats)
	e.Result, e.Stats = result, stats
	if err != nil {
		return err
	}
	if probeErr := p.collectFileJoinStats(e); probeErr != nil {
		return probeErr
	}
	return e.Workspace.checkCanceled()
}

// collectFileJoinStats folds every durable worker's probe tallies into the
// execution's stats and reports the first I/O error any probe parked.
//
// The error check is the reason this cannot be skipped when stats are not
// wanted. A durable probe that faults a page has no way to fail the row it was
// deciding — predicate evaluation returns a bool — so it rejects the row and
// records the fault, and the only thing standing between that and a silently
// short result is this sweep.
func (p *plan) collectFileJoinStats(e *Exec) error {
	if len(p.joins) == 0 {
		return nil
	}
	var first error
	for i := range p.joins {
		slot := p.joins[i].slot
		for worker := range e.file.workers {
			probes := e.file.workers[worker].eval.probes
			if slot >= len(probes) {
				continue
			}
			probe := &probes[slot]
			e.Stats.JoinProbes += probe.probes
			e.Stats.JoinFilterRejected += probe.tested - probe.admitted
			if probe.err != nil && first == nil {
				first = probe.err
			}
		}
	}
	if first == nil {
		first = e.Workspace.checkCanceled()
	}
	return first
}

// collectFile is planJoin.collect over a durable inner snapshot: it runs the
// inner predicate and appends the surviving rows' join-key values to b,
// reporting whether the set passed limit.
//
// The batching is the heap version's, and for the same reason — the bound has
// to be a bound on work and memory, not only on what survives. What differs is
// where the rows come from: an ordered page scan rather than a gathered
// columnar read, and one that stops by returning an error rather than by
// breaking a loop.
func (j *planJoin) collectFile(
	b *joinBinding,
	inner *durable.Snapshot,
	limit, outerRows, ratio int,
	stoppable bool,
	workBudget *heapWorkBudget,
) (bool, error) {
	scan := &b.scan
	if err := scan.checkCanceled(); err != nil {
		return false, err
	}
	scan.heapWorkParent = workBudget
	scan.heapWorkTextReserved = 0
	scan.eval.setWork(workBudget)
	scan.candidateUsed = 0
	scan.storeMaskUsed = 0
	scan.text = scan.text[:0]
	f := &b.file

	// The inner side plans its candidates through the same durable entry point
	// the driving side uses, rather than calling the generic planner directly.
	// The inner scan resolves candidates from the declared index catalog,
	// bounded by the join's remaining work budget.
	remaining := int64(^uint64(0) >> 1)
	if workBudget != nil {
		remaining = workBudget.remaining()
	}
	masks, plannedBytes, err := j.inner.fileCandidateMasksBounded(
		inner, &f.index, scan, remaining,
	)
	if err != nil {
		return false, err
	}
	if err := scan.checkCanceled(); err != nil {
		return false, err
	}
	if plannedBytes != 0 && workBudget != nil {
		if err := workBudget.admit(
			"durable candidate-planner workspace",
			plannedBytes,
		); err != nil {
			return false, err
		}
	}

	// The candidate count is exact in both branches, which is what the filter
	// decision needs. A bound scan popcounts the masks it will walk; an
	// unbounded one reads the snapshot's own live-row count. Neither is an
	// estimate, so joinBloomWorthwhile keeps comparing measured quantities
	// here exactly as it does on the heap.
	candidates := 0
	if masks != nil {
		for i, mask := range masks {
			if err := cancellationCheckpoint(scan.cancel, i); err != nil {
				return false, err
			}
			candidates += bits.OnesCount64(mask.Bits)
		}
	} else {
		candidates = int(inner.Len())
	}
	b.candidates = candidates
	b.outerRows = outerRows
	b.ratio = joinFileEffectiveRatio(ratio)
	b.filtering = stoppable && joinBloomWorthwhile(candidates, outerRows, limit, b.ratio)

	f.data = f.data[:0]
	f.ends = f.ends[:0]
	clear(f.keys)
	f.keys = f.keys[:0]
	f.keyText = f.keyText[:0]
	f.batchRows = 0
	var batchBudget fileJoinBatchBudget
	var batchWork heapWorkBudget
	var batchAccount *heapWorkBudget
	if workBudget != nil {
		batchWork.begin(workBudget.limit)
		batchAccount = &batchWork
	}

	needKeys := j.innerPath == joinPrimaryKey
	over := false
	appendRow := func(key, value []byte) error {
		if err := scan.checkCanceled(); err != nil {
			return err
		}
		if err := batchBudget.admitRow(
			batchAccount, j.inner, key, value, needKeys,
		); err != nil {
			return err
		}
		f.data = append(f.data, value...)
		f.ends = append(f.ends, len(f.data))
		if needKeys {
			// The key borrows a page lease that is released when this callback
			// returns, so it is copied into the batch arena. The arena is
			// refilled per batch, and every key kept past the batch has already
			// been copied again into the binding's own storage by appendString.
			start := len(f.keyText)
			f.keyText = append(f.keyText, key...)
			f.keys = append(f.keys, byteview.String(f.keyText[start:len(f.keyText):len(f.keyText)]))
		}
		f.batchRows++
		if f.batchRows < joinBatchRows {
			return nil
		}
		stop, drainErr := j.drainFile(b, limit, stoppable, workBudget)
		if drainErr != nil {
			return drainErr
		}
		batchBudget.resetBatch()
		if stop {
			over = true
			return errJoinInnerStop
		}
		return nil
	}

	if masks != nil {
		f.overflow, err = inner.RangeMasksRawBuffer(masks, f.overflow[:0], appendRow)
	} else {
		f.overflow, err = inner.RangeRawBuffer(f.overflow[:0], appendRow)
	}
	if err != nil {
		if !errors.Is(err, errJoinInnerStop) {
			return false, err
		}
		return true, nil
	}
	stop, err := j.drainFile(b, limit, stoppable, workBudget)
	if err != nil {
		return false, err
	}
	// A filtering scan never stops early, so its overflow is reported by the
	// flag the seeding step set rather than by the drain's return.
	return over || stop || b.overflowed, nil
}

// drainFile filters one accumulated batch of durable inner rows and collects
// the join-key value of each survivor, then empties the batch. It reports
// whether the collected set passed limit.
//
// The batch is turned into a Segment and run through the inner plan's own
// extraction and selection, which is what makes this identical to the heap
// binder's drain rather than merely analogous to it. The Segment is dense — one
// row per scanned document, in scan order — so the selection it returns indexes
// the batch directly and no location mapping is needed.
func (j *planJoin) drainFile(
	b *joinBinding,
	limit int,
	stoppable bool,
	workBudget *heapWorkBudget,
) (bool, error) {
	f := &b.file
	if f.batchRows == 0 {
		return false, b.scan.checkCanceled()
	}
	scan := &b.scan
	if err := scan.checkCanceled(); err != nil {
		return false, err
	}
	f.docs.Reset()
	start := 0
	for row, end := range f.ends {
		if err := cancellationCheckpoint(scan.cancel, row); err != nil {
			return false, err
		}
		if _, err := f.docs.Append(f.data[start:end]); err != nil {
			return false, err
		}
		start = end
	}
	scan.text = scan.text[:0]
	ctx := &scan.ctx
	ctx.s, ctx.rows = &f.docs, f.docs.Len()
	if err := ctx.extract(j.inner, nil, scan); err != nil {
		return false, err
	}
	selected, err := j.inner.selectRows(ctx, nil, false, scan)
	if err != nil {
		return false, err
	}
	if err := scan.eval.firstError(); err != nil {
		return false, err
	}

	stop := false
	for at, row := range selected {
		if err := cancellationCheckpoint(scan.cancel, at); err != nil {
			return false, err
		}
		if b.overflowed {
			// Past the threshold the set is not grown further, but the scan is
			// still running because a filter is being built out of it, and a
			// filter that saw only some of the keys would produce false
			// negatives — the one error a Bloom filter must never have.
			b.bloom.insert(hashJoinKey(f.keys[row]))
			continue
		}
		if j.innerPath == joinPrimaryKey {
			if err := b.appendString(f.keys[row], workBudget); err != nil {
				return false, err
			}
		} else {
			value := ctx.values[j.innerPath][row]
			if value.kind == kindNull {
				continue
			}
			if err := b.appendValue(value, workBudget); err != nil {
				return false, err
			}
		}
		if !stoppable || len(b.lits) <= limit {
			continue
		}
		if !b.filtering {
			stop = true
			break
		}
		if workBudget != nil {
			if err := workBudget.resetJoinBloom(&b.bloom, b.candidates); err != nil {
				return false, err
			}
		} else {
			b.bloom.reset(b.candidates)
		}
		b.overflowed = true
		for i := range b.lits {
			if err := cancellationCheckpoint(scan.cancel, i); err != nil {
				return false, err
			}
			b.bloom.insert(hashJoinKey(b.lits[i].sval))
		}
	}
	b.scanned += f.batchRows

	f.data = f.data[:0]
	f.ends = f.ends[:0]
	clear(f.keys)
	f.keys = f.keys[:0]
	f.keyText = f.keyText[:0]
	f.batchRows = 0
	// The next batch refills the decoded-string arena in place, so nothing may
	// still be viewing it. appendString and appendValue have already copied
	// everything kept into b's own storage, and the filter keeps hashes.
	scan.text = scan.text[:0]

	if stop {
		return true, nil
	}
	if b.overflowed && !b.keepFiltering() {
		b.filtering = false
		b.bloom.disable()
		return true, nil
	}
	return false, nil
}

// probeFile answers one outer row against a durable inner collection: it copies
// the addressed document out of the page cache and evaluates the inner filter
// against it.
//
// It is the durable twin of joinBinding.probe and differs from it in the two
// ways the backend forces. The document is copied rather than borrowed, because
// a durable snapshot reads through evictable page frames and AppendRaw is the
// only form that never hands out a slice into one; the copy lands in the
// per-worker probe buffer, which is retained across rows and executions so the
// copy costs no allocation after the first document of a given width.
//
// And the lookup can fail with an I/O error, which the heap's cannot. A
// predicate evaluates to a bool with no error channel, so a fault is recorded
// on the probe scratch and the row is rejected; the executor checks every
// worker's probe for an error once the filter phase has finished and fails the
// whole execution if any is set. Rejecting the row in the meantime is safe
// precisely because the result is thrown away.
func (b *joinBinding) probeFile(cell scalar, pr *joinProbe) bool {
	if err := cancellationError(b.scan.cancel); err != nil {
		if pr.err == nil {
			pr.err = err
		}
		return false
	}
	if cell.kind != kindString {
		return false // collection keys are strings; nothing else can name one
	}
	if b.bloom.active && !b.bloom.admits(hashJoinKey(cell.sval), pr) {
		return false
	}
	pr.probes++
	raw, ok, err := b.file.snapshot.AppendRaw(pr.raw[:0], []byte(cell.sval))
	pr.raw = raw
	if err != nil {
		if pr.err == nil {
			pr.err = err
		}
		return false
	}
	if cancelErr := cancellationError(b.scan.cancel); cancelErr != nil {
		if pr.err == nil {
			pr.err = cancelErr
		}
		return false
	}
	if !ok {
		return false
	}
	if b.plan.where == nil {
		return true
	}
	cols := pr.sized(len(b.plan.valuePaths))
	pr.inner.text = pr.inner.text[:0]
	document := vibejson.RawValue{Src: raw}
	for i := range b.plan.valuePaths {
		if cancelErr := cancellationCheckpoint(b.scan.cancel, i); cancelErr != nil {
			if pr.err == nil {
				pr.err = cancelErr
			}
			return false
		}
		value, found, pointerErr := document.PointerCompiled(b.plan.valuePaths[i].pointer)
		if pointerErr != nil || !found {
			value = vibejson.RawValue{}
		}
		cols[i][0] = classifyRawInto(value, &pr.inner.text)
	}
	return b.plan.where.eval(cols, 0, &pr.inner)
}
