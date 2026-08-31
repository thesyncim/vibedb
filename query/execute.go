package query

import (
	"bytes"
	"fmt"
	"math/bits"
	"slices"

	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/document"
)

// Workspace owns all transient query execution storage. It reaches execution
// as a field of an [Exec], which is where a caller normally retains it; its
// zero value is ready to use. Reusing one Workspace turns a warmed execution
// whose row, posting-frontier, decoded-text, and group high-water marks fit the
// retained capacity into a zero-allocation operation. That contract includes
// posting merges, escaped-string classification, containment indexing, stable
// ordering, aggregation, grouping, and result materialization. Every backend
// uses it, the durable one for its persistent-index planning as well.
//
// A Workspace is single-consumer and not safe for concurrent use. A compiled
// Query remains safe for concurrent use when each goroutine supplies a distinct
// Exec. Storage borrowed by a Result written by [Query.RunInto] is valid only
// until the next execution reusing the same Workspace or Result.
//
// A query with a [Join] clause also runs its inner side out of this Workspace,
// through one nested set of buffers per clause. That is where a join's
// collected key set, its index needles, and its per-row probe scratch live —
// never in the compiled plan, which is shared by every concurrent execution.
//
// Retained capacity is a high-water mark: it grows to the largest execution the
// Workspace has served and is never reduced by a smaller one, which is what
// makes the steady state allocation-free. One unusually large execution
// therefore pins its working set for the Workspace's lifetime. Call [Workspace.Release]
// to give that memory back, the same trade-off [Result.Release] offers.
type Workspace struct {
	ctx execCtx

	// cancel is the caller-owned signal for the execution currently borrowing
	// this Workspace. Nested join scans and parked workers receive the same
	// pointer while active; no execution path owns or resets it.
	cancel *CancelFlag

	raws             [][]vibejson.RawValue
	numRaws          []vibejson.RawValue
	selected         []int
	candidates       [][]int
	candidateUsed    int
	emptyCandidate   [1]int
	storeMasks       [][]store.Mask
	storeMaskUsed    int
	storeIndexProbes int
	storeRows        []store.Location
	storeIndexes     []store.IndexInfo
	emptyStoreMask   [1]store.Mask
	// needleScratch backs every AppendIndexMasks/AppendIndexCandidateMasks
	// call the generic candidate planner (candidates_mask.go) makes: passing
	// an already-existing, reused slice instead of building one from scalars
	// keeps those variadic calls allocation-free across a generic dictionary
	// dispatch, where Go's escape analysis otherwise can't prove a
	// freshly-built variadic backing array stays off the heap.
	needleScratch [store.MaxIndexColumns]vibejson.Index

	// eval is the calling goroutine's own evaluator scratch: the containment
	// indexer's entry storage, plus the probe state a join leaf writes. The
	// filter phase's workers each carry their own; this is the one the serial
	// path and every non-filter recheck use.
	eval evalScratch

	// text and lateText are the decoded-string arenas of the two extraction
	// phases: text holds the columns WHERE reads, lateText the columns only the
	// output reads. They are two arenas rather than one because each phase
	// pre-grows its arena from a prescan of its own raw values and then appends
	// without reallocating, which is what keeps a string view handed to an
	// earlier column pointing at storage the later columns cannot move. One
	// shared arena would have to be grown a second time, between the phases, at
	// a size the first phase could not have predicted — and a grow that lands
	// on a fresh backing array leaves the second phase writing somewhere the
	// first phase's views do not live, so the two would ping-pong reallocations
	// forever instead of settling on a high-water mark.
	text     []byte
	lateText []byte

	// lateRows and lateStoreRows address the surviving rows for the late
	// materialization gather, and liveMasks is the stable-slot universe a
	// snapshot scan resolves scan ordinals through. They are separate from the
	// candidate and storeRows buffers because a gather reads those while
	// writing these.
	lateRows      []int
	lateStoreRows []store.Location
	liveMasks     []store.Mask

	// The fan-out expansion's pair space: per output row, the driving scan
	// ordinal its filter columns are at, the driving address its output columns
	// are read from, and the joined address the joined collection's columns are
	// read from. pairValues is the spread destination for the filter columns,
	// swapped with ctx.values so both buffers keep their high-water capacity.
	//
	// These are the buffers a fan-out join scales its output through, and they
	// are retained for exactly that reason: a fan-out produces more rows than
	// the driving collection has, so anything allocated per pair would scale
	// with the result rather than with the collection.
	pairOuterScan []int
	pairOuterRows []store.Location
	pairInner     []store.Location
	pairInnerPair []int
	pairValues    [][]scalar
	pairNums      [][]scalar

	// pool holds the parked filter-phase workers, and identity is the row
	// permutation a segment worker addresses its share of an uncompacted scan
	// through. The pool is a pointer to a separate object because parked
	// goroutines have to outlive any one execution while holding nothing that
	// points back at this Workspace; see scanPool.
	pool     *scanPool
	identity []int
	// scanUsed is how many of the pool's workers the last filter phase woke.
	// The pool is a high-water mark, so a tally summed across it has to stop
	// here or it reports a previous, wider execution's work as this one's.
	scanUsed int

	// joins is one execution-time binding per compiled join clause, indexed by
	// the slot its predInBound leaf carries. It lives here rather than in the
	// plan because a compiled Query is shared by every concurrent execution
	// while a join's collected set, its probe scratch, and its inner snapshot
	// belong to exactly one of them.
	joins []joinBinding
	// marks is the immutable-after-bind grouped state for correlated predicate
	// subqueries. Each evaluator aliases it read-only during the outer scan.
	marks []markBinding
	// correlations is the execution-local scalar tuple supplied by a containing
	// APPLY. Compiled plans carry only slot ordinals; values are copied here at
	// the synchronous child boundary and cleared before the Workspace is reused.
	correlations           []scalar
	correlationNeedles     []vibejson.Index
	correlationNeedleText  []byte
	correlationStarts      []int
	correlationEntries     []vibejson.IndexEntry
	correlationNeedleBytes int64

	// joinPairBudget bounds the multiplicative workspace of the one supported
	// fan-out join. It is configured before join binding and activated only
	// when pair materialization begins, so ordinary filter-column extraction
	// is not charged as pair storage.
	joinPairBudget joinPairBudget

	accs       []aggAcc
	interner   store.KeyInterner
	groups     []group
	groupKey   []byte
	groupOrder []int
	stringHash []uint32
	stringSlot []uint32

	// heapWorkBudget bounds the fixed row-proportional columns, selections,
	// ordering/grouping state, decoded text, and distinct group keys retained
	// by Segment and heap-snapshot execution. A durable database join also uses
	// it for the joined side's membership, Bloom filter, and reusable batch;
	// the driving durable scan keeps its own batch/spill accounting.
	heapWorkBudget heapWorkBudget
	// heapWorkParent is non-nil only on a join binding's nested scan Workspace.
	// Its decoded-text growth belongs to the parent execution's one account,
	// not to an independent per-clause limit.
	heapWorkParent       *heapWorkBudget
	heapWorkTextReserved int64

	aggregateBudget aggregateBudget
	aggregateOut    []byte
	aggregateLease  aggregateLease
}

func (w *Workspace) activeHeapWorkBudget() *heapWorkBudget {
	if w.heapWorkParent != nil {
		return w.heapWorkParent
	}
	return &w.heapWorkBudget
}

func (w *Workspace) checkCanceled() error {
	return cancellationError(w.cancel)
}

func (w *Workspace) admitDecodedText(bytes int) error {
	if w.heapWorkParent == nil {
		return w.heapWorkBudget.admitDecodedText(bytes)
	}
	return w.heapWorkParent.admitHighWater(
		"heap join decoded-text workspace",
		saturatedProduct(int64(bytes), 2),
		&w.heapWorkTextReserved,
	)
}

// clearBorrowedViews severs every document-backed view retained by a nested
// join scan while preserving its capacities. It is intentionally narrower
// than Release: decoded-text arenas, index tapes, masks, and ordinal buffers
// own their storage and remain warm; RawValue and scalar columns do not.
func (w *Workspace) clearBorrowedViews() {
	resetAggs(w.accs)
	for i := range w.raws {
		clear(w.raws[i])
		w.raws[i] = w.raws[i][:0]
	}
	clear(w.numRaws)
	w.numRaws = w.numRaws[:0]
	for i := range w.ctx.values {
		clear(w.ctx.values[i])
		w.ctx.values[i] = w.ctx.values[i][:0]
	}
	for i := range w.ctx.nums {
		clear(w.ctx.nums[i].vals)
		w.ctx.nums[i].vals = w.ctx.nums[i].vals[:0]
	}
	for i := range w.pairValues {
		clear(w.pairValues[i])
		w.pairValues[i] = w.pairValues[i][:0]
	}
	for i := range w.pairNums {
		clear(w.pairNums[i])
		w.pairNums[i] = w.pairNums[i][:0]
	}
	for i := range w.groups {
		clear(w.groups[i].scalars)
		w.groups[i].scalars = w.groups[i].scalars[:0]
		resetAggs(w.groups[i].accs)
	}
	w.groups = w.groups[:0]
	clear(w.storeIndexes)
	w.storeIndexes = w.storeIndexes[:0]
	w.ctx.s = nil
	w.ctx.rows = 0
	w.eval.bindTo(nil)
	w.eval.bindMarks(nil)
	w.resetCorrelationBindings()
	w.eval.setWork(nil)
	if w.pool != nil {
		for i := range w.pool.workers {
			w.pool.workers[i].eval.bindTo(nil)
			w.pool.workers[i].eval.bindMarks(nil)
			w.pool.workers[i].eval.bindCorrelations(nil)
			w.pool.workers[i].eval.setWork(nil)
			w.pool.workers[i].release()
		}
	}
}

// Release drops all storage retained by w, returning it to its zero value.
// Reusing a warm Workspace normally gives better throughput, since
// the retained capacity is what makes a steady-state execution allocation-free;
// Release is for after an unusually large execution whose high-water capacity
// should not be pinned for the rest of the Workspace's lifetime.
//
// A Workspace also holds raw bytes and strings borrowed from the documents of
// the execution that filled it, so releasing one lets the source those views
// point into be collected.
func (w *Workspace) Release() {
	if w == nil {
		return
	}
	// The parked scan workers are retired before the reset, because the reset
	// is what makes them unreachable.
	w.releaseScanPool()
	*w = Workspace{}
}

func (w *Workspace) nextStoreMasks() []store.Mask {
	if w.storeMaskUsed == len(w.storeMasks) {
		w.storeMasks = append(w.storeMasks, nil)
	}
	i := w.storeMaskUsed
	w.storeMaskUsed++
	return w.storeMasks[i][:0]
}

func (w *Workspace) keepStoreMasks(masks []store.Mask) {
	w.storeMasks[w.storeMaskUsed-1] = masks
}

// execCtx is the columnar state for one execution. Its inner column slices
// persist inside Workspace and are overwritten on the next call.
type execCtx struct {
	s      *store.Segment
	cache  store.ShapeCache
	rows   int
	values [][]scalar
	nums   []numColumn
}

type numColumn struct {
	vals []scalar
}

// An evalScratch is everything a predicate mutates while evaluating a row, as
// opposed to the plan it reads, which is immutable and shared.
//
// It exists as one parameter rather than as Workspace fields because the filter
// phase runs on several goroutines over disjoint row ranges. Each worker has
// its own, so two workers evaluating the same predicate touch no common
// mutable state. That was already true of the containment indexer's entry
// scratch; a join leaf makes it load-bearing, because a lookup-bound one
// resolves a document and counts a probe on every row it sees.
//
// binds is the exception, and deliberately so: it is aliased from the
// Workspace rather than copied, because a binding is read-only once bindJoins
// has finished and copying the collected set or the Bloom filter per worker
// would undo the whole point of summarizing them once. What is per worker is
// probes, which is where every write during evaluation lands.
type evalScratch struct {
	entries []vibejson.IndexEntry
	// work is the one whole-execution account shared by every evaluator.
	// entriesReserved is this evaluator's current-execution high-water, so
	// several parallel tapes are summed rather than each receiving the limit.
	work            *heapWorkBudget
	entriesReserved int64
	err             error
	// text is the decoded-string arena a nested evaluation classifies through.
	// Only a join probe uses it; a top-level scan classifies into the phase
	// arenas the extraction owns.
	text              []byte
	binds             []joinBinding
	probes            []joinProbe
	marks             []markBinding
	correlations      []scalar
	markLeftEntries   []vibejson.IndexEntry
	markRightEntries  []vibejson.IndexEntry
	markLeftReserved  int64
	markRightReserved int64
}

// containsTape indexes one containment haystack into the evaluator's reused
// entry storage and returns the resulting Index, which borrows src.
//
// It builds first and grows on [document.ErrIndexFull] rather than sizing the
// tape with [vibejson.RequiredIndexEntries] first. That count is a second
// complete validation pass over the same bytes, and it runs once per row here.
// The containment benchmark shows that exact pre-sizing costs more than the
// build it sizes. Build-and-retry is the shape store.Collection.validateDocument
// and store.Segment.buildDoc already use, for exactly this reason; measured
// results live in docs/performance.md.
//
// The scratch outlives the scan, so a retry is a warmup cost and not a
// per-row one: the cold rows of a scan double the buffer a few times, and
// every row after that builds once into a buffer already big enough. The
// buffer only ever grows, so no size outlier can make it thrash, and growth
// doubles and stops at the width no document can exceed — the tape records
// one entry per value and per object key, and each of those spans at least
// one distinct source byte, so len(src)+2 entries always suffice — which is
// what bounds the loop.
func (s *evalScratch) containsTape(src []byte) (vibejson.Index, error) {
	return s.containsTapeFrom(src, len(src)/8+8)
}

// containsTapeFrom is containsTape with the cold-start width spelled out, the
// seam its growth test drives with a width too small for any real haystack.
//
// seed is deliberately an underestimate of a dense document rather than a
// bound on one: it is the same len/8+8 store.Collection.validateDocument
// opens with, and it is what keeps the retained buffer near the widest
// haystack actually seen instead of near the widest one imaginable. A tape
// sized to the theoretical maximum would be five times the true width on
// ordinary documents and would stay that way for the evaluator's life, which
// is the cost this shape is meant to avoid, not to move somewhere else.
func (s *evalScratch) containsTapeFrom(src []byte, seed int) (vibejson.Index, error) {
	need := max(seed, 1)
	limit := len(src)
	if limit > int(^uint(0)>>1)-2 {
		limit = int(^uint(0) >> 1)
	} else {
		limit += 2
	}
	for {
		if cap(s.entries) < need {
			peak := saturatedBytes(int64(cap(s.entries)), int64(need))
			if s.work != nil {
				if err := s.work.admitContainmentTape(
					peak,
					&s.entriesReserved,
				); err != nil {
					s.parkError(err)
					return vibejson.Index{}, err
				}
			}
			s.entries = make([]vibejson.IndexEntry, need)
		}
		index, err := vibejson.BuildIndex(src, s.entries[:cap(s.entries)])
		if err != document.ErrIndexFull || cap(s.entries) >= limit {
			// At or above limit the buffer cannot be the reason a build
			// failed, so the error is reported rather than retried; that is
			// what keeps a broken width invariant from spinning here forever.
			if err == nil && s.work != nil {
				if budgetErr := s.work.admitContainmentTape(
					int64(len(index.Entries)),
					&s.entriesReserved,
				); budgetErr != nil {
					s.parkError(budgetErr)
					return vibejson.Index{}, budgetErr
				}
			}
			if err != nil {
				s.parkError(err)
			}
			return index, err
		}
		current := cap(s.entries)
		if current >= limit-current {
			need = limit
		} else {
			need = current * 2
		}
	}
}

func (s *evalScratch) setWork(work *heapWorkBudget) {
	s.work = work
	s.entriesReserved = 0
	s.err = nil
	s.markLeftReserved = 0
	s.markRightReserved = 0
	for i := range s.probes {
		s.probes[i].inner.setWork(work)
	}
}

func (s *evalScratch) parkError(err error) {
	if err != nil && s.err == nil {
		s.err = err
	}
}

func (s *evalScratch) firstError() error {
	if s.err != nil {
		return s.err
	}
	for i := range s.probes {
		if s.probes[i].err != nil {
			return s.probes[i].err
		}
		if s.probes[i].inner.err != nil {
			return s.probes[i].inner.err
		}
	}
	return nil
}

// errorOr preserves an error the evaluator already observed over a later
// cooperative cancellation. In particular, a durable join probe can park an
// I/O error in a bool-valued predicate; cancellation arriving after that read
// must not relabel storage failure as an ordinary client abort.
func (s *evalScratch) errorOr(fallback error) error {
	if err := s.firstError(); err != nil {
		return err
	}
	return fallback
}

// bindTo points s at the executing Workspace's join bindings and gives it one
// probe scratch per clause. It is called once per evaluator per execution —
// once on the calling goroutine, once per filter-phase worker — before any row
// is evaluated.
func (s *evalScratch) bindTo(binds []joinBinding) {
	for i := range s.probes {
		s.probes[i].reset()
	}
	s.binds = binds
	if len(binds) == 0 {
		s.probes = s.probes[:0]
		return
	}
	s.probes = resize(s.probes, len(binds))
	for i := range s.probes {
		s.probes[i].inner.setWork(s.work)
	}
}

// collectJoinStats folds every evaluator's probe tallies into the execution's
// stats. The counters live on the per-worker scratch rather than being written
// through a shared pointer during the scan, because a lookup binding counts a
// probe in the tightest loop this package has — and because a shared counter
// incremented from several workers would be a data race rather than a slow
// path.
func (w *Workspace) collectJoinStats(p *plan, stats *ExecStats) {
	if len(p.joins) == 0 {
		return
	}
	if p.fanOutJoin >= 0 {
		stats.JoinPairs = uint64(len(w.pairOuterScan))
	}
	for i := range p.joins {
		slot := p.joins[i].slot
		probe := &w.eval.probes[slot]
		stats.JoinProbes += probe.probes
		stats.JoinFilterRejected += probe.tested - probe.admitted
		w.eachScanWorker(func(sw *scanWorker) {
			if slot < len(sw.eval.probes) {
				stats.JoinProbes += sw.eval.probes[slot].probes
				stats.JoinFilterRejected += sw.eval.probes[slot].tested - sw.eval.probes[slot].admitted
			}
		})
	}
}

// nextCandidates returns an independent empty posting buffer. Candidate-tree
// evaluation never aliases its inputs with a merge output, so AND/OR can be
// assembled in linear passes without allocations after the buffers warm.
func (w *Workspace) nextCandidates() []int {
	if w.candidateUsed == len(w.candidates) {
		w.candidates = append(w.candidates, nil)
	}
	i := w.candidateUsed
	w.candidateUsed++
	return w.candidates[i][:0]
}

func (w *Workspace) keepCandidates(rows []int) {
	w.candidates[w.candidateUsed-1] = rows
}

// runInto executes p, overwriting dst while retaining its column and cell
// capacity. Callers must not reuse dst or w concurrently.
func (p *plan) runInto(dst *Result, s *store.Segment, w *Workspace, workers int) error {
	if err := w.checkCanceled(); err != nil {
		return err
	}
	if p.hasLimit && p.limit == 0 && !p.requiresSQLDomainScan() {
		return prepareResult(dst, p, 0)
	}
	if err := w.activeHeapWorkBudget().admitPlanner(
		p, s.Len(), heapWorkSegment, s.Postings, 0,
	); err != nil {
		return err
	}
	w.candidateUsed = 0
	w.text = w.text[:0]
	w.lateText = w.lateText[:0]
	w.groupKey = w.groupKey[:0]
	w.groupOrder = w.groupOrder[:0]
	w.interner.Reset()

	candidates := p.candidateRows(s, w)
	if err := w.checkCanceled(); err != nil {
		return err
	}
	if candidates == nil {
		if handled, err := p.runDirectSegmentScalarCount(dst, s, w); err != nil {
			return err
		} else if handled {
			return nil
		}
	}
	compact := preferSparseRows(len(candidates), s.Len(), candidates != nil)
	var sourceRows []int
	if compact {
		sourceRows = candidates
	}
	scanRows := s.Len()
	if compact {
		scanRows = len(candidates)
	}
	if err := w.activeHeapWorkBudget().admitRows(
		p, scanRows, heapWorkSegment, workers,
	); err != nil {
		return err
	}

	ctx := &w.ctx
	ctx.s, ctx.rows = s, s.Len()
	if compact {
		ctx.rows = len(sourceRows)
	}
	if p.where == nil {
		// An unfiltered plan has no filter phase: every scanned row survives,
		// so splitting the extraction in two would only push the selection
		// out of cache between the pass that writes it and the pass that reads
		// it back. Materialize first, exactly as a scan always did.
		if err := ctx.extract(p, sourceRows, w); err != nil {
			return err
		}
		selected, err := p.selectRows(ctx, candidates, compact, w)
		if err != nil {
			return err
		}
		return p.emit(dst, ctx, selected, w)
	}
	selected, err := p.filterSegmentRows(ctx, w, candidates, sourceRows, compact, workers)
	if err != nil {
		return err
	}
	selected, err = ctx.materialize(p, selected, sourceRows, compact, w)
	if err != nil {
		return err
	}
	return p.emit(dst, ctx, selected, w)
}

// emit reduces the selection to the result, whichever shape the plan has.
func (p *plan) emit(dst *Result, ctx *execCtx, selected []int, w *Workspace) error {
	if err := w.checkCanceled(); err != nil {
		return err
	}
	switch {
	case p.grouped:
		return p.runGroupedInto(dst, ctx, selected, w)
	case p.singleRow:
		return p.runAggregateInto(dst, ctx, selected, w)
	default:
		return p.runProjectionInto(dst, ctx, selected, w)
	}
}

// filterSegmentRows runs the filter phase over a Segment, in parallel when the
// scan is large enough to pay for the split. Parallelism is confined to the
// dense forms — a whole scan, or a compacted candidate list — because a
// candidate list walked in place is already a sparse read whose cost is the
// indirection rather than the per-row work a split would divide.
func (p *plan) filterSegmentRows(ctx *execCtx, w *Workspace, candidates, sourceRows []int, compact bool, workers int) ([]int, error) {
	n := scanWorkerCount(ctx.rows, workers)
	if n > 1 && len(p.filterCols) > 0 && (compact || candidates == nil) {
		rows := sourceRows
		if !compact {
			rows = w.identityRows(ctx.rows)
		}
		return p.selectSegmentParallel(ctx, w, rows, n)
	}
	if err := ctx.extractValues(p, p.filterCols, sourceRows, compact, &w.text, w); err != nil {
		return nil, err
	}
	selected, err := p.selectRows(ctx, candidates, compact, w)
	if err != nil {
		return nil, err
	}
	return selected, w.eval.firstError()
}

// runSnapshotInto executes p over one heap snapshot. catalog is the database
// snapshot that snapshot was captured with, or the zero value when the source
// named a single collection; it is what a join clause resolves its inner
// collection out of, and a plan carrying one never reaches here without it.
func (p *plan) runSnapshotInto(e *Exec, snapshot store.Snapshot, catalog store.DatabaseSnapshot) error {
	if err := e.Workspace.checkCanceled(); err != nil {
		return err
	}
	e.Workspace.joinPairBudget.configure(-1)
	if p.fanOutJoin >= 0 {
		pairBytes, err := normalizeJoinPairBytes(e.Options)
		if err != nil {
			return err
		}
		e.Workspace.joinPairBudget.configure(pairBytes)
	}
	// Joins bind before anything else looks at the predicate: every later stage,
	// from the direct-answer lanes through candidate masks to the filter phase's
	// per-row recheck, reads a join leaf as an ordinary membership, and it is
	// only an ordinary membership once its slot is filled. Binding here also
	// means it happens on the calling goroutine, before any worker exists, which
	// is what lets the bindings be shared read-only across the filter phase.
	// Runtime-typed SQL joins still bind and scan at LIMIT 0: PostgreSQL resolves
	// the ON operator before LIMIT can discard its rows.
	if p.hasLimit && p.limit == 0 && !p.requiresSQLDomainScan() {
		for i := range p.joins {
			if _, ok := catalog.Collection(p.joins[i].collection); !ok {
				return fmt.Errorf(
					"query: join: collection %q is not in the database snapshot",
					p.joins[i].collection,
				)
			}
		}
		if err := p.validateHeapMarkDependencies(catalog); err != nil {
			return err
		}
		e.Stats = ExecStats{}
		return prepareResult(&e.Result, p, 0)
	}
	if err := p.bindJoins(
		&e.Workspace, snapshot, catalog,
		e.Options.JoinMembershipMax, e.Options.JoinFilterScanRatio,
		&e.Workspace.joinPairBudget,
		e.Workspace.activeHeapWorkBudget(),
		&e.Stats,
	); err != nil {
		return err
	}
	if err := p.bindMarks(
		&e.Workspace, snapshot, catalog, e.Workspace.activeHeapWorkBudget(),
	); err != nil {
		return err
	}
	if err := e.Workspace.checkCanceled(); err != nil {
		return err
	}
	err := p.runSnapshotRows(&e.Result, snapshot, catalog, &e.Workspace, e.Options.Workers)
	e.Workspace.collectJoinStats(p, &e.Stats)
	return err
}

func (p *plan) runSnapshotRows(dst *Result, snapshot store.Snapshot, catalog store.DatabaseSnapshot, w *Workspace, workers int) error {
	if err := w.checkCanceled(); err != nil {
		return err
	}
	w.candidateUsed = 0
	w.storeMaskUsed = 0
	w.text = w.text[:0]
	w.lateText = w.lateText[:0]
	w.groupKey = w.groupKey[:0]
	w.groupOrder = w.groupOrder[:0]
	// The pair space is emptied here rather than in the expansion, so that a
	// plan that never reaches the expansion — a direct-answer lane, or an error
	// on the way — cannot leave the previous execution's pair count to be read
	// back as this one's.
	w.pairInner = w.pairInner[:0]
	w.pairInnerPair = w.pairInnerPair[:0]
	w.interner.Reset()
	if p.fanOutJoin < 0 && len(p.marks) == 0 {
		if handled, err := p.runDirectSnapshotAggregate(dst, snapshot, w); err != nil {
			return err
		} else if handled {
			return nil
		}
	}
	if p.fanOutJoin < 0 && len(p.marks) == 0 {
		if handled, err := p.runDirectSnapshotStringCountGroups(dst, snapshot, w); err != nil {
			return err
		} else if handled {
			return nil
		}
	}
	chunks := snapshot.Chunks()
	if err := w.activeHeapWorkBudget().admitPlanner(
		p, snapshot.Len(), heapWorkSnapshot, false, chunks,
	); err != nil {
		return err
	}
	if p.fanOutJoin < 0 && len(p.marks) == 0 {
		// The direct-answer lanes count and reduce the driving collection's
		// rows. A fan-out plan's row count is its pairs, which those lanes have
		// no way to reach, so they decline rather than answer confidently about
		// the wrong cardinality.
		if handled, err := p.runDirectSnapshotIndexedCount(dst, snapshot, w); err != nil {
			return err
		} else if handled {
			return nil
		}
	}
	if p.fanOutJoin < 0 && len(p.marks) == 0 {
		if handled, err := p.runDirectSnapshotScalarCount(dst, snapshot, w); err != nil {
			return err
		} else if handled {
			return nil
		}
	}
	masks, err := p.storeCandidateMasks(snapshot, w)
	if err != nil {
		return err
	}
	if err := w.checkCanceled(); err != nil {
		return err
	}
	candidateCount := 0
	for i, mask := range masks {
		if err := cancellationCheckpoint(w.cancel, i); err != nil {
			return err
		}
		candidateCount += bits.OnesCount64(mask.Bits)
	}
	compact := masks != nil && candidateCount <= snapshot.Len()/2
	scanRows := snapshot.Len()
	if compact {
		scanRows = candidateCount
	}
	if err := w.activeHeapWorkBudget().admitRows(
		p, scanRows, heapWorkSnapshot, workers,
	); err != nil {
		return err
	}
	w.storeRows = w.storeRows[:0]
	if compact {
		for i, mask := range masks {
			if err := cancellationCheckpoint(w.cancel, i); err != nil {
				return err
			}
			for word := mask.Bits; word != 0; word &= word - 1 {
				w.storeRows = append(w.storeRows, store.Location{
					Chunk: mask.Chunk,
					Slot:  uint8(bits.TrailingZeros64(word)),
				})
			}
		}
	}

	ctx := &w.ctx
	ctx.s, ctx.rows = nil, snapshot.Len()
	if compact {
		ctx.rows = len(w.storeRows)
	}
	if p.where == nil && p.fanOutJoin < 0 {
		if err := ctx.extractSnapshotValues(p, snapshot, p.lateCols, w.storeRows, compact, &w.lateText, w); err != nil {
			return err
		}
		if err := ctx.extractSnapshotNums(p, snapshot, w.storeRows, compact, w); err != nil {
			return err
		}
		selected, err := p.selectRows(ctx, nil, compact, w)
		if err != nil {
			return err
		}
		return p.emit(dst, ctx, selected, w)
	}
	if p.where == nil {
		if err := ctx.extractSnapshotValues(
			p, snapshot, p.filterCols, w.storeRows, compact, &w.text, w,
		); err != nil {
			return err
		}
		selected, err := p.selectRows(ctx, nil, compact, w)
		if err != nil {
			return err
		}
		selected, err = ctx.materializeFanOut(
			p, snapshot, catalog, selected, compact, w,
		)
		if err != nil {
			return err
		}
		return p.emit(dst, ctx, selected, w)
	}
	selected, err := p.filterSnapshotRows(ctx, w, snapshot, compact, workers)
	if err != nil {
		return err
	}
	if p.fanOutJoin >= 0 {
		selected, err = ctx.materializeFanOut(p, snapshot, catalog, selected, compact, w)
	} else {
		selected, err = ctx.materializeSnapshot(p, snapshot, selected, compact, w)
	}
	if err != nil {
		return err
	}
	return p.emit(dst, ctx, selected, w)
}

// filterSnapshotRows is filterSegmentRows over a heap snapshot. The parallel
// path may decline, which is why it answers with a flag rather than a nil
// selection: a filter that selected nothing is a real answer and must not be
// mistaken for one.
func (p *plan) filterSnapshotRows(ctx *execCtx, w *Workspace, snapshot store.Snapshot, compact bool, workers int) ([]int, error) {
	if n := scanWorkerCount(ctx.rows, workers); n > 1 && len(p.filterCols) > 0 {
		selected, ok, err := p.selectSnapshotParallel(ctx, w, snapshot, compact, n)
		if err != nil || ok {
			return selected, err
		}
	}
	if err := ctx.extractSnapshotValues(p, snapshot, p.filterCols, w.storeRows, compact, &w.text, w); err != nil {
		return nil, err
	}
	selected, err := p.selectRows(ctx, nil, compact, w)
	if err != nil {
		return nil, err
	}
	return selected, w.eval.firstError()
}

func preferSparseRows(candidates, total int, hasBound bool) bool {
	return hasBound && candidates <= total/2
}

// Late materialization.
//
// A scan used to classify every value path for every scanned row and only then
// apply WHERE. At the selectivity a filter is usually written for that is the
// wrong order by two orders of magnitude: at one percent, ninety-nine of every
// hundred rows had every projected, ordered, and grouped column resolved,
// decoded, and classified purely to be dropped by the next pass.
//
// The plan already distinguishes the columns WHERE reads (filterCols) from the
// ones only the output reads (lateCols), so execution runs in two phases: the
// filter columns are extracted for every scanned row and reduced to a
// selection, and the output columns are then gathered for the survivors alone.
// The gather is the same sparse row-address read an index bound already used,
// so nothing new had to be taught to the storage layer.
//
// Compaction is what lets the second phase stay a plain dense column. Once the
// output columns hold one entry per survivor, the filter columns are gathered
// down onto the same positions and the selection is renumbered to the identity,
// so every consumer downstream — projection, ordering, grouping, aggregation —
// keeps indexing columns by row with no idea a filter ran at all.

// lateGatherRejectShift sets how much of a scan a filter must reject before the
// output columns are gathered for the survivors instead of read in full: one
// eighth, so a selection of up to seven eighths of the scanned rows still
// gathers.
//
// The threshold is this permissive because the premise it was written against
// did not survive measurement. A gather was expected to cost meaningfully more
// per row than the fused sequential column read, which would have put the
// crossover near half; on a 65536-row corpus it does not, because the sparse
// read resolves each row through the same proven shape the sequential one does.
// Segment ns/doc for a one-column projection, by threshold and selectivity:
//
//	selectivity    50%     75%    100%
//	gather ≤1/2    127.7   128.9  132.9   (no gather at any of these)
//	gather ≤7/8    100.4   117.9  131.3
//	gather always   97.7   113.9  129.3
//
// So gathering is never the worse choice, and the only thing the threshold
// still buys is skipping the survivor-address list and the compaction pass for
// a predicate that rejects almost nothing — where they are pure overhead and
// the numbers above are within noise of each other.
const lateGatherRejectShift = 3

// gatherSurvivors reports whether the output columns should be gathered for the
// selected rows rather than read in full. An unfiltered scan never gathers:
// every row survives, so the gather would read the same values through the
// slower address-at-a-time path. Neither does a plan whose output columns are
// all already extracted, where compaction would be a copy that saves no read.
func (p *plan) gatherSurvivors(selected, rows int) bool {
	if p.where == nil || len(p.lateCols)+len(p.numPaths) == 0 {
		return false
	}
	return selected <= rows-rows>>lateGatherRejectShift
}

// truncateEarly applies LIMIT before materialization, for the plans where the
// limit alone decides which rows reach the result. An ordered plan must sort
// every survivor before it knows which ones those are, and a grouped or
// aggregated plan reduces all of them, so both keep the full selection.
func (p *plan) truncateEarly(selected []int) []int {
	if p.grouped || p.singleRow || len(p.order) != 0 || !p.hasLimit {
		return selected
	}
	if len(selected) > p.limit {
		return selected[:p.limit]
	}
	return selected
}

// materialize produces the columns the output reads, either for the surviving
// rows alone or for every scanned row, and returns the selection renumbered
// onto whichever the columns now hold.
func (ctx *execCtx) materialize(p *plan, selected, sourceRows []int, compact bool, w *Workspace) ([]int, error) {
	if !p.gatherSurvivors(len(selected), ctx.rows) {
		if err := ctx.extractValues(p, p.lateCols, sourceRows, compact, &w.lateText, w); err != nil {
			return nil, err
		}
		if err := ctx.extractNums(p, sourceRows, compact, w); err != nil {
			return nil, err
		}
		return selected, nil
	}
	selected = p.truncateEarly(selected)
	// A compacted scan numbers its rows by position in the candidate list, so
	// the survivors have to be translated back to segment ordinals before the
	// storage layer can address them.
	rows := selected
	if compact {
		gathered := resize(w.lateRows, len(selected))
		if w.cancel == nil {
			for i, row := range selected {
				gathered[i] = sourceRows[row]
			}
		} else {
			for i, row := range selected {
				if err := cancellationCheckpoint(w.cancel, i); err != nil {
					return nil, err
				}
				gathered[i] = sourceRows[row]
			}
		}
		w.lateRows = gathered
		rows = gathered
	}
	if err := ctx.extractValues(p, p.lateCols, rows, true, &w.lateText, w); err != nil {
		return nil, err
	}
	if err := ctx.extractNums(p, rows, true, w); err != nil {
		return nil, err
	}
	return ctx.compactOnto(p, selected, w.cancel)
}

// materializeSnapshot is materialize over a heap snapshot, whose rows are
// addressed by stable chunk/slot location rather than by segment ordinal.
func (ctx *execCtx) materializeSnapshot(p *plan, snapshot store.Snapshot, selected []int, compact bool, w *Workspace) ([]int, error) {
	if !p.gatherSurvivors(len(selected), ctx.rows) {
		if err := ctx.extractSnapshotValues(p, snapshot, p.lateCols, w.storeRows, compact, &w.lateText, w); err != nil {
			return nil, err
		}
		if err := ctx.extractSnapshotNums(p, snapshot, w.storeRows, compact, w); err != nil {
			return nil, err
		}
		return selected, nil
	}
	selected = p.truncateEarly(selected)
	rows := w.lateStoreRows[:0]
	if compact {
		if w.cancel == nil {
			for _, row := range selected {
				rows = append(rows, w.storeRows[row])
			}
		} else {
			for i, row := range selected {
				if err := cancellationCheckpoint(w.cancel, i); err != nil {
					return nil, err
				}
				rows = append(rows, w.storeRows[row])
			}
		}
	} else {
		// An uncompacted scan numbered its rows by position in the snapshot's
		// live chunk/slot order, which is exactly the order AppendPointer
		// produced them in and exactly the order the live masks enumerate. The
		// masks are therefore the map from scan ordinal back to address.
		w.liveMasks = snapshot.AppendLiveMasks(w.liveMasks[:0])
		if err := w.checkCanceled(); err != nil {
			return nil, err
		}
		var err error
		rows, err = appendMaskLocations(rows, w.liveMasks, selected, w.cancel)
		if err != nil {
			return nil, err
		}
	}
	w.lateStoreRows = rows
	if err := ctx.extractSnapshotValues(p, snapshot, p.lateCols, rows, true, &w.lateText, w); err != nil {
		return nil, err
	}
	if err := ctx.extractSnapshotNums(p, snapshot, rows, true, w); err != nil {
		return nil, err
	}
	return ctx.compactOnto(p, selected, w.cancel)
}

// compactOnto gathers the filter columns down onto the positions the late
// gather just wrote and renumbers the selection to the identity, so the whole
// column set is dense over the survivors and every downstream consumer keeps
// indexing by row. The gather is in place and safe because the selection is
// ascending, so selected[i] is never below i.
func (ctx *execCtx) compactOnto(
	p *plan,
	selected []int,
	cancel *CancelFlag,
) ([]int, error) {
	if cancel == nil {
		for _, c := range p.filterCols {
			col := ctx.values[c]
			for i, row := range selected {
				col[i] = col[row]
			}
			ctx.values[c] = col[:len(selected)]
		}
		ctx.rows = len(selected)
		for i := range selected {
			selected[i] = i
		}
		return selected, nil
	}
	for _, c := range p.filterCols {
		col := ctx.values[c]
		for i, row := range selected {
			if err := cancellationCheckpoint(cancel, i); err != nil {
				return selected, err
			}
			col[i] = col[row]
		}
		ctx.values[c] = col[:len(selected)]
	}
	ctx.rows = len(selected)
	for i := range selected {
		if err := cancellationCheckpoint(cancel, i); err != nil {
			return selected, err
		}
		selected[i] = i
	}
	return selected, cancellationError(cancel)
}

// appendMaskLocations resolves ascending scan ordinals to stable chunk/slot
// addresses through the mask sequence that enumerated them. It walks each
// mask's set bits forward alongside the ordinals it is asked for, so the whole
// resolution costs one pass over the live universe rather than a popcount
// search per row.
func appendMaskLocations(
	dst []store.Location,
	masks []store.Mask,
	rows []int,
	cancel *CancelFlag,
) ([]store.Location, error) {
	base, next := 0, 0
	if cancel == nil {
		for _, mask := range masks {
			count := bits.OnesCount64(mask.Bits)
			word, ordinal := mask.Bits, base
			for next < len(rows) && rows[next] < base+count {
				for ordinal < rows[next] {
					word &= word - 1
					ordinal++
				}
				dst = append(dst, store.Location{
					Chunk: mask.Chunk,
					Slot:  uint8(bits.TrailingZeros64(word)),
				})
				next++
			}
			base += count
		}
		return dst, nil
	}
	for i, mask := range masks {
		if err := cancellationCheckpoint(cancel, i); err != nil {
			return dst, err
		}
		count := bits.OnesCount64(mask.Bits)
		word, ordinal := mask.Bits, base
		for next < len(rows) && rows[next] < base+count {
			if err := cancellationCheckpoint(cancel, next); err != nil {
				return dst, err
			}
			for ordinal < rows[next] {
				word &= word - 1
				ordinal++
			}
			dst = append(dst, store.Location{
				Chunk: mask.Chunk,
				Slot:  uint8(bits.TrailingZeros64(word)),
			})
			next++
		}
		base += count
	}
	return dst, cancellationError(cancel)
}

// extract materializes every value and numeric column for the rows in scope.
// It is the whole-plan form the durable batch executor runs, where a batch has
// already been rebuilt into a throwaway per-worker Segment: nothing there is
// retained across batches for a filter-first split to save reading twice, and
// the batch is sized to fit in cache anyway.
func (ctx *execCtx) extract(p *plan, sourceRows []int, w *Workspace) error {
	gather := sourceRows != nil
	if err := ctx.extractValues(p, p.filterCols, sourceRows, gather, &w.text, w); err != nil {
		return err
	}
	if err := ctx.extractValues(p, p.lateCols, sourceRows, gather, &w.lateText, w); err != nil {
		return err
	}
	return ctx.extractNums(p, sourceRows, gather, w)
}

// extractValues gathers the named value columns and classifies them. Every raw
// value is gathered before any string is classified, which permits one exact
// pre-growth of text: escaped strings then append without moving the views
// produced for earlier columns in the same phase.
func (ctx *execCtx) extractValues(p *plan, cols []int, rows []int, gather bool, text *[]byte, w *Workspace) error {
	w.raws = resize(w.raws, len(p.valuePaths))
	ctx.values = resize(ctx.values, len(p.valuePaths))
	if len(cols) == 0 {
		return nil
	}
	textNeed := 0
	for at, c := range cols {
		if err := cancellationCheckpoint(w.cancel, at); err != nil {
			return err
		}
		raws, err := ctx.rawColumn(w.raws[c][:0], p.valuePaths[c], rows, gather)
		if err != nil {
			return err
		}
		if err := w.checkCanceled(); err != nil {
			return err
		}
		w.raws[c] = raws
		escaped, err := escapedTextBytesCancelable(raws, w.cancel)
		if err != nil {
			return err
		}
		textNeed += escaped
	}
	return ctx.classifyColumns(cols, text, textNeed, w)
}

func (ctx *execCtx) extractSnapshotValues(p *plan, snapshot store.Snapshot, cols []int, rows []store.Location, gather bool, text *[]byte, w *Workspace) error {
	w.raws = resize(w.raws, len(p.valuePaths))
	ctx.values = resize(ctx.values, len(p.valuePaths))
	if len(cols) == 0 {
		return nil
	}
	textNeed := 0
	for at, c := range cols {
		if err := cancellationCheckpoint(w.cancel, at); err != nil {
			return err
		}
		var raws []vibejson.RawValue
		var err error
		cp := p.valuePaths[c]
		switch {
		// A single top-level field goes through the fused shape-routed column
		// primitive, in both the dense and the gathered phase, exactly as the
		// Segment backend's rawColumn does. That is not only the faster read:
		// the pointer walk and the field primitive disagree on a document
		// whose root is an array, where a named pointer token is a pointer
		// error and an absent member is simply absent. Routing every
		// extraction of a single field through one primitive is what keeps a
		// query's result — and whether it reports an error at all —
		// independent of how much of the collection the planner chose to
		// read, which block pruning and late materialization now both vary.
		case cp.single && gather:
			raws = snapshot.AppendFieldRows(w.raws[c][:0], rows, cp.name, &ctx.cache)
		case cp.single:
			raws = snapshot.AppendField(w.raws[c][:0], cp.name, &ctx.cache)
		case gather:
			raws, err = snapshot.AppendPointerRows(w.raws[c][:0], rows, cp.pointerForStore())
		default:
			raws, err = snapshot.AppendPointer(w.raws[c][:0], cp.pointerForStore())
		}
		if err != nil {
			return err
		}
		if err := w.checkCanceled(); err != nil {
			return err
		}
		w.raws[c] = raws
		escaped, err := escapedTextBytesCancelable(raws, w.cancel)
		if err != nil {
			return err
		}
		textNeed += escaped
	}
	return ctx.classifyColumns(cols, text, textNeed, w)
}

// escapedTextBytes is the exact decoded-text budget a column needs: only a
// string carrying a backslash decodes into the arena, and its decoding is never
// longer than its source.
func escapedTextBytes(raws []vibejson.RawValue) int {
	need := 0
	for _, r := range raws {
		b := r.Bytes()
		if r.Kind() == document.String && bytes.IndexByte(b, '\\') >= 0 {
			need += len(b)
		}
	}
	return need
}

func escapedTextBytesCancelable(raws []vibejson.RawValue, cancel *CancelFlag) (int, error) {
	if cancel == nil {
		return escapedTextBytes(raws), nil
	}
	need := 0
	for i, r := range raws {
		if err := cancellationCheckpoint(cancel, i); err != nil {
			return 0, err
		}
		b := r.Bytes()
		if r.Kind() == document.String && bytes.IndexByte(b, '\\') >= 0 {
			need += len(b)
		}
	}
	if err := cancellationError(cancel); err != nil {
		return 0, err
	}
	return need, nil
}

func (ctx *execCtx) classifyColumns(cols []int, text *[]byte, textNeed int, w *Workspace) error {
	if err := w.joinPairBudget.admitText(textNeed); err != nil {
		return err
	}
	if err := w.admitDecodedText(textNeed); err != nil {
		return err
	}
	if cap(*text) < textNeed {
		*text = make([]byte, 0, growCap(cap(*text), textNeed))
	}
	if w.cancel == nil {
		for _, c := range cols {
			raws := w.raws[c]
			col := resize(ctx.values[c], len(raws))
			for j, r := range raws {
				col[j] = classifyRawInto(r, text)
			}
			ctx.values[c] = col
		}
		return nil
	}
	for _, c := range cols {
		raws := w.raws[c]
		col := resize(ctx.values[c], len(raws))
		for j, r := range raws {
			if err := cancellationCheckpoint(w.cancel, j); err != nil {
				return err
			}
			col[j] = classifyRawInto(r, text)
		}
		ctx.values[c] = col
	}
	return w.checkCanceled()
}

func (ctx *execCtx) extractNums(p *plan, rows []int, gather bool, w *Workspace) error {
	ctx.nums = resize(ctx.nums, len(p.numPaths))
	for i, cp := range p.numPaths {
		if err := cancellationCheckpoint(w.cancel, i); err != nil {
			return err
		}
		nc, err := ctx.numericColumn(ctx.nums[i], cp, rows, gather, w)
		if err != nil {
			return err
		}
		ctx.nums[i] = nc
	}
	return nil
}

func (ctx *execCtx) extractSnapshotNums(p *plan, snapshot store.Snapshot, rows []store.Location, gather bool, w *Workspace) error {
	return ctx.extractSnapshotNumCols(p, snapshot, p.lateNums, rows, gather, w)
}

// extractSnapshotNumCols is extractSnapshotNums restricted to a column list, so
// a fan-out clause can fill its own numeric columns from its own collection
// through the same routine the driving side uses.
func (ctx *execCtx) extractSnapshotNumCols(p *plan, snapshot store.Snapshot, cols []int, rows []store.Location, gather bool, w *Workspace) error {
	ctx.nums = resize(ctx.nums, len(p.numPaths))
	for at, i := range cols {
		if err := cancellationCheckpoint(w.cancel, at); err != nil {
			return err
		}
		cp := p.numPaths[i]
		var raws []vibejson.RawValue
		var err error
		switch {
		case cp.single && gather:
			// The field primitive rather than the pointer walk, for the reason
			// on AppendFieldRows — a
			// document whose root is an array makes a named pointer token an
			// error and a named member merely absent, and which of those a
			// query sees must not depend on how many rows the planner decided
			// to read.
			raws = snapshot.AppendFieldRows(w.numRaws[:0], rows, cp.name, &ctx.cache)
		case cp.single:
			raws = snapshot.AppendField(w.numRaws[:0], cp.name, &ctx.cache)
		case gather:
			raws, err = snapshot.AppendPointerRows(w.numRaws[:0], rows, cp.pointerForStore())
		default:
			raws, err = snapshot.AppendPointer(w.numRaws[:0], cp.pointerForStore())
		}
		if err != nil {
			return err
		}
		if err := w.checkCanceled(); err != nil {
			return err
		}
		w.numRaws = raws
		ctx.nums[i], err = numericRawsCancelable(ctx.nums[i], raws, w.cancel)
		if err != nil {
			return err
		}
	}
	return nil
}

// rawColumn reads one value column, either as a fused sequential pass over
// every row or as a sparse gather of exactly rows. The choice is an explicit
// flag rather than a nil check on rows, because an empty gather is a real
// answer: a filter that selected nothing must read nothing, not fall back to
// scanning the whole column.
func (ctx *execCtx) rawColumn(dst []vibejson.RawValue, cp compiledPath, rows []int, gather bool) ([]vibejson.RawValue, error) {
	if gather {
		if cp.single {
			return ctx.cache.AppendFieldRows(dst, ctx.s, rows, cp.name), nil
		}
		return ctx.s.AppendPointerRows(dst, rows, cp.pointer)
	}
	if cp.single {
		return ctx.cache.AppendField(dst, ctx.s, cp.name), nil
	}
	return ctx.s.AppendPointer(dst, cp.pointer)
}

func (ctx *execCtx) numericColumn(dst numColumn, cp compiledPath, rows []int, gather bool, w *Workspace) (numColumn, error) {
	raws, err := ctx.rawColumn(w.numRaws[:0], cp, rows, gather)
	if err != nil {
		return numColumn{}, err
	}
	if err := w.checkCanceled(); err != nil {
		return numColumn{}, err
	}
	w.numRaws = raws
	return numericRawsCancelable(dst, raws, w.cancel)
}

func numericRaws(dst numColumn, raws []vibejson.RawValue) numColumn {
	vals := resize(dst.vals, len(raws))
	clear(vals)
	for i, r := range raws {
		if r.Kind() == document.Number {
			raw := r.Bytes()
			value := scalar{kind: kindNumber, num: raw, raw: raw}
			if integer, ok := r.Int64(); ok {
				value.isInt, value.ival = true, integer
			}
			vals[i] = value
		}
	}
	return numColumn{vals: vals}
}

func numericRawsCancelable(dst numColumn, raws []vibejson.RawValue, cancel *CancelFlag) (numColumn, error) {
	if cancel == nil {
		return numericRaws(dst, raws), nil
	}
	vals := resize(dst.vals, len(raws))
	clear(vals)
	for i, r := range raws {
		if err := cancellationCheckpoint(cancel, i); err != nil {
			return numColumn{vals: vals}, err
		}
		if r.Kind() == document.Number {
			raw := r.Bytes()
			value := scalar{kind: kindNumber, num: raw, raw: raw}
			if integer, ok := r.Int64(); ok {
				value.isInt, value.ival = true, integer
			}
			vals[i] = value
		}
	}
	if err := cancellationError(cancel); err != nil {
		return numColumn{vals: vals}, err
	}
	return numColumn{vals: vals}, nil
}

func (p *plan) selectRows(ctx *execCtx, candidates []int, compact bool, w *Workspace) ([]int, error) {
	if err := p.validateSQLJoinOuterDomains(
		ctx.values, w.eval.binds, 0, ctx.rows, w.cancel,
	); err != nil {
		return nil, err
	}
	if err := p.validateSQLPathDomains(
		ctx.values, w.eval.correlations, 0, ctx.rows, w.cancel,
	); err != nil {
		return nil, err
	}
	selected := w.selected[:0]
	cancel := w.cancel
	if cancel != nil {
		switch {
		case p.where == nil:
			for row := 0; row < ctx.rows; row++ {
				if err := cancellationCheckpoint(cancel, row); err != nil {
					w.selected = selected
					return nil, err
				}
				selected = append(selected, row)
			}
		case compact || candidates == nil:
			var err error
			selected, err = selectSpanCancelable(
				p.where, selected, ctx.values, 0, ctx.rows, &w.eval, cancel,
			)
			if err != nil {
				w.selected = selected
				return nil, err
			}
		default:
			for i, row := range candidates {
				if err := cancellationCheckpoint(cancel, i); err != nil {
					w.selected = selected
					return nil, w.eval.errorOr(err)
				}
				if p.where.eval(ctx.values, row, &w.eval) {
					selected = append(selected, row)
				}
			}
		}
		w.selected = selected
		if err := cancellationError(cancel); err != nil {
			return nil, w.eval.errorOr(err)
		}
		return selected, nil
	}
	switch {
	case p.where == nil:
		for row := 0; row < ctx.rows; row++ {
			selected = append(selected, row)
		}
	case compact || candidates == nil:
		// A dense scan is where the per-row dispatch is worth hoisting, and it
		// is the only shape that can be: a candidate list is already a sparse
		// walk whose cost is the indirection, not the switch.
		if out, ok := p.where.selectSpan(selected, ctx.values, 0, ctx.rows); ok {
			selected = out
			break
		}
		for row := 0; row < ctx.rows; row++ {
			if p.where.eval(ctx.values, row, &w.eval) {
				selected = append(selected, row)
			}
		}
	default:
		for _, row := range candidates {
			if p.where.eval(ctx.values, row, &w.eval) {
				selected = append(selected, row)
			}
		}
	}
	w.selected = selected
	return selected, nil
}

// selectSpanCancelable keeps the dense predicate evaluator's hoisted dispatch
// while inserting a checkpoint between fixed-size spans. Falling back to eval
// is still span-bounded for predicate shapes selectSpan deliberately declines.
// The nil cancellation path never calls this helper, so its single whole-span
// dispatch remains exactly as before.
func selectSpanCancelable(
	p *compiledPredicate,
	dst []int,
	cols [][]scalar,
	lo, hi int,
	eval *evalScratch,
	cancel *CancelFlag,
) ([]int, error) {
	const spanRows = cancellationCheckMask + 1
	for lo < hi {
		if err := cancellationError(cancel); err != nil {
			return dst, eval.errorOr(err)
		}
		end := min(lo+spanRows, hi)
		if out, ok := p.selectSpan(dst, cols, lo, end); ok {
			dst = out
		} else {
			for row := lo; row < end; row++ {
				if p.eval(cols, row, eval) {
					dst = append(dst, row)
				}
			}
		}
		lo = end
	}
	if err := cancellationError(cancel); err != nil {
		return dst, eval.errorOr(err)
	}
	return dst, nil
}

func (p *plan) candidateRows(s *store.Segment, w *Workspace) []int {
	if p.where == nil || !s.Postings || p.requiresSQLDomainScan() {
		return nil
	}
	rows, ok := p.where.candidates(s, w)
	if !ok {
		return nil
	}
	if rows == nil {
		return w.emptyCandidate[:0]
	}
	return rows
}

func (p *plan) runProjectionInto(dst *Result, ctx *execCtx, selected []int, w *Workspace) error {
	if err := w.checkCanceled(); err != nil {
		return err
	}
	if len(p.order) != 0 {
		slices.SortStableFunc(selected, func(a, b int) int {
			return p.compareRows(ctx, a, b)
		})
		if err := w.checkCanceled(); err != nil {
			return err
		}
	}
	if p.hasLimit && len(selected) > p.limit {
		selected = selected[:p.limit]
	}
	if err := prepareResult(dst, p, len(selected)); err != nil {
		return err
	}
	for r, row := range selected {
		if err := cancellationCheckpoint(w.cancel, r); err != nil {
			return err
		}
		for c, col := range p.columns {
			cell := Cell{kind: TypeNull}
			if col.value >= 0 {
				cell = cellFromScalar(ctx.values[col.value][row])
			}
			if err := dst.admitResultCell(cell); err != nil {
				return err
			}
			dst.Columns[c].Cells[r] = cell
		}
	}
	return w.checkCanceled()
}

func (p *plan) compareRows(ctx *execCtx, a, b int) int {
	for _, o := range p.order {
		c := compareScalar(ctx.values[o.value][a], ctx.values[o.value][b])
		if o.dir == Desc {
			c = -c
		}
		if c != 0 {
			return c
		}
	}
	return 0
}

func (p *plan) runAggregateInto(dst *Result, ctx *execCtx, selected []int, w *Workspace) error {
	w.accs = resize(w.accs, len(p.columns))
	resetAggs(w.accs)
	for i, row := range selected {
		if err := cancellationCheckpoint(w.cancel, i); err != nil {
			return err
		}
		if err := p.accumulate(w.accs, ctx, row, &w.aggregateBudget); err != nil {
			return err
		}
	}
	rows := 1
	if p.hasLimit && p.limit == 0 {
		rows = 0
	}
	if err := prepareResult(dst, p, rows); err != nil {
		return err
	}
	if rows != 0 {
		return p.fillAggregateCells(dst, 0, w.accs, nil, w)
	}
	return nil
}

func (p *plan) runGroupedInto(dst *Result, ctx *execCtx, selected []int, w *Workspace) error {
	if err := w.activeHeapWorkBudget().admitGroups(p, len(selected)); err != nil {
		return err
	}
	groupCount := 0
	for at, row := range selected {
		if err := cancellationCheckpoint(w.cancel, at); err != nil {
			return err
		}
		keyBytes := p.groupKeyBytes(ctx, row)
		if err := w.activeHeapWorkBudget().admitGroupKeyScratch(keyBytes); err != nil {
			return err
		}
		w.groupKey = p.groupKey(w.groupKey[:0], ctx, row)
		id, exists := w.interner.Lookup(w.groupKey)
		if !exists {
			if err := w.activeHeapWorkBudget().admitGroupKey(len(w.groupKey)); err != nil {
				return err
			}
			id = w.interner.Intern(w.groupKey)
		}
		if !exists {
			w.groups = resize(w.groups, groupCount+1)
			g := &w.groups[int(id)]
			g.scalars = resize(g.scalars, len(p.groupCols))
			g.accs = resize(g.accs, len(p.columns))
			resetAggs(g.accs)
			for i, gc := range p.groupCols {
				g.scalars[i] = ctx.values[gc][row]
			}
			groupCount++
		}
		if err := p.accumulate(w.groups[int(id)].accs, ctx, row, &w.aggregateBudget); err != nil {
			return err
		}
	}
	// Groups beyond this run's count keep their scalars from the previous run,
	// and those scalars carry raw bytes and strings borrowed from that run's
	// documents. Truncating alone leaves them reachable through the retained
	// capacity, so a Workspace would pin the whole previous Segment's byte
	// arena for as long as it lives. Clearing the elements releases those
	// references while keeping the slices themselves, so the next run still
	// reuses the storage — the same discipline prepareResult applies to the
	// result cells it retains.
	for i := groupCount; i < len(w.groups); i++ {
		clear(w.groups[i].scalars)
		resetAggs(w.groups[i].accs)
	}
	w.groups = w.groups[:groupCount]
	w.groupOrder = resize(w.groupOrder[:0], groupCount)
	for i := range w.groupOrder {
		w.groupOrder[i] = i
	}
	if len(p.order) != 0 {
		if err := w.checkCanceled(); err != nil {
			return err
		}
		slices.SortStableFunc(w.groupOrder, func(a, b int) int {
			return p.compareGroups(&w.groups[a], &w.groups[b])
		})
		if err := w.checkCanceled(); err != nil {
			return err
		}
	}
	if p.hasLimit && len(w.groupOrder) > p.limit {
		w.groupOrder = w.groupOrder[:p.limit]
	}
	if err := prepareResult(dst, p, len(w.groupOrder)); err != nil {
		return err
	}
	for row, id := range w.groupOrder {
		if err := cancellationCheckpoint(w.cancel, row); err != nil {
			return err
		}
		g := &w.groups[id]
		if err := p.fillAggregateCells(dst, row, g.accs, g, w); err != nil {
			return err
		}
	}
	return nil
}

func (p *plan) compareGroups(a, b *group) int {
	for _, o := range p.order {
		c := compareScalar(a.scalars[o.slot], b.scalars[o.slot])
		if o.dir == Desc {
			c = -c
		}
		if c != 0 {
			return c
		}
	}
	return 0
}

func (p *plan) groupKey(dst []byte, ctx *execCtx, row int) []byte {
	for _, gc := range p.groupCols {
		dst = appendGroupKey(dst, ctx.values[gc][row])
	}
	return dst
}

func (p *plan) groupKeyBytes(ctx *execCtx, row int) int64 {
	var bytes int64
	for _, gc := range p.groupCols {
		bytes = saturatedBytes(bytes, scalarGroupKeyBytes(ctx.values[gc][row]))
	}
	return bytes
}

type group struct {
	scalars []scalar
	accs    []aggAcc
}

func (p *plan) accumulate(accs []aggAcc, ctx *execCtx, row int, budget *aggregateBudget) error {
	for c, col := range p.columns {
		switch col.agg {
		case aggCount:
			// COUNT(path) follows SQL's COUNT(expr): both an absent path and
			// an explicit JSON null are excluded. `present` intentionally has
			// different semantics for EXISTS/IS PRESENT predicates.
			if col.value < 0 || ctx.values[col.value][row].kind != kindNull {
				accs[c].count++
			}
		case aggSum, aggAvg, aggMin, aggMax:
			nc := ctx.nums[col.num]
			value := nc.vals[row]
			if value.kind != kindNumber {
				continue
			}
			if err := accs[c].accumulateNumber(col.agg, value, budget); err != nil {
				return err
			}
		}
	}
	return nil
}

func (p *plan) fillAggregateCells(dst *Result, row int, accs []aggAcc, g *group, w *Workspace) error {
	for c, col := range p.columns {
		if err := cancellationCheckpoint(w.cancel, c); err != nil {
			return err
		}
		var cell Cell
		var err error
		switch col.agg {
		case aggNone:
			cell = cellFromScalar(g.scalars[col.slot])
		case aggCount:
			cell = w.countCell(accs[c].count)
		case aggSum:
			cell, err = w.exactSumCell(&accs[c])
		case aggAvg:
			cell, err = w.exactAverageCell(&accs[c])
		case aggMin:
			if accs[c].num == nil || accs[c].num.n == 0 {
				cell = nullCell()
			} else {
				cell = cellFromScalar(accs[c].num.extreme)
			}
		case aggMax:
			if accs[c].num == nil || accs[c].num.n == 0 {
				cell = nullCell()
			} else {
				cell = cellFromScalar(accs[c].num.extreme)
			}
		}
		if err != nil {
			return err
		}
		if err := dst.admitResultCell(cell); err != nil {
			return err
		}
		dst.Columns[c].Cells[row] = cell
	}
	return nil
}

func (w *Workspace) countCell(n int) Cell {
	return Cell{kind: TypeNumber, flag: cellInteger, word: uint64(n)}
}

func prepareResult(dst *Result, p *plan, rows int) error {
	if err := dst.admitResultShape(len(p.columns), rows); err != nil {
		return err
	}
	if cap(dst.Columns) < len(p.columns) {
		dst.Columns = make([]ResultColumn, len(p.columns))
	} else {
		for i := len(p.columns); i < len(dst.Columns); i++ {
			cells := dst.Columns[i].Cells
			clear(cells)
			dst.Columns[i] = ResultColumn{Cells: cells[:0]}
		}
		dst.Columns = dst.Columns[:len(p.columns)]
	}
	for i := range p.columns {
		cells := dst.Columns[i].Cells
		if rows < len(cells) {
			clear(cells[rows:])
		}
		cells = resize(cells, rows)
		dst.Columns[i].Header = p.headers[i]
		dst.Columns[i].Cells = cells
	}
	dst.RowCount = rows
	return nil
}

func resize[T any](s []T, n int) []T {
	if cap(s) < n {
		out := make([]T, n, growCap(cap(s), n))
		copy(out, s)
		return out
	}
	return s[:n]
}

func growCap(old, need int) int {
	n := old * 2
	if n < 8 {
		n = 8
	}
	if n < need {
		n = need
	}
	return n
}

// --- fan-out expansion ------------------------------------------------------

// Fan-out turns a selection of driving rows into a selection of (driving,
// joined) pairs, and then makes every column in the plan indexed by pair rather
// than by driving row. That second half is the point: once the columns are
// expanded, projection, ordering, grouping, aggregation, and result
// materialization index them by row exactly as they always have, and none of
// them has to know that two of the columns came from a different collection.
//
// The order is defined and structural: driving ordinal ascending, and within
// one driving row, joined-row address ascending. The first half is the
// selection the filter phase produced, which is ascending by construction —
// including across parallel workers, whose ranges are concatenated in range
// order. The second is the chain order joinBuild.index deliberately builds.
// Nothing here sorts to obtain either.

// expandPairs walks the selection and produces one pair per matching joined
// row, filling the pair's driving scan ordinal, driving address, and joined
// address.
//
// limit stops the walk once the result cannot grow: for a plan whose output
// order is the pair order, the pairs past the limit cannot reach the result, so
// producing them is work with no consumer. An ordered, grouped, or aggregated
// plan passes no limit, because it has to see every pair before it knows which
// ones survive.
func (ctx *execCtx) expandPairs(p *plan, j *planJoin, b *joinBinding, selected []int, addresses []store.Location, limit int, w *Workspace) error {
	outerScan, outerRows := w.pairOuterScan[:0], w.pairOuterRows[:0]
	inner, innerPair := w.pairInner[:0], w.pairInnerPair[:0]
	joinCol := j.outerPath
	mapInner := j.left && (len(j.innerCols) != 0 || len(j.innerNums) != 0)
	maxPairs := w.joinPairBudget.maxPairs()
	if limit > 0 && (maxPairs < 0 || limit < maxPairs) {
		maxPairs = limit
	}
	for i, row := range selected {
		if err := cancellationCheckpoint(w.cancel, i); err != nil {
			w.pairOuterScan, w.pairOuterRows = outerScan, outerRows
			w.pairInner, w.pairInnerPair = inner, innerPair
			return err
		}
		remaining := -1
		if maxPairs >= 0 {
			remaining = maxPairs - len(outerScan)
			if remaining <= 0 {
				if limit > 0 && len(outerScan) >= limit {
					break
				}
				w.pairOuterScan, w.pairOuterRows = outerScan, outerRows
				w.pairInner, w.pairInnerPair = inner, innerPair
				w.joinPairBudget.commitPairs(len(outerScan))
				return w.joinPairBudget.pairError(len(outerScan) + 1)
			}
		}
		var (
			found    int
			overflow bool
		)
		innerLimit := -1
		if remaining >= 0 {
			innerLimit = len(inner) + remaining
		}
		cell := ctx.values[joinCol][row]
		if strictSQLJoinOrigin(j.origin) {
			if err := b.sqlComparisonError(
				cell, j.slot, bareJoinOrigin(j.origin),
			); err != nil {
				w.pairOuterScan, w.pairOuterRows = outerScan, outerRows
				w.pairInner, w.pairInnerPair = inner, innerPair
				return err
			}
		}
		inner, found, overflow = b.build.appendMatches(
			inner, cell, innerLimit,
		)
		pairAt := len(outerScan)
		if mapInner {
			for range found {
				outerScan = append(outerScan, row)
				outerRows = append(outerRows, addresses[i])
				innerPair = append(innerPair, pairAt)
				pairAt++
			}
		} else {
			for range found {
				outerScan = append(outerScan, row)
				outerRows = append(outerRows, addresses[i])
			}
		}
		if found == 0 && j.left {
			outerScan = append(outerScan, row)
			outerRows = append(outerRows, addresses[i])
		}
		if overflow {
			if limit > 0 && len(outerScan) >= limit {
				outerScan, outerRows = outerScan[:limit], outerRows[:limit]
				break
			}
			w.pairOuterScan, w.pairOuterRows = outerScan, outerRows
			w.pairInner, w.pairInnerPair = inner, innerPair
			w.joinPairBudget.commitPairs(len(outerScan))
			return w.joinPairBudget.pairError(len(outerScan) + 1)
		}
		if limit > 0 && len(outerScan) >= limit {
			outerScan, outerRows = outerScan[:limit], outerRows[:limit]
			break
		}
	}
	w.pairOuterScan, w.pairOuterRows = outerScan, outerRows
	w.pairInner, w.pairInnerPair = inner, innerPair
	w.joinPairBudget.commitPairs(len(outerScan))
	return nil
}

// pairLimit is the number of pairs the expansion may stop at, or zero when it
// must produce all of them. It is truncateEarly's rule restated over pairs: a
// LIMIT decides the result only when nothing downstream can reorder or reduce
// what reaches it.
func (p *plan) pairLimit() int {
	if p.grouped || p.singleRow || len(p.order) != 0 || !p.hasLimit {
		return 0
	}
	return p.limit
}

// locateSelected resolves the selection's scan ordinals to stable addresses in
// the driving collection, which is what a gather at pair granularity needs.
func (ctx *execCtx) locateSelected(
	snapshot store.Snapshot,
	selected []int,
	compact bool,
	w *Workspace,
) ([]store.Location, error) {
	rows := w.lateStoreRows[:0]
	if compact {
		if w.cancel == nil {
			for _, row := range selected {
				rows = append(rows, w.storeRows[row])
			}
		} else {
			for i, row := range selected {
				if err := cancellationCheckpoint(w.cancel, i); err != nil {
					return rows, err
				}
				rows = append(rows, w.storeRows[row])
			}
		}
	} else {
		// An uncompacted scan numbered its rows by position in the snapshot's
		// live chunk/slot order, which is the order the live masks enumerate.
		w.liveMasks = snapshot.AppendLiveMasks(w.liveMasks[:0])
		if err := w.checkCanceled(); err != nil {
			return rows, err
		}
		var err error
		rows, err = appendMaskLocations(rows, w.liveMasks, selected, w.cancel)
		if err != nil {
			return rows, err
		}
	}
	w.lateStoreRows = rows
	return rows, cancellationError(w.cancel)
}

// materializeFanOut is materializeSnapshot for a plan whose join produces pairs.
//
// Every column ends up indexed by pair. The driving collection's output columns
// and its numeric columns are gathered directly at the pairs' driving
// addresses, which repeat — a gather already accepts repeats, so fan-out needs
// nothing new from the storage layer. The joined collection's columns are
// gathered at the pairs' joined addresses through the same routine, against its
// own snapshot. The driving collection's filter columns already exist at scan
// granularity and are spread onto the pair positions instead of re-read.
func (ctx *execCtx) materializeFanOut(p *plan, snapshot store.Snapshot, catalog store.DatabaseSnapshot, selected []int, compact bool, w *Workspace) ([]int, error) {
	j := &p.joins[p.fanOutJoin]
	b := &w.joins[j.slot]
	w.joinPairBudget.begin(p, j)
	if p.hasLimit && p.limit == 0 {
		w.pairOuterScan = w.pairOuterScan[:0]
		w.pairOuterRows = w.pairOuterRows[:0]
		w.pairInner = w.pairInner[:0]
		w.pairInnerPair = w.pairInnerPair[:0]
		ctx.rows = 0
		w.selected = w.selected[:0]
		return w.selected, nil
	}
	addresses, err := ctx.locateSelected(snapshot, selected, compact, w)
	if err != nil {
		return nil, err
	}
	if err := ctx.expandPairs(p, j, b, selected, addresses, p.pairLimit(), w); err != nil {
		return nil, err
	}

	if err := ctx.extractSnapshotValues(
		p, snapshot, p.lateCols, w.pairOuterRows, true, &w.lateText, w,
	); err != nil {
		return nil, err
	}
	if err := ctx.extractSnapshotNumCols(
		p, snapshot, p.lateNums, w.pairOuterRows, true, w,
	); err != nil {
		return nil, err
	}
	joined, ok := catalog.Collection(j.collection)
	if !ok {
		return nil, fmt.Errorf(
			"query: join: collection %q is not in the database snapshot", j.collection)
	}
	if err := ctx.extractSnapshotValues(
		p, joined, j.innerCols, w.pairInner, true, &w.lateText, w,
	); err != nil {
		return nil, err
	}
	if err := ctx.extractSnapshotNumCols(
		p, joined, j.innerNums, w.pairInner, true, w,
	); err != nil {
		return nil, err
	}
	if j.left {
		ctx.nullExtendJoined(p, j, w)
	}
	return ctx.spreadOnto(p, w.pairOuterScan, w), nil
}

// nullExtendJoined scatters the matched joined columns onto the full LEFT JOIN
// pair space. Positions without a build-side row remain the zero scalar, which
// is the executor's NULL representation.
func (ctx *execCtx) nullExtendJoined(p *plan, j *planJoin, w *Workspace) {
	pairs := len(w.pairOuterScan)
	w.pairValues = resize(w.pairValues, len(ctx.values))
	for _, col := range j.innerCols {
		src := ctx.values[col]
		dst := resize(w.pairValues[col], pairs)
		clear(dst)
		for match, pair := range w.pairInnerPair {
			dst[pair] = src[match]
		}
		ctx.values[col], w.pairValues[col] = dst, src
	}
	w.pairNums = resize(w.pairNums, len(ctx.nums))
	for _, col := range j.innerNums {
		src := ctx.nums[col].vals
		dst := resize(w.pairNums[col], pairs)
		clear(dst)
		for match, pair := range w.pairInnerPair {
			dst[pair] = src[match]
		}
		ctx.nums[col].vals, w.pairNums[col] = dst, src
	}
}

// spreadOnto rewrites the driving collection's filter columns so that they are
// indexed by pair, and renumbers the selection to the identity — the same
// contract compactOnto has, for a permutation that repeats rather than one that
// only ever moves a row down.
//
// It cannot work in place for that reason: a pair list is longer than the
// selection it came from, so writing pair i would overwrite a driving row some
// later pair still has to read. Each column is therefore spread into a second
// buffer and the two are swapped, which keeps both at their high-water marks
// and leaves the next execution's extraction refilling storage it already owns.
func (ctx *execCtx) spreadOnto(p *plan, pairs []int, w *Workspace) []int {
	w.pairValues = resize(w.pairValues, len(ctx.values))
	for _, c := range p.filterCols {
		src := ctx.values[c]
		dst := resize(w.pairValues[c], len(pairs))
		for i, row := range pairs {
			dst[i] = src[row]
		}
		ctx.values[c], w.pairValues[c] = dst, src
	}
	ctx.rows = len(pairs)
	selected := resize(w.selected[:0], len(pairs))
	for i := range selected {
		selected[i] = i
	}
	w.selected = selected
	return selected
}
