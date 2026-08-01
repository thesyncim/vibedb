package query

import (
	"bufio"
	"container/heap"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"math/bits"
	"os"
	"runtime"
	"slices"
	"strconv"
	"strings"

	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
	"github.com/thesyncim/vibejson/x/byteview"
)

// ExecOptions controls execution resource bounds. The zero value selects one
// worker per GOMAXPROCS, the default batch, memory, result, exact-aggregate,
// spill, and join budgets, so a caller who does not care never has to fill it
// in. MemoryBytes applies to every source: it is the durable batch/merge target
// and the admission limit for heap/Segment row-proportional work, decoded text,
// grouping, join memberships, and durable index planning. BatchRows,
// BatchBytes, SpillDirectory, and SpillBytes are durable-only.
//
// For durable sources MemoryBytes is deliberately a batch/merge target, not a
// total resident-capacity ceiling: fixed worker/ring minima and one document
// bounded by the collection's MaxDocumentBytes schema may exceed it. Optional
// index planning, join membership, and containment tapes are data-dependent
// additions and still use fail-closed admission. MemoryBytes is separate from
// the returned Result; ResultRows and ResultBytes independently bound
// materialized output for every source.
type ExecOptions struct {
	Workers     int
	BatchRows   int
	BatchBytes  int64
	MemoryBytes int64
	// Cancel is an optional reusable cooperative cancellation signal. A nil
	// flag is the default and adds no allocation or atomic operation to the
	// execution path. When non-nil, cancellation returns [ErrCanceled] only
	// after workers, reusable channels, spill files, and borrowed resources
	// have been cleaned up. Clear a canceled flag with [CancelFlag.Reset] only
	// after the execution has returned.
	Cancel *CancelFlag
	// ResultRows bounds rows materialized before HAVING/OFFSET cursor filtering.
	// ResultBytes bounds logical ResultColumn/Cell storage plus every cell's
	// retained variable-width representation, including bytes borrowed from a
	// heap Segment and bytes copied out of durable snapshots. A string charges
	// both its exact JSON spelling and decoded contents because Result retains
	// both. Zero selects the conservative defaults [DefaultResultRows] and
	// [DefaultResultBytes]; -1 explicitly disables that limit. Exceeding either
	// returns *ResultBudgetError before the rejected result storage is grown.
	ResultRows  int
	ResultBytes int64
	// AggregateBytes bounds the whole execution's retained exact-decimal
	// coefficient, exponent, extremum, and result-digit storage across every
	// group and worker partial. Zero selects a conservative 16 MiB default.
	// Exceeding it returns *AggregateBudgetError before an unbounded decimal
	// alignment or zero run can be allocated.
	AggregateBytes int64
	SpillDirectory string
	// SpillBytes bounds live temporary run files for durable ORDER BY and GROUP
	// BY execution. Zero selects 1 GiB; -1 explicitly disables the quota.
	// Writers enforce it before a file grows past the remaining allowance,
	// including the old and new runs that coexist during a merge.
	SpillBytes int64

	// JoinMembershipMax is how many inner join-key values a semi-join collects
	// before it abandons the membership strategy and drives the join as a
	// per-row keyed lookup instead. Zero selects the measured default; see
	// joinMembershipMax in join.go for how that number was arrived at.
	//
	// It is exposed because it is the one knob whose right value depends on the
	// caller's data rather than on this package's measurements, and because the
	// two strategies must be independently exercisable: forcing it to either
	// extreme makes the same query take the other branch, which is how the
	// differential proves the adaptive choice changes cost and never the answer.
	// It never changes which rows a query returns.
	JoinMembershipMax int

	// JoinPairBytes bounds the retained pair-address, gathered-column, and
	// decoded-text workspace of an inner join. Zero selects 64 MiB; -1
	// explicitly disables the bound. A multiplicative fan-out that exceeds it
	// returns *JoinPairBudgetError rather than allocating until OOM. This is
	// separate from ResultBytes because COUNT(*) may have one result cell while
	// still needing to inspect every joined pair.
	JoinPairBytes int64

	// JoinFilterScanRatio is how many inner rows a semi-join clause that has
	// outgrown its membership will scan, per outer row, to build the prefilter
	// that lets it skip probes. Zero selects the measured default
	// (joinBloomScanRatio); a negative value declines the filter outright and
	// probes every outer row.
	//
	// It bounds the work spent on a filter whose selectivity cannot be known
	// until it is used, and it is exposed for the same two reasons the
	// threshold above is: a caller whose inner rows are unusually cheap or
	// unusually expensive to scan knows something this package cannot, and both
	// arms have to be independently exercisable to be independently measured.
	// Like the threshold, it never changes which rows a query returns — the
	// filter's error is one-sided and the exact probe behind it is
	// authoritative.
	JoinFilterScanRatio int
}

// fileWorkspace owns the reusable storage one durable execution needs beyond
// the general [Workspace]: the persistent-index probe I/O workspace, the
// residual bitmap, the group frontier, and — since
// the batched executor stopped minting its scan storage per batch — the
// per-worker scan workspaces, the in-flight batch ring, and the merge
// frontier. It lives unexported inside [Exec] because it is neither
// configuration nor output — the previous separate handle in the options
// struct only made callers carry a second workspace whose lifetime already
// matched the Exec's.
//
// Everything here is high-water capacity, retained so a second execution of
// the same shape finds it warm. That is the whole point: the batched executor
// used to declare a Workspace, a batch pair of buffers, and an accumulator
// inside each batch, which meant no amount of Exec reuse could ever warm the
// scan — every BatchRows documents re-grew all of it from empty.
type fileWorkspace struct {
	index      durable.IndexSession
	overflow   []byte
	accs       []aggAcc
	fileGroups []fileGroup

	// workers is one scan Workspace per worker goroutine, indexed by worker
	// number. Indexing by worker rather than by batch is deliberate: nothing a
	// Workspace holds outlives the makeFilePartial call that filled it (every
	// value a partial keeps is detached first), so a worker can reuse its own
	// across consecutive batches and keep its column, text, selection, and
	// shape-cache storage warm for the whole execution.
	workers []Workspace
	// segments is one scan Segment per worker goroutine, indexed like workers
	// and reset rather than rebuilt between batches. It is a slice of pointers
	// because a Segment carries a mutex, so a slice of values could not be grown
	// by copy, and because a worker's segment must keep its arena identity across
	// a resize.
	//
	// This is the same argument as workers, and it is where the batched
	// executor's remaining cost lived: nothing the Segment holds outlives the
	// makeFilePartial call that filled it, because every value a partial keeps is
	// copied out of the batch by ownScalars first, so a worker may reuse its own
	// across consecutive batches. Minting one per batch instead meant every batch
	// re-grew a source arena from 8 KiB and an entry arena from 512 entries,
	// doubling and discarding all the way to the batch's size — 411 of the 430
	// allocations a warm execution made, and essentially all of its bytes.
	segments []*store.Segment
	// arenas is one set of arena generations per worker goroutine, indexed like
	// workers, for the row and group storage the consumer keeps past the batch
	// that produced it. See fileArenaSet.
	arenas []fileArenaSet
	// compact is the consumer's own pair of arenas, for the rows that survive an
	// unordered LIMIT's trim. It is the consumer's rather than a worker's
	// because only the owning goroutine may write a worker's storage, and it is
	// a pair because a trim reads the previous trim's output while writing the
	// next. Both are bounded by the limit.
	compact [2]fileArena
	// slots is the in-flight batch ring, indexed by batch sequence modulo its
	// length. See fileSlot for why the modulus is safe.
	slots []fileSlot
	// rows and groupSet are the ordered-projection and grouped merge frontiers,
	// groupIndex addresses the latter by key, and mergedGroups is the flattened
	// group set a spilled execution's merge produces.
	//
	// The frontier is a slice with a key-to-position index rather than a map of
	// pointers because a map of pointers costs one allocation per distinct group
	// per execution, and nothing here needs a group's address to be stable: a
	// slice that grows moves its elements, but an index into it does not change.
	rows         []fileRow
	groupIndex   map[string]int
	groupSet     []fileGroup
	mergedGroups []fileGroup
	rowRuns      []spillRun
	groupRuns    []spillRun
	spillFiles   map[string]struct{}

	// pool holds the parked scanner and worker goroutines, and the channels
	// between them. It is a pointer to a separate object because parked
	// goroutines have to outlive any one execution while holding nothing that
	// points back at this workspace; see filePool.
	pool *filePool
}

// release drops storage retained by durable index planning and batch scanning.
func (w *fileWorkspace) release() {
	if w == nil {
		return
	}
	// The parked goroutines are retired before the fields are dropped, because
	// dropping them is what makes the pool unreachable.
	w.stopPool()
	w.index.Release()
	w.overflow = nil
	w.accs = nil
	w.fileGroups = nil
	w.workers = nil
	w.segments = nil
	w.arenas = nil
	w.compact = [2]fileArena{}
	w.slots = nil
	w.rows = nil
	w.groupIndex = nil
	w.groupSet = nil
	w.mergedGroups = nil
	w.rowRuns = nil
	w.groupRuns = nil
	w.spillFiles = nil
}

// abort severs every logical value a canceled durable execution retained while
// preserving the top-level high-water buffers and parked worker pool for
// reuse. Spill files are removed by spillManager's defer before this runs.
func (w *fileWorkspace) abort() {
	if w == nil {
		return
	}
	resetAggs(w.accs)
	clear(w.fileGroups)
	w.fileGroups = w.fileGroups[:0]
	for i := range w.workers {
		w.workers[i].clearBorrowedViews()
	}
	for i := range w.slots {
		resetAggs(w.slots[i].accs)
		clear(w.slots[i].rows)
		w.slots[i].rows = w.slots[i].rows[:0]
		clear(w.slots[i].groups)
		w.slots[i].groups = w.slots[i].groups[:0]
		clear(w.slots[i].byKey)
		w.slots[i].partial = filePartial{}
		w.slots[i].ready = false
	}
	clear(w.rows)
	w.rows = w.rows[:0]
	clear(w.groupIndex)
	clear(w.groupSet)
	w.groupSet = w.groupSet[:0]
	clear(w.mergedGroups)
	w.mergedGroups = w.mergedGroups[:0]
	clear(w.rowRuns)
	w.rowRuns = w.rowRuns[:0]
	clear(w.groupRuns)
	w.groupRuns = w.groupRuns[:0]
}

// A fileSlot is the scratch one in-flight batch sequence owns: the scan
// buffers the scanner fills, the partial-result storage the worker fills, and
// the reorder buffer the consumer parks the finished partial in until its turn
// comes. Slots form a ring indexed by batch sequence modulo len(slots).
//
// The ring is what makes batch storage reusable without a pool or a free list,
// and its correctness rests entirely on the credit protocol. Write C for the
// credit channel's capacity and L = C+1 for the ring's length.
//
// The scanner sends one credit and then publishes one batch, with nothing
// between the two that can fail or return, so the number of credits held is
// exactly (batches published) - (batches whose credit the consumer has
// released). The consumer releases a batch's credit only after consuming its
// partial in sequence order. Therefore, at the moment the scanner takes the slot
// for sequence t, sequences 0..t-1 have been published and at most C of them are
// unconsumed — so every unconsumed sequence lies in [t-C, t-1]. Those, together
// with t itself, are C+1 = L consecutive sequences, which occupy L distinct
// slots modulo L. Sequence t-L = t-C-1 is outside the window and has therefore
// been scanned, reduced, and consumed: nothing still refers to the slot about to
// be rewound.
//
// The +1 is load-bearing and measured, not conservative:
// TestRunFileSnapshotBatchRingReuseDifferential fails at both C/2+1 and exactly
// C. The bound is stated in credits rather than in workers because the credit
// channel is what actually enforces it — the executor derives L from
// cap(pool.credits), so a pool whose channels were built for a different width
// cannot leave the two disagreeing.
//
// Reusing the pool across executions does not weaken any of this. The credit
// channel is empty at the start of every execution (see filePool for why),
// so the count of held credits starts at zero exactly as it did when the channel
// was minted per call.
//
// Only storage the consumer copies out of belongs here. A projected row's
// detached scalars and a group's accumulators do not: the consumer retains those
// until the execution materializes its Result, so they live in the per-worker
// fileArena, which is rewound at the start of an execution and retired by a
// spill — which is what lets a spill hand their bytes back.
type fileSlot struct {
	batch   fileBatch
	accs    []aggAcc
	rows    []fileRow
	groups  []fileGroup
	byKey   map[string]int
	partial filePartial
	ready   bool
}

// fileSlotCount is the batch ring length for a credit bound of credits. See
// fileSlot for the derivation of the +1.
func fileSlotCount(credits int) int { return credits + 1 }

// clearTail zeroes the elements of s past n, releasing whatever a shorter
// refill left reachable through retained capacity while keeping the storage.
func clearTail[T any](s []T, n int) {
	if n < len(s) {
		clear(s[n:])
	}
}

// ExecStats describes the physical work performed by the last execution into
// an [Exec]. The durable backend measures the scan and index fields; heap
// sources reset them. The Join fields are the exception: they are written by
// either coherent database source, because which strategy a join measured its
// way into is the one execution decision this package makes that a caller
// cannot predict from the plan.
// RowsTotal is the snapshot cardinality while
// RowsScanned is the number of JSON documents admitted to execution after
// persistent-index pushdown. IndexCertificateRows were decided from a
// collision-free posting representative or compact categorical cover without
// opening JSON;
// IndexRecheckRows required exact document comparison. An ordinary
// IndexBounded execution still evaluates the complete predicate. BufferedBytes
// is the largest observed batch or in-memory merge frontier; it excludes the
// caller-owned final Result. CoveringColumns counts distinct typed columns
// reduced without admitting JSON.
type ExecStats struct {
	Workers              int
	RowsTotal            uint64
	RowsScanned          uint64
	Batches              uint64
	PeakBatchRows        int
	PeakBatchBytes       int64
	BufferedBytes        int64
	SpillRuns            uint64
	SpilledBytes         int64
	IndexBounded         bool
	IndexLookups         int
	IndexPostingPages    int
	IndexCertificateRows uint64
	IndexRecheckRows     uint64
	CandidateRows        uint64
	CandidateChunks      int
	CoveringColumns      int

	// JoinMemberships counts the join clauses whose inner side fit under the
	// threshold and were pushed into the outer predicate as a membership;
	// JoinLookups counts those that overflowed it and are answered by a probe
	// per outer row. The two sum to the plan's join count.
	JoinMemberships int
	JoinLookups     int
	// JoinKeys is the total number of distinct inner join-key values collected
	// by the membership-bound clauses, after deduplication. JoinProbes is the
	// number of inner-collection lookups the lookup-bound clauses performed —
	// after any prefilter, so it is the probes that actually ran rather than
	// the outer rows that reached the join.
	JoinKeys   uint64
	JoinProbes uint64
	// JoinFilters counts the lookup-bound clauses that also built a semi-join
	// reduction filter, and JoinFilterKeys the inner keys those filters
	// summarize. JoinFilterRejected is how many outer rows the filters answered
	// on their own; it is the work the filters saved, and it is zero for a
	// filter that turned out to admit everything.
	JoinFilters        int
	JoinFilterKeys     uint64
	JoinFilterRejected uint64
	// JoinBuilds counts the clauses that fanned out, and JoinBuildRows the
	// joined rows those builds materialized. JoinPairs is how many (driving,
	// joined) pairs the expansion produced, which is the result's row count
	// before grouping, ordering, and limiting. A query with no fan-out reports
	// zero for all three, which is how a caller tells the two operators apart
	// without inspecting the plan.
	JoinBuilds    int
	JoinBuildRows uint64
	JoinPairs     uint64
}

const (
	defaultFileMemory = int64(64 << 20)
	minimumFileMemory = int64(64 << 10)
	defaultSpillBytes = int64(1 << 30)
	defaultBatchRows  = 4096
	maxSpillFanIn     = 32
)

var errFileExecutionStopped = errors.New("query: file execution stopped")

// errFileExecutionUnbalanced reports that an execution left one of the pool's
// reused channels non-empty. It is a bug in this package rather than anything a
// caller did, and it is surfaced as an error rather than a panic because the
// pool has already been drained by the time it is raised: the execution that
// detected it is the one with the doubtful result, and the next one is clean.
var errFileExecutionUnbalanced = errors.New(
	"query: file execution left its batch pipeline unbalanced")

type normalizedFileOptions struct {
	workers     int
	batchRows   int
	batchBytes  int64
	memoryBytes int64
	mergeBytes  int64
	spillBytes  int64
	spillDir    string
}

func normalizeFileOptions(opts ExecOptions) (normalizedFileOptions, error) {
	workers := opts.Workers
	if workers == 0 {
		workers = runtime.GOMAXPROCS(0)
	}
	if workers < 1 {
		return normalizedFileOptions{}, fmt.Errorf("query: Workers must be positive")
	}
	memoryBytes := opts.MemoryBytes
	if memoryBytes == 0 {
		memoryBytes = defaultFileMemory
	}
	if memoryBytes < minimumFileMemory {
		return normalizedFileOptions{}, fmt.Errorf("query: MemoryBytes must be at least 64 KiB")
	}
	batchRows := opts.BatchRows
	if batchRows == 0 {
		batchRows = defaultBatchRows
	}
	if batchRows < 1 {
		return normalizedFileOptions{}, fmt.Errorf("query: BatchRows must be positive")
	}
	batchBytes := opts.BatchBytes
	if batchBytes == 0 {
		// At most two batches per worker are admitted by the job queue. Leave
		// half the target for indexes, worker columns, and the merge frontier.
		batchBytes = memoryBytes / int64(workers*4)
		if batchBytes < 16<<10 {
			batchBytes = 16 << 10
		}
	}
	if batchBytes < 1 {
		return normalizedFileOptions{}, fmt.Errorf("query: BatchBytes must be positive")
	}
	spillBytes := opts.SpillBytes
	switch {
	case spillBytes < -1:
		return normalizedFileOptions{}, fmt.Errorf(
			"query: SpillBytes must be -1, zero, or positive",
		)
	case spillBytes == 0:
		spillBytes = defaultSpillBytes
	}
	return normalizedFileOptions{
		workers: workers, batchRows: batchRows, batchBytes: batchBytes,
		memoryBytes: memoryBytes, mergeBytes: memoryBytes / 2,
		spillBytes: spillBytes, spillDir: opts.SpillDirectory,
	}, nil
}

// runFileInto executes p over a page-backed snapshot in parallel batches under
// e.Options and into e's Result, Workspace, and durable planning storage.
// catalog is the database cut the driving snapshot came from, empty when the
// Source named a single file; a join clause resolves its inner collection out
// of it. Ordered projections and grouped reductions spill sorted runs once
// their merge frontier reaches MemoryBytes after any admitted index-planning
// capacity is debited; spill merges open at most 32 files at a time. Repeated
// calls reuse the Result's column cells and packed variable-width value arena,
// so once their observed high-water marks fit, result materialization allocates
// nothing. Materialized cells own their bytes and stay valid after the snapshot
// is closed, until e is reused or released. The snapshot stays owned by the
// caller.
func (p *plan) runFileInto(e *Exec, snapshot *durable.Snapshot, catalog durable.DatabaseSnapshot) error {
	e.Result.fileData = e.Result.fileData[:0]
	e.Stats = ExecStats{}
	if err := e.Workspace.checkCanceled(); err != nil {
		return err
	}
	n, err := normalizeFileOptions(e.Options)
	if err != nil {
		return err
	}
	if snapshot == nil {
		return fmt.Errorf("query: FromFile was given a nil snapshot")
	}
	stats := ExecStats{Workers: n.workers, RowsTotal: snapshot.Len()}
	if len(p.joins) != 0 {
		// The direct dispatchers below answer straight out of the persistent
		// index or a covering projection, without ever evaluating the compiled
		// predicate row by row. A join leaf is only evaluated there, so a
		// covered count or aggregate would silently answer the query with the
		// join clause dropped. Skipping them is a cost decision reversed, not a
		// correctness workaround: they are unsafe here, not merely unprofitable.
		return p.runFileJoinedBatched(e, snapshot, catalog, n, stats)
	}
	// At most half of work memory is offered to an index plan. The admitted
	// worst-case capacity is debited from the batched executor if the direct
	// lane declines, while the other half guarantees a scan fallback remains
	// available without turning an optional index into a resource error.
	plannerMemory := n.memoryBytes / 2
	directIndex, handled, directErr := p.runDirectFileIndexedCount(
		snapshot, e, plannerMemory,
	)
	if handled {
		stats.IndexBounded = directIndex.bounded
		stats.IndexLookups = directIndex.lookups
		stats.IndexPostingPages = directIndex.postingPages
		stats.IndexCertificateRows = directIndex.certificates
		stats.IndexRecheckRows = directIndex.rechecks
		stats.CandidateRows = directIndex.rows
		stats.CandidateChunks = directIndex.chunks
		e.Stats = stats
		if directErr == nil {
			directErr = e.Workspace.checkCanceled()
		}
		return directErr
	}
	coveringColumns, handled, directErr := p.runDirectFileAggregate(snapshot, e)
	if handled {
		stats.CoveringColumns = coveringColumns
		e.Stats = stats
		if directErr == nil {
			directErr = e.Workspace.checkCanceled()
		}
		return directErr
	}
	result, stats, err := p.runFileSnapshotBatched(e, snapshot, nil, n, stats)
	e.Result, e.Stats = result, stats
	return err
}

// runFileOverlayInto executes the exact merged view of one durable snapshot
// and a bounded staged-write overlay. It is intentionally a separate dispatch
// from runFileInto: ordinary and read-only transaction queries retain all
// persistent index, covering aggregate, zone-pruning, and direct-count paths
// without even consulting an overlay.
func (p *plan) runFileOverlayInto(e *Exec, snapshot *durable.Snapshot, overlay FileOverlay) error {
	e.Result.fileData = e.Result.fileData[:0]
	e.Stats = ExecStats{}
	if err := e.Workspace.checkCanceled(); err != nil {
		return err
	}
	n, err := normalizeFileOptions(e.Options)
	if err != nil {
		return err
	}
	if snapshot == nil {
		return fmt.Errorf("query: FromFileOverlay was given a nil snapshot")
	}
	if overlay == nil {
		return fmt.Errorf("query: FromFileOverlay was given a nil overlay")
	}
	if len(p.joins) != 0 {
		return fmt.Errorf("query: FromFileOverlay does not support joins")
	}
	rows := int64(snapshot.Len()) + overlay.LenDelta()
	if rows < 0 {
		return fmt.Errorf("query: FileOverlay LenDelta underflows the base snapshot")
	}
	stats := ExecStats{Workers: n.workers, RowsTotal: uint64(rows)}
	result, stats, err := p.runFileSnapshotBatched(e, snapshot, overlay, n, stats)
	e.Result, e.Stats = result, stats
	return err
}

// runFileSnapshotBatched is kept outside the direct covering dispatcher so
// goroutine captures in the general executor cannot force the fast path's
// stats and fallback workspace onto the heap.
func (p *plan) runFileSnapshotBatched(
	e *Exec,
	snapshot *durable.Snapshot,
	overlay FileOverlay,
	n normalizedFileOptions,
	base ExecStats,
) (result Result, stats ExecStats, err error) {
	// The result travels as a local value so the merge and materialization
	// tails below stay one expression each; the caller stores it back into the
	// Exec. Taking it from e keeps its retained cell and arena capacity.
	result, stats = e.Result, base
	if err := e.Workspace.checkCanceled(); err != nil {
		return result, stats, err
	}
	work := &e.Workspace.heapWorkBudget
	if !work.active {
		work.begin(n.memoryBytes)
	}
	candidateMasks, plannerBytes, err := p.fileCandidateMasksBounded(
		snapshot, &e.file.index, &e.Workspace, work.remaining()/2,
	)
	if err != nil {
		return result, stats, err
	}
	if err := e.Workspace.checkCanceled(); err != nil {
		return result, stats, err
	}
	if plannerBytes != 0 {
		if err := work.admit(
			"durable candidate-planner workspace",
			plannerBytes,
		); err != nil {
			return result, stats, err
		}
	}
	n.memoryBytes = work.remaining()
	// A fileArena's controlled growth retains earlier generations while the
	// merge frontier still references them. Its cumulative capacity is less
	// than four times the logical bytes the frontier reports, so spilling at a
	// quarter of the unreserved account leaves room for that real backing
	// storage rather than treating logical row bytes as allocated capacity.
	n.mergeBytes = max(int64(1), n.memoryBytes/4)
	stats.IndexLookups = e.Workspace.storeIndexProbes
	stats.IndexBounded = candidateMasks != nil
	if stats.IndexBounded {
		for _, mask := range candidateMasks {
			rows := bits.OnesCount64(mask.Bits)
			if rows == 0 {
				continue
			}
			stats.CandidateRows += uint64(rows)
			stats.CandidateChunks++
		}
	}
	spills := newSpillManager(
		n.spillDir, e.file.spillFiles, p.columns, len(p.groupCols),
		&e.Workspace.aggregateBudget, n.spillBytes, e.Options.Cancel,
	)
	// The spill tallies are folded into the named result here, because the
	// function returns from a dozen places and every one of them has to report
	// what was written to disk — including the error paths, whose runs still
	// have to be accounted for and removed.
	defer func() {
		stats.SpillRuns, stats.SpilledBytes = spills.runs, spills.size
		var cleanupErr error
		e.file.spillFiles, cleanupErr = spills.cleanup()
		if cleanupErr != nil {
			err = errors.Join(err, cleanupErr)
		}
	}()

	pool := e.file.poolFor(n.workers)
	// The ring is sized from the credit channel's capacity rather than from the
	// worker count, because the capacity is what the soundness argument is
	// actually about and the two are only equal by construction. See fileSlot.
	slots := resize(e.file.slots, fileSlotCount(cap(pool.credits)))
	e.file.slots = slots
	e.file.workers = resize(e.file.workers, n.workers)
	e.file.segments = resize(e.file.segments, n.workers)
	e.file.arenas = resize(e.file.arenas, n.workers)
	// The previous execution's group keys are views into the arenas, and the
	// index and frontier holding them are dropped here — before the arenas are
	// rewound and, more importantly, before any worker is woken to overwrite the
	// bytes those keys address. Doing it after would leave a live map whose keys
	// mutate underneath it: harmless today because clear neither hashes nor
	// compares, and a silent corruption the moment anything else touches the map
	// in that window.
	if e.file.groupIndex == nil {
		e.file.groupIndex = make(map[string]int)
	}
	byKey := e.file.groupIndex
	clear(byKey)
	clear(e.file.groupSet)
	groups := e.file.groupSet[:0]
	// The arenas are rewound here, on the driver, before any worker is woken.
	// This is the point at which nothing can still be reading them: the previous
	// execution's Result copied every cell it kept into its own storage, and the
	// merge frontier it was built from has just been cleared.
	for i := range e.file.arenas {
		e.file.arenas[i].reset()
	}
	for i := range e.file.compact {
		e.file.compact[i].rewind()
	}
	// Each worker's evaluator is pointed at the one set of bindings the binder
	// produced, on this goroutine, before any worker is woken. That is the same
	// read-only/per-worker split bindScanWorkers performs for the heap filter
	// phase: the collected set and the Bloom filter are immutable after bind and
	// are shared, and everything a probe writes — its copy-out buffer, its
	// one-row columns, its tallies, its parked I/O error — lands in the
	// per-worker scratch installed here.
	//
	// A plan with no joins passes nil rather than the Workspace's retained
	// bindings. That is not tidiness: the pool parks its workers and their
	// Workspaces across executions by design, so an Exec reused for an unjoined
	// query after a joined one would otherwise leave every parked evaluator
	// still aliasing the previous execution's collected sets and Bloom filters,
	// pinning the inner collection they were read from for the Exec's lifetime.
	var binds []joinBinding
	if len(p.joins) != 0 {
		binds = e.Workspace.joins
	}
	for worker := range e.file.workers {
		e.file.workers[worker].heapWorkParent = work
		e.file.workers[worker].heapWorkTextReserved = 0
		e.file.workers[worker].cancel = e.Options.Cancel
		e.file.workers[worker].eval.setWork(work)
		e.file.workers[worker].eval.bindTo(binds)
	}
	pool.start(fileJob{
		p: p, snapshot: snapshot, overlay: overlay, masks: candidateMasks,
		overflow: &e.file.overflow, slots: slots,
		spaces: e.file.workers, segments: e.file.segments,
		arenas: e.file.arenas, opts: n, active: n.workers,
		budget: &e.Workspace.aggregateBudget, cancel: e.Options.Cancel,
	})
	cancel := func() { pool.stopped.Store(true) }

	var firstErr error
	rows := e.file.rows[:0]
	var rowBytes int64
	var resultRows int
	var resultPayloadBytes int64
	rowRuns := e.file.rowRuns[:0]
	var accs []aggAcc
	// compactAt alternates the consumer's own row arena; see the unordered
	// LIMIT trim below.
	compactAt := 0
	var groupBytes int64
	groupRuns := e.file.groupRuns[:0]

	// nextSequence is the batch the consumer is working on, and it is declared
	// out here because consume publishes it: every sequence up to and including
	// it has been consumed, so when consume also drops their rows, that is the
	// frontier the workers rewind their arenas against.
	nextSequence := uint64(0)
	consume := func(part filePartial) {
		if part.err != nil {
			if firstErr == nil {
				firstErr = part.err
			}
			cancel()
			return
		}
		if firstErr != nil {
			return
		}
		if cancelErr := cancellationError(e.Options.Cancel); cancelErr != nil {
			firstErr = cancelErr
			cancel()
			return
		}
		switch {
		case p.grouped:
			for i := range part.groups {
				if cancelErr := cancellationCheckpoint(e.Options.Cancel, i); cancelErr != nil {
					firstErr = cancelErr
					cancel()
					return
				}
				g := &part.groups[i]
				if id, ok := byKey[g.key]; ok {
					dst := &groups[id]
					if err := mergeAggs(
						dst.accs, g.accs, p.columns, &e.Workspace.aggregateBudget,
					); err != nil {
						firstErr = err
						cancel()
						return
					}
					if g.first < dst.first {
						dst.first = g.first
					}
					continue
				}
				byKey[g.key] = len(groups)
				groups = append(groups, *g)
				groupBytes += g.bytes
			}
			if groupBytes >= n.mergeBytes && len(groups) != 0 {
				run, spillErr := spills.writeGroups(groups)
				if spillErr != nil {
					firstErr = spillErr
					cancel()
				} else {
					groupRuns = append(groupRuns, run)
					// Clearing rather than replacing the index and the
					// frontier keeps their capacity for the next fill while
					// still dropping every reference to the spilled groups —
					// handing that memory back is the whole point of the
					// spill, and the arena retirement below is the other half
					// of it. The index has to go with them: writeGroups sorted
					// the frontier in place, so the positions it holds are no
					// longer the groups they named.
					clear(byKey)
					clear(groups)
					groups = groups[:0]
					groupBytes = 0
					pool.dropped.Store(nextSequence + 1)
				}
			}
		case p.singleRow:
			if cancelErr := cancellationError(e.Options.Cancel); cancelErr != nil {
				firstErr = cancelErr
				cancel()
				return
			}
			if accs == nil {
				accs = resize(e.file.accs, len(p.columns))
				resetAggs(accs)
				e.file.accs = accs
			}
			if err := mergeAggs(
				accs, part.accs, p.columns, &e.Workspace.aggregateBudget,
			); err != nil {
				firstErr = err
				cancel()
				return
			}
		default:
			if len(p.order) == 0 {
				admit := len(part.rows)
				if p.hasLimit {
					admit = min(admit, max(p.limit-resultRows, 0))
				}
				for i := 0; i < admit; i++ {
					if cancelErr := cancellationCheckpoint(e.Options.Cancel, i); cancelErr != nil {
						firstErr = cancelErr
						cancel()
						return
					}
					payload, ok := fileResultPayloadBytes(part.rows[i])
					if !ok || resultPayloadBytes > int64(^uint64(0)>>1)-payload {
						firstErr = e.Result.resultByteBudgetError(
							resultRows+1, int64(^uint64(0)>>1),
						)
						cancel()
						return
					}
					resultPayloadBytes += payload
				}
				resultRows += admit
				if _, err := e.Result.checkResultBudget(
					len(p.columns), resultRows, resultPayloadBytes,
				); err != nil {
					firstErr = err
					cancel()
					return
				}
			}
			rows = append(rows, part.rows...)
			rowBytes += part.bytes
			// An unordered LIMIT needs only the earliest source ordinals.
			if len(p.order) == 0 && p.hasLimit &&
				len(rows) > unorderedTrimThreshold(p.limit) {
				slices.SortFunc(rows, compareFileOrdinal)
				rows = rows[:p.limit]
				// The rows just dropped are the majority, and their bytes live
				// in the workers' arenas, which only their owning goroutine may
				// touch. Re-detaching the few survivors into the consumer's own
				// arena is what makes those generations abandonable — without
				// it, a LIMIT over a corpus with many matches would accumulate
				// every selected row it ever discarded, which is exactly the
				// unbounded frontier the trim exists to prevent.
				//
				// The consumer's arena is double-buffered because the survivors
				// are read out of the buffer the previous trim packed them into
				// while the current trim is writing the next one. Both are
				// bounded by the limit, so neither grows with the corpus.
				survivors := &e.file.compact[compactAt]
				compactAt ^= 1
				survivors.rewind()
				for i := range rows {
					if cancelErr := cancellationCheckpoint(e.Options.Cancel, i); cancelErr != nil {
						firstErr = cancelErr
						cancel()
						return
					}
					repacked, repackErr := repackRow(survivors, rows[i])
					if repackErr != nil {
						firstErr = repackErr
						cancel()
						return
					}
					rows[i] = repacked
				}
				pool.dropped.Store(nextSequence + 1)
				rowBytes = estimateRows(rows)
			}
			if len(p.order) != 0 && rowBytes >= n.mergeBytes && len(rows) != 0 {
				run, spillErr := spills.writeRows(p, rows)
				if spillErr != nil {
					firstErr = spillErr
					cancel()
				} else {
					rowRuns = append(rowRuns, run)
					// The spilled rows own the bytes they detached, so
					// truncating alone would leave the retained header
					// array pinning every one of them and the frontier
					// would never actually shrink. Clearing first drops
					// the references and keeps only the capacity, and
					// retiring the arenas is the other half: the bytes
					// themselves live in the workers' storage, and only
					// their owning goroutine may abandon it.
					clear(rows)
					rows = rows[:0]
					rowBytes = 0
					pool.dropped.Store(nextSequence + 1)
				}
			}
		}
		if part.bytes > stats.BufferedBytes {
			stats.BufferedBytes = part.bytes
		}
		if rowBytes+groupBytes > stats.BufferedBytes {
			stats.BufferedBytes = rowBytes + groupBytes
		}
	}
	// Partials arrive in completion order and must be consumed in sequence
	// order. The reorder buffer is the batch ring rather than a map keyed by
	// sequence: the credit protocol already bounds the number of in-flight
	// sequences to the ring's length, so a slot is always free when its
	// sequence arrives, and a fixed ring costs no hashing and no per-execution
	// map.
	//
	// The loop ends when the scanner has reported how many batches it published
	// and every one of them has been consumed. That is the same termination
	// condition closing the partials channel used to express, restated as a
	// count so the channel can outlive the execution — and it is what makes the
	// balance argument on filePool a fact about this loop rather than a hope:
	// every credit taken is released here, exactly once, before the loop can
	// decide it is finished. scanDone is read through a local the receive nils
	// out, so the arm stops being selectable once it has fired.
	var scan fileScanResult
	scanDone := pool.scanDone
	for scanDone != nil || nextSequence < scan.batches {
		select {
		case part := <-pool.partials:
			slots[part.seq%uint64(len(slots))].partial = part
			slots[part.seq%uint64(len(slots))].ready = true
			for {
				slot := &slots[nextSequence%uint64(len(slots))]
				if !slot.ready {
					break
				}
				slot.ready = false
				part := slot.partial
				slot.partial = filePartial{}
				consume(part)
				<-pool.credits
				nextSequence++
			}
		case done := <-scanDone:
			scan, scanDone = done, nil
		}
	}
	if !pool.finish() && firstErr == nil {
		firstErr = errFileExecutionUnbalanced
	}
	// The locals above grew by append; handing their backing arrays back to
	// the Exec is what makes the next execution of the same shape find them
	// warm. The group frontier goes back here rather than in the grouped arm
	// below, because the error return between the two would otherwise leave
	// e.file.groupSet at a previous execution's length with this one's entries
	// past it — reachable through the retained capacity, never cleared by the
	// next execution's clear, and each pinning the arena storage of a group it
	// names, for the Exec's whole lifetime.
	e.file.rows, e.file.rowRuns, e.file.groupRuns = rows, rowRuns, groupRuns
	e.file.groupSet = groups
	stats.RowsScanned = scan.rows
	stats.Batches = scan.batches
	stats.PeakBatchRows = scan.peakRows
	stats.PeakBatchBytes = scan.peakBytes
	firstErr = fileExecutionError(firstErr, scan.err)
	if firstErr == nil {
		firstErr = cancellationError(e.Options.Cancel)
	}
	if firstErr != nil {
		clear(rows)
		e.file.rows = rows[:0]
		clear(groups)
		e.file.groupSet = groups[:0]
		return result, stats, firstErr
	}

	switch {
	case p.grouped:
		if len(groupRuns) != 0 {
			if len(groups) != 0 {
				run, spillErr := spills.writeGroups(groups)
				if spillErr != nil {
					return result, stats, spillErr
				}
				groupRuns = append(groupRuns, run)
			}
			groupRuns, err = spills.reduceGroupRuns(groupRuns)
			e.file.groupRuns = groupRuns
			if err != nil {
				return result, stats, err
			}
			previousMerged := e.file.mergedGroups
			var merged []fileGroup
			merged, err = spills.readMergedGroups(
				p, &result, previousMerged[:0], groupRuns, n.memoryBytes,
			)
			clearTail(previousMerged, len(merged))
			e.file.mergedGroups = merged
			if err != nil {
				clear(merged)
				e.file.mergedGroups = merged[:0]
				return result, stats, err
			}
			result, err = p.fileGroupResultInto(result, merged, &e.Workspace)
			return result, stats, err
		}
		// Unspilled, the frontier is already the group set, so materialization
		// sorts it where it stands rather than through a copy.
		result, err = p.fileGroupResultInto(result, groups, &e.Workspace)
		return result, stats, err
	case p.singleRow:
		if accs == nil {
			accs = resize(e.file.accs, len(p.columns))
			resetAggs(accs)
			e.file.accs = accs
		}
		resultRows := 1
		if p.hasLimit && p.limit == 0 {
			resultRows = 0
		}
		if err := prepareResult(&result, p, resultRows); err != nil {
			return result, stats, err
		}
		if resultRows != 0 {
			if err := p.fillAggregateCells(&result, 0, accs, nil, &e.Workspace); err != nil {
				return result, stats, err
			}
		}
		return result, stats, nil
	default:
		if len(rowRuns) != 0 {
			if len(rows) != 0 {
				run, spillErr := spills.writeRows(p, rows)
				if spillErr != nil {
					return result, stats, spillErr
				}
				rowRuns = append(rowRuns, run)
				clear(rows)
				rows = rows[:0]
			}
			rowRuns, err = spills.reduceRowRuns(p, rowRuns)
			e.file.rowRuns = rowRuns
			if err != nil {
				return result, stats, err
			}
			rows, err = spills.readMergedRows(
				p, &result, rowRuns, p.resultLimit(), rows[:0],
			)
			if err != nil {
				clear(rows)
				e.file.rows = rows[:0]
				return result, stats, err
			}
		} else if len(p.order) != 0 {
			slices.SortStableFunc(rows, p.compareFileRows)
		} else {
			slices.SortFunc(rows, compareFileOrdinal)
		}
		if limit := p.resultLimit(); limit >= 0 && len(rows) > limit {
			// Clearing before truncating, not after: the rows past the limit
			// are dropped here and the clear below only reaches the ones that
			// survive, so without this each of them would stay reachable
			// through the retained capacity and pin the arena generation its
			// scalars were packed into.
			clear(rows[limit:])
			rows = rows[:limit]
		}
		result, err = p.fileRowResultInto(result, rows, e.Options.Cancel)
		if err != nil {
			clear(rows)
			e.file.rows = rows[:0]
			return result, stats, err
		}
		// Every cell now owns its bytes inside the Result's arena, so the
		// frontier's detached scalars have no reader left. Keeping the header
		// array but clearing it retains the capacity that makes the next
		// execution allocation-free without pinning a whole result's worth of
		// copied document bytes between calls.
		clear(rows)
		e.file.rows = rows[:0]
		return result, stats, nil
	}
}

// fileExecutionError combines the consumer/worker outcome with the scanner's
// terminal error. A real storage or overlay failure wins over cancellation:
// the scanner may have observed it first but still be delayed publishing
// scanDone while a queued partial notices the flag. Stopped is only the
// pipeline's internal response to an error already held by the consumer.
func fileExecutionError(first, scan error) error {
	if scan == nil || errors.Is(scan, errFileExecutionStopped) {
		return first
	}
	if first == nil ||
		errors.Is(first, ErrCanceled) && !errors.Is(scan, ErrCanceled) {
		return scan
	}
	return first
}

type fileBatch struct {
	seq   uint64
	base  uint64
	data  []byte
	ends  []int
	bytes int64
}

// takeFileBatch rewinds the ring slot sequence seq owns and returns it ready to
// fill. The buffers survive from the last time this slot was used, which is why
// a warm execution scans without allocating: the document bytes of a batch are
// appended into a buffer that already reached the corpus's batch high-water
// mark, instead of one that starts at 64 KiB and doubles its way there once per
// batch, discarding every intermediate.
//
// Any buffer bought here goes into the slot before the batch is returned, not
// only when the batch is later flushed. A scan always takes one batch it never
// fills — the one after the last flush — and on a corpus whose batch count is
// below the ring length that batch's slot is reached by nothing else, so leaving
// the write-back to flush alone dropped a fresh 64 KiB buffer on the floor once
// per execution, forever. Storing at take time costs one slice-header write and
// makes the trailing batch as warm as every other.
func takeFileBatch(slots []fileSlot, seq, base uint64) fileBatch {
	slot := &slots[seq%uint64(len(slots))]
	b := slot.batch
	b.seq, b.base, b.bytes = seq, base, 0
	b.data, b.ends = b.data[:0], b.ends[:0]
	slot.batch = b
	return b
}

type fileScanResult struct {
	err       error
	rows      uint64
	batches   uint64
	peakRows  int
	peakBytes int64
}

type filePartial struct {
	seq    uint64
	rows   []fileRow
	groups []fileGroup
	accs   []aggAcc
	bytes  int64
	err    error
}

type fileRow struct {
	values  []scalar
	order   []scalar
	ordinal uint64
}

type fileGroup struct {
	key     string
	scalars []scalar
	accs    []aggAcc
	first   uint64
	bytes   int64
}

// makeFilePartial reduces one batch of raw documents to a partial result. w is
// the calling worker's retained scan workspace, docs its retained scan Segment,
// and slots the in-flight batch ring; everything the partial hands to the
// consumer either comes from the ring slot this batch owns or is freshly
// allocated because the consumer keeps it past the slot's next reuse.
//
// Resetting docs rather than replacing it is safe for the same reason reusing w
// is: the partial this call returns holds nothing that points into the Segment.
// Projected and grouped values are copied into their own arena by ownScalars, a
// group key is cloned into a string, and an aggregate is a number, so by the
// time the worker takes its next batch the consumer's view of this one is
// already detached. See Segment.Reset for the contract that makes the
// distinction load-bearing: after the reset a retained Index would report the
// next batch's bytes rather than fault.
func (p *plan) makeFilePartial(
	batch fileBatch,
	w *Workspace,
	docs *store.Segment,
	slots []fileSlot,
	arena *fileArena,
	budget *aggregateBudget,
) filePartial {
	part := filePartial{seq: batch.seq}
	if err := w.checkCanceled(); err != nil {
		part.err = err
		return part
	}
	slot := &slots[batch.seq%uint64(len(slots))]
	// The batch Segment carries no postings, and that is a decision, not an
	// omission. Postings are an inverted index over a Segment's top-level keys
	// and scalars; building one costs a hash and a bucket append per member of
	// every document, and this Segment is probed exactly once and then dropped,
	// so the build can never amortize. The filtered-count benchmark covers the
	// most favorable pruning case and still makes the temporary index slower
	// and allocation-heavy; exact results live in docs/performance.md. Real
	// pushdown happens before the scan, against the snapshot's persistent
	// indexes (fileCandidateMasks); once a batch is in hand the compiled
	// predicate's single pass over at most BatchRows rows is already cheaper
	// than indexing them. Candidate selection and the full scan return the same
	// rows by construction, so this changes cost only.
	docs.Reset()
	start := 0
	for row, end := range batch.ends {
		if err := cancellationCheckpoint(w.cancel, row); err != nil {
			part.err = err
			return part
		}
		document := batch.data[start:end]
		if _, err := docs.Append(document); err != nil {
			part.err = err
			return part
		}
		start = end
	}
	w.text = w.text[:0]
	w.groupKey = w.groupKey[:0]
	ctx := &w.ctx
	ctx.s, ctx.rows = docs, docs.Len()
	if err := ctx.extract(p, nil, w); err != nil {
		part.err = err
		return part
	}
	if err := w.checkCanceled(); err != nil {
		part.err = err
		return part
	}
	selected, err := p.selectRows(ctx, nil, false, w)
	if err != nil {
		part.err = err
		return part
	}
	if err := w.eval.firstError(); err != nil {
		part.err = err
		return part
	}
	switch {
	case p.grouped:
		if slot.byKey == nil {
			slot.byKey = make(map[string]int)
		}
		byKey := slot.byKey
		clear(byKey)
		prev := slot.groups
		groups := prev[:0]
		for at, row := range selected {
			if err := cancellationCheckpoint(w.cancel, at); err != nil {
				part.err = err
				return part
			}
			// The grown key buffer goes back into the Workspace. Building the
			// key into w.groupKey[:0] and dropping the result meant every row
			// re-grew the encoding from zero capacity — one to two allocations
			// per grouped row, which no amount of Exec reuse could warm away
			// because nothing retained what the growth bought.
			w.groupKey = p.groupKey(w.groupKey[:0], ctx, row)
			id, ok := byKey[string(w.groupKey)]
			if !ok {
				id = len(groups)
				// One copy serves both the lookup key and the group's own
				// key. Copying twice stored the same bytes twice for every
				// distinct group in every batch.
				key, arenaErr := arena.takeString(w.groupKey)
				if arenaErr != nil {
					part.err = arenaErr
					return part
				}
				byKey[key] = id
				g := fileGroup{key: key, first: batch.base + uint64(row)}
				g.scalars, g.bytes, arenaErr = ownScalars(
					arena, ctx, row, nil, nil, p.groupCols,
				)
				if arenaErr != nil {
					part.err = arenaErr
					return part
				}
				g.accs, arenaErr = arena.takeAccs(len(p.columns))
				if arenaErr != nil {
					part.err = arenaErr
					return part
				}
				g.bytes += int64(len(g.key)) + int64(len(g.accs))*aggAccStructBytes + 64
				groups = append(groups, g)
				part.bytes += g.bytes
			}
			if err := p.accumulate(groups[id].accs, ctx, row, budget); err != nil {
				part.err = err
				return part
			}
		}
		// Entries past this batch's group count still hold the previous
		// batch's key, detached scalars, and accumulators. Truncating alone
		// leaves them reachable through the retained capacity, so every ring
		// slot would pin one stale group set for the Exec's whole lifetime;
		// clearing releases them and keeps the storage.
		clearTail(prev, len(groups))
		slot.groups, part.groups = groups, groups
	case p.singleRow:
		accs := resize(slot.accs, len(p.columns))
		resetAggs(accs)
		for at, row := range selected {
			if err := cancellationCheckpoint(w.cancel, at); err != nil {
				part.err = err
				return part
			}
			if err := p.accumulate(accs, ctx, row, budget); err != nil {
				part.err = err
				return part
			}
		}
		slot.accs, part.accs = accs, accs
		part.bytes = int64(len(accs)) * aggAccStructBytes
	default:
		if len(p.order) != 0 {
			if err := w.checkCanceled(); err != nil {
				part.err = err
				return part
			}
			slices.SortStableFunc(selected, func(a, b int) int { return p.compareRows(ctx, a, b) })
			if err := w.checkCanceled(); err != nil {
				part.err = err
				return part
			}
		}
		if p.hasLimit && len(selected) > p.limit {
			selected = selected[:p.limit]
		}
		// Reserve the whole batch's scalar-header slab before handing out the
		// first row. Growing it a row at a time leaves every superseded array
		// live through the rows already pointing into it; one exact batch
		// reservation removes those transient generations and makes both the
		// allocation and retained-memory cost proportional to the slab kept.
		headsPerRow := len(p.columns) + len(p.order)
		if headsPerRow != 0 &&
			len(selected) > int(^uint(0)>>1)/headsPerRow {
			part.err = fmt.Errorf("query: durable result scalar arena overflows int")
			return part
		}
		if err := arena.reserveHeads(len(selected) * headsPerRow); err != nil {
			part.err = err
			return part
		}
		prev := slot.rows
		rows := prev[:0]
		for at, row := range selected {
			if err := cancellationCheckpoint(w.cancel, at); err != nil {
				part.err = err
				return part
			}
			r := fileRow{ordinal: batch.base + uint64(row)}
			// The projected and ordering scalars share one header span split in
			// two, and every byte they detach is packed into one span of the
			// worker's arena, so a row of c columns ordered by k keys costs two
			// reservations instead of the 2+2(c+k) allocations that a slice per
			// role plus a clone per field used to cost — and, since the arena
			// outlives the execution, no allocation at all once it is warm.
			var used int64
			var arenaErr error
			r.values, used, arenaErr = ownScalars(
				arena, ctx, row, p.columns, p.order, nil,
			)
			if arenaErr != nil {
				part.err = arenaErr
				return part
			}
			r.order = r.values[len(p.columns):len(r.values):len(r.values)]
			r.values = r.values[:len(p.columns):len(p.columns)]
			part.bytes += fileRowStructBytes + used
			rows = append(rows, r)
		}
		// Same reason as the grouped branch: a stale row header still owns the
		// bytes it detached from a batch two rings ago.
		clearTail(prev, len(rows))
		slot.rows, part.rows = rows, rows
	}
	for i := range part.groups {
		detachAggregateExtremes(part.groups[i].accs, p.columns)
	}
	detachAggregateExtremes(part.accs, p.columns)
	return part
}

// A fileArena is one batch's bump storage for everything its partial detaches
// and hands to the consumer: the scalar bytes of projected rows and group keys,
// the scalar headers addressing them, and the group accumulators.
//
// The consumer holds what it is given until the execution materializes its
// Result, and that retention is the reason to own the storage rather than an
// argument against it: "the consumer keeps it" says the bytes must outlive the
// batch ring, not that they must be bought fresh. Before these arenas existed a
// projected row cost two allocations and a distinct group four, per row and per
// batch — 1,564 and 6,784 allocations on the two shapes the alloc test measures,
// which no amount of Exec reuse could warm away because nothing retained what
// they bought.
//
// Growth replaces the backing array rather than copying it: views already handed
// out keep pointing at the superseded array, which stays alive exactly as long
// as the rows referencing it do, and the successor starts empty. The successor
// is sized to hold everything handed out since the arena was last rewound, not
// merely the request that overflowed, so a batch that needs the same total again
// finds it in one array instead of growing through the same sequence forever.
type fileArena struct {
	bytes []byte
	heads []scalar
	accs  []aggAcc
	// used is how many bytes this array and every array it superseded have
	// handed out since the last rewind. It is the growth target; see above.
	used int
	// seq is the batch whose storage this is, and live says whether it holds
	// any. See fileArenaSet.
	seq  uint64
	live bool
}

// A fileArenaSet is one worker's arena generations, one per batch in flight.
//
// A single bump arena per worker cannot work, and the reason is worth stating
// because the obvious repair does not work either. The arena has to be rewound
// somewhere, or a LIMIT over a large corpus accumulates every row it ever
// discarded; but a worker's storage may only be rewound once the consumer has
// dropped every row in it, and the newest thing in a single arena is always the
// batch the worker just finished — which is by construction one the consumer has
// not reached. A rewind conditioned on that never fires when the pipeline is
// busy: the race-detector regression test showed retention depending on whether
// the scheduler happened to leave workers idle. A bound that holds only when
// the machine is not busy is not a bound; measured provenance lives in
// docs/performance.md.
//
// Giving each batch its own generation makes the question answerable. A
// generation is reusable once the consumer's drop frontier has passed the batch
// it belongs to, which is a fact about that batch alone and becomes true in
// finite time for every one of them. Sets grow to the number of generations
// actually in flight: for a shape that drops — a spill, or an unordered LIMIT's
// trim — that is a handful, and for one that never drops it is one per batch,
// which is correct, because such an execution really does retain every row it
// selected. Either way the set is retained by the Exec, so the next execution of
// the same shape reuses all of them and allocates nothing.
type fileArenaSet struct {
	gens []fileArena
	at   int
}

// reset marks every generation free for a new execution. The buffers keep their
// capacity; each is rewound when begin hands it out.
func (s *fileArenaSet) reset() {
	for i := range s.gens {
		s.gens[i].live = false
	}
	s.at = 0
}

// begin returns the generation batch seq will pack into, given the consumer's
// drop frontier. It prefers a generation the consumer has finished with, and
// adds one only when every existing generation still holds rows someone can
// read.
//
// The scan starts after the generation used last so that the set is walked in
// age order, which is the order generations become reusable in: the oldest is
// the likeliest to have been dropped.
func (s *fileArenaSet) begin(seq, dropped uint64) *fileArena {
	for i := range s.gens {
		at := (s.at + 1 + i) % len(s.gens)
		g := &s.gens[at]
		if g.live && g.seq >= dropped {
			continue
		}
		s.at = at
		g.rewind()
		g.seq, g.live = seq, true
		return g
	}
	s.gens = append(s.gens, fileArena{seq: seq, live: true})
	s.at = len(s.gens) - 1
	return &s.gens[s.at]
}

// retained is how many bytes the set's generations hold, for the tests that
// assert the bound.
func (s *fileArenaSet) retained() int {
	n := 0
	for i := range s.gens {
		n += cap(s.gens[i].bytes)
	}
	return n
}

// rewind empties one generation.
//
// The stale headers past the new length are deliberately not cleared. Every
// pointer a detached scalar holds points into this generation's own byte
// storage, so once its capacity has converged and no array is being superseded,
// a stale header pins nothing the generation was not keeping anyway — and
// clearing would cost a pass over the whole high-water frontier on every batch.
func (a *fileArena) rewind() {
	a.bytes, a.heads, a.accs = a.bytes[:0], a.heads[:0], a.accs[:0]
	a.used = 0
}

// takeBytes reserves exactly n bytes and returns them as an empty slice capped
// at n, so appends into it land in the arena and cannot be seen past their
// reservation. The exact capping is what lets one reservation serve a whole row:
// see ownScalarInto for why the storage a row is packed into must never move
// while the row is being packed.
func (a *fileArena) takeBytes(n int) ([]byte, error) {
	if n < 0 || a.used > int(^uint(0)>>1)-n {
		return nil, fmt.Errorf("query: durable result arena byte size overflows int")
	}
	nextUsed := a.used + n
	nextCapacity := cap(a.bytes)
	if cap(a.bytes)-len(a.bytes) < n {
		nextCapacity = growCap(cap(a.bytes), nextUsed)
	}
	a.used = nextUsed
	if cap(a.bytes)-len(a.bytes) < n {
		a.bytes = make([]byte, 0, nextCapacity)
	}
	at := len(a.bytes)
	a.bytes = a.bytes[:at+n]
	return a.bytes[at : at : at+n], nil
}

// reserveHeads ensures n more scalar headers fit without extending the active
// span. It lets a batch reserve once before any returned row can pin the
// current array; takeHeads remains the per-row partitioning operation.
func (a *fileArena) reserveHeads(n int) error {
	if n < 0 || len(a.heads) > int(^uint(0)>>1)-n {
		return fmt.Errorf("query: durable result scalar arena overflows int")
	}
	required := len(a.heads) + n
	nextCapacity := cap(a.heads)
	if cap(a.heads)-len(a.heads) < n {
		nextCapacity = growCap(cap(a.heads), required)
	}
	if cap(a.heads)-len(a.heads) < n {
		a.heads = make([]scalar, 0, nextCapacity)
	}
	return nil
}

// takeHeads reserves n scalar headers. Their contents are whatever a previous
// batch left; every caller fills each header it takes before anyone reads it.
func (a *fileArena) takeHeads(n int) ([]scalar, error) {
	if err := a.reserveHeads(n); err != nil {
		return nil, err
	}
	at := len(a.heads)
	a.heads = a.heads[:at+n]
	return a.heads[at : at+n : at+n], nil
}

// takeAccs reserves n zeroed accumulators. These are zeroed where the headers
// are not, because an accumulator is folded into rather than assigned, so a
// previous batch's tallies would be counted as this one's.
func (a *fileArena) takeAccs(n int) ([]aggAcc, error) {
	if n < 0 || len(a.accs) > int(^uint(0)>>1)-n {
		return nil, fmt.Errorf("query: durable result aggregate arena overflows int")
	}
	required := len(a.accs) + n
	nextCapacity := cap(a.accs)
	if cap(a.accs)-len(a.accs) < n {
		nextCapacity = growCap(cap(a.accs), required)
	}
	if cap(a.accs)-len(a.accs) < n {
		a.accs = make([]aggAcc, 0, nextCapacity)
	}
	at := len(a.accs)
	a.accs = a.accs[:at+n]
	out := a.accs[at : at+n : at+n]
	resetAggs(out)
	return out, nil
}

// takeString copies src into the arena and returns it as a string. The result is
// used as a map key by both the per-batch group index and the consumer's group
// frontier, so it stays valid exactly as long as the generation does — which is
// until the consumer has dropped the batch, and every map holding such a key is
// cleared by the same drop.
func (a *fileArena) takeString(src []byte) (string, error) {
	dst, err := a.takeBytes(len(src))
	if err != nil {
		return "", err
	}
	return byteview.String(append(dst, src...)), nil
}

// ownScalars detaches one row's scalars — the value column of each entry in
// cols, then of each entry in order, then each of groupCols — into a single
// span of the worker's scalar headers backed by a single span of its byte
// arena, and reports the retained size the spill accounting charges for them.
// Callers pass the two roles they have: a projected row passes cols and order,
// a group passes groupCols.
//
// Packing is what makes it worth having: the headers and the bytes are one
// reservation each regardless of how many columns a row projects or orders by,
// and the byte span is sized exactly, in one sizing pass, from the same aliasing
// tests ownScalarInto applies — so the two passes cannot disagree and overrun
// the reservation. Sizing exactly also means the span is never grown, which is
// the invariant the returned views depend on: a later append that moved it would
// silently repoint every scalar handed out for an earlier column of the row.
func ownScalars(
	a *fileArena,
	ctx *execCtx,
	row int,
	cols []planColumn,
	order []planOrder,
	groupCols []int,
) ([]scalar, int64, error) {
	n := len(cols) + len(order) + len(groupCols)
	if n == 0 {
		return nil, 0, nil
	}
	need := 0
	for i := range cols {
		need += scalarOwnBytes(ctx.values[cols[i].value][row])
	}
	for i := range order {
		need += scalarOwnBytes(ctx.values[order[i].value][row])
	}
	for _, col := range groupCols {
		need += scalarOwnBytes(ctx.values[col][row])
	}
	// A zero-length reservation still has to be non-nil, so that a scalar whose
	// raw bytes are empty but not nil keeps that shape — Cell.JSON distinguishes
	// the two. Hence the floor of one byte.
	arena, err := a.takeBytes(max(need, 1))
	if err != nil {
		return nil, 0, err
	}
	out, err := a.takeHeads(n)
	if err != nil {
		return nil, 0, err
	}
	out = out[:0]
	for i := range cols {
		var s scalar
		arena, s = ownScalarInto(arena, ctx.values[cols[i].value][row])
		out = append(out, s)
	}
	for i := range order {
		var s scalar
		arena, s = ownScalarInto(arena, ctx.values[order[i].value][row])
		out = append(out, s)
	}
	for _, col := range groupCols {
		var s scalar
		arena, s = ownScalarInto(arena, ctx.values[col][row])
		out = append(out, s)
	}
	return out, int64(n)*scalarStructBytes + int64(need), nil
}

// repackRow re-detaches a surviving row into a, so that the storage a worker
// packed it into can be abandoned. It is ownScalars applied to an already
// detached row rather than to a batch column, and it holds the same invariant:
// the byte span is sized exactly first, so it is never grown while the views
// into it are being handed out.
func repackRow(a *fileArena, r fileRow) (fileRow, error) {
	n := len(r.values) + len(r.order)
	if n == 0 {
		return r, nil
	}
	need := 0
	for _, s := range r.values {
		need += scalarOwnBytes(s)
	}
	for _, s := range r.order {
		need += scalarOwnBytes(s)
	}
	arena, err := a.takeBytes(max(need, 1))
	if err != nil {
		return fileRow{}, err
	}
	out, err := a.takeHeads(n)
	if err != nil {
		return fileRow{}, err
	}
	out = out[:0]
	for _, s := range r.values {
		var d scalar
		arena, d = ownScalarInto(arena, s)
		out = append(out, d)
	}
	for _, s := range r.order {
		var d scalar
		arena, d = ownScalarInto(arena, s)
		out = append(out, d)
	}
	values := len(r.values)
	return fileRow{
		values:  out[:values:values],
		order:   out[values:len(out):len(out)],
		ordinal: r.ordinal,
	}, nil
}

// A classified scalar's fields overlap: classifyRawInto gives a number the same
// slice for num and raw, and gives an unescaped string an sval that points
// inside raw, between the quotes. Detaching each field independently would
// therefore store a number's digits twice and an unescaped string's content
// twice. Copying raw once and re-deriving the views that came from it keeps
// every field's contents identical while copying each byte once.
//
// The alias tests are exact rather than heuristic. A number's num is raw by
// construction, checked by pointer identity. An unescaped string is exactly its
// raw bytes minus the two quotes, and any escape shrinks the decoded form by at
// least one byte, so len(sval)+2 == len(raw) holds precisely when no escape was
// resolved — which is the only case where sval points into raw. An escaped
// string's sval lives in the classification arena and is detached on its own.
//
// They are functions rather than inline expressions because three places must
// agree on them exactly: the sizing pass, the packing pass, and the spill
// accounting. A disagreement between the first two would overrun a row's arena
// and silently repoint scalars already handed out; a disagreement with the
// third would mis-budget the spill threshold.
func numAliasesRaw(s scalar) bool {
	return len(s.num) != 0 && len(s.num) == len(s.raw) && &s.num[0] == &s.raw[0]
}

func svalAliasesRaw(s scalar) bool {
	return s.kind == kindString && len(s.raw) == len(s.sval)+2
}

// scalarOwnBytes is the number of arena bytes detaching s copies.
func scalarOwnBytes(s scalar) int {
	n := len(s.raw)
	if !numAliasesRaw(s) {
		n += len(s.num)
	}
	if !svalAliasesRaw(s) {
		n += len(s.sval)
	}
	return n
}

// ownScalarInto detaches s from the document it was classified against, so it
// can outlive the batch that produced it, by packing the bytes it must own into
// arena. It returns the extended arena and the detached scalar.
//
// The caller must have reserved scalarOwnBytes(s) of spare capacity: the views
// returned for earlier scalars point into arena, so an append that grew it
// would leave them addressing freed storage. Reserving exactly what the whole
// row needs up front is what makes one allocation serve every column of a row.
func ownScalarInto(arena []byte, s scalar) ([]byte, scalar) {
	numAliases, svalAliases := numAliasesRaw(s), svalAliasesRaw(s)
	raw := arena
	if s.raw != nil {
		arena, raw = appendArena(arena, s.raw)
	} else {
		raw = nil
	}
	s.raw = raw
	switch {
	case numAliases:
		s.num = raw
	case s.num != nil:
		arena, s.num = appendArena(arena, s.num)
	}
	switch {
	case svalAliases:
		s.sval = byteview.String(raw[1 : len(raw)-1])
	case s.sval != "":
		var view []byte
		arena, view = appendArena(arena, byteview.Bytes(s.sval))
		s.sval = byteview.String(view)
	}
	return arena, s
}

// appendArena copies src onto arena and returns the extended arena together
// with a capacity-clipped view of the copy, so a later append into the arena
// can never be seen through the view.
func appendArena(arena, src []byte) ([]byte, []byte) {
	start := len(arena)
	arena = append(arena, src...)
	return arena, arena[start:len(arena):len(arena)]
}

// The struct sizes the spill accounting charges. They are constants rather
// than unsafe.Sizeof so this package keeps no unsafe scope of its own;
// TestSpillAccountingStructSizes asserts they still match the real layouts, so
// a field added to either type cannot silently un-budget the spill threshold.
//
// The previous constant here was 48 for both, which under-charged a scalar by
// 40 bytes and made a spill trigger at roughly twice its configured target.
const (
	scalarStructBytes  = 88
	fileRowStructBytes = 56
	aggAccStructBytes  = 32
)

// scalarBytes is the retained size of one detached scalar: the struct itself
// plus the storage ownScalar cloned for it. num and sval are counted only when
// they are separate allocations, mirroring ownScalar's aliasing exactly, so the
// spill threshold reflects bytes actually held.
func scalarBytes(s scalar) int64 {
	return int64(scalarStructBytes) + int64(scalarOwnBytes(s))
}

func rowBytes(r fileRow) int64 {
	n := int64(fileRowStructBytes)
	for _, s := range r.values {
		n += scalarBytes(s)
	}
	for _, s := range r.order {
		n += scalarBytes(s)
	}
	return n
}

func fileResultPayloadBytes(r fileRow) (int64, bool) {
	var bytes int64
	for i := range r.values {
		add := resultCellPayloadBytes(cellFromScalar(r.values[i]))
		if add < 0 || bytes > int64(^uint64(0)>>1)-add {
			return 0, false
		}
		bytes += add
	}
	return bytes, true
}

func estimateRows(rows []fileRow) int64 {
	var n int64
	for i := range rows {
		n += rowBytes(rows[i])
	}
	return n
}

func mergeAggs(
	dst, src []aggAcc,
	columns []planColumn,
	budget *aggregateBudget,
) error {
	for i := range dst {
		d := &dst[i]
		s := src[i]
		d.count += s.count
		if s.num == nil || s.num.n == 0 {
			continue
		}
		dn, err := d.number(budget)
		if err != nil {
			return err
		}
		switch columns[i].agg {
		case aggSum, aggAvg:
			if err := dn.sum.addSum(&s.num.sum, &d.lease, budget); err != nil {
				return err
			}
		case aggMin:
			if dn.n == 0 || compareScalar(s.num.extreme, dn.extreme) < 0 {
				if err := d.lease.reserve(
					budget, aggregateAccBaseBytes+int64(len(s.num.extreme.num)),
				); err != nil {
					return err
				}
				copyAggregateExtreme(dn, s.num.extreme)
			}
		case aggMax:
			if dn.n == 0 || compareScalar(s.num.extreme, dn.extreme) > 0 {
				if err := d.lease.reserve(
					budget, aggregateAccBaseBytes+int64(len(s.num.extreme.num)),
				); err != nil {
					return err
				}
				copyAggregateExtreme(dn, s.num.extreme)
			}
		}
		dn.n += s.num.n
	}
	return nil
}

func (p *plan) compareFileRows(a, b fileRow) int {
	for i, o := range p.order {
		c := compareScalar(a.order[i], b.order[i])
		if o.dir == Desc {
			c = -c
		}
		if c != 0 {
			return c
		}
	}
	return compareFileOrdinal(a, b)
}

func compareFileOrdinal(a, b fileRow) int {
	switch {
	case a.ordinal < b.ordinal:
		return -1
	case a.ordinal > b.ordinal:
		return 1
	default:
		return 0
	}
}

func (p *plan) compareFileGroups(a, b fileGroup) int {
	for _, o := range p.order {
		c := compareScalar(a.scalars[o.slot], b.scalars[o.slot])
		if o.dir == Desc {
			c = -c
		}
		if c != 0 {
			return c
		}
	}
	switch {
	case a.first < b.first:
		return -1
	case a.first > b.first:
		return 1
	default:
		return 0
	}
}

func (p *plan) compareResultGroups(a, b fileGroup) int {
	if len(p.order) != 0 {
		return p.compareFileGroups(a, b)
	}
	switch {
	case a.first < b.first:
		return -1
	case a.first > b.first:
		return 1
	default:
		return 0
	}
}

func (p *plan) resultLimit() int {
	if p.hasLimit {
		return p.limit
	}
	return -1
}

func (p *plan) fileRowResultInto(result Result, rows []fileRow, cancel *CancelFlag) (Result, error) {
	if err := cancellationError(cancel); err != nil {
		return result, err
	}
	if err := prepareResult(&result, p, len(rows)); err != nil {
		return result, err
	}
	for row := range rows {
		if err := cancellationCheckpoint(cancel, row); err != nil {
			return result, err
		}
		for col := range p.columns {
			cell := cellFromScalar(rows[row].values[col])
			if err := result.admitResultCell(cell); err != nil {
				return result, err
			}
			cell = result.ownFileCell(cell)
			result.Columns[col].Cells[row] = cell
		}
	}
	return result, nil
}

func (p *plan) fileGroupResultInto(
	result Result,
	groups []fileGroup,
	w *Workspace,
) (Result, error) {
	if err := w.checkCanceled(); err != nil {
		return result, err
	}
	slices.SortStableFunc(groups, p.compareResultGroups)
	if err := w.checkCanceled(); err != nil {
		return result, err
	}
	if p.hasLimit && len(groups) > p.limit {
		groups = groups[:p.limit]
	}
	if err := prepareResult(&result, p, len(groups)); err != nil {
		return result, err
	}
	for row := range groups {
		if err := cancellationCheckpoint(w.cancel, row); err != nil {
			return result, err
		}
		g := group{scalars: groups[row].scalars, accs: groups[row].accs}
		if err := p.fillAggregateCells(&result, row, g.accs, &g, w); err != nil {
			return result, err
		}
		for column := range result.Columns {
			result.Columns[column].Cells[row] =
				result.ownFileCell(result.Columns[column].Cells[row])
		}
	}
	return result, nil
}

// The spill representation uses exported fields so encoding/gob can stream
// one record at a time without retaining an entire run in memory.
type diskScalar struct {
	Kind  uint8
	Bool  bool
	Num   []byte
	IsInt bool
	Int   int64
	Text  string
	Raw   []byte
}

type diskRow struct {
	Values  []diskScalar
	Order   []diskScalar
	Ordinal uint64
}

type diskAgg struct {
	Count int
	Num   *diskNumberAcc
}

type diskNumberAcc struct {
	N        int
	SumSet   bool
	SumCoeff string
	SumScale string
	Extreme  diskScalar
}

type diskGroup struct {
	Key     string
	Scalars []diskScalar
	Aggs    []diskAgg
	First   uint64
	Bytes   int64
}

func scalarToDisk(s scalar) diskScalar {
	return diskScalar{Kind: uint8(s.kind), Bool: s.bval, Num: s.num, IsInt: s.isInt, Int: s.ival, Text: s.sval, Raw: s.raw}
}

func scalarFromDisk(s diskScalar) scalar {
	return scalar{kind: scalarKind(s.Kind), bval: s.Bool, num: s.Num, isInt: s.IsInt, ival: s.Int, sval: s.Text, raw: s.Raw}
}

func validateDiskScalar(s diskScalar) error {
	if scalarKind(s.Kind) > kindContainer {
		return fmt.Errorf("%w: invalid scalar kind %d", ErrSpillCorrupt, s.Kind)
	}
	if scalarKind(s.Kind) == kindNumber &&
		!validJSONNumber(byteview.String(s.Num)) {
		return fmt.Errorf("%w: invalid JSON number", ErrSpillCorrupt)
	}
	return nil
}

func rowToDisk(r fileRow) diskRow {
	d := diskRow{Values: make([]diskScalar, len(r.values)), Order: make([]diskScalar, len(r.order)), Ordinal: r.ordinal}
	for i := range r.values {
		d.Values[i] = scalarToDisk(r.values[i])
	}
	for i := range r.order {
		d.Order[i] = scalarToDisk(r.order[i])
	}
	return d
}

func rowFromDisk(d diskRow) fileRow {
	r := fileRow{values: make([]scalar, len(d.Values)), order: make([]scalar, len(d.Order)), ordinal: d.Ordinal}
	for i := range d.Values {
		r.values[i] = scalarFromDisk(d.Values[i])
	}
	for i := range d.Order {
		r.order[i] = scalarFromDisk(d.Order[i])
	}
	return r
}

func groupToDisk(g fileGroup) diskGroup {
	d := diskGroup{Key: g.key, Scalars: make([]diskScalar, len(g.scalars)), Aggs: make([]diskAgg, len(g.accs)), First: g.first, Bytes: g.bytes}
	for i := range g.scalars {
		d.Scalars[i] = scalarToDisk(g.scalars[i])
	}
	for i, a := range g.accs {
		d.Aggs[i] = aggregateToDisk(a)
	}
	return d
}

func groupFromDisk(d diskGroup, budget *aggregateBudget) (fileGroup, error) {
	g := fileGroup{key: d.Key, scalars: make([]scalar, len(d.Scalars)), accs: make([]aggAcc, len(d.Aggs)), first: d.First, bytes: d.Bytes}
	for i := range d.Scalars {
		if err := validateDiskScalar(d.Scalars[i]); err != nil {
			return fileGroup{}, err
		}
		g.scalars[i] = scalarFromDisk(d.Scalars[i])
	}
	for i, a := range d.Aggs {
		var err error
		g.accs[i], err = aggregateFromDisk(a, budget)
		if err != nil {
			return fileGroup{}, err
		}
	}
	return g, nil
}

func aggregateToDisk(a aggAcc) diskAgg {
	out := diskAgg{Count: a.count}
	if a.num == nil {
		return out
	}
	n := &diskNumberAcc{N: a.num.n, Extreme: scalarToDisk(a.num.extreme)}
	if a.num.sum.set {
		n.SumSet = true
		if a.num.sum.big {
			n.SumCoeff = a.num.sum.coeff.String()
			n.SumScale = a.num.sum.scale.String()
		} else {
			n.SumCoeff = strconv.FormatInt(a.num.sum.smallCoeff, 10)
			n.SumScale = strconv.FormatInt(a.num.sum.smallScale, 10)
		}
	}
	out.Num = n
	return out
}

func aggregateFromDisk(a diskAgg, budget *aggregateBudget) (aggAcc, error) {
	if a.Count < 0 {
		return aggAcc{}, fmt.Errorf(
			"%w: negative aggregate count", ErrSpillCorrupt)
	}
	out := aggAcc{count: a.Count}
	if a.Num == nil {
		return out, nil
	}
	if a.Num.N < 0 {
		return aggAcc{}, fmt.Errorf(
			"%w: negative numeric aggregate count", ErrSpillCorrupt)
	}
	if err := validateDiskScalar(a.Num.Extreme); err != nil {
		return aggAcc{}, err
	}
	payload := saturatedBytes(
		int64(len(a.Num.SumCoeff)),
		int64(len(a.Num.SumScale)),
	)
	payload = saturatedBytes(payload, int64(len(a.Num.Extreme.Num)))
	payload = saturatedBytes(payload, 32)
	need := saturatedBytes(
		aggregateAccBaseBytes,
		saturatedProduct(8, payload),
	)
	if err := out.lease.reserve(budget, need); err != nil {
		return aggAcc{}, err
	}
	out.num = &numberAcc{n: a.Num.N, extreme: scalarFromDisk(a.Num.Extreme)}
	if !a.Num.SumSet {
		return out, nil
	}
	out.num.sum.set = true
	if _, ok := out.num.sum.coeff.SetString(a.Num.SumCoeff, 10); !ok {
		return aggAcc{}, fmt.Errorf(
			"%w: invalid exact aggregate coefficient", ErrSpillCorrupt)
	}
	if _, ok := out.num.sum.scale.SetString(a.Num.SumScale, 10); !ok {
		return aggAcc{}, fmt.Errorf(
			"%w: invalid exact aggregate exponent", ErrSpillCorrupt)
	}
	out.num.sum.big = true
	out.num.sum.normalizeBig()
	return out, nil
}

type spillRun struct {
	path string
	size int64
}

type spillManager struct {
	dir          string
	files        map[string]struct{}
	columns      []planColumn
	groupScalars int
	budget       *aggregateBudget
	limit        int64
	live         int64
	cancel       *CancelFlag
	// removeFile is a test seam for hostile-filesystem cleanup failures. A nil
	// hook is the production fast path and calls os.Remove directly.
	removeFile func(string) error
	// runs and size tally what this execution spilled, and are folded into
	// ExecStats when it ends. The manager counts into itself rather than
	// through a pointer to the caller's ExecStats because that pointer was the
	// last thing forcing a warm execution to allocate: an ExecStats is a
	// quarter-kilobyte struct, and addressing the local one made escape
	// analysis move it to the heap on every single call, spilling or not.
	runs uint64
	size int64
}

// newSpillManager reuses files as its live-run set. An execution that never
// spills — the overwhelming majority — must not pay for a map it never writes
// to, so the set is created on the first create and handed back by cleanup for
// the next execution to reuse.
func newSpillManager(
	dir string,
	files map[string]struct{},
	columns []planColumn,
	groupScalars int,
	budget *aggregateBudget,
	limit int64,
	cancel *CancelFlag,
) spillManager {
	manager := spillManager{
		dir: dir, files: files, columns: columns, groupScalars: groupScalars,
		budget: budget, limit: limit, cancel: cancel,
	}
	for path := range files {
		info, err := os.Stat(path)
		switch {
		case err == nil:
			manager.live = saturatedBytes(manager.live, info.Size())
		case errors.Is(err, os.ErrNotExist):
			delete(files, path)
		default:
			// An unstatable leftover still consumes unknown storage. Charge
			// the whole quota so another run cannot be admitted on the
			// assumption that it disappeared.
			if limit >= 0 {
				manager.live = limit
			}
		}
	}
	return manager
}

type spillQuotaWriter struct {
	dst       io.Writer
	remaining int64
	limit     int64
	cancel    *CancelFlag
}

func (w *spillQuotaWriter) Write(src []byte) (int, error) {
	if err := cancellationError(w.cancel); err != nil {
		return 0, err
	}
	if w.limit >= 0 && int64(len(src)) > w.remaining {
		return 0, &SpillBudgetError{
			Bytes: saturatedBytes(w.limit, 1),
			Limit: w.limit,
		}
	}
	n, err := w.dst.Write(src)
	if w.limit >= 0 {
		w.remaining -= int64(n)
	}
	return n, err
}

func (s *spillManager) writer(file *os.File) io.Writer {
	if s.limit < 0 && s.cancel == nil {
		return file
	}
	return &spillQuotaWriter{
		dst: file, remaining: max(s.limit-s.live, 0), limit: s.limit,
		cancel: s.cancel,
	}
}

func (s *spillManager) create(pattern string) (*os.File, error) {
	if err := cancellationError(s.cancel); err != nil {
		return nil, err
	}
	f, err := os.CreateTemp(s.dir, pattern)
	if err == nil {
		if s.files == nil {
			s.files = make(map[string]struct{})
		}
		s.files[f.Name()] = struct{}{}
	}
	return f, err
}

func (s *spillManager) unlink(path string) error {
	if s.removeFile != nil {
		return s.removeFile(path)
	}
	return os.Remove(path)
}

func (s *spillManager) finish(f *os.File) (spillRun, error) {
	if err := cancellationError(s.cancel); err != nil {
		_ = f.Close()
		return spillRun{}, err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return spillRun{}, err
	}
	info, err := f.Stat()
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return spillRun{}, err
	}
	if err := cancellationError(s.cancel); err != nil {
		return spillRun{}, err
	}
	run := spillRun{path: f.Name(), size: info.Size()}
	if s.limit >= 0 && (run.size > s.limit-s.live) {
		budgetErr := &SpillBudgetError{
			Bytes: saturatedBytes(s.live, run.size),
			Limit: s.limit,
		}
		if removeErr := s.unlink(run.path); removeErr != nil &&
			!errors.Is(removeErr, os.ErrNotExist) {
			// The rejected run still occupies disk. Keep it tracked and charge
			// its actual size so cleanup and the next execution cannot pretend
			// the quota was reclaimed.
			s.live = saturatedBytes(s.live, run.size)
			return spillRun{}, errors.Join(budgetErr, removeErr)
		}
		delete(s.files, run.path)
		return spillRun{}, budgetErr
	}
	s.runs++
	s.size = saturatedBytes(s.size, run.size)
	s.live = saturatedBytes(s.live, run.size)
	return run, nil
}

func (s *spillManager) remove(run spillRun) error {
	if err := s.unlink(run.path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	delete(s.files, run.path)
	s.live = max(s.live-run.size, 0)
	return nil
}

// cleanup removes every run this execution left behind and returns the emptied
// set so the next execution can reuse its buckets.
func (s *spillManager) cleanup() (map[string]struct{}, error) {
	var cleanupErr error
	for path := range s.files {
		if err := s.unlink(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			cleanupErr = errors.Join(cleanupErr, err)
			continue
		}
		delete(s.files, path)
	}
	if cleanupErr == nil {
		s.live = 0
	}
	return s.files, cleanupErr
}

func (s *spillManager) writeRows(p *plan, rows []fileRow) (spillRun, error) {
	if err := cancellationError(s.cancel); err != nil {
		return spillRun{}, err
	}
	slices.SortStableFunc(rows, p.compareFileRows)
	if err := cancellationError(s.cancel); err != nil {
		return spillRun{}, err
	}
	f, err := s.create("vibejson-query-rows-*")
	if err != nil {
		return spillRun{}, err
	}
	w := bufio.NewWriterSize(s.writer(f), 64<<10)
	enc := gob.NewEncoder(w)
	for i := range rows {
		if err := cancellationCheckpoint(s.cancel, i); err != nil {
			_ = f.Close()
			return spillRun{}, err
		}
		if err := enc.Encode(rowToDisk(rows[i])); err != nil {
			_ = f.Close()
			return spillRun{}, err
		}
	}
	if err := w.Flush(); err != nil {
		_ = f.Close()
		return spillRun{}, err
	}
	return s.finish(f)
}

// writeGroups sorts the group frontier by key and streams it to a run file.
//
// The sort is in place, which invalidates the caller's key-to-position index —
// so every caller either clears that index immediately after a success or is
// abandoning the execution on a failure. Sorting a copy instead would cost one
// allocation per spill for a frontier the caller is about to drop anyway.
func (s *spillManager) writeGroups(groups []fileGroup) (spillRun, error) {
	if err := cancellationError(s.cancel); err != nil {
		return spillRun{}, err
	}
	slices.SortFunc(groups, func(a, b fileGroup) int { return strings.Compare(a.key, b.key) })
	if err := cancellationError(s.cancel); err != nil {
		return spillRun{}, err
	}
	f, err := s.create("vibejson-query-groups-*")
	if err != nil {
		return spillRun{}, err
	}
	w := bufio.NewWriterSize(s.writer(f), 64<<10)
	enc := gob.NewEncoder(w)
	for i := range groups {
		if err := cancellationCheckpoint(s.cancel, i); err != nil {
			_ = f.Close()
			return spillRun{}, err
		}
		if err := enc.Encode(groupToDisk(groups[i])); err != nil {
			_ = f.Close()
			return spillRun{}, err
		}
	}
	if err := w.Flush(); err != nil {
		_ = f.Close()
		return spillRun{}, err
	}
	return s.finish(f)
}

type rowCursor struct {
	file *os.File
	dec  *gob.Decoder
	row  fileRow
	p    *plan
}

func openRowCursor(p *plan, run spillRun) (*rowCursor, error) {
	f, err := os.Open(run.path)
	if err != nil {
		return nil, err
	}
	c := &rowCursor{
		file: f,
		dec:  gob.NewDecoder(bufio.NewReaderSize(f, 64<<10)),
		p:    p,
	}
	if err := c.next(); err != nil {
		_ = f.Close()
		return nil, err
	}
	return c, nil
}

func (c *rowCursor) next() error {
	var d diskRow
	if err := c.dec.Decode(&d); err != nil {
		return spillDecodeError(err)
	}
	if len(d.Values) != len(c.p.columns) || len(d.Order) != len(c.p.order) {
		return fmt.Errorf(
			"%w: row shape is %d values/%d order keys, want %d/%d",
			ErrSpillCorrupt, len(d.Values), len(d.Order),
			len(c.p.columns), len(c.p.order),
		)
	}
	for i := range d.Values {
		if err := validateDiskScalar(d.Values[i]); err != nil {
			return err
		}
	}
	for i := range d.Order {
		if err := validateDiskScalar(d.Order[i]); err != nil {
			return err
		}
	}
	c.row = rowFromDisk(d)
	return nil
}

func spillDecodeError(err error) error {
	if errors.Is(err, io.EOF) {
		return err
	}
	return fmt.Errorf("%w: %v", ErrSpillCorrupt, err)
}

type rowHeap struct {
	p     *plan
	items []*rowCursor
}

func (h rowHeap) Len() int           { return len(h.items) }
func (h rowHeap) Less(i, j int) bool { return h.p.compareFileRows(h.items[i].row, h.items[j].row) < 0 }
func (h rowHeap) Swap(i, j int)      { h.items[i], h.items[j] = h.items[j], h.items[i] }
func (h *rowHeap) Push(x any)        { h.items = append(h.items, x.(*rowCursor)) }
func (h *rowHeap) Pop() any {
	n := len(h.items) - 1
	x := h.items[n]
	h.items = h.items[:n]
	return x
}

func openRowHeap(p *plan, runs []spillRun) (*rowHeap, error) {
	h := &rowHeap{p: p}
	for _, run := range runs {
		c, err := openRowCursor(p, run)
		if errors.Is(err, io.EOF) {
			continue
		}
		if err != nil {
			closeRowHeap(h)
			return nil, err
		}
		heap.Push(h, c)
	}
	return h, nil
}

func closeRowHeap(h *rowHeap) {
	for _, c := range h.items {
		_ = c.file.Close()
	}
}

func (s *spillManager) mergeRowRuns(p *plan, runs []spillRun) (spillRun, error) {
	if err := cancellationError(s.cancel); err != nil {
		return spillRun{}, err
	}
	h, err := openRowHeap(p, runs)
	if err != nil {
		return spillRun{}, err
	}
	defer closeRowHeap(h)
	f, err := s.create("vibejson-query-rowmerge-*")
	if err != nil {
		return spillRun{}, err
	}
	w := bufio.NewWriterSize(s.writer(f), 64<<10)
	enc := gob.NewEncoder(w)
	at := 0
	for h.Len() != 0 {
		if err := cancellationCheckpoint(s.cancel, at); err != nil {
			_ = f.Close()
			return spillRun{}, err
		}
		at++
		c := heap.Pop(h).(*rowCursor)
		if err := enc.Encode(rowToDisk(c.row)); err != nil {
			_ = c.file.Close()
			_ = f.Close()
			return spillRun{}, err
		}
		if err := c.next(); err == nil {
			heap.Push(h, c)
		} else if errors.Is(err, io.EOF) {
			_ = c.file.Close()
		} else {
			_ = c.file.Close()
			_ = f.Close()
			return spillRun{}, err
		}
	}
	if err := w.Flush(); err != nil {
		_ = f.Close()
		return spillRun{}, err
	}
	return s.finish(f)
}

func (s *spillManager) reduceRowRuns(p *plan, runs []spillRun) ([]spillRun, error) {
	for len(runs) > maxSpillFanIn {
		if err := cancellationError(s.cancel); err != nil {
			return nil, err
		}
		next := make([]spillRun, 0, (len(runs)+maxSpillFanIn-1)/maxSpillFanIn)
		for start := 0; start < len(runs); start += maxSpillFanIn {
			if err := cancellationCheckpoint(s.cancel, start/maxSpillFanIn); err != nil {
				return nil, err
			}
			end := min(start+maxSpillFanIn, len(runs))
			merged, err := s.mergeRowRuns(p, runs[start:end])
			if err != nil {
				return nil, err
			}
			for _, run := range runs[start:end] {
				if err := s.remove(run); err != nil {
					return nil, err
				}
			}
			next = append(next, merged)
		}
		runs = next
	}
	return runs, nil
}

func (s *spillManager) readMergedRows(
	p *plan,
	result *Result,
	runs []spillRun,
	limit int,
	rows []fileRow,
) ([]fileRow, error) {
	if err := cancellationError(s.cancel); err != nil {
		return rows, err
	}
	h, err := openRowHeap(p, runs)
	if err != nil {
		return rows, err
	}
	defer closeRowHeap(h)
	var payloadBytes int64
	for h.Len() != 0 && (limit < 0 || len(rows) < limit) {
		if err := cancellationCheckpoint(s.cancel, len(rows)); err != nil {
			return rows, err
		}
		c := heap.Pop(h).(*rowCursor)
		payload, ok := fileResultPayloadBytes(c.row)
		if !ok || payloadBytes > int64(^uint64(0)>>1)-payload {
			_ = c.file.Close()
			return rows, result.resultByteBudgetError(
				len(rows)+1, int64(^uint64(0)>>1),
			)
		}
		payloadBytes += payload
		if _, err := result.checkResultBudget(
			len(p.columns), len(rows)+1, payloadBytes,
		); err != nil {
			_ = c.file.Close()
			return rows, err
		}
		rows = append(rows, c.row)
		if err := c.next(); err == nil {
			heap.Push(h, c)
		} else if errors.Is(err, io.EOF) {
			_ = c.file.Close()
		} else {
			_ = c.file.Close()
			return rows, err
		}
	}
	return rows, nil
}

type groupCursor struct {
	file         *os.File
	dec          *gob.Decoder
	group        fileGroup
	budget       *aggregateBudget
	columns      int
	groupScalars int
}

func openGroupCursor(
	run spillRun,
	budget *aggregateBudget,
	columns, groupScalars int,
) (*groupCursor, error) {
	f, err := os.Open(run.path)
	if err != nil {
		return nil, err
	}
	c := &groupCursor{
		file: f, dec: gob.NewDecoder(bufio.NewReaderSize(f, 64<<10)),
		budget: budget, columns: columns, groupScalars: groupScalars,
	}
	if err := c.next(); err != nil {
		_ = f.Close()
		return nil, err
	}
	return c, nil
}

func (c *groupCursor) next() error {
	var d diskGroup
	if err := c.dec.Decode(&d); err != nil {
		return spillDecodeError(err)
	}
	if len(d.Aggs) != c.columns || len(d.Scalars) != c.groupScalars {
		return fmt.Errorf(
			"%w: group shape is %d scalars/%d accumulators, want %d/%d",
			ErrSpillCorrupt, len(d.Scalars), len(d.Aggs),
			c.groupScalars, c.columns,
		)
	}
	var err error
	c.group, err = groupFromDisk(d, c.budget)
	return err
}

type groupHeap struct{ items []*groupCursor }

func (h groupHeap) Len() int           { return len(h.items) }
func (h groupHeap) Less(i, j int) bool { return h.items[i].group.key < h.items[j].group.key }
func (h groupHeap) Swap(i, j int)      { h.items[i], h.items[j] = h.items[j], h.items[i] }
func (h *groupHeap) Push(x any)        { h.items = append(h.items, x.(*groupCursor)) }
func (h *groupHeap) Pop() any {
	n := len(h.items) - 1
	x := h.items[n]
	h.items = h.items[:n]
	return x
}

func openGroupHeap(
	runs []spillRun,
	budget *aggregateBudget,
	columns, groupScalars int,
) (*groupHeap, error) {
	h := &groupHeap{}
	for _, run := range runs {
		c, err := openGroupCursor(run, budget, columns, groupScalars)
		if errors.Is(err, io.EOF) {
			continue
		}
		if err != nil {
			closeGroupHeap(h)
			return nil, err
		}
		heap.Push(h, c)
	}
	return h, nil
}

func closeGroupHeap(h *groupHeap) {
	for _, c := range h.items {
		_ = c.file.Close()
	}
}

func (s *spillManager) mergeGroups(runs []spillRun, emit func(fileGroup) error) error {
	if err := cancellationError(s.cancel); err != nil {
		return err
	}
	h, err := openGroupHeap(
		runs, s.budget, len(s.columns), s.groupScalars,
	)
	if err != nil {
		return err
	}
	defer closeGroupHeap(h)
	var current *fileGroup
	at := 0
	for h.Len() != 0 {
		if err := cancellationCheckpoint(s.cancel, at); err != nil {
			return err
		}
		at++
		c := heap.Pop(h).(*groupCursor)
		g := c.group
		if current == nil || current.key != g.key {
			if current != nil {
				if err := emit(*current); err != nil {
					_ = c.file.Close()
					return err
				}
			}
			copy := g
			current = &copy
		} else {
			if err := mergeAggs(current.accs, g.accs, s.columns, s.budget); err != nil {
				_ = c.file.Close()
				return err
			}
			if g.first < current.first {
				current.first = g.first
			}
		}
		if err := c.next(); err == nil {
			heap.Push(h, c)
		} else if errors.Is(err, io.EOF) {
			_ = c.file.Close()
		} else {
			_ = c.file.Close()
			return err
		}
	}
	if current != nil {
		return emit(*current)
	}
	return nil
}

func (s *spillManager) mergeGroupRuns(runs []spillRun) (spillRun, error) {
	if err := cancellationError(s.cancel); err != nil {
		return spillRun{}, err
	}
	f, err := s.create("vibejson-query-groupmerge-*")
	if err != nil {
		return spillRun{}, err
	}
	w := bufio.NewWriterSize(s.writer(f), 64<<10)
	enc := gob.NewEncoder(w)
	err = s.mergeGroups(runs, func(g fileGroup) error { return enc.Encode(groupToDisk(g)) })
	if err == nil {
		err = w.Flush()
	}
	if err != nil {
		_ = f.Close()
		return spillRun{}, err
	}
	return s.finish(f)
}

func (s *spillManager) reduceGroupRuns(runs []spillRun) ([]spillRun, error) {
	for len(runs) > maxSpillFanIn {
		if err := cancellationError(s.cancel); err != nil {
			return nil, err
		}
		next := make([]spillRun, 0, (len(runs)+maxSpillFanIn-1)/maxSpillFanIn)
		for start := 0; start < len(runs); start += maxSpillFanIn {
			if err := cancellationCheckpoint(s.cancel, start/maxSpillFanIn); err != nil {
				return nil, err
			}
			end := min(start+maxSpillFanIn, len(runs))
			merged, err := s.mergeGroupRuns(runs[start:end])
			if err != nil {
				return nil, err
			}
			for _, run := range runs[start:end] {
				if err := s.remove(run); err != nil {
					return nil, err
				}
			}
			next = append(next, merged)
		}
		runs = next
	}
	return runs, nil
}

type resultGroupHeap struct {
	p      *plan
	groups []fileGroup
}

func (h resultGroupHeap) Len() int { return len(h.groups) }
func (h resultGroupHeap) Less(i, j int) bool {
	// The worst retained group is the root, so a better candidate can replace
	// it in O(log LIMIT) while the merged key stream is read once.
	return h.p.compareResultGroups(h.groups[i], h.groups[j]) > 0
}
func (h resultGroupHeap) Swap(i, j int) { h.groups[i], h.groups[j] = h.groups[j], h.groups[i] }
func (h *resultGroupHeap) Push(value any) {
	h.groups = append(h.groups, value.(fileGroup))
}
func (h *resultGroupHeap) Pop() any {
	last := len(h.groups) - 1
	value := h.groups[last]
	h.groups[last] = fileGroup{}
	h.groups = h.groups[:last]
	return value
}

func retainedGroupBytes(group fileGroup) int64 {
	return max(group.bytes, 64)
}

func saturatedBytes(a, b int64) int64 {
	if a < 0 || b < 0 || a > int64(^uint64(0)>>1)-b {
		return int64(^uint64(0) >> 1)
	}
	return a + b
}

func (s *spillManager) readMergedGroups(
	p *plan,
	result *Result,
	groups []fileGroup,
	runs []spillRun,
	memoryBytes int64,
) ([]fileGroup, error) {
	if err := cancellationError(s.cancel); err != nil {
		return groups, err
	}
	h := resultGroupHeap{p: p, groups: groups}
	// Merged groups are the immediate representation of the final output, not
	// an independent hash/sort workspace: fileGroupResultInto consumes them
	// directly into Result. Let that frontier use the finite result allowance
	// in addition to work memory. This preserves work_mem-style semantics for
	// results larger than MemoryBytes while keeping hostile executions bounded.
	// When ResultBytes is explicitly disabled, the frontier remains strictly
	// capped by MemoryBytes.
	frontierBytes := memoryBytes
	if result.resultBytesLimit >= 0 {
		frontierBytes = saturatedBytes(frontierBytes, result.resultBytesLimit)
	}

	keep := -1
	if p.hasLimit {
		keep = p.limit
	}
	hardRows := -1
	if result.resultRowsLimit >= 0 &&
		(!p.hasLimit || p.limit > result.resultRowsLimit) {
		hardRows = result.resultRowsLimit
		if keep < 0 || hardRows < keep {
			keep = hardRows
		}
	}
	if keep >= 0 {
		heap.Init(&h)
	}

	total := 0
	var retained int64
	err := s.mergeGroups(runs, func(group fileGroup) error {
		if cancelErr := cancellationCheckpoint(s.cancel, total); cancelErr != nil {
			return cancelErr
		}
		total++
		if hardRows >= 0 && total > hardRows {
			return &ResultBudgetError{
				Rows: total, RowLimit: result.resultRowsLimit,
			}
		}
		if keep == 0 {
			return nil
		}

		groupBytes := retainedGroupBytes(group)
		switch {
		case keep < 0 || len(h.groups) < keep:
			next := saturatedBytes(retained, groupBytes)
			if next > frontierBytes {
				return &WorkBudgetError{
					Resource: "merged group frontier",
					Bytes:    next,
					Limit:    frontierBytes,
				}
			}
			retained = next
			h.groups = append(h.groups, group)
			if keep >= 0 {
				heap.Fix(&h, len(h.groups)-1)
			}
		case p.compareResultGroups(group, h.groups[0]) < 0:
			next := retained - retainedGroupBytes(h.groups[0])
			next = saturatedBytes(next, groupBytes)
			if next > frontierBytes {
				return &WorkBudgetError{
					Resource: "merged group frontier",
					Bytes:    next,
					Limit:    frontierBytes,
				}
			}
			retained = next
			h.groups[0] = group
			heap.Fix(&h, 0)
		}
		return nil
	})
	return h.groups, err
}
