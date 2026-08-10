package query

import (
	"errors"
	"fmt"
	"sync"
	"unsafe"

	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
)

// A sourceKind discriminates the collection a Source names. The invalid state
// is deliberately the zero value, so a Source that was declared but never
// constructed is rejected by RunInto instead of silently executing over an
// empty Segment and returning a plausible, wrong, empty result.
type sourceKind uint8

const (
	sourceInvalid sourceKind = iota
	sourceSegment
	sourceHeapSnapshot
	sourceFileSnapshot
	sourceFileOverlay
	sourceSnapshotOverlay
	sourceDatabase
	sourceFileDatabase
	sourceRelationSpool
)

// A Source is the collection a compiled query runs over. Construct one with
// exactly one of [FromSegment], [FromSnapshot], [FromFile],
// [FromFileOverlay], [FromSnapshotOverlay], [FromDatabase], or
// [FromFileDatabase]; the zero Source names nothing and every execution
// rejects it.
//
// Source is a concrete discriminated struct rather than an interface. A
// Segment, a heap snapshot, a durable snapshot, an overlay, and a coherent
// multi-collection snapshot expose materially different operations and
// lifetimes; an interface would not remove their dispatch. Keeping the handle
// concrete also preserves [Query.RunInto]'s warmed allocation-free boundary
// without relying on whether a particular snapshot value can be boxed without
// escaping.
type Source struct {
	kind sourceKind
	// payload is a *store.Segment for sourceSegment and a *FileOverlaySource
	// for sourceFileOverlay and sourceSnapshotOverlay. Those variants are
	// disjoint, so one GC-visible pointer keeps Source the same size it had
	// before overlay support; adding an interface field here made every
	// ordinary database/sql query retain 16 extra bytes even though it never
	// used an overlay.
	payload unsafe.Pointer
	heap    store.Snapshot
	file    *durable.Snapshot
	catalog store.DatabaseSnapshot
	files   durable.DatabaseSnapshot
	name    string
}

func (s Source) subquerySource(outer, collection string) (Source, error) {
	if collection == "" {
		// A FROM-less child still needs the incoming database source while it
		// resolves its own CTEs, derived relations, set leaves, or predicate
		// subqueries. Statement substitutes the synthetic unit row only for that
		// child's final scalar scan; replacing the source here destroys the
		// catalog before deeper physical reads can bind.
		return s, nil
	}
	switch s.kind {
	case sourceDatabase:
		return FromDatabase(s.catalog, collection), nil
	case sourceFileDatabase:
		return FromFileDatabase(s.files, collection), nil
	case sourceHeapSnapshot, sourceFileSnapshot, sourceSegment, sourceSnapshotOverlay:
		if collection == outer {
			return s, nil
		}
	}
	return Source{}, fmt.Errorf(
		"query: subquery reads collection %q beside %q, so both must come from one database snapshot; use FromDatabase or FromFileDatabase",
		collection, outer)
}

// FromSegment names an in-memory [store.Segment]. The Segment is not modified by
// execution, and projected result cells borrow its bytes.
func FromSegment(s *store.Segment) Source {
	return Source{kind: sourceSegment, payload: unsafe.Pointer(s)}
}

var sqlUnit struct {
	once    sync.Once
	segment store.Segment
}

// sourceIndependentSQLSource lazily builds the immutable one-row relation used
// only by a FROM-less SQL SELECT. Keeping it private preserves Source{} as an
// invalid caller value while letting the statement layer run its ordinary
// scalar plan over exactly one input row. Laziness keeps ordinary builder-only
// users from paying for an SQL-only Segment.
func sourceIndependentSQLSource() Source {
	sqlUnit.once.Do(func() {
		if _, err := sqlUnit.segment.Append([]byte("{}")); err != nil {
			panic("query: invalid internal SQL unit document: " + err.Error())
		}
	})
	return FromSegment(&sqlUnit.segment)
}

// fromRelationSpool binds the private statement-owned columnar relation
// source. It is intentionally unexported: callers cannot provide the lifetime
// and ownership guarantees Statement establishes before returning this handle.
func fromRelationSpool(s *relationSpool) Source {
	return Source{kind: sourceRelationSpool, payload: unsafe.Pointer(s)}
}

// FromSnapshot names an immutable heap [store.Snapshot]. Declared exact
// indexes are bound from that snapshot's catalog at execution time, so a Query
// compiled before an online index creation can use the index as soon as it is
// Ready without being recompiled. Candidate predicates combine native
// stable-slot masks and only surviving rows are decoded, before exact
// predicate recheck and late projection, grouping, aggregation, ordering, and
// limiting.
func FromSnapshot(s store.Snapshot) Source {
	return Source{kind: sourceHeapSnapshot, heap: s}
}

// FromFile names a page-backed [durable.Snapshot]. The snapshot stays owned by
// the caller; execution reads it in bounded parallel batches under
// [Exec.Options] and copies every selected value into Result-owned storage, so
// result cells survive snapshot close and page eviction.
func FromFile(s *durable.Snapshot) Source {
	return Source{kind: sourceFileSnapshot, file: s}
}

// FileOverlay is the bounded staged-write layer over a durable snapshot.
//
// Lookup is called once for each row in the base snapshot. shadowed reports
// whether the overlay owns the key; present then distinguishes a replacement
// from a delete. RangeInserts visits only present documents whose keys were
// absent from the base snapshot, because replacements are emitted in the base
// row's position during a full scan. RangePresent visits every present staged
// document; a candidate-bounded scan uses it after suppressing shadowed base
// candidates, so a replacement that entered the predicate is considered
// exactly once. LenDelta is the signed difference between overlay-visible and
// base rows.
//
// Implementations may lend key and value bytes only for the duration of each
// call. Query execution copies every admitted document into its bounded batch
// storage before returning to the overlay.
type FileOverlay interface {
	Lookup(key []byte) (value []byte, present, shadowed bool)
	RangeInserts(visit func(value []byte) error) error
	RangePresent(visit func(value []byte) error) error
	LenDelta() int64
}

// FileOverlaySource binds a reusable overlay implementation to Sources. Its
// zero value is unbound. Keep it alive and unchanged until query execution
// returns.
//
// The holder exists so Source can retain one pointer rather than one interface:
// that preserves the size and allocation cost of every non-overlay Source.
type FileOverlaySource struct {
	overlay FileOverlay
}

// NewFileOverlaySource returns a holder bound to overlay.
func NewFileOverlaySource(overlay FileOverlay) FileOverlaySource {
	return FileOverlaySource{overlay: overlay}
}

// Bind replaces the holder's overlay. Binding nil makes it invalid until a
// non-nil overlay is installed.
func (s *FileOverlaySource) Bind(overlay FileOverlay) {
	if s != nil {
		s.overlay = overlay
	}
}

// FromFileOverlay names one page-backed snapshot plus a bounded staged-write
// layer. The durable executor merges them in one pass: replacements shadow
// their base row, deletes suppress it, and inserts follow the base rows.
// Persistent candidate masks remain useful: shadowed base candidates are
// suppressed and every present staged document is exact-evaluated separately,
// preserving O(base candidates + staged writes) work without trusting a
// base-only posting as the final answer. Covering shortcuts are bypassed because
// their aggregates describe the base generation alone.
//
// The snapshot and overlay stay owned by the caller and must remain valid until
// execution returns. A nil overlay is rejected; callers with no writes use
// [FromFile], which retains the ordinary durable fast path unchanged.
func FromFileOverlay(s *durable.Snapshot, overlay *FileOverlaySource) Source {
	return Source{
		kind: sourceFileOverlay, file: s,
		payload: unsafe.Pointer(overlay),
	}
}

// FromSnapshotOverlay names one heap [store.Snapshot] plus a bounded
// staged-write layer. It is the Memory-profile dual of [FromFileOverlay]:
// overlay hits shadow the snapshot row, inserts append after the base rows,
// and [FileOverlay.LenDelta] corrects visible cardinality. Joins and
// correlated subqueries are rejected the same way [FromSnapshot] rejects them
// — there is no catalog behind this Source.
//
// The snapshot and overlay stay owned by the caller and must remain valid until
// execution returns. A nil overlay is rejected; callers with no writes use
// [FromSnapshot], which retains the ordinary heap fast path unchanged.
func FromSnapshotOverlay(s store.Snapshot, overlay *FileOverlaySource) Source {
	return Source{
		kind: sourceSnapshotOverlay, heap: s,
		payload: unsafe.Pointer(overlay),
	}
}

// FromDatabase names collection as the driving side of a query executed
// against catalog, a [store.DatabaseSnapshot] capturing every collection at one
// instant. Execution over the driving collection is exactly [FromSnapshot]'s;
// what the catalog adds is the ability to resolve a join clause's inner
// collection at that same instant.
//
// This is the heap Source a query with a join accepts. The durable counterpart
// is [FromFileDatabase]. Requiring one of those catalog snapshots is the reason
// snapshot skew across a join is not expressible rather than merely
// discouraged. Two separately taken snapshots can disagree — one collection
// observed before a commit and another after it — and a join resolving
// references against a state its driving side never saw would return rows that
// were never simultaneously true. Taking the catalog and the driving name
// together makes the consistent cut the only thing there is to pass.
//
// A query with no join is unaffected and may use whichever Source is
// convenient.
func FromDatabase(catalog store.DatabaseSnapshot, collection string) Source {
	return Source{kind: sourceDatabase, catalog: catalog, name: collection}
}

// FromFileDatabase names collection as the driving side of a query executed
// against catalog, a [durable.DatabaseSnapshot] capturing every collection in a
// durable database at one instant. It is [FromDatabase] for the persistent
// backend: execution over the driving collection is exactly [FromFile]'s, and
// what the catalog adds is the ability to resolve a join clause's inner
// collection at that same instant.
//
// It is the only durable Source a query with a join accepts, for the reason
// [FromDatabase] gives — two separately taken durable snapshots can disagree,
// and a join resolving references against a state its driving side never saw
// would return rows that were never simultaneously true. Taking the catalog and
// the driving name together makes the consistent cut the only thing there is to
// pass.
//
// The caller owns the catalog and must close it; every result cell is copied
// into Result-owned storage before execution returns, exactly as [FromFile]'s
// are, so a Result outlives the snapshots it was read from.
func FromFileDatabase(catalog durable.DatabaseSnapshot, collection string) Source {
	return Source{kind: sourceFileDatabase, files: catalog, name: collection}
}

// An Exec is the caller-owned storage one execution stream runs against: the
// destination Result, the transient Workspace, the bounded execution Options,
// and the Stats only backends that measure physical work write. The zero Exec
// is ready to use.
//
// An Exec is single-consumer, exactly like the Result and Workspace it
// contains. It holds the high-water buffers that make a warmed [Query.RunInto]
// allocation-free, so two goroutines sharing one Exec would interleave writes
// into a single set of column, posting, group, and result buffers. A compiled
// Query stays safe for concurrent execution; give each goroutine its own Exec.
//
// Result cells produced from a Segment or a heap snapshot borrow that
// collection and this Exec's Workspace, and stay valid until the collection is
// modified or the next RunInto reuses this Exec. Cells produced from a durable
// snapshot are copied into Result-owned storage instead, because the page
// frames they were read from may be evicted as soon as the snapshot is closed.
type Exec struct {
	// Result is the execution destination. RunInto overwrites it while keeping
	// its column and cell capacity.
	Result Result
	// Workspace is the transient scan, predicate, grouping, and ordering
	// storage for every backend, including the durable backend's index
	// planning. Retaining it across calls is what makes the steady state
	// allocation-free.
	Workspace Workspace
	// Options tune bounded execution. Workers and MemoryBytes apply to every
	// backend: durable execution uses MemoryBytes as its batch/merge frontier,
	// while Segment and heap-snapshot execution admit their row-proportional
	// columns, selections, ordering/grouping state, decoded text, and join
	// memberships against it before growth. The durable frontier is a target,
	// not a total resident-capacity quota over fixed worker/ring storage;
	// data-dependent planner, join, and containment additions still fail
	// admission explicitly. BatchRows, BatchBytes, SpillDirectory, and
	// SpillBytes are durable-only; result, aggregate, and fan-out pair limits
	// apply wherever their corresponding operation runs.
	Options ExecOptions
	// Stats reports the physical work the last execution performed. Only
	// backends that measure a field write it, and every execution resets the
	// whole struct first, so a previous durable execution's numbers can never be
	// read back as if they described a Segment scan — and the join fields a heap
	// execution does write describe that execution alone.
	Stats ExecStats

	// file is the durable backend's persistent-index planning storage. It is
	// unexported because it has no caller-serviceable parts: unlike Options it
	// is not configuration, and unlike Result it is not output. Folding it in
	// here is what lets the durable entry point drop the separate workspace
	// handle its options used to carry, and it shares the general Workspace for
	// candidate planning rather than nesting a second copy of one.
	file fileWorkspace

	// overlayDocs is the retained materialization scratch [FromSnapshotOverlay]
	// refills before handing the merged view to the Segment executor. Keeping
	// it on Exec is what makes a warmed overlay read reuse Segment capacity
	// instead of allocating a fresh collection on every RunInto.
	overlayDocs  store.Segment
	overlayMerge snapshotOverlayMerge
}

// Release drops every buffer e retains, returning it to its zero state.
// Reusing a warm Exec normally gives better throughput, since the retained
// high-water capacity is exactly what makes a steady-state execution
// allocation-free; Release is for after one unusually large execution whose
// working set should not stay pinned for the rest of e's lifetime. It also
// drops the document bytes and decoded text the last Result borrowed, letting
// the collection those views point into be collected.
func (e *Exec) Release() {
	if e == nil {
		return
	}
	e.Result.Release()
	e.Workspace.Release()
	e.file.release()
	e.overlayDocs = store.Segment{}
	e.overlayMerge = snapshotOverlayMerge{}
	e.Stats = ExecStats{}
}

// Run executes q over src and returns the column-oriented result, compiling q
// on first use. It owns the transient storage for the call, which makes it the
// form to reach for when a query runs once; [Query.RunInto] is the form for a
// hot loop and the only one that reports [ExecStats]. Run does not modify src
// and may be called concurrently on one compiled Query, each call using its
// own execution state.
func (q *Query) Run(src Source) (Result, error) {
	var e Exec
	err := q.RunInto(&e, src)
	// A durable execution parks a scanner and one worker per configured worker
	// in the Exec, so that a hot loop through RunInto starts none. Run has no
	// second call to amortize them against and no Release to retire them with —
	// it returns the Result and drops the Exec — so it retires them itself.
	// Leaving that to the cleanup registered against an abandoned Exec would
	// make a loop of one-shot queries accumulate parked goroutines until a
	// collection it does not allocate enough to provoke. The Result is
	// unaffected: a durable execution's cells own their bytes.
	e.file.stopPool()
	return e.Result, err
}

// RunInto executes q into e's caller-owned storage. After one warm-up, an
// execution over a Segment or a heap snapshot whose row count, posting
// frontier, decoded text, and group count fit e's retained high-water marks
// allocates no heap memory at all — stable ordering, grouping, containment
// indexing, typed aggregates, and escaped-string decoding included. Use
// [Cell.AppendJSON] with a retained destination to consume computed aggregates
// without allocating either. Exact decimal aggregate state and encodings share
// the whole-execution [ExecOptions.AggregateBytes] budget.
//
// RunInto overwrites e.Result and e.Stats and leaves e.Options alone. It
// rejects the zero Source, which names no collection at all: a query that
// forgets its source must fail rather than report an empty result.
func (q *Query) RunInto(e *Exec, src Source) (err error) {
	return q.runInto(e, src, nil)
}

// runIntoCorrelations is the private APPLY execution boundary. values is one
// immutable tuple for this synchronous call; the public builder path always
// passes nil and therefore cannot manufacture correlation state.
func (q *Query) runIntoCorrelations(
	e *Exec,
	src Source,
	values []scalar,
) error {
	return q.runInto(e, src, values)
}

func (q *Query) runInto(e *Exec, src Source, correlations []scalar) (err error) {
	if e == nil {
		return fmt.Errorf("query: RunInto requires a non-nil Exec")
	}
	// Invalidate the previous logical result immediately. If any later phase
	// fails, abortResult clears its borrowed cell references while preserving
	// capacity for reuse; callers can never mistake an earlier success for the
	// result of this failed execution.
	e.Result.RowCount = 0
	e.Stats = ExecStats{}
	defer func() {
		// Candidate planning retains only reusable scratch. Unbind its borrowed
		// durable snapshot at the call boundary so a warm Exec never keeps a
		// closed collection graph alive between executions.
		e.file.index.Reset(nil)
		if err != nil {
			e.Result.abortResult()
			if errors.Is(err, ErrCanceled) ||
				e.Options.Cancel != nil && e.Options.Cancel.Canceled() {
				// A canceled bind can leave earlier clauses fully bound. Drop
				// every borrowed inner snapshot and worker evaluator binding
				// now, rather than pinning them until this Exec happens to run
				// again. Checking the flag as well as the returned error covers
				// a durable I/O failure that correctly wins error precedence
				// after cancellation was observed. Retained owned capacity
				// remains warm.
				e.Workspace.clearBorrowedViews()
				e.Workspace.resetJoinBindings()
				e.Workspace.resetMarkBindings()
				e.file.abort()
			}
		}
	}()
	e.Workspace.clearBorrowedViews()
	for i := range e.file.workers {
		e.file.workers[i].clearBorrowedViews()
	}
	e.Workspace.cancel = e.Options.Cancel
	if err := e.Workspace.checkCanceled(); err != nil {
		return err
	}
	e.Workspace.resetJoinBindings()
	e.Workspace.resetMarkBindings()
	p, err := q.compiled()
	if err != nil {
		return err
	}
	if len(correlations) != p.correlationSlots {
		return fmt.Errorf(
			"query: compiled plan requires %d correlation slot(s), got %d",
			p.correlationSlots, len(correlations),
		)
	}
	if err := e.Workspace.bindCorrelations(correlations); err != nil {
		return err
	}
	resultRows, resultBytes, limitErr := normalizeResultBudget(e.Options)
	if limitErr != nil {
		return limitErr
	}
	e.Result.beginResultBudget(resultRows, resultBytes)
	if p.hasAggregate {
		aggregateBytes, limitErr := normalizeAggregateBytes(e.Options)
		if limitErr != nil {
			return limitErr
		}
		e.Workspace.aggregateBudget.begin(aggregateBytes)
		e.Workspace.aggregateOut = e.Workspace.aggregateOut[:0]
	}
	e.Workspace.heapWorkParent = nil
	switch src.kind {
	case sourceSegment, sourceHeapSnapshot, sourceSnapshotOverlay, sourceDatabase, sourceRelationSpool:
		memoryBytes, limitErr := normalizeHeapMemoryBytes(e.Options)
		if limitErr != nil {
			return limitErr
		}
		e.Workspace.heapWorkBudget.begin(memoryBytes)
	default:
		e.Workspace.heapWorkBudget.disable()
	}
	e.Workspace.eval.setWork(e.Workspace.activeHeapWorkBudget())
	if err := e.Workspace.buildCorrelationNeedles(); err != nil {
		return err
	}
	switch src.kind {
	case sourceSegment:
		docs := (*store.Segment)(src.payload)
		if docs == nil {
			return fmt.Errorf("query: FromSegment was given a nil Segment")
		}
		if err := rejectJoins(p, "FromSegment"); err != nil {
			return err
		}
		e.Stats = ExecStats{}
		return p.runInto(&e.Result, docs, &e.Workspace, e.Options.Workers)
	case sourceRelationSpool:
		spool := (*relationSpool)(src.payload)
		if spool == nil {
			return fmt.Errorf("query: relation spool source is nil")
		}
		if err := rejectJoins(p, "relation spool"); err != nil {
			return err
		}
		e.Stats = ExecStats{}
		err := p.runRelationInto(&e.Result, spool, &e.Workspace)
		if err == nil {
			e.Stats.JoinBuilds += spool.joinStats.builds
			e.Stats.JoinBuildRows += spool.joinStats.buildRows
			e.Stats.JoinPairs += spool.joinStats.pairs
		}
		return err
	case sourceHeapSnapshot:
		if err := rejectJoins(p, "FromSnapshot"); err != nil {
			return err
		}
		e.Stats = ExecStats{}
		return p.runSnapshotInto(e, src.heap, store.DatabaseSnapshot{})
	case sourceFileSnapshot:
		if err := rejectJoins(p, "FromFile"); err != nil {
			return err
		}
		return p.runFileInto(e, src.file, durable.DatabaseSnapshot{})
	case sourceFileOverlay:
		if err := rejectJoins(p, "FromFileOverlay"); err != nil {
			return err
		}
		var overlay FileOverlay
		if src.payload != nil {
			overlay = (*FileOverlaySource)(src.payload).overlay
		}
		return p.runFileOverlayInto(e, src.file, overlay)
	case sourceSnapshotOverlay:
		if err := rejectJoins(p, "FromSnapshotOverlay"); err != nil {
			return err
		}
		var overlay FileOverlay
		if src.payload != nil {
			overlay = (*FileOverlaySource)(src.payload).overlay
		}
		e.Stats = ExecStats{}
		return p.runSnapshotOverlayInto(e, src.heap, overlay)
	case sourceFileDatabase:
		driving, ok := src.files.Collection(src.name)
		if !ok {
			return fmt.Errorf(
				"query: FromFileDatabase names collection %q, which the database snapshot does not hold",
				src.name)
		}
		if driving == nil {
			return p.runEmptyFileDatabaseInto(e, src.files)
		}
		return p.runFileInto(e, driving, src.files)
	case sourceDatabase:
		driving, ok := src.catalog.Collection(src.name)
		if !ok {
			return fmt.Errorf(
				"query: FromDatabase names collection %q, which the database snapshot does not hold",
				src.name)
		}
		e.Stats = ExecStats{}
		return p.runSnapshotInto(e, driving, src.catalog)
	default:
		return fmt.Errorf(
			"query: the zero Source names no collection; " +
				"build one with FromSegment, FromSnapshot, FromFile, FromFileOverlay, " +
				"FromSnapshotOverlay, FromDatabase, or FromFileDatabase",
		)
	}
}

// runEmptyFileDatabaseInto executes a cataloged durable collection whose
// representation is nil because no durable file has ever been allocated for
// it. This state is distinct from an absent collection: the source dispatch
// above proves the driving name is present, and this method proves every join
// dependency is present before taking the zero-row path.
//
// No join side is read because an empty driving relation cannot produce a pair
// or invoke a membership leaf. The ordinary heap zero-snapshot reducer remains
// authoritative for aggregate-over-empty and projection schema semantics.
func (p *plan) runEmptyFileDatabaseInto(
	e *Exec,
	catalog durable.DatabaseSnapshot,
) error {
	e.Result.fileData = e.Result.fileData[:0]
	e.Stats = ExecStats{}
	if err := e.Workspace.checkCanceled(); err != nil {
		return err
	}
	n, err := normalizeFileOptions(e.Options)
	if err != nil {
		return err
	}
	for i := range p.joins {
		if _, ok := catalog.Collection(p.joins[i].collection); !ok {
			return fmt.Errorf(
				"query: join: collection %q is not in the database snapshot",
				p.joins[i].collection,
			)
		}
	}
	if err := p.validateFileMarkDependencies(catalog); err != nil {
		return err
	}
	e.Workspace.joinPairBudget.configure(-1)
	for len(e.Workspace.joins) < len(p.joins) {
		e.Workspace.joins = append(e.Workspace.joins, joinBinding{})
	}
	e.Workspace.eval.bindTo(e.Workspace.joins[:len(p.joins)])
	e.Workspace.eval.bindMarks(nil)
	e.Stats = ExecStats{Workers: n.workers}
	err = p.runSnapshotRows(
		&e.Result, store.Snapshot{}, store.DatabaseSnapshot{},
		&e.Workspace, n.workers,
	)
	e.Workspace.collectJoinStats(p, &e.Stats)
	return err
}

// rejectJoins refuses a join plan on a Source that names one collection. There
// is no catalog behind such a Source to resolve the inner collection from, and
// resolving it from a second, independently taken snapshot is precisely the
// skew a join must not be able to express — so this is a hard rejection rather
// than a fallback that would quietly answer a different question.
func rejectJoins(p *plan, constructor string) error {
	if len(p.joins) == 0 && len(p.marks) == 0 {
		return nil
	}
	if len(p.joins) == 0 {
		return fmt.Errorf(
			"query: this query has a correlated subquery over collection %q, so both sides must come from one "+
				"database snapshot; build the Source with FromDatabase or FromFileDatabase instead of %s",
			p.marks[0].collection, constructor,
		)
	}
	return fmt.Errorf(
		"query: this query joins collection %q, so both sides must come from one "+
			"database snapshot; build the Source with FromDatabase or "+
			"FromFileDatabase instead of %s",
		p.joins[0].collection, constructor)
}
