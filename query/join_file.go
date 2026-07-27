package query

import (
	"errors"
	"fmt"
	"math/bits"

	vibejson "github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/x/byteview"
	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
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

// # The thresholds, re-measured
//
// joinMembershipMax and joinBloomScanRatio were both measured against the heap
// backend, and neither number transfers by assumption. This is what
// BenchmarkDurableJoinCostModel measures on the durable one (Apple M4 Max, Go
// 1.26, warm page cache, 20,000 driving rows, ns per row, three-arm method
// identical to BenchmarkJoinCostModel's — probe cost is the joined scan minus
// the unjoined one):
//
//	inner collection   inner row scanned   outer row probed   ratio
//	           1,000               136.0              226.7     1.7
//	          10,000                59.0              278.3     4.7
//	         100,000                16.7              266.9    16.0
//
// The heap's corresponding table is flat in both columns — 44.8 to 52.4 ns to
// scan a row, 199.8 to 546.3 to probe one, break-even 4.5 to 10.4. The durable
// table is not flat, and the two reasons are worth separating because only one
// of them is real.
//
// The inner-scan column falls with size because it is not a marginal cost at
// small N. A durable execution has a fixed setup — a scanner goroutine, a
// worker pool, channels, a batch ring — and dividing that by a thousand rows
// charges each row a share of something no row caused. By 100,000 rows the
// fixed part has amortized and 18.3 ns is the honest marginal cost of reading
// one inner row: a page-cache hit, an append into the batch buffer, and a share
// of one columnar pass over a Segment. That is roughly 2.5x cheaper per row
// than the heap's gathered extraction, which is not a surprise — the durable
// inner side reads rows densely in scan order and never gathers at scattered
// addresses.
//
// The probe column is flat at about 244 ns and that is the real finding. A
// durable probe is a key-tree walk, a chunk-directory walk, a page admission,
// and a document copy-out, and none of those grow with the collection because
// all three structures are trees. On the heap the probe cost climbs with size
// as the hash structure stops fitting cache; here the page cache absorbs that.
//
// This table alone would suggest the durable break-even is about 16 at the size
// where it is trustworthy — well above the heap's 4.5 to 10.4 — and therefore
// that the shipped ratio of 4 is merely conservative here. That conclusion is
// wrong, and it is wrong in an instructive way: this table's inner-scan column
// is measured through the parallel batched executor, and the scan that actually
// builds a filter is serial, because a binding must exist before the pool is
// started. joinFileBloomScanRatio below measures the crossover directly instead
// of deriving it from these two columns, and that is the number that governs.
//
// joinMembershipMax needed no such re-derivation. It bounds how much memory a
// collected set may occupy and how many comparisons an outer row may cost, and
// both of those are properties of the set rather than of the storage the set
// was read from.

// joinFileBloomScanRatio is joinBloomScanRatio for a durable inner side: how
// many inner rows the binder will scan, per outer row, to build a semi-join
// reduction filter.
//
// It is a separate constant from joinBloomScanRatio because the measurement
// says it has to be, and the reason is structural rather than a matter of
// degree.
//
// BenchmarkDurableJoinFilterCrossover measures the crossover directly: 20,000
// driving rows against inner sides from 10,000 to 80,000 — the ratio this
// constant governs, swept — at 1% inner selectivity, with the filter forced on
// and forced off. Apple M4 Max, Go 1.26, warm, ns per driving row:
//
//	inner/outer   unfiltered   forced filter   filter is
//	        0.5        270.7            91.2     2.97x faster
//	        1.0        252.3           129.2     1.95x faster
//	        2.0        255.2           208.5     1.22x faster
//	        4.0        260.9           369.1     1.41x SLOWER
//
// The unfiltered arm is flat because it does not read the inner side at all;
// the filtered arm rises linearly because it reads all of it. Those two lines
// cross at a ratio of about 2.6. Four — the heap's constant — is past the
// crossing, which is why running the durable backend under it measured a loss
// even in the case most favourable to the filter: at 1% selectivity the filter
// survived and rejected 19,800 of 20,000 probes and still finished 1.27x slower
// warm and 1.56x slower cold. Rejecting 99% of the probes and losing anyway is
// the whole finding, and it says the probe being avoided is cheap relative to
// the scan being spent to avoid it.
//
// Two is the floor of the measured crossing, chosen the same way
// joinBloomScanRatio took the floor of its own measured range, and the table
// shows the window it keeps is real rather than a polite way of disabling the
// feature: a filter is still admitted at ratios up to 2, where it is worth
// between 1.22x and 2.97x.
//
// The expectation going in was the opposite of all of this — that a durable
// backend would favour the filter more than the heap does, because a filter
// rejection converts page I/O into a cache-line test. That reasoning is sound
// about the probe and wrong about the scan, and the scan is what dominates. The
// inner scan that builds a filter runs on the binding goroutine alone, because
// a binding has to exist before the driving scan starts and therefore before
// the worker pool is started. A standalone durable query over the same
// collection runs through that pool and reaches 16.7 ns per row
// (BenchmarkDurableJoinCostModel); the serial bind scan behind the table above
// costs roughly 140 ns per row, eight times more, and the entire difference is
// the parallelism the bind cannot use. The heap has the same serial bind, but a
// heap inner row costs about 45 ns to scan whether or not anything is parallel,
// so its ratio survives and this one does not.
//
// # The measurement to redo before touching this
//
// Parallelising the bind scan is what would change this constant, and it would
// change it a lot: at 16.7 ns per inner row instead of 140, the crossing moves
// from 2.6 to somewhere above 20, and every row of the table above becomes a
// filter win. That is a real opportunity rather than a hypothetical, because
// the rows the constant currently declines are exactly the ones a parallel bind
// would turn into large wins.
//
// It is deliberately not done here. The inner side would have to run through
// the same filePool the driving scan uses, and that pool's correctness rests on
// a balance argument over four reused channels — every credit taken is
// released, every batch queued is taken, every partial published is consumed —
// that a second job shape with a different output path and a sequential
// early-stop rule would have to be re-derived against rather than assumed. That
// is a change to freshly landed concurrency, and pairing it with a first
// implementation of durable joins in one step is how a silent imbalance gets
// introduced. The constant below is correct for the serial bind that exists.
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
// it reads, the index workspace its candidate probe reuses, and the batch
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
	// index is the persistent-index planning workspace for the inner side's own
	// candidate probe. It is per clause rather than shared with the driving
	// side's because the two probe different collections, and a workspace
	// carries the last probe's stats.
	index durable.IndexWorkspace

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

// reset returns the side to an unbound state while keeping its storage.
//
// The keys are cleared rather than truncated, the same discipline
// joinBinding.reset applies to every other borrowed view: a retained key string
// views keyText, and leaving stale ones reachable through the retained capacity
// would pin an arena the next execution is about to overwrite in place.
func (f *fileJoinSide) reset() {
	f.snapshot = nil
	f.data = f.data[:0]
	f.ends = f.ends[:0]
	clear(f.keys)
	f.keys = f.keys[:0]
	f.keyText = f.keyText[:0]
	f.batchRows = 0
}

// release drops every buffer the side retains.
func (f *fileJoinSide) release() {
	f.reset()
	f.index.Release()
	f.docs.Reset()
	f.data = nil
	f.ends = nil
	f.keys = nil
	f.keyText = nil
	f.overflow = nil
	f.masks = nil
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
	stats *ExecStats,
) error {
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
	defer func() { w.eval.bindTo(w.joins) }()
	w.storeIndexes = outer.AppendIndexes(w.storeIndexes[:0])
	for i := range p.joins {
		j := &p.joins[i]
		b := &w.joins[j.slot]
		if err := j.bindFile(b, catalog, limit, int(outer.Len()), ratio, stats); err != nil {
			return err
		}
		if b.mode == joinBindSet {
			b.buildNeedles(p.valuePaths[j.outerPath].indexPath(), w.storeIndexes)
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
	stats *ExecStats,
) error {
	b.reset()
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

	stoppable := j.innerPath == joinPrimaryKey
	overflowed, err := j.collectFile(b, inner, limit, outerRows, ratio, stoppable)
	if err != nil {
		return err
	}
	b.file.snapshot = inner
	b.plan = j.inner
	j.installStrategy(b, overflowed, stats)
	return nil
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
	if err := p.bindFileJoins(
		&e.Workspace, snapshot, catalog,
		e.Options.JoinMembershipMax, e.Options.JoinFilterScanRatio, &stats,
	); err != nil {
		return err
	}
	result, stats, err := p.runFileSnapshotBatched(e, snapshot, nil, n, stats)
	e.Result, e.Stats = result, stats
	if err != nil {
		return err
	}
	if probeErr := p.collectFileJoinStats(e); probeErr != nil {
		return probeErr
	}
	return nil
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
) (bool, error) {
	scan := &b.scan
	scan.candidateUsed = 0
	scan.storeMaskUsed = 0
	scan.text = scan.text[:0]
	f := &b.file

	// The inner side plans its candidates through the same durable entry point
	// the driving side uses, rather than calling the generic planner directly.
	// That is what gives the inner scan the chunk-summary pruning tier and the
	// narrow *durable.Snapshot capability statement for free — sourceCaps.zone
	// is an interface field, and handing it the five-pointer QuerySnapshot
	// instead of the bare snapshot would heap-box a copy once per bind.
	masks, err := j.inner.fileCandidateMasks(inner, &f.index, scan)
	if err != nil {
		return false, err
	}

	// The candidate count is exact in both branches, which is what the filter
	// decision needs. A bound scan popcounts the masks it will walk; an
	// unbounded one reads the snapshot's own live-row count. Neither is an
	// estimate, so joinBloomWorthwhile keeps comparing measured quantities
	// here exactly as it does on the heap.
	candidates := 0
	if masks != nil {
		for _, mask := range masks {
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

	needKeys := j.innerPath == joinPrimaryKey
	over := false
	appendRow := func(key, value []byte) error {
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
		stop, drainErr := j.drainFile(b, limit, stoppable)
		if drainErr != nil {
			return drainErr
		}
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
	stop, err := j.drainFile(b, limit, stoppable)
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
func (j *planJoin) drainFile(b *joinBinding, limit int, stoppable bool) (bool, error) {
	f := &b.file
	if f.batchRows == 0 {
		return false, nil
	}
	scan := &b.scan
	f.docs.Reset()
	start := 0
	for _, end := range f.ends {
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
	selected := j.inner.selectRows(ctx, nil, false, scan)

	stop := false
	for _, row := range selected {
		if b.overflowed {
			// Past the threshold the set is not grown further, but the scan is
			// still running because a filter is being built out of it, and a
			// filter that saw only some of the keys would produce false
			// negatives — the one error a Bloom filter must never have.
			b.bloom.insert(hashJoinKey(f.keys[row]))
			continue
		}
		if j.innerPath == joinPrimaryKey {
			b.appendString(f.keys[row])
		} else {
			b.appendValue(ctx.values[j.innerPath][row])
		}
		if !stoppable || len(b.lits) <= limit {
			continue
		}
		if !b.filtering {
			stop = true
			break
		}
		b.overflowed = true
		b.bloom.reset(b.candidates)
		for i := range b.lits {
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
	if cell.kind != kindString {
		return false // collection keys are strings; nothing else can name one
	}
	if b.bloom.active && !b.bloom.admits(hashJoinKey(cell.sval), pr) {
		return false
	}
	pr.probes++
	raw, ok, err := b.file.snapshot.AppendRaw(pr.raw[:0], cell.sval)
	pr.raw = raw
	if err != nil {
		if pr.err == nil {
			pr.err = err
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
		value, found, pointerErr := document.PointerCompiled(b.plan.valuePaths[i].pointer)
		if pointerErr != nil || !found {
			value = vibejson.RawValue{}
		}
		cols[i][0] = classifyRawInto(value, &pr.inner.text)
	}
	return b.plan.where.eval(cols, 0, &pr.inner)
}
