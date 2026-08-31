package query

import (
	"math/bits"
	"runtime"
	"sync"

	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibejson"
)

// Parallel filter phase for the in-memory backends.
//
// Only the filter phase is split. That is not a staging decision, it is what
// makes the parallelism free of ordering risk: the phase's whole output is a
// selection of row ordinals, workers own disjoint ascending ranges of those
// ordinals, and concatenating their selections in range order reproduces
// exactly the order one goroutine would have produced. Everything after the
// selection — the late gather, ordering, grouping, aggregation, and result
// materialization — stays on the calling goroutine and is untouched, so
// ORDER BY stability and the source-ordinal ordering of an unordered
// projection are the same guarantees they were before.
//
// The scalar columns are the one thing workers share, and they share them by
// writing disjoint index ranges of one pre-sized column. That is not a race:
// distinct elements of a slice are distinct memory, and no worker reads
// another's range. Keeping the columns shared is what lets the late phase's
// compaction work on one dense column, rather than having to stitch per-worker
// columns back together in a pass that would cost what the split saved.

// A scanWorker is one goroutine's share of a parallel filter phase.
//
// Every mutable thing the phase touches is per worker: the shape cache, the raw
// gather buffers, the snapshot address list, the decoded-text arena, the
// containment index scratch, and the local selection. That is not contention
// avoidance, it is the [Workspace] contract — a Workspace is single-consumer,
// and sharing any one of these would be a data race rather than a slow path.
// The buffers live in the Workspace across executions for the same reason every
// other buffer does: a warmed execution must allocate nothing, and allocating
// per worker per batch would put the allocation back one level down.
type scanWorker struct {
	// The job. It is stored on the worker rather than passed to it because a
	// parked worker is woken by a bare channel send, which carries nothing and
	// allocates nothing, while handing it arguments would mean boxing them into
	// a fresh closure per execution.
	plan      *plan
	ctx       *execCtx
	snapshot  store.Snapshot
	rows      []int            // segment: the scan's row ordinals
	addresses []store.Location // snapshot: a compacted candidate's addresses
	masks     []store.Mask     // snapshot: the live universe to expand from

	cache    store.ShapeCache
	raws     [][]vibejson.RawValue
	locs     []store.Location
	text     []byte
	eval     evalScratch
	selected []int
	work     *heapWorkBudget
	cancel   *CancelFlag
	// lo and hi are this worker's half-open range of scan row ordinals;
	// maskLo and maskHi are the live-mask window covering it, for a snapshot
	// scan that has to expand its own addresses.
	lo, hi         int
	maskLo, maskHi int
	err            error
}

// parallelScanMinRows is the smallest scan the default policy will split, and
// parallelScanRowsPerWorker is how much of one a worker must be given before
// another is added. Below the minimum, starting and joining goroutines costs
// more than the scan does, and a split is a straight regression on the small
// queries an interactive workload is mostly made of.
//
// BenchmarkSegmentParallel and BenchmarkNarrowRowParallel establish the
// crossover across both a cheap narrow row and a wider row. The policy is set
// by the cheaper corpus, where fixed split/join cost is most visible. One
// thousand rows split four ways is the first conservative point that is a win
// whatever a row costs, which is what a default has to be.
const (
	parallelScanMinRows       = 1024
	parallelScanRowsPerWorker = 256
)

// scanWorkerCount is the parallelism policy. A caller's explicit request wins
// outright, including a request for one, so a caller who has measured their own
// workload is never overridden. Otherwise the default is conservative by
// construction: it stays at one until the scan is large enough for the split to
// pay, then grows by whole worker-sized slices of work, and never past the
// processors the program is actually allowed to use.
func scanWorkerCount(rows, requested int) int {
	if requested > 0 {
		if requested > rows {
			return max(rows, 1)
		}
		return requested
	}
	if rows < parallelScanMinRows {
		return 1
	}
	workers := rows / parallelScanRowsPerWorker
	if procs := runtime.GOMAXPROCS(0); workers > procs {
		workers = procs
	}
	return max(workers, 1)
}

// A scanPool is a Workspace's parked filter-phase workers, one goroutine each,
// created on the first split scan and reused by every later one.
//
// They are parked rather than started per execution because starting a
// goroutine is not free: when neither the running processor's free list nor the
// scheduler's has a goroutine to hand back, the runtime allocates one on a path
// whose warmed contract is allocation-free — the more so the shorter the scan,
// because a fast scan cycles goroutines faster than the free lists refill.
//
// The pool is a separate object from the Workspace, and the workers hold no
// pointer back into one between scans, which is what start's release loop is
// for. That is what lets a Workspace nobody released still be collected: the
// cleanup registered against it can only run while the parked goroutines are
// not themselves keeping it alive.
type scanPool struct {
	workers []scanWorker
	wake    []chan struct{}
	done    sync.WaitGroup
}

// scanPoolFor returns this Workspace's pool sized for workers, starting any
// goroutine the pool does not have yet. Widths are a high-water mark like every
// other buffer here: a pool that once served sixteen keeps sixteen parked.
func (w *Workspace) scanPoolFor(workers int) *scanPool {
	if w.pool == nil {
		w.pool = new(scanPool)
		// A Workspace is normally released, and Release stops the pool. This
		// covers the one that is not: without it an abandoned Workspace would
		// leak its parked goroutines for the life of the program. The cleanup
		// closes over the pool alone, never the Workspace, or it would keep
		// alive the very object whose collection is meant to trigger it.
		runtime.AddCleanup(w, (*scanPool).stop, w.pool)
	}
	pool := w.pool
	pool.workers = resize(pool.workers, workers)
	for i := len(pool.wake); i < workers; i++ {
		// Buffered by one so waking a worker never blocks the driver behind
		// that worker's scheduling latency, which would serialize the starts it
		// is trying to overlap.
		wake := make(chan struct{}, 1)
		pool.wake = append(pool.wake, wake)
		go pool.serve(i, wake)
	}
	return pool
}

// serve is one parked worker's whole life: wait to be woken, run its share,
// report it finished. It exits when stop closes its channel.
func (pool *scanPool) serve(at int, wake chan struct{}) {
	for range wake {
		pool.workers[at].filter()
		pool.done.Done()
	}
}

// stop retires every parked goroutine. It is idempotent, because a Workspace
// may be released more than once and the cleanup may still be pending when it
// is.
func (pool *scanPool) stop() {
	for _, wake := range pool.wake {
		close(wake)
	}
	pool.wake = nil
}

// releaseScanPool retires the parked workers before the Workspace that owns
// them is reset. It runs before the reset, not after, because the reset is what
// makes the pool unreachable.
func (w *Workspace) releaseScanPool() {
	if w.pool != nil {
		w.pool.stop()
		w.pool = nil
	}
}

// split lays out one worker per contiguous ascending row range, sized so the
// remainder is spread over the leading ranges rather than piled onto the last
// one.
func (pool *scanPool) split(rows, workers int) []scanWorker {
	scan := pool.workers[:workers]
	span, extra, at := rows/workers, rows%workers, 0
	for i := range scan {
		size := span
		if i < extra {
			size++
		}
		scan[i].lo, scan[i].hi = at, at+size
		at += size
	}
	return scan
}

// start runs the workers, taking the first share on the calling goroutine —
// which has nothing to do but wait for it, so one fewer hand-off is one fewer
// scheduling latency on the critical path — and collects their selections in
// range order, which is scan order.
func (p *plan) start(w *Workspace, pool *scanPool, scan []scanWorker) ([]int, error) {
	pool.done.Add(len(scan) - 1)
	for i := 1; i < len(scan); i++ {
		pool.wake[i] <- struct{}{}
	}
	scan[0].filter()
	pool.done.Wait()

	selected := w.selected[:0]
	var err error
	for i := range scan {
		if scan[i].err != nil && err == nil {
			err = scan[i].err
		}
		selected = append(selected, scan[i].selected...)
		// The job references a whole collection and a whole plan. Dropping them
		// here keeps a Workspace from pinning the Segment or Snapshot of an
		// execution that finished long ago, the same discipline runGroupedInto
		// applies to the scalars it retains.
		scan[i].release()
	}
	w.selected = selected
	if err != nil {
		return nil, err
	}
	return selected, nil
}

// release drops every pointer the job gave this worker. It is what keeps a
// parked worker from holding its Workspace — and through it the collection and
// the plan of an execution that finished long ago — alive.
func (sw *scanWorker) release() {
	for i := range sw.raws {
		clear(sw.raws[i])
		sw.raws[i] = sw.raws[i][:0]
	}
	sw.plan, sw.ctx = nil, nil
	sw.snapshot = store.Snapshot{}
	sw.rows, sw.addresses, sw.masks = nil, nil, nil
	sw.work = nil
	sw.cancel = nil
	sw.eval.setWork(nil)
}

// filter runs this worker's share: gather its rows of every filter column into
// its own buffers, classify them into its own text arena and its own range of
// the shared columns, and reduce that range to the rows WHERE accepts.
func (sw *scanWorker) filter() {
	if sw.rows != nil {
		sw.filterSegment()
		return
	}
	sw.filterSnapshot()
}

// identityRows returns 0..n-1. It is grown once and never rewritten, because
// the identity permutation of a prefix is the same whatever the previous
// execution's size was — so a warmed Workspace pays nothing for it.
func (w *Workspace) identityRows(n int) []int {
	for i := len(w.identity); i < n; i++ {
		w.identity = append(w.identity, i)
	}
	return w.identity[:n]
}

// sizeFilterColumns pre-sizes the shared filter columns to the scan, so workers
// write into their own row ranges instead of appending — appending is what would
// make the shared column a race.
func (ctx *execCtx) sizeFilterColumns(p *plan, w *Workspace) {
	w.raws = resize(w.raws, len(p.valuePaths))
	ctx.values = resize(ctx.values, len(p.valuePaths))
	for _, c := range p.filterCols {
		ctx.values[c] = resize(ctx.values[c], ctx.rows)
	}
}

// selectSegmentParallel runs the filter phase over a Segment across workers and
// returns the merged selection.
func (p *plan) selectSegmentParallel(ctx *execCtx, w *Workspace, rows []int, workers int) ([]int, error) {
	ctx.sizeFilterColumns(p, w)
	pool := w.scanPoolFor(workers)
	scan := pool.split(ctx.rows, workers)
	for i := range scan {
		scan[i].plan, scan[i].ctx, scan[i].rows = p, ctx, rows
	}
	w.bindScanWorkers(scan)
	return p.start(w, pool, scan)
}

// bindScanWorkers gives each worker in this execution's share its own join
// probe scratch, pointed at the one set of bindings bindJoins produced. It runs
// on the calling goroutine before any worker is woken, which is what makes the
// bindings shared-read-only rather than shared-mutable: everything a probe
// writes lands in the per-worker scratch this installs.
//
// The used count is recorded because the pool is a high-water mark. A pool that
// once served sixteen workers keeps sixteen parked, and folding the tallies of
// workers this execution never woke would report a previous execution's probes
// as this one's.
func (w *Workspace) bindScanWorkers(scan []scanWorker) {
	for i := range scan {
		scan[i].work = w.activeHeapWorkBudget()
		scan[i].cancel = w.cancel
		scan[i].eval.setWork(scan[i].work)
		scan[i].eval.bindTo(w.joins)
		scan[i].eval.bindMarks(w.marks)
		scan[i].eval.bindCorrelations(w.correlations)
	}
	w.scanUsed = len(scan)
}

// eachScanWorker visits the workers this execution used, for the tallies that
// have to be summed across them after the phase has finished.
func (w *Workspace) eachScanWorker(fn func(*scanWorker)) {
	if w.pool == nil {
		return
	}
	for i := 0; i < w.scanUsed && i < len(w.pool.workers); i++ {
		fn(&w.pool.workers[i])
	}
}

func (sw *scanWorker) filterSegment() {
	p, ctx, rows := sw.plan, sw.ctx, sw.rows
	sw.err = nil
	sw.selected = sw.selected[:0]
	if err := cancellationError(sw.cancel); err != nil {
		sw.err = err
		return
	}
	sw.raws = resize(sw.raws, len(p.valuePaths))
	span := rows[sw.lo:sw.hi]
	textNeed := 0
	for at, c := range p.filterCols {
		if err := cancellationCheckpoint(sw.cancel, at); err != nil {
			sw.err = err
			return
		}
		cp := p.valuePaths[c]
		var raws []vibejson.RawValue
		var err error
		if cp.single {
			raws = sw.cache.AppendFieldRows(sw.raws[c][:0], ctx.s, span, cp.name)
		} else {
			raws, err = ctx.s.AppendPointerRows(sw.raws[c][:0], span, cp.pointer)
		}
		if err != nil {
			sw.err = err
			return
		}
		sw.raws[c] = raws
		escaped, err := escapedTextBytesCancelable(raws, sw.cancel)
		if err != nil {
			sw.err = err
			return
		}
		textNeed += escaped
	}
	if err := sw.classifyAndSelect(p, ctx, textNeed); err != nil {
		sw.err = err
	}
}

// selectSnapshotParallel is selectSegmentParallel over a heap snapshot. The
// split is by chunk rather than by row when the scan is not already compacted,
// because a snapshot row is addressed by chunk and stable slot: partitioning
// the live masks gives every worker a self-contained address range to expand,
// so building the addresses is parallel too rather than a serial pass over the
// whole collection performed only to enable one.
func (p *plan) selectSnapshotParallel(ctx *execCtx, w *Workspace, snapshot store.Snapshot, compact bool, workers int) ([]int, bool, error) {
	ctx.sizeFilterColumns(p, w)
	pool := w.scanPoolFor(workers)
	scan := pool.split(ctx.rows, workers)
	var masks []store.Mask
	if !compact {
		if err := w.checkCanceled(); err != nil {
			return nil, false, err
		}
		w.liveMasks = snapshot.AppendLiveMasks(w.liveMasks[:0])
		if err := w.checkCanceled(); err != nil {
			return nil, false, err
		}
		masks = w.liveMasks
		if !assignMaskSpans(scan, masks) {
			// The live universe and the scan disagree about how many rows
			// exist, which can only mean the snapshot changed shape underneath
			// this execution. Decline rather than address rows through a map
			// that is no longer true of them; the serial path reads the
			// collection the same way it always did.
			return nil, false, nil
		}
	}
	for i := range scan {
		scan[i].plan, scan[i].ctx = p, ctx
		scan[i].snapshot, scan[i].addresses, scan[i].masks = snapshot, w.storeRows, masks
	}
	w.bindScanWorkers(scan)
	selected, err := p.start(w, pool, scan)
	return selected, err == nil, err
}

// assignMaskSpans gives each worker the mask range covering its row range,
// reusing the workers' lo/hi row bounds as the cut points. It reports false if
// the masks do not describe exactly the rows the scan claims.
func assignMaskSpans(scan []scanWorker, masks []store.Mask) bool {
	at, ordinal := 0, 0
	for i := range scan {
		start := at
		for ordinal < scan[i].hi && at < len(masks) {
			ordinal += bits.OnesCount64(masks[at].Bits)
			at++
		}
		if ordinal != scan[i].hi {
			return false
		}
		scan[i].maskLo, scan[i].maskHi = start, at
	}
	return at == len(masks)
}

func (sw *scanWorker) filterSnapshot() {
	p, ctx, snapshot, masks := sw.plan, sw.ctx, sw.snapshot, sw.masks
	sw.err = nil
	sw.selected = sw.selected[:0]
	if err := cancellationError(sw.cancel); err != nil {
		sw.err = err
		return
	}
	sw.raws = resize(sw.raws, len(p.valuePaths))
	// A compacted scan already holds every candidate's address, so a worker
	// takes its slice of them; an uncompacted one expands its own mask window
	// instead, which is what keeps address building parallel rather than a
	// serial pass over the whole collection performed only to enable one.
	var span []store.Location
	if masks != nil {
		var err error
		sw.locs, err = expandMasks(sw.locs[:0], masks[sw.maskLo:sw.maskHi], sw.cancel)
		if err != nil {
			sw.err = err
			return
		}
		span = sw.locs
	} else {
		span = sw.addresses[sw.lo:sw.hi]
	}
	textNeed := 0
	for at, c := range p.filterCols {
		if err := cancellationCheckpoint(sw.cancel, at); err != nil {
			sw.err = err
			return
		}
		raws, err := snapshot.AppendPointerRows(sw.raws[c][:0], span, p.valuePaths[c].pointerForStore())
		if err != nil {
			sw.err = err
			return
		}
		sw.raws[c] = raws
		escaped, err := escapedTextBytesCancelable(raws, sw.cancel)
		if err != nil {
			sw.err = err
			return
		}
		textNeed += escaped
	}
	if err := sw.classifyAndSelect(p, ctx, textNeed); err != nil {
		sw.err = err
	}
}

// expandMasks writes one address per live slot, in the stable chunk/slot order
// that defines a snapshot scan's row numbering.
func expandMasks(dst []store.Location, masks []store.Mask, cancel *CancelFlag) ([]store.Location, error) {
	for i, mask := range masks {
		if err := cancellationCheckpoint(cancel, i); err != nil {
			return dst, err
		}
		for word := mask.Bits; word != 0; word &= word - 1 {
			dst = append(dst, store.Location{
				Chunk: mask.Chunk,
				Slot:  uint8(bits.TrailingZeros64(word)),
			})
		}
	}
	return dst, cancellationError(cancel)
}

// classifyAndSelect is the half of a worker's job that does not depend on where
// the raw values came from: classify them into this worker's own text arena,
// writing into this worker's range of the shared columns, then reduce that
// range to the rows WHERE accepts.
func (sw *scanWorker) classifyAndSelect(p *plan, ctx *execCtx, textNeed int) error {
	sw.text = sw.text[:0]
	if sw.work != nil {
		if err := sw.work.admitDecodedText(textNeed); err != nil {
			return err
		}
	}
	if cap(sw.text) < textNeed {
		sw.text = make([]byte, 0, growCap(cap(sw.text), textNeed))
	}
	if sw.cancel == nil {
		for _, c := range p.filterCols {
			col := ctx.values[c]
			for j, r := range sw.raws[c] {
				col[sw.lo+j] = classifyRawInto(r, &sw.text)
			}
		}
		if err := p.validateSQLJoinOuterDomains(
			ctx.values, sw.eval.binds, sw.lo, sw.hi, nil,
		); err != nil {
			return err
		}
		if err := p.validateSQLPathDomains(
			ctx.values, sw.eval.correlations, sw.lo, sw.hi, nil,
		); err != nil {
			return err
		}
		if out, ok := p.where.selectSpan(sw.selected, ctx.values, sw.lo, sw.hi); ok {
			sw.selected = out
			return nil
		}
		for row := sw.lo; row < sw.hi; row++ {
			if p.where.eval(ctx.values, row, &sw.eval) {
				sw.selected = append(sw.selected, row)
			}
		}
		return sw.eval.firstError()
	}
	for _, c := range p.filterCols {
		col := ctx.values[c]
		for j, r := range sw.raws[c] {
			if err := cancellationCheckpoint(sw.cancel, j); err != nil {
				return err
			}
			col[sw.lo+j] = classifyRawInto(r, &sw.text)
		}
	}
	if err := p.validateSQLJoinOuterDomains(
		ctx.values, sw.eval.binds, sw.lo, sw.hi, sw.cancel,
	); err != nil {
		return err
	}
	if err := p.validateSQLPathDomains(
		ctx.values, sw.eval.correlations, sw.lo, sw.hi, sw.cancel,
	); err != nil {
		return err
	}
	var err error
	sw.selected, err = selectSpanCancelable(
		p.where, sw.selected, ctx.values, sw.lo, sw.hi, &sw.eval, sw.cancel,
	)
	if err != nil {
		return err
	}
	return sw.eval.firstError()
}
