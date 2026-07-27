package query

import (
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
)

// A filePool is an [Exec]'s parked batched-execution goroutines and the
// channels they hand work across: one scanner, one worker per configured
// parallel worker, and the four channels between them. It is built on the first
// batched durable execution and reused by every later one.
//
// It exists for the reason scanPool does, and is built the same way. A batched
// execution used to mint five channels and start its scanner, its workers, and
// a partial-channel closer with `go` on every call. A channel is one or two
// allocations; `go f(a, b)` boxes its arguments into a fresh closure, so a
// goroutine start is one more; and the start itself allocates a whole g
// whenever neither the running processor's free list nor the scheduler's can
// supply one, which happens the more often the shorter the scan, because a fast
// scan cycles goroutines faster than the free lists refill. That scaffolding was
// the entire residual allocation of a warm durable execution — eighteen
// allocations and 2.3 kB that did not move with the corpus at all, being fixed
// per execution rather than per batch or per document.
//
// # Reusing channels means draining them
//
// A channel that outlives the execution that used it is only safe if it is
// empty when that execution ends, and empty for a reason rather than by
// observation: a leftover credit lets the next execution's scanner run further
// ahead of its consumer than the batch ring has slots for, and a leftover
// partial is consumed by the next execution as if it were its own batch. Either
// one returns a wrong answer silently. The argument that they are empty is:
//
//   - credits and jobs are written only by the scanner, once per published
//     batch, in that order and with no intervening branch that could publish one
//     without the other. So after the scan there are exactly out.batches credits
//     taken and out.batches batches queued.
//   - partials is written only by the workers, exactly once per batch they take
//     from jobs. Every queued batch is taken, because every worker keeps taking
//     until it sees the end-of-stream sentinel.
//   - the consumer receives exactly one partial and releases exactly one credit
//     per sequence, and it does not stop until nextSequence has reached
//     out.batches — the count the scanner reports, not a guess. So every partial
//     is received and every credit released.
//   - jobs also carries one sentinel per woken worker at the end, and each
//     worker takes exactly one and parks, so those balance too.
//   - scanDone carries exactly one value and the driver receives it.
//
// That covers the abort paths as well, which is the point of deriving it this
// way rather than from the happy path: an execution that stops early — a
// document that fails to index, a spill that fails to write — stops the scanner
// from publishing *more* batches, and never from balancing the ones it already
// published. Cancellation is a flag rather than a closed channel for exactly
// this reason: a closed channel cannot be reopened, and a cancellation that
// abandoned a queued batch would leave the imbalance this argument rules out.
//
// drained re-checks the conclusion after every execution rather than trusting
// it, because the failure it guards is silent.
type filePool struct {
	// job is the execution the woken goroutines serve. It lives here rather
	// than in the arguments of a `go` statement because those arguments are
	// what allocates, and it is cleared when the execution ends because a
	// parked goroutine holding a pointer into the Exec would keep the Exec —
	// and through it the snapshot and the compiled plan — alive forever, and
	// would defeat the cleanup that retires an abandoned pool.
	job fileJob

	jobs     chan fileBatch
	partials chan filePartial
	credits  chan struct{}
	scanDone chan fileScanResult

	wake     []chan struct{}
	scanWake chan struct{}

	// done counts the workers this execution woke that have not yet parked.
	done sync.WaitGroup
	// scanState is the scanner goroutine's working state. See serveScan.
	scanState fileScanState
	// dropped is the consumer's drop frontier: every batch sequence below it has
	// been consumed and had its rows released, by a spill or by an unordered
	// LIMIT's trim. Workers read it between batches to decide whether their row
	// arena can be rewound; see fileArena.rewindable. It is published by the
	// consumer after the drop, so the atomic's release/acquire pair is what
	// orders the release of the references before the storage is reused.
	dropped atomic.Uint64
	// stopped asks the scanner to stop publishing. It replaces the closed
	// channel the previous design broadcast on, which no reused pool could
	// offer twice. Nothing depends on the scanner noticing promptly: the
	// consumer drains until the scanner reports how many batches it published,
	// so a scanner that misses the flag for a few rows costs work, never
	// correctness or a deadlock.
	stopped atomic.Bool
}

// A fileJob is one batched execution's inputs, shared by the pool's goroutines.
// Every field is read-only to them except segments, whose element at index i is
// written only by worker i.
type fileJob struct {
	p        *plan
	snapshot *durable.Snapshot
	overlay  FileOverlay
	masks    []store.Mask
	overflow *[]byte
	slots    []fileSlot
	spaces   []Workspace
	segments []*store.Segment
	arenas   []fileArenaSet
	opts     normalizedFileOptions
	// active is how many workers this execution woke. The pool's goroutines are
	// a high-water mark, so the scanner must send exactly this many
	// end-of-stream sentinels — one per worker actually woken, not one per
	// worker that exists.
	active int
}

// poolFor returns this workspace's pool, sized for workers and with every
// goroutine it needs started.
func (w *fileWorkspace) poolFor(workers int) *filePool {
	if w.pool == nil {
		w.pool = new(filePool)
		// An Exec is normally released, and Release stops the pool. This covers
		// the one that is not: without it an abandoned Exec would leak a
		// scanner and every worker for the life of the program. The cleanup
		// closes over the pool alone, never the workspace, or it would keep
		// alive the very object whose collection is meant to trigger it — which
		// is also why the pool drops its job when an execution ends.
		runtime.AddCleanup(w, (*filePool).stop, w.pool)
	}
	pool := w.pool
	pool.size(workers)
	return pool
}

// size makes the pool ready to serve an execution of the given width.
//
// The three per-execution channels are rebuilt when the width changes rather
// than kept at a high-water capacity, because the credit channel's capacity is
// not a performance knob: it is the bound the batch ring's soundness rests on,
// and the ring is sized from it. Keeping a wider execution's credit channel
// would let a narrower one's scanner run further ahead than its own ring — and
// would also spend more memory on in-flight batches than the caller's
// MemoryBytes was divided up for. A width change is rare; a wrong answer is
// not recoverable.
func (pool *filePool) size(workers int) {
	if cap(pool.credits) != workers*2 {
		pool.jobs = make(chan fileBatch, workers)
		pool.partials = make(chan filePartial, workers)
		pool.credits = make(chan struct{}, workers*2)
	}
	if pool.scanDone == nil {
		pool.scanDone = make(chan fileScanResult, 1)
	}
	if pool.scanWake == nil {
		// Buffered by one so the driver never blocks behind the scanner's
		// scheduling latency, which would serialize the very overlap it is
		// starting the scanner for.
		pool.scanWake = make(chan struct{}, 1)
		go pool.serveScan(pool.scanWake)
	}
	for i := len(pool.wake); i < workers; i++ {
		wake := make(chan struct{}, 1)
		pool.wake = append(pool.wake, wake)
		go pool.serveWork(i, wake)
	}
}

// stopPool retires the workspace's parked goroutines, if it has any, without
// dropping the storage the rest of release does. It is for a one-shot execution,
// whose Result outlives the Exec but never points at the pool.
func (w *fileWorkspace) stopPool() {
	if w.pool != nil {
		w.pool.stop()
		w.pool = nil
	}
}

// stop retires every parked goroutine. It is idempotent, because an Exec may be
// released more than once and the cleanup may still be pending when it is.
func (pool *filePool) stop() {
	for _, wake := range pool.wake {
		close(wake)
	}
	pool.wake = nil
	if pool.scanWake != nil {
		close(pool.scanWake)
		pool.scanWake = nil
	}
}

// start hands the pool an execution and wakes exactly the goroutines it needs.
func (pool *filePool) start(job fileJob) {
	pool.job = job
	pool.stopped.Store(false)
	pool.dropped.Store(0)
	pool.done.Add(job.active)
	for i := range job.active {
		pool.wake[i] <- struct{}{}
	}
	pool.scanWake <- struct{}{}
}

// finish parks the pool after an execution and reports whether the channels
// came back empty. It must be called only once the driver has received the
// scanner's result and consumed every sequence it reported, which is the state
// in which the balance argument on filePool says all four channels are empty.
//
// The check is not an assertion about this package's present shape; it is the
// one place a future change that unbalances the protocol can be caught at all.
// Everything it would otherwise corrupt is a query result that still looks
// well-formed.
func (pool *filePool) finish() bool {
	pool.done.Wait()
	// Dropping the job before anything else is what lets a Workspace nobody
	// released still be collected: the parked goroutines must hold no pointer
	// back into the Exec between executions.
	pool.job = fileJob{}
	drained := len(pool.jobs) == 0 && len(pool.partials) == 0 &&
		len(pool.credits) == 0 && len(pool.scanDone) == 0
	if !drained {
		// Leave the channels empty even so. Reporting the fault and continuing
		// with poisoned channels would turn one wrong answer into every
		// subsequent one.
		pool.drain()
	}
	return drained
}

// drain empties every channel without blocking. It runs only after finish has
// found the protocol unbalanced, when by construction no goroutine is still
// sending.
func (pool *filePool) drain() {
	for {
		select {
		case <-pool.jobs:
		case <-pool.partials:
		case <-pool.credits:
		case <-pool.scanDone:
		default:
			return
		}
	}
}

// serveWork is one parked worker's whole life: wait to be woken, reduce batches
// until the scanner says there are no more, report itself parked. It exits when
// stop closes its channel.
func (pool *filePool) serveWork(at int, wake chan struct{}) {
	for range wake {
		pool.work(at)
		pool.done.Done()
	}
}

// work is one worker's share of one execution. Its locals — the plan, the scan
// workspace, the scan Segment — are dropped when it returns, which is what keeps
// a parked worker from pinning the execution that woke it.
func (pool *filePool) work(at int) {
	job := &pool.job
	p, slots := job.p, job.slots
	w := &job.spaces[at]
	// The worker's scan Segment is minted on the execution that first needs
	// this worker number and reset by every batch after, so a warm Exec
	// allocates none at all.
	if job.segments[at] == nil {
		job.segments[at] = &store.Segment{}
	}
	docs := job.segments[at]
	arenas := &job.arenas[at]
	for {
		batch := <-pool.jobs
		// The end-of-stream sentinel. flush never publishes a batch with no
		// rows, so an empty one cannot be mistaken for work; the scanner sends
		// exactly one per woken worker and each worker takes exactly one, which
		// is what keeps jobs balanced without a close the next execution could
		// not undo.
		if len(batch.ends) == 0 {
			return
		}
		// This batch packs into a generation the consumer has finished with if
		// there is one, and into a new one otherwise. The choice is made here,
		// on the owning goroutine and between batches, because no other
		// goroutine may touch a worker's storage.
		part := p.makeFilePartial(batch, w, docs, slots, arenas.begin(batch.seq, pool.dropped.Load()))
		if part.err != nil {
			pool.stopped.Store(true)
		}
		pool.partials <- part
	}
}

// serveScan is the parked scanner's whole life.
func (pool *filePool) serveScan(wake chan struct{}) {
	// The row callback is built once, here, and reused by every scan. It has to
	// be a closure because the snapshot range primitives take a func value, and
	// a closure built per scan is a heap allocation per execution — the last one
	// left on this path once the goroutines and channels stopped being minted.
	// Closing over the pool alone rather than over the scan's own locals is what
	// makes one instance serve every scan; the locals it used to capture live in
	// scanState instead, which is sound because exactly one scanner goroutine
	// ever exists.
	row := pool.appendRow
	overlayRow := pool.appendOverlayRow
	overlayCandidate := pool.appendOverlayCandidate
	overlayInsert := pool.appendOverlayInsert
	for range wake {
		pool.runScan(row, overlayRow, overlayCandidate, overlayInsert)
	}
}

// fileScanState is the scanner's per-execution working state. It lives on the
// pool rather than in the scanner's frame so the row callback can close over
// nothing but the pool; see serveScan.
type fileScanState struct {
	batch fileBatch
	out   fileScanResult
}

// runScan reads the snapshot into batches, publishes each under one credit, and
// reports what it did.
func (pool *filePool) runScan(
	row func(key, value []byte) error,
	overlayRow func(key, value []byte) error,
	overlayCandidate func(key, value []byte) error,
	overlayInsert func(value []byte) error,
) {
	job := &pool.job
	pool.scanState = fileScanState{batch: takeFileBatch(job.slots, 0, 0, job.opts)}
	var err error
	switch {
	case job.overlay != nil && job.masks != nil:
		*job.overflow, err = job.snapshot.RangeMasksRawBuffer(
			job.masks, (*job.overflow)[:0], overlayCandidate,
		)
		if err == nil {
			err = job.overlay.RangePresent(overlayInsert)
		}
	case job.overlay != nil:
		*job.overflow, err = job.snapshot.RangeRawReadAheadBuffer((*job.overflow)[:0], overlayRow)
		if err == nil {
			err = job.overlay.RangeInserts(overlayInsert)
		}
	case job.masks == nil:
		*job.overflow, err = job.snapshot.RangeRawReadAheadBuffer((*job.overflow)[:0], row)
	default:
		*job.overflow, err = job.snapshot.RangeMasksRawBuffer(job.masks, (*job.overflow)[:0], row)
	}
	if err == nil {
		err = pool.flush()
	}
	out := pool.scanState.out
	if out.err == nil {
		out.err = err
	}
	// One end-of-stream sentinel per worker this execution woke. It replaces
	// closing the jobs channel, which a reused channel cannot offer twice, and
	// it balances exactly: the scanner sends job.active of them and each woken
	// worker takes one and parks.
	for range job.active {
		pool.jobs <- fileBatch{}
	}
	pool.scanState = fileScanState{}
	pool.scanDone <- out
}

// flush publishes the batch under construction, if it has any rows.
func (pool *filePool) flush() error {
	st := &pool.scanState
	if len(st.batch.ends) == 0 {
		return nil
	}
	if pool.stopped.Load() {
		return errFileExecutionStopped
	}
	slots := pool.job.slots
	// The grown buffers go back into the slot before the batch is published: the
	// slice headers travelling down the jobs channel are a copy, so without this
	// the capacity every append just bought would be dropped on the floor the
	// moment the local is reassigned.
	slots[st.batch.seq%uint64(len(slots))].batch = st.batch
	// The credit and the batch are taken and sent with nothing between them that
	// can fail. That is what makes the two counts equal at the end of the scan,
	// which is what makes reusing both channels safe; see filePool. Both sends
	// can block — on the consumer's progress and on the workers' — and neither
	// can block forever, because the consumer drains until the scanner tells it
	// how many batches there were, and the scanner has not told it yet.
	pool.credits <- struct{}{}
	pool.jobs <- st.batch
	st.out.batches++
	st.batch = takeFileBatch(slots, st.batch.seq+1, st.out.rows, pool.job.opts)
	return nil
}

// appendRow accumulates one document into the batch under construction and
// publishes the batch when it reaches either bound.
func (pool *filePool) appendRow(_, value []byte) error {
	if pool.stopped.Load() {
		return errFileExecutionStopped
	}
	st := &pool.scanState
	st.batch.data = append(st.batch.data, value...)
	st.batch.ends = append(st.batch.ends, len(st.batch.data))
	st.batch.bytes += int64(len(value))
	st.out.rows++
	if len(st.batch.ends) > st.out.peakRows {
		st.out.peakRows = len(st.batch.ends)
	}
	if st.batch.bytes > st.out.peakBytes {
		st.out.peakBytes = st.batch.bytes
	}
	opts := pool.job.opts
	if len(st.batch.ends) >= opts.batchRows || st.batch.bytes >= opts.batchBytes {
		return pool.flush()
	}
	return nil
}

// appendOverlayRow merges one base row with the staged-write layer. It is a
// distinct callback so the ordinary scanner has no per-row overlay branch.
func (pool *filePool) appendOverlayRow(key, value []byte) error {
	if replacement, present, shadowed := pool.job.overlay.Lookup(key); shadowed {
		if !present {
			return nil
		}
		value = replacement
	}
	return pool.appendRow(key, value)
}

// appendOverlayCandidate emits only untouched base candidates. Every shadowing
// replacement is evaluated once through RangePresent after the base candidate
// pass, including replacements whose old value was not a candidate.
func (pool *filePool) appendOverlayCandidate(key, value []byte) error {
	if _, _, shadowed := pool.job.overlay.Lookup(key); shadowed {
		return nil
	}
	return pool.appendRow(key, value)
}

func (pool *filePool) appendOverlayInsert(value []byte) error {
	return pool.appendRow(nil, value)
}
