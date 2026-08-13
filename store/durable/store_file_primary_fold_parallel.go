package durable

import (
	"errors"
	"fmt"
	"runtime"
	"unsafe"

	"github.com/thesyncim/vibedb/internal/storeio"
)

const (
	// primaryNativeFoldMaxWorkers keeps the foreground fan-out small enough that
	// one checkpoint cannot monopolize the scheduler. Four independent 64 KiB
	// leaf images are enough to cover the expensive codec work while the serial
	// allocator and parent publication remain the ultimate throughput bound.
	primaryNativeFoldMaxWorkers = 4
	// The actual retained context is much smaller than this allowance (one
	// maximum leaf image, one builder, and 256 replacement descriptors). Charging
	// a worker per 8 MiB of configured residency deliberately prevents a small
	// collection from multiplying its non-cache scratch merely because the host
	// has a large GOMAXPROCS.
	primaryNativeFoldResidentBytesPerWorker = 8 << 20
)

// primaryNativeFoldContext is one bounded foreground codec lane. Its result is
// valid only until this context receives the next job. The materializer stages
// every successful image before reusing the wave, so at most one live image is
// owned by each context.
type primaryNativeFoldContext struct {
	page         []byte
	builder      *storeio.UnifiedPrimaryLeafBuilder
	replacements []storeio.CommonPrimaryUnifiedReplacement

	jobs chan primaryNativeFoldJob
	done chan struct{}

	image  []byte
	native bool
	// inspected/allPuts preserve the worker's coalesced bucket result for the
	// ordered serial fallback. A decline must not walk the same overlay history
	// a second time merely because it needs the broader planner.
	inspected  bool
	allPuts    bool
	sourceSafe bool
	// retrySerial is set only when concurrent PageCache pinning made an
	// otherwise eligible source temporarily unavailable. The coordinator retries
	// the same native certificate after every lease in the wave has released.
	retrySerial bool
	err         error
}

type primaryNativeFoldJob struct {
	pending    *filePrimaryPendingParent
	base       *fileStoreState
	visible    *fileStoreState
	generation uint64
}

func (context *primaryNativeFoldContext) resetResult() {
	if context == nil {
		return
	}
	context.image = nil
	context.native = false
	context.inspected = false
	context.allPuts = false
	context.sourceSafe = false
	context.retrySerial = false
	context.err = nil
	context.replacements = context.replacements[:0]
}

func (c *Collection) resetPrimaryNativeFoldResults(count int) {
	if c == nil {
		return
	}
	count = min(count, len(c.primaryNativeFoldContexts))
	for index := 0; index < count; index++ {
		c.primaryNativeFoldContexts[index].resetResult()
	}
}

// setupPrimaryNativeFoldContexts retains the small fixed codec pool. Worker
// contexts are private; coordinator context zero borrows writer scratch that is
// otherwise idle during its ordered wave slot. Every bound is known at
// construction: CPU count, resident allowance, and the normalized
// pending-parent window.
func (c *Collection) setupPrimaryNativeFoldContexts() {
	if c == nil || c.primaryUnifiedOverlay == nil ||
		!c.primaryNativeFoldContextEligible() ||
		c.primaryUnifiedBuilder == nil ||
		len(c.primaryLeafScratch) < storeio.CommonPrimaryLeafMaxExtentBytes ||
		cap(c.primaryPendingParents) == 0 ||
		cap(c.primaryUnifiedReplacementScratch) <
			storeio.CommonPrimaryLeafWideSlots {
		return
	}
	workers := min(runtime.GOMAXPROCS(0), primaryNativeFoldMaxWorkers)
	residentWorkers := int(c.options.ResidentBytes /
		primaryNativeFoldResidentBytesPerWorker)
	workers = min(workers, max(1, residentWorkers))
	workers = min(workers, cap(c.primaryPendingParents))
	if c.options.RecoveryJournal {
		// Per-mutation durability is device-bound and serialized. Keep its native
		// fold acceleration footprint to the coordinator that borrows the writer's
		// existing page, replacement, and builder scratch: no extra 64 KiB images,
		// channels, or idle goroutines for a lane that cannot publish concurrently.
		workers = min(workers, 1)
	}
	if workers <= 0 {
		return
	}
	c.primaryNativeFoldContexts = make(
		[]primaryNativeFoldContext, workers,
	)
	for index := range c.primaryNativeFoldContexts {
		context := &c.primaryNativeFoldContexts[index]
		if index == 0 {
			// The coordinator runs on the writer goroutine and its image is staged
			// before a later context can enter serial fallback. Borrow the writer's
			// otherwise-idle page and replacement scratch instead of retaining a
			// duplicate ~80 KiB lane.
			context.page = c.primaryLeafScratch[:storeio.CommonPrimaryLeafMaxExtentBytes]
			context.replacements = c.primaryUnifiedReplacementScratch[:0]
		} else {
			context.page = make(
				[]byte, storeio.CommonPrimaryLeafMaxExtentBytes,
			)
			context.replacements = make(
				[]storeio.CommonPrimaryUnifiedReplacement, 0,
				storeio.CommonPrimaryLeafWideSlots,
			)
		}
		if index == 0 {
			context.builder = c.primaryUnifiedBuilder
		} else {
			context.builder = storeio.NewCompactPrimaryPatchBuilder()
			context.jobs = make(chan primaryNativeFoldJob)
			context.done = make(chan struct{})
		}
	}
}

// primaryNativeFoldContextEligible is the construction-time ownership
// predicate for the bounded foreground codec pool. It deliberately does not
// depend on primaryConcurrentContexts: the serialized buffered-journal lane
// publishes into the same compact-leaf overlay and can precompute the same
// device-independent fold even though it must retain exclusive publication and
// recovery-journal ordering.
func (c *Collection) primaryNativeFoldContextEligible() bool {
	return c != nil && c.buffered() && c.primaryUnifiedOverlay != nil &&
		c.options.Collection.Schema == nil && len(c.options.indexes) == 0
}

// primaryNativeFoldEligible is the runtime use predicate. An online index
// cutover may make a pool retained at construction ineligible later; recovery
// replay likewise stays on the established serial fold. RecoveryJournal is not
// a veto: preparation reads only the immutable compact base and overlay, while
// allocation, staging, retirement, journal recycle, and whole-cut publication
// remain on the exclusive writer goroutine.
func (c *Collection) primaryNativeFoldEligible() bool {
	return c != nil && len(c.primaryNativeFoldContexts) != 0 &&
		c.primaryNativeFoldContextEligible() && c.primaryEpoch == nil &&
		!c.onlineIndexBuild.Load() && !c.journalReplaying
}

// runPrimaryNativeFoldWorker lives only for one foreground materialization.
// Channels and codec contexts are retained, but no collection owns an idle
// background goroutine between folds.
func (c *Collection) runPrimaryNativeFoldWorker(
	context *primaryNativeFoldContext,
) {
	for {
		job := <-context.jobs
		if job.pending == nil {
			context.done <- struct{}{}
			return
		}
		c.preparePrimaryNativeFold(
			context, job.pending, job.base, job.visible, job.generation,
		)
		context.done <- struct{}{}
	}
}

func (c *Collection) startPrimaryNativeFoldWorkers(count int) {
	for index := 1; index < count; index++ {
		go c.runPrimaryNativeFoldWorker(
			&c.primaryNativeFoldContexts[index],
		)
	}
}

func (c *Collection) stopPrimaryNativeFoldWorkers(count int) {
	for index := 1; index < count; index++ {
		c.primaryNativeFoldContexts[index].jobs <- primaryNativeFoldJob{}
	}
	for index := 1; index < count; index++ {
		<-c.primaryNativeFoldContexts[index].done
	}
}

func (c *Collection) primaryNativeFoldActiveContexts(pending int) int {
	if pending <= 0 || !c.primaryNativeFoldEligible() {
		return 0
	}
	return min(
		len(c.primaryNativeFoldContexts), pending,
		max(1, runtime.GOMAXPROCS(0)),
	)
}

// preparePrimaryNativeFoldWave performs only the device-independent portion of
// a checkpoint leaf rewrite. The caller still owns the collection writer
// exclusively, so the overlay and routed identities are immutable for the
// duration. Cache leases are independently pinned by each context.
func (c *Collection) preparePrimaryNativeFoldWave(
	first, count int,
	base, visible *fileStoreState,
	generation uint64,
) {
	for offset := 1; offset < count; offset++ {
		c.primaryNativeFoldContexts[offset].jobs <- primaryNativeFoldJob{
			pending:    &c.primaryPendingParents[first+offset],
			base:       base,
			visible:    visible,
			generation: generation,
		}
	}
	c.preparePrimaryNativeFold(
		&c.primaryNativeFoldContexts[0],
		&c.primaryPendingParents[first], base, visible, generation,
	)
	for offset := 1; offset < count; offset++ {
		<-c.primaryNativeFoldContexts[offset].done
	}
}

// preparePrimaryNativeFold performs the exact compact-column replacement
// certificate before the serial foreground fold. A successful image changes
// only its replanned scalar streams and is byte-identical to the complete
// compact planner. Structural, volatile-overflow, or plan-changing buckets
// decline to the deterministic serial fallback.
func (c *Collection) preparePrimaryNativeFold(
	context *primaryNativeFoldContext,
	pending *filePrimaryPendingParent,
	base, visible *fileStoreState,
	generation uint64,
) {
	if context == nil {
		return
	}
	context.resetResult()
	if c == nil || pending == nil || base == nil ||
		visible == nil || c.primaryUnifiedOverlay == nil {
		return
	}
	var lease storeio.PageLease
	var err error
	if acquire := c.primaryNativeFoldAcquire; acquire != nil {
		lease, err = acquire(pending.volatileRef)
	} else {
		lease, err = c.cache.Acquire(pending.volatileRef)
	}
	if err != nil {
		// Parallel pinning can temporarily exhaust a cache where the old
		// one-at-a-time path succeeds. Once the complete wave joins, the serial
		// planner retries this leaf with every worker lease released.
		if errors.Is(err, storeio.ErrPageCachePinned) {
			context.retrySerial = true
			return
		}
		context.err = err
		return
	}
	defer lease.Release()
	if storeio.PrimaryLeafClass(lease.Page()) != storeio.CommonPrimaryLeafCompact {
		context.err = storeio.ErrCommonPrimaryLeafCorrupt
		return
	}
	stripe, ok := storeio.AdmittedCompactPrimaryStripe(
		lease.Page(), c.storeID, pending.leafRoute.Bucket,
	)
	if !ok {
		context.err = fmt.Errorf(
			"%w: checkpoint compact bucket=%d ref=%+v header=%+v bytes=%d",
			storeio.ErrCommonPrimaryLeafCorrupt,
			pending.leafRoute.Bucket, pending.volatileRef,
			lease.Header(), len(lease.Page()),
		)
		return
	}
	context.sourceSafe = !stripe.HasOverflowRows() ||
		pending.volatileRef.Offset < base.fileEnd
	if !context.sourceSafe {
		return
	}
	if !c.primaryUnifiedOverlay.pendingBucket(pending.leafRoute.Bucket) {
		header := stripe.Header()
		if generation < header.Generation {
			context.err = storeio.ErrGenerationOrder
			return
		}
		if generation == header.Generation {
			context.image = context.page[:len(lease.Page())]
			copy(context.image, lease.Page())
			context.native = true
			return
		}
		image, cloneErr := storeio.CloneCompactPrimaryStripeGeneration(
			context.page, lease.Page(), generation,
		)
		if cloneErr != nil {
			context.err = cloneErr
			return
		}
		context.image = image
		context.native = true
		return
	}
	replacements, allPuts, inspectErr :=
		c.primaryUnifiedOverlay.primaryUnifiedFixedReplacements(
			context.replacements[:0], pending.leafRoute.Bucket,
			generation,
		)
	context.inspected = true
	context.allPuts = allPuts
	context.replacements = replacements
	if inspectErr != nil {
		context.err = inspectErr
		return
	}
	if !allPuts || len(replacements) == 0 {
		return
	}
	image, patched, patchErr := stripe.PatchCompactPrimaryStripeReplacements(
		context.page, generation, replacements, context.builder,
	)
	c.primaryCompactColumnPatchAttempts.Add(1)
	if patchErr != nil {
		context.err = patchErr
		return
	}
	if patched {
		context.image = image
		context.native = true
		c.primaryCompactColumnPatches.Add(1)
	}
}

func (c *Collection) primaryNativeFoldAdditionalScratchBytes() uint64 {
	var bytes uint64
	for index := range c.primaryNativeFoldContexts {
		context := &c.primaryNativeFoldContexts[index]
		if index != 0 {
			bytes += uint64(len(context.page))
			bytes += uint64(cap(context.replacements)) *
				uint64(unsafe.Sizeof(storeio.CommonPrimaryUnifiedReplacement{}))
		}
		if index != 0 {
			// Context zero borrows primaryUnifiedBuilder, whose compact capacity is
			// already charged by PrimaryMutationScratchBytes.
			bytes += context.builder.CompactPatchCapacityBytes()
		}
		bytes += uint64(unsafe.Sizeof(*context))
		// Runtime channel headers are opaque. Charge a conservative two cache
		// lines per retained unbuffered channel rather than silently omitting
		// their fixed allocation from the public scratch accounting. Context zero
		// is the coordinator and deliberately owns no channels.
		if context.jobs != nil {
			bytes += 2 * 64
		}
		if context.done != nil {
			bytes += 2 * 64
		}
	}
	return bytes
}
