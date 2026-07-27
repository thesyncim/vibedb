package storeio

import (
	"encoding/binary"
	"fmt"
	"slices"
)

type materializationCommitMode uint8

const (
	materializationPatchOnly materializationCommitMode = iota + 1
	materializationHybrid
)

type materializationCommitResult struct {
	CompletedPhases   uint8
	CompletedBarriers uint8
	RootAttempted     bool
}

// materializationDevice executes patch-only two-fence or hybrid three-fence
// durability without hiding the crash-relevant result inside the ordinary
// Commit contract.
type materializationDevice interface {
	CommitMaterialized(
		journal Write, targets []Write, root Write,
		mode materializationCommitMode,
	) (materializationCommitResult, error)
}

// MaterializationSequence returns the exact sequence the caller must encode
// into this batch's journal capsule. It remains reserved only by the
// single-producer ownership of Batch; Abort leaves it available to the next
// materialized batch, while a successful Publish advances it.
func (b *Batch) MaterializationSequence() (uint64, error) {
	if b == nil || b.state.Load() != batchOwned || !b.materialized {
		return 0, ErrBatchState
	}
	return b.committer.materializationNextSequence.Load(), nil
}

// BeginMaterialized acquires one isolated canonical-materialization batch.
// patchWriteCount is the exact number of journal patch spans, not the number
// of complete target pages. It is the patch-only spelling retained for callers
// that do not also publish immutable copy-on-write pages.
func (c *Committer) BeginMaterialized(patchWriteCount int) (*Batch, error) {
	return c.beginHybridMaterialized(0, patchWriteCount)
}

// beginHybridMaterialized acquires one isolated generation containing both
// immutable copy-on-write pages and journal-covered canonical patches. The
// durable order is journal, one offset-sorted combined data phase, then root.
// A crash therefore either leaves the old root recoverable by rolling back the
// patches, or exposes every full page and patch named by the new root. Readers
// never consult a delta, journal, or overlay.
//
// This constructor is deliberately package-private. Only
// BeginHybridWriteTransaction may expose the full-page lane, because its
// allocator proves that every unjournaled page owns a fresh or recovery-safe
// extent. Raw Batch callers get the patch-only BeginMaterialized API.
func (c *Committer) beginHybridMaterialized(
	fullWriteCount, patchWriteCount int,
) (*Batch, error) {
	return c.beginHybridMaterializedFrames(
		fullWriteCount, patchWriteCount, false,
	)
}

func (c *Committer) beginHybridMaterializedFrames(
	fullWriteCount, patchWriteCount int,
	frameNative bool,
) (*Batch, error) {
	if c == nil {
		return nil, ErrClosed
	}
	if c.options.MaterializationDamageGranule == 0 {
		return nil, ErrUnsupported
	}
	if fullWriteCount < 0 || patchWriteCount <= 0 {
		return nil, fmt.Errorf("%w: materialization requires a target write", ErrInvalidWrite)
	}
	requiredBuffers := fullWriteCount + patchWriteCount + 2
	if frameNative {
		requiredBuffers = patchWriteCount + 2
	}
	if fullWriteCount > c.options.MaxPagesPerBatch ||
		patchWriteCount > MaterializationJournalMaxPatches ||
		fullWriteCount > c.options.MaxPagesPerBatch-patchWriteCount ||
		requiredBuffers > c.bufferCount {
		return nil, ErrTooManyPages
	}
	bufferedPageCount := fullWriteCount + patchWriteCount
	if frameNative {
		bufferedPageCount = patchWriteCount
	}
	batch, err := c.begin(
		fullWriteCount+patchWriteCount, bufferedPageCount,
	)
	if err != nil {
		return nil, err
	}
	buffer, err := c.acquire(c.freeBuffers)
	if err != nil {
		_ = batch.Abort()
		return nil, err
	}
	batch.journal = Write{Buffer: uint16(buffer)}
	batch.materializationPatchCount = uint16(patchWriteCount)
	batch.materialized = true
	return batch, nil
}

// materializationPageBuffer returns one private full-page staging span in a
// hybrid materialization batch. Patch buffers remain inaccessible: they are
// populated only by StageMaterializationTarget from a checksum-proved complete
// after-image.
func (b *Batch) materializationPageBuffer(page int) ([]byte, error) {
	if b == nil || b.state.Load() != batchOwned || !b.materialized ||
		page < 0 || page >= b.materializationFullWriteCount() {
		return nil, ErrBatchState
	}
	index := int(b.materializationPatchCount) + page
	return b.committer.buffers[b.pages[index].Buffer], nil
}

// stageMaterializationPage records one complete allocator-proven immutable
// page for the hybrid data phase. Publish also verifies its common checksum and
// requires its birth generation to equal the root generation.
func (b *Batch) stageMaterializationPage(
	page int, offset int64, length int, kind PageKind,
) error {
	if b == nil || b.state.Load() != batchOwned || !b.materialized ||
		page < 0 || page >= b.materializationFullWriteCount() ||
		!validPageKind(kind) {
		return ErrBatchState
	}
	if offset < 0 || length <= 0 ||
		uint64(length) > uint64(b.committer.bufferSize) {
		return ErrInvalidWrite
	}
	index := int(b.materializationPatchCount) + page
	b.pages[index].Offset = offset
	b.pages[index].Length = uint32(length)
	b.pages[index].kind = kind
	return nil
}

func (b *Batch) materializationFullWriteCount() int {
	if b == nil {
		return 0
	}
	return len(b.pages) - int(b.materializationPatchCount)
}

// resizeMaterializationPages returns unused trailing full-page buffers while
// retaining every fixed journal-patch buffer. It is the hybrid counterpart of
// ResizePages and cannot grow the reservation.
func (b *Batch) resizeMaterializationPages(fullWriteCount int) error {
	if b == nil || b.state.Load() != batchOwned || !b.materialized ||
		fullWriteCount < 0 ||
		fullWriteCount > b.materializationFullWriteCount() {
		return ErrBatchState
	}
	keep := int(b.materializationPatchCount) + fullWriteCount
	releaseEnd := int(b.dataBufferCount)
	if keep < releaseEnd {
		released := b.bufferIndexes[:releaseEnd-keep]
		for rank := range released {
			released[rank] = uint32(b.pages[keep+rank].Buffer)
		}
		b.committer.freeBuffers.pushN(released)
		b.dataBufferCount = uint16(keep)
	}
	for rank := keep; rank < len(b.pages); rank++ {
		b.pages[rank] = Write{}
	}
	b.pages = b.pages[:keep]
	return nil
}

// MaterializationJournalBuffer returns the dedicated fixed-capsule buffer.
// The caller encodes one complete capsule, then calls
// SealMaterializationJournal before staging complete target after-images.
func (b *Batch) MaterializationJournalBuffer() ([]byte, error) {
	if b == nil || b.state.Load() != batchOwned || !b.materialized {
		return nil, ErrBatchState
	}
	return b.committer.buffers[b.journal.Buffer], nil
}

// SealMaterializationJournal validates and freezes the capsule framing. The
// physical offsets are never caller supplied: Publish derives the exact two
// allocator-excluded slots from MutableStoreLayout and selects the
// recovery-seeded next slot.
func (b *Batch) SealMaterializationJournal() error {
	if b == nil || b.state.Load() != batchOwned || !b.materialized {
		return ErrBatchState
	}
	buffer := b.committer.buffers[b.journal.Buffer]
	view, err := OpenMaterializationJournal(buffer[:MaterializationJournalSize])
	if err != nil {
		return err
	}
	header := view.Header()
	if header.SectorSize != b.committer.options.MaterializationDamageGranule ||
		view.PatchLen() != int(b.materializationPatchCount) {
		return fmt.Errorf("%w: materialization journal geometry", ErrInvalidWrite)
	}
	b.journal.Length = MaterializationJournalSize
	b.journalCapsuleChecksum = PageChecksum(buffer[:MaterializationJournalSize])
	b.materializationTargetMask = 0
	clear(b.materializationPatchChecksums[:])
	return nil
}

// StageMaterializationTarget proves one complete after-image against the
// journal's strong bounded-sector digest, unchanged context, and exact PageRef,
// then extracts only that target's aligned dirty sectors into Device buffers.
// The complete image remains caller-owned and can be discarded on return.
// Publish rechecks a checksum for every extracted sector, so a stale retained
// sparse buffer cannot slip under the new root.
func (b *Batch) StageMaterializationTarget(targetRank int, after []byte) error {
	view, target, err := b.openMaterializationTargetForStage(
		targetRank, len(after),
	)
	if err != nil {
		return err
	}
	if err := view.ValidateAfterImage(targetRank, after); err != nil {
		return fmt.Errorf("%w: materialization after-image: %v", ErrInvalidWrite, err)
	}
	return b.stageValidatedMaterializationTarget(
		view, target, targetRank, after,
	)
}

// StageBuiltMaterializationTarget stages a trusted builder-owned after-image
// without repeating its strong bounded-sector digest. built must be the exact,
// unmodified value returned by BuildMaterializationTarget and must equal the
// durable descriptor already sealed in this batch's journal. The transient
// built marker and after checksum are never encoded, so a decoded or manually
// constructed target cannot enter this path.
//
// The after-image still passes its stored common-page checksum, checksum
// complement, complete OpenPage validation, Store identity, and exact PageRef
// identity before any sparse sector is extracted. Direct or untrusted callers
// must use StageMaterializationTarget, which independently validates the
// journal's strong after-patch digest and unchanged context.
func (b *Batch) StageBuiltMaterializationTarget(
	targetRank int,
	built MaterializationTarget,
	after []byte,
) error {
	view, target, err := b.openMaterializationTargetForStage(
		targetRank, len(after),
	)
	if err != nil {
		return err
	}
	if built.builtMarker != ^built.builtAfterChecksum ||
		!materializationTargetDurableEqual(target, built) {
		return fmt.Errorf(
			"%w: materialization built target descriptor", ErrInvalidWrite,
		)
	}
	trailer := after[len(after)-PageTrailerSize:]
	if binary.LittleEndian.Uint32(trailer[0:4]) != built.builtAfterChecksum ||
		binary.LittleEndian.Uint32(trailer[4:8]) != ^built.builtAfterChecksum {
		return fmt.Errorf(
			"%w: materialization built after checksum", ErrInvalidWrite,
		)
	}
	header, _, openErr := OpenPage(after)
	if openErr != nil || header.StoreID != view.Header().StoreID ||
		!materializationPageHeaderMatchesRef(header, target.Ref) {
		return fmt.Errorf(
			"%w: materialization built after-image", ErrInvalidWrite,
		)
	}
	return b.stageValidatedMaterializationTarget(
		view, target, targetRank, after,
	)
}

func (b *Batch) openMaterializationTargetForStage(
	targetRank int,
	afterLength int,
) (MaterializationJournalView, MaterializationTarget, error) {
	if b == nil || b.state.Load() != batchOwned || !b.materialized ||
		b.journal.Length != MaterializationJournalSize {
		return MaterializationJournalView{}, MaterializationTarget{},
			ErrBatchState
	}
	journalBuffer := b.committer.buffers[b.journal.Buffer]
	if PageChecksum(journalBuffer[:MaterializationJournalSize]) != b.journalCapsuleChecksum {
		return MaterializationJournalView{}, MaterializationTarget{},
			ErrMaterializationJournalCorrupt
	}
	view, err := OpenMaterializationJournal(journalBuffer[:MaterializationJournalSize])
	if err != nil {
		return MaterializationJournalView{}, MaterializationTarget{}, err
	}
	target, ok := view.TargetAt(targetRank)
	if !ok || afterLength != int(target.Ref.Length) {
		return MaterializationJournalView{}, MaterializationTarget{},
			fmt.Errorf("%w: materialization after-image length", ErrInvalidWrite)
	}
	return view, target, nil
}

func (b *Batch) stageValidatedMaterializationTarget(
	view MaterializationJournalView,
	target MaterializationTarget,
	targetRank int,
	after []byte,
) error {
	first, count, ok := view.TargetPatchRange(targetRank)
	if !ok || count == 0 {
		return fmt.Errorf("%w: materialization target patches", ErrInvalidWrite)
	}
	for patchRank := first; patchRank < first+count; patchRank++ {
		patch, _ := view.PatchAt(patchRank)
		end := uint64(patch.Offset) + uint64(len(patch.Data))
		if len(patch.Data) > b.committer.bufferSize || end > uint64(len(after)) {
			return fmt.Errorf("%w: materialization sparse sector", ErrInvalidWrite)
		}
		if patchRank >= int(b.materializationPatchCount) {
			return fmt.Errorf("%w: materialization patch rank", ErrInvalidWrite)
		}
		buffer := b.committer.buffers[b.pages[patchRank].Buffer]
		copy(buffer, after[patch.Offset:uint32(end)])
		b.pages[patchRank].Offset = int64(target.Ref.Offset + uint64(patch.Offset))
		b.pages[patchRank].Length = uint32(len(patch.Data))
		b.pages[patchRank].kind = 0
		b.materializationPatchChecksums[patchRank] =
			PageChecksum(buffer[:len(patch.Data)])
	}
	b.materializationTargetMask |= uint8(1) << targetRank
	return nil
}

// InitializeMaterializationRecovery seeds the next capsule sequence and the
// physical slot opposite the checksum-valid capsule selected by recovery. It
// is independent of root-slot state and must run before the first materialized
// publication. Fresh files require no call: they start at sequence one, slot
// zero.
func (c *Committer) InitializeMaterializationRecovery(
	selectedSequence uint64,
	selectedSlot int,
) error {
	if c == nil || c.closing.Load() ||
		c.options.MaterializationDamageGranule == 0 ||
		selectedSequence == 0 || selectedSequence == ^uint64(0) ||
		selectedSlot < 0 || selectedSlot >= materializationJournalCopies ||
		c.materializationPublished.Load() ||
		!c.materializationRecoveryInitialized.CompareAndSwap(false, true) {
		return ErrGenerationOrder
	}
	c.materializationNextSequence.Store(selectedSequence + 1)
	c.materializationNextSlot.Store(uint32(selectedSlot ^ 1))
	return nil
}

func (c *Committer) validateMaterializedBatch(batch *Batch, generation uint64) (uint64, error) {
	if batch.journal.Length != MaterializationJournalSize {
		return 0, fmt.Errorf("%w: materialization journal is not sealed", ErrInvalidWrite)
	}
	journalBuffer := c.buffers[batch.journal.Buffer]
	if PageChecksum(journalBuffer[:MaterializationJournalSize]) != batch.journalCapsuleChecksum {
		return 0, ErrMaterializationJournalCorrupt
	}
	view, err := OpenMaterializationJournal(journalBuffer[:MaterializationJournalSize])
	if err != nil {
		return 0, err
	}
	header := view.Header()
	if header.TargetGeneration != generation {
		return 0, ErrGenerationOrder
	}
	if header.Sequence != c.materializationNextSequence.Load() ||
		header.Sequence == ^uint64(0) ||
		header.SectorSize != c.options.MaterializationDamageGranule ||
		int(batch.materializationPatchCount) != view.PatchLen() ||
		int(batch.materializationPatchCount) > len(batch.pages) {
		return 0, fmt.Errorf("%w: materialization sequence or geometry", ErrInvalidWrite)
	}
	wantTargets := uint8(1)<<view.Len() - 1
	if batch.materializationTargetMask != wantTargets {
		return 0, fmt.Errorf("%w: incomplete materialization after-images", ErrInvalidWrite)
	}

	if batch.root.Length < InlineSuperblockSize {
		return 0, fmt.Errorf("%w: materialization requires an inline root", ErrInvalidWrite)
	}
	rootBuffer := c.buffers[batch.root.Buffer]
	root, err := DecodeInlineSuperblock(rootBuffer[:batch.root.Length])
	if err != nil {
		return 0, err
	}
	if batch.rootGeneration != generation ||
		root.Generation != generation || root.StoreID != header.StoreID ||
		root.PageSize != header.PageSize || root.PageSize != batch.root.Length {
		return 0, fmt.Errorf("%w: materialization root identity", ErrInvalidWrite)
	}
	layout, err := MutableStoreLayout(root.PageSize)
	if err != nil {
		return 0, err
	}
	journalSlot := c.materializationNextSlot.Load()
	if journalSlot >= materializationJournalCopies {
		return 0, fmt.Errorf("%w: materialization journal slot", ErrInvalidWrite)
	}
	batch.journal.Offset = int64(layout.MaterializationJournalOffsets[journalSlot])

	patchWrites := batch.pages[:batch.materializationPatchCount]
	fullWrites := batch.pages[batch.materializationPatchCount:]
	for rank, write := range patchWrites {
		if _, writeErr := validateWrite(
			c.bufferCount, c.bufferSize, write,
		); writeErr != nil {
			return 0, writeErr
		}
		patch, ok := view.PatchAt(rank)
		if !ok {
			return 0, fmt.Errorf("%w: materialization patch rank", ErrInvalidWrite)
		}
		target, ok := view.TargetAt(int(patch.Target))
		if !ok || target.Ref.Offset > root.FileEnd ||
			uint64(target.Ref.Length) > root.FileEnd-target.Ref.Offset {
			return 0, fmt.Errorf("%w: materialization target rank", ErrInvalidWrite)
		}
		offset := target.Ref.Offset + uint64(patch.Offset)
		if offset > uint64(^uint64(0)>>1) ||
			write.Offset != int64(offset) ||
			uint64(write.Length) != uint64(len(patch.Data)) ||
			uint64(write.Offset) < layout.DataStart ||
			PageChecksum(c.buffers[write.Buffer][:write.Length]) !=
				batch.materializationPatchChecksums[rank] {
			return 0, fmt.Errorf("%w: unjournaled materialization write", ErrInvalidWrite)
		}
	}
	for rank := range fullWrites {
		write := &fullWrites[rank]
		if _, writeErr := validateWrite(
			c.bufferCount, c.bufferSize, *write,
		); writeErr != nil {
			return 0, writeErr
		}
		if uint64(write.Offset) < layout.DataStart ||
			uint64(write.Offset) > root.FileEnd ||
			uint64(write.Length) > root.FileEnd-uint64(write.Offset) {
			return 0, fmt.Errorf("%w: materialization full-page bounds", ErrInvalidWrite)
		}
		var page []byte
		if write.frameNative() {
			cache := c.frameCache.Load()
			if cache == nil || int(write.frameIndex) >= len(cache.frames) {
				return 0, ErrInvalidWrite
			}
			page = cache.extentBytes(
				int(write.frameIndex), write.Length,
			)
		} else {
			page = c.buffers[write.Buffer][:write.Length]
		}
		pageHeader, _, openErr := OpenPage(page)
		if openErr != nil ||
			pageHeader.StoreID != header.StoreID ||
			pageHeader.Generation != generation ||
			pageHeader.PageSize != write.Length ||
			pageHeader.Kind != write.kind ||
			pageHeader.Kind == PageStateRoot ||
			pageHeader.LogicalID <= StateRootLogicalID ||
			pageHeader.LogicalID >= root.State.NextLogicalID {
			return 0, fmt.Errorf(
				"%w: materialization full-page identity",
				ErrInvalidWrite,
			)
		}
		write.kind = pageHeader.Kind
	}
	slices.SortFunc(fullWrites, func(a, b Write) int {
		switch {
		case a.Offset < b.Offset:
			return -1
		case a.Offset > b.Offset:
			return 1
		default:
			return 0
		}
	})
	if err := validateMaterializationDataWrites(
		c.bufferCount, c.bufferSize, c.producerSeen,
		patchWrites, fullWrites, batch.root,
	); err != nil {
		return 0, err
	}
	return header.Sequence, nil
}

func validateMaterializationDataWrites(
	bufferCount, bufferSize int,
	seen []uint64,
	patchWrites, fullWrites []Write,
	root Write,
) error {
	clear(seen)
	rootEnd, err := validateWrite(bufferCount, bufferSize, root)
	if err != nil {
		return err
	}
	validateGroup := func(writes []Write) error {
		var previousEnd int64
		for rank, write := range writes {
			end, writeErr := validateWrite(bufferCount, bufferSize, write)
			if writeErr != nil {
				return writeErr
			}
			if !write.frameNative() {
				word, bit := int(write.Buffer)>>6, uint(write.Buffer)&63
				mask := uint64(1) << bit
				if seen[word]&mask != 0 {
					return ErrDuplicateBuffer
				}
				seen[word] |= mask
			}
			if rank != 0 && write.Offset < previousEnd {
				return ErrOverlappingWrite
			}
			if write.Offset < rootEnd && root.Offset < end {
				return ErrOverlappingWrite
			}
			previousEnd = end
		}
		return nil
	}
	if err := validateGroup(patchWrites); err != nil {
		return err
	}
	if err := validateGroup(fullWrites); err != nil {
		return err
	}
	for patchRank, fullRank := 0, 0; patchRank < len(patchWrites) && fullRank < len(fullWrites); {
		patch := patchWrites[patchRank]
		full := fullWrites[fullRank]
		patchEnd := patch.Offset + int64(patch.Length)
		fullEnd := full.Offset + int64(full.Length)
		switch {
		case patchEnd <= full.Offset:
			patchRank++
		case fullEnd <= patch.Offset:
			fullRank++
		default:
			return ErrOverlappingWrite
		}
	}
	return nil
}

func (c *Committer) commitMaterialized(device Device, batch *Batch) error {
	layout, err := MutableStoreLayout(batch.root.Length)
	if err != nil {
		return err
	}
	if batch.journalSlot >= materializationJournalCopies ||
		batch.journal.Offset !=
			int64(layout.MaterializationJournalOffsets[batch.journalSlot]) {
		return fmt.Errorf("%w: materialization journal slot changed", ErrInvalidWrite)
	}

	rootSlot := c.nextRootSlot.Load()
	if batch.rootGeneration != 0 {
		batch.root.Offset = int64(layout.RootOffsets[rootSlot])
	}
	patchWrites := batch.pages[:batch.materializationPatchCount]
	fullWrites := batch.pages[batch.materializationPatchCount:]
	dataWrites, mergeErr := mergeMaterializationDataWrites(
		c.commitScratch[:0], patchWrites, fullWrites,
	)
	if mergeErr != nil {
		return mergeErr
	}
	var targetBytes uint64
	for _, write := range patchWrites {
		targetBytes += uint64(write.Length)
	}
	var fullWriteBytes uint64
	for _, write := range fullWrites {
		fullWriteBytes += uint64(write.Length)
	}
	materializer, ok := device.(materializationDevice)
	if !ok {
		return ErrUnsupported
	}
	if err := c.bindDeviceFrameArena(device, dataWrites); err != nil {
		return err
	}
	mode := materializationPatchOnly
	if len(fullWrites) != 0 {
		mode = materializationHybrid
	}
	locked, lockErr := c.lockFrameWrites(dataWrites)
	if lockErr != nil {
		return lockErr
	}
	result, commitErr := materializer.CommitMaterialized(
		batch.journal, dataWrites, batch.root, mode,
	)
	c.unlockFrameWrites(dataWrites, locked)
	if result.CompletedPhases > 3 || result.CompletedBarriers > 3 {
		return fmt.Errorf("%w: materialization completion count", ErrInvalidWrite)
	}
	c.deviceCommits.Add(uint64(result.CompletedPhases))
	c.materializationBarriers.Add(uint64(result.CompletedBarriers))
	if result.CompletedPhases >= 1 {
		c.deviceBytes.Add(uint64(batch.journal.Length))
		c.materializationJournalBytes.Add(uint64(batch.journal.Length))
	}
	if result.CompletedPhases >= 2 {
		c.deviceBytes.Add(targetBytes + fullWriteBytes)
		c.materializationTargetBytes.Add(targetBytes)
		c.materializationFullWriteBytes.Add(fullWriteBytes)
	}
	if result.CompletedPhases >= 3 {
		c.deviceBytes.Add(uint64(batch.root.Length))
	}
	if commitErr != nil {
		if result.RootAttempted {
			return commitOutcomeUnknown(commitErr)
		}
		return commitErr
	}
	wantBarriers := uint8(2)
	if mode == materializationHybrid {
		wantBarriers = 3
	}
	if result.CompletedPhases != 3 ||
		result.CompletedBarriers != wantBarriers ||
		!result.RootAttempted {
		return fmt.Errorf("%w: incomplete materialization phases", ErrInvalidWrite)
	}

	if batch.rootGeneration != 0 {
		c.nextRootSlot.Store(rootSlot ^ 1)
		// The older physical root can reference the canonical bytes just
		// overwritten. The retained capsule makes it crash-recoverable, but it
		// is not an independently readable fallback after success.
		c.fallback.Store(batch.generation)
	}
	c.batchesDone.Add(1)
	c.materializedDone.Add(1)
	for old := c.largestGroup.Load(); old < 1 &&
		!c.largestGroup.CompareAndSwap(old, 1); old = c.largestGroup.Load() {
	}
	c.durable.Store(batch.generation)
	if callbacks := c.callbacks.Load(); callbacks != nil &&
		callbacks.durable != nil {
		callbacks.durable(batch.generation)
	}
	// Match the ordinary worker's physical-durable/callback-settled split.
	c.settled.Store(batch.generation)
	c.broadcast()
	c.release(batch)
	return nil
}

func mergeMaterializationDataWrites(
	dst, patchWrites, fullWrites []Write,
) ([]Write, error) {
	if cap(dst) < len(patchWrites)+len(fullWrites) {
		return nil, ErrTooManyPages
	}
	dst = dst[:0]
	for patchRank, fullRank := 0, 0; patchRank < len(patchWrites) || fullRank < len(fullWrites); {
		if fullRank == len(fullWrites) ||
			patchRank < len(patchWrites) &&
				patchWrites[patchRank].Offset < fullWrites[fullRank].Offset {
			dst = append(dst, patchWrites[patchRank])
			patchRank++
			continue
		}
		dst = append(dst, fullWrites[fullRank])
		fullRank++
	}
	return dst, nil
}
