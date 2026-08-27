package storeio

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// ErrPhysicalCapacity reports that a copy-on-write allocation would extend
// past the caller's currently proven physical high-water mark. It is a typed,
// retryable refusal: callers may strictly allocate more of a sealed file and
// retry without changing logical state.
var ErrPhysicalCapacity = errors.New("vibedb: Store physical capacity is unavailable")

// WriteTransactionOptions binds one copy-on-write publication to its current
// physical and logical allocation high-water marks.
type WriteTransactionOptions struct {
	StoreID    [16]byte
	Generation uint64
	PageSize   uint32
	FileEnd    uint64
	// PhysicalHighWaterBytes is the currently proven physical allocation high-water.
	// Zero preserves the elastic allocator. A non-zero value must be page
	// aligned and may be smaller than the immutable file-format ceiling.
	PhysicalHighWaterBytes uint64
	NextLogicalID          uint64
	// Reusable contains snapshot- and recovery-safe physical extents owned by
	// the serialized caller. Allocate shrinks entries in place; Publish keeps
	// those edits and Abort restores them from ReuseJournal.
	Reusable []FreeExtent
	// ReuseJournal is caller-owned scratch with capacity for maxPages edits.
	ReuseJournal []ReuseEdit
	// ReusableIndex accelerates exact lowest-offset first-fit over Reusable.
	// It is optional so existing callers retain the linear fallback.
	ReusableIndex *FreeExtentIndex
	// ReusablePromoter may make clean durable free-image segments resident
	// before an allocation chooses its exact first fit. The returned slice must
	// remain offset ordered and retain every extent named by edits.
	ReusablePromoter ReusableExtentPromoter
}

// ReusableExtentPromoter lazily admits durable free extents that were left on
// disk at open. beforeOffset is the current resident first fit, or ^uint64(0)
// when no resident extent fits. Implementations only need to promote an extent
// below that bound, because anything above it cannot change exact first-fit.
type ReusableExtentPromoter interface {
	PromoteReusable(
		want, beforeOffset uint64, edits []ReuseEdit,
	) ([]FreeExtent, bool, error)
}

// ReuseEdit is one allocator rollback record. Callers supply storage but must
// otherwise treat values as opaque.
type ReuseEdit struct {
	Index  uint32
	Before FreeExtent
}

// WriteTransaction reserves worst-case committer capacity, allocates immutable
// append extents, and publishes exactly one state root. It is single-owner and
// must be aborted or published.
type WriteTransaction struct {
	committer    *Committer
	cache        *PageCache
	batch        *Batch
	options      WriteTransactionOptions
	fileEnd      uint64
	nextID       uint64
	allocated    int
	reuseEdits   []ReuseEdit
	reuseEnabled bool
	active       bool
}

// UnrootedGenerationReservation is an append-only physical/logical namespace
// made collision-proof by publishing its advanced high-waters before any
// background staging write enters the region.
type UnrootedGenerationReservation struct {
	Offset, Length uint64
	FirstLogicalID uint64
	LogicalIDCount uint64
}

// ReserveUnrootedGeneration advances this transaction's authoritative file
// and logical-ID high-waters without staging a referenced page. The caller must
// publish the transaction before writing into the returned range. Until a later
// conditional install names those pages, the range is intentionally unreachable
// and must not enter the reusable allocator.
func (t *WriteTransaction) ReserveUnrootedGeneration(bytes, logicalIDs uint64) (UnrootedGenerationReservation, error) {
	var reservation UnrootedGenerationReservation
	if t == nil || !t.active || t.batch == nil || bytes == 0 ||
		bytes%uint64(t.options.PageSize) != 0 || logicalIDs == 0 ||
		t.fileEnd > maxSuperblockFileOffset-bytes || t.nextID > ^uint64(0)-logicalIDs {
		return reservation, fmt.Errorf("%w: unrooted generation reservation", ErrInvalidWrite)
	}
	reservation = UnrootedGenerationReservation{Offset: t.fileEnd, Length: bytes, FirstLogicalID: t.nextID, LogicalIDCount: logicalIDs}
	t.fileEnd += bytes
	t.nextID += logicalIDs
	return reservation, nil
}

// TransactionPage is one cache-frame staging span and its prospective durable
// reference. Transactions without a cache use the committer buffer.
// The value borrows its transaction and is invalid after Publish or Abort.
type TransactionPage struct {
	tx    *WriteTransaction
	index int
	ref   PageRef
	bytes []byte
	frame int
}

// Ref returns the prospective immutable page reference.
func (p TransactionPage) Ref() PageRef { return p.ref }

// Bytes returns the exact capacity-clipped encoding buffer.
func (p TransactionPage) Bytes() []byte { return p.bytes }

// Stage verifies foreign bytes, admits the page as dirty when applicable, and
// records its positional write. The exact capacity-clipped buffer reserved by
// this transaction is trusted after its seal marker and identity are checked:
// the encoder already computed the CRC while the bytes remained under this
// transaction's ownership, so neither Stage nor cache admission rehashes it.
func (p TransactionPage) Stage() error {
	if p.tx == nil || !p.tx.active || p.index < 0 || p.index >= p.tx.allocated || len(p.bytes) != int(p.ref.Length) {
		return ErrBatchState
	}
	writeIndex := p.index + int(p.tx.batch.materializationPatchCount)
	if writeIndex < 0 || writeIndex >= len(p.tx.batch.pages) {
		return ErrBatchState
	}
	write := &p.tx.batch.pages[writeIndex]
	trusted := p.tx.ownsExactPageBuffer(p.index, p.bytes, p.ref.Length)
	var header PageHeader
	if trusted {
		var ok bool
		header, ok = decodePageHeader(p.bytes)
		trailer := len(p.bytes) - PageTrailerSize
		if !ok || trailer < PageHeaderSize ||
			binary.LittleEndian.Uint32(p.bytes[trailer+4:]) !=
				^binary.LittleEndian.Uint32(p.bytes[trailer:trailer+4]) {
			return fmt.Errorf("%w: trusted staged page was not sealed", ErrPageCorrupt)
		}
		if !allZero(p.bytes[13:16]) || !allZero(p.bytes[56:64]) {
			return fmt.Errorf("%w: reserved bytes", ErrPageCorrupt)
		}
	} else {
		var err error
		header, _, err = OpenPage(p.bytes)
		if err != nil {
			return err
		}
	}
	if header.StoreID != p.tx.options.StoreID || header.Generation != p.tx.options.Generation ||
		header.LogicalID != p.ref.LogicalID || header.PageSize != p.ref.Length ||
		header.Kind != p.ref.Kind {
		return fmt.Errorf("%w: staged page identity", ErrInvalidWrite)
	}
	if write.Length != 0 {
		return ErrBatchState
	}
	if p.tx.cache != nil {
		var err error
		if trusted && write.frameNative() {
			err = p.tx.cache.sealDirtyFrame(
				p.frame, p.ref, p.tx.options.Generation, header, p.bytes,
			)
		} else if trusted {
			err = p.tx.cache.admitDirtyTrusted(
				p.ref, p.bytes, p.tx.options.Generation, header,
			)
		} else {
			err = p.tx.cache.AdmitDirty(
				p.ref, p.bytes, p.tx.options.Generation,
			)
		}
		if err != nil {
			return err
		}
	}
	// WriteTransaction already owns and bounds this exact descriptor. Recording
	// it directly keeps the ordinary COW staging path free of a hybrid-mode
	// branch and an extra atomic ownership load.
	write.Offset = int64(p.ref.Offset)
	write.Length = p.ref.Length
	write.kind = p.ref.Kind
	return nil
}

// ownsExactPageBuffer authenticates the private fast path from transaction
// state, not from a caller-provided flag or checksum. Both the starting address
// and the capacity-clipped length must be the reservation assigned to index.
func (t *WriteTransaction) ownsExactPageBuffer(
	index int, page []byte, length uint32,
) bool {
	write := t.writeAt(index)
	if write == nil || len(page) == 0 || len(page) != int(length) ||
		cap(page) != int(length) {
		return false
	}
	if write.frameNative() {
		if t.cache == nil || int(write.frameIndex) >= len(t.cache.frames) {
			return false
		}
		exact := t.cache.extentBytes(int(write.frameIndex), length)
		return &page[0] == &exact[0]
	}
	if int(write.Buffer) >= len(t.committer.buffers) {
		return false
	}
	buffer := t.committer.buffers[write.Buffer]
	if int(length) > len(buffer) {
		return false
	}
	exact := buffer[:int(length):int(length)]
	return &page[0] == &exact[0]
}

// BeginWriteTransaction acquires bounded worst-case staging capacity.
func BeginWriteTransaction(committer *Committer, cache *PageCache, maxPages int, options WriteTransactionOptions) (*WriteTransaction, error) {
	transaction := &WriteTransaction{}
	if err := transaction.Reset(committer, cache, maxPages, options); err != nil {
		return nil, err
	}
	return transaction, nil
}

// Reset acquires bounded worst-case staging capacity into an inactive,
// caller-owned transaction. A transaction may only be reset after Publish or
// Abort has released its Batch; rejecting an active receiver makes accidental
// overlapping reuse fail closed. Reset overwrites every field, including
// caller-owned allocator slices and promotion hooks retained by the preceding
// generation.
func (t *WriteTransaction) Reset(
	committer *Committer,
	cache *PageCache,
	maxPages int,
	options WriteTransactionOptions,
) error {
	if t == nil || t.active || t.batch != nil {
		return fmt.Errorf("%w: transaction identity, state, or bounds", ErrInvalidWrite)
	}
	// Drop every reference retained by the preceding inactive generation even
	// when validation or committer admission below fails.
	*t = WriteTransaction{}
	layout, layoutErr := MutableStoreLayout(options.PageSize)
	if committer == nil || options.StoreID == ([16]byte{}) || options.Generation == 0 ||
		layoutErr != nil || options.FileEnd < layout.DataStart ||
		options.FileEnd%uint64(options.PageSize) != 0 || options.FileEnd > maxSuperblockFileOffset ||
		options.PhysicalHighWaterBytes != 0 &&
			(options.PhysicalHighWaterBytes < options.FileEnd ||
				options.PhysicalHighWaterBytes > maxSuperblockFileOffset ||
				options.PhysicalHighWaterBytes%uint64(options.PageSize) != 0) ||
		options.NextLogicalID == 0 {
		return fmt.Errorf("%w: transaction identity, state, or bounds", ErrInvalidWrite)
	}
	if options.ReusableIndex != nil &&
		options.ReusableIndex.Len() != len(options.Reusable) {
		return fmt.Errorf("%w: reusable extent index", ErrInvalidWrite)
	}
	var batch *Batch
	var err error
	if cache != nil {
		if err = committer.bindFrameCache(cache); err != nil {
			return err
		}
		batch, err = committer.beginFrameNative(maxPages)
	} else {
		batch, err = committer.Begin(maxPages)
	}
	if err != nil {
		return err
	}
	*t = WriteTransaction{
		committer: committer, cache: cache, batch: batch, options: options,
		fileEnd: options.FileEnd, nextID: options.NextLogicalID,
		reuseEdits: options.ReuseJournal[:0], reuseEnabled: true, active: true,
	}
	return nil
}

// SetPublicationDescriptor attaches the canonical logical mutation batch that
// produced this transaction. The committer borrows descriptor until Publish or
// Abort; no copy or string conversion occurs on the publication hot path.
func (t *WriteTransaction) SetPublicationDescriptor(descriptor []byte) error {
	if t == nil || !t.active || t.batch == nil || len(descriptor) == 0 {
		return ErrBatchState
	}
	return t.batch.SetPublicationDescriptor(descriptor)
}

// BeginHybridWriteTransaction acquires one copy-on-write transaction whose
// immutable pages share a crash-safe publication with patchWriteCount
// journal-covered canonical writes. The caller prepares and seals the journal
// through the transaction's Materialization methods before PublishInline.
//
// This transaction proves the full-page lane owns fresh or recovery-safe
// allocator extents. It does not make canonical cache replacement or snapshot
// exclusion implicit: the Store integration must hold its snapshot publication
// gate across its final active-reader proof, cache replacement, and
// PublishInline, and must restore every replacement if publication fails.
func BeginHybridWriteTransaction(
	committer *Committer,
	cache *PageCache,
	maxPages, patchWriteCount int,
	options WriteTransactionOptions,
) (*WriteTransaction, error) {
	layout, layoutErr := MutableStoreLayout(options.PageSize)
	if committer == nil || options.StoreID == ([16]byte{}) || options.Generation == 0 ||
		patchWriteCount <= 0 ||
		layoutErr != nil || options.FileEnd < layout.DataStart ||
		options.FileEnd%uint64(options.PageSize) != 0 || options.FileEnd > maxSuperblockFileOffset ||
		options.PhysicalHighWaterBytes != 0 &&
			(options.PhysicalHighWaterBytes < options.FileEnd ||
				options.PhysicalHighWaterBytes > maxSuperblockFileOffset ||
				options.PhysicalHighWaterBytes%uint64(options.PageSize) != 0) ||
		options.NextLogicalID == 0 {
		return nil, fmt.Errorf("%w: transaction identity or bounds", ErrInvalidWrite)
	}
	if options.ReusableIndex != nil &&
		options.ReusableIndex.Len() != len(options.Reusable) {
		return nil, fmt.Errorf("%w: reusable extent index", ErrInvalidWrite)
	}
	if cache != nil {
		if err := committer.bindFrameCache(cache); err != nil {
			return nil, err
		}
	}
	batch, err := committer.beginHybridMaterializedFrames(
		maxPages, patchWriteCount, cache != nil,
	)
	if err != nil {
		return nil, err
	}
	return &WriteTransaction{
		committer: committer, cache: cache, batch: batch, options: options,
		fileEnd: options.FileEnd, nextID: options.NextLogicalID,
		reuseEdits: options.ReuseJournal[:0], reuseEnabled: true, active: true,
	}, nil
}

func (t *WriteTransaction) writeAt(index int) *Write {
	if t == nil || t.batch == nil || index < 0 {
		return nil
	}
	index += int(t.batch.materializationPatchCount)
	if index >= len(t.batch.pages) {
		return nil
	}
	return &t.batch.pages[index]
}

func (t *WriteTransaction) fullWrites() []Write {
	if t == nil || t.batch == nil {
		return nil
	}
	return t.batch.pages[t.batch.materializationPatchCount:]
}

func (t *WriteTransaction) resizePages(pageCount int) error {
	if t == nil || t.batch == nil {
		return ErrBatchState
	}
	var err error
	if t.batch.materialized {
		err = t.batch.resizeMaterializationPages(pageCount)
	} else {
		err = t.batch.ResizePages(pageCount)
	}
	return err
}

// MaterializationSequence returns the capsule sequence reserved by a hybrid
// transaction.
func (t *WriteTransaction) MaterializationSequence() (uint64, error) {
	if t == nil || !t.active || t.batch == nil || !t.batch.materialized {
		return 0, ErrBatchState
	}
	return t.batch.MaterializationSequence()
}

// MaterializationJournalBuffer returns the hybrid transaction's private
// journal-capsule buffer.
func (t *WriteTransaction) MaterializationJournalBuffer() ([]byte, error) {
	if t == nil || !t.active || t.batch == nil || !t.batch.materialized {
		return nil, ErrBatchState
	}
	return t.batch.MaterializationJournalBuffer()
}

// SealMaterializationJournal validates and freezes the hybrid transaction's
// recovery capsule.
func (t *WriteTransaction) SealMaterializationJournal() error {
	if t == nil || !t.active || t.batch == nil || !t.batch.materialized {
		return ErrBatchState
	}
	return t.batch.SealMaterializationJournal()
}

// StageMaterializationTarget checksum-proves one complete canonical
// after-image and extracts only its journal-covered sectors.
func (t *WriteTransaction) StageMaterializationTarget(
	targetRank int, after []byte,
) error {
	if t == nil || !t.active || t.batch == nil || !t.batch.materialized {
		return ErrBatchState
	}
	return t.batch.StageMaterializationTarget(targetRank, after)
}

// StageBuiltMaterializationTarget stages the exact trusted target returned by
// BuildMaterializationTarget while reusing its already-verified after checksum.
// Direct or decoded journal targets must use StageMaterializationTarget.
func (t *WriteTransaction) StageBuiltMaterializationTarget(
	targetRank int,
	built MaterializationTarget,
	after []byte,
) error {
	if t == nil || !t.active || t.batch == nil || !t.batch.materialized {
		return ErrBatchState
	}
	return t.batch.StageBuiltMaterializationTarget(targetRank, built, after)
}

// Allocate reserves one append-only extent. logicalID zero allocates a new
// logical identity; non-zero rewrites that logical page at the new generation.
func (t *WriteTransaction) Allocate(kind PageKind, length uint32, logicalID uint64) (TransactionPage, error) {
	return t.allocate(kind, length, logicalID, 0)
}

// AllocateNear is Allocate with a placement hint: a preferred file offset,
// typically the current offset of the page this copy-on-write replaces. When a
// reusable extent large enough for the request exists, the allocator takes the
// one physically nearest hint rather than the lowest-offset first fit, so a
// retired page's neighbourhood is recycled in place instead of the request
// growing the file tail. A zero hint, an absent reusable index, or the absence
// of any reusable fit falls back to exact Allocate behaviour, so AllocateNear
// never changes which requests can be satisfied — only where a reused extent is
// taken from.
func (t *WriteTransaction) AllocateNear(kind PageKind, length uint32, logicalID, hint uint64) (TransactionPage, error) {
	return t.allocate(kind, length, logicalID, hint)
}

func (t *WriteTransaction) allocate(kind PageKind, length uint32, logicalID, hint uint64) (TransactionPage, error) {
	validLength := validPhysicalPageSize(length)
	if kind == PageOverflow || kind == PagePrimaryLeaf {
		validLength = validPageExtentSize(kind, length)
	}
	if t == nil || !t.active || t.batch == nil || !validPageKind(kind) ||
		!validLength || length < t.options.PageSize || length%t.options.PageSize != 0 {
		return TransactionPage{}, ErrTooManyPages
	}
	if !variableTransactionExtent(kind) && length != t.options.PageSize {
		return TransactionPage{}, fmt.Errorf("%w: variable metadata extent", ErrInvalidWrite)
	}
	if uint64(length) > uint64(t.committer.bufferSize) {
		return TransactionPage{}, ErrTooManyPages
	}
	index := t.allocated
	writeIndex := index + int(t.batch.materializationPatchCount)
	if writeIndex >= len(t.batch.pages) {
		return TransactionPage{}, ErrTooManyPages
	}
	write := &t.batch.pages[writeIndex]
	allocateLogicalID := logicalID == 0
	if logicalID == 0 {
		logicalID = t.nextID
		if logicalID == 0 || logicalID == ^uint64(0) {
			return TransactionPage{}, fmt.Errorf("%w: logical id exhausted", ErrInvalidWrite)
		}
	} else if logicalID >= t.nextID {
		return TransactionPage{}, fmt.Errorf("%w: replacement logical id", ErrInvalidWrite)
	}
	offset, reused, err := t.allocatePhysicalNear(length, hint)
	if err != nil {
		return TransactionPage{}, err
	}
	if allocateLogicalID {
		t.nextID++
	}
	ref := PageRef{
		Offset: offset, LogicalID: logicalID, Generation: t.options.Generation,
		Length: length, Kind: kind,
	}
	frameIndex := -1
	var buffer []byte
	if t.cache != nil {
		frameIndex, buffer, err = t.cache.reserveDirtyFrame(
			ref, t.options.Generation,
		)
		if err != nil {
			t.rollbackPhysicalAllocation(reused, allocateLogicalID)
			return TransactionPage{}, err
		}
		write.frameIndex = uint32(frameIndex)
		write.pendingFlags |= pendingWriteFrameNative
	} else {
		buffer = t.committer.buffers[write.Buffer][:int(length):int(length)]
	}
	if !reused {
		t.fileEnd += uint64(length)
	}
	t.allocated++
	return TransactionPage{
		tx: t, index: index, ref: ref,
		bytes: buffer[:int(length):int(length)], frame: frameIndex,
	}, nil
}

func (t *WriteTransaction) rollbackPhysicalAllocation(
	reused, allocatedLogicalID bool,
) {
	if allocatedLogicalID {
		t.nextID--
	}
	if reused && len(t.reuseEdits) != 0 {
		edit := t.reuseEdits[len(t.reuseEdits)-1]
		t.options.Reusable[edit.Index] = edit.Before
		if t.options.ReusableIndex != nil {
			t.options.ReusableIndex.Update(
				t.options.Reusable, int(edit.Index),
			)
		}
		t.reuseEdits = t.reuseEdits[:len(t.reuseEdits)-1]
	}
}

// variableTransactionExtent reports immutable value/accelerator pages whose
// packed size is intentionally independent of the metadata allocation
// quantum. Tree nodes and publication roots remain exactly one PageSize so
// their bounded copy-on-write geometry cannot silently expand.
func variableTransactionExtent(kind PageKind) bool {
	switch kind {
	case PageOverflow, PagePrimaryCatalog, PagePrimaryLocator, PageTabletRoute,
		PagePrimaryAnchor, PagePrimaryLeaf,
		PagePrimaryExactRoot, PagePrimaryExactLeaf, PagePrimaryExactCatalog,
		PageMigrationExactRun, PageMigrationStagingChain, PageMigrationPadding:
		return true
	default:
		return false
	}
}

// allocatePhysicalNear prefers the resident reusable extent nearest hint before
// falling back to lowest-offset first fit. It only diverges from allocatePhysical
// when a hint is present, reuse is live, an index is available, and that index
// finds a resident fit: exactly the copy-on-write case that wants to stay in its
// physical neighbourhood. Every other case, including a hinted request with no
// nearby resident fit, defers to allocatePhysical so the promoter and file-tail
// append still serve requests the resident set cannot.
func (t *WriteTransaction) allocatePhysicalNear(length uint32, hint uint64) (uint64, bool, error) {
	if hint != 0 && t.reuseEnabled && t.options.ReusableIndex != nil {
		want := uint64(length)
		if index, found := t.options.ReusableIndex.NearestFit(
			t.options.Reusable, want, hint,
		); found {
			return t.allocateFromReusable(index, t.options.Reusable[index], want)
		}
	}
	return t.allocatePhysical(length)
}

// allocatePhysical takes the first extent large enough, scanning the whole
// reusable set from its lowest offset. A transaction is no longer confined to
// one extent: the free log records a commit's complete diff however many
// extents it touched, so there is nothing left to bound. Confining it was what
// made a store with plenty of free space in several extents grow the file to
// satisfy a request none of them could serve alone.
//
// First fit rather than best fit, and from the low end, because it concentrates
// allocation into the extents nearest the start of the file and leaves the high
// ones whole. Best fit would spread small allocations across every extent and
// so widen each commit's diff for no gain in packing.
func (t *WriteTransaction) allocatePhysical(length uint32) (uint64, bool, error) {
	want := uint64(length)
	if t.reuseEnabled {
		index, found := t.firstReusableFit(want)
		if t.options.ReusablePromoter != nil {
			before := ^uint64(0)
			if found {
				before = t.options.Reusable[index].Offset
			}
			reusable, changed, err := t.options.ReusablePromoter.PromoteReusable(
				want, before, t.reuseEdits,
			)
			if changed {
				t.options.Reusable = reusable
				if remapErr := t.remapReuseEdits(); remapErr != nil {
					return 0, false, remapErr
				}
				if t.options.ReusableIndex != nil &&
					t.options.ReusableIndex.Len() != len(t.options.Reusable) {
					return 0, false, fmt.Errorf("%w: promoted reusable extent index", ErrInvalidWrite)
				}
				index, found = t.firstReusableFit(want)
			}
			if err != nil {
				return 0, false, err
			}
		}
		if found {
			return t.allocateFromReusable(index, t.options.Reusable[index], want)
		}
	}
	if want > maxSuperblockFileOffset-t.fileEnd {
		return 0, false, fmt.Errorf("%w: physical file exhausted", ErrInvalidWrite)
	}
	if t.options.PhysicalHighWaterBytes != 0 &&
		want > t.options.PhysicalHighWaterBytes-t.fileEnd {
		return 0, false, fmt.Errorf(
			"%w: need=%d allocated=%d", ErrPhysicalCapacity,
			t.fileEnd+want, t.options.PhysicalHighWaterBytes,
		)
	}
	return t.fileEnd, false, nil
}

func (t *WriteTransaction) firstReusableFit(want uint64) (int, bool) {
	if len(t.options.Reusable) == 0 {
		return 0, false
	}
	// Keep the overwhelmingly common first-extent case independent of the
	// hierarchy walk and its validation branches.
	if t.options.Reusable[0].Length >= want {
		return 0, true
	}
	if t.options.ReusableIndex != nil {
		return t.options.ReusableIndex.firstFitAfterFirst(t.options.Reusable, want)
	}
	for i := 1; i < len(t.options.Reusable); i++ {
		if t.options.Reusable[i].Length >= want {
			return i, true
		}
	}
	return 0, false
}

func (t *WriteTransaction) allocateFromReusable(index int, extent FreeExtent, want uint64) (uint64, bool, error) {
	layout, err := MutableStoreLayout(t.options.PageSize)
	if err != nil {
		return 0, false, err
	}
	if extent.Offset < layout.DataStart || extent.Offset%uint64(t.options.PageSize) != 0 ||
		extent.Length%uint64(t.options.PageSize) != 0 || extent.Length > t.options.FileEnd ||
		extent.Offset > t.options.FileEnd-extent.Length ||
		extent.RetiredGeneration == 0 || extent.RetiredGeneration >= t.options.Generation {
		return 0, false, fmt.Errorf("%w: reusable extent", ErrInvalidWrite)
	}
	if len(t.reuseEdits) == cap(t.reuseEdits) {
		return 0, false, ErrTooManyPages
	}
	t.reuseEdits = append(t.reuseEdits, ReuseEdit{Index: uint32(index), Before: extent})
	offset := extent.Offset + extent.Length - want
	extent.Length -= want
	if extent.Length == 0 {
		// Keep the offset while the transaction is active. A lazy promotion may
		// insert an earlier extent and remap rollback records by offset; the
		// zero length still makes this entry unavailable to the allocator and
		// finalize removes it after publication.
		extent = FreeExtent{
			Offset: extent.Offset, RetiredGeneration: extent.RetiredGeneration,
		}
	}
	t.options.Reusable[index] = extent
	if t.options.ReusableIndex != nil &&
		!t.options.ReusableIndex.Update(t.options.Reusable, index) {
		t.options.Reusable[index] = t.reuseEdits[len(t.reuseEdits)-1].Before
		t.reuseEdits = t.reuseEdits[:len(t.reuseEdits)-1]
		return 0, false, fmt.Errorf("%w: reusable extent index update", ErrInvalidWrite)
	}
	return offset, true, nil
}

// remapReuseEdits restores journal ranks after a promoter inserts or evicts
// untouched extents. Consumed entries retain their offset until finalize, so
// the remap is exact even when an allocation took an extent whole.
func (t *WriteTransaction) remapReuseEdits() error {
	for i := range t.reuseEdits {
		offset := t.reuseEdits[i].Before.Offset
		lo, hi := 0, len(t.options.Reusable)
		for lo < hi {
			middle := int(uint(lo+hi) >> 1)
			if t.options.Reusable[middle].Offset < offset {
				lo = middle + 1
			} else {
				hi = middle
			}
		}
		if lo == len(t.options.Reusable) ||
			t.options.Reusable[lo].Offset != offset {
			return fmt.Errorf("%w: promoted reusable extent journal", ErrInvalidWrite)
		}
		t.reuseEdits[i].Index = uint32(lo)
	}
	return nil
}

// DisableReuse seals allocator edits for this transaction. Subsequent pages
// append above FileEnd and so change nothing the free log has already been
// asked to describe. The free-log writer uses it as its termination guarantee:
// sizing the delta pages is a fixed point, and if the fixed point has not
// settled after a bounded number of rounds, the remaining pages are appended
// instead, which cannot produce another round.
func (t *WriteTransaction) DisableReuse() {
	if t != nil {
		t.reuseEnabled = false
	}
}

// DisableReusablePromotion freezes the reusable slice's shape while metadata
// code holds ranges or scratch derived from it. Reuse of already resident
// extents remains enabled.
func (t *WriteTransaction) DisableReusablePromotion() {
	if t != nil {
		t.options.ReusablePromoter = nil
	}
}

// ReuseEdits returns the caller-owned allocator journal accumulated so far.
func (t *WriteTransaction) ReuseEdits() []ReuseEdit {
	if t == nil {
		return nil
	}
	return t.reuseEdits
}

// FileEnd returns the prospective exclusive allocation high-water mark.
func (t *WriteTransaction) FileEnd() uint64 {
	if t == nil {
		return 0
	}
	return t.fileEnd
}

// NextLogicalID returns the prospective logical high-water mark.
func (t *WriteTransaction) NextLogicalID() uint64 {
	if t == nil {
		return 0
	}
	return t.nextID
}

// Generation returns the transaction publication generation.
func (t *WriteTransaction) Generation() uint64 {
	if t == nil {
		return 0
	}
	return t.options.Generation
}

// StoreID returns the transaction's immutable collection identity.
func (t *WriteTransaction) StoreID() [16]byte {
	if t == nil {
		return [16]byte{}
	}
	return t.options.StoreID
}

// Abort releases every reserved buffer. It is idempotent after a successful
// publication only in the sense that it returns ErrBatchState and changes
// nothing.
func (t *WriteTransaction) Abort() error {
	if t == nil || !t.active || t.batch == nil {
		return ErrBatchState
	}
	err := t.batch.Abort()
	if err == nil {
		for i := len(t.reuseEdits) - 1; i >= 0; i-- {
			edit := t.reuseEdits[i]
			t.options.Reusable[edit.Index] = edit.Before
			if t.options.ReusableIndex != nil &&
				!t.options.ReusableIndex.Update(t.options.Reusable, int(edit.Index)) {
				err = fmt.Errorf("%w: reusable extent index rollback", ErrInvalidWrite)
			}
		}
		t.reuseEdits = t.reuseEdits[:0]
		t.active = false
		t.batch = nil
		if t.cache != nil {
			err = t.cache.DiscardDirty(t.options.Generation)
		}
	}
	return err
}
