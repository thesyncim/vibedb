package storeio

import (
	"errors"
	"fmt"
	"io"
	"os"
)

// MutableInlineRecovery is the bounded result of recovering an inline-root
// mutable Store. Catalog is the exact immutable definition already validated
// while selecting Root, so Open need not read and decode the same chain again.
// JournalSlot is -1 and JournalSequence is zero when neither fixed capsule slot
// contains a valid committed journal.
//
// The journal coordinates returned here seed the next writer: it must advance
// JournalSequence and write the slot opposite JournalSlot. Deriving the slot
// from sequence parity after recovery is unsafe because one physical slot may
// have been torn independently of the sequence it was intended to carry.
type MutableInlineRecovery struct {
	Root               InlineSuperblock
	State              StateRoot
	RootSlot           int
	FallbackGeneration uint64
	JournalSlot        int
	JournalSequence    uint64
	Catalog            *CanonicalPageCatalog
}

// RecoverMutableInlineStateRoot recovers inline roots together with the
// allocator-excluded materialization undo capsules.
//
// Recovery first orders structurally valid roots without dereferencing their
// pages. It then resolves the newest valid capsule against each root candidate.
// A candidate older than the capsule target is exposed only after every target
// has been preflighted, rolled back, and synchronized. A candidate exactly at
// the target is exposed only when every target is its strong after-image. A
// newer candidate never inspects the stale capsule because later serialized
// commits may already have reused its former target extents.
//
// pageScratch belongs to the caller and must hold the largest top-level
// reference or journal target named by either root. The fixed root and journal
// records use separate stack storage because MaterializationJournalView borrows
// its encoded capsule for the complete recovery operation.
//
// The caller must hold the Store writer lock. It must not open a PageCache or
// Committer over file until this function returns successfully.
func RecoverMutableInlineStateRoot(
	file *os.File,
	pageSize, damageGranule uint32,
	pageScratch []byte,
) (MutableInlineRecovery, error) {
	return recoverMutableInlineStateRoot(
		file, pageSize, damageGranule, pageScratch,
		// A repaired canonical page must cross the same power-loss boundary
		// required after the journal phase. Ordinary fsync is not that
		// boundary on every supported platform (notably Darwin).
		func() error { return materializationSync(file) },
	)
}

func recoverMutableInlineStateRoot(
	file *os.File,
	pageSize, damageGranule uint32,
	pageScratch []byte,
	syncFile func() error,
) (MutableInlineRecovery, error) {
	if file == nil || syncFile == nil || !validPhysicalPageSize(pageSize) ||
		damageGranule != 0 &&
			(damageGranule < MaterializationJournalMinSectorSize ||
				damageGranule&(damageGranule-1) != 0 ||
				damageGranule > pageSize || pageSize%damageGranule != 0) {
		return MutableInlineRecovery{}, fmt.Errorf(
			"%w: invalid mutable recovery file or damage granule", ErrInvalidWrite,
		)
	}
	layout, err := MutableStoreLayout(pageSize)
	if err != nil {
		return MutableInlineRecovery{}, err
	}
	var rootRecords [superblockCopies][InlineSuperblockSize]byte
	for slot := range rootRecords {
		if err := readRecoveryRecord(
			file, rootRecords[slot][:], int64(layout.RootOffsets[slot]),
		); err != nil {
			return MutableInlineRecovery{}, err
		}
	}
	candidates, candidateCount, err := orderedInlineSuperblocks(
		rootRecords[0][:], rootRecords[1][:],
	)
	if err != nil {
		return MutableInlineRecovery{}, err
	}
	persistedGranule :=
		candidates[0].root.State.MaterializationDamageGranule
	for rank := 1; rank < candidateCount; rank++ {
		if candidates[rank].root.State.MaterializationDamageGranule !=
			persistedGranule {
			return MutableInlineRecovery{},
				ErrMaterializationJournalConflict
		}
	}
	if damageGranule == 0 {
		damageGranule = persistedGranule
	} else if damageGranule != persistedGranule {
		return MutableInlineRecovery{}, ErrMaterializationJournalConflict
	}

	info, err := file.Stat()
	if err != nil {
		return MutableInlineRecovery{}, err
	}
	if info.Size() < 0 {
		return MutableInlineRecovery{}, ErrSuperblockNotFound
	}
	fileSize := uint64(info.Size())

	var journalRecords [materializationJournalCopies][MaterializationJournalSize]byte
	for slot := range journalRecords {
		if err := readRecoveryRecord(
			file, journalRecords[slot][:],
			int64(layout.MaterializationJournalOffsets[slot]),
		); err != nil {
			return MutableInlineRecovery{}, err
		}
	}
	journal, journalSlot, journalPresent, err := selectRecoveryMaterializationJournal(
		journalRecords[0][:], journalRecords[1][:],
	)
	if err != nil {
		return MutableInlineRecovery{}, err
	}
	if journalPresent {
		header := journal.Header()
		newest := candidates[0].root
		if damageGranule == 0 ||
			header.StoreID != newest.StoreID ||
			header.PageSize != pageSize ||
			header.SectorSize != damageGranule {
			return MutableInlineRecovery{}, ErrMaterializationJournalConflict
		}
	}

	scratchNeed := int(pageSize)
	for rank := range candidateCount {
		scratchNeed = max(scratchNeed, inlineRecoveryScratchNeed(candidates[rank].root))
	}
	if journalPresent {
		for rank := 0; rank < journal.Len(); rank++ {
			target, _ := journal.TargetAt(rank)
			scratchNeed = max(scratchNeed, int(target.Ref.Length))
		}
	}
	if len(pageScratch) < scratchNeed {
		return MutableInlineRecovery{}, fmt.Errorf(
			"%w: have=%d need=%d",
			ErrRecoveryBufferTooSmall, len(pageScratch), scratchNeed,
		)
	}

	selectedRank := -1
	rolledBack := false
	var catalogErr error
	var recoveredCatalog *CanonicalPageCatalog
	var exactTargetRootSlots uint8
	if journalPresent {
		targetGeneration := journal.Header().TargetGeneration
		for rank := range candidateCount {
			if candidates[rank].root.Generation == targetGeneration {
				exactTargetRootSlots |= uint8(1) << candidates[rank].slot
			}
		}
	}
	for rank := range candidateCount {
		candidate := candidates[rank]
		root := candidate.root
		if root.PageSize != pageSize || root.FileEnd > fileSize {
			continue
		}
		if journalPresent {
			targetGeneration := journal.Header().TargetGeneration
			switch {
			case root.Generation == targetGeneration:
				if err := validateJournalTargetsForCandidate(
					journal, root, layout, fileSize,
				); err != nil {
					return MutableInlineRecovery{}, err
				}
				exact, exactErr := validateMaterializationAfterImages(
					file, journal, pageScratch,
				)
				if exactErr != nil {
					return MutableInlineRecovery{}, exactErr
				}
				if !exact {
					continue
				}
			case root.Generation < targetGeneration && !rolledBack:
				if err := validateJournalTargetsForCandidate(
					journal, root, layout, fileSize,
				); err != nil {
					return MutableInlineRecovery{}, err
				}
				if err := preflightMaterializationRollback(
					file, journal, pageScratch,
				); err != nil {
					return MutableInlineRecovery{}, err
				}
				if err := applyMaterializationRollback(
					file, journal, pageScratch, syncFile,
				); err != nil {
					return MutableInlineRecovery{}, err
				}
				rolledBack = true
			}
		}
		ok, candidateCatalog, validateErr := validateRecoveredInlineRefs(
			file, root, pageScratch, recoveredCatalog,
		)
		if candidateCatalog != nil {
			recoveredCatalog = candidateCatalog
		}
		if validateErr != nil {
			if errors.Is(validateErr, ErrPageCatalogCorrupt) {
				catalogErr = errors.Join(catalogErr, validateErr)
				continue
			}
			return MutableInlineRecovery{}, validateErr
		}
		if ok {
			selectedRank = rank
			break
		}
	}
	if selectedRank < 0 {
		if catalogErr != nil {
			return MutableInlineRecovery{},
				errors.Join(ErrSuperblockNotFound, catalogErr)
		}
		return MutableInlineRecovery{}, ErrSuperblockNotFound
	}

	selected := candidates[selectedRank]
	if rolledBack {
		// Cancellation has three separately durable prefixes. The capsule
		// remains authoritative through rollback and exact-root invalidation;
		// only then may it disappear. Otherwise a later open could accept a
		// checksum-valid target root whose canonical pages were just restored
		// to the preceding generation.
		if exactTargetRootSlots != 0 {
			if err := invalidateRecoveryInlineRoots(
				file, layout.RootOffsets, &rootRecords,
				exactTargetRootSlots, syncFile,
			); err != nil {
				return MutableInlineRecovery{}, err
			}
		}
		if err := clearRecoveryMaterializationJournal(
			file,
			int64(layout.MaterializationJournalOffsets[journalSlot]),
			journalRecords[journalSlot][:],
			syncFile,
		); err != nil {
			return MutableInlineRecovery{}, err
		}
		journal, journalSlot, journalPresent, err =
			selectRecoveryMaterializationJournal(
				journalRecords[0][:], journalRecords[1][:],
			)
		if err != nil {
			return MutableInlineRecovery{}, err
		}
	}

	fallbackGeneration := selected.root.Generation
	for rank := selectedRank + 1; rank < candidateCount; rank++ {
		candidate := candidates[rank]
		root := candidate.root
		if root.PageSize != pageSize || root.FileEnd > fileSize {
			continue
		}
		// A committed selected root needs the after-images. Rolling the file
		// back merely to validate its older physical alternate would destroy
		// the selected root. Treat the selected generation itself as the
		// conservative one-root recovery fence. After cancellation, journal
		// names the surviving prior capsule, so this also preserves the
		// one-root fence of a preceding successful materialization.
		if journalPresent &&
			selected.root.Generation >= journal.Header().TargetGeneration &&
			root.Generation < journal.Header().TargetGeneration {
			continue
		}
		if journalPresent &&
			root.Generation == journal.Header().TargetGeneration {
			if err := validateJournalTargetsForCandidate(
				journal, root, layout, fileSize,
			); err != nil {
				return MutableInlineRecovery{}, err
			}
			exact, exactErr := validateMaterializationAfterImages(
				file, journal, pageScratch,
			)
			if exactErr != nil {
				return MutableInlineRecovery{}, exactErr
			}
			if !exact {
				continue
			}
		}
		ok, candidateCatalog, validateErr := validateRecoveredInlineRefs(
			file, root, pageScratch, recoveredCatalog,
		)
		if candidateCatalog != nil {
			recoveredCatalog = candidateCatalog
		}
		if validateErr != nil {
			if errors.Is(validateErr, ErrPageCatalogCorrupt) {
				continue
			}
			return MutableInlineRecovery{}, validateErr
		}
		if ok {
			fallbackGeneration = root.Generation
			break
		}
	}

	result := MutableInlineRecovery{
		Root: selected.root, State: selected.root.State,
		RootSlot: selected.slot, FallbackGeneration: fallbackGeneration,
		JournalSlot: -1, Catalog: recoveredCatalog,
	}
	if journalPresent {
		result.JournalSlot = journalSlot
		result.JournalSequence = journal.Header().Sequence
	}
	return result, nil
}

func invalidateRecoveryInlineRoots(
	file *os.File,
	offsets [superblockCopies]uint64,
	records *[superblockCopies][InlineSuperblockSize]byte,
	slots uint8,
	syncFile func() error,
) error {
	if file == nil || records == nil || slots == 0 ||
		slots>>superblockCopies != 0 || syncFile == nil {
		return ErrInvalidWrite
	}
	wrote := false
	for slot := range superblockCopies {
		if slots&(uint8(1)<<slot) == 0 {
			continue
		}
		clear(records[slot][:])
		n, err := file.WriteAt(records[slot][:], int64(offsets[slot]))
		if n != 0 {
			wrote = true
		}
		if err == nil && n != len(records[slot]) {
			err = io.ErrShortWrite
		}
		if err != nil {
			return syncMaterializationRepairOnError(syncFile, wrote, err)
		}
	}
	return syncFile()
}

func clearRecoveryMaterializationJournal(
	file *os.File,
	offset int64,
	record []byte,
	syncFile func() error,
) error {
	if file == nil || offset < 0 ||
		len(record) != MaterializationJournalSize || syncFile == nil {
		return ErrInvalidWrite
	}
	clear(record)
	n, err := file.WriteAt(record, offset)
	if err == nil && n != len(record) {
		err = io.ErrShortWrite
	}
	if err != nil {
		if n == 0 {
			return err
		}
		return errors.Join(err, syncFile())
	}
	return syncFile()
}

func validateMaterializationAfterImages(
	file *os.File,
	journal MaterializationJournalView,
	pageScratch []byte,
) (bool, error) {
	for rank := 0; rank < journal.Len(); rank++ {
		target, _ := journal.TargetAt(rank)
		page := pageScratch[:int(target.Ref.Length)]
		ok, err := readRecoveryPage(file, target.Ref, page)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
		if err := journal.ValidateAfterImage(rank, page); err != nil {
			if errors.Is(err, ErrMaterializationTargetDiverged) {
				return false, nil
			}
			return false, err
		}
	}
	return true, nil
}

func readRecoveryRecord(file *os.File, dst []byte, offset int64) error {
	clear(dst)
	n, err := file.ReadAt(dst, offset)
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	if n < len(dst) {
		clear(dst[n:])
	}
	return nil
}

func selectRecoveryMaterializationJournal(
	first, second []byte,
) (MaterializationJournalView, int, bool, error) {
	journal, slot, err := SelectMaterializationJournal(first, second)
	if errors.Is(err, ErrMaterializationJournalNotFound) {
		return MaterializationJournalView{}, -1, false, nil
	}
	if err != nil {
		return MaterializationJournalView{}, -1, false, err
	}
	return journal, slot, true, nil
}

func inlineRecoveryScratchNeed(root InlineSuperblock) int {
	need := int(root.PageSize)
	refs := [...]PageRef{
		root.State.ChunkDirectory,
		root.State.KeyDirectory,
		root.State.IndexDirectory,
		root.State.Float64ScanHead,
		root.State.IndexGroupHead,
		root.State.PageCatalogHead,
		root.State.PrimaryRoot,
		root.State.ExactIndexRoot,
		root.FreeDelta.IndexHead(),
		root.FreeDelta.ExternalPrev(),
	}
	for _, ref := range refs {
		need = max(need, int(ref.Length))
	}
	return need
}

func validateJournalTargetsForCandidate(
	journal MaterializationJournalView,
	root InlineSuperblock,
	layout MutableStoreFileLayout,
	fileSize uint64,
) error {
	catalogExtent, catalogLogicalEnd, hasCatalog :=
		stateRootPageCatalogRun(root.State)
	for rank := 0; rank < journal.Len(); rank++ {
		target, _ := journal.TargetAt(rank)
		ref := target.Ref
		end := ref.Offset + uint64(ref.Length)
		if target.StoreID != root.StoreID ||
			ref.Offset < layout.DataStart ||
			end < ref.Offset ||
			end > root.FileEnd ||
			end > fileSize {
			return fmt.Errorf(
				"%w: materialization target outside fallback root",
				ErrMaterializationJournalConflict,
			)
		}
		if hasCatalog {
			targetExtent := FreeExtent{
				Offset: ref.Offset,
				Length: uint64(ref.Length),
			}
			if extentsOverlap(targetExtent, catalogExtent) ||
				ref.LogicalID >= root.State.PageCatalogHead.LogicalID &&
					ref.LogicalID < catalogLogicalEnd {
				return fmt.Errorf(
					"%w: materialization target overlaps immutable catalog",
					ErrMaterializationJournalConflict,
				)
			}
		}
	}
	return nil
}

// preflightMaterializationRollback proves every target is recoverable before
// the first repair write. It prevents deterministic corruption in a later
// target from leaving an earlier target partially repaired.
func preflightMaterializationRollback(
	file *os.File,
	journal MaterializationJournalView,
	pageScratch []byte,
) error {
	for rank := 0; rank < journal.Len(); rank++ {
		target, _ := journal.TargetAt(rank)
		page := pageScratch[:int(target.Ref.Length)]
		ok, err := readRecoveryPage(file, target.Ref, page)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf(
				"%w: short materialization target", ErrMaterializationTargetDiverged,
			)
		}
		if _, err := journal.Rollback(rank, page); err != nil {
			return err
		}
	}
	return nil
}

func applyMaterializationRollback(
	file *os.File,
	journal MaterializationJournalView,
	pageScratch []byte,
	syncFile func() error,
) error {
	wrote := false
	for rank := 0; rank < journal.Len(); rank++ {
		target, _ := journal.TargetAt(rank)
		page := pageScratch[:int(target.Ref.Length)]
		ok, err := readRecoveryPage(file, target.Ref, page)
		if err != nil {
			return syncMaterializationRepairOnError(syncFile, wrote, err)
		}
		if !ok {
			return syncMaterializationRepairOnError(
				syncFile, wrote,
				fmt.Errorf(
					"%w: short materialization target",
					ErrMaterializationTargetDiverged,
				),
			)
		}
		result, err := journal.Rollback(rank, page)
		if err != nil {
			return syncMaterializationRepairOnError(syncFile, wrote, err)
		}
		if result != MaterializationRollbackApplied {
			continue
		}
		first, count, _ := journal.TargetPatchRange(rank)
		for patchRank := first; patchRank < first+count; patchRank++ {
			patch, _ := journal.PatchAt(patchRank)
			src := page[patch.Offset:uint32(uint64(patch.Offset)+uint64(len(patch.Data)))]
			n, writeErr := file.WriteAt(
				src, int64(target.Ref.Offset+uint64(patch.Offset)),
			)
			if n != 0 {
				wrote = true
			}
			if writeErr == nil && n != len(src) {
				writeErr = io.ErrShortWrite
			}
			if writeErr != nil {
				return syncMaterializationRepairOnError(syncFile, wrote, writeErr)
			}
		}
	}
	// Even an already-restored image may come from a previous process recovery
	// that stopped after its writes and before its barrier.
	return syncFile()
}

func syncMaterializationRepairOnError(
	syncFile func() error,
	wrote bool,
	primary error,
) error {
	if !wrote {
		return primary
	}
	return errors.Join(primary, syncFile())
}

func readRecoveryPage(
	file *os.File,
	ref PageRef,
	dst []byte,
) (bool, error) {
	n, err := file.ReadAt(dst, int64(ref.Offset))
	if err != nil && !errors.Is(err, io.EOF) {
		return false, err
	}
	return n == len(dst), nil
}

func validateRecoveredInlineRefs(
	file *os.File,
	root InlineSuperblock,
	scratch []byte,
	catalog *CanonicalPageCatalog,
) (bool, *CanonicalPageCatalog, error) {
	stateRefs := [...]PageRef{
		root.State.ChunkDirectory,
		root.State.KeyDirectory,
		root.State.IndexDirectory,
		root.State.Float64ScanHead,
		root.State.IndexGroupHead,
		root.State.PrimaryRoot,
		root.State.ExactIndexRoot,
	}
	for _, ref := range stateRefs {
		if ref == (PageRef{}) {
			continue
		}
		ok, err := validateRecoveredStateRef(file, root, ref, scratch)
		if err != nil || !ok {
			return ok, catalog, err
		}
	}
	if catalog == nil {
		var catalogErr error
		catalog, catalogErr = openRecoveredPageCatalog(
			file, root.State, root.FileEnd, scratch,
		)
		if catalogErr != nil {
			return false, nil, catalogErr
		}
	}

	indexHead := root.FreeDelta.IndexHead()
	if indexHead != (PageRef{}) {
		page := scratch[:int(indexHead.Length)]
		ok, err := readRecoveryPage(file, indexHead, page)
		if err != nil || !ok {
			return ok, catalog, err
		}
		index, openErr := OpenFreeIndexPage(
			page, root.FileEnd, root.State.NextLogicalID,
		)
		header := index.Header()
		if openErr != nil ||
			!recoveryFreeHeaderMatchesRef(header, root.StoreID, indexHead) ||
			header.PageSize != root.PageSize ||
			header.Generation > root.Generation {
			return false, catalog, nil
		}
	}

	externalPrev := root.FreeDelta.ExternalPrev()
	if externalPrev != (PageRef{}) {
		page := scratch[:int(externalPrev.Length)]
		ok, err := readRecoveryPage(file, externalPrev, page)
		if err != nil || !ok {
			return ok, catalog, err
		}
		delta, openErr := OpenFreeDeltaPage(
			page, root.FileEnd, root.State.NextLogicalID,
		)
		header := delta.Header()
		if openErr != nil ||
			!recoveryFreeHeaderMatchesRef(header, root.StoreID, externalPrev) ||
			header.PageSize != root.PageSize ||
			header.Generation > root.Generation ||
			delta.IndexHead() != indexHead {
			return false, catalog, nil
		}
	}
	return true, catalog, nil
}

func validateRecoveredStateRef(
	file *os.File,
	root InlineSuperblock,
	ref PageRef,
	scratch []byte,
) (bool, error) {
	page := scratch[:int(ref.Length)]
	ok, err := readRecoveryPage(file, ref, page)
	if err != nil || !ok {
		return ok, err
	}
	header, _, openErr := OpenPage(page)
	if openErr != nil ||
		!recoveryPageHeaderMatchesRef(header, root.StoreID, ref) {
		return false, nil
	}

	state := root.State
	switch ref.Kind {
	case PageChunkDirectory:
		_, openErr = OpenChunkDirectoryPage(
			page, root.FileEnd, state.NextLogicalID,
		)
	case PageKeyDirectory:
		_, openErr = OpenKeyDirectoryPage(
			page, root.FileEnd, state.NextLogicalID,
			state.ChunkHighWater, uint8(state.ChunkDocuments),
		)
	case PageFingerprintDirectory:
		_, openErr = OpenPageFingerprintDirectory(
			page, root.FileEnd, state.NextLogicalID,
			state.ChunkHighWater, state.ChunkDocuments,
		)
	case PageIndexDirectory:
		_, openErr = OpenIndexDirectoryPage(
			page, root.FileEnd, state.NextLogicalID, state.IndexCount,
		)
	case PageFloat64Catalog:
		_, openErr = OpenFloat64Directory(
			page, root.FileEnd, state.NextLogicalID, root.PageSize,
		)
	case PageIndexGroupCatalog:
		_, openErr = OpenIndexGroupCatalog(
			page, state.IndexCount, state.ChunkHighWater,
			state.ChunkDocuments, root.FileEnd, state.NextLogicalID,
			root.PageSize,
		)
	case PagePrimaryCatalog:
		_, openErr = OpenGlobalTabletCatalogNode(
			page, ref, GlobalTabletCatalogBounds{
				StoreID:                state.StoreID,
				SelectedRootGeneration: state.Generation,
				FileEnd:                root.FileEnd, NextLogicalID: state.NextLogicalID,
			},
		)
	case PagePrimaryExactRoot:
		_, openErr = OpenPrimaryExactRootPage(
			page, ref, PrimaryExactIndexBounds{
				StoreID: state.StoreID, Generation: state.Generation,
				FileEnd: root.FileEnd, NextLogicalID: state.NextLogicalID,
				AllocationQuantum: state.PageSize,
				MaxPageSize:       state.MaxPageSize, IndexCount: state.IndexCount,
			},
		)
	default:
		return false, nil
	}
	return openErr == nil, nil
}

func recoveryPageHeaderMatchesRef(
	header PageHeader,
	storeID [16]byte,
	ref PageRef,
) bool {
	return header.StoreID == storeID &&
		header.LogicalID == ref.LogicalID &&
		header.Generation == ref.Generation &&
		header.PageSize == ref.Length &&
		header.Kind == ref.Kind &&
		header.Flags == ref.Flags
}

func recoveryFreeHeaderMatchesRef(
	header FreeLogHeader,
	storeID [16]byte,
	ref PageRef,
) bool {
	return header.StoreID == storeID &&
		header.LogicalID == ref.LogicalID &&
		header.Generation == ref.Generation
}
