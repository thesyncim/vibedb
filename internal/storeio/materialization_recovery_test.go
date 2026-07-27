package storeio

import (
	"bytes"
	"errors"
	"os"
	"testing"
)

type mutableRecoveryFixture struct {
	file       *os.File
	layout     MutableStoreFileLayout
	pageSize   uint32
	storeID    [16]byte
	before     [][]byte
	after      [][]byte
	refs       []PageRef
	journal    []byte
	target     uint64
	syncCalled int
}

func newMutableRecoveryFixture(
	t *testing.T,
	targetGeneration, sequence uint64,
	lengths ...uint32,
) *mutableRecoveryFixture {
	t.Helper()
	const pageSize = uint32(4096)
	layout, err := MutableStoreLayout(pageSize)
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.CreateTemp(t.TempDir(), "mutable-recovery-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })
	fixture := &mutableRecoveryFixture{
		file: file, layout: layout, pageSize: pageSize,
		storeID: [16]byte{0xa1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		target:  targetGeneration,
	}
	if len(lengths) == 0 {
		lengths = []uint32{pageSize}
	}
	header := MaterializationJournalHeader{
		StoreID: fixture.storeID, Sequence: sequence,
		TargetGeneration: targetGeneration,
		PageSize:         pageSize, SectorSize: MaterializationJournalMinSectorSize,
	}
	var targets []MaterializationTarget
	var patches []MaterializationPatch
	offset := layout.DataStart
	for rank, length := range lengths {
		ref := PageRef{
			Offset: offset, LogicalID: uint64(rank + 2), Generation: 1,
			Length: length, Kind: PageDocument,
		}
		before := makeMutableRecoveryPage(t, fixture.storeID, ref, byte(0x20+rank))
		after := append([]byte(nil), before...)
		after[PageHeaderSize+17] ^= byte(0x71 + rank)
		if _, err := SealPage(after); err != nil {
			t.Fatal(err)
		}
		firstSector := uint32(0)
		trailerSector := length - header.SectorSize
		patches = append(patches,
			MaterializationPatch{
				Target: uint16(rank), Offset: firstSector,
				Data: before[:header.SectorSize],
			},
			MaterializationPatch{
				Target: uint16(rank), Offset: trailerSector,
				Data: before[trailerSector:length],
			},
		)
		fixture.refs = append(fixture.refs, ref)
		fixture.before = append(fixture.before, before)
		fixture.after = append(fixture.after, after)
		offset += uint64(length)
	}
	for rank, ref := range fixture.refs {
		target, err := BuildMaterializationTarget(
			header, ref, fixture.before[rank], fixture.after[rank],
			patches, uint16(rank),
		)
		if err != nil {
			t.Fatal(err)
		}
		targets = append(targets, target)
	}
	fixture.journal = make([]byte, MaterializationJournalSize)
	if _, err := EncodeMaterializationJournal(
		fixture.journal, header, targets, patches,
	); err != nil {
		t.Fatal(err)
	}
	if err := file.Truncate(int64(offset)); err != nil {
		t.Fatal(err)
	}
	for rank := range fixture.refs {
		if _, err := file.WriteAt(
			fixture.after[rank], int64(fixture.refs[rank].Offset),
		); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := file.WriteAt(
		fixture.journal, int64(layout.MaterializationJournalOffsets[0]),
	); err != nil {
		t.Fatal(err)
	}
	return fixture
}

func makeMutableRecoveryPage(
	t *testing.T,
	storeID [16]byte,
	ref PageRef,
	fill byte,
) []byte {
	t.Helper()
	page := make([]byte, ref.Length)
	payload, err := InitPage(page, PageHeader{
		StoreID: storeID, Generation: ref.Generation, LogicalID: ref.LogicalID,
		PageSize: ref.Length, PayloadLength: 128, Kind: ref.Kind,
	})
	if err != nil {
		t.Fatal(err)
	}
	for index := range payload {
		payload[index] = fill + byte(index%23)
	}
	if _, err := SealPage(page); err != nil {
		t.Fatal(err)
	}
	return page
}

func (f *mutableRecoveryFixture) root(generation uint64) InlineSuperblock {
	fileEnd := f.layout.DataStart
	for _, ref := range f.refs {
		fileEnd = max(fileEnd, ref.Offset+uint64(ref.Length))
	}
	state := StateRoot{
		StoreID: f.storeID, Generation: generation, PageSize: f.pageSize,
		MaxPageSize:   f.pageSize,
		NextLogicalID: uint64(len(f.refs) + 2), ChunkDocuments: 64,
		Options:                      StateOptionCanonicalMaterialization,
		MaterializationDamageGranule: MaterializationJournalMinSectorSize,
	}
	return InlineSuperblock{
		StoreID: f.storeID, Generation: generation, FileEnd: fileEnd,
		PageSize: f.pageSize, State: state,
	}
}

func (f *mutableRecoveryFixture) writeRoot(
	t *testing.T,
	slot int,
	root InlineSuperblock,
	corrupt bool,
) {
	t.Helper()
	page := make([]byte, f.pageSize)
	if _, err := EncodeInlineSuperblock(page, root); err != nil {
		t.Fatal(err)
	}
	if corrupt {
		page[0] ^= 0x80
	}
	if _, err := f.file.WriteAt(
		page, int64(f.layout.RootOffsets[slot]),
	); err != nil {
		t.Fatal(err)
	}
}

func (f *mutableRecoveryFixture) recover(
	scratch []byte,
) (MutableInlineRecovery, error) {
	return recoverMutableInlineStateRoot(
		f.file, f.pageSize, MaterializationJournalMinSectorSize, scratch,
		func() error {
			f.syncCalled++
			return f.file.Sync()
		},
	)
}

func TestMutableRecoveryCommittedRootNeverRollsBackForFallback(t *testing.T) {
	fixture := newMutableRecoveryFixture(t, 2, 17)
	fixture.writeRoot(t, 0, fixture.root(1), false)
	fixture.writeRoot(t, 1, fixture.root(2), false)

	result, err := fixture.recover(make([]byte, fixture.pageSize))
	if err != nil {
		t.Fatal(err)
	}
	if result.Root.Generation != 2 || result.RootSlot != 1 ||
		result.FallbackGeneration != 2 ||
		result.JournalSlot != 0 || result.JournalSequence != 17 {
		t.Fatalf("recovery result = %+v", result)
	}
	if fixture.syncCalled != 0 {
		t.Fatalf("committed root synchronized rollback %d times", fixture.syncCalled)
	}
	page := make([]byte, fixture.refs[0].Length)
	if _, err := fixture.file.ReadAt(page, int64(fixture.refs[0].Offset)); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(page, fixture.after[0]) {
		t.Fatal("valid committed root was destroyed to validate its fallback")
	}
}

func TestMutableRecoveryRejectsExactRootUntilEveryTargetIsAfterImage(t *testing.T) {
	fixture := newMutableRecoveryFixture(t, 2, 171, 4096, 4096)
	fixture.writeRoot(t, 0, fixture.root(1), false)
	fixture.writeRoot(t, 1, fixture.root(2), false)

	torn := append([]byte(nil), fixture.after[1]...)
	view, err := OpenMaterializationJournal(fixture.journal)
	if err != nil {
		t.Fatal(err)
	}
	first, _, _ := view.TargetPatchRange(1)
	patch, _ := view.PatchAt(first)
	half := len(patch.Data) / 2
	copy(
		torn[patch.Offset:patch.Offset+uint32(half)],
		fixture.before[1][patch.Offset:patch.Offset+uint32(half)],
	)
	if _, err := fixture.file.WriteAt(
		torn, int64(fixture.refs[1].Offset),
	); err != nil {
		t.Fatal(err)
	}

	result, err := fixture.recover(make([]byte, fixture.pageSize))
	if err != nil {
		t.Fatal(err)
	}
	if result.Root.Generation != 1 || result.RootSlot != 0 ||
		result.FallbackGeneration != 1 || result.JournalSlot != -1 ||
		result.JournalSequence != 0 || fixture.syncCalled != 3 {
		t.Fatalf("recovery=(%+v sync=%d)", result, fixture.syncCalled)
	}
	for rank, ref := range fixture.refs {
		page := make([]byte, ref.Length)
		if _, err := fixture.file.ReadAt(page, int64(ref.Offset)); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(page, fixture.before[rank]) {
			t.Fatalf("target %d was not rolled back atomically", rank)
		}
	}

	fixture.syncCalled = 0
	result, err = fixture.recover(make([]byte, fixture.pageSize))
	if err != nil || result.Root.Generation != 1 ||
		result.RootSlot != 0 || fixture.syncCalled != 0 {
		t.Fatalf(
			"post-cancellation reopen=(%+v sync=%d err=%v)",
			result, fixture.syncCalled, err,
		)
	}

	// Reusing the skipped generation must not conflict with or be rejected by
	// the failed capsule that recovery canceled.
	for rank, ref := range fixture.refs {
		if _, err := fixture.file.WriteAt(
			fixture.after[rank], int64(ref.Offset),
		); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := fixture.file.WriteAt(
		fixture.journal,
		int64(fixture.layout.MaterializationJournalOffsets[1]),
	); err != nil {
		t.Fatal(err)
	}
	fixture.writeRoot(t, 1, fixture.root(2), false)
	fixture.syncCalled = 0
	result, err = fixture.recover(make([]byte, fixture.pageSize))
	if err != nil || result.Root.Generation != 2 ||
		result.RootSlot != 1 || result.JournalSlot != 1 ||
		result.JournalSequence != 171 || fixture.syncCalled != 0 {
		t.Fatalf(
			"generation-reuse reopen=(%+v sync=%d err=%v)",
			result, fixture.syncCalled, err,
		)
	}
}

func TestMutableRecoveryNewerRootDoesNotAdvertiseDivergedExactFallback(
	t *testing.T,
) {
	fixture := newMutableRecoveryFixture(t, 2, 172)
	fixture.writeRoot(t, 0, fixture.root(2), false)
	fixture.writeRoot(t, 1, fixture.root(3), false)
	reusedRef := fixture.refs[0]
	reusedRef.LogicalID++
	diverged := makeMutableRecoveryPage(
		t, fixture.storeID, reusedRef, 0x71,
	)
	if _, err := fixture.file.WriteAt(
		diverged, int64(fixture.refs[0].Offset),
	); err != nil {
		t.Fatal(err)
	}

	result, err := fixture.recover(make([]byte, fixture.pageSize))
	if err != nil {
		t.Fatal(err)
	}
	if result.Root.Generation != 3 || result.RootSlot != 1 ||
		result.FallbackGeneration != 3 || fixture.syncCalled != 0 {
		t.Fatalf("recovery=(%+v sync=%d)", result, fixture.syncCalled)
	}
}

func TestMutableRecoveryCancellationFenceFailuresRemainRecoverable(
	t *testing.T,
) {
	for failFence := 1; failFence <= 3; failFence++ {
		t.Run(
			[...]string{"", "rollback", "root-invalidation", "journal-clear"}[failFence],
			func(t *testing.T) {
				fixture := newMutableRecoveryFixture(
					t, 2, uint64(180+failFence),
				)
				fixture.writeRoot(t, 0, fixture.root(1), false)
				fixture.writeRoot(t, 1, fixture.root(2), false)
				if _, err := fixture.file.WriteAt(
					fixture.before[0], int64(fixture.refs[0].Offset),
				); err != nil {
					t.Fatal(err)
				}
				sentinel := errors.New("injected cancellation fence failure")
				calls := 0
				_, err := recoverMutableInlineStateRoot(
					fixture.file, fixture.pageSize,
					MaterializationJournalMinSectorSize,
					make([]byte, fixture.pageSize),
					func() error {
						calls++
						if calls == failFence {
							return sentinel
						}
						return fixture.file.Sync()
					},
				)
				if !errors.Is(err, sentinel) || calls != failFence {
					t.Fatalf(
						"fence %d recovery=(calls=%d err=%v)",
						failFence, calls, err,
					)
				}

				fixture.syncCalled = 0
				result, err := fixture.recover(
					make([]byte, fixture.pageSize),
				)
				if err != nil || result.Root.Generation != 1 ||
					result.RootSlot != 0 {
					t.Fatalf(
						"fence %d reopen=(%+v sync=%d err=%v)",
						failFence, result, fixture.syncCalled, err,
					)
				}
			},
		)
	}
}

func TestMutableRecoveryNewerRootSkipsStaleTargetWithoutExactAlternate(
	t *testing.T,
) {
	fixture := newMutableRecoveryFixture(t, 2, 173)
	fixture.writeRoot(t, 0, fixture.root(2), true)
	fixture.writeRoot(t, 1, fixture.root(3), false)
	reusedRef := fixture.refs[0]
	reusedRef.LogicalID++
	diverged := makeMutableRecoveryPage(
		t, fixture.storeID, reusedRef, 0x72,
	)
	if _, err := fixture.file.WriteAt(
		diverged, int64(fixture.refs[0].Offset),
	); err != nil {
		t.Fatal(err)
	}

	result, err := fixture.recover(make([]byte, fixture.pageSize))
	if err != nil {
		t.Fatal(err)
	}
	if result.Root.Generation != 3 || result.FallbackGeneration != 3 ||
		fixture.syncCalled != 0 {
		t.Fatalf("recovery=(%+v sync=%d)", result, fixture.syncCalled)
	}
}

func TestMutableRecoveryTornRootRollsBackBeforeFallbackValidation(t *testing.T) {
	fixture := newMutableRecoveryFixture(t, 2, 18)
	fixture.writeRoot(t, 0, fixture.root(1), false)
	fixture.writeRoot(t, 1, fixture.root(2), true)

	result, err := fixture.recover(make([]byte, fixture.pageSize))
	if err != nil {
		t.Fatal(err)
	}
	if result.Root.Generation != 1 || result.RootSlot != 0 ||
		result.FallbackGeneration != 1 || result.JournalSlot != -1 ||
		result.JournalSequence != 0 || fixture.syncCalled != 2 {
		t.Fatalf("recovery=(%+v sync=%d)", result, fixture.syncCalled)
	}
	page := make([]byte, fixture.refs[0].Length)
	if _, err := fixture.file.ReadAt(page, int64(fixture.refs[0].Offset)); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(page, fixture.before[0]) {
		t.Fatal("fallback root was exposed before its target was rolled back")
	}

	fixture.syncCalled = 0
	result, err = fixture.recover(make([]byte, fixture.pageSize))
	if err != nil || result.Root.Generation != 1 || fixture.syncCalled != 0 {
		t.Fatalf("idempotent recovery=(%+v sync=%d err=%v)",
			result, fixture.syncCalled, err)
	}
}

func TestMutableRecoveryRollsBackTopLevelReferenceBeforeValidation(t *testing.T) {
	const pageSize = uint32(4096)
	layout, err := MutableStoreLayout(pageSize)
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.CreateTemp(t.TempDir(), "mutable-top-level-recovery-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	storeID := [16]byte{0xb1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	ref := PageRef{
		Offset: layout.DataStart, LogicalID: 2, Generation: 1,
		Length: pageSize, Kind: PageIndexDirectory,
	}
	before := make([]byte, pageSize)
	after := make([]byte, pageSize)
	header := IndexDirectoryHeader{
		StoreID: storeID, Generation: 1, LogicalID: ref.LogicalID,
		PageSize: pageSize,
	}
	entry := IndexDirectoryEntry{
		Key: IndexDirectoryKey{TupleHash: 7}, Bits: 1,
		Kind: IndexEntryInlineMask,
	}
	if _, err := EncodeIndexDirectoryLeaf(
		before, header, []IndexDirectoryEntry{entry}, nil, 3, 1,
	); err != nil {
		t.Fatal(err)
	}
	entry.Bits = 2
	if _, err := EncodeIndexDirectoryLeaf(
		after, header, []IndexDirectoryEntry{entry}, nil, 3, 1,
	); err != nil {
		t.Fatal(err)
	}
	journalHeader := MaterializationJournalHeader{
		StoreID: storeID, Sequence: 23, TargetGeneration: 2,
		PageSize: pageSize, SectorSize: MaterializationJournalMinSectorSize,
	}
	patches := []MaterializationPatch{
		{Offset: 0, Data: before[:MaterializationJournalMinSectorSize]},
		{
			Offset: pageSize - MaterializationJournalMinSectorSize,
			Data:   before[pageSize-MaterializationJournalMinSectorSize:],
		},
	}
	target, err := BuildMaterializationTarget(
		journalHeader, ref, before, after, patches, 0,
	)
	if err != nil {
		t.Fatal(err)
	}
	journal := make([]byte, MaterializationJournalSize)
	if _, err := EncodeMaterializationJournal(
		journal, journalHeader, []MaterializationTarget{target}, patches,
	); err != nil {
		t.Fatal(err)
	}
	catalog, err := BuildCanonicalPageCatalog(PageCatalogDefinition{
		Indexes: []PageCatalogIndex{{
			Name: "index", Paths: []string{"/value"},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	catalogRef := PageRef{
		Offset:    layout.DataStart + uint64(pageSize),
		LogicalID: 3, Generation: 1,
		Length: pageSize, Kind: PageCatalogSegment,
	}
	fileEnd := layout.DataStart + 2*uint64(pageSize)
	catalogPage := make([]byte, pageSize)
	if _, err := EncodePageCatalogSegment(
		catalogPage,
		PageCatalogSegmentHeader{
			StoreID: storeID, Generation: 1,
			LogicalID: catalogRef.LogicalID,
		},
		catalog,
		PageCatalogBounds{
			StoreID: storeID, Generation: 2,
			PageSize: pageSize, DataStart: layout.DataStart,
			FileEnd: fileEnd, NextLogicalID: 4,
			ExpectedDigest: catalog.Digest(),
		},
	); err != nil {
		t.Fatal(err)
	}
	if err := file.Truncate(int64(fileEnd)); err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt(after, int64(ref.Offset)); err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt(catalogPage, int64(catalogRef.Offset)); err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt(
		journal, int64(layout.MaterializationJournalOffsets[0]),
	); err != nil {
		t.Fatal(err)
	}
	makeRoot := func(generation uint64) InlineSuperblock {
		state := StateRoot{
			StoreID: storeID, Generation: generation, PageSize: pageSize,
			MaxPageSize: pageSize, NextLogicalID: 4, ChunkDocuments: 64,
			IndexCount: 1, IndexCatalogHash: 1, IndexDirectory: ref,
			PageCatalogHead:              catalogRef,
			PageCatalogDigest:            catalog.Digest(),
			PageCatalogBytes:             uint32(catalog.CanonicalSize()),
			Options:                      StateOptionCanonicalMaterialization,
			MaterializationDamageGranule: MaterializationJournalMinSectorSize,
		}
		return InlineSuperblock{
			StoreID: storeID, Generation: generation,
			FileEnd:  fileEnd,
			PageSize: pageSize, State: state,
		}
	}
	for slot, root := range []InlineSuperblock{makeRoot(1), makeRoot(2)} {
		encoded := make([]byte, pageSize)
		if _, err := EncodeInlineSuperblock(encoded, root); err != nil {
			t.Fatal(err)
		}
		if slot == 1 {
			encoded[0] ^= 1
		}
		if _, err := file.WriteAt(encoded, int64(layout.RootOffsets[slot])); err != nil {
			t.Fatal(err)
		}
	}

	result, err := RecoverMutableInlineStateRoot(
		file, pageSize, MaterializationJournalMinSectorSize,
		make([]byte, pageSize),
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.Root.Generation != 1 {
		t.Fatalf("recovered generation = %d, want 1", result.Root.Generation)
	}
	restored := make([]byte, pageSize)
	if _, err := file.ReadAt(restored, int64(ref.Offset)); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(restored, before) {
		t.Fatal("top-level page was validated before rollback")
	}
}

func TestMutableRecoveryBothRootsPredateJournal(t *testing.T) {
	fixture := newMutableRecoveryFixture(t, 3, 19)
	fixture.writeRoot(t, 0, fixture.root(1), false)
	fixture.writeRoot(t, 1, fixture.root(2), false)

	result, err := fixture.recover(make([]byte, fixture.pageSize))
	if err != nil {
		t.Fatal(err)
	}
	if result.Root.Generation != 2 || result.FallbackGeneration != 1 ||
		result.JournalSlot != -1 || result.JournalSequence != 0 ||
		fixture.syncCalled != 2 {
		t.Fatalf("recovery=(%+v sync=%d)", result, fixture.syncCalled)
	}
}

func TestMutableRecoveryCancellationPreservesPriorMaterializedFence(
	t *testing.T,
) {
	fixture := newMutableRecoveryFixture(t, 5, 51)
	fixture.writeRoot(t, 0, fixture.root(3), false)
	fixture.writeRoot(t, 1, fixture.root(4), false)

	priorBefore := makeMutableRecoveryPage(
		t, fixture.storeID, fixture.refs[0], 0x11,
	)
	priorAfter := fixture.before[0]
	priorPatches := []MaterializationPatch{
		{
			Target: 0, Offset: 0,
			Data: priorBefore[:MaterializationJournalMinSectorSize],
		},
		{
			Target: 0,
			Offset: fixture.refs[0].Length -
				MaterializationJournalMinSectorSize,
			Data: priorBefore[len(priorBefore)-
				MaterializationJournalMinSectorSize:],
		},
	}
	priorHeader := MaterializationJournalHeader{
		StoreID: fixture.storeID, Sequence: 50, TargetGeneration: 4,
		PageSize:   fixture.pageSize,
		SectorSize: MaterializationJournalMinSectorSize,
	}
	priorTarget, err := BuildMaterializationTarget(
		priorHeader, fixture.refs[0], priorBefore, priorAfter,
		priorPatches, 0,
	)
	if err != nil {
		t.Fatal(err)
	}
	priorJournal := make([]byte, MaterializationJournalSize)
	if _, err := EncodeMaterializationJournal(
		priorJournal, priorHeader,
		[]MaterializationTarget{priorTarget}, priorPatches,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.file.WriteAt(
		priorJournal,
		int64(fixture.layout.MaterializationJournalOffsets[1]),
	); err != nil {
		t.Fatal(err)
	}

	result, err := fixture.recover(make([]byte, fixture.pageSize))
	if err != nil {
		t.Fatal(err)
	}
	if result.Root.Generation != 4 || result.RootSlot != 1 ||
		result.FallbackGeneration != 4 ||
		result.JournalSlot != 1 || result.JournalSequence != 50 ||
		fixture.syncCalled != 2 {
		t.Fatalf("recovery=(%+v sync=%d)", result, fixture.syncCalled)
	}

	fixture.syncCalled = 0
	result, err = fixture.recover(make([]byte, fixture.pageSize))
	if err != nil || result.Root.Generation != 4 ||
		result.FallbackGeneration != 4 || fixture.syncCalled != 0 {
		t.Fatalf(
			"prior-capsule reopen=(%+v sync=%d err=%v)",
			result, fixture.syncCalled, err,
		)
	}
}

func TestMutableRecoveryPreflightsEveryTargetBeforeWriting(t *testing.T) {
	fixture := newMutableRecoveryFixture(t, 2, 20, 4096, 4096)
	fixture.writeRoot(t, 0, fixture.root(1), false)
	fixture.writeRoot(t, 1, fixture.root(2), true)
	corrupt := append([]byte(nil), fixture.after[1]...)
	corrupt[MaterializationJournalMinSectorSize+37] ^= 0x40
	if _, err := fixture.file.WriteAt(corrupt, int64(fixture.refs[1].Offset)); err != nil {
		t.Fatal(err)
	}

	_, err := fixture.recover(make([]byte, fixture.pageSize))
	if !errors.Is(err, ErrMaterializationTargetDiverged) {
		t.Fatalf("recovery error = %v, want %v", err, ErrMaterializationTargetDiverged)
	}
	first := make([]byte, fixture.refs[0].Length)
	if _, err := fixture.file.ReadAt(first, int64(fixture.refs[0].Offset)); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, fixture.after[0]) {
		t.Fatal("target zero was repaired before target one failed preflight")
	}
	if fixture.syncCalled != 0 {
		t.Fatalf("preflight failure synchronized %d times", fixture.syncCalled)
	}
}

func TestMutableRecoveryRejectsShortScratchBeforeMutation(t *testing.T) {
	fixture := newMutableRecoveryFixture(t, 2, 21, 8192)
	fixture.writeRoot(t, 0, fixture.root(1), false)
	fixture.writeRoot(t, 1, fixture.root(2), true)

	_, err := fixture.recover(make([]byte, fixture.pageSize))
	if !errors.Is(err, ErrRecoveryBufferTooSmall) {
		t.Fatalf("recovery error = %v, want %v", err, ErrRecoveryBufferTooSmall)
	}
	page := make([]byte, fixture.refs[0].Length)
	if _, err := fixture.file.ReadAt(page, int64(fixture.refs[0].Offset)); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(page, fixture.after[0]) || fixture.syncCalled != 0 {
		t.Fatal("short scratch mutated recovery state")
	}
}

func TestMutableRecoveryTypedValidatesOptionalFloat64Root(t *testing.T) {
	fixture := newMutableRecoveryFixture(t, 3, 22)
	clear(fixture.journal)
	if _, err := fixture.file.WriteAt(
		fixture.journal, int64(fixture.layout.MaterializationJournalOffsets[0]),
	); err != nil {
		t.Fatal(err)
	}
	old := fixture.root(1)
	catalog, err := BuildCanonicalPageCatalog(PageCatalogDefinition{
		Float64Paths: []string{"/value"},
	})
	if err != nil {
		t.Fatal(err)
	}
	catalogRef := PageRef{
		Offset:    fixture.layout.DataStart + uint64(fixture.pageSize),
		LogicalID: 3, Generation: 1,
		Length: fixture.pageSize, Kind: PageCatalogSegment,
	}
	fileEnd := fixture.layout.DataStart + 2*uint64(fixture.pageSize)
	catalogPage := make([]byte, fixture.pageSize)
	if _, err := EncodePageCatalogSegment(
		catalogPage,
		PageCatalogSegmentHeader{
			StoreID: fixture.storeID, Generation: 1,
			LogicalID: catalogRef.LogicalID,
		},
		catalog,
		PageCatalogBounds{
			StoreID: fixture.storeID, Generation: 2,
			PageSize:  fixture.pageSize,
			DataStart: fixture.layout.DataStart,
			FileEnd:   fileEnd, NextLogicalID: 4,
			ExpectedDigest: catalog.Digest(),
		},
	); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.file.WriteAt(
		catalogPage, int64(catalogRef.Offset),
	); err != nil {
		t.Fatal(err)
	}
	old.FileEnd = fileEnd
	old.State.Options |= StateOptionFloat64Columns
	old.State.IndexCatalogHash = 1
	old.State.NextLogicalID = 4
	old.State.PageCatalogHead = catalogRef
	old.State.PageCatalogDigest = catalog.Digest()
	old.State.PageCatalogBytes = uint32(catalog.CanonicalSize())
	newest := fixture.root(2)
	newest.FileEnd = old.FileEnd
	newest.State.Options = old.State.Options
	newest.State.IndexCatalogHash = old.State.IndexCatalogHash
	newest.State.NextLogicalID = old.State.NextLogicalID
	newest.State.PageCatalogHead = old.State.PageCatalogHead
	newest.State.PageCatalogDigest = old.State.PageCatalogDigest
	newest.State.PageCatalogBytes = old.State.PageCatalogBytes
	newest.State.Float64ScanHead = PageRef{
		Offset: fixture.layout.DataStart, LogicalID: 2, Generation: 2,
		Length: fixture.pageSize, Kind: PageFloat64Catalog,
	}
	fixture.writeRoot(t, 0, old, false)
	fixture.writeRoot(t, 1, newest, false)
	malformed := make([]byte, fixture.pageSize)
	if _, err := InitPage(malformed, PageHeader{
		StoreID: fixture.storeID, Generation: 2, LogicalID: 2,
		PageSize: fixture.pageSize, Kind: PageFloat64Catalog,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := SealPage(malformed); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.file.WriteAt(
		malformed, int64(fixture.layout.DataStart),
	); err != nil {
		t.Fatal(err)
	}

	result, err := fixture.recover(make([]byte, fixture.pageSize))
	if err != nil {
		t.Fatal(err)
	}
	if result.Root.Generation != 1 || result.RootSlot != 0 {
		t.Fatalf("typed malformed optional root selected: %+v", result)
	}
}
