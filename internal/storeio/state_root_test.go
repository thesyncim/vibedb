package storeio

import (
	"encoding/binary"
	"errors"
	"os"
	"testing"
)

func testStatePageRef(kind PageKind, page, logical, generation uint64) PageRef {
	return PageRef{
		Offset:     page * uint64(testSuperblockPageSize),
		LogicalID:  logical,
		Generation: generation,
		Length:     testSuperblockPageSize,
		Kind:       kind,
	}
}

// testStateRoot returns a valid populated ordered-primary state root: an empty
// chunk census (the chunk layout is gone), a published PrimaryRoot as the sole
// document root, and the immutable admission bounds. fileEnd covers the primary
// catalog extent exactly.
func testStateRoot(generation uint64) (StateRoot, uint64) {
	layout, err := MutableStoreLayout(testSuperblockPageSize)
	if err != nil {
		panic(err)
	}
	root := StateRoot{
		StoreID:          testStoreID,
		Generation:       generation,
		PageSize:         testSuperblockPageSize,
		MaxPageSize:      64 << 10,
		Options:          StateOptionShapeTapes | StateOptionHashKeys,
		DocumentCount:    129,
		NextLogicalID:    PrimaryFirstDynamicLogicalID,
		ChunkDocuments:   64,
		MaxKeyBytes:      256,
		InlineValueBytes: 512,
		MaxDocumentBytes: 4 << 20,
		PrimaryRoot: PageRef{
			Offset:     layout.DataStart,
			LogicalID:  PrimaryCatalogRootLogicalID,
			Generation: generation,
			Length:     GlobalTabletCatalogRootBytes,
			Kind:       PagePrimaryCatalog,
		},
	}
	fileEnd := layout.DataStart + uint64(GlobalTabletCatalogRootBytes)
	return root, fileEnd
}

func TestStateRootPageRoundTrip(t *testing.T) {
	want, fileEnd := testStateRoot(11)
	page := make([]byte, testSuperblockPageSize)
	encoded, err := EncodeStateRootPage(page, want, fileEnd)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) != int(testSuperblockPageSize) {
		t.Fatalf("encoded length = %d", len(encoded))
	}
	got, err := DecodeStateRootPage(encoded, fileEnd)
	if err != nil || got != want {
		t.Fatalf("DecodeStateRootPage = (%+v,%v), want (%+v,nil)", got, err, want)
	}
	for cut := range encoded {
		if _, err := DecodeStateRootPage(encoded[:cut], fileEnd); !errors.Is(err, ErrStateRootCorrupt) {
			t.Fatalf("cut %d = %v, want %v", cut, err, ErrStateRootCorrupt)
		}
	}
}

func TestStateRootPrimaryRootRoundTrip(t *testing.T) {
	want, fileEnd := testStateRoot(11)
	page := make([]byte, testSuperblockPageSize)
	encoded, err := EncodeStateRootPage(page, want, fileEnd)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeStateRootPage(encoded, fileEnd)
	if err != nil || got != want {
		t.Fatalf("primary state root = (%+v,%v), want (%+v,nil)", got, err, want)
	}

	for name, mutate := range map[string]func(*StateRoot){
		"kind": func(root *StateRoot) {
			root.PrimaryRoot.Kind = PageTabletRoute
		},
		"logical": func(root *StateRoot) {
			root.PrimaryRoot.LogicalID = PrimaryCatalogRootLogicalID - 1
		},
		"length": func(root *StateRoot) {
			root.PrimaryRoot.Length = GlobalTabletCatalogNodeBytes
		},
		"namespace": func(root *StateRoot) {
			root.NextLogicalID = PrimaryFirstDynamicLogicalID - 1
		},
	} {
		t.Run(name, func(t *testing.T) {
			invalid := want
			mutate(&invalid)
			if _, err := EncodeStateRootPage(
				page, invalid, fileEnd,
			); !errors.Is(err, ErrInvalidWrite) {
				t.Fatalf("EncodeStateRootPage = %v, want %v", err, ErrInvalidWrite)
			}
		})
	}
}

// TestStateRootExactIndexRootRoundTrip covers the ordered-primary exact-index
// root, published as posting tiles beside the ordered graph, together with the
// canonical catalog its index count requires.
func TestStateRootExactIndexRootRoundTrip(t *testing.T) {
	want, _ := testStateRoot(11)
	layout, err := MutableStoreLayout(testSuperblockPageSize)
	if err != nil {
		t.Fatal(err)
	}
	afterPrimary := layout.DataStart + uint64(GlobalTabletCatalogRootBytes)
	want.IndexCount = 2
	want.IndexMaxDepth = 1024
	want.IndexCatalogHash = 0x123456789abcdef0
	want.PageCatalogHead = PageRef{
		Offset: afterPrimary, LogicalID: PrimaryCatalogRootLogicalID + 1,
		Generation: want.Generation, Length: testSuperblockPageSize,
		Kind: PageCatalogSegment,
	}
	want.PageCatalogBytes = PageCatalogCanonicalHeaderSize
	want.ExactIndexRoot = PageRef{
		Offset: afterPrimary + uint64(testSuperblockPageSize),
		LogicalID: PrimaryCatalogRootLogicalID + 2, Generation: want.Generation,
		Length: testSuperblockPageSize, Kind: PagePrimaryExactRoot,
	}
	fileEnd := afterPrimary + 2*uint64(testSuperblockPageSize)
	page := make([]byte, testSuperblockPageSize)
	encoded, err := EncodeStateRootPage(page, want, fileEnd)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeStateRootPage(encoded, fileEnd)
	if err != nil || got != want {
		t.Fatalf("exact-index state root = (%+v,%v), want (%+v,nil)", got, err, want)
	}

	// The exact-index root may not appear without a declared index count.
	invalid := want
	invalid.IndexCount = 0
	invalid.PageCatalogHead = PageRef{}
	invalid.PageCatalogBytes = 0
	invalid.IndexCatalogHash = 0
	if _, err := EncodeStateRootPage(page, invalid, fileEnd); !errors.Is(err, ErrInvalidWrite) {
		t.Fatalf("exact root without index = %v, want %v", err, ErrInvalidWrite)
	}
}

func TestStateRootSchemaOnlyCatalogRoundTrip(t *testing.T) {
	want := StateRoot{
		StoreID:          testStoreID,
		Generation:       1,
		PageSize:         testSuperblockPageSize,
		MaxPageSize:      64 << 10,
		Options:          StateOptionSchema,
		NextLogicalID:    3,
		ChunkDocuments:   64,
		IndexCatalogHash: 0x6d4b3a291807f5e3,
		PageCatalogHead: testStatePageRef(
			PageCatalogSegment,
			uint64(testMutableStoreDataStart(testSuperblockPageSize))/
				uint64(testSuperblockPageSize),
			2, 1,
		),
		PageCatalogBytes: PageCatalogCanonicalHeaderSize,
	}
	fileEnd := testMutableStoreDataStart(testSuperblockPageSize) +
		uint64(testSuperblockPageSize)
	page := make([]byte, testSuperblockPageSize)
	encoded, err := EncodeStateRootPage(page, want, fileEnd)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeStateRootPage(encoded, fileEnd)
	if err != nil || got != want {
		t.Fatalf(
			"schema state root = (%+v,%v), want (%+v,nil)",
			got, err, want,
		)
	}

	invalid := want
	invalid.IndexCatalogHash = 0
	if _, err := EncodeStateRootPage(
		page, invalid, fileEnd,
	); !errors.Is(err, ErrInvalidWrite) {
		t.Fatalf("schema without identity = %v, want %v", err, ErrInvalidWrite)
	}
}

func TestStateRootCanonicalMaterializationRoundTrip(t *testing.T) {
	want, fileEnd := testStateRoot(11)
	want.Options |= StateOptionCanonicalMaterialization
	want.MaterializationDamageGranule =
		MaterializationJournalMinSectorSize
	page := make([]byte, testSuperblockPageSize)
	encoded, err := EncodeStateRootPage(page, want, fileEnd)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeStateRootPage(encoded, fileEnd)
	if err != nil || got != want {
		t.Fatalf(
			"materialized state root = (%+v,%v), want (%+v,nil)",
			got, err, want,
		)
	}

	for _, invalid := range []StateRoot{
		func() StateRoot {
			root := want
			root.Options &^= StateOptionCanonicalMaterialization
			return root
		}(),
		func() StateRoot {
			root := want
			root.MaterializationDamageGranule = 0
			return root
		}(),
		func() StateRoot {
			root := want
			root.MaterializationDamageGranule = 768
			return root
		}(),
	} {
		if _, err := EncodeStateRootPage(
			page, invalid, fileEnd,
		); !errors.Is(err, ErrInvalidWrite) {
			t.Fatalf(
				"invalid materialization geometry = %v, want %v",
				err, ErrInvalidWrite,
			)
		}
	}
}

func TestStateRootValidation(t *testing.T) {
	valid, fileEnd := testStateRoot(11)
	for _, test := range []struct {
		name   string
		mutate func(*StateRoot, *uint64)
	}{
		{"options", func(root *StateRoot, _ *uint64) { root.Options |= 1 << 31 }},
		{"zero maximum page", func(root *StateRoot, _ *uint64) { root.MaxPageSize = 0 }},
		{"small maximum page", func(root *StateRoot, _ *uint64) { root.MaxPageSize = 2048 }},
		{"non-power maximum page", func(root *StateRoot, _ *uint64) { root.MaxPageSize = 48 << 10 }},
		{"oversized maximum page", func(root *StateRoot, _ *uint64) {
			root.MaxPageSize = MaxPhysicalPageSize << 1
		}},
		{"partial key admission", func(root *StateRoot, _ *uint64) { root.MaxKeyBytes = 0 }},
		{"partial inline admission", func(root *StateRoot, _ *uint64) { root.InlineValueBytes = 0 }},
		{"partial document admission", func(root *StateRoot, _ *uint64) { root.MaxDocumentBytes = 0 }},
		{"inline above document", func(root *StateRoot, _ *uint64) {
			root.InlineValueBytes = root.MaxDocumentBytes + 1
		}},
		{"chunk documents", func(root *StateRoot, _ *uint64) { root.ChunkDocuments = 65 }},
		{"live high water", func(root *StateRoot, _ *uint64) { root.LiveChunks = root.ChunkHighWater + 1 }},
		{"free chunk hint", func(root *StateRoot, _ *uint64) { root.FreeChunkHint = root.ChunkHighWater + 1 }},
		{"next logical id", func(root *StateRoot, _ *uint64) { root.NextLogicalID = StateRootLogicalID }},
		{"missing primary namespace", func(root *StateRoot, _ *uint64) {
			root.NextLogicalID = PrimaryFirstDynamicLogicalID - 1
		}},
		{"wrong primary kind", func(root *StateRoot, _ *uint64) { root.PrimaryRoot.Kind = PageTabletRoute }},
		{"future primary generation", func(root *StateRoot, _ *uint64) { root.PrimaryRoot.Generation++ }},
		{"short primary ref", func(root *StateRoot, _ *uint64) { root.PrimaryRoot.Length-- }},
		{"unaligned primary ref", func(root *StateRoot, _ *uint64) { root.PrimaryRoot.Offset++ }},
		{"primary outside file", func(root *StateRoot, _ *uint64) { root.PrimaryRoot.Offset = fileEnd }},
		{"exact root without index", func(root *StateRoot, _ *uint64) {
			root.ExactIndexRoot = testStatePageRef(PagePrimaryExactRoot, 2, 3, root.Generation)
		}},
		{"unaligned file end", func(_ *StateRoot, end *uint64) { (*end)-- }},
	} {
		t.Run(test.name, func(t *testing.T) {
			root, end := valid, fileEnd
			test.mutate(&root, &end)
			page := make([]byte, testSuperblockPageSize)
			if _, err := EncodeStateRootPage(page, root, end); !errors.Is(err, ErrInvalidWrite) {
				t.Fatalf("EncodeStateRootPage = %v, want %v", err, ErrInvalidWrite)
			}
		})
	}

	empty := StateRoot{
		StoreID:        testStoreID,
		Generation:     1,
		PageSize:       testSuperblockPageSize,
		MaxPageSize:    64 << 10,
		NextLogicalID:  2,
		ChunkDocuments: 64,
	}
	page := make([]byte, testSuperblockPageSize)
	if _, err := EncodeStateRootPage(
		page, empty, testMutableStoreDataStart(testSuperblockPageSize),
	); err != nil {
		t.Fatalf("empty state root: %v", err)
	}
}

func TestStateRootRejectsResealedSemanticCorruption(t *testing.T) {
	root, fileEnd := testStateRoot(3)
	page := make([]byte, testSuperblockPageSize)
	if _, err := EncodeStateRootPage(page, root, fileEnd); err != nil {
		t.Fatal(err)
	}
	// The retired chunk/fingerprint slots and the trailing reserved region must
	// decode as entirely blank; any set bit is semantic corruption a valid
	// checksum cannot hide.
	for _, offset := range []int{
		PageHeaderSize + stateRootChunkRefOffset,
		PageHeaderSize + stateRootFloat64Offset,
		PageHeaderSize + stateRootIndexGroupOffset,
		PageHeaderSize + stateRootReservedOffset,
	} {
		corrupt := append([]byte(nil), page...)
		corrupt[offset] = 1
		resealTestPage(corrupt)
		if _, err := DecodeStateRootPage(corrupt, fileEnd); !errors.Is(err, ErrStateRootCorrupt) {
			t.Fatalf("offset %d = %v, want %v", offset, err, ErrStateRootCorrupt)
		}
	}

	// A resealed page whose version field no longer matches the format must fail
	// before any field is trusted.
	corrupt := append([]byte(nil), page...)
	binary.LittleEndian.PutUint32(corrupt[PageHeaderSize:PageHeaderSize+4], stateRootVersion+1)
	resealTestPage(corrupt)
	if _, err := DecodeStateRootPage(corrupt, fileEnd); !errors.Is(err, ErrStateRootCorrupt) {
		t.Fatalf("semantic corruption = %v, want %v", err, ErrStateRootCorrupt)
	}
}

func TestRecoverStateRootFallsBackOnSemanticMismatch(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "state-root-recovery")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	pageSize := uint64(testSuperblockPageSize)
	fileEnd := 6 * pageSize
	empty := func(generation uint64) StateRoot {
		return StateRoot{
			StoreID: testStoreID, Generation: generation, PageSize: testSuperblockPageSize,
			MaxPageSize: 64 << 10, NextLogicalID: 2, ChunkDocuments: 64,
		}
	}
	state1 := make([]byte, testSuperblockPageSize)
	state2 := make([]byte, testSuperblockPageSize)
	if _, err := EncodeStateRootPage(state1, empty(1), fileEnd); err != nil {
		t.Fatal(err)
	}
	if _, err := EncodeStateRootPage(state2, empty(2), fileEnd); err != nil {
		t.Fatal(err)
	}
	root1 := testSuperblock(1, 4*pageSize, state1)
	root1.FileEnd = fileEnd
	root2 := testSuperblock(2, 5*pageSize, state2)
	root2.FileEnd = fileEnd
	first := encodeTestSuperblock(t, root1)
	second := encodeTestSuperblock(t, root2)
	if err := file.Truncate(int64(fileEnd)); err != nil {
		t.Fatal(err)
	}
	writeAtTest(t, file, first[:], 0)
	writeAtTest(t, file, second[:], int64(pageSize))
	writeAtTest(t, file, state1, int64(root1.StateOffset))
	writeAtTest(t, file, state2, int64(root2.StateOffset))
	scratch := make([]byte, testSuperblockPageSize)

	gotSuper, gotState, slot, fallbackGeneration, err := RecoverStateRootWithFallback(
		file, testSuperblockPageSize, scratch,
	)
	if err != nil || gotSuper != root2 || gotState != empty(2) || slot != 1 ||
		fallbackGeneration != 1 {
		t.Fatalf(
			"recover newest = (%+v,%+v,%d,fallback=%d,%v)",
			gotSuper, gotState, slot, fallbackGeneration, err,
		)
	}

	// Keep both CRC layers valid while breaking the state/superblock generation
	// binding. Recovery must reject generation two and select generation one.
	binary.LittleEndian.PutUint64(state2[24:32], 7)
	resealTestPage(state2)
	root2.StateChecksum = PageChecksum(state2)
	second = encodeTestSuperblock(t, root2)
	writeAtTest(t, file, state2, int64(root2.StateOffset))
	writeAtTest(t, file, second[:], int64(pageSize))
	gotSuper, gotState, slot, fallbackGeneration, err = RecoverStateRootWithFallback(
		file, testSuperblockPageSize, scratch,
	)
	if err != nil || gotSuper != root1 || gotState != empty(1) || slot != 0 ||
		fallbackGeneration != 1 {
		t.Fatalf(
			"semantic fallback = (%+v,%+v,%d,fallback=%d,%v)",
			gotSuper, gotState, slot, fallbackGeneration, err,
		)
	}
}
