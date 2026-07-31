package storeio

import (
	"encoding/binary"
	"errors"
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

// testStateRoot returns a valid populated ordered-primary state root and its
// exact file extent.
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
		DocumentCount:    129,
		NextLogicalID:    PrimaryFirstDynamicLogicalID,
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

func encodeTestStateRootPayload(
	dst []byte, root StateRoot, fileEnd uint64,
) ([]byte, error) {
	if len(dst) < StateRootPayloadSize {
		return nil, ErrInvalidWrite
	}
	if err := validateStateRoot(root, fileEnd); err != nil {
		return nil, err
	}
	payload := dst[:StateRootPayloadSize]
	encodeStateRootPayload(payload, root)
	return payload, nil
}

func decodeTestStateRootPayload(
	payload []byte, identity StateRoot, fileEnd uint64,
) (StateRoot, error) {
	if len(payload) < StateRootPayloadSize {
		return StateRoot{}, ErrStateRootCorrupt
	}
	return decodeStateRootPayload(
		payload[:StateRootPayloadSize], identity.StoreID,
		identity.Generation, identity.PageSize, fileEnd,
	)
}

func TestStateRootPayloadRoundTrip(t *testing.T) {
	want, fileEnd := testStateRoot(11)
	page := make([]byte, testSuperblockPageSize)
	encoded, err := encodeTestStateRootPayload(page, want, fileEnd)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) != StateRootPayloadSize {
		t.Fatalf("encoded length = %d", len(encoded))
	}
	got, err := decodeTestStateRootPayload(encoded, want, fileEnd)
	if err != nil || got != want {
		t.Fatalf("decodeTestStateRootPayload = (%+v,%v), want (%+v,nil)", got, err, want)
	}
	for cut := range encoded {
		if _, err := decodeTestStateRootPayload(encoded[:cut], want, fileEnd); !errors.Is(err, ErrStateRootCorrupt) {
			t.Fatalf("cut %d = %v, want %v", cut, err, ErrStateRootCorrupt)
		}
	}
}

func TestStateRootPrimaryRootRoundTrip(t *testing.T) {
	want, fileEnd := testStateRoot(11)
	page := make([]byte, testSuperblockPageSize)
	encoded, err := encodeTestStateRootPayload(page, want, fileEnd)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeTestStateRootPayload(encoded, want, fileEnd)
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
			if _, err := encodeTestStateRootPayload(
				page, invalid, fileEnd,
			); !errors.Is(err, ErrInvalidWrite) {
				t.Fatalf("encodeTestStateRootPayload = %v, want %v", err, ErrInvalidWrite)
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
		Offset:    afterPrimary + uint64(testSuperblockPageSize),
		LogicalID: PrimaryCatalogRootLogicalID + 2, Generation: want.Generation,
		Length: testSuperblockPageSize, Kind: PagePrimaryExactRoot,
	}
	fileEnd := afterPrimary + 2*uint64(testSuperblockPageSize)
	page := make([]byte, testSuperblockPageSize)
	encoded, err := encodeTestStateRootPayload(page, want, fileEnd)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeTestStateRootPayload(encoded, want, fileEnd)
	if err != nil || got != want {
		t.Fatalf("exact-index state root = (%+v,%v), want (%+v,nil)", got, err, want)
	}

	// The exact-index root may not appear without a declared index count.
	invalid := want
	invalid.IndexCount = 0
	invalid.PageCatalogHead = PageRef{}
	invalid.PageCatalogBytes = 0
	invalid.IndexCatalogHash = 0
	if _, err := encodeTestStateRootPayload(page, invalid, fileEnd); !errors.Is(err, ErrInvalidWrite) {
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
	encoded, err := encodeTestStateRootPayload(page, want, fileEnd)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeTestStateRootPayload(encoded, want, fileEnd)
	if err != nil || got != want {
		t.Fatalf(
			"schema state root = (%+v,%v), want (%+v,nil)",
			got, err, want,
		)
	}

	invalid := want
	invalid.IndexCatalogHash = 0
	if _, err := encodeTestStateRootPayload(
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
	encoded, err := encodeTestStateRootPayload(page, want, fileEnd)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeTestStateRootPayload(encoded, want, fileEnd)
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
		if _, err := encodeTestStateRootPayload(
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
		{"next logical id", func(root *StateRoot, _ *uint64) { root.NextLogicalID = 0 }},
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
			if _, err := encodeTestStateRootPayload(page, root, end); !errors.Is(err, ErrInvalidWrite) {
				t.Fatalf("encodeTestStateRootPayload = %v, want %v", err, ErrInvalidWrite)
			}
		})
	}

	empty := StateRoot{
		StoreID:       testStoreID,
		Generation:    1,
		PageSize:      testSuperblockPageSize,
		MaxPageSize:   64 << 10,
		NextLogicalID: 2,
	}
	page := make([]byte, testSuperblockPageSize)
	if _, err := encodeTestStateRootPayload(
		page, empty, testMutableStoreDataStart(testSuperblockPageSize),
	); err != nil {
		t.Fatalf("empty state root: %v", err)
	}
}

func TestStateRootRejectsResealedSemanticCorruption(t *testing.T) {
	root, fileEnd := testStateRoot(3)
	page := make([]byte, testSuperblockPageSize)
	if _, err := encodeTestStateRootPayload(page, root, fileEnd); err != nil {
		t.Fatal(err)
	}
	// The trailing reserved region must decode as entirely blank; any set bit
	// is semantic corruption a valid checksum cannot hide.
	corrupt := append([]byte(nil), page...)
	corrupt[stateRootReservedOffset] = 1
	if _, err := decodeTestStateRootPayload(corrupt, root, fileEnd); !errors.Is(err, ErrStateRootCorrupt) {
		t.Fatalf("reserved byte = %v, want %v", err, ErrStateRootCorrupt)
	}
	for offset := stateRootPrimaryOffset + 29; offset < stateRootPrimaryEnd; offset++ {
		corrupt = append([]byte(nil), page...)
		corrupt[offset] = 1
		if _, err := decodeTestStateRootPayload(corrupt, root, fileEnd); !errors.Is(err, ErrStateRootCorrupt) {
			t.Fatalf("PageRef reserved byte %d = %v, want %v", offset, err, ErrStateRootCorrupt)
		}
	}

	// A resealed page whose version field no longer matches the format must fail
	// before any field is trusted.
	corrupt = append([]byte(nil), page...)
	binary.LittleEndian.PutUint32(corrupt[0:4], stateRootVersion+1)
	if _, err := decodeTestStateRootPayload(corrupt, root, fileEnd); !errors.Is(err, ErrStateRootCorrupt) {
		t.Fatalf("semantic corruption = %v, want %v", err, ErrStateRootCorrupt)
	}
}
