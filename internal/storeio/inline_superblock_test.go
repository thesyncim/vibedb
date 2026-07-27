package storeio

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"testing"
)

func testInlineState(generation uint64) StateRoot {
	return StateRoot{
		StoreID: testStoreID, Generation: generation,
		PageSize: testSuperblockPageSize, NextLogicalID: 2,
		MaxPageSize: 64 << 10, ChunkDocuments: 64,
	}
}

func testInlineSuperblock(generation uint64) InlineSuperblock {
	return InlineSuperblock{
		StoreID: testStoreID, Generation: generation,
		FileEnd:  testMutableStoreDataStart(testSuperblockPageSize),
		PageSize: testSuperblockPageSize, State: testInlineState(generation),
	}
}

func testInlineFreeSuperblock(generation, filePages uint64) InlineSuperblock {
	root := testInlineSuperblock(generation)
	root.FileEnd = filePages * uint64(testSuperblockPageSize)
	root.State.NextLogicalID = 32
	return root
}

func encodeTestInlineSuperblock(t *testing.T, root InlineSuperblock) [InlineSuperblockSize]byte {
	t.Helper()
	var encoded [InlineSuperblockSize]byte
	if _, err := EncodeInlineSuperblock(encoded[:], root); err != nil {
		t.Fatal(err)
	}
	return encoded
}

func resealTestInlineSuperblock(encoded []byte) {
	checksum := PageChecksum(encoded[:inlineSuperblockChecksumFrom])
	binary.LittleEndian.PutUint32(
		encoded[inlineSuperblockChecksumFrom:inlineSuperblockChecksumFrom+4], checksum,
	)
	binary.LittleEndian.PutUint32(
		encoded[inlineSuperblockChecksumFrom+4:InlineSuperblockSize], ^checksum,
	)
}

func writeInlineRootPage(t *testing.T, file *os.File, slot int, encoded []byte) {
	t.Helper()
	page := make([]byte, testSuperblockPageSize)
	copy(page, encoded)
	writeAtTest(t, file, page, int64(slot)*int64(testSuperblockPageSize))
}

func TestInlineSuperblockCodecRejectsCorruptionAndExternalFormat(t *testing.T) {
	root := testInlineSuperblock(7)
	encoded := encodeTestInlineSuperblock(t, root)
	decoded, err := DecodeInlineSuperblock(encoded[:])
	if err != nil {
		t.Fatal(err)
	}
	if decoded != root {
		t.Fatalf("decoded = %+v, want %+v", decoded, root)
	}

	for i := range encoded {
		corrupt := encoded
		corrupt[i] ^= 1
		if _, err := DecodeInlineSuperblock(corrupt[:]); !errors.Is(err, ErrSuperblockCorrupt) {
			t.Fatalf("byte %d corruption = %v, want %v", i, err, ErrSuperblockCorrupt)
		}
	}

	for _, test := range []struct {
		name   string
		mutate func([]byte)
	}{
		{"unknown flags", func(b []byte) { binary.LittleEndian.PutUint32(b[16:20], 1) }},
		{"generation complement", func(b []byte) { b[32] ^= 1 }},
		{"free checksum complement", func(b []byte) { b[64] ^= 1 }},
		{"header reserve", func(b []byte) { b[68] = 1 }},
		{"state reserve", func(b []byte) { b[inlineSuperblockStateEnd-1] = 1 }},
		{"invalid state counts", func(b []byte) {
			binary.LittleEndian.PutUint64(b[inlineSuperblockStateOffset+8:inlineSuperblockStateOffset+16], 1)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			corrupt := encoded
			test.mutate(corrupt[:])
			resealTestInlineSuperblock(corrupt[:])
			if _, err := DecodeInlineSuperblock(corrupt[:]); !errors.Is(err, ErrSuperblockCorrupt) {
				t.Fatalf("DecodeInlineSuperblock = %v, want %v", err, ErrSuperblockCorrupt)
			}
		})
	}

	statePage := make([]byte, testSuperblockPageSize)
	state := testInlineState(7)
	dataStart := testMutableStoreDataStart(testSuperblockPageSize)
	if _, err := EncodeStateRootPage(statePage, state, dataStart+uint64(testSuperblockPageSize)); err != nil {
		t.Fatal(err)
	}
	external := encodeTestSuperblock(t, testSuperblock(
		7, dataStart, statePage,
	))
	if _, err := DecodeInlineSuperblock(external[:]); !errors.Is(err, ErrSuperblockCorrupt) {
		t.Fatalf("inline decoder accepted external format: %v", err)
	}
	if _, err := DecodeSuperblock(encoded[:]); !errors.Is(err, ErrSuperblockCorrupt) {
		t.Fatalf("external decoder accepted inline format: %v", err)
	}
}

func TestInlineSuperblockSharesCanonicalStatePayload(t *testing.T) {
	state := testInlineState(3)
	fileEnd := testMutableStoreDataStart(testSuperblockPageSize)
	statePage := make([]byte, testSuperblockPageSize)
	if _, err := EncodeStateRootPage(statePage, state, fileEnd); err != nil {
		t.Fatal(err)
	}
	_, pagePayload, err := OpenPage(statePage)
	if err != nil {
		t.Fatal(err)
	}
	inline := encodeTestInlineSuperblock(t, InlineSuperblock{
		StoreID: testStoreID, Generation: state.Generation,
		FileEnd: fileEnd, PageSize: state.PageSize, State: state,
	})
	inlinePayload := inline[inlineSuperblockStateOffset:inlineSuperblockStateEnd]
	if !bytes.Equal(inlinePayload, pagePayload) {
		t.Fatal("inline and standalone StateRoot payloads differ")
	}
	decoded, err := decodeStateRootPayload(
		inlinePayload, state.StoreID, state.Generation, state.PageSize, fileEnd,
	)
	if err != nil || decoded != state {
		t.Fatalf("decodeStateRootPayload = (%+v,%v)", decoded, err)
	}
}

func TestInlineFreeDeltaRoundTripAndLatestWins(t *testing.T) {
	pageSize := uint64(testSuperblockPageSize)
	root := testInlineFreeSuperblock(7, 16)
	prev := PageRef{
		Offset: 4 * pageSize, LogicalID: 2, Generation: 6,
		Length: testSuperblockPageSize, Kind: PageFreeDelta,
	}
	indexHead := PageRef{
		Offset: 5 * pageSize, LogicalID: 3, Generation: 5,
		Length: testSuperblockPageSize, Kind: PageFreeIndex,
	}
	root.FreeDelta = NewInlineFreeDelta(prev, indexHead)
	if err := root.FreeDelta.Append([]FreeDelta{
		{Op: FreeOpSet, Extent: FreeExtent{
			Offset: 6 * pageSize, Length: 2 * pageSize, RetiredGeneration: 4,
		}},
		{Op: FreeOpDelete, Extent: FreeExtent{Offset: 9 * pageSize}},
		{Op: FreeOpDelete, Extent: FreeExtent{Offset: 6 * pageSize}},
	}, root.PageSize, root.FileEnd); err != nil {
		t.Fatal(err)
	}
	if root.FreeDelta.Len() != 2 {
		t.Fatalf("canonical records = %d, want 2", root.FreeDelta.Len())
	}
	latest, ok := root.FreeDelta.Latest(6 * pageSize)
	if !ok || latest.Op != FreeOpDelete {
		t.Fatalf("latest = (%+v,%v), want delete", latest, ok)
	}

	encoded := encodeTestInlineSuperblock(t, root)
	decoded, err := DecodeInlineSuperblock(encoded[:])
	if err != nil || decoded != root {
		t.Fatalf("DecodeInlineSuperblock = (%+v,%v), want %+v", decoded, err, root)
	}
	if decoded.FreeDelta.ExternalPrev() != prev ||
		decoded.FreeDelta.IndexHead() != indexHead {
		t.Fatalf(
			"inline free anchors = (%+v,%+v)",
			decoded.FreeDelta.ExternalPrev(), decoded.FreeDelta.IndexHead(),
		)
	}
	for rank := 0; rank < root.FreeDelta.Len(); rank++ {
		got, ok := decoded.FreeDelta.DeltaAt(rank)
		want, _ := root.FreeDelta.DeltaAt(rank)
		if !ok || got != want {
			t.Fatalf("DeltaAt(%d) = (%+v,%v), want %+v", rank, got, ok, want)
		}
	}
	usedEnd := inlineFreeDeltaRecordsOffset +
		root.FreeDelta.Len()*FreeDeltaRecordSize
	if !allZero(encoded[usedEnd:inlineSuperblockChecksumFrom]) {
		t.Fatal("unused inline free-delta tail is non-zero")
	}
}

func TestInlineFreeDeltaCanonicalizesOffsetOrder(t *testing.T) {
	pageSize := uint64(testSuperblockPageSize)
	root := testInlineFreeSuperblock(7, 16)
	records := []FreeDelta{
		{Op: FreeOpDelete, Extent: FreeExtent{Offset: 9 * pageSize}},
		{Op: FreeOpDelete, Extent: FreeExtent{Offset: 4 * pageSize}},
		{Op: FreeOpSet, Extent: FreeExtent{
			Offset: 6 * pageSize, Length: pageSize, RetiredGeneration: 5,
		}},
	}
	if err := root.FreeDelta.Append(records, root.PageSize, root.FileEnd); err != nil {
		t.Fatal(err)
	}
	var previous uint64
	for rank := 0; rank < root.FreeDelta.Len(); rank++ {
		record, ok := root.FreeDelta.DeltaAt(rank)
		if !ok || rank != 0 && record.Extent.Offset <= previous {
			t.Fatalf("record %d = (%+v,%v), previous offset %d", rank, record, ok, previous)
		}
		previous = record.Extent.Offset
	}

	other := testInlineFreeSuperblock(7, 16)
	for index := len(records) - 1; index >= 0; index-- {
		if err := other.FreeDelta.Append(records[index:index+1], other.PageSize, other.FileEnd); err != nil {
			t.Fatal(err)
		}
	}
	first := encodeTestInlineSuperblock(t, root)
	second := encodeTestInlineSuperblock(t, other)
	if first != second {
		t.Fatal("equivalent cumulative maps have history-dependent encoding")
	}
}

func TestInlineFreeDeltaCapacityAndTransactionalAppend(t *testing.T) {
	pageSize := uint64(testSuperblockPageSize)
	if external := FreeDeltaRecordCapacity(testSuperblockPageSize); InlineFreeDeltaCapacity > external {
		t.Fatalf(
			"inline capacity %d exceeds one external delta page (%d)",
			InlineFreeDeltaCapacity, external,
		)
	}
	fileEnd := uint64(InlineFreeDeltaCapacity+4) * pageSize
	var delta InlineFreeDelta
	records := make([]FreeDelta, InlineFreeDeltaCapacity)
	for rank := range records {
		records[rank] = FreeDelta{
			Op:     FreeOpDelete,
			Extent: FreeExtent{Offset: uint64(rank+4) * pageSize},
		}
	}
	if err := delta.Append(records, testSuperblockPageSize, fileEnd); err != nil {
		t.Fatal(err)
	}
	if delta.Len() != InlineFreeDeltaCapacity {
		t.Fatalf("records = %d, want %d", delta.Len(), InlineFreeDeltaCapacity)
	}
	full := delta
	if err := delta.Append([]FreeDelta{{
		Op:     FreeOpDelete,
		Extent: FreeExtent{Offset: uint64(InlineFreeDeltaCapacity+4) * pageSize},
	}}, testSuperblockPageSize, fileEnd); !errors.Is(err, ErrInlineFreeDeltaFull) {
		t.Fatalf("overflow = %v, want %v", err, ErrInlineFreeDeltaFull)
	}
	if delta != full {
		t.Fatal("overflow changed cumulative records")
	}

	if err := delta.Append([]FreeDelta{{
		Op: FreeOpSet,
		Extent: FreeExtent{
			Offset: 4 * pageSize, Length: pageSize, RetiredGeneration: 3,
		},
	}}, testSuperblockPageSize, fileEnd); err != nil {
		t.Fatal(err)
	}
	if delta.Len() != InlineFreeDeltaCapacity {
		t.Fatalf("replacement grew count to %d", delta.Len())
	}
	latest, ok := delta.Latest(4 * pageSize)
	if !ok || latest.Op != FreeOpSet {
		t.Fatalf("replacement latest = (%+v,%v)", latest, ok)
	}

	beforeInvalid := delta
	if err := delta.Append([]FreeDelta{{
		Op:     FreeOpDelete,
		Extent: FreeExtent{Offset: 5 * pageSize, Length: pageSize},
	}}, testSuperblockPageSize, fileEnd); !errors.Is(err, ErrInvalidWrite) {
		t.Fatalf("invalid record = %v, want %v", err, ErrInvalidWrite)
	}
	if delta != beforeInvalid {
		t.Fatal("invalid append changed cumulative records")
	}
}

func TestInlineFreeDeltaRejectsOverlapAndMalformedEncoding(t *testing.T) {
	pageSize := uint64(testSuperblockPageSize)
	root := testInlineFreeSuperblock(7, 20)
	if err := root.FreeDelta.Append([]FreeDelta{
		{Op: FreeOpSet, Extent: FreeExtent{
			Offset: 4 * pageSize, Length: 3 * pageSize, RetiredGeneration: 2,
		}},
		{Op: FreeOpSet, Extent: FreeExtent{
			Offset: 6 * pageSize, Length: pageSize, RetiredGeneration: 3,
		}},
	}, root.PageSize, root.FileEnd); !errors.Is(err, ErrInvalidWrite) {
		t.Fatalf("overlap = %v, want %v", err, ErrInvalidWrite)
	}
	if root.FreeDelta.Len() != 0 {
		t.Fatal("overlapping append changed cumulative records")
	}
	if err := root.FreeDelta.Append([]FreeDelta{
		{Op: FreeOpDelete, Extent: FreeExtent{Offset: 4 * pageSize}},
		{Op: FreeOpDelete, Extent: FreeExtent{Offset: 5 * pageSize}},
	}, root.PageSize, root.FileEnd); err != nil {
		t.Fatal(err)
	}
	encoded := encodeTestInlineSuperblock(t, root)

	for _, test := range []struct {
		name   string
		mutate func([]byte)
	}{
		{"count", func(b []byte) {
			binary.LittleEndian.PutUint16(
				b[inlineFreeDeltaCountOffset:inlineFreeDeltaCountOffset+2],
				InlineFreeDeltaCapacity+1,
			)
		}},
		{"header reserve", func(b []byte) { b[inlineFreeDeltaCountOffset+2] = 1 }},
		{"record operation", func(b []byte) { b[inlineFreeDeltaRecordsOffset] = 0 }},
		{"record reserve", func(b []byte) { b[inlineFreeDeltaRecordsOffset+1] = 1 }},
		{"duplicate offset", func(b []byte) {
			copy(
				b[inlineFreeDeltaRecordsOffset+FreeDeltaRecordSize+8:inlineFreeDeltaRecordsOffset+FreeDeltaRecordSize+16],
				b[inlineFreeDeltaRecordsOffset+8:inlineFreeDeltaRecordsOffset+16],
			)
		}},
		{"nonzero tail", func(b []byte) {
			b[inlineFreeDeltaRecordsOffset+2*FreeDeltaRecordSize] = 1
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			corrupt := encoded
			test.mutate(corrupt[:])
			resealTestInlineSuperblock(corrupt[:])
			if _, err := DecodeInlineSuperblock(corrupt[:]); !errors.Is(err, ErrSuperblockCorrupt) {
				t.Fatalf("DecodeInlineSuperblock = %v, want %v", err, ErrSuperblockCorrupt)
			}
		})
	}
}

func TestInlineFreeDeltaValidatesAnchorsAndLiveExtents(t *testing.T) {
	pageSize := uint64(testSuperblockPageSize)
	valid := testInlineFreeSuperblock(7, 16)
	prev := PageRef{
		Offset: 4 * pageSize, LogicalID: 2, Generation: 6,
		Length: testSuperblockPageSize, Kind: PageFreeDelta,
	}
	indexHead := PageRef{
		Offset: 5 * pageSize, LogicalID: 3, Generation: 6,
		Length: testSuperblockPageSize, Kind: PageFreeIndex,
	}
	valid.FreeDelta = NewInlineFreeDelta(prev, indexHead)
	for _, test := range []struct {
		name   string
		mutate func(*InlineSuperblock)
	}{
		{"prev kind", func(root *InlineSuperblock) {
			root.FreeDelta.externalPrev.Kind = PageFreeIndex
		}},
		{"index kind", func(root *InlineSuperblock) {
			root.FreeDelta.indexHead.Kind = PageFreeDelta
		}},
		{"future generation", func(root *InlineSuperblock) {
			root.FreeDelta.externalPrev.Generation = root.Generation + 1
		}},
		{"logical bound", func(root *InlineSuperblock) {
			root.FreeDelta.indexHead.LogicalID = root.State.NextLogicalID
		}},
		{"duplicate physical", func(root *InlineSuperblock) {
			root.FreeDelta.indexHead.Offset = root.FreeDelta.externalPrev.Offset
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := valid
			test.mutate(&root)
			var encoded [InlineSuperblockSize]byte
			if _, err := EncodeInlineSuperblock(encoded[:], root); !errors.Is(err, ErrInvalidWrite) {
				t.Fatalf("EncodeInlineSuperblock = %v, want %v", err, ErrInvalidWrite)
			}
		})
	}

	root := valid
	if err := root.FreeDelta.Append([]FreeDelta{{
		Op: FreeOpSet,
		Extent: FreeExtent{
			Offset: prev.Offset, Length: uint64(prev.Length), RetiredGeneration: 5,
		},
	}}, root.PageSize, root.FileEnd); err != nil {
		t.Fatal(err)
	}
	var encoded [InlineSuperblockSize]byte
	if _, err := EncodeInlineSuperblock(encoded[:], root); !errors.Is(err, ErrInvalidWrite) {
		t.Fatalf("live external extent = %v, want %v", err, ErrInvalidWrite)
	}

	root = valid
	if err := root.FreeDelta.Append([]FreeDelta{{
		Op: FreeOpSet,
		Extent: FreeExtent{
			Offset: 8 * pageSize, Length: pageSize,
			RetiredGeneration: root.Generation + 1,
		},
	}}, root.PageSize, root.FileEnd); err != nil {
		t.Fatal(err)
	}
	if _, err := EncodeInlineSuperblock(encoded[:], root); !errors.Is(err, ErrInvalidWrite) {
		t.Fatalf("future retirement = %v, want %v", err, ErrInvalidWrite)
	}

	root = testInlineFreeSuperblock(7, 16)
	root.State.IndexGroupHead = PageRef{
		Offset: 4 * pageSize, LogicalID: 4, Generation: 6,
		Length: 2 * testSuperblockPageSize, Kind: PageIndexGroupCatalog,
	}
	root.FreeDelta = NewInlineFreeDelta(PageRef{
		Offset: 5 * pageSize, LogicalID: 8, Generation: 6,
		Length: testSuperblockPageSize, Kind: PageFreeDelta,
	}, PageRef{})
	if _, err := EncodeInlineSuperblock(encoded[:], root); !errors.Is(err, ErrInvalidWrite) {
		t.Fatalf("anchor overlapping second state page = %v, want %v", err, ErrInvalidWrite)
	}
	root.FreeDelta.externalPrev.Offset = 7 * pageSize
	root.FreeDelta.externalPrev.LogicalID = root.State.IndexGroupHead.LogicalID
	if _, err := EncodeInlineSuperblock(encoded[:], root); !errors.Is(err, ErrInvalidWrite) {
		t.Fatalf("anchor logical collision = %v, want %v", err, ErrInvalidWrite)
	}

	root = testInlineFreeSuperblock(7, 16)
	catalogCapacity, ok := pageCatalogSegmentDataCapacity(root.PageSize)
	if !ok {
		t.Fatal("catalog segment geometry")
	}
	root.State.Options |= StateOptionSchema
	root.State.IndexCatalogHash = 1
	root.State.PageCatalogHead = PageRef{
		Offset: 4 * pageSize, LogicalID: 4, Generation: 6,
		Length: root.PageSize, Kind: PageCatalogSegment,
	}
	root.State.PageCatalogBytes = uint32(catalogCapacity + 1)
	if err := root.FreeDelta.Append([]FreeDelta{{
		Op: FreeOpSet,
		Extent: FreeExtent{
			Offset: 5 * pageSize, Length: pageSize,
			RetiredGeneration: 5,
		},
	}}, root.PageSize, root.FileEnd); err != nil {
		t.Fatal(err)
	}
	if _, err := EncodeInlineSuperblock(
		encoded[:], root,
	); !errors.Is(err, ErrInvalidWrite) {
		t.Fatalf(
			"retirement overlapping catalog tail = %v, want %v",
			err, ErrInvalidWrite,
		)
	}
}

func TestInlineSuperblockSelectionAndConflict(t *testing.T) {
	root1 := testInlineSuperblock(1)
	root2 := testInlineSuperblock(2)
	first := encodeTestInlineSuperblock(t, root1)
	second := encodeTestInlineSuperblock(t, root2)

	got, slot, err := SelectInlineSuperblock(first[:], second[:])
	if err != nil || got != root2 || slot != 1 {
		t.Fatalf("newest = %+v slot %d err %v", got, slot, err)
	}
	second[0] ^= 1
	got, slot, err = SelectInlineSuperblock(first[:], second[:])
	if err != nil || got != root1 || slot != 0 {
		t.Fatalf("fallback = %+v slot %d err %v", got, slot, err)
	}
	first[0] ^= 1
	if _, _, err := SelectInlineSuperblock(first[:], second[:]); !errors.Is(err, ErrSuperblockNotFound) {
		t.Fatalf("both corrupt = %v, want %v", err, ErrSuperblockNotFound)
	}

	first = encodeTestInlineSuperblock(t, root1)
	root2.StoreID[0] ^= 1
	root2.State.StoreID = root2.StoreID
	second = encodeTestInlineSuperblock(t, root2)
	if _, _, err := SelectInlineSuperblock(first[:], second[:]); !errors.Is(err, ErrSuperblockConflict) {
		t.Fatalf("foreign root = %v, want %v", err, ErrSuperblockConflict)
	}
}

func TestRecoverInlineStateRootUsesOnlyFixedPagesAndFallsBack(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "inline-superblock-recovery-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if err := file.Truncate(
		int64(testMutableStoreDataStart(testSuperblockPageSize)),
	); err != nil {
		t.Fatal(err)
	}
	root1 := testInlineSuperblock(1)
	root2 := testInlineSuperblock(2)
	first := encodeTestInlineSuperblock(t, root1)
	second := encodeTestInlineSuperblock(t, root2)
	writeInlineRootPage(t, file, 0, first[:])
	writeInlineRootPage(t, file, 1, second[:])

	scratch := make([]byte, testSuperblockPageSize)
	got, state, slot, fallback, err := RecoverInlineStateRootWithFallback(
		file, testSuperblockPageSize, scratch,
	)
	if err != nil || got != root2 || state != root2.State || slot != 1 || fallback != 1 {
		t.Fatalf("recover = (%+v,%+v,%d,%d,%v)", got, state, slot, fallback, err)
	}

	second[17] ^= 1
	writeInlineRootPage(t, file, 1, second[:])
	got, state, slot, fallback, err = RecoverInlineStateRootWithFallback(
		file, testSuperblockPageSize, scratch,
	)
	if err != nil || got != root1 || state != root1.State || slot != 0 || fallback != 1 {
		t.Fatalf("torn fallback = (%+v,%+v,%d,%d,%v)", got, state, slot, fallback, err)
	}

	first[0] ^= 1
	writeInlineRootPage(t, file, 0, first[:])
	if _, _, _, err := RecoverInlineStateRoot(
		file, testSuperblockPageSize, scratch,
	); !errors.Is(err, ErrSuperblockNotFound) {
		t.Fatalf("both torn = %v, want %v", err, ErrSuperblockNotFound)
	}
}

func TestRecoverInlineFreeDeltaFallbackIsSelfContained(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "inline-free-fallback-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if err := file.Truncate(6 * int64(testSuperblockPageSize)); err != nil {
		t.Fatal(err)
	}
	pageSize := uint64(testSuperblockPageSize)
	root1 := testInlineFreeSuperblock(1, 6)
	if err := root1.FreeDelta.Append([]FreeDelta{{
		Op: FreeOpDelete, Extent: FreeExtent{Offset: 4 * pageSize},
	}}, root1.PageSize, root1.FileEnd); err != nil {
		t.Fatal(err)
	}
	root2 := testInlineFreeSuperblock(2, 6)
	root2.FreeDelta = root1.FreeDelta
	if err := root2.FreeDelta.Append([]FreeDelta{{
		Op: FreeOpDelete, Extent: FreeExtent{Offset: 5 * pageSize},
	}}, root2.PageSize, root2.FileEnd); err != nil {
		t.Fatal(err)
	}
	first := encodeTestInlineSuperblock(t, root1)
	second := encodeTestInlineSuperblock(t, root2)
	writeInlineRootPage(t, file, 0, first[:])
	writeInlineRootPage(t, file, 1, second[:])

	scratch := make([]byte, testSuperblockPageSize)
	got, _, slot, err := RecoverInlineStateRoot(file, testSuperblockPageSize, scratch)
	if err != nil || got != root2 || slot != 1 || got.FreeDelta.Len() != 2 {
		t.Fatalf("newest cumulative root = (%+v,%d,%v)", got, slot, err)
	}

	second[0] ^= 1
	writeInlineRootPage(t, file, 1, second[:])
	got, _, slot, err = RecoverInlineStateRoot(file, testSuperblockPageSize, scratch)
	if err != nil || got != root1 || slot != 0 || got.FreeDelta.Len() != 1 {
		t.Fatalf("fallback cumulative root = (%+v,%d,%v)", got, slot, err)
	}
	if _, ok := got.FreeDelta.Latest(4 * pageSize); !ok {
		t.Fatal("fallback lost its cumulative record")
	}
	if _, ok := got.FreeDelta.Latest(5 * pageSize); ok {
		t.Fatal("fallback observed a newer root's record")
	}
}

func TestRecoverInlineFreeDeltaValidatesExternalAnchor(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "inline-free-external-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if err := file.Truncate(6 * int64(testSuperblockPageSize)); err != nil {
		t.Fatal(err)
	}
	pageSize := uint64(testSuperblockPageSize)
	root1 := testInlineFreeSuperblock(1, 6)
	root2 := testInlineFreeSuperblock(2, 6)
	externalPrev := PageRef{
		Offset: 4 * pageSize, LogicalID: 2, Generation: 1,
		Length: testSuperblockPageSize, Kind: PageFreeDelta,
	}
	root2.FreeDelta = NewInlineFreeDelta(externalPrev, PageRef{})
	if err := root2.FreeDelta.Append([]FreeDelta{{
		Op: FreeOpDelete, Extent: FreeExtent{Offset: externalPrev.Offset},
	}}, root2.PageSize, root2.FileEnd); err != nil {
		t.Fatal(err)
	}
	externalPage := make([]byte, testSuperblockPageSize)
	if _, err := EncodeFreeDeltaPage(
		externalPage,
		FreeLogHeader{
			StoreID: testStoreID, Generation: externalPrev.Generation,
			LogicalID: externalPrev.LogicalID, PageSize: testSuperblockPageSize,
		},
		[]FreeDelta{{
			Op: FreeOpDelete, Extent: FreeExtent{Offset: 5 * pageSize},
		}},
		PageRef{}, PageRef{}, root2.FileEnd, root2.State.NextLogicalID,
	); err != nil {
		t.Fatal(err)
	}
	first := encodeTestInlineSuperblock(t, root1)
	second := encodeTestInlineSuperblock(t, root2)
	writeInlineRootPage(t, file, 0, first[:])
	writeInlineRootPage(t, file, 1, second[:])
	writeAtTest(t, file, externalPage, int64(externalPrev.Offset))

	scratch := make([]byte, testSuperblockPageSize)
	got, _, slot, err := RecoverInlineStateRoot(file, testSuperblockPageSize, scratch)
	if err != nil || got != root2 || slot != 1 {
		t.Fatalf("external anchored root = (%+v,%d,%v)", got, slot, err)
	}

	one := []byte{0}
	writeAtTest(t, file, one, int64(externalPrev.Offset))
	got, _, slot, err = RecoverInlineStateRoot(file, testSuperblockPageSize, scratch)
	if err != nil || got != root1 || slot != 0 {
		t.Fatalf("corrupt external fallback = (%+v,%d,%v)", got, slot, err)
	}
}

func TestRecoverInlineFreeDeltaValidatesIndexOnlyAnchor(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "inline-free-index-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if err := file.Truncate(7 * int64(testSuperblockPageSize)); err != nil {
		t.Fatal(err)
	}
	pageSize := uint64(testSuperblockPageSize)
	root1 := testInlineFreeSuperblock(1, 7)
	root2 := testInlineFreeSuperblock(2, 7)
	indexHead := PageRef{
		Offset: 4 * pageSize, LogicalID: 2, Generation: 1,
		Length: testSuperblockPageSize, Kind: PageFreeIndex,
	}
	root2.FreeDelta = NewInlineFreeDelta(PageRef{}, indexHead)
	indexPage := make([]byte, testSuperblockPageSize)
	if _, err := EncodeFreeIndexPage(
		indexPage,
		FreeLogHeader{
			StoreID: testStoreID, Generation: indexHead.Generation,
			LogicalID: indexHead.LogicalID, PageSize: testSuperblockPageSize,
		},
		[]FreeSegment{{
			Ref: PageRef{
				Offset: 5 * pageSize, LogicalID: 3, Generation: 1,
				Length: testSuperblockPageSize, Kind: PageFreeImage,
			},
			FirstOffset: 6 * pageSize, LargestFree: pageSize, Count: 1,
		}},
		PageRef{}, root2.FileEnd, root2.State.NextLogicalID,
	); err != nil {
		t.Fatal(err)
	}
	first := encodeTestInlineSuperblock(t, root1)
	second := encodeTestInlineSuperblock(t, root2)
	writeInlineRootPage(t, file, 0, first[:])
	writeInlineRootPage(t, file, 1, second[:])
	writeAtTest(t, file, indexPage, int64(indexHead.Offset))

	scratch := make([]byte, testSuperblockPageSize)
	got, _, slot, err := RecoverInlineStateRoot(file, testSuperblockPageSize, scratch)
	if err != nil || got != root2 || slot != 1 {
		t.Fatalf("index-only anchored root = (%+v,%d,%v)", got, slot, err)
	}

	writeAtTest(t, file, []byte{0}, int64(indexHead.Offset))
	got, _, slot, err = RecoverInlineStateRoot(file, testSuperblockPageSize, scratch)
	if err != nil || got != root1 || slot != 0 {
		t.Fatalf("corrupt index-only fallback = (%+v,%d,%v)", got, slot, err)
	}
}

func TestRecoverInlineStateRootFallsBackOnReferencedPageCorruption(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "inline-superblock-semantic-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	root1 := testInlineSuperblock(1)
	root2 := testInlineSuperblock(2)
	root2.FileEnd = 6 * uint64(testSuperblockPageSize)
	root2.State.NextLogicalID = 4
	root2.State.DocumentCount = 1
	root2.State.ChunkHighWater = 1
	root2.State.LiveChunks = 1
	root2.State.ChunkDirectory = PageRef{
		Offset: 4 * uint64(testSuperblockPageSize), LogicalID: 2,
		Generation: 2, Length: testSuperblockPageSize, Kind: PageChunkDirectory,
	}
	root2.State.KeyDirectory = PageRef{
		Offset: 5 * uint64(testSuperblockPageSize), LogicalID: 3,
		Generation: 2, Length: testSuperblockPageSize, Kind: PageKeyDirectory,
	}
	first := encodeTestInlineSuperblock(t, root1)
	second := encodeTestInlineSuperblock(t, root2)
	writeInlineRootPage(t, file, 0, first[:])
	writeInlineRootPage(t, file, 1, second[:])
	for _, ref := range []PageRef{root2.State.ChunkDirectory, root2.State.KeyDirectory} {
		page := make([]byte, testSuperblockPageSize)
		payload, err := InitPage(page, PageHeader{
			StoreID: testStoreID, Generation: ref.Generation, LogicalID: ref.LogicalID,
			PageSize: ref.Length, PayloadLength: 0, Kind: ref.Kind,
		})
		if err != nil || len(payload) != 0 {
			t.Fatalf("InitPage = (%d,%v)", len(payload), err)
		}
		if _, err := sealInitializedPage(page); err != nil {
			t.Fatal(err)
		}
		writeAtTest(t, file, page, int64(ref.Offset))
	}

	scratch := make([]byte, testSuperblockPageSize)
	got, _, slot, err := RecoverInlineStateRoot(file, testSuperblockPageSize, scratch)
	if err != nil || got != root2 || slot != 1 {
		t.Fatalf("recover newest = (%+v,%d,%v)", got, slot, err)
	}

	one := []byte{0}
	writeAtTest(t, file, one, int64(root2.State.KeyDirectory.Offset))
	got, _, slot, err = RecoverInlineStateRoot(file, testSuperblockPageSize, scratch)
	if err != nil || got != root1 || slot != 0 {
		t.Fatalf("semantic fallback = (%+v,%d,%v)", got, slot, err)
	}
}

func TestPublishInlineEliminatesDedicatedStatePage(t *testing.T) {
	externalCommitter, externalFile, pageSize := newPortableCommitter(t, 4, 1)
	defer externalCommitter.Close()
	dataStart := testMutableStoreDataStart(uint32(pageSize))
	if err := externalFile.Truncate(int64(dataStart)); err != nil {
		t.Fatal(err)
	}
	externalTx, err := BeginWriteTransaction(
		externalCommitter, nil, 1, WriteTransactionOptions{
			StoreID: testStoreID, Generation: 1, PageSize: uint32(pageSize),
			FileEnd: dataStart, NextLogicalID: 2,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	statePage, err := externalTx.Allocate(PageStateRoot, uint32(pageSize), StateRootLogicalID)
	if err != nil {
		t.Fatal(err)
	}
	externalState := StateRoot{
		StoreID: testStoreID, Generation: 1, PageSize: uint32(pageSize),
		MaxPageSize: uint32(pageSize), NextLogicalID: 2, ChunkDocuments: 64,
	}
	if _, err := EncodeStateRootPage(statePage.Bytes(), externalState, externalTx.FileEnd()); err != nil {
		t.Fatal(err)
	}
	if err := statePage.Stage(); err != nil {
		t.Fatal(err)
	}
	if err := externalTx.Publish(
		statePage.Ref(), PageChecksum(statePage.Bytes()), 0, 0, 0,
	); err != nil {
		t.Fatal(err)
	}
	if err := externalCommitter.Wait(1); err != nil {
		t.Fatal(err)
	}
	externalInfo, err := externalFile.Stat()
	if err != nil {
		t.Fatal(err)
	}

	inlineCommitter, inlineFile, inlinePageSize := newPortableCommitter(t, 2, 0)
	defer inlineCommitter.Close()
	inlineDataStart := testMutableStoreDataStart(uint32(inlinePageSize))
	if err := inlineFile.Truncate(int64(inlineDataStart)); err != nil {
		t.Fatal(err)
	}
	inlineTx, err := BeginWriteTransaction(
		inlineCommitter, nil, 0, WriteTransactionOptions{
			StoreID: testStoreID, Generation: 1, PageSize: uint32(inlinePageSize),
			FileEnd: inlineDataStart, NextLogicalID: 2,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	inlineState := StateRoot{
		StoreID: testStoreID, Generation: 1, PageSize: uint32(inlinePageSize),
		MaxPageSize: uint32(inlinePageSize), NextLogicalID: 2, ChunkDocuments: 64,
	}
	if err := inlineTx.PublishInline(inlineState, InlineFreeDelta{}); err != nil {
		t.Fatal(err)
	}
	if err := inlineCommitter.Wait(1); err != nil {
		t.Fatal(err)
	}
	inlineInfo, err := inlineFile.Stat()
	if err != nil {
		t.Fatal(err)
	}

	externalBytes := externalCommitter.Stats().DeviceBytes
	inlineBytes := inlineCommitter.Stats().DeviceBytes
	if externalBytes != uint64(2*pageSize) || inlineBytes != uint64(inlinePageSize) ||
		externalBytes-inlineBytes != uint64(inlinePageSize) {
		t.Fatalf(
			"device bytes external=%d inline=%d page=%d",
			externalBytes, inlineBytes, inlinePageSize,
		)
	}
	if externalTx.FileEnd()-inlineTx.FileEnd() != uint64(inlinePageSize) ||
		externalInfo.Size()-inlineInfo.Size() != int64(inlinePageSize) {
		t.Fatalf(
			"file bytes external=(logical=%d physical=%d) inline=(logical=%d physical=%d)",
			externalTx.FileEnd(), externalInfo.Size(), inlineTx.FileEnd(), inlineInfo.Size(),
		)
	}
	scratch := make([]byte, inlinePageSize)
	_, recovered, _, err := RecoverInlineStateRoot(
		inlineFile, uint32(inlinePageSize), scratch,
	)
	if err != nil || recovered != inlineState {
		t.Fatalf("RecoverInlineStateRoot = (%+v,%v)", recovered, err)
	}
}

func TestPublishInlineClearsPhysicalTailAboveCodecSize(t *testing.T) {
	const pageSize = uint32(8192)
	file, err := os.CreateTemp(t.TempDir(), "inline-large-root-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	dataStart := testMutableStoreDataStart(pageSize)
	if err := file.Truncate(int64(dataStart)); err != nil {
		t.Fatal(err)
	}
	committer, err := NewCommitter(file, DeviceOptions{
		Backend: BackendPortable, BufferCount: 2,
		BufferSize: max(os.Getpagesize(), int(pageSize)),
	}, CommitterOptions{QueueSlots: 2, MaxPagesPerBatch: 1, GroupLimit: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer committer.Close()
	tx, err := BeginWriteTransaction(committer, nil, 0, WriteTransactionOptions{
		StoreID: testStoreID, Generation: 1, PageSize: pageSize,
		FileEnd: dataStart, NextLogicalID: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	state := StateRoot{
		StoreID: testStoreID, Generation: 1, PageSize: pageSize,
		MaxPageSize: pageSize, NextLogicalID: 2, ChunkDocuments: 64,
	}
	if err := tx.PublishInline(state, InlineFreeDelta{}); err != nil {
		t.Fatal(err)
	}
	if err := committer.Wait(1); err != nil {
		t.Fatal(err)
	}
	offset, err := SuperblockOffset(1, pageSize)
	if err != nil {
		t.Fatal(err)
	}
	physical := make([]byte, pageSize)
	if _, err := file.ReadAt(physical, int64(offset)); err != nil {
		t.Fatal(err)
	}
	if !allZero(physical[InlineSuperblockSize:]) {
		t.Fatal("inline physical root tail is non-zero")
	}
	if _, err := DecodeInlineSuperblock(physical); err != nil {
		t.Fatal(err)
	}
}

func TestPublishInlineEliminatesRoutineStateAndFreeDeltaPages(t *testing.T) {
	externalCommitter, externalFile, pageSize := newPortableCommitter(t, 5, 2)
	defer externalCommitter.Close()
	dataStart := testMutableStoreDataStart(uint32(pageSize))
	if err := externalFile.Truncate(int64(dataStart)); err != nil {
		t.Fatal(err)
	}
	externalTx, err := BeginWriteTransaction(
		externalCommitter, nil, 2, WriteTransactionOptions{
			StoreID: testStoreID, Generation: 1, PageSize: uint32(pageSize),
			FileEnd: dataStart, NextLogicalID: 2,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	freePage, err := externalTx.Allocate(PageFreeDelta, uint32(pageSize), 0)
	if err != nil {
		t.Fatal(err)
	}
	statePage, err := externalTx.Allocate(
		PageStateRoot, uint32(pageSize), StateRootLogicalID,
	)
	if err != nil {
		t.Fatal(err)
	}
	externalDelta := FreeDelta{
		Op:     FreeOpDelete,
		Extent: FreeExtent{Offset: dataStart},
	}
	if _, err := EncodeFreeDeltaPage(
		freePage.Bytes(),
		FreeLogHeader{
			StoreID: testStoreID, Generation: 1, LogicalID: freePage.Ref().LogicalID,
			PageSize: uint32(pageSize),
		},
		[]FreeDelta{externalDelta}, PageRef{}, PageRef{},
		externalTx.FileEnd(), externalTx.NextLogicalID(),
	); err != nil {
		t.Fatal(err)
	}
	if err := freePage.Stage(); err != nil {
		t.Fatal(err)
	}
	externalState := StateRoot{
		StoreID: testStoreID, Generation: 1, PageSize: uint32(pageSize),
		MaxPageSize:   uint32(pageSize),
		NextLogicalID: externalTx.NextLogicalID(), ChunkDocuments: 64,
	}
	if _, err := EncodeStateRootPage(
		statePage.Bytes(), externalState, externalTx.FileEnd(),
	); err != nil {
		t.Fatal(err)
	}
	if err := statePage.Stage(); err != nil {
		t.Fatal(err)
	}
	if err := externalTx.Publish(
		statePage.Ref(), PageChecksum(statePage.Bytes()),
		freePage.Ref().Offset, freePage.Ref().Length, PageChecksum(freePage.Bytes()),
	); err != nil {
		t.Fatal(err)
	}
	if err := externalCommitter.Wait(1); err != nil {
		t.Fatal(err)
	}
	externalInfo, err := externalFile.Stat()
	if err != nil {
		t.Fatal(err)
	}

	inlineCommitter, inlineFile, inlinePageSize := newPortableCommitter(t, 2, 0)
	defer inlineCommitter.Close()
	inlineDataStart := testMutableStoreDataStart(uint32(inlinePageSize))
	if err := inlineFile.Truncate(int64(inlineDataStart)); err != nil {
		t.Fatal(err)
	}
	inlineTx, err := BeginWriteTransaction(
		inlineCommitter, nil, 0, WriteTransactionOptions{
			StoreID: testStoreID, Generation: 1, PageSize: uint32(inlinePageSize),
			FileEnd: inlineDataStart, NextLogicalID: 2,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	inlineState := StateRoot{
		StoreID: testStoreID, Generation: 1, PageSize: uint32(inlinePageSize),
		MaxPageSize: uint32(inlinePageSize), NextLogicalID: 2, ChunkDocuments: 64,
	}
	var inlineDelta InlineFreeDelta
	if err := inlineDelta.Append([]FreeDelta{{
		Op:     FreeOpDelete,
		Extent: FreeExtent{Offset: inlineDataStart},
	}}, uint32(inlinePageSize), inlineTx.FileEnd()); err != nil {
		t.Fatal(err)
	}
	if err := inlineTx.PublishInline(inlineState, inlineDelta); err != nil {
		t.Fatal(err)
	}
	if err := inlineCommitter.Wait(1); err != nil {
		t.Fatal(err)
	}
	inlineInfo, err := inlineFile.Stat()
	if err != nil {
		t.Fatal(err)
	}

	externalBytes := externalCommitter.Stats().DeviceBytes
	inlineBytes := inlineCommitter.Stats().DeviceBytes
	if externalBytes != uint64(3*pageSize) ||
		inlineBytes != uint64(inlinePageSize) ||
		externalBytes-inlineBytes != uint64(2*inlinePageSize) {
		t.Fatalf(
			"metadata device bytes external=%d inline=%d page=%d",
			externalBytes, inlineBytes, inlinePageSize,
		)
	}
	if externalTx.FileEnd()-inlineTx.FileEnd() != uint64(2*inlinePageSize) ||
		externalInfo.Size()-inlineInfo.Size() != int64(2*inlinePageSize) {
		t.Fatalf(
			"metadata file bytes external=(logical=%d physical=%d) inline=(logical=%d physical=%d)",
			externalTx.FileEnd(), externalInfo.Size(), inlineTx.FileEnd(), inlineInfo.Size(),
		)
	}
}

func TestInlineSuperblockCommitSteadyStateDoesNotAllocate(t *testing.T) {
	committer, _, pageSize := newPortableCommitter(t, 2, 0)
	defer committer.Close()
	var generation uint64
	state := StateRoot{
		StoreID: testStoreID, PageSize: uint32(pageSize),
		MaxPageSize: uint32(pageSize), NextLogicalID: 2, ChunkDocuments: 64,
	}
	if allocs := testing.AllocsPerRun(20, func() {
		generation++
		batch, err := committer.Begin(0)
		if err != nil {
			panic(err)
		}
		state.Generation = generation
		if err := batch.SetInlineSuperblock(InlineSuperblock{
			StoreID: testStoreID, Generation: generation,
			FileEnd:  testMutableStoreDataStart(uint32(pageSize)),
			PageSize: uint32(pageSize), State: state,
		}); err != nil {
			panic(err)
		}
		if err := batch.Publish(generation); err != nil {
			panic(err)
		}
		if err := committer.Wait(generation); err != nil {
			panic(err)
		}
	}); allocs != 0 {
		t.Fatalf("inline superblock commit steady allocations = %g, want 0", allocs)
	}
}

func TestInlineFreeDeltaCodecDoesNotAllocate(t *testing.T) {
	root := testInlineFreeSuperblock(7, InlineFreeDeltaCapacity+4)
	pageSize := uint64(root.PageSize)
	records := make([]FreeDelta, InlineFreeDeltaCapacity)
	for rank := range records {
		records[rank] = FreeDelta{
			Op:     FreeOpDelete,
			Extent: FreeExtent{Offset: uint64(rank+4) * pageSize},
		}
	}
	if allocs := testing.AllocsPerRun(20, func() {
		root.FreeDelta.Reset(PageRef{}, PageRef{})
		if err := root.FreeDelta.Append(records, root.PageSize, root.FileEnd); err != nil {
			panic(err)
		}
	}); allocs != 0 {
		t.Fatalf("inline free append allocations = %g, want 0", allocs)
	}

	var encoded [InlineSuperblockSize]byte
	if allocs := testing.AllocsPerRun(20, func() {
		if _, err := EncodeInlineSuperblock(encoded[:], root); err != nil {
			panic(err)
		}
		decoded, err := DecodeInlineSuperblock(encoded[:])
		if err != nil || decoded.FreeDelta.Len() != InlineFreeDeltaCapacity {
			panic("inline free codec")
		}
	}); allocs != 0 {
		t.Fatalf("inline free codec allocations = %g, want 0", allocs)
	}
}
