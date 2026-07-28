package storeio

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

const format0PageSize = uint32(4096)

var format0StoreID = [16]byte{
	0x10, 0x21, 0x32, 0x43, 0x54, 0x65, 0x76, 0x87,
	0x98, 0xa9, 0xba, 0xcb, 0xdc, 0xed, 0xfe, 0x0f,
}

func format0EmptyState(generation uint64, pageSize uint32) StateRoot {
	return StateRoot{
		StoreID:          format0StoreID,
		Generation:       generation,
		PageSize:         pageSize,
		MaxPageSize:      64 << 10,
		NextLogicalID:    StateRootLogicalID + 1,
		ChunkDocuments:   64,
		MaxKeyBytes:      256,
		InlineValueBytes: 512,
		MaxDocumentBytes: 4 << 20,
	}
}

func format0StateRootPage(t *testing.T) []byte {
	t.Helper()
	layout, err := MutableStoreLayout(format0PageSize)
	if err != nil {
		t.Fatal(err)
	}
	page := make([]byte, format0PageSize)
	encoded, err := EncodeStateRootPage(
		page, format0EmptyState(7, format0PageSize), layout.DataStart,
	)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func format0PrimaryStateRootPage(t *testing.T) []byte {
	t.Helper()
	layout, err := MutableStoreLayout(format0PageSize)
	if err != nil {
		t.Fatal(err)
	}
	root := format0EmptyState(7, format0PageSize)
	root.NextLogicalID = PrimaryFirstDynamicLogicalID
	root.PrimaryRoot = PageRef{
		Offset:     layout.DataStart,
		LogicalID:  PrimaryCatalogRootLogicalID,
		Generation: root.Generation,
		Length:     GlobalTabletCatalogRootBytes,
		Kind:       PagePrimaryCatalog,
	}
	page := make([]byte, format0PageSize)
	encoded, err := EncodeStateRootPage(
		page, root, layout.DataStart+uint64(root.PrimaryRoot.Length),
	)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func format0InlineRoot(t *testing.T, generation uint64) []byte {
	t.Helper()
	layout, err := MutableStoreLayout(format0PageSize)
	if err != nil {
		t.Fatal(err)
	}
	root := InlineSuperblock{
		StoreID:    format0StoreID,
		Generation: generation,
		FileEnd:    layout.DataStart,
		PageSize:   format0PageSize,
		State:      format0EmptyState(generation, format0PageSize),
	}
	dst := make([]byte, InlineSuperblockSize)
	encoded, err := EncodeInlineSuperblock(dst, root)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func format0StandaloneSuperblock(t *testing.T) []byte {
	t.Helper()
	state := format0StateRootPage(t)
	layout, err := MutableStoreLayout(format0PageSize)
	if err != nil {
		t.Fatal(err)
	}
	root := Superblock{
		StoreID:       format0StoreID,
		Generation:    7,
		StateOffset:   layout.DataStart,
		StateLength:   uint32(len(state)),
		StateChecksum: PageChecksum(state),
		FileEnd:       layout.DataStart + uint64(format0PageSize),
		PageSize:      format0PageSize,
	}
	dst := make([]byte, SuperblockSize)
	encoded, err := EncodeSuperblock(dst, root)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func format0MutablePrefix(t *testing.T) []byte {
	t.Helper()
	layout, err := MutableStoreLayout(format0PageSize)
	if err != nil {
		t.Fatal(err)
	}
	prefix := make([]byte, layout.DataStart)
	copy(prefix[layout.RootOffsets[0]:], format0InlineRoot(t, 7))
	copy(prefix[layout.RootOffsets[1]:], format0InlineRoot(t, 8))
	return prefix
}

func format0DocumentPage(t *testing.T) []byte {
	t.Helper()
	header := DocumentPageHeader{
		StoreID:    format0StoreID,
		Generation: 7,
		LogicalID:  2,
		PageSize:   format0PageSize,
		ChunkID:    3,
		Live:       uint64(1) << 5,
	}
	rows := []DocumentRecord{{
		Slot: 5,
		Key:  []byte("format0:key"),
		JSON: []byte(`{"format":0,"live":true}`),
	}}
	encoded, err := EncodeDocumentPage(
		make([]byte, format0PageSize), header, rows, 32,
	)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func format0FingerprintPage(t *testing.T) []byte {
	t.Helper()
	const hash = uint64(0x0123_4567_89ab_cdef)
	header := PageKeyDirectoryHeader{
		StoreID:    format0StoreID,
		Generation: 7,
		LogicalID:  3,
		PageSize:   format0PageSize,
		MinHash:    hash,
		MaxHash:    hash,
	}
	entries := []PageKeyLocation{{Hash: hash, Chunk: 3, Slot: 5}}
	encoded, err := EncodePageFingerprintLeaf(
		make([]byte, format0PageSize), header, entries,
		64*uint64(format0PageSize), 32, 4, 64,
	)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func format0PostingPage(t *testing.T) []byte {
	t.Helper()
	header := PostingPageHeader{
		StoreID:    format0StoreID,
		Generation: 7,
		LogicalID:  4,
		PageSize:   format0PageSize,
		IndexID:    1,
	}
	segments := []PostingSegment{{
		StreamID:    9,
		TupleHash:   0xfedc_ba98_7654_3210,
		Certificate: []byte(`"format0"`),
		Entries: []PostingEntry{
			{Chunk: 3, Bits: uint64(1) << 5},
			{Chunk: 5, Bits: uint64(1)<<1 | uint64(1)<<7},
		},
	}}
	encoded, err := EncodePostingPage(
		make([]byte, format0PageSize), header, segments, 32, 2,
	)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func format0MaterializationMaxPatches(t *testing.T) []byte {
	t.Helper()
	const pageSize = uint32(8192)
	layout, err := MutableStoreLayout(pageSize)
	if err != nil {
		t.Fatal(err)
	}
	header := MaterializationJournalHeader{
		StoreID:          format0StoreID,
		Sequence:         17,
		TargetGeneration: 8,
		PageSize:         pageSize,
		SectorSize:       MaterializationJournalMinSectorSize,
	}
	ref := PageRef{
		Offset:     layout.DataStart,
		LogicalID:  5,
		Generation: 7,
		Length:     pageSize,
		Kind:       PageDocument,
	}
	before := make([]byte, pageSize)
	payload, err := InitPage(before, PageHeader{
		StoreID:       format0StoreID,
		Generation:    ref.Generation,
		LogicalID:     ref.LogicalID,
		PageSize:      ref.Length,
		PayloadLength: ref.Length - PageHeaderSize - PageTrailerSize,
		Kind:          ref.Kind,
	})
	if err != nil {
		t.Fatal(err)
	}
	clear(payload)
	if _, err := SealPage(before); err != nil {
		t.Fatal(err)
	}
	after := append([]byte(nil), before...)
	offsets := [...]uint32{0, 1024, 2048, 3072, 4096, 5120, 7680}
	patches := make([]MaterializationPatch, len(offsets))
	for rank, offset := range offsets {
		patches[rank] = MaterializationPatch{
			Offset: offset,
			Data:   before[offset : offset+MaterializationJournalMinSectorSize],
		}
		if offset != 7680 {
			at := int(offset) + 64 + rank
			after[at] ^= byte(0x41 + rank)
		}
	}
	if _, err := SealPage(after); err != nil {
		t.Fatal(err)
	}
	target, err := BuildMaterializationTarget(
		header, ref, before, after, patches, 0,
	)
	if err != nil {
		t.Fatal(err)
	}
	dst := make([]byte, MaterializationJournalSize)
	encoded, err := EncodeMaterializationJournal(
		dst, header, []MaterializationTarget{target}, patches,
	)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func format0MaterializationMaxTargets(t *testing.T) []byte {
	t.Helper()
	layout, err := MutableStoreLayout(format0PageSize)
	if err != nil {
		t.Fatal(err)
	}
	header := MaterializationJournalHeader{
		StoreID:          format0StoreID,
		Sequence:         18,
		TargetGeneration: 8,
		PageSize:         format0PageSize,
		SectorSize:       MaterializationJournalMinSectorSize,
	}
	targets := make([]MaterializationTarget, MaterializationJournalMaxTargets)
	patches := make([]MaterializationPatch, MaterializationJournalMaxTargets)
	for rank := range targets {
		ref := PageRef{
			Offset:    layout.DataStart + uint64(rank)*uint64(format0PageSize),
			LogicalID: uint64(10 + rank), Generation: 7,
			Length: format0PageSize, Kind: PageDocument,
		}
		before := make([]byte, format0PageSize)
		payload, initErr := InitPage(before, PageHeader{
			StoreID: format0StoreID, Generation: ref.Generation,
			LogicalID: ref.LogicalID, PageSize: ref.Length,
			PayloadLength: 128, Kind: ref.Kind,
		})
		if initErr != nil {
			t.Fatal(initErr)
		}
		clear(payload)
		if _, sealErr := SealPage(before); sealErr != nil {
			t.Fatal(sealErr)
		}
		patches[rank] = MaterializationPatch{
			Target: uint16(rank),
			Offset: 0,
			Data:   before[:MaterializationJournalMinSectorSize],
		}
		target, buildErr := BuildMaterializationTarget(
			header, ref, before, before, patches[rank:rank+1], uint16(rank),
		)
		if buildErr != nil {
			t.Fatal(buildErr)
		}
		targets[rank] = target
	}
	dst := make([]byte, MaterializationJournalSize)
	encoded, err := EncodeMaterializationJournal(dst, header, targets, patches)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func readFormat0Golden(t *testing.T, name string) []byte {
	t.Helper()
	path := filepath.Join("testdata", "format0", name+".hex")
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	var result []byte
	var occupied []bool
	scanner := bufio.NewScanner(file)
	for lineNumber := 1; scanner.Scan(); lineNumber++ {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[0] == "size" {
			size, sizeErr := strconv.Atoi(fields[1])
			if sizeErr != nil || size <= 0 || result != nil {
				t.Fatalf("%s:%d: invalid size", path, lineNumber)
			}
			result = make([]byte, size)
			occupied = make([]bool, size)
			continue
		}
		if len(fields) != 2 || result == nil {
			t.Fatalf("%s:%d: expected OFFSET HEX", path, lineNumber)
		}
		offset, offsetErr := strconv.ParseUint(fields[0], 16, 64)
		chunk, decodeErr := hex.DecodeString(fields[1])
		if offsetErr != nil || decodeErr != nil || len(chunk) == 0 ||
			offset > uint64(len(result)) || uint64(len(chunk)) > uint64(len(result))-offset {
			t.Fatalf("%s:%d: invalid sparse chunk", path, lineNumber)
		}
		for index := range chunk {
			at := int(offset) + index
			if occupied[at] {
				t.Fatalf("%s:%d: overlapping sparse chunk", path, lineNumber)
			}
			occupied[at] = true
		}
		copy(result[int(offset):], chunk)
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if result == nil {
		t.Fatalf("%s: missing size", path)
	}
	return result
}

func compareFormat0Golden(t *testing.T, name string, got []byte) {
	t.Helper()
	want := readFormat0Golden(t, name)
	if bytes.Equal(got, want) {
		return
	}
	limit := min(len(got), len(want))
	at := limit
	for index := range limit {
		if got[index] != want[index] {
			at = index
			break
		}
	}
	t.Fatalf(
		"%s differs at byte %#x: got length=%d want length=%d",
		name, at, len(got), len(want),
	)
}

func sparseFormat0Golden(image []byte) string {
	var output strings.Builder
	fmt.Fprintf(&output, "# storeio development format 0; unspecified bytes are zero\nsize %d\n", len(image))
	for start := 0; start < len(image); start += 32 {
		end := min(start+32, len(image))
		if allZero(image[start:end]) {
			continue
		}
		fmt.Fprintf(&output, "%08x %x\n", start, image[start:end])
	}
	return output.String()
}

func TestFormat0PrintGolden(t *testing.T) {
	name := os.Getenv("STOREIO_FORMAT0_GOLDEN")
	builders := map[string]func(*testing.T) []byte{
		"mutable_prefix_4k":           format0MutablePrefix,
		"empty_state_root_page":       format0StateRootPage,
		"primary_state_root_page":     format0PrimaryStateRootPage,
		"empty_inline_superblock":     func(t *testing.T) []byte { return format0InlineRoot(t, 7) },
		"standalone_superblock":       format0StandaloneSuperblock,
		"document_page":               format0DocumentPage,
		"fingerprint_leaf":            format0FingerprintPage,
		"posting_page":                format0PostingPage,
		"materialization_max_patches": format0MaterializationMaxPatches,
		"materialization_max_targets": format0MaterializationMaxTargets,
	}
	build, ok := builders[name]
	if !ok {
		t.Skip("set STOREIO_FORMAT0_GOLDEN to one fixture name")
	}
	fmt.Print(sparseFormat0Golden(build(t)))
}

func assertFormat0Zero(t *testing.T, image []byte, start, end int) {
	t.Helper()
	if start < 0 || end < start || end > len(image) || !allZero(image[start:end]) {
		t.Fatalf("bytes [%#x:%#x) are not reserved zero", start, end)
	}
}

func assertFormat0Checksum(t *testing.T, image []byte, trailer int) {
	t.Helper()
	checksum := binary.LittleEndian.Uint32(image[trailer : trailer+4])
	complement := binary.LittleEndian.Uint32(image[trailer+4 : trailer+8])
	if complement != ^checksum || PageChecksum(image[:trailer]) != checksum {
		t.Fatalf(
			"trailer %#x checksum/complement = %08x/%08x",
			trailer, checksum, complement,
		)
	}
}

func resealFormat0Page(image []byte) {
	trailer := len(image) - PageTrailerSize
	checksum := PageChecksum(image[:trailer])
	binary.LittleEndian.PutUint32(image[trailer:trailer+4], checksum)
	binary.LittleEndian.PutUint32(image[trailer+4:], ^checksum)
}

func resealFormat0Inline(image []byte) {
	checksum := PageChecksum(image[:inlineSuperblockChecksumFrom])
	binary.LittleEndian.PutUint32(
		image[inlineSuperblockChecksumFrom:inlineSuperblockChecksumFrom+4], checksum,
	)
	binary.LittleEndian.PutUint32(
		image[inlineSuperblockChecksumFrom+4:InlineSuperblockSize], ^checksum,
	)
}

func resealFormat0Journal(image []byte) {
	checksum := PageChecksum(image[:materializationTrailerAt])
	binary.LittleEndian.PutUint32(
		image[materializationTrailerAt:materializationTrailerAt+4], checksum,
	)
	binary.LittleEndian.PutUint32(
		image[materializationTrailerAt+4:], ^checksum,
	)
}

func TestFormat0LayoutConstantsAndKinds(t *testing.T) {
	for name, gotWant := range map[string][2]int{
		"PageHeaderSize":                    {PageHeaderSize, 64},
		"PageTrailerSize":                   {PageTrailerSize, 8},
		"StateRootPayloadSize":              {StateRootPayloadSize, 512},
		"stateRootMaxPageSizeOffset":        {stateRootMaxPageSizeOffset, 220},
		"stateRootPageCatalogOffset":        {stateRootPageCatalogOffset, 224},
		"stateRootPageCatalogEnd":           {stateRootPageCatalogEnd, 256},
		"stateRootPageCatalogDigestEnd":     {stateRootPageCatalogDigestEnd, 272},
		"stateRootPageCatalogBytesEnd":      {stateRootPageCatalogBytesEnd, 276},
		"stateRootMaxKeyBytesEnd":           {stateRootMaxKeyBytesEnd, 280},
		"stateRootInlineValueBytesEnd":      {stateRootInlineValueBytesEnd, 284},
		"stateRootMaxDocumentBytesEnd":      {stateRootMaxDocumentBytesEnd, 288},
		"stateRootPrimaryOffset":            {stateRootPrimaryOffset, 288},
		"stateRootPrimaryEnd":               {stateRootPrimaryEnd, 320},
		"stateRootJournalIDOffset":          {stateRootJournalIDOffset, 320},
		"stateRootJournalIDEnd":             {stateRootJournalIDEnd, 336},
		"stateRootReservedOffset":           {stateRootReservedOffset, 336},
		"PageRefSize":                       {PageRefSize, 32},
		"InlineSuperblockSize":              {InlineSuperblockSize, 4096},
		"InlineFreeDeltaCapacity":           {InlineFreeDeltaCapacity, 106},
		"SuperblockSize":                    {SuperblockSize, 128},
		"MaterializationJournalSize":        {MaterializationJournalSize, 4096},
		"MaterializationJournalHeaderSize":  {MaterializationJournalHeaderSize, 112},
		"MaterializationTargetRecordSize":   {MaterializationTargetRecordSize, 56},
		"MaterializationPatchRecordSize":    {MaterializationPatchRecordSize, 16},
		"MaterializationJournalMaxTargets":  {MaterializationJournalMaxTargets, 6},
		"MaterializationJournalMaxPatches":  {MaterializationJournalMaxPatches, 7},
		"MaterializationJournalMaxData":     {MaterializationJournalMaxData, 3584},
		"DocumentPagePayloadHeaderSize":     {DocumentPagePayloadHeaderSize, 32},
		"DocumentPageRecordSize":            {DocumentPageRecordSize, 8},
		"PageKeyDirectoryPayloadHeaderSize": {PageKeyDirectoryPayloadHeaderSize, 64},
		"PageKeyLeafEntrySize":              {PageKeyLeafEntrySize, 16},
		"PageKeyBranchEntrySize":            {PageKeyBranchEntrySize, 40},
		"PostingPagePayloadHeaderSize":      {PostingPagePayloadHeaderSize, 32},
		"PostingSegmentHeaderSize":          {PostingSegmentHeaderSize, 48},
	} {
		if gotWant[0] != gotWant[1] {
			t.Errorf("%s = %d, want format-0 value %d", name, gotWant[0], gotWant[1])
		}
	}
	if DevelopmentFormatVersion != 0 || StateRootLogicalID != 1 ||
		MaterializationJournalMinSectorSize != 512 {
		t.Fatalf(
			"format identity = version %d state logical %d sector %d",
			DevelopmentFormatVersion, StateRootLogicalID,
			MaterializationJournalMinSectorSize,
		)
	}
	kinds := [...]PageKind{
		PageStateRoot,
		PageDocument,
		PageOverflow,
		PageChunkDirectory,
		PageKeyDirectory,
		PageIndexDirectory,
		PageIndexPosting,
		PageDocumentGroup,
		PageFloat64Group,
		PageFloat64Catalog,
		PageFloat64Stripe,
		PageIndexGroupCatalog,
		PageFreeImage,
		PageFreeDelta,
		PageFreeIndex,
		PageFingerprintDirectory,
		PageCatalogSegment,
		PagePrimaryCatalog,
		PageTabletDirectory,
		PagePrimaryLocator,
		PageTabletRoute,
		PagePrimaryAnchor,
		PagePrimaryLeaf,
	}
	for index, kind := range kinds {
		if want := PageKind(index + 1); kind != want {
			t.Fatalf("PageKind rank %d = %d, want %d", index, kind, want)
		}
	}

	for _, fixture := range []struct {
		pageSize uint32
		roots    [2]uint64
		journals [2]uint64
		data     uint64
	}{
		{4096, [2]uint64{0, 4096}, [2]uint64{8192, 12288}, 16384},
		{8192, [2]uint64{0, 8192}, [2]uint64{16384, 20480}, 24576},
		{65536, [2]uint64{0, 65536}, [2]uint64{131072, 135168}, 196608},
	} {
		layout, err := MutableStoreLayout(fixture.pageSize)
		if err != nil {
			t.Fatal(err)
		}
		if layout.RootOffsets != fixture.roots ||
			layout.MaterializationJournalOffsets != fixture.journals ||
			layout.DataStart != fixture.data {
			t.Fatalf("MutableStoreLayout(%d) = %+v", fixture.pageSize, layout)
		}
	}
}

func TestFormat0GoldenMutablePrefix(t *testing.T) {
	prefix := format0MutablePrefix(t)
	compareFormat0Golden(t, "mutable_prefix_4k", prefix)
	layout, _ := MutableStoreLayout(format0PageSize)
	if uint64(len(prefix)) != layout.DataStart {
		t.Fatalf("prefix length = %d, want %d", len(prefix), layout.DataStart)
	}
	for rank, offset := range layout.RootOffsets {
		root, err := DecodeInlineSuperblock(prefix[offset : offset+InlineSuperblockSize])
		if err != nil || root.Generation != uint64(rank+7) {
			t.Fatalf("root %d = (%+v,%v)", rank, root, err)
		}
	}
	for _, offset := range layout.MaterializationJournalOffsets {
		slot := prefix[offset : offset+MaterializationJournalSize]
		if !allZero(slot) {
			t.Fatalf("empty journal at %#x is non-zero", offset)
		}
		if _, err := OpenMaterializationJournal(slot); !errors.Is(
			err, ErrMaterializationJournalNotFound,
		) {
			t.Fatalf("empty journal at %#x = %v", offset, err)
		}
	}
}

func TestFormat0GoldenEmptyStateAndRoots(t *testing.T) {
	statePage := format0StateRootPage(t)
	compareFormat0Golden(t, "empty_state_root_page", statePage)
	layout, _ := MutableStoreLayout(format0PageSize)
	state, err := DecodeStateRootPage(statePage, layout.DataStart)
	if err != nil || state != format0EmptyState(7, format0PageSize) {
		t.Fatalf("state root = (%+v,%v)", state, err)
	}
	if string(statePage[0:8]) != pageMagic ||
		binary.LittleEndian.Uint16(statePage[8:10]) != pageVersion ||
		binary.LittleEndian.Uint16(statePage[10:12]) != PageHeaderSize ||
		PageKind(statePage[12]) != PageStateRoot {
		t.Fatal("standalone StateRoot common-page offsets changed")
	}
	assertFormat0Zero(t, statePage, 14, 16)
	assertFormat0Zero(t, statePage, 56, 64)
	assertFormat0Zero(
		t, statePage,
		PageHeaderSize+stateRootReservedOffset,
		PageHeaderSize+StateRootPayloadSize,
	)
	assertFormat0Zero(
		t, statePage,
		PageHeaderSize+StateRootPayloadSize,
		len(statePage)-PageTrailerSize,
	)
	assertFormat0Checksum(t, statePage, len(statePage)-PageTrailerSize)

	primaryStatePage := format0PrimaryStateRootPage(t)
	compareFormat0Golden(t, "primary_state_root_page", primaryStatePage)
	primaryFileEnd := layout.DataStart + uint64(GlobalTabletCatalogRootBytes)
	primaryState, err := DecodeStateRootPage(primaryStatePage, primaryFileEnd)
	if err != nil || primaryState.PrimaryRoot == (PageRef{}) {
		t.Fatalf("primary state root = (%+v,%v)", primaryState, err)
	}
	assertFormat0Zero(
		t, primaryStatePage,
		PageHeaderSize+stateRootPrimaryEnd,
		PageHeaderSize+StateRootPayloadSize,
	)
	assertFormat0Checksum(
		t, primaryStatePage, len(primaryStatePage)-PageTrailerSize,
	)

	inline := format0InlineRoot(t, 7)
	compareFormat0Golden(t, "empty_inline_superblock", inline)
	decoded, err := DecodeInlineSuperblock(inline)
	if err != nil || decoded.State != state {
		t.Fatalf("inline root = (%+v,%v)", decoded, err)
	}
	if string(inline[0:8]) != inlineSuperblockMagic ||
		binary.LittleEndian.Uint32(inline[8:12]) != DevelopmentFormatVersion ||
		binary.LittleEndian.Uint32(inline[12:16]) != InlineSuperblockSize ||
		binary.LittleEndian.Uint64(inline[32:40]) != ^uint64(7) ||
		!bytes.Equal(inline[72:88], format0StoreID[:]) {
		t.Fatal("inline root fixed offsets changed")
	}
	assertFormat0Zero(t, inline, 48, 72)
	assertFormat0Zero(t, inline, 88, inlineSuperblockStateOffset)
	assertFormat0Zero(
		t, inline,
		inlineSuperblockStateOffset+stateRootReservedOffset,
		inlineSuperblockStateEnd,
	)
	assertFormat0Zero(t, inline, inlineFreeDeltaCountOffset+2, inlineFreeDeltaPrevOffset)
	assertFormat0Zero(t, inline, inlineFreeDeltaRecordsOffset, inlineSuperblockChecksumFrom)
	assertFormat0Checksum(t, inline, inlineSuperblockChecksumFrom)

	superblock := format0StandaloneSuperblock(t)
	compareFormat0Golden(t, "standalone_superblock", superblock)
	if _, err := DecodeSuperblock(superblock); err != nil {
		t.Fatal(err)
	}
	assertFormat0Zero(t, superblock, 60, 64)
	assertFormat0Zero(t, superblock, 92, 96)
	assertFormat0Zero(t, superblock, 112, 120)
	assertFormat0Checksum(t, superblock, 120)
}

func TestFormat0GoldenRepresentativePageKinds(t *testing.T) {
	fixtures := []struct {
		name string
		kind PageKind
		page func(*testing.T) []byte
		open func([]byte) error
	}{
		{
			name: "document_page", kind: PageDocument, page: format0DocumentPage,
			open: func(page []byte) error {
				_, err := OpenDocumentPage(page, 4, 32)
				return err
			},
		},
		{
			name: "fingerprint_leaf", kind: PageFingerprintDirectory,
			page: format0FingerprintPage,
			open: func(page []byte) error {
				_, err := OpenPageFingerprintDirectory(
					page, 64*uint64(format0PageSize), 32, 4, 64,
				)
				return err
			},
		},
		{
			name: "posting_page", kind: PageIndexPosting, page: format0PostingPage,
			open: func(page []byte) error {
				_, err := OpenPostingPage(page, 32, 2)
				return err
			},
		},
	}
	for _, fixture := range fixtures {
		t.Run(fixture.name, func(t *testing.T) {
			page := fixture.page(t)
			compareFormat0Golden(t, fixture.name, page)
			header, _, err := OpenPage(page)
			if err != nil || header.Kind != fixture.kind {
				t.Fatalf("common page = (%+v,%v), want kind %d", header, err, fixture.kind)
			}
			if err := fixture.open(page); err != nil {
				t.Fatal(err)
			}
			assertFormat0Zero(t, page, 14, 16)
			assertFormat0Zero(t, page, 56, 64)
			assertFormat0Checksum(t, page, len(page)-PageTrailerSize)
		})
	}
}

func TestFormat0GoldenMaterializationJournals(t *testing.T) {
	maxPatches := format0MaterializationMaxPatches(t)
	compareFormat0Golden(t, "materialization_max_patches", maxPatches)
	view, err := OpenMaterializationJournal(maxPatches)
	if err != nil {
		t.Fatal(err)
	}
	if view.Len() != 1 ||
		view.PatchLen() != MaterializationJournalMaxPatches ||
		view.DataLen() != MaterializationJournalMaxData {
		t.Fatalf(
			"max-patch journal = targets %d patches %d data %d",
			view.Len(), view.PatchLen(), view.DataLen(),
		)
	}
	if got := binary.LittleEndian.Uint32(maxPatches[88:92]); got != MaterializationJournalHeaderSize {
		t.Fatalf("target table offset = %d", got)
	}
	wantPatchOffset := MaterializationJournalHeaderSize + MaterializationTargetRecordSize
	wantDataOffset := wantPatchOffset +
		MaterializationJournalMaxPatches*MaterializationPatchRecordSize
	if got := binary.LittleEndian.Uint32(maxPatches[92:96]); got != uint32(wantPatchOffset) {
		t.Fatalf("patch table offset = %d, want %d", got, wantPatchOffset)
	}
	if got := binary.LittleEndian.Uint32(maxPatches[96:100]); got != uint32(wantDataOffset) {
		t.Fatalf("data offset = %d, want %d", got, wantDataOffset)
	}
	for rank := 0; rank < view.PatchLen(); rank++ {
		patch, ok := view.PatchAt(rank)
		if !ok || len(patch.Data) != MaterializationJournalMinSectorSize {
			t.Fatalf("patch %d = (%+v,%v)", rank, patch, ok)
		}
		record := wantPatchOffset + rank*MaterializationPatchRecordSize
		assertFormat0Zero(t, maxPatches, record+12, record+16)
	}
	dataEnd := wantDataOffset + view.DataLen()
	assertFormat0Zero(t, maxPatches, dataEnd, materializationTrailerAt)
	assertFormat0Checksum(t, maxPatches, materializationTrailerAt)

	maxTargets := format0MaterializationMaxTargets(t)
	compareFormat0Golden(t, "materialization_max_targets", maxTargets)
	targetView, err := OpenMaterializationJournal(maxTargets)
	if err != nil {
		t.Fatal(err)
	}
	if targetView.Len() != MaterializationJournalMaxTargets ||
		targetView.PatchLen() != MaterializationJournalMaxTargets ||
		targetView.DataLen() != MaterializationJournalMaxTargets*
			MaterializationJournalMinSectorSize {
		t.Fatalf(
			"max-target journal = targets %d patches %d data %d",
			targetView.Len(), targetView.PatchLen(), targetView.DataLen(),
		)
	}
	assertFormat0Checksum(t, maxTargets, materializationTrailerAt)
}

func TestFormat0GoldensRejectCorruptionAndPriorPageVersion(t *testing.T) {
	state := append([]byte(nil), readFormat0Golden(t, "empty_state_root_page")...)
	state[PageHeaderSize+stateRootReservedOffset] ^= 1
	if _, err := SealPage(state); err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeStateRootPage(
		state, testMutableStoreDataStart(format0PageSize),
	); !errors.Is(err, ErrStateRootCorrupt) {
		t.Fatalf("resealed StateRoot reserve = %v", err)
	}

	for _, prior := range []uint16{2, 3} {
		state = append([]byte(nil), readFormat0Golden(t, "empty_state_root_page")...)
		binary.LittleEndian.PutUint16(state[8:10], prior)
		resealFormat0Page(state)
		if _, err := DecodeStateRootPage(
			state, testMutableStoreDataStart(format0PageSize),
		); !errors.Is(err, ErrStateRootCorrupt) {
			t.Fatalf("resealed common-page version %d = %v", prior, err)
		}
	}

	inline := append([]byte(nil), readFormat0Golden(t, "empty_inline_superblock")...)
	inline[48] ^= 1
	resealFormat0Inline(inline)
	if _, err := DecodeInlineSuperblock(inline); !errors.Is(err, ErrSuperblockCorrupt) {
		t.Fatalf("resealed inline reserve = %v", err)
	}
	inline = append([]byte(nil), readFormat0Golden(t, "empty_inline_superblock")...)
	inline[8] ^= 1
	resealFormat0Inline(inline)
	if _, err := DecodeInlineSuperblock(inline); !errors.Is(err, ErrSuperblockCorrupt) {
		t.Fatalf("resealed inline version = %v", err)
	}
	inline = append([]byte(nil), readFormat0Golden(t, "empty_inline_superblock")...)
	inline[inlineSuperblockStateOffset+stateRootReservedOffset] ^= 1
	resealFormat0Inline(inline)
	if _, err := DecodeInlineSuperblock(inline); !errors.Is(err, ErrSuperblockCorrupt) {
		t.Fatalf("resealed embedded-state reserve = %v", err)
	}
	inline = append([]byte(nil), readFormat0Golden(t, "empty_inline_superblock")...)
	inline[inlineFreeDeltaCountOffset+2] ^= 1
	resealFormat0Inline(inline)
	if _, err := DecodeInlineSuperblock(inline); !errors.Is(err, ErrSuperblockCorrupt) {
		t.Fatalf("resealed free header reserve = %v", err)
	}
	inline = append([]byte(nil), readFormat0Golden(t, "empty_inline_superblock")...)
	inline[32] ^= 1
	resealFormat0Inline(inline)
	if _, err := DecodeInlineSuperblock(inline); !errors.Is(err, ErrSuperblockCorrupt) {
		t.Fatalf("resealed generation complement = %v", err)
	}
	inline = append([]byte(nil), readFormat0Golden(t, "empty_inline_superblock")...)
	inline[inlineSuperblockChecksumFrom] ^= 1
	if _, err := DecodeInlineSuperblock(inline); !errors.Is(err, ErrSuperblockCorrupt) {
		t.Fatalf("inline checksum bit = %v", err)
	}
	inline = append([]byte(nil), readFormat0Golden(t, "empty_inline_superblock")...)
	inline[inlineSuperblockChecksumFrom+4] ^= 1
	if _, err := DecodeInlineSuperblock(inline); !errors.Is(err, ErrSuperblockCorrupt) {
		t.Fatalf("inline checksum complement bit = %v", err)
	}

	journal := append([]byte(nil), readFormat0Golden(t, "materialization_max_patches")...)
	journal[8] ^= 1
	resealFormat0Journal(journal)
	if _, err := OpenMaterializationJournal(journal); !errors.Is(
		err, ErrMaterializationJournalCorrupt,
	) {
		t.Fatalf("resealed journal version = %v", err)
	}
	journal = append([]byte(nil), readFormat0Golden(t, "materialization_max_patches")...)
	journal[24] ^= 1
	resealFormat0Journal(journal)
	if _, err := OpenMaterializationJournal(journal); !errors.Is(
		err, ErrMaterializationJournalCorrupt,
	) {
		t.Fatalf("resealed commit complement = %v", err)
	}
	journal = append([]byte(nil), readFormat0Golden(t, "materialization_max_patches")...)
	patchOffset := int(binary.LittleEndian.Uint32(journal[92:96]))
	journal[patchOffset+12] ^= 1
	resealFormat0Journal(journal)
	if _, err := OpenMaterializationJournal(journal); !errors.Is(
		err, ErrMaterializationJournalCorrupt,
	) {
		t.Fatalf("resealed patch reserve = %v", err)
	}
	journal = append([]byte(nil), readFormat0Golden(t, "materialization_max_patches")...)
	dataEnd := int(binary.LittleEndian.Uint32(journal[96:100]) +
		binary.LittleEndian.Uint32(journal[100:104]))
	journal[dataEnd] ^= 1
	resealFormat0Journal(journal)
	if _, err := OpenMaterializationJournal(journal); !errors.Is(
		err, ErrMaterializationJournalCorrupt,
	) {
		t.Fatalf("resealed journal tail = %v", err)
	}
	journal = append([]byte(nil), readFormat0Golden(t, "materialization_max_patches")...)
	journal[materializationTrailerAt] ^= 1
	if _, err := OpenMaterializationJournal(journal); !errors.Is(
		err, ErrMaterializationJournalCorrupt,
	) {
		t.Fatalf("journal checksum bit = %v", err)
	}

	// In the immediately superseded layout common-page version 2, durable kind
	// 7 meant PageTTLDirectory. In format 0's current version 3 it means
	// PageIndexPosting. A checksum-valid legacy envelope must therefore fail
	// before its kind can be dispatched to the posting decoder.
	removedTTL := append([]byte(nil), readFormat0Golden(t, "posting_page")...)
	binary.LittleEndian.PutUint16(removedTTL[8:10], 2)
	resealFormat0Page(removedTTL)
	if PageKind(removedTTL[12]) != PageIndexPosting {
		t.Fatalf("removed-TTL test kind = %d, want current posting kind", removedTTL[12])
	}
	if _, _, err := OpenPage(removedTTL); !errors.Is(err, ErrPageCorrupt) {
		t.Fatalf("version-2 removed-TTL numbering = %v", err)
	}
	if _, err := OpenPostingPage(removedTTL, 32, 2); !errors.Is(
		err, ErrPostingPageCorrupt,
	) {
		t.Fatalf("posting decoder accepted removed-TTL numbering: %v", err)
	}
}
