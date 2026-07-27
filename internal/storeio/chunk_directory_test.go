package storeio

import (
	"encoding/binary"
	"errors"
	"math/bits"
	"testing"
)

const testChunkDirectoryNextLogicalID = uint64(1000)

func testChunkDirectoryHeader(shift uint8, prefix uint32, bitmap uint64) ChunkDirectoryHeader {
	return ChunkDirectoryHeader{
		StoreID:    testStoreID,
		Generation: 11,
		LogicalID:  2,
		PageSize:   testSuperblockPageSize,
		Prefix:     prefix,
		Bitmap:     bitmap,
		Shift:      shift,
	}
}

func testChunkDirectoryRefs(header ChunkDirectoryHeader) []PageRef {
	refs := make([]PageRef, 0, bits.OnesCount64(header.Bitmap))
	kind := PageChunkDirectory
	if header.Shift == 0 {
		kind = PageDocument
	}
	for lane, rank := 0, 0; lane < 64; lane++ {
		if header.Bitmap&(uint64(1)<<lane) == 0 {
			continue
		}
		refs = append(refs, PageRef{
			Offset:     testMutableStoreDataStart(testSuperblockPageSize) + uint64(rank)*uint64(testSuperblockPageSize),
			LogicalID:  uint64(rank + 3),
			Generation: header.Generation - uint64(rank&1),
			Length:     testSuperblockPageSize,
			Kind:       kind,
		})
		rank++
	}
	return refs
}

func testChunkDirectoryFileEnd(refs []PageRef) uint64 {
	return testMutableStoreDataStart(testSuperblockPageSize) +
		uint64(len(refs))*uint64(testSuperblockPageSize)
}

func encodeTestChunkDirectory(t *testing.T, header ChunkDirectoryHeader, refs []PageRef) ([]byte, uint64) {
	t.Helper()
	page := make([]byte, testSuperblockPageSize)
	fileEnd := testChunkDirectoryFileEnd(refs)
	encoded, err := EncodeChunkDirectoryPage(page, header, refs, fileEnd, testChunkDirectoryNextLogicalID)
	if err != nil {
		t.Fatal(err)
	}
	return encoded, fileEnd
}

func TestChunkDirectorySparseLeafRoundTripAndLookup(t *testing.T) {
	const bitmap = uint64(1)<<0 | uint64(1)<<5 | uint64(1)<<63
	header := testChunkDirectoryHeader(0, 128, bitmap)
	refs := testChunkDirectoryRefs(header)
	page, fileEnd := encodeTestChunkDirectory(t, header, refs)

	view, err := OpenChunkDirectoryPage(page, fileEnd, testChunkDirectoryNextLogicalID)
	if err != nil {
		t.Fatal(err)
	}
	if got := view.Header(); got != header {
		t.Fatalf("Header = %+v, want %+v", got, header)
	}
	if got := view.Len(); got != len(refs) {
		t.Fatalf("Len = %d, want %d", got, len(refs))
	}
	for rank, lane := range []uint32{0, 5, 63} {
		chunkID := header.Prefix | lane
		got, ok := view.Lookup(chunkID)
		if !ok || got != refs[rank] {
			t.Fatalf("Lookup(%d) = (%+v,%v), want (%+v,true)", chunkID, got, ok, refs[rank])
		}
		got, ok = view.RefAt(rank)
		if !ok || got != refs[rank] {
			t.Fatalf("RefAt(%d) = (%+v,%v), want (%+v,true)", rank, got, ok, refs[rank])
		}
	}
	for _, chunkID := range []uint32{129, 134, 127, 192} {
		if got, ok := view.Lookup(chunkID); ok {
			t.Fatalf("Lookup(%d) = (%+v,true), want miss", chunkID, got)
		}
	}
	for _, rank := range []int{-1, len(refs)} {
		if got, ok := view.RefAt(rank); ok {
			t.Fatalf("RefAt(%d) = (%+v,true), want miss", rank, got)
		}
	}

	wantPayloadLength := ChunkDirectoryPayloadHeaderSize + len(refs)*PageRefSize
	common, payload, err := OpenPage(page)
	if err != nil {
		t.Fatal(err)
	}
	if common.Kind != PageChunkDirectory || len(payload) != wantPayloadLength || cap(payload) != wantPayloadLength {
		t.Fatalf("common page = (%+v,len=%d,cap=%d), want kind=%d payload=%d", common, len(payload), cap(payload), PageChunkDirectory, wantPayloadLength)
	}
}

func TestChunkDirectoryLeafAcceptsExactQuantumDocumentExtents(t *testing.T) {
	for _, pages := range []uint32{3, 5, 7} {
		header := testChunkDirectoryHeader(0, 0, 1)
		refs := testChunkDirectoryRefs(header)
		refs[0].Length = pages * testSuperblockPageSize
		fileEnd := refs[0].Offset + uint64(refs[0].Length)
		page := make([]byte, testSuperblockPageSize)
		if _, err := EncodeChunkDirectoryPage(
			page, header, refs, fileEnd, testChunkDirectoryNextLogicalID,
		); err != nil {
			t.Fatalf("%d-page EncodeChunkDirectoryPage: %v", pages, err)
		}
		view, err := OpenChunkDirectoryPage(
			page, fileEnd, testChunkDirectoryNextLogicalID,
		)
		if err != nil {
			t.Fatalf("%d-page OpenChunkDirectoryPage: %v", pages, err)
		}
		got, ok := view.Lookup(0)
		if !ok || got != refs[0] {
			t.Fatalf("%d-page Lookup = (%+v,%v), want (%+v,true)",
				pages, got, ok, refs[0])
		}
	}
}

func TestChunkDirectoryEveryRadixLevel(t *testing.T) {
	for _, test := range []struct {
		shift  uint8
		prefix uint32
		lanes  []uint8
	}{
		{0, 0x8123_4540, []uint8{0, 17, 63}},
		{6, 0x8123_4000, []uint8{0, 7, 63}},
		{12, 0x8120_0000, []uint8{1, 32, 62}},
		{18, 0x8100_0000, []uint8{0, 13, 63}},
		{24, 0x8000_0000, []uint8{0, 1, 63}},
		{30, 0, []uint8{0, 1, 3}},
	} {
		t.Run(string(rune('0'+test.shift/6)), func(t *testing.T) {
			var bitmap uint64
			for _, lane := range test.lanes {
				bitmap |= uint64(1) << lane
			}
			header := testChunkDirectoryHeader(test.shift, test.prefix, bitmap)
			refs := testChunkDirectoryRefs(header)
			page, fileEnd := encodeTestChunkDirectory(t, header, refs)
			view, err := OpenChunkDirectoryPage(page, fileEnd, testChunkDirectoryNextLogicalID)
			if err != nil {
				t.Fatal(err)
			}
			for rank, lane := range test.lanes {
				chunkID := test.prefix | uint32(lane)<<test.shift
				got, ok := view.Lookup(chunkID)
				if !ok || got != refs[rank] {
					t.Fatalf("shift=%d lane=%d = (%+v,%v), want (%+v,true)", test.shift, lane, got, ok, refs[rank])
				}
			}
		})
	}
}

func TestChunkDirectoryRejectsInvalidWrites(t *testing.T) {
	valid := testChunkDirectoryHeader(6, 0x1234_5000, uint64(1)<<0|uint64(1)<<63)
	validRefs := testChunkDirectoryRefs(valid)
	validEnd := testChunkDirectoryFileEnd(validRefs)
	for _, test := range []struct {
		name   string
		mutate func(*ChunkDirectoryHeader, *[]PageRef, *uint64, *uint64)
	}{
		{"store id", func(h *ChunkDirectoryHeader, _ *[]PageRef, _ *uint64, _ *uint64) { h.StoreID = [16]byte{} }},
		{"generation", func(h *ChunkDirectoryHeader, _ *[]PageRef, _ *uint64, _ *uint64) { h.Generation = 0 }},
		{"directory logical id", func(h *ChunkDirectoryHeader, _ *[]PageRef, _ *uint64, _ *uint64) { h.LogicalID = StateRootLogicalID }},
		{"directory future logical id", func(h *ChunkDirectoryHeader, _ *[]PageRef, _ *uint64, next *uint64) { h.LogicalID = *next }},
		{"page size", func(h *ChunkDirectoryHeader, _ *[]PageRef, _ *uint64, _ *uint64) { h.PageSize = 5000 }},
		{"flags", func(h *ChunkDirectoryHeader, _ *[]PageRef, _ *uint64, _ *uint64) { h.Flags = 1 }},
		{"empty bitmap", func(h *ChunkDirectoryHeader, refs *[]PageRef, _ *uint64, _ *uint64) { h.Bitmap = 0; *refs = nil }},
		{"count", func(_ *ChunkDirectoryHeader, refs *[]PageRef, _ *uint64, _ *uint64) { *refs = (*refs)[:1] }},
		{"shift", func(h *ChunkDirectoryHeader, _ *[]PageRef, _ *uint64, _ *uint64) { h.Shift = 7 }},
		{"prefix", func(h *ChunkDirectoryHeader, _ *[]PageRef, _ *uint64, _ *uint64) { h.Prefix++ }},
		{"high lane", func(h *ChunkDirectoryHeader, refs *[]PageRef, _ *uint64, _ *uint64) {
			h.Shift = 30
			h.Prefix = 0
			h.Bitmap = 1 << 4
			*refs = (*refs)[:1]
		}},
		{"file end", func(_ *ChunkDirectoryHeader, _ *[]PageRef, end *uint64, _ *uint64) { (*end)-- }},
		{"child kind", func(_ *ChunkDirectoryHeader, refs *[]PageRef, _ *uint64, _ *uint64) { (*refs)[0].Kind = PageDocument }},
		{"child flags", func(_ *ChunkDirectoryHeader, refs *[]PageRef, _ *uint64, _ *uint64) { (*refs)[0].Flags = 1 }},
		{"child length", func(_ *ChunkDirectoryHeader, refs *[]PageRef, _ *uint64, _ *uint64) { (*refs)[0].Length-- }},
		{"branch oversized length", func(_ *ChunkDirectoryHeader, refs *[]PageRef, end *uint64, _ *uint64) {
			(*refs)[0].Length *= 2
			*end += uint64(testSuperblockPageSize)
		}},
		{"child generation", func(h *ChunkDirectoryHeader, refs *[]PageRef, _ *uint64, _ *uint64) {
			(*refs)[0].Generation = h.Generation + 1
		}},
		{"child logical id", func(_ *ChunkDirectoryHeader, refs *[]PageRef, _ *uint64, _ *uint64) {
			(*refs)[0].LogicalID = StateRootLogicalID
		}},
		{"child future logical id", func(_ *ChunkDirectoryHeader, refs *[]PageRef, _ *uint64, next *uint64) { (*refs)[0].LogicalID = *next }},
		{"child unaligned", func(_ *ChunkDirectoryHeader, refs *[]PageRef, _ *uint64, _ *uint64) { (*refs)[0].Offset++ }},
		{"child outside", func(_ *ChunkDirectoryHeader, refs *[]PageRef, end *uint64, _ *uint64) { (*refs)[0].Offset = *end }},
		{"duplicate logical", func(_ *ChunkDirectoryHeader, refs *[]PageRef, _ *uint64, _ *uint64) {
			(*refs)[1].LogicalID = (*refs)[0].LogicalID
		}},
		{"duplicate physical", func(_ *ChunkDirectoryHeader, refs *[]PageRef, _ *uint64, _ *uint64) {
			(*refs)[1].Offset = (*refs)[0].Offset
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			header := valid
			refs := append([]PageRef(nil), validRefs...)
			fileEnd := validEnd
			nextLogicalID := testChunkDirectoryNextLogicalID
			test.mutate(&header, &refs, &fileEnd, &nextLogicalID)
			page := make([]byte, testSuperblockPageSize)
			if _, err := EncodeChunkDirectoryPage(page, header, refs, fileEnd, nextLogicalID); !errors.Is(err, ErrInvalidWrite) {
				t.Fatalf("EncodeChunkDirectoryPage = %v, want %v", err, ErrInvalidWrite)
			}
		})
	}

	leaf := testChunkDirectoryHeader(0, 0, 1)
	for _, length := range []uint32{
		testSuperblockPageSize - 1,
		testSuperblockPageSize + 1,
	} {
		refs := testChunkDirectoryRefs(leaf)
		refs[0].Length = length
		page := make([]byte, testSuperblockPageSize)
		fileEnd := refs[0].Offset + uint64(length) + uint64(testSuperblockPageSize)
		fileEnd &^= uint64(testSuperblockPageSize - 1)
		if _, err := EncodeChunkDirectoryPage(page, leaf, refs, fileEnd, testChunkDirectoryNextLogicalID); !errors.Is(err, ErrInvalidWrite) {
			t.Fatalf("leaf length %d = %v, want %v", length, err, ErrInvalidWrite)
		}
	}
	groupRefs := testChunkDirectoryRefs(leaf)
	groupRefs[0].Kind = PageDocumentGroup
	groupRefs[0].Length = 3 * testSuperblockPageSize
	groupEnd := groupRefs[0].Offset + uint64(groupRefs[0].Length)
	if _, err := EncodeChunkDirectoryPage(
		make([]byte, testSuperblockPageSize), leaf, groupRefs, groupEnd,
		testChunkDirectoryNextLogicalID,
	); !errors.Is(err, ErrInvalidWrite) {
		t.Fatalf("non-power document group = %v, want %v", err, ErrInvalidWrite)
	}
}

func TestChunkDirectoryRejectsResealedSemanticCorruption(t *testing.T) {
	header := testChunkDirectoryHeader(0, 128, uint64(1)<<0|uint64(1)<<5)
	refs := testChunkDirectoryRefs(header)
	page, fileEnd := encodeTestChunkDirectory(t, header, refs)

	for _, test := range []struct {
		name   string
		mutate func([]byte)
	}{
		{"version", func(p []byte) { binary.LittleEndian.PutUint32(p[PageHeaderSize:PageHeaderSize+4], 1) }},
		{"prefix", func(p []byte) { p[PageHeaderSize+4]++ }},
		{"bitmap", func(p []byte) { p[PageHeaderSize+8] ^= 2 }},
		{"shift", func(p []byte) { p[PageHeaderSize+16] = 1 }},
		{"count", func(p []byte) { p[PageHeaderSize+17]++ }},
		{"flags", func(p []byte) { p[PageHeaderSize+18] = 1 }},
		{"header reserved", func(p []byte) { p[PageHeaderSize+20] = 1 }},
		{"reference kind", func(p []byte) { p[PageHeaderSize+ChunkDirectoryPayloadHeaderSize+28] = byte(PageIndexDirectory) }},
		{"reference reserved", func(p []byte) { p[PageHeaderSize+ChunkDirectoryPayloadHeaderSize+30] = 1 }},
		{"duplicate logical", func(p []byte) {
			first := PageHeaderSize + ChunkDirectoryPayloadHeaderSize
			second := first + PageRefSize
			copy(p[second+8:second+16], p[first+8:first+16])
		}},
		{"duplicate physical", func(p []byte) {
			first := PageHeaderSize + ChunkDirectoryPayloadHeaderSize
			second := first + PageRefSize
			copy(p[second:second+8], p[first:first+8])
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			corrupt := append([]byte(nil), page...)
			test.mutate(corrupt)
			resealTestPage(corrupt)
			if _, err := OpenChunkDirectoryPage(corrupt, fileEnd, testChunkDirectoryNextLogicalID); !errors.Is(err, ErrChunkDirectoryCorrupt) {
				t.Fatalf("OpenChunkDirectoryPage = %v, want %v", err, ErrChunkDirectoryCorrupt)
			}
		})
	}

	for _, cut := range []int{0, PageHeaderSize - 1, len(page) - 1} {
		if _, err := OpenChunkDirectoryPage(page[:cut], fileEnd, testChunkDirectoryNextLogicalID); !errors.Is(err, ErrChunkDirectoryCorrupt) {
			t.Fatalf("cut %d = %v, want %v", cut, err, ErrChunkDirectoryCorrupt)
		}
	}
}
