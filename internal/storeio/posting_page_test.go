package storeio

import (
	"encoding/binary"
	"errors"
	"testing"
)

const (
	testPostingNextLogicalID  = uint64(10000)
	testPostingIndexHighWater = uint32(2)
)

func testPostingHeader() PostingPageHeader {
	return PostingPageHeader{
		StoreID:    testStoreID,
		Generation: 11,
		LogicalID:  2,
		PageSize:   testSuperblockPageSize,
		IndexID:    1,
	}
}

func testPostingSegments() []PostingSegment {
	return []PostingSegment{
		{
			StreamID:  7,
			TupleHash: 0x1234_5678_9abc_def0,
			Next:      PostingLink{LogicalID: 3, Segment: 7},
			Entries: []PostingEntry{
				{Chunk: 10, Bits: 1 << 3},
				{Chunk: 12, Bits: uint64(1)<<5 | uint64(1)<<7},
				{Chunk: 1000, Bits: uint64(1) << 63},
			},
		},
		{
			StreamID: 11, TupleHash: 0x55, Certificate: []byte(`"cat"`),
			Entries: []PostingEntry{{Chunk: 21, Bits: 1}},
		},
		{
			StreamID: 99, TupleHash: 0xaa, Flags: PostingSegmentCollision,
			Certificate: []byte(`"first"`), Entries: []PostingEntry{{Chunk: 31, Bits: 3}},
		},
	}
}

func clonePostingSegments(src []PostingSegment) []PostingSegment {
	dst := append([]PostingSegment(nil), src...)
	for i := range dst {
		dst[i].Entries = append([]PostingEntry(nil), src[i].Entries...)
		dst[i].Certificate = append([]byte(nil), src[i].Certificate...)
	}
	return dst
}

func encodeTestPostingPage(t *testing.T, header PostingPageHeader, segments []PostingSegment) []byte {
	t.Helper()
	page := make([]byte, testSuperblockPageSize)
	encoded, err := EncodePostingPage(page, header, segments, testPostingNextLogicalID, testPostingIndexHighWater)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func TestPostingPagePackedStreamsRoundTripAndLookup(t *testing.T) {
	header := testPostingHeader()
	segments := testPostingSegments()
	page := encodeTestPostingPage(t, header, segments)
	view, err := OpenPostingPage(page, testPostingNextLogicalID, testPostingIndexHighWater)
	if err != nil {
		t.Fatal(err)
	}
	if got := view.Header(); got != header {
		t.Fatalf("Header = %+v, want %+v", got, header)
	}
	if got := view.Len(); got != len(segments) {
		t.Fatalf("Len = %d, want %d", got, len(segments))
	}
	for i, want := range segments {
		segment, ok := view.Lookup(want.StreamID)
		if !ok {
			t.Fatalf("Lookup(%d) missed", want.StreamID)
		}
		at, ok := view.SegmentAt(i)
		if !ok || at.Header() != segment.Header() {
			t.Fatalf("SegmentAt(%d) mismatch", i)
		}
		gotHeader := segment.Header()
		if gotHeader.StreamID != want.StreamID || gotHeader.TupleHash != want.TupleHash ||
			gotHeader.Next != want.Next || gotHeader.Flags != want.Flags ||
			segment.Len() != len(want.Entries) ||
			string(segment.Certificate()) != string(want.Certificate) {
			t.Fatalf("segment %d header = %+v, want stream/hash/next/count from %+v", i, gotHeader, want)
		}
		it := segment.Iterator()
		for entryIndex, wantEntry := range want.Entries {
			got, next := it.Next()
			if !next || got != wantEntry {
				t.Fatalf("segment %d Next %d = (%+v,%v), want (%+v,true)", i, entryIndex, got, next, wantEntry)
			}
		}
		if got, next := it.Next(); next {
			t.Fatalf("segment %d tail Next = (%+v,true), want exhausted", i, got)
		}
	}
	for _, streamID := range []uint32{0, 8, 100} {
		if _, ok := view.Lookup(streamID); ok {
			t.Fatalf("Lookup(%d) unexpectedly hit", streamID)
		}
	}
	if _, ok := view.SegmentAt(-1); ok {
		t.Fatal("SegmentAt(-1) unexpectedly hit")
	}
	if _, ok := view.SegmentAt(len(segments)); ok {
		t.Fatal("SegmentAt(end) unexpectedly hit")
	}

	encodedBytes := 0
	for _, segment := range segments {
		size, sizeErr := PostingEntriesEncodedSize(segment.Entries)
		if sizeErr != nil {
			t.Fatal(sizeErr)
		}
		encodedBytes += size + len(segment.Certificate)
	}
	_, payload, err := OpenPage(page)
	if err != nil {
		t.Fatal(err)
	}
	wantPayload := PostingPagePayloadHeaderSize + len(segments)*PostingSegmentHeaderSize + encodedBytes
	if len(payload) != wantPayload || cap(payload) != len(payload) {
		t.Fatalf("payload = %d/%d, want %d/%d", len(payload), cap(payload), wantPayload, wantPayload)
	}
}

func TestPostingPagePrefix(t *testing.T) {
	entries := []PostingEntry{
		{Chunk: 10, Bits: 1 << 3},
		{Chunk: 12, Bits: 1 << 5},
		{Chunk: 1000, Bits: 3},
	}
	firstSize, err := PostingEntriesEncodedSize(entries[:1])
	if err != nil {
		t.Fatal(err)
	}
	twoSize, err := PostingEntriesEncodedSize(entries[:2])
	if err != nil {
		t.Fatal(err)
	}
	lastSize, err := PostingEntriesEncodedSize(entries[2:])
	if err != nil {
		t.Fatal(err)
	}
	if count, size, err := PostingPagePrefix(entries, twoSize); err != nil || count != 2 || size != twoSize {
		t.Fatalf("PostingPagePrefix = (%d,%d,%v), want (2,%d,nil)", count, size, err, twoSize)
	}
	if count, size, err := PostingPagePrefix(entries[2:], lastSize); err != nil || count != 1 || size != lastSize {
		t.Fatalf("reset PostingPagePrefix = (%d,%d,%v), want (1,%d,nil)", count, size, err, lastSize)
	}
	if _, _, err := PostingPagePrefix(entries, firstSize-1); !errors.Is(err, ErrInvalidWrite) {
		t.Fatalf("undersized PostingPagePrefix = %v, want %v", err, ErrInvalidWrite)
	}
	invalid := append([]PostingEntry(nil), entries...)
	invalid[1].Chunk = invalid[0].Chunk
	if _, _, err := PostingPagePrefix(invalid, twoSize); !errors.Is(err, ErrInvalidWrite) {
		t.Fatalf("unordered PostingPagePrefix = %v, want %v", err, ErrInvalidWrite)
	}
}

func TestPostingPageRejectsInvalidWrites(t *testing.T) {
	validHeader := testPostingHeader()
	validSegments := testPostingSegments()
	for _, test := range []struct {
		name   string
		mutate func(*PostingPageHeader, *[]PostingSegment, *uint64, *uint32)
	}{
		{"store id", func(h *PostingPageHeader, _ *[]PostingSegment, _ *uint64, _ *uint32) { h.StoreID = [16]byte{} }},
		{"generation", func(h *PostingPageHeader, _ *[]PostingSegment, _ *uint64, _ *uint32) { h.Generation = 0 }},
		{"logical id", func(h *PostingPageHeader, _ *[]PostingSegment, _ *uint64, _ *uint32) {
			h.LogicalID = StateRootLogicalID
		}},
		{"future logical id", func(h *PostingPageHeader, _ *[]PostingSegment, next *uint64, _ *uint32) { h.LogicalID = *next }},
		{"page size", func(h *PostingPageHeader, _ *[]PostingSegment, _ *uint64, _ *uint32) { h.PageSize = 5000 }},
		{"index id", func(h *PostingPageHeader, _ *[]PostingSegment, _ *uint64, high *uint32) { h.IndexID = *high }},
		{"index high water", func(_ *PostingPageHeader, _ *[]PostingSegment, _ *uint64, high *uint32) { *high = 0 }},
		{"page flags", func(h *PostingPageHeader, _ *[]PostingSegment, _ *uint64, _ *uint32) { h.Flags = 1 }},
		{"empty", func(_ *PostingPageHeader, segments *[]PostingSegment, _ *uint64, _ *uint32) { *segments = nil }},
		{"stream zero", func(_ *PostingPageHeader, segments *[]PostingSegment, _ *uint64, _ *uint32) {
			(*segments)[0].StreamID = 0
		}},
		{"stream duplicate", func(_ *PostingPageHeader, segments *[]PostingSegment, _ *uint64, _ *uint32) {
			(*segments)[1].StreamID = (*segments)[0].StreamID
		}},
		{"segment flags", func(_ *PostingPageHeader, segments *[]PostingSegment, _ *uint64, _ *uint32) { (*segments)[0].Flags = 1 }},
		{"collision without certificate", func(_ *PostingPageHeader, segments *[]PostingSegment, _ *uint64, _ *uint32) {
			(*segments)[0].Flags = PostingSegmentCollision
		}},
		{"link same page", func(h *PostingPageHeader, segments *[]PostingSegment, _ *uint64, _ *uint32) {
			(*segments)[0].Next.LogicalID = h.LogicalID
		}},
		{"link missing page", func(_ *PostingPageHeader, segments *[]PostingSegment, _ *uint64, _ *uint32) {
			(*segments)[1].Next.Segment = 1
		}},
		{"empty entries", func(_ *PostingPageHeader, segments *[]PostingSegment, _ *uint64, _ *uint32) {
			(*segments)[0].Entries = nil
		}},
		{"zero mask", func(_ *PostingPageHeader, segments *[]PostingSegment, _ *uint64, _ *uint32) {
			(*segments)[0].Entries[1].Bits = 0
		}},
		{"unordered", func(_ *PostingPageHeader, segments *[]PostingSegment, _ *uint64, _ *uint32) {
			(*segments)[0].Entries[1].Chunk = (*segments)[0].Entries[0].Chunk
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			header := validHeader
			segments := clonePostingSegments(validSegments)
			nextLogicalID := testPostingNextLogicalID
			indexHighWater := testPostingIndexHighWater
			test.mutate(&header, &segments, &nextLogicalID, &indexHighWater)
			page := make([]byte, testSuperblockPageSize)
			if _, err := EncodePostingPage(page, header, segments, nextLogicalID, indexHighWater); !errors.Is(err, ErrInvalidWrite) {
				t.Fatalf("EncodePostingPage = %v, want %v", err, ErrInvalidWrite)
			}
		})
	}

	many := make([]PostingSegment, 100)
	for i := range many {
		many[i] = PostingSegment{StreamID: uint32(i + 1), Entries: []PostingEntry{{Chunk: uint32(i), Bits: 1}}}
	}
	page := make([]byte, testSuperblockPageSize)
	if _, err := EncodePostingPage(page, validHeader, many, testPostingNextLogicalID, testPostingIndexHighWater); !errors.Is(err, ErrInvalidWrite) {
		t.Fatalf("oversized posting page = %v, want %v", err, ErrInvalidWrite)
	}
}

func TestPostingPageRejectsResealedSemanticCorruption(t *testing.T) {
	header := testPostingHeader()
	segments := testPostingSegments()
	page := encodeTestPostingPage(t, header, segments)
	record := PageHeaderSize + PostingPagePayloadHeaderSize
	entryStart := PageHeaderSize + PostingPagePayloadHeaderSize + len(segments)*PostingSegmentHeaderSize
	for _, test := range []struct {
		name   string
		mutate func([]byte)
	}{
		{"version", func(p []byte) { binary.LittleEndian.PutUint32(p[PageHeaderSize:PageHeaderSize+4], 3) }},
		{"index", func(p []byte) {
			binary.LittleEndian.PutUint32(p[PageHeaderSize+4:PageHeaderSize+8], testPostingIndexHighWater)
		}},
		{"count", func(p []byte) { p[PageHeaderSize+8]++ }},
		{"page flags", func(p []byte) { p[PageHeaderSize+10] = 1 }},
		{"directory bytes", func(p []byte) { p[PageHeaderSize+12]++ }},
		{"data bytes", func(p []byte) { p[PageHeaderSize+16]++ }},
		{"page reserved", func(p []byte) { p[PageHeaderSize+31] = 1 }},
		{"stream zero", func(p []byte) { binary.LittleEndian.PutUint32(p[record:record+4], 0) }},
		{"stream order", func(p []byte) {
			binary.LittleEndian.PutUint32(p[record+PostingSegmentHeaderSize:record+PostingSegmentHeaderSize+4], 7)
		}},
		{"rows", func(p []byte) { p[record+12]++ }},
		{"last", func(p []byte) { p[record+8]++ }},
		{"next self", func(p []byte) { binary.LittleEndian.PutUint64(p[record+24:record+32], header.LogicalID) }},
		{"data gap", func(p []byte) { p[record+32]++ }},
		{"data length", func(p []byte) { p[record+36]++ }},
		{"entry count", func(p []byte) { p[record+40]++ }},
		{"segment flags", func(p []byte) { p[record+44] = 1 }},
		{"segment reserved", func(p []byte) { p[record+46] = 1 }},
		{"noncanonical varint", func(p []byte) { p[entryStart], p[entryStart+1] = 0x80, 0x00 }},
		{"dense singleton", func(p []byte) {
			binary.LittleEndian.PutUint64(p[entryStart+2:entryStart+10], 1)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			corrupt := append([]byte(nil), page...)
			test.mutate(corrupt)
			resealTestPage(corrupt)
			if _, err := OpenPostingPage(corrupt, testPostingNextLogicalID, testPostingIndexHighWater); !errors.Is(err, ErrPostingPageCorrupt) {
				t.Fatalf("OpenPostingPage = %v, want %v", err, ErrPostingPageCorrupt)
			}
		})
	}
	for _, cut := range []int{0, PageHeaderSize - 1, len(page) - 1} {
		if _, err := OpenPostingPage(page[:cut], testPostingNextLogicalID, testPostingIndexHighWater); !errors.Is(err, ErrPostingPageCorrupt) {
			t.Fatalf("cut %d = %v, want %v", cut, err, ErrPostingPageCorrupt)
		}
	}
}
