package storeio

import (
	"encoding/binary"
	"errors"
	"testing"
)

const testOverflowNextLogicalID = uint64(100)

func testOverflowHeader(logicalID, offset, total uint64, next PageRef) OverflowPageHeader {
	return OverflowPageHeader{
		StoreID: testStoreID, Generation: 11, LogicalID: logicalID,
		PageSize: testSuperblockPageSize,
		Total:    total, Offset: offset, Next: next,
	}
}

func testOverflowRef(logicalID, page, generation uint64) PageRef {
	return PageRef{
		Offset: page * uint64(testSuperblockPageSize), LogicalID: logicalID, Generation: generation,
		Length: testSuperblockPageSize, Kind: PageOverflow,
	}
}

func TestOverflowPageChainRoundTrip(t *testing.T) {
	pieces := [][]byte{[]byte("hello"), []byte(" wor"), []byte("ld!")}
	refs := []PageRef{
		testOverflowRef(10, 10, 11), testOverflowRef(11, 11, 11), testOverflowRef(12, 12, 11),
	}
	fileEnd := uint64(16 * testSuperblockPageSize)
	var offset uint64
	var joined []byte
	for i, piece := range pieces {
		var next PageRef
		if i+1 < len(refs) {
			next = refs[i+1]
		}
		header := testOverflowHeader(refs[i].LogicalID, offset, 12, next)
		page := make([]byte, testSuperblockPageSize)
		encoded, err := EncodeOverflowPage(page, header, piece, fileEnd, testOverflowNextLogicalID, testSuperblockPageSize)
		if err != nil {
			t.Fatal(err)
		}
		view, err := OpenOverflowPage(encoded, fileEnd, testOverflowNextLogicalID, testSuperblockPageSize)
		if err != nil {
			t.Fatal(err)
		}
		if view.Header() != header || string(view.Data()) != string(piece) || cap(view.Data()) != len(piece) {
			t.Fatalf("piece %d = (%+v,%q), want (%+v,%q)", i, view.Header(), view.Data(), header, piece)
		}
		joined = append(joined, view.Data()...)
		offset += uint64(len(piece))
	}
	if string(joined) != "hello world!" {
		t.Fatalf("joined = %q", joined)
	}
}

func TestOverflowPageExactQuantumExtents(t *testing.T) {
	const dataLength = 5000
	fileEnd := uint64(64 * testSuperblockPageSize)
	for _, pages := range []uint32{3, 5, 7} {
		header := testOverflowHeader(10, 0, dataLength, PageRef{})
		header.PageSize = pages * testSuperblockPageSize
		data := make([]byte, dataLength)
		page := make([]byte, header.PageSize)
		if _, err := EncodeOverflowPage(
			page, header, data, fileEnd, testOverflowNextLogicalID,
			testSuperblockPageSize,
		); err != nil {
			t.Fatalf("%d-page EncodeOverflowPage: %v", pages, err)
		}
		view, err := OpenOverflowPage(
			page, fileEnd, testOverflowNextLogicalID,
			testSuperblockPageSize,
		)
		if err != nil || view.Header() != header ||
			len(view.Data()) != len(data) {
			t.Fatalf("%d-page extent = (%+v,%d,%v)",
				pages, view.Header(), len(view.Data()), err)
		}
	}

	next := testOverflowRef(11, 20, 11)
	next.Length = 5 * testSuperblockPageSize
	header := testOverflowHeader(10, 0, 8, next)
	header.PageSize = 3 * testSuperblockPageSize
	page := make([]byte, header.PageSize)
	if _, err := EncodeOverflowPage(
		page, header, []byte("1234"), uint64(32*testSuperblockPageSize),
		testOverflowNextLogicalID, testSuperblockPageSize,
	); err != nil {
		t.Fatalf("exact-extent continuation: %v", err)
	}
}

func TestOverflowPageRejectsInvalidAndCorrupt(t *testing.T) {
	fileEnd := uint64(16 * testSuperblockPageSize)
	validNext := testOverflowRef(11, 11, 11)
	valid := testOverflowHeader(10, 0, 8, validNext)
	for _, mutate := range []func(*OverflowPageHeader, *[]byte){
		func(h *OverflowPageHeader, _ *[]byte) { h.StoreID = [16]byte{} },
		func(h *OverflowPageHeader, _ *[]byte) { h.LogicalID = 0 },
		func(h *OverflowPageHeader, _ *[]byte) { h.Total = 4 },
		func(h *OverflowPageHeader, _ *[]byte) { h.Offset = 8 },
		func(h *OverflowPageHeader, _ *[]byte) { h.Next.LogicalID = h.LogicalID },
		func(h *OverflowPageHeader, _ *[]byte) { h.Next.Kind = PagePrimaryLeaf },
		func(_ *OverflowPageHeader, data *[]byte) { *data = nil },
	} {
		header := valid
		data := []byte("1234")
		mutate(&header, &data)
		page := make([]byte, testSuperblockPageSize)
		if _, err := EncodeOverflowPage(page, header, data, fileEnd, testOverflowNextLogicalID, testSuperblockPageSize); !errors.Is(err, ErrInvalidWrite) {
			t.Fatalf("EncodeOverflowPage = %v, want %v", err, ErrInvalidWrite)
		}
	}

	page := make([]byte, testSuperblockPageSize)
	if _, err := EncodeOverflowPage(page, valid, []byte("1234"), fileEnd, testOverflowNextLogicalID, testSuperblockPageSize); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func([]byte){
		func(p []byte) { binary.LittleEndian.PutUint32(p[PageHeaderSize:], 2) },
		func(p []byte) { p[PageHeaderSize+4] = 1 },
		func(p []byte) { binary.LittleEndian.PutUint32(p[PageHeaderSize+56:], 3) },
		func(p []byte) { binary.LittleEndian.PutUint64(p[PageHeaderSize+48:], 8) },
		func(p []byte) { p[PageHeaderSize+8+29] = 1 },
	} {
		corrupt := append([]byte(nil), page...)
		mutate(corrupt)
		resealTestPage(corrupt)
		if _, err := OpenOverflowPage(corrupt, fileEnd, testOverflowNextLogicalID, testSuperblockPageSize); !errors.Is(err, ErrOverflowPageCorrupt) {
			t.Fatalf("OpenOverflowPage = %v, want %v", err, ErrOverflowPageCorrupt)
		}
	}
}

func TestOverflowPageSteadyAllocation(t *testing.T) {
	fileEnd := uint64(16 * testSuperblockPageSize)
	header := testOverflowHeader(10, 0, 4, PageRef{})
	data := []byte("1234")
	page := make([]byte, testSuperblockPageSize)
	if allocs := testing.AllocsPerRun(1000, func() {
		if _, err := EncodeOverflowPage(page, header, data, fileEnd, testOverflowNextLogicalID, testSuperblockPageSize); err != nil {
			panic(err)
		}
		view, err := OpenOverflowPage(page, fileEnd, testOverflowNextLogicalID, testSuperblockPageSize)
		if err != nil {
			panic(err)
		}
		overflowDataSink = view.Data()
	}); allocs != 0 {
		t.Fatalf("overflow-page codec allocations = %g, want 0", allocs)
	}
}

var overflowDataSink []byte
