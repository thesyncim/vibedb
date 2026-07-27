package storeio

import (
	"encoding/binary"
	"errors"
	"testing"
)

func TestIndexGroupCatalogRoundTrip(t *testing.T) {
	header := IndexGroupCatalogHeader{
		StoreID: testStoreID, Generation: 7, LogicalID: 11,
		PageSize: testSuperblockPageSize, CoveredIndexes: 0b101,
		DocumentCount: 6,
	}
	entries := []IndexGroupCatalogEntry{
		{IndexID: 0, Value: []byte(`"a"`), Count: 4, First: 0},
		{IndexID: 0, Value: []byte(`null`), Count: 2, First: 3},
		{IndexID: 2, Value: []byte(`1.0`), Count: 3, First: 1},
		{IndexID: 2, Value: []byte(`2`), Count: 3, First: 4},
	}
	page := make([]byte, testSuperblockPageSize)
	encoded, err := EncodeIndexGroupCatalogPage(page, header, entries, 3, 1, 8)
	if err != nil {
		t.Fatal(err)
	}
	view, err := OpenIndexGroupCatalog(
		encoded, 3, 1, 8, 64*uint64(testSuperblockPageSize), 32,
		testSuperblockPageSize,
	)
	if err != nil {
		t.Fatal(err)
	}
	if view.Header() != header || view.Len() != len(entries) ||
		!view.Covered(0) || view.Covered(1) || !view.Covered(2) {
		t.Fatalf("catalog header = %+v len %d", view.Header(), view.Len())
	}
	iterator := view.Iterator()
	for position, want := range entries {
		got, ok := iterator.Next()
		if !ok || got.IndexID != want.IndexID || got.Count != want.Count ||
			got.First != want.First || string(got.Value) != string(want.Value) {
			t.Fatalf("entry %d = (%+v,%v), want %+v", position, got, ok, want)
		}
	}
	if _, ok := iterator.Next(); ok {
		t.Fatal("iterator returned an extra entry")
	}
	allocs := testing.AllocsPerRun(100, func() {
		if _, encodeErr := EncodeIndexGroupCatalogPage(
			page, header, entries, 3, 1, 8,
		); encodeErr != nil {
			panic(encodeErr)
		}
		if _, openErr := OpenIndexGroupCatalog(
			page, 3, 1, 8, 64*uint64(testSuperblockPageSize), 32,
			testSuperblockPageSize,
		); openErr != nil {
			panic(openErr)
		}
	})
	if allocs != 0 {
		t.Fatalf("catalog encode/open allocated %.2f times", allocs)
	}
	corrupt := append([]byte(nil), encoded...)
	countAt := PageHeaderSize + IndexGroupCatalogPayloadHeaderSize + 8
	binary.LittleEndian.PutUint64(corrupt[countAt:countAt+8], 5)
	if _, err := SealPage(corrupt); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenIndexGroupCatalog(
		corrupt, 3, 1, 8, 64*uint64(testSuperblockPageSize), 32,
		testSuperblockPageSize,
	); !errors.Is(err, ErrIndexGroupCatalogCorrupt) {
		t.Fatalf("resealed count corruption = %v, want %v", err, ErrIndexGroupCatalogCorrupt)
	}
	corrupt = append(corrupt[:0], encoded...)
	lengthAt := PageHeaderSize +
		IndexGroupCatalogPayloadHeaderSize + 4
	binary.LittleEndian.PutUint32(
		corrupt[lengthAt:lengthAt+4], ^uint32(0),
	)
	if _, err := SealPage(corrupt); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenIndexGroupCatalog(
		corrupt, 3, 1, 8, 64*uint64(testSuperblockPageSize), 32,
		testSuperblockPageSize,
	); !errors.Is(err, ErrIndexGroupCatalogCorrupt) {
		t.Fatalf(
			"resealed length corruption = %v, want %v",
			err, ErrIndexGroupCatalogCorrupt,
		)
	}
	for cut := 0; cut < len(encoded); cut++ {
		if _, err := OpenIndexGroupCatalog(
			encoded[:cut], 3, 1, 8, 64*uint64(testSuperblockPageSize), 32,
			testSuperblockPageSize,
		); !errors.Is(err, ErrIndexGroupCatalogCorrupt) {
			t.Fatalf("cut %d = %v, want %v", cut, err, ErrIndexGroupCatalogCorrupt)
		}
	}
}

func TestSegmentedIndexGroupCatalogRoundTrip(t *testing.T) {
	next := PageRef{
		Offset:    10 * uint64(testSuperblockPageSize),
		LogicalID: 12, Generation: 7,
		Length: testSuperblockPageSize, Kind: PageIndexGroupCatalog,
	}
	header := IndexGroupCatalogHeader{
		StoreID: testStoreID, Generation: 7, LogicalID: 11,
		PageSize: testSuperblockPageSize, CoveredIndexes: 0b11,
		DocumentCount: 10, Next: next,
	}
	entries := []IndexGroupCatalogEntry{
		{IndexID: 0, Value: []byte(`"a"`), Count: 3, First: 0},
		{IndexID: 0, Value: []byte(`"b"`), Count: 2, First: 1},
	}
	page := make([]byte, testSuperblockPageSize)
	encoded, err := EncodeSegmentedIndexGroupCatalogPage(
		page, header, entries, 2, 2, 8,
		64*uint64(testSuperblockPageSize), 32,
		testSuperblockPageSize,
	)
	if err != nil {
		t.Fatal(err)
	}
	view, err := OpenIndexGroupCatalog(
		encoded, 2, 2, 8,
		64*uint64(testSuperblockPageSize), 32,
		testSuperblockPageSize,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !view.Segmented() || view.Header() != header ||
		view.Len() != len(entries) {
		t.Fatalf(
			"segmented catalog = (%v,%+v,%d), want (true,%+v,%d)",
			view.Segmented(), view.Header(), view.Len(),
			header, len(entries),
		)
	}
	if allocs := testing.AllocsPerRun(100, func() {
		if _, encodeErr := EncodeSegmentedIndexGroupCatalogPage(
			page, header, entries, 2, 2, 8,
			64*uint64(testSuperblockPageSize), 32,
			testSuperblockPageSize,
		); encodeErr != nil {
			panic(encodeErr)
		}
		opened, openErr := OpenIndexGroupCatalog(
			page, 2, 2, 8,
			64*uint64(testSuperblockPageSize), 32,
			testSuperblockPageSize,
		)
		if openErr != nil || !opened.Segmented() {
			panic("segmented catalog open")
		}
	}); allocs != 0 {
		t.Fatalf(
			"segmented catalog warm allocations = %.2f, want zero",
			allocs,
		)
	}
}

func TestIndexGroupCatalogRejectsInvalidCoverage(t *testing.T) {
	header := IndexGroupCatalogHeader{
		StoreID: testStoreID, Generation: 7, LogicalID: 11,
		PageSize: testSuperblockPageSize, CoveredIndexes: 1,
		DocumentCount: 2,
	}
	page := make([]byte, testSuperblockPageSize)
	for _, test := range []struct {
		name    string
		entries []IndexGroupCatalogEntry
	}{
		{"short count", []IndexGroupCatalogEntry{
			{IndexID: 0, Value: []byte(`true`), Count: 1, First: 0},
		}},
		{"wrong index", []IndexGroupCatalogEntry{
			{IndexID: 1, Value: []byte(`true`), Count: 2, First: 0},
		}},
		{"empty value", []IndexGroupCatalogEntry{
			{IndexID: 0, Count: 2, First: 0},
		}},
		{"slot outside chunk", []IndexGroupCatalogEntry{
			{IndexID: 0, Value: []byte(`true`), Count: 2, First: 8},
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := EncodeIndexGroupCatalogPage(
				page, header, test.entries, 1, 1, 8,
			); !errors.Is(err, ErrInvalidWrite) {
				t.Fatalf("encode = %v, want %v", err, ErrInvalidWrite)
			}
		})
	}
}
