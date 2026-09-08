package storeio

import (
	"bytes"
	"errors"
	"testing"
)

func primaryExactPackTestBounds(t *testing.T, pages uint64) (MutableStoreFileLayout, PrimaryExactIndexBounds) {
	t.Helper()
	layout, err := MutableStoreLayout(4096)
	if err != nil {
		t.Fatal(err)
	}
	return layout, PrimaryExactIndexBounds{StoreID: testStoreID, Generation: 3, FileEnd: layout.DataStart + pages*4096, NextLogicalID: 100, AllocationQuantum: 4096, MaxPageSize: 64 << 10, IndexCount: 2}
}

func TestPrimaryExactPackPageRoundTripRaw(t *testing.T) {
	layout, bounds := primaryExactPackTestBounds(t, 2)
	var encoder PrimaryExactPackEncoder
	if err := encoder.Prepare(2, 512); err != nil {
		t.Fatal(err)
	}
	want := [][]byte{bytes.Repeat([]byte{1, 2, 3}, 30), bytes.Repeat([]byte{9, 8}, 40)}
	for i := range want {
		if err := encoder.Append(uint32(7+i), want[i]); err != nil {
			t.Fatal(err)
		}
	}
	pack, err := encoder.EncodeRaw(make([]byte, 0, 1024), 4096)
	if err != nil {
		t.Fatal(err)
	}
	ref := PageRef{Offset: layout.DataStart, LogicalID: 11, Generation: 3, Length: 4096, Kind: PagePrimaryExactPack}
	page := make([]byte, 4096)
	if _, err := EncodePrimaryExactPackPage(page, testStoreID, 3, ref.LogicalID, pack); err != nil {
		t.Fatal(err)
	}
	var decoder PrimaryExactPackDecoder
	if err := decoder.Prepare(1024); err != nil {
		t.Fatal(err)
	}
	if err := OpenPrimaryExactPackPage(page, ref, bounds, &decoder); err != nil {
		t.Fatal(err)
	}
	for i := range want {
		got, err := decoder.Member(i, uint32(7+i))
		if err != nil || !bytes.Equal(got, want[i]) {
			t.Fatalf("member %d: %v", i, err)
		}
	}
	badRef := ref
	badRef.Kind = PagePrimaryExactLeaf
	if err := OpenPrimaryExactPackPage(page, badRef, bounds, &decoder); !errors.Is(err, ErrPrimaryExactIndexCorrupt) {
		t.Fatalf("wrong ref: %v", err)
	}
	page[len(page)-1] ^= 1
	if err := OpenPrimaryExactPackPage(page, ref, bounds, &decoder); !errors.Is(err, ErrPrimaryExactIndexCorrupt) {
		t.Fatalf("corrupt envelope: %v", err)
	}
}

func TestPrimaryExactInventoryPageRoundTripAndRejectsOrder(t *testing.T) {
	layout, bounds := primaryExactPackTestBounds(t, 8)
	refs := []PageRef{
		{Offset: layout.DataStart, LogicalID: 20, Generation: 2, Length: 4096, Kind: PagePrimaryExactPack},
		{Offset: layout.DataStart + 4096, LogicalID: 21, Generation: 3, Length: 8192, Kind: PagePrimaryExactPack},
	}
	invRef := PageRef{Offset: layout.DataStart + 3*4096, LogicalID: 30, Generation: 3, Length: 4096, Kind: PagePrimaryExactInventory}
	page := make([]byte, 4096)
	if _, err := EncodePrimaryExactInventoryPage(page, testStoreID, 3, invRef.LogicalID, PageRef{}, refs); err != nil {
		t.Fatal(err)
	}
	view, err := OpenPrimaryExactInventoryPage(page, invRef, bounds)
	if err != nil {
		t.Fatal(err)
	}
	if view.Len() != 2 || view.Next() != (PageRef{}) {
		t.Fatalf("view=%d next=%+v", view.Len(), view.Next())
	}
	for i, want := range refs {
		got, ok := view.Entry(uint32(i))
		if !ok || got != want {
			t.Fatalf("entry %d=%+v,%v", i, got, ok)
		}
	}
	if _, err := EncodePrimaryExactInventoryPage(make([]byte, 4096), testStoreID, 3, 31, PageRef{}, []PageRef{refs[1], refs[0]}); !errors.Is(err, ErrInvalidWrite) {
		t.Fatalf("unsorted: %v", err)
	}
	if _, err := EncodePrimaryExactInventoryPage(make([]byte, 4096), testStoreID, 3, 31, PageRef{}, []PageRef{refs[0], refs[0]}); !errors.Is(err, ErrInvalidWrite) {
		t.Fatalf("duplicate: %v", err)
	}
	if _, err := EncodePrimaryExactInventoryPage(make([]byte, 4096), testStoreID, 3, 31, PageRef{}, nil); !errors.Is(err, ErrInvalidWrite) {
		t.Fatalf("empty: %v", err)
	}
}

func TestPrimaryExactInventoryRejectsSelfCycleAndGraft(t *testing.T) {
	layout, bounds := primaryExactPackTestBounds(t, 5)
	pack := PageRef{Offset: layout.DataStart, LogicalID: 4, Generation: 3, Length: 4096, Kind: PagePrimaryExactPack}
	self := PageRef{Offset: layout.DataStart + 4096, LogicalID: 5, Generation: 3, Length: 4096, Kind: PagePrimaryExactInventory}
	page := make([]byte, 4096)
	if _, err := EncodePrimaryExactInventoryPage(page, testStoreID, 3, self.LogicalID, self, []PageRef{pack}); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenPrimaryExactInventoryPage(page, self, bounds); !errors.Is(err, ErrPrimaryExactIndexCorrupt) {
		t.Fatalf("cycle: %v", err)
	}
	other := testStoreID
	other[0] ^= 1
	if _, err := EncodePrimaryExactInventoryPage(page, other, 3, self.LogicalID, PageRef{}, []PageRef{pack}); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenPrimaryExactInventoryPage(page, self, bounds); !errors.Is(err, ErrPrimaryExactIndexCorrupt) {
		t.Fatalf("graft: %v", err)
	}
}
