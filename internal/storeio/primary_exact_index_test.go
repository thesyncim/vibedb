package storeio

import (
	"errors"
	"testing"
)

func TestPrimaryExactRootRejectsGraft(t *testing.T) {
	const pageSize = uint32(4096)
	layout, err := MutableStoreLayout(pageSize)
	if err != nil {
		t.Fatal(err)
	}
	storeID := testStoreID
	otherStoreID := storeID
	otherStoreID[0] ^= 0xff
	leaf := PageRef{
		Offset: layout.DataStart, LogicalID: 2, Generation: 1,
		Length: pageSize, Kind: PagePrimaryExactLeaf,
	}
	rootRef := PageRef{
		Offset:    layout.DataStart + uint64(pageSize),
		LogicalID: 3, Generation: 1, Length: pageSize,
		Kind: PagePrimaryExactRoot,
	}
	page := make([]byte, pageSize)
	if _, err := EncodePrimaryExactRootPage(
		page, otherStoreID, 1, rootRef.LogicalID, []PageRef{leaf},
	); err != nil {
		t.Fatal(err)
	}
	_, err = OpenPrimaryExactRootPage(
		page, rootRef, PrimaryExactIndexBounds{
			StoreID: storeID, Generation: 1,
			FileEnd:       layout.DataStart + 2*uint64(pageSize),
			NextLogicalID: 4, AllocationQuantum: pageSize,
			MaxPageSize: pageSize, IndexCount: 1,
		},
	)
	if !errors.Is(err, ErrPrimaryExactIndexCorrupt) {
		t.Fatalf("grafted exact root = %v, want %v",
			err, ErrPrimaryExactIndexCorrupt)
	}
}

// TestPrimaryExactRootByteIdenticalRebuild pins the deterministic byte image of
// the exact reference catalog: the same leaf refs encode to the same bytes on
// every build, so a rebuilt ordered-primary index is byte-identical.
func TestPrimaryExactRootByteIdenticalRebuild(t *testing.T) {
	const pageSize = uint32(4096)
	layout, err := MutableStoreLayout(pageSize)
	if err != nil {
		t.Fatal(err)
	}
	leaves := []PageRef{
		{
			Offset: layout.DataStart, LogicalID: 2, Generation: 1,
			Length: pageSize, Kind: PagePrimaryExactLeaf,
		},
		{}, // an empty physical index consumes only its zero root entry
		{
			Offset: layout.DataStart + uint64(pageSize), LogicalID: 3,
			Generation: 1, Length: pageSize, Kind: PagePrimaryExactLeaf,
		},
	}
	first := make([]byte, pageSize)
	second := make([]byte, pageSize)
	if _, err := EncodePrimaryExactRootPage(
		first, testStoreID, 1, 4, leaves,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := EncodePrimaryExactRootPage(
		second, testStoreID, 1, 4, leaves,
	); err != nil {
		t.Fatal(err)
	}
	if string(first) != string(second) {
		t.Fatal("exact reference catalog is not a deterministic byte image")
	}
	bounds := PrimaryExactIndexBounds{
		StoreID: testStoreID, Generation: 1,
		FileEnd:       layout.DataStart + 4*uint64(pageSize),
		NextLogicalID: 5, AllocationQuantum: pageSize,
		MaxPageSize: pageSize, IndexCount: 3,
	}
	rootRef := PageRef{
		Offset:    layout.DataStart + 3*uint64(pageSize),
		LogicalID: 4, Generation: 1, Length: pageSize,
		Kind: PagePrimaryExactRoot,
	}
	view, err := OpenPrimaryExactRootPage(first, rootRef, bounds)
	if err != nil {
		t.Fatal(err)
	}
	if view.Len() != 3 {
		t.Fatalf("exact root len = %d, want 3", view.Len())
	}
	if empty, ok := view.Leaf(1); !ok || empty != (PageRef{}) {
		t.Fatalf("empty physical index leaf = %+v ok=%v", empty, ok)
	}
}
