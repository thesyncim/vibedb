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
	catalog := PageRef{
		Offset: layout.DataStart, LogicalID: 2, Generation: 1,
		Length: pageSize, Kind: PagePrimaryExactCatalog,
	}
	rootRef := PageRef{
		Offset:    layout.DataStart + uint64(pageSize),
		LogicalID: 3, Generation: 1, Length: pageSize,
		Kind: PagePrimaryExactRoot,
	}
	page := make([]byte, pageSize)
	if _, err := EncodePrimaryExactRootPage(
		page, otherStoreID, 1, rootRef.LogicalID,
		[]PrimaryExactRootEntry{{Catalog: catalog, LeafCount: 1}},
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

// TestPrimaryExactRootByteIdenticalRebuild pins the deterministic byte image
// of the exact root: the same per-index catalog entries encode to the same
// bytes on every build, so a rebuilt ordered-primary index is byte-identical.
func TestPrimaryExactRootByteIdenticalRebuild(t *testing.T) {
	const pageSize = uint32(4096)
	layout, err := MutableStoreLayout(pageSize)
	if err != nil {
		t.Fatal(err)
	}
	entries := []PrimaryExactRootEntry{
		{
			Catalog: PageRef{
				Offset: layout.DataStart, LogicalID: 2, Generation: 1,
				Length: pageSize, Kind: PagePrimaryExactCatalog,
			},
			LeafCount: 7,
		},
		{}, // an empty physical index consumes only its zero root entry
		{
			Catalog: PageRef{
				Offset: layout.DataStart + uint64(pageSize), LogicalID: 3,
				Generation: 1, Length: pageSize, Kind: PagePrimaryExactCatalog,
			},
			LeafCount: 1,
		},
	}
	first := make([]byte, pageSize)
	second := make([]byte, pageSize)
	if _, err := EncodePrimaryExactRootPage(
		first, testStoreID, 1, 4, entries,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := EncodePrimaryExactRootPage(
		second, testStoreID, 1, 4, entries,
	); err != nil {
		t.Fatal(err)
	}
	if string(first) != string(second) {
		t.Fatal("exact root is not a deterministic byte image")
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
	if empty, ok := view.Entry(1); !ok || empty != (PrimaryExactRootEntry{}) {
		t.Fatalf("empty physical index entry = %+v ok=%v", empty, ok)
	}
	if entry, ok := view.Entry(0); !ok || entry.LeafCount != 7 ||
		entry.Catalog != entries[0].Catalog {
		t.Fatalf("entry 0 = %+v ok=%v", entry, ok)
	}
}

// TestPrimaryExactCatalogPageRoundTrip pins both catalog page levels: a
// level-0 page's ordered term-leaf entries (refs, first tiles, flags,
// prefixes) and a level-1 page's ordered children survive an encode/open
// round trip, deterministically, and out-of-order entries fail admission.
func TestPrimaryExactCatalogPageRoundTrip(t *testing.T) {
	const pageSize = uint32(4096)
	layout, err := MutableStoreLayout(pageSize)
	if err != nil {
		t.Fatal(err)
	}
	leafAt := func(i int) PageRef {
		return PageRef{
			Offset:    layout.DataStart + uint64(i)*uint64(pageSize),
			LogicalID: uint64(2 + i), Generation: 1,
			Length: pageSize, Kind: PagePrimaryExactLeaf,
		}
	}
	entries := []PrimaryExactCatalogEntry{
		{Leaf: leafAt(0), FirstTile: 0, Flags: PrimaryExactCatalogRunCut,
			Prefix: []byte("alpha")},
		{Leaf: leafAt(1), FirstTile: 0, Flags: PrimaryExactCatalogPiece,
			Prefix: []byte("beta")},
		{Leaf: leafAt(2), FirstTile: 4096, Flags: PrimaryExactCatalogPiece,
			Prefix: []byte("beta")},
		{Leaf: leafAt(3), FirstTile: 0, Prefix: []byte("gamma")},
	}
	bounds := PrimaryExactIndexBounds{
		StoreID: testStoreID, Generation: 1,
		FileEnd:       layout.DataStart + 16*uint64(pageSize),
		NextLogicalID: 20, AllocationQuantum: pageSize,
		MaxPageSize: pageSize, IndexCount: 1,
	}
	catalogRef := PageRef{
		Offset:    layout.DataStart + 8*uint64(pageSize),
		LogicalID: 10, Generation: 1, Length: pageSize,
		Kind: PagePrimaryExactCatalog,
	}
	page := make([]byte, pageSize)
	if _, err := EncodePrimaryExactCatalogLeafPage(
		page, testStoreID, 1, catalogRef.LogicalID, entries,
	); err != nil {
		t.Fatal(err)
	}
	view, err := OpenPrimaryExactCatalogPage(page, catalogRef, bounds)
	if err != nil {
		t.Fatal(err)
	}
	if view.Level() != 0 || view.Len() != len(entries) {
		t.Fatalf("catalog view level=%d len=%d", view.Level(), view.Len())
	}
	at := 0
	if err := view.ForEachEntry(func(entry PrimaryExactCatalogEntry) error {
		want := entries[at]
		if entry.Leaf != want.Leaf || entry.FirstTile != want.FirstTile ||
			entry.Flags != want.Flags ||
			string(entry.Prefix) != string(want.Prefix) {
			t.Fatalf("entry %d = %+v, want %+v", at, entry, want)
		}
		at++
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// Ordering is admission-checked: swapping two full-key prefixes must
	// fail closed.
	swapped := []PrimaryExactCatalogEntry{entries[3], entries[0]}
	if _, err := EncodePrimaryExactCatalogLeafPage(
		page, testStoreID, 1, catalogRef.LogicalID, swapped,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenPrimaryExactCatalogPage(
		page, catalogRef, bounds,
	); !errors.Is(err, ErrPrimaryExactIndexCorrupt) {
		t.Fatalf("out-of-order catalog admitted: %v", err)
	}

	// Level 1: ordered children round-trip.
	childA := PageRef{
		Offset:    layout.DataStart + 9*uint64(pageSize),
		LogicalID: 11, Generation: 1, Length: pageSize,
		Kind: PagePrimaryExactCatalog,
	}
	childB := PageRef{
		Offset:    layout.DataStart + 10*uint64(pageSize),
		LogicalID: 12, Generation: 1, Length: pageSize,
		Kind: PagePrimaryExactCatalog,
	}
	if _, err := EncodePrimaryExactCatalogIndexPage(
		page, testStoreID, 1, catalogRef.LogicalID,
		[]PageRef{childA, childB},
	); err != nil {
		t.Fatal(err)
	}
	view, err = OpenPrimaryExactCatalogPage(page, catalogRef, bounds)
	if err != nil {
		t.Fatal(err)
	}
	if view.Level() != 1 || view.Len() != 2 {
		t.Fatalf("catalog index view level=%d len=%d", view.Level(), view.Len())
	}
	got, ok := view.Child(1)
	if !ok || got != childB {
		t.Fatalf("child 1 = %+v ok=%v", got, ok)
	}
}

func TestPrimaryExactRootAcceptsPersistentLeafReuseOrder(t *testing.T) {
	const pageSize = uint32(4096)
	layout, err := MutableStoreLayout(pageSize)
	if err != nil {
		t.Fatal(err)
	}
	// Physical index order is canonical path order. Adding a path that sorts
	// before an existing one puts the new, later-allocated leaf before the old
	// leaf in the root. Allocation order must not prevent immutable leaf reuse.
	catalogs := []PageRef{
		{
			Offset: layout.DataStart + 2*uint64(pageSize), LogicalID: 4,
			Generation: 2, Length: pageSize, Kind: PagePrimaryExactCatalog,
		},
		{
			Offset: layout.DataStart, LogicalID: 2, Generation: 1,
			Length: pageSize, Kind: PagePrimaryExactCatalog,
		},
	}
	entries := []PrimaryExactRootEntry{
		{Catalog: catalogs[0], LeafCount: 2},
		{Catalog: catalogs[1], LeafCount: 1},
	}
	rootRef := PageRef{
		Offset: layout.DataStart + 3*uint64(pageSize), LogicalID: 5,
		Generation: 2, Length: pageSize, Kind: PagePrimaryExactRoot,
	}
	page := make([]byte, pageSize)
	if _, err := EncodePrimaryExactRootPage(
		page, testStoreID, 2, rootRef.LogicalID, entries,
	); err != nil {
		t.Fatal(err)
	}
	bounds := PrimaryExactIndexBounds{
		StoreID: testStoreID, Generation: 2,
		FileEnd:       layout.DataStart + 4*uint64(pageSize),
		NextLogicalID: 6, AllocationQuantum: pageSize,
		MaxPageSize: pageSize, IndexCount: 2,
	}
	if _, err := OpenPrimaryExactRootPage(page, rootRef, bounds); err != nil {
		t.Fatal(err)
	}

	duplicate := []PrimaryExactRootEntry{
		{Catalog: catalogs[1], LeafCount: 1},
		{Catalog: catalogs[1], LeafCount: 1},
	}
	if _, err := EncodePrimaryExactRootPage(
		page, testStoreID, 2, rootRef.LogicalID, duplicate,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenPrimaryExactRootPage(
		page, rootRef, bounds,
	); !errors.Is(err, ErrPrimaryExactIndexCorrupt) {
		t.Fatalf("duplicate exact catalog = %v, want %v",
			err, ErrPrimaryExactIndexCorrupt)
	}
}
