package storeio

import (
	"os"
	"testing"
)

func TestVisitPrimaryExactIndexRefsStreamsAuthenticatedGraph(t *testing.T) {
	const pageSize = uint32(4096)
	layout, err := MutableStoreLayout(pageSize)
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.CreateTemp(t.TempDir(), "exact-visit-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	nextOffset := layout.DataStart
	nextID := uint64(2)
	ref := func(kind PageKind) PageRef {
		result := PageRef{Offset: nextOffset, LogicalID: nextID, Generation: 1, Length: pageSize, Kind: kind}
		nextOffset += uint64(pageSize)
		nextID++
		return result
	}
	leaves := []PageRef{ref(PagePrimaryExactLeaf), ref(PagePrimaryExactLeaf), ref(PagePrimaryExactLeaf)}
	level0 := []PageRef{ref(PagePrimaryExactCatalog), ref(PagePrimaryExactCatalog)}
	level1 := ref(PagePrimaryExactCatalog)
	root := ref(PagePrimaryExactRoot)
	write := func(ref PageRef, page []byte) {
		t.Helper()
		if _, err := file.WriteAt(page, int64(ref.Offset)); err != nil {
			t.Fatal(err)
		}
	}
	for _, leaf := range leaves {
		page := make([]byte, pageSize)
		if _, err := EncodePrimaryExactLeafPage(page, testStoreID, 1, leaf.LogicalID, make([]byte, indexTermLeafHeaderBytes)); err != nil {
			t.Fatal(err)
		}
		write(leaf, page)
	}
	for index, catalog := range level0 {
		first, last := 0, 2
		if index == 1 {
			first, last = 2, 3
		}
		entries := make([]PrimaryExactCatalogEntry, 0, last-first)
		for leafIndex := first; leafIndex < last; leafIndex++ {
			entries = append(entries, PrimaryExactCatalogEntry{Leaf: leaves[leafIndex], Prefix: []byte{byte('a' + leafIndex)}})
		}
		page := make([]byte, pageSize)
		if _, err := EncodePrimaryExactCatalogLeafPage(page, testStoreID, 1, catalog.LogicalID, entries); err != nil {
			t.Fatal(err)
		}
		write(catalog, page)
	}
	page := make([]byte, pageSize)
	if _, err := EncodePrimaryExactCatalogIndexPage(page, testStoreID, 1, level1.LogicalID, level0); err != nil {
		t.Fatal(err)
	}
	write(level1, page)
	if _, err := EncodePrimaryExactRootPage(page, testStoreID, 1, root.LogicalID, []PrimaryExactRootEntry{{Catalog: level1, LeafCount: 3}}); err != nil {
		t.Fatal(err)
	}
	write(root, page)
	if err := file.Truncate(int64(nextOffset)); err != nil {
		t.Fatal(err)
	}
	bounds := PrimaryExactIndexBounds{StoreID: testStoreID, Generation: 1, FileEnd: nextOffset, NextLogicalID: nextID, AllocationQuantum: pageSize, MaxPageSize: pageSize, IndexCount: 1}
	cache, err := NewPageCache(file, PageCacheOptions{PageSize: int(pageSize), MaxPageSize: int(pageSize), ResidentBytes: 8 * int64(pageSize), StoreID: testStoreID, ReadConcurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()
	seen := make(map[PageRef]struct{}, 7)
	if err := VisitPrimaryExactIndexRefs(cache, root, bounds, func(ref PageRef) error {
		if _, duplicate := seen[ref]; duplicate {
			t.Fatalf("duplicate ref %+v", ref)
		}
		seen[ref] = struct{}{}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 7 {
		t.Fatalf("visited %d pages, want 7", len(seen))
	}
}
