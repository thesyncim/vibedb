package storeio

import (
	"bytes"
	"testing"
)

// An overflow at the last row returns before advancing to the next leaf.
// Re-entry with row == Len must advance, even with an upper/prefix fence.
func TestBoundedPrimaryGraphDrainResumesAfterExhaustedLeaf(t *testing.T) {
	image, records := buildPrimaryGraphTestImage(t, 10_000)
	cache, err := NewPageCache(image.file, PageCacheOptions{
		PageSize: int(format0PageSize), MaxPageSize: GlobalTabletCatalogRootBytes,
		ResidentBytes: 4 << 20, StoreID: image.root.StoreID, ReadConcurrency: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()
	for _, prefix := range []bool{false, true} {
		var cursor PrimaryGraphCursor
		bounds := CommonPrimaryLeafBounds{FileEnd: image.bounds.FileEnd, NextLogicalID: image.bounds.NextLogicalID, AllocationQuantum: format0PageSize}
		if prefix {
			err = InitPrimaryGraphPrefixCursor(&cursor, cache, image.root.PrimaryRoot, image.bounds, bounds, []byte("key-"))
		} else {
			err = InitPrimaryGraphCursor(&cursor, cache, image.root.PrimaryRoot, image.bounds, bounds, nil, []byte(records[len(records)-1].Key+"\x00"))
		}
		if err != nil {
			t.Fatal(err)
		}
		first := cursor.leaf.Len()
		if first == len(records) {
			cursor.Close()
			t.Fatal("fixture must span multiple leaves")
		}
		cursor.row = first
		at := first
		var decoder CompactPrimaryScanDecoder
		key, ref, err := cursor.VisitInlineDecoded(&decoder, func(key, value []byte) error {
			if at >= len(records) || string(key) != records[at].Key || !bytes.Equal(value, []byte(records[at].Value)) {
				t.Fatalf("prefix=%t row %d mismatch", prefix, at)
			}
			at++
			return nil
		})
		cursor.Close()
		if err != nil || key != nil || ref != (PageRef{}) || at != len(records) {
			t.Fatalf("prefix=%t resumed rows=%d want=%d key=%q ref=%v err=%v", prefix, at-first, len(records)-first, key, ref, err)
		}
	}
}
