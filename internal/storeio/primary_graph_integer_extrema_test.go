package storeio

import (
	"fmt"
	"os"
	"testing"
)

// buildPrimaryGraphIntegerExtremaImage keeps the graph construction identical
// to the ordinary primary-graph fixture while allowing the test to put an
// exact FOR target in one leaf and an unsupported target in its successor.
func buildPrimaryGraphIntegerExtremaImage(
	t testing.TB, records []PrimaryGraphRecord,
) *primaryGraphTestImage {
	t.Helper()
	file, err := os.CreateTemp(t.TempDir(), "primary-graph-extrema-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })
	const maxPages = 600
	committer, err := NewCommitter(file, DeviceOptions{
		Backend: BackendPortable, BufferCount: 1024,
		BufferSize: GlobalTabletCatalogRootBytes,
	}, CommitterOptions{
		QueueSlots: 2, MaxPagesPerBatch: maxPages, GroupLimit: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = committer.Close() })
	layout, err := MutableStoreLayout(format0PageSize)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := BeginWriteTransaction(
		committer, nil, maxPages,
		WriteTransactionOptions{
			StoreID: format0StoreID, Generation: 1, PageSize: format0PageSize,
			FileEnd:       layout.DataStart,
			NextLogicalID: PrimaryFirstDynamicLogicalID,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	primaryRoot, err := BuildPrimaryGraph(tx, records)
	if err != nil {
		_ = tx.Abort()
		t.Fatal(err)
	}
	root := StateRoot{
		StoreID: format0StoreID, Generation: 1, PageSize: format0PageSize,
		MaxPageSize:   GlobalTabletCatalogRootBytes,
		NextLogicalID: tx.NextLogicalID(), PrimaryRoot: primaryRoot,
	}
	if err := tx.PublishInline(root, InlineFreeDelta{}); err != nil {
		t.Fatal(err)
	}
	if err := committer.Wait(root.Generation); err != nil {
		t.Fatal(err)
	}
	return &primaryGraphTestImage{
		file: file, root: root,
		bounds: GlobalTabletCatalogBounds{
			StoreID: format0StoreID, SelectedRootGeneration: root.Generation,
			FileEnd: tx.FileEnd(), NextLogicalID: root.NextLogicalID,
		},
	}
}

func TestPrimaryGraphIntegerExtremaDeclineResetsSeededProgress(t *testing.T) {
	const rows = 2 * CompactPrimaryStripeMaxRows
	records := make([]PrimaryGraphRecord, rows)
	for row := range records {
		value := fmt.Sprintf(`{"n":%d}`, ((row*73)&1023)-512)
		if row >= CompactPrimaryStripeMaxRows {
			value = `{"n":"late"}`
		}
		records[row] = PrimaryGraphRecord{
			Key: fmt.Sprintf("key-%08d", row), Value: value,
		}
	}
	image := buildPrimaryGraphIntegerExtremaImage(t, records)
	cache, err := NewPageCache(image.file, PageCacheOptions{
		PageSize: int(format0PageSize), MaxPageSize: GlobalTabletCatalogRootBytes,
		ResidentBytes: 4 << 20, StoreID: image.root.StoreID, ReadConcurrency: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()
	leafBounds := CommonPrimaryLeafBounds{
		FileEnd: image.bounds.FileEnd, NextLogicalID: image.bounds.NextLogicalID,
		AllocationQuantum: format0PageSize,
	}
	var cursor PrimaryGraphCursor
	if err := InitPrimaryGraphCursor(
		&cursor, cache, image.root.PrimaryRoot, image.bounds, leafBounds, nil, nil,
	); err != nil {
		t.Fatal(err)
	}
	defer cursor.Close()
	if cursor.leaf.Len() != CompactPrimaryStripeMaxRows {
		t.Fatalf("first leaf rows=%d, want %d", cursor.leaf.Len(), CompactPrimaryStripeMaxRows)
	}
	filter, err := NewUnifiedIntegerExtremaFilter([]byte("/n"))
	if err != nil {
		t.Fatal(err)
	}
	first, ok := cursor.leaf.CountResolvedIntegerExtrema(&filter.resolver)
	if !ok || !first.Found || first.Min >= first.Max {
		t.Fatalf("first leaf extrema=%+v supported=%t, want valid FOR result", first, ok)
	}
	progress := UnifiedIntegerExtremaProgress{
		Min: -99, Max: 99, Found: true, Scanned: 123,
	}
	supported, err := cursor.FilterIntegerExtrema(filter, &progress)
	if err != nil {
		t.Fatal(err)
	}
	if supported {
		t.Fatal("late non-FOR leaf was accepted")
	}
	if progress != (UnifiedIntegerExtremaProgress{}) {
		t.Fatalf("declined progress=%+v, want zero after atomic decline", progress)
	}
}
