package storeio

import (
	"fmt"
	"os"
	"testing"
)

func TestGenerationMigrationTabletBuilderPublishesThroughFinalVectorFold(t *testing.T) {
	sink := &migrationExactMemorySink{storeID: testStoreID, generation: 9, nextID: PrimaryFirstDynamicLogicalID, nextAt: 64 << 10, pages: make(map[PageRef][]byte)}
	builder, err := NewGenerationMigrationTabletBuilder(sink, 0, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	records := make([]PrimaryGraphRecord, 300)
	placements := make([]PrimaryGraphPlacement, len(records))
	for index := range records {
		records[index] = PrimaryGraphRecord{Key: fmt.Sprintf("key-%04d", index), Value: fmt.Sprintf(`{"n":%d}`, index)}
	}
	if err := builder.StageWindow(records[:256], placements[:256]); err != nil {
		t.Fatal(err)
	}
	if err := builder.StageWindow(records[256:], placements[256:]); err != nil {
		t.Fatal(err)
	}
	emission, err := builder.Finish(nil)
	if err != nil {
		t.Fatal(err)
	}
	if emission.TabletID != 0 || emission.Records != 300 || emission.Ref.Kind != PageTabletRoute || string(emission.FirstKey) != "key-0000" || string(emission.LastKey) != "key-0299" {
		t.Fatalf("emission=%+v", emission)
	}
	folder, err := NewPrimaryGraphCatalogFolder(sink)
	if err != nil {
		t.Fatal(err)
	}
	if err := folder.AddTabletRef(emission.TabletID, emission.FirstKey, emission.LastKey, emission.Ref); err != nil {
		t.Fatal(err)
	}
	root, err := folder.Finish()
	if err != nil {
		t.Fatal(err)
	}
	if root.Kind != PagePrimaryCatalog || root.LogicalID != GlobalTabletCatalogRootLogicalID {
		t.Fatalf("root=%+v", root)
	}
}

func TestFoldGenerationMigrationTabletVectorBuildsAuthenticatedCatalog(t *testing.T) {
	image, _ := buildPrimaryGraphTestImage(t, 1000)
	cache, err := NewPageCache(image.file, PageCacheOptions{PageSize: int(format0PageSize), MaxPageSize: GlobalTabletCatalogRootBytes, ResidentBytes: 4 << 20, StoreID: image.root.StoreID, ReadConcurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()
	var tablets []PageRef
	if err := VisitPrimaryGraphRefs(cache, image.root.PrimaryRoot, image.bounds, func(ref PageRef) error {
		if ref.Kind == PageTabletRoute {
			tablets = append(tablets, ref)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	vectorFile, err := os.CreateTemp(t.TempDir(), "tablet-vector-fold-*")
	if err != nil {
		t.Fatal(err)
	}
	defer vectorFile.Close()
	vector, err := OpenGenerationMigrationTabletVector(vectorFile, 0, uint32(len(tablets)), image.root.StoreID, [16]byte{3})
	if err != nil {
		t.Fatal(err)
	}
	for tabletID, ref := range tablets {
		if err := vector.Put(uint32(tabletID), GenerationMigrationTabletRef{Source: ref, Target: ref}); err != nil {
			t.Fatal(err)
		}
	}
	sink := &migrationExactMemorySink{storeID: image.root.StoreID, generation: 2, nextID: image.root.NextLogicalID, nextAt: image.bounds.FileEnd + 64<<10, pages: make(map[PageRef][]byte)}
	folder, err := NewPrimaryGraphCatalogFolder(sink)
	if err != nil {
		t.Fatal(err)
	}
	root, err := FoldGenerationMigrationTabletVector(vector, cache, image.bounds, folder)
	if err != nil {
		t.Fatal(err)
	}
	if root.Kind != PagePrimaryCatalog || root.Generation != 2 {
		t.Fatalf("root=%+v", root)
	}
}
