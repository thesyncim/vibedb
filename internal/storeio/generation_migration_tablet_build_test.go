package storeio

import (
	"fmt"
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
