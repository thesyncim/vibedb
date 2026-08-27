package storeio

import "testing"

func TestGenerationMigrationSourceCatalogExtentRequiresPublishedLineage(t *testing.T) {
	m := GenerationMigrationManifest{
		Phase: GenerationMigrationPublished, SourceGeneration: 7,
		SourceCatalogHead:  PageRef{Offset: 64 << 10, LogicalID: 4, Generation: 7, Length: 4096, Kind: PageCatalogSegment},
		SourceCatalogBytes: PageCatalogCanonicalHeaderSize,
	}
	extent, ok, err := GenerationMigrationSourceCatalogExtent(m, 4096, 7)
	if err != nil || !ok {
		t.Fatalf("extent: %+v ok=%v err=%v", extent, ok, err)
	}
	if extent.Offset != m.SourceCatalogHead.Offset || extent.Length != 4096 || extent.RetiredGeneration != 7 {
		t.Fatalf("extent = %+v", extent)
	}
	m.Phase = GenerationMigrationReady
	if _, _, err := GenerationMigrationSourceCatalogExtent(m, 4096, 7); err == nil {
		t.Fatal("unpublished lineage retired")
	}
}
