package storeio

import (
	"os"
	"testing"
)

func TestStageCanonicalPageCatalogStreamsIntoReservedGeneration(t *testing.T) {
	catalog, err := BuildCanonicalPageCatalog(PageCatalogDefinition{
		Indexes:   []PageCatalogIndex{{Name: "by_tenant", Paths: []string{"/tenant"}}},
		SkipPaths: []string{"/score"},
		Schema: &PageCatalogSchema{
			Root: PageCatalogSchemaObject,
			Fields: []PageCatalogSchemaField{
				{Path: "/tenant", Types: PageCatalogSchemaString, Required: true},
				{Path: "/score", Types: PageCatalogSchemaInteger},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.CreateTemp(t.TempDir(), "migration-catalog-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	reservation := UnrootedGenerationReservation{
		Offset: 64 << 10, Length: 4 << 20,
		FirstLogicalID: 100, LogicalIDCount: 1 << 12,
	}
	writer, err := NewUnrootedGenerationWriter(
		file, reservation, testStoreID, 17, 0,
	)
	if err != nil {
		t.Fatal(err)
	}
	sink, err := NewUnrootedPrimaryGraphSink(
		writer, testStoreID, 17, reservation.FirstLogicalID,
		reservation.Offset+reservation.Length, make([]byte, 64<<10),
	)
	if err != nil {
		t.Fatal(err)
	}
	staged, err := StageCanonicalPageCatalog(sink, catalog, format0PageSize)
	if err != nil {
		t.Fatal(err)
	}
	if staged.Head == (PageRef{}) || staged.Pages == 0 ||
		staged.Bytes != uint32(catalog.CanonicalSize()) ||
		staged.Digest != catalog.Digest() {
		t.Fatalf("staged catalog = %+v", staged)
	}
	if err := writer.Sync(); err != nil {
		t.Fatal(err)
	}
	bounds := PageCatalogBounds{
		StoreID: testStoreID, Generation: 17, PageSize: format0PageSize,
		DataStart:     staged.Head.Offset,
		FileEnd:       reservation.Offset + reservation.Length,
		NextLogicalID: sink.BuildNextLogicalID(),
		TotalBytes:    staged.Bytes, ExpectedDigest: staged.Digest,
	}
	opened, err := OpenPageCatalogChainAt(
		file, staged.Head, bounds, make([]byte, format0PageSize),
	)
	if err != nil {
		t.Fatal(err)
	}
	if !opened.Equal(catalog) {
		t.Fatal("staged catalog differs after reopen")
	}
}
