package storeio

import (
	"fmt"
	"testing"
)

var (
	pageCatalogBenchmarkCatalog *CanonicalPageCatalog
	pageCatalogBenchmarkChain   []PageCatalogChainPage
	pageCatalogBenchmarkBounds  PageCatalogBounds
)

func benchmarkPageCatalogDefinition() PageCatalogDefinition {
	definition := PageCatalogDefinition{
		Schema: &PageCatalogSchema{Root: PageCatalogSchemaObject},
	}
	for i := range 64 {
		path := fmt.Sprintf("/tenant/fields/%03d", i)
		definition.Indexes = append(definition.Indexes, PageCatalogIndex{
			Name: fmt.Sprintf("index_%03d", i), Paths: []string{path},
		})
		definition.Schema.Fields = append(
			definition.Schema.Fields,
			PageCatalogSchemaField{
				Path: path, Types: PageCatalogSchemaString, Required: i%4 == 0,
			},
		)
	}
	for i := range 32 {
		definition.Float64Paths = append(
			definition.Float64Paths, fmt.Sprintf("/metrics/%03d", i),
		)
	}
	return definition
}

func BenchmarkBuildCanonicalPageCatalog(b *testing.B) {
	definition := benchmarkPageCatalogDefinition()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		var err error
		pageCatalogBenchmarkCatalog, err = BuildCanonicalPageCatalog(definition)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkOpenCanonicalPageCatalog(b *testing.B) {
	catalog, err := BuildCanonicalPageCatalog(benchmarkPageCatalogDefinition())
	if err != nil {
		b.Fatal(err)
	}
	image := catalog.AppendCanonical(nil)
	b.SetBytes(int64(len(image)))
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		pageCatalogBenchmarkCatalog, err = OpenCanonicalPageCatalog(image)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEncodePageCatalogSegment(b *testing.B) {
	catalog, err := BuildCanonicalPageCatalog(benchmarkPageCatalogDefinition())
	if err != nil {
		b.Fatal(err)
	}
	pages, bounds := encodePageCatalogTestChain(b, catalog, testStoreID, 7)
	header := PageCatalogSegmentHeader{
		StoreID: testStoreID, Generation: 7,
		LogicalID: pages[0].Ref.LogicalID, Ordinal: 0,
	}
	if len(pages) > 1 {
		header.Next = pages[1].Ref
	}
	dst := make([]byte, PageCatalogSegmentPageSize)
	b.SetBytes(int64(PageCatalogSegmentPageSize))
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err = EncodePageCatalogSegment(dst, header, catalog, bounds); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkOpenPageCatalogSegment(b *testing.B) {
	catalog, err := BuildCanonicalPageCatalog(benchmarkPageCatalogDefinition())
	if err != nil {
		b.Fatal(err)
	}
	pages, bounds := encodePageCatalogTestChain(b, catalog, testStoreID, 7)
	page := pages[0].Page
	b.SetBytes(int64(len(page)))
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err = OpenPageCatalogSegment(page, bounds); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkOpenPageCatalogChain(b *testing.B) {
	catalog, err := BuildCanonicalPageCatalog(benchmarkPageCatalogDefinition())
	if err != nil {
		b.Fatal(err)
	}
	pages, bounds := encodePageCatalogTestChain(b, catalog, testStoreID, 7)
	bounds.ExpectedDigest = catalog.Digest()
	bytes := int64(len(pages) * int(PageCatalogSegmentPageSize))
	b.SetBytes(bytes)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		pageCatalogBenchmarkCatalog, err = OpenPageCatalogChain(pages, bounds)
		if err != nil {
			b.Fatal(err)
		}
	}
	pageCatalogBenchmarkChain = pages
	pageCatalogBenchmarkBounds = bounds
}
