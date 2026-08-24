package driver

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/thesyncim/vibejson"
)

func testCanonicalCatalogImage(t testing.TB) []byte {
	t.Helper()
	catalog := catalogFile{Version: catalogVersion, Tables: map[string]*tableMeta{
		"zeta":  {PrimaryKey: "/id", Storage: "zeta-0000000000000001"},
		"alpha": {PrimaryKey: "/key", Indexes: []indexMeta{{Name: "by_kind", Paths: []string{"/kind"}}}},
	}, Views: map[string]*viewMeta{
		"active": {Query: "SELECT id FROM zeta", Outputs: []string{"id"}, TableDependencies: []string{"zeta"}},
	}, ShardStore: &ShardStoreIdentity{
		Distribution: "catalog-distribution", Shard: "catalog-shard",
		AllocationGeneration: 1, LogID: [16]byte{1},
	}}
	encoded := catalogFileVibe(catalog)
	raw, err := vibejson.Marshal(&encoded)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestCatalogVibeJSONCanonicalOwnershipAndTrailingRejection(t *testing.T) {
	raw := testCanonicalCatalogImage(t)
	var decoded catalogFileVibe
	if err := decodeCatalogJSON(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	want := append([]byte(nil), raw...)
	for i := range raw {
		raw[i] = ' '
	}
	if got := catalogFile(decoded).Tables["alpha"].PrimaryKey; got != "/key" {
		t.Fatalf("decoded catalog aliases caller bytes: %q", got)
	}
	if got := catalogFile(decoded).ShardStore.Distribution; got != "catalog-distribution" {
		t.Fatalf("nested identity aliases caller bytes: %q", got)
	}
	reencoded, err := vibejson.Marshal(&decoded)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(reencoded, want) {
		t.Fatalf("canonical re-encode changed:\n got %s\nwant %s", reencoded, want)
	}
	if err := decodeCatalogJSON(append(append([]byte(nil), want...), []byte(" null")...), &decoded); err == nil {
		t.Fatal("accepted trailing JSON value")
	}
}

func TestCatalogVibeJSONRejectsDeepValueAtGrammarDepth(t *testing.T) {
	const extra = maxCatalogJSONDepth + 2
	raw := []byte(`{"version":0,"tables":{"docs":` +
		strings.Repeat("[", extra) + "0" + strings.Repeat("]", extra) + `}}`)
	var decoded catalogFileVibe
	if err := decodeCatalogJSON(raw, &decoded); err == nil ||
		!strings.Contains(err.Error(), "maximum JSON depth") {
		t.Fatalf("deep catalog decode = %v", err)
	}
}

func TestCatalogVibeJSONRejectsHugeEscapedKeyBeforeDecodeAllocation(t *testing.T) {
	// This stays under the total catalog bound but would materialize a decoded
	// multi-megabyte key if it reached DecodeCursor.NextField.
	escaped := bytes.Repeat([]byte(`\u0061`), (maxCatalogBytes-64)/6)
	raw := make([]byte, 0, len(escaped)+48)
	raw = append(raw, `{"`...)
	raw = append(raw, escaped...)
	raw = append(raw, `":0,"version":0,"tables":{}}`...)
	if len(raw) > maxCatalogBytes {
		t.Fatalf("fixture exceeds catalog bound: %d", len(raw))
	}
	var decoded catalogFileVibe
	if err := decodeCatalogJSON(raw, &decoded); err == nil ||
		!strings.Contains(err.Error(), "encoded byte bound") {
		t.Fatalf("huge escaped key decode = %v", err)
	}
	result := testing.Benchmark(func(b *testing.B) {
		for b.Loop() {
			if err := decodeCatalogJSON(raw, &decoded); err == nil {
				b.Fatal("accepted huge escaped key")
			}
		}
	})
	if allocated := result.AllocedBytesPerOp(); allocated > 64<<10 {
		t.Fatalf("rejected escaped key allocated %d bytes/op", allocated)
	}
}

func TestCatalogVibeJSONMalformedNestedValue(t *testing.T) {
	for _, raw := range [][]byte{
		[]byte(`{"version":0,"tables":{"docs":[}`),
		[]byte(`{"version":0,"tables":{"docs":{"primary_key":[]}}}`),
		[]byte(`{"version":0,"tables":{"docs":{"schema":{"fields":[{"path":{}}]}}}}`),
	} {
		var decoded catalogFileVibe
		if err := decodeCatalogJSON(raw, &decoded); err == nil {
			t.Fatalf("accepted malformed nested catalog %q", raw)
		}
	}
}

func TestCatalogVibeJSONRejectsNullVersionDirectly(t *testing.T) {
	var decoded catalogFileVibe
	err := decodeCatalogJSON([]byte(`{"version":null,"tables":{}}`), &decoded)
	if err == nil || !strings.Contains(err.Error(), "version must not be null") {
		t.Fatalf("null version decode = %v", err)
	}
}

func representativeLargeCatalog() catalogFile {
	catalog := catalogFile{Version: catalogVersion, Tables: make(map[string]*tableMeta, maxCatalogTables)}
	for i := 0; i < maxCatalogTables; i++ {
		name := fmt.Sprintf("table_%03d", i)
		catalog.Tables[name] = &tableMeta{PrimaryKey: "/id", Indexes: []indexMeta{{Name: "by_payload", Paths: []string{"/payload", "/kind"}}}, Storage: name + "-0000000000000001", Materialized: true}
	}
	return catalog
}

func TestCatalogVibeEncoderUsesCallerCapacityAndRejectsMalformedNestedMetadata(t *testing.T) {
	catalog := representativeLargeCatalog()
	bound, err := catalogSizeUpperBound(catalog)
	if err != nil {
		t.Fatal(err)
	}
	raw, encodeErr := appendCatalogJSON(make([]byte, 0, bound), catalog)
	if encodeErr != nil || len(raw) == 0 || cap(raw) != bound {
		t.Fatalf("catalog encoder did not retain caller capacity: bytes=%d cap=%d bound=%d err=%v", len(raw), cap(raw), bound, encodeErr)
	}
	malformed := catalogFile{Version: catalogVersion, Tables: map[string]*tableMeta{}, ReplicatedShardStore: &ReplicatedShardStoreIdentity{Format: ReplicatedShardStoreFormat, RelationCount: 2}}
	deferred := func() (raw []byte, err error, recovered any) {
		defer func() { recovered = recover() }()
		raw, err = malformed.MarshalJSON()
		return raw, err, nil
	}
	malformedRaw, marshalErr, recovered := deferred()
	if recovered != nil || marshalErr == nil || malformedRaw != nil {
		t.Fatalf("malformed catalog marshal = %q, %v, panic=%v", malformedRaw, marshalErr, recovered)
	}
}

func BenchmarkCatalogVibeEncoderPreallocated(b *testing.B) {
	catalog := representativeLargeCatalog()
	bound, err := catalogSizeUpperBound(catalog)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		raw, err := appendCatalogJSON(make([]byte, 0, bound), catalog)
		if err != nil || len(raw) == 0 {
			b.Fatal(err)
		}
	}
}

func FuzzCatalogVibeJSON(f *testing.F) {
	f.Add(testCanonicalCatalogImage(f))
	f.Add([]byte(`{"version":0,"tables":{}}`))
	f.Add([]byte(`{"version":0,"version":0,"tables":{}}`))
	f.Add([]byte(`{"version":0,"tables":{"docs":[}`))
	f.Add([]byte(`{"version":0,"tables":{"docs":{"primary_key":[]}}}`))
	f.Add([]byte(`{"version":0,"tables":{"docs":[[[[[[[[[0]]]]]]]]]}}`))
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > maxCatalogBytes {
			return
		}
		var decoded catalogFileVibe
		if err := decodeCatalogJSON(raw, &decoded); err != nil {
			return
		}
		canonical, err := vibejson.Marshal(&decoded)
		if err != nil {
			t.Fatalf("accepted catalog did not encode: %v", err)
		}
		var roundTrip catalogFileVibe
		if err := decodeCatalogJSON(canonical, &roundTrip); err != nil {
			t.Fatalf("canonical catalog did not decode: %v", err)
		}
		again, err := vibejson.Marshal(&roundTrip)
		if err != nil || !bytes.Equal(again, canonical) {
			t.Fatalf("catalog grammar is not byte-unique: %v", err)
		}
	})
}

func BenchmarkCatalogVibeJSON(b *testing.B) {
	raw := testCanonicalCatalogImage(b)
	b.ReportAllocs()
	b.SetBytes(int64(len(raw)))
	for b.Loop() {
		var decoded catalogFileVibe
		if err := decodeCatalogJSON(raw, &decoded); err != nil {
			b.Fatal(err)
		}
		encoded, err := vibejson.Marshal(&decoded)
		if err != nil {
			b.Fatal(err)
		}
		if len(encoded) == 0 {
			b.Fatal("empty catalog")
		}
	}
}
