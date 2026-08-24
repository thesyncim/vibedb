package driver

import (
	"bytes"
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
	if err := vibejson.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	want := append([]byte(nil), raw...)
	for i := range raw {
		raw[i] = ' '
	}
	if got := catalogFile(decoded).Tables["alpha"].PrimaryKey; got != "/key" {
		t.Fatalf("decoded catalog aliases caller bytes: %q", got)
	}
	reencoded, err := vibejson.Marshal(&decoded)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(reencoded, want) {
		t.Fatalf("canonical re-encode changed:\n got %s\nwant %s", reencoded, want)
	}
	if err := vibejson.Unmarshal(append(append([]byte(nil), want...), []byte(" null")...), &decoded); err == nil {
		t.Fatal("accepted trailing JSON value")
	}
}

func FuzzCatalogVibeJSON(f *testing.F) {
	f.Add(testCanonicalCatalogImage(f))
	f.Add([]byte(`{"version":0,"tables":{}}`))
	f.Add([]byte(`{"version":0,"version":0,"tables":{}}`))
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > maxCatalogBytes {
			return
		}
		var decoded catalogFileVibe
		if err := vibejson.Unmarshal(raw, &decoded); err != nil {
			return
		}
		canonical, err := vibejson.Marshal(&decoded)
		if err != nil {
			t.Fatalf("accepted catalog did not encode: %v", err)
		}
		var roundTrip catalogFileVibe
		if err := vibejson.Unmarshal(canonical, &roundTrip); err != nil {
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
		if err := vibejson.Unmarshal(raw, &decoded); err != nil {
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
