package storeio

import (
	"bytes"
	"errors"
	"testing"
)

func mustBuildTinCatalog(t *testing.T, definition PageCatalogDefinition) *CanonicalPageCatalog {
	t.Helper()
	catalog, err := BuildCanonicalPageCatalog(definition)
	if err != nil {
		t.Fatal(err)
	}
	return catalog
}

func TestPageCatalogTinRoundTrip(t *testing.T) {
	input := PageCatalogDefinition{
		Indexes: []PageCatalogIndex{
			{Name: "id", Paths: []string{"/id"}},
		},
		TinIndexes: []PageCatalogTinIndex{
			{Name: "body_tin", Path: "/body"},
			{Name: "title_tin", Path: "/title"},
		},
	}
	catalog := mustBuildTinCatalog(t, input)
	definition := catalog.Definition()
	if len(definition.TinIndexes) != 2 ||
		definition.TinIndexes[0].Name != "body_tin" ||
		definition.TinIndexes[0].Path != "/body" ||
		definition.TinIndexes[1].Name != "title_tin" ||
		definition.TinIndexes[1].Path != "/title" {
		t.Fatalf("tin definition = %+v", definition.TinIndexes)
	}
	// Caller order does not affect the canonical image.
	reordered := input
	reordered.TinIndexes = []PageCatalogTinIndex{
		{Name: "title_tin", Path: "/title"},
		{Name: "body_tin", Path: "/body"},
	}
	other := mustBuildTinCatalog(t, reordered)
	if !catalog.Equal(other) {
		t.Fatal("tin catalog depends on caller order")
	}
	// The byte image reopens to the same definition.
	raw := catalog.AppendCanonical(nil)
	reopened, err := OpenCanonicalPageCatalog(raw)
	if err != nil {
		t.Fatal(err)
	}
	if !reopened.Equal(catalog) {
		t.Fatal("reopened tin catalog differs")
	}
	redefinition := reopened.Definition()
	if len(redefinition.TinIndexes) != 2 ||
		redefinition.TinIndexes[0] != definition.TinIndexes[0] ||
		redefinition.TinIndexes[1] != definition.TinIndexes[1] {
		t.Fatalf("reopened tin definition = %+v", redefinition.TinIndexes)
	}
}

func TestPageCatalogTinEmptyOmitsSection(t *testing.T) {
	plain := mustBuildTinCatalog(t, PageCatalogDefinition{
		Indexes: []PageCatalogIndex{{Name: "id", Paths: []string{"/id"}}},
	})
	raw := plain.AppendCanonical(nil)
	if len(raw) < PageCatalogCanonicalHeaderSize {
		t.Fatal("catalog too short")
	}
	// No tin flag, zero tin count and version: a tin-free definition keeps
	// the exact historical spelling.
	if raw[14]&byte(pageCatalogCanonicalTinIndexes) != 0 ||
		raw[50] != 0 || raw[51] != 0 || raw[52] != 0 || raw[53] != 0 ||
		raw[54] != 0 || raw[55] != 0 {
		t.Fatal("tin-free catalog carries tin header state")
	}
	withTin := mustBuildTinCatalog(t, PageCatalogDefinition{
		Indexes:    []PageCatalogIndex{{Name: "id", Paths: []string{"/id"}}},
		TinIndexes: []PageCatalogTinIndex{{Name: "body_tin", Path: "/body"}},
	})
	if plain.Equal(withTin) {
		t.Fatal("tin catalog equals tin-free catalog")
	}
}

func TestPageCatalogTinDefinitionRejects(t *testing.T) {
	cases := map[string]PageCatalogDefinition{
		"empty name": {
			TinIndexes: []PageCatalogTinIndex{{Name: "", Path: "/body"}},
		},
		"bad path": {
			TinIndexes: []PageCatalogTinIndex{{Name: "t", Path: "body"}},
		},
		"duplicate": {
			TinIndexes: []PageCatalogTinIndex{
				{Name: "t", Path: "/a"}, {Name: "t", Path: "/b"},
			},
		},
		"shadow exact": {
			Indexes:    []PageCatalogIndex{{Name: "same", Paths: []string{"/id"}}},
			TinIndexes: []PageCatalogTinIndex{{Name: "same", Path: "/body"}},
		},
		"too many": {
			TinIndexes: func() []PageCatalogTinIndex {
				out := make([]PageCatalogTinIndex, PageCatalogMaxTinIndexes+1)
				for i := range out {
					out[i] = PageCatalogTinIndex{
						Name: string(rune('a' + i%26)) + string(rune('0'+i/26)),
						Path: "/body",
					}
				}
				return out
			}(),
		},
	}
	for name, definition := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := BuildCanonicalPageCatalog(definition); !errors.Is(
				err, ErrPageCatalogDefinition,
			) {
				t.Fatalf("Build = %v, want definition error", err)
			}
		})
	}
}

func TestPageCatalogTinCorruptRefusals(t *testing.T) {
	catalog := mustBuildTinCatalog(t, PageCatalogDefinition{
		TinIndexes: []PageCatalogTinIndex{{Name: "body_tin", Path: "/body"}},
	})
	raw := catalog.AppendCanonical(nil)
	mutate := func(fn func([]byte)) []byte {
		out := bytes.Clone(raw)
		fn(out)
		return out
	}
	// Locate the tin record: last recordSize bytes of the image.
	record := len(raw) - pageCatalogTinRecordSize
	cases := map[string][]byte{
		"flag cleared": mutate(func(b []byte) {
			b[14] &^= byte(pageCatalogCanonicalTinIndexes)
		}),
		"count bumped": mutate(func(b []byte) { b[50]++ }),
		"version zeroed": mutate(func(b []byte) {
			b[54], b[55] = 0, 0
		}),
		"record version bumped": mutate(func(b []byte) {
			b[record+4]++
		}),
		"record reserved set": mutate(func(b []byte) {
			b[record+6] = 1
		}),
		"trailing garbage": append(bytes.Clone(raw), 0),
	}
	for name, image := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := OpenCanonicalPageCatalog(image); !errors.Is(
				err, ErrPageCatalogCorrupt,
			) {
				t.Fatalf("Open = %v, want corrupt error", err)
			}
		})
	}
}
