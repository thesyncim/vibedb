package vibedb_test

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb"
	"github.com/thesyncim/vibedb/store"
)

// The native facade exposes tin end to end on both backends: declare with
// CreateTinIndex, search with TinSearch. Memory answers from the heap
// sidecar, Durable from the generation-pinned postings; both rank by BM25
// and report the same contract errors.
func TestFacadeTinSearchBothBackends(t *testing.T) {
	for _, tc := range []struct {
		name    string
		profile vibedb.Durability
	}{
		{"memory", vibedb.Memory},
		{"durable", vibedb.Durable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			profile := tc.profile
			db, err := vibedb.Open(
				filepath.Join(t.TempDir(), "catalog.vdb"),
				vibedb.WithDurability(profile),
			)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			docs := db.Collection("docs")
			for _, doc := range []struct{ key, body string }{
				{"one", `{"body":"luxury goods and vintage watches"}`},
				{"two", `{"body":"cheap goods everyday"}`},
				{"three", `{"body":"luxury watches"}`},
			} {
				if _, err := docs.Put(doc.key, []byte(doc.body)); err != nil {
					t.Fatalf("Put(%s): %v", doc.key, err)
				}
			}
			if err := docs.CreateTinIndex("body_tin", "/body"); err != nil {
				t.Fatalf("CreateTinIndex: %v", err)
			}
			hits, err := docs.TinSearch("/body", "luxury", 10)
			if err != nil {
				t.Fatalf("TinSearch: %v", err)
			}
			if len(hits) != 2 {
				t.Fatalf("hits = %+v, want 2", hits)
			}
			seen := map[string]bool{}
			for _, hit := range hits {
				seen[hit.Key] = true
				if hit.Score <= 0 {
					t.Fatalf("hit %+v has non-positive score", hit)
				}
			}
			if !seen["one"] || !seen["three"] {
				t.Fatalf("hits = %+v, want one and three", hits)
			}
			if hits[0].Score < hits[1].Score {
				t.Fatalf("hits not score-ordered: %+v", hits)
			}
			if one, err := docs.TinSearch("/body", "vintage", 10); err != nil ||
				len(one) != 1 || one[0].Key != "one" {
				t.Fatalf("vintage = (%+v, %v), want [one]", one, err)
			}
			if none, err := docs.TinSearch("/body", "luxury", 0); err != nil ||
				none != nil {
				t.Fatalf("topK=0 = (%+v, %v), want nil", none, err)
			}
			if _, err := docs.TinSearch("/missing", "luxury", 10); !errors.Is(
				err, store.ErrIndexNotFound,
			) {
				t.Fatalf("unindexed path = %v, want %v", err, store.ErrIndexNotFound)
			}
			if _, err := docs.TinSearch("/body", `"unclosed`, 10); err == nil {
				t.Fatal("invalid TINQL = nil, want parse error")
			}
			if err := docs.CreateTinIndex("body_tin", "/body"); !errors.Is(
				err, store.ErrIndexExists,
			) {
				t.Fatalf("duplicate = %v, want %v", err, store.ErrIndexExists)
			}
			if err := docs.CreateTinIndex("", "/body"); err == nil {
				t.Fatal("empty name = nil, want definition error")
			}
		})
	}
}
