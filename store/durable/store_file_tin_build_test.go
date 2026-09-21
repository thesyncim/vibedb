package durable

import (
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/store"
)

func openTinDatabase(t *testing.T) (*Database, *Collection) {
	t.Helper()
	db, err := OpenDatabase(t.TempDir(), DatabaseOptions{Options: testDatabaseOptions()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	options := testDatabaseOptions()
	options.Indexes = []store.IndexDefinition{
		{Name: "body_tin", Paths: []string{"/body"}, Kind: store.IndexTin},
	}
	docs, err := db.CreateCollection("docs", options)
	if err != nil {
		t.Fatal(err)
	}
	mustPut(t, docs, "one", `{"body":"luxury goods and vintage watches"}`)
	mustPut(t, docs, "two", `{"body":"cheap goods"}`)
	mustPut(t, docs, "three", `{"body":"luxury watches","id":3}`)
	return db, docs
}

func TestDurableTinSearchFindsDocuments(t *testing.T) {
	_, docs := openTinDatabase(t)
	snap, err := docs.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Close()
	hits, err := docs.TinSearch(snap, "/body", "luxury", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 2 {
		t.Fatalf("TinSearch(luxury) = %+v, want 2 hits", hits)
	}
	seen := map[string]bool{}
	for _, hit := range hits {
		seen[hit.Key] = true
	}
	if !seen["one"] || !seen["three"] {
		t.Fatalf("TinSearch(luxury) keys = %+v", hits)
	}
	if hits[0].Score < hits[1].Score {
		t.Fatalf("hits not ranked: %+v", hits)
	}
	one, err := docs.TinSearch(snap, "/body", "vintage", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(one) != 1 || one[0].Key != "one" {
		t.Fatalf("TinSearch(vintage) = %+v", one)
	}
	if _, err := docs.TinSearch(snap, "/body", "luxury", 0); err != nil {
		t.Fatal(err)
	}
}

func TestDurableTinSearchRebuildsAcrossGenerations(t *testing.T) {
	_, docs := openTinDatabase(t)
	snap, err := docs.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	before, err := docs.TinSearch(snap, "/body", "watches", 10)
	if err != nil {
		t.Fatal(err)
	}
	snap.Close()
	if len(before) != 2 {
		t.Fatalf("before = %+v", before)
	}
	// A write publishes a new generation; the next search builds for it and
	// sees the new document while the old snapshot's build stays valid.
	mustPut(t, docs, "four", `{"body":"pocket watches"}`)
	afterSnap, err := docs.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer afterSnap.Close()
	after, err := docs.TinSearch(afterSnap, "/body", "watches", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != 3 {
		t.Fatalf("after = %+v, want 3 hits", after)
	}
}

func TestDurableTinSearchErrors(t *testing.T) {
	_, docs := openTinDatabase(t)
	snap, err := docs.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Close()
	if _, err := docs.TinSearch(snap, "/title", "luxury", 10); !errors.Is(
		err, store.ErrIndexNotFound,
	) {
		t.Fatalf("missing path = %v, want ErrIndexNotFound", err)
	}
	if _, err := docs.TinIndexForPath(snap, "/title"); !errors.Is(
		err, store.ErrIndexNotFound,
	) {
		t.Fatalf("missing path index = %v, want ErrIndexNotFound", err)
	}
	if _, err := docs.TinSearch(snap, "/body", "(((", 10); err == nil {
		t.Fatal("invalid TINQL = nil")
	}
}
