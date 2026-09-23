package durable

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/tin"
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

// TestDurableTinSealedBuildAgreesWithHeap proves the sealed generation
// build reads exactly like an open heap index over the same documents:
// generation builds seal after their single scan, so every TinSearch runs
// the packed readers. Ordinals follow RangeRaw order on both sides.
func TestDurableTinSealedBuildAgreesWithHeap(t *testing.T) {
	_, docs := openTinDatabase(t)
	snap, err := docs.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Close()
	var keys, texts []string
	if err := snap.RangeRaw(func(key, value []byte) error {
		var doc struct {
			Body string `json:"body"`
		}
		if err := json.Unmarshal(value, &doc); err != nil {
			return err
		}
		keys = append(keys, string(key))
		texts = append(texts, doc.Body)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	heap := tin.NewIndex()
	for i, text := range texts {
		heap.Add(tin.DocID(i), text)
	}
	build, err := snap.TinBuildForPath("/body")
	if err != nil {
		t.Fatal(err)
	}
	for _, input := range []string{
		"luxury", "luxury AND watches", `"luxury watches"`, "vintage OR cheap",
	} {
		q, err := build.Index().ParseTINQL(input)
		if err != nil {
			t.Fatalf("ParseTINQL(%q): %v", input, err)
		}
		want, err := docs.TinSearch(snap, "/body", input, 10)
		if err != nil {
			t.Fatalf("TinSearch(%q): %v", input, err)
		}
		got := heap.Score(q, 10, nil)
		if len(got) != len(want) {
			t.Fatalf("%q: heap %d hits, sealed %d", input, len(got), len(want))
		}
		for i := range got {
			if key := keys[uint64(got[i].Doc)]; key != want[i].Key {
				t.Fatalf("%q hit %d: heap key %q, sealed %q", input, i, key, want[i].Key)
			}
			if got[i].Score != want[i].Score {
				t.Fatalf("%q hit %d: heap score %v, sealed %v", input, i, got[i].Score, want[i].Score)
			}
		}
	}
}

// The by-name accessor resolves the same generation-pinned build the path
// accessor returns, and reports ErrIndexNotFound for anything the pinned
// catalog does not declare — mirroring the heap Collection contract.
func TestDurableTinIndexByName(t *testing.T) {
	_, docs := openTinDatabase(t)
	snap, err := docs.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Close()
	byName, err := docs.TinIndex(snap, "body_tin")
	if err != nil {
		t.Fatalf("TinIndex(body_tin): %v", err)
	}
	byPath, err := docs.TinIndexForPath(snap, "/body")
	if err != nil {
		t.Fatalf("TinIndexForPath(/body): %v", err)
	}
	if byName != byPath {
		t.Fatal("by-name and by-path accessors built different indexes")
	}
	if _, err := docs.TinIndex(snap, "missing"); !errors.Is(err, store.ErrIndexNotFound) {
		t.Fatalf("TinIndex(missing) = %v, want %v", err, store.ErrIndexNotFound)
	}
	if _, err := docs.TinIndex(snap, "id_exact"); !errors.Is(err, store.ErrIndexNotFound) {
		t.Fatalf("TinIndex(exact name) = %v, want %v", err, store.ErrIndexNotFound)
	}
}
