package query

import (
	"fmt"
	"os"
	"testing"

	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
)

type testFileOverlayEntry struct {
	key     string
	doc     []byte
	present bool
	insert  bool
}

type testFileOverlay struct {
	byKey   map[string]testFileOverlayEntry
	entries []testFileOverlayEntry
	delta   int64
}

func (o *testFileOverlay) Lookup(key []byte) ([]byte, bool, bool) {
	entry, ok := o.byKey[string(key)]
	if !ok {
		return nil, false, false
	}
	return entry.doc, entry.present, true
}

func (o *testFileOverlay) RangeInserts(visit func([]byte) error) error {
	for _, entry := range o.entries {
		if entry.insert && entry.present {
			if err := visit(entry.doc); err != nil {
				return err
			}
		}
	}
	return nil
}

func (o *testFileOverlay) RangePresent(visit func([]byte) error) error {
	for _, entry := range o.entries {
		if entry.present {
			if err := visit(entry.doc); err != nil {
				return err
			}
		}
	}
	return nil
}

func (o *testFileOverlay) LenDelta() int64 { return o.delta }

// A base exact index describes the retained root, not the staged writes.
// Replacements that enter or leave its predicate, a delete, and a new insert
// must all be decided by exact evaluation of the merged stream.
func TestFileOverlayMergesWritesOverIndexedSnapshot(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "query-file-overlay-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	collection, err := durable.Create(file, durable.Options{
		Collection: store.Options{ChunkDocuments: 4},
		Indexes: []store.IndexDefinition{{
			Name: "active", Paths: []string{"/active"},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()
	documents := []string{
		`{"id":0,"active":false}`,
		`{"id":1,"active":true}`,
		`{"id":2,"active":true}`,
		`{"id":3,"active":false}`,
	}
	for i, document := range documents {
		if _, err := collection.Put([]byte(fmt.Sprintf("k%d", i)), []byte(document)); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()

	entries := []testFileOverlayEntry{
		{key: "k0", doc: []byte(`{"id":0,"active":true}`), present: true},
		{key: "k1", present: false},
		{key: "k2", doc: []byte(`{"id":2,"active":false}`), present: true},
		{key: "new", doc: []byte(`{"id":4,"active":true}`), present: true, insert: true},
	}
	overlay := &testFileOverlay{
		byKey:   make(map[string]testFileOverlayEntry, len(entries)),
		entries: entries,
		delta:   -1 + 1,
	}
	for _, entry := range entries {
		overlay.byKey[entry.key] = entry
	}

	q := Select(Path("id")).Where(Cmp("active", Eq, true)).OrderBy("id", Asc)
	var e Exec
	source := NewFileOverlaySource(overlay)
	if err := q.RunInto(&e, FromFileOverlay(snapshot, &source)); err != nil {
		t.Fatal(err)
	}
	if got := resultKey(e.Result); got != "|id\n3:0|\n3:4|\n" {
		t.Fatalf("merged result = %q, want 0 and 4", got)
	}
	if !e.Stats.IndexBounded || e.Stats.IndexLookups == 0 {
		t.Fatalf("base index did not bound the corrected overlay execution: %+v", e.Stats)
	}
	if e.Stats.RowsTotal != 4 || e.Stats.CandidateRows != 2 || e.Stats.RowsScanned != 3 {
		t.Fatalf("overlay stats = %+v, want four visible, two base candidates, three exact scans", e.Stats)
	}
}

func TestFileOverlayRejectsInvalidSources(t *testing.T) {
	q := Select(Count())
	var e Exec
	empty := NewFileOverlaySource(&testFileOverlay{})
	if err := q.RunInto(&e, FromFileOverlay(nil, &empty)); err == nil {
		t.Fatal("nil snapshot accepted")
	}
	file, err := os.CreateTemp(t.TempDir(), "query-file-overlay-reject-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	collection, err := durable.Create(file, durable.Options{
		Collection: store.Options{ChunkDocuments: 4},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()
	if _, err := collection.Put([]byte("k000000"), []byte(`{"id":1}`)); err != nil {
		t.Fatal(err)
	}
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	if err := q.RunInto(&e, FromFileOverlay(snapshot, nil)); err == nil {
		t.Fatal("nil overlay accepted")
	}
	badDelta := &testFileOverlay{delta: -2}
	bad := NewFileOverlaySource(badDelta)
	if err := q.RunInto(&e, FromFileOverlay(snapshot, &bad)); err == nil {
		t.Fatal("underflowing LenDelta accepted")
	}
}
