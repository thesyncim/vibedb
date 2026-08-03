package query

import (
	"fmt"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/store"
)

func TestSnapshotOverlayGoldenParity(t *testing.T) {
	const rows = 256
	collection, err := store.New(store.Options{ChunkDocuments: 32, ShapeTapes: true})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < rows; i++ {
		doc := fmt.Sprintf(
			`{"id":%d,"sel":%d,"tag":"t%c","score":%d,"nested":{"bucket":%d},"obj":{"x":%d},"active":%v}`,
			i, i%100, 'A'+i%13, i*7%997, i%23, i%3, i%2 == 0,
		)
		if _, err := collection.Put(fmt.Sprintf("k%08d", i), []byte(doc)); err != nil {
			t.Fatal(err)
		}
	}
	base, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}

	entries := []testFileOverlayEntry{
		{key: "k00000000", doc: []byte(`{"id":0,"sel":0,"tag":"tA","score":1,"nested":{"bucket":0},"obj":{"x":0},"active":true}`), present: true},
		{key: "k00000001", present: false},
		{key: "k00000002", doc: []byte(`{"id":2,"sel":99,"tag":"tZ","score":2,"nested":{"bucket":2},"obj":{"x":2},"active":false}`), present: true},
		{key: "k00001000", doc: []byte(`{"id":1000,"sel":7,"tag":"tN","score":3,"nested":{"bucket":7},"obj":{"x":1},"active":true}`), present: true, insert: true},
		{key: "k00001001", doc: []byte(`{"id":1001,"sel":8,"tag":"tO","score":4,"nested":{"bucket":8},"obj":{"x":0},"active":false}`), present: true, insert: true},
	}
	overlay := &testFileOverlay{
		byKey:   make(map[string]testFileOverlayEntry, len(entries)),
		entries: entries,
		delta:   -1 + 2,
	}
	for _, entry := range entries {
		overlay.byKey[entry.key] = entry
		switch {
		case entry.insert && entry.present:
			if _, err := collection.Put(entry.key, entry.doc); err != nil {
				t.Fatal(err)
			}
		case !entry.present:
			if _, err := collection.Delete(entry.key); err != nil {
				t.Fatal(err)
			}
		default:
			if _, err := collection.Put(entry.key, entry.doc); err != nil {
				t.Fatal(err)
			}
		}
	}
	committed, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}

	holder := NewFileOverlaySource(overlay)
	shapes := []struct {
		name string
		q    *Query
	}{
		{"scan", Select(Path("id")).OrderBy("id", Asc)},
		{"filter", Select(Path("id")).Where(Cmp("sel", Lt, 50)).OrderBy("id", Asc)},
		{"nested", Select(Path("id")).Where(Cmp("nested.bucket", Eq, 7)).OrderBy("id", Asc)},
		{"replace-visible", Select(Path("id"), Path("sel")).Where(Cmp("id", Eq, 2)).OrderBy("id", Asc)},
		{"delete-absent", Select(Path("id")).Where(Cmp("id", Eq, 1)).OrderBy("id", Asc)},
		{"insert-visible", Select(Path("id")).Where(Cmp("id", Ge, 1000)).OrderBy("id", Asc)},
		{"ordered-limit", Select(Path("id"), Path("tag")).Where(Cmp("sel", Lt, 40)).OrderBy("id", Asc).Limit(32)},
		{"group", Select(Path("nested.bucket"), Count(), Sum("score")).GroupBy("nested.bucket").Where(Cmp("sel", Lt, 80)).OrderBy("nested.bucket", Asc)},
		{"aggregate", Select(Count(), Sum("score")).Where(Cmp("sel", Lt, 50))},
		{"contains", Select(Path("id")).Where(Contains("obj", `{"x":1}`)).OrderBy("id", Asc)},
		{"exists", Select(Path("id")).Where(Exists("score")).OrderBy("id", Asc)},
		{"active", Select(Path("id")).Where(Cmp("active", Eq, true)).OrderBy("id", Asc)},
	}
	for _, shape := range shapes {
		t.Run(shape.name, func(t *testing.T) {
			want, err := shape.q.Run(FromSnapshot(committed))
			if err != nil {
				t.Fatalf("FromSnapshot: %v", err)
			}
			got, err := shape.q.Run(FromSnapshotOverlay(base, &holder))
			if err != nil {
				t.Fatalf("FromSnapshotOverlay: %v", err)
			}
			if gotKey, wantKey := resultKey(got), resultKey(want); gotKey != wantKey {
				t.Fatalf("overlay result diverged from committed snapshot\ngot:\n%s\nwant:\n%s", gotKey, wantKey)
			}
		})
	}
}

func TestSnapshotOverlayMergesWritesLikeFileOverlay(t *testing.T) {
	collection, err := store.New(store.Options{ChunkDocuments: 4, ShapeTapes: true})
	if err != nil {
		t.Fatal(err)
	}
	documents := []string{
		`{"id":0,"active":false}`,
		`{"id":1,"active":true}`,
		`{"id":2,"active":true}`,
		`{"id":3,"active":false}`,
	}
	for i, document := range documents {
		if _, err := collection.Put(fmt.Sprintf("k%d", i), []byte(document)); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}

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
	if err := q.RunInto(&e, FromSnapshotOverlay(snapshot, &source)); err != nil {
		t.Fatal(err)
	}
	if got := resultKey(e.Result); got != "|id\n3:0|\n3:4|\n" {
		t.Fatalf("merged result = %q, want 0 and 4", got)
	}
}

func TestSnapshotOverlayRejectsInvalidSources(t *testing.T) {
	q := Select(Count())
	var e Exec
	if err := q.RunInto(&e, FromSnapshotOverlay(store.Snapshot{}, nil)); err == nil {
		t.Fatal("nil overlay accepted")
	}
	badDelta := &testFileOverlay{delta: -1}
	bad := NewFileOverlaySource(badDelta)
	if err := q.RunInto(&e, FromSnapshotOverlay(store.Snapshot{}, &bad)); err == nil {
		t.Fatal("underflowing LenDelta accepted")
	}

	collection, err := store.New(store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := collection.Put("k", []byte(`{"id":1}`)); err != nil {
		t.Fatal(err)
	}
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	joined := Select(Path("id")).Join(JoinOn("other", "id", JoinKey))
	holder := NewFileOverlaySource(&testFileOverlay{})
	err = joined.RunInto(&e, FromSnapshotOverlay(snapshot, &holder))
	if err == nil || !strings.Contains(err.Error(), "FromSnapshotOverlay") {
		t.Fatalf("join rejection = %v, want FromSnapshotOverlay", err)
	}
}

func TestSnapshotOverlaySteadyAllocs(t *testing.T) {
	const rows = 4096
	builder, err := store.NewBuilder(store.Options{ChunkDocuments: 64, ShapeTapes: true})
	if err != nil {
		t.Fatal(err)
	}
	for i, doc := range parallelCorpus(rows) {
		if err := builder.Append(fmt.Sprintf("k%08d", i), doc); err != nil {
			t.Fatal(err)
		}
	}
	collection, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}

	// An empty overlay exercises the merge+Segment path without forcing the
	// caller's Lookup implementation to allocate on every base key.
	holder := NewFileOverlaySource(emptySnapshotOverlay{})
	q := Select(Path("id"), Path("tag")).Where(Cmp("sel", Lt, 50)).OrderBy("id", Asc)
	e := Exec{Options: ExecOptions{Workers: 4}}
	src := FromSnapshotOverlay(snapshot, &holder)
	// Two warm-ups grow Segment/result capacity; the third builds the retained
	// Range and insert func values once capacity has settled.
	for i := 0; i < 3; i++ {
		if err := q.RunInto(&e, src); err != nil {
			t.Fatal(err)
		}
	}
	allocs := testing.AllocsPerRun(25, func() {
		if err := q.RunInto(&e, src); err != nil {
			panic(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("warmed FromSnapshotOverlay allocated %.2f times, want 0", allocs)
	}
}

// emptySnapshotOverlay is a zero-write FileOverlay that never converts keys to
// strings, so the steadystate allocation contract measures the query merge path
// rather than a test double's map Lookup.
type emptySnapshotOverlay struct{}

func (emptySnapshotOverlay) Lookup([]byte) ([]byte, bool, bool) { return nil, false, false }
func (emptySnapshotOverlay) RangeInserts(func([]byte) error) error {
	return nil
}
func (emptySnapshotOverlay) RangePresent(func([]byte) error) error {
	return nil
}
func (emptySnapshotOverlay) LenDelta() int64 { return 0 }
