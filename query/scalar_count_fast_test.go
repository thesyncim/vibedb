package query

import (
	"os"
	"testing"

	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
)

func TestScalarCountFastPathMatchesJSONSemantics(t *testing.T) {
	docs := [][]byte{
		[]byte(`{"country":"PT","n":1,"active":true,"meta":{"region":"eu"},"a/b":{"~key":"hit"}}`),
		[]byte(`{"country":"P\u0054","n":1.0,"active":false,"meta":{"region":"eu"},"a/b":{"~key":"hit"}}`),
		[]byte(`{"country":"US","n":2,"active":true,"meta":{"region":"us"}}`),
		[]byte(`{"n":1,"active":null,"meta":{}}`),
		[]byte(`{"coun\u0074ry":"PT","n":3,"active":false,"meta":{"region":"eu"}}`),
	}

	set := &store.Segment{ShapeTapes: true}
	for _, doc := range docs {
		if _, err := set.Append(doc); err != nil {
			t.Fatal(err)
		}
	}
	db, err := store.New(store.Options{ChunkDocuments: 8, ShapeTapes: true})
	if err != nil {
		t.Fatal(err)
	}
	for i, doc := range docs {
		if _, err := db.Put(string(rune('a'+i)), doc); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err := db.Snapshot()
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name  string
		q     *Query
		want  int64
		token bool
	}{
		{name: "escaped string", q: Select(Count()).Where(Cmp("country", Eq, "PT")), want: 3, token: true},
		{name: "exact number", q: Select(Count()).Where(Cmp("n", Eq, 1)), want: 3, token: true},
		{name: "boolean", q: Select(Count()).Where(Cmp("active", Eq, true)), want: 2, token: true},
		{name: "missing never equals", q: Select(Count()).Where(Cmp("country", Eq, "")), want: 0, token: true},
		{name: "nested string", q: Select(Count()).Where(Cmp("meta.region", Eq, "eu")), want: 3, token: true},
		{name: "escaped pointer", q: Select(Count()).Where(Cmp("/a~1b/~0key", Eq, "hit")), want: 2, token: true},
	}
	for _, source := range []struct {
		name string
		src  Source
	}{
		{name: "segment", src: FromSegment(set)},
		{name: "snapshot", src: FromSnapshot(snapshot)},
	} {
		for _, tc := range cases {
			t.Run(source.name+"/"+tc.name, func(t *testing.T) {
				var e Exec
				if err := tc.q.RunInto(&e, source.src); err != nil {
					t.Fatal(err)
				}
				column, ok := e.Result.Column("count(*)")
				if !ok || len(column.Cells) != 1 || !countIs(column.Cells[0], tc.want) {
					t.Fatalf("count = %s, want %d", resultKey(e.Result), tc.want)
				}
			})
		}
	}

	file, err := os.CreateTemp(t.TempDir(), "scalar-count-*")
	if err != nil {
		t.Fatal(err)
	}
	collection, err := durable.Create(file, durable.Options{
		Collection: store.Options{ChunkDocuments: 8, ShapeTapes: true},
		Durability: durable.DurabilityAsyncVisible,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = collection.Close()
		_ = file.Close()
	}()
	for i, doc := range docs {
		if _, err := collection.Put([]byte(string(rune('a'+i))), doc); err != nil {
			t.Fatal(err)
		}
	}
	fileSnapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer fileSnapshot.Close()
	for _, tc := range cases {
		t.Run("durable/"+tc.name, func(t *testing.T) {
			var e Exec
			if err := tc.q.RunInto(&e, FromFile(fileSnapshot)); err != nil {
				t.Fatal(err)
			}
			column, ok := e.Result.Column("count(*)")
			if !ok || len(column.Cells) != 1 || !countIs(column.Cells[0], tc.want) {
				t.Fatalf("count = %s, want %d", resultKey(e.Result), tc.want)
			}
			if tc.token {
				if e.Stats.IndexBounded || e.Stats.RowsScanned != uint64(len(docs)) ||
					e.Stats.TokenFilterRows+e.Stats.TokenFilterFallbackRows != uint64(len(docs)) ||
					e.Stats.Batches != 0 || e.Stats.Workers != 1 {
					t.Fatalf("token scan stats = %+v", e.Stats)
				}
			}
		})
	}
}

func TestDurableScalarCountTokenScanWarmAllocations(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "scalar-count-alloc-*")
	if err != nil {
		t.Fatal(err)
	}
	collection, err := durable.Create(file, durable.Options{
		Collection: store.Options{ChunkDocuments: 8, ShapeTapes: true},
		Durability: durable.DurabilityAsyncVisible,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = collection.Close()
		_ = file.Close()
	}()
	for i := 0; i < 32; i++ {
		country := "US"
		if i%4 == 0 {
			country = "PT"
		}
		doc := []byte(`{"country":"` + country + `","active":true}`)
		if _, err := collection.Put([]byte{byte(i)}, doc); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()

	q := Select(Count()).Where(Cmp("country", Eq, "PT"))
	if err := q.Prepare(); err != nil {
		t.Fatal(err)
	}
	var e Exec
	defer e.Release()
	if err := q.RunInto(&e, FromFile(snapshot)); err != nil {
		t.Fatal(err)
	}
	if allocations := testing.AllocsPerRun(5, func() {
		if err := q.RunInto(&e, FromFile(snapshot)); err != nil {
			t.Fatal(err)
		}
	}); allocations != 0 {
		t.Fatalf("warmed durable token count allocates %v/op", allocations)
	}
}
