package store_test

import (
	"fmt"

	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibejson"
)

func ExampleCollection() {
	var s store.Collection
	_, _ = s.Put("user:42", []byte(`{"name":"Ada","score":7}`))
	before, _ := s.Snapshot()

	created, _ := s.Put("user:42", []byte(`{"name":"Ada","score":8}`))
	current, _ := s.GetRaw("user:42")
	old, _ := before.GetRaw("user:42")
	fmt.Printf("created=%v current=%s old=%s\n", created, current.Bytes(), old.Bytes())

	// Output:
	// created=false current={"name":"Ada","score":8} old={"name":"Ada","score":7}
}

func ExampleBuilder() {
	builder, _ := store.NewBuilder(store.Options{ShapeTapes: true})
	_ = builder.CreateIndex(store.IndexDefinition{
		Name: "country", Paths: []string{"/profile/country"},
	})
	_ = builder.Append("user:1", []byte(`{"profile":{"country":"PT"}}`))
	_ = builder.Append("user:2", []byte(`{"profile":{"country":"US"}}`))
	s, _ := builder.Build()

	snapshot, _ := s.Snapshot()
	keys, _ := snapshot.AppendIndexRawKeys(nil, "country", []byte(`"PT"`))
	fmt.Println(s.Generation(), keys)

	// Output:
	// 1 [user:1]
}

func ExampleCollection_AddIndex() {
	s, _ := store.New(store.Options{ChunkDocuments: 2, ShapeTapes: true})
	_, _ = s.Put("a", []byte(`{"team":"compiler"}`))
	_, _ = s.Put("b", []byte(`{"team":"runtime"}`))
	_, _ = s.Put("c", []byte(`{"team":"compiler"}`))

	info, _ := s.AddIndex("team-search", store.IndexPostings)
	for info.State != store.IndexReady {
		info, _ = s.BackfillIndex("team-search", 1)
	}

	src := []byte(`"compiler"`)
	need, _ := vibejson.RequiredIndexEntries(src)
	needle, _ := vibejson.BuildIndex(src, make([]vibejson.IndexEntry, 0, need))
	keys := s.AppendWhereContainsIndexKeys(make([]string, 0, s.Len()), "team", needle)
	fmt.Println(keys)

	// Output:
	// [a c]
}
