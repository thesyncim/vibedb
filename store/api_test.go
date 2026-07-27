package store_test

import (
	"testing"

	"github.com/thesyncim/vibedb/store"
)

func TestCanonicalStoreAPI(t *testing.T) {
	schema, err := store.CompileSchema(store.SchemaDefinition{
		Root: store.SchemaObject,
		Fields: []store.SchemaField{
			{Path: "/profile/country", Types: store.SchemaString, Required: true},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	db, err := store.New(store.Options{ChunkDocuments: 8, Schema: schema})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Put("user:1", []byte(`{"profile":{"country":"PT"}}`)); err != nil {
		t.Fatal(err)
	}
	info, err := db.CreateIndex(store.IndexDefinition{
		Name: "country", Paths: []string{"/profile/country"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if info, err = db.BackfillIndex(info.Name, 0); err != nil || info.State != store.IndexReady {
		t.Fatalf("BackfillIndex = (%+v,%v)", info, err)
	}
	snapshot, _ := db.Snapshot()
	if raw, ok := snapshot.GetRaw("user:1"); !ok || string(raw.Bytes()) != `{"profile":{"country":"PT"}}` {
		t.Fatalf("GetRaw = (%q,%v)", raw.Bytes(), ok)
	}
}
