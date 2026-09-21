package durable

import (
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/store"
)

// A USING tin definition on a durable collection is refused, never built.
// The durable page catalog has no tin family: accepting one would compile
// and build it as an exact index over the path, reporting Kind exact while
// answering tin queries with exact postings.
func TestDurableRefusesTinIndexDefinitions(t *testing.T) {
	options := testDatabaseOptions()
	db, err := OpenDatabase(t.TempDir(), DatabaseOptions{Options: options})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	docs, err := db.CreateCollection("docs", options)
	if err != nil {
		t.Fatal(err)
	}
	mustPut(t, docs, "one", `{"body":"luxury goods"}`)
	def := store.IndexDefinition{
		Name: "body_tin", Paths: []string{"/body"}, Kind: store.IndexTin,
	}
	if _, err := docs.CreateIndex(def); !errors.Is(err, ErrTinIndexUnsupported) {
		t.Fatalf("CreateIndex(tin) = %v, want %v", err, ErrTinIndexUnsupported)
	}
	if _, err := docs.CreateIndexContext(t.Context(), def); !errors.Is(err, ErrTinIndexUnsupported) {
		t.Fatalf("CreateIndexContext(tin) = %v, want %v", err, ErrTinIndexUnsupported)
	}
	// The definition must not have leaked into the catalog as exact.
	snap, err := docs.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Close()
	for _, info := range snap.AppendIndexes(nil) {
		if info.Name == "body_tin" {
			t.Fatalf("tin definition cataloged as %+v", info)
		}
	}
}

// Tin definitions are refused at collection open too: normalized options
// compile every declared index through CompileExactIndex, which rejects
// non-exact Kinds.
func TestDurableOpenRefusesTinIndexDefinitions(t *testing.T) {
	db, err := OpenDatabase(t.TempDir(), DatabaseOptions{Options: testDatabaseOptions()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	options := testDatabaseOptions()
	options.Indexes = []store.IndexDefinition{
		{Name: "body_tin", Paths: []string{"/body"}, Kind: store.IndexTin},
	}
	if _, err := db.CreateCollection("docs", options); err == nil {
		t.Fatal("CreateCollection with tin index = nil, want definition error")
	} else if !errors.Is(err, store.ErrIndexDefinition) {
		t.Fatalf("CreateCollection with tin index = %v, want %v", err, store.ErrIndexDefinition)
	}
}
