package durable

import (
	"bytes"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/store"
)

// An online USING tin declaration on a durable collection publishes the
// declaration in a catalog-only generation: postings build lazily per
// generation on first query use, so unlike exact indexes there is no scan.
// The declaration is usable immediately and survives reopen.
func TestDurableDeclaresTinIndexOnline(t *testing.T) {
	dir := t.TempDir()
	databaseOptions := DatabaseOptions{Options: testDatabaseOptions()}
	db, err := OpenDatabase(dir, databaseOptions)
	if err != nil {
		t.Fatal(err)
	}
	options := testDatabaseOptions()
	docs, err := db.CreateCollection("docs", options)
	if err != nil {
		t.Fatal(err)
	}
	mustPut(t, docs, "one", `{"body":"luxury goods"}`)
	def := store.IndexDefinition{
		Name: "body_tin", Paths: []string{"/body"}, Kind: store.IndexTin,
	}
	info, err := docs.CreateIndexContext(t.Context(), def)
	if err != nil {
		t.Fatalf("CreateIndexContext(tin) = %v", err)
	}
	if info.Name != "body_tin" || info.Kind != store.IndexTin ||
		info.State != store.IndexReady || info.ColumnCount != 1 ||
		info.Columns[0] != "/body" {
		t.Fatalf("CreateIndexContext(tin) info = %+v", info)
	}
	// A duplicate declaration and one shadowing an exact alias both fail
	// without disturbing the published declaration.
	exactDef := store.IndexDefinition{Name: "id_exact", Paths: []string{"/id"}}
	if _, err := docs.CreateIndex(exactDef); err != nil {
		t.Fatalf("CreateIndex(exact) = %v", err)
	}
	if _, err := docs.CreateIndex(def); !errors.Is(err, store.ErrIndexExists) {
		t.Fatalf("duplicate CreateIndex(tin) = %v, want %v", err, store.ErrIndexExists)
	}
	shadow := store.IndexDefinition{
		Name: "id_exact", Paths: []string{"/body"}, Kind: store.IndexTin,
	}
	if _, err := docs.CreateIndex(shadow); !errors.Is(err, store.ErrIndexExists) {
		t.Fatalf("shadowing CreateIndex(tin) = %v, want %v", err, store.ErrIndexExists)
	}
	// Malformed shapes fail before publication: empty names, non-single
	// paths, uniqueness, and bad pointers.
	for name, bad := range map[string]store.IndexDefinition{
		"empty name":  {Name: "", Paths: []string{"/body"}, Kind: store.IndexTin},
		"two paths":   {Name: "t", Paths: []string{"/a", "/b"}, Kind: store.IndexTin},
		"unique":      {Name: "t", Paths: []string{"/body"}, Kind: store.IndexTin, Unique: true},
		"bad pointer": {Name: "t", Paths: []string{"body"}, Kind: store.IndexTin},
	} {
		if _, err := docs.CreateIndex(bad); !errors.Is(err, store.ErrIndexDefinition) {
			t.Fatalf("%s CreateIndex(tin) = %v, want %v", name, err, store.ErrIndexDefinition)
		}
	}
	// The declaration answers immediately: the first query builds the
	// generation's postings.
	snap, err := docs.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	hits, err := docs.TinSearch(snap, "/body", "luxury", 10)
	snap.Close()
	if err != nil {
		t.Fatalf("TinSearch after declare = %v", err)
	}
	if len(hits) != 1 || hits[0].Key != "one" {
		t.Fatalf("TinSearch after declare = %+v, want [one]", hits)
	}
	// The declaration survives reopen with its exact neighbor intact.
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenDatabase(dir, databaseOptions)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	redocs, ok := reopened.Collection("docs")
	if !ok {
		t.Fatal("reopened database lost collection docs")
	}
	rsnap, err := redocs.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer rsnap.Close()
	var exact, tin *store.IndexInfo
	for _, info := range rsnap.AppendIndexes(nil) {
		switch info.Name {
		case "id_exact":
			ii := info
			exact = &ii
		case "body_tin":
			ii := info
			tin = &ii
		}
	}
	if exact == nil || exact.Kind != store.IndexExact ||
		exact.State != store.IndexReady {
		t.Fatalf("reopened exact advertisement = %+v", exact)
	}
	if tin == nil || tin.Kind != store.IndexTin ||
		tin.State != store.IndexReady || tin.ColumnCount != 1 ||
		tin.Columns[0] != "/body" {
		t.Fatalf("reopened tin advertisement = %+v", tin)
	}
	rhits, err := redocs.TinSearch(rsnap, "/body", "luxury", 10)
	if err != nil {
		t.Fatalf("TinSearch after reopen = %v", err)
	}
	if len(rhits) != 1 || rhits[0].Key != "one" {
		t.Fatalf("TinSearch after reopen = %+v, want [one]", rhits)
	}
}

// Tin declarations at creation persist in the versioned catalog section and
// reopen identically. Postings build lazily per generation, so declarations
// advertise IndexReady while readers observe the declaration exactly.
func TestDurablePersistsTinIndexDefinitions(t *testing.T) {
	dir := t.TempDir()
	databaseOptions := DatabaseOptions{Options: testDatabaseOptions()}
	db, err := OpenDatabase(dir, databaseOptions)
	if err != nil {
		t.Fatal(err)
	}
	options := testDatabaseOptions()
	options.Indexes = []store.IndexDefinition{
		{Name: "id_exact", Paths: []string{"/id"}},
		{Name: "body_tin", Paths: []string{"/body"}, Kind: store.IndexTin},
	}
	docs, err := db.CreateCollection("docs", options)
	if err != nil {
		t.Fatal(err)
	}
	mustPut(t, docs, "one", `{"id":1,"body":"luxury goods"}`)
	assertTinAdvertised(t, docs, false)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenDatabase(dir, databaseOptions)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	redocs, ok := reopened.Collection("docs")
	if !ok {
		t.Fatal("reopened database lost collection docs")
	}
	assertTinAdvertised(t, redocs, true)
	// Writes keep working beside the declaration, and the document reads back.
	mustPut(t, redocs, "two", `{"id":2,"body":"vintage watches"}`)
	got, found, err := redocs.AppendRaw(nil, []byte("two"))
	if err != nil || !found {
		t.Fatalf("AppendRaw(two) = (%s,%v,%v)", got, found, err)
	}
	if !bytes.Contains(got, []byte("vintage watches")) {
		t.Fatalf("AppendRaw(two) = %s", got)
	}
}

func assertTinAdvertised(t *testing.T, docs *Collection, reopened bool) {
	t.Helper()
	snap, err := docs.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Close()
	infos := snap.AppendIndexes(nil)
	var exact, tin *store.IndexInfo
	for i := range infos {
		switch infos[i].Name {
		case "id_exact":
			exact = &infos[i]
		case "body_tin":
			tin = &infos[i]
		}
	}
	if exact == nil || exact.Kind != store.IndexExact ||
		exact.State != store.IndexReady {
		t.Fatalf("reopened=%v exact advertisement = %+v", reopened, exact)
	}
	if tin == nil {
		t.Fatalf("reopened=%v: tin declaration missing from catalog", reopened)
	}
	if tin.Kind != store.IndexTin || tin.State != store.IndexReady ||
		tin.ColumnCount != 1 || tin.Columns[0] != "/body" {
		t.Fatalf("reopened=%v tin advertisement = %+v", reopened, tin)
	}
}

// Tin declarations reject the same malformed shapes the in-memory sidecar
// rejects: empty names, non-single paths, uniqueness, bad pointers, and
// names shadowing an exact alias.
func TestDurableRejectsMalformedTinDefinitions(t *testing.T) {
	base := func() Options {
		options := testDatabaseOptions()
		return options
	}
	cases := map[string][]store.IndexDefinition{
		"empty name":   {{Name: "", Paths: []string{"/body"}, Kind: store.IndexTin}},
		"no path":      {{Name: "t", Paths: nil, Kind: store.IndexTin}},
		"two paths":    {{Name: "t", Paths: []string{"/a", "/b"}, Kind: store.IndexTin}},
		"unique":       {{Name: "t", Paths: []string{"/body"}, Kind: store.IndexTin, Unique: true}},
		"bad pointer":  {{Name: "t", Paths: []string{"body"}, Kind: store.IndexTin}},
		"shadow exact": {{Name: "same", Paths: []string{"/id"}}, {Name: "same", Paths: []string{"/body"}, Kind: store.IndexTin}},
		"duplicate":    {{Name: "t", Paths: []string{"/a"}, Kind: store.IndexTin}, {Name: "t", Paths: []string{"/b"}, Kind: store.IndexTin}},
	}
	for name, indexes := range cases {
		t.Run(name, func(t *testing.T) {
			options := base()
			options.Indexes = indexes
			db, err := OpenDatabase(t.TempDir(), DatabaseOptions{Options: testDatabaseOptions()})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if _, err := db.CreateCollection("docs", options); err == nil {
				t.Fatal("CreateCollection with malformed tin = nil")
			} else if !errors.Is(err, store.ErrIndexDefinition) &&
				!errors.Is(err, store.ErrIndexExists) {
				t.Fatalf("CreateCollection with malformed tin = %v", err)
			}
		})
	}
}
