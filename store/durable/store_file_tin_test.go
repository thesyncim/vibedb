package durable

import (
	"bytes"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/store"
)

// An online USING tin build on a durable collection is refused: the durable
// page catalog persists the declaration but carries no tin postings yet, so
// building one would compile it as an exact index over the path — silent
// corruption. Tin declarations enter the catalog only at creation, until the
// postings slice lands.
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

// Tin declarations at creation persist in the versioned catalog section and
// reopen identically. They advertise as IndexBuilding — no postings exist —
// so planners keep their fallback while readers observe the declaration.
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
	if tin.Kind != store.IndexTin || tin.State != store.IndexBuilding ||
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
