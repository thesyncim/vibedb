package durable

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
)

// TestCreateBuildsEmptyPrimaryStore verifies the creation-time empty ordered
// primary graph across every durability lane: a freshly created store is a
// primary-layout store, its first Put fills the empty leaf, data survives a
// reopen, and deletes empty the leaf again without collapsing the graph.
func TestCreateBuildsEmptyPrimaryStore(t *testing.T) {
	cases := []struct {
		name       string
		durability DurabilityMode
		journal    bool
	}{
		{name: "sync", durability: DurabilitySync},
		{name: "async", durability: DurabilityAsyncVisible},
		{name: "buffered", durability: DurabilityBufferedVisible},
		{name: "buffered_journal", durability: DurabilityBufferedVisible, journal: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "empty-primary.vjs")
			file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
			if err != nil {
				t.Fatal(err)
			}
			options := testFileStoreOptions()
			options.Durability = tc.durability
			options.RecoveryJournal = tc.journal

			fs, err := Create(file, options)
			if err != nil {
				file.Close()
				t.Fatalf("Create empty primary: %v", err)
			}
			if fs.state.Load().root.PrimaryRoot == (storeio.PageRef{}) {
				t.Fatal("freshly created store has no ordered-primary root")
			}
			if fs.Len() != 0 {
				t.Fatalf("empty store Len = %d, want 0", fs.Len())
			}
			if got, ok, err := fs.AppendRaw(nil, []byte("missing")); err != nil || ok || got != nil {
				t.Fatalf("AppendRaw on empty store = (%q,%v,%v)", got, ok, err)
			}

			const n = 40
			for i := 0; i < n; i++ {
				key := fmt.Sprintf("k%03d", i)
				doc := fmt.Sprintf(`{"id":%d,"name":%q}`, i, key)
				created, err := fs.Put([]byte(key), []byte(doc))
				if err != nil {
					t.Fatalf("Put %s: %v", key, err)
				}
				if !created {
					t.Fatalf("Put %s reported update on fresh key", key)
				}
			}
			if fs.Len() != n {
				t.Fatalf("Len after %d puts = %d", n, fs.Len())
			}
			if err := fs.Flush(); err != nil {
				t.Fatalf("Flush: %v", err)
			}
			if err := fs.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			file.Close()

			// Reopen and confirm every document survived the round trip.
			file, err = os.OpenFile(path, os.O_RDWR, 0o600)
			if err != nil {
				t.Fatal(err)
			}
			fs, err = Open(file, options)
			if err != nil {
				file.Close()
				t.Fatalf("Open after empty-primary create: %v", err)
			}
			if fs.Len() != n {
				t.Fatalf("Len after reopen = %d, want %d", fs.Len(), n)
			}
			for i := 0; i < n; i++ {
				key := fmt.Sprintf("k%03d", i)
				want := fmt.Sprintf(`{"id":%d,"name":%q}`, i, key)
				got, ok, err := fs.AppendRaw(nil, []byte(key))
				if err != nil || !ok {
					t.Fatalf("AppendRaw %s after reopen = (ok=%v,%v)", key, ok, err)
				}
				if string(got) != want {
					t.Fatalf("AppendRaw %s = %s, want %s", key, got, want)
				}
			}

			// Delete every document; the leaf empties but the graph stays valid.
			for i := 0; i < n; i++ {
				key := fmt.Sprintf("k%03d", i)
				deleted, err := fs.Delete([]byte(key))
				if err != nil {
					t.Fatalf("Delete %s: %v", key, err)
				}
				if !deleted {
					t.Fatalf("Delete %s reported miss", key)
				}
			}
			if fs.Len() != 0 {
				t.Fatalf("Len after deleting all = %d, want 0", fs.Len())
			}
			// A fresh Put after emptying must still route and fill.
			if _, err := fs.Put([]byte("reborn"), []byte(`{"v":1}`)); err != nil {
				t.Fatalf("Put after empty: %v", err)
			}
			if got, ok, err := fs.AppendRaw(nil, []byte("reborn")); err != nil || !ok || string(got) != `{"v":1}` {
				t.Fatalf("AppendRaw reborn = (%q,%v,%v)", got, ok, err)
			}
			if err := fs.Close(); err != nil {
				t.Fatalf("Close after deletes: %v", err)
			}
			file.Close()
		})
	}
}

// TestCreateBuildsEmptyIndexedPrimaryStore verifies an indexed collection is
// created empty (an empty exact-index root beside the empty primary graph) and
// that the first indexed Put populates the posting tiles.
func TestCreateBuildsEmptyIndexedPrimaryStore(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty-indexed.vjs")
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	options := testFileStoreOptions()
	options.Indexes = []store.IndexDefinition{
		{Name: "status", Paths: []string{"/status"}},
	}
	fs, err := Create(file, options)
	if err != nil {
		t.Fatalf("Create empty indexed primary: %v", err)
	}
	if fs.Len() != 0 {
		t.Fatalf("empty indexed store Len = %d", fs.Len())
	}
	for i := 0; i < 8; i++ {
		status := "idle"
		if i%2 == 0 {
			status = "active"
		}
		doc := fmt.Sprintf(`{"id":%d,"status":%q}`, i, status)
		if _, err := fs.Put([]byte(fmt.Sprintf("k%02d", i)), []byte(doc)); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	if fs.Len() != 8 {
		t.Fatalf("Len after indexed puts = %d", fs.Len())
	}
	if err := fs.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}
