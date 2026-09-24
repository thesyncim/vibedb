package vibedb_test

import (
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb"
)

// A durable tin declaration is catalog state: it must survive close and
// reopen, and search the reopened data without redeclaring.
func TestFacadeDurableTinIndexSurvivesReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.vdb")
	db, err := vibedb.Open(path, vibedb.WithDurability(vibedb.Durable))
	if err != nil {
		t.Fatal(err)
	}
	docs := db.Collection("docs")
	for key, body := range map[string]string{
		"one": `{"body":"luxury goods and vintage watches"}`, "two": `{"body":"cheap goods"}`,
	} {
		if _, err := docs.Put(key, []byte(body)); err != nil {
			t.Fatal(err)
		}
	}
	if err := docs.CreateTinIndex("body_tin", "/body"); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	for round := 0; round < 2; round++ {
		db, err = vibedb.Open(path, vibedb.WithDurability(vibedb.Durable))
		if err != nil {
			t.Fatalf("reopen %d: %v", round, err)
		}
		hits, err := db.Collection("docs").TinSearch("/body", "vintage", 10)
		if err != nil || len(hits) != 1 || hits[0].Key != "one" {
			t.Fatalf("reopen %d search = (%+v, %v)", round, hits, err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
