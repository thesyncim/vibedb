package query

import (
	"fmt"
	"os"
	"slices"
	"strconv"
	"testing"

	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
)

// TestSQLMatchFilePrunedAgreesWithTinSearch proves durable ==> pruning is
// recall-identical to the mask-free Go API: for every query shape, the
// pruned file scan returns exactly the TinSearch key set, with pruning
// engaged (IndexBounded) and one candidate row per hit. TinSearch scores
// the generation-pinned index without masks, so any ordinal-to-slot
// misalignment — a missed hit — fails here. The second generation mixes
// deletes and inserts to prove the mapping under churn.
func TestSQLMatchFilePrunedAgreesWithTinSearch(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "tin-file-prune-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })
	collection, err := durable.Create(file, durable.Options{
		Indexes: []store.IndexDefinition{
			{Name: "body_tin", Paths: []string{"/body"}, Kind: store.IndexTin},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = collection.Close() })
	put := func(key, body string) {
		t.Helper()
		doc := `{"id":` + strconv.Quote(key) + `,"body":` + strconv.Quote(body) + `}`
		if _, err := collection.Put([]byte(key), []byte(doc)); err != nil {
			t.Fatal(err)
		}
	}
	for i := range 512 {
		body := "filler words padding out this document body"
		switch {
		case i%8 == 0:
			body = "luxury vintage watches " + body
		case i%8 == 1:
			body = "vintage luxury goods " + body
		}
		put(fmt.Sprintf("d%04d", i), body)
	}
	queries := []string{
		`luxury`,
		`"vintage watches"`,
		`luxury AND vintage`,
		`luxury OR nonexistenttermxyz`,
		`luxury AND NOT watches`,
		`luxury NEAR/3 vintage`,
		`lux*`,
		`luxury~1`,
	}
	check := func(tag string) {
		t.Helper()
		snapshot, err := collection.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		defer snapshot.Close()
		for _, tinql := range queries {
			q := Select(Path("id")).Where(Match("body", tinql)).OrderBy("id", Asc)
			exec := Exec{}
			if err := q.RunInto(&exec, FromFile(snapshot)); err != nil {
				t.Fatalf("%s %q: %v", tag, tinql, err)
			}
			col, ok := exec.Result.Column("id")
			if !ok {
				t.Fatalf("%s %q: no id column", tag, tinql)
			}
			var got []string
			for _, cell := range col.Cells {
				text, ok := cell.Text()
				if !ok {
					t.Fatalf("%s %q: non-string id cell", tag, tinql)
				}
				got = append(got, text)
			}
			if !exec.Stats.IndexBounded {
				t.Fatalf("%s %q: pruning did not engage", tag, tinql)
			}
			if exec.Stats.CandidateRows != uint64(len(got)) {
				t.Fatalf("%s %q: candidates %d != hits %d", tag, tinql, exec.Stats.CandidateRows, len(got))
			}
			hits, err := collection.TinSearch(snapshot, "/body", tinql, 10000)
			if err != nil {
				t.Fatalf("%s %q: TinSearch: %v", tag, tinql, err)
			}
			var want []string
			for _, hit := range hits {
				want = append(want, hit.Key)
			}
			slices.Sort(want)
			if !slices.Equal(got, want) {
				t.Fatalf("%s %q:\n got %q\nwant %q", tag, tinql, got, want)
			}
			exec.Release()
		}
	}
	check("gen1")
	// Churn: delete a hit and a non-hit, insert a fresh hit and a fresh
	// non-hit. The next generation rebuilds index and live map together.
	if _, err := collection.Delete([]byte("d0000")); err != nil {
		t.Fatal(err)
	}
	if _, err := collection.Delete([]byte("d0002")); err != nil {
		t.Fatal(err)
	}
	put("d9998", "brand new luxury arrival")
	put("d9999", "brand new ordinary arrival")
	check("gen2")
}
