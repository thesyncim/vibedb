package query

import (
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
)

func durableTinCorpus(t *testing.T, indexes []store.IndexDefinition) *durable.Snapshot {
	t.Helper()
	file, err := os.CreateTemp(t.TempDir(), "tin-file-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })
	collection, err := durable.Create(file, durable.Options{
		Collection: store.Options{ChunkDocuments: 8},
		Indexes:    indexes,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = collection.Close() })
	docs := []struct{ key, body string }{
		{"d001", `{"id":"d001","body":"luxury goods"}`},
		{"d002", `{"id":"d002","body":"cheap goods"}`},
		{"d003", `{"id":"d003","body":"luxury watches"}`},
	}
	for _, doc := range docs {
		if _, err := collection.Put([]byte(doc.key), []byte(doc.body)); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = snapshot.Close() })
	return snapshot
}

func fileMatchIDs(t *testing.T, snapshot *durable.Snapshot, pattern string) []string {
	t.Helper()
	q := Select(Path("id")).Where(Match("body", pattern)).OrderBy("id", Asc)
	exec := Exec{}
	defer exec.Release()
	if err := q.RunInto(&exec, FromFile(snapshot)); err != nil {
		t.Fatal(err)
	}
	col, ok := exec.Result.Column("id")
	if !ok {
		t.Fatal("no id column")
	}
	var got []string
	for _, cell := range col.Cells {
		id, ok := cell.Text()
		if !ok {
			t.Fatalf("id cell = %s, want string", cell.JSON())
		}
		got = append(got, id)
	}
	return got
}

func TestFileMatchBindsDurableTin(t *testing.T) {
	snapshot := durableTinCorpus(t, []store.IndexDefinition{
		{Name: "body_tin", Paths: []string{"/body"}, Kind: store.IndexTin},
	})
	if got := fileMatchIDs(t, snapshot, "luxury"); !slices.Equal(
		got, []string{"d001", "d003"},
	) {
		t.Fatalf("luxury ids=%v", got)
	}
	if got := fileMatchIDs(t, snapshot, "luxury AND goods"); !slices.Equal(
		got, []string{"d001"},
	) {
		t.Fatalf("luxury AND goods ids=%v", got)
	}
	if got := fileMatchIDs(t, snapshot, "cheap"); !slices.Equal(
		got, []string{"d002"},
	) {
		t.Fatalf("cheap ids=%v", got)
	}
}

func TestFileMatchWithoutTinIndexIsAStatementError(t *testing.T) {
	snapshot := durableTinCorpus(t, nil)
	q := Select(Path("id")).Where(Match("body", "luxury"))
	exec := Exec{}
	defer exec.Release()
	if err := q.RunInto(&exec, FromFile(snapshot)); err == nil ||
		!strings.Contains(err.Error(), "requires a tin index") {
		t.Fatalf("error = %v, want missing-tin statement error", err)
	}
}

func TestFileMatchInvalidTINQLIsAStatementError(t *testing.T) {
	snapshot := durableTinCorpus(t, []store.IndexDefinition{
		{Name: "body_tin", Paths: []string{"/body"}, Kind: store.IndexTin},
	})
	q := Select(Path("id")).Where(Match("body", "((("))
	exec := Exec{}
	defer exec.Release()
	if err := q.RunInto(&exec, FromFile(snapshot)); err == nil ||
		!strings.Contains(err.Error(), "invalid TINQL") {
		t.Fatalf("error = %v, want invalid-TINQL statement error", err)
	}
}
