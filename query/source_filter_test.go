package query

import (
	"fmt"
	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
	"os"
	"testing"
)

type evenFileKeyFilter struct{ none bool }

func (f *evenFileKeyFilter) Keep(key []byte) bool { return !f.none && len(key) == 2 && key[1]%2 == 0 }

func TestFileFilterPrecedesCountOrderLimitAndIndexedPredicate(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "filter")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	c, err := durable.Create(f, durable.Options{Collection: store.Options{ChunkDocuments: 2}, Indexes: []store.IndexDefinition{{Name: "active", Paths: []string{"/active"}}}})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	for i := 0; i < 6; i++ {
		if _, err := c.Put([]byte(fmt.Sprintf("k%d", i)), []byte(fmt.Sprintf(`{"id":%d,"active":true}`, i))); err != nil {
			t.Fatal(err)
		}
	}
	snap, err := c.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Close()
	filter := evenFileKeyFilter{}
	source := NewFileFilterSource(&filter)
	for _, test := range []struct{ sql, want string }{
		{`SELECT COUNT(*) FROM docs`, "3"},
		{`SELECT COUNT(*) FROM docs WHERE active = true`, "3"},
		{`SELECT id FROM docs ORDER BY id DESC LIMIT 1`, "4"},
		{`SELECT SUM(id) FROM docs`, "6"},
	} {
		t.Run(test.sql, func(t *testing.T) {
			statement, err := PrepareStatement(test.sql)
			if err != nil {
				t.Fatal(err)
			}
			defer statement.Release()
			var exec Exec
			defer exec.Release()
			cursor, err := statement.RunInto(&exec, FromFileFiltered(snap, &source), nil)
			if err != nil {
				t.Fatal(err)
			}
			if !cursor.Next() || string(cursor.Cell(0).AppendJSON(nil)) != test.want {
				t.Fatalf("unexpected result %v", exec.Result)
			}
			if cursor.Next() {
				t.Fatal("extra row")
			}
		})
	}
	filter.none = true
	var exec Exec
	defer exec.Release()
	q := Select(Count())
	if err := q.RunInto(&exec, FromFileFiltered(snap, &source)); err != nil {
		t.Fatal(err)
	}
	if got := string(exec.Result.Columns[0].Cells[0].AppendJSON(nil)); got != "0" {
		t.Fatalf("empty filtered count=%s", got)
	}
}
