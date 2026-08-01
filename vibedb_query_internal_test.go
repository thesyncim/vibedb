package vibedb

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/query"
)

func TestSessionDurableQueryUsesExactIndex(t *testing.T) {
	db, err := Open(
		filepath.Join(t.TempDir(), "indexed-session.vdb"),
		WithDurability(Buffered),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	collection := db.Collection("events")
	for i := range 128 {
		status := "inactive"
		if i == 73 {
			status = "active"
		}
		if _, err := collection.Put(
			fmt.Sprintf("event:%03d", i),
			[]byte(fmt.Sprintf(`{"status":%q,"n":%d}`, status, i)),
		); err != nil {
			t.Fatal(err)
		}
	}
	if err := collection.CreateIndex("by_status", "/status"); err != nil {
		t.Fatal(err)
	}
	compiled := query.Select(query.Path("n")).
		Where(query.Cmp("status", query.Eq, "active"))
	session := collection.NewSession()
	defer session.Release()
	result, err := session.Run(compiled)
	if err != nil {
		t.Fatal(err)
	}
	if result.RowCount != 1 {
		t.Fatalf("indexed result rows = %d, want 1", result.RowCount)
	}
	if !session.exec.Stats.IndexBounded || session.exec.Stats.IndexLookups == 0 {
		t.Fatalf("durable session did not bind exact index: %+v", session.exec.Stats)
	}
}
