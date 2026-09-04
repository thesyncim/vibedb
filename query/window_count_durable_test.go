package query

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
)

func TestSQLWindowCountExclusionsHeapAndDurable(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "window-count-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	collection, err := durable.Create(file, durable.Options{Collection: store.Options{ChunkDocuments: 8}})
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()
	segment := &store.Segment{}
	const rows = 72
	for i := range rows {
		value := "7"
		if i%5 == 0 {
			value = "null"
		}
		doc := fmt.Appendf(nil, `{"id":%d,"team":%d,"score":%d,"value":%s,"ok":%t}`, i, i%2, i%4, value, i%3 != 0)
		if _, err := segment.Append(doc); err != nil {
			t.Fatal(err)
		}
		if _, err := collection.Put([]byte(fmt.Sprintf("k%03d", i)), doc); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	for _, exclusion := range []string{"GROUP", "TIES"} {
		statement, err := PrepareStatement(fmt.Sprintf(`SELECT id,
   COUNT(*) OVER w AS total,
   COUNT(value) OVER w AS eligible
   FROM events WINDOW w AS (PARTITION BY team ORDER BY score
   ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE %s)
   ORDER BY id`, exclusion))
		if err != nil {
			t.Fatal(err)
		}
		defer statement.Release()
		var want strings.Builder
		for i := range rows {
			total, eligible := 0, 0
			for j := range rows {
				if i%2 != j%2 || i%4 == j%4 && !(exclusion == "TIES" && i == j) {
					continue
				}
				total++
				if j%5 != 0 {
					eligible++
				}
			}
			fmt.Fprintf(&want, "3:%d|3:%d|3:%d|\n", i, total, eligible)
		}
		for _, source := range []struct {
			name  string
			value Source
		}{
			{"heap", FromSegment(segment)}, {"durable", FromFile(snapshot)},
		} {
			t.Run(exclusion+"/"+source.name, func(t *testing.T) {
				got := runStatement(t, statement, source.value)
				if body := strings.SplitN(got, "\n", 2)[1]; body != want.String() {
					t.Fatalf("rows:\n%s\nwant:\n%s", body, want.String())
				}
			})
		}
	}
}
