package query

import (
	"fmt"
	"os"
	"testing"

	"github.com/thesyncim/vibedb/store/durable"
)

func TestNullOrderingFileSpillAndGroups(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "null-order")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	fs, err := durable.Create(f, durable.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()
	var docs []string
	for i := range 600 {
		n := "null"
		if i%3 != 0 {
			n = fmt.Sprint(i % 17)
		}
		doc := fmt.Sprintf(`{"id":%d,"n":%s}`, i, n)
		docs = append(docs, doc)
		if _, err := fs.Put([]byte(fmt.Sprint(i)), []byte(doc)); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err := fs.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	segment := mustSegment(t, docs...)
	for _, direction := range []Direction{AscNullsLast, DescNullsFirst} {
		for _, grouped := range []bool{false, true} {
			q := Select(Path("id"), Path("n")).OrderBy("n", direction).OrderBy("id", Asc)
			if grouped {
				q.GroupBy("id", "n")
			}
			want, err := q.Run(FromSegment(segment))
			if err != nil {
				t.Fatal(err)
			}
			e := Exec{Options: ExecOptions{Workers: 2, BatchRows: 17, MemoryBytes: 64 << 10, SpillDirectory: t.TempDir()}}
			if err := q.RunInto(&e, FromFile(snapshot)); err != nil {
				t.Fatal(err)
			}
			if got := resultKey(e.Result); got != resultKey(want) {
				t.Fatal("file null ordering differs from segment ordering")
			}
			first := want.Columns[1].Cells[0].Kind() == TypeNull
			last := want.Columns[1].Cells[len(docs)-1].Kind() == TypeNull
			if first != (direction == DescNullsFirst) || last != (direction == AscNullsLast) {
				t.Fatalf("wrong null boundaries: %v/%v", first, last)
			}
			if e.Stats.SpillRuns == 0 {
				t.Fatal("test did not exercise the spill merge")
			}
		}
	}
}
