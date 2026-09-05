package query

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
)

func TestFileProjectionBorrowsLargeDictionaryReview(t *testing.T) {
	builder, err := store.NewBuilder(store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	text := strings.Repeat("dictionary value ", 512)
	for row := range 4 {
		key := fmt.Sprintf("%05d", row)
		if err := builder.Append(key, fmt.Appendf(nil, `{"id":%q,"text":%q}`, key, text)); err != nil {
			t.Fatal(err)
		}
	}
	built, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.CreateTemp(t.TempDir(), "dictionary-review-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	opts := durable.Options{InlineValueBytes: 16 << 10}
	if _, err := durable.CreateFromPrimary(built, file, opts); err != nil {
		t.Fatal(err)
	}
	collection, err := durable.Open(file, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	span := NewFileRangeSource([]byte("00000"), []byte("00004"), false)
	span.BindPrimaryOrder("/id")
	span.BindPrimaryPredicate("/id")
	query := Select(Path("/text")).
		Where(And(Cmp("/id", Ge, "00000"), Cmp("/id", Lt, "00004"))).
		OrderBy("/id", Asc).Limit(2)
	var execution Exec
	defer execution.Release()
	execution.Options = ExecOptions{Workers: 1, MemoryBytes: 64 << 10}
	if err := query.RunInto(&execution, FromFileRange(snapshot, &span)); err != nil {
		t.Fatal(err)
	}
	if execution.Stats.ProjectedRows != 2 || execution.Result.RowCount != 2 {
		t.Fatalf("large dictionary silently fell back: %+v", execution.Stats)
	}
	if cap(execution.file.small.projectionValues) > fileProjectionValueBytes {
		t.Fatal("borrowed dictionary inflated projection scratch")
	}
	if got, want := len(execution.Result.fileData), 2*(len(text)+2); got != want {
		t.Fatalf("owned %d bytes, want %d", got, want)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	for _, cell := range execution.Result.Columns[0].Cells {
		if got, ok := cell.Text(); !ok || got != text {
			t.Fatal("dictionary text did not survive snapshot close")
		}
	}
}
