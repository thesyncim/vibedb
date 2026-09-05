package query

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
)

// Compare storage projection with the general heap executor over the exact
// stored documents, including fields whose type changes between compact shapes.
// Reusing one Exec across all queries also exercises scratch left by fallbacks.
func TestProjectedRangeReviewDifferential(t *testing.T) {
	builder, err := store.NewBuilder(store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	fields := []string{
		`"value":1,"text":"plain","nested":{"x":3}`,
		`"value":1.000e+0,"text":"quote\"slash\\newline\n"`,
		`"value":9007199254740993,"text":"\u00e9\ud83d\ude00"`,
		`"value":-0.0,"text":""`,
		`"value":null,"text":null`,
		`"other":true`,
		`"value":false,"text":true`,
		`"value":"123","text":123`,
		`"value":[1,null,"x"],"text":{"x":1}`,
		`"value":{"a":1,"a":2},"nested":{"x":1},"nested":{"x":2}`,
		`"value":1,"value":2,"text":"first","text":"last"`,
		`"value":1,"\u0076alue":3,"te\u0078t":"escaped key"`,
		`"value":{"a":1,"b":2},"value":3,"text":"container then scalar"`,
		`"value":[1,2,3],"value":4,"text":"array then scalar"`,
		`"nested":{"x":1,"y":[2,3]},"nested":{"x":4},"value":5`,
		`"a/b":4,"til~de":5,"array":[{"x":6}]`,
	}
	for row := range 384 {
		key := fmt.Sprintf("%04d", row)
		raw := fmt.Appendf(nil, `{"id":%q,%s`, key, fields[row%len(fields)])
		raw = append(raw, '}')
		if err := builder.Append(key, raw); err != nil {
			t.Fatal(err)
		}
	}
	built, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.CreateTemp(t.TempDir(), "projected-review-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	opts := durable.Options{InlineValueBytes: 4096}
	if _, err := durable.CreateFromPrimary(built, file, opts); err != nil {
		t.Fatal(err)
	}
	collection, err := durable.Open(file, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()
	// The bulk builder accepts inline rows only. Install the overflow through
	// ordinary mutation before taking either reference or tested snapshots.
	overflow := fmt.Appendf(nil, `{"id":"0097",%s,"padding":%q}`,
		fields[97%len(fields)], strings.Repeat("wide-payload-", 4096))
	if _, err := collection.Put([]byte("0097"), overflow); err != nil {
		t.Fatal(err)
	}
	old, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	oldDocs := projectedReviewHeap(t, old)
	if _, err := collection.Put([]byte("0050"), []byte(`{"id":"0050","value":777,"text":"changed"}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := collection.Delete([]byte("0051")); err != nil {
		t.Fatal(err)
	}
	current, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer current.Close()
	currentDocs := projectedReviewHeap(t, current)
	var exec Exec
	defer exec.Release()
	for _, cut := range []struct {
		name     string
		snapshot *durable.Snapshot
		heap     *store.Segment
	}{{"old", old, oldDocs}, {"current", current, currentDocs}, {"old-again", old, oldDocs}} {
		for projection, paths := range [][]string{
			{"/value"}, {"/text", "/value", "/id"}, {"/absent"},
			{"/value", "/value"}, {"/nested/x"}, {"/array/0/x"},
			{"/a~1b", "/til~0de"}, {""}, {"/id"},
		} {
			for _, limit := range []int{1, 17, 128, 256} {
				for mode, residual := range []Predicate{
					{}, Cmp("/value", Eq, Number("1e0")),
					Or(IsNull("/value"), Cmp("/value", Eq, "123")),
					And(Exists("/text"), Not(IsNull("/text"))),
					Or(Like("/text", "%last%"), In("/value", 2, 3, 777)),
				} {
					t.Run(fmt.Sprintf("%s/projection=%d/limit=%d/predicate=%d", cut.name, projection, limit, mode), func(t *testing.T) {
						columns := make([]Column, len(paths))
						for i, path := range paths {
							columns[i] = Path(path)
						}
						predicate := And(Cmp("/id", Gt, "0048"), Cmp("/id", Lt, "0300"))
						if mode != 0 {
							predicate = And(predicate, residual)
						}
						q := Select(columns...).Where(predicate).OrderBy("/id", Asc).Limit(limit)
						want, err := q.Run(FromSegment(cut.heap))
						if err != nil {
							t.Fatal(err)
						}
						span := NewFileRangeSource([]byte("0048"), []byte("0300"), true)
						span.BindPrimaryOrder("/id")
						if mode == 0 {
							span.BindPrimaryPredicate("/id")
						}
						exec.Options = ExecOptions{Workers: 1, BatchRows: 7, BatchBytes: 4096}
						if err := q.RunInto(&exec, FromFileRange(cut.snapshot, &span)); err != nil {
							t.Fatal(err)
						}
						projectedReviewEqual(t, exec.Result, want)
						if mode == 0 && exec.Stats.RowsScanned != uint64(want.RowCount) {
							t.Fatalf("scanned %d for %d results", exec.Stats.RowsScanned, want.RowCount)
						}
					})
				}
			}
		}
	}
}

func projectedReviewHeap(t *testing.T, snapshot *durable.Snapshot) *store.Segment {
	t.Helper()
	docs := &store.Segment{}
	if err := snapshot.RangeRaw(func(_, raw []byte) error {
		_, err := docs.Append(raw)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return docs
}

func projectedReviewEqual(t *testing.T, got, want Result) {
	t.Helper()
	if got.RowCount != want.RowCount || len(got.Columns) != len(want.Columns) {
		t.Fatalf("shape got=%d/%d want=%d/%d", got.RowCount, len(got.Columns), want.RowCount, len(want.Columns))
	}
	for col := range want.Columns {
		for row, expected := range want.Columns[col].Cells {
			actual := got.Columns[col].Cells[row]
			actualText, actualString := actual.Text()
			expectedText, expectedString := expected.Text()
			if actual.Kind() != expected.Kind() || !bytes.Equal(actual.JSON(), expected.JSON()) ||
				actual.flag&cellMissing != expected.flag&cellMissing ||
				actualString != expectedString || actualText != expectedText {
				t.Fatalf("row=%d col=%d got=%s kind=%v missing=%t text=%q want=%s kind=%v missing=%t text=%q",
					row, col, actual.JSON(), actual.Kind(), actual.flag&cellMissing != 0, actualText,
					expected.JSON(), expected.Kind(), expected.flag&cellMissing != 0, expectedText)
			}
		}
	}
}
