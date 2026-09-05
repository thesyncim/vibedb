package query

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
)

func TestNativeIntegerBudgetReview(t *testing.T) {
	values := []int64{-1 << 63, 1<<63 - 1, -1, 0, 1}
	for power := int64(10); power <= 1_000_000_000_000_000_000; power *= 10 {
		values = append(values, power-1, power, power+1, -power+1, -power, -power-1)
		if power == 1_000_000_000_000_000_000 {
			break
		}
	}
	for _, value := range values {
		cell := Cell{kind: TypeNumber, flag: cellInteger, word: uint64(value)}
		want := strconv.FormatInt(value, 10)
		if resultCellPayloadBytes(cell) != int64(len(want)) || projectedCellPayloadBytes(cell) != int64(len(want)) {
			t.Fatalf("native budget for %d differs from exact spelling length %d", value, len(want))
		}
		var result Result
		owned := result.ownProjectedCell(cell)
		if len(result.fileData) != 0 || string(owned.JSON()) != want {
			t.Fatalf("native ownership formatted or changed %d", value)
		}
	}
}

func TestNativeScalarComparisonReview(t *testing.T) {
	for _, number := range []int64{-9007199254741120, -127, 0, 127, 9007199254741120} {
		for _, suffix := range []string{".0", ".1", ".000000000000000000000000000001"} {
			raw := fmt.Appendf(nil, "%d", number)
			decimal := append(append([]byte(nil), raw...), suffix...)
			native := scalar{kind: kindNumber, isInt: true, ival: number}
			encoded := scalar{kind: kindNumber, num: decimal}
			want := compareNumberBytes(raw, decimal)
			if got := compareScalar(native, encoded); got != want {
				t.Errorf("native %d vs %s = %d, want %d", number, decimal, got, want)
			}
			if got := compareScalar(encoded, native); got != -want {
				t.Errorf("%s vs native %d = %d, want %d", decimal, number, got, -want)
			}
		}
	}
}

// This scalar corpus checks exact output and native execution across covered
// and residual scalar predicates; containment remains on its raw-aware lane.
func TestProjectedNativeReview(t *testing.T) {
	builder, err := store.NewBuilder(store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	for row := range 1024 {
		key := fmt.Sprintf("%04d", row)
		number := (row * 7919) % 1024
		doc := fmt.Appendf(nil,
			`{"id":%q,"n":%d,"wide":%d,"negative":%d,"flag":%t,"text":%q,"decimal":1.000e+0,"monotone":%d,"unused":%q}`,
			key, number, int64(9007199254740993)+int64(number), -int64(9007199254740993)-int64(number),
			row%3 == 0, fmt.Sprintf("group-%d", row%7), row, strings.Repeat("unused payload ", 64))
		if err := builder.Append(key, doc); err != nil {
			t.Fatal(err)
		}
	}
	built, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.CreateTemp(t.TempDir(), "native-review-*")
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
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	reference := projectedReviewHeap(t, snapshot)
	columns := []Column{Path("/n"), Path("/text"), Path("/wide"), Path("/negative"), Path("/flag"), Path("/decimal"), Path("/missing"), Path("/n"), Path("/monotone")}
	cases := []struct {
		name      string
		predicate Predicate
	}{
		{"covered", Predicate{}},
		{"native-integer", Cmp("/n", Ge, 127)},
		{"decimal-equality", Cmp("/n", Eq, Number("127.000e0"))},
		{"fraction-between-integers", Cmp("/n", Gt, Number("127.0000000000000000001"))},
		{"fraction-no-match", Cmp("/n", Eq, Number("127.0000000000000000001"))},
		{"wide-exact", Cmp("/wide", Eq, Number("9007199254741120.0"))},
		{"wide-fraction", Cmp("/wide", Gt, Number("9007199254741120.1"))},
		{"negative-exact", Cmp("/negative", Eq, Number("-9007199254741120.0"))},
		{"negative-fraction", Cmp("/negative", Lt, Number("-9007199254741120.1"))},
		{"large-positive-exponent", Cmp("/n", Lt, Number("1e1000"))},
		{"large-negative-exponent", Cmp("/n", Gt, Number("1e-1000"))},
		{"mixed-membership", In("/n", 127, Number("128.0"), Number("129.00000001"), 130)},
		{"native-boolean", Cmp("/flag", Eq, true)},
		{"dictionary-string", Like("/text", "%3")},
		{"contains-number", Contains("/n", "127")},
		{"contains-boolean", Contains("/flag", "true")},
		{"contains-string", Contains("/text", `"group-3"`)},
		{"shared-filter-output", And(Cmp("/n", Ge, 127), Cmp("/wide", Lt, Number("9007199254741500.0")), Cmp("/flag", Eq, true))},
		{"missing", Exists("/missing")},
	}
	var execution Exec
	defer execution.Release()
	for _, start := range []string{"0030", "0063", "0255"} {
		for _, limit := range []int{1, 32, 256} {
			for _, test := range cases {
				t.Run(fmt.Sprintf("%s/%d/%s", start, limit, test.name), func(t *testing.T) {
					predicate := And(Cmp("/id", Gt, start), Cmp("/id", Lt, "0999"))
					if test.name != "covered" {
						predicate = And(predicate, test.predicate)
					}
					statement := Select(columns...).Where(predicate).OrderBy("/id", Asc).Limit(limit)
					want, err := statement.Run(FromSegment(reference))
					if err != nil {
						t.Fatal(err)
					}
					span := NewFileRangeSource([]byte(start), []byte("0999"), true)
					span.BindPrimaryOrder("/id")
					if test.name == "covered" {
						span.BindPrimaryPredicate("/id")
					}
					execution.Options = ExecOptions{Workers: 1, MemoryBytes: 64 << 10}
					if err := statement.RunInto(&execution, FromFileRange(snapshot, &span)); err != nil {
						t.Fatal(err)
					}
					projectedReviewEqual(t, execution.Result, want)
					if test.name == "covered" && execution.Result.RowCount != 0 && len(execution.Result.Columns[0].Cells[0].raw) != 0 {
						t.Fatal("fixture did not exercise a native integer output")
					}
					nativeRequired := !strings.HasPrefix(test.name, "contains-")
					if nativeRequired && (execution.file.small == nil || execution.file.small.docs.Len() != 0) {
						t.Fatal("scalar query reconstructed a Segment")
					}
					if nativeRequired {
						fields := 9 // eight unique output paths plus the residual id predicate
						if test.name == "covered" {
							fields = 8
						}
						if len(execution.file.small.projectionPaths) != fields {
							t.Fatalf("selected %d paths, want %d unique required paths", len(execution.file.small.projectionPaths), fields)
						}
					}
					if nativeRequired && (execution.Stats.ProjectedRows != uint64(execution.Result.RowCount)) {
						t.Fatalf("native query silently fell back: %+v", execution.Stats)
					}
					if test.name == "covered" && execution.Stats.RowsScanned != uint64(want.RowCount) {
						t.Fatalf("covered scan read %d rows for %d results", execution.Stats.RowsScanned, want.RowCount)
					}
				})
			}
		}
	}
	span := NewFileRangeSource([]byte("0063"), []byte("0999"), true)
	span.BindPrimaryOrder("/id")
	statement := Select(columns...).Where(Cmp("/flag", Eq, true)).OrderBy("/id", Asc).Limit(32)
	for _, options := range []ExecOptions{{ResultRows: 1}, {ResultBytes: 128}} {
		execution.Options = options
		if err := statement.RunInto(&execution, FromFileRange(snapshot, &span)); !errors.Is(err, ErrResultBudget) {
			t.Fatalf("budget returned %v, want ErrResultBudget", err)
		}
		if execution.Result.RowCount != 0 {
			t.Fatal("budget failure published partial output")
		}
	}
	execution.Options = ExecOptions{Workers: 1, MemoryBytes: 64 << 10}
	if err := statement.RunInto(&execution, FromFileRange(snapshot, &span)); err != nil {
		t.Fatalf("reuse after result budget failure: %v", err)
	}
	want := resultKey(execution.Result)
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if got := resultKey(execution.Result); got != want {
		t.Fatal("result changed after snapshot close")
	}
}
