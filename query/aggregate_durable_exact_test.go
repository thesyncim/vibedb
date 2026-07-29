package query

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
)

func TestExactAggregatesSurviveDurableParallelSpill(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "exact-aggregate-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	collection, err := durable.Create(file, durable.Options{
		Collection: store.Options{ChunkDocuments: 8},
	})
	if err != nil {
		t.Fatal(err)
	}

	segment := &store.Segment{}
	const groups = 128
	for group := range groups {
		label := fmt.Sprintf("group-%03d-%s", group, strings.Repeat("x", 1024))
		for row, number := range []string{"9007199254740993", "-9007199254740992"} {
			document := []byte(fmt.Sprintf(`{"g":%q,"n":%s}`, label, number))
			if _, err := segment.Append(document); err != nil {
				t.Fatal(err)
			}
			if _, err := collection.Put(
				[]byte(fmt.Sprintf("k-%03d-%d", group, row)), document,
			); err != nil {
				t.Fatal(err)
			}
		}
	}
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}

	query := Select(
		Path("g"), Sum("n"), Avg("n"), Min("n"), Max("n"),
	).GroupBy("g").OrderBy("g", Asc)
	want, err := query.Run(FromSegment(segment))
	if err != nil {
		t.Fatal(err)
	}
	exec := Exec{Options: ExecOptions{
		Workers: 4, BatchRows: 7, BatchBytes: 16 << 10,
		MemoryBytes: 64 << 10, AggregateBytes: 32 << 20,
		SpillDirectory: t.TempDir(),
	}}
	if err := query.RunInto(&exec, FromFile(snapshot)); err != nil {
		t.Fatal(err)
	}
	if exec.Stats.Workers != 4 || exec.Stats.Batches < 2 ||
		exec.Stats.SpillRuns == 0 || exec.Stats.SpilledBytes == 0 {
		t.Fatalf("parallel spill path not exercised: %+v", exec.Stats)
	}

	// Durable results own every byte, including exact aggregate encodings.
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}
	assertResultJSONEqual(t, exec.Result, want)
	assertColumnJSON(t, exec.Result, "sum(n)", repeatString("1", groups)...)
	assertColumnJSON(t, exec.Result, "avg(n)", repeatString("0.5", groups)...)
	assertColumnJSON(t, exec.Result, "min(n)", repeatString("-9007199254740992", groups)...)
	assertColumnJSON(t, exec.Result, "max(n)", repeatString("9007199254740993", groups)...)
}

func TestCorruptExactAggregateSpillReturnsError(t *testing.T) {
	tests := []diskNumberAcc{
		{SumSet: true, SumCoeff: "not-a-number", SumScale: "0"},
		{SumSet: true, SumCoeff: "1", SumScale: "not-an-exponent"},
	}
	for _, number := range tests {
		var budget aggregateBudget
		budget.begin(defaultAggregateBytes)
		if _, err := aggregateFromDisk(
			diskAgg{Num: &number}, &budget,
		); !errors.Is(err, ErrSpillCorrupt) {
			t.Fatalf(
				"corrupt aggregate spill error = %v, want ErrSpillCorrupt",
				err,
			)
		}
	}
}

func repeatString(value string, n int) []string {
	values := make([]string, n)
	for i := range values {
		values[i] = value
	}
	return values
}

func assertResultJSONEqual(t testing.TB, got, want Result) {
	t.Helper()
	if got.RowCount != want.RowCount || len(got.Columns) != len(want.Columns) {
		t.Fatalf("result shape = %d rows/%d columns, want %d/%d",
			got.RowCount, len(got.Columns), want.RowCount, len(want.Columns))
	}
	for column := range want.Columns {
		if got.Columns[column].Header != want.Columns[column].Header {
			t.Fatalf("column %d header = %q, want %q",
				column, got.Columns[column].Header, want.Columns[column].Header)
		}
		for row := range want.Columns[column].Cells {
			gotJSON := got.Columns[column].Cells[row].JSON()
			wantJSON := want.Columns[column].Cells[row].JSON()
			if !equalBytes(gotJSON, wantJSON) {
				t.Fatalf("column %q row %d = %.160s, want %.160s",
					got.Columns[column].Header, row, gotJSON, wantJSON)
			}
		}
	}
}
