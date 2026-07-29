package query

import (
	"testing"

	"github.com/thesyncim/vibedb/store"
)

func TestPreparedQueryUnifiesBuilderAndSQL(t *testing.T) {
	docs := [][]byte{
		[]byte(`{"bucket":"a","score":3,"live":true}`),
		[]byte(`{"bucket":"b","score":7,"live":false}`),
		[]byte(`{"bucket":"a","score":5,"live":true}`),
	}
	set := buildSegment(t, docs, storageMode{"", true, true})
	builder := Select(Path("bucket"), Count(), Sum("score")).
		Where(Cmp("live", Eq, true)).
		GroupBy("bucket").
		OrderBy("bucket", Asc)
	if err := builder.Prepare(); err != nil {
		t.Fatal(err)
	}
	statement, err := PrepareStatement(`SELECT bucket, COUNT(*), SUM(score) FROM docs WHERE live = true GROUP BY bucket ORDER BY bucket`)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()

	gotBuilder, err := builder.Run(FromSegment(set))
	if err != nil {
		t.Fatal(err)
	}
	var sqlExec Exec
	cursor, err := statement.RunInto(&sqlExec, FromSegment(set), nil)
	if err != nil {
		t.Fatal(err)
	}
	for cursor.Next() {
	}
	gotSQL := sqlExec.Result
	if got, want := dumpResult(gotBuilder), dumpResult(gotSQL); got != want {
		t.Fatalf("prepared builder and SQL differ:\n%s\n%s", got, want)
	}
	if gotBuilder.Columns[1].Cells[0].raw != nil {
		t.Fatal("COUNT unexpectedly retained a formatted numeric buffer")
	}
	if raw := gotBuilder.Columns[2].Cells[0].raw; string(raw) != "8" {
		t.Fatalf("exact SUM raw encoding = %q, want 8", raw)
	}
	for col := 1; col < len(gotBuilder.Columns); col++ {
		buf := make([]byte, 0, 32)
		if got := gotBuilder.Columns[col].Cells[0].AppendJSON(buf); len(got) == 0 {
			t.Fatalf("aggregate column %d appended no JSON", col)
		}
	}

	schema := builder.AppendSchema(make([]OutputColumn, 0, 3))
	want := []OutputColumn{
		{Header: "bucket", Ordinal: 0, Reduction: ReductionNone, Type: TypeAny},
		{Header: "count(*)", Ordinal: 1, Reduction: ReductionCount, Type: TypeNumber},
		{Header: "sum(score)", Ordinal: 2, Reduction: ReductionSum, Type: TypeNumber},
	}
	if len(schema) != len(want) {
		t.Fatalf("schema length = %d, want %d", len(schema), len(want))
	}
	for i := range want {
		if schema[i] != want[i] {
			t.Fatalf("schema[%d] = %+v, want %+v", i, schema[i], want[i])
		}
	}

	allocs := testing.AllocsPerRun(100, func() {
		schema = builder.AppendSchema(schema[:0])
	})
	if allocs != 0 {
		t.Fatalf("warmed AppendSchema allocated %.2f times, want 0", allocs)
	}
}

// TestUncompilableQueryFailsEagerly pins the eager-failure contract Prepare
// exists for: a query that cannot compile reports the same error from Prepare,
// from AppendSchema (as an absent schema), and from execution, so a caller who
// prepares never learns about a malformed query from inside a hot loop.
func TestUncompilableQueryFailsEagerly(t *testing.T) {
	q := Select(Path("bucket"), Count()) // a projection mixed with an aggregate
	if err := q.Prepare(); err == nil {
		t.Fatal("Prepare accepted a projection mixed with an aggregate")
	}
	if got := q.AppendSchema(nil); got != nil {
		t.Fatalf("uncompilable query schema = %+v, want nil", got)
	}
	var e Exec
	if err := q.RunInto(&e, FromSegment(&store.Segment{})); err == nil {
		t.Fatal("RunInto executed an uncompilable query")
	}
}
