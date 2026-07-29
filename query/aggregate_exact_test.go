package query

import (
	"errors"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/store"
)

func TestExactNumericAggregatesBeyondFloat64(t *testing.T) {
	segment := aggregateSegment(t,
		`{"n":9007199254740993}`,
		`{"n":1}`,
		`{"n":-9007199254740994}`,
	)
	result, err := Select(Sum("n"), Min("n"), Max("n")).Run(FromSegment(segment))
	if err != nil {
		t.Fatal(err)
	}
	assertAggregateJSON(t, result, "sum(n)", "0")
	assertAggregateJSON(t, result, "min(n)", "-9007199254740994")
	assertAggregateJSON(t, result, "max(n)", "9007199254740993")
}

func TestExactSumCommonDecimals(t *testing.T) {
	segment := aggregateSegment(t,
		`{"n":0.1}`,
		`{"n":0.2}`,
		`{"n":9007199254740993.25}`,
		`{"n":-9007199254740993.00}`,
	)
	result, err := Select(Sum("n")).Run(FromSegment(segment))
	if err != nil {
		t.Fatal(err)
	}
	assertAggregateJSON(t, result, "sum(n)", "0.55")
}

func TestExactAveragePolicy(t *testing.T) {
	t.Run("finite result stays exact", func(t *testing.T) {
		result, err := Select(Avg("n")).Run(FromSegment(aggregateSegment(t,
			`{"n":1}`, `{"n":2}`,
		)))
		if err != nil {
			t.Fatal(err)
		}
		assertAggregateJSON(t, result, "avg(n)", "1.5")
	})

	t.Run("nonterminating result is 34 digit half even", func(t *testing.T) {
		result, err := Select(Avg("n")).Run(FromSegment(aggregateSegment(t,
			`{"n":1}`, `{"n":0}`, `{"n":0}`,
		)))
		if err != nil {
			t.Fatal(err)
		}
		assertAggregateJSON(t, result, "avg(n)", "0."+strings.Repeat("3", averageDigits))
	})

	for _, test := range []struct {
		name  string
		value string
		want  string
	}{
		{
			name:  "halfway retained digit even rounds down",
			value: "12345678901234567890123456789012345",
			want:  "12345678901234567890123456789012340",
		},
		{
			name:  "halfway retained digit odd rounds up",
			value: "12345678901234567890123456789012355",
			want:  "12345678901234567890123456789012360",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			result, err := Select(Avg("n")).Run(FromSegment(aggregateSegment(t,
				`{"n":`+test.value+`}`, `{"n":`+test.value+`}`,
			)))
			if err != nil {
				t.Fatal(err)
			}
			assertAggregateJSON(t, result, "avg(n)", test.want)
		})
	}
}

func TestExactAggregateHugeExponent(t *testing.T) {
	exponent := "1" + strings.Repeat("0", 96)
	segment := aggregateSegment(t,
		`{"n":1e`+exponent+`}`,
		`{"n":2e`+exponent+`}`,
	)
	result, err := Select(Sum("n"), Min("n"), Max("n")).Run(FromSegment(segment))
	if err != nil {
		t.Fatal(err)
	}
	assertAggregateJSON(t, result, "sum(n)", "3e"+exponent)
	assertAggregateJSON(t, result, "min(n)", "1e"+exponent)
	assertAggregateJSON(t, result, "max(n)", "2e"+exponent)
	if value, ok := result.Columns[0].Cells[0].Float64(); ok || value != 0 {
		t.Fatalf("Float64(huge exact SUM) = (%v,%v), want (0,false)", value, ok)
	}
}

func TestWideProjectedNumberFloat64DeclinesOverflow(t *testing.T) {
	exponent := "1" + strings.Repeat("0", 96)
	result, err := Select(Path("n")).Run(FromSegment(aggregateSegment(
		t, `{"n":1e`+exponent+`}`,
	)))
	if err != nil {
		t.Fatal(err)
	}
	cell := result.Columns[0].Cells[0]
	if got := string(cell.JSON()); got != "1e"+exponent {
		t.Fatalf("projected JSON = %s", got)
	}
	if value, ok := cell.Float64(); ok || value != 0 {
		t.Fatalf("Float64(projected huge number) = (%v,%v), want (0,false)", value, ok)
	}
}

func TestExactAggregateBudgetRejectsUnboundedAlignment(t *testing.T) {
	exponent := "1" + strings.Repeat("0", 96)
	segment := aggregateSegment(t,
		`{"n":1e`+exponent+`}`,
		`{"n":1}`,
	)
	query := Select(Sum("n"))
	var exec Exec
	exec.Options.AggregateBytes = 4096
	err := query.RunInto(&exec, FromSegment(segment))
	if !errors.Is(err, ErrAggregateBudget) {
		t.Fatalf("RunInto error = %v, want ErrAggregateBudget", err)
	}
	var budgetErr *AggregateBudgetError
	if !errors.As(err, &budgetErr) || budgetErr.Limit != exec.Options.AggregateBytes {
		t.Fatalf("RunInto error = %#v, want typed limit %d", err, exec.Options.AggregateBytes)
	}
}

func TestExactAggregateBudgetIsWholeExecutionAcrossGroups(t *testing.T) {
	segment := aggregateSegment(t,
		`{"g":"a","n":1}`,
		`{"g":"b","n":2}`,
	)
	query := Select(Path("g"), Sum("n")).GroupBy("g")
	var exec Exec
	exec.Options.AggregateBytes = 1024
	err := query.RunInto(&exec, FromSegment(segment))
	if !errors.Is(err, ErrAggregateBudget) {
		t.Fatalf("grouped RunInto error = %v, want ErrAggregateBudget", err)
	}
	var budgetErr *AggregateBudgetError
	if !errors.As(err, &budgetErr) || budgetErr.Used == 0 {
		t.Fatalf("grouped RunInto error = %#v, want a charged whole-execution budget", err)
	}
}

func TestAggregateBudgetOverflowFailsWithPositiveRequest(t *testing.T) {
	budget := aggregateBudget{limit: int64(^uint64(0) >> 1), epoch: 1}
	budget.used.Store(17)
	var lease aggregateLease
	err := lease.reserve(&budget, -1)
	var budgetErr *AggregateBudgetError
	if !errors.As(err, &budgetErr) {
		t.Fatalf("overflow reserve error = %v, want AggregateBudgetError", err)
	}
	if budgetErr.Requested <= 0 || budgetErr.Used != 17 {
		t.Fatalf("overflow reserve accounting = %+v", budgetErr)
	}
}

func TestExactGroupedAggregates(t *testing.T) {
	segment := aggregateSegment(t,
		`{"g":"a","n":9007199254740993}`,
		`{"g":"b","n":-9007199254740994}`,
		`{"g":"a","n":7}`,
		`{"g":"b","n":-6}`,
	)
	result, err := Select(Path("g"), Sum("n"), Avg("n"), Min("n"), Max("n")).
		GroupBy("g").
		OrderBy("g", Asc).
		Run(FromSegment(segment))
	if err != nil {
		t.Fatal(err)
	}
	assertColumnJSON(t, result, "g", `"a"`, `"b"`)
	assertColumnJSON(t, result, "sum(n)", "9007199254741000", "-9007199254741000")
	assertColumnJSON(t, result, "avg(n)", "4503599627370500", "-4503599627370500")
	assertColumnJSON(t, result, "min(n)", "7", "-9007199254740994")
	assertColumnJSON(t, result, "max(n)", "9007199254740993", "-6")
}

func TestExactAggregateHavingDoesNotRoundThroughFloat64(t *testing.T) {
	segment := aggregateSegment(t,
		`{"g":"above","n":9007199254740993}`,
		`{"g":"equal","n":9007199254740992}`,
	)
	statement, err := PrepareStatement(
		`SELECT g, SUM(n) FROM docs GROUP BY g ` +
			`HAVING SUM(n) > 9007199254740992 ORDER BY g`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	var exec Exec
	cursor, err := statement.RunInto(&exec, FromSegment(segment), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !cursor.Next() {
		t.Fatal("exact HAVING returned no row")
	}
	if got := string(cursor.Cell(0).JSON()); got != `"above"` {
		t.Fatalf("exact HAVING group = %s, want above", got)
	}
	if got := string(cursor.Cell(1).JSON()); got != "9007199254740993" {
		t.Fatalf("exact HAVING SUM = %s", got)
	}
	if cursor.Next() {
		t.Fatal("exact HAVING kept a value equal to its threshold")
	}
}

func TestExactIntegerSumWarmAllocations(t *testing.T) {
	segment := aggregateSegment(t,
		`{"n":9007199254740993}`,
		`{"n":7}`,
		`{"n":-3}`,
	)
	query := Select(Sum("n"))
	var exec Exec
	if err := query.RunInto(&exec, FromSegment(segment)); err != nil {
		t.Fatal(err)
	}
	if got := testing.AllocsPerRun(1_000, func() {
		if err := query.RunInto(&exec, FromSegment(segment)); err != nil {
			panic(err)
		}
	}); got != 0 {
		t.Fatalf("warmed exact integer SUM allocated %.2f times per run", got)
	}
	assertAggregateJSON(t, exec.Result, "sum(n)", "9007199254740997")
}

func TestExactDecimalSumWarmAllocations(t *testing.T) {
	segment := aggregateSegment(t,
		`{"n":0.1}`,
		`{"n":0.2}`,
		`{"n":0.3}`,
	)
	query := Select(Sum("n"))
	var exec Exec
	if err := query.RunInto(&exec, FromSegment(segment)); err != nil {
		t.Fatal(err)
	}
	if got := testing.AllocsPerRun(1_000, func() {
		if err := query.RunInto(&exec, FromSegment(segment)); err != nil {
			panic(err)
		}
	}); got != 0 {
		t.Fatalf("warmed exact decimal SUM allocated %.2f times per run", got)
	}
	assertAggregateJSON(t, exec.Result, "sum(n)", "0.6")
}

func TestExactAverageWarmAllocations(t *testing.T) {
	segment := aggregateSegment(t,
		`{"n":1}`,
		`{"n":2}`,
		`{"n":3}`,
	)
	query := Select(Avg("n"))
	var exec Exec
	if err := query.RunInto(&exec, FromSegment(segment)); err != nil {
		t.Fatal(err)
	}
	if got := testing.AllocsPerRun(1_000, func() {
		if err := query.RunInto(&exec, FromSegment(segment)); err != nil {
			panic(err)
		}
	}); got != 0 {
		t.Fatalf("warmed exact AVG allocated %.2f times per run", got)
	}
	assertAggregateJSON(t, exec.Result, "avg(n)", "2")
}

func aggregateSegment(t testing.TB, documents ...string) *store.Segment {
	t.Helper()
	segment := &store.Segment{}
	for _, document := range documents {
		if _, err := segment.Append([]byte(document)); err != nil {
			t.Fatalf("Append(%s): %v", document, err)
		}
	}
	return segment
}

func assertAggregateJSON(t testing.TB, result Result, column, want string) {
	t.Helper()
	got, ok := result.Column(column)
	if !ok || len(got.Cells) != 1 {
		t.Fatalf("column %q = (%v,%v), want one cell", column, got, ok)
	}
	if value := string(got.Cells[0].JSON()); value != want {
		t.Fatalf("%s = %s, want %s", column, value, want)
	}
}

func assertColumnJSON(t testing.TB, result Result, column string, want ...string) {
	t.Helper()
	got, ok := result.Column(column)
	if !ok || len(got.Cells) != len(want) {
		t.Fatalf("column %q = (%v,%v), want %d cells", column, got, ok, len(want))
	}
	for i := range want {
		if value := string(got.Cells[i].JSON()); value != want[i] {
			t.Fatalf("%s[%d] = %s, want %s", column, i, value, want[i])
		}
	}
}
