package gateway

import (
	"errors"
	"strconv"
	"testing"

	"github.com/thesyncim/vibedb/shardservice"
	sqlast "github.com/thesyncim/vibedb/sql"
)

// cell builds a non-null result cell over already-encoded JSON bytes.
func cell(jsonText string) shardservice.Cell { return shardservice.Cell{Bytes: []byte(jsonText)} }

// nullCell builds the null result cell.
func nullCell() shardservice.Cell { return shardservice.Cell{Null: true} }

// rowsOf builds a single-column row response from one JSON value per row.
func rowsOf(values ...string) *shardservice.ShardResponse {
	rows := make([][]shardservice.Cell, len(values))
	for i, v := range values {
		rows[i] = []shardservice.Cell{cell(v)}
	}
	return &shardservice.ShardResponse{Kind: shardservice.ResponseRows, Columns: []shardservice.Column{{Name: "k"}}, Rows: rows}
}

// sign normalizes a comparison result to -1, 0, or 1.
func sign(c int) int {
	switch {
	case c < 0:
		return -1
	case c > 0:
		return 1
	default:
		return 0
	}
}

// TestCompareCells proves the cross-shard comparator reproduces the query
// engine's total order over JSON values: null < bool < number < string <
// container, numbers by exact decimal value beyond float64 precision.
func TestCompareCells(t *testing.T) {
	tests := []struct {
		name string
		a, b shardservice.Cell
		want int
	}{
		{"int_less", cell("1"), cell("2"), -1},
		{"int_equal_decimal_spelling", cell("1"), cell("1.0"), 0},
		{"decimal", cell("1.5"), cell("1.25"), 1},
		{"exact_beyond_float64", cell("9007199254740992"), cell("9007199254740993"), -1},
		{"huge_exponent", cell("1e1000000000"), cell("9e999999999"), 1},
		{"huge_negative_exponent", cell("1e-1000000000"), cell("9e-999999999"), -1},
		{"negative", cell("-5"), cell("3"), -1},
		{"null_before_number", nullCell(), cell("0"), -1},
		{"explicit_null_equals_null_cell", nullCell(), cell("null"), 0},
		{"bool_before_number", cell("true"), cell("0"), -1},
		{"bool_order", cell("false"), cell("true"), -1},
		{"number_before_string", cell("5"), cell(`"5"`), -1},
		{"string_order", cell(`"apple"`), cell(`"banana"`), -1},
		{"string_before_container", cell(`"z"`), cell(`[1]`), -1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := sign(compareCells(classifyCell(tc.a), classifyCell(tc.b)))
			if got != tc.want {
				t.Fatalf("compareCells = %d, want %d", got, tc.want)
			}
			// Antisymmetry: reversing the operands negates the sign.
			if rev := sign(compareCells(classifyCell(tc.b), classifyCell(tc.a))); rev != -tc.want {
				t.Fatalf("reverse compareCells = %d, want %d", rev, -tc.want)
			}
		})
	}

	left := classifyCell(cell("1.0000000000000000000001e1000000000"))
	right := classifyCell(cell("9.999999999999999999999e999999999"))
	if allocs := testing.AllocsPerRun(1000, func() {
		if compareCells(left, right) <= 0 {
			panic("unexpected number order")
		}
	}); allocs != 0 {
		t.Fatalf("cross-shard exact number comparison allocations = %v, want 0", allocs)
	}
}

func BenchmarkCrossShardExactNumberCompare(b *testing.B) {
	left := classifyCell(cell("1.0000000000000000000001e1000000000"))
	right := classifyCell(cell("9.999999999999999999999e999999999"))
	b.ReportAllocs()
	b.SetBytes(int64(len(left.num) + len(right.num)))
	for b.Loop() {
		_ = compareCells(left, right)
	}
}

func BenchmarkMergeGroupedPartialAggregates1K(b *testing.B) {
	const groups = 1024
	columns := []shardservice.Column{{Name: "k"}, {Name: "count"}, {Name: "sum"}}
	results := make([]*shardservice.ShardResponse, 2)
	var inputBytes int64
	for shard := range results {
		rows := make([][]shardservice.Cell, groups)
		for group := range rows {
			key := strconv.Itoa(group)
			value := strconv.Itoa(group + shard + 1)
			rows[group] = []shardservice.Cell{cell(key), cell("1"), cell(value)}
			inputBytes += int64(len(key) + 1 + len(value))
		}
		results[shard] = &shardservice.ShardResponse{
			Kind: shardservice.ResponseRows, Columns: columns, Rows: rows,
		}
	}
	kinds := []sqlast.AggKind{sqlast.AggNone, sqlast.AggCount, sqlast.AggSum}
	b.ReportAllocs()
	b.SetBytes(inputBytes)
	for b.Loop() {
		_, rows, err := mergeGroupedAggregateRows(results, kinds, []int{0}, 64<<20)
		if err != nil || len(rows) != groups {
			b.Fatalf("merge = %d rows, %v", len(rows), err)
		}
	}
}

// decodeInts decodes a single-column integer result into its int64 sequence.
func decodeInts(t *testing.T, rows [][]shardservice.Cell) []int64 {
	t.Helper()
	out := make([]int64, len(rows))
	for i, row := range rows {
		v := classifyCell(row[0])
		if v.kind != ckNumber || !v.isInt {
			t.Fatalf("row %d is not an integer cell: %+v", i, row[0])
		}
		out[i] = v.ival
	}
	return out
}

// TestMergeRows exercises the merge shapes: concatenation without order keys, a
// k-way ascending and descending merge over already-ordered shards, and the
// global LIMIT trim.
func TestMergeRows(t *testing.T) {
	tests := []struct {
		name    string
		results []*shardservice.ShardResponse
		order   []OrderKey
		limit   int
		want    []int64
	}{
		{
			name:    "concat_no_order",
			results: []*shardservice.ShardResponse{rowsOf("1", "3"), rowsOf("2", "4")},
			want:    []int64{1, 3, 2, 4},
		},
		{
			name:    "kway_ascending",
			results: []*shardservice.ShardResponse{rowsOf("1", "4", "7"), rowsOf("2", "3", "8")},
			order:   []OrderKey{{Column: 0}},
			want:    []int64{1, 2, 3, 4, 7, 8},
		},
		{
			name:    "kway_descending",
			results: []*shardservice.ShardResponse{rowsOf("7", "4", "1"), rowsOf("8", "3", "2")},
			order:   []OrderKey{{Column: 0, Desc: true}},
			want:    []int64{8, 7, 4, 3, 2, 1},
		},
		{
			name:    "kway_limit_trim",
			results: []*shardservice.ShardResponse{rowsOf("1", "4", "7"), rowsOf("2", "3", "8")},
			order:   []OrderKey{{Column: 0}},
			limit:   3,
			want:    []int64{1, 2, 3},
		},
		{
			name:    "concat_limit_trim",
			results: []*shardservice.ShardResponse{rowsOf("1", "3"), rowsOf("2", "4")},
			limit:   3,
			want:    []int64{1, 3, 2},
		},
		{
			name:    "kway_with_empty_shard",
			results: []*shardservice.ShardResponse{rowsOf(), rowsOf("2", "5"), rowsOf("1", "9")},
			order:   []OrderKey{{Column: 0}},
			want:    []int64{1, 2, 5, 9},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cols, rows, err := mergeRows(tc.results, tc.order, tc.limit)
			if err != nil {
				t.Fatalf("mergeRows: %v", err)
			}
			if len(cols) != 1 || cols[0].Name != "k" {
				t.Fatalf("columns = %+v, want the first shard's columns", cols)
			}
			got := decodeInts(t, rows)
			if len(got) != len(tc.want) {
				t.Fatalf("rows = %v, want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("rows = %v, want %v", got, tc.want)
				}
			}
		})
	}
}

// TestMergeRejectsNonRows proves a multi-shard result carrying a non-row frame
// fails closed rather than merging, since cross-shard writes are out of scope.
func TestMergeRejectsNonRows(t *testing.T) {
	completion := &shardservice.ShardResponse{Kind: shardservice.ResponseCompletion, RowsAffected: 1}
	_, _, err := mergeRows([]*shardservice.ShardResponse{rowsOf("1"), completion}, nil, 0)
	if !errors.Is(err, ErrUnmergeableResult) {
		t.Fatalf("err = %v, want errors.Is ErrUnmergeableResult", err)
	}
}

// TestMergeRejectsBadOrderColumn proves a sort key outside the row width fails
// closed rather than panicking.
func TestMergeRejectsBadOrderColumn(t *testing.T) {
	_, _, err := mergeRows([]*shardservice.ShardResponse{rowsOf("1"), rowsOf("2")}, []OrderKey{{Column: 5}}, 0)
	if !errors.Is(err, ErrMergeColumn) {
		t.Fatalf("err = %v, want errors.Is ErrMergeColumn", err)
	}
}

func TestMergeRejectsInvalidJSONOrderKeyBeforeHeapAdmission(t *testing.T) {
	_, _, err := mergeRows(
		[]*shardservice.ShardResponse{rowsOf("1.5"), rowsOf("1x")},
		[]OrderKey{{Column: 0}}, 0,
	)
	if !errors.Is(err, ErrMergeValue) {
		t.Fatalf("invalid merge key error = %v, want ErrMergeValue", err)
	}
}

func aggregateResponse(values ...shardservice.Cell) *shardservice.ShardResponse {
	columns := make([]shardservice.Column, len(values))
	for i := range columns {
		columns[i] = shardservice.Column{Name: []string{"count", "sum", "min", "max"}[i]}
	}
	return &shardservice.ShardResponse{
		Kind: shardservice.ResponseRows, Columns: columns,
		Rows: [][]shardservice.Cell{values},
	}
}

func TestMergeAggregateRowsExact(t *testing.T) {
	results := []*shardservice.ShardResponse{
		aggregateResponse(cell("2"), cell("1.25"), cell("1"), cell("4")),
		aggregateResponse(cell("3"), cell("2.75"), cell("-2"), cell("9")),
		aggregateResponse(cell("5"), nullCell(), cell("0"), cell("7")),
	}
	columns, rows, err := mergeAggregateRows(results, []sqlast.AggKind{
		sqlast.AggCount, sqlast.AggSum, sqlast.AggMin, sqlast.AggMax,
	}, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if len(columns) != 4 || len(rows) != 1 || len(rows[0]) != 4 {
		t.Fatalf("shape = %d columns, %v rows", len(columns), rows)
	}
	want := []string{"10", "4", "-2", "9"}
	for i := range want {
		if got := string(rows[0][i].Bytes); got != want[i] || rows[0][i].Null {
			t.Fatalf("column %d = %q/null=%v, want %q", i, got, rows[0][i].Null, want[i])
		}
	}
}

func groupedResponse(rows ...[]shardservice.Cell) *shardservice.ShardResponse {
	return &shardservice.ShardResponse{
		Kind: shardservice.ResponseRows,
		Columns: []shardservice.Column{
			{Name: "k"}, {Name: "count"}, {Name: "sum"}, {Name: "min"}, {Name: "max"},
		},
		Rows: rows,
	}
}

func TestMergeGroupedAggregateRowsExactCanonicalKeys(t *testing.T) {
	results := []*shardservice.ShardResponse{
		groupedResponse(
			[]shardservice.Cell{cell("1"), cell("2"), cell("3.5"), cell("1"), cell("2.5")},
			[]shardservice.Cell{cell(`"caf\u00e9"`), cell("1"), cell("5"), cell("5"), cell("5")},
		),
		groupedResponse(
			[]shardservice.Cell{cell("1.0"), cell("3"), cell("2.5"), cell("-1"), cell("4")},
			[]shardservice.Cell{cell(`"café"`), cell("4"), cell("1"), cell("0"), cell("1")},
		),
	}
	kinds := []sqlast.AggKind{
		sqlast.AggNone, sqlast.AggCount, sqlast.AggSum, sqlast.AggMin, sqlast.AggMax,
	}
	columns, rows, err := mergeGroupedAggregateRows(results, kinds, []int{0}, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if len(columns) != 5 || len(rows) != 2 {
		t.Fatalf("shape = %d columns, %d rows", len(columns), len(rows))
	}
	want := [][]string{
		{"1", "5", "6", "-1", "4"},
		{`"caf\u00e9"`, "5", "6", "0", "5"},
	}
	for row := range want {
		for column := range want[row] {
			if got := string(rows[row][column].Bytes); rows[row][column].Null || got != want[row][column] {
				t.Fatalf("row %d column %d = %q/null=%v, want %q",
					row, column, got, rows[row][column].Null, want[row][column])
			}
		}
	}
}

func TestMergeGroupedAggregateRowsAdmissionAndMalformedKey(t *testing.T) {
	kinds := []sqlast.AggKind{sqlast.AggNone, sqlast.AggCount}
	columns := []shardservice.Column{{Name: "k"}, {Name: "count"}}
	malformed := &shardservice.ShardResponse{
		Kind: shardservice.ResponseRows, Columns: columns,
		Rows: [][]shardservice.Cell{{cell("1x"), cell("1")}},
	}
	if _, _, err := mergeGroupedAggregateRows(
		[]*shardservice.ShardResponse{malformed}, kinds, []int{0}, 1<<20,
	); !errors.Is(err, ErrMergeAggregate) {
		t.Fatalf("malformed grouped key error = %v, want ErrMergeAggregate", err)
	}

	valid := &shardservice.ShardResponse{
		Kind: shardservice.ResponseRows, Columns: columns,
		Rows: [][]shardservice.Cell{{cell(`"large-group-key"`), cell("1")}},
	}
	if _, _, err := mergeGroupedAggregateRows(
		[]*shardservice.ShardResponse{valid}, kinds, []int{0}, 1,
	); !errors.Is(err, ErrMergeAggregate) {
		t.Fatalf("grouped state cap error = %v, want ErrMergeAggregate", err)
	}

	if _, _, err := mergeGroupedAggregateRows(
		[]*shardservice.ShardResponse{valid}, kinds, []int{1}, 1<<20,
	); !errors.Is(err, ErrMergeAggregate) {
		t.Fatalf("invalid grouped program error = %v, want ErrMergeAggregate", err)
	}
}

func TestMergeGroupedAggregateIntegerPromotionAndKeyOnlyDedup(t *testing.T) {
	columns := []shardservice.Column{{Name: "k"}, {Name: "count"}, {Name: "sum"}}
	results := []*shardservice.ShardResponse{
		{
			Kind: shardservice.ResponseRows, Columns: columns,
			Rows: [][]shardservice.Cell{{
				cell("1"), cell("18446744073709551615"), cell("9223372036854775807"),
			}},
		},
		{
			Kind: shardservice.ResponseRows, Columns: columns,
			Rows: [][]shardservice.Cell{{cell("1.0"), cell("1"), cell("1")}},
		},
	}
	_, rows, err := mergeGroupedAggregateRows(results,
		[]sqlast.AggKind{sqlast.AggNone, sqlast.AggCount, sqlast.AggSum},
		[]int{0}, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || string(rows[0][1].Bytes) != "18446744073709551616" ||
		string(rows[0][2].Bytes) != "9223372036854775808" {
		t.Fatalf("promoted grouped state = %+v", rows)
	}

	keyOnlyColumns := []shardservice.Column{{Name: "k"}}
	keyOnly := []*shardservice.ShardResponse{
		{Kind: shardservice.ResponseRows, Columns: keyOnlyColumns,
			Rows: [][]shardservice.Cell{{cell("1")}}},
		{Kind: shardservice.ResponseRows, Columns: keyOnlyColumns,
			Rows: [][]shardservice.Cell{{cell("1e0")}}},
	}
	_, rows, err = mergeGroupedAggregateRows(
		keyOnly, []sqlast.AggKind{sqlast.AggNone}, []int{0}, 1<<20,
	)
	if err != nil || len(rows) != 1 || string(rows[0][0].Bytes) != "1" {
		t.Fatalf("key-only grouped dedup = %+v, %v", rows, err)
	}
}

func TestMergeAggregateNullAndMalformedStates(t *testing.T) {
	nulls := []*shardservice.ShardResponse{
		aggregateResponse(cell("1"), nullCell(), nullCell(), nullCell()),
		aggregateResponse(cell("2"), nullCell(), nullCell(), nullCell()),
	}
	_, rows, err := mergeAggregateRows(nulls, []sqlast.AggKind{
		sqlast.AggCount, sqlast.AggSum, sqlast.AggMin, sqlast.AggMax,
	}, 1024)
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i < 4; i++ {
		if !rows[0][i].Null {
			t.Fatalf("aggregate %d = %+v, want NULL", i, rows[0][i])
		}
	}

	bad := aggregateResponse(cell("1"))
	bad.Rows = append(bad.Rows, []shardservice.Cell{cell("2")})
	if _, _, err := mergeAggregateRows([]*shardservice.ShardResponse{bad}, []sqlast.AggKind{sqlast.AggCount}, 1024); !errors.Is(err, ErrMergeAggregate) {
		t.Fatalf("malformed aggregate error = %v", err)
	}
	malformedExtrema := []*shardservice.ShardResponse{
		aggregateResponse(cell("1"), cell("1"), cell("number-ish"), cell("1")),
	}
	if _, _, err := mergeAggregateRows(malformedExtrema, []sqlast.AggKind{
		sqlast.AggCount, sqlast.AggSum, sqlast.AggMin, sqlast.AggMax,
	}, 1024); !errors.Is(err, ErrMergeAggregate) {
		t.Fatalf("malformed extrema error = %v, want ErrMergeAggregate", err)
	}
	malformedNumber := []*shardservice.ShardResponse{
		aggregateResponse(cell("1"), cell("1"), cell("1x"), cell("1")),
	}
	if _, _, err := mergeAggregateRows(malformedNumber, []sqlast.AggKind{
		sqlast.AggCount, sqlast.AggSum, sqlast.AggMin, sqlast.AggMax,
	}, 1024); !errors.Is(err, ErrMergeAggregate) {
		t.Fatalf("malformed numeric extrema error = %v, want ErrMergeAggregate", err)
	}
}

func TestMergeSumRejectsExponentExpansionBeforeBigArithmetic(t *testing.T) {
	results := []*shardservice.ShardResponse{
		aggregateResponse(cell("1"), cell("1e1000000000")),
		aggregateResponse(cell("1"), cell("-1e1000000000")),
	}
	_, _, err := mergeAggregateRows(results,
		[]sqlast.AggKind{sqlast.AggCount, sqlast.AggSum}, 1024)
	if !errors.Is(err, ErrMergeAggregate) {
		t.Fatalf("merge error = %v, want ErrMergeAggregate", err)
	}
}
