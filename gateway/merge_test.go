package gateway

import (
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/shardservice"
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
