package query

import (
	"fmt"
	"math"
	"testing"

	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/document"
)

func buildTestSegment(t *testing.T, docs []string) *store.Segment {
	t.Helper()
	seg := &store.Segment{ShapeTapes: true, Options: document.IndexOptions{HashKeys: true}}
	for i, d := range docs {
		if _, err := seg.Append([]byte(d)); err != nil {
			t.Fatalf("append doc %d (%s): %v", i, d, err)
		}
	}
	return seg
}

func collectCursorMatrix(t *testing.T, cur *Cursor, cols int) [][]Cell {
	t.Helper()
	var rows [][]Cell
	var arena []byte
	for cur.Next() {
		row := make([]Cell, cols)
		for c := range cols {
			var owned Cell
			owned, arena = ownResultCellInto(arena, cur.Cell(c))
			row[c] = owned
		}
		rows = append(rows, row)
	}
	return rows
}

func TestExtAggAndDistinctOnWarmedZeroAlloc(t *testing.T) {
	seg := buildExtAggBenchSegment(t, 500, 10, 13)
	src := FromSegment(seg)

	cases := []struct {
		name     string
		sql      string
		wantRows int
	}{
		{
			name: "grouped_count_distinct_sum_filter_having_order_limit",
			sql: `
				SELECT dept,
				       COUNT(DISTINCT tag) AS dtags,
				       SUM(amount) FILTER (WHERE active = TRUE) AS active_sum,
				       AVG(amount) AS avg_amt,
				       MIN(amount) AS min_amt,
				       MAX(amount) AS max_amt
				FROM events
				GROUP BY dept
				HAVING COUNT(DISTINCT tag) >= 5
				ORDER BY active_sum DESC, dept ASC
				LIMIT 5
			`,
			wantRows: 5,
		},
		{
			name: "string_array_json_bool_agg",
			sql: `
				SELECT dept,
				       STRING_AGG(DISTINCT tag, ',') AS tags,
				       ARRAY_AGG(DISTINCT tag) AS tag_arr,
				       JSON_AGG(amount) FILTER (WHERE active = TRUE) AS amounts,
				       BOOL_AND(active) AS all_active,
				       BOOL_OR(active) AS any_active
				FROM events
				GROUP BY dept
				ORDER BY dept ASC
			`,
			wantRows: 10,
		},
		{
			name: "ungrouped_count_sum_distinct",
			sql: `
				SELECT COUNT(DISTINCT tag), SUM(DISTINCT amount), AVG(amount)
				FROM events
			`,
			wantRows: 1,
		},
		{
			name: "distinct_on",
			sql: `
				SELECT DISTINCT ON (dept) dept, id, ts
				FROM events
				ORDER BY dept ASC, ts DESC
				LIMIT 7
			`,
			wantRows: 7,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := PrepareStatement(tc.sql)
			if err != nil {
				t.Fatalf("PrepareStatement: %v", err)
			}
			defer stmt.Release()

			var exec Exec
			defer exec.Release()

			run := func() {
				cur, err := stmt.RunInto(&exec, src, nil)
				if err != nil {
					panic(err)
				}
				if got := countCursorRows(&cur); got != tc.wantRows {
					panic(fmt.Sprintf("got %d rows, want %d", got, tc.wantRows))
				}
			}
			// Warmup to high-water mark.
			run()
			run()
			run()
			if allocs := testing.AllocsPerRun(50, run); allocs != 0 {
				t.Fatalf("%s allocated %.1f times per run, want 0", tc.name, allocs)
			}
		})
	}
}

func TestExtAggInt64OverflowPromotesToExactBigRat(t *testing.T) {
	seg := buildTestSegment(t, []string{
		fmt.Sprintf(`{"g":"pos","v":%d}`, int64(math.MaxInt64)),
		fmt.Sprintf(`{"g":"pos","v":%d}`, int64(math.MaxInt64)),
		fmt.Sprintf(`{"g":"neg","v":%d}`, int64(math.MinInt64)),
		fmt.Sprintf(`{"g":"neg","v":%d}`, int64(math.MinInt64)),
	})
	stmt, err := PrepareStatement(`
		SELECT g, SUM(DISTINCT v) FILTER (WHERE v IS NOT NULL), SUM(v) FILTER (WHERE v IS NOT NULL), AVG(v) FILTER (WHERE v IS NOT NULL)
		FROM t
		GROUP BY g
		ORDER BY g ASC
	`)
	if err != nil {
		t.Fatalf("PrepareStatement: %v", err)
	}
	defer stmt.Release()

	var exec Exec
	defer exec.Release()
	cur, err := stmt.RunInto(&exec, FromSegment(seg), nil)
	if err != nil {
		t.Fatalf("RunInto: %v", err)
	}
	rows := collectCursorMatrix(t, &cur, 4)
	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(rows))
	}
	// neg: MinInt64 * 2 = -18446744073709551616, avg = MinInt64
	if got := string(rows[0][2].JSON()); got != "-18446744073709551616" {
		t.Fatalf("neg SUM got %s, want -18446744073709551616", got)
	}
	if got, ok := rows[0][3].Int64(); !ok || got != math.MinInt64 {
		t.Fatalf("neg AVG got (%d, %v), want (%d, true)", got, ok, int64(math.MinInt64))
	}
	// pos: MaxInt64 * 2 = 18446744073709551614, avg = MaxInt64
	if got := string(rows[1][2].JSON()); got != "18446744073709551614" {
		t.Fatalf("pos SUM got %s, want 18446744073709551614", got)
	}
	if got, ok := rows[1][3].Int64(); !ok || got != math.MaxInt64 {
		t.Fatalf("pos AVG got (%d, %v), want (%d, true)", got, ok, int64(math.MaxInt64))
	}
}

func TestExtAggDecimalsAndExactCompare(t *testing.T) {
	seg := buildTestSegment(t, []string{
		`{"g":"a","v":1.5}`,
		`{"g":"a","v":2.25}`,
		`{"g":"a","v":10}`,
		`{"g":"a","v":-0.75}`,
		`{"g":"b","v":1e2}`,
		`{"g":"b","v":50.5}`,
	})
	stmt, err := PrepareStatement(`
		SELECT g,
		       SUM(v) FILTER (WHERE v IS NOT NULL) AS s,
		       AVG(v) FILTER (WHERE v IS NOT NULL) AS a,
		       MIN(v) FILTER (WHERE v IS NOT NULL) AS mn,
		       MAX(v) FILTER (WHERE v IS NOT NULL) AS mx
		FROM t
		GROUP BY g
		HAVING SUM(v) >= 13
		ORDER BY s DESC
	`)
	if err != nil {
		t.Fatalf("PrepareStatement: %v", err)
	}
	defer stmt.Release()

	var exec Exec
	defer exec.Release()
	cur, err := stmt.RunInto(&exec, FromSegment(seg), nil)
	if err != nil {
		t.Fatalf("RunInto: %v", err)
	}
	rows := collectCursorMatrix(t, &cur, 5)
	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(rows))
	}
	// b: sum=150.5, avg=75.25, min=50.5, max=1e2
	if txt, _ := rows[0][0].Text(); txt != "b" {
		t.Fatalf("row 0 group got %q, want b", txt)
	}
	if f, ok := rows[0][1].Float64(); !ok || f != 150.5 {
		t.Fatalf("b SUM got (%v, %v), want 150.5", f, ok)
	}
	if f, ok := rows[0][2].Float64(); !ok || f != 75.25 {
		t.Fatalf("b AVG got (%v, %v), want 75.25", f, ok)
	}
	if f, ok := rows[0][3].Float64(); !ok || f != 50.5 {
		t.Fatalf("b MIN got (%v, %v), want 50.5", f, ok)
	}
	if f, ok := rows[0][4].Float64(); !ok || f != 100 {
		t.Fatalf("b MAX got (%v, %v), want 100", f, ok)
	}
	// a: sum=13 (exact int!), avg=3.25, min=-0.75, max=10
	if txt, _ := rows[1][0].Text(); txt != "a" {
		t.Fatalf("row 1 group got %q, want a", txt)
	}
	if iv, ok := rows[1][1].Int64(); !ok || iv != 13 {
		t.Fatalf("a SUM got (%d, %v), want (13, true)", iv, ok)
	}
	if f, ok := rows[1][2].Float64(); !ok || f != 3.25 {
		t.Fatalf("a AVG got (%v, %v), want 3.25", f, ok)
	}
	if f, ok := rows[1][3].Float64(); !ok || f != -0.75 {
		t.Fatalf("a MIN got (%v, %v), want -0.75", f, ok)
	}
	if iv, ok := rows[1][4].Int64(); !ok || iv != 10 {
		t.Fatalf("a MAX got (%d, %v), want (10, true)", iv, ok)
	}
}

func TestExtAggEscapedStringsInStringAgg(t *testing.T) {
	seg := buildTestSegment(t, []string{
		`{"g":"k","s":"a\"b"}`,
		`{"g":"k","s":"c\\d"}`,
		`{"g":"k","s":"e\nf\tg"}`,
		`{"g":"k","s":"日本語"}`,
	})
	stmt, err := PrepareStatement(`
		SELECT g, STRING_AGG(s, ' | ')
		FROM t
		GROUP BY g
	`)
	if err != nil {
		t.Fatalf("PrepareStatement: %v", err)
	}
	defer stmt.Release()

	var exec Exec
	defer exec.Release()
	cur, err := stmt.RunInto(&exec, FromSegment(seg), nil)
	if err != nil {
		t.Fatalf("RunInto: %v", err)
	}
	rows := collectCursorMatrix(t, &cur, 2)
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(rows))
	}
	wantText := "a\"b | c\\d | e\nf\tg | 日本語"
	gotText, ok := rows[0][1].Text()
	if !ok || gotText != wantText {
		t.Fatalf("STRING_AGG text got (%q, %v), want %q", gotText, ok, wantText)
	}
	rawJSON := rows[0][1].JSON()
	if !vibejson.Valid(rawJSON) {
		t.Fatalf("STRING_AGG JSON is invalid: %s", rawJSON)
	}
	parsed, err := ParseJSONCell(rawJSON)
	if err != nil {
		t.Fatalf("ParseJSONCell(%s): %v", rawJSON, err)
	}
	if rt, _ := parsed.Text(); rt != wantText {
		t.Fatalf("STRING_AGG JSON round-trip got %q, want %q", rt, wantText)
	}
}

func TestExtAggMultiColumnGroupKeyNoCollisionAndNulls(t *testing.T) {
	seg := buildTestSegment(t, []string{
		`{"a":"x","b":"yz","v":1}`,
		`{"a":"xy","b":"z","v":2}`,
		`{"a":null,"b":"z","v":3}`,
		`{"a":"x","b":null,"v":4}`,
		`{"a":null,"b":null,"v":5}`,
		`{"a":null,"b":null,"v":6}`,
	})
	stmt, err := PrepareStatement(`
		SELECT a, b, COUNT(DISTINCT v), SUM(v) FILTER (WHERE v > 0)
		FROM t
		GROUP BY a, b
		ORDER BY a ASC NULLS FIRST, b ASC NULLS LAST
	`)
	if err != nil {
		t.Fatalf("PrepareStatement: %v", err)
	}
	defer stmt.Release()

	var exec Exec
	defer exec.Release()
	cur, err := stmt.RunInto(&exec, FromSegment(seg), nil)
	if err != nil {
		t.Fatalf("RunInto: %v", err)
	}
	rows := collectCursorMatrix(t, &cur, 4)
	if len(rows) != 5 {
		t.Fatalf("got %d groups, want 5", len(rows))
	}
	// First two rows have a IS NULL; NULLS LAST on b puts (NULL, "z") before (NULL, NULL).
	if !rows[0][0].IsNull() {
		t.Fatalf("row 0 a should be NULL")
	}
	if b0, _ := rows[0][1].Text(); b0 != "z" {
		t.Fatalf("row 0 b got %q, want z", b0)
	}
	if !rows[1][0].IsNull() || !rows[1][1].IsNull() {
		t.Fatalf("row 1 should be (NULL, NULL)")
	}
	if s1, _ := rows[1][3].Int64(); s1 != 11 {
		t.Fatalf("(NULL, NULL) SUM got %d, want 11", s1)
	}
}

func TestExtAggEmptyInputAndReuseAcrossDatasets(t *testing.T) {
	emptySeg := buildTestSegment(t, nil)
	fullSeg := buildTestSegment(t, []string{
		`{"g":"a","v":10,"ok":true}`,
		`{"g":"a","v":20,"ok":false}`,
		`{"g":"b","v":30,"ok":true}`,
	})

	// 1. Ungrouped over empty segment -> 1 row with count=0, rest NULL; then re-run on fullSeg.
	ungrouped, err := PrepareStatement(`
		SELECT COUNT(*), COUNT(DISTINCT v), SUM(v), AVG(v), MIN(v), MAX(v),
		       STRING_AGG(g, ','), ARRAY_AGG(v), JSON_AGG(v), BOOL_AND(ok), BOOL_OR(ok)
		FROM t
	`)
	if err != nil {
		t.Fatalf("PrepareStatement ungrouped: %v", err)
	}
	defer ungrouped.Release()

	var exec Exec
	defer exec.Release()
	cur, err := ungrouped.RunInto(&exec, FromSegment(emptySeg), nil)
	if err != nil {
		t.Fatalf("RunInto empty ungrouped: %v", err)
	}
	rows := collectCursorMatrix(t, &cur, 11)
	if len(rows) != 1 {
		t.Fatalf("empty ungrouped got %d rows, want 1", len(rows))
	}
	if c0, ok := rows[0][0].Int64(); !ok || c0 != 0 {
		t.Fatalf("empty COUNT(*) got (%d, %v), want 0", c0, ok)
	}
	if c1, ok := rows[0][1].Int64(); !ok || c1 != 0 {
		t.Fatalf("empty COUNT(DISTINCT v) got (%d, %v), want 0", c1, ok)
	}
	for col := 2; col < 11; col++ {
		if !rows[0][col].IsNull() {
			t.Fatalf("empty ungrouped col %d expected NULL, got %s", col, rows[0][col].JSON())
		}
	}

	// Re-run same prepared statement on non-empty segment, then back on empty segment.
	cur, err = ungrouped.RunInto(&exec, FromSegment(fullSeg), nil)
	if err != nil {
		t.Fatalf("RunInto full ungrouped: %v", err)
	}
	rows = collectCursorMatrix(t, &cur, 11)
	if len(rows) != 1 {
		t.Fatalf("full ungrouped got %d rows, want 1", len(rows))
	}
	if c0, _ := rows[0][0].Int64(); c0 != 3 {
		t.Fatalf("full COUNT(*) got %d, want 3", c0)
	}
	if s0, _ := rows[0][2].Int64(); s0 != 60 {
		t.Fatalf("full SUM(v) got %d, want 60", s0)
	}

	cur, err = ungrouped.RunInto(&exec, FromSegment(emptySeg), nil)
	if err != nil {
		t.Fatalf("RunInto empty again: %v", err)
	}
	rows = collectCursorMatrix(t, &cur, 11)
	if c0, _ := rows[0][0].Int64(); c0 != 0 || !rows[0][2].IsNull() {
		t.Fatalf("state leaked across runs: count=%d sum=%s", c0, rows[0][2].JSON())
	}

	// 2. Grouped over empty segment -> 0 rows.
	grouped, err := PrepareStatement(`SELECT g, COUNT(DISTINCT v) FROM t GROUP BY g`)
	if err != nil {
		t.Fatalf("PrepareStatement grouped: %v", err)
	}
	defer grouped.Release()
	cur, err = grouped.RunInto(&exec, FromSegment(emptySeg), nil)
	if err != nil {
		t.Fatalf("RunInto empty grouped: %v", err)
	}
	if n := countCursorRows(&cur); n != 0 {
		t.Fatalf("empty grouped got %d rows, want 0", n)
	}
}

func TestDistinctOnMultiKeyOffsetLimitAndEmpty(t *testing.T) {
	seg := buildTestSegment(t, []string{
		`{"a":"x","b":"yz","id":1}`,
		`{"a":"x","b":"yz","id":2}`,
		`{"a":"xy","b":"z","id":3}`,
		`{"a":null,"b":"z","id":4}`,
		`{"a":null,"b":"z","id":5}`,
		`{"a":"z","b":"a","id":6}`,
	})
	stmt, err := PrepareStatement(`
		SELECT DISTINCT ON (a, b) a, b, id
		FROM t
		ORDER BY a ASC, b ASC, id ASC
		OFFSET ? LIMIT ?
	`)
	if err != nil {
		t.Fatalf("PrepareStatement: %v", err)
	}
	defer stmt.Release()

	var exec Exec
	defer exec.Release()

	// All 4 distinct (a, b) pairs.
	cur, err := stmt.RunInto(&exec, FromSegment(seg), []any{int64(0), int64(10)})
	if err != nil {
		t.Fatalf("RunInto all: %v", err)
	}
	allRows := collectCursorMatrix(t, &cur, 3)
	if len(allRows) != 4 {
		t.Fatalf("got %d distinct rows, want 4", len(allRows))
	}

	// Offset 1, Limit 2.
	cur, err = stmt.RunInto(&exec, FromSegment(seg), []any{int64(1), int64(2)})
	if err != nil {
		t.Fatalf("RunInto sliced: %v", err)
	}
	sliced := collectCursorMatrix(t, &cur, 3)
	if len(sliced) != 2 {
		t.Fatalf("got %d sliced rows, want 2", len(sliced))
	}
	id1, _ := allRows[1][2].Int64()
	got1, _ := sliced[0][2].Int64()
	if got1 != id1 {
		t.Fatalf("sliced[0] id got %d, want %d", got1, id1)
	}

	// Empty segment -> 0 rows.
	emptySeg := buildTestSegment(t, nil)
	cur, err = stmt.RunInto(&exec, FromSegment(emptySeg), []any{int64(0), int64(10)})
	if err != nil {
		t.Fatalf("RunInto empty: %v", err)
	}
	if n := countCursorRows(&cur); n != 0 {
		t.Fatalf("empty DISTINCT ON got %d rows, want 0", n)
	}
}

func TestExtAggParameterizedHaving(t *testing.T) {
	seg := buildTestSegment(t, []string{
		`{"g":"a","v":10}`,
		`{"g":"a","v":20}`,
		`{"g":"b","v":100}`,
		`{"g":"c","v":5}`,
		`{"g":"c","v":5}`,
		`{"g":"c","v":15}`,
	})
	stmt, err := PrepareStatement(`
		SELECT g, COUNT(DISTINCT v) AS dv, SUM(v) AS sv
		FROM t
		GROUP BY g
		HAVING COUNT(DISTINCT v) >= ? AND SUM(v) <= ?
		ORDER BY g ASC
	`)
	if err != nil {
		t.Fatalf("PrepareStatement: %v", err)
	}
	defer stmt.Release()

	var exec Exec
	defer exec.Release()
	minDistinct := int64(2)
	maxSum := Number("25")
	cur, err := stmt.RunInto(&exec, FromSegment(seg), []any{&minDistinct, &maxSum})
	if err != nil {
		t.Fatalf("RunInto: %v", err)
	}
	rows := collectCursorMatrix(t, &cur, 3)
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(rows))
	}
	if g, _ := rows[0][0].Text(); g != "c" {
		t.Fatalf("got group %q, want c", g)
	}
}

func TestExtAggAndDistinctOnTextCellsWithoutRawJSON(t *testing.T) {
	// Cell{kind: TypeString, text: ..., raw: nil} is produced by
	// operandToExtAggCell, paramToExtAggCell, and set/CTE text coercions.
	// Verify AppendJSON, key encoding, and cross-type comparison handle raw==nil.
	strA := Cell{kind: TypeString, text: "alpha"}
	strB := Cell{kind: TypeString, text: "beta"}
	strEsc := Cell{kind: TypeString, text: "a\"b\nc"}
	if got := string(strA.AppendJSON(nil)); got != `"alpha"` {
		t.Fatalf("Cell{TypeString, raw:nil}.AppendJSON got %q, want %q", got, `"alpha"`)
	}
	if got := string(strEsc.AppendJSON(nil)); got != `"a\"b\nc"` {
		t.Fatalf("escaped Cell{TypeString, raw:nil}.AppendJSON got %q, want %q", got, `"a\"b\nc"`)
	}
	keyA := appendExtSingleKeyCell(nil, strA)
	keyB := appendExtSingleKeyCell(nil, strB)
	if string(keyA) == string(keyB) {
		t.Fatalf("appendExtSingleKeyCell collided raw==nil strings: %q == %q", keyA, keyB)
	}
	mKeyA := appendExtKeyCell(nil, strA)
	mKeyB := appendExtKeyCell(nil, strB)
	if string(mKeyA) == string(mKeyB) {
		t.Fatalf("appendExtKeyCell collided raw==nil strings: %q == %q", mKeyA, mKeyB)
	}
	// Cross-type comparison: JSON for [1] (`[1]`, 0x5b) > JSON for "a" (`"a"`, 0x22),
	// and JSON for "a" (`"a"`, 0x22) must not collapse to "".
	arrCell, err := ParseJSONCell([]byte(`[1]`))
	if err != nil {
		t.Fatalf("ParseJSONCell: %v", err)
	}
	if compareExtAggCells(arrCell, strA) <= 0 {
		t.Fatalf("expected [1] > \"alpha\" in cross-type JSON comparison")
	}
	// Invalid Number param must be rejected rather than parsed as 0.
	if _, ok := paramToExtAggCell(Number("not-a-number")); ok {
		t.Fatalf("paramToExtAggCell accepted invalid Number")
	}
	badNum := Number("12.3.4")
	if _, ok := paramToExtAggCell(&badNum); ok {
		t.Fatalf("paramToExtAggCell accepted invalid *Number")
	}
	if c, ok := paramToExtAggCell(Number("25.5")); !ok || string(c.JSON()) != "25.5" {
		t.Fatalf("paramToExtAggCell valid decimal Number got (%s, %v), want (25.5, true)", c.JSON(), ok)
	}

	// End-to-end UNION ALL with coerced text column feeding GROUP BY, COUNT(DISTINCT),
	// ARRAY_AGG, and DISTINCT ON.
	seg := buildTestSegment(t, []string{
		`{"s":"alpha","n":1,"m":2}`,
	})
	src := FromSegment(seg)
	var exec Exec
	defer exec.Release()

	stmt, err := PrepareStatement(`
		WITH u AS (
			SELECT CAST(s AS TEXT) AS k FROM t
			UNION ALL
			SELECT CAST(n AS TEXT) AS k FROM t
			UNION ALL
			SELECT CAST(m AS TEXT) AS k FROM t
		)
		SELECT k, COUNT(*) AS cnt, COUNT(DISTINCT k) AS dcnt, ARRAY_AGG(k) AS arr
		FROM u
		GROUP BY k
		ORDER BY k ASC
	`)
	if err != nil {
		t.Fatalf("PrepareStatement grouped: %v", err)
	}
	defer stmt.Release()

	cur, err := stmt.RunInto(&exec, src, nil)
	if err != nil {
		t.Fatalf("RunInto grouped: %v", err)
	}
	rows := collectCursorMatrix(t, &cur, 4)
	if len(rows) != 3 {
		t.Fatalf("GROUP BY coerced strings got %d groups, want 3", len(rows))
	}
	wantKeys := []string{"1", "2", "alpha"}
	for i, want := range wantKeys {
		got, ok := rows[i][0].Text()
		if !ok || got != want {
			t.Fatalf("row %d key got (%q, %v), want %q", i, got, ok, want)
		}
		arrJSON := rows[i][3].JSON()
		if !vibejson.Valid(arrJSON) {
			t.Fatalf("row %d ARRAY_AGG JSON invalid: %s", i, arrJSON)
		}
		wantArr := fmt.Sprintf("[%q]", want)
		if string(arrJSON) != wantArr {
			t.Fatalf("row %d ARRAY_AGG got %s, want %s", i, arrJSON, wantArr)
		}
	}

	distOn, err := PrepareStatement(`
		WITH u AS (
			SELECT CAST(s AS TEXT) AS k, 10 AS v FROM t
			UNION ALL
			SELECT CAST(n AS TEXT) AS k, 20 AS v FROM t
			UNION ALL
			SELECT CAST(m AS TEXT) AS k, 30 AS v FROM t
		)
		SELECT DISTINCT ON (k) k, v
		FROM u
		ORDER BY k ASC, v ASC
	`)
	if err != nil {
		t.Fatalf("PrepareStatement distinct on: %v", err)
	}
	defer distOn.Release()

	cur, err = distOn.RunInto(&exec, src, nil)
	if err != nil {
		t.Fatalf("RunInto distinct on: %v", err)
	}
	drows := collectCursorMatrix(t, &cur, 2)
	if len(drows) != 3 {
		t.Fatalf("DISTINCT ON coerced strings got %d rows, want 3", len(drows))
	}
}

func TestExtAggAndDistinctOnCancelAndNegativeBounds(t *testing.T) {
	seg := buildExtAggBenchSegment(t, 200, 10, 5)
	src := FromSegment(seg)

	// 1. CancelFlag must stop both extended agg and DISTINCT ON.
	var cancel CancelFlag
	cancel.Cancel()
	for _, sqlText := range []string{
		`SELECT dept, COUNT(DISTINCT tag) FROM events GROUP BY dept`,
		`SELECT DISTINCT ON (dept) dept, id FROM events ORDER BY dept ASC, id ASC`,
	} {
		stmt, err := PrepareStatement(sqlText)
		if err != nil {
			t.Fatalf("PrepareStatement(%s): %v", sqlText, err)
		}
		var exec Exec
		exec.Options.Cancel = &cancel
		_, err = stmt.RunInto(&exec, src, nil)
		exec.Release()
		stmt.Release()
		if err == nil {
			t.Fatalf("expected cancellation error for %s, got nil", sqlText)
		}
	}

	// 2. DISTINCT ON must reject negative LIMIT/OFFSET parameters.
	stmt, err := PrepareStatement(`SELECT DISTINCT ON (dept) dept, id FROM events ORDER BY dept ASC, id ASC LIMIT ?`)
	if err != nil {
		t.Fatalf("PrepareStatement: %v", err)
	}
	defer stmt.Release()
	var exec Exec
	defer exec.Release()
	neg := int64(-1)
	if _, err := stmt.RunInto(&exec, src, []any{&neg}); err == nil {
		t.Fatalf("expected error for negative *int64 LIMIT parameter, got nil")
	}
	if _, err := stmt.RunInto(&exec, src, []any{-1}); err == nil {
		t.Fatalf("expected error for negative int LIMIT parameter, got nil")
	}
}
