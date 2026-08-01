package sql

import (
	"errors"
	"strings"
	"testing"
)

func TestWindowParseASTDumpAndParserReuse(t *testing.T) {
	src := `SELECT
		ROW_NUMBER() OVER (PARTITION BY team ORDER BY score DESC NULLS LAST) AS rn,
		LAG(value, ?, NULL) OVER (ORDER BY seq NULLS FIRST) AS previous,
		SUM(value) OVER (PARTITION BY team ORDER BY seq ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS rolling
	FROM scores ORDER BY rn DESC`
	var parser Parser
	var statement SelectStmt
	for attempt := 0; attempt < 2; attempt++ {
		if err := parser.Parse(&statement, src); err != nil {
			t.Fatal(err)
		}
		if statement.Params != 1 || len(statement.Columns) != 3 {
			t.Fatalf("shape = params %d, columns %d", statement.Params, len(statement.Columns))
		}
		rowNumber := statement.Columns[0].Window
		if rowNumber == nil || rowNumber.Kind != WindowRowNumber ||
			len(rowNumber.Spec.PartitionBy) != 1 || len(rowNumber.Spec.OrderBy) != 1 ||
			!rowNumber.Spec.OrderBy[0].Desc || rowNumber.Spec.OrderBy[0].Nulls != WindowNullsLast {
			t.Fatalf("ROW_NUMBER AST = %+v", rowNumber)
		}
		lag := statement.Columns[1].Window
		if lag == nil || lag.Kind != WindowLag || !lag.HasOffset ||
			lag.Offset.Kind != OperandParam || lag.Offset.Ordinal != 0 ||
			!lag.HasDefault || !lag.DefaultNull {
			t.Fatalf("LAG AST = %+v", lag)
		}
		sum := statement.Columns[2].Window
		if sum == nil || sum.Kind != WindowSum || !sum.Spec.Frame.Explicit ||
			sum.Spec.Frame.Start.Kind != WindowPreceding ||
			sum.Spec.Frame.Start.Offset.Text != "2" ||
			sum.Spec.Frame.End.Kind != WindowCurrentRow {
			t.Fatalf("SUM AST = %+v", sum)
		}
		if len(statement.OrderBy) != 1 || statement.OrderBy[0].Output != 1 ||
			statement.OrderBy[0].Path != nil || !statement.OrderBy[0].Desc {
			t.Fatalf("ORDER BY alias AST = %+v", statement.OrderBy)
		}
		want := "row_number() over(partition 0:team order 0:score:desc:nulls-last) as rn"
		if dump := dumpStmt(&statement); !strings.Contains(dump, want) ||
			!strings.Contains(dump, "lag(0:value,?0,null) over(order 0:seq:asc:nulls-first) as previous") ||
			!strings.Contains(dump, "rows n2-preceding to current-row") ||
			!strings.Contains(dump, "order output(0):desc") {
			t.Fatalf("dump = %s", dump)
		}
	}
}

func TestWindowAggregateDefaultsAndExplicitRowsForms(t *testing.T) {
	tests := []string{
		`SELECT COUNT(*) OVER () FROM t`,
		`SELECT SUM(value) OVER (ROWS UNBOUNDED PRECEDING) FROM t`,
		`SELECT AVG(value) OVER (ORDER BY seq ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM t`,
		`SELECT MIN(value) OVER (ORDER BY seq ROWS 3 PRECEDING) FROM t`,
		`SELECT MAX(value) OVER (ORDER BY seq DESC NULLS FIRST ROWS BETWEEN ? PRECEDING AND ? FOLLOWING) FROM t`,
		`SELECT SUM(value) OVER (ORDER BY seq GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t`,
	}
	for _, src := range tests {
		statement, err := Parse(src)
		if err != nil {
			t.Fatalf("Parse(%q): %v", src, err)
		}
		if statement.Columns[0].Window == nil {
			t.Fatalf("Parse(%q) produced no window", src)
		}
	}
}

func TestWindowAdvancedFunctionsAndGroups(t *testing.T) {
	src := `SELECT
		NTILE(?) OVER (PARTITION BY team ORDER BY score) AS tile,
		PERCENT_RANK() OVER (ORDER BY score) AS percent,
		CUME_DIST() OVER (ORDER BY score DESC) AS cumulative,
		FIRST_VALUE(value) OVER (ORDER BY score GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW) AS first,
		LAST_VALUE(value) OVER (ORDER BY score) AS last,
		NTH_VALUE(value, 2) OVER (ORDER BY score ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS second
	FROM scores`
	statement, err := Parse(src)
	if err != nil {
		t.Fatal(err)
	}
	wantKinds := [...]WindowFunctionKind{
		WindowNTile, WindowPercentRank, WindowCumeDist,
		WindowFirstValue, WindowLastValue, WindowNthValue,
	}
	if statement.Params != 1 || len(statement.Columns) != len(wantKinds) {
		t.Fatalf("shape = params %d, columns %d", statement.Params, len(statement.Columns))
	}
	for i, want := range wantKinds {
		if got := statement.Columns[i].Window; got == nil || got.Kind != want {
			t.Fatalf("column %d window = %+v, want %s", i, got, want)
		}
	}
	if tile := statement.Columns[0].Window; !tile.HasBuckets ||
		tile.Buckets.Kind != OperandParam || tile.Buckets.Ordinal != 0 {
		t.Fatalf("NTILE AST = %+v", tile)
	}
	if frame := statement.Columns[3].Window.Spec.Frame; !frame.Explicit ||
		frame.Unit != WindowFrameGroups || frame.Start.Kind != WindowPreceding ||
		frame.End.Kind != WindowCurrentRow {
		t.Fatalf("GROUPS frame = %+v", frame)
	}
	if nth := statement.Columns[5].Window; !nth.HasNth || nth.Nth.Text != "2" {
		t.Fatalf("NTH_VALUE AST = %+v", nth)
	}
	dump := dumpStmt(statement)
	for _, fragment := range []string{
		"ntile(?0) over(partition 0:team order 0:score:asc)",
		"percent_rank() over(order 0:score:asc)",
		"cume_dist() over(order 0:score:desc)",
		"first_value(0:value) over(order 0:score:asc groups n1-preceding to current-row)",
		"nth_value(0:value,n2) over(order 0:score:asc rows unbounded-preceding to current-row)",
	} {
		if !strings.Contains(dump, fragment) {
			t.Fatalf("dump %q lacks %q", dump, fragment)
		}
	}
}

func TestWindowRangeExclusionAndNamedInheritance(t *testing.T) {
	src := `SELECT
		SUM(v) OVER framed AS direct,
		COUNT(*) OVER (ordered RANGE BETWEEN ? PRECEDING AND 1.2500 FOLLOWING EXCLUDE TIES) AS copied,
		MAX(v) OVER (ordered RANGE CURRENT ROW EXCLUDE NO OTHERS) AS current
	FROM t
	WINDOW partitioned AS (PARTITION BY tenant),
	       ordered AS (partitioned ORDER BY score DESC NULLS LAST),
	       framed AS (ordered RANGE BETWEEN 2.5 PRECEDING AND CURRENT ROW EXCLUDE GROUP)`
	statement, err := Parse(src)
	if err != nil {
		t.Fatal(err)
	}
	if statement.Params != 1 || len(statement.Windows) != 3 {
		t.Fatalf("shape = params %d, windows %d", statement.Params, len(statement.Windows))
	}
	framed := statement.Columns[0].Window
	if framed == nil || !framed.DirectName || framed.Spec.Name != "framed" ||
		len(framed.Spec.PartitionBy) != 1 || len(framed.Spec.OrderBy) != 1 ||
		framed.Spec.Frame.Unit != WindowFrameRange ||
		framed.Spec.Frame.Start.Offset.Text != "2.5" ||
		framed.Spec.Frame.Exclusion != WindowExcludeGroup {
		t.Fatalf("direct named RANGE = %+v", framed)
	}
	copied := statement.Columns[1].Window
	if copied == nil || copied.DirectName || copied.Spec.Name != "ordered" ||
		copied.Spec.Frame.Start.Offset.Kind != OperandParam ||
		copied.Spec.Frame.Start.Offset.Ordinal != 0 ||
		copied.Spec.Frame.End.Offset.Text != "1.2500" ||
		copied.Spec.Frame.Exclusion != WindowExcludeTies {
		t.Fatalf("copied named RANGE = %+v", copied)
	}
	if ordered := &statement.Windows[1]; ordered.Spec.Name != "partitioned" ||
		!ordered.Spec.PartitionInherited || ordered.Spec.OrderInherited ||
		len(ordered.Spec.PartitionBy) != 1 || len(ordered.Spec.OrderBy) != 1 {
		t.Fatalf("ordered definition = %+v", ordered)
	}
	dump := dumpStmt(statement)
	for _, fragment := range []string{
		"range n2.5-preceding to current-row exclude group",
		"range ?0-preceding to n1.2500-following exclude ties",
		"window partitioned=(partition 0:tenant)",
		"ordered=(name=partitioned partition 0:tenant order 0:score:desc:nulls-last)",
	} {
		if !strings.Contains(dump, fragment) {
			t.Fatalf("dump %q lacks %q", dump, fragment)
		}
	}
}

func TestWindowAllExclusionVariants(t *testing.T) {
	for _, exclusion := range []string{
		"CURRENT ROW", "GROUP", "TIES", "NO OTHERS",
	} {
		src := `SELECT SUM(v) OVER (ORDER BY k ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE ` + exclusion + `) FROM t`
		statement, err := Parse(src)
		if err != nil {
			t.Fatalf("EXCLUDE %s: %v", exclusion, err)
		}
		if !statement.Columns[0].Window.Spec.Frame.ExclusionExplicit {
			t.Fatalf("EXCLUDE %s was not retained", exclusion)
		}
	}
}

func TestWindowExactRangeOffsetComparison(t *testing.T) {
	for _, test := range []struct {
		left, right string
		want        int
	}{
		{"1", "1.0000", 0},
		{"0.0001", "1e-4", 0},
		{"9007199254740992", "9007199254740993", -1},
		{"1e1000", "9e999", 1},
		{"-0.000e100", "0", 0},
	} {
		got, known := compareNonNegativeSQLNumbers(test.left, test.right)
		if !known || got != test.want {
			t.Fatalf("compare(%s,%s) = %d/%t, want %d/true",
				test.left, test.right, got, known, test.want)
		}
	}
	valid := `SELECT SUM(v) OVER (ORDER BY k RANGE BETWEEN ` +
		`9007199254740993 PRECEDING AND 9007199254740992 PRECEDING) FROM t`
	if _, err := Parse(valid); err != nil {
		t.Fatalf("exact adjacent valid frame: %v", err)
	}
	invalid := `SELECT SUM(v) OVER (ORDER BY k RANGE BETWEEN ` +
		`9007199254740992 PRECEDING AND 9007199254740993 PRECEDING) FROM t`
	if _, err := Parse(invalid); err == nil || !strings.Contains(err.Error(), "cannot precede") {
		t.Fatalf("exact adjacent invalid frame = %v", err)
	}
	for _, src := range []string{
		`SELECT SUM(v) OVER (ORDER BY k RANGE BETWEEN 0 FOLLOWING AND CURRENT ROW) FROM t`,
		`SELECT SUM(v) OVER (ORDER BY k RANGE BETWEEN ? FOLLOWING AND CURRENT ROW) FROM t`,
	} {
		if _, err := Parse(src); err != nil {
			t.Fatalf("zero-equivalent RANGE bound %q: %v", src, err)
		}
	}
	invalid = `SELECT SUM(v) OVER (` +
		`ORDER BY k RANGE BETWEEN 1 FOLLOWING AND CURRENT ROW) FROM t`
	if _, err := Parse(invalid); err == nil || !strings.Contains(err.Error(), "cannot precede") {
		t.Fatalf("positive FOLLOWING to CURRENT frame = %v", err)
	}
}

func TestNamedWindowScopeAndNestedRebase(t *testing.T) {
	src := `SELECT d.inner_rn, ROW_NUMBER() OVER w AS outer_rn
		FROM (
			SELECT ROW_NUMBER() OVER w AS inner_rn
			FROM "é"
			WINDOW w AS (ORDER BY inner_key RANGE BETWEEN ? PRECEDING AND CURRENT ROW)
		) AS d
		WINDOW w AS (ORDER BY outer_key)`
	statement, err := Parse(src)
	if err != nil {
		t.Fatal(err)
	}
	if len(statement.Windows) != 1 || len(statement.From) != 1 ||
		statement.From[0].Query == nil || len(statement.From[0].Query.Windows) != 1 {
		t.Fatalf("named-window scopes = outer %d, inner %+v",
			len(statement.Windows), statement.From[0].Query)
	}
	inner := statement.From[0].Query
	if got := inner.Windows[0].Spec.OrderBy[0].Path.Pos; got != strings.Index(src, "inner_key") {
		t.Fatalf("nested inherited path position = %d, want %d", got, strings.Index(src, "inner_key"))
	}
	if inner.Columns[0].Window.Spec.OrderBy[0].Path !=
		inner.Windows[0].Spec.OrderBy[0].Path {
		t.Fatal("direct named window did not retain shared resolved path identity")
	}
	if inner.Columns[0].Window.Spec.Frame.Start.Offset.Pos != strings.Index(src, "?") ||
		inner.Windows[0].Spec.Frame.Start.Offset.Pos != strings.Index(src, "?") {
		t.Fatalf("nested frame positions = expression %d, definition %d, want %d",
			inner.Columns[0].Window.Spec.Frame.Start.Offset.Pos,
			inner.Windows[0].Spec.Frame.Start.Offset.Pos, strings.Index(src, "?"))
	}
	if got := statement.Windows[0].Spec.OrderBy[0].Path.Spec(); got != "outer_key" {
		t.Fatalf("outer scope resolved %q, want outer_key", got)
	}
}

func TestNamedWindowDuplicateIsPositioned(t *testing.T) {
	src := `SELECT ROW_NUMBER() OVER w FROM t WINDOW w AS (ORDER BY a), w AS (ORDER BY b)`
	_, err := Parse(src)
	if err == nil {
		t.Fatal("duplicate named window succeeded")
	}
	var positioned *ParseError
	if !errors.As(err, &positioned) {
		t.Fatalf("duplicate error = %T, want ParseError", err)
	}
	want := strings.LastIndex(src, "w AS")
	if positioned.Pos != want || !strings.Contains(err.Error(), "declared twice") {
		t.Fatalf("duplicate error = %v at %d, want %d", err, positioned.Pos, want)
	}
}

func TestWindowPositionedRefusals(t *testing.T) {
	tests := []struct {
		src     string
		at      string
		message string
		feature bool
	}{
		{`SELECT ROW_NUMBER() OVER named FROM t`, "named", "not defined", false},
		{`SELECT SUM(v) OVER (ORDER BY a, b RANGE BETWEEN 1.5 PRECEDING AND CURRENT ROW) FROM t`, "1.5", "exactly one ORDER BY", false},
		{`SELECT SUM(v) OVER (ORDER BY a RANGE BETWEEN -1.5 PRECEDING AND CURRENT ROW) FROM t`, "-1.5", "must not be negative", false},
		{`SELECT SUM(v) OVER (base PARTITION BY p) FROM t WINDOW base AS (ORDER BY k)`, "PARTITION", "cannot override PARTITION", false},
		{`SELECT SUM(v) OVER (base ORDER BY p) FROM t WINDOW base AS (ORDER BY k)`, "ORDER BY p", "cannot override ORDER BY", false},
		{`SELECT SUM(v) OVER (framed) FROM t WINDOW framed AS (ORDER BY k ROWS CURRENT ROW)`, "framed", "frame clause", false},
		{`SELECT SUM(v) OVER later FROM t WINDOW earlier AS (later ORDER BY k), later AS (ORDER BY k)`, "later ORDER", "not defined", false},
		{`SELECT SUM(v) OVER (GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t`, "GROUPS", "requires ORDER BY", false},
		{`SELECT NTILE(0) OVER (ORDER BY k) FROM t`, "0", "greater than zero", false},
		{`SELECT NTH_VALUE(v, 0) OVER (ORDER BY k) FROM t`, "0", "greater than zero", false},
		{`SELECT LAG(v) FROM t`, "FROM", "requires OVER", false},
		{`SELECT SUM(v) OVER (ROWS BETWEEN UNBOUNDED FOLLOWING AND CURRENT ROW) FROM t`, "UNBOUNDED", "cannot start", false},
		{`SELECT SUM(v) OVER (ROWS BETWEEN CURRENT ROW AND UNBOUNDED PRECEDING) FROM t`, "UNBOUNDED", "cannot end", false},
		{`SELECT SUM(v) OVER (ROWS BETWEEN 1 FOLLOWING AND 1 PRECEDING) FROM t`, "1 PRECEDING", "cannot precede", false},
		{`SELECT ROW_NUMBER(1) OVER () FROM t`, "1", "takes no arguments", false},
	}
	for _, test := range tests {
		t.Run(test.at+test.message, func(t *testing.T) {
			_, err := Parse(test.src)
			if err == nil {
				t.Fatal("expected error")
			}
			var positioned *ParseError
			if !errors.As(err, &positioned) {
				t.Fatalf("error %T has no position: %v", err, err)
			}
			wantPos := strings.Index(test.src, test.at)
			if positioned.Pos != wantPos {
				t.Fatalf("position = %d, want %d: %v", positioned.Pos, wantPos, err)
			}
			if !strings.Contains(err.Error(), test.message) {
				t.Fatalf("error %q lacks %q", err, test.message)
			}
			var unsupported *FeatureNotSupportedError
			if errors.As(err, &unsupported) != test.feature {
				t.Fatalf("FeatureNotSupported = %v, want %v: %v", unsupported != nil, test.feature, err)
			}
		})
	}
}

func TestWindowNestedUTF8PositionRebase(t *testing.T) {
	for _, test := range []struct {
		src     string
		at      string
		feature bool
	}{
		{`SELECT d.rn FROM (SELECT ROW_NUMBER() OVER named FROM "é") AS d`, "named", false},
		{`SELECT d.tile FROM (SELECT NTILE(0) OVER (ORDER BY score) AS tile FROM "é") AS d`, "0", false},
	} {
		_, err := Parse(test.src)
		if err == nil {
			t.Fatalf("Parse(%q) succeeded", test.src)
		}
		var positioned *ParseError
		if !errors.As(err, &positioned) {
			t.Fatalf("error = %T, want ParseError", err)
		}
		if want := strings.Index(test.src, test.at); positioned.Pos != want {
			t.Fatalf("position = %d, want %d", positioned.Pos, want)
		}
		var unsupported *FeatureNotSupportedError
		if errors.As(err, &unsupported) != test.feature {
			t.Fatalf("FeatureNotSupported = %v, want %v", unsupported != nil, test.feature)
		}
	}
}

func TestWindowParserWarmedAllocations(t *testing.T) {
	src := `SELECT ROW_NUMBER() OVER ordered, SUM(score) OVER (ordered RANGE BETWEEN 2.5 PRECEDING AND CURRENT ROW EXCLUDE TIES) FROM scores WINDOW partitioned AS (PARTITION BY team), ordered AS (partitioned ORDER BY score DESC NULLS LAST)`
	var parser Parser
	var statement SelectStmt
	if err := parser.Parse(&statement, src); err != nil {
		t.Fatal(err)
	}
	allocations := testing.AllocsPerRun(100, func() {
		if err := parser.Parse(&statement, src); err != nil {
			panic(err)
		}
	})
	if allocations != 0 {
		t.Fatalf("warmed parse allocated %.2f times", allocations)
	}
}

func BenchmarkWindowParse(b *testing.B) {
	src := `SELECT ROW_NUMBER() OVER ordered, SUM(score) OVER (ordered RANGE BETWEEN 2.5 PRECEDING AND CURRENT ROW EXCLUDE TIES) FROM scores WINDOW partitioned AS (PARTITION BY team), ordered AS (partitioned ORDER BY score DESC NULLS LAST)`
	var parser Parser
	var statement SelectStmt
	if err := parser.Parse(&statement, src); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := parser.Parse(&statement, src); err != nil {
			b.Fatal(err)
		}
	}
}
