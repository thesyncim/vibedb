package query

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"unsafe"

	sqlast "github.com/thesyncim/vibedb/sql"
)

func pathComparisonRows(t testing.TB, segment Source, source string) ([]string, error) {
	t.Helper()
	statement, err := PrepareStatement(source)
	if err != nil {
		return nil, err
	}
	defer statement.Release()
	var exec Exec
	cursor, err := statement.RunInto(&exec, segment, nil)
	if err != nil {
		return nil, err
	}
	rows := make([]string, 0, exec.Result.RowCount)
	for cursor.Next() {
		rows = append(rows, string(cursor.Cell(0).JSON()))
	}
	return rows, nil
}

func TestSQLWherePathComparisonAllOperatorsAndThreeValuedLogic(t *testing.T) {
	segment := FromSegment(mustSegment(t,
		`{"id":"equal","a":1,"b":1,"x":true,"y":true}`,
		`{"id":"less","a":1,"b":2,"x":true,"y":false}`,
		`{"id":"greater","a":2,"b":1,"x":false,"y":true}`,
		`{"id":"left-null","a":null,"b":1}`,
		`{"id":"right-null","a":1,"b":null}`,
		`{"id":"left-missing","b":1}`,
		`{"id":"right-missing","a":1}`,
		`{"id":"both-missing"}`,
		`{"id":"exact-decimal","a":9007199254740993,"b":9007199254740993.0}`,
	))
	tests := []struct {
		predicate string
		want      string
	}{
		{`a = b`, `"equal","exact-decimal"`},
		{`a <> b`, `"less","greater"`},
		{`a < b`, `"less"`},
		{`a <= b`, `"equal","less","exact-decimal"`},
		{`a > b`, `"greater"`},
		{`a >= b`, `"equal","greater","exact-decimal"`},
		{`NOT (a = b)`, `"less","greater"`},
		{`a = b OR a <> b`, `"equal","less","greater","exact-decimal"`},
		{`a = b AND x = y`, `"equal"`},
	}
	for _, test := range tests {
		t.Run(test.predicate, func(t *testing.T) {
			rows, err := pathComparisonRows(t, segment,
				`SELECT id FROM docs WHERE `+test.predicate)
			if err != nil {
				t.Fatal(err)
			}
			if got := strings.Join(rows, ","); got != test.want {
				t.Fatalf("rows = %s, want %s", got, test.want)
			}
		})
	}
}

func TestSQLWherePathComparisonScalarDomainsAndQuotedPaths(t *testing.T) {
	segment := FromSegment(mustSegment(t,
		`{"id":"same","left text":"alpha","right text":"alpha","x":false,"y":false}`,
		`{"id":"ordered","left text":"alpha","right text":"beta","x":false,"y":true}`,
		`{"id":"null","left text":null,"right text":"alpha","x":null,"y":true}`,
	))
	tests := []struct {
		source string
		want   string
	}{
		{`SELECT d.id FROM docs AS d WHERE d."left text" = d."right text"`, `"same"`},
		{`SELECT id FROM docs WHERE "left text" < "right text"`, `"ordered"`},
		{`SELECT id FROM docs WHERE x < y`, `"ordered"`},
	}
	for _, test := range tests {
		rows, err := pathComparisonRows(t, segment, test.source)
		if err != nil {
			t.Fatalf("%s: %v", test.source, err)
		}
		if got := strings.Join(rows, ","); got != test.want {
			t.Fatalf("%s rows = %s, want %s", test.source, got, test.want)
		}
	}
}

func TestSQLWherePathComparisonRejectsIncompatibleLiveDomains(t *testing.T) {
	segment := FromSegment(mustSegment(t, `{"id":"bad","a":1,"b":"1"}`))
	for _, test := range []struct {
		authored  string
		canonical string
	}{
		{"=", "="}, {"<>", "<>"}, {"!=", "<>"}, {"<", "<"},
		{"<=", "<="}, {">", ">"}, {">=", ">="},
	} {
		source := `SELECT id FROM docs WHERE a ` + test.authored + ` b`
		_, err := pathComparisonRows(t, segment, source)
		var undefined *sqlast.UndefinedOperatorError
		if !errors.As(err, &undefined) {
			t.Fatalf("%s error = %T %v", test.authored, err, err)
		}
		if undefined.Left != "numeric" || undefined.Operator != test.canonical ||
			undefined.Right != "text" || undefined.Pos != strings.Index(source, test.authored) {
			t.Fatalf("%s error = %+v", test.authored, undefined)
		}
		want := "operator does not exist: numeric " + test.canonical + " text"
		if undefined.Msg != want {
			t.Fatalf("%s message = %q, want %q", test.authored, undefined.Msg, want)
		}
	}
}

func TestSQLWherePathComparisonDomainResolutionPrecedesFilteringAndLimit(t *testing.T) {
	segment := FromSegment(mustSegment(t,
		`{"id":"bad","keep":true,"a":1,"b":"1"}`,
		`{"id":"safe","keep":true,"a":1,"b":1}`,
	))
	for _, source := range []string{
		`SELECT id FROM docs WHERE keep = FALSE AND a = b`,
		`SELECT id FROM docs WHERE keep = TRUE OR a = b`,
		`SELECT id FROM docs WHERE id = 'safe' AND a = b`,
		`SELECT id FROM docs WHERE a = b LIMIT 0`,
	} {
		t.Run(source, func(t *testing.T) {
			_, err := pathComparisonRows(t, segment, source)
			var undefined *sqlast.UndefinedOperatorError
			if !errors.As(err, &undefined) || undefined.Left != "numeric" ||
				undefined.Operator != "=" || undefined.Right != "text" ||
				undefined.Pos != strings.LastIndex(source, "=") {
				t.Fatalf("error = %T %+v", err, undefined)
			}
		})
	}
}

func TestSQLWherePathComparisonParallelValidationUsesFirstRow(t *testing.T) {
	documents := make([]string, 2048)
	documents[0] = `{"id":0,"keep":false,"a":1,"b":"1"}`
	for i := 1; i < len(documents); i++ {
		documents[i] = fmt.Sprintf(`{"id":%d,"keep":true,"a":1,"b":1}`, i)
	}
	statement, err := PrepareStatement(
		`SELECT id FROM docs WHERE keep = TRUE AND a = b`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	var execution Exec
	execution.Options.Workers = 4
	_, err = statement.RunInto(
		&execution, FromSegment(mustSegment(t, documents...)), nil,
	)
	var undefined *sqlast.UndefinedOperatorError
	if !errors.As(err, &undefined) || undefined.Left != "numeric" ||
		undefined.Operator != "=" || undefined.Right != "text" {
		t.Fatalf("parallel error = %T %+v", err, undefined)
	}
}

func TestSQLWherePathComparisonReachesDerivedCTEWindowAndAggregateInputs(t *testing.T) {
	segment := FromSegment(mustSegment(t,
		`{"id":"a","a":1,"b":1}`,
		`{"id":"b","a":1,"b":2}`,
		`{"id":"c","a":2,"b":2}`,
		`{"id":"d","a":null,"b":null}`,
	))
	tests := []struct {
		name   string
		source string
		want   string
	}{
		{"derived", `SELECT d.id FROM (SELECT id, a, b FROM docs) d WHERE d.a = d.b ORDER BY d.id`, `"a","c"`},
		{"cte", `WITH d AS (SELECT id, a, b FROM docs) SELECT id FROM d WHERE a = b ORDER BY id`, `"a","c"`},
		{"window", `SELECT id, ROW_NUMBER() OVER (ORDER BY id) FROM docs WHERE a = b ORDER BY id`, `"a","c"`},
		{"aggregate", `SELECT SUM(a) FROM docs WHERE a = b`, `3`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rows, err := pathComparisonRows(t, segment, test.source)
			if err != nil {
				t.Fatal(err)
			}
			if got := strings.Join(rows, ","); got != test.want {
				t.Fatalf("rows = %s, want %s", got, test.want)
			}
		})
	}
}

func TestSQLHavingSearchedCaseAcceptsPathComparison(t *testing.T) {
	segment := FromSegment(mustSegment(t,
		`{"id":"equal-1","a":1,"b":1}`,
		`{"id":"different","a":1,"b":2}`,
		`{"id":"equal-2","a":2,"b":2}`,
		`{"id":"unknown","a":null,"b":2}`,
	))
	rows, err := pathComparisonRows(t, segment,
		`SELECT a FROM docs GROUP BY a, b `+
			`HAVING CASE WHEN a = b THEN TRUE ELSE FALSE END = TRUE ORDER BY a`)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(rows, ","); got != `1,2` {
		t.Fatalf("HAVING searched CASE rows = %s, want 1,2", got)
	}
	rows, err = pathComparisonRows(t, segment,
		`SELECT id FROM docs GROUP BY id, a, b HAVING `+
			`NOT (CASE WHEN a = b THEN TRUE ELSE FALSE END = TRUE) ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(rows, ","); got != `"different","unknown"` {
		t.Fatalf("HAVING searched CASE UNKNOWN collapse rows = %s", got)
	}

	mismatch := `SELECT a FROM docs GROUP BY a, b HAVING ` +
		`CASE WHEN a != b THEN TRUE ELSE FALSE END = TRUE`
	_, err = pathComparisonRows(t,
		FromSegment(mustSegment(t, `{"a":1,"b":"1"}`)), mismatch)
	var undefined *sqlast.UndefinedOperatorError
	if !errors.As(err, &undefined) || undefined.Left != "numeric" ||
		undefined.Operator != "<>" || undefined.Right != "text" ||
		undefined.Pos != strings.Index(mismatch, "!=") {
		t.Fatalf("HAVING mismatch = %T %+v", err, undefined)
	}
}

func TestSQLPathComparisonReferenceRejectsUndefinedDomains(t *testing.T) {
	tree, err := sqlast.Parse(`SELECT a FROM docs WHERE a != b`)
	if err != nil {
		t.Fatal(err)
	}
	document := map[string]any{"a": json.Number("1"), "b": "1"}
	if _, err := refTri(tree.Where, document, nil); err == nil ||
		!strings.Contains(err.Error(), "operator does not exist") {
		t.Fatalf("reference mismatch error = %v", err)
	}
}

func TestSQLWherePathComparisonExplainAndLayoutGate(t *testing.T) {
	statement, err := PrepareStatement(`SELECT id FROM docs WHERE left_value <= right_value`)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	plan, err := statement.Explain()
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`"access_path":"full-scan"`,
		`"predicate":{"kind":"path-comparison","path":"left_value","right_path":"right_value","operator":"\u003c="}`,
	} {
		if !strings.Contains(plan, want) {
			t.Fatalf("EXPLAIN missing %s: %s", want, plan)
		}
	}
	// These are the existing 64-bit layout baselines. The repository also
	// cross-compiles 386; pointer-rich structs naturally have a distinct size
	// there, so the architecture-neutral guarantee is supplied by production
	// code adding no fields and this numeric gate applies where it was measured.
	if unsafe.Sizeof(uintptr(0)) == 8 {
		if got := unsafe.Sizeof(Predicate{}); got != 136 {
			t.Fatalf("unsafe.Sizeof(Predicate{}) = %d, want unchanged 136", got)
		}
		if got := unsafe.Sizeof(compiledPredicate{}); got != 360 {
			t.Fatalf("unsafe.Sizeof(compiledPredicate{}) = %d, want unchanged 360", got)
		}
	}
}

func TestSQLWherePathComparisonExplainCanonicalizesNotEqual(t *testing.T) {
	for _, authored := range []string{"!=", "<>"} {
		statement, err := PrepareStatement(
			`SELECT id FROM docs WHERE left_value ` + authored + ` right_value`,
		)
		if err != nil {
			t.Fatal(err)
		}
		plan, err := statement.Explain()
		statement.Release()
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(plan, `"kind":"path-comparison"`) ||
			!strings.Contains(plan, `"operator":"\u003c\u003e"`) {
			t.Fatalf("EXPLAIN %s did not canonicalize to <>: %s", authored, plan)
		}
	}
}

func TestSQLWherePathComparisonKeepsAdversarialPlansSourceFree(t *testing.T) {
	leaves := make([]string, 128)
	for i := range leaves {
		leaves[i] = "a = b"
	}
	source := `SELECT id FROM docs WHERE ` + strings.Join(leaves, " AND ") +
		` /*` + strings.Repeat("x", 256<<10) + `*/`
	statement, err := PrepareStatement(source)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	plan, err := statement.q.compiled()
	if err != nil {
		t.Fatal(err)
	}
	compared := 0
	var walk func(*compiledPredicate)
	walk = func(predicate *compiledPredicate) {
		if predicate == nil {
			return
		}
		if predicate.kind == predCmpPath {
			compared++
			if predicate.pattern != "" {
				t.Fatal("path comparison retained per-leaf SQL source")
			}
		}
		for _, child := range predicate.kids {
			walk(child)
		}
	}
	walk(plan.where)
	if compared != len(leaves) {
		t.Fatalf("compiled %d path comparisons, want %d", compared, len(leaves))
	}
	textCapacity := 0
	for _, chunk := range statement.c.text.chunks {
		textCapacity += len(chunk)
	}
	if textCapacity >= len(source)/16 {
		t.Fatalf("compiler text capacity = %d for %d-byte repeated source", textCapacity, len(source))
	}
}

func TestSQLPathComparisonColdScalarLayoutGate(t *testing.T) {
	switch unsafe.Sizeof(uintptr(0)) {
	case 8:
		if got := unsafe.Sizeof(statementScalar{}); got != 824 {
			t.Fatalf("unsafe.Sizeof(statementScalar{}) = %d, want unchanged 824", got)
		}
		if got := unsafe.Sizeof(statementScalarPredicate{}); got != 72 {
			t.Fatalf("unsafe.Sizeof(statementScalarPredicate{}) = %d, want unchanged 72", got)
		}
		if got := unsafe.Sizeof(relationJoinKey{}); got != 160 {
			t.Fatalf("unsafe.Sizeof(relationJoinKey{}) = %d, want unchanged 160", got)
		}
	case 4:
		if got := unsafe.Sizeof(statementScalar{}); got != 428 {
			t.Fatalf("unsafe.Sizeof(statementScalar{}) = %d, want unchanged 428", got)
		}
		if got := unsafe.Sizeof(statementScalarPredicate{}); got != 44 {
			t.Fatalf("unsafe.Sizeof(statementScalarPredicate{}) = %d, want unchanged 44", got)
		}
	}
}

var pathComparisonErrorSink error

func pathComparisonMismatchAllocs(t testing.TB, rows int) float64 {
	t.Helper()
	documents := make([]string, rows)
	for i := range documents {
		documents[i] = fmt.Sprintf(`{"id":%d,"a":%d,"b":"%d"}`, i, i, i)
	}
	segment := FromSegment(mustSegment(t, documents...))
	statement, err := PrepareStatement(`SELECT id FROM docs WHERE a = b`)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	var exec Exec
	exec.Options.Workers = 1
	run := func() {
		_, pathComparisonErrorSink = statement.RunInto(&exec, segment, nil)
		if pathComparisonErrorSink == nil {
			panic("mismatched live domains succeeded")
		}
	}
	run()
	run()
	return testing.AllocsPerRun(50, run)
}

func TestSQLWherePathComparisonParksOnlyFirstRuntimeError(t *testing.T) {
	one := pathComparisonMismatchAllocs(t, 1)
	many := pathComparisonMismatchAllocs(t, 512)
	if many > one+1 {
		t.Fatalf("mismatch allocations scale with rows: one=%.2f, 512=%.2f", one, many)
	}
}

func TestSQLWherePathComparisonZeroCostSteadyState(t *testing.T) {
	segment := FromSegment(mustSegment(t,
		`{"id":"a","a":1,"b":1}`,
		`{"id":"b","a":1,"b":2}`,
		`{"id":"c","a":null,"b":1}`,
	))
	for _, source := range []string{
		`SELECT id FROM docs WHERE a = 1`,
		`SELECT id FROM docs WHERE a = b`,
		`SELECT id FROM docs WHERE NOT (a = b)`,
	} {
		statement, err := PrepareStatement(source)
		if err != nil {
			t.Fatal(err)
		}
		var exec Exec
		run := func() {
			cursor, runErr := statement.RunInto(&exec, segment, nil)
			if runErr != nil {
				t.Fatal(runErr)
			}
			for cursor.Next() {
				sqlSink += len(cursor.Cell(0).Payload())
			}
		}
		run()
		run()
		if got := testing.AllocsPerRun(100, run); got != 0 {
			t.Fatalf("%s allocates %.2f times per warmed execution", source, got)
		}
		statement.Release()
	}
}
