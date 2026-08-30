package query

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestSQLScalarCastExactValuesRepresentationsAndLazyOwnership(t *testing.T) {
	segment := mustSegment(t,
		`{"n":" +001.2300e+2 ","yes":" YeS ","no":"OFF","b":true,"x":12.50,"obj":{"k":1},"j":"{\"b\":1,\"a\":\"\\u0061\",\"b\":2}"}`,
	)
	statement, err := PrepareStatement(`SELECT
		CAST(n AS NUMERIC) AS n,
		CAST(yes AS BOOLEAN) AS yes,
		CAST("no" AS BOOL) AS no,
		CAST(b AS TEXT) AS bt,
		CAST(x AS TEXT) AS xt,
		CAST(obj AS TEXT) AS ot,
		CAST(j AS JSON) AS j
		FROM docs`)
	if err != nil {
		t.Fatal(err)
	}
	schema := statement.AppendSchema(nil)
	wantTypes := []ValueType{TypeNumber, TypeBool, TypeBool, TypeString, TypeString, TypeString, TypeAny}
	wantReps := []OutputRepresentation{OutputSQLNumber, OutputSQLBool, OutputSQLBool, OutputSQLText, OutputSQLText, OutputSQLText, OutputJSON}
	if len(schema) != len(wantTypes) {
		t.Fatalf("schema = %+v", schema)
	}
	for i := range schema {
		if schema[i].Type != wantTypes[i] || schema[i].Representation != wantReps[i] {
			t.Fatalf("schema[%d] = %+v, want type=%d rep=%d", i, schema[i], wantTypes[i], wantReps[i])
		}
	}

	var exec Exec
	cursor, err := statement.RunInto(&exec, FromSegment(segment), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !cursor.Next() {
		t.Fatal("missing CAST row")
	}
	wantJSON := []string{
		`1.23e2`, `true`, `false`, `"true"`, `"12.50"`, `"{\"k\":1}"`,
		`{"b":1,"a":"\u0061","b":2}`,
	}
	for i, want := range wantJSON {
		if got := string(cursor.Cell(i).JSON()); got != want {
			t.Fatalf("column %d = %s, want %s", i, got, want)
		}
	}
	if cursor.Next() {
		t.Fatal("unexpected second CAST row")
	}

	// Run again with a different source to prove the exact JSON output was
	// copied out of the per-row evaluation arena rather than left borrowing it.
	other := mustSegment(t, `{"n":"2","yes":"t","no":"f","b":false,"x":1,"obj":[],"j":"null"}`)
	if _, err := statement.RunInto(&exec, FromSegment(other), nil); err != nil {
		t.Fatal(err)
	}
}

func TestSQLScalarCastJSONExactRowsOwnEvaluationArena(t *testing.T) {
	segment := mustSegment(t,
		`{"id":1,"j":"{\"row\":1,\"s\":\"\\u0061\"}"}`,
		`{"id":2,"j":"\"\\u0062\""}`,
		`{"id":3,"j":"[1,true,null]"}`,
	)
	statement, err := PrepareStatement(`SELECT CAST(j AS JSON) FROM docs ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	var exec Exec
	cursor, err := statement.RunInto(&exec, FromSegment(segment), nil)
	if err != nil {
		t.Fatal(err)
	}
	want := [][]byte{
		[]byte(`{"row":1,"s":"\u0061"}`),
		[]byte(`"\u0062"`),
		[]byte(`[1,true,null]`),
	}
	for row := range want {
		if !cursor.Next() {
			t.Fatalf("missing JSON CAST row %d", row)
		}
		if got := cursor.Cell(0).JSON(); !equalBytes(got, want[row]) {
			t.Fatalf("JSON CAST row %d = %s, want %s", row, got, want[row])
		}
	}
	if cursor.Next() {
		t.Fatal("unexpected fourth JSON CAST row")
	}
}

func TestSQLScalarCastJSONIsIdempotentForExactStrings(t *testing.T) {
	segment := mustSegment(t, `{"j":"\"\\u0061\""}`)
	statement, err := PrepareStatement(
		`SELECT CAST(CAST(j AS JSON) AS JSON), CAST(CAST(j AS JSON) AS TEXT) FROM docs`,
	)
	if err != nil {
		t.Fatal(err)
	}
	var exec Exec
	cursor, err := statement.RunInto(&exec, FromSegment(segment), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !cursor.Next() {
		t.Fatal("missing nested JSON CAST row")
	}
	if got := cursor.Cell(0).JSON(); !equalBytes(got, []byte(`"\u0061"`)) {
		t.Fatalf("nested JSON identity = %s, want exact authored escape", got)
	}
	text, ok := cursor.Cell(1).TextBytes()
	if !ok || !equalBytes(text, []byte("a")) {
		t.Fatalf("nested JSON to text = %q/%v, want a/true", text, ok)
	}
	if cursor.Next() {
		t.Fatal("unexpected second nested JSON CAST row")
	}
}

func TestSQLScalarCastJSONWorkspaceIsAdmittedBeforeArenaGrowth(t *testing.T) {
	left := statementScalarValue{value: scalar{kind: kindString, sval: `{"a":1}`}}
	budget := intermediateBudget{limit: 13}
	var arena []byte
	var charge int64
	_, err := castScalarJSON(9, left, &arena, &budget, &charge)
	if !errors.Is(err, ErrIntermediateBudget) || len(arena) != 0 || budget.used != 0 || charge != 0 {
		t.Fatalf("rejected JSON CAST = arena=%d used=%d charge=%d err=%T %v",
			len(arena), budget.used, charge, err, err)
	}

	budget.limit = 14
	value, err := castScalarJSON(9, left, &arena, &budget, &charge)
	if err != nil || !value.exact || string(value.cell.raw) != `{"a":1}` ||
		budget.used != 14 || charge != 14 {
		t.Fatalf("admitted JSON CAST = value=%+v used=%d charge=%d err=%v",
			value, budget.used, charge, err)
	}
	budget.release(charge)
	if budget.used != 0 {
		t.Fatalf("JSON CAST workspace retained %d bytes", budget.used)
	}
}

func TestSQLScalarCastBooleanUniquePrefixGrammar(t *testing.T) {
	for _, test := range []struct {
		text  string
		value bool
		ok    bool
	}{
		{"t", true, true}, {"tr", true, true}, {"tru", true, true}, {"TRUE", true, true},
		{"f", false, true}, {"fa", false, true}, {"fals", false, true},
		{"y", true, true}, {"ye", true, true}, {"YES", true, true},
		{"n", false, true}, {"no", false, true},
		{"on", true, true}, {"of", false, true}, {"OFF", false, true},
		{"1", true, true}, {"0", false, true},
		{"", false, false}, {"o", false, false}, {"truth", false, false},
		{"2", false, false}, {"yesplease", false, false},
	} {
		value, ok := parseSQLBoolean(test.text)
		if value != test.value || ok != test.ok {
			t.Fatalf("parseSQLBoolean(%q) = %v/%v, want %v/%v",
				test.text, value, ok, test.value, test.ok)
		}
	}

	segment := mustSegment(t, `{}`)
	statement, err := PrepareStatement(`SELECT CAST(? AS BOOLEAN) FROM docs`)
	if err != nil {
		t.Fatal(err)
	}
	var exec Exec
	for _, test := range []struct {
		text  string
		value bool
	}{{"  Tr  ", true}, {"\tOF\n", false}} {
		cursor, err := statement.RunInto(&exec, FromSegment(segment), []any{test.text})
		if err != nil || !cursor.Next() {
			t.Fatalf("CAST BOOLEAN prefix %q = cursor/error %v", test.text, err)
		}
		value, ok := cursor.Cell(0).Bool()
		if !ok || value != test.value || cursor.Next() {
			t.Fatalf("CAST BOOLEAN prefix %q = %v/%v, want %v", test.text, value, ok, test.value)
		}
	}
	for _, text := range []string{"", " o "} {
		_, err := statement.RunInto(&exec, FromSegment(segment), []any{text})
		if !errors.Is(err, ErrScalarInvalidText) {
			t.Fatalf("CAST BOOLEAN ambiguous %q error = %T %v", text, err, err)
		}
	}
}

func TestSQLScalarOutputsAreLazyAfterWhereOffsetAndLimit(t *testing.T) {
	badFirst := mustSegment(t,
		`{"id":1,"v":"not-numeric","z":0}`,
		`{"id":2,"v":"2","z":2}`,
	)
	goodFirst := mustSegment(t,
		`{"id":1,"v":"2","z":2}`,
		`{"id":2,"v":"not-numeric","z":0}`,
	)
	tests := []struct {
		name    string
		source  string
		segment Source
		rows    int
	}{
		{
			"filtered", `SELECT CAST(v AS NUMERIC), 10 / z FROM docs WHERE id + 0 = 2`,
			FromSegment(badFirst), 1,
		},
		{
			"constant false", `SELECT CAST(v AS NUMERIC), 10 / z FROM docs WHERE 1 = 0`,
			FromSegment(badFirst), 0,
		},
		{
			"offset", `SELECT CAST(v AS NUMERIC), 10 / z FROM docs WHERE id + 0 >= 1 ORDER BY id OFFSET 1 LIMIT 1`,
			FromSegment(badFirst), 1,
		},
		{
			"limit", `SELECT CAST(v AS NUMERIC), 10 / z FROM docs ORDER BY id LIMIT 1`,
			FromSegment(goodFirst), 1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			statement, err := PrepareStatement(test.source)
			if err != nil {
				t.Fatal(err)
			}
			var exec Exec
			cursor, err := statement.RunInto(&exec, test.segment, nil)
			if err != nil {
				t.Fatalf("a non-admitted projection raised %T: %v", err, err)
			}
			rows := 0
			for cursor.Next() {
				rows++
				if string(cursor.Cell(0).JSON()) != "2" || string(cursor.Cell(1).JSON()) != "5" {
					t.Fatalf("admitted row = %s/%s", cursor.Cell(0).JSON(), cursor.Cell(1).JSON())
				}
			}
			if rows != test.rows {
				t.Fatalf("rows = %d, want %d", rows, test.rows)
			}
			if statement.nested.frame.intermediate.used != 0 {
				t.Fatalf("statement retained %d intermediate bytes", statement.nested.frame.intermediate.used)
			}
		})
	}
}

func TestSQLScalarCastJSONFilteredOffsetBudgetReleaseAndRecovery(t *testing.T) {
	pad := strings.Repeat("x", 4096)
	jsonValue := `{"pad":"` + pad + `"}`
	documents := make([]string, 128)
	for row := range documents {
		value, divisor := "not-numeric", 0
		if row == len(documents)-1 {
			value, divisor = "2", 2
		}
		documents[row] = fmt.Sprintf(
			`{"id":%d,"p":"{\"pad\":\"%s\"}","v":"%s","z":%d}`,
			row, pad, value, divisor,
		)
	}
	segment := mustSegment(t, documents...)
	statement, err := PrepareStatement(`SELECT CAST(v AS NUMERIC), 10 / z FROM docs
		WHERE CAST(p AS JSON) = CAST(? AS JSON)
		ORDER BY id OFFSET 127 LIMIT 1`)
	if err != nil {
		t.Fatal(err)
	}
	var exec Exec
	exec.Options.IntermediateBytes = 128
	_, err = statement.RunInto(&exec, FromSegment(segment), []any{jsonValue})
	if !errors.Is(err, ErrIntermediateBudget) {
		t.Fatalf("low-budget error = %T %v", err, err)
	}
	if statement.nested.frame.intermediate.used != 0 {
		t.Fatalf("failed run retained %d intermediate bytes", statement.nested.frame.intermediate.used)
	}

	// The dependency relation is about 1 MiB and one row's two JSON CASTs
	// need about 16 KiB. A leaked charge per skipped row would exceed 3 MiB;
	// 2 MiB therefore admits correct peak-live accounting and rejects the old
	// accumulation deterministically.
	exec.Options.IntermediateBytes = 2 << 20
	cursor, err := statement.RunInto(&exec, FromSegment(segment), []any{jsonValue})
	if err != nil {
		t.Fatalf("filtered/OFFSET recovery: %v", err)
	}
	if !cursor.Next() || string(cursor.Cell(0).JSON()) != "2" ||
		string(cursor.Cell(1).JSON()) != "5" || cursor.Next() {
		t.Fatal("filtered/OFFSET recovery returned the wrong row")
	}
	if statement.nested.frame.intermediate.used != 0 {
		t.Fatalf("successful run retained %d intermediate bytes", statement.nested.frame.intermediate.used)
	}
}

func TestSQLScalarCastInvalidTextRangeTypeAtomicAndRecovery(t *testing.T) {
	tests := []struct {
		source string
		value  string
		check  func(error) bool
	}{
		{`SELECT CAST(v AS BOOLEAN) FROM docs`, `wat`, func(err error) bool {
			var target *ScalarInvalidTextError
			return errors.As(err, &target) && errors.Is(err, ErrScalarInvalidText) && target.Pos == 7 && target.Target == "BOOLEAN"
		}},
		{`SELECT CAST(v AS NUMERIC) FROM docs`, `1.2.3`, func(err error) bool {
			var target *ScalarInvalidTextError
			return errors.As(err, &target) && target.Pos == 7 && target.Target == "NUMERIC"
		}},
		{`SELECT CAST(v AS JSON) FROM docs`, `not-json`, func(err error) bool {
			var target *ScalarInvalidTextError
			return errors.As(err, &target) && target.Pos == 7 && target.Target == "JSON"
		}},
		{`SELECT CAST(v AS NUMERIC) FROM docs`, `1e999999999999999999999`, func(err error) bool {
			var target *ScalarNumericRangeError
			return errors.As(err, &target) && target.Pos == 7 && target.Operation == "cast to numeric"
		}},
	}
	for _, test := range tests {
		statement, err := PrepareStatement(test.source)
		if err != nil {
			t.Fatalf("prepare %q: %v", test.source, err)
		}
		bad := mustSegment(t, `{"v":"`+test.value+`"}`)
		var exec Exec
		_, err = statement.RunInto(&exec, FromSegment(bad), nil)
		if !test.check(err) {
			t.Fatalf("%q error = %T %v", test.source, err, err)
		}
		if exec.Result.RowCount != 0 {
			t.Fatalf("%q published %d partial rows", test.source, exec.Result.RowCount)
		}
	}

	typed, err := PrepareStatement(`SELECT CAST(v AS NUMERIC) FROM docs`)
	if err != nil {
		t.Fatal(err)
	}
	var exec Exec
	_, err = typed.RunInto(&exec, FromSegment(mustSegment(t, `{"v":true}`)), nil)
	var mismatch *ScalarTypeError
	if !errors.As(err, &mismatch) || mismatch.Pos != 7 || mismatch.Operation != "cast to numeric" {
		t.Fatalf("type error = %T %v", err, err)
	}
	cursor, err := typed.RunInto(&exec, FromSegment(mustSegment(t, `{"v":"42"}`)), nil)
	if err != nil {
		t.Fatalf("prepared recovery error = %v", err)
	}
	if !cursor.Next() {
		t.Fatal("prepared recovery returned no row")
	}
	if got := string(cursor.Cell(0).JSON()); got != "4.2e1" {
		t.Fatalf("prepared recovery value = %s, want 4.2e1", got)
	}
	if cursor.Next() {
		t.Fatal("prepared recovery returned an extra row")
	}
}

func TestSQLScalarCastPreparedWarmZeroAlloc(t *testing.T) {
	segment := mustSegment(t, `{"n":"001.2500e2","b":"ON","j":"\"a\\nb\""}`)
	statement, err := PrepareStatement(`SELECT CAST(n AS DECIMAL), CAST(b AS BOOL), CAST(j AS JSON) FROM docs`)
	if err != nil {
		t.Fatal(err)
	}
	var exec Exec
	run := func() {
		cursor, runErr := statement.RunInto(&exec, FromSegment(segment), nil)
		if runErr != nil {
			t.Fatal(runErr)
		}
		if !cursor.Next() {
			t.Fatal("missing warmed CAST result")
		}
		if !equalBytes(cursor.Cell(0).JSON(), castWarmNumber) ||
			!equalBytes(cursor.Cell(1).JSON(), trueBytes) ||
			!equalBytes(cursor.Cell(2).JSON(), castWarmJSON) || cursor.Next() {
			t.Fatal("unexpected warmed CAST result")
		}
	}
	run()
	if allocs := testing.AllocsPerRun(100, run); allocs != 0 {
		t.Fatalf("warmed CAST statement allocated %.2f/run", allocs)
	}
}

func TestSQLPostgreSQLCastPreparedWarmZeroAlloc(t *testing.T) {
	segment := mustSegment(t, `{"n":"001.2500e2","b":"ON","j":"\"a\\nb\""}`)
	statement, err := PrepareStatement(
		`SELECT n::DECIMAL, b::BOOL, j::JSON, -'1'::NUMERIC FROM docs`,
	)
	if err != nil {
		t.Fatal(err)
	}
	var exec Exec
	run := func() {
		cursor, runErr := statement.RunInto(&exec, FromSegment(segment), nil)
		if runErr != nil {
			t.Fatal(runErr)
		}
		if !cursor.Next() {
			t.Fatal("missing warmed :: result")
		}
		if !equalBytes(cursor.Cell(0).JSON(), castWarmNumber) ||
			!equalBytes(cursor.Cell(1).JSON(), trueBytes) ||
			!equalBytes(cursor.Cell(2).JSON(), castWarmJSON) ||
			!equalBytes(cursor.Cell(3).JSON(), castWarmNegative) || cursor.Next() {
			t.Fatal("unexpected warmed :: result")
		}
	}
	run()
	if allocs := testing.AllocsPerRun(100, run); allocs != 0 {
		t.Fatalf("warmed :: statement allocated %.2f/run", allocs)
	}
}

var (
	castWarmNumber   = []byte("1.25e2")
	castWarmJSON     = []byte(`"a\nb"`)
	castWarmNegative = []byte("-1")
)

func BenchmarkSQLScalarCastWarm(b *testing.B) {
	for _, test := range []struct {
		name   string
		source string
	}{
		{"numeric", `SELECT CAST(n AS NUMERIC) FROM docs`},
		{"boolean", `SELECT CAST(b AS BOOLEAN) FROM docs`},
		{"json", `SELECT CAST(j AS JSON) FROM docs`},
		{"combined", `SELECT CAST(n AS NUMERIC), CAST(b AS BOOLEAN), CAST(j AS JSON) FROM docs`},
	} {
		b.Run(test.name, func(b *testing.B) {
			segment := mustSegment(b, `{"n":"001.2500e2","b":"ON","j":"\"a\\nb\""}`)
			statement, err := PrepareStatement(test.source)
			if err != nil {
				b.Fatal(err)
			}
			var exec Exec
			for range 2 {
				if _, err := statement.RunInto(&exec, FromSegment(segment), nil); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if _, err := statement.RunInto(&exec, FromSegment(segment), nil); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func FuzzSQLScalarCastNeverPanics(f *testing.F) {
	for _, seed := range []string{"0", "+001.20", ".5", "1.", "1e99999999999999999999", "true", `{"a":1}`, `"x"`, "\xff"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, value string) {
		if len(value) > 4096 {
			t.Skip()
		}
		for _, target := range []string{"NUMERIC", "BOOLEAN", "JSON", "TEXT"} {
			// Parameters safely carry arbitrary fuzz bytes as SQL text without
			// constructing an invalid JSON fixture.
			parameterized, err := PrepareStatement(`SELECT CAST(? AS ` + target + `) FROM docs`)
			if err != nil {
				t.Fatal(err)
			}
			var exec Exec
			_, _ = parameterized.RunInto(&exec, FromSegment(mustSegment(t, `{}`)), []any{value})
		}
	})
}
