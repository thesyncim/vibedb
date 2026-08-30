package query

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/thesyncim/vibedb/store"
)

func relationJoinDatabase(t testing.TB) *store.Database {
	t.Helper()
	db := &store.Database{}
	put := func(name string, docs ...string) {
		t.Helper()
		collection, err := db.CreateCollection(name, store.Options{ChunkDocuments: 2})
		if err != nil {
			t.Fatalf("CreateCollection(%q): %v", name, err)
		}
		for i, doc := range docs {
			if _, err := collection.Put(fmt.Sprintf("%s-%d", name, i), []byte(doc)); err != nil {
				t.Fatalf("Put(%q, %d): %v", name, i, err)
			}
		}
	}
	put("a",
		`{"k":1,"zone":"x","enabled":false,"label":"a1"}`,
		`{"k":2,"zone":"x","enabled":true,"label":"a2"}`,
		`{"k":3,"zone":"y","enabled":false,"label":"a3"}`,
	)
	put("b",
		`{"k":2,"zone":"x","keep":true,"label":"b2x"}`,
		`{"k":2,"zone":"y","keep":true,"label":"b2y"}`,
		`{"k":4,"zone":"x","keep":true,"label":"b4"}`,
	)
	put("c",
		`{"k":2,"zone":"x","label":"c2"}`,
		`{"k":3,"zone":"y","label":"c3"}`,
	)
	put("missing_left", `{"k":1}`, `{"k":2}`)
	put("missing_right", `{"k":1}`)
	put("escaped_left", `{"k":"\u0061"}`, `{"k":"\u0062"}`)
	put("escaped_right", `{"k":"a"}`, `{"k":"b"}`)
	put("exact_left",
		`{"k":1.0,"tag":"n","s":"\u0061"}`,
		`{"k":9007199254740993,"tag":"wide","s":"wide"}`,
		`{"k":9007199254740992,"tag":"adjacent","s":"lower"}`,
		`{"k":null,"tag":"null","s":null}`,
		`{"tag":"missing","s":"missing"}`,
	)
	put("exact_right",
		`{"k":10e-1,"tag":"n","s":"a"}`,
		`{"k":9007199254740993.0,"tag":"wide","s":"wide"}`,
		`{"k":9007199254740993,"tag":"adjacent","s":"higher"}`,
		`{"k":null,"tag":"null","s":null}`,
		`{"tag":"missing","s":"missing"}`,
	)
	return db
}

func runRelationJoinSQL(t *testing.T, db *store.Database, src string) (*Statement, *Exec, []string) {
	t.Helper()
	statement, err := PrepareStatement(src)
	if err != nil {
		t.Fatalf("PrepareStatement(%q): %v", src, err)
	}
	var exec Exec
	cursor, err := statement.RunInto(
		&exec, FromDatabase(db.Snapshot(), statement.Collection()), nil,
	)
	if err != nil {
		t.Fatalf("RunInto(%q): %v", src, err)
	}
	rows := make([]string, 0, exec.Result.RowCount)
	for cursor.Next() {
		cells := make([]string, len(statement.Columns()))
		for i := range cells {
			cells[i] = string(cursor.Cell(i).JSON())
		}
		rows = append(rows, strings.Join(cells, ","))
	}
	return statement, &exec, rows
}

func TestRelationJoinSelectorPreservesLegacyFastPath(t *testing.T) {
	for _, tc := range []struct {
		name        string
		sql         string
		generalized bool
	}{
		{"absent", `SELECT a.k FROM a`, false},
		{"physical inner", `SELECT a.k, b.label FROM a JOIN b ON a.k = b.k`, false},
		{"physical left", `SELECT a.k, b.label FROM a LEFT JOIN b ON a.k = b.k`, false},
		{"chain", `SELECT a.k FROM a JOIN b ON a.k = b.k JOIN c ON a.k = c.k`, true},
		{"mixed where", `SELECT a.k FROM a JOIN b ON a.k = b.k WHERE a.k = 2 OR b.k = 4`, true},
		{"nullable where", `SELECT a.k FROM a LEFT JOIN b ON a.k = b.k WHERE b.k IS NULL`, true},
		{"composite", `SELECT a.k FROM a JOIN b ON a.k = b.k AND a.zone = b.zone`, true},
		{"cross", `SELECT a.k FROM a CROSS JOIN b`, true},
		{"comma cross", `SELECT a.k FROM a, b`, true},
		{"right", `SELECT a.k FROM a RIGHT JOIN b ON a.k = b.k`, true},
		{"derived", `SELECT d.k FROM (SELECT k FROM a) d JOIN b ON d.k = b.k`, true},
		{"cte", `WITH d AS (SELECT k FROM a) SELECT d.k FROM d JOIN b ON d.k = b.k`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			statement, err := PrepareStatement(tc.sql)
			if err != nil {
				t.Fatal(err)
			}
			if got := statement.relationJoin() != nil; got != tc.generalized {
				t.Fatalf("generalized = %v, want %v", got, tc.generalized)
			}
			if !tc.generalized && len(statement.tree.From) > 1 && len(statement.q.joins) != 1 {
				t.Fatalf("legacy join count = %d, want one storage-aware join", len(statement.q.joins))
			}
		})
	}
}

func TestRelationJoinSelectorDoesNotChangeResultSchema(t *testing.T) {
	legacy, err := PrepareStatement(
		`SELECT a.k, b.label FROM a LEFT JOIN b ON a.k = b.k`,
	)
	if err != nil {
		t.Fatal(err)
	}
	generalized, err := PrepareStatement(
		`SELECT a.k, b.label FROM a LEFT JOIN b ON a.k = b.k WHERE b.label IS NULL`,
	)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := strings.Join(generalized.Columns(), ","), strings.Join(legacy.Columns(), ","); got != want {
		t.Fatalf("generalized schema = %q, legacy schema = %q", got, want)
	}
	if got := strings.Join(generalized.Columns(), ","); got != "k,b.label" {
		t.Fatalf("stable schema = %q, want k,b.label", got)
	}
}

func TestRelationJoinFullCompositeUsingAndMergedProjection(t *testing.T) {
	db := relationJoinDatabase(t)
	statement, exec, got := runRelationJoinSQL(t, db, `
		SELECT k, zone, a.label, b.label
		FROM a FULL JOIN b USING (k, zone)
		ORDER BY k, zone, a.label, b.label`)
	if gotColumns := strings.Join(statement.Columns(), ","); gotColumns != "k,zone,label,b.label" {
		t.Fatalf("statement schema = %q, want merged bare names and qualified joined name", gotColumns)
	}
	var resultHeaders []string
	for i := range exec.Result.Columns {
		resultHeaders = append(resultHeaders, exec.Result.Columns[i].Header)
	}
	if gotHeaders := strings.Join(resultHeaders, ","); gotHeaders != "k,zone,label,b.label" {
		t.Fatalf("result schema = %q, want statement schema", gotHeaders)
	}
	want := []string{
		`1,"x","a1",null`,
		`2,"x","a2","b2x"`,
		`2,"y",null,"b2y"`,
		`3,"y","a3",null`,
		`4,"x",null,"b4"`,
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("FULL composite rows:\n got %q\nwant %q", got, want)
	}
	if statement.relationJoin() == nil || exec.Stats.JoinPairs != uint64(len(want)) {
		t.Fatalf("generalized stats = %+v", exec.Stats)
	}
}

func TestRelationJoinChainsPriorSourcesAndRepeatedUsing(t *testing.T) {
	db := relationJoinDatabase(t)
	_, _, got := runRelationJoinSQL(t, db, `
		SELECT a.label, b.label, c.label
		FROM a JOIN b ON a.k = b.k JOIN c ON a.k = c.k
		ORDER BY a.label, b.label, c.label`)
	want := []string{`"a2","b2x","c2"`, `"a2","b2y","c2"`}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("prior-source chain:\n got %q\nwant %q", got, want)
	}

	_, _, got = runRelationJoinSQL(t, db, `
		SELECT k, a.label, b.label, c.label
		FROM a JOIN b USING (k) FULL JOIN c USING (k)
		ORDER BY k, a.label, b.label, c.label`)
	want = []string{
		`2,"a2","b2x","c2"`,
		`2,"a2","b2y","c2"`,
		`3,null,null,"c3"`,
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("repeated USING chain:\n got %q\nwant %q", got, want)
	}
}

func TestRelationJoinResidualAndOldSideOnlyOuterSemantics(t *testing.T) {
	db := relationJoinDatabase(t)
	_, _, got := runRelationJoinSQL(t, db, `
		SELECT a.label, b.label
		FROM a LEFT JOIN b
		ON a.k = b.k AND a.zone = b.zone AND b.keep = TRUE
		ORDER BY a.label, b.label`)
	want := []string{
		`"a1",null`,
		`"a2","b2x"`,
		`"a3",null`,
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("residual LEFT rows:\n got %q\nwant %q", got, want)
	}

	_, _, got = runRelationJoinSQL(t, db, `
		SELECT a.label, b.label
		FROM a LEFT JOIN b ON a.enabled = TRUE
		ORDER BY a.label, b.label`)
	want = []string{
		`"a1",null`,
		`"a2","b2x"`,
		`"a2","b2y"`,
		`"a2","b4"`,
		`"a3",null`,
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("old-side-only LEFT rows:\n got %q\nwant %q", got, want)
	}

	_, _, got = runRelationJoinSQL(t, db, `
		SELECT a.label, b.label FROM a LEFT JOIN b ON FALSE
		ORDER BY a.label`)
	want = []string{`"a1",null`, `"a2",null`, `"a3",null`}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("ON FALSE LEFT rows:\n got %q\nwant %q", got, want)
	}
}

func TestRelationJoinCrossDerivedAndCTEOperands(t *testing.T) {
	db := relationJoinDatabase(t)
	_, _, got := runRelationJoinSQL(t, db, `SELECT COUNT(*) FROM a CROSS JOIN b`)
	if len(got) != 1 || got[0] != "9" {
		t.Fatalf("CROSS count = %q, want 9", got)
	}
	_, _, comma := runRelationJoinSQL(t, db, `SELECT COUNT(*) FROM a, b`)
	if strings.Join(comma, "\n") != strings.Join(got, "\n") {
		t.Fatalf("comma FROM count = %q, want explicit CROSS JOIN result %q", comma, got)
	}

	for _, src := range []string{
		`SELECT d.k, b.label FROM (SELECT k FROM a WHERE k >= 2) d JOIN b ON d.k = b.k ORDER BY d.k, b.label`,
		`WITH d AS (SELECT k FROM a WHERE k >= 2) SELECT d.k, b.label FROM d JOIN b ON d.k = b.k ORDER BY d.k, b.label`,
	} {
		_, _, got = runRelationJoinSQL(t, db, src)
		want := []string{`2,"b2x"`, `2,"b2y"`}
		if strings.Join(got, "\n") != strings.Join(want, "\n") {
			t.Fatalf("relation operand %q:\n got %q\nwant %q", src, got, want)
		}
	}
}

func TestRelationJoinNestedOperandPlaceholderBasesAndPreparedReuse(t *testing.T) {
	db := relationJoinDatabase(t)
	statement, err := PrepareStatement(`
		WITH d AS (SELECT k FROM a WHERE k >= ?)
		SELECT d.k, x.label
		FROM d
		JOIN (SELECT k, label FROM b WHERE k <= ?) x
		ON d.k = x.k AND x.label LIKE ?
		ORDER BY d.k, x.label`)
	if err != nil {
		t.Fatal(err)
	}
	if statement.NumParams() != 3 {
		t.Fatalf("NumParams = %d, want 3", statement.NumParams())
	}
	source := FromDatabase(db.Snapshot(), statement.Collection())
	var exec Exec
	collect := func(args []any) []string {
		cursor, err := statement.RunInto(&exec, source, args)
		if err != nil {
			t.Fatal(err)
		}
		var rows []string
		for cursor.Next() {
			rows = append(rows,
				string(cursor.Cell(0).JSON())+","+string(cursor.Cell(1).JSON()))
		}
		return rows
	}
	firstArgs := []any{int64(2), int64(2), "b2%"}
	if got := collect(firstArgs); strings.Join(got, "\n") != "2,\"b2x\"\n2,\"b2y\"" {
		t.Fatalf("first binding = %q", got)
	}
	if got := collect([]any{int64(3), int64(4), "b4"}); len(got) != 0 {
		t.Fatalf("second binding retained stale rows: %q", got)
	}
	run := func() {
		if _, err := statement.RunInto(&exec, source, firstArgs); err != nil {
			panic(err)
		}
	}
	run()
	if got := testing.AllocsPerRun(50, run); got != 0 {
		t.Fatalf("placeholder relation join warmed allocations = %.2f, want 0", got)
	}
}

func TestRelationJoinCTEMaterializationPolicies(t *testing.T) {
	db := relationJoinDatabase(t)
	for _, tc := range []struct {
		name string
		hint string
		want uint64
	}{
		{"default multiple", "", 1},
		{"materialized", "MATERIALIZED", 1},
		{"not materialized", "NOT MATERIALIZED", 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			statement, err := PrepareStatement(fmt.Sprintf(`
				WITH d AS %s (SELECT k, label FROM a)
				SELECT x.k, y.k
				FROM d x JOIN d y ON x.k = y.k AND x.label = y.label`, tc.hint))
			if err != nil {
				t.Fatal(err)
			}
			var exec Exec
			_, err = statement.RunInto(
				&exec, FromDatabase(db.Snapshot(), statement.Collection()), nil,
			)
			if err != nil {
				t.Fatal(err)
			}
			def := statement.cteCatalog().defs[0]
			if def.runEvaluations != tc.want {
				t.Fatalf("evaluations = %d, want %d", def.runEvaluations, tc.want)
			}
		})
	}
}

func TestRelationJoinCountSchemaHasNoPathPanic(t *testing.T) {
	statement, err := PrepareStatement(`SELECT COUNT(*) FROM a CROSS JOIN b`)
	if err != nil {
		t.Fatal(err)
	}
	if got := statement.Columns(); len(got) != 1 || got[0] != "count(*)" {
		t.Fatalf("Columns = %q, want count(*)", got)
	}
}

func TestRelationJoinPreservesExactValuesAndOuterNullPresence(t *testing.T) {
	db := relationJoinDatabase(t)
	_, _, got := runRelationJoinSQL(t, db, `
		SELECT l.k, r.k, l.s, r.s
		FROM exact_left l JOIN exact_right r
		ON l.k = r.k AND l.tag = r.tag
		ORDER BY l.k`)
	want := []string{
		`1.0,10e-1,"\u0061","a"`,
		`9007199254740993,9007199254740993.0,"wide","wide"`,
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("exact joined values:\n got %q\nwant %q", got, want)
	}

	_, _, got = runRelationJoinSQL(t, db, `
		SELECT l.k
		FROM missing_left l LEFT JOIN missing_right r ON l.k = r.k
		WHERE r.value IS MISSING
		ORDER BY l.k`)
	if len(got) != 1 || got[0] != "1" {
		t.Fatalf("IS MISSING after outer extension = %q, want only matched missing row 1", got)
	}
}

func TestRelationJoinCompositeNullNeverMatches(t *testing.T) {
	db := relationJoinDatabase(t)
	_, _, got := runRelationJoinSQL(t, db, `
		SELECT l.tag, r.tag
		FROM exact_left l FULL JOIN exact_right r
		ON l.k = r.k AND l.tag = r.tag`)
	want := []string{
		`"n","n"`,
		`"wide","wide"`,
		`"adjacent",null`,
		`"null",null`,
		`"missing",null`,
		`null,"adjacent"`,
		`null,"null"`,
		`null,"missing"`,
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("composite NULL/missing rows:\n got %q\nwant %q", got, want)
	}
}

func TestRelationJoinMergedUsingOwnsEscapedDecodedStrings(t *testing.T) {
	db := relationJoinDatabase(t)
	statement, err := PrepareStatement(`
		SELECT k FROM escaped_left l FULL JOIN escaped_right r USING (k)`)
	if err != nil {
		t.Fatal(err)
	}
	source := FromDatabase(db.Snapshot(), statement.Collection())
	var exec Exec
	cursor, err := statement.RunInto(&exec, source, nil)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for cursor.Next() {
		text, _ := cursor.Cell(0).Text()
		got = append(got, string(cursor.Cell(0).JSON())+":"+text)
	}
	want := []string{`"\u0061":a`, `"\u0062":b`}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("merged escaped strings = %q, want %q", got, want)
	}
	run := func() {
		_, err := statement.RunInto(&exec, source, nil)
		if err != nil {
			panic(err)
		}
	}
	run()
	if got := testing.AllocsPerRun(50, run); got != 0 {
		t.Fatalf("escaped merged USING warmed allocations = %.2f, want 0", got)
	}
}

func TestRelationJoinRightAndFullUnmatchedSourceOrder(t *testing.T) {
	db := relationJoinDatabase(t)
	_, _, right := runRelationJoinSQL(t, db, `
		SELECT a.label, b.label FROM a RIGHT JOIN b ON a.k = b.k`)
	wantRight := []string{
		`"a2","b2x"`,
		`"a2","b2y"`,
		`null,"b4"`,
	}
	if strings.Join(right, "\n") != strings.Join(wantRight, "\n") {
		t.Fatalf("RIGHT source order:\n got %q\nwant %q", right, wantRight)
	}

	_, _, full := runRelationJoinSQL(t, db, `
		SELECT a.label, b.label
		FROM a FULL JOIN b ON a.k = b.k AND a.zone = b.zone`)
	wantFull := []string{
		`"a1",null`,
		`"a2","b2x"`,
		`"a3",null`,
		`null,"b2y"`,
		`null,"b4"`,
	}
	if strings.Join(full, "\n") != strings.Join(wantFull, "\n") {
		t.Fatalf("FULL source order:\n got %q\nwant %q", full, wantFull)
	}
}

func TestRelationJoinExplainReportsLogicalAndMeasuredStages(t *testing.T) {
	db := relationJoinDatabase(t)
	src := `
		SELECT a.label, b.label
		FROM a FULL JOIN b
		ON a.k = b.k AND a.zone = b.zone AND b.keep = TRUE
		ORDER BY a.label, b.label`
	statement, err := PrepareStatement(src)
	if err != nil {
		t.Fatal(err)
	}
	logical, err := statement.Explain()
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`"type":"full"`,
		`"access_path":"relation-spool"`,
		`"algorithm":"composite-hash"`,
		`"build_side":"right"`,
		`"keys":[{"left":"a.k","right":"b.k"},{"left":"a.zone","right":"b.zone"}]`,
		`"key_count":2`,
		`"residual":true`,
		`"cross":false`,
	} {
		if !strings.Contains(logical, want) {
			t.Errorf("logical EXPLAIN missing %s: %s", want, logical)
		}
	}

	var exec Exec
	_, err = statement.RunInto(
		&exec, FromDatabase(db.Snapshot(), statement.Collection()), nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	actual, err := statement.ExplainAnalyze(ExplainOptions{}, ExplainAnalysis{
		Rows: exec.Result.RowCount, Stats: exec.Stats,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`"actual_algorithm":"composite-hash"`,
		fmt.Sprintf(`"pairs":%d`, exec.Stats.JoinPairs),
		fmt.Sprintf(`"join_pairs":%d`, exec.Stats.JoinPairs),
	} {
		if !strings.Contains(actual, want) {
			t.Errorf("EXPLAIN ANALYZE missing %s: %s", want, actual)
		}
	}

	cross, err := PrepareStatement(`SELECT COUNT(*) FROM a CROSS JOIN b`)
	if err != nil {
		t.Fatal(err)
	}
	crossPlan, err := cross.Explain()
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`"type":"cross"`,
		`"algorithm":"nested-loop-cross"`,
		`"key_count":0`,
		`"residual":false`,
		`"cross":true`,
	} {
		if !strings.Contains(crossPlan, want) {
			t.Errorf("CROSS EXPLAIN missing %s: %s", want, crossPlan)
		}
	}
	if strings.Contains(crossPlan, `"build_side"`) {
		t.Fatalf("CROSS EXPLAIN claims a build side: %s", crossPlan)
	}
	nested, err := PrepareStatement(`SELECT a.k FROM a LEFT JOIN b ON a.enabled = TRUE`)
	if err != nil {
		t.Fatal(err)
	}
	nestedPlan, err := nested.Explain()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(nestedPlan, `"algorithm":"bounded-nested-loop"`) ||
		strings.Contains(nestedPlan, `"build_side"`) {
		t.Fatalf("nested-loop EXPLAIN is not truthful: %s", nestedPlan)
	}
}

func TestRelationJoinBudgetsCancellationAndReuse(t *testing.T) {
	db := relationJoinDatabase(t)
	statement, err := PrepareStatement(`
		SELECT k, a.label, b.label
		FROM a FULL JOIN b USING (k)
		ORDER BY k, a.label, b.label`)
	if err != nil {
		t.Fatal(err)
	}
	source := FromDatabase(db.Snapshot(), statement.Collection())
	var exec Exec
	exec.Options.JoinPairBytes = 1
	_, err = statement.RunInto(&exec, source, nil)
	var pairBudget *JoinPairBudgetError
	if !errors.As(err, &pairBudget) {
		t.Fatalf("JoinPairBytes error = %T %v, want *JoinPairBudgetError", err, err)
	}
	if exec.Result.RowCount != 0 {
		t.Fatalf("pair-budget failure published %d rows", exec.Result.RowCount)
	}

	exec.Options.JoinPairBytes = -1
	exec.Options.IntermediateBytes = 1
	_, err = statement.RunInto(&exec, source, nil)
	var intermediate *IntermediateBudgetError
	if !errors.As(err, &intermediate) {
		t.Fatalf("IntermediateBytes error = %T %v, want *IntermediateBudgetError", err, err)
	}
	if exec.Result.RowCount != 0 {
		t.Fatalf("intermediate-budget failure published %d rows", exec.Result.RowCount)
	}

	exec.Options.IntermediateBytes = -1
	var cancel CancelFlag
	exec.Options.Cancel = &cancel
	cancel.Cancel()
	_, err = statement.RunInto(&exec, source, nil)
	if !errors.Is(err, ErrCanceled) {
		t.Fatalf("cancellation = %v, want ErrCanceled", err)
	}
	if exec.Result.RowCount != 0 {
		t.Fatalf("canceled execution published %d rows", exec.Result.RowCount)
	}
	cancel.Reset()
	cursor, err := statement.RunInto(&exec, source, nil)
	if err != nil || !cursor.Next() {
		t.Fatalf("reuse after failures: next=%v err=%v", cursor.Row() >= 0, err)
	}
}

func TestRelationJoinFinalResultLimitDoesNotTruncateOperands(t *testing.T) {
	db := relationJoinDatabase(t)
	statement, err := PrepareStatement(`SELECT a.label, b.label FROM a CROSS JOIN b LIMIT 2`)
	if err != nil {
		t.Fatal(err)
	}
	exec := Exec{Options: ExecOptions{ResultRows: 2}}
	_, err = statement.RunInto(
		&exec, FromDatabase(db.Snapshot(), statement.Collection()), nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if exec.Result.RowCount != 2 || exec.Stats.JoinPairs != 9 {
		t.Fatalf("root limit rows/pairs = %d/%d, want 2/9",
			exec.Result.RowCount, exec.Stats.JoinPairs)
	}
}

func TestRelationJoinPairCountOverflowGuard(t *testing.T) {
	next, err := relationJoinNextPair(0, math.MaxInt, -1)
	var budget *JoinPairBudgetError
	if next != math.MaxInt || !errors.As(err, &budget) || budget.Bytes != math.MaxInt64 {
		t.Fatalf("overflow guard = next %d, error %T %+v", next, err, err)
	}
}

func TestRelationJoinWarmExecutionAllocations(t *testing.T) {
	db := relationJoinDatabase(t)
	for _, src := range []string{
		`SELECT k, a.label, b.label FROM a FULL JOIN b USING (k) ORDER BY k, a.label, b.label`,
		`SELECT COUNT(*) FROM a, b`,
	} {
		statement, err := PrepareStatement(src)
		if err != nil {
			t.Fatal(err)
		}
		source := FromDatabase(db.Snapshot(), statement.Collection())
		exec := Exec{Options: ExecOptions{IntermediateBytes: -1, JoinPairBytes: -1}}
		run := func() {
			if _, err := statement.RunInto(&exec, source, nil); err != nil {
				panic(err)
			}
		}
		run()
		if got := testing.AllocsPerRun(50, run); got != 0 {
			t.Fatalf("%q warmed allocations = %.2f, want 0", src, got)
		}
	}
}

func TestRelationJoinIndependentPreparedStatementsRace(t *testing.T) {
	db := relationJoinDatabase(t)
	snapshot := db.Snapshot()
	const goroutines = 8
	var wait sync.WaitGroup
	errs := make(chan error, goroutines)
	for worker := 0; worker < goroutines; worker++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			statement, err := PrepareStatement(`SELECT COUNT(*) FROM a CROSS JOIN b`)
			if err != nil {
				errs <- err
				return
			}
			var exec Exec
			source := FromDatabase(snapshot, statement.Collection())
			for run := 0; run < 20; run++ {
				cursor, err := statement.RunInto(&exec, source, nil)
				if err != nil {
					errs <- err
					return
				}
				if !cursor.Next() || string(cursor.Cell(0).JSON()) != "9" {
					errs <- fmt.Errorf("COUNT result changed on run %d", run)
					return
				}
			}
		}()
	}
	wait.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

type relationJoinReferenceRow struct {
	id   int
	key  int
	zone int
	keep bool
}

func TestRelationJoinDeterministicRandomizedDifferential(t *testing.T) {
	random := rand.New(rand.NewSource(0x5eedc0de))
	makeRows := func(count int) []relationJoinReferenceRow {
		rows := make([]relationJoinReferenceRow, count)
		for i := range rows {
			rows[i] = relationJoinReferenceRow{
				id: i + 1, key: random.Intn(6), zone: random.Intn(3),
				keep: random.Intn(3) != 0,
			}
		}
		return rows
	}
	left, right, third := makeRows(17), makeRows(13), makeRows(11)
	db := &store.Database{}
	put := func(name string, rows []relationJoinReferenceRow) {
		collection, err := db.CreateCollection(name, store.Options{ChunkDocuments: 4})
		if err != nil {
			t.Fatal(err)
		}
		for _, row := range rows {
			document := fmt.Sprintf(
				`{"id":%d,"k":%d,"zone":%d,"keep":%t}`,
				row.id, row.key, row.zone, row.keep,
			)
			if _, err := collection.Put(fmt.Sprintf("%s-%d", name, row.id), []byte(document)); err != nil {
				t.Fatal(err)
			}
		}
	}
	put("random_left", left)
	put("random_right", right)
	put("random_third", third)

	singleReference := func(kind string) []string {
		var rows []string
		matchedRight := make([]bool, len(right))
		for _, lrow := range left {
			matched := false
			for ri, rrow := range right {
				if lrow.key != rrow.key || lrow.zone != rrow.zone || !rrow.keep {
					continue
				}
				matched = true
				matchedRight[ri] = true
				rows = append(rows, fmt.Sprintf("%d,%d", lrow.id, rrow.id))
			}
			if !matched && (kind == "LEFT" || kind == "FULL") {
				rows = append(rows, fmt.Sprintf("%d,null", lrow.id))
			}
		}
		if kind == "RIGHT" || kind == "FULL" {
			for ri, rrow := range right {
				if !matchedRight[ri] {
					rows = append(rows, fmt.Sprintf("null,%d", rrow.id))
				}
			}
		}
		sort.Strings(rows)
		return rows
	}
	for _, kind := range []string{"INNER", "LEFT", "RIGHT", "FULL"} {
		join := kind + " JOIN"
		if kind == "INNER" {
			join = "JOIN"
		}
		_, _, got := runRelationJoinSQL(t, db, fmt.Sprintf(`
			SELECT l.id, r.id
			FROM random_left l %s random_right r
			ON l.k = r.k AND l.zone = r.zone AND r.keep = TRUE`, join))
		sort.Strings(got)
		want := singleReference(kind)
		if strings.Join(got, "\n") != strings.Join(want, "\n") {
			t.Fatalf("%s randomized differential:\n got %q\nwant %q", kind, got, want)
		}
	}

	_, _, cross := runRelationJoinSQL(t, db, `
		SELECT l.id, r.id FROM random_left l CROSS JOIN random_right r`)
	if len(cross) != len(left)*len(right) {
		t.Fatalf("CROSS rows = %d, want %d", len(cross), len(left)*len(right))
	}

	var chainWant []string
	for _, lrow := range left {
		for _, rrow := range right {
			if lrow.key != rrow.key || !rrow.keep {
				continue
			}
			matched := false
			for _, crow := range third {
				if lrow.zone != crow.zone || !crow.keep {
					continue
				}
				matched = true
				chainWant = append(chainWant, fmt.Sprintf("%d,%d,%d", lrow.id, rrow.id, crow.id))
			}
			if !matched {
				chainWant = append(chainWant, fmt.Sprintf("%d,%d,null", lrow.id, rrow.id))
			}
		}
	}
	_, _, chainGot := runRelationJoinSQL(t, db, `
		SELECT l.id, r.id, c.id
		FROM random_left l
		JOIN random_right r ON l.k = r.k AND r.keep = TRUE
		LEFT JOIN random_third c ON l.zone = c.zone AND c.keep = TRUE`)
	sort.Strings(chainGot)
	sort.Strings(chainWant)
	if strings.Join(chainGot, "\n") != strings.Join(chainWant, "\n") {
		t.Fatalf("chain randomized differential:\n got %q\nwant %q", chainGot, chainWant)
	}
}

func BenchmarkRelationJoinWarmed(b *testing.B) {
	db := relationJoinDatabase(b)
	for _, shape := range []struct {
		name string
		sql  string
	}{
		{"composite_hash", `SELECT a.label, b.label FROM a FULL JOIN b ON a.k = b.k AND a.zone = b.zone`},
		{"cross", `SELECT COUNT(*) FROM a CROSS JOIN b`},
		{"residual_nested", `SELECT a.label, b.label FROM a LEFT JOIN b ON a.enabled = TRUE`},
	} {
		b.Run(shape.name, func(b *testing.B) {
			statement, err := PrepareStatement(shape.sql)
			if err != nil {
				b.Fatal(err)
			}
			source := FromDatabase(db.Snapshot(), statement.Collection())
			exec := Exec{Options: ExecOptions{IntermediateBytes: -1, JoinPairBytes: -1}}
			if _, err := statement.RunInto(&exec, source, nil); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := statement.RunInto(&exec, source, nil); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
