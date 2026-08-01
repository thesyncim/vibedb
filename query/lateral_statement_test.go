package query

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	sqlast "github.com/thesyncim/vibedb/sql"
	"github.com/thesyncim/vibedb/store"
)

func lateralStatementDatabase(t testing.TB) *store.Database {
	t.Helper()
	db := &store.Database{}
	put := func(name string, docs ...string) {
		collection, err := db.CreateCollection(name, store.Options{ChunkDocuments: 2})
		if err != nil {
			t.Fatal(err)
		}
		for i, doc := range docs {
			if _, err := collection.Put(fmt.Sprintf("%s-%d", name, i), []byte(doc)); err != nil {
				t.Fatal(err)
			}
		}
	}
	put("accounts",
		`{"id":1,"enabled":true}`,
		`{"id":2,"enabled":false}`,
		`{"id":3,"enabled":true}`,
	)
	put("items",
		`{"id":"a","owner":1,"active":true}`,
		`{"id":"b","owner":1,"active":false}`,
		`{"id":"c","owner":2,"active":false}`,
		`{"id":"d","owner":4,"active":true}`,
	)
	return db
}

func runLateralStatement(
	t testing.TB,
	db *store.Database,
	sql string,
	args ...any,
) (*Statement, *Exec, []string) {
	t.Helper()
	statement, err := PrepareStatement(sql)
	if err != nil {
		t.Fatal(err)
	}
	exec := new(Exec)
	cursor, err := statement.RunInto(
		exec, FromDatabase(db.Snapshot(), statement.Collection()), args,
	)
	if err != nil {
		statement.Release()
		exec.Release()
		t.Fatal(err)
	}
	rows := make([]string, 0, exec.Result.RowCount)
	for cursor.Next() {
		cells := make([]string, len(statement.Columns()))
		for i := range cells {
			cells[i] = string(cursor.Cell(i).JSON())
		}
		rows = append(rows, strings.Join(cells, ","))
	}
	return statement, exec, rows
}

func TestSQLLateralCrossAndLeftApplySemantics(t *testing.T) {
	db := lateralStatementDatabase(t)
	statement, exec, got := runLateralStatement(t, db, correlatedLateralSQL)
	defer statement.Release()
	defer exec.Release()
	want := []string{`1,"a"`, `1,"b"`, `2,"c"`}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("CROSS LATERAL rows = %q, want %q", got, want)
	}
	if gotSchema := exec.Result.Columns; len(gotSchema) != 2 ||
		gotSchema[0].Header != "id" || gotSchema[1].Header != "d.id" {
		t.Fatalf("CROSS LATERAL result schema = %+v", gotSchema)
	}
	if lateral := statement.relationJoin().operands[1].lateral; lateral.evaluations != 3 {
		t.Fatalf("child evaluations = %d, want once per left row", lateral.evaluations)
	}

	left, leftExec, got := runLateralStatement(t, db, `
		SELECT a.id, d.id FROM accounts a
		LEFT JOIN LATERAL (
			SELECT i.id, i.active FROM items i WHERE i.owner = a.id
		) d ON d.active = TRUE`)
	defer left.Release()
	defer leftExec.Release()
	want = []string{`1,"a"`, `2,null`, `3,null`}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("LEFT LATERAL rows = %q, want %q", got, want)
	}
}

func TestSQLLateralDifferentialJoinKindsAndResiduals(t *testing.T) {
	db := lateralStatementDatabase(t)
	type account struct{ id int }
	type item struct {
		id     string
		owner  int
		active bool
	}
	accounts := []account{{1}, {2}, {3}}
	items := []item{{"a", 1, true}, {"b", 1, false}, {"c", 2, false}, {"d", 4, true}}
	for _, test := range []struct {
		name     string
		join     string
		residual bool
		left     bool
	}{
		{name: "cross", join: "CROSS JOIN", residual: false},
		{name: "inner", join: "JOIN", residual: true},
		{name: "left", join: "LEFT JOIN", residual: true, left: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			on := ""
			if test.residual {
				on = " ON d.active = TRUE"
			}
			src := `SELECT a.id, d.id FROM accounts a ` + test.join + ` LATERAL (` +
				`SELECT i.id, i.active FROM items i WHERE i.owner = a.id` +
				`) d` + on
			statement, exec, got := runLateralStatement(t, db, src)
			defer statement.Release()
			defer exec.Release()
			var want []string
			for _, a := range accounts {
				found := false
				for _, i := range items {
					if i.owner != a.id || test.residual && !i.active {
						continue
					}
					found = true
					want = append(want, fmt.Sprintf(`%d,%q`, a.id, i.id))
				}
				if test.left && !found {
					want = append(want, fmt.Sprintf(`%d,null`, a.id))
				}
			}
			if strings.Join(got, "\n") != strings.Join(want, "\n") {
				t.Fatalf("rows = %q, reference = %q", got, want)
			}
		})
	}
}

func TestSQLLateralComparisonDirectionAndPlaceholderReuse(t *testing.T) {
	db := lateralStatementDatabase(t)
	statement, err := PrepareStatement(`
		SELECT a.id, d.id FROM accounts a
		JOIN LATERAL (
			SELECT i.id FROM items i
			WHERE a.id <= i.owner AND i.id <> ?
		) d ON d.id <> ?`)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	source := FromDatabase(db.Snapshot(), statement.Collection())
	var exec Exec
	for run := 0; run < 2; run++ {
		cursor, err := statement.RunInto(&exec, source, []any{"d", "c"})
		if err != nil {
			t.Fatal(err)
		}
		var got []string
		for cursor.Next() {
			got = append(got, string(cursor.Cell(0).JSON())+","+string(cursor.Cell(1).JSON()))
		}
		want := []string{`1,"a"`, `1,"b"`}
		if strings.Join(got, "\n") != strings.Join(want, "\n") {
			t.Fatalf("run %d rows = %q, want %q", run, got, want)
		}
	}
}

func TestSQLLateralChainsBindAnyPrecedingSource(t *testing.T) {
	db := lateralStatementDatabase(t)
	statement, exec, got := runLateralStatement(t, db, `
		SELECT a.id, d1.id, d2.id FROM accounts a
		CROSS JOIN LATERAL (
			SELECT i.id, i.owner FROM items i WHERE i.owner = a.id
		) d1
		LEFT JOIN LATERAL (
			SELECT i.id FROM items i WHERE i.owner = d1.owner
		) d2 ON d2.id <> d1.id`)
	defer statement.Release()
	defer exec.Release()
	want := []string{
		`1,"a","b"`, `1,"b","a"`, `2,"c",null`,
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("chained LATERAL rows = %q, want %q", got, want)
	}
}

func TestSQLLateralBudgetsCancellationAndRecovery(t *testing.T) {
	db := lateralStatementDatabase(t)
	statement, err := PrepareStatement(correlatedLateralSQL)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	source := FromDatabase(db.Snapshot(), statement.Collection())
	var exec Exec
	exec.Options.JoinPairBytes = 1
	_, err = statement.RunInto(&exec, source, nil)
	var pairBudget *JoinPairBudgetError
	if !errors.As(err, &pairBudget) || exec.Result.RowCount != 0 {
		t.Fatalf("pair budget = %T %v, rows=%d", err, err, exec.Result.RowCount)
	}

	exec.Options.JoinPairBytes = -1
	exec.Options.IntermediateBytes = 1
	_, err = statement.RunInto(&exec, source, nil)
	var intermediate *IntermediateBudgetError
	if !errors.As(err, &intermediate) || exec.Result.RowCount != 0 {
		t.Fatalf("intermediate budget = %T %v, rows=%d", err, err, exec.Result.RowCount)
	}

	var cancel CancelFlag
	cancel.Cancel()
	exec.Options.IntermediateBytes = -1
	exec.Options.Cancel = &cancel
	_, err = statement.RunInto(&exec, source, nil)
	if !errors.Is(err, ErrCanceled) || exec.Result.RowCount != 0 {
		t.Fatalf("cancel = %v, rows=%d", err, exec.Result.RowCount)
	}
	cancel.Reset()
	cursor, err := statement.RunInto(&exec, source, nil)
	if err != nil {
		t.Fatal(err)
	}
	rows := 0
	for cursor.Next() {
		rows++
	}
	if rows != 3 {
		t.Fatalf("post-error recovery rows = %d, want 3", rows)
	}
}

func TestSQLLateralExactIntermediateAccounting(t *testing.T) {
	db := lateralStatementDatabase(t)
	statement, err := PrepareStatement(correlatedLateralSQL)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	source := FromDatabase(db.Snapshot(), statement.Collection())
	exec := Exec{Options: ExecOptions{IntermediateBytes: -1, JoinPairBytes: -1}}
	if _, err := statement.RunInto(&exec, source, nil); err != nil {
		t.Fatal(err)
	}
	required := statement.nested.frame.intermediate.used
	if required <= 1 {
		t.Fatalf("accounted intermediate bytes = %d", required)
	}
	exec.Options.IntermediateBytes = required - 1
	_, err = statement.RunInto(&exec, source, nil)
	var budget *IntermediateBudgetError
	if !errors.As(err, &budget) || exec.Result.RowCount != 0 {
		t.Fatalf("one-byte-short execution = %T %v, rows=%d", err, err, exec.Result.RowCount)
	}
	exec.Options.IntermediateBytes = required
	cursor, err := statement.RunInto(&exec, source, nil)
	if err != nil {
		t.Fatalf("exact %d-byte execution: %v", required, err)
	}
	rows := 0
	for cursor.Next() {
		rows++
	}
	if rows != 3 || statement.nested.frame.intermediate.used != required {
		t.Fatalf(
			"exact execution rows/bytes = %d/%d, want 3/%d",
			rows, statement.nested.frame.intermediate.used, required,
		)
	}
}

func TestSQLLateralMidApplyTypeFailurePublishesNothingAndRecovers(t *testing.T) {
	makeDB := func(t *testing.T, outer ...string) *store.Database {
		db := &store.Database{}
		for name, docs := range map[string][]string{
			"outer_bad": outer,
			"inner_bad": {`{"id":"match","k":1}`},
		} {
			collection, err := db.CreateCollection(name, store.Options{})
			if err != nil {
				t.Fatal(err)
			}
			for i, doc := range docs {
				if _, err := collection.Put(fmt.Sprint(i), []byte(doc)); err != nil {
					t.Fatal(err)
				}
			}
		}
		return db
	}
	statement, err := PrepareStatement(`
		SELECT o.id, d.id FROM outer_bad o
		CROSS JOIN LATERAL (
			SELECT i.id FROM inner_bad i WHERE i.k = o.k
		) d`)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	bad := makeDB(t, `{"id":"first","k":1}`, `{"id":"bad","k":{"x":1}}`)
	var exec Exec
	_, err = statement.RunInto(
		&exec, FromDatabase(bad.Snapshot(), statement.Collection()), nil,
	)
	var valueErr *LateralBindingValueError
	if !errors.As(err, &valueErr) || exec.Result.RowCount != 0 {
		t.Fatalf("mid-APPLY error = %T %v, rows=%d", err, err, exec.Result.RowCount)
	}
	good := makeDB(t, `{"id":"first","k":1}`)
	cursor, err := statement.RunInto(
		&exec, FromDatabase(good.Snapshot(), statement.Collection()), nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !cursor.Next() || string(cursor.Cell(0).JSON()) != `"first"` ||
		string(cursor.Cell(1).JSON()) != `"match"` || cursor.Next() {
		t.Fatal("prepared statement did not recover after a mid-APPLY type error")
	}
}

func TestSQLLateralStrictRightArityRefusalDoesNotPoisonReuse(t *testing.T) {
	db := lateralStatementDatabase(t)
	statement, err := PrepareStatement(correlatedLateralSQL)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	source := FromDatabase(db.Snapshot(), statement.Collection())
	operand := &statement.relationJoin().operands[1]
	operand.columns++
	var exec Exec
	_, err = statement.RunInto(&exec, source, nil)
	var arity *ApplyRightArityError
	if !errors.As(err, &arity) || exec.Result.RowCount != 0 {
		t.Fatalf("arity = %T %v, rows=%d", err, err, exec.Result.RowCount)
	}
	operand.columns--
	cursor, err := statement.RunInto(&exec, source, nil)
	if err != nil {
		t.Fatal(err)
	}
	rows := 0
	for cursor.Next() {
		rows++
	}
	if rows != 3 {
		t.Fatalf("post-arity recovery rows = %d, want 3", rows)
	}
}

func TestSQLLateralExplainIsTruthful(t *testing.T) {
	db := lateralStatementDatabase(t)
	statement, err := PrepareStatement(`
		SELECT a.id, d.id FROM accounts a
		LEFT JOIN LATERAL (
			SELECT i.id, i.active FROM items i WHERE i.owner = a.id
		) d ON d.active = TRUE`)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	logical, err := statement.Explain()
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`"type":"left"`, `"access_path":"lateral-apply"`,
		`"algorithm":"bounded-lateral-apply"`, `"residual":true`,
	} {
		if !strings.Contains(logical, want) {
			t.Errorf("EXPLAIN missing %s: %s", want, logical)
		}
	}
	if strings.Contains(logical, `"build_side"`) {
		t.Fatalf("LATERAL APPLY claims a hash build side: %s", logical)
	}
	var exec Exec
	if _, err := statement.RunInto(
		&exec, FromDatabase(db.Snapshot(), statement.Collection()), nil,
	); err != nil {
		t.Fatal(err)
	}
	actual, err := statement.ExplainAnalyze(ExplainOptions{}, ExplainAnalysis{
		Rows: exec.Result.RowCount, Stats: exec.Stats,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`"actual_algorithm":"bounded-lateral-apply"`,
		fmt.Sprintf(`"pairs":%d`, exec.Stats.JoinPairs),
	} {
		if !strings.Contains(actual, want) {
			t.Errorf("EXPLAIN ANALYZE missing %s: %s", want, actual)
		}
	}
}

func TestSQLLateralExactDecimalStringNullAndMissingBindings(t *testing.T) {
	db := &store.Database{}
	put := func(name string, docs ...string) {
		collection, err := db.CreateCollection(name, store.Options{})
		if err != nil {
			t.Fatal(err)
		}
		for i, doc := range docs {
			if _, err := collection.Put(fmt.Sprint(i), []byte(doc)); err != nil {
				t.Fatal(err)
			}
		}
	}
	put("outer_exact",
		`{"id":"decimal","k":1.0}`,
		`{"id":"wide","k":9007199254740993}`,
		`{"id":"escaped","k":"\u0061"}`,
		`{"id":"null","k":null}`,
		`{"id":"missing"}`,
	)
	put("inner_exact",
		`{"id":"decimal","k":10e-1}`,
		`{"id":"wide","k":9007199254740993.0}`,
		`{"id":"adjacent","k":9007199254740992}`,
		`{"id":"plain","k":"a"}`,
		`{"id":"null","k":null}`,
		`{"id":"missing"}`,
	)
	statement, exec, got := runLateralStatement(t, db, `
		SELECT o.id, d.id FROM outer_exact o
		LEFT JOIN LATERAL (
			SELECT i.id FROM inner_exact i WHERE i.k = o.k
		) d ON TRUE`)
	defer statement.Release()
	defer exec.Release()
	want := []string{
		`"decimal","decimal"`, `"wide","wide"`, `"escaped","plain"`,
		`"null",null`, `"missing",null`,
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("exact LATERAL rows = %q, want %q", got, want)
	}
}

func TestSQLLateralWarmExecutionIsAllocationFree(t *testing.T) {
	db := lateralStatementDatabase(t)
	statement, err := PrepareStatement(correlatedLateralSQL)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	source := FromDatabase(db.Snapshot(), statement.Collection())
	exec := Exec{Options: ExecOptions{IntermediateBytes: -1, JoinPairBytes: -1}}
	defer exec.Release()
	run := func() {
		cursor, err := statement.RunInto(&exec, source, nil)
		if err != nil {
			panic(err)
		}
		for cursor.Next() {
			sqlSink += len(cursor.Cell(1).Payload())
		}
	}
	run()
	run()
	if got := testing.AllocsPerRun(100, run); got != 0 {
		t.Fatalf("warmed LATERAL allocations = %.2f, want 0", got)
	}
}

func TestSQLLateralIndependentPreparedStatementsRace(t *testing.T) {
	db := lateralStatementDatabase(t)
	snapshot := db.Snapshot()
	const workers = 6
	var wait sync.WaitGroup
	errs := make(chan error, workers)
	for worker := 0; worker < workers; worker++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			statement, err := PrepareStatement(correlatedLateralSQL)
			if err != nil {
				errs <- err
				return
			}
			defer statement.Release()
			var exec Exec
			defer exec.Release()
			source := FromDatabase(snapshot, statement.Collection())
			for run := 0; run < 20; run++ {
				cursor, err := statement.RunInto(&exec, source, nil)
				if err != nil {
					errs <- err
					return
				}
				rows := 0
				for cursor.Next() {
					rows++
				}
				if rows != 3 {
					errs <- fmt.Errorf("run %d returned %d rows", run, rows)
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

func TestSQLLateralUnsupportedReferenceLocationStaysTyped(t *testing.T) {
	const src = `SELECT a.id, d.outer_id FROM accounts a CROSS JOIN LATERAL (` +
		`SELECT a.id AS outer_id FROM items i WHERE i.owner = a.id) d`
	statement, err := PrepareStatement(src)
	if statement != nil {
		statement.Release()
		t.Fatal("projected outer reference prepared without a projection adapter")
	}
	var unsupported *sqlast.FeatureNotSupportedError
	if !errors.As(err, &unsupported) || unsupported.Pos != strings.Index(src, "a.id AS") {
		t.Fatalf("error = %T %+v", err, unsupported)
	}
}
