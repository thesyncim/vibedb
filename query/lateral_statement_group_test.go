package query

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	sqlast "github.com/thesyncim/vibedb/sql"
	"github.com/thesyncim/vibedb/store"
)

func TestSQLLateralCorrelatedGroupedAggregatesAndHaving(t *testing.T) {
	db := lateralStatementDatabase(t)
	statement, exec, got := runLateralStatement(t, db, `
		SELECT a.id, d.owner, d.n, d.total, d.mean, d.lo, d.hi
		FROM accounts a LEFT JOIN LATERAL (
			SELECT a.id AS owner, COUNT(a.id) AS n, SUM(a.id) AS total,
				AVG(a.id) AS mean, MIN(a.id) AS lo, MAX(a.id) AS hi
			FROM items i WHERE i.owner = a.id
			GROUP BY a.id HAVING SUM(a.id) > 1
		) d ON TRUE`)
	t.Cleanup(statement.Release)
	t.Cleanup(exec.Release)
	want := []string{
		`1,1,2,2,1,1,1`,
		`2,2,1,2,2,2,2`,
		`3,null,null,null,null,null,null`,
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("grouped correlated aggregates = %q, want %q", got, want)
	}
	if schema := statement.Columns(); strings.Join(schema, ",") !=
		"id,d.owner,d.n,d.total,d.mean,d.lo,d.hi" {
		t.Fatalf("schema = %q", schema)
	}
}

func TestSQLLateralCorrelatedAggregateEmptyAndMixedGroups(t *testing.T) {
	db := lateralStatementDatabase(t)
	statement, exec, got := runLateralStatement(t, db, `
		SELECT a.id, d.total FROM accounts a CROSS JOIN LATERAL (
			SELECT SUM(a.id) AS total FROM items i WHERE i.owner = a.id
		) d`)
	t.Cleanup(statement.Release)
	t.Cleanup(exec.Release)
	want := []string{`1,2`, `2,2`, `3,null`}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("ungrouped correlated SUM = %q, want %q", got, want)
	}

	mixed, mixedExec, got := runLateralStatement(t, db, `
		SELECT a.id, d.active, d.total FROM accounts a CROSS JOIN LATERAL (
			SELECT i.active, SUM(a.id) AS total FROM items i
			WHERE i.owner = a.id GROUP BY a.id, i.active
			HAVING SUM(a.id) >= ?
		) d`, int64(1))
	t.Cleanup(mixed.Release)
	t.Cleanup(mixedExec.Release)
	want = []string{`1,true,1`, `1,false,1`, `2,false,2`}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("mixed grouped correlated SUM = %q, want %q", got, want)
	}
}

func TestSQLLateralCorrelatedGroupedWildcardAndInnerResidual(t *testing.T) {
	db := lateralStatementDatabase(t)
	statement, exec, got := runLateralStatement(t, db, `
		SELECT d.* FROM accounts a INNER JOIN LATERAL (
			SELECT a.id AS owner, SUM(a.id) AS total, COUNT(*) AS n
			FROM items i WHERE i.owner = a.id GROUP BY a.id
		) d ON d.total > 1`)
	t.Cleanup(statement.Release)
	t.Cleanup(exec.Release)
	if want := []string{`1,2,2`, `2,2,1`}; strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("wildcard/INNER residual rows = %q, want %q", got, want)
	}
	if names := statement.Columns(); strings.Join(names, ",") != "owner,total,n" {
		t.Fatalf("wildcard schema = %q, want owner,total,n", names)
	}

	plain, plainExec, got := runLateralStatement(t, db, `
		SELECT d.* FROM accounts a CROSS JOIN LATERAL (
			SELECT SUM(a.id) FROM items i WHERE i.owner = a.id
		) d`)
	t.Cleanup(plain.Release)
	t.Cleanup(plainExec.Release)
	if names := plain.Columns(); len(names) != 1 || names[0] != "sum(id)" {
		t.Fatalf("unaliased aggregate wildcard schema = %q, want sum(id)", names)
	}
	if want := []string{`2`, `2`, `null`}; strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("unaliased aggregate rows = %q, want %q", got, want)
	}
}

func TestSQLLateralCorrelatedHavingUnprojectedKeysAndThreeValuedLogic(t *testing.T) {
	db := lateralStatementDatabase(t)
	statement, exec, got := runLateralStatement(t, db, `
		SELECT a.id, d.n FROM accounts a LEFT JOIN LATERAL (
			SELECT COUNT(*) AS n FROM items i WHERE i.owner = a.id
			GROUP BY a.id, i.active
			HAVING a.id > 1 AND i.active IN (FALSE, ?)
		) d ON TRUE`, nil)
	t.Cleanup(statement.Release)
	t.Cleanup(exec.Release)
	// The NULL list member does not erase the exact FALSE match. Account 1 is
	// rejected by the captured key; account 3 has no groups and is null-extended.
	want := []string{`1,null`, `2,1`, `3,null`}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("unprojected HAVING keys = %q, want %q", got, want)
	}
}

func TestSQLLateralCorrelatedAggregateExactDecimalsNullMissingAndContainers(t *testing.T) {
	db := &store.Database{}
	outer, err := db.CreateCollection("lateral_exact_outer", store.Options{ChunkDocuments: 2})
	if err != nil {
		t.Fatal(err)
	}
	for i, doc := range []string{
		`{"id":1,"v":9007199254740993.0000000000000000001}`,
		`{"id":2,"v":null}`,
		`{"id":3}`,
		`{"id":4,"v":{"exact":true}}`,
	} {
		if _, err := outer.Put(fmt.Sprint(i), []byte(doc)); err != nil {
			t.Fatal(err)
		}
	}
	inner, err := db.CreateCollection("lateral_exact_inner", store.Options{ChunkDocuments: 2})
	if err != nil {
		t.Fatal(err)
	}
	for i, doc := range []string{
		`{"owner":1,"x":9007199254740993.0000000000000000001}`,
		`{"owner":1,"x":9007199254740993.0000000000000000001}`,
		`{"owner":2,"x":null}`, `{"owner":3}`,
		`{"owner":4,"x":{"exact":true}}`,
	} {
		if _, err := inner.Put(fmt.Sprint(i), []byte(doc)); err != nil {
			t.Fatal(err)
		}
	}
	statement, exec, got := runLateralStatement(t, db, `
		SELECT a.id, d.n, d.total, d.mean, d.lo FROM lateral_exact_outer a
		CROSS JOIN LATERAL (
			SELECT COUNT(a.v) AS n, SUM(a.v) AS total, AVG(a.v) AS mean,
				MIN(a.v) AS lo
			FROM lateral_exact_inner i WHERE i.owner = a.id
		) d`)
	t.Cleanup(statement.Release)
	t.Cleanup(exec.Release)
	want := []string{
		`1,2,18014398509481986.0000000000000000002,9007199254740993,9007199254740993.0000000000000000001`,
		`2,0,null,null,null`,
		`3,0,null,null,null`,
		`4,1,null,null,null`,
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("exact/null/missing/container aggregates = %q, want %q", got, want)
	}
	oracle, oracleExec, oracleRows := runLateralStatement(t, db, `
		SELECT a.id, d.n, d.total, d.mean, d.lo FROM lateral_exact_outer a
		CROSS JOIN LATERAL (
			SELECT COUNT(i.x) AS n, SUM(i.x) AS total, AVG(i.x) AS mean,
				MIN(i.x) AS lo
			FROM lateral_exact_inner i WHERE i.owner = a.id
		) d`)
	t.Cleanup(oracle.Release)
	t.Cleanup(oracleExec.Release)
	if strings.Join(got, "\n") != strings.Join(oracleRows, "\n") {
		t.Fatalf("captured/local aggregate differential = %q / %q", got, oracleRows)
	}
}

func TestSQLLateralCorrelatedGroupedAggregateFailuresAreAtomicAndRecover(t *testing.T) {
	db := lateralStatementDatabase(t)
	statement, err := PrepareStatement(`
		SELECT a.id, d.total FROM accounts a CROSS JOIN LATERAL (
			SELECT SUM(a.id) AS total FROM items i WHERE i.owner = a.id
		) d`)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	source := FromDatabase(db.Snapshot(), statement.Collection())
	exec := &Exec{Options: ExecOptions{AggregateBytes: aggregateAccBaseBytes}}
	defer exec.Release()
	if _, err := statement.RunInto(exec, source, nil); err == nil {
		t.Fatal("tiny exact aggregate budget succeeded")
	} else {
		var budget *AggregateBudgetError
		if !errors.As(err, &budget) || exec.Result.RowCount != 0 {
			t.Fatalf("budget error/result = %T %v / %d", err, err, exec.Result.RowCount)
		}
	}
	exec.Options.AggregateBytes = 0
	cursor, err := statement.RunInto(exec, source, nil)
	if err != nil {
		t.Fatal(err)
	}
	rows := 0
	for cursor.Next() {
		rows++
	}
	if rows != 3 {
		t.Fatalf("recovered rows = %d, want 3", rows)
	}
}

func TestSQLLateralCorrelatedHavingUnsupportedOperatorIsPositioned(t *testing.T) {
	src := `SELECT a.id, d.n FROM accounts a CROSS JOIN LATERAL (` +
		`SELECT COUNT(*) AS n FROM items i GROUP BY a.id HAVING a.id IS MISSING) d`
	statement, err := PrepareStatement(src)
	if statement != nil {
		statement.Release()
		t.Fatal("unsupported post-reduction IS MISSING prepared")
	}
	var unsupported *sqlast.FeatureNotSupportedError
	position := strings.Index(src, "a.id IS MISSING")
	if !errors.As(err, &unsupported) || unsupported.Pos != position {
		t.Fatalf("error = %T %+v, want positioned 0A000 at %d", err, unsupported, position)
	}
}

func TestSQLLateralCorrelatedHavingTailRefusalsArePositioned(t *testing.T) {
	for _, test := range []struct {
		name string
		tail string
	}{
		{name: "limit", tail: " LIMIT 1"},
		{name: "offset", tail: " OFFSET 1"},
		{name: "aggregate order", tail: " ORDER BY a.id"},
	} {
		t.Run(test.name, func(t *testing.T) {
			src := `/* préfix */ SELECT a.id, d.total FROM accounts a CROSS JOIN LATERAL (` +
				`SELECT SUM(a.id) AS total FROM items i WHERE i.owner = a.id ` +
				`GROUP BY a.id HAVING SUM(a.id) > 0` + test.tail + `) d`
			statement, err := PrepareStatement(src)
			if statement != nil {
				statement.Release()
				t.Fatal("unsupported correlated reduction tail prepared")
			}
			var unsupported *sqlast.FeatureNotSupportedError
			if !errors.As(err, &unsupported) {
				t.Fatalf("error = %T %v, want positioned 0A000", err, err)
			}
			if unsupported.Pos < strings.Index(src, strings.TrimSpace(test.tail)) {
				t.Fatalf("error position = %d, want tail at/after %d",
					unsupported.Pos, strings.Index(src, strings.TrimSpace(test.tail)))
			}
		})
	}
}

func TestSQLLateralCorrelatedGroupedBudgetsCancellationAndRecovery(t *testing.T) {
	db := lateralStatementDatabase(t)
	statement, err := PrepareStatement(`
		SELECT a.id, d.total FROM accounts a LEFT JOIN LATERAL (
			SELECT SUM(a.id) AS total FROM items i WHERE i.owner = a.id
			GROUP BY a.id HAVING SUM(a.id) >= ?
		) d ON TRUE`)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	source := FromDatabase(db.Snapshot(), statement.Collection())
	exec := Exec{Options: ExecOptions{IntermediateBytes: -1, JoinPairBytes: -1}}
	defer exec.Release()
	threshold := Number("1")
	if _, err := statement.RunInto(&exec, source, []any{&threshold}); err != nil {
		t.Fatal(err)
	}
	required := statement.nested.frame.intermediate.used
	if required <= 0 {
		t.Fatalf("retained intermediate bytes = %d", required)
	}
	exec.Options.IntermediateBytes = required - 1
	_, err = statement.RunInto(&exec, source, []any{&threshold})
	var budget *IntermediateBudgetError
	if !errors.As(err, &budget) || exec.Result.RowCount != 0 {
		t.Fatalf("one-byte-short execution = %T %v, rows=%d", err, err, exec.Result.RowCount)
	}
	var cancel CancelFlag
	cancel.Cancel()
	exec.Options.IntermediateBytes = required
	exec.Options.Cancel = &cancel
	_, err = statement.RunInto(&exec, source, []any{&threshold})
	if !errors.Is(err, ErrCanceled) || exec.Result.RowCount != 0 {
		t.Fatalf("canceled execution = %v, rows=%d", err, exec.Result.RowCount)
	}
	cancel.Reset()
	cursor, err := statement.RunInto(&exec, source, []any{&threshold})
	if err != nil {
		t.Fatal(err)
	}
	rows := 0
	for cursor.Next() {
		rows++
	}
	if rows != 3 || statement.nested.frame.intermediate.used != required {
		t.Fatalf("recovery rows/bytes = %d/%d, want 3/%d",
			rows, statement.nested.frame.intermediate.used, required)
	}
}

func TestSQLLateralCorrelatedGroupedWarmExecutionIsAllocationFree(t *testing.T) {
	db := lateralStatementDatabase(t)
	statement, err := PrepareStatement(`
		SELECT a.id, d.total FROM accounts a LEFT JOIN LATERAL (
			SELECT SUM(a.id) AS total FROM items i WHERE i.owner = a.id
			GROUP BY a.id HAVING SUM(a.id) >= ?
		) d ON TRUE`)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	source := FromDatabase(db.Snapshot(), statement.Collection())
	exec := Exec{Options: ExecOptions{IntermediateBytes: -1, JoinPairBytes: -1}}
	defer exec.Release()
	threshold := Number("1")
	args := []any{&threshold}
	run := func() {
		cursor, err := statement.RunInto(&exec, source, args)
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
		t.Fatalf("warmed grouped LATERAL allocations = %.2f, want 0", got)
	}
}

func TestSQLLateralCorrelatedGroupedIndependentPreparedStatementsRace(t *testing.T) {
	db := lateralStatementDatabase(t)
	snapshot := db.Snapshot()
	const workers = 4
	var wait sync.WaitGroup
	errs := make(chan error, workers)
	for worker := 0; worker < workers; worker++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			statement, err := PrepareStatement(`
				SELECT a.id, d.total FROM accounts a CROSS JOIN LATERAL (
					SELECT SUM(a.id) AS total FROM items i WHERE i.owner = a.id
					GROUP BY a.id HAVING SUM(a.id) >= 1
				) d`)
			if err != nil {
				errs <- err
				return
			}
			defer statement.Release()
			var exec Exec
			defer exec.Release()
			for run := 0; run < 20; run++ {
				cursor, err := statement.RunInto(
					&exec, FromDatabase(snapshot, statement.Collection()), nil,
				)
				if err != nil {
					errs <- err
					return
				}
				var rows int
				for cursor.Next() {
					rows++
				}
				if rows != 2 {
					errs <- fmt.Errorf("grouped race run returned %d rows", rows)
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

func TestLateralMultiplyDecimalMatchesRepeatedExactAccumulation(t *testing.T) {
	for _, spelling := range []string{"0.1", "-3.75", "9007199254740993.0000000001"} {
		for count := 1; count <= 17; count++ {
			value := joinNumberScalar([]byte(spelling))
			var direct, repeated aggAcc
			var directBudget, repeatedBudget aggregateBudget
			directBudget.begin(defaultAggregateBytes)
			repeatedBudget.begin(defaultAggregateBytes)
			number, err := direct.number(&directBudget)
			if err != nil {
				t.Fatal(err)
			}
			if err := number.sum.add(value, &direct.lease, &directBudget); err != nil {
				t.Fatal(err)
			}
			number.n = count
			if err := lateralMultiplyDecimal(&number.sum, count, &direct.lease, &directBudget); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < count; i++ {
				if err := repeated.accumulateNumber(aggSum, value, &repeatedBudget); err != nil {
					t.Fatal(err)
				}
			}
			var directWork, repeatedWork Workspace
			directWork.aggregateBudget.begin(defaultAggregateBytes)
			repeatedWork.aggregateBudget.begin(defaultAggregateBytes)
			got, err := directWork.exactSumCell(&direct)
			if err != nil {
				t.Fatal(err)
			}
			want, err := repeatedWork.exactSumCell(&repeated)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got.AppendJSON(nil), want.AppendJSON(nil)) {
				t.Fatalf("%s x %d = %s, want %s", spelling, count, got.String(), want.String())
			}
		}
	}
}
