package query

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
)

const nestedInheritedLateralSQL = `
	SELECT a.id, q.id FROM accounts a CROSS JOIN LATERAL (
		SELECT d.id AS id
		FROM items i CROSS JOIN LATERAL (
			SELECT x.id FROM items x
			WHERE x.owner = a.id AND x.active = i.active
		) d
		WHERE i.owner = a.id
	) q`

func TestSQLLateralDepthTwoInheritedFrame(t *testing.T) {
	db := lateralStatementDatabase(t)
	statement, exec, got := runLateralStatement(
		t, db, nestedInheritedLateralSQL,
	)
	defer statement.Release()
	defer exec.Release()

	want := []string{`1,"a"`, `1,"b"`, `2,"c"`}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("nested inherited LATERAL rows = %q, want %q", got, want)
	}
	outerOperand := &statement.relationJoin().operands[1]
	outer := outerOperand.lateral
	inner := outerOperand.stmt.relationJoin().operands[1].lateral
	if outer == nil || inner == nil {
		t.Fatal("nested inherited LATERAL adapters were not prepared")
	}
	if len(inner.inherited) != 2 || inner.inherited[0].apply == nil &&
		inner.inherited[1].apply == nil {
		t.Fatalf("inner inherited binding plan = %+v", inner.inherited)
	}
	if outer.bindingReady || inner.bindingReady {
		t.Fatal("successful execution retained an active correlation frame")
	}
}

func TestSQLLateralDepthThreeInheritedFrame(t *testing.T) {
	db := lateralStatementDatabase(t)
	statement, exec, got := runLateralStatement(t, db, `
		SELECT a.id, q.id FROM accounts a CROSS JOIN LATERAL (
			SELECT d.id AS id
			FROM items i CROSS JOIN LATERAL (
				SELECT e.id AS id
				FROM items x CROSS JOIN LATERAL (
					SELECT y.id FROM items y
					WHERE y.owner = a.id AND y.active = x.active
				) e
				WHERE x.owner = a.id AND x.active = i.active
			) d
			WHERE i.owner = a.id
		) q`)
	defer statement.Release()
	defer exec.Release()

	want := []string{`1,"a"`, `1,"b"`, `2,"c"`}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("depth-three inherited LATERAL rows = %q, want %q", got, want)
	}
	levelOne := statement.relationJoin().operands[1].lateral
	levelTwoOperand := &statement.relationJoin().operands[1].stmt.relationJoin().operands[1]
	levelTwo := levelTwoOperand.lateral
	levelThree := levelTwoOperand.stmt.relationJoin().operands[1].lateral
	if levelOne == nil || levelTwo == nil || levelThree == nil {
		t.Fatal("three lexical APPLY frames were not prepared")
	}
	inherited := false
	for i := range levelThree.inherited {
		if levelThree.inherited[i].apply == levelTwo {
			inherited = true
		}
	}
	if !inherited {
		t.Fatal("deep binding did not inherit through the immediate lexical frame")
	}
	if levelOne.bindingReady || levelTwo.bindingReady || levelThree.bindingReady {
		t.Fatal("nested execution retained a stale active frame")
	}
}

func TestSQLLateralInheritedFrameBudgetsCancellationAndRecovery(t *testing.T) {
	db := lateralStatementDatabase(t)
	statement, err := PrepareStatement(nestedInheritedLateralSQL)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	source := FromDatabase(db.Snapshot(), statement.Collection())
	exec := Exec{Options: ExecOptions{IntermediateBytes: -1, JoinPairBytes: -1}}
	defer exec.Release()
	if _, err := statement.RunInto(&exec, source, nil); err != nil {
		t.Fatal(err)
	}
	required := statement.nested.frame.intermediate.used
	if required <= 1 {
		t.Fatalf("nested APPLY peak bytes = %d", required)
	}
	// A nested child releases transient reservations before the root returns,
	// so the retained total can be below the execution peak. Find the exact
	// admission boundary; this test then proves required-1 fails before output
	// while required succeeds.
	low := required - 1
	for {
		exec.Options.IntermediateBytes = required
		_, err = statement.RunInto(&exec, source, nil)
		if err == nil {
			break
		}
		var peak *IntermediateBudgetError
		if !errors.As(err, &peak) || required > int64(^uint64(0)>>2) {
			t.Fatalf("discovering nested APPLY peak at %d: %T %v",
				required, err, err)
		}
		low = required
		required *= 2
	}
	for low+1 < required {
		middle := low + (required-low)/2
		exec.Options.IntermediateBytes = middle
		_, err = statement.RunInto(&exec, source, nil)
		if err == nil {
			required = middle
			continue
		}
		var peak *IntermediateBudgetError
		if !errors.As(err, &peak) {
			t.Fatalf("probing nested APPLY peak at %d: %T %v", middle, err, err)
		}
		low = middle
	}

	exec.Options.IntermediateBytes = required - 1
	_, err = statement.RunInto(&exec, source, nil)
	var budget *IntermediateBudgetError
	if !errors.As(err, &budget) || exec.Result.RowCount != 0 {
		t.Fatalf("one-byte-short nested APPLY = %T %v, rows=%d",
			err, err, exec.Result.RowCount)
	}
	assertLateralFramesInactive(t, statement)

	var cancel CancelFlag
	cancel.Cancel()
	exec.Options.IntermediateBytes = required
	exec.Options.Cancel = &cancel
	_, err = statement.RunInto(&exec, source, nil)
	if !errors.Is(err, ErrCanceled) || exec.Result.RowCount != 0 {
		t.Fatalf("canceled nested APPLY = %v, rows=%d", err, exec.Result.RowCount)
	}
	assertLateralFramesInactive(t, statement)

	exec.Options.Cancel = nil
	cursor, err := statement.RunInto(&exec, source, nil)
	if err != nil {
		t.Fatal(err)
	}
	rows := 0
	for cursor.Next() {
		rows++
	}
	if rows != 3 {
		t.Fatalf("recovered nested APPLY rows = %d, want 3", rows)
	}
	assertLateralFramesInactive(t, statement)
}

func TestSQLLateralInheritedFrameWarmExecutionIsAllocationFree(t *testing.T) {
	db := lateralStatementDatabase(t)
	statement, err := PrepareStatement(nestedInheritedLateralSQL)
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
			t.Fatal(err)
		}
		rows := 0
		for cursor.Next() {
			rows++
		}
		if rows != 3 {
			t.Fatalf("nested APPLY rows = %d, want 3", rows)
		}
	}
	run()
	if got := testing.AllocsPerRun(100, run); got != 0 {
		t.Fatalf("warmed inherited LATERAL allocations = %.2f, want 0", got)
	}
}

func TestSQLLateralInheritedFrameIndependentPreparedStatementsRace(t *testing.T) {
	db := lateralStatementDatabase(t)
	snapshot := db.Snapshot()
	const workers = 4
	var wait sync.WaitGroup
	errs := make(chan error, workers)
	for worker := 0; worker < workers; worker++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			statement, err := PrepareStatement(nestedInheritedLateralSQL)
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
					errs <- fmt.Errorf("nested race run returned %d rows", rows)
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

func assertLateralFramesInactive(t testing.TB, statement *Statement) {
	t.Helper()
	root := &statement.relationJoin().operands[1]
	outer := root.lateral
	inner := root.stmt.relationJoin().operands[1].lateral
	if outer.bindingReady || inner.bindingReady {
		t.Fatalf("stale active LATERAL frames: outer=%t inner=%t",
			outer.bindingReady, inner.bindingReady)
	}
}
