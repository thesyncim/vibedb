package query

import (
	"errors"
	"testing"
)

func TestSQLSubqueriesShareOneIntermediateBudget(t *testing.T) {
	catalog := subqueryDatabase(t)
	stmt, err := PrepareStatement(
		`SELECT id FROM orders WHERE ` +
			`customer IN (SELECT id FROM customers WHERE tier = 'pro') OR ` +
			`customer IN (SELECT id FROM customers WHERE score = 9)`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Release()

	var exec Exec
	if _, err := stmt.RunInto(&exec, FromDatabase(catalog, "orders"), nil); err != nil {
		t.Fatal(err)
	}
	if len(stmt.nested.subqueries) != 2 {
		t.Fatalf("nested subqueries = %d, want 2", len(stmt.nested.subqueries))
	}
	measure := func(sub *statementSubquery) int64 {
		t.Helper()
		var nested Exec
		if _, err := sub.stmt.RunInto(
			&nested,
			FromDatabase(catalog, sub.stmt.Collection()),
			nil,
		); err != nil {
			t.Fatal(err)
		}
		return nested.Result.resultBytesUsed
	}
	first := measure(&stmt.nested.subqueries[0])
	second := measure(&stmt.nested.subqueries[1])
	if first <= 0 || second <= 0 {
		t.Fatalf("nested result charges = (%d, %d), want positive", first, second)
	}

	exec.Options.IntermediateBytes = first + second - 1
	_, err = stmt.RunInto(&exec, FromDatabase(catalog, "orders"), nil)
	var budgetErr *IntermediateBudgetError
	if !errors.As(err, &budgetErr) || !errors.Is(err, ErrIntermediateBudget) {
		t.Fatalf("RunInto error = %#v, want shared intermediate budget", err)
	}
	if exec.Result.RowCount != 0 {
		t.Fatalf("failed statement retained %d rows from its prior success", exec.Result.RowCount)
	}
}

func TestSQLSubqueryIntermediateBudgetDoesNotMultiplyWithDepth(t *testing.T) {
	catalog := subqueryDatabase(t)
	stmt, err := PrepareStatement(
		`SELECT id FROM orders WHERE customer IN (` +
			`SELECT id FROM customers WHERE id IN (` +
			`SELECT customer FROM orders WHERE id = 'o1'))`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Release()

	var exec Exec
	exec.Options.IntermediateBytes = 1
	_, err = stmt.RunInto(&exec, FromDatabase(catalog, "orders"), nil)
	var budgetErr *IntermediateBudgetError
	if !errors.As(err, &budgetErr) {
		t.Fatalf("RunInto error = %#v, want nested intermediate budget", err)
	}
}

func TestSQLPredicateSubqueryIgnoresFinalResultRowLimit(t *testing.T) {
	catalog := subqueryDatabase(t)
	stmt, err := PrepareStatement(
		`SELECT id FROM orders WHERE customer IN (` +
			`SELECT id FROM customers) ORDER BY id LIMIT 1`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Release()
	var exec Exec
	exec.Options.ResultRows = 1
	cursor, err := stmt.RunInto(&exec, FromDatabase(catalog, "orders"), nil)
	if err != nil {
		t.Fatalf("RunInto: %v", err)
	}
	if !cursor.Next() || cursor.Cell(0).String() != `"o1"` || cursor.Next() {
		t.Fatal("outer LIMIT 1 did not return exactly the first predicate match")
	}
}

func TestSQLPredicateValuesFailBeforeGrowthAndReleaseChildBorrows(t *testing.T) {
	catalog := subqueryDatabase(t)
	const text = `SELECT id FROM orders WHERE customer IN (` +
		`SELECT id FROM customers)`

	probe, err := PrepareStatement(text)
	if err != nil {
		t.Fatal(err)
	}
	probeSub := &probe.nested.subqueries[0]
	var nested Exec
	if _, err := probeSub.stmt.RunInto(
		&nested,
		FromDatabase(catalog, probeSub.stmt.Collection()),
		nil,
	); err != nil {
		t.Fatal(err)
	}
	resultBytes := nested.Result.resultBytesUsed
	probe.Release()
	// Three known strings retain two decoded bytes each.
	valueBytes := predicateValuesRetainedBytes(3, 6)

	stmt, err := PrepareStatement(text)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Release()
	var exec Exec
	exec.Options.IntermediateBytes = resultBytes + valueBytes - 1
	_, err = stmt.RunInto(&exec, FromDatabase(catalog, "orders"), nil)
	var budgetErr *IntermediateBudgetError
	if !errors.As(err, &budgetErr) || budgetErr.Resource != "predicate subquery values" {
		t.Fatalf("RunInto error = %#v, want predicate-value budget", err)
	}
	sub := &stmt.nested.subqueries[0]
	if cap(sub.slots) != 0 || cap(sub.values) != 0 {
		t.Fatalf("rejected predicate values grew slots=%d values=%d",
			cap(sub.slots), cap(sub.values))
	}
	if sub.resultBytes != 0 || sub.activeBytes != 0 || sub.exec.Result.RowCount != 0 {
		t.Fatalf("failed predicate retained result=%d values=%d rows=%d",
			sub.resultBytes, sub.activeBytes, sub.exec.Result.RowCount)
	}
	assertWorkspaceBorrowedViewsCleared(t, &sub.exec.Workspace)
}

func TestSQLPredicateSubquerySuccessReleasesPrivateMaterializations(t *testing.T) {
	catalog := subqueryDatabase(t)
	stmt, err := PrepareStatement(
		`SELECT id FROM orders WHERE customer IN (` +
			`SELECT id FROM customers) ORDER BY id`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Release()
	var exec Exec
	if _, err := stmt.RunInto(&exec, FromDatabase(catalog, "orders"), nil); err != nil {
		t.Fatal(err)
	}
	sub := &stmt.nested.subqueries[0]
	if sub.resultBytes != 0 || sub.activeBytes != 0 ||
		sub.exec.Result.RowCount != 0 || len(sub.slots) != 0 || len(sub.values) != 0 {
		t.Fatalf("successful predicate retained private materialization: result=%d values=%d rows=%d slots=%d interfaces=%d",
			sub.resultBytes, sub.activeBytes, sub.exec.Result.RowCount,
			len(sub.slots), len(sub.values))
	}
	assertWorkspaceBorrowedViewsCleared(t, &sub.exec.Workspace)
}
