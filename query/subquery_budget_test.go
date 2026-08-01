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
	first := stmt.nested.subqueries[0].exec.Result.resultBytesUsed
	second := stmt.nested.subqueries[1].exec.Result.resultBytesUsed
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
