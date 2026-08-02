package query

import (
	"errors"
	"testing"
)

func TestNestedStatementFrameDoesNotRetainCallerArguments(t *testing.T) {
	stmt, err := PrepareStatement(`
		WITH selected AS MATERIALIZED (
			SELECT id FROM customers WHERE tier = ?
		)
		SELECT id FROM selected WHERE id = ?`)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Release()
	if stmt.nested == nil {
		t.Fatal("test statement did not prepare nested state")
	}

	snapshot := subqueryDatabase(t)
	var exec Exec
	args := []any{[]byte("pro"), []byte("c1")}
	if _, err := stmt.RunInto(
		&exec, FromDatabase(snapshot, stmt.Collection()), args,
	); err != nil {
		t.Fatal(err)
	}
	if stmt.nested.frame.args != nil {
		t.Fatal("successful nested execution retained caller arguments")
	}

	exec.Options.IntermediateBytes = 1
	if _, err := stmt.RunInto(
		&exec, FromDatabase(snapshot, stmt.Collection()), args,
	); !errors.Is(err, ErrIntermediateBudget) {
		t.Fatalf("bounded execution error = %v, want %v", err, ErrIntermediateBudget)
	}
	if stmt.nested.frame.args != nil {
		t.Fatal("failed nested execution retained caller arguments")
	}

	exec.Options.IntermediateBytes = -1
	if _, err := stmt.RunInto(
		&exec, FromDatabase(snapshot, stmt.Collection()), []any{"pro", "c1"},
	); err != nil {
		t.Fatalf("reuse after bounded failure: %v", err)
	}
	if stmt.nested.frame.args != nil {
		t.Fatal("reused nested execution retained caller arguments")
	}
	exec.Release()
}
