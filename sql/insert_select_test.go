package sql

import (
	"errors"
	"strings"
	"testing"
)

func TestParseInsertSelectKeepsSourceAndReturningDistinct(t *testing.T) {
	const source = "INSERT INTO dst SELECT * FROM src WHERE id >= ? " +
		"ON CONFLICT DO NOTHING RETURNING id"
	statement, err := ParseStatement(source)
	if err != nil {
		t.Fatal(err)
	}
	insert := statement.Insert
	if statement.Kind != KindInsert || insert == nil || insert.Source == nil {
		t.Fatalf("statement = %#v, want INSERT SELECT", statement)
	}
	if len(insert.Rows) != 0 || len(insert.Columns) != 0 {
		t.Fatalf("INSERT SELECT retained VALUES state: rows=%d columns=%d",
			len(insert.Rows), len(insert.Columns))
	}
	if insert.Source == insert.Returning {
		t.Fatal("source SELECT aliases RETURNING SELECT")
	}
	if got := insert.Source.From[0].Name; got != "src" {
		t.Fatalf("source table = %q, want src", got)
	}
	if got := insert.Returning.From[0].Name; got != "dst" {
		t.Fatalf("RETURNING table = %q, want dst", got)
	}
	if !insert.OnConflictDoNothing || statement.Params() != 1 {
		t.Fatalf("conflict=%v params=%d, want true/1",
			insert.OnConflictDoNothing, statement.Params())
	}
	if insert.Source.Columns[0].Pos <= insert.SourcePos ||
		insert.Returning.Columns[0].Pos <= insert.Source.Columns[0].Pos {
		t.Fatalf("positions source=%d column=%d returning=%d",
			insert.SourcePos, insert.Source.Columns[0].Pos,
			insert.Returning.Columns[0].Pos)
	}
}

func TestParseInsertSelectDoesNotGuessQueryIdentifiersAsTail(t *testing.T) {
	statement, err := ParseStatement(
		"INSERT INTO dst SELECT returning FROM src ORDER BY returning",
	)
	if err != nil {
		t.Fatal(err)
	}
	if statement.Insert.Source == nil || statement.Insert.Returning != nil {
		t.Fatalf("source=%p returning=%p",
			statement.Insert.Source, statement.Insert.Returning)
	}
}

func TestParseInsertQueryExpressionLeaders(t *testing.T) {
	for _, source := range []string{
		"WITH picked AS (SELECT * FROM src) SELECT * FROM picked",
		"TABLE src",
		"(VALUES (?))",
	} {
		statement, err := ParseStatement("INSERT INTO dst " + source)
		if err != nil {
			t.Fatalf("source %q: %v", source, err)
		}
		if statement.Insert.Source == nil || len(statement.Insert.Rows) != 0 {
			t.Fatalf("source %q parsed as %#v", source, statement.Insert)
		}
	}
}

func TestParseInsertSelectRejectsColumnConstructionAndStatementSplit(t *testing.T) {
	_, err := ParseStatement("INSERT INTO dst (doc) SELECT * FROM src")
	var unsupported *FeatureNotSupportedError
	if !errors.As(err, &unsupported) || unsupported.Pos != 22 {
		t.Fatalf("column-list error = %#v", err)
	}
	if _, err := ParseStatement(
		"INSERT INTO dst SELECT * FROM src; RETURNING id",
	); err == nil {
		t.Fatal("semicolon before RETURNING was accepted")
	}
}

func TestParseInsertSelectAdversarialTailProbeIsLinearAndBounded(t *testing.T) {
	var source strings.Builder
	source.Grow(32 + maxClauseItems*len("returning, "))
	source.WriteString("INSERT INTO dst SELECT ")
	for i := 0; i < maxClauseItems; i++ {
		if i != 0 {
			source.WriteString(", ")
		}
		source.WriteString("returning")
	}
	// The dangling WHERE makes the complete source malformed after every
	// keyword-shaped identifier. The old reverse-probe algorithm reparsed all
	// 1024 prefixes; the error-position algorithm parses and scans each byte a
	// constant number of times.
	source.WriteString(" FROM src WHERE")
	sqlText := source.String()

	checks := 0
	var parser Parser
	parser.SetCancellationCheck(func() error {
		checks++
		return nil
	})
	var statement Statement
	if err := parser.ParseStatement(&statement, sqlText); err == nil {
		t.Fatal("malformed adversarial INSERT SELECT succeeded")
	}
	if checks > maxClauseItems*12 {
		t.Fatalf("cancellation/work checks = %d, want linear bound <= %d",
			checks, maxClauseItems*12)
	}
	if statement != (Statement{}) {
		t.Fatalf("rejected parse retained AST: %+v", statement)
	}

	parser.SetCancellationCheck(nil)
	var runErr error
	allocs := testing.AllocsPerRun(20, func() {
		runErr = parser.ParseStatement(&statement, sqlText)
	})
	if runErr == nil {
		t.Fatal("warmed malformed adversarial parse succeeded")
	}
	if allocs > 12 {
		t.Fatalf("warmed adversarial parse allocated %.2f times, want <= 12", allocs)
	}
	if err := parser.ParseStatement(
		&statement,
		"INSERT INTO dst SELECT returning FROM src RETURNING returning",
	); err != nil {
		t.Fatalf("parser was not reusable after adversarial refusal: %v", err)
	}
}
