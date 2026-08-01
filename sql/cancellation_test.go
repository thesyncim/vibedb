package sql

import (
	"errors"
	"strings"
	"testing"
)

func TestParserCancellationIsBoundedAcrossLargeLexicalRuns(t *testing.T) {
	const payloadBytes = 1 << 20
	cases := []struct {
		name        string
		src         string
		extraChecks int
	}{
		{
			name: "block comment",
			src:  "SELECT * FROM users /*" + strings.Repeat("x", payloadBytes) + "*/",
			// One token checkpoint follows admission; the next callback is the
			// first bounded byte checkpoint in the long comment.
			extraChecks: 2,
		},
		{
			name: "quoted literal",
			src: "SELECT * FROM users WHERE name = '" +
				strings.Repeat("x", payloadBytes) + "'",
			extraChecks: 2,
		},
		{
			name: "bare JSON document",
			src: "INSERT INTO users VALUES ({\"pad\":\"" +
				strings.Repeat("x", payloadBytes) + "\"})",
			// The JSON scanner checks on entry and again after one bounded run.
			extraChecks: 3,
		},
	}
	want := errors.New("stop parsing")
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			admissionChecks := 1 + (len(tc.src)-1)/parserCancelByteInterval
			stopAt := admissionChecks + tc.extraChecks
			checks := 0
			var parser Parser
			parser.SetCancellationCheck(func() error {
				checks++
				if checks >= stopAt {
					return want
				}
				return nil
			})
			var statement Statement
			err := parser.ParseStatement(&statement, tc.src)
			if err != want {
				t.Fatalf("ParseStatement cancellation = %v, want exact %v", err, want)
			}
			if checks != stopAt {
				t.Fatalf("cancellation checks = %d, want %d", checks, stopAt)
			}
			if statement.Kind != 0 || statement.Select != nil || statement.Insert != nil {
				t.Fatalf("canceled parse returned a partial statement: %+v", statement)
			}

			parser.SetCancellationCheck(nil)
			if err := parser.ParseStatement(&statement, "SELECT * FROM users"); err != nil {
				t.Fatalf("parser was not reusable after cancellation: %v", err)
			}
		})
	}
}

func TestParserReturnsPreexistingCancellationUnchanged(t *testing.T) {
	want := errors.New("already canceled")
	var parser Parser
	parser.SetCancellationCheck(func() error { return want })
	var statement Statement
	if err := parser.ParseStatement(&statement, "SELECT * FROM users"); err != want {
		t.Fatalf("ParseStatement = %v, want exact %v", err, want)
	}
}

func TestParserCancellationRemainsBoundedInsideDerivedTable(t *testing.T) {
	const payloadBytes = 1 << 20
	src := `SELECT d.id FROM (SELECT id FROM docs /*` +
		strings.Repeat("x", payloadBytes) + `*/) AS d`
	admissionChecks := 1 + (len(src)-1)/parserCancelByteInterval
	// Admission validates the complete source first. The following token check
	// and bounded comment scan exercise the parent parser while it delimits the
	// nested SELECT; cancellation must still escape unchanged.
	stopAt := admissionChecks + 3
	checks := 0
	want := errors.New("cancel derived parse")
	var parser Parser
	parser.SetCancellationCheck(func() error {
		checks++
		if checks >= stopAt {
			return want
		}
		return nil
	})
	var statement SelectStmt
	if err := parser.Parse(&statement, src); err != want {
		t.Fatalf("Parse cancellation = %v, want exact %v", err, want)
	}
	if len(statement.Columns) != 0 || len(statement.From) != 0 {
		t.Fatalf("canceled derived parse returned a partial AST: %+v", statement)
	}
}

func TestParserCancellationRemainsBoundedInsideCTE(t *testing.T) {
	const payloadBytes = 1 << 20
	src := `WITH active AS (SELECT id FROM docs /*` +
		strings.Repeat("x", payloadBytes) + `*/) SELECT id FROM active`
	admissionChecks := 1 + (len(src)-1)/parserCancelByteInterval
	stopAt := admissionChecks + 3
	checks := 0
	want := errors.New("cancel CTE parse")
	var parser Parser
	parser.SetCancellationCheck(func() error {
		checks++
		if checks >= stopAt {
			return want
		}
		return nil
	})
	var statement SelectStmt
	if err := parser.Parse(&statement, src); err != want {
		t.Fatalf("Parse cancellation = %v, want exact %v", err, want)
	}
	if statement.With != nil || len(statement.Columns) != 0 || len(statement.From) != 0 {
		t.Fatalf("canceled CTE parse returned a partial AST: %+v", statement)
	}
	parser.SetCancellationCheck(nil)
	if err := parser.Parse(&statement, `WITH c AS (SELECT id FROM docs) SELECT id FROM c`); err != nil {
		t.Fatalf("parser was not reusable after CTE cancellation: %v", err)
	}
}
