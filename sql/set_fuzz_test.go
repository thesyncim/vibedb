package sql

import (
	"errors"
	"testing"
)

// FuzzSetExpressionAST concentrates mutation on the set grammar while also
// alternating an ordinary SELECT through the same Parser. It catches both
// malformed-tree acceptance and cold-sidecar state leaking into the hot path.
func FuzzSetExpressionAST(f *testing.F) {
	seeds := []string{
		`SELECT id FROM a UNION SELECT id FROM b`,
		`SELECT id FROM a UNION ALL SELECT id FROM b INTERSECT SELECT id FROM c EXCEPT ALL SELECT id FROM d`,
		`(SELECT id FROM a ORDER BY id LIMIT ?) UNION DISTINCT (SELECT id FROM b EXCEPT SELECT id FROM c) OFFSET ?`,
		`WITH q AS (SELECT id FROM source) SELECT id FROM q UNION ALL SELECT id FROM q`,
		`SELECT a, b FROM one UNION SELECT c FROM two`,
		`SELECT id FROM one UNION ALL DISTINCT SELECT id FROM two`,
		`((((SELECT id FROM one))))`,
		"SELECT \"café\" AS id FROM one\nINTERSECT ALL\nVALUES (1)",
	}
	for _, seed := range seeds {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, source string) {
		var parser Parser
		var statement SelectStmt
		err := parser.Parse(&statement, source)
		if err != nil {
			var positioned *ParseError
			if !errors.As(err, &positioned) {
				t.Fatalf("set parse returned unpositioned %T: %v", err, err)
			}
			if positioned.Pos < 0 || positioned.Pos > len(source) ||
				positioned.Line < 1 || positioned.Col < 1 || positioned.Msg == "" {
				t.Fatalf("invalid positioned rejection: %+v", positioned)
			}
			if statement.Set != nil || len(statement.Columns) != 0 || len(statement.From) != 0 {
				t.Fatal("rejected input retained a partial set AST")
			}
			return
		}

		checkStatementInvariants(t, &statement)
		first := dumpStmt(&statement)
		wasSet := statement.Set != nil

		if err := parser.Parse(&statement, `SELECT id FROM ordinary WHERE tenant = ?`); err != nil {
			t.Fatalf("ordinary parser-reuse control failed: %v", err)
		}
		if statement.Set != nil || statement.Params != 1 {
			t.Fatal("set sidecar leaked into ordinary parser reuse")
		}
		if err := parser.Parse(&statement, source); err != nil {
			t.Fatalf("deterministic reparse rejected accepted input: %v", err)
		}
		if (statement.Set != nil) != wasSet {
			t.Fatal("reparse changed whether the statement is a set expression")
		}
		if second := dumpStmt(&statement); second != first {
			t.Fatalf("set AST changed across arena reuse:\nfirst  %s\nsecond %s", first, second)
		}
	})
}
