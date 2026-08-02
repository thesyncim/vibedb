package sql

import "testing"

func FuzzViewStatementGrammar(f *testing.F) {
	for _, source := range []string{
		`CREATE VIEW v AS SELECT id FROM docs`,
		`CREATE VIEW v (renamed) AS SELECT id FROM docs`,
		`DROP VIEW IF EXISTS v RESTRICT`,
		`CREATE MATERIALIZED VIEW v AS SELECT id FROM docs`,
		`REFRESH MATERIALIZED VIEW v WITH NO DATA`,
	} {
		f.Add(source)
	}
	f.Fuzz(func(t *testing.T, source string) {
		if len(source) > 4096 {
			t.Skip()
		}
		var parser Parser
		var statement Statement
		err := parser.ParseStatement(&statement, source)
		if err != nil {
			if statement != (Statement{}) {
				t.Fatal("failed view parse published a partial statement")
			}
			return
		}
		switch statement.Kind {
		case KindCreateView:
			if statement.CreateView == nil || statement.CreateView.Query == nil ||
				statement.CreateView.Name == "" || statement.Params() != 0 {
				t.Fatalf("invalid CREATE VIEW AST: %+v", statement)
			}
		case KindDropView:
			if statement.DropView == nil || statement.DropView.Name == "" ||
				statement.Params() != 0 {
				t.Fatalf("invalid DROP VIEW AST: %+v", statement)
			}
		}
	})
}
