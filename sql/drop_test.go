package sql

import "testing"

func TestDropTableAST(t *testing.T) {
	for _, tc := range []struct {
		src      string
		ifExists bool
	}{
		{src: `DROP TABLE docs`},
		{src: `DROP TABLE IF EXISTS docs`, ifExists: true},
	} {
		statement, err := ParseStatement(tc.src)
		if err != nil {
			t.Fatalf("ParseStatement(%q): %v", tc.src, err)
		}
		if statement.Kind != KindDropTable || statement.DropTable == nil {
			t.Fatalf("ParseStatement(%q) = %#v, want DROP TABLE", tc.src, statement)
		}
		if statement.DropTable.Table != "docs" ||
			statement.DropTable.IfExists != tc.ifExists {
			t.Fatalf("DROP TABLE AST = %+v", *statement.DropTable)
		}
	}
}

func TestCatalogRemovalStatementContracts(t *testing.T) {
	tests := []struct {
		src      string
		kind     Kind
		kindText string
		table    string
		ifExists bool
		hasTable bool
	}{
		{`TRUNCATE docs`, KindTruncate, "TRUNCATE", "docs", false, true},
		{`TRUNCATE TABLE docs`, KindTruncate, "TRUNCATE", "docs", false, true},
		{`DROP INDEX by_kind`, KindDropIndex, "DROP INDEX", "", false, false},
		{`DROP INDEX IF EXISTS by_kind ON docs`, KindDropIndex, "DROP INDEX", "docs", true, true},
	}
	for _, tc := range tests {
		t.Run(tc.src, func(t *testing.T) {
			statement, err := ParseStatement(tc.src)
			if err != nil {
				t.Fatal(err)
			}
			if statement.Kind != tc.kind || KindOf(tc.src) != tc.kind {
				t.Fatalf("kinds = (%v, %v), want %v", statement.Kind, KindOf(tc.src), tc.kind)
			}
			if statement.Kind.String() != tc.kindText {
				t.Fatalf("Kind.String() = %q, want %q", statement.Kind.String(), tc.kindText)
			}
			if statement.Table() != tc.table {
				t.Fatalf("Table() = %q, want %q", statement.Table(), tc.table)
			}
			if statement.Params() != 0 || statement.ReturnsRows() {
				t.Fatalf("Params/ReturnsRows = %d/%v, want 0/false", statement.Params(), statement.ReturnsRows())
			}
			switch tc.kind {
			case KindTruncate:
				if statement.Truncate == nil || statement.Truncate.Table != tc.table {
					t.Fatalf("TRUNCATE AST = %+v", statement.Truncate)
				}
				if statement.Truncate.Pos < 0 || statement.Truncate.Pos >= len(tc.src) {
					t.Fatalf("TRUNCATE name position = %d", statement.Truncate.Pos)
				}
			case KindDropIndex:
				if statement.DropIndex == nil || statement.DropIndex.Name != "by_kind" ||
					statement.DropIndex.IfExists != tc.ifExists ||
					statement.DropIndex.HasTable != tc.hasTable {
					t.Fatalf("DROP INDEX AST = %+v", statement.DropIndex)
				}
				if statement.DropIndex.Pos < 0 || statement.DropIndex.Pos >= len(tc.src) {
					t.Fatalf("DROP INDEX name position = %d", statement.DropIndex.Pos)
				}
				if tc.hasTable && (statement.DropIndex.TablePos <= statement.DropIndex.Pos ||
					statement.DropIndex.TablePos >= len(tc.src)) {
					t.Fatalf("DROP INDEX table position = %d", statement.DropIndex.TablePos)
				}
			}
		})
	}
}

func TestCatalogRemovalASTResetsAcrossParserReuse(t *testing.T) {
	var p Parser
	var statement Statement
	if err := p.ParseStatement(&statement, `DROP INDEX IF EXISTS by_kind ON docs`); err != nil {
		t.Fatal(err)
	}
	if statement.DropIndex == nil || !statement.DropIndex.IfExists ||
		!statement.DropIndex.HasTable {
		t.Fatalf("initial DROP INDEX AST = %+v", statement.DropIndex)
	}
	if err := p.ParseStatement(&statement, `DROP INDEX by_name`); err != nil {
		t.Fatal(err)
	}
	if statement.DropIndex == nil || statement.DropIndex.IfExists ||
		statement.DropIndex.HasTable || statement.DropIndex.Table != "" ||
		statement.DropIndex.TablePos != 0 {
		t.Fatalf("reused DROP INDEX AST retained state: %+v", statement.DropIndex)
	}
	if err := p.ParseStatement(&statement, `TRUNCATE TABLE docs`); err != nil {
		t.Fatal(err)
	}
	if statement.Kind != KindTruncate || statement.Truncate == nil ||
		statement.DropIndex != nil {
		t.Fatalf("reused TRUNCATE AST = %+v", statement)
	}
	if err := p.ParseStatement(&statement, `DROP TABLE IF EXISTS docs`); err != nil {
		t.Fatal(err)
	}
	if statement.Kind != KindDropTable || statement.DropTable == nil ||
		statement.Truncate != nil || statement.DropIndex != nil {
		t.Fatalf("final reused AST = %+v", statement)
	}
	if err := p.ParseStatement(&statement, `DROP INDEX IF`); err == nil {
		t.Fatal("incomplete DROP INDEX was accepted")
	}
	if statement != (Statement{}) {
		t.Fatalf("rejected parse left AST state: %+v", statement)
	}
}
