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
