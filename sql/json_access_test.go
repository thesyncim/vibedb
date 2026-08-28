package sql

import "testing"

func TestJSONAccessCompiledPathsAndEquality(t *testing.T) {
	for _, source := range []string{
		`SELECT * FROM documents WHERE "$doc"->>'city' = 'Lisbon'`,
		`SELECT * FROM documents WHERE documents."$doc"->>'city' = 'Lisbon'`,
		`SELECT * FROM documents d WHERE d."$doc"->>'city' = 'Lisbon'`,
		`SELECT * FROM documents WHERE 'Lisbon' = "$doc"->>'city'`,
	} {
		stmt, err := Parse(source)
		if err != nil {
			t.Fatalf("%s: %v", source, err)
		}
		if stmt.Where.Kind != ExprCompare || stmt.Where.Path.Spec() != "city" || stmt.Where.Path.Source != 0 {
			t.Fatalf("not the native path predicate: %s", dumpStmt(stmt))
		}
	}
	stmt, err := Parse(`SELECT d."$doc"->'address'->>'city', "$doc"->'items'->>'name' FROM documents d`)
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range []string{"address.city", "items.name"} {
		s := stmt.Columns[i].Scalar
		if s.Kind != ScalarCast || s.Cast != ScalarCastText || s.Left.Path.Spec() != want || s.Left.Path.Source != 0 {
			t.Fatalf("column %d: %s", i, dumpStmt(stmt))
		}
	}
	for _, literal := range []string{"92", "-1", "true", "false", "{}", "[]"} {
		stmt, err := Parse(`SELECT * FROM documents WHERE "$doc"->>'value' = '` + literal + `'`)
		if err != nil {
			t.Fatal(err)
		}
		if stmt.Where.Kind != ExprScalarCompare {
			t.Fatalf("unsafe conversion removal for %q", literal)
		}
	}
}

func TestJSONAccessRejectsUnsupportedAndIncomplete(t *testing.T) {
	for _, source := range []string{
		`SELECT * FROM documents WHERE "$doc"->>'city'`,
		`SELECT "$doc"->>? FROM documents`,
		`SELECT "$doc"->>-1 FROM documents`,
		`SELECT "$doc"->>0 FROM documents`,
		`SELECT "$doc"->>'0' FROM documents`,
		`SELECT "$doc"->>1.5 FROM documents`,
		`SELECT "$doc"->>'a'->>'b' FROM documents`,
		`SELECT absent."$doc"->>'city' FROM documents`,
		`SELECT "$doc"->>'city' FROM documents a JOIN documents b ON a.id=b.id`,
	} {
		if _, err := Parse(source); err == nil {
			t.Fatalf("accepted %s", source)
		}
	}
}

func TestJSONAccessWarmParserZeroAlloc(t *testing.T) {
	var p Parser
	var stmt SelectStmt
	for _, source := range []string{
		`SELECT * FROM documents WHERE "$doc"->>'city' = 'Lisbon'`,
		`SELECT "$doc"->'items'->>'name' FROM documents WHERE "$doc"->>'score' = ?`,
	} {
		run := func() {
			if err := p.Parse(&stmt, source); err != nil {
				t.Fatal(err)
			}
		}
		run()
		run()
		if n := testing.AllocsPerRun(100, run); n != 0 {
			t.Fatalf("%s: %g allocations", source, n)
		}
	}
}
