package query

import "testing"

func TestSQLJSONAccessValuesNullsAndTextSemantics(t *testing.T) {
	segment := mustSegment(t, `{"city":"Lisbon","score":92,"active":true,"nil":null,"nested":{"city":"Porto"},"items":["one",2],"a.b":"literal","q'k":"quote"}`)
	source := `SELECT "$doc"->>'city', "$doc"->>'score', "$doc"->>'active', "$doc"->>'nil', "$doc"->>'absent', "$doc"->'nested'->>'city', "$doc"->>'items', "$doc"->>'nested', "$doc"->>'a.b', "$doc"->>'q''k', "$doc"->'score'->>'city' FROM documents`
	stmt, err := PrepareStatement(source)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Release()
	var exec Exec
	cur, err := stmt.RunInto(&exec, FromSegment(segment), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !cur.Next() {
		t.Fatal("no row")
	}
	for i, want := range []string{`"Lisbon"`, `"92"`, `"true"`, `null`, `null`, `"Porto"`, `"[\"one\",2]"`, `"{\"city\":\"Porto\"}"`, `"literal"`, `"quote"`, `null`} {
		if got := string(cur.Cell(i).JSON()); got != want {
			t.Fatalf("column %d = %s, want %s", i, got, want)
		}
		if stmt.AppendSchema(nil)[i].Representation != OutputSQLText {
			t.Fatalf("column %d not text", i)
		}
	}
	if cur.Next() {
		t.Fatal("extra row")
	}
}

func TestSQLJSONAccessArrayTraversalIsExplicitlyUnsupported(t *testing.T) {
	segment := mustSegment(t, `{"items":[{"name":"one"}]}`)
	stmt, err := PrepareStatement(`SELECT "$doc"->'items'->>'name' FROM documents`)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Release()
	var exec Exec
	if _, err := stmt.RunInto(&exec, FromSegment(segment), nil); err == nil {
		t.Fatal("array traversal was silently accepted as an object lookup")
	}
}

func TestSQLJSONAccessWholeDocumentPredicatesAndZeroAlloc(t *testing.T) {
	segment := mustSegment(t, `{"id":"a","city":"Lisbon","score":92,"active":true}`, `{"id":"b","city":"Porto","score":"92"}`, `{"id":"c","city":null}`)
	for _, tc := range []struct {
		sql    string
		rows   int
		native bool
	}{
		{`SELECT * FROM documents WHERE "$doc"->>'city' = 'Lisbon'`, 1, true},
		{`SELECT * FROM documents WHERE documents."$doc"->>'score' = '92'`, 2, false},
		{`SELECT * FROM documents WHERE "$doc"->>'active' = 'true'`, 1, false},
		{`SELECT * FROM documents WHERE "$doc"->>'city' IS NULL`, 1, false},
		{`SELECT * FROM documents WHERE "$doc"->>'missing' IS NOT NULL`, 0, false},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			stmt, err := PrepareStatement(tc.sql)
			if err != nil {
				t.Fatal(err)
			}
			defer stmt.Release()
			if tc.native && stmt.scalarStatement() != nil {
				t.Fatal("native equality acquired a scalar execution stage")
			}
			var exec Exec
			run := func() {
				cur, err := stmt.RunInto(&exec, FromSegment(segment), nil)
				if err != nil {
					t.Fatal(err)
				}
				n := 0
				for cur.Next() {
					n++
					if len(cur.Cell(0).JSON()) == 0 {
						t.Fatal("lost whole document")
					}
				}
				if n != tc.rows {
					t.Fatalf("rows %d want %d", n, tc.rows)
				}
			}
			run()
			run()
			if n := testing.AllocsPerRun(100, run); n != 0 {
				t.Fatalf("%g allocations", n)
			}
		})
	}
}

func BenchmarkSQLJSONAccessNativeEquality(b *testing.B) {
	segment := mustSegment(b, `{"id":"a","city":"Lisbon"}`, `{"id":"b","city":"Porto"}`)
	for _, source := range []string{`SELECT * FROM documents WHERE city = 'Lisbon'`, `SELECT * FROM documents WHERE "$doc"->>'city' = 'Lisbon'`} {
		b.Run(source, func(b *testing.B) {
			stmt, err := PrepareStatement(source)
			if err != nil {
				b.Fatal(err)
			}
			defer stmt.Release()
			var exec Exec
			run := func() {
				cur, err := stmt.RunInto(&exec, FromSegment(segment), nil)
				if err != nil {
					b.Fatal(err)
				}
				for cur.Next() {
				}
			}
			run()
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				run()
			}
		})
	}
}
