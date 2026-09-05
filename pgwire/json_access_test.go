package pgwire

import "testing"

func TestJSONAccessSimpleAndPreparedWire(t *testing.T) {
	c := connectSQLCatalog(t)
	for _, q := range []string{
		`CREATE TABLE documents (id STRING PRIMARY KEY, city STRING, score NUMBER)`,
		`INSERT INTO documents (id,city,score) VALUES ('a','Lisbon',92),('b','Porto',87)`,
	} {
		msgs := c.query(q)
		if has(msgs, msgErrorResponse) {
			t.Fatal(formatError(find(t, msgs, msgErrorResponse).body))
		}
	}
	for _, q := range []string{
		`SELECT * FROM public.documents WHERE "$doc"->>'city' = 'Lisbon'`,
		`SELECT * FROM public.documents WHERE documents."$doc"->>'score' = '92'`,
	} {
		msgs := c.query(q)
		if has(msgs, msgErrorResponse) {
			t.Fatal(formatError(find(t, msgs, msgErrorResponse).body))
		}
		if rows := rowsOf(t, msgs); len(rows) != 1 {
			t.Fatalf("%s: %q", q, rows)
		}
	}
	q := `SELECT d."$doc"->>'city', d."$doc"->>'score', d."$doc"->>'missing' FROM public.documents d WHERE d."$doc"->>'city' = $1`
	for _, city := range []string{"Lisbon", "Porto"} {
		msgs := extendedSQL(c, q, [][]byte{[]byte(city)})
		if has(msgs, msgErrorResponse) {
			t.Fatal(formatError(find(t, msgs, msgErrorResponse).body))
		}
		rows := rowsOf(t, msgs)
		if len(rows) != 1 || string(rows[0][0]) != city || rows[0][2] != nil {
			t.Fatalf("prepared %s: %q", city, rows)
		}
	}
	msgs := c.query(`SELECT "$doc"->>'score' AS score FROM documents WHERE id='a'`)
	cols := decodeRowDescription(t, find(t, msgs, msgRowDescription).body)
	if cols[0].oid != oidText || string(rowsOf(t, msgs)[0][0]) != "92" {
		t.Fatalf("text metadata/encoding: %+v", cols)
	}
	expectError(t, c.query(`SELECT * FROM documents WHERE "$doc"->>'city'`), sqlstateDatatypeMismatch)
}
