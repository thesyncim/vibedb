package driver

import (
	sqldriver "database/sql/driver"
	"testing"
)

func BenchmarkPreparedOrdinaryViewPointQuery(b *testing.B) {
	connection := directTestConn(b)
	directExec(b, connection,
		`CREATE TABLE docs (id STRING PRIMARY KEY, n NUMBER NOT NULL)`, nil)
	directExec(b, connection, `INSERT INTO docs VALUES (?)`,
		[]sqldriver.NamedValue{{Ordinal: 1, Value: `{"id":"a","n":1}`}})
	directExec(b, connection,
		`CREATE VIEW docs_view AS SELECT id, n FROM docs`, nil)
	statement, err := connection.Prepare(`SELECT n FROM docs_view WHERE id = ?`)
	if err != nil {
		b.Fatal(err)
	}
	defer statement.Close()
	args := []sqldriver.Value{"a"}
	destination := make([]sqldriver.Value, 1)
	if err := runDirectQuery(statement, args, destination); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := runDirectQuery(statement, args, destination); err != nil {
			b.Fatal(err)
		}
	}
}
