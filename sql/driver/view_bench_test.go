package driver

import (
	sqldriver "database/sql/driver"
	"fmt"
	"testing"
)

func BenchmarkPrepareUnrelatedSelectViewCatalog(b *testing.B) {
	for _, views := range [...]int{0, maxCatalogViews} {
		b.Run(fmt.Sprintf("views=%d", views), func(b *testing.B) {
			connection := directTestConn(b).(*conn)
			directExec(b, connection,
				`CREATE TABLE docs (id STRING PRIMARY KEY, n NUMBER NOT NULL)`, nil)
			connection.db.mu.Lock()
			for i := 0; i < views; i++ {
				name := fmt.Sprintf("unrelated_%03d", i)
				connection.db.catalog.Views[name] = &viewMeta{
					Query: `SELECT id FROM docs`, Outputs: []string{"id"},
					TableDependencies: []string{"docs"},
				}
			}
			connection.db.mu.Unlock()

			const source = `SELECT n FROM docs WHERE id = ?`
			statement, err := connection.Prepare(source)
			if err != nil {
				b.Fatal(err)
			}
			if err := statement.Close(); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				statement, err := connection.Prepare(source)
				if err != nil {
					b.Fatal(err)
				}
				if err := statement.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

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
