package driver

import (
	sqldriver "database/sql/driver"
	"io"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/query"
)

func TestPointMaterializationBudgetFallsBackToDurableScan(t *testing.T) {
	connection := directTestConn(t).(*conn)
	directExec(t, connection,
		`CREATE TABLE docs (id STRING PRIMARY KEY)`, nil)
	large := `{"id":"large","payload":"` +
		strings.Repeat("x", 8<<10) + `"}`
	for _, document := range []string{
		large,
		`{"id":"small","payload":"ok"}`,
	} {
		directExec(t, connection, `INSERT INTO docs VALUES (?)`,
			[]sqldriver.NamedValue{{Ordinal: 1, Value: document}})
	}
	connection.exec.Options = query.ExecOptions{
		MemoryBytes: driverMinimumQueryMemory,
	}
	statement, err := connection.Prepare(
		`SELECT id FROM docs WHERE id = ?`)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Close()

	values := make([]sqldriver.Value, 1)
	rows, err := statement.Query([]sqldriver.Value{"large"})
	if err != nil {
		t.Fatal(err)
	}
	if err := rows.Next(values); err != nil {
		t.Fatal(err)
	}
	if got := string(values[0].([]byte)); got != "large" {
		t.Fatalf("fallback result = %q, want large", got)
	}
	if err := rows.Next(values); err != io.EOF {
		t.Fatalf("fallback second Next = %v, want EOF", err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if connection.pointDocs.Len() != 0 {
		t.Fatalf(
			"oversized point source retained %d logical rows",
			connection.pointDocs.Len(),
		)
	}

	rows, err = statement.Query([]sqldriver.Value{"small"})
	if err != nil {
		t.Fatal(err)
	}
	if err := rows.Next(values); err != nil {
		t.Fatal(err)
	}
	if got := string(values[0].([]byte)); got != "small" {
		t.Fatalf("connection reuse result = %q, want small", got)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
}
