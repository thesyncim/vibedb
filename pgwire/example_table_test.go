package pgwire

import (
	"os"
	"strings"
	"testing"
)

func TestDeclaredThousandEmployeeExample(t *testing.T) {
	data, err := os.ReadFile("../docs/examples/employees-1000.sql")
	if err != nil {
		t.Fatal(err)
	}
	c := connectSQLCatalog(t)
	for _, statement := range strings.Split(string(data), ";") {
		msgs := c.query(statement)
		if has(msgs, msgErrorResponse) {
			t.Fatal(formatError(find(t, msgs, msgErrorResponse).body))
		}
	}
	msgs := c.query(`SELECT COUNT(*) FROM employees`)
	rows := rowsOf(t, msgs)
	if len(rows) != 1 || string(rows[0][0]) != "1000" {
		t.Fatalf("row count: %q", rows)
	}
	msgs = c.query(`SELECT id, name, team, city, score, active FROM employees ORDER BY id`)
	rows = rowsOf(t, msgs)
	if len(rows) != 1000 || len(rows[0]) != 6 || string(rows[0][0]) != `"employee-0001"` || string(rows[999][0]) != `"employee-1000"` {
		t.Fatalf("declared row shape/count: %d", len(rows))
	}
}

func TestDeclaredMultiColumnExample(t *testing.T) {
	data, err := os.ReadFile("../docs/examples/multi-column-table.sql")
	if err != nil {
		t.Fatal(err)
	}
	c := connectSQLCatalog(t)
	count := 0
	// This fixture has no semicolons inside quoted values or comments.
	for _, statement := range strings.Split(string(data), ";") {
		msgs := c.query(statement)
		if has(msgs, msgErrorResponse) {
			t.Fatal(formatError(find(t, msgs, msgErrorResponse).body))
		}
		count += len(rowsOf(t, msgs))
	}
	if count != 7 {
		t.Fatalf("example returned %d rows, want 3+2+2", count)
	}
	shape := discoveryTestShape(t, "RetrieveColumns")
	msgs := c.query(strings.ReplaceAll(shape.SQL, ":schema_id", "2200"))
	if has(msgs, msgErrorResponse) {
		t.Fatal(formatError(find(t, msgs, msgErrorResponse).body))
	}
	names := map[string]bool{}
	column := -1
	for i, c := range shape.Columns {
		if c == "column_name" {
			column = i
		}
	}
	if column < 0 {
		t.Fatal("missing column_name descriptor")
	}
	for _, row := range rowsOf(t, msgs) {
		names[string(row[column])] = true
	}
	for _, name := range []string{"id", "name", "team", "city", "score", "active"} {
		if !names[name] {
			t.Fatalf("missing declared column %q: %v", name, names)
		}
	}
	for _, q := range []string{
		`INSERT INTO employees (id,name,team,score,active) VALUES ('bad-type','Bad','QA','not a number',true)`,
		`INSERT INTO employees (id,team,score,active) VALUES ('missing-name','QA',1,true)`,
	} {
		if msgs := c.query(q); !has(msgs, msgErrorResponse) {
			t.Fatalf("declaration not enforced: %s", q)
		}
	}
}
