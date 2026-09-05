package sql

import "testing"

func TestAggregateHavingHiddenBinding(t *testing.T) {
	for _, source := range []string{
		`SELECT team FROM docs GROUP BY team HAVING SUM(n)>1`,
		`SELECT 1 FROM docs HAVING COUNT(*)>0`,
		`SELECT COALESCE(SUM(n),0) FROM docs HAVING COUNT(*)>1`,
	} {
		statement, err := Parse(source)
		if err != nil {
			t.Fatalf("%s: %v", source, err)
		}
		if len(statement.Columns) != 1 || statement.Having.Column != -1 || statement.Having.Agg == AggNone {
			t.Fatalf("hidden dependency altered output or lost binding: %+v", statement)
		}
	}
	if _, err := Parse(`SELECT n FROM docs HAVING COUNT(*)>0`); err == nil {
		t.Fatal("ungrouped path accepted with hidden aggregate")
	}
}
