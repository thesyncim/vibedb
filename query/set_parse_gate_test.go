package query

import (
	"testing"

	sqlast "github.com/thesyncim/vibedb/sql"
)

func TestPreparedStatementConsumesParsedSetTreeInsteadOfMirroredFirstLeaf(t *testing.T) {
	const text = "SELECT id FROM customers UNION ALL SELECT id FROM archived"
	tree, err := sqlast.Parse(text)
	if err != nil {
		t.Fatal(err)
	}
	if tree.Set == nil {
		t.Fatal("parser did not retain the compound-query sidecar")
	}

	for _, prepare := range []struct {
		name string
		fn   func() (*Statement, error)
	}{
		{name: "source", fn: func() (*Statement, error) {
			return PrepareStatement(text)
		}},
		{name: "parsed", fn: func() (*Statement, error) {
			return PrepareParsedStatement(text, tree)
		}},
	} {
		t.Run(prepare.name, func(t *testing.T) {
			stmt, err := prepare.fn()
			if err != nil {
				t.Fatal(err)
			}
			defer stmt.Release()
			if stmt.setSQL() == nil || stmt.outputs != 1 {
				t.Fatal("compound query was not attached to the physical set runtime")
			}
		})
	}
}
