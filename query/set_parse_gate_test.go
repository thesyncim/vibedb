package query

import (
	"errors"
	"testing"

	sqlast "github.com/thesyncim/vibedb/sql"
)

func TestPreparedStatementRejectsParsedSetTreeBeforeFirstLeafLowering(t *testing.T) {
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
			if stmt != nil {
				stmt.Release()
				t.Fatal("compound query unexpectedly produced a first-leaf Statement")
			}
			var unsupported *sqlast.FeatureNotSupportedError
			if !errors.As(err, &unsupported) {
				t.Fatalf("error = %T %v, want *sql.FeatureNotSupportedError", err, err)
			}
			if unsupported.Pos != tree.Set.Pos {
				t.Fatalf("error offset = %d, want set position %d", unsupported.Pos, tree.Set.Pos)
			}
		})
	}
}
