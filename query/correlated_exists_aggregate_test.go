package query

import (
	"errors"
	"strings"
	"testing"

	sqlast "github.com/thesyncim/vibedb/sql"
)

func TestCorrelatedExistsAggregateProjectionIsPositionedFeatureRefusal(t *testing.T) {
	tests := []struct {
		name   string
		source string
		at     string
	}{
		{
			name: "exists count empty inner",
			source: `SELECT o.id FROM ce_outer o WHERE EXISTS (` +
				`SELECT COUNT(*) FROM ce_inner i WHERE i.k = o.k AND i.id = -1)`,
			at: "COUNT",
		},
		{
			name: "exists sum nonempty inner",
			source: `SELECT o.id FROM ce_outer o WHERE EXISTS (` +
				`SELECT SUM(i.score) FROM ce_inner i WHERE i.k = o.k)`,
			at: "SUM",
		},
		{
			name: "not exists min empty inner",
			source: `SELECT o.id FROM ce_outer o WHERE NOT EXISTS (` +
				`SELECT MIN(i.score) FROM ce_inner i WHERE i.k = o.k AND i.id = -1)`,
			at: "MIN",
		},
		{
			name: "not exists nested aggregate nonempty inner",
			source: `SELECT o.id FROM ce_outer o WHERE NOT EXISTS (` +
				`SELECT COUNT(*) + 1 FROM ce_inner i WHERE i.k = o.k)`,
			at: "COUNT",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			statement, err := PrepareStatement(test.source)
			if statement != nil {
				statement.Release()
				t.Fatal("aggregate-bearing correlated EXISTS was prepared and could be silently decorrelated")
			}
			var unsupported *sqlast.FeatureNotSupportedError
			if !errors.As(err, &unsupported) {
				t.Fatalf("error = %T %v, want *sql.FeatureNotSupportedError", err, err)
			}
			want := strings.Index(test.source, test.at)
			if unsupported.Pos != want {
				t.Fatalf("feature position = %d, want first %s at %d", unsupported.Pos, test.at, want)
			}
			if !strings.Contains(unsupported.Error(), "empty-input cardinality") {
				t.Fatalf("feature reason does not identify the semantic hazard: %v", unsupported)
			}
		})
	}
}
