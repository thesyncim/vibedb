package query

import (
	"errors"
	"strings"
	"testing"

	sqlast "github.com/thesyncim/vibedb/sql"
)

const correlatedLateralSQL = `SELECT a.id, d.id
FROM accounts a
CROSS JOIN LATERAL (
	SELECT i.id FROM items i WHERE i.owner = a.id
) d`

func TestPrepareStatementRefusesCorrelatedLateralBeforeLowering(t *testing.T) {
	statement, err := PrepareStatement(correlatedLateralSQL)
	if statement != nil {
		statement.Release()
		t.Fatal("correlated LATERAL prepared")
	}
	var unsupported *sqlast.FeatureNotSupportedError
	if !errors.As(err, &unsupported) {
		t.Fatalf("PrepareStatement error = %T %v, want *sql.FeatureNotSupportedError", err, err)
	}
	wantPos := strings.Index(correlatedLateralSQL, "LATERAL")
	if unsupported.Pos != wantPos ||
		!strings.Contains(unsupported.Msg, "correlated LATERAL execution") {
		t.Fatalf("typed refusal = %+v, want position %d", unsupported, wantPos)
	}
}

func TestPrepareStatementAcceptsDecorrelatedLateralAsDerivedInput(t *testing.T) {
	statement, err := PrepareStatement(`SELECT a.id, d.id
		FROM accounts a
		CROSS JOIN LATERAL (SELECT i.id FROM items i) d`)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	if !statement.UsesGeneralizedRelationJoin() {
		t.Fatal("decorrelated LATERAL did not retain the derived relation join plan")
	}
}
