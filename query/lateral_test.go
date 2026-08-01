package query

import (
	"testing"
)

const correlatedLateralSQL = `SELECT a.id, d.id
FROM accounts a
CROSS JOIN LATERAL (
	SELECT i.id FROM items i WHERE i.owner = a.id
) d`

func TestPrepareStatementWiresCorrelatedLateralBeforeLowering(t *testing.T) {
	statement, err := PrepareStatement(correlatedLateralSQL)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	join := statement.relationJoin()
	if join == nil || len(join.operands) != 2 || join.operands[1].lateral == nil {
		t.Fatal("correlated LATERAL did not prepare an APPLY operand")
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
