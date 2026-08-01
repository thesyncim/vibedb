//go:build 386

package query

import "testing"

func TestSQLLateral386PairAccountingAndExecution(t *testing.T) {
	db := lateralStatementDatabase(t)
	statement, exec, rows := runLateralStatement(t, db, correlatedLateralSQL)
	defer statement.Release()
	defer exec.Release()
	if len(rows) != 3 || relationJoinPairBytes(0, len(rows)) <= 0 {
		t.Fatalf("386 LATERAL rows/accounting = %d/%d", len(rows), relationJoinPairBytes(0, len(rows)))
	}
}
