//go:build 386

package sql

import "testing"

func TestSetExpression386GroupedParameterRanges(t *testing.T) {
	statement := parseSetForTest(t,
		`(SELECT id FROM one WHERE a = ? LIMIT ?) UNION ALL `+
			`SELECT id FROM two WHERE b IN (?, ?) INTERSECT `+
			`(SELECT id FROM three WHERE c = ? OFFSET ?) LIMIT ?`)
	if statement.Params != 7 || statement.Set.Root.Params != 6 ||
		statement.Set.Tail.ParamBase != 6 || statement.Set.Tail.Limit.Ordinal != 6 {
		t.Fatalf("386 parameter accounting = %s", dumpStmt(statement))
	}
	checkStatementInvariants(t, statement)
}
