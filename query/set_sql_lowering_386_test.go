//go:build 386

package query

import "testing"

func TestSQLSetLowering386AbsoluteRangesAndTails(t *testing.T) {
	statement, err := PrepareStatement(
		`(SELECT v FROM docs WHERE v >= ? LIMIT ?) UNION ALL ` +
			`SELECT v FROM docs WHERE v <= ? LIMIT ? OFFSET ?`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	if statement.NumParams() != 5 || statement.setSQL() == nil {
		t.Fatalf("386 set descriptor params/runtime = %d/%v",
			statement.NumParams(), statement.setSQL())
	}
}
