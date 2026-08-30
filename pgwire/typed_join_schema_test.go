package pgwire

import "testing"

func TestPostgreSQLTypedDerivedJoinWireSchema(t *testing.T) {
	c := connect(t)
	tests := []struct {
		name     string
		sql      string
		wantOIDs []int32
		boolAt   int
		textAt   int
	}{
		{
			name: "direct",
			sql: `SELECT q.v FROM users AS u CROSS JOIN
				(SELECT BOOL 't' AS v) AS q WHERE u.id = 1`,
			wantOIDs: []int32{oidBool},
			boolAt:   0,
			textAt:   -1,
		},
		{
			name: "derived wildcard",
			sql: `SELECT q.* FROM users AS u CROSS JOIN
				(SELECT BOOL 't' AS v, TEXT 'x' AS s) AS q WHERE u.id = 1`,
			wantOIDs: []int32{oidBool, oidText},
			boolAt:   0,
			textAt:   1,
		},
		{
			name: "mixed identity projection",
			sql: `SELECT u.*, q.v, q.s FROM users AS u CROSS JOIN
				(SELECT BOOL 't' AS v, TEXT 'x' AS s) AS q WHERE u.id = 1`,
			wantOIDs: []int32{oidJSON, oidBool, oidText},
			boolAt:   1,
			textAt:   2,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			messages := c.query(test.sql)
			description := decodeRowDescription(
				t, find(t, messages, msgRowDescription).body,
			)
			if len(description) != len(test.wantOIDs) {
				t.Fatalf("RowDescription = %+v, want OIDs %v", description, test.wantOIDs)
			}
			for column := range description {
				if description[column].oid != test.wantOIDs[column] {
					t.Fatalf(
						"RowDescription[%d] = %+v, want OID %d",
						column, description[column], test.wantOIDs[column],
					)
				}
			}
			rows := rowsOf(t, messages)
			if len(rows) != 1 || string(rows[0][test.boolAt]) != "t" {
				t.Fatalf("typed derived rows = %q", rows)
			}
			if test.textAt >= 0 && string(rows[0][test.textAt]) != "x" {
				t.Fatalf("typed derived text row = %q", rows)
			}
		})
	}
}
