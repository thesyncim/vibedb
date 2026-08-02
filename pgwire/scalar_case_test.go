package pgwire

import "testing"

func TestScalarCaseRowsDescriptionsErrorsAndRecovery(t *testing.T) {
	c := connect(t)
	messages := c.query(`SELECT
		CASE WHEN flag THEN name ELSE 'unknown' END AS label,
		CASE id WHEN 1 THEN TRUE ELSE FALSE END AS first,
		CASE WHEN id = 1 THEN age + 1 ELSE 0 END AS score
		FROM users WHERE id <= 2 ORDER BY id`)
	description := decodeRowDescription(t, find(t, messages, msgRowDescription).body)
	if len(description) != 3 || description[0].oid != oidText ||
		description[1].oid != oidBool || description[2].oid != oidJSON {
		t.Fatalf("CASE RowDescription = %+v", description)
	}
	rows := rowsOf(t, messages)
	if len(rows) != 2 || string(rows[0][0]) != "unknown" || string(rows[0][1]) != "t" ||
		string(rows[0][2]) != "3.1e1" || string(rows[1][0]) != "unknown" ||
		string(rows[1][1]) != "f" || string(rows[1][2]) != "0" {
		t.Fatalf("CASE rows = %q", rows)
	}

	messages = extendedSQL(c,
		`SELECT CASE id WHEN $1 THEN 'hit' ELSE 'miss' END FROM users WHERE id = 1`,
		[][]byte{[]byte("1")})
	description = decodeRowDescription(t, find(t, messages, msgRowDescription).body)
	rows = rowsOf(t, messages)
	if len(description) != 1 || description[0].oid != oidText ||
		len(rows) != 1 || string(rows[0][0]) != "hit" {
		t.Fatalf("extended CASE = %+v / %q", description, rows)
	}

	failure := c.query(`SELECT CASE WHEN name THEN 1 ELSE 0 END FROM users`)
	fields := expectError(t, failure, sqlstateDatatypeMismatch)
	assertReadyStatus(t, failure, statusIdle)
	if fields['P'] == "" {
		t.Fatal("CASE datatype mismatch omitted position")
	}
	for _, source := range []string{
		`SELECT CASE WHEN TRUE THEN 1 ELSE 'x' END FROM users`,
		`SELECT CASE WHEN name LIKE 'a%' THEN 1 ELSE 0 END FROM users`,
	} {
		failure = c.query(source)
		fields = expectError(t, failure, sqlstateFeatureNotSupported)
		assertReadyStatus(t, failure, statusIdle)
		if fields['P'] == "" {
			t.Fatalf("CASE refusal %q omitted position", source)
		}
	}
	messages = c.query(`SELECT CASE WHEN TRUE THEN 1 ELSE 0 END FROM users WHERE id = 1`)
	if has(messages, msgErrorResponse) || len(rowsOf(t, messages)) != 1 {
		t.Fatalf("CASE session did not recover: %s", tags(messages))
	}
}
