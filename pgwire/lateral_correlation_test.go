package pgwire

import "testing"

func seedInheritedLateralProtocol(t *testing.T, c *testClient) {
	t.Helper()
	for _, statement := range []string{
		`CREATE TABLE lateral_frame_accounts (id STRING PRIMARY KEY)`,
		`CREATE TABLE lateral_frame_items (` +
			`id STRING PRIMARY KEY, owner STRING, active BOOL)`,
		`INSERT INTO lateral_frame_accounts VALUES ` +
			`({"id":"a1"}),({"id":"a2"})`,
		`INSERT INTO lateral_frame_items VALUES ` +
			`({"id":"i1","owner":"a1","active":true}),` +
			`({"id":"i2","owner":"a1","active":false}),` +
			`({"id":"i3","owner":"a2","active":false})`,
	} {
		if messages := c.query(statement); has(messages, msgErrorResponse) {
			t.Fatalf("%s: %s", statement,
				formatError(find(t, messages, msgErrorResponse).body))
		}
	}
}

func TestPGWireInheritedLateralSimpleExtendedSchemaAndReuse(t *testing.T) {
	c := connectSQLCatalog(t)
	seedInheritedLateralProtocol(t, c)
	query := func(active string) string {
		return `SELECT a.id, q.id FROM lateral_frame_accounts a CROSS JOIN LATERAL (` +
			`SELECT d.id AS id FROM lateral_frame_items i CROSS JOIN LATERAL (` +
			`SELECT x.id FROM lateral_frame_items x ` +
			`WHERE x.owner = a.id AND x.active = i.active AND x.active = ` + active +
			`) d WHERE i.owner = a.id) q ORDER BY a.id`
	}
	assert := func(messages []backendMessage, want [][]string) {
		t.Helper()
		description := decodeRowDescription(
			t, find(t, messages, msgRowDescription).body,
		)
		if len(description) != 2 || description[0].name != "id" ||
			description[1].name != "q.id" || description[0].oid != oidJSON ||
			description[1].oid != oidJSON {
			t.Fatalf("inherited LATERAL RowDescription = %+v", description)
		}
		assertWindowProtocolRows(t, rowsOf(t, messages), want)
	}
	assert(c.query(query("TRUE")), [][]string{{`"a1"`, `"i1"`}})
	extended := query("$1")
	assert(extendedSQL(c, extended, [][]byte{[]byte("false")}), [][]string{
		{`"a1"`, `"i2"`}, {`"a2"`, `"i3"`},
	})
	// A second bind proves no inherited slot survives the prior portal.
	assert(extendedSQL(c, extended, [][]byte{[]byte("true")}), [][]string{
		{`"a1"`, `"i1"`},
	})
}
