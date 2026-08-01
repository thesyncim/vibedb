package pgwire

import (
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"
)

func seedLateralGroupProtocol(t *testing.T, c *testClient) {
	t.Helper()
	for _, statement := range []string{
		`CREATE TABLE lateral_group_accounts (id STRING PRIMARY KEY, value NUMBER)`,
		`CREATE TABLE lateral_group_items (id STRING PRIMARY KEY, owner STRING)`,
		`INSERT INTO lateral_group_accounts VALUES ` +
			`({"id":"a","value":0.1}),({"id":"b","value":2})`,
		`INSERT INTO lateral_group_items VALUES ` +
			`({"id":"i1","owner":"a"}),({"id":"i2","owner":"a"}),` +
			`({"id":"i3","owner":"b"})`,
	} {
		if messages := c.query(statement); has(messages, msgErrorResponse) {
			t.Fatalf("%s: %s", statement,
				formatError(find(t, messages, msgErrorResponse).body))
		}
	}
}

func TestPGWireLateralGroupedAggregateSimpleExtendedSchemaAndRecovery(t *testing.T) {
	c := connectSQLCatalog(t)
	seedLateralGroupProtocol(t, c)
	plain := `SELECT a.id, d.total FROM lateral_group_accounts a LEFT JOIN LATERAL (` +
		`SELECT SUM(a.value) AS total FROM lateral_group_items i ` +
		`WHERE i.owner = a.id GROUP BY a.id HAVING SUM(a.value) >= 0.2` +
		`) d ON TRUE ORDER BY a.id`
	extended := strings.Replace(plain, "0.2", "$1", 1)
	for _, messages := range [][]backendMessage{
		c.query(plain), extendedSQL(c, extended, [][]byte{[]byte("0.2")}),
	} {
		description := decodeRowDescription(t, find(t, messages, msgRowDescription).body)
		if len(description) != 2 || description[0].name != "id" ||
			description[1].name != "d.total" || description[0].oid != oidJSON ||
			description[1].oid != oidJSON {
			t.Fatalf("grouped LATERAL RowDescription = %+v", description)
		}
		assertWindowProtocolRows(t, rowsOf(t, messages), [][]string{
			{`"a"`, `0.2`}, {`"b"`, `2`},
		})
		if got := commandTagOf(t, messages); got != "SELECT 2" {
			t.Fatalf("grouped LATERAL command tag = %q, want SELECT 2", got)
		}
	}

	unsupported := `/* préfix */ SELECT a.id, d.total FROM lateral_group_accounts a CROSS JOIN LATERAL (` +
		`SELECT SUM(a.value) AS total FROM lateral_group_items i ` +
		`WHERE i.owner = a.id GROUP BY a.id HAVING SUM(a.value) > 0 LIMIT 1) d`
	messages := c.query(unsupported)
	fields := expectError(t, messages, sqlstateFeatureNotSupported)
	position, err := strconv.Atoi(fields['P'])
	bytePosition := strings.Index(unsupported, "1) d")
	wantPosition := utf8.RuneCountInString(unsupported[:bytePosition]) + 1
	if err != nil || position != wantPosition {
		t.Fatalf("grouped LATERAL refusal position = %q/%v, want %d",
			fields['P'], err, wantPosition)
	}
	if has(messages, msgDataRow) {
		t.Fatal("unsupported grouped LATERAL tail emitted a partial row")
	}
	if got := commandTagOf(t, c.query(`SELECT id FROM lateral_group_accounts ORDER BY id`)); got != "SELECT 2" {
		t.Fatalf("post-error recovery command tag = %q, want SELECT 2", got)
	}
}
