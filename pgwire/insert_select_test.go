package pgwire

import (
	"slices"
	"strconv"
	"strings"
	"testing"
)

func TestPGWireInsertSelectSimpleExtendedReturningAndRecovery(t *testing.T) {
	c := connectSQLCatalog(t)
	for _, statement := range []string{
		`CREATE TABLE wire_insert_source (` +
			`id STRING PRIMARY KEY, keep BOOL NOT NULL)`,
		`CREATE TABLE wire_insert_target (` +
			`id STRING PRIMARY KEY, keep BOOL NOT NULL)`,
		`INSERT INTO wire_insert_source VALUES ` +
			`({"id":"a","keep":true}),` +
			`({"id":"b","keep":false}),` +
			`({"id":"c","keep":true})`,
	} {
		messages := c.query(statement)
		if has(messages, msgErrorResponse) {
			t.Fatalf("%s: %s", statement,
				formatError(find(t, messages, msgErrorResponse).body))
		}
	}

	messages := c.query(
		`INSERT INTO wire_insert_target ` +
			`SELECT * FROM wire_insert_source WHERE keep = true ORDER BY id ` +
			`RETURNING id`,
	)
	if got := commandTagOf(t, messages); got != "INSERT 0 2" {
		t.Fatalf("simple tag = %q", got)
	}
	rows := rowsOf(t, messages)
	if len(rows) != 2 || string(rows[0][0]) != `"a"` ||
		string(rows[1][0]) != `"c"` {
		t.Fatalf("simple RETURNING rows = %q", rows)
	}

	messages = extendedSQL(
		c,
		`INSERT INTO wire_insert_target `+
			`SELECT * FROM wire_insert_source WHERE id >= $1 ORDER BY id `+
			`ON CONFLICT DO NOTHING RETURNING id`,
		[][]byte{[]byte(`b`)},
	)
	if got := commandTagOf(t, messages); got != "INSERT 0 1" {
		t.Fatalf("extended tag = %q", got)
	}
	rows = rowsOf(t, messages)
	if len(rows) != 1 || string(rows[0][0]) != `"b"` {
		t.Fatalf("extended RETURNING rows = %q", rows)
	}

	failure := c.query(`INSERT INTO wire_insert_target SELECT * FROM wire_insert_source`)
	fields := expectError(t, failure, sqlstateUniqueViolation)
	assertReadyStatus(t, failure, statusIdle)
	if !strings.Contains(fields['M'], "primary key") {
		t.Fatalf("unique message = %q", fields['M'])
	}
	rows = rowsOf(t, c.query(`SELECT id FROM wire_insert_target ORDER BY id`))
	if len(rows) != 3 {
		t.Fatalf("session recovery rows = %q", rows)
	}
}

func TestPGWireInsertSelectDynamicTypeSQLStatePositionAtomicity(t *testing.T) {
	c := connectSQLCatalog(t)
	for _, statement := range []string{
		`CREATE TABLE wire_shape_source (id STRING PRIMARY KEY)`,
		`CREATE TABLE wire_shape_target (id STRING PRIMARY KEY)`,
		`INSERT INTO wire_shape_source VALUES ` +
			`({"id":"a","payload":{"id":"new"}}),` +
			`({"id":"b","payload":"not-a-document"})`,
	} {
		if messages := c.query(statement); has(messages, msgErrorResponse) {
			t.Fatalf("%s: %s", statement,
				formatError(find(t, messages, msgErrorResponse).body))
		}
	}
	statement := `INSERT INTO wire_shape_target ` +
		`SELECT payload FROM wire_shape_source ORDER BY id RETURNING id`
	failure := c.query(statement)
	fields := expectError(t, failure, sqlstateDatatypeMismatch)
	assertReadyStatus(t, failure, statusIdle)
	wantPosition := strconv.Itoa(strings.Index(statement, "payload") + 1)
	if fields['P'] != wantPosition {
		t.Fatalf("dynamic type position = %q, want %q", fields['P'], wantPosition)
	}
	rows := rowsOf(t, c.query(`SELECT count(*) FROM wire_shape_target`))
	if len(rows) != 1 || string(rows[0][0]) != "0" {
		t.Fatalf("dynamic type failure published rows: %q", rows)
	}
	if got := commandTagOf(t, c.query(
		`INSERT INTO wire_shape_target `+
			`SELECT payload FROM wire_shape_source WHERE id = 'a'`,
	)); got != "INSERT 0 1" {
		t.Fatalf("recovery tag = %q", got)
	}
}

func TestPGWireInsertSelectValuesDocumentParameterTyping(t *testing.T) {
	c := connectSQLCatalog(t)
	if messages := c.query(
		`CREATE TABLE wire_values_target (id STRING PRIMARY KEY)`,
	); has(messages, msgErrorResponse) {
		t.Fatal(formatError(find(t, messages, msgErrorResponse).body))
	}

	statement := `INSERT INTO wire_values_target (VALUES ($1)) RETURNING id`
	messages := extendedSQL(c, statement, [][]byte{[]byte(`{"id":"wire"}`)})
	if got := commandTagOf(t, messages); got != "INSERT 0 1" {
		t.Fatalf("VALUES source tag = %q", got)
	}
	if got := decodeParameterDescription(
		t, find(t, messages, msgParameterDesc).body,
	); !slices.Equal(got, []int32{oidJSON}) {
		t.Fatalf("VALUES source parameter OIDs = %v, want [%d]", got, oidJSON)
	}
	rows := rowsOf(t, messages)
	if len(rows) != 1 || string(rows[0][0]) != `"wire"` {
		t.Fatalf("VALUES source RETURNING rows = %q", rows)
	}

	compound := `INSERT INTO wire_values_target ` +
		`((VALUES ($1)) UNION ALL (VALUES ($2))) RETURNING id`
	messages = extendedSQL(c, compound, [][]byte{
		[]byte(`{"id":"left"}`), []byte(`{"id":"right"}`),
	})
	if got := decodeParameterDescription(
		t, find(t, messages, msgParameterDesc).body,
	); !slices.Equal(got, []int32{oidJSON, oidJSON}) {
		t.Fatalf("compound VALUES parameter OIDs = %v, want json/json", got)
	}
	if got := rowsOf(t, messages); len(got) != 2 ||
		string(got[0][0]) != `"left"` || string(got[1][0]) != `"right"` {
		t.Fatalf("compound VALUES rows = %q", got)
	}

	// The same wire spelling resolves to PostgreSQL text for standalone VALUES;
	// only an INSERT source's document output owns JSON typing.
	messages = extendedSQL(c, `VALUES ($1)`, [][]byte{[]byte(`{"id":"scalar"}`)})
	if got := decodeParameterDescription(
		t, find(t, messages, msgParameterDesc).body,
	); !slices.Equal(got, []int32{oidText}) {
		t.Fatalf("standalone VALUES parameter OIDs = %v, want text", got)
	}
	if got := rowsOf(t, messages); len(got) != 1 ||
		string(got[0][0]) != `{"id":"scalar"}` {
		t.Fatalf("standalone VALUES scalar row = %q", got)
	}

	failure := extendedSQL(
		c, statement, [][]byte{[]byte(`{"id":"broken"`)},
	)
	fields := expectError(t, failure, sqlstateInvalidParameterValue)
	assertReadyStatus(t, failure, statusIdle)
	if fields['C'] != sqlstateInvalidParameterValue {
		t.Fatalf("invalid document SQLSTATE = %q", fields['C'])
	}
	if fields['P'] != strconv.Itoa(strings.Index(statement, "$1")+1) {
		t.Fatalf("invalid document position = %q", fields['P'])
	}
	if !strings.Contains(fields['M'], "document parameter $1") ||
		strings.Contains(fields['M'], `{"id":"broken"`) {
		t.Fatalf("invalid document message = %q", fields['M'])
	}
}

func TestPGWireInsertSelectNestedValuesLineageReuseAndConflict(t *testing.T) {
	c := connectSQLCatalog(t)
	for _, statement := range []string{
		`CREATE TABLE wire_lineage_seed (id STRING PRIMARY KEY)`,
		`CREATE TABLE wire_lineage_target (id STRING PRIMARY KEY)`,
	} {
		if messages := c.query(statement); has(messages, msgErrorResponse) {
			t.Fatalf("%s: %s", statement,
				formatError(find(t, messages, msgErrorResponse).body))
		}
	}

	// Both authored occurrences of $1 contribute the one stored output. The
	// wire parameter is therefore one coherent JSON role, not a false
	// scalar/document conflict at the nested SELECT boundary.
	reused := `INSERT INTO wire_lineage_target ` +
		`SELECT v.* FROM (` +
		`SELECT * FROM wire_lineage_seed WHERE id = 'missing' ` +
		`UNION ALL VALUES ($1)) AS v ` +
		`UNION ALL VALUES ($1) ON CONFLICT DO NOTHING RETURNING id`
	messages := extendedSQL(c, reused, [][]byte{[]byte(`{"id":"shared"}`)})
	if has(messages, msgErrorResponse) {
		t.Fatal(formatError(find(t, messages, msgErrorResponse).body))
	}
	if got := decodeParameterDescription(
		t, find(t, messages, msgParameterDesc).body,
	); !slices.Equal(got, []int32{oidJSON}) {
		t.Fatalf("reused nested document OIDs = %v", got)
	}
	if got := rowsOf(t, messages); len(got) != 1 ||
		string(got[0][0]) != `"shared"` {
		t.Fatalf("reused nested document rows = %q", got)
	}

	// Reusing one wire value as a predicate scalar and as a whole document is
	// genuinely incompatible and remains a positioned prepare-time refusal.
	conflict := `INSERT INTO wire_lineage_target ` +
		`SELECT v.* FROM (` +
		`SELECT * FROM wire_lineage_seed WHERE id = $1 ` +
		`UNION ALL VALUES ($1)) AS v`
	failure := extendedSQL(
		c, conflict, [][]byte{[]byte(`{"id":"conflict"}`)},
	)
	fields := expectError(t, failure, sqlstateDatatypeMismatch)
	assertReadyStatus(t, failure, statusIdle)
	if !strings.Contains(fields['M'], "both a JSON document and a scalar") {
		t.Fatalf("reused incompatible role message = %q", fields['M'])
	}
}

func TestPGWireInsertSelectNestedInvalidDocumentIdentityPositionRecovery(t *testing.T) {
	c := connectSQLCatalog(t)
	if messages := c.query(
		`CREATE TABLE wire_position_target (id STRING PRIMARY KEY)`,
	); has(messages, msgErrorResponse) {
		t.Fatal(formatError(find(t, messages, msgErrorResponse).body))
	}

	statement := `INSERT INTO wire_position_target ` +
		`WITH "café"(doc) AS ((VALUES ($2))) SELECT doc FROM "café"`
	secret := `{"secret":"DO-NOT-ECHO"`
	failure := extendedSQL(c, statement, [][]byte{
		[]byte("unused"), []byte(secret),
	})
	fields := expectError(t, failure, sqlstateInvalidParameterValue)
	assertReadyStatus(t, failure, statusIdle)
	wantPosition := strconv.Itoa(charPosition(
		statement, strings.Index(statement, "$2"),
	))
	if fields['P'] != wantPosition {
		t.Fatalf("nested invalid document position = %q, want %q",
			fields['P'], wantPosition)
	}
	if !strings.Contains(fields['M'], "document parameter $2") ||
		strings.Contains(fields['M'], secret) || strings.Contains(fields['M'], "DO-NOT-ECHO") {
		t.Fatalf("nested invalid document message = %q", fields['M'])
	}
	if rows := rowsOf(t, c.query(`SELECT count(*) FROM wire_position_target`)); len(rows) != 1 || string(rows[0][0]) != "0" {
		t.Fatalf("bind failure recovery rows = %q", rows)
	}
}
