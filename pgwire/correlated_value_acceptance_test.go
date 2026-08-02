package pgwire

import (
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/query"
)

const correlatedValueWirePreparedSQL = `
	SELECT o.id
	FROM cv_wire_outer AS o
	WHERE o.probe IN (
		SELECT i.value FROM cv_wire_inner AS i
		WHERE i.tenant = o.tenant
		  AND i.region = o.region
		  AND i.enabled = $1
	)
	ORDER BY o.id`

func seedCorrelatedValueWire(t *testing.T, c *testClient) {
	t.Helper()
	for _, statement := range []string{
		`CREATE TABLE cv_wire_outer (` +
			`id STRING PRIMARY KEY, tenant STRING, region STRING, probe ANY)`,
		`CREATE INDEX cv_wire_outer_lookup ON cv_wire_outer (tenant, region, probe)`,
		`CREATE TABLE cv_wire_inner (` +
			`id STRING PRIMARY KEY, tenant STRING, region STRING, value ANY, enabled BOOL)`,
		`INSERT INTO cv_wire_outer VALUES ` +
			`('{"id":"a_match","tenant":"t1","region":"r1","probe":10.00}'),` +
			`('{"id":"b_unknown","tenant":"t1","region":"r1","probe":12}'),` +
			`('{"id":"c_empty_null","tenant":"t3","region":"r3","probe":null}'),` +
			`('{"id":"d_known_no_match","tenant":"t5","region":"r5","probe":9}'),` +
			`('{"id":"e_object","tenant":"t4","region":"r4","probe":{"b":2,"a":1}}'),` +
			`('{"id":"f_decimal","tenant":"t7","region":"r7","probe":9007199254740993.000}')`,
		`INSERT INTO cv_wire_inner VALUES ` +
			`('{"id":"t1-match","tenant":"t1","region":"r1","value":10,"enabled":true}'),` +
			`('{"id":"t1-null","tenant":"t1","region":"r1","value":null,"enabled":true}'),` +
			`('{"id":"t4-object","tenant":"t4","region":"r4","value":{"a":1,"b":2},"enabled":true}'),` +
			`('{"id":"t5-known","tenant":"t5","region":"r5","value":8,"enabled":true}'),` +
			`('{"id":"t7-decimal","tenant":"t7","region":"r7","value":9007199254740993,"enabled":true}')`,
	} {
		messages := c.query(statement)
		if has(messages, msgErrorResponse) {
			t.Fatalf("%s: %s", statement,
				formatError(find(t, messages, msgErrorResponse).body))
		}
		requireCorrelatedExistsWireCycle(t, c, messages, statusIdle)
	}
}

func correlatedValueWireIDs(t *testing.T, messages []backendMessage) []string {
	t.Helper()
	rows := rowsOf(t, messages)
	ids := make([]string, 0, len(rows))
	for _, row := range rows {
		if len(row) != 1 {
			t.Fatalf("row = %q, want one column", row)
		}
		ids = append(ids, string(row[0]))
	}
	return ids
}

func requireCorrelatedValueWireRows(
	t *testing.T,
	c *testClient,
	messages []backendMessage,
	want []string,
) {
	t.Helper()
	requireCorrelatedExistsWireCycle(t, c, messages, statusIdle)
	if got := correlatedValueWireIDs(t, messages); !slices.Equal(got, want) {
		t.Fatalf("rows = %q, want %q", got, want)
	}
	if got := commandTagOf(t, messages); got != "SELECT "+strconv.Itoa(len(want)) {
		t.Fatalf("command tag = %q, want SELECT %d", got, len(want))
	}
}

func TestPGWireCorrelatedValueSimpleExtendedAndNamedPrepared(t *testing.T) {
	c := connectSQLCatalog(t)
	seedCorrelatedValueWire(t, c)

	compositeExists := `SELECT o.id FROM cv_wire_outer AS o WHERE EXISTS (` +
		`SELECT 1 FROM cv_wire_inner AS i WHERE i.tenant = o.tenant ` +
		`AND i.region = o.region) ORDER BY o.id`
	requireCorrelatedValueWireRows(t, c, c.query(compositeExists),
		[]string{`"a_match"`, `"b_unknown"`, `"d_known_no_match"`, `"e_object"`, `"f_decimal"`})
	directNotExists := strings.Replace(compositeExists,
		"WHERE EXISTS (", "WHERE NOT (EXISTS (", 1)
	directNotExists = strings.Replace(directNotExists, ") ORDER BY", ")) ORDER BY", 1)
	requireCorrelatedValueWireRows(t, c, c.query(directNotExists),
		[]string{`"c_empty_null"`})

	correlatedIn := strings.ReplaceAll(correlatedValueWirePreparedSQL, "$1", "TRUE")
	requireCorrelatedValueWireRows(t, c, c.query(correlatedIn),
		[]string{`"a_match"`, `"e_object"`, `"f_decimal"`})
	correlatedNotIn := strings.Replace(correlatedIn, "o.probe IN (", "o.probe NOT IN (", 1)
	requireCorrelatedValueWireRows(t, c, c.query(correlatedNotIn),
		[]string{`"c_empty_null"`, `"d_known_no_match"`})
	directNotIn := strings.Replace(correlatedIn,
		"WHERE o.probe IN (", "WHERE NOT (o.probe IN (", 1)
	directNotIn = strings.Replace(directNotIn, ")\n\tORDER BY", "))\n\tORDER BY", 1)
	requireCorrelatedValueWireRows(t, c, c.query(directNotIn),
		[]string{`"c_empty_null"`, `"d_known_no_match"`})

	extended := extendedSQL(c, correlatedValueWirePreparedSQL, [][]byte{[]byte("true")})
	requireCorrelatedValueWireRows(t, c, extended,
		[]string{`"a_match"`, `"e_object"`, `"f_decimal"`})

	c.send(msgParse, parseMsg("correlated-value", correlatedValueWirePreparedSQL))
	c.send(msgSync, nil)
	parsed := c.until(msgReadyForQuery)
	requireCorrelatedExistsWireCycle(t, c, parsed, statusIdle)
	if !has(parsed, msgParseComplete) {
		t.Fatalf("named correlated value statement did not parse: %s", tags(parsed))
	}
	for i, run := range []struct {
		value string
		want  []string
	}{
		{"true", []string{`"a_match"`, `"e_object"`, `"f_decimal"`}},
		{"false", nil},
		{"true", []string{`"a_match"`, `"e_object"`, `"f_decimal"`}},
	} {
		messages := executeCorrelatedExistsWirePrepared(c, "correlated-value",
			"correlated-value-"+strconv.Itoa(i), [][]byte{[]byte(run.value)})
		requireCorrelatedValueWireRows(t, c, messages, run.want)
	}
}

func seedCorrelatedScalarWire(t *testing.T, c *testClient) {
	t.Helper()
	for _, statement := range []string{
		`CREATE TABLE cv_wire_scalar_outer (` +
			`id STRING PRIMARY KEY, tenant STRING, region STRING, probe ANY)`,
		`CREATE TABLE cv_wire_scalar_inner (` +
			`id STRING PRIMARY KEY, tenant STRING, region STRING, value ANY)`,
		`INSERT INTO cv_wire_scalar_outer VALUES ` +
			`('{"id":"a_good","tenant":"good","region":"r","probe":5}'),` +
			`('{"id":"b_dup_equal","tenant":"dup-equal","region":"r","probe":7}'),` +
			`('{"id":"c_dup_null","tenant":"dup-null","region":"r","probe":null}'),` +
			`('{"id":"d_filtered_bad","tenant":"filtered","region":"r","probe":9}')`,
		`INSERT INTO cv_wire_scalar_inner VALUES ` +
			`('{"id":"good","tenant":"good","region":"r","value":5.00}'),` +
			`('{"id":"equal-a","tenant":"dup-equal","region":"r","value":7}'),` +
			`('{"id":"equal-b","tenant":"dup-equal","region":"r","value":7.0}'),` +
			`('{"id":"null-a","tenant":"dup-null","region":"r","value":null}'),` +
			`('{"id":"null-b","tenant":"dup-null","region":"r"}'),` +
			`('{"id":"filtered-a","tenant":"filtered","region":"r","value":9}'),` +
			`('{"id":"filtered-b","tenant":"filtered","region":"r","value":10}'),` +
			`('{"id":"unprobed-a","tenant":"unprobed","region":"r","value":1}'),` +
			`('{"id":"unprobed-b","tenant":"unprobed","region":"r","value":2}')`,
	} {
		messages := c.query(statement)
		if has(messages, msgErrorResponse) {
			t.Fatalf("%s: %s", statement,
				formatError(find(t, messages, msgErrorResponse).body))
		}
		requireCorrelatedExistsWireCycle(t, c, messages, statusIdle)
	}
}

const correlatedScalarWireByID = `
	SELECT o.id FROM cv_wire_scalar_outer AS o
	WHERE o.id = $1 AND o.probe = (
		SELECT i.value FROM cv_wire_scalar_inner AS i
		WHERE i.tenant = o.tenant AND i.region = o.region
	)`

func requireCorrelatedValueWireCardinalityFailure(
	t *testing.T,
	c *testClient,
	messages []backendMessage,
	wantStatus byte,
) {
	t.Helper()
	requireCorrelatedExistsWireFailure(
		t, messages, sqlstateCardinalityViolation, wantStatus,
	)
	if wantStatus == statusIdle {
		requireCorrelatedExistsWireSentinel(t, c, statusIdle)
	}
}

func TestPGWireCorrelatedScalarCardinalityIsAtomicAndProtocolRecovers(t *testing.T) {
	c := connectSQLCatalog(t)
	seedCorrelatedScalarWire(t, c)

	// Multi-row groups not reached by the outer predicate, and groups with no
	// outer row at all, cannot raise 21000 during the grouped build.
	goodSimple := strings.Replace(correlatedScalarWireByID, "$1", "'a_good'", 1)
	requireCorrelatedValueWireRows(t, c, c.query(goodSimple), []string{`"a_good"`})

	// This scan can encounter and stage a valid row before it reaches either
	// duplicate group. The protocol still publishes no DataRow or completion
	// tag because the SQL result is materialized atomically before encoding.
	allGroups := `SELECT o.id FROM cv_wire_scalar_outer AS o WHERE o.probe = (` +
		`SELECT i.value FROM cv_wire_scalar_inner AS i ` +
		`WHERE i.tenant = o.tenant AND i.region = o.region) ORDER BY o.id`
	requireCorrelatedValueWireCardinalityFailure(t, c, c.query(allGroups), statusIdle)

	dupNull := strings.Replace(correlatedScalarWireByID, "$1", "'c_dup_null'", 1)
	requireCorrelatedValueWireCardinalityFailure(t, c,
		extendedSQL(c, dupNull, nil), statusIdle)

	c.send(msgParse, parseMsg("correlated-scalar", correlatedScalarWireByID))
	c.send(msgSync, nil)
	parsed := c.until(msgReadyForQuery)
	requireCorrelatedExistsWireCycle(t, c, parsed, statusIdle)
	if !has(parsed, msgParseComplete) {
		t.Fatalf("named scalar statement did not parse: %s", tags(parsed))
	}
	requireCorrelatedValueWireRows(t, c,
		executeCorrelatedExistsWirePrepared(c, "correlated-scalar", "scalar-good-1",
			[][]byte{[]byte("a_good")}),
		[]string{`"a_good"`})
	requireCorrelatedValueWireCardinalityFailure(t, c,
		executeCorrelatedExistsWirePrepared(c, "correlated-scalar", "scalar-bad",
			[][]byte{[]byte("b_dup_equal")}), statusIdle)
	// Parse survives an Execute-time failure and its reusable state must not
	// retain the failed group's cardinality marker.
	requireCorrelatedValueWireRows(t, c,
		executeCorrelatedExistsWirePrepared(c, "correlated-scalar", "scalar-good-2",
			[][]byte{[]byte("a_good")}),
		[]string{`"a_good"`})

	begin := c.query("BEGIN")
	requireCorrelatedExistsWireCycle(t, c, begin, statusInTx)
	badInTransaction := strings.Replace(correlatedScalarWireByID, "$1", "'b_dup_equal'", 1)
	requireCorrelatedValueWireCardinalityFailure(t, c,
		c.query(badInTransaction), statusFailedT)
	rollback := c.query("ROLLBACK")
	requireCorrelatedExistsWireCycle(t, c, rollback, statusIdle)
	if got := commandTagOf(t, rollback); got != "ROLLBACK" {
		t.Fatalf("rollback tag = %q, want ROLLBACK", got)
	}
	requireCorrelatedValueWireRows(t, c, c.query(goodSimple), []string{`"a_good"`})
}

func TestPGWireCorrelatedValueCancellationPublishesNoProtocolResultAndRecovers(t *testing.T) {
	c, server := connectSQLCatalogWithServer(t)
	seedCorrelatedValueWire(t, c)
	server.mu.Lock()
	session := server.sessions[c.pid]
	server.mu.Unlock()
	if session == nil {
		t.Fatal("correlated-value wire session is not registered")
	}
	var cancel query.CancelFlag
	if err := session.sql.SetCancelFlag(&cancel); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := session.sql.SetCancelFlag(&session.queryCancel); err != nil {
			t.Error(err)
		}
	}()

	c.send(msgParse, parseMsg("cancel-correlated-value", correlatedValueWirePreparedSQL))
	c.send(msgSync, nil)
	parsed := c.until(msgReadyForQuery)
	requireCorrelatedExistsWireCycle(t, c, parsed, statusIdle)
	if !has(parsed, msgParseComplete) {
		t.Fatalf("cancel fixture did not prepare: %s", tags(parsed))
	}
	cancel.Cancel()
	failure := executeCorrelatedExistsWirePrepared(c, "cancel-correlated-value",
		"cancel-correlated-value-portal", [][]byte{[]byte("true")})
	requireCorrelatedExistsWireFailure(t, failure, sqlstateQueryCanceled, statusIdle)
	requireCorrelatedExistsWireSentinel(t, c, statusIdle)

	cancel.Reset()
	recovered := executeCorrelatedExistsWirePrepared(c, "cancel-correlated-value",
		"recovered-correlated-value-portal", [][]byte{[]byte("true")})
	requireCorrelatedValueWireRows(t, c, recovered,
		[]string{`"a_match"`, `"e_object"`, `"f_decimal"`})
}
