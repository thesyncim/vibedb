package pgwire

import (
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"
)

func seedLateralCorrelationSlotsProtocol(t *testing.T, c *testClient) {
	t.Helper()
	for _, statement := range []string{
		`CREATE TABLE lateral_slot_wire_accounts (id STRING PRIMARY KEY)`,
		`CREATE TABLE lateral_slot_wire_items (` +
			`id STRING PRIMARY KEY, owner STRING, active BOOL)`,
		`INSERT INTO lateral_slot_wire_accounts VALUES ` +
			`({"id":"a1"}),({"id":"a2"})`,
		`INSERT INTO lateral_slot_wire_items VALUES ` +
			`({"id":"i1","owner":"a1","active":true}),` +
			`({"id":"i2","owner":"a1","active":false}),` +
			`({"id":"i3","owner":"a2","active":false})`,
	} {
		messages := c.query(statement)
		if has(messages, msgErrorResponse) {
			t.Fatalf("%s: %s", statement,
				formatError(find(t, messages, msgErrorResponse).body))
		}
	}
}

func lateralCorrelationSlotsWireSQL(active string) string {
	return `SELECT a.id, q.id FROM lateral_slot_wire_accounts a CROSS JOIN LATERAL (` +
		`SELECT d.id AS id FROM lateral_slot_wire_items i CROSS JOIN LATERAL (` +
		`SELECT x.id FROM lateral_slot_wire_items x ` +
		`WHERE x.owner = a.id AND x.active = i.active AND x.active = ` + active +
		`) d WHERE i.owner = a.id) q ORDER BY a.id, q.id`
}

func requireLateralCorrelationSlotRows(
	t *testing.T,
	messages []backendMessage,
	want [][]string,
) {
	t.Helper()
	if has(messages, msgErrorResponse) {
		t.Fatalf("LATERAL execution failed: %s",
			formatError(find(t, messages, msgErrorResponse).body))
	}
	got := rowsOf(t, messages)
	if len(got) != len(want) {
		t.Fatalf("LATERAL rows = %q, want %q", got, want)
	}
	for row := range want {
		if len(got[row]) != len(want[row]) {
			t.Fatalf("LATERAL row %d width = %d, want %d", row, len(got[row]), len(want[row]))
		}
		for column := range want[row] {
			if string(got[row][column]) != want[row][column] {
				t.Fatalf("LATERAL cell %d/%d = %q, want %q",
					row, column, got[row][column], want[row][column])
			}
		}
	}
	if commandTagOf(t, messages) != "SELECT "+strconv.Itoa(len(want)) {
		t.Fatalf("LATERAL command tags = %s", tags(messages))
	}
	assertReadyStatus(t, messages, statusIdle)
}

func executeNamedLateralCorrelationSlots(
	c *testClient,
	statement, portal, active string,
) []backendMessage {
	c.t.Helper()
	c.send(msgBind, bindMsg(portal, statement, nil, [][]byte{[]byte(active)}, nil))
	c.send(msgExecute, executeMsg(portal, 0))
	c.send(msgSync, nil)
	return c.until(msgReadyForQuery)
}

func TestPGWireLateralCorrelationSlotsSimpleExtendedNamedReuseAndRecovery(t *testing.T) {
	c := connectSQLCatalog(t)
	seedLateralCorrelationSlotsProtocol(t, c)

	requireLateralCorrelationSlotRows(t, c.query(lateralCorrelationSlotsWireSQL("TRUE")),
		[][]string{{`"a1"`, `"i1"`}})
	requireLateralCorrelationSlotRows(t,
		extendedSQL(c, lateralCorrelationSlotsWireSQL("$1"), [][]byte{[]byte("false")}),
		[][]string{{`"a1"`, `"i2"`}, {`"a2"`, `"i3"`}})

	const preparedName = "lateral-correlation-slots"
	c.send(msgParse, parseMsg(preparedName, lateralCorrelationSlotsWireSQL("$1")))
	c.send(msgSync, nil)
	parsed := c.until(msgReadyForQuery)
	if has(parsed, msgErrorResponse) || !has(parsed, msgParseComplete) {
		t.Fatalf("named LATERAL Parse = %s", tags(parsed))
	}
	assertReadyStatus(t, parsed, statusIdle)
	for i, run := range []struct {
		active string
		want   [][]string
	}{
		{"true", [][]string{{`"a1"`, `"i1"`}}},
		{"false", [][]string{{`"a1"`, `"i2"`}, {`"a2"`, `"i3"`}}},
		{"true", [][]string{{`"a1"`, `"i1"`}}},
	} {
		requireLateralCorrelationSlotRows(t,
			executeNamedLateralCorrelationSlots(c, preparedName,
				"lateral-correlation-slots-"+strconv.Itoa(i), run.active),
			run.want)
	}

	begin := c.query("BEGIN")
	assertReadyStatus(t, begin, statusInTx)
	unsupported := `/* préfix */ SELECT a.id, d.id FROM lateral_slot_wire_accounts a ` +
		`CROSS JOIN LATERAL (SELECT i.id FROM lateral_slot_wire_items i ` +
		`WHERE i.owner = a.id AND EXISTS (` +
		`SELECT x.id FROM lateral_slot_wire_items x WHERE x.owner = i.owner)) d`
	failure := c.query(unsupported)
	fields := expectError(t, failure, sqlstateFeatureNotSupported)
	if !strings.Contains(fields['M'], "predicate subqueries") ||
		!strings.Contains(fields['M'], "LATERAL") {
		t.Fatalf("predicate-subquery refusal = %q", fields['M'])
	}
	bytePosition := strings.Index(unsupported, "EXISTS")
	wantPosition := utf8.RuneCountInString(unsupported[:bytePosition]) + 1
	if fields['P'] != strconv.Itoa(wantPosition) {
		t.Fatalf("predicate-subquery position = %q, want %d", fields['P'], wantPosition)
	}
	if has(failure, msgDataRow) || has(failure, msgCommandComplete) {
		t.Fatalf("predicate-subquery refusal published output: %s", tags(failure))
	}
	assertReadyStatus(t, failure, statusFailedT)

	rollback := c.query("ROLLBACK")
	if got := commandTagOf(t, rollback); got != "ROLLBACK" {
		t.Fatalf("rollback command tag = %q", got)
	}
	assertReadyStatus(t, rollback, statusIdle)
	requireLateralCorrelationSlotRows(t,
		executeNamedLateralCorrelationSlots(c, preparedName,
			"lateral-correlation-slots-recovered", "true"),
		[][]string{{`"a1"`, `"i1"`}})
}
