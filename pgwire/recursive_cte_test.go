package pgwire

import (
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"
)

const recursiveCTEWireExtendedSQL = `WITH RECURSIVE reachable(node) AS (
	SELECT src AS node FROM recursive_edges WHERE src = $1
	UNION
	SELECT e.dst AS node
	FROM reachable AS r
	JOIN recursive_edges AS e ON r.node = e.src
)
SELECT node FROM reachable ORDER BY node`

const recursiveCTEWireDependencySQL = `WITH RECURSIVE
filtered(src, dst) AS MATERIALIZED (
	SELECT src, dst FROM recursive_edges WHERE dst <= $1
),
reachable(node) AS (
	SELECT src FROM filtered WHERE src = $2
	UNION
	SELECT e.dst FROM reachable r JOIN filtered e ON r.node = e.src
)
SELECT node FROM reachable ORDER BY node`

func TestPGWireRecursiveCTESimpleExtendedAndPositionedRefusal(t *testing.T) {
	c := connectSQLCatalog(t)
	for _, statement := range []string{
		`CREATE TABLE recursive_edges (` +
			`id STRING PRIMARY KEY, src INTEGER NOT NULL, dst INTEGER NOT NULL)`,
		`INSERT INTO recursive_edges VALUES ` +
			`({"id":"e01","src":0,"dst":1}),` +
			`({"id":"e12","src":1,"dst":2}),` +
			`({"id":"e23","src":2,"dst":3})`,
	} {
		if msgs := c.query(statement); has(msgs, msgErrorResponse) {
			t.Fatalf("%s: %s", statement,
				formatError(find(t, msgs, msgErrorResponse).body))
		}
	}

	simple := strings.Replace(recursiveCTEWireExtendedSQL, "$1", "0", 1)
	for name, msgs := range map[string][]backendMessage{
		"simple":   c.query(simple),
		"extended": extendedSQL(c, recursiveCTEWireExtendedSQL, [][]byte{[]byte("0")}),
	} {
		rows := rowsOf(t, msgs)
		if len(rows) != 4 {
			t.Fatalf("%s recursive CTE rows = %q, want four rows", name, rows)
		}
		for i := range rows {
			if len(rows[i]) != 1 || string(rows[i][0]) != strconv.Itoa(i) {
				t.Fatalf("%s recursive CTE rows = %q, want [[0] [1] [2] [3]]",
					name, rows)
			}
		}
		if got := commandTagOf(t, msgs); got != "SELECT 4" {
			t.Fatalf("%s recursive CTE tag = %q, want SELECT 4", name, got)
		}
	}

	dependencySimple := strings.NewReplacer("$1", "3", "$2", "0").Replace(
		recursiveCTEWireDependencySQL,
	)
	for name, msgs := range map[string][]backendMessage{
		"simple dependency": c.query(dependencySimple),
		"extended dependency": extendedSQL(
			c, recursiveCTEWireDependencySQL, [][]byte{[]byte("3"), []byte("0")},
		),
	} {
		rows := rowsOf(t, msgs)
		if len(rows) != 4 {
			t.Fatalf("%s recursive CTE rows = %q, want four rows", name, rows)
		}
		for i := range rows {
			if len(rows[i]) != 1 || string(rows[i][0]) != strconv.Itoa(i) {
				t.Fatalf("%s recursive CTE rows = %q, want [[0] [1] [2] [3]]",
					name, rows)
			}
		}
		if got := commandTagOf(t, msgs); got != "SELECT 4" {
			t.Fatalf("%s recursive CTE tag = %q, want SELECT 4", name, got)
		}
	}

	unsupported := `/* préfix */ WITH RECURSIVE reachable(node) AS (` +
		`SELECT src FROM recursive_edges WHERE src = 0 ` +
		`UNION SELECT e.dst FROM reachable r ` +
		`JOIN recursive_edges e ON r.node = e.src` +
		`) SEARCH DEPTH FIRST BY node SET traversal_order ` +
		`SELECT node FROM reachable`
	bytePos := strings.Index(unsupported, "SEARCH")
	wantPosition := strconv.Itoa(utf8.RuneCountInString(unsupported[:bytePos]) + 1)
	for name, msgs := range map[string][]backendMessage{
		"simple":   c.query(unsupported),
		"extended": extendedSQL(c, unsupported, nil),
	} {
		fields := expectError(t, msgs, sqlstateFeatureNotSupported)
		if fields['P'] != wantPosition {
			t.Fatalf("%s recursive 0A000 position = %q, want %q",
				name, fields['P'], wantPosition)
		}
		if !strings.Contains(fields['M'], "SEARCH") {
			t.Fatalf("%s recursive 0A000 message = %q, want SEARCH", name, fields['M'])
		}
		if has(msgs, msgDataRow) {
			t.Fatalf("%s recursive refusal emitted a partial DataRow", name)
		}
	}

	msgs := c.query(`SELECT src FROM recursive_edges WHERE src = 0`)
	if got := commandTagOf(t, msgs); got != "SELECT 1" {
		t.Fatalf("post-recursive-refusal recovery tag = %q, want SELECT 1", got)
	}
}
