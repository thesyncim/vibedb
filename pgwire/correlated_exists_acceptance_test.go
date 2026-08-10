package pgwire

import (
	"slices"
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"
)

const correlatedExistsWireSQL = `
	SELECT o.id
	FROM ce_wire_outer AS o
	WHERE EXISTS (
		SELECT 1 FROM ce_wire_inner AS i
		WHERE i.match_key = o.match_key AND i.active = $1
	)
	ORDER BY o.id`

func seedCorrelatedExistsWire(t *testing.T, c *testClient) {
	t.Helper()
	for _, statement := range []string{
		`CREATE TABLE ce_wire_outer (id STRING PRIMARY KEY, match_key STRING)`,
		`CREATE INDEX ce_wire_outer_match ON ce_wire_outer (match_key)`,
		`CREATE TABLE ce_wire_inner (` +
			`id STRING PRIMARY KEY, match_key STRING, active BOOL)`,
		`INSERT INTO ce_wire_outer VALUES ` +
			`('{"id":"a_dup","match_key":"x"}'),` +
			`('{"id":"b_filtered","match_key":"y"}'),` +
			`('{"id":"c_local_reject","match_key":"z"}'),` +
			`('{"id":"d_null","match_key":null}'),` +
			`('{"id":"e_missing"}'),` +
			`('{"id":"f_empty","match_key":"none"}')`,
		`INSERT INTO ce_wire_inner VALUES ` +
			`('{"id":"i1","match_key":"x","active":true}'),` +
			`('{"id":"i2","match_key":"x","active":true}'),` +
			`('{"id":"i3","match_key":"y","active":true}'),` +
			`('{"id":"i4","match_key":"z","active":false}'),` +
			`('{"id":"i5","match_key":null,"active":true}')`,
	} {
		messages := c.query(statement)
		if has(messages, msgErrorResponse) {
			t.Fatalf("%s: %s", statement,
				formatError(find(t, messages, msgErrorResponse).body))
		}
		requireCorrelatedExistsWireCycle(t, c, messages, statusIdle)
	}
}

func correlatedExistsWireIDs(t *testing.T, messages []backendMessage) []string {
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

func executeCorrelatedExistsWirePrepared(
	c *testClient,
	statement, portal string,
	params [][]byte,
) []backendMessage {
	c.t.Helper()
	c.send(msgBind, bindMsg(portal, statement, nil, params, nil))
	c.send(msgExecute, executeMsg(portal, 0))
	c.send(msgSync, nil)
	return c.until(msgReadyForQuery)
}

func requireCorrelatedExistsWireReady(
	t *testing.T,
	messages []backendMessage,
	wantStatus byte,
) {
	t.Helper()
	if has(messages, msgErrorResponse) {
		t.Fatalf("wire query failed: %s",
			formatError(find(t, messages, msgErrorResponse).body))
	}
	if countTag(messages, msgReadyForQuery) != 1 {
		t.Fatalf("ReadyForQuery count = %d, want 1: %s",
			countTag(messages, msgReadyForQuery), tags(messages))
	}
	assertReadyStatus(t, messages, wantStatus)
}

func requireCorrelatedExistsWireCycle(
	t *testing.T,
	c *testClient,
	messages []backendMessage,
	wantStatus byte,
) {
	t.Helper()
	requireCorrelatedExistsWireReady(t, messages, wantStatus)
	requireCorrelatedExistsWireSentinel(t, c, wantStatus)
}

func requireCorrelatedExistsWireSentinel(
	t *testing.T,
	c *testClient,
	wantStatus byte,
) {
	t.Helper()
	// until(msgReadyForQuery) necessarily stops at the first ReadyForQuery. A
	// verified query in the following cycle is the bounded fence that detects an
	// erroneous extra ReadyForQuery left unread by the cycle under test.
	sentinel := c.query(`SELECT 7340033`)
	requireCorrelatedExistsWireReady(t, sentinel, wantStatus)
	if got, want := correlatedExistsWireIDs(t, sentinel), []string{"7340033"}; !slices.Equal(got, want) {
		t.Fatalf("sentinel rows = %q, want %q; prior cycle may have left messages",
			got, want)
	}
	if got := commandTagOf(t, sentinel); got != "SELECT 1" {
		t.Fatalf("sentinel command tag = %q, want SELECT 1", got)
	}
}

func requireCorrelatedExistsWireFailure(
	t *testing.T,
	messages []backendMessage,
	code string,
	wantStatus byte,
) map[byte]string {
	t.Helper()
	if got := countTag(messages, msgErrorResponse); got != 1 {
		t.Fatalf("ErrorResponse count = %d, want 1: %s", got, tags(messages))
	}
	if got := countTag(messages, msgDataRow); got != 0 {
		t.Fatalf("failed cycle published %d DataRow messages: %s", got, tags(messages))
	}
	if got := countTag(messages, msgCommandComplete); got != 0 {
		t.Fatalf("failed cycle published %d CommandComplete messages: %s", got, tags(messages))
	}
	if got := countTag(messages, msgReadyForQuery); got != 1 {
		t.Fatalf("ReadyForQuery count = %d, want 1: %s", got, tags(messages))
	}
	fields := expectError(t, messages, code)
	assertReadyStatus(t, messages, wantStatus)
	return fields
}

func TestPGWireCorrelatedExistsSimpleExtendedPreparedReuse(t *testing.T) {
	c := connectSQLCatalog(t)
	seedCorrelatedExistsWire(t, c)

	simple := c.query(strings.ReplaceAll(correlatedExistsWireSQL, "$1", "TRUE"))
	requireCorrelatedExistsWireCycle(t, c, simple, statusIdle)
	if got, want := correlatedExistsWireIDs(t, simple), []string{`"a_dup"`, `"b_filtered"`}; !slices.Equal(got, want) {
		t.Fatalf("simple EXISTS rows = %q, want %q", got, want)
	}
	if got := commandTagOf(t, simple); got != "SELECT 2" {
		t.Fatalf("simple command tag = %q, want SELECT 2", got)
	}

	c.send(msgParse, parseMsg("correlated-exists", correlatedExistsWireSQL))
	c.send(msgSync, nil)
	parsed := c.until(msgReadyForQuery)
	requireCorrelatedExistsWireCycle(t, c, parsed, statusIdle)
	if !has(parsed, msgParseComplete) {
		t.Fatalf("named correlated EXISTS was not prepared: %s", tags(parsed))
	}
	for i, run := range []struct {
		value string
		want  []string
	}{
		{"true", []string{`"a_dup"`, `"b_filtered"`}},
		{"false", []string{`"c_local_reject"`}},
		{"true", []string{`"a_dup"`, `"b_filtered"`}},
	} {
		messages := executeCorrelatedExistsWirePrepared(c, "correlated-exists",
			"correlated-exists-portal-"+strconv.Itoa(i), [][]byte{[]byte(run.value)})
		requireCorrelatedExistsWireCycle(t, c, messages, statusIdle)
		if got := correlatedExistsWireIDs(t, messages); !slices.Equal(got, run.want) {
			t.Fatalf("extended active=%s rows = %q, want %q", run.value, got, run.want)
		}
		if got := commandTagOf(t, messages); got != "SELECT "+strconv.Itoa(len(run.want)) {
			t.Fatalf("extended active=%s tag = %q", run.value, got)
		}
	}

	reused := c.query(`SELECT id FROM ce_wire_outer WHERE id = 'a_dup'`)
	requireCorrelatedExistsWireCycle(t, c, reused, statusIdle)
	if got := correlatedExistsWireIDs(t, reused); !slices.Equal(got, []string{`"a_dup"`}) {
		t.Fatalf("session reuse rows = %q", got)
	}
}

func TestPGWireCorrelatedNotExistsNullMissingDuplicatesAndEmpty(t *testing.T) {
	c := connectSQLCatalog(t)
	seedCorrelatedExistsWire(t, c)
	tests := []struct {
		name      string
		statement string
		want      []string
	}{
		{
			name: "anti join",
			statement: `SELECT o.id FROM ce_wire_outer AS o WHERE NOT EXISTS (` +
				`SELECT 1 FROM ce_wire_inner AS i ` +
				`WHERE i.match_key = o.match_key AND i.active = TRUE) ORDER BY o.id`,
			want: []string{`"c_local_reject"`, `"d_null"`, `"e_missing"`, `"f_empty"`},
		},
		{
			name: "empty child",
			statement: `SELECT o.id FROM ce_wire_outer AS o WHERE NOT EXISTS (` +
				`SELECT 1 FROM ce_wire_inner AS i ` +
				`WHERE i.match_key = o.match_key AND i.match_key = 'absent') ORDER BY o.id`,
			want: []string{
				`"a_dup"`, `"b_filtered"`, `"c_local_reject"`,
				`"d_null"`, `"e_missing"`, `"f_empty"`,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			messages := c.query(test.statement)
			requireCorrelatedExistsWireCycle(t, c, messages, statusIdle)
			if got := correlatedExistsWireIDs(t, messages); !slices.Equal(got, test.want) {
				t.Fatalf("rows = %q, want %q", got, test.want)
			}
		})
	}

	extended := extendedSQL(c, `
		SELECT o.id FROM ce_wire_outer AS o WHERE NOT EXISTS (
			SELECT 1 FROM ce_wire_inner AS i
			WHERE i.match_key = o.match_key AND i.active = $1
		) ORDER BY o.id`, [][]byte{[]byte("true")})
	requireCorrelatedExistsWireCycle(t, c, extended, statusIdle)
	if got, want := correlatedExistsWireIDs(t, extended),
		[]string{`"c_local_reject"`, `"d_null"`, `"e_missing"`, `"f_empty"`}; !slices.Equal(got, want) {
		t.Fatalf("extended NOT EXISTS rows = %q, want %q", got, want)
	}
}

func TestPGWireCorrelatedExistsTransactionSnapshotAndReadYourWrites(t *testing.T) {
	c, server := connectSQLCatalogWithServer(t)
	seedCorrelatedExistsWire(t, c)
	outside := dial(t, server)
	outside.startup(map[string]string{"user": "outside", "database": "app"})

	begin := c.query("BEGIN ISOLATION LEVEL REPEATABLE READ")
	requireCorrelatedExistsWireCycle(t, c, begin, statusInTx)
	for _, statement := range []string{
		`INSERT INTO ce_wire_outer VALUES (` +
			`'{"id":"outside_outer","match_key":"x"}')`,
		`INSERT INTO ce_wire_inner VALUES (` +
			`'{"id":"outside_inner","match_key":"none","active":true}')`,
	} {
		messages := outside.query(statement)
		requireCorrelatedExistsWireCycle(t, outside, messages, statusIdle)
	}

	query := strings.ReplaceAll(correlatedExistsWireSQL, "$1", "TRUE")
	messages := c.query(query)
	requireCorrelatedExistsWireCycle(t, c, messages, statusInTx)
	if got, want := correlatedExistsWireIDs(t, messages), []string{`"a_dup"`, `"b_filtered"`}; !slices.Equal(got, want) {
		t.Fatalf("transaction snapshot rows = %q, want %q", got, want)
	}
	staged := extendedSQL(c, `INSERT INTO ce_wire_inner VALUES ($1)`,
		[][]byte{[]byte(`{"id":"pending","match_key":"none","active":true}`)})
	requireCorrelatedExistsWireCycle(t, c, staged, statusInTx)
	messages = c.query(query)
	requireCorrelatedExistsWireCycle(t, c, messages, statusInTx)
	if got, want := correlatedExistsWireIDs(t, messages),
		[]string{`"a_dup"`, `"b_filtered"`, `"f_empty"`}; !slices.Equal(got, want) {
		t.Fatalf("transaction read-your-writes rows = %q, want %q", got, want)
	}
	rollback := c.query("ROLLBACK")
	requireCorrelatedExistsWireCycle(t, c, rollback, statusIdle)

	messages = c.query(query)
	requireCorrelatedExistsWireCycle(t, c, messages, statusIdle)
	if got, want := correlatedExistsWireIDs(t, messages),
		[]string{`"a_dup"`, `"b_filtered"`, `"f_empty"`, `"outside_outer"`}; !slices.Equal(got, want) {
		t.Fatalf("post-rollback rows = %q, want %q", got, want)
	}
}

func TestPGWireCorrelatedExistsDroppedDependencyAndRefusalRecovery(t *testing.T) {
	t.Run("dropped prepared dependency", func(t *testing.T) {
		c := connectSQLCatalog(t)
		seedCorrelatedExistsWire(t, c)
		dropSQL := "/* préfix */" + correlatedExistsWireSQL
		c.send(msgParse, parseMsg("correlated-drop", dropSQL))
		c.send(msgSync, nil)
		requireCorrelatedExistsWireCycle(t, c, c.until(msgReadyForQuery), statusIdle)
		initial := executeCorrelatedExistsWirePrepared(c, "correlated-drop", "before-drop",
			[][]byte{[]byte("true")})
		requireCorrelatedExistsWireCycle(t, c, initial, statusIdle)
		if got, want := correlatedExistsWireIDs(t, initial),
			[]string{`"a_dup"`, `"b_filtered"`}; !slices.Equal(got, want) {
			t.Fatalf("pre-drop rows = %q, want %q", got, want)
		}
		requireCorrelatedExistsWireCycle(
			t, c, c.query(`DROP TABLE ce_wire_inner`), statusIdle,
		)

		failure := executeCorrelatedExistsWirePrepared(c, "correlated-drop", "after-drop",
			[][]byte{[]byte("true")})
		fields := requireCorrelatedExistsWireFailure(
			t, failure, sqlstateUndefinedTable, statusIdle,
		)
		bytePosition := strings.Index(dropSQL, "ce_wire_inner")
		wantPosition := utf8.RuneCountInString(dropSQL[:bytePosition]) + 1
		if fields['P'] != strconv.Itoa(wantPosition) {
			t.Fatalf("dropped dependency position = %q, want %d", fields['P'], wantPosition)
		}
		requireCorrelatedExistsWireSentinel(t, c, statusIdle)
		reused := c.query(`SELECT id FROM ce_wire_outer WHERE id = 'a_dup'`)
		requireCorrelatedExistsWireCycle(t, c, reused, statusIdle)
		if got, want := correlatedExistsWireIDs(t, reused), []string{`"a_dup"`}; !slices.Equal(got, want) {
			t.Fatalf("post-drop reuse rows = %q, want %q", got, want)
		}
		if got := commandTagOf(t, reused); got != "SELECT 1" {
			t.Fatalf("post-drop reuse command tag = %q, want SELECT 1", got)
		}
	})

	t.Run("positioned simple and extended refusals are reusable", func(t *testing.T) {
		c := connectSQLCatalog(t)
		seedCorrelatedExistsWire(t, c)
		tests := []struct {
			name     string
			source   string
			marker   string
			last     bool
			extended bool
		}{
			{
				name: "OR simple",
				source: `/* préfix */ SELECT o.id FROM ce_wire_outer AS o ` +
					`WHERE o.id = 'a_dup' OR EXISTS (` +
					`SELECT 1 FROM ce_wire_inner AS i WHERE i.match_key = o.match_key)`,
				marker: "EXISTS",
			},
			{
				name: "nested simple",
				source: `/* préfix */ SELECT o.id FROM ce_wire_outer AS o WHERE EXISTS (` +
					`SELECT 1 FROM ce_wire_inner AS i WHERE i.match_key = o.match_key AND EXISTS (` +
					`SELECT 1 FROM ce_wire_outer AS n WHERE n.id = i.id))`,
				marker: "EXISTS",
				last:   true,
			},
			{
				name: "JOIN ON extended",
				source: `/* préfix */ SELECT o.id FROM ce_wire_outer AS o ` +
					`JOIN ce_wire_inner AS i ON EXISTS (` +
					`SELECT 1 FROM ce_wire_outer AS n WHERE n.id = o.id)`,
				marker:   "EXISTS",
				extended: true,
			},
		}
		for i, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				var failure []backendMessage
				if test.extended {
					c.send(msgParse, parseMsg("unsupported-"+strconv.Itoa(i), test.source))
					c.send(msgSync, nil)
					failure = c.until(msgReadyForQuery)
				} else {
					failure = c.query(test.source)
				}
				fields := requireCorrelatedExistsWireFailure(
					t, failure, sqlstateFeatureNotSupported, statusIdle,
				)
				bytePosition := strings.Index(test.source, test.marker)
				if test.last {
					bytePosition = strings.LastIndex(test.source, test.marker)
				}
				wantPosition := utf8.RuneCountInString(test.source[:bytePosition]) + 1
				if fields['P'] != strconv.Itoa(wantPosition) {
					t.Fatalf("position = %q, want %d at %q", fields['P'], wantPosition, test.marker)
				}
				requireCorrelatedExistsWireSentinel(t, c, statusIdle)
			})
		}

		reused := c.query(strings.ReplaceAll(correlatedExistsWireSQL, "$1", "TRUE"))
		requireCorrelatedExistsWireCycle(t, c, reused, statusIdle)
		if got := correlatedExistsWireIDs(t, reused); len(got) != 2 {
			t.Fatalf("session reuse rows = %q, want two", got)
		}
	})

	t.Run("extended refusal marks transaction failed and recovers", func(t *testing.T) {
		c := connectSQLCatalog(t)
		seedCorrelatedExistsWire(t, c)
		begin := c.query("BEGIN")
		requireCorrelatedExistsWireCycle(t, c, begin, statusInTx)
		unsupported := `/* préfix */ SELECT o.id FROM ce_wire_outer AS o WHERE o.enabled = TRUE OR EXISTS (` +
			`SELECT 1 FROM ce_wire_inner AS i WHERE i.match_key = o.match_key)`
		c.send(msgParse, parseMsg("unsupported-in-transaction", unsupported))
		c.send(msgSync, nil)
		failure := c.until(msgReadyForQuery)
		fields := requireCorrelatedExistsWireFailure(
			t, failure, sqlstateFeatureNotSupported, statusFailedT,
		)
		bytePosition := strings.Index(unsupported, "EXISTS")
		wantPosition := utf8.RuneCountInString(unsupported[:bytePosition]) + 1
		if fields['P'] != strconv.Itoa(wantPosition) {
			t.Fatalf("failed-transaction position = %q, want %d", fields['P'], wantPosition)
		}

		rollback := c.query("ROLLBACK")
		requireCorrelatedExistsWireCycle(t, c, rollback, statusIdle)
		if got := commandTagOf(t, rollback); got != "ROLLBACK" {
			t.Fatalf("failed-transaction recovery tag = %q, want ROLLBACK", got)
		}
		reused := c.query(strings.ReplaceAll(correlatedExistsWireSQL, "$1", "TRUE"))
		requireCorrelatedExistsWireCycle(t, c, reused, statusIdle)
		if got, want := correlatedExistsWireIDs(t, reused),
			[]string{`"a_dup"`, `"b_filtered"`}; !slices.Equal(got, want) {
			t.Fatalf("post-rollback reuse rows = %q, want %q", got, want)
		}
	})
}

func TestPGWireCorrelatedExistsContainerKeysUseCanonicalStoredJSONIdentity(t *testing.T) {
	c := connectSQLCatalog(t)
	for _, statement := range []string{
		`CREATE TABLE ce_wire_value_outer (id STRING PRIMARY KEY, match_key ANY)`,
		`CREATE INDEX ce_wire_value_outer_match ON ce_wire_value_outer (match_key)`,
		`CREATE TABLE ce_wire_value_inner (id STRING PRIMARY KEY, match_key ANY)`,
		`INSERT INTO ce_wire_value_outer VALUES ` +
			`('{"id":"a_array","match_key":[1,2]}'),` +
			`('{"id":"b_object","match_key":{"a":1,"b":2}}'),` +
			`('{"id":"c_object_order","match_key":{"b":2,"a":1}}'),` +
			`('{"id":"d_scalar","match_key":"scalar"}'),` +
			`('{"id":"e_array_order","match_key":[2,1]}'),` +
			`('{"id":"f_object_value","match_key":{"a":1,"b":3}}'),` +
			`('{"id":"g_object_member","match_key":{"a":1,"c":2}}'),` +
			`('{"id":"h_missing"}')`,
		`INSERT INTO ce_wire_value_inner VALUES ` +
			`('{"id":"i_scalar","match_key":"scalar"}'),` +
			`('{"id":"i_array","match_key":[1,2]}'),` +
			`('{"id":"i_object","match_key":{"a":1,"b":2}}'),` +
			`('{"id":"i_object_duplicate","match_key":{"a":1,"b":2}}')`,
	} {
		requireCorrelatedExistsWireCycle(t, c, c.query(statement), statusIdle)
	}
	exists := `SELECT o.id FROM ce_wire_value_outer AS o WHERE EXISTS (` +
		`SELECT 1 FROM ce_wire_value_inner AS i WHERE i.match_key = o.match_key) ORDER BY o.id`
	messages := c.query(exists)
	requireCorrelatedExistsWireCycle(t, c, messages, statusIdle)
	if got, want := correlatedExistsWireIDs(t, messages),
		[]string{`"a_array"`, `"b_object"`, `"c_object_order"`, `"d_scalar"`}; !slices.Equal(got, want) {
		t.Fatalf("simple container EXISTS rows = %q, want %q", got, want)
	}

	begin := c.query("BEGIN")
	requireCorrelatedExistsWireCycle(t, c, begin, statusInTx)
	messages = extendedSQL(c, exists, nil)
	requireCorrelatedExistsWireCycle(t, c, messages, statusInTx)
	if got, want := correlatedExistsWireIDs(t, messages),
		[]string{`"a_array"`, `"b_object"`, `"c_object_order"`, `"d_scalar"`}; !slices.Equal(got, want) {
		t.Fatalf("extended container EXISTS rows = %q, want %q", got, want)
	}
	rollback := c.query("ROLLBACK")
	requireCorrelatedExistsWireCycle(t, c, rollback, statusIdle)

	anti := `SELECT o.id FROM ce_wire_value_outer AS o WHERE NOT EXISTS (` +
		`SELECT 1 FROM ce_wire_value_inner AS i WHERE i.match_key = o.match_key) ORDER BY o.id`
	messages = c.query(anti)
	requireCorrelatedExistsWireCycle(t, c, messages, statusIdle)
	if got, want := correlatedExistsWireIDs(t, messages),
		[]string{`"e_array_order"`, `"f_object_value"`, `"g_object_member"`, `"h_missing"`}; !slices.Equal(got, want) {
		t.Fatalf("container NOT EXISTS rows = %q, want %q", got, want)
	}

	reused := c.query(`SELECT id FROM ce_wire_value_outer WHERE id = 'a_array'`)
	requireCorrelatedExistsWireCycle(t, c, reused, statusIdle)
	if got := correlatedExistsWireIDs(t, reused); !slices.Equal(got, []string{`"a_array"`}) {
		t.Fatalf("container session reuse rows = %q", got)
	}
}
