package pgwire

import "testing"

func queryTextRows(t *testing.T, c *testClient, sql string) [][]string {
	t.Helper()
	raw := rowsOf(t, requireSelect(t, c, sql))
	out := make([][]string, len(raw))
	for i, row := range raw {
		out[i] = make([]string, len(row))
		for j, cell := range row {
			out[i][j] = string(cell)
		}
	}
	return out
}

func TestSequenceAndIdentityOverPGWire(t *testing.T) {
	c := connectSQLCatalog(t)

	// CREATE SEQUENCE / nextval / currval / setval / lastval / ALTER / DROP.
	createMsgs := requireQueryReady(t, c, `CREATE SEQUENCE pg_seq START WITH 10 INCREMENT BY 2 MAXVALUE 14 NO CYCLE`)
	if got := commandTagOf(t, createMsgs); got != "CREATE SEQUENCE" {
		t.Fatalf("CREATE SEQUENCE tag = %q, want CREATE SEQUENCE", got)
	}

	// currval / lastval before nextval -> SQLSTATE 55000.
	expectError(t, c.query(`SELECT currval('pg_seq')`), sqlstateObjectNotInPrereqState)
	expectError(t, c.query(`SELECT lastval()`), sqlstateObjectNotInPrereqState)

	if rows := queryTextRows(t, c, `SELECT nextval('pg_seq')`); len(rows) != 1 || rows[0][0] != "10" {
		t.Fatalf("nextval[1] = %v, want [[10]]", rows)
	}
	if rows := queryTextRows(t, c, `SELECT currval('pg_seq'), lastval()`); len(rows) != 1 || rows[0][0] != "10" || rows[0][1] != "10" {
		t.Fatalf("currval/lastval = %v, want [[10 10]]", rows)
	}
	if rows := queryTextRows(t, c, `SELECT nextval('pg_seq')`); len(rows) != 1 || rows[0][0] != "12" {
		t.Fatalf("nextval[2] = %v, want [[12]]", rows)
	}
	if rows := queryTextRows(t, c, `SELECT nextval('pg_seq')`); len(rows) != 1 || rows[0][0] != "14" {
		t.Fatalf("nextval[3] = %v, want [[14]]", rows)
	}
	// Exhausted -> SQLSTATE 2200H.
	expectError(t, c.query(`SELECT nextval('pg_seq')`), sqlstateSequenceGeneratorLimitExceeded)

	// ALTER SEQUENCE + RESTART.
	alterMsgs := requireQueryReady(t, c, `ALTER SEQUENCE pg_seq MAXVALUE 100 RESTART WITH 20`)
	if got := commandTagOf(t, alterMsgs); got != "ALTER SEQUENCE" {
		t.Fatalf("ALTER SEQUENCE tag = %q, want ALTER SEQUENCE", got)
	}
	if rows := queryTextRows(t, c, `SELECT nextval('pg_seq')`); len(rows) != 1 || rows[0][0] != "20" {
		t.Fatalf("nextval after RESTART = %v, want [[20]]", rows)
	}
	if rows := queryTextRows(t, c, `SELECT setval('pg_seq', 50, false)`); len(rows) != 1 || rows[0][0] != "50" {
		t.Fatalf("setval(50, false) = %v, want [[50]]", rows)
	}
	if rows := queryTextRows(t, c, `SELECT nextval('pg_seq')`); len(rows) != 1 || rows[0][0] != "50" {
		t.Fatalf("nextval after setval(50, false) = %v, want [[50]]", rows)
	}

	dropMsgs := requireQueryReady(t, c, `DROP SEQUENCE pg_seq`)
	if got := commandTagOf(t, dropMsgs); got != "DROP SEQUENCE" {
		t.Fatalf("DROP SEQUENCE tag = %q, want DROP SEQUENCE", got)
	}

	// SERIAL + INSERT RETURNING + DROP TABLE cascade.
	requireWireOK(t, c.query(`CREATE TABLE pg_serial_tbl (id BIGSERIAL PRIMARY KEY, label TEXT)`))
	if rows := queryTextRows(t, c, `INSERT INTO pg_serial_tbl (label) VALUES ('a'), ('b') RETURNING id, label`); len(rows) != 2 || rows[0][0] != "1" || rows[1][0] != "2" {
		t.Fatalf("SERIAL INSERT RETURNING = %v, want ids 1, 2", rows)
	}
	requireWireOK(t, c.query(`DROP TABLE pg_serial_tbl`))
	expectError(t, c.query(`SELECT nextval('pg_serial_tbl_id_seq')`), sqlstateUndefinedTable)

	// GENERATED ALWAYS AS IDENTITY -> SQLSTATE 428C9 without OVERRIDING SYSTEM VALUE.
	requireWireOK(t, c.query(`CREATE TABLE pg_id_tbl (id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 100 INCREMENT BY 5) PRIMARY KEY, name TEXT)`))
	expectError(t, c.query(`INSERT INTO pg_id_tbl (id, name) VALUES (1, 'bad')`), sqlstateGeneratedAlways)
	if rows := queryTextRows(t, c, `INSERT INTO pg_id_tbl (name) VALUES ('auto') RETURNING id`); len(rows) != 1 || rows[0][0] != "100" {
		t.Fatalf("IDENTITY auto RETURNING = %v, want [[100]]", rows)
	}
	if rows := queryTextRows(t, c, `INSERT INTO pg_id_tbl (id, name) OVERRIDING SYSTEM VALUE VALUES (777, 'sys') RETURNING id`); len(rows) != 1 || rows[0][0] != "777" {
		t.Fatalf("IDENTITY OVERRIDING SYSTEM VALUE = %v, want [[777]]", rows)
	}
	if rows := queryTextRows(t, c, `INSERT INTO pg_id_tbl (id, name) OVERRIDING USER VALUE VALUES (888, 'usr') RETURNING id`); len(rows) != 1 || rows[0][0] != "105" {
		t.Fatalf("IDENTITY OVERRIDING USER VALUE = %v, want [[105]]", rows)
	}
}
