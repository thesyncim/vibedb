package pgwire

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	sqlast "github.com/thesyncim/vibedb/sql"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

func TestSessionCatalogOIDsAreUniqueAndStableAcrossConcurrentCreate(t *testing.T) {
	tables := make([]sqldriver.TableInfo, 128)
	for i := range tables {
		tables[i].Name = fmt.Sprintf("table_%03d", i)
	}
	var s session
	if !s.ensureCatalogOIDs(tables) || len(s.catalogOIDs) != len(tables) {
		t.Fatalf("initial oid assignment = %d entries", len(s.catalogOIDs))
	}
	seen := make(map[uint32]string, len(tables))
	stable := make(map[string]uint32, len(tables))
	for _, entry := range s.catalogOIDs {
		if previous, exists := seen[entry.oid]; exists {
			t.Fatalf("oid %d assigned to both %q and %q", entry.oid, previous, entry.name)
		}
		seen[entry.oid] = entry.name
		stable[entry.name] = entry.oid
	}

	// A lexically earlier table appears between psql's resolve and detail
	// queries. Existing continuation tokens must not move.
	withEarlier := append([]sqldriver.TableInfo{{Name: "aaa_new"}}, tables...)
	if !s.ensureCatalogOIDs(withEarlier) {
		t.Fatal("oid assignment rejected a bounded catalog")
	}
	for _, entry := range s.catalogOIDs {
		if old, existed := stable[entry.name]; existed && old != entry.oid {
			t.Fatalf("oid for %q moved from %d to %d", entry.name, old, entry.oid)
		}
	}
	answer := catalogAnswer{tables: withEarlier, oids: s.catalogOIDs}
	for name, oid := range stable {
		table := answer.tableByOID(strconv.FormatUint(uint64(oid), 10))
		if table == nil || table.Name != name {
			t.Fatalf("oid %d resolved to %+v, want %q", oid, table, name)
		}
	}
	if answer.tableByOID("4294967295") != nil {
		t.Fatal("unknown oid resolved to a table")
	}
	absent := catalogAnswer{tables: withEarlier[:1], oids: s.catalogOIDs}
	if absent.tableByOID(strconv.FormatUint(uint64(stable["table_000"]), 10)) != nil {
		t.Fatal("oid for a table absent from the current snapshot returned stale metadata")
	}
}

// The catalog shim's contract has three parts, and each part has a test that
// would fail if it broke: the exact psql query texts are answered with the
// rows psql needs (shape by shape, values and declared types both); anything
// that is not exactly one of those texts keeps the SQL front end's original
// error byte for byte; and the successful-query path is provably unaware the
// shim exists, measured by allocation count with the recognition table
// present and removed.

// catalogQueryFor rebuilds the verbatim wire text of one recognized shape,
// substituting capture for each variable span. Tests build psql's texts this
// way so a template edit and its test change together; the \dt text is also
// pinned literally below so the table cannot drift wholesale unnoticed.
func catalogQueryFor(t *testing.T, command, capture string) string {
	t.Helper()
	for i := range catalogShapes {
		if catalogShapes[i].command == command {
			return strings.Join(catalogShapes[i].segments, capture)
		}
	}
	t.Fatalf("no recognized catalog shape is registered for %q", command)
	return ""
}

// pinnedListTablesQuery is the exact SQL psql 18.4 sends for \dt against a
// version-16 server, duplicated here on purpose: the recognition table must
// keep matching what psql sends, not what the table was edited to say.
const pinnedListTablesQuery = `SELECT n.nspname as "Schema",
  c.relname as "Name",
  CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 't' THEN 'TOAST table' WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'partitioned table' WHEN 'I' THEN 'partitioned index' END as "Type",
  pg_catalog.pg_get_userbyid(c.relowner) as "Owner"
FROM pg_catalog.pg_class c
     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
     LEFT JOIN pg_catalog.pg_am am ON am.oid = c.relam
WHERE c.relkind IN ('r','p','')
      AND n.nspname <> 'pg_catalog'
      AND n.nspname !~ '^pg_toast'
      AND n.nspname <> 'information_schema'
  AND pg_catalog.pg_table_is_visible(c.oid)
ORDER BY 1,2`

// connectIntrospectionCatalog opens a SQL-catalog connection holding the
// schema the psql capture was made over: two tables, a primary key on each,
// and one secondary exact index.
func connectIntrospectionCatalog(t *testing.T) *testClient {
	t.Helper()
	c := connectSQLCatalog(t)
	for _, ddl := range []string{
		`CREATE TABLE users (id STRING PRIMARY KEY, name STRING NOT NULL, tier STRING, n INTEGER)`,
		`CREATE INDEX users_by_tier ON users(tier)`,
		`CREATE TABLE orders (id STRING PRIMARY KEY, user_id STRING NOT NULL, total NUMBER NOT NULL)`,
	} {
		if msgs := c.query(ddl); has(msgs, msgErrorResponse) {
			t.Fatalf("%s: %s", ddl, formatError(find(t, msgs, msgErrorResponse).body))
		}
	}
	return c
}

// expectShimResult asserts one recognized query's full wire answer: the
// RowDescription's names and declared type OIDs, every row's values (nil is
// SQL NULL), and PostgreSQL's SELECT tag with the row count.
func expectShimResult(t *testing.T, msgs []backendMessage,
	wantNames []string, wantOIDs []int32, wantRows [][]any) {
	t.Helper()
	cols := decodeRowDescription(t, find(t, msgs, msgRowDescription).body)
	if len(cols) != len(wantNames) {
		t.Fatalf("RowDescription has %d columns, want %d: %+v", len(cols), len(wantNames), cols)
	}
	for i := range cols {
		if cols[i].name != wantNames[i] {
			t.Errorf("column %d is named %q, want %q", i, cols[i].name, wantNames[i])
		}
		if cols[i].oid != wantOIDs[i] {
			t.Errorf("column %q declares OID %d, want %d", cols[i].name, cols[i].oid, wantOIDs[i])
		}
		if cols[i].format != formatText {
			t.Errorf("column %q declares format %d, want text", cols[i].name, cols[i].format)
		}
	}
	// Decoded locally rather than with rowsOf because this assertion needs
	// the distinction rowsOf erases: a NULL (length -1) and an empty string
	// (length 0) are different wire values, and the shim emits both.
	var rows [][]*string
	for _, m := range msgs {
		if m.tag != msgDataRow {
			continue
		}
		f := fields{b: m.body}
		n := f.int16()
		row := make([]*string, 0, n)
		for range n {
			size := f.int32()
			if size == -1 {
				row = append(row, nil)
				continue
			}
			row = append(row, strPtr(string(f.slice(int(size)))))
		}
		if err := f.end(); err != nil {
			t.Fatalf("malformed DataRow: %v", err)
		}
		rows = append(rows, row)
	}
	if len(rows) != len(wantRows) {
		t.Fatalf("got %d rows, want %d", len(rows), len(wantRows))
	}
	for r, want := range wantRows {
		if len(rows[r]) != len(want) {
			t.Fatalf("row %d has %d values, want %d", r, len(rows[r]), len(want))
		}
		for i, cell := range want {
			switch expected := cell.(type) {
			case nil:
				if rows[r][i] != nil {
					t.Errorf("row %d column %d = %q, want NULL", r, i, *rows[r][i])
				}
			case string:
				if rows[r][i] == nil {
					t.Errorf("row %d column %d is NULL, want %q", r, i, expected)
				} else if *rows[r][i] != expected {
					t.Errorf("row %d column %d = %q, want %q", r, i, *rows[r][i], expected)
				}
			default:
				t.Fatalf("row %d column %d: unsupported expectation %T", r, i, cell)
			}
		}
	}
	if got, want := commandTagOf(t, msgs), "SELECT "+strconv.Itoa(len(wantRows)); got != want {
		t.Errorf("command tag = %q, want %q", got, want)
	}
	assertReadyStatus(t, msgs, statusIdle)
}

func TestPSQLListMetaCommandsAnswerFromCatalog(t *testing.T) {
	c := connectIntrospectionCatalog(t)
	text := func(command string) string { return catalogQueryFor(t, command, "") }
	textOIDs := func(n int) []int32 {
		oids := make([]int32, n)
		for i := range oids {
			oids[i] = oidText
		}
		return oids
	}

	// \dt is sent as the pinned literal, not rebuilt from the table.
	expectShimResult(t, c.query(pinnedListTablesQuery),
		[]string{"Schema", "Name", "Type", "Owner"}, textOIDs(4),
		[][]any{
			{"public", "orders", "table", "tester"},
			{"public", "users", "table", "tester"},
		})

	expectShimResult(t, c.query(text(`\l`)),
		[]string{"Name", "Owner", "Encoding", "Locale Provider", "Collate",
			"Ctype", "Locale", "ICU Rules", "Access privileges"},
		textOIDs(9),
		[][]any{{"app", "tester", "UTF8", nil, nil, nil, nil, nil, nil}})

	expectShimResult(t, c.query(text(`\dn`)),
		[]string{"Name", "Owner"}, textOIDs(2),
		[][]any{{"public", "tester"}})

	expectShimResult(t, c.query(text(`\d`)),
		[]string{"Schema", "Name", "Type", "Owner"}, textOIDs(4),
		[][]any{
			{"public", "orders", "table", "tester"},
			{"public", "users", "table", "tester"},
		})

	expectShimResult(t, c.query(text(`\di`)),
		[]string{"Schema", "Name", "Type", "Owner", "Table"}, textOIDs(5),
		[][]any{
			{"public", "orders_pkey", "index", "tester", "orders"},
			{"public", "users_by_tier", "index", "tester", "users"},
			{"public", "users_pkey", "index", "tester", "users"},
		})

	// The degrade-gracefully set: recognized, honestly empty, never invented.
	expectShimResult(t, c.query(text(`\dv`)),
		[]string{"Schema", "Name", "Type", "Owner"}, textOIDs(4), nil)
	expectShimResult(t, c.query(text(`\df`)),
		[]string{"Schema", "Name", "Result data type", "Argument data types", "Type"},
		textOIDs(5), nil)
	expectShimResult(t, c.query(text(`\du`)),
		[]string{"rolname", "rolsuper", "rolinherit", "rolcreaterole", "rolcreatedb",
			"rolcanlogin", "rolconnlimit", "rolvaliduntil", "rolreplication", "rolbypassrls"},
		[]int32{oidText, oidBool, oidBool, oidBool, oidBool, oidBool,
			oidInt8, oidText, oidBool, oidBool},
		nil)
}

func TestPSQLDescribeTableSequenceAnswersFromCatalog(t *testing.T) {
	c := connectIntrospectionCatalog(t)
	resolve := catalogQueryFor(t, `\d name: resolve oid`, "users")
	resolvedRows := rowsOf(t, c.query(resolve))
	if len(resolvedRows) != 1 || len(resolvedRows[0]) != 3 {
		t.Fatalf("oid resolution rows = %q", resolvedRows)
	}
	oid := string(resolvedRows[0][0])

	// Step 1: psql resolves the name to an oid.
	expectShimResult(t, c.query(resolve),
		[]string{"oid", "nspname", "relname"},
		[]int32{oidInt8, oidText, oidText},
		[][]any{{oid, "public", "users"}})

	// Step 2: the pg_class row for that oid.
	expectShimResult(t, c.query(catalogQueryFor(t, `\d name: pg_class row`, oid)),
		[]string{"relchecks", "relkind", "relhasindex", "relhasrules", "relhastriggers",
			"relrowsecurity", "relforcerowsecurity", "relhasoids", "relispartition",
			"reloptions", "reltablespace", "reloftype", "relpersistence", "relreplident",
			"amname"},
		[]int32{oidInt8, oidText, oidBool, oidBool, oidBool, oidBool, oidBool,
			oidBool, oidBool, oidText, oidInt8, oidText, oidText, oidText, oidText},
		[][]any{{"0", "r", "t", "f", "f", "f", "f", "f", "f", "", "0", "", "p", "d", nil}})

	// Step 3: the pg_attribute rows, in the catalog's canonical path-sorted
	// order, with the dialect's own type vocabulary and the catalog's real
	// nullability.
	expectShimResult(t, c.query(catalogQueryFor(t, `\d name: pg_attribute rows`, oid)),
		[]string{"attname", "format_type", "default", "attnotnull", "attcollation",
			"attidentity", "attgenerated"},
		[]int32{oidText, oidText, oidText, oidBool, oidText, oidText, oidText},
		[][]any{
			{"id", "string", nil, "t", nil, "", ""},
			{"n", "integer", nil, "f", nil, "", ""},
			{"name", "string", nil, "t", nil, "", ""},
			{"tier", "string", nil, "f", nil, "", ""},
		})

	// Step 4: the index list, primary key first, definitions spelled as the
	// exact indexes they are.
	expectShimResult(t, c.query(catalogQueryFor(t, `\d name: index list`, oid)),
		[]string{"relname", "indisprimary", "indisunique", "indisclustered", "indisvalid",
			"pg_get_indexdef", "pg_get_constraintdef", "contype", "condeferrable",
			"condeferred", "indisreplident", "reltablespace", "conperiod"},
		[]int32{oidText, oidBool, oidBool, oidBool, oidBool, oidText, oidText, oidText,
			oidBool, oidBool, oidBool, oidInt8, oidBool},
		[][]any{
			{"users_pkey", "t", "t", "f", "t",
				"CREATE UNIQUE INDEX users_pkey ON public.users USING exact (id)",
				"PRIMARY KEY (id)", "p", "f", "f", "f", "0", "f"},
			{"users_by_tier", "f", "f", "f", "t",
				"CREATE INDEX users_by_tier ON public.users USING exact (tier)",
				nil, nil, nil, nil, "f", "0", nil},
		})

	// Steps 5..: the catalogs that do not exist here answer with their shape
	// and zero rows, so psql prints nothing instead of failing the \d.
	for _, command := range []string{
		`\d name: foreign keys`,
		`\d name: referencing foreign keys`,
		`\d name: row policies`,
		`\d name: extended statistics`,
		`\d name: publications`,
		`\d name: inheritance parents`,
		`\d name: partition children`,
	} {
		msgs := c.query(catalogQueryFor(t, command, oid))
		if has(msgs, msgErrorResponse) {
			t.Fatalf("%s errored: %s", command,
				formatError(find(t, msgs, msgErrorResponse).body))
		}
		if rows := rowsOf(t, msgs); len(rows) != 0 {
			t.Fatalf("%s fabricated rows: %q", command, rows)
		}
		if got := commandTagOf(t, msgs); got != "SELECT 0" {
			t.Fatalf("%s command tag = %q, want SELECT 0", command, got)
		}
		assertReadyStatus(t, msgs, statusIdle)
	}
}

// A \d of a missing table must resolve to zero rows without a server error:
// psql sees the empty resolution and prints its own "Did not find any
// relation named ..." diagnostic.
func TestPSQLDescribeMissingTableResolvesToZeroRows(t *testing.T) {
	c := connectIntrospectionCatalog(t)
	msgs := c.query(catalogQueryFor(t, `\d name: resolve oid`, "missing_table"))
	if has(msgs, msgErrorResponse) {
		t.Fatalf("missing-table resolution errored: %s",
			formatError(find(t, msgs, msgErrorResponse).body))
	}
	if rows := rowsOf(t, msgs); len(rows) != 0 {
		t.Fatalf("missing-table resolution returned rows: %q", rows)
	}
	if got := commandTagOf(t, msgs); got != "SELECT 0" {
		t.Fatalf("command tag = %q, want SELECT 0", got)
	}
	assertReadyStatus(t, msgs, statusIdle)

	// An unknown oid in the detail queries is the same contract.
	// The fixture owns two tables and therefore legitimately assigns 16384 and
	// 16385. Use the largest valid uint32 oid so this remains unknown as the
	// fixture grows without relying on the current table count.
	msgs = c.query(catalogQueryFor(t, `\d name: pg_class row`, "4294967295"))
	if has(msgs, msgErrorResponse) || len(rowsOf(t, msgs)) != 0 {
		t.Fatalf("unknown-oid detail query did not answer zero rows: %s", tags(msgs))
	}
}

// Anything that is not exactly a recognized psql text keeps the SQL front
// end's original refusal — every error field, including the position — and
// the protocol state that goes with it.
func TestUnrecognizedCatalogQueryKeepsOriginalParseError(t *testing.T) {
	nearMisses := []string{
		// A catalog-ish query psql never sends.
		`SELECT relname FROM pg_catalog.pg_class`,
		// The recognized \dt text truncated by one byte.
		pinnedListTablesQuery[:len(pinnedListTablesQuery)-1],
		// The recognized \dt text with one identifier changed.
		strings.Replace(pinnedListTablesQuery, "pg_class", "pg_klass", 1),
		// The recognized \dt text with trailing garbage.
		pinnedListTablesQuery + " LIMIT 1",
		// A resolve-oid pattern that is a real regex, not a plain name.
		strings.Join(findShape(t, `\d name: resolve oid`).segments, "users.*"),
		// A referenced-by query whose two oids disagree; psql always copies
		// one oid, so this is not a psql text.
		mismatchedOIDQuery(t),
	}

	collect := func(c *testClient) []map[byte]string {
		var out []map[byte]string
		for _, sql := range nearMisses {
			msgs := c.query(sql)
			m := find(t, msgs, msgErrorResponse)
			assertReadyStatus(t, msgs, statusIdle)
			out = append(out, errorFields(m.body))
		}
		return out
	}

	withShim := collect(connectIntrospectionCatalog(t))

	saved := catalogShapes
	catalogShapes = nil
	defer func() { catalogShapes = saved }()
	withoutShim := collect(connectIntrospectionCatalog(t))

	for i := range nearMisses {
		if len(withShim[i]) != len(withoutShim[i]) {
			t.Fatalf("query %d: error field counts differ with the shim present: %v vs %v",
				i, withShim[i], withoutShim[i])
		}
		for field, want := range withoutShim[i] {
			if got := withShim[i][field]; got != want {
				t.Errorf("query %d: error field %q = %q with the shim, %q without",
					i, field, got, want)
			}
		}
	}
}

func findShape(t *testing.T, command string) *catalogShape {
	t.Helper()
	for i := range catalogShapes {
		if catalogShapes[i].command == command {
			return &catalogShapes[i]
		}
	}
	t.Fatalf("no shape registered for %q", command)
	return nil
}

func mismatchedOIDQuery(t *testing.T) string {
	t.Helper()
	segments := findShape(t, `\d name: referencing foreign keys`).segments
	if len(segments) != 3 {
		t.Fatalf("the referencing-foreign-keys shape has %d segments, want 3", len(segments))
	}
	return segments[0] + "16385" + segments[1] + "16386" + segments[2]
}

// Ordinary statements before and after shim answers, on one connection, in
// both protocols: the shim must leave no protocol state behind.
func TestOrdinaryQueriesAreUnaffectedAroundShimAnswers(t *testing.T) {
	c := connectIntrospectionCatalog(t)
	c.query(`INSERT INTO users (id, name, tier, n) VALUES ('u1', 'Ada', 'pro', 7)`)

	before := c.query(`SELECT name FROM users WHERE id = 'u1'`)
	if rows := rowsOf(t, before); len(rows) != 1 || string(rows[0][0]) != `"Ada"` {
		t.Fatalf("SELECT before the shim answered %q", rows)
	}
	assertReadyStatus(t, before, statusIdle)

	expectShimResult(t, c.query(pinnedListTablesQuery),
		[]string{"Schema", "Name", "Type", "Owner"},
		[]int32{oidText, oidText, oidText, oidText},
		[][]any{
			{"public", "orders", "table", "tester"},
			{"public", "users", "table", "tester"},
		})

	after := c.query(`SELECT name FROM users WHERE id = 'u1'`)
	if rows := rowsOf(t, after); len(rows) != 1 || string(rows[0][0]) != `"Ada"` {
		t.Fatalf("SELECT after the shim answered %q", rows)
	}
	assertReadyStatus(t, after, statusIdle)

	// The extended protocol is likewise untouched after a shim answer.
	extended := extendedSQL(c, `SELECT name FROM users WHERE id = $1`,
		[][]byte{[]byte("u1")})
	if rows := rowsOf(t, extended); len(rows) != 1 || string(rows[0][0]) != `"Ada"` {
		t.Fatalf("extended SELECT after the shim answered %q", rows)
	}
	assertReadyStatus(t, extended, statusIdle)

	// Inside an explicit transaction the front end's refusal has already
	// failed the transaction before recognition could run, so the shim stands
	// aside: the original parse error is reported, the transaction is failed,
	// and ROLLBACK recovers — the same contract as without the shim.
	assertReadyStatus(t, c.query(`BEGIN`), statusInTx)
	inTx := c.query(pinnedListTablesQuery)
	if !has(inTx, msgErrorResponse) {
		t.Fatal("a shim answer inside an explicit transaction would contradict the failed state")
	}
	assertReadyStatus(t, inTx, statusFailedT)
	assertReadyStatus(t, c.query(`ROLLBACK`), statusIdle)
}

// The shim sits behind SCRAM exactly as every statement does: recognition is
// reachable only from session.prepare, which only the post-startup message
// loop calls, so an unauthenticated peer can never reach it. This test walks
// the whole path — SCRAM exchange, then a meta-command — to keep that true.
func TestCatalogShimAnswersOnSCRAMAuthenticatedConnection(t *testing.T) {
	database, err := sqldriver.Open(filepath.Join(t.TempDir(), "scram-shim.vdb"))
	if err != nil {
		t.Fatalf("open SQL catalog: %v", err)
	}
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("close SQL catalog: %v", err)
		}
	})
	verifier, err := NewVerifier("correct-horse")
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}
	server, err := NewServer(database, Options{
		Auth: SCRAM(func(name string) (Verifier, bool) {
			return verifier, name == "alice"
		}),
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	t.Cleanup(func() {
		if err := server.Close(); err != nil {
			t.Errorf("close server: %v", err)
		}
	})
	c := dial(t, server)
	sc := &scramClient{t: t, c: c, user: "alice", password: "correct-horse", gs2: "n"}
	if m := sc.authenticate(); m.tag != msgAuthentication {
		t.Fatalf("SCRAM authentication failed: %s", formatError(m.body))
	}
	for c.recv().tag != msgReadyForQuery {
	}

	if msgs := c.query(`CREATE TABLE docs (id STRING PRIMARY KEY)`); has(msgs, msgErrorResponse) {
		t.Fatalf("CREATE TABLE over SCRAM: %s", formatError(find(t, msgs, msgErrorResponse).body))
	}
	msgs := c.query(pinnedListTablesQuery)
	rows := rowsOf(t, msgs)
	if len(rows) != 1 || string(rows[0][1]) != "docs" {
		t.Fatalf("\\dt over SCRAM answered %q", rows)
	}
	assertReadyStatus(t, msgs, statusIdle)
}

// The zero-cost claim, measured rather than asserted: a successful simple
// query's allocation count is identical with the recognition table present
// and removed, because the only call site sits behind a parse failure. The
// same session, the same warmed statement path, the same measurement — only
// the shim's existence differs.
func TestCatalogShimPresenceDoesNotChangeSuccessfulQueryAllocations(t *testing.T) {
	database, err := sqldriver.Open(filepath.Join(t.TempDir(), "alloc-shim.vdb"))
	if err != nil {
		t.Fatalf("open SQL catalog: %v", err)
	}
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("close SQL catalog: %v", err)
		}
	})
	runtime, err := database.NewSession(context.Background())
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	s := &session{
		database:   "app",
		user:       "tester",
		params:     map[string]string{},
		statements: map[string]*prepared{},
		portals:    map[string]*portal{},
		sql:        runtime,
	}
	s.w = newWriter(io.Discard, 16<<10)
	t.Cleanup(s.release)

	mustQuery := func(sql string) {
		t.Helper()
		if err := s.simpleQuery(sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	mustQuery(`CREATE TABLE docs (id STRING PRIMARY KEY, name STRING NOT NULL)`)
	mustQuery(`INSERT INTO docs (id, name) VALUES ('a', 'Ada')`)

	run := func() {
		if err := s.simpleQuery(`SELECT name FROM docs WHERE id = 'a'`); err != nil {
			panic(err)
		}
	}
	run()
	run()
	withShim := testing.AllocsPerRun(200, run)

	saved := catalogShapes
	catalogShapes = nil
	defer func() { catalogShapes = saved }()
	withoutShim := testing.AllocsPerRun(200, run)

	t.Logf("successful simple query: %.2f allocs/op with the shim present, %.2f with it removed",
		withShim, withoutShim)
	if withShim != withoutShim {
		t.Fatalf("the shim's presence changed a successful query's allocations: %.2f vs %.2f",
			withShim, withoutShim)
	}
}

// FuzzCatalogShimRecognizer keeps the recognizer on the hostile-input footing
// the rest of the protocol surface lives on. Its seeds are every recognized
// text with benign captures plus the near-misses that must not match; the
// property is that recognition never panics, only ever succeeds with a
// validated capture, and every produced answer is encodable row by row.
func FuzzCatalogShimRecognizer(f *testing.F) {
	for i := range catalogShapes {
		capture := ""
		switch catalogShapes[i].capture {
		case captureRelName:
			capture = "users"
		case captureOID:
			capture = "16385"
		}
		f.Add(strings.Join(catalogShapes[i].segments, capture))
	}
	full := catalogShapes[2] // \dt
	joined := strings.Join(full.segments, "")
	f.Add(joined[:len(joined)-1])                                 // truncated by one byte
	f.Add(joined + " LIMIT 1")                                    // trailing garbage
	f.Add(strings.Join(catalogShapes[8].segments, `users.*`))     // regex, not a name
	f.Add(strings.Join(catalogShapes[8].segments, ``))            // empty capture
	f.Add(strings.Join(catalogShapes[9].segments, `99999999999`)) // oid past uint32
	f.Add(strings.Join(catalogShapes[9].segments, `16384'; --`))  // injection shape
	seg := catalogShapes[13].segments                             // referencing foreign keys: two captures
	f.Add(seg[0] + "16385" + seg[1] + "16386" + seg[2])
	f.Add("")
	f.Add("SELECT")
	f.Add(strings.Repeat("SELECT c.oid,\n", 64))

	fuzzTables := []sqldriver.TableInfo{
		{
			Name:       "orders",
			PrimaryKey: "/id",
			Columns: []sqldriver.ColumnInfo{
				{Path: "/id", Types: sqlast.TypeString, Required: true},
				{Path: "/total", Types: sqlast.TypeNumber | sqlast.TypeNull},
			},
		},
		{
			Name:       "users",
			PrimaryKey: "/id",
			Columns: []sqldriver.ColumnInfo{
				{Path: "/id", Types: sqlast.TypeString, Required: true},
				{Path: "/tier", Types: sqlast.TypeString | sqlast.TypeNull},
			},
			Indexes: []sqldriver.IndexInfo{
				{Name: "users_by_tier", Paths: []string{"/tier"}},
			},
		},
	}

	f.Fuzz(func(t *testing.T, text string) {
		shape, capture, ok := recognizeCatalogQuery(text)
		if !ok {
			return
		}
		if shape.capture == captureNone && capture != "" {
			t.Fatalf("%s captured %q from a capture-free shape", shape.command, capture)
		}
		if shape.capture != captureNone && !validCatalogCapture(shape.capture, capture) {
			t.Fatalf("%s accepted invalid capture %q", shape.command, capture)
		}
		answer := &catalogAnswer{database: "app", user: "u", tables: fuzzTables}
		fixed := shape.respond(answer, capture)
		if fixed == nil {
			t.Fatalf("%s recognized but produced no result", shape.command)
		}
		w := newWriter(io.Discard, 1<<20)
		if err := w.rowDescription(fixed.cols, nil); err != nil {
			t.Fatalf("%s produced an unencodable RowDescription: %v", shape.command, err)
		}
		for _, row := range fixed.rows {
			if len(row) != len(fixed.cols) {
				t.Fatalf("%s produced a row of width %d against %d columns",
					shape.command, len(row), len(fixed.cols))
			}
			if err := w.fixedRow(fixed.cols, row, nil); err != nil {
				t.Fatalf("%s produced an unencodable row: %v", shape.command, err)
			}
		}
	})
}
