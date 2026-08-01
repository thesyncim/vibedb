package driver

import (
	stdsql "database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/thesyncim/vibedb/query"
	sqlast "github.com/thesyncim/vibedb/sql"
	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
)

func openTestDB(t *testing.T) *stdsql.DB {
	t.Helper()
	db, err := stdsql.Open("vibedb", filepath.Join(t.TempDir(), "catalog.vdb"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	return db
}

func TestDriverConnectionUsesOneQueryWorker(t *testing.T) {
	raw, err := (Driver{}).Open(filepath.Join(t.TempDir(), "catalog.vdb"))
	if err != nil {
		t.Fatal(err)
	}
	connection := raw.(*conn)
	defer connection.Close()
	if got := connection.exec.Options.Workers; got != 1 {
		t.Fatalf("connection query workers = %d, want 1", got)
	}
}

func TestRowsCloseBreaksConnectionStatementRetention(t *testing.T) {
	connection := &conn{open: true}
	statement := &stmt{conn: connection, query: &query.Statement{}}
	rowset := connection.resetRows(statement, query.Cursor{}, nil)
	schema := []query.OutputColumn{{Header: "borrowed-plan-header"}}
	rowset.schema = schema
	rowset.schemaOK = true
	if err := statement.Close(); err != nil {
		t.Fatal(err)
	}
	if err := rowset.Close(); err != nil {
		t.Fatal(err)
	}
	if rowset.conn != nil || rowset.stmt != nil ||
		rowset.cursor != (query.Cursor{}) {
		t.Fatal("closed connection rowset retained its connection, statement, or cursor")
	}
	if connection.open {
		t.Fatal("closing rows did not release the connection")
	}
	if rowset.schemaOK || len(rowset.schema) != 0 ||
		schema[0] != (query.OutputColumn{}) {
		t.Fatal("closed connection rowset retained borrowed statement schema")
	}
}

func TestStmtCloseReleasesParsedAndCompiledStorage(t *testing.T) {
	statement := &stmt{
		tree:     new(sqlast.Statement),
		query:    new(query.Statement),
		mutation: new(query.DMLStatement),
		params:   3,
	}
	if err := statement.Close(); err != nil {
		t.Fatal(err)
	}
	if statement.tree != nil || statement.query != nil ||
		statement.mutation != nil || statement.primaryPoint ||
		statement.conn != nil {
		t.Fatal("closed statement retained parsed or compiled storage")
	}
	if got := statement.NumInput(); got != 3 {
		t.Fatalf("closed statement NumInput = %d, want retained count 3", got)
	}
	if err := statement.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestConnCloseReleasesOwnershipAndHighWaterStorage(t *testing.T) {
	raw, err := (Driver{}).Open(filepath.Join(t.TempDir(), "catalog.vdb"))
	if err != nil {
		t.Fatal(err)
	}
	connection := raw.(*conn)
	owner := connection.owner
	database := connection.db

	connection.args = make([]any, 8)
	connection.pointRaw = make([]byte, 0, 1024)
	connection.pointKeyRaw = make([]byte, 0, 1024)
	connection.pointKeyEnds = make([]int, 0, 32)
	connection.pointKeys = make([]string, 0, 32)
	connection.matchKeys = make([]string, 0, 32)
	if _, err := connection.pointDocs.Append([]byte(`{"id":"retained"}`)); err != nil {
		t.Fatal(err)
	}
	connection.exec.Options.MemoryBytes = 1 << 20
	connection.rowset.scratch = make([]byte, 0, 1024)
	connection.rowset.schema = make([]query.OutputColumn, 4)

	if err := connection.Close(); err != nil {
		t.Fatal(err)
	}
	if connection.db != nil || connection.owner != nil || connection.tx != nil {
		t.Fatal("closed connection retained its database ownership graph")
	}
	if connection.args != nil || connection.pointRaw != nil ||
		connection.pointKeyRaw != nil || connection.pointKeyEnds != nil ||
		connection.pointKeys != nil || connection.matchKeys != nil {
		t.Fatal("closed connection retained argument or point-query high-water buffers")
	}
	if connection.pointDocs.Len() != 0 ||
		connection.exec.Options != (query.ExecOptions{}) ||
		connection.rowset.scratch != nil || connection.rowset.schema != nil {
		t.Fatal("closed connection retained execution, segment, or rows high-water storage")
	}
	if !connection.rowset.closed {
		t.Fatal("released reusable rows value is not terminally closed")
	}
	if owner.db != nil || owner.refs != 0 || !owner.closed {
		t.Fatal("connection cleared ownership before its connector reference was released")
	}
	if !database.closeDone {
		t.Fatal("last connection close did not complete the terminal database close")
	}
	if err := connection.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestSQLCatalogHasOneWriterAcrossConnectors(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.vdb")
	first, err := stdsql.Open("vibedb", path)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Ping(); err != nil {
		t.Fatal(err)
	}

	second, err := stdsql.Open("vibedb", path)
	if err == nil {
		err = second.Ping()
		_ = second.Close()
	}
	if !errors.Is(err, durable.ErrWriterLocked) {
		t.Fatalf("second catalog writer = %v, want ErrWriterLocked", err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := stdsql.Open("vibedb", path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if err := reopened.Ping(); err != nil {
		t.Fatalf("catalog did not unlock after Close: %v", err)
	}
}

func TestDBCloseKeepsActiveRowsAndDefersCatalogRelease(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.vdb")
	db, err := stdsql.Open("vibedb", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE docs (PRIMARY KEY (id))`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		`INSERT INTO docs VALUES (?), (?)`,
		`{"id":"a"}`, `{"id":"b"}`,
	); err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query(`SELECT id FROM docs ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	blocked, err := stdsql.Open("vibedb", path)
	if err == nil {
		err = blocked.Ping()
		_ = blocked.Close()
	}
	if !errors.Is(err, durable.ErrWriterLocked) {
		t.Fatalf("catalog unlocked while rows remained active: %v", err)
	}

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 || ids[0] != "a" || ids[1] != "b" {
		t.Fatalf("rows after DB.Close = %v, want [a b]", ids)
	}

	reopened, err := stdsql.Open("vibedb", path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if err := reopened.Ping(); err != nil {
		t.Fatalf("catalog stayed locked after active rows closed: %v", err)
	}
}

func TestCatalogTableNamesAreUTF8AndFilesystemIndependent(t *testing.T) {
	db := openTestDB(t)
	invalid := string([]byte{'b', 'a', 'd', 0xff})
	if _, err := db.Exec(
		`CREATE TABLE "` + invalid + `" (PRIMARY KEY (id))`,
	); err == nil || !strings.Contains(err.Error(), "valid UTF-8") {
		t.Fatalf("invalid UTF-8 table name = %v", err)
	}

	name := strings.Repeat("long_name_", 40)
	quoted := `"` + name + `"`
	if _, err := db.Exec(
		`CREATE TABLE ` + quoted + ` (PRIMARY KEY (id))`,
	); err != nil {
		t.Fatalf("long table name CREATE: %v", err)
	}
	if _, err := db.Exec(
		`INSERT INTO `+quoted+` VALUES (?)`, `{"id":"works"}`,
	); err != nil {
		t.Fatalf("long table name first INSERT: %v", err)
	}
	var count int64
	if err := db.QueryRow(
		`SELECT COUNT(*) FROM ` + quoted,
	).Scan(&count); err != nil || count != 1 {
		t.Fatalf("long table query = (%d, %v), want (1, nil)", count, err)
	}
}

func TestDDLRejectsDefinitionsTheDurableCatalogCannotEncode(t *testing.T) {
	t.Run("schema path", func(t *testing.T) {
		db := openTestDB(t)
		field := strings.Repeat("s", 1<<16)
		_, err := db.Exec(fmt.Sprintf(
			`CREATE TABLE docs ("%s" STRING PRIMARY KEY)`, field))
		if !errors.Is(err, store.ErrSchemaDefinition) {
			t.Fatalf("oversized schema path = %v, want ErrSchemaDefinition", err)
		}
	})

	t.Run("explicit index name", func(t *testing.T) {
		db := openTestDB(t)
		if _, err := db.Exec(`CREATE TABLE docs (PRIMARY KEY (id))`); err != nil {
			t.Fatal(err)
		}
		name := strings.Repeat("i", 1<<16)
		_, err := db.Exec(fmt.Sprintf(
			`CREATE INDEX "%s" ON docs (kind)`, name))
		if !errors.Is(err, store.ErrIndexDefinition) {
			t.Fatalf("oversized explicit index name = %v, want ErrIndexDefinition", err)
		}
		if _, err := db.Exec(
			`INSERT INTO docs VALUES (?)`, `{"id":"still-writable"}`,
		); err != nil {
			t.Fatalf("rejected index poisoned first INSERT: %v", err)
		}
	})
}

func TestDatabaseSQLLifecycle(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE docs (PRIMARY KEY (id))`); err != nil {
		t.Fatal(err)
	}
	documents := []string{
		`{"id":"a","kind":"x","n":1}`,
		`{"id":"b","kind":"y","n":2}`,
		`{"id":"c","kind":"x","n":3}`,
	}
	for _, document := range documents {
		if _, err := db.Exec(`INSERT INTO docs VALUES (?)`, []byte(document)); err != nil {
			t.Fatal(err)
		}
	}
	prepared, err := db.Prepare(`SELECT * FROM docs WHERE id = ?`)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	var raw []byte
	if err := prepared.QueryRow("b").Scan(&raw); err != nil {
		t.Fatal(err)
	}
	if string(raw) != documents[1] {
		t.Fatalf("document = %s, want %s", raw, documents[1])
	}

	rows, err := db.Query(`SELECT id FROM docs ORDER BY id LIMIT 2`)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		got = append(got, id)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("limited scan = %v, want [a b]", got)
	}

	deleted, err := db.Exec(`DELETE FROM docs WHERE id = ?`, "b")
	if err != nil {
		t.Fatal(err)
	}
	if n, _ := deleted.RowsAffected(); n != 1 {
		t.Fatalf("RowsAffected = %d, want 1", n)
	}
	if err := prepared.QueryRow("b").Scan(&raw); !errors.Is(err, stdsql.ErrNoRows) {
		t.Fatalf("deleted lookup = %v, want sql.ErrNoRows", err)
	}
}

func TestExactIndexCountAndAutocommitMaintenance(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE docs (PRIMARY KEY (id))`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE INDEX ON docs(kind)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO docs VALUES (?)`, `{"id":"seed","kind":"x"}`); err != nil {
		t.Fatal(err)
	}
	var count int64
	if err := db.QueryRow(`SELECT COUNT(*) FROM docs WHERE kind = ?`, "x").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("COUNT(*) = %d, want 1", count)
	}
	if _, err := db.Exec(
		`INSERT INTO docs VALUES (?), (?)`,
		`{"id":"later","kind":"x"}`,
		`{"id":"other","kind":"x"}`,
	); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM docs WHERE kind = ?`, "x").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Fatalf("COUNT(*) after indexed multi-row INSERT = %d, want 3", count)
	}
	if _, err := db.Exec(
		`UPDATE docs SET "$doc" = ? WHERE id = ?`,
		`{"id":"later","kind":"y"}`, "later",
	); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM docs WHERE kind = ?`, "x").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("COUNT(*) after indexed UPDATE = %d, want 2", count)
	}
	if _, err := db.Exec(
		`DELETE FROM docs WHERE id IN (?, ?)`, "seed", "other",
	); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM docs WHERE kind = ?`, "x").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("COUNT(*) after indexed multi-document DELETE = %d, want 0", count)
	}
}

func TestTypedSchemaValidationPersistsAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.vdb")
	db, err := stdsql.Open("vibedb", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		CREATE TABLE docs (
			id STRING PRIMARY KEY,
			name STRING NOT NULL,
			nickname STRING NULL,
			age INTEGER
		)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		`INSERT INTO docs VALUES (?)`,
		`{"id":"ok","name":"Ada","nickname":null}`,
	); err != nil {
		t.Fatalf("nullable insert: %v", err)
	}
	for _, document := range []string{
		`{"id":"wrong-name","name":7}`,
		`{"id":"missing-name","age":3}`,
		`{"id":"fractional-age","name":"Grace","age":1.5}`,
	} {
		if _, err := db.Exec(`INSERT INTO docs VALUES (?)`, document); !errors.Is(err, store.ErrSchemaViolation) {
			t.Fatalf("INSERT %s = %v, want ErrSchemaViolation", document, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = stdsql.Open("vibedb", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(
		`INSERT INTO docs VALUES (?)`,
		`{"id":"after-reopen","name":false}`,
	); !errors.Is(err, store.ErrSchemaViolation) {
		t.Fatalf("reopened wrong-type INSERT = %v, want ErrSchemaViolation", err)
	}
	var name string
	if err := db.QueryRow(`SELECT name FROM docs WHERE id = ?`, "ok").Scan(&name); err != nil {
		t.Fatal(err)
	}
	if name != "Ada" {
		t.Fatalf("name = %q, want Ada", name)
	}
}

func TestTypedSchemaValidatesEveryJSONKind(t *testing.T) {
	db := openTestDB(t)
	tests := []struct {
		name         string
		sqlType      string
		valid        []string
		invalid      string
		acceptAbsent bool
	}{
		{
			name: "null", sqlType: "NULL",
			valid: []string{"null"}, invalid: "true", acceptAbsent: true,
		},
		{
			name: "bool", sqlType: "BOOL NOT NULL",
			valid: []string{"true", "false"}, invalid: "0",
		},
		{
			name: "number", sqlType: "NUMBER NOT NULL",
			valid: []string{"1.25", "2"}, invalid: `"2"`,
		},
		{
			name: "integer", sqlType: "INTEGER NOT NULL",
			valid: []string{"-2"}, invalid: "1.5",
		},
		{
			name: "string", sqlType: "STRING NOT NULL",
			valid: []string{`"value"`}, invalid: "false",
		},
		{
			name: "array", sqlType: "ARRAY NOT NULL",
			valid: []string{`[1,"two"]`}, invalid: `{"zero":0}`,
		},
		{
			name: "object", sqlType: "OBJECT NOT NULL",
			valid: []string{`{"nested":true}`}, invalid: `[false]`,
		},
		{
			// ANY and its JSON alias accept every non-null JSON kind here;
			// NOT NULL gives both declarations a rejection boundary to prove.
			name: "any", sqlType: "ANY NOT NULL",
			valid:   []string{"true", "1.25", `"value"`, `[1]`, `{"one":1}`},
			invalid: "null",
		},
		{
			name: "json", sqlType: "JSON NOT NULL",
			valid:   []string{"false", "-3", `"value"`, `[]`, `{}`},
			invalid: "null",
		},
		{
			// Nullable columns distinguish absence from a present wrong kind:
			// both missing and null are admitted, while a bool is not a string.
			name: "nullable_string", sqlType: "STRING",
			valid: []string{`"value"`, "null"}, invalid: "false", acceptAbsent: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			table := "schema_" + test.name
			if _, err := db.Exec(fmt.Sprintf(
				`CREATE TABLE %s (id STRING PRIMARY KEY, value %s)`,
				table, test.sqlType,
			)); err != nil {
				t.Fatalf("CREATE TABLE: %v", err)
			}
			inserted := int64(0)
			for i, value := range test.valid {
				document := fmt.Sprintf(
					`{"id":"valid-%d","value":%s}`, i, value,
				)
				if _, err := db.Exec(
					fmt.Sprintf(`INSERT INTO %s VALUES (?)`, table),
					document,
				); err != nil {
					t.Fatalf("valid %s INSERT: %v", value, err)
				}
				inserted++
			}
			if test.acceptAbsent {
				if _, err := db.Exec(
					fmt.Sprintf(`INSERT INTO %s VALUES (?)`, table),
					`{"id":"absent"}`,
				); err != nil {
					t.Fatalf("absent nullable value: %v", err)
				}
				inserted++
			}
			if _, err := db.Exec(
				fmt.Sprintf(`INSERT INTO %s VALUES (?)`, table),
				fmt.Sprintf(`{"id":"wrong","value":%s}`, test.invalid),
			); !errors.Is(err, store.ErrSchemaViolation) {
				t.Fatalf(
					"wrong-kind %s INSERT = %v, want ErrSchemaViolation",
					test.invalid, err,
				)
			}
			var count int64
			if err := db.QueryRow(
				fmt.Sprintf(`SELECT COUNT(*) FROM %s`, table),
			).Scan(&count); err != nil {
				t.Fatalf("COUNT after rejection: %v", err)
			}
			if count != inserted {
				t.Fatalf("COUNT = %d, want %d valid documents", count, inserted)
			}
		})
	}
}

func TestCompoundIndexSchemaReopenAndMutation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.vdb")
	db, err := stdsql.Open("vibedb", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		CREATE TABLE docs (
			id STRING PRIMARY KEY,
			tenant STRING NOT NULL,
			kind STRING NOT NULL
		)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE INDEX by_tenant_kind ON docs (tenant, kind)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		`INSERT INTO docs VALUES (?), (?), (?)`,
		`{"id":"a","tenant":"acme","kind":"open"}`,
		`{"id":"b","tenant":"acme","kind":"closed"}`,
		`{"id":"c","tenant":"other","kind":"open"}`,
	); err != nil {
		t.Fatal(err)
	}
	var count int64
	if err := db.QueryRow(
		`SELECT COUNT(*) FROM docs WHERE tenant = ? AND kind = ?`,
		"acme", "open",
	).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("compound count = %d, want 1", count)
	}
	if _, err := db.Exec(
		`INSERT INTO docs VALUES (?)`,
		`{"id":"d","tenant":"acme","kind":"open"}`,
	); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = stdsql.Open("vibedb", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.QueryRow(
		`SELECT COUNT(*) FROM docs WHERE tenant = ? AND kind = ?`,
		"acme", "open",
	).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("reopened compound count = %d, want 2", count)
	}
}

func TestNumericPrimaryKeysUseExactCanonicalIdentity(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE docs (id NUMBER PRIMARY KEY, value STRING NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO docs VALUES (?)`, `{"id":1.0,"value":"first"}`); err != nil {
		t.Fatal(err)
	}
	var value string
	if err := db.QueryRow(`SELECT value FROM docs WHERE id = ?`, int64(1)).Scan(&value); err != nil {
		t.Fatal(err)
	}
	if value != "first" {
		t.Fatalf("value = %q, want first", value)
	}
	if _, err := db.Exec(
		`INSERT INTO docs VALUES (?)`, `{"id":1e0,"value":"duplicate"}`,
	); !errors.Is(err, ErrDuplicatePrimaryKey) {
		t.Fatalf("equivalent numeric INSERT = %v, want ErrDuplicatePrimaryKey", err)
	}
	var count int64
	if err := db.QueryRow(`SELECT COUNT(*) FROM docs`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("COUNT(*) = %d, want one canonical numeric key", count)
	}
	if err := db.QueryRow(`SELECT value FROM docs WHERE id = ?`, int64(1)).Scan(&value); err != nil {
		t.Fatal(err)
	}
	if value != "first" {
		t.Fatalf("duplicate INSERT replaced value with %q", value)
	}
	if _, err := db.Exec(
		`UPDATE docs SET "$doc" = ? WHERE id = ?`,
		`{"id":1e0,"value":"replaced"}`, int64(1),
	); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT value FROM docs WHERE id = ?`, int64(1)).Scan(&value); err != nil {
		t.Fatal(err)
	}
	if value != "replaced" {
		t.Fatalf("explicit UPDATE = %q, want replaced", value)
	}
	if _, err := db.Exec(
		`INSERT INTO docs VALUES (?), (?)`,
		`{"id":2,"value":"a"}`, `{"id":2.0,"value":"b"}`,
	); err == nil {
		t.Fatal("duplicate canonical primary keys in one VALUES batch succeeded")
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM docs`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("duplicate batch changed table: COUNT(*) = %d", count)
	}
	if _, err := db.Exec(`INSERT INTO docs VALUES (?)`, `{"id":-0,"value":"zero"}`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		`INSERT INTO docs VALUES (?)`, `{"id":0.0,"value":"duplicate-zero"}`,
	); !errors.Is(err, ErrDuplicatePrimaryKey) {
		t.Fatalf("equivalent zero INSERT = %v, want ErrDuplicatePrimaryKey", err)
	}
	if _, err := db.Exec(
		`INSERT INTO docs VALUES (?)`,
		`{"id":0e999999999999999999999999,"value":"duplicate-wide-zero"}`,
	); !errors.Is(err, ErrDuplicatePrimaryKey) {
		t.Fatalf("huge-exponent zero INSERT = %v, want ErrDuplicatePrimaryKey", err)
	}
	if err := db.QueryRow(`SELECT value FROM docs WHERE id = ?`, int64(0)).Scan(&value); err != nil {
		t.Fatal(err)
	}
	if value != "zero" {
		t.Fatalf("zero value = %q, want zero", value)
	}
	if _, err := db.Exec(
		`INSERT INTO docs VALUES (?)`,
		`{"id":1e9223372036854775808,"value":"wide"}`,
	); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(
		`SELECT value FROM docs WHERE id = ?`,
		json.Number("0.1e9223372036854775809"),
	).Scan(&value); err != nil {
		t.Fatal(err)
	}
	if value != "wide" {
		t.Fatalf("wide numeric key value = %q, want wide", value)
	}
	if _, err := db.Exec(
		`INSERT INTO docs VALUES (?)`,
		`{"id":10e9223372036854775807,"value":"duplicate-wide"}`,
	); !errors.Is(err, ErrDuplicatePrimaryKey) {
		t.Fatalf("equivalent wide numeric INSERT = %v, want ErrDuplicatePrimaryKey", err)
	}
}

func TestPrimaryKeyPointLookupPreservesSQLNullSemantics(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE docs (id STRING PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO docs VALUES (?)`, `{"id":"present"}`); err != nil {
		t.Fatal(err)
	}
	var count int64
	if err := db.QueryRow(`SELECT COUNT(*) FROM docs WHERE id = ?`, nil).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("primary_key = NULL count = %d, want 0", count)
	}
	if err := db.QueryRow(
		`SELECT COUNT(*) FROM docs WHERE id IN (?, ?)`, "present", nil,
	).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("primary_key IN (present, NULL) count = %d, want 1", count)
	}
}

func TestPrimaryKeyScalarFamiliesRemainDistinct(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE docs (PRIMARY KEY (id))`); err != nil {
		t.Fatal(err)
	}
	for _, document := range []string{
		`{"id":true}`, `{"id":"true"}`, `{"id":1}`, `{"id":"1"}`,
	} {
		if _, err := db.Exec(`INSERT INTO docs VALUES (?)`, document); err != nil {
			t.Fatal(err)
		}
	}
	var count int64
	if err := db.QueryRow(`SELECT COUNT(*) FROM docs`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 4 {
		t.Fatalf("COUNT(*) = %d, want four type-distinct primary keys", count)
	}
}

func TestStringPrimaryKeyUsesDecodedJSONIdentity(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE docs (id STRING PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		`INSERT INTO docs VALUES (?)`,
		`{"id":"a","value":"literal"}`,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		`INSERT INTO docs VALUES (?)`,
		`{"id":"\u0061","value":"escaped"}`,
	); !errors.Is(err, ErrDuplicatePrimaryKey) {
		t.Fatalf("equivalent escaped string INSERT = %v, want ErrDuplicatePrimaryKey", err)
	}
	var value string
	if err := db.QueryRow(
		`SELECT value FROM docs WHERE id = ?`, "a",
	).Scan(&value); err != nil {
		t.Fatal(err)
	}
	if value != "literal" {
		t.Fatalf("duplicate escaped identity replaced value with %q", value)
	}
}

func TestFlatInsertAndConcurrentReaders(t *testing.T) {
	db := openTestDB(t)
	db.SetMaxOpenConns(8)
	if _, err := db.Exec(`CREATE TABLE docs (PRIMARY KEY (id))`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO docs (id, active, score) VALUES (?, ?, ?)`, "a", true, int64(7)); err != nil {
		t.Fatal(err)
	}
	const readers = 8
	var wg sync.WaitGroup
	errs := make(chan error, readers)
	for range readers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var score int64
			errs <- db.QueryRow(`SELECT score FROM docs WHERE id = ?`, "a").Scan(&score)
			if score != 7 {
				errs <- errors.New("unexpected score")
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
}

// A transaction publishes its staged changes as one atomic unit.
func TestTransactionCommitsAndRollsBackAsAWhole(t *testing.T) {
	db := openTestDB(t)
	db.SetMaxOpenConns(2)
	if _, err := db.Exec(`CREATE TABLE docs (PRIMARY KEY (id))`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO docs VALUES (?)`, `{"id":"base","n":0}`); err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO docs VALUES (?)`, `{"id":"committed","n":1}`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`DELETE FROM docs WHERE id = ?`, "base"); err != nil {
		t.Fatal(err)
	}
	var count int64
	if err := db.QueryRow(`SELECT COUNT(*) FROM docs WHERE id = ?`, "committed").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatal("uncommitted insert was externally visible")
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM docs WHERE id = ?`, "committed").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatal("committed insert was not visible")
	}

	rolled, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rolled.Exec(`INSERT INTO docs VALUES (?)`, `{"id":"rolled","n":2}`); err != nil {
		t.Fatal(err)
	}
	if err := rolled.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM docs WHERE id = ?`, "rolled").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatal("rolled-back insert was visible")
	}
}

func TestCatalogAndCollectionReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.vdb")
	db, err := stdsql.Open("vibedb", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE docs (PRIMARY KEY (id))`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO docs VALUES (?)`, `{"id":"persisted","n":9}`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = stdsql.Open("vibedb", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var n int64
	if err := db.QueryRow(`SELECT n FROM docs WHERE id = ?`, "persisted").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 9 {
		t.Fatalf("n = %d, want 9", n)
	}
}
