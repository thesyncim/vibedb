package driver

import (
	"context"
	stdsql "database/sql"
	sqldriver "database/sql/driver"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/query"
	sqlast "github.com/thesyncim/vibedb/sql"
)

func TestDatabaseSQLOrdinaryViewsNestedAliasesAndRestrict(t *testing.T) {
	db := openTestDB(t)
	db.SetMaxOpenConns(2)
	if _, err := db.Exec(`CREATE TABLE docs (
		id STRING PRIMARY KEY,
		kind STRING NOT NULL,
		score NUMBER NOT NULL
	)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		`INSERT INTO docs VALUES (?), (?), (?)`,
		`{"id":"a","kind":"open","score":1}`,
		`{"id":"b","kind":"open","score":3}`,
		`{"id":"c","kind":"closed","score":5}`,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE VIEW open_docs AS
		SELECT id, score FROM docs WHERE kind = 'open'`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE VIEW ranked (doc_id, points) AS
		SELECT id, score FROM open_docs WHERE score >= 2`); err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query(`SELECT doc_id, points FROM ranked ORDER BY doc_id`)
	if err != nil {
		t.Fatal(err)
	}
	columns, err := rows.Columns()
	if err != nil {
		rows.Close()
		t.Fatal(err)
	}
	if !reflect.DeepEqual(columns, []string{"doc_id", "points"}) {
		rows.Close()
		t.Fatalf("columns = %q", columns)
	}
	var got []any
	for rows.Next() {
		var id string
		var score float64
		if err := rows.Scan(&id, &score); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		got = append(got, id, score)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, []any{"b", float64(3)}) {
		t.Fatalf("rows = %#v", got)
	}

	if _, err := db.Exec(`DROP VIEW open_docs`); !errors.Is(err, ErrDependentObjects) {
		t.Fatalf("DROP dependency = %v, want ErrDependentObjects", err)
	}
	if _, err := db.Exec(`DROP TABLE docs`); !errors.Is(err, ErrDependentObjects) {
		t.Fatalf("DROP table dependency = %v, want ErrDependentObjects", err)
	}
	if _, err := db.Exec(`CREATE TABLE ranked (id STRING PRIMARY KEY)`); !errors.Is(err, ErrTableExists) {
		t.Fatalf("table/view namespace collision = %v, want ErrTableExists", err)
	}
	if _, err := db.Exec(`DROP VIEW ranked RESTRICT`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DROP VIEW IF EXISTS ranked`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DROP VIEW open_docs`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DROP TABLE docs`); err != nil {
		t.Fatal(err)
	}
}

func TestDatabaseSQLViewReopenRevalidationAndPreparedGeneration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.vdb")
	db, err := stdsql.Open("vibedb", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE docs (id STRING PRIMARY KEY, n NUMBER NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO docs VALUES (?), (?)`,
		`{"id":"a","n":1}`, `{"id":"b","n":2}`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE VIEW selected AS SELECT id, n FROM docs WHERE n >= 1`); err != nil {
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
	prepared, err := db.Prepare(`SELECT id FROM selected ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	if got := queryViewStrings(t, func() (*stdsql.Rows, error) {
		return prepared.Query()
	}); !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Fatalf("reopened rows = %v", got)
	}
	if _, err := db.Exec(`DROP VIEW selected`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE VIEW selected AS SELECT id, n FROM docs WHERE n >= 2`); err != nil {
		t.Fatal(err)
	}
	rows, err := prepared.Query()
	if rows != nil {
		rows.Close()
	}
	if !errors.Is(err, ErrViewChanged) {
		t.Fatalf("stale prepared view = %v, want ErrViewChanged", err)
	}
	if got := queryViewStrings(t, func() (*stdsql.Rows, error) {
		return db.Query(`SELECT id FROM selected ORDER BY id`)
	}); !reflect.DeepEqual(got, []string{"b"}) {
		t.Fatalf("recreated view rows = %v", got)
	}
}

func TestDatabaseSQLViewTransactionRetainsDefinitionAndTableSnapshot(t *testing.T) {
	db := openTestDB(t)
	db.SetMaxOpenConns(2)
	if _, err := db.Exec(`CREATE TABLE docs (id STRING PRIMARY KEY, n NUMBER NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO docs VALUES (?)`, `{"id":"old","n":1}`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE VIEW stable AS SELECT id, n FROM docs`); err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(context.Background(), &stdsql.TxOptions{
		Isolation: stdsql.LevelSnapshot,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if got := queryViewStrings(t, func() (*stdsql.Rows, error) {
		return tx.Query(`SELECT id FROM stable`)
	}); !reflect.DeepEqual(got, []string{"old"}) {
		t.Fatalf("initial transaction view = %v", got)
	}
	if _, err := db.Exec(`DROP VIEW stable`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DROP TABLE docs`); err != nil {
		t.Fatal(err)
	}
	if got := queryViewStrings(t, func() (*stdsql.Rows, error) {
		return tx.Query(`SELECT id FROM stable`)
	}); !reflect.DeepEqual(got, []string{"old"}) {
		t.Fatalf("transaction view after catalog removal = %v", got)
	}
	if _, err := db.Exec(`CREATE TABLE docs (id STRING PRIMARY KEY, n NUMBER NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO docs VALUES (?)`, `{"id":"new","n":2}`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE VIEW stable AS SELECT id, n FROM docs WHERE n >= 2`); err != nil {
		t.Fatal(err)
	}
	if got := queryViewStrings(t, func() (*stdsql.Rows, error) {
		return tx.Query(`SELECT id FROM stable`)
	}); !reflect.DeepEqual(got, []string{"old"}) {
		t.Fatalf("transaction view after DROP/recreate = %v", got)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if got := queryViewStrings(t, func() (*stdsql.Rows, error) {
		return db.Query(`SELECT id FROM stable`)
	}); !reflect.DeepEqual(got, []string{"new"}) {
		t.Fatalf("outside recreated view = %v", got)
	}
}

func TestDatabaseSQLViewRefusalsAreTypedAndAtomic(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE docs (id STRING PRIMARY KEY, n NUMBER NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		source string
		is     error
	}{
		{`CREATE VIEW wild AS SELECT * FROM docs`, nil},
		{`CREATE VIEW duplicate AS SELECT id, id FROM docs`, ErrDuplicateViewColumn},
		{`CREATE VIEW missing AS SELECT id FROM absent`, ErrTableNotFound},
		{`CREATE VIEW self_ref AS SELECT id FROM self_ref`, query.ErrSQLViewCycle},
		{`CREATE MATERIALIZED VIEW mat AS SELECT id FROM docs`, nil},
	}
	for _, test := range tests {
		t.Run(test.source, func(t *testing.T) {
			_, err := db.Exec(test.source)
			if err == nil {
				t.Fatal("statement succeeded")
			}
			if test.is != nil && !errors.Is(err, test.is) {
				t.Fatalf("error = %v, want errors.Is(%v)", err, test.is)
			}
			if test.is == nil {
				var unsupported *sqlast.FeatureNotSupportedError
				if !errors.As(err, &unsupported) {
					t.Fatalf("error = %T %v, want FeatureNotSupported", err, err)
				}
			}
		})
	}
	for _, name := range []string{"wild", "duplicate", "missing", "self_ref", "mat"} {
		if _, err := db.Exec(`DROP VIEW ` + name); !errors.Is(err, ErrViewNotFound) {
			t.Fatalf("failed CREATE published %q: %v", name, err)
		}
	}
}

func TestDirectPreparedViewWarmAllocations(t *testing.T) {
	connection := directTestConn(t)
	directExec(t, connection,
		`CREATE TABLE docs (id STRING PRIMARY KEY, n NUMBER NOT NULL)`, nil)
	directExec(t, connection, `INSERT INTO docs VALUES (?)`,
		[]sqldriver.NamedValue{{Ordinal: 1, Value: `{"id":"a","n":1}`}})
	directExec(t, connection,
		`CREATE VIEW docs_view AS SELECT id, n FROM docs`, nil)
	statement, err := connection.Prepare(
		`SELECT n FROM docs_view WHERE id = ?`)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Close()
	args := []sqldriver.Value{"a"}
	destination := make([]sqldriver.Value, 1)
	if err := runDirectQuery(statement, args, destination); err != nil {
		t.Fatal(err)
	}
	var runErr error
	allocations := testing.AllocsPerRun(100, func() {
		runErr = runDirectQuery(statement, args, destination)
	})
	if runErr != nil {
		t.Fatal(runErr)
	}
	if allocations != 0 {
		t.Fatalf("warmed view query allocated %.2f times, want zero", allocations)
	}
}

func TestDatabaseSQLViewCancellationAndDepthFailureAreAtomicAndReusable(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE docs (id STRING PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	prepared, err := db.Prepare(`CREATE VIEW cancellable AS SELECT id FROM docs`)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := prepared.ExecContext(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled CREATE VIEW = %v, want context.Canceled", err)
	}
	if _, err := db.Exec(`DROP VIEW cancellable`); !errors.Is(err, ErrViewNotFound) {
		t.Fatalf("canceled CREATE published metadata: %v", err)
	}
	if _, err := prepared.Exec(); err != nil {
		t.Fatalf("prepared CREATE after cancellation: %v", err)
	}
	if _, err := db.Exec(`DROP VIEW cancellable`); err != nil {
		t.Fatal(err)
	}

	previous := "docs"
	failed := ""
	failedStatement := ""
	for depth := 0; depth < 2*maxSQLViewDepthForDriverTest; depth++ {
		name := "depth_view_" + strconv.Itoa(depth)
		statement := `CREATE VIEW ` + name + ` AS SELECT id FROM ` + previous
		_, err := db.Exec(statement)
		if errors.Is(err, query.ErrSQLViewExpansionLimit) {
			failed = name
			failedStatement = statement
			var positioned interface{ Position() int }
			if !errors.As(err, &positioned) {
				t.Fatalf("depth error has no position: %T %v", err, err)
			}
			if want := strings.LastIndex(statement, previous); positioned.Position() != want {
				t.Fatalf("depth error position = %d, want %d in %q",
					positioned.Position(), want, statement)
			}
			break
		}
		if err != nil {
			t.Fatalf("depth %d CREATE VIEW: %v", depth, err)
		}
		previous = name
	}
	if failed == "" {
		t.Fatal("view dependency chain did not reach its finite depth bound")
	}
	if failedStatement == "" {
		t.Fatal("depth refusal lost its authored statement")
	}
	if _, err := db.Exec(`DROP VIEW ` + failed); !errors.Is(err, ErrViewNotFound) {
		t.Fatalf("depth failure published %q: %v", failed, err)
	}
	if got := queryViewStrings(t, func() (*stdsql.Rows, error) {
		return db.Query(`SELECT id FROM ` + previous)
	}); len(got) != 0 {
		t.Fatalf("last valid view returned rows: %v", got)
	}
	if _, err := db.Exec(`CREATE VIEW after_depth_failure AS SELECT id FROM docs`); err != nil {
		t.Fatalf("connection was not reusable after depth refusal: %v", err)
	}
	if _, err := db.Exec(`DROP VIEW after_depth_failure`); err != nil {
		t.Fatalf("cleanup after depth refusal: %v", err)
	}
}

const maxSQLViewDepthForDriverTest = 40

func TestPreparedViewAndTransactionReleaseCatalogGenerations(t *testing.T) {
	raw, err := (Driver{}).Open(filepath.Join(t.TempDir(), "catalog.vdb"))
	if err != nil {
		t.Fatal(err)
	}
	connection := raw.(*conn)
	defer connection.Close()
	for _, statement := range []string{
		`CREATE TABLE docs (id STRING PRIMARY KEY)`,
		`CREATE VIEW stable AS SELECT id FROM docs`,
	} {
		if _, err := connection.ExecContext(
			context.Background(), statement, nil,
		); err != nil {
			t.Fatal(err)
		}
	}
	prepared, err := connection.prepareContext(
		context.Background(), `SELECT id FROM stable`,
	)
	if err != nil {
		t.Fatal(err)
	}
	if prepared.views == nil || len(prepared.views.dependencies) != 1 ||
		prepared.views.dependencies[0].meta == nil {
		t.Fatalf("prepared view state = %+v", prepared.views)
	}
	if err := prepared.Close(); err != nil {
		t.Fatal(err)
	}
	if prepared.views != nil || prepared.tree != nil || prepared.query != nil {
		t.Fatal("closed statement retained view generation or plan storage")
	}
	transaction, err := connection.beginTx(
		context.Background(), sqldriver.TxOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if transaction.views["stable"] == nil {
		t.Fatal("transaction did not snapshot the view generation")
	}
	if err := transaction.Rollback(); err != nil {
		t.Fatal(err)
	}
	if transaction.views != nil || transaction.tables != nil || transaction.conn != nil {
		t.Fatal("finished transaction retained view or table catalog generations")
	}
}

func TestTransactionWithoutViewsHasNoViewCatalogSidecar(t *testing.T) {
	raw, err := (Driver{}).Open(filepath.Join(t.TempDir(), "catalog.vdb"))
	if err != nil {
		t.Fatal(err)
	}
	connection := raw.(*conn)
	defer connection.Close()
	transaction, err := connection.beginTx(
		context.Background(), sqldriver.TxOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if transaction.views != nil {
		t.Fatal("view-free transaction allocated a view catalog sidecar")
	}
	if err := transaction.Rollback(); err != nil {
		t.Fatal(err)
	}
}

func queryViewStrings(
	t testing.TB,
	query func() (*stdsql.Rows, error),
) []string {
	t.Helper()
	rows, err := query()
	if err != nil {
		t.Fatal(err)
	}
	return scanViewStrings(t, rows)
}

func scanViewStrings(t testing.TB, rows *stdsql.Rows) []string {
	t.Helper()
	defer rows.Close()
	var values []string
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			t.Fatal(err)
		}
		values = append(values, value)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return values
}

func TestCatalogViewMetadataStrictDecodeAndSemanticReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.vdb")
	database, err := openDatabase(path)
	if err != nil {
		t.Fatal(err)
	}
	createTree, err := sqlast.ParseStatement(
		`CREATE TABLE docs (id STRING PRIMARY KEY, n NUMBER NOT NULL)`,
	)
	if err != nil {
		t.Fatal(err)
	}
	create, err := query.PrepareParsedDML("CREATE TABLE docs", createTree)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.createTableLocked(create); err != nil {
		create.Release()
		t.Fatal(err)
	}
	create.Release()
	meta, err := buildViewMeta(
		context.Background(), nil,
		"stable", `SELECT id, n FROM docs`, nil,
		database.catalog.Views, database.tables, 0,
	)
	if err != nil {
		t.Fatal(err)
	}
	database.catalog.Views["stable"] = meta
	if _, err := database.persistCatalogLocked(); err != nil {
		t.Fatal(err)
	}
	if err := database.closeTerminal(); err != nil {
		t.Fatal(err)
	}

	reopened, err := openDatabase(path)
	if err != nil {
		t.Fatal(err)
	}
	if reopened.catalog.Views["stable"] == nil {
		reopened.closeTerminal()
		t.Fatal("reopen lost durable view")
	}
	if err := reopened.closeTerminal(); err != nil {
		t.Fatal(err)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var decoded catalogFile
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	decoded.Views["stable"].Outputs[0] = "wrong"
	semanticCorruption, err := json.MarshalIndent(decoded, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, semanticCorruption, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := openDatabase(path); err == nil || !strings.Contains(err.Error(), "metadata does not match") {
		t.Fatalf("semantically corrupt view reopen = %v", err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	corrupt := strings.Replace(
		string(raw), `"outputs": [`, `"unknown": true, "outputs": [`, 1,
	)
	if err := os.WriteFile(path, []byte(corrupt), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := openDatabase(path); err == nil || !strings.Contains(err.Error(), "unknown member") {
		t.Fatalf("corrupt view metadata reopen = %v", err)
	}
}
