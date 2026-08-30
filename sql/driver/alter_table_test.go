package driver

import (
	"context"
	stdsql "database/sql"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	sqlast "github.com/thesyncim/vibedb/sql"
)

func TestAlterTableAddColumnPreservesRowsIndexesAndReopens(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.vdb")
	db, err := stdsql.Open("vibedb", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		CREATE TABLE docs (
			id STRING PRIMARY KEY,
			kind STRING NOT NULL
		)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE INDEX by_kind ON docs(kind)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO docs VALUES (?), (?)`,
		`{"id":"a","kind":"x","payload":{"kept":true}}`,
		`{"id":"b","kind":"x","payload":{"kept":false}}`,
	); err != nil {
		t.Fatal(err)
	}

	result, err := db.Exec(`ALTER TABLE docs ADD COLUMN score INTEGER`)
	if err != nil {
		t.Fatal(err)
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 0 {
		t.Fatalf("ALTER rows affected = %d, err %v, want 0", affected, err)
	}
	result, err = db.Exec(`UPDATE docs SET score = 7 WHERE id = 'a'`)
	if err != nil {
		t.Fatal(err)
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		t.Fatalf("UPDATE rows affected = %d, err %v, want 1", affected, err)
	}

	var kind string
	var score int64
	var kept bool
	if err := db.QueryRow(`
		SELECT kind, score, payload.kept FROM docs WHERE id = 'a'
	`).Scan(&kind, &score, &kept); err != nil {
		t.Fatal(err)
	}
	if kind != "x" || score != 7 || !kept {
		t.Fatalf("updated row = (%q, %d, %v), want (x, 7, true)", kind, score, kept)
	}
	var absent any
	if err := db.QueryRow(`SELECT score FROM docs WHERE id = 'b'`).Scan(&absent); err != nil {
		t.Fatal(err)
	}
	if absent != nil {
		t.Fatalf("older row score = %#v, want NULL", absent)
	}
	var indexed int64
	if err := db.QueryRow(`SELECT COUNT(*) FROM docs WHERE kind = 'x'`).Scan(&indexed); err != nil {
		t.Fatal(err)
	}
	if indexed != 2 {
		t.Fatalf("indexed row count = %d, want 2", indexed)
	}
	if _, err := db.Exec(`UPDATE docs SET score = 'bad' WHERE id = 'a'`); err == nil {
		t.Fatal("wrong-type declared UPDATE succeeded")
	}
	if err := db.QueryRow(`SELECT score FROM docs WHERE id = 'a'`).Scan(&score); err != nil {
		t.Fatal(err)
	}
	if score != 7 {
		t.Fatalf("failed UPDATE changed score to %d, want 7", score)
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = stdsql.Open("vibedb", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.QueryRow(`SELECT COUNT(*) FROM docs WHERE kind = 'x'`).Scan(&indexed); err != nil {
		t.Fatal(err)
	}
	if indexed != 2 {
		t.Fatalf("reopened indexed row count = %d, want 2", indexed)
	}
	if _, err := db.Exec(`UPDATE docs SET score = 9 WHERE id = 'b'`); err != nil {
		t.Fatalf("declared UPDATE after reopen: %v", err)
	}
	if err := db.QueryRow(`SELECT score FROM docs WHERE id = 'b'`).Scan(&score); err != nil {
		t.Fatal(err)
	}
	if score != 9 {
		t.Fatalf("reopened updated score = %d, want 9", score)
	}
	if _, err := db.Exec(`DROP INDEX by_kind ON docs`); err != nil {
		t.Fatalf("persisted index after reopen: %v", err)
	}
}

func TestAlterTableAddColumnDuplicateAndIfNotExists(t *testing.T) {
	ctx := context.Background()
	database, err := Open(filepath.Join(t.TempDir(), "catalog.vdb"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	session, err := database.NewSession(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	for _, statement := range []string{
		`CREATE TABLE docs (id STRING PRIMARY KEY, kind STRING)`,
		`INSERT INTO docs VALUES ('{"id":"seed","kind":"x"}')`,
	} {
		if err := testRuntimeExec(session, statement, nil); err != nil {
			t.Fatal(err)
		}
	}

	core := database.connector.db
	core.mu.RLock()
	before := core.tables["docs"]
	storage := before.meta.Storage
	core.mu.RUnlock()
	if err := testRuntimeExec(
		session, `ALTER TABLE docs ADD COLUMN kind STRING`, nil,
	); !errors.Is(err, ErrColumnExists) {
		t.Fatalf("duplicate ALTER = %v, want ErrColumnExists", err)
	}
	if err := testRuntimeExec(session,
		`ALTER TABLE docs ADD COLUMN IF NOT EXISTS kind INTEGER NOT NULL`, nil,
	); err != nil {
		t.Fatalf("ALTER IF NOT EXISTS = %v", err)
	}
	core.mu.RLock()
	after := core.tables["docs"]
	core.mu.RUnlock()
	if after != before || after.meta.Storage != storage {
		t.Fatal("duplicate/no-op ALTER replaced the table storage incarnation")
	}

	tables, err := session.Tables(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(tables) != 1 || len(tables[0].Columns) != 2 {
		t.Fatalf("table metadata after no-op = %+v", tables)
	}
	kind := tables[0].Columns[1]
	if kind.Path != "/kind" || kind.Required ||
		kind.Types != sqlast.TypeNull|sqlast.TypeString {
		t.Fatalf("kind metadata after no-op = %+v", kind)
	}
	if err := testRuntimeExec(
		session, `INSERT INTO docs VALUES ('{"id":"missing-kind"}')`, nil,
	); err != nil {
		t.Fatalf("original nullable declaration changed: %v", err)
	}
	if err := testRuntimeExec(
		session, `INSERT INTO docs VALUES ('{"id":"wrong-kind","kind":3}')`, nil,
	); err == nil {
		t.Fatal("IF NOT EXISTS changed the existing column type")
	}
}

func TestAlterTableAddColumnRejectsInvalidExistingRowsAtomically(t *testing.T) {
	for _, test := range []struct {
		name          string
		documents     []string
		alter         string
		afterDocument string
	}{
		{
			name: "wrong existing type",
			documents: []string{
				`{"id":"a","kind":"x","score":"bad"}`,
				`{"id":"b","kind":"x","score":3}`,
			},
			alter:         `ALTER TABLE docs ADD COLUMN score INTEGER`,
			afterDocument: `{"id":"after","kind":"x","score":"still-text"}`,
		},
		{
			name: "missing required value",
			documents: []string{
				`{"id":"a","kind":"x","score":1}`,
				`{"id":"b","kind":"x"}`,
			},
			alter:         `ALTER TABLE docs ADD COLUMN score INTEGER NOT NULL`,
			afterDocument: `{"id":"after","kind":"x"}`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			database, err := Open(filepath.Join(t.TempDir(), "catalog.vdb"))
			if err != nil {
				t.Fatal(err)
			}
			defer database.Close()
			session, err := database.NewSession(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer session.Close()
			for _, statement := range []string{
				`CREATE TABLE docs (PRIMARY KEY (id))`,
				`CREATE INDEX by_kind ON docs(kind)`,
			} {
				if err := testRuntimeExec(session, statement, nil); err != nil {
					t.Fatal(err)
				}
			}
			insert := runtimePrepare(t, session, `INSERT INTO docs VALUES (?), (?)`)
			if _, err := insert.Exec(ctx, []any{test.documents[0], test.documents[1]}); err != nil {
				t.Fatal(err)
			}

			core := database.connector.db
			core.mu.RLock()
			before := core.tables["docs"]
			storage := before.meta.Storage
			collection := before.collection
			core.mu.RUnlock()
			if err := testRuntimeExec(session, test.alter, nil); err == nil {
				t.Fatalf("%s succeeded with incompatible existing rows", test.alter)
			}
			core.mu.RLock()
			after := core.tables["docs"]
			core.mu.RUnlock()
			if after != before || after.collection != collection ||
				after.meta.Storage != storage || after.meta.Schema != nil {
				t.Fatal("failed ALTER changed the table catalog or storage incarnation")
			}
			snapshot, err := after.collection.Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			if got := snapshotDocumentCount(t, snapshot); got != 2 {
				t.Fatalf("rows after failed ALTER = %d, want 2", got)
			}
			if got := snapshotIndexNames(snapshot); len(got) != 1 || got[0] != "by_kind" {
				t.Fatalf("indexes after failed ALTER = %v, want [by_kind]", got)
			}
			if err := snapshot.Close(); err != nil {
				t.Fatal(err)
			}
			if err := testRuntimeExec(
				session, `INSERT INTO docs VALUES (?)`, []any{test.afterDocument},
			); err != nil {
				t.Fatalf("failed ALTER changed write validation: %v", err)
			}
			if err := testRuntimeExec(
				session, `UPDATE docs SET score = 9 WHERE id = 'a'`, nil,
			); err == nil {
				t.Fatal("failed ALTER nevertheless declared the new column")
			}
		})
	}
}

func TestAlterTableAddColumnDeclaresExistingSchemalessValues(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE docs (PRIMARY KEY (id))`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO docs VALUES (?), (?)`,
		`{"id":"a","score":1,"keep":"first"}`,
		`{"id":"b","score":2,"keep":"second"}`,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		`ALTER TABLE docs ADD COLUMN score INTEGER NOT NULL`,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE docs SET score = 8 WHERE id = 'a'`); err != nil {
		t.Fatal(err)
	}
	var score int64
	var keep string
	if err := db.QueryRow(`SELECT score, keep FROM docs WHERE id = 'a'`).Scan(
		&score, &keep,
	); err != nil {
		t.Fatal(err)
	}
	if score != 8 || keep != "first" {
		t.Fatalf("declared existing row = (%d, %q), want (8, first)", score, keep)
	}
	for _, document := range []string{
		`{"id":"missing"}`,
		`{"id":"wrong","score":"bad"}`,
		`{"id":{"nested":true},"score":3}`,
	} {
		if _, err := db.Exec(`INSERT INTO docs VALUES (?)`, document); err == nil {
			t.Fatalf("post-ALTER validation accepted %s", document)
		}
	}
	if _, err := db.Exec(
		`INSERT INTO docs VALUES (?)`, `{"id":"valid","score":3}`,
	); err != nil {
		t.Fatalf("post-ALTER valid insert: %v", err)
	}
}

func TestAlterTableAddColumnOnUnmaterializedTable(t *testing.T) {
	ctx := context.Background()
	database, err := Open(filepath.Join(t.TempDir(), "catalog.vdb"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	session, err := database.NewSession(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	if err := testRuntimeExec(
		session, `CREATE TABLE docs (PRIMARY KEY (id))`, nil,
	); err != nil {
		t.Fatal(err)
	}
	core := database.connector.db
	core.mu.RLock()
	before := core.tables["docs"]
	core.mu.RUnlock()
	if before.collection != nil || before.meta.Materialized {
		t.Fatal("empty CREATE TABLE unexpectedly materialized storage")
	}
	if err := testRuntimeExec(session,
		`ALTER TABLE docs ADD COLUMN score INTEGER NOT NULL`, nil,
	); err != nil {
		t.Fatal(err)
	}
	core.mu.RLock()
	after := core.tables["docs"]
	core.mu.RUnlock()
	if after.collection != nil || after.meta.Materialized || after.meta.Schema == nil {
		t.Fatal("ALTER on empty table did not remain an unmaterialized schema change")
	}
	if err := testRuntimeExec(
		session, `INSERT INTO docs VALUES ('{"id":"valid","score":1}')`, nil,
	); err != nil {
		t.Fatalf("first valid insert after ALTER: %v", err)
	}
	if err := testRuntimeExec(
		session, `INSERT INTO docs VALUES ('{"id":"missing"}')`, nil,
	); err == nil {
		t.Fatal("first materialization lost the added NOT NULL column")
	}
}

func TestAlterTableAddColumnRejectedInTransaction(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE docs (id STRING PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(
		`ALTER TABLE docs ADD COLUMN score INTEGER`,
	); !errors.Is(err, ErrDDLInTransaction) {
		_ = tx.Rollback()
		t.Fatalf("transactional ALTER = %v, want ErrDDLInTransaction", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE docs SET score = 1 WHERE id = 'absent'`); err == nil {
		t.Fatal("rejected transactional ALTER declared the column")
	}
	if _, err := db.Exec(`ALTER TABLE docs ADD COLUMN score INTEGER`); err != nil {
		t.Fatalf("autocommit ALTER after rollback: %v", err)
	}
}

func TestAlterTableAddColumnPreservesOldSnapshotAndConflictsWriter(t *testing.T) {
	ctx := context.Background()
	database, err := Open(filepath.Join(t.TempDir(), "catalog.vdb"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ddlSession, err := database.NewSession(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer ddlSession.Close()
	txSession, err := database.NewSession(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer txSession.Close()
	for _, statement := range []string{
		`CREATE TABLE docs (PRIMARY KEY (id))`,
		`CREATE INDEX by_kind ON docs(kind)`,
		`INSERT INTO docs VALUES ('{"id":"a","kind":"x","score":1}')`,
	} {
		if err := testRuntimeExec(ddlSession, statement, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := txSession.Begin(ctx, TxOptions{}); err != nil {
		t.Fatal(err)
	}
	update := runtimePrepare(t, txSession,
		`UPDATE docs SET "$doc" = ? WHERE id = ?`)
	if _, err := update.Exec(ctx, []any{
		`{"id":"a","kind":"x","score":2}`, "a",
	}); err != nil {
		t.Fatal(err)
	}

	core := database.connector.db
	core.mu.RLock()
	oldTable := core.tables["docs"]
	oldSnapshot, err := oldTable.collection.Snapshot()
	core.mu.RUnlock()
	if err != nil {
		t.Fatal(err)
	}
	defer oldSnapshot.Close()
	if err := testRuntimeExec(
		ddlSession, `ALTER TABLE docs ADD COLUMN score INTEGER`, nil,
	); err != nil {
		t.Fatal(err)
	}
	core.mu.RLock()
	current := core.tables["docs"]
	core.mu.RUnlock()
	if current == oldTable || current.meta.Storage == oldTable.meta.Storage {
		t.Fatal("ALTER did not publish a new storage incarnation")
	}
	if got := snapshotDocumentCount(t, oldSnapshot); got != 1 {
		t.Fatalf("old snapshot row count = %d, want 1", got)
	}
	key, err := primaryScalarKey("a")
	if err != nil {
		t.Fatal(err)
	}
	raw, found, err := oldSnapshot.AppendRaw(nil, []byte(key))
	if err != nil || !found || !strings.Contains(string(raw), `"score":1`) {
		t.Fatalf("old snapshot row = (%s, %v, %v)", raw, found, err)
	}
	if err := txSession.Commit(ctx); !errors.Is(err, ErrTransactionConflict) {
		t.Fatalf("pre-ALTER transaction commit = %v, want ErrTransactionConflict", err)
	}
	currentSnapshot, err := current.collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer currentSnapshot.Close()
	raw, found, err = currentSnapshot.AppendRaw(nil, []byte(key))
	if err != nil || !found || !strings.Contains(string(raw), `"score":1`) {
		t.Fatalf("current row after rejected transaction = (%s, %v, %v)", raw, found, err)
	}
	if got := snapshotIndexNames(currentSnapshot); len(got) != 1 || got[0] != "by_kind" {
		t.Fatalf("current indexes = %v, want [by_kind]", got)
	}
}
