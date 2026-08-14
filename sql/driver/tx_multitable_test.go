package driver

import (
	stdsql "database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"unsafe"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store/durable"
)

func TestMultiTableTransactionCommitRollbackAndConflict(t *testing.T) {
	db := openTestDB(t)
	for _, ddl := range []string{
		`CREATE TABLE a (PRIMARY KEY (id))`,
		`CREATE TABLE b (PRIMARY KEY (id))`,
	} {
		if _, err := db.Exec(ddl); err != nil {
			t.Fatal(err)
		}
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO a VALUES (?)`, `{"id":"1","v":"a"}`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO b VALUES (?)`, `{"id":"1","v":"b"}`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	assertTableCount(t, db, "a", 0)
	assertTableCount(t, db, "b", 0)

	tx, err = db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO a VALUES (?)`, `{"id":"1","v":"a"}`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO b VALUES (?)`, `{"id":"1","v":"b"}`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	assertTableField(t, db, "a", "1", "v", "a")
	assertTableField(t, db, "b", "1", "v", "b")

	blocker, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Rollback()
	if _, err := blocker.Exec(
		`UPDATE a SET "$doc" = ? WHERE id = ?`,
		`{"id":"1","v":"blocked"}`, "1",
	); err != nil {
		t.Fatal(err)
	}

	conflict, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conflict.Exec(
		`UPDATE a SET "$doc" = ? WHERE id = ?`,
		`{"id":"1","v":"loser"}`, "1",
	); err != nil {
		t.Fatal(err)
	}
	if _, err := conflict.Exec(
		`UPDATE b SET "$doc" = ? WHERE id = ?`,
		`{"id":"1","v":"loser-b"}`, "1",
	); err != nil {
		t.Fatal(err)
	}
	if err := blocker.Commit(); err != nil {
		t.Fatal(err)
	}
	err = conflict.Commit()
	if !errors.Is(err, ErrTransactionConflict) {
		t.Fatalf("conflict commit = %v, want ErrTransactionConflict", err)
	}
	assertTableField(t, db, "a", "1", "v", "blocked")
	assertTableField(t, db, "b", "1", "v", "b")
}

func TestCommittedMultiTableTransactionDropReopens(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.vdb")
	db, err := stdsql.Open("vibedb", path)
	if err != nil {
		t.Fatal(err)
	}
	for _, ddl := range []string{
		`CREATE TABLE a (PRIMARY KEY (id))`,
		`CREATE TABLE b (PRIMARY KEY (id))`,
	} {
		if _, err := db.Exec(ddl); err != nil {
			t.Fatal(err)
		}
	}
	commitSQLTxnPair(t, db, "a", "b", "committed")
	if _, err := db.Exec(`DROP TABLE a`); err != nil {
		t.Fatalf("DROP after multi-table commit: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = stdsql.Open("vibedb", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	assertTableCount(t, db, "b", 1)
	if err := db.QueryRow(`SELECT COUNT(*) FROM a`).Scan(new(int)); !errors.Is(err, ErrTableNotFound) {
		t.Fatalf("dropped participant after reopen = %v, want ErrTableNotFound", err)
	}
}

func TestMultiTableCommitAfterDDLCollectionReplacement(t *testing.T) {
	for _, test := range []struct {
		name string
		ddl  string
	}{
		{name: "drop", ddl: `DROP TABLE c`},
		{name: "truncate replacement", ddl: `TRUNCATE TABLE c`},
	} {
		t.Run(test.name, func(t *testing.T) {
			db := openTestDB(t)
			for _, ddl := range []string{
				`CREATE TABLE a (PRIMARY KEY (id))`,
				`CREATE TABLE b (PRIMARY KEY (id))`,
				`CREATE TABLE c (PRIMARY KEY (id))`,
			} {
				if _, err := db.Exec(ddl); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := db.Exec(`INSERT INTO c VALUES (?)`, `{"id":"seed"}`); err != nil {
				t.Fatal(err)
			}
			commitSQLTxnPair(t, db, "a", "b", "before-ddl")
			if _, err := db.Exec(test.ddl); err != nil {
				t.Fatalf("%s: %v", test.ddl, err)
			}
			commitSQLTxnPair(t, db, "a", "b", "after-ddl")
			assertTableCount(t, db, "a", 2)
			assertTableCount(t, db, "b", 2)
		})
	}
}

func TestDDLTransactionLogRegistrationRollsBackOnDefiniteCatalogFailure(
	t *testing.T,
) {
	for _, test := range []struct {
		name string
		ddl  string
	}{
		{name: "drop", ddl: `DROP TABLE c`},
		{name: "truncate replacement", ddl: `TRUNCATE TABLE c`},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "catalog.vdb")
			database, err := openDatabase(path)
			if err != nil {
				t.Fatal(err)
			}
			db := stdsql.OpenDB(&dbConnector{db: database})
			t.Cleanup(func() { _ = db.Close() })
			for _, ddl := range []string{
				`CREATE TABLE a (PRIMARY KEY (id))`,
				`CREATE TABLE b (PRIMARY KEY (id))`,
				`CREATE TABLE c (PRIMARY KEY (id))`,
			} {
				if _, err := db.Exec(ddl); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := db.Exec(`INSERT INTO c VALUES (?)`, `{"id":"keep"}`); err != nil {
				t.Fatal(err)
			}
			commitSQLTxnPair(t, db, "a", "b", "before-failure")

			database.mu.Lock()
			originalPath := database.path
			database.path = filepath.Join(t.TempDir(), "missing", "catalog.vdb")
			database.mu.Unlock()
			_, ddlErr := db.Exec(test.ddl)
			database.mu.Lock()
			database.path = originalPath
			rolledBack := database.tables["c"].collection
			database.mu.Unlock()
			if ddlErr == nil || errors.Is(ddlErr, durable.ErrCommitOutcomeUnknown) {
				t.Fatalf("definitely unpublished %s = %v", test.ddl, ddlErr)
			}
			if !txnLogHasCollection(database.txnLog, rolledBack) {
				t.Fatal("definite catalog failure did not re-register old collection")
			}
			assertTableCount(t, db, "c", 1)
			commitSQLTxnPair(t, db, "a", "b", "after-failure")
			assertTableCount(t, db, "a", 2)
			assertTableCount(t, db, "b", 2)
		})
	}
}

func commitSQLTxnPair(
	t *testing.T, db *stdsql.DB, left, right, id string,
) {
	t.Helper()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	document := fmt.Sprintf(`{"id":%q}`, id)
	if _, err := tx.Exec(`INSERT INTO `+left+` VALUES (?)`, document); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO `+right+` VALUES (?)`, document); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestMultiTableTransactionTooLarge(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.vdb")
	database, err := openDatabase(path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.close()
	database.txnLimits = durable.TxnLimits{
		MaxCollections: 1,
		MaxDocuments:   64,
		MaxBytes:       1 << 20,
	}

	db := stdsql.OpenDB(&dbConnector{db: database})
	defer db.Close()
	for _, ddl := range []string{
		`CREATE TABLE a (PRIMARY KEY (id))`,
		`CREATE TABLE b (PRIMARY KEY (id))`,
	} {
		if _, err := db.Exec(ddl); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec(`INSERT INTO a VALUES (?)`, `{"id":"seed-a"}`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO b VALUES (?)`, `{"id":"seed-b"}`); err != nil {
		t.Fatal(err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`INSERT INTO a VALUES (?)`, `{"id":"1"}`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO b VALUES (?)`, `{"id":"1"}`); err != nil {
		t.Fatal(err)
	}
	err = tx.Commit()
	if !errors.Is(err, ErrTransactionTooLarge) {
		t.Fatalf("commit = %v, want ErrTransactionTooLarge", err)
	}
}

func TestMultiTableEmptyAtBeginAllOrNothingOnMintFault(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE a (PRIMARY KEY (id))`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO a VALUES (?)`, `{"id":"keep"}`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE b (PRIMARY KEY (id))`); err != nil {
		t.Fatal(err)
	}

	storeio.ProgramTxnMarkerCreateFault(storeio.TxnMarkerFaultPlan{
		Phase: storeio.TxnMarkerFaultCreateFileSync,
	})
	t.Cleanup(func() {
		storeio.ProgramTxnMarkerCreateFault(storeio.TxnMarkerFaultPlan{})
	})

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(
		`UPDATE a SET "$doc" = ? WHERE id = ?`,
		`{"id":"keep","v":"new"}`, "keep",
	); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO b VALUES (?)`, `{"id":"1","v":"b"}`); err != nil {
		t.Fatal(err)
	}
	err = tx.Commit()
	if err == nil {
		t.Fatal("mint-faulted multi-table commit succeeded")
	}
	if !storeio.TxnMarkerCreateFaulted() {
		t.Fatal("create-fence fault did not fire")
	}
	assertTableField(t, db, "a", "keep", "id", "keep")
	assertTableCount(t, db, "b", 0)
}

func TestMultiTableDecisionSyncUnknownOutcomePoisonsCatalog(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.vdb")
	database, err := openDatabase(path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.close()

	db := stdsql.OpenDB(&dbConnector{db: database})
	defer db.Close()
	for _, ddl := range []string{
		`CREATE TABLE a (PRIMARY KEY (id))`,
		`CREATE TABLE b (PRIMARY KEY (id))`,
	} {
		if _, err := db.Exec(ddl); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec(`INSERT INTO a VALUES (?)`, `{"id":"seed-a"}`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO b VALUES (?)`, `{"id":"seed-b"}`); err != nil {
		t.Fatal(err)
	}

	var fault *storeio.FaultTxnMarker
	setDatabaseTxnAfterMintHook(t, func(log *durable.TxnLog) {
		marker := txnLogMarker(log)
		fault = storeio.NewFaultTxnMarker(marker)
		fault.Program(storeio.TxnMarkerFaultPlan{
			Phase:     storeio.TxnMarkerFaultSyncError,
			SyncIndex: 0,
		})
	})

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO a VALUES (?)`, `{"id":"1","v":"a"}`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO b VALUES (?)`, `{"id":"1","v":"b"}`); err != nil {
		t.Fatal(err)
	}
	err = tx.Commit()
	if !errors.Is(err, durable.ErrCommitOutcomeUnknown) {
		t.Fatalf("commit = %v, want ErrCommitOutcomeUnknown", err)
	}
	if fault == nil || !fault.Faulted() {
		t.Fatal("decision sync fault did not fire")
	}

	_, writeErr := db.Exec(`INSERT INTO a VALUES (?)`, `{"id":"2"}`)
	if writeErr == nil || !errors.Is(writeErr, durable.ErrCommitOutcomeUnknown) {
		t.Fatalf("post-unknown write = %v, want catalog poison", writeErr)
	}

	_ = db.Close()
	_ = database.close()

	reopened, err := openDatabase(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.close()
	aHas := collectionHasKey(t, reopened.tables["a"].collection, "1")
	bHas := collectionHasKey(t, reopened.tables["b"].collection, "1")
	if aHas != bHas {
		t.Fatalf("torn reopen: a=%v b=%v", aHas, bHas)
	}
}

func TestSingleTableCommitDoesNotEngageDecisionLog(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.vdb")
	db, err := stdsql.Open("vibedb", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE docs (PRIMARY KEY (id))`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO docs VALUES (?)`, `{"id":"seed"}`); err != nil {
		t.Fatal(err)
	}
	before := readSoleJournal(t, path+".tables")
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO docs VALUES (?)`, `{"id":"1","v":1}`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	after := readSoleJournal(t, path+".tables")
	if _, err := os.Stat(filepath.Join(path+".tables", "txn.vtm")); !os.IsNotExist(err) {
		t.Fatalf("single-table commit created txn.vtm: %v", err)
	}
	// The journal region is preallocated, so length is stable; content must
	// still change, and the decision log must stay unminted — the single-table
	// path is Collection.Update / kind-3, not UpdateCollections / kind-4.
	if string(after) == string(before) {
		t.Fatal("single-table commit left journal contents unchanged")
	}
}

func assertTableCount(t *testing.T, db *stdsql.DB, table string, want int) {
	t.Helper()
	var got int
	if err := db.QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("table %s count = %d, want %d", table, got, want)
	}
}

func assertTableDoc(t *testing.T, db *stdsql.DB, table, id, want string) {
	t.Helper()
	var got string
	err := db.QueryRow(
		`SELECT * FROM `+table+` WHERE id = ?`, id,
	).Scan(&got)
	if err != nil {
		t.Fatalf("table %s id %s: %v", table, id, err)
	}
	if got != want {
		t.Fatalf("table %s id %s = %s, want %s", table, id, got, want)
	}
}

func assertTableField(t *testing.T, db *stdsql.DB, table, id, field, want string) {
	t.Helper()
	var got string
	err := db.QueryRow(
		`SELECT `+field+` FROM `+table+` WHERE id = ?`, id,
	).Scan(&got)
	if err != nil {
		t.Fatalf("table %s id %s field %s: %v", table, id, field, err)
	}
	if got != want {
		t.Fatalf("table %s id %s.%s = %s, want %s", table, id, field, got, want)
	}
}

func collectionHasKey(t *testing.T, c *durable.Collection, key string) bool {
	t.Helper()
	snap, err := c.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Close()
	_, found, err := snap.AppendRaw(nil, []byte(`"`+key+`"`))
	if err != nil {
		t.Fatal(err)
	}
	return found
}

func readSoleJournal(t *testing.T, dir string) []byte {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var journals []string
	for _, entry := range entries {
		name := entry.Name()
		if name == "txn.vtm" || entry.IsDir() {
			continue
		}
		full := filepath.Join(dir, name)
		journal := durable.RecoveryJournalPath(full)
		if info, statErr := os.Stat(journal); statErr == nil && info.Mode().IsRegular() {
			journals = append(journals, journal)
		}
	}
	if len(journals) != 1 {
		t.Fatalf("want one journal under %s, found %v", dir, journals)
	}
	raw, err := os.ReadFile(journals[0])
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func txnLogMarker(log *durable.TxnLog) *storeio.TxnMarker {
	v := reflect.ValueOf(log).Elem().FieldByName("marker")
	return *(**storeio.TxnMarker)(unsafe.Pointer(v.UnsafeAddr()))
}

func txnLogHasCollection(
	log *durable.TxnLog, collection *durable.Collection,
) bool {
	v := reflect.ValueOf(log).Elem().FieldByName("collections")
	collections := *(*[]*durable.Collection)(unsafe.Pointer(v.UnsafeAddr()))
	for _, registered := range collections {
		if registered == collection {
			return true
		}
	}
	return false
}
