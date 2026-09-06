package driver

import (
	"context"
	stdsql "database/sql"
	sqldriver "database/sql/driver"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/query"
	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
	vibejson "github.com/thesyncim/vibejson"
)

type transactionBinaryKeys [][]byte

func (keys transactionBinaryKeys) Len() int             { return len(keys) }
func (keys transactionBinaryKeys) Key(index int) []byte { return keys[index] }

// Transaction reads include their own staged inserts and replacements.
func TestTransactionSelectReadsStagedInsertAndUpdate(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE docs (PRIMARY KEY (id))`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO docs VALUES (?)`, `{"id":"base","name":"before"}`); err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`INSERT INTO docs VALUES (?)`, `{"id":"new","name":"inserted"}`); err != nil {
		t.Fatal(err)
	}
	var name string
	if err := tx.QueryRow(`SELECT name FROM docs WHERE id = ?`, "new").Scan(&name); err != nil {
		t.Fatal(err)
	}
	if name != "inserted" {
		t.Fatalf("staged INSERT name = %q", name)
	}
	if _, err := tx.Exec(`UPDATE docs SET "$doc" = ? WHERE id = ?`,
		`{"id":"base","name":"updated"}`, "base"); err != nil {
		t.Fatal(err)
	}
	if err := tx.QueryRow(`SELECT name FROM docs WHERE id = ?`, "base").Scan(&name); err != nil {
		t.Fatal(err)
	}
	if name != "updated" {
		t.Fatalf("staged UPDATE name = %q", name)
	}
}

// This overlay-correctness case deliberately uses a scan predicate: it proves a
// WHERE-filtered read inside a transaction reflects pending updates, deletes,
// and inserts over the base rows without making index candidate pruning part of
// the fixture.
func TestTransactionOverlayCorrectsFilteredBaseCandidates(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`
		CREATE TABLE docs (
			id STRING PRIMARY KEY,
			active BOOL NOT NULL
		)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		`INSERT INTO docs VALUES (?), (?), (?)`,
		`{"id":"a","active":false}`,
		`{"id":"b","active":true}`,
		`{"id":"c","active":true}`,
	); err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(
		`UPDATE docs SET "$doc" = ? WHERE id = ?`,
		`{"id":"a","active":true}`, "a",
	); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`DELETE FROM docs WHERE id = ?`, "b"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(
		`UPDATE docs SET "$doc" = ? WHERE id = ?`,
		`{"id":"c","active":false}`, "c",
	); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(
		`INSERT INTO docs VALUES (?)`, `{"id":"d","active":true}`,
	); err != nil {
		t.Fatal(err)
	}

	rows, err := tx.Query(
		`SELECT id FROM docs WHERE active = TRUE ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		got = append(got, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(got) != "[a d]" {
		t.Fatalf("filtered overlay rows = %v, want [a d]", got)
	}
}

func TestTransactionUnmaterializedTableUsesPendingView(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`
		CREATE TABLE docs (
			id STRING PRIMARY KEY,
			state STRING NOT NULL
		)`); err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(
		`INSERT INTO docs VALUES (?)`, `{"id":"new","state":"draft"}`,
	); err != nil {
		t.Fatal(err)
	}
	var state string
	if err := tx.QueryRow(
		`SELECT state FROM docs WHERE state = 'draft'`,
	).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "draft" {
		t.Fatalf("pending state = %q, want draft", state)
	}
	updated, err := tx.Exec(
		`UPDATE docs SET "$doc" = ? WHERE state = 'draft'`,
		`{"id":"new","state":"ready"}`,
	)
	if err != nil {
		t.Fatal(err)
	}
	if n, _ := updated.RowsAffected(); n != 1 {
		t.Fatalf("updated %d rows, want 1", n)
	}
	deleted, err := tx.Exec(`DELETE FROM docs WHERE state = 'ready'`)
	if err != nil {
		t.Fatal(err)
	}
	if n, _ := deleted.RowsAffected(); n != 1 {
		t.Fatalf("deleted %d rows, want 1", n)
	}
	var count int64
	if err := tx.QueryRow(`SELECT COUNT(*) FROM docs`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("pending row count = %d, want 0", count)
	}
}

func TestTransactionRefusesExecWhileRowsBorrowWorkspace(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE docs (PRIMARY KEY (id))`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		`INSERT INTO docs VALUES (?), (?)`,
		`{"id":"a"}`, `{"id":"b"}`,
	); err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	rows, err := tx.Query(`SELECT id FROM docs ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("query returned no first row")
	}
	var first string
	if err := rows.Scan(&first); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`DELETE FROM docs WHERE id = ?`, "a"); err == nil ||
		!strings.Contains(err.Error(), "close the current rows") {
		t.Fatalf("Exec with rows open = %v, want lifecycle error", err)
	}
	if !rows.Next() {
		t.Fatal("rejected Exec invalidated the remaining query rows")
	}
	var second string
	if err := rows.Scan(&second); err != nil {
		t.Fatal(err)
	}
	if first != "a" || second != "b" {
		t.Fatalf("rows after rejected Exec = [%s %s], want [a b]", first, second)
	}
}

func TestFailedFirstTransactionCommitMayLeaveEmptyTableResidue(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.vdb")
	database, err := openDatabase(path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.close()

	create, err := query.PrepareDML(`CREATE TABLE docs (PRIMARY KEY (id))`)
	if err != nil {
		t.Fatal(err)
	}
	defer create.Release()
	database.mu.Lock()
	_, err = database.createTableLocked(create)
	database.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}

	connection := &conn{db: database}
	database.mu.Lock()
	conflicts := &database.tables["docs"].conflicts
	conflictRevision := conflicts.begin()
	database.mu.Unlock()
	state := &txTable{
		pending: map[string]*txMutation{
			`"broken"`: {document: []byte(`{"id":"broken"`)},
		},
		order:            []string{`"broken"`},
		conflicts:        conflicts,
		conflictRevision: conflictRevision,
	}
	state.name = "docs"
	transaction := &tx{
		conn: connection, tables: map[string]*txTable{"docs": state},
	}
	connection.tx = transaction
	if err := transaction.Commit(); err == nil {
		t.Fatal("invalid first transactional publication succeeded")
	}

	// COMMIT materializes absent tables empty before applying staged seeds. A
	// failed apply can leave that empty durable file as documented residue; it
	// must not retain the rejected document.
	database.mu.RLock()
	collection := database.tables["docs"].collection
	database.mu.RUnlock()
	if collection != nil {
		snap, snapErr := collection.Snapshot()
		if snapErr != nil {
			t.Fatal(snapErr)
		}
		defer snap.Close()
		if _, found, lookErr := snap.AppendRaw(nil, []byte(`"broken"`)); lookErr != nil {
			t.Fatal(lookErr)
		} else if found {
			t.Fatal("failed first transaction retained the rejected document")
		}
	}

	index, err := query.PrepareDML(`CREATE INDEX ON docs (kind)`)
	if err != nil {
		t.Fatal(err)
	}
	defer index.Release()
	database.mu.Lock()
	_, err = database.createIndexLocked(index)
	database.mu.Unlock()
	if err != nil {
		t.Fatalf("CREATE INDEX after failed first transaction: %v", err)
	}
}

// The BEGIN snapshot excludes later replacements and phantoms.
func TestTransactionRepeatableReadsExcludeConcurrentPhantoms(t *testing.T) {
	db := openTestDB(t)
	db.SetMaxOpenConns(4)
	if _, err := db.Exec(`CREATE TABLE docs (PRIMARY KEY (id))`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO docs VALUES (?)`, `{"id":"base","kind":"x","name":"before"}`); err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(context.Background(), &stdsql.TxOptions{
		Isolation: stdsql.LevelRepeatableRead,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := db.Exec(`UPDATE docs SET "$doc" = ? WHERE id = ?`,
		`{"id":"base","kind":"x","name":"outside"}`, "base"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO docs VALUES (?)`, `{"id":"phantom","kind":"x"}`); err != nil {
		t.Fatal(err)
	}
	var name string
	if err := tx.QueryRow(`SELECT name FROM docs WHERE id = ?`, "base").Scan(&name); err != nil {
		t.Fatal(err)
	}
	if name != "before" {
		t.Fatalf("point read = %q, want begin snapshot", name)
	}
	var count int64
	if err := tx.QueryRow(`SELECT COUNT(*) FROM docs`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("transaction count = %d, want 1", count)
	}
}

// A conflicting transaction publishes none of its otherwise-disjoint batch.
func TestTransactionConcurrentConflictPublishesNothing(t *testing.T) {
	db := openTestDB(t)
	db.SetMaxOpenConns(4)
	if _, err := db.Exec(`CREATE TABLE docs (PRIMARY KEY (id))`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO docs VALUES (?)`, `{"id":"base","writer":"initial"}`); err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`UPDATE docs SET "$doc" = ? WHERE id = ?`,
		`{"id":"base","writer":"tx"}`, "base"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO docs VALUES (?)`, `{"id":"atomic"}`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE docs SET "$doc" = ? WHERE id = ?`,
		`{"id":"base","writer":"winner"}`, "base"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); !errors.Is(err, ErrTransactionConflict) {
		t.Fatalf("Commit = %v, want ErrTransactionConflict", err)
	}
	var count int64
	if err := db.QueryRow(`SELECT COUNT(*) FROM docs WHERE id = ?`, "atomic").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatal("non-conflicting member of rejected batch was published")
	}
}

// A transaction is bound to the exact catalog incarnation captured at BEGIN,
// even when that table had not materialized a durable file yet. Without the
// identity check, DROP followed by same-name CREATE could redirect the staged
// batch into a logically unrelated table.
func TestTransactionCannotCommitIntoSameNameReplacement(t *testing.T) {
	db := openTestDB(t)
	db.SetMaxOpenConns(2)
	if _, err := db.Exec(`CREATE TABLE docs (PRIMARY KEY (id))`); err != nil {
		t.Fatal(err)
	}

	transaction, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := transaction.Exec(
		`INSERT INTO docs VALUES (?)`, `{"id":"stale"}`,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DROP TABLE docs`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE docs (PRIMARY KEY (id))`); err != nil {
		t.Fatal(err)
	}
	if err := transaction.Commit(); !errors.Is(err, ErrTransactionConflict) {
		t.Fatalf("Commit into replacement = %v, want ErrTransactionConflict", err)
	}

	var count int64
	if err := db.QueryRow(`SELECT COUNT(*) FROM docs`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("replacement row count = %d, want 0", count)
	}
}

// Two transactions opened concurrently from one database/sql handle conflict
// when they replace the same key.
func TestConcurrentTransactionsOnOneHandle(t *testing.T) {
	db := openTestDB(t)
	db.SetMaxOpenConns(4)
	if _, err := db.Exec(`CREATE TABLE docs (PRIMARY KEY (id))`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO docs VALUES (?)`, `{"id":"base","writer":"initial"}`); err != nil {
		t.Fatal(err)
	}
	first, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	second, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Exec(`UPDATE docs SET "$doc" = ? WHERE id = ?`,
		`{"id":"base","writer":"first"}`, "base"); err != nil {
		t.Fatal(err)
	}
	if _, err := second.Exec(`UPDATE docs SET "$doc" = ? WHERE id = ?`,
		`{"id":"base","writer":"second"}`, "base"); err != nil {
		t.Fatal(err)
	}
	if err := first.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := second.Commit(); !errors.Is(err, ErrTransactionConflict) {
		t.Fatalf("second Commit = %v, want ErrTransactionConflict", err)
	}
}

func TestTransactionRejectsABAWriteConflict(t *testing.T) {
	db := openTestDB(t)
	db.SetMaxOpenConns(4)
	if _, err := db.Exec(`CREATE TABLE docs (PRIMARY KEY (id))`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		`INSERT INTO docs VALUES (?)`,
		`{"id":"base","writer":"initial"}`,
	); err != nil {
		t.Fatal(err)
	}

	transaction, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := transaction.Exec(
		`UPDATE docs SET "$doc" = ? WHERE id = ?`,
		`{"id":"base","writer":"transaction"}`, "base",
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		`UPDATE docs SET "$doc" = ? WHERE id = ?`,
		`{"id":"base","writer":"away"}`, "base",
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		`UPDATE docs SET "$doc" = ? WHERE id = ?`,
		`{"id":"base","writer":"initial"}`, "base",
	); err != nil {
		t.Fatal(err)
	}
	if err := transaction.Commit(); !errors.Is(err, ErrTransactionConflict) {
		t.Fatalf("Commit after X -> Y -> X = %v, want ErrTransactionConflict", err)
	}

	var writer string
	if err := db.QueryRow(
		`SELECT writer FROM docs WHERE id = ?`, "base",
	).Scan(&writer); err != nil {
		t.Fatal(err)
	}
	if writer != "initial" {
		t.Fatalf("writer after rejected ABA commit = %q, want initial", writer)
	}
}

func TestTransactionRejectsAbsentInsertDeleteABA(t *testing.T) {
	db := openTestDB(t)
	db.SetMaxOpenConns(4)
	if _, err := db.Exec(`CREATE TABLE docs (PRIMARY KEY (id))`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		`INSERT INTO docs VALUES (?)`, `{"id":"materialized"}`,
	); err != nil {
		t.Fatal(err)
	}

	transaction, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := transaction.Exec(
		`INSERT INTO docs VALUES (?)`, `{"id":"transient","writer":"transaction"}`,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		`INSERT INTO docs VALUES (?)`, `{"id":"transient","writer":"away"}`,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		`DELETE FROM docs WHERE id = ?`, "transient",
	); err != nil {
		t.Fatal(err)
	}
	if err := transaction.Commit(); !errors.Is(err, ErrTransactionConflict) {
		t.Fatalf(
			"Commit after absent -> present -> absent = %v, want ErrTransactionConflict",
			err,
		)
	}

	var count int64
	if err := db.QueryRow(
		`SELECT COUNT(*) FROM docs WHERE id = ?`, "transient",
	).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("transient row count after rejected ABA commit = %d, want 0", count)
	}
}

func TestConcurrentTransactionsCommitDisjointKeys(t *testing.T) {
	db := openTestDB(t)
	db.SetMaxOpenConns(4)
	if _, err := db.Exec(`CREATE TABLE docs (PRIMARY KEY (id))`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		`INSERT INTO docs VALUES (?), (?)`,
		`{"id":"a","writer":"initial"}`,
		`{"id":"b","writer":"initial"}`,
	); err != nil {
		t.Fatal(err)
	}

	first, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	second, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Exec(
		`UPDATE docs SET "$doc" = ? WHERE id = ?`,
		`{"id":"a","writer":"first"}`, "a",
	); err != nil {
		t.Fatal(err)
	}
	if _, err := second.Exec(
		`UPDATE docs SET "$doc" = ? WHERE id = ?`,
		`{"id":"b","writer":"second"}`, "b",
	); err != nil {
		t.Fatal(err)
	}
	if err := first.Commit(); err != nil {
		t.Fatalf("first disjoint Commit: %v", err)
	}
	if err := second.Commit(); err != nil {
		t.Fatalf("second disjoint Commit: %v", err)
	}

	var firstWriter, secondWriter string
	if err := db.QueryRow(
		`SELECT writer FROM docs WHERE id = ?`, "a",
	).Scan(&firstWriter); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(
		`SELECT writer FROM docs WHERE id = ?`, "b",
	).Scan(&secondWriter); err != nil {
		t.Fatal(err)
	}
	if firstWriter != "first" || secondWriter != "second" {
		t.Fatalf(
			"disjoint writers = (%q, %q), want (first, second)",
			firstWriter, secondWriter,
		)
	}
}

func TestTransactionConflictClockIsHardBounded(t *testing.T) {
	var clock txConflictClock
	old := clock.begin()
	for i := 0; i <= txConflictHistoryKeys; i++ {
		clock.recordKeys([]string{fmt.Sprintf("key-%d", i)})
		if len(clock.writes) > txConflictHistoryKeys {
			t.Fatalf(
				"conflict history grew to %d keys, limit %d",
				len(clock.writes), txConflictHistoryKeys,
			)
		}
	}
	if _, overflow, conflict := clock.conflict(old, []string{"untouched"}); !conflict || !overflow {
		t.Fatalf(
			"pre-overflow transaction conflict = (overflow %v, conflict %v), want true, true",
			overflow, conflict,
		)
	}

	afterOverflow := clock.begin()
	clock.recordKeys([]string{"later"})
	key, overflow, conflict := clock.conflict(
		afterOverflow, []string{"later"},
	)
	if !conflict || overflow || key != "later" {
		t.Fatalf(
			"post-overflow exact conflict = (%q, overflow %v, conflict %v)",
			key, overflow, conflict,
		)
	}
	clock.finish(old)
	if len(clock.writes) != 1 {
		t.Fatalf("history after doomed transaction finished = %d, want 1", len(clock.writes))
	}
	clock.finish(afterOverflow)
	if clock.writes != nil || clock.active != nil {
		t.Fatalf(
			"last transaction retained history: writes %d active %d",
			len(clock.writes), len(clock.active),
		)
	}
}

func TestTransactionConflictClockInactiveWriteIsAllocationFree(t *testing.T) {
	var clock txConflictClock
	if !clock.recordWriteIfNoActive() {
		t.Fatal("inactive clock requested exact keys")
	}
	before := clock.observe()
	if allocations := testing.AllocsPerRun(1000, func() {
		if !clock.recordWriteIfNoActive() {
			panic("inactive clock requested exact keys")
		}
	}); allocations != 0 {
		t.Fatalf("inactive clock write allocations = %.2f", allocations)
	}
	if clock.observe() <= before || clock.writes != nil || clock.active != nil {
		t.Fatalf("inactive clock = revision %d writes %v active %v",
			clock.observe(), clock.writes, clock.active)
	}
	active := clock.begin()
	if clock.recordWriteIfNoActive() {
		t.Fatal("active clock omitted exact keys")
	}
	keys := transactionBinaryKeys{[]byte("binary-a"), []byte("binary-b")}
	clock.recordBinary(&keys)
	if allocations := testing.AllocsPerRun(1000, func() {
		clock.recordBinary(&keys)
	}); allocations != 0 {
		t.Fatalf("active binary clock allocations = %.2f", allocations)
	}
	if key, overflow, conflict := clock.conflict(
		active, []string{"binary-b"},
	); !conflict || overflow || key != "binary-b" {
		t.Fatalf("active binary conflict = %q, %v, %v", key, overflow, conflict)
	}
	clock.finish(active)
}

// Rollback discards rows that were visible through the transaction overlay.
func TestTransactionRollbackDiscardsSelectedOverlay(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE docs (PRIMARY KEY (id))`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO docs VALUES (?)`, `{"id":"seed"}`); err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO docs VALUES (?)`, `{"id":"rolled","name":"inside"}`); err != nil {
		t.Fatal(err)
	}
	var name string
	if err := tx.QueryRow(`SELECT name FROM docs WHERE id = ?`, "rolled").Scan(&name); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT name FROM docs WHERE id = ?`, "rolled").Scan(&name); !errors.Is(err, stdsql.ErrNoRows) {
		t.Fatalf("read after rollback = %v, want sql.ErrNoRows", err)
	}
}

// Commit and rollback both release the transaction's leased snapshot.
func TestTransactionCommitAndRollbackReleaseSnapshot(t *testing.T) {
	db := openTestDB(t)
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`CREATE TABLE docs (PRIMARY KEY (id))`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO docs VALUES (?)`, `{"id":"seed"}`); err != nil {
		t.Fatal(err)
	}
	rolled, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if err := rolled.Rollback(); err != nil {
		t.Fatal(err)
	}
	committed, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := committed.Exec(`INSERT INTO docs VALUES (?)`, `{"id":"committed"}`); err != nil {
		t.Fatal(err)
	}
	if err := committed.Commit(); err != nil {
		t.Fatal(err)
	}
	after, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if err := after.Rollback(); err != nil {
		t.Fatal(err)
	}
}

// Only the isolation levels whose semantics the driver implements are accepted.
func TestTransactionIsolationLevelContract(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE docs (PRIMARY KEY (id))`); err != nil {
		t.Fatal(err)
	}
	serializable, err := db.BeginTx(context.Background(),
		&stdsql.TxOptions{Isolation: stdsql.LevelSerializable})
	if err != nil {
		t.Fatal(err)
	}
	if err := serializable.Rollback(); err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(context.Background(),
		&stdsql.TxOptions{Isolation: stdsql.LevelSnapshot})
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
}

// Transaction staging refuses to exceed the captured durable batch bound.
func TestTransactionRefusesToExceedTheBatchBound(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE docs (PRIMARY KEY (id))`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO docs VALUES (?)`, `{"id":"seed"}`); err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	for i := 0; i < 64; i++ {
		if _, err := tx.Exec(`INSERT INTO docs VALUES (?)`,
			fmt.Sprintf(`{"id":"tx-%02d"}`, i)); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}
	_, err = tx.Exec(`INSERT INTO docs VALUES (?)`, `{"id":"overflow"}`)
	if !errors.Is(err, ErrTransactionTooLarge) || !errors.Is(err, durable.ErrBatchTooLarge) {
		t.Fatalf("overflow = %v, want typed driver and durable batch errors", err)
	}
}

// Prepared statements retain their lifecycle across transactions.
func TestPreparedStatementReuseAcrossTransactions(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE docs (PRIMARY KEY (id))`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO docs VALUES (?)`, `{"id":"a","n":1}`); err != nil {
		t.Fatal(err)
	}
	prepared, err := db.Prepare(`SELECT n FROM docs WHERE id = ?`)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	for i := 0; i < 2; i++ {
		tx, err := db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		var n int64
		if err := tx.Stmt(prepared).QueryRow("a").Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 1 {
			t.Fatalf("transaction %d read %d", i, n)
		}
		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
	}
}

// A closed prepared statement is terminal.
func TestStatementAfterClose(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE docs (PRIMARY KEY (id))`); err != nil {
		t.Fatal(err)
	}
	statement, err := db.Prepare(`SELECT * FROM docs`)
	if err != nil {
		t.Fatal(err)
	}
	if err := statement.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := statement.Query(); err == nil {
		t.Fatal("Query after statement Close succeeded")
	}
}

// Driver values preserve exact large integers and every JSON value kind.
func TestDriverValueRoundTrips(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE docs (PRIMARY KEY (id))`); err != nil {
		t.Fatal(err)
	}
	raw := `{"id":"values","nil":null,"bool":true,"int":9007199254740993,"float":0.1,"string":"hi","array":[1,2]}`
	if _, err := db.Exec(`INSERT INTO docs VALUES (?)`, raw); err != nil {
		t.Fatal(err)
	}
	var nilValue any
	var boolean bool
	var integer int64
	var decimal, text, array []byte
	if err := db.QueryRow(
		`SELECT nil, bool, int, float, string, array FROM docs WHERE id = ?`, "values",
	).Scan(&nilValue, &boolean, &integer, &decimal, &text, &array); err != nil {
		t.Fatal(err)
	}
	if nilValue != nil || !boolean || integer != 9007199254740993 ||
		string(decimal) != "0.1" || string(text) != "hi" || string(array) != "[1,2]" {
		t.Fatalf("round trip = %#v %v %d %q %q %q",
			nilValue, boolean, integer, decimal, text, array)
	}
}

func TestTransactionMaintainsIndexesAcrossAtomicBatch(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE docs (PRIMARY KEY (id))`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE INDEX ON docs(kind)`); err != nil {
		t.Fatal(err)
	}
	// The first write materializes the indexed table and publishes every seed in
	// the transaction's one batch. This used to be a lifecycle-dependent special
	// case, so keep it in the same conformance gate as steady-state commits.
	first, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Exec(
		`INSERT INTO docs VALUES (?), (?)`,
		`{"id":"a","kind":"seed"}`,
		`{"id":"b","kind":"seed"}`,
	); err != nil {
		t.Fatalf("staging initial indexed batch: %v", err)
	}
	if err := first.Commit(); err != nil {
		t.Fatalf("committing initial indexed batch: %v", err)
	}

	transaction, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := transaction.Exec(
		`UPDATE docs SET "$doc" = ? WHERE id = ?`,
		`{"id":"a","kind":"updated"}`, "a",
	); err != nil {
		t.Fatalf("staging indexed update: %v", err)
	}
	if _, err := transaction.Exec(`DELETE FROM docs WHERE id = ?`, "b"); err != nil {
		t.Fatalf("staging indexed delete: %v", err)
	}
	if _, err := transaction.Exec(
		`INSERT INTO docs VALUES (?), (?)`,
		`{"id":"c","kind":"inserted"}`,
		`{"id":"d","kind":"inserted"}`,
	); err != nil {
		t.Fatalf("staging indexed inserts: %v", err)
	}
	if err := transaction.Commit(); err != nil {
		t.Fatalf("committing indexed mixed batch: %v", err)
	}

	assertKindCount := func(kind string, want int64) {
		t.Helper()
		var got int64
		if err := db.QueryRow(
			`SELECT COUNT(*) FROM docs WHERE kind = ?`, kind,
		).Scan(&got); err != nil {
			t.Fatalf("count kind %q: %v", kind, err)
		}
		if got != want {
			t.Fatalf("count kind %q = %d, want %d", kind, got, want)
		}
	}
	assertKindCount("seed", 0)
	assertKindCount("updated", 1)
	assertKindCount("inserted", 2)

	rolledBack, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rolledBack.Exec(
		`UPDATE docs SET "$doc" = ? WHERE id = ?`,
		`{"id":"a","kind":"rolled-back"}`, "a",
	); err != nil {
		t.Fatalf("staging rollback update: %v", err)
	}
	if _, err := rolledBack.Exec(`DELETE FROM docs WHERE id = ?`, "c"); err != nil {
		t.Fatalf("staging rollback delete: %v", err)
	}
	if _, err := rolledBack.Exec(
		`INSERT INTO docs VALUES (?)`, `{"id":"e","kind":"rolled-back"}`,
	); err != nil {
		t.Fatalf("staging rollback insert: %v", err)
	}
	if err := rolledBack.Rollback(); err != nil {
		t.Fatalf("rolling back indexed mixed batch: %v", err)
	}
	assertKindCount("updated", 1)
	assertKindCount("inserted", 2)
	assertKindCount("rolled-back", 0)
}

func TestTransactionValidatesSchemaAtExec(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`
		CREATE TABLE docs (
			id STRING PRIMARY KEY,
			name STRING NOT NULL
		)`); err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(
		`INSERT INTO docs VALUES (?)`, `{"id":"bad","name":7}`,
	); !errors.Is(err, store.ErrSchemaViolation) {
		t.Fatalf("transactional schema error = %v, want ErrSchemaViolation", err)
	}
}

func TestTransactionInsertRejectsExistingPrimaryKeyAtExec(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE docs (PRIMARY KEY (id))`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO docs VALUES (?)`, `{"id":"same","v":1}`); err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(
		`INSERT INTO docs VALUES (?)`, `{"id":"same","v":2}`,
	); !errors.Is(err, ErrDuplicatePrimaryKey) {
		t.Fatalf("transactional duplicate INSERT = %v, want ErrDuplicatePrimaryKey", err)
	}
}

func TestTransactionDeleteRetainsOneBoundedExistenceScratch(t *testing.T) {
	// A transactional batch on the ordered primary stores each document inline
	// and does not overflow (overflow is the single-Put path), so a staged
	// document is bounded by the inline value size, not by MaxDocumentBytes.
	// Two documents that comfortably fit that bound still make the assertion
	// sharp: the DELETE existence scratch retains one document, never both.
	payload := strings.Repeat("x", 400)
	firstDocument := []byte(`{"id":"a","payload":"` + payload + `"}`)
	secondDocument := []byte(`{"id":"b","payload":"` + payload + `"}`)
	_, transaction, keys := beginRawDocsTransaction(
		t, firstDocument, secondDocument,
	)
	state := transaction.tables["docs"]
	for _, key := range keys {
		if err := transaction.stage(state, key, nil, true); err != nil {
			t.Fatal(err)
		}
	}

	if got, want := state.stagedBytes, len(keys[0])+len(keys[1]); got != want {
		t.Fatalf("staged delete bytes = %d, want key-only %d", got, want)
	}
	retained := cap(state.existenceScratch)
	for _, mutation := range state.pending {
		retained += cap(mutation.document)
		if mutation.document != nil {
			t.Fatal("DELETE retained a staged or BEGIN document")
		}
	}
	if retained > state.limits.MaxDocumentBytes {
		t.Fatalf(
			"DELETE existence workspace retained %d bytes, one-document limit %d",
			retained, state.limits.MaxDocumentBytes,
		)
	}
	if retained >= len(firstDocument)+len(secondDocument) {
		t.Fatalf(
			"DELETE retained %d bytes for %d bytes of BEGIN documents",
			retained, len(firstDocument)+len(secondDocument),
		)
	}
	if err := transaction.Rollback(); err != nil {
		t.Fatal(err)
	}
}

func TestFinishedTransactionReleasesRetainedState(t *testing.T) {
	database := &database{}
	connection := &conn{db: database}
	transaction := &tx{
		conn: connection,
		tables: map[string]*txTable{
			"docs": {
				name: "docs",
				pending: map[string]*txMutation{
					"key": {document: make([]byte, 1<<20)},
				},
				order:          []string{"key"},
				validationTape: make([]vibejson.IndexEntry, 1024),
			},
		},
	}
	connection.tx = transaction

	if err := transaction.Rollback(); err != nil {
		t.Fatal(err)
	}
	if transaction.conn != nil || transaction.tables != nil {
		t.Fatal("finished transaction retained connection or staged table state")
	}
	if connection.tx != nil {
		t.Fatal("finished transaction remained installed on its connection")
	}
	if err := transaction.Rollback(); err != nil {
		t.Fatalf("second Rollback: %v", err)
	}
}

func TestTransactionValidationTapeGrowsOnceAndReuses(t *testing.T) {
	var dense strings.Builder
	dense.WriteString(`{"id":"dense","values":[`)
	for i := 0; i < 4096; i++ {
		if i != 0 {
			dense.WriteByte(',')
		}
		dense.WriteByte('0')
	}
	dense.WriteString(`]}`)
	document := []byte(dense.String())
	state := &txTable{}
	if err := validateDocument(
		nil, document, len(document), &state.validationTape,
	); err != nil {
		t.Fatal(err)
	}
	seed := len(document)/8 + 8
	if cap(state.validationTape) <= seed {
		t.Fatalf(
			"dense document did not exercise tape growth: capacity %d seed %d",
			cap(state.validationTape), seed,
		)
	}
	if cap(state.validationTape) >= len(document)+2 {
		t.Fatalf(
			"dense document retained theoretical tape maximum %d for %d source bytes",
			cap(state.validationTape), len(document),
		)
	}
	first := &state.validationTape[0]
	allocations := testing.AllocsPerRun(100, func() {
		if err := validateDocument(
			nil, document, len(document), &state.validationTape,
		); err != nil {
			panic(err)
		}
	})
	if allocations != 0 {
		t.Fatalf("warm transaction validation allocated %.2f times, want zero", allocations)
	}
	if first != &state.validationTape[0] {
		t.Fatal("warm transaction validation replaced its retained tape")
	}
}

func TestTransactionOversizedValuesNeverEnterPendingState(t *testing.T) {
	base := []byte(`{"id":"base","payload":"intact"}`)
	database, transaction, keys := beginRawDocsTransaction(t, base)
	state := transaction.tables["docs"]
	tooLarge := `{"id":"base","payload":"` +
		strings.Repeat("x", state.limits.MaxDocumentBytes) + `"}`

	insert, err := query.PrepareDML(`INSERT INTO docs VALUES (?)`)
	if err != nil {
		t.Fatal(err)
	}
	defer insert.Release()
	if _, err := transaction.execMutation(
		insert, []any{tooLarge},
	); !errors.Is(err, durable.ErrDocumentTooLarge) {
		t.Fatalf("oversized INSERT = %v, want ErrDocumentTooLarge", err)
	}
	assertEmptyTransactionPending(t, state)

	tooLongKey := strings.Repeat("k", state.limits.MaxKeyBytes+1)
	if _, err := transaction.execMutation(
		insert, []any{`{"id":"` + tooLongKey + `"}`},
	); !errors.Is(err, durable.ErrKeyTooLarge) {
		t.Fatalf("oversized INSERT key = %v, want ErrKeyTooLarge", err)
	}
	assertEmptyTransactionPending(t, state)

	update, err := query.PrepareDML(
		`UPDATE docs SET "$doc" = ? WHERE id = ?`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer update.Release()
	if _, err := transaction.execMutation(
		update, []any{tooLarge, "base"},
	); !errors.Is(err, durable.ErrDocumentTooLarge) {
		t.Fatalf("oversized UPDATE = %v, want ErrDocumentTooLarge", err)
	}
	assertEmptyTransactionPending(t, state)

	if err := transaction.Commit(); err != nil {
		t.Fatal(err)
	}
	assertRawDocument(t, database, keys[0], base)

	connection := &conn{db: database}
	rolledBack, err := connection.beginTx(
		context.Background(), sqldriver.TxOptions{}, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	connection.tx = rolledBack
	if _, err := rolledBack.execMutation(
		insert, []any{tooLarge},
	); !errors.Is(err, durable.ErrDocumentTooLarge) {
		t.Fatalf("rollback oversized INSERT = %v, want ErrDocumentTooLarge", err)
	}
	rolledState := rolledBack.tables["docs"]
	assertEmptyTransactionPending(t, rolledState)
	if err := rolledBack.Rollback(); err != nil {
		t.Fatal(err)
	}
	assertRawDocument(t, database, keys[0], base)
}

func TestFailedStatementDoesNotPartiallyChangeTransactionOverlay(t *testing.T) {
	original := []byte(`{"id":"existing","value":"original"}`)
	state := &txTable{
		snapshot: &durable.Snapshot{},
		pending: map[string]*txMutation{
			"existing": {
				document: append([]byte(nil), original...),
				existed:  true,
			},
		},
		order:       []string{"existing"},
		stagedBytes: len("existing") + len(original),
	}
	transaction := &tx{}
	beforeBytes := state.stagedBytes
	err := transaction.applyStagedMutations(state, []stagedTxMutation{
		{
			key:      "existing",
			document: []byte(`{"id":"existing","value":"replacement"}`),
		},
		{
			key:      "unreadable",
			document: []byte(`{"id":"unreadable"}`),
		},
	})
	if !errors.Is(err, durable.ErrClosed) {
		t.Fatalf("staging with closed BEGIN snapshot = %v, want ErrClosed", err)
	}
	if got := state.pending["existing"]; got == nil ||
		string(got.document) != string(original) ||
		got.remove || !got.existed {
		t.Fatalf("failed statement changed existing pending entry: %#v", got)
	}
	if _, exists := state.pending["unreadable"]; exists {
		t.Fatal("failed statement installed its later pending entry")
	}
	if len(state.order) != 1 || state.order[0] != "existing" ||
		state.stagedBytes != beforeBytes {
		t.Fatalf(
			"failed statement changed overlay accounting: order=%v bytes=%d want %d",
			state.order, state.stagedBytes, beforeBytes,
		)
	}
	raw, found, readErr := state.appendRaw(nil, "existing")
	if readErr != nil || !found || string(raw) != string(original) {
		t.Fatalf(
			"read-your-writes after failed statement = (%s, %t, %v), want original",
			raw, found, readErr,
		)
	}
}

func TestTransactionInsertPreflightsRemainingBatchRows(t *testing.T) {
	_, transaction, _ := beginRawDocsTransaction(t)
	state := transaction.tables["docs"]
	var sql strings.Builder
	sql.WriteString(`INSERT INTO docs VALUES `)
	for i := 0; i <= state.limits.MaxBatchDocuments; i++ {
		if i != 0 {
			sql.WriteString(", ")
		}
		sql.WriteString("(?)")
	}
	insert, err := query.PrepareDML(sql.String())
	if err != nil {
		t.Fatal(err)
	}
	defer insert.Release()
	if _, err := transaction.execMutation(
		insert, nil,
	); !errors.Is(err, ErrTransactionTooLarge) ||
		!errors.Is(err, durable.ErrBatchTooLarge) {
		t.Fatalf(
			"oversized INSERT row count = %v, want ErrTransactionTooLarge and ErrBatchTooLarge",
			err,
		)
	}
	assertEmptyTransactionPending(t, state)
}

func TestTransactionInsertEnforcesCumulativeBytesBeforeStaging(t *testing.T) {
	_, transaction, _ := beginRawDocsTransaction(t)
	state := transaction.tables["docs"]
	state.limits.MaxBatchBytes = 128
	insert, err := query.PrepareDML(`INSERT INTO docs VALUES (?), (?)`)
	if err != nil {
		t.Fatal(err)
	}
	defer insert.Release()
	document := `{"id":"one","payload":"` + strings.Repeat("x", 80) + `"}`
	second := `{"id":"two","payload":"` + strings.Repeat("x", 80) + `"}`
	if _, err := transaction.execMutation(
		insert, []any{document, second},
	); !errors.Is(err, ErrTransactionTooLarge) ||
		!errors.Is(err, durable.ErrBatchTooLarge) {
		t.Fatalf(
			"oversized cumulative INSERT = %v, want ErrTransactionTooLarge and ErrBatchTooLarge",
			err,
		)
	}
	assertEmptyTransactionPending(t, state)
}

func TestTransactionInsertCanReplacePendingDeleteAtBatchLimit(t *testing.T) {
	base := []byte(`{"id":"base","payload":"before"}`)
	_, transaction, keys := beginRawDocsTransaction(t, base)
	state := transaction.tables["docs"]
	state.limits.MaxBatchDocuments = 1
	if err := transaction.stage(state, keys[0], nil, true); err != nil {
		t.Fatal(err)
	}
	insert, err := query.PrepareDML(`INSERT INTO docs VALUES (?)`)
	if err != nil {
		t.Fatal(err)
	}
	defer insert.Release()
	replacement := `{"id":"base","payload":"after"}`
	if _, err := transaction.execMutation(
		insert, []any{replacement},
	); err != nil {
		t.Fatalf("replacement INSERT at batch limit: %v", err)
	}
	if len(state.order) != 1 || len(state.pending) != 1 ||
		state.pending[keys[0]].remove {
		t.Fatalf(
			"replacement changed distinct pending keys: order %d entries %d remove %v",
			len(state.order), len(state.pending), state.pending[keys[0]].remove,
		)
	}
}

func beginRawDocsTransaction(
	t *testing.T,
	documents ...[]byte,
) (*database, *tx, []string) {
	t.Helper()
	database, err := openDatabase(filepath.Join(t.TempDir(), "catalog.vdb"))
	if err != nil {
		t.Fatal(err)
	}
	create, err := query.PrepareDML(
		`CREATE TABLE docs (PRIMARY KEY (id))`,
	)
	if err != nil {
		_ = database.close()
		t.Fatal(err)
	}
	defer create.Release()

	var keys []string
	database.mu.Lock()
	_, setupErr := database.createTableLocked(create)
	if setupErr == nil && len(documents) != 0 {
		table := database.tables["docs"]
		limits, limitsErr := tableMutationLimits(table)
		if limitsErr != nil {
			setupErr = limitsErr
		}
		seeds := make([]seedDocument, 0, len(documents))
		keys = make([]string, 0, len(documents))
		for _, document := range documents {
			if setupErr != nil {
				break
			}
			key, keyErr := documentKey(
				document, table.meta.PrimaryKey, table.primary,
				limits.MaxKeyBytes,
			)
			if keyErr != nil {
				setupErr = keyErr
				break
			}
			keys = append(keys, key)
			seeds = append(seeds, seedDocument{
				key: key, document: document,
			})
		}
		if setupErr == nil {
			_, setupErr = database.materializeLocked("docs", seeds)
		}
	}
	database.mu.Unlock()
	if setupErr != nil {
		_ = database.close()
		t.Fatal(setupErr)
	}

	connection := &conn{db: database}
	transaction, err := connection.beginTx(
		context.Background(), sqldriver.TxOptions{}, nil,
	)
	if err != nil {
		_ = database.close()
		t.Fatal(err)
	}
	connection.tx = transaction
	if err := transaction.refreshStatementCut(
		context.Background(), "docs",
		[]physicalDependency{{name: "docs"}}, nil,
	); err != nil {
		_ = transaction.Rollback()
		_ = database.close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if !transaction.done {
			_ = transaction.Rollback()
		}
		if err := database.close(); err != nil {
			t.Error(err)
		}
	})
	return database, transaction, keys
}

func assertEmptyTransactionPending(t *testing.T, state *txTable) {
	t.Helper()
	if len(state.pending) != 0 || len(state.order) != 0 ||
		state.stagedBytes != 0 || cap(state.existenceScratch) != 0 {
		t.Fatalf(
			"failed oversized statement retained pending state: entries %d, order %d, bytes %d, scratch %d",
			len(state.pending), len(state.order), state.stagedBytes,
			cap(state.existenceScratch),
		)
	}
}

func assertRawDocument(
	t *testing.T,
	database *database,
	key string,
	want []byte,
) {
	t.Helper()
	database.mu.RLock()
	collection := database.tables["docs"].collection
	database.mu.RUnlock()
	got, found, err := collection.AppendRaw(nil, []byte(key))
	if err != nil {
		t.Fatal(err)
	}
	if !found || string(got) != string(want) {
		t.Fatalf("raw document after failed oversized statement = (%s, %v)", got, found)
	}
}
