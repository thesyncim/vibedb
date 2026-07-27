package vibesql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
)

// The write path's contract.
//
// The load-bearing test in this file is TestDeleteRemovesExactlyWhatSelectReturns.
// Everything else checks a rule; that one checks the property the whole design
// exists to hold — that a mutation and a query agree about which documents a
// condition names — over a corpus built to carry explicit nulls, absent paths,
// and the negations where the engine and SQL disagree.

// --- fixtures ---------------------------------------------------------------

// writableDurable creates a durable collection with the given options, fills
// it, and opens it through the driver.
//
// It registers the handle with AttachCollection rather than using the path form
// of the DSN, and that is load-bearing rather than incidental: a path DSN opens
// the file with default options, so a collection created with
// MaxBatchDocuments: 2 would be reopened with 64 and the batch-bound tests
// would silently pass against a bound they never reached. The package
// documentation says the same thing about declared indexes for the same reason.
func writableDurable(t testing.TB, name string, docs map[string]string, opts durable.Options) *sql.DB {
	t.Helper()
	path := filepath.Join(t.TempDir(), name+".vj")
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	collection, err := durable.Create(file, opts)
	if err != nil {
		t.Fatal(err)
	}
	for key, doc := range docs {
		if _, err := collection.Put(key, []byte(doc)); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		_ = collection.Close()
		_ = file.Close()
	})
	// The registered name is the collection name a FROM clause spells, so it is
	// the plain name rather than something unique per test; the tests in this
	// package are sequential and each detaches in its own cleanup.
	dsn, err := AttachCollection(name, collection)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { Detach(name) })
	handle, err := sql.Open("vibejson", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = handle.Close() })
	return handle
}

// keyedCorpus is the corpus keyed the way a mutation addresses it.
func keyedCorpus() map[string]string {
	out := make(map[string]string, len(corpus))
	for i, doc := range corpus {
		out[strconv.Itoa(i+1)] = doc
	}
	return out
}

// keysOf reads every key the collection holds, in sorted order.
func keysOf(t testing.TB, db *sql.DB, collection string) []string {
	t.Helper()
	rows, err := db.Query("SELECT id FROM " + collection)
	if err != nil {
		t.Fatalf("reading %s: %v", collection, err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var id any
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		out = append(out, fmt.Sprint(id))
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// --- the property the design exists for -------------------------------------

// Given one corpus and one condition, when the condition is run as a SELECT and
// then as a DELETE, then the documents the DELETE removes are exactly the
// documents the SELECT returned — including for conditions over explicit nulls
// and absent paths, and under the negations where this engine's two-valued
// leaves and SQL's three-valued logic disagree.
//
// This is the differential the whole arrangement in query/sqldml.go exists to
// make structural rather than hoped for: the DELETE filters through the same
// compiled predicate the SELECT does. A conditions list that avoided nulls
// would pass against a second evaluator too, so every entry here either reads a
// path some document lacks or negates a comparison over one.
func TestDeleteRemovesExactlyWhatSelectReturns(t *testing.T) {
	conditions := []string{
		`age > 25`,
		`age IS NULL`,
		`age IS NOT NULL`,
		`NOT (age = 30)`,
		`NOT (age > 25)`,
		`tier = 'pro'`,
		`tier IS MISSING`,
		`NOT (tier = 'pro')`,
		`age IN (21, 30)`,
		`age NOT IN (21, 30)`,
		`age BETWEEN 20 AND 31`,
		`age NOT BETWEEN 20 AND 31`,
		`flag = TRUE`,
		`NOT (flag = TRUE)`,
		`ratio > 1`,
		`name = ''`,
		`big = 9007199254740993`,
		`age > 25 OR tier = 'free'`,
		`NOT (age > 25 OR tier = 'free')`,
		`age > 20 AND NOT (tier = 'pro')`,
		`meta @> {"x":1}`,
	}
	for _, condition := range conditions {
		t.Run(condition, func(t *testing.T) {
			db := writableDurable(t, "users", keyedCorpus(), durable.Options{MaxBatchDocuments: 64})
			before := keysOf(t, db, "users")

			// What the query says the condition selects.
			rows, err := db.Query("SELECT id FROM users WHERE " + condition)
			if err != nil {
				t.Fatalf("SELECT: %v", err)
			}
			selected := map[string]bool{}
			for rows.Next() {
				var id any
				if err := rows.Scan(&id); err != nil {
					t.Fatal(err)
				}
				selected[fmt.Sprint(id)] = true
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			_ = rows.Close()

			result, err := db.Exec("DELETE FROM users WHERE " + condition)
			if err != nil {
				t.Fatalf("DELETE: %v", err)
			}
			affected, err := result.RowsAffected()
			if err != nil {
				t.Fatal(err)
			}
			if int(affected) != len(selected) {
				t.Errorf("DELETE reported %d rows affected, SELECT returned %d", affected, len(selected))
			}
			after := keysOf(t, db, "users")
			// The corpus keys documents by their own "id" field, so the id a
			// SELECT projects is the key a DELETE removes; the two sets are
			// therefore directly comparable.
			for _, id := range before {
				gone := true
				for _, remaining := range after {
					if remaining == id {
						gone = false
						break
					}
				}
				if gone != selected[id] {
					t.Errorf("document %s: deleted=%v, SELECT selected it=%v", id, gone, selected[id])
				}
			}
		})
	}
}

// --- INSERT ------------------------------------------------------------------

// Given an INSERT with a bound document, when it runs, then the document is
// stored under the given key and RowsAffected counts it.
func TestInsertWritesADocument(t *testing.T) {
	db := writableDurable(t, "users", keyedCorpus(), durable.Options{})
	result, err := db.Exec(`INSERT INTO users VALUES ('99', ?)`, []byte(`{"id":99,"name":"zed","age":50}`))
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	if affected, _ := result.RowsAffected(); affected != 1 {
		t.Errorf("RowsAffected = %d, want 1", affected)
	}
	var name string
	if err := db.QueryRow(`SELECT name FROM users WHERE id = 99`).Scan(&name); err != nil {
		t.Fatalf("reading it back: %v", err)
	}
	if name != "zed" {
		t.Errorf("name = %q, want zed", name)
	}
}

// Given a multi-row INSERT, when it runs, then every row is written and the
// count is the number of rows.
func TestInsertWritesSeveralDocumentsAtOnce(t *testing.T) {
	db := writableDurable(t, "users", keyedCorpus(), durable.Options{})
	result, err := db.Exec(
		`INSERT INTO users VALUES ('a', {"id":100,"tier":"pro"}), ('b', {"id":101,"tier":"pro"})`)
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	if affected, _ := result.RowsAffected(); affected != 2 {
		t.Errorf("RowsAffected = %d, want 2", affected)
	}
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM users WHERE id > 99`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Errorf("counted %d new documents, want 2", n)
	}
}

// Given a key a document already occupies, when an INSERT names it, then the
// statement is refused and nothing is written — INSERT is not an upsert.
func TestInsertRefusesAnExistingKey(t *testing.T) {
	db := writableDurable(t, "users", keyedCorpus(), durable.Options{})
	_, err := db.Exec(`INSERT INTO users VALUES ('1', {"id":1,"name":"overwritten"})`)
	if err == nil {
		t.Fatal("INSERT onto an existing key succeeded, want a refusal")
	}
	if !strings.Contains(err.Error(), "already exists") {
		t.Errorf("err = %v, want it to say the key exists", err)
	}
	var name string
	if err := db.QueryRow(`SELECT name FROM users WHERE id = 1`).Scan(&name); err != nil {
		t.Fatal(err)
	}
	if name != "amy" {
		t.Errorf("name = %q, want the original amy: the refused INSERT wrote anyway", name)
	}
}

// Given malformed JSON, when an INSERT carries it, then the statement fails and
// the collection is unchanged.
func TestInsertRefusesMalformedJSON(t *testing.T) {
	db := writableDurable(t, "users", keyedCorpus(), durable.Options{})
	if _, err := db.Exec(`INSERT INTO users VALUES ('99', ?)`, []byte(`{"a":`)); err == nil {
		t.Fatal("INSERT of malformed JSON succeeded")
	}
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM users`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != len(corpus) {
		t.Errorf("collection holds %d documents, want the original %d", n, len(corpus))
	}
}

// --- LastInsertId -------------------------------------------------------------

// Given any successful mutation, when LastInsertId is called, then it reports an
// error rather than a number, because a key is a caller-chosen string and no
// integer stands for one.
func TestLastInsertIdReportsAnError(t *testing.T) {
	db := writableDurable(t, "users", keyedCorpus(), durable.Options{})
	result, err := db.Exec(`INSERT INTO users VALUES ('99', {"id":99})`)
	if err != nil {
		t.Fatal(err)
	}
	id, err := result.LastInsertId()
	if err == nil {
		t.Fatalf("LastInsertId() = %d, nil; want an error", id)
	}
	if !strings.Contains(err.Error(), "no meaning") {
		t.Errorf("err = %v, want it to explain that there is no id", err)
	}
}

// --- UPDATE ------------------------------------------------------------------

// Given a filtered UPDATE, when it runs, then every matching document is
// replaced whole and the count is the number replaced.
func TestUpdateReplacesMatchingDocuments(t *testing.T) {
	db := writableDurable(t, "users", keyedCorpus(), durable.Options{MaxBatchDocuments: 64})
	result, err := db.Exec(`UPDATE users SET "$doc" = ? WHERE tier = 'free'`, []byte(`{"id":0,"tier":"free"}`))
	if err != nil {
		t.Fatalf("UPDATE: %v", err)
	}
	affected, _ := result.RowsAffected()
	if affected != 3 {
		t.Errorf("RowsAffected = %d, want 3 free-tier documents", affected)
	}
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM users WHERE id = 0`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Errorf("%d documents carry the replacement, want 3", n)
	}
}

// Given a keyed UPDATE naming a key the collection does not hold, when it runs,
// then it affects nothing and creates nothing — an UPDATE is not an insert.
func TestUpdateOfAnAbsentKeyWritesNothing(t *testing.T) {
	db := writableDurable(t, "users", keyedCorpus(), durable.Options{})
	result, err := db.Exec(`UPDATE users SET "$doc" = ? WHERE "$key" = 'absent'`, []byte(`{"id":123}`))
	if err != nil {
		t.Fatalf("UPDATE: %v", err)
	}
	if affected, _ := result.RowsAffected(); affected != 0 {
		t.Errorf("RowsAffected = %d, want 0", affected)
	}
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM users`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != len(corpus) {
		t.Errorf("collection holds %d documents, want the original %d", n, len(corpus))
	}
}

// --- DELETE ------------------------------------------------------------------

// Given a keyed DELETE naming both a present and an absent key, when it runs,
// then it removes the present one and reports one row affected.
func TestDeleteByKeyCountsOnlyWhatItRemoved(t *testing.T) {
	db := writableDurable(t, "users", keyedCorpus(), durable.Options{})
	result, err := db.Exec(`DELETE FROM users WHERE "$key" IN ('1', 'absent')`)
	if err != nil {
		t.Fatalf("DELETE: %v", err)
	}
	if affected, _ := result.RowsAffected(); affected != 1 {
		t.Errorf("RowsAffected = %d, want 1", affected)
	}
	if got := len(keysOf(t, db, "users")); got != len(corpus)-1 {
		t.Errorf("collection holds %d documents, want %d", got, len(corpus)-1)
	}
}

// Given a DELETE with no WHERE, when the collection fits one batch, then every
// document is removed.
func TestDeleteWithNoConditionRemovesEverything(t *testing.T) {
	db := writableDurable(t, "users", keyedCorpus(), durable.Options{MaxBatchDocuments: 64})
	result, err := db.Exec(`DELETE FROM users`)
	if err != nil {
		t.Fatalf("DELETE: %v", err)
	}
	if affected, _ := result.RowsAffected(); int(affected) != len(corpus) {
		t.Errorf("RowsAffected = %d, want %d", affected, len(corpus))
	}
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM users`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("collection holds %d documents, want 0", n)
	}
}

// --- the batch bound ----------------------------------------------------------

// Given a collection whose batch publishes at most two documents, when a
// statement matches more, then it fails naming both numbers and writes nothing
// — the batch is refused whole rather than split across two commits.
func TestStatementExceedingTheBatchBoundWritesNothing(t *testing.T) {
	db := writableDurable(t, "users", keyedCorpus(), durable.Options{MaxBatchDocuments: 2})
	_, err := db.Exec(`DELETE FROM users WHERE age > 0`)
	if err == nil {
		t.Fatal("an over-large DELETE succeeded, want ErrBatchTooLarge")
	}
	if !errors.Is(err, durable.ErrBatchTooLarge) {
		t.Errorf("err = %v, want it to wrap durable.ErrBatchTooLarge", err)
	}
	if !strings.Contains(err.Error(), "at most 2") {
		t.Errorf("err = %v, want it to name the limit", err)
	}
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM users`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != len(corpus) {
		t.Errorf("collection holds %d documents, want the original %d: a refused batch wrote anyway", n, len(corpus))
	}
}

// --- routing ------------------------------------------------------------------

// Given a SELECT, when it is run through Exec, then it is refused rather than
// executed with its rows discarded.
func TestExecRefusesASelect(t *testing.T) {
	db := writableDurable(t, "users", keyedCorpus(), durable.Options{})
	if _, err := db.Exec(`SELECT id FROM users`); err == nil {
		t.Fatal("Exec of a SELECT succeeded, want a refusal")
	}
}

// Given a mutation, when it is run through Query, then it is refused rather
// than executed and reported as an empty result.
func TestQueryRefusesAMutation(t *testing.T) {
	db := writableDurable(t, "users", keyedCorpus(), durable.Options{})
	rows, err := db.Query(`DELETE FROM users WHERE id = 1`)
	if err == nil {
		_ = rows.Close()
		t.Fatal("Query of a DELETE succeeded, want a refusal")
	}
	if !strings.Contains(err.Error(), "returns no rows") {
		t.Errorf("err = %v, want it to say the statement returns no rows", err)
	}
}

// --- the heap backend ---------------------------------------------------------

// Given a heap database, when a mutation runs against it, then it is applied —
// the two backends accept the same statements, and only their atomicity
// differs.
func TestMutationsWorkAgainstAHeapDatabase(t *testing.T) {
	db := memoryDatabase(t, map[string][]string{"users": corpus})
	if _, err := db.Exec(`INSERT INTO users VALUES ('99', {"id":99,"tier":"pro"})`); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	result, err := db.Exec(`DELETE FROM users WHERE tier = 'free'`)
	if err != nil {
		t.Fatalf("DELETE: %v", err)
	}
	if affected, _ := result.RowsAffected(); affected != 3 {
		t.Errorf("RowsAffected = %d, want 3", affected)
	}
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM users`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != len(corpus)+1-3 {
		t.Errorf("collection holds %d documents, want %d", n, len(corpus)+1-3)
	}
}

// --- DDL ----------------------------------------------------------------------

// Given a heap database, when CREATE TABLE runs with a column list, then the
// collection exists and its declared schema is enforced on every write.
func TestCreateTableDeclaresAnEnforcedSchema(t *testing.T) {
	db := memoryDatabase(t, map[string][]string{"seed": {`{"a":1}`}})
	if _, err := db.Exec(`CREATE TABLE people (id STRING PRIMARY KEY, age INTEGER)`); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO people VALUES ('p1', {"id":"p1","age":40})`); err != nil {
		t.Fatalf("INSERT into the new table: %v", err)
	}
	// age is declared INTEGER, so a fractional number violates the schema.
	if _, err := db.Exec(`INSERT INTO people VALUES ('p2', {"id":"p2","age":40.5})`); err == nil {
		t.Error("a document violating the declared type was accepted")
	}
	// id is the declared primary key, so it is required; what a declared
	// primary key does and does not do today is documented on LowerTable.
	if _, err := db.Exec(`INSERT INTO people VALUES ('p3', {"age":1})`); err == nil {
		t.Error("a document missing the declared primary-key path was accepted")
	}
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM people`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("people holds %d documents, want 1", n)
	}
}

// Given a table with no column list, when it is created, then it validates
// nothing beyond JSON syntax — the schemaless default.
func TestCreateTableWithoutColumnsDeclaresNoSchema(t *testing.T) {
	db := memoryDatabase(t, map[string][]string{"seed": {`{"a":1}`}})
	if _, err := db.Exec(`CREATE TABLE loose`); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO loose VALUES ('x', {"anything":[1,"two",null]})`); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE loose`); err == nil {
		t.Error("creating an existing table succeeded, want a refusal")
	}
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS loose`); err != nil {
		t.Errorf("IF NOT EXISTS on an existing table: %v", err)
	}
}

// Given a heap collection, when CREATE INDEX runs, then the index exists,
// covers every document already stored, and answers a query.
func TestCreateIndexIsUsableImmediately(t *testing.T) {
	heap := &store.Database{}
	collection, err := heap.CreateCollection("users", store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	for i, doc := range corpus {
		if _, err := collection.Put(strconv.Itoa(i+1), []byte(doc)); err != nil {
			t.Fatal(err)
		}
	}
	dsn, err := Attach("ddlindex-"+t.Name(), heap)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { Detach("ddlindex-" + t.Name()) })
	db, err := sql.Open("vibejson", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if _, err := db.Exec(`CREATE INDEX ON users (tier)`); err != nil {
		t.Fatalf("CREATE INDEX: %v", err)
	}
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	infos := snapshot.AppendIndexes(nil)
	if len(infos) != 1 {
		t.Fatalf("the collection reports %d indexes, want 1", len(infos))
	}
	if infos[0].State != store.IndexReady {
		t.Errorf("index state = %v, want Ready: CREATE INDEX must backfill before it returns", infos[0].State)
	}
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM users WHERE tier = 'pro'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Errorf("counted %d pro documents through the index, want 3", n)
	}
}

// Given a durable collection, whose indexes are frozen when the file is
// created, when CREATE INDEX names it, then it is refused with the reason
// rather than silently doing nothing.
func TestCreateIndexRefusesADurableCollection(t *testing.T) {
	db := writableDurable(t, "users", keyedCorpus(), durable.Options{})
	_, err := db.Exec(`CREATE INDEX ON users (tier)`)
	if err == nil {
		t.Fatal("CREATE INDEX against a durable collection succeeded")
	}
	if !strings.Contains(err.Error(), "frozen") {
		t.Errorf("err = %v, want it to say the indexes are frozen", err)
	}
}

// --- transactions -------------------------------------------------------------

// Given a transaction, when it commits, then every statement's writes become
// visible together, and when it rolls back, none of them do.
func TestTransactionCommitsAndRollsBackAsAWhole(t *testing.T) {
	db := writableDurable(t, "users", keyedCorpus(), durable.Options{MaxBatchDocuments: 64})
	db.SetMaxOpenConns(2)

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if _, err := tx.Exec(`INSERT INTO users VALUES ('t1', {"id":201,"tier":"pro"})`); err != nil {
		t.Fatalf("INSERT in tx: %v", err)
	}
	if _, err := tx.Exec(`DELETE FROM users WHERE "$key" = '1'`); err != nil {
		t.Fatalf("DELETE in tx: %v", err)
	}
	// Nothing is visible outside the transaction before the commit.
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM users WHERE id = 201`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Error("an uncommitted insert was visible outside its transaction")
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM users WHERE id = 201`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Error("a committed insert is not visible")
	}
	if got := len(keysOf(t, db, "users")); got != len(corpus) {
		t.Errorf("collection holds %d documents, want %d after one insert and one delete", got, len(corpus))
	}

	rolled, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rolled.Exec(`INSERT INTO users VALUES ('t2', {"id":202})`); err != nil {
		t.Fatal(err)
	}
	if err := rolled.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM users WHERE id = 202`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Error("a rolled-back insert was published")
	}
}

// Given a transaction that has already written a key, when a later statement in
// the same transaction reads it, then it sees the transaction's own write.
func TestTransactionReadsItsOwnWrites(t *testing.T) {
	db := writableDurable(t, "users", keyedCorpus(), durable.Options{MaxBatchDocuments: 64})
	db.SetMaxOpenConns(1)
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	// Insert a document the collection does not hold, then delete it by a
	// condition only that document satisfies. The DELETE has to see the staged
	// insert to affect anything.
	if _, err := tx.Exec(`INSERT INTO users VALUES ('t9', {"id":900,"tier":"staged"})`); err != nil {
		t.Fatal(err)
	}
	result, err := tx.Exec(`DELETE FROM users WHERE tier = 'staged'`)
	if err != nil {
		t.Fatalf("DELETE in tx: %v", err)
	}
	if affected, _ := result.RowsAffected(); affected != 1 {
		t.Errorf("RowsAffected = %d, want 1: the DELETE did not see the transaction's own insert", affected)
	}
}

// Given a transaction that read a document, when another writer changes that
// document before the commit, then the commit fails with ErrConflict and writes
// nothing.
func TestTransactionConflictAbortsTheCommit(t *testing.T) {
	db := writableDurable(t, "users", keyedCorpus(), durable.Options{MaxBatchDocuments: 64})
	db.SetMaxOpenConns(4)

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`UPDATE users SET "$doc" = ? WHERE "$key" = '1'`, []byte(`{"id":1,"writer":"tx"}`)); err != nil {
		t.Fatalf("UPDATE in tx: %v", err)
	}
	// A second, independent writer changes the same document.
	if _, err := db.Exec(`UPDATE users SET "$doc" = ? WHERE "$key" = '1'`, []byte(`{"id":1,"writer":"other"}`)); err != nil {
		t.Fatalf("concurrent UPDATE: %v", err)
	}
	err = tx.Commit()
	if err == nil {
		t.Fatal("the commit succeeded over a concurrent write, want a conflict")
	}
	if !errors.Is(err, ErrConflict) {
		t.Errorf("err = %v, want ErrConflict", err)
	}
	var writer string
	if err := db.QueryRow(`SELECT writer FROM users WHERE id = 1`).Scan(&writer); err != nil {
		t.Fatal(err)
	}
	if writer != "other" {
		t.Errorf("writer = %q, want the concurrent writer's value: the conflicting commit wrote anyway", writer)
	}
}

// Given a transaction, when a statement would take it past the collection's
// batch bound, then that statement fails and the transaction is left exactly as
// it was.
func TestTransactionRefusesToExceedTheBatchBound(t *testing.T) {
	db := writableDurable(t, "users", keyedCorpus(), durable.Options{MaxBatchDocuments: 2})
	db.SetMaxOpenConns(1)
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`DELETE FROM users WHERE "$key" = '1'`); err != nil {
		t.Fatal(err)
	}
	_, err = tx.Exec(`DELETE FROM users WHERE age > 0`)
	if err == nil {
		t.Fatal("a statement past the batch bound succeeded")
	}
	if !errors.Is(err, durable.ErrBatchTooLarge) {
		t.Errorf("err = %v, want ErrBatchTooLarge", err)
	}
	// The transaction is still usable and still holds only its first write.
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit after the refusal: %v", err)
	}
	if got := len(keysOf(t, db, "users")); got != len(corpus)-1 {
		t.Errorf("collection holds %d documents, want %d", got, len(corpus)-1)
	}
}

// Given a heap database, which has no operation that publishes several
// documents together, when a transaction is opened over it, then it is refused
// rather than returned as a promise the driver cannot keep.
func TestTransactionRefusesAHeapDatabase(t *testing.T) {
	db := memoryDatabase(t, map[string][]string{"users": corpus})
	_, err := db.Begin()
	if err == nil {
		t.Fatal("Begin over a heap database succeeded")
	}
	if !strings.Contains(err.Error(), "durable collection") {
		t.Errorf("err = %v, want it to name the requirement", err)
	}
}

// Given an isolation level this engine does not implement, when a transaction
// asks for it, then it is refused rather than silently downgraded.
func TestTransactionRefusesAnUnsupportedIsolationLevel(t *testing.T) {
	db := writableDurable(t, "users", keyedCorpus(), durable.Options{})
	_, err := db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err == nil {
		t.Fatal("BeginTx accepted LevelSerializable")
	}
	if !strings.Contains(err.Error(), "snapshot isolation") {
		t.Errorf("err = %v, want it to name what is provided", err)
	}
	tx, err := db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelSnapshot})
	if err != nil {
		t.Fatalf("BeginTx(LevelSnapshot): %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
}

// Given a reader holding a snapshot, when a writer commits, then the reader
// continues to observe the generation it opened on — snapshot isolation for
// readers, stated as a test rather than left to be inferred.
func TestReaderKeepsItsGenerationAcrossACommit(t *testing.T) {
	db := writableDurable(t, "users", keyedCorpus(), durable.Options{MaxBatchDocuments: 64})
	db.SetMaxOpenConns(4)

	rows, err := db.Query(`SELECT id FROM users`)
	if err != nil {
		t.Fatal(err)
	}
	// The rows hold an open snapshot. Read one row so the cursor is live, then
	// commit a delete from a different connection.
	if !rows.Next() {
		t.Fatal("the query returned no rows")
	}
	if _, err := db.Exec(`DELETE FROM users WHERE "$key" = '1'`); err != nil {
		t.Fatalf("concurrent DELETE: %v", err)
	}
	seen := 1
	for rows.Next() {
		seen++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	_ = rows.Close()
	if seen != len(corpus) {
		t.Errorf("the reader saw %d rows, want the %d its snapshot opened on", seen, len(corpus))
	}
	if got := len(keysOf(t, db, "users")); got != len(corpus)-1 {
		t.Errorf("a later reader sees %d documents, want %d", got, len(corpus)-1)
	}
}

// --- concurrency ---------------------------------------------------------------

// Given a pool handing many connections out at once, when readers and writers
// run concurrently against one durable collection, then every operation
// completes and the final document count is exactly what the writes imply.
//
// This is the shape the brief called the most likely source of a subtle race or
// a deadlock, and the reason is worth stating: an autocommit write takes a
// snapshot from inside the collection's Update callback, which already holds
// the writer lock. That is safe only because Snapshot takes the snapshot gate
// and Update does not hold it while the callback runs — a fact this test
// exercises rather than asserts, since a mistaken lock order would hang here
// rather than fail an assertion.
func TestConcurrentReadersAndWritersMakeProgress(t *testing.T) {
	db := writableDurable(t, "users", keyedCorpus(), durable.Options{MaxBatchDocuments: 64})
	db.SetMaxOpenConns(8)

	const writers, readers, each = 4, 4, 20
	var wg sync.WaitGroup
	failures := make(chan error, (writers+readers)*each)

	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < each; i++ {
				key := fmt.Sprintf("w%d-%d", w, i)
				doc := fmt.Sprintf(`{"id":%d,"tier":"bulk"}`, 1000+w*each+i)
				if _, err := db.Exec(`INSERT INTO users VALUES (?, ?)`, key, []byte(doc)); err != nil {
					failures <- fmt.Errorf("insert %s: %w", key, err)
					return
				}
				if _, err := db.Exec(`DELETE FROM users WHERE "$key" = ?`, key); err != nil {
					failures <- fmt.Errorf("delete %s: %w", key, err)
					return
				}
			}
		}(w)
	}
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < each; i++ {
				var n int
				if err := db.QueryRow(`SELECT COUNT(*) FROM users WHERE tier = 'pro'`).Scan(&n); err != nil {
					failures <- fmt.Errorf("read: %w", err)
					return
				}
				// The pro-tier documents are never touched by the writers, so
				// every read sees the same count whatever generation it landed
				// on. A reader that saw a partial batch would not.
				if n != 3 {
					failures <- fmt.Errorf("a reader counted %d pro documents, want 3", n)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(failures)
	for err := range failures {
		t.Error(err)
	}
	if got := len(keysOf(t, db, "users")); got != len(corpus) {
		t.Errorf("collection holds %d documents, want the original %d: every insert was deleted", got, len(corpus))
	}
}
