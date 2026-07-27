package vibesql

import (
	"database/sql"
	"fmt"
	"strconv"
	"testing"

	"github.com/thesyncim/vibedb/query"
	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
)

// What the driver costs on top of the engine.
//
// Every shape is measured three ways over one corpus, so the differences are
// attributable rather than assumed:
//
//	Native      the builder query, one query.Exec, RunInto, cells read directly
//	Prepared    a *sql.Stmt reused across executions
//	AdHoc       db.Query with the statement text, parsed and compiled per call
//
// Native is the floor. Prepared minus Native is what database/sql and this
// package add per execution: the pool handoff, the driver.Rows, the Scan
// conversions, and — for a statement with placeholders — one re-lowering of the
// plan. AdHoc minus Prepared is the parse and compile an unprepared query pays
// on every call, which is the number the package documentation refuses to hide.

const benchRows = 2000

// benchCorpus builds a corpus large enough that per-row work dominates the
// per-execution overhead in the scan benchmarks and small enough to stay in
// cache.
func benchCorpus() []string {
	tiers := []string{"free", "pro", "team"}
	docs := make([]string, 0, benchRows)
	for i := 0; i < benchRows; i++ {
		docs = append(docs, fmt.Sprintf(
			`{"id":%d,"name":"user-%d","age":%d,"tier":%q,"score":%d}`,
			i, i, 18+i%60, tiers[i%3], i*7%1000))
	}
	return docs
}

// benchDatabase attaches a heap database holding the corpus and returns the
// *sql.DB, the equivalent Segment, and the store.Database the join benchmark
// needs.
func benchDatabase(b *testing.B, collections map[string][]string) (*sql.DB, *store.Database) {
	b.Helper()
	db := &store.Database{}
	for name, docs := range collections {
		collection, err := db.CreateCollection(name, store.Options{})
		if err != nil {
			b.Fatal(err)
		}
		for i, doc := range docs {
			if _, err := collection.Put(strconv.Itoa(i), []byte(doc)); err != nil {
				b.Fatal(err)
			}
		}
	}
	name := fmt.Sprintf("b%p", db)
	dsn, err := Attach(name, db)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { Detach(name) })
	handle, err := sql.Open("vibejson", dsn)
	if err != nil {
		b.Fatal(err)
	}
	handle.SetMaxOpenConns(1)
	b.Cleanup(func() { _ = handle.Close() })
	return handle, db
}

// benchNative runs the builder query against exactly the source the driver
// resolves for the same statement — the same database, the same per-execution
// snapshot — so the difference between the two is the front end and nothing
// else. Comparing against a bare Segment instead would have measured the
// absence of the snapshot's posting indexes and called it driver overhead.
func benchNative(b *testing.B, db *store.Database, collection string, q *query.Query) {
	b.Helper()
	if err := q.Prepare(); err != nil {
		b.Fatal(err)
	}
	var e query.Exec
	if err := q.RunInto(&e, query.FromDatabase(db.Snapshot(), collection)); err != nil {
		b.Fatal(err)
	}
	sinkResult(b, &e.Result)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := q.RunInto(&e, query.FromDatabase(db.Snapshot(), collection)); err != nil {
			b.Fatal(err)
		}
		sinkResult(b, &e.Result)
	}
}

// --- point query ------------------------------------------------------------
//
// A single-row lookup by an equality on a top-level field. It is the shape
// where per-execution overhead is the whole cost, so it is where the driver's
// own price is most visible.
//
// It is spelled as an equality rather than as a primary-key probe because the
// '$key' pseudo-path is an outer-path form the engine does not yet have; when
// it lands, this statement becomes a keyed lookup without any change here.

func BenchmarkPointNative(b *testing.B) {
	_, db := benchDatabase(b, map[string][]string{"docs": benchCorpus()})
	benchNative(b, db, "docs", query.Select(query.Path("name"), query.Path("score")).
		Where(query.Cmp("id", query.Eq, 1234)))
}

func BenchmarkPointPrepared(b *testing.B) {
	db, _ := benchDatabase(b, map[string][]string{"docs": benchCorpus()})
	stmt, err := db.Prepare(`SELECT name, score FROM docs WHERE id = ?`)
	if err != nil {
		b.Fatal(err)
	}
	defer stmt.Close()
	benchScan(b, func() (*sql.Rows, error) { return stmt.Query(1234) })
}

func BenchmarkPointPreparedLiteral(b *testing.B) {
	db, _ := benchDatabase(b, map[string][]string{"docs": benchCorpus()})
	stmt, err := db.Prepare(`SELECT name, score FROM docs WHERE id = 1234`)
	if err != nil {
		b.Fatal(err)
	}
	defer stmt.Close()
	benchScan(b, func() (*sql.Rows, error) { return stmt.Query() })
}

func BenchmarkPointAdHoc(b *testing.B) {
	db, _ := benchDatabase(b, map[string][]string{"docs": benchCorpus()})
	benchScan(b, func() (*sql.Rows, error) {
		return db.Query(`SELECT name, score FROM docs WHERE id = ?`, 1234)
	})
}

// --- filtered scan ----------------------------------------------------------

func BenchmarkScanNative(b *testing.B) {
	_, db := benchDatabase(b, map[string][]string{"docs": benchCorpus()})
	benchNative(b, db, "docs", query.Select(query.Path("name"), query.Path("score")).
		Where(query.Cmp("age", query.Ge, 60)))
}

func BenchmarkScanPrepared(b *testing.B) {
	db, _ := benchDatabase(b, map[string][]string{"docs": benchCorpus()})
	stmt, err := db.Prepare(`SELECT name, score FROM docs WHERE age >= ?`)
	if err != nil {
		b.Fatal(err)
	}
	defer stmt.Close()
	benchScan(b, func() (*sql.Rows, error) { return stmt.Query(60) })
}

func BenchmarkScanAdHoc(b *testing.B) {
	db, _ := benchDatabase(b, map[string][]string{"docs": benchCorpus()})
	benchScan(b, func() (*sql.Rows, error) {
		return db.Query(`SELECT name, score FROM docs WHERE age >= ?`, 60)
	})
}

// --- grouped aggregate ------------------------------------------------------

func BenchmarkGroupNative(b *testing.B) {
	_, db := benchDatabase(b, map[string][]string{"docs": benchCorpus()})
	benchNative(b, db, "docs", query.Select(query.Path("tier"), query.Count(), query.Sum("score")).
		GroupBy("tier").OrderBy("tier", query.Asc))
}

func BenchmarkGroupPrepared(b *testing.B) {
	db, _ := benchDatabase(b, map[string][]string{"docs": benchCorpus()})
	stmt, err := db.Prepare(
		`SELECT tier, COUNT(*), SUM(score) FROM docs GROUP BY tier ORDER BY tier`)
	if err != nil {
		b.Fatal(err)
	}
	defer stmt.Close()
	benchScan(b, func() (*sql.Rows, error) { return stmt.Query() })
}

func BenchmarkGroupAdHoc(b *testing.B) {
	db, _ := benchDatabase(b, map[string][]string{"docs": benchCorpus()})
	benchScan(b, func() (*sql.Rows, error) {
		return db.Query(`SELECT tier, COUNT(*), SUM(score) FROM docs GROUP BY tier ORDER BY tier`)
	})
}

// --- join -------------------------------------------------------------------
//
// Two shapes, because they take different operators. The fan-out arm is SQL's
// inner join: it projects a joined column and emits one row per matching pair,
// four per driving row here. The semi-join arm joins on the joined
// collection's primary key and reads nothing from it, which the planner proves
// equivalent to a filter and answers without materializing a match.

func benchJoinCollections() map[string][]string {
	orders := make([]string, 0, benchRows)
	for i := 0; i < benchRows; i++ {
		orders = append(orders, fmt.Sprintf(
			`{"id":%d,"cust":%q,"total":%d}`, i, strconv.Itoa(i%500), i*3%900))
	}
	// The customer documents carry a non-key "region" holding the same value
	// as their key, so the two arms join the same pairs by different paths:
	// one through the primary key, which the planner can prove unique, and one
	// through an ordinary field, which fans out. Four customers share each
	// region value, so the fan-out arm emits four rows per driving row.
	customers := make([]string, 0, 2000)
	tiers := []string{"free", "pro"}
	for i := 0; i < 2000; i++ {
		customers = append(customers, fmt.Sprintf(`{"tier":%q,"region":%q}`,
			tiers[i%2], strconv.Itoa(i%500)))
	}
	return map[string][]string{"orders": orders, "customers": customers}
}

func BenchmarkJoinNative(b *testing.B) {
	_, db := benchDatabase(b, benchJoinCollections())
	benchNative(b, db, "orders", query.Select(query.Path("id"), query.Path("c.tier")).
		Join(query.JoinOn("customers", "cust", "region").As("c")))
}

func BenchmarkJoinPrepared(b *testing.B) {
	db, _ := benchDatabase(b, benchJoinCollections())
	stmt, err := db.Prepare(
		`SELECT o.id, c.tier FROM orders o JOIN customers c ON c.region = o.cust`)
	if err != nil {
		b.Fatal(err)
	}
	defer stmt.Close()
	benchScan(b, func() (*sql.Rows, error) { return stmt.Query() })
}

func BenchmarkJoinAdHoc(b *testing.B) {
	db, _ := benchDatabase(b, benchJoinCollections())
	benchScan(b, func() (*sql.Rows, error) {
		return db.Query(
			`SELECT o.id, c.tier FROM orders o JOIN customers c ON c.region = o.cust`)
	})
}

func BenchmarkSemiJoinNative(b *testing.B) {
	_, db := benchDatabase(b, benchJoinCollections())
	benchNative(b, db, "orders", query.Select(query.Path("id"), query.Path("total")).
		Join(query.JoinOn("customers", "cust", query.JoinKey).As("c").
			Where(query.Cmp("tier", query.Eq, "pro"))))
}

func BenchmarkSemiJoinPrepared(b *testing.B) {
	db, _ := benchDatabase(b, benchJoinCollections())
	stmt, err := db.Prepare(
		`SELECT o.id, o.total FROM orders o JOIN customers c ON c."$key" = o.cust ` +
			`WHERE c.tier = ?`)
	if err != nil {
		b.Fatal(err)
	}
	defer stmt.Close()
	benchScan(b, func() (*sql.Rows, error) { return stmt.Query("pro") })
}

func BenchmarkSemiJoinAdHoc(b *testing.B) {
	db, _ := benchDatabase(b, benchJoinCollections())
	benchScan(b, func() (*sql.Rows, error) {
		return db.Query(
			`SELECT o.id, o.total FROM orders o JOIN customers c ON c."$key" = o.cust `+
				`WHERE c.tier = ?`, "pro")
	})
}

// --- harness ----------------------------------------------------------------

// benchScan runs one query per iteration and reads every row through
// sql.RawBytes, which is the one Scan destination database/sql does not copy
// into. Scanning into a string or a []byte would measure database/sql's own
// per-value copy, which this package neither performs nor can avoid; RawBytes
// measures what reaches the driver.
func benchScan(b *testing.B, run func() (*sql.Rows, error)) {
	b.Helper()
	// One warm-up execution, so the connection's Exec, the statement's
	// compiler arenas, and the pool are all at their steady state before the
	// timer starts.
	rows, err := run()
	if err != nil {
		b.Fatal(err)
	}
	columns, err := rows.Columns()
	if err != nil {
		b.Fatal(err)
	}
	drain(b, rows, len(columns))

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := run()
		if err != nil {
			b.Fatal(err)
		}
		drain(b, rows, len(columns))
	}
}

var sink int

func drain(b *testing.B, rows *sql.Rows, columns int) {
	b.Helper()
	cells := make([]sql.RawBytes, columns)
	into := make([]any, columns)
	for i := range cells {
		into[i] = &cells[i]
	}
	for rows.Next() {
		if err := rows.Scan(into...); err != nil {
			b.Fatal(err)
		}
		for _, cell := range cells {
			sink += len(cell)
		}
	}
	if err := rows.Err(); err != nil {
		b.Fatal(err)
	}
	if err := rows.Close(); err != nil {
		b.Fatal(err)
	}
}

// sinkResult reads every cell of a native result, so the builder side does the
// same materialization work the driver side does.
func sinkResult(b *testing.B, r *query.Result) {
	b.Helper()
	for row := 0; row < r.RowCount; row++ {
		for _, column := range r.Columns {
			sink += len(column.Cells[row].Payload())
		}
	}
}

// --- the write path ----------------------------------------------------------

// What a mutation costs, split into the two shapes whose costs are entirely
// different things.
//
// A keyed write is dominated by the store: one durable generation per
// statement, which is a document-page rebuild, a copy-on-write descent per
// directory, a state root, and a fence. The SQL layer's contribution to it is
// noise.
//
// A filtered scan is dominated by this package: it reads every document and
// runs the compiled predicate over it. What the benchmark is watching there is
// the allocation count, which must be constant in the number of documents
// scanned — the batch buffers and the scratch Segment are retained on the
// prepared statement, so a scan that examines five hundred documents allocates
// the same handful as one that examines five.

func benchWritableCollection(b *testing.B, documents int) *sql.DB {
	b.Helper()
	docs := make(map[string]string, documents)
	for i := 0; i < documents; i++ {
		docs[strconv.Itoa(i)] = fmt.Sprintf(`{"id":%d,"tier":"pro","age":%d}`, i, i%80)
	}
	return writableDurable(b, "users", docs, durable.Options{MaxBatchDocuments: 64})
}

// BenchmarkWriteKeyed measures one INSERT and one keyed DELETE, which is two
// published generations and no scan at all.
func BenchmarkWriteKeyed(b *testing.B) {
	db := benchWritableCollection(b, 500)
	db.SetMaxOpenConns(1)
	insert, err := db.Prepare(`INSERT INTO users VALUES (?, ?)`)
	if err != nil {
		b.Fatal(err)
	}
	remove, err := db.Prepare(`DELETE FROM users WHERE "$key" = ?`)
	if err != nil {
		b.Fatal(err)
	}
	document := []byte(`{"id":9999,"tier":"bench"}`)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := insert.Exec("bench", document); err != nil {
			b.Fatal(err)
		}
		if _, err := remove.Exec("bench"); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkWriteFilteredScan measures a filtered DELETE whose condition matches
// nothing, so every iteration scans the whole collection and publishes no
// generation. That isolates the filter pass: the per-document cost of the
// scan and the predicate, with none of the store's commit cost mixed in.
func BenchmarkWriteFilteredScan(b *testing.B) {
	const documents = 500
	db := benchWritableCollection(b, documents)
	db.SetMaxOpenConns(1)
	stmt, err := db.Prepare(`DELETE FROM users WHERE tier = 'absent'`)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := stmt.Exec(); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(documents), "docs/op")
}

// BenchmarkTransactionReadOnlySelect is the retained-snapshot control. With no
// staged writes a transaction hands the ordinary FromFile source to the query
// executor, so this must track the durable read path without overlay work.
func BenchmarkTransactionReadOnlySelect(b *testing.B) {
	benchmarkTransactionSelect(b, false)
}

// BenchmarkTransactionStagedSelect measures the bounded overlay merge: one map
// probe per base row plus one staged insert, without copying the collection.
func BenchmarkTransactionStagedSelect(b *testing.B) {
	benchmarkTransactionSelect(b, true)
}

// BenchmarkTransactionIndexedStagedSelect measures the correction lane for an
// exact-index predicate. Base candidates remain bounded by the persistent
// posting masks and the staged set is evaluated separately.
func BenchmarkTransactionIndexedStagedSelect(b *testing.B) {
	const documents = 2000
	docs := make(map[string]string, documents)
	baseMatches := 0
	for i := 0; i < documents; i++ {
		age := i % 80
		if age == 42 {
			baseMatches++
		}
		docs[strconv.Itoa(i)] = fmt.Sprintf(`{"id":%d,"tier":"pro","age":%d}`, i, age)
	}
	db := writableDurable(b, "users", docs, durable.Options{
		Indexes:           []store.IndexDefinition{{Name: "age", Paths: []string{"/age"}}},
		MaxBatchDocuments: 64,
	})
	db.SetMaxOpenConns(1)
	tx, err := db.Begin()
	if err != nil {
		b.Fatal(err)
	}
	if _, err := tx.Exec(
		`UPDATE users SET "$doc" = ? WHERE "$key" = '0'`,
		[]byte(`{"id":0,"tier":"pro","age":42}`),
	); err != nil {
		b.Fatal(err)
	}
	if _, err := tx.Exec(
		`INSERT INTO users VALUES ('overlay', {"id":999999,"tier":"pro","age":42})`,
	); err != nil {
		b.Fatal(err)
	}
	stmt, err := tx.Prepare(`SELECT COUNT(*) FROM users WHERE age = 42`)
	if err != nil {
		b.Fatal(err)
	}
	var count int
	if err := stmt.QueryRow().Scan(&count); err != nil {
		b.Fatal(err)
	}
	if want := baseMatches + 2; count != want {
		b.Fatalf("count = %d, want %d", count, want)
	}

	b.ReportAllocs()
	b.ReportMetric(float64(baseMatches), "base_candidates/op")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := stmt.QueryRow().Scan(&count); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if err := stmt.Close(); err != nil {
		b.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		b.Fatal(err)
	}
}

func benchmarkTransactionSelect(b *testing.B, staged bool) {
	const documents = 2000
	db := benchWritableCollection(b, documents)
	db.SetMaxOpenConns(1)
	tx, err := db.Begin()
	if err != nil {
		b.Fatal(err)
	}
	if staged {
		if _, err := tx.Exec(
			`INSERT INTO users VALUES ('overlay', {"id":999999,"tier":"pro","age":42})`,
		); err != nil {
			b.Fatal(err)
		}
	}
	stmt, err := tx.Prepare(`SELECT COUNT(*) FROM users WHERE age >= 0`)
	if err != nil {
		b.Fatal(err)
	}
	want := documents
	if staged {
		want++
	}
	var count int
	if err := stmt.QueryRow().Scan(&count); err != nil {
		b.Fatal(err)
	}
	if count != want {
		b.Fatalf("count = %d, want %d", count, want)
	}

	b.ReportAllocs()
	b.ReportMetric(float64(want), "docs/op")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := stmt.QueryRow().Scan(&count); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if err := stmt.Close(); err != nil {
		b.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		b.Fatal(err)
	}
}
