package driver

import (
	"bytes"
	"context"
	stdsql "database/sql"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/query"
)

func seedGeneralizedJoinTables(t *testing.T, db *stdsql.DB) {
	t.Helper()
	for _, statement := range []string{
		`CREATE TABLE accounts (` +
			`id STRING PRIMARY KEY, tenant STRING, region STRING, name STRING)`,
		`CREATE TABLE orders (` +
			`id STRING PRIMARY KEY, account_id STRING, tenant STRING, region STRING, total INTEGER)`,
		`CREATE TABLE payments (` +
			`id STRING PRIMARY KEY, order_id STRING, state STRING)`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	for table, documents := range map[string][]string{
		"accounts": {
			`{"id":"a1","tenant":"t1","region":"north","name":"Ada"}`,
			`{"id":"a2","tenant":"t1","region":"south","name":"Bob"}`,
			`{"id":"a3","tenant":"t2","region":"west","name":"Cara"}`,
		},
		"orders": {
			`{"id":"o1","account_id":"a1","tenant":"t1","region":"north","total":10}`,
			`{"id":"o2","account_id":"a1","tenant":"t1","region":"north","total":20}`,
			`{"id":"o3","account_id":"a2","tenant":"t1","region":"south","total":30}`,
			`{"id":"orphan","account_id":"absent","tenant":"t9","region":"east","total":40}`,
		},
		"payments": {
			`{"id":"p1","order_id":"o1","state":"settled"}`,
			`{"id":"p2","order_id":"o2","state":"failed"}`,
			`{"id":"p3","order_id":"orphan","state":"settled"}`,
		},
	} {
		for _, document := range documents {
			if _, err := db.Exec(`INSERT INTO `+table+` VALUES (?)`, document); err != nil {
				t.Fatalf("insert %s: %v", table, err)
			}
		}
	}
}

func TestGeneralizedJoinChainResidualAndRelationOperands(t *testing.T) {
	db := openTestDB(t)
	seedGeneralizedJoinTables(t, db)

	rows, err := db.Query(`
		WITH eligible AS MATERIALIZED (
			SELECT id, name FROM accounts WHERE tenant = 't1'
		)
		SELECT e.name, o.id, p.state
		FROM eligible AS e
		JOIN (
			SELECT id, account_id, total FROM orders WHERE total >= ?
		) AS o ON e.id = o.account_id AND o.total <= ?
		LEFT JOIN payments AS p
			ON o.id = p.order_id AND p.state = 'settled'
		ORDER BY o.id`, int64(10), int64(30))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	type joined struct {
		name, order string
		state       stdsql.NullString
	}
	var got []joined
	for rows.Next() {
		var row joined
		if err := rows.Scan(&row.name, &row.order, &row.state); err != nil {
			t.Fatal(err)
		}
		got = append(got, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	want := []joined{
		{name: "Ada", order: "o1", state: stdsql.NullString{String: "settled", Valid: true}},
		{name: "Ada", order: "o2"},
		{name: "Bob", order: "o3"},
	}
	if !slices.Equal(got, want) {
		t.Fatalf("generalized relation chain = %+v, want %+v", got, want)
	}
}

func TestGeneralizedFullCrossAndRightJoin(t *testing.T) {
	db := openTestDB(t)
	seedGeneralizedJoinTables(t, db)

	rows, err := db.Query(`
		SELECT a.name, o.id
		FROM accounts AS a
		FULL JOIN orders AS o ON a.id = o.account_id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	counts := make(map[string]int)
	for rows.Next() {
		var name, order stdsql.NullString
		if err := rows.Scan(&name, &order); err != nil {
			t.Fatal(err)
		}
		counts[name.String+"/"+order.String]++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	want := map[string]int{
		"Ada/o1": 1, "Ada/o2": 1, "Bob/o3": 1,
		"Cara/": 1, "/orphan": 1,
	}
	if len(counts) != len(want) {
		t.Fatalf("FULL JOIN rows = %v, want %v", counts, want)
	}
	for key, count := range want {
		if counts[key] != count {
			t.Fatalf("FULL JOIN rows = %v, want %v", counts, want)
		}
	}

	var cross int64
	if err := db.QueryRow(`
		SELECT COUNT(*) FROM accounts CROSS JOIN payments`).Scan(&cross); err != nil {
		t.Fatal(err)
	}
	if cross != 9 {
		t.Fatalf("CROSS JOIN count = %d, want 9", cross)
	}

	var account stdsql.NullString
	if err := db.QueryRow(`
		SELECT a.name
		FROM accounts AS a
		RIGHT JOIN orders AS o ON a.id = o.account_id
		WHERE o.id = 'orphan'`).Scan(&account); err != nil {
		t.Fatal(err)
	}
	if account.Valid {
		t.Fatalf("RIGHT JOIN unmatched account = %q, want NULL", account.String)
	}
}

func TestGeneralizedCompositeUsingAndStableNames(t *testing.T) {
	db := openTestDB(t)
	for _, statement := range []string{
		`CREATE TABLE join_left (` +
			`pk STRING PRIMARY KEY, tenant STRING, id STRING, value STRING)`,
		`CREATE TABLE join_right (` +
			`pk STRING PRIMARY KEY, tenant STRING, id STRING, value STRING)`,
		`INSERT INTO join_left VALUES (` +
			`'{"pk":"l1","tenant":"t1","id":"x","value":"left-x"}'), (` +
			`'{"pk":"l2","tenant":"t1","id":"y","value":"left-y"}')`,
		`INSERT INTO join_right VALUES (` +
			`'{"pk":"r1","tenant":"t1","id":"x","value":"right-x"}'), (` +
			`'{"pk":"r2","tenant":"t2","id":"x","value":"right-other"}')`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	rows, err := db.Query(`
		SELECT l.id, r.id, l.value, r.value
		FROM join_left AS l
		JOIN join_right AS r USING (tenant, id)`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"id", "r.id", "value", "r.value"}; !slices.Equal(columns, want) {
		t.Fatalf("composite join column names = %v, want %v", columns, want)
	}
	if !rows.Next() {
		t.Fatal("composite USING returned no row")
	}
	var leftID, rightID, leftValue, rightValue string
	if err := rows.Scan(&leftID, &rightID, &leftValue, &rightValue); err != nil {
		t.Fatal(err)
	}
	if leftID != "x" || rightID != "x" || leftValue != "left-x" || rightValue != "right-x" {
		t.Fatalf("composite USING row = %q/%q/%q/%q", leftID, rightID, leftValue, rightValue)
	}
	if rows.Next() {
		t.Fatal("composite USING returned an extra row")
	}
}

func TestGeneralizedJoinPreparedAndExplainRevalidateDependencies(t *testing.T) {
	db := openTestDB(t)
	seedGeneralizedJoinTables(t, db)
	queryText := `
		WITH eligible AS (SELECT id FROM accounts)
		SELECT eligible.id, p.state
		FROM eligible
		JOIN (SELECT id, account_id FROM orders) AS o
			ON eligible.id = o.account_id
		LEFT JOIN payments AS p ON o.id = p.order_id`
	queryStmt, err := db.Prepare(queryText)
	if err != nil {
		t.Fatal(err)
	}
	defer queryStmt.Close()
	explainStmt, err := db.Prepare(`EXPLAIN ` + queryText)
	if err != nil {
		t.Fatal(err)
	}
	defer explainStmt.Close()
	analyzeStmt, err := db.Prepare(`EXPLAIN ANALYZE ` + queryText)
	if err != nil {
		t.Fatal(err)
	}
	defer analyzeStmt.Close()
	for name, statement := range map[string]*stdsql.Stmt{
		"query": queryStmt, "explain": explainStmt, "analyze": analyzeStmt,
	} {
		rows, err := statement.Query()
		if err != nil {
			t.Fatalf("prepared %s before DROP: %v", name, err)
		}
		_ = rows.Close()
	}
	if _, err := db.Exec(`DROP TABLE payments`); err != nil {
		t.Fatal(err)
	}
	for name, statement := range map[string]*stdsql.Stmt{
		"query": queryStmt, "explain": explainStmt, "analyze": analyzeStmt,
	} {
		rows, err := statement.Query()
		if rows != nil {
			_ = rows.Close()
		}
		if !errors.Is(err, ErrTableNotFound) {
			t.Fatalf("prepared %s after DROP = %v, want ErrTableNotFound", name, err)
		}
	}
	rows, err := db.Query(`EXPLAIN ` + queryText)
	if rows != nil {
		_ = rows.Close()
	}
	if !errors.Is(err, ErrTableNotFound) {
		t.Fatalf("plain EXPLAIN after DROP = %v, want ErrTableNotFound", err)
	}
}

func TestGeneralizedJoinTransactionSnapshotAndReadYourWrites(t *testing.T) {
	db := openTestDB(t)
	db.SetMaxOpenConns(4)
	seedGeneralizedJoinTables(t, db)
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := db.Exec(`INSERT INTO orders VALUES (?)`,
		`{"id":"outside","account_id":"a1","tenant":"t1","region":"north","total":50}`,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO orders VALUES (?)`,
		`{"id":"pending","account_id":"a1","tenant":"t1","region":"north","total":60}`,
	); err != nil {
		t.Fatal(err)
	}
	rows, err := tx.Query(`
		WITH selected AS MATERIALIZED (
			SELECT id, account_id FROM orders
		)
		SELECT o.id
		FROM accounts AS a
		JOIN selected AS o ON a.id = o.account_id
		WHERE a.id = 'a1'
		ORDER BY o.id`)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := scanGeneralizedJoinIDs(t, rows), []string{"o1", "o2", "pending"}; !slices.Equal(got, want) {
		t.Fatalf("transaction joined rows = %v, want %v", got, want)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
}

func TestGeneralizedJoinBudgetCancellationAndRecovery(t *testing.T) {
	ctx := context.Background()
	database, session := openRuntimeSession(t)
	defer database.Close()
	defer session.Close()
	for _, statement := range []string{
		`CREATE TABLE accounts (id STRING PRIMARY KEY)`,
		`CREATE TABLE orders (id STRING PRIMARY KEY, account_id STRING)`,
		`INSERT INTO accounts VALUES ('{"id":"a1"}')`,
		`INSERT INTO orders VALUES ('{"id":"o1","account_id":"a1"}')`,
	} {
		prepared := runtimePrepare(t, session, statement)
		if _, err := prepared.Exec(ctx, nil); err != nil {
			t.Fatal(err)
		}
	}
	prepared := runtimePrepare(t, session, `
		SELECT a.id, o.id
		FROM accounts AS a
		JOIN (SELECT id, account_id FROM orders) AS o
			ON a.id = o.account_id`)
	if err := session.SetIntermediateLimit(1); err != nil {
		t.Fatal(err)
	}
	cursor, err := prepared.Query(ctx, nil)
	if cursor != nil {
		_ = cursor.Close()
		t.Fatal("intermediate failure published a partial cursor")
	}
	if !errors.Is(err, query.ErrIntermediateBudget) {
		t.Fatalf("join intermediate limit = %v, want ErrIntermediateBudget", err)
	}
	if err := session.SetIntermediateLimit(-1); err != nil {
		t.Fatal(err)
	}
	var cancel query.CancelFlag
	if err := session.SetCancelFlag(&cancel); err != nil {
		t.Fatal(err)
	}
	cancel.Cancel()
	cursor, err = prepared.Query(ctx, nil)
	if cursor != nil {
		_ = cursor.Close()
		t.Fatal("canceled join published a partial cursor")
	}
	if !errors.Is(err, query.ErrCanceled) {
		t.Fatalf("canceled join = %v, want ErrCanceled", err)
	}
	cancel.Reset()
	cursor, err = prepared.Query(ctx, nil)
	if err != nil {
		t.Fatalf("join after cancellation: %v", err)
	}
	if !cursor.Next() {
		t.Fatal("join after cancellation returned no row")
	}
	if err := cursor.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestGeneralizedJoinSourceClassificationPreservesLegacyAndPointBaselines(t *testing.T) {
	ctx := context.Background()
	database, session := openRuntimeSession(t)
	defer database.Close()
	defer session.Close()
	for _, statement := range []string{
		`CREATE TABLE accounts (` +
			`id STRING PRIMARY KEY, tenant STRING, name STRING)`,
		`CREATE TABLE orders (id STRING PRIMARY KEY, account_id STRING)`,
		`CREATE INDEX accounts_tenant_exact ON accounts (tenant)`,
		`INSERT INTO accounts VALUES (` +
			`'{"id":"a1","tenant":"t1","name":"Ada"}'), (` +
			`'{"id":"a2","tenant":"t2","name":"Bob"}')`,
		`INSERT INTO orders VALUES (` +
			`'{"id":"o1","account_id":"a1"}')`,
	} {
		prepared := runtimePrepare(t, session, statement)
		if _, err := prepared.Exec(ctx, nil); err != nil {
			t.Fatal(err)
		}
	}
	joined := runtimePrepare(t, session, `
		SELECT a.id
		FROM accounts AS a
		JOIN orders AS o ON a.id = o.account_id
		WHERE a.tenant = ?`)
	if joined.statement.usesDirectDurableCatalog() {
		t.Fatal("legacy physical equi-join was moved onto the generalized source path")
	}
	cursor, err := joined.Query(ctx, []any{"t1"})
	if err != nil {
		t.Fatal(err)
	}
	if !cursor.Next() || cursor.Cell(0).String() != `"a1"` || cursor.Next() {
		t.Fatal("durable indexed semi-join returned the wrong rows")
	}
	if err := cursor.Close(); err != nil {
		t.Fatal(err)
	}
	stats := session.conn.exec.Stats
	if stats.JoinBuilds+stats.JoinMemberships+stats.JoinLookups == 0 {
		t.Fatalf("legacy physical join lost its join strategy: %+v", stats)
	}

	generalized := runtimePrepare(t, session, `
		WITH filtered AS (
			SELECT id FROM accounts WHERE tenant = ?
		)
		SELECT filtered.id
		FROM filtered
		JOIN orders AS o ON filtered.id = o.account_id`)
	if !generalized.statement.usesDirectDurableCatalog() {
		t.Fatal("CTE relation join did not select the direct durable catalog")
	}
	joinArgs := []any{"t1"}
	wantJoinedID := []byte(`"a1"`)
	var reused Cursor
	runJoined := func() {
		if err := generalized.QueryInto(ctx, joinArgs, &reused); err != nil {
			panic(err)
		}
		if !reused.Next() || !bytes.Equal(reused.Cell(0).JSON(), wantJoinedID) || reused.Next() {
			panic("warmed durable join returned the wrong rows")
		}
		if err := reused.Close(); err != nil {
			panic(err)
		}
	}
	runJoined()
	if allocs := testing.AllocsPerRun(200, runJoined); allocs != 0 {
		t.Fatalf("warmed generalized join allocated %.2f times, want zero", allocs)
	}

	point := runtimePrepare(t, session,
		`SELECT name FROM accounts WHERE id = ?`)
	if !point.statement.primaryPoint || point.statement.requiresCatalogSource() ||
		point.statement.usesDirectDurableCatalog() {
		t.Fatal("ordinary point query baseline changed under generalized joins")
	}
	if allocs := testing.AllocsPerRun(1000, func() {
		if point.statement.requiresCatalogSource() {
			panic("ordinary point query requires a catalog")
		}
	}); allocs != 0 {
		t.Fatalf("ordinary point-query classification allocated %.2f times", allocs)
	}
}

func scanGeneralizedJoinIDs(t *testing.T, rows *stdsql.Rows) []string {
	t.Helper()
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return ids
}

func TestGeneralizedJoinExplainAnalyzeReportsRelationPlan(t *testing.T) {
	db := openTestDB(t)
	seedGeneralizedJoinTables(t, db)
	var plan string
	if err := db.QueryRow(`
		EXPLAIN ANALYZE
		SELECT a.name, o.id, p.state
		FROM accounts AS a
		JOIN orders AS o ON a.id = o.account_id
		LEFT JOIN payments AS p ON o.id = p.order_id`).Scan(&plan); err != nil {
		t.Fatal(err)
	}
	for _, marker := range []string{`"joins":`, `"analyze":`, `"rows":`} {
		if !strings.Contains(plan, marker) {
			t.Fatalf("EXPLAIN ANALYZE missing %s: %s", marker, plan)
		}
	}
}
