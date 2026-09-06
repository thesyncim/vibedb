package shardservice

import (
	"context"
	"fmt"
	"testing"

	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

// TestShardStmtCacheReusesAndInvalidates pins the per-connection preparation
// cache contract directly: an identical request reuses the same lowered plan,
// preparations made under a transaction are never cached, a DDL publish
// retires the entry, and the cache stays bounded.
func TestShardStmtCacheReusesAndInvalidates(t *testing.T) {
	srv, _ := newServer(t, Options{})
	conn := dial(t, srv)
	exec(t, conn, ownedRequest(ddlDocs))
	exec(t, conn, ownedRequest(
		`INSERT INTO docs (id, name, n) VALUES (?, ?, ?)`,
		StringParam("a"), StringParam("Ada"), NumberParam("7")))

	sess, err := srv.db.NewSession(context.Background())
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	t.Cleanup(func() { _ = sess.Close() })
	c := &shardConn{server: srv, sess: sess}
	ctx := context.Background()

	const pointRead = `SELECT name FROM docs WHERE id = ?`
	first, release, err := c.prepareCached(ctx, pointRead, nil, false)
	if err != nil {
		t.Fatalf("prepareCached: %v", err)
	}
	release()
	second, release, err := c.prepareCached(ctx, pointRead, nil, false)
	if err != nil {
		t.Fatalf("prepareCached: %v", err)
	}
	release()
	if second != first {
		t.Fatalf("repeated prepare returned distinct statements, want cache reuse")
	}
	if got := len(c.stmtCache); got != 1 {
		t.Fatalf("cache entries = %d, want 1", got)
	}

	// A preparation made under a transaction observes the begin layout, not
	// the published generation, so it must never enter the cache. A distinct
	// text isolates this from the entry cached above.
	const txRead = `SELECT n FROM docs WHERE id = ?`
	if err := sess.Begin(ctx, sqldriver.TxOptions{
		ReadOnly: true, Isolation: sqldriver.IsolationRepeatableRead,
	}); err != nil {
		t.Fatalf("Begin: %v", err)
	}
	txFirst, release, err := c.prepareCached(ctx, txRead, nil, false)
	if err != nil {
		t.Fatalf("prepareCached in transaction: %v", err)
	}
	release()
	if txFirst.LayoutCurrent() {
		t.Fatalf("in-transaction prepare claims a reusable layout, want unstamped")
	}
	txSecond, release, err := c.prepareCached(ctx, txRead, nil, false)
	if err != nil {
		t.Fatalf("prepareCached in transaction: %v", err)
	}
	release()
	if err := sess.Rollback(context.Background()); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	if txSecond == txFirst {
		t.Fatalf("in-transaction prepare was cached, want a fresh plan per request")
	}
	if got := len(c.stmtCache); got != 1 {
		t.Fatalf("cache entries after transaction = %d, want 1", got)
	}

	// Any DDL publish retires the entry: the next identical request must
	// lower a fresh plan instead of serving the pre-DDL one.
	exec(t, conn, ownedRequest(`CREATE TABLE retired (id STRING PRIMARY KEY)`))
	third, release, err := c.prepareCached(ctx, pointRead, nil, false)
	if err != nil {
		t.Fatalf("prepareCached after DDL: %v", err)
	}
	release()
	if third == first {
		t.Fatalf("post-DDL prepare reused the pre-DDL plan, want invalidation")
	}
	if got := len(c.stmtCache); got != 1 {
		t.Fatalf("cache entries after DDL = %d, want 1", got)
	}

	// Sixteen further distinct statements evict the oldest entry first.
	for i := range shardStmtCacheMax {
		stmt := fmt.Sprintf(`SELECT n FROM docs WHERE n > %d`, i)
		if _, release, err := c.prepareCached(ctx, stmt, nil, false); err != nil {
			t.Fatalf("prepareCached %q: %v", stmt, err)
		} else {
			release()
		}
	}
	if got := len(c.stmtCache); got != shardStmtCacheMax {
		t.Fatalf("cache entries = %d, want bound %d", got, shardStmtCacheMax)
	}
	fourth, release, err := c.prepareCached(ctx, pointRead, nil, false)
	if err != nil {
		t.Fatalf("prepareCached after eviction: %v", err)
	}
	release()
	if fourth == third {
		t.Fatalf("evicted statement was still served, want FIFO eviction")
	}
}

// TestShardStmtCacheEndToEndInvalidation drives the cache through a real
// connection: repeated reads stay correct, a DROP is observed immediately
// (no stale plan serves the retired table), and recreating the table
// recovers without restarting the connection.
func TestShardStmtCacheEndToEndInvalidation(t *testing.T) {
	srv, _ := newServer(t, Options{})
	conn := dial(t, srv)
	exec(t, conn, ownedRequest(ddlDocs))
	exec(t, conn, ownedRequest(
		`INSERT INTO docs (id, name, n) VALUES (?, ?, ?)`,
		StringParam("a"), StringParam("Ada"), NumberParam("7")))

	const pointRead = `SELECT n FROM docs WHERE id = ?`
	read := ownedRequest(pointRead, StringParam("a"))
	read.ExecutionMode = ExecutionReadOnly
	for range 3 {
		got := exec(t, conn, read)
		if len(got.Rows) != 1 || cellText(t, got, 0, 0) != "7" {
			t.Fatalf("point read rows = %+v, want one row with n = 7", got.Rows)
		}
	}

	exec(t, conn, ownedRequest(`DROP TABLE docs`))
	if got := roundTrip(t, conn, read); got.Kind != ResponseError {
		t.Fatalf("post-DROP read kind = %s, want Error", got.Kind)
	}

	exec(t, conn, ownedRequest(ddlDocs))
	exec(t, conn, ownedRequest(
		`INSERT INTO docs (id, name, n) VALUES (?, ?, ?)`,
		StringParam("a"), StringParam("Ada"), NumberParam("7")))
	got := exec(t, conn, read)
	if len(got.Rows) != 1 || cellText(t, got, 0, 0) != "7" {
		t.Fatalf("post-recreate read rows = %+v, want one row with n = 7", got.Rows)
	}
}
