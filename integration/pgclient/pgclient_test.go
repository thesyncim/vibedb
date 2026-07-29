package pgclient_test

import (
	"context"
	stdsql "database/sql"
	"errors"
	"net"
	"net/url"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/lib/pq"
	"github.com/thesyncim/vibedb/pgwire"
	vibedriver "github.com/thesyncim/vibedb/sql/driver"
)

const (
	testUser     = "release-gate"
	testPassword = "correct horse battery staple"
	testDatabase = "app"
	exactNumber  = "9007199254740993123"
)

// TestPostgreSQLClients is the release interoperability gate for the two
// PostgreSQL clients this repository claims. It uses real loopback TCP, SCRAM,
// the extended and simple protocols, writable catalog DDL/DML, indexes, joins,
// explicit transactions, SQLSTATEs, and a close/reopen durability cycle.
func TestPostgreSQLClients(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	path := filepath.Join(t.TempDir(), "catalog.vdb")
	database := openCatalog(t, path)
	first, dsn := serveCatalog(t, database)

	testPGX(t, ctx, dsn)
	testLibPQ(t, ctx, dsn)

	first.stop()
	if err := database.Close(); err != nil {
		t.Fatalf("close first catalog owner: %v", err)
	}

	reopened := openCatalog(t, path)
	second, reopenedDSN := serveCatalog(t, reopened)
	testReopenedCatalog(t, ctx, reopenedDSN)
	second.stop()
	if err := reopened.Close(); err != nil {
		t.Fatalf("close reopened catalog owner: %v", err)
	}
}

func testPGX(t *testing.T, ctx context.Context, dsn string) {
	t.Helper()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("pgx connect over SCRAM: %v", err)
	}
	defer func() {
		if err := conn.Close(context.Background()); err != nil {
			t.Errorf("pgx deferred close: %v", err)
		}
	}()

	mustPGXExec(t, ctx, conn, `
		CREATE TABLE users (
			id STRING PRIMARY KEY,
			name STRING NOT NULL,
			tier STRING,
			big NUMBER
		)`)
	mustPGXExec(t, ctx, conn, `CREATE INDEX users_by_tier ON users(tier)`)
	mustPGXExec(t, ctx, conn, `
		CREATE TABLE orders (
			id STRING PRIMARY KEY,
			user_id STRING NOT NULL,
			total NUMBER NOT NULL
		)`)
	mustPGXExec(t, ctx, conn, `CREATE INDEX orders_by_user ON orders(user_id)`)

	const insertUser = "insert_user"
	if _, err := conn.Prepare(ctx, insertUser, `INSERT INTO users VALUES ($1)`); err != nil {
		t.Fatalf("pgx prepare whole-document INSERT: %v", err)
	}
	if tag, err := conn.Exec(ctx, insertUser,
		`{"id":"u1","name":"Ada","tier":"pro","big":`+exactNumber+`}`); err != nil {
		t.Fatalf("pgx execute prepared INSERT: %v", err)
	} else if tag.RowsAffected() != 1 {
		t.Fatalf("pgx INSERT affected %d rows, want 1", tag.RowsAffected())
	}

	if _, err := conn.Exec(ctx, insertUser,
		`{"id":"u1","name":"duplicate","tier":"pro","big":1}`); pgxCode(err) != "23505" {
		t.Fatalf("pgx duplicate INSERT = %v (code %q), want 23505", err, pgxCode(err))
	}
	if _, err := conn.Exec(ctx, insertUser,
		`{"id":"missing-name","tier":"free","big":1}`); pgxCode(err) != "23514" {
		t.Fatalf("pgx schema violation = %v (code %q), want 23514", err, pgxCode(err))
	}
	var indexedUser []byte
	if err := conn.QueryRow(ctx,
		`SELECT id FROM users WHERE tier = $1`, "pro").Scan(&indexedUser); err != nil {
		t.Fatalf("pgx secondary-index predicate: %v", err)
	}
	if string(indexedUser) != `"u1"` {
		t.Fatalf("pgx secondary-index predicate returned %s, want %q",
			indexedUser, `"u1"`)
	}

	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatalf("pgx begin rollback transaction: %v", err)
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO orders VALUES ($1)`,
		`{"id":"rolled","user_id":"u1","total":1.25}`); err != nil {
		t.Fatalf("pgx transaction INSERT before rollback: %v", err)
	}
	var inside int64
	if err := tx.QueryRow(ctx,
		`SELECT COUNT(*) FROM orders WHERE id = $1`, "rolled").Scan(&inside); err != nil {
		t.Fatalf("pgx read-your-writes SELECT: %v", err)
	}
	if inside != 1 {
		t.Fatalf("pgx read-your-writes count = %d, want 1", inside)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("pgx rollback: %v", err)
	}

	tx, err = conn.Begin(ctx)
	if err != nil {
		t.Fatalf("pgx begin commit transaction: %v", err)
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO orders VALUES ($1)`,
		`{"id":"o1","user_id":"u1","total":12.50}`); err != nil {
		t.Fatalf("pgx transaction INSERT before commit: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("pgx commit: %v", err)
	}

	const join = "user_order"
	if _, err := conn.Prepare(ctx, join, `
		SELECT users.name, users.big, orders.total
		FROM users
		JOIN orders ON users.id = orders.user_id
		WHERE users.id = $1`); err != nil {
		t.Fatalf("pgx prepare JOIN: %v", err)
	}
	var name, big, total []byte
	if err := conn.QueryRow(ctx, join, "u1").Scan(&name, &big, &total); err != nil {
		t.Fatalf("pgx execute prepared JOIN: %v", err)
	}
	if string(name) != `"Ada"` || string(big) != exactNumber ||
		string(total) != "12.50" {
		t.Fatalf("pgx JOIN row = (%s, %s, %s)", name, big, total)
	}

	if tag, err := conn.Exec(ctx,
		`UPDATE users SET "$doc" = $1 WHERE id = $2`,
		`{"id":"u1","name":"Augusta","tier":"pro","big":`+exactNumber+`}`,
		"u1",
	); err != nil {
		t.Fatalf("pgx UPDATE whole document: %v", err)
	} else if tag.RowsAffected() != 1 {
		t.Fatalf("pgx UPDATE affected %d rows, want 1", tag.RowsAffected())
	}
}

func testLibPQ(t *testing.T, ctx context.Context, dsn string) {
	t.Helper()
	db, err := stdsql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("lib/pq open: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Errorf("lib/pq deferred close: %v", err)
		}
	}()
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("lib/pq ping over SCRAM: %v", err)
	}

	if _, err := db.ExecContext(ctx, `
		CREATE TABLE events (
			id STRING PRIMARY KEY,
			kind STRING NOT NULL,
			ok BOOLEAN NOT NULL,
			n INTEGER
		)`); err != nil {
		t.Fatalf("lib/pq CREATE TABLE: %v", err)
	}
	if _, err := db.ExecContext(ctx,
		`CREATE INDEX events_by_kind ON events(kind)`); err != nil {
		t.Fatalf("lib/pq CREATE INDEX: %v", err)
	}
	insert, err := db.PrepareContext(ctx, `INSERT INTO events VALUES ($1)`)
	if err != nil {
		t.Fatalf("lib/pq prepare INSERT: %v", err)
	}
	defer insert.Close()
	if _, err := insert.ExecContext(ctx,
		`{"id":"e1","kind":"release","ok":true,"n":7}`); err != nil {
		t.Fatalf("lib/pq execute prepared INSERT: %v", err)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("lib/pq begin: %v", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM events WHERE id = $1`, "e1"); err != nil {
		t.Fatalf("lib/pq transactional DELETE: %v", err)
	}
	var count int64
	if err := tx.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM events WHERE id = $1`, "e1").Scan(&count); err != nil {
		t.Fatalf("lib/pq transaction SELECT: %v", err)
	}
	if count != 0 {
		t.Fatalf("lib/pq transaction count = %d, want 0", count)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("lib/pq rollback: %v", err)
	}

	var kind, okValue, n []byte
	if err := db.QueryRowContext(ctx,
		`SELECT kind, ok, n FROM events WHERE id = $1`, "e1",
	).Scan(&kind, &okValue, &n); err != nil {
		t.Fatalf("lib/pq indexed SELECT: %v", err)
	}
	if string(kind) != `"release"` || string(okValue) != "true" || string(n) != "7" {
		t.Fatalf("lib/pq row = (%s, %s, %s)", kind, okValue, n)
	}
	var indexedEvent []byte
	if err := db.QueryRowContext(ctx,
		`SELECT id FROM events WHERE kind = $1`, "release").Scan(&indexedEvent); err != nil {
		t.Fatalf("lib/pq secondary-index predicate: %v", err)
	}
	if string(indexedEvent) != `"e1"` {
		t.Fatalf("lib/pq secondary-index predicate returned %s, want %q",
			indexedEvent, `"e1"`)
	}

	if _, err := db.ExecContext(ctx, `INSERT INTO events VALUES ($1)`,
		`{"id":"e1","kind":"duplicate","ok":false,"n":0}`); pqCode(err) != "23505" {
		t.Fatalf("lib/pq duplicate INSERT = %v (code %q), want 23505", err, pqCode(err))
	}
}

func testReopenedCatalog(t *testing.T, ctx context.Context, dsn string) {
	t.Helper()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("pgx reconnect after catalog reopen: %v", err)
	}
	defer conn.Close(context.Background())

	var user, order, event int64
	if err := conn.QueryRow(ctx, `SELECT COUNT(*) FROM users`).Scan(&user); err != nil {
		t.Fatalf("reopened users SELECT: %v", err)
	}
	if err := conn.QueryRow(ctx, `SELECT COUNT(*) FROM orders`).Scan(&order); err != nil {
		t.Fatalf("reopened orders SELECT: %v", err)
	}
	if err := conn.QueryRow(ctx, `SELECT COUNT(*) FROM events`).Scan(&event); err != nil {
		t.Fatalf("reopened events SELECT: %v", err)
	}
	if user != 1 || order != 1 || event != 1 {
		t.Fatalf("reopened counts = users:%d orders:%d events:%d, want 1/1/1",
			user, order, event)
	}
	var indexedUser, indexedOrder, indexedEvent []byte
	if err := conn.QueryRow(ctx,
		`SELECT id FROM users WHERE tier = 'pro'`).Scan(&indexedUser); err != nil {
		t.Fatalf("reopened users secondary index: %v", err)
	}
	if err := conn.QueryRow(ctx,
		`SELECT id FROM orders WHERE user_id = 'u1'`).Scan(&indexedOrder); err != nil {
		t.Fatalf("reopened orders secondary index: %v", err)
	}
	if err := conn.QueryRow(ctx,
		`SELECT id FROM events WHERE kind = 'release'`).Scan(&indexedEvent); err != nil {
		t.Fatalf("reopened events secondary index: %v", err)
	}
	if string(indexedUser) != `"u1"` || string(indexedOrder) != `"o1"` ||
		string(indexedEvent) != `"e1"` {
		t.Fatalf("reopened secondary-index rows = users:%s orders:%s events:%s",
			indexedUser, indexedOrder, indexedEvent)
	}
}

func mustPGXExec(t *testing.T, ctx context.Context, conn *pgx.Conn, sql string) {
	t.Helper()
	if _, err := conn.Exec(ctx, sql); err != nil {
		t.Fatalf("pgx Exec %q: %v", sql, err)
	}
}

func pgxCode(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}

func pqCode(err error) string {
	var pgErr *pq.Error
	if errors.As(err, &pgErr) {
		return string(pgErr.Code)
	}
	return ""
}

func openCatalog(t *testing.T, path string) *vibedriver.Database {
	t.Helper()
	database, err := vibedriver.Open(path)
	if err != nil {
		t.Fatalf("open SQL catalog: %v", err)
	}
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("cleanup SQL catalog: %v", err)
		}
	})
	return database
}

type catalogServer struct {
	t        *testing.T
	server   *pgwire.Server
	listener net.Listener
	served   chan error
	stopOnce sync.Once
}

func serveCatalog(t *testing.T, database *vibedriver.Database) (*catalogServer, string) {
	t.Helper()
	return serveCatalogAt(t, database, "127.0.0.1:0", nil)
}

func serveCatalogAt(
	t *testing.T,
	database *vibedriver.Database,
	address string,
	configure func(*pgwire.Options),
) (*catalogServer, string) {
	t.Helper()
	verifier, err := pgwire.NewVerifier(testPassword)
	if err != nil {
		t.Fatalf("derive SCRAM verifier: %v", err)
	}
	options := pgwire.Options{
		Auth: pgwire.SCRAM(func(user string) (pgwire.Verifier, bool) {
			return verifier, user == testUser
		}),
		Database: testDatabase,
	}
	if configure != nil {
		configure(&options)
	}
	server, err := pgwire.NewServer(
		pgwire.FromSQLDatabase(database),
		options,
	)
	if err != nil {
		t.Fatalf("create pgwire server: %v", err)
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatalf("listen on %s: %v", address, err)
	}
	h := &catalogServer{
		t: t, server: server, listener: listener, served: make(chan error, 1),
	}
	go func() {
		h.served <- server.Serve(listener)
	}()
	t.Cleanup(h.stop)

	dsn := (&url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(testUser, testPassword),
		Host:     listener.Addr().String(),
		Path:     testDatabase,
		RawQuery: "sslmode=disable",
	}).String()
	return h, dsn
}

func (h *catalogServer) stop() {
	h.stopOnce.Do(func() {
		if err := h.server.Close(); err != nil {
			h.t.Errorf("close pgwire server: %v", err)
		}
		select {
		case err := <-h.served:
			if !errors.Is(err, pgwire.ErrServerClosed) {
				h.t.Errorf("Serve returned %v, want ErrServerClosed", err)
			}
		case <-time.After(5 * time.Second):
			h.t.Error("Serve did not return after Server.Close")
		}
	})
}
