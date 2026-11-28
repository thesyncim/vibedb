package driver

import (
	"context"
	stdsql "database/sql"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"sync"
	"testing"

	sqlast "github.com/thesyncim/vibedb/sql"
)

func openTestSequenceDB(t *testing.T, dir string) *stdsql.DB {
	t.Helper()
	dsn := filepath.Join(dir, "seq.vibedb")
	db, err := stdsql.Open("vibedb", dsn)
	if err != nil {
		t.Fatalf("open vibedb: %v", err)
	}
	return db
}

func queryInt64(t *testing.T, q interface {
	QueryRowContext(context.Context, string, ...any) *stdsql.Row
}, sqlText string) int64 {
	t.Helper()
	var v int64
	if err := q.QueryRowContext(context.Background(), sqlText).Scan(&v); err != nil {
		t.Fatalf("%s: %v", sqlText, err)
	}
	return v
}

func TestSequenceDDLBoundsCycleAndOverflow(t *testing.T) {
	dir := t.TempDir()
	db := openTestSequenceDB(t, dir)
	defer db.Close()
	db.SetMaxOpenConns(1)

	ctx := context.Background()

	// CREATE SEQUENCE + IF NOT EXISTS + duplicate error.
	if _, err := db.ExecContext(ctx, `CREATE SEQUENCE s_basic`); err != nil {
		t.Fatalf("CREATE SEQUENCE: %v", err)
	}
	if _, err := db.ExecContext(ctx, `CREATE SEQUENCE IF NOT EXISTS s_basic`); err != nil {
		t.Fatalf("CREATE SEQUENCE IF NOT EXISTS: %v", err)
	}
	if _, err := db.ExecContext(ctx, `CREATE SEQUENCE s_basic`); !errors.Is(err, ErrSequenceExists) {
		t.Fatalf("duplicate CREATE SEQUENCE err = %v, want ErrSequenceExists", err)
	}

	// Invalid definitions.
	for _, bad := range []string{
		`CREATE SEQUENCE s_bad1 INCREMENT 0`,
		`CREATE SEQUENCE s_bad2 CACHE 0`,
		`CREATE SEQUENCE s_bad3 MINVALUE 10 MAXVALUE 5`,
		`CREATE SEQUENCE s_bad4 MINVALUE 10 MAXVALUE 20 START 5`,
	} {
		if _, err := db.ExecContext(ctx, bad); !errors.Is(err, ErrInvalidSequenceDefinition) {
			t.Fatalf("%q err = %v, want ErrInvalidSequenceDefinition", bad, err)
		}
	}

	// Basic ascending nextval + currval + lastval.
	for want := int64(1); want <= 5; want++ {
		if got := queryInt64(t, db, `SELECT nextval('s_basic')`); got != want {
			t.Fatalf("nextval = %d, want %d", got, want)
		}
		if got := queryInt64(t, db, `SELECT currval('s_basic')`); got != want {
			t.Fatalf("currval = %d, want %d", got, want)
		}
		if got := queryInt64(t, db, `SELECT lastval()`); got != want {
			t.Fatalf("lastval = %d, want %d", got, want)
		}
	}

	// setval(seq, val) (is_called=true) and setval(seq, val, false).
	if got := queryInt64(t, db, `SELECT setval('s_basic', 100)`); got != 100 {
		t.Fatalf("setval(100) = %d", got)
	}
	if got := queryInt64(t, db, `SELECT nextval('s_basic')`); got != 101 {
		t.Fatalf("nextval after setval(100) = %d, want 101", got)
	}
	if got := queryInt64(t, db, `SELECT setval('s_basic', 200, false)`); got != 200 {
		t.Fatalf("setval(200, false) = %d", got)
	}
	if got := queryInt64(t, db, `SELECT nextval('s_basic')`); got != 200 {
		t.Fatalf("nextval after setval(200, false) = %d, want 200", got)
	}

	// ALTER SEQUENCE: increment, restart, cycle.
	if _, err := db.ExecContext(ctx, `ALTER SEQUENCE s_basic INCREMENT BY 10 RESTART WITH 50 CACHE 4`); err != nil {
		t.Fatalf("ALTER SEQUENCE: %v", err)
	}
	if got := queryInt64(t, db, `SELECT nextval('s_basic')`); got != 50 {
		t.Fatalf("nextval after RESTART WITH 50 = %d, want 50", got)
	}
	if got := queryInt64(t, db, `SELECT nextval('s_basic')`); got != 60 {
		t.Fatalf("nextval after INCREMENT 10 = %d, want 60", got)
	}

	// Negative increment + NO CYCLE exhaustion.
	if _, err := db.ExecContext(ctx, `CREATE SEQUENCE s_desc INCREMENT -2 MINVALUE -5 MAXVALUE 0 START 0 NO CYCLE`); err != nil {
		t.Fatalf("CREATE SEQUENCE s_desc: %v", err)
	}
	for _, want := range []int64{0, -2, -4} {
		if got := queryInt64(t, db, `SELECT nextval('s_desc')`); got != want {
			t.Fatalf("s_desc nextval = %d, want %d", got, want)
		}
	}
	var unused int64
	if err := db.QueryRowContext(ctx, `SELECT nextval('s_desc')`).Scan(&unused); !errors.Is(err, ErrSequenceExhausted) {
		t.Fatalf("s_desc exhausted err = %v, want ErrSequenceExhausted", err)
	}

	// CYCLE wrap-around (both ascending and descending).
	if _, err := db.ExecContext(ctx, `CREATE SEQUENCE s_cyc INCREMENT 2 MINVALUE 1 MAXVALUE 5 START 1 CYCLE`); err != nil {
		t.Fatalf("CREATE SEQUENCE s_cyc: %v", err)
	}
	for _, want := range []int64{1, 3, 5, 1, 3, 5, 1} {
		if got := queryInt64(t, db, `SELECT nextval('s_cyc')`); got != want {
			t.Fatalf("s_cyc nextval = %d, want %d", got, want)
		}
	}

	// Exact int64 overflow boundary: ascending at math.MaxInt64.
	if _, err := db.ExecContext(ctx, fmt.Sprintf(
		`CREATE SEQUENCE s_max64 INCREMENT 1 MINVALUE %d MAXVALUE %d START %d NO CYCLE`,
		int64(math.MaxInt64-2), int64(math.MaxInt64), int64(math.MaxInt64-1),
	)); err != nil {
		t.Fatalf("CREATE SEQUENCE s_max64: %v", err)
	}
	if got := queryInt64(t, db, `SELECT nextval('s_max64')`); got != math.MaxInt64-1 {
		t.Fatalf("s_max64[0] = %d", got)
	}
	if got := queryInt64(t, db, `SELECT nextval('s_max64')`); got != math.MaxInt64 {
		t.Fatalf("s_max64[1] = %d", got)
	}
	if err := db.QueryRowContext(ctx, `SELECT nextval('s_max64')`).Scan(&unused); !errors.Is(err, ErrSequenceExhausted) {
		t.Fatalf("s_max64 exhausted err = %v, want ErrSequenceExhausted", err)
	}

	// Exact int64 overflow boundary: descending at math.MinInt64.
	if _, err := db.ExecContext(ctx, fmt.Sprintf(
		`CREATE SEQUENCE s_min64 INCREMENT -1 MINVALUE %d MAXVALUE %d START %d NO CYCLE`,
		int64(math.MinInt64), int64(math.MinInt64+2), int64(math.MinInt64+1),
	)); err != nil {
		t.Fatalf("CREATE SEQUENCE s_min64: %v", err)
	}
	if got := queryInt64(t, db, `SELECT nextval('s_min64')`); got != math.MinInt64+1 {
		t.Fatalf("s_min64[0] = %d", got)
	}
	if got := queryInt64(t, db, `SELECT nextval('s_min64')`); got != math.MinInt64 {
		t.Fatalf("s_min64[1] = %d", got)
	}
	if err := db.QueryRowContext(ctx, `SELECT nextval('s_min64')`).Scan(&unused); !errors.Is(err, ErrSequenceExhausted) {
		t.Fatalf("s_min64 exhausted err = %v, want ErrSequenceExhausted", err)
	}

	// DROP SEQUENCE + IF EXISTS + missing error.
	if _, err := db.ExecContext(ctx, `DROP SEQUENCE s_basic`); err != nil {
		t.Fatalf("DROP SEQUENCE: %v", err)
	}
	if _, err := db.ExecContext(ctx, `DROP SEQUENCE IF EXISTS s_basic`); err != nil {
		t.Fatalf("DROP SEQUENCE IF EXISTS: %v", err)
	}
	if _, err := db.ExecContext(ctx, `DROP SEQUENCE s_basic`); !errors.Is(err, ErrSequenceNotFound) {
		t.Fatalf("DROP missing SEQUENCE err = %v, want ErrSequenceNotFound", err)
	}
	if _, err := db.ExecContext(ctx, `ALTER SEQUENCE IF EXISTS s_basic RESTART`); err != nil {
		t.Fatalf("ALTER SEQUENCE IF EXISTS missing: %v", err)
	}
}

func TestSequenceSessionIsolation(t *testing.T) {
	dir := t.TempDir()
	db := openTestSequenceDB(t, dir)
	defer db.Close()
	db.SetMaxOpenConns(4)

	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `CREATE SEQUENCE s_iso`); err != nil {
		t.Fatalf("CREATE SEQUENCE: %v", err)
	}

	conn1, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("conn1: %v", err)
	}
	defer conn1.Close()

	conn2, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("conn2: %v", err)
	}
	defer conn2.Close()

	var v int64
	if err := conn1.QueryRowContext(ctx, `SELECT currval('s_iso')`).Scan(&v); !errors.Is(err, ErrSequenceStateNotInitialized) {
		t.Fatalf("conn1 currval before nextval err = %v, want ErrSequenceStateNotInitialized", err)
	}
	if err := conn1.QueryRowContext(ctx, `SELECT lastval()`).Scan(&v); !errors.Is(err, ErrSequenceStateNotInitialized) {
		t.Fatalf("conn1 lastval before nextval err = %v, want ErrSequenceStateNotInitialized", err)
	}

	if got := queryInt64(t, conn1, `SELECT nextval('s_iso')`); got != 1 {
		t.Fatalf("conn1 nextval = %d, want 1", got)
	}
	// conn2 still has not called nextval.
	if err := conn2.QueryRowContext(ctx, `SELECT currval('s_iso')`).Scan(&v); !errors.Is(err, ErrSequenceStateNotInitialized) {
		t.Fatalf("conn2 currval before nextval err = %v, want ErrSequenceStateNotInitialized", err)
	}
	if got := queryInt64(t, conn2, `SELECT nextval('s_iso')`); got != 2 {
		t.Fatalf("conn2 nextval = %d, want 2", got)
	}
	// conn1 still sees 1, conn2 sees 2.
	if got := queryInt64(t, conn1, `SELECT currval('s_iso')`); got != 1 {
		t.Fatalf("conn1 currval = %d, want 1", got)
	}
	if got := queryInt64(t, conn2, `SELECT currval('s_iso')`); got != 2 {
		t.Fatalf("conn2 currval = %d, want 2", got)
	}
	if got := queryInt64(t, conn1, `SELECT lastval()`); got != 1 {
		t.Fatalf("conn1 lastval = %d, want 1", got)
	}
	if got := queryInt64(t, conn2, `SELECT lastval()`); got != 2 {
		t.Fatalf("conn2 lastval = %d, want 2", got)
	}
}

func TestSequenceConcurrencyAndReopenDurability(t *testing.T) {
	dir := t.TempDir()
	db := openTestSequenceDB(t, dir)
	db.SetMaxOpenConns(16)

	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `CREATE SEQUENCE s_conc CACHE 32`); err != nil {
		t.Fatalf("CREATE SEQUENCE: %v", err)
	}

	const workers = 16
	const perWorker = 250
	const total = workers * perWorker

	results := make(chan int64, total)
	errs := make(chan error, total)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				var v int64
				if err := db.QueryRowContext(ctx, `SELECT nextval('s_conc')`).Scan(&v); err != nil {
					errs <- err
					return
				}
				results <- v
			}
		}()
	}
	wg.Wait()
	close(results)
	close(errs)

	for err := range errs {
		t.Fatalf("concurrent nextval error: %v", err)
	}
	seen := make(map[int64]bool, total)
	var maxIssued int64
	for v := range results {
		if seen[v] {
			t.Fatalf("duplicate sequence value issued: %d", v)
		}
		seen[v] = true
		if v > maxIssued {
			maxIssued = v
		}
	}
	if len(seen) != total || maxIssued != total {
		t.Fatalf("got %d unique values (max=%d), want %d", len(seen), maxIssued, total)
	}

	// Advance a second sequence partially through its 32-value batch window, then close & reopen.
	if _, err := db.ExecContext(ctx, `CREATE SEQUENCE s_durable`); err != nil {
		t.Fatalf("CREATE SEQUENCE s_durable: %v", err)
	}
	for i := int64(1); i <= 5; i++ {
		if got := queryInt64(t, db, `SELECT nextval('s_durable')`); got != i {
			t.Fatalf("s_durable[%d] = %d", i, got)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	// Reopen: s_durable MUST resume strictly after persisted high-water mark (32 -> 33),
	// never reusing any value in 1..5!
	db2 := openTestSequenceDB(t, dir)
	defer db2.Close()
	nextAfterReopen := queryInt64(t, db2, `SELECT nextval('s_durable')`)
	if nextAfterReopen <= 5 {
		t.Fatalf("reused value after reopen: got %d, must be > 5", nextAfterReopen)
	}
	if nextAfterReopen != defaultSequenceLogBatch+1 {
		t.Fatalf("nextval after reopen = %d, want %d (strictly after batch HWM)", nextAfterReopen, defaultSequenceLogBatch+1)
	}
	nextConc := queryInt64(t, db2, `SELECT nextval('s_conc')`)
	if nextConc <= maxIssued {
		t.Fatalf("s_conc reused value after reopen: got %d, maxIssued=%d", nextConc, maxIssued)
	}
}

func TestSerialAndIdentityColumnsE2E(t *testing.T) {
	dir := t.TempDir()
	db := openTestSequenceDB(t, dir)
	defer db.Close()
	db.SetMaxOpenConns(1)
	ctx := context.Background()

	// 1. SMALLSERIAL / SERIAL / BIGSERIAL + multi-row INSERT + DEFAULT VALUES + RETURNING + ON CONFLICT.
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE widgets (
			id BIGSERIAL PRIMARY KEY,
			code SMALLSERIAL,
			batch SERIAL,
			name TEXT DEFAULT 'unnamed'
		)
	`); err != nil {
		t.Fatalf("CREATE TABLE widgets: %v", err)
	}

	// Single row + RETURNING.
	var id int64
	if err := db.QueryRowContext(ctx, `INSERT INTO widgets (name) VALUES ('alpha') RETURNING id`).Scan(&id); err != nil {
		t.Fatalf("INSERT RETURNING: %v", err)
	}
	if id != 1 {
		t.Fatalf("first RETURNING id = %d, want 1", id)
	}
	// Multi-row INSERT.
	if _, err := db.ExecContext(ctx, `INSERT INTO widgets (name) VALUES ('beta'), ('gamma')`); err != nil {
		t.Fatalf("multi-row INSERT: %v", err)
	}
	// DEFAULT VALUES.
	if err := db.QueryRowContext(ctx, `INSERT INTO widgets DEFAULT VALUES RETURNING id`).Scan(&id); err != nil {
		t.Fatalf("INSERT DEFAULT VALUES: %v", err)
	}
	if id != 4 {
		t.Fatalf("DEFAULT VALUES RETURNING id = %d, want 4", id)
	}
	// Explicit DEFAULT in VALUES.
	if err := db.QueryRowContext(ctx, `INSERT INTO widgets (id, name) VALUES (DEFAULT, 'delta') RETURNING id`).Scan(&id); err != nil {
		t.Fatalf("INSERT VALUES (DEFAULT, ...): %v", err)
	}
	if id != 5 {
		t.Fatalf("VALUES(DEFAULT) RETURNING id = %d, want 5", id)
	}
	// ON CONFLICT DO NOTHING on explicit duplicate id.
	if _, err := db.ExecContext(ctx, `INSERT INTO widgets (id, name) VALUES (1, 'dup') ON CONFLICT (id) DO NOTHING`); err != nil {
		t.Fatalf("ON CONFLICT DO NOTHING: %v", err)
	}
	// Owned sequence cannot be dropped directly while table exists.
	if _, err := db.ExecContext(ctx, `DROP SEQUENCE widgets_id_seq`); !errors.Is(err, ErrDependentObjects) {
		t.Fatalf("DROP owned sequence err = %v, want ErrDependentObjects", err)
	}
	// DROP TABLE cascades to owned sequences.
	if _, err := db.ExecContext(ctx, `DROP TABLE widgets`); err != nil {
		t.Fatalf("DROP TABLE widgets: %v", err)
	}
	var unused int64
	if err := db.QueryRowContext(ctx, `SELECT nextval('widgets_id_seq')`).Scan(&unused); !errors.Is(err, ErrSequenceNotFound) {
		t.Fatalf("nextval after DROP TABLE err = %v, want ErrSequenceNotFound", err)
	}

	// 2. GENERATED ALWAYS AS IDENTITY + OVERRIDING SYSTEM VALUE / USER VALUE.
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE orders (
			id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 100 INCREMENT BY 10) PRIMARY KEY,
			note TEXT
		)
	`); err != nil {
		t.Fatalf("CREATE TABLE orders: %v", err)
	}
	// Rejects user-supplied value without OVERRIDING SYSTEM VALUE.
	if _, err := db.ExecContext(ctx, `INSERT INTO orders (id, note) VALUES (999, 'bad')`); !errors.Is(err, ErrIdentityAlwaysViolation) {
		t.Fatalf("GENERATED ALWAYS user value err = %v, want ErrIdentityAlwaysViolation", err)
	}
	// Accepts omitted column.
	if err := db.QueryRowContext(ctx, `INSERT INTO orders (note) VALUES ('first') RETURNING id`).Scan(&id); err != nil {
		t.Fatalf("INSERT omitted identity: %v", err)
	}
	if id != 100 {
		t.Fatalf("first identity id = %d, want 100", id)
	}
	// Accepts OVERRIDING SYSTEM VALUE.
	if err := db.QueryRowContext(ctx, `INSERT INTO orders (id, note) OVERRIDING SYSTEM VALUE VALUES (777, 'override') RETURNING id`).Scan(&id); err != nil {
		t.Fatalf("OVERRIDING SYSTEM VALUE: %v", err)
	}
	if id != 777 {
		t.Fatalf("OVERRIDING SYSTEM VALUE id = %d, want 777", id)
	}
	// Accepts OVERRIDING USER VALUE (ignores 888, generates 110).
	if err := db.QueryRowContext(ctx, `INSERT INTO orders (id, note) OVERRIDING USER VALUE VALUES (888, 'ignored') RETURNING id`).Scan(&id); err != nil {
		t.Fatalf("OVERRIDING USER VALUE: %v", err)
	}
	if id != 110 {
		t.Fatalf("OVERRIDING USER VALUE id = %d, want 110", id)
	}
	// Accepts VALUES (DEFAULT, ...).
	if err := db.QueryRowContext(ctx, `INSERT INTO orders (id, note) VALUES (DEFAULT, 'def') RETURNING id`).Scan(&id); err != nil {
		t.Fatalf("GENERATED ALWAYS VALUES(DEFAULT): %v", err)
	}
	if id != 120 {
		t.Fatalf("VALUES(DEFAULT) id = %d, want 120", id)
	}

	// 3. GENERATED BY DEFAULT AS IDENTITY.
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE notes (
			id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
			body TEXT
		)
	`); err != nil {
		t.Fatalf("CREATE TABLE notes: %v", err)
	}
	if err := db.QueryRowContext(ctx, `INSERT INTO notes (body) VALUES ('auto') RETURNING id`).Scan(&id); err != nil {
		t.Fatalf("BY DEFAULT auto: %v", err)
	}
	if id != 1 {
		t.Fatalf("BY DEFAULT auto id = %d, want 1", id)
	}
	if err := db.QueryRowContext(ctx, `INSERT INTO notes (id, body) VALUES (50, 'manual') RETURNING id`).Scan(&id); err != nil {
		t.Fatalf("BY DEFAULT manual: %v", err)
	}
	if id != 50 {
		t.Fatalf("BY DEFAULT manual id = %d, want 50", id)
	}
	if err := db.QueryRowContext(ctx, `INSERT INTO notes (id, body) OVERRIDING USER VALUE VALUES (99, 'override_user') RETURNING id`).Scan(&id); err != nil {
		t.Fatalf("BY DEFAULT OVERRIDING USER VALUE: %v", err)
	}
	if id != 2 {
		t.Fatalf("BY DEFAULT OVERRIDING USER VALUE id = %d, want 2", id)
	}

	// 4. Explicit DEFAULT nextval('custom_seq') + ALTER TABLE ADD COLUMN SERIAL.
	if _, err := db.ExecContext(ctx, `CREATE SEQUENCE custom_seq START WITH 500`); err != nil {
		t.Fatalf("CREATE SEQUENCE custom_seq: %v", err)
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE custom_tbl (id BIGINT PRIMARY KEY DEFAULT nextval('custom_seq'), label TEXT)`); err != nil {
		t.Fatalf("CREATE TABLE custom_tbl: %v", err)
	}
	if err := db.QueryRowContext(ctx, `INSERT INTO custom_tbl (label) VALUES ('x') RETURNING id`).Scan(&id); err != nil {
		t.Fatalf("custom default nextval: %v", err)
	}
	if id != 500 {
		t.Fatalf("custom default id = %d, want 500", id)
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE alter_seq_tbl (id BIGINT PRIMARY KEY, label TEXT)`); err != nil {
		t.Fatalf("CREATE TABLE alter_seq_tbl: %v", err)
	}
	if _, err := db.ExecContext(ctx, `ALTER TABLE alter_seq_tbl ADD COLUMN seq_no SERIAL`); err != nil {
		t.Fatalf("ALTER TABLE ADD COLUMN SERIAL: %v", err)
	}
	if _, err := db.ExecContext(ctx, `ALTER TABLE alter_seq_tbl ADD COLUMN gen_no INTEGER GENERATED BY DEFAULT AS IDENTITY (START WITH 10)`); err != nil {
		t.Fatalf("ALTER TABLE ADD COLUMN IDENTITY: %v", err)
	}
	var seqNo, genNo int64
	if err := db.QueryRowContext(ctx, `INSERT INTO alter_seq_tbl (id, label) VALUES (1, 'y') RETURNING seq_no, gen_no`).Scan(&seqNo, &genNo); err != nil {
		t.Fatalf("INSERT after ALTER TABLE ADD COLUMN SERIAL/IDENTITY: %v", err)
	}
	if seqNo != 1 || genNo != 10 {
		t.Fatalf("after ALTER ADD SERIAL/IDENTITY: seq_no=%d gen_no=%d, want 1, 10", seqNo, genNo)
	}
}

func TestSequenceReviewRegressions(t *testing.T) {
	dir := t.TempDir()
	db := openTestSequenceDB(t, dir)
	defer db.Close()
	db.SetMaxOpenConns(1)
	ctx := context.Background()

	// 1. O(1) batch allocation for huge CACHE (must not hang).
	if _, err := db.ExecContext(ctx, `CREATE SEQUENCE s_huge_cache CACHE 1000000000000`); err != nil {
		t.Fatalf("CREATE SEQUENCE s_huge_cache: %v", err)
	}
	if got := queryInt64(t, db, `SELECT nextval('s_huge_cache')`); got != 1 {
		t.Fatalf("s_huge_cache nextval[0] = %d, want 1", got)
	}
	if got := queryInt64(t, db, `SELECT nextval('s_huge_cache')`); got != 2 {
		t.Fatalf("s_huge_cache nextval[1] = %d, want 2", got)
	}

	// 2. setval(seq, val, true) updates session currval/lastval; setval(seq, val, false) does not.
	if _, err := db.ExecContext(ctx, `CREATE SEQUENCE s_setval`); err != nil {
		t.Fatalf("CREATE SEQUENCE s_setval: %v", err)
	}
	if got := queryInt64(t, db, `SELECT setval('s_setval', 42)`); got != 42 {
		t.Fatalf("setval(42) = %d, want 42", got)
	}
	if got := queryInt64(t, db, `SELECT currval('s_setval')`); got != 42 {
		t.Fatalf("currval after setval(42) = %d, want 42", got)
	}
	if got := queryInt64(t, db, `SELECT lastval()`); got != 42 {
		t.Fatalf("lastval after setval(42) = %d, want 42", got)
	}
	if got := queryInt64(t, db, `SELECT setval('s_setval', 100, false)`); got != 100 {
		t.Fatalf("setval(100, false) = %d, want 100", got)
	}
	// currval must remain 42 until nextval is called.
	if got := queryInt64(t, db, `SELECT currval('s_setval')`); got != 42 {
		t.Fatalf("currval after setval(100, false) = %d, want 42", got)
	}
	if got := queryInt64(t, db, `SELECT nextval('s_setval')`); got != 100 {
		t.Fatalf("nextval after setval(100, false) = %d, want 100", got)
	}

	// 3. ALTER SEQUENCE NO MAXVALUE preserves SMALLINT data type upper bound (32767).
	if _, err := db.ExecContext(ctx, `CREATE SEQUENCE s_small AS SMALLINT MAXVALUE 100`); err != nil {
		t.Fatalf("CREATE SEQUENCE s_small: %v", err)
	}
	if _, err := db.ExecContext(ctx, `ALTER SEQUENCE s_small NO MAXVALUE`); err != nil {
		t.Fatalf("ALTER SEQUENCE s_small NO MAXVALUE: %v", err)
	}
	if got := queryInt64(t, db, `SELECT setval('s_small', 32767, true)`); got != 32767 {
		t.Fatalf("setval(s_small, 32767) = %d, want 32767", got)
	}
	var unused int64
	if err := db.QueryRowContext(ctx, `SELECT nextval('s_small')`).Scan(&unused); !errors.Is(err, ErrSequenceExhausted) {
		t.Fatalf("s_small past 32767 err = %v, want ErrSequenceExhausted", err)
	}

	// 4. CREATE INDEX, TRUNCATE, ALTER RENAME COLUMN, ALTER DROP COLUMN preserve metadata & clean up owned sequences.
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE idx_tbl (
			id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
			tag SERIAL,
			name TEXT
		)
	`); err != nil {
		t.Fatalf("CREATE TABLE idx_tbl: %v", err)
	}
	if _, err := db.ExecContext(ctx, `CREATE INDEX idx_tbl_name ON idx_tbl (name)`); err != nil {
		t.Fatalf("CREATE INDEX: %v", err)
	}
	// GENERATED ALWAYS must still reject explicit values after CREATE INDEX!
	if _, err := db.ExecContext(ctx, `INSERT INTO idx_tbl (id, name) VALUES (99, 'bad')`); !errors.Is(err, ErrIdentityAlwaysViolation) {
		t.Fatalf("GENERATED ALWAYS after CREATE INDEX err = %v, want ErrIdentityAlwaysViolation", err)
	}
	var id, tag int64
	if err := db.QueryRowContext(ctx, `INSERT INTO idx_tbl (name) VALUES ('a') RETURNING id, tag`).Scan(&id, &tag); err != nil {
		t.Fatalf("INSERT after CREATE INDEX: %v", err)
	}
	if id != 1 || tag != 1 {
		t.Fatalf("after CREATE INDEX: id=%d tag=%d, want 1, 1", id, tag)
	}
	if _, err := db.ExecContext(ctx, `TRUNCATE TABLE idx_tbl`); err != nil {
		t.Fatalf("TRUNCATE TABLE: %v", err)
	}
	if err := db.QueryRowContext(ctx, `INSERT INTO idx_tbl (name) VALUES ('b') RETURNING id, tag`).Scan(&id, &tag); err != nil {
		t.Fatalf("INSERT after TRUNCATE: %v", err)
	}
	if id != 2 || tag != 2 {
		t.Fatalf("after TRUNCATE: id=%d tag=%d, want 2, 2", id, tag)
	}
	// RENAME COLUMN (on empty table) preserves SERIAL metadata and updates OwnerColumn.
	if _, err := db.ExecContext(ctx, `TRUNCATE TABLE idx_tbl`); err != nil {
		t.Fatalf("TRUNCATE before RENAME COLUMN: %v", err)
	}
	if _, err := db.ExecContext(ctx, `ALTER TABLE idx_tbl RENAME COLUMN tag TO tag_v2`); err != nil {
		t.Fatalf("ALTER RENAME COLUMN: %v", err)
	}
	if err := db.QueryRowContext(ctx, `INSERT INTO idx_tbl (name) VALUES ('c') RETURNING id, tag_v2`).Scan(&id, &tag); err != nil {
		t.Fatalf("INSERT after RENAME COLUMN: %v", err)
	}
	if id != 3 || tag != 3 {
		t.Fatalf("after RENAME COLUMN: id=%d tag_v2=%d, want 3, 3", id, tag)
	}
	// DROP COLUMN drops owned sequence cleanly.
	if _, err := db.ExecContext(ctx, `ALTER TABLE idx_tbl DROP COLUMN tag_v2`); err != nil {
		t.Fatalf("ALTER DROP COLUMN: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT nextval('idx_tbl_tag_seq')`).Scan(&unused); !errors.Is(err, ErrSequenceNotFound) {
		t.Fatalf("nextval after DROP COLUMN err = %v, want ErrSequenceNotFound", err)
	}

	// 5. INSERT INTO ... SELECT targeting SERIAL and GENERATED ALWAYS AS IDENTITY (outside & inside tx).
	if _, err := db.ExecContext(ctx, `CREATE TABLE src_names (name TEXT PRIMARY KEY)`); err != nil {
		t.Fatalf("CREATE TABLE src_names: %v", err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO src_names (name) VALUES ('x'), ('y')`); err != nil {
		t.Fatalf("INSERT src_names: %v", err)
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE dst_ser (id SERIAL PRIMARY KEY, name TEXT)`); err != nil {
		t.Fatalf("CREATE TABLE dst_ser: %v", err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO dst_ser (name) SELECT name FROM src_names ORDER BY name`); err != nil {
		t.Fatalf("INSERT SELECT dst_ser: %v", err)
	}
	var cnt int64
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM dst_ser`).Scan(&cnt); err != nil || cnt != 2 {
		t.Fatalf("dst_ser count = %d, err = %v, want 2", cnt, err)
	}
	// Inside a transaction.
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO dst_ser (name) SELECT name FROM src_names ORDER BY name`); err != nil {
		_ = tx.Rollback()
		t.Fatalf("tx INSERT SELECT dst_ser: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("tx Commit: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM dst_ser`).Scan(&cnt); err != nil || cnt != 4 {
		t.Fatalf("dst_ser count after tx = %d, err = %v, want 4", cnt, err)
	}

	// GENERATED ALWAYS rejects explicit id in INSERT ... SELECT unless OVERRIDING SYSTEM VALUE.
	if _, err := db.ExecContext(ctx, `CREATE TABLE dst_always (id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, name TEXT)`); err != nil {
		t.Fatalf("CREATE TABLE dst_always: %v", err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO dst_always (id, name) SELECT 10, name FROM src_names WHERE name = 'x'`); !errors.Is(err, ErrIdentityAlwaysViolation) {
		t.Fatalf("INSERT SELECT explicit id into GENERATED ALWAYS err = %v, want ErrIdentityAlwaysViolation", err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO dst_always (id, name) OVERRIDING SYSTEM VALUE SELECT 10, name FROM src_names WHERE name = 'x'`); err != nil {
		t.Fatalf("INSERT SELECT OVERRIDING SYSTEM VALUE: %v", err)
	}

	// 6. Table-backed SELECT nextval('s') evaluates per row + ROLLBACK does not roll back sequence.
	if _, err := db.ExecContext(ctx, `CREATE SEQUENCE s_rows`); err != nil {
		t.Fatalf("CREATE SEQUENCE s_rows: %v", err)
	}
	rows, err := db.QueryContext(ctx, `SELECT nextval('s_rows'), name FROM src_names ORDER BY name`)
	if err != nil {
		t.Fatalf("SELECT nextval FROM src_names: %v", err)
	}
	var seqVals []int64
	for rows.Next() {
		var sv int64
		var nm string
		if err := rows.Scan(&sv, &nm); err != nil {
			rows.Close()
			t.Fatalf("scan: %v", err)
		}
		seqVals = append(seqVals, sv)
	}
	rows.Close()
	if len(seqVals) != 2 || seqVals[0] != 1 || seqVals[1] != 2 {
		t.Fatalf("table-backed SELECT nextval = %v, want [1 2]", seqVals)
	}

	// ROLLBACK does not rewind sequence.
	tx2, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx2: %v", err)
	}
	if got := queryInt64(t, tx2, `SELECT nextval('s_rows')`); got != 3 {
		_ = tx2.Rollback()
		t.Fatalf("nextval in tx2 = %d, want 3", got)
	}
	if err := tx2.Rollback(); err != nil {
		t.Fatalf("Rollback tx2: %v", err)
	}
	if got := queryInt64(t, db, `SELECT nextval('s_rows')`); got != 4 {
		t.Fatalf("nextval after ROLLBACK = %d, want 4", got)
	}
}

func BenchmarkSequenceNextval(b *testing.B) {
	for _, tc := range []struct {
		name  string
		cache int64
	}{
		{"Cache1024", 1024},
		{"DefaultBatch32", 1},
	} {
		b.Run(tc.name, func(b *testing.B) {
			dir := b.TempDir()
			dsn := filepath.Join(dir, "bench_seq.vibedb")
			db, err := openDatabase(dsn)
			if err != nil {
				b.Fatalf("openDatabase: %v", err)
			}
			defer db.closeTerminal()

			ctx := context.Background()
			db.mu.Lock()
			_, err = db.createSequenceLocked(ctx, &sqlast.CreateSequenceStmt{
				Name: "bench_seq",
				Options: sqlast.SequenceOptions{
					HasCache: true,
					Cache:    tc.cache,
				},
			})
			db.mu.Unlock()
			if err != nil {
				b.Fatalf("createSequenceLocked: %v", err)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := db.nextSequenceValue("bench_seq"); err != nil {
					b.Fatalf("nextSequenceValue: %v", err)
				}
			}
		})
	}
}
