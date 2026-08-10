package driver

import (
	"context"
	stdsql "database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/query"
	sqlast "github.com/thesyncim/vibedb/sql"
)

const correlatedExistsDriverSQL = `
	SELECT o.id
	FROM ce_outer AS o
	WHERE EXISTS (
		SELECT 1
		FROM ce_inner AS i
		WHERE i.match_key = o.match_key
		  AND i.active = ?
	)
	ORDER BY o.id`

func seedCorrelatedExistsDriver(t testing.TB, db *stdsql.DB) {
	t.Helper()
	for _, statement := range []string{
		`CREATE TABLE ce_outer (` +
			`id STRING PRIMARY KEY, match_key STRING, region STRING, enabled BOOL)`,
		`CREATE INDEX ce_outer_match_exact ON ce_outer (match_key)`,
		`CREATE TABLE ce_inner (` +
			`id STRING PRIMARY KEY, match_key STRING, active BOOL, region STRING, owner STRING)`,
		`CREATE TABLE ce_nested (id STRING PRIMARY KEY, owner STRING)`,
		`INSERT INTO ce_outer VALUES ` +
			`('{"id":"a_dup","match_key":"x","region":"north","enabled":true}'),` +
			`('{"id":"b_filtered","match_key":"y","region":"south","enabled":true}'),` +
			`('{"id":"c_local_reject","match_key":"z","region":"west","enabled":false}'),` +
			`('{"id":"d_null","match_key":null,"region":"north","enabled":true}'),` +
			`('{"id":"e_missing","region":"north","enabled":true}'),` +
			`('{"id":"f_empty","match_key":"none","region":"north","enabled":true}')`,
		`INSERT INTO ce_inner VALUES ` +
			`('{"id":"i1","match_key":"x","active":true,"region":"north","owner":"a_dup"}'),` +
			`('{"id":"i2","match_key":"x","active":true,"region":"north","owner":"a_dup"}'),` +
			`('{"id":"i3","match_key":"y","active":true,"region":"south","owner":"b_filtered"}'),` +
			`('{"id":"i4","match_key":"z","active":false,"region":"west","owner":"c_local_reject"}'),` +
			`('{"id":"i5","match_key":null,"active":true,"region":"north","owner":"d_null"}')`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
}

func correlatedExistsDriverIDs(
	t testing.TB,
	q interface {
		Query(query string, args ...any) (*stdsql.Rows, error)
	},
	statement string,
	args ...any,
) []string {
	t.Helper()
	rows, err := q.Query(statement, args...)
	if err != nil {
		t.Fatalf("%s: %v", statement, err)
	}
	closed := false
	defer func() {
		if !closed {
			if closeErr := rows.Close(); closeErr != nil {
				t.Errorf("close rows after test failure: %v", closeErr)
			}
		}
	}()
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
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	closed = true
	return ids
}

func correlatedExistsPreparedIDs(
	t testing.TB,
	statement *stdsql.Stmt,
	args ...any,
) []string {
	t.Helper()
	rows, err := statement.Query(args...)
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	defer func() {
		if !closed {
			if closeErr := rows.Close(); closeErr != nil {
				t.Errorf("close prepared rows after test failure: %v", closeErr)
			}
		}
	}()
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
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	closed = true
	return ids
}

func TestCorrelatedExistsAutocommitPreparedAndThreeValuedSemantics(t *testing.T) {
	db := openTestDB(t)
	seedCorrelatedExistsDriver(t, db)

	tests := []struct {
		name      string
		statement string
		want      []string
	}{
		{
			name: "exists local filter and duplicate inner keys",
			statement: `SELECT o.id FROM ce_outer AS o WHERE EXISTS (` +
				`SELECT 1 FROM ce_inner AS i ` +
				`WHERE i.match_key = o.match_key AND i.active = TRUE) ORDER BY o.id`,
			want: []string{"a_dup", "b_filtered"},
		},
		{
			name: "not exists keeps null missing and unmatched keys",
			statement: `SELECT o.id FROM ce_outer AS o WHERE NOT EXISTS (` +
				`SELECT 1 FROM ce_inner AS i ` +
				`WHERE i.match_key = o.match_key AND i.active = TRUE) ORDER BY o.id`,
			want: []string{"c_local_reject", "d_null", "e_missing", "f_empty"},
		},
		{
			name: "empty filtered child",
			statement: `SELECT o.id FROM ce_outer AS o WHERE EXISTS (` +
				`SELECT 1 FROM ce_inner AS i ` +
				`WHERE i.match_key = o.match_key AND i.region = 'absent') ORDER BY o.id`,
		},
		{
			name: "anti join over empty filtered child",
			statement: `SELECT o.id FROM ce_outer AS o WHERE NOT EXISTS (` +
				`SELECT 1 FROM ce_inner AS i ` +
				`WHERE i.match_key = o.match_key AND i.region = 'absent') ORDER BY o.id`,
			want: []string{
				"a_dup", "b_filtered", "c_local_reject", "d_null", "e_missing", "f_empty",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := correlatedExistsDriverIDs(t, db, test.statement); !slices.Equal(got, test.want) {
				t.Fatalf("ids = %v, want %v", got, test.want)
			}
		})
	}

	prepared, err := db.Prepare(correlatedExistsDriverSQL)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	for _, run := range []struct {
		active bool
		want   []string
	}{
		{true, []string{"a_dup", "b_filtered"}},
		{false, []string{"c_local_reject"}},
		{true, []string{"a_dup", "b_filtered"}},
	} {
		if got := correlatedExistsPreparedIDs(t, prepared, run.active); !slices.Equal(got, run.want) {
			t.Fatalf("prepared active=%t ids = %v, want %v", run.active, got, run.want)
		}
	}

	antiPrepared, err := db.Prepare(strings.Replace(
		correlatedExistsDriverSQL, "WHERE EXISTS (", "WHERE NOT EXISTS (", 1))
	if err != nil {
		t.Fatal(err)
	}
	defer antiPrepared.Close()
	for _, run := range []struct {
		active bool
		want   []string
	}{
		{true, []string{"c_local_reject", "d_null", "e_missing", "f_empty"}},
		{false, []string{"a_dup", "b_filtered", "d_null", "e_missing", "f_empty"}},
	} {
		if got := correlatedExistsPreparedIDs(t, antiPrepared, run.active); !slices.Equal(got, run.want) {
			t.Fatalf("prepared NOT EXISTS active=%t ids = %v, want %v", run.active, got, run.want)
		}
	}
}

func TestCorrelatedExistsTransactionUsesBeginSnapshotAndPendingInnerWrites(t *testing.T) {
	db := openTestDB(t)
	db.SetMaxOpenConns(4)
	seedCorrelatedExistsDriver(t, db)
	prepared, err := db.Prepare(correlatedExistsDriverSQL)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()

	tx, err := db.BeginTx(context.Background(), &stdsql.TxOptions{
		Isolation: stdsql.LevelRepeatableRead,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := db.Exec(`INSERT INTO ce_outer VALUES (` +
		`'{"id":"outside_outer","match_key":"x","region":"north","enabled":true}')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO ce_inner VALUES (` +
		`'{"id":"outside_inner","match_key":"none","active":true,"region":"north"}')`); err != nil {
		t.Fatal(err)
	}

	txPrepared := tx.Stmt(prepared)
	defer txPrepared.Close()
	if got, want := correlatedExistsPreparedIDs(t, txPrepared, true),
		[]string{"a_dup", "b_filtered"}; !slices.Equal(got, want) {
		t.Fatalf("BEGIN snapshot ids = %v, want %v", got, want)
	}
	if _, err := tx.Exec(`INSERT INTO ce_inner VALUES (` +
		`'{"id":"pending_inner","match_key":"none","active":true,"region":"north"}')`); err != nil {
		t.Fatal(err)
	}
	if got, want := correlatedExistsPreparedIDs(t, txPrepared, true),
		[]string{"a_dup", "b_filtered", "f_empty"}; !slices.Equal(got, want) {
		t.Fatalf("read-your-writes ids = %v, want %v", got, want)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	if got, want := correlatedExistsPreparedIDs(t, prepared, true),
		[]string{"a_dup", "b_filtered", "f_empty", "outside_outer"}; !slices.Equal(got, want) {
		t.Fatalf("post-rollback autocommit ids = %v, want %v", got, want)
	}
}

func TestCorrelatedExistsPreparedDependencyDropFailsBeforeScanning(t *testing.T) {
	db := openTestDB(t)
	seedCorrelatedExistsDriver(t, db)
	const source = `SELECT o.id FROM ce_outer AS o WHERE EXISTS (` +
		`SELECT 1 FROM ce_inner AS i WHERE i.match_key = o.match_key)`
	prepared, err := db.Prepare(source)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	if rows, err := prepared.Query(); err != nil {
		t.Fatal(err)
	} else if closeErr := rows.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	if _, err := db.Exec(`DROP TABLE ce_inner`); err != nil {
		t.Fatal(err)
	}
	rows, err := prepared.Query()
	if rows != nil {
		_ = rows.Close()
		t.Fatal("dropped correlated dependency published a cursor")
	}
	if !errors.Is(err, ErrTableNotFound) {
		t.Fatalf("dropped dependency = %T %v, want ErrTableNotFound", err, err)
	}
	var positioned interface{ Position() int }
	if !errors.As(err, &positioned) || positioned.Position() != strings.Index(source, "ce_inner") {
		t.Fatalf("dropped dependency position = %T %v, want byte %d",
			err, err, strings.Index(source, "ce_inner"))
	}
	if got := correlatedExistsDriverIDs(t, db,
		`SELECT id FROM ce_outer ORDER BY id`); len(got) != 6 {
		t.Fatalf("connection was not reusable after dropped dependency: %v", got)
	}
}

func TestCorrelatedExistsContainerKeysUseCanonicalStoredJSONIdentity(t *testing.T) {
	db := openTestDB(t)
	for _, statement := range []string{
		`CREATE TABLE ce_value_outer (id STRING PRIMARY KEY, match_key ANY)`,
		`CREATE INDEX ce_value_outer_match ON ce_value_outer (match_key)`,
		`CREATE TABLE ce_value_inner (id STRING PRIMARY KEY, match_key ANY)`,
		`INSERT INTO ce_value_outer VALUES ` +
			`('{"id":"a_array","match_key":[1,2]}'),` +
			`('{"id":"b_object","match_key":{"a":1,"b":2}}'),` +
			`('{"id":"c_object_order","match_key":{"b":2,"a":1}}'),` +
			`('{"id":"d_scalar","match_key":"scalar"}'),` +
			`('{"id":"e_array_order","match_key":[2,1]}'),` +
			`('{"id":"f_object_value","match_key":{"a":1,"b":3}}'),` +
			`('{"id":"g_object_member","match_key":{"a":1,"c":2}}'),` +
			`('{"id":"h_missing"}')`,
		`INSERT INTO ce_value_inner VALUES ` +
			`('{"id":"i_scalar","match_key":"scalar"}'),` +
			`('{"id":"i_array","match_key":[1,2]}'),` +
			`('{"id":"i_object","match_key":{"a":1,"b":2}}'),` +
			`('{"id":"i_object_duplicate","match_key":{"a":1,"b":2}}')`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	exists := `SELECT o.id FROM ce_value_outer AS o WHERE EXISTS (` +
		`SELECT 1 FROM ce_value_inner AS i WHERE i.match_key = o.match_key) ORDER BY o.id`
	if got, want := correlatedExistsDriverIDs(t, db, exists),
		[]string{"a_array", "b_object", "c_object_order", "d_scalar"}; !slices.Equal(got, want) {
		t.Fatalf("container EXISTS rows = %v, want %v", got, want)
	}
	anti := `SELECT o.id FROM ce_value_outer AS o WHERE NOT EXISTS (` +
		`SELECT 1 FROM ce_value_inner AS i WHERE i.match_key = o.match_key) ORDER BY o.id`
	if got, want := correlatedExistsDriverIDs(t, db, anti),
		[]string{"e_array_order", "f_object_value", "g_object_member", "h_missing"}; !slices.Equal(got, want) {
		t.Fatalf("container NOT EXISTS rows = %v, want %v", got, want)
	}
	if got := correlatedExistsDriverIDs(t, db,
		`SELECT id FROM ce_value_outer WHERE id = 'a_array'`); !slices.Equal(got, []string{"a_array"}) {
		t.Fatalf("connection reuse rows = %v, want [a_array]", got)
	}
}

func TestCorrelatedExistsPreCanceledExecutionPublishesNoCursorAndReuses(t *testing.T) {
	database, session := openRuntimeSession(t)
	defer database.Close()
	defer session.Close()
	for _, statement := range []string{
		`CREATE TABLE ce_cancel_outer (id STRING PRIMARY KEY, match_key STRING)`,
		`CREATE TABLE ce_cancel_inner (id STRING PRIMARY KEY, match_key STRING)`,
		`INSERT INTO ce_cancel_outer VALUES ` +
			`('{"id":"a","match_key":"x"}'),('{"id":"b","match_key":"none"}')`,
		`INSERT INTO ce_cancel_inner VALUES ('{"id":"i","match_key":"x"}')`,
	} {
		correlatedExistsRuntimeExec(t, session, statement, nil)
	}
	prepared := correlatedExistsRuntimePrepare(t, session, `
		SELECT o.id FROM ce_cancel_outer AS o WHERE EXISTS (
			SELECT 1 FROM ce_cancel_inner AS i
			WHERE i.match_key = o.match_key
		) ORDER BY o.id`)
	var cancel query.CancelFlag
	if err := session.SetCancelFlag(&cancel); err != nil {
		t.Fatal(err)
	}
	cancel.Cancel()
	cursor, err := prepared.Query(context.Background(), nil)
	if cursor != nil {
		_ = cursor.Close()
		t.Fatal("pre-canceled correlated EXISTS published a cursor")
	}
	if !errors.Is(err, query.ErrCanceled) {
		t.Fatalf("pre-canceled execution = %T %v, want ErrCanceled", err, err)
	}
	if session.State() != SessionIdle {
		t.Fatalf("pre-canceled autocommit state = %s, want idle", session.State())
	}

	cancel.Reset()
	cursor, err = prepared.Query(context.Background(), nil)
	if err != nil {
		t.Fatalf("execution after CancelFlag.Reset: %v", err)
	}
	rows := 0
	for cursor.Next() {
		rows++
	}
	if rows != 1 {
		_ = cursor.Close()
		t.Fatalf("execution after reset returned %d rows, want 1", rows)
	}
	if err := cursor.Close(); err != nil {
		t.Fatal(err)
	}
	if err := session.SetCancelFlag(nil); err != nil {
		t.Fatal(err)
	}
}

func TestCorrelatedExistsOneByteShortWorkBudgetPublishesNoCursorAndReuses(t *testing.T) {
	database, session := openRuntimeSession(t)
	defer database.Close()
	defer session.Close()
	for _, statement := range []string{
		`CREATE TABLE ce_budget_outer (id STRING PRIMARY KEY, match_key STRING)`,
		`CREATE TABLE ce_budget_inner (id STRING PRIMARY KEY, match_key STRING)`,
	} {
		correlatedExistsRuntimeExec(t, session, statement, nil)
	}
	const padding = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx" +
		"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
	insertRows := func(table, prefix string, rows, keys int) {
		t.Helper()
		for base := 0; base < rows; base += 64 {
			end := min(base+64, rows)
			var statement strings.Builder
			statement.Grow((end - base) * 160)
			statement.WriteString("INSERT INTO ")
			statement.WriteString(table)
			statement.WriteString(" VALUES ")
			for i := base; i < end; i++ {
				if i != base {
					statement.WriteByte(',')
				}
				statement.WriteString("('")
				_, _ = fmt.Fprintf(&statement,
					`{"id":"%s-%04d","match_key":"key-%04d-%s"}`,
					prefix, i, i%keys, padding,
				)
				statement.WriteString("')")
			}
			correlatedExistsRuntimeExec(t, session, statement.String(), nil)
		}
	}
	insertRows("ce_budget_outer", "outer", 512, 512)
	insertRows("ce_budget_inner", "inner", 1024, 512)

	prepared := correlatedExistsRuntimePrepare(t, session, `
		SELECT o.id FROM ce_budget_outer AS o WHERE EXISTS (
			SELECT 1 FROM ce_budget_inner AS i
			WHERE i.match_key = o.match_key
		)`)
	const minimum = int64(64 << 10)
	attempt := func(limit int64) (bool, *query.WorkBudgetError) {
		t.Helper()
		if err := session.SetMemoryLimit(limit); err != nil {
			t.Fatalf("set work-memory limit %d: %v", limit, err)
		}
		cursor, err := prepared.Query(context.Background(), nil)
		if err == nil {
			if cursor == nil {
				t.Fatal("successful budget calibration returned a nil cursor")
			}
			rows := 0
			for cursor.Next() {
				rows++
			}
			if closeErr := cursor.Close(); closeErr != nil {
				t.Fatal(closeErr)
			}
			if rows != 512 {
				t.Fatalf("budget calibration rows = %d, want 512", rows)
			}
			return true, nil
		}
		if cursor != nil {
			_ = cursor.Close()
			t.Fatal("work-budget calibration failure published a cursor")
		}
		var budgetErr *query.WorkBudgetError
		if !errors.Is(err, query.ErrWorkBudget) || !errors.As(err, &budgetErr) {
			t.Fatalf("budget calibration = %T %v, want WorkBudgetError", err, err)
		}
		// Nested durable operators report the exact admission's remaining
		// sub-budget, not necessarily the session-wide ceiling. The refusal must
		// still prove that its requested bytes exceed its own local limit.
		if budgetErr.Bytes <= budgetErr.Limit {
			t.Fatalf("budget calibration error = %+v, want bytes above local limit",
				budgetErr)
		}
		if session.State() != SessionIdle {
			t.Fatalf("budget calibration state = %s, want idle", session.State())
		}
		return false, budgetErr
	}
	if ok, _ := attempt(minimum); ok {
		t.Fatalf("correlated fixture fit in minimum work-memory limit %d", minimum)
	}
	low, high := minimum, int64(64<<20)
	if ok, budgetErr := attempt(high); !ok {
		t.Fatalf("correlated fixture exceeded bounded calibration ceiling %d: %+v",
			high, budgetErr)
	}
	for high-low > 1 {
		mid := low + (high-low)/2
		ok, budgetErr := attempt(mid)
		if ok {
			high = mid
			continue
		}
		low = mid
		// The typed refusal proves every smaller limit below Bytes fails at
		// this same admission, so skip that already-proved interval.
		if proved := budgetErr.Bytes - 1; proved > low && proved < high {
			low = proved
		}
	}
	exact := high

	if err := session.SetMemoryLimit(exact - 1); err != nil {
		t.Fatal(err)
	}
	cursor, err := prepared.Query(context.Background(), nil)
	if cursor != nil {
		_ = cursor.Close()
		t.Fatal("one-byte-short correlated execution published a cursor")
	}
	var budgetErr *query.WorkBudgetError
	if !errors.Is(err, query.ErrWorkBudget) || !errors.As(err, &budgetErr) ||
		budgetErr.Bytes <= budgetErr.Limit {
		t.Fatalf("one-byte-short execution = %T %+v, want a typed local refusal",
			err, budgetErr)
	}
	if session.State() != SessionIdle {
		t.Fatalf("one-byte-short state = %s, want idle", session.State())
	}

	if err := session.SetMemoryLimit(exact); err != nil {
		t.Fatal(err)
	}
	cursor, err = prepared.Query(context.Background(), nil)
	if err != nil || cursor == nil {
		t.Fatalf("raised-limit reuse cursor/error = (%v, %v)", cursor, err)
	}
	rows := 0
	for cursor.Next() {
		rows++
	}
	if closeErr := cursor.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	if rows != 512 {
		t.Fatalf("raised-limit reuse rows = %d, want 512", rows)
	}
}

func TestCorrelatedExistsUnsupportedShapesAreTypedAndPositioned(t *testing.T) {
	db := openTestDB(t)
	seedCorrelatedExistsDriver(t, db)
	tests := []struct {
		name   string
		source string
		marker string
		last   bool
	}{
		{
			name: "correlation below OR",
			source: `SELECT o.id FROM ce_outer AS o WHERE o.enabled = TRUE OR EXISTS (` +
				`SELECT 1 FROM ce_inner AS i WHERE i.match_key = o.match_key)`,
			marker: "EXISTS",
		},
		{
			name: "nested predicate subquery",
			source: `SELECT o.id FROM ce_outer AS o WHERE EXISTS (` +
				`SELECT 1 FROM ce_inner AS i WHERE i.match_key = o.match_key AND EXISTS (` +
				`SELECT 1 FROM ce_nested AS n WHERE n.owner = i.id))`,
			marker: "EXISTS",
			last:   true,
		},
		{
			name: "correlated predicate in JOIN ON",
			source: `SELECT o.id FROM ce_outer AS o JOIN ce_inner AS i ON EXISTS (` +
				`SELECT 1 FROM ce_nested AS n WHERE n.owner = o.id)`,
			marker: "EXISTS",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			prepared, err := db.Prepare(test.source)
			if prepared != nil {
				_ = prepared.Close()
				t.Fatal("unsupported correlated shape prepared")
			}
			var unsupported *sqlast.FeatureNotSupportedError
			if !errors.As(err, &unsupported) {
				t.Fatalf("error = %T %v, want FeatureNotSupportedError", err, err)
			}
			want := strings.Index(test.source, test.marker)
			if test.last {
				want = strings.LastIndex(test.source, test.marker)
			}
			if unsupported.Pos != want {
				t.Fatalf("position = %d, want %d at %q: %v",
					unsupported.Pos, want, test.marker, unsupported)
			}
		})
	}
}

func correlatedExistsRuntimePrepare(
	t testing.TB,
	session *Session,
	statement string,
) *Prepared {
	t.Helper()
	prepared, err := session.Prepare(context.Background(), statement)
	if err != nil {
		t.Fatalf("prepare %s: %v", statement, err)
	}
	t.Cleanup(func() { _ = prepared.Close() })
	return prepared
}

func correlatedExistsRuntimeExec(
	t testing.TB,
	session *Session,
	statement string,
	args []any,
) {
	t.Helper()
	prepared, err := session.Prepare(context.Background(), statement)
	if err != nil {
		t.Fatalf("prepare %s: %v", statement, err)
	}
	if _, err := prepared.Exec(context.Background(), args); err != nil {
		_ = prepared.Close()
		t.Fatalf("execute %s: %v", statement, err)
	}
	if err := prepared.Close(); err != nil {
		t.Fatalf("close %s: %v", statement, err)
	}
}

type correlatedExistsExplainAnalysis struct {
	Rows            int    `json:"rows"`
	RowsTotal       uint64 `json:"rows_total"`
	RowsScanned     uint64 `json:"rows_scanned"`
	IndexBounded    bool   `json:"index_bounded"`
	IndexLookups    int    `json:"index_lookups"`
	JoinMemberships int    `json:"join_memberships"`
	JoinLookups     int    `json:"join_lookups"`
	JoinBuilds      int    `json:"join_builds"`
	JoinPairs       uint64 `json:"join_pairs"`
}

type correlatedExistsExplainDocument struct {
	Plan struct {
		Joins []struct {
			AccessPath string `json:"access_path"`
		} `json:"joins"`
		Analyze *correlatedExistsExplainAnalysis `json:"analyze"`
	} `json:"plan"`
}

func correlatedExistsRuntimeExplainAnalyze(
	t testing.TB,
	session *Session,
	statement string,
) correlatedExistsExplainDocument {
	t.Helper()
	prepared, err := session.Prepare(context.Background(), "EXPLAIN ANALYZE "+statement)
	if err != nil {
		t.Fatalf("prepare EXPLAIN ANALYZE: %v", err)
	}
	defer func() {
		if closeErr := prepared.Close(); closeErr != nil {
			t.Errorf("close EXPLAIN ANALYZE: %v", closeErr)
		}
	}()
	cursor, err := prepared.Query(context.Background(), nil)
	if err != nil {
		t.Fatalf("execute EXPLAIN ANALYZE: %v", err)
	}
	if cursor == nil {
		t.Fatal("EXPLAIN ANALYZE returned a nil cursor")
	}
	if !cursor.Next() {
		_ = cursor.Close()
		t.Fatal("EXPLAIN ANALYZE returned no plan row")
	}
	plan, ok := cursor.Cell(0).Text()
	if !ok {
		_ = cursor.Close()
		t.Fatalf("EXPLAIN ANALYZE cell = %s, want text", cursor.Cell(0).JSON())
	}
	if cursor.Next() {
		_ = cursor.Close()
		t.Fatal("EXPLAIN ANALYZE returned more than one plan row")
	}
	if closeErr := cursor.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	var document correlatedExistsExplainDocument
	if err := json.Unmarshal([]byte(plan), &document); err != nil {
		t.Fatalf("decode EXPLAIN ANALYZE %q: %v", plan, err)
	}
	if document.Plan.Analyze == nil {
		t.Fatalf("EXPLAIN ANALYZE omitted measured statistics: %s", plan)
	}
	return document
}

func TestCorrelatedExistsDirectDurableStrategiesAndZeroAllocation(t *testing.T) {
	database, session := openRuntimeSession(t)
	defer database.Close()
	defer session.Close()
	for _, statement := range []string{
		`CREATE TABLE ce_stats_outer (id STRING PRIMARY KEY, match_key STRING)`,
		`CREATE INDEX ce_stats_outer_match ON ce_stats_outer (match_key)`,
		`CREATE TABLE ce_stats_inner (id STRING PRIMARY KEY, match_key STRING, active BOOL)`,
		`INSERT INTO ce_stats_outer VALUES ` +
			`('{"id":"a","match_key":"x"}'),` +
			`('{"id":"b","match_key":"y"}'),` +
			`('{"id":"c","match_key":"none"}')`,
		`INSERT INTO ce_stats_inner VALUES ` +
			`('{"id":"i1","match_key":"x","active":true}'),` +
			`('{"id":"i2","match_key":"x","active":true}'),` +
			`('{"id":"i3","match_key":"y","active":true}')`,
	} {
		correlatedExistsRuntimeExec(t, session, statement, nil)
	}
	const scalarSQL = `
		SELECT o.id FROM ce_stats_outer AS o WHERE EXISTS (
			SELECT 1 FROM ce_stats_inner AS i
			WHERE i.match_key = o.match_key AND i.active = TRUE
		) ORDER BY o.id`
	prepared := correlatedExistsRuntimePrepare(t, session, scalarSQL)
	if !prepared.statement.usesDirectDurableCatalog() {
		t.Fatal("decorrelated EXISTS did not select the coherent direct durable catalog")
	}

	ctx := context.Background()
	var cursor Cursor
	run := func() {
		if err := prepared.QueryInto(ctx, nil, &cursor); err != nil {
			panic(err)
		}
		rows := 0
		for cursor.Next() {
			rows++
		}
		if rows != 2 {
			panic(fmt.Sprintf("decorrelated EXISTS returned %d rows, want 2", rows))
		}
		if err := cursor.Close(); err != nil {
			panic(err)
		}
	}
	run()
	scalarPlan := correlatedExistsRuntimeExplainAnalyze(t, session, scalarSQL)
	if len(scalarPlan.Plan.Joins) != 1 ||
		scalarPlan.Plan.Joins[0].AccessPath != "decorrelated-exists-semi" {
		t.Fatalf("scalar correlated plan = %+v, want one decorrelated EXISTS semi-join",
			scalarPlan.Plan.Joins)
	}
	stats := scalarPlan.Plan.Analyze
	if !stats.IndexBounded || stats.IndexLookups == 0 {
		t.Fatalf("scalar-only correlation did not use the safe outer exact index: %+v", stats)
	}
	if stats.JoinMemberships+stats.JoinLookups != 1 ||
		stats.JoinBuilds != 0 || stats.JoinPairs != 0 {
		t.Fatalf("decorrelated EXISTS was not one fan-out-free adaptive semi-join: %+v", stats)
	}
	if allocs := testing.AllocsPerRun(200, run); allocs != 0 {
		t.Fatalf("warmed decorrelated EXISTS allocated %.2f times, want zero", allocs)
	}

	const scalarAntiSQL = `
		SELECT o.id FROM ce_stats_outer AS o WHERE NOT EXISTS (
			SELECT 1 FROM ce_stats_inner AS i
			WHERE i.match_key = o.match_key AND i.active = TRUE
		) ORDER BY o.id`
	anti := correlatedExistsRuntimePrepare(t, session, scalarAntiSQL)
	if !anti.statement.usesDirectDurableCatalog() {
		t.Fatal("decorrelated NOT EXISTS did not select the coherent direct durable catalog")
	}
	var antiCursor Cursor
	runAnti := func() {
		if err := anti.QueryInto(ctx, nil, &antiCursor); err != nil {
			panic(err)
		}
		rows := 0
		for antiCursor.Next() {
			rows++
		}
		if rows != 1 {
			panic(fmt.Sprintf("decorrelated NOT EXISTS returned %d rows, want 1", rows))
		}
		if err := antiCursor.Close(); err != nil {
			panic(err)
		}
	}
	runAnti()
	antiPlan := correlatedExistsRuntimeExplainAnalyze(t, session, scalarAntiSQL)
	if len(antiPlan.Plan.Joins) != 1 ||
		antiPlan.Plan.Joins[0].AccessPath != "decorrelated-exists-anti" {
		t.Fatalf("scalar anti plan = %+v, want one decorrelated EXISTS anti-join",
			antiPlan.Plan.Joins)
	}
	antiStats := antiPlan.Plan.Analyze
	if antiStats.JoinMemberships+antiStats.JoinLookups != 1 ||
		antiStats.JoinBuilds != 0 || antiStats.JoinPairs != 0 {
		t.Fatalf("decorrelated NOT EXISTS was not one fan-out-free adaptive anti-join: %+v",
			antiStats)
	}
	if allocs := testing.AllocsPerRun(200, runAnti); allocs != 0 {
		t.Fatalf("warmed decorrelated NOT EXISTS allocated %.2f times, want zero", allocs)
	}

	for _, statement := range []string{
		`CREATE TABLE ce_stats_container_outer (id STRING PRIMARY KEY, match_key ANY)`,
		`CREATE INDEX ce_stats_container_match ON ce_stats_container_outer (match_key)`,
		`CREATE TABLE ce_stats_container_inner (id STRING PRIMARY KEY, match_key ANY)`,
		`INSERT INTO ce_stats_container_outer VALUES ` +
			`('{"id":"a_array","match_key":[1,2]}'),` +
			`('{"id":"b_object","match_key":{"a":1,"b":2}}'),` +
			`('{"id":"c_object_order","match_key":{"b":2,"a":1}}'),` +
			`('{"id":"d_scalar","match_key":"scalar"}'),` +
			`('{"id":"e_array_order","match_key":[2,1]}'),` +
			`('{"id":"f_object_value","match_key":{"a":1,"b":3}}'),` +
			`('{"id":"g_object_member","match_key":{"a":1,"c":2}}'),` +
			`('{"id":"h_missing"}')`,
		`INSERT INTO ce_stats_container_inner VALUES ` +
			`('{"id":"i_scalar","match_key":"scalar"}'),` +
			`('{"id":"i_array","match_key":[1,2]}'),` +
			`('{"id":"i_object","match_key":{"a":1,"b":2}}')`,
	} {
		correlatedExistsRuntimeExec(t, session, statement, nil)
	}
	const containerSQL = `
		SELECT o.id FROM ce_stats_container_outer AS o WHERE EXISTS (
			SELECT 1 FROM ce_stats_container_inner AS i
			WHERE i.match_key = o.match_key
		) ORDER BY o.id`
	container := correlatedExistsRuntimePrepare(t, session, containerSQL)
	if !container.statement.usesDirectDurableCatalog() {
		t.Fatal("container decorrelation did not retain the direct durable catalog")
	}
	containerWant := [...]string{"a_array", "b_object", "c_object_order", "d_scalar"}
	var containerCursor Cursor
	runContainer := func() {
		if err := container.QueryInto(ctx, nil, &containerCursor); err != nil {
			panic(err)
		}
		rows := 0
		for containerCursor.Next() {
			id, ok := containerCursor.Cell(0).Text()
			if !ok || rows >= len(containerWant) || id != containerWant[rows] {
				panic(fmt.Sprintf("container row %d = %q/%t", rows, id, ok))
			}
			rows++
		}
		if rows != len(containerWant) {
			panic(fmt.Sprintf("container decorrelation returned %d rows, want %d",
				rows, len(containerWant)))
		}
		if err := containerCursor.Close(); err != nil {
			panic(err)
		}
	}
	runContainer()
	containerPlan := correlatedExistsRuntimeExplainAnalyze(t, session, containerSQL)
	if len(containerPlan.Plan.Joins) != 1 ||
		containerPlan.Plan.Joins[0].AccessPath != "decorrelated-exists-semi" {
		t.Fatalf("container correlated plan = %+v, want one decorrelated EXISTS semi-join",
			containerPlan.Plan.Joins)
	}
	containerStats := containerPlan.Plan.Analyze
	if containerStats.IndexBounded || containerStats.IndexLookups != 0 {
		t.Fatalf("container membership unsafely bounded/probed the outer index: %+v", containerStats)
	}
	if containerStats.RowsScanned != containerStats.RowsTotal || containerStats.RowsTotal != 8 {
		t.Fatalf("container membership did not answer from one complete outer scan: %+v", containerStats)
	}
	if containerStats.JoinMemberships+containerStats.JoinLookups != 1 ||
		containerStats.JoinBuilds != 0 || containerStats.JoinPairs != 0 {
		t.Fatalf("container EXISTS was not one fan-out-free adaptive semi-join: %+v", containerStats)
	}
	if allocs := testing.AllocsPerRun(200, runContainer); allocs != 0 {
		t.Fatalf("warmed container decorrelation allocated %.2f times, want zero", allocs)
	}

	const containerAntiSQL = `
		SELECT o.id FROM ce_stats_container_outer AS o WHERE NOT EXISTS (
			SELECT 1 FROM ce_stats_container_inner AS i
			WHERE i.match_key = o.match_key
		) ORDER BY o.id`
	containerAnti := correlatedExistsRuntimePrepare(t, session, containerAntiSQL)
	if !containerAnti.statement.usesDirectDurableCatalog() {
		t.Fatal("container anti-decorrelation did not retain the direct durable catalog")
	}
	containerAntiWant := [...]string{
		"e_array_order", "f_object_value", "g_object_member", "h_missing",
	}
	var containerAntiCursor Cursor
	runContainerAnti := func() {
		if err := containerAnti.QueryInto(ctx, nil, &containerAntiCursor); err != nil {
			panic(err)
		}
		rows := 0
		for containerAntiCursor.Next() {
			id, ok := containerAntiCursor.Cell(0).Text()
			if !ok || rows >= len(containerAntiWant) || id != containerAntiWant[rows] {
				panic(fmt.Sprintf("container anti row %d = %q/%t", rows, id, ok))
			}
			rows++
		}
		if rows != len(containerAntiWant) {
			panic(fmt.Sprintf("container anti returned %d rows, want %d",
				rows, len(containerAntiWant)))
		}
		if err := containerAntiCursor.Close(); err != nil {
			panic(err)
		}
	}
	runContainerAnti()
	containerAntiPlan := correlatedExistsRuntimeExplainAnalyze(t, session, containerAntiSQL)
	if len(containerAntiPlan.Plan.Joins) != 1 ||
		containerAntiPlan.Plan.Joins[0].AccessPath != "decorrelated-exists-anti" {
		t.Fatalf("container anti plan = %+v, want one decorrelated EXISTS anti-join",
			containerAntiPlan.Plan.Joins)
	}
	containerAntiStats := containerAntiPlan.Plan.Analyze
	if containerAntiStats.IndexBounded || containerAntiStats.IndexLookups != 0 ||
		containerAntiStats.RowsScanned != containerAntiStats.RowsTotal ||
		containerAntiStats.RowsTotal != 8 ||
		containerAntiStats.JoinMemberships+containerAntiStats.JoinLookups != 1 ||
		containerAntiStats.JoinBuilds != 0 || containerAntiStats.JoinPairs != 0 {
		t.Fatalf("container NOT EXISTS plan was not one full-scan adaptive anti-join: %+v",
			containerAntiStats)
	}
	if allocs := testing.AllocsPerRun(200, runContainerAnti); allocs != 0 {
		t.Fatalf("warmed container NOT EXISTS allocated %.2f times, want zero", allocs)
	}
}

func BenchmarkCorrelatedExistsDecorrelated(b *testing.B) {
	database, err := Open(filepath.Join(b.TempDir(), "catalog.vdb"))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = database.Close() })
	session, err := database.NewSession(context.Background())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = session.Close() })
	for _, statement := range []string{
		`CREATE TABLE ce_bench_outer (id STRING PRIMARY KEY, match_key STRING)`,
		`CREATE INDEX ce_bench_outer_match ON ce_bench_outer (match_key)`,
		`CREATE TABLE ce_bench_inner (id STRING PRIMARY KEY, match_key STRING, active BOOL)`,
	} {
		correlatedExistsRuntimeExec(b, session, statement, nil)
	}
	insertOuter := correlatedExistsRuntimePrepare(b, session,
		`INSERT INTO ce_bench_outer VALUES (?)`)
	for i := 0; i < 128; i++ {
		if _, err := insertOuter.Exec(context.Background(), []any{fmt.Sprintf(
			`{"id":"o%03d","match_key":"k%02d"}`, i, i%32)}); err != nil {
			b.Fatal(err)
		}
	}
	insertInner := correlatedExistsRuntimePrepare(b, session,
		`INSERT INTO ce_bench_inner VALUES (?)`)
	for i := 0; i < 32; i++ {
		if _, err := insertInner.Exec(context.Background(), []any{fmt.Sprintf(
			`{"id":"i%02d","match_key":"k%02d","active":true}`, i, i)}); err != nil {
			b.Fatal(err)
		}
	}
	const benchmarkSQL = `
		SELECT o.id FROM ce_bench_outer AS o WHERE EXISTS (
			SELECT 1 FROM ce_bench_inner AS i
			WHERE i.match_key = o.match_key AND i.active = TRUE
		)`
	prepared := correlatedExistsRuntimePrepare(b, session, benchmarkSQL)
	if !prepared.statement.usesDirectDurableCatalog() {
		b.Fatal("benchmark did not select the direct durable catalog")
	}
	ctx := context.Background()
	var cursor Cursor
	run := func() {
		if err := prepared.QueryInto(ctx, nil, &cursor); err != nil {
			b.Fatal(err)
		}
		rows := 0
		for cursor.Next() {
			rows++
		}
		if rows != 128 {
			b.Fatalf("rows = %d, want 128", rows)
		}
		if err := cursor.Close(); err != nil {
			b.Fatal(err)
		}
	}
	run()
	plan := correlatedExistsRuntimeExplainAnalyze(b, session, benchmarkSQL)
	stats := plan.Plan.Analyze
	if !stats.IndexBounded || stats.IndexLookups == 0 ||
		stats.JoinMemberships+stats.JoinLookups != 1 ||
		stats.JoinBuilds != 0 || stats.JoinPairs != 0 {
		b.Fatalf("unexpected decorrelated plan statistics: %+v", stats)
	}
	b.ReportAllocs()
	b.ReportMetric(128, "outer-rows/op")
	b.ResetTimer()
	for b.Loop() {
		run()
	}
}
