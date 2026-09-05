package driver

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/query"
)

type replicatedPointRawCase struct {
	name         string
	text         string
	args         []any
	found        bool
	sessionKey   []byte
	candidateKey []byte
}

type replicatedPointRawRows [][]string

func replicatedPointRawKey(t *testing.T, fixture replicatedPointSessionFixture, document []byte) []byte {
	t.Helper()
	core := fixture.claim.database
	core.mu.RLock()
	table := core.tables[fixture.base.UserTable]
	key, err := documentKey(document, table.meta.PrimaryKey, table.primary, table.collection.MaxKeyBytes())
	core.mu.RUnlock()
	if err != nil {
		t.Fatalf("documentKey(%s): %v", document, err)
	}
	return []byte(key)
}

// runReplicatedPointRawCase executes one point session through the public
// candidate-key path or through the same statement with its Segment source
// forced. Keeping the source choice here makes the result comparison exercise
// the driver boundary while leaving the SQL plan and candidate key identical.
func runReplicatedPointRawCase(
	t testing.TB,
	fixture replicatedPointSessionFixture,
	raw []byte,
	found bool,
	sessionKey []byte,
	candidateKey []byte,
	text string,
	args []any,
	options query.ExecOptions,
	validatedRaw bool,
	mutateInput bool,
) (replicatedPointRawRows, int, error) {
	t.Helper()
	ctx := context.Background()
	primaryPath := []byte(fixture.base.UserPrimaryKey)
	if !found {
		raw = nil // A point miss is required to carry no value.
	}
	session, err := fixture.claim.NewPointReadSession(
		ctx, 1, sessionKey, found, raw, primaryPath, options,
	)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = session.Close() }()
	prepared, err := session.Prepare(ctx, text)
	if err != nil {
		return nil, 0, err
	}
	if options.Cancel != nil {
		// Arm cancellation after preparation so the test exercises execution
		// and raw-wrapper cleanup, rather than stopping inside the SQL parser.
		options.Cancel.Cancel()
	}
	if mutateInput && len(raw) != 0 {
		// NewPointReadSession promises detached ownership after its validation
		// boundary. Corrupting the caller's buffer catches accidental borrowing.
		raw[0] = 'x'
	}

	var cursor Cursor
	if validatedRaw {
		err = prepared.QueryCandidateKeysInto(
			ctx, args, primaryPath, [][]byte{candidateKey}, &cursor,
		)
	} else {
		state := session.conn.tx.tables[prepared.statement.query.Collection()]
		if state == nil {
			return nil, 0, errors.New("point test: missing transaction table state")
		}
		source, sourceErr := session.conn.pointTransactionSource(
			ctx, state, []string{string(candidateKey)}, false,
		)
		if sourceErr != nil {
			return nil, 0, sourceErr
		}
		bound, bindErr := session.conn.runtimeValues(
			prepared.statement.paramKinds, args,
		)
		if bindErr != nil {
			return nil, 0, bindErr
		}
		defer clear(bound)
		queryCursor, runErr := prepared.statement.query.RunInto(
			&session.conn.exec, source, bound,
		)
		session.conn.pointSource.Bind(nil)
		if runErr == nil {
			session.conn.open = true
			rowset := session.conn.resetRows(prepared.statement, queryCursor, nil)
			cursor = Cursor{
				session:  &session.session,
				prepared: prepared,
				rows:     rowset,
			}
			session.session.current = &cursor
		}
		err = runErr
	}
	if err != nil {
		return nil, session.conn.pointDocs.Len(), err
	}
	defer func() { _ = cursor.Close() }()
	if mutateInput {
		// Result cells must also outlive the synchronous raw source itself.
		// The session owns this copy, independently of the caller buffer above.
		state := session.conn.tx.tables[prepared.statement.query.Collection()]
		clear(state.pointDocument)
	}
	pointDocs := session.conn.pointDocs.Len()
	columns := len(prepared.Columns())
	rows := make(replicatedPointRawRows, 0, 1)
	for cursor.Next() {
		row := make([]string, columns)
		for column := range row {
			row[column] = string(cursor.Cell(column).AppendJSON(nil))
		}
		rows = append(rows, row)
	}
	if closeErr := cursor.Close(); closeErr != nil {
		return nil, pointDocs, closeErr
	}
	return rows, pointDocs, nil
}

func assertReplicatedPointRawWrapperEmpty(
	t *testing.T,
	prepared *Prepared,
	connection *conn,
) {
	t.Helper()
	var execution query.Exec
	defer execution.Release()
	_, err := prepared.statement.query.RunInto(
		&execution, query.FromValidatedRaw(&connection.pointSource), nil,
	)
	if err == nil || !strings.Contains(err.Error(), "empty source") {
		t.Fatalf("validated point wrapper after execution = %v, want empty-source error", err)
	}
}

func TestReplicatedPointRawMatchesForcedSegmentFallback(t *testing.T) {
	fixture := newReplicatedPointSessionFixture(t,
		`{"id":"a","score":10,"nullable":null,"name":"quote \" slash \\","nested":{"value":7}}`,
	)
	wrongKey := replicatedPointRawKey(
		t, fixture, []byte(`{"id":"wrong"}`),
	)
	cases := []replicatedPointRawCase{
		{
			name:       "full predicate with null missing nested and escaped scalar",
			text:       `SELECT id, nullable, missing, nested, name FROM docs WHERE id = 'a' AND score >= ? ORDER BY id LIMIT 1`,
			args:       []any{int64(10)},
			found:      true,
			sessionKey: fixture.key, candidateKey: fixture.key,
		},
		{
			name:       "residual predicate rejects row",
			text:       `SELECT id, name FROM docs WHERE id = 'a' AND score > ?`,
			args:       []any{int64(10)},
			found:      true,
			sessionKey: fixture.key, candidateKey: fixture.key,
		},
		{
			name:       "aggregate fallback",
			text:       `SELECT COUNT(*) FROM docs WHERE id = 'a'`,
			found:      true,
			sessionKey: fixture.key, candidateKey: fixture.key,
		},
		{
			name:       "point miss",
			text:       `SELECT id, name FROM docs WHERE id = 'a'`,
			found:      false,
			sessionKey: fixture.key, candidateKey: fixture.key,
		},
		{
			name:       "wrong candidate key",
			text:       `SELECT id, name FROM docs WHERE id = 'a'`,
			found:      true,
			sessionKey: fixture.key, candidateKey: wrongKey,
		},
	}
	options := query.ExecOptions{Workers: 1, ResultRows: -1, ResultBytes: -1}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			opt, optDocs, optErr := runReplicatedPointRawCase(
				t, fixture, fixture.raw, test.found, test.sessionKey, test.candidateKey,
				test.text, test.args, options, true, false,
			)
			fallback, fallbackDocs, fallbackErr := runReplicatedPointRawCase(
				t, fixture, fixture.raw, test.found, test.sessionKey, test.candidateKey,
				test.text, test.args, options, false, false,
			)
			if optErr != nil || fallbackErr != nil {
				t.Fatalf("successful differential case failed: optimized=%v, fallback=%v", optErr, fallbackErr)
			}
			if !reflect.DeepEqual(opt, fallback) {
				t.Fatalf("optimized rows=%v, fallback rows=%v", opt, fallback)
			}
			if optDocs != 0 {
				t.Fatalf("validated point source materialized %d Segment rows", optDocs)
			}
			if test.found && test.name != "wrong candidate key" &&
				test.name != "point miss" && fallbackDocs == 0 {
				t.Fatal("forced fallback did not materialize its point document")
			}
			if test.name == "full predicate with null missing nested and escaped scalar" &&
				len(opt) != 1 {
				t.Fatalf("full point result rows=%d, want 1", len(opt))
			}
			if test.name == "full predicate with null missing nested and escaped scalar" {
				want := replicatedPointRawRows{{
					`"a"`, `null`, `null`, `{"value":7}`, `"quote \" slash \\"`,
				}}
				if !reflect.DeepEqual(opt, want) {
					t.Fatalf("full point rows=%v, want %v", opt, want)
				}
			}
			if (test.name == "point miss" || test.name == "wrong candidate key") && len(opt) != 0 {
				t.Fatalf("absent point returned %d rows", len(opt))
			}
			if test.name == "residual predicate rejects row" && len(opt) != 0 {
				t.Fatalf("residual result rows=%d, want 0", len(opt))
			}
			if test.name == "aggregate fallback" &&
				!reflect.DeepEqual(opt, replicatedPointRawRows{{"1"}}) {
				t.Fatalf("aggregate result=%v, want [[1]]", opt)
			}
		})
	}
}

func TestReplicatedPointRawAlternatesFreshOwnedDocuments(t *testing.T) {
	fixture := newReplicatedPointSessionFixture(t,
		`{"id":"a","score":1,"name":"initial"}`,
	)
	options := query.ExecOptions{Workers: 1, ResultRows: -1, ResultBytes: -1}
	for _, score := range []int{11, 22, 33, 44} {
		t.Run(fmt.Sprint(score), func(t *testing.T) {
			raw := []byte(fmt.Sprintf(
				`{"id":"a","score":%d,"name":"fresh-%d"}`,
				score, score,
			))
			rows, pointDocs, err := runReplicatedPointRawCase(
				t, fixture, raw, true, fixture.key, fixture.key,
				`SELECT score, name FROM docs WHERE id = 'a'`,
				nil, options, true, true,
			)
			if err != nil {
				t.Fatal(err)
			}
			if pointDocs != 0 {
				t.Fatalf("fresh point source materialized %d Segment rows", pointDocs)
			}
			want := replicatedPointRawRows{{fmt.Sprint(score), fmt.Sprintf(`"fresh-%d"`, score)}}
			if !reflect.DeepEqual(rows, want) {
				t.Fatalf("fresh document rows=%v, want %v", rows, want)
			}
		})
	}
}

func TestReplicatedPointRawBudgetAndCancellationMatchFallback(t *testing.T) {
	fixture := newReplicatedPointSessionFixture(t,
		`{"id":"a","score":10,"name":"small"}`,
	)
	largeRaw := []byte(fmt.Sprintf(
		`{"id":"a","score":10,"pad":%q}`,
		strings.Repeat("x", 40<<10),
	))
	tight := query.ExecOptions{
		Workers: 1, MemoryBytes: 64 << 10, ResultRows: -1, ResultBytes: -1,
	}
	for _, validatedRaw := range []bool{true, false} {
		_, pointDocs, err := runReplicatedPointRawCase(
			t, fixture, largeRaw, true, fixture.key, fixture.key,
			`SELECT id FROM docs WHERE id = 'a'`, nil, tight, validatedRaw, false,
		)
		if !errors.Is(err, errPointMaterializationTooLarge) {
			t.Fatalf("validatedRaw=%v budget error=%v, want materialization bound", validatedRaw, err)
		}
		if pointDocs != 0 {
			t.Fatalf("validatedRaw=%v retained %d Segment rows after budget failure", validatedRaw, pointDocs)
		}
	}

	for _, validatedRaw := range []bool{true, false} {
		var cancel query.CancelFlag
		options := query.ExecOptions{
			Workers: 1, Cancel: &cancel, ResultRows: -1, ResultBytes: -1,
		}
		_, pointDocs, err := runReplicatedPointRawCase(
			t, fixture, fixture.raw, true, fixture.key, fixture.key,
			`SELECT id FROM docs WHERE id = 'a'`, nil, options, validatedRaw, false,
		)
		if !errors.Is(err, query.ErrCanceled) {
			t.Fatalf("validatedRaw=%v cancellation error=%v, want ErrCanceled", validatedRaw, err)
		}
		if validatedRaw && pointDocs != 0 {
			t.Fatalf("validatedRaw=%v retained %d Segment rows after cancellation", validatedRaw, pointDocs)
		}
	}
}

func TestReplicatedPointRawWrapperClearsAcrossHitMissAndError(t *testing.T) {
	fixture := newReplicatedPointSessionFixture(t,
		`{"id":"a","score":10,"name":"alpha"}`,
	)
	ctx := context.Background()
	options := query.ExecOptions{Workers: 1, ResultRows: -1, ResultBytes: -1}
	session, err := fixture.claim.NewPointReadSession(
		ctx, 1, fixture.key, true, fixture.raw,
		[]byte(fixture.base.UserPrimaryKey), options,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = session.Close() }()
	prepared, err := session.Prepare(ctx, `SELECT id FROM docs WHERE id = 'a'`)
	if err != nil {
		t.Fatal(err)
	}
	var cursor Cursor
	if err := prepared.QueryCandidateKeysInto(
		ctx, nil, []byte(fixture.base.UserPrimaryKey), [][]byte{fixture.key}, &cursor,
	); err != nil {
		t.Fatal(err)
	}
	if !cursor.Next() || string(cursor.Cell(0).AppendJSON(nil)) != `"a"` {
		t.Fatalf("point hit result=%s, want %s", cursor.Cell(0).JSON(), `"a"`)
	}
	if err := cursor.Close(); err != nil {
		t.Fatal(err)
	}
	assertReplicatedPointRawWrapperEmpty(t, prepared, &session.conn)

	wrongKey := replicatedPointRawKey(
		t, fixture, []byte(`{"id":"wrong"}`),
	)
	cursor = Cursor{}
	if err := prepared.QueryCandidateKeysInto(
		ctx, nil, []byte(fixture.base.UserPrimaryKey), [][]byte{wrongKey}, &cursor,
	); err != nil {
		t.Fatal(err)
	}
	if cursor.Next() {
		_ = cursor.Close()
		t.Fatal("wrong candidate returned the retained point document")
	}
	if err := cursor.Close(); err != nil {
		t.Fatal(err)
	}
	assertReplicatedPointRawWrapperEmpty(t, prepared, &session.conn)

	state := session.conn.tx.tables[prepared.statement.query.Collection()]
	state.pointDocument = []byte("{")
	cursor = Cursor{}
	if err := prepared.QueryCandidateKeysInto(
		ctx, nil, []byte(fixture.base.UserPrimaryKey), [][]byte{fixture.key}, &cursor,
	); err == nil {
		_ = cursor.Close()
		t.Fatal("malformed borrowed point document unexpectedly succeeded")
	}
	assertReplicatedPointRawWrapperEmpty(t, prepared, &session.conn)

	// The malformed source failed the session transaction. A fresh session must
	// still be able to execute the same SQL against a fresh document owner.
	fresh, err := fixture.claim.NewPointReadSession(
		ctx, 1, fixture.key, true, fixture.raw,
		[]byte(fixture.base.UserPrimaryKey), options,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = fresh.Close() }()
	freshPrepared, err := fresh.Prepare(ctx, `SELECT id FROM docs WHERE id = 'a'`)
	if err != nil {
		t.Fatal(err)
	}
	cursor = Cursor{}
	if err := freshPrepared.QueryCandidateKeysInto(
		ctx, nil, []byte(fixture.base.UserPrimaryKey), [][]byte{fixture.key}, &cursor,
	); err != nil {
		t.Fatal(err)
	}
	if !cursor.Next() || !bytes.Equal(cursor.Cell(0).AppendJSON(nil), []byte(`"a"`)) {
		t.Fatalf("fresh point result=%s, want %s", cursor.Cell(0).JSON(), `"a"`)
	}
	if err := cursor.Close(); err != nil {
		t.Fatal(err)
	}
}

// The final context checkpoint runs after the raw result has materialized,
// before queryRowsCandidates publishes its Cursor to the session.
type replicatedPointRawPanicContext struct {
	context.Context
	connection *conn
	panicked   bool
}

func (ctx *replicatedPointRawPanicContext) Err() error {
	if !ctx.panicked && ctx.connection.exec.Result.RowCount != 0 {
		ctx.panicked = true
		panic("point result checkpoint")
	}
	return nil
}

func TestReplicatedPointRawPanicClearsBorrowedWrapper(t *testing.T) {
	fixture := newReplicatedPointSessionFixture(t, `{"id":"a","name":"alpha"}`)
	session, err := fixture.claim.NewPointReadSession(
		context.Background(), 1, fixture.key, true, fixture.raw,
		[]byte(fixture.base.UserPrimaryKey), query.ExecOptions{Workers: 1},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = session.Close() }()
	prepared, err := session.Prepare(context.Background(), `SELECT name FROM docs WHERE id = 'a'`)
	if err != nil {
		t.Fatal(err)
	}
	ctx := &replicatedPointRawPanicContext{Context: context.Background(), connection: &session.conn}
	func() {
		defer func() {
			if got := recover(); got != "point result checkpoint" {
				t.Fatalf("panic=%v, want point result checkpoint", got)
			}
		}()
		var cursor Cursor
		_ = prepared.QueryCandidateKeysInto(
			ctx, nil, []byte(fixture.base.UserPrimaryKey), [][]byte{fixture.key}, &cursor,
		)
	}()
	if !ctx.panicked || session.conn.pointDocs.Len() != 0 {
		t.Fatal("panic regression did not execute the raw point lane")
	}
	assertReplicatedPointRawWrapperEmpty(t, prepared, &session.conn)
	if err := session.Close(); err != nil {
		t.Fatalf("close after point execution panic: %v", err)
	}
}
