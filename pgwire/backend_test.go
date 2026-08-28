package pgwire

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/thesyncim/vibedb/query"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

type recordingBackend struct {
	embeddedBackend
	mu         sync.Mutex
	identities []SessionIdentity
	closed     int
}

func (backend *recordingBackend) NewSession(ctx context.Context, identity SessionIdentity) (BackendSession, error) {
	session, err := backend.embeddedBackend.NewSession(ctx, identity)
	if err != nil {
		return nil, err
	}
	backend.mu.Lock()
	backend.identities = append(backend.identities, identity)
	backend.mu.Unlock()
	return &recordingBackendSession{BackendSession: session, backend: backend}, nil
}

type recordingBackendSession struct {
	BackendSession
	backend *recordingBackend
	once    sync.Once
}

func (session *recordingBackendSession) Close() error {
	err := session.BackendSession.Close()
	session.once.Do(func() {
		session.backend.mu.Lock()
		session.backend.closed++
		session.backend.mu.Unlock()
	})
	return err
}

func TestExecutionBackendReceivesAuthenticatedIdentityAndCloses(t *testing.T) {
	backend := &recordingBackend{embeddedBackend: embeddedBackend{testDatabase(t, "users", corpus)}}
	server, err := NewServerWithBackend(backend, Options{Auth: Trust(), Database: "app"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })
	client := dial(t, server)
	client.startup(map[string]string{"user": "tester", "database": "app"})
	backend.mu.Lock()
	if len(backend.identities) != 1 || backend.identities[0] != (SessionIdentity{User: "tester", Database: "app"}) {
		t.Errorf("backend identities = %+v", backend.identities)
	}
	backend.mu.Unlock()
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if backend.closed != 1 {
		t.Fatalf("closed sessions = %d", backend.closed)
	}
}

func TestExecutionBackendRejectsInvalidComposition(t *testing.T) {
	if _, err := NewServerWithBackend(nil, Options{Auth: Trust()}); err == nil {
		t.Fatal("nil backend accepted")
	}
	backend := &recordingBackend{embeddedBackend: embeddedBackend{testDatabase(t, "users", nil)}}
	if _, err := NewServerWithBackend(backend, Options{}); err == nil {
		t.Fatal("implicit authentication accepted")
	}
}

func TestExecutionBackendIsNotOpenedBeforeAuthentication(t *testing.T) {
	backend := &recordingBackend{embeddedBackend: embeddedBackend{testDatabase(t, "users", nil)}}
	verifier, err := NewVerifier("correct-password")
	if err != nil {
		t.Fatal(err)
	}
	server, err := NewServerWithBackend(backend, Options{Auth: SCRAM(func(user string) (Verifier, bool) {
		return verifier, user == "alice"
	})})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })
	client := &scramClient{t: t, c: dial(t, server), user: "alice", password: "wrong", gs2: "n"}
	if result := client.authenticate(); result.tag != msgErrorResponse {
		t.Fatalf("bad credentials accepted: %v", result.tag)
	}
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if len(backend.identities) != 0 || backend.closed != 0 {
		t.Fatalf("unauthenticated client reached backend: identities=%v closed=%d", backend.identities, backend.closed)
	}
}

func TestEmbeddedBackendAddsNoWarmExecutionAllocations(t *testing.T) {
	db := testDatabase(t, "docs", []string{`{"value":1}`, `{"value":2}`})
	ctx := context.Background()
	direct, err := db.NewSession(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer direct.Close()
	directStatement, err := direct.Prepare(ctx, "SELECT value FROM docs")
	if err != nil {
		t.Fatal(err)
	}
	defer directStatement.Close()
	wrapped, err := (embeddedBackend{db}).NewSession(ctx, SessionIdentity{})
	if err != nil {
		t.Fatal(err)
	}
	defer wrapped.Close()
	wrappedStatement, err := wrapped.Prepare(ctx, "SELECT value FROM docs")
	if err != nil {
		t.Fatal(err)
	}
	defer wrappedStatement.Close()
	var directRows sqldriver.Cursor
	var wrappedRows BackendRows
	runDirect := func() {
		if err := directStatement.QueryInto(ctx, nil, &directRows); err != nil {
			panic(err)
		}
		for directRows.Next() {
			_ = directRows.Cell(0)
		}
		if err := directRows.Close(); err != nil {
			panic(err)
		}
	}
	runWrapped := func() {
		if err := wrappedStatement.QueryInto(ctx, nil, &wrappedRows); err != nil {
			panic(err)
		}
		for wrappedRows.Next() {
			_ = wrappedRows.Cell(0)
		}
		if err := wrappedRows.Close(); err != nil {
			panic(err)
		}
	}
	runDirect()
	runWrapped()
	directAllocs := testing.AllocsPerRun(100, runDirect)
	wrappedAllocs := testing.AllocsPerRun(100, runWrapped)
	if wrappedAllocs != directAllocs {
		t.Fatalf("backend added warm execution allocations: direct=%g wrapped=%g", directAllocs, wrappedAllocs)
	}
	t.Logf("warm execution allocations: direct=%g wrapped=%g", directAllocs, wrappedAllocs)
}

func TestMaterializedBackendRowsWarmLifecycleAllocatesNothing(t *testing.T) {
	cursor := query.NewTextCursor("value", "distributed result")
	var rows BackendRows
	releases := 0
	release := func() error { releases++; return nil }
	run := func() {
		if err := rows.SetMaterialized(cursor, release); err != nil {
			panic(err)
		}
		preflight := rows.Snapshot()
		if !preflight.Next() || !rows.Next() {
			panic("missing materialized row")
		}
		_ = rows.Cell(0)
		if err := rows.Close(); err != nil {
			panic(err)
		}
	}
	run()
	if allocations := testing.AllocsPerRun(1000, run); allocations != 0 {
		t.Fatalf("materialized row lifecycle allocations=%g, want zero", allocations)
	}
	if releases == 0 {
		t.Fatal("result lifetime not released")
	}
}

func TestBackendRowsPreflightAndRelease(t *testing.T) {
	var rows BackendRows
	closed := 0
	releaseErr := errors.New("release witness")
	if err := rows.SetMaterialized(query.NewTextCursor("value", "rf3"), func() error { closed++; return releaseErr }); err != nil {
		t.Fatal(err)
	}
	if err := rows.SetMaterialized(query.Cursor{}, nil); !errors.Is(err, sqldriver.ErrCursorOpen) {
		t.Fatalf("live overwrite = %v", err)
	}
	preflight := rows.Snapshot()
	if !preflight.Next() || !rows.Next() {
		t.Fatal("preflight advanced the live cursor")
	}
	if rows.Next() {
		t.Fatal("unexpected second row")
	}
	if err := rows.Close(); !errors.Is(err, releaseErr) {
		t.Fatalf("close = %v", err)
	}
	if err := rows.Close(); err != nil || closed != 1 {
		t.Fatalf("second close = %v; releases=%d", err, closed)
	}
	if rows.Next() {
		t.Fatal("closed cursor advanced")
	}
	if err := rows.SetMaterialized(query.NewTextCursor("value", "reused"), nil); err != nil {
		t.Fatal(err)
	}
	if !rows.Next() {
		t.Fatal("closed storage could not be reused")
	}
	_ = rows.Close()
}
