package gateway

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/pgwire"
	"github.com/thesyncim/vibedb/query"
	driver "github.com/thesyncim/vibedb/sql/driver"
)

func TestPostgreSQLRF3ReadOnlyStateLimitsAndCancellation(t *testing.T) {
	executor, client := newSQLRF3TestExecutor(t)
	authority := serviceauthz.Authority{Generation: 1}
	authority.Node[0] = 1
	backend := &PostgreSQLBackend{Executor: executor, Authorize: func(pgwire.SessionIdentity) (serviceauthz.Authority, error) { return authority, nil }}
	ctx := context.Background()
	session, err := backend.NewSession(ctx, pgwire.SessionIdentity{})
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	if _, err := session.Prepare(ctx, `DELETE FROM messages WHERE id = 'a'`); !errors.Is(err, driver.ErrReadOnlyTransaction) {
		t.Fatalf("write not refused: %v", err)
	}
	if client.queries != 0 {
		t.Fatal("write dispatched")
	}
	if err := session.Begin(ctx, driver.TxOptions{Isolation: driver.IsolationSerializable}); err == nil {
		t.Fatal("invented serializable isolation")
	}
	if err := session.Begin(ctx, driver.TxOptions{}); err != nil {
		t.Fatal(err)
	}
	session.MarkFailed()
	if _, err := session.Prepare(ctx, `SELECT 1`); !errors.Is(err, driver.ErrTransactionFailed) {
		t.Fatal(err)
	}
	if err := session.Rollback(ctx); err != nil || session.State() != driver.SessionIdle {
		t.Fatal(err)
	}
	statement, err := session.Prepare(ctx, `SELECT id FROM messages`)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Close()
	if err := session.SetResultLimits(1, 4<<20); err != nil {
		t.Fatal(err)
	}
	var rows pgwire.BackendRows
	defer rows.Close()
	if err := statement.QueryInto(ctx, nil, &rows); !errors.Is(err, ErrResultLimit) {
		t.Fatalf("aggregate row limit not enforced: %v", err)
	}
	if rows.Next() {
		t.Fatal("partial rows escaped")
	}
	var flag query.CancelFlag
	if err := session.SetCancelFlag(&flag); err != nil {
		t.Fatal(err)
	}
	flag.Cancel()
	before := client.queries
	if err := statement.QueryInto(ctx, nil, &rows); !errors.Is(err, query.ErrCanceled) {
		t.Fatal(err)
	}
	if client.queries != before {
		t.Fatal("canceled query dispatched")
	}
	flag.Reset()
	point, err := session.Prepare(ctx, `SELECT id FROM messages WHERE id = 'a'`)
	if err != nil {
		t.Fatal(err)
	}
	defer point.Close()
	client.started = make(chan struct{}, 1)
	done := make(chan error, 1)
	go func() { done <- point.QueryInto(ctx, nil, &rows) }()
	select {
	case <-client.started:
	case <-time.After(5 * time.Second):
		t.Fatal("query did not start")
	}
	flag.Cancel()
	session.(*postgresSession).Cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("lost cancellation: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("in-flight query was not canceled")
	}
	if rows.Next() {
		t.Fatal("canceled query exposed rows")
	}
}

func TestPostgreSQLRF3MaterializesNull(t *testing.T) {
	executor, transport := newSQLRF3TestExecutor(t)
	transport.nullResult = true
	authority := serviceauthz.Authority{Generation: 1}
	authority.Node[0] = 1
	backend := &PostgreSQLBackend{Executor: executor, Authorize: func(pgwire.SessionIdentity) (serviceauthz.Authority, error) { return authority, nil }}
	s, err := backend.NewSession(context.Background(), pgwire.SessionIdentity{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	p, err := s.Prepare(context.Background(), `SELECT id FROM messages`)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	var rows pgwire.BackendRows
	defer rows.Close()
	if err := p.QueryInto(context.Background(), nil, &rows); err != nil {
		t.Fatal(err)
	}
	n := 0
	for rows.Next() {
		n++
		if !rows.Cell(0).IsNull() {
			t.Fatalf("native NULL became invalid cell: %+v", rows.Cell(0))
		}
	}
	if n != 2 {
		t.Fatalf("scatter NULL rows=%d", n)
	}
}
