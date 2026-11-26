package gateway

import (
	"context"
	"testing"

	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/pgwire"
	"github.com/thesyncim/vibedb/shardservice"
	"github.com/thesyncim/vibedb/sql/driver"
)

func TestDistributedPGWireInteractiveTxAndSavepoints(t *testing.T) {
	executor, _ := newSQLRF3TestExecutor(t)
	var committed []Query
	backend := &PostgreSQLBackend{
		Executor: executor,
		Authorize: func(pgwire.SessionIdentity) (serviceauthz.Authority, error) {
			return serviceauthz.Authority{Node: rafttransport.NodeID{1}, Generation: 1}, nil
		},
		Write: func(_ context.Context, _ serviceauthz.Authority, q Query) (*Result, error) {
			return &Result{Kind: shardservice.ResponseCompletion, RowsAffected: 1}, nil
		},
		WriteBatch: func(_ context.Context, _ serviceauthz.Authority, qs []Query) (*Result, error) {
			committed = append(committed, qs...)
			return &Result{Kind: shardservice.ResponseCompletion, RowsAffected: int64(len(qs))}, nil
		},
	}
	session, err := backend.NewSession(context.Background(), pgwire.SessionIdentity{User: "local"})
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	defer session.Close()

	if err := session.Begin(context.Background(), driver.TxOptions{}); err != nil {
		t.Fatalf("Begin: %v", err)
	}
	st1, err := session.Prepare(context.Background(), `INSERT INTO messages (id, n) VALUES ('k1', 1)`)
	if err != nil {
		t.Fatalf("Prepare 1: %v", err)
	}
	if _, err := st1.Exec(context.Background(), nil); err != nil {
		t.Fatalf("Exec 1: %v", err)
	}
	_ = st1.Close()

	if err := session.Savepoint(context.Background(), "sp1"); err != nil {
		t.Fatalf("Savepoint: %v", err)
	}
	st2, err := session.Prepare(context.Background(), `INSERT INTO messages (id, n) VALUES ('k2', 2)`)
	if err != nil {
		t.Fatalf("Prepare 2: %v", err)
	}
	if _, err := st2.Exec(context.Background(), nil); err != nil {
		t.Fatalf("Exec 2: %v", err)
	}
	_ = st2.Close()

	if err := session.RollbackTo(context.Background(), "sp1"); err != nil {
		t.Fatalf("RollbackTo: %v", err)
	}
	st3, err := session.Prepare(context.Background(), `INSERT INTO messages (id, n) VALUES ('k3', 3)`)
	if err != nil {
		t.Fatalf("Prepare 3: %v", err)
	}
	if _, err := st3.Exec(context.Background(), nil); err != nil {
		t.Fatalf("Exec 3: %v", err)
	}
	_ = st3.Close()

	if err := session.Commit(context.Background()); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if len(committed) != 2 {
		t.Fatalf("committed %d queries, want 2 (k1 and k3): %+v", len(committed), committed)
	}
}
