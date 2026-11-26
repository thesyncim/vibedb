package gateway

import (
	"context"
	"testing"

	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/pgwire"
)

func TestDistributedViewsCreateListExpandDrop(t *testing.T) {
	executor, _ := newSQLRF3TestExecutor(t)
	backend := &PostgreSQLBackend{
		Executor: executor,
		Authorize: func(pgwire.SessionIdentity) (serviceauthz.Authority, error) {
			return serviceauthz.Authority{Node: rafttransport.NodeID{1}, Generation: 1}, nil
		},
	}
	session, err := backend.NewSession(context.Background(), pgwire.SessionIdentity{User: "local"})
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	defer session.Close()

	// CREATE VIEW
	createStmt, err := session.Prepare(context.Background(), `CREATE VIEW recent_msgs (msg_id) AS SELECT id FROM messages`)
	if err != nil {
		t.Fatalf("Prepare CREATE VIEW: %v", err)
	}
	if _, err := createStmt.Exec(context.Background(), nil); err != nil {
		t.Fatalf("Exec CREATE VIEW: %v", err)
	}
	_ = createStmt.Close()

	// Views() exposes the new view
	views, err := session.(*postgresSession).Views(context.Background())
	if err != nil {
		t.Fatalf("Views: %v", err)
	}
	if len(views) != 1 || views[0].Name != "recent_msgs" {
		t.Fatalf("Views = %+v, want [recent_msgs]", views)
	}

	// SELECT against the view prepares cleanly via CTE expansion
	selStmt, err := session.Prepare(context.Background(), `SELECT msg_id FROM recent_msgs`)
	if err != nil {
		t.Fatalf("Prepare SELECT from view: %v", err)
	}
	_ = selStmt.Close()

	// DROP VIEW
	dropStmt, err := session.Prepare(context.Background(), `DROP VIEW recent_msgs RESTRICT`)
	if err != nil {
		t.Fatalf("Prepare DROP VIEW: %v", err)
	}
	if _, err := dropStmt.Exec(context.Background(), nil); err != nil {
		t.Fatalf("Exec DROP VIEW: %v", err)
	}
	_ = dropStmt.Close()

	views, err = session.(*postgresSession).Views(context.Background())
	if err != nil || len(views) != 0 {
		t.Fatalf("Views after drop = %+v, err=%v; want empty", views, err)
	}
}
