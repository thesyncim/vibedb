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

func TestPostgresWriteStatementReturningProjection(t *testing.T) {
	executor, _ := newSQLRF3TestExecutor(t)
	backend := &PostgreSQLBackend{
		Executor: executor,
		Authorize: func(pgwire.SessionIdentity) (serviceauthz.Authority, error) {
			return serviceauthz.Authority{Node: rafttransport.NodeID{1}, Generation: 1}, nil
		},
		Write: func(_ context.Context, _ serviceauthz.Authority, q Query) (*Result, error) {
			return &Result{
				Kind:          shardservice.ResponseCompletion,
				RowsAffected:  1,
				ReturningDocs: [][]byte{[]byte(`{"id":"k1","n":42}`)},
			}, nil
		},
	}
	session, err := backend.NewSession(context.Background(), pgwire.SessionIdentity{User: "local"})
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	defer session.Close()

	stmt, err := session.Prepare(context.Background(), `INSERT INTO messages (id, n) VALUES ('k1', 42) RETURNING id, n + 1 AS next_n`)
	if err != nil {
		t.Fatalf("Prepare RETURNING: %v", err)
	}
	defer stmt.Close()
	if !stmt.ReturnsRows() {
		t.Fatal("expected ReturnsRows() == true")
	}
	cols := stmt.Columns()
	if len(cols) != 2 || cols[0] != "id" || cols[1] != "next_n" {
		t.Fatalf("Columns = %v, want [id next_n]", cols)
	}
	var rows pgwire.BackendRows
	if err := stmt.QueryInto(context.Background(), nil, &rows); err != nil {
		t.Fatalf("QueryInto RETURNING: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("expected 1 returning row")
	}
	gotID := string(rows.Cell(0).AppendJSON(nil))
	gotNext := string(rows.Cell(1).AppendJSON(nil))
	if gotID != `"k1"` || (gotNext != `43` && gotNext != `4.3e1`) {
		t.Fatalf("got (%s, %s), want (\"k1\", 43)", gotID, gotNext)
	}
	if rows.Next() {
		t.Fatal("expected only 1 returning row")
	}
	_ = driver.SessionIdle
}
