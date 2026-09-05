package gateway

import (
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/pgwire"
	"github.com/thesyncim/vibedb/shardservice"
	sqlast "github.com/thesyncim/vibedb/sql"
)

func TestSnapshotPrepareAllowsConflictUpdateWithoutGlobalIndex(t *testing.T) {
	const text = `INSERT INTO messages (tenant_id, n) VALUES (?, ?) ON CONFLICT DO UPDATE SET n = EXCLUDED.n`

	prepared, err := testSnapshot(t, 1).Prepare(context.Background(), text)
	if err != nil || prepared == nil {
		t.Fatalf("Prepare error = %v, plan = %v", err, prepared)
	}
}

func TestSnapshotPrepareRejectsConflictNothingWithReadyGlobalIndex(t *testing.T) {
	config, endpoints := globalIndexCatalog(t)
	snapshot, err := NewSnapshotWithIndexes(
		config, endpoints, 1, []IndexDescriptor{testGlobalIndexDescriptor()},
	)
	if err != nil {
		t.Fatal(err)
	}

	const text = `INSERT INTO messages (tenant_id, id, email) VALUES (?, ?, ?) ON CONFLICT DO NOTHING`
	prepared, err := snapshot.Prepare(context.Background(), text)
	if !errors.Is(err, ErrDistributedWriteUnsupported) {
		t.Fatalf("Prepare error = %v, want ErrDistributedWriteUnsupported", err)
	}
	if prepared != nil {
		t.Fatal("indexed conflict skip returned a bindable plan")
	}
	prepared, err = snapshot.Prepare(context.Background(), `INSERT INTO messages (tenant_id,id,email) VALUES (?,?,?) ON CONFLICT DO UPDATE SET email=EXCLUDED.email`)
	if !errors.Is(err, ErrDistributedWriteUnsupported) || prepared != nil {
		t.Fatalf("indexed conflict update was not refused: %v %v", prepared, err)
	}
}

func TestSnapshotAllowsConflictNothingWithoutGlobalIndex(t *testing.T) {
	bound := writePlanBind(
		t, testSnapshot(t, 1),
		`INSERT INTO messages (tenant_id, n) VALUES (?, ?) ON CONFLICT DO NOTHING`,
		[]shardservice.Param{
			shardservice.StringParam("tenant-a"),
			shardservice.NumberParam("1"),
		},
		nil,
	)
	if bound == nil || len(bound.rowKeys) != 1 {
		t.Fatalf("non-indexed conflict skip bound plan = %#v", bound)
	}
}

func TestReplicatedSQLMutationInputCountAcceptsComputedConflictUpdate(t *testing.T) {
	parsed, err := sqlast.ParseStatement(
		`INSERT INTO messages (id, value) VALUES (?, ?) ON CONFLICT DO UPDATE SET value = messages.value || EXCLUDED.value`,
	)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Insert == nil || parsed.Insert.OnConflictUpdate == nil {
		t.Fatalf("parsed conflict action = %#v", parsed.Insert)
	}

	statement := replicatedSQLBoundStatement{
		prepared: &PreparedPlan{statement: *parsed},
		bound:    &BoundWritePlan{rowKeys: make([][]distribution.Scalar, 1), insertDoc: []byte(`{"id":"a","value":"b"}`)},
		profile:  ReplicatedTableProfile{Relation: 1},
	}
	statement.bound.rowKeys[0] = make([]distribution.Scalar, 1)
	count, err := replicatedSQLMutationInputCount(&statement)
	if err != nil || count != 1 {
		t.Fatalf("input count = %d, error = %v, want 1", count, err)
	}
}

func unsupportedPosition(err *sqlast.FeatureNotSupportedError) int {
	if err == nil {
		return -1
	}
	return err.Pos
}

func TestPostgreSQLRF3PreparesComputedUpdateAndKeepsReturningFenced(t *testing.T) {
	const text = `UPDATE messages SET value = value || '-next' WHERE id = ?`
	executor, _ := newSQLRF3TestExecutor(t)
	authority := serviceauthz.Authority{Generation: 1}
	authority.Node[0] = 1
	dispatches := 0
	var dispatched Query
	backend := &PostgreSQLBackend{
		Executor: executor,
		Authorize: func(pgwire.SessionIdentity) (serviceauthz.Authority, error) {
			return authority, nil
		},
		Write: func(_ context.Context, _ serviceauthz.Authority, query Query) (*Result, error) {
			dispatches++
			dispatched = query
			return &Result{Kind: shardservice.ResponseCompletion, RowsAffected: 1}, nil
		},
	}
	session, err := backend.NewSession(context.Background(), pgwire.SessionIdentity{})
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	prepared, err := session.Prepare(context.Background(), text)
	if err != nil || prepared == nil {
		t.Fatalf("Prepare computed UPDATE = %v, %v", prepared, err)
	}
	result, err := prepared.Exec(context.Background(), []any{"message-1"})
	if err != nil || result.RowsAffected != 1 {
		t.Fatalf("Exec computed UPDATE = %+v, %v", result, err)
	}
	if dispatches != 1 || dispatched.SQL != text || len(dispatched.Params) != 1 ||
		string(dispatched.Params[0].Bytes) != "message-1" {
		t.Fatalf("computed dispatches=%d query=%+v", dispatches, dispatched)
	}
	if err := prepared.Close(); err != nil {
		t.Fatal(err)
	}

	returning, err := session.Prepare(
		context.Background(), text+` RETURNING value`,
	)
	var unsupported *sqlast.FeatureNotSupportedError
	if !errors.As(err, &unsupported) || returning != nil {
		t.Fatalf("RETURNING prepare = %v, %T %v", returning, err, err)
	}
	if dispatches != 1 || len(session.(*postgresSession).statements) != 0 {
		t.Fatalf("RETURNING dispatches=%d retained=%d", dispatches, len(session.(*postgresSession).statements))
	}
}

func TestPostgreSQLRF3PreparesComputedConflictUpdate(t *testing.T) {
	config, endpoints, descriptor, profile := testReplicatedTableInput(t)
	snapshot, err := NewSnapshotWithReplicatedTableMetadata(config, endpoints, 5, nil, nil,
		[]ReplicatedShardDescriptor{descriptor}, []ReplicatedTableProfile{profile},
		[]ReplicatedTableDeclaration{{Table: "messages", CreateTable: `CREATE TABLE messages (id TEXT PRIMARY KEY,n INTEGER,city TEXT)`}})
	if err != nil {
		t.Fatal(err)
	}
	authority := serviceauthz.Authority{Generation: 1}
	authority.Node[0] = 1
	dispatches := 0
	backend := &PostgreSQLBackend{Executor: NewExecutor(nil, NewCatalogHolder(snapshot), Options{}),
		Authorize: func(pgwire.SessionIdentity) (serviceauthz.Authority, error) { return authority, nil },
		Write: func(context.Context, serviceauthz.Authority, Query) (*Result, error) {
			dispatches++
			return &Result{Kind: shardservice.ResponseCompletion, RowsAffected: 1}, nil
		},
	}
	session, err := backend.NewSession(t.Context(), pgwire.SessionIdentity{})
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	prepared, err := session.Prepare(t.Context(), `INSERT INTO messages (id,n) VALUES (?,1) ON CONFLICT DO UPDATE SET n=messages.n+?`)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	for i := 0; i < 2; i++ {
		result, err := prepared.Exec(t.Context(), []any{"a", int64(i + 1)})
		if err != nil || result.RowsAffected != 1 {
			t.Fatalf("result=%+v err=%v", result, err)
		}
	}
	if dispatches != 2 {
		t.Fatalf("dispatches=%d", dispatches)
	}
}
