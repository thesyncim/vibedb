package gateway

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/pgwire"
	"github.com/thesyncim/vibedb/shardservice"
	sqlast "github.com/thesyncim/vibedb/sql"
)

func TestSnapshotPrepareRejectsConflictUpdateBeforeBind(t *testing.T) {
	const text = `INSERT INTO messages (tenant_id, n) VALUES (?, ?) ON CONFLICT DO UPDATE SET n = EXCLUDED.n`

	prepared, err := testSnapshot(t, 1).Prepare(context.Background(), text)
	if !errors.Is(err, ErrDistributedWriteUnsupported) {
		t.Fatalf("Prepare error = %v, want ErrDistributedWriteUnsupported", err)
	}
	if prepared != nil {
		t.Fatal("unsupported conflict update returned a bindable plan")
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

func TestReplicatedSQLMutationInputCountRejectsConflictUpdate(t *testing.T) {
	parsed, err := sqlast.ParseStatement(
		`INSERT INTO messages (id, value) VALUES (?, ?) ON CONFLICT DO UPDATE SET value = EXCLUDED.value`,
	)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Insert == nil || parsed.Insert.OnConflictUpdate == nil {
		t.Fatalf("parsed conflict action = %#v", parsed.Insert)
	}

	statement := replicatedSQLBoundStatement{
		prepared: &PreparedPlan{statement: *parsed},
		bound:    &BoundWritePlan{},
		profile:  ReplicatedTableProfile{Relation: 1},
	}
	count, err := replicatedSQLMutationInputCount(&statement)
	if !errors.Is(err, ErrReplicatedSQLTransactionUnsupported) {
		t.Fatalf("input count = %d, error = %v, want ErrReplicatedSQLTransactionUnsupported", count, err)
	}
}

func TestPostgreSQLRF3PrepareRejectsConflictActionsAsFeatureNotSupported(t *testing.T) {
	executor, _ := newSQLRF3TestExecutor(t)
	authority := serviceauthz.Authority{Generation: 1}
	authority.Node[0] = 1
	dispatches := 0
	backend := &PostgreSQLBackend{
		Executor: executor,
		Authorize: func(pgwire.SessionIdentity) (serviceauthz.Authority, error) {
			return authority, nil
		},
		Write: func(context.Context, serviceauthz.Authority, Query) (*Result, error) {
			dispatches++
			return nil, nil
		},
	}
	session, err := backend.NewSession(context.Background(), pgwire.SessionIdentity{})
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	for _, test := range []struct {
		text   string
		marker string
	}{
		{
			text:   `INSERT INTO messages (id, value) VALUES (?, ?) ON CONFLICT DO NOTHING`,
			marker: "ON CONFLICT",
		},
		{
			text:   `INSERT INTO messages (id, value) VALUES (?, ?) ON CONFLICT DO UPDATE SET value = EXCLUDED.value`,
			marker: "UPDATE",
		},
	} {
		prepared, err := session.Prepare(context.Background(), test.text)
		var unsupported *sqlast.FeatureNotSupportedError
		wantPosition := strings.Index(test.text, test.marker)
		if !errors.As(err, &unsupported) || unsupported.Pos != wantPosition {
			t.Fatalf(
				"Prepare(%q) error = %T %v at %d, want *sql.FeatureNotSupportedError at %d",
				test.text, err, err, unsupportedPosition(unsupported), wantPosition,
			)
		}
		if prepared != nil {
			t.Fatalf("RF3 conflict action returned a prepared statement for %q", test.text)
		}
	}
	if dispatches != 0 {
		t.Fatalf("write dispatches = %d, want 0", dispatches)
	}
	if len(session.(*postgresSession).statements) != 0 {
		t.Fatal("RF3 session retained a refused conflict statement")
	}
}

func unsupportedPosition(err *sqlast.FeatureNotSupportedError) int {
	if err == nil {
		return -1
	}
	return err.Pos
}

func TestPostgreSQLRF3PrepareRejectsComputedUpdateBeforeRetention(t *testing.T) {
	const text = `UPDATE messages SET value = value || '-next' WHERE id = ?`
	wantPosition := strings.Index(text, `||`)
	executor, _ := newSQLRF3TestExecutor(t)
	authority := serviceauthz.Authority{Generation: 1}
	authority.Node[0] = 1
	dispatches := 0
	backend := &PostgreSQLBackend{
		Executor: executor,
		Authorize: func(pgwire.SessionIdentity) (serviceauthz.Authority, error) {
			return authority, nil
		},
		Write: func(context.Context, serviceauthz.Authority, Query) (*Result, error) {
			dispatches++
			return nil, nil
		},
	}
	session, err := backend.NewSession(context.Background(), pgwire.SessionIdentity{})
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	prepared, err := session.Prepare(context.Background(), text)
	var unsupported *sqlast.FeatureNotSupportedError
	if !errors.As(err, &unsupported) || unsupported.Pos != wantPosition {
		t.Fatalf(
			"Prepare error = %T %v, want positioned FeatureNotSupported at %d",
			err, err, wantPosition,
		)
	}
	if prepared != nil {
		t.Fatal("RF3 computed UPDATE returned a prepared statement")
	}
	if dispatches != 0 {
		t.Fatalf("write dispatches = %d, want 0", dispatches)
	}
	if len(session.(*postgresSession).statements) != 0 {
		t.Fatal("RF3 session retained a refused computed UPDATE")
	}
}
