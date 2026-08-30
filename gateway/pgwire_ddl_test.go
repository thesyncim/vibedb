package gateway

import (
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/pgwire"
	sqlast "github.com/thesyncim/vibedb/sql"
	driver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibedb/store/durable"
)

func TestPostgreSQLDDLRejectsUniqueIndexBeforeStatementOrDispatch(t *testing.T) {
	executor, transport := newSQLRF3TestExecutor(t)
	authority := serviceauthz.Authority{Generation: 1}
	authority.Node[0] = 1
	dispatches := 0
	backend := &PostgreSQLBackend{
		Executor:  executor,
		Authorize: func(pgwire.SessionIdentity) (serviceauthz.Authority, error) { return authority, nil },
		DDL: func(context.Context, serviceauthz.Authority, string) error {
			dispatches++
			return nil
		},
	}
	session, err := backend.NewSession(context.Background(), pgwire.SessionIdentity{})
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	prepared, err := session.Prepare(
		context.Background(), `CREATE UNIQUE INDEX employees_email ON employees (email)`,
	)
	var unsupported *sqlast.FeatureNotSupportedError
	if !errors.As(err, &unsupported) {
		t.Fatalf("Prepare error = %T %v, want *sql.FeatureNotSupportedError", err, err)
	}
	if prepared != nil {
		t.Fatal("refused unique index returned a prepared statement")
	}
	if dispatches != 0 || transport.queries != 0 {
		t.Fatalf("refused unique index dispatched DDL=%d queries=%d", dispatches, transport.queries)
	}
	if len(session.(*postgresSession).statements) != 0 {
		t.Fatal("refused unique index leaked a session statement")
	}
}

func TestPostgreSQLDDLUsesCoordinatorOnlyAtExecution(t *testing.T) {
	executor, transport := newSQLRF3TestExecutor(t)
	authority := serviceauthz.Authority{Generation: 1}
	authority.Node[0] = 1
	var calls []string
	var failure error
	backend := &PostgreSQLBackend{Executor: executor,
		Authorize: func(pgwire.SessionIdentity) (serviceauthz.Authority, error) { return authority, nil },
		DDL: func(ctx context.Context, got serviceauthz.Authority, sql string) error {
			if got != authority || ctx.Err() != nil {
				t.Fatal("lost DDL authority/context")
			}
			calls = append(calls, sql)
			return failure
		},
	}
	ctx := context.Background()
	s, err := backend.NewSession(ctx, pgwire.SessionIdentity{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, text := range []string{
		`CREATE TABLE employees (id TEXT PRIMARY KEY, name TEXT NOT NULL, team TEXT NOT NULL, city TEXT, score INTEGER NOT NULL, active BOOLEAN NOT NULL)`,
		`CREATE INDEX employees_city ON employees (city)`,
		`DROP INDEX employees_city ON employees`,
		`TRUNCATE TABLE employees`,
		`DROP TABLE employees`,
	} {
		before := len(calls)
		p, err := s.Prepare(ctx, text)
		if err != nil {
			t.Fatalf("%s: %v", text, err)
		}
		if len(calls) != before || transport.queries != 0 {
			t.Fatal("prepare executed DDL")
		}
		if p.ReturnsRows() || p.NumParams() != 0 {
			t.Fatal("invalid DDL description")
		}
		if _, err := p.Exec(ctx, nil); err != nil {
			t.Fatal(err)
		}
		if len(calls) != before+1 || calls[before] != text {
			t.Fatal("DDL not dispatched exactly once")
		}
		failure = durable.ErrCommitOutcomeUnknown
		if _, err := p.Exec(ctx, nil); !errors.Is(err, failure) {
			t.Fatalf("lost uncertain outcome: %v", err)
		}
		failure = nil
		if err := s.Begin(ctx, driver.TxOptions{}); err != nil {
			t.Fatal(err)
		}
		before = len(calls)
		if _, err := p.Exec(ctx, nil); err == nil {
			t.Fatal("DDL inside transaction accepted")
		}
		if len(calls) != before {
			t.Fatal("transactional DDL dispatched")
		}
		if err := s.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
		if err := p.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := p.Exec(ctx, nil); !errors.Is(err, driver.ErrSessionClosed) {
			t.Fatal(err)
		}
	}
	if transport.queries != 0 {
		t.Fatal("DDL reached row query path")
	}
	if len(s.(*postgresSession).statements) != 0 {
		t.Fatal("DDL statement leaked")
	}
}
