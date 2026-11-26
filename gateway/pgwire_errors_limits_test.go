package gateway

import (
	"context"
	"errors"
	"testing"

	sqlast "github.com/thesyncim/vibedb/sql"
	"github.com/thesyncim/vibedb/sql/driver"
)

func TestPostgresSessionTypedErrorsAndUnlimitedLimits(t *testing.T) {
	session := &postgresSession{state: driver.SessionIdle}

	if err := session.SetResultLimits(-1, -1); err != nil {
		t.Fatalf("SetResultLimits(-1, -1) returned error: %v", err)
	}
	if err := session.SetIntermediateLimit(-1); err != nil {
		t.Fatalf("SetIntermediateLimit(-1) returned error: %v", err)
	}
	if err := session.SetResultLimits(0, 1024); !errors.Is(err, ErrResultLimit) {
		t.Fatalf("SetResultLimits(0, 1024) = %v, want ErrResultLimit", err)
	}
	if err := session.SetResultLimits(-2, 1024); !errors.Is(err, ErrResultLimit) {
		t.Fatalf("SetResultLimits(-2, 1024) = %v, want ErrResultLimit", err)
	}
	if err := session.SetIntermediateLimit(0); !errors.Is(err, ErrResultLimit) {
		t.Fatalf("SetIntermediateLimit(0) = %v, want ErrResultLimit", err)
	}

	if err := session.Begin(context.Background(), driver.TxOptions{Isolation: driver.IsolationSerializable}); !errors.Is(err, driver.ErrUnsupportedIsolation) {
		t.Fatalf("Begin(SERIALIZABLE) = %v, want ErrUnsupportedIsolation", err)
	}
	if err := session.Begin(context.Background(), driver.TxOptions{Isolation: driver.IsolationReadCommitted}); err != nil {
		t.Fatalf("Begin(READ COMMITTED) failed: %v", err)
	}
	if err := session.Begin(context.Background(), driver.TxOptions{}); !errors.Is(err, driver.ErrTransactionActive) {
		t.Fatalf("nested Begin = %v, want ErrTransactionActive", err)
	}

	if err := session.Savepoint(context.Background(), "sp1"); err != nil {
		t.Fatalf("Savepoint in active tx failed: %v", err)
	}
	if err := session.RollbackTo(context.Background(), "sp1"); err != nil {
		t.Fatalf("RollbackTo in active tx failed: %v", err)
	}
	if err := session.ReleaseSavepoint(context.Background(), "sp1"); err != nil {
		t.Fatalf("ReleaseSavepoint in active tx failed: %v", err)
	}
	if err := session.Rollback(context.Background()); err != nil {
		t.Fatalf("Rollback failed: %v", err)
	}
	if err := session.Savepoint(context.Background(), "sp_idle"); !errors.Is(err, driver.ErrNoTransaction) {
		t.Fatalf("Savepoint when idle = %v, want ErrNoTransaction", err)
	}
	_ = sqlast.KindSelect
}
