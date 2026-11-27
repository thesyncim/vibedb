package gateway

import (
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/resourceguard"
)

type stubResourceGuard struct {
	readErr  error
	writeErr error
}

func (g stubResourceGuard) AdmitRead() error  { return g.readErr }
func (g stubResourceGuard) AdmitWrite() error { return g.writeErr }

func TestExecutorEnforcesResourceGuardWatermarks(t *testing.T) {
	exec := NewExecutor(nil, nil, Options{
		ResourceGuard: stubResourceGuard{
			readErr:  resourceguard.ErrMemoryWatermarkExceeded,
			writeErr: resourceguard.ErrDiskWatermarkExceeded,
		},
	})
	if _, err := exec.Query(context.Background(), Query{SQL: "SELECT 1"}); !errors.Is(err, resourceguard.ErrMemoryWatermarkExceeded) {
		t.Fatalf("Query err = %v, want ErrMemoryWatermarkExceeded", err)
	}
	if _, err := exec.Exec(context.Background(), Query{SQL: "INSERT INTO t VALUES (1)"}); !errors.Is(err, resourceguard.ErrDiskWatermarkExceeded) {
		t.Fatalf("Exec err = %v, want ErrDiskWatermarkExceeded", err)
	}
}
