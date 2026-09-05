package gatewayruntime

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftservice"
)

type bootstrapCatalogReadFunc func(context.Context) (*gateway.Snapshot, error)

func (read bootstrapCatalogReadFunc) Read(ctx context.Context) (*gateway.Snapshot, error) {
	return read(ctx)
}

func TestCatalogBootstrapStartupWaitsForDesignatedPublisher(t *testing.T) {
	current := testReplicaHealthSnapshot(t)
	var published atomic.Bool
	var reads atomic.Int32
	missing := make(chan struct{})
	reader := bootstrapCatalogReadFunc(func(ctx context.Context) (*gateway.Snapshot, error) {
		if deadline, ok := ctx.Deadline(); !ok || time.Until(deadline) > time.Second {
			t.Error("discovery read lost startup bound")
		}
		if reads.Add(1) == 1 {
			close(missing)
			return nil, fmt.Errorf("head discovery: %w", gateway.ErrReplicatedCatalogMissing)
		}
		if !published.Load() {
			return nil, gateway.ErrReplicatedCatalogMissing
		}
		return current, nil
	})
	done := make(chan error, 1)
	go func() {
		snapshot, err := waitReplicatedCatalogBootstrap(t.Context(), reader, 2, 500*time.Millisecond)
		if err == nil && snapshot != current {
			err = errors.New("bootstrap discovery changed snapshot")
		}
		done <- err
	}()
	awaitRuntimeSignal(t, missing, "initial missing catalog discovery")
	published.Store(true)
	if err := awaitRuntimeError(t, done, "designated catalog publication"); err != nil {
		t.Fatal(err)
	}
	if reads.Load() < 2 {
		t.Fatalf("discovery made %d reads, want initial and published head", reads.Load())
	}
}

func TestCatalogBootstrapStartupCancellationAndBound(t *testing.T) {
	for _, cancelParent := range []bool{false, true} {
		name := "budget"
		if cancelParent {
			name = "parent-cancellation"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var reads atomic.Int32
			reader := bootstrapCatalogReadFunc(func(context.Context) (*gateway.Snapshot, error) {
				reads.Add(1)
				if cancelParent {
					cancel()
				}
				return nil, gateway.ErrReplicatedCatalogMissing
			})
			_, err := waitReplicatedCatalogBootstrap(ctx, reader, 1, 20*time.Millisecond)
			cause := context.DeadlineExceeded
			if cancelParent {
				cause = context.Canceled
			}
			if !errors.Is(err, cause) || !errors.Is(err, gateway.ErrReplicatedCatalogMissing) || reads.Load() != 1 {
				t.Fatalf("discovery err=%v reads=%d", err, reads.Load())
			}
		})
	}
	t.Run("canceled-in-flight-read", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		entered := make(chan struct{})
		reader := bootstrapCatalogReadFunc(func(ctx context.Context) (*gateway.Snapshot, error) {
			close(entered)
			<-ctx.Done()
			return nil, ctx.Err()
		})
		done := make(chan error, 1)
		go func() { _, err := waitReplicatedCatalogBootstrap(ctx, reader, 1, time.Second); done <- err }()
		awaitRuntimeSignal(t, entered, "discovery read")
		cancel()
		if err := awaitRuntimeError(t, done, "discovery cancellation"); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	})
}

func TestCatalogBootstrapStartupDoesNotRetryTerminalFailures(t *testing.T) {
	for _, failure := range []error{gateway.ErrReplicatedCatalog, gateway.ErrReplicatedCatalogConflict,
		gateway.ErrReplicatedUnauthorized, raftservice.ErrOutcomeUnknown,
		errors.Join(gateway.ErrReplicatedCatalogMissing, gateway.ErrReplicatedUnauthorized),
		errors.Join(gateway.ErrReplicatedCatalogMissing, gateway.ErrReplicatedCatalogConflict),
		errors.Join(gateway.ErrReplicatedCatalogMissing, raftservice.ErrOutcomeUnknown),
	} {
		var reads int
		reader := bootstrapCatalogReadFunc(func(context.Context) (*gateway.Snapshot, error) { reads++; return nil, failure })
		_, err := waitReplicatedCatalogBootstrap(t.Context(), reader, 2, time.Second)
		if err != failure || reads != 1 {
			t.Fatalf("terminal failure was retried or replaced: %v reads=%d", err, reads)
		}
	}
	reader := bootstrapCatalogReadFunc(func(context.Context) (*gateway.Snapshot, error) { return nil, nil })
	if _, err := waitReplicatedCatalogBootstrap(t.Context(), reader, 1, time.Second); !errors.Is(err, gateway.ErrReplicatedCatalog) {
		t.Fatalf("nil successful catalog accepted: %v", err)
	}
}
