package gatewayruntime

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/shardservice"
)

func TestRuntimeOpenRejectsConcurrentJournalOwnerAndReleasesFailedStartup(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var unblocked sync.Once
	transport := &runtimeRejectingTransport{entered: entered, release: release}
	config := runtimeReplicatedConfig(t, transport)
	first := make(chan error, 1)
	go func() {
		runtime, err := Open(context.Background(), config)
		if runtime != nil {
			_ = runtime.Close()
		}
		first <- err
	}()
	t.Cleanup(func() { unblocked.Do(func() { close(release) }) })
	awaitRuntimeSignal(t, entered, "catalog session recovery")
	duplicate, err := Open(context.Background(), config)
	if duplicate != nil {
		_ = duplicate.Close()
	}
	if !errors.Is(err, storeio.ErrWriterLocked) || transport.calls.Load() != 1 {
		t.Fatalf("duplicate Open reached live recovery state: calls=%d err=%v", transport.calls.Load(), err)
	}
	unblocked.Do(func() { close(release) })
	if err := awaitRuntimeError(t, first, "failed Open"); !errors.Is(err, gateway.ErrReplicatedUnauthorized) {
		t.Fatalf("lost recovery failure: %v", err)
	}
	before := transport.calls.Load()
	if _, err := Open(context.Background(), config); !errors.Is(err, gateway.ErrReplicatedUnauthorized) ||
		errors.Is(err, storeio.ErrWriterLocked) || transport.calls.Load() <= before {
		t.Fatalf("failed startup retained journal ownership: calls=%d err=%v", transport.calls.Load(), err)
	}
	if transport.closed.Load() != 0 {
		t.Fatal("Open failure closed the caller-owned semantic transport")
	}
}

func TestRuntimeJournalOwnershipLastsThroughDrainUntilClose(t *testing.T) {
	transport := &runtimeRejectingTransport{}
	config := runtimeReplicatedConfig(t, transport)
	runtime, err := Open(context.Background(), Config{
		CatalogPath: runtimeLifecycleCatalog(t), DevStaticCatalog: true, DevPlaintext: true,
		Listener: newBlockingRuntimeListener(), Logf: func(string, ...any) {},
	})
	if err != nil {
		t.Fatal(err)
	}
	// Exercise the same resource ownership acquired before replicated recovery,
	// without requiring a Raft quorum merely to test filesystem lock lifetime.
	runtime.journalLock, err = openGatewayJournalLock(config.CatalogSessionJournal)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Close() })
	if err := runtime.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(context.Background(), config); !errors.Is(err, storeio.ErrWriterLocked) {
		t.Fatalf("Drain released journal ownership before Close: %v", err)
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(context.Background(), config); !errors.Is(err, gateway.ErrReplicatedUnauthorized) ||
		errors.Is(err, storeio.ErrWriterLocked) || transport.calls.Load() == 0 {
		t.Fatalf("Close retained journal ownership: %v", err)
	}
	if transport.closed.Load() != 0 {
		t.Fatal("runtime closed borrowed transport")
	}
}

func TestGatewayJournalLockRejectsSymlink(t *testing.T) {
	base := filepath.Join(t.TempDir(), "session")
	target := filepath.Join(t.TempDir(), "other")
	if err := os.WriteFile(target, []byte("retained"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, base+".gateway.lock"); err != nil {
		t.Fatal(err)
	}
	if file, err := openGatewayJournalLock(base); !errors.Is(err, ErrInvalidConfig) || file != nil {
		t.Fatalf("symlink accepted as journal ownership: %v", err)
	}
	if contents, err := os.ReadFile(target); err != nil || string(contents) != "retained" {
		t.Fatalf("symlink target changed: %q %v", contents, err)
	}
}

func runtimeReplicatedConfig(t *testing.T, transport SemanticTransport) Config {
	t.Helper()
	root := t.TempDir()
	path := filepath.Join(root, "catalog")
	if err := gateway.SaveSnapshot(path, catalogRouteSeedSnapshot(t, 1, "127.0.0.1:7101")); err != nil {
		t.Fatal(err)
	}
	return Config{
		CatalogPath: path, CatalogRouteSeedPath: filepath.Join(root, "route"),
		CatalogSessionJournal: filepath.Join(root, "session"), CatalogRelation: 1,
		CatalogClientID: replication.ID128{1}, CatalogRetryHome: replication.RetryHome{2},
		CatalogAttempts: 1, AckKey: gateway.DurableRequestAckDerivationKey{3},
		DevPlaintext: true, Transport: transport,
		InternalAuthority: serviceauthz.Authority{Node: [16]byte{99}, Generation: 1},
		Logf:              func(string, ...any) {},
	}
}

type runtimeRejectingTransport struct {
	entered, release chan struct{}
	once             sync.Once
	calls, closed    atomic.Int32
}

func (transport *runtimeRejectingTransport) DoReplicated(ctx context.Context, _ gateway.ReplicatedEndpoint,
	_ *shardservice.ReplicatedRequest) (*shardservice.ReplicatedResponse, error) {
	transport.calls.Add(1)
	if transport.entered != nil {
		transport.once.Do(func() { close(transport.entered) })
	}
	if transport.release != nil {
		select {
		case <-transport.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return nil, gateway.ErrReplicatedUnauthorized
}

func (transport *runtimeRejectingTransport) Close() error {
	transport.closed.Add(1)
	return nil
}
