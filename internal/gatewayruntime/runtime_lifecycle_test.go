package gatewayruntime

import (
	"context"
	"errors"
	"io"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
)

func TestRuntimeDrainBeforeServeIsTerminal(t *testing.T) {
	catalogPath := runtimeLifecycleCatalog(t)
	runtime, err := Open(context.Background(), Config{
		CatalogPath:      catalogPath,
		DevStaticCatalog: true,
		DevPlaintext:     true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if runtime.Listener() == nil {
		t.Fatal("Open returned a nil listener")
	}
	if err := runtime.Drain(context.Background()); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if err := runtime.Close(); err != nil {
		t.Fatalf("Close after Drain: %v", err)
	}
	if err := runtime.Serve(context.Background()); !errors.Is(err, ErrAlreadyServed) {
		t.Fatalf("Serve after Drain = %v, want %v", err, ErrAlreadyServed)
	}
}

func TestRuntimeCloseWaitsAfterTimedDrain(t *testing.T) {
	listener := newBlockingRuntimeListener()
	runtime, err := Open(context.Background(), Config{
		CatalogPath:      runtimeLifecycleCatalog(t),
		DevStaticCatalog: true,
		DevPlaintext:     true,
		Listener:         listener,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	conn := newBlockingRuntimeConn()
	closeConn := func() { conn.releaseOnce.Do(func() { close(conn.release) }) }
	t.Cleanup(func() {
		closeConn()
		_ = runtime.Close()
	})
	served := make(chan error, 1)
	go func() { served <- runtime.Serve(context.Background()) }()
	listener.accept(conn)
	select {
	case <-conn.readStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("gateway did not begin reading the blocked request")
	}

	drainCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	drainErr := runtime.Drain(drainCtx)
	cancel()
	if !errors.Is(drainErr, context.DeadlineExceeded) {
		t.Fatalf("timed Drain = %v, want deadline exceeded", drainErr)
	}

	closed := make(chan error, 1)
	go func() { closed <- runtime.Close() }()
	select {
	case err := <-closed:
		t.Fatalf("Close returned while request was blocked: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	closeConn()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close after request release: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not wait for the blocked request to drain")
	}
	select {
	case err := <-served:
		if err != nil {
			t.Fatalf("Serve: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not finish after Close")
	}
}

type blockingRuntimeListener struct {
	addr     net.Addr
	accepted chan net.Conn
	closed   chan struct{}
	close    sync.Once
}

func newBlockingRuntimeListener() *blockingRuntimeListener {
	return &blockingRuntimeListener{
		addr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 1}, accepted: make(chan net.Conn), closed: make(chan struct{}),
	}
}

func (listener *blockingRuntimeListener) Accept() (net.Conn, error) {
	select {
	case conn := <-listener.accepted:
		return conn, nil
	case <-listener.closed:
		return nil, net.ErrClosed
	}
}

func (listener *blockingRuntimeListener) Close() error {
	listener.close.Do(func() { close(listener.closed) })
	return nil
}

func (listener *blockingRuntimeListener) Addr() net.Addr { return listener.addr }

func (listener *blockingRuntimeListener) accept(conn net.Conn) { listener.accepted <- conn }

type blockingRuntimeConn struct {
	release       chan struct{}
	releaseOnce   sync.Once
	readStarted   chan struct{}
	readStartOnce sync.Once
	closed        chan struct{}
	closeOnce     sync.Once
}

func newBlockingRuntimeConn() *blockingRuntimeConn {
	return &blockingRuntimeConn{release: make(chan struct{}), readStarted: make(chan struct{}), closed: make(chan struct{})}
}

func (conn *blockingRuntimeConn) Read([]byte) (int, error) {
	conn.readStartOnce.Do(func() { close(conn.readStarted) })
	<-conn.release
	return 0, io.EOF
}

func (conn *blockingRuntimeConn) Write(data []byte) (int, error) { return len(data), nil }

func (conn *blockingRuntimeConn) Close() error {
	conn.closeOnce.Do(func() { close(conn.closed) })
	return nil
}

func (conn *blockingRuntimeConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (conn *blockingRuntimeConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (conn *blockingRuntimeConn) SetDeadline(time.Time) error      { return nil }
func (conn *blockingRuntimeConn) SetReadDeadline(time.Time) error  { return nil }
func (conn *blockingRuntimeConn) SetWriteDeadline(time.Time) error { return nil }

func runtimeLifecycleCatalog(t *testing.T) string {
	t.Helper()
	manifest, err := distribution.NewManifest("tenant_data", 1, []distribution.Shard{{
		ID: "all", AllocationGeneration: 1,
		Range: keyRange(0, true, 0), Leaders: []distribution.EndpointID{"node-a"},
	}})
	if err != nil {
		t.Fatalf("NewManifest: %v", err)
	}
	snapshot, err := gateway.NewSnapshot(distribution.ClusterConfig{
		Distributions: []distribution.DistributionSpec{{Name: "tenant_data", Arity: 1, MapperVersion: 1}},
		Placements:    []distribution.TablePlacement{{Table: "messages", Distribution: "tenant_data", Columns: []string{"/tenant_id"}}},
		Manifests:     []*distribution.Manifest{manifest},
	}, map[distribution.EndpointID]string{"node-a": "127.0.0.1:1"}, 1)
	if err != nil {
		t.Fatalf("NewSnapshot: %v", err)
	}
	path := filepath.Join(t.TempDir(), "catalog.json")
	if err := gateway.SaveSnapshot(path, snapshot); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	return path
}
