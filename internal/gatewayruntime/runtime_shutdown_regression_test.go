package gatewayruntime

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestRuntimeDrainJoinsPostgreSQLWritersAndObservers(t *testing.T) {
	ready := make(chan struct{})
	runtime, err := Open(context.Background(), Config{
		CatalogPath: runtimeLifecycleCatalog(t), DevStaticCatalog: true, DevPlaintext: true,
		Listener: newBlockingRuntimeListener(),
		Logf: func(format string, _ ...any) {
			if strings.HasPrefix(format, "vibedb-gateway serving") {
				close(ready)
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	pg := newRuntimeBlockingCloser()
	writer := newRuntimeBlockingCloser()
	metricsDone := make(chan struct{})
	var metricsRelease sync.Once
	runtime.pg, runtime.pgWriter, runtime.metricsDone = pg, writer, metricsDone
	t.Cleanup(func() {
		pg.unblock()
		writer.unblock()
		metricsRelease.Do(func() { close(metricsDone) })
		_ = runtime.Close()
	})
	served := make(chan error, 1)
	go func() { served <- runtime.Serve(context.Background()) }()
	awaitRuntimeSignal(t, ready, "public listener readiness")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	err = runtime.Drain(ctx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Drain returned before PostgreSQL sessions joined: %v", err)
	}
	awaitRuntimeSignal(t, pg.entered, "PostgreSQL shutdown")
	select {
	case <-writer.entered:
		t.Fatal("writer journals closed before PostgreSQL sessions joined")
	default:
	}
	closed := make(chan error, 1)
	go func() { closed <- runtime.Close() }()
	pg.unblock()
	awaitRuntimeSignal(t, writer.entered, "writer recovery shutdown")
	assertRuntimeWaiting(t, closed, "writer recovery")
	writer.unblock()
	assertRuntimeWaiting(t, closed, "metrics observer")
	metricsRelease.Do(func() { close(metricsDone) })
	if err := awaitRuntimeError(t, closed, "Close"); err != nil {
		t.Fatal(err)
	}
	if err := awaitRuntimeError(t, served, "Serve"); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeServeFailuresCancelAndJoinOptionalServices(t *testing.T) {
	for _, startup := range []bool{false, true} {
		name := "listener"
		if startup {
			name = "optional-startup"
		}
		t.Run(name, func(t *testing.T) {
			listener := &runtimeFailingListener{blockingRuntimeListener: newBlockingRuntimeListener(),
				failure: errors.New("listener failed")}
			runtime, err := Open(context.Background(), Config{
				CatalogPath: runtimeLifecycleCatalog(t), DevStaticCatalog: true, DevPlaintext: true,
				Listener: listener, Logf: func(string, ...any) {},
			})
			if err != nil {
				t.Fatal(err)
			}
			// Simulate an already-started optional user that needs to settle an
			// in-flight transport call after observing cancellation.
			canceled, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var unblock sync.Once
			runtime.replicaControllersDone = done
			go func() {
				<-runtime.ctx.Done()
				close(canceled)
				<-release
				close(done)
			}()
			t.Cleanup(func() {
				unblock.Do(func() { close(release) })
				_ = runtime.Close()
			})
			if startup {
				// Force a failure after optional-service startup without needing
				// a whole RF3 cluster to exercise the lifecycle contract.
				runtime.config.PGListenAddress = "127.0.0.1:0"
			}
			served := make(chan error, 1)
			go func() { served <- runtime.Serve(context.Background()) }()
			awaitRuntimeSignal(t, canceled, "cancellation after serving failure")
			assertRuntimeWaiting(t, served, "optional controller")
			unblock.Do(func() { close(release) })
			err = awaitRuntimeError(t, served, "Serve")
			if startup {
				if err == nil || !strings.Contains(err.Error(), "requires durable write service") {
					t.Fatalf("lost startup failure: %v", err)
				}
			} else if !errors.Is(err, listener.failure) {
				t.Fatalf("lost listener failure: %v", err)
			}
		})
	}
}

func TestGatewayListenerFailureCancelsAcceptedConnectionBeforeJoining(t *testing.T) {
	conn := &runtimeCancellationConn{newBlockingRuntimeConn()}
	failure := errors.New("listener failed after admitting a connection")
	listener := &runtimeAdmittedFailureListener{blockingRuntimeListener: newBlockingRuntimeListener(),
		conn: conn, failure: failure}
	executor, _, err := newGateway(runtimeLifecycleCatalog(t))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	served := make(chan error, 1)
	go func() { served <- serveGateway(ctx, listener, executor, func(string, ...any) {}) }()
	awaitRuntimeSignal(t, conn.readStarted, "accepted connection read")
	if err := awaitRuntimeError(t, served, "listener failure drain"); !errors.Is(err, failure) {
		t.Fatalf("listener failure lost: %v", err)
	}
	select {
	case <-conn.closed:
	default:
		t.Fatal("Serve returned with an accepted connection still open")
	}
}

type runtimeCancellationConn struct{ *blockingRuntimeConn }

func (conn *runtimeCancellationConn) Read([]byte) (int, error) {
	conn.readStartOnce.Do(func() { close(conn.readStarted) })
	<-conn.closed
	return 0, net.ErrClosed
}

type runtimeAdmittedFailureListener struct {
	*blockingRuntimeListener
	conn     *runtimeCancellationConn
	admitted bool
	failure  error
}

func (listener *runtimeAdmittedFailureListener) Accept() (net.Conn, error) {
	if !listener.admitted {
		listener.admitted = true
		return listener.conn, nil
	}
	select {
	case <-listener.conn.readStarted:
		return nil, listener.failure
	case <-listener.closed:
		return nil, net.ErrClosed
	}
}

type runtimeFailingListener struct {
	*blockingRuntimeListener
	failure error
}

func (listener *runtimeFailingListener) Accept() (net.Conn, error) {
	return nil, listener.failure
}

type runtimeBlockingCloser struct {
	entered, release chan struct{}
	close, released  sync.Once
}

func newRuntimeBlockingCloser() *runtimeBlockingCloser {
	return &runtimeBlockingCloser{entered: make(chan struct{}), release: make(chan struct{})}
}

func (closer *runtimeBlockingCloser) Close() error {
	closer.close.Do(func() { close(closer.entered); <-closer.release })
	return nil
}

func (closer *runtimeBlockingCloser) unblock() {
	closer.released.Do(func() { close(closer.release) })
}

func awaitRuntimeSignal(t *testing.T, signal <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func awaitRuntimeError(t *testing.T, result <-chan error, what string) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		return nil
	}
}

func assertRuntimeWaiting(t *testing.T, result <-chan error, what string) {
	t.Helper()
	select {
	case err := <-result:
		t.Fatalf("shutdown returned before joining %s: %v", what, err)
	case <-time.After(20 * time.Millisecond):
	}
}
