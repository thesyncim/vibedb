//go:build linux

package gatewayruntime

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestWaitSeamlessScaleManifestGatewayWaitsForPostgresReady(t *testing.T) {
	nativeListener, nativeAccepted := startSeamlessScaleReadinessListener(t)
	postgresListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = postgresListener.Close() })
	manifest := writeSeamlessScaleReadinessManifest(t, nativeListener.Addr().String(), postgresListener.Addr().String())

	allowReady := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(allowReady) }) }
	stopServer := make(chan struct{})
	var stopOnce sync.Once
	stop := func() { stopOnce.Do(func() { close(stopServer) }) }
	firstProbeClosed := make(chan struct{})
	var firstProbeOnce sync.Once
	startupReceived := make(chan struct{})
	var startupOnce sync.Once
	serverDone := make(chan struct{})
	serverErr := make(chan error, 1)
	var probes atomic.Int32
	go func() {
		defer close(serverDone)
		for {
			connection, acceptErr := postgresListener.Accept()
			if acceptErr != nil {
				select {
				case <-stopServer:
					serverErr <- nil
				default:
					serverErr <- acceptErr
				}
				return
			}
			if err := readSeamlessScaleReadinessStartup(connection); err != nil {
				_ = connection.Close()
				select {
				case <-stopServer:
					serverErr <- nil
					return
				default:
					continue
				}
			}
			if probes.Add(1) == 1 {
				_ = connection.Close()
				firstProbeOnce.Do(func() { close(firstProbeClosed) })
				continue
			}
			startupOnce.Do(func() { close(startupReceived) })
			peerClosed := make(chan struct{})
			go func() {
				var one [1]byte
				_, _ = connection.Read(one[:])
				close(peerClosed)
			}()
			select {
			case <-peerClosed:
				_ = connection.Close()
				continue
			case <-stopServer:
				_ = connection.Close()
				<-peerClosed
				serverErr <- nil
				return
			case <-allowReady:
			}
			select {
			case <-peerClosed:
				_ = connection.Close()
				continue
			default:
			}
			if _, err := connection.Write(seamlessScaleReadinessReadyForQuery()); err != nil {
				_ = connection.Close()
				<-peerClosed
				continue
			}
			_ = connection.Close()
			<-peerClosed
		}
	}()
	t.Cleanup(func() {
		release()
		stop()
		_ = postgresListener.Close()
		select {
		case <-serverDone:
		case <-time.After(2 * time.Second):
			t.Error("controlled PostgreSQL readiness server did not stop")
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	readyDone := make(chan error, 1)
	readyExited := make(chan struct{})
	go func() {
		defer close(readyExited)
		readyDone <- waitSeamlessScaleManifestGateway(ctx, manifest)
	}()
	t.Cleanup(func() {
		select {
		case <-readyExited:
		case <-time.After(2 * time.Second):
			t.Error("gateway readiness waiter did not exit")
		}
	})

	select {
	case <-nativeAccepted:
	case <-ctx.Done():
		t.Fatal("native gateway probe did not connect")
	}
	select {
	case <-firstProbeClosed:
	case <-ctx.Done():
		t.Fatal("initial PostgreSQL probe was not closed to force a retry")
	}
	select {
	case <-startupReceived:
	case <-ctx.Done():
		t.Fatal("PostgreSQL startup handshake did not reach the held server")
	}
	select {
	case err := <-readyDone:
		t.Fatalf("readiness returned before PostgreSQL ReadyForQuery: %v", err)
	default:
	}

	release()
	select {
	case err := <-readyDone:
		if err != nil {
			t.Fatalf("wait for PostgreSQL readiness: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("readiness did not finish after ReadyForQuery: %v", ctx.Err())
	}
	if got := probes.Load(); got < 2 {
		t.Fatalf("PostgreSQL startup probes=%d, want initial EOF and retried handshake", got)
	}
	stop()
	_ = postgresListener.Close()
	select {
	case <-serverDone:
		if err := <-serverErr; err != nil {
			t.Fatalf("controlled PostgreSQL server: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("controlled PostgreSQL server did not finish: %v", ctx.Err())
	}
}

func TestWaitSeamlessScaleManifestGatewayNativeOnly(t *testing.T) {
	nativeListener, _ := startSeamlessScaleReadinessListener(t)
	manifest := writeSeamlessScaleReadinessManifest(t, nativeListener.Addr().String(), "")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := waitSeamlessScaleManifestGateway(ctx, manifest); err != nil {
		t.Fatalf("native-only readiness: %v", err)
	}
}

func TestWaitSeamlessScaleManifestGatewayCancellationClosesPostgresProbe(t *testing.T) {
	nativeListener, _ := startSeamlessScaleReadinessListener(t)
	postgresListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	manifest := writeSeamlessScaleReadinessManifest(t, nativeListener.Addr().String(), postgresListener.Addr().String())

	startupReceived := make(chan struct{})
	peerClosed := make(chan error, 1)
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		connection, acceptErr := postgresListener.Accept()
		if acceptErr != nil {
			peerClosed <- acceptErr
			return
		}
		defer connection.Close()
		if err := readSeamlessScaleReadinessStartup(connection); err != nil {
			peerClosed <- err
			return
		}
		close(startupReceived)
		var one [1]byte
		_, readErr := connection.Read(one[:])
		peerClosed <- readErr
	}()
	t.Cleanup(func() {
		_ = postgresListener.Close()
		select {
		case <-serverDone:
		case <-time.After(2 * time.Second):
			t.Error("canceled PostgreSQL readiness server did not stop")
		}
	})

	cause := errors.New("test canceled PG readiness")
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	readyDone := make(chan error, 1)
	readyExited := make(chan struct{})
	go func() {
		defer close(readyExited)
		readyDone <- waitSeamlessScaleManifestGateway(ctx, manifest)
	}()
	t.Cleanup(func() {
		select {
		case <-readyExited:
		case <-time.After(2 * time.Second):
			t.Error("canceled gateway readiness waiter did not exit")
		}
	})
	select {
	case <-startupReceived:
	case <-time.After(2 * time.Second):
		t.Fatal("PostgreSQL startup handshake did not reach the held server")
	}
	cancel(cause)
	select {
	case err := <-readyDone:
		if !errors.Is(err, cause) {
			t.Fatalf("readiness error=%v, want cancellation cause %v", err, cause)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("readiness did not return after cancellation")
	}
	select {
	case err := <-peerClosed:
		if err == nil {
			t.Fatal("PostgreSQL probe connection remained open after cancellation")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("PostgreSQL server did not observe probe socket closure")
	}
	select {
	case <-serverDone:
	case <-time.After(2 * time.Second):
		t.Fatal("canceled PostgreSQL readiness server did not exit")
	}
	select {
	case <-readyExited:
	case <-time.After(2 * time.Second):
		t.Fatal("canceled gateway readiness waiter did not exit")
	}
}

func TestWaitSeamlessScaleManifestGatewayReturnsPostgresStartupFailure(t *testing.T) {
	nativeListener, _ := startSeamlessScaleReadinessListener(t)
	postgresListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	manifest := writeSeamlessScaleReadinessManifest(t, nativeListener.Addr().String(), postgresListener.Addr().String())

	var accepted atomic.Int32
	serverDone := make(chan error, 1)
	serverExited := make(chan struct{})
	go func() {
		defer close(serverExited)
		connection, acceptErr := postgresListener.Accept()
		if acceptErr != nil {
			serverDone <- acceptErr
			return
		}
		defer connection.Close()
		accepted.Add(1)
		if err := readSeamlessScaleReadinessStartup(connection); err != nil {
			serverDone <- err
			return
		}
		_, writeErr := connection.Write(seamlessScaleReadinessErrorResponse("XX000", "startup rejected"))
		serverDone <- writeErr
	}()
	t.Cleanup(func() {
		_ = postgresListener.Close()
		select {
		case <-serverExited:
		case <-time.After(2 * time.Second):
			t.Error("failed PostgreSQL readiness server did not stop")
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err = waitSeamlessScaleManifestGateway(ctx, manifest)
	if err == nil || !strings.Contains(err.Error(), "startup rejected") || !strings.Contains(err.Error(), postgresListener.Addr().String()) {
		t.Fatalf("persistent PostgreSQL startup failure=%v", err)
	}
	if got := accepted.Load(); got != 1 {
		t.Fatalf("PostgreSQL readiness attempts=%d, want one terminal protocol failure", got)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("startup error was hidden by readiness timeout: %v", err)
	}
	select {
	case serverErr := <-serverDone:
		if serverErr != nil {
			t.Fatalf("controlled PostgreSQL server: %v", serverErr)
		}
	case <-ctx.Done():
		t.Fatalf("controlled PostgreSQL server did not finish: %v", ctx.Err())
	}
	select {
	case <-serverExited:
	case <-ctx.Done():
		t.Fatalf("failed PostgreSQL readiness server did not exit: %v", ctx.Err())
	}
}

func startSeamlessScaleReadinessListener(t *testing.T) (net.Listener, <-chan struct{}) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	accepted := make(chan struct{}, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			connection, err := listener.Accept()
			if err != nil {
				return
			}
			select {
			case accepted <- struct{}{}:
			default:
			}
			_ = connection.Close()
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("native readiness listener did not stop")
		}
	})
	return listener, accepted
}

func writeSeamlessScaleReadinessManifest(t *testing.T, gatewayAddress, postgresAddress string) string {
	t.Helper()
	manifest := struct {
		Gateway struct {
			Listen   string `json:"listen"`
			PGListen string `json:"pg_listen,omitempty"`
		} `json:"gateway"`
	}{}
	manifest.Gateway.Listen = gatewayAddress
	manifest.Gateway.PGListen = postgresAddress
	raw, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "serve-node.vibejson")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func readSeamlessScaleReadinessStartup(connection net.Conn) error {
	var header [4]byte
	if _, err := io.ReadFull(connection, header[:]); err != nil {
		return err
	}
	length := int(binary.BigEndian.Uint32(header[:]))
	if length < 8 || length > 1<<20 {
		return fmt.Errorf("invalid PostgreSQL startup packet length %d", length)
	}
	_, err := io.CopyN(io.Discard, connection, int64(length-4))
	return err
}

func seamlessScaleReadinessReadyForQuery() []byte {
	return []byte{'Z', 0, 0, 0, 5, 'I'}
}

func seamlessScaleReadinessErrorResponse(code, message string) []byte {
	payload := []byte{'S'}
	payload = append(payload, "FATAL\x00"...)
	payload = append(payload, 'C')
	payload = append(payload, code...)
	payload = append(payload, 0, 'M')
	payload = append(payload, message...)
	payload = append(payload, 0, 0)
	response := []byte{'E'}
	response = binary.BigEndian.AppendUint32(response, uint32(len(payload)+4))
	return append(response, payload...)
}
