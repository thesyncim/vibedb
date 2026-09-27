package gatewayruntime

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/pgwire"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

const scaleShutdownTestSQLStateTooManyConnections = "53300"

type scaleShutdownReadGate struct {
	readLimit int
	readBytes int
	mu        sync.Mutex
	entered   chan struct{}
	release   chan struct{}
	enterOnce sync.Once
	relOnce   sync.Once
}

func newScaleShutdownReadGate(readLimit int) *scaleShutdownReadGate {
	return &scaleShutdownReadGate{readLimit: readLimit, entered: make(chan struct{}), release: make(chan struct{})}
}

func (gate *scaleShutdownReadGate) releaseReads() {
	gate.relOnce.Do(func() { close(gate.release) })
}

type scaleShutdownGatedConn struct {
	net.Conn
	gate *scaleShutdownReadGate
}

func (connection *scaleShutdownGatedConn) Read(buffer []byte) (int, error) {
	gate := connection.gate
	gate.mu.Lock()
	block := gate.readBytes >= gate.readLimit
	gate.mu.Unlock()
	if block {
		gate.enterOnce.Do(func() { close(gate.entered) })
		<-gate.release
	}
	count, err := connection.Conn.Read(buffer)
	if count > 0 {
		gate.mu.Lock()
		gate.readBytes += count
		gate.mu.Unlock()
	}
	return count, err
}

type scaleShutdownGatedListener struct {
	net.Listener
	gate    *scaleShutdownReadGate
	wrapped bool
}

func (listener *scaleShutdownGatedListener) Accept() (net.Conn, error) {
	connection, err := listener.Listener.Accept()
	if err != nil {
		return nil, err
	}
	if !listener.wrapped {
		listener.wrapped = true
		return &scaleShutdownGatedConn{Conn: connection, gate: listener.gate}, nil
	}
	return connection, nil
}

type scaleShutdownReadObservedConn struct {
	net.Conn
	started chan struct{}
	once    sync.Once
}

type scaleShutdownIgnoreDeadlineConn struct {
	net.Conn
}

func (connection scaleShutdownIgnoreDeadlineConn) SetDeadline(time.Time) error { return nil }

type scaleShutdownCancelDeadlineConn struct {
	net.Conn
	cancel       context.CancelCauseFunc
	cause        error
	cancelOnce   sync.Once
	mu           sync.Mutex
	writtenBytes int
}

func (connection *scaleShutdownCancelDeadlineConn) SetDeadline(deadline time.Time) error {
	if err := connection.Conn.SetDeadline(deadline); err != nil {
		return err
	}
	connection.cancelOnce.Do(func() { connection.cancel(connection.cause) })
	return nil
}

func (connection *scaleShutdownCancelDeadlineConn) Write(payload []byte) (int, error) {
	connection.mu.Lock()
	connection.writtenBytes += len(payload)
	connection.mu.Unlock()
	return connection.Conn.Write(payload)
}

func (connection *scaleShutdownCancelDeadlineConn) writeCount() int {
	connection.mu.Lock()
	defer connection.mu.Unlock()
	return connection.writtenBytes
}

type scaleShutdownCancelOnEOFConn struct {
	net.Conn
	cancel context.CancelCauseFunc
	cause  error
	once   sync.Once
}

func (connection *scaleShutdownCancelOnEOFConn) Read(buffer []byte) (int, error) {
	count, err := connection.Conn.Read(buffer)
	if count == 0 && errors.Is(err, io.EOF) {
		connection.once.Do(func() { connection.cancel(connection.cause) })
	}
	return count, err
}

func (connection *scaleShutdownReadObservedConn) Read(buffer []byte) (int, error) {
	connection.once.Do(func() { close(connection.started) })
	return connection.Conn.Read(buffer)
}

func startScaleShutdownTestServer(t *testing.T) (string, *scaleShutdownReadGate, func()) {
	t.Helper()
	database, err := sqldriver.Open(filepath.Join(t.TempDir(), "catalog"))
	if err != nil {
		t.Fatal(err)
	}
	server, err := pgwire.NewServer(database, pgwire.Options{
		Auth: pgwire.Trust(), Database: "vibedb", MaxConnections: 1,
		ReadTimeout: 5 * time.Second, IdleTimeout: 5 * time.Second,
	})
	if err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = server.Close()
		_ = database.Close()
		t.Fatal(err)
	}
	const startupPacketBytes = 36
	gate := newScaleShutdownReadGate(startupPacketBytes)
	wrapped := &scaleShutdownGatedListener{Listener: listener, gate: gate}
	served := make(chan error, 1)
	go func() { served <- server.Serve(wrapped) }()
	cleanup := func() {
		gate.releaseReads()
		_ = server.Close()
		select {
		case <-served:
		case <-time.After(3 * time.Second):
			t.Error("PostgreSQL test server did not stop")
		}
		_ = database.Close()
	}
	return listener.Addr().String(), gate, cleanup
}

func dialScaleShutdownTestSQL(ctx context.Context, address string) (net.Conn, ddlWireResult, error) {
	connection, err := (&net.Dialer{}).DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, ddlWireResult{}, err
	}
	if deadline, ok := ctx.Deadline(); ok {
		if err := connection.SetDeadline(deadline); err != nil {
			_ = connection.Close()
			return nil, ddlWireResult{}, err
		}
	}
	packet := binary.BigEndian.AppendUint32(nil, 0)
	packet = binary.BigEndian.AppendUint32(packet, 196608)
	packet = append(packet, []byte("user\x00local\x00database\x00vibedb\x00\x00")...)
	binary.BigEndian.PutUint32(packet[:4], uint32(len(packet)))
	for len(packet) != 0 {
		written, writeErr := connection.Write(packet)
		if writeErr != nil {
			_ = connection.Close()
			return nil, ddlWireResult{}, writeErr
		}
		if written <= 0 {
			_ = connection.Close()
			return nil, ddlWireResult{}, io.ErrShortWrite
		}
		packet = packet[written:]
	}
	result, err := readScaleShutdownStartupResult(connection)
	if err != nil {
		_ = connection.Close()
		return nil, result, err
	}
	return connection, result, nil
}

func readScaleShutdownStartupResult(connection net.Conn) (ddlWireResult, error) {
	var result ddlWireResult
	for {
		var header [5]byte
		if _, err := io.ReadFull(connection, header[:]); err != nil {
			if result.code != "" && (errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)) {
				return result, nil
			}
			return result, err
		}
		size := int(binary.BigEndian.Uint32(header[1:])) - 4
		if size < 0 || size > 1<<20 {
			return result, fmt.Errorf("invalid PostgreSQL startup frame length %d", size)
		}
		payload := make([]byte, size)
		if _, err := io.ReadFull(connection, payload); err != nil {
			return result, err
		}
		switch header[0] {
		case 'E':
			for len(payload) > 1 {
				end := 0
				for end+1 < len(payload) && payload[end+1] != 0 {
					end++
				}
				if end+1 >= len(payload) {
					return result, errors.New("invalid PostgreSQL ErrorResponse")
				}
				if payload[0] == 'C' {
					result.code = string(payload[1 : end+1])
				}
				if payload[0] == 'M' {
					result.message = string(payload[1 : end+1])
				}
				payload = payload[end+2:]
			}
		case 'C':
			result.tag = string(payload)
		case 'Z':
			return result, nil
		}
	}
}

// TestScalePostgresPlainCloseCanLeaveMaxConnectionSlotOccupied reproduces the
// bare-Close race from the process qualification. The listener completes
// startup for the first client, then holds the server's next read. Closing the
// client's socket cannot release the server's live-session slot while that
// read is held.
func TestScalePostgresPlainCloseCanLeaveMaxConnectionSlotOccupied(t *testing.T) {
	address, gate, cleanup := startScaleShutdownTestServer(t)
	t.Cleanup(cleanup)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	first, result, err := dialScaleShutdownTestSQL(ctx, address)
	if err != nil || result.code != "" {
		t.Fatalf("first PostgreSQL startup result=%+v err=%v", result, err)
	}
	defer first.Close()
	select {
	case <-gate.entered:
	case <-ctx.Done():
		t.Fatalf("server did not reach its held post-startup read: %v", context.Cause(ctx))
	}
	if err := first.Close(); err != nil {
		t.Fatalf("close first client socket: %v", err)
	}
	second, result, err := dialScaleShutdownTestSQL(ctx, address)
	if second != nil {
		_ = second.Close()
	}
	if err != nil {
		t.Fatalf("fresh startup after client close transport: %v", err)
	}
	if result.code != scaleShutdownTestSQLStateTooManyConnections {
		t.Fatalf("fresh startup after client close returned SQLSTATE %s (%s), want deterministic slot-held refusal %s", result.code, result.message, scaleShutdownTestSQLStateTooManyConnections)
	}
}

func TestScalePostgresTerminateBarrierWaitsThenReleasesExactlyOneSlot(t *testing.T) {
	address, gate, cleanup := startScaleShutdownTestServer(t)
	t.Cleanup(cleanup)
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	first, result, err := dialScaleShutdownTestSQL(ctx, address)
	if err != nil || result.code != "" {
		t.Fatalf("first PostgreSQL startup result=%+v err=%v", result, err)
	}
	defer first.Close()
	select {
	case <-gate.entered:
	case <-ctx.Done():
		t.Fatalf("server did not reach its held post-startup read: %v", context.Cause(ctx))
	}

	observed := &scaleShutdownReadObservedConn{Conn: first, started: make(chan struct{})}
	shutdownCtx, shutdownCancel := context.WithTimeout(ctx, 8*time.Second)
	defer shutdownCancel()
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- terminateAndWaitPostgresConnections(shutdownCtx, []net.Conn{observed}) }()
	select {
	case <-observed.started:
	case <-ctx.Done():
		t.Fatalf("shutdown barrier did not begin waiting for peer EOF: %v", context.Cause(ctx))
	}
	assertScaleShutdownStillWaiting(t, shutdownDone)

	blocked, result, err := dialScaleShutdownTestSQL(ctx, address)
	if blocked != nil {
		_ = blocked.Close()
	}
	if err != nil || result.code != scaleShutdownTestSQLStateTooManyConnections {
		t.Fatalf("startup while server read is held result=%+v err=%v, want SQLSTATE %s", result, err, scaleShutdownTestSQLStateTooManyConnections)
	}
	assertScaleShutdownStillWaiting(t, shutdownDone)

	// The Terminate message is already queued by the client, but the controlled
	// server-side read gate has not exposed it to session.serve. Releasing the
	// gate lets normal Terminate processing unregister the session before the
	// server closes its socket and the barrier accepts peer EOF.
	gate.releaseReads()
	select {
	case err := <-shutdownDone:
		if err != nil {
			t.Fatalf("wait for PostgreSQL Terminate EOF: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("shutdown barrier did not observe server EOF: %v", context.Cause(ctx))
	}

	fresh, result, err := dialScaleShutdownTestSQL(ctx, address)
	if err != nil || result.code != "" {
		if fresh != nil {
			_ = fresh.Close()
		}
		t.Fatalf("fresh startup after checked barrier result=%+v err=%v", result, err)
	}
	defer fresh.Close()
	second, result, err := dialScaleShutdownTestSQL(ctx, address)
	if second != nil {
		_ = second.Close()
	}
	if err != nil || result.code != scaleShutdownTestSQLStateTooManyConnections {
		t.Fatalf("second startup while one fresh session is held result=%+v err=%v, want SQLSTATE %s", result, err, scaleShutdownTestSQLStateTooManyConnections)
	}
	if err := terminateAndWaitPostgresConnections(ctx, []net.Conn{fresh}); err != nil {
		t.Fatalf("release final test session through Terminate barrier: %v", err)
	}
}

func assertScaleShutdownStillWaiting(t *testing.T, completed <-chan error) {
	t.Helper()
	select {
	case err := <-completed:
		t.Fatalf("shutdown barrier completed before the held server read was released: %v", err)
	default:
	}
}

func TestScalePostgresTerminateBarrierRejectsUnboundedContextAndUnexpectedData(t *testing.T) {
	t.Run("requires bounded context", func(t *testing.T) {
		if err := terminateAndWaitPostgresConnections(context.Background(), nil); err == nil || !strings.Contains(err.Error(), "deadline") {
			t.Fatalf("unbounded shutdown context error=%v", err)
		}
	})
	t.Run("reports write error", func(t *testing.T) {
		client, peer := net.Pipe()
		if err := peer.Close(); err != nil {
			t.Fatal(err)
		}
		defer client.Close()
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		err := terminateAndWaitPostgresConnections(ctx, []net.Conn{scaleShutdownIgnoreDeadlineConn{Conn: client}})
		if err == nil || !strings.Contains(err.Error(), "write PostgreSQL Terminate") {
			t.Fatalf("closed peer write error=%v, want Terminate write failure", err)
		}
	})
	t.Run("requires peer close before context deadline", func(t *testing.T) {
		client, peer := net.Pipe()
		defer client.Close()
		defer peer.Close()
		peerDone := make(chan error, 1)
		go func() {
			var message [5]byte
			_, err := io.ReadFull(peer, message[:])
			if err == nil && message != [5]byte{'X', 0, 0, 0, 4} {
				err = fmt.Errorf("unexpected Terminate frame %v", message)
			}
			peerDone <- err
		}()
		cause := errors.New("peer did not close after PostgreSQL Terminate")
		ctx, cancel := context.WithTimeoutCause(t.Context(), 100*time.Millisecond, cause)
		defer cancel()
		shutdownDone := make(chan error, 1)
		go func() { shutdownDone <- terminateAndWaitPostgresConnections(ctx, []net.Conn{client}) }()
		if err := <-peerDone; err != nil {
			t.Fatalf("peer did not receive Terminate frame: %v", err)
		}
		select {
		case err := <-shutdownDone:
			var networkError net.Error
			if !errors.Is(err, cause) && (!errors.As(err, &networkError) || !networkError.Timeout()) {
				t.Fatalf("shutdown error=%v, want context cause %v or its deadline timeout", err, cause)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("shutdown did not stop at its context deadline")
		}
	})
	t.Run("unexpected peer data", func(t *testing.T) {
		client, peer := net.Pipe()
		defer client.Close()
		defer peer.Close()
		peerDone := make(chan error, 1)
		go func() {
			var message [5]byte
			if _, err := io.ReadFull(peer, message[:]); err != nil {
				peerDone <- err
				return
			}
			if message != [5]byte{'X', 0, 0, 0, 4} {
				peerDone <- fmt.Errorf("unexpected Terminate frame %v", message)
				return
			}
			_, err := peer.Write([]byte{'Z'})
			peerDone <- err
		}()
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		err := terminateAndWaitPostgresConnections(ctx, []net.Conn{client})
		if err == nil || !strings.Contains(err.Error(), "unexpected data") {
			t.Fatalf("shutdown accepted non-EOF response: %v", err)
		}
		if err := <-peerDone; err != nil {
			t.Fatalf("peer did not observe the complete Terminate frame and reply: %v", err)
		}
	})
	t.Run("preserves cancellation cause", func(t *testing.T) {
		client, peer := net.Pipe()
		defer client.Close()
		defer peer.Close()
		parent, cancelParent := context.WithCancelCause(context.Background())
		defer cancelParent(nil)
		ctx, cancelDeadline := context.WithTimeout(parent, 5*time.Second)
		defer cancelDeadline()
		messageRead := make(chan error, 1)
		peerDone := make(chan struct{})
		go func() {
			defer close(peerDone)
			var message [5]byte
			if _, err := io.ReadFull(peer, message[:]); err != nil {
				messageRead <- err
				return
			}
			messageRead <- nil
			<-ctx.Done()
		}()
		shutdownDone := make(chan error, 1)
		go func() { shutdownDone <- terminateAndWaitPostgresConnections(ctx, []net.Conn{client}) }()
		if err := <-messageRead; err != nil {
			t.Fatalf("read Terminate frame: %v", err)
		}
		cause := errors.New("test shutdown canceled")
		cancelParent(cause)
		select {
		case err := <-shutdownDone:
			if !errors.Is(err, cause) {
				t.Fatalf("shutdown error=%v, want preserved cause %v", err, cause)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("shutdown did not wake after context cancellation")
		}
		select {
		case <-peerDone:
		case <-time.After(2 * time.Second):
			t.Fatal("peer test goroutine did not finish")
		}
	})
	t.Run("cancellation during deadline setup prevents writes", func(t *testing.T) {
		client, peer := net.Pipe()
		defer client.Close()
		defer peer.Close()
		parent, cancelCause := context.WithCancelCause(t.Context())
		defer cancelCause(nil)
		bounded, cancelDeadline := context.WithTimeout(parent, 5*time.Second)
		defer cancelDeadline()
		cause := errors.New("deadline setup canceled")
		gated := &scaleShutdownCancelDeadlineConn{Conn: client, cancel: cancelCause, cause: cause}
		shutdownDone := make(chan error, 1)
		go func() { shutdownDone <- terminateAndWaitPostgresConnections(bounded, []net.Conn{gated}) }()
		select {
		case err := <-shutdownDone:
			if !errors.Is(err, cause) {
				t.Fatalf("shutdown error=%v, want cancellation cause %v", err, cause)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("shutdown did not stop after cancellation during deadline setup")
		}
		if writes := gated.writeCount(); writes != 0 {
			t.Fatalf("shutdown attempted %d Terminate byte(s) after cancellation", writes)
		}
	})
	t.Run("EOF concurrent with cancellation is rejected", func(t *testing.T) {
		client, peer := net.Pipe()
		defer client.Close()
		defer peer.Close()
		parent, cancelCause := context.WithCancelCause(t.Context())
		defer cancelCause(nil)
		bounded, cancelDeadline := context.WithTimeout(parent, 5*time.Second)
		defer cancelDeadline()
		cause := errors.New("canceled as peer EOF arrived")
		observed := &scaleShutdownCancelOnEOFConn{Conn: client, cancel: cancelCause, cause: cause}
		peerDone := make(chan error, 1)
		go func() {
			var message [5]byte
			if _, err := io.ReadFull(peer, message[:]); err != nil {
				peerDone <- err
				return
			}
			if message != [5]byte{'X', 0, 0, 0, 4} {
				peerDone <- fmt.Errorf("unexpected Terminate frame %v", message)
				return
			}
			peerDone <- peer.Close()
		}()
		err := terminateAndWaitPostgresConnections(bounded, []net.Conn{observed})
		if !errors.Is(err, cause) {
			t.Fatalf("shutdown error=%v, want cancellation cause %v after genuine EOF", err, cause)
		}
		if err := <-peerDone; err != nil {
			t.Fatalf("peer did not observe Terminate before closing: %v", err)
		}
	})
}

func terminateAndWaitPostgresConnections(ctx context.Context, connections []net.Conn) error {
	if ctx == nil {
		return errors.New("PostgreSQL shutdown requires a bounded context")
	}
	deadline, bounded := ctx.Deadline()
	if !bounded {
		return errors.New("PostgreSQL shutdown requires a context deadline")
	}
	if len(connections) == 0 {
		return nil
	}
	for index, connection := range connections {
		if connection == nil {
			return fmt.Errorf("PostgreSQL shutdown connection %d is nil", index+1)
		}
	}
	if cause := context.Cause(ctx); cause != nil {
		return fmt.Errorf("PostgreSQL shutdown canceled before Terminate: %w", cause)
	}
	for index, connection := range connections {
		if err := connection.SetDeadline(deadline); err != nil {
			if cause := context.Cause(ctx); cause != nil {
				return fmt.Errorf("set PostgreSQL shutdown deadline for connection %d: %w", index+1, cause)
			}
			return fmt.Errorf("set PostgreSQL shutdown deadline for connection %d: %w", index+1, err)
		}
	}
	if cause := context.Cause(ctx); cause != nil {
		return fmt.Errorf("PostgreSQL shutdown canceled during deadline setup: %w", cause)
	}
	interruptDone := make(chan struct{})
	stopInterrupt := context.AfterFunc(ctx, func() {
		defer close(interruptDone)
		for _, connection := range connections {
			_ = connection.SetDeadline(time.Now())
		}
	})
	defer func() {
		if !stopInterrupt() {
			<-interruptDone
		}
	}()
	terminate := [...]byte{'X', 0, 0, 0, 4}
	for index, connection := range connections {
		frame := terminate[:]
		for len(frame) != 0 {
			written, err := connection.Write(frame)
			if err != nil {
				if cause := context.Cause(ctx); cause != nil {
					return fmt.Errorf("write PostgreSQL Terminate on connection %d: %w", index+1, cause)
				}
				return fmt.Errorf("write PostgreSQL Terminate on connection %d: %w", index+1, err)
			}
			if written <= 0 {
				return fmt.Errorf("write PostgreSQL Terminate on connection %d: %w", index+1, io.ErrShortWrite)
			}
			frame = frame[written:]
		}
	}
	for index, connection := range connections {
		var received [1]byte
		count, err := connection.Read(received[:])
		if count != 0 {
			return fmt.Errorf("PostgreSQL connection %d sent %d unexpected data byte(s) after Terminate", index+1, count)
		}
		if !errors.Is(err, io.EOF) {
			if cause := context.Cause(ctx); cause != nil {
				return fmt.Errorf("wait for PostgreSQL peer EOF on connection %d: %w", index+1, cause)
			}
			if err == nil {
				return fmt.Errorf("wait for PostgreSQL peer EOF on connection %d: read returned no data or error", index+1)
			}
			return fmt.Errorf("wait for PostgreSQL peer EOF on connection %d: %w", index+1, err)
		}
	}
	if cause := context.Cause(ctx); cause != nil {
		return fmt.Errorf("PostgreSQL shutdown canceled before peer EOF was fully verified: %w", cause)
	}
	return nil
}
