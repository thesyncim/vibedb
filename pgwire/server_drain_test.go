package pgwire

import (
	"errors"
	"net"
	"testing"
	"time"
)

func TestBeginDrainClosesAdmissionKeepsExistingSQLSession(t *testing.T) {
	server := newTestServer(t, Options{})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(listener) }()

	connection, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	client := newTestClient(t, connection)
	client.startup(map[string]string{"user": "tester", "database": "app"})
	state := server.AdmissionState()
	if state.ActiveConnections != 1 || state.ActiveSessions != 1 {
		t.Fatalf("before drain state = %+v, want one live SQL session", state)
	}

	ack := server.BeginDrain()
	if !ack.Draining || !ack.AdmissionDrained || ack.Revision == 0 {
		t.Fatalf("drain acknowledgement = %+v", ack)
	}
	if ack.ActiveConnections != 1 || ack.ActiveSessions != 1 {
		t.Fatalf("drain closed an existing session: %+v", ack)
	}
	if replacement, dialErr := net.DialTimeout("tcp", listener.Addr().String(), 250*time.Millisecond); dialErr == nil {
		_ = replacement.Close()
		t.Fatal("a new PostgreSQL connection was admitted after BeginDrain")
	}
	select {
	case err := <-serveErr:
		if !errors.Is(err, ErrServerDraining) {
			t.Fatalf("Serve after admission drain = %v, want ErrServerDraining", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not observe the closed admission listener")
	}

	// An idle session remains able to execute after admission is closed. This
	// also proves that BeginDrain did not cancel its SQL context.
	if messages := client.query("SELECT * FROM users"); len(messages) == 0 {
		t.Fatal("existing session produced no response after drain")
	}
	client.terminate()
	client.drainWrites()
	_ = connection.Close()
	deadline := time.Now().Add(2 * time.Second)
	for {
		state = server.AdmissionState()
		if state.ActiveConnections == 0 && state.ActiveSessions == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("session did not release after close: %+v", state)
		}
		time.Sleep(time.Millisecond)
	}
	if err := server.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		t.Fatalf("Close: %v", err)
	}
}

func TestBeginDrainPreservesActiveTransactionUntilClientCloses(t *testing.T) {
	server := newTestServer(t, Options{})
	client := dial(t, server)
	client.startup(map[string]string{"user": "tester", "database": "app"})
	client.query("BEGIN")

	ack := server.BeginDrain()
	if !ack.AdmissionDrained || ack.ActiveSessions != 1 {
		t.Fatalf("active transaction was not retained: %+v", ack)
	}
	// The transaction owner remains usable after the drain fence. The test
	// client cleanup closes it; no shutdown path is allowed to force-close it.
	if messages := client.query("SELECT 1"); len(messages) == 0 {
		t.Fatal("active transaction did not remain usable after drain")
	}
	state := server.AdmissionState()
	if state.ActiveSessions != 1 {
		t.Fatalf("session released before client close: %+v", state)
	}
}
