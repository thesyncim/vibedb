package shardcontrol

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"

	"github.com/thesyncim/vibedb/internal/rafttransport"
)

type testHandler struct {
	want []byte
	done chan error
}

func (handler *testHandler) Serve(_ context.Context, connection rafttransport.PeerConnection) error {
	defer connection.Close()
	raw := make([]byte, len(handler.want))
	_, err := io.ReadFull(connection, raw)
	if err == nil && string(raw) != string(handler.want) {
		err = errors.New("wrong replayed request")
	}
	handler.done <- err
	return err
}

type testConnection struct {
	net.Conn
	class rafttransport.TrafficClass
}

func (connection *testConnection) PeerIdentity() rafttransport.PeerIdentity {
	return rafttransport.PeerIdentity{}
}
func (connection *testConnection) TrafficClass() rafttransport.TrafficClass { return connection.class }

func TestMuxDispatchesAndReplaysExactDiscriminator(t *testing.T) {
	firstMagic := [8]byte{'V', 'B', 'F', 'I', 'R', 'S', 'T', 0}
	secondMagic := [8]byte{'V', 'B', 'S', 'E', 'C', 'O', 'N', 'D'}
	first := &testHandler{want: append(firstMagic[:], 1, 2, 3), done: make(chan error, 1)}
	second := &testHandler{want: append(secondMagic[:], 4, 5), done: make(chan error, 1)}
	mux, err := New(Route{Discriminator: firstMagic, Handler: first},
		Route{Discriminator: secondMagic, Handler: second})
	if err != nil {
		t.Fatal(err)
	}
	client, server := net.Pipe()
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- mux.Serve(context.Background(), &testConnection{
			Conn: server, class: rafttransport.TrafficShardControl,
		})
	}()
	if _, err = client.Write(second.want); err != nil {
		t.Fatal(err)
	}
	if err = <-second.done; err != nil {
		t.Fatal(err)
	}
	if err = <-serveDone; err != nil {
		t.Fatal(err)
	}
}

func TestMuxRejectsUnknownDuplicateAndWrongTraffic(t *testing.T) {
	magic := [8]byte{'V', 'B', 'O', 'N', 'L', 'Y', 0, 0}
	handler := &testHandler{want: magic[:], done: make(chan error, 1)}
	if _, err := New(Route{Discriminator: magic, Handler: handler},
		Route{Discriminator: magic, Handler: handler}); !errors.Is(err, ErrMux) {
		t.Fatalf("duplicate err=%v", err)
	}
	mux, err := New(Route{Discriminator: magic, Handler: handler})
	if err != nil {
		t.Fatal(err)
	}
	for _, fixture := range []struct {
		class rafttransport.TrafficClass
		raw   [8]byte
	}{
		{class: rafttransport.TrafficShardControl, raw: [8]byte{'U'}},
		{class: rafttransport.TrafficOrdinary, raw: magic},
	} {
		client, server := net.Pipe()
		done := make(chan error, 1)
		go func() {
			done <- mux.Serve(context.Background(), &testConnection{Conn: server, class: fixture.class})
		}()
		_, _ = client.Write(fixture.raw[:])
		_ = client.Close()
		if err = <-done; !errors.Is(err, ErrMux) {
			t.Fatalf("serve err=%v", err)
		}
	}
}

func TestMuxRouteBoundCoversCompleteControlPlane(t *testing.T) {
	handler := &testHandler{}
	routes := make([]Route, MaxRoutes)
	for index := range routes {
		routes[index] = Route{
			Discriminator: [8]byte{'V', 'B', 'B', 'O', 'U', 'N', 'D', byte(index + 1)},
			Handler:       handler,
		}
	}
	if _, err := New(routes...); err != nil {
		t.Fatalf("maximum bounded route set rejected: %v", err)
	}
	routes = append(routes, Route{
		Discriminator: [8]byte{'V', 'B', 'B', 'O', 'U', 'N', 'D', byte(MaxRoutes + 1)},
		Handler:       handler,
	})
	if _, err := New(routes...); !errors.Is(err, ErrMux) {
		t.Fatalf("route set beyond bound err=%v", err)
	}
}

func TestMuxDispatchesExactSnapshotTrafficWithoutAcceptingControl(t *testing.T) {
	magic := [8]byte{'V', 'B', 'S', 'N', 'A', 'P', 0, 0}
	handler := &testHandler{want: append(magic[:], 9), done: make(chan error, 1)}
	mux, err := NewForTraffic(
		rafttransport.TrafficSnapshot,
		Route{Discriminator: magic, Handler: handler},
	)
	if err != nil {
		t.Fatal(err)
	}
	client, server := net.Pipe()
	done := make(chan error, 1)
	go func() {
		done <- mux.Serve(t.Context(), &testConnection{Conn: server, class: rafttransport.TrafficSnapshot})
	}()
	if _, err = client.Write(handler.want); err != nil {
		t.Fatal(err)
	}
	if err = <-handler.done; err != nil {
		t.Fatal(err)
	}
	if err = <-done; err != nil {
		t.Fatal(err)
	}

	client, server = net.Pipe()
	go func() {
		done <- mux.Serve(t.Context(), &testConnection{Conn: server, class: rafttransport.TrafficShardControl})
	}()
	_ = client.Close()
	if err = <-done; !errors.Is(err, ErrMux) {
		t.Fatalf("control traffic err=%v", err)
	}
}
