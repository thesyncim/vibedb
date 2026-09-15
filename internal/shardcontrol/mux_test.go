package shardcontrol

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

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
func (*testConnection) PeerKeyDigest() [32]byte                             { return [32]byte{} }
func (connection *testConnection) TrafficClass() rafttransport.TrafficClass { return connection.class }

type delayedReadHandler struct {
	done chan error
}

func (handler *delayedReadHandler) Serve(_ context.Context, connection rafttransport.PeerConnection) error {
	defer connection.Close()
	var prefix [DiscriminatorBytes]byte
	if _, err := io.ReadFull(connection, prefix[:]); err != nil {
		handler.done <- err
		return err
	}
	var payload [1]byte
	_, err := io.ReadFull(connection, payload[:])
	handler.done <- err
	return err
}

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

func TestMuxBoundsIdleDiscriminator(t *testing.T) {
	magic := [8]byte{'V', 'B', 'I', 'D', 'L', 'E', 0, 1}
	mux, err := New(Route{Discriminator: magic, Handler: &testHandler{want: magic[:], done: make(chan error, 1)}})
	if err != nil {
		t.Fatal(err)
	}
	mux.discriminatorTimeout = 20 * time.Millisecond
	client, server := net.Pipe()
	defer client.Close()
	done := make(chan error, 1)
	started := time.Now()
	go func() {
		done <- mux.Serve(context.Background(), &testConnection{
			Conn: server, class: rafttransport.TrafficShardControl,
		})
	}()
	select {
	case err = <-done:
		if !errors.Is(err, ErrMux) {
			t.Fatalf("idle discriminator err=%v", err)
		}
		if elapsed := time.Since(started); elapsed > time.Second {
			t.Fatalf("idle discriminator was not bounded: %s", elapsed)
		}
	case <-time.After(time.Second):
		t.Fatal("idle discriminator remained blocked")
	}
}

func TestMuxClearsDiscriminatorDeadlineBeforeDispatch(t *testing.T) {
	magic := [8]byte{'V', 'B', 'D', 'L', 'E', 'A', 'R', 1}
	handler := &delayedReadHandler{done: make(chan error, 1)}
	mux, err := New(Route{Discriminator: magic, Handler: handler})
	if err != nil {
		t.Fatal(err)
	}
	mux.discriminatorTimeout = 20 * time.Millisecond
	client, server := net.Pipe()
	defer client.Close()
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- mux.Serve(context.Background(), &testConnection{
			Conn: server, class: rafttransport.TrafficShardControl,
		})
	}()
	_ = client.SetWriteDeadline(time.Now().Add(time.Second))
	if _, err = client.Write(magic[:]); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if _, err = client.Write([]byte{7}); err != nil {
		t.Fatalf("handler inherited discriminator deadline: %v", err)
	}
	if err = <-handler.done; err != nil {
		t.Fatalf("handler read: %v", err)
	}
	if err = <-serveDone; err != nil {
		t.Fatalf("serve: %v", err)
	}
}
