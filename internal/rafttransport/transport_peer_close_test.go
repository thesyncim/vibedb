package rafttransport

import (
	"context"
	"io"
	"sync/atomic"
	"testing"
)

// TLS close_notify/EOF can arrive while small writes still succeed into local
// TCP buffers. The send-only stream must observe its read side to reconnect.
type peerEOFConnection struct {
	*transportTestConnection
	remoteClosed chan struct{}
}

func (connection *peerEOFConnection) Read([]byte) (int, error) {
	select {
	case <-connection.remoteClosed:
	case <-connection.closed:
	}
	return 0, io.EOF
}

func TestOrdinaryTransportReconnectsAfterPeerReadEOF(t *testing.T) {
	fixture := newTransportTestFixture(t)
	first := &peerEOFConnection{
		transportTestConnection: newTransportTestConnection(fixture.registry, fixture.remote[0].Node),
		remoteClosed: make(chan struct{}),
	}
	second := newTransportTestConnection(fixture.registry, fixture.remote[0].Node)
	var calls atomic.Int32
	dial := ordinaryDialFunc(func(context.Context, NodeID) (PeerConnection, error) {
		if calls.Add(1) == 1 {
			return first, nil
		}
		return second, nil
	})
	transport, err := NewOrdinaryTransport(transportTestOptions(fixture, dial))
	if err != nil {
		t.Fatal(err)
	}
	cancel, done := runTransportTest(t, transport)
	defer stopTransportTest(t, transport, cancel, done)
	if err := transport.Send(fixture.outbound(0, 1)); err != nil {
		t.Fatal(err)
	}
	transportTestEventually(t, func() bool { return first.writeCallCount() != 0 })
	close(first.remoteClosed)
	transportTestEventually(t, func() bool {
		select {
		case <-first.closed:
			return true
		default:
			return false
		}
	})
	if err := transport.Send(fixture.outbound(0, 2)); err != nil {
		t.Fatal(err)
	}
	transportTestEventually(t, func() bool { return second.writeCallCount() != 0 })
	if calls.Load() != 2 {
		t.Fatalf("reconnected %d times, want two authenticated connections", calls.Load())
	}
}
