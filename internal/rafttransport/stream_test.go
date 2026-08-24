package rafttransport

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "go.etcd.io/raft/v3/raftpb"
)

func TestOrdinaryReceiverReadsShortIOAndAdmitsBeforeOwnedDelivery(t *testing.T) {
	group := testGroup(111)
	sender, registry, from, to := frameTestRegistries(t, 2, group)
	stream := make([]byte, 0, 1024)
	for index := uint64(1); index <= 3; index++ {
		message := frameTestMessage(pb.MsgHeartbeat, from, to)
		message.Index = frameU64(index)
		frame := frameTestEncode(t, sender, group, message)
		var err error
		stream, err = appendStreamRecord(stream, frame)
		if err != nil {
			t.Fatal(err)
		}
	}
	connection := newStreamTestConnection(testPeerIdentity(registry, sender.LocalNode()), stream)
	connection.maxRead = 1
	var deadlineCalls atomic.Uint32
	var delivered []Inbound
	receiver, err := NewOrdinaryReceiver(OrdinaryReceiverOptions{
		Registry: registry,
		ReadDeadline: func() time.Time {
			deadlineCalls.Add(1)
			return time.Now().Add(time.Minute)
		},
		Handle: func(_ context.Context, inbound Inbound) error {
			delivered = append(delivered, inbound)
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := receiver.Serve(context.Background(), connection); err != nil {
		t.Fatal(err)
	}
	clear(stream)
	if len(delivered) != 3 {
		t.Fatalf("deliveries = %d", len(delivered))
	}
	for index, inbound := range delivered {
		if inbound.Group != group || inbound.Message.GetIndex() != uint64(index+1) {
			t.Fatalf("delivery %d = %+v", index, inbound)
		}
	}
	if deadlineCalls.Load() != 4 {
		t.Fatalf("read deadline calls = %d, want 4", deadlineCalls.Load())
	}
}

func TestOrdinaryReceiverServeTLSAuthenticatesBeforeDelivery(t *testing.T) {
	group := testGroup(115)
	sender, registry, from, to := frameTestRegistries(t, 2, group)
	domain := TrustDomain{
		ClusterID: group.ClusterID, ClusterIncarnation: group.ClusterIncarnation,
	}
	authority := newPeerTLSTestAuthority(t, 20)
	clientIdentity := PeerIdentity{TrustDomain: domain, Node: sender.LocalNode()}
	serverIdentity := PeerIdentity{TrustDomain: domain, Node: registry.LocalNode()}
	clientTLS := newPeerTLSTestProfile(t, authority, clientIdentity)
	serverTLS := newPeerTLSTestProfile(t, authority, serverIdentity)
	message := frameTestMessage(pb.MsgHeartbeat, from, to)
	frame := frameTestEncode(t, sender, group, message)
	stream, err := appendStreamRecord(nil, frame)
	if err != nil {
		t.Fatal(err)
	}
	delivered := make(chan Inbound, 1)
	receiver := newStreamTestReceiver(t, registry, func(_ context.Context, inbound Inbound) error {
		delivered <- inbound
		return nil
	})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	deadline := func() time.Time { return time.Now().Add(5 * time.Second) }
	serverDone := make(chan error, 1)
	go func() {
		raw, acceptErr := listener.Accept()
		if acceptErr != nil {
			serverDone <- acceptErr
			return
		}
		serverDone <- receiver.ServeTLS(ctx, raw, serverTLS, deadline)
	}()
	raw, err := (&net.Dialer{}).DialContext(ctx, "tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	connection, err := clientTLS.Client(
		ctx, raw, serverIdentity.Node, TrafficOrdinary, deadline,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeFull(connection, stream); err != nil {
		t.Fatal(err)
	}
	if err := connection.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case inbound := <-delivered:
		if inbound.Group != group || inbound.Message.GetFrom() != from {
			t.Fatalf("TLS delivery = %+v", inbound)
		}
	case <-ctx.Done():
		t.Fatal("authenticated frame was not delivered")
	}
	select {
	case err := <-serverDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("TLS receiver leaked")
	}
}

func TestOrdinaryReceiverRejectsStaticFrameBeforeDelivery(t *testing.T) {
	group := testGroup(112)
	sender, registry, from, to := frameTestRegistries(t, 2, group)
	frame := frameTestEncode(t, sender, group, frameTestMessage(pb.MsgHeartbeat, from, to))
	frame[frameTestRosterOffset] ^= 0xff
	stream, err := appendStreamRecord(nil, frame)
	if err != nil {
		t.Fatal(err)
	}
	connection := newStreamTestConnection(testPeerIdentity(registry, sender.LocalNode()), stream)
	deliveries := 0
	receiver := newStreamTestReceiver(t, registry, func(context.Context, Inbound) error {
		deliveries++
		return nil
	})
	if err := receiver.Serve(context.Background(), connection); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("error = %v, want ErrUnauthorized", err)
	}
	if deliveries != 0 {
		t.Fatalf("invalid frame deliveries = %d", deliveries)
	}
}

func TestOrdinaryReceiverRejectsCrossDomainConnectionBeforeRead(t *testing.T) {
	group := testGroup(116)
	sender, registry, _, _ := frameTestRegistries(t, 2, group)
	identity := testPeerIdentity(registry, sender.LocalNode())
	identity.TrustDomain.ClusterIncarnation[0]++
	connection := newStreamTestConnection(identity, nil)
	receiver := newStreamTestReceiver(t, registry, func(context.Context, Inbound) error {
		t.Fatal("cross-domain connection reached handler")
		return nil
	})
	if err := receiver.Serve(context.Background(), connection); !errors.Is(err, ErrInvalidTransport) {
		t.Fatalf("error = %v, want ErrInvalidTransport", err)
	}
	connection.mu.Lock()
	closed := connection.closed
	connection.mu.Unlock()
	if !closed {
		t.Fatal("cross-domain connection was not closed")
	}
}

func TestOrdinaryReceiverRejectsWrongPeerClassAndPartialRecords(t *testing.T) {
	group := testGroup(113)
	sender, registry, from, to := frameTestRegistries(t, 2, group)
	frame := frameTestEncode(t, sender, group, frameTestMessage(pb.MsgHeartbeat, from, to))
	valid, err := appendStreamRecord(nil, frame)
	if err != nil {
		t.Fatal(err)
	}
	tooLarge := make([]byte, StreamRecordHeaderBytes)
	binary.BigEndian.PutUint32(tooLarge, uint32(MaxFrameBytes+1))
	tests := []struct {
		name   string
		stream []byte
		node   NodeID
		class  TrafficClass
		want   error
	}{
		{name: "partial header", stream: valid[:3], node: sender.LocalNode(), class: TrafficOrdinary, want: ErrInvalidFrame},
		{name: "partial body", stream: valid[:len(valid)-1], node: sender.LocalNode(), class: TrafficOrdinary, want: ErrInvalidFrame},
		{name: "declared too large", stream: tooLarge, node: sender.LocalNode(), class: TrafficOrdinary, want: ErrFrameTooLarge},
		{name: "wrong authenticated node", stream: valid, node: testNode(99), class: TrafficOrdinary, want: ErrUnauthorized},
		{name: "snapshot class", stream: valid, node: sender.LocalNode(), class: TrafficSnapshot, want: ErrInvalidTransport},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			connection := newStreamTestConnection(testPeerIdentity(registry, test.node), test.stream)
			connection.class = test.class
			receiver := newStreamTestReceiver(t, registry, func(context.Context, Inbound) error {
				t.Fatal("invalid stream reached handler")
				return nil
			})
			if err := receiver.Serve(context.Background(), connection); !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestOrdinaryReceiverCancellationClosesBlockedReadWithoutLeak(t *testing.T) {
	group := testGroup(114)
	sender, registry, _, _ := frameTestRegistries(t, 2, group)
	local, remote := net.Pipe()
	connection := &streamAuthenticatedConnection{
		Conn: local, identity: testPeerIdentity(registry, sender.LocalNode()), class: TrafficOrdinary,
	}
	receiver := newStreamTestReceiver(t, registry, func(context.Context, Inbound) error {
		t.Fatal("blocked stream delivered a frame")
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- receiver.Serve(ctx, connection) }()
	time.Sleep(10 * time.Millisecond)
	cancel()
	defer remote.Close()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled receiver error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("canceled receiver leaked")
	}
}

func TestStreamRecordAndWriteFullRejectInvalidOrZeroProgress(t *testing.T) {
	if _, err := appendStreamRecord(nil, make([]byte, FrameHeaderBytes-1)); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("short frame error = %v", err)
	}
	if err := writeFull(zeroProgressWriter{}, []byte("frame")); !errors.Is(err, io.ErrNoProgress) {
		t.Fatalf("zero-progress error = %v", err)
	}
	writer := &shortStreamWriter{maximum: 1}
	if err := writeFull(writer, []byte("complete")); err != nil {
		t.Fatal(err)
	}
	if writer.buffer.String() != "complete" || writer.calls != len("complete") {
		t.Fatalf("short writer = %q in %d calls", writer.buffer.String(), writer.calls)
	}
}

func TestWaitAndRawDialFailureCloseContracts(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := WaitWithTimer(ctx, 0, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("zero-delay canceled wait error = %v", err)
	}
	if err := WaitWithTimer(nil, time.Second, nil); !errors.Is(err, ErrInvalidTransport) {
		t.Fatalf("nil-context wait error = %v", err)
	}

	raw := newStreamTestConnection(PeerIdentity{Node: testNode(1)}, nil)
	dialer := TLSOrdinaryDialer{
		TLS: &PeerTLS{},
		Dial: func(context.Context, NodeID) (net.Conn, error) {
			return raw, io.ErrUnexpectedEOF
		},
	}
	if _, err := dialer.DialOrdinary(context.Background(), testNode(2)); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("raw dial error = %v", err)
	}
	raw.mu.Lock()
	closed := raw.closed
	raw.mu.Unlock()
	if !closed {
		t.Fatal("raw connection was not closed after dial error")
	}
}

func TestFrameBufferPoolClearsOnlyLiveBytes(t *testing.T) {
	pool := frameBufferPool{retain: DefaultRetainedFrameBytes}
	buffer := &pooledFrameBuffer{
		bytes: make([]byte, FrameHeaderBytes, DefaultRetainedFrameBytes),
	}
	for index := range buffer.bytes {
		buffer.bytes[index] = 0xff
	}
	retained := buffer.bytes[:cap(buffer.bytes)]
	retained[len(retained)-1] = 0x7f
	pool.put(buffer)

	retained = buffer.bytes[:cap(buffer.bytes)]
	for index, value := range retained[:FrameHeaderBytes] {
		if value != 0 {
			t.Fatalf("live byte %d was not cleared", index)
		}
	}
	if retained[len(retained)-1] != 0x7f {
		t.Fatal("put cleared bytes beyond the live frame")
	}
}

func newStreamTestReceiver(
	t testing.TB,
	registry *StaticRegistry,
	handler InboundHandler,
) *OrdinaryReceiver {
	t.Helper()
	receiver, err := NewOrdinaryReceiver(OrdinaryReceiverOptions{
		Registry: registry,
		ReadDeadline: func() time.Time {
			return time.Now().Add(time.Minute)
		},
		Handle: handler,
	})
	if err != nil {
		t.Fatal(err)
	}
	return receiver
}

type streamTestConnection struct {
	identity PeerIdentity
	class    TrafficClass
	reader   *bytes.Reader
	maxRead  int

	mu        sync.Mutex
	closed    bool
	deadlines int
}

func newStreamTestConnection(identity PeerIdentity, stream []byte) *streamTestConnection {
	return &streamTestConnection{
		identity: identity, class: TrafficOrdinary,
		reader: bytes.NewReader(bytes.Clone(stream)),
	}
}

func (connection *streamTestConnection) PeerIdentity() PeerIdentity {
	return connection.identity
}
func (connection *streamTestConnection) TrafficClass() TrafficClass {
	return connection.class
}
func (connection *streamTestConnection) Read(buffer []byte) (int, error) {
	if connection.maxRead > 0 && len(buffer) > connection.maxRead {
		buffer = buffer[:connection.maxRead]
	}
	return connection.reader.Read(buffer)
}
func (connection *streamTestConnection) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }
func (connection *streamTestConnection) Close() error {
	connection.mu.Lock()
	connection.closed = true
	connection.mu.Unlock()
	return nil
}
func (connection *streamTestConnection) LocalAddr() net.Addr         { return transportTestAddr("local") }
func (connection *streamTestConnection) RemoteAddr() net.Addr        { return transportTestAddr("remote") }
func (connection *streamTestConnection) SetDeadline(time.Time) error { return nil }
func (connection *streamTestConnection) SetReadDeadline(time.Time) error {
	connection.mu.Lock()
	connection.deadlines++
	connection.mu.Unlock()
	return nil
}
func (connection *streamTestConnection) SetWriteDeadline(time.Time) error { return nil }

type streamAuthenticatedConnection struct {
	net.Conn
	identity PeerIdentity
	class    TrafficClass
}

func (connection *streamAuthenticatedConnection) PeerIdentity() PeerIdentity {
	return connection.identity
}
func (connection *streamAuthenticatedConnection) TrafficClass() TrafficClass {
	return connection.class
}

type zeroProgressWriter struct{}

func (zeroProgressWriter) Write([]byte) (int, error) { return 0, nil }

type shortStreamWriter struct {
	buffer  bytes.Buffer
	maximum int
	calls   int
}

func (writer *shortStreamWriter) Write(buffer []byte) (int, error) {
	writer.calls++
	if len(buffer) > writer.maximum {
		buffer = buffer[:writer.maximum]
	}
	return writer.buffer.Write(buffer)
}
