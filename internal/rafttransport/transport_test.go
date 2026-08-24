package rafttransport

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/raftmember"
	pb "go.etcd.io/raft/v3/raftpb"
)

type ordinaryDialFunc func(context.Context, NodeID) (PeerConnection, error)

func (dial ordinaryDialFunc) DialOrdinary(
	ctx context.Context,
	node NodeID,
) (PeerConnection, error) {
	return dial(ctx, node)
}

type transportTestFixture struct {
	group    raftmember.GroupKey
	registry *StaticRegistry
	members  []Member
	local    Member
	remote   [2]Member
}

func newTransportTestFixture(t testing.TB) transportTestFixture {
	t.Helper()
	group := testGroup(101)
	members := []Member{
		{Group: group, ReplicaSetVersion: 1, MemberID: 11, Node: testNode(1), Role: MemberVoter},
		{Group: group, ReplicaSetVersion: 1, MemberID: 12, Node: testNode(2), Role: MemberVoter},
		{Group: group, ReplicaSetVersion: 1, MemberID: 13, Node: testNode(3), Role: MemberVoter},
	}
	registry, err := NewStaticRegistry(
		members[2].Node, members, Limits{MaxGroups: 1, MaxMembers: len(members)},
	)
	if err != nil {
		t.Fatal(err)
	}
	return transportTestFixture{
		group: group, registry: registry, members: members, local: members[2],
		remote: [2]Member{members[0], members[1]},
	}
}

func (fixture transportTestFixture) outbound(
	to int,
	ordinal uint64,
) raftmember.OutboundMessage {
	message := frameTestMessage(
		pb.MsgHeartbeat, fixture.local.MemberID, fixture.remote[to].MemberID,
	)
	message.Index = frameU64(ordinal)
	return raftmember.OutboundMessage{
		Group: fixture.group, From: fixture.local.MemberID,
		To: fixture.remote[to].MemberID, Message: message,
	}
}

func transportTestOptions(
	fixture transportTestFixture,
	dialer OrdinaryDialer,
) OrdinaryTransportOptions {
	return OrdinaryTransportOptions{
		Registry: fixture.registry,
		Peers:    []NodeID{fixture.remote[0].Node, fixture.remote[1].Node},
		Dialer:   dialer,
		Queue: QueueLimits{
			PerPeerFrames: 8, PerPeerBytes: 1 << 20,
			GlobalFrames: 16, GlobalBytes: 2 << 20,
		},
		Coalesce: CoalesceLimits{
			MaxFrames: 4, MaxBytes: 1 << 20,
			RetainedBytes: DefaultRetainedFrameBytes,
		},
		Wait: func(ctx context.Context, _ time.Duration, _ <-chan struct{}) error {
			select {
			case <-ctx.Done():
				return context.Cause(ctx)
			default:
				return nil
			}
		},
		Backoff: func(failures uint32) time.Duration {
			return time.Duration(failures) * time.Millisecond
		},
		MaxReconnectDelay:  time.Second,
		WriteDeadline:      func() time.Time { return time.Now().Add(time.Minute) },
		RetainedFrameBytes: DefaultRetainedFrameBytes,
	}
}

func runTransportTest(
	t testing.TB,
	transport *OrdinaryTransport,
) (context.CancelFunc, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- transport.Run(ctx) }()
	transportTestEventually(t, func() bool {
		return transport.state.Load() == transportRunning
	})
	return cancel, done
}

func stopTransportTest(
	t testing.TB,
	transport *OrdinaryTransport,
	cancel context.CancelFunc,
	done <-chan error,
) {
	t.Helper()
	cancel()
	_ = transport.Close()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("transport worker leaked")
	}
}

func transportTestEventually(t testing.TB, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("condition did not become true")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestOrdinaryTransportBoundsFixedRingAndRetainedScratchGeometry(t *testing.T) {
	fixture := newTransportTestFixture(t)
	dialer := ordinaryDialFunc(func(context.Context, NodeID) (PeerConnection, error) {
		return nil, io.ErrClosedPipe
	})
	valid := transportTestOptions(fixture, dialer)
	valid.Peers = valid.Peers[:1]
	valid.Queue.PerPeerFrames = AbsoluteMaxQueuedFrames
	valid.Queue.GlobalFrames = AbsoluteMaxQueuedFrames
	transport, err := NewOrdinaryTransport(valid)
	if err != nil {
		t.Fatalf("maximum fixed-ring geometry: %v", err)
	}
	_ = transport.Close()

	invalidRing := transportTestOptions(fixture, dialer)
	invalidRing.Peers = make([]NodeID, AbsoluteMaxTransportPeers)
	invalidRing.Queue.PerPeerFrames = AbsoluteMaxQueuedFrames
	invalidRing.Queue.GlobalFrames = AbsoluteMaxQueuedFrames
	if _, err := NewOrdinaryTransport(invalidRing); !errors.Is(err, ErrInvalidTransport) {
		t.Fatalf("oversized fixed-ring geometry error = %v", err)
	}
	if got := testing.AllocsPerRun(1000, func() {
		_, _ = NewOrdinaryTransport(invalidRing)
	}); got != 0 {
		t.Fatalf("oversized fixed-ring rejection allocations = %v, want 0", got)
	}

	invalidScratch := transportTestOptions(fixture, dialer)
	retained := int(AbsoluteMaxRetainedCoalesceBytes/2) + 1
	invalidScratch.Coalesce.MaxBytes = retained
	invalidScratch.Coalesce.RetainedBytes = retained
	invalidScratch.Queue.PerPeerBytes = int64(retained)
	invalidScratch.Queue.GlobalBytes = int64(retained) * 2
	if _, err := NewOrdinaryTransport(invalidScratch); !errors.Is(err, ErrInvalidTransport) {
		t.Fatalf("oversized retained-scratch geometry error = %v", err)
	}
}

func TestOrdinaryTransportFailsFastAtPerPeerAndGlobalBounds(t *testing.T) {
	fixture := newTransportTestFixture(t)
	frame, _, err := fixture.registry.EncodeOutbound(nil, fixture.outbound(0, 1))
	if err != nil {
		t.Fatal(err)
	}
	frameBytes := int64(len(frame))
	dialer := ordinaryDialFunc(func(context.Context, NodeID) (PeerConnection, error) {
		return nil, io.ErrClosedPipe
	})
	options := transportTestOptions(fixture, dialer)
	options.Queue = QueueLimits{
		PerPeerFrames: 2, PerPeerBytes: frameBytes * 2,
		GlobalFrames: 4, GlobalBytes: frameBytes * 3,
	}
	options.Coalesce.MaxFrames = 2
	options.Coalesce.MaxBytes = int(frameBytes+StreamRecordHeaderBytes) * 2
	options.Coalesce.RetainedBytes = options.Coalesce.MaxBytes
	options.Wait = func(ctx context.Context, _ time.Duration, _ <-chan struct{}) error {
		<-ctx.Done()
		return context.Cause(ctx)
	}
	transport, err := NewOrdinaryTransport(options)
	if err != nil {
		t.Fatal(err)
	}
	cancel, done := runTransportTest(t, transport)
	defer stopTransportTest(t, transport, cancel, done)

	if err := transport.Send(fixture.outbound(0, 1)); err != nil {
		t.Fatal(err)
	}
	if err := transport.Send(fixture.outbound(0, 2)); err != nil {
		t.Fatal(err)
	}
	if err := transport.Send(fixture.outbound(0, 3)); !errors.Is(err, ErrBackpressure) {
		t.Fatalf("per-peer error = %v", err)
	}
	if err := transport.Send(fixture.outbound(1, 4)); err != nil {
		t.Fatal(err)
	}
	if err := transport.Send(fixture.outbound(1, 5)); !errors.Is(err, ErrBackpressure) {
		t.Fatalf("global error = %v", err)
	}
	if frames, _ := transport.GlobalQueueStats(); frames != 3 {
		t.Fatalf("global frames = %d, want 3", frames)
	}
}

func TestOrdinaryTransportReconnectUsesInjectedBackoffAndRetainsFrame(t *testing.T) {
	fixture := newTransportTestFixture(t)
	connection := newTransportTestConnection(fixture.remote[0].Node)
	var attempts atomic.Uint32
	dialer := ordinaryDialFunc(func(_ context.Context, node NodeID) (PeerConnection, error) {
		if node != fixture.remote[0].Node {
			return newTransportTestConnection(node), nil
		}
		if attempts.Add(1) <= 2 {
			return nil, io.ErrClosedPipe
		}
		return connection, nil
	})
	options := transportTestOptions(fixture, dialer)
	var delayMu sync.Mutex
	var delays []time.Duration
	options.Wait = func(ctx context.Context, delay time.Duration, _ <-chan struct{}) error {
		delayMu.Lock()
		delays = append(delays, delay)
		delayMu.Unlock()
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		default:
			return nil
		}
	}
	options.Backoff = func(failures uint32) time.Duration {
		return time.Duration(failures) * 10 * time.Millisecond
	}
	options.MaxReconnectDelay = 15 * time.Millisecond
	transport, err := NewOrdinaryTransport(options)
	if err != nil {
		t.Fatal(err)
	}
	cancel, done := runTransportTest(t, transport)
	defer stopTransportTest(t, transport, cancel, done)
	if err := transport.Send(fixture.outbound(0, 9)); err != nil {
		t.Fatal(err)
	}
	transportTestEventually(t, func() bool {
		stats, _ := transport.Stats(fixture.remote[0].Node)
		return stats.SentFrames == 1
	})
	stats, _ := transport.Stats(fixture.remote[0].Node)
	if stats.DialAttempts != 3 || stats.DialFailures != 2 || stats.Connections != 1 {
		t.Fatalf("reconnect stats = %+v", stats)
	}
	delayMu.Lock()
	gotDelays := append([]time.Duration(nil), delays...)
	delayMu.Unlock()
	if len(gotDelays) != 2 || gotDelays[0] != 10*time.Millisecond ||
		gotDelays[1] != 15*time.Millisecond {
		t.Fatalf("backoff delays = %v", gotDelays)
	}
	if records := transportTestRecords(t, connection.writtenBytes()); len(records) != 1 {
		t.Fatalf("written records = %d", len(records))
	}
}

func TestOrdinaryTransportHandlesPartialWritesAndRetriesFailedWrite(t *testing.T) {
	fixture := newTransportTestFixture(t)
	failed := newTransportTestConnection(fixture.remote[0].Node)
	failed.maxWrite = 3
	failed.failAfter = 17
	succeeded := newTransportTestConnection(fixture.remote[0].Node)
	succeeded.maxWrite = 2
	var dials atomic.Uint32
	dialer := ordinaryDialFunc(func(_ context.Context, node NodeID) (PeerConnection, error) {
		if node != fixture.remote[0].Node {
			return newTransportTestConnection(node), nil
		}
		if dials.Add(1) == 1 {
			return failed, nil
		}
		return succeeded, nil
	})
	transport, err := NewOrdinaryTransport(transportTestOptions(fixture, dialer))
	if err != nil {
		t.Fatal(err)
	}
	cancel, done := runTransportTest(t, transport)
	defer stopTransportTest(t, transport, cancel, done)
	if err := transport.Send(fixture.outbound(0, 17)); err != nil {
		t.Fatal(err)
	}
	transportTestEventually(t, func() bool {
		stats, _ := transport.Stats(fixture.remote[0].Node)
		return stats.SentFrames == 1
	})
	stats, _ := transport.Stats(fixture.remote[0].Node)
	if stats.WriteFailures != 1 || stats.Connections != 2 {
		t.Fatalf("write retry stats = %+v", stats)
	}
	if len(failed.writtenBytes()) != failed.failAfter {
		t.Fatalf("failed stream bytes = %d, want %d", len(failed.writtenBytes()), failed.failAfter)
	}
	if records := transportTestRecords(t, succeeded.writtenBytes()); len(records) != 1 {
		t.Fatalf("successful records = %d", len(records))
	}
}

func TestOrdinaryTransportCoalescesInOrderWithinHardCount(t *testing.T) {
	fixture := newTransportTestFixture(t)
	connection := newTransportTestConnection(fixture.remote[0].Node)
	dialGate := make(chan struct{})
	dialer := ordinaryDialFunc(func(ctx context.Context, node NodeID) (PeerConnection, error) {
		if node != fixture.remote[0].Node {
			return newTransportTestConnection(node), nil
		}
		select {
		case <-ctx.Done():
			return nil, context.Cause(ctx)
		case <-dialGate:
			return connection, nil
		}
	})
	options := transportTestOptions(fixture, dialer)
	options.Coalesce.MaxFrames = 3
	transport, err := NewOrdinaryTransport(options)
	if err != nil {
		t.Fatal(err)
	}
	cancel, done := runTransportTest(t, transport)
	defer stopTransportTest(t, transport, cancel, done)
	for ordinal := uint64(1); ordinal <= 5; ordinal++ {
		if err := transport.Send(fixture.outbound(0, ordinal)); err != nil {
			t.Fatal(err)
		}
	}
	close(dialGate)
	transportTestEventually(t, func() bool {
		stats, _ := transport.Stats(fixture.remote[0].Node)
		return stats.SentFrames == 5
	})
	if calls := connection.writeCallCount(); calls != 2 {
		t.Fatalf("write calls = %d, want 2", calls)
	}
	records := transportTestRecords(t, connection.writtenBytes())
	if len(records) != 5 {
		t.Fatalf("record count = %d", len(records))
	}
	receiver, err := NewStaticRegistry(
		fixture.remote[0].Node, fixture.members,
		Limits{MaxGroups: 1, MaxMembers: len(fixture.members)},
	)
	if err != nil {
		t.Fatal(err)
	}
	for index, frame := range records {
		inbound, err := receiver.DecodeInbound(fixture.local.Node, frame)
		if err != nil {
			t.Fatal(err)
		}
		if inbound.Message.GetIndex() != uint64(index+1) {
			t.Fatalf("record %d index = %d", index, inbound.Message.GetIndex())
		}
	}
}

func TestOrdinaryTransportCoalesceDelayIsInjectedAndWakesForNextFrame(t *testing.T) {
	fixture := newTransportTestFixture(t)
	connection := newTransportTestConnection(fixture.remote[0].Node)
	dialer := ordinaryDialFunc(func(_ context.Context, node NodeID) (PeerConnection, error) {
		if node == fixture.remote[0].Node {
			return connection, nil
		}
		return newTransportTestConnection(node), nil
	})
	options := transportTestOptions(fixture, dialer)
	options.Coalesce.MaxDelay = 7 * time.Millisecond
	waiting := make(chan time.Duration, 1)
	options.Wait = func(ctx context.Context, delay time.Duration, wake <-chan struct{}) error {
		waiting <- delay
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-wake:
			return nil
		}
	}
	transport, err := NewOrdinaryTransport(options)
	if err != nil {
		t.Fatal(err)
	}
	cancel, done := runTransportTest(t, transport)
	defer stopTransportTest(t, transport, cancel, done)
	if err := transport.Send(fixture.outbound(0, 1)); err != nil {
		t.Fatal(err)
	}
	select {
	case delay := <-waiting:
		if delay != options.Coalesce.MaxDelay {
			t.Fatalf("coalesce delay = %v, want %v", delay, options.Coalesce.MaxDelay)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("coalesce wait was not invoked")
	}
	if err := transport.Send(fixture.outbound(0, 2)); err != nil {
		t.Fatal(err)
	}
	transportTestEventually(t, func() bool {
		stats, _ := transport.Stats(fixture.remote[0].Node)
		return stats.SentFrames == 2
	})
	if calls := connection.writeCallCount(); calls != 1 {
		t.Fatalf("coalesced write calls = %d, want 1", calls)
	}
}

func TestOrdinaryTransportCoalescingHonorsHardByteBound(t *testing.T) {
	fixture := newTransportTestFixture(t)
	frame, _, err := fixture.registry.EncodeOutbound(nil, fixture.outbound(0, 1))
	if err != nil {
		t.Fatal(err)
	}
	connection := newTransportTestConnection(fixture.remote[0].Node)
	dialGate := make(chan struct{})
	dialer := ordinaryDialFunc(func(ctx context.Context, node NodeID) (PeerConnection, error) {
		if node != fixture.remote[0].Node {
			return newTransportTestConnection(node), nil
		}
		select {
		case <-ctx.Done():
			return nil, context.Cause(ctx)
		case <-dialGate:
			return connection, nil
		}
	})
	options := transportTestOptions(fixture, dialer)
	options.Coalesce.MaxBytes = len(frame) + StreamRecordHeaderBytes
	options.Coalesce.RetainedBytes = options.Coalesce.MaxBytes
	transport, err := NewOrdinaryTransport(options)
	if err != nil {
		t.Fatal(err)
	}
	cancel, done := runTransportTest(t, transport)
	defer stopTransportTest(t, transport, cancel, done)
	if err := transport.Send(fixture.outbound(0, 1)); err != nil {
		t.Fatal(err)
	}
	if err := transport.Send(fixture.outbound(0, 2)); err != nil {
		t.Fatal(err)
	}
	close(dialGate)
	transportTestEventually(t, func() bool {
		stats, _ := transport.Stats(fixture.remote[0].Node)
		return stats.SentFrames == 2
	})
	if calls := connection.writeCallCount(); calls != 2 {
		t.Fatalf("byte-bounded write calls = %d, want 2", calls)
	}
}

func TestOrdinaryTransportPeerWritersAreIndependent(t *testing.T) {
	fixture := newTransportTestFixture(t)
	blocked := newTransportTestConnection(fixture.remote[0].Node)
	blocked.writeGate = make(chan struct{})
	ready := newTransportTestConnection(fixture.remote[1].Node)
	dialer := ordinaryDialFunc(func(_ context.Context, node NodeID) (PeerConnection, error) {
		if node == fixture.remote[0].Node {
			return blocked, nil
		}
		return ready, nil
	})
	transport, err := NewOrdinaryTransport(transportTestOptions(fixture, dialer))
	if err != nil {
		t.Fatal(err)
	}
	cancel, done := runTransportTest(t, transport)
	defer stopTransportTest(t, transport, cancel, done)
	if err := transport.Send(fixture.outbound(0, 1)); err != nil {
		t.Fatal(err)
	}
	if err := transport.Send(fixture.outbound(1, 2)); err != nil {
		t.Fatal(err)
	}
	transportTestEventually(t, func() bool {
		stats, _ := transport.Stats(fixture.remote[1].Node)
		return stats.SentFrames == 1
	})
	if stats, _ := transport.Stats(fixture.remote[0].Node); stats.SentFrames != 0 {
		t.Fatalf("blocked peer sent frames = %d", stats.SentFrames)
	}
	close(blocked.writeGate)
	transportTestEventually(t, func() bool {
		stats, _ := transport.Stats(fixture.remote[0].Node)
		return stats.SentFrames == 1
	})
}

func TestOrdinaryTransportConcurrentSendDrainsWithinExactBounds(t *testing.T) {
	fixture := newTransportTestFixture(t)
	connections := [2]*transportTestConnection{
		newTransportTestConnection(fixture.remote[0].Node),
		newTransportTestConnection(fixture.remote[1].Node),
	}
	dialer := ordinaryDialFunc(func(_ context.Context, node NodeID) (PeerConnection, error) {
		if node == fixture.remote[0].Node {
			return connections[0], nil
		}
		return connections[1], nil
	})
	options := transportTestOptions(fixture, dialer)
	options.Queue.PerPeerFrames = 512
	options.Queue.GlobalFrames = 1024
	transport, err := NewOrdinaryTransport(options)
	if err != nil {
		t.Fatal(err)
	}
	cancel, done := runTransportTest(t, transport)
	defer stopTransportTest(t, transport, cancel, done)

	const (
		workers       = 8
		framesPerWork = 100
	)
	errorsFound := make(chan error, workers)
	var sends sync.WaitGroup
	sends.Add(workers)
	for worker := range workers {
		go func() {
			defer sends.Done()
			to := worker % len(fixture.remote)
			for ordinal := range framesPerWork {
				outbound := fixture.outbound(to, uint64(worker*framesPerWork+ordinal+1))
				for {
					err := transport.Send(outbound)
					if err == nil {
						break
					}
					if !errors.Is(err, ErrBackpressure) {
						errorsFound <- err
						return
					}
					runtime.Gosched()
				}
			}
		}()
	}
	sends.Wait()
	select {
	case err := <-errorsFound:
		t.Fatal(err)
	default:
	}
	transportTestEventually(t, func() bool {
		first, _ := transport.Stats(fixture.remote[0].Node)
		second, _ := transport.Stats(fixture.remote[1].Node)
		return first.SentFrames+second.SentFrames == workers*framesPerWork
	})
	if frames, bytes := transport.GlobalQueueStats(); frames != 0 || bytes != 0 {
		t.Fatalf("drained global queue = %d frames, %d bytes", frames, bytes)
	}
}

func TestOrdinaryTransportRejectsWrongAuthenticatedDialResult(t *testing.T) {
	fixture := newTransportTestFixture(t)
	var attempts atomic.Uint32
	valid := newTransportTestConnection(fixture.remote[0].Node)
	dialer := ordinaryDialFunc(func(_ context.Context, node NodeID) (PeerConnection, error) {
		if node != fixture.remote[0].Node {
			return newTransportTestConnection(node), nil
		}
		switch attempts.Add(1) {
		case 1:
			return newTransportTestConnection(testNode(99)), nil
		case 2:
			wrongClass := newTransportTestConnection(node)
			wrongClass.class = TrafficSnapshot
			return wrongClass, nil
		default:
			return valid, nil
		}
	})
	transport, err := NewOrdinaryTransport(transportTestOptions(fixture, dialer))
	if err != nil {
		t.Fatal(err)
	}
	cancel, done := runTransportTest(t, transport)
	defer stopTransportTest(t, transport, cancel, done)
	if err := transport.Send(fixture.outbound(0, 1)); err != nil {
		t.Fatal(err)
	}
	transportTestEventually(t, func() bool {
		stats, _ := transport.Stats(fixture.remote[0].Node)
		return stats.SentFrames == 1
	})
	stats, _ := transport.Stats(fixture.remote[0].Node)
	if stats.DialFailures != 2 || stats.DialAttempts != 3 {
		t.Fatalf("authentication retry stats = %+v", stats)
	}
}

type transportTestConnection struct {
	node  NodeID
	class TrafficClass

	mu         sync.Mutex
	written    bytes.Buffer
	writeCalls int
	maxWrite   int
	failAfter  int
	writeGate  chan struct{}
	closed     chan struct{}
	closeOnce  sync.Once
}

func newTransportTestConnection(node NodeID) *transportTestConnection {
	return &transportTestConnection{
		node: node, class: TrafficOrdinary, closed: make(chan struct{}),
	}
}

func (connection *transportTestConnection) PeerNode() NodeID { return connection.node }
func (connection *transportTestConnection) TrafficClass() TrafficClass {
	return connection.class
}
func (connection *transportTestConnection) Read([]byte) (int, error) { return 0, io.EOF }
func (connection *transportTestConnection) Write(buffer []byte) (int, error) {
	if connection.writeGate != nil {
		select {
		case <-connection.closed:
			return 0, net.ErrClosed
		case <-connection.writeGate:
		}
	}
	connection.mu.Lock()
	defer connection.mu.Unlock()
	connection.writeCalls++
	remaining := len(buffer)
	if connection.maxWrite > 0 && remaining > connection.maxWrite {
		remaining = connection.maxWrite
	}
	if connection.failAfter > 0 {
		untilFailure := connection.failAfter - connection.written.Len()
		if untilFailure <= 0 {
			return 0, io.ErrUnexpectedEOF
		}
		if remaining > untilFailure {
			remaining = untilFailure
		}
	}
	written, _ := connection.written.Write(buffer[:remaining])
	if connection.failAfter > 0 && connection.written.Len() >= connection.failAfter {
		return written, io.ErrUnexpectedEOF
	}
	return written, nil
}
func (connection *transportTestConnection) Close() error {
	connection.closeOnce.Do(func() { close(connection.closed) })
	return nil
}
func (connection *transportTestConnection) LocalAddr() net.Addr              { return transportTestAddr("local") }
func (connection *transportTestConnection) RemoteAddr() net.Addr             { return transportTestAddr("remote") }
func (connection *transportTestConnection) SetDeadline(time.Time) error      { return nil }
func (connection *transportTestConnection) SetReadDeadline(time.Time) error  { return nil }
func (connection *transportTestConnection) SetWriteDeadline(time.Time) error { return nil }

func (connection *transportTestConnection) writtenBytes() []byte {
	connection.mu.Lock()
	defer connection.mu.Unlock()
	return bytes.Clone(connection.written.Bytes())
}

func (connection *transportTestConnection) writeCallCount() int {
	connection.mu.Lock()
	defer connection.mu.Unlock()
	return connection.writeCalls
}

type transportTestAddr string

func (address transportTestAddr) Network() string { return "test" }
func (address transportTestAddr) String() string  { return string(address) }

func transportTestRecords(t testing.TB, stream []byte) [][]byte {
	t.Helper()
	var records [][]byte
	for len(stream) != 0 {
		if len(stream) < StreamRecordHeaderBytes {
			t.Fatalf("truncated stream header: %d", len(stream))
		}
		length := int(binary.BigEndian.Uint32(stream[:StreamRecordHeaderBytes]))
		stream = stream[StreamRecordHeaderBytes:]
		if length < FrameHeaderBytes || length > len(stream) {
			t.Fatalf("invalid stream record length %d with %d bytes", length, len(stream))
		}
		records = append(records, bytes.Clone(stream[:length]))
		stream = stream[length:]
	}
	return records
}
