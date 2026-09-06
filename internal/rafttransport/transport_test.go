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
	frameCapacity, _, _ := frameBufferCapacity(len(frame), DefaultRetainedFrameBytes)
	frameBytes := int64(frameCapacity)
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

func TestOrdinaryTransportReservationsBoundConcurrentPreEncodeOwnership(t *testing.T) {
	fixture := newTransportTestFixture(t)
	dialer := ordinaryDialFunc(func(ctx context.Context, _ NodeID) (PeerConnection, error) {
		<-ctx.Done()
		return nil, context.Cause(ctx)
	})
	options := transportTestOptions(fixture, dialer)
	options.Peers = options.Peers[:1]
	options.Queue.PerPeerFrames = 4
	options.Queue.GlobalFrames = 4
	options.Coalesce.MaxFrames = 4
	transport, err := NewOrdinaryTransport(options)
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{}, options.Queue.GlobalFrames)
	release := make(chan struct{})
	transport.beforeEncode = func() {
		entered <- struct{}{}
		<-release
	}
	cancel, done := runTransportTest(t, transport)

	const senders = 32
	results := make(chan error, senders)
	for ordinal := range senders {
		go func() {
			results <- transport.Send(fixture.outbound(0, uint64(ordinal+1)))
		}()
	}
	for range options.Queue.GlobalFrames {
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("reserved Send did not reach encode seam")
		}
	}
	plan, err := fixture.registry.preflightOutbound(fixture.outbound(0, 1))
	if err != nil {
		t.Fatal(err)
	}
	ownedSize, _, _ := framedFrameCapacity(plan.frameSize, options.RetainedFrameBytes)
	wantReservedBytes := int64(options.Queue.GlobalFrames * ownedSize)
	stats, err := transport.Stats(fixture.remote[0].Node)
	if err != nil {
		t.Fatal(err)
	}
	if stats.QueuedFrames != 0 || stats.ReservedFrames != options.Queue.GlobalFrames ||
		stats.ReservedBytes != wantReservedBytes || stats.DialAttempts != 0 {
		t.Fatalf("in-flight reservation stats = %+v", stats)
	}
	if frames, bytes := transport.GlobalQueueStats(); frames != options.Queue.GlobalFrames || bytes != stats.ReservedBytes {
		t.Fatalf("global reservation = %d/%d, peer = %+v", frames, bytes, stats)
	}
	ownedFrames, ownedBytes, freeFrames, closed := transport.frames.stats()
	if ownedFrames != options.Queue.GlobalFrames || ownedBytes > options.Queue.GlobalBytes ||
		freeFrames != 0 || closed {
		t.Fatalf("cache ownership = %d/%d free %d closed %v",
			ownedFrames, ownedBytes, freeFrames, closed)
	}
	assertTransportOwnershipInvariants(t, transport)

	close(release)
	succeeded, backpressured := 0, 0
	for range senders {
		switch err := <-results; {
		case err == nil:
			succeeded++
		case errors.Is(err, ErrBackpressure):
			backpressured++
		default:
			t.Fatalf("Send error = %v", err)
		}
	}
	if succeeded != options.Queue.GlobalFrames || backpressured != senders-succeeded {
		t.Fatalf("Send outcomes = %d success, %d backpressure", succeeded, backpressured)
	}
	stats, _ = transport.Stats(fixture.remote[0].Node)
	if stats.QueuedFrames != succeeded || stats.ReservedFrames != 0 || stats.ReservedBytes != 0 {
		t.Fatalf("published reservation stats = %+v", stats)
	}
	assertTransportOwnershipInvariants(t, transport)
	stopTransportTest(t, transport, cancel, done)
	ownedFrames, ownedBytes, freeFrames, closed = transport.frames.stats()
	if ownedFrames != 0 || ownedBytes != 0 || freeFrames != 0 || !closed {
		t.Fatalf("closed cache ownership = %d/%d free %d closed %v",
			ownedFrames, ownedBytes, freeFrames, closed)
	}
	assertTransportOwnershipInvariants(t, transport)
}

func TestOrdinaryTransportCloseWaitsForInFlightEncodeReservation(t *testing.T) {
	fixture := newTransportTestFixture(t)
	dialer := ordinaryDialFunc(func(ctx context.Context, _ NodeID) (PeerConnection, error) {
		<-ctx.Done()
		return nil, context.Cause(ctx)
	})
	options := transportTestOptions(fixture, dialer)
	options.Peers = options.Peers[:1]
	options.Queue.PerPeerFrames = 2
	options.Queue.GlobalFrames = 2
	options.Coalesce.MaxFrames = 2
	transport, err := NewOrdinaryTransport(options)
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	transport.beforeEncode = func() {
		entered <- struct{}{}
		<-release
	}
	cancel, done := runTransportTest(t, transport)
	defer cancel()
	sendDone := make(chan error, 1)
	go func() { sendDone <- transport.Send(fixture.outbound(0, 1)) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("Send did not reach encode seam")
	}
	if err := transport.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		t.Fatalf("Run returned before active Send unwound: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	stats, _ := transport.Stats(fixture.remote[0].Node)
	if stats.QueuedFrames != 0 || stats.ReservedFrames != 1 || stats.ReservedBytes <= 0 {
		t.Fatalf("closing reservation stats = %+v", stats)
	}
	if frames, bytes := transport.GlobalQueueStats(); frames != 1 || bytes != stats.ReservedBytes {
		t.Fatalf("closing global reservation = %d/%d, peer = %+v", frames, bytes, stats)
	}
	assertTransportOwnershipInvariants(t, transport)
	close(release)
	if err := <-sendDone; !errors.Is(err, ErrTransportClosed) {
		t.Fatalf("closing Send error = %v, want ErrTransportClosed", err)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not quiesce after reservation unwind")
	}
	stats, _ = transport.Stats(fixture.remote[0].Node)
	if stats.QueuedFrames != 0 || stats.QueuedBytes != 0 ||
		stats.ReservedFrames != 0 || stats.ReservedBytes != 0 {
		t.Fatalf("quiesced peer stats = %+v", stats)
	}
	if frames, bytes := transport.GlobalQueueStats(); frames != 0 || bytes != 0 {
		t.Fatalf("quiesced global reservation = %d/%d", frames, bytes)
	}
	ownedFrames, ownedBytes, freeFrames, closed := transport.frames.stats()
	if ownedFrames != 0 || ownedBytes != 0 || freeFrames != 0 || !closed {
		t.Fatalf("quiesced cache = %d/%d free %d closed %v",
			ownedFrames, ownedBytes, freeFrames, closed)
	}
	assertTransportOwnershipInvariants(t, transport)
}

func TestOrdinaryTransportEncodeFailureUnwindsReservationAndOwnership(t *testing.T) {
	fixture := newTransportTestFixture(t)
	dialer := ordinaryDialFunc(func(context.Context, NodeID) (PeerConnection, error) {
		return nil, io.ErrClosedPipe
	})
	transport, err := NewOrdinaryTransport(transportTestOptions(fixture, dialer))
	if err != nil {
		t.Fatal(err)
	}
	transport.state.Store(transportRunning)
	outbound := fixture.outbound(0, 1)
	transport.beforeEncode = func() {
		outbound.Message.Context = []byte("changed after exact preflight")
	}
	if err := transport.Send(outbound); !errors.Is(err, ErrInvalidFrame) {
		t.Fatalf("encode error = %v, want ErrInvalidFrame", err)
	}
	stats, _ := transport.Stats(fixture.remote[0].Node)
	if stats.QueuedFrames != 0 || stats.QueuedBytes != 0 ||
		stats.ReservedFrames != 0 || stats.ReservedBytes != 0 {
		t.Fatalf("failed encode peer stats = %+v", stats)
	}
	if frames, bytes := transport.GlobalQueueStats(); frames != 0 || bytes != 0 {
		t.Fatalf("failed encode global ownership = %d/%d", frames, bytes)
	}
	ownedFrames, ownedBytes, freeFrames, closed := transport.frames.stats()
	if ownedFrames != 1 || ownedBytes <= 0 || freeFrames != 1 || closed {
		t.Fatalf("failed encode cache = %d/%d free %d closed %v",
			ownedFrames, ownedBytes, freeFrames, closed)
	}
	transport.state.Store(transportReady)
	if err := transport.Close(); err != nil {
		t.Fatal(err)
	}
	ownedFrames, ownedBytes, freeFrames, closed = transport.frames.stats()
	if ownedFrames != 0 || ownedBytes != 0 || freeFrames != 0 || !closed {
		t.Fatalf("closed failed-encode cache = %d/%d free %d closed %v",
			ownedFrames, ownedBytes, freeFrames, closed)
	}
}

func TestOrdinaryTransportReturnsBuffersOutsideGlobalQueueLock(t *testing.T) {
	fixture := newTransportTestFixture(t)
	dialer := ordinaryDialFunc(func(context.Context, NodeID) (PeerConnection, error) {
		return nil, io.ErrClosedPipe
	})
	transport, err := NewOrdinaryTransport(transportTestOptions(fixture, dialer))
	if err != nil {
		t.Fatal(err)
	}
	transport.state.Store(transportRunning)
	if err := transport.Send(fixture.outbound(0, 1)); err != nil {
		t.Fatal(err)
	}
	peer := transport.byNode[fixture.remote[0].Node]
	entered := make(chan struct{})
	release := make(chan struct{})
	transport.beforeFrameReturn = func() {
		close(entered)
		<-release
	}
	committed := make(chan struct{})
	go func() {
		transport.commitPeerBatch(peer, 1)
		close(committed)
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("commit did not reach frame-return seam")
	}
	statsDone := make(chan struct{})
	go func() {
		stats, _ := transport.Stats(peer.node)
		frames, bytes := transport.GlobalQueueStats()
		if stats.QueuedFrames != 0 || frames != 0 || bytes != 0 {
			t.Errorf("queue while frame return blocked = %+v, %d/%d", stats, frames, bytes)
		}
		close(statsDone)
	}()
	select {
	case <-statsDone:
	case <-time.After(time.Second):
		t.Fatal("global queue lock remained held during frame return")
	}
	close(release)
	select {
	case <-committed:
	case <-time.After(5 * time.Second):
		t.Fatal("commit did not finish")
	}
	transport.state.Store(transportReady)
	if err := transport.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestBoundedFrameBufferCacheBestFitAndHardOwnership(t *testing.T) {
	cache := newBoundedFrameBufferCache(3, 1024, 1024)
	first, err := cache.get(400)
	if err != nil {
		t.Fatal(err)
	}
	second, err := cache.get(200)
	if err != nil {
		t.Fatal(err)
	}
	third, err := cache.get(100)
	if err != nil {
		t.Fatal(err)
	}
	cache.put(first)
	cache.put(second)
	cache.put(third)
	best, err := cache.get(150)
	if err != nil {
		t.Fatal(err)
	}
	if cap(best.bytes) != 256 {
		t.Fatalf("best-fit class capacity = %d, want 256", cap(best.bytes))
	}
	next, err := cache.get(350)
	if err != nil {
		t.Fatal(err)
	}
	if cap(next.bytes) != 512 {
		t.Fatalf("next class capacity = %d, want 512", cap(next.bytes))
	}
	if _, err := cache.get(500); !errors.Is(err, ErrBackpressure) {
		t.Fatalf("physical ownership error = %v, want ErrBackpressure", err)
	}
	ownedFrames, ownedBytes, _, _ := cache.stats()
	if ownedFrames > 3 || ownedBytes > 1024 {
		t.Fatalf("cache exceeded ownership = %d/%d", ownedFrames, ownedBytes)
	}
	cache.put(best)
	cache.put(next)
	cache.close()
	ownedFrames, ownedBytes, freeFrames, closed := cache.stats()
	if ownedFrames != 0 || ownedBytes != 0 || freeFrames != 0 || !closed {
		t.Fatalf("closed cache = %d/%d free %d closed %v",
			ownedFrames, ownedBytes, freeFrames, closed)
	}
}

func TestFrameBufferCapacityClassesBoundRetainedOverhead(t *testing.T) {
	tests := []struct {
		size, retain int
		capacity     int
		cacheable    bool
	}{
		{size: FrameHeaderBytes, retain: DefaultRetainedFrameBytes, capacity: 256, cacheable: true},
		{size: 256, retain: DefaultRetainedFrameBytes, capacity: 256, cacheable: true},
		{size: 257, retain: DefaultRetainedFrameBytes, capacity: 512, cacheable: true},
		{size: 400, retain: 500, capacity: 500, cacheable: true},
		{size: 501, retain: 500, capacity: 501, cacheable: false},
	}
	for _, test := range tests {
		capacity, _, cacheable := frameBufferCapacity(test.size, test.retain)
		if capacity != test.capacity || cacheable != test.cacheable {
			t.Fatalf("capacity(%d, %d) = %d/%v, want %d/%v",
				test.size, test.retain, capacity, cacheable,
				test.capacity, test.cacheable)
		}
	}
	for size := FrameHeaderBytes; size <= DefaultRetainedFrameBytes; size++ {
		capacity, _, cacheable := frameBufferCapacity(size, DefaultRetainedFrameBytes)
		if !cacheable || capacity < size || capacity >= size*2 {
			t.Fatalf("retained capacity(%d) = %d/%v", size, capacity, cacheable)
		}
	}
}

func TestFramedFrameCapacityChargesStreamPrefix(t *testing.T) {
	capacity, class, cacheable := framedFrameCapacity(252, 256)
	if capacity != 256 || class != 8 || !cacheable {
		t.Fatalf("framed capacity at retained boundary = %d/%d/%v, want 256/8/true",
			capacity, class, cacheable)
	}
	capacity, class, cacheable = framedFrameCapacity(253, 256)
	if capacity != 257 || class != 0 || cacheable {
		t.Fatalf("framed capacity over retained boundary = %d/%d/%v, want 257/0/false",
			capacity, class, cacheable)
	}
	cache := newBoundedFrameBufferCache(1, 256, 256)
	buffer, err := cache.getFramed(252)
	if err != nil {
		t.Fatal(err)
	}
	if !buffer.framed || len(buffer.bytes) != 252 || len(buffer.record) != 256 ||
		cap(buffer.record) != 256 || buffer.ownedCapacity() != 256 {
		t.Fatalf("framed storage = framed %v, body %d, record %d/%d, owned %d",
			buffer.framed, len(buffer.bytes), len(buffer.record), cap(buffer.record),
			buffer.ownedCapacity())
	}
	if &buffer.record[StreamRecordHeaderBytes] != &buffer.bytes[0] {
		t.Fatal("framed body does not follow reserved record prefix")
	}
	cache.put(buffer)
	ownedFrames, ownedBytes, freeFrames, closed := cache.stats()
	if ownedFrames != 1 || ownedBytes != 256 || freeFrames != 1 || closed {
		t.Fatalf("framed cache after return = %d/%d free %d closed %v",
			ownedFrames, ownedBytes, freeFrames, closed)
	}
	cache.close()
}

func TestFramedFrameCacheReusesAndEvictsByReservedCapacity(t *testing.T) {
	cache := newBoundedFrameBufferCache(2, 512, 256)
	first, err := cache.getFramed(252)
	if err != nil {
		t.Fatal(err)
	}
	cache.put(first)
	reused, err := cache.getFramed(252)
	if err != nil {
		t.Fatal(err)
	}
	if reused != first {
		t.Fatal("framed cache did not reuse the retained record")
	}
	cache.put(reused)
	activeFirst, err := cache.getFramed(252)
	if err != nil {
		t.Fatal(err)
	}
	activeSecond, err := cache.getFramed(252)
	if err != nil {
		t.Fatal(err)
	}
	cache.put(activeFirst)
	cache.put(activeSecond)
	third, err := cache.getFramed(124)
	if err != nil {
		t.Fatal(err)
	}
	if activeFirst.ownedCapacity() != 0 && activeSecond.ownedCapacity() != 0 {
		t.Fatal("different framed size did not evict a retained class")
	}
	if third.ownedCapacity() != 128 {
		t.Fatalf("evicted-class replacement capacity = %d, want 128", third.ownedCapacity())
	}
	cache.put(third)
	uncached, err := cache.getFramed(253)
	if err != nil {
		t.Fatal(err)
	}
	if uncached.ownedCapacity() != 257 {
		t.Fatalf("over-retain framed capacity = %d, want 257", uncached.ownedCapacity())
	}
	cache.put(uncached)
	ownedFrames, ownedBytes, freeFrames, closed := cache.stats()
	if ownedFrames != 1 || ownedBytes != 128 || freeFrames != 1 || closed {
		t.Fatalf("framed cache ownership after eviction = %d/%d free %d closed %v",
			ownedFrames, ownedBytes, freeFrames, closed)
	}
	cache.close()
}

func TestOrdinaryTransportSingletonBatchUsesReservedRecord(t *testing.T) {
	fixture := newTransportTestFixture(t)
	dialer := ordinaryDialFunc(func(context.Context, NodeID) (PeerConnection, error) {
		return nil, io.ErrClosedPipe
	})
	transport, err := NewOrdinaryTransport(transportTestOptions(fixture, dialer))
	if err != nil {
		t.Fatal(err)
	}
	transport.state.Store(transportRunning)
	if err := transport.Send(fixture.outbound(0, 1)); err != nil {
		t.Fatal(err)
	}
	peer := transport.byNode[fixture.remote[0].Node]
	transport.mu.Lock()
	storage := peer.queue[peer.head].buffer
	transport.mu.Unlock()
	batch, frames := transport.buildPeerBatch(peer)
	if frames != 1 || storage == nil || peer.directFrame != storage {
		t.Fatalf("singleton batch = frames %d, storage %p, direct %p",
			frames, storage, peer.directFrame)
	}
	if len(batch) != StreamRecordHeaderBytes+len(storage.bytes) ||
		&batch[0] != &storage.record[0] ||
		&batch[StreamRecordHeaderBytes] != &storage.bytes[0] {
		t.Fatal("singleton batch did not use the owned framed record")
	}
	if got := binary.BigEndian.Uint32(batch[:StreamRecordHeaderBytes]); got != uint32(len(storage.bytes)) {
		t.Fatalf("singleton record length = %d, want %d", got, len(storage.bytes))
	}
	if len(peer.writeBuffer) != 0 {
		t.Fatalf("singleton batch allocated scratch of %d bytes", len(peer.writeBuffer))
	}
	transport.releasePeerBatch(peer)
	if peer.directFrame != nil {
		t.Fatal("singleton direct frame retained after release")
	}
	transport.commitPeerBatch(peer, frames)
	transport.state.Store(transportReady)
	if err := transport.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestOrdinaryTransportReconnectUsesInjectedBackoffAndRetainsFrame(t *testing.T) {
	fixture := newTransportTestFixture(t)
	connection := newTransportTestConnection(fixture.registry, fixture.remote[0].Node)
	var attempts atomic.Uint32
	dialer := ordinaryDialFunc(func(_ context.Context, node NodeID) (PeerConnection, error) {
		if node != fixture.remote[0].Node {
			return newTransportTestConnection(fixture.registry, node), nil
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
	outbound := fixture.outbound(0, 17)
	expected, _, err := fixture.registry.EncodeOutbound(nil, outbound)
	if err != nil {
		t.Fatal(err)
	}
	failed := newTransportTestConnection(fixture.registry, fixture.remote[0].Node)
	failed.maxWrite = 3
	failed.failAfter = 17
	succeeded := newTransportTestConnection(fixture.registry, fixture.remote[0].Node)
	succeeded.maxWrite = 2
	var dials atomic.Uint32
	dialer := ordinaryDialFunc(func(_ context.Context, node NodeID) (PeerConnection, error) {
		if node != fixture.remote[0].Node {
			return newTransportTestConnection(fixture.registry, node), nil
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
	if err := transport.Send(outbound); err != nil {
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
	} else if !bytes.Equal(records[0], expected) {
		t.Fatal("successful retry record differs from the original frame")
	}
}

func TestOrdinaryTransportCoalescesInOrderWithinHardCount(t *testing.T) {
	fixture := newTransportTestFixture(t)
	connection := newTransportTestConnection(fixture.registry, fixture.remote[0].Node)
	dialGate := make(chan struct{})
	dialer := ordinaryDialFunc(func(ctx context.Context, node NodeID) (PeerConnection, error) {
		if node != fixture.remote[0].Node {
			return newTransportTestConnection(fixture.registry, node), nil
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
		inbound, err := receiver.DecodeInbound(
			testPeerIdentity(receiver, fixture.local.Node), frame,
		)
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
	connection := newTransportTestConnection(fixture.registry, fixture.remote[0].Node)
	dialer := ordinaryDialFunc(func(_ context.Context, node NodeID) (PeerConnection, error) {
		if node == fixture.remote[0].Node {
			return connection, nil
		}
		return newTransportTestConnection(fixture.registry, node), nil
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
	connection := newTransportTestConnection(fixture.registry, fixture.remote[0].Node)
	dialGate := make(chan struct{})
	dialer := ordinaryDialFunc(func(ctx context.Context, node NodeID) (PeerConnection, error) {
		if node != fixture.remote[0].Node {
			return newTransportTestConnection(fixture.registry, node), nil
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
	blocked := newTransportTestConnection(fixture.registry, fixture.remote[0].Node)
	blocked.writeGate = make(chan struct{})
	ready := newTransportTestConnection(fixture.registry, fixture.remote[1].Node)
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
		newTransportTestConnection(fixture.registry, fixture.remote[0].Node),
		newTransportTestConnection(fixture.registry, fixture.remote[1].Node),
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
	valid := newTransportTestConnection(fixture.registry, fixture.remote[0].Node)
	dialer := ordinaryDialFunc(func(_ context.Context, node NodeID) (PeerConnection, error) {
		if node != fixture.remote[0].Node {
			return newTransportTestConnection(fixture.registry, node), nil
		}
		switch attempts.Add(1) {
		case 1:
			return newTransportTestConnection(fixture.registry, testNode(99)), nil
		case 2:
			wrongDomain := newTransportTestConnection(fixture.registry, node)
			wrongDomain.identity.TrustDomain.ClusterID[0]++
			return wrongDomain, nil
		case 3:
			wrongClass := newTransportTestConnection(fixture.registry, node)
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
	if stats.DialFailures != 3 || stats.DialAttempts != 4 {
		t.Fatalf("authentication retry stats = %+v", stats)
	}
}

type transportTestConnection struct {
	identity PeerIdentity
	class    TrafficClass

	mu         sync.Mutex
	written    bytes.Buffer
	writeCalls int
	maxWrite   int
	failAfter  int
	writeGate  chan struct{}
	closed     chan struct{}
	closeOnce  sync.Once
}

func newTransportTestConnection(
	registry *StaticRegistry,
	node NodeID,
) *transportTestConnection {
	return &transportTestConnection{
		identity: testPeerIdentity(registry, node),
		class:    TrafficOrdinary, closed: make(chan struct{}),
	}
}

func (connection *transportTestConnection) PeerIdentity() PeerIdentity {
	return connection.identity
}
func (connection *transportTestConnection) TrafficClass() TrafficClass {
	return connection.class
}
func (connection *transportTestConnection) Read([]byte) (int, error) {
	<-connection.closed
	return 0, io.EOF
}
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

func assertTransportOwnershipInvariants(t testing.TB, transport *OrdinaryTransport) {
	t.Helper()
	transport.mu.Lock()
	frames, bytes, activeSends := 0, int64(0), 0
	for _, peer := range transport.peers {
		if peer.count < 0 || peer.bytes < 0 ||
			peer.reservedFrames < 0 || peer.reservedBytes < 0 {
			transport.mu.Unlock()
			t.Fatalf("negative peer ownership for %x", peer.node)
		}
		frames += peer.count + peer.reservedFrames
		bytes += peer.bytes + peer.reservedBytes
		activeSends += peer.reservedFrames
	}
	globalFrames, globalBytes := transport.globalFrames, transport.globalBytes
	trackedSends := transport.activeSends
	transport.mu.Unlock()
	if frames != globalFrames || bytes != globalBytes || activeSends != trackedSends ||
		globalFrames < 0 || globalBytes < 0 ||
		globalFrames > transport.queueLimits.GlobalFrames ||
		globalBytes > transport.queueLimits.GlobalBytes {
		t.Fatalf("ownership invariant = peers %d/%d sends %d, global %d/%d sends %d",
			frames, bytes, activeSends, globalFrames, globalBytes, trackedSends)
	}
	ownedFrames, ownedBytes, freeFrames, _ := transport.frames.stats()
	if ownedFrames < 0 || ownedBytes < 0 || freeFrames < 0 ||
		freeFrames > ownedFrames || ownedFrames > transport.queueLimits.GlobalFrames ||
		ownedBytes > transport.queueLimits.GlobalBytes {
		t.Fatalf("cache invariant = owned %d/%d free %d", ownedFrames, ownedBytes, freeFrames)
	}
}
