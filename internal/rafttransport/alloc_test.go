package rafttransport

import (
	"context"
	"io"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmember"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

var allocationFrameSink []byte
var allocationInboundSink Inbound

func TestEncodeOutboundAndPreflightAllocateNothingWithCallerBuffer(t *testing.T) {
	group := testGroup(91)
	sender, _, from, to := frameTestRegistries(t, 2, group)
	message := frameTestMessage(pb.MsgHeartbeat, from, to)
	payload, err := (proto.MarshalOptions{Deterministic: true}).Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 0, FrameHeaderBytes+len(payload))
	outbound := raftmember.OutboundMessage{
		Group: group, From: from, To: to, Message: message,
	}
	if got := testing.AllocsPerRun(1000, func() {
		var encodeErr error
		allocationFrameSink, _, encodeErr = sender.EncodeOutbound(buffer[:0], outbound)
		if encodeErr != nil {
			panic(encodeErr)
		}
	}); got != 0 {
		t.Fatalf("EncodeOutbound allocations = %v, want 0", got)
	}
	if got := testing.AllocsPerRun(1000, func() {
		plan, preflightErr := sender.preflightOutbound(outbound)
		if preflightErr != nil || plan.destination == (NodeID{}) || plan.frameSize == 0 {
			panic(preflightErr)
		}
	}); got != 0 {
		t.Fatalf("outbound preflight allocations = %v, want 0", got)
	}
	if got := testing.AllocsPerRun(1000, func() {
		if preflightErr := preflightOrdinaryPayload(payload); preflightErr != nil {
			panic(preflightErr)
		}
	}); got != 0 {
		t.Fatalf("preflight allocations = %v, want 0", got)
	}
}

func TestOrdinaryTransportWarmSendQueueRoundTripAllocatesNothing(t *testing.T) {
	fixture := newTransportTestFixture(t)
	dialer := ordinaryDialFunc(func(context.Context, NodeID) (PeerConnection, error) {
		return nil, io.ErrClosedPipe
	})
	transport, err := NewOrdinaryTransport(transportTestOptions(fixture, dialer))
	if err != nil {
		t.Fatal(err)
	}
	transport.state.Store(transportRunning)
	peer := transport.byNode[fixture.remote[0].Node]
	outbound := fixture.outbound(0, 1)
	if err := transport.Send(outbound); err != nil {
		t.Fatal(err)
	}
	transport.commitPeerBatch(peer, 1)

	if got := testing.AllocsPerRun(1000, func() {
		if sendErr := transport.Send(outbound); sendErr != nil {
			panic(sendErr)
		}
		transport.commitPeerBatch(peer, 1)
	}); got != 0 {
		t.Fatalf("warm Send queue round trip allocations = %v, want 0", got)
	}
	if got := testing.AllocsPerRun(1000, func() {
		if _, statsErr := transport.Stats(peer.node); statsErr != nil {
			panic(statsErr)
		}
		transport.GlobalQueueStats()
	}); got != 0 {
		t.Fatalf("transport stats allocations = %v, want 0", got)
	}
}

func BenchmarkOrdinaryTransportWarmSendQueueRoundTrip(b *testing.B) {
	fixture := newTransportTestFixture(b)
	dialer := ordinaryDialFunc(func(context.Context, NodeID) (PeerConnection, error) {
		return nil, io.ErrClosedPipe
	})
	transport, err := NewOrdinaryTransport(transportTestOptions(fixture, dialer))
	if err != nil {
		b.Fatal(err)
	}
	transport.state.Store(transportRunning)
	peer := transport.byNode[fixture.remote[0].Node]
	outbound := fixture.outbound(0, 1)
	if err := transport.Send(outbound); err != nil {
		b.Fatal(err)
	}
	queuedBytes := peer.bytes
	transport.commitPeerBatch(peer, 1)

	b.ReportAllocs()
	b.SetBytes(queuedBytes)
	b.ResetTimer()
	for b.Loop() {
		if err := transport.Send(outbound); err != nil {
			b.Fatal(err)
		}
		transport.commitPeerBatch(peer, 1)
	}
}

func BenchmarkOrdinaryTransportFrameReturnWithConcurrentStats(b *testing.B) {
	fixture := newTransportTestFixture(b)
	dialer := ordinaryDialFunc(func(context.Context, NodeID) (PeerConnection, error) {
		return nil, io.ErrClosedPipe
	})
	transport, err := NewOrdinaryTransport(transportTestOptions(fixture, dialer))
	if err != nil {
		b.Fatal(err)
	}
	transport.state.Store(transportRunning)
	peer := transport.byNode[fixture.remote[0].Node]
	outbound := fixture.outbound(0, 1)
	if err := transport.Send(outbound); err != nil {
		b.Fatal(err)
	}
	transport.commitPeerBatch(peer, 1)

	var stop atomic.Bool
	var readers sync.WaitGroup
	for range max(1, runtime.GOMAXPROCS(0)-1) {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for !stop.Load() {
				_, _ = transport.Stats(peer.node)
				transport.GlobalQueueStats()
			}
		}()
	}
	b.Cleanup(func() {
		stop.Store(true)
		readers.Wait()
	})
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := transport.Send(outbound); err != nil {
			b.Fatal(err)
		}
		transport.commitPeerBatch(peer, 1)
	}
	b.StopTimer()
	stop.Store(true)
	readers.Wait()
}

func BenchmarkBoundedFrameBufferCacheParallelHeartbeat(b *testing.B) {
	workers := max(1, runtime.GOMAXPROCS(0))
	const size = FrameHeaderBytes + 19
	capacity, _, _ := frameBufferCapacity(size, DefaultRetainedFrameBytes)
	cache := newBoundedFrameBufferCache(
		workers, int64(workers*capacity), DefaultRetainedFrameBytes,
	)
	buffers := make([]*pooledFrameBuffer, workers)
	for index := range buffers {
		buffer, err := cache.get(size)
		if err != nil {
			b.Fatal(err)
		}
		buffers[index] = buffer
	}
	for _, buffer := range buffers {
		cache.put(buffer)
	}
	b.ReportAllocs()
	b.SetBytes(size)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			buffer, err := cache.get(size)
			if err != nil {
				b.Fatal(err)
			}
			buffer.bytes[len(buffer.bytes)-1] = 1
			cache.put(buffer)
		}
	})
}

func BenchmarkBoundedFrameBufferCacheParallelMixed64KiBHeartbeat(b *testing.B) {
	workers := max(1, runtime.GOMAXPROCS(0))
	const heartbeat = FrameHeaderBytes + 19
	heartbeatCapacity, _, _ := frameBufferCapacity(heartbeat, DefaultRetainedFrameBytes)
	cache := newBoundedFrameBufferCache(
		workers*2,
		int64(workers*(DefaultRetainedFrameBytes+heartbeatCapacity)),
		DefaultRetainedFrameBytes,
	)
	buffers := make([]*pooledFrameBuffer, workers*2)
	for index := range buffers {
		size := heartbeat
		if index%2 == 0 {
			size = DefaultRetainedFrameBytes
		}
		buffer, err := cache.get(size)
		if err != nil {
			b.Fatal(err)
		}
		buffers[index] = buffer
	}
	for _, buffer := range buffers {
		cache.put(buffer)
	}
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		ordinal := 0
		for pb.Next() {
			size := heartbeat
			if ordinal&15 == 0 {
				size = DefaultRetainedFrameBytes
			}
			buffer, err := cache.get(size)
			if err != nil {
				b.Fatal(err)
			}
			buffer.bytes[len(buffer.bytes)-1] = 1
			cache.put(buffer)
			ordinal++
		}
	})
}

func BenchmarkStaticRegistryDecodeInboundCanonical(b *testing.B) {
	group := testGroup(92)
	sender, receiver, from, to := frameTestRegistries(b, 3, group)
	message := frameTestMessage(pb.MsgHeartbeat, from, to)
	frame, _, err := sender.EncodeOutbound(nil, raftmember.OutboundMessage{
		Group: group, From: from, To: to, Message: message,
	})
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.SetBytes(int64(len(frame)))
	b.ResetTimer()
	for b.Loop() {
		allocationInboundSink, err = receiver.DecodeInbound(testPeerIdentity(receiver, sender.LocalNode()), frame)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFrameBufferPoolHeartbeatAfterRetained64KiB(b *testing.B) {
	pool := frameBufferPool{retain: DefaultRetainedFrameBytes}
	large := pool.get(DefaultRetainedFrameBytes)
	pool.put(large)

	b.ReportAllocs()
	b.SetBytes(FrameHeaderBytes + 19)
	b.ResetTimer()
	for b.Loop() {
		buffer := pool.get(FrameHeaderBytes + 19)
		buffer.bytes[len(buffer.bytes)-1] = 1
		pool.put(buffer)
	}
}
