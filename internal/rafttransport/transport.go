package rafttransport

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/thesyncim/vibedb/internal/raftmember"
)

const (
	AbsoluteMaxTransportPeers              = 4096
	AbsoluteMaxQueuedFrames                = 65536
	AbsoluteMaxQueuedBytes           int64 = 1 << 30
	AbsoluteMaxCoalescedFrames             = 256
	AbsoluteMaxRetainedCoalesceBytes int64 = 256 << 20

	AbsoluteMaxCoalesceDelay  = time.Second
	AbsoluteMaxReconnectDelay = time.Minute
)

// QueueLimits bounds owned encoded frames before any network wait. A frame
// that cannot fit is rejected synchronously with ErrBackpressure.
type QueueLimits struct {
	PerPeerFrames int
	PerPeerBytes  int64
	GlobalFrames  int
	GlobalBytes   int64
}

// CoalesceLimits bounds one ordinary stream write. MaxBytes includes every
// four-byte stream record header. RetainedBytes caps warmed write scratch.
type CoalesceLimits struct {
	MaxFrames     int
	MaxBytes      int
	MaxDelay      time.Duration
	RetainedBytes int
}

// ReconnectBackoff returns the delay after consecutive failures. Core
// clamps the result to [0, MaxReconnectDelay].
type ReconnectBackoff func(failures uint32) time.Duration

// OrdinaryTransportOptions owns one outbound stream and bounded queue per
// remote node. Peers must be nonzero, unique, and different from LocalNode.
type OrdinaryTransportOptions struct {
	Registry           *StaticRegistry
	Peers              []NodeID
	Dialer             OrdinaryDialer
	Queue              QueueLimits
	Coalesce           CoalesceLimits
	Wait               DelayWaitFunc
	Backoff            ReconnectBackoff
	MaxReconnectDelay  time.Duration
	WriteDeadline      DeadlineFunc
	RetainedFrameBytes int
}

type outboundFrame struct {
	buffer *pooledFrameBuffer
}

type ordinaryPeer struct {
	node NodeID

	queue []outboundFrame
	head  int
	count int
	bytes int64
	wake  chan struct{}

	writeBuffer []byte
	batchFrames []*pooledFrameBuffer
	connection  PeerConnection

	dialAttempts  atomic.Uint64
	dialFailures  atomic.Uint64
	writeFailures atomic.Uint64
	connections   atomic.Uint64
	sentFrames    atomic.Uint64
	sentBytes     atomic.Uint64
}

const (
	transportReady uint32 = iota
	transportRunning
	transportClosed
)

// OrdinaryTransport is a bounded, fail-fast outbound ordinary-message lane.
// One worker and one persistent authenticated stream belong to each peer, so a
// stalled peer cannot block another peer's queue or writer.
type OrdinaryTransport struct {
	registry      *StaticRegistry
	dialer        OrdinaryDialer
	queueLimits   QueueLimits
	coalesce      CoalesceLimits
	wait          DelayWaitFunc
	backoff       ReconnectBackoff
	maxBackoff    time.Duration
	writeDeadline DeadlineFunc
	frames        frameBufferPool

	peers  []*ordinaryPeer
	byNode map[NodeID]*ordinaryPeer

	mu           sync.Mutex
	globalFrames int
	globalBytes  int64

	state  atomic.Uint32
	ctx    context.Context
	cancel context.CancelCauseFunc
}

// NewOrdinaryTransport allocates only bounded peer rings and control channels.
// Run starts the network workers.
func NewOrdinaryTransport(options OrdinaryTransportOptions) (*OrdinaryTransport, error) {
	retain := options.RetainedFrameBytes
	if retain == 0 {
		retain = DefaultRetainedFrameBytes
	}
	if err := validateOrdinaryTransportOptions(options, retain); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	transport := &OrdinaryTransport{
		registry: options.Registry, dialer: options.Dialer,
		queueLimits: options.Queue, coalesce: options.Coalesce,
		wait: options.Wait, backoff: options.Backoff,
		maxBackoff: options.MaxReconnectDelay, writeDeadline: options.WriteDeadline,
		frames: frameBufferPool{retain: retain},
		peers:  make([]*ordinaryPeer, 0, len(options.Peers)),
		byNode: make(map[NodeID]*ordinaryPeer, len(options.Peers)),
		ctx:    ctx, cancel: cancel,
	}
	for _, node := range options.Peers {
		peer := &ordinaryPeer{
			node: node, queue: make([]outboundFrame, options.Queue.PerPeerFrames),
			wake:        make(chan struct{}, 1),
			batchFrames: make([]*pooledFrameBuffer, options.Coalesce.MaxFrames),
		}
		transport.peers = append(transport.peers, peer)
		transport.byNode[node] = peer
	}
	return transport, nil
}

func validateOrdinaryTransportOptions(options OrdinaryTransportOptions, retain int) error {
	peers := len(options.Peers)
	queue := options.Queue
	coalesce := options.Coalesce
	if options.Registry == nil || options.Dialer == nil || options.Wait == nil ||
		options.Backoff == nil || options.WriteDeadline == nil ||
		peers == 0 || peers > AbsoluteMaxTransportPeers ||
		queue.PerPeerFrames <= 0 || queue.PerPeerFrames > AbsoluteMaxQueuedFrames ||
		queue.GlobalFrames < queue.PerPeerFrames || queue.GlobalFrames > AbsoluteMaxQueuedFrames ||
		int64(peers)*int64(queue.PerPeerFrames) > int64(queue.GlobalFrames) ||
		queue.PerPeerBytes < FrameHeaderBytes || queue.PerPeerBytes > AbsoluteMaxQueuedBytes ||
		queue.GlobalBytes < queue.PerPeerBytes || queue.GlobalBytes > AbsoluteMaxQueuedBytes ||
		coalesce.MaxFrames <= 0 || coalesce.MaxFrames > queue.PerPeerFrames ||
		coalesce.MaxFrames > AbsoluteMaxCoalescedFrames ||
		coalesce.MaxBytes < StreamRecordHeaderBytes+FrameHeaderBytes ||
		int64(coalesce.MaxBytes) > queue.PerPeerBytes+int64(StreamRecordHeaderBytes*coalesce.MaxFrames) ||
		coalesce.MaxDelay < 0 || coalesce.MaxDelay > AbsoluteMaxCoalesceDelay ||
		coalesce.RetainedBytes < StreamRecordHeaderBytes+FrameHeaderBytes ||
		coalesce.RetainedBytes > coalesce.MaxBytes ||
		int64(peers)*int64(coalesce.RetainedBytes) > AbsoluteMaxRetainedCoalesceBytes ||
		retain < FrameHeaderBytes || retain > MaxFrameBytes ||
		options.MaxReconnectDelay < 0 || options.MaxReconnectDelay > AbsoluteMaxReconnectDelay {
		return ErrInvalidTransport
	}
	seen := make(map[NodeID]struct{}, peers)
	local := options.Registry.LocalNode()
	for _, node := range options.Peers {
		if node == (NodeID{}) || node == local {
			return ErrInvalidTransport
		}
		if _, duplicate := seen[node]; duplicate {
			return ErrInvalidTransport
		}
		seen[node] = struct{}{}
	}
	return nil
}

// Run serves every peer until parent is canceled or Close is called. It may be
// called once. Every dial and wait hook must honor the derived context.
func (transport *OrdinaryTransport) Run(parent context.Context) error {
	if transport == nil || parent == nil ||
		!transport.state.CompareAndSwap(transportReady, transportRunning) {
		return ErrTransportClosed
	}
	stopParent := context.AfterFunc(parent, func() {
		cause := context.Cause(parent)
		if cause == nil {
			cause = context.Canceled
		}
		transport.cancel(cause)
	})
	defer stopParent()
	if cause := context.Cause(parent); cause != nil {
		transport.cancel(cause)
	}

	var workers sync.WaitGroup
	workers.Add(len(transport.peers))
	for _, peer := range transport.peers {
		go func() {
			defer workers.Done()
			transport.runPeer(peer)
		}()
	}
	workers.Wait()
	transport.state.Store(transportClosed)
	transport.drainQueues()
	cause := context.Cause(transport.ctx)
	if cause == nil {
		return ErrTransportClosed
	}
	return cause
}

// Close stops every worker and closes every active stream. It is idempotent.
func (transport *OrdinaryTransport) Close() error {
	if transport == nil {
		return nil
	}
	for {
		state := transport.state.Load()
		switch state {
		case transportReady:
			if !transport.state.CompareAndSwap(transportReady, transportClosed) {
				continue
			}
			transport.cancel(ErrTransportClosed)
			transport.drainQueues()
			return nil
		case transportRunning:
			if !transport.state.CompareAndSwap(transportRunning, transportClosed) {
				continue
			}
			transport.cancel(ErrTransportClosed)
			return nil
		default:
			return nil
		}
	}
}

// Send encodes and takes ownership of one ordinary frame during the call. The
// outbound message and its nested bytes remain borrowed and are not retained.
func (transport *OrdinaryTransport) Send(outbound raftmember.OutboundMessage) error {
	if transport == nil || transport.state.Load() != transportRunning ||
		context.Cause(transport.ctx) != nil {
		return ErrTransportClosed
	}
	measured := 0
	if outbound.Message != nil {
		if size, err := raftmember.MeasureOrdinaryMessage(outbound.Message); err == nil &&
			size <= MaxFrameBytes-FrameHeaderBytes {
			measured = FrameHeaderBytes + size
		}
	}
	if measured < FrameHeaderBytes {
		measured = FrameHeaderBytes
	}
	storage := transport.frames.get(measured)
	frame, destination, err := transport.registry.EncodeOutbound(storage.bytes[:0], outbound)
	if err != nil {
		transport.frames.put(storage)
		return err
	}
	peer := transport.byNode[destination]
	if peer == nil {
		transport.frames.put(storage)
		return ErrNodeNotFound
	}
	frameBytes := int64(len(frame))
	wireBytes := len(frame) + StreamRecordHeaderBytes
	if wireBytes > transport.coalesce.MaxBytes {
		transport.frames.put(storage)
		return ErrFrameTooLarge
	}

	transport.mu.Lock()
	closed := transport.state.Load() != transportRunning || context.Cause(transport.ctx) != nil
	if closed ||
		peer.count >= len(peer.queue) ||
		frameBytes > transport.queueLimits.PerPeerBytes-peer.bytes ||
		transport.globalFrames >= transport.queueLimits.GlobalFrames ||
		frameBytes > transport.queueLimits.GlobalBytes-transport.globalBytes {
		transport.mu.Unlock()
		transport.frames.put(storage)
		if closed {
			return ErrTransportClosed
		}
		return ErrBackpressure
	}
	tail := (peer.head + peer.count) % len(peer.queue)
	storage.bytes = frame
	peer.queue[tail] = outboundFrame{buffer: storage}
	peer.count++
	peer.bytes += frameBytes
	transport.globalFrames++
	transport.globalBytes += frameBytes
	transport.mu.Unlock()
	peer.notify()
	return nil
}

func (peer *ordinaryPeer) notify() {
	select {
	case peer.wake <- struct{}{}:
	default:
	}
}

func (transport *OrdinaryTransport) runPeer(peer *ordinaryPeer) {
	var connection PeerConnection
	var stopConnection func() bool
	failures := uint32(0)
	defer func() {
		if stopConnection != nil {
			stopConnection()
		}
		if connection != nil {
			_ = connection.Close()
			transport.clearConnection(peer)
		}
	}()

	for {
		if !transport.peerHasFrames(peer) {
			select {
			case <-transport.ctx.Done():
				return
			case <-peer.wake:
			}
		}
		if context.Cause(transport.ctx) != nil {
			return
		}
		if connection == nil {
			if failures != 0 {
				delay := transport.reconnectDelay(failures)
				if err := transport.wait(transport.ctx, delay, nil); err != nil {
					if context.Cause(transport.ctx) == nil {
						transport.cancel(err)
					}
					return
				}
			}
			peer.dialAttempts.Add(1)
			candidate, err := transport.dialer.DialOrdinary(transport.ctx, peer.node)
			if err != nil || candidate == nil || candidate.PeerNode() != peer.node ||
				candidate.TrafficClass() != TrafficOrdinary {
				if candidate != nil {
					_ = candidate.Close()
				}
				peer.dialFailures.Add(1)
				failures = saturatingIncrement(failures)
				continue
			}
			connection = candidate
			transport.setConnection(peer, connection)
			peer.connections.Add(1)
			active := connection
			stopConnection = context.AfterFunc(
				transport.ctx, func() { _ = active.Close() },
			)
		}

		if transport.shouldDelayCoalesce(peer) && transport.coalesce.MaxDelay != 0 {
			if err := transport.wait(
				transport.ctx, transport.coalesce.MaxDelay, peer.wake,
			); err != nil {
				if context.Cause(transport.ctx) == nil {
					transport.cancel(err)
				}
				return
			}
		}
		batch, frames := transport.buildPeerBatch(peer)
		if frames == 0 {
			continue
		}
		deadline := transport.writeDeadline()
		if deadline.IsZero() {
			transport.cancel(ErrInvalidTransport)
			return
		}
		writeErr := connection.SetWriteDeadline(deadline)
		if writeErr == nil {
			writeErr = writeFull(connection, batch)
		}
		transport.releasePeerBatch(peer)
		if writeErr != nil {
			peer.writeFailures.Add(1)
			if stopConnection != nil {
				stopConnection()
				stopConnection = nil
			}
			_ = connection.Close()
			transport.clearConnection(peer)
			connection = nil
			failures = saturatingIncrement(failures)
			continue
		}
		transport.commitPeerBatch(peer, frames)
		failures = 0
	}
}

func saturatingIncrement(value uint32) uint32 {
	if value == math.MaxUint32 {
		return value
	}
	return value + 1
}

func (transport *OrdinaryTransport) reconnectDelay(failures uint32) time.Duration {
	delay := transport.backoff(failures)
	if delay < 0 {
		return 0
	}
	if delay > transport.maxBackoff {
		return transport.maxBackoff
	}
	return delay
}

func (transport *OrdinaryTransport) peerHasFrames(peer *ordinaryPeer) bool {
	transport.mu.Lock()
	hasFrames := peer.count != 0
	transport.mu.Unlock()
	return hasFrames
}

func (transport *OrdinaryTransport) shouldDelayCoalesce(peer *ordinaryPeer) bool {
	transport.mu.Lock()
	delay := peer.count == 1 && transport.coalesce.MaxFrames > 1
	transport.mu.Unlock()
	return delay
}

func (transport *OrdinaryTransport) buildPeerBatch(peer *ordinaryPeer) ([]byte, int) {
	transport.mu.Lock()
	frames := 0
	wireBytes := 0
	for frames < peer.count && frames < transport.coalesce.MaxFrames {
		index := (peer.head + frames) % len(peer.queue)
		frame := peer.queue[index].buffer.bytes
		if wireBytes+StreamRecordHeaderBytes+len(frame) > transport.coalesce.MaxBytes {
			break
		}
		peer.batchFrames[frames] = peer.queue[index].buffer
		wireBytes += StreamRecordHeaderBytes + len(frame)
		frames++
	}
	transport.mu.Unlock()

	if cap(peer.writeBuffer) < wireBytes {
		peer.writeBuffer = make([]byte, 0, wireBytes)
	} else {
		peer.writeBuffer = peer.writeBuffer[:0]
	}
	for index := range frames {
		var err error
		peer.writeBuffer, err = appendStreamRecord(
			peer.writeBuffer, peer.batchFrames[index].bytes,
		)
		peer.batchFrames[index] = nil
		if err != nil {
			clear(peer.batchFrames[index+1 : frames])
			transport.releasePeerBatch(peer)
			transport.cancel(err)
			return nil, 0
		}
	}
	return peer.writeBuffer, frames
}

func (transport *OrdinaryTransport) releasePeerBatch(peer *ordinaryPeer) {
	if cap(peer.writeBuffer) > transport.coalesce.RetainedBytes {
		peer.writeBuffer = nil
		return
	}
	clear(peer.writeBuffer)
	peer.writeBuffer = peer.writeBuffer[:0]
}

func (transport *OrdinaryTransport) commitPeerBatch(peer *ordinaryPeer, frames int) {
	transport.mu.Lock()
	if frames > peer.count {
		transport.mu.Unlock()
		transport.cancel(ErrInvalidTransport)
		return
	}
	var sentBytes uint64
	for range frames {
		frame := peer.queue[peer.head].buffer
		peer.queue[peer.head] = outboundFrame{}
		peer.head = (peer.head + 1) % len(peer.queue)
		peer.count--
		frameBytes := int64(len(frame.bytes))
		peer.bytes -= frameBytes
		transport.globalFrames--
		transport.globalBytes -= frameBytes
		sentBytes += uint64(len(frame.bytes))
		transport.frames.put(frame)
	}
	remaining := peer.count != 0
	transport.mu.Unlock()
	peer.sentFrames.Add(uint64(frames))
	peer.sentBytes.Add(sentBytes)
	if remaining {
		peer.notify()
	}
}

func (transport *OrdinaryTransport) setConnection(
	peer *ordinaryPeer,
	connection PeerConnection,
) {
	transport.mu.Lock()
	peer.connection = connection
	transport.mu.Unlock()
}

func (transport *OrdinaryTransport) clearConnection(peer *ordinaryPeer) {
	transport.mu.Lock()
	peer.connection = nil
	transport.mu.Unlock()
}

func (transport *OrdinaryTransport) drainQueues() {
	var connections []PeerConnection
	transport.mu.Lock()
	for _, peer := range transport.peers {
		if peer.connection != nil {
			connections = append(connections, peer.connection)
			peer.connection = nil
		}
		for peer.count != 0 {
			frame := peer.queue[peer.head].buffer
			peer.queue[peer.head] = outboundFrame{}
			peer.head = (peer.head + 1) % len(peer.queue)
			peer.count--
			transport.frames.put(frame)
		}
		peer.bytes = 0
		clear(peer.writeBuffer)
		peer.writeBuffer = nil
	}
	transport.globalFrames = 0
	transport.globalBytes = 0
	transport.mu.Unlock()
	for _, connection := range connections {
		_ = connection.Close()
	}
}

// PeerStats is a point-in-time transport snapshot. SentBytes excludes stream
// record headers.
type PeerStats struct {
	QueuedFrames  int
	QueuedBytes   int64
	DialAttempts  uint64
	DialFailures  uint64
	WriteFailures uint64
	Connections   uint64
	SentFrames    uint64
	SentBytes     uint64
}

// Stats reports one configured peer without allocating.
func (transport *OrdinaryTransport) Stats(node NodeID) (PeerStats, error) {
	if transport == nil {
		return PeerStats{}, ErrNodeNotFound
	}
	peer := transport.byNode[node]
	if peer == nil {
		return PeerStats{}, ErrNodeNotFound
	}
	transport.mu.Lock()
	queuedFrames, queuedBytes := peer.count, peer.bytes
	transport.mu.Unlock()
	return PeerStats{
		QueuedFrames: queuedFrames, QueuedBytes: queuedBytes,
		DialAttempts: peer.dialAttempts.Load(), DialFailures: peer.dialFailures.Load(),
		WriteFailures: peer.writeFailures.Load(), Connections: peer.connections.Load(),
		SentFrames: peer.sentFrames.Load(), SentBytes: peer.sentBytes.Load(),
	}, nil
}

// GlobalQueueStats reports the exact current owned queue budget.
func (transport *OrdinaryTransport) GlobalQueueStats() (frames int, bytes int64) {
	if transport == nil {
		return 0, 0
	}
	transport.mu.Lock()
	frames, bytes = transport.globalFrames, transport.globalBytes
	transport.mu.Unlock()
	return frames, bytes
}
