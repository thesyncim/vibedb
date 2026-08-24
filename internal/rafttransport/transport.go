package rafttransport

import (
	"context"
	"errors"
	"math"
	"math/bits"
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

// QueueLimits bounds owned encoded frames before any network wait. Byte limits
// charge retained buffer capacity, including any size-class rounding. A frame
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

type boundedFrameBufferCache struct {
	mu sync.Mutex

	maxFrames  int
	maxBytes   int64
	retain     int
	free       [bits.UintSize]*pooledFrameBuffer
	freeFrames int

	ownedFrames int
	ownedBytes  int64
	closed      bool
}

func newBoundedFrameBufferCache(maxFrames int, maxBytes int64, retain int) boundedFrameBufferCache {
	return boundedFrameBufferCache{
		maxFrames: maxFrames,
		maxBytes:  maxBytes,
		retain:    retain,
	}
}

// frameBufferCapacity rounds retained buffers into a fixed power-of-two class.
// The final class is capped at retain when retain is not a power of two.
func frameBufferCapacity(size, retain int) (capacity int, class int, cacheable bool) {
	if size <= 0 || retain <= 0 {
		return 0, 0, false
	}
	if size > retain {
		return size, 0, false
	}
	class = bits.Len(uint(size - 1))
	capacity = 1 << class
	if capacity > retain {
		capacity = retain
	}
	return capacity, class, true
}

func (cache *boundedFrameBufferCache) get(size int) (*pooledFrameBuffer, error) {
	if cache == nil || size <= 0 || int64(size) > cache.maxBytes {
		return nil, ErrInvalidTransport
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if cache.closed {
		return nil, ErrTransportClosed
	}
	capacity, class, cacheable := frameBufferCapacity(size, cache.retain)
	if int64(capacity) > cache.maxBytes {
		return nil, ErrBackpressure
	}
	if cacheable && cache.free[class] != nil {
		buffer := cache.free[class]
		cache.free[class] = buffer.next
		buffer.next = nil
		cache.freeFrames--
		buffer.bytes = buffer.bytes[:size]
		return buffer, nil
	}
	for cache.freeFrames != 0 &&
		(cache.ownedFrames == cache.maxFrames ||
			int64(capacity) > cache.maxBytes-cache.ownedBytes) {
		cache.evictFree()
	}
	if cache.ownedFrames == cache.maxFrames ||
		int64(capacity) > cache.maxBytes-cache.ownedBytes {
		return nil, ErrBackpressure
	}
	buffer := &pooledFrameBuffer{bytes: make([]byte, size, capacity)}
	cache.ownedFrames++
	cache.ownedBytes += int64(capacity)
	return buffer, nil
}

func (cache *boundedFrameBufferCache) evictFree() {
	for class := len(cache.free) - 1; class >= 0; class-- {
		buffer := cache.free[class]
		if buffer == nil {
			continue
		}
		cache.free[class] = buffer.next
		buffer.next = nil
		cache.freeFrames--
		cache.ownedFrames--
		cache.ownedBytes -= int64(cap(buffer.bytes))
		buffer.bytes = nil
		return
	}
}

func (cache *boundedFrameBufferCache) put(buffer *pooledFrameBuffer) {
	if cache == nil || buffer == nil {
		return
	}
	capacity := cap(buffer.bytes)
	cacheable := capacity <= cache.retain
	if cacheable {
		clear(buffer.bytes)
		buffer.bytes = buffer.bytes[:0]
	}
	cache.mu.Lock()
	if cache.closed || !cacheable {
		cache.ownedFrames--
		cache.ownedBytes -= int64(capacity)
		buffer.bytes = nil
		cache.mu.Unlock()
		return
	}
	_, class, validClass := frameBufferCapacity(capacity, cache.retain)
	if !validClass {
		cache.ownedFrames--
		cache.ownedBytes -= int64(capacity)
		buffer.bytes = nil
		cache.mu.Unlock()
		return
	}
	buffer.next = cache.free[class]
	cache.free[class] = buffer
	cache.freeFrames++
	cache.mu.Unlock()
}

func (cache *boundedFrameBufferCache) close() {
	if cache == nil {
		return
	}
	cache.mu.Lock()
	cache.closed = true
	for cache.freeFrames != 0 {
		cache.evictFree()
	}
	cache.mu.Unlock()
}

func (cache *boundedFrameBufferCache) stats() (
	ownedFrames int,
	ownedBytes int64,
	freeFrames int,
	closed bool,
) {
	if cache == nil {
		return 0, 0, 0, true
	}
	cache.mu.Lock()
	ownedFrames, ownedBytes = cache.ownedFrames, cache.ownedBytes
	freeFrames, closed = cache.freeFrames, cache.closed
	cache.mu.Unlock()
	return ownedFrames, ownedBytes, freeFrames, closed
}

type ordinaryPeer struct {
	node NodeID

	queue []outboundFrame
	head  int
	count int
	bytes int64

	reservedFrames int
	reservedBytes  int64
	wake           chan struct{}

	writeBuffer   []byte
	batchFrames   []*pooledFrameBuffer
	releaseFrames []*pooledFrameBuffer
	connection    PeerConnection

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
	frames        boundedFrameBufferCache

	peers  []*ordinaryPeer
	byNode map[NodeID]*ordinaryPeer

	mu           sync.Mutex
	reservations *sync.Cond
	activeSends  int
	globalFrames int
	globalBytes  int64

	// beforeEncode is a test-only scheduling seam. Production leaves it nil.
	beforeEncode func()
	// beforeFrameReturn is a test-only scheduling seam after queue unlock.
	beforeFrameReturn func()

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
		frames: newBoundedFrameBufferCache(
			options.Queue.GlobalFrames, options.Queue.GlobalBytes, retain,
		),
		peers:  make([]*ordinaryPeer, 0, len(options.Peers)),
		byNode: make(map[NodeID]*ordinaryPeer, len(options.Peers)),
		ctx:    ctx, cancel: cancel,
	}
	transport.reservations = sync.NewCond(&transport.mu)
	for _, node := range options.Peers {
		peer := &ordinaryPeer{
			node: node, queue: make([]outboundFrame, options.Queue.PerPeerFrames),
			wake:          make(chan struct{}, 1),
			batchFrames:   make([]*pooledFrameBuffer, options.Coalesce.MaxFrames),
			releaseFrames: make([]*pooledFrameBuffer, options.Coalesce.MaxFrames),
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
	minimumOwnedBytes, _, _ := frameBufferCapacity(FrameHeaderBytes, retain)
	if options.Registry == nil || options.Dialer == nil || options.Wait == nil ||
		options.Backoff == nil || options.WriteDeadline == nil ||
		peers == 0 || peers > AbsoluteMaxTransportPeers ||
		queue.PerPeerFrames <= 0 || queue.PerPeerFrames > AbsoluteMaxQueuedFrames ||
		queue.GlobalFrames < queue.PerPeerFrames || queue.GlobalFrames > AbsoluteMaxQueuedFrames ||
		int64(peers)*int64(queue.PerPeerFrames) > int64(queue.GlobalFrames) ||
		queue.PerPeerBytes < int64(minimumOwnedBytes) || queue.PerPeerBytes > AbsoluteMaxQueuedBytes ||
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
	transport.waitForActiveSends()
	transport.drainQueues()
	transport.frames.close()
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
			transport.frames.close()
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
	plan, err := transport.registry.preflightOutbound(outbound)
	if err != nil {
		return err
	}
	wireBytes := plan.frameSize + StreamRecordHeaderBytes
	if wireBytes > transport.coalesce.MaxBytes {
		return ErrFrameTooLarge
	}
	ownedSize, _, _ := frameBufferCapacity(plan.frameSize, transport.frames.retain)
	peer, err := transport.reserveOutbound(plan, ownedSize)
	if err != nil {
		return err
	}
	storage, err := transport.frames.get(plan.frameSize)
	if err != nil {
		transport.unwindReservation(peer, ownedSize)
		if !errors.Is(err, ErrTransportClosed) && !errors.Is(err, ErrBackpressure) {
			transport.cancel(err)
		}
		return err
	}
	if transport.beforeEncode != nil {
		transport.beforeEncode()
	}
	frame, err := transport.registry.appendOutbound(storage.bytes[:0], outbound, plan)
	if err != nil {
		transport.unwindReservation(peer, ownedSize)
		transport.frames.put(storage)
		return err
	}
	storage.bytes = frame
	if err := transport.publishReservation(peer, storage, plan.frameSize, ownedSize); err != nil {
		transport.frames.put(storage)
		return err
	}
	peer.notify()
	return nil
}

func (transport *OrdinaryTransport) reserveOutbound(
	plan outboundFramePlan,
	ownedSize int,
) (*ordinaryPeer, error) {
	peer := transport.byNode[plan.destination]
	if peer == nil {
		return nil, ErrNodeNotFound
	}
	frameBytes := int64(ownedSize)
	transport.mu.Lock()
	closed := transport.state.Load() != transportRunning || context.Cause(transport.ctx) != nil
	if closed ||
		peer.count+peer.reservedFrames >= len(peer.queue) ||
		frameBytes > transport.queueLimits.PerPeerBytes-peer.bytes-peer.reservedBytes ||
		transport.globalFrames >= transport.queueLimits.GlobalFrames ||
		frameBytes > transport.queueLimits.GlobalBytes-transport.globalBytes {
		transport.mu.Unlock()
		if closed {
			return nil, ErrTransportClosed
		}
		return nil, ErrBackpressure
	}
	peer.reservedFrames++
	peer.reservedBytes += frameBytes
	transport.activeSends++
	transport.globalFrames++
	transport.globalBytes += frameBytes
	transport.mu.Unlock()
	return peer, nil
}

func (transport *OrdinaryTransport) publishReservation(
	peer *ordinaryPeer,
	storage *pooledFrameBuffer,
	frameSize int,
	ownedSize int,
) error {
	if storage == nil || len(storage.bytes) != frameSize || cap(storage.bytes) != ownedSize {
		transport.unwindReservation(peer, ownedSize)
		transport.cancel(ErrInvalidTransport)
		return ErrInvalidTransport
	}
	frameBytes := int64(ownedSize)
	transport.mu.Lock()
	if peer.reservedFrames <= 0 || peer.reservedBytes < frameBytes ||
		transport.activeSends <= 0 {
		transport.mu.Unlock()
		transport.cancel(ErrInvalidTransport)
		return ErrInvalidTransport
	}
	peer.reservedFrames--
	peer.reservedBytes -= frameBytes
	transport.activeSends--
	if transport.activeSends == 0 {
		transport.reservations.Broadcast()
	}
	if transport.state.Load() != transportRunning || context.Cause(transport.ctx) != nil {
		if transport.globalFrames <= 0 || transport.globalBytes < frameBytes {
			transport.mu.Unlock()
			transport.cancel(ErrInvalidTransport)
			return ErrInvalidTransport
		}
		transport.globalFrames--
		transport.globalBytes -= frameBytes
		transport.mu.Unlock()
		return ErrTransportClosed
	}
	tail := (peer.head + peer.count) % len(peer.queue)
	peer.queue[tail] = outboundFrame{buffer: storage}
	peer.count++
	peer.bytes += frameBytes
	transport.mu.Unlock()
	return nil
}

func (transport *OrdinaryTransport) unwindReservation(peer *ordinaryPeer, ownedSize int) {
	frameBytes := int64(ownedSize)
	transport.mu.Lock()
	if peer.reservedFrames <= 0 || peer.reservedBytes < frameBytes ||
		transport.activeSends <= 0 ||
		transport.globalFrames <= 0 || transport.globalBytes < frameBytes {
		transport.mu.Unlock()
		transport.cancel(ErrInvalidTransport)
		return
	}
	peer.reservedFrames--
	peer.reservedBytes -= frameBytes
	transport.activeSends--
	transport.globalFrames--
	transport.globalBytes -= frameBytes
	if transport.activeSends == 0 {
		transport.reservations.Broadcast()
	}
	transport.mu.Unlock()
}

func (transport *OrdinaryTransport) waitForActiveSends() {
	transport.mu.Lock()
	for transport.activeSends != 0 {
		transport.reservations.Wait()
	}
	transport.mu.Unlock()
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
			identity := PeerIdentity{}
			if candidate != nil {
				identity = candidate.PeerIdentity()
			}
			if err != nil || candidate == nil || !validPeerIdentity(identity) ||
				identity.Node != peer.node ||
				identity.TrustDomain != transport.registry.TrustDomain() ||
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
	if frames > peer.count || frames > len(peer.releaseFrames) {
		transport.mu.Unlock()
		transport.cancel(ErrInvalidTransport)
		return
	}
	var sentBytes uint64
	for index := range frames {
		frame := peer.queue[peer.head].buffer
		peer.queue[peer.head] = outboundFrame{}
		peer.head = (peer.head + 1) % len(peer.queue)
		peer.count--
		frameBytes := int64(cap(frame.bytes))
		peer.bytes -= frameBytes
		transport.globalFrames--
		transport.globalBytes -= frameBytes
		sentBytes += uint64(len(frame.bytes))
		peer.releaseFrames[index] = frame
	}
	remaining := peer.count != 0
	transport.mu.Unlock()
	if transport.beforeFrameReturn != nil {
		transport.beforeFrameReturn()
	}
	for index := range frames {
		transport.frames.put(peer.releaseFrames[index])
		peer.releaseFrames[index] = nil
	}
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
	var buffers []*pooledFrameBuffer
	transport.mu.Lock()
	buffers = make([]*pooledFrameBuffer, 0, transport.globalFrames)
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
			frameBytes := int64(cap(frame.bytes))
			peer.bytes -= frameBytes
			transport.globalFrames--
			transport.globalBytes -= frameBytes
			buffers = append(buffers, frame)
		}
		clear(peer.writeBuffer)
		peer.writeBuffer = nil
	}
	transport.mu.Unlock()
	for _, connection := range connections {
		_ = connection.Close()
	}
	for _, buffer := range buffers {
		transport.frames.put(buffer)
	}
}

// PeerStats is a point-in-time transport snapshot. Queue byte fields charge
// buffer capacity. SentFrames and SentBytes count successful local writes, not
// receiver acknowledgement. SentBytes excludes stream record headers.
type PeerStats struct {
	QueuedFrames   int
	QueuedBytes    int64
	ReservedFrames int
	ReservedBytes  int64
	DialAttempts   uint64
	DialFailures   uint64
	WriteFailures  uint64
	Connections    uint64
	SentFrames     uint64
	SentBytes      uint64
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
	reservedFrames, reservedBytes := peer.reservedFrames, peer.reservedBytes
	transport.mu.Unlock()
	return PeerStats{
		QueuedFrames: queuedFrames, QueuedBytes: queuedBytes,
		ReservedFrames: reservedFrames, ReservedBytes: reservedBytes,
		DialAttempts: peer.dialAttempts.Load(), DialFailures: peer.dialFailures.Load(),
		WriteFailures: peer.writeFailures.Load(), Connections: peer.connections.Load(),
		SentFrames: peer.sentFrames.Load(), SentBytes: peer.sentBytes.Load(),
	}, nil
}

// GlobalQueueStats reports the exact current queued and reserved frame budget.
func (transport *OrdinaryTransport) GlobalQueueStats() (frames int, bytes int64) {
	if transport == nil {
		return 0, 0
	}
	transport.mu.Lock()
	frames, bytes = transport.globalFrames, transport.globalBytes
	transport.mu.Unlock()
	return frames, bytes
}
