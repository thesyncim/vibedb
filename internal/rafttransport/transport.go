package rafttransport

import (
	"context"
	"crypto/sha256"
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
// configured remote node. The slice may be empty for a node-scoped process
// that enrolls its first physical peer after startup; every supplied peer
// must be nonzero, unique, enrolled, and different from LocalNode.
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
	node   NodeID
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
	// started is protected by OrdinaryTransport.mu.  It distinguishes a
	// ready queue (whose done channel has no worker owner) from a running
	// worker, so retirement can join without ever double-closing done.
	started  bool
	retiring bool

	queue []outboundFrame
	head  int
	count int
	bytes int64

	reservedFrames int
	reservedBytes  int64
	// inFlightFrames/Bytes cover a batch that has been detached from the
	// ring's head for encoding or network write. Retirement must wait for this
	// writer-owned work as well as queued/reserved frames.
	inFlightFrames int
	inFlightBytes  int64
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

	peers    []*ordinaryPeer
	byNode   map[NodeID]*ordinaryPeer
	workerWG sync.WaitGroup

	mu           sync.Mutex
	reservations *sync.Cond
	activeSends  int
	globalFrames int
	globalBytes  int64

	// beforeEncode is a test-only scheduling seam. Production leaves it nil.
	beforeEncode func()
	// beforeFrameReturn is a test-only scheduling seam after queue unlock.
	beforeFrameReturn func()

	state   atomic.Uint32
	started chan struct{}
	ctx     context.Context
	cancel  context.CancelCauseFunc
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
		peers:   make([]*ordinaryPeer, 0, len(options.Peers)),
		byNode:  make(map[NodeID]*ordinaryPeer, len(options.Peers)),
		started: make(chan struct{}), ctx: ctx, cancel: cancel,
	}
	transport.reservations = sync.NewCond(&transport.mu)
	for _, node := range options.Peers {
		peer := transport.newPeer(node)
		transport.peers = append(transport.peers, peer)
		transport.byNode[node] = peer
	}
	return transport, nil
}

func (transport *OrdinaryTransport) newPeer(node NodeID) *ordinaryPeer {
	peerCtx, cancel := context.WithCancel(transport.ctx)
	return &ordinaryPeer{
		node: node, ctx: peerCtx, cancel: cancel, done: make(chan struct{}),
		queue:         make([]outboundFrame, transport.queueLimits.PerPeerFrames),
		wake:          make(chan struct{}, 1),
		batchFrames:   make([]*pooledFrameBuffer, transport.coalesce.MaxFrames),
		releaseFrames: make([]*pooledFrameBuffer, transport.coalesce.MaxFrames),
	}
}

func validateOrdinaryTransportOptions(options OrdinaryTransportOptions, retain int) error {
	peers := len(options.Peers)
	queue := options.Queue
	coalesce := options.Coalesce
	minimumOwnedBytes, _, _ := frameBufferCapacity(FrameHeaderBytes, retain)
	if options.Registry == nil || options.Dialer == nil || options.Wait == nil ||
		options.Backoff == nil || options.WriteDeadline == nil ||
		peers > AbsoluteMaxTransportPeers ||
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
		if !options.Registry.IsPeerEnrolled(node) {
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
	if transport == nil || parent == nil {
		return ErrTransportClosed
	}
	// Serialize the state transition and initial worker launch with AddPeer.
	// Without this lock an AddPeer arriving after the CAS but before the
	// initial clone could start a worker that Run then starts a second time.
	transport.mu.Lock()
	if !transport.state.CompareAndSwap(transportReady, transportRunning) {
		transport.mu.Unlock()
		return ErrTransportClosed
	}
	close(transport.started)
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

	// Keep one supervisor counted while the transport is running.  This makes
	// WaitGroup.Add safe for AddPeer racing with Run's wait, including a
	// genuinely empty node whose first peer arrives later.
	transport.workerWG.Add(1)
	go func() {
		defer transport.workerWG.Done()
		<-transport.ctx.Done()
	}()
	for _, peer := range transport.peers {
		transport.startPeerLocked(peer)
	}
	transport.mu.Unlock()
	transport.workerWG.Wait()
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
			close(transport.started)
			transport.mu.Lock()
			for _, peer := range transport.peers {
				peer.cancel()
			}
			transport.mu.Unlock()
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

// Started closes exactly once when Run acquires the transport or Close retires
// it before start. After receiving, Running distinguishes those two states.
func (transport *OrdinaryTransport) Started() <-chan struct{} {
	if transport == nil {
		return nil
	}
	return transport.started
}

// Running reports whether Run owns the outbound workers.
func (transport *OrdinaryTransport) Running() bool {
	return transport != nil && transport.state.Load() == transportRunning &&
		context.Cause(transport.ctx) == nil
}

// AddPeer installs one bounded queue for an already enrolled physical peer.
// It is idempotent for the same live queue and starts its worker immediately
// when Run is already active.  The method performs no network I/O while the
// transport mutex is held.
func (transport *OrdinaryTransport) AddPeer(node NodeID) error {
	if transport == nil || node == (NodeID{}) || node == transport.registry.LocalNode() {
		return ErrInvalidTransport
	}
	if !transport.registry.IsPeerEnrolled(node) {
		return ErrPeerUnauthorized
	}
	transport.mu.Lock()
	if existing := transport.byNode[node]; existing != nil {
		if existing.retiring {
			transport.mu.Unlock()
			return ErrPeerConflict
		}
		transport.mu.Unlock()
		return nil
	}
	if transport.state.Load() == transportClosed || context.Cause(transport.ctx) != nil {
		transport.mu.Unlock()
		return ErrTransportClosed
	}
	// The first check is intentionally repeated under the transport lock. A
	// retirement may complete between the caller-side directory read and this
	// point; never create a queue for a peer that is no longer enrolled.
	if !transport.registry.IsPeerEnrolled(node) {
		transport.mu.Unlock()
		return ErrPeerUnauthorized
	}
	if !transport.peerCapacityAvailableLocked() {
		transport.mu.Unlock()
		return ErrRegistryBound
	}
	peer := transport.newPeer(node)
	transport.peers = append(transport.peers, peer)
	transport.byNode[node] = peer
	if transport.state.Load() == transportRunning {
		transport.startPeerLocked(peer)
	}
	transport.mu.Unlock()
	return nil
}

// EnrollPeer atomically publishes a physical enrollment and installs its
// queue before the enrollment becomes visible to ordinary frame admission.
func (transport *OrdinaryTransport) EnrollPeer(
	intent EnrollmentIntent,
	verifier EnrollmentVerifier,
) error {
	return transport.EnrollPeerContext(context.Background(), intent, verifier)
}

// EnrollPeerContext carries cancellation into a remote catalog verifier while
// retaining the same queue-before-directory publication fence.
func (transport *OrdinaryTransport) EnrollPeerContext(
	ctx context.Context,
	intent EnrollmentIntent,
	verifier EnrollmentVerifier,
) error {
	if transport == nil || transport.registry == nil {
		return ErrInvalidTransport
	}
	return transport.registry.EnrollPeerContextWithCommit(ctx, intent, verifier, func() error {
		return transport.addPeerPrepared(intent.Peer)
	})
}

// EnrollMember is the atomic physical-peer plus existing-group member
// enrollment used immediately before a certified membership grant.  It does
// not publish MemberVoter or MemberLearner authority.
func (transport *OrdinaryTransport) EnrollMember(
	intent EnrollmentIntent,
	verifier EnrollmentVerifier,
) error {
	return transport.EnrollMemberContext(context.Background(), intent, verifier)
}

// EnrollMemberContext is the cancellation-aware exact-member enrollment path.
// Verifier I/O completes before registry/transport locks are taken.
func (transport *OrdinaryTransport) EnrollMemberContext(
	ctx context.Context,
	intent EnrollmentIntent,
	verifier EnrollmentVerifier,
) error {
	if transport == nil || transport.registry == nil {
		return ErrInvalidTransport
	}
	return transport.registry.EnrollMemberContextWithCommit(ctx, intent, verifier, func() error {
		return transport.addPeerPrepared(intent.Peer)
	})
}

// addPeerPrepared is called only from a registry commit callback while the
// registry's dynamic publication lock is held. The new directory record is
// intentionally not visible yet; installing the queue first closes the
// publication window in which frame admission could observe a peer without a
// matching worker. The expected record is therefore validated locally rather
// than re-reading the not-yet-published registry.
func (transport *OrdinaryTransport) addPeerPrepared(expected PhysicalPeer) error {
	node := expected.NodeID
	if node == (NodeID{}) || node == transport.registry.LocalNode() ||
		expected.State != PeerEnrolled || expected.TrustDomain != transport.registry.TrustDomain() {
		return ErrInvalidTransport
	}
	transport.mu.Lock()
	defer transport.mu.Unlock()
	if existing := transport.byNode[node]; existing != nil {
		if existing.retiring {
			return ErrPeerConflict
		}
		return nil
	}
	if transport.state.Load() == transportClosed || context.Cause(transport.ctx) != nil {
		return ErrTransportClosed
	}
	if !transport.peerCapacityAvailableLocked() {
		return ErrRegistryBound
	}
	peer := transport.newPeer(node)
	transport.peers = append(transport.peers, peer)
	transport.byNode[node] = peer
	if transport.state.Load() == transportRunning {
		transport.startPeerLocked(peer)
	}
	return nil
}

// peerCapacityAvailableLocked accounts for the fixed per-peer ring and
// coalescing scratch allocated by newPeer. The frame/byte queues remain
// separately bounded at send time, but their retained slices must also stay
// within the process-wide arena when churn adds idle peers after Run starts.
// transport.mu must be held by the caller.
func (transport *OrdinaryTransport) peerCapacityAvailableLocked() bool {
	if transport == nil {
		return false
	}
	next := len(transport.peers) + 1
	if next > peerLimit(transport.registry.limits) || next > AbsoluteMaxTransportPeers {
		return false
	}
	if int64(next)*int64(transport.queueLimits.PerPeerFrames) > int64(transport.queueLimits.GlobalFrames) {
		return false
	}
	return int64(next)*int64(transport.coalesce.RetainedBytes) <= AbsoluteMaxRetainedCoalesceBytes
}

func (transport *OrdinaryTransport) startPeerLocked(peer *ordinaryPeer) {
	if peer == nil || peer.started || peer.retiring {
		return
	}
	peer.started = true
	transport.workerWG.Add(1)
	go func() {
		defer transport.workerWG.Done()
		defer close(peer.done)
		transport.runPeer(peer)
	}()
}

// RetirePeer removes one transport worker only after the registry proves that
// no current authority or transition references the physical identity and the
// queue has no outstanding send.  The worker is joined before its map entry is
// forgotten, which prevents abandoned reconnect goroutines during churn.
func (transport *OrdinaryTransport) RetirePeer(node NodeID) error {
	if transport == nil || node == (NodeID{}) {
		return ErrNodeNotFound
	}
	peerRecord, err := transport.registry.PhysicalPeer(node)
	if err != nil {
		return err
	}
	proof := PeerRetirementProof{
		NodeID: node, Incarnation: peerRecord.Incarnation, Revision: peerRecord.Revision,
		DirectoryRevision: transport.registry.PeerDirectoryRevision(),
		DirectoryDigest:   transport.registry.PeerDirectoryDigest(),
	}
	return transport.RetirePeerWithProof(proof)
}

func (transport *OrdinaryTransport) RetirePeerWithProof(proof PeerRetirementProof) error {
	if transport == nil || proof.NodeID == (NodeID{}) {
		return ErrNodeNotFound
	}
	var peer *ordinaryPeer
	var connection PeerConnection
	var started bool
	err := transport.registry.RetirePhysicalPeer(proof, func() error {
		transport.mu.Lock()
		defer transport.mu.Unlock()
		peer = transport.byNode[proof.NodeID]
		if peer == nil {
			// A registry may be used without a local queue (for example before
			// an empty node starts its transport).  Retiring its directory entry
			// is still safe; there is no worker or pending frame to join.
			return nil
		}
		if peer.retiring {
			return ErrPeerConflict
		}
		if peer.count != 0 || peer.reservedFrames != 0 || peer.bytes != 0 || peer.reservedBytes != 0 ||
			peer.inFlightFrames != 0 || peer.inFlightBytes != 0 {
			return ErrPeerBusy
		}
		peer.retiring = true
		started = peer.started
		connection = peer.connection
		return nil
	})
	if err != nil {
		return err
	}
	// All network operations happen after the registry and transport locks have
	// been released.  Closing the connection interrupts a blocked write/read.
	if peer == nil {
		return nil
	}
	peer.cancel()
	if connection != nil {
		_ = connection.Close()
	}
	peer.notify()
	if started {
		<-peer.done
	} else {
		// No worker owns a ready transport.  A closed channel keeps the join
		// invariant true for callers that retire before Run.
		close(peer.done)
	}
	transport.mu.Lock()
	if transport.byNode[proof.NodeID] == peer {
		delete(transport.byNode, proof.NodeID)
		for index, candidate := range transport.peers {
			if candidate != peer {
				continue
			}
			copy(transport.peers[index:], transport.peers[index+1:])
			transport.peers[len(transport.peers)-1] = nil
			transport.peers = transport.peers[:len(transport.peers)-1]
			break
		}
	}
	transport.mu.Unlock()
	return nil
}

// RemovePeer is a lifecycle spelling retained for transport owners that call
// the operation remove step rather than retirement.
func (transport *OrdinaryTransport) RemovePeer(node NodeID) error {
	return transport.RetirePeer(node)
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
		if errors.Is(err, errRetiredOutboundDestination) {
			return nil // Committed removal canceled this queued ordinary packet.
		}
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
	frameBytes := int64(ownedSize)
	transport.mu.Lock()
	peer := transport.byNode[plan.destination]
	if peer == nil {
		transport.mu.Unlock()
		return nil, ErrNodeNotFound
	}
	closed := transport.state.Load() != transportRunning || context.Cause(transport.ctx) != nil
	if closed || peer.retiring ||
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
	var connectionDone <-chan struct{}
	failures := uint32(0)
	closeConnection := func() {
		if stopConnection != nil {
			stopConnection()
			stopConnection = nil
		}
		if connection != nil {
			_ = connection.Close()
			// Close interrupts the reader. Join it before replacing the stream,
			// keeping at most one reader and one writer per peer.
			<-connectionDone
			transport.clearConnection(peer)
			connection, connectionDone = nil, nil
		}
	}
	defer closeConnection()

	for {
		if context.Cause(peer.ctx) != nil {
			return
		}
		select {
		case <-connectionDone:
			closeConnection()
			failures = saturatingIncrement(failures)
		default:
		}
		if !transport.peerHasFrames(peer) {
			select {
			case <-peer.ctx.Done():
				return
			case <-connectionDone:
				continue
			case <-peer.wake:
			}
		}
		if context.Cause(peer.ctx) != nil {
			return
		}
		if connection == nil {
			if failures != 0 {
				delay := transport.reconnectDelay(failures)
				if err := transport.wait(peer.ctx, delay, nil); err != nil {
					if context.Cause(peer.ctx) == nil {
						transport.cancel(err)
					}
					return
				}
			}
			peer.dialAttempts.Add(1)
			physical, physicalErr := transport.registry.PhysicalPeer(peer.node)
			if physicalErr != nil || physical.State != PeerEnrolled {
				return
			}
			candidate, err := transport.dialOrdinary(peer.ctx, peer.node, physical.Endpoint)
			identity := PeerIdentity{}
			if candidate != nil {
				identity = candidate.PeerIdentity()
			}
			if err != nil || candidate == nil || !validPeerIdentity(identity) ||
				identity.Node != peer.node ||
				identity.TrustDomain != transport.registry.TrustDomain() ||
				!transport.registry.IsPeerEnrolled(identity.Node) ||
				candidate.TrafficClass() != TrafficOrdinary ||
				transport.registry.VerifyPeerConnectionBinding(candidate) != nil {
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
				peer.ctx, func() { _ = active.Close() },
			)
			done := make(chan struct{})
			connectionDone = done
			go func() {
				defer close(done)
				// Ordinary streams are unidirectional. Read the unused return
				// lane to observe TLS close_notify/EOF even when buffered TCP
				// writes still succeed. Any return data is also invalid here.
				var unexpected [1]byte
				for {
					n, err := active.Read(unexpected[:])
					if n != 0 || err != nil {
						_ = active.Close()
						return
					}
				}
			}()
		}

		if transport.shouldDelayCoalesce(peer) && transport.coalesce.MaxDelay != 0 {
			if err := transport.wait(
				peer.ctx, transport.coalesce.MaxDelay, peer.wake,
			); err != nil {
				if context.Cause(peer.ctx) == nil {
					transport.cancel(err)
				}
				return
			}
		}
		batch, frames := transport.buildPeerBatch(peer)
		if frames == 0 {
			continue
		}
		// The directory may rotate a node's service key while this pooled
		// stream is alive. Recheck immediately before writing each batch; the
		// receiver performs the same per-frame check on the other side.
		physical, physicalErr := transport.registry.PhysicalPeer(peer.node)
		if physicalErr != nil || physical.State != PeerEnrolled ||
			transport.registry.VerifyPeerConnectionBinding(connection) != nil ||
			physical.ServiceKeyDigest != ([sha256.Size]byte{}) &&
				connection.PeerKeyDigest() != physical.ServiceKeyDigest {
			transport.releasePeerBatch(peer)
			closeConnection()
			failures = saturatingIncrement(failures)
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
			closeConnection()
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

func (transport *OrdinaryTransport) dialOrdinary(
	ctx context.Context,
	node NodeID,
	endpoint string,
) (PeerConnection, error) {
	if endpoint != "" {
		if endpointDialer, ok := transport.dialer.(OrdinaryEndpointDialer); ok {
			return endpointDialer.DialOrdinaryEndpoint(ctx, node, endpoint)
		}
	}
	return transport.dialer.DialOrdinary(ctx, node)
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
	ownedBytes := int64(0)
	for frames < peer.count && frames < transport.coalesce.MaxFrames {
		index := (peer.head + frames) % len(peer.queue)
		buffer := peer.queue[index].buffer
		frame := buffer.bytes
		if wireBytes+StreamRecordHeaderBytes+len(frame) > transport.coalesce.MaxBytes {
			break
		}
		peer.batchFrames[frames] = peer.queue[index].buffer
		wireBytes += StreamRecordHeaderBytes + len(frame)
		ownedBytes += int64(cap(buffer.bytes))
		frames++
	}
	peer.inFlightFrames = frames
	peer.inFlightBytes = ownedBytes
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
	} else {
		clear(peer.writeBuffer)
		peer.writeBuffer = peer.writeBuffer[:0]
	}
	transport.mu.Lock()
	peer.inFlightFrames = 0
	peer.inFlightBytes = 0
	transport.mu.Unlock()
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
	transport.mu.Lock()
	peer := transport.byNode[node]
	if peer == nil {
		transport.mu.Unlock()
		return PeerStats{}, ErrNodeNotFound
	}
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
