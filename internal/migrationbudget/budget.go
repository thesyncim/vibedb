// Package migrationbudget bounds the work performed by replica migrations on
// one physical node.  A Budget is deliberately independent of a Raft group:
// callers construct one instance per process/node and pass the same pointer to
// every source and target transfer path owned by that node.
package migrationbudget

import (
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// ErrInvalidConfig indicates that a budget cannot provide a bounded,
	// cancellation-aware schedule.
	ErrInvalidConfig = errors.New("migrationbudget: invalid configuration")
	// ErrClosed indicates that the node budget has been shut down.
	ErrClosed = errors.New("migrationbudget: budget is closed")
	// ErrLeaseReleased indicates that a caller tried to account work after its
	// active permit was released.
	ErrLeaseReleased = errors.New("migrationbudget: lease is released")
)

const (
	// AbsoluteMaxActive prevents a malformed manifest from turning the budget
	// into an unbounded migration worker pool.
	AbsoluteMaxActive = 256
	// AbsoluteMaxRate bounds arithmetic in the pacing path and keeps a single
	// configuration from bypassing the intended resource guard.
	AbsoluteMaxRate = uint64(1 << 40)
	// AbsoluteMaxBurst bounds both the initial burst and the amount retained by
	// a token bucket. Oversized chunks are split into bounded reservations.
	AbsoluteMaxBurst = uint64(1 << 30)
	// AbsoluteMaxBufferBytes bounds the node-scoped transient migration
	// workspace pool. It is separate from active permits so receivers can wait
	// for memory without holding a heavyweight phase slot.
	AbsoluteMaxBufferBytes = uint64(1 << 32)
	// DefaultBufferBytes is the conservative transient workspace ceiling used
	// when an older in-memory caller omits the optional setting.
	DefaultBufferBytes = uint64(16 << 20)

	// pressureScaleMax is the fixed-point representation used by the
	// foreground-pressure controller.  Keeping it integral makes downshift
	// and recovery atomic and avoids a floating-point control path.
	pressureScaleMax = uint32(1_000_000)
	// PressureScaleMax is the full effective-rate multiplier in PressureStatus.
	PressureScaleMax = pressureScaleMax
)

// RateLimit describes one byte-work token bucket. BytesPerSecond is the
// sustained rate and BurstBytes is the largest reservation admitted at once.
// A request larger than BurstBytes is split into multiple paced reservations.
type RateLimit struct {
	BytesPerSecond uint64
	BurstBytes     uint64
}

// Config is a physical-node migration budget. The five classes are kept
// separate so a disk-heavy export cannot accidentally consume the network
// allowance, or a receive can be charged twice to the same bucket.
type Config struct {
	MaxActive int
	// BufferBytes bounds all transient migration chunk workspaces owned by the
	// physical node. Zero selects DefaultBufferBytes for legacy callers.
	BufferBytes uint64

	CPU            RateLimit
	DiskRead       RateLimit
	DiskWrite      RateLimit
	NetworkSend    RateLimit
	NetworkReceive RateLimit

	// Pressure controls the optional foreground-pressure feedback loop. A zero
	// value selects DefaultPressureConfig, which keeps existing in-memory
	// callers compatible while shipped manifests retain one conservative node
	// budget.
	Pressure PressureConfig
}

// PressureConfig describes the hysteresis used by ApplyPressure. Queue and
// wait thresholds are expressed as parts per million. Backpressure events
// always trigger a rate downshift; severe pressure must persist for
// PauseWindows before new heavyweight migration phases pause. Recovery is
// additive and requires RecoveryWindows quiet samples per step.
type PressureConfig struct {
	HighQueuePPM    uint32
	SevereQueuePPM  uint32
	LowQueuePPM     uint32
	HighWaitNanos   uint64
	SevereWaitNanos uint64
	PauseWindows    uint32
	RecoveryWindows uint32
	RecoveryStepPPM uint32
	MinimumScalePPM uint32
}

// DefaultPressureConfig keeps migration responsive to a saturated node log
// without treating a single transient queue sample as a failure. The sampler
// uses interval deltas for wait/backpressure counters and never derives
// pressure from cumulative totals directly.
func DefaultPressureConfig() PressureConfig {
	return PressureConfig{
		HighQueuePPM:    750_000,
		SevereQueuePPM:  900_000,
		LowQueuePPM:     250_000,
		HighWaitNanos:   uint64(25 * time.Millisecond),
		SevereWaitNanos: uint64(100 * time.Millisecond),
		PauseWindows:    2,
		RecoveryWindows: 3,
		RecoveryStepPPM: 125_000,
		MinimumScalePPM: 125_000,
	}
}

// DefaultConfig is intentionally conservative for a serving node. It keeps a
// small migration burst while leaving foreground Raft, SQL, and WAL work room
// on ordinary instances. Operators can raise rates after measuring their
// workload; the bounds remain enforceable at every value accepted here.
func DefaultConfig() Config {
	const (
		cpuRate      = uint64(64 << 20)
		diskRate     = uint64(64 << 20)
		netRate      = uint64(32 << 20)
		burst        = uint64(4 << 20)
		networkBurst = uint64(2 << 20)
	)
	return Config{
		MaxActive:      2,
		BufferBytes:    DefaultBufferBytes,
		CPU:            RateLimit{BytesPerSecond: cpuRate, BurstBytes: burst},
		DiskRead:       RateLimit{BytesPerSecond: diskRate, BurstBytes: burst},
		DiskWrite:      RateLimit{BytesPerSecond: diskRate, BurstBytes: burst},
		NetworkSend:    RateLimit{BytesPerSecond: netRate, BurstBytes: networkBurst},
		NetworkReceive: RateLimit{BytesPerSecond: netRate, BurstBytes: networkBurst},
		Pressure:       DefaultPressureConfig(),
	}
}

// Validate checks every bound before any state is allocated.
func (config Config) Validate() error {
	if config.MaxActive <= 0 || config.MaxActive > AbsoluteMaxActive {
		return ErrInvalidConfig
	}
	if config.BufferBytes > AbsoluteMaxBufferBytes {
		return ErrInvalidConfig
	}
	for _, limit := range [...]RateLimit{
		config.CPU, config.DiskRead, config.DiskWrite,
		config.NetworkSend, config.NetworkReceive,
	} {
		if limit.BytesPerSecond == 0 || limit.BytesPerSecond > AbsoluteMaxRate ||
			limit.BurstBytes == 0 || limit.BurstBytes > AbsoluteMaxBurst {
			return ErrInvalidConfig
		}
	}
	if config.Pressure != (PressureConfig{}) {
		if err := config.Pressure.validate(); err != nil {
			return err
		}
	}
	return nil
}

func (config PressureConfig) validate() error {
	if config.HighQueuePPM == 0 || config.HighQueuePPM > pressureScaleMax ||
		config.SevereQueuePPM < config.HighQueuePPM || config.SevereQueuePPM > pressureScaleMax ||
		config.LowQueuePPM >= config.HighQueuePPM ||
		config.HighWaitNanos == 0 || config.SevereWaitNanos < config.HighWaitNanos ||
		config.PauseWindows == 0 || config.RecoveryWindows == 0 ||
		config.RecoveryStepPPM == 0 || config.RecoveryStepPPM > pressureScaleMax ||
		config.MinimumScalePPM == 0 || config.MinimumScalePPM > pressureScaleMax {
		return ErrInvalidConfig
	}
	return nil
}

// Cost is one bounded unit of byte-work. CPU is a serialized snapshot
// encoding/hash workload; disk fields describe local repository/stage I/O;
// network fields describe bytes crossing the authenticated transfer stream.
type Cost struct {
	CPU            uint64
	DiskRead       uint64
	DiskWrite      uint64
	NetworkSend    uint64
	NetworkReceive uint64
}

// PressureSample is one interval observation from the physical node's
// foreground durability lane. Counters are deltas since the previous sample;
// QueueDepth and QueueCapacity are instantaneous gauges. Sequence is a local
// monotonic sample number and is the freshness authority. Timestamp is kept
// for diagnostics and scheduling visibility only.
type PressureSample struct {
	Sequence                uint64
	Timestamp               time.Time
	QueueDepth              uint64
	QueueCapacity           uint64
	BackpressureSubmissions uint64
	ReadyQueueWaitNanos     uint64
	ReadySubmissions        uint64
	Initial                 bool
}

// PressureStatus is a detached, constant-size view of foreground feedback.
// ScalePPM is the current multiplier on the configured migration rates. The
// configured rates and bursts remain hard ceilings even when pressure is
// clear. Paused is explicit so operators can distinguish foreground headroom
// from a stalled migration transport.
type PressureStatus struct {
	Sequence                uint64
	Timestamp               time.Time
	QueuePressurePPM        uint32
	WaitPressurePPM         uint32
	BackpressureSubmissions uint64
	BackpressureTotal       uint64
	ScalePPM                uint32
	Paused                  bool
	HighWindows             uint32
	SevereWindows           uint32
	LowWindows              uint32
	Downshifts              uint64
	RecoverySteps           uint64
}

// Resource identifies one token bucket when a caller needs to segment an
// actual I/O operation at the configured burst boundary.
type Resource uint8

const (
	ResourceCPU Resource = iota
	ResourceDiskRead
	ResourceDiskWrite
	ResourceNetworkSend
	ResourceNetworkReceive

	// Short aliases keep call sites readable while the Resource-prefixed names
	// remain unambiguous in larger packages.
	CPU            = ResourceCPU
	DiskRead       = ResourceDiskRead
	DiskWrite      = ResourceDiskWrite
	NetworkSend    = ResourceNetworkSend
	NetworkReceive = ResourceNetworkReceive
)

// BufferClass identifies the direction of a transfer workspace. Send and
// receive credits have separate ceilings so opposite moves cannot each hold
// the last node buffer while waiting for the peer to drain its socket.
type BufferClass uint8

const (
	BufferClassGeneral BufferClass = iota
	BufferClassSend
	BufferClassReceive
)

func (cost Cost) empty() bool {
	return cost == (Cost{})
}

// Timer is the small timer surface required by Clock. It allows deterministic
// test clocks to wake budget waiters without changing production scheduling.
type Timer interface {
	C() <-chan time.Time
	Stop() bool
}

// Clock supplies both monotonic scheduling time and timers. Production uses a
// real monotonic time source. Tests can advance a fake clock and return a
// controlled timer channel to make pacing deterministic.
type Clock interface {
	Now() time.Time
	NewTimer(time.Duration) Timer
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

func (realClock) NewTimer(delay time.Duration) Timer {
	return realTimer{Timer: time.NewTimer(delay)}
}

type realTimer struct{ *time.Timer }

func (timer realTimer) C() <-chan time.Time { return timer.Timer.C }

// Budget is a process-local, node-scoped scheduler. Active permits are held
// only by callers doing a heavyweight local phase. Token consumption is
// independent and may be used for a network receive after its wire read;
// keeping those phases separate avoids a source/target request deadlock.
type Budget struct {
	active          chan struct{}
	closed          chan struct{}
	close           sync.Once
	clock           Clock
	pressure        *pressureController
	pressureScaleMu sync.Mutex

	bufferMu       sync.Mutex
	bufferCapacity uint64
	bufferUsed     uint64
	bufferSendCap  uint64
	bufferRecvCap  uint64
	bufferSendUsed uint64
	bufferRecvUsed uint64
	bufferWake     chan struct{}

	cpu            tokenBucket
	diskRead       tokenBucket
	diskWrite      tokenBucket
	networkSend    tokenBucket
	networkReceive tokenBucket

	waiting        atomic.Int64
	acquires       atomic.Uint64
	cancellations  atomic.Uint64
	releases       atomic.Uint64
	consumeCalls   atomic.Uint64
	consumeErrors  atomic.Uint64
	bufferWaiters  atomic.Int64
	bufferAcquires atomic.Uint64
	bufferCancels  atomic.Uint64
	bufferReleases atomic.Uint64
}

// New constructs a production budget using a monotonic wall clock.
func New(config Config) (*Budget, error) {
	return NewWithClock(config, realClock{})
}

// NewWithClock is equivalent to New but accepts a deterministic clock for
// pacing tests and controlled simulations.
func NewWithClock(config Config, clock Clock) (*Budget, error) {
	if err := config.Validate(); err != nil || clock == nil {
		return nil, ErrInvalidConfig
	}
	pressureConfig := config.Pressure
	if pressureConfig == (PressureConfig{}) {
		pressureConfig = DefaultPressureConfig()
	}
	bufferCapacity := config.BufferBytes
	if bufferCapacity == 0 {
		bufferCapacity = DefaultBufferBytes
	}
	bufferSendCap := bufferCapacity / 2
	bufferRecvCap := bufferCapacity - bufferSendCap
	return &Budget{
		active:         make(chan struct{}, config.MaxActive),
		closed:         make(chan struct{}),
		clock:          clock,
		pressure:       newPressureController(pressureConfig),
		bufferCapacity: bufferCapacity,
		bufferSendCap:  bufferSendCap,
		bufferRecvCap:  bufferRecvCap,
		bufferWake:     make(chan struct{}),
		cpu:            newTokenBucket(config.CPU, clock.Now()),
		diskRead:       newTokenBucket(config.DiskRead, clock.Now()),
		diskWrite:      newTokenBucket(config.DiskWrite, clock.Now()),
		networkSend:    newTokenBucket(config.NetworkSend, clock.Now()),
		networkReceive: newTokenBucket(config.NetworkReceive, clock.Now()),
	}, nil
}

// BufferLease reserves transient migration workspace bytes from the
// node-scoped pool. It is intentionally separate from Lease: callers may wait
// for a buffer while no heavy local phase permit is held, avoiding a
// buffer/active deadlock between opposite ends of a move.
type BufferLease struct {
	budget   *Budget
	bytes    uint64
	class    BufferClass
	buffer   []byte
	released atomic.Bool
}

// AcquireBuffer waits for bytes in the node-scoped workspace pool. The
// reservation is cancellation-aware and never exceeds BufferBytes.
func (budget *Budget) AcquireBuffer(ctx context.Context, bytes uint64) (*BufferLease, error) {
	return budget.acquireBuffer(ctx, bytes, BufferClassGeneral)
}

// AcquireSendBuffer reserves a node-scoped workspace for bytes which will be
// written to the peer. Its dedicated half of the pool preserves progress when
// the peer is simultaneously sending a move back to this node.
func (budget *Budget) AcquireSendBuffer(ctx context.Context, bytes uint64) (*BufferLease, error) {
	return budget.acquireBuffer(ctx, bytes, BufferClassSend)
}

// AcquireReceiveBuffer reserves a node-scoped workspace for bytes read from a
// peer. Its dedicated half of the pool prevents bidirectional socket waits
// from forming a memory-credit cycle with senders.
func (budget *Budget) AcquireReceiveBuffer(ctx context.Context, bytes uint64) (*BufferLease, error) {
	return budget.acquireBuffer(ctx, bytes, BufferClassReceive)
}

// BufferCapacity returns the current static ceiling for one workspace class.
// General callers use the full pool; directional callers use their dedicated
// half. It is a sizing hint for chunk segmentation, not a reservation.
func (budget *Budget) BufferCapacity(class BufferClass) uint64 {
	if budget == nil {
		return 0
	}
	switch class {
	case BufferClassGeneral:
		return budget.bufferCapacity
	case BufferClassSend:
		return budget.bufferSendCap
	case BufferClassReceive:
		return budget.bufferRecvCap
	default:
		return 0
	}
}

func (budget *Budget) acquireBuffer(ctx context.Context, bytes uint64, class BufferClass) (*BufferLease, error) {
	if budget == nil || ctx == nil || bytes == 0 {
		return nil, ErrInvalidConfig
	}
	classCapacity := budget.BufferCapacity(class)
	if classCapacity == 0 || bytes > classCapacity {
		return nil, ErrInvalidConfig
	}
	for {
		if err := ctx.Err(); err != nil {
			budget.bufferCancels.Add(1)
			return nil, err
		}
		select {
		case <-budget.closed:
			return nil, ErrClosed
		default:
		}
		budget.bufferMu.Lock()
		classAvailable := classCapacity
		switch class {
		case BufferClassSend:
			classAvailable -= min(budget.bufferSendUsed, classCapacity)
		case BufferClassReceive:
			classAvailable -= min(budget.bufferRecvUsed, classCapacity)
		}
		if budget.bufferCapacity-budget.bufferUsed >= bytes && classAvailable >= bytes {
			budget.bufferUsed += bytes
			switch class {
			case BufferClassSend:
				budget.bufferSendUsed += bytes
			case BufferClassReceive:
				budget.bufferRecvUsed += bytes
			}
			budget.bufferAcquires.Add(1)
			budget.bufferMu.Unlock()
			return &BufferLease{budget: budget, bytes: bytes, class: class, buffer: make([]byte, bytes)}, nil
		}
		wake := budget.bufferWake
		budget.bufferWaiters.Add(1)
		budget.bufferMu.Unlock()
		select {
		case <-wake:
		case <-ctx.Done():
			budget.bufferWaiters.Add(-1)
			budget.bufferCancels.Add(1)
			return nil, ctx.Err()
		case <-budget.closed:
			budget.bufferWaiters.Add(-1)
			return nil, ErrClosed
		}
		budget.bufferWaiters.Add(-1)
	}
}

// Bytes returns the reserved workspace. The returned slice is valid until
// Release and has exactly the requested capacity.
func (lease *BufferLease) Bytes() []byte {
	if lease == nil || lease.released.Load() {
		return nil
	}
	return lease.buffer
}

// Release returns workspace bytes exactly once and wakes all waiters. The
// backing array is cleared before it becomes unreachable so a completed
// migration does not retain snapshot contents through a pool reference.
func (lease *BufferLease) Release() {
	if lease == nil || lease.budget == nil || lease.released.Swap(true) {
		return
	}
	clear(lease.buffer)
	lease.buffer = nil
	budget := lease.budget
	budget.bufferMu.Lock()
	if lease.bytes > budget.bufferUsed {
		budget.bufferUsed = 0
	} else {
		budget.bufferUsed -= lease.bytes
	}
	switch lease.class {
	case BufferClassSend:
		if lease.bytes > budget.bufferSendUsed {
			budget.bufferSendUsed = 0
		} else {
			budget.bufferSendUsed -= lease.bytes
		}
	case BufferClassReceive:
		if lease.bytes > budget.bufferRecvUsed {
			budget.bufferRecvUsed = 0
		} else {
			budget.bufferRecvUsed -= lease.bytes
		}
	}
	close(budget.bufferWake)
	budget.bufferWake = make(chan struct{})
	budget.bufferReleases.Add(1)
	budget.bufferMu.Unlock()
}

// Acquire waits for one active permit. Waiting is cancellation-aware and a
// canceled waiter never consumes capacity.
func (budget *Budget) Acquire(ctx context.Context) (*Lease, error) {
	if budget == nil || ctx == nil {
		return nil, ErrInvalidConfig
	}
	if err := ctx.Err(); err != nil {
		budget.cancellations.Add(1)
		return nil, err
	}
	if err := budget.WaitPressure(ctx); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			budget.cancellations.Add(1)
		}
		return nil, err
	}
	select {
	case <-budget.closed:
		return nil, ErrClosed
	default:
	}
	for {
		budget.waiting.Add(1)
		select {
		case budget.active <- struct{}{}:
			budget.waiting.Add(-1)
			if err := ctx.Err(); err != nil {
				<-budget.active
				budget.cancellations.Add(1)
				return nil, err
			}
			select {
			case <-budget.closed:
				<-budget.active
				return nil, ErrClosed
			default:
			}
			// A pressure pause may have started while this waiter was queued.
			// Return the permit before waiting for recovery so a paused phase never
			// occupies the last active slot.
			if budget.Pressure().Paused {
				<-budget.active
				if err := budget.WaitPressure(ctx); err != nil {
					if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
						budget.cancellations.Add(1)
					}
					return nil, err
				}
				continue
			}
			budget.acquires.Add(1)
			return &Lease{budget: budget}, nil
		case <-ctx.Done():
			budget.waiting.Add(-1)
			budget.cancellations.Add(1)
			return nil, ctx.Err()
		case <-budget.closed:
			budget.waiting.Add(-1)
			return nil, ErrClosed
		}
	}
}

// Consume paces byte-work without taking an active permit. It is useful for a
// phase that must not hold a local operation permit while waiting on a peer.
// All resources are charged exactly once per call; canceled waits never refund
// tokens because the caller may already have crossed an I/O boundary.
func (budget *Budget) Consume(ctx context.Context, cost Cost) error {
	if budget == nil || ctx == nil {
		return ErrInvalidConfig
	}
	if cost.empty() {
		return nil
	}
	if err := budget.consume(ctx, cost); err != nil {
		budget.consumeErrors.Add(1)
		return err
	}
	return nil
}

// ConsumeChunk reserves one bounded part of cost and returns the exact amount
// reserved for each resource. A pressure downshift while the caller waits may
// make a returned field smaller than requested; callers must use the returned
// amount to bound the actual I/O or hash operation. Unlike Consume, this
// method never loops to satisfy the entire request.
func (budget *Budget) ConsumeChunk(ctx context.Context, cost Cost) (Cost, error) {
	if budget == nil || ctx == nil {
		return Cost{}, ErrInvalidConfig
	}
	if cost.empty() {
		return Cost{}, nil
	}
	result, err := budget.consumeChunk(ctx, cost)
	if err != nil {
		budget.consumeErrors.Add(1)
		return result, err
	}
	return result, nil
}

// Burst returns the maximum bytes a single actual operation should issue for
// a resource. It is zero for a nil budget or unknown resource.
func (budget *Budget) Burst(resource Resource) uint64 {
	if budget == nil {
		return 0
	}
	switch resource {
	case ResourceCPU:
		return budget.cpu.currentBurst()
	case ResourceDiskRead:
		return budget.diskRead.currentBurst()
	case ResourceDiskWrite:
		return budget.diskWrite.currentBurst()
	case ResourceNetworkSend:
		return budget.networkSend.currentBurst()
	case ResourceNetworkReceive:
		return budget.networkReceive.currentBurst()
	default:
		return 0
	}
}

// ApplyPressure feeds one fresh physical-node sample into the migration
// controller. It never waits on or takes a host/authority lock. Stale or
// duplicate sequence numbers are ignored, and an Initial sample establishes a
// baseline without interpreting cumulative counters as interval pressure.
func (budget *Budget) ApplyPressure(sample PressureSample) {
	if budget == nil || budget.pressure == nil || sample.Sequence == 0 {
		return
	}
	budget.pressureScaleMu.Lock()
	defer budget.pressureScaleMu.Unlock()
	previousScale, nextScale := budget.pressure.apply(sample)
	if previousScale != nextScale {
		budget.setPressureScale(nextScale)
	}
}

// Pressure returns the detached foreground-pressure state.
func (budget *Budget) Pressure() PressureStatus {
	if budget == nil || budget.pressure == nil {
		return PressureStatus{}
	}
	return budget.pressure.status()
}

// WaitPressure blocks only while the controller has paused new heavyweight
// migration work. Existing bounded I/O can finish and cancellation always
// wakes the caller. The budget close channel is separate so shutdown does not
// require taking the pressure mutex.
func (budget *Budget) WaitPressure(ctx context.Context) error {
	if budget == nil || budget.pressure == nil || ctx == nil {
		return ErrInvalidConfig
	}
	return budget.pressure.wait(ctx, budget.closed)
}

func (budget *Budget) setPressureScale(scale uint32) {
	if scale == 0 {
		scale = 1
	}
	now := budget.clock.Now()
	budget.cpu.setScale(scale, now)
	budget.diskRead.setScale(scale, now)
	budget.diskWrite.setScale(scale, now)
	budget.networkSend.setScale(scale, now)
	budget.networkReceive.setScale(scale, now)
}

// Close wakes all waiters. Existing leases remain valid until released, so a
// caller can finish a monotonic cleanup path after the budget is stopped.
func (budget *Budget) Close() error {
	if budget == nil {
		return nil
	}
	budget.close.Do(func() { close(budget.closed) })
	return nil
}

// Lease holds one active permit. Release is idempotent and safe from a defer.
type Lease struct {
	budget   *Budget
	released atomic.Bool
}

// Consume charges work while retaining the lease. Any failure releases the
// permit automatically, which makes cancellation safe even when a caller
// returns directly from an error path.
func (lease *Lease) Consume(ctx context.Context, cost Cost) error {
	if lease == nil || lease.budget == nil || lease.released.Load() {
		return ErrLeaseReleased
	}
	if err := lease.budget.consumeWithPressure(ctx, cost, false); err != nil {
		lease.Release()
		return err
	}
	return nil
}

// ConsumeChunk is the bounded counterpart to Lease.Consume. The lease is
// released on cancellation or any other pacing error, matching Consume's
// failure semantics.
func (lease *Lease) ConsumeChunk(ctx context.Context, cost Cost) (Cost, error) {
	if lease == nil || lease.budget == nil || lease.released.Load() {
		return Cost{}, ErrLeaseReleased
	}
	result, err := lease.budget.consumeChunkWithPressure(ctx, cost, false)
	if err != nil {
		lease.Release()
	}
	return result, err
}

// Release returns the active permit exactly once. Token reservations are
// intentionally never refunded, including on cancellation or transport
// uncertainty.
func (lease *Lease) Release() {
	if lease == nil || lease.budget == nil || lease.released.Swap(true) {
		return
	}
	lease.budget.releases.Add(1)
	<-lease.budget.active
}

func (budget *Budget) consume(ctx context.Context, cost Cost) error {
	return budget.consumeWithPressure(ctx, cost, true)
}

func (budget *Budget) consumeWithPressure(ctx context.Context, cost Cost, waitPressure bool) error {
	if ctx == nil {
		return ErrInvalidConfig
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if waitPressure {
		if err := budget.WaitPressure(ctx); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-budget.closed:
		return ErrClosed
	default:
	}
	budget.consumeCalls.Add(1)
	// Keep the order stable. A canceled operation may have consumed an earlier
	// class already; refusing to refund makes retries conservative and avoids
	// duplicated refunds around durable cursor boundaries.
	if err := budget.cpu.take(ctx, cost.CPU, budget, waitPressure); err != nil {
		return err
	}
	if err := budget.diskRead.take(ctx, cost.DiskRead, budget, waitPressure); err != nil {
		return err
	}
	if err := budget.diskWrite.take(ctx, cost.DiskWrite, budget, waitPressure); err != nil {
		return err
	}
	if err := budget.networkSend.take(ctx, cost.NetworkSend, budget, waitPressure); err != nil {
		return err
	}
	return budget.networkReceive.take(ctx, cost.NetworkReceive, budget, waitPressure)
}

// consumeChunk is the single-reservation implementation. It is kept
// separate from consume so the legacy Consume API can retain its exact
// all-bytes semantics while actual I/O paths opt into returned segmentation.
func (budget *Budget) consumeChunk(ctx context.Context, cost Cost) (Cost, error) {
	return budget.consumeChunkWithPressure(ctx, cost, true)
}

func (budget *Budget) consumeChunkWithPressure(ctx context.Context, cost Cost, waitPressure bool) (Cost, error) {
	if ctx == nil {
		return Cost{}, ErrInvalidConfig
	}
	if err := ctx.Err(); err != nil {
		return Cost{}, err
	}
	if waitPressure {
		if err := budget.WaitPressure(ctx); err != nil {
			return Cost{}, err
		}
	}
	select {
	case <-budget.closed:
		return Cost{}, ErrClosed
	default:
	}
	budget.consumeCalls.Add(1)
	result := Cost{}
	var err error
	result.CPU, err = budget.cpu.takeChunk(ctx, cost.CPU, budget, waitPressure)
	if err != nil {
		return result, err
	}
	result.DiskRead, err = budget.diskRead.takeChunk(ctx, cost.DiskRead, budget, waitPressure)
	if err != nil {
		return result, err
	}
	result.DiskWrite, err = budget.diskWrite.takeChunk(ctx, cost.DiskWrite, budget, waitPressure)
	if err != nil {
		return result, err
	}
	result.NetworkSend, err = budget.networkSend.takeChunk(ctx, cost.NetworkSend, budget, waitPressure)
	if err != nil {
		return result, err
	}
	result.NetworkReceive, err = budget.networkReceive.takeChunk(ctx, cost.NetworkReceive, budget, waitPressure)
	return result, err
}

// Metrics is a constant-size detached view of the node budget.
type Metrics struct {
	Active                     int
	ActiveCapacity             int
	Waiting                    int
	Acquires                   uint64
	Cancellations              uint64
	Releases                   uint64
	ConsumeCalls               uint64
	ConsumeErrors              uint64
	BufferUsedBytes            uint64
	BufferCapacityBytes        uint64
	BufferSendUsedBytes        uint64
	BufferSendCapacityBytes    uint64
	BufferReceiveUsedBytes     uint64
	BufferReceiveCapacityBytes uint64
	BufferWaiters              int
	BufferAcquires             uint64
	BufferCancellations        uint64
	BufferReleases             uint64

	CPU            ResourceMetrics
	DiskRead       ResourceMetrics
	DiskWrite      ResourceMetrics
	NetworkSend    ResourceMetrics
	NetworkReceive ResourceMetrics
	Pressure       PressureStatus
}

// ResourceMetrics contains cumulative work and current bucket availability.
// AvailableBytes is a scheduling hint; the configured rate and burst remain
// authoritative bounds.
type ResourceMetrics struct {
	ConsumedBytes               uint64
	ThrottleEvents              uint64
	ThrottledNanos              uint64
	AvailableBytes              uint64
	RateBytesPerSecond          uint64
	BurstBytes                  uint64
	EffectiveRateBytesPerSecond uint64
	RateScalePPM                uint32
}

// Metrics returns current gauges and monotonic counters without borrowing
// mutable budget state.
func (budget *Budget) Metrics() Metrics {
	if budget == nil {
		return Metrics{}
	}
	result := Metrics{
		Active: len(budget.active), ActiveCapacity: cap(budget.active),
		Waiting: int(maxInt64(budget.waiting.Load())), Acquires: budget.acquires.Load(),
		Cancellations: budget.cancellations.Load(), Releases: budget.releases.Load(),
		ConsumeCalls: budget.consumeCalls.Load(), ConsumeErrors: budget.consumeErrors.Load(),
		BufferWaiters:  int(maxInt64(budget.bufferWaiters.Load())),
		BufferAcquires: budget.bufferAcquires.Load(), BufferCancellations: budget.bufferCancels.Load(),
		BufferReleases: budget.bufferReleases.Load(),
	}
	budget.bufferMu.Lock()
	result.BufferUsedBytes = budget.bufferUsed
	result.BufferCapacityBytes = budget.bufferCapacity
	result.BufferSendUsedBytes = budget.bufferSendUsed
	result.BufferSendCapacityBytes = budget.bufferSendCap
	result.BufferReceiveUsedBytes = budget.bufferRecvUsed
	result.BufferReceiveCapacityBytes = budget.bufferRecvCap
	budget.bufferMu.Unlock()
	result.CPU = budget.cpu.metrics(budget.clock.Now())
	result.DiskRead = budget.diskRead.metrics(budget.clock.Now())
	result.DiskWrite = budget.diskWrite.metrics(budget.clock.Now())
	result.NetworkSend = budget.networkSend.metrics(budget.clock.Now())
	result.NetworkReceive = budget.networkReceive.metrics(budget.clock.Now())
	result.Pressure = budget.Pressure()
	return result
}

// Stats is an intentionally short alias for Metrics for callers that expose a
// generic resource status endpoint.
func (budget *Budget) Stats() Metrics { return budget.Metrics() }

type tokenBucket struct {
	mu        sync.Mutex
	rate      uint64
	burst     uint64
	scale     uint32
	tokens    float64
	last      time.Time
	consumed  atomic.Uint64
	throttle  atomic.Uint64
	waitNanos atomic.Uint64
}

func newTokenBucket(limit RateLimit, now time.Time) tokenBucket {
	return tokenBucket{rate: limit.BytesPerSecond, burst: limit.BurstBytes,
		scale: pressureScaleMax, tokens: float64(limit.BurstBytes), last: now}
}

func (bucket *tokenBucket) setScale(scale uint32, now time.Time) {
	bucket.mu.Lock()
	if scale == 0 {
		scale = 1
	}
	effectiveRate := scaledRate(bucket.rate, bucket.scale)
	bucket.refillLocked(now, effectiveRate)
	bucket.scale = scale
	// Downshifting must not leave tokens that can create a catch-up burst.
	// Raising the scale never manufactures tokens; the next normal refill does
	// so at the new effective rate.
	capTokens := float64(scaledBurst(bucket.burst, scale))
	if bucket.tokens > capTokens {
		bucket.tokens = capTokens
	}
	bucket.mu.Unlock()
}

func (bucket *tokenBucket) take(ctx context.Context, amount uint64, budget *Budget, waitPressure bool) error {
	for amount != 0 {
		part := amount
		if burst := bucket.currentBurst(); part > burst {
			part = burst
		}
		consumed, err := bucket.takePart(ctx, part, budget, waitPressure)
		if err != nil {
			return err
		}
		amount -= consumed
	}
	return nil
}

func (bucket *tokenBucket) takeChunk(ctx context.Context, amount uint64, budget *Budget, waitPressure bool) (uint64, error) {
	if amount == 0 {
		return 0, nil
	}
	for {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		if waitPressure {
			if err := budget.WaitPressure(ctx); err != nil {
				return 0, err
			}
		}
		select {
		case <-budget.closed:
			return 0, ErrClosed
		default:
		}
		now := budget.clock.Now()
		bucket.mu.Lock()
		effectiveRate := scaledRate(bucket.rate, bucket.scale)
		bucket.refillLocked(now, effectiveRate)
		// Re-evaluate the dynamic burst after every wait. If pressure changed
		// while tokens were being accumulated, return only the newly legal
		// segment so a caller cannot emit a stale larger I/O.
		if maximum := scaledBurst(bucket.burst, bucket.scale); amount > maximum {
			amount = maximum
		}
		if bucket.tokens >= float64(amount) {
			bucket.tokens -= float64(amount)
			bucket.consumed.Add(amount)
			bucket.mu.Unlock()
			return amount, nil
		}
		missing := float64(amount) - bucket.tokens
		delay := pacingDelay(missing, effectiveRate)
		bucket.throttle.Add(1)
		bucket.mu.Unlock()
		started := budget.clock.Now()
		timer := budget.clock.NewTimer(delay)
		select {
		case <-timer.C():
			bucket.waitNanos.Add(durationNanosSince(started, budget.clock.Now()))
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C():
				default:
				}
			}
			return 0, ctx.Err()
		case <-budget.closed:
			if !timer.Stop() {
				select {
				case <-timer.C():
				default:
				}
			}
			return 0, ErrClosed
		}
	}
}

func (bucket *tokenBucket) currentBurst() uint64 {
	bucket.mu.Lock()
	burst := scaledBurst(bucket.burst, bucket.scale)
	bucket.mu.Unlock()
	return burst
}

func (bucket *tokenBucket) takePart(ctx context.Context, amount uint64, budget *Budget, waitPressure bool) (uint64, error) {
	for {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		if waitPressure {
			if err := budget.WaitPressure(ctx); err != nil {
				return 0, err
			}
		}
		select {
		case <-budget.closed:
			return 0, ErrClosed
		default:
		}
		now := budget.clock.Now()
		bucket.mu.Lock()
		effectiveRate := scaledRate(bucket.rate, bucket.scale)
		bucket.refillLocked(now, effectiveRate)
		if maximum := scaledBurst(bucket.burst, bucket.scale); amount > maximum {
			amount = maximum
		}
		if bucket.tokens >= float64(amount) {
			bucket.tokens -= float64(amount)
			bucket.consumed.Add(amount)
			bucket.mu.Unlock()
			return amount, nil
		}
		missing := float64(amount) - bucket.tokens
		delay := pacingDelay(missing, effectiveRate)
		bucket.throttle.Add(1)
		bucket.mu.Unlock()
		started := budget.clock.Now()
		timer := budget.clock.NewTimer(delay)
		select {
		case <-timer.C():
			bucket.waitNanos.Add(durationNanosSince(started, budget.clock.Now()))
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C():
				default:
				}
			}
			return 0, ctx.Err()
		case <-budget.closed:
			if !timer.Stop() {
				select {
				case <-timer.C():
				default:
				}
			}
			return 0, ErrClosed
		}
	}
}

func (bucket *tokenBucket) refillLocked(now time.Time, rate uint64) {
	if !now.After(bucket.last) {
		return
	}
	elapsed := now.Sub(bucket.last).Seconds()
	bucket.tokens = math.Min(float64(scaledBurst(bucket.burst, bucket.scale)), bucket.tokens+elapsed*float64(rate))
	bucket.last = now
}

func (bucket *tokenBucket) metrics(now time.Time) ResourceMetrics {
	bucket.mu.Lock()
	effectiveRate := scaledRate(bucket.rate, bucket.scale)
	bucket.refillLocked(now, effectiveRate)
	result := ResourceMetrics{
		ConsumedBytes:      bucket.consumed.Load(),
		ThrottleEvents:     bucket.throttle.Load(),
		ThrottledNanos:     bucket.waitNanos.Load(),
		AvailableBytes:     uint64(maxFloat(bucket.tokens)),
		RateBytesPerSecond: bucket.rate, BurstBytes: bucket.burst,
		EffectiveRateBytesPerSecond: effectiveRate, RateScalePPM: bucket.scale,
	}
	bucket.mu.Unlock()
	return result
}

func scaledRate(rate uint64, scale uint32) uint64 {
	if scale == 0 {
		return 1
	}
	result := (rate*uint64(scale) + uint64(pressureScaleMax) - 1) / uint64(pressureScaleMax)
	if result == 0 {
		return 1
	}
	return result
}

func scaledBurst(burst uint64, scale uint32) uint64 {
	if scale >= pressureScaleMax {
		return burst
	}
	result := (burst * uint64(scale)) / uint64(pressureScaleMax)
	if result == 0 {
		return 1
	}
	return result
}

func pacingDelay(missing float64, rate uint64) time.Duration {
	if missing <= 0 {
		return 0
	}
	seconds := missing / float64(rate)
	if seconds >= float64(math.MaxInt64)/float64(time.Second) {
		return time.Duration(math.MaxInt64)
	}
	delay := time.Duration(math.Ceil(seconds * float64(time.Second)))
	if delay <= 0 {
		return time.Nanosecond
	}
	return delay
}

func durationNanosSince(start, end time.Time) uint64 {
	if !end.After(start) {
		return 0
	}
	return uint64(end.Sub(start))
}

func maxFloat(value float64) float64 {
	if value <= 0 {
		return 0
	}
	if value >= float64(math.MaxUint64) {
		return float64(math.MaxUint64)
	}
	return value
}

func maxInt64(value int64) int64 {
	if value < 0 {
		return 0
	}
	return value
}
