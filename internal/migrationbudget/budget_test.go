package migrationbudget

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type fakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*fakeTimer
}

type fakeTimer struct {
	clock    *fakeClock
	deadline time.Time
	channel  chan time.Time
	mu       sync.Mutex
	stopped  bool
}

func newFakeClock() *fakeClock { return &fakeClock{now: time.Unix(10, 0)} }

func (clock *fakeClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.now
}

func (clock *fakeClock) NewTimer(delay time.Duration) Timer {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	timer := &fakeTimer{clock: clock, deadline: clock.now.Add(delay), channel: make(chan time.Time, 1)}
	clock.timers = append(clock.timers, timer)
	return timer
}

func (clock *fakeClock) Advance(delta time.Duration) {
	clock.mu.Lock()
	clock.now = clock.now.Add(delta)
	now := clock.now
	timers := append([]*fakeTimer(nil), clock.timers...)
	clock.mu.Unlock()
	for _, timer := range timers {
		timer.mu.Lock()
		if !timer.stopped && !now.Before(timer.deadline) {
			select {
			case timer.channel <- now:
			default:
			}
			timer.stopped = true
		}
		timer.mu.Unlock()
	}
}

func (timer *fakeTimer) C() <-chan time.Time { return timer.channel }

func (timer *fakeTimer) Stop() bool {
	timer.mu.Lock()
	defer timer.mu.Unlock()
	if timer.stopped {
		return false
	}
	timer.stopped = true
	return true
}

func testConfig() Config {
	limit := RateLimit{BytesPerSecond: 1, BurstBytes: 4}
	return Config{MaxActive: 1, CPU: limit, DiskRead: limit, DiskWrite: limit,
		NetworkSend: limit, NetworkReceive: limit}
}

func TestDefaultConfigValid(t *testing.T) {
	if err := DefaultConfig().Validate(); err != nil {
		t.Fatalf("DefaultConfig().Validate() = %v", err)
	}
	for _, invalid := range []Config{
		{},
		func() Config { c := DefaultConfig(); c.MaxActive = 0; return c }(),
		func() Config { c := DefaultConfig(); c.MaxActive = AbsoluteMaxActive + 1; return c }(),
		func() Config { c := DefaultConfig(); c.CPU.BytesPerSecond = 0; return c }(),
		func() Config { c := DefaultConfig(); c.DiskWrite.BurstBytes = AbsoluteMaxBurst + 1; return c }(),
	} {
		if err := invalid.Validate(); !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("invalid config error = %v", err)
		}
	}
}

func TestActivePermitIsSharedAndCancellationReleasesWaiter(t *testing.T) {
	budget, err := New(testConfig())
	if err != nil {
		t.Fatal(err)
	}
	first, err := budget.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		lease, acquireErr := budget.Acquire(ctx)
		if lease != nil {
			lease.Release()
		}
		result <- acquireErr
	}()
	deadline := time.Now().Add(time.Second)
	for budget.Metrics().Waiting == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	if acquireErr := <-result; !errors.Is(acquireErr, context.Canceled) {
		t.Fatalf("canceled acquire = %v", acquireErr)
	}
	if metrics := budget.Metrics(); metrics.Active != 1 || metrics.Waiting != 0 {
		t.Fatalf("metrics after canceled waiter = %+v", metrics)
	}
	first.Release()
	if metrics := budget.Metrics(); metrics.Active != 0 || metrics.Releases != 1 {
		t.Fatalf("metrics after release = %+v", metrics)
	}
}

func TestOversizedCostIsPacedInBoundedBursts(t *testing.T) {
	clock := newFakeClock()
	budget, err := NewWithClock(testConfig(), clock)
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- budget.Consume(context.Background(), Cost{CPU: 10}) }()
	deadline := time.Now().Add(time.Second)
	for budget.Metrics().CPU.ThrottleEvents == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := budget.Metrics().CPU.ConsumedBytes; got != 4 {
		t.Fatalf("initial burst consumed %d, want 4", got)
	}
	clock.Advance(4 * time.Second)
	deadline = time.Now().Add(time.Second)
	for budget.Metrics().CPU.ConsumedBytes < 8 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	clock.Advance(2 * time.Second)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if got := budget.Metrics().CPU.ConsumedBytes; got != 10 {
		t.Fatalf("consumed %d, want 10", got)
	}
}

func TestLeaseCancellationReleasesActiveCapacityWithoutRefund(t *testing.T) {
	clock := newFakeClock()
	budget, err := NewWithClock(testConfig(), clock)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := budget.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err = lease.Consume(context.Background(), Cost{CPU: 4}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- lease.Consume(ctx, Cost{CPU: 1}) }()
	deadline := time.Now().Add(time.Second)
	for budget.Metrics().CPU.ThrottleEvents == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("lease consume cancellation = %v", err)
	}
	if metrics := budget.Metrics(); metrics.Active != 0 || metrics.Releases != 1 {
		t.Fatalf("lease was not released: %+v", metrics)
	}
	if metrics := budget.Metrics(); metrics.CPU.ConsumedBytes != 4 {
		t.Fatalf("canceled cost was refunded/consumed: %+v", metrics.CPU)
	}
	lease.Release() // idempotent after automatic cancellation release.
}

func TestCloseWakesResourceWaiters(t *testing.T) {
	budget, err := New(testConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err = budget.Consume(context.Background(), Cost{CPU: 4}); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- budget.Consume(context.Background(), Cost{CPU: 1}) }()
	deadline := time.Now().Add(time.Second)
	for budget.Metrics().CPU.ThrottleEvents == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if err = budget.Close(); err != nil {
		t.Fatal(err)
	}
	if err = <-result; !errors.Is(err, ErrClosed) {
		t.Fatalf("closed wait = %v", err)
	}
}

func TestBufferPoolBoundsCancellationAndRelease(t *testing.T) {
	config := testConfig()
	config.BufferBytes = 8
	budget, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	first, err := budget.AcquireBuffer(context.Background(), 8)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		second, acquireErr := budget.AcquireBuffer(ctx, 1)
		if second != nil {
			second.Release()
		}
		result <- acquireErr
	}()
	deadline := time.Now().Add(time.Second)
	for budget.Metrics().BufferWaiters == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled buffer wait = %v", err)
	}
	if got := budget.Metrics(); got.BufferUsedBytes != 8 || got.BufferCapacityBytes != 8 || got.BufferCancellations != 1 {
		t.Fatalf("buffer metrics before release = %+v", got)
	}
	first.Release()
	second, err := budget.AcquireBuffer(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	second.Release()
	if got := budget.Metrics(); got.BufferUsedBytes != 0 || got.BufferReleases != 2 {
		t.Fatalf("buffer metrics after release = %+v", got)
	}
}

func TestDirectionalBufferCreditsAvoidOppositeSocketCycle(t *testing.T) {
	config := testConfig()
	config.BufferBytes = 8
	budget, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	send, err := budget.AcquireSendBuffer(context.Background(), 4)
	if err != nil {
		t.Fatal(err)
	}
	receive, err := budget.AcquireReceiveBuffer(context.Background(), 4)
	if err != nil {
		send.Release()
		t.Fatal(err)
	}
	if got := budget.Metrics(); got.BufferSendUsedBytes != 4 || got.BufferReceiveUsedBytes != 4 {
		t.Fatalf("directional reservations = %+v", got)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	results := make(chan error, 2)
	go func() {
		lease, acquireErr := budget.AcquireSendBuffer(ctx, 4)
		if lease != nil {
			lease.Release()
		}
		results <- acquireErr
	}()
	go func() {
		lease, acquireErr := budget.AcquireReceiveBuffer(ctx, 4)
		if lease != nil {
			lease.Release()
		}
		results <- acquireErr
	}()
	deadline := time.Now().Add(time.Second)
	for budget.Metrics().BufferWaiters < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if budget.Metrics().BufferWaiters != 2 {
		t.Fatalf("directional waiters = %+v", budget.Metrics())
	}
	cancel()
	for range 2 {
		if err := <-results; !errors.Is(err, context.Canceled) {
			t.Fatalf("directional cancellation = %v", err)
		}
	}
	send.Release()
	receive.Release()
	if got := budget.Metrics(); got.BufferUsedBytes != 0 || got.BufferSendUsedBytes != 0 || got.BufferReceiveUsedBytes != 0 {
		t.Fatalf("directional release = %+v", got)
	}
}

func TestConsumeChunkReturnsDownshiftedActualSegment(t *testing.T) {
	clock := newFakeClock()
	budget, err := NewWithClock(testConfig(), clock)
	if err != nil {
		t.Fatal(err)
	}
	if err := budget.Consume(context.Background(), Cost{CPU: 4}); err != nil {
		t.Fatal(err)
	}
	result := make(chan struct {
		cost Cost
		err  error
	}, 1)
	go func() {
		cost, consumeErr := budget.ConsumeChunk(context.Background(), Cost{CPU: 4})
		result <- struct {
			cost Cost
			err  error
		}{cost: cost, err: consumeErr}
	}()
	deadline := time.Now().Add(time.Second)
	for budget.Metrics().CPU.ThrottleEvents == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	budget.ApplyPressure(PressureSample{Sequence: 1, Initial: true, QueueCapacity: 4})
	budget.ApplyPressure(PressureSample{Sequence: 2, QueueDepth: 4, QueueCapacity: 4})
	clock.Advance(4 * time.Second)
	select {
	case got := <-result:
		if got.err != nil {
			t.Fatal(got.err)
		}
		if got.cost.CPU != 2 {
			t.Fatalf("downshifted segment = %+v, want CPU=2", got.cost)
		}
	case <-time.After(time.Second):
		t.Fatal("downshifted chunk did not wake")
	}
}
