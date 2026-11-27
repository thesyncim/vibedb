package controllerlease

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock(start time.Time) *fakeClock {
	return &fakeClock{now: start}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

func TestLeaseRecordBinaryCodecRoundTripAndValidation(t *testing.T) {
	rec := LeaseRecord{
		Holder:            "gateway-node-1/inc-42",
		Epoch:             7,
		ExpiresAtUnixNano: time.Date(2026, 11, 27, 18, 0, 0, 0, time.UTC).UnixNano(),
	}
	raw, err := rec.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary: %v", err)
	}
	got, err := UnmarshalBinary(raw)
	if err != nil {
		t.Fatalf("UnmarshalBinary: %v", err)
	}
	if got != rec {
		t.Fatalf("round-trip = %+v, want %+v", got, rec)
	}

	// Gracefully released record (Holder == "", ExpiresAtUnixNano == 0, Epoch > 0).
	released := LeaseRecord{Epoch: 7}
	rawRel, err := released.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary(released): %v", err)
	}
	gotRel, err := UnmarshalBinary(rawRel)
	if err != nil || gotRel != released {
		t.Fatalf("released round-trip = %+v, err=%v", gotRel, err)
	}

	// Malformed records must fail closed.
	if _, err := (LeaseRecord{}).MarshalBinary(); !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("zero record err = %v, want ErrInvalidRecord", err)
	}
	if _, err := UnmarshalBinary([]byte{1, 2, 3}); !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("short frame err = %v, want ErrInvalidRecord", err)
	}
	badVer := append([]byte(nil), raw...)
	badVer[0] = 99
	if _, err := UnmarshalBinary(badVer); !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("bad version err = %v, want ErrInvalidRecord", err)
	}
}

func TestElectorAcquireRenewFailoverAndFencing(t *testing.T) {
	store := NewMemoryStore()
	clock := newFakeClock(time.Date(2026, 11, 27, 18, 0, 0, 0, time.UTC))

	var leader1CtxCancelled atomic.Bool
	var leader1LostEpoch atomic.Uint64
	e1, err := NewElector(store, Config{
		Key:           "vibedb/controller/split",
		Holder:        "frontend-1",
		LeaseTTL:      10 * time.Second,
		RenewInterval: 3 * time.Second,
		RetryInterval: 2 * time.Second,
		Clock:         clock.Now,
		OnAcquire: func(ctx context.Context, _ uint64) {
			<-ctx.Done()
			leader1CtxCancelled.Store(true)
		},
		OnLose: func(epoch uint64) {
			leader1LostEpoch.Store(epoch)
		},
	})
	if err != nil {
		t.Fatalf("NewElector e1: %v", err)
	}

	e2, err := NewElector(store, Config{
		Key:           "vibedb/controller/split",
		Holder:        "frontend-2",
		LeaseTTL:      10 * time.Second,
		RenewInterval: 3 * time.Second,
		RetryInterval: 2 * time.Second,
		Clock:         clock.Now,
	})
	if err != nil {
		t.Fatalf("NewElector e2: %v", err)
	}

	ctx := t.Context()

	// 1. e1 acquires epoch 1; e2 cannot steal while unexpired.
	ok, epoch, err := e1.Step(ctx)
	if err != nil || !ok || epoch != 1 {
		t.Fatalf("e1 acquire = (%v, %d, %v), want (true, 1, nil)", ok, epoch, err)
	}
	ok2, obs2, err := e2.Step(ctx)
	if err != nil || ok2 || obs2 != 1 {
		t.Fatalf("e2 while e1 valid = (%v, %d, %v), want (false, 1, nil)", ok2, obs2, err)
	}

	// 2. e1 renews at +4s: keeps epoch 1 and extends deadline to +14s.
	clock.Advance(4 * time.Second)
	ok, epoch, err = e1.Step(ctx)
	if err != nil || !ok || epoch != 1 {
		t.Fatalf("e1 renew = (%v, %d, %v), want (true, 1, nil)", ok, epoch, err)
	}
	if curEpoch, curOK := e1.Lease(); !curOK || curEpoch != 1 {
		t.Fatalf("e1.Lease() after renew = (%d, %v), want (1, true)", curEpoch, curOK)
	}

	// 3. Simulate e1 pause past its renewed deadline (+11s -> total +15s > +14s).
	// Even before e1 runs Step, e1.Lease() MUST fail closed (no split brain).
	clock.Advance(11 * time.Second)
	if _, curOK := e1.Lease(); curOK {
		t.Fatal("e1.Lease() returned true past its lease deadline")
	}

	// 4. e2 acquires epoch 2 cleanly via CAS.
	ok2, epoch2, err := e2.Step(ctx)
	if err != nil || !ok2 || epoch2 != 2 {
		t.Fatalf("e2 failover = (%v, %d, %v), want (true, 2, nil)", ok2, epoch2, err)
	}
	if err := Guard(1, epoch2); !errors.Is(err, ErrStaleEpoch) {
		t.Fatalf("Guard(1, 2) = %v, want ErrStaleEpoch", err)
	}
	if err := Guard(epoch2, epoch2); err != nil {
		t.Fatalf("Guard(2, 2) = %v, want nil", err)
	}

	// 5. Paused e1 wakes up and runs Step: must step down cleanly and cancel its
	// leadership context without disturbing e2.
	ok, epoch, err = e1.Step(ctx)
	if err != nil || ok {
		t.Fatalf("paused e1 wakeup Step = (%v, %d, %v), want (false, _, nil)", ok, epoch, err)
	}
	if !leader1CtxCancelled.Load() {
		t.Fatal("e1 leadership context was not cancelled on step-down")
	}
	if leader1LostEpoch.Load() != 1 {
		t.Fatalf("e1 OnLose epoch = %d, want 1", leader1LostEpoch.Load())
	}
	if curEpoch, curOK := e2.Lease(); !curOK || curEpoch != 2 {
		t.Fatalf("e2.Lease() after e1 wakeup = (%d, %v), want (2, true)", curEpoch, curOK)
	}
}

func TestElectorGracefulReleaseImmediateHandoff(t *testing.T) {
	store := NewMemoryStore()
	clock := newFakeClock(time.Date(2026, 11, 27, 18, 0, 0, 0, time.UTC))

	e1, err := NewElector(store, Config{
		Key:      "vibedb/controller/replica_move",
		Holder:   "frontend-a",
		LeaseTTL: 30 * time.Second,
		Clock:    clock.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	e2, err := NewElector(store, Config{
		Key:      "vibedb/controller/replica_move",
		Holder:   "frontend-b",
		LeaseTTL: 30 * time.Second,
		Clock:    clock.Now,
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx := t.Context()
	if ok, epoch, err := e1.Step(ctx); err != nil || !ok || epoch != 1 {
		t.Fatalf("e1 acquire = (%v, %d, %v)", ok, epoch, err)
	}

	// Release without advancing clock (30s TTL still remaining).
	if err := e1.Release(ctx); err != nil {
		t.Fatalf("e1.Release: %v", err)
	}
	if _, ok := e1.Lease(); ok {
		t.Fatal("e1 still reports leader after Release")
	}

	// e2 acquires epoch 2 immediately at the exact same timestamp.
	if ok, epoch, err := e2.Step(ctx); err != nil || !ok || epoch != 2 {
		t.Fatalf("e2 immediate handoff = (%v, %d, %v), want (true, 2, nil)", ok, epoch, err)
	}
}

func TestElectorConcurrentContentionSingleLeader(t *testing.T) {
	store := NewMemoryStore()
	const workers = 5

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	var activeLeaders atomic.Int32
	var maxConcurrent atomic.Int32
	var lastEpoch atomic.Uint64
	var acquisitions atomic.Int64

	electors := make([]*Elector, workers)
	var wg sync.WaitGroup
	for i := range workers {
		e, err := NewElector(store, Config{
			Key:           "vibedb/controller/split",
			Holder:        fmt.Sprintf("frontend-%d", i),
			LeaseTTL:      60 * time.Millisecond,
			RenewInterval: 15 * time.Millisecond,
			RetryInterval: 10 * time.Millisecond,
			OnAcquire: func(lctx context.Context, epoch uint64) {
				cur := activeLeaders.Add(1)
				for {
					prev := maxConcurrent.Load()
					if cur <= prev || maxConcurrent.CompareAndSwap(prev, cur) {
						break
					}
				}
				prevEpoch := lastEpoch.Swap(epoch)
				if epoch <= prevEpoch {
					t.Errorf("non-monotonic epoch: got %d after %d", epoch, prevEpoch)
				}
				acquisitions.Add(1)
				<-lctx.Done()
				activeLeaders.Add(-1)
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		electors[i] = e
		wg.Add(1)
		go func(el *Elector) {
			defer wg.Done()
			_ = el.Run(ctx)
		}(e)
	}

	// Wait for initial election, then trigger two graceful releases to verify
	// clean failover under live contention.
	deadline := time.Now().Add(3 * time.Second)
	for acquisitions.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if acquisitions.Load() == 0 {
		t.Fatal("no frontend acquired lease")
	}

	for round := 0; round < 2; round++ {
		before := acquisitions.Load()
		for _, el := range electors {
			if _, ok := el.Lease(); ok {
				_ = el.Release(ctx)
				break
			}
		}
		waitUntil := time.Now().Add(2 * time.Second)
		for acquisitions.Load() == before && time.Now().Before(waitUntil) {
			time.Sleep(5 * time.Millisecond)
		}
	}

	cancel()
	wg.Wait()

	if mc := maxConcurrent.Load(); mc > 1 {
		t.Fatalf("split brain detected: max concurrent leaders = %d, want <= 1", mc)
	}
	if acquisitions.Load() < 2 {
		t.Fatalf("expected failover acquisitions >= 2, got %d", acquisitions.Load())
	}
}
