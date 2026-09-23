package gatewayruntime

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// A periodic refresher that re-enters immediately must not starve a waiter
// that needs its own write fenced; the waiter is either served in FIFO order
// or covered by a round that started after its request.
func TestControlRefreshGateDoesNotStarveBehindReenteringRefresher(t *testing.T) {
	var gate controlRefreshGate
	stop := make(chan struct{})
	var rounds atomic.Uint64
	var periodic sync.WaitGroup
	periodic.Add(1)
	go func() {
		defer periodic.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			ticket, run, err := gate.acquire(context.Background())
			if err != nil {
				t.Error(err)
				return
			}
			if run {
				rounds.Add(1)
				time.Sleep(2 * time.Millisecond)
				ticket.release(nil)
			}
		}
	}()
	defer func() { close(stop); periodic.Wait() }()
	for rounds.Load() == 0 {
		time.Sleep(time.Millisecond)
	}
	for attempt := 0; attempt < 50; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		ticket, run, err := gate.acquire(ctx)
		cancel()
		if err != nil {
			t.Fatalf("attempt %d starved behind the periodic refresher: %v", attempt, err)
		}
		if run {
			ticket.release(nil)
		}
	}
}

func TestControlRefreshGateCoalescesOnlyRoundsStartedAfterRequest(t *testing.T) {
	var gate controlRefreshGate
	first, run, err := gate.acquire(context.Background())
	if err != nil || !run {
		t.Fatalf("first acquire run=%t err=%v", run, err)
	}
	type result struct {
		ticket controlRefreshTicket
		run    bool
		err    error
	}
	// Both waiters arrive while round 1 (which began before them) is running.
	early, late := make(chan result, 1), make(chan result, 1)
	go func() {
		ticket, run, err := gate.acquire(context.Background())
		early <- result{ticket, run, err}
	}()
	waitForControlRefreshWaiters(t, &gate, 1)
	go func() {
		ticket, run, err := gate.acquire(context.Background())
		late <- result{ticket, run, err}
	}()
	waitForControlRefreshWaiters(t, &gate, 2)
	// Round 1 predates both requests, so its success covers neither.
	first.release(nil)
	got := <-early
	if got.err != nil || !got.run {
		t.Fatalf("early waiter was covered by a round that began before it: run=%t err=%v", got.run, got.err)
	}
	// Round 2 began after the late request and succeeded: coalesce it.
	got.ticket.release(nil)
	if got := <-late; got.err != nil || got.run {
		t.Fatalf("late waiter ran a duplicate round: run=%t err=%v", got.run, got.err)
	}

	// A failed round covers nobody.
	failed, run, err := gate.acquire(context.Background())
	if err != nil || !run {
		t.Fatalf("acquire after coalesced round run=%t err=%v", run, err)
	}
	retry := make(chan result, 1)
	go func() {
		ticket, run, err := gate.acquire(context.Background())
		retry <- result{ticket, run, err}
	}()
	waitForControlRefreshWaiters(t, &gate, 1)
	failed.release(errors.New("receiver barrier failed"))
	got = <-retry
	if got.err != nil || !got.run {
		t.Fatalf("waiter after failed round run=%t err=%v", got.run, got.err)
	}
	got.ticket.release(nil)
	if gate.held || len(gate.waiters) != 0 {
		t.Fatalf("gate not idle: held=%t waiters=%d", gate.held, len(gate.waiters))
	}
}

func TestControlRefreshGateCancellationNeverStrandsOwnership(t *testing.T) {
	var gate controlRefreshGate
	holder, _, _ := gate.acquire(context.Background())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, _, err := gate.acquire(ctx)
		done <- err
	}()
	waitForControlRefreshWaiters(t, &gate, 1)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled waiter err=%v", err)
	}
	holder.release(nil)
	if gate.held || len(gate.waiters) != 0 {
		t.Fatalf("cancelled waiter left gate held=%t waiters=%d", gate.held, len(gate.waiters))
	}

	// Grant and cancellation racing: ownership must pass on, not leak.
	holder, _, _ = gate.acquire(context.Background())
	waiter := &controlRefreshWaiter{ready: make(chan struct{})}
	gate.mu.Lock()
	gate.waiters = append(gate.waiters, waiter)
	gate.handOffLocked() // simulate release granting the queued waiter
	gate.mu.Unlock()
	_ = holder
	cancelled, cancelNow := context.WithCancel(context.Background())
	cancelNow()
	gate.mu.Lock()
	if !waiter.granted {
		t.Fatal("waiter was not granted")
	}
	gate.handOffLocked() // the cancel path of a granted waiter
	gate.mu.Unlock()
	if gate.held {
		t.Fatal("granted-then-cancelled waiter stranded ownership")
	}
	if _, run, err := gate.acquire(cancelled); !errors.Is(err, context.Canceled) || run {
		t.Fatalf("acquire with cancelled context run=%t err=%v", run, err)
	}
}

func waitForControlRefreshWaiters(t *testing.T, gate *controlRefreshGate, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		gate.mu.Lock()
		got := len(gate.waiters)
		gate.mu.Unlock()
		if got == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("gate waiters never reached %d", want)
}
