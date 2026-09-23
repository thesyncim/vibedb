package storeio

import (
	"errors"
	"sync"
	"testing"
	"time"
)

// A waiter that observes a persistence failure must not hold waitMu while it
// waits for the failing worker to drain: the worker broadcasts under waitMu
// before it signals the drain, so holding the lock deadlocked both forever.
func TestCommitterWaitDoesNotHoldLockWhileFailureDrains(t *testing.T) {
	c := &Committer{done: make(chan struct{}), failureNotified: make(chan struct{})}
	c.wait = sync.NewCond(&c.waitMu)
	c.published.Store(1)

	waited := make(chan error, 1)
	go func() { waited <- c.Wait(1) }()
	for c.waiters.Load() == 0 {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(10 * time.Millisecond) // let the waiter park in the condition

	// The worker records the failure; an unrelated progress broadcast wakes the
	// waiter first, so it observes the failure before the worker's own
	// broadcast and drain signal.
	injected := errors.New("injected device failure")
	c.failure.Store(&commitFailure{err: injected})
	c.broadcast()
	time.Sleep(10 * time.Millisecond)

	worker := make(chan struct{})
	go func() {
		defer close(worker)
		c.broadcast()
		c.notifyFailureDrained()
	}()
	select {
	case <-worker:
	case <-time.After(5 * time.Second):
		t.Fatal("failing worker deadlocked broadcasting to a waiter that holds waitMu")
	}
	select {
	case err := <-waited:
		if !errors.Is(err, injected) {
			t.Fatalf("Wait err=%v, want injected failure", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("waiter never observed the drained failure")
	}
}
