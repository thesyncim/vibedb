package gatewayruntime

import (
	"context"
	"sync"
)

// controlRefreshGate serializes live control-directory refreshes with a FIFO
// hand-off and coalesces requests that a newer refresh already satisfied.
//
// The periodic refresher re-enters every tick. A polled TryLock lets it win
// every race, so a topology controller that needs its own publication fenced
// (split publish, table DDL) could wait indefinitely behind healthy rounds.
// Here ownership passes to the oldest waiter on release, and a waiter whose
// request predates a refresh that started later and succeeded returns without
// running a duplicate receiver barrier: that round read the directory after
// the waiter's own write. Waiting stays cancellable. The zero value is ready.
type controlRefreshGate struct {
	mu      sync.Mutex
	held    bool
	waiters []*controlRefreshWaiter
	// started counts refresh rounds that obtained ownership; covered is the
	// highest such round that completed successfully.
	started uint64
	covered uint64
}

type controlRefreshWaiter struct {
	ready   chan struct{}
	granted bool
}

// controlRefreshTicket is ownership of one refresh round. Release must be
// called exactly once with the round's outcome.
type controlRefreshTicket struct {
	gate  *controlRefreshGate
	round uint64
}

// acquire returns (ticket, true, nil) when the caller must run a refresh,
// (zero, false, nil) when a round that started after this request already
// succeeded, or the context cause when cancelled while waiting.
func (gate *controlRefreshGate) acquire(ctx context.Context) (controlRefreshTicket, bool, error) {
	if err := ctx.Err(); err != nil {
		return controlRefreshTicket{}, false, context.Cause(ctx)
	}
	gate.mu.Lock()
	// Any round numbered above this one begins after the request, so it reads
	// state at least as new as the caller's preceding writes.
	need := gate.started + 1
	if !gate.held {
		gate.held = true
		gate.started++
		round := gate.started
		gate.mu.Unlock()
		return controlRefreshTicket{gate: gate, round: round}, true, nil
	}
	waiter := &controlRefreshWaiter{ready: make(chan struct{})}
	gate.waiters = append(gate.waiters, waiter)
	gate.mu.Unlock()

	select {
	case <-waiter.ready:
	case <-ctx.Done():
		gate.mu.Lock()
		if !waiter.granted {
			gate.removeWaiterLocked(waiter)
			gate.mu.Unlock()
			return controlRefreshTicket{}, false, context.Cause(ctx)
		}
		// Ownership raced cancellation; pass it on rather than strand it.
		gate.handOffLocked()
		gate.mu.Unlock()
		return controlRefreshTicket{}, false, context.Cause(ctx)
	}
	gate.mu.Lock()
	defer gate.mu.Unlock()
	if gate.covered >= need {
		gate.handOffLocked()
		return controlRefreshTicket{}, false, nil
	}
	gate.started++
	return controlRefreshTicket{gate: gate, round: gate.started}, true, nil
}

func (ticket controlRefreshTicket) release(err error) {
	gate := ticket.gate
	if gate == nil {
		return
	}
	gate.mu.Lock()
	defer gate.mu.Unlock()
	if err == nil && ticket.round > gate.covered {
		gate.covered = ticket.round
	}
	gate.handOffLocked()
}

// handOffLocked transfers ownership to the oldest waiter, or frees the gate.
func (gate *controlRefreshGate) handOffLocked() {
	if len(gate.waiters) == 0 {
		gate.held = false
		return
	}
	next := gate.waiters[0]
	gate.waiters[0] = nil
	gate.waiters = gate.waiters[1:]
	next.granted = true
	close(next.ready)
}

func (gate *controlRefreshGate) removeWaiterLocked(waiter *controlRefreshWaiter) {
	for index, candidate := range gate.waiters {
		if candidate == waiter {
			gate.waiters = append(gate.waiters[:index], gate.waiters[index+1:]...)
			return
		}
	}
}
