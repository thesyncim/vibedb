package replicacontrol

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"syscall"

	"github.com/thesyncim/vibedb/internal/rafttransport"
)

// HealthRound amortizes authenticated connection setup across one catalog sweep.
// Requests retain independent authorization, deadlines, and exact response checks.
// Close must be called at the end of the sweep.
type HealthRound struct {
	client  *Client
	ctx     context.Context
	cancel  context.CancelFunc
	mu      sync.Mutex
	closed  bool
	limit   int
	entries map[rafttransport.NodeID]*healthRoundEntry
}

type healthRoundEntry struct {
	gate       chan struct{}
	connection rafttransport.PeerConnection
}

func (client *Client) BeginHealthRound(ctx context.Context) *HealthRound {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithCancel(ctx)
	round := &HealthRound{client: client, ctx: ctx, cancel: cancel}
	if client != nil && client.healthRoundActive.CompareAndSwap(false, true) {
		round.limit = client.maxHealthRoundConnections
		// A zero-sized round does not own the cache reservation.
		if round.limit == 0 {
			client.healthRoundActive.Store(false)
		}
	}
	round.entries = make(map[rafttransport.NodeID]*healthRoundEntry)
	return round
}

func (round *HealthRound) Close() error {
	round.mu.Lock()
	if round.closed {
		round.mu.Unlock()
		return nil
	}
	round.closed = true
	round.cancel()
	entries := round.entries
	round.mu.Unlock()
	for _, entry := range entries {
		entry.gate <- struct{}{}
		if entry.connection != nil {
			_ = entry.connection.Close()
			entry.connection = nil
		}
		<-entry.gate
	}
	if round.limit > 0 {
		round.client.healthRoundActive.Store(false)
	}
	return nil
}

func (round *HealthRound) ObserveHealth(ctx context.Context, node rafttransport.NodeID, request Request) (HealthObservation, error) {
	request.HealthOnly = true
	if round == nil || round.client == nil || ctx == nil || node == (rafttransport.NodeID{}) || !validRequest(request) {
		return HealthObservation{}, ErrControl
	}
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(round.ctx, cancel)
	defer func() { stop(); cancel() }()
	round.mu.Lock()
	if round.closed || context.Cause(round.ctx) != nil {
		round.mu.Unlock()
		return HealthObservation{}, context.Canceled
	}
	entry := round.entries[node]
	if entry == nil && len(round.entries) < round.limit {
		entry = &healthRoundEntry{gate: make(chan struct{}, 1)}
		round.entries[node] = entry
	}
	round.mu.Unlock()
	if entry == nil {
		return round.client.ObserveHealth(ctx, node, request)
	}
	select {
	case entry.gate <- struct{}{}:
	case <-ctx.Done():
		return HealthObservation{}, context.Cause(ctx)
	}
	defer func() { <-entry.gate }()
	if err := context.Cause(round.ctx); err != nil {
		return HealthObservation{}, err
	}
	if err := context.Cause(ctx); err != nil {
		return HealthObservation{}, err
	}
	reused := entry.connection != nil
	for attempt := 0; attempt < 2; attempt++ {
		if entry.connection == nil {
			connection, err := round.client.opener.OpenShardControl(ctx, node)
			if err != nil {
				if connection != nil {
					_ = connection.Close()
				}
				return HealthObservation{}, err
			}
			if connection == nil {
				return HealthObservation{}, ErrControl
			}
			entry.connection = connection
		}
		observation, err := round.client.observeHealthConnection(ctx, node, request, entry.connection)
		if err == nil {
			return observation, nil
		}
		_ = entry.connection.Close()
		entry.connection = nil
		// Older peers close after one response; bounded servers also rotate streams.
		// Replay only a transport closure on an existing stream, never a bad frame,
		// failed authorization, timeout, or canceled request.
		if !reused || attempt != 0 || context.Cause(ctx) != nil || !healthStreamClosed(err) {
			return HealthObservation{}, err
		}
	}
	return HealthObservation{}, ErrControl
}

func healthStreamClosed(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, io.ErrClosedPipe) || errors.Is(err, net.ErrClosed) ||
		errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ECONNRESET)
}
