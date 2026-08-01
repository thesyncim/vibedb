package driver

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/thesyncim/vibedb/query"
)

// contextLockPoll bounds cancellation latency when a catalog lock is
// contended. The uncontended and non-cancellable paths never create a timer.
const contextLockPoll = 100 * time.Microsecond

// backgroundContext keeps the non-cancellable context interface boxed once.
// Passing context.Background() from a database/sql fallback method otherwise
// makes the interface box escape on every call, even when the branch using it
// (for example JOIN materialization) is not taken.
var backgroundContext = context.Background()

// contextCancelScope bridges one request context into the query executor's
// cooperative cancellation flag. It reuses a flag installed by the typed
// runtime, or owns a local flag when the connection has none. It exists only
// when ctx has a Done channel: context.Background and every legacy Stmt method
// retain the allocation-free nil-flag path.
//
// The watcher is joined before finish returns. That makes restoring the
// connection's prior flag race-free and prevents a canceled request from
// poisoning the next statement on the pooled connection.
type contextCancelScope struct {
	conn     *conn
	ctx      context.Context
	previous *query.CancelFlag
	local    query.CancelFlag
	flag     *query.CancelFlag
	stop     chan struct{}
	stopped  chan struct{}
}

func (c *conn) beginContextCancellation(
	ctx context.Context,
) (*contextCancelScope, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if ctx.Done() == nil {
		return nil, nil
	}
	scope := &contextCancelScope{
		conn: c, ctx: ctx, previous: c.exec.Options.Cancel,
		stop: make(chan struct{}), stopped: make(chan struct{}),
	}
	scope.flag = scope.previous
	if scope.flag == nil {
		scope.flag = &scope.local
	}
	c.exec.Options.Cancel = scope.flag
	go func() {
		defer close(scope.stopped)
		select {
		case <-ctx.Done():
			scope.flag.Cancel()
		case <-scope.stop:
		}
	}()
	return scope, nil
}

func (s *contextCancelScope) finish(err error) error {
	if s == nil {
		return err
	}
	close(s.stop)
	<-s.stopped
	s.conn.exec.Options.Cancel = s.previous
	contextErr := s.ctx.Err()
	if contextErr != nil && s.previous != nil {
		// The installed flag belongs to the typed caller, but this operation's
		// context armed it. Execution has joined every worker, so it is now safe
		// to clear only that operation-local cancellation before reuse.
		s.previous.Reset()
	}
	if errors.Is(err, query.ErrCanceled) && contextErr != nil {
		return contextErr
	}
	return err
}

// mutexLockContext is the cancellable acquisition used by the connector
// ownership lock. Connector.Close can hold that lock while it performs the
// database's terminal durability fences, so Connect must not turn a canceled
// context into an unbounded wait behind that work.
//
// As with the catalog RWMutex helpers below, a context without a Done channel
// takes the ordinary Lock path. That keeps the database/sql background hot path
// allocation-free.
func mutexLockContext(ctx context.Context, mu *sync.Mutex) error {
	if ctx.Done() == nil {
		mu.Lock()
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if mu.TryLock() {
		if err := ctx.Err(); err != nil {
			mu.Unlock()
			return err
		}
		return nil
	}

	timer := time.NewTimer(contextLockPoll)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			if mu.TryLock() {
				if err := ctx.Err(); err != nil {
					mu.Unlock()
					return err
				}
				return nil
			}
			timer.Reset(contextLockPoll)
		}
	}
}

// lockContext acquires mu for writing while the acquisition remains
// cancellable. A context without a Done channel takes the ordinary Lock path,
// which is the zero-allocation database/sql hot path.
func lockContext(ctx context.Context, mu *sync.RWMutex) error {
	if ctx.Done() == nil {
		mu.Lock()
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if mu.TryLock() {
		if err := ctx.Err(); err != nil {
			mu.Unlock()
			return err
		}
		return nil
	}

	timer := time.NewTimer(contextLockPoll)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			if mu.TryLock() {
				if err := ctx.Err(); err != nil {
					mu.Unlock()
					return err
				}
				return nil
			}
			timer.Reset(contextLockPoll)
		}
	}
}

// rlockContext is the read-lock counterpart to lockContext.
func rlockContext(ctx context.Context, mu *sync.RWMutex) error {
	if ctx.Done() == nil {
		mu.RLock()
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if mu.TryRLock() {
		if err := ctx.Err(); err != nil {
			mu.RUnlock()
			return err
		}
		return nil
	}

	timer := time.NewTimer(contextLockPoll)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			if mu.TryRLock() {
				if err := ctx.Err(); err != nil {
					mu.RUnlock()
					return err
				}
				return nil
			}
			timer.Reset(contextLockPoll)
		}
	}
}

// contextCheckpoint is placed immediately before a durable publication.
//
// Once a publication has begun it must run to its storage outcome; returning
// early would let the caller observe context cancellation while the write
// continued in the background. That outcome may explicitly be
// durable.ErrCommitOutcomeUnknown after a namespace fence failure.
// Cancellation therefore controls all work up to this point of no return.
func contextCheckpoint(ctx context.Context) error {
	if ctx.Done() == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}
