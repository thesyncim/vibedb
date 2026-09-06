package query

import (
	"errors"
	"sync/atomic"
)

// ErrCanceled reports that an execution observed its [CancelFlag].
//
// Cancellation is cooperative: an executor reaches the next phase or bounded
// loop checkpoint, parks every worker, balances every reusable pipeline
// channel, and removes every spill file before returning this error. A failed
// execution exposes no partial Result.
var ErrCanceled = errors.New("query: execution canceled")

// A CancelFlag is a reusable, allocation-free cancellation signal for query
// execution. Its zero value is ready to use.
//
// Install a pointer in [ExecOptions.Cancel], call Cancel from any goroutine,
// and wait for the execution to return [ErrCanceled]. Reset prepares the flag
// for another execution and must be called only after every execution using
// the flag has returned. A CancelFlag must not be copied after first use.
//
// CancelFlag is deliberately smaller than a context: execution needs only one
// monotonic bit, not values, deadlines, or a newly allocated cancellation
// tree. A caller can bind an existing context Done channel before execution;
// checkpoints observe it directly without a watcher goroutine or callback.
type CancelFlag struct {
	canceled atomic.Bool
	done     <-chan struct{}
}

// BindDone binds a cancellation channel without starting a watcher. It must
// only be called while no execution or concurrent flag operation is running.
// Reset and Take clear the explicit signal only; a closed bound channel remains
// canceled until BindDone replaces it. BindDone(nil) removes the binding.
func (f *CancelFlag) BindDone(done <-chan struct{}) { f.done = done }

// ObservesDone reports whether f directly observes this non-nil channel.
// Its binding must remain unchanged throughout the caller's execution.
func (f *CancelFlag) ObservesDone(done <-chan struct{}) bool {
	return f != nil && done != nil && f.done == done
}

// Cancel asks every execution using f to stop at its next cooperative
// checkpoint.
// It is safe to call concurrently and is idempotent. Calling it on nil is a
// no-op, which keeps optional owner cleanup simple.
func (f *CancelFlag) Cancel() {
	if f != nil {
		f.canceled.Store(true)
	}
}

// Canceled reports an explicit cancellation or a closed bound channel.
func (f *CancelFlag) Canceled() bool {
	if f == nil {
		return false
	}
	if f.canceled.Load() {
		return true
	}
	return f.doneCanceled()
}

func (f *CancelFlag) doneCanceled() bool {
	if f.done == nil {
		return false
	}
	select {
	case <-f.done:
		return true
	default:
		return false
	}
}

// Take atomically consumes an explicit cancellation and also reports whether
// the bound channel is closed. Channel cancellation cannot be consumed.
//
// If Cancel races with Take, the request is either returned by this call or
// remains armed for the next call; it cannot be lost between a separate load
// and reset. Like Reset, Take must not clear a flag while an execution using it
// is still running. Calling it on nil returns false.
func (f *CancelFlag) Take() bool {
	return f != nil && (f.canceled.Swap(false) || f.doneCanceled())
}

// Reset clears the explicit signal; it does not replace the bound channel.
// It must be called only after every execution using
// f has returned; resetting a flag still in use can let that execution continue.
// Calling it on nil is a no-op.
func (f *CancelFlag) Reset() {
	if f != nil {
		f.canceled.Store(false)
	}
}

// cancellationError is the single nil-cheap probe every execution path uses.
// Keep it small enough to inline: the overwhelmingly common default is one
// pointer comparison and no atomic operation.
func cancellationError(flag *CancelFlag) error {
	if flag.Canceled() {
		return ErrCanceled
	}
	return nil
}

const cancellationCheckMask = 255

// cancellationCheckpoint amortizes an armed atomic load across a bounded
// number of loop iterations. Callers still check once before entering a long
// operation and once after it returns.
func cancellationCheckpoint(flag *CancelFlag, at int) error {
	if flag != nil && at&cancellationCheckMask == 0 && flag.Canceled() {
		return ErrCanceled
	}
	return nil
}
