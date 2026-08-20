package gateway

import (
	"context"
	cryptorand "crypto/rand"
	"errors"
	"sync"
	"time"

	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/shardservice"
)

// snapshotFanout establishes a short-lived vector cut across every routed
// shard before executing the ordinary bounded fan-out. Point reads retain the
// one-round-trip single path; only multi-shard strong reads pay this bridge
// until replicated MVCC timestamps are available.
func (e *Executor) snapshotFanout(ctx context.Context, plan *plan, profile Profile) (*Result, error) {
	id, err := e.acquireReadFences(ctx, plan.calls, profile)
	if err != nil {
		return nil, err
	}
	for i := range plan.calls {
		plan.calls[i].req.ReadFenceID = id
	}
	result, queryErr := e.fanout(ctx, plan, profile)
	releaseErr := e.releaseReadFences(ctx, plan.calls, profile, id)
	return result, errors.Join(queryErr, releaseErr)
}

// acquireReadFences uses an all-or-nothing try/release loop. This avoids a
// distributed lock cycle when a transaction participant and a reader each win
// admission on a different shard. Every retry gets a fresh identity so a late
// release from an uncertain prior attempt cannot tear down the new cut.
func (e *Executor) acquireReadFences(
	ctx context.Context,
	calls []shardCall,
	profile Profile,
) (distributedtxn.ID, error) {
	for attempt := 0; ; attempt++ {
		id, err := newTransactionID(cryptorand.Reader)
		if err != nil {
			return distributedtxn.ID{}, err
		}
		errs := e.readFencePhase(
			ctx, calls, profile, id, shardservice.TransactionAcquireReadFence,
		)
		phaseErr := errors.Join(errs...)
		if phaseErr == nil {
			return id, nil
		}
		acquired := successfulReadFenceCalls(calls, errs)
		releaseErr := e.releaseReadFences(ctx, acquired, profile, id)
		if releaseErr != nil || !onlyReadFenceBusy(errs) {
			return distributedtxn.ID{}, errors.Join(phaseErr, releaseErr)
		}
		base := 100 * time.Microsecond
		base <<= min(attempt, 5)
		if base > 5*time.Millisecond {
			base = 5 * time.Millisecond
		}
		delay := base + time.Duration(id[0])*base/256
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return distributedtxn.ID{}, ctx.Err()
		}
	}
}

func onlyReadFenceBusy(errs []error) bool {
	found := false
	for i := range errs {
		if errs[i] == nil {
			continue
		}
		if !errors.Is(errs[i], ErrReadFenceBusy) {
			return false
		}
		found = true
	}
	return found
}

func successfulReadFenceCalls(calls []shardCall, errs []error) []shardCall {
	count := 0
	for i := range errs {
		if errs[i] == nil {
			count++
		}
	}
	if count == len(calls) {
		return calls
	}
	acquired := make([]shardCall, 0, count)
	for i := range errs {
		if errs[i] == nil {
			acquired = append(acquired, calls[i])
		}
	}
	return acquired
}

func (e *Executor) releaseReadFences(
	ctx context.Context,
	calls []shardCall,
	profile Profile,
	id distributedtxn.ID,
) error {
	return errors.Join(e.readFencePhase(
		ctx, calls, profile, id, shardservice.TransactionReleaseReadFence,
	)...)
}

func (e *Executor) readFencePhase(
	ctx context.Context,
	calls []shardCall,
	profile Profile,
	id distributedtxn.ID,
	operation shardservice.TransactionOperation,
) []error {
	errs := make([]error, len(calls))
	if len(calls) == 0 {
		return errs
	}
	jobs := make(chan int)
	workers := min(max(1, profile.MaxConcurrency), len(calls))
	var wait sync.WaitGroup
	for range workers {
		wait.Go(func() {
			for i := range jobs {
				request := transactionRequest(calls[i].req, profile, operation, id, 1, nil)
				if operation == shardservice.TransactionAcquireReadFence {
					request.BucketBits = calls[i].req.BucketBits
					request.AccessScopes = calls[i].req.AccessScopes
					request.Deadline = profile.GlobalDeadline
				}
				_, errs[i] = e.transactionRoundTrip(ctx, calls[i].address, request, profile)
			}
		})
	}
	for i := range calls {
		select {
		case jobs <- i:
		case <-ctx.Done():
			errs[i] = ctx.Err()
		}
	}
	close(jobs)
	wait.Wait()
	return errs
}
