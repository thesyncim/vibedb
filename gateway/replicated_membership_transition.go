package gateway

import (
	"context"
	"errors"
	"fmt"

	"github.com/thesyncim/vibedb/internal/raftservice"
)

// ErrReplicatedMembershipTransition identifies an authenticated serving
// observation whose membership version has advanced while the catalog command
// is still the otherwise exact predecessor. It is a planning hint only: the
// caller must refresh catalog authority before attempting any data operation.
var ErrReplicatedMembershipTransition = errors.New(
	"gateway: replicated shard membership transition is in progress",
)

// ReplicatedMembershipTransitionError retains only fixed-width fence evidence
// for a pre-admission catalog refresh. It deliberately carries no SQL, keys,
// documents, or endpoint payloads.
type ReplicatedMembershipTransitionError struct {
	CatalogCommand  raftservice.CommandFence
	ObservedCommand raftservice.CommandFence
}

func (e *ReplicatedMembershipTransitionError) Error() string {
	if e == nil {
		return ErrReplicatedMembershipTransition.Error()
	}
	return "gateway: replicated membership transition observed: " +
		fmt.Sprintf("catalog replica-set=%d observed replica-set=%d",
			e.CatalogCommand.ReplicaSetVersion, e.ObservedCommand.ReplicaSetVersion)
}

// Is exposes a small stable sentinel to callers that must stop an outer
// preparation retry after this inner bounded refresh policy has run.
func (e *ReplicatedMembershipTransitionError) Is(target error) bool {
	return target == ErrReplicatedMembershipTransition
}

// Unwrap preserves the historical route error classification. Internal
// transition classification must recognize this concrete type before walking
// the compatibility unwrap, otherwise the route leaf would disqualify it.
func (e *ReplicatedMembershipTransitionError) Unwrap() error {
	return ErrReplicatedRoute
}

// isReplicatedMembershipTransitionPlanningError accepts only a complete error
// tree made from the authenticated transition hint and the known leader
// aggregation sentinel. A generic route, refusal, transport, context, or
// terminal sibling makes the whole tree ineligible for catalog refresh.
func isReplicatedMembershipTransitionPlanningError(err error) bool {
	if err == nil {
		return false
	}
	// One read may aggregate up to the supported sixteen discovery attempts.
	// Each attempt has at most R probe leaves, one join per probe, one
	// discovery join, one read-attempt join, and one leader sentinel; the final
	// read aggregation contributes one join and one leader sentinel. Use the
	// protocol's maximum route-member bound (not today's usual RF3 width) so
	// every supported nested aggregation remains covered.
	const maximum = AbsoluteMaxReplicatedAttempts*(2*AbsoluteMaxReplicatedRouteMembers+3) + 2
	var pending [maximum]error
	pending[0] = err
	count, examined := 1, 0
	found := false
	for count != 0 {
		count--
		current := pending[count]
		pending[count] = nil
		examined++
		if examined > maximum || current == nil {
			return false
		}
		switch transition := current.(type) {
		case *ReplicatedMembershipTransitionError:
			if transition == nil || !transition.valid() {
				return false
			}
			found = true
			// Do not follow Unwrap: ErrReplicatedRoute is compatibility
			// metadata, not an independent cause for this predicate.
			continue
		}
		if current == ErrReplicatedLeader {
			continue
		}
		// Refusal errors unwrap some availability sentinels (including
		// ErrReplicatedLeader), but the refusal itself is terminal evidence for
		// this classifier and must not be flattened into a leader leaf.
		if _, ok := current.(*ReplicatedRefusalError); ok {
			return false
		}
		if joined, ok := current.(interface{ Unwrap() []error }); ok {
			children := joined.Unwrap()
			if len(children) == 0 || len(children) > maximum-examined-count {
				return false
			}
			for _, child := range children {
				if child == nil {
					return false
				}
				pending[count] = child
				count++
			}
			continue
		}
		if wrapped, ok := current.(interface{ Unwrap() error }); ok {
			child := wrapped.Unwrap()
			if child == nil {
				return false
			}
			if count >= maximum {
				return false
			}
			pending[count] = child
			count++
			continue
		}
		return false
	}
	return found
}

// durableSQLCatalogRefreshState bounds all catalog refresh/wait/replan cycles
// made by one pre-admission invocation. The limit is inherited from the
// ReplicatedExecutor's existing bounded attempt policy; no new public tuning
// option is introduced.
type durableSQLCatalogRefreshState struct {
	cycles      int
	missRetried bool
	cycleLimit  int
}

func newDurableSQLCatalogRefreshState(data *ReplicatedExecutor) durableSQLCatalogRefreshState {
	limit := 1
	if data != nil && data.maxAttempts > 0 && data.maxAttempts <= AbsoluteMaxReplicatedAttempts {
		limit = data.maxAttempts
	}
	return durableSQLCatalogRefreshState{cycleLimit: limit}
}

func (state *durableSQLCatalogRefreshState) refreshMissing(
	ctx context.Context, planner *Executor, staleGeneration uint64,
) error {
	if state == nil || state.missRetried {
		return ErrStaleGeneration
	}
	state.missRetried = true
	return state.refreshOnce(ctx, planner, staleGeneration)
}

// refreshMembership waits through only the existing bounded failover policy.
// CatalogHolder.refreshAfter performs checked publication and returns
// ErrStaleGeneration when the authority has not advanced; only that exact
// unchanged-generation result may consume another bounded cycle.
func (state *durableSQLCatalogRefreshState) refreshMembership(
	ctx context.Context, planner *Executor, staleGeneration uint64,
) error {
	if state == nil {
		return ErrStaleGeneration
	}
	for {
		if ctx == nil {
			return ErrStaleGeneration
		}
		if err := ctx.Err(); err != nil {
			return context.Cause(ctx)
		}
		if refreshErr := state.refreshOnce(ctx, planner, staleGeneration); refreshErr == nil {
			return nil
		} else if refreshErr != ErrStaleGeneration {
			return refreshErr
		} else if state.cycles >= state.cycleLimit {
			return refreshErr
		} else if waitErr := waitReplicatedFailoverRetry(ctx, state.cycles-1); waitErr != nil {
			return errors.Join(refreshErr, waitErr)
		}
	}
}

func (e *ReplicatedMembershipTransitionError) valid() bool {
	return e != nil && e.CatalogCommand.Valid() && e.ObservedCommand.Valid() &&
		e.ObservedCommand.ReplicaSetVersion > e.CatalogCommand.ReplicaSetVersion &&
		replicatedMembershipTransitionCommandsMatch(e.CatalogCommand, e.ObservedCommand)
}

func (state *durableSQLCatalogRefreshState) refreshOnce(
	ctx context.Context, planner *Executor, staleGeneration uint64,
) error {
	if state == nil || state.cycles >= state.cycleLimit || planner == nil ||
		planner.catalog == nil {
		return ErrStaleGeneration
	}
	if ctx == nil {
		return ErrStaleGeneration
	}
	if err := ctx.Err(); err != nil {
		return context.Cause(ctx)
	}
	state.cycles++
	err := planner.refreshAfterCatalogMiss(ctx, staleGeneration)
	if err == nil {
		if contextErr := context.Cause(ctx); contextErr != nil {
			return contextErr
		}
	}
	return err
}
