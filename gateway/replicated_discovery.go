package gateway

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
)

// Only round trippers explicitly supporting concurrent, cancelable probes
// opt in. The authenticated pool bounds connections, handshakes and waiters.
func (*AuthenticatedReplicatedClient) parallelReplicatedDiscovery() {}

func (executor *ReplicatedExecutor) parallelDiscovery() bool {
	_, ok := executor.client.(interface{ parallelReplicatedDiscovery() })
	return ok
}

// discoverResponsiveLeader hedges read-only discovery after a short head start
// for the last observed leader. It never submits concurrent proposals. At most
// the certified RF3 plus its enrolled catalog target are probed, once each;
// every losing probe is canceled and joined before this call returns.
func (executor *ReplicatedExecutor) discoverResponsiveLeader(
	ctx context.Context, route ReplicatedRoute, candidates []ReplicatedEndpoint,
	preferred uint64, capability serviceauthz.Capability, catalog bool,
) (ReplicatedEndpoint, shardservice.ReplicatedMemberState, error) {
	if len(candidates) == 0 || len(candidates) > ServingReplicaCount+1 {
		return ReplicatedEndpoint{}, shardservice.ReplicatedMemberState{}, ErrReplicatedRoute
	}
	type observation struct {
		endpoint ReplicatedEndpoint
		response *shardservice.ReplicatedResponse
		err      error
	}
	ctx, cancel := context.WithCancel(ctx)
	var workers sync.WaitGroup
	defer func() { cancel(); workers.Wait() }()
	results := make(chan observation, len(candidates))
	var visited uint8
	pending := 0
	start := func(index int) {
		visited |= 1 << index
		pending++
		endpoint := candidates[index]
		workers.Add(1)
		go func() {
			defer workers.Done()
			attempt, stop := context.WithTimeout(ctx, executor.attemptTimeout)
			defer stop()
			var response *shardservice.ReplicatedResponse
			var err error
			if catalog {
				if client, ok := executor.client.(interface {
					probeCatalog(context.Context, ReplicatedRoute, ReplicatedEndpoint) (*shardservice.ReplicatedResponse, error)
				}); ok {
					response, err = client.probeCatalog(attempt, route, endpoint)
				} else {
					response, err = executor.doReplicated(attempt, endpoint, &shardservice.ReplicatedRequest{
						Operation: shardservice.ReplicatedProbe, Capability: capability,
						Fence: shardservice.ReplicatedFence{Group: route.Group, AllocationGeneration: route.AllocationGeneration},
					})
				}
			} else {
				response, endpoint, err = executor.probeReplicated(attempt, route, endpoint, capability)
			}
			results <- observation{endpoint, response, err}
		}()
	}
	startNext := func(member uint64) {
		index := -1
		for candidate := range candidates {
			if visited&(1<<candidate) == 0 && (index == -1 || candidates[candidate].Member == member) {
				index = candidate
				if candidates[candidate].Member == member {
					break
				}
			}
		}
		if index != -1 {
			start(index)
		}
	}
	startNext(preferred)
	hedge := time.NewTimer(min(25*time.Millisecond, executor.attemptTimeout))
	defer hedge.Stop()
	var joined error
	for pending > 0 {
		select {
		case <-ctx.Done():
			return ReplicatedEndpoint{}, shardservice.ReplicatedMemberState{}, errors.Join(ErrReplicatedLeader, joined, context.Cause(ctx))
		case <-hedge.C:
			for index := range candidates {
				if visited&(1<<index) == 0 {
					start(index)
				}
			}
		case result := <-results:
			pending--
			if result.err != nil {
				joined = errors.Join(joined, result.err)
				startNext(0)
				continue
			}
			if validReplicatedUnauthorizedWithoutState(result.response) {
				return ReplicatedEndpoint{}, shardservice.ReplicatedMemberState{}, &ReplicatedRefusalError{Code: result.response.Refusal}
			}
			observedRoute := route
			if catalog && result.response != nil && catalogCommandProgression(route.Command, result.response.State.Fence.Command) {
				observedRoute.Command = result.response.State.Fence.Command
			}
			endpoint, err := bindReplicatedObservation(observedRoute, result.endpoint, result.response)
			if err != nil {
				joined = errors.Join(joined, err)
				startNext(0)
				continue
			}
			if result.response.State.LeaderID == endpoint.Member {
				return endpoint, result.response.State, nil
			}
			startNext(result.response.State.LeaderID)
		}
	}
	if joined == nil {
		joined = errReplicatedLeaderUnobserved
	}
	return ReplicatedEndpoint{}, shardservice.ReplicatedMemberState{}, errors.Join(ErrReplicatedLeader, joined)
}
