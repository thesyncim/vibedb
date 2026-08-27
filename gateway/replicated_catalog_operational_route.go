package gateway

import (
	"context"
	"errors"

	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
)

func catalogBootstrapRoute(route ReplicatedRoute) bool {
	return validReplicatedRoute(route) && route.Distribution == ReplicatedCatalogDistribution &&
		route.Shard == ReplicatedCatalogShard
}

// The catalog RF3 is the authority for its own placement. Its bootstrap
// coordinates cannot also require a catalog publication between each adjacent
// membership step: that would prevent the controller from journaling the step.
// Only placement coordinates may advance. Policy, protection, schema, full
// group/allocation, and authenticated physical member identities remain exact.
func catalogCommandProgression(before, after raftservice.CommandFence) bool {
	return before.Valid() && after.Valid() && before.ActivePolicyGeneration == after.ActivePolicyGeneration &&
		before.ProtectionEpoch == after.ProtectionEpoch && before.SchemaGeneration == after.SchemaGeneration &&
		before.RelationManifestDigest == after.RelationManifestDigest &&
		after.ReplicaSetVersion >= before.ReplicaSetVersion && after.OwnershipEpoch >= before.OwnershipEpoch &&
		after.RoutingVersion >= before.RoutingVersion && after.RouteGeneration >= before.RouteGeneration
}

func (session *NativeSession) catalogOperationalRoute(ctx context.Context) (ReplicatedRoute, error) {
	snapshot := session.catalogBootstrap
	if session.catalogHolder != nil && session.catalogHolder.Current() != nil {
		snapshot = session.catalogHolder.Current()
	}
	return session.executor.catalogOperationalRoute(ctx, session.route, snapshot)
}

func (executor *ReplicatedExecutor) catalogOperationalRoute(ctx context.Context, bootstrap ReplicatedRoute,
	snapshot *Snapshot,
) (ReplicatedRoute, error) {
	if executor == nil || ctx == nil || !catalogBootstrapRoute(bootstrap) {
		return ReplicatedRoute{}, ErrReplicatedCatalog
	}
	for attempt := 0; ; attempt++ {
		route, err := executor.catalogOperationalRouteOnce(ctx, bootstrap, snapshot)
		if err == nil || !errors.Is(err, errReplicatedLeaderUnobserved) || attempt+1 >= executor.maxAttempts {
			return route, err
		}
		if waitErr := waitReplicatedFailoverRetry(ctx, attempt); waitErr != nil {
			return ReplicatedRoute{}, errors.Join(err, waitErr)
		}
	}
}

func (executor *ReplicatedExecutor) catalogOperationalRouteOnce(ctx context.Context, bootstrap ReplicatedRoute,
	snapshot *Snapshot,
) (ReplicatedRoute, error) {
	var candidates [ServingReplicaCount + 1]ReplicatedEndpoint
	count := copy(candidates[:], bootstrap.Replicas)
	if snapshot != nil {
		var replicas [ServingReplicaCount]ReplicatedEndpoint
		membership, ok := snapshot.ResolveReplicatedMembershipRoute(bootstrap.Distribution, bootstrap.Shard, replicas[:0])
		if ok && (membership.Serving.Group != bootstrap.Group ||
			membership.Serving.AllocationGeneration != bootstrap.AllocationGeneration) {
			return ReplicatedRoute{}, ErrReplicatedCatalogConflict
		}
		if ok {
			count = copy(candidates[:], membership.Serving.Replicas)
		}
		if ok && membership.HasEnrolledTarget && !replicatedRouteContainsMember(membership.Serving, membership.EnrolledTarget.Member) {
			candidates[count] = membership.EnrolledTarget
			count++
		}
	}
	if executor.parallelDiscovery() {
		preferred := bootstrap.Replicas[0].Member
		if last, _, ok := executor.leaderHints.lookup(bootstrap); ok {
			preferred = last.Member
		}
		endpoint, state, err := executor.discoverResponsiveLeader(ctx, bootstrap, candidates[:count], preferred, serviceauthz.CapabilityTopology, true)
		if err != nil {
			return ReplicatedRoute{}, err
		}
		route := bootstrap
		route.Command = state.Fence.Command
		route.Replicas = append([]ReplicatedEndpoint(nil), candidates[:ServingReplicaCount]...)
		index := 0 // The fourth, non-serving target displaces one bootstrap voter.
		for ordinal := range route.Replicas {
			if route.Replicas[ordinal].Member == endpoint.Member {
				index = ordinal
				break
			}
		}
		route.Replicas[index] = endpoint
		executor.leaderHints.publish(route, endpoint, state)
		return route, nil
	}
	var joined error
	order := [ServingReplicaCount + 1]int{0, 1, 2, 3}
	if last, _, ok := executor.leaderHints.lookup(bootstrap); ok {
		for index := 0; index < count; index++ {
			if sameReplicatedEndpoint(candidates[index], last) {
				order[0], order[index] = order[index], order[0]
				break
			}
		}
	}
	// A cached observation orders candidates only. Every catalog operation
	// still freshly authenticates the current leader and its placement fence.
	for probe := 0; probe < count; probe++ {
		index := order[probe]
		endpoint := candidates[index]
		var response *shardservice.ReplicatedResponse
		var err error
		if observer, ok := executor.client.(interface {
			probeCatalog(context.Context, ReplicatedRoute, ReplicatedEndpoint) (*shardservice.ReplicatedResponse, error)
		}); ok {
			attempt, cancel := context.WithTimeout(ctx, executor.attemptTimeout)
			response, err = observer.probeCatalog(attempt, bootstrap, endpoint)
			cancel()
		} else {
			response, err = executor.doReplicated(ctx, endpoint, &shardservice.ReplicatedRequest{
				Operation: shardservice.ReplicatedProbe, Capability: serviceauthz.CapabilityTopology,
				Fence: shardservice.ReplicatedFence{Group: bootstrap.Group, AllocationGeneration: bootstrap.AllocationGeneration},
			})
		}
		if err != nil {
			joined = errors.Join(joined, err)
			continue
		}
		if validReplicatedUnauthorizedWithoutState(response) {
			return ReplicatedRoute{}, &ReplicatedRefusalError{Code: response.Refusal}
		}
		if response == nil || !catalogCommandProgression(bootstrap.Command, response.State.Fence.Command) {
			joined = errors.Join(joined, ErrReplicatedRoute)
			continue
		}
		route := bootstrap
		route.Command = response.State.Fence.Command
		observed, bindErr := bindReplicatedObservation(route, endpoint, response)
		if bindErr != nil || response.State.LeaderID != endpoint.Member {
			joined = errors.Join(joined, bindErr)
			if bindErr == nil {
				joined = errors.Join(joined, errReplicatedLeaderUnobserved)
			}
			continue
		}
		// This is an ephemeral control reachability set, never a published
		// data roster. ReadIndex and proposal admission use the exact leader's
		// current fence; another membership change requires fresh discovery.
		route.Replicas = append([]ReplicatedEndpoint(nil), candidates[:ServingReplicaCount]...)
		if index >= ServingReplicaCount {
			route.Replicas[0] = observed
		} else {
			route.Replicas[index] = observed
		}
		executor.leaderHints.publish(route, observed, response.State)
		return route, nil
	}
	if joined == nil {
		joined = errReplicatedLeaderUnobserved
	}
	return ReplicatedRoute{}, errors.Join(ErrReplicatedLeader, joined)
}
