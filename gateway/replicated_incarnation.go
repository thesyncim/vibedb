package gateway

import (
	"context"

	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
)

// ProbeReplicated observes a process restart on the same authenticated node,
// member and store. Catalog authority is not refreshed here: all group,
// allocation and command fences must still match, and incarnation may only
// advance. Subsequent operations use the exact observed incarnation through
// DoReplicated; they cannot use this observation path to bypass a stale fence.
func (client *AuthenticatedReplicatedClient) ProbeReplicated(
	ctx context.Context, route ReplicatedRoute, endpoint ReplicatedEndpoint,
	capability serviceauthz.Capability,
) (*shardservice.ReplicatedResponse, error) {
	return client.probeReplicatedBound(ctx, route, endpoint, capability, false)
}

func (client *AuthenticatedReplicatedClient) probeCatalog(ctx context.Context, route ReplicatedRoute,
	endpoint ReplicatedEndpoint,
) (*shardservice.ReplicatedResponse, error) {
	if !catalogBootstrapRoute(route) {
		return nil, ErrReplicatedRoute
	}
	return client.probeReplicatedBound(ctx, route, endpoint, serviceauthz.CapabilityTopology, true)
}

func (client *AuthenticatedReplicatedClient) probeReplicatedBound(
	ctx context.Context, route ReplicatedRoute, endpoint ReplicatedEndpoint,
	capability serviceauthz.Capability, catalog bool,
) (*shardservice.ReplicatedResponse, error) {
	if ctx == nil {
		return nil, ErrReplicatedUnauthorized
	}
	authority, authorized := serviceauthz.FromContext(ctx)
	if !authorized || !authority.Valid() {
		return nil, ErrReplicatedUnauthorized
	}
	if !validReplicatedRoute(route) || !validAuthenticatedEndpoint(endpoint) {
		return nil, ErrReplicatedRoute
	}
	connection, err := client.acquireClass(ctx, endpoint, replicatedCapabilityUsesControlReserve(capability))
	if err != nil {
		return nil, err
	}
	response, err := shardservice.RoundTripReplicated(ctx, connection.conn, &shardservice.ReplicatedRequest{
		Operation: shardservice.ReplicatedProbe, Authority: authority, Capability: capability,
		Fence: shardservice.ReplicatedFence{Group: route.Group, AllocationGeneration: route.AllocationGeneration},
	})
	healthy := err == nil && context.Cause(ctx) == nil
	if err == nil && !validReplicatedUnauthorizedWithoutState(response) {
		if catalog && response != nil && catalogCommandProgression(route.Command, response.State.Fence.Command) {
			route.Command = response.State.Fence.Command
		}
		if _, err = bindReplicatedObservation(route, endpoint, response); err != nil {
			healthy = false
		}
	}
	client.release(connection, healthy)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func bindReplicatedObservation(route ReplicatedRoute, endpoint ReplicatedEndpoint,
	response *shardservice.ReplicatedResponse,
) (ReplicatedEndpoint, error) {
	if response == nil || response.Kind != shardservice.ReplicatedHandshake ||
		!validReplicatedResponseState(response) || !validReplicatedNonterminalResponse(response) {
		return ReplicatedEndpoint{}, ErrReplicatedRoute
	}
	fence := response.State.Fence
	if fence.Group != route.Group || fence.AllocationGeneration != route.AllocationGeneration ||
		fence.Command != route.Command || fence.MemberID != endpoint.Member ||
		fence.StoreID != endpoint.StoreID || endpoint.NodeIncarnation == 0 ||
		fence.NodeIncarnation < endpoint.NodeIncarnation {
		return ReplicatedEndpoint{}, ErrReplicatedRoute
	}
	endpoint.NodeIncarnation = fence.NodeIncarnation
	return endpoint, nil
}

func (executor *ReplicatedExecutor) probeReplicated(ctx context.Context, route ReplicatedRoute,
	endpoint ReplicatedEndpoint, capability serviceauthz.Capability,
) (*shardservice.ReplicatedResponse, ReplicatedEndpoint, error) {
	observer, ok := executor.client.(interface {
		ProbeReplicated(context.Context, ReplicatedRoute, ReplicatedEndpoint, serviceauthz.Capability) (*shardservice.ReplicatedResponse, error)
	})
	if !ok {
		response, err := executor.doReplicated(ctx, endpoint, &shardservice.ReplicatedRequest{
			Operation: shardservice.ReplicatedProbe, Capability: capability,
			Fence: shardservice.ReplicatedFence{Group: route.Group, AllocationGeneration: route.AllocationGeneration},
		})
		return response, endpoint, err
	}
	attempt, cancel := context.WithTimeout(ctx, executor.attemptTimeout)
	defer cancel()
	response, err := observer.ProbeReplicated(attempt, route, endpoint, capability)
	if err != nil || validReplicatedUnauthorizedWithoutState(response) {
		return response, endpoint, err
	}
	observed, err := bindReplicatedObservation(route, endpoint, response)
	return response, observed, err
}
