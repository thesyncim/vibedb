package gateway

import (
	"context"

	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
)

func (client *ReplicatedNodeClient) parallelReplicatedDiscoveryEnabled() bool {
	if client == nil {
		return false
	}
	if client.remote == nil {
		return true
	}
	if remote, ok := client.remote.(interface{ parallelReplicatedDiscoveryEnabled() bool }); ok {
		return remote.parallelReplicatedDiscoveryEnabled()
	}
	_, ok := client.remote.(interface{ parallelReplicatedDiscovery() })
	return ok
}

// ProbeReplicated preserves the authenticated transport's observation path:
// a retained member may advance its incarnation, while every ordinary call
// continues to require the exact observed incarnation.
func (client *ReplicatedNodeClient) ProbeReplicated(ctx context.Context, route ReplicatedRoute,
	endpoint ReplicatedEndpoint, capability serviceauthz.Capability,
) (*shardservice.ReplicatedResponse, error) {
	return client.probeReplicatedBound(ctx, route, endpoint, capability, false)
}

func (client *ReplicatedNodeClient) probeCatalog(ctx context.Context, route ReplicatedRoute,
	endpoint ReplicatedEndpoint,
) (*shardservice.ReplicatedResponse, error) {
	if !catalogBootstrapRoute(route) {
		return nil, ErrReplicatedRoute
	}
	return client.probeReplicatedBound(ctx, route, endpoint, serviceauthz.CapabilityTopology, true)
}

func (client *ReplicatedNodeClient) probeReplicatedBound(ctx context.Context, route ReplicatedRoute,
	endpoint ReplicatedEndpoint, capability serviceauthz.Capability, catalog bool,
) (*shardservice.ReplicatedResponse, error) {
	if ctx == nil {
		return nil, ErrReplicatedUnauthorized
	}
	authority, authorized := serviceauthz.FromContext(ctx)
	if !authorized || !authority.Valid() {
		return nil, ErrReplicatedUnauthorized
	}
	if client == nil || !validReplicatedRoute(route) || !validAuthenticatedEndpoint(endpoint) {
		return nil, ErrReplicatedRoute
	}
	request := shardservice.ReplicatedRequest{Operation: shardservice.ReplicatedProbe,
		Authority: authority, Capability: capability,
		Fence: shardservice.ReplicatedFence{Group: route.Group, AllocationGeneration: route.AllocationGeneration}}
	if err := attachFrontendContinuation(ctx, &request); err != nil {
		return nil, err
	}
	client.legacyCalls.Add(1)
	var response *shardservice.ReplicatedResponse
	var err error
	if endpoint.Node == client.localNode {
		if client.localServer == nil {
			return nil, ErrReplicatedRoute
		}
		client.localCalls.Add(1)
		lease, dispatchErr := client.localServer.DispatchReplicated(ctx, shardservice.ReplicatedCall{Request: request})
		if dispatchErr != nil {
			return nil, dispatchErr
		}
		defer lease.Release()
		reply := lease.Reply()
		if err := shardservice.ValidateReplicatedReply(reply); err != nil {
			return nil, err
		}
		if reply.SQL != nil {
			return nil, ErrReplicatedRoute
		}
		// A valid observation carries no borrowed payload. Copy its fixed
		// response before the admission lease is released.
		copy := reply.Response
		response = &copy
	} else {
		if client.remote == nil {
			return nil, ErrReplicatedDial
		}
		client.remoteCalls.Add(1)
		if observer, ok := client.remote.(interface {
			probeCatalog(context.Context, ReplicatedRoute, ReplicatedEndpoint) (*shardservice.ReplicatedResponse, error)
		}); ok && catalog {
			response, err = observer.probeCatalog(ctx, route, endpoint)
		} else if observer, ok := client.remote.(interface {
			ProbeReplicated(context.Context, ReplicatedRoute, ReplicatedEndpoint, serviceauthz.Capability) (*shardservice.ReplicatedResponse, error)
		}); ok {
			response, err = observer.ProbeReplicated(ctx, route, endpoint, capability)
		} else {
			response, err = client.remote.DoReplicated(ctx, endpoint, &request)
		}
		if err != nil {
			return nil, err
		}
	}
	if !validReplicatedUnauthorizedWithoutState(response) {
		if catalog && response != nil && catalogCommandProgression(route.Command, response.State.Fence.Command) {
			route.Command = response.State.Fence.Command
		}
		if _, err := bindReplicatedObservation(route, endpoint, response); err != nil {
			return nil, err
		}
	}
	return response, nil
}
