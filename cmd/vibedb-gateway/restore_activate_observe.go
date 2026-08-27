package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
)

// A restore plan seals initial incarnations, while every actual process restart
// durably increments them. Observe each exact certificate-bound target before
// constructing strict pooled routes; only incarnation may advance.
func refreshGatewayRestoreCatalogRoute(ctx context.Context, profile *rafttransport.PeerTLS,
	operator serviceauthz.Authority, route gateway.ReplicatedRoute, attempts int, timeout time.Duration,
) (gateway.ReplicatedRoute, error) {
	if ctx == nil || profile == nil || !operator.Valid() || attempts <= 0 || timeout <= 0 {
		return gateway.ReplicatedRoute{}, gateway.ErrRestoreActivation
	}
	route.Replicas = append([]gateway.ReplicatedEndpoint(nil), route.Replicas...)
	for index, endpoint := range route.Replicas {
		var observed error
		for attempt := 0; attempt < attempts; attempt++ {
			current, err := probeGatewayRestoreCatalogReplica(ctx, profile, operator, route, endpoint, timeout)
			if err == nil {
				route.Replicas[index] = current
				observed = nil
				break
			}
			observed = err
			if cause := context.Cause(ctx); cause != nil {
				return gateway.ReplicatedRoute{}, cause
			}
		}
		if observed != nil {
			return gateway.ReplicatedRoute{}, fmt.Errorf("restore catalog replica %d observation: %w", index, observed)
		}
	}
	return route, nil
}

func probeGatewayRestoreCatalogReplica(parent context.Context, profile *rafttransport.PeerTLS,
	operator serviceauthz.Authority, route gateway.ReplicatedRoute, endpoint gateway.ReplicatedEndpoint,
	timeout time.Duration,
) (gateway.ReplicatedEndpoint, error) {
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	raw, err := dialGatewayRestore(ctx, endpoint.Address)
	if err != nil {
		return gateway.ReplicatedEndpoint{}, err
	}
	deadline := func() time.Time { return time.Now().Add(timeout) }
	connection, err := profile.Client(ctx, raw, endpoint.Node, rafttransport.TrafficShardNative, deadline)
	if err != nil {
		return gateway.ReplicatedEndpoint{}, errors.Join(err, raw.Close())
	}
	defer connection.Close()
	response, err := shardservice.RoundTripReplicated(ctx, connection, &shardservice.ReplicatedRequest{
		Operation: shardservice.ReplicatedProbe, Authority: operator, Capability: serviceauthz.CapabilityTopology,
		Fence: shardservice.ReplicatedFence{Group: route.Group, AllocationGeneration: route.AllocationGeneration},
	})
	if err != nil || response == nil {
		return gateway.ReplicatedEndpoint{}, fmt.Errorf("restore catalog probe transport: %w", errors.Join(gateway.ErrRestoreActivation, err))
	}
	if response.Kind != shardservice.ReplicatedHandshake || !response.HasState {
		return gateway.ReplicatedEndpoint{}, fmt.Errorf("restore catalog probe kind=%d refusal=%d state=%t: %w", response.Kind, response.Refusal, response.HasState, gateway.ErrRestoreActivation)
	}
	return bindGatewayRestoreCatalogObservation(route, endpoint, response.State)
}

func bindGatewayRestoreCatalogObservation(route gateway.ReplicatedRoute,
	endpoint gateway.ReplicatedEndpoint, state shardservice.ReplicatedMemberState,
) (gateway.ReplicatedEndpoint, error) {
	fence := state.Fence
	var mismatch string
	switch {
	case fence.Group != route.Group:
		mismatch = "group"
	case fence.AllocationGeneration != route.AllocationGeneration:
		mismatch = "allocation generation"
	case fence.MemberID != endpoint.Member:
		mismatch = "member"
	case fence.StoreID != endpoint.StoreID:
		mismatch = "store"
	case fence.NodeIncarnation < endpoint.NodeIncarnation || endpoint.NodeIncarnation == 0:
		mismatch = "node incarnation"
	case fence.Term == 0:
		mismatch = "zero term"
	case fence.Command != route.Command:
		mismatch = "command fence"
	}
	if mismatch != "" {
		return gateway.ReplicatedEndpoint{}, fmt.Errorf("restore catalog observation %s mismatch: %w", mismatch, gateway.ErrRestoreActivation)
	}
	endpoint.NodeIncarnation = fence.NodeIncarnation
	return endpoint, nil
}
