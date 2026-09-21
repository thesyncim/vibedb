package gateway

import (
	"context"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
)

// Model the authenticated probe and exact current ReadIndex admission used
// by the wire service, including a cached leader fence becoming stale.
type membershipReadClient struct{ *scatterReadClient }

func (client *membershipReadClient) ProbeReplicated(ctx context.Context, route ReplicatedRoute, endpoint ReplicatedEndpoint,
	capability serviceauthz.Capability,
) (*shardservice.ReplicatedResponse, error) {
	response, err := client.scatterReadClient.DoReplicated(ctx, endpoint, &shardservice.ReplicatedRequest{
		Operation: shardservice.ReplicatedProbe, Capability: capability,
		Fence: shardservice.ReplicatedFence{Group: route.Group},
	})
	if err == nil {
		_, err = bindReplicatedObservation(route, endpoint, response)
	}
	return response, err
}

func (client *membershipReadClient) DoReplicated(ctx context.Context, endpoint ReplicatedEndpoint, request *shardservice.ReplicatedRequest) (*shardservice.ReplicatedResponse, error) {
	client.mu.Lock()
	state := client.states[request.Fence.Group][endpoint.Member]
	client.mu.Unlock()
	if request.Operation != shardservice.ReplicatedProbe && request.Fence != state.Fence {
		return &shardservice.ReplicatedResponse{Kind: shardservice.ReplicatedRefusal,
			Refusal: shardservice.ReplicatedRefusalStaleFence, HasState: true, State: state}, nil
	}
	if request.Operation == shardservice.ReplicatedReadLeader || request.Operation == shardservice.ReplicatedReadFollower {
		return &shardservice.ReplicatedResponse{Kind: shardservice.ReplicatedReadFound, HasState: true,
			State: state, ReadApplied: state.Applied, Value: []byte("value-000")}, nil
	}
	return client.scatterReadClient.DoReplicated(ctx, endpoint, request)
}

func TestOrdinaryReadsRetainLogicalRouteAcrossMembershipStages(t *testing.T) {
	fixture, request := sameGroupSQLReadFixture(t)
	client := &scatterReadClient{}
	reader := newScatterReader(t, fixture, client, nil, 2)
	reader.executor.client = &membershipReadClient{client}
	route := fixture.routes[0].Route
	for _, version := range []uint64{route.Command.ReplicaSetVersion, route.Command.ReplicaSetVersion + 3,
		route.Command.ReplicaSetVersion + 6, route.Command.ReplicaSetVersion + 8} {
		for member, state := range client.states[route.Group] {
			state.Fence.Command.ReplicaSetVersion = version
			client.states[route.Group][member] = state
		}
		result, err := reader.ReadSQLBatch(t.Context(), request)
		if err != nil || result.Count() != 2 || len(result.Observations) != 1 {
			result.Release()
			t.Fatalf("membership %d interrupted held SQL/native reader: %v", version, err)
		}
		result.Release()
		for _, linearizable := range []bool{true, false} {
			point, err := reader.executor.ReadPoint(t.Context(), route, ReplicatedPointRead{
				Relation: 1, Key: []byte("key"), MinimumApplied: 1, MaxValueBytes: 1024, Linearizable: linearizable,
			})
			if err != nil || !point.Found || string(point.Value) != "value-000" || point.State.Fence.Command.ReplicaSetVersion != version {
				t.Fatalf("membership %d point linearizable=%t: %v", version, linearizable, err)
			}
		}
	}
}

func TestOrdinaryReadColdDiscoveryRefreshesLogicalOwnership(t *testing.T) {
	oldFixture := newScatterCatalogFixture(t, 1, 5)
	freshFixture := newScatterCatalogFixture(t, 1, 6)
	freshFixture.descriptors[0].Command.OwnershipEpoch++
	freshFixture.descriptors[0].Command.RoutingVersion++
	freshFixture.descriptors[0].Command.RouteGeneration++
	manifest, err := distribution.NewManifest("scatter-000", 4, []distribution.Shard{{ID: "all", AllocationGeneration: 1,
		Range:   distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}},
		Leaders: []distribution.EndpointID{"peer-a", "peer-b", "peer-c"}, Epoch: 8}})
	if err != nil {
		t.Fatal(err)
	}
	freshFixture.config.Manifests[0] = manifest
	fresh, err := NewSnapshotWithReplicatedTableMetadata(freshFixture.config, freshFixture.endpoints, 6, nil, nil,
		freshFixture.descriptors, freshFixture.profiles)
	if err != nil {
		t.Fatal(err)
	}
	client := &scatterReadClient{}
	refreshes := 0
	reader := newScatterReader(t, oldFixture, client, func(context.Context, uint64) (*Snapshot, error) {
		refreshes++
		return fresh, nil
	}, 1)
	reader.executor.client = &membershipReadClient{client}
	for group, members := range client.states {
		for member, state := range members {
			state.Fence.Command = freshFixture.descriptors[0].Command
			client.states[group][member] = state
		}
	}
	result, err := reader.ReadSQLBatch(t.Context(), scatterSQLReadRequest(oldFixture))
	defer result.Release()
	if err != nil || refreshes != 1 || result.Count() != 1 {
		t.Fatalf("cold ownership refresh: count=%d refreshes=%d err=%v", result.Count(), refreshes, err)
	}
}
