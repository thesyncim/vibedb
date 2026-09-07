package raftservice_test

import (
	"bytes"
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/orderedkey"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
)

type countedQueryOwner struct {
	*Owner
	state  ServingState
	probes atomic.Uint64
}

func (owner *countedQueryOwner) Probe(
	context.Context,
	raftmember.GroupKey,
) (ServingState, error) {
	owner.probes.Add(1)
	return owner.state, nil
}

type forwardingQueryOwner struct {
	*Owner
	probes atomic.Uint64
}

func (owner *forwardingQueryOwner) Probe(
	ctx context.Context,
	group raftmember.GroupKey,
) (ServingState, error) {
	owner.probes.Add(1)
	return owner.Owner.Probe(ctx, group)
}

func TestRF3SQLReadUsesAcceptedFenceWithoutPostProbe(t *testing.T) {
	cluster := newMultiGroupRF3Cluster(t, 1)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	group := cluster.groups[0].key
	if err := cluster.owners[0].Campaign(ctx, group); err != nil {
		t.Fatal(err)
	}
	leader := waitRF3Leader(t, ctx, cluster.owners[:], nil, group)
	waitRF3Applied(t, ctx, cluster.owners[:], nil, group, 2)
	live, err := cluster.owners[leader].Probe(ctx, group)
	if err != nil || live.Status.Applied < 2 {
		t.Fatalf("live state=%+v err=%v", live, err)
	}

	advertised := live
	advertised.Status.Applied--
	advertised.Status.Commit = advertised.Status.Applied
	if advertised.Status.CheckpointApplied > advertised.Status.Applied {
		advertised.Status.CheckpointApplied = advertised.Status.Applied
	}
	owner := &countedQueryOwner{Owner: cluster.owners[leader], state: advertised}
	server, err := shardservice.NewReplicatedServer(
		owner, shardservice.DefaultReplicatedInFlightFrameBytes, 5*time.Second,
	)
	if err != nil {
		t.Fatal(err)
	}
	client := newMultiGroupRF3RoundTripper(t, cluster)
	client.servers[leader] = server

	authority := serviceauthz.Authority{
		Node: rafttransport.NodeID{99}, Generation: 1,
	}
	inner := shardservice.ShardRequest{
		Authority: authority, SQL: "SELECT COUNT(*) FROM docs",
		Distribution:         distribution.DistributionName(live.Identity.Distribution),
		Shard:                distribution.ShardID(live.Identity.Shard),
		AllocationGeneration: distribution.ShardAllocationGeneration(live.Identity.AllocationGeneration),
		RoutingVersion:       distribution.RoutingVersion(live.Command.RoutingVersion),
		OwnershipEpoch:       distribution.OwnershipEpoch(live.Command.OwnershipEpoch),
		ReadPolicy:           shardservice.ReadStrong,
		ExecutionMode:        shardservice.ExecutionReadOnly,
		MaxRows:              1,
		MaxResultBytes:       4096,
	}
	var query bytes.Buffer
	if err := shardservice.EncodeRequest(&query, &inner); err != nil {
		t.Fatal(err)
	}
	fence := live.Fence()
	request := &shardservice.ReplicatedRequest{
		Operation: shardservice.ReplicatedQueryLeader,
		Authority: authority, Capability: serviceauthz.CapabilityDataRead,
		Fence: shardservice.ReplicatedFence{
			Group: fence.Group, AllocationGeneration: fence.AllocationGeneration,
			Command: fence.Command, MemberID: fence.MemberID, StoreID: fence.StoreID,
			NodeIncarnation: fence.NodeIncarnation, Term: fence.Term,
		},
		Query: query.Bytes(), MaxValueBytes: 4096,
	}
	response, err := client.DoReplicated(ctx, cluster.route(0).Replicas[leader], request)
	if err != nil {
		t.Fatal(err)
	}
	if response.Kind != shardservice.ReplicatedQueryResult || owner.probes.Load() != 1 ||
		response.State.Fence != request.Fence || response.ReadApplied <= advertised.Status.Applied ||
		response.State.Applied != response.ReadApplied ||
		response.State.Commit != response.ReadApplied {
		t.Fatalf("response=%+v probes=%d advertised=%d",
			response, owner.probes.Load(), advertised.Status.Applied)
	}
	if decoded, err := shardservice.DecodeResponse(bytes.NewReader(response.Value)); err != nil ||
		decoded == nil || decoded.Kind != shardservice.ResponseRows {
		t.Fatalf("query result=%+v err=%v", decoded, err)
	}
}

func TestRF3SQLPointReadUsesAcceptedCutWithoutProbe(t *testing.T) {
	cluster := newMultiGroupRF3Cluster(t, 1)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	group := cluster.groups[0].key
	if err := cluster.owners[0].Campaign(ctx, group); err != nil {
		t.Fatal(err)
	}
	leader := waitRF3Leader(t, ctx, cluster.owners[:], nil, group)
	waitRF3Applied(t, ctx, cluster.owners[:], nil, group, 2)
	live, err := cluster.owners[leader].Probe(ctx, group)
	if err != nil || live.Status.Applied < 2 {
		t.Fatalf("live state=%+v err=%v", live, err)
	}

	owner := &countedQueryOwner{Owner: cluster.owners[leader], state: live}
	server, err := shardservice.NewReplicatedServer(
		owner, shardservice.DefaultReplicatedInFlightFrameBytes, 5*time.Second,
	)
	if err != nil {
		t.Fatal(err)
	}
	client := newMultiGroupRF3RoundTripper(t, cluster)
	client.servers[leader] = server
	authority := serviceauthz.Authority{Node: rafttransport.NodeID{99}, Generation: 1}
	endpoint := cluster.route(0).Replicas[leader]
	request := newRF3PointReadRequest(t, group, live, authority, distribution.ShardID(live.Identity.Shard), "missing")
	response, err := client.DoReplicated(ctx, endpoint, request)
	if err != nil || response.Kind != shardservice.ReplicatedQueryResult || owner.probes.Load() != 0 ||
		shardservice.ValidateReplicatedResponse(response) != nil {
		t.Fatalf("point response=%+v err=%v probes=%d", response, err, owner.probes.Load())
	}
	if decoded, err := shardservice.DecodeResponse(bytes.NewReader(response.Value)); err != nil ||
		decoded == nil || decoded.Kind != shardservice.ResponseRows {
		t.Fatalf("point result=%+v err=%v", decoded, err)
	}

	wrong := newRF3PointReadRequest(t, group, live, authority, distribution.ShardID("wrong"), "missing")
	wrongResponse, err := client.DoReplicated(ctx, endpoint, wrong)
	if err != nil || wrongResponse.Kind != shardservice.ReplicatedRefusal ||
		wrongResponse.Refusal != shardservice.ReplicatedRefusalStaleFence || owner.probes.Load() != 0 ||
		shardservice.ValidateReplicatedResponse(wrongResponse) != nil {
		t.Fatalf("wrong identity response=%+v err=%v probes=%d", wrongResponse, err, owner.probes.Load())
	}
}

func newRF3PointReadRequest(
	t testing.TB,
	group raftmember.GroupKey,
	live ServingState,
	authority serviceauthz.Authority,
	shard distribution.ShardID,
	id string,
) *shardservice.ReplicatedRequest {
	t.Helper()
	key, ok := orderedkey.AppendJSONString(nil, []byte(`"`+id+`"`), orderedkey.Ascending)
	if !ok {
		t.Fatal("point key encoding")
	}
	inner := shardservice.ShardRequest{
		Authority: authority, SQL: "SELECT id FROM docs WHERE id = '" + id + "'",
		Distribution:         distribution.DistributionName(live.Identity.Distribution),
		Shard:                shard,
		AllocationGeneration: distribution.ShardAllocationGeneration(live.Identity.AllocationGeneration),
		RoutingVersion:       distribution.RoutingVersion(live.Command.RoutingVersion),
		OwnershipEpoch:       distribution.OwnershipEpoch(live.Command.OwnershipEpoch),
		ReadPolicy:           shardservice.ReadStrong,
		ExecutionMode:        shardservice.ExecutionReadOnly,
		MaxRows:              1, MaxResultBytes: 4096,
		PrimaryKeyRead: shardservice.PrimaryKeyReadRequest{
			Relation: 1, MaxDocumentBytes: 4 << 20,
			PrimaryPath: []byte("/id"), Keys: [][]byte{key},
		},
	}
	var encoded bytes.Buffer
	if err := shardservice.EncodeRequest(&encoded, &inner); err != nil {
		t.Fatal(err)
	}
	fence := live.Fence()
	return &shardservice.ReplicatedRequest{
		Operation: shardservice.ReplicatedQueryLeader, Authority: authority,
		Capability: serviceauthz.CapabilityDataRead,
		Fence: shardservice.ReplicatedFence{
			Group: group, AllocationGeneration: fence.AllocationGeneration,
			Command: fence.Command, MemberID: fence.MemberID,
			StoreID: fence.StoreID, NodeIncarnation: fence.NodeIncarnation,
			Term: fence.Term,
		},
		Query: encoded.Bytes(), MaxValueBytes: 4096,
	}
}

func BenchmarkRF3SQLPointReadForwarded(b *testing.B) {
	cluster := newMultiGroupRF3Cluster(b, 1)
	ctx, cancel := context.WithTimeout(b.Context(), 30*time.Second)
	defer cancel()
	group := cluster.groups[0].key
	if err := cluster.owners[0].Campaign(ctx, group); err != nil {
		b.Fatal(err)
	}
	leader := waitRF3Leader(b, ctx, cluster.owners[:], nil, group)
	waitRF3Applied(b, ctx, cluster.owners[:], nil, group, 2)
	live, err := cluster.owners[leader].Probe(ctx, group)
	if err != nil {
		b.Fatal(err)
	}
	owner := &forwardingQueryOwner{Owner: cluster.owners[leader]}
	server, err := shardservice.NewReplicatedServer(
		owner, shardservice.DefaultReplicatedInFlightFrameBytes, 5*time.Second,
	)
	if err != nil {
		b.Fatal(err)
	}
	client := newMultiGroupRF3RoundTripper(b, cluster)
	client.servers[leader] = server
	authority := serviceauthz.Authority{Node: rafttransport.NodeID{99}, Generation: 1}
	request := newRF3PointReadRequest(b, group, live, authority,
		distribution.ShardID(live.Identity.Shard), "missing")
	endpoint := cluster.route(0).Replicas[leader]
	if response, err := client.DoReplicated(ctx, endpoint, request); err != nil ||
		response == nil || response.Kind != shardservice.ReplicatedQueryResult {
		b.Fatalf("point setup response=%+v err=%v probes=%d", response, err, owner.probes.Load())
	}
	setupProbes := owner.probes.Load()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		response, err := client.DoReplicated(ctx, endpoint, request)
		if err != nil || response == nil || response.Kind != shardservice.ReplicatedQueryResult {
			b.Fatalf("point response=%+v err=%v", response, err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(owner.probes.Load()-setupProbes)/float64(b.N), "probes/op")
}
