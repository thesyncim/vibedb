package raftservice_test

import (
	"bytes"
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/distribution"
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
