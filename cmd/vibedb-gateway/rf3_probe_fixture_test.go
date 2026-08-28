package main

import (
	"bytes"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
)

// Raw round trippers do not inject executor authority. Process probes must
// name the actual authenticated observer and its configured policy generation.
func rf3FixtureProbeRequest(route gateway.ReplicatedRoute, authority serviceauthz.Authority,
	capability serviceauthz.Capability) *shardservice.ReplicatedRequest {
	return &shardservice.ReplicatedRequest{
		Operation: shardservice.ReplicatedProbe, Authority: authority, Capability: capability,
		Fence: shardservice.ReplicatedFence{Group: route.Group, AllocationGeneration: route.AllocationGeneration},
	}
}

func rf3FixtureProbeState(response *shardservice.ReplicatedResponse, group raftmember.GroupKey,
	requireLeader bool) (shardservice.ReplicatedMemberState, error) {
	if response == nil || response.Kind != shardservice.ReplicatedHandshake || !response.HasState ||
		response.State.Fence.Group != group || requireLeader && response.State.LeaderID == 0 {
		return shardservice.ReplicatedMemberState{}, errors.New("external RF3: incomplete probe")
	}
	return response.State, nil
}

func TestRF3FixtureIsolatedMemberProbeDoesNotRequireLeader(t *testing.T) {
	group := raftmember.GroupKey{GroupID: [16]byte{1}}
	response := &shardservice.ReplicatedResponse{Kind: shardservice.ReplicatedHandshake,
		HasState: true, State: shardservice.ReplicatedMemberState{Fence: shardservice.ReplicatedFence{Group: group}}}
	if _, err := rf3FixtureProbeState(response, group, false); err != nil {
		t.Fatalf("isolated follower's valid leaderless state: %v", err)
	}
	if _, err := rf3FixtureProbeState(response, group, true); err == nil {
		t.Fatal("leaderless member counted toward quorum leader readiness")
	}
	response.State.LeaderID = 2
	if _, err := rf3FixtureProbeState(response, group, true); err != nil {
		t.Fatal(err)
	}
	response.HasState = false
	if _, err := rf3FixtureProbeState(response, group, false); err == nil {
		t.Fatal("missing state accepted")
	}
	response.HasState = true
	if _, err := rf3FixtureProbeState(response, raftmember.GroupKey{}, false); err == nil {
		t.Fatal("foreign group accepted")
	}
}

func TestRF3FixtureProbeCarriesExactObserverAuthority(t *testing.T) {
	route := gateway.ReplicatedRoute{
		Group: raftmember.GroupKey{ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2},
			TopologyRecoveryEpoch: 3, ShardIncarnation: [16]byte{4}, GroupID: [16]byte{5}},
		AllocationGeneration: 6,
	}
	authority := serviceauthz.Authority{Node: rafttransport.NodeID{7}, Generation: 5}
	for _, capability := range []serviceauthz.Capability{serviceauthz.CapabilityTopology,
		serviceauthz.CapabilityRequestLedger, serviceauthz.CapabilityDataWrite} {
		request := rf3FixtureProbeRequest(route, authority, capability)
		var wire bytes.Buffer
		if err := shardservice.EncodeReplicatedRequestBorrowed(&wire, request); err != nil {
			t.Fatal(err)
		}
		decoded, err := shardservice.DecodeReplicatedRequest(&wire)
		if err != nil || decoded.Operation != shardservice.ReplicatedProbe ||
			decoded.Authority != authority || decoded.Capability != capability ||
			decoded.Fence.Group != route.Group || decoded.Fence.AllocationGeneration != route.AllocationGeneration {
			t.Fatalf("probe lost exact authority/route: %v", err)
		}
		for _, invalid := range []serviceauthz.Authority{{}, {Node: authority.Node}, {Generation: authority.Generation}} {
			wire.Reset()
			request.Authority = invalid
			if err := shardservice.EncodeReplicatedRequestBorrowed(&wire, request); !errors.Is(err, shardservice.ErrReplicatedWire) || wire.Len() != 0 {
				t.Fatalf("missing or partial observer authority was encoded: bytes=%d err=%v", wire.Len(), err)
			}
		}
	}
}
