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
