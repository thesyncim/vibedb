package gatewayruntime

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
)

// Preserve the authenticated catch-up observation. A second probe after
// success can race the next election and turn proven catch-up into a failure.
func rf3FixtureWaitApplied(ctx context.Context, timeout time.Duration, required uint64,
	probe func() (shardservice.ReplicatedMemberState, error),
) (shardservice.ReplicatedMemberState, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	var state shardservice.ReplicatedMemberState
	var err error
	for ctx.Err() == nil {
		state, err = probe()
		if err == nil && state.Applied >= required && state.LeaderID != 0 {
			return state, nil
		}
		select {
		case <-ctx.Done():
			return state, errors.Join(ctx.Err(), err)
		case <-ticker.C:
		}
	}
	return state, errors.Join(ctx.Err(), err)
}

func TestRF3FixtureCatchUpRetainsSuccessfulObservation(t *testing.T) {
	calls := 0
	state, err := rf3FixtureWaitApplied(t.Context(), time.Second, 81, func() (shardservice.ReplicatedMemberState, error) {
		calls++
		if calls == 1 {
			return shardservice.ReplicatedMemberState{Applied: 81, LeaderID: 2}, nil
		}
		return shardservice.ReplicatedMemberState{}, errors.New("subsequent election")
	})
	if err != nil || state.Applied != 81 || calls != 1 {
		t.Fatalf("lost catch-up proof: state=%+v calls=%d err=%v", state, calls, err)
	}
}

func TestRF3FixtureCatchUpRejectsLagAndLeaderlessState(t *testing.T) {
	calls := 0
	state, err := rf3FixtureWaitApplied(t.Context(), time.Second, 81, func() (shardservice.ReplicatedMemberState, error) {
		calls++
		switch calls {
		case 1:
			return shardservice.ReplicatedMemberState{Applied: 80, LeaderID: 2}, nil
		case 2:
			return shardservice.ReplicatedMemberState{Applied: 81}, nil
		default:
			return shardservice.ReplicatedMemberState{Applied: 81, LeaderID: 2}, nil
		}
	})
	if err != nil || state.Applied != 81 || calls != 3 {
		t.Fatalf("invalid catch-up admission: state=%+v calls=%d err=%v", state, calls, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = rf3FixtureWaitApplied(ctx, time.Second, 81, func() (shardservice.ReplicatedMemberState, error) {
		t.Fatal("probe after cancellation")
		return shardservice.ReplicatedMemberState{}, nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation=%v", err)
	}
}

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
