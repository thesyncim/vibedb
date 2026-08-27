//go:build darwin || linux

package main

import (
	"bytes"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/shardservice"
)

// Run the actual external-harness request builder through the shipped wire
// grammar on every host, before Linux process/fault qualification is needed.
func TestRF3FaultReadRequestCanonicalPreflight(t *testing.T) {
	store := rf3CommandStoreIdentity(1)
	identity := raftmember.RuntimeIdentity{Group: rf3CommandGroup(), AllocationGeneration: store.AllocationGeneration,
		MemberID: store.MemberID, StoreID: store.StoreID, NodeIncarnation: 1, RelationManifestDigest: [32]byte{1}}
	fixture := &rf3FaultFixture{nodes: [rf3CommandMembers]rafttransport.NodeID{{1}, {2}, {3}}, authority: rf3CommandAuthority(), maxReadValueBytes: replication.MaxMutationValueBytes}
	state := shardservice.ReplicatedMemberState{Fence: shardservice.ReplicatedFence{Group: identity.Group,
		AllocationGeneration: identity.AllocationGeneration, MemberID: identity.MemberID, StoreID: identity.StoreID,
		NodeIncarnation: identity.NodeIncarnation, Term: 2, Command: commandFenceFromPublication(fixture.authority, identity, 1)}, Applied: 9}
	for _, name := range []string{"isolated-former-leader", "response-lost", "retained-23"} {
		request := fixture.readRequest(0, state, rf3FaultKey(t, name))
		var frame bytes.Buffer
		if err := shardservice.EncodeReplicatedRequestBorrowed(&frame, request); err != nil {
			t.Fatal("harness read rejected before server", err)
		}
		decoded, err := shardservice.DecodeReplicatedRequest(&frame)
		if err != nil || decoded.Fence != state.Fence || decoded.MinimumApplied != state.Applied || decoded.MaxValueBytes != fixture.maxReadValueBytes || !bytes.Equal(decoded.Key, request.Key) {
			t.Fatalf("decoded=%+v err=%v", decoded, err)
		}
	}
	state.Applied = 0
	if err := shardservice.EncodeReplicatedRequestBorrowed(new(bytes.Buffer), fixture.readRequest(0, state, rf3FaultKey(t, "missing-floor"))); !errors.Is(err, shardservice.ErrReplicatedWire) {
		t.Fatal("invalid zero observation floor was hidden", err)
	}
}

func TestRF3FaultLeaderObservationRequiresLiveLeaderAndTerm(t *testing.T) {
	state := func(member, leader, term uint64) shardservice.ReplicatedMemberState {
		return shardservice.ReplicatedMemberState{LeaderID: leader,
			Fence: shardservice.ReplicatedFence{MemberID: member, Term: term}}
	}
	states := map[int]shardservice.ReplicatedMemberState{1: state(2, 1, 4), 2: state(3, 1, 4)}
	if _, ok := rf3FaultObservedLeader([]int{1, 2}, states); ok {
		t.Fatal("stopped leader inferred from stale follower hints")
	}
	states[1], states[2] = state(2, 2, 5), state(3, 2, 5)
	if leader, ok := rf3FaultObservedLeader([]int{1, 2}, states); !ok || leader != 1 {
		t.Fatal("live replacement leader not recognized")
	}
	for _, bad := range []shardservice.ReplicatedMemberState{
		state(3, 2, 4), state(3, 3, 5), state(2, 2, 5), {},
	} {
		states[2] = bad
		if _, ok := rf3FaultObservedLeader([]int{1, 2}, states); ok {
			t.Fatal("inconsistent member/term/leader observation accepted")
		}
	}
	if _, ok := rf3FaultObservedLeader(nil, states); ok {
		t.Fatal("empty observation accepted")
	}
}
