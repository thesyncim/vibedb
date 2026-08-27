//go:build darwin || linux

package main

import (
	"bytes"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/shardservice"
)

// Run the actual external-harness request builder through the shipped wire
// grammar on every host, before Linux process/fault qualification is needed.
func TestRF3FaultReadRequestCanonicalPreflight(t *testing.T) {
	store := rf3CommandStoreIdentity(1)
	identity := raftmember.RuntimeIdentity{Group: rf3CommandGroup(), AllocationGeneration: store.AllocationGeneration,
		MemberID: store.MemberID, StoreID: store.StoreID, NodeIncarnation: 1, RelationManifestDigest: [32]byte{1}}
	fixture := &rf3FaultFixture{nodes: [rf3CommandMembers]rafttransport.NodeID{{1}, {2}, {3}}, authority: rf3CommandAuthority()}
	state := shardservice.ReplicatedMemberState{Fence: shardservice.ReplicatedFence{Group: identity.Group,
		AllocationGeneration: identity.AllocationGeneration, MemberID: identity.MemberID, StoreID: identity.StoreID,
		NodeIncarnation: identity.NodeIncarnation, Term: 2, Command: commandFenceFromPublication(fixture.authority, identity, 1)}, Applied: 9}
	for _, test := range []struct {
		name    string
		maximum uint32
	}{
		{"isolated-former-leader", 1 << 20},
		{"response-lost", 1 << 20},
		{"retained-23", walRetentionDocumentBytes + 4096},
	} {
		request := fixture.readRequest(0, state, rf3FaultKey(t, test.name))
		request.MaxValueBytes = test.maximum
		var frame bytes.Buffer
		if err := shardservice.EncodeReplicatedRequestBorrowed(&frame, request); err != nil {
			t.Fatal("harness read rejected before server", err)
		}
		decoded, err := shardservice.DecodeReplicatedRequest(&frame)
		if err != nil || decoded.Fence != state.Fence || decoded.MinimumApplied != state.Applied || decoded.MaxValueBytes != test.maximum || !bytes.Equal(decoded.Key, request.Key) {
			t.Fatalf("decoded=%+v err=%v", decoded, err)
		}
	}
	state.Applied = 0
	if err := shardservice.EncodeReplicatedRequestBorrowed(new(bytes.Buffer), fixture.readRequest(0, state, rf3FaultKey(t, "missing-floor"))); !errors.Is(err, shardservice.ErrReplicatedWire) {
		t.Fatal("invalid zero observation floor was hidden", err)
	}
}
