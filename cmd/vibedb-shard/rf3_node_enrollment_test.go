package main

import (
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
)

func TestRF3InitialNodeRetirementAllowsFreshMemberWithoutRestart(t *testing.T) {
	group := serveRF3TestGroup()
	domain := rafttransport.TrustDomain{ClusterID: group.ClusterID, ClusterIncarnation: group.ClusterIncarnation}
	var members []rafttransport.Member
	var peers []rafttransport.PhysicalPeer
	for i := byte(1); i <= 3; i++ {
		node := rafttransport.NodeID{i}
		members = append(members, rafttransport.Member{Group: group, MemberID: uint64(i), Node: node,
			Role: rafttransport.MemberVoter, ReplicaSetVersion: 1})
		peers = append(peers, rafttransport.PhysicalPeer{NodeID: node, TrustDomain: domain, Incarnation: 1,
			Revision: 1, ServiceKeyDigest: [32]byte{i}, Endpoint: "127.0.0.1:9000", State: rafttransport.PeerEnrolled})
	}
	registry, err := rafttransport.NewNodeRegistryWithDirectory(peers[0].NodeID, members, peers, 1,
		rafttransport.Limits{MaxGroups: 1, MaxMembers: 3, MaxPeers: 3})
	if err != nil {
		t.Fatal(err)
	}
	runtime := &rf3NodeRuntime{registry: registry}
	old := raftmember.RuntimeIdentity{Group: group, MemberID: 1}
	for i := 0; i < 2; i++ {
		if err := runtime.Unregister(old); err != nil {
			t.Fatalf("retirement retry %d: %v", i, err)
		}
	}
	if _, err := registry.LocalMember(group); !errors.Is(err, rafttransport.ErrGroupNotFound) {
		t.Fatalf("retired mapping remains: %v", err)
	}
	members[0].MemberID = 4
	for i := range members {
		members[i].ReplicaSetVersion = 3
	}
	if err := registry.InstallGroup(members, func(publish func()) error { publish(); return nil }); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Unregister(old); !errors.Is(err, raftservice.ErrServingFence) {
		t.Fatalf("stale retirement removed new member: %v", err)
	}
	if member, err := registry.LocalMember(group); err != nil || member != 4 {
		t.Fatalf("new local member=%d err=%v", member, err)
	}
}

func TestRF3InitialNativeAuthorityRetiresExactGroup(t *testing.T) {
	registry, gate, prepared, states := nativeAuthorityFixture(t, 2)
	authority, err := newRF3NativeAuthorities(registry, gate, prepared, nativeAuthorityIdentities(states), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	foreign := states[0].Identity
	foreign.StoreID[0]++
	if err := authority.unregisterDynamic(foreign); !errors.Is(err, raftservice.ErrServingFence) {
		t.Fatalf("foreign retirement: %v", err)
	}
	if !authority.serving(states[0]) {
		t.Fatal("foreign retirement changed serving authority")
	}
	for i := 0; i < 2; i++ {
		if err := authority.unregisterDynamic(states[0].Identity); err != nil {
			t.Fatal(err)
		}
	}
	if authority.serving(states[0]) || !authority.serving(states[1]) {
		t.Fatal("retirement did not isolate the exact group")
	}
}
