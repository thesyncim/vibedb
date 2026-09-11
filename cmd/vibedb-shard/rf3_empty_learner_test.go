package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/nodecontrol"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
)

func TestRF3EnrollCertifiedRosterPeersPublishesDirectory(t *testing.T) {
	local := rafttransport.NodeID{9}
	remote := rafttransport.NodeID{1}
	second := rafttransport.NodeID{2}
	third := rafttransport.NodeID{3}
	domain := rafttransport.TrustDomain{ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2}}
	registry, err := rafttransport.NewEmptyRegistry(local, domain, rafttransport.Limits{
		MaxGroups: 1, MaxMembers: 4, MaxPeers: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	group := raftmember.GroupKey{
		ClusterID: domain.ClusterID, ClusterIncarnation: domain.ClusterIncarnation,
		TopologyRecoveryEpoch: 1, ShardIncarnation: [16]byte{3}, GroupID: [16]byte{4},
	}
	spec := nodecontrol.PreparationSpec{
		Target: nodecontrol.PreparationMember{Node: local, ServiceKeyDigest: replication.Digest{9}, NodeIncarnation: 1, NodeRevision: 1},
		InitialVoters: [3]nodecontrol.PreparationMember{
			{MemberID: 1, Node: remote, PeerAddress: "127.0.0.1:21001", ServiceKeyDigest: replication.Digest{1}, NodeIncarnation: 7, NodeRevision: 11},
			{MemberID: 2, Node: second, PeerAddress: "127.0.0.1:21002", ServiceKeyDigest: replication.Digest{2}, NodeIncarnation: 8, NodeRevision: 12},
			{MemberID: 3, Node: third, PeerAddress: "127.0.0.1:21003", ServiceKeyDigest: replication.Digest{3}, NodeIncarnation: 9, NodeRevision: 13},
		},
	}
	roster := []rafttransport.Member{
		{Group: group, ReplicaSetVersion: 1, MemberID: 1, Node: remote, Role: rafttransport.MemberVoter},
		{Group: group, ReplicaSetVersion: 1, MemberID: 2, Node: second, Role: rafttransport.MemberVoter},
		{Group: group, ReplicaSetVersion: 1, MemberID: 3, Node: third, Role: rafttransport.MemberVoter},
		{Group: group, ReplicaSetVersion: 1, MemberID: 4, Node: local, Role: rafttransport.MemberLearner},
	}
	if err := registry.InstallGroup(roster, func(func()) error { return nil }); !errors.Is(err, rafttransport.ErrNodeNotFound) {
		t.Fatalf("un-enrolled group install error = %v, want ErrNodeNotFound", err)
	}
	certified := replication.Digest(sha256.Sum256([]byte("certified-manifest")))
	if err := rf3EnrollCertifiedRosterPeers(context.Background(), registry, registry, spec, certified, domain); err != nil {
		t.Fatalf("enroll certified roster: %v", err)
	}
	if err := rf3EnrollCertifiedRosterPeers(context.Background(), registry, registry, spec, certified, domain); err != nil {
		t.Fatalf("idempotent enroll: %v", err)
	}
	for _, voter := range spec.InitialVoters {
		peer, lookupErr := registry.PhysicalPeer(voter.Node)
		if lookupErr != nil || peer.Endpoint != voter.PeerAddress ||
			peer.ServiceKeyDigest != [32]byte(voter.ServiceKeyDigest) {
			t.Fatalf("enrolled %x peer=%+v err=%v", voter.Node[:1], peer, lookupErr)
		}
	}
	if err := registry.InstallGroup(roster, func(publish func()) error {
		publish()
		return nil
	}); err != nil {
		t.Fatalf("dynamic group install after enrollment: %v", err)
	}
}
