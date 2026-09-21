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
	// Another group may certify the same voters with a different manifest.
	// Its own proof must be checked without changing the physical directory
	// proof or treating the shared endpoints as conflicting enrollments.
	before := registry.PeerDirectoryRevision()
	secondCertificate := replication.Digest(sha256.Sum256([]byte("another-certified-manifest")))
	if err := rf3EnrollCertifiedRosterPeers(context.Background(), registry, registry, spec, secondCertificate, domain); err != nil {
		t.Fatalf("shared peers from another group: %v", err)
	}
	priorPeer, err := registry.PhysicalPeer(remote)
	if err != nil {
		t.Fatal(err)
	}
	priorEnrollmentDigest := priorPeer.EnrollmentDigest
	if registry.PeerDirectoryRevision() != before {
		t.Fatal("unchanged physical peers advanced the directory revision")
	}
	// A source can remain the same authenticated physical peer while its
	// committed directory revision advances during draining. The empty target
	// must refresh that monotone revision before replaying the roster; rejecting
	// it strands a valid bootstrap behind a stale physical-directory cut.
	updatedSpec := spec
	updatedSpec.InitialVoters[0].NodeRevision++
	if err := rf3EnrollCertifiedRosterPeersFiltered(
		context.Background(), registry, registry, updatedSpec, secondCertificate, domain,
		func(rafttransport.NodeID) bool { return true },
	); err != nil {
		t.Fatalf("authenticated newer source directory revision: %v", err)
	}
	updatedPeer, err := registry.PhysicalPeer(remote)
	if err != nil || updatedPeer.Revision != updatedSpec.InitialVoters[0].NodeRevision ||
		updatedPeer.EnrollmentDigest != priorEnrollmentDigest {
		t.Fatalf("updated source directory revision peer=%+v err=%v", updatedPeer, err)
	}
	if err := rf3EnrollCertifiedRosterPeersFiltered(
		context.Background(), registry, registry, spec, secondCertificate, domain,
		func(rafttransport.NodeID) bool { return true },
	); err != nil {
		t.Fatalf("historical receipt cannot replay the unchanged physical identity: %v", err)
	}
	if current, err := registry.PhysicalPeer(remote); err != nil || current != updatedPeer {
		t.Fatalf("historical receipt replaced current physical proof: %+v, %v", current, err)
	}
	changedSpec := spec
	changedSpec.InitialVoters[0].ServiceKeyDigest = replication.Digest{42}
	changedSpec.InitialVoters[0].NodeRevision = updatedSpec.InitialVoters[0].NodeRevision
	if err := rf3EnrollCertifiedRosterPeersFiltered(
		context.Background(), registry, registry, changedSpec, secondCertificate, domain,
		func(rafttransport.NodeID) bool { return true },
	); !errors.Is(err, rafttransport.ErrPeerConflict) {
		t.Fatalf("changed physical key reused old enrollment: %v", err)
	}
	changedEndpoint := updatedSpec
	changedEndpoint.InitialVoters[0].PeerAddress = "127.0.0.1:21999"
	if err := rf3EnrollCertifiedRosterPeersFiltered(
		context.Background(), registry, registry, changedEndpoint, secondCertificate, domain,
		func(rafttransport.NodeID) bool { return true },
	); !errors.Is(err, rafttransport.ErrPeerConflict) {
		t.Fatalf("changed physical endpoint reused old enrollment: %v", err)
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

func TestRF3RetainedPeerRecoveryPreservesCurrentPhysicalProof(t *testing.T) {
	domain := rafttransport.TrustDomain{ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2}}
	peer := rafttransport.PhysicalPeer{
		NodeID: rafttransport.NodeID{2}, Node: rafttransport.NodeID{2}, TrustDomain: domain,
		Incarnation: 3, Revision: 4, ServiceKeyDigest: [32]byte{5}, EnrollmentDigest: [32]byte{6},
		Endpoint: "127.0.0.1:21001", Address: "127.0.0.1:21001", State: rafttransport.PeerEnrolled,
	}
	for _, test := range []struct {
		name   string
		change func(*rafttransport.PhysicalPeer)
		replay bool
	}{
		{name: "same physical identity from another group"},
		{name: "changed key", change: func(p *rafttransport.PhysicalPeer) { p.ServiceKeyDigest[0]++ }},
		{name: "changed incarnation", change: func(p *rafttransport.PhysicalPeer) { p.Incarnation++ }},
		{name: "historical revision", change: func(p *rafttransport.PhysicalPeer) { p.Revision-- }, replay: true},
		{name: "changed endpoint", change: func(p *rafttransport.PhysicalPeer) { p.Endpoint = "127.0.0.1:21002"; p.Address = p.Endpoint }},
		{name: "retired peer", change: func(p *rafttransport.PhysicalPeer) { p.State = rafttransport.PeerRetired }},
		{name: "wrong domain", change: func(p *rafttransport.PhysicalPeer) { p.TrustDomain.ClusterIncarnation[0]++ }},
	} {
		t.Run(test.name, func(t *testing.T) {
			registry, err := rafttransport.NewEmptyRegistry(rafttransport.NodeID{1}, domain,
				rafttransport.Limits{MaxGroups: 1, MaxMembers: 4, MaxPeers: 4})
			if err != nil {
				t.Fatal(err)
			}
			if err = rf3EnrollRetainedPeers(t.Context(), registry, registry, []rafttransport.PhysicalPeer{peer}); err != nil {
				t.Fatalf("initial recovery: %v", err)
			}
			revision := registry.PeerDirectoryRevision()
			retained := peer
			retained.EnrollmentDigest = [32]byte{7}
			if test.change != nil {
				test.change(&retained)
			}
			for attempt := 0; attempt < 2; attempt++ {
				err = rf3EnrollRetainedPeers(t.Context(), registry, registry, []rafttransport.PhysicalPeer{retained})
				if (test.change == nil || test.replay) && err != nil {
					t.Fatalf("recover identical peer from another certified group: %v", err)
				}
				if test.change != nil && !test.replay && err == nil {
					t.Fatal("changed physical identity accepted")
				}
				current, lookupErr := registry.PhysicalPeer(peer.NodeID)
				if lookupErr != nil || current != peer || registry.PeerDirectoryRevision() != revision {
					t.Fatalf("recovery changed current directory proof: peer=%+v error=%v", current, lookupErr)
				}
			}
		})
	}
}
