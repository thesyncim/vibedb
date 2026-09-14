package main

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"go.etcd.io/raft/v3/raftpb"
)

func TestRF3EnrollmentPeerReceiptRestoresExactDynamicEndpoint(t *testing.T) {
	root := t.TempDir()
	group := serveRF3TestGroup()
	base := serveRF3TestManifest()
	base.Route = rf3ManifestGroupRoute{Group: group, MembershipGrantPath: filepath.Join(root, "membership-grant")}
	grantManifest := base
	grantManifest.EnrolledTarget = serveRF3TestEnrolledTarget()
	grant := rf3MembershipGrantFixture(grantManifest, group, 9)
	if err := persistRF3MembershipGrant(base.Route.MembershipGrantPath, grant); err != nil {
		t.Fatal(err)
	}
	stable := make([]rafttransport.Member, len(base.memberRoster()))
	for index, member := range base.memberRoster() {
		stable[index] = rafttransport.Member{Group: group, ReplicaSetVersion: grant.InitialReplicaSetVersion,
			MemberID: member.MemberID, Node: member.NodeID, Role: rafttransport.MemberVoter}
	}
	rosterDigest, err := rafttransport.StableRosterDigest(stable)
	if err != nil {
		t.Fatal(err)
	}
	domain := rafttransport.TrustDomain{ClusterID: group.ClusterID, ClusterIncarnation: group.ClusterIncarnation}
	peer := rafttransport.PhysicalPeer{NodeID: grantManifest.EnrolledTarget.NodeID, Node: grantManifest.EnrolledTarget.NodeID,
		TrustDomain: domain, Incarnation: 6, Revision: 7, ServiceKeyDigest: [32]byte{11},
		EnrollmentDigest: grant.Digest(), Endpoint: grantManifest.EnrolledTarget.PeerAddress,
		Address: grantManifest.EnrolledTarget.PeerAddress, State: rafttransport.PeerEnrolled}
	intent := rafttransport.EnrollmentIntent{
		Digest: grant.Digest(), Domain: domain, Peer: peer, Group: group,
		Member: rafttransport.Member{Group: group, ReplicaSetVersion: grant.InitialReplicaSetVersion,
			MemberID: grant.TargetMember, Node: peer.NodeID, Role: rafttransport.MemberEnrolled},
		ExpectedRosterDigest: rosterDigest, DirectoryRevision: 2,
	}
	store, err := openRF3EnrollmentPeerStore(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.record(intent, grant); err != nil {
		t.Fatal(err)
	}
	reopened, err := openRF3EnrollmentPeerStore(root)
	if err != nil {
		t.Fatal(err)
	}
	publication := raftmodel.Publication{ReplicaSetVersion: 10,
		ConfState: &raftpb.ConfState{Voters: []uint64{1, 2, 3}, Learners: []uint64{4}}}
	target, restored, err := rf3DynamicEnrollmentTarget(base, group, publication, reopened.snapshot())
	if err != nil {
		t.Fatal(err)
	}
	if target == nil || target.MemberID != 4 || target.NodeID != peer.NodeID || target.PeerAddress != peer.Endpoint ||
		restored != peer {
		t.Fatalf("restored target=%+v peer=%+v, want target=%+v peer=%+v", target, restored, target, peer)
	}

	tampered := reopened.snapshot()
	tampered[0].PeerAddress = "foreign.example:17400"
	if _, _, err := rf3DynamicEnrollmentTarget(base, group, publication, tampered); !errors.Is(err, errRF3EnrollmentPeerReceipt) {
		t.Fatalf("tampered endpoint error=%v, want receipt rejection", err)
	}
	foreign := publication
	foreign.ConfState = &raftpb.ConfState{Voters: []uint64{1, 2, 3}, Learners: []uint64{5}}
	if _, _, err := rf3DynamicEnrollmentTarget(base, group, foreign, reopened.snapshot()); !errors.Is(err, errRF3EnrollmentPeerReceipt) {
		t.Fatalf("foreign member error=%v, want receipt rejection", err)
	}
}

func TestRF3EnrollmentReceiptPersistsOnlyAfterGrantAndRosterCommit(t *testing.T) {
	root := t.TempDir()
	group := serveRF3TestGroup()
	manifest := serveRF3TestManifest()
	manifest.Route.Group = group
	manifest.EnrolledTarget = serveRF3TestEnrolledTarget()
	grant := rf3MembershipGrantFixture(manifest, group, 9)
	domain := rafttransport.TrustDomain{ClusterID: group.ClusterID, ClusterIncarnation: group.ClusterIncarnation}
	initial := []rafttransport.Member{
		{Group: group, ReplicaSetVersion: grant.InitialReplicaSetVersion, MemberID: 1, Node: rafttransport.NodeID{1}, Role: rafttransport.MemberVoter},
		{Group: group, ReplicaSetVersion: grant.InitialReplicaSetVersion, MemberID: 2, Node: rafttransport.NodeID{2}, Role: rafttransport.MemberVoter},
		{Group: group, ReplicaSetVersion: grant.InitialReplicaSetVersion, MemberID: 3, Node: rafttransport.NodeID{3}, Role: rafttransport.MemberVoter},
	}
	peer := func(node rafttransport.NodeID, endpoint string, digest byte) rafttransport.PhysicalPeer {
		return rafttransport.PhysicalPeer{NodeID: node, Node: node, TrustDomain: domain, Incarnation: 1, Revision: 1,
			ServiceKeyDigest: [32]byte{digest}, Endpoint: endpoint, Address: endpoint, State: rafttransport.PeerEnrolled}
	}
	registry, err := rafttransport.NewStaticRegistryWithDirectory(rafttransport.NodeID{1}, initial,
		[]rafttransport.PhysicalPeer{peer(rafttransport.NodeID{1}, "member-1.internal:17400", 1),
			peer(rafttransport.NodeID{2}, "member-2.internal:17400", 2), peer(rafttransport.NodeID{3}, "member-3.internal:17400", 3),
			{NodeID: grant.TargetNode, Node: grant.TargetNode, TrustDomain: domain, Incarnation: 6, Revision: 7,
				ServiceKeyDigest: [32]byte{11}, Endpoint: grantManifestTargetAddress(manifest), Address: grantManifestTargetAddress(manifest), State: rafttransport.PeerEnrolled}},
		1, rafttransport.Limits{MaxGroups: 1, MaxMembers: 4, MaxPeers: 4})
	if err != nil {
		t.Fatal(err)
	}
	rosterDigest, ok := registry.RosterDigest(group)
	if !ok {
		t.Fatal("source roster digest missing")
	}
	intent := rafttransport.EnrollmentIntent{Digest: grant.Digest(), Domain: domain,
		Peer: rafttransport.PhysicalPeer{NodeID: grant.TargetNode, Node: grant.TargetNode, TrustDomain: domain,
			Incarnation: 6, Revision: 7, ServiceKeyDigest: [32]byte{11}, Endpoint: grantManifestTargetAddress(manifest),
			Address: grantManifestTargetAddress(manifest), EnrollmentDigest: grant.Digest(), State: rafttransport.PeerEnrolled},
		Group: group, Member: rafttransport.Member{Group: group, ReplicaSetVersion: grant.InitialReplicaSetVersion,
			MemberID: grant.TargetMember, Node: grant.TargetNode, Role: rafttransport.MemberEnrolled},
		ExpectedRosterDigest: rosterDigest, DirectoryRevision: 1, Grant: grant}
	store, err := openRF3EnrollmentPeerStore(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateRF3EnrollmentGrant(registry, intent); err != nil {
		t.Fatalf("valid pre-roster grant rejected before member commit: %v", err)
	}
	if err := registry.EnrollMember(intent, rafttransport.EnrollmentVerifierFunc(func(rafttransport.EnrollmentIntent) error { return nil })); err != nil {
		t.Fatalf("enroll target: %v", err)
	}
	if err := validateRF3EnrollmentGrant(registry, intent); err != nil {
		t.Fatalf("grant rejected after exact member commit: %v", err)
	}
	if err := store.record(intent, grant); err != nil {
		t.Fatalf("persist receipt: %v", err)
	}
	reopened, err := openRF3EnrollmentPeerStore(root)
	if err != nil || len(reopened.snapshot()) != 1 {
		t.Fatalf("receipt was not durable before ACK boundary: count=%d err=%v", len(reopened.snapshot()), err)
	}
	bad := intent
	bad.Grant.TargetNode[0]++
	if err := validateRF3EnrollmentGrant(registry, bad); !errors.Is(err, errRF3EnrollmentPeerReceipt) {
		t.Fatalf("foreign grant accepted: %v", err)
	}
}

func grantManifestTargetAddress(manifest rf3Manifest) string {
	return manifest.EnrolledTarget.PeerAddress
}

func TestRF3EnrollmentPeerReceiptRejectsAmbiguousReplay(t *testing.T) {
	group := serveRF3TestGroup()
	manifest := serveRF3TestManifest()
	manifest.Route.Group = group
	publication := raftmodel.Publication{ReplicaSetVersion: 10,
		ConfState: &raftpb.ConfState{Voters: []uint64{1, 2, 3}, Learners: []uint64{4}}}
	receipts := []rf3EnrollmentPeerReceipt{{Group: group, MemberID: 4}, {Group: group, MemberID: 4}}
	if _, _, err := rf3DynamicEnrollmentTarget(manifest, group, publication, receipts); !errors.Is(err, errRF3EnrollmentPeerReceipt) {
		t.Fatalf("ambiguous receipt error=%v, want receipt rejection", err)
	}
}
