package main

import (
	"errors"
	"path/filepath"
	"slices"
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
	restored, err := rf3RecoveredEnrollmentRoster(base, group, 1, publication, reopened.snapshot())
	if err != nil {
		t.Fatal(err)
	}
	if restored.dynamicMember != 4 || len(restored.members) != 4 || len(restored.peers) != 1 || restored.peers[0] != peer || restored.endpoints[peer.NodeID] != peer.Endpoint {
		t.Fatalf("restored roster=%+v, want exact peer=%+v", restored, peer)
	}

	refreshed := base
	refreshed.Members[0] = rf3ManifestMember{MemberID: uint64(peer.NodeID[0]), NodeID: peer.NodeID, PeerAddress: peer.Endpoint}
	slices.SortFunc(refreshed.Members[:], func(a, b rf3ManifestMember) int {
		if a.MemberID < b.MemberID {
			return -1
		}
		if a.MemberID > b.MemberID {
			return 1
		}
		return 0
	})
	refreshed.Route.MembershipGrantPath = ""
	completed := raftmodel.Publication{ReplicaSetVersion: 12, ConfState: &raftpb.ConfState{Voters: []uint64{2, 3, 4}}}
	if _, err := rf3RecoveredEnrollmentRoster(refreshed, group, 2, completed, reopened.snapshot()); err != nil {
		t.Fatalf("current operator roster rejected historical receipt: %v", err)
	}

	tampered := reopened.snapshot()
	tampered[0].PeerAddress = "foreign.example:17400"
	if _, err := rf3RecoveredEnrollmentRoster(base, group, 1, publication, tampered); !errors.Is(err, errRF3EnrollmentPeerReceipt) {
		t.Fatalf("tampered endpoint error=%v, want receipt rejection", err)
	}
	foreign := publication
	foreign.ConfState = &raftpb.ConfState{Voters: []uint64{1, 2, 3}, Learners: []uint64{5}}
	if _, err := rf3RecoveredEnrollmentRoster(base, group, 1, foreign, reopened.snapshot()); !errors.Is(err, errRF3EnrollmentPeerReceipt) {
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
	// A serving-process restart rebuilds its registry directory fence from 1,
	// while an earlier process may have persisted this receipt at a later
	// directory revision. The grant and physical endpoint remain the same, so
	// both the corrected current revision and the restart-time value are safe
	// idempotent replays. The stored receipt remains the durable witness.
	for _, revision := range []uint64{4, 1} {
		replay := intent
		replay.DirectoryRevision = revision
		if err := store.record(replay, grant); err != nil {
			t.Fatalf("directory revision %d replay: %v", revision, err)
		}
	}
	stored := store.snapshot()
	if len(stored) != 1 || stored[0].EnrollmentDigest != intent.Digest || stored[0].DirectoryRevision != intent.DirectoryRevision {
		t.Fatalf("replay changed durable identity: stored=%+v", stored)
	}
	changedEndpoint := intent
	changedEndpoint.Peer.Endpoint = "foreign.example:17400"
	changedEndpoint.Peer.Address = changedEndpoint.Peer.Endpoint
	if err := store.record(changedEndpoint, grant); !errors.Is(err, errRF3EnrollmentPeerReceipt) {
		t.Fatalf("changed endpoint replay error=%v, want receipt rejection", err)
	}
	changedRevision := intent
	changedRevision.Peer.Revision++
	if err := store.record(changedRevision, grant); !errors.Is(err, errRF3EnrollmentPeerReceipt) {
		t.Fatalf("changed node revision replay error=%v, want receipt rejection", err)
	}
	// Reproduce the restart direction from the qualification: the durable
	// source receipt was written at revision 4, then the rebuilt registry
	// accepts the same authenticated intent at its initial revision 1.
	restartedRoot := t.TempDir()
	restarted, err := openRF3EnrollmentPeerStore(restartedRoot)
	if err != nil {
		t.Fatal(err)
	}
	storedAtFour := intent
	storedAtFour.DirectoryRevision = 4
	if err := restarted.record(storedAtFour, grant); err != nil {
		t.Fatalf("persist revision-4 receipt: %v", err)
	}
	restartReplay := intent
	restartReplay.DirectoryRevision = 1
	restartRegistry, err := rafttransport.NewStaticRegistryWithDirectory(
		rafttransport.NodeID{1}, initial,
		[]rafttransport.PhysicalPeer{
			peer(rafttransport.NodeID{1}, "member-1.internal:17400", 1),
			peer(rafttransport.NodeID{2}, "member-2.internal:17400", 2),
			peer(rafttransport.NodeID{3}, "member-3.internal:17400", 3),
			{NodeID: grant.TargetNode, Node: grant.TargetNode, TrustDomain: domain, Incarnation: 6, Revision: 7,
				ServiceKeyDigest: [32]byte{11}, Endpoint: grantManifestTargetAddress(manifest), Address: grantManifestTargetAddress(manifest), State: rafttransport.PeerEnrolled},
		},
		1, rafttransport.Limits{MaxGroups: 1, MaxMembers: 4, MaxPeers: 4},
	)
	if err != nil {
		t.Fatal(err)
	}
	callback := func() error {
		if err := validateRF3EnrollmentGrant(restartRegistry, restartReplay); err != nil {
			return err
		}
		return restarted.record(restartReplay, restartReplay.Grant)
	}
	if err := restartRegistry.EnrollMemberWithCommit(restartReplay,
		rafttransport.EnrollmentVerifierFunc(func(rafttransport.EnrollmentIntent) error { return nil }), callback); err != nil {
		t.Fatalf("restart revision-1 callback replay: %v", err)
	}
	restartedReceipts := restarted.snapshot()
	if len(restartedReceipts) != 1 || restartedReceipts[0].DirectoryRevision != 4 || restartedReceipts[0].EnrollmentDigest != intent.Digest {
		t.Fatalf("restart replay changed durable receipt: %+v", restartedReceipts)
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
	if _, err := rf3RecoveredEnrollmentRoster(manifest, group, 1, publication, receipts); !errors.Is(err, errRF3EnrollmentPeerReceipt) {
		t.Fatalf("ambiguous receipt error=%v, want receipt rejection", err)
	}
}

func TestRF3EnrollmentRosterRecoversRepeatedMovesAndNodeReuse(t *testing.T) {
	base := serveRF3TestManifest()
	group := serveRF3TestGroup()
	current := base
	var receipts []rf3EnrollmentPeerReceipt
	for cycle := 0; cycle < 3; cycle++ {
		version := uint64(1 + cycle*3)
		target := serveRF3TestEnrolledTarget()
		target.MemberID = uint64(4 + cycle)
		if cycle > 0 {
			target.NodeID = base.Members[cycle-1].NodeID
			target.PeerAddress = base.Members[cycle-1].PeerAddress
		}
		current.EnrolledTarget = target
		grant := rf3MembershipGrantFixture(current, group, version)
		grant.CatalogGeneration += uint64(cycle)
		members := rf3GrantTransportMembers(current, group, version, rafttransport.MemberEnrolled)
		digest, err := rafttransport.StableRosterDigest(members[:3])
		if err != nil {
			t.Fatal(err)
		}
		domain := rafttransport.TrustDomain{ClusterID: group.ClusterID, ClusterIncarnation: group.ClusterIncarnation}
		peer := rafttransport.PhysicalPeer{NodeID: target.NodeID, Node: target.NodeID, TrustDomain: domain, Incarnation: 1, Revision: 1, ServiceKeyDigest: [32]byte{11}, EnrollmentDigest: grant.Digest(), Endpoint: target.PeerAddress, Address: target.PeerAddress, State: rafttransport.PeerEnrolled}
		receipt, err := newRF3EnrollmentPeerReceipt(rafttransport.EnrollmentIntent{Digest: grant.Digest(), Domain: domain, Peer: peer, Group: group, Member: members[3], ExpectedRosterDigest: digest, DirectoryRevision: 1}, grant)
		if err != nil {
			t.Fatal(err)
		}
		receipts = append(receipts, receipt)
		current.Members[0] = rf3ManifestMember{MemberID: target.MemberID, NodeID: target.NodeID, PeerAddress: target.PeerAddress}
		slices.SortFunc(current.Members[:], func(a, b rf3ManifestMember) int {
			if a.MemberID < b.MemberID {
				return -1
			}
			if a.MemberID > b.MemberID {
				return 1
			}
			return 0
		})
		voters := []uint64{current.Members[0].MemberID, current.Members[1].MemberID, current.Members[2].MemberID}
		publication := raftmodel.Publication{ReplicaSetVersion: version + 3, ConfState: &raftpb.ConfState{Voters: voters}}
		recovered, err := rf3RecoveredEnrollmentRoster(base, group, voters[0], publication, receipts)
		if err != nil {
			t.Fatalf("cycle %d: %v", cycle, err)
		}
		if len(recovered.members) != 3 || len(recovered.endpoints) != 3 || !recovered.native {
			t.Fatalf("cycle %d accumulated obsolete mappings: %+v", cycle, recovered)
		}
		for _, member := range recovered.members {
			if !slices.Contains(voters, member.MemberID) || member.Role != rafttransport.MemberVoter {
				t.Fatalf("obsolete serving identity: %+v", member)
			}
		}
		if cycle > 0 {
			if _, err := rf3RecoveredEnrollmentRoster(base, group, voters[0], publication, receipts[1:]); !errors.Is(err, errRF3EnrollmentPeerReceipt) {
				t.Fatalf("broken chain accepted: %v", err)
			}
		}
	}
	bad := slices.Clone(receipts)
	bad[2].Grant.InitialRosterDigest[0]++
	bad[2].EnrollmentDigest = bad[2].Grant.Digest()
	bad[2].ReceiptDigest = bad[2].computedDigest()
	publication := raftmodel.Publication{ReplicaSetVersion: 10, ConfState: &raftpb.ConfState{Voters: []uint64{4, 5, 6}}}
	if _, err := rf3RecoveredEnrollmentRoster(base, group, 4, publication, bad); !errors.Is(err, errRF3EnrollmentPeerReceipt) {
		t.Fatalf("divergent certified chain accepted: %v", err)
	}
}
