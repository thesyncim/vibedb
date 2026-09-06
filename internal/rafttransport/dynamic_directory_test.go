package rafttransport

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/membershipgrant"
	"github.com/thesyncim/vibedb/internal/raftmember"
	pb "go.etcd.io/raft/v3/raftpb"
)

func dynamicPeerIntent(
	registry *StaticRegistry,
	peer NodeID,
	digest byte,
	group raftmember.GroupKey,
	memberID uint64,
	roster [sha256.Size]byte,
) EnrollmentIntent {
	intent := EnrollmentIntent{
		Digest: sha256.Sum256([]byte{digest}),
		Domain: registry.TrustDomain(),
		Peer: PhysicalPeer{
			NodeID:           peer,
			TrustDomain:      registry.TrustDomain(),
			Incarnation:      1,
			Revision:         1,
			ServiceKeyDigest: sha256.Sum256(append([]byte("test-service-key/"), peer[:]...)),
			Endpoint:         "127.0.0.1:25001",
			State:            PeerEnrolled,
		},
		DirectoryRevision: registry.PeerDirectoryRevision(),
	}
	if group != (raftmember.GroupKey{}) {
		intent.Group = group
		intent.Member = Member{
			Group:             group,
			ReplicaSetVersion: 1,
			MemberID:          memberID,
			Node:              peer,
			Role:              MemberEnrolled,
		}
		intent.ExpectedRosterDigest = roster
	}
	return intent
}

func allowEnrollment(EnrollmentIntent) error { return nil }

func TestPhysicalPeerBindingUsesExactLeafKeyDigest(t *testing.T) {
	group := testGroup(205)
	domain := TrustDomain{ClusterID: group.ClusterID, ClusterIncarnation: group.ClusterIncarnation}
	local, remote := testNode(1), testNode(2)
	localKey := sha256.Sum256([]byte("local-leaf-spki"))
	remoteKey := sha256.Sum256([]byte("remote-leaf-spki"))
	registry, err := NewStaticRegistryWithDirectory(
		local,
		[]Member{
			{Group: group, ReplicaSetVersion: 1, MemberID: 1, Node: local, Role: MemberVoter},
			{Group: group, ReplicaSetVersion: 1, MemberID: 2, Node: remote, Role: MemberVoter},
		},
		[]PhysicalPeer{
			{NodeID: local, TrustDomain: domain, Incarnation: 7, Revision: 9,
				ServiceKeyDigest: localKey, State: PeerEnrolled},
			{NodeID: remote, TrustDomain: domain, Incarnation: 4, Revision: 8,
				ServiceKeyDigest: remoteKey, State: PeerEnrolled},
		},
		12,
		Limits{MaxGroups: 1, MaxMembers: 2, MaxPeers: 2},
	)
	if err != nil {
		t.Fatal(err)
	}
	identity := PeerIdentity{TrustDomain: domain, Node: remote}
	if err := registry.VerifyPeerBinding(identity, remoteKey); err != nil {
		t.Fatalf("exact key rejected: %v", err)
	}
	if err := registry.VerifyPeerBinding(identity, sha256.Sum256([]byte("rotated-leaf-spki"))); !errors.Is(err, ErrPeerKeyMismatch) {
		t.Fatalf("rotated key error = %v, want ErrPeerKeyMismatch", err)
	}
	if err := registry.VerifyPeerBinding(identity, [sha256.Size]byte{}); !errors.Is(err, ErrPeerKeyMismatch) {
		t.Fatalf("missing key error = %v, want ErrPeerKeyMismatch", err)
	}
}

func TestNewEmptyRegistryEnrollsPeerBeforeDynamicGroupInstall(t *testing.T) {
	group := testGroup(201)
	domain := TrustDomain{ClusterID: group.ClusterID, ClusterIncarnation: group.ClusterIncarnation}
	local, remote := testNode(1), testNode(2)
	registry, err := NewEmptyRegistry(local, domain, Limits{
		MaxGroups: 2, MaxMembers: 4, MaxPeers: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	roster := []Member{
		{Group: group, ReplicaSetVersion: 1, MemberID: 1, Node: local, Role: MemberVoter},
		{Group: group, ReplicaSetVersion: 1, MemberID: 2, Node: remote, Role: MemberVoter},
	}
	if err := registry.InstallGroup(roster, func(func()) error { return nil }); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("un-enrolled group install error = %v, want ErrNodeNotFound", err)
	}
	intent := dynamicPeerIntent(registry, remote, 1, raftmember.GroupKey{}, 0, [sha256.Size]byte{})
	if err := registry.EnrollPeer(intent, EnrollmentVerifierFunc(allowEnrollment)); err != nil {
		t.Fatalf("EnrollPeer: %v", err)
	}
	if !registry.IsPeerEnrolled(remote) {
		t.Fatal("enrolled peer is not active")
	}
	if got := len(registry.PeerDirectory()); got != 2 {
		t.Fatalf("directory size = %d, want 2", got)
	}
	if err := registry.InstallGroup(roster, func(publish func()) error {
		publish()
		return nil
	}); err != nil {
		t.Fatalf("dynamic group install after enrollment: %v", err)
	}
	if member, err := registry.LocalMember(group); err != nil || member != 1 {
		t.Fatalf("LocalMember = %d, %v", member, err)
	}
}

func TestEmptyRegistryRestoresCertifiedLocalIncarnation(t *testing.T) {
	group := testGroup(206)
	domain := TrustDomain{ClusterID: group.ClusterID, ClusterIncarnation: group.ClusterIncarnation}
	local := testNode(1)
	key := sha256.Sum256([]byte("restored-local-key"))
	registry, err := NewEmptyRegistryWithLocalPeer(PhysicalPeer{
		NodeID: local, TrustDomain: domain, Incarnation: 9, Revision: 14,
		ServiceKeyDigest: key, Endpoint: "127.0.0.1:25009", State: PeerEnrolled,
	}, Limits{MaxGroups: 1, MaxMembers: 2, MaxPeers: 2})
	if err != nil {
		t.Fatal(err)
	}
	peer, err := registry.PhysicalPeer(local)
	if err != nil || peer.Incarnation != 9 || peer.Revision != 14 || peer.ServiceKeyDigest != key {
		t.Fatalf("restored local peer=%+v err=%v", peer, err)
	}
	if err := registry.VerifyPeerBinding(PeerIdentity{TrustDomain: domain, Node: local}, key); err != nil {
		t.Fatalf("restored local key binding: %v", err)
	}
}

func TestEnrollMemberKeepsOldDigestForStaggeredAuthorizedTraffic(t *testing.T) {
	group := testGroup(202)
	members := []Member{
		{Group: group, ReplicaSetVersion: 1, MemberID: 1, Node: testNode(1), Role: MemberVoter},
		{Group: group, ReplicaSetVersion: 1, MemberID: 2, Node: testNode(2), Role: MemberVoter},
		{Group: group, ReplicaSetVersion: 1, MemberID: 3, Node: testNode(3), Role: MemberVoter},
	}
	newRegistry := func(local NodeID) *StaticRegistry {
		registry, err := NewStaticRegistry(local, members, Limits{MaxGroups: 1, MaxMembers: 4, MaxPeers: 4})
		if err != nil {
			t.Fatal(err)
		}
		return registry
	}
	sender, receiver := newRegistry(testNode(1)), newRegistry(testNode(2))
	oldDigest, ok := sender.rosterDigest(group)
	if !ok {
		t.Fatal("missing initial roster digest")
	}
	target := testNode(4)
	senderIntent := dynamicPeerIntent(sender, target, 4, group, 4, oldDigest)
	receiverIntent := dynamicPeerIntent(receiver, target, 4, group, 4, oldDigest)
	if err := sender.EnrollMember(senderIntent, EnrollmentVerifierFunc(allowEnrollment)); err != nil {
		t.Fatalf("sender EnrollMember: %v", err)
	}
	if _, err := sender.Role(group, 4); !errors.Is(err, ErrMemberNotFound) {
		t.Fatalf("target role after enrollment error = %v, want ErrMemberNotFound", err)
	}
	message := frameTestMessage(pb.MsgHeartbeat, 1, 2)
	frame, _, err := sender.EncodeOutbound(nil, raftmember.OutboundMessage{
		Group: group, From: 1, To: 2, Message: message,
	})
	if err != nil {
		t.Fatalf("updated sender encoded old-member traffic: %v", err)
	}
	header, _, err := parseFrame(frame)
	if err != nil || header.roster != oldDigest {
		t.Fatalf("updated sender roster = %x, parse error %v; want legacy %x", header.roster, err, oldDigest)
	}
	if _, err := receiver.DecodeInbound(testPeerIdentity(receiver, testNode(1)), frame); err != nil {
		t.Fatalf("old receiver rejected staggered old-digest traffic: %v", err)
	}
	if err := receiver.EnrollMember(receiverIntent, EnrollmentVerifierFunc(allowEnrollment)); err != nil {
		t.Fatalf("receiver EnrollMember: %v", err)
	}
	if _, err := receiver.DecodeInbound(testPeerIdentity(receiver, testNode(1)), frame); err != nil {
		t.Fatalf("updated receiver rejected queued old-digest traffic: %v", err)
	}
	if _, _, err := sender.EncodeOutbound(nil, raftmember.OutboundMessage{
		Group: group, From: 1, To: 4, Message: frameTestMessage(pb.MsgHeartbeat, 1, 4),
	}); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("ungranted target traffic error = %v, want ErrUnauthorized", err)
	}
}

func TestEnrollmentDigestRotatesAcrossCompletedReplacement(t *testing.T) {
	group := testGroup(204)
	members := []Member{
		{Group: group, ReplicaSetVersion: 1, MemberID: 1, Node: testNode(1), Role: MemberVoter},
		{Group: group, ReplicaSetVersion: 1, MemberID: 2, Node: testNode(2), Role: MemberVoter},
		{Group: group, ReplicaSetVersion: 1, MemberID: 3, Node: testNode(3), Role: MemberVoter},
	}
	newRegistry := func(local NodeID) *StaticRegistry {
		registry, err := NewStaticRegistry(local, members, Limits{
			MaxGroups: 1, MaxMembers: 5, MaxPeers: 5,
		})
		if err != nil {
			t.Fatal(err)
		}
		return registry
	}
	sender := newRegistry(testNode(2))
	oldReceiver := newRegistry(testNode(3))
	updatedReceiver := newRegistry(testNode(3))
	firstDigest, ok := sender.rosterDigest(group)
	if !ok {
		t.Fatal("missing initial roster digest")
	}
	firstTarget := testNode(4)
	for _, registry := range []*StaticRegistry{sender, oldReceiver, updatedReceiver} {
		if err := registry.EnrollMember(
			dynamicPeerIntent(registry, firstTarget, 4, group, 4, firstDigest),
			EnrollmentVerifierFunc(allowEnrollment),
		); err != nil {
			t.Fatalf("first EnrollMember: %v", err)
		}
		grant := replacementTestGrant(group, 1, 4, firstTarget, [3]uint64{1, 2, 3})
		if err := registry.InstallTransitionGrant(grant); err != nil {
			t.Fatalf("InstallTransitionGrant: %v", err)
		}
		if err := registry.PublishCommittedAuthority(group, 2, &pb.ConfState{
			Voters: []uint64{1, 2, 3}, Learners: []uint64{4},
		}); err != nil {
			t.Fatalf("publish learner cut: %v", err)
		}
		if err := registry.PublishCommittedAuthority(group, 3, &pb.ConfState{
			Voters: []uint64{1, 2, 3, 4},
		}); err != nil {
			t.Fatalf("publish promotion cut: %v", err)
		}
	}
	firstCycleDigest, ok := sender.rosterDigest(group)
	if !ok || firstCycleDigest == firstDigest {
		t.Fatalf("first replacement did not change roster digest: %x", firstCycleDigest)
	}
	for _, registry := range []*StaticRegistry{sender, oldReceiver, updatedReceiver} {
		if err := registry.CompleteRosterHandoff(RosterHandoffProof{
			Group: group, LegacyDigest: firstDigest, CurrentDigest: firstCycleDigest,
			AuthorityVersion: 3, DirectoryRevision: registry.PeerDirectoryRevision(),
		}); err != nil {
			t.Fatalf("complete first roster handoff: %v", err)
		}
		if err := registry.PublishCommittedAuthority(group, 4, &pb.ConfState{
			Voters: []uint64{2, 3, 4},
		}); err != nil {
			t.Fatalf("publish source removal cut: %v", err)
		}
	}
	queuedBeforeCompaction, _, err := sender.EncodeOutbound(nil, raftmember.OutboundMessage{
		Group: group, From: 2, To: 3, Message: frameTestMessage(pb.MsgHeartbeat, 2, 3),
	})
	if err != nil {
		t.Fatalf("pre-compaction queued frame: %v", err)
	}
	for _, registry := range []*StaticRegistry{sender, oldReceiver, updatedReceiver} {
		grant := replacementTestGrant(group, 1, 4, firstTarget, [3]uint64{1, 2, 3})
		if err := registry.RevokeTransitionGrant(grant); err != nil {
			t.Fatalf("revoke completed grant: %v", err)
		}
		before := registry.PeerDirectoryRevision()
		if err := registry.RetireMember(MemberRetirementProof{
			Group: group, MemberID: 1, Node: testNode(1),
			AuthorityVersion: 4, RosterDigest: firstCycleDigest,
			DirectoryRevision: before,
		}); err != nil {
			t.Fatalf("compact removed source mapping: %v", err)
		}
	}
	secondDigest, ok := sender.rosterDigest(group)
	if !ok || secondDigest == firstCycleDigest {
		t.Fatalf("source compaction did not change roster digest: %x", secondDigest)
	}
	if _, err := oldReceiver.DecodeInbound(testPeerIdentity(oldReceiver, testNode(2)), queuedBeforeCompaction); err != nil {
		t.Fatalf("old receiver rejected queued pre-compaction frame: %v", err)
	}
	for _, registry := range []*StaticRegistry{sender, oldReceiver, updatedReceiver} {
		if err := registry.CompleteRosterHandoff(RosterHandoffProof{
			Group: group, LegacyDigest: firstCycleDigest, CurrentDigest: secondDigest,
			AuthorityVersion: 4, DirectoryRevision: registry.PeerDirectoryRevision(),
		}); err != nil {
			t.Fatalf("complete compaction roster handoff: %v", err)
		}
	}
	secondTarget := testNode(5)
	for _, registry := range []*StaticRegistry{sender, updatedReceiver} {
		intent := dynamicPeerIntent(registry, secondTarget, 5, group, 5, secondDigest)
		intent.Member.ReplicaSetVersion = 4
		if err := registry.EnrollMember(
			intent,
			EnrollmentVerifierFunc(allowEnrollment),
		); err != nil {
			t.Fatalf("second EnrollMember: %v", err)
		}
	}
	frame, _, err := sender.EncodeOutbound(nil, raftmember.OutboundMessage{
		Group: group, From: 2, To: 3, Message: frameTestMessage(pb.MsgHeartbeat, 2, 3),
	})
	if err != nil {
		t.Fatalf("old-member traffic after second enrollment: %v", err)
	}
	header, _, err := parseFrame(frame)
	if err != nil || header.roster != secondDigest {
		t.Fatalf("second-cycle outbound roster = %x, parse error %v; want adjacent %x", header.roster, err, secondDigest)
	}
	// The receiver that stopped after source compaction is still on secondDigest
	// and accepts the queued frame during the staggered update.
	if _, err := oldReceiver.DecodeInbound(testPeerIdentity(oldReceiver, testNode(2)), frame); err != nil {
		t.Fatalf("old receiver rejected queued frame: %v", err)
	}
	// The second-cycle receiver has a new current digest and one adjacent legacy
	// cut (secondDigest), so the same preencoded frame remains admissible.
	if _, err := updatedReceiver.DecodeInbound(testPeerIdentity(updatedReceiver, testNode(2)), frame); err != nil {
		t.Fatalf("updated receiver rejected queued frame: %v", err)
	}
	if _, _, err := sender.EncodeOutbound(nil, raftmember.OutboundMessage{
		Group: group, From: 2, To: 5, Message: frameTestMessage(pb.MsgHeartbeat, 2, 5),
	}); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("ungranted second target traffic error = %v, want ErrUnauthorized", err)
	}
}

func replacementTestGrant(
	group raftmember.GroupKey,
	source, target uint64,
	targetNode NodeID,
	initial [3]uint64,
) membershipgrant.Grant {
	grant := membershipgrant.Grant{
		Group: group, TransitionID: [16]byte{byte(target)}, MetadataEpoch: 7,
		CatalogGeneration: 9, InitialReplicaSetVersion: 1,
		InitialVoters: initial, InitialDescriptorDigest: [32]byte{2},
		SourceMember: source, TargetMember: target, TargetNode: [16]byte(targetNode),
	}
	var roster [3]membershipgrant.RosterMember
	for index, memberID := range initial {
		roster[index] = membershipgrant.RosterMember{
			Member: memberID, Node: [16]byte(testNode(byte(memberID))),
		}
	}
	grant.InitialRosterDigest = membershipgrant.CertifiedRosterDigest(group, 1, roster)
	return grant
}

func TestEmptyTransportEnrollsAndRetiresBoundedPeer(t *testing.T) {
	group := testGroup(203)
	registry, err := NewEmptyRegistry(testNode(1), TrustDomain{
		ClusterID: group.ClusterID, ClusterIncarnation: group.ClusterIncarnation,
	}, Limits{MaxGroups: 1, MaxMembers: 4, MaxPeers: 4})
	if err != nil {
		t.Fatal(err)
	}
	options := OrdinaryTransportOptions{
		Registry: registry,
		Dialer: ordinaryDialFunc(func(ctx context.Context, _ NodeID) (PeerConnection, error) {
			select {
			case <-ctx.Done():
				return nil, context.Cause(ctx)
			default:
				return nil, io.ErrClosedPipe
			}
		}),
		Queue:    QueueLimits{PerPeerFrames: 4, PerPeerBytes: 1 << 16, GlobalFrames: 8, GlobalBytes: 1 << 17},
		Coalesce: CoalesceLimits{MaxFrames: 2, MaxBytes: 1 << 16, RetainedBytes: DefaultRetainedFrameBytes},
		Wait:     WaitWithTimer, Backoff: func(uint32) time.Duration { return time.Millisecond },
		MaxReconnectDelay: time.Second,
		WriteDeadline:     func() time.Time { return time.Now().Add(time.Second) },
	}
	transport, err := NewOrdinaryTransport(options)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- transport.Run(ctx) }()
	transportTestEventually(t, transport.Running)
	remote := testNode(2)
	intent := dynamicPeerIntent(registry, remote, 2, raftmember.GroupKey{}, 0, [sha256.Size]byte{})
	if err := transport.EnrollPeer(intent, EnrollmentVerifierFunc(allowEnrollment)); err != nil {
		t.Fatalf("transport EnrollPeer: %v", err)
	}
	if _, err := transport.Stats(remote); err != nil {
		t.Fatalf("Stats after dynamic queue install: %v", err)
	}
	if err := transport.RetirePeer(remote); err != nil {
		t.Fatalf("RetirePeer: %v", err)
	}
	if _, err := transport.Stats(remote); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("Stats after retirement = %v, want ErrNodeNotFound", err)
	}
	if peer, err := registry.PhysicalPeer(remote); err != nil || peer.State != PeerRetired {
		t.Fatalf("retired peer = %+v, %v", peer, err)
	}
	cancel()
	_ = transport.Close()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("empty transport did not stop")
	}
}

func TestStaticPeerBindingPreservesAuthenticatedManifestIdentities(t *testing.T) {
	fixture := newTransportTestFixture(t)
	domain := fixture.registry.TrustDomain()
	identity := PeerIdentity{TrustDomain: domain, Node: fixture.remote[0].Node}
	key := sha256.Sum256([]byte("authenticated-static-leaf"))
	if err := fixture.registry.VerifyPeerBinding(identity, key); err != nil {
		t.Fatalf("static manifest identity rejected: %v", err)
	}
	identity.Node = testNode(99)
	if err := fixture.registry.VerifyPeerBinding(identity, key); !errors.Is(err, ErrPeerUnauthorized) {
		t.Fatalf("unknown static identity: %v", err)
	}
	identity.Node = fixture.remote[0].Node
	identity.TrustDomain.ClusterIncarnation[0] ^= 1
	if err := fixture.registry.VerifyPeerBinding(identity, key); !errors.Is(err, ErrPeerUnauthorized) {
		t.Fatalf("wrong trust domain: %v", err)
	}
}
