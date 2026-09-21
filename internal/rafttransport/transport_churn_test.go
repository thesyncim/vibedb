package rafttransport

import (
	"context"
	"io"
	"net"
	"sync/atomic"
	"testing"

	"github.com/thesyncim/vibedb/internal/membershipgrant"
	"github.com/thesyncim/vibedb/internal/raftmember"
	pb "go.etcd.io/raft/v3/raftpb"
)

func TestOrdinaryTransportRepeatedReplacementReclaimsQueues(t *testing.T) {
	group := testGroup(206)
	voters := [3]uint64{1, 2, 3}
	members := enrollmentTestMembers(group)
	registry, err := NewStaticRegistry(testNode(1), members, Limits{MaxGroups: 1, MaxMembers: 4, MaxPeers: 16})
	if err != nil {
		t.Fatal(err)
	}
	fixture := transportTestFixture{group: group, registry: registry, local: members[0], remote: [2]Member{members[1], members[2]}}
	dialer := ordinaryDialFunc(func(_ context.Context, node NodeID) (PeerConnection, error) {
		left, right := net.Pipe()
		peer, err := registry.PhysicalPeer(node)
		if err != nil {
			return nil, err
		}
		go func() { _, _ = io.Copy(io.Discard, right); _ = right.Close() }()
		return &enrollmentTestConnection{Conn: left, identity: PeerIdentity{TrustDomain: registry.TrustDomain(), Node: node}, key: peer.ServiceKeyDigest, class: TrafficOrdinary}, nil
	})
	options := transportTestOptions(fixture, dialer)
	options.Queue.GlobalFrames = 3 * options.Queue.PerPeerFrames
	options.Wait = WaitWithTimer
	transport, err := NewOrdinaryTransport(options)
	if err != nil {
		t.Fatal(err)
	}
	cancel, done := runTransportTest(t, transport)
	defer stopTransportTest(t, transport, cancel, done)
	var prior membershipgrant.Grant
	version := uint64(1)
	for cycle := range 8 {
		target := uint64(cycle + 4)
		source := voters[1]
		grant := replacementTestGrant(group, source, target, testNode(byte(target)), voters)
		grant.InitialReplicaSetVersion, grant.CatalogGeneration = version, uint64(cycle+1)
		var roster [3]membershipgrant.RosterMember
		for i, member := range voters {
			roster[i] = membershipgrant.RosterMember{Member: member, Node: [16]byte(testNode(byte(member)))}
			members[i] = Member{Group: group, ReplicaSetVersion: version, MemberID: member, Node: testNode(byte(member)), Role: MemberVoter}
		}
		grant.InitialRosterDigest = membershipgrant.CertifiedRosterDigest(group, version, roster)
		digest, _ := StableRosterDigest(members)
		intent := dynamicPeerIntent(registry, testNode(byte(target)), byte(target), group, target, digest)
		intent.Grant, intent.Digest, intent.Member.ReplicaSetVersion = grant, grant.Digest(), version
		if err := transport.EnrollMember(intent, EnrollmentVerifierFunc(allowEnrollment)); err != nil {
			t.Fatalf("cycle %d enrollment: %v", cycle+1, err)
		}
		if prior.Valid() {
			err = registry.ReplaceTransitionGrantWithCommit(prior, grant, nil)
		} else {
			err = registry.InstallTransitionGrant(grant)
		}
		if err != nil {
			t.Fatal(err)
		}
		packet := raftmember.OutboundMessage{Group: group, From: 1, To: target, Message: frameTestMessage(pb.MsgHeartbeat, 1, target)}
		if err := transport.Send(packet); err != nil {
			t.Fatalf("cycle %d first append to installed target: %v", cycle+1, err)
		}
		transportTestEventually(t, func() bool {
			transport.mu.Lock()
			defer transport.mu.Unlock()
			peer := transport.byNode[testNode(byte(target))]
			return peer != nil && peer.sentFrames.Load() == 1 && transport.globalFrames == 0 && transport.globalBytes == 0
		})
		for offset, conf := range []*pb.ConfState{
			{Voters: voters[:], Learners: []uint64{target}},
			{Voters: []uint64{voters[0], voters[1], voters[2], target}},
			{Voters: []uint64{voters[0], voters[2], target}},
		} {
			if err := registry.PublishCommittedAuthority(group, version+uint64(offset)+1, conf); err != nil {
				t.Fatal(err)
			}
		}
		transport.mu.Lock()
		queues := len(transport.peers)
		transport.mu.Unlock()
		if queues > 3 {
			t.Fatalf("cycle %d retained %d queues beyond unchanged bound", cycle+1, queues)
		}
		if !registry.IsPeerEnrolled(testNode(byte(source))) {
			t.Fatal("queue reclamation changed durable physical identity")
		}
		prior, version = grant, version+3
		voters = [3]uint64{voters[0], voters[2], target}
	}
}

func TestQueueReclamationJoinsWorkerAndEncoderBeforeReuse(t *testing.T) {
	group := testGroup(207)
	members := append(enrollmentTestMembers(group), Member{Group: group, ReplicaSetVersion: 1, MemberID: 4, Node: testNode(4), Role: MemberEnrolled})
	registry, err := NewStaticRegistry(testNode(1), members, Limits{MaxGroups: 1, MaxMembers: 4})
	if err != nil {
		t.Fatal(err)
	}
	grant := replacementTestGrant(group, 2, 4, testNode(4), [3]uint64{1, 2, 3})
	if err := registry.InstallTransitionGrant(grant); err != nil {
		t.Fatal(err)
	}
	fixture := transportTestFixture{group: group, registry: registry, local: members[0], remote: [2]Member{members[1], members[2]}}
	dialer := ordinaryDialFunc(func(ctx context.Context, _ NodeID) (PeerConnection, error) {
		<-ctx.Done()
		return nil, context.Cause(ctx)
	})
	options := transportTestOptions(fixture, dialer)
	options.Wait = WaitWithTimer
	transport, err := NewOrdinaryTransport(options)
	if err != nil {
		t.Fatal(err)
	}
	cancel, done := runTransportTest(t, transport)
	defer stopTransportTest(t, transport, cancel, done)
	if err := transport.Send(fixture.outbound(0, 1)); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var first atomic.Bool
	transport.beforeEncode = func() {
		if first.CompareAndSwap(false, true) {
			close(entered)
			<-release
		}
	}
	oldSend := make(chan error, 1)
	go func() { oldSend <- transport.Send(fixture.outbound(0, 2)) }()
	<-entered
	transport.mu.Lock()
	old := transport.byNode[testNode(2)]
	if old.count != 1 || old.reservedFrames != 1 {
		t.Fatal("missing queued and encoder-owned frames")
	}
	transport.mu.Unlock()
	publishQueueTestReplacement(t, registry, group)
	newSend := make(chan error, 1)
	go func() {
		newSend <- transport.Send(raftmember.OutboundMessage{Group: group, From: 1, To: 4, Message: frameTestMessage(pb.MsgHeartbeat, 1, 4)})
	}()
	transportTestEventually(t, func() bool {
		transport.mu.Lock()
		defer transport.mu.Unlock()
		return old.retiring
	})
	transport.mu.Lock()
	if transport.byNode[testNode(2)] != old || transport.byNode[testNode(4)] != nil {
		t.Fatal("reused capacity before the old encoder released its handle")
	}
	transport.mu.Unlock()
	close(release)
	if err := <-oldSend; err == nil {
		t.Fatal("removed destination published its delayed encoded frame")
	}
	if err := <-newSend; err != nil {
		t.Fatal(err)
	}
	select {
	case <-old.done:
	default:
		t.Fatal("old worker survived replacement")
	}
	transport.mu.Lock()
	if transport.byNode[testNode(2)] != nil || old.count != 0 || old.reservedFrames != 0 || old.bytes != 0 || old.reservedBytes != 0 {
		t.Fatal("old queue retained frames or reservations")
	}
	current := transport.byNode[testNode(4)]
	if transport.globalFrames != 1 || transport.globalBytes != current.bytes || current.count != 1 || len(transport.peers) != 2 {
		t.Fatal("replacement escaped the shared queue accounting")
	}
	transport.mu.Unlock()
}

func TestQueueReclamationKeepsParticipantsOfOtherGroups(t *testing.T) {
	group := testGroup(208)
	other := group
	other.GroupID[0]++
	members := append(enrollmentTestMembers(group), Member{Group: group, ReplicaSetVersion: 1, MemberID: 4, Node: testNode(4), Role: MemberEnrolled})
	registry, err := NewStaticRegistry(testNode(1), members, Limits{MaxGroups: 2, MaxMembers: 8})
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.InstallGroup(enrollmentTestMembers(other), func(publish func()) error { publish(); return nil }); err != nil {
		t.Fatal(err)
	}
	if err := registry.InstallTransitionGrant(replacementTestGrant(group, 2, 4, testNode(4), [3]uint64{1, 2, 3})); err != nil {
		t.Fatal(err)
	}
	fixture := transportTestFixture{group: group, registry: registry, local: members[0], remote: [2]Member{members[1], members[2]}}
	options := transportTestOptions(fixture, ordinaryDialFunc(func(context.Context, NodeID) (PeerConnection, error) { return nil, io.ErrClosedPipe }))
	transport, err := NewOrdinaryTransport(options)
	if err != nil {
		t.Fatal(err)
	}
	defer transport.Close()
	publishQueueTestReplacement(t, registry, group)
	transport.reclaimUnusedPeers(testNode(4))
	if transport.byNode[testNode(2)] == nil {
		t.Fatal("reclaimed a voter still used by another group")
	}
	if err := registry.RemoveGroup(other, func(withdraw func()) error { withdraw(); return nil }); err != nil {
		t.Fatal(err)
	}
	transport.reclaimUnusedPeers(testNode(4))
	if transport.byNode[testNode(2)] != nil {
		t.Fatal("completed historical grant retained an obsolete queue")
	}

	// Reopening transport must not warm historical physical enrollments. The
	// durable role view above is already recovered before the first send.
	options.Peers = nil
	reopened, err := NewOrdinaryTransport(options)
	if err != nil {
		t.Fatal(err)
	}
	if len(reopened.peers) != 0 {
		t.Fatal("cold start retained historical queues")
	}
	cancel, done := runTransportTest(t, reopened)
	defer stopTransportTest(t, reopened, cancel, done)
	if err := reopened.Send(raftmember.OutboundMessage{Group: group, From: 1, To: 4, Message: frameTestMessage(pb.MsgHeartbeat, 1, 4)}); err != nil {
		t.Fatal(err)
	}
	reopened.mu.Lock()
	if len(reopened.peers) != 1 || reopened.byNode[testNode(4)] == nil || reopened.byNode[testNode(2)] != nil {
		t.Fatal("cold queue reconstruction did not follow recovered membership")
	}
	reopened.mu.Unlock()
}

func publishQueueTestReplacement(t *testing.T, registry *StaticRegistry, group raftmember.GroupKey) {
	t.Helper()
	for index, conf := range []*pb.ConfState{
		{Voters: []uint64{1, 2, 3}, Learners: []uint64{4}},
		{Voters: []uint64{1, 2, 3, 4}},
		{Voters: []uint64{1, 3, 4}},
	} {
		if err := registry.PublishCommittedAuthority(group, uint64(index+2), conf); err != nil {
			t.Fatal(err)
		}
	}
}
