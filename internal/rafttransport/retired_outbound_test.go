package rafttransport

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmember"
	pb "go.etcd.io/raft/v3/raftpb"
)

func TestTransportDiscardsOnlyCommittedRemovedSource(t *testing.T) {
	group := testGroup(31)
	members := []Member{
		{Group: group, ReplicaSetVersion: 5, MemberID: 1, Node: testNode(1), Role: MemberVoter},
		{Group: group, ReplicaSetVersion: 5, MemberID: 2, Node: testNode(2), Role: MemberVoter},
		{Group: group, ReplicaSetVersion: 5, MemberID: 3, Node: testNode(3), Role: MemberEnrolled},
		{Group: group, ReplicaSetVersion: 5, MemberID: 4, Node: testNode(4), Role: MemberVoter},
	}
	registry, err := NewStaticRegistry(testNode(1), members, Limits{MaxGroups: 1, MaxMembers: 4})
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.InstallTransitionGrant(authorityTestGrant(group)); err != nil {
		t.Fatal(err)
	}
	fixture := transportTestFixture{registry: registry, local: members[0], remote: [2]Member{members[1], members[2]}}
	transport, err := NewOrdinaryTransport(transportTestOptions(fixture,
		ordinaryDialFunc(func(context.Context, NodeID) (PeerConnection, error) {
			return nil, errors.New("unexpected dial")
		})))
	if err != nil {
		t.Fatal(err)
	}
	transport.state.Store(transportRunning)
	defer transport.Close()
	packet := func(kind pb.MessageType) raftmember.OutboundMessage {
		message := frameTestMessage(kind, 1, 2)
		if kind == pb.MsgTimeoutNow {
			message = frameTimeoutNow(1, 2, 5)
		}
		return raftmember.OutboundMessage{Group: group, From: 1, To: 2, Message: message}
	}
	pending := packet(pb.MsgHeartbeatResp)
	if _, _, err := registry.EncodeOutbound(nil, pending); err != nil {
		t.Fatal(err)
	}
	for _, change := range []struct {
		version uint64
		conf    *pb.ConfState
	}{
		{6, &pb.ConfState{Voters: []uint64{1, 2, 4}, Learners: []uint64{3}}},
		{7, &pb.ConfState{Voters: []uint64{1, 2, 3, 4}}},
		{8, &pb.ConfState{Voters: []uint64{2, 3, 4}}},
	} {
		if err := registry.PublishCommittedAuthority(group, change.version, change.conf); err != nil {
			t.Fatal(err)
		}
	}
	for _, kind := range []pb.MessageType{pb.MsgHeartbeat, pb.MsgApp, pb.MsgAppResp, pb.MsgHeartbeatResp, pb.MsgVote, pb.MsgVoteResp, pb.MsgPreVote, pb.MsgPreVoteResp, pb.MsgTimeoutNow} {
		outbound := packet(kind)
		before := []byte("unchanged")
		frame, destination, err := registry.EncodeOutbound(before, outbound)
		if !errors.Is(err, ErrUnauthorized) || !errors.Is(err, errRetiredOutboundSource) ||
			!bytes.Equal(frame, before) || destination != (NodeID{}) {
			t.Fatalf("removed source was framed for %s: frame=%q destination=%v err=%v", kind, frame, destination, err)
		}
		if err := transport.Send(outbound); err != nil {
			t.Fatalf("removed source stopped transport on %s: %v", kind, err)
		}
	}
	if transport.globalFrames != 0 || transport.globalBytes != 0 || transport.activeSends != 0 {
		t.Fatal("removed source retained queue ownership")
	}
	if owned, _, _, _ := transport.frames.stats(); owned != 0 {
		t.Fatalf("removed source allocated %d frames", owned)
	}
	malformed := pending
	malformed.From = 4
	if err := transport.Send(malformed); !errors.Is(err, ErrInvalidFrame) {
		t.Fatalf("removed source hid mismatched identity: %v", err)
	}
	badConfig := packet(pb.MsgApp)
	badConfig.Message = authorizedConfigurationMessage(t, group, pb.ConfChangeRemoveNode, 1, 1, 2)
	badConfig.Message.Entries[0].Data[len(badConfig.Message.Entries[0].Data)-1] ^= 1
	if err := transport.Send(badConfig); !errors.Is(err, ErrUnauthorized) || errors.Is(err, errRetiredOutboundSource) {
		t.Fatalf("removed source hid foreign configuration: %v", err)
	}
	view, _ := registry.currentAuthority(group)
	withoutRemoval := *view
	withoutRemoval.retiredVersion = 0
	if retiredOutboundSource(&withoutRemoval, pending.Message) {
		t.Fatal("discarded a source without committed removal")
	}
}

func TestTransportDiscardsOnlyCommittedRemovedDestination(t *testing.T) {
	group := testGroup(31)
	members := []Member{
		{Group: group, ReplicaSetVersion: 5, MemberID: 1, Node: testNode(1), Role: MemberVoter},
		{Group: group, ReplicaSetVersion: 5, MemberID: 2, Node: testNode(2), Role: MemberVoter},
		{Group: group, ReplicaSetVersion: 5, MemberID: 3, Node: testNode(3), Role: MemberEnrolled},
		{Group: group, ReplicaSetVersion: 5, MemberID: 4, Node: testNode(4), Role: MemberVoter},
	}
	registry, err := NewStaticRegistry(testNode(2), members, Limits{MaxGroups: 1, MaxMembers: 4})
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.InstallTransitionGrant(authorityTestGrant(group)); err != nil {
		t.Fatal(err)
	}
	outbound := func(kind pb.MessageType, from, to uint64) raftmember.OutboundMessage {
		message := frameTestMessage(kind, from, to)
		if kind == pb.MsgTimeoutNow {
			message = &pb.Message{Type: message.Type, From: message.From, To: message.To, Term: message.Term}
		}
		return raftmember.OutboundMessage{Group: group, From: from, To: to, Message: message}
	}
	// Raft created this message while voter 1 was still committed.
	pending := outbound(pb.MsgHeartbeat, 2, 1)
	if _, _, err := registry.EncodeOutbound(nil, pending); err != nil {
		t.Fatal(err)
	}
	fixture := transportTestFixture{registry: registry, local: members[1], remote: [2]Member{members[0], members[2]}}
	transport, err := NewOrdinaryTransport(transportTestOptions(fixture,
		ordinaryDialFunc(func(context.Context, NodeID) (PeerConnection, error) {
			return nil, errors.New("unexpected dial")
		})))
	if err != nil {
		t.Fatal(err)
	}
	// No network workers are needed: discarded packets must take no queue or
	// buffer ownership, and rejected packets must remain errors.
	transport.state.Store(transportRunning)
	defer transport.Close()
	if err := transport.Send(outbound(pb.MsgHeartbeat, 2, 3)); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("uncommitted destination=%v", err)
	}
	for _, next := range []struct {
		version uint64
		conf    *pb.ConfState
	}{
		{6, &pb.ConfState{Voters: []uint64{1, 2, 4}, Learners: []uint64{3}}},
		{7, &pb.ConfState{Voters: []uint64{1, 2, 3, 4}}},
		{8, &pb.ConfState{Voters: []uint64{2, 3, 4}}},
	} {
		if err := registry.PublishCommittedAuthority(group, next.version, next.conf); err != nil {
			t.Fatal(err)
		}
	}
	for _, kind := range []pb.MessageType{pb.MsgHeartbeat, pb.MsgApp, pb.MsgAppResp, pb.MsgHeartbeatResp, pb.MsgVote, pb.MsgVoteResp, pb.MsgPreVote, pb.MsgPreVoteResp, pb.MsgTimeoutNow} {
		packet := outbound(kind, 2, 1)
		before := []byte("unchanged")
		frame, destination, err := registry.EncodeOutbound(before, packet)
		if !errors.Is(err, ErrUnauthorized) || !errors.Is(err, errRetiredOutboundDestination) ||
			errors.Is(err, ErrRetiredAuthority) || !bytes.Equal(frame, before) || destination != (NodeID{}) {
			t.Fatalf("%s frame=%q destination=%v error=%v", kind, frame, destination, err)
		}
		if err := transport.Send(packet); err != nil {
			t.Fatalf("discard %s: %v", kind, err)
		}
	}
	if transport.globalFrames != 0 || transport.globalBytes != 0 || transport.activeSends != 0 {
		t.Fatalf("discard retained queue ownership: frames=%d bytes=%d sends=%d", transport.globalFrames, transport.globalBytes, transport.activeSends)
	}
	if owned, _, _, _ := transport.frames.stats(); owned != 0 {
		t.Fatalf("discard allocated %d frames", owned)
	}
	malformed := pending
	malformed.From = 4 // A retired destination must not hide an identity mismatch.
	if err := transport.Send(malformed); !errors.Is(err, ErrInvalidFrame) {
		t.Fatalf("mismatched source=%v", err)
	}
	if err := transport.Send(outbound(pb.MsgHeartbeat, 2, 99)); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("unknown destination=%v", err)
	}
	badConfig := outbound(pb.MsgApp, 2, 1)
	badConfig.Message = authorizedConfigurationMessage(t, group, pb.ConfChangeRemoveNode, 1, 2, 1)
	badConfig.Message.Entries[0].Data[len(badConfig.Message.Entries[0].Data)-1] ^= 1
	if err := transport.Send(badConfig); !errors.Is(err, ErrUnauthorized) || errors.Is(err, errRetiredOutboundDestination) {
		t.Fatalf("foreign configuration=%v", err)
	}
	// The source role gate remains fail-closed even with proof of destination removal.
	view, _ := registry.currentAuthority(group)
	copy := *view
	copy.roles = map[uint64]MemberRole{2: MemberLearner, 3: MemberVoter, 4: MemberVoter}
	if retiredOutboundDestination(&copy, pending.Message) {
		t.Fatal("learner originated leader traffic")
	}
	if !retiredOutboundDestination(&copy, outbound(pb.MsgAppResp, 2, 1).Message) {
		t.Fatal("valid learner response not discarded")
	}
	delete(copy.roles, 2)
	if retiredOutboundDestination(&copy, outbound(pb.MsgAppResp, 2, 1).Message) {
		t.Fatal("removed source classified as stale destination")
	}
}
