package rafttransport

import (
	"encoding/binary"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestProspectiveSameGrantSequenceCatchesUpDisconnectedSurvivor(t *testing.T) {
	group := testGroup(109)
	grant := authorityTestGrant(group)
	members := []Member{
		{Group: group, ReplicaSetVersion: 5, MemberID: 1, Node: testNode(1), Role: MemberVoter},
		{Group: group, ReplicaSetVersion: 5, MemberID: 2, Node: testNode(2), Role: MemberVoter},
		{Group: group, ReplicaSetVersion: 5, MemberID: 3, Node: testNode(3), Role: MemberEnrolled},
		{Group: group, ReplicaSetVersion: 5, MemberID: 4, Node: testNode(4), Role: MemberVoter},
	}
	open := func(local byte) *StaticRegistry {
		registry, err := NewStaticRegistry(testNode(local), members, Limits{MaxGroups: 1, MaxMembers: 4})
		if err != nil {
			t.Fatal(err)
		}
		if err := registry.InstallTransitionGrant(grant); err != nil {
			t.Fatal(err)
		}
		return registry
	}
	leader, follower := open(2), open(4)
	entries := []*pb.Entry{
		replayConfigurationEntry(t, grant, 6, pb.ConfChangeAddLearnerNode, 3),
		replayConfigurationEntry(t, grant, 7, pb.ConfChangeAddNode, 3),
		replayConfigurationEntry(t, grant, 8, pb.ConfChangeRemoveNode, 1),
	}
	for index, conf := range []*pb.ConfState{
		{Voters: []uint64{1, 2, 4}, Learners: []uint64{3}},
		{Voters: []uint64{1, 2, 3, 4}},
		{Voters: []uint64{2, 3, 4}},
	} {
		if err := leader.PublishCommittedAuthority(group, uint64(index+6), conf); err != nil {
			t.Fatal(err)
		}
	}
	store := raft.NewMemoryStorage()
	if err := store.ApplySnapshot(&pb.Snapshot{Metadata: &pb.SnapshotMetadata{
		Index: proto.Uint64(5), Term: proto.Uint64(5), ConfState: &pb.ConfState{Voters: []uint64{1, 2, 4}},
	}}); err != nil {
		t.Fatal(err)
	}
	if err := store.Append(entries); err != nil {
		t.Fatal(err)
	}
	if err := leader.PublishCommittedAuthorityWithReplay(group, 8,
		&pb.ConfState{Voters: []uint64{2, 3, 4}}, &retainedConfigurationLog{store: store}); err != nil {
		t.Fatal(err)
	}
	message := &pb.Message{Type: pb.MsgApp.Enum(), From: proto.Uint64(2), To: proto.Uint64(4),
		Term: proto.Uint64(5), LogTerm: proto.Uint64(5), Index: proto.Uint64(5), Commit: proto.Uint64(8), Entries: entries}
	frame, _, err := leader.EncodeOutbound(nil, raftmember.OutboundMessage{Group: group, From: 2, To: 4, Message: message})
	if err != nil {
		t.Fatal(err)
	}
	inbound, err := follower.DecodeInbound(testPeerIdentity(follower, testNode(2)), frame)
	if err != nil || !proto.Equal(inbound.Message, message) {
		t.Fatalf("disconnected survivor cannot receive exact granted sequence: message=%+v err=%v", inbound.Message, err)
	}
	view, _ := follower.currentAuthority(group)
	if view.version != 5 || view.roles[1] != MemberVoter || view.roles[3] != MemberEnrolled || view.prospective != nil {
		t.Fatalf("request-local sequence published uncommitted roles: %+v", view)
	}
	negative := []struct {
		name    string
		version uint64
		mutate  func(*pb.Message)
	}{
		{"reordered", 8, func(message *pb.Message) {
			message.Entries[0].Data, message.Entries[1].Data = message.Entries[1].Data, message.Entries[0].Data
		}},
		{"duplicate index", 8, func(message *pb.Message) { message.Entries[1].Index = proto.Uint64(6) }},
		{"foreign grant", 8, func(message *pb.Message) { message.Entries[1].Data[len(message.Entries[1].Data)-1] ^= 1 }},
		{"wrong target", 8, func(message *pb.Message) {
			message.Entries[0] = replayConfigurationEntry(t, grant, 6, pb.ConfChangeAddLearnerNode, 4)
		}},
		{"wrong source", 8, func(message *pb.Message) {
			message.Entries[2] = replayConfigurationEntry(t, grant, 8, pb.ConfChangeRemoveNode, 2)
		}},
		{"fourth change", 9, func(message *pb.Message) {
			message.Commit = proto.Uint64(9)
			message.Entries = append(message.Entries, replayConfigurationEntry(t, grant, 9, pb.ConfChangeRemoveNode, 1))
		}},
		{"uncommitted final cut", 8, func(message *pb.Message) { message.Commit = proto.Uint64(7) }},
		{"header beyond final cut", 9, func(message *pb.Message) { message.Commit = proto.Uint64(9) }},
		{"removed sender", 8, func(message *pb.Message) { message.From = proto.Uint64(1) }},
		{"vote cannot project roles", 8, func(message *pb.Message) { message.Type = pb.MsgVote.Enum(); message.Entries = nil }},
	}
	for _, test := range negative {
		t.Run(test.name, func(t *testing.T) {
			changed := proto.Clone(message).(*pb.Message)
			test.mutate(changed)
			forged := frameTestReplacePayload(t, frame, changed)
			binary.BigEndian.PutUint64(forged[112:120], test.version)
			binary.BigEndian.PutUint64(forged[120:128], changed.GetFrom())
			if _, err := follower.DecodeInbound(testPeerIdentity(follower, testNode(byte(changed.GetFrom()))), forged); err == nil {
				t.Fatal("invalid prospective sequence accepted")
			}
		})
	}
	if _, err := follower.Role(group, 3); !errors.Is(err, ErrMemberNotFound) {
		t.Fatalf("prospective target acquired a runtime role: %v", err)
	}
	t.Run("compacted RF4 survivor", func(t *testing.T) {
		compactedMembers := append([]Member(nil), members...)
		for index := range compactedMembers {
			compactedMembers[index].ReplicaSetVersion = 7
			compactedMembers[index].Role = MemberVoter
		}
		compacted, err := NewStaticRegistry(testNode(4), compactedMembers, Limits{MaxGroups: 1, MaxMembers: 4})
		if err != nil {
			t.Fatal(err)
		}
		if err := compacted.InstallTransitionGrant(grant); err != nil {
			t.Fatal(err)
		}
		compactedLog := raft.NewMemoryStorage()
		conf := &pb.ConfState{Voters: []uint64{1, 2, 3, 4}}
		if err := compactedLog.ApplySnapshot(&pb.Snapshot{Metadata: &pb.SnapshotMetadata{
			Index: proto.Uint64(7), Term: proto.Uint64(5), ConfState: conf,
		}}); err != nil {
			t.Fatal(err)
		}
		if err := compacted.PublishCommittedAuthorityWithReplay(group, 7, conf,
			&retainedConfigurationLog{store: compactedLog}); err != nil {
			t.Fatal(err)
		}
		inbound, err := compacted.DecodeInbound(testPeerIdentity(compacted, testNode(2)), frame)
		if err != nil || inbound.Message == nil || len(inbound.Message.GetEntries()) != 0 || inbound.Message.GetIndex() != 5 {
			t.Fatalf("compacted survivor did not receive an empty committed-prefix probe: %+v %v", inbound.Message, err)
		}
		current, _ := compacted.currentAuthority(group)
		if current.version != 7 || current.roles[1] != MemberVoter {
			t.Fatal("sanitized prospective probe published removal")
		}
		ack := &pb.Message{Type: pb.MsgAppResp.Enum(), From: proto.Uint64(4), To: proto.Uint64(2),
			Term: proto.Uint64(5), Index: proto.Uint64(7)}
		ackFrame, _, err := compacted.EncodeOutbound(nil, raftmember.OutboundMessage{
			Group: group, From: 4, To: 2, Message: ack,
		})
		if err != nil {
			t.Fatal(err)
		}
		if received, err := leader.DecodeInbound(testPeerIdentity(leader, testNode(4)), ackFrame); err != nil || !proto.Equal(received.Message, ack) {
			t.Fatalf("survivor's committed-prefix ACK cannot advance leader progress: %+v %v", received.Message, err)
		}
		for _, kind := range []pb.MessageType{pb.MsgApp, pb.MsgVote} {
			stale := &pb.Message{Type: kind.Enum(), From: proto.Uint64(4), To: proto.Uint64(2),
				Term: proto.Uint64(5), LogTerm: proto.Uint64(5), Index: proto.Uint64(7)}
			staleFrame := frameTestReplacePayload(t, ackFrame, stale)
			if _, err := leader.DecodeInbound(testPeerIdentity(leader, testNode(4)), staleFrame); err == nil {
				t.Fatalf("retired generation admitted %s instead of only survivor responses", kind)
			}
		}
		removedAck := proto.Clone(ack).(*pb.Message)
		removedAck.From = proto.Uint64(1)
		removedFrame := frameTestReplacePayload(t, ackFrame, removedAck)
		binary.BigEndian.PutUint64(removedFrame[120:128], 1)
		if _, err := leader.DecodeInbound(testPeerIdentity(leader, testNode(1)), removedFrame); err == nil {
			t.Fatal("retired source response regained authority")
		}
		for _, alter := range []func(*pb.Message){
			func(message *pb.Message) { message.Entries[2].Data[len(message.Entries[2].Data)-1] ^= 1 },
			func(message *pb.Message) { message.From = proto.Uint64(1) },
		} {
			changed := proto.Clone(message).(*pb.Message)
			alter(changed)
			forged := frameTestReplacePayload(t, frame, changed)
			binary.BigEndian.PutUint64(forged[120:128], changed.GetFrom())
			if _, err := compacted.DecodeInbound(testPeerIdentity(compacted, testNode(byte(changed.GetFrom()))), forged); err == nil {
				t.Fatal("invalid future grant or removed sender reached committed-prefix shortcut")
			}
		}
	})
}
