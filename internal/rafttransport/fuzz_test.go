package rafttransport

import (
	"bytes"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmember"
	pb "go.etcd.io/raft/v3/raftpb"
)

func FuzzDecodeInbound(f *testing.F) {
	group := testGroup(31)
	sender, receiver, from, to := frameTestRegistries(f, 2, group)
	for _, messageType := range []pb.MessageType{
		pb.MsgApp,
		pb.MsgAppResp,
		pb.MsgVote,
		pb.MsgVoteResp,
		pb.MsgHeartbeat,
		pb.MsgHeartbeatResp,
		pb.MsgPreVote,
		pb.MsgPreVoteResp,
	} {
		f.Add(frameTestEncode(f, sender, group, frameTestMessage(messageType, from, to)))
	}
	f.Add([]byte(nil))
	f.Add([]byte("VDRF"))
	f.Add(wireBytes(nil, 14, []byte{0x80}))

	f.Fuzz(func(t *testing.T, frame []byte) {
		inbound, err := receiver.DecodeInbound(testPeerIdentity(receiver, sender.LocalNode()), frame)
		if err != nil {
			return
		}
		if inbound.Group != group || inbound.Message == nil ||
			inbound.Message.GetFrom() != from || inbound.Message.GetTo() != to {
			t.Fatalf("decoder admitted unexpected route: group=%+v message=%v", inbound.Group, inbound.Message)
		}
		reencoded, destination, err := sender.EncodeOutbound(nil, raftmember.OutboundMessage{
			Group:   inbound.Group,
			From:    inbound.Message.GetFrom(),
			To:      inbound.Message.GetTo(),
			Message: inbound.Message,
		})
		if err != nil {
			t.Fatalf("accepted frame cannot be re-encoded: %v", err)
		}
		if destination != receiver.LocalNode() || !bytes.Equal(reencoded, frame) {
			t.Fatalf("accepted frame is not the unique canonical encoding")
		}
	})
}

func FuzzPreflightOrdinaryPayload(f *testing.F) {
	for _, messageType := range []pb.MessageType{pb.MsgApp, pb.MsgHeartbeat, pb.MsgVote} {
		frame := frameTestMessage(messageType, 12, 11)
		encoded := frameTestEncode(f, mustFuzzSender(f), testGroup(32), frame)
		f.Add(bytes.Clone(encoded[FrameHeaderBytes:]))
	}
	f.Add([]byte(nil))
	f.Add([]byte{0x80})
	f.Add(wireBytes(nil, 9, []byte{0x80}))
	f.Add(wireBytes(nil, 14, []byte{0x80}))

	f.Fuzz(func(t *testing.T, payload []byte) {
		_ = preflightOrdinaryPayload(payload)
	})
}

func mustFuzzSender(f *testing.F) *StaticRegistry {
	f.Helper()
	group := testGroup(32)
	members := []Member{
		{Group: group, ReplicaSetVersion: 1, MemberID: 11, Node: testNode(1), Role: MemberVoter},
		{Group: group, ReplicaSetVersion: 1, MemberID: 12, Node: testNode(2), Role: MemberVoter},
	}
	registry, err := NewStaticRegistry(testNode(2), members, Limits{MaxGroups: 1, MaxMembers: 2})
	if err != nil {
		f.Fatalf("sender registry: %v", err)
	}
	return registry
}
