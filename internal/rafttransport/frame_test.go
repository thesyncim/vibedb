package rafttransport

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

const (
	frameTestGroupOffset  = 8
	frameTestRosterOffset = frameTestGroupOffset + 72
	frameTestFromOffset   = FrameHeaderBytes - 20
	frameTestToOffset     = FrameHeaderBytes - 12
	frameTestLengthOffset = FrameHeaderBytes - 4
)

func TestFrameCanonicalRoundTripRF2RF3(t *testing.T) {
	types := []pb.MessageType{
		pb.MsgApp,
		pb.MsgAppResp,
		pb.MsgVote,
		pb.MsgVoteResp,
		pb.MsgHeartbeat,
		pb.MsgHeartbeatResp,
		pb.MsgPreVote,
		pb.MsgPreVoteResp,
		pb.MsgTimeoutNow,
	}
	for _, replicas := range []int{2, 3} {
		group := testGroup(byte(40 + replicas))
		sender, receiver, from, to := frameTestRegistries(t, replicas, group)
		for _, messageType := range types {
			t.Run(messageType.String()+"/RF"+string(rune('0'+replicas)), func(t *testing.T) {
				message := frameTestMessage(messageType, from, to)
				if messageType == pb.MsgTimeoutNow {
					message = frameTimeoutNow(from, to, 5)
				}
				prefix := []byte("preserved-prefix")
				encoded, destination, err := sender.EncodeOutbound(
					append([]byte(nil), prefix...),
					raftmember.OutboundMessage{Group: group, From: from, To: to, Message: message},
				)
				if err != nil {
					t.Fatalf("EncodeOutbound: %v", err)
				}
				if destination != receiver.LocalNode() {
					t.Fatalf("destination = %x, want receiver %x", destination, receiver.LocalNode())
				}
				if !bytes.Equal(encoded[:len(prefix)], prefix) {
					t.Fatalf("destination prefix changed: %x", encoded[:len(prefix)])
				}
				frame := encoded[len(prefix):]
				if len(frame) > MaxFrameBytes {
					t.Fatalf("frame bytes = %d, max %d", len(frame), MaxFrameBytes)
				}

				encodedAgain, destinationAgain, err := sender.EncodeOutbound(
					nil,
					raftmember.OutboundMessage{Group: group, From: from, To: to, Message: message},
				)
				if err != nil {
					t.Fatalf("second EncodeOutbound: %v", err)
				}
				if destinationAgain != destination || !bytes.Equal(encodedAgain, frame) {
					t.Fatalf("encoding is not deterministic")
				}

				inbound, err := receiver.DecodeInbound(testPeerIdentity(receiver, sender.LocalNode()), frame)
				if err != nil {
					t.Fatalf("DecodeInbound: %v", err)
				}
				if inbound.Group != group {
					t.Fatalf("group = %+v, want %+v", inbound.Group, group)
				}
				if !proto.Equal(inbound.Message, message) {
					t.Fatalf("message = %v, want %v", inbound.Message, message)
				}
			})
		}
	}
}

func TestFrameGoldenHeartbeat(t *testing.T) {
	group := testGroup(7)
	sender, receiver, from, to := frameTestRegistries(t, 2, group)
	message := frameTestMessage(pb.MsgHeartbeat, from, to)
	frame, destination, err := sender.EncodeOutbound(nil, raftmember.OutboundMessage{
		Group: group, From: from, To: to, Message: message,
	})
	if err != nil {
		t.Fatalf("EncodeOutbound: %v", err)
	}
	if destination != receiver.LocalNode() {
		t.Fatalf("destination = %x, want %x", destination, receiver.LocalNode())
	}
	const golden = "5644524600010100070100000000000000000000000000000702000000000000000000000000000000000000000000070703000000000000000000000000000007040000000000000000000000000000e189d5c58d02f5c295e58fc50c9e72272a3ed5fe4b6c03628814a0f57b73d63f000000000000000c000000000000000b000000130808100b180c20052804300740076203637478"
	if got := hex.EncodeToString(frame); got != golden {
		t.Fatalf("golden frame changed:\n got %s\nwant %s", got, golden)
	}
}

func TestTimeoutNowFrameRejectsWrongPeerAndNoncanonicalPayload(t *testing.T) {
	group := testGroup(8)
	sender, receiver, from, to := frameTestRegistries(t, 3, group)
	frame := frameTestEncode(t, sender, group, frameTimeoutNow(from, to, 5))
	if _, err := receiver.DecodeInbound(testPeerIdentity(receiver, testNode(2)), frame); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("wrong authenticated node = %v, want ErrUnauthorized", err)
	}

	payload := bytes.Clone(frame[FrameHeaderBytes:])
	tests := []struct {
		name  string
		frame []byte
	}{
		{name: "truncated", frame: bytes.Clone(frame[:len(frame)-1])},
		{name: "trailing", frame: append(bytes.Clone(frame), 0)},
		{name: "duplicate term", frame: frameTestReplaceRawPayload(frame, wireVarint(payload, 4, 5))},
		{name: "unexpected index", frame: frameTestReplaceRawPayload(frame, wireVarint(payload, 6, 0))},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := receiver.DecodeInbound(testPeerIdentity(receiver, sender.LocalNode()), test.frame); !errors.Is(err, ErrInvalidFrame) {
				t.Fatalf("error = %v, want ErrInvalidFrame", err)
			}
		})
	}
}

func TestFrameRejectsCrossDomainPeerAndOutboundGroup(t *testing.T) {
	group := testGroup(73)
	sender, receiver, from, to := frameTestRegistries(t, 2, group)
	outbound := raftmember.OutboundMessage{
		Group: group, From: from, To: to,
		Message: frameTestMessage(pb.MsgHeartbeat, from, to),
	}
	frame, _, err := sender.EncodeOutbound(nil, outbound)
	if err != nil {
		t.Fatal(err)
	}
	identity := testPeerIdentity(receiver, sender.LocalNode())
	identity.TrustDomain.ClusterID[0]++
	if _, err := receiver.DecodeInbound(identity, frame); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("cross-domain peer error = %v, want ErrUnauthorized", err)
	}
	outbound.Group.ClusterIncarnation[0]++
	if _, _, err := sender.EncodeOutbound(nil, outbound); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("cross-domain outbound error = %v, want ErrUnauthorized", err)
	}
}

func TestEncodeOutboundRejectsInvalidAuthorityAndShape(t *testing.T) {
	group := testGroup(9)
	sender, _, from, to := frameTestRegistries(t, 3, group)
	valid := frameTestMessage(pb.MsgHeartbeat, from, to)
	prefix := []byte("unchanged")
	tests := []struct {
		name     string
		registry *StaticRegistry
		outbound raftmember.OutboundMessage
		want     error
	}{
		{
			name: "nil registry",
			outbound: raftmember.OutboundMessage{
				Group: group, From: from, To: to, Message: valid,
			},
			want: ErrInvalidFrame,
		},
		{
			name:     "nil message",
			registry: sender,
			outbound: raftmember.OutboundMessage{Group: group, From: from, To: to},
			want:     ErrInvalidFrame,
		},
		{
			name:     "outer source mismatch",
			registry: sender,
			outbound: raftmember.OutboundMessage{Group: group, From: from + 100, To: to, Message: valid},
			want:     ErrInvalidFrame,
		},
		{
			name:     "remote source",
			registry: sender,
			outbound: raftmember.OutboundMessage{Group: group, From: to, To: from, Message: frameTestMessage(pb.MsgHeartbeat, to, from)},
			want:     ErrUnauthorized,
		},
		{
			name:     "unsupported type",
			registry: sender,
			outbound: raftmember.OutboundMessage{Group: group, From: from, To: to, Message: frameBaseMessage(pb.MsgSnap, from, to)},
			want:     ErrUnsupportedFrame,
		},
		{
			name:     "snapshot graph",
			registry: sender,
			outbound: raftmember.OutboundMessage{Group: group, From: from, To: to, Message: func() *pb.Message {
				message := frameTestMessage(pb.MsgHeartbeat, from, to)
				message.Snapshot = &pb.Snapshot{}
				return message
			}()},
			want: ErrUnsupportedFrame,
		},
		{
			name:     "invalid term",
			registry: sender,
			outbound: raftmember.OutboundMessage{Group: group, From: from, To: to, Message: func() *pb.Message {
				message := frameTestMessage(pb.MsgHeartbeat, from, to)
				message.Term = frameU64(0)
				return message
			}()},
			want: ErrInvalidFrame,
		},
		{
			name:     "context bound",
			registry: sender,
			outbound: raftmember.OutboundMessage{Group: group, From: from, To: to, Message: func() *pb.Message {
				message := frameTestMessage(pb.MsgHeartbeat, from, to)
				message.Context = make([]byte, raftmodel.MaxReadContextBytes+1)
				return message
			}()},
			want: ErrFrameTooLarge,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dst := append([]byte(nil), prefix...)
			got, destination, err := test.registry.EncodeOutbound(dst, test.outbound)
			if !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
			if destination != (NodeID{}) {
				t.Fatalf("destination on error = %x, want zero", destination)
			}
			if !bytes.Equal(got, prefix) {
				t.Fatalf("dst changed on error: %x", got)
			}
		})
	}
}

func TestStaticRoleTrafficMatrix(t *testing.T) {
	group := testGroup(10)
	members := []Member{
		{Group: group, ReplicaSetVersion: 1, MemberID: 11, Node: testNode(1), Role: MemberLearner},
		{Group: group, ReplicaSetVersion: 1, MemberID: 12, Node: testNode(2), Role: MemberVoter},
		{Group: group, ReplicaSetVersion: 1, MemberID: 13, Node: testNode(3), Role: MemberVoter},
	}
	limits := Limits{MaxGroups: 1, MaxMembers: len(members)}
	registries := make(map[uint64]*StaticRegistry, len(members))
	for _, member := range members {
		registry, err := NewStaticRegistry(member.Node, members, limits)
		if err != nil {
			t.Fatalf("registry %d: %v", member.MemberID, err)
		}
		registries[member.MemberID] = registry
	}

	allowed := []struct {
		name        string
		messageType pb.MessageType
		from, to    uint64
	}{
		{name: "voter app to learner", messageType: pb.MsgApp, from: 12, to: 11},
		{name: "learner app response to voter", messageType: pb.MsgAppResp, from: 11, to: 12},
		{name: "voter heartbeat to learner", messageType: pb.MsgHeartbeat, from: 12, to: 11},
		{name: "learner heartbeat response to voter", messageType: pb.MsgHeartbeatResp, from: 11, to: 12},
		{name: "voter leader transfer to voter", messageType: pb.MsgTimeoutNow, from: 12, to: 13},
	}
	for _, test := range allowed {
		t.Run("allows "+test.name, func(t *testing.T) {
			message := frameTestMessage(test.messageType, test.from, test.to)
			if test.messageType == pb.MsgTimeoutNow {
				message = frameTimeoutNow(test.from, test.to, 5)
			}
			frame, _, err := registries[test.from].EncodeOutbound(nil, raftmember.OutboundMessage{
				Group: group, From: test.from, To: test.to, Message: message,
			})
			if err != nil {
				t.Fatalf("EncodeOutbound: %v", err)
			}
			if _, err := registries[test.to].DecodeInbound(
				testPeerIdentity(registries[test.to], testNode(byte(test.from-10))), frame,
			); err != nil {
				t.Fatalf("DecodeInbound: %v", err)
			}
		})
	}

	rejected := []struct {
		name        string
		messageType pb.MessageType
		from, to    uint64
	}{
		{name: "learner app", messageType: pb.MsgApp, from: 11, to: 12},
		{name: "learner heartbeat", messageType: pb.MsgHeartbeat, from: 11, to: 12},
		{name: "learner vote request", messageType: pb.MsgVote, from: 11, to: 12},
		{name: "learner vote response", messageType: pb.MsgVoteResp, from: 11, to: 12},
		{name: "learner pre-vote request", messageType: pb.MsgPreVote, from: 11, to: 12},
		{name: "learner pre-vote response", messageType: pb.MsgPreVoteResp, from: 11, to: 12},
		{name: "learner vote target", messageType: pb.MsgVote, from: 12, to: 11},
		{name: "learner vote response target", messageType: pb.MsgVoteResp, from: 12, to: 11},
		{name: "learner leader-transfer source", messageType: pb.MsgTimeoutNow, from: 11, to: 12},
		{name: "learner leader-transfer target", messageType: pb.MsgTimeoutNow, from: 12, to: 11},
	}
	for _, test := range rejected {
		t.Run("rejects "+test.name, func(t *testing.T) {
			message := frameTestMessage(test.messageType, test.from, test.to)
			if test.messageType == pb.MsgTimeoutNow {
				message = frameTimeoutNow(test.from, test.to, 5)
			}
			_, _, err := registries[test.from].EncodeOutbound(nil, raftmember.OutboundMessage{
				Group: group, From: test.from, To: test.to, Message: message,
			})
			if !errors.Is(err, ErrUnauthorized) {
				t.Fatalf("error = %v, want ErrUnauthorized", err)
			}
		})
	}
}

func TestStaticFrameRejectsConfigurationEntry(t *testing.T) {
	group := testGroup(18)
	sender, _, from, to := frameTestRegistries(t, 2, group)
	for _, entryType := range []pb.EntryType{pb.EntryConfChange, pb.EntryConfChangeV2} {
		t.Run(entryType.String(), func(t *testing.T) {
			message := frameTestMessage(pb.MsgApp, from, to)
			message.Entries[0].Type = entryType.Enum()
			_, _, err := sender.EncodeOutbound(nil, raftmember.OutboundMessage{
				Group: group, From: from, To: to, Message: message,
			})
			if !errors.Is(err, ErrUnsupportedFrame) {
				t.Fatalf("error = %v, want ErrUnsupportedFrame", err)
			}
		})
	}
}

func TestDecodeInboundRejectsChangedStaticRoster(t *testing.T) {
	group := testGroup(19)
	sender, receiver, from, to := frameTestRegistries(t, 3, group)
	frame := frameTestEncode(t, sender, group, frameTestMessage(pb.MsgHeartbeat, from, to))
	baseMembers := []Member{
		{Group: group, ReplicaSetVersion: 1, MemberID: 11, Node: testNode(1), Role: MemberVoter},
		{Group: group, ReplicaSetVersion: 1, MemberID: 12, Node: testNode(2), Role: MemberVoter},
		{Group: group, ReplicaSetVersion: 1, MemberID: 13, Node: testNode(3), Role: MemberVoter},
	}
	tests := []struct {
		name   string
		mutate func([]Member)
	}{
		{name: "role", mutate: func(members []Member) { members[1].Role = MemberLearner }},
		{name: "node", mutate: func(members []Member) { members[1].Node = testNode(4) }},
		{name: "replica-set version", mutate: func(members []Member) {
			for i := range members {
				members[i].ReplicaSetVersion++
			}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			members := append([]Member(nil), baseMembers...)
			test.mutate(members)
			changed, err := NewStaticRegistry(receiver.LocalNode(), members, Limits{MaxGroups: 1, MaxMembers: 3})
			if err != nil {
				t.Fatalf("changed registry: %v", err)
			}
			if _, err := changed.DecodeInbound(testPeerIdentity(changed, sender.LocalNode()), frame); !errors.Is(err, ErrUnauthorized) {
				t.Fatalf("error = %v, want ErrUnauthorized", err)
			}
		})
	}
}

func TestEncodeOutboundRejectsWritableDestinationAliasingMessage(t *testing.T) {
	group := testGroup(20)
	sender, _, from, to := frameTestRegistries(t, 2, group)
	backing := make([]byte, 64, 512)
	copy(backing, "payload-alias")
	message := frameTestMessage(pb.MsgApp, from, to)
	message.Entries[0].Data = backing[:13]
	message.Context = nil
	dst := backing[:0]
	got, destination, err := sender.EncodeOutbound(dst, raftmember.OutboundMessage{
		Group: group, From: from, To: to, Message: message,
	})
	if !errors.Is(err, ErrInvalidFrame) {
		t.Fatalf("error = %v, want ErrInvalidFrame", err)
	}
	if len(got) != 0 || destination != (NodeID{}) {
		t.Fatalf("failure result = (%d bytes, %x), want empty and zero", len(got), destination)
	}
}

func TestEncodeOutboundRelocatesBeforePartialCapacityCanOverwriteMessage(t *testing.T) {
	group := testGroup(21)
	sender, receiver, from, to := frameTestRegistries(t, 2, group)
	backing := make([]byte, FrameHeaderBytes+16)
	data := backing[8:24]
	copy(data, "partial-cap-data")
	message := frameTestMessage(pb.MsgApp, from, to)
	message.Entries[0].Data = data
	wantData := bytes.Clone(data)
	dst := backing[:0]
	frame, destination, err := sender.EncodeOutbound(dst, raftmember.OutboundMessage{
		Group: group, From: from, To: to, Message: message,
	})
	if err != nil {
		t.Fatalf("EncodeOutbound: %v", err)
	}
	if destination != receiver.LocalNode() {
		t.Fatalf("destination = %x, want %x", destination, receiver.LocalNode())
	}
	if !bytes.Equal(message.Entries[0].Data, wantData) {
		t.Fatalf("message data mutated: got %x want %x", message.Entries[0].Data, wantData)
	}
	if len(frame) <= cap(backing) || &frame[0] == &backing[0] {
		t.Fatal("partial-capacity encode did not relocate complete frame")
	}
	inbound, err := receiver.DecodeInbound(testPeerIdentity(receiver, sender.LocalNode()), frame)
	if err != nil {
		t.Fatalf("DecodeInbound: %v", err)
	}
	if !bytes.Equal(inbound.Message.GetEntries()[0].GetData(), wantData) {
		t.Fatalf("decoded data = %x, want %x", inbound.Message.GetEntries()[0].GetData(), wantData)
	}
}

func TestDecodeInboundRejectsForgedPeerGroupAndMember(t *testing.T) {
	group := testGroup(11)
	sender, receiver, from, to := frameTestRegistries(t, 3, group)
	frame := frameTestEncode(t, sender, group, frameTestMessage(pb.MsgHeartbeat, from, to))

	t.Run("authenticated node", func(t *testing.T) {
		if _, err := receiver.DecodeInbound(testPeerIdentity(receiver, testNode(2)), frame); !errors.Is(err, ErrUnauthorized) {
			t.Fatalf("error = %v, want ErrUnauthorized", err)
		}
	})
	t.Run("missing authenticated node", func(t *testing.T) {
		if _, err := receiver.DecodeInbound(PeerIdentity{}, frame); !errors.Is(err, ErrUnauthorized) {
			t.Fatalf("error = %v, want ErrUnauthorized", err)
		}
	})

	coordinates := []struct {
		name  string
		start int
	}{
		{name: "cluster", start: frameTestGroupOffset},
		{name: "cluster incarnation", start: frameTestGroupOffset + 16},
		{name: "recovery epoch", start: frameTestGroupOffset + 32},
		{name: "shard incarnation", start: frameTestGroupOffset + 40},
		{name: "group", start: frameTestGroupOffset + 56},
	}
	for _, coordinate := range coordinates {
		t.Run(coordinate.name, func(t *testing.T) {
			forged := bytes.Clone(frame)
			forged[coordinate.start] ^= 0x80
			if _, err := receiver.DecodeInbound(testPeerIdentity(receiver, sender.LocalNode()), forged); !errors.Is(err, ErrUnauthorized) {
				t.Fatalf("error = %v, want ErrUnauthorized", err)
			}
		})
	}

	tests := []struct {
		name          string
		authenticated NodeID
		mutate        func([]byte)
		want          error
	}{
		{
			name:          "unknown source member",
			authenticated: sender.LocalNode(),
			mutate: func(frame []byte) {
				binary.BigEndian.PutUint64(frame[frameTestFromOffset:frameTestToOffset], 999)
			},
			want: ErrUnauthorized,
		},
		{
			name:          "unknown destination member",
			authenticated: sender.LocalNode(),
			mutate: func(frame []byte) {
				binary.BigEndian.PutUint64(frame[frameTestToOffset:frameTestLengthOffset], 999)
			},
			want: ErrUnauthorized,
		},
		{
			name:          "remote destination member",
			authenticated: sender.LocalNode(),
			mutate: func(frame []byte) {
				binary.BigEndian.PutUint64(frame[frameTestToOffset:frameTestLengthOffset], 12)
			},
			want: ErrUnauthorized,
		},
		{
			name:          "authorized source does not match protobuf",
			authenticated: testNode(2),
			mutate: func(frame []byte) {
				binary.BigEndian.PutUint64(frame[frameTestFromOffset:frameTestToOffset], 12)
			},
			want: ErrInvalidFrame,
		},
		{
			name:          "roster digest",
			authenticated: sender.LocalNode(),
			mutate:        func(frame []byte) { frame[frameTestRosterOffset] ^= 0x80 },
			want:          ErrUnauthorized,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			forged := bytes.Clone(frame)
			test.mutate(forged)
			if _, err := receiver.DecodeInbound(testPeerIdentity(receiver, test.authenticated), forged); !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestDecodeInboundRejectsOuterInnerMismatch(t *testing.T) {
	group := testGroup(13)
	sender, receiver, from, to := frameTestRegistries(t, 3, group)
	frame := frameTestEncode(t, sender, group, frameTestMessage(pb.MsgHeartbeat, from, to))

	tests := []struct {
		name   string
		mutate func(*pb.Message)
	}{
		{name: "from", mutate: func(message *pb.Message) { message.From = frameU64(12) }},
		{name: "to", mutate: func(message *pb.Message) { message.To = frameU64(12) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			message := frameTestMessage(pb.MsgHeartbeat, from, to)
			test.mutate(message)
			forged := frameTestReplacePayload(t, frame, message)
			if _, err := receiver.DecodeInbound(testPeerIdentity(receiver, sender.LocalNode()), forged); !errors.Is(err, ErrInvalidFrame) {
				t.Fatalf("error = %v, want ErrInvalidFrame", err)
			}
		})
	}
}

func TestDecodeInboundClassifiesCanonicalMalformedMessage(t *testing.T) {
	group := testGroup(14)
	sender, receiver, from, to := frameTestRegistries(t, 2, group)
	valid := frameTestEncode(t, sender, group, frameTestMessage(pb.MsgHeartbeat, from, to))
	malformed := frameTestMessage(pb.MsgHeartbeat, from, to)
	malformed.Term = frameU64(0)
	frame := frameTestReplacePayload(t, valid, malformed)
	if _, err := receiver.DecodeInbound(testPeerIdentity(receiver, sender.LocalNode()), frame); !errors.Is(err, ErrInvalidFrame) {
		t.Fatalf("error = %v, want ErrInvalidFrame", err)
	}
}

func TestDecodeInboundRejectsEveryTruncationAndHeaderDamage(t *testing.T) {
	group := testGroup(15)
	sender, receiver, from, to := frameTestRegistries(t, 2, group)
	frame := frameTestEncode(t, sender, group, frameTestMessage(pb.MsgHeartbeat, from, to))

	for length := 0; length < len(frame); length++ {
		if _, err := receiver.DecodeInbound(testPeerIdentity(receiver, sender.LocalNode()), frame[:length]); err == nil {
			t.Fatalf("prefix length %d unexpectedly accepted", length)
		}
	}
	if _, err := receiver.DecodeInbound(testPeerIdentity(receiver, sender.LocalNode()), append(bytes.Clone(frame), 0)); !errors.Is(err, ErrInvalidFrame) {
		t.Fatalf("trailing byte error = %v, want ErrInvalidFrame", err)
	}
	if _, err := receiver.DecodeInbound(testPeerIdentity(receiver, sender.LocalNode()), make([]byte, MaxFrameBytes+1)); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("oversized complete frame error = %v, want ErrFrameTooLarge", err)
	}

	headerTests := []struct {
		name   string
		mutate func([]byte)
		want   error
	}{
		{name: "magic", mutate: func(frame []byte) { frame[0] ^= 0xff }, want: ErrUnsupportedFrame},
		{name: "format", mutate: func(frame []byte) { binary.BigEndian.PutUint16(frame[4:6], frameCodecFormat+1) }, want: ErrUnsupportedFrame},
		{name: "kind", mutate: func(frame []byte) { frame[6]++ }, want: ErrUnsupportedFrame},
		{name: "flags", mutate: func(frame []byte) { frame[7] = 1 }, want: ErrUnsupportedFrame},
		{name: "zero cluster", mutate: func(frame []byte) { clear(frame[frameTestGroupOffset : frameTestGroupOffset+16]) }, want: ErrInvalidFrame},
		{name: "zero cluster incarnation", mutate: func(frame []byte) { clear(frame[frameTestGroupOffset+16 : frameTestGroupOffset+32]) }, want: ErrInvalidFrame},
		{name: "zero recovery epoch", mutate: func(frame []byte) { clear(frame[frameTestGroupOffset+32 : frameTestGroupOffset+40]) }, want: ErrInvalidFrame},
		{name: "zero shard incarnation", mutate: func(frame []byte) { clear(frame[frameTestGroupOffset+40 : frameTestGroupOffset+56]) }, want: ErrInvalidFrame},
		{name: "zero group", mutate: func(frame []byte) { clear(frame[frameTestGroupOffset+56 : frameTestRosterOffset]) }, want: ErrInvalidFrame},
		{name: "zero from", mutate: func(frame []byte) { clear(frame[frameTestFromOffset:frameTestToOffset]) }, want: ErrInvalidFrame},
		{name: "zero to", mutate: func(frame []byte) { clear(frame[frameTestToOffset:frameTestLengthOffset]) }, want: ErrInvalidFrame},
		{name: "same peer", mutate: func(frame []byte) {
			copy(frame[frameTestToOffset:frameTestLengthOffset], frame[frameTestFromOffset:frameTestToOffset])
		}, want: ErrInvalidFrame},
		{name: "short declared payload", mutate: func(frame []byte) {
			binary.BigEndian.PutUint32(
				frame[frameTestLengthOffset:FrameHeaderBytes],
				binary.BigEndian.Uint32(frame[frameTestLengthOffset:FrameHeaderBytes])-1,
			)
		}, want: ErrInvalidFrame},
		{name: "oversized declared payload", mutate: func(frame []byte) {
			binary.BigEndian.PutUint32(frame[frameTestLengthOffset:FrameHeaderBytes], uint32(raftmodel.MaxInboundMessageBytes+1))
		}, want: ErrFrameTooLarge},
	}
	for _, test := range headerTests {
		t.Run(test.name, func(t *testing.T) {
			corrupt := bytes.Clone(frame)
			test.mutate(corrupt)
			if _, err := receiver.DecodeInbound(testPeerIdentity(receiver, sender.LocalNode()), corrupt); !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestDecodeInboundRejectsHostileProtobuf(t *testing.T) {
	group := testGroup(16)
	sender, receiver, from, to := frameTestRegistries(t, 2, group)
	valid := frameTestEncode(t, sender, group, frameTestMessage(pb.MsgHeartbeat, from, to))
	recursive := []byte{0x80}
	for range 128 {
		recursive = wireBytes(nil, 14, recursive)
	}
	tinyEntries := make([]byte, 0, 2*(raftmodel.MaxMessageEntries+1))
	for range raftmodel.MaxMessageEntries + 1 {
		tinyEntries = wireBytes(tinyEntries, 7, nil)
	}
	reordered := wireBytes(nil, 12, []byte("ctx"))
	reordered = wireVarint(reordered, 8, 7)
	reordered = wireVarint(reordered, 6, 7)
	reordered = wireVarint(reordered, 5, 4)
	reordered = wireVarint(reordered, 4, 5)
	reordered = wireVarint(reordered, 3, from)
	reordered = wireVarint(reordered, 2, to)
	reordered = wireVarint(reordered, 1, uint64(pb.MsgHeartbeat))
	tests := []struct {
		name    string
		payload []byte
		want    error
	}{
		{name: "unknown field", payload: wireVarint(nil, 15, 1), want: ErrInvalidFrame},
		{name: "wrong wire type", payload: wireBytes(nil, 1, nil), want: ErrInvalidFrame},
		{name: "noncanonical varint", payload: []byte{0x08, 0x88, 0x00}, want: ErrInvalidFrame},
		{name: "duplicate singular", payload: []byte{0x08, 0x08, 0x08, 0x08}, want: ErrInvalidFrame},
		{name: "noncanonical field order", payload: reordered, want: ErrInvalidFrame},
		{name: "snapshot", payload: wireBytes(nil, 9, []byte{0x80}), want: ErrUnsupportedFrame},
		{name: "local vote", payload: wireVarint(nil, 13, 0), want: ErrUnsupportedFrame},
		{name: "recursive responses", payload: recursive, want: ErrUnsupportedFrame},
		{name: "too many tiny entries", payload: tinyEntries, want: ErrFrameTooLarge},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			frame := frameTestReplaceRawPayload(valid, test.payload)
			if _, err := receiver.DecodeInbound(testPeerIdentity(receiver, sender.LocalNode()), frame); !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestFrameOwnershipIsDetached(t *testing.T) {
	group := testGroup(17)
	sender, receiver, from, to := frameTestRegistries(t, 2, group)
	message := frameTestMessage(pb.MsgApp, from, to)
	want := proto.Clone(message).(*pb.Message)
	frame := frameTestEncode(t, sender, group, message)

	*message.Term = 99
	*message.Entries[0].Term = 99
	message.Entries[0].Data[0] ^= 0xff
	message.Context = []byte("mutated")

	inbound, err := receiver.DecodeInbound(testPeerIdentity(receiver, sender.LocalNode()), frame)
	if err != nil {
		t.Fatalf("DecodeInbound: %v", err)
	}
	if !proto.Equal(inbound.Message, want) {
		t.Fatalf("decoded message changed through source alias: got %v, want %v", inbound.Message, want)
	}
	clear(frame)
	if !proto.Equal(inbound.Message, want) {
		t.Fatalf("decoded message changed through frame alias: got %v, want %v", inbound.Message, want)
	}
}

func frameTestRegistries(
	t testing.TB,
	replicas int,
	group raftmember.GroupKey,
) (sender, receiver *StaticRegistry, from, to uint64) {
	t.Helper()
	members := make([]Member, replicas)
	for i := range members {
		members[i] = Member{
			Group:             group,
			ReplicaSetVersion: 1,
			MemberID:          uint64(11 + i),
			Node:              testNode(byte(i + 1)),
			Role:              MemberVoter,
		}
	}
	limits := Limits{MaxGroups: 1, MaxMembers: replicas}
	receiver, err := NewStaticRegistry(testNode(1), members, limits)
	if err != nil {
		t.Fatalf("receiver registry: %v", err)
	}
	sender, err = NewStaticRegistry(testNode(byte(replicas)), members, limits)
	if err != nil {
		t.Fatalf("sender registry: %v", err)
	}
	return sender, receiver, uint64(10 + replicas), 11
}

func frameTestMessage(messageType pb.MessageType, from, to uint64) *pb.Message {
	message := frameBaseMessage(messageType, from, to)
	switch messageType {
	case pb.MsgApp:
		message.Entries = []*pb.Entry{{
			Type:  pb.EntryNormal.Enum(),
			Term:  frameU64(5),
			Index: frameU64(8),
			Data:  []byte("row-change"),
		}}
	case pb.MsgAppResp:
		message.Reject = frameBool(true)
		message.RejectHint = frameU64(6)
	case pb.MsgVoteResp, pb.MsgPreVoteResp:
		message.Reject = frameBool(true)
	case pb.MsgHeartbeat, pb.MsgHeartbeatResp, pb.MsgVote, pb.MsgPreVote:
		message.Context = []byte("ctx")
	}
	return message
}

func frameTimeoutNow(from, to, term uint64) *pb.Message {
	return &pb.Message{
		Type: pb.MsgTimeoutNow.Enum(), From: frameU64(from), To: frameU64(to), Term: frameU64(term),
	}
}

func frameBaseMessage(messageType pb.MessageType, from, to uint64) *pb.Message {
	return &pb.Message{
		Type:    messageType.Enum(),
		To:      frameU64(to),
		From:    frameU64(from),
		Term:    frameU64(5),
		LogTerm: frameU64(4),
		Index:   frameU64(7),
		Commit:  frameU64(7),
	}
}

func frameTestEncode(t testing.TB, sender *StaticRegistry, group raftmember.GroupKey, message *pb.Message) []byte {
	t.Helper()
	frame, _, err := sender.EncodeOutbound(nil, raftmember.OutboundMessage{
		Group: group, From: message.GetFrom(), To: message.GetTo(), Message: message,
	})
	if err != nil {
		t.Fatalf("EncodeOutbound: %v", err)
	}
	return frame
}

func frameTestReplacePayload(t testing.TB, frame []byte, message *pb.Message) []byte {
	t.Helper()
	payload, err := (proto.MarshalOptions{Deterministic: true}).Marshal(message)
	if err != nil {
		t.Fatalf("marshal replacement payload: %v", err)
	}
	return frameTestReplaceRawPayload(frame, payload)
}

func frameTestReplaceRawPayload(frame, payload []byte) []byte {
	replaced := append(bytes.Clone(frame[:FrameHeaderBytes]), payload...)
	binary.BigEndian.PutUint32(replaced[frameTestLengthOffset:FrameHeaderBytes], uint32(len(payload)))
	return replaced
}

func frameU64(value uint64) *uint64 { return &value }

func frameBool(value bool) *bool { return &value }
