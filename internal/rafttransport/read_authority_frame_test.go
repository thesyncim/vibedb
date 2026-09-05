package rafttransport

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/raftauthority"
	"github.com/thesyncim/vibedb/internal/raftmember"
	pb "go.etcd.io/raft/v3/raftpb"
)

func TestAuthorityFrameRoundTripRequestAndGrant(t *testing.T) {
	group := testGroup(101)
	sender, receiver, from, to := frameTestRegistries(t, 3, group)
	cases := []struct {
		name string
		kind raftauthority.MessageKind
		from uint64
		to   uint64
	}{
		{name: "request", kind: raftauthority.MessageRequest, from: from, to: to},
		{name: "grant", kind: raftauthority.MessageGrant, from: from, to: to},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			message := readAuthorityFrameMessage(group, test.kind, test.from, test.to)
			outbound := raftmember.OutboundMessage{
				Group: group, From: test.from, To: test.to, Authority: &message,
			}
			frame, destination, err := sender.EncodeOutbound(nil, outbound)
			if err != nil {
				t.Fatalf("EncodeOutbound: %v", err)
			}
			if destination != receiver.LocalNode() {
				t.Fatalf("destination = %x, want %x", destination, receiver.LocalNode())
			}
			if len(frame) != FrameHeaderBytes+raftauthority.CanonicalMessageBytes {
				t.Fatalf("frame bytes = %d, want %d", len(frame), FrameHeaderBytes+raftauthority.CanonicalMessageBytes)
			}
			again, againDestination, err := sender.EncodeOutbound(nil, outbound)
			if err != nil {
				t.Fatalf("second EncodeOutbound: %v", err)
			}
			if againDestination != destination || !bytes.Equal(again, frame) {
				t.Fatal("authority encoding is not deterministic")
			}
			inbound, err := receiver.DecodeInbound(testPeerIdentity(receiver, sender.LocalNode()), frame)
			if err != nil {
				t.Fatalf("DecodeInbound: %v", err)
			}
			if inbound.Group != group || inbound.Message != nil || inbound.Authority == nil {
				t.Fatalf("inbound = %+v, want detached authority payload", inbound)
			}
			if *inbound.Authority != message {
				t.Fatalf("authority = %+v, want %+v", *inbound.Authority, message)
			}
		})
	}
}

func TestAuthorityFrameRejectsSpoofedRoutesAndGroup(t *testing.T) {
	group := testGroup(102)
	sender, receiver, from, to := frameTestRegistries(t, 3, group)
	request := readAuthorityFrameMessage(group, raftauthority.MessageRequest, from, to)
	grant := readAuthorityFrameMessage(group, raftauthority.MessageGrant, from, to)
	cases := []struct {
		name    string
		message raftauthority.Message
		want    error
	}{
		{
			name: "request holder spoof",
			message: func() raftauthority.Message {
				mutated := request
				mutated.Request.Holder = to
				return mutated
			}(),
			want: ErrInvalidFrame,
		},
		{
			name: "grant voter spoof",
			message: func() raftauthority.Message {
				mutated := grant
				mutated.Grant.Voter = to
				return mutated
			}(),
			want: ErrInvalidFrame,
		},
		{
			name: "grant holder route spoof",
			message: func() raftauthority.Message {
				mutated := grant
				mutated.Request.Holder = from
				mutated.Grant.Request.Holder = from
				return mutated
			}(),
			want: ErrInvalidFrame,
		},
		{
			name: "request group mismatch",
			message: func() raftauthority.Message {
				mutated := request
				mutated.Request.Group.GroupID[0]++
				return mutated
			}(),
			want: ErrInvalidFrame,
		},
		{
			name: "grant config arm mismatch",
			message: func() raftauthority.Message {
				mutated := grant
				mutated.Grant.Request.Config.Digest[0]++
				return mutated
			}(),
			want: ErrInvalidFrame,
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			outbound := raftmember.OutboundMessage{Group: group, From: from, To: to, Authority: &test.message}
			if _, _, err := sender.EncodeOutbound(nil, outbound); !errors.Is(err, test.want) {
				t.Fatalf("EncodeOutbound error = %v, want %v", err, test.want)
			}
		})
	}

	requestFrame, _, err := sender.EncodeOutbound(nil, raftmember.OutboundMessage{
		Group: group, From: from, To: to, Authority: &request,
	})
	if err != nil {
		t.Fatalf("valid request EncodeOutbound: %v", err)
	}
	grantFrame, _, err := sender.EncodeOutbound(nil, raftmember.OutboundMessage{
		Group: group, From: from, To: to, Authority: &grant,
	})
	if err != nil {
		t.Fatalf("valid grant EncodeOutbound: %v", err)
	}
	decodeCases := []struct {
		name   string
		frame  []byte
		mutate func() (raftauthority.Message, error)
	}{
		{
			name: "request holder spoof", frame: requestFrame,
			mutate: func() (raftauthority.Message, error) {
				mutated := request
				mutated.Request.Holder = to
				return mutated, nil
			},
		},
		{
			name: "grant voter spoof", frame: grantFrame,
			mutate: func() (raftauthority.Message, error) {
				mutated := grant
				mutated.Grant.Voter = to
				return mutated, nil
			},
		},
		{
			name: "grant holder route spoof", frame: grantFrame,
			mutate: func() (raftauthority.Message, error) {
				mutated := grant
				mutated.Request.Holder = from
				mutated.Grant.Request.Holder = from
				return mutated, nil
			},
		},
		{
			name: "request group mismatch", frame: requestFrame,
			mutate: func() (raftauthority.Message, error) {
				mutated := request
				mutated.Request.Group.GroupID[0]++
				return mutated, nil
			},
		},
	}
	for _, test := range decodeCases {
		t.Run("decode/"+test.name, func(t *testing.T) {
			mutated, err := test.mutate()
			if err != nil {
				t.Fatal(err)
			}
			payload, err := raftauthority.AppendCanonical(nil, mutated)
			if err != nil {
				t.Fatalf("canonical payload: %v", err)
			}
			if _, err := receiver.DecodeInbound(testPeerIdentity(receiver, sender.LocalNode()), frameTestReplaceRawPayload(test.frame, payload)); !errors.Is(err, ErrInvalidFrame) {
				t.Fatalf("DecodeInbound error = %v, want ErrInvalidFrame", err)
			}
		})
	}
}

func TestAuthorityFrameRejectsRetiredGenerationAndUnsupportedUnion(t *testing.T) {
	group := testGroup(103)
	members := []Member{
		{Group: group, ReplicaSetVersion: 5, MemberID: 1, Node: testNode(1), Role: MemberVoter},
		{Group: group, ReplicaSetVersion: 5, MemberID: 2, Node: testNode(2), Role: MemberVoter},
		{Group: group, ReplicaSetVersion: 5, MemberID: 3, Node: testNode(3), Role: MemberEnrolled},
		{Group: group, ReplicaSetVersion: 5, MemberID: 4, Node: testNode(4), Role: MemberVoter},
	}
	open := func(local NodeID) *StaticRegistry {
		registry, err := NewStaticRegistry(local, members, Limits{MaxGroups: 1, MaxMembers: 4})
		if err != nil {
			t.Fatal(err)
		}
		if err := registry.InstallTransitionGrant(authorityTestGrant(group)); err != nil {
			t.Fatal(err)
		}
		if err := registry.PublishCommittedAuthority(group, 6, &pb.ConfState{
			Voters: []uint64{1, 2, 4}, Learners: []uint64{3},
		}); err != nil {
			t.Fatalf("publish generation 6: %v", err)
		}
		if err := registry.PublishCommittedAuthority(group, 7, &pb.ConfState{
			Voters: []uint64{1, 2, 3, 4},
		}); err != nil {
			t.Fatalf("publish generation 7: %v", err)
		}
		return registry
	}
	sender, receiver := open(testNode(1)), open(testNode(2))
	request := readAuthorityFrameMessage(group, raftauthority.MessageRequest, 1, 2)
	frame, _, err := sender.EncodeOutbound(nil, raftmember.OutboundMessage{
		Group: group, From: 1, To: 2, Authority: &request,
	})
	if err != nil {
		t.Fatalf("EncodeOutbound: %v", err)
	}
	unsupported := bytes.Clone(frame)
	unsupported[6] = 0x7f
	if _, err := receiver.DecodeInbound(testPeerIdentity(receiver, sender.LocalNode()), unsupported); !errors.Is(err, ErrUnsupportedFrame) {
		t.Fatalf("unsupported frame kind error = %v, want ErrUnsupportedFrame", err)
	}

	unsupportedUnion := bytes.Clone(frame[FrameHeaderBytes:])
	unsupportedUnion[8] = 0x7f
	if _, err := receiver.DecodeInbound(testPeerIdentity(receiver, sender.LocalNode()), frameTestReplaceRawPayload(frame, unsupportedUnion)); !errors.Is(err, ErrInvalidFrame) {
		t.Fatalf("unsupported authority union error = %v, want ErrInvalidFrame", err)
	}

	if err := sender.PublishCommittedAuthority(group, 11, &pb.ConfState{Voters: []uint64{2, 3, 4}}); err != nil {
		t.Fatalf("retire source generation: %v", err)
	}
	if err := receiver.PublishCommittedAuthority(group, 11, &pb.ConfState{Voters: []uint64{2, 3, 4}}); err != nil {
		t.Fatalf("retire receiver generation: %v", err)
	}
	retired := bytes.Clone(frame)
	if _, err := receiver.DecodeInbound(testPeerIdentity(receiver, sender.LocalNode()), retired); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("retired generation error = %v, want ErrUnauthorized", err)
	}
}

func readAuthorityFrameMessage(
	group raftmember.GroupKey,
	kind raftauthority.MessageKind,
	from, to uint64,
) raftauthority.Message {
	request := raftauthority.AuthorityRequest{
		Group: raftauthority.GroupIdentity{
			ClusterID: group.ClusterID, ClusterIncarnation: group.ClusterIncarnation,
			TopologyRecoveryEpoch: group.TopologyRecoveryEpoch,
			ShardIncarnation:      group.ShardIncarnation, GroupID: group.GroupID,
		},
		Term: 5, Holder: from, HolderIncarnation: 7,
		Config:        raftauthority.ConfigIdentity{AppliedVersion: 9, Digest: [32]byte{0x44}},
		PolicyVersion: 3, PolicyDigest: [32]byte{0x55}, Nonce: 11,
		StartAt: 12 * time.Millisecond,
	}
	message := raftauthority.Message{Kind: kind, Request: request}
	if kind == raftauthority.MessageGrant {
		message.Grant = raftauthority.AuthorityGrant{
			Request: request, Voter: from, GrantedAt: 20 * time.Millisecond,
			PromiseUntil: 1 * time.Second,
		}
		// Grant route is voter -> holder, so the embedded request names the
		// destination holder rather than the sender.
		message.Request.Holder = to
		message.Grant.Request.Holder = to
	}
	return message
}
