package multiraft

import (
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	pb "go.etcd.io/raft/v3/raftpb"
)

func TestDecodedTransportMessageTransfersIntoHostWithoutSecondClone(t *testing.T) {
	host, err := NewHost(testHostLimits())
	if err != nil {
		t.Fatal(err)
	}
	runtime := newFakeRuntime(83)
	if err := host.addRuntime(runtime); err != nil {
		t.Fatal(err)
	}
	localNode := rafttransport.NodeID{1}
	remoteNode := rafttransport.NodeID{2}
	remoteMember := uint64(99)
	members := []rafttransport.Member{
		{
			Group: runtime.identity.Group, ReplicaSetVersion: 1,
			MemberID: runtime.identity.MemberID, Node: localNode, Role: rafttransport.MemberVoter,
		},
		{
			Group: runtime.identity.Group, ReplicaSetVersion: 1,
			MemberID: remoteMember, Node: remoteNode, Role: rafttransport.MemberVoter,
		},
	}
	limits := rafttransport.Limits{MaxGroups: 1, MaxMembers: 2}
	sender, err := rafttransport.NewStaticRegistry(remoteNode, members, limits)
	if err != nil {
		t.Fatal(err)
	}
	receiver, err := rafttransport.NewStaticRegistry(localNode, members, limits)
	if err != nil {
		t.Fatal(err)
	}
	message := transportHeartbeat(remoteMember, runtime.identity.MemberID)
	frame, destination, err := sender.EncodeOutbound(nil, raftmember.OutboundMessage{
		Group: runtime.identity.Group, From: remoteMember,
		To: runtime.identity.MemberID, Message: message,
	})
	if err != nil || destination != localNode {
		t.Fatalf("EncodeOutbound = %x, %v", destination, err)
	}
	inbound, err := receiver.DecodeInbound(rafttransport.PeerIdentity{
		TrustDomain: receiver.TrustDomain(), Node: remoteNode,
	}, frame)
	if err != nil {
		t.Fatal(err)
	}
	owned := inbound.Message
	if err := host.AdoptMessage(inbound.Group, owned); err != nil {
		t.Fatal(err)
	}
	if queued := host.groups[runtime.identity.Group].messages.items[0].message; queued != owned {
		t.Fatal("Host cloned transport-owned message")
	}
	progress, done, err := host.RunOne()
	if err != nil || !done || progress.Kind != ProgressMessage {
		t.Fatalf("RunOne = %+v, %t, %v", progress, done, err)
	}
	if len(runtime.messages) != 1 || runtime.messages[0].GetFrom() != remoteMember {
		t.Fatalf("runtime messages = %+v", runtime.messages)
	}
}

func TestDecodedLeaderTransferMessageReachesRF3Host(t *testing.T) {
	host, err := NewHost(testHostLimits())
	if err != nil {
		t.Fatal(err)
	}
	runtime := newFakeRuntime(84)
	if err := host.addRuntime(runtime); err != nil {
		t.Fatal(err)
	}
	localNode := rafttransport.NodeID{1}
	remoteNode := rafttransport.NodeID{2}
	remoteMember := uint64(99)
	members := []rafttransport.Member{
		{Group: runtime.identity.Group, ReplicaSetVersion: 1, MemberID: runtime.identity.MemberID, Node: localNode, Role: rafttransport.MemberVoter},
		{Group: runtime.identity.Group, ReplicaSetVersion: 1, MemberID: remoteMember, Node: remoteNode, Role: rafttransport.MemberVoter},
	}
	limits := rafttransport.Limits{MaxGroups: 1, MaxMembers: 2}
	sender, err := rafttransport.NewStaticRegistry(remoteNode, members, limits)
	if err != nil {
		t.Fatal(err)
	}
	receiver, err := rafttransport.NewStaticRegistry(localNode, members, limits)
	if err != nil {
		t.Fatal(err)
	}
	term := uint64(5)
	message := &pb.Message{
		Type: pb.MsgTimeoutNow.Enum(), From: &remoteMember,
		To: &runtime.identity.MemberID, Term: &term,
	}
	frame, _, err := sender.EncodeOutbound(nil, raftmember.OutboundMessage{
		Group: runtime.identity.Group, From: remoteMember, To: runtime.identity.MemberID, Message: message,
	})
	if err != nil {
		t.Fatal(err)
	}
	inbound, err := receiver.DecodeInbound(rafttransport.PeerIdentity{
		TrustDomain: receiver.TrustDomain(), Node: remoteNode,
	}, frame)
	if err != nil {
		t.Fatal(err)
	}
	if err := host.AdoptMessage(inbound.Group, inbound.Message); err != nil {
		t.Fatal(err)
	}
	if progress, done, err := host.RunOne(); err != nil || !done || progress.Kind != ProgressMessage {
		t.Fatalf("RunOne = %+v, %t, %v", progress, done, err)
	}
	if len(runtime.messages) != 1 || runtime.messages[0].GetType() != pb.MsgTimeoutNow ||
		runtime.messages[0].GetTerm() != term {
		t.Fatalf("runtime messages = %+v", runtime.messages)
	}
}

func transportHeartbeat(from, to uint64) *pb.Message {
	term := uint64(1)
	return &pb.Message{
		Type: pb.MsgHeartbeat.Enum(), From: &from, To: &to, Term: &term,
	}
}
