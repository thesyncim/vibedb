package shardservice

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

func TestServeAuthenticatedAllowlistAndRotationEndToEnd(t *testing.T) {
	authority := newShardTLSAuthority(t)
	serverIdentity := shardPeerIdentity(7, 41)
	firstIdentity := shardPeerIdentity(7, 61)
	secondIdentity := shardPeerIdentity(7, 81)
	serverProfile := authority.profile(t, serverIdentity)
	replacementProfile := authority.profile(t, serverIdentity)
	firstProfile := authority.profile(t, firstIdentity)
	secondProfile := authority.profile(t, secondIdentity)
	capability, err := NewReplicatedServerTLS(serverProfile, []rafttransport.NodeID{firstIdentity.Node})
	if err != nil {
		t.Fatal(err)
	}
	fence := testReplicatedFence()
	state := raftservice.ServingState{Identity: raftmember.RuntimeIdentity{Group: fence.Group,
		AllocationGeneration: fence.AllocationGeneration, MemberID: fence.MemberID,
		StoreID: fence.StoreID, NodeIncarnation: fence.NodeIncarnation}, Command: fence.Command,
		Status: raftmember.RuntimeStatus{MemberID: fence.MemberID, LeaderID: fence.MemberID,
			Term: fence.Term, Commit: 8, Applied: 8, CheckpointApplied: 7}}
	server := testReplicatedServer(&fakeReplicatedOwner{state: state})
	client := rafttransport.NodeID{101}
	policy, err := serviceauthz.NewPolicy(1, []serviceauthz.Entry{
		{Node: firstIdentity.Node, Capabilities: serviceauthz.CapabilityDelegate},
		{Node: secondIdentity.Node, Capabilities: serviceauthz.CapabilityDelegate},
		{Node: client, Capabilities: serviceauthz.CapabilityDataRead},
	})
	if err != nil {
		t.Fatal(err)
	}
	gate, _ := serviceauthz.NewGate(policy)
	if err = server.BindAuthorization(gate, nil); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	serveDone := make(chan error, 1)
	deadline := func() time.Time { return time.Now().Add(time.Second) }
	go func() { serveDone <- server.ServeAuthenticated(ctx, listener, capability, deadline, 4, 2) }()
	dial := func(profile *rafttransport.PeerTLS) (rafttransport.PeerConnection, error) {
		raw, err := (&net.Dialer{}).DialContext(ctx, "tcp", listener.Addr().String())
		if err != nil {
			return nil, err
		}
		return profile.Client(ctx, raw, serverIdentity.Node, rafttransport.TrafficShardNative, deadline)
	}
	request := &ReplicatedRequest{Operation: ReplicatedProbe,
		Authority: serviceauthz.Authority{Node: client, Generation: 1},
		Fence:     ReplicatedFence{Group: fence.Group, AllocationGeneration: fence.AllocationGeneration}}
	first, err := dial(firstProfile)
	if err != nil {
		t.Fatal(err)
	}
	if response, err := RoundTripReplicated(ctx, first, request); err != nil || response.Kind != ReplicatedHandshake {
		t.Fatalf("first response=%+v err=%v", response, err)
	}
	denied, err := dial(secondProfile)
	if err == nil {
		_, err = RoundTripReplicated(ctx, denied, request)
		_ = denied.Close()
	}
	if err == nil {
		t.Fatal("non-allowlisted gateway served a request")
	}
	if err := capability.Rotate(replacementProfile, []rafttransport.NodeID{secondIdentity.Node}); err != nil {
		t.Fatal(err)
	}
	if _, err := RoundTripReplicated(ctx, first, request); err == nil {
		t.Fatal("old TLS generation remained usable after rotation")
	}
	_ = first.Close()
	second, err := dial(secondProfile)
	if err != nil {
		t.Fatal(err)
	}
	if response, err := RoundTripReplicated(ctx, second, request); err != nil || response.Kind != ReplicatedHandshake {
		t.Fatalf("second response=%+v err=%v", response, err)
	}
	_ = second.Close()
	stats := capability.Stats()
	if stats.Generation != 2 || stats.Authenticated != 2 || stats.AuthenticationRejected == 0 {
		t.Fatalf("TLS stats=%+v", stats)
	}
	cancel()
	_ = listener.Close()
	select {
	case <-serveDone:
	case <-time.After(2 * time.Second):
		t.Fatal("authenticated server did not stop")
	}
}

func TestReplicatedGatewayAllowlistRequiresUniqueBinaryNodes(t *testing.T) {
	node := rafttransport.NodeID{1}
	allowed, err := replicatedGatewayAllowlist([]rafttransport.NodeID{node, {2}})
	if err != nil || len(allowed) != 2 {
		t.Fatalf("allowlist = %v, %v", allowed, err)
	}
	for _, input := range [][]rafttransport.NodeID{nil, {{0}}, {node, node}} {
		if _, err := replicatedGatewayAllowlist(input); !errors.Is(err, ErrReplicatedAuthentication) {
			t.Fatalf("input %v error = %v", input, err)
		}
	}
}
