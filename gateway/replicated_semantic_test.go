package gateway

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
)

type semanticGatewayOwner struct {
	*raftservice.Owner
	state  raftservice.ServingState
	probes int
}

func (owner *semanticGatewayOwner) Probe(context.Context, raftmember.GroupKey) (raftservice.ServingState, error) {
	owner.probes++
	return owner.state, nil
}

type semanticRecordingRemote struct {
	request *shardservice.ReplicatedRequest
	state   shardservice.ReplicatedMemberState
}

func (remote *semanticRecordingRemote) DoReplicated(_ context.Context, endpoint ReplicatedEndpoint, request *shardservice.ReplicatedRequest) (*shardservice.ReplicatedResponse, error) {
	remote.request = request
	state := remote.state
	state.Fence.MemberID, state.Fence.StoreID, state.Fence.NodeIncarnation = endpoint.Member, endpoint.StoreID, endpoint.NodeIncarnation
	var encoded bytes.Buffer
	if err := shardservice.EncodeResponse(&encoded, shardservice.RowsResponse([]shardservice.Column{{Name: "value"}}, [][]shardservice.Cell{{{Bytes: []byte(`"remote"`)}}})); err != nil {
		return nil, err
	}
	return &shardservice.ReplicatedResponse{Kind: shardservice.ReplicatedQueryResult, HasState: true, State: state, ReadApplied: state.Applied, Value: encoded.Bytes()}, nil
}

func TestReplicatedNodeClientPhysicalSelectionIdentityAndEncodingStats(t *testing.T) {
	route, _, states := testReplicatedRouteCommand(t)
	endpoint := route.Replicas[0]
	wireState := states[endpoint.Address]
	state := raftservice.ServingState{Identity: raftmember.RuntimeIdentity{Group: wireState.Fence.Group,
		AllocationGeneration: wireState.Fence.AllocationGeneration, MemberID: endpoint.Member, StoreID: endpoint.StoreID, NodeIncarnation: endpoint.NodeIncarnation},
		Command: wireState.Fence.Command, Status: raftmember.RuntimeStatus{MemberID: endpoint.Member, LeaderID: endpoint.Member, Term: wireState.Fence.Term, Applied: wireState.Applied, Commit: wireState.Commit, CheckpointApplied: wireState.CheckpointApplied}}
	owner := &semanticGatewayOwner{state: state}
	server, err := shardservice.NewReplicatedServer(owner, shardservice.DefaultReplicatedInFlightFrameBytes, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	credentials := newGatewayTLSAuthority(t)
	domain := rafttransport.TrustDomain{ClusterID: route.Group.ClusterID, ClusterIncarnation: route.Group.ClusterIncarnation}
	storage := credentials.profile(t, rafttransport.PeerIdentity{TrustDomain: domain, Node: endpoint.Node})
	principal := credentials.profile(t, rafttransport.PeerIdentity{TrustDomain: domain, Node: rafttransport.NodeID{92}})
	actor := serviceauthz.Authority{Node: rafttransport.NodeID{93}, Generation: 1}
	capability, err := shardservice.NewReplicatedServerTLS(storage, []rafttransport.NodeID{principal.LocalIdentity().Node})
	if err != nil {
		t.Fatal(err)
	}
	policy, _ := serviceauthz.NewPolicy(1, []serviceauthz.Entry{
		{Node: principal.LocalIdentity().Node, Capabilities: serviceauthz.CapabilityDelegate},
		{Node: actor.Node, Capabilities: serviceauthz.CapabilityDataRead},
	})
	gate, _ := serviceauthz.NewGate(policy)
	if err := server.BindAuthorization(gate, nil); err != nil {
		t.Fatal(err)
	}
	remote := &semanticRecordingRemote{state: wireState}
	client, err := NewReplicatedNodeClient(capability, principal, server, remote)
	if err != nil {
		t.Fatal(err)
	}
	probe := shardservice.ReplicatedRequest{Operation: shardservice.ReplicatedProbe, Authority: actor, Capability: serviceauthz.CapabilityDataRead,
		Fence: shardservice.ReplicatedFence{Group: route.Group, AllocationGeneration: route.AllocationGeneration}}
	if _, err := client.DoReplicated(t.Context(), endpoint, &probe); err != nil {
		t.Fatal(err)
	}
	if stats := client.Stats(); stats.LocalCalls != 1 || stats.LegacyCalls != 1 || stats.RemoteCalls != 0 || stats.SemanticSQLCalls != 0 {
		t.Fatalf("legacy local double-counted: %+v", stats)
	}
	call := shardservice.ReplicatedCall{Request: shardservice.ReplicatedRequest{Operation: shardservice.ReplicatedQueryLeader,
		Authority: actor, Capability: serviceauthz.CapabilityDataRead, Fence: wireState.Fence, MaxValueBytes: 4096},
		SQL: &shardservice.ShardRequest{Authority: actor, SQL: "SELECT 1"}}
	other := endpoint
	other.Node[0] ^= 0x40 // Address is deliberately identical to the local listener.
	reply, err := client.DoReplicatedCall(t.Context(), other, &call)
	if err != nil || reply.SQL == nil || string(reply.SQL.Rows[0][0].Bytes) != `"remote"` {
		t.Fatalf("remote reply=%+v err=%v", reply, err)
	}
	if owner.probes != 1 || remote.request == nil || len(remote.request.Query) == 0 {
		t.Fatal("address equality selected local service")
	}
	if stats := client.Stats(); stats.RemoteCalls != 1 || stats.LocalCalls != 1 || stats.SemanticSQLCalls != 1 || stats.SQLRequestEncodings != 1 || stats.SQLRequestEncodedBytes != uint64(len(remote.request.Query)) {
		t.Fatalf("actual encoding stats=%+v", stats)
	}
	if len(call.Request.Query) != 0 {
		t.Fatal("remote adapter mutated caller's semantic envelope")
	}
	wrong := endpoint
	wrong.StoreID[0] ^= 1
	if _, err := client.DoReplicated(t.Context(), wrong, &probe); !errors.Is(err, ErrReplicatedRoute) {
		t.Fatalf("wrong store reply error=%v", err)
	}
	if got := server.Stats().InFlightFrameBytes; got != 0 {
		t.Fatalf("identity rejection leaked %d bytes", got)
	}
}

func TestReplicatedSemanticClientsRejectTypedNil(t *testing.T) {
	var client *ReplicatedNodeClient
	if _, err := client.DoReplicatedCall(t.Context(), ReplicatedEndpoint{}, &shardservice.ReplicatedCall{}); err == nil {
		t.Fatal("nil node client accepted")
	}
	var remote *AuthenticatedReplicatedClient
	if _, err := remote.DoReplicatedCall(t.Context(), ReplicatedEndpoint{}, &shardservice.ReplicatedCall{}); err == nil {
		t.Fatal("nil authenticated client accepted")
	}
	if stats := client.Stats(); stats != (ReplicatedNodeClientStats{}) {
		t.Fatal(stats)
	}
}
