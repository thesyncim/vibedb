package main

import (
	"context"
	"encoding/asn1"
	"errors"
	"net"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/membershipgrant"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/rebalance"
	"github.com/thesyncim/vibedb/internal/rebalanceexec"
	"github.com/thesyncim/vibedb/internal/replicaaction"
	"github.com/thesyncim/vibedb/internal/replicacontrol"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/rf3testfixture"
	"github.com/thesyncim/vibedb/internal/servicetls"
	"github.com/thesyncim/vibedb/shardservice"
)

type gatewayTestGrantSource struct{ grant membershipgrant.Grant }

func (source gatewayTestGrantSource) ReadMembershipGrant(
	context.Context, raftmember.GroupKey,
) (membershipgrant.Grant, bool, error) {
	return source.grant, true, nil
}

type gatewayTestGrantInstaller struct {
	nodes  []rafttransport.NodeID
	failAt int
}

func (installer *gatewayTestGrantInstaller) InstallMembershipGrant(
	_ context.Context, node rafttransport.NodeID, _ membershipgrant.Grant,
) error {
	installer.nodes = append(installer.nodes, node)
	if installer.failAt != 0 && len(installer.nodes) == installer.failAt {
		return errors.New("injected install failure")
	}
	return nil
}

type gatewayTestMembershipApplier struct{ calls int }

func (applier *gatewayTestMembershipApplier) ApplyMembership(
	context.Context, gateway.ReplicatedMembershipRoute, shardservice.ReplicatedMembershipRequest,
) (gateway.ReplicatedMembershipResult, error) {
	applier.calls++
	return gateway.ReplicatedMembershipResult{}, nil
}

func TestGatewayGrantedMembershipInstallsEveryPeerBeforeProposal(t *testing.T) {
	grant, route, request := gatewayMembershipFixture()
	installer := new(gatewayTestGrantInstaller)
	applier := new(gatewayTestMembershipApplier)
	client := gatewayGrantedMembershipClient{grants: gatewayTestGrantSource{grant},
		installer: installer, applier: applier}
	if _, err := client.ApplyMembership(t.Context(), route, request); err != nil {
		t.Fatal(err)
	}
	want := []rafttransport.NodeID{{1}, {2}, {3}, {4}}
	if !slices.Equal(installer.nodes, want) || applier.calls != 1 {
		t.Fatalf("installed=%v apply=%d", installer.nodes, applier.calls)
	}
	installer.nodes = nil
	installer.failAt = 3
	if _, err := client.ApplyMembership(t.Context(), route, request); err != nil || applier.calls != 2 {
		t.Fatalf("one failed voter err=%v apply=%d", err, applier.calls)
	}
	installer.nodes = nil
	installer.failAt = 4
	if _, err := client.ApplyMembership(t.Context(), route, request); err == nil || applier.calls != 2 {
		t.Fatalf("missing target grant err=%v apply=%d", err, applier.calls)
	}
}

func gatewayMembershipFixture() (membershipgrant.Grant, gateway.ReplicatedMembershipRoute,
	shardservice.ReplicatedMembershipRequest) {
	group := raftmember.GroupKey{ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2},
		TopologyRecoveryEpoch: 3, ShardIncarnation: [16]byte{4}, GroupID: [16]byte{5}}
	voters := [3]membershipgrant.RosterMember{{Member: 1, Node: [16]byte{1}},
		{Member: 2, Node: [16]byte{2}}, {Member: 3, Node: [16]byte{3}}}
	grant := membershipgrant.Grant{Group: group, TransitionID: [16]byte{6}, MetadataEpoch: 7,
		CatalogGeneration: 8, InitialReplicaSetVersion: 9, InitialVoters: [3]uint64{1, 2, 3},
		InitialRosterDigest:     membershipgrant.CertifiedRosterDigest(group, 9, voters),
		InitialDescriptorDigest: [32]byte{10}, SourceMember: 1, TargetMember: 4,
		TargetNode: [16]byte{4}}
	route := gateway.ReplicatedMembershipRoute{Serving: gateway.ReplicatedRoute{Group: group,
		Replicas: []gateway.ReplicatedEndpoint{{Member: 1, Node: [16]byte{1}},
			{Member: 2, Node: [16]byte{2}}, {Member: 3, Node: [16]byte{3}}}},
		EnrolledTarget: gateway.ReplicatedEndpoint{Member: 4, Node: [16]byte{4}}, HasEnrolledTarget: true}
	request := shardservice.ReplicatedMembershipRequest{Kind: raftservice.MembershipAddLearner,
		TransitionID: grant.TransitionID, MetadataEpoch: grant.MetadataEpoch,
		CatalogGeneration: grant.CatalogGeneration, ExpectedReplicaSetVersion: 9,
		SourceMember: 1, TargetMember: 4}
	return grant, route, request
}

func TestGatewayGrantedMembershipInstallsPublishedTargetBeforeSourceRemoval(t *testing.T) {
	grant, route, request := gatewayMembershipFixture()
	route.Serving.Replicas[0] = route.EnrolledTarget
	route.HasEnrolledTarget = false
	route.EnrolledTarget = gateway.ReplicatedEndpoint{}
	request.Kind = raftservice.MembershipRemoveVoter
	request.TransferTerm = 2
	installer := new(gatewayTestGrantInstaller)
	applier := new(gatewayTestMembershipApplier)
	client := gatewayGrantedMembershipClient{grants: gatewayTestGrantSource{grant}, installer: installer, applier: applier}
	if _, err := client.ApplyMembership(t.Context(), route, request); err != nil {
		t.Fatalf("published target lost its membership grant route: %v", err)
	}
	if applier.calls != 1 || !slices.Contains(installer.nodes, rafttransport.NodeID(grant.TargetNode)) {
		t.Fatalf("target not installed before removal: nodes=%v calls=%d", installer.nodes, applier.calls)
	}
}

type gatewayTestObservationClient struct{ observation replicacontrol.Observation }

func (client gatewayTestObservationClient) Observe(
	_ context.Context, _ rafttransport.NodeID, request replicacontrol.Request,
) (replicacontrol.Observation, error) {
	result := client.observation
	result.Request = request
	return result, nil
}

type gatewayTestActionClient struct {
	node    rafttransport.NodeID
	request replicaaction.Request
	err     error
}

type gatewayTestMembershipLeader struct {
	state shardservice.ReplicatedMemberState
}

func (client gatewayTestMembershipLeader) ObserveMembershipLeader(context.Context, gateway.ReplicatedMembershipRoute) (shardservice.ReplicatedMemberState, error) {
	return client.state, nil
}

func (client *gatewayTestActionClient) Execute(
	_ context.Context, node rafttransport.NodeID, request replicaaction.Request,
) error {
	if _, err := replicaaction.AppendRequest(nil, request); err != nil {
		return err
	}
	client.node, client.request = node, request
	return client.err
}

func TestGatewayReplicaRemoteActionsBuildExactOwnershipAndRetirementFences(t *testing.T) {
	binding := replicatedstate.Binding{ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2},
		TopologyRecoveryEpoch: 3, Distribution: "data", Shard: "all",
		AllocationGeneration: 5, ShardIncarnation: [16]byte{4}, GroupID: [16]byte{6},
		ActivePolicyGeneration: 7, ProtectionEpoch: 8, OwnershipEpoch: 9,
		SchemaGeneration: 10, RoutingVersion: 11, RouteGeneration: 12,
		OwnedRange: distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}}}
	command, err := replicatedstate.AppendOwnershipTransition(nil, replicatedstate.OwnershipTransition{
		From: binding, ExpectedReplicaSetVersion: 13, SourceMember: 1, TargetMember: 4,
		ToOwnershipEpoch: 10, ToRoutingVersion: 12, ToRouteGeneration: 13,
		ToOwnedRange: binding.OwnedRange})
	if err != nil {
		t.Fatal(err)
	}
	commandFence := raftservice.CommandFence{ReplicaSetVersion: 13,
		ActivePolicyGeneration: 7, ProtectionEpoch: 8, OwnershipEpoch: 9,
		SchemaGeneration: 10, RoutingVersion: 11, RouteGeneration: 12,
		RelationManifestDigest: [32]byte{14}}
	target := gateway.ReplicatedEndpoint{Member: 4, Node: [16]byte{4}, StoreID: [16]byte{15}, NodeIncarnation: 16}
	leader := gateway.ReplicatedEndpoint{Member: 2, Node: [16]byte{2}, StoreID: [16]byte{21}, NodeIncarnation: 22}
	route := gateway.ReplicatedMembershipRoute{Serving: gateway.ReplicatedRoute{
		Group: raftmember.GroupKey{ClusterID: binding.ClusterID, ClusterIncarnation: binding.ClusterIncarnation,
			TopologyRecoveryEpoch: 3, ShardIncarnation: binding.ShardIncarnation, GroupID: binding.GroupID},
		AllocationGeneration: 5, Command: commandFence,
		Replicas: []gateway.ReplicatedEndpoint{
			{Member: 1, Node: [16]byte{1}, StoreID: [16]byte{19}, NodeIncarnation: 20}, leader,
			{Member: 3, Node: [16]byte{3}, StoreID: [16]byte{23}, NodeIncarnation: 24},
		}}, EnrolledTarget: target, HasEnrolledTarget: true}
	actions := new(gatewayTestActionClient)
	remote := gatewayReplicaRemoteActions{observer: gatewayTestObservationClient{observation: replicacontrol.Observation{
		Status: raftmember.RuntimeStatus{MemberID: leader.Member, LeaderID: leader.Member, Term: 17},
	}}, actions: actions, native: gatewayTestMembershipLeader{state: shardservice.ReplicatedMemberState{
		LeaderID: leader.Member, Fence: shardservice.ReplicatedFence{Group: route.Serving.Group,
			AllocationGeneration: route.Serving.AllocationGeneration, Command: commandFence,
			MemberID: leader.Member, StoreID: leader.StoreID, NodeIncarnation: leader.NodeIncarnation + 1, Term: 17}}}}
	operation := rebalance.OperationID{18}
	step := [32]byte{19}
	if err = remote.ProposeReplicaMoveOwnership(t.Context(), operation, step, route, command); err != nil {
		t.Fatal(err)
	}
	if actions.node != leader.Node || actions.request.Kind != replicaaction.OwnershipTransition ||
		actions.request.Fence.Term != 17 || actions.request.SourceMember != 1 ||
		actions.request.TargetMember != 4 || actions.request.Fence.MemberID != leader.Member ||
		actions.request.Fence.StoreID != leader.StoreID ||
		actions.request.Fence.NodeIncarnation != leader.NodeIncarnation+1 {
		t.Fatalf("ownership request=%+v node=%x", actions.request, actions.node)
	}
	staleLeader := &raftservice.NotLeaderError{Status: raftmember.RuntimeStatus{
		MemberID: leader.Member, LeaderID: 3, Term: 18,
	}}
	actions.err = staleLeader
	if err = remote.ProposeReplicaMoveOwnership(
		t.Context(), operation, step, route, command,
	); err != staleLeader {
		t.Fatalf("changed leader err=%v, want exact retryable witness", err)
	}
	actions.err = nil
	source := gateway.ReplicatedEndpoint{Member: 1, Node: [16]byte{1}, StoreID: [16]byte{20}, NodeIncarnation: 21}
	if err = remote.RetireReplicaSource(t.Context(), rebalanceexec.SourceRetirementRequest{
		Operation: [32]byte(operation), Step: step, Group: route.Serving.Group,
		AllocationGeneration: 5, Command: commandFence, Source: source, Target: target, Term: 22,
	}); err != nil {
		t.Fatal(err)
	}
	if actions.node != source.Node || actions.request.Kind != replicaaction.SourceRetirement ||
		actions.request.Fence.Command != commandFence || actions.request.Fence.Term != 22 {
		t.Fatalf("retirement request=%+v node=%x", actions.request, actions.node)
	}
}

func TestGatewayShardControlOpenerBoundsAndReleasesAuthenticatedStreams(t *testing.T) {
	domain := rafttransport.TrustDomain{ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2}}
	clientNode := rafttransport.NodeID{1}
	serverNode := rafttransport.NodeID{2}
	oid := asn1.ObjectIdentifier{1, 3, 6, 1, 4, 1, 32473, 1, 1}
	credentials, roots, err := rf3testfixture.WriteCredentials(
		t.TempDir(), oid, domain, []rafttransport.NodeID{clientNode, serverNode},
	)
	if err != nil {
		t.Fatal(err)
	}
	clientTLS, err := servicetls.LoadProfile(
		credentials[0].Certificate, credentials[0].Key, roots, oid.String(), time.Now,
	)
	if err != nil {
		t.Fatal(err)
	}
	serverTLS, err := servicetls.LoadProfile(
		credentials[1].Certificate, credentials[1].Key, roots, oid.String(), time.Now,
	)
	if err != nil {
		t.Fatal(err)
	}
	deadline := func() time.Time { return time.Now().Add(5 * time.Second) }
	serverConnections := make(chan rafttransport.PeerConnection, 2)
	serverErrors := make(chan error, 2)
	var dials atomic.Int32
	dial := func(ctx context.Context, address string) (net.Conn, error) {
		if address != "authenticated-control" {
			return nil, errors.New("wrong control address")
		}
		dials.Add(1)
		client, server := net.Pipe()
		go func() {
			connection, serveErr := serverTLS.Server(
				ctx, server, rafttransport.TrafficShardControl, deadline,
			)
			if serveErr != nil {
				serverErrors <- serveErr
				return
			}
			serverConnections <- connection
		}()
		return client, nil
	}
	opener, err := newGatewayShardControlOpener(
		clientTLS, deadline, dial, []gateway.ReplicatedEndpoint{{
			Node: serverNode, ControlAddress: "authenticated-control",
		}}, 1,
	)
	if err != nil {
		t.Fatal(err)
	}
	first, err := opener.OpenShardControl(t.Context(), serverNode)
	if err != nil {
		t.Fatal(err)
	}
	firstServer := <-serverConnections

	blockedCtx, cancelBlocked := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancelBlocked()
	blocked, err := opener.OpenShardControl(blockedCtx, serverNode)
	if blocked != nil || !errors.Is(err, context.DeadlineExceeded) || dials.Load() != 1 {
		t.Fatalf("saturated open connection=%v dials=%d err=%v", blocked, dials.Load(), err)
	}
	_ = firstServer.SetWriteDeadline(time.Now())
	_ = firstServer.Close()
	_ = first.Close()
	// Close is deliberately idempotent for capacity accounting: a duplicate
	// cleanup call must not release a second semaphore slot.
	_ = first.Close()
	second, err := opener.OpenShardControl(t.Context(), serverNode)
	if err != nil || dials.Load() != 2 {
		t.Fatalf("open after release connection=%v dials=%d err=%v", second, dials.Load(), err)
	}
	secondServer := <-serverConnections
	_ = secondServer.SetWriteDeadline(time.Now())
	_ = secondServer.Close()
	_ = second.Close()
	select {
	case serveErr := <-serverErrors:
		t.Fatalf("authenticated server handshake: %v", serveErr)
	default:
	}
}
