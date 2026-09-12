package splitcontroller

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

type planObservationTestConnection struct {
	net.Conn
	peer  rafttransport.PeerIdentity
	class rafttransport.TrafficClass
}

func (connection *planObservationTestConnection) PeerIdentity() rafttransport.PeerIdentity {
	return connection.peer
}
func (*planObservationTestConnection) PeerKeyDigest() [32]byte { return [32]byte{} }
func (connection *planObservationTestConnection) TrafficClass() rafttransport.TrafficClass {
	return connection.class
}

type planObservationTestProvider struct {
	mu       sync.Mutex
	state    SourcePlanObservation
	requests []planObservationWireRequest
	badFence bool
	runtime  *ChildObservation
}

func (provider *planObservationTestProvider) ObserveSplitSource(
	_ context.Context, request PlanObservationRequest, member uint64,
) (SourcePlanObservation, error) {
	provider.mu.Lock()
	defer provider.mu.Unlock()
	provider.requests = append(provider.requests, planObservationWireRequest{
		Kind: planObservationSource, TargetMember: member, Request: request,
	})
	result := provider.state
	result.RequestDigest = request.RequestDigest
	result.Serving = servingForPlanObservation(request, member, result.State.Applied)
	result.Status = result.Serving.Status
	if provider.badFence {
		result.Serving.Command.RouteGeneration++
	}
	return result, nil
}

func (provider *planObservationTestProvider) ObserveSplitChild(
	_ context.Context, request PlanObservationRequest, member uint64,
) (ChildPlanObservation, error) {
	provider.mu.Lock()
	defer provider.mu.Unlock()
	provider.requests = append(provider.requests, planObservationWireRequest{
		Kind: planObservationChild, TargetMember: member, Request: request,
	})
	serving := servingForPlanObservation(request, member, 41)
	runtime := cloneChildPlanRuntime(provider.runtime)
	if runtime == nil {
		return ChildPlanObservation{RequestDigest: request.RequestDigest}, nil
	}
	runtime.Child = request.Child
	runtime.ReadyReplicas = []raftservice.ServingState{serving}
	return ChildPlanObservation{
		RequestDigest: request.RequestDigest,
		Runtime:       runtime,
	}, nil
}

func servingForPlanObservation(
	request PlanObservationRequest, member, applied uint64,
) raftservice.ServingState {
	var store [16]byte
	store[0] = byte(member)
	identity := raftmember.RuntimeIdentity{
		Group: request.Group, Distribution: string(request.Distribution),
		Shard: string(request.Shard), AllocationGeneration: uint64(request.Allocation),
		MemberID: member, StoreID: store, NodeIncarnation: 1,
	}
	status := raftmember.RuntimeStatus{
		MemberID: member, LeaderID: 1, Term: 7, Commit: applied, Applied: applied,
		RaftState: raft.StateFollower,
	}
	if member == 1 {
		status.RaftState = raft.StateLeader
	}
	return raftservice.ServingState{Identity: identity, Command: request.Command, Status: status}
}

type planObservationTestDirectory struct {
	peers map[distribution.EndpointID]PlanObservationPeer
}

type planObservationCatalogReader struct{ snapshot *gateway.Snapshot }

func (reader planObservationCatalogReader) Read(context.Context) (*gateway.Snapshot, error) {
	return reader.snapshot, nil
}

func (directory *planObservationTestDirectory) ResolvePlanObservationPeer(
	_ context.Context, _ PlanObservationRequest, endpoint distribution.EndpointID,
) (PlanObservationPeer, error) {
	peer, ok := directory.peers[endpoint]
	if !ok {
		return PlanObservationPeer{}, ErrPlanObservation
	}
	return peer, nil
}

type planObservationTestOpener struct {
	service    *PlanObservationService
	controller rafttransport.PeerIdentity
	peers      map[rafttransport.NodeID]rafttransport.PeerIdentity
	mu         sync.Mutex
	opened     []rafttransport.NodeID
	errors     chan error
}

func (opener *planObservationTestOpener) OpenShardControl(
	ctx context.Context, node rafttransport.NodeID,
) (rafttransport.PeerConnection, error) {
	peer, ok := opener.peers[node]
	if !ok {
		return nil, ErrPlanObservation
	}
	client, server := net.Pipe()
	opener.mu.Lock()
	opener.opened = append(opener.opened, node)
	opener.mu.Unlock()
	go func() {
		err := opener.service.Serve(ctx, &planObservationTestConnection{
			Conn: server, peer: opener.controller, class: rafttransport.TrafficShardControl,
		})
		if err != nil && opener.errors != nil {
			opener.errors <- err
		}
	}()
	return &planObservationTestConnection{
		Conn: client, peer: peer, class: rafttransport.TrafficShardControl,
	}, nil
}

func TestNetworkPlanObservationClientServesExactSourceAndMultiMemberChild(t *testing.T) {
	request, state, trust := networkPlanObservationFixture(t)
	provider := &planObservationTestProvider{state: SourcePlanObservation{State: state}}
	service, client, serverErrors := networkPlanObservationPair(t, request, trust, provider, MaxPlanObservationResponseBytes)
	_ = service

	source, err := client.ObserveSplitSource(t.Context(), request)
	if err != nil || source.State.Applied != state.Applied ||
		source.State.Binding != state.Binding ||
		source.Serving.Status.RaftState != raft.StateLeader ||
		source.RequestDigest != request.RequestDigest {
		t.Fatalf("source=%+v err=%v", source, err)
	}
	childRequest := request
	childRequest.Child = 1
	childRequest.RequestDigest = planObservationRequestDigest(childRequest)
	child, err := client.ObserveSplitChild(t.Context(), childRequest)
	if err != nil || child.Runtime != nil || child.RequestDigest != childRequest.RequestDigest {
		select {
		case serverErr := <-serverErrors:
			t.Logf("server error: %v", serverErr)
		default:
		}
		t.Fatalf("child=%+v err=%v", child, err)
	}
	provider.mu.Lock()
	defer provider.mu.Unlock()
	if len(provider.requests) != 4 {
		t.Fatalf("provider requests=%d want=4", len(provider.requests))
	}
}

func TestPlanObservationServiceRejectsWrongServingFence(t *testing.T) {
	request, state, trust := networkPlanObservationFixture(t)
	provider := &planObservationTestProvider{
		state: SourcePlanObservation{State: state}, badFence: true,
	}
	_, client, _ := networkPlanObservationPair(t, request, trust, provider, MaxPlanObservationResponseBytes)
	if _, err := client.ObserveSplitSource(t.Context(), request); !errors.Is(err, ErrPlanObservation) {
		t.Fatalf("wrong-fence error=%v", err)
	}
}

func TestCatalogPlanObservationPeerDirectoryBindsExactRF3Cut(t *testing.T) {
	manifest, err := distribution.NewManifest("orders", 11, []distribution.Shard{{
		ID: "source", AllocationGeneration: 7,
		Range:   distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}},
		Leaders: []distribution.EndpointID{"data-a", "data-b", "data-c"}, Epoch: 5,
	}})
	if err != nil {
		t.Fatal(err)
	}
	group := raftmember.GroupKey{
		ClusterID: testID(1), ClusterIncarnation: testID(2), TopologyRecoveryEpoch: 1,
		ShardIncarnation: testID(3), GroupID: testID(4),
	}
	command := raftservice.CommandFence{
		ReplicaSetVersion: 1, ActivePolicyGeneration: 1, ProtectionEpoch: 1,
		OwnershipEpoch: 5, SchemaGeneration: 1,
		RelationManifestDigest: sha256.Sum256([]byte("relations")),
		RoutingVersion:         11, RouteGeneration: 19,
	}
	addresses := make(map[distribution.EndpointID]string)
	replicas := make([]gateway.ReplicatedReplicaDescriptor, 3)
	for index, suffix := range []string{"a", "b", "c"} {
		data := distribution.EndpointID("data-" + suffix)
		native := distribution.EndpointID("native-" + suffix)
		control := distribution.EndpointID("control-" + suffix)
		addresses[data], addresses[native], addresses[control] =
			"127.0.0.1:1", "127.0.0.1:2", "127.0.0.1:3"
		replicas[index] = gateway.ReplicatedReplicaDescriptor{
			Member: uint64(index + 1), Node: rafttransport.NodeID(testID(byte(10 + index))),
			StoreID: testID(byte(20 + index)), NodeIncarnation: 1,
			Endpoint: data, NativeEndpoint: native, ControlEndpoint: control,
		}
	}
	snapshot, err := gateway.NewSnapshotWithReplicatedMetadata(
		distribution.ClusterConfig{
			Distributions: []distribution.DistributionSpec{{
				Name: "orders", Arity: 1, MapperVersion: distribution.NativeMapperVersion,
			}},
			Placements: []distribution.TablePlacement{{
				Table: "docs", Distribution: "orders", Columns: []string{"/tenant"},
			}},
			Manifests: []*distribution.Manifest{manifest},
		},
		addresses, 19, nil, nil,
		[]gateway.ReplicatedShardDescriptor{{
			Distribution: "orders", Shard: "source", Group: group,
			AllocationGeneration: 7, Command: command, Replicas: replicas,
			RangeIdentity: replication.Digest{0x71}, LineageDigest: replication.Digest{0x72},
			ForwardingRuleDigest: replication.Digest{0x73},
			RequestLedgerRanges: []gateway.DurableRequestLedgerRangeDescriptor{{
				Identity: replication.Digest{0x91},
			}},
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := gateway.CatalogSnapshotDigest(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	request := PlanObservationRequest{
		Operation:         OperationID(sha256.Sum256([]byte("operation"))),
		CatalogGeneration: 19, CatalogDigest: digest, Distribution: "orders",
		Shard: "source", Allocation: 7, Group: group, Command: command,
		ControlEndpoints: []distribution.EndpointID{"control-a", "control-b", "control-c"},
	}
	request.RequestDigest = planObservationRequestDigest(request)
	directory, err := NewCatalogPlanObservationPeerDirectory(planObservationCatalogReader{snapshot})
	if err != nil {
		t.Fatal(err)
	}
	peer, err := directory.ResolvePlanObservationPeer(t.Context(), request, "control-b")
	if err != nil || peer.MemberID != 2 || peer.Node != replicas[1].Node {
		t.Fatalf("peer=%+v err=%v", peer, err)
	}
	request.Command.RouteGeneration++
	request.RequestDigest = planObservationRequestDigest(request)
	if _, err = directory.ResolvePlanObservationPeer(t.Context(), request, "control-b"); !errors.Is(err, ErrPlanObservation) {
		t.Fatalf("wrong command error=%v", err)
	}
}

func TestPlanObservationResponseBoundFailsBeforeWrite(t *testing.T) {
	request, state, trust := networkPlanObservationFixture(t)
	provider := &planObservationTestProvider{state: SourcePlanObservation{State: state}}
	_, client, _ := networkPlanObservationPair(t, request, trust, provider, 64)
	if _, err := client.ObserveSplitSource(t.Context(), request); !errors.Is(err, ErrPlanObservation) {
		t.Fatalf("response-bound error=%v", err)
	}
}

func TestPlanObservationWireRejectsNonCanonicalAndDigestDrift(t *testing.T) {
	request, _, _ := networkPlanObservationFixture(t)
	wire := planObservationWireRequest{
		Format: planObservationWireFormat, Kind: planObservationSource,
		TargetMember: 1, Request: request,
	}
	var buffer bytesBuffer
	if err := writePlanObservationRequest(&buffer, wire); err != nil {
		t.Fatal(err)
	}
	raw := append([]byte(nil), buffer.raw...)
	raw[len(raw)-1] = ' '
	if _, err := readPlanObservationRequest(&bytesBuffer{raw: raw}); !errors.Is(err, ErrPlanObservation) {
		t.Fatalf("noncanonical error=%v", err)
	}
	wire.Request.CatalogGeneration++
	if err := writePlanObservationRequest(&bytesBuffer{}, wire); !errors.Is(err, ErrPlanObservation) {
		t.Fatalf("digest-drift error=%v", err)
	}
}

func TestMergeChildPlanObservationsRequiresIdenticalLifecycleAndDistinctMembers(t *testing.T) {
	request, _, _ := networkPlanObservationFixture(t)
	request.Child = 1
	request.RequestDigest = planObservationRequestDigest(request)
	results := make([]planObservationMemberResult, 3)
	for index := range results {
		results[index].cut = ChildPlanObservation{
			RequestDigest: request.RequestDigest,
			Runtime: &ChildObservation{
				Child: 1, Phase: ChildPhaseRuntimeAdopted,
				ReadyReplicas: []raftservice.ServingState{
					servingForPlanObservation(request, uint64(index+1), 41),
				},
			},
		}
	}
	merged, err := mergeChildPlanObservations(request, results)
	if err != nil || merged.Runtime == nil || len(merged.Runtime.ReadyReplicas) != 3 {
		t.Fatalf("merged=%+v err=%v", merged, err)
	}
	results[2].cut.Runtime.Phase = ChildPhaseWALCreated
	if _, err = mergeChildPlanObservations(request, results); !errors.Is(err, ErrPlanObservation) {
		t.Fatalf("lifecycle disagreement error=%v", err)
	}
	results[2].cut.Runtime.Phase = ChildPhaseRuntimeAdopted
	results[2].cut.Runtime.ReadyReplicas[0] = results[1].cut.Runtime.ReadyReplicas[0]
	if _, err = mergeChildPlanObservations(request, results); !errors.Is(err, ErrPlanObservation) {
		t.Fatalf("duplicate member error=%v", err)
	}
}

type bytesBuffer struct {
	raw []byte
	at  int
}

func (buffer *bytesBuffer) Write(raw []byte) (int, error) {
	buffer.raw = append(buffer.raw, raw...)
	return len(raw), nil
}
func (buffer *bytesBuffer) Read(dst []byte) (int, error) {
	if buffer.at == len(buffer.raw) {
		return 0, io.EOF
	}
	n := copy(dst, buffer.raw[buffer.at:])
	buffer.at += n
	return n, nil
}

func TestPlanObservationSourceSealIsExactReadOnlyScope(t *testing.T) {
	request, state, _ := networkPlanObservationFixture(t)
	transition := PlanObservationSourceTransition{From: request.Command, To: request.Command,
		Range: distribution.KeyRange{End: distribution.KeyspaceEnd{Point: distribution.KeyspacePoint{128}}}}
	transition.To.OwnershipEpoch++
	transition.To.RoutingVersion++
	transition.To.RouteGeneration++
	request.SourceTransition = &transition
	request.RequestDigest = planObservationRequestDigest(request)
	state.Binding.OwnershipEpoch = transition.To.OwnershipEpoch
	state.Binding.RoutingVersion = transition.To.RoutingVersion
	state.Binding.RouteGeneration = transition.To.RouteGeneration
	state.Binding.OwnedRange = transition.Range
	cut := SourcePlanObservation{State: state, Serving: servingForPlanObservation(request, 1, state.Applied)}
	cut.Serving.Command = transition.To
	wire := planObservationWireRequest{Request: request, TargetMember: 1}
	if !validNetworkSourceObservation(wire, cut) {
		t.Fatal("exact source seal cannot be observed before catalog publication")
	}
	if validNetworkChildObservation(wire, ChildPlanObservation{}) {
		t.Fatal("source transition widened child observation authority")
	}
	for _, mutate := range []func(*SourcePlanObservation){
		func(c *SourcePlanObservation) { c.State.Binding.OwnedRange.Start[0]++ },
		func(c *SourcePlanObservation) { c.State.Binding.OwnershipEpoch++ },
		func(c *SourcePlanObservation) { c.Serving.Command.RouteGeneration++ },
		func(c *SourcePlanObservation) { c.State.ReplicaSetVersion++ },
		func(c *SourcePlanObservation) { c.Serving.Command.RelationManifestDigest[0]++ },
		func(c *SourcePlanObservation) { c.State.Binding.GroupID[0]++ },
	} {
		forged := cut
		mutate(&forged)
		if validNetworkSourceObservation(wire, forged) {
			t.Fatal("substituted sealed source passed exact observation")
		}
	}
	for _, mutate := range []func(*PlanObservationSourceTransition){
		func(s *PlanObservationSourceTransition) { s.To.OwnershipEpoch++ },
		func(s *PlanObservationSourceTransition) { s.To.SchemaGeneration++ },
		func(s *PlanObservationSourceTransition) { s.To.ProtectionEpoch++ },
		func(s *PlanObservationSourceTransition) { s.To.ReplicaSetVersion++ },
		func(s *PlanObservationSourceTransition) { s.To.RelationManifestDigest[0]++ },
		func(s *PlanObservationSourceTransition) { s.Range = distribution.KeyRange{} },
	} {
		forged := transition
		mutate(&forged)
		candidate := request
		candidate.SourceTransition = &forged
		if validNetworkPlanObservationRequest(candidate) {
			t.Fatal("source transition substitution escaped request digest")
		}
		candidate.RequestDigest = planObservationRequestDigest(candidate)
		if validNetworkPlanObservationRequest(candidate) {
			t.Fatal("source transition changed non-ownership authority")
		}
	}
}

func networkPlanObservationFixture(
	t testing.TB,
) (PlanObservationRequest, replicatedstate.State, rafttransport.TrustDomain) {
	t.Helper()
	plan, snapshot, target, _ := testPlan(t)
	state := testSourceState(plan)
	state.ReplicaSetVersion = 1
	state.LastKind = replicatedstate.RecordNormal
	state.LastEntryType = pb.EntryNormal
	state.ApplyContractDigest = sha256.Sum256([]byte("apply-contract"))
	state.ConfState = &pb.ConfState{Voters: []uint64{1, 2, 3}}
	group := raftmember.GroupKey{
		ClusterID:             [16]byte(state.Binding.ClusterID),
		ClusterIncarnation:    [16]byte(state.Binding.ClusterIncarnation),
		TopologyRecoveryEpoch: state.Binding.TopologyRecoveryEpoch,
		ShardIncarnation:      [16]byte(state.Binding.ShardIncarnation),
		GroupID:               [16]byte(state.Binding.GroupID),
	}
	command := raftservice.CommandFence{
		ReplicaSetVersion:      state.ReplicaSetVersion,
		ActivePolicyGeneration: state.Binding.ActivePolicyGeneration,
		ProtectionEpoch:        state.Binding.ProtectionEpoch,
		OwnershipEpoch:         state.Binding.OwnershipEpoch,
		SchemaGeneration:       state.Binding.SchemaGeneration,
		RelationManifestDigest: target.RelationManifestDigest,
		RoutingVersion:         state.Binding.RoutingVersion,
		RouteGeneration:        state.Binding.RouteGeneration,
	}
	digest, err := gateway.CatalogSnapshotDigest(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	request := PlanObservationRequest{
		Operation: plan.OperationID(), CatalogGeneration: snapshot.Generation(),
		CatalogDigest: digest, Distribution: plan.source.Distribution,
		Shard: plan.source.Shard, Allocation: plan.source.AllocationGeneration,
		Group: group, Command: command,
		ControlEndpoints: []distribution.EndpointID{"control-a", "control-b", "control-c"},
	}
	request.RequestDigest = planObservationRequestDigest(request)
	return request, state, rafttransport.TrustDomain{
		ClusterID: group.ClusterID, ClusterIncarnation: group.ClusterIncarnation,
	}
}

func networkPlanObservationPair(
	t testing.TB, request PlanObservationRequest, trust rafttransport.TrustDomain,
	provider *planObservationTestProvider, maxResponse int,
) (*PlanObservationService, *NetworkPlanObservationClient, <-chan error) {
	t.Helper()
	deadline := func() time.Time { return time.Now().Add(5 * time.Second) }
	service, err := NewPlanObservationService(PlanObservationServiceOptions{
		Provider: provider, Authorize: func(
			peer rafttransport.PeerIdentity, got PlanObservationRequest, member uint64, _ bool,
		) bool {
			return peer.Node != (rafttransport.NodeID{}) && got.Operation == request.Operation &&
				got.CatalogDigest == request.CatalogDigest && member != 0
		},
		ReadDeadline: deadline, WriteDeadline: deadline,
		MaxConcurrent: 4, MaxResponseBytes: maxResponse,
	})
	if err != nil {
		t.Fatal(err)
	}
	controllerNode := rafttransport.NodeID(testID(70))
	controller := rafttransport.PeerIdentity{TrustDomain: trust, Node: controllerNode}
	directory := &planObservationTestDirectory{peers: make(map[distribution.EndpointID]PlanObservationPeer)}
	opener := &planObservationTestOpener{
		service: service, controller: controller,
		peers:  make(map[rafttransport.NodeID]rafttransport.PeerIdentity),
		errors: make(chan error, 32),
	}
	for index, endpoint := range request.ControlEndpoints {
		node := rafttransport.NodeID(testID(byte(80 + index)))
		directory.peers[endpoint] = PlanObservationPeer{Node: node, MemberID: uint64(index + 1)}
		opener.peers[node] = rafttransport.PeerIdentity{TrustDomain: trust, Node: node}
	}
	client, err := NewNetworkPlanObservationClient(NetworkPlanObservationClientOptions{
		Opener: opener, Directory: directory, ReadDeadline: deadline, WriteDeadline: deadline,
		MaxConcurrent: 3, MaxResponseBytes: maxResponse,
	})
	if err != nil {
		t.Fatal(err)
	}
	return service, client, opener.errors
}
