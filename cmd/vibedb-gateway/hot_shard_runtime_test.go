package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/hotshard"
	"github.com/thesyncim/vibedb/internal/membershipgrant"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/rebalance"
	"github.com/thesyncim/vibedb/internal/replicacontrol"
	"github.com/thesyncim/vibedb/internal/topologyscheduler"
	pb "go.etcd.io/raft/v3/raftpb"
)

func TestGatewayHotShardCapacityRequiresCanonicalBoundedFile(t *testing.T) {
	var capacity autosplit.CapacityVector
	for resource := range autosplit.ResourceCount {
		capacity[resource] = 100
	}
	config := hotshard.StaticCapacityConfig{Format: hotshard.StaticCapacityFormat,
		RecorderLanes: 2, WindowCapacity: capacity, NodeCapacity: capacity,
		MigrationCapacity: 1024, ShardMigrationBytes: 512, MaxReceives: 1,
		Nodes: []hotshard.StaticCapacityNode{{Endpoint: "member-1", FailureDomain: 1}}}
	raw, err := hotshard.AppendStaticCapacityConfig(nil, config)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "capacity.vibejson")
	if err = os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err = loadGatewayHotShardCapacity(path); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, append(raw, ' '), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err = loadGatewayHotShardCapacity(path); err == nil {
		t.Fatal("noncanonical capacity file accepted")
	}
}

func TestGatewayHotShardServeModeRequiresExactReplicatedOperationAuthority(t *testing.T) {
	for _, test := range []struct {
		name                 string
		capacity, control    string
		devStatic, devPlain  bool
		wantMissingAuthority bool
	}{
		{name: "disabled"},
		{name: "authenticated", capacity: "capacity.vibejson", control: "replicas.vibejson"},
		{name: "missing-control", capacity: "capacity.vibejson", wantMissingAuthority: true},
		{name: "local-catalog", capacity: "capacity.vibejson", control: "replicas.vibejson", devStatic: true, wantMissingAuthority: true},
		{name: "plaintext", capacity: "capacity.vibejson", control: "replicas.vibejson", devPlain: true, wantMissingAuthority: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := validateGatewayHotShardServeMode(
				test.capacity, test.control, test.devStatic, test.devPlain,
			)
			if errors.Is(err, errGatewayHotShardMissingOperationAuthority) != test.wantMissingAuthority {
				t.Fatalf("error=%v want_missing_authority=%t", err, test.wantMissingAuthority)
			}
		})
	}
}

type gatewayHotShardTestAuthority struct {
	record gateway.ReplicatedPressureRecord
	reads  int
	writes int
}

func (authority *gatewayHotShardTestAuthority) ReadPressureRecord(
	context.Context,
) (gateway.ReplicatedPressureRecord, error) {
	authority.reads++
	if authority.record.AuthorityRevision == 0 {
		return gateway.ReplicatedPressureRecord{}, gateway.ErrReplicatedPressureMissing
	}
	return authority.record, nil
}

func (authority *gatewayHotShardTestAuthority) PublishPressureRecord(
	context.Context, uint64, gateway.ReplicatedPressureRecord,
) error {
	authority.writes++
	return errors.New("unexpected pressure publication")
}

func (*gatewayHotShardTestAuthority) RetryPending(context.Context) error { return nil }

type gatewayHotMoveObservations struct {
	publication raftmodel.Publication
	grant       membershipgrant.Grant
}

func (observations gatewayHotMoveObservations) ReadMembershipGrant(
	_ context.Context, group raftmember.GroupKey,
) (membershipgrant.Grant, bool, error) {
	return observations.grant, observations.grant.Group == group, nil
}

func (observations gatewayHotMoveObservations) Observe(
	_ context.Context, node rafttransport.NodeID, request replicacontrol.Request,
) (replicacontrol.Observation, error) {
	if node != (rafttransport.NodeID{2}) {
		return replicacontrol.Observation{}, errors.New("not leader")
	}
	return replicacontrol.Observation{Request: request, Publication: observations.publication,
		Status: raftmember.RuntimeStatus{MemberID: 2, LeaderID: 2, Term: 4}}, nil
}

type gatewayHotMoveSubmitter struct {
	fail       bool
	operations []rebalance.OperationID
}

func (submitter *gatewayHotMoveSubmitter) Submit(
	_ context.Context, plan *rebalance.Plan,
) (rebalance.Action, error) {
	if plan == nil {
		return rebalance.Action{}, errors.New("nil plan")
	}
	submitter.operations = append(submitter.operations, plan.OperationID())
	if submitter.fail {
		return rebalance.Action{}, errors.New("outcome unknown")
	}
	return rebalance.Action{Kind: rebalance.ActionAddLearner}, nil
}

func TestGatewayHotShardPressurePassCreatesExactEnrolledReplicaMove(t *testing.T) {
	catalog, record, observations := gatewayHotShardMoveFixture(t)
	controller, err := hotshard.New(hotshard.DefaultPolicy())
	if err != nil {
		t.Fatal(err)
	}
	submitter := &gatewayHotMoveSubmitter{}
	runtime := &gatewayHotShardRuntime{authority: &gatewayHotShardTestAuthority{record: record},
		controller: controller, operationsBound: true,
		operations: gatewayHotShardOperationAuthorities{
			moves:   gatewayHotReplicaMoveFactory{observations: observations, grants: observations},
			moveRun: submitter,
		}}
	pass, err := runtime.runPressurePass(context.Background(), catalog)
	if err != nil || pass.Admission.MoveCount != 1 || pass.Admission.SplitCount != 0 ||
		len(submitter.operations) != 1 || submitter.operations[0] == (rebalance.OperationID{}) {
		t.Fatalf("pass=%+v operations=%x err=%v", pass, submitter.operations, err)
	}
}

func TestGatewayHotShardOutcomeUnknownRestartRetriesSameOperation(t *testing.T) {
	catalog, record, observations := gatewayHotShardMoveFixture(t)
	submitter := &gatewayHotMoveSubmitter{fail: true}
	makeRuntime := func() *gatewayHotShardRuntime {
		controller, err := hotshard.New(hotshard.DefaultPolicy())
		if err != nil {
			t.Fatal(err)
		}
		return &gatewayHotShardRuntime{authority: &gatewayHotShardTestAuthority{record: record},
			controller: controller, operationsBound: true,
			operations: gatewayHotShardOperationAuthorities{
				moves:   gatewayHotReplicaMoveFactory{observations: observations, grants: observations},
				moveRun: submitter,
			}}
	}
	if _, err := makeRuntime().runPressurePass(context.Background(), catalog); err == nil {
		t.Fatal("outcome-unknown submission reported success")
	}
	submitter.fail = false
	pass, err := makeRuntime().runPressurePass(context.Background(), catalog)
	if err != nil || pass.Admission.MoveCount != 1 || len(submitter.operations) != 2 ||
		submitter.operations[0] != submitter.operations[1] {
		t.Fatalf("restart pass=%+v operations=%x err=%v", pass, submitter.operations, err)
	}
}

func TestGatewayHotShardPressurePassRefusesMissingTopologyAuthority(t *testing.T) {
	catalog, record, observations := gatewayHotShardMoveFixture(t)
	controller, _ := hotshard.New(hotshard.DefaultPolicy())
	runtime := &gatewayHotShardRuntime{authority: &gatewayHotShardTestAuthority{record: record},
		controller: controller}
	pass, err := runtime.runPressurePass(context.Background(), catalog)
	if !errors.Is(err, hotshard.ErrInvalidPressureCut) || pass.Admission.MoveCount != 1 {
		t.Fatalf("pass=%+v err=%v", pass, err)
	}
	controller, _ = hotshard.New(hotshard.DefaultPolicy())
	observations.grant = membershipgrant.Grant{}
	submitter := &gatewayHotMoveSubmitter{}
	runtime = &gatewayHotShardRuntime{authority: &gatewayHotShardTestAuthority{record: record},
		controller: controller, operationsBound: true,
		operations: gatewayHotShardOperationAuthorities{
			moves:   gatewayHotReplicaMoveFactory{observations: observations, grants: observations},
			moveRun: submitter,
		}}
	pass, err = runtime.runPressurePass(context.Background(), catalog)
	if !errors.Is(err, hotshard.ErrInvalidPressureCut) || pass.Admission.MoveCount != 1 ||
		len(submitter.operations) != 0 {
		t.Fatalf("ungranted pass=%+v operations=%x err=%v", pass, submitter.operations, err)
	}
}

func TestGatewayHotShardStopsAfterOneAdmissionPerCatalogGeneration(t *testing.T) {
	catalog, record, _ := gatewayHotShardMoveFixture(t)
	authority := &gatewayHotShardTestAuthority{record: record}
	runtime := &gatewayHotShardRuntime{holder: gateway.NewCatalogHolder(catalog), authority: authority,
		admitted: catalog.Generation()}
	runtime.generation.Store(catalog.Generation())
	if err := runtime.PublishOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if authority.reads != 0 || authority.writes != 0 {
		t.Fatalf("admitted generation performed pressure I/O: reads=%d writes=%d",
			authority.reads, authority.writes)
	}
}

func gatewayHotShardMoveFixture(
	t testing.TB,
) (*gateway.Snapshot, gateway.ReplicatedPressureRecord, gatewayHotMoveObservations) {
	t.Helper()
	group := raftmember.GroupKey{ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2},
		TopologyRecoveryEpoch: 3, ShardIncarnation: [16]byte{4}, GroupID: [16]byte{5}}
	manifest, err := distribution.NewManifest("data", 7, []distribution.Shard{{
		ID: "all", AllocationGeneration: 11,
		Range:   distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}},
		Leaders: []distribution.EndpointID{"one", "two", "three"}, Epoch: 13,
	}})
	if err != nil {
		t.Fatal(err)
	}
	endpoints := map[distribution.EndpointID]string{
		"one": "127.0.0.1:1", "one-native": "127.0.0.1:11", "one-control": "127.0.0.1:21",
		"two": "127.0.0.1:2", "two-native": "127.0.0.1:12", "two-control": "127.0.0.1:22",
		"three": "127.0.0.1:3", "three-native": "127.0.0.1:13", "three-control": "127.0.0.1:23",
		"target": "127.0.0.1:9", "target-native": "127.0.0.1:19", "target-control": "127.0.0.1:29",
	}
	replicas := make([]gateway.ReplicatedReplicaDescriptor, gateway.ServingReplicaCount)
	for index, name := range []string{"one", "two", "three"} {
		member := uint64(index + 1)
		replicas[index] = gateway.ReplicatedReplicaDescriptor{Member: member,
			Node: [16]byte{byte(member)}, StoreID: [16]byte{byte(member + 10)},
			NodeIncarnation: member + 20, Endpoint: distribution.EndpointID(name),
			NativeEndpoint:  distribution.EndpointID(name + "-native"),
			ControlEndpoint: distribution.EndpointID(name + "-control")}
	}
	target := gateway.ReplicatedReplicaDescriptor{Member: 4, Node: [16]byte{4},
		StoreID: [16]byte{14}, NodeIncarnation: 24, Endpoint: "target",
		NativeEndpoint: "target-native", ControlEndpoint: "target-control"}
	catalog, err := gateway.NewSnapshotWithReplicatedMetadata(distribution.ClusterConfig{
		Distributions: []distribution.DistributionSpec{{Name: "data", Arity: 1, MapperVersion: 1}},
		Manifests:     []*distribution.Manifest{manifest},
	}, endpoints, 9, nil, nil, []gateway.ReplicatedShardDescriptor{{
		Distribution: "data", Shard: "all", Group: group, AllocationGeneration: 11,
		Command: raftservice.CommandFence{ReplicaSetVersion: 7, OwnershipEpoch: 13,
			RoutingVersion: 7, RouteGeneration: 9, ActivePolicyGeneration: 1,
			ProtectionEpoch: 1, SchemaGeneration: 1, RelationManifestDigest: [32]byte{1}},
		RangeIdentity: [32]byte{2}, LineageDigest: [32]byte{3}, ForwardingRuleDigest: [32]byte{4},
		Replicas: replicas, EnrolledTarget: &target,
	}})
	if err != nil {
		t.Fatal(err)
	}
	source := autosplit.SourceIdentity{Distribution: "data", Shard: "all",
		AllocationGeneration: 11,
		Range:                distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}},
		BucketBits:           distribution.DefaultVirtualBucketBits,
		RoutingVersion:       7, OwnershipEpoch: 13}
	var capacity autosplit.CapacityVector
	for resource := range autosplit.ResourceCount {
		capacity[resource] = 1000
	}
	node := func(endpoint distribution.EndpointID, domain uint32, used uint64) topologyscheduler.NodeCapacity {
		load := autosplit.CapacityVector{}
		load[autosplit.ResourceLiveBytes] = used
		return topologyscheduler.NodeCapacity{CatalogGeneration: 9, Endpoint: endpoint,
			FailureDomain: domain, Flags: topologyscheduler.NodePlacementReady,
			Capacity: capacity, Used: load, MigrationCapacity: 1000, MaxReceives: 1}
	}
	demand := autosplit.CapacityVector{}
	demand[autosplit.ResourceLiveBytes] = 300
	view := hotshard.View{CatalogGeneration: 9, AuthorityRevision: 1,
		Nodes: []topologyscheduler.NodeCapacity{node("one", 1, 900), node("two", 2, 300),
			node("three", 3, 300), node("target", 4, 100)},
		Reports: []hotshard.Report{{Group: group, Demand: demand, MigrationBytes: 100,
			Recommendation: autosplit.Recommendation{Source: source, WindowSequence: 1,
				Kind: autosplit.RecommendationNone, Reason: autosplit.ReasonBelowTrigger,
				CurrentPressurePPM: 950_000}}}}
	raw, err := hotshard.AppendView(nil, view)
	if err != nil {
		t.Fatal(err)
	}
	record := gateway.ReplicatedPressureRecord{CatalogGeneration: 9, AuthorityRevision: 1,
		PayloadDigest: sha256.Sum256(raw), Payload: raw}
	grant, err := gateway.BuildReplicaReplacementMembershipGrant(
		catalog, group, [16]byte{8}, 1, 1, 4,
	)
	if err != nil {
		t.Fatal(err)
	}
	observations := gatewayHotMoveObservations{publication: raftmodel.Publication{
		Applied: 8, ReplicaSetVersion: 7, ConfState: &pb.ConfState{Voters: []uint64{1, 2, 3}},
	}, grant: grant}
	return catalog, record, observations
}
