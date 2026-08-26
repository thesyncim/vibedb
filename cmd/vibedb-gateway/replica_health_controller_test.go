package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/rebalance"
	"github.com/thesyncim/vibedb/internal/replicacontrol"
	pb "go.etcd.io/raft/v3/raftpb"
)

type testReplicaHealthCatalog struct{ snapshot *gateway.Snapshot }

func (catalog testReplicaHealthCatalog) Read(context.Context) (*gateway.Snapshot, error) {
	return catalog.snapshot, nil
}

type testFailureAuthority struct {
	certificates []rebalance.FailureQuorumCertificate
}

func (authority testFailureAuthority) VisitFailureCertificates(
	_ context.Context, _ *gateway.Snapshot, visit func(rebalance.FailureQuorumCertificate) error,
) error {
	for _, certificate := range authority.certificates {
		if err := visit(certificate); err != nil {
			return err
		}
	}
	return nil
}

type testHealthObserver struct{ rejected distribution.ShardID }

func (observer testHealthObserver) ObserveReplicaHealth(
	_ context.Context, _ *gateway.Snapshot, certificate rebalance.FailureQuorumCertificate,
) (gatewayReplicaHealthObservation, error) {
	if certificate.Shard == observer.rejected {
		return gatewayReplicaHealthObservation{}, errors.New("injected observation failure")
	}
	return gatewayReplicaHealthObservation{Publication: raftmodel.Publication{Applied: 1}}, nil
}

type testCandidateInventory struct{}

func (testCandidateInventory) ReplacementCandidates(
	context.Context, *gateway.Snapshot, rebalance.FailureQuorumCertificate,
) ([]rebalance.ReplacementCandidate, error) {
	return []rebalance.ReplacementCandidate{{Member: 9}}, nil
}

type testFailedReplicaSink struct{}

func (testFailedReplicaSink) SubmitFailedReplicaMove(context.Context, rebalance.FailedReplicaMoveIntent) error {
	return nil
}

func TestReplicaHealthControllerStreamsIndependentCertifiedFailures(t *testing.T) {
	snapshot := testReplicaHealthSnapshot(t)
	controller, err := newGatewayReplicaHealthController(
		testReplicaHealthCatalog{snapshot},
		testFailureAuthority{certificates: []rebalance.FailureQuorumCertificate{{Shard: "a"}, {Shard: "b"}}},
		testHealthObserver{rejected: "a"}, testCandidateInventory{}, testFailedReplicaSink{},
	)
	if err != nil {
		t.Fatal(err)
	}
	var scheduled int
	controller.schedule = func(
		_ context.Context, cut rebalance.FailedReplicaPlanningCut, _ rebalance.FailedReplicaMoveSink,
	) (rebalance.FailedReplicaMoveIntent, error) {
		scheduled++
		if cut.Catalog != snapshot || cut.Certificate.Shard != "b" || len(cut.Candidates) != 1 {
			t.Fatalf("wrong detached cut: %+v", cut)
		}
		return rebalance.FailedReplicaMoveIntent{}, nil
	}
	pass, err := controller.RunPass(context.Background())
	if err == nil || pass.Certificates != 2 || pass.Eligible != 1 || pass.Submitted != 1 || scheduled != 1 {
		t.Fatalf("pass=%+v scheduled=%d err=%v", pass, scheduled, err)
	}
}

type oneReplicaHealthPass struct {
	cancel context.CancelFunc
	calls  int
}

func (runner *oneReplicaHealthPass) RunPass(context.Context) (gatewayReplicaHealthPass, error) {
	runner.calls++
	runner.cancel()
	return gatewayReplicaHealthPass{Certificates: 2, Submitted: 1}, nil
}

func TestRunReplicaHealthControllerStartsImmediately(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	runner := &oneReplicaHealthPass{cancel: cancel}
	var logs int
	runReplicaHealthController(ctx, runner, time.Hour, func(string, ...any) { logs++ })
	if runner.calls != 1 || logs != 1 {
		t.Fatalf("calls=%d logs=%d", runner.calls, logs)
	}
}

func TestReplicaControllerLifecycleJoinsBothLoops(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	moves := &oneReplicaMovePass{cancel: cancel}
	health := &oneReplicaHealthPass{cancel: cancel}
	done, err := startGatewayReplicaControllers(
		ctx, moves, health, time.Hour, func(string, ...any) {},
	)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("replica controllers did not join after cancellation")
	}
	if moves.calls != 1 || health.calls != 1 {
		t.Fatalf("move calls=%d health calls=%d", moves.calls, health.calls)
	}
}

func TestReplicaHealthRuntimeFailsClosedWithoutAuthorityOrTransport(t *testing.T) {
	if controller, err := newGatewayReplicaHealthRuntime(
		testReplicaHealthCatalog{testReplicaHealthSnapshot(t)}, nil, nil,
		testCandidateInventory{}, nil,
	); controller != nil || !errors.Is(err, errGatewayReplicaHealth) {
		t.Fatalf("controller=%v err=%v", controller, err)
	}
}

type testAuthenticatedHealthClient struct{}

func (testAuthenticatedHealthClient) Observe(
	_ context.Context, _ rafttransport.NodeID, request replicacontrol.Request,
) (replicacontrol.Observation, error) {
	if request.TargetMember == 1 {
		return replicacontrol.Observation{}, errors.New("failed member unavailable")
	}
	status := raftmember.RuntimeStatus{MemberID: request.TargetMember, LeaderID: 2,
		Term: 4, Commit: 30, Applied: 30}
	return replicacontrol.Observation{Request: request, Status: status,
		Publication: raftmodel.Publication{Applied: 30, ReplicaSetVersion: 7,
			ConfState: &pb.ConfState{Voters: []uint64{1, 2, 3}}}}, nil
}

func TestAuthenticatedHealthObserverRequiresCurrentQuorumAndLeader(t *testing.T) {
	snapshot, certificate := testReplicatedHealthSnapshot(t)
	observer := gatewayAuthenticatedHealthObserver{client: testAuthenticatedHealthClient{}}
	cut, err := observer.ObserveReplicaHealth(context.Background(), snapshot, certificate)
	if err != nil || cut.Leader.MemberID != 2 || len(cut.Healthy) != 2 ||
		cut.Healthy[0].Member != 2 || cut.Healthy[1].Member != 3 {
		t.Fatalf("cut=%+v err=%v", cut, err)
	}
	certificate.LeaderTerm++
	if _, err = observer.ObserveReplicaHealth(context.Background(), snapshot, certificate); err == nil {
		t.Fatal("stale certificate term accepted")
	}
}

func testReplicaHealthSnapshot(t testing.TB) *gateway.Snapshot {
	t.Helper()
	manifest, err := distribution.NewManifest("data", 1, []distribution.Shard{{
		ID: "a", AllocationGeneration: 1,
		Range:   distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}},
		Leaders: []distribution.EndpointID{"endpoint"}, Epoch: 1,
	}})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := gateway.NewSnapshot(distribution.ClusterConfig{
		Distributions: []distribution.DistributionSpec{{Name: "data", Arity: 1, MapperVersion: 1}},
		Manifests:     []*distribution.Manifest{manifest},
	}, map[distribution.EndpointID]string{"endpoint": "127.0.0.1:1"}, 1)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func testReplicatedHealthSnapshot(t testing.TB) (*gateway.Snapshot, rebalance.FailureQuorumCertificate) {
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
	}
	replicas := make([]gateway.ReplicatedReplicaDescriptor, 3)
	for index, name := range []string{"one", "two", "three"} {
		member := uint64(index + 1)
		replicas[index] = gateway.ReplicatedReplicaDescriptor{Member: member,
			Node: [16]byte{byte(member)}, StoreID: [16]byte{byte(member + 10)},
			NodeIncarnation: member + 20, Endpoint: distribution.EndpointID(name),
			NativeEndpoint:  distribution.EndpointID(name + "-native"),
			ControlEndpoint: distribution.EndpointID(name + "-control")}
	}
	snapshot, err := gateway.NewSnapshotWithReplicatedMetadata(distribution.ClusterConfig{
		Distributions: []distribution.DistributionSpec{{Name: "data", Arity: 1, MapperVersion: 1}},
		Manifests:     []*distribution.Manifest{manifest},
	}, endpoints, 9, nil, nil, []gateway.ReplicatedShardDescriptor{{
		Distribution: "data", Shard: "all", Group: group, AllocationGeneration: 11,
		Command: raftservice.CommandFence{ReplicaSetVersion: 7, OwnershipEpoch: 13,
			RoutingVersion: 7, RouteGeneration: 9, ActivePolicyGeneration: 1,
			ProtectionEpoch: 1, SchemaGeneration: 1, RelationManifestDigest: [32]byte{1}},
		Replicas: replicas,
	}})
	if err != nil {
		t.Fatal(err)
	}
	return snapshot, rebalance.FailureQuorumCertificate{Distribution: "data", Shard: "all",
		Group: group, CatalogGeneration: 9, ReplicaSetVersion: 7, LeaderTerm: 4,
		CommitIndex: 25, FirstFailureEpoch: 10, ConfirmedEpoch: 12, SuspectMember: 1,
		Confirmations: []rebalance.FailureConfirmation{
			{Member: 2, FirstFailureEpoch: 10, ConfirmedEpoch: 12, LeaderTerm: 4, ReplicaSetVersion: 7, CommitIndex: 25},
			{Member: 3, FirstFailureEpoch: 10, ConfirmedEpoch: 12, LeaderTerm: 4, ReplicaSetVersion: 7, CommitIndex: 25},
		}}
}
