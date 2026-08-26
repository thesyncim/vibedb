package rebalanceexec

import (
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/membershipgrant"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/rebalance"
	"github.com/thesyncim/vibedb/internal/snapshottransfer"
	"github.com/thesyncim/vibedb/shardservice"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

type executorFixture struct {
	cut        MoveRoute
	grant      membershipgrant.Grant
	grantFound bool

	membershipRequests []shardservice.ReplicatedMembershipRequest
	snapshotRequests   []SnapshotExportRequest
	bootstrapRequests  []snapshottransfer.BootstrapRequest
	awaits             []rebalance.ReplicatedMoveExecution
	ownershipCommands  [][]byte
	retirements        []SourceRetirementRequest
	finalizes          int
	retries            int
	unknownFinalize    bool
	membershipErr      error
	membershipHook     func()
	drainRequests      []gateway.ClusterCatalogDrainRequest
	drainCertificate   func(gateway.ClusterCatalogDrainRequest) gateway.ClusterCatalogDrainCertificate
}

func (fixture *executorFixture) ResolveReplicaMove(
	context.Context, rebalance.OperationID, *rebalance.Plan,
	rebalance.ReplicatedMoveExecution,
) (MoveRoute, error) {
	return fixture.cut, nil
}

func (fixture *executorFixture) ReadMembershipGrant(
	context.Context, raftmember.GroupKey,
) (membershipgrant.Grant, bool, error) {
	return fixture.grant, fixture.grantFound, nil
}

func (fixture *executorFixture) ApplyMembership(
	_ context.Context, route gateway.ReplicatedMembershipRoute,
	request shardservice.ReplicatedMembershipRequest,
) (gateway.ReplicatedMembershipResult, error) {
	if route.Serving.Command.ReplicaSetVersion != request.ExpectedReplicaSetVersion {
		return gateway.ReplicatedMembershipResult{}, ErrExecutionFence
	}
	fixture.membershipRequests = append(fixture.membershipRequests, request)
	if fixture.membershipHook != nil {
		fixture.membershipHook()
	}
	return gateway.ReplicatedMembershipResult{}, fixture.membershipErr
}

func (fixture *executorFixture) PrepareReplicaMoveSnapshot(
	_ context.Context, request SnapshotExportRequest,
) (snapshottransfer.Descriptor, error) {
	fixture.snapshotRequests = append(fixture.snapshotRequests, request)
	return snapshottransfer.Descriptor{
		Group: request.Group, SourceMember: request.SourceMember,
		TargetMember: request.TargetMember, TargetStore: request.TargetStore,
		TargetIncarnation: request.TargetIncarnation, SchemaGeneration: 7,
		ReplicaSetVersion: request.ReplicaSetVersion, SnapshotIndex: 9,
		SnapshotTerm: 3, Lineage: [32]byte{1}, ArtifactHash: [32]byte{2},
		ArtifactBytes: 4096, ChunkBytes: snapshottransfer.MinChunkBytes,
	}, nil
}

func (fixture *executorFixture) Execute(
	_ context.Context, _ rafttransport.NodeID, request snapshottransfer.BootstrapRequest,
) (snapshottransfer.BootstrapRecord, error) {
	fixture.bootstrapRequests = append(fixture.bootstrapRequests, request)
	return snapshottransfer.BootstrapRecord{
		Request: request, Revision: 2, State: snapshottransfer.BootstrapComplete,
	}, nil
}

func (fixture *executorFixture) AwaitReplicaMove(
	_ context.Context, _ rebalance.OperationID, _ *rebalance.Plan,
	execution rebalance.ReplicatedMoveExecution,
) error {
	fixture.awaits = append(fixture.awaits, execution)
	return nil
}

func (fixture *executorFixture) ProposeReplicaMoveOwnership(
	_ context.Context, _ rebalance.OperationID, _ [32]byte,
	_ gateway.ReplicatedMembershipRoute, command []byte,
) error {
	fixture.ownershipCommands = append(fixture.ownershipCommands, append([]byte(nil), command...))
	return nil
}

func (fixture *executorFixture) PublishReplicaReplacement(
	context.Context, uint64, *gateway.Snapshot, membershipgrant.Grant,
) error {
	return nil
}

func (fixture *executorFixture) FinalizeReplicaReplacement(
	_ context.Context, grant membershipgrant.Grant,
) error {
	if grant != fixture.grant {
		return ErrExecutionFence
	}
	fixture.finalizes++
	fixture.grantFound = false
	if fixture.unknownFinalize {
		fixture.unknownFinalize = false
		return gateway.ErrReplicatedCatalogPending
	}
	return nil
}

func (fixture *executorFixture) PublishReplicaReplacementPostRemove(
	_ context.Context, _ uint64, next *gateway.Snapshot,
	_ membershipgrant.Grant, _ uint64,
) error {
	fixture.cut.Catalog = next
	return nil
}

func (fixture *executorFixture) RetryPending(context.Context) error {
	fixture.retries++
	return nil
}

func (fixture *executorFixture) Read(context.Context) (*gateway.Snapshot, error) {
	return fixture.cut.Catalog, nil
}

func (fixture *executorFixture) CertifyClusterCatalogDrain(
	_ context.Context, request gateway.ClusterCatalogDrainRequest,
) (gateway.ClusterCatalogDrainCertificate, error) {
	fixture.drainRequests = append(fixture.drainRequests, request)
	if fixture.drainCertificate != nil {
		return fixture.drainCertificate(request), nil
	}
	return gateway.ClusterCatalogDrainCertificate{
		Request: request, FenceDigest: [32]byte{1},
		RosterDigest: [32]byte{2}, Proof: [32]byte{3},
	}, nil
}

func (fixture *executorFixture) RetireReplicaSource(
	_ context.Context, request SourceRetirementRequest,
) error {
	fixture.retirements = append(fixture.retirements, request)
	return nil
}

func TestExecutorMapsExactMembershipSnapshotWaitAndDrainActions(t *testing.T) {
	plan, fixture := newExecutorFixture(t)
	executor, err := New(Options{
		Routes: fixture, Grants: fixture, Membership: fixture, Snapshots: fixture,
		Bootstrap: fixture, Awaiter: fixture, Ownership: fixture, Catalog: fixture,
		Drainer: fixture, Retirer: fixture,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, test := range []struct {
		kind rebalance.ActionKind
		want raftservice.MembershipKind
	}{
		{rebalance.ActionAddLearner, raftservice.MembershipAddLearner},
		{rebalance.ActionPromoteVoter, raftservice.MembershipPromoteVoter},
		{rebalance.ActionTransferLeader, raftservice.MembershipTransferLeader},
		{rebalance.ActionRemoveSource, raftservice.MembershipRemoveVoter},
	} {
		execution := testExecution(test.kind)
		if err = executor.ExecuteReplicaMove(ctx, plan.OperationID(), plan, execution); err != nil {
			t.Fatalf("%s: %v", test.kind, err)
		}
		request := fixture.membershipRequests[len(fixture.membershipRequests)-1]
		if request.Kind != test.want || request.TransitionID != fixture.grant.TransitionID ||
			request.ExpectedReplicaSetVersion != execution.PublicationReplicaSet ||
			request.SourceMember != plan.RetiringMember() || request.TargetMember != plan.TargetMember() ||
			(test.kind == rebalance.ActionRemoveSource) != (request.TransferTerm == execution.LeaderTerm) {
			t.Fatalf("%s request=%+v", test.kind, request)
		}
	}

	snapshotExecution := testExecution(rebalance.ActionCreateSnapshotBase)
	if err = executor.ExecuteReplicaMove(
		ctx, plan.OperationID(), plan, snapshotExecution,
	); err != nil {
		t.Fatal(err)
	}
	if len(fixture.snapshotRequests) != 1 || len(fixture.bootstrapRequests) != 1 ||
		fixture.snapshotRequests[0].Operation != [32]byte(plan.OperationID()) ||
		fixture.snapshotRequests[0].Step != snapshotExecution.Proof ||
		fixture.bootstrapRequests[0].Operation != [32]byte(plan.OperationID()) ||
		fixture.bootstrapRequests[0].Step != snapshotExecution.Proof {
		t.Fatalf("snapshot=%+v bootstrap=%+v",
			fixture.snapshotRequests, fixture.bootstrapRequests)
	}

	for _, kind := range []rebalance.ActionKind{
		rebalance.ActionAwaitLeader, rebalance.ActionAwaitSnapshotInstall,
		rebalance.ActionAwaitCatchUp,
	} {
		if err = executor.ExecuteReplicaMove(
			ctx, plan.OperationID(), plan, testExecution(kind),
		); err != nil {
			t.Fatalf("%s: %v", kind, err)
		}
	}
	if len(fixture.awaits) != 3 {
		t.Fatalf("await calls=%d", len(fixture.awaits))
	}
	drain := testExecution(rebalance.ActionAwaitCatalogDrain)
	drain.Action.CatalogGeneration = plan.NextCatalogGeneration()
	fixture.cut.Catalog, err = plan.CatalogSnapshot(fixture.cut.Catalog)
	if err != nil {
		t.Fatal(err)
	}
	if err = executor.ExecuteReplicaMove(ctx, plan.OperationID(), plan, drain); err != nil {
		t.Fatal(err)
	}
	digest, err := gateway.CatalogSnapshotDigest(fixture.cut.Catalog)
	if err != nil || len(fixture.drainRequests) != 1 ||
		fixture.drainRequests[0] != (gateway.ClusterCatalogDrainRequest{
			Operation: [32]byte(plan.OperationID()), Step: drain.Proof,
			Generation: plan.NextCatalogGeneration(), CatalogDigest: digest,
		}) {
		t.Fatalf("drain request=%+v digest=%x err=%v", fixture.drainRequests, digest, err)
	}
	fixture.cut.Catalog, err = gateway.BuildManifestTransition(
		fixture.cut.Catalog, plan.TargetManifest(), plan.PostRemoveCatalogGeneration(),
	)
	if err != nil {
		t.Fatal(err)
	}
	postRemoveDrain := testExecution(rebalance.ActionAwaitCatalogDrain)
	postRemoveDrain.Proof = [32]byte{0x92}
	postRemoveDrain.Action.CatalogGeneration = plan.PostRemoveCatalogGeneration()
	if err = executor.ExecuteReplicaMove(
		ctx, plan.OperationID(), plan, postRemoveDrain,
	); err != nil {
		t.Fatal(err)
	}
	postRemoveDigest, err := gateway.CatalogSnapshotDigest(fixture.cut.Catalog)
	if err != nil || len(fixture.drainRequests) != 2 ||
		fixture.drainRequests[1].Generation != plan.PostRemoveCatalogGeneration() ||
		fixture.drainRequests[1].CatalogDigest != postRemoveDigest ||
		fixture.drainRequests[1].Step != postRemoveDrain.Proof || digest == postRemoveDigest {
		t.Fatalf("post-remove drain=%+v digest=%x err=%v",
			fixture.drainRequests, postRemoveDigest, err)
	}
}

func TestExecutorCatalogDrainFailsClosedOnLocalOrCertificateCutMismatch(t *testing.T) {
	plan, fixture := newExecutorFixture(t)
	executor, err := New(Options{
		Routes: fixture, Grants: fixture, Membership: fixture, Snapshots: fixture,
		Bootstrap: fixture, Awaiter: fixture, Ownership: fixture, Catalog: fixture,
		Drainer: fixture, Retirer: fixture,
	})
	if err != nil {
		t.Fatal(err)
	}
	drain := testExecution(rebalance.ActionAwaitCatalogDrain)
	drain.Action.CatalogGeneration = plan.NextCatalogGeneration()
	if err = executor.ExecuteReplicaMove(
		context.Background(), plan.OperationID(), plan, drain,
	); !errors.Is(err, ErrExecutionFence) || len(fixture.drainRequests) != 0 {
		t.Fatalf("stale local catalog error=%v requests=%+v", err, fixture.drainRequests)
	}
	fixture.cut.Catalog, err = plan.CatalogSnapshot(fixture.cut.Catalog)
	if err != nil {
		t.Fatal(err)
	}
	fixture.drainCertificate = func(request gateway.ClusterCatalogDrainRequest) gateway.ClusterCatalogDrainCertificate {
		request.Generation++
		return gateway.ClusterCatalogDrainCertificate{
			Request: request, FenceDigest: [32]byte{1},
			RosterDigest: [32]byte{2}, Proof: [32]byte{3},
		}
	}
	if err = executor.ExecuteReplicaMove(
		context.Background(), plan.OperationID(), plan, drain,
	); !errors.Is(err, ErrExecutionFence) || len(fixture.drainRequests) != 1 {
		t.Fatalf("mismatched certificate error=%v requests=%+v", err, fixture.drainRequests)
	}
}

func TestExecutorRetiresBeforeExactGrantFinalizationAndSettlesUnknown(t *testing.T) {
	plan, fixture := newExecutorFixture(t)
	fixture.unknownFinalize = true
	executor, err := New(Options{
		Routes: fixture, Grants: fixture, Membership: fixture, Snapshots: fixture,
		Bootstrap: fixture, Awaiter: fixture, Ownership: fixture, Catalog: fixture,
		Drainer: fixture, Retirer: fixture,
	})
	if err != nil {
		t.Fatal(err)
	}
	execution := testExecution(rebalance.ActionRetireSource)
	if err = executor.ExecuteReplicaMove(
		context.Background(), plan.OperationID(), plan, execution,
	); err != nil {
		t.Fatal(err)
	}
	if len(fixture.retirements) != 1 || fixture.finalizes != 1 || fixture.retries != 1 ||
		fixture.grantFound || fixture.retirements[0].Operation != [32]byte(plan.OperationID()) ||
		fixture.retirements[0].Step != execution.Proof {
		t.Fatalf("retire=%+v finalizes=%d retries=%d found=%v",
			fixture.retirements, fixture.finalizes, fixture.retries, fixture.grantFound)
	}
	// A crash after finalization exact-retries retirement without requiring a
	// deleted grant and never recreates or re-finalizes authority.
	if err = executor.ExecuteReplicaMove(
		context.Background(), plan.OperationID(), plan, execution,
	); err != nil || len(fixture.retirements) != 2 || fixture.finalizes != 1 {
		t.Fatalf("retirement replay calls=%d finalizes=%d err=%v",
			len(fixture.retirements), fixture.finalizes, err)
	}
}

func TestExecutorFailsClosedBeforeMembershipWhenGrantIsMissing(t *testing.T) {
	plan, fixture := newExecutorFixture(t)
	fixture.grantFound = false
	executor, _ := New(Options{
		Routes: fixture, Grants: fixture, Membership: fixture, Snapshots: fixture,
		Bootstrap: fixture, Awaiter: fixture, Ownership: fixture, Catalog: fixture,
		Drainer: fixture, Retirer: fixture,
	})
	err := executor.ExecuteReplicaMove(
		context.Background(), plan.OperationID(), plan,
		testExecution(rebalance.ActionRemoveSource),
	)
	if !errors.Is(err, ErrGrantUnavailable) || len(fixture.membershipRequests) != 0 {
		t.Fatalf("requests=%d err=%v", len(fixture.membershipRequests), err)
	}
}

type controllerJournal struct {
	record  gateway.ReplicatedOperationRecord
	present bool
}

func (journal *controllerJournal) ReadOperation(
	_ context.Context, id [32]byte,
) (gateway.ReplicatedOperationRecord, error) {
	if !journal.present || journal.record.ID != id {
		return gateway.ReplicatedOperationRecord{}, gateway.ErrReplicatedOperationMissing
	}
	return journal.record, nil
}

func (journal *controllerJournal) SubmitOperation(
	_ context.Context, record gateway.ReplicatedOperationRecord,
) error {
	if journal.present || record.Revision != 1 {
		return gateway.ErrReplicatedCatalogConflict
	}
	journal.record, journal.present = record, true
	return nil
}

func (journal *controllerJournal) PublishOperation(
	_ context.Context, expected uint64, record gateway.ReplicatedOperationRecord,
) error {
	if !journal.present || journal.record.Revision != expected || record.Revision != expected+1 {
		return gateway.ErrReplicatedCatalogConflict
	}
	journal.record = record
	return nil
}

func (*controllerJournal) DeleteOperation(context.Context, [32]byte, uint64) error {
	return gateway.ErrReplicatedCatalogConflict
}

func (*controllerJournal) RetryPending(context.Context) error {
	return gateway.ErrReplicatedCatalogPending
}

type controllerObserver struct{ cut rebalance.ReplicatedMoveCut }

func (observer *controllerObserver) ObserveReplicaMove(
	context.Context, rebalance.OperationID, gateway.ReplicatedOperationRecord, *rebalance.Plan,
) (rebalance.ReplicatedMoveCut, error) {
	return observer.cut, nil
}

func TestExecutorOutcomeUnknownAdvancesOnlyFromNewObservation(t *testing.T) {
	plan, fixture := newExecutorFixture(t)
	executor, err := New(Options{
		Routes: fixture, Grants: fixture, Membership: fixture, Snapshots: fixture,
		Bootstrap: fixture, Awaiter: fixture, Ownership: fixture, Catalog: fixture,
		Drainer: fixture, Retirer: fixture,
	})
	if err != nil {
		t.Fatal(err)
	}
	observer := &controllerObserver{cut: rebalance.ReplicatedMoveCut{Observation: rebalance.Observation{
		Catalog: fixture.cut.Catalog,
		Publication: raftmodel.Publication{
			Applied: 5, ReplicaSetVersion: 4,
			ConfState: &pb.ConfState{Voters: []uint64{1, 3, 4}},
		},
		LeaderStatus: raftmember.RuntimeStatus{
			MemberID: 1, LeaderID: 1, Term: 3, Commit: 5, Applied: 5,
			RaftState: raft.StateLeader,
		},
	}}}
	unknown := errors.New("membership response lost")
	fixture.membershipErr = errors.Join(raftservice.ErrOutcomeUnknown, unknown)
	fixture.membershipHook = func() {
		observer.cut.Publication = raftmodel.Publication{
			Applied: 6, ReplicaSetVersion: 6,
			ConfState: &pb.ConfState{Voters: []uint64{1, 3, 4}, Learners: []uint64{2}},
		}
		observer.cut.LeaderStatus.Commit = 6
		observer.cut.LeaderStatus.Applied = 6
	}
	journal := new(controllerJournal)
	action, err := rebalance.ExecuteReplicatedMoveStep(
		context.Background(), plan.OperationID(), plan, journal, observer, executor,
	)
	if !errors.Is(err, raftservice.ErrOutcomeUnknown) ||
		action.Kind != rebalance.ActionAddLearner || len(fixture.membershipRequests) != 1 {
		t.Fatalf("unknown action=%+v membership=%d err=%v",
			action, len(fixture.membershipRequests), err)
	}
	fixture.membershipErr, fixture.membershipHook = nil, nil
	action, err = rebalance.ExecuteReplicatedMoveStep(
		context.Background(), plan.OperationID(), nil, journal, observer, executor,
	)
	if err != nil || action.Kind != rebalance.ActionCreateSnapshotBase ||
		len(fixture.membershipRequests) != 1 || len(fixture.snapshotRequests) != 1 {
		t.Fatalf("settled action=%+v membership=%d snapshots=%d err=%v",
			action, len(fixture.membershipRequests), len(fixture.snapshotRequests), err)
	}
}

func newExecutorFixture(t testing.TB) (*rebalance.Plan, *executorFixture) {
	t.Helper()
	group := raftmember.GroupKey{
		ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2},
		TopologyRecoveryEpoch: 3, ShardIncarnation: [16]byte{4}, GroupID: [16]byte{5},
	}
	manifest, err := distribution.NewManifest("data", 7, []distribution.Shard{{
		ID: "all", AllocationGeneration: 11,
		Range:   distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}},
		Leaders: []distribution.EndpointID{"source"}, Epoch: 13,
	}})
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := gateway.NewSnapshot(distribution.ClusterConfig{
		Distributions: []distribution.DistributionSpec{{Name: "data", Arity: 1, MapperVersion: 1}},
		Placements: []distribution.TablePlacement{{
			Table: "docs", Distribution: "data", Columns: []string{"/id"},
		}},
		Manifests: []*distribution.Manifest{manifest},
	}, map[distribution.EndpointID]string{
		"source": "127.0.0.1:7001", "target": "127.0.0.1:7002",
		"source-control": "127.0.0.1:7201",
	}, 9)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := rebalance.PlanReplicaMove(catalog, raftmodel.Publication{
		Applied: 5, ReplicaSetVersion: 4,
		ConfState: &pb.ConfState{Voters: []uint64{1, 3, 4}},
	}, rebalance.MoveRequest{
		Distribution: "data", Shard: "all", Group: group,
		RetiringMember: 1, SnapshotSourceMember: 3, TargetMember: 2,
		Source: "source", Target: "target",
		RetiringReplica: rebalance.ReplicaIdentity{
			Member: 1, Node: rafttransport.NodeID{1}, StoreID: [16]byte{11},
			NodeIncarnation: 21, ControlEndpoint: "source-control",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	source := gateway.ReplicatedEndpoint{
		Member: 1, Node: rafttransport.NodeID{1}, StoreID: [16]byte{11},
		NodeIncarnation: 21, Endpoint: "source", NativeEndpoint: "source-native",
		Address: "127.0.0.1:7101", ControlEndpoint: "source-control",
		ControlAddress: "127.0.0.1:7201",
	}
	target := gateway.ReplicatedEndpoint{
		Member: 2, Node: rafttransport.NodeID{2}, StoreID: [16]byte{12},
		NodeIncarnation: 22, Endpoint: "target", NativeEndpoint: "target-native",
		Address: "127.0.0.1:7102", ControlEndpoint: "target-control",
		ControlAddress: "127.0.0.1:7202",
	}
	command := raftservice.CommandFence{
		ReplicaSetVersion: 4, ActivePolicyGeneration: 2, ProtectionEpoch: 3,
		OwnershipEpoch: 13, SchemaGeneration: 7, RelationManifestDigest: [32]byte{8},
		RoutingVersion: 7, RouteGeneration: 9,
	}
	grant := membershipgrant.Grant{
		Group: group, TransitionID: [16]byte{9}, MetadataEpoch: 10,
		CatalogGeneration: 9, InitialReplicaSetVersion: 4,
		InitialVoters: [3]uint64{1, 3, 4}, InitialRosterDigest: [32]byte{10},
		InitialDescriptorDigest: [32]byte{11}, SourceMember: 1, TargetMember: 2,
		TargetNode: [16]byte(target.Node),
	}
	return plan, &executorFixture{
		grant: grant, grantFound: true,
		cut: MoveRoute{Catalog: catalog, Retiring: source, SnapshotSource: gateway.ReplicatedEndpoint{
			Member: 3, Node: rafttransport.NodeID{3}, StoreID: [16]byte{13},
			NodeIncarnation: 23, Endpoint: "three", NativeEndpoint: "three-native",
			Address: "127.0.0.1:7103",
		}, Target: target, Command: command,
			Membership: gateway.ReplicatedMembershipRoute{
				Serving: gateway.ReplicatedRoute{Distribution: "data", Shard: "all",
					Group: group, AllocationGeneration: 11, Command: command,
					Replicas: []gateway.ReplicatedEndpoint{source,
						{Member: 3, Node: rafttransport.NodeID{3}, StoreID: [16]byte{13},
							NodeIncarnation: 23, Endpoint: "three", NativeEndpoint: "three-native",
							Address: "127.0.0.1:7103"},
						{Member: 4, Node: rafttransport.NodeID{4}, StoreID: [16]byte{14},
							NodeIncarnation: 24, Endpoint: "four", NativeEndpoint: "four-native",
							Address: "127.0.0.1:7104"}},
				},
				EnrolledTarget: target, HasEnrolledTarget: true,
			},
		},
	}
}

func TestExactRetiringReplicaRejectsReusedControlIdentity(t *testing.T) {
	plan, fixture := newExecutorFixture(t)
	exact := plan.RetiringReplica()
	if !exactRetiringReplica(fixture.cut.Retiring, exact) {
		t.Fatal("exact retiring identity was rejected")
	}
	for name, mutate := range map[string]func(*gateway.ReplicatedEndpoint){
		"member":           func(endpoint *gateway.ReplicatedEndpoint) { endpoint.Member++ },
		"node":             func(endpoint *gateway.ReplicatedEndpoint) { endpoint.Node[0]++ },
		"store":            func(endpoint *gateway.ReplicatedEndpoint) { endpoint.StoreID[0]++ },
		"incarnation":      func(endpoint *gateway.ReplicatedEndpoint) { endpoint.NodeIncarnation++ },
		"control endpoint": func(endpoint *gateway.ReplicatedEndpoint) { endpoint.ControlEndpoint += "-reused" },
	} {
		t.Run(name, func(t *testing.T) {
			changed := fixture.cut.Retiring
			mutate(&changed)
			if exactRetiringReplica(changed, exact) {
				t.Fatal("forged retiring identity accepted")
			}
		})
	}
}

func testExecution(kind rebalance.ActionKind) rebalance.ReplicatedMoveExecution {
	action := rebalance.Action{Kind: kind}
	switch kind {
	case rebalance.ActionAddLearner, rebalance.ActionAwaitSnapshotInstall,
		rebalance.ActionAwaitCatchUp,
		rebalance.ActionPromoteVoter, rebalance.ActionTransferLeader,
		rebalance.ActionAdvanceOwnership:
		action.Member = 2
	case rebalance.ActionCreateSnapshotBase:
		action.Member = 3
	case rebalance.ActionRemoveSource, rebalance.ActionRetireSource:
		action.Member = 1
	}
	return rebalance.ReplicatedMoveExecution{
		Action: action, PublicationApplied: 9, PublicationReplicaSet: 6,
		LeaderTerm: 12, Proof: [32]byte{0x91},
	}
}
