//go:build darwin || linux

package raftservice_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/rebalance"
	"github.com/thesyncim/vibedb/internal/rebalanceexec"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/internal/splitcontroller"
	pb "go.etcd.io/raft/v3/raftpb"
)

type processReplicaMoveObserver struct{ cut rebalance.ReplicatedMoveCut }

func (observer *processReplicaMoveObserver) ObserveReplicaMove(
	context.Context,
	rebalance.OperationID,
	gateway.ReplicatedOperationRecord,
	*rebalance.Plan,
) (rebalance.ReplicatedMoveCut, error) {
	return observer.cut, nil
}

type processReplicaMoveExecutor struct {
	actions []rebalance.ReplicatedMoveExecution
}

func (executor *processReplicaMoveExecutor) ExecuteReplicaMove(
	_ context.Context,
	_ rebalance.OperationID,
	_ *rebalance.Plan,
	execution rebalance.ReplicatedMoveExecution,
) error {
	executor.actions = append(executor.actions, execution)
	return nil
}

// TestReplicatedCatalogAuthorityRF3QuorumReplayAndControllerRestart proves the
// control-plane relation through a dedicated three-process RF3 catalog group.
// The process harness is intentionally reused without sharing the data group,
// adding another consensus implementation, or using an in-memory substitute.
func TestReplicatedCatalogAuthorityRF3QuorumReplayAndControllerRestart(t *testing.T) {
	if os.Getenv(processHelperEnvironment) != "" {
		return
	}
	cluster, err := startProcessCatalogRF3Cluster(t)
	if errors.Is(err, errProcessStrictAllocation) {
		t.Skip(err)
	}
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		cluster.close(t)
		cluster.logDiagnostics(t)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ctx = processAuthorizedContext(t, ctx)
	leader := cluster.elect(t, ctx, 1)
	route := cluster.route()
	if fence, ok := cluster.probeFence(); !ok || fence.Group != route.Group ||
		fence.AllocationGeneration != route.AllocationGeneration || route.Group == processGroup() ||
		route.AllocationGeneration == 7 {
		t.Fatalf("catalog probe route retained data-group identity: %+v", route)
	}
	client := newFaultProcessClient(t, cluster)
	session := newProcessCatalogSession(t, route, client, 0xa1, serviceauthz.CapabilityTopology)
	if _, err = session.Open(ctx, 2_000_000_000_000_000_000); err != nil {
		t.Fatalf("open catalog session: %v", err)
	}
	// Session creation is itself a replicated proposal and may overlap the one
	// startup term transition. Establish a fresh serving witness so this test
	// measures catalog publication, not cluster boot readiness.
	leader = cluster.waitStableLeader(t, ctx)
	first := processControlPlaneSnapshot(t, cluster, 1)
	var dataReplicas [processVoters]gateway.ReplicatedEndpoint
	dataRoute, ok := first.ResolveReplicatedRoute("orders", "0000-ffff", dataReplicas[:0])
	if !ok {
		t.Fatal("published data RF3 group did not resolve")
	}
	if route.Distribution != gateway.ReplicatedCatalogDistribution ||
		route.Shard != gateway.ReplicatedCatalogShard ||
		dataRoute.Distribution == route.Distribution || dataRoute.Shard == route.Shard ||
		dataRoute.Group == route.Group || route.Group != processCatalogGroup() {
		t.Fatalf("catalog serving group=%+v published data group=%+v", route.Group, dataRoute.Group)
	}
	if route.Command.RelationManifestDigest == dataRoute.Command.RelationManifestDigest {
		t.Fatal("catalog and data RF3 groups reused one relation manifest")
	}
	for index := range route.Replicas {
		if route.Replicas[index].StoreID == dataRoute.Replicas[index].StoreID {
			t.Fatalf("catalog member %d reused published data store identity", index+1)
		}
	}
	authority, err := gateway.NewReplicatedCatalogAuthority(gateway.ReplicatedCatalogAuthorityOptions{
		Executor: client.executor, Route: route, Relation: 1,
		Holder: gateway.NewCatalogHolder(nil), Session: session,
		Authority: processRequestAuthority(),
	})
	if err != nil {
		t.Fatal(err)
	}

	client.resetAttempts()
	if err = authority.Publish(ctx, 0, first); err != nil {
		t.Fatalf("publish initial catalog: %v; proposal trace=%+v", err, client.proposalTrace())
	}
	attempts, _ := client.snapshot()
	if len(attempts) != 1 {
		t.Fatalf("catalog proposal attempts = %d, want 1; trace=%+v",
			len(attempts), client.proposalTrace())
	}
	acknowledged, ok := client.lastProposalResult()
	if !ok || acknowledged.Outcome.CompletionAppliedSequence == 0 ||
		acknowledged.Outcome.CompletionBytes != len(acknowledged.Completion) {
		t.Fatalf("initial catalog publication result=%+v present=%t",
			acknowledged.Outcome, ok)
	}
	// Replaying the byte-identical proposal may append a physical no-op entry,
	// but it must return the original durable logical result and never apply the
	// put-if-absent command a second time.
	replayed, err := client.executor.ProposeTopology(ctx, route, attempts[0])
	if err != nil {
		t.Fatalf("replay catalog proposal: %v", err)
	}
	replayedAgain, err := client.executor.ProposeTopology(ctx, route, attempts[0])
	if err != nil || !bytes.Equal(acknowledged.Completion, replayed.Completion) ||
		!bytes.Equal(acknowledged.Completion, replayedAgain.Completion) ||
		acknowledged.Outcome.Code != replayed.Outcome.Code ||
		replayed.Outcome.Code != replayedAgain.Outcome.Code ||
		acknowledged.Outcome.CompletionAppliedSequence !=
			replayed.Outcome.CompletionAppliedSequence ||
		replayed.Outcome.CompletionAppliedSequence !=
			replayedAgain.Outcome.CompletionAppliedSequence ||
		acknowledged.Outcome.CompletionBytes != replayed.Outcome.CompletionBytes ||
		replayed.Outcome.CompletionBytes != replayedAgain.Outcome.CompletionBytes ||
		replayed.Outcome.CompletionBytes != len(replayed.Completion) ||
		replayedAgain.Outcome.CompletionBytes != len(replayedAgain.Completion) ||
		acknowledged.Outcome.AppliedIndex < acknowledged.Outcome.CompletionAppliedSequence ||
		replayed.Outcome.AppliedIndex < replayed.Outcome.CompletionAppliedSequence ||
		replayedAgain.Outcome.AppliedIndex < replayedAgain.Outcome.CompletionAppliedSequence ||
		replayed.Outcome.AppliedIndex < acknowledged.Outcome.AppliedIndex ||
		replayedAgain.Outcome.AppliedIndex < replayed.Outcome.AppliedIndex ||
		acknowledged.State.Applied < acknowledged.Outcome.AppliedIndex ||
		replayed.State.Applied < replayed.Outcome.AppliedIndex ||
		replayedAgain.State.Applied < replayedAgain.Outcome.AppliedIndex {
		t.Fatalf("idempotent replay changed result: acknowledged=%+v first=%+v second=%+v err=%v",
			acknowledged.Outcome, replayed.Outcome, replayedAgain.Outcome, err)
	}
	// A byte-identical retry may occupy a later Raft entry, so AppliedIndex is a
	// physical catch-up witness. CompletionAppliedSequence and Completion are
	// the immutable logical result that prove the catalog mutation was not run
	// again.
	for member := uint64(1); member <= processVoters; member++ {
		cluster.waitCommittedApplied(t, ctx, member, replayedAgain.Outcome.AppliedIndex, 0)
	}
	visible, err := authority.Read(ctx)
	if err != nil || visible.Generation() != first.Generation() {
		t.Fatalf("catalog after exact replay=%v err=%v", visible, err)
	}
	wantVisible, wantErr := gateway.AppendSnapshotDocument(nil, first)
	gotVisible, gotErr := gateway.AppendSnapshotDocument(nil, visible)
	if wantErr != nil || gotErr != nil || !bytes.Equal(wantVisible, gotVisible) {
		t.Fatalf("exact replay changed catalog bytes: wantErr=%v gotErr=%v", wantErr, gotErr)
	}

	// Reconstruct all controller-local state from the RF3 relation. A fresh
	// session and holder have no catalog bytes or pending command in memory.
	limitedExecutor, err := gateway.NewReplicatedExecutor(client, 1, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	client.executor = limitedExecutor
	restartedSession := newProcessCatalogSession(t, route, client, 0xa2,
		serviceauthz.CapabilityTopology)
	if _, err = restartedSession.Open(ctx, 2_000_000_000_000_000_000); err != nil {
		t.Fatalf("open restarted catalog session: %v", err)
	}
	restartedHolder := gateway.NewCatalogHolder(nil)
	restarted, err := gateway.NewReplicatedCatalogAuthority(gateway.ReplicatedCatalogAuthorityOptions{
		Executor: client.executor, Route: route, Relation: 1,
		Holder: restartedHolder, Session: restartedSession,
		Authority: processRequestAuthority(),
	})
	if err != nil {
		t.Fatal(err)
	}
	read, err := restarted.Read(ctx)
	if err != nil || read.Generation() != 1 {
		t.Fatalf("catalog after controller restart = %v, err=%v", read, err)
	}

	// Submit a real replica-move record through the shipped controller, then
	// discard all controller-local state. The replacement controller discovers
	// the operation from the process RF3 directory and advances the next action
	// from a new detached shard observation; no in-memory journal or work
	// queue participates in recovery.
	movePublication := raftmodel.Publication{
		Applied: 10, ReplicaSetVersion: dataRoute.Command.ReplicaSetVersion,
		ConfState: &pb.ConfState{Voters: []uint64{1, 2, 3}},
	}
	movePlan, err := rebalance.PlanReplicaMove(read, movePublication, rebalance.MoveRequest{
		Distribution: dataRoute.Distribution, Shard: dataRoute.Shard, Group: dataRoute.Group,
		RetiringMember: 1, SnapshotSourceMember: 2, TargetMember: 4,
		Source: distribution.EndpointID(dataRoute.Replicas[0].Endpoint), Target: "rf3-target",
	})
	if err != nil {
		t.Fatal(err)
	}
	moveObserver := &processReplicaMoveObserver{cut: rebalance.ReplicatedMoveCut{
		Observation: rebalance.Observation{
			Catalog: read, Publication: movePublication,
			LeaderStatus: raftmember.RuntimeStatus{
				MemberID: 2, LeaderID: 2, Term: 3, Commit: 10, Applied: 10,
			},
		},
	}}
	moveExecutor := new(processReplicaMoveExecutor)
	moveController, err := rebalanceexec.NewController(
		restarted, restarted, moveObserver, moveExecutor,
	)
	if err != nil {
		t.Fatal(err)
	}
	action, err := moveController.Submit(ctx, movePlan)
	if err != nil || action.Kind != rebalance.ActionAddLearner || len(moveExecutor.actions) != 1 {
		t.Fatalf("initial move action=%+v executed=%d err=%v",
			action, len(moveExecutor.actions), err)
	}
	moveObserver.cut.Publication = raftmodel.Publication{
		Applied: 11, ReplicaSetVersion: dataRoute.Command.ReplicaSetVersion + 1,
		ConfState: &pb.ConfState{Voters: []uint64{1, 2, 3}, Learners: []uint64{4}},
	}
	moveObserver.cut.LeaderStatus.Commit = 11
	moveObserver.cut.LeaderStatus.Applied = 11
	restartedMoveController, err := rebalanceexec.NewController(
		restarted, restarted, moveObserver, moveExecutor,
	)
	if err != nil {
		t.Fatal(err)
	}
	movePass, err := restartedMoveController.RunPass(ctx)
	if err != nil || movePass.Moves != 1 || movePass.Advanced != 1 ||
		len(moveExecutor.actions) != 2 ||
		moveExecutor.actions[1].Action.Kind != rebalance.ActionCreateSnapshotBase {
		t.Fatalf("restarted move pass=%+v executed=%+v err=%v",
			movePass, moveExecutor.actions, err)
	}

	// The same authority is the split controller's replicated journal. Its
	// operation survives reconstruction through a linearizable relation read.
	var journal splitcontroller.ReplicatedOperationJournal = restarted
	record := gateway.ReplicatedOperationRecord{
		ID: [32]byte{0xc1}, Kind: gateway.ReplicatedOperationSplit,
		State: gateway.ReplicatedOperationPlanned, Revision: 1,
		CatalogGeneration: 1, Cursor: [8]uint64{1}, Proof: [32]byte{0xd1},
	}
	record.Intent = []byte(`{}`)
	record.IntentDigest = sha256.Sum256(record.Intent)
	leader = cluster.waitLeader(t, ctx)
	client.arm(leader, faultAfterDecodedResponseBeforeClientDelivery)
	err = journal.SubmitOperation(ctx, record)
	if !errors.Is(err, gateway.ErrReplicatedCatalogPending) {
		t.Fatalf("unknown split operation publication: %v", err)
	}
	pending := restartedSession.PendingCommand()
	if err = journal.RetryPending(ctx); err != nil {
		t.Fatalf("settle split operation publication: %v", err)
	}
	attempts, _ = client.snapshot()
	if len(attempts) < 2 || !bytes.Equal(pending, attempts[len(attempts)-1]) {
		t.Fatalf("split retry attempts=%d exact=%v", len(attempts),
			len(attempts) != 0 && bytes.Equal(pending, attempts[len(attempts)-1]))
	}
	loaded, err := journal.ReadOperation(ctx, record.ID)
	if err != nil || !loaded.Equal(record) {
		t.Fatalf("read split operation = %+v, err=%v", loaded, err)
	}

	// A fresh controller session reconstructs the running operation from RF3,
	// advances it to a terminal witness, and removes that exact revision.
	recoveredSession := newProcessCatalogSession(t, route, client, 0xa3,
		serviceauthz.CapabilityTopology)
	if _, err = recoveredSession.Open(ctx, 2_000_000_000_000_000_000); err != nil {
		t.Fatalf("open recovered controller session: %v", err)
	}
	recovered, err := gateway.NewReplicatedCatalogAuthority(gateway.ReplicatedCatalogAuthorityOptions{
		Executor: limitedExecutor, Route: route, Relation: 1,
		Holder: restartedHolder, Session: recoveredSession,
		Authority: processRequestAuthority(),
	})
	if err != nil {
		t.Fatal(err)
	}
	complete := loaded
	complete.State, complete.Revision = gateway.ReplicatedOperationComplete, loaded.Revision+1
	if err = recovered.PublishOperation(ctx, loaded.Revision, complete); err != nil {
		t.Fatalf("publish terminal split operation: %v", err)
	}
	if err = recovered.DeleteOperation(ctx, complete.ID, complete.Revision); err != nil {
		t.Fatalf("GC terminal split operation: %v", err)
	}
	if _, err = recovered.ReadOperation(ctx, complete.ID); !errors.Is(err, gateway.ErrReplicatedOperationMissing) {
		t.Fatalf("terminal split operation survived GC: %v", err)
	}

	second := processControlPlaneSnapshot(t, cluster, 2)
	if err = restarted.Publish(ctx, 1, second); err != nil {
		t.Fatalf("catalog CAS generation 1->2: %v", err)
	}
	if err = restarted.Publish(ctx, 1, second); !errors.Is(err, gateway.ErrCatalogGenerationMismatch) {
		t.Fatalf("stale catalog generation = %v", err)
	}

	// Leader loss cannot expose the local holder as authority. The read travels
	// through the replacement leader and returns the committed generation.
	cluster.killAndElect(t, leader)
	afterLoss, err := restarted.Read(ctx)
	if err != nil || afterLoss.Generation() != 2 {
		t.Fatalf("catalog after leader loss = %v, err=%v", afterLoss, err)
	}
}

func TestProcessProbeFenceTracksRoleSpecificRoute(t *testing.T) {
	addresses := [processVoters]string{
		"127.0.0.1:201", "127.0.0.1:202", "127.0.0.1:203",
	}
	command := processCommandFence(sha256.Sum256([]byte("probe-fence-relation-manifest")))
	for _, role := range []processRuntimeRole{processDataRole, processCatalogRole} {
		route, err := processRoleRoute(role, addresses, command)
		if err != nil {
			t.Fatalf("role %d route: %v", role, err)
		}
		cluster := &processRF3Cluster{routeValue: route}
		fence, ok := cluster.probeFence()
		if !ok || fence.Group != route.Group ||
			fence.AllocationGeneration != route.AllocationGeneration {
			t.Fatalf("role %d probe fence=%+v ok=%t route=%+v", role, fence, ok, route)
		}
	}
}

func processControlPlaneSnapshot(
	t testing.TB, cluster *processRF3Cluster, generation uint64,
) *gateway.Snapshot {
	t.Helper()
	const (
		distributionName distribution.DistributionName = "orders"
		shardID          distribution.ShardID          = "0000-ffff"
	)
	endpointIDs := [processVoters]distribution.EndpointID{
		"rf3-member-1", "rf3-member-2", "rf3-member-3",
	}
	nativeEndpointIDs := [processVoters]distribution.EndpointID{
		"rf3-native-1", "rf3-native-2", "rf3-native-3",
	}
	sqlAddresses := [processVoters]string{"127.0.0.1:1", "127.0.0.1:2", "127.0.0.1:3"}
	manifest, err := distribution.NewManifest(distributionName, 17, []distribution.Shard{{
		ID: shardID, AllocationGeneration: 7,
		Range:   distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}},
		Leaders: endpointIDs[:], Epoch: 11,
	}})
	if err != nil {
		t.Fatal(err)
	}
	config := distribution.ClusterConfig{
		Distributions: []distribution.DistributionSpec{{
			Name: distributionName, Arity: 1,
			MapperVersion: distribution.NativeMapperVersion,
		}},
		Placements: []distribution.TablePlacement{{
			Table: "docs", Distribution: distributionName, Columns: []string{"/id"},
		}},
		Manifests: []*distribution.Manifest{manifest},
	}
	endpoints := make(map[distribution.EndpointID]string, processVoters+1)
	endpoints["rf3-target"] = "127.0.0.1:4"
	controlEndpointIDs := [processVoters]distribution.EndpointID{"rf3-control-1", "rf3-control-2", "rf3-control-3"}
	replicas := make([]gateway.ReplicatedReplicaDescriptor, processVoters)
	for index := 0; index < processVoters; index++ {
		endpoints[endpointIDs[index]] = sqlAddresses[index]
		endpoints[nativeEndpointIDs[index]] = cluster.nativeAddresses[index]
		endpoints[controlEndpointIDs[index]] = fmt.Sprintf("127.0.0.1:%d", 301+index)
		replicas[index] = gateway.ReplicatedReplicaDescriptor{
			Member: uint64(index + 1), Node: processNode(uint64(index + 1)),
			StoreID:         processStoreIdentity(uint64(index + 1)).StoreID,
			NodeIncarnation: 1, Endpoint: endpointIDs[index],
			NativeEndpoint:  nativeEndpointIDs[index],
			ControlEndpoint: controlEndpointIDs[index],
		}
	}
	dataManifestDigest := sha256.Sum256([]byte("rf3-process-data-relation-manifest"))
	rangeIdentity, lineageDigest, forwardingRuleDigest := processRF3DescriptorIdentity(
		processGroup(), shardID, 7,
	)
	snapshot, err := gateway.NewSnapshotWithReplicatedMetadata(
		config, endpoints, generation, nil, nil, []gateway.ReplicatedShardDescriptor{{
			Distribution: distributionName, Shard: shardID,
			Group: processGroup(), AllocationGeneration: 7,
			Command: processCommandFence(dataManifestDigest), RangeIdentity: rangeIdentity,
			LineageDigest: lineageDigest, ForwardingRuleDigest: forwardingRuleDigest,
			Replicas: replicas,
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}
