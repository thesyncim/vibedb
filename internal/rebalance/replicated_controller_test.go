package rebalance

import (
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
)

type memoryMoveJournal struct {
	record        gateway.ReplicatedOperationRecord
	present       bool
	unknownNext   bool
	unknownDelete bool
	pending       bool
	retries       int
}

func (journal *memoryMoveJournal) ReadOperation(
	_ context.Context, id [32]byte,
) (gateway.ReplicatedOperationRecord, error) {
	if !journal.present || journal.record.ID != id {
		return gateway.ReplicatedOperationRecord{}, gateway.ErrReplicatedOperationMissing
	}
	return journal.record, nil
}

func (journal *memoryMoveJournal) SubmitOperation(
	ctx context.Context, record gateway.ReplicatedOperationRecord,
) error {
	if record.Revision != 1 || record.State != gateway.ReplicatedOperationPlanned {
		return gateway.ErrReplicatedCatalogConflict
	}
	return journal.publish(ctx, 0, record)
}

func (journal *memoryMoveJournal) PublishOperation(
	ctx context.Context, expected uint64, record gateway.ReplicatedOperationRecord,
) error {
	return journal.publish(ctx, expected, record)
}

func (journal *memoryMoveJournal) publish(
	_ context.Context, expected uint64, record gateway.ReplicatedOperationRecord,
) error {
	current := uint64(0)
	if journal.present {
		current = journal.record.Revision
	}
	if current != expected || record.Revision != expected+1 ||
		journal.present && (record.ID != journal.record.ID ||
			record.IntentDigest != journal.record.IntentDigest) {
		return gateway.ErrReplicatedCatalogConflict
	}
	journal.record, journal.present = record, true
	if journal.unknownNext {
		journal.unknownNext, journal.pending = false, true
		return errors.Join(gateway.ErrReplicatedCatalogPending, errors.New("response lost"))
	}
	return nil
}

func (journal *memoryMoveJournal) RetryPending(context.Context) error {
	if !journal.pending {
		return gateway.ErrReplicatedCatalogPending
	}
	journal.pending = false
	journal.retries++
	return nil
}

func (journal *memoryMoveJournal) DeleteOperation(
	_ context.Context, id [32]byte, revision uint64,
) error {
	if !journal.present {
		return nil
	}
	if journal.record.ID != id || journal.record.Revision != revision ||
		journal.record.State != gateway.ReplicatedOperationComplete {
		return gateway.ErrReplicatedCatalogConflict
	}
	journal.present = false
	if journal.unknownDelete {
		journal.unknownDelete, journal.pending = false, true
		return errors.Join(gateway.ErrReplicatedCatalogPending, errors.New("delete response lost"))
	}
	if journal.unknownNext {
		journal.unknownNext, journal.pending = false, true
		return errors.Join(gateway.ErrReplicatedCatalogPending, errors.New("response lost"))
	}
	return nil
}

type fixedMoveObserver struct {
	cut   ReplicatedMoveCut
	err   error
	calls int
}

func (observer *fixedMoveObserver) ObserveReplicaMove(
	_ context.Context, _ OperationID,
) (ReplicatedMoveCut, error) {
	observer.calls++
	return observer.cut, observer.err
}

type moveActionExecutor struct {
	journal *memoryMoveJournal
	calls   []Action
	fail    error
	unknown bool
	bound   bool
}

func (executor *moveActionExecutor) ExecuteReplicaMove(
	_ context.Context, _ OperationID, plan *Plan, execution ReplicatedMoveExecution,
) error {
	action := execution.Action
	executor.calls = append(executor.calls, action)
	executor.bound = plan.SnapshotBaseBound()
	if executor.unknown {
		executor.unknown = false
		executor.journal.unknownNext = true
	}
	if executor.fail != nil {
		err := executor.fail
		executor.fail = nil
		return err
	}
	return nil
}

func TestReplicatedMoveControllerSettlesEveryCrashBoundary(t *testing.T) {
	plan, catalog := moveTestPlan(t)
	cut := ReplicatedMoveCut{Observation: Observation{
		Catalog: catalog,
		Publication: raftmodel.Publication{
			Applied: 5, ReplicaSetVersion: 4, ConfState: plan.initialConf,
		},
		LeaderStatus: leaderStatus(1, 5),
	}}
	journal := &memoryMoveJournal{unknownNext: true}
	observer := &fixedMoveObserver{cut: cut}
	executeErr := errors.New("crash after durable action outcome became unknown")
	executor := &moveActionExecutor{journal: journal, fail: executeErr}
	action, err := ExecuteReplicatedMoveStep(
		context.Background(), plan.OperationID(), plan, journal, observer, executor,
	)
	if !errors.Is(err, executeErr) || action.Kind != ActionAddLearner ||
		len(executor.calls) != 1 || journal.retries != 1 ||
		journal.record.State != gateway.ReplicatedOperationRunning ||
		journal.record.Cursor[3] != replicaMoveCursorExecuting {
		t.Fatalf("first action=%+v calls=%d record=%+v retries=%d err=%v",
			action, len(executor.calls), journal.record, journal.retries, err)
	}
	// A fresh controller has no process-local plan. The immutable journal intent
	// recovers it, and the same idempotency tuple settles the unknown action.
	executor.unknown = true
	action, err = ExecuteReplicatedMoveStep(
		context.Background(), plan.OperationID(), nil, journal, observer, executor,
	)
	if err != nil || action.Kind != ActionAddLearner || len(executor.calls) != 2 ||
		journal.record.Cursor[3] != replicaMoveCursorApplied || journal.retries != 2 {
		t.Fatalf("restart action=%+v calls=%d record=%+v retries=%d err=%v",
			action, len(executor.calls), journal.record, journal.retries, err)
	}
	// Durable post-execution proof suppresses duplicate execution while the
	// observed Raft cut has not advanced yet.
	if _, err = ExecuteReplicatedMoveStep(
		context.Background(), plan.OperationID(), nil, journal, observer, executor,
	); err != nil || len(executor.calls) != 2 {
		t.Fatalf("settled action re-executed: calls=%d err=%v", len(executor.calls), err)
	}

	observer.cut.Publication = raftmodel.Publication{
		Applied: 6, ReplicaSetVersion: 6, ConfState: plan.learnerConf,
	}
	observer.cut.LeaderStatus = leaderStatus(1, 6)
	action, err = ExecuteReplicatedMoveStep(
		context.Background(), plan.OperationID(), nil, journal, observer, executor,
	)
	if err != nil || action.Kind != ActionCreateSnapshotBase || len(executor.calls) != 3 ||
		journal.record.Cursor[3] != replicaMoveCursorApplied ||
		journal.record.Revision != 6 {
		t.Fatalf("next action=%+v calls=%d record=%+v err=%v",
			action, len(executor.calls), journal.record, err)
	}
}

func TestReplicatedMoveControllerRecoversBoundIntentAfterPromotion(t *testing.T) {
	plan, catalog := moveTestPlan(t)
	bound := bindMoveTestPlan(plan)
	intent, err := AppendReplicaMoveIntent(nil, catalog, plan)
	if err != nil {
		t.Fatal(err)
	}
	initialCut := ReplicatedMoveCut{Observation: Observation{
		Catalog: catalog,
		Publication: raftmodel.Publication{
			Applied: 6, ReplicaSetVersion: 6, ConfState: plan.learnerConf,
		},
		LeaderStatus: leaderStatus(1, 6),
	}}
	initialAction := Action{Kind: ActionCreateSnapshotBase, Member: plan.TargetMember()}
	initial := newReplicaMoveRecord(
		plan.OperationID(), catalog.Generation(), intent, plan, initialCut, initialAction,
	)
	initial.State = gateway.ReplicatedOperationRunning
	initial.Revision = 7
	initial.Cursor, initial.Proof = replicaMoveActionWitness(
		plan.OperationID(), initial.IntentDigest, plan, initialCut, initialAction,
		replicaMoveCursorApplied,
	)
	journal := &memoryMoveJournal{record: initial, present: true}
	state := bound.baseState
	state.Applied = 9
	state.ConfState = plan.voterConf
	state.ReplicaSetVersion = 9
	observer := &fixedMoveObserver{cut: ReplicatedMoveCut{
		Observation: Observation{
			Catalog: catalog,
			Publication: raftmodel.Publication{
				Applied: 9, ReplicaSetVersion: 9, ConfState: plan.voterConf,
			},
			LeaderStatus: leaderStatus(1, 9), TargetStatus: raftMemberStatus(2, 9),
			TargetState: state,
			TargetProgress: raftmodel.MemberProgress{
				Match: 9, Next: 10, RecentActive: true,
			},
			ProgressFound: true,
		},
		SnapshotBase: &replicatedstate.SnapshotBaseCertificate{
			Manifest: replicatedstate.SnapshotArtifactManifest{State: bound.baseState},
			Digest:   bound.baseDigest,
		},
	}}
	executor := &moveActionExecutor{journal: journal}
	action, err := ExecuteReplicatedMoveStep(
		context.Background(), plan.OperationID(), nil, journal, observer, executor,
	)
	if err != nil || action.Kind != ActionTransferLeader || len(executor.calls) != 1 ||
		!executor.bound || journal.record.IntentDigest != initial.IntentDigest ||
		string(journal.record.Intent) != string(initial.Intent) {
		t.Fatalf("recovered action=%+v bound=%v record=%+v err=%v",
			action, executor.bound, journal.record, err)
	}
}

func TestReplicatedMoveControllerRejectsForgedProofBeforeExecution(t *testing.T) {
	plan, catalog := moveTestPlan(t)
	intent, err := AppendReplicaMoveIntent(nil, catalog, plan)
	if err != nil {
		t.Fatal(err)
	}
	cut := ReplicatedMoveCut{Observation: Observation{
		Catalog: catalog,
		Publication: raftmodel.Publication{
			Applied: 5, ReplicaSetVersion: 4, ConfState: plan.initialConf,
		},
		LeaderStatus: leaderStatus(1, 5),
	}}
	record := newReplicaMoveRecord(
		plan.OperationID(), catalog.Generation(), intent, plan, cut,
		Action{Kind: ActionAddLearner, Member: plan.TargetMember()},
	)
	record.Proof[0]++
	journal := &memoryMoveJournal{record: record, present: true}
	observer := &fixedMoveObserver{cut: cut}
	executor := &moveActionExecutor{journal: journal}
	if _, err = ExecuteReplicatedMoveStep(
		context.Background(), plan.OperationID(), nil, journal, observer, executor,
	); !errors.Is(err, ErrReplicatedMove) || len(executor.calls) != 0 {
		t.Fatalf("forged proof calls=%d err=%v", len(executor.calls), err)
	}
}

func TestReplicatedMoveExecutionProofBindsEveryCommandFence(t *testing.T) {
	plan, catalog := moveTestPlan(t)
	cut := ReplicatedMoveCut{Observation: Observation{
		Catalog: catalog,
		Publication: raftmodel.Publication{
			Applied: 5, ReplicaSetVersion: 4, ConfState: plan.initialConf,
		},
		LeaderStatus: leaderStatus(1, 5),
	}}
	action := Action{Kind: ActionAddLearner, Member: plan.TargetMember()}
	digest := [32]byte{0x31}
	_, original := replicaMoveActionWitness(
		plan.OperationID(), digest, plan, cut, action, replicaMoveCursorExecuting,
	)
	assertChanged := func(name string, nextPlan *Plan, nextCut ReplicatedMoveCut, next Action) {
		t.Helper()
		_, changed := replicaMoveActionWitness(
			plan.OperationID(), digest, nextPlan, nextCut, next, replicaMoveCursorExecuting,
		)
		if changed == original {
			t.Fatalf("%s did not change execution proof", name)
		}
	}
	nextCut := cut
	nextCut.Publication.ReplicaSetVersion++
	assertChanged("replica-set version", plan, nextCut, action)
	nextCut = cut
	nextCut.Publication.Applied++
	assertChanged("publication applied", plan, nextCut, action)
	nextCut = cut
	nextCut.LeaderStatus.Term++
	assertChanged("leader term", plan, nextCut, action)
	nextAction := action
	nextAction.Member++
	assertChanged("member", plan, cut, nextAction)
	bound := bindMoveTestPlan(plan)
	assertChanged("snapshot base", bound, cut, action)
}

func TestReplicatedMoveControllerJournalsPostRemoveFenceRefresh(t *testing.T) {
	plan, sourceCatalog := moveTestPlan(t)
	bound := bindMoveTestPlan(plan)
	targetCatalog, err := plan.CatalogSnapshot(sourceCatalog)
	if err != nil {
		t.Fatal(err)
	}
	state := bound.baseState
	state.Binding.OwnershipEpoch++
	state.Binding.RoutingVersion++
	state.Binding.RouteGeneration++
	state.Applied = 11
	state.ReplicaSetVersion = 11
	state.ConfState = plan.removedConf
	publication := raftmodel.Publication{
		Applied: 11, ReplicaSetVersion: 11, ConfState: plan.removedConf,
	}
	priorCut := ReplicatedMoveCut{Observation: Observation{
		Catalog: targetCatalog, Publication: publication,
		LeaderStatus: leaderStatus(2, 11), TargetStatus: leaderStatus(2, 11),
		TargetState: state, DrainedCatalogGeneration: plan.NextCatalogGeneration(),
	}}
	priorAction := Action{Kind: ActionRemoveSource, Member: plan.RetiringMember()}
	intent, err := AppendReplicaMoveIntent(nil, targetCatalog, bound)
	if err != nil {
		t.Fatal(err)
	}
	record := newReplicaMoveRecord(
		plan.OperationID(), targetCatalog.Generation(), intent, bound, priorCut, priorAction,
	)
	record.State = gateway.ReplicatedOperationRunning
	record.Revision = 20
	record.Cursor, record.Proof = replicaMoveActionWitness(
		plan.OperationID(), record.IntentDigest, bound, priorCut, priorAction,
		replicaMoveCursorApplied,
	)
	journal := &memoryMoveJournal{record: record, present: true}
	refreshCut := priorCut
	refreshCut.SnapshotBase = &replicatedstate.SnapshotBaseCertificate{
		Manifest: replicatedstate.SnapshotArtifactManifest{State: bound.baseState},
		Digest:   bound.baseDigest,
	}
	observer := &fixedMoveObserver{cut: refreshCut}
	executor := &moveActionExecutor{journal: journal}
	action, err := ExecuteReplicatedMoveStep(
		context.Background(), plan.OperationID(), nil, journal, observer, executor,
	)
	if err != nil || action.Kind != ActionRefreshCatalogFence || len(executor.calls) != 1 ||
		!journal.present || action.CatalogGeneration != plan.PostRemoveCatalogGeneration() ||
		action.ReplicaSetVersion != publication.ReplicaSetVersion {
		t.Fatalf("refresh action=%+v calls=%d record=%+v err=%v",
			action, len(executor.calls), journal.record, err)
	}
}

func raftMemberStatus(member, applied uint64) raftmember.RuntimeStatus {
	return raftmember.RuntimeStatus{MemberID: member, Applied: applied}
}
