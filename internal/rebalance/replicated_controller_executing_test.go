package rebalance

import (
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"go.etcd.io/raft/v3"
)

// executingIdentityJournal models the observed failure boundary: a stale
// controller may publish a replacement Planned record, but the following
// Executing publication fails before it mutates the journal. The repaired
// controller must never reach that downgrade when the same action is still
// proved by Reconcile.
type executingIdentityJournal struct {
	*memoryMoveJournal
	downgradeObserved             bool
	rejectExecutingAfterDowngrade bool
}

func (journal *executingIdentityJournal) PublishOperation(
	ctx context.Context, expected uint64, record gateway.ReplicatedOperationRecord,
) error {
	if journal.rejectExecutingAfterDowngrade &&
		record.State == gateway.ReplicatedOperationRunning &&
		record.Cursor[3] == replicaMoveCursorExecuting {
		journal.rejectExecutingAfterDowngrade = false
		return errors.New("executing publication rejected after downgrade")
	}
	if journal.present && journal.record.State == gateway.ReplicatedOperationRunning &&
		journal.record.Cursor[3] == replicaMoveCursorExecuting &&
		record.State == gateway.ReplicatedOperationPlanned &&
		record.Cursor[3] == replicaMoveCursorReady &&
		sameReplicaMoveAction(journal.record.Cursor, record.Cursor) {
		journal.downgradeObserved = true
		journal.rejectExecutingAfterDowngrade = true
	}
	return journal.memoryMoveJournal.PublishOperation(ctx, expected, record)
}

type rejectingExecutingJournal struct {
	*memoryMoveJournal
	rejectKind ActionKind
	rejectNext bool
	rejectErr  error
}

func (journal *rejectingExecutingJournal) PublishOperation(
	ctx context.Context, expected uint64, record gateway.ReplicatedOperationRecord,
) error {
	if journal.rejectNext && record.State == gateway.ReplicatedOperationRunning &&
		record.Cursor[3] == replicaMoveCursorExecuting &&
		ActionKind(record.Cursor[0]) == journal.rejectKind {
		journal.rejectNext = false
		return journal.rejectErr
	}
	return journal.memoryMoveJournal.PublishOperation(ctx, expected, record)
}

func TestReplicatedMoveExecutingWitnessSurvivesObserverAdvance(t *testing.T) {
	plan, catalog := moveTestPlan(t)
	observer := &fixedMoveObserver{cut: ReplicatedMoveCut{Observation: Observation{
		Catalog: catalog,
		Publication: raftmodel.Publication{
			Applied: 5, ReplicaSetVersion: 4, ConfState: plan.initialConf,
		},
		LeaderStatus: leaderStatus(1, 5),
	}}}
	baseJournal := &memoryMoveJournal{}
	journal := &executingIdentityJournal{memoryMoveJournal: baseJournal}
	firstFailure := errors.New("learner response became uncertain")
	executor := &moveActionExecutor{journal: baseJournal, fail: firstFailure}
	action, err := ExecuteReplicatedMoveStep(
		context.Background(), plan.OperationID(), plan, journal, observer, executor,
	)
	if !errors.Is(err, firstFailure) || action.Kind != ActionAddLearner ||
		len(executor.executions) != 1 || journal.record.State != gateway.ReplicatedOperationRunning ||
		journal.record.Cursor[3] != replicaMoveCursorExecuting {
		t.Fatalf("uncertain learner action=%+v executions=%d record=%+v err=%v",
			action, len(executor.executions), journal.record, err)
	}
	witness := executor.executions[0]
	priorRevision := journal.record.Revision
	executingCursor := journal.record.Cursor
	executingBase := witness.SnapshotBaseDigest

	// Only the detached publication/status watermarks advance. Membership,
	// replica-set version, leader and term remain the same, so Reconcile still
	// proves the original AddLearner action.
	observer.cut.Publication.Applied = 6
	observer.cut.LeaderStatus.Applied = 6
	observer.cut.LeaderStatus.Commit = 6
	executor.fail = nil
	action, err = ExecuteReplicatedMoveStep(
		context.Background(), plan.OperationID(), nil, journal, observer, executor,
	)
	if journal.downgradeObserved {
		t.Fatal("executing learner was downgraded to Planned before retry")
	}
	if err != nil || action.Kind != ActionAddLearner || len(executor.executions) != 2 {
		t.Fatalf("advanced learner retry action=%+v executions=%d err=%v", action, len(executor.executions), err)
	}
	if got := executor.executions[1]; got != witness {
		t.Fatalf("retry changed exact execution witness: before=%+v after=%+v", witness, got)
	}
	expectedCursor := executingCursor
	expectedCursor[3] = replicaMoveCursorApplied
	expectedProof := replicaMoveActionProof(
		plan.OperationID(), journal.record.IntentDigest, executingBase, expectedCursor,
	)
	if journal.record.Cursor != expectedCursor || journal.record.Proof != expectedProof {
		t.Fatalf("retry rebound execution witness: got cursor=%v proof=%x want cursor=%v proof=%x",
			journal.record.Cursor, journal.record.Proof, expectedCursor, expectedProof)
	}
	if journal.record.State != gateway.ReplicatedOperationRunning ||
		journal.record.Cursor[3] != replicaMoveCursorApplied ||
		journal.record.Revision != priorRevision+1 {
		t.Fatalf("retry settled unexpected record=%+v prior_revision=%d", journal.record, priorRevision)
	}
}

func TestOpenReplicatedMoveExecutionUsesFullBaseDigest(t *testing.T) {
	plan, catalog := moveTestPlan(t)
	cut := ReplicatedMoveCut{Observation: Observation{
		Catalog: catalog,
		Publication: raftmodel.Publication{
			Applied: 5, ReplicaSetVersion: 4, ConfState: plan.initialConf,
		},
		LeaderStatus: leaderStatus(1, 5),
	}}
	action := Action{Kind: ActionAddLearner, Member: plan.TargetMember()}
	intent, err := AppendReplicaMoveIntent(nil, catalog, plan)
	if err != nil {
		t.Fatal(err)
	}

	// A controller can recover a bound plan after an earlier unbound witness
	// was durably admitted. The proof, rather than the cursor tag, identifies
	// which base was actually used by that execution.
	unbound := newReplicaMoveRecord(plan.OperationID(), catalog.Generation(), intent, plan, cut, action)
	unbound.State = gateway.ReplicatedOperationRunning
	unbound.Cursor, unbound.Proof = replicaMoveActionWitness(
		plan.OperationID(), unbound.IntentDigest, plan, cut, action, replicaMoveCursorExecuting,
	)
	bound := bindMoveTestPlan(plan)
	if execution, ok := OpenReplicatedMoveExecution(unbound, bound, cut); !ok ||
		execution.SnapshotBaseDigest != ([32]byte{}) {
		t.Fatalf("historical unbound witness accepted=%t execution=%+v", ok, execution)
	}

	// The low 64 bits are intentionally zero. A tag-only check would classify
	// this valid bound witness as unbound and reject its authenticated proof.
	bound.baseDigest = [32]byte{}
	bound.baseDigest[8] = 1
	bound.baseState.SnapshotBaseDigest = bound.baseDigest
	boundRecord := newReplicaMoveRecord(plan.OperationID(), catalog.Generation(), intent, bound, cut, action)
	boundRecord.State = gateway.ReplicatedOperationRunning
	boundRecord.Cursor, boundRecord.Proof = replicaMoveActionWitness(
		plan.OperationID(), boundRecord.IntentDigest, bound, cut, action, replicaMoveCursorExecuting,
	)
	if boundRecord.Cursor[7] != 0 {
		t.Fatalf("test digest low-word is not zero: cursor=%v", boundRecord.Cursor)
	}
	execution, ok := OpenReplicatedMoveExecution(boundRecord, bound, cut)
	if !ok || execution.SnapshotBaseDigest != bound.baseDigest {
		t.Fatalf("zero-prefix bound witness accepted=%t execution=%+v want_digest=%x",
			ok, execution, bound.baseDigest)
	}
}

// TestOpenReplicatedMoveExecutionRestoresTransitionReceiptDigest exercises
// recovering an action that is already mid-execution (the retry path any
// crash or network partition takes) once its predecessor receipt has since
// become observable. TransitionReceiptDigest is documented as "zero only for
// the first publication of an operation" - it must not stay frozen at the
// stale, receipt-not-yet-found observation an earlier attempt saw.
func TestOpenReplicatedMoveExecutionRestoresTransitionReceiptDigest(t *testing.T) {
	plan, catalog := moveTestPlan(t)
	cut := ReplicatedMoveCut{Observation: Observation{
		Catalog: catalog,
		Publication: raftmodel.Publication{
			Applied: 5, ReplicaSetVersion: 4, ConfState: plan.initialConf,
		},
		LeaderStatus: leaderStatus(1, 5),
	}}
	action := Action{Kind: ActionAddLearner, Member: plan.TargetMember()}
	intent, err := AppendReplicaMoveIntent(nil, catalog, plan)
	if err != nil {
		t.Fatal(err)
	}
	record := newReplicaMoveRecord(plan.OperationID(), catalog.Generation(), intent, plan, cut, action)
	record.State = gateway.ReplicatedOperationRunning
	record.Cursor, record.Proof = replicaMoveActionWitness(
		plan.OperationID(), record.IntentDigest, plan, cut, action, replicaMoveCursorExecuting,
	)

	// No receipt observed yet: the digest stays zero, matching a first
	// publication.
	execution, ok := OpenReplicatedMoveExecution(record, plan, cut)
	if !ok || execution.TransitionReceiptDigest != ([32]byte{}) {
		t.Fatalf("execution=%+v accepted=%t, want zero TransitionReceiptDigest with no receipt observed",
			execution, ok)
	}

	// The predecessor receipt becomes observable on a later recovery of this
	// exact same journaled action.
	receipt := gateway.GroupPublicationReceipt{
		Key: gateway.GroupTransitionKey{
			OperationID: [32]byte(plan.OperationID()), Distribution: "catalog", Shard: "controlplane",
			Group: plan.Group(), SourceAllocationGeneration: 1,
			SourceDescriptorDigest: [32]byte{1}, SourceCommandFenceDigest: [32]byte{2},
		},
		Phase: gateway.TransitionPhasePreRemove,
		PredecessorGroupDigest: [32]byte{3}, PredecessorHeadGeneration: 1, PredecessorHeadDigest: [32]byte{4},
		PredecessorGroupGeneration: 1, PredecessorGroupHeadDigest: [32]byte{5},
		PredecessorRosterDigest: [32]byte{6}, PredecessorRouteDigest: [32]byte{7},
		CommittedHeadGeneration: 2, CommittedHeadDigest: [32]byte{8}, CommittedGroupGeneration: 2,
		CommittedGroupDigest: [32]byte{9}, CommittedRosterDigest: [32]byte{10}, CommittedRouteDigest: [32]byte{11},
		CommittedCommandFenceDigest: [32]byte{12}, CommittedDistributionVersion: 1,
		SourceRouteDigest: [32]byte{13}, SourceRosterDigest: [32]byte{14},
	}
	if !receipt.Valid() {
		t.Fatalf("test receipt is not valid: %+v", receipt)
	}
	wantDigest, err := receipt.ReceiptDigest()
	if err != nil {
		t.Fatal(err)
	}
	cut.TransitionReceipt, cut.TransitionReceiptFound = receipt, true
	execution, ok = OpenReplicatedMoveExecution(record, plan, cut)
	if !ok || execution.TransitionReceiptDigest != wantDigest {
		t.Fatalf("execution=%+v accepted=%t, want TransitionReceiptDigest=%x", execution, ok, wantDigest)
	}
}

func TestReplicatedMoveExecutingLearnerEffectAdvancesToSnapshot(t *testing.T) {
	plan, catalog := moveTestPlan(t)
	observer := &fixedMoveObserver{cut: ReplicatedMoveCut{Observation: Observation{
		Catalog: catalog,
		Publication: raftmodel.Publication{
			Applied: 5, ReplicaSetVersion: 4, ConfState: plan.initialConf,
		},
		LeaderStatus: leaderStatus(1, 5),
	}}}
	journal := &memoryMoveJournal{}
	firstFailure := errors.New("learner effect was delayed")
	executor := &moveActionExecutor{journal: journal, fail: firstFailure}
	action, err := ExecuteReplicatedMoveStep(
		context.Background(), plan.OperationID(), plan, journal, observer, executor,
	)
	if !errors.Is(err, firstFailure) || action.Kind != ActionAddLearner ||
		len(executor.executions) != 1 || journal.record.State != gateway.ReplicatedOperationRunning ||
		journal.record.Cursor[3] != replicaMoveCursorExecuting {
		t.Fatalf("initial learner action=%+v err=%v", action, err)
	}
	observer.cut.Publication = raftmodel.Publication{
		Applied: 6, ReplicaSetVersion: 6, ConfState: plan.learnerConf,
	}
	observer.cut.LeaderStatus = leaderStatus(1, 6)
	executor.fail = nil
	action, err = ExecuteReplicatedMoveStep(
		context.Background(), plan.OperationID(), nil, journal, observer, executor,
	)
	if err != nil || action.Kind != ActionCreateSnapshotBase || len(executor.executions) != 2 {
		t.Fatalf("delayed learner effect action=%+v executions=%d err=%v", action, len(executor.executions), err)
	}
	if executor.executions[0].Action.Kind != ActionAddLearner ||
		executor.executions[1].Action.Kind != ActionCreateSnapshotBase ||
		journal.record.Cursor[3] != replicaMoveCursorApplied {
		t.Fatalf("successor did not settle after learner effect: executions=%+v record=%+v",
			executor.executions, journal.record)
	}
	expectedCursor, expectedProof := replicaMoveActionWitness(
		plan.OperationID(), journal.record.IntentDigest, plan, observer.cut, action, replicaMoveCursorApplied,
	)
	if journal.record.Cursor != expectedCursor || journal.record.Proof != expectedProof {
		t.Fatalf("successor settlement rebound its execution witness: got cursor=%v proof=%x want cursor=%v proof=%x",
			journal.record.Cursor, journal.record.Proof, expectedCursor, expectedProof)
	}
}

func TestReplicatedMoveExecutingLearnerSurvivesLeaderlessCut(t *testing.T) {
	plan, catalog := moveTestPlan(t)
	observer := &fixedMoveObserver{cut: ReplicatedMoveCut{Observation: Observation{
		Catalog: catalog,
		Publication: raftmodel.Publication{
			Applied: 5, ReplicaSetVersion: 4, ConfState: plan.initialConf,
		},
		LeaderStatus: leaderStatus(1, 5),
	}}}
	journal := &memoryMoveJournal{}
	firstFailure := errors.New("learner result became uncertain")
	executor := &moveActionExecutor{journal: journal, fail: firstFailure}
	if action, err := ExecuteReplicatedMoveStep(
		context.Background(), plan.OperationID(), plan, journal, observer, executor,
	); !errors.Is(err, firstFailure) || action.Kind != ActionAddLearner {
		t.Fatalf("initial uncertain learner action=%+v err=%v", action, err)
	}
	prior := journal.record

	observer.cut.Publication.Applied = 6
	observer.cut.LeaderStatus = leaderStatus(1, 6)
	observer.cut.LeaderStatus.LeaderID = 0
	observer.cut.LeaderStatus.RaftState = raft.StateFollower
	action, err := ExecuteReplicatedMoveStep(
		context.Background(), plan.OperationID(), nil, journal, observer, executor,
	)
	if err != nil || action.Kind != ActionAwaitLeader || len(executor.executions) != 1 ||
		!journal.record.Equal(prior) {
		t.Fatalf("leaderless cut replaced uncertain learner: action=%+v executions=%d record=%+v prior=%+v err=%v",
			action, len(executor.executions), journal.record, prior, err)
	}

	observer.cut.Publication = raftmodel.Publication{
		Applied: 7, ReplicaSetVersion: 6, ConfState: plan.learnerConf,
	}
	observer.cut.LeaderStatus = leaderStatus(1, 7)
	executor.fail = nil
	action, err = ExecuteReplicatedMoveStep(
		context.Background(), plan.OperationID(), nil, journal, observer, executor,
	)
	if err != nil || action.Kind != ActionCreateSnapshotBase || len(executor.executions) != 2 ||
		executor.executions[1].Action.Kind != ActionCreateSnapshotBase ||
		journal.record.Cursor[3] != replicaMoveCursorApplied {
		t.Fatalf("learner successor did not recover after leaderless cut: action=%+v executions=%+v record=%+v err=%v",
			action, executor.executions, journal.record, err)
	}
}

func TestReplicatedMoveExecutingWitnessRejectsRegressedEvidence(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*ReplicatedMoveCut, *gateway.ReplicatedOperationRecord, *gateway.Snapshot, *Plan) error
	}{
		{
			name: "lower-applied",
			mutate: func(cut *ReplicatedMoveCut, _ *gateway.ReplicatedOperationRecord, _ *gateway.Snapshot, _ *Plan) error {
				cut.Publication.Applied = 4
				cut.LeaderStatus.Commit = 4
				cut.LeaderStatus.Applied = 4
				return nil
			},
		},
		{
			name: "lower-term",
			mutate: func(cut *ReplicatedMoveCut, _ *gateway.ReplicatedOperationRecord, _ *gateway.Snapshot, _ *Plan) error {
				cut.LeaderStatus.Term = 2
				return nil
			},
		},
		{
			name: "changed-replica-set",
			mutate: func(cut *ReplicatedMoveCut, _ *gateway.ReplicatedOperationRecord, _ *gateway.Snapshot, _ *Plan) error {
				cut.Publication.Applied = 6
				cut.Publication.ReplicaSetVersion = 5
				cut.LeaderStatus.Commit = 6
				cut.LeaderStatus.Applied = 6
				return nil
			},
		},
		{
			name: "successor-lower-term",
			mutate: func(cut *ReplicatedMoveCut, _ *gateway.ReplicatedOperationRecord, _ *gateway.Snapshot, plan *Plan) error {
				cut.Publication.Applied = 6
				cut.Publication.ReplicaSetVersion = 6
				cut.Publication.ConfState = plan.learnerConf
				cut.LeaderStatus.Commit = 6
				cut.LeaderStatus.Applied = 6
				cut.LeaderStatus.Term = 2
				return nil
			},
		},
		{
			name: "changed-catalog",
			mutate: func(cut *ReplicatedMoveCut, _ *gateway.ReplicatedOperationRecord, catalog *gateway.Snapshot, plan *Plan) error {
				changed, err := plan.CatalogSnapshot(catalog)
				if err != nil {
					return err
				}
				cut.Catalog = changed
				return nil
			},
		},
		{
			name: "corrupted-proof",
			mutate: func(_ *ReplicatedMoveCut, record *gateway.ReplicatedOperationRecord, _ *gateway.Snapshot, _ *Plan) error {
				record.Proof[0]++
				return nil
			},
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			plan, catalog := moveTestPlan(t)
			observer := &fixedMoveObserver{cut: ReplicatedMoveCut{Observation: Observation{
				Catalog: catalog,
				Publication: raftmodel.Publication{
					Applied: 5, ReplicaSetVersion: 4, ConfState: plan.initialConf,
				},
				LeaderStatus: leaderStatus(1, 5),
			}}}
			journal := &memoryMoveJournal{}
			firstFailure := errors.New("learner result became uncertain")
			executor := &moveActionExecutor{journal: journal, fail: firstFailure}
			if _, err := ExecuteReplicatedMoveStep(
				context.Background(), plan.OperationID(), plan, journal, observer, executor,
			); !errors.Is(err, firstFailure) {
				t.Fatalf("initial uncertain learner error=%v", err)
			}
			if err := testCase.mutate(&observer.cut, &journal.record, catalog, plan); err != nil {
				t.Fatal(err)
			}
			expected := journal.record
			if _, err := ExecuteReplicatedMoveStep(
				context.Background(), plan.OperationID(), nil, journal, observer, executor,
			); err == nil || len(executor.executions) != 1 || !journal.record.Equal(expected) {
				t.Fatalf("unsafe executing witness accepted: calls=%d record=%+v prior=%+v err=%v",
					len(executor.executions), journal.record, expected, err)
			}
		})
	}
}

func TestReplicatedMoveSuccessorPublicationFailureRetainsPlannedRecord(t *testing.T) {
	plan, catalog := moveTestPlan(t)
	observer := &fixedMoveObserver{cut: ReplicatedMoveCut{Observation: Observation{
		Catalog: catalog,
		Publication: raftmodel.Publication{
			Applied: 5, ReplicaSetVersion: 4, ConfState: plan.initialConf,
		},
		LeaderStatus: leaderStatus(1, 5),
	}}}
	baseJournal := &memoryMoveJournal{}
	publicationFailure := errors.New("successor execution publication failed")
	journal := &rejectingExecutingJournal{
		memoryMoveJournal: baseJournal, rejectKind: ActionCreateSnapshotBase,
		rejectNext: true, rejectErr: publicationFailure,
	}
	executor := &moveActionExecutor{journal: baseJournal}
	if action, err := ExecuteReplicatedMoveStep(
		context.Background(), plan.OperationID(), plan, journal, observer, executor,
	); err != nil || action.Kind != ActionAddLearner {
		t.Fatalf("initial learner action=%+v err=%v", action, err)
	}
	observer.cut.Publication = raftmodel.Publication{
		Applied: 6, ReplicaSetVersion: 6, ConfState: plan.learnerConf,
	}
	observer.cut.LeaderStatus = leaderStatus(1, 6)
	action, err := ExecuteReplicatedMoveStep(
		context.Background(), plan.OperationID(), nil, journal, observer, executor,
	)
	if !errors.Is(err, publicationFailure) || action != (Action{}) || len(executor.executions) != 1 {
		t.Fatalf("successor publication failure action=%+v executions=%d err=%v", action, len(executor.executions), err)
	}
	if journal.record.State != gateway.ReplicatedOperationPlanned ||
		journal.record.Cursor[3] != replicaMoveCursorReady ||
		ActionKind(journal.record.Cursor[0]) != ActionCreateSnapshotBase {
		t.Fatalf("failed successor publication mutated durable phase: record=%+v", journal.record)
	}

	action, err = ExecuteReplicatedMoveStep(
		context.Background(), plan.OperationID(), nil, journal, observer, executor,
	)
	if err != nil || action.Kind != ActionCreateSnapshotBase || len(executor.executions) != 2 ||
		journal.record.State != gateway.ReplicatedOperationRunning ||
		journal.record.Cursor[3] != replicaMoveCursorApplied {
		t.Fatalf("successor retry action=%+v executions=%d record=%+v err=%v",
			action, len(executor.executions), journal.record, err)
	}
}
