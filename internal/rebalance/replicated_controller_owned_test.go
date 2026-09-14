package rebalance

import (
	"bytes"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
)

func ownedControllerTestPlan(t testing.TB) (*Plan, *fixedMoveObserver) {
	t.Helper()
	cut := failedReplicaEnrolledTestCut(t)
	planned, err := PlanFailedReplicaReplacement(cut)
	if err != nil {
		t.Fatal(err)
	}
	if !planned.Plan.transitionReady {
		t.Fatal("fixture has no durable owned transition")
	}
	leader := leaderStatus(cut.Leader.MemberID, cut.Publication.Applied)
	leader.Term = cut.Leader.Term
	return planned.Plan, &fixedMoveObserver{cut: ReplicatedMoveCut{Observation: Observation{
		Catalog: cut.Catalog, Publication: cut.Publication, LeaderStatus: leader,
	}}}
}

func advanceOwnedControllerTestCut(t testing.TB, observer *fixedMoveObserver) {
	t.Helper()
	observer.cut.Catalog = advanceOwnedTestHead(t, observer.cut.Catalog, observer.cut.Catalog.Generation()+10)
	observer.cut.Publication.Applied++
	observer.cut.LeaderStatus.Applied++
	observer.cut.LeaderStatus.Commit++
	observer.cut.LeaderStatus.Term++
}

func TestReplicatedMoveOwnedPlannedLearnerSurvivesUnrelatedHeadAdvance(t *testing.T) {
	plan, observer := ownedControllerTestPlan(t)
	record, err := PrepareReplicatedMoveRecord(t.Context(), plan, observer)
	if err != nil || ActionKind(record.Cursor[0]) != ActionAddLearner {
		t.Fatalf("prepare learner: cursor=%v err=%v", record.Cursor, err)
	}
	journal := &memoryMoveJournal{record: record, present: true}
	executor := &moveActionExecutor{journal: journal}
	advanceOwnedControllerTestCut(t, observer)
	action, err := ExecuteReplicatedMoveStep(t.Context(), plan.OperationID(), nil, journal, observer, executor)
	if err != nil || action.Kind != ActionAddLearner || len(executor.executions) != 1 {
		t.Fatalf("owned planned learner stranded by unrelated head: action=%+v calls=%d err=%v", action, len(executor.executions), err)
	}
	if journal.record.IntentDigest != record.IntentDigest || journal.record.CatalogGeneration != observer.cut.Catalog.Generation() {
		t.Fatal("planned refresh changed intent or retained stale observation head")
	}
}

func TestReplicatedMoveOwnedExecutingSnapshotSurvivesUnrelatedHeadAdvance(t *testing.T) {
	plan, observer := ownedControllerTestPlan(t)
	observer.cut.Publication.Applied++
	observer.cut.Publication.ReplicaSetVersion = observer.cut.Publication.Applied
	observer.cut.Publication.ConfState = plan.learnerConf
	observer.cut.LeaderStatus.Applied = observer.cut.Publication.Applied
	observer.cut.LeaderStatus.Commit = observer.cut.Publication.Applied
	journal := &memoryMoveJournal{}
	uncertain := errors.New("snapshot reply lost")
	executor := &moveActionExecutor{journal: journal, fail: uncertain}
	action, err := ExecuteReplicatedMoveStep(t.Context(), plan.OperationID(), plan, journal, observer, executor)
	if !errors.Is(err, uncertain) || action.Kind != ActionCreateSnapshotBase || len(executor.executions) != 1 {
		t.Fatalf("start snapshot: action=%+v calls=%d err=%v", action, len(executor.executions), err)
	}
	before := journal.record
	witness := executor.executions[0]
	advanceOwnedControllerTestCut(t, observer)
	action, err = ExecuteReplicatedMoveStep(t.Context(), plan.OperationID(), nil, journal, observer, executor)
	if err != nil || action.Kind != ActionCreateSnapshotBase || len(executor.executions) != 2 {
		t.Fatalf("owned executing snapshot stranded by unrelated head: action=%+v calls=%d err=%v", action, len(executor.executions), err)
	}
	if executor.executions[1] != witness || journal.record.Revision != before.Revision+1 ||
		journal.record.State != gateway.ReplicatedOperationRunning || journal.record.Cursor[3] != replicaMoveCursorApplied ||
		journal.record.CatalogGeneration != before.CatalogGeneration {
		t.Fatal("snapshot retry changed the admitted execution witness")
	}
}

func TestReplicatedMoveOwnedHeadAdvanceRejectsChangedEvidence(t *testing.T) {
	for _, executing := range []bool{false, true} {
		for _, change := range []string{"head", "record-head", "proof", "base", "descriptor", "roster", "applied", "term", "replica-set"} {
			name := "planned/" + change
			if executing {
				name = "executing/" + change
			}
			t.Run(name, func(t *testing.T) {
				plan, observer := ownedControllerTestPlan(t)
				observer.cut.Catalog = advanceOwnedTestHead(t, observer.cut.Catalog, 19)
				if executing {
					observer.cut.Publication.Applied++
					observer.cut.Publication.ReplicaSetVersion = observer.cut.Publication.Applied
					observer.cut.Publication.ConfState = plan.learnerConf
					observer.cut.LeaderStatus.Applied = observer.cut.Publication.Applied
					observer.cut.LeaderStatus.Commit = observer.cut.Publication.Applied
				}
				// Admission may follow an unrelated head publication, while the
				// operation retains its immutable source-generation provenance.
				recovered, err := OpenReplicaMoveIntent(mustOwnedControllerIntent(t, plan), observer.cut.Catalog, observer.cut.Publication, nil)
				if err != nil {
					t.Fatal(err)
				}
				action, err := Reconcile(recovered, observer.cut.Observation)
				if err != nil {
					t.Fatal(err)
				}
				record := newReplicaMoveRecord(plan.OperationID(), 19, mustOwnedControllerIntent(t, plan), recovered, observer.cut, action)
				if executing {
					record.State = gateway.ReplicatedOperationRunning
					record.Cursor, record.Proof = replicaMoveActionWitness(plan.OperationID(), record.IntentDigest, recovered, observer.cut, action, replicaMoveCursorExecuting)
				}
				advanceOwnedControllerTestCut(t, observer)
				switch change {
				case "head":
					observer.cut.Catalog = advanceOwnedTestHead(t, observer.cut.Catalog, 18)
				case "record-head":
					record.CatalogGeneration = plan.CatalogGeneration() - 1
				case "proof":
					record.Proof[0]++
				case "base":
					record.Cursor[7] = 1
					record.Proof = replicaMoveActionProof(plan.OperationID(), record.IntentDigest, [32]byte{1}, record.Cursor)
				case "descriptor", "roster":
					raw, err := gateway.AppendSnapshotDocument(nil, observer.cut.Catalog)
					if err != nil {
						t.Fatal(err)
					}
					from, to := []byte(`"logical_schema_digest":"63`), []byte(`"logical_schema_digest":"64`)
					if change == "roster" {
						from, to = []byte(`"node_incarnation":21`), []byte(`"node_incarnation":31`)
					}
					if !bytes.Contains(raw, from) {
						t.Fatal("catalog mutation fixture not found")
					}
					observer.cut.Catalog, err = gateway.OpenSnapshotDocument(bytes.ReplaceAll(raw, from, to))
					if err != nil {
						t.Fatal(err)
					}
				case "applied":
					observer.cut.Publication.Applied = record.Cursor[5] - 1
				case "term":
					observer.cut.LeaderStatus.Term = record.Cursor[6] - 1
				case "replica-set":
					observer.cut.Publication.ReplicaSetVersion++
				}
				journal := &memoryMoveJournal{record: record, present: true}
				executor := &moveActionExecutor{journal: journal}
				if _, err := ExecuteReplicatedMoveStep(t.Context(), plan.OperationID(), nil, journal, observer, executor); err == nil ||
					len(executor.calls) != 0 || journal.record.Revision != record.Revision {
					t.Fatalf("changed evidence accepted: calls=%d revision=%d err=%v", len(executor.calls), journal.record.Revision, err)
				}
			})
		}
	}
}

func mustOwnedControllerIntent(t testing.TB, plan *Plan) []byte {
	t.Helper()
	cut := failedReplicaEnrolledTestCut(t)
	raw, err := AppendReplicaMoveIntent(nil, cut.Catalog, plan)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestReplicatedMoveCompletedCleanupSurvivesObserverAdvance(t *testing.T) {
	for _, change := range []string{"advance", "proof", "base", "applied", "term", "replica-set"} {
		t.Run(change, func(t *testing.T) {
			initial, source := moveTestPlan(t)
			plan := bindMoveTestPlan(initial)
			intent, err := AppendReplicaMoveIntent(nil, source, initial)
			if err != nil {
				t.Fatal(err)
			}
			state := plan.baseState
			state.Applied, state.ReplicaSetVersion, state.ConfState = 11, 11, plan.removedConf
			state.Binding.OwnershipEpoch++
			state.Binding.RoutingVersion++
			state.Binding.RouteGeneration++
			observer := &fixedMoveObserver{cut: ReplicatedMoveCut{Observation: Observation{
				Catalog:      moveTestPostRemoveCatalog(t, plan, 11),
				Publication:  raftmodel.Publication{Applied: 11, ReplicaSetVersion: 11, ConfState: plan.removedConf},
				LeaderStatus: leaderStatus(plan.TargetMember(), 11), TargetState: state,
				DrainedCatalogGeneration: 11, RetiringReplicaRetired: true,
			}, SnapshotBase: &replicatedstate.SnapshotBaseCertificate{
				Manifest: replicatedstate.SnapshotArtifactManifest{State: plan.baseState}, Digest: plan.baseDigest,
			}}}
			action, err := Reconcile(plan, observer.cut.Observation)
			if err != nil || action.Kind != ActionComplete {
				t.Fatalf("terminal fixture action=%+v err=%v", action, err)
			}
			record := newReplicaMoveRecord(plan.OperationID(), 11, intent, plan, observer.cut, action)
			record.State = gateway.ReplicatedOperationComplete
			record.Cursor, record.Proof = replicaMoveActionWitness(plan.OperationID(), record.IntentDigest, plan, observer.cut, action, replicaMoveCursorApplied)
			observer.cut.Publication.Applied++
			observer.cut.TargetState.Applied++
			observer.cut.LeaderStatus.Applied++
			observer.cut.LeaderStatus.Commit++
			observer.cut.LeaderStatus.Term++
			switch change {
			case "proof":
				record.Proof[0]++
			case "base":
				observer.cut.SnapshotBase.Digest[8]++
				observer.cut.SnapshotBase.Manifest.State.SnapshotBaseDigest = observer.cut.SnapshotBase.Digest
			case "applied":
				observer.cut.Publication.Applied = record.Cursor[5] - 1
			case "term":
				observer.cut.LeaderStatus.Term = record.Cursor[6] - 1
			case "replica-set":
				observer.cut.Publication.ReplicaSetVersion++
			}
			journal := &memoryMoveJournal{record: record, present: true}
			executor := &moveActionExecutor{journal: journal}
			action, err = ExecuteReplicatedMoveStep(t.Context(), plan.OperationID(), nil, journal, observer, executor)
			if change == "advance" {
				if err != nil || action.Kind != ActionComplete || journal.present {
					t.Fatalf("terminal observation advance stranded cleanup: action=%+v present=%t err=%v", action, journal.present, err)
				}
			} else if err == nil || !journal.present {
				t.Fatalf("changed terminal evidence accepted: present=%t err=%v", journal.present, err)
			}
			if len(executor.calls) != 0 {
				t.Fatal("completed operation executed another action")
			}
		})
	}
}

func TestReplicatedMoveOwnedCompletedCleanupSurvivesUnrelatedHeadAdvance(t *testing.T) {
	initial, observer := ownedControllerTestPlan(t)
	intent := mustOwnedControllerIntent(t, initial)
	plan := bindMoveTestPlan(initial)
	observer.cut.Catalog = advanceOwnedTestHead(t, observer.cut.Catalog, 19)
	command := plan.transition.SourceDescriptor.Command
	command.ReplicaSetVersion = 40
	command.OwnershipEpoch++
	command.RoutingVersion++
	command.RouteGeneration++
	pre, err := plan.CatalogSnapshotAtHead(observer.cut.Catalog, gateway.TransitionPhasePreRemove, plan.transition.Replacement, command)
	if err != nil {
		t.Fatal(err)
	}
	command.ReplicaSetVersion++
	post, err := plan.CatalogSnapshotAtHead(pre, gateway.TransitionPhasePostRemove, plan.transition.Replacement, command)
	if err != nil {
		t.Fatal(err)
	}
	preDescriptor := pre.ReplicatedShardDescriptors()[0]
	postDescriptor := post.ReplicatedShardDescriptors()[0]
	receipt := gateway.GroupPublicationReceipt{
		Key: plan.transition.Key, Phase: gateway.TransitionPhasePostRemove,
		PredecessorHeadGeneration: pre.Generation(), PredecessorHeadDigest: [32]byte{1},
		PredecessorGroupGeneration: preDescriptor.Command.RouteGeneration, PredecessorGroupHeadDigest: [32]byte{2},
		PredecessorGroupDigest:  gateway.DigestReplicatedShardDescriptor(preDescriptor),
		PredecessorRosterDigest: gateway.DigestReplicaRoster(preDescriptor.Replicas),
		PredecessorRouteDigest:  gateway.DigestRouteFor(pre, plan.request.Distribution, plan.request.Shard),
		CommittedHeadGeneration: post.Generation(), CommittedHeadDigest: [32]byte{3},
		CommittedGroupGeneration:     postDescriptor.Command.RouteGeneration,
		CommittedGroupDigest:         gateway.DigestReplicatedShardDescriptor(postDescriptor),
		CommittedRosterDigest:        gateway.DigestReplicaRoster(postDescriptor.Replicas),
		CommittedRouteDigest:         gateway.DigestRouteFor(post, plan.request.Distribution, plan.request.Shard),
		CommittedCommandFenceDigest:  gateway.DigestCommandFence(command),
		CommittedDistributionVersion: plan.transition.TargetDistributionVersion,
		SourceRouteDigest:            plan.transition.SourceRouteDigest, SourceRosterDigest: plan.transition.SourceRosterDigest,
	}
	if !receipt.Valid() {
		t.Fatal("invalid committed receipt fixture")
	}
	state := plan.baseState
	state.Applied, state.ReplicaSetVersion, state.ConfState = 41, 41, plan.removedConf
	state.Binding.OwnershipEpoch++
	state.Binding.RoutingVersion++
	state.Binding.RouteGeneration++
	observer.cut = ReplicatedMoveCut{Observation: Observation{
		Catalog: post, Publication: raftmodel.Publication{Applied: 41, ReplicaSetVersion: 41, ConfState: plan.removedConf},
		LeaderStatus: leaderStatus(plan.TargetMember(), 41), TargetState: state,
		DrainedCatalogGeneration: post.Generation(), RetiringReplicaRetired: true,
		TransitionReceipt: receipt, TransitionReceiptFound: true,
	}, SnapshotBase: &replicatedstate.SnapshotBaseCertificate{
		Manifest: replicatedstate.SnapshotArtifactManifest{State: plan.baseState}, Digest: plan.baseDigest,
	}}
	action, err := Reconcile(plan, observer.cut.Observation)
	if err != nil || action.Kind != ActionComplete {
		t.Fatalf("owned terminal fixture action=%+v err=%v", action, err)
	}
	record := newReplicaMoveRecord(plan.OperationID(), post.Generation(), intent, plan, observer.cut, action)
	record.State = gateway.ReplicatedOperationComplete
	record.Cursor, record.Proof = replicaMoveActionWitness(plan.OperationID(), record.IntentDigest, plan, observer.cut, action, replicaMoveCursorApplied)
	advanceOwnedControllerTestCut(t, observer)
	observer.cut.TargetState.Applied++
	journal := &memoryMoveJournal{record: record, present: true}
	executor := &moveActionExecutor{journal: journal}
	action, err = ExecuteReplicatedMoveStep(t.Context(), plan.OperationID(), nil, journal, observer, executor)
	if err != nil || action.Kind != ActionComplete || journal.present || len(executor.calls) != 0 {
		t.Fatalf("owned terminal cleanup stranded: action=%+v present=%t calls=%d err=%v", action, journal.present, len(executor.calls), err)
	}
}
