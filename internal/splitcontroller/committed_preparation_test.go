package splitcontroller

import (
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/gateway"
)

type committedPreparationProbe struct {
	journal     *memoryReplicatedOperationJournal
	calls       int
	lostReceipt bool
}

func (p *committedPreparationProbe) PrepareCommittedPlan(_ context.Context, plan *Plan) error {
	p.calls++
	r := p.journal.record
	if !p.journal.present || r.ID != [32]byte(plan.OperationID()) || r.State != gateway.ReplicatedOperationRunning ||
		r.Cursor != preparationCursor(preparationPending) || r.Proof != preparationProof(r.ID, r.IntentDigest, preparationPending) {
		return ErrReplicatedExecution
	}
	if p.lostReceipt {
		p.lostReceipt = false
		return ErrRuntimeStoreOutcomeUnknown
	}
	return nil
}

type committedAdmissionProbe struct {
	journal *memoryReplicatedOperationJournal
	calls   int
}

func (p *committedAdmissionProbe) AdmitPlan(context.Context, *gateway.Snapshot, *Plan, PlanAdmission) error {
	p.calls++
	r := p.journal.record
	if r.Cursor[0] == 0 && (r.Cursor != preparationCursor(preparationSettled) || r.Proof != preparationProof(r.ID, r.IntentDigest, preparationSettled)) {
		return ErrReplicatedExecution
	}
	return nil
}

type preparationStoppingObserver struct{ calls int }

var errStopAfterCommittedPreparation = errors.New("stop after committed preparation")

func (o *preparationStoppingObserver) ObservePlan(context.Context, *Plan) (Observation, error) {
	o.calls++
	return Observation{}, errStopAfterCommittedPreparation
}

type preparationUnusedGateway struct{}

func (preparationUnusedGateway) ExecuteGatewaySplitAction(context.Context, *Plan, Observation, Action) error {
	return ErrControllerTrigger
}

func TestServingPreparationRequiresCommittedIntentAndResumesLostReceipt(t *testing.T) {
	plan, snapshot, _, _ := testPlan(t)
	journal := &memoryReplicatedOperationJournal{}
	catalog := &testControllerCatalog{memoryReplicatedOperationJournal: journal, catalog: snapshot}
	prepare := &committedPreparationProbe{journal: journal, lostReceipt: true}
	admission := &committedAdmissionProbe{journal: journal}
	observer := &preparationStoppingObserver{}
	newService := func() *ControllerService {
		s, err := NewServingControllerService(catalog, observer, new(testShardControlRouter), admission, preparationUnusedGateway{}, prepare)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	if _, err := newService().ExecuteReplicatedOperation(t.Context(), [32]byte(plan.OperationID())); err == nil || prepare.calls != 0 {
		t.Fatal("uncommitted operation prepared children")
	}
	if _, err := AdmitReplicatedPlan(t.Context(), journal, snapshot, plan); err != nil {
		t.Fatal(err)
	}
	if prepare.calls != 0 {
		t.Fatal("admission allocated child storage")
	}
	if _, err := newService().ExecuteReplicatedOperation(t.Context(), [32]byte(plan.OperationID())); !errors.Is(err, ErrRuntimeStoreOutcomeUnknown) || prepare.calls != 1 || admission.calls != 0 || observer.calls != 0 {
		t.Fatalf("lost receipt calls prepare=%d admission=%d observe=%d err=%v", prepare.calls, admission.calls, observer.calls, err)
	}
	if journal.record.Cursor != preparationCursor(preparationPending) {
		t.Fatal("unknown preparation advanced")
	}
	if _, err := newService().ExecuteReplicatedOperation(t.Context(), [32]byte(plan.OperationID())); !errors.Is(err, errStopAfterCommittedPreparation) || prepare.calls != 2 || admission.calls != 1 {
		t.Fatalf("restart did not resume committed preparation: %v", err)
	}
	if journal.record.Cursor != preparationCursor(preparationSettled) {
		t.Fatal("receipts not durably settled")
	}
	if _, err := newService().ExecuteReplicatedOperation(t.Context(), [32]byte(plan.OperationID())); !errors.Is(err, errStopAfterCommittedPreparation) || prepare.calls != 2 {
		t.Fatal("restart repeated settled preparation")
	}
	for _, kind := range []ActionKind{ActionActivateChild, ActionAwaitCatalogDrain, ActionComplete} {
		journal.record.Cursor = replicatedActionCursor(Action{Kind: kind})
		journal.record.Proof = replicatedActionProof(journal.record.ID, journal.record.Cursor)
		journal.record.State = gateway.ReplicatedOperationRunning
		if kind == ActionComplete {
			journal.record.State = gateway.ReplicatedOperationComplete
		}
		if _, err := newService().ExecuteReplicatedOperation(t.Context(), [32]byte(plan.OperationID())); !errors.Is(err, errStopAfterCommittedPreparation) || prepare.calls != 2 {
			t.Fatalf("late state recreated preparation kind=%d: %v", kind, err)
		}
	}
}

func TestCommittedPreparationRejectsForgedReservedCursorBeforeEffects(t *testing.T) {
	for _, mutate := range []func(*gateway.ReplicatedOperationRecord){
		func(r *gateway.ReplicatedOperationRecord) { r.IntentDigest[0]++ },
		func(r *gateway.ReplicatedOperationRecord) { r.Cursor = preparationCursor(3) },
		func(r *gateway.ReplicatedOperationRecord) {
			r.Cursor = preparationCursor(preparationSettled)
			r.Proof = preparationProof(r.ID, [32]byte{99}, preparationSettled)
		},
		func(r *gateway.ReplicatedOperationRecord) { r.State = gateway.ReplicatedOperationComplete },
		func(r *gateway.ReplicatedOperationRecord) { r.Revision = ^uint64(0) },
	} {
		plan, snapshot, _, _ := testPlan(t)
		journal := &memoryReplicatedOperationJournal{}
		if _, err := AdmitReplicatedPlan(t.Context(), journal, snapshot, plan); err != nil {
			t.Fatal(err)
		}
		mutate(&journal.record)
		prepare := &committedPreparationProbe{journal: journal}
		service := &ControllerService{catalog: &testControllerCatalog{memoryReplicatedOperationJournal: journal, catalog: snapshot}, preparer: prepare}
		if _, err := service.ExecuteReplicatedOperation(t.Context(), [32]byte(plan.OperationID())); err == nil || prepare.calls != 0 {
			t.Fatalf("forged preparation caused effects: %v", err)
		}
	}
}

type preparationPublishCrashCatalog struct {
	*testControllerCatalog
	crash bool
}

func (c *preparationPublishCrashCatalog) PublishOperation(ctx context.Context, revision uint64, record gateway.ReplicatedOperationRecord) error {
	if c.crash && record.Cursor == preparationCursor(preparationSettled) {
		c.crash = false
		return context.Canceled
	}
	return c.testControllerCatalog.PublishOperation(ctx, revision, record)
}
func TestCommittedPreparationCrashAfterReceiptsBeforeCompletionRecord(t *testing.T) {
	plan, snapshot, _, _ := testPlan(t)
	journal := &memoryReplicatedOperationJournal{}
	if _, err := AdmitReplicatedPlan(t.Context(), journal, snapshot, plan); err != nil {
		t.Fatal(err)
	}
	catalog := &preparationPublishCrashCatalog{testControllerCatalog: &testControllerCatalog{memoryReplicatedOperationJournal: journal, catalog: snapshot}, crash: true}
	prepare := &committedPreparationProbe{journal: journal}
	observer := &preparationStoppingObserver{}
	service, err := NewServingControllerService(catalog, observer, new(testShardControlRouter), &committedAdmissionProbe{journal: journal}, preparationUnusedGateway{}, prepare)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.ExecuteReplicatedOperation(t.Context(), [32]byte(plan.OperationID())); !errors.Is(err, context.Canceled) || prepare.calls != 1 || observer.calls != 0 {
		t.Fatalf("completion crash=%v calls=%d", err, prepare.calls)
	}
	if journal.record.Cursor != preparationCursor(preparationPending) {
		t.Fatal("failed completion lost retry authority")
	}
	restarted, err := NewServingControllerService(catalog, observer, new(testShardControlRouter), &committedAdmissionProbe{journal: journal}, preparationUnusedGateway{}, prepare)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.ExecuteReplicatedOperation(t.Context(), [32]byte(plan.OperationID())); !errors.Is(err, errStopAfterCommittedPreparation) || prepare.calls != 2 || observer.calls != 1 {
		t.Fatalf("completion restart=%v calls=%d", err, prepare.calls)
	}
}
