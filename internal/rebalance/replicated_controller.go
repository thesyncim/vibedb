package rebalance

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
)

var ErrReplicatedMove = errors.New("rebalance: replicated replica move conflicts with durable evidence")

const (
	replicaMoveCursorReady uint64 = iota + 1
	replicaMoveCursorExecuting
	replicaMoveCursorApplied
)

// ReplicatedOperationJournal is the sole durable controller journal. The
// shipped implementation is gateway.ReplicatedCatalogAuthority, backed by the
// catalog RF3 group; the controller creates no local side journal or second
// consensus authority.
type ReplicatedOperationJournal interface {
	ReadOperation(context.Context, [32]byte) (gateway.ReplicatedOperationRecord, error)
	SubmitOperation(context.Context, gateway.ReplicatedOperationRecord) error
	PublishOperation(context.Context, uint64, gateway.ReplicatedOperationRecord) error
	DeleteOperation(context.Context, [32]byte, uint64) error
	RetryPending(context.Context) error
}

// ReplicatedOperationSetJournal atomically admits multiple independent move
// records into the catalog RF3 work directory. Execution remains a per-group
// saga; only admission is cross-group atomic.
type ReplicatedOperationSetJournal interface {
	SubmitOperationsIfDirectory(context.Context, []gateway.ReplicatedOperationRecord, [][32]byte) error
}

// ReplicatedMoveCut is one detached observation. SnapshotBase is the verified
// durable certificate recovered from the shard runtime after snapshot
// creation. It is never copied into the bounded catalog operation record.
type ReplicatedMoveCut struct {
	Observation
	SnapshotBase *replicatedstate.SnapshotBaseCertificate
}

// ReplicatedMoveObserver obtains the complete evidence cut for one exact
// operation identity. Command, transport, and snapshot wiring remain outside
// the controller core.
type ReplicatedMoveObserver interface {
	ObserveReplicaMove(
		context.Context, OperationID, gateway.ReplicatedOperationRecord, *Plan,
	) (ReplicatedMoveCut, error)
}

// ReplicatedMoveActionExecutor executes one idempotent action. OperationID and
// Action are the complete retry key. Returning nil means the action's durable
// effect has completed; an outcome-unknown crash can invoke the same tuple
// again safely.
type ReplicatedMoveActionExecutor interface {
	ExecuteReplicaMove(context.Context, OperationID, *Plan, ReplicatedMoveExecution) error
}

// ReplicatedMoveExecution is the exact, constant-size request witness passed to
// an executor. Reconcile's Action alone is not an idempotency key: membership
// commands depend on the observed replica-set version, and leader transfer is
// term-fenced. Proof also commits to the full snapshot-base digest when bound.
type ReplicatedMoveExecution struct {
	Action                Action
	PublicationApplied    uint64
	PublicationReplicaSet uint64
	LeaderTerm            uint64
	SnapshotBaseDigest    [32]byte
	// TransitionReceiptDigest is the predecessor receipt observed immediately
	// before a receipt-aware publication. It is zero only for the first
	// publication of an operation.
	TransitionReceiptDigest [32]byte
	Proof                   [32]byte
}

// OpenReplicatedMoveExecution verifies the already-journaled execution cut.
// A move-set publisher may combine siblings only after each one has frozen
// its own exact publication action; a fresh observation alone is not enough.
//
// TransitionReceiptDigest is deliberately excluded from that frozen witness:
// unlike Proof, it is not part of this action's idempotency identity - it is
// only a pass-through of the already-durable predecessor receipt a
// receipt-aware publication chains against. The caller's current cut
// supplies it fresh on every call (including every retry), the same way a
// first-time execution derives it, so a receipt published after this
// action's cursor first entered the executing state is never permanently
// invisible to it.
func OpenReplicatedMoveExecution(
	record gateway.ReplicatedOperationRecord, plan *Plan, cut ReplicatedMoveCut,
) (ReplicatedMoveExecution, bool) {
	if plan == nil || !validReplicaMoveRecord(record, plan.OperationID()) || record.State != gateway.ReplicatedOperationRunning || record.Cursor[3] != replicaMoveCursorExecuting {
		return ReplicatedMoveExecution{}, false
	}
	base, ok := replicaMoveRecordBaseDigest(record, plan)
	if !ok {
		return ReplicatedMoveExecution{}, false
	}
	if record.Proof != replicaMoveActionProof(plan.OperationID(), record.IntentDigest, base, record.Cursor) {
		return ReplicatedMoveExecution{}, false
	}
	action := Action{Kind: ActionKind(record.Cursor[0]), Member: record.Cursor[1], CatalogGeneration: record.Cursor[2]}
	if action.Kind == ActionRefreshCatalogFence {
		action.ReplicaSetVersion = record.Cursor[4]
	}
	execution := ReplicatedMoveExecution{Action: action, PublicationApplied: record.Cursor[5],
		PublicationReplicaSet: record.Cursor[4], LeaderTerm: record.Cursor[6], SnapshotBaseDigest: base, Proof: record.Proof}
	if cut.TransitionReceiptFound {
		if digest, err := cut.TransitionReceipt.ReceiptDigest(); err == nil {
			execution.TransitionReceiptDigest = digest
		}
	}
	return execution, true
}

// ExecuteReplicatedMoveStep recovers one immutable move intent, derives exactly
// one action from current durable evidence, journals it before execution, and
// journals successful completion afterwards. The optional initial plan is used
// only to submit a missing operation; restarts recover exclusively from the
// canonical record and observed authorities.
func ExecuteReplicatedMoveStep(
	ctx context.Context,
	operation OperationID,
	initial *Plan,
	journal ReplicatedOperationJournal,
	observer ReplicatedMoveObserver,
	executor ReplicatedMoveActionExecutor,
) (Action, error) {
	if ctx == nil || operation == (OperationID{}) || journal == nil || observer == nil ||
		executor == nil || initial != nil && initial.OperationID() != operation {
		return Action{}, ErrReplicatedMove
	}
	record, readErr := journal.ReadOperation(ctx, [32]byte(operation))
	if readErr != nil && !errors.Is(readErr, gateway.ErrReplicatedOperationMissing) {
		return Action{}, readErr
	}
	cut, err := observer.ObserveReplicaMove(ctx, operation, record, initial)
	if err != nil || cut.Catalog == nil {
		return Action{}, errors.Join(err, ErrReplicatedMove)
	}
	var plan *Plan
	if errors.Is(readErr, gateway.ErrReplicatedOperationMissing) {
		if initial == nil {
			return Action{}, gateway.ErrReplicatedOperationMissing
		}
		plan = initial
		intent, appendErr := AppendReplicaMoveIntent(nil, cut.Catalog, plan)
		if appendErr != nil {
			return Action{}, errors.Join(appendErr, ErrReplicatedMove)
		}
		action, reconcileErr := Reconcile(plan, cut.Observation)
		if reconcileErr != nil {
			return Action{}, reconcileErr
		}
		record = newReplicaMoveRecord(
			operation, cut.Catalog.Generation(), intent, plan, cut, action,
		)
		if err = settleReplicaMoveSubmit(ctx, journal, record); err != nil {
			return Action{}, err
		}
	} else {
		if !validReplicaMoveRecord(record, operation) {
			return Action{}, ErrReplicatedMove
		}
		plan, err = OpenReplicaMoveIntent(
			record.Intent, cut.Catalog, cut.Publication, cut.SnapshotBase,
		)
		if err != nil || plan.OperationID() != operation {
			return Action{}, fmt.Errorf("rebalance: recover move intent: %w", errors.Join(err, ErrReplicatedMove))
		}
	}
	action, err := Reconcile(plan, cut.Observation)
	if err != nil {
		return Action{}, fmt.Errorf("rebalance: reconcile move: %w", err)
	}
	if (action.Kind == ActionRefreshCatalogFence) != (action.ReplicaSetVersion != 0) ||
		action.ReplicaSetVersion != 0 &&
			action.ReplicaSetVersion != cut.Publication.ReplicaSetVersion {
		return Action{}, ErrReplicatedMove
	}
	if record.State == gateway.ReplicatedOperationComplete {
		if action.Kind != ActionComplete || !replicaMoveRecordMatches(
			record, operation, plan, cut, action, replicaMoveCursorApplied,
		) {
			return Action{}, fmt.Errorf("%w: completed record differs from current action %d", ErrReplicatedMove, action.Kind)
		}
		if err = settleReplicaMoveDelete(ctx, journal, record); err != nil {
			return Action{}, err
		}
		return action, nil
	}
	recordBaseDigest, recordBaseOK := replicaMoveRecordBaseDigest(record, plan)
	if !recordBaseOK || record.Proof != replicaMoveActionProof(
		operation, record.IntentDigest, recordBaseDigest, record.Cursor,
	) {
		return Action{}, fmt.Errorf("%w: action witness digest differs at cursor %v", ErrReplicatedMove, record.Cursor)
	}
	if record.State == gateway.ReplicatedOperationRunning &&
		record.Cursor[3] == replicaMoveCursorExecuting &&
		(record.Cursor[5] > cut.Publication.Applied || record.Cursor[6] > cut.LeaderStatus.Term) {
		return Action{}, fmt.Errorf("%w: executing evidence regressed at cursor %v publication=%d term=%d", ErrReplicatedMove, record.Cursor, cut.Publication.Applied, cut.LeaderStatus.Term)
	}
	// A prepared snapshot export and its target bootstrap share one exact Step.
	// In particular, a catalog self-move advances Publication.Applied just by
	// journaling this action. Keep its admitted witness across retries; making a
	// new proof would strand the already prepared source export or bootstrap.
	// Reconcile above still proves the current membership/catalog stage, and no
	// replica-set, snapshot-base, or catalog authority change is accepted here.
	if action.Kind == ActionCreateSnapshotBase &&
		(record.State == gateway.ReplicatedOperationPlanned && record.Cursor[3] == replicaMoveCursorReady ||
			record.State == gateway.ReplicatedOperationRunning && record.Cursor[3] == replicaMoveCursorExecuting) &&
		sameReplicaMoveAction(record.Cursor, replicaMoveActionCursor(action, record.Cursor[3], plan, cut)) {
		if (!plan.transitionReady && record.CatalogGeneration != cut.Catalog.Generation()) ||
			record.Cursor[4] != cut.Publication.ReplicaSetVersion ||
			record.Cursor[5] > cut.Publication.Applied || record.Cursor[6] > cut.LeaderStatus.Term ||
			record.Proof != replicaMoveActionProof(operation, record.IntentDigest, recordBaseDigest, record.Cursor) {
			return Action{}, fmt.Errorf("%w: snapshot witness cursor=%v catalog=%d/%d publication=%d/%d term=%d", ErrReplicatedMove, record.Cursor, record.CatalogGeneration, cut.Catalog.Generation(), cut.Publication.ReplicaSetVersion, cut.Publication.Applied, cut.LeaderStatus.Term)
		}
		cut.Publication.Applied = record.Cursor[5]
		cut.LeaderStatus.Term = record.Cursor[6]
	}
	wanted, wantedProof := replicaMoveActionWitness(
		operation, record.IntentDigest, plan, cut, action, replicaMoveCursorReady,
	)
	currentCursor := wanted
	currentCursor[3] = record.Cursor[3]
	currentProof := replicaMoveActionProof(
		operation, record.IntentDigest, recordBaseDigest, currentCursor,
	)
	if record.Cursor == currentCursor && record.Proof != currentProof {
		return Action{}, fmt.Errorf("%w: action witness digest differs at cursor %v", ErrReplicatedMove, record.Cursor)
	}
	executingEvidenceOK := record.State == gateway.ReplicatedOperationRunning &&
		record.Cursor[3] == replicaMoveCursorExecuting &&
		record.Cursor[5] <= cut.Publication.Applied &&
		record.Cursor[6] <= cut.LeaderStatus.Term &&
		record.CatalogGeneration == cut.Catalog.Generation() &&
		record.Cursor[4] == cut.Publication.ReplicaSetVersion &&
		recordBaseOK && recordBaseDigest == replicaMovePlanBaseDigest(plan)
	sameAction := sameReplicaMoveAction(record.Cursor, wanted)
	// ActionRefreshCatalogFence.ReplicaSetVersion is always re-derived from
	// the live observation (Reconcile sets it from
	// observed.Publication.ReplicaSetVersion), unlike other actions' frozen
	// parameters: it is expected to legitimately advance across retries as
	// unrelated catalog activity commits. That advance alone must not read as
	// "evidence regressed" and permanently strand this action; the cursor
	// mismatch it produces still falls through to the ordinary re-plan path
	// below, which re-freezes the witness against the current live value.
	refreshFenceAdvanced := action.Kind == ActionRefreshCatalogFence &&
		ActionKind(record.Cursor[0]) == ActionRefreshCatalogFence &&
		cut.Publication.ReplicaSetVersion > record.Cursor[4]
	if record.State == gateway.ReplicatedOperationRunning &&
		record.Cursor[3] == replicaMoveCursorExecuting && sameAction && !executingEvidenceOK && !refreshFenceAdvanced {
		return Action{}, fmt.Errorf("%w: executing action evidence regressed at cursor %v", ErrReplicatedMove, record.Cursor)
	}
	if record.State == gateway.ReplicatedOperationRunning &&
		record.Cursor[3] == replicaMoveCursorExecuting && action.Kind == ActionAwaitLeader && !executingEvidenceOK {
		return Action{}, fmt.Errorf("%w: cannot replace executing action while leader is unavailable", ErrReplicatedMove)
	}
	if executingEvidenceOK && action.Kind == ActionAwaitLeader && !sameAction {
		return action, nil
	}
	if (record.Cursor != currentCursor || record.Proof != currentProof) && !(executingEvidenceOK && sameAction) {
		if record.State == gateway.ReplicatedOperationRunning &&
			record.Cursor[3] == replicaMoveCursorApplied &&
			sameReplicaMoveAction(record.Cursor, wanted) {
			return action, nil
		}
		// Atomic set admission can itself advance the catalog RF3 log. A
		// still-planned action has never been dispatched, so its observation
		// may advance at the same placement cut. A passive wait can also be
		// satisfied while the controller is down (election/catchup/drain).
		// Reconcile has authenticated its successor above. Verify the old
		// witness before publishing the replacement; an executing action must
		// retain its original external idempotency tuple instead.
		// A partition can hide completion of AddLearner after its immutable
		// intent was admitted. Reconcile has authenticated the exact learner
		// roster; do not strand that already-applied transition at its old step.
		learnerObserved := ActionKind(record.Cursor[0]) == ActionAddLearner &&
			record.Cursor[1] == plan.TargetMember() && action.Kind == ActionCreateSnapshotBase &&
			cut.Publication.ReplicaSetVersion > record.Cursor[4]
		refreshPlanned := record.State == gateway.ReplicatedOperationPlanned &&
			record.Cursor[3] == replicaMoveCursorReady &&
			(sameReplicaMoveAction(record.Cursor, wanted) || passiveReplicaMoveAction(ActionKind(record.Cursor[0])) || learnerObserved) &&
			record.CatalogGeneration == cut.Catalog.Generation() &&
			(record.Cursor[4] == cut.Publication.ReplicaSetVersion || learnerObserved) &&
			record.Cursor[5] <= cut.Publication.Applied && record.Cursor[6] <= cut.LeaderStatus.Term &&
			record.Proof == replicaMoveActionProof(operation, record.IntentDigest, recordBaseDigest, record.Cursor)
		if record.State != gateway.ReplicatedOperationRunning && !refreshPlanned {
			return Action{}, fmt.Errorf("%w: cannot refresh planned action: state=%d cursor=%v wanted=%v catalog=%d/%d", ErrReplicatedMove, record.State, record.Cursor, wanted, record.CatalogGeneration, cut.Catalog.Generation())
		}
		next := record
		next.State = gateway.ReplicatedOperationPlanned
		next.Revision++
		next.CatalogGeneration = cut.Catalog.Generation()
		next.Cursor = wanted
		next.Proof = wantedProof
		if err = settleReplicaMovePublish(ctx, journal, record.Revision, next); err != nil {
			return Action{}, err
		}
		record = next
	}
	if action.Kind == ActionComplete {
		next := record
		next.State = gateway.ReplicatedOperationComplete
		next.Revision++
		next.Cursor, next.Proof = replicaMoveActionWitness(
			operation, next.IntentDigest, plan, cut, action, replicaMoveCursorApplied,
		)
		if err = settleReplicaMovePublish(ctx, journal, record.Revision, next); err != nil {
			return Action{}, err
		}
		if err = settleReplicaMoveDelete(ctx, journal, next); err != nil {
			return Action{}, err
		}
		return action, nil
	}
	if record.State == gateway.ReplicatedOperationRunning {
		phase := record.Cursor[3]
		switch phase {
		case replicaMoveCursorApplied:
			return action, nil
		case replicaMoveCursorExecuting:
		default:
			return Action{}, ErrReplicatedMove
		}
	} else if record.State == gateway.ReplicatedOperationPlanned {
		if record.Cursor[3] != replicaMoveCursorReady {
			return Action{}, ErrReplicatedMove
		}
		next := record
		next.State = gateway.ReplicatedOperationRunning
		next.Revision++
		next.Cursor, next.Proof = replicaMoveActionWitness(
			operation, next.IntentDigest, plan, cut, action, replicaMoveCursorExecuting,
		)
		if err = settleReplicaMovePublish(ctx, journal, record.Revision, next); err != nil {
			return Action{}, err
		}
		record = next
	} else {
		return Action{}, ErrReplicatedMove
	}
	var execution ReplicatedMoveExecution
	if record.State == gateway.ReplicatedOperationRunning &&
		record.Cursor[3] == replicaMoveCursorExecuting {
		var ok bool
		execution, ok = OpenReplicatedMoveExecution(record, plan, cut)
		if !ok || execution.Action != action {
			return Action{}, fmt.Errorf("%w: executing action witness does not match current action", ErrReplicatedMove)
		}
	} else {
		execution = replicaMoveExecution(operation, record.IntentDigest, plan, cut, action)
	}
	if err = executor.ExecuteReplicaMove(ctx, operation, plan, execution); err != nil {
		return action, err
	}
	next := record
	next.Revision++
	next.Cursor[3] = replicaMoveCursorApplied
	next.Proof = replicaMoveActionProof(
		operation, next.IntentDigest, execution.SnapshotBaseDigest, next.Cursor,
	)
	if err = settleReplicaMovePublish(ctx, journal, record.Revision, next); err != nil {
		return action, err
	}
	return action, nil
}

// PrepareReplicatedMoveRecord observes and freezes revision one without
// executing an external action. It is the admission half used by a move set;
// ordinary Resume performs a fresh observation after the atomic catalog batch.
func PrepareReplicatedMoveRecord(
	ctx context.Context, plan *Plan, observer ReplicatedMoveObserver,
) (gateway.ReplicatedOperationRecord, error) {
	if ctx == nil || plan == nil || observer == nil || plan.OperationID() == (OperationID{}) {
		return gateway.ReplicatedOperationRecord{}, ErrReplicatedMove
	}
	operation := plan.OperationID()
	cut, err := observer.ObserveReplicaMove(ctx, operation, gateway.ReplicatedOperationRecord{}, plan)
	if err != nil || cut.Catalog == nil {
		return gateway.ReplicatedOperationRecord{}, errors.Join(err, ErrReplicatedMove)
	}
	intent, err := AppendReplicaMoveIntent(nil, cut.Catalog, plan)
	if err != nil {
		return gateway.ReplicatedOperationRecord{}, errors.Join(err, ErrReplicatedMove)
	}
	action, err := Reconcile(plan, cut.Observation)
	if err != nil {
		return gateway.ReplicatedOperationRecord{}, err
	}
	return newReplicaMoveRecord(operation, cut.Catalog.Generation(), intent, plan, cut, action), nil
}

func newReplicaMoveRecord(
	operation OperationID,
	catalogGeneration uint64,
	intent []byte,
	plan *Plan,
	cut ReplicatedMoveCut,
	action Action,
) gateway.ReplicatedOperationRecord {
	digest := sha256.Sum256(intent)
	cursor, proof := replicaMoveActionWitness(
		operation, digest, plan, cut, action, replicaMoveCursorReady,
	)
	return gateway.ReplicatedOperationRecord{
		ID: [32]byte(operation), Kind: gateway.ReplicatedOperationMove,
		State: gateway.ReplicatedOperationPlanned, Revision: 1,
		CatalogGeneration: catalogGeneration, Cursor: cursor,
		Proof:        proof,
		IntentDigest: digest, Intent: intent,
	}
}

func validReplicaMoveRecord(record gateway.ReplicatedOperationRecord, operation OperationID) bool {
	if record.ID != [32]byte(operation) || record.Kind != gateway.ReplicatedOperationMove ||
		record.Revision == 0 || record.CatalogGeneration == 0 || len(record.Intent) == 0 ||
		sha256.Sum256(record.Intent) != record.IntentDigest ||
		record.State < gateway.ReplicatedOperationPlanned ||
		record.State > gateway.ReplicatedOperationComplete {
		return false
	}
	phase := record.Cursor[3]
	if phase < replicaMoveCursorReady || phase > replicaMoveCursorApplied ||
		record.Cursor[4] == 0 || record.Cursor[5] == 0 {
		return false
	}
	return record.Proof != ([32]byte{})
}

func replicaMoveRecordMatches(
	record gateway.ReplicatedOperationRecord,
	operation OperationID,
	plan *Plan,
	cut ReplicatedMoveCut,
	action Action,
	phase uint64,
) bool {
	cursor, proof := replicaMoveActionWitness(
		operation, record.IntentDigest, plan, cut, action, phase,
	)
	return record.Cursor == cursor && record.Proof == proof
}

func replicaMoveRecordBaseDigest(
	record gateway.ReplicatedOperationRecord, plan *Plan,
) ([32]byte, bool) {
	if plan == nil {
		return [32]byte{}, false
	}
	// Cursor[7] is only a compact hint. The proof authenticates the complete
	// digest, so use it to distinguish an historical unbound witness from a
	// currently bound digest whose low word happens to be zero.
	zero := [32]byte{}
	if plan.baseBound && record.Cursor[7] == binary.LittleEndian.Uint64(plan.baseDigest[:8]) &&
		record.Proof == replicaMoveActionProof(
			plan.OperationID(), record.IntentDigest, plan.baseDigest, record.Cursor,
		) {
		return plan.baseDigest, true
	}
	if record.Cursor[7] == 0 && record.Proof == replicaMoveActionProof(
		plan.OperationID(), record.IntentDigest, zero, record.Cursor,
	) {
		return zero, true
	}
	return zero, false
}

func replicaMovePlanBaseDigest(plan *Plan) [32]byte {
	if plan != nil && plan.baseBound {
		return plan.baseDigest
	}
	return [32]byte{}
}

func replicaMoveActionCursor(
	action Action, phase uint64, plan *Plan, cut ReplicatedMoveCut,
) [8]uint64 {
	baseTag := uint64(0)
	if plan != nil && plan.baseBound {
		baseTag = binary.LittleEndian.Uint64(plan.baseDigest[:8])
	}
	return [8]uint64{
		uint64(action.Kind), action.Member, action.CatalogGeneration, phase,
		cut.Publication.ReplicaSetVersion, cut.Publication.Applied,
		cut.LeaderStatus.Term, baseTag,
	}
}

func sameReplicaMoveAction(left, right [8]uint64) bool {
	return left[0] == right[0] && left[1] == right[1] && left[2] == right[2]
}

func passiveReplicaMoveAction(kind ActionKind) bool {
	switch kind {
	case ActionAwaitLeader, ActionAwaitSnapshotInstall, ActionAwaitCatchUp, ActionAwaitCatalogDrain:
		return true
	default:
		return false
	}
}

func replicaMoveActionProof(
	operation OperationID,
	intentDigest [32]byte,
	baseDigest [32]byte,
	cursor [8]uint64,
) [32]byte {
	var raw [32 + 32 + 32 + 8*8]byte
	copy(raw[:32], operation[:])
	copy(raw[32:64], intentDigest[:])
	copy(raw[64:96], baseDigest[:])
	for index := range cursor {
		binary.LittleEndian.PutUint64(raw[96+index*8:], cursor[index])
	}
	return sha256.Sum256(raw[:])
}

func replicaMoveActionWitness(
	operation OperationID,
	intentDigest [32]byte,
	plan *Plan,
	cut ReplicatedMoveCut,
	action Action,
	phase uint64,
) ([8]uint64, [32]byte) {
	cursor := replicaMoveActionCursor(action, phase, plan, cut)
	baseDigest := [32]byte{}
	if plan != nil && plan.baseBound {
		baseDigest = plan.baseDigest
	}
	return cursor, replicaMoveActionProof(operation, intentDigest, baseDigest, cursor)
}

func replicaMoveExecution(
	operation OperationID,
	intentDigest [32]byte,
	plan *Plan,
	cut ReplicatedMoveCut,
	action Action,
) ReplicatedMoveExecution {
	_, proof := replicaMoveActionWitness(
		operation, intentDigest, plan, cut, action, replicaMoveCursorExecuting,
	)
	execution := ReplicatedMoveExecution{
		Action: action, PublicationApplied: cut.Publication.Applied,
		PublicationReplicaSet: cut.Publication.ReplicaSetVersion,
		LeaderTerm:            cut.LeaderStatus.Term, Proof: proof,
	}
	if plan != nil && plan.baseBound {
		execution.SnapshotBaseDigest = plan.baseDigest
	}
	if cut.TransitionReceiptFound {
		if digest, err := cut.TransitionReceipt.ReceiptDigest(); err == nil {
			execution.TransitionReceiptDigest = digest
		}
	}
	return execution
}

func settleReplicaMoveSubmit(
	ctx context.Context,
	journal ReplicatedOperationJournal,
	record gateway.ReplicatedOperationRecord,
) error {
	err := journal.SubmitOperation(ctx, record)
	if !errors.Is(err, gateway.ErrReplicatedCatalogPending) {
		return err
	}
	if err = journal.RetryPending(ctx); err != nil {
		return err
	}
	settled, err := journal.ReadOperation(ctx, record.ID)
	if err != nil || !settled.Equal(record) {
		return errors.Join(err, ErrReplicatedMove)
	}
	return nil
}

func settleReplicaMovePublish(
	ctx context.Context,
	journal ReplicatedOperationJournal,
	expected uint64,
	record gateway.ReplicatedOperationRecord,
) error {
	err := journal.PublishOperation(ctx, expected, record)
	if !errors.Is(err, gateway.ErrReplicatedCatalogPending) {
		return err
	}
	if err = journal.RetryPending(ctx); err != nil {
		return err
	}
	settled, err := journal.ReadOperation(ctx, record.ID)
	if err != nil || !settled.Equal(record) {
		return errors.Join(err, ErrReplicatedMove)
	}
	return nil
}

func settleReplicaMoveDelete(
	ctx context.Context,
	journal ReplicatedOperationJournal,
	record gateway.ReplicatedOperationRecord,
) error {
	err := journal.DeleteOperation(ctx, record.ID, record.Revision)
	if errors.Is(err, gateway.ErrReplicatedCatalogPending) {
		if err = journal.RetryPending(ctx); err != nil {
			return err
		}
	}
	if err != nil {
		return err
	}
	_, err = journal.ReadOperation(ctx, record.ID)
	if !errors.Is(err, gateway.ErrReplicatedOperationMissing) {
		return errors.Join(err, ErrReplicatedMove)
	}
	return nil
}
