package rebalanceexec

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/rebalance"
)

var ErrControllerConfig = errors.New("rebalanceexec: invalid replica move controller configuration")

// MoveDirectory is the bounded replicated catalog work directory. It contains
// multiple operation kinds; Controller ignores records it does not own.
type MoveDirectory interface {
	ReadOperationIDs(context.Context) ([][32]byte, error)
	ReadOperation(context.Context, [32]byte) (gateway.ReplicatedOperationRecord, error)
}

// Controller composes durable move discovery with the exact one-step
// reconciler. It intentionally has no process-local queue or progress cursor:
// every restart rediscovers work and resumes from the replicated record.
type Controller struct {
	passMu            sync.Mutex
	directory         MoveDirectory
	journal           rebalance.ReplicatedOperationJournal
	observer          rebalance.ReplicatedMoveObserver
	executor          rebalance.ReplicatedMoveActionExecutor
	abandonment       *AbandonmentScheduler
	abandonmentCursor AbandonmentSchedulerCursor
	// Diagnostic only: replaced after each complete directory pass and never
	// consulted by the reconciler or any authorization/completion decision.
	failures    atomic.Pointer[map[[32]byte]string]
	lastPass    atomic.Pointer[MoveControllerPassDiagnostic]
	lastAttempt atomic.Pointer[MoveControllerAttemptDiagnostic]
}

func (controller *Controller) LastFailure(operation [32]byte) string {
	if controller == nil {
		return ""
	}
	if failures := controller.failures.Load(); failures != nil {
		return (*failures)[operation]
	}
	return ""
}

func (controller *Controller) InstallAbandonmentScheduler(scheduler *AbandonmentScheduler) bool {
	if controller == nil || scheduler == nil || controller.abandonment != nil {
		return false
	}
	controller.abandonment = scheduler
	return true
}

func NewController(
	directory MoveDirectory,
	journal rebalance.ReplicatedOperationJournal,
	observer rebalance.ReplicatedMoveObserver,
	executor rebalance.ReplicatedMoveActionExecutor,
) (*Controller, error) {
	if directory == nil || journal == nil || observer == nil || executor == nil {
		return nil, ErrControllerConfig
	}
	return &Controller{
		directory: directory, journal: journal, observer: observer, executor: executor,
	}, nil
}

// Submit executes the first journaled step for a newly planned move. The plan
// is used only if its operation record is absent; subsequent calls recover the
// immutable intent from the replicated journal.
func (controller *Controller) Submit(
	ctx context.Context, plan *rebalance.Plan,
) (rebalance.Action, error) {
	if controller == nil || ctx == nil || plan == nil ||
		plan.OperationID() == (rebalance.OperationID{}) {
		return rebalance.Action{}, ErrControllerConfig
	}
	return rebalance.ExecuteReplicatedMoveStep(
		ctx, plan.OperationID(), plan, controller.journal, controller.observer,
		controller.executor,
	)
}

// SubmitSet atomically admits every move record through the catalog RF3 group,
// then advances each group once. Snapshot/catch-up work may overlap across
// later passes, while each topology publication retains its exact generation
// CAS. A crash before admission exposes none of the set; after admission the
// ordinary directory scan resumes every child without process-local state.
func (controller *Controller) SubmitSet(
	ctx context.Context, plans []*rebalance.Plan,
) ([]rebalance.Action, error) {
	if controller == nil || ctx == nil || len(plans) == 0 {
		return nil, ErrControllerConfig
	}
	journal, ok := controller.journal.(rebalance.ReplicatedOperationSetJournal)
	if !ok {
		return nil, ErrControllerConfig
	}
	records, ids, err := controller.prepareSet(ctx, plans)
	if err != nil {
		return nil, err
	}
	if err := journal.SubmitOperationsIfDirectory(ctx, records, ids); err != nil {
		if !errors.Is(err, gateway.ErrReplicatedCatalogPending) {
			return nil, err
		}
		if err = controller.journal.RetryPending(ctx); err != nil {
			return nil, err
		}
		for _, record := range records {
			settled, readErr := controller.journal.ReadOperation(ctx, record.ID)
			if readErr != nil || !settled.Equal(record) {
				return nil, errors.Join(readErr, rebalance.ErrReplicatedMove)
			}
		}
	}
	actions := make([]rebalance.Action, len(plans))
	var failures error
	for index, plan := range plans {
		action, err := controller.Resume(ctx, plan.OperationID())
		actions[index] = action
		failures = errors.Join(failures, err)
	}
	return actions, failures
}

// Prepare observes and freezes an immutable move without publishing or
// executing it. The returned directory is the overlap fence for an atomic
// enrollment handoff; callers must admit both records before Resume.
func (controller *Controller) Prepare(ctx context.Context, plan *rebalance.Plan) (gateway.ReplicatedOperationRecord, [][32]byte, error) {
	if controller == nil || ctx == nil || plan == nil {
		return gateway.ReplicatedOperationRecord{}, nil, ErrControllerConfig
	}
	records, ids, err := controller.prepareSet(ctx, []*rebalance.Plan{plan})
	if err != nil {
		return gateway.ReplicatedOperationRecord{}, nil, err
	}
	return records[0], ids, nil
}

func (controller *Controller) prepareSet(ctx context.Context, plans []*rebalance.Plan) ([]gateway.ReplicatedOperationRecord, [][32]byte, error) {
	ids, err := controller.directory.ReadOperationIDs(ctx)
	if err != nil {
		return nil, nil, err
	}
	for index, plan := range plans {
		if plan == nil {
			return nil, nil, ErrControllerConfig
		}
		for _, prior := range plans[:index] {
			if prior.Group() == plan.Group() {
				return nil, nil, ErrControllerConfig
			}
		}
	}
	for _, id := range ids {
		record, readErr := controller.directory.ReadOperation(ctx, id)
		if readErr != nil {
			return nil, nil, readErr
		}
		if record.Kind != gateway.ReplicatedOperationMove || record.State == gateway.ReplicatedOperationCancelled {
			continue
		}
		identity, inspectErr := rebalance.InspectReplicaMoveIntent(record.Intent)
		if inspectErr != nil {
			return nil, nil, inspectErr
		}
		for _, plan := range plans {
			if identity.Request.Group == plan.Group() &&
				(record.State != gateway.ReplicatedOperationComplete || identity.SourceGeneration == plan.CatalogGeneration()) {
				// A newer failure certificate can assign a different operation ID
				// to the same physical replacement. RunPass must resume the original
				// immutable intent; never admit a competing saga for that group.
				return nil, nil, ErrAwaitMoveSet
			}
		}
	}
	records := make([]gateway.ReplicatedOperationRecord, len(plans))
	for index, plan := range plans {
		if plan == nil {
			return nil, nil, ErrControllerConfig
		}
		record, err := rebalance.PrepareReplicatedMoveRecord(ctx, plan, controller.observer)
		if err != nil {
			return nil, nil, err
		}
		records[index] = record
	}
	return records, ids, nil
}

// Resume executes one journaled step for an already submitted move.
func (controller *Controller) Resume(
	ctx context.Context, operation rebalance.OperationID,
) (rebalance.Action, error) {
	if controller == nil || ctx == nil || operation == (rebalance.OperationID{}) {
		return rebalance.Action{}, ErrControllerConfig
	}
	return rebalance.ExecuteReplicatedMoveStep(
		ctx, operation, nil, controller.journal, controller.observer,
		controller.executor,
	)
}

type ControllerPass struct {
	Discovered           uint32
	Moves                uint32
	Advanced             uint32
	Completed            uint32
	AbandonmentScanned   uint32
	AbandonmentWitnessed uint32
	AbandonmentDeleted   uint32
	AbandonmentBytes     uint64
}

// MoveControllerAttemptDiagnostic is the most recent durable move action
// attempted by RunPass. It is detached from controller state and exists only
// for on-demand diagnostics; callers must not use it as execution authority.
type MoveControllerAttemptDiagnostic struct {
	StartedAt   time.Time                        `json:"started_at"`
	FinishedAt  time.Time                        `json:"finished_at"`
	OperationID string                           `json:"operation_id"`
	Revision    uint64                           `json:"revision"`
	State       gateway.ReplicatedOperationState `json:"state"`
	Cursor      [8]uint64                        `json:"cursor"`
	Action      string                           `json:"action"`
	Error       string                           `json:"error,omitempty"`
}

// MoveControllerRecordDiagnostic is the highest-revision active move seen in
// the most recent directory pass, refreshed from the durable journal only
// when a diagnostic snapshot is requested.
type MoveControllerRecordDiagnostic struct {
	OperationID string                           `json:"operation_id"`
	GroupID     string                           `json:"group_id"`
	Revision    uint64                           `json:"revision"`
	State       gateway.ReplicatedOperationState `json:"state"`
	Cursor      [8]uint64                        `json:"cursor"`
	LastFailure string                           `json:"last_failure,omitempty"`
}

// MoveControllerPassDiagnostic records one bounded controller pass and the
// active record with the highest revision that the pass observed.
type MoveControllerPassDiagnostic struct {
	StartedAt       time.Time                      `json:"started_at"`
	FinishedAt      time.Time                      `json:"finished_at"`
	Pass            ControllerPass                 `json:"pass"`
	Error           string                         `json:"error,omitempty"`
	HighestRevision MoveControllerRecordDiagnostic `json:"highest_revision_move"`
	MoveRecordFound bool                           `json:"move_record_found"`
}

// MoveControllerDiagnosticSnapshot is collected on demand. The current
// durable record is reread only for the highest-revision move from the last
// pass, avoiding extra catalog reads on the controller or data paths.
type MoveControllerDiagnosticSnapshot struct {
	CapturedAt           time.Time                       `json:"captured_at"`
	LastPass             MoveControllerPassDiagnostic    `json:"last_pass"`
	LastPassAvailable    bool                            `json:"last_pass_available"`
	LastAttempt          MoveControllerAttemptDiagnostic `json:"last_attempt"`
	LastAttemptAvailable bool                            `json:"last_attempt_available"`
	CurrentMove          MoveControllerRecordDiagnostic  `json:"current_move"`
	CurrentMoveAvailable bool                            `json:"current_move_available"`
	JournalError         string                          `json:"journal_error,omitempty"`
}

// DiagnosticSnapshot returns a bounded detached view of the latest move
// controller pass. It is intended for explicit diagnostic signals only.
func (controller *Controller) DiagnosticSnapshot(ctx context.Context) MoveControllerDiagnosticSnapshot {
	snapshot := MoveControllerDiagnosticSnapshot{CapturedAt: time.Now().UTC()}
	if controller == nil || ctx == nil || controller.directory == nil {
		snapshot.JournalError = ErrControllerConfig.Error()
		return snapshot
	}
	if pass := controller.lastPass.Load(); pass != nil {
		snapshot.LastPass = *pass
		snapshot.LastPassAvailable = true
	}
	if attempt := controller.lastAttempt.Load(); attempt != nil {
		snapshot.LastAttempt = *attempt
		snapshot.LastAttemptAvailable = true
	}
	if !snapshot.LastPassAvailable || !snapshot.LastPass.MoveRecordFound {
		return snapshot
	}
	rawID, err := hex.DecodeString(snapshot.LastPass.HighestRevision.OperationID)
	if err != nil || len(rawID) != len([32]byte{}) {
		snapshot.JournalError = "invalid operation identity in move diagnostics"
		return snapshot
	}
	var operation [32]byte
	copy(operation[:], rawID)
	record, err := controller.directory.ReadOperation(ctx, operation)
	if err != nil {
		snapshot.JournalError = err.Error()
		return snapshot
	}
	if record.Kind != gateway.ReplicatedOperationMove {
		snapshot.JournalError = "highest-revision operation is no longer a move"
		return snapshot
	}
	detail := snapshot.LastPass.HighestRevision
	detail.Revision = record.Revision
	detail.State = record.State
	detail.Cursor = record.Cursor
	detail.LastFailure = controller.LastFailure(record.ID)
	snapshot.CurrentMove = detail
	snapshot.CurrentMoveAvailable = true
	return snapshot
}

// RunPass discovers the complete catalog-bounded work directory and advances
// every move by at most one durable step. A failed move does not starve an
// unrelated shard: errors are joined after all discoverable records have had
// their turn. Catalog CAS fences still serialize conflicting topology changes.
func (controller *Controller) RunPass(ctx context.Context) (result ControllerPass, resultErr error) {
	if controller == nil || ctx == nil {
		return ControllerPass{}, ErrControllerConfig
	}
	// Scaling and ordinary move scheduling share this controller. Serialize
	// scans so they cannot race the local abandonment cursor or dispatch the
	// same pending step concurrently from this process.
	controller.passMu.Lock()
	defer controller.passMu.Unlock()
	passStartedAt := time.Now().UTC()
	var highestRevision MoveControllerRecordDiagnostic
	var moveRecordFound bool
	defer func() {
		diagnostic := &MoveControllerPassDiagnostic{
			StartedAt: passStartedAt, FinishedAt: time.Now().UTC(), Pass: result,
			Error: errorString(resultErr), HighestRevision: highestRevision,
			MoveRecordFound: moveRecordFound,
		}
		controller.lastPass.Store(diagnostic)
	}()
	ids, err := controller.directory.ReadOperationIDs(ctx)
	if err != nil {
		return ControllerPass{}, err
	}
	pass := ControllerPass{Discovered: uint32(len(ids))}
	var reported map[[32]byte]string
	defer func() {
		if len(reported) == 0 {
			controller.failures.Store(nil)
		} else {
			controller.failures.Store(&reported)
		}
	}()
	var failures error
	if controller.abandonment != nil {
		abandoned, abandonErr := controller.abandonment.RunPass(ctx, controller.abandonmentCursor)
		controller.abandonmentCursor = abandoned.Cursor
		if abandoned.Done {
			controller.abandonmentCursor = AbandonmentSchedulerCursor{}
		}
		pass.AbandonmentScanned, pass.AbandonmentWitnessed, pass.AbandonmentDeleted =
			abandoned.Scanned, abandoned.Witnessed, abandoned.Deleted
		pass.AbandonmentBytes = abandoned.ScheduledBytes
		failures = errors.Join(failures, abandonErr)
	}
	for index := range ids {
		if err = ctx.Err(); err != nil {
			return pass, errors.Join(failures, err)
		}
		record, readErr := controller.directory.ReadOperation(ctx, ids[index])
		if errors.Is(readErr, gateway.ErrReplicatedOperationMissing) {
			continue
		}
		if readErr != nil {
			failures = errors.Join(failures, readErr)
			continue
		}
		if record.Kind != gateway.ReplicatedOperationMove {
			continue
		}
		if record.State == gateway.ReplicatedOperationCancelled {
			continue
		}
		pass.Moves++
		if record.State != gateway.ReplicatedOperationComplete &&
			(!moveRecordFound || record.Revision > highestRevision.Revision) {
			identity, inspectErr := rebalance.InspectReplicaMoveIntent(record.Intent)
			if inspectErr == nil {
				highestRevision = MoveControllerRecordDiagnostic{
					OperationID: fmt.Sprintf("%x", record.ID),
					GroupID:     fmt.Sprintf("%x", identity.Request.Group.GroupID),
					Revision:    record.Revision, State: record.State, Cursor: record.Cursor,
				}
				moveRecordFound = true
			}
		}
		attempt := &MoveControllerAttemptDiagnostic{
			StartedAt: time.Now().UTC(), OperationID: fmt.Sprintf("%x", record.ID),
			Revision: record.Revision, State: record.State, Cursor: record.Cursor,
		}
		action, stepErr := controller.Resume(ctx, rebalance.OperationID(record.ID))
		attempt.FinishedAt = time.Now().UTC()
		attempt.Action = action.Kind.String()
		if stepErr != nil {
			attempt.Error = stepErr.Error()
			failures = errors.Join(failures, stepErr)
			if reported == nil {
				reported = make(map[[32]byte]string)
			}
			detail := fmt.Sprintf("move operation=%x revision=%d state=%d cursor=%x failure_at=%s: %v",
				record.ID, record.Revision, record.State, record.Cursor, time.Now().UTC().Format(time.RFC3339Nano), stepErr)
			if len(detail) > 2048 {
				detail = detail[:2048]
			}
			reported[record.ID] = detail
		} else {
			pass.Advanced++
			if action.Kind == rebalance.ActionComplete {
				pass.Completed++
			}
		}
		controller.lastAttempt.Store(attempt)
	}
	return pass, failures
}

func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
