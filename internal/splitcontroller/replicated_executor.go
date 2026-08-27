package splitcontroller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"

	"github.com/thesyncim/vibedb/gateway"
)

var ErrReplicatedExecution = errors.New("splitcontroller: replicated operation record conflicts with plan")

// ReplicatedOperationJournal is the narrow catalog-Raft capability consumed by
// the split controller. The shipped implementation is
// gateway.ReplicatedCatalogAuthority; no controller-local journal or second
// consensus mechanism is permitted at this boundary.
type ReplicatedOperationJournal interface {
	ReadOperation(context.Context, [32]byte) (gateway.ReplicatedOperationRecord, error)
	SubmitOperation(context.Context, gateway.ReplicatedOperationRecord) error
	PublishOperation(context.Context, uint64, gateway.ReplicatedOperationRecord) error
	DeleteOperation(context.Context, [32]byte, uint64) error
	RetryPending(context.Context) error
}

// ReplicatedActionExecutor executes one idempotent reconciliation action. The
// stable split OperationID and fixed action tuple are its complete retry key.
type ReplicatedActionExecutor func(context.Context, OperationID, Action) error

// AdmitReplicatedPlan publishes the immutable split intent into catalog RF3
// without guessing its first data-plane action. The source that owns the
// range binds Cursor and Proof from one coherent observation before execution.
// Repeating admission for the same exact plan is idempotent; any conflicting
// record for the operation identity fails closed.
func AdmitReplicatedPlan(
	ctx context.Context,
	journal ReplicatedOperationJournal,
	catalog *gateway.Snapshot,
	plan *Plan,
) (gateway.ReplicatedOperationRecord, error) {
	if ctx == nil || journal == nil || catalog == nil || plan == nil ||
		catalog.Generation() != plan.current {
		return gateway.ReplicatedOperationRecord{}, ErrReplicatedExecution
	}
	intent, err := AppendPlanIntent(nil, catalog, plan)
	if err != nil {
		return gateway.ReplicatedOperationRecord{}, errors.Join(ErrReplicatedExecution, err)
	}
	record := gateway.ReplicatedOperationRecord{
		ID: [32]byte(plan.OperationID()), Kind: gateway.ReplicatedOperationSplit,
		State: gateway.ReplicatedOperationPlanned, Revision: 1,
		CatalogGeneration: catalog.Generation(), IntentDigest: sha256.Sum256(intent),
		Intent: intent,
	}
	record.Cursor = preparationCursor(preparationPending)
	record.Proof = preparationProof(record.ID, record.IntentDigest, preparationPending)
	if !record.Valid() {
		return gateway.ReplicatedOperationRecord{}, ErrReplicatedExecution
	}
	existing, readErr := journal.ReadOperation(ctx, record.ID)
	switch {
	case errors.Is(readErr, gateway.ErrReplicatedOperationMissing):
		if err := settleReplicatedOperationSubmit(ctx, journal, record); err != nil {
			return gateway.ReplicatedOperationRecord{}, err
		}
		return record, nil
	case readErr != nil:
		return gateway.ReplicatedOperationRecord{}, readErr
	case existing.ID == record.ID && existing.Kind == record.Kind &&
		existing.Revision >= record.Revision &&
		existing.CatalogGeneration >= record.CatalogGeneration &&
		existing.CatalogGeneration <= record.CatalogGeneration+1 &&
		existing.IntentDigest == record.IntentDigest && bytes.Equal(existing.Intent, record.Intent) &&
		existing.State >= gateway.ReplicatedOperationPlanned &&
		existing.State <= gateway.ReplicatedOperationComplete:
		return existing, nil
	default:
		return gateway.ReplicatedOperationRecord{}, ErrReplicatedExecution
	}
}

// ExecuteReplicatedStep records intent in the catalog RF3 group before invoking
// one reconciled action. On restart, an outcome-unknown remote wave settles
// before observation may advance the operation to the next planned action.
func ExecuteReplicatedStep(
	ctx context.Context,
	journal ReplicatedOperationJournal,
	plan *Plan,
	observed Observation,
	execute ReplicatedActionExecutor,
) (Action, error) {
	if ctx == nil || journal == nil || plan == nil || execute == nil || observed.Catalog == nil {
		return Action{}, ErrReplicatedExecution
	}
	action, err := Reconcile(plan, observed)
	if err != nil {
		return Action{}, err
	}
	id := [32]byte(plan.OperationID())
	intent, err := AppendPlanIntent(nil, observed.Catalog, plan)
	if err != nil {
		return Action{}, errors.Join(ErrReplicatedExecution, err)
	}
	intentDigest := sha256.Sum256(intent)
	wantCursor := replicatedActionCursor(action)
	wantProof := replicatedActionProof(id, wantCursor)
	record, readErr := journal.ReadOperation(ctx, id)
	switch {
	case errors.Is(readErr, gateway.ErrReplicatedOperationMissing):
		if action.Kind == ActionComplete {
			return action, nil
		}
		record = gateway.ReplicatedOperationRecord{
			ID: id, Kind: gateway.ReplicatedOperationSplit,
			State: gateway.ReplicatedOperationPlanned, Revision: 1,
			CatalogGeneration: observed.Catalog.Generation(), Cursor: wantCursor,
			Proof: wantProof, IntentDigest: intentDigest, Intent: intent,
		}
		if err := settleReplicatedOperationSubmit(ctx, journal, record); err != nil {
			return Action{}, err
		}
	case readErr != nil:
		return Action{}, readErr
	case record.ID != id || record.Kind != gateway.ReplicatedOperationSplit ||
		record.IntentDigest != intentDigest || !bytes.Equal(record.Intent, intent) ||
		record.CatalogGeneration > observed.Catalog.Generation() ||
		record.State < gateway.ReplicatedOperationPlanned ||
		record.State > gateway.ReplicatedOperationComplete:
		return Action{}, ErrReplicatedExecution
	case record.Cursor != wantCursor || record.Proof != wantProof:
		if len(record.Execution) != 0 && !record.ExecutionSettled {
			prior, err := pendingRemoteAction(record)
			if err != nil {
				return Action{}, err
			}
			return prior, execute(ctx, plan.OperationID(), prior)
		}
		unbound := record.Cursor == preparationCursor(preparationPending) && record.Proof == preparationProof(id, intentDigest, preparationPending)
		if record.State != gateway.ReplicatedOperationRunning &&
			!(record.State == gateway.ReplicatedOperationPlanned && unbound) {
			return Action{}, ErrReplicatedExecution
		}
		next := record
		next.State = gateway.ReplicatedOperationPlanned
		next.Revision++
		next.CatalogGeneration = observed.Catalog.Generation()
		next.Cursor, next.Proof = wantCursor, wantProof
		next.Execution, next.ExecutionRevision, next.ExecutionSettled = nil, 0, false
		if err := settleReplicatedOperationPublish(ctx, journal, record.Revision, next); err != nil {
			return Action{}, err
		}
		record = next
	}
	if action.Kind == ActionComplete {
		if record.State != gateway.ReplicatedOperationComplete {
			next := record
			next.State = gateway.ReplicatedOperationComplete
			next.Execution, next.ExecutionRevision, next.ExecutionSettled = nil, 0, false
			next.Revision++
			if err := settleReplicatedOperationPublish(ctx, journal, record.Revision, next); err != nil {
				return Action{}, err
			}
			record = next
		}
		if err := execute(ctx, plan.OperationID(), action); err != nil {
			return Action{}, err
		}
		if err := settleReplicatedOperationDelete(ctx, journal, record); err != nil {
			return Action{}, err
		}
		return action, nil
	}
	if record.State == gateway.ReplicatedOperationComplete ||
		record.State == gateway.ReplicatedOperationCancelled {
		return Action{}, ErrReplicatedExecution
	}
	if record.State == gateway.ReplicatedOperationPlanned {
		next := record
		next.State = gateway.ReplicatedOperationRunning
		next.Revision++
		if err := settleReplicatedOperationPublish(ctx, journal, record.Revision, next); err != nil {
			return Action{}, err
		}
	}
	return action, execute(ctx, plan.OperationID(), action)
}

func settleReplicatedOperationSubmit(
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
		return errors.Join(err, ErrReplicatedExecution)
	}
	return nil
}

func settleReplicatedOperationPublish(
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
		return errors.Join(err, ErrReplicatedExecution)
	}
	return nil
}

func settleReplicatedOperationDelete(
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
		return errors.Join(err, ErrReplicatedExecution)
	}
	return nil
}

func replicatedActionCursor(action Action) [8]uint64 {
	return [8]uint64{uint64(action.Kind), uint64(action.Child), action.CatalogGeneration}
}

func replicatedActionProof(operation [32]byte, cursor [8]uint64) [32]byte {
	var raw [32 + 8*8]byte
	copy(raw[:32], operation[:])
	for index := range cursor {
		binary.LittleEndian.PutUint64(raw[32+index*8:], cursor[index])
	}
	return sha256.Sum256(raw[:])
}
