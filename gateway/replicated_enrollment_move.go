package gateway

import (
	"bytes"
	"context"
	"errors"
)

// AdmitEnrollmentMove publishes the first move record, the exact enrolled
// parent's Moving edge, its operation-directory entry and cumulative parent
// progress in one conditional catalog command. No executor can discover the
// move before its enrollment durably owns it.
func (authority *ReplicatedCatalogAuthority) AdmitEnrollmentMove(ctx context.Context,
	next GroupEnrollmentIntent, expectedRevision uint64, move ReplicatedOperationRecord, expectedOperations [][32]byte,
) error {
	if !next.Valid() || next.State != EnrollmentMoving || next.MoveOperationID != move.ID ||
		next.Revision != expectedRevision+1 || !move.Valid() || move.Kind != ReplicatedOperationMove ||
		move.State != ReplicatedOperationPlanned || move.Revision != 1 || next.Receipt == nil ||
		move.CatalogGeneration < next.Receipt.EnrolledCatalogGeneration {
		return ErrInvalidScalingMetadata
	}
	err := authority.putEnrollmentIntentWithMove(ctx, next, expectedRevision, false, &move, expectedOperations)
	if !errors.Is(err, ErrReplicatedCatalogPending) {
		return err
	}
	if err = authority.RetryPending(ctx); err != nil {
		return err
	}
	// Settling a shared session can complete a different caller's prior
	// command. Verify this exact parent/child pair before reporting success.
	return authority.putEnrollmentIntentWithMove(ctx, next, expectedRevision, false, &move, expectedOperations)
}

// A lost admission response remains retryable after the independent executor
// advances the move. Only its immutable intent is compared; a changed or
// cancelled move cannot be mistaken for the admitted child.
func (authority *ReplicatedCatalogAuthority) validateAdmittedEnrollmentMove(ctx context.Context, move ReplicatedOperationRecord) error {
	key := replicatedOperationKey(move.ID)
	result, err := authority.readRaw(ctx, key[:], MaxReplicatedOperationBytes)
	if err != nil {
		return err
	}
	if !result.Found {
		return ErrReplicatedCatalogConflict
	}
	current, err := openReplicatedOperation(result.Value)
	if err != nil || current.ID != move.ID || current.Kind != move.Kind ||
		current.State == ReplicatedOperationCancelled || current.IntentDigest != move.IntentDigest ||
		!bytes.Equal(current.Intent, move.Intent) {
		return ErrReplicatedCatalogConflict
	}
	return nil
}
