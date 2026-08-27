package splitcontroller

import (
	"context"
	"crypto/sha256"
	"encoding/binary"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/gateway"
)

// CommittedPlanPreparer performs only the bounded preparation named by a
// validated catalog-Raft intent. It must settle exact idempotent receipts.
type CommittedPlanPreparer interface {
	PrepareCommittedPlan(context.Context, *Plan) error
}

const (
	preparationPending uint64 = 1
	preparationSettled uint64 = 2
)

// Action zero is reserved for preparation; all reconciled actions start at
// one. Both phases live in the existing operation record, never a local log.
func preparationCursor(phase uint64) [8]uint64 { return [8]uint64{0, phase} }

func preparationProof(id, intent [32]byte, phase uint64) [32]byte {
	h := sha256.New()
	_, _ = h.Write([]byte("vibedb/splitcontroller/committed-preparation\x00"))
	_, _ = h.Write(id[:])
	_, _ = h.Write(intent[:])
	var raw [8]byte
	binary.LittleEndian.PutUint64(raw[:], phase)
	_, _ = h.Write(raw[:])
	var digest [32]byte
	_ = h.Sum(digest[:0])
	return digest
}

func (service *ControllerService) prepareCommittedPlan(ctx context.Context, record gateway.ReplicatedOperationRecord, plan *Plan, catalog *gateway.Snapshot) error {
	if service.preparer == nil {
		return nil
	}
	if record.Kind != gateway.ReplicatedOperationSplit || record.ID != [32]byte(plan.OperationID()) ||
		record.Revision == 0 || record.Revision >= ^uint64(0)-1 ||
		record.State < gateway.ReplicatedOperationPlanned || record.State > gateway.ReplicatedOperationComplete {
		return ErrReplicatedExecution
	}
	if record.Cursor[0] != 0 {
		kind := ActionKind(record.Cursor[0])
		if record.Cursor[0] > uint64(ActionComplete) || kind < ActionAwaitSourceLeader || record.Cursor[1] >= autosplit.MaxSplitChildren ||
			record.Cursor[3] != 0 || record.Cursor[4] != 0 || record.Cursor[5] != 0 || record.Cursor[6] != 0 || record.Cursor[7] != 0 ||
			record.Proof != replicatedActionProof(record.ID, record.Cursor) {
			return ErrReplicatedExecution
		}
		return nil
	}
	if catalog == nil || catalog.Generation() != plan.current || record.CatalogGeneration != plan.current {
		return ErrReplicatedExecution
	}
	if record.Cursor == preparationCursor(preparationPending) && record.Proof == preparationProof(record.ID, record.IntentDigest, preparationPending) && record.State == gateway.ReplicatedOperationPlanned {
		next := record
		next.State, next.Revision = gateway.ReplicatedOperationRunning, record.Revision+1
		next.Cursor, next.Proof = preparationCursor(preparationPending), preparationProof(record.ID, record.IntentDigest, preparationPending)
		if err := settleReplicatedOperationPublish(ctx, service.catalog, record.Revision, next); err != nil {
			return err
		}
		record = next
	}
	if record.State != gateway.ReplicatedOperationRunning {
		return ErrReplicatedExecution
	}
	if record.Cursor == preparationCursor(preparationSettled) && record.Proof == preparationProof(record.ID, record.IntentDigest, preparationSettled) {
		return nil
	}
	if record.Cursor != preparationCursor(preparationPending) || record.Proof != preparationProof(record.ID, record.IntentDigest, preparationPending) {
		return ErrReplicatedExecution
	}
	if err := service.preparer.PrepareCommittedPlan(ctx, plan); err != nil {
		return err
	}
	next := record
	next.Revision++
	next.Cursor, next.Proof = preparationCursor(preparationSettled), preparationProof(record.ID, record.IntentDigest, preparationSettled)
	return settleReplicatedOperationPublish(ctx, service.catalog, record.Revision, next)
}

// ChildDescriptor returns only geometry already bound by the immutable plan.
// The bounded copy is used to replay preparation after a gateway restart.
func (p *Plan) ChildDescriptor(child uint8) (autosplit.SplitChild, error) {
	if p == nil || child >= p.childCount {
		return autosplit.SplitChild{}, ErrInvalidPlan
	}
	identity := p.children[child]
	ordinal, ok := p.targetManifest.ShardOrdinalForRange(identity.Range)
	if !ok {
		return autosplit.SplitChild{}, ErrInvalidPlan
	}
	shard, ok := p.targetManifest.ShardInfo(ordinal)
	if !ok {
		return autosplit.SplitChild{}, ErrInvalidPlan
	}
	return autosplit.SplitChild{Range: identity.Range, Shard: identity.Shard, AllocationGeneration: identity.AllocationGeneration,
		OwnershipEpoch: identity.OwnershipEpoch, Retained: identity.Retained, Leaders: shard.Leaders}, nil
}
