package splitcontroller

import (
	"bytes"
	"crypto/sha256"
	"errors"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/rangesplit"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
)

var pruneStepRecordMagic = [8]byte{'V', 'D', 'B', 'S', 'P', 'R', 'A', 1}

const pruneStepRecordHeader = 8 + sha256.Size

// The cursor and its input receipt share one atomic runtime-store replace.
// This closes the gap between a successful bounded advance and the separate
// remote action journal recording its reply. The receipt never certifies a
// delete: an awaiting-apply cursor still requires the replicated batch and
// capture verification before reconciliation can complete the operation.
func (s *DurableRuntimeStore) persistRetainedPruneStep(revision uint64, cursor []byte, receipt [32]byte) error {
	if receipt == ([32]byte{}) || len(cursor) > MaxPruneControlBytes-pruneStepRecordHeader {
		return ErrRuntimeStore
	}
	if _, err := rangesplit.OpenRetainedPruneCursor(cursor); err != nil {
		return errors.Join(ErrRuntimeStore, err)
	}
	raw := make([]byte, pruneStepRecordHeader+len(cursor))
	copy(raw, pruneStepRecordMagic[:])
	copy(raw[8:pruneStepRecordHeader], receipt[:])
	copy(raw[pruneStepRecordHeader:], cursor)
	return s.Persist(RuntimeStatePrune, 0, revision, raw)
}

// Legacy cursor-only records remain readable, but cannot claim that a stale
// action has already committed a step. The enclosing runtime checksum covers
// both the receipt and cursor in the new representation.
func openRetainedPruneRecord(raw []byte) ([]byte, [32]byte, error) {
	var receipt [32]byte
	if len(raw) >= len(pruneStepRecordMagic) && bytes.Equal(raw[:8], pruneStepRecordMagic[:]) {
		if len(raw) <= pruneStepRecordHeader || len(raw) > MaxPruneControlBytes {
			return nil, receipt, ErrRuntimeStore
		}
		copy(receipt[:], raw[8:pruneStepRecordHeader])
		if receipt == ([32]byte{}) {
			return nil, receipt, ErrRuntimeStore
		}
		raw = raw[pruneStepRecordHeader:]
	}
	if _, err := rangesplit.OpenRetainedPruneCursor(raw); err != nil {
		return nil, receipt, errors.Join(ErrRuntimeStore, err)
	}
	return raw, receipt, nil
}

func retainedPruneStepReceipt(plan *Plan, observed Observation) ([32]byte, error) {
	var zero [32]byte
	if plan == nil || observed.Catalog == nil || observed.Certificate == nil {
		return zero, ErrTopologyConflict
	}
	catalog, err := gateway.CatalogSnapshotDigest(observed.Catalog)
	if err != nil {
		return zero, err
	}
	state, err := replicatedstate.AppendState(nil, observed.SourceState)
	if err != nil {
		return zero, err
	}
	prune, err := appendOptionalPrune(observed.Prune)
	if err != nil {
		return zero, err
	}
	certificate := observed.Certificate.Digest()
	hash := sha256.New()
	_, _ = hash.Write([]byte("vibedb/retained-prune-step\x00"))
	_, _ = hash.Write(plan.operation[:])
	_, _ = hash.Write(catalog[:])
	_, _ = hash.Write(certificate[:])
	_, _ = hash.Write(state)
	_, _ = hash.Write(prune)
	hash.Sum(zero[:0])
	return zero, nil
}
