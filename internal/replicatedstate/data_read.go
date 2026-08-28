package replicatedstate

import (
	"errors"

	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/store/durable"
)

// ErrDataReadOpen prevents replacing a cut while an executor still borrows it.
var ErrDataReadOpen = errors.New("replicatedstate: data read cut is still open")

// DataReadCut is caller-owned, reusable storage for an immutable data-only cut.
// It exposes neither the private system image nor backup/export operations.
// The caller must obtain minimumApplied from a quorum read barrier and verify
// Fence against the serving generation before executing. Independent cuts on
// different groups do not constitute a global transactional snapshot.
//
// A cut is single-consumer, must not be copied, and must outlive every borrowed
// relation and executor. Close releases leases but retains capture workspace.
type DataReadCut struct {
	owner      *Machine
	cut        durable.DatabaseSnapshot
	fence      SnapshotFence
	relations  [replication.MaxRelationsPerBundle]*durable.Snapshot
	validators [replication.MaxRelationsPerBundle]OwnershipPointValidator
	selected   uint64
	open       bool
}

// Relation borrows the physical snapshot of an admitted relation. A trusted
// execution adapter MUST apply OwnsKey to base rows before SQL evaluation when
// FullOwnership is false: split sources can retain physically unowned rows.
// Filtering after aggregation, LIMIT, or joins is too late.
func (cut *DataReadCut) Relation(id replication.RelationID) (*durable.Snapshot, bool) {
	if cut == nil || !cut.open || id == 0 || int(id) > len(cut.relations) ||
		cut.selected&(uint64(1)<<(id-1)) == 0 {
		return nil, false
	}
	return cut.relations[id-1], true
}

// FullOwnership permits the ordinary indexed executor without a range filter.
func (cut *DataReadCut) FullOwnership() bool {
	return cut != nil && cut.open && completeOwnershipRange(cut.fence.Binding.OwnedRange)
}

// OwnsKey uses the immutable placement validator and ownership range at this
// cut, not the machine's potentially newer range. Placement is not lexical key
// order; a SQL key-range predicate cannot substitute for this check.
func (cut *DataReadCut) OwnsKey(id replication.RelationID, key []byte) bool {
	if _, ok := cut.Relation(id); !ok || len(key) == 0 {
		return false
	}
	if validator := cut.validators[id-1]; validator != nil {
		return validator.ValidatePointOwnership(key, cut.fence.Binding.OwnedRange) == MutationValidationAccept
	}
	return cut.FullOwnership()
}

func (cut *DataReadCut) Fence() SnapshotFence {
	if cut == nil || !cut.open {
		return SnapshotFence{}
	}
	return cut.fence
}

func (cut *DataReadCut) Close() error {
	if cut == nil || !cut.open {
		return nil
	}
	cut.open = false
	cut.owner = nil
	cut.selected, cut.fence = 0, SnapshotFence{}
	clear(cut.relations[:])
	clear(cut.validators[:])
	return cut.cut.Close()
}

// DataReadCutInto pins all admitted relations at the same publication, after
// checking the applied floor and unresolved intents under the publication
// lock. Intent refusal is conservative at group granularity. The durable
// intent-row count is maintained on every apply; the reopen-only intent map
// is not a live read authority. Exact point reads retain their narrower check.
// Nil ids selects every data relation, never private system/capture state.
// Reusing a closed destination is allocation-free after capture warmup.
func (m *Machine) DataReadCutInto(
	ids []replication.RelationID, minimumApplied uint64, dst *DataReadCut,
) error {
	if m == nil || dst == nil || ids != nil && len(ids) == 0 || len(ids) > replication.MaxRelationsPerBundle {
		return ErrInvalidCollection
	}
	if dst.open {
		return ErrDataReadOpen
	}
	if minimumApplied == 0 {
		return ErrReadBehind
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkUsable(); err != nil {
		return err
	}
	if m.publication.Applied < minimumApplied {
		return ErrReadBehind
	}
	var all [replication.MaxRelationsPerBundle]replication.RelationID
	if ids == nil {
		for i := range m.relations {
			all[i] = m.relations[i].id
		}
		ids = all[:len(m.relations)]
	}
	var selected uint64
	full := completeOwnershipRange(m.state.Binding.OwnedRange)
	for _, id := range ids {
		if id == 0 || int(id) > len(m.relations) || m.relations[id-1].id != id {
			return ErrInvalidCollection
		}
		bit := uint64(1) << (id - 1)
		if selected&bit != 0 {
			return ErrInvalidCollection
		}
		if _, ok := m.relations[id-1].target.Validator.(OwnershipPointValidator); !ok && !full {
			return ErrWrongBinding
		}
		selected |= bit
	}
	if m.state.TransactionIntentRows != 0 {
		return ErrTransactionIntentActive
	}
	// SnapshotInto retains its workspace; no row copy, system scan, or backup
	// certificate is needed. Apply already maintains the publication invariant.
	if err := durable.SnapshotCollectionsInto(&dst.cut, m.members); err != nil {
		return m.fail(err)
	}
	for _, id := range ids {
		relation := &m.relations[id-1]
		snapshot, ok := dst.cut.CollectionHandle(relation.target.Collection)
		if !ok || snapshot == nil {
			clear(dst.relations[:])
			clear(dst.validators[:])
			return m.fail(errors.Join(ErrInconsistentSnapshot, dst.cut.Close()))
		}
		dst.relations[id-1] = snapshot
		dst.validators[id-1], _ = relation.target.Validator.(OwnershipPointValidator)
	}
	dst.fence = SnapshotFence{
		Binding: m.state.Binding, RelationManifestDigest: m.manifestDigest,
		ReplicaSetVersion: m.publication.ReplicaSetVersion,
		Applied:           m.state.Applied, LastTerm: m.state.LastTerm,
		LastEntryDigest: m.state.LastEntryDigest, DataChainDigest: m.state.DataChainDigest,
		SnapshotBaseDigest: m.state.SnapshotBaseDigest,
	}
	dst.selected, dst.open = selected, true
	dst.owner = m
	return nil
}

// OwnsDataReadCut prevents binding a physical cut to a different SQL owner.
// It checks provenance only; serving and catalog fences remain mandatory.
func (m *Machine) OwnsDataReadCut(cut *DataReadCut) bool {
	return m != nil && cut != nil && cut.open && cut.owner == m
}
