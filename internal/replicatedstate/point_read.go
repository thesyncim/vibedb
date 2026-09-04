package replicatedstate

import (
	"errors"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/replication"
)

var (
	// ErrReadBehind reports that the exact local publication has not reached
	// the caller's explicit applied-index floor.
	ErrReadBehind = errors.New("replicatedstate: publication is below read floor")
	// ErrReadBufferBound reports that the caller-provided response ceiling is
	// smaller than the frozen maximum value size of the selected relation.
	ErrReadBufferBound = errors.New("replicatedstate: point-read response bound is too small")
)

// PointReadResult is one detached byte-native lookup from a coherent bundle
// cut. Empty is meaningful when Found is true; missing is represented only by
// Found=false. Value is owned by the caller through dst.
type PointReadResult struct {
	Fence SnapshotFence
	Found bool
	Value []byte
}

// PointReadInto reads the hidden intent and selected row under the machine
// publication lock. Every collection is exclusively mutated by this machine,
// so both live reads belong to the same completed applied state. No detached
// snapshot escapes and no physical checkpoint is needed to materialize one.
// The minimum is an applied-index contract, never a wall-clock staleness claim.
func (m *Machine) PointReadInto(
	relation replication.RelationID,
	key []byte,
	minimumApplied uint64,
	maxValueBytes int,
	dst []byte,
) (PointReadResult, error) {
	if m == nil || relation == 0 || int(relation) > len(m.relations) ||
		len(key) == 0 || minimumApplied == 0 || maxValueBytes < 0 {
		return PointReadResult{}, ErrInvalidCollection
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkUsable(); err != nil {
		return PointReadResult{}, err
	}
	selected := m.relations[int(relation)-1]
	if selected.id != relation || len(key) > selected.target.Limits.MaxKeyBytes {
		return PointReadResult{}, ErrInvalidCollection
	}
	if !relationOwnsPoint(selected, key, m.state.Binding.OwnedRange) {
		return PointReadResult{}, ErrWrongBinding
	}
	// Admission precedes snapshot acquisition and payload materialization.
	if maxValueBytes < selected.target.Limits.MaxDocumentBytes {
		return PointReadResult{}, ErrReadBufferBound
	}
	if m.publication.Applied < minimumApplied {
		return PointReadResult{}, ErrReadBehind
	}
	// Apply and activation hold m.mu until the entire bundle is published. The
	// live primary router includes acknowledged overlay mutations even when
	// the rooted physical graph still belongs to an older checkpoint.
	_, blocked, intentErr := lookupTransactionIntentOwner(
		pointSnapshot{live: m.system.Collection}, relation, key,
	)
	if intentErr != nil {
		return PointReadResult{}, m.fail(intentErr)
	}
	if blocked {
		return PointReadResult{}, ErrTransactionIntentActive
	}
	value, found, err := selected.target.Collection.AppendRaw(dst[:0], key)
	if err != nil {
		return PointReadResult{}, m.fail(err)
	}
	if len(value) > maxValueBytes {
		return PointReadResult{}, ErrReadBufferBound
	}
	result := PointReadResult{
		Fence: SnapshotFence{
			Binding: m.state.Binding, RelationManifestDigest: m.manifestDigest,
			ReplicaSetVersion: m.publication.ReplicaSetVersion,
			Applied:           m.state.Applied, LastTerm: m.state.LastTerm,
			LastEntryDigest:    m.state.LastEntryDigest,
			DataChainDigest:    m.state.DataChainDigest,
			SnapshotBaseDigest: m.state.SnapshotBaseDigest,
		},
		Found: found, Value: value,
	}
	return result, nil
}

// relationOwnsPoint fails closed after a split whenever a relation's key
// grammar cannot prove the mapped placement point. Range-less validators are
// safe only while the group still owns the complete keyspace; retaining that
// compatibility keeps unsplit global-index images readable without allowing a
// narrowed source to serve keys that may belong to a child.
func relationOwnsPoint(
	relation relationCollection,
	key []byte,
	owned distribution.KeyRange,
) bool {
	validator, ok := relation.target.Validator.(OwnershipPointValidator)
	if ok {
		return validator.ValidatePointOwnership(key, owned) == MutationValidationAccept
	}
	return completeOwnershipRange(owned)
}

func completeOwnershipRange(owned distribution.KeyRange) bool {
	return owned.Start == (distribution.KeyspacePoint{}) && owned.End.Max &&
		owned.End.Point == (distribution.KeyspacePoint{})
}
