package replicatedstate

import (
	"errors"

	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/store/durable"
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

// PointReadInto captures every dense relation and the hidden state collection
// at one database-snapshot generation, then resolves one relation/key. The
// minimum is an applied-index contract, never a wall-clock staleness claim.
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
	// Admission precedes snapshot acquisition and payload materialization.
	if maxValueBytes < selected.target.Limits.MaxDocumentBytes {
		return PointReadResult{}, ErrReadBufferBound
	}
	if m.publication.Applied < minimumApplied {
		return PointReadResult{}, ErrReadBehind
	}
	if err := durable.SnapshotCollectionsInto(&m.applyCut, m.members); err != nil {
		return PointReadResult{}, m.fail(err)
	}
	snapshot, ok := m.applyCut.CollectionHandle(selected.target.Collection)
	if !ok || snapshot == nil {
		return PointReadResult{}, m.fail(errors.Join(ErrInconsistentSnapshot, m.applyCut.Close()))
	}
	value, found, err := snapshot.AppendRaw(dst[:0], key)
	if err != nil {
		return PointReadResult{}, m.fail(errors.Join(err, m.applyCut.Close()))
	}
	if len(value) > maxValueBytes {
		return PointReadResult{}, errors.Join(ErrReadBufferBound, m.applyCut.Close())
	}
	result := PointReadResult{
		Fence: SnapshotFence{
			Binding: m.state.Binding, RelationManifestDigest: m.manifestDigest,
			Applied: m.state.Applied, LastTerm: m.state.LastTerm,
			LastEntryDigest:    m.state.LastEntryDigest,
			DataChainDigest:    m.state.DataChainDigest,
			SnapshotBaseDigest: m.state.SnapshotBaseDigest,
		},
		Found: found, Value: value,
	}
	if err := m.applyCut.Close(); err != nil {
		return PointReadResult{}, m.fail(err)
	}
	return result, nil
}
