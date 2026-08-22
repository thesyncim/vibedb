package replicatedstate

import (
	"bytes"
	"errors"
	"fmt"
	"sync"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/store/durable"
)

// LookupCompletion resolves data's client tuple and then exact-compares every
// collision-sensitive request field. A conflict returns the original exact
// completion together with a typed RequestConflictError.
func (m *Machine) LookupCompletion(data []byte) (CompletionLookup, error) {
	command, err := replication.OpenCommand(data)
	if err != nil {
		return CompletionLookup{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkUsable(); err != nil {
		return CompletionLookup{}, err
	}
	if !m.initialized || !m.immutableBindingMatches(command) {
		return CompletionLookup{}, ErrWrongBinding
	}
	cut, err := durable.SnapshotCollections([]durable.NamedCollection{{
		Name: systemCollectionName, Collection: m.system.Collection,
	}})
	if err != nil {
		return CompletionLookup{}, m.fail(err)
	}
	snapshot, ok := cut.Collection(systemCollectionName)
	if !ok || snapshot == nil {
		return CompletionLookup{}, m.fail(errors.Join(ErrInconsistentSnapshot, cut.Close()))
	}
	digest := CompletionKey(command.Tenant, command.ClientID, command.ClientEpoch, command.ClientSequence)
	key := completionStorageKey(digest)
	record, found, readErr := completionAt(snapshot, key)
	err = errors.Join(readErr, cut.Close())
	if err != nil {
		return CompletionLookup{}, m.fail(err)
	}
	if !found {
		return CompletionLookup{}, ErrCompletionNotFound
	}
	completion, err := replication.OpenCompletion(record.Completion)
	if err != nil {
		return CompletionLookup{}, m.fail(fmt.Errorf("%w: %v", ErrCompletionCorrupt, err))
	}
	if err := m.validateCompletionResult(completion); err != nil {
		return CompletionLookup{}, m.fail(err)
	}
	result := CompletionLookup{
		Key: digest, Bytes: bytes.Clone(record.Completion),
		AppliedSequence: completion.AppliedSequence,
	}
	if !recordTupleMatchesCommand(record, command) {
		return CompletionLookup{}, m.fail(fmt.Errorf("%w: completion-key hash collision", ErrCompletionCorrupt))
	}
	if !recordMatchesCommand(record, command) {
		return result, &RequestConflictError{Key: digest}
	}
	return result, nil
}

// ReadSnapshot pins the sole user collection and its hidden state collection
// at one publication cut.
type ReadSnapshot struct {
	cut              durable.DatabaseSnapshot
	publication      raftmodel.Publication
	state            State
	userName         string
	validation       ValidationProfile
	validationDigest [32]byte
	validator        MutationValidator
	once             sync.Once
	closeErr         error
}

// SnapshotFence is the allocation-free, immutable publication identity paired
// with a ReadSnapshot. It excludes ConfState because consumers that only need
// to reject a changed data/ownership cut should not clone Raft membership.
type SnapshotFence struct {
	Binding            Binding
	Applied            uint64
	LastTerm           uint64
	LastEntryDigest    [32]byte
	DataChainDigest    [32]byte
	SnapshotBaseDigest [32]byte
}

// Fence returns the exact data and routing identity paired with this cut.
func (s *ReadSnapshot) Fence() SnapshotFence {
	if s == nil {
		return SnapshotFence{}
	}
	return SnapshotFence{
		Binding: s.state.Binding, Applied: s.state.Applied, LastTerm: s.state.LastTerm,
		LastEntryDigest: s.state.LastEntryDigest, DataChainDigest: s.state.DataChainDigest,
		SnapshotBaseDigest: s.state.SnapshotBaseDigest,
	}
}

// State returns the complete durable state record paired with this cut.
func (s *ReadSnapshot) State() State {
	if s == nil {
		return State{}
	}
	return cloneState(s.state)
}

// Publication returns the exact applied publication paired with this cut.
func (s *ReadSnapshot) Publication() raftmodel.Publication {
	if s == nil {
		return raftmodel.Publication{}
	}
	return clonePublication(s.publication)
}

// Collection returns the sole user collection snapshot. The hidden system
// collection is intentionally not exposed.
func (s *ReadSnapshot) Collection(name string) (*durable.Snapshot, bool) {
	if s == nil || name != s.userName {
		return nil, false
	}
	return s.cut.Collection(name)
}

// RangeSystem exports the bounded canonical system key/value image used by the
// portable snapshot artifact. Callback bytes are borrowed for the call.
func (s *ReadSnapshot) RangeSystem(fn func(key, value []byte) error) error {
	if s == nil || fn == nil {
		return ErrInconsistentSnapshot
	}
	snapshot, ok := s.cut.Collection(systemCollectionName)
	if !ok || snapshot == nil {
		return ErrInconsistentSnapshot
	}
	return snapshot.RangeRaw(fn)
}

// CanonicalImageDigest performs an explicit, complete user-image audit. It is
// intentionally O(rows) and belongs at certification, import, or offline
// verification boundaries—not on ordinary reads, admission, or apply.
func (s *ReadSnapshot) CanonicalImageDigest() ([32]byte, error) {
	if s == nil {
		return [32]byte{}, ErrInconsistentSnapshot
	}
	user, ok := s.cut.Collection(s.userName)
	if !ok || user == nil {
		return [32]byte{}, ErrInconsistentSnapshot
	}
	return canonicalImageDigest(
		s.userName, s.validation, s.validationDigest, s.validator, user, nil,
	)
}

// Close releases both generation leases. It is idempotent.
func (s *ReadSnapshot) Close() error {
	if s == nil {
		return nil
	}
	s.once.Do(func() { s.closeErr = s.cut.Close() })
	return s.closeErr
}

// Snapshot captures the sole user collection plus the hidden state row under
// the Machine publication lock. names may be empty or contain exactly the sole
// user name; the system collection is always included automatically.
func (m *Machine) Snapshot(names ...string) (*ReadSnapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkUsable(); err != nil {
		return nil, err
	}
	if len(names) > 1 || len(names) == 1 && names[0] != m.userName {
		return nil, fmt.Errorf("%w: snapshot names", ErrInvalidCollection)
	}
	cut, systemSnapshot, _, err := m.captureApplyCutLocked()
	if err != nil {
		return nil, m.fail(err)
	}
	state, present, completionCount, err := scanSystemSnapshot(systemSnapshot, m.options.MaxCompletions)
	if err != nil || present != m.initialized {
		closeErr := cut.Close()
		if err != nil {
			return nil, m.fail(errors.Join(err, closeErr))
		}
		return nil, m.fail(errors.Join(ErrInconsistentSnapshot, closeErr))
	}
	if present {
		if completionCount != state.CompletionCount || !equalState(state, m.state) ||
			!equalStatePublication(state, m.publication.Applied, m.publication.DataChainDigest,
				m.publication.ConfState, m.publication.ReplicaSetVersion) {
			return nil, m.fail(errors.Join(ErrInconsistentSnapshot, cut.Close()))
		}
		if err := m.validateRetainedCompletions(systemSnapshot, state); err != nil {
			return nil, m.fail(errors.Join(err, cut.Close()))
		}
	}
	return &ReadSnapshot{
		cut: cut, publication: clonePublication(m.publication), state: cloneState(m.state),
		userName: m.userName, validation: m.user.Validation,
		validationDigest: m.user.ValidationDigest, validator: m.user.Validator,
	}, nil
}
