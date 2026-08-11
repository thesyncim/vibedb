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
	command, err := replication.OpenCommandV1(data)
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
	digest := CompletionKeyV1(command.Tenant, command.ClientID, command.ClientEpoch, command.ClientSequence)
	key := completionStorageKey(digest)
	record, found, readErr := completionAt(snapshot, key)
	err = errors.Join(readErr, cut.Close())
	if err != nil {
		return CompletionLookup{}, m.fail(err)
	}
	if !found {
		return CompletionLookup{}, ErrCompletionNotFound
	}
	completion, err := replication.OpenCompletionV1(record.Completion)
	if err != nil {
		return CompletionLookup{}, m.fail(fmt.Errorf("%w: %v", ErrCompletionCorrupt, err))
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
	cut         durable.DatabaseSnapshot
	publication raftmodel.Publication
	state       StateV1
	userName    string
	once        sync.Once
	closeErr    error
}

// State returns the complete durable state record paired with this cut.
func (s *ReadSnapshot) State() StateV1 {
	if s == nil {
		return StateV1{}
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

// RangeSystem exports the bounded canonical system key/value image needed by a
// future runtime snapshot manifest. Callback bytes are borrowed for the call.
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
		return nil, fmt.Errorf("%w: v1 snapshot names", ErrInvalidCollection)
	}
	cut, systemSnapshot, userSnapshot, err := m.captureApplyCutLocked()
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
			!equalStatePublication(state, m.publication.Applied, m.publication.LogicalDigest,
				m.publication.ConfState, m.publication.ReplicaSetVersion) {
			return nil, m.fail(errors.Join(ErrInconsistentSnapshot, cut.Close()))
		}
		if err := m.validateRetainedCompletions(systemSnapshot, state); err != nil {
			return nil, m.fail(errors.Join(err, cut.Close()))
		}
	}
	logical, err := logicalDigestV1(
		m.userName, m.user.Validation, m.user.ValidationDigest, userSnapshot, nil,
	)
	if err != nil {
		return nil, m.fail(errors.Join(err, cut.Close()))
	}
	if logical != m.state.LogicalDigest || logical != m.publication.LogicalDigest {
		return nil, m.fail(errors.Join(ErrInconsistentSnapshot, cut.Close()))
	}
	return &ReadSnapshot{
		cut: cut, publication: clonePublication(m.publication), state: cloneState(m.state),
		userName: m.userName,
	}, nil
}
