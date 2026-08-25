package replicatedstate

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/store/durable"
	pb "go.etcd.io/raft/v3/raftpb"
)

// LookupCompletion resolves data through the bounded session ring. A conflict
// returns the reconstructed original completion together with a typed
// RequestConflictError. A logically reclaimed retry returns ErrRetryRetired
// and is never re-executed.
func (m *Machine) LookupCompletion(data []byte) (CompletionLookup, error) {
	return m.lookupCompletion(data, nil)
}

// LookupCompletionInto is LookupCompletion with caller-owned result storage.
// dst is reused from length zero. It must have capacity for the largest
// metadata-only session completion so lookup never allocates result bytes.
func (m *Machine) LookupCompletionInto(
	data []byte,
	dst []byte,
) (CompletionLookup, error) {
	if cap(dst) < replication.MaxEmptyResultCompletionEnvelopeBytes {
		return CompletionLookup{}, ErrCompletionBufferSmall
	}
	result, err := m.lookupCompletion(
		data,
		dst[:0:replication.MaxEmptyResultCompletionEnvelopeBytes],
	)
	if len(result.Bytes) != 0 && &result.Bytes[0] != &dst[:cap(dst)][0] {
		return CompletionLookup{}, ErrCompletionCorrupt
	}
	return result, err
}

// CompletionLookupWorkspace owns one reusable, exact system-collection cut for
// a serialized batch of completion lookups. The zero value is ready for use.
// It is single-consumer, must not be copied, and retains only bounded durable
// snapshot scratch after EndCompletionLookupBatch.
type CompletionLookupWorkspace struct {
	cut         durable.DatabaseSnapshot
	catalog     [1]durable.NamedCollection
	snapshot    *durable.Snapshot
	owner       *Machine
	scratch     commandPlanScratch
	sessionRead [MaxSessionRecordBytes]byte
	slotRead    [MaxSessionSlotRecordBytes]byte
	// The compact opaque-value point decoder may first materialize another
	// row's front-compression restart value before shrinking to the requested
	// session or slot record. Every value the private system collection writes
	// is bounded by maxCompletionLookupDecodeBytes.
	decodeRead [maxCompletionLookupDecodeBytes]byte
}

const maxCompletionLookupDecodeBytes = max(
	MaxStateEnvelopeBytes, MaxSessionRecordBytes, MaxSessionSlotRecordBytes,
)

// BeginCompletionLookupBatch captures one exact system-collection generation
// and holds the Machine publication lock until EndCompletionLookupBatch. Every
// lookup in the batch therefore observes the same durable session-ring cut.
func (m *Machine) BeginCompletionLookupBatch(
	workspace *CompletionLookupWorkspace,
	expected raftmodel.Publication,
) error {
	return m.beginCompletionLookupBatch(workspace, expected, true)
}

func (m *Machine) beginCompletionLookupBatch(
	workspace *CompletionLookupWorkspace,
	expected raftmodel.Publication,
	validatePublication bool,
) error {
	if m == nil || workspace == nil {
		return ErrCompletionWorkspaceBusy
	}
	if workspace.owner != nil {
		return ErrCompletionWorkspaceBusy
	}
	m.mu.Lock()
	if err := m.checkUsable(); err != nil {
		m.mu.Unlock()
		return err
	}
	if !m.initialized {
		m.mu.Unlock()
		return ErrWrongBinding
	}
	if validatePublication && (m.state.Applied != expected.Applied ||
		m.state.DataChainDigest != expected.DataChainDigest ||
		m.state.ReplicaSetVersion != expected.ReplicaSetVersion ||
		!equalCompletionConfState(m.state.ConfState, expected.ConfState)) {
		m.mu.Unlock()
		return ErrCompletionPublication
	}
	workspace.catalog[0] = durable.NamedCollection{
		Name: systemCollectionName, Collection: m.system.Collection,
	}
	if err := durable.SnapshotCollectionsInto(&workspace.cut, workspace.catalog[:]); err != nil {
		clear(workspace.catalog[:])
		err = m.fail(err)
		m.mu.Unlock()
		return err
	}
	snapshot, ok := workspace.cut.Collection(systemCollectionName)
	if !ok || snapshot == nil {
		err := m.fail(errors.Join(ErrInconsistentSnapshot, workspace.cut.Close()))
		clear(workspace.catalog[:])
		m.mu.Unlock()
		return err
	}
	workspace.scratch.sessionRead = workspace.sessionRead[:0]
	workspace.scratch.slotRead = workspace.slotRead[:0]
	workspace.scratch.decodeRead = workspace.decodeRead[:0]
	workspace.snapshot = snapshot
	workspace.owner = m
	return nil
}

func equalCompletionConfState(left, right *pb.ConfState) bool {
	if left == nil || right == nil {
		return left == right
	}
	if (left.AutoLeave == nil) != (right.AutoLeave == nil) ||
		left.AutoLeave != nil && *left.AutoLeave != *right.AutoLeave {
		return false
	}
	return slices.Equal(left.Voters, right.Voters) &&
		slices.Equal(left.Learners, right.Learners) &&
		slices.Equal(left.VotersOutgoing, right.VotersOutgoing) &&
		slices.Equal(left.LearnersNext, right.LearnersNext) &&
		slices.Equal(left.ProtoReflect().GetUnknown(), right.ProtoReflect().GetUnknown())
}

// LookupCompletionIntoWorkspace resolves one command through the exact cut
// held by BeginCompletionLookupBatch. Result bytes are detached into dst.
func (m *Machine) LookupCompletionIntoWorkspace(
	workspace *CompletionLookupWorkspace,
	data []byte,
	dst []byte,
) (CompletionLookup, error) {
	if workspace == nil || workspace.owner != m || workspace.snapshot == nil {
		return CompletionLookup{}, ErrCompletionWorkspaceBusy
	}
	if cap(dst) < replication.MaxEmptyResultCompletionEnvelopeBytes {
		return CompletionLookup{}, ErrCompletionBufferSmall
	}
	command, err := replication.OpenCommand(data)
	if err != nil {
		return CompletionLookup{}, err
	}
	if !m.immutableBindingMatches(command) {
		return CompletionLookup{}, ErrWrongBinding
	}
	result, err := m.lookupCompletionAtSnapshot(
		command,
		dst[:0:replication.MaxEmptyResultCompletionEnvelopeBytes],
		workspace,
	)
	if len(result.Bytes) != 0 && &result.Bytes[0] != &dst[:cap(dst)][0] {
		return CompletionLookup{}, ErrCompletionCorrupt
	}
	return result, err
}

// EndCompletionLookupBatch releases the exact cut and Machine publication
// lock. The workspace remains warm for a later batch.
func (m *Machine) EndCompletionLookupBatch(
	workspace *CompletionLookupWorkspace,
) error {
	if workspace == nil || workspace.owner != m {
		return ErrCompletionWorkspaceBusy
	}
	workspace.owner = nil
	workspace.snapshot = nil
	clear(workspace.catalog[:])
	workspace.scratch.sessionRead = workspace.sessionRead[:0]
	workspace.scratch.slotRead = workspace.slotRead[:0]
	workspace.scratch.decodeRead = workspace.decodeRead[:0]
	err := workspace.cut.Close()
	if err != nil {
		err = m.fail(err)
	}
	m.mu.Unlock()
	return err
}

// Release drops every inactive reusable snapshot buffer retained by workspace.
func (workspace *CompletionLookupWorkspace) Release() error {
	if workspace == nil {
		return nil
	}
	if workspace.owner != nil {
		return ErrCompletionWorkspaceBusy
	}
	workspace.snapshot = nil
	clear(workspace.catalog[:])
	workspace.scratch = commandPlanScratch{}
	clear(workspace.sessionRead[:])
	clear(workspace.slotRead[:])
	clear(workspace.decodeRead[:])
	return workspace.cut.Release()
}

func (m *Machine) lookupCompletion(
	data []byte,
	completionScratch []byte,
) (CompletionLookup, error) {
	command, err := replication.OpenCommand(data)
	if err != nil {
		return CompletionLookup{}, err
	}
	var workspace CompletionLookupWorkspace
	if err := m.beginCompletionLookupBatch(
		&workspace, raftmodel.Publication{}, false,
	); err != nil {
		return CompletionLookup{}, err
	}
	if !m.immutableBindingMatches(command) {
		endErr := m.EndCompletionLookupBatch(&workspace)
		_ = workspace.Release()
		if endErr != nil {
			return CompletionLookup{}, endErr
		}
		return CompletionLookup{}, ErrWrongBinding
	}
	result, lookupErr := m.lookupCompletionAtSnapshot(
		command, completionScratch, &workspace,
	)
	endErr := m.EndCompletionLookupBatch(&workspace)
	_ = workspace.Release()
	if endErr != nil {
		return CompletionLookup{}, errors.Join(lookupErr, endErr)
	}
	return result, lookupErr
}

func (m *Machine) lookupCompletionAtSnapshot(
	command replication.CommandView,
	completionScratch []byte,
	workspace *CompletionLookupWorkspace,
) (CompletionLookup, error) {
	snapshot := workspace.snapshot
	scratch := &workspace.scratch
	digest := SessionKey(command.Tenant, command.ClientID)
	key := SessionStorageKey(digest)
	session, found, readErr := sessionAt(pointSnapshot{value: snapshot}, key, scratch)
	if readErr == nil && found &&
		(session.Digest != digest || !bytes.Equal(session.Tenant, command.Tenant) ||
			session.ClientID != command.ClientID) {
		readErr = fmt.Errorf("%w: session-key hash collision", ErrSessionCorrupt)
	}
	if readErr == nil && found && session.RetryWindow != m.options.RetryWindow {
		readErr = fmt.Errorf("%w: retry-window mismatch", ErrSessionCorrupt)
	}
	if readErr != nil {
		return CompletionLookup{}, m.fail(readErr)
	}
	if !found {
		orphanErr := ensureNoSessionSlots(
			pointSnapshot{value: snapshot}, digest, m.options.RetryWindow,
		)
		if orphanErr != nil {
			return CompletionLookup{}, m.fail(orphanErr)
		}
	}
	if command.Kind() == replication.CommandSessionOpen {
		if !found {
			return CompletionLookup{}, ErrCompletionNotFound
		}
		openKey, keyErr := SessionSlotStorageKey(digest, 0)
		if keyErr != nil {
			return CompletionLookup{}, m.fail(keyErr)
		}
		record, slotFound, slotErr := sessionSlotAt(
			pointSnapshot{value: snapshot}, openKey, scratch,
		)
		if slotErr == nil && (!slotFound || record.SessionDigest != digest ||
			record.Slot != 0 || record.ClientEpoch != session.ClientEpoch) {
			slotErr = fmt.Errorf("%w: opening slot mismatch", ErrSessionCorrupt)
		}
		if slotErr == nil {
			slotErr = validateStoredSessionSlot(m.state, record)
		}
		if slotErr == nil {
			slotErr = validateSessionSlotAgainstHeader(session, record)
		}
		currentOpen := slotErr == nil && record.ClientSequence == 1 &&
			record.ResultCode == ResultSessionOpened
		var completionBytes []byte
		if slotErr == nil && currentOpen {
			completionBytes, slotErr = m.appendSessionCompletion(
				completionScratch[:0], session, record,
			)
		}
		if slotErr != nil {
			return CompletionLookup{}, m.fail(slotErr)
		}
		if !currentOpen {
			// A canonical later result now occupies physical slot zero, so the
			// identity of sequence one's Open is no longer retained. Every Open
			// retry is terminally outside the bounded window.
			return CompletionLookup{}, ErrRetryRetired
		}
		result := CompletionLookup{
			Key: digest, Bytes: completionBytes,
			AppliedSequence: record.AppliedSequence,
		}
		matches := session.RetryHome == command.RetryHome &&
			record.Fingerprint == command.Fingerprint &&
			record.LogicalCommandDigest == LogicalCommandDigest(command)
		if !matches {
			if session.Status == SessionRetired {
				return CompletionLookup{}, ErrSessionRetired
			}
			return result, &RequestConflictError{Key: digest}
		}
		// Apply the retry floor only after proving this is the exact retained
		// Open. This preserves admission/lookup parity for a competing Open
		// while the original sequence-one slot is still identifiable.
		if sessionRetryFloor(session) >= 1 {
			return CompletionLookup{}, ErrRetryRetired
		}
		return result, nil
	}
	if command.Kind() == replication.CommandSessionRelease {
		if !found || command.ClientEpoch < session.ClientEpoch {
			if command.ClientEpoch <= m.state.SessionEpochHighWater {
				return CompletionLookup{}, ErrSessionReleased
			}
			return CompletionLookup{}, ErrCompletionNotFound
		}
		if command.ClientEpoch > session.ClientEpoch {
			return CompletionLookup{}, ErrCompletionNotFound
		}
		if session.Status != SessionRetired {
			return CompletionLookup{}, ErrSessionActive
		}
		if command.ClientSequence != session.HighSequence {
			return CompletionLookup{}, ErrSessionSequence
		}
		if command.AckThrough != session.HighSequence-1 {
			return CompletionLookup{}, ErrSessionAck
		}
		retirementSlot := uint16((session.HighSequence - 1) % uint64(session.RetryWindow))
		retirementKey, keyErr := SessionSlotStorageKey(digest, retirementSlot)
		if keyErr != nil {
			return CompletionLookup{}, m.fail(keyErr)
		}
		record, slotFound, slotErr := sessionSlotAt(
			pointSnapshot{value: snapshot}, retirementKey, scratch,
		)
		if slotErr == nil && (!slotFound || record.SessionDigest != digest ||
			record.Slot != retirementSlot || record.ClientEpoch != session.ClientEpoch ||
			record.ClientSequence != session.HighSequence ||
			!isSessionTerminalResult(record.ResultCode)) {
			slotErr = fmt.Errorf("%w: retirement slot mismatch", ErrSessionCorrupt)
		}
		if slotErr == nil {
			slotErr = validateStoredSessionSlot(m.state, record)
		}
		if slotErr == nil {
			slotErr = validateSessionSlotAgainstHeader(session, record)
		}
		if slotErr != nil {
			return CompletionLookup{}, m.fail(slotErr)
		}
		if session.RetryHome != command.RetryHome || record.Fingerprint != command.Fingerprint {
			return CompletionLookup{Key: digest}, &RequestConflictError{Key: digest}
		}
		return CompletionLookup{}, ErrCompletionNotFound
	}
	if !found {
		if command.ClientEpoch <= m.state.SessionEpochHighWater {
			return CompletionLookup{}, ErrRetryRetired
		}
		return CompletionLookup{}, ErrCompletionNotFound
	}
	if command.ClientEpoch < session.ClientEpoch ||
		command.ClientEpoch == session.ClientEpoch &&
			command.ClientSequence <= sessionRetryFloor(session) {
		return CompletionLookup{}, ErrRetryRetired
	}
	if command.ClientEpoch > session.ClientEpoch || command.ClientSequence > session.HighSequence {
		if command.ClientEpoch == session.ClientEpoch && session.Status == SessionRetired {
			return CompletionLookup{}, ErrSessionRetired
		}
		return CompletionLookup{}, ErrCompletionNotFound
	}
	slot := uint16((command.ClientSequence - 1) % uint64(session.RetryWindow))
	slotKey, keyErr := SessionSlotStorageKey(digest, slot)
	if keyErr != nil {
		return CompletionLookup{}, m.fail(keyErr)
	}
	record, slotFound, slotErr := sessionSlotAt(
		pointSnapshot{value: snapshot}, slotKey, scratch,
	)
	if slotErr == nil && (!slotFound || record.SessionDigest != digest || record.Slot != slot ||
		record.ClientEpoch != command.ClientEpoch || record.ClientSequence != command.ClientSequence) {
		slotErr = fmt.Errorf("%w: retained slot mismatch", ErrSessionCorrupt)
	}
	if slotErr == nil {
		slotErr = validateStoredSessionSlot(m.state, record)
	}
	if slotErr == nil {
		slotErr = validateSessionSlotAgainstHeader(session, record)
	}
	var completionBytes []byte
	if slotErr == nil {
		completionBytes, slotErr = m.appendSessionCompletion(
			completionScratch[:0], session, record,
		)
		if slotErr != nil {
			slotErr = fmt.Errorf("%w: reconstruct completion: %v", ErrSessionCorrupt, slotErr)
		}
	}
	if slotErr != nil {
		return CompletionLookup{}, m.fail(slotErr)
	}
	result := CompletionLookup{
		Key: digest, Bytes: completionBytes,
		AppliedSequence: record.AppliedSequence,
	}
	if session.RetryHome != command.RetryHome ||
		record.Fingerprint != command.Fingerprint ||
		record.LogicalCommandDigest != LogicalCommandDigest(command) {
		return result, &RequestConflictError{Key: digest}
	}
	return result, nil
}

// LookupSessionLease returns the exact retained lease and sequence fence for
// one issued client epoch. It performs only indexed point reads. The result is
// observational state for a future authenticated serving layer; it grants no
// authority to renew or revoke the session.
func (m *Machine) LookupSessionLease(
	tenant []byte,
	clientID replication.ID128,
	clientEpoch uint64,
) (SessionLeaseLookup, error) {
	if len(tenant) == 0 || len(tenant) > replication.MaxIdentityBytes ||
		clientID == (replication.ID128{}) || clientEpoch == 0 {
		return SessionLeaseLookup{}, ErrSessionEpoch
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkUsable(); err != nil {
		return SessionLeaseLookup{}, err
	}
	if !m.initialized {
		return SessionLeaseLookup{}, ErrWrongBinding
	}
	cut, err := durable.SnapshotCollections([]durable.NamedCollection{{
		Name: systemCollectionName, Collection: m.system.Collection,
	}})
	if err != nil {
		return SessionLeaseLookup{}, m.fail(err)
	}
	snapshot, ok := cut.Collection(systemCollectionName)
	if !ok || snapshot == nil {
		return SessionLeaseLookup{}, m.fail(errors.Join(ErrInconsistentSnapshot, cut.Close()))
	}
	digest := SessionKey(tenant, clientID)
	key := SessionStorageKey(digest)
	session, found, readErr := sessionAt(pointSnapshot{value: snapshot}, key, nil)
	if readErr == nil && found &&
		(session.Digest != digest || !bytes.Equal(session.Tenant, tenant) ||
			session.ClientID != clientID) {
		readErr = fmt.Errorf("%w: session-key hash collision", ErrSessionCorrupt)
	}
	if readErr == nil && found && session.RetryWindow != m.options.RetryWindow {
		readErr = fmt.Errorf("%w: retry-window mismatch", ErrSessionCorrupt)
	}
	if readErr != nil {
		return SessionLeaseLookup{}, m.fail(errors.Join(readErr, cut.Close()))
	}
	if !found {
		orphanErr := ensureNoSessionSlots(
			pointSnapshot{value: snapshot}, digest, m.options.RetryWindow,
		)
		closeErr := cut.Close()
		if orphanErr != nil || closeErr != nil {
			return SessionLeaseLookup{}, m.fail(errors.Join(orphanErr, closeErr))
		}
		if clientEpoch <= m.state.SessionEpochHighWater {
			return SessionLeaseLookup{}, ErrSessionReleased
		}
		return SessionLeaseLookup{}, ErrSessionEpoch
	}
	if clientEpoch != session.ClientEpoch {
		closeErr := cut.Close()
		if closeErr != nil {
			return SessionLeaseLookup{}, m.fail(closeErr)
		}
		if clientEpoch < session.ClientEpoch {
			return SessionLeaseLookup{}, ErrRetryRetired
		}
		return SessionLeaseLookup{}, ErrSessionEpoch
	}
	slot := uint16((session.HighSequence - 1) % uint64(session.RetryWindow))
	slotKey, keyErr := SessionSlotStorageKey(digest, slot)
	if keyErr != nil {
		_ = cut.Close()
		return SessionLeaseLookup{}, m.fail(keyErr)
	}
	record, slotFound, slotErr := sessionSlotAt(
		pointSnapshot{value: snapshot}, slotKey, nil,
	)
	if slotErr == nil && (!slotFound || record.SessionDigest != digest ||
		record.Slot != slot || record.ClientEpoch != clientEpoch ||
		record.ClientSequence != session.HighSequence) {
		slotErr = fmt.Errorf("%w: latest lease slot mismatch", ErrSessionCorrupt)
	}
	if slotErr == nil {
		slotErr = validateStoredSessionSlot(m.state, record)
	}
	if slotErr == nil {
		slotErr = validateSessionSlotAgainstHeader(session, record)
	}
	closeErr := cut.Close()
	if slotErr != nil || closeErr != nil {
		return SessionLeaseLookup{}, m.fail(errors.Join(slotErr, closeErr))
	}
	result := SessionLeaseLookup{
		ClientEpoch: session.ClientEpoch, HighSequence: session.HighSequence,
		AckThrough:            session.AckThrough,
		LeaseDeadlineUnixNano: session.LeaseDeadlineUnixNano,
		Status:                session.Status,
	}
	if session.Status == SessionRetired {
		result.TerminalResult = record.ResultCode
	}
	return result, nil
}

// appendSessionCompletion reconstructs the one canonical public completion
// from fixed-width retained metadata plus identities stored once in the
// machine binding and session header.
func (m *Machine) appendSessionCompletion(
	dst []byte,
	session SessionView,
	slot SessionSlotView,
) ([]byte, error) {
	if session.Digest != slot.SessionDigest || session.ClientEpoch != slot.ClientEpoch {
		return dst, ErrSessionCorrupt
	}
	resultDigest := replication.CompletionResultDigest(
		slot.ResultCode, ResultFormatMutation, nil,
	)
	return replication.AppendCompletionBytes(dst, replication.CompletionBytes{
		ClusterID:              m.binding.ClusterID,
		ClusterIncarnation:     m.binding.ClusterIncarnation,
		TopologyRecoveryEpoch:  m.binding.TopologyRecoveryEpoch,
		Distribution:           m.distribution,
		Shard:                  m.shard,
		AllocationGeneration:   m.binding.AllocationGeneration,
		ShardIncarnation:       m.binding.ShardIncarnation,
		GroupID:                m.binding.GroupID,
		ReplicaSetVersion:      slot.ReplicaSetVersion,
		ActivePolicyGeneration: slot.ActivePolicyGeneration,
		ProtectionEpoch:        slot.ProtectionEpoch,
		RoutingVersion:         slot.RoutingVersion,
		RouteGeneration:        slot.RouteGeneration,
		Tenant:                 session.Tenant,
		ClientID:               session.ClientID,
		ClientEpoch:            slot.ClientEpoch,
		ClientSequence:         slot.ClientSequence,
		Fingerprint:            slot.Fingerprint,
		RetryHome:              session.RetryHome,
		AppliedSequence:        slot.AppliedSequence,
		ResultCode:             slot.ResultCode,
		ResultFormat:           ResultFormatMutation,
		Storage:                replication.CompletionInline,
		ResultDigest:           resultDigest,
	})
}

// ReadSnapshot pins the sole user collection and its hidden state collection
// at one publication cut.
type ReadSnapshot struct {
	cut              durable.DatabaseSnapshot
	publication      raftmodel.Publication
	state            State
	manifestDigest   [32]byte
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
	Binding                Binding
	RelationManifestDigest [32]byte
	ReplicaSetVersion      uint64
	Applied                uint64
	LastTerm               uint64
	LastEntryDigest        [32]byte
	DataChainDigest        [32]byte
	SnapshotBaseDigest     [32]byte
}

// Fence returns the exact data and routing identity paired with this cut.
func (s *ReadSnapshot) Fence() SnapshotFence {
	if s == nil {
		return SnapshotFence{}
	}
	return SnapshotFence{
		Binding: s.state.Binding, RelationManifestDigest: s.manifestDigest,
		ReplicaSetVersion: s.publication.ReplicaSetVersion,
		Applied:           s.state.Applied, LastTerm: s.state.LastTerm,
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
	state, present, sessionCount, slotCount, err := scanSessionSystemSnapshot(
		systemSnapshot, m.options.MaxSessions, m.options.RetryWindow,
	)
	if err != nil || present != m.initialized {
		closeErr := cut.Close()
		if err != nil {
			return nil, m.fail(errors.Join(err, closeErr))
		}
		return nil, m.fail(errors.Join(ErrInconsistentSnapshot, closeErr))
	}
	if present {
		if sessionCount != state.SessionCount || slotCount != state.SessionSlotCount ||
			!equalState(state, m.state) ||
			!equalStatePublication(state, m.publication.Applied, m.publication.DataChainDigest,
				m.publication.ConfState, m.publication.ReplicaSetVersion) {
			return nil, m.fail(errors.Join(ErrInconsistentSnapshot, cut.Close()))
		}
	}
	return &ReadSnapshot{
		cut: cut, publication: clonePublication(m.publication), state: cloneState(m.state),
		manifestDigest: m.manifestDigest,
		userName:       m.userName, validation: m.user.Validation,
		validationDigest: m.user.ValidationDigest, validator: m.user.Validator,
	}, nil
}
