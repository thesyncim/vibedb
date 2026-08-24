package replicatedstate

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"slices"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/store/durable"
	"github.com/thesyncim/vibejson"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

var (
	normalEntryDigestDomain = []byte("vibedb/replicated-state/normal-entry\x00")
	configEntryDigestDomain = []byte("vibedb/replicated-state/config-entry\x00")
	errStopSessionPrefix    = errors.New("replicatedstate: stop session-prefix probe")
)

type commandPlan struct {
	command         replication.CommandView
	sessionDigest   [32]byte
	sessionKey      [33]byte
	slotKey         [35]byte
	sessionRecord   []byte
	slotRecord      []byte
	changes         []finalMutation
	dataChainDigest [32]byte
	resultCode      uint32
	refusal         error
	writeSession    bool
	writeSlot       bool
	newSession      bool
	newPhysicalSlot bool
	advanceEpoch    uint64
	deleteSession   bool
	deleteSlots     uint16
	release         bool
	exactDuplicate  bool
	conflict        bool
}

// commandPlanScratch is optional caller-owned storage for the bounded raw
// records a plan reads and emits. A batch plan is consumed into its logical
// overlays before the next call reuses these buffers. Singleton planning passes
// nil and retains its existing ownership behavior.
type commandPlanScratch struct {
	sessionRead   []byte
	slotRead      []byte
	decodeRead    []byte
	sessionRecord []byte
	slotRecord    []byte
	currentValue  []byte
	descriptors   []mutationValueDescriptor
	// logicalValueReads counts mutation before-value reads in the current
	// physical batch. It is retained only as bounded qualification telemetry.
	logicalValueReads uint32
	// physicalBaseValueReads is the subset served by the initial durable user
	// snapshot rather than the in-memory overlay. It proves planning does not
	// reread base values while still counting logical overlay dependencies.
	physicalBaseValueReads uint32
	// logicalValueHashes counts changed, found before-values reduced to fixed
	// descriptors. Equal puts and rejected/no-op mutations must leave it zero.
	logicalValueHashes uint32
	// logicalAfterValueHashes counts changed put values reduced to fixed
	// descriptors. The outer transition digest and overlay consume that one
	// descriptor without hashing command bytes again.
	logicalAfterValueHashes    uint32
	hybridClassificationPasses uint32
	hybridDescriptorRereads    uint32
}

func (s *commandPlanScratch) begin() {
	if s != nil {
		s.currentValue = s.currentValue[:0]
		clear(s.descriptors)
		s.descriptors = s.descriptors[:0]
	}
}

func (s *commandPlanScratch) release() {
	if s == nil {
		return
	}
	clear(s.sessionRead)
	clear(s.slotRead)
	clear(s.decodeRead)
	clear(s.sessionRecord)
	clear(s.slotRecord)
	clear(s.currentValue)
	clear(s.descriptors)
	s.sessionRead = s.sessionRead[:0]
	s.slotRead = s.slotRead[:0]
	s.decodeRead = s.decodeRead[:0]
	s.sessionRecord = s.sessionRecord[:0]
	s.slotRecord = s.slotRecord[:0]
	s.descriptors = s.descriptors[:0]
	if cap(s.sessionRead) > maxNormalBatchRetainedBufferBytes {
		s.sessionRead = nil
	}
	if cap(s.slotRead) > maxNormalBatchRetainedBufferBytes {
		s.slotRead = nil
	}
	if cap(s.decodeRead) > maxNormalBatchRetainedBufferBytes {
		s.decodeRead = nil
	}
	if cap(s.sessionRecord) > maxNormalBatchRetainedBufferBytes {
		s.sessionRecord = nil
	}
	if cap(s.slotRecord) > maxNormalBatchRetainedBufferBytes {
		s.slotRecord = nil
	}
	if cap(s.currentValue) > maxNormalBatchRetainedBufferBytes {
		s.currentValue = nil
	} else {
		s.currentValue = s.currentValue[:0]
	}
	if cap(s.descriptors) > maxNormalBatchRetainedOverlayEntries {
		s.descriptors = nil
	}
}

func (s *commandPlanScratch) appendSessionRecord(
	record SessionRecord,
) ([]byte, error) {
	if s == nil {
		return AppendSessionRecord(nil, record)
	}
	encoded, err := AppendSessionRecord(s.sessionRecord[:0], record)
	if err == nil {
		s.sessionRecord = encoded
	}
	return encoded, err
}

func (s *commandPlanScratch) appendSessionSlot(
	slot SessionSlot,
) ([]byte, error) {
	if s == nil {
		return AppendSessionSlot(nil, slot)
	}
	encoded, err := AppendSessionSlot(s.slotRecord[:0], slot)
	if err == nil {
		s.slotRecord = encoded
	}
	return encoded, err
}

// ApplyNormal implements raftmodel.StateMachine.
func (m *Machine) ApplyNormal(meta raftmodel.ApplyMeta, data []byte) (raftmodel.Publication, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	defer m.releaseMutationPlan()
	if err := m.checkUsable(); err != nil {
		return raftmodel.Publication{}, err
	}
	if meta.Type != pb.EntryNormal || meta.Term == 0 || meta.Term == math.MaxUint64 {
		return raftmodel.Publication{}, m.fail(fmt.Errorf("%w: invalid normal metadata", ErrApplySequence))
	}
	if len(data) > replication.MaxCommandBytes {
		return raftmodel.Publication{}, m.fail(ErrAdmissionBound)
	}
	kind := RecordNormal
	if IsOwnershipTransition(data) {
		kind = RecordOwnership
	}
	digest := normalEntryDigest(meta, data)
	replay, err := m.checkTransition(meta, kind, digest)
	if err != nil {
		return raftmodel.Publication{}, m.fail(err)
	}
	if replay {
		if kind == RecordOwnership {
			transition, openErr := OpenOwnershipTransition(data)
			if openErr != nil || m.state.Binding.OwnershipEpoch != transition.ToOwnershipEpoch ||
				m.state.Binding.RoutingVersion != transition.ToRoutingVersion ||
				m.state.Binding.RouteGeneration != transition.ToRouteGeneration {
				return raftmodel.Publication{}, m.fail(ErrOwnershipTransition)
			}
		}
		return clonePublication(m.publication), nil
	}
	if kind == RecordOwnership {
		transition, openErr := OpenOwnershipTransition(data)
		if openErr != nil {
			return raftmodel.Publication{}, m.fail(openErr)
		}
		binding, transitionErr := m.ownershipTransitionBinding(transition)
		if transitionErr != nil {
			return raftmodel.Publication{}, m.fail(transitionErr)
		}
		next := m.nextState(meta, RecordOwnership, digest)
		next.Binding = binding
		if err := m.persistTransition(next, nil, commandPlan{}); err != nil {
			return raftmodel.Publication{}, m.fail(err)
		}
		return clonePublication(m.publication), nil
	}
	if len(data) == 0 {
		next := m.nextState(meta, RecordNormal, digest)
		if err := m.persistTransition(next, nil, commandPlan{}); err != nil {
			return raftmodel.Publication{}, m.fail(err)
		}
		return clonePublication(m.publication), nil
	}
	command, err := replication.OpenCommand(data)
	if err != nil {
		return raftmodel.Publication{}, m.fail(err)
	}
	if !m.immutableBindingMatches(command) {
		return raftmodel.Publication{}, m.fail(ErrWrongBinding)
	}
	cut, systemSnapshot, userSnapshot, err := m.captureApplyCut()
	if err != nil {
		return raftmodel.Publication{}, m.fail(err)
	}
	plan, planErr := m.planCommand(
		command, meta.Index, m.state,
		pointSnapshot{value: systemSnapshot}, pointSnapshot{value: userSnapshot},
		nil,
	)
	err = errors.Join(planErr, cut.Close())
	if err != nil {
		return raftmodel.Publication{}, m.fail(err)
	}
	next := m.nextState(meta, RecordNormal, digest)
	if err := applyCommandPlanToState(&next, plan); err != nil {
		return raftmodel.Publication{}, m.fail(err)
	}
	if err := m.persistTransition(next, plan.changes, plan); err != nil {
		return raftmodel.Publication{}, m.fail(err)
	}
	return clonePublication(m.publication), nil
}

// ApplyConfiguration implements raftmodel.StateMachine.
func (m *Machine) ApplyConfiguration(meta raftmodel.ApplyMeta, conf *pb.ConfState) (raftmodel.Publication, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkUsable(); err != nil {
		return raftmodel.Publication{}, err
	}
	if (meta.Type != pb.EntryConfChange && meta.Type != pb.EntryConfChangeV2) ||
		meta.Term == 0 || meta.Term == math.MaxUint64 {
		return raftmodel.Publication{}, m.fail(fmt.Errorf("%w: invalid configuration metadata", ErrApplySequence))
	}
	if err := raftmodel.ValidateConfState(conf, meta.Index); err != nil {
		return raftmodel.Publication{}, m.fail(err)
	}
	digest, err := configurationEntryDigest(meta, conf)
	if err != nil {
		return raftmodel.Publication{}, m.fail(err)
	}
	replay, err := m.checkTransition(meta, RecordConfiguration, digest)
	if err != nil {
		return raftmodel.Publication{}, m.fail(err)
	}
	if replay {
		if !proto.Equal(m.state.ConfState, conf) {
			return raftmodel.Publication{}, m.fail(fmt.Errorf("%w: replay ConfState mismatch", ErrApplySequence))
		}
		return clonePublication(m.publication), nil
	}
	next := m.nextState(meta, RecordConfiguration, digest)
	next.ConfState = cloneConfState(conf)
	next.ReplicaSetVersion = meta.Index
	if err := m.persistTransition(next, nil, commandPlan{}); err != nil {
		return raftmodel.Publication{}, m.fail(err)
	}
	return clonePublication(m.publication), nil
}

// InstallSnapshot implements raftmodel.StateMachine. The index-one bootstrap
// initializes an empty store. A newer certificate may only bind an already
// staged, fully validated candidate at the exact same publication; collection
// rows are never carried or rewritten through Raft snapshot data.
func (m *Machine) InstallSnapshot(snapshot *pb.Snapshot) (raftmodel.Publication, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkUsable(); err != nil {
		return raftmodel.Publication{}, err
	}
	encoded, digest, bootstrapErr := validateBootstrap(snapshot)
	if bootstrapErr == nil && digest == m.bootstrapDigest && bytes.Equal(encoded, m.bootstrap) {
		if m.initialized {
			if m.state.Applied == 1 && m.state.LastKind == RecordStaticSnapshot &&
				m.state.LastTerm == 1 && m.state.LastEntryDigest == m.bootstrapDigest &&
				m.state.SnapshotBaseDigest == m.bootstrapDigest &&
				proto.Equal(m.state.ConfState, snapshot.GetMetadata().GetConfState()) {
				return clonePublication(m.publication), nil
			}
			return raftmodel.Publication{}, m.fail(ErrStaticSnapshotOnly)
		}
		next := State{
			Binding: m.binding, Applied: 1, LastTerm: 1,
			LastKind: RecordStaticSnapshot, LastEntryType: pb.EntryNormal,
			LastEntryDigest: m.bootstrapDigest, DataChainDigest: m.state.DataChainDigest,
			ApplyContractDigest: m.applyContract,
			ConfState:           cloneConfState(snapshot.GetMetadata().GetConfState()),
			ReplicaSetVersion:   1, BootstrapDigest: m.bootstrapDigest,
			SnapshotBaseDigest: m.bootstrapDigest,
		}
		if err := m.persistTransition(next, nil, commandPlan{}); err != nil {
			return raftmodel.Publication{}, m.fail(err)
		}
		return clonePublication(m.publication), nil
	}

	certificate, certificateErr := OpenSnapshotBase(snapshot)
	if certificateErr != nil {
		return raftmodel.Publication{}, m.fail(errors.Join(ErrStaticSnapshotOnly, certificateErr))
	}
	certBootstrap, certBootstrapDigest, err := validateBootstrap(certificate.StaticBootstrap)
	if err != nil || certBootstrapDigest != m.bootstrapDigest ||
		!bytes.Equal(certBootstrap, m.bootstrap) || !m.initialized ||
		certificate.Manifest.State.Binding != m.binding ||
		!bytes.Equal(certificate.Manifest.UserCollection, []byte(m.userName)) ||
		certificate.Manifest.UserRows != m.user.Collection.Len() ||
		certificate.Manifest.State.BootstrapDigest != m.bootstrapDigest ||
		!equalStateExceptSnapshotBaseDigest(certificate.Manifest.State, m.state) {
		return raftmodel.Publication{}, m.fail(ErrSnapshotBase)
	}
	imageDigest, imageErr := m.snapshotBaseImageDigest()
	if imageErr != nil || certificate.Manifest.ImageDigest != imageDigest {
		return raftmodel.Publication{}, m.fail(errors.Join(ErrSnapshotBase, imageErr))
	}
	switch m.state.SnapshotBaseDigest {
	case certificate.Digest:
		return clonePublication(m.publication), nil
	case certificate.Manifest.State.SnapshotBaseDigest:
		next := cloneState(m.state)
		next.SnapshotBaseDigest = certificate.Digest
		if err := m.persistTransition(next, nil, commandPlan{}); err != nil {
			return raftmodel.Publication{}, m.fail(err)
		}
		return clonePublication(m.publication), nil
	default:
		return raftmodel.Publication{}, m.fail(ErrSnapshotBase)
	}
}

func (m *Machine) snapshotBaseImageDigest() ([32]byte, error) {
	if m == nil || m.user.Collection == nil {
		return [32]byte{}, ErrSnapshotBase
	}
	currentGeneration := m.user.Collection.Generation()
	if currentGeneration != 0 && m.openedImageApplied == m.state.Applied &&
		m.openedImageGeneration == currentGeneration &&
		m.openedImageDigest != ([32]byte{}) {
		return m.openedImageDigest, nil
	}
	snapshot, err := m.user.Collection.Snapshot()
	if err != nil {
		return [32]byte{}, err
	}
	generation := snapshot.Generation()
	digest, scanErr := canonicalImageDigest(
		m.userName, m.user.Validation, m.user.ValidationDigest,
		m.user.Validator, snapshot, nil,
	)
	closeErr := snapshot.Close()
	if scanErr != nil || closeErr != nil || generation == 0 ||
		m.user.Collection.Generation() != generation {
		return [32]byte{}, errors.Join(ErrInconsistentSnapshot, scanErr, closeErr)
	}
	m.openedImageDigest = digest
	m.openedImageApplied = m.state.Applied
	m.openedImageGeneration = generation
	return digest, nil
}

// AdmitCommand performs the complete non-reserving pre-proposal check.
// Serving remains forbidden because a successful return does not reserve the
// proved storage for the future committed entry.
func (m *Machine) AdmitCommand(data []byte) error {
	if IsOwnershipTransition(data) {
		transition, err := OpenOwnershipTransition(data)
		if err != nil {
			return err
		}
		m.mu.Lock()
		defer m.mu.Unlock()
		defer m.releaseMutationPlan()
		if err := m.checkUsable(); err != nil {
			return err
		}
		if !m.initialized || m.state.Applied == math.MaxUint64-1 {
			return ErrAdmissionBound
		}
		binding, err := m.ownershipTransitionBinding(transition)
		if err != nil {
			return err
		}
		meta := raftmodel.ApplyMeta{Index: m.state.Applied + 1, Term: 1, Type: pb.EntryNormal}
		next := m.nextState(meta, RecordOwnership, normalEntryDigest(meta, transition.Bytes()))
		next.Binding = binding
		return m.checkTransitionCapacity(next, nil, commandPlan{})
	}
	command, err := replication.OpenCommand(data)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	defer m.releaseMutationPlan()
	if err := m.checkUsable(); err != nil {
		return err
	}
	if !m.initialized || m.state.Applied == math.MaxUint64-1 {
		return ErrAdmissionBound
	}
	if !m.immutableBindingMatches(command) {
		return ErrWrongBinding
	}
	cut, systemSnapshot, userSnapshot, err := m.captureApplyCutLocked()
	if err != nil {
		return m.fail(err)
	}
	plan, planErr := m.planCommand(
		command, m.state.Applied+1, m.state,
		pointSnapshot{value: systemSnapshot}, pointSnapshot{value: userSnapshot},
		nil,
	)
	closeErr := cut.Close()
	if planErr != nil || closeErr != nil {
		joined := errors.Join(planErr, closeErr)
		if closeErr == nil && errors.Is(planErr, ErrAdmissionBound) {
			return planErr
		}
		return m.fail(joined)
	}
	switch {
	case plan.conflict:
		return &RequestConflictError{Key: plan.sessionDigest}
	case plan.refusal != nil:
		return plan.refusal
	case plan.exactDuplicate:
		return m.checkTransitionCapacity(m.hypotheticalState(command, plan), nil, plan)
	case plan.release:
		return m.checkTransitionCapacity(m.hypotheticalState(command, plan), nil, plan)
	case plan.resultCode == ResultStaleFence:
		return ErrStaleCommand
	case plan.resultCode == ResultSessionRetired:
		return m.checkTransitionCapacity(m.hypotheticalState(command, plan), nil, plan)
	case plan.resultCode == ResultSessionOpened:
		return m.checkTransitionCapacity(m.hypotheticalState(command, plan), nil, plan)
	case plan.resultCode == ResultSessionRenewed:
		return m.checkTransitionCapacity(m.hypotheticalState(command, plan), nil, plan)
	case plan.resultCode == ResultSessionRevoked:
		return m.checkTransitionCapacity(m.hypotheticalState(command, plan), nil, plan)
	case plan.resultCode != ResultApplied:
		return ErrAdmissionBound
	default:
		return m.checkTransitionCapacity(m.hypotheticalState(command, plan), plan.changes, plan)
	}
}

func (m *Machine) hypotheticalState(command replication.CommandView, plan commandPlan) State {
	meta := raftmodel.ApplyMeta{Index: m.state.Applied + 1, Term: 1, Type: pb.EntryNormal}
	next := m.nextState(meta, RecordNormal, normalEntryDigest(meta, command.Bytes()))
	if err := applyCommandPlanToState(&next, plan); err != nil {
		return State{}
	}
	return next
}

func applyCommandPlanToState(next *State, plan commandPlan) error {
	if next == nil {
		return ErrStateCorrupt
	}
	next.DataChainDigest = plan.dataChainDigest
	if plan.newSession {
		next.SessionCount++
	}
	if plan.newPhysicalSlot {
		next.SessionSlotCount++
	}
	if plan.advanceEpoch != 0 {
		next.SessionEpochHighWater = plan.advanceEpoch
	}
	if plan.deleteSession {
		if next.SessionCount == 0 || next.SessionSlotCount < uint64(plan.deleteSlots) {
			return ErrSessionCorrupt
		}
		next.SessionCount--
		next.SessionSlotCount -= uint64(plan.deleteSlots)
	}
	return nil
}

func (m *Machine) checkTransition(meta raftmodel.ApplyMeta, kind RecordKind, digest [32]byte) (bool, error) {
	if !m.initialized {
		return false, fmt.Errorf("%w: static bootstrap is not installed", ErrApplySequence)
	}
	if meta.Index == m.state.Applied {
		if m.state.LastKind == kind && m.state.LastEntryType == meta.Type &&
			m.state.LastTerm == meta.Term && m.state.LastEntryDigest == digest {
			return true, nil
		}
		return false, fmt.Errorf("%w: different entry at published index", ErrApplySequence)
	}
	if m.state.Applied == math.MaxUint64-1 || meta.Index != m.state.Applied+1 ||
		meta.Index == math.MaxUint64 || meta.Term < m.state.LastTerm {
		return false, fmt.Errorf("%w: have %d, entry %d", ErrApplySequence, m.state.Applied, meta.Index)
	}
	return false, nil
}

func (m *Machine) nextState(meta raftmodel.ApplyMeta, kind RecordKind, digest [32]byte) State {
	return State{
		Binding: m.binding, Applied: meta.Index, LastTerm: meta.Term,
		LastKind: kind, LastEntryType: meta.Type, LastEntryDigest: digest,
		DataChainDigest: m.state.DataChainDigest, ConfState: cloneConfState(m.state.ConfState),
		ApplyContractDigest: m.applyContract,
		ReplicaSetVersion:   m.state.ReplicaSetVersion,
		BootstrapDigest:     m.bootstrapDigest, SnapshotBaseDigest: m.state.SnapshotBaseDigest,
		SessionCount: m.state.SessionCount, SessionSlotCount: m.state.SessionSlotCount,
		SessionEpochHighWater: m.state.SessionEpochHighWater,
	}
}

func normalEntryDigest(meta raftmodel.ApplyMeta, data []byte) [32]byte {
	h := sha256.New()
	_, _ = h.Write(normalEntryDigestDomain)
	var header [25]byte
	binary.LittleEndian.PutUint64(header[0:8], meta.Index)
	binary.LittleEndian.PutUint64(header[8:16], meta.Term)
	header[16] = byte(meta.Type)
	binary.LittleEndian.PutUint64(header[17:25], uint64(len(data)))
	_, _ = h.Write(header[:])
	_, _ = h.Write(data)
	var digest [32]byte
	_ = h.Sum(digest[:0])
	return digest
}

func configurationEntryDigest(meta raftmodel.ApplyMeta, conf *pb.ConfState) ([32]byte, error) {
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(conf)
	if err != nil {
		return [32]byte{}, err
	}
	h := sha256.New()
	_, _ = h.Write(configEntryDigestDomain)
	var header [17]byte
	binary.LittleEndian.PutUint64(header[0:8], meta.Index)
	binary.LittleEndian.PutUint64(header[8:16], meta.Term)
	header[16] = byte(meta.Type)
	_, _ = h.Write(header[:])
	writeHashFrame(h, encoded)
	var digest [32]byte
	_ = h.Sum(digest[:0])
	return digest, nil
}

func (m *Machine) captureApplyCut() (durable.DatabaseSnapshot, *durable.Snapshot, *durable.Snapshot, error) {
	return m.captureApplyCutLocked()
}

func (m *Machine) captureApplyCutLocked() (durable.DatabaseSnapshot, *durable.Snapshot, *durable.Snapshot, error) {
	cut, err := durable.SnapshotCollections([]durable.NamedCollection{
		{Name: systemCollectionName, Collection: m.system.Collection},
		{Name: m.userName, Collection: m.user.Collection},
	})
	if err != nil {
		return durable.DatabaseSnapshot{}, nil, nil, err
	}
	systemSnapshot, systemOK := cut.Collection(systemCollectionName)
	userSnapshot, userOK := cut.Collection(m.userName)
	if !systemOK || !userOK || systemSnapshot == nil || userSnapshot == nil {
		return durable.DatabaseSnapshot{}, nil, nil,
			errors.Join(ErrInconsistentSnapshot, cut.Close())
	}
	return cut, systemSnapshot, userSnapshot, nil
}

func (m *Machine) planCommand(
	command replication.CommandView,
	applied uint64,
	state State,
	systemSnapshot, userSnapshot pointSnapshot,
	scratch *commandPlanScratch,
) (commandPlan, error) {
	scratch.begin()
	plan := commandPlan{command: command, dataChainDigest: state.DataChainDigest}
	plan.sessionDigest = SessionKey(command.Tenant, command.ClientID)
	plan.sessionKey = SessionStorageKey(plan.sessionDigest)
	session, found, err := sessionAt(systemSnapshot, plan.sessionKey, scratch)
	if err != nil {
		return commandPlan{}, err
	}
	if command.Kind() == replication.CommandSessionOpen {
		return m.planSessionOpen(
			command, applied, state, systemSnapshot, plan, session, found, scratch,
		)
	}
	if command.Kind() == replication.CommandSessionRelease {
		return m.planSessionRelease(command, state, systemSnapshot, plan, session, found)
	}
	logicalDigest := LogicalCommandDigest(command)

	var next SessionRecord
	if !found {
		if err := ensureNoSessionSlots(
			systemSnapshot, plan.sessionDigest, m.options.RetryWindow,
		); err != nil {
			return commandPlan{}, err
		}
		if command.ClientEpoch <= state.SessionEpochHighWater {
			plan.refusal = ErrRetryRetired
		} else {
			plan.refusal = ErrSessionEpoch
		}
		return plan, nil
	} else {
		if session.Digest != plan.sessionDigest ||
			!bytes.Equal(session.Tenant, command.Tenant) || session.ClientID != command.ClientID {
			return commandPlan{}, fmt.Errorf("%w: session-key hash collision", ErrSessionCorrupt)
		}
		if session.RetryWindow != m.options.RetryWindow {
			return commandPlan{}, fmt.Errorf("%w: retry-window mismatch", ErrSessionCorrupt)
		}
		switch {
		case command.ClientEpoch < session.ClientEpoch:
			plan.refusal = ErrRetryRetired
			return plan, nil
		case command.ClientEpoch > session.ClientEpoch:
			plan.refusal = ErrSessionEpoch
			return plan, nil
		default:
			if command.ClientSequence <= session.HighSequence {
				if command.ClientSequence <= sessionRetryFloor(session) {
					plan.refusal = ErrRetryRetired
					return plan, nil
				}
				slot := uint16((command.ClientSequence - 1) % uint64(session.RetryWindow))
				plan.slotKey, err = SessionSlotStorageKey(plan.sessionDigest, slot)
				if err != nil {
					return commandPlan{}, err
				}
				existing, slotFound, slotErr := sessionSlotAt(
					systemSnapshot, plan.slotKey, scratch,
				)
				if slotErr != nil {
					return commandPlan{}, slotErr
				}
				if !slotFound || existing.SessionDigest != plan.sessionDigest ||
					existing.Slot != slot || existing.ClientEpoch != command.ClientEpoch ||
					existing.ClientSequence != command.ClientSequence {
					return commandPlan{}, fmt.Errorf("%w: retained slot mismatch", ErrSessionCorrupt)
				}
				if err := validateStoredSessionSlot(state, existing); err != nil {
					return commandPlan{}, err
				}
				if err := validateSessionSlotAgainstHeader(session, existing); err != nil {
					return commandPlan{}, err
				}
				if session.RetryHome != command.RetryHome ||
					existing.Fingerprint != command.Fingerprint ||
					existing.LogicalCommandDigest != logicalDigest {
					plan.conflict = true
					return plan, nil
				}
				plan.exactDuplicate = true
				if command.AckThrough > session.AckThrough {
					next = sessionRecord(session)
					next.AckThrough = command.AckThrough
					plan.sessionRecord, err = scratch.appendSessionRecord(next)
					if err != nil {
						return commandPlan{}, err
					}
					plan.writeSession = true
				}
				return plan, nil
			}
			if session.Status == SessionRetired {
				plan.refusal = ErrSessionRetired
				return plan, nil
			}
			if session.HighSequence == math.MaxUint64 ||
				command.ClientSequence != session.HighSequence+1 {
				plan.refusal = ErrSessionSequence
				return plan, nil
			}
			if command.AckThrough < session.AckThrough {
				plan.refusal = ErrSessionAck
				return plan, nil
			}
			if session.RetryHome != command.RetryHome {
				plan.conflict = true
				return plan, nil
			}
			next = sessionRecord(session)
		}
	}

	// Reserve the terminal uint64 value for retirement or revocation. An
	// ordinary command at this sequence could leave an active epoch with no
	// possible successor.
	if command.Kind() != replication.CommandSessionRetire &&
		command.Kind() != replication.CommandSessionRevoke &&
		command.ClientSequence == math.MaxUint64 {
		plan.refusal = ErrSessionSequence
		return plan, nil
	}
	switch command.Kind() {
	case replication.CommandSessionRetire:
		if command.AckThrough != next.HighSequence {
			plan.refusal = ErrSessionAck
			return plan, nil
		}
		if !m.mutableBindingMatchesState(command, state) {
			// The terminal sequence cannot retain a stale completion because that
			// would strand an active epoch at MaxUint64. Leave the session
			// unchanged so the same sequence can be proposed with refreshed
			// mutable fences. The Raft apply position still advances.
			if command.ClientSequence == math.MaxUint64 {
				plan.refusal = ErrStaleCommand
				return plan, nil
			}
			plan.resultCode = ResultStaleFence
		} else {
			plan.resultCode = ResultSessionRetired
		}
	case replication.CommandSessionRenew:
		if command.ExpectedDeadlineUnixNano != next.LeaseDeadlineUnixNano ||
			command.NextDeadlineUnixNano <= command.ExpectedDeadlineUnixNano {
			plan.refusal = ErrSessionLeaseDeadline
			return plan, nil
		}
		if !m.mutableBindingMatchesState(command, state) {
			plan.resultCode = ResultStaleFence
		} else {
			plan.resultCode = ResultSessionRenewed
			next.LeaseDeadlineUnixNano = command.NextDeadlineUnixNano
		}
	case replication.CommandSessionRevoke:
		if command.AckThrough != next.HighSequence {
			plan.refusal = ErrSessionAck
			return plan, nil
		}
		if command.ExpectedDeadlineUnixNano != next.LeaseDeadlineUnixNano {
			plan.refusal = ErrSessionLeaseDeadline
			return plan, nil
		}
		if !m.mutableBindingMatchesState(command, state) {
			if command.ClientSequence == math.MaxUint64 {
				plan.refusal = ErrStaleCommand
				return plan, nil
			}
			plan.resultCode = ResultStaleFence
		} else {
			plan.resultCode = ResultSessionRevoked
			next.LeaseDeadlineUnixNano = 0
		}
	default:
		relations := command.RelationBatches()
		hasRelation := relations.Next()
		relation := relations.Batch()
		switch {
		case !m.mutableBindingMatchesState(command, state):
			plan.resultCode = ResultStaleFence
		case command.RelationCount() != 1 || !hasRelation ||
			relation.Relation != replication.RelationID(1):
			plan.resultCode = ResultUnknownCollection
		default:
			plan.changes, plan.resultCode, err = m.planMutations(
				relation, userSnapshot, scratch,
			)
			if err != nil {
				return commandPlan{}, err
			}
			if plan.resultCode == ResultApplied && len(plan.changes) != 0 {
				var descriptors []mutationValueDescriptor
				if scratch != nil {
					descriptors = scratch.descriptors
				}
				plan.dataChainDigest, err = dataChainTransitionDigest(
					m.dataChainHash, state.DataChainDigest, m.applyContract,
					plan.changes, descriptors,
				)
				if err != nil {
					return commandPlan{}, err
				}
			}
		}
	}

	slot := uint16((command.ClientSequence - 1) % uint64(next.RetryWindow))
	plan.slotKey, err = SessionSlotStorageKey(plan.sessionDigest, slot)
	if err != nil {
		return commandPlan{}, err
	}
	if slot >= next.PhysicalSlotCount {
		if slot != next.PhysicalSlotCount {
			return commandPlan{}, fmt.Errorf("%w: noncontiguous physical slot", ErrSessionCorrupt)
		}
		next.PhysicalSlotCount++
		plan.newPhysicalSlot = true
	}
	next.HighSequence = command.ClientSequence
	next.AckThrough = command.AckThrough
	if isSessionTerminalResult(plan.resultCode) {
		next.Status = SessionRetired
	}
	plan.sessionRecord, err = scratch.appendSessionRecord(next)
	if err != nil {
		return commandPlan{}, err
	}
	plan.slotRecord, err = scratch.appendSessionSlot(SessionSlot{
		Slot:                   slot,
		SessionDigest:          plan.sessionDigest,
		ClientEpoch:            command.ClientEpoch,
		ClientSequence:         command.ClientSequence,
		AppliedSequence:        applied,
		Fingerprint:            command.Fingerprint,
		LogicalCommandDigest:   logicalDigest,
		ResultCode:             plan.resultCode,
		ReplicaSetVersion:      command.ReplicaSetVersion,
		ActivePolicyGeneration: command.ActivePolicyGeneration,
		ProtectionEpoch:        command.ProtectionEpoch,
		RoutingVersion:         command.RoutingVersion,
		RouteGeneration:        command.RouteGeneration,
	})
	if err != nil {
		return commandPlan{}, err
	}
	plan.writeSession, plan.writeSlot = true, true
	return plan, nil
}

// planSessionOpen is the only path that creates or advances a client session.
// The command deliberately carries epoch zero: ordered apply assigns the Raft
// index as the globally unique shard-local epoch, so concurrent opens never
// race on a caller-guessed H+1 token. Opening contains no user mutations; an
// old open retried after reclamation can therefore create only an empty session.
func (m *Machine) planSessionOpen(
	command replication.CommandView,
	applied uint64,
	state State,
	systemSnapshot pointSnapshot,
	plan commandPlan,
	session SessionView,
	found bool,
	scratch *commandPlanScratch,
) (commandPlan, error) {
	logicalDigest := LogicalCommandDigest(command)
	if found {
		if session.Digest != plan.sessionDigest ||
			!bytes.Equal(session.Tenant, command.Tenant) || session.ClientID != command.ClientID {
			return commandPlan{}, fmt.Errorf("%w: session-key hash collision", ErrSessionCorrupt)
		}
		if session.RetryWindow != m.options.RetryWindow || session.PhysicalSlotCount == 0 {
			return commandPlan{}, fmt.Errorf("%w: open session header", ErrSessionCorrupt)
		}
		openKey, err := SessionSlotStorageKey(plan.sessionDigest, 0)
		if err != nil {
			return commandPlan{}, err
		}
		record, slotFound, err := sessionSlotAt(systemSnapshot, openKey, scratch)
		if err != nil {
			return commandPlan{}, err
		}
		if !slotFound || record.SessionDigest != plan.sessionDigest || record.Slot != 0 ||
			record.ClientEpoch != session.ClientEpoch {
			return commandPlan{}, fmt.Errorf("%w: opening slot mismatch", ErrSessionCorrupt)
		}
		if err := validateStoredSessionSlot(state, record); err != nil {
			return commandPlan{}, err
		}
		if err := validateSessionSlotAgainstHeader(session, record); err != nil {
			return commandPlan{}, err
		}
		if record.ClientSequence == 1 {
			if record.ResultCode != ResultSessionOpened {
				return commandPlan{}, fmt.Errorf("%w: opening result", ErrSessionCorrupt)
			}
			if session.RetryHome == command.RetryHome &&
				record.Fingerprint == command.Fingerprint &&
				record.LogicalCommandDigest == logicalDigest {
				if sessionRetryFloor(session) >= 1 {
					plan.refusal = ErrRetryRetired
					return plan, nil
				}
				plan.exactDuplicate = true
				return plan, nil
			}
			if session.Status == SessionActive {
				plan.conflict = true
				return plan, nil
			}
		}
		// When the bounded ring has replaced sequence one's physical slot, the
		// Open identity is no longer retained and every possible retry is
		// terminally old. Do not reinterpret the canonical replacement as a
		// live competing Open.
		if record.ClientSequence != 1 && sessionRetryFloor(session) >= 1 {
			plan.refusal = ErrRetryRetired
		} else if session.Status == SessionActive {
			plan.refusal = ErrSessionActive
		} else {
			plan.refusal = ErrSessionRetired
		}
		return plan, nil
	} else {
		if err := ensureNoSessionSlots(
			systemSnapshot, plan.sessionDigest, m.options.RetryWindow,
		); err != nil {
			return commandPlan{}, err
		}
		if state.SessionCount >= m.options.MaxSessions {
			plan.refusal = ErrAdmissionBound
			return plan, nil
		}
	}
	if !m.mutableBindingMatchesState(command, state) {
		// An Open completion carries the mutable fences used to mint its token.
		// Never persist a token whose retained slot would fail the same binding
		// validation on lookup or reopen.
		plan.refusal = ErrStaleCommand
		return plan, nil
	}

	// Apply indices are strictly increasing and never MaxUint64. Using the index
	// as the epoch removes an allocator/reservation round trip while preserving a
	// durable anti-resurrection high-water after the session rows are released.
	if applied == 0 || applied == math.MaxUint64 || applied <= state.SessionEpochHighWater {
		return commandPlan{}, fmt.Errorf("%w: session epoch allocation", ErrApplySequence)
	}
	if found {
		// Every found-session branch above returns. Keep this fail-closed guard at
		// the allocation boundary so later lifecycle edits cannot accidentally
		// account an existing header as a newly created session.
		return commandPlan{}, fmt.Errorf("%w: existing session reached allocation", ErrSessionCorrupt)
	}
	next := SessionRecord{
		Tenant: command.Tenant, ClientID: command.ClientID,
		ClientEpoch: applied, RetryHome: command.RetryHome,
		AckThrough: 0, HighSequence: 1, Status: SessionActive,
		LeaseDeadlineUnixNano: command.NextDeadlineUnixNano,
		RetryWindow:           m.options.RetryWindow, PhysicalSlotCount: 1,
	}
	plan.newSession = true
	plan.slotKey, _ = SessionSlotStorageKey(plan.sessionDigest, 0)
	plan.newPhysicalSlot = true
	var err error
	plan.sessionRecord, err = scratch.appendSessionRecord(next)
	if err != nil {
		return commandPlan{}, err
	}
	plan.slotRecord, err = scratch.appendSessionSlot(SessionSlot{
		Slot:                   0,
		SessionDigest:          plan.sessionDigest,
		ClientEpoch:            applied,
		ClientSequence:         1,
		AppliedSequence:        applied,
		Fingerprint:            command.Fingerprint,
		LogicalCommandDigest:   logicalDigest,
		ResultCode:             ResultSessionOpened,
		ReplicaSetVersion:      command.ReplicaSetVersion,
		ActivePolicyGeneration: command.ActivePolicyGeneration,
		ProtectionEpoch:        command.ProtectionEpoch,
		RoutingVersion:         command.RoutingVersion,
		RouteGeneration:        command.RouteGeneration,
	})
	if err != nil {
		return commandPlan{}, err
	}
	plan.writeSession, plan.writeSlot = true, true
	plan.advanceEpoch = applied
	plan.resultCode = ResultSessionOpened
	return plan, nil
}

// planSessionRelease verifies the exact retired postcondition before removing
// its bounded session image. Release is deliberately not another sequenced
// request: it acknowledges the retained retirement tuple and is idempotent by
// postcondition after the header has disappeared.
func (m *Machine) planSessionRelease(
	command replication.CommandView,
	state State,
	systemSnapshot pointSnapshot,
	plan commandPlan,
	session SessionView,
	found bool,
) (commandPlan, error) {
	plan.release = true
	if !found {
		if err := ensureNoSessionSlots(
			systemSnapshot, plan.sessionDigest, m.options.RetryWindow,
		); err != nil {
			return commandPlan{}, err
		}
		if command.ClientEpoch <= state.SessionEpochHighWater {
			return plan, nil
		}
		plan.release = false
		plan.refusal = ErrSessionEpoch
		return plan, nil
	}
	if session.Digest != plan.sessionDigest ||
		!bytes.Equal(session.Tenant, command.Tenant) || session.ClientID != command.ClientID {
		return commandPlan{}, fmt.Errorf("%w: session-key hash collision", ErrSessionCorrupt)
	}
	if session.RetryWindow != m.options.RetryWindow {
		return commandPlan{}, fmt.Errorf("%w: retry-window mismatch", ErrSessionCorrupt)
	}
	switch {
	case command.ClientEpoch < session.ClientEpoch:
		// A newer epoch at the same stable identity proves the named older epoch
		// was already retired. Never let an old release delete the newer image.
		return plan, nil
	case command.ClientEpoch > session.ClientEpoch:
		plan.release = false
		plan.refusal = ErrSessionEpoch
		return plan, nil
	}
	if session.Status != SessionRetired {
		plan.release = false
		plan.refusal = ErrSessionActive
		return plan, nil
	}
	if command.ClientSequence != session.HighSequence {
		plan.release = false
		plan.refusal = ErrSessionSequence
		return plan, nil
	}
	if command.AckThrough != session.HighSequence-1 {
		plan.release = false
		plan.refusal = ErrSessionAck
		return plan, nil
	}
	retirementSlot := uint16((session.HighSequence - 1) % uint64(session.RetryWindow))
	retirementSeen := false
	retirementMatches := false
	var appliedOrder sessionAppliedOrder
	var seen uint16
	err := systemSnapshot.rangeSessionSlots(plan.sessionDigest, func(key, value []byte) error {
		// The prefix scan is both the validation pass and the extra-slot check:
		// an exact release must account for every physical row before deleting
		// any of them in the later atomic transaction.
		if seen >= session.PhysicalSlotCount {
			return fmt.Errorf("%w: extra release slot", ErrSessionCorrupt)
		}
		wantKey, keyErr := SessionSlotStorageKey(plan.sessionDigest, seen)
		if keyErr != nil {
			return keyErr
		}
		if !bytes.Equal(key, wantKey[:]) {
			return fmt.Errorf("%w: noncontiguous release slot", ErrSessionCorrupt)
		}
		record, openErr := OpenSessionSlot(value)
		if openErr != nil {
			return openErr
		}
		if record.SessionDigest != plan.sessionDigest || record.Slot != seen ||
			record.ClientEpoch != session.ClientEpoch {
			return fmt.Errorf("%w: release slot mismatch", ErrSessionCorrupt)
		}
		if err := validateStoredSessionSlot(state, record); err != nil {
			return err
		}
		if err := validateSessionSlotAgainstHeader(session, record); err != nil {
			return err
		}
		if err := appliedOrder.observe(
			session.HighSequence, session.RetryWindow, record,
		); err != nil {
			return err
		}
		if seen == retirementSlot {
			if record.ClientSequence != session.HighSequence ||
				!isSessionTerminalResult(record.ResultCode) {
				return fmt.Errorf("%w: missing retirement slot", ErrSessionCorrupt)
			}
			retirementMatches = session.RetryHome == command.RetryHome &&
				record.Fingerprint == command.Fingerprint
			retirementSeen = true
		}
		seen++
		return nil
	})
	if err != nil {
		return commandPlan{}, err
	}
	if seen != session.PhysicalSlotCount {
		return commandPlan{}, fmt.Errorf("%w: incomplete release ring", ErrSessionCorrupt)
	}
	if !retirementSeen {
		return commandPlan{}, fmt.Errorf("%w: missing retirement slot", ErrSessionCorrupt)
	}
	if err := appliedOrder.finish(session.HighSequence, session.RetryWindow); err != nil {
		return commandPlan{}, err
	}
	if !retirementMatches {
		plan.release = false
		plan.conflict = true
		return plan, nil
	}
	plan.deleteSession = true
	plan.deleteSlots = session.PhysicalSlotCount
	return plan, nil
}

func sessionRecord(view SessionView) SessionRecord {
	return SessionRecord{
		Tenant: view.Tenant, ClientID: view.ClientID, ClientEpoch: view.ClientEpoch,
		RetryHome: view.RetryHome, AckThrough: view.AckThrough,
		HighSequence: view.HighSequence, LeaseDeadlineUnixNano: view.LeaseDeadlineUnixNano,
		Status:      view.Status,
		RetryWindow: view.RetryWindow, PhysicalSlotCount: view.PhysicalSlotCount,
	}
}

func sessionRetryFloor(session SessionView) uint64 {
	floor := session.AckThrough
	window := uint64(session.RetryWindow)
	if session.HighSequence > window && session.HighSequence-window > floor {
		floor = session.HighSequence - window
	}
	return floor
}

func (m *Machine) planMutations(
	batch replication.RelationBatchView,
	snapshot pointSnapshot,
	scratch *commandPlanScratch,
) ([]finalMutation, uint32, error) {
	if cap(m.mutationPlan) == 0 {
		m.mutationPlan = m.mutationInline[:0]
	}
	clear(m.mutationPlan)
	ordered := m.mutationPlan[:0]
	defer func() { m.mutationPlan = ordered }()
	iterator := batch.Mutations()
	for iterator.Next() {
		mutation := iterator.Mutation()
		at := -1
		for index := range ordered {
			if bytes.Equal(ordered[index].key, mutation.Key) {
				at = index
				break
			}
		}
		if at >= 0 {
			ordered[at].delete = mutation.Kind == replication.MutationDelete
			ordered[at].value = mutation.Value
			continue
		}
		ordered = append(ordered, finalMutation{
			key: mutation.Key, value: mutation.Value,
			delete: mutation.Kind == replication.MutationDelete,
		})
		if len(ordered) > m.user.Limits.MaxDistinctMutations {
			return nil, ResultTargetBound, nil
		}
	}
	rawUpperBytes := 0
	rawUpperExceeds := false
	for _, mutation := range ordered {
		if len(mutation.key) > m.user.Limits.MaxBatchBytes-rawUpperBytes {
			rawUpperExceeds = true
			break
		}
		rawUpperBytes += len(mutation.key)
		if len(mutation.value) > m.user.Limits.MaxBatchBytes-rawUpperBytes {
			rawUpperExceeds = true
			break
		}
		rawUpperBytes += len(mutation.value)
	}
	if rawUpperExceeds && scratch != nil {
		scratch.hybridClassificationPasses++
	}
	stagedBytes := 0
	changes := ordered[:0]
	for _, mutation := range ordered {
		if len(mutation.key) > m.user.Limits.MaxKeyBytes ||
			len(mutation.value) > m.user.Limits.MaxDocumentBytes {
			return nil, ResultTargetBound, nil
		}
		if !mutation.delete {
			if err := vibejson.Validate(mutation.value); err != nil {
				return nil, ResultInvalidDocument, nil
			}
		}
		current, found, err := snapshot.appendRawForPlan(mutation.key, scratch)
		if err != nil {
			return nil, 0, err
		}
		mutation.beforeFound = found
		if m.user.Validation == ValidationDeterministicMutation {
			validation := MutationValidation(0)
			if mutation.delete {
				validation = m.user.Validator.ValidateDelete(mutation.key, current, found)
			} else {
				validation = m.user.Validator.ValidatePut(mutation.key, mutation.value)
			}
			switch validation {
			case MutationValidationAccept:
			case MutationValidationInvalid:
				return nil, ResultInvalidDocument, nil
			case MutationValidationTargetBound:
				return nil, ResultTargetBound, nil
			case MutationValidationWrongShard:
				return nil, ResultWrongShard, nil
			default:
				return nil, 0, fmt.Errorf(
					"%w: mutation validator returned %d", ErrInvalidCollection, validation,
				)
			}
		}
		if mutation.delete && !found || !mutation.delete && found && bytes.Equal(current, mutation.value) {
			continue
		}
		if len(mutation.key) > m.user.Limits.MaxBatchBytes-stagedBytes {
			return nil, ResultTargetBound, nil
		}
		stagedBytes += len(mutation.key)
		if len(mutation.value) > m.user.Limits.MaxBatchBytes-stagedBytes {
			return nil, ResultTargetBound, nil
		}
		stagedBytes += len(mutation.value)
		if !rawUpperExceeds {
			if err := describeFinalMutation(&mutation, current, scratch); err != nil {
				return nil, 0, err
			}
		}
		changes = append(changes, mutation)
	}
	if rawUpperExceeds {
		for index := range changes {
			mutation := &changes[index]
			current, found, err := snapshot.appendRawForPlan(mutation.key, scratch)
			if err != nil {
				return nil, 0, err
			}
			if found != mutation.beforeFound {
				return nil, 0, ErrInconsistentSnapshot
			}
			if scratch != nil {
				scratch.hybridDescriptorRereads++
			}
			if err := describeFinalMutation(mutation, current, scratch); err != nil {
				return nil, 0, err
			}
		}
	}
	slices.SortFunc(changes, func(left, right finalMutation) int {
		return bytes.Compare(left.key, right.key)
	})
	return changes, ResultApplied, nil
}

func describeFinalMutation(
	mutation *finalMutation,
	current []byte,
	scratch *commandPlanScratch,
) error {
	if mutation == nil {
		return ErrInvalidCollection
	}
	if scratch == nil {
		mutation.before = current
		return nil
	}
	if len(scratch.descriptors) >= math.MaxUint16 {
		return ErrInvalidCollection
	}
	descriptor := mutationValueDescriptor{}
	if mutation.beforeFound {
		descriptor.beforeLength = uint64(len(current))
		descriptor.beforeDigest = sha256.Sum256(current)
		scratch.logicalValueHashes++
	}
	if !mutation.delete {
		descriptor.afterLength = uint64(len(mutation.value))
		descriptor.afterDigest = sha256.Sum256(mutation.value)
		scratch.logicalAfterValueHashes++
	}
	mutation.descriptorIndex = uint16(len(scratch.descriptors))
	mutation.described = true
	scratch.descriptors = append(scratch.descriptors, descriptor)
	return nil
}

func (m *Machine) releaseMutationPlan() {
	clear(m.mutationPlan)
	m.mutationPlan = m.mutationPlan[:0]
}

// pointSnapshot is the planning capability. It exposes point reads plus one
// session-prefix existence probe used only by Open/Release orphan checks; it
// cannot perform an arbitrary collection scan. Keeping the wrapper concrete
// also lets hot point reads inline without interface escapes.
type pointSnapshot struct {
	value   *durable.Snapshot
	overlay *logicalOverlay
}

func (s pointSnapshot) appendRaw(dst []byte, key []byte) ([]byte, bool, error) {
	if s.overlay != nil {
		return s.overlay.appendRaw(dst, key)
	}
	return s.value.AppendRaw(dst, key)
}

func (s pointSnapshot) appendRawForPlan(
	key []byte,
	scratch *commandPlanScratch,
) ([]byte, bool, error) {
	if scratch == nil {
		return s.appendRaw(nil, key)
	}
	scratch.logicalValueReads++
	var appended []byte
	var found bool
	var err error
	if s.overlay != nil {
		appended, found, err = s.overlay.appendRawTracked(
			scratch.currentValue[:0], key, &scratch.physicalBaseValueReads,
		)
	} else {
		appended, found, err = s.appendRaw(scratch.currentValue[:0], key)
	}
	if err != nil {
		return nil, false, err
	}
	scratch.currentValue = appended
	return appended, found, nil
}

func (s pointSnapshot) hasRawPrefix(prefix []byte) (bool, error) {
	found := false
	visit := func(_, _ []byte) error {
		found = true
		return errStopSessionPrefix
	}
	var err error
	if s.overlay != nil {
		err = s.overlay.rangePrefixRaw(prefix, visit)
	} else {
		err = s.value.RangePrefixRaw(prefix, visit)
	}
	if errors.Is(err, errStopSessionPrefix) {
		return true, nil
	}
	return found, err
}

func (s pointSnapshot) rangeSessionSlots(
	digest [sha256.Size]byte,
	visit func(key, value []byte) error,
) error {
	if digest == ([sha256.Size]byte{}) {
		return ErrSessionCorrupt
	}
	var prefix [1 + sha256.Size]byte
	prefix[0] = 2
	copy(prefix[1:], digest[:])
	if s.overlay != nil {
		return s.overlay.rangePrefixRaw(prefix[:], visit)
	}
	return s.value.RangePrefixRaw(prefix[:], visit)
}

func sessionAt(
	snapshot pointSnapshot,
	key [33]byte,
	scratch *commandPlanScratch,
) (SessionView, bool, error) {
	var dst []byte
	decodeCopy := false
	if scratch != nil {
		dst = scratch.sessionRead[:0]
		if cap(scratch.decodeRead) != 0 {
			dst = scratch.decodeRead[:0]
			decodeCopy = true
		}
	}
	record, found, err := snapshot.appendRaw(dst, key[:])
	if err != nil || !found {
		return SessionView{}, found, err
	}
	if scratch != nil {
		if decodeCopy {
			if len(record) > cap(scratch.sessionRead) {
				return SessionView{}, false, ErrSessionCorrupt
			}
			scratch.sessionRead = append(scratch.sessionRead[:0], record...)
			record = scratch.sessionRead
		} else {
			scratch.sessionRead = record
		}
	}
	view, err := OpenSessionRecord(record)
	if err != nil {
		return SessionView{}, false, err
	}
	if key != SessionStorageKey(view.Digest) {
		return SessionView{}, false, fmt.Errorf("%w: session storage key mismatch", ErrSessionCorrupt)
	}
	return view, true, nil
}

func sessionSlotAt(
	snapshot pointSnapshot,
	key [35]byte,
	scratch *commandPlanScratch,
) (SessionSlotView, bool, error) {
	var dst []byte
	decodeCopy := false
	if scratch != nil {
		dst = scratch.slotRead[:0]
		if cap(scratch.decodeRead) != 0 {
			dst = scratch.decodeRead[:0]
			decodeCopy = true
		}
	}
	record, found, err := snapshot.appendRaw(dst, key[:])
	if err != nil || !found {
		return SessionSlotView{}, found, err
	}
	if scratch != nil {
		if decodeCopy {
			if len(record) > cap(scratch.slotRead) {
				return SessionSlotView{}, false, ErrSessionCorrupt
			}
			scratch.slotRead = append(scratch.slotRead[:0], record...)
			record = scratch.slotRead
		} else {
			scratch.slotRead = record
		}
	}
	view, err := OpenSessionSlot(record)
	if err != nil {
		return SessionSlotView{}, false, err
	}
	want, err := SessionSlotStorageKey(view.SessionDigest, view.Slot)
	if err != nil || key != want {
		return SessionSlotView{}, false, fmt.Errorf("%w: slot storage key mismatch", ErrSessionCorrupt)
	}
	return view, true, nil
}

func ensureNoSessionSlots(
	snapshot pointSnapshot,
	digest [sha256.Size]byte,
	retryWindow uint16,
) error {
	if retryWindow == 0 || retryWindow > MaxSessionRetryWindow {
		return ErrSessionCorrupt
	}
	var prefix [1 + sha256.Size]byte
	prefix[0] = 2
	copy(prefix[1:], digest[:])
	found, err := snapshot.hasRawPrefix(prefix[:])
	if err != nil {
		return err
	}
	if found {
		return fmt.Errorf("%w: orphan session slot", ErrSessionCorrupt)
	}
	return nil
}

func (m *Machine) persistTransition(
	next State,
	changes []finalMutation,
	plan commandPlan,
) error {
	defer m.releaseCaptureChanges()
	var transition CapturedTransition
	var captureRecord []byte
	if m.shouldCaptureTransition(next) {
		transition = m.capturedTransition(next, changes)
		if !validCapturedTransition(transition) {
			return ErrTransitionCapture
		}
		maxRecord, err := m.capture.MaxEncodedBytes(transition.Bounds())
		if err != nil || maxRecord <= 0 {
			return errors.Join(ErrTransitionCapture, err)
		}
		captureRecord, err = m.capture.AppendTransition(m.captureBuffer[:0], transition)
		if err != nil || len(captureRecord) == 0 || len(captureRecord) > maxRecord ||
			len(captureRecord) > m.captureTarget.Collection.MaxDocumentBytes() {
			return errors.Join(ErrTransitionCapture, err)
		}
		m.captureBuffer = captureRecord
	}
	stateEnvelope, err := AppendState(nil, next)
	if err != nil {
		return err
	}
	if err := m.checkTransitionCapacityWithCapture(
		next, changes, plan, len(captureRecord),
	); err != nil {
		return err
	}
	systemDocuments := 1
	if plan.writeSession {
		systemDocuments++
	}
	if plan.writeSlot {
		systemDocuments++
	}
	if plan.deleteSession {
		systemDocuments += 1 + int(plan.deleteSlots)
	}
	members := []durable.NamedCollection{{
		Name: systemCollectionName, Collection: m.system.Collection,
		BatchDocumentsHint: systemDocuments,
	}}
	if len(changes) != 0 {
		members = append(members, durable.NamedCollection{
			Name: m.userName, Collection: m.user.Collection,
			BatchDocumentsHint: len(changes),
		})
	}
	if len(captureRecord) != 0 {
		members = append(members, durable.NamedCollection{
			Name: m.captureTarget.Name, Collection: m.captureTarget.Collection,
			BatchDocumentsHint: 1,
		})
	}
	writeTransition := func(batch *durable.DatabaseBatch) error {
		systemBatch, err := batch.Collection(systemCollectionName)
		if err != nil {
			return err
		}
		if err := systemBatch.Put(stateKey, stateEnvelope); err != nil {
			return err
		}
		if plan.writeSession {
			if err := systemBatch.Put(plan.sessionKey[:], plan.sessionRecord); err != nil {
				return err
			}
		}
		if plan.writeSlot {
			if err := systemBatch.Put(plan.slotKey[:], plan.slotRecord); err != nil {
				return err
			}
		}
		if plan.deleteSession {
			if err := systemBatch.Delete(plan.sessionKey[:]); err != nil {
				return err
			}
			for slot := uint16(0); slot < plan.deleteSlots; slot++ {
				key, keyErr := SessionSlotStorageKey(plan.sessionDigest, slot)
				if keyErr != nil {
					return keyErr
				}
				if err := systemBatch.Delete(key[:]); err != nil {
					return err
				}
			}
		}
		if len(captureRecord) != 0 {
			captureBatch, captureErr := batch.Collection(m.captureTarget.Name)
			if captureErr != nil {
				return captureErr
			}
			binary.BigEndian.PutUint64(m.captureKey[:], transition.Applied)
			if captureErr = captureBatch.Put(m.captureKey[:], captureRecord); captureErr != nil {
				return captureErr
			}
		}
		if len(changes) == 0 {
			return nil
		}
		userBatch, err := batch.Collection(m.userName)
		if err != nil {
			return err
		}
		for _, mutation := range changes {
			if mutation.delete {
				err = userBatch.Delete(mutation.key)
			} else {
				err = userBatch.Put(mutation.key, mutation.value)
			}
			if err != nil {
				return err
			}
		}
		return nil
	}
	if m.checkpointGroup != nil {
		err = m.checkpointGroup.Update(
			next.Applied, members, m.options.TxnLimits, writeTransition,
		)
	} else {
		err = durable.UpdateCollections(
			m.txnLog, members, m.options.TxnLimits, writeTransition,
		)
	}
	if len(changes) != 0 && m.user.ObserveMutationAttempt != nil {
		m.user.ObserveMutationAttempt(AttemptedMutationKeys{changes: changes}, err)
	}
	if err != nil {
		return err
	}
	if len(changes) == 0 && m.openedImageGeneration != 0 &&
		m.user.Collection.Generation() == m.openedImageGeneration {
		m.openedImageApplied = next.Applied
	} else {
		m.openedImageApplied = 0
		m.openedImageGeneration = 0
	}
	m.state = next
	if m.binding.Distribution != next.Binding.Distribution {
		m.distribution = []byte(next.Binding.Distribution)
	}
	if m.binding.Shard != next.Binding.Shard {
		m.shard = []byte(next.Binding.Shard)
	}
	m.binding = next.Binding
	m.initialized = true
	m.publication = publicationFromState(next)
	if len(captureRecord) != 0 {
		if err := m.capture.Published(transition); err != nil {
			return errors.Join(ErrTransitionCapture, err)
		}
	}
	return nil
}

func (m *Machine) checkTransitionCapacity(
	next State,
	changes []finalMutation,
	plan commandPlan,
) error {
	defer m.releaseCaptureChanges()
	captureBytes := 0
	if m.shouldCaptureTransition(next) {
		transition := m.capturedTransition(next, changes)
		if !validCapturedTransition(transition) {
			return ErrTransitionCapture
		}
		var err error
		captureBytes, err = m.capture.MaxEncodedBytes(transition.Bounds())
		if err != nil || captureBytes <= 0 ||
			captureBytes > m.captureTarget.Collection.MaxDocumentBytes() {
			return errors.Join(ErrTransitionCapture, err)
		}
	}
	return m.checkTransitionCapacityWithCapture(
		next, changes, plan, captureBytes,
	)
}

func (m *Machine) checkTransitionCapacityWithCapture(
	next State,
	changes []finalMutation,
	plan commandPlan,
	captureBytes int,
) error {
	stateEnvelope, err := AppendState(nil, next)
	if err != nil {
		return err
	}
	stateBytes := len(stateKey) + len(stateEnvelope)
	systemDocs := 1
	if plan.writeSession {
		if len(plan.sessionRecord) == 0 || len(plan.sessionRecord) > m.system.Limits.MaxDocumentBytes {
			return ErrAdmissionBound
		}
		stateBytes += len(plan.sessionKey) + len(plan.sessionRecord)
		systemDocs++
	}
	if plan.writeSlot {
		if len(plan.slotRecord) == 0 || len(plan.slotRecord) > m.system.Limits.MaxDocumentBytes {
			return ErrAdmissionBound
		}
		stateBytes += len(plan.slotKey) + len(plan.slotRecord)
		systemDocs++
	}
	if plan.deleteSession {
		if plan.writeSession || plan.writeSlot || plan.deleteSlots == 0 ||
			plan.deleteSlots > m.options.RetryWindow {
			return ErrAdmissionBound
		}
		deleteBytes := len(plan.sessionKey) + int(plan.deleteSlots)*len(plan.slotKey)
		if deleteBytes < 0 || stateBytes > math.MaxInt-deleteBytes {
			return ErrAdmissionBound
		}
		stateBytes += deleteBytes
		systemDocs += 1 + int(plan.deleteSlots)
	}
	if len(stateEnvelope) > m.system.Limits.MaxDocumentBytes ||
		systemDocs > m.system.Limits.MaxDistinctMutations ||
		stateBytes > m.system.Limits.MaxBatchBytes {
		return ErrAdmissionBound
	}
	userBytes := 0
	for _, mutation := range changes {
		userBytes += len(mutation.key) + len(mutation.value)
	}
	if len(changes) > m.user.Limits.MaxDistinctMutations || userBytes > m.user.Limits.MaxBatchBytes {
		return ErrAdmissionBound
	}
	collections := 1
	if len(changes) != 0 {
		collections++
	}
	if captureBytes != 0 {
		if captureBytes > m.captureTarget.Collection.MaxBatchBytes()-8 ||
			userBytes > math.MaxInt-captureBytes-8 {
			return ErrAdmissionBound
		}
		collections++
		userBytes += 8 + captureBytes
		systemDocs++
	}
	if collections > m.options.TxnLimits.MaxCollections ||
		systemDocs+len(changes) > m.options.TxnLimits.MaxDocuments ||
		int64(stateBytes) > math.MaxInt64-int64(userBytes) ||
		int64(stateBytes)+int64(userBytes) > m.options.TxnLimits.MaxBytes {
		return ErrAdmissionBound
	}
	return nil
}
