package replicatedstate

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/store/durable"
	"github.com/thesyncim/vibejson"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

var (
	normalEntryDigestDomain = []byte("vibedb/replicated-state/normal-entry/v1\x00")
	configEntryDigestDomain = []byte("vibedb/replicated-state/config-entry/v1\x00")
)

type commandPlan struct {
	command        replication.CommandViewV1
	key            [33]byte
	completionDoc  []byte
	changes        []finalMutation
	logicalDigest  [32]byte
	resultCode     uint32
	newCompletion  bool
	exactDuplicate bool
	conflict       bool
}

// ApplyNormal implements raftmodel.StateMachine.
func (m *Machine) ApplyNormal(meta raftmodel.ApplyMeta, data []byte) (raftmodel.Publication, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkUsable(); err != nil {
		return raftmodel.Publication{}, err
	}
	if meta.Type != pb.EntryNormal || meta.Term == 0 || meta.Term == math.MaxUint64 {
		return raftmodel.Publication{}, m.fail(fmt.Errorf("%w: invalid normal metadata", ErrApplySequence))
	}
	if len(data) > replication.MaxCommandBytes {
		return raftmodel.Publication{}, m.fail(ErrAdmissionBound)
	}
	digest := normalEntryDigest(meta, data)
	replay, err := m.checkTransition(meta, RecordNormal, digest)
	if err != nil {
		return raftmodel.Publication{}, m.fail(err)
	}
	if replay {
		return clonePublication(m.publication), nil
	}
	if len(data) == 0 {
		next := m.nextState(meta, RecordNormal, digest)
		if err := m.persistTransition(next, nil, [33]byte{}, nil); err != nil {
			return raftmodel.Publication{}, m.fail(err)
		}
		return clonePublication(m.publication), nil
	}
	command, err := replication.OpenCommandV1(data)
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
	plan, planErr := m.planCommand(command, meta.Index, systemSnapshot, userSnapshot)
	err = errors.Join(planErr, cut.Close())
	if err != nil {
		return raftmodel.Publication{}, m.fail(err)
	}
	next := m.nextState(meta, RecordNormal, digest)
	next.LogicalDigest = plan.logicalDigest
	if plan.newCompletion {
		next.CompletionCount++
	}
	var completionDoc []byte
	if plan.newCompletion {
		completionDoc = plan.completionDoc
	}
	if err := m.persistTransition(next, plan.changes, plan.key, completionDoc); err != nil {
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
	if err := m.persistTransition(next, nil, [33]byte{}, nil); err != nil {
		return raftmodel.Publication{}, m.fail(err)
	}
	return clonePublication(m.publication), nil
}

// InstallSnapshot implements raftmodel.StateMachine. V1 accepts only the
// deterministic exact static bootstrap fixed at Open.
func (m *Machine) InstallSnapshot(snapshot *pb.Snapshot) (raftmodel.Publication, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkUsable(); err != nil {
		return raftmodel.Publication{}, err
	}
	encoded, digest, err := validateBootstrap(snapshot)
	if err != nil || digest != m.bootstrapDigest || !bytes.Equal(encoded, m.bootstrap) {
		return raftmodel.Publication{}, m.fail(ErrStaticSnapshotOnly)
	}
	if m.initialized {
		if m.state.Applied == 1 && m.state.LastKind == RecordStaticSnapshot &&
			m.state.LastTerm == 1 && m.state.LastEntryDigest == m.bootstrapDigest &&
			proto.Equal(m.state.ConfState, snapshot.GetMetadata().GetConfState()) {
			return clonePublication(m.publication), nil
		}
		return raftmodel.Publication{}, m.fail(ErrStaticSnapshotOnly)
	}
	next := StateV1{
		Binding: m.binding, Applied: 1, LastTerm: 1,
		LastKind: RecordStaticSnapshot, LastEntryType: pb.EntryNormal,
		LastEntryDigest: m.bootstrapDigest, LogicalDigest: m.state.LogicalDigest,
		ConfState:         cloneConfState(snapshot.GetMetadata().GetConfState()),
		ReplicaSetVersion: 1, BootstrapDigest: m.bootstrapDigest,
	}
	if err := m.persistTransition(next, nil, [33]byte{}, nil); err != nil {
		return raftmodel.Publication{}, m.fail(err)
	}
	return clonePublication(m.publication), nil
}

// AdmitCommand performs the complete non-reserving v1 pre-proposal check.
// Serving remains forbidden because a successful return does not reserve the
// proved storage for the future committed entry.
func (m *Machine) AdmitCommand(data []byte) error {
	command, err := replication.OpenCommandV1(data)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
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
	plan, planErr := m.planCommand(command, m.state.Applied+1, systemSnapshot, userSnapshot)
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
		var digest [32]byte
		copy(digest[:], plan.key[1:])
		return &RequestConflictError{Key: digest}
	case plan.exactDuplicate:
		return m.checkTransitionCapacity(m.hypotheticalState(command, plan), nil, [33]byte{}, nil)
	case plan.resultCode == ResultStaleFence:
		return ErrStaleCommand
	case plan.resultCode != ResultApplied:
		return ErrAdmissionBound
	default:
		return m.checkTransitionCapacity(m.hypotheticalState(command, plan), plan.changes, plan.key, plan.completionDoc)
	}
}

func (m *Machine) hypotheticalState(command replication.CommandViewV1, plan commandPlan) StateV1 {
	meta := raftmodel.ApplyMeta{Index: m.state.Applied + 1, Term: 1, Type: pb.EntryNormal}
	next := m.nextState(meta, RecordNormal, normalEntryDigest(meta, command.Bytes()))
	next.LogicalDigest = plan.logicalDigest
	if plan.newCompletion {
		next.CompletionCount++
	}
	return next
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

func (m *Machine) nextState(meta raftmodel.ApplyMeta, kind RecordKind, digest [32]byte) StateV1 {
	return StateV1{
		Binding: m.binding, Applied: meta.Index, LastTerm: meta.Term,
		LastKind: kind, LastEntryType: meta.Type, LastEntryDigest: digest,
		LogicalDigest: m.state.LogicalDigest, ConfState: cloneConfState(m.state.ConfState),
		ReplicaSetVersion: m.state.ReplicaSetVersion,
		BootstrapDigest:   m.bootstrapDigest, CompletionCount: m.state.CompletionCount,
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
	command replication.CommandViewV1,
	applied uint64,
	systemSnapshot, userSnapshot *durable.Snapshot,
) (commandPlan, error) {
	plan := commandPlan{command: command, logicalDigest: m.state.LogicalDigest}
	digest := CompletionKeyV1(command.Tenant, command.ClientID, command.ClientEpoch, command.ClientSequence)
	plan.key = completionStorageKey(digest)
	existing, found, err := completionAt(systemSnapshot, plan.key)
	if err != nil {
		return commandPlan{}, err
	}
	if found {
		if !recordTupleMatchesCommand(existing, command) {
			return commandPlan{}, fmt.Errorf("%w: completion-key hash collision", ErrCompletionCorrupt)
		}
		if recordMatchesCommand(existing, command) {
			plan.exactDuplicate = true
			return plan, nil
		}
		plan.conflict = true
		return plan, nil
	}
	if m.state.CompletionCount >= m.options.MaxCompletions {
		return commandPlan{}, ErrAdmissionBound
	}
	plan.newCompletion = true
	switch {
	case !m.mutableBindingMatches(command):
		plan.resultCode = ResultStaleFence
	case string(command.Collection) != m.userName:
		plan.resultCode = ResultUnknownCollection
	default:
		plan.changes, plan.resultCode, err = m.planMutations(command, userSnapshot)
		if err != nil {
			return commandPlan{}, err
		}
		if plan.resultCode == ResultApplied {
			plan.logicalDigest, err = logicalDigestV1(m.userName, userSnapshot, plan.changes)
			if err != nil {
				return commandPlan{}, err
			}
		}
	}
	plan.completionDoc, err = m.makeCompletionDocument(command, applied, plan.resultCode)
	if err != nil {
		return commandPlan{}, err
	}
	return plan, nil
}

func (m *Machine) planMutations(
	command replication.CommandViewV1,
	snapshot *durable.Snapshot,
) ([]finalMutation, uint32, error) {
	ordered := make([]finalMutation, 0, min(command.MutationCount(), m.user.Limits.MaxDistinctMutations+1))
	positions := make(map[string]int, min(command.MutationCount(), m.user.Limits.MaxDistinctMutations+1))
	iterator := command.Mutations()
	for iterator.Next() {
		mutation := iterator.Mutation()
		if at, ok := positions[string(mutation.Key)]; ok {
			ordered[at].delete = mutation.Kind == replication.MutationDelete
			ordered[at].value = bytes.Clone(mutation.Value)
			continue
		}
		positions[string(mutation.Key)] = len(ordered)
		ordered = append(ordered, finalMutation{
			key: bytes.Clone(mutation.Key), value: bytes.Clone(mutation.Value),
			delete: mutation.Kind == replication.MutationDelete,
		})
		if len(ordered) > m.user.Limits.MaxDistinctMutations {
			return nil, ResultTargetBound, nil
		}
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
		current, found, err := snapshot.AppendRaw(nil, mutation.key)
		if err != nil {
			return nil, 0, err
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
		changes = append(changes, mutation)
	}
	return changes, ResultApplied, nil
}

func (m *Machine) makeCompletionDocument(
	command replication.CommandViewV1,
	applied uint64,
	code uint32,
) ([]byte, error) {
	resultDigest := replication.CompletionResultDigestV1(code, ResultFormatMutationV1, nil)
	completion, err := replication.AppendCompletionV1(nil, replication.CompletionV1{
		ClusterID: command.ClusterID, ClusterIncarnation: command.ClusterIncarnation,
		TopologyRecoveryEpoch: command.TopologyRecoveryEpoch,
		Distribution:          string(command.Distribution), Shard: string(command.Shard),
		AllocationGeneration: command.AllocationGeneration,
		ShardIncarnation:     command.ShardIncarnation, GroupID: command.GroupID,
		ReplicaSetVersion:      command.ReplicaSetVersion,
		ActivePolicyGeneration: command.ActivePolicyGeneration,
		ProtectionEpoch:        command.ProtectionEpoch, RoutingVersion: command.RoutingVersion,
		RouteGeneration: command.RouteGeneration, Tenant: command.Tenant,
		ClientID: command.ClientID, ClientEpoch: command.ClientEpoch,
		ClientSequence: command.ClientSequence, Fingerprint: command.Fingerprint,
		RetryHome: command.RetryHome, AppliedSequence: applied,
		ResultCode: code, ResultFormat: ResultFormatMutationV1,
		Storage: replication.CompletionInline, ResultDigest: resultDigest,
	})
	if err != nil {
		return nil, err
	}
	record, err := AppendCompletionRecordV1(nil, CompletionRecordV1{
		Tenant: command.Tenant, ClientID: command.ClientID,
		ClientEpoch: command.ClientEpoch, ClientSequence: command.ClientSequence,
		RetryHome: command.RetryHome, Fingerprint: command.Fingerprint,
		CommandDigest: CommandDigestV1(command.Bytes()), Collection: string(command.Collection),
		Completion: completion,
	})
	if err != nil {
		return nil, err
	}
	return wrapJSONHex(nil, record), nil
}

func completionAt(snapshot *durable.Snapshot, key [33]byte) (CompletionRecordV1, bool, error) {
	document, found, err := snapshot.AppendRaw(nil, key[:])
	if err != nil || !found {
		return CompletionRecordV1{}, found, err
	}
	raw, err := unwrapJSONHex(document, MaxCompletionRecordBytes, ErrCompletionCorrupt)
	if err != nil {
		return CompletionRecordV1{}, false, err
	}
	record, err := OpenCompletionRecordV1(raw)
	return record, err == nil, err
}

func (m *Machine) persistTransition(
	next StateV1,
	changes []finalMutation,
	completionKey [33]byte,
	completionDocument []byte,
) error {
	stateEnvelope, err := AppendStateV1(nil, next)
	if err != nil {
		return err
	}
	stateDocument := wrapJSONHex(nil, stateEnvelope)
	if err := m.checkTransitionCapacity(next, changes, completionKey, completionDocument); err != nil {
		return err
	}
	members := []durable.NamedCollection{{Name: systemCollectionName, Collection: m.system.Collection}}
	if len(changes) != 0 {
		members = append(members, durable.NamedCollection{Name: m.userName, Collection: m.user.Collection})
	}
	err = durable.UpdateCollections(m.txnLog, members, m.options.TxnLimits, func(batch *durable.DatabaseBatch) error {
		systemBatch, err := batch.Collection(systemCollectionName)
		if err != nil {
			return err
		}
		if err := systemBatch.Put(stateKey, stateDocument); err != nil {
			return err
		}
		if len(completionDocument) != 0 {
			if err := systemBatch.Put(completionKey[:], completionDocument); err != nil {
				return err
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
	})
	if err != nil {
		return err
	}
	m.state = next
	m.initialized = true
	m.publication = publicationFromState(next)
	return nil
}

func (m *Machine) checkTransitionCapacity(
	next StateV1,
	changes []finalMutation,
	completionKey [33]byte,
	completionDocument []byte,
) error {
	stateEnvelope, err := AppendStateV1(nil, next)
	if err != nil {
		return err
	}
	stateBytes := len(stateKey) + 2*len(stateEnvelope) + 2
	systemDocs := 1
	if len(completionDocument) != 0 {
		if len(completionDocument) > m.system.Limits.MaxDocumentBytes {
			return ErrAdmissionBound
		}
		stateBytes += len(completionKey) + len(completionDocument)
		systemDocs++
	}
	if 2*len(stateEnvelope)+2 > m.system.Limits.MaxDocumentBytes ||
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
	if collections > m.options.TxnLimits.MaxCollections ||
		systemDocs+len(changes) > m.options.TxnLimits.MaxDocuments ||
		int64(stateBytes+userBytes) > m.options.TxnLimits.MaxBytes {
		return ErrAdmissionBound
	}
	return nil
}
