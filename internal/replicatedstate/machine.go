package replicatedstate

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/store/durable"
	"github.com/thesyncim/vibejson"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// SystemCollectionName is the reserved durable collection name owned by a
// Machine. Catalogs and adapters must reject it as a user collection name.
const SystemCollectionName = "__vibedb_replicated_state"

const systemCollectionName = SystemCollectionName

var (
	stateKey              = []byte{0}
	bootstrapDigestDomain = []byte("vibedb/replicated-state/static-bootstrap\x00")
)

// Machine is a serial, unserved implementation of raftmodel.StateMachine.
// Every supplied collection must be exclusively mutated through this Machine
// while it is open.
type Machine struct {
	mu sync.RWMutex

	binding         Binding
	bootstrap       []byte
	bootstrapDigest [32]byte
	system          CollectionTarget
	userName        string
	user            CollectionTarget
	txnLog          *durable.TxnLog
	options         Options

	state       State
	publication raftmodel.Publication
	initialized bool
	poison      error
}

// CompletionCapacityState is the constant-size durable apply cut used by
// higher-level completion-capacity proofs. Initialized distinguishes the empty
// pre-bootstrap machine from the durable static snapshot at Applied index 1.
type CompletionCapacityState struct {
	Initialized     bool
	Applied         uint64
	CompletionCount uint64
}

// Open validates and freezes one system collection and exactly one user
// collection. An empty system collection represents a not-yet-installed static
// bootstrap and is valid only with an empty user collection.
func Open(
	binding Binding,
	bootstrap *pb.Snapshot,
	system CollectionTarget,
	userSpec UserCollection,
	txnLog *durable.TxnLog,
	options Options,
) (result *Machine, resultErr error) {
	if err := binding.validate(); err != nil {
		return nil, err
	}
	if err := options.validate(); err != nil {
		return nil, err
	}
	if err := system.validate(); err != nil {
		return nil, fmt.Errorf("system collection: %w", err)
	}
	if system.Validation != ValidationSchemaFreeJSON ||
		system.ValidationDigest != ([32]byte{}) || system.Validator != nil ||
		system.ObserveMutationAttempt != nil {
		return nil, fmt.Errorf("%w: system collection requires schema-free validation", ErrInvalidCollection)
	}
	if txnLog == nil {
		return nil, fmt.Errorf("%w: nil transaction log", ErrInvalidOptions)
	}
	userName, user := userSpec.Name, userSpec.Target
	if userName == "" || userName == systemCollectionName ||
		len(userName) > replication.MaxCollectionBytes || !utf8.ValidString(userName) ||
		strings.IndexByte(userName, 0) >= 0 {
		return nil, fmt.Errorf("%w: user collection name", ErrInvalidCollection)
	}
	if err := user.validate(); err != nil {
		return nil, fmt.Errorf("user collection %q: %w", userName, err)
	}
	if user.Validation != ValidationDeterministicMutation {
		return nil, fmt.Errorf(
			"user collection %q: %w: deterministic validation is required",
			userName, ErrInvalidCollection,
		)
	}
	if user.Limits.MaxKeyBytes > replication.MaxMutationKeyBytes ||
		user.Limits.MaxDocumentBytes > replication.MaxMutationValueBytes ||
		user.Limits.MaxDistinctMutations > MaxDistinctMutations {
		return nil, fmt.Errorf("%w: user limits exceed the command profile", ErrInvalidCollection)
	}
	if user.Collection == system.Collection {
		return nil, fmt.Errorf("%w: system and user handles alias", ErrInvalidCollection)
	}
	maxStateDocument := 2*MaxStateEnvelopeBytes + 2
	maxCompletionDocument := 2*MaxCompletionRecordBytes + 2
	maxSystemBatchBytes := len(stateKey) + maxStateDocument +
		sha256.Size + 1 + maxCompletionDocument
	if system.Limits.MaxKeyBytes < sha256.Size+1 ||
		system.Limits.MaxDocumentBytes < maxCompletionDocument ||
		system.Limits.MaxDistinctMutations < 2 ||
		system.Limits.MaxBatchBytes < maxSystemBatchBytes {
		return nil, fmt.Errorf("%w: system collection cannot hold bounded records", ErrInvalidCollection)
	}
	maxUserBatchBytes := min(user.Limits.MaxBatchBytes, replication.MaxCommandBytes)
	requiredTxnBytes, ok := checkedTxnBytes(maxUserBatchBytes, maxSystemBatchBytes)
	if !ok {
		return nil, fmt.Errorf("%w: transaction byte proof overflows", ErrInvalidOptions)
	}
	if options.TxnLimits.MaxDocuments < user.Limits.MaxDistinctMutations+2 ||
		options.TxnLimits.MaxBytes < requiredTxnBytes {
		return nil, fmt.Errorf("%w: transaction limits do not cover the frozen apply profile", ErrInvalidOptions)
	}
	if err := txnLog.ValidateCollections([]durable.NamedCollection{
		{Name: systemCollectionName, Collection: system.Collection},
		{Name: userName, Collection: user.Collection},
	}); err != nil {
		return nil, fmt.Errorf("%w: transaction-log binding: %w", ErrInvalidCollection, err)
	}
	bootstrapBytes, bootstrapDigest, err := validateBootstrap(bootstrap)
	if err != nil {
		return nil, err
	}

	binding.Distribution = strings.Clone(binding.Distribution)
	binding.Shard = strings.Clone(binding.Shard)
	userName = strings.Clone(userName)
	m := &Machine{
		binding: binding, bootstrap: bootstrapBytes,
		bootstrapDigest: bootstrapDigest, system: system,
		userName: userName, user: user, txnLog: txnLog, options: options,
	}
	cut, err := durable.SnapshotCollections([]durable.NamedCollection{
		{Name: systemCollectionName, Collection: system.Collection},
		{Name: userName, Collection: user.Collection},
	})
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := cut.Close(); closeErr != nil {
			result = nil
			resultErr = errors.Join(resultErr, closeErr)
		}
	}()
	userSnapshot, ok := cut.Collection(userName)
	if !ok || userSnapshot == nil {
		return nil, fmt.Errorf("%w: missing user snapshot", ErrInconsistentSnapshot)
	}
	if err := validateExistingRows(userSnapshot, user); err != nil {
		return nil, fmt.Errorf("user collection %q: %w", userName, err)
	}
	logical, err := logicalDigest(
		userName, user.Validation, user.ValidationDigest, user.Validator, userSnapshot, nil,
	)
	if err != nil {
		return nil, err
	}
	systemSnapshot, ok := cut.Collection(systemCollectionName)
	if !ok || systemSnapshot == nil {
		return nil, fmt.Errorf("%w: missing system snapshot", ErrInconsistentSnapshot)
	}
	state, present, completionCount, err := scanSystemSnapshot(systemSnapshot, options.MaxCompletions)
	if err != nil {
		return nil, err
	}
	if !present {
		if completionCount != 0 || userSnapshot.Len() != 0 {
			return nil, fmt.Errorf("%w: uninitialized system with durable rows", ErrStateCorrupt)
		}
		m.state = State{
			Binding: binding, LogicalDigest: logical,
			ConfState: new(pb.ConfState), BootstrapDigest: bootstrapDigest,
		}
		m.publication = raftmodel.Publication{LogicalDigest: logical, ConfState: new(pb.ConfState)}
		return m, nil
	}
	if !bindingAdvancesFrom(binding, state.Binding) || state.BootstrapDigest != bootstrapDigest ||
		state.LogicalDigest != logical || state.CompletionCount != completionCount ||
		state.CompletionCount > options.MaxCompletions {
		return nil, fmt.Errorf("%w: persisted publication disagrees with construction", ErrStateCorrupt)
	}
	if state.LastKind == RecordStaticSnapshot &&
		(state.LastEntryDigest != bootstrapDigest ||
			!proto.Equal(state.ConfState, bootstrap.GetMetadata().GetConfState())) {
		return nil, fmt.Errorf("%w: static publication differs from bootstrap", ErrStateCorrupt)
	}
	if err := m.validateRetainedCompletions(systemSnapshot, state); err != nil {
		return nil, err
	}
	m.state = state
	m.binding = state.Binding
	m.initialized = true
	m.publication = publicationFromState(state)
	return m, nil
}

// bindingAdvancesFrom permits reopen through the write-once SQL/WAL binding
// after one or more replicated intact-shard ownership transitions. Immutable
// allocation identity and policy/schema fences remain exact. The three serving
// coordinates must advance together by the same distance because the sole
// ownership control grammar increments each exactly once per committed move.
func bindingAdvancesFrom(initial, current Binding) bool {
	if initial.ClusterID != current.ClusterID ||
		initial.ClusterIncarnation != current.ClusterIncarnation ||
		initial.TopologyRecoveryEpoch != current.TopologyRecoveryEpoch ||
		initial.Distribution != current.Distribution || initial.Shard != current.Shard ||
		initial.AllocationGeneration != current.AllocationGeneration ||
		initial.ShardIncarnation != current.ShardIncarnation || initial.GroupID != current.GroupID ||
		initial.ActivePolicyGeneration != current.ActivePolicyGeneration ||
		initial.ProtectionEpoch != current.ProtectionEpoch ||
		initial.SchemaGeneration != current.SchemaGeneration ||
		current.OwnershipEpoch < initial.OwnershipEpoch ||
		current.RoutingVersion < initial.RoutingVersion ||
		current.RouteGeneration < initial.RouteGeneration {
		return false
	}
	delta := current.OwnershipEpoch - initial.OwnershipEpoch
	return current.RoutingVersion-initial.RoutingVersion == delta &&
		current.RouteGeneration-initial.RouteGeneration == delta
}

func checkedTxnBytes(userBatchBytes, systemBatchBytes int) (int64, bool) {
	if userBatchBytes < 0 || systemBatchBytes < 0 {
		return 0, false
	}
	userBytes, systemBytes := int64(userBatchBytes), int64(systemBatchBytes)
	if userBytes > math.MaxInt64-systemBytes {
		return 0, false
	}
	return userBytes + systemBytes, true
}

func validateExistingRows(snapshot *durable.Snapshot, target CollectionTarget) error {
	if target.Validation != ValidationDeterministicMutation {
		return nil
	}
	return snapshot.RangeRaw(func(key, value []byte) error {
		if err := vibejson.Validate(value); err != nil {
			return fmt.Errorf("%w: malformed JSON in existing row", ErrSchemaProfile)
		}
		switch validation := target.Validator.ValidatePut(key, value); validation {
		case MutationValidationAccept:
			return nil
		case MutationValidationInvalid:
			return fmt.Errorf("%w: mutation validator rejected an existing row", ErrSchemaProfile)
		case MutationValidationTargetBound:
			return fmt.Errorf("%w: existing row exceeds the mutation validator target", ErrSchemaProfile)
		case MutationValidationWrongShard:
			return fmt.Errorf("%w: existing row belongs to another shard", ErrSchemaProfile)
		default:
			return fmt.Errorf(
				"%w: mutation validator returned %d for an existing row",
				ErrInvalidCollection, validation,
			)
		}
	})
}

func validateBootstrap(snapshot *pb.Snapshot) ([]byte, [32]byte, error) {
	if snapshot == nil || snapshot.GetMetadata() == nil ||
		snapshot.GetMetadata().GetIndex() != 1 || snapshot.GetMetadata().GetTerm() != 1 ||
		len(snapshot.GetData()) > MaxStaticBootstrapBytes ||
		(len(snapshot.GetData()) >= len(snapshotBaseMagic) &&
			bytes.Equal(snapshot.GetData()[:len(snapshotBaseMagic)], snapshotBaseMagic[:])) ||
		len(snapshot.ProtoReflect().GetUnknown()) != 0 ||
		len(snapshot.GetMetadata().ProtoReflect().GetUnknown()) != 0 {
		return nil, [32]byte{}, ErrStaticSnapshotOnly
	}
	conf := snapshot.GetMetadata().GetConfState()
	if conf == nil || len(conf.ProtoReflect().GetUnknown()) != 0 {
		return nil, [32]byte{}, ErrStaticSnapshotOnly
	}
	if conf.GetAutoLeave() || len(conf.GetVotersOutgoing()) != 0 ||
		len(conf.GetLearnersNext()) != 0 ||
		len(conf.GetVoters())+len(conf.GetLearners()) > MaxStaticBootstrapMembers {
		return nil, [32]byte{}, ErrStaticSnapshotOnly
	}
	if err := raftmodel.ValidateConfState(conf, 1); err != nil {
		return nil, [32]byte{}, fmt.Errorf("%w: %v", ErrStaticSnapshotOnly, err)
	}
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(snapshot)
	if err != nil || len(encoded) > MaxStaticBootstrapEnvelopeBytes {
		return nil, [32]byte{}, ErrStaticSnapshotOnly
	}
	h := sha256.New()
	_, _ = h.Write(bootstrapDigestDomain)
	_, _ = h.Write(encoded)
	var digest [32]byte
	_ = h.Sum(digest[:0])
	return encoded, digest, nil
}

func scanSystemSnapshot(snapshot *durable.Snapshot, maxCompletions uint64) (State, bool, uint64, error) {
	var state State
	var statePresent bool
	var completions uint64
	err := snapshot.RangeRaw(func(key, value []byte) error {
		if bytes.Equal(key, stateKey) {
			if statePresent {
				return fmt.Errorf("%w: duplicate state row", ErrStateCorrupt)
			}
			raw, err := unwrapJSONHex(value, MaxStateEnvelopeBytes, ErrStateCorrupt)
			if err != nil {
				return err
			}
			state, err = OpenState(raw)
			statePresent = err == nil
			return err
		}
		if len(key) != sha256.Size+1 || key[0] != 1 {
			return fmt.Errorf("%w: unknown system key", ErrCompletionCorrupt)
		}
		raw, err := unwrapJSONHex(value, MaxCompletionRecordBytes, ErrCompletionCorrupt)
		if err != nil {
			return err
		}
		record, err := OpenCompletionRecord(raw)
		if err != nil {
			return err
		}
		want := CompletionKey(record.Tenant, record.ClientID, record.ClientEpoch, record.ClientSequence)
		if !bytes.Equal(key[1:], want[:]) {
			return fmt.Errorf("%w: completion key mismatch", ErrCompletionCorrupt)
		}
		if completions >= maxCompletions {
			return fmt.Errorf("%w: completion count exceeds configured bound", ErrCompletionCorrupt)
		}
		completions++
		return nil
	})
	return state, statePresent, completions, err
}

func (m *Machine) validateRetainedCompletions(snapshot *durable.Snapshot, state State) error {
	seenApplied := make(map[uint64]struct{}, int(state.CompletionCount))
	return snapshot.RangeRaw(func(key, value []byte) error {
		if bytes.Equal(key, stateKey) {
			return nil
		}
		raw, err := unwrapJSONHex(value, MaxCompletionRecordBytes, ErrCompletionCorrupt)
		if err != nil {
			return err
		}
		record, err := OpenCompletionRecord(raw)
		if err != nil {
			return err
		}
		completion, err := replication.OpenCompletion(record.Completion)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrCompletionCorrupt, err)
		}
		if err := m.validateCompletionResult(completion); err != nil {
			return err
		}
		if completion.AppliedSequence < 2 || completion.AppliedSequence > state.Applied ||
			completion.ClusterID != state.Binding.ClusterID ||
			completion.ClusterIncarnation != state.Binding.ClusterIncarnation ||
			completion.TopologyRecoveryEpoch != state.Binding.TopologyRecoveryEpoch ||
			!bytes.Equal(completion.Distribution, []byte(state.Binding.Distribution)) ||
			!bytes.Equal(completion.Shard, []byte(state.Binding.Shard)) ||
			completion.AllocationGeneration != state.Binding.AllocationGeneration ||
			completion.ShardIncarnation != state.Binding.ShardIncarnation ||
			completion.GroupID != state.Binding.GroupID {
			return fmt.Errorf("%w: retained completion binding", ErrCompletionCorrupt)
		}
		if _, duplicate := seenApplied[completion.AppliedSequence]; duplicate {
			return fmt.Errorf("%w: duplicate completion applied sequence", ErrCompletionCorrupt)
		}
		seenApplied[completion.AppliedSequence] = struct{}{}
		if state.ReplicaSetVersion > 1 && completion.AppliedSequence == state.ReplicaSetVersion {
			return fmt.Errorf("%w: completion occupies configuration index", ErrCompletionCorrupt)
		}
		if completion.ResultCode != ResultStaleFence &&
			(completion.ReplicaSetVersion >= completion.AppliedSequence ||
				completion.ReplicaSetVersion > state.ReplicaSetVersion ||
				completion.ActivePolicyGeneration != state.Binding.ActivePolicyGeneration ||
				completion.ProtectionEpoch != state.Binding.ProtectionEpoch ||
				completion.RoutingVersion > state.Binding.RoutingVersion ||
				completion.RouteGeneration > state.Binding.RouteGeneration) {
			return fmt.Errorf("%w: retained completion mutable binding", ErrCompletionCorrupt)
		}
		switch completion.ResultCode {
		case ResultStaleFence:
		case ResultUnknownCollection:
			if record.Collection == m.userName {
				return fmt.Errorf("%w: unknown-collection result names user collection", ErrCompletionCorrupt)
			}
		default:
			if record.Collection != m.userName {
				return fmt.Errorf("%w: result names unknown collection", ErrCompletionCorrupt)
			}
		}
		return nil
	})
}

func (m *Machine) validateCompletionResult(completion replication.CompletionView) error {
	if completion.ResultFormat != ResultFormatMutation || completion.ResultCode < ResultApplied ||
		completion.ResultCode > ResultWrongShard {
		return fmt.Errorf("%w: unsupported completion result grammar", ErrCompletionCorrupt)
	}
	return nil
}

func publicationFromState(state State) raftmodel.Publication {
	return raftmodel.Publication{
		Applied: state.Applied, LogicalDigest: state.LogicalDigest,
		ConfState: cloneConfState(state.ConfState), ReplicaSetVersion: state.ReplicaSetVersion,
	}
}

func clonePublication(p raftmodel.Publication) raftmodel.Publication {
	p.ConfState = cloneConfState(p.ConfState)
	return p
}

// Applied implements raftmodel.StateMachine.
func (m *Machine) Applied() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.publication.Applied
}

// CompletionCapacityState returns a read-only, constant-size view of the
// machine state needed by completion-capacity qualification. A poisoned
// machine fails closed instead of advertising its last publication as usable.
func (m *Machine) CompletionCapacityState() (CompletionCapacityState, error) {
	if m == nil {
		return CompletionCapacityState{}, ErrApplyPoisoned
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := m.checkUsable(); err != nil {
		return CompletionCapacityState{}, err
	}
	return CompletionCapacityState{
		Initialized:     m.initialized,
		Applied:         m.state.Applied,
		CompletionCount: m.state.CompletionCount,
	}, nil
}

// Published implements raftmodel.StateMachine.
func (m *Machine) Published() raftmodel.Publication {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return clonePublication(m.publication)
}

func (m *Machine) fail(err error) error {
	if err == nil {
		return nil
	}
	if m.poison == nil {
		m.poison = err
	}
	return err
}

func (m *Machine) checkUsable() error {
	if m.poison != nil {
		return fmt.Errorf("%w: %v", ErrApplyPoisoned, m.poison)
	}
	return nil
}

func (m *Machine) immutableBindingMatches(command replication.CommandView) bool {
	b := m.binding
	return command.ClusterID == b.ClusterID &&
		command.ClusterIncarnation == b.ClusterIncarnation &&
		command.TopologyRecoveryEpoch == b.TopologyRecoveryEpoch &&
		bytes.Equal(command.Distribution, []byte(b.Distribution)) &&
		bytes.Equal(command.Shard, []byte(b.Shard)) &&
		command.AllocationGeneration == b.AllocationGeneration &&
		command.ShardIncarnation == b.ShardIncarnation && command.GroupID == b.GroupID
}

func (m *Machine) mutableBindingMatches(command replication.CommandView) bool {
	b := m.binding
	return command.ReplicaSetVersion == m.state.ReplicaSetVersion &&
		command.ActivePolicyGeneration == b.ActivePolicyGeneration &&
		command.ProtectionEpoch == b.ProtectionEpoch && command.OwnershipEpoch == b.OwnershipEpoch &&
		command.SchemaGeneration == b.SchemaGeneration && command.RoutingVersion == b.RoutingVersion &&
		command.RouteGeneration == b.RouteGeneration
}

var _ raftmodel.StateMachine = (*Machine)(nil)
