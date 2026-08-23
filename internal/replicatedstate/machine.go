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
	userNameBytes   []byte
	user            CollectionTarget
	distribution    []byte
	shard           []byte
	applyContract   [32]byte
	dataChainHash   *dataChainHasher
	mutationPlan    []finalMutation
	mutationInline  [8]finalMutation
	txnLog          *durable.TxnLog
	options         Options
	capture         TransitionCapture
	captureTarget   TransitionCaptureTarget
	captureBuffer   []byte
	captureChanges  []finalMutation
	captureKey      [8]byte

	state              State
	publication        raftmodel.Publication
	openedImageDigest  [32]byte
	openedImageApplied uint64
	initialized        bool
	poison             error
}

// SessionCapacityState is the constant-size durable apply cut exposed to the
// Raft integration. Initialized distinguishes the empty pre-bootstrap machine
// from the durable static snapshot at Applied index 1.
type SessionCapacityState struct {
	Initialized      bool
	Applied          uint64
	SessionCount     uint64
	SessionSlotCount uint64
}

type openInputs struct {
	binding         Binding
	bootstrap       []byte
	bootstrapDigest [sha256.Size]byte
	system          CollectionTarget
	userName        string
	user            CollectionTarget
	applyContract   [32]byte
	txnLog          *durable.TxnLog
	options         Options
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
	prepared, err := prepareOpenInputs(
		binding, bootstrap, system, userSpec, txnLog, options,
	)
	if err != nil {
		return nil, err
	}
	binding, system = prepared.binding, prepared.system
	userName, user := prepared.userName, prepared.user
	bootstrapBytes, bootstrapDigest := prepared.bootstrap, prepared.bootstrapDigest
	m := &Machine{
		binding: binding, bootstrap: bootstrapBytes,
		bootstrapDigest: bootstrapDigest, system: system,
		userName: userName, userNameBytes: []byte(userName), user: user,
		distribution: []byte(binding.Distribution), shard: []byte(binding.Shard),
		applyContract: prepared.applyContract,
		dataChainHash: newDataChainHasher(),
		txnLog:        prepared.txnLog, options: prepared.options,
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
	imageDigest, err := canonicalImageDigest(
		userName, user.Validation, user.ValidationDigest, user.Validator, userSnapshot, nil,
	)
	if err != nil {
		return nil, fmt.Errorf("user collection %q: %w", userName, err)
	}
	seedDigest, err := dataChainSeedDigest(prepared.applyContract, imageDigest)
	if err != nil {
		return nil, fmt.Errorf("user collection %q: %w", userName, err)
	}
	systemSnapshot, ok := cut.Collection(systemCollectionName)
	if !ok || systemSnapshot == nil {
		return nil, fmt.Errorf("%w: missing system snapshot", ErrInconsistentSnapshot)
	}
	state, present, sessionCount, slotCount, err := scanSessionSystemSnapshot(
		systemSnapshot, options.MaxSessions, options.RetryWindow,
	)
	if err != nil {
		return nil, err
	}
	if !present {
		if options.TransitionCapture != nil {
			return nil, ErrTransitionCapture
		}
		if sessionCount != 0 || slotCount != 0 || userSnapshot.Len() != 0 {
			return nil, fmt.Errorf("%w: uninitialized system with durable rows", ErrStateCorrupt)
		}
		m.state = State{
			Binding: binding, DataChainDigest: seedDigest,
			ApplyContractDigest: prepared.applyContract,
			ConfState:           new(pb.ConfState), BootstrapDigest: bootstrapDigest,
		}
		m.publication = raftmodel.Publication{DataChainDigest: seedDigest, ConfState: new(pb.ConfState)}
		m.openedImageDigest = imageDigest
		return m, nil
	}
	if !bindingAdvancesFrom(binding, state.Binding) || state.BootstrapDigest != bootstrapDigest ||
		state.ApplyContractDigest != prepared.applyContract ||
		state.SessionCount != sessionCount || state.SessionSlotCount != slotCount ||
		state.SessionCount > options.MaxSessions {
		return nil, fmt.Errorf("%w: persisted publication disagrees with construction", ErrStateCorrupt)
	}
	if (state.LastKind == RecordStaticSnapshot || state.LastKind == RecordImportedSnapshot) &&
		state.DataChainDigest != seedDigest {
		return nil, fmt.Errorf("%w: persisted base data-chain seed", ErrStateCorrupt)
	}
	if state.LastKind == RecordStaticSnapshot &&
		(state.LastEntryDigest != bootstrapDigest ||
			!proto.Equal(state.ConfState, bootstrap.GetMetadata().GetConfState())) {
		return nil, fmt.Errorf("%w: static publication differs from bootstrap", ErrStateCorrupt)
	}
	m.state = state
	m.openedImageDigest = imageDigest
	m.openedImageApplied = state.Applied
	m.binding = state.Binding
	m.distribution = []byte(state.Binding.Distribution)
	m.shard = []byte(state.Binding.Shard)
	m.initialized = true
	m.publication = publicationFromState(state)
	if options.TransitionCapture != nil {
		if err := m.beginTransitionCapture(options.TransitionCapture); err != nil {
			return nil, err
		}
	}
	return m, nil
}

func prepareOpenInputs(
	binding Binding,
	bootstrap *pb.Snapshot,
	system CollectionTarget,
	userSpec UserCollection,
	txnLog *durable.TxnLog,
	options Options,
) (openInputs, error) {
	if err := binding.validate(); err != nil {
		return openInputs{}, err
	}
	if err := options.validate(); err != nil {
		return openInputs{}, err
	}
	if err := system.validate(); err != nil {
		return openInputs{}, fmt.Errorf("system collection: %w", err)
	}
	if system.Validation != ValidationOpaqueBinary ||
		system.ValidationDigest != ([32]byte{}) || system.Validator != nil ||
		system.ObserveMutationAttempt != nil {
		return openInputs{}, fmt.Errorf(
			"%w: system collection requires opaque binary validation", ErrInvalidCollection,
		)
	}
	if txnLog == nil {
		return openInputs{}, fmt.Errorf("%w: nil transaction log", ErrInvalidOptions)
	}
	userName, user := userSpec.Name, userSpec.Target
	if userName == "" || userName == systemCollectionName ||
		len(userName) > replication.MaxCollectionBytes || !utf8.ValidString(userName) ||
		strings.IndexByte(userName, 0) >= 0 {
		return openInputs{}, fmt.Errorf("%w: user collection name", ErrInvalidCollection)
	}
	if err := user.validate(); err != nil {
		return openInputs{}, fmt.Errorf("user collection %q: %w", userName, err)
	}
	if user.Validation != ValidationDeterministicMutation {
		return openInputs{}, fmt.Errorf(
			"user collection %q: %w: deterministic validation is required",
			userName, ErrInvalidCollection,
		)
	}
	if user.Limits.MaxKeyBytes > replication.MaxMutationKeyBytes ||
		user.Limits.MaxDocumentBytes > replication.MaxMutationValueBytes ||
		user.Limits.MaxDistinctMutations > MaxDistinctMutations {
		return openInputs{}, fmt.Errorf(
			"%w: user limits exceed the command profile", ErrInvalidCollection,
		)
	}
	if user.Collection == system.Collection {
		return openInputs{}, fmt.Errorf(
			"%w: system and user handles alias", ErrInvalidCollection,
		)
	}
	maxSystemDocument := max(
		MaxStateEnvelopeBytes, MaxSessionRecordBytes, MaxSessionSlotRecordBytes,
	)
	maxSystemBatchBytes := len(stateKey) + MaxStateEnvelopeBytes +
		sha256.Size + 1 + MaxSessionRecordBytes +
		sha256.Size + 3 + MaxSessionSlotRecordBytes
	if system.Limits.MaxKeyBytes < sha256.Size+3 ||
		system.Limits.MaxDocumentBytes < maxSystemDocument ||
		system.Limits.MaxDistinctMutations < 3 ||
		system.Limits.MaxBatchBytes < maxSystemBatchBytes {
		return openInputs{}, fmt.Errorf(
			"%w: system collection cannot hold bounded records", ErrInvalidCollection,
		)
	}
	maxUserBatchBytes := min(user.Limits.MaxBatchBytes, replication.MaxCommandBytes)
	requiredTxnBytes, ok := checkedTxnBytes(maxUserBatchBytes, maxSystemBatchBytes)
	if !ok {
		return openInputs{}, fmt.Errorf(
			"%w: transaction byte proof overflows", ErrInvalidOptions,
		)
	}
	if options.TxnLimits.MaxDocuments < user.Limits.MaxDistinctMutations+3 ||
		options.TxnLimits.MaxBytes < requiredTxnBytes {
		return openInputs{}, fmt.Errorf(
			"%w: transaction limits do not cover the frozen apply profile", ErrInvalidOptions,
		)
	}
	if err := txnLog.ValidateCollections([]durable.NamedCollection{
		{Name: systemCollectionName, Collection: system.Collection},
		{Name: userName, Collection: user.Collection},
	}); err != nil {
		return openInputs{}, fmt.Errorf(
			"%w: transaction-log binding: %w", ErrInvalidCollection, err,
		)
	}
	bootstrapBytes, bootstrapDigest, err := validateBootstrap(bootstrap)
	if err != nil {
		return openInputs{}, err
	}
	binding.Distribution = strings.Clone(binding.Distribution)
	binding.Shard = strings.Clone(binding.Shard)
	contractDigest, err := applyContractDigest(
		userName, user, options.MaxSessions, options.RetryWindow,
	)
	if err != nil {
		return openInputs{}, err
	}
	return openInputs{
		binding: binding, bootstrap: bootstrapBytes, bootstrapDigest: bootstrapDigest,
		system: system, userName: strings.Clone(userName), user: user,
		applyContract: contractDigest,
		txnLog:        txnLog, options: options,
	}, nil
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

type scannedSession struct {
	epoch               uint64
	highSequence        uint64
	physicalSlots       uint16
	retryWindow         uint16
	status              SessionStatus
	seenSlots           uint16
	currentSlots        uint16
	latestSeen          bool
	latestResult        uint32
	orderedLastApplied  uint64
	wrappedFirstApplied uint64
	wrappedLastApplied  uint64
	seen                [MaxSessionRetryWindow / 64]uint64
}

// scanSessionSystemSnapshot performs one ordered, bounded pass over the hidden
// image. It validates state, collision-verifiable session headers, and every
// compact physical ring slot before returning counts.
// Scratch space is proportional to session identities, never operation count.
func scanSessionSystemSnapshot(
	snapshot *durable.Snapshot,
	maxSessions uint64,
	retryWindow uint16,
) (State, bool, uint64, uint64, error) {
	var state State
	var statePresent bool
	var sessionCount, slotCount uint64
	var sessions map[[sha256.Size]byte]scannedSession
	err := snapshot.RangeRaw(func(key, value []byte) error {
		switch {
		case bytes.Equal(key, stateKey):
			if statePresent {
				return fmt.Errorf("%w: duplicate state row", ErrStateCorrupt)
			}
			var err error
			state, err = OpenState(value)
			if err != nil {
				return err
			}
			if state.SessionCount > maxSessions || retryWindow == 0 ||
				state.SessionSlotCount > state.SessionCount*uint64(retryWindow) {
				return fmt.Errorf("%w: bounded session counts", ErrStateCorrupt)
			}
			statePresent = true
			sessions = make(map[[sha256.Size]byte]scannedSession, int(state.SessionCount))
			return nil

		case len(key) == 1+sha256.Size && key[0] == 1:
			if !statePresent {
				return fmt.Errorf("%w: session before state", ErrSessionCorrupt)
			}
			view, err := OpenSessionRecord(value)
			if err != nil {
				return err
			}
			want := SessionStorageKey(view.Digest)
			if !bytes.Equal(key, want[:]) || view.RetryWindow != retryWindow ||
				view.HighSequence == 0 || view.AckThrough >= view.HighSequence ||
				view.PhysicalSlotCount == 0 {
				return fmt.Errorf("%w: session header", ErrSessionCorrupt)
			}
			if _, duplicate := sessions[view.Digest]; duplicate {
				return fmt.Errorf("%w: duplicate session", ErrSessionCorrupt)
			}
			if sessionCount >= maxSessions {
				return fmt.Errorf("%w: session count exceeds configured bound", ErrSessionCorrupt)
			}
			sessions[view.Digest] = scannedSession{
				epoch:        view.ClientEpoch,
				highSequence: view.HighSequence, physicalSlots: view.PhysicalSlotCount,
				retryWindow: view.RetryWindow, status: view.Status,
			}
			sessionCount++
			return nil

		case len(key) == 1+sha256.Size+2 && key[0] == 2:
			if !statePresent {
				return fmt.Errorf("%w: slot before state", ErrSessionCorrupt)
			}
			view, err := OpenSessionSlot(value)
			if err != nil {
				return err
			}
			want, err := SessionSlotStorageKey(view.SessionDigest, view.Slot)
			if err != nil || !bytes.Equal(key, want[:]) {
				return fmt.Errorf("%w: slot key mismatch", ErrSessionCorrupt)
			}
			summary, ok := sessions[view.SessionDigest]
			if !ok || view.Slot >= summary.physicalSlots ||
				view.ClientEpoch > summary.epoch ||
				view.Slot != uint16((view.ClientSequence-1)%uint64(summary.retryWindow)) {
				return fmt.Errorf("%w: slot outside session header", ErrSessionCorrupt)
			}
			word, bit := view.Slot/64, view.Slot%64
			if summary.seen[word]&(uint64(1)<<bit) != 0 {
				return fmt.Errorf("%w: duplicate physical slot", ErrSessionCorrupt)
			}
			summary.seen[word] |= uint64(1) << bit
			summary.seenSlots++
			if err := validateStoredSessionSlot(state, view); err != nil {
				return err
			}
			baseSequence := uint64(view.Slot) + 1
			if baseSequence <= summary.highSequence {
				expectedSequence := baseSequence +
					(summary.highSequence-baseSequence)/uint64(summary.retryWindow)*
						uint64(summary.retryWindow)
				if view.ClientEpoch != summary.epoch ||
					view.ClientSequence != expectedSequence {
					return fmt.Errorf("%w: missing current session retry", ErrSessionCorrupt)
				}
				summary.currentSlots++
				wrapped := summary.highSequence > uint64(summary.retryWindow) &&
					uint64(view.Slot) < summary.highSequence%uint64(summary.retryWindow)
				if wrapped {
					if summary.wrappedLastApplied != 0 &&
						view.AppliedSequence <= summary.wrappedLastApplied {
						return fmt.Errorf("%w: session result order", ErrSessionCorrupt)
					}
					if summary.wrappedFirstApplied == 0 {
						summary.wrappedFirstApplied = view.AppliedSequence
					}
					summary.wrappedLastApplied = view.AppliedSequence
				} else {
					if summary.orderedLastApplied != 0 &&
						view.AppliedSequence <= summary.orderedLastApplied {
						return fmt.Errorf("%w: session result order", ErrSessionCorrupt)
					}
					summary.orderedLastApplied = view.AppliedSequence
				}
				if view.ResultCode == ResultSessionRetired &&
					view.ClientSequence != summary.highSequence {
					return fmt.Errorf("%w: early session retirement result", ErrSessionCorrupt)
				}
				if view.ClientSequence == summary.highSequence {
					summary.latestSeen = true
					summary.latestResult = view.ResultCode
				}
			} else if view.ClientEpoch == summary.epoch {
				return fmt.Errorf("%w: future current-epoch slot", ErrSessionCorrupt)
			}
			sessions[view.SessionDigest] = summary
			slotCount++
			return nil

		default:
			return fmt.Errorf("%w: unknown system key", ErrSessionCorrupt)
		}
	})
	if err != nil {
		return State{}, false, 0, 0, err
	}
	if !statePresent {
		if sessionCount != 0 || slotCount != 0 {
			return State{}, false, 0, 0, fmt.Errorf("%w: rows without state", ErrStateCorrupt)
		}
		return State{}, false, 0, 0, nil
	}
	if sessionCount != state.SessionCount || slotCount != state.SessionSlotCount {
		return State{}, false, 0, 0, fmt.Errorf("%w: session row counts", ErrStateCorrupt)
	}
	for _, summary := range sessions {
		requiredCurrent := uint64(summary.retryWindow)
		if summary.highSequence < requiredCurrent {
			requiredCurrent = summary.highSequence
		}
		wrappedOrderInvalid := summary.highSequence > uint64(summary.retryWindow) &&
			summary.highSequence%uint64(summary.retryWindow) != 0 &&
			(summary.orderedLastApplied == 0 || summary.wrappedFirstApplied == 0 ||
				summary.orderedLastApplied >= summary.wrappedFirstApplied)
		if summary.seenSlots != summary.physicalSlots ||
			uint64(summary.currentSlots) != requiredCurrent || !summary.latestSeen ||
			wrappedOrderInvalid ||
			summary.status == SessionRetired && summary.latestResult != ResultSessionRetired ||
			summary.status == SessionActive && summary.latestResult == ResultSessionRetired {
			return State{}, false, 0, 0, fmt.Errorf("%w: incomplete session ring", ErrSessionCorrupt)
		}
	}
	return state, true, sessionCount, slotCount, nil
}

func validateStoredSessionSlot(state State, slot SessionSlotView) error {
	if slot.ResultCode < ResultApplied || slot.ResultCode > ResultSessionRetired {
		return fmt.Errorf("%w: unsupported completion result grammar", ErrSessionCorrupt)
	}
	if slot.AppliedSequence < 2 || slot.AppliedSequence > state.Applied ||
		state.ReplicaSetVersion > 1 && slot.AppliedSequence == state.ReplicaSetVersion {
		return fmt.Errorf("%w: retained session result binding", ErrSessionCorrupt)
	}
	if slot.ResultCode != ResultStaleFence {
		if slot.ReplicaSetVersion >= slot.AppliedSequence ||
			slot.ReplicaSetVersion > state.ReplicaSetVersion ||
			slot.ActivePolicyGeneration != state.Binding.ActivePolicyGeneration ||
			slot.ProtectionEpoch != state.Binding.ProtectionEpoch ||
			slot.RoutingVersion > state.Binding.RoutingVersion ||
			slot.RouteGeneration > state.Binding.RouteGeneration ||
			state.Binding.RoutingVersion-slot.RoutingVersion !=
				state.Binding.RouteGeneration-slot.RouteGeneration {
			return fmt.Errorf("%w: retained session result mutable binding", ErrSessionCorrupt)
		}
	}
	return nil
}

func validateSessionSlotAgainstHeader(session SessionView, slot SessionSlotView) error {
	if session.Digest != slot.SessionDigest || session.ClientEpoch != slot.ClientEpoch {
		return fmt.Errorf("%w: retained session result identity", ErrSessionCorrupt)
	}
	if slot.ResultCode == ResultSessionRetired {
		if session.Status != SessionRetired || slot.ClientSequence != session.HighSequence {
			return fmt.Errorf("%w: misplaced session retirement result", ErrSessionCorrupt)
		}
	} else if session.Status == SessionRetired && slot.ClientSequence == session.HighSequence {
		return fmt.Errorf("%w: retired session lacks terminal result", ErrSessionCorrupt)
	}
	return nil
}

func (m *Machine) validateCompletionResult(completion replication.CompletionView) error {
	if completion.ResultFormat != ResultFormatMutation || completion.ResultCode < ResultApplied ||
		completion.ResultCode > ResultSessionRetired {
		return fmt.Errorf("%w: unsupported completion result grammar", ErrCompletionCorrupt)
	}
	return nil
}

func publicationFromState(state State) raftmodel.Publication {
	return raftmodel.Publication{
		Applied: state.Applied, DataChainDigest: state.DataChainDigest,
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

// SessionCapacityState returns a read-only, constant-size view of the machine
// state needed by Raft integration checks. A poisoned
// machine fails closed instead of advertising its last publication as usable.
func (m *Machine) SessionCapacityState() (SessionCapacityState, error) {
	if m == nil {
		return SessionCapacityState{}, ErrApplyPoisoned
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := m.checkUsable(); err != nil {
		return SessionCapacityState{}, err
	}
	return SessionCapacityState{
		Initialized: m.initialized, Applied: m.state.Applied,
		SessionCount: m.state.SessionCount, SessionSlotCount: m.state.SessionSlotCount,
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
		bytes.Equal(command.Distribution, m.distribution) &&
		bytes.Equal(command.Shard, m.shard) &&
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
