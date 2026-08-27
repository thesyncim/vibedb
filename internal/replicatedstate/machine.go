package replicatedstate

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"math"
	"slices"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/executionpin"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/internal/routegate"
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

	binding                Binding
	bootstrap              []byte
	bootstrapDigest        [32]byte
	system                 CollectionTarget
	userName               string
	user                   CollectionTarget
	relations              []relationCollection
	manifestDigest         [sha256.Size]byte
	members                []durable.NamedCollection
	distribution           []byte
	shard                  []byte
	applyContract          [32]byte
	dataChainHash          *dataChainHasher
	mutationPlan           []finalMutation
	mutationInline         [8]finalMutation
	bundlePlan             []finalMutation
	bundleRelations        []plannedRelationChanges
	transitionMembers      []durable.NamedCollection
	batchTelemetry         normalBatchTelemetry
	txnLog                 *durable.TxnLog
	checkpointGroup        *durable.CheckpointGroup
	options                Options
	transactionIntents     map[transactionIntentIdentity]reopenedTransactionIntentOwner
	transactionIntentKeys  []byte
	routeGate              *routegate.Machine
	routeGateMaxRecords    uint64
	applyCut               durable.DatabaseSnapshot
	capture                TransitionCapture
	splitCaptureActivation *SplitCaptureActivation
	captureTarget          TransitionCaptureTarget
	reservedCaptureTarget  TransitionCaptureTarget
	captureBuffer          []byte
	captureChanges         []finalMutation
	captureKey             [8]byte
	requestLedgerSteps     [requestledger.MaxPendingWaveSteps]requestledger.StepRef

	state                 State
	publication           raftmodel.Publication
	openedImageDigest     [32]byte
	openedImageApplied    uint64
	openedImageGeneration uint64
	initialized           bool
	schemaTransitioned    bool
	poison                error
}

// SessionCapacityState is the constant-size durable apply cut exposed to the
// Raft integration. Initialized distinguishes the empty pre-bootstrap machine
// from the durable static snapshot at Applied index 1.
type SessionCapacityState struct {
	Initialized           bool
	Applied               uint64
	SessionCount          uint64
	SessionSlotCount      uint64
	SessionEpochHighWater uint64
	AuthorityBindingCount uint64
}

type openInputs struct {
	binding         Binding
	bootstrap       []byte
	bootstrapDigest [sha256.Size]byte
	system          CollectionTarget
	userName        string
	user            CollectionTarget
	relations       []relationCollection
	manifestDigest  [sha256.Size]byte
	members         []durable.NamedCollection
	applyContract   [32]byte
	txnLog          *durable.TxnLog
	checkpointGroup *durable.CheckpointGroup
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
	return OpenBundle(
		binding, bootstrap, system,
		[]RelationCollection{{
			Relation: 1, Kind: RelationJSON, Name: userSpec.Name, Target: userSpec.Target,
			LocalIndexes: userSpec.LocalIndexes,
		}},
		txnLog, options,
	)
}

// OpenBundle validates and freezes one hidden system collection plus a dense,
// schema-generation-bound relation bundle. Every relation handle is included
// in the fixed transaction/checkpoint membership before committed input can be
// admitted.
func OpenBundle(
	binding Binding,
	bootstrap *pb.Snapshot,
	system CollectionTarget,
	relationSpecs []RelationCollection,
	txnLog *durable.TxnLog,
	options Options,
) (result *Machine, resultErr error) {
	if len(relationSpecs) == 0 {
		return nil, ErrInvalidCollection
	}
	prepared, err := prepareOpenInputs(
		binding, bootstrap, system,
		UserCollection{Name: relationSpecs[0].Name, Target: relationSpecs[0].Target},
		txnLog, options, true,
	)
	if err != nil {
		return nil, err
	}
	relations, manifest, err := prepareRelationCollections(binding, relationSpecs)
	if err != nil {
		return nil, err
	}
	contract, err := bundleApplyContractDigest(
		manifest, relations, options.MaxSessions, options.RetryWindow,
		options.RequestLedgerCapacityBytes, options.RequestLedgerCleanupReserveBytes,
		options.RequestLedgerRange,
		routeGateRecordLimit(),
	)
	if err != nil {
		return nil, err
	}
	prepared.relations = relations
	prepared.manifestDigest = manifest
	prepared.applyContract = contract
	prepared.members = make([]durable.NamedCollection, 1, len(relations)+1)
	prepared.members[0] = durable.NamedCollection{
		Name: systemCollectionName, Collection: system.Collection,
	}
	for i := range relations {
		prepared.members = append(prepared.members, durable.NamedCollection{
			Name: relations[i].name, Collection: relations[i].target.Collection,
		})
	}
	if target := options.TransitionCaptureTarget; target.Collection != nil || target.Name != "" {
		if !validReservedTransitionCaptureTarget(target, system, relations) {
			return nil, ErrTransitionCapture
		}
		prepared.members = append(prepared.members, durable.NamedCollection{
			Name: target.Name, Collection: target.Collection,
		})
	}
	if options.CheckpointGroup != nil && !options.CheckpointGroup.Owns(prepared.members) {
		return nil, fmt.Errorf("%w: checkpoint-group bundle ownership", ErrInvalidOptions)
	}
	if err := txnLog.ValidateCollections(prepared.members); err != nil {
		return nil, fmt.Errorf("%w: transaction-log bundle binding: %w", ErrInvalidCollection, err)
	}
	if err := validateBundleTransactionProfile(system, relations, options); err != nil {
		return nil, fmt.Errorf("%w: bundle transaction profile", err)
	}
	binding, system = prepared.binding, prepared.system
	userName := prepared.userName
	bootstrapDigest := prepared.bootstrapDigest
	m := newMachineFromOpenInputs(prepared)
	cut, err := durable.SnapshotCollections(prepared.members)
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
	imageGeneration := userSnapshot.Generation()
	if imageGeneration == 0 {
		return nil, fmt.Errorf("%w: missing user image generation", ErrInconsistentSnapshot)
	}
	var importedIncrementalImage [sha256.Size]byte
	for i := range m.relations {
		snapshot, exists := cut.Collection(m.relations[i].name)
		if !exists || snapshot == nil || snapshot.Generation() == 0 {
			return nil, fmt.Errorf("%w: missing relation snapshot", ErrInconsistentSnapshot)
		}
		if err := validateRelationIndexCatalog(snapshot, m.relations[i].localIndexes); err != nil {
			return nil, fmt.Errorf("relation %d index catalog: %w", m.relations[i].id, err)
		}
		var relationImage [sha256.Size]byte
		var placement relationPlacementAccumulator
		var relationErr error
		if len(m.relations) == 1 {
			relationImage, importedIncrementalImage, placement, relationErr =
				openedRelationImageDigests(&m.relations[i], snapshot, binding.OwnedRange)
		} else {
			relationImage, placement, relationErr = openedRelationImageDigest(
				&m.relations[i], snapshot, binding.OwnedRange,
			)
		}
		if relationErr != nil {
			return nil, fmt.Errorf("relation %d image: %w", m.relations[i].id, relationErr)
		}
		m.relations[i].openedImage = relationImage
		m.relations[i].openedGen = snapshot.Generation()
		m.relations[i].placement = placement
		m.relations[i].placementGen = snapshot.Generation()
	}
	imageDigest, err := canonicalRelationImageDigest(m.relations)
	if err != nil {
		return nil, err
	}
	seedDigest, err := dataChainSeedDigest(prepared.applyContract, imageDigest)
	if err != nil {
		return nil, fmt.Errorf("user collection %q: %w", userName, err)
	}
	systemSnapshot, ok := cut.Collection(systemCollectionName)
	if !ok || systemSnapshot == nil {
		return nil, fmt.Errorf("%w: missing system snapshot", ErrInconsistentSnapshot)
	}
	var openedTransactions scannedTransactions
	state, present, sessionCount, slotCount, authorityCount, openedRouteGate, err := scanSessionSystemSnapshot(
		systemSnapshot, options.MaxSessions, options.RetryWindow,
		options.RequestLedgerCapacityBytes, options.RequestLedgerCleanupReserveBytes,
		options.RequestLedgerRange, m.routeGateMaxRecords, &openedTransactions,
	)
	if err != nil {
		return nil, err
	}
	if !present {
		if m.checkpointGroup != nil && m.checkpointGroup.CheckpointAppliedIndex() != 0 {
			return nil, fmt.Errorf(
				"%w: empty system at checkpoint cut %d",
				ErrStateCorrupt, m.checkpointGroup.CheckpointAppliedIndex(),
			)
		}
		if options.TransitionCapture != nil {
			return nil, ErrTransitionCapture
		}
		relationRowsPresent := false
		for i := range m.relations {
			snapshot, _ := cut.Collection(m.relations[i].name)
			relationRowsPresent = relationRowsPresent || snapshot.Len() != 0
		}
		if sessionCount != 0 || slotCount != 0 || relationRowsPresent {
			return nil, fmt.Errorf("%w: uninitialized system with durable rows", ErrStateCorrupt)
		}
		m.state = State{
			Binding: binding, DataChainDigest: seedDigest,
			ApplyContractDigest: prepared.applyContract,
			ConfState:           new(pb.ConfState), BootstrapDigest: bootstrapDigest,
		}
		m.state.RelationPlacementDigest = relationPlacementStateDigest(
			binding.SchemaGeneration, m.manifestDigest, m.relations,
		)
		m.publication = raftmodel.Publication{DataChainDigest: seedDigest, ConfState: new(pb.ConfState)}
		m.openedImageDigest = imageDigest
		m.openedImageGeneration = imageGeneration
		for i := range m.relations {
			m.relations[i].openedApplied = 0
			m.relations[i].placementApplied = 0
		}
		m.routeGate = openedRouteGate
		return m, nil
	}
	if m.checkpointGroup != nil && m.checkpointGroup.SeedStateAuthoritative() {
		if seedApplied, seeded := m.checkpointGroup.SeedAppliedIndex(); seeded &&
			state.Applied == seedApplied {
			envelope, encodeErr := AppendState(nil, state)
			if encodeErr != nil {
				return nil, encodeErr
			}
			matched, seedErr := m.checkpointGroup.ValidateSeedState(
				state.Applied, systemCollectionName, envelope,
			)
			// The seed is the exact source state at its artifact cut. A live
			// source may most recently have applied a normal/session or
			// configuration entry. The required same-index InstallSnapshot
			// transition preserves that entry kind while binding the new
			// snapshot-base digest.
			if seedErr != nil || !matched {
				return nil, errors.Join(
					fmt.Errorf("%w: imported state seed commitment", ErrStateCorrupt),
					seedErr,
				)
			}
		}
	}
	if m.checkpointGroup != nil &&
		(state.Applied != m.checkpointGroup.CheckpointAppliedIndex() ||
			state.Applied != m.checkpointGroup.AppliedIndex()) {
		return nil, fmt.Errorf(
			"%w: state applied %d disagrees with checkpoint cut %d",
			ErrStateCorrupt, state.Applied,
			m.checkpointGroup.CheckpointAppliedIndex(),
		)
	}
	if !bindingMatchesFenceOrigin(binding, state) || state.BootstrapDigest != bootstrapDigest ||
		state.ApplyContractDigest != prepared.applyContract ||
		state.SessionCount != sessionCount || state.SessionSlotCount != slotCount ||
		state.AuthorityBindingCount != authorityCount ||
		state.RelationPlacementDigest != relationPlacementStateDigest(
			state.Binding.SchemaGeneration, m.manifestDigest, m.relations,
		) ||
		state.SessionCount > options.MaxSessions {
		return nil, fmt.Errorf("%w: persisted publication disagrees with construction", ErrStateCorrupt)
	}
	if state.LastKind == RecordSchema {
		if err := validateOpenedSchemaTransition(
			state, binding, prepared.manifestDigest, prepared.applyContract, options,
		); err != nil {
			return nil, err
		}
	}
	if state.LastKind == RecordStaticSnapshot && state.DataChainDigest != seedDigest {
		return nil, fmt.Errorf("%w: persisted base data-chain seed", ErrStateCorrupt)
	}
	if state.LastKind == RecordImportedSnapshot && state.DataChainDigest != seedDigest {
		incrementalSeed, incrementalErr := dataChainSeedDigest(
			prepared.applyContract, importedIncrementalImage,
		)
		if incrementalErr != nil || state.DataChainDigest != incrementalSeed {
			return nil, fmt.Errorf("%w: persisted imported data-chain seed", ErrStateCorrupt)
		}
		imageDigest = importedIncrementalImage
		m.relations[0].openedImage = importedIncrementalImage
	}
	if state.LastKind == RecordStaticSnapshot &&
		(state.LastEntryDigest != bootstrapDigest ||
			!proto.Equal(state.ConfState, bootstrap.GetMetadata().GetConfState())) {
		return nil, fmt.Errorf("%w: static publication differs from bootstrap", ErrStateCorrupt)
	}
	m.state = state
	m.openedImageDigest = imageDigest
	m.openedImageApplied = state.Applied
	m.openedImageGeneration = imageGeneration
	for i := range m.relations {
		m.relations[i].openedApplied = state.Applied
		m.relations[i].placementApplied = state.Applied
	}
	m.binding = state.Binding
	m.distribution = []byte(state.Binding.Distribution)
	m.shard = []byte(state.Binding.Shard)
	m.initialized = true
	m.publication = publicationFromState(state)
	m.transactionIntents = openedTransactions.intents
	m.transactionIntentKeys = openedTransactions.intentKeys
	m.splitCaptureActivation = openedTransactions.activation
	m.routeGate = openedRouteGate
	if openedTransactions.activation != nil {
		if options.TransitionCaptureFactory == nil || options.TransitionCapture != nil {
			return nil, ErrSplitCaptureActivation
		}
		capture, captureErr := options.TransitionCaptureFactory(*openedTransactions.activation)
		if captureErr != nil || capture == nil {
			return nil, errors.Join(captureErr, ErrSplitCaptureActivation)
		}
		if err := m.beginTransitionCapture(capture); err != nil {
			return nil, err
		}
	} else if options.TransitionCapture != nil {
		if err := m.beginTransitionCapture(options.TransitionCapture); err != nil {
			return nil, err
		}
	}
	return m, nil
}

func validateOpenedSchemaTransition(
	state State,
	binding Binding,
	manifest, contract [sha256.Size]byte,
	options Options,
) error {
	transition, err := OpenSchemaTransition(options.SchemaTransition)
	witness := options.SchemaMembershipWitness
	meta := raftmodel.ApplyMeta{
		Index: state.Applied, Term: state.LastTerm, Type: state.LastEntryType,
	}
	if err != nil || normalEntryDigest(meta, options.SchemaTransition) != state.LastEntryDigest ||
		schemaTransitionBinding(transition) != binding ||
		transition.ToManifest != manifest || transition.ToApplyContract != contract ||
		transition.ToPlacementDigest != state.RelationPlacementDigest ||
		transition.MembershipSequence != witness.Sequence ||
		transition.MembershipSource != witness.Source ||
		transition.MembershipTarget != witness.Target ||
		transition.AuthorizationDigest != options.SchemaAuthorizationDigest ||
		transition.CatalogCASDigest != options.SchemaCatalogCASDigest {
		return errors.Join(ErrSchemaTransition, err)
	}
	return nil
}

func newMachineFromOpenInputs(prepared openInputs) *Machine {
	binding := prepared.binding
	routeGateMax := routeGateRecordLimit()
	gate, _ := routegate.NewMachine(1, routeGateMax)
	return &Machine{
		binding: binding, bootstrap: prepared.bootstrap,
		bootstrapDigest: prepared.bootstrapDigest, system: prepared.system,
		userName: prepared.userName, user: prepared.user,
		relations: prepared.relations, manifestDigest: prepared.manifestDigest,
		members:      prepared.members,
		distribution: []byte(binding.Distribution), shard: []byte(binding.Shard),
		applyContract: prepared.applyContract,
		dataChainHash: newDataChainHasher(),
		txnLog:        prepared.txnLog, checkpointGroup: prepared.checkpointGroup,
		options:   prepared.options,
		routeGate: gate, routeGateMaxRecords: routeGateMax,
		reservedCaptureTarget: prepared.options.TransitionCaptureTarget,
		transitionMembers:     make([]durable.NamedCollection, 0, len(prepared.relations)+2),
	}
}

func routeGateRecordLimit() uint64 {
	// Every command mutates at most one pin row, so physical transaction
	// geometry does not constrain concurrent participants. The only retained
	// bound is the exact 64 MiB canonical gate-image ceiling.
	return routegate.MaxRetainedRecords
}

func prepareOpenInputs(
	binding Binding,
	bootstrap *pb.Snapshot,
	system CollectionTarget,
	userSpec UserCollection,
	txnLog *durable.TxnLog,
	options Options,
	deferBundleMembership bool,
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
	if options.CheckpointGroup != nil && !deferBundleMembership &&
		options.TransitionCapture != nil {
		return openInputs{}, fmt.Errorf(
			"%w: checkpoint-group capture startup", ErrInvalidOptions,
		)
	}
	requiredSystem, limitsOK := RequiredSystemCollectionLimits(
		options.RetryWindow, options.RequestLedgerRange.enabled(),
	)
	if !limitsOK || system.Limits.MaxKeyBytes < requiredSystem.MaxKeyBytes ||
		system.Limits.MaxDocumentBytes < requiredSystem.MaxDocumentBytes ||
		system.Limits.MaxDistinctMutations < requiredSystem.MaxDistinctMutations ||
		system.Limits.MaxBatchBytes < requiredSystem.MaxBatchBytes {
		return openInputs{}, fmt.Errorf(
			"%w: system collection cannot hold bounded records", ErrInvalidCollection,
		)
	}
	maxUserBatchBytes := min(user.Limits.MaxBatchBytes, replication.MaxCommandBytes)
	requiredTxnBytes, ok := checkedTxnBytes(maxUserBatchBytes, requiredSystem.MaxBatchBytes)
	if !ok {
		return openInputs{}, fmt.Errorf(
			"%w: transaction byte proof overflows", ErrInvalidOptions,
		)
	}
	if options.TxnLimits.MaxDocuments < max(
		user.Limits.MaxDistinctMutations+4,
		requiredSystem.MaxDistinctMutations,
	) ||
		options.TxnLimits.MaxBytes < requiredTxnBytes {
		return openInputs{}, fmt.Errorf(
			"%w: transaction limits do not cover the frozen apply profile", ErrInvalidOptions,
		)
	}
	bootstrapBytes, bootstrapDigest, err := validateBootstrap(bootstrap)
	if err != nil {
		return openInputs{}, err
	}
	binding.Distribution = strings.Clone(binding.Distribution)
	binding.Shard = strings.Clone(binding.Shard)
	prepared := openInputs{
		binding: binding, bootstrap: bootstrapBytes, bootstrapDigest: bootstrapDigest,
		system: system, userName: strings.Clone(userName), user: user,
		txnLog: txnLog, checkpointGroup: options.CheckpointGroup, options: options,
	}
	if !deferBundleMembership {
		relations, manifest, relationErr := prepareRelationCollections(binding, []RelationCollection{{
			Relation: 1, Kind: RelationJSON, Name: userName, Target: user,
			LocalIndexes: userSpec.LocalIndexes,
		}})
		if relationErr != nil {
			return openInputs{}, relationErr
		}
		prepared.relations = relations
		prepared.manifestDigest = manifest
		prepared.applyContract, relationErr = bundleApplyContractDigest(
			manifest, relations, options.MaxSessions, options.RetryWindow,
			options.RequestLedgerCapacityBytes, options.RequestLedgerCleanupReserveBytes,
			options.RequestLedgerRange,
			routeGateRecordLimit(),
		)
		if relationErr != nil {
			return openInputs{}, relationErr
		}
		prepared.members = []durable.NamedCollection{
			{Name: systemCollectionName, Collection: system.Collection},
			{Name: userName, Collection: user.Collection},
		}
		if target := options.TransitionCaptureTarget; target.Collection != nil || target.Name != "" {
			if !validReservedTransitionCaptureTarget(target, system, relations) {
				return openInputs{}, ErrTransitionCapture
			}
			prepared.members = append(prepared.members, durable.NamedCollection{
				Name: target.Name, Collection: target.Collection,
			})
		}
		if options.CheckpointGroup != nil && !options.CheckpointGroup.Owns(prepared.members) {
			return openInputs{}, fmt.Errorf(
				"%w: checkpoint-group ownership", ErrInvalidOptions,
			)
		}
		if err := txnLog.ValidateCollections(prepared.members); err != nil {
			return openInputs{}, fmt.Errorf(
				"%w: transaction-log binding: %w", ErrInvalidCollection, err,
			)
		}
		if err := validateBundleTransactionProfile(system, relations, options); err != nil {
			return openInputs{}, fmt.Errorf("%w: transaction profile", err)
		}
	}
	return prepared, nil
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
		!ownershipRangeContains(initial.OwnedRange, current.OwnedRange) ||
		current.OwnershipEpoch < initial.OwnershipEpoch ||
		current.RoutingVersion < initial.RoutingVersion ||
		current.RouteGeneration < initial.RouteGeneration {
		return false
	}
	delta := current.OwnershipEpoch - initial.OwnershipEpoch
	return (delta != 0 || current.OwnedRange == initial.OwnedRange) &&
		current.RoutingVersion-initial.RoutingVersion == delta &&
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
	leaseDeadline       int64
	physicalSlots       uint16
	retryWindow         uint16
	status              SessionStatus
	authorityClass      replication.CommandAuthorityClass
	tenantOffset        uint32
	tenantBytes         uint8
	clientID            replication.ID128
	seenSlots           uint16
	currentSlots        uint16
	latestSeen          bool
	latestResult        uint32
	orderedLastApplied  uint64
	wrappedFirstApplied uint64
	wrappedLastApplied  uint64
}

type scannedTransactionKey struct {
	role distributedtxn.ReplicatedRole
	id   distributedtxn.ID
}

type transactionIntentIdentity struct {
	relation replication.RelationID
	digest   [sha256.Size]byte
}

type reopenedTransactionIntentOwner struct {
	id        distributedtxn.ID
	keyOffset uint32
	keyBytes  uint32
}

type scannedMutationKey struct {
	keyOffset  uint32
	keyBytes   uint32
	intentSeen bool
}

type scannedTransactionControl struct {
	control TransactionControl

	residentControl  uint64
	residentPayload  uint64
	residentManifest uint64
	residentMutation uint64
	residentIntent   uint64
	payloadRows      uint64
	intentRows       uint64
	payloadSeen      bool

	manifestDescriptor      distributedtxn.ManifestDescriptor
	manifestDescriptorSeen  bool
	manifestNextPage        uint32
	manifestNextParticipant uint64
	manifestEncodedBytes    uint64
	manifestChain           distributedtxn.Digest

	mutationRows uint64
	mutationKeys map[transactionIntentIdentity]scannedMutationKey
}

type scannedTransactions struct {
	intents    map[transactionIntentIdentity]reopenedTransactionIntentOwner
	intentKeys []byte
	activation *SplitCaptureActivation
}

type scannedExecutionPin struct {
	record     executionpin.Record
	digest     [sha256.Size]byte
	activeSeen bool
}

// scanSessionSystemSnapshot performs one ordered, bounded pass over the hidden
// image. It validates state, collision-verifiable session headers, and every
// compact physical ring slot before returning counts.
// Scratch space is proportional to session identities, never operation count.
func scanSessionSystemSnapshot(
	snapshot *durable.Snapshot,
	maxSessions uint64,
	retryWindow uint16,
	requestLedgerCapacity uint64,
	requestLedgerCleanup uint64,
	requestLedgerRange RequestLedgerRange,
	routeGateMaxRecords uint64,
	transactionResult ...*scannedTransactions,
) (State, bool, uint64, uint64, uint64, *routegate.Machine, error) {
	var state State
	var statePresent bool
	var sessionCount, slotCount, authorityCount uint64
	var sessions map[[sha256.Size]byte]scannedSession
	var authorities map[[sha256.Size]byte]replication.CommandAuthorityClass
	var knownSessionDigests map[[sha256.Size]byte]struct{}
	var routeGateStatus routegate.Status
	var routeGateHeadPresent bool
	var routeGateRecords []routegate.PinRecord
	var routeGateResultCount uint64
	var activeIdentities map[[sha256.Size]byte][sha256.Size]byte
	var tenantArena []byte
	var sessionEpochs []uint64
	var transactionControls map[scannedTransactionKey]*scannedTransactionControl
	var transactionControlCount, activeTransactionCount uint64
	var transactionPayloadRows, transactionIntentRows, transactionResidentBytes uint64
	var activeTransactionIntents map[transactionIntentIdentity]reopenedTransactionIntentOwner
	var transactionIntentKeys []byte
	var splitActivation *SplitCaptureActivation
	var historicalFences map[[18]byte]sessionFence
	var historicalSeen map[[18]byte]uint64
	var unfencedSlots uint64
	var transactionScopeScratch []distributedtxn.IntentScope
	var manifestParticipantScratch []distributedtxn.ParticipantRef
	var manifestIdentityScratch []byte
	var mutationScanID distributedtxn.ID
	var mutationScanControl *scannedTransactionControl
	var mutationScanHash hash.Hash
	var mutationScanRelations uint16
	var mutationScanMutations uint64
	var mutationScanLastRelation replication.RelationID
	ledgerScan := newRequestLedgerImageScanner(
		requestLedgerCapacity, requestLedgerCleanup, requestLedgerRange,
	)
	var executionPinRecordCount, activeExecutionPinCount, executionPinResidentBytes uint64
	var activeExecutionPins map[executionpin.PinID]*scannedExecutionPin
	finishMutationScan := func() error {
		if mutationScanControl == nil {
			return nil
		}
		control := mutationScanControl.control
		if mutationScanRelations != control.PayloadRelationCount ||
			mutationScanMutations != control.PayloadCount {
			return fmt.Errorf("%w: packed relation row counts", ErrTransactionStateCorrupt)
		}
		var canonicalDigest [sha256.Size]byte
		_ = mutationScanHash.Sum(canonicalDigest[:0])
		var framing [8]byte
		binary.LittleEndian.PutUint16(framing[0:2], mutationScanRelations)
		if mutationScanRelations == 1 {
			binary.LittleEndian.PutUint16(framing[2:4], uint16(mutationScanLastRelation))
		}
		binary.LittleEndian.PutUint32(framing[4:8], uint32(mutationScanMutations))
		var material [len(scannedTransactionMutationDigestDomain) + 8 + sha256.Size]byte
		cursor := copy(material[:], scannedTransactionMutationDigestDomain[:])
		cursor += copy(material[cursor:], framing[:])
		copy(material[cursor:], canonicalDigest[:])
		if distributedtxn.Digest(sha256.Sum256(material[:])) != control.MutationDigest {
			return fmt.Errorf("%w: packed relation mutation digest", ErrTransactionStateCorrupt)
		}
		return nil
	}
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
			if state.SessionCount > maxSessions || state.AuthorityBindingCount > maxSessions ||
				retryWindow == 0 ||
				state.SessionSlotCount > state.SessionCount*uint64(retryWindow) {
				return fmt.Errorf("%w: bounded session counts", ErrStateCorrupt)
			}
			statePresent = true
			historicalFences = make(map[[18]byte]sessionFence, int(state.HistoricalFenceCount))
			historicalSeen = make(map[[18]byte]uint64, int(state.HistoricalFenceCount))
			if state.TransactionControlCount > MaxRetainedTransactions {
				return fmt.Errorf("%w: bounded transaction count", ErrStateCorrupt)
			}
			sessions = make(map[[sha256.Size]byte]scannedSession, int(state.SessionCount))
			authorities = make(map[[sha256.Size]byte]replication.CommandAuthorityClass,
				int(state.AuthorityBindingCount))
			knownSessionDigests = make(map[[sha256.Size]byte]struct{},
				int(state.AuthorityBindingCount))
			activeIdentities = make(map[[sha256.Size]byte][sha256.Size]byte, int(state.SessionCount))
			if state.SessionCount > math.MaxInt32/16 {
				return fmt.Errorf("%w: tenant arena bound", ErrSessionCorrupt)
			}
			initialTenantBytes := state.SessionCount * 16
			tenantArena = make([]byte, 0, int(initialTenantBytes))
			sessionEpochs = make([]uint64, 0, int(state.SessionCount))
			transactionControls = make(map[scannedTransactionKey]*scannedTransactionControl,
				int(state.ActiveTransactionCount))
			activeTransactionIntents = make(map[transactionIntentIdentity]reopenedTransactionIntentOwner,
				int(state.TransactionIntentRows))
			activeExecutionPins = make(map[executionpin.PinID]*scannedExecutionPin,
				int(state.ActiveExecutionPinCount))
			if state.TransactionResidentBytes > math.MaxInt32 {
				return fmt.Errorf("%w: transaction resident reopen arena", ErrTransactionStateCorrupt)
			}
			transactionIntentKeys = make([]byte, 0, min(int(state.TransactionResidentBytes), 1<<20))
			return nil

		case len(key) == 18 && bytes.Equal(key[:2], sessionFencePrefix[:]):
			if !statePresent || uint64(len(historicalFences)) >= state.HistoricalFenceCount {
				return ErrSessionCorrupt
			}
			f, err := openSessionFence(value)
			if err != nil || !validHistoricalSessionFence(state, f) {
				return ErrSessionCorrupt
			}
			want := sessionFenceKey(f.routing, f.generation)
			if !bytes.Equal(key, want[:]) {
				return ErrSessionCorrupt
			}
			historicalFences[want] = f
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
				view.ClientEpoch > state.SessionEpochHighWater ||
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
			stableDigest := AuthorityIdentityKey(view.Tenant, view.ClientID)
			if _, duplicate := activeIdentities[stableDigest]; duplicate ||
				len(tenantArena) > math.MaxInt32-len(view.Tenant) {
				return fmt.Errorf("%w: duplicate or excessive stable identity", ErrSessionCorrupt)
			}
			offset := len(tenantArena)
			tenantArena = append(tenantArena, view.Tenant...)
			sessions[view.Digest] = scannedSession{
				epoch: view.ClientEpoch, authorityClass: view.AuthorityClass,
				tenantOffset: uint32(offset), tenantBytes: uint8(len(view.Tenant)),
				clientID:     view.ClientID,
				highSequence: view.HighSequence, leaseDeadline: view.LeaseDeadlineUnixNano,
				physicalSlots: view.PhysicalSlotCount,
				retryWindow:   view.RetryWindow, status: view.Status,
			}
			activeIdentities[stableDigest] = view.Digest
			sessionEpochs = append(sessionEpochs, view.ClientEpoch)
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
			if !ok || view.Slot >= summary.physicalSlots {
				return fmt.Errorf("%w: slot outside session header", ErrSessionCorrupt)
			}
			// RangeRaw emits each live KV key once in bytewise order. The exact
			// key/value round trip above therefore makes every increment a
			// distinct slot ordinal. Since each ordinal is also below
			// physicalSlots, the exact count checked after the scan proves the
			// complete [0, physicalSlots) set without a per-session bitmap.
			summary.seenSlots++
			if err := validateStoredSessionSlot(state, view, sessionFenceLookup{fences: historicalFences}); err != nil {
				return err
			}
			if view.ResultCode == ResultStaleFence {
				unfencedSlots++
			} else if view.RoutingVersion != state.Binding.RoutingVersion || view.RouteGeneration != state.Binding.RouteGeneration {
				historicalSeen[sessionFenceKey(view.RoutingVersion, view.RouteGeneration)]++
			}
			if err := validateSessionSlotAgainstHeader(SessionView{
				Digest: view.SessionDigest, ClientEpoch: summary.epoch,
				AuthorityClass: summary.authorityClass,
				HighSequence:   summary.highSequence, RetryWindow: summary.retryWindow,
				LeaseDeadlineUnixNano: summary.leaseDeadline,
				PhysicalSlotCount:     summary.physicalSlots, Status: summary.status,
			}, view); err != nil {
				return err
			}
			order := sessionAppliedOrder{
				orderedLast:  summary.orderedLastApplied,
				wrappedFirst: summary.wrappedFirstApplied,
				wrappedLast:  summary.wrappedLastApplied,
			}
			if err := order.observe(summary.highSequence, summary.retryWindow, view); err != nil {
				return err
			}
			summary.orderedLastApplied = order.orderedLast
			summary.wrappedFirstApplied = order.wrappedFirst
			summary.wrappedLastApplied = order.wrappedLast
			summary.currentSlots++
			if view.ClientSequence == summary.highSequence {
				summary.latestSeen = true
				summary.latestResult = view.ResultCode
			}
			sessions[view.SessionDigest] = summary
			slotCount++
			return nil

		case len(key) == 1+sha256.Size && key[0] == 3:
			if !statePresent {
				return fmt.Errorf("%w: authority binding before state", ErrSessionCorrupt)
			}
			view, err := OpenAuthorityBinding(value)
			want := AuthorityBindingStorageKey(view.Digest)
			if err != nil || !bytes.Equal(key, want[:]) {
				return errors.Join(err, fmt.Errorf("%w: authority binding key", ErrSessionCorrupt))
			}
			if _, duplicate := authorities[view.Digest]; duplicate || authorityCount >= maxSessions {
				return fmt.Errorf("%w: duplicate or excessive authority binding", ErrSessionCorrupt)
			}
			if sessionDigest, active := activeIdentities[view.Digest]; active {
				summary := sessions[sessionDigest]
				start, end := int(summary.tenantOffset), int(summary.tenantOffset)+int(summary.tenantBytes)
				if end > len(tenantArena) || summary.clientID != view.ClientID ||
					!bytes.Equal(tenantArena[start:end], view.Tenant) {
					return fmt.Errorf("%w: authority identity collision", ErrSessionCorrupt)
				}
			}
			authorities[view.Digest] = view.AuthorityClass
			knownSessionDigests[SessionKey(view.AuthorityClass, view.Tenant, view.ClientID)] = struct{}{}
			authorityCount++
			return nil

		case bytes.Equal(key, routeGateHeadKey):
			if !statePresent || routeGateHeadPresent || routeGateMaxRecords == 0 {
				return fmt.Errorf("%w: route-gate head", ErrStateCorrupt)
			}
			var headErr error
			routeGateStatus, headErr = routegate.OpenHead(value)
			if headErr != nil || routeGateStatus.RetainedRecords > routeGateMaxRecords ||
				routeGateStatus.RetainedRecords > math.MaxInt32 {
				return errors.Join(headErr, fmt.Errorf("%w: route-gate head", ErrStateCorrupt))
			}
			routeGateRecords = make([]routegate.PinRecord, 0, int(routeGateStatus.RetainedRecords))
			routeGateHeadPresent = true
			return nil

		case len(key) == routeGatePinKeyBytes && key[0] == routeGatePinPrefix:
			if !statePresent || !routeGateHeadPresent ||
				uint64(len(routeGateRecords)) >= routeGateStatus.RetainedRecords {
				return fmt.Errorf("%w: route-gate pin", ErrStateCorrupt)
			}
			var identity routegate.Identity
			copy(identity[:], key[1:])
			record, pinErr := routegate.OpenStoredPin(identity, value)
			want, keyErr := routeGatePinStorageKey(identity)
			if pinErr != nil || keyErr != nil || !bytes.Equal(key, want[:]) {
				return errors.Join(pinErr, keyErr, fmt.Errorf("%w: route-gate pin", ErrStateCorrupt))
			}
			routeGateRecords = append(routeGateRecords, record)
			return nil

		case len(key) == routeGateResultKeyBytes && key[0] == routeGateResultPrefix:
			if !statePresent {
				return fmt.Errorf("%w: route-gate result", ErrStateCorrupt)
			}
			record, resultErr := openRouteGateResult(value)
			want, keyErr := routeGateResultStorageKey(record.SessionDigest, record.Slot)
			if resultErr != nil || keyErr != nil || !bytes.Equal(key, want[:]) {
				return errors.Join(resultErr, keyErr, fmt.Errorf("%w: route-gate result", ErrStateCorrupt))
			}
			if _, known := knownSessionDigests[record.SessionDigest]; !known {
				return fmt.Errorf("%w: unbound route-gate result", ErrStateCorrupt)
			}
			routeGateResultCount++
			return nil

		case len(key) == transactionControlStorageKeyBytes && key[0] == transactionControlPrefix:
			if !statePresent {
				return fmt.Errorf("%w: transaction control before state", ErrTransactionStateCorrupt)
			}
			if transactionScopeScratch == nil {
				transactionScopeScratch = make([]distributedtxn.IntentScope, distributedtxn.MaxIntentScopes)
			}
			view, openErr := OpenTransactionControlInto(value, transactionScopeScratch)
			want, keyErr := view.StorageKey()
			if openErr != nil || keyErr != nil || !bytes.Equal(key, want[:]) ||
				view.LastAppliedIndex > state.Applied {
				return errors.Join(openErr, keyErr, ErrTransactionStateCorrupt)
			}
			if transactionControlCount >= state.TransactionControlCount {
				return fmt.Errorf("%w: excessive transaction control", ErrTransactionStateCorrupt)
			}
			control := view.TransactionControl
			control.IntentScopes = nil
			active := transactionControlActive(control)
			transactionControlCount++
			if active {
				identity := scannedTransactionKey{role: view.Role, id: view.ID}
				if _, duplicate := transactionControls[identity]; duplicate {
					return fmt.Errorf("%w: duplicate active transaction control", ErrTransactionStateCorrupt)
				}
				transactionControls[identity] = &scannedTransactionControl{
					control:         control,
					residentControl: uint64(len(key) + len(value)),
				}
				activeTransactionCount++
			} else if uint64(len(key)+len(value)) != control.ResidentControlBytes {
				return fmt.Errorf("%w: terminal transaction control bytes", ErrTransactionStateCorrupt)
			}
			return addTransactionResidentBytes(&transactionResidentBytes, len(key), len(value))

		case len(key) == transactionPayloadStorageKeyBytes && key[0] == transactionPayloadPrefix:
			if !statePresent {
				return ErrTransactionStateCorrupt
			}
			view, openErr := OpenTransactionCoordinatorPayload(value)
			want, keyErr := view.StorageKey()
			summary := transactionControls[scannedTransactionKey{
				role: distributedtxn.ReplicatedRoleCoordinator, id: view.ID,
			}]
			if openErr != nil || keyErr != nil || !bytes.Equal(key, want[:]) || summary == nil ||
				summary.payloadSeen || view.Kind != summary.control.PayloadKind ||
				view.Digest != summary.control.PayloadDigest {
				return errors.Join(openErr, keyErr, ErrTransactionStateCorrupt)
			}
			summary.payloadSeen = true
			summary.payloadRows++
			summary.residentPayload += uint64(len(key) + len(value))
			if view.Kind == distributedtxn.ReplicatedPayloadCoordinator {
				var participantScratch [distributedtxn.MaxInlineParticipants]distributedtxn.ParticipantRef
				record, recordErr := distributedtxn.OpenCoordinatorInto(view.Payload, participantScratch[:])
				if recordErr != nil || uint64(len(view.Payload)) != summary.control.PayloadBytes ||
					uint64(len(record.Participants)) != summary.control.PayloadCount {
					return errors.Join(recordErr, ErrTransactionStateCorrupt)
				}
			} else {
				record, recordErr := distributedtxn.OpenManifestCoordinator(view.Payload)
				if recordErr != nil || record.Manifest.EncodedBytes != summary.control.PayloadBytes ||
					record.Manifest.ParticipantCount != summary.control.PayloadCount {
					return errors.Join(recordErr, ErrTransactionStateCorrupt)
				}
				summary.manifestDescriptor = record.Manifest
				summary.manifestDescriptorSeen = true
			}
			transactionPayloadRows++
			if residentErr := addTransactionResidentBytes(&transactionResidentBytes, len(key), len(value)); residentErr != nil {
				return residentErr
			}
			return nil

		case len(key) == transactionManifestKeyBytes && key[0] == transactionManifestPrefix:
			if !statePresent {
				return ErrTransactionStateCorrupt
			}
			if manifestParticipantScratch == nil {
				manifestParticipantScratch = make([]distributedtxn.ParticipantRef,
					distributedtxn.MaxManifestPageParticipants)
				manifestIdentityScratch = make([]byte,
					distributedtxn.MaxManifestPageParticipants*distributedtxn.MaxShardIdentityBytes*2)
			}
			view, openErr := OpenTransactionManifestPageInto(
				value, manifestParticipantScratch, manifestIdentityScratch,
			)
			want, keyErr := view.StorageKey()
			summary := transactionControls[scannedTransactionKey{
				role: distributedtxn.ReplicatedRoleCoordinator, id: view.ID,
			}]
			if openErr != nil || keyErr != nil || !bytes.Equal(key, want[:]) || summary == nil ||
				summary.control.PayloadKind != distributedtxn.ReplicatedPayloadManifestCoordinator ||
				view.Index != summary.manifestNextPage ||
				view.FirstParticipant != summary.manifestNextParticipant {
				return errors.Join(openErr, keyErr, ErrTransactionStateCorrupt)
			}
			summary.manifestNextPage++
			summary.manifestNextParticipant += uint64(view.ParticipantCount)
			if summary.manifestEncodedBytes > math.MaxUint64-uint64(len(view.Raw)) {
				return ErrTransactionStateCorrupt
			}
			summary.manifestEncodedBytes += uint64(len(view.Raw))
			summary.manifestChain = advanceTransactionManifestChain(
				summary.manifestChain, view.Index, view.Digest,
			)
			summary.payloadRows++
			summary.residentManifest += uint64(len(key) + len(value))
			transactionPayloadRows++
			return addTransactionResidentBytes(&transactionResidentBytes, len(key), len(value))

		case len(key) == transactionRelationPayloadKeyBytes && key[0] == transactionMutationPrefix:
			if !statePresent {
				return ErrTransactionStateCorrupt
			}
			view, openErr := OpenTransactionRelationPayload(value)
			want, keyErr := view.StorageKey()
			summary := transactionControls[scannedTransactionKey{
				role: distributedtxn.ReplicatedRoleParticipant, id: view.ID,
			}]
			if openErr != nil || keyErr != nil || !bytes.Equal(key, want[:]) || summary == nil {
				return errors.Join(openErr, keyErr, ErrTransactionStateCorrupt)
			}
			if mutationScanControl == nil || mutationScanID != view.ID {
				if finishErr := finishMutationScan(); finishErr != nil {
					return finishErr
				}
				mutationScanID = view.ID
				mutationScanControl = summary
				if mutationScanHash == nil {
					mutationScanHash = sha256.New()
				} else {
					mutationScanHash.Reset()
				}
				mutationScanRelations = 0
				mutationScanMutations = 0
				mutationScanLastRelation = 0
			}
			if summary != mutationScanControl || view.Relation <= mutationScanLastRelation ||
				mutationScanRelations == math.MaxUint16 ||
				mutationScanMutations > math.MaxUint32-uint64(view.Count) {
				return ErrTransactionStateCorrupt
			}
			if summary.control.PayloadRelationCount > 1 {
				var header [8]byte
				binary.LittleEndian.PutUint16(header[0:2], uint16(view.Relation))
				binary.LittleEndian.PutUint16(header[2:4], uint16(view.Count))
				binary.LittleEndian.PutUint32(header[4:8], uint32(len(view.MutationBytes())))
				_, _ = mutationScanHash.Write(header[:])
			}
			_, _ = mutationScanHash.Write(view.MutationBytes())
			mutationScanRelations++
			mutationScanMutations += uint64(view.Count)
			mutationScanLastRelation = view.Relation
			if summary.mutationKeys == nil {
				summary.mutationKeys = make(map[transactionIntentIdentity]scannedMutationKey)
			}
			mutations := view.Batch.Mutations()
			for mutations.Next() {
				mutation := mutations.Mutation()
				mutationIdentity := transactionIntentIdentity{
					relation: view.Relation, digest: sha256.Sum256(mutation.Key),
				}
				if prior, exists := summary.mutationKeys[mutationIdentity]; exists {
					start, end := uint64(prior.keyOffset), uint64(prior.keyOffset)+uint64(prior.keyBytes)
					if end > uint64(len(transactionIntentKeys)) ||
						!bytes.Equal(transactionIntentKeys[start:end], mutation.Key) {
						return fmt.Errorf("%w: mutation key hash collision", ErrTransactionStateCorrupt)
					}
					continue
				}
				if uint64(len(transactionIntentKeys))+uint64(len(mutation.Key)) > math.MaxUint32 {
					return fmt.Errorf("%w: transaction intent key arena", ErrTransactionStateCorrupt)
				}
				offset := len(transactionIntentKeys)
				transactionIntentKeys = append(transactionIntentKeys, mutation.Key...)
				summary.mutationKeys[mutationIdentity] = scannedMutationKey{
					keyOffset: uint32(offset), keyBytes: uint32(len(mutation.Key)),
				}
			}
			summary.mutationRows += uint64(view.Count)
			summary.payloadRows++
			summary.residentMutation += uint64(len(key) + len(value))
			transactionPayloadRows++
			return addTransactionResidentBytes(&transactionResidentBytes, len(key), len(value))

		case len(key) == transactionIntentKeyBytes && key[0] == transactionIntentPrefix:
			if !statePresent {
				return ErrTransactionStateCorrupt
			}
			view, openErr := OpenTransactionIntent(value)
			want, keyErr := view.StorageKey()
			summary := transactionControls[scannedTransactionKey{
				role: distributedtxn.ReplicatedRoleParticipant, id: view.ID,
			}]
			identity := transactionIntentIdentity{relation: view.Relation, digest: view.KeyHash}
			mutationKey, mutationFound := scannedMutationKey{}, false
			if summary != nil {
				mutationKey, mutationFound = summary.mutationKeys[identity]
			}
			mutationKeyStart := uint64(mutationKey.keyOffset)
			mutationKeyEnd := mutationKeyStart + uint64(mutationKey.keyBytes)
			if openErr != nil || keyErr != nil || !bytes.Equal(key, want[:]) || summary == nil ||
				!mutationFound || mutationKey.intentSeen ||
				mutationKeyEnd > uint64(len(transactionIntentKeys)) ||
				!bytes.Equal(transactionIntentKeys[mutationKeyStart:mutationKeyEnd], view.RawKey) {
				return errors.Join(openErr, keyErr, ErrTransactionStateCorrupt)
			}
			if _, collision := activeTransactionIntents[identity]; collision {
				return fmt.Errorf("%w: duplicate intent ownership", ErrTransactionStateCorrupt)
			}
			activeTransactionIntents[identity] = reopenedTransactionIntentOwner{
				id: view.ID, keyOffset: mutationKey.keyOffset, keyBytes: mutationKey.keyBytes,
			}
			mutationKey.intentSeen = true
			summary.mutationKeys[identity] = mutationKey
			summary.intentRows++
			summary.residentIntent += uint64(len(key) + len(value))
			transactionIntentRows++
			return addTransactionResidentBytes(&transactionResidentBytes, len(key), len(value))

		case len(key) >= requestledger.FixedStorageKeyBytes && key[0] == requestledger.StoragePrefix:
			if !statePresent {
				return fmt.Errorf("%w: request ledger row before state", ErrStateCorrupt)
			}
			return ledgerScan.observe(key, value)

		case len(key) == requestledger.PlanningExpiryKeyBytes && key[0] == requestledger.PlanningExpiryStoragePrefix:
			if !statePresent {
				return fmt.Errorf("%w: request ledger planning expiry before state", ErrStateCorrupt)
			}
			return ledgerScan.observePlanningExpiry(key, value)

		case len(key) == requestledger.IssuerHighwaterKeyBytes && key[0] == requestledger.IssuerHighwaterStoragePrefix:
			if !statePresent {
				return fmt.Errorf("%w: request ledger issuer high-water before state", ErrStateCorrupt)
			}
			return ledgerScan.observeIssuerHighwater(key, value)

		case len(key) == requestledger.IssuerSequenceKeyBytes && key[0] == requestledger.IssuerSequenceStoragePrefix:
			if !statePresent {
				return fmt.Errorf("%w: request ledger issuer sequence before state", ErrStateCorrupt)
			}
			return ledgerScan.observeIssuerSequence(key, value)

		case len(key) == executionPinRecordStorageKeyBytes && key[0] == executionPinRecordPrefix:
			if !statePresent {
				return ErrExecutionPinStateCorrupt
			}
			record, openErr := executionpin.OpenRecord(value)
			want := executionPinRecordStorageKey(record.PinID)
			if openErr != nil || !bytes.Equal(key, want[:]) ||
				record.LastApplied > state.Applied || record.AcquireApplied > state.Applied ||
				record.LeaseApplied > state.Applied || record.TerminalApplied > state.Applied ||
				executionPinRecordCount >= state.ExecutionPinRecordCount {
				return errors.Join(openErr, ErrExecutionPinStateCorrupt)
			}
			executionPinRecordCount++
			executionPinResidentBytes += uint64(len(key) + len(value))
			if record.Status == executionpin.StatusActive {
				if _, duplicate := activeExecutionPins[record.PinID]; duplicate {
					return ErrExecutionPinStateCorrupt
				}
				activeExecutionPins[record.PinID] = &scannedExecutionPin{
					record: record, digest: sha256.Sum256(value),
				}
			}
			return nil

		case len(key) == executionPinActiveStorageKeyBytes && key[0] == executionPinActivePrefix:
			if !statePresent || len(value) != executionPinActiveValueBytes {
				return ErrExecutionPinStateCorrupt
			}
			var pin executionpin.PinID
			copy(pin[:], key[33:])
			summary := activeExecutionPins[pin]
			if summary == nil || summary.activeSeen {
				return ErrExecutionPinStateCorrupt
			}
			want := executionPinActiveStorageKey(summary.record)
			if !bytes.Equal(key, want[:]) || !bytes.Equal(value, summary.digest[:]) {
				return ErrExecutionPinStateCorrupt
			}
			summary.activeSeen = true
			activeExecutionPinCount++
			executionPinResidentBytes += uint64(len(key) + len(value))
			return nil

		case bytes.Equal(key, splitCaptureActivationKey[:]):
			if !statePresent || splitActivation != nil {
				return ErrSplitCaptureActivation
			}
			activation, openErr := openSplitCaptureActivation(value)
			if openErr != nil || activation.Applied > state.Applied {
				return errors.Join(openErr, ErrSplitCaptureActivation)
			}
			splitActivation = &activation
			return nil

		default:
			return fmt.Errorf("%w: unknown system key", ErrSessionCorrupt)
		}
	})
	if err != nil {
		return State{}, false, 0, 0, 0, nil, err
	}
	if err := finishMutationScan(); err != nil {
		return State{}, false, 0, 0, 0, nil, err
	}
	if err := ledgerScan.finish(state); err != nil {
		return State{}, false, 0, 0, 0, nil, err
	}
	if !statePresent {
		if sessionCount != 0 || slotCount != 0 || authorityCount != 0 || routeGateHeadPresent ||
			len(routeGateRecords) != 0 || routeGateResultCount != 0 {
			return State{}, false, 0, 0, 0, nil, fmt.Errorf("%w: rows without state", ErrStateCorrupt)
		}
		gate, gateOK := routegate.NewMachine(1, routeGateMaxRecords)
		if !gateOK {
			return State{}, false, 0, 0, 0, nil, ErrInvalidOptions
		}
		return State{}, false, 0, 0, 0, gate, nil
	}
	if sessionCount != state.SessionCount || slotCount != state.SessionSlotCount ||
		authorityCount != state.AuthorityBindingCount {
		return State{}, false, 0, 0, 0, nil, fmt.Errorf("%w: session row counts", ErrStateCorrupt)
	}
	var historicalSlots uint64
	for key, f := range historicalFences {
		if historicalSeen[key] != f.refs || historicalSlots > math.MaxUint64-f.refs {
			return State{}, false, 0, 0, 0, nil, ErrSessionCorrupt
		}
		historicalSlots += f.refs
	}
	if uint64(len(historicalFences)) != state.HistoricalFenceCount || historicalSlots != state.HistoricalFenceSlots || unfencedSlots != state.UnfencedSessionSlots {
		return State{}, false, 0, 0, 0, nil, fmt.Errorf("%w: fence reference counts", ErrSessionCorrupt)
	}
	if routeGateResultCount > authorityCount*uint64(retryWindow) {
		return State{}, false, 0, 0, 0, nil, fmt.Errorf("%w: route-gate result bound", ErrStateCorrupt)
	}
	var openedRouteGate *routegate.Machine
	if routeGateHeadPresent {
		var restoreErr error
		openedRouteGate, restoreErr = routegate.RestoreMachine(
			routeGateStatus, routeGateMaxRecords, routeGateRecords,
		)
		if restoreErr != nil {
			return State{}, false, 0, 0, 0, nil, errors.Join(restoreErr, ErrStateCorrupt)
		}
	} else {
		var ok bool
		openedRouteGate, ok = routegate.NewMachine(1, routeGateMaxRecords)
		if !ok || len(routeGateRecords) != 0 || routeGateResultCount != 0 {
			return State{}, false, 0, 0, 0, nil, ErrStateCorrupt
		}
	}
	// Session tokens are Raft apply indices and therefore shard-wide unique. A
	// compact sorted vector costs eight bytes per active session—materially less
	// reopen RSS than a second Go map—while keeping corruption detection bounded
	// by active sessions rather than historical operations.
	slices.Sort(sessionEpochs)
	for i := 1; i < len(sessionEpochs); i++ {
		if sessionEpochs[i-1] == sessionEpochs[i] {
			return State{}, false, 0, 0, 0, nil, fmt.Errorf("%w: duplicate session epoch", ErrSessionCorrupt)
		}
	}
	for authorityDigest, sessionDigest := range activeIdentities {
		summary := sessions[sessionDigest]
		if class, ok := authorities[authorityDigest]; !ok || class != summary.authorityClass {
			return State{}, false, 0, 0, 0, nil,
				fmt.Errorf("%w: session authority binding digest=%x class=%d",
					ErrSessionCorrupt, authorityDigest, summary.authorityClass)
		}
	}
	for _, summary := range sessions {
		requiredCurrent := uint64(summary.retryWindow)
		if summary.highSequence < requiredCurrent {
			requiredCurrent = summary.highSequence
		}
		orderErr := (sessionAppliedOrder{
			orderedLast:  summary.orderedLastApplied,
			wrappedFirst: summary.wrappedFirstApplied,
			wrappedLast:  summary.wrappedLastApplied,
		}).finish(summary.highSequence, summary.retryWindow)
		if summary.seenSlots != summary.physicalSlots ||
			uint64(summary.currentSlots) != requiredCurrent || !summary.latestSeen ||
			orderErr != nil ||
			summary.status == SessionRetired && !isSessionTerminalResult(summary.latestResult) ||
			summary.status == SessionActive && isSessionTerminalResult(summary.latestResult) ||
			summary.latestResult == ResultSessionRevoked && summary.leaseDeadline != 0 ||
			summary.latestResult == ResultSessionRetired && summary.leaseDeadline == 0 {
			return State{}, false, 0, 0, 0, nil, fmt.Errorf("%w: incomplete session ring", ErrSessionCorrupt)
		}
	}
	if transactionControlCount != state.TransactionControlCount ||
		activeTransactionCount != state.ActiveTransactionCount ||
		transactionPayloadRows != state.TransactionPayloadRows ||
		transactionIntentRows != state.TransactionIntentRows ||
		transactionResidentBytes != state.TransactionResidentBytes ||
		uint64(len(activeTransactionIntents)) != state.TransactionIntentRows {
		return State{}, false, 0, 0, 0, nil,
			fmt.Errorf("%w: transaction image accounting", ErrTransactionStateCorrupt)
	}
	if executionPinRecordCount != state.ExecutionPinRecordCount ||
		activeExecutionPinCount != state.ActiveExecutionPinCount ||
		executionPinResidentBytes != state.ExecutionPinResidentBytes ||
		uint64(len(activeExecutionPins)) != state.ActiveExecutionPinCount {
		return State{}, false, 0, 0, 0, nil,
			fmt.Errorf("%w: execution-pin image accounting", ErrExecutionPinStateCorrupt)
	}
	for _, summary := range activeExecutionPins {
		if !summary.activeSeen {
			return State{}, false, 0, 0, 0, nil,
				fmt.Errorf("%w: missing active-scope row", ErrExecutionPinStateCorrupt)
		}
	}
	for _, summary := range transactionControls {
		control := summary.control
		if summary.residentControl != control.ResidentControlBytes ||
			summary.residentPayload != control.ResidentPayloadBytes ||
			summary.residentManifest != control.ResidentManifestBytes ||
			summary.residentMutation != control.ResidentMutationBytes ||
			summary.residentIntent != control.ResidentIntentBytes {
			return State{}, false, 0, 0, 0, nil,
				fmt.Errorf("%w: transaction resident counters", ErrTransactionStateCorrupt)
		}
		if control.Role == distributedtxn.ReplicatedRoleCoordinator {
			if !summary.payloadSeen || summary.residentPayload == 0 || summary.payloadRows == 0 {
				return State{}, false, 0, 0, 0, nil,
					fmt.Errorf("%w: coordinator creation payload", ErrTransactionStateCorrupt)
			}
			if control.PayloadKind == distributedtxn.ReplicatedPayloadManifestCoordinator {
				if !summary.manifestDescriptorSeen || summary.manifestNextPage == 0 ||
					summary.manifestNextParticipant == 0 || summary.manifestEncodedBytes == 0 ||
					summary.manifestNextPage != control.ManifestNextPage ||
					summary.manifestNextParticipant != control.ManifestNextParticipant ||
					summary.manifestEncodedBytes != control.ManifestEncodedBytes ||
					summary.manifestChain != control.ManifestChainDigest {
					return State{}, false, 0, 0, 0, nil,
						fmt.Errorf("%w: manifest progress witness", ErrTransactionStateCorrupt)
				}
				descriptor := summary.manifestDescriptor
				complete := summary.manifestNextPage == descriptor.SegmentCount &&
					summary.manifestNextParticipant == descriptor.ParticipantCount &&
					summary.manifestEncodedBytes == descriptor.EncodedBytes
				if complete && finishTransactionManifestRoot(
					summary.manifestChain, descriptor.ParticipantCount,
					descriptor.EncodedBytes, descriptor.SegmentCount,
				) != descriptor.Root {
					return State{}, false, 0, 0, 0, nil,
						fmt.Errorf("%w: manifest root", ErrTransactionStateCorrupt)
				}
				if distributedtxn.CoordinatorState(control.State) == distributedtxn.CoordinatorCommitted &&
					!complete {
					return State{}, false, 0, 0, 0, nil,
						fmt.Errorf("%w: committed incomplete manifest", ErrTransactionStateCorrupt)
				}
			}
			continue
		}
		if summary.mutationRows != control.PayloadCount ||
			uint64(len(summary.mutationKeys)) != summary.intentRows ||
			summary.intentRows == 0 {
			return State{}, false, 0, 0, 0, nil,
				fmt.Errorf("%w: participant child row counts", ErrTransactionStateCorrupt)
		}
		for _, mutationKey := range summary.mutationKeys {
			if !mutationKey.intentSeen {
				return State{}, false, 0, 0, 0, nil,
					fmt.Errorf("%w: participant mutation lacks intent", ErrTransactionStateCorrupt)
			}
		}
	}
	if len(transactionResult) != 0 && transactionResult[0] != nil {
		transactionResult[0].intents = activeTransactionIntents
		transactionResult[0].intentKeys = transactionIntentKeys
		transactionResult[0].activation = splitActivation
	}
	return state, true, sessionCount, slotCount, authorityCount, openedRouteGate, nil
}

func transactionControlActive(control TransactionControl) bool {
	if control.Role == distributedtxn.ReplicatedRoleCoordinator {
		return distributedtxn.CoordinatorState(control.State) != distributedtxn.CoordinatorRetired
	}
	return distributedtxn.ParticipantState(control.State) != distributedtxn.ParticipantReleased
}

func addTransactionResidentBytes(total *uint64, keyBytes, valueBytes int) error {
	if total == nil || keyBytes < 0 || valueBytes < 0 ||
		uint64(keyBytes) > math.MaxUint64-uint64(valueBytes) ||
		*total > math.MaxUint64-uint64(keyBytes)-uint64(valueBytes) {
		return fmt.Errorf("%w: transaction resident byte overflow", ErrTransactionStateCorrupt)
	}
	*total += uint64(keyBytes + valueBytes)
	return nil
}

var scannedTransactionMutationDigestDomain = [...]byte{
	'V', 'i', 'b', 'e', 'D', 'B', '/', 't', 'x', 'n', '/', 'r', 'e', 'l', '/', '1', 0,
}

func validateStoredSessionSlot(state State, slot SessionSlotView, lookup ...sessionFenceLookup) error {
	if !isSessionResultCode(slot.ResultCode) {
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
			slot.RouteGeneration > state.Binding.RouteGeneration {
			return fmt.Errorf("%w: retained session result mutable binding", ErrSessionCorrupt)
		}
		if slot.RoutingVersion == state.Binding.RoutingVersion && slot.RouteGeneration == state.Binding.RouteGeneration {
			if slot.AppliedSequence <= state.FenceApplied {
				return ErrSessionCorrupt
			}
		} else {
			if len(lookup) != 1 {
				return ErrSessionCorrupt
			}
			f, err := lookup[0].get(state, slot.RoutingVersion, slot.RouteGeneration)
			if err != nil || slot.AppliedSequence <= f.start || slot.AppliedSequence >= f.end {
				return ErrSessionCorrupt
			}
		}
	}
	return nil
}

func validateSessionSlotAgainstHeader(session SessionView, slot SessionSlotView) error {
	if session.Digest != slot.SessionDigest || session.ClientEpoch != slot.ClientEpoch ||
		session.AuthorityClass != slot.AuthorityClass {
		return fmt.Errorf("%w: retained session result identity", ErrSessionCorrupt)
	}
	expected, ok := canonicalSessionSlotSequence(
		session.HighSequence, session.RetryWindow, slot.Slot,
	)
	if !ok || slot.Slot >= session.PhysicalSlotCount || slot.ClientSequence != expected {
		return fmt.Errorf("%w: retained session result sequence", ErrSessionCorrupt)
	}
	if isSessionTerminalResult(slot.ResultCode) {
		if session.Status != SessionRetired || slot.ClientSequence != session.HighSequence {
			return fmt.Errorf("%w: misplaced session retirement result", ErrSessionCorrupt)
		}
		if slot.ResultCode == ResultSessionRevoked && session.LeaseDeadlineUnixNano != 0 ||
			slot.ResultCode == ResultSessionRetired && session.LeaseDeadlineUnixNano == 0 {
			return fmt.Errorf("%w: terminal session lease mismatch", ErrSessionCorrupt)
		}
	} else if session.Status == SessionRetired && slot.ClientSequence == session.HighSequence {
		return fmt.Errorf("%w: retired session lacks terminal result", ErrSessionCorrupt)
	}
	return nil
}

func canonicalSessionSlotSequence(high uint64, window uint16, slot uint16) (uint64, bool) {
	if window == 0 || slot >= window {
		return 0, false
	}
	base := uint64(slot) + 1
	if base > high {
		return 0, false
	}
	width := uint64(window)
	return base + (high-base)/width*width, true
}

type sessionAppliedOrder struct {
	orderedLast  uint64
	wrappedFirst uint64
	wrappedLast  uint64
}

func (o *sessionAppliedOrder) observe(high uint64, window uint16, slot SessionSlotView) error {
	if o == nil || window == 0 {
		return fmt.Errorf("%w: nil session apply order", ErrSessionCorrupt)
	}
	wrapped := high > uint64(window) &&
		uint64(slot.Slot) < high%uint64(window)
	if wrapped {
		if o.wrappedLast != 0 && slot.AppliedSequence <= o.wrappedLast {
			return fmt.Errorf("%w: session result order", ErrSessionCorrupt)
		}
		if o.wrappedFirst == 0 {
			o.wrappedFirst = slot.AppliedSequence
		}
		o.wrappedLast = slot.AppliedSequence
		return nil
	}
	if o.orderedLast != 0 && slot.AppliedSequence <= o.orderedLast {
		return fmt.Errorf("%w: session result order", ErrSessionCorrupt)
	}
	o.orderedLast = slot.AppliedSequence
	return nil
}

func (o sessionAppliedOrder) finish(high uint64, window uint16) error {
	if window != 0 && high > uint64(window) && high%uint64(window) != 0 &&
		(o.orderedLast == 0 || o.wrappedFirst == 0 || o.orderedLast >= o.wrappedFirst) {
		return fmt.Errorf("%w: session result order", ErrSessionCorrupt)
	}
	return nil
}

func (m *Machine) validateCompletionResult(completion replication.CompletionView) error {
	switch completion.ResultFormat {
	case ResultFormatMutation:
		if !isMutationResultCode(completion.ResultCode) {
			return fmt.Errorf("%w: unsupported mutation result grammar", ErrCompletionCorrupt)
		}
		_, err := OpenMutationCompletionResult(completion.ResultCode, completion.InlineResult)
		return err
	case ResultFormatRouteGate:
		if completion.ResultCode != ResultRouteGate {
			return fmt.Errorf("%w: unsupported route-gate result grammar", ErrCompletionCorrupt)
		}
		if _, err := routegate.OpenOutcome(completion.InlineResult); err != nil {
			return fmt.Errorf("%w: route-gate outcome", ErrCompletionCorrupt)
		}
		return nil
	default:
		return fmt.Errorf("%w: unsupported completion result grammar", ErrCompletionCorrupt)
	}
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

// CheckpointAppliedIndex is the durable contiguous apply cut supplied to WAL
// retention qualification. Replay-backed machines return their authenticated
// group certificate cut; ordinary synchronous machines retain their applied
// cut. The index alone never authorizes WAL deletion.
func (m *Machine) CheckpointAppliedIndex() uint64 {
	if m == nil {
		return 0
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.poison != nil {
		return 0
	}
	if m.checkpointGroup != nil {
		return m.checkpointGroup.CheckpointAppliedIndex()
	}
	return m.state.Applied
}

// RelationManifestDigest returns the immutable, schema-generation-bound
// logical relation manifest opened by this machine. It deliberately excludes
// replica-local storage identities, so RF3 registration can reject replicas
// that advertise the same schema generation but opened different relation
// semantics. It grants no serving authority.
func (m *Machine) RelationManifestDigest() ([sha256.Size]byte, error) {
	if m == nil {
		return [sha256.Size]byte{}, ErrApplyPoisoned
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := m.checkUsable(); err != nil {
		return [sha256.Size]byte{}, err
	}
	if m.manifestDigest == ([sha256.Size]byte{}) {
		return [sha256.Size]byte{}, ErrStateCorrupt
	}
	return m.manifestDigest, nil
}

// ApplyContractDigest returns the immutable command/result semantics opened
// with the current relation bundle. Schema rollout preparation uses it to bind
// the exact old contract before proposing the ordered transition; a machine
// already fenced by a committed transition fails closed.
func (m *Machine) ApplyContractDigest() ([sha256.Size]byte, error) {
	if m == nil {
		return [sha256.Size]byte{}, ErrApplyPoisoned
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := m.checkUsable(); err != nil {
		return [sha256.Size]byte{}, err
	}
	if m.applyContract == ([sha256.Size]byte{}) {
		return [sha256.Size]byte{}, ErrStateCorrupt
	}
	return m.applyContract, nil
}

// ObserveSchemaTransition proves that command is the exact final durable Raft
// entry which fenced this source bundle. It recomputes the authenticated entry
// digest from the persisted term and index, so matching only a target schema
// generation or apply contract can never authorize catalog publication.
//
// A later entry cannot hide the transition: a committed schema transition
// permanently fences the old bundle, leaving RecordSchema as its final kind
// until the exact target bundle is activated.
func (m *Machine) ObserveSchemaTransition(command []byte) (uint64, bool, error) {
	if m == nil {
		return 0, false, ErrApplyPoisoned
	}
	transition, err := OpenSchemaTransition(command)
	if err != nil {
		return 0, false, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.poison != nil {
		return 0, false, fmt.Errorf("%w: %v", ErrApplyPoisoned, m.poison)
	}
	state := m.state
	if !m.schemaTransitioned || state.LastKind != RecordSchema ||
		state.LastEntryType != pb.EntryNormal || state.LastTerm == 0 ||
		state.Binding != schemaTransitionBinding(transition) ||
		state.ApplyContractDigest != transition.ToApplyContract || state.RelationPlacementDigest != transition.ToPlacementDigest {
		return state.Applied, false, nil
	}
	digest := normalEntryDigest(raftmodel.ApplyMeta{
		Index: state.Applied, Term: state.LastTerm, Type: state.LastEntryType,
	}, command)
	return state.Applied, digest == state.LastEntryDigest, nil
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
		SessionEpochHighWater: m.state.SessionEpochHighWater,
		AuthorityBindingCount: m.state.AuthorityBindingCount,
	}, nil
}

// RequestLedgerUsage returns the exact replicated ledger counters and the
// immutable range authority used by this machine. It performs no row scan and
// therefore remains constant-time under large retained ledgers.
func (m *Machine) RequestLedgerUsage() (RequestLedgerUsage, error) {
	if m == nil {
		return RequestLedgerUsage{}, ErrApplyPoisoned
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := m.checkUsable(); err != nil {
		return RequestLedgerUsage{}, err
	}
	return RequestLedgerUsage{
		Enabled:             m.options.RequestLedgerRange.enabled(),
		Rows:                m.state.RequestLedgerRows,
		ResidentBytes:       m.state.RequestLedgerResidentBytes,
		ReservedBytes:       m.state.RequestLedgerReservedBytes,
		AckRows:             m.state.RequestLedgerAckRows,
		AckBytes:            m.state.RequestLedgerAckBytes,
		CapacityBytes:       m.options.RequestLedgerCapacityBytes,
		CleanupReserveBytes: m.options.RequestLedgerCleanupReserveBytes,
		Range:               m.options.RequestLedgerRange,
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
	if m.schemaTransitioned {
		return ErrSchemaTransitionPending
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
	return m.mutableBindingMatchesState(command, m.state)
}

func (m *Machine) mutableBindingMatchesState(command replication.CommandView, state State) bool {
	b := m.binding
	return command.ReplicaSetVersion == state.ReplicaSetVersion &&
		command.ActivePolicyGeneration == b.ActivePolicyGeneration &&
		command.ProtectionEpoch == b.ProtectionEpoch && command.OwnershipEpoch == b.OwnershipEpoch &&
		command.SchemaGeneration == b.SchemaGeneration && command.RoutingVersion == b.RoutingVersion &&
		command.RouteGeneration == b.RouteGeneration
}

var _ raftmodel.StateMachine = (*Machine)(nil)
