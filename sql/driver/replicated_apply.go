package driver

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"math"
	"os"
	"path/filepath"
	"strings"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/orderedkey"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/store/durable"
	"github.com/thesyncim/vibejson"
	pb "go.etcd.io/raft/v3/raftpb"
)

const (
	// ReplicatedApplyFormat is the current catalog-owned hidden apply-store
	// profile. It is deliberately separate from the public SQL table catalog:
	// the hidden collection is a state-machine participant, never a SQL relation.
	ReplicatedApplyFormat uint16 = 0

	// ReplicatedPlacementProfileFormat is the current exact key-to-shard
	// validation profile. It is deliberately limited to the sole primary-key
	// column.
	ReplicatedPlacementProfileFormat uint16 = 0

	replicatedApplyKeyProfile uint16 = 0
)

var (
	ErrReplicatedApplyUninitialized = errors.New("vibedb: replicated apply is not initialized")
	ErrReplicatedApplyMismatch      = errors.New("vibedb: replicated apply profile mismatch")
	ErrReplicatedApplyBusy          = errors.New("vibedb: replicated apply already has an owner")
	ErrReplicatedApplyClosed        = errors.New("vibedb: replicated apply is closed")
)

var replicatedApplyProfileDomain = []byte("vibedb/sql/replicated-apply-profile\x00")

// ReplicatedPlacementProfile freezes the only placement proof supported by
// replicated apply: one primary-key/shard-key scalar mapped by the native
// tuple/mapper revisions into one exact half-open shard range.
type ReplicatedPlacementProfile struct {
	Format        uint16
	ShardKey      string
	TupleVersion  distribution.TupleVersion
	MapperVersion distribution.MapperVersion
	Range         distribution.KeyRange
}

// ReplicatedApplyOptions fixes the bounded hidden state and cross-collection
// transaction profile. Every dimension is required and is persisted exactly;
// this non-serving slice performs no zero-to-default substitution.
type ReplicatedApplyOptions struct {
	MaxCompletions uint64
	TxnLimits      durable.TxnLimits
	Placement      ReplicatedPlacementProfile
}

// ReplicatedApplyIdentity is the complete retained identity of the private
// state-machine participant. Storage is random local namespace identity; the
// remaining fields freeze the portable validation and bounded apply profile.
// Exact restart must retain this value separately from the base SQL/WAL binding.
type ReplicatedApplyIdentity struct {
	Format            uint16
	Storage           string
	ValidationProfile uint8
	ValidationDigest  [32]byte
	SystemLimits      ReplicatedShardStoreLimits
	MaxCompletions    uint64
	TxnLimits         durable.TxnLimits
	Placement         ReplicatedPlacementProfile
	Sidecars          ReplicatedApplySidecarProfile
}

// ReplicatedApplyCapacityProfile is the detached, constant-size apply cut used
// to qualify a fixed no-compaction WAL. Binding is the exact catalog-owned WAL
// and authority lineage; ApplyFormat freezes the result/completion grammar;
// Applied and CompletionCount come from the same locked machine publication.
// This profile does not reserve physical storage or grant serving authority.
type ReplicatedApplyCapacityProfile struct {
	Binding         ReplicatedShardStoreBinding
	ApplyFormat     uint16
	MaxCompletions  uint64
	Initialized     bool
	Applied         uint64
	CompletionCount uint64
}

// ReplicatedApply is the opaque, singleton trusted apply claim over one bound
// SQL root. Its implementation deliberately does not expose the underlying
// durable collections, transaction log, or replicated-state Machine.
type ReplicatedApply struct {
	owner             *dbConnector
	database          *database
	machine           *replicatedstate.Machine
	table             *table
	identity          ReplicatedApplyIdentity
	closed            bool
	attemptGeneration uint64
	attemptActive     bool
}

var _ raftmodel.StateMachine = (*ReplicatedApply)(nil)

type replicatedApplyMeta struct {
	Format            uint16
	Storage           string
	ValidationProfile uint8
	ValidationDigest  [32]byte
	SystemLimits      ReplicatedShardStoreLimits
	MaxCompletions    uint64
	TxnMaxCollections int
	TxnMaxDocuments   int
	TxnMaxBytes       int64
	Placement         ReplicatedPlacementProfile
	Sidecars          ReplicatedApplySidecarProfile
}

// OpenReplicatedShardStoreWithApply opens an activated root only when both the
// retained base SQL/WAL identity and the complete private apply identity match.
// Both comparisons occur before namespace or transaction recovery.
func OpenReplicatedShardStoreWithApply(
	path string,
	expected ReplicatedShardStoreIdentity,
	expectedApply ReplicatedApplyIdentity,
) (*Database, error) {
	if err := validateReplicatedShardStoreIdentity(expected); err != nil {
		return nil, err
	}
	if err := validateReplicatedApplyIdentity(expectedApply, expected); err != nil {
		return nil, err
	}
	absolute, err := canonicalCatalogPath(path)
	if err != nil {
		return nil, err
	}
	if _, err := os.Stat(absolute); err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("%w: %s", ErrReplicatedApplyUninitialized, absolute)
		}
		return nil, err
	}
	core, err := openDatabaseWithShardStorePolicy(path, nil, shardStoreOpenPolicy{
		mode:                    shardStoreOpenReplicatedApplyExisting,
		expectedReplicated:      ownedReplicatedShardStoreIdentity(expected),
		expectedReplicatedApply: expectedApply,
	})
	if err != nil {
		return nil, err
	}
	return &Database{connector: &dbConnector{db: core}}, nil
}

// OpenReplicatedShardStoreWithApplyForSettlement recovers the crash window in
// which activation's catalog publication became durable but the random hidden
// storage identity did not reach its caller. The retained base identity and
// complete intended options are checked before recovery; success returns the
// full identity that must be durably retained for ordinary future opens.
func OpenReplicatedShardStoreWithApplyForSettlement(
	path string,
	expected ReplicatedShardStoreIdentity,
	options ReplicatedApplyOptions,
) (*Database, ReplicatedApplyIdentity, error) {
	if err := validateReplicatedShardStoreIdentity(expected); err != nil {
		return nil, ReplicatedApplyIdentity{}, err
	}
	if err := validateReplicatedApplyOptions(expected, options); err != nil {
		return nil, ReplicatedApplyIdentity{}, err
	}
	absolute, err := canonicalCatalogPath(path)
	if err != nil {
		return nil, ReplicatedApplyIdentity{}, err
	}
	if _, err := os.Stat(absolute); err != nil {
		if os.IsNotExist(err) {
			return nil, ReplicatedApplyIdentity{}, fmt.Errorf(
				"%w: %s", ErrReplicatedApplyUninitialized, absolute,
			)
		}
		return nil, ReplicatedApplyIdentity{}, err
	}
	core, err := openDatabaseWithShardStorePolicy(path, nil, shardStoreOpenPolicy{
		mode:                      shardStoreOpenReplicatedApplySettlement,
		expectedReplicated:        ownedReplicatedShardStoreIdentity(expected),
		expectedReplicatedOptions: options,
	})
	if err != nil {
		return nil, ReplicatedApplyIdentity{}, err
	}
	identity := core.catalog.ReplicatedApply.identity()
	return &Database{connector: &dbConnector{db: core}}, identity, nil
}

func (d *database) replicatedApplyPath(meta *replicatedApplyMeta) string {
	if d == nil || meta == nil {
		return ""
	}
	return filepath.Join(d.dataDir, meta.Storage+".vjc")
}

func replicatedApplySystemLimits() ReplicatedShardStoreLimits {
	const (
		stateDocumentBytes      = 2*replicatedstate.MaxStateEnvelopeBytes + 2
		completionDocumentBytes = 2*replicatedstate.MaxCompletionRecordBytes + 2
		stateKeyBytes           = 1
		completionKeyBytes      = sha256.Size + 1
	)
	return ReplicatedShardStoreLimits{
		MaxKeyBytes:       completionKeyBytes,
		MaxDocumentBytes:  completionDocumentBytes,
		MaxBatchDocuments: 2,
		MaxBatchBytes: stateKeyBytes + stateDocumentBytes +
			completionKeyBytes + completionDocumentBytes,
	}
}

func replicatedApplyDurableOptions() durable.Options {
	limits := replicatedApplySystemLimits()
	sidecars := canonicalReplicatedApplySidecars()
	return durable.Options{
		Durability:                 durable.DurabilitySync,
		MaxKeyBytes:                limits.MaxKeyBytes,
		MaxDocumentBytes:           limits.MaxDocumentBytes,
		MaxBatchDocuments:          limits.MaxBatchDocuments,
		MaxBatchBytes:              limits.MaxBatchBytes,
		SealedRecoveryJournalBytes: sidecars.SystemRecoveryJournalBytes,
	}
}

func validateReplicatedApplyCollection(
	collection *durable.Collection,
	sidecars ReplicatedApplySidecarProfile,
) error {
	limits := replicatedApplySystemLimits()
	if collection == nil || collection.HasSchema() || collection.HasIndexes() ||
		!collection.HasSynchronousDurability() || !collection.SupportsUpdate() ||
		collection.MaxKeyBytes() != limits.MaxKeyBytes ||
		collection.MaxDocumentBytes() != limits.MaxDocumentBytes ||
		collection.MaxBatchDocuments() != limits.MaxBatchDocuments ||
		collection.MaxBatchBytes() != limits.MaxBatchBytes ||
		collection.SealedRecoveryJournalBytes() != sidecars.SystemRecoveryJournalBytes {
		return fmt.Errorf("%w: hidden collection profile", ErrReplicatedApplyMismatch)
	}
	return nil
}

// OpenReplicatedApply creates or acquires the sole trusted apply claim over an
// exactly bound root. First activation requires an empty user collection and
// publishes the hidden system participant before its catalog descriptor. The
// returned identity must be retained for exact restart. If catalog publication
// is indeterminate, the identity is returned with the error and no claim; an
// exact same-handle retry or the settlement open resolves the outcome.
func (d *Database) OpenReplicatedApply(
	expected ReplicatedShardStoreIdentity,
	bootstrap *pb.Snapshot,
	options ReplicatedApplyOptions,
) (*ReplicatedApply, ReplicatedApplyIdentity, error) {
	return d.openReplicatedApply(expected, bootstrap, options, nil)
}

func (d *Database) openReplicatedApply(
	expected ReplicatedShardStoreIdentity,
	bootstrap *pb.Snapshot,
	options ReplicatedApplyOptions,
	persist func(*database) (bool, error),
) (*ReplicatedApply, ReplicatedApplyIdentity, error) {
	if err := validateReplicatedShardStoreIdentity(expected); err != nil {
		return nil, ReplicatedApplyIdentity{}, err
	}
	if err := validateReplicatedApplyOptions(expected, options); err != nil {
		return nil, ReplicatedApplyIdentity{}, err
	}
	expected = ownedReplicatedShardStoreIdentity(expected)
	if d == nil || d.connector == nil {
		return nil, ReplicatedApplyIdentity{}, ErrDatabaseClosed
	}

	// Keep connector.mu through activation and claim publication. Connect takes
	// the same lock, so no SQL session can slip between the refs==0 proof and
	// the lifetime reference installed below.
	d.connector.mu.Lock()
	defer d.connector.mu.Unlock()
	if d.connector.closed || d.connector.db == nil {
		return nil, ReplicatedApplyIdentity{}, ErrDatabaseClosed
	}
	if d.connector.refs != 0 {
		return nil, ReplicatedApplyIdentity{}, fmt.Errorf(
			"%w: %d live SQL session(s)", ErrReplicatedApplyBusy, d.connector.refs,
		)
	}
	core := d.connector.db
	core.mu.Lock()
	defer core.mu.Unlock()
	if core.closed {
		return nil, ReplicatedApplyIdentity{}, ErrDatabaseClosed
	}
	if core.replicatedApplyClaim != nil {
		return nil, ReplicatedApplyIdentity{}, ErrReplicatedApplyBusy
	}
	if core.catalog.ReplicatedShardStore == nil ||
		*core.catalog.ReplicatedShardStore != expected {
		return nil, ReplicatedApplyIdentity{}, ErrReplicatedShardStoreIdentityMismatch
	}
	if err := core.settleCatalogLocked(); err != nil {
		return nil, ReplicatedApplyIdentity{}, fmt.Errorf(
			"vibedb: settle SQL catalog before replicated apply: %w", err,
		)
	}
	wantTxnOptions := durable.TxnLogOptions{
		Capacity:       expected.Sidecars.TransactionMarkerBytes,
		SealedCapacity: true,
	}
	if core.txnLog == nil || core.txnLog.Options() != wantTxnOptions {
		return nil, ReplicatedApplyIdentity{}, fmt.Errorf(
			"%w: transaction-marker profile", ErrReplicatedApplyMismatch,
		)
	}
	if err := core.txnLog.EnsureMinted(); err != nil {
		return nil, ReplicatedApplyIdentity{}, fmt.Errorf(
			"vibedb: qualify replicated transaction marker: %w", err,
		)
	}

	t := core.tables[expected.UserTable]
	if t == nil || t.collection == nil {
		return nil, ReplicatedApplyIdentity{}, fmt.Errorf(
			"%w: replicated user collection is unavailable", ErrReplicatedApplyMismatch,
		)
	}
	if core.catalog.ReplicatedApply == nil {
		// A prior definite descriptor failure may have retained an unpublished
		// candidate because detaching it from the transaction log could not yet
		// complete. Retry ownership discharge and cleanup before allocating a new
		// identity; never overwrite the only pointers to a still-registered store.
		if core.replicatedApplyCollection != nil || core.replicatedApplyFile != nil {
			if core.replicatedApplyCollection == nil || core.replicatedApplyFile == nil {
				return nil, ReplicatedApplyIdentity{}, errors.New(
					"vibedb: incomplete unpublished replicated apply ownership",
				)
			}
			if err := core.txnLog.DetachCollection(
				core.replicatedApplyCollection,
			); err != nil {
				return nil, ReplicatedApplyIdentity{}, fmt.Errorf(
					"vibedb: retry unpublished replicated apply detach: %w", err,
				)
			}
			path := core.replicatedApplyFile.Name()
			cleanupErr := core.discardUnpublishedStorageLocked(
				core.replicatedApplyCollection, core.replicatedApplyFile, path,
			)
			core.replicatedApplyCollection = nil
			core.replicatedApplyFile = nil
			if cleanupErr != nil {
				return nil, ReplicatedApplyIdentity{}, fmt.Errorf(
					"vibedb: retry unpublished replicated apply cleanup: %w",
					cleanupErr,
				)
			}
		}
		if t.collection.Len() != 0 {
			return nil, ReplicatedApplyIdentity{}, fmt.Errorf(
				"%w: first activation requires an empty user collection",
				ErrReplicatedApplyMismatch,
			)
		}
		identity, err := core.createReplicatedApplyStorageLocked(expected, options)
		if err != nil {
			return nil, ReplicatedApplyIdentity{}, err
		}
		previousPending := core.catalogWritePending
		stored := replicatedApplyMetaFromIdentity(identity)
		core.catalog.ReplicatedApply = &stored
		core.catalogWritePending = true
		var published bool
		if persist != nil {
			published, err = persist(core)
		} else {
			published, err = core.persistCatalogLocked()
		}
		if err == nil && !published {
			err = errors.New("catalog persistence returned without publication")
		}
		if err != nil {
			publicationErr := fmt.Errorf("vibedb: publish replicated apply descriptor: %w", err)
			if published || errors.Is(err, durable.ErrCommitOutcomeUnknown) {
				core.catalogWritePending = !published
				return nil, identity, publicationErr
			}
			core.catalog.ReplicatedApply = nil
			core.catalogWritePending = previousPending
			path := core.replicatedApplyPath(&stored)
			detachErr := core.txnLog.DetachCollection(core.replicatedApplyCollection)
			if detachErr != nil {
				// Keep the unpublished candidate owned by the database. Closing or
				// unlinking it while the transaction log still names the handle would
				// leave stale catalog scope; the detach failure already makes retry
				// unsafe and database teardown will release both owners.
				return nil, ReplicatedApplyIdentity{}, errors.Join(
					publicationErr,
					fmt.Errorf(
						"vibedb: detach unpublished replicated apply storage: %w",
						detachErr,
					),
				)
			}
			cleanupErr := core.discardUnpublishedStorageLocked(
				core.replicatedApplyCollection, core.replicatedApplyFile, path,
			)
			core.replicatedApplyCollection = nil
			core.replicatedApplyFile = nil
			return nil, ReplicatedApplyIdentity{}, errors.Join(publicationErr, cleanupErr)
		}
	} else if !replicatedApplyMetaMatchesOptions(
		core.catalog.ReplicatedApply, expected, options,
	) {
		return nil, ReplicatedApplyIdentity{}, ErrReplicatedApplyMismatch
	}

	identity := core.catalog.ReplicatedApply.identity()
	if core.replicatedApplyCollection == nil {
		return nil, identity, fmt.Errorf(
			"%w: hidden collection is not open", ErrReplicatedApplyMismatch,
		)
	}
	claim := &ReplicatedApply{
		owner: d.connector, database: core, table: t, identity: identity,
	}
	validator := newReplicatedSQLMutationValidator(expected, t, identity.Placement)
	machine, err := replicatedstate.Open(
		replicatedStateBinding(expected), bootstrap,
		replicatedstate.CollectionTarget{
			Collection: core.replicatedApplyCollection,
			Validation: replicatedstate.ValidationSchemaFreeJSON,
			Limits:     replicatedStateCollectionLimits(identity.SystemLimits),
		},
		replicatedstate.UserCollection{
			Name: expected.UserTable,
			Target: replicatedstate.CollectionTarget{
				Collection:             t.collection,
				Validation:             replicatedstate.ValidationProfile(identity.ValidationProfile),
				ValidationDigest:       identity.ValidationDigest,
				Validator:              validator,
				ObserveMutationAttempt: claim.observeMutationAttempt,
				Limits:                 replicatedStateCollectionLimits(expected.UserLimits),
			},
		},
		core.txnLog,
		replicatedstate.Options{
			TxnLimits: identity.TxnLimits, MaxCompletions: identity.MaxCompletions,
		},
	)
	if err != nil {
		return nil, identity, fmt.Errorf("vibedb: open replicated state machine: %w", err)
	}
	claim.machine = machine
	core.replicatedApplyClaim = claim
	d.connector.refs++
	return claim, identity, nil
}

func (d *database) createReplicatedApplyStorageLocked(
	base ReplicatedShardStoreIdentity,
	options ReplicatedApplyOptions,
) (ReplicatedApplyIdentity, error) {
	if err := d.checkRetirementCapacityLocked(1); err != nil {
		return ReplicatedApplyIdentity{}, err
	}
	if err := d.ensureDataDir(); err != nil {
		return ReplicatedApplyIdentity{}, err
	}
	storage, err := d.newStorageIdentityLocked()
	if err != nil {
		return ReplicatedApplyIdentity{}, err
	}
	meta := newReplicatedApplyMeta(base, storage, options)
	path := d.replicatedApplyPath(&meta)
	file, err := createPublishableTableTemp(
		d.dataDir, "."+filepath.Base(path)+".tmp-",
	)
	if err != nil {
		return ReplicatedApplyIdentity{}, err
	}
	tmpPath := file.Name()
	collection, err := durable.Create(file, replicatedApplyDurableOptions())
	if err != nil {
		return ReplicatedApplyIdentity{}, errors.Join(err,
			d.discardUnpublishedStorageLocked(collection, file, tmpPath))
	}
	file, collection, err = d.publishTableStorageLocked(
		tmpPath, path, file, collection, replicatedApplyDurableOptions(),
	)
	if err != nil {
		return ReplicatedApplyIdentity{}, fmt.Errorf(
			"vibedb: publish replicated apply storage: %w", err,
		)
	}
	if err := validateReplicatedApplyCollection(collection, meta.Sidecars); err != nil {
		return ReplicatedApplyIdentity{}, errors.Join(err,
			d.discardUnpublishedStorageLocked(collection, file, path))
	}
	if err := d.txnLog.AdoptCollection(collection); err != nil {
		return ReplicatedApplyIdentity{}, errors.Join(err,
			d.discardUnpublishedStorageLocked(collection, file, path))
	}
	d.replicatedApplyFile = file
	d.replicatedApplyCollection = collection
	return meta.identity(), nil
}

func replicatedStateBinding(identity ReplicatedShardStoreIdentity) replicatedstate.Binding {
	b := identity.Binding
	return replicatedstate.Binding{
		ClusterID:             replication.ID128(b.ClusterID),
		ClusterIncarnation:    replication.ID128(b.ClusterIncarnation),
		TopologyRecoveryEpoch: b.TopologyRecoveryEpoch,
		Distribution:          b.Distribution, Shard: b.Shard,
		AllocationGeneration:   b.AllocationGeneration,
		ShardIncarnation:       replication.ID128(b.ShardIncarnation),
		GroupID:                replication.ID128(b.GroupID),
		ActivePolicyGeneration: b.Authority.ActivePolicyGeneration,
		ProtectionEpoch:        b.Authority.ProtectionEpoch,
		OwnershipEpoch:         b.Authority.OwnershipEpoch,
		SchemaGeneration:       b.Authority.SchemaGeneration,
		RoutingVersion:         b.Authority.RoutingVersion,
		RouteGeneration:        b.Authority.RouteGeneration,
	}
}

func replicatedStateCollectionLimits(limits ReplicatedShardStoreLimits) replicatedstate.CollectionLimits {
	return replicatedstate.CollectionLimits{
		MaxKeyBytes:          limits.MaxKeyBytes,
		MaxDocumentBytes:     limits.MaxDocumentBytes,
		MaxDistinctMutations: limits.MaxBatchDocuments,
		MaxBatchBytes:        limits.MaxBatchBytes,
	}
}

type replicatedSQLMutationValidator struct {
	primaryKey  string
	primary     vibejson.CompiledPointer
	maxKeyBytes int
	placement   replicatedSQLPlacementValidator
}

type replicatedSQLPlacementValidator struct {
	binding placementBinding
	mapper  *distribution.NativeMapper
	target  distribution.KeyRange
}

func newReplicatedSQLMutationValidator(
	identity ReplicatedShardStoreIdentity,
	table *table,
	profile ReplicatedPlacementProfile,
) replicatedSQLMutationValidator {
	validator := replicatedSQLMutationValidator{
		primaryKey: identity.UserPrimaryKey, primary: table.primary,
		maxKeyBytes: identity.UserLimits.MaxKeyBytes,
	}
	mapper := distribution.NewNativeMapper(1)
	validator.placement = replicatedSQLPlacementValidator{
		binding: placementBinding{
			placement: distribution.TablePlacement{
				Table: identity.UserTable, Distribution: distribution.DistributionName(identity.Binding.Distribution),
				Columns: []string{strings.Clone(profile.ShardKey)},
			},
			spec: distribution.DistributionSpec{
				Name: distribution.DistributionName(identity.Binding.Distribution), Arity: 1,
				MapperVersion: profile.MapperVersion,
			},
			mapper: mapper, native: mapper,
			pointers: []vibejson.CompiledPointer{table.primary},
		},
		mapper: mapper, target: profile.Range,
	}
	return validator
}

func (v replicatedSQLMutationValidator) ValidatePut(key, value []byte) replicatedstate.MutationValidation {
	derived, err := documentKey(value, v.primaryKey, v.primary, v.maxKeyBytes)
	if errors.Is(err, durable.ErrKeyTooLarge) {
		return replicatedstate.MutationValidationTargetBound
	}
	if err != nil || derived != string(key) {
		return replicatedstate.MutationValidationInvalid
	}
	var scalarScratch [1]distribution.Scalar
	values, _, err := documentShardKey(value, &v.placement.binding, scalarScratch[:0], nil)
	if err != nil {
		return replicatedstate.MutationValidationInvalid
	}
	point, err := v.placement.mapper.PointFor(values)
	if err != nil {
		return replicatedstate.MutationValidationInvalid
	}
	if !v.placement.target.Contains(point) {
		return replicatedstate.MutationValidationWrongShard
	}
	return replicatedstate.MutationValidationAccept
}

func (v replicatedSQLMutationValidator) ValidateDelete(
	key, current []byte,
	found bool,
) replicatedstate.MutationValidation {
	component, decoded, next, err := orderedkey.DecodeComponent(nil, key, 0)
	if err != nil || next != len(key) || component.Descending || component.Kind == orderedkey.KindNull {
		return replicatedstate.MutationValidationInvalid
	}
	if found {
		return v.ValidatePut(key, current)
	}
	var scalar distribution.Scalar
	switch component.Kind {
	case orderedkey.KindString:
		scalar = distribution.NewString(string(decoded[component.PayloadStart:component.PayloadEnd]))
	case orderedkey.KindNumber:
		var scalarErr error
		scalar, scalarErr = distribution.NewNumber(
			string(decoded[component.PayloadStart:component.PayloadEnd]),
		)
		if scalarErr != nil {
			return replicatedstate.MutationValidationInvalid
		}
	default:
		return replicatedstate.MutationValidationInvalid
	}
	var scalarScratch [1]distribution.Scalar
	scalarScratch[0] = scalar
	point, err := v.placement.mapper.PointFor(scalarScratch[:])
	if err != nil {
		return replicatedstate.MutationValidationInvalid
	}
	if !v.placement.target.Contains(point) {
		return replicatedstate.MutationValidationWrongShard
	}
	return replicatedstate.MutationValidationAccept
}

func (a *ReplicatedApply) observeMutationAttempt(keys replicatedstate.AttemptedMutationKeys) {
	if a == nil || a.table == nil || keys.Len() == 0 {
		return
	}
	// The core invokes this only while the wrapper still holds database.mu.
	// A generation move proves publication; a sticky persistence failure makes
	// the in-process outcome uncertain and therefore advances conservatively.
	if !a.attemptActive ||
		(a.table.collection.Generation() == a.attemptGeneration &&
			a.table.collection.PersistenceError() == nil) {
		return
	}
	changed := make([]string, keys.Len())
	for i := range changed {
		changed[i] = string(keys.Key(i))
	}
	a.table.conflicts.recordKeys(changed)
}

func (a *ReplicatedApply) checkLocked() error {
	if a == nil || a.database == nil || a.machine == nil || a.closed ||
		a.database.closed || a.database.replicatedApplyClaim != a {
		return ErrReplicatedApplyClosed
	}
	return nil
}

// Identity returns the detached identity that must be retained for exact open.
func (a *ReplicatedApply) Identity() (ReplicatedApplyIdentity, error) {
	if a == nil || a.database == nil {
		return ReplicatedApplyIdentity{}, ErrReplicatedApplyClosed
	}
	a.database.mu.RLock()
	defer a.database.mu.RUnlock()
	if err := a.checkLocked(); err != nil {
		return ReplicatedApplyIdentity{}, err
	}
	identity := a.identity
	identity.Storage = strings.Clone(identity.Storage)
	identity.Placement = ownedReplicatedPlacementProfile(identity.Placement)
	return identity, nil
}

// CapacityQualificationProfile returns the exact binding and constant-size
// durable apply cut needed by a higher-level completion-capacity proof. It
// fails closed for a closed or poisoned claim.
func (a *ReplicatedApply) CapacityQualificationProfile() (
	ReplicatedApplyCapacityProfile,
	error,
) {
	if a == nil || a.database == nil {
		return ReplicatedApplyCapacityProfile{}, ErrReplicatedApplyClosed
	}
	a.database.mu.RLock()
	defer a.database.mu.RUnlock()
	if err := a.checkLocked(); err != nil {
		return ReplicatedApplyCapacityProfile{}, err
	}
	base := a.database.catalog.ReplicatedShardStore
	if base == nil {
		return ReplicatedApplyCapacityProfile{}, ErrReplicatedApplyMismatch
	}
	state, err := a.machine.CompletionCapacityState()
	if err != nil {
		return ReplicatedApplyCapacityProfile{}, err
	}
	return ReplicatedApplyCapacityProfile{
		Binding:         ownedReplicatedShardStoreBinding(base.Binding),
		ApplyFormat:     a.identity.Format,
		MaxCompletions:  a.identity.MaxCompletions,
		Initialized:     state.Initialized,
		Applied:         state.Applied,
		CompletionCount: state.CompletionCount,
	}, nil
}

// ClaimRuntimeOwnership proves that database is the live owner of this apply
// claim, requires the claim's lifetime reference to be the connector's only
// live reference, and atomically retires the connector against future SQL
// sessions. This is a one-way ownership transfer: after success, the caller
// must close the apply claim before the Database, and the root cannot return to
// ordinary session ownership without a complete close/reopen.
func (a *ReplicatedApply) ClaimRuntimeOwnership(database *Database) error {
	if a == nil || database == nil || database.connector == nil {
		return ErrReplicatedApplyClosed
	}
	connector := database.connector
	connector.mu.Lock()
	defer connector.mu.Unlock()
	if connector.closed || connector.db == nil {
		return ErrDatabaseClosed
	}
	if a.owner != connector || a.database != connector.db || connector.refs != 1 {
		return fmt.Errorf(
			"%w: runtime ownership requires the apply claim as the sole live reference",
			ErrReplicatedApplyBusy,
		)
	}
	core := connector.db
	core.mu.RLock()
	defer core.mu.RUnlock()
	if err := a.checkLocked(); err != nil {
		return err
	}
	if core.replicatedApplyClaim != a {
		return ErrReplicatedApplyClosed
	}
	connector.closed = true
	return nil
}

// Close releases the singleton apply claim and its connector lifetime
// reference. It does not unbind or remove the durable hidden participant.
func (a *ReplicatedApply) Close() error {
	if a == nil || a.database == nil {
		return nil
	}
	core := a.database
	core.mu.Lock()
	if a.closed {
		core.mu.Unlock()
		return nil
	}
	if core.replicatedApplyClaim != a {
		core.mu.Unlock()
		return ErrReplicatedApplyClosed
	}
	a.closed = true
	core.replicatedApplyClaim = nil
	core.mu.Unlock()
	return a.owner.release()
}

// Applied implements raftmodel.StateMachine.
func (a *ReplicatedApply) Applied() uint64 {
	if a == nil || a.database == nil {
		return 0
	}
	a.database.mu.RLock()
	defer a.database.mu.RUnlock()
	if a.checkLocked() != nil {
		return 0
	}
	return a.machine.Applied()
}

// Published implements raftmodel.StateMachine.
func (a *ReplicatedApply) Published() raftmodel.Publication {
	if a == nil || a.database == nil {
		return raftmodel.Publication{}
	}
	a.database.mu.RLock()
	defer a.database.mu.RUnlock()
	if a.checkLocked() != nil {
		return raftmodel.Publication{}
	}
	return a.machine.Published()
}

// ApplyNormal implements raftmodel.StateMachine under the SQL publication
// lock, pairing durable user/system mutation with conflict-clock publication.
func (a *ReplicatedApply) ApplyNormal(
	meta raftmodel.ApplyMeta,
	data []byte,
) (raftmodel.Publication, error) {
	if a == nil || a.database == nil {
		return raftmodel.Publication{}, ErrReplicatedApplyClosed
	}
	a.database.mu.Lock()
	defer a.database.mu.Unlock()
	if err := a.checkLocked(); err != nil {
		return raftmodel.Publication{}, err
	}
	a.attemptGeneration = a.table.collection.Generation()
	a.attemptActive = true
	publication, err := a.machine.ApplyNormal(meta, data)
	a.attemptActive = false
	a.attemptGeneration = 0
	return publication, err
}

// ApplyConfiguration implements raftmodel.StateMachine under the SQL
// publication lock.
func (a *ReplicatedApply) ApplyConfiguration(
	meta raftmodel.ApplyMeta,
	conf *pb.ConfState,
) (raftmodel.Publication, error) {
	if a == nil || a.database == nil {
		return raftmodel.Publication{}, ErrReplicatedApplyClosed
	}
	a.database.mu.Lock()
	defer a.database.mu.Unlock()
	if err := a.checkLocked(); err != nil {
		return raftmodel.Publication{}, err
	}
	return a.machine.ApplyConfiguration(meta, conf)
}

// InstallSnapshot implements raftmodel.StateMachine. The underlying Phase-1b
// machine accepts only its exact static bootstrap snapshot.
func (a *ReplicatedApply) InstallSnapshot(
	snapshot *pb.Snapshot,
) (raftmodel.Publication, error) {
	if a == nil || a.database == nil {
		return raftmodel.Publication{}, ErrReplicatedApplyClosed
	}
	a.database.mu.Lock()
	defer a.database.mu.Unlock()
	if err := a.checkLocked(); err != nil {
		return raftmodel.Publication{}, err
	}
	return a.machine.InstallSnapshot(snapshot)
}

// AdmitCommand delegates the bounded, non-reserving pre-proposal check. A nil
// result does not grant serving or reserve completion capacity.
func (a *ReplicatedApply) AdmitCommand(data []byte) error {
	if a == nil || a.database == nil {
		return ErrReplicatedApplyClosed
	}
	a.database.mu.RLock()
	defer a.database.mu.RUnlock()
	if err := a.checkLocked(); err != nil {
		return err
	}
	err := a.machine.AdmitCommand(data)
	if err == nil {
		return nil
	}
	// Machine admission returns the exact first causal error even when that
	// error poisons the durable apply boundary. Attach the constant-size health
	// result so a Runtime can terminally classify the first failure rather than
	// discovering ErrApplyPoisoned only on a later proposal.
	if _, healthErr := a.machine.CompletionCapacityState(); healthErr != nil {
		return errors.Join(err, healthErr)
	}
	return err
}

// LookupCompletion returns an owned exact completion envelope.
func (a *ReplicatedApply) LookupCompletion(
	data []byte,
) (replicatedstate.CompletionLookup, error) {
	if a == nil || a.database == nil {
		return replicatedstate.CompletionLookup{}, ErrReplicatedApplyClosed
	}
	a.database.mu.RLock()
	defer a.database.mu.RUnlock()
	if err := a.checkLocked(); err != nil {
		return replicatedstate.CompletionLookup{}, err
	}
	return a.machine.LookupCompletion(data)
}

func replicatedApplyProfileDigest(
	identity ReplicatedShardStoreIdentity,
	placement ReplicatedPlacementProfile,
) [32]byte {
	h := sha256.New()
	_, _ = h.Write(replicatedApplyProfileDomain)
	var keyProfile [2]byte
	binary.LittleEndian.PutUint16(keyProfile[:], replicatedApplyKeyProfile)
	_, _ = h.Write(keyProfile[:])
	writeReplicatedApplyHashFrame(h, []byte(identity.UserTable))
	writeReplicatedApplyHashFrame(h, []byte(identity.UserPrimaryKey))
	var limits [32]byte
	binary.LittleEndian.PutUint64(limits[0:8], uint64(identity.UserLimits.MaxKeyBytes))
	binary.LittleEndian.PutUint64(limits[8:16], uint64(identity.UserLimits.MaxDocumentBytes))
	binary.LittleEndian.PutUint64(limits[16:24], uint64(identity.UserLimits.MaxBatchDocuments))
	binary.LittleEndian.PutUint64(limits[24:32], uint64(identity.UserLimits.MaxBatchBytes))
	_, _ = h.Write(limits[:])
	writeReplicatedApplyHashFrame(h, []byte(identity.Binding.Distribution))
	writeReplicatedApplyHashFrame(h, []byte(identity.Binding.Shard))
	var generations [24]byte
	binary.LittleEndian.PutUint64(generations[0:8], identity.Binding.AllocationGeneration)
	binary.LittleEndian.PutUint64(generations[8:16], identity.Binding.Authority.RoutingVersion)
	binary.LittleEndian.PutUint64(generations[16:24], identity.Binding.Authority.RouteGeneration)
	_, _ = h.Write(generations[:])
	var placementVersions [10]byte
	binary.LittleEndian.PutUint16(placementVersions[0:2], placement.Format)
	binary.LittleEndian.PutUint32(placementVersions[2:6], uint32(placement.TupleVersion))
	binary.LittleEndian.PutUint32(placementVersions[6:10], uint32(placement.MapperVersion))
	_, _ = h.Write(placementVersions[:])
	writeReplicatedApplyHashFrame(h, []byte(placement.ShardKey))
	_, _ = h.Write(placement.Range.Start[:])
	_, _ = h.Write(placement.Range.End.Point[:])
	if placement.Range.End.Max {
		_, _ = h.Write([]byte{1})
	} else {
		_, _ = h.Write([]byte{0})
	}
	var digest [32]byte
	_ = h.Sum(digest[:0])
	return digest
}

func writeReplicatedApplyHashFrame(h hash.Hash, value []byte) {
	var length [8]byte
	binary.LittleEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = h.Write(length[:])
	_, _ = h.Write(value)
}

func newReplicatedApplyMeta(
	identity ReplicatedShardStoreIdentity,
	storage string,
	options ReplicatedApplyOptions,
) replicatedApplyMeta {
	return replicatedApplyMeta{
		Format:            ReplicatedApplyFormat,
		Storage:           strings.Clone(storage),
		ValidationProfile: uint8(replicatedstate.ValidationDeterministicMutation),
		ValidationDigest:  replicatedApplyProfileDigest(identity, options.Placement),
		SystemLimits:      replicatedApplySystemLimits(),
		MaxCompletions:    options.MaxCompletions,
		TxnMaxCollections: options.TxnLimits.MaxCollections,
		TxnMaxDocuments:   options.TxnLimits.MaxDocuments,
		TxnMaxBytes:       options.TxnLimits.MaxBytes,
		Placement:         ownedReplicatedPlacementProfile(options.Placement),
		Sidecars:          canonicalReplicatedApplySidecars(),
	}
}

func validateReplicatedApplyOptions(
	identity ReplicatedShardStoreIdentity,
	options ReplicatedApplyOptions,
) error {
	if identity.UserTable == replicatedstate.SystemCollectionName {
		return fmt.Errorf(
			"%w: user table uses the reserved system collection name",
			ErrReplicatedApplyMismatch,
		)
	}
	if options.MaxCompletions == 0 ||
		options.MaxCompletions > replicatedstate.MaxRetainedCompletions ||
		options.TxnLimits.MaxCollections < 2 ||
		options.TxnLimits.MaxDocuments < identity.UserLimits.MaxBatchDocuments+2 ||
		options.TxnLimits.MaxBytes <= 0 {
		return fmt.Errorf("%w: invalid transaction or retention limits", ErrReplicatedApplyMismatch)
	}
	userBytes := min(identity.UserLimits.MaxBatchBytes, replication.MaxCommandBytes)
	systemBytes := replicatedApplySystemLimits().MaxBatchBytes
	if userBytes < 0 || systemBytes < 0 ||
		int64(userBytes) > math.MaxInt64-int64(systemBytes) ||
		options.TxnLimits.MaxBytes < int64(userBytes)+int64(systemBytes) {
		return fmt.Errorf("%w: transaction byte limit does not cover one apply", ErrReplicatedApplyMismatch)
	}
	if err := validateReplicatedPlacementProfile(options.Placement, identity); err != nil {
		return err
	}
	return nil
}

func validateReplicatedPlacementProfile(
	profile ReplicatedPlacementProfile,
	identity ReplicatedShardStoreIdentity,
) error {
	if validateReplicatedPlacementProfileGrammar(profile) != nil ||
		profile.ShardKey != identity.UserPrimaryKey {
		return fmt.Errorf(
			"%w: unsupported or non-canonical one-primary-key placement profile",
			ErrReplicatedApplyMismatch,
		)
	}
	return nil
}

func validateReplicatedPlacementProfileGrammar(profile ReplicatedPlacementProfile) error {
	shardKey, err := vibejson.CompilePointer(profile.ShardKey)
	if profile.Format != ReplicatedPlacementProfileFormat ||
		err != nil || len(shardKey.Tokens) == 0 || shardKey.String() != profile.ShardKey ||
		profile.TupleVersion != distribution.CurrentTupleVersion ||
		profile.MapperVersion != distribution.NativeMapperVersion ||
		!profile.Range.Valid() ||
		(profile.Range.End.Max && profile.Range.End.Point != (distribution.KeyspacePoint{})) {
		return fmt.Errorf(
			"%w: unsupported or non-canonical one-primary-key placement profile",
			ErrReplicatedApplyMismatch,
		)
	}
	return nil
}

func ownedReplicatedPlacementProfile(profile ReplicatedPlacementProfile) ReplicatedPlacementProfile {
	profile.ShardKey = strings.Clone(profile.ShardKey)
	return profile
}

func replicatedApplyMetaMatchesOptions(
	meta *replicatedApplyMeta,
	identity ReplicatedShardStoreIdentity,
	options ReplicatedApplyOptions,
) bool {
	if meta == nil || validateReplicatedApplyOptions(identity, options) != nil {
		return false
	}
	want := newReplicatedApplyMeta(identity, meta.Storage, options)
	return *meta == want
}

func (m replicatedApplyMeta) options() ReplicatedApplyOptions {
	return ReplicatedApplyOptions{
		MaxCompletions: m.MaxCompletions,
		TxnLimits: durable.TxnLimits{
			MaxCollections: m.TxnMaxCollections,
			MaxDocuments:   m.TxnMaxDocuments,
			MaxBytes:       m.TxnMaxBytes,
		},
		Placement: ownedReplicatedPlacementProfile(m.Placement),
	}
}

func (m replicatedApplyMeta) identity() ReplicatedApplyIdentity {
	return ReplicatedApplyIdentity{
		Format: m.Format, Storage: strings.Clone(m.Storage),
		ValidationProfile: m.ValidationProfile, ValidationDigest: m.ValidationDigest,
		SystemLimits: m.SystemLimits, MaxCompletions: m.MaxCompletions,
		TxnLimits: m.options().TxnLimits, Placement: ownedReplicatedPlacementProfile(m.Placement),
		Sidecars: m.Sidecars,
	}
}

func replicatedApplyMetaFromIdentity(identity ReplicatedApplyIdentity) replicatedApplyMeta {
	return replicatedApplyMeta{
		Format: identity.Format, Storage: strings.Clone(identity.Storage),
		ValidationProfile: identity.ValidationProfile,
		ValidationDigest:  identity.ValidationDigest, SystemLimits: identity.SystemLimits,
		MaxCompletions:    identity.MaxCompletions,
		TxnMaxCollections: identity.TxnLimits.MaxCollections,
		TxnMaxDocuments:   identity.TxnLimits.MaxDocuments,
		TxnMaxBytes:       identity.TxnLimits.MaxBytes,
		Placement:         ownedReplicatedPlacementProfile(identity.Placement),
		Sidecars:          identity.Sidecars,
	}
}

func validateReplicatedApplyIdentity(
	identity ReplicatedApplyIdentity,
	base ReplicatedShardStoreIdentity,
) error {
	meta := replicatedApplyMetaFromIdentity(identity)
	return validateReplicatedApplyMeta(&meta, &base)
}

func validateReplicatedApplyMeta(
	m *replicatedApplyMeta,
	identity *ReplicatedShardStoreIdentity,
) error {
	if m == nil {
		return nil
	}
	if identity == nil {
		return fmt.Errorf("%w: replicated shard binding is missing", ErrReplicatedApplyMismatch)
	}
	if m.Format != ReplicatedApplyFormat ||
		m.ValidationProfile != uint8(replicatedstate.ValidationDeterministicMutation) ||
		validateReplicatedPlacementProfile(m.Placement, *identity) != nil ||
		m.ValidationDigest != replicatedApplyProfileDigest(*identity, m.Placement) {
		return ErrReplicatedApplyMismatch
	}
	if err := validateStorageIdentity(m.Storage); err != nil || m.Storage == "" ||
		m.Storage == identity.UserStorage {
		return fmt.Errorf("%w: invalid or aliased system storage identity", ErrReplicatedApplyMismatch)
	}
	if m.SystemLimits != replicatedApplySystemLimits() {
		return fmt.Errorf("%w: system collection limits", ErrReplicatedApplyMismatch)
	}
	if err := validateReplicatedApplySidecarsForLimits(m.Sidecars, m.SystemLimits); err != nil {
		return err
	}
	options := m.options()
	if err := validateReplicatedApplyOptions(*identity, options); err != nil {
		return err
	}
	return nil
}

func (m replicatedApplyMeta) MarshalJSON() ([]byte, error) {
	if m.Format != ReplicatedApplyFormat {
		return nil, fmt.Errorf("vibedb: unsupported replicated apply format %d", m.Format)
	}
	if err := validateReplicatedPlacementProfileGrammar(m.Placement); err != nil {
		return nil, err
	}
	type encoded struct {
		Format            uint16                        `json:"format"`
		Storage           string                        `json:"storage"`
		ValidationProfile uint8                         `json:"validation_profile"`
		ValidationDigest  string                        `json:"validation_digest"`
		SystemLimits      ReplicatedShardStoreLimits    `json:"system_limits"`
		MaxCompletions    uint64                        `json:"max_completions"`
		TxnMaxCollections int                           `json:"txn_max_collections"`
		TxnMaxDocuments   int                           `json:"txn_max_documents"`
		TxnMaxBytes       int64                         `json:"txn_max_bytes"`
		Placement         ReplicatedPlacementProfile    `json:"placement"`
		Sidecars          ReplicatedApplySidecarProfile `json:"sidecars"`
	}
	return json.Marshal(encoded{
		Format: m.Format, Storage: m.Storage,
		ValidationProfile: m.ValidationProfile,
		ValidationDigest:  hex.EncodeToString(m.ValidationDigest[:]),
		SystemLimits:      m.SystemLimits,
		MaxCompletions:    m.MaxCompletions,
		TxnMaxCollections: m.TxnMaxCollections,
		TxnMaxDocuments:   m.TxnMaxDocuments,
		TxnMaxBytes:       m.TxnMaxBytes,
		Placement:         m.Placement,
		Sidecars:          m.Sidecars,
	})
}

func (profile ReplicatedPlacementProfile) MarshalJSON() ([]byte, error) {
	if err := validateReplicatedPlacementProfileGrammar(profile); err != nil {
		return nil, err
	}
	type encoded struct {
		Format        uint16 `json:"format"`
		ShardKey      string `json:"shard_key"`
		TupleVersion  uint32 `json:"tuple_version"`
		MapperVersion uint32 `json:"mapper_version"`
		RangeStart    string `json:"range_start"`
		RangeEnd      string `json:"range_end"`
		RangeEndMax   bool   `json:"range_end_max"`
	}
	return json.Marshal(encoded{
		Format: profile.Format, ShardKey: profile.ShardKey,
		TupleVersion: uint32(profile.TupleVersion), MapperVersion: uint32(profile.MapperVersion),
		RangeStart: hex.EncodeToString(profile.Range.Start[:]),
		RangeEnd:   hex.EncodeToString(profile.Range.End.Point[:]), RangeEndMax: profile.Range.End.Max,
	})
}

// MarshalJSON emits the same strict grammar stored in the SQL
// catalog so orchestrators can retain an exact restart identity.
func (identity ReplicatedApplyIdentity) MarshalJSON() ([]byte, error) {
	return replicatedApplyMetaFromIdentity(identity).MarshalJSON()
}

// UnmarshalJSON decodes the strict retained grammar. Base-binding-dependent
// profile validation occurs in the exact open API before recovery.
func (identity *ReplicatedApplyIdentity) UnmarshalJSON(data []byte) error {
	var meta replicatedApplyMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return err
	}
	*identity = meta.identity()
	return nil
}

func (profile *ReplicatedPlacementProfile) UnmarshalJSON(data []byte) error {
	var decoded ReplicatedPlacementProfile
	present := make(map[string]bool, 7)
	err := decodeCatalogObject(data, "replicated placement", func(name string, d *json.Decoder) error {
		present[name] = true
		switch name {
		case "format":
			return decodeRequiredCatalogUint16(
				d, "replicated placement format", &decoded.Format,
			)
		case "shard_key":
			return d.Decode(&decoded.ShardKey)
		case "tuple_version":
			return d.Decode(&decoded.TupleVersion)
		case "mapper_version":
			return d.Decode(&decoded.MapperVersion)
		case "range_start":
			return decodeReplicatedPlacementPoint(d, "range start", &decoded.Range.Start)
		case "range_end":
			return decodeReplicatedPlacementPoint(d, "range end", &decoded.Range.End.Point)
		case "range_end_max":
			return d.Decode(&decoded.Range.End.Max)
		default:
			return unknownCatalogMember("replicated placement", name)
		}
	})
	if err != nil {
		return err
	}
	for _, name := range []string{
		"format", "shard_key", "tuple_version", "mapper_version",
		"range_start", "range_end", "range_end_max",
	} {
		if !present[name] {
			return fmt.Errorf("vibedb: replicated placement is missing member %q", name)
		}
	}
	if decoded.Range.End.Max && decoded.Range.End.Point != (distribution.KeyspacePoint{}) {
		return errors.New("vibedb: replicated placement max range end must use the zero point")
	}
	if err := validateReplicatedPlacementProfileGrammar(decoded); err != nil {
		return err
	}
	*profile = decoded
	return nil
}

func decodeReplicatedPlacementPoint(
	decoder *json.Decoder,
	label string,
	dst *distribution.KeyspacePoint,
) error {
	var value string
	if err := decoder.Decode(&value); err != nil {
		return err
	}
	if len(value) != distribution.KeyspaceWidth*2 || value != strings.ToLower(value) {
		return fmt.Errorf("vibedb: replicated placement %s must be lowercase 8-byte hexadecimal", label)
	}
	if _, err := hex.Decode(dst[:], []byte(value)); err != nil {
		return fmt.Errorf("vibedb: replicated placement %s: %w", label, err)
	}
	return nil
}

func (m *replicatedApplyMeta) UnmarshalJSON(data []byte) error {
	var decoded replicatedApplyMeta
	present := make(map[string]bool, 11)
	err := decodeCatalogObject(data, "replicated apply", func(name string, d *json.Decoder) error {
		present[name] = true
		switch name {
		case "format":
			return decodeRequiredCatalogUint16(
				d, "replicated apply format", &decoded.Format,
			)
		case "storage":
			return d.Decode(&decoded.Storage)
		case "validation_profile":
			return d.Decode(&decoded.ValidationProfile)
		case "validation_digest":
			var value string
			if err := d.Decode(&value); err != nil {
				return err
			}
			if len(value) != sha256.Size*2 || value != strings.ToLower(value) {
				return errors.New("vibedb: replicated apply validation digest must be lowercase SHA-256 hexadecimal")
			}
			if _, err := hex.Decode(decoded.ValidationDigest[:], []byte(value)); err != nil {
				return fmt.Errorf("vibedb: replicated apply validation digest: %w", err)
			}
			return nil
		case "system_limits":
			return d.Decode(&decoded.SystemLimits)
		case "max_completions":
			return d.Decode(&decoded.MaxCompletions)
		case "txn_max_collections":
			return d.Decode(&decoded.TxnMaxCollections)
		case "txn_max_documents":
			return d.Decode(&decoded.TxnMaxDocuments)
		case "txn_max_bytes":
			return d.Decode(&decoded.TxnMaxBytes)
		case "placement":
			return d.Decode(&decoded.Placement)
		case "sidecars":
			return d.Decode(&decoded.Sidecars)
		default:
			return unknownCatalogMember("replicated apply", name)
		}
	})
	if err != nil {
		return err
	}
	for _, name := range []string{
		"format", "storage", "validation_profile", "validation_digest",
		"system_limits", "max_completions", "txn_max_collections",
		"txn_max_documents", "txn_max_bytes", "placement", "sidecars",
	} {
		if !present[name] {
			return fmt.Errorf("vibedb: replicated apply is missing member %q", name)
		}
	}
	if decoded.Format != ReplicatedApplyFormat {
		return fmt.Errorf("vibedb: unsupported replicated apply format %d", decoded.Format)
	}
	if decoded.ValidationProfile != uint8(replicatedstate.ValidationDeterministicMutation) {
		return errors.New("vibedb: replicated apply has the wrong validation profile")
	}
	*m = decoded
	return nil
}
