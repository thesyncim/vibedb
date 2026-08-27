package driver

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/orderedkey"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/rangesplit"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/x/byteview"
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
	ErrReplicatedApplyBasePending   = errors.New("vibedb: replicated child snapshot base is not installed")
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
	MaxSessions uint64
	RetryWindow uint16
	TxnLimits   durable.TxnLimits
	Placement   ReplicatedPlacementProfile
	// RequestLedgerCapacityBytes is the exact resident-plus-future byte budget
	// for a dedicated request-ledger group. All request-ledger fields must be
	// zero to keep the ordinary data-group path disabled.
	RequestLedgerCapacityBytes uint64
	// RequestLedgerCleanupReserveBytes prevents new requests from consuming the
	// bytes required for ACK, recovery, and bounded garbage collection.
	RequestLedgerCleanupReserveBytes uint64
	// RequestLedgerRangeStart and RequestLedgerRangeEnd are a half-open SHA-256
	// key interval. An all-zero end is the canonical unbounded upper endpoint.
	RequestLedgerRangeStart [sha256.Size]byte
	RequestLedgerRangeEnd   [sha256.Size]byte
	// RequestLedgerRangeIdentity fences stale routing against a different
	// immutable ledger-range generation.
	RequestLedgerRangeIdentity [sha256.Size]byte
}

// ReplicatedApplyIdentity is the complete retained identity of the private
// state-machine participant. Storage is random local namespace identity; the
// remaining fields freeze the portable validation and bounded apply profile.
// Exact restart must retain this value separately from the base SQL/WAL binding.
type ReplicatedApplyIdentity struct {
	Format                           uint16
	Storage                          string
	CaptureStorage                   string
	ValidationProfile                uint8
	ValidationDigest                 [32]byte
	SystemLimits                     ReplicatedShardStoreLimits
	CaptureLimits                    ReplicatedShardStoreLimits
	MaxSessions                      uint64
	RetryWindow                      uint16
	TxnLimits                        durable.TxnLimits
	Placement                        ReplicatedPlacementProfile
	RequestLedgerCapacityBytes       uint64
	RequestLedgerCleanupReserveBytes uint64
	RequestLedgerRangeStart          [sha256.Size]byte
	RequestLedgerRangeEnd            [sha256.Size]byte
	RequestLedgerRangeIdentity       [sha256.Size]byte
	Sidecars                         ReplicatedApplySidecarProfile
}

// ReplicatedApplyCapacityProfile is the detached, constant-size apply cut used
// to qualify a fixed no-compaction WAL. Binding is the exact catalog-owned WAL
// and authority lineage; ApplyFormat freezes the result/session grammar; all
// counters come from the same locked machine publication.
// RelationManifestDigest is the portable logical digest: it excludes every
// replica-local storage name and is safe to compare during RF3 registration.
// This profile does not reserve physical storage or grant serving authority.
type ReplicatedApplyCapacityProfile struct {
	Binding                ReplicatedShardStoreBinding
	ApplyFormat            uint16
	RelationManifestDigest [sha256.Size]byte
	MaxSessions            uint64
	RetryWindow            uint16
	Initialized            bool
	Applied                uint64
	CheckpointApplied      uint64
	SessionCount           uint64
	SessionSlotCount       uint64
	SessionEpochHighWater  uint64
}

// ReplicatedApply is the opaque, singleton trusted apply claim over one bound
// SQL root. Its implementation deliberately does not expose the underlying
// durable collections, transaction log, or replicated-state Machine.
type ReplicatedApply struct {
	owner                *dbConnector
	database             *database
	machine              *replicatedstate.Machine
	table                *table
	identity             ReplicatedApplyIdentity
	rangeSplitCapture    *rangesplit.SourceCapture
	closed               bool
	attemptGeneration    uint64
	attemptActive        bool
	attemptBatch         bool
	attemptKeys          replicatedAttemptBinaryKeys
	walBaseCaptureActive bool
	walBaseSelectActive  bool
	walBaseSelectPending bool
	walBasePending       raftstore.GenerationActivationIdentity
	// exclusiveConnector is set only by a no-copy child-stage handoff. It keeps
	// SQL sessions fenced until raftmember atomically retires the connector or
	// the apply claim is explicitly closed.
	exclusiveConnector bool
	// activationBasePending is the exact compact snapshot-base identity that
	// must be installed before a no-copy child claim may propose, apply, look
	// up completions, or export another snapshot.
	activationBasePending [sha256.Size]byte
}

// CompletionLookupWorkspace owns one reusable exact completion snapshot for a
// serialized ReplicatedApply batch. The zero value is ready for use. It is
// single-consumer, must not be copied, and retains only bounded snapshot
// scratch between batches.
type CompletionLookupWorkspace struct {
	machine replicatedstate.CompletionLookupWorkspace
	owner   *ReplicatedApply
}

// replicatedAttemptBinaryKeys is stable storage for the interface value passed
// into txnclock. Pointing the interface at this claim-owned adapter keeps the
// synchronous borrowed-key call allocation-free. Keys is cleared immediately
// after publication and is never retained by the clock.
type replicatedAttemptBinaryKeys struct {
	keys replicatedstate.AttemptedMutationKeys
}

func (keys *replicatedAttemptBinaryKeys) Len() int { return keys.keys.Len() }
func (keys *replicatedAttemptBinaryKeys) Key(index int) []byte {
	return keys.keys.Key(index)
}

var _ raftmodel.StateMachine = (*ReplicatedApply)(nil)
var _ raftmodel.NormalBatchStateMachine = (*ReplicatedApply)(nil)

type replicatedApplyMeta struct {
	Format                           uint16
	Storage                          string
	CaptureStorage                   string
	ValidationProfile                uint8
	ValidationDigest                 [32]byte
	SystemLimits                     ReplicatedShardStoreLimits
	CaptureLimits                    ReplicatedShardStoreLimits
	MaxSessions                      uint64
	RetryWindow                      uint16
	TxnMaxCollections                int
	TxnMaxDocuments                  int
	TxnMaxBytes                      int64
	Placement                        ReplicatedPlacementProfile
	RequestLedgerCapacityBytes       uint64
	RequestLedgerCleanupReserveBytes uint64
	RequestLedgerRangeStart          [sha256.Size]byte
	RequestLedgerRangeEnd            [sha256.Size]byte
	RequestLedgerRangeIdentity       [sha256.Size]byte
	Sidecars                         ReplicatedApplySidecarProfile
}

// OpenReplicatedShardStoreWithApply opens an activated root only when both the
// retained base SQL/WAL identity and the complete private apply identity match.
// Both comparisons occur before namespace or transaction recovery.
func OpenReplicatedShardStoreWithApply(
	path string,
	expected ReplicatedShardStoreIdentity,
	expectedApply ReplicatedApplyIdentity,
	opening ...ReplicatedOpenOptions,
) (*Database, error) {
	openOptions, err := replicatedOpeningOptions(opening)
	if err != nil {
		return nil, err
	}
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
	if schemaRecovery, recoveryErr := replicatedSchemaActivationMatchesCatalog(absolute); recoveryErr != nil {
		return nil, recoveryErr
	} else if schemaRecovery {
		return OpenReplicatedShardStoreWithSchemaTransition(path, expected, expectedApply, openOptions)
	}
	if _, err := os.Stat(absolute); err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("%w: %s", ErrReplicatedApplyUninitialized, absolute)
		}
		return nil, err
	}
	core, err := openDatabaseWithShardStorePolicy(path, nil, shardStoreOpenPolicy{
		mode:                    shardStoreOpenReplicatedApplyExisting,
		openOptions:             openOptions,
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

// OpenReplicatedShardStoreForChildStageResume recovers only the non-serving
// split-child activation interval after the apply descriptor is durable. It is
// the sole SQL open policy allowed to admit a missing checkpoint certificate
// beside an exact clean staged user image. NewSession and ordinary apply stay
// fenced until OpenReplicatedChildStage reclaims a sealed cursor and completes
// or reuses the authenticated seed transition.
func OpenReplicatedShardStoreForChildStageResume(
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
		mode:                      shardStoreOpenReplicatedChildStageResume,
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

func (d *database) replicatedCapturePath(meta *replicatedApplyMeta) string {
	if d == nil || meta == nil {
		return ""
	}
	return filepath.Join(d.dataDir, meta.CaptureStorage+".vjc")
}

func replicatedApplySystemLimits(retryWindow uint16) ReplicatedShardStoreLimits {
	return replicatedApplySystemLimitsForLedger(retryWindow, false)
}

func replicatedApplySystemLimitsForLedger(retryWindow uint16, ledger bool, transactionDocuments ...int) ReplicatedShardStoreLimits {
	required, ok := replicatedstate.RequiredSystemCollectionLimits(retryWindow, ledger)
	if len(transactionDocuments) == 1 {
		required, ok = replicatedstate.RequiredTransactionSystemCollectionLimits(retryWindow, ledger, transactionDocuments[0])
	} else if len(transactionDocuments) != 0 {
		return ReplicatedShardStoreLimits{}
	}
	if !ok {
		return ReplicatedShardStoreLimits{}
	}
	return ReplicatedShardStoreLimits{
		MaxKeyBytes: required.MaxKeyBytes, MaxDocumentBytes: required.MaxDocumentBytes,
		MaxBatchDocuments: required.MaxDistinctMutations,
		MaxBatchBytes:     required.MaxBatchBytes,
	}
}

func replicatedApplyTransactionSystemLimits(
	identity ReplicatedShardStoreIdentity, retryWindow uint16, ledger bool, transactionDocuments int,
) ReplicatedShardStoreLimits {
	documents := 0
	for ordinal := 0; ordinal < int(identity.RelationCount); ordinal++ {
		documents += identity.Relations[ordinal].Limits.MaxBatchDocuments
	}
	// The system participant stores one intent per distinct relation key,
	// one packed row per relation, two controls, a coordinator payload, an
	// initial manifest pack, and state. Do not reserve unrelated collections'
	// transaction slots in every hidden collection.
	documents += int(identity.RelationCount) + distributedtxn.MaxManifestSegmentsPerCommand + 4
	minimum, ok := replicatedstate.RequiredSystemCollectionLimits(retryWindow, ledger)
	if !ok {
		return ReplicatedShardStoreLimits{}
	}
	documents = min(transactionDocuments, max(documents, minimum.MaxDistinctMutations))
	return replicatedApplySystemLimitsForLedger(retryWindow, ledger, documents)
}

func replicatedApplyDurableOptions(limits ReplicatedShardStoreLimits) durable.Options {
	sidecars := canonicalReplicatedApplySidecarsForLimits(limits)
	return durable.Options{
		Durability:                 durable.DurabilitySync,
		OpaqueValues:               true,
		MaxKeyBytes:                limits.MaxKeyBytes,
		MaxDocumentBytes:           limits.MaxDocumentBytes,
		MaxBatchDocuments:          limits.MaxBatchDocuments,
		MaxBatchBytes:              limits.MaxBatchBytes,
		SealedRecoveryJournalBytes: sidecars.SystemRecoveryJournalBytes,
	}
}

func replicatedCaptureLimits(base ReplicatedShardStoreIdentity) (ReplicatedShardStoreLimits, error) {
	maximum, err := rangesplit.MaximumSourceCaptureRecordBytes(
		base.UserLimits.MaxBatchDocuments, base.UserLimits.MaxKeyBytes,
		base.UserLimits.MaxDocumentBytes, base.UserLimits.MaxBatchBytes,
	)
	if err != nil || maximum > math.MaxInt-8 {
		return ReplicatedShardStoreLimits{}, errors.Join(err, ErrReplicatedApplyMismatch)
	}
	return ReplicatedShardStoreLimits{
		MaxKeyBytes: 8, MaxDocumentBytes: maximum,
		MaxBatchDocuments: 1, MaxBatchBytes: maximum + 8,
	}, nil
}

func replicatedCaptureDurableOptions(limits ReplicatedShardStoreLimits) durable.Options {
	return durable.Options{
		Durability: durable.DurabilitySync, OpaqueValues: true,
		MaxKeyBytes: limits.MaxKeyBytes, MaxDocumentBytes: limits.MaxDocumentBytes,
		MaxBatchDocuments: limits.MaxBatchDocuments, MaxBatchBytes: limits.MaxBatchBytes,
	}
}

// ReplicatedApplyTransactionByteFloor returns the exact frozen transaction
// byte ceiling required to admit one worst-case base/index apply together with
// its source-split capture record and replicated system state. Operators use
// this before first activation; the retained identity cannot grow the limit on
// a later split attempt. This preparation helper describes data-only apply;
// OpenReplicatedApply additionally checks the larger exact control-record floor
// when the supplied options enable a request ledger.
func ReplicatedApplyTransactionByteFloor(
	identity ReplicatedShardStoreIdentity,
	retryWindow uint16,
) (int64, error) {
	return replicatedApplyTransactionByteFloor(identity, retryWindow, false)
}

func replicatedApplyTransactionByteFloor(
	identity ReplicatedShardStoreIdentity,
	retryWindow uint16,
	ledger bool,
	transactionDocuments ...int,
) (int64, error) {
	if retryWindow == 0 || retryWindow > replicatedstate.MaxSessionRetryWindow ||
		validateReplicatedShardStoreIdentity(identity) != nil {
		return 0, ErrReplicatedApplyMismatch
	}
	relationBytes := int64(0)
	for ordinal := 0; ordinal < int(identity.RelationCount); ordinal++ {
		limit := int64(identity.Relations[ordinal].Limits.MaxBatchBytes)
		if limit <= 0 || relationBytes > math.MaxInt64-limit {
			return 0, ErrReplicatedApplyMismatch
		}
		relationBytes = min(int64(replication.MaxCommandBytes), relationBytes+limit)
	}
	documents := defaultDriverTxnLimits().MaxDocuments
	if len(transactionDocuments) == 1 {
		documents = transactionDocuments[0]
	} else if len(transactionDocuments) != 0 {
		return 0, ErrReplicatedApplyMismatch
	}
	systemBytes := int64(replicatedApplyTransactionSystemLimits(identity, retryWindow, ledger, documents).MaxBatchBytes)
	capture, err := replicatedCaptureLimits(identity)
	if err != nil || systemBytes <= 0 || capture.MaxBatchBytes <= 0 ||
		relationBytes > math.MaxInt64-systemBytes ||
		relationBytes+systemBytes > math.MaxInt64-int64(capture.MaxBatchBytes) {
		return 0, errors.Join(err, ErrReplicatedApplyMismatch)
	}
	return relationBytes + systemBytes + int64(capture.MaxBatchBytes), nil
}

func validateReplicatedApplyCollection(
	collection *durable.Collection,
	limits ReplicatedShardStoreLimits,
	sidecars ReplicatedApplySidecarProfile,
) error {
	if collection == nil || !collection.HasOpaqueValues() ||
		collection.HasSchema() || collection.HasIndexes() ||
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
	if core.replicatedSeedPending {
		return nil, ReplicatedApplyIdentity{}, ErrReplicatedChildStageBusy
	}
	if core.replicatedApplyClaim != nil {
		return nil, ReplicatedApplyIdentity{}, ErrReplicatedApplyBusy
	}
	if core.catalog.ReplicatedShardStore == nil ||
		!core.catalog.ReplicatedShardStore.Equal(expected) {
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
	if core.checkpointGroup == nil {
		if err := core.txnLog.EnsureMinted(); err != nil {
			return nil, ReplicatedApplyIdentity{}, fmt.Errorf(
				"vibedb: qualify replicated transaction marker: %w", err,
			)
		}
	}

	t := core.tables[expected.UserTable]
	if t == nil || t.collection == nil {
		return nil, ReplicatedApplyIdentity{}, fmt.Errorf(
			"%w: replicated user collection is unavailable", ErrReplicatedApplyMismatch,
		)
	}
	if core.catalog.ReplicatedApply == nil {
		for ordinal := 0; ordinal < int(expected.RelationCount); ordinal++ {
			relation := expected.Relations[ordinal]
			table := core.tables[relation.Table]
			if table == nil || table.collection == nil || table.collection.Len() != 0 {
				return nil, ReplicatedApplyIdentity{}, fmt.Errorf(
					"%w: first activation requires empty relation %q",
					ErrReplicatedApplyMismatch, relation.Table,
				)
			}
		}
	}
	identity, err := core.prepareReplicatedApplyStorageLocked(expected, options, persist)
	if err != nil {
		return nil, identity, err
	}
	if core.replicatedApplyCollection == nil {
		return nil, identity, fmt.Errorf(
			"%w: hidden collection is not open", ErrReplicatedApplyMismatch,
		)
	}
	if core.replicatedCaptureCollection == nil {
		return nil, identity, fmt.Errorf(
			"%w: hidden capture collection is not open", ErrReplicatedApplyMismatch,
		)
	}
	groupMembers, err := replicatedApplyCheckpointMembers(expected, core)
	if err != nil {
		return nil, identity, err
	}
	if core.checkpointGroup == nil {
		core.checkpointGroup, err = durable.NewCheckpointGroup(
			core.txnLog, groupMembers, durable.CheckpointGroupOptions{},
		)
		if err != nil {
			return nil, identity, fmt.Errorf(
				"vibedb: activate replicated checkpoint group: %w", err,
			)
		}
	} else if !core.checkpointGroup.Owns(groupMembers) {
		return nil, identity, fmt.Errorf(
			"%w: checkpoint-group membership", ErrReplicatedApplyMismatch,
		)
	}
	claim := &ReplicatedApply{
		owner: d.connector, database: core, table: t, identity: identity,
	}
	relations, err := replicatedApplyRelations(expected, identity, core, claim)
	if err != nil {
		return nil, identity, err
	}
	initialBinding := replicatedStateBindingAt(expected, options.Placement.Range)
	machine, err := replicatedstate.OpenBundle(
		initialBinding, bootstrap,
		replicatedstate.CollectionTarget{
			Collection: core.replicatedApplyCollection,
			Validation: replicatedstate.ValidationOpaqueBinary,
			Limits:     replicatedStateCollectionLimits(identity.SystemLimits),
		},
		relations,
		core.txnLog,
		replicatedstate.Options{
			TxnLimits: identity.TxnLimits, MaxSessions: identity.MaxSessions,
			RetryWindow: identity.RetryWindow, CheckpointGroup: core.checkpointGroup,
			RequestLedgerCapacityBytes:       identity.RequestLedgerCapacityBytes,
			RequestLedgerCleanupReserveBytes: identity.RequestLedgerCleanupReserveBytes,
			RequestLedgerRange: replicatedstate.RequestLedgerRange{
				Start:    requestledger.LedgerHome(identity.RequestLedgerRangeStart),
				End:      requestledger.LedgerHome(identity.RequestLedgerRangeEnd),
				Identity: requestledger.Digest(identity.RequestLedgerRangeIdentity),
			},
			TransitionCaptureTarget: replicatedstate.TransitionCaptureTarget{
				Name:       replicatedstate.TransitionCaptureCollectionName,
				Collection: core.replicatedCaptureCollection,
			},
			TransitionCaptureFactory: func(activation replicatedstate.SplitCaptureActivation) (replicatedstate.TransitionCapture, error) {
				partitioner, openErr := rangesplit.OpenPortablePartitioner(activation.Command.Spec)
				if openErr != nil || partitioner.Digest() != activation.Command.PartitionerDigest {
					return nil, errors.Join(openErr, replicatedstate.ErrSplitCaptureActivation)
				}
				capture, captureErr := rangesplit.NewSourceCapture(
					partitioner, replicatedstate.TransitionCaptureCollectionName,
					core.replicatedCaptureCollection,
				)
				if captureErr == nil {
					claim.rangeSplitCapture = capture
				}
				return capture, captureErr
			},
			SchemaTransition:          core.schemaTransition,
			SchemaMembershipWitness:   core.schemaMembership,
			SchemaAuthorizationDigest: core.schemaAuthorization,
			SchemaCatalogCASDigest:    core.schemaCatalogCAS,
			SchemaSourceRecovery:      core.schemaSourceRecovery,
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

// replicatedApplyLocalIndexes converts the cold SQL catalog into the native
// exact-index manifest consumed by replicatedstate.Open. No name or pointer is
// resolved while applying a command: Open canonicalizes these definitions,
// authenticates them in the schema-generation contract, and binds them to the
// already-open collection handle.
func replicatedApplyLocalIndexes(t *table) []store.IndexDefinition {
	if t == nil || t.meta == nil || len(t.meta.Indexes) == 0 {
		return nil
	}
	result := make([]store.IndexDefinition, len(t.meta.Indexes))
	for i := range t.meta.Indexes {
		result[i] = store.IndexDefinition{
			Name:  strings.Clone(t.meta.Indexes[i].Name),
			Paths: append([]string(nil), t.meta.Indexes[i].Paths...),
		}
	}
	return result
}

func replicatedApplyRelations(
	base ReplicatedShardStoreIdentity,
	apply ReplicatedApplyIdentity,
	database *database,
	claim *ReplicatedApply,
) ([]replicatedstate.RelationCollection, error) {
	baseTable := database.tables[base.UserTable]
	if baseTable == nil || baseTable.collection == nil {
		return nil, ErrReplicatedApplyMismatch
	}
	result, err := replicatedRelationSchemas(base, apply, replicatedApplyLocalIndexes(baseTable))
	if err != nil {
		return nil, err
	}
	for ordinal := range result {
		relation := base.Relations[ordinal]
		table := database.tables[relation.Table]
		if table == nil || table.collection == nil {
			return nil, fmt.Errorf(
				"%w: relation %q is unavailable", ErrReplicatedApplyMismatch, relation.Table,
			)
		}
		spec := &result[ordinal]
		spec.Target.Collection = table.collection
		switch relation.Kind {
		case ReplicatedShardRelationJSON:
			spec.Target.Validator = newReplicatedSQLMutationValidator(base, baseTable, apply.Placement)
			spec.Target.ObserveMutationAttempt = claim.observeMutationAttempt
		case ReplicatedShardRelationGlobalIndex:
			spec.Target.Validator = replicatedGlobalIndexMutationValidator{
				relation: relation, placement: apply.Placement.Range,
			}
		default:
			return nil, ErrReplicatedApplyMismatch
		}
	}
	return result, nil
}

// replicatedApplyCheckpointMembers returns the exact dense ownership set used
// by apply, snapshot capture, receiver activation, and restart. Keeping this
// construction in one place prevents a newly added relation from escaping the
// checkpoint cut while still preserving the singleton member order.
func replicatedApplyCheckpointMembers(
	base ReplicatedShardStoreIdentity,
	database *database,
) ([]durable.NamedCollection, error) {
	if database == nil || database.replicatedApplyCollection == nil ||
		database.replicatedCaptureCollection == nil || base.RelationCount == 0 ||
		len(base.Relations) != int(base.RelationCount) {
		return nil, ErrReplicatedApplyMismatch
	}
	members := make([]durable.NamedCollection, 0, int(base.RelationCount)+2)
	members = append(members, durable.NamedCollection{
		Name: replicatedstate.SystemCollectionName, Collection: database.replicatedApplyCollection,
	})
	for ordinal := range base.Relations {
		relation := base.Relations[ordinal]
		table := database.tables[relation.Table]
		if table == nil || table.collection == nil {
			return nil, fmt.Errorf(
				"%w: relation %q is unavailable", ErrReplicatedApplyMismatch, relation.Table,
			)
		}
		members = append(members, durable.NamedCollection{
			Name: relation.Table, Collection: table.collection,
		})
	}
	return append(members, durable.NamedCollection{
		Name:       replicatedstate.TransitionCaptureCollectionName,
		Collection: database.replicatedCaptureCollection,
	}), nil
}

var replicatedGlobalIndexValidationDomain = []byte(
	"vibedb/sql/replicated-global-index-validation\x00",
)

var replicatedRelationApplyManifestDomain = []byte(
	"vibedb/sql/replicated-relation-apply-manifest\x00",
)

// replicatedRelationApplyManifestDigest is the portable deterministic schema
// contract shared by every replica. The retained catalog manifest separately
// authenticates each replica's local storage identities; those physical names
// must never enter replicated results, data-chain witnesses, or snapshot
// identities because healthy replicas necessarily use different files.
func replicatedRelationApplyManifestDigest(
	identity ReplicatedShardStoreIdentity,
) [sha256.Size]byte {
	h := sha256.New()
	_, _ = h.Write(replicatedRelationApplyManifestDomain)
	writeReplicatedRelationDescriptors(h, identity, false)
	var result [sha256.Size]byte
	_ = h.Sum(result[:0])
	return result
}

func replicatedGlobalIndexValidationDigest(
	base ReplicatedShardStoreIdentity,
	relation ReplicatedShardRelationIdentity,
	logicalManifest [sha256.Size]byte,
) [sha256.Size]byte {
	h := sha256.New()
	_, _ = h.Write(replicatedGlobalIndexValidationDomain)
	_, _ = h.Write(logicalManifest[:])
	var fixed [40]byte
	binary.LittleEndian.PutUint64(fixed[0:8], base.RelationSchemaGeneration)
	binary.LittleEndian.PutUint16(fixed[8:10], relation.Relation)
	fixed[10] = relation.LocatorCount
	if relation.Unique {
		fixed[11] = 1
	}
	fixed[12] = byte(relation.KeyEncoding)
	fixed[13] = relation.KeyArity
	fixed[14] = relation.BucketBits
	binary.LittleEndian.PutUint64(fixed[16:24], relation.IndexID)
	binary.LittleEndian.PutUint64(fixed[24:32], relation.Incarnation)
	binary.LittleEndian.PutUint32(fixed[32:36], uint32(relation.TupleVersion))
	binary.LittleEndian.PutUint32(fixed[36:40], uint32(relation.MapperVersion))
	_, _ = h.Write(fixed[:])
	var result [sha256.Size]byte
	_ = h.Sum(result[:0])
	return result
}

type replicatedGlobalIndexMutationValidator struct {
	relation  ReplicatedShardRelationIdentity
	placement distribution.KeyRange
}

func (v replicatedGlobalIndexMutationValidator) ValidatePut(
	key, _ []byte,
) replicatedstate.MutationValidation {
	point, ok := v.relation.GlobalIndexStorageKeyPoint(key)
	if !ok {
		return replicatedstate.MutationValidationInvalid
	}
	if !v.placement.Contains(point) {
		return replicatedstate.MutationValidationWrongShard
	}
	return replicatedstate.MutationValidationAccept
}

func (v replicatedGlobalIndexMutationValidator) ValidateDelete(
	key, _ []byte, _ bool,
) replicatedstate.MutationValidation {
	return v.ValidatePut(key, nil)
}

func (v replicatedGlobalIndexMutationValidator) ValidatePutOwnership(
	key, _ []byte,
	owned distribution.KeyRange,
) replicatedstate.MutationValidation {
	point, ok := v.relation.GlobalIndexStorageKeyPoint(key)
	if !ok {
		return replicatedstate.MutationValidationInvalid
	}
	if !owned.Contains(point) {
		return replicatedstate.MutationValidationWrongShard
	}
	return replicatedstate.MutationValidationAccept
}

func (v replicatedGlobalIndexMutationValidator) ValidateDeleteOwnership(
	key, _ []byte,
	_ bool,
	owned distribution.KeyRange,
) replicatedstate.MutationValidation {
	return v.ValidatePutOwnership(key, nil, owned)
}

func (v replicatedGlobalIndexMutationValidator) ValidatePointOwnership(
	key []byte,
	owned distribution.KeyRange,
) replicatedstate.MutationValidation {
	return v.ValidatePutOwnership(key, nil, owned)
}

func (v replicatedGlobalIndexMutationValidator) GlobalIndexPlacementPoint(
	key []byte,
) (distribution.KeyspacePoint, bool) {
	return v.relation.GlobalIndexStorageKeyPoint(key)
}

func (v replicatedGlobalIndexMutationValidator) GlobalIndexPlacementRange() distribution.KeyRange {
	return v.placement
}

// prepareReplicatedApplyStorageLocked makes the hidden participant and its
// descriptor durable, or validates the exact already-published participant.
// The caller owns database.mu and is responsible for deciding whether a
// non-empty user collection is legal before calling this common publication
// boundary.
func (d *database) prepareReplicatedApplyStorageLocked(
	expected ReplicatedShardStoreIdentity,
	options ReplicatedApplyOptions,
	persist func(*database) (bool, error),
) (ReplicatedApplyIdentity, error) {
	if d.catalog.ReplicatedApply != nil {
		if !replicatedApplyMetaMatchesOptions(d.catalog.ReplicatedApply, expected, options) {
			return ReplicatedApplyIdentity{}, ErrReplicatedApplyMismatch
		}
		identity := d.catalog.ReplicatedApply.identity()
		if d.replicatedApplyCollection == nil || d.replicatedCaptureCollection == nil {
			return identity, fmt.Errorf(
				"%w: hidden collection is not open", ErrReplicatedApplyMismatch,
			)
		}
		return identity, nil
	}

	// A prior definite descriptor failure may have retained an unpublished
	// candidate because detaching it from the transaction log could not yet
	// complete. Retry ownership discharge and cleanup before allocating a new
	// identity; never overwrite the only pointers to a still-registered store.
	if d.replicatedApplyCollection != nil || d.replicatedApplyFile != nil ||
		d.replicatedCaptureCollection != nil || d.replicatedCaptureFile != nil {
		if d.replicatedApplyCollection == nil || d.replicatedApplyFile == nil ||
			d.replicatedCaptureCollection == nil || d.replicatedCaptureFile == nil {
			return ReplicatedApplyIdentity{}, errors.New(
				"vibedb: incomplete unpublished replicated apply ownership",
			)
		}
		if err := errors.Join(
			d.detachReplicatedApplyCollection(d.replicatedApplyCollection),
			d.detachReplicatedApplyCollection(d.replicatedCaptureCollection),
		); err != nil {
			return ReplicatedApplyIdentity{}, fmt.Errorf(
				"vibedb: retry unpublished replicated apply detach: %w", err,
			)
		}
		path, capturePath := d.replicatedApplyFile.Name(), d.replicatedCaptureFile.Name()
		cleanupErr := errors.Join(d.discardUnpublishedStorageLocked(
			d.replicatedApplyCollection, d.replicatedApplyFile, path,
		), d.discardUnpublishedStorageLocked(
			d.replicatedCaptureCollection, d.replicatedCaptureFile, capturePath,
		))
		d.replicatedApplyCollection = nil
		d.replicatedApplyFile = nil
		d.replicatedCaptureCollection = nil
		d.replicatedCaptureFile = nil
		if cleanupErr != nil {
			return ReplicatedApplyIdentity{}, fmt.Errorf(
				"vibedb: retry unpublished replicated apply cleanup: %w", cleanupErr,
			)
		}
	}
	// Generic detach recycles txn.vtm after discharging its conditional window.
	// That clean recycle retains a nonzero monotonic BaseSequence. Reset it before
	// publishing a new apply identity: the SQL descriptor is durable before the
	// checkpoint certificate, and recovery must be able to distinguish that
	// legitimate activation seam from deletion of an already-live certificate.
	if err := d.txnLog.ResetDischargedForCheckpointGroupActivation(); err != nil {
		return ReplicatedApplyIdentity{}, fmt.Errorf(
			"vibedb: prepare replicated checkpoint activation baseline: %w", err,
		)
	}

	reserved := d.catalog.ReplicatedChildApply
	if reserved != nil && !replicatedApplyMetaMatchesOptions(reserved, expected, options) {
		return ReplicatedApplyIdentity{}, ErrReplicatedApplyMismatch
	}
	identity, err := d.createReplicatedApplyStorageLocked(expected, options, reserved)
	if err != nil {
		return ReplicatedApplyIdentity{}, err
	}
	if reserved != nil && identity != reserved.identity() {
		return ReplicatedApplyIdentity{}, ErrReplicatedApplyMismatch
	}
	previousPending := d.catalogWritePending
	stored := replicatedApplyMetaFromIdentity(identity)
	d.catalog.ReplicatedApply = &stored
	d.catalog.ReplicatedChildApply = nil
	d.catalogWritePending = true
	var published bool
	if persist != nil {
		published, err = persist(d)
	} else {
		published, err = d.persistCatalogLocked()
	}
	if err == nil && !published {
		err = errors.New("catalog persistence returned without publication")
	}
	if err == nil {
		return identity, nil
	}
	publicationErr := fmt.Errorf("vibedb: publish replicated apply descriptor: %w", err)
	if published || errors.Is(err, durable.ErrCommitOutcomeUnknown) {
		d.catalogWritePending = !published
		return identity, publicationErr
	}
	d.catalog.ReplicatedApply = nil
	if reserved != nil {
		owned := *reserved
		d.catalog.ReplicatedChildApply = &owned
	}
	d.catalogWritePending = previousPending
	path := d.replicatedApplyPath(&stored)
	capturePath := d.replicatedCapturePath(&stored)
	detachErr := errors.Join(
		d.detachReplicatedApplyCollection(d.replicatedApplyCollection),
		d.detachReplicatedApplyCollection(d.replicatedCaptureCollection),
	)
	if detachErr != nil {
		// Keep the unpublished candidate owned by the database. Closing or
		// unlinking it while the transaction log still names the handle would
		// leave stale catalog scope; the detach failure already makes retry unsafe.
		return ReplicatedApplyIdentity{}, errors.Join(
			publicationErr,
			fmt.Errorf(
				"vibedb: detach unpublished replicated apply storage: %w", detachErr,
			),
		)
	}
	cleanupErr := errors.Join(d.discardUnpublishedStorageLocked(
		d.replicatedApplyCollection, d.replicatedApplyFile, path,
	), d.discardUnpublishedStorageLocked(
		d.replicatedCaptureCollection, d.replicatedCaptureFile, capturePath,
	))
	d.replicatedApplyCollection = nil
	d.replicatedApplyFile = nil
	d.replicatedCaptureCollection = nil
	d.replicatedCaptureFile = nil
	return ReplicatedApplyIdentity{}, errors.Join(publicationErr, cleanupErr)
}

func (d *database) createReplicatedApplyStorageLocked(
	base ReplicatedShardStoreIdentity,
	options ReplicatedApplyOptions,
	reserved *replicatedApplyMeta,
) (ReplicatedApplyIdentity, error) {
	if err := d.checkRetirementCapacityLocked(2); err != nil {
		return ReplicatedApplyIdentity{}, err
	}
	if err := d.ensureDataDir(); err != nil {
		return ReplicatedApplyIdentity{}, err
	}
	var storage, captureStorage string
	if reserved != nil {
		storage, captureStorage = reserved.Storage, reserved.CaptureStorage
		if validateReplicatedApplyMeta(reserved, &base) != nil ||
			!replicatedApplyMetaMatchesOptions(reserved, base, options) {
			return ReplicatedApplyIdentity{}, ErrReplicatedApplyMismatch
		}
	} else {
		var err error
		storage, err = d.newStorageIdentityLocked()
		if err != nil {
			return ReplicatedApplyIdentity{}, err
		}
		captureStorage, err = d.newStorageIdentityLocked()
		if err != nil || captureStorage == storage {
			return ReplicatedApplyIdentity{}, errors.Join(err, ErrReplicatedApplyMismatch)
		}
	}
	meta := newReplicatedApplyMeta(base, storage, captureStorage, options)
	path := d.replicatedApplyPath(&meta)
	file, err := createPublishableTableTemp(
		d.dataDir, "."+filepath.Base(path)+".tmp-",
	)
	if err != nil {
		return ReplicatedApplyIdentity{}, err
	}
	tmpPath := file.Name()
	collection, err := durable.Create(file, replicatedApplyDurableOptions(meta.SystemLimits))
	if err != nil {
		return ReplicatedApplyIdentity{}, errors.Join(err,
			d.discardUnpublishedStorageLocked(collection, file, tmpPath))
	}
	file, collection, err = d.publishTableStorageLocked(
		tmpPath, path, file, collection, replicatedApplyDurableOptions(meta.SystemLimits),
	)
	if err != nil {
		return ReplicatedApplyIdentity{}, fmt.Errorf(
			"vibedb: publish replicated apply storage: %w", err,
		)
	}
	if err := validateReplicatedApplyCollection(
		collection, meta.SystemLimits, meta.Sidecars,
	); err != nil {
		return ReplicatedApplyIdentity{}, errors.Join(err,
			d.discardUnpublishedStorageLocked(collection, file, path))
	}
	capturePath := d.replicatedCapturePath(&meta)
	captureFile, err := createPublishableTableTemp(
		d.dataDir, "."+filepath.Base(capturePath)+".tmp-",
	)
	if err != nil {
		return ReplicatedApplyIdentity{}, errors.Join(err,
			d.discardUnpublishedStorageLocked(collection, file, path))
	}
	captureTemp := captureFile.Name()
	capture, err := durable.Create(captureFile, replicatedCaptureDurableOptions(meta.CaptureLimits))
	if err != nil {
		return ReplicatedApplyIdentity{}, errors.Join(err,
			d.discardUnpublishedStorageLocked(capture, captureFile, captureTemp),
			d.discardUnpublishedStorageLocked(collection, file, path))
	}
	captureFile, capture, err = d.publishTableStorageLocked(
		captureTemp, capturePath, captureFile, capture,
		replicatedCaptureDurableOptions(meta.CaptureLimits),
	)
	if err != nil {
		return ReplicatedApplyIdentity{}, errors.Join(err,
			d.discardUnpublishedStorageLocked(collection, file, path))
	}
	if err = d.adoptReplicatedApplyCollection(collection); err != nil {
		return ReplicatedApplyIdentity{}, errors.Join(err,
			d.discardUnpublishedStorageLocked(capture, captureFile, capturePath),
			d.discardUnpublishedStorageLocked(collection, file, path))
	}
	if err = d.adoptReplicatedApplyCollection(capture); err != nil {
		if detachErr := d.detachReplicatedApplyCollection(collection); detachErr != nil {
			// Both fully published handles remain owned for the exact retry path.
			// Closing either while the first remains registered would violate the
			// transaction log's participant lifetime contract.
			d.replicatedApplyFile = file
			d.replicatedApplyCollection = collection
			d.replicatedCaptureFile = captureFile
			d.replicatedCaptureCollection = capture
			return ReplicatedApplyIdentity{}, errors.Join(err, fmt.Errorf(
				"vibedb: retain two-store apply candidate after detach: %w", detachErr,
			))
		}
		return ReplicatedApplyIdentity{}, errors.Join(err,
			d.discardUnpublishedStorageLocked(capture, captureFile, capturePath),
			d.discardUnpublishedStorageLocked(collection, file, path))
	}
	d.replicatedApplyFile = file
	d.replicatedApplyCollection = collection
	d.replicatedCaptureFile = captureFile
	d.replicatedCaptureCollection = capture
	return meta.identity(), nil
}

func (d *database) adoptReplicatedApplyCollection(collection *durable.Collection) error {
	if d != nil && d.adoptCollection != nil {
		return d.adoptCollection(collection)
	}
	return d.txnLog.AdoptCollection(collection)
}

func (d *database) detachReplicatedApplyCollection(collection *durable.Collection) error {
	if d != nil && d.detachCollection != nil {
		return d.detachCollection(collection)
	}
	return d.txnLog.DetachCollection(collection)
}

func replicatedStateBindingAt(
	identity ReplicatedShardStoreIdentity,
	owned distribution.KeyRange,
) replicatedstate.Binding {
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
		OwnedRange:             owned,
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
	// Machine apply is serial, but detached snapshot audits may share this
	// validator concurrently. The mutex gives the reusable owned scratch one
	// caller at a time; request-backed placement Scalars remain stack-local.
	mu            sync.Mutex
	primaryKey    string
	primary       vibejson.CompiledPointer
	maxKeyBytes   int
	placement     replicatedSQLPlacementValidator
	keyScratch    []byte
	decodeScratch []byte
}

type replicatedSQLPlacementValidator struct {
	mapper *distribution.NativeMapper
	target distribution.KeyRange
}

func newReplicatedSQLMutationValidator(
	identity ReplicatedShardStoreIdentity,
	table *table,
	profile ReplicatedPlacementProfile,
) *replicatedSQLMutationValidator {
	validator := &replicatedSQLMutationValidator{
		primaryKey: identity.UserPrimaryKey, primary: table.primary,
		maxKeyBytes: identity.UserLimits.MaxKeyBytes,
	}
	mapper := distribution.NewNativeMapper(1)
	validator.placement = replicatedSQLPlacementValidator{
		mapper: mapper, target: profile.Range,
	}
	return validator
}

func (v *replicatedSQLMutationValidator) ValidatePut(
	key, value []byte,
) replicatedstate.MutationValidation {
	v.mu.Lock()
	defer v.mu.Unlock()

	encoded, _, err := appendDocumentKey(
		v.keyScratch[:0], value, v.primaryKey, v.primary, v.maxKeyBytes,
	)
	v.keyScratch = encoded
	if errors.Is(err, durable.ErrKeyTooLarge) {
		return replicatedstate.MutationValidationTargetBound
	}
	if err != nil || !bytes.Equal(encoded, key) {
		return replicatedstate.MutationValidationInvalid
	}
	point, ok := v.pointForEncodedKeyLocked(encoded)
	if !ok {
		return replicatedstate.MutationValidationInvalid
	}
	if !v.placement.target.Contains(point) {
		return replicatedstate.MutationValidationWrongShard
	}
	return replicatedstate.MutationValidationAccept
}

func (v *replicatedSQLMutationValidator) ValidateDelete(
	key, current []byte,
	found bool,
) replicatedstate.MutationValidation {
	if found {
		return v.ValidatePut(key, current)
	}

	v.mu.Lock()
	defer v.mu.Unlock()

	point, ok := v.pointForEncodedKeyLocked(key)
	if !ok {
		return replicatedstate.MutationValidationInvalid
	}
	if !v.placement.target.Contains(point) {
		return replicatedstate.MutationValidationWrongShard
	}
	return replicatedstate.MutationValidationAccept
}

func (v *replicatedSQLMutationValidator) ValidatePutOwnership(
	key, value []byte,
	owned distribution.KeyRange,
) replicatedstate.MutationValidation {
	v.mu.Lock()
	defer v.mu.Unlock()
	encoded, _, err := appendDocumentKey(
		v.keyScratch[:0], value, v.primaryKey, v.primary, v.maxKeyBytes,
	)
	v.keyScratch = encoded
	if errors.Is(err, durable.ErrKeyTooLarge) {
		return replicatedstate.MutationValidationTargetBound
	}
	if err != nil || !bytes.Equal(encoded, key) {
		return replicatedstate.MutationValidationInvalid
	}
	return v.validateOwnedKeyLocked(key, owned)
}

func (v *replicatedSQLMutationValidator) ValidateDeleteOwnership(
	key, current []byte,
	found bool,
	owned distribution.KeyRange,
) replicatedstate.MutationValidation {
	if found {
		return v.ValidatePutOwnership(key, current, owned)
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.validateOwnedKeyLocked(key, owned)
}

func (v *replicatedSQLMutationValidator) ValidatePointOwnership(
	key []byte,
	owned distribution.KeyRange,
) replicatedstate.MutationValidation {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.validateOwnedKeyLocked(key, owned)
}

func (v *replicatedSQLMutationValidator) validateOwnedKeyLocked(
	key []byte,
	owned distribution.KeyRange,
) replicatedstate.MutationValidation {
	point, ok := v.pointForEncodedKeyLocked(key)
	if !ok {
		return replicatedstate.MutationValidationInvalid
	}
	if !owned.Contains(point) {
		return replicatedstate.MutationValidationWrongShard
	}
	return replicatedstate.MutationValidationAccept
}

// pointForEncodedKeyLocked maps the already-validated primary-key encoding.
// Replicated placement freezes the shard key to that same primary key, so this
// avoids reparsing the document and bounds decode scratch by the compact key
// profile plus canonical-number expansion, never by document size.
func (v *replicatedSQLMutationValidator) pointForEncodedKeyLocked(
	key []byte,
) (distribution.KeyspacePoint, bool) {
	component, decoded, next, err := orderedkey.DecodeComponent(v.decodeScratch[:0], key, 0)
	v.decodeScratch = decoded
	if err != nil || next != len(key) || component.Descending || component.Kind == orderedkey.KindNull {
		return distribution.KeyspacePoint{}, false
	}
	var scalar distribution.Scalar
	payload := decoded[component.PayloadStart:component.PayloadEnd]
	switch component.Kind {
	case orderedkey.KindString:
		scalar = distribution.NewString(byteview.String(payload))
	case orderedkey.KindNumber:
		var scalarErr error
		scalar, scalarErr = distribution.NewNumber(byteview.String(payload))
		if scalarErr != nil {
			return distribution.KeyspacePoint{}, false
		}
	default:
		return distribution.KeyspacePoint{}, false
	}
	scalarScratch := [1]distribution.Scalar{scalar}
	point, err := v.placement.mapper.PointFor(scalarScratch[:])
	if err != nil {
		return distribution.KeyspacePoint{}, false
	}
	return point, true
}

func (a *ReplicatedApply) observeMutationAttempt(
	keys replicatedstate.AttemptedMutationKeys,
	updateErr error,
) {
	if a == nil || a.table == nil || keys.Len() == 0 {
		return
	}
	// The core invokes this only while the wrapper still holds database.mu.
	// A generation move proves publication; a sticky persistence failure makes
	// the in-process outcome uncertain. A checkpoint-group decision failure can
	// report the same uncertainty without changing either collection signal, so
	// its explicit outcome-unknown classification also advances conservatively.
	if !a.attemptActive ||
		(a.table.collection.Generation() == a.attemptGeneration &&
			a.table.collection.PersistenceError() == nil &&
			!errors.Is(updateErr, durable.ErrCommitOutcomeUnknown) &&
			!(a.attemptBatch && updateErr == nil)) {
		return
	}
	if a.table.conflicts.recordWriteIfNoActive() {
		return
	}
	a.attemptKeys.keys = keys
	a.table.conflicts.recordBinary(&a.attemptKeys)
	a.attemptKeys.keys = replicatedstate.AttemptedMutationKeys{}
}

func (a *ReplicatedApply) checkLocked() error {
	if a == nil || a.database == nil || a.machine == nil || a.closed ||
		a.database.closed || a.database.replicatedApplyClaim != a {
		return ErrReplicatedApplyClosed
	}
	return nil
}

func (a *ReplicatedApply) checkActivationBaseLocked() error {
	if a.walBaseSelectActive || a.walBaseSelectPending {
		return ErrReplicatedApplyBusy
	}
	if a.activationBasePending != ([sha256.Size]byte{}) {
		return ErrReplicatedApplyBasePending
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
// durable apply cut needed by a higher-level capacity proof. It fails closed
// for a closed or poisoned claim.
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
	state, err := a.machine.SessionCapacityState()
	if err != nil {
		return ReplicatedApplyCapacityProfile{}, err
	}
	manifestDigest, err := a.machine.RelationManifestDigest()
	if err != nil {
		return ReplicatedApplyCapacityProfile{}, err
	}
	return ReplicatedApplyCapacityProfile{
		Binding:     ownedReplicatedShardStoreBinding(base.Binding),
		ApplyFormat: a.identity.Format, MaxSessions: a.identity.MaxSessions,
		RelationManifestDigest: manifestDigest,
		RetryWindow:            a.identity.RetryWindow, Initialized: state.Initialized,
		Applied: state.Applied, CheckpointApplied: a.machine.CheckpointAppliedIndex(),
		SessionCount:          state.SessionCount,
		SessionSlotCount:      state.SessionSlotCount,
		SessionEpochHighWater: state.SessionEpochHighWater,
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
	if a.owner != connector || a.database != connector.db || connector.refs != 1 ||
		(connector.exclusive && !a.exclusiveConnector) {
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
	if a.walBaseCaptureActive || a.walBaseSelectActive || a.walBaseSelectPending {
		return ErrReplicatedApplyBusy
	}
	connector.closed = true
	connector.exclusive = false
	return nil
}

// Close releases the singleton apply claim and its connector lifetime
// reference. It does not unbind or remove the durable hidden participant. A
// pending WAL-generation selection retires the connector before releasing the
// claim: only a complete root close/reopen may reconstruct that durable fence,
// so the same Database can never mint an unfenced replacement claim.
func (a *ReplicatedApply) Close() error {
	if a == nil || a.database == nil || a.owner == nil {
		return nil
	}
	connector := a.owner
	core := a.database
	// Connector -> database is the common ownership lock order used by Connect,
	// OpenReplicatedApply, and ClaimRuntimeOwnership. Holding connector.mu keeps
	// replacement sessions and claims out until a pending selection has made
	// retirement irrevocable.
	connector.mu.Lock()
	defer connector.mu.Unlock()
	core.mu.Lock()
	if a.closed {
		core.mu.Unlock()
		return nil
	}
	if core.replicatedApplyClaim != a {
		core.mu.Unlock()
		return ErrReplicatedApplyClosed
	}
	if a.walBaseCaptureActive || a.walBaseSelectActive {
		core.mu.Unlock()
		return ErrReplicatedApplyBusy
	}
	if core.checkpointGroup == nil {
		core.mu.Unlock()
		return ErrReplicatedApplyMismatch
	}
	if err := core.checkpointGroup.Checkpoint(); err != nil {
		core.mu.Unlock()
		return err
	}
	if a.walBaseSelectPending {
		connector.closed = true
	}
	a.closed = true
	core.replicatedApplyClaim = nil
	core.mu.Unlock()
	if a.exclusiveConnector {
		connector.exclusive = false
	}
	return connector.releaseLocked()
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

// BeginRangeSplitCapture installs or recovers the exact source transition log
// in the capture participant reserved at first apply activation. The database
// lock orders this cold control operation before subsequent committed apply.
func (a *ReplicatedApply) BeginRangeSplitCapture(
	partitioner *rangesplit.Partitioner,
) (*rangesplit.SourceCapture, error) {
	if a == nil || a.database == nil || partitioner == nil {
		return nil, ErrReplicatedApplyClosed
	}
	a.database.mu.Lock()
	defer a.database.mu.Unlock()
	if err := a.checkLocked(); err != nil || a.database.replicatedCaptureCollection == nil {
		return nil, errors.Join(err, ErrReplicatedApplyMismatch)
	}
	if a.rangeSplitCapture != nil {
		if a.rangeSplitCapture.PartitionerDigest() != partitioner.Digest() {
			return nil, replicatedstate.ErrSplitCaptureActivation
		}
		return a.rangeSplitCapture, nil
	}
	capture, err := rangesplit.NewSourceCapture(
		partitioner, replicatedstate.TransitionCaptureCollectionName,
		a.database.replicatedCaptureCollection,
	)
	if err != nil {
		return nil, err
	}
	if err = a.machine.BeginTransitionCapture(capture); err != nil {
		return nil, err
	}
	a.rangeSplitCapture = capture
	return capture, nil
}

// CheckpointAppliedIndex is the authenticated durable apply cut supplied to
// Raft WAL retention qualification. It may trail Applied by at most the fixed
// checkpoint batch and is not standalone deletion authority.
func (a *ReplicatedApply) CheckpointAppliedIndex() uint64 {
	if a == nil || a.database == nil {
		return 0
	}
	a.database.mu.RLock()
	defer a.database.mu.RUnlock()
	if a.checkLocked() != nil {
		return 0
	}
	return a.machine.CheckpointAppliedIndex()
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

// PointReadInto serves one dense relation from an exact replicated publication
// cut. It does not resolve table names and the applied floor is explicit.
func (a *ReplicatedApply) PointReadInto(
	relation replication.RelationID,
	key []byte,
	minimumApplied uint64,
	maxValueBytes int,
	dst []byte,
) (replicatedstate.PointReadResult, error) {
	if a == nil || a.database == nil {
		return replicatedstate.PointReadResult{}, ErrReplicatedApplyClosed
	}
	a.database.mu.RLock()
	defer a.database.mu.RUnlock()
	if err := a.checkLocked(); err != nil {
		return replicatedstate.PointReadResult{}, err
	}
	if err := a.checkActivationBaseLocked(); err != nil {
		return replicatedstate.PointReadResult{}, err
	}
	return a.machine.PointReadInto(relation, key, minimumApplied, maxValueBytes, dst)
}

// TransactionRecoveryReadInto forwards the closed hidden-state recovery read
// to the replicated machine under the same live apply/activation fence as an
// ordinary replicated point read. The caller owns both bounded arenas; SQL
// names, planning, and the legacy local transaction journal are not involved.
func (a *ReplicatedApply) TransactionRecoveryReadInto(
	request replicatedstate.TransactionRecoveryReadRequest,
	records []replicatedstate.TransactionRecoveryRecord,
	payload []byte,
) (replicatedstate.TransactionRecoveryReadResult, error) {
	if a == nil || a.database == nil {
		return replicatedstate.TransactionRecoveryReadResult{}, ErrReplicatedApplyClosed
	}
	a.database.mu.RLock()
	defer a.database.mu.RUnlock()
	if err := a.checkLocked(); err != nil {
		return replicatedstate.TransactionRecoveryReadResult{}, err
	}
	if err := a.checkActivationBaseLocked(); err != nil {
		return replicatedstate.TransactionRecoveryReadResult{}, err
	}
	return a.machine.TransactionRecoveryReadInto(request, records, payload)
}

// RequestLedgerReadInto forwards one full-key hidden request-ledger read under
// the same live apply/activation fence as transaction recovery. The result
// aliases dst and never exposes the private system collection or relation ID.
func (a *ReplicatedApply) RequestLedgerReadInto(
	request replicatedstate.RequestLedgerReadRequest,
	dst []byte,
) (replicatedstate.RequestLedgerReadResult, error) {
	if a == nil || a.database == nil {
		return replicatedstate.RequestLedgerReadResult{}, ErrReplicatedApplyClosed
	}
	a.database.mu.RLock()
	defer a.database.mu.RUnlock()
	if err := a.checkLocked(); err != nil {
		return replicatedstate.RequestLedgerReadResult{}, err
	}
	if err := a.checkActivationBaseLocked(); err != nil {
		return replicatedstate.RequestLedgerReadResult{}, err
	}
	return a.machine.RequestLedgerReadInto(request, dst)
}

// SnapshotArtifactCut captures one coherent, read-only system/relation/capture
// cut for streaming snapshot export. The returned handle owns every durable
// collection snapshot until Close; it carries no SQL session or serving authority.
func (a *ReplicatedApply) SnapshotArtifactCut() (*replicatedstate.ReadSnapshot, error) {
	if a == nil || a.database == nil {
		return nil, ErrReplicatedApplyClosed
	}
	a.database.mu.RLock()
	defer a.database.mu.RUnlock()
	if err := a.checkLocked(); err != nil {
		return nil, err
	}
	if err := a.checkActivationBaseLocked(); err != nil {
		return nil, err
	}
	base := a.database.catalog.ReplicatedShardStore
	if base == nil || base.RelationCount == 0 {
		return nil, ErrReplicatedApplyMismatch
	}
	return a.machine.Snapshot()
}

// SnapshotArtifactCutAt is the live-backup source boundary. The floor must be
// obtained from the serving leader's quorum ReadIndex; a behind local apply is
// rejected rather than silently weakening the cut.
func (a *ReplicatedApply) SnapshotArtifactCutAt(minimumApplied uint64) (*replicatedstate.ReadSnapshot, error) {
	if minimumApplied == 0 {
		return nil, replicatedstate.ErrReadBehind
	}
	cut, err := a.SnapshotArtifactCut()
	if err != nil {
		return nil, err
	}
	if cut.Fence().Applied < minimumApplied {
		_ = cut.Close()
		return nil, replicatedstate.ErrReadBehind
	}
	return cut, nil
}

// RangeSplitSnapshot returns the same coherent full apply cut through the
// narrow split-source capability consumed by the distributed split runtime.
func (a *ReplicatedApply) RangeSplitSnapshot() (*replicatedstate.ReadSnapshot, error) {
	return a.SnapshotArtifactCut()
}

// RangeSplitRelationManifestDigest returns the machine-authenticated logical
// relation bundle identity without exposing the machine itself.
func (a *ReplicatedApply) RangeSplitRelationManifestDigest() ([sha256.Size]byte, error) {
	if a == nil || a.database == nil {
		return [sha256.Size]byte{}, ErrReplicatedApplyClosed
	}
	a.database.mu.RLock()
	defer a.database.mu.RUnlock()
	if err := a.checkLocked(); err != nil {
		return [sha256.Size]byte{}, err
	}
	return a.machine.RelationManifestDigest()
}

// SchemaApplyContractDigest returns the exact replicated command/result
// contract for schema rollout fencing. It never exposes the underlying state
// machine and fails once an ordered schema transition has fenced this bundle.
func (a *ReplicatedApply) SchemaApplyContractDigest() ([sha256.Size]byte, error) {
	if a == nil || a.database == nil {
		return [sha256.Size]byte{}, ErrReplicatedApplyClosed
	}
	a.database.mu.RLock()
	defer a.database.mu.RUnlock()
	if err := a.checkLocked(); err != nil {
		return [sha256.Size]byte{}, err
	}
	return a.machine.ApplyContractDigest()
}

// RangeSplitCaptureCount is an O(1) recovery guard for a persisted capture
// descriptor. Zero proves that no matching capture participant exists locally.
func (a *ReplicatedApply) RangeSplitCaptureCount() (uint64, error) {
	if a == nil || a.database == nil {
		return 0, ErrReplicatedApplyClosed
	}
	a.database.mu.RLock()
	defer a.database.mu.RUnlock()
	if err := a.checkLocked(); err != nil || a.database.replicatedCaptureCollection == nil {
		return 0, errors.Join(err, ErrReplicatedApplyMismatch)
	}
	return a.database.replicatedCaptureCollection.Len(), nil
}

// BuildBundleSnapshotBase captures and certifies the hidden apply image plus
// every fixed bundle relation at one database-snapshot cut. The certificate is
// small; the durable member images are transported under its authenticated
// relation manifest and checkpoint-group membership.
func (a *ReplicatedApply) BuildBundleSnapshotBase() (
	*pb.Snapshot,
	replicatedstate.SnapshotArtifactManifest,
	error,
) {
	if a == nil || a.database == nil {
		return nil, replicatedstate.SnapshotArtifactManifest{}, ErrReplicatedApplyClosed
	}
	a.database.mu.RLock()
	defer a.database.mu.RUnlock()
	if err := a.checkLocked(); err != nil {
		return nil, replicatedstate.SnapshotArtifactManifest{}, err
	}
	if err := a.checkActivationBaseLocked(); err != nil {
		return nil, replicatedstate.SnapshotArtifactManifest{}, err
	}
	base := a.database.catalog.ReplicatedShardStore
	if base == nil || base.RelationCount == 0 {
		return nil, replicatedstate.SnapshotArtifactManifest{}, ErrReplicatedApplyMismatch
	}
	return a.machine.BuildBundleSnapshotBase()
}

// ApplyNormal implements raftmodel.StateMachine under the SQL publication
// lock, pairing durable user/system mutation with conflict-clock publication.
func (a *ReplicatedApply) ApplyNormal(
	meta raftmodel.ApplyMeta,
	data []byte,
) (raftmodel.Publication, error) {
	publication, _, err := a.applyNormal(meta, data, false)
	return publication, err
}

// ApplyNormalWithCompletion carries the original durable result to the Raft
// settlement lane without changing explicit post-apply lookup semantics.
func (a *ReplicatedApply) ApplyNormalWithCompletion(
	meta raftmodel.ApplyMeta, data []byte,
) (raftmodel.Publication, []byte, error) {
	return a.applyNormal(meta, data, true)
}

func (a *ReplicatedApply) applyNormal(
	meta raftmodel.ApplyMeta, data []byte, captureCompletion bool,
) (raftmodel.Publication, []byte, error) {
	if a == nil || a.database == nil {
		return raftmodel.Publication{}, nil, ErrReplicatedApplyClosed
	}
	a.database.mu.Lock()
	defer a.database.mu.Unlock()
	if err := a.checkLocked(); err != nil {
		return raftmodel.Publication{}, nil, err
	}
	if err := a.checkActivationBaseLocked(); err != nil {
		return raftmodel.Publication{}, nil, err
	}
	a.attemptGeneration = a.table.collection.Generation()
	a.attemptActive = true
	var publication raftmodel.Publication
	var completion []byte
	var err error
	if captureCompletion {
		publication, completion, err = a.machine.ApplyNormalWithCompletion(meta, data)
	} else {
		publication, err = a.machine.ApplyNormal(meta, data)
	}
	a.attemptActive = false
	a.attemptGeneration = 0
	return publication, completion, err
}

// ApplyNormalBatch implements raftmodel.NormalBatchStateMachine under one SQL
// publication lock. The underlying Machine publishes an exact prefix through
// one checkpoint-group transaction and reports the union of its logically
// attempted user keys to the conflict clock.
func (a *ReplicatedApply) ApplyNormalBatch(
	entries []raftmodel.NormalApply,
	dataChainWitnesses [][32]byte,
) (int, raftmodel.Publication, error) {
	clear(dataChainWitnesses[:min(len(entries), len(dataChainWitnesses))])
	if a == nil || a.database == nil {
		return 0, raftmodel.Publication{}, ErrReplicatedApplyClosed
	}
	a.database.mu.Lock()
	defer a.database.mu.Unlock()
	if err := a.checkLocked(); err != nil {
		return 0, raftmodel.Publication{}, err
	}
	if err := a.checkActivationBaseLocked(); err != nil {
		return 0, raftmodel.Publication{}, err
	}
	a.attemptGeneration = a.table.collection.Generation()
	a.attemptActive = true
	a.attemptBatch = true
	applied, publication, err := a.machine.ApplyNormalBatch(entries, dataChainWitnesses)
	a.attemptBatch = false
	a.attemptActive = false
	a.attemptGeneration = 0
	return applied, publication, err
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
	if err := a.checkActivationBaseLocked(); err != nil {
		return raftmodel.Publication{}, err
	}
	return a.machine.ApplyConfiguration(meta, conf)
}

// InstallSnapshot implements raftmodel.StateMachine. The underlying machine
// initializes from its exact static bootstrap or binds an already-staged exact
// candidate to a certified immutable Raft base.
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
	if a.walBaseSelectActive || a.walBaseSelectPending {
		return raftmodel.Publication{}, ErrReplicatedApplyBusy
	}
	pending := a.activationBasePending
	if pending != ([sha256.Size]byte{}) {
		certificate, err := replicatedstate.OpenSnapshotBase(snapshot)
		if err != nil || certificate.Digest != pending {
			return raftmodel.Publication{}, errors.Join(
				ErrReplicatedApplyBasePending, err,
			)
		}
	}
	publication, err := a.machine.InstallSnapshot(snapshot)
	if err == nil && pending != ([sha256.Size]byte{}) {
		if a.database.checkpointGroup == nil {
			return publication, ErrReplicatedApplyMismatch
		}
		if err = a.database.checkpointGroup.Checkpoint(); err != nil {
			return publication, err
		}
		a.activationBasePending = [sha256.Size]byte{}
		a.database.replicatedSeedPending = false
	}
	return publication, err
}

// AdmitCommand delegates the bounded, non-reserving pre-proposal check. A nil
// result does not grant serving or reserve session capacity.
func (a *ReplicatedApply) AdmitCommand(data []byte) error {
	if a == nil || a.database == nil {
		return ErrReplicatedApplyClosed
	}
	a.database.mu.RLock()
	defer a.database.mu.RUnlock()
	if err := a.checkLocked(); err != nil {
		return err
	}
	if err := a.checkActivationBaseLocked(); err != nil {
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
	if _, healthErr := a.machine.SessionCapacityState(); healthErr != nil {
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
	if err := a.checkActivationBaseLocked(); err != nil {
		return replicatedstate.CompletionLookup{}, err
	}
	return a.machine.LookupCompletion(data)
}

// LookupCompletionInto returns an exact completion in caller-owned storage.
// The destination contract is defined by
// [replicatedstate.Machine.LookupCompletionInto].
func (a *ReplicatedApply) LookupCompletionInto(
	data []byte,
	dst []byte,
) (replicatedstate.CompletionLookup, error) {
	if a == nil || a.database == nil {
		return replicatedstate.CompletionLookup{}, ErrReplicatedApplyClosed
	}
	a.database.mu.RLock()
	defer a.database.mu.RUnlock()
	if err := a.checkLocked(); err != nil {
		return replicatedstate.CompletionLookup{}, err
	}
	if err := a.checkActivationBaseLocked(); err != nil {
		return replicatedstate.CompletionLookup{}, err
	}
	return a.machine.LookupCompletionInto(data, dst)
}

// BeginCompletionLookupBatch captures one exact completion snapshot and holds
// the SQL publication fence until EndCompletionLookupBatch. Every lookup in
// the batch observes the same activated ReplicatedApply owner and durable cut.
func (a *ReplicatedApply) BeginCompletionLookupBatch(
	workspace *CompletionLookupWorkspace,
	expected raftmodel.Publication,
) error {
	if a == nil || a.database == nil {
		return ErrReplicatedApplyClosed
	}
	if workspace == nil || workspace.owner != nil {
		return replicatedstate.ErrCompletionWorkspaceBusy
	}
	a.database.mu.RLock()
	if err := a.checkLocked(); err != nil {
		a.database.mu.RUnlock()
		return err
	}
	if err := a.checkActivationBaseLocked(); err != nil {
		a.database.mu.RUnlock()
		return err
	}
	if err := a.machine.BeginCompletionLookupBatch(&workspace.machine, expected); err != nil {
		a.database.mu.RUnlock()
		return err
	}
	workspace.owner = a
	return nil
}

// LookupCompletionIntoWorkspace resolves one command through the exact cut
// held by BeginCompletionLookupBatch into caller-owned result storage.
func (a *ReplicatedApply) LookupCompletionIntoWorkspace(
	workspace *CompletionLookupWorkspace,
	data []byte,
	dst []byte,
) (replicatedstate.CompletionLookup, error) {
	if workspace == nil || workspace.owner != a {
		return replicatedstate.CompletionLookup{}, replicatedstate.ErrCompletionWorkspaceBusy
	}
	return a.machine.LookupCompletionIntoWorkspace(&workspace.machine, data, dst)
}

// EndCompletionLookupBatch releases the exact completion snapshot and SQL
// publication fence. The workspace remains warm for a later batch.
func (a *ReplicatedApply) EndCompletionLookupBatch(
	workspace *CompletionLookupWorkspace,
) error {
	if a == nil || a.database == nil || workspace == nil || workspace.owner != a {
		return replicatedstate.ErrCompletionWorkspaceBusy
	}
	workspace.owner = nil
	err := a.machine.EndCompletionLookupBatch(&workspace.machine)
	a.database.mu.RUnlock()
	return err
}

// Release drops every inactive reusable snapshot buffer retained by workspace.
func (workspace *CompletionLookupWorkspace) Release() error {
	if workspace == nil {
		return nil
	}
	if workspace.owner != nil {
		return replicatedstate.ErrCompletionWorkspaceBusy
	}
	return workspace.machine.Release()
}

// DurabilityStats returns detached physical checkpoint-group counters for
// apply batching and checkpoint observability. It exposes no storage handle or
// serving authority.
func (a *ReplicatedApply) DurabilityStats() (durable.CheckpointGroupStats, error) {
	if a == nil || a.database == nil {
		return durable.CheckpointGroupStats{}, ErrReplicatedApplyClosed
	}
	a.database.mu.RLock()
	defer a.database.mu.RUnlock()
	if err := a.checkLocked(); err != nil {
		return durable.CheckpointGroupStats{}, err
	}
	if a.database.checkpointGroup == nil {
		return durable.CheckpointGroupStats{}, ErrReplicatedApplyMismatch
	}
	return a.database.checkpointGroup.Stats(), nil
}

// ReplicatedApplyResourceStats is one detached, fixed-space storage snapshot
// for a serving replicated bundle. Relations are dense in authenticated
// relation-ID order. The hidden system and split-capture participants are
// reported separately so benchmark tooling can account for every physical
// byte without acquiring a collection or database capability.
type ReplicatedApplyResourceStats struct {
	System        durable.Stats
	Capture       durable.Stats
	Relations     [replication.MaxRelationsPerBundle]durable.Stats
	RelationCount uint16
}

// ResourceStats returns exact collection I/O and space counters without
// exposing storage handles. The database catalog lock keeps the relation
// manifest and collection pointers on one coherent cut.
func (a *ReplicatedApply) ResourceStats() (ReplicatedApplyResourceStats, error) {
	if a == nil || a.database == nil {
		return ReplicatedApplyResourceStats{}, ErrReplicatedApplyClosed
	}
	a.database.mu.RLock()
	defer a.database.mu.RUnlock()
	if err := a.checkLocked(); err != nil {
		return ReplicatedApplyResourceStats{}, err
	}
	identity := a.database.catalog.ReplicatedShardStore
	if identity == nil || identity.RelationCount == 0 ||
		identity.RelationCount > uint16(len((ReplicatedApplyResourceStats{}).Relations)) ||
		a.database.replicatedApplyCollection == nil ||
		a.database.replicatedCaptureCollection == nil {
		return ReplicatedApplyResourceStats{}, ErrReplicatedApplyMismatch
	}
	result := ReplicatedApplyResourceStats{
		System:        a.database.replicatedApplyCollection.Stats(),
		Capture:       a.database.replicatedCaptureCollection.Stats(),
		RelationCount: identity.RelationCount,
	}
	for ordinal := uint16(0); ordinal < identity.RelationCount; ordinal++ {
		relation := identity.Relations[ordinal]
		table := a.database.tables[relation.Table]
		if table == nil || table.collection == nil {
			return ReplicatedApplyResourceStats{}, ErrReplicatedApplyMismatch
		}
		result.Relations[ordinal] = table.collection.Stats()
	}
	return result, nil
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
	var relationHeader [10]byte
	binary.LittleEndian.PutUint16(relationHeader[0:2], identity.RelationCount)
	binary.LittleEndian.PutUint64(
		relationHeader[2:10], identity.RelationSchemaGeneration,
	)
	_, _ = h.Write(relationHeader[:])
	logicalManifest := replicatedRelationApplyManifestDigest(identity)
	_, _ = h.Write(logicalManifest[:])
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
	storage, captureStorage string,
	options ReplicatedApplyOptions,
) replicatedApplyMeta {
	captureLimits, _ := replicatedCaptureLimits(identity)
	systemLimits := replicatedApplyTransactionSystemLimits(identity, options.RetryWindow,
		options.RequestLedgerRangeIdentity != ([sha256.Size]byte{}), options.TxnLimits.MaxDocuments)
	return replicatedApplyMeta{
		Format:                           ReplicatedApplyFormat,
		Storage:                          strings.Clone(storage),
		CaptureStorage:                   strings.Clone(captureStorage),
		ValidationProfile:                uint8(replicatedstate.ValidationDeterministicMutation),
		ValidationDigest:                 replicatedApplyProfileDigest(identity, options.Placement),
		SystemLimits:                     systemLimits,
		CaptureLimits:                    captureLimits,
		MaxSessions:                      options.MaxSessions,
		RetryWindow:                      options.RetryWindow,
		TxnMaxCollections:                options.TxnLimits.MaxCollections,
		TxnMaxDocuments:                  options.TxnLimits.MaxDocuments,
		TxnMaxBytes:                      options.TxnLimits.MaxBytes,
		Placement:                        ownedReplicatedPlacementProfile(options.Placement),
		RequestLedgerCapacityBytes:       options.RequestLedgerCapacityBytes,
		RequestLedgerCleanupReserveBytes: options.RequestLedgerCleanupReserveBytes,
		RequestLedgerRangeStart:          options.RequestLedgerRangeStart,
		RequestLedgerRangeEnd:            options.RequestLedgerRangeEnd,
		RequestLedgerRangeIdentity:       options.RequestLedgerRangeIdentity,
		Sidecars:                         canonicalReplicatedApplySidecarsForLimits(systemLimits),
	}
}

func validateReplicatedApplyOptions(
	identity ReplicatedShardStoreIdentity,
	options ReplicatedApplyOptions,
) error {
	if identity.UserTable == replicatedstate.SystemCollectionName ||
		identity.UserTable == replicatedstate.TransitionCaptureCollectionName {
		return fmt.Errorf(
			"%w: user table uses the reserved system collection name",
			ErrReplicatedApplyMismatch,
		)
	}
	relationCount := int(identity.RelationCount)
	relationDocuments := 0
	for ordinal := 0; ordinal < int(identity.RelationCount); ordinal++ {
		limits := identity.Relations[ordinal].Limits
		relationDocuments = min(
			replication.MaxMutations, relationDocuments+limits.MaxBatchDocuments,
		)
	}
	maxTxnDocuments, err := replicatedstate.RequiredBundleTransactionDocuments(
		relationDocuments, options.RetryWindow, true,
	)
	if err != nil {
		return fmt.Errorf("%w: transaction document profile", ErrReplicatedApplyMismatch)
	}
	if options.MaxSessions == 0 ||
		options.MaxSessions > replicatedstate.MaxRetainedSessions ||
		options.RetryWindow == 0 ||
		options.RetryWindow > replicatedstate.MaxSessionRetryWindow ||
		options.TxnLimits.MaxCollections < relationCount+2 ||
		options.TxnLimits.MaxDocuments < maxTxnDocuments ||
		options.TxnLimits.MaxBytes <= 0 {
		return fmt.Errorf("%w: invalid transaction or retention limits", ErrReplicatedApplyMismatch)
	}
	requiredBytes, err := replicatedApplyTransactionByteFloor(identity, options.RetryWindow,
		options.RequestLedgerRangeIdentity != ([sha256.Size]byte{}), options.TxnLimits.MaxDocuments)
	if err != nil || options.TxnLimits.MaxBytes < requiredBytes {
		return fmt.Errorf("%w: transaction byte limit does not cover apply and capture", ErrReplicatedApplyMismatch)
	}
	if err := validateReplicatedPlacementProfile(options.Placement, identity); err != nil {
		return err
	}
	if err := validateReplicatedRequestLedgerOptions(options); err != nil {
		return err
	}
	return nil
}

func validateReplicatedRequestLedgerOptions(options ReplicatedApplyOptions) error {
	enabled := options.RequestLedgerCapacityBytes != 0 ||
		options.RequestLedgerCleanupReserveBytes != 0 ||
		options.RequestLedgerRangeStart != ([sha256.Size]byte{}) ||
		options.RequestLedgerRangeEnd != ([sha256.Size]byte{}) ||
		options.RequestLedgerRangeIdentity != ([sha256.Size]byte{})
	if !enabled {
		return nil
	}
	if options.RequestLedgerCapacityBytes == 0 ||
		options.RequestLedgerCapacityBytes > math.MaxInt64 ||
		options.RequestLedgerCleanupReserveBytes == 0 ||
		options.RequestLedgerCleanupReserveBytes >= options.RequestLedgerCapacityBytes ||
		options.RequestLedgerRangeIdentity == ([sha256.Size]byte{}) ||
		(options.RequestLedgerRangeEnd != ([sha256.Size]byte{}) &&
			bytes.Compare(options.RequestLedgerRangeStart[:], options.RequestLedgerRangeEnd[:]) >= 0) {
		return fmt.Errorf("%w: invalid request-ledger capacity or range", ErrReplicatedApplyMismatch)
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
	want := newReplicatedApplyMeta(identity, meta.Storage, meta.CaptureStorage, options)
	return *meta == want
}

func (m replicatedApplyMeta) options() ReplicatedApplyOptions {
	return ReplicatedApplyOptions{
		MaxSessions: m.MaxSessions, RetryWindow: m.RetryWindow,
		TxnLimits: durable.TxnLimits{
			MaxCollections: m.TxnMaxCollections,
			MaxDocuments:   m.TxnMaxDocuments,
			MaxBytes:       m.TxnMaxBytes,
		},
		Placement:                        ownedReplicatedPlacementProfile(m.Placement),
		RequestLedgerCapacityBytes:       m.RequestLedgerCapacityBytes,
		RequestLedgerCleanupReserveBytes: m.RequestLedgerCleanupReserveBytes,
		RequestLedgerRangeStart:          m.RequestLedgerRangeStart,
		RequestLedgerRangeEnd:            m.RequestLedgerRangeEnd,
		RequestLedgerRangeIdentity:       m.RequestLedgerRangeIdentity,
	}
}

func (m replicatedApplyMeta) identity() ReplicatedApplyIdentity {
	return ReplicatedApplyIdentity{
		Format: m.Format, Storage: strings.Clone(m.Storage), CaptureStorage: strings.Clone(m.CaptureStorage),
		ValidationProfile: m.ValidationProfile, ValidationDigest: m.ValidationDigest,
		SystemLimits: m.SystemLimits, CaptureLimits: m.CaptureLimits, MaxSessions: m.MaxSessions,
		RetryWindow: m.RetryWindow,
		TxnLimits:   m.options().TxnLimits, Placement: ownedReplicatedPlacementProfile(m.Placement),
		RequestLedgerCapacityBytes:       m.RequestLedgerCapacityBytes,
		RequestLedgerCleanupReserveBytes: m.RequestLedgerCleanupReserveBytes,
		RequestLedgerRangeStart:          m.RequestLedgerRangeStart,
		RequestLedgerRangeEnd:            m.RequestLedgerRangeEnd,
		RequestLedgerRangeIdentity:       m.RequestLedgerRangeIdentity,
		Sidecars:                         m.Sidecars,
	}
}

func replicatedApplyMetaFromIdentity(identity ReplicatedApplyIdentity) replicatedApplyMeta {
	return replicatedApplyMeta{
		Format: identity.Format, Storage: strings.Clone(identity.Storage), CaptureStorage: strings.Clone(identity.CaptureStorage),
		ValidationProfile: identity.ValidationProfile,
		ValidationDigest:  identity.ValidationDigest, SystemLimits: identity.SystemLimits,
		CaptureLimits:                    identity.CaptureLimits,
		MaxSessions:                      identity.MaxSessions,
		RetryWindow:                      identity.RetryWindow,
		TxnMaxCollections:                identity.TxnLimits.MaxCollections,
		TxnMaxDocuments:                  identity.TxnLimits.MaxDocuments,
		TxnMaxBytes:                      identity.TxnLimits.MaxBytes,
		Placement:                        ownedReplicatedPlacementProfile(identity.Placement),
		RequestLedgerCapacityBytes:       identity.RequestLedgerCapacityBytes,
		RequestLedgerCleanupReserveBytes: identity.RequestLedgerCleanupReserveBytes,
		RequestLedgerRangeStart:          identity.RequestLedgerRangeStart,
		RequestLedgerRangeEnd:            identity.RequestLedgerRangeEnd,
		RequestLedgerRangeIdentity:       identity.RequestLedgerRangeIdentity,
		Sidecars:                         identity.Sidecars,
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
	if err := validateStorageIdentity(m.CaptureStorage); err != nil ||
		m.CaptureStorage == "" || m.CaptureStorage == identity.UserStorage ||
		m.CaptureStorage == m.Storage {
		return fmt.Errorf("%w: invalid or aliased capture storage identity", ErrReplicatedApplyMismatch)
	}
	if m.SystemLimits != replicatedApplyTransactionSystemLimits(*identity, m.RetryWindow,
		m.RequestLedgerRangeIdentity != ([sha256.Size]byte{}), m.TxnMaxDocuments) {
		return fmt.Errorf("%w: system collection limits", ErrReplicatedApplyMismatch)
	}
	captureLimits, err := replicatedCaptureLimits(*identity)
	if err != nil || m.CaptureLimits != captureLimits {
		return fmt.Errorf("%w: capture collection limits", ErrReplicatedApplyMismatch)
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
