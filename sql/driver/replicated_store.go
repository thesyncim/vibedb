package driver

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/store/durable"
	vibejson "github.com/thesyncim/vibejson"
)

const (
	// ReplicatedShardStoreFormat is the current write-once SQL catalog binding
	// between a prepared local shard store and one replicated WAL/apply lineage.
	ReplicatedShardStoreFormat uint16 = 0

	replicatedMaxIdentityBytes     = 255
	replicatedMaxKeyBytes          = 256
	replicatedMaxDocumentBytes     = 4 << 20
	replicatedMaxDistinctMutations = 64
	replicatedMaxBatchBytes        = replication.MaxCommandBytes +
		replicatedMaxDistinctMutations*replicatedMaxKeyBytes
)

var (
	ErrReplicatedShardStoreUnbound          = errors.New("vibedb: SQL catalog is not bound to a replicated shard store")
	ErrReplicatedShardStoreIdentityMismatch = errors.New("vibedb: replicated shard store identity mismatch")
	ErrReplicatedShardStoreProfile          = errors.New("vibedb: SQL catalog does not satisfy the replicated shard store profile")
	ErrReplicatedShardStoreBusy             = errors.New("vibedb: SQL catalog cannot be bound while it has live users or pending retirement")
	// ErrDirectWriteFenced reports SQL DML or DDL refused because the catalog is
	// write-once bound to replicated apply. Reads and read-only transactions stay
	// available; a future trusted apply path must own a separate lower-level
	// capability boundary.
	ErrDirectWriteFenced = errors.New("vibedb: direct SQL writes are fenced by replicated mode")
)

// ReplicatedAuthorityProfile freezes the six topology authority generations
// that a future admission/apply adapter must match. None is a local serving
// lease.
type ReplicatedAuthorityProfile struct {
	ActivePolicyGeneration uint64
	ProtectionEpoch        uint64
	OwnershipEpoch         uint64
	SchemaGeneration       uint64
	RoutingVersion         uint64
	RouteGeneration        uint64
}

// ReplicatedShardStoreBinding is an exact WAL, placement, recovery, and
// authority lineage. Each prepared SQL root can bind to only one such tuple;
// this record does not prevent another root from naming the same tuple, so
// external orchestration and the retained full identity select the intended
// root. StoreID is a WAL identity, semantically distinct from GroupID and the
// SQL LogID carried by ReplicatedShardStoreIdentity.
type ReplicatedShardStoreBinding struct {
	ClusterID             [16]byte
	ClusterIncarnation    [16]byte
	TopologyRecoveryEpoch uint64
	Distribution          string
	Shard                 string
	AllocationGeneration  uint64
	ShardIncarnation      [16]byte
	GroupID               [16]byte
	MemberID              uint64
	StoreID               [16]byte
	Authority             ReplicatedAuthorityProfile
}

// ReplicatedShardStoreLimits is the exact durable admission profile frozen at
// bind. MaxBatchDocuments is the replicated state's distinct-mutation bound.
type ReplicatedShardStoreLimits struct {
	MaxKeyBytes       int
	MaxDocumentBytes  int
	MaxBatchDocuments int
	MaxBatchBytes     int
}

// ReplicatedShardRelationKind freezes the meaning of one dense physical slot
// in a catalog-owned replicated shard bundle. A bundle always has one JSON
// base relation at slot one followed by global-index relations.
type ReplicatedShardRelationKind uint8

const (
	ReplicatedShardRelationJSON ReplicatedShardRelationKind = iota + 1
	ReplicatedShardRelationGlobalIndex
)

// ReplicatedGlobalIndexRelation is the cold provisioning input for one global
// exact-index image. Relation IDs must be dense from two; no table name or
// index identity is carried by replicated mutation commands.
type ReplicatedGlobalIndexRelation struct {
	Relation     uint16
	Table        string
	IndexID      uint64
	Incarnation  uint64
	LocatorCount uint8
	Unique       bool
}

// ReplicatedShardRelationIdentity is one immutable, comparable manifest slot.
// Storage and limits name the exact local durable image. Global fields are
// zero for the base JSON relation and mandatory for a global-index relation.
type ReplicatedShardRelationIdentity struct {
	Relation         uint16
	Kind             ReplicatedShardRelationKind
	Table            string
	Storage          string
	Limits           ReplicatedShardStoreLimits
	LocalIndexDigest [sha256.Size]byte
	IndexID          uint64
	Incarnation      uint64
	LocatorCount     uint8
	Unique           bool
}

// ReplicatedShardStoreIdentity is the complete reopen identity. LogID is the
// already-durable SQL shard-store identity: callers must retain it and supply
// it to OpenReplicatedShardStore so a different SQL root bound to the same WAL
// tuple is rejected. It cannot distinguish or revoke a byte-identical SQL-root
// copy opened under that same WAL tuple; identity comparison alone likewise
// cannot revoke a byte-identical copy of the WAL root.
type ReplicatedShardStoreIdentity struct {
	Format         uint16
	Binding        ReplicatedShardStoreBinding
	LogID          [16]byte
	UserTable      string
	UserStorage    string
	UserPrimaryKey string
	UserLimits     ReplicatedShardStoreLimits
	Sidecars       ReplicatedShardStoreSidecarProfile

	// RelationCount zero is the byte-identical legacy singleton catalog. A
	// value of two or more activates the fixed dense relation manifest below.
	RelationCount            uint16
	RelationSchemaGeneration uint64
	RelationManifestDigest   [sha256.Size]byte
	Relations                [replication.MaxRelationsPerBundle]ReplicatedShardRelationIdentity
}

// BindReplicatedShardStore permanently converts one opened, prepared local
// shard root into replicated mode. It requires no live Sessions, exactly one
// schema-free/view-free unmaterialized table, an absent transaction
// marker, and no prior local serving fence. Bind creates a fresh sealed user
// storage incarnation and publishes it with the complete identity in one
// catalog cut; already-materialized or marker-bearing roots are rejected rather
// than converted in place.
// Before calling, the orchestrator must call ShardStoreIdentity and durably
// retain its LogID together with binding and userTable; a crash after catalog
// publication but before this method returns can then be settled through
// OpenReplicatedShardStoreForSettlement. On success the returned full identity
// replaces or augments that pre-bind recovery record.
func (d *Database) BindReplicatedShardStore(
	binding ReplicatedShardStoreBinding,
	userTable string,
) (ReplicatedShardStoreIdentity, error) {
	return d.bindReplicatedShardStore(binding, userTable, nil)
}

// BindReplicatedShardStoreBundle permanently binds one base JSON table and a
// dense list of independently stored global-index relations to one replicated
// shard group. All named tables must be the complete, unmaterialized catalog;
// they are created, transaction-log adopted, and catalog-published as one
// activation. The legacy singleton binder remains byte-identical when this API
// is not used.
func (d *Database) BindReplicatedShardStoreBundle(
	binding ReplicatedShardStoreBinding,
	userTable string,
	globalIndexes []ReplicatedGlobalIndexRelation,
) (ReplicatedShardStoreIdentity, error) {
	return d.bindReplicatedShardStoreBundle(binding, userTable, globalIndexes, nil)
}

func (d *Database) bindReplicatedShardStoreBundle(
	binding ReplicatedShardStoreBinding,
	userTable string,
	globalIndexes []ReplicatedGlobalIndexRelation,
	persist func(*database) (bool, error),
) (ReplicatedShardStoreIdentity, error) {
	if err := validateReplicatedShardStoreBinding(binding); err != nil {
		return ReplicatedShardStoreIdentity{}, err
	}
	if err := validateReplicatedUserTableName(userTable); err != nil {
		return ReplicatedShardStoreIdentity{}, err
	}
	if len(globalIndexes) == 0 || len(globalIndexes)+1 > replication.MaxRelationsPerBundle {
		return ReplicatedShardStoreIdentity{}, fmt.Errorf(
			"%w: global relation count", ErrReplicatedShardStoreProfile,
		)
	}
	ownedGlobals := make([]ReplicatedGlobalIndexRelation, len(globalIndexes))
	seenTables := map[string]struct{}{userTable: {}}
	for i := range globalIndexes {
		relation := globalIndexes[i]
		if relation.Relation != uint16(i+2) ||
			validateReplicatedUserTableName(relation.Table) != nil ||
			relation.IndexID == 0 || relation.Incarnation == 0 ||
			relation.LocatorCount == 0 || relation.LocatorCount > 8 {
			return ReplicatedShardStoreIdentity{}, fmt.Errorf(
				"%w: invalid global relation slot %d", ErrReplicatedShardStoreProfile, i+2,
			)
		}
		if _, duplicate := seenTables[relation.Table]; duplicate {
			return ReplicatedShardStoreIdentity{}, fmt.Errorf(
				"%w: duplicate relation table", ErrReplicatedShardStoreProfile,
			)
		}
		seenTables[relation.Table] = struct{}{}
		relation.Table = strings.Clone(relation.Table)
		ownedGlobals[i] = relation
	}
	sidecars := canonicalReplicatedShardStoreSidecars()
	binding = ownedReplicatedShardStoreBinding(binding)
	userTable = strings.Clone(userTable)
	if d == nil || d.connector == nil {
		return ReplicatedShardStoreIdentity{}, ErrDatabaseClosed
	}

	d.connector.mu.Lock()
	defer d.connector.mu.Unlock()
	if d.connector.closed || d.connector.db == nil {
		return ReplicatedShardStoreIdentity{}, ErrDatabaseClosed
	}
	if d.connector.refs != 0 {
		return ReplicatedShardStoreIdentity{}, fmt.Errorf(
			"%w: %d live SQL session(s)", ErrReplicatedShardStoreBusy, d.connector.refs,
		)
	}
	core := d.connector.db
	core.mu.Lock()
	defer core.mu.Unlock()
	if core.closed {
		return ReplicatedShardStoreIdentity{}, ErrDatabaseClosed
	}
	if current := core.catalog.ReplicatedShardStore; current != nil {
		if current.Binding != binding || current.UserTable != userTable ||
			current.Sidecars != sidecars ||
			!replicatedBundleProvisioningMatches(*current, ownedGlobals) {
			return ReplicatedShardStoreIdentity{}, fmt.Errorf(
				"%w: catalog is already bound", ErrReplicatedShardStoreIdentityMismatch,
			)
		}
		if err := core.settleCatalogLocked(); err != nil {
			return ownedReplicatedShardStoreIdentity(*current), fmt.Errorf(
				"vibedb: settle replicated shard bundle binding: %w", err,
			)
		}
		if err := core.txnLog.EnsureMinted(); err != nil {
			return ownedReplicatedShardStoreIdentity(*current), fmt.Errorf(
				"vibedb: qualify replicated transaction marker: %w", err,
			)
		}
		return ownedReplicatedShardStoreIdentity(*current), nil
	}
	if err := core.settleCatalogLocked(); err != nil {
		return ReplicatedShardStoreIdentity{}, fmt.Errorf(
			"vibedb: settle SQL catalog before replicated bundle bind: %w", err,
		)
	}
	if core.servingClaim != nil || core.catalog.ShardStoreFence != nil || len(core.retired) != 0 {
		return ReplicatedShardStoreIdentity{}, fmt.Errorf(
			"%w: local serving, a durable serving fence, or table retirement is present",
			ErrReplicatedShardStoreBusy,
		)
	}
	if core.catalog.ShardStore == nil {
		return ReplicatedShardStoreIdentity{}, &ShardStoreError{
			Op: "bind replicated bundle", Path: core.path, Err: ErrShardStoreUnbound,
		}
	}
	local := *core.catalog.ShardStore
	if string(local.Distribution) != binding.Distribution ||
		string(local.Shard) != binding.Shard ||
		uint64(local.AllocationGeneration) != binding.AllocationGeneration {
		return ReplicatedShardStoreIdentity{}, &ShardStoreError{
			Op: "bind replicated bundle", Path: core.path,
			Expected: ShardStoreBinding{
				Distribution: distribution.DistributionName(binding.Distribution),
				Shard:        distribution.ShardID(binding.Shard),
				AllocationGeneration: distribution.ShardAllocationGeneration(
					binding.AllocationGeneration,
				),
			},
			Actual: local, Err: ErrReplicatedShardStoreIdentityMismatch,
		}
	}
	if len(core.catalog.Tables) != len(ownedGlobals)+1 || len(core.catalog.Views) != 0 {
		return ReplicatedShardStoreIdentity{}, fmt.Errorf(
			"%w: relation manifest must name the complete table catalog",
			ErrReplicatedShardStoreProfile,
		)
	}
	type pendingRelation struct {
		name      string
		original  *table
		candidate *table
		global    *ReplicatedGlobalIndexRelation
	}
	pending := make([]pendingRelation, 0, len(ownedGlobals)+1)
	appendInput := func(name string, global *ReplicatedGlobalIndexRelation) error {
		t := core.tables[name]
		if t == nil || core.catalog.Tables[name] == nil {
			return fmt.Errorf("%w: missing relation table %q", ErrReplicatedShardStoreProfile, name)
		}
		if err := validateReplicatedTableCatalogProfile(name, t); err != nil {
			return err
		}
		if global != nil && len(t.meta.Indexes) != 0 {
			return fmt.Errorf(
				"%w: global relation %q must be locally unindexed",
				ErrReplicatedShardStoreProfile, name,
			)
		}
		if t.collection != nil || t.file != nil || t.meta.Materialized {
			return fmt.Errorf(
				"%w: relation table %q must be unmaterialized",
				ErrReplicatedShardStoreProfile, name,
			)
		}
		pending = append(pending, pendingRelation{name: name, original: t, global: global})
		return nil
	}
	if err := appendInput(userTable, nil); err != nil {
		return ReplicatedShardStoreIdentity{}, err
	}
	for i := range ownedGlobals {
		if err := appendInput(ownedGlobals[i].Table, &ownedGlobals[i]); err != nil {
			return ReplicatedShardStoreIdentity{}, err
		}
	}
	if err := core.checkRetirementCapacityLocked(len(pending)); err != nil {
		return ReplicatedShardStoreIdentity{}, err
	}
	if core.txnLog == nil {
		return ReplicatedShardStoreIdentity{}, fmt.Errorf(
			"%w: transaction log is unavailable", ErrReplicatedShardStoreProfile,
		)
	}
	previousTxnOptions := core.txnLog.Options()
	sealedTxnOptions := durable.TxnLogOptions{
		Capacity: sidecars.TransactionMarkerBytes, SealedCapacity: true,
	}
	if err := core.txnLog.ReconfigureUnminted(sealedTxnOptions); err != nil {
		return ReplicatedShardStoreIdentity{}, fmt.Errorf(
			"%w: transaction marker must be absent before bind: %w",
			ErrReplicatedShardStoreProfile, err,
		)
	}
	rollback := func(cause error) error {
		errs := []error{cause}
		for i := len(pending) - 1; i >= 0; i-- {
			candidate := pending[i].candidate
			if candidate == nil || candidate.collection == nil {
				continue
			}
			if detachErr := core.txnLog.DetachCollection(candidate.collection); detachErr != nil {
				core.retired = append(core.retired, retiredTable{
					name:    pending[i].name + " (unpublished replicated bundle bind)",
					path:    core.tablePathForMeta(candidate.meta),
					journal: durable.RecoveryJournalPath(core.tablePathForMeta(candidate.meta)),
					file:    candidate.file, collection: candidate.collection,
				})
				candidate.file = nil
				candidate.collection = nil
				errs = append(errs, detachErr)
				continue
			}
			errs = append(errs, core.discardTableStorageLocked(pending[i].name, candidate))
		}
		errs = append(errs, core.txnLog.ReconfigureUnminted(previousTxnOptions))
		return errors.Join(errs...)
	}
	for i := range pending {
		storage, err := core.newStorageIdentityLocked()
		if err != nil {
			return ReplicatedShardStoreIdentity{}, rollback(err)
		}
		meta := cloneTableMeta(pending[i].original.meta)
		meta.Storage = storage
		meta.Materialized = false
		meta.SealedRecoveryJournalBytes = sidecars.UserRecoveryJournalBytes
		candidate := &table{
			meta: meta, schema: pending[i].original.schema, primary: pending[i].original.primary,
		}
		pending[i].candidate = candidate
		if err := durable.ValidateOptions(durableOptions(candidate)); err != nil {
			return ReplicatedShardStoreIdentity{}, rollback(fmt.Errorf(
				"%w: relation %q sealed options: %v",
				ErrReplicatedShardStoreProfile, pending[i].name, err,
			))
		}
		if err := core.buildReplacementStorageLocked(
			context.Background(), pending[i].name, pending[i].original, candidate, false,
		); err != nil {
			return ReplicatedShardStoreIdentity{}, rollback(err)
		}
		if err := core.txnLog.AdoptCollection(candidate.collection); err != nil {
			cleanupErr := core.discardTableStorageLocked(pending[i].name, candidate)
			candidate.collection = nil
			candidate.file = nil
			return ReplicatedShardStoreIdentity{}, rollback(errors.Join(err, cleanupErr))
		}
	}
	identity, err := replicatedIdentityForTable(
		binding, local.LogID, userTable, pending[0].candidate, sidecars, true,
	)
	if err != nil {
		return ReplicatedShardStoreIdentity{}, rollback(err)
	}
	identity.RelationCount = uint16(len(pending))
	identity.RelationSchemaGeneration = binding.Authority.SchemaGeneration
	for i := range pending {
		candidate := pending[i].candidate
		relation := ReplicatedShardRelationIdentity{
			Relation: uint16(i + 1), Table: strings.Clone(pending[i].name),
			Storage: strings.Clone(candidate.meta.Storage),
			Limits: ReplicatedShardStoreLimits{
				MaxKeyBytes:       candidate.collection.MaxKeyBytes(),
				MaxDocumentBytes:  candidate.collection.MaxDocumentBytes(),
				MaxBatchDocuments: candidate.collection.MaxBatchDocuments(),
				MaxBatchBytes:     candidate.collection.MaxBatchBytes(),
			},
		}
		if pending[i].global == nil {
			relation.Kind = ReplicatedShardRelationJSON
			relation.LocalIndexDigest = replicatedLocalIndexDigest(candidate.meta.Indexes)
		} else {
			relation.Kind = ReplicatedShardRelationGlobalIndex
			relation.IndexID = pending[i].global.IndexID
			relation.Incarnation = pending[i].global.Incarnation
			relation.LocatorCount = pending[i].global.LocatorCount
			relation.Unique = pending[i].global.Unique
		}
		identity.Relations[i] = relation
	}
	identity.RelationManifestDigest = replicatedRelationManifestDigest(identity)
	if err := validateReplicatedShardStoreIdentity(identity); err != nil {
		return ReplicatedShardStoreIdentity{}, rollback(err)
	}
	members := make([]durable.NamedCollection, len(pending))
	for i := range pending {
		members[i] = durable.NamedCollection{
			Name: pending[i].name, Collection: pending[i].candidate.collection,
		}
	}
	if err := core.txnLog.ValidateCollections(members); err != nil {
		return ReplicatedShardStoreIdentity{}, rollback(fmt.Errorf(
			"%w: transaction-log bundle membership: %v",
			ErrReplicatedShardStoreProfile, err,
		))
	}
	previousPending := core.catalogWritePending
	for i := range pending {
		core.catalog.Tables[pending[i].name] = pending[i].candidate.meta
		core.tables[pending[i].name] = pending[i].candidate
	}
	stored := ownedReplicatedShardStoreIdentity(identity)
	core.catalog.ReplicatedShardStore = &stored
	core.catalogWritePending = true
	core.advanceLayoutEpochLocked()
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
		if !published && !errors.Is(err, durable.ErrCommitOutcomeUnknown) {
			for i := range pending {
				core.catalog.Tables[pending[i].name] = pending[i].original.meta
				core.tables[pending[i].name] = pending[i].original
			}
			core.catalog.ReplicatedShardStore = nil
			core.catalogWritePending = previousPending
			core.advanceLayoutEpochLocked()
			return ReplicatedShardStoreIdentity{}, rollback(fmt.Errorf(
				"vibedb: publish replicated shard bundle binding: %w", err,
			))
		}
		if !published {
			core.catalogWritePending = true
		}
		return ownedReplicatedShardStoreIdentity(identity), fmt.Errorf(
			"vibedb: publish replicated shard bundle binding: %w", err,
		)
	}
	if err := core.txnLog.EnsureMinted(); err != nil {
		return ownedReplicatedShardStoreIdentity(identity), fmt.Errorf(
			"vibedb: qualify replicated transaction marker: %w", err,
		)
	}
	return ownedReplicatedShardStoreIdentity(identity), nil
}

func replicatedBundleProvisioningMatches(
	identity ReplicatedShardStoreIdentity,
	globalIndexes []ReplicatedGlobalIndexRelation,
) bool {
	if int(identity.RelationCount) != len(globalIndexes)+1 {
		return false
	}
	for i := range globalIndexes {
		got := identity.Relations[i+1]
		want := globalIndexes[i]
		if got.Relation != want.Relation ||
			got.Kind != ReplicatedShardRelationGlobalIndex || got.Table != want.Table ||
			got.IndexID != want.IndexID || got.Incarnation != want.Incarnation ||
			got.LocatorCount != want.LocatorCount || got.Unique != want.Unique {
			return false
		}
	}
	return true
}

func (d *Database) bindReplicatedShardStore(
	binding ReplicatedShardStoreBinding,
	userTable string,
	persist func(*database) (bool, error),
) (ReplicatedShardStoreIdentity, error) {
	if err := validateReplicatedShardStoreBinding(binding); err != nil {
		return ReplicatedShardStoreIdentity{}, err
	}
	if err := validateReplicatedUserTableName(userTable); err != nil {
		return ReplicatedShardStoreIdentity{}, err
	}
	sidecars := canonicalReplicatedShardStoreSidecars()
	binding = ownedReplicatedShardStoreBinding(binding)
	userTable = strings.Clone(userTable)
	if d == nil || d.connector == nil {
		return ReplicatedShardStoreIdentity{}, ErrDatabaseClosed
	}

	// Keep connector.mu continuously through publication. Connect takes the same
	// lock, so either it establishes a live ref first (and bind returns Busy), or
	// it receives a connection whose immutable direct-write flag is fenced.
	d.connector.mu.Lock()
	defer d.connector.mu.Unlock()
	if d.connector.closed || d.connector.db == nil {
		return ReplicatedShardStoreIdentity{}, ErrDatabaseClosed
	}
	if d.connector.refs != 0 {
		return ReplicatedShardStoreIdentity{}, fmt.Errorf(
			"%w: %d live SQL session(s)", ErrReplicatedShardStoreBusy, d.connector.refs,
		)
	}
	core := d.connector.db
	core.mu.Lock()
	defer core.mu.Unlock()
	if core.closed {
		return ReplicatedShardStoreIdentity{}, ErrDatabaseClosed
	}
	if current := core.catalog.ReplicatedShardStore; current != nil {
		if current.Binding != binding || current.UserTable != userTable ||
			current.Sidecars != sidecars {
			return ReplicatedShardStoreIdentity{}, fmt.Errorf(
				"%w: catalog is already bound", ErrReplicatedShardStoreIdentityMismatch,
			)
		}
		if err := core.settleCatalogLocked(); err != nil {
			return ownedReplicatedShardStoreIdentity(*current), fmt.Errorf(
				"vibedb: settle replicated shard store binding: %w", err,
			)
		}
		if err := core.txnLog.EnsureMinted(); err != nil {
			return ownedReplicatedShardStoreIdentity(*current), fmt.Errorf(
				"vibedb: qualify replicated transaction marker: %w", err,
			)
		}
		return ownedReplicatedShardStoreIdentity(*current), nil
	}
	if err := core.settleCatalogLocked(); err != nil {
		return ReplicatedShardStoreIdentity{}, fmt.Errorf(
			"vibedb: settle SQL catalog before replicated bind: %w", err,
		)
	}
	if core.servingClaim != nil || core.catalog.ShardStoreFence != nil || len(core.retired) != 0 {
		return ReplicatedShardStoreIdentity{}, fmt.Errorf(
			"%w: local serving, a durable serving fence, or table retirement is present",
			ErrReplicatedShardStoreBusy,
		)
	}
	if core.catalog.ShardStore == nil {
		return ReplicatedShardStoreIdentity{}, &ShardStoreError{
			Op: "bind replicated", Path: core.path, Err: ErrShardStoreUnbound,
		}
	}
	local := *core.catalog.ShardStore
	if string(local.Distribution) != binding.Distribution ||
		string(local.Shard) != binding.Shard ||
		uint64(local.AllocationGeneration) != binding.AllocationGeneration {
		return ReplicatedShardStoreIdentity{}, &ShardStoreError{
			Op: "bind replicated", Path: core.path,
			Expected: ShardStoreBinding{
				Distribution: distribution.DistributionName(binding.Distribution),
				Shard:        distribution.ShardID(binding.Shard),
				AllocationGeneration: distribution.ShardAllocationGeneration(
					binding.AllocationGeneration,
				),
			},
			Actual: local, Err: ErrReplicatedShardStoreIdentityMismatch,
		}
	}
	if len(core.catalog.Tables) != 1 || len(core.catalog.Views) != 0 {
		return ReplicatedShardStoreIdentity{}, fmt.Errorf(
			"%w: require exactly one table and no views", ErrReplicatedShardStoreProfile,
		)
	}
	t := core.tables[userTable]
	if t == nil || core.catalog.Tables[userTable] == nil {
		return ReplicatedShardStoreIdentity{}, fmt.Errorf(
			"%w: user table %q is not the sole catalog table",
			ErrReplicatedShardStoreProfile, userTable,
		)
	}
	if err := validateReplicatedTableCatalogProfile(userTable, t); err != nil {
		return ReplicatedShardStoreIdentity{}, err
	}
	if t.collection != nil || t.file != nil || t.meta.Materialized {
		return ReplicatedShardStoreIdentity{}, fmt.Errorf(
			"%w: user table %q must be unmaterialized",
			ErrReplicatedShardStoreProfile, userTable,
		)
	}
	if err := core.checkRetirementCapacityLocked(1); err != nil {
		return ReplicatedShardStoreIdentity{}, err
	}
	if core.txnLog == nil {
		return ReplicatedShardStoreIdentity{}, fmt.Errorf(
			"%w: transaction log is unavailable", ErrReplicatedShardStoreProfile,
		)
	}
	previousTxnOptions := core.txnLog.Options()
	sealedTxnOptions := durable.TxnLogOptions{
		Capacity: sidecars.TransactionMarkerBytes, SealedCapacity: true,
	}
	if err := core.txnLog.ReconfigureUnminted(sealedTxnOptions); err != nil {
		return ReplicatedShardStoreIdentity{}, fmt.Errorf(
			"%w: transaction marker must be absent before bind: %w",
			ErrReplicatedShardStoreProfile, err,
		)
	}
	storage, err := core.newStorageIdentityLocked()
	if err != nil {
		return ReplicatedShardStoreIdentity{}, errors.Join(
			err, core.txnLog.ReconfigureUnminted(previousTxnOptions),
		)
	}
	meta := cloneTableMeta(t.meta)
	meta.Storage = storage
	meta.Materialized = false
	meta.SealedRecoveryJournalBytes = sidecars.UserRecoveryJournalBytes
	candidate := &table{meta: meta, schema: t.schema, primary: t.primary}
	rollbackProfile := func(cause error) error {
		return errors.Join(cause, core.txnLog.ReconfigureUnminted(previousTxnOptions))
	}
	if err := durable.ValidateOptions(durableOptions(candidate)); err != nil {
		return ReplicatedShardStoreIdentity{}, rollbackProfile(fmt.Errorf(
			"%w: sealed user storage options: %v",
			ErrReplicatedShardStoreProfile, err,
		))
	}
	if err := core.buildReplacementStorageLocked(
		context.Background(), userTable, t, candidate, false,
	); err != nil {
		return ReplicatedShardStoreIdentity{}, rollbackProfile(err)
	}
	if err := core.txnLog.AdoptCollection(candidate.collection); err != nil {
		return ReplicatedShardStoreIdentity{}, rollbackProfile(errors.Join(
			err, core.discardTableStorageLocked(userTable, candidate),
		))
	}
	rollbackCandidate := func(cause error) error {
		detachErr := core.txnLog.DetachCollection(candidate.collection)
		if detachErr != nil {
			core.retired = append(core.retired, retiredTable{
				name: userTable + " (unpublished replicated bind)",
				path: core.tablePathForMeta(candidate.meta),
				journal: durable.RecoveryJournalPath(
					core.tablePathForMeta(candidate.meta),
				),
				file: candidate.file, collection: candidate.collection,
			})
			candidate.file = nil
			candidate.collection = nil
			return rollbackProfile(errors.Join(cause, detachErr))
		}
		return rollbackProfile(errors.Join(
			cause, core.discardTableStorageLocked(userTable, candidate),
		))
	}
	identity, err := replicatedIdentityForTable(
		binding, local.LogID, userTable, candidate, sidecars, true,
	)
	if err != nil {
		return ReplicatedShardStoreIdentity{}, rollbackCandidate(err)
	}
	if err := core.txnLog.ValidateCollections([]durable.NamedCollection{{
		Name: userTable, Collection: candidate.collection,
	}}); err != nil {
		return ReplicatedShardStoreIdentity{}, rollbackCandidate(
			fmt.Errorf(
				"%w: transaction-log membership: %v",
				ErrReplicatedShardStoreProfile, err,
			),
		)
	}

	previousPending := core.catalogWritePending
	oldMeta := core.catalog.Tables[userTable]
	stored := ownedReplicatedShardStoreIdentity(identity)
	core.catalog.Tables[userTable] = meta
	core.tables[userTable] = candidate
	core.catalog.ReplicatedShardStore = &stored
	core.catalogWritePending = true
	core.advanceLayoutEpochLocked()
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
		if !published && !errors.Is(err, durable.ErrCommitOutcomeUnknown) {
			core.catalog.Tables[userTable] = oldMeta
			core.tables[userTable] = t
			core.catalog.ReplicatedShardStore = nil
			core.catalogWritePending = previousPending
			core.advanceLayoutEpochLocked()
			detachErr := core.txnLog.DetachCollection(candidate.collection)
			if detachErr != nil {
				core.retired = append(core.retired, retiredTable{
					name: userTable + " (unpublished replicated bind)",
					path: core.tablePathForMeta(candidate.meta),
					journal: durable.RecoveryJournalPath(
						core.tablePathForMeta(candidate.meta),
					),
					file: candidate.file, collection: candidate.collection,
				})
				candidate.file = nil
				candidate.collection = nil
				restoreErr := core.txnLog.ReconfigureUnminted(previousTxnOptions)
				return ReplicatedShardStoreIdentity{}, errors.Join(
					fmt.Errorf("vibedb: publish replicated shard store binding: %w", err),
					detachErr, restoreErr,
				)
			}
			cleanupErr := core.discardTableStorageLocked(userTable, candidate)
			restoreErr := core.txnLog.ReconfigureUnminted(previousTxnOptions)
			return ReplicatedShardStoreIdentity{}, errors.Join(
				fmt.Errorf("vibedb: publish replicated shard store binding: %w", err),
				cleanupErr, restoreErr,
			)
		} else if !published {
			core.catalogWritePending = true
		}
		publicationErr := fmt.Errorf(
			"vibedb: publish replicated shard store binding: %w", err,
		)
		if published || errors.Is(err, durable.ErrCommitOutcomeUnknown) {
			return ownedReplicatedShardStoreIdentity(identity), publicationErr
		}
		return ReplicatedShardStoreIdentity{}, publicationErr
	}
	if err := core.txnLog.EnsureMinted(); err != nil {
		return ownedReplicatedShardStoreIdentity(identity), fmt.Errorf(
			"vibedb: qualify replicated transaction marker: %w", err,
		)
	}
	return ownedReplicatedShardStoreIdentity(identity), nil
}

func validateReplicatedTableCatalogProfile(name string, t *table) error {
	if t == nil || t.meta == nil || t.schema != nil || t.meta.Schema != nil ||
		t.meta.Storage == "" {
		return fmt.Errorf(
			"%w: table %q catalog metadata must be schema-free with an explicit storage identity",
			ErrReplicatedShardStoreProfile, name,
		)
	}
	primary, err := vibejson.CompilePointer(t.meta.PrimaryKey)
	if err != nil || len(primary.Tokens) == 0 {
		return fmt.Errorf(
			"%w: table %q has an invalid primary key", ErrReplicatedShardStoreProfile, name,
		)
	}
	normalized, err := durable.NormalizeOptions(durableOptions(t))
	if err != nil {
		return fmt.Errorf("%w: table %q options: %v", ErrReplicatedShardStoreProfile, name, err)
	}
	limits := ReplicatedShardStoreLimits{
		MaxKeyBytes: normalized.MaxKeyBytes, MaxDocumentBytes: normalized.MaxDocumentBytes,
		MaxBatchDocuments: normalized.MaxBatchDocuments, MaxBatchBytes: normalized.MaxBatchBytes,
	}
	if err := validateReplicatedShardStoreLimits(limits); err != nil {
		return fmt.Errorf("%w: table %q limits: %v", ErrReplicatedShardStoreProfile, name, err)
	}
	return nil
}

// OpenReplicatedShardStore opens only the complete retained identity. The
// comparison, including SQL LogID and explicit table profile, occurs before
// namespace or transaction recovery.
func OpenReplicatedShardStore(
	path string,
	expected ReplicatedShardStoreIdentity,
) (*Database, error) {
	if err := validateReplicatedShardStoreIdentity(expected); err != nil {
		return nil, err
	}
	expected = ownedReplicatedShardStoreIdentity(expected)
	absolute, err := canonicalCatalogPath(path)
	if err != nil {
		return nil, err
	}
	if _, err := os.Stat(absolute); err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("%w: %s", ErrReplicatedShardStoreUnbound, absolute)
		}
		return nil, err
	}
	core, err := openDatabaseWithShardStorePolicy(path, nil, shardStoreOpenPolicy{
		mode:               shardStoreOpenReplicatedExisting,
		expectedReplicated: expected,
	})
	if err != nil {
		return nil, err
	}
	return &Database{connector: &dbConnector{db: core}}, nil
}

// OpenReplicatedShardStoreForSettlement recovers the narrow crash window in
// which the catalog rename published but BindReplicatedShardStore's full return
// value never reached its caller. Callers must have retained the preexisting
// SQL LogID before bind, as well as the intended WAL binding and user table.
// All three are compared before namespace or transaction recovery; a binding-
// only recovery is intentionally unavailable. On success the returned full
// identity becomes the exact input to future OpenReplicatedShardStore calls.
func OpenReplicatedShardStoreForSettlement(
	path string,
	expected ReplicatedShardStoreBinding,
	expectedLogID [16]byte,
	userTable string,
) (*Database, ReplicatedShardStoreIdentity, error) {
	if err := validateReplicatedShardStoreBinding(expected); err != nil {
		return nil, ReplicatedShardStoreIdentity{}, err
	}
	if expectedLogID == ([16]byte{}) {
		return nil, ReplicatedShardStoreIdentity{}, errors.New(
			"vibedb: replicated settlement requires the retained SQL log id",
		)
	}
	if err := validateReplicatedUserTableName(userTable); err != nil {
		return nil, ReplicatedShardStoreIdentity{}, err
	}
	sidecars := canonicalReplicatedShardStoreSidecars()
	expected = ownedReplicatedShardStoreBinding(expected)
	userTable = strings.Clone(userTable)
	absolute, err := canonicalCatalogPath(path)
	if err != nil {
		return nil, ReplicatedShardStoreIdentity{}, err
	}
	if _, err := os.Stat(absolute); err != nil {
		if os.IsNotExist(err) {
			return nil, ReplicatedShardStoreIdentity{}, fmt.Errorf(
				"%w: %s", ErrReplicatedShardStoreUnbound, absolute,
			)
		}
		return nil, ReplicatedShardStoreIdentity{}, err
	}
	core, err := openDatabaseWithShardStorePolicy(path, nil, shardStoreOpenPolicy{
		mode: shardStoreOpenReplicatedSettlement,
		expectedReplicated: ReplicatedShardStoreIdentity{
			Binding: expected, Sidecars: sidecars,
		},
		expectedReplicatedLogID:     expectedLogID,
		expectedReplicatedUserTable: userTable,
	})
	if err != nil {
		return nil, ReplicatedShardStoreIdentity{}, err
	}
	identity := ownedReplicatedShardStoreIdentity(*core.catalog.ReplicatedShardStore)
	return &Database{connector: &dbConnector{db: core}}, identity, nil
}

// ReplicatedShardStoreIdentity returns the complete immutable binding and
// explicit collection profile. Ordinary/local shard catalogs are unbound.
func (d *Database) ReplicatedShardStoreIdentity() (ReplicatedShardStoreIdentity, error) {
	if d == nil || d.connector == nil {
		return ReplicatedShardStoreIdentity{}, ErrDatabaseClosed
	}
	d.connector.mu.Lock()
	if d.connector.closed || d.connector.db == nil {
		d.connector.mu.Unlock()
		return ReplicatedShardStoreIdentity{}, ErrDatabaseClosed
	}
	core := d.connector.db
	d.connector.mu.Unlock()
	core.mu.RLock()
	defer core.mu.RUnlock()
	if core.closed {
		return ReplicatedShardStoreIdentity{}, ErrDatabaseClosed
	}
	if core.catalog.ReplicatedShardStore == nil {
		return ReplicatedShardStoreIdentity{}, fmt.Errorf(
			"%w: %s", ErrReplicatedShardStoreUnbound, core.path,
		)
	}
	return ownedReplicatedShardStoreIdentity(*core.catalog.ReplicatedShardStore), nil
}

// RequireReplicatedShardStore compares the entire retained identity, including
// SQL LogID and the explicit user table profile.
func (d *Database) RequireReplicatedShardStore(
	expected ReplicatedShardStoreIdentity,
) (ReplicatedShardStoreIdentity, error) {
	if err := validateReplicatedShardStoreIdentity(expected); err != nil {
		return ReplicatedShardStoreIdentity{}, err
	}
	actual, err := d.ReplicatedShardStoreIdentity()
	if err != nil {
		return ReplicatedShardStoreIdentity{}, err
	}
	if actual != expected {
		return ReplicatedShardStoreIdentity{}, ErrReplicatedShardStoreIdentityMismatch
	}
	return actual, nil
}

func replicatedIdentityForTable(
	binding ReplicatedShardStoreBinding,
	logID [16]byte,
	name string,
	t *table,
	sidecars ReplicatedShardStoreSidecarProfile,
	requireEmpty bool,
) (ReplicatedShardStoreIdentity, error) {
	if t == nil || t.meta == nil || t.collection == nil ||
		t.schema != nil || t.meta.Schema != nil || t.collection.HasSchema() ||
		t.collection.HasIndexes() != (len(t.meta.Indexes) != 0) ||
		!t.collection.HasSynchronousDurability() || !t.collection.SupportsUpdate() ||
		t.collection.SealedRecoveryJournalBytes() != sidecars.UserRecoveryJournalBytes ||
		!t.meta.Materialized || t.meta.Storage == "" {
		return ReplicatedShardStoreIdentity{}, fmt.Errorf(
			"%w: table %q must be materialized, schema-free, synchronously indexed, and exactly sealed",
			ErrReplicatedShardStoreProfile, name,
		)
	}
	if requireEmpty && t.collection.Len() != 0 {
		return ReplicatedShardStoreIdentity{}, fmt.Errorf(
			"%w: table %q must be empty at bind", ErrReplicatedShardStoreProfile, name,
		)
	}
	limits := ReplicatedShardStoreLimits{
		MaxKeyBytes:       t.collection.MaxKeyBytes(),
		MaxDocumentBytes:  t.collection.MaxDocumentBytes(),
		MaxBatchDocuments: t.collection.MaxBatchDocuments(),
		MaxBatchBytes:     t.collection.MaxBatchBytes(),
	}
	identity := ReplicatedShardStoreIdentity{
		Format:         ReplicatedShardStoreFormat,
		Binding:        binding,
		LogID:          logID,
		UserTable:      strings.Clone(name),
		UserStorage:    strings.Clone(t.meta.Storage),
		UserPrimaryKey: strings.Clone(t.meta.PrimaryKey),
		UserLimits:     limits,
		Sidecars:       sidecars,
	}
	if err := validateReplicatedShardStoreIdentity(identity); err != nil {
		return ReplicatedShardStoreIdentity{}, fmt.Errorf(
			"%w: %v", ErrReplicatedShardStoreProfile, err,
		)
	}
	return identity, nil
}

func validateReplicatedCatalog(catalog catalogFile) error {
	r := catalog.ReplicatedShardStore
	if r == nil {
		if catalog.ReplicatedApply != nil {
			return fmt.Errorf("%w: replicated apply requires a replicated shard binding", ErrReplicatedApplyMismatch)
		}
		return nil
	}
	if err := validateReplicatedShardStoreIdentity(*r); err != nil {
		return err
	}
	if catalog.ShardStore == nil {
		return fmt.Errorf("%w: local shard identity is missing", ErrReplicatedShardStoreProfile)
	}
	if catalog.ShardStoreFence != nil {
		return fmt.Errorf("%w: local serving fence is present", ErrReplicatedShardStoreProfile)
	}
	local := catalog.ShardStore
	if r.LogID != local.LogID || r.Binding.Distribution != string(local.Distribution) ||
		r.Binding.Shard != string(local.Shard) ||
		r.Binding.AllocationGeneration != uint64(local.AllocationGeneration) {
		return ErrReplicatedShardStoreIdentityMismatch
	}
	wantTables := 1
	if r.RelationCount != 0 {
		wantTables = int(r.RelationCount)
	}
	if len(catalog.Tables) != wantTables || len(catalog.Views) != 0 {
		return fmt.Errorf(
			"%w: relation manifest must name the complete table catalog",
			ErrReplicatedShardStoreProfile,
		)
	}
	validateRelation := func(relation ReplicatedShardRelationIdentity) error {
		meta := catalog.Tables[relation.Table]
		if meta == nil || meta.Schema != nil || !meta.Materialized ||
			meta.Storage == "" || meta.Storage != relation.Storage ||
			meta.SealedRecoveryJournalBytes != r.Sidecars.UserRecoveryJournalBytes ||
			relation.Kind == ReplicatedShardRelationGlobalIndex && len(meta.Indexes) != 0 {
			return fmt.Errorf(
				"%w: durable relation %q metadata differs",
				ErrReplicatedShardStoreProfile, relation.Table,
			)
		}
		if relation.Kind == ReplicatedShardRelationJSON &&
			(meta.PrimaryKey != r.UserPrimaryKey ||
				replicatedLocalIndexDigest(meta.Indexes) != relation.LocalIndexDigest) {
			return fmt.Errorf("%w: durable base relation metadata differs", ErrReplicatedShardStoreProfile)
		}
		normalized, err := durable.NormalizeOptions(durableOptions(&table{meta: meta}))
		if err != nil {
			return fmt.Errorf("%w: %v", ErrReplicatedShardStoreProfile, err)
		}
		limits := ReplicatedShardStoreLimits{
			MaxKeyBytes: normalized.MaxKeyBytes, MaxDocumentBytes: normalized.MaxDocumentBytes,
			MaxBatchDocuments: normalized.MaxBatchDocuments, MaxBatchBytes: normalized.MaxBatchBytes,
		}
		if limits != relation.Limits {
			return fmt.Errorf(
				"%w: durable relation %q limits differ",
				ErrReplicatedShardStoreProfile, relation.Table,
			)
		}
		return nil
	}
	if r.RelationCount == 0 {
		var localIndexDigest [sha256.Size]byte
		if meta := catalog.Tables[r.UserTable]; meta != nil {
			localIndexDigest = replicatedLocalIndexDigest(meta.Indexes)
		}
		if err := validateRelation(ReplicatedShardRelationIdentity{
			Relation: 1, Kind: ReplicatedShardRelationJSON,
			Table: r.UserTable, Storage: r.UserStorage, Limits: r.UserLimits,
			LocalIndexDigest: localIndexDigest,
		}); err != nil {
			return err
		}
	} else {
		for ordinal := 0; ordinal < int(r.RelationCount); ordinal++ {
			if err := validateRelation(r.Relations[ordinal]); err != nil {
				return err
			}
		}
	}
	if err := validateReplicatedApplyMeta(catalog.ReplicatedApply, r); err != nil {
		return err
	}
	return nil
}

func validateOpenedReplicatedCatalog(d *database) error {
	if d == nil || d.catalog.ReplicatedShardStore == nil {
		return ErrReplicatedShardStoreUnbound
	}
	r := *d.catalog.ReplicatedShardStore
	t := d.tables[r.UserTable]
	actual, err := replicatedIdentityForTable(
		r.Binding, r.LogID, r.UserTable, t, r.Sidecars, false,
	)
	if err != nil {
		return err
	}
	if r.RelationCount != 0 {
		actual.RelationCount = r.RelationCount
		actual.RelationSchemaGeneration = r.RelationSchemaGeneration
		actual.RelationManifestDigest = r.RelationManifestDigest
		actual.Relations = r.Relations
	}
	if actual != r {
		return ErrReplicatedShardStoreIdentityMismatch
	}
	members := make([]durable.NamedCollection, 0, max(1, int(r.RelationCount))+1)
	if r.RelationCount == 0 {
		members = append(members, durable.NamedCollection{
			Name: r.UserTable, Collection: t.collection,
		})
	} else {
		for ordinal := 0; ordinal < int(r.RelationCount); ordinal++ {
			relation := r.Relations[ordinal]
			table := d.tables[relation.Table]
			if !openedReplicatedRelationMatches(table, relation, r.Sidecars) {
				return fmt.Errorf(
					"%w: opened relation %q differs",
					ErrReplicatedShardStoreProfile, relation.Table,
				)
			}
			members = append(members, durable.NamedCollection{
				Name: relation.Table, Collection: table.collection,
			})
		}
	}
	if d.catalog.ReplicatedApply != nil {
		if err := validateReplicatedApplyCollection(
			d.replicatedApplyCollection, d.catalog.ReplicatedApply.SystemLimits,
			d.catalog.ReplicatedApply.Sidecars,
		); err != nil {
			return err
		}
		members = append(members, durable.NamedCollection{
			Name:       replicatedstate.SystemCollectionName,
			Collection: d.replicatedApplyCollection,
		})
	}
	if err := d.txnLog.ValidateCollections(members); err != nil {
		return fmt.Errorf("%w: transaction-log membership: %v", ErrReplicatedShardStoreProfile, err)
	}
	if d.txnLog.Options() != (durable.TxnLogOptions{
		Capacity: r.Sidecars.TransactionMarkerBytes, SealedCapacity: true,
	}) {
		return fmt.Errorf(
			"%w: transaction-marker profile differs",
			ErrReplicatedShardStoreProfile,
		)
	}
	return nil
}

func openedReplicatedRelationMatches(
	t *table,
	relation ReplicatedShardRelationIdentity,
	sidecars ReplicatedShardStoreSidecarProfile,
) bool {
	if t == nil || t.meta == nil || t.collection == nil || t.schema != nil ||
		t.meta.Schema != nil || t.collection.HasSchema() ||
		!t.collection.HasSynchronousDurability() || !t.collection.SupportsUpdate() ||
		t.collection.SealedRecoveryJournalBytes() != sidecars.UserRecoveryJournalBytes ||
		!t.meta.Materialized || t.meta.Storage != relation.Storage ||
		relation.Limits != (ReplicatedShardStoreLimits{
			MaxKeyBytes:       t.collection.MaxKeyBytes(),
			MaxDocumentBytes:  t.collection.MaxDocumentBytes(),
			MaxBatchDocuments: t.collection.MaxBatchDocuments(),
			MaxBatchBytes:     t.collection.MaxBatchBytes(),
		}) {
		return false
	}
	switch relation.Kind {
	case ReplicatedShardRelationJSON:
		return t.collection.HasIndexes() == (len(t.meta.Indexes) != 0) &&
			replicatedLocalIndexDigest(t.meta.Indexes) == relation.LocalIndexDigest
	case ReplicatedShardRelationGlobalIndex:
		return !t.collection.HasIndexes() && len(t.meta.Indexes) == 0
	default:
		return false
	}
}

func validateReplicatedShardStoreBinding(b ReplicatedShardStoreBinding) error {
	a := b.Authority
	if b.ClusterID == ([16]byte{}) || b.ClusterIncarnation == ([16]byte{}) ||
		b.ShardIncarnation == ([16]byte{}) || b.GroupID == ([16]byte{}) ||
		b.StoreID == ([16]byte{}) || b.TopologyRecoveryEpoch == 0 ||
		b.AllocationGeneration == 0 || b.MemberID == 0 ||
		b.MemberID == math.MaxUint64 || b.MemberID == math.MaxUint64-1 ||
		a.ActivePolicyGeneration == 0 || a.ProtectionEpoch == 0 ||
		a.OwnershipEpoch == 0 || a.SchemaGeneration == 0 ||
		a.RoutingVersion == 0 || a.RouteGeneration == 0 {
		return errors.New("vibedb: replicated shard store binding contains a zero identity or generation")
	}
	for label, value := range map[string]string{"distribution": b.Distribution, "shard": b.Shard} {
		if value == "" || len(value) > replicatedMaxIdentityBytes ||
			!utf8.ValidString(value) || strings.IndexByte(value, 0) >= 0 {
			return fmt.Errorf("vibedb: replicated %s is not a valid bounded identity", label)
		}
	}
	return nil
}

func validateReplicatedUserTableName(name string) error {
	if err := validateCatalogTableName(name); err != nil {
		return fmt.Errorf("%w: %v", ErrReplicatedShardStoreProfile, err)
	}
	if strings.IndexByte(name, 0) >= 0 {
		return fmt.Errorf("%w: table name contains NUL", ErrReplicatedShardStoreProfile)
	}
	return nil
}

func validateReplicatedShardStoreIdentity(i ReplicatedShardStoreIdentity) error {
	if i.Format != ReplicatedShardStoreFormat {
		return fmt.Errorf("vibedb: unsupported replicated shard store format %d", i.Format)
	}
	if err := validateReplicatedShardStoreBinding(i.Binding); err != nil {
		return err
	}
	if i.LogID == ([16]byte{}) {
		return errors.New("vibedb: SQL log id must be nonzero")
	}
	if err := validateReplicatedUserTableName(i.UserTable); err != nil {
		return err
	}
	if i.UserStorage == "" {
		return fmt.Errorf("%w: user storage identity is empty", ErrReplicatedShardStoreProfile)
	}
	if err := validateStorageIdentity(i.UserStorage); err != nil {
		return fmt.Errorf("%w: user storage identity: %v", ErrReplicatedShardStoreProfile, err)
	}
	primary, err := vibejson.CompilePointer(i.UserPrimaryKey)
	if err != nil || len(primary.Tokens) == 0 || primary.String() != i.UserPrimaryKey {
		return fmt.Errorf("%w: invalid user primary key", ErrReplicatedShardStoreProfile)
	}
	if err := validateReplicatedShardStoreLimits(i.UserLimits); err != nil {
		return err
	}
	if err := validateReplicatedShardStoreSidecarsForLimits(i.Sidecars, i.UserLimits); err != nil {
		return err
	}
	if err := validateReplicatedRelationManifest(i); err != nil {
		return err
	}
	return nil
}

var replicatedRelationManifestDomain = []byte(
	"vibedb/sql/replicated-relation-manifest\x00",
)

var replicatedLocalIndexManifestDomain = []byte(
	"vibedb/sql/replicated-local-index-manifest\x00",
)

func validateReplicatedRelationManifest(identity ReplicatedShardStoreIdentity) error {
	count := int(identity.RelationCount)
	if count == 0 {
		if identity.RelationSchemaGeneration != 0 ||
			identity.RelationManifestDigest != ([sha256.Size]byte{}) {
			return fmt.Errorf("%w: partial singleton relation manifest", ErrReplicatedShardStoreProfile)
		}
		for i := range identity.Relations {
			if identity.Relations[i] != (ReplicatedShardRelationIdentity{}) {
				return fmt.Errorf("%w: singleton relation manifest residue", ErrReplicatedShardStoreProfile)
			}
		}
		return nil
	}
	if count < 2 || count > replication.MaxRelationsPerBundle ||
		identity.RelationSchemaGeneration != identity.Binding.Authority.SchemaGeneration ||
		identity.RelationManifestDigest == ([sha256.Size]byte{}) {
		return fmt.Errorf("%w: invalid relation manifest header", ErrReplicatedShardStoreProfile)
	}
	seen := make(map[string]struct{}, count)
	for ordinal := 0; ordinal < count; ordinal++ {
		relation := identity.Relations[ordinal]
		if relation.Relation != uint16(ordinal+1) ||
			validateReplicatedUserTableName(relation.Table) != nil ||
			relation.Storage == "" || validateStorageIdentity(relation.Storage) != nil ||
			validateReplicatedShardStoreLimits(relation.Limits) != nil {
			return fmt.Errorf("%w: invalid relation slot %d", ErrReplicatedShardStoreProfile, ordinal+1)
		}
		if _, duplicate := seen[relation.Table]; duplicate {
			return fmt.Errorf("%w: duplicate relation table", ErrReplicatedShardStoreProfile)
		}
		seen[relation.Table] = struct{}{}
		switch relation.Kind {
		case ReplicatedShardRelationJSON:
			if ordinal != 0 || relation.Table != identity.UserTable ||
				relation.Storage != identity.UserStorage || relation.Limits != identity.UserLimits ||
				relation.IndexID != 0 || relation.Incarnation != 0 ||
				relation.LocatorCount != 0 || relation.Unique {
				return fmt.Errorf("%w: invalid base relation", ErrReplicatedShardStoreProfile)
			}
		case ReplicatedShardRelationGlobalIndex:
			if ordinal == 0 || relation.LocalIndexDigest != ([sha256.Size]byte{}) ||
				relation.IndexID == 0 || relation.Incarnation == 0 ||
				relation.LocatorCount == 0 || relation.LocatorCount > 8 {
				return fmt.Errorf("%w: invalid global-index relation", ErrReplicatedShardStoreProfile)
			}
		default:
			return fmt.Errorf("%w: unknown relation kind", ErrReplicatedShardStoreProfile)
		}
	}
	for ordinal := count; ordinal < len(identity.Relations); ordinal++ {
		if identity.Relations[ordinal] != (ReplicatedShardRelationIdentity{}) {
			return fmt.Errorf("%w: trailing relation manifest residue", ErrReplicatedShardStoreProfile)
		}
	}
	if replicatedRelationManifestDigest(identity) != identity.RelationManifestDigest {
		return fmt.Errorf("%w: relation manifest digest", ErrReplicatedShardStoreProfile)
	}
	return nil
}

func replicatedRelationManifestDigest(identity ReplicatedShardStoreIdentity) [sha256.Size]byte {
	h := sha256.New()
	_, _ = h.Write(replicatedRelationManifestDomain)
	writeReplicatedRelationDescriptors(h, identity, true)
	var result [sha256.Size]byte
	_ = h.Sum(result[:0])
	return result
}

// writeReplicatedRelationDescriptors is the one frozen descriptor field order
// shared by the local exact manifest and the portable replicated apply
// manifest. includeStorage is true only for local catalog/reopen identity;
// physical image names are deliberately absent from replicated semantics.
func writeReplicatedRelationDescriptors(
	h interface{ Write([]byte) (int, error) },
	identity ReplicatedShardStoreIdentity,
	includeStorage bool,
) {
	var fixed [48]byte
	binary.LittleEndian.PutUint64(fixed[0:8], identity.RelationSchemaGeneration)
	binary.LittleEndian.PutUint64(fixed[8:16], uint64(identity.RelationCount))
	_, _ = h.Write(fixed[:16])
	for ordinal := 0; ordinal < int(identity.RelationCount); ordinal++ {
		relation := identity.Relations[ordinal]
		binary.LittleEndian.PutUint16(fixed[0:2], relation.Relation)
		fixed[2] = byte(relation.Kind)
		fixed[3] = relation.LocatorCount
		if relation.Unique {
			fixed[4] = 1
		} else {
			fixed[4] = 0
		}
		clear(fixed[5:8])
		binary.LittleEndian.PutUint64(fixed[8:16], relation.IndexID)
		binary.LittleEndian.PutUint64(fixed[16:24], relation.Incarnation)
		binary.LittleEndian.PutUint64(fixed[24:32], uint64(relation.Limits.MaxKeyBytes))
		binary.LittleEndian.PutUint64(fixed[32:40], uint64(relation.Limits.MaxDocumentBytes))
		binary.LittleEndian.PutUint32(fixed[40:44], uint32(relation.Limits.MaxBatchDocuments))
		binary.LittleEndian.PutUint32(fixed[44:48], uint32(relation.Limits.MaxBatchBytes))
		_, _ = h.Write(fixed[:])
		writeReplicatedRelationFrame(h, []byte(relation.Table))
		if includeStorage {
			writeReplicatedRelationFrame(h, []byte(relation.Storage))
		}
		_, _ = h.Write(relation.LocalIndexDigest[:])
	}
}

func replicatedLocalIndexDigest(indexes []indexMeta) [sha256.Size]byte {
	if len(indexes) == 0 {
		return [sha256.Size]byte{}
	}
	canonical := cloneIndexMeta(indexes)
	slices.SortFunc(canonical, func(left, right indexMeta) int {
		return strings.Compare(left.Name, right.Name)
	})
	h := sha256.New()
	_, _ = h.Write(replicatedLocalIndexManifestDomain)
	var count [8]byte
	binary.LittleEndian.PutUint64(count[:], uint64(len(canonical)))
	_, _ = h.Write(count[:])
	for i := range canonical {
		writeReplicatedRelationFrame(h, []byte(canonical[i].Name))
		binary.LittleEndian.PutUint64(count[:], uint64(len(canonical[i].Paths)))
		_, _ = h.Write(count[:])
		for _, path := range canonical[i].Paths {
			writeReplicatedRelationFrame(h, []byte(path))
		}
	}
	var result [sha256.Size]byte
	_ = h.Sum(result[:0])
	return result
}

func writeReplicatedRelationFrame(
	h interface{ Write([]byte) (int, error) }, value []byte,
) {
	var length [8]byte
	binary.LittleEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = h.Write(length[:])
	_, _ = h.Write(value)
}

func validateReplicatedShardStoreLimits(l ReplicatedShardStoreLimits) error {
	if l.MaxKeyBytes <= 0 || l.MaxKeyBytes > replicatedMaxKeyBytes ||
		l.MaxDocumentBytes <= 0 || l.MaxDocumentBytes > replicatedMaxDocumentBytes ||
		l.MaxBatchDocuments <= 0 || l.MaxBatchDocuments > replicatedMaxDistinctMutations ||
		l.MaxBatchBytes <= 0 || l.MaxBatchBytes > replicatedMaxBatchBytes {
		return fmt.Errorf("%w: user limits exceed replicated bounds", ErrReplicatedShardStoreProfile)
	}
	return nil
}

func ownedReplicatedShardStoreBinding(b ReplicatedShardStoreBinding) ReplicatedShardStoreBinding {
	b.Distribution = strings.Clone(b.Distribution)
	b.Shard = strings.Clone(b.Shard)
	return b
}

func ownedReplicatedShardStoreIdentity(i ReplicatedShardStoreIdentity) ReplicatedShardStoreIdentity {
	i.Binding = ownedReplicatedShardStoreBinding(i.Binding)
	i.UserTable = strings.Clone(i.UserTable)
	i.UserStorage = strings.Clone(i.UserStorage)
	i.UserPrimaryKey = strings.Clone(i.UserPrimaryKey)
	for ordinal := 0; ordinal < int(i.RelationCount); ordinal++ {
		i.Relations[ordinal].Table = strings.Clone(i.Relations[ordinal].Table)
		i.Relations[ordinal].Storage = strings.Clone(i.Relations[ordinal].Storage)
	}
	return i
}

func decodeReplicatedHex128(label, encoded string, dst *[16]byte) error {
	if len(encoded) != 32 || encoded != strings.ToLower(encoded) {
		return fmt.Errorf("vibedb: replicated %s must be 128-bit lowercase hexadecimal", label)
	}
	if _, err := hex.Decode(dst[:], []byte(encoded)); err != nil {
		return fmt.Errorf("vibedb: replicated %s is not hexadecimal: %w", label, err)
	}
	return nil
}

func decodeReplicatedHex256(label, encoded string, dst *[sha256.Size]byte) error {
	if len(encoded) != sha256.Size*2 || encoded != strings.ToLower(encoded) {
		return fmt.Errorf("vibedb: replicated %s must be 256-bit lowercase hexadecimal", label)
	}
	if _, err := hex.Decode(dst[:], []byte(encoded)); err != nil {
		return fmt.Errorf("vibedb: replicated %s is not hexadecimal: %w", label, err)
	}
	return nil
}

func (a ReplicatedAuthorityProfile) MarshalJSON() ([]byte, error) {
	type encoded struct {
		ActivePolicyGeneration uint64 `json:"active_policy_generation"`
		ProtectionEpoch        uint64 `json:"protection_epoch"`
		OwnershipEpoch         uint64 `json:"ownership_epoch"`
		SchemaGeneration       uint64 `json:"schema_generation"`
		RoutingVersion         uint64 `json:"routing_version"`
		RouteGeneration        uint64 `json:"route_generation"`
	}
	return json.Marshal(encoded(a))
}

func (a *ReplicatedAuthorityProfile) UnmarshalJSON(data []byte) error {
	var decoded ReplicatedAuthorityProfile
	present := make(map[string]bool, 6)
	err := decodeCatalogObject(data, "replicated authority profile", func(name string, d *json.Decoder) error {
		present[name] = true
		switch name {
		case "active_policy_generation":
			return d.Decode(&decoded.ActivePolicyGeneration)
		case "protection_epoch":
			return d.Decode(&decoded.ProtectionEpoch)
		case "ownership_epoch":
			return d.Decode(&decoded.OwnershipEpoch)
		case "schema_generation":
			return d.Decode(&decoded.SchemaGeneration)
		case "routing_version":
			return d.Decode(&decoded.RoutingVersion)
		case "route_generation":
			return d.Decode(&decoded.RouteGeneration)
		default:
			return unknownCatalogMember("replicated authority profile", name)
		}
	})
	if err != nil {
		return err
	}
	for _, name := range []string{"active_policy_generation", "protection_epoch", "ownership_epoch", "schema_generation", "routing_version", "route_generation"} {
		if !present[name] {
			return fmt.Errorf("vibedb: replicated authority profile is missing member %q", name)
		}
	}
	if decoded.ActivePolicyGeneration == 0 || decoded.ProtectionEpoch == 0 ||
		decoded.OwnershipEpoch == 0 || decoded.SchemaGeneration == 0 ||
		decoded.RoutingVersion == 0 || decoded.RouteGeneration == 0 {
		return errors.New("vibedb: replicated authority profile contains a zero generation")
	}
	*a = decoded
	return nil
}

func (b ReplicatedShardStoreBinding) MarshalJSON() ([]byte, error) {
	type encoded struct {
		ClusterID             string                     `json:"cluster_id"`
		ClusterIncarnation    string                     `json:"cluster_incarnation"`
		TopologyRecoveryEpoch uint64                     `json:"topology_recovery_epoch"`
		Distribution          string                     `json:"distribution"`
		Shard                 string                     `json:"shard"`
		AllocationGeneration  uint64                     `json:"allocation_generation"`
		ShardIncarnation      string                     `json:"shard_incarnation"`
		GroupID               string                     `json:"group_id"`
		MemberID              uint64                     `json:"member_id"`
		StoreID               string                     `json:"store_id"`
		Authority             ReplicatedAuthorityProfile `json:"authority"`
	}
	return json.Marshal(encoded{
		ClusterID: hex.EncodeToString(b.ClusterID[:]), ClusterIncarnation: hex.EncodeToString(b.ClusterIncarnation[:]),
		TopologyRecoveryEpoch: b.TopologyRecoveryEpoch, Distribution: b.Distribution, Shard: b.Shard,
		AllocationGeneration: b.AllocationGeneration, ShardIncarnation: hex.EncodeToString(b.ShardIncarnation[:]),
		GroupID: hex.EncodeToString(b.GroupID[:]), MemberID: b.MemberID, StoreID: hex.EncodeToString(b.StoreID[:]),
		Authority: b.Authority,
	})
}

func (b *ReplicatedShardStoreBinding) UnmarshalJSON(data []byte) error {
	var decoded ReplicatedShardStoreBinding
	present := make(map[string]bool, 11)
	err := decodeCatalogObject(data, "replicated shard store binding", func(name string, d *json.Decoder) error {
		present[name] = true
		decodeID := func(label string, dst *[16]byte) error {
			var value string
			if err := d.Decode(&value); err != nil {
				return err
			}
			return decodeReplicatedHex128(label, value, dst)
		}
		switch name {
		case "cluster_id":
			return decodeID(name, &decoded.ClusterID)
		case "cluster_incarnation":
			return decodeID(name, &decoded.ClusterIncarnation)
		case "topology_recovery_epoch":
			return d.Decode(&decoded.TopologyRecoveryEpoch)
		case "distribution":
			return d.Decode(&decoded.Distribution)
		case "shard":
			return d.Decode(&decoded.Shard)
		case "allocation_generation":
			return d.Decode(&decoded.AllocationGeneration)
		case "shard_incarnation":
			return decodeID(name, &decoded.ShardIncarnation)
		case "group_id":
			return decodeID(name, &decoded.GroupID)
		case "member_id":
			return d.Decode(&decoded.MemberID)
		case "store_id":
			return decodeID(name, &decoded.StoreID)
		case "authority":
			return d.Decode(&decoded.Authority)
		default:
			return unknownCatalogMember("replicated shard store binding", name)
		}
	})
	if err != nil {
		return err
	}
	for _, name := range []string{"cluster_id", "cluster_incarnation", "topology_recovery_epoch", "distribution", "shard", "allocation_generation", "shard_incarnation", "group_id", "member_id", "store_id", "authority"} {
		if !present[name] {
			return fmt.Errorf("vibedb: replicated shard store binding is missing member %q", name)
		}
	}
	if err := validateReplicatedShardStoreBinding(decoded); err != nil {
		return err
	}
	*b = ownedReplicatedShardStoreBinding(decoded)
	return nil
}

func (l ReplicatedShardStoreLimits) MarshalJSON() ([]byte, error) {
	type encoded struct {
		MaxKeyBytes       int `json:"max_key_bytes"`
		MaxDocumentBytes  int `json:"max_document_bytes"`
		MaxBatchDocuments int `json:"max_batch_documents"`
		MaxBatchBytes     int `json:"max_batch_bytes"`
	}
	return json.Marshal(encoded(l))
}

func (l *ReplicatedShardStoreLimits) UnmarshalJSON(data []byte) error {
	var decoded ReplicatedShardStoreLimits
	present := make(map[string]bool, 4)
	err := decodeCatalogObject(data, "replicated shard store limits", func(name string, d *json.Decoder) error {
		present[name] = true
		switch name {
		case "max_key_bytes":
			return d.Decode(&decoded.MaxKeyBytes)
		case "max_document_bytes":
			return d.Decode(&decoded.MaxDocumentBytes)
		case "max_batch_documents":
			return d.Decode(&decoded.MaxBatchDocuments)
		case "max_batch_bytes":
			return d.Decode(&decoded.MaxBatchBytes)
		default:
			return unknownCatalogMember("replicated shard store limits", name)
		}
	})
	if err != nil {
		return err
	}
	for _, name := range []string{"max_key_bytes", "max_document_bytes", "max_batch_documents", "max_batch_bytes"} {
		if !present[name] {
			return fmt.Errorf("vibedb: replicated shard store limits are missing member %q", name)
		}
	}
	if err := validateReplicatedShardStoreLimits(decoded); err != nil {
		return err
	}
	*l = decoded
	return nil
}

func (r ReplicatedShardRelationIdentity) MarshalJSON() ([]byte, error) {
	type encoded struct {
		Relation         uint16                      `json:"relation"`
		Kind             ReplicatedShardRelationKind `json:"kind"`
		Table            string                      `json:"table"`
		Storage          string                      `json:"storage"`
		Limits           ReplicatedShardStoreLimits  `json:"limits"`
		LocalIndexDigest string                      `json:"local_index_digest"`
		IndexID          uint64                      `json:"index_id"`
		Incarnation      uint64                      `json:"incarnation"`
		LocatorCount     uint8                       `json:"locator_count"`
		Unique           bool                        `json:"unique"`
	}
	return json.Marshal(encoded{
		Relation: r.Relation, Kind: r.Kind, Table: r.Table, Storage: r.Storage,
		Limits: r.Limits, LocalIndexDigest: hex.EncodeToString(r.LocalIndexDigest[:]),
		IndexID: r.IndexID, Incarnation: r.Incarnation,
		LocatorCount: r.LocatorCount, Unique: r.Unique,
	})
}

func (r *ReplicatedShardRelationIdentity) UnmarshalJSON(data []byte) error {
	var decoded ReplicatedShardRelationIdentity
	present := make(map[string]bool, 10)
	err := decodeCatalogObject(data, "replicated shard relation", func(name string, d *json.Decoder) error {
		present[name] = true
		switch name {
		case "relation":
			return d.Decode(&decoded.Relation)
		case "kind":
			return d.Decode(&decoded.Kind)
		case "table":
			return d.Decode(&decoded.Table)
		case "storage":
			return d.Decode(&decoded.Storage)
		case "limits":
			return d.Decode(&decoded.Limits)
		case "local_index_digest":
			var value string
			if err := d.Decode(&value); err != nil {
				return err
			}
			return decodeReplicatedHex256(name, value, &decoded.LocalIndexDigest)
		case "index_id":
			return d.Decode(&decoded.IndexID)
		case "incarnation":
			return d.Decode(&decoded.Incarnation)
		case "locator_count":
			return d.Decode(&decoded.LocatorCount)
		case "unique":
			return d.Decode(&decoded.Unique)
		default:
			return unknownCatalogMember("replicated shard relation", name)
		}
	})
	if err != nil {
		return err
	}
	for _, name := range []string{
		"relation", "kind", "table", "storage", "limits", "local_index_digest",
		"index_id", "incarnation", "locator_count", "unique",
	} {
		if !present[name] {
			return fmt.Errorf("vibedb: replicated shard relation is missing member %q", name)
		}
	}
	*r = decoded
	return nil
}

type replicatedShardRelationList struct {
	count uint16
	items [replication.MaxRelationsPerBundle]ReplicatedShardRelationIdentity
}

func (list *replicatedShardRelationList) UnmarshalJSON(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	opening, err := decoder.Token()
	if err != nil || opening != json.Delim('[') {
		return fmt.Errorf("vibedb: replicated relations must be an array")
	}
	var decoded replicatedShardRelationList
	for decoder.More() {
		if int(decoded.count) >= len(decoded.items) {
			return fmt.Errorf(
				"vibedb: replicated relation manifest exceeds %d slots", len(decoded.items),
			)
		}
		if err := decoder.Decode(&decoded.items[decoded.count]); err != nil {
			return err
		}
		decoded.count++
	}
	closing, err := decoder.Token()
	if err != nil || closing != json.Delim(']') {
		return fmt.Errorf("vibedb: replicated relations have invalid framing")
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return fmt.Errorf("vibedb: replicated relations have trailing bytes")
	}
	*list = decoded
	return nil
}

func (i ReplicatedShardStoreIdentity) MarshalJSON() ([]byte, error) {
	type encoded struct {
		Format                   uint16                             `json:"format"`
		Binding                  ReplicatedShardStoreBinding        `json:"binding"`
		LogID                    string                             `json:"log_id"`
		UserTable                string                             `json:"user_table"`
		UserStorage              string                             `json:"user_storage"`
		UserPrimaryKey           string                             `json:"user_primary_key"`
		UserLimits               ReplicatedShardStoreLimits         `json:"user_limits"`
		Sidecars                 ReplicatedShardStoreSidecarProfile `json:"sidecars"`
		RelationSchemaGeneration uint64                             `json:"relation_schema_generation,omitempty"`
		RelationManifestDigest   string                             `json:"relation_manifest_digest,omitempty"`
		Relations                []ReplicatedShardRelationIdentity  `json:"relations,omitempty"`
	}
	value := encoded{
		Format: i.Format, Binding: i.Binding, LogID: hex.EncodeToString(i.LogID[:]),
		UserTable: i.UserTable, UserStorage: i.UserStorage,
		UserPrimaryKey: i.UserPrimaryKey, UserLimits: i.UserLimits,
		Sidecars: i.Sidecars,
	}
	if i.RelationCount != 0 {
		value.RelationSchemaGeneration = i.RelationSchemaGeneration
		value.RelationManifestDigest = hex.EncodeToString(i.RelationManifestDigest[:])
		value.Relations = make([]ReplicatedShardRelationIdentity, int(i.RelationCount))
		copy(value.Relations, i.Relations[:i.RelationCount])
	}
	return json.Marshal(value)
}

func (i *ReplicatedShardStoreIdentity) UnmarshalJSON(data []byte) error {
	var decoded ReplicatedShardStoreIdentity
	present := make(map[string]bool, 11)
	err := decodeCatalogObject(data, "replicated shard store identity", func(name string, d *json.Decoder) error {
		present[name] = true
		switch name {
		case "format":
			return decodeRequiredCatalogUint16(
				d, "replicated shard store format", &decoded.Format,
			)
		case "binding":
			return d.Decode(&decoded.Binding)
		case "log_id":
			var value string
			if err := d.Decode(&value); err != nil {
				return err
			}
			return decodeReplicatedHex128(name, value, &decoded.LogID)
		case "user_table":
			return d.Decode(&decoded.UserTable)
		case "user_storage":
			return d.Decode(&decoded.UserStorage)
		case "user_primary_key":
			return d.Decode(&decoded.UserPrimaryKey)
		case "user_limits":
			return d.Decode(&decoded.UserLimits)
		case "sidecars":
			return d.Decode(&decoded.Sidecars)
		case "relation_schema_generation":
			return d.Decode(&decoded.RelationSchemaGeneration)
		case "relation_manifest_digest":
			var value string
			if err := d.Decode(&value); err != nil {
				return err
			}
			return decodeReplicatedHex256(name, value, &decoded.RelationManifestDigest)
		case "relations":
			var relations replicatedShardRelationList
			if err := d.Decode(&relations); err != nil {
				return err
			}
			decoded.RelationCount = relations.count
			decoded.Relations = relations.items
			return nil
		default:
			return unknownCatalogMember("replicated shard store identity", name)
		}
	})
	if err != nil {
		return err
	}
	for _, name := range []string{"format", "binding", "log_id", "user_table", "user_storage", "user_primary_key", "user_limits", "sidecars"} {
		if !present[name] {
			return fmt.Errorf("vibedb: replicated shard store identity is missing member %q", name)
		}
	}
	extension := present["relation_schema_generation"] ||
		present["relation_manifest_digest"] || present["relations"]
	if extension && (!present["relation_schema_generation"] ||
		!present["relation_manifest_digest"] || !present["relations"]) {
		return errors.New("vibedb: replicated shard relation manifest is partial")
	}
	if err := validateReplicatedShardStoreIdentity(decoded); err != nil {
		return err
	}
	*i = ownedReplicatedShardStoreIdentity(decoded)
	return nil
}
