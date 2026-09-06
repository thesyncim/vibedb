package driver

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unsafe"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/query"
	"github.com/thesyncim/vibejson"
)

// NewPointReadSession builds a read-only SQL session over one exact point
// result returned by the replicated apply machine. The value is detached into
// the session before this method returns; a miss is represented by found=false
// and an empty value. No durable snapshot is acquired or retained.
//
// The primary path is part of the admission proof even though the prepared
// statement receives it again through QueryCandidateKeysInto. Checking both
// copies keeps a stale gateway descriptor from widening the physical source.
// The caller must keep the apply claim alive until the returned session closes,
// just as it does for NewDataReadSession.
func (a *ReplicatedApply) NewPointReadSession(
	ctx context.Context,
	relation replication.RelationID,
	key []byte,
	found bool,
	raw []byte,
	primaryPath []byte,
	options query.ExecOptions,
) (*ReplicatedReadSession, error) {
	return a.newPointReadSessionInto(
		ctx, relation, key, found, raw, primaryPath, options, nil,
	)
}

// newPointReadSessionInto repeats the live point proof and installs a fresh
// detached value in a retained session object. The caller owns the key/value
// only for this call; the reuse lane clears all copies when its lease ends.
func (a *ReplicatedApply) newPointReadSessionInto(
	ctx context.Context,
	relation replication.RelationID,
	key []byte,
	found bool,
	raw []byte,
	primaryPath []byte,
	options query.ExecOptions,
	reuse *ReplicatedReadSession,
) (*ReplicatedReadSession, error) {
	if a == nil || a.database == nil || ctx == nil {
		return nil, ErrReplicatedApplyClosed
	}
	if err := contextCheckpoint(ctx); err != nil {
		return nil, err
	}
	reader := reuse
	if reader == nil {
		reader = &ReplicatedReadSession{}
	}

	a.database.mu.RLock()
	defer a.database.mu.RUnlock()

	if err := a.checkLocked(); err != nil {
		return nil, err
	}
	if err := a.checkActivationBaseLocked(); err != nil {
		return nil, err
	}
	capacity, err := a.machine.SessionCapacityState()
	if err != nil {
		return nil, err
	}
	if !capacity.Initialized {
		return nil, fmt.Errorf(
			"%w: replicated apply has no initialized publication",
			ErrReplicatedApplyMismatch,
		)
	}
	layout, _, _, err := a.pointReadSessionLayoutLocked(
		relation, primaryPath, key, found, raw, &reader.pointProof,
	)
	if err != nil {
		return nil, err
	}
	if err := contextCheckpoint(ctx); err != nil {
		return nil, err
	}

	if options.Workers == 0 {
		options.Workers = driverQueryWorkers
	}
	base := a.database.catalog.ReplicatedShardStore
	state, err := newTransactionTableState(base.UserTable, layout, false)
	if err != nil {
		return nil, err
	}
	state.pointBacked = true
	state.pointKey = string(key)
	state.pointFound = found
	if found {
		// The validator above has already bounded and checked raw. Copy only
		// after those checks so an oversized or invalid caller value cannot cause
		// an unbounded retention allocation.
		state.pointDocument = append([]byte(nil), raw...)
	}
	state.filterSource = query.NewFileFilterSource(state)

	var prepared map[*Prepared]struct{}
	if reuse != nil {
		if reader.session.current != nil || reader.conn.open {
			return nil, ErrCursorOpen
		}
		if reader.conn.tx != nil {
			return nil, errors.New("vibedb: reusable replicated read has an active transaction")
		}
		prepared = reader.session.prepared
	}
	// Reuse readers have already passed resetForReadReuse. Bind the fresh
	// identity/options in place so the large, scrubbed Exec need not be copied.
	reader.conn.db = a.database
	reader.conn.directWritesFenced = true
	reader.conn.exec.Options = options
	transaction := &tx{
		conn:        &reader.conn,
		readOnly:    true,
		isolation:   IsolationRepeatableRead,
		layoutEpoch: a.database.layoutEpoch,
		tables:      map[string]*txTable{state.name: state},
	}
	transaction.views = transaction.layoutEpoch.views
	transaction.conn.tx = transaction
	reader.session = Session{
		conn: &reader.conn, state: SessionInTransaction, prepared: prepared,
	}
	return reader, nil
}

// NewPointReadSessionAtPath is the same bounded point session constructor with
// the path beside the relation. It is a small spelling convenience for callers
// whose request fields are ordered relation, path, key, found, value.
func (a *ReplicatedApply) NewPointReadSessionAtPath(
	ctx context.Context,
	relation replication.RelationID,
	primaryPath []byte,
	key []byte,
	found bool,
	raw []byte,
	options query.ExecOptions,
) (*ReplicatedReadSession, error) {
	return a.NewPointReadSession(
		ctx, relation, key, found, raw, primaryPath, options,
	)
}

// pointReadSessionLayoutLocked validates the complete live identity needed by
// a one-table point session. The caller holds database.mu.RLock for the full
// check and must not use the returned layout after changing the catalog.
func (a *ReplicatedApply) pointReadSessionLayoutLocked(
	relation replication.RelationID,
	primaryPath, key []byte,
	found bool,
	raw []byte,
	proof *replicatedPointSessionProof,
) (transactionTableLayout, *table, ReplicatedShardRelationIdentity, error) {
	base := a.database.catalog.ReplicatedShardStore
	if base == nil {
		return transactionTableLayout{}, nil, ReplicatedShardRelationIdentity{},
			ErrReplicatedApplyMismatch
	}
	warmProof := proof.valid && proof.base.Equal(*base) && proof.apply == a.identity
	if !warmProof {
		if err := validateReplicatedShardStoreIdentity(*base); err != nil {
			return transactionTableLayout{}, nil, ReplicatedShardRelationIdentity{},
				fmt.Errorf("%w: invalid replicated shard store identity: %v", ErrReplicatedApplyMismatch, err)
		}
		if err := validateReplicatedApplyIdentity(a.identity, *base); err != nil {
			return transactionTableLayout{}, nil, ReplicatedShardRelationIdentity{},
				fmt.Errorf("%w: invalid replicated apply identity: %v", ErrReplicatedApplyMismatch, err)
		}
	}
	if relation == 0 || int(relation) > len(base.Relations) ||
		int(base.RelationCount) != len(base.Relations) {
		return transactionTableLayout{}, nil, ReplicatedShardRelationIdentity{},
			fmt.Errorf("%w: relation %d is outside the live manifest", ErrReplicatedApplyMismatch, relation)
	}
	descriptor := base.Relations[int(relation)-1]
	// SQL point sessions are restricted to the one JSON base relation. Global
	// index rows are a separate replicated relation and have no SQL document
	// schema or primary-path source.
	if relation != 1 || descriptor.Relation != uint16(relation) ||
		descriptor.Kind != ReplicatedShardRelationJSON ||
		descriptor.Table != base.UserTable {
		return transactionTableLayout{}, nil, ReplicatedShardRelationIdentity{},
			fmt.Errorf("%w: relation %d is not the live JSON base relation", ErrReplicatedApplyMismatch, relation)
	}

	table := a.database.tables[base.UserTable]
	if table == nil || table != a.table || table.meta == nil ||
		table.meta.PrimaryKey != base.UserPrimaryKey ||
		!openedReplicatedRelationMatches(table, descriptor, base.Sidecars) {
		return transactionTableLayout{}, nil, ReplicatedShardRelationIdentity{},
			fmt.Errorf("%w: live replicated relation metadata differs", ErrReplicatedApplyMismatch)
	}
	if a.database.layoutEpoch == nil {
		return transactionTableLayout{}, nil, ReplicatedShardRelationIdentity{},
			fmt.Errorf("%w: live catalog layout is unavailable", ErrReplicatedApplyMismatch)
	}
	layout, ok := a.database.layoutEpoch.tables[base.UserTable]
	if !ok || layout.incarnation != table || layout.primaryKey != base.UserPrimaryKey ||
		layout.schema != table.schema {
		return transactionTableLayout{}, nil, ReplicatedShardRelationIdentity{},
			fmt.Errorf("%w: live catalog layout differs from the apply relation", ErrReplicatedApplyMismatch)
	}
	manifest, err := a.machine.RelationManifestDigest()
	if err != nil {
		return transactionTableLayout{}, nil, ReplicatedShardRelationIdentity{}, err
	}
	expectedManifest := proof.manifest
	if !warmProof {
		expectedManifest, err = replicatedSchemaManifestValidated(
			*base, a.identity, replicatedApplyLocalIndexes(table),
		)
	}
	if err != nil || manifest != expectedManifest {
		return transactionTableLayout{}, nil, ReplicatedShardRelationIdentity{},
			fmt.Errorf("%w: live relation manifest differs from the apply claim", ErrReplicatedApplyMismatch)
	}
	if !warmProof {
		proof.base = base.Clone()
		proof.apply = a.identity
		proof.apply.Storage = strings.Clone(a.identity.Storage)
		proof.apply.CaptureStorage = strings.Clone(a.identity.CaptureStorage)
		proof.apply.Placement = ownedReplicatedPlacementProfile(a.identity.Placement)
		proof.manifest, proof.valid = expectedManifest, true
	}
	if len(primaryPath) == 0 || string(primaryPath) != base.UserPrimaryKey {
		return transactionTableLayout{}, nil, ReplicatedShardRelationIdentity{},
			fmt.Errorf("%w: primary path does not match the live relation", ErrReplicatedApplyMismatch)
	}
	if len(key) == 0 || len(key) > descriptor.Limits.MaxKeyBytes {
		return transactionTableLayout{}, nil, ReplicatedShardRelationIdentity{},
			fmt.Errorf("%w: point key exceeds the live relation bound", ErrReplicatedApplyMismatch)
	}
	if !found && len(raw) != 0 {
		return transactionTableLayout{}, nil, ReplicatedShardRelationIdentity{},
			fmt.Errorf("%w: a point miss cannot carry a value", ErrReplicatedApplyMismatch)
	}
	if found && (len(raw) == 0 || len(raw) > descriptor.Limits.MaxDocumentBytes) {
		return transactionTableLayout{}, nil, ReplicatedShardRelationIdentity{},
			fmt.Errorf("%w: point value exceeds the live relation bound", ErrReplicatedApplyMismatch)
	}

	// Re-run the same exact key, schema, and owned-range proof used by apply.
	// This protects the constructor if a caller supplies bytes that did not come
	// directly from PointReadInto, and keeps malformed or stale keys fail-closed.
	validator := &proof.validator
	if validator.placement.mapper == nil {
		validator.placement.mapper = distribution.NewNativeMapper(1)
	}
	validator.primaryKey, validator.primary = base.UserPrimaryKey, table.primary
	validator.maxKeyBytes = base.UserLimits.MaxKeyBytes
	validator.schema, validator.maxDocumentBytes = table.schema, base.UserLimits.MaxDocumentBytes
	validator.placement.target = a.identity.Placement.Range
	defer func() {
		// The slot owns scratch, but the schema and compiled primary pointer
		// belong to this live catalog check and must not survive it.
		validator.primaryKey, validator.primary, validator.schema = "", vibejson.CompiledPointer{}, nil
		clear(validator.keyScratch[:cap(validator.keyScratch)])
		clear(validator.decodeScratch[:cap(validator.decodeScratch)])
		clear(validator.schemaTape[:cap(validator.schemaTape)])
		if proof.validatorBytes() > replicatedReadReuseResultBytes {
			validator.keyScratch, validator.decodeScratch, validator.schemaTape = nil, nil, nil
		}
	}()
	var validation replicatedstate.MutationValidation
	if found {
		validation = validator.ValidatePutOwnership(
			key, raw, a.identity.Placement.Range,
		)
	} else {
		validation = validator.ValidatePointOwnership(
			key, a.identity.Placement.Range,
		)
	}
	if validation != replicatedstate.MutationValidationAccept {
		return transactionTableLayout{}, nil, ReplicatedShardRelationIdentity{},
			fmt.Errorf("%w: point key or value failed the live relation proof", ErrReplicatedApplyMismatch)
	}
	return layout, table, descriptor, nil
}

// Memoize only independently owned identity values and their validated digest.
// Every read still compares the complete live base/apply identities, opened
// relation metadata, layout, and machine manifest. No catalog pointer or row is
// retained, and even in-place descriptor mutations force full revalidation.
type replicatedPointSessionProof struct {
	base      ReplicatedShardStoreIdentity
	apply     ReplicatedApplyIdentity
	manifest  [32]byte
	valid     bool
	validator replicatedSQLMutationValidator
}

func (p *replicatedPointSessionProof) validatorBytes() int64 {
	v := &p.validator
	return int64(cap(v.keyScratch)) + int64(cap(v.decodeScratch)) +
		int64(cap(v.schemaTape))*int64(unsafe.Sizeof(vibejson.IndexEntry{})) + int64(unsafe.Sizeof(distribution.NativeMapper{}))
}

func (p *replicatedPointSessionProof) retainedBytes() int64 {
	if p == nil || !p.valid {
		return 0
	}
	n := int64(cap(p.base.Relations)) * int64(unsafe.Sizeof(ReplicatedShardRelationIdentity{}))
	for _, s := range []string{p.base.Binding.Distribution, p.base.Binding.Shard,
		p.base.UserTable, p.base.UserStorage, p.base.UserPrimaryKey,
		p.apply.Storage, p.apply.CaptureStorage, p.apply.Placement.ShardKey} {
		n += int64(len(s))
	}
	for _, r := range p.base.Relations {
		n += int64(len(r.Table)) + int64(len(r.Storage))
	}
	return n + p.validatorBytes()
}

var _ interface {
	NewPointReadSession(
		context.Context,
		replication.RelationID,
		[]byte,
		bool,
		[]byte,
		[]byte,
		query.ExecOptions,
	) (*ReplicatedReadSession, error)
} = (*ReplicatedApply)(nil)
