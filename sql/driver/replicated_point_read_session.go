package driver

import (
	"context"
	"errors"
	"fmt"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/query"
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
		relation, primaryPath, key, found, raw,
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

	reader := reuse
	var prepared map[*Prepared]struct{}
	if reader == nil {
		reader = &ReplicatedReadSession{}
	} else {
		if reader.session.current != nil || reader.conn.open {
			return nil, ErrCursorOpen
		}
		if reader.conn.tx != nil {
			return nil, errors.New("vibedb: reusable replicated read has an active transaction")
		}
		prepared = reader.session.prepared
	}
	reader.conn = conn{
		db: a.database, directWritesFenced: true,
		exec: query.Exec{Options: options},
	}
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
) (transactionTableLayout, *table, ReplicatedShardRelationIdentity, error) {
	base := a.database.catalog.ReplicatedShardStore
	if base == nil {
		return transactionTableLayout{}, nil, ReplicatedShardRelationIdentity{},
			ErrReplicatedApplyMismatch
	}
	if err := validateReplicatedShardStoreIdentity(*base); err != nil {
		return transactionTableLayout{}, nil, ReplicatedShardRelationIdentity{},
			fmt.Errorf("%w: invalid replicated shard store identity: %v", ErrReplicatedApplyMismatch, err)
	}
	if err := validateReplicatedApplyIdentity(a.identity, *base); err != nil {
		return transactionTableLayout{}, nil, ReplicatedShardRelationIdentity{},
			fmt.Errorf("%w: invalid replicated apply identity: %v", ErrReplicatedApplyMismatch, err)
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
	expectedManifest, err := replicatedSchemaManifestValidated(
		*base, a.identity, replicatedApplyLocalIndexes(table),
	)
	if err != nil || manifest != expectedManifest {
		return transactionTableLayout{}, nil, ReplicatedShardRelationIdentity{},
			fmt.Errorf("%w: live relation manifest differs from the apply claim", ErrReplicatedApplyMismatch)
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
	validator := newReplicatedSQLMutationValidator(
		*base, table, a.identity.Placement,
	)
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
