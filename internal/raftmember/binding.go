// Package raftmember binds one open Raft WAL to one prepared replicated SQL
// shard root and can adopt that exact pair into one non-serving synchronous
// Runtime. Runtime owns incarnation minting, Node construction, proposal
// admission order, and Ready lifecycle; it deliberately owns no network
// transport or serving authority.
package raftmember

import (
	"errors"
	"fmt"

	"github.com/thesyncim/vibedb/internal/raftstore"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

var (
	// ErrWALUnavailable reports a nil, closed, poisoned, or unsettled WAL.
	ErrWALUnavailable = errors.New("raftmember: WAL is unavailable for SQL binding")
	// ErrWALQuarantined reports a WAL recovered through the torn-current-slot
	// fallback. Such a member requires operator-visible quarantine and must not
	// bind or reopen a SQL root for runtime use.
	ErrWALQuarantined = errors.New("raftmember: WAL recovered a torn current slot")
	// ErrBindingMismatch reports that a retained full SQL identity names a
	// different WAL tuple or authority profile from the supplied live WAL.
	ErrBindingMismatch = errors.New("raftmember: retained SQL identity does not match live WAL")
	// ErrExpectedSQLLogID reports an open attempted without the local SQL log
	// incarnation returned by BindPreparedSQL. StoreID is never substituted for
	// this independently minted identity.
	ErrExpectedSQLLogID = errors.New("raftmember: retained SQL identity has no LogID")
	// ErrInvalidDatabase reports a nil SQL database passed to BindPreparedSQL.
	ErrInvalidDatabase = errors.New("raftmember: nil SQL database")
)

// BindingFromWAL derives every Raft-owned identity coordinate from an actual
// healthy open Store and combines it with the caller's trusted Phase-1b
// authority profile. It never invents an SQL LogID and performs no mutation.
//
// The caller must exclusively own wal for the startup sequence. In particular,
// it must not close or mutate the Store concurrently with this call or the
// subsequent bind, settlement, or exact-open operation.
func BindingFromWAL(
	wal *raftstore.Store,
	authority sqldriver.ReplicatedAuthorityProfile,
) (sqldriver.ReplicatedShardStoreBinding, error) {
	if wal == nil {
		return sqldriver.ReplicatedShardStoreBinding{}, ErrWALUnavailable
	}
	// Every raft.Storage read crosses Store.checkLocked. This rejects a closed,
	// poisoned, or pending-unknown handle before any SQL API can be reached.
	if _, _, err := wal.InitialState(); err != nil {
		return sqldriver.ReplicatedShardStoreBinding{}, fmt.Errorf(
			"%w: inspect stable state: %w", ErrWALUnavailable, err,
		)
	}
	if wal.RecoveredTornCurrentSlot() {
		return sqldriver.ReplicatedShardStoreBinding{}, ErrWALQuarantined
	}

	identity := wal.Identity()
	return sqldriver.ReplicatedShardStoreBinding{
		ClusterID:             identity.ClusterID,
		ClusterIncarnation:    identity.ClusterIncarnation,
		TopologyRecoveryEpoch: wal.TopologyRecoveryEpoch(),
		Distribution:          identity.Distribution,
		Shard:                 identity.Shard,
		AllocationGeneration:  identity.AllocationGeneration,
		ShardIncarnation:      identity.ShardIncarnation,
		GroupID:               identity.GroupID,
		MemberID:              identity.MemberID,
		StoreID:               identity.StoreID,
		Authority:             authority,
	}, nil
}

// BindPreparedSQL permanently puts an already-prepared local shard root into
// replicated mode using only identity derived from wal. The returned full
// identity includes the SQL root's existing independent LogID and exact user
// layout. Before calling, the orchestrator must inspect database through
// ShardStoreIdentity and durably retain its LogID together with the exact
// BindingFromWAL result and userTable. That pre-bind record is required by
// OpenBoundSQLForSettlement if the process dies after catalog publication but
// before this function returns. On success, durably replace or augment it with
// the returned full identity and supply that identity to every OpenBoundSQL.
//
// This function does not call BeginIncarnation, construct a Node, or grant
// serving authority.
func BindPreparedSQL(
	wal *raftstore.Store,
	database *sqldriver.Database,
	authority sqldriver.ReplicatedAuthorityProfile,
	userTable string,
) (sqldriver.ReplicatedShardStoreIdentity, error) {
	binding, err := BindingFromWAL(wal, authority)
	if err != nil {
		return sqldriver.ReplicatedShardStoreIdentity{}, err
	}
	if database == nil {
		return sqldriver.ReplicatedShardStoreIdentity{}, ErrInvalidDatabase
	}
	return database.BindReplicatedShardStore(binding, userTable)
}

// OpenBoundSQLForSettlement recovers only the crash window in which the SQL
// catalog durably published BindPreparedSQL but its complete returned identity
// did not reach the caller. expectedLogID must be the independent SQL LogID
// retained before bind; a live WAL binding alone is insufficient. On success,
// the caller must durably retain the returned complete identity and use
// OpenBoundSQL on all ordinary restarts.
//
// The retained LogID distinguishes independently prepared SQL roots. It cannot
// distinguish a byte-identical copy of the same SQL root used with the original
// live WAL.
//
// WAL health, torn-slot quarantine, and the retained LogID are checked before
// the SQL driver opens or recovers the root.
func OpenBoundSQLForSettlement(
	path string,
	wal *raftstore.Store,
	authority sqldriver.ReplicatedAuthorityProfile,
	expectedLogID [16]byte,
	userTable string,
) (*sqldriver.Database, sqldriver.ReplicatedShardStoreIdentity, error) {
	binding, err := BindingFromWAL(wal, authority)
	if err != nil {
		return nil, sqldriver.ReplicatedShardStoreIdentity{}, err
	}
	if expectedLogID == ([16]byte{}) {
		return nil, sqldriver.ReplicatedShardStoreIdentity{}, ErrExpectedSQLLogID
	}
	return sqldriver.OpenReplicatedShardStoreForSettlement(
		path, binding, expectedLogID, userTable,
	)
}

// OpenBoundSQL opens a previously bound SQL root only when expected is the
// complete identity returned by BindPreparedSQL and its embedded WAL/profile
// binding exactly matches wal plus authority. Requiring the retained SQL LogID
// prevents silently accepting a separately prepared root bound to the same
// Raft member tuple. It does not distinguish a byte-identical copy of the same
// SQL root used with the original live WAL.
//
// WAL health, torn-slot quarantine, the retained LogID, and the embedded
// binding are checked before the SQL driver opens or recovers the root.
func OpenBoundSQL(
	path string,
	wal *raftstore.Store,
	authority sqldriver.ReplicatedAuthorityProfile,
	expected sqldriver.ReplicatedShardStoreIdentity,
) (*sqldriver.Database, error) {
	binding, err := BindingFromWAL(wal, authority)
	if err != nil {
		return nil, err
	}
	if expected.LogID == ([16]byte{}) {
		return nil, ErrExpectedSQLLogID
	}
	if expected.Binding != binding {
		return nil, ErrBindingMismatch
	}
	return sqldriver.OpenReplicatedShardStore(path, expected)
}
