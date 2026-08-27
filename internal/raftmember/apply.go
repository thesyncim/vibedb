package raftmember

import (
	"errors"
	"fmt"

	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	pb "go.etcd.io/raft/v3/raftpb"
)

// OpenPreparedApply activates or acquires trusted apply on an already-open,
// exactly bound SQL root. WAL health and the static bootstrap are read before
// the SQL catalog can be mutated. It does not mint an incarnation, construct a
// Node, or grant serving authority.
func OpenPreparedApply(
	wal *raftstore.Store,
	database *sqldriver.Database,
	authority sqldriver.ReplicatedAuthorityProfile,
	expectedSQL sqldriver.ReplicatedShardStoreIdentity,
	options sqldriver.ReplicatedApplyOptions,
) (*sqldriver.ReplicatedApply, sqldriver.ReplicatedApplyIdentity, error) {
	binding, bootstrap, err := applyPrerequisites(wal, authority)
	if err != nil {
		return nil, sqldriver.ReplicatedApplyIdentity{}, err
	}
	if database == nil {
		return nil, sqldriver.ReplicatedApplyIdentity{}, ErrInvalidDatabase
	}
	if expectedSQL.Binding != binding {
		return nil, sqldriver.ReplicatedApplyIdentity{}, ErrBindingMismatch
	}
	if _, err := database.RequireReplicatedShardStore(expectedSQL); err != nil {
		return nil, sqldriver.ReplicatedApplyIdentity{}, err
	}
	return database.OpenReplicatedApply(expectedSQL, bootstrap, options)
}

// OpenBoundSQLWithApply performs exact base+apply identity comparison before
// SQL namespace and transaction recovery, then acquires the opaque claim using
// the WAL's exact static bootstrap.
func OpenBoundSQLWithApply(
	path string,
	wal *raftstore.Store,
	authority sqldriver.ReplicatedAuthorityProfile,
	expectedSQL sqldriver.ReplicatedShardStoreIdentity,
	expectedApply sqldriver.ReplicatedApplyIdentity,
) (*sqldriver.Database, *sqldriver.ReplicatedApply, error) {
	return openBoundSQLWithApply(path, wal, authority, expectedSQL, expectedApply, sqldriver.OpenReplicatedShardStoreWithApply)
}

func openBoundSQLWithApply(
	path string,
	wal *raftstore.Store,
	authority sqldriver.ReplicatedAuthorityProfile,
	expectedSQL sqldriver.ReplicatedShardStoreIdentity,
	expectedApply sqldriver.ReplicatedApplyIdentity,
	open func(string, sqldriver.ReplicatedShardStoreIdentity, sqldriver.ReplicatedApplyIdentity) (*sqldriver.Database, error),
) (*sqldriver.Database, *sqldriver.ReplicatedApply, error) {
	binding, bootstrap, err := applyPrerequisites(wal, authority)
	if err != nil {
		return nil, nil, err
	}
	if expectedSQL.Binding != binding {
		return nil, nil, ErrBindingMismatch
	}
	database, err := open(
		path, expectedSQL, expectedApply,
	)
	if err != nil {
		return nil, nil, err
	}
	claim, actual, err := database.OpenReplicatedApply(
		expectedSQL, bootstrap, sqldriver.ReplicatedApplyOptions{
			MaxSessions: expectedApply.MaxSessions, RetryWindow: expectedApply.RetryWindow,
			TxnLimits: expectedApply.TxnLimits, Placement: expectedApply.Placement,
			RequestLedgerCapacityBytes:       expectedApply.RequestLedgerCapacityBytes,
			RequestLedgerCleanupReserveBytes: expectedApply.RequestLedgerCleanupReserveBytes,
			RequestLedgerRangeStart:          expectedApply.RequestLedgerRangeStart,
			RequestLedgerRangeEnd:            expectedApply.RequestLedgerRangeEnd,
			RequestLedgerRangeIdentity:       expectedApply.RequestLedgerRangeIdentity,
		},
	)
	if err != nil || actual != expectedApply {
		if claim != nil {
			_ = claim.Close()
		}
		closeErr := database.Close()
		// A source-not-committed proof permits an ordinary reopen only after
		// this recovery handle closed cleanly. Never hide cleanup uncertainty
		// behind the otherwise retryable exact-cut sentinel.
		if closeErr != nil && errors.Is(err, sqldriver.ErrSchemaSourceNotCommitted) {
			err = fmt.Errorf("%w: schema source cleanup: %v", ErrRuntimeOwnership, err)
		}
		if err == nil {
			err = sqldriver.ErrReplicatedApplyMismatch
		}
		return nil, nil, errors.Join(err, closeErr)
	}
	return database, claim, nil
}

// OpenBoundSQLWithApplyForSettlement resolves activation publication whose
// random hidden storage identity was not returned to its caller. The retained
// base identity and intended options are checked before SQL recovery; the
// returned full apply identity must be durably retained for exact future open.
func OpenBoundSQLWithApplyForSettlement(
	path string,
	wal *raftstore.Store,
	authority sqldriver.ReplicatedAuthorityProfile,
	expectedSQL sqldriver.ReplicatedShardStoreIdentity,
	options sqldriver.ReplicatedApplyOptions,
) (*sqldriver.Database, *sqldriver.ReplicatedApply, sqldriver.ReplicatedApplyIdentity, error) {
	binding, bootstrap, err := applyPrerequisites(wal, authority)
	if err != nil {
		return nil, nil, sqldriver.ReplicatedApplyIdentity{}, err
	}
	if expectedSQL.Binding != binding {
		return nil, nil, sqldriver.ReplicatedApplyIdentity{}, ErrBindingMismatch
	}
	database, identity, err := sqldriver.OpenReplicatedShardStoreWithApplyForSettlement(
		path, expectedSQL, options,
	)
	if err != nil {
		return nil, nil, sqldriver.ReplicatedApplyIdentity{}, err
	}
	claim, actual, err := database.OpenReplicatedApply(expectedSQL, bootstrap, options)
	if err != nil || actual != identity {
		if claim != nil {
			_ = claim.Close()
		}
		closeErr := database.Close()
		if err == nil {
			err = sqldriver.ErrReplicatedApplyMismatch
		}
		return nil, nil, sqldriver.ReplicatedApplyIdentity{}, errors.Join(err, closeErr)
	}
	return database, claim, identity, nil
}

func applyPrerequisites(
	wal *raftstore.Store,
	authority sqldriver.ReplicatedAuthorityProfile,
) (sqldriver.ReplicatedShardStoreBinding, *pb.Snapshot, error) {
	binding, err := BindingFromWAL(wal, authority)
	if err != nil {
		return sqldriver.ReplicatedShardStoreBinding{}, nil, err
	}
	base, err := wal.Snapshot()
	if err != nil {
		return sqldriver.ReplicatedShardStoreBinding{}, nil, fmt.Errorf(
			"%w: read snapshot base: %w", ErrWALUnavailable, err,
		)
	}
	bootstrap, err := replicatedstate.StaticBootstrapForSnapshot(base)
	if err != nil {
		return sqldriver.ReplicatedShardStoreBinding{}, nil, fmt.Errorf(
			"%w: recover static bootstrap from snapshot base: %w", ErrWALUnavailable, err,
		)
	}
	return binding, bootstrap, nil
}
