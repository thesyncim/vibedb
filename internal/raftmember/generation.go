package raftmember

import (
	"errors"

	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	pb "go.etcd.io/raft/v3/raftpb"
)

// PrepareWALGeneration binds one live SQL retention preparation to the exact
// captured Raft cut. The caller must exclusively own wal and apply through the
// later publish/settle sequence; concurrent runtime work can only stale the
// candidate and is never silently incorporated.
func PrepareWALGeneration(
	wal *raftstore.Store,
	apply *sqldriver.ReplicatedApply,
	preparation *sqldriver.WALBasePreparation,
	key raftstore.Key,
) (*raftstore.GenerationBuilder, error) {
	if wal == nil || apply == nil || preparation == nil {
		return nil, ErrWALUnavailable
	}
	if err := validateGenerationApplyBinding(wal, apply); err != nil {
		return nil, err
	}
	if err := apply.ValidateWALBasePreparation(preparation); err != nil {
		return nil, err
	}
	input, err := preparation.GenerationInput()
	if err != nil {
		return nil, err
	}
	return wal.PrepareGeneration(input, key)
}

// PublishWALGeneration revalidates the exact live SQL retention witness before
// selecting builder's already-synced candidate. Success fences the source WAL;
// it does not reclaim the logical leaf until CommitGenerationSelection invokes
// the SQL apply claim as its durable settler.
func PublishWALGeneration(
	wal *raftstore.Store,
	apply *sqldriver.ReplicatedApply,
	preparation *sqldriver.WALBasePreparation,
	builder *raftstore.GenerationBuilder,
) error {
	if wal == nil || apply == nil || preparation == nil || builder == nil {
		return ErrWALUnavailable
	}
	if err := validateGenerationApplyBinding(wal, apply); err != nil {
		return err
	}
	return apply.PublishWALGenerationSelection(preparation, wal, builder)
}

func validateGenerationApplyBinding(
	wal *raftstore.Store,
	apply *sqldriver.ReplicatedApply,
) error {
	profile, err := apply.CapacityQualificationProfile()
	if err != nil {
		return err
	}
	live, err := BindingFromWAL(wal, profile.Binding.Authority)
	if err != nil {
		return err
	}
	if live != profile.Binding {
		return ErrBindingMismatch
	}
	return nil
}

// OpenBoundSQLWithApplyForGenerationActivation is the sole restart path for a
// selected generation. Ordinary WAL inspection stays fenced, so this function
// first authenticates the pending activation, derives the unchanged immutable
// binding, and recovers the exact SQL apply owner needed by settlement.
func OpenBoundSQLWithApplyForGenerationActivation(
	path string,
	wal *raftstore.Store,
	authority sqldriver.ReplicatedAuthorityProfile,
	expectedSQL sqldriver.ReplicatedShardStoreIdentity,
	expectedApply sqldriver.ReplicatedApplyIdentity,
	opening ...sqldriver.ReplicatedOpenOptions,
) (*sqldriver.Database, *sqldriver.ReplicatedApply, error) {
	binding, bootstrap, err := pendingGenerationApplyPrerequisites(wal, authority)
	if err != nil {
		return nil, nil, err
	}
	if expectedSQL.Binding != binding {
		return nil, nil, ErrBindingMismatch
	}
	database, err := sqldriver.OpenReplicatedShardStoreWithApply(
		path, expectedSQL, expectedApply, opening...,
	)
	if err != nil {
		return nil, nil, err
	}
	options := sqldriver.ReplicatedApplyOptions{
		MaxSessions: expectedApply.MaxSessions,
		RetryWindow: expectedApply.RetryWindow,
		TxnLimits:   expectedApply.TxnLimits,
		Placement:   expectedApply.Placement,
	}
	claim, actual, err := database.OpenReplicatedApply(expectedSQL, bootstrap, options)
	if err != nil || actual != expectedApply {
		if claim != nil {
			_ = claim.Close()
		}
		closeErr := database.Close()
		if err == nil {
			err = sqldriver.ErrReplicatedApplyMismatch
		}
		return nil, nil, errors.Join(err, closeErr)
	}
	activation, err := wal.PendingGenerationActivation()
	if err == nil {
		err = claim.LatchGenerationActivation(activation)
	}
	if err != nil {
		closeErr := errors.Join(claim.Close(), database.Close())
		return nil, nil, errors.Join(err, closeErr)
	}
	return database, claim, nil
}

func pendingGenerationApplyPrerequisites(
	wal *raftstore.Store,
	authority sqldriver.ReplicatedAuthorityProfile,
) (sqldriver.ReplicatedShardStoreBinding, *pb.Snapshot, error) {
	if wal == nil {
		return sqldriver.ReplicatedShardStoreBinding{}, nil, ErrWALUnavailable
	}
	activation, err := wal.PendingGenerationActivation()
	if err != nil {
		return sqldriver.ReplicatedShardStoreBinding{}, nil, errors.Join(
			ErrWALUnavailable, err,
		)
	}
	if wal.RecoveredTornCurrentSlot() || wal.RecoveredTornFamilySlot() {
		return sqldriver.ReplicatedShardStoreBinding{}, nil, ErrWALQuarantined
	}
	identity := wal.Identity()
	binding := bindingFromIdentity(
		identity, wal.TopologyRecoveryEpoch(), authority,
	)
	bootstrap, err := replicatedstate.StaticBootstrapForSnapshot(activation.Snapshot)
	if err != nil {
		return sqldriver.ReplicatedShardStoreBinding{}, nil, errors.Join(
			ErrWALUnavailable, err,
		)
	}
	return binding, bootstrap, nil
}
