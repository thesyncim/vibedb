package main

import (
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftstore"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	pb "go.etcd.io/raft/v3/raftpb"
)

// Recovery uses an authenticated group log, independent of whether the physical
// durability unit is a legacy range file or a shared node store. Only these two
// sealed backends can bind SQL; an arbitrary reader is never serving authority.
type rf3RecoveryLog interface {
	rf3SchemaWALReader
	LastIndex() (uint64, error)
	Snapshot() (*pb.Snapshot, error)
}

func validateRF3RecoveryLog(log rf3RecoveryLog) error {
	switch value := log.(type) {
	case *raftstore.Store:
		if value != nil {
			return nil
		}
	case *raftstore.GroupView:
		if value != nil {
			return nil
		}
	}
	return raftmember.ErrWALUnavailable
}

func openRF3SelectedLog(path string, log rf3RecoveryLog,
	base sqldriver.ReplicatedShardStoreIdentity, apply sqldriver.ReplicatedApplyIdentity,
	opening ...sqldriver.ReplicatedOpenOptions,
) (*sqldriver.Database, *sqldriver.ReplicatedApply, error) {
	switch value := log.(type) {
	case *raftstore.Store:
		return raftmember.OpenBoundSQLWithApplyRecoveringGeneration(path, value, base.Binding.Authority, base, apply, opening...)
	case *raftstore.GroupView:
		return raftmember.OpenBoundNodeSQLWithApply(path, value, base.Binding.Authority, base, apply, opening...)
	default:
		return nil, nil, raftmember.ErrWALUnavailable
	}
}

func openRF3SchemaSourceLog(path string, log rf3RecoveryLog,
	base sqldriver.ReplicatedShardStoreIdentity, apply sqldriver.ReplicatedApplyIdentity,
	command []byte, opening ...sqldriver.ReplicatedOpenOptions,
) (*sqldriver.Database, *sqldriver.ReplicatedApply, error) {
	switch value := log.(type) {
	case *raftstore.Store:
		return raftmember.OpenBoundSQLWithApplyForSchemaSourceTransition(path, value, base.Binding.Authority, base, apply, command, opening...)
	case *raftstore.GroupView:
		// The SQL source opener authenticates its exact prepared or committed
		// checkpoint. Applied SQL may durably certify commit before an async
		// commit-only HardState reaches disk, so that marker is not a gate.
		// The recovery flow also supplies the retained command alias; the binder
		// checks the group identity, snapshot and source transition proof.
		return raftmember.OpenBoundNodeSQLWithApplyForSchemaSourceTransition(path, value, base.Binding.Authority, base, apply, command, opening...)
	default:
		return nil, nil, raftmember.ErrWALUnavailable
	}
}

func rf3DurableLogCommit(log rf3RecoveryLog) (uint64, error) {
	switch value := log.(type) {
	case *raftstore.Store:
		return value.DurableCommit()
	case *raftstore.GroupView:
		hard, _, err := value.InitialState()
		if err != nil {
			return 0, err
		}
		return hard.GetCommit(), nil
	default:
		return 0, raftmember.ErrWALUnavailable
	}
}
