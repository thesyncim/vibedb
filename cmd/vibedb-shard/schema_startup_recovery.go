package main

import (
	"errors"
	"io"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/schemainstall"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

// openRF3RetainedApply settles an already Raft-authorized schema publication
// before a host, socket handler or result registry can use the old source.
// A source recovery handle is never adopted into Raft: after its exact final
// command is proved and the catalog CAS settles, only the target is opened.
func openRF3RetainedApply(path string, wal *raftstore.Store,
	base sqldriver.ReplicatedShardStoreIdentity, applyID sqldriver.ReplicatedApplyIdentity,
	opening ...sqldriver.ReplicatedOpenOptions,
) (sqldriver.ReplicatedShardStoreIdentity, sqldriver.ReplicatedApplyIdentity, *sqldriver.Database, *sqldriver.ReplicatedApply, error) {
	_, published, err := sqldriver.ObservePublishedReplicatedSchemaTransition(path)
	if err != nil {
		return base, applyID, nil, nil, err
	}
	if published {
		targetBase, targetApply, selected, err := sqldriver.PublishedReplicatedSchemaActivationIdentity(path)
		if err != nil || !selected || !rf3SchemaSuccessorMatchesRetained(base, applyID, targetBase, targetApply) {
			return base, applyID, nil, nil, errors.Join(err, schemainstall.ErrConflict)
		}
		base, applyID = targetBase, targetApply
	} else {
		transition, found, err := sqldriver.ObservePersistedReplicatedSchemaTransition(path)
		if err != nil {
			return base, applyID, nil, nil, err
		}
		if found {
			database, source, err := raftmember.OpenBoundSQLWithApplyForSchemaSourceTransition(path, wal, base.Binding.Authority, base, applyID, transition.Bytes(), opening...)
			if err != nil && !errors.Is(err, sqldriver.ErrSchemaSourceNotCommitted) {
				return base, applyID, nil, nil, err
			}
			if err == nil {
				if err := settleRF3SchemaSourceBeforeServing(source, database, transition.Bytes()); err != nil {
					return base, applyID, nil, nil, err
				}
				targetBase, targetApply, selected, err := sqldriver.PublishedReplicatedSchemaActivationIdentity(path)
				if err != nil || !selected || !rf3SchemaSuccessorMatchesRetained(base, applyID, targetBase, targetApply) {
					return base, applyID, nil, nil, errors.Join(err, schemainstall.ErrConflict)
				}
				base, applyID = targetBase, targetApply
			}
		}
	}
	database, apply, err := raftmember.OpenBoundSQLWithApplyRecoveringGeneration(path, wal, base.Binding.Authority, base, applyID, opening...)
	return base, applyID, database, apply, err
}

type rf3SchemaSourceRecovery interface {
	ObserveReplicatedSchemaTransition([]byte) (uint64, bool, error)
	PublishReplicatedSchemaCatalog() (bool, error)
	Close() error
}

func settleRF3SchemaSourceBeforeServing(source rf3SchemaSourceRecovery, database io.Closer, command []byte) (err error) {
	defer func() { err = errors.Join(err, source.Close(), database.Close()) }()
	applied, committed, err := source.ObserveReplicatedSchemaTransition(command)
	if err != nil || !committed || applied == 0 {
		return errors.Join(err, schemainstall.ErrConflict)
	}
	published, err := source.PublishReplicatedSchemaCatalog()
	if err != nil || !published {
		return errors.Join(err, schemainstall.ErrOutcomeUnknown)
	}
	return nil
}
