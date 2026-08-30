package main

import (
	"errors"
	"fmt"
	"io"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/schemainstall"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"go.etcd.io/raft/v3"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

// openRF3RetainedApply settles an already Raft-authorized schema publication
// before a host, socket handler or result registry can use the old source.
// A source recovery handle is never adopted into Raft: after its exact final
// command is proved and the catalog CAS settles, only the target is opened.
func openRF3RetainedApply(path string, wal *raftstore.Store,
	base sqldriver.ReplicatedShardStoreIdentity, applyID sqldriver.ReplicatedApplyIdentity,
	opening ...sqldriver.ReplicatedOpenOptions,
) (sqldriver.ReplicatedShardStoreIdentity, sqldriver.ReplicatedApplyIdentity, *sqldriver.Database, *sqldriver.ReplicatedApply, error) {
	targetSelected := false
	_, published, err := sqldriver.ObservePublishedReplicatedSchemaTransition(path)
	if err != nil {
		return base, applyID, nil, nil, fmt.Errorf("observe published schema transition: %w", err)
	}
	if published {
		targetBase, targetApply, selected, err := sqldriver.PublishedReplicatedSchemaActivationIdentity(path)
		if err != nil || !selected {
			return base, applyID, nil, nil, errors.Join(fmt.Errorf("read published schema activation identity: %w", err), fmt.Errorf("published schema activation is not selectable: selected=%t", selected), schemainstall.ErrConflict)
		}
		if mismatch := rf3SchemaSuccessorMismatch(base, applyID, targetBase, targetApply); mismatch != nil {
			return base, applyID, nil, nil, errors.Join(mismatch, schemainstall.ErrConflict)
		}
		base, applyID = targetBase, targetApply
		targetSelected = true
	} else {
		transition, found, err := sqldriver.ObservePersistedReplicatedSchemaTransition(path)
		if err != nil {
			return base, applyID, nil, nil, fmt.Errorf("observe retained schema transition: %w", err)
		}
		if found {
			sourceOpening := append([]sqldriver.ReplicatedOpenOptions(nil), opening...)
			preparedApplied, preCommandApplied, emptySuffix, proofErr :=
				sqldriver.ObservePersistedReplicatedSchemaEmptySuffix(path, transition.Bytes())
			if proofErr != nil {
				return base, applyID, nil, nil, fmt.Errorf("observe retained schema empty suffix: %w", proofErr)
			}
			if emptySuffix {
				if err := rf3SchemaReplayNeutralSuffix(wal, preparedApplied, preCommandApplied,
					transition.RequestDigest, rf3SchemaTransitionGroup(transition.From)); err != nil {
					return base, applyID, nil, nil, fmt.Errorf("verify retained schema empty suffix: %w", err)
				}
				if len(sourceOpening) == 0 {
					sourceOpening = []sqldriver.ReplicatedOpenOptions{{}}
				}
				if len(sourceOpening) != 1 {
					return base, applyID, nil, nil, schemainstall.ErrConflict
				}
				sourceOpening[0].SchemaEmptySuffixPreparedApplied = preparedApplied
				sourceOpening[0].SchemaEmptySuffixPreCommandApplied = preCommandApplied
			}
			committedApplied, committedErr := sqldriver.ObservePersistedReplicatedSchemaCommittedApplied(path, transition.Bytes())
			if committedErr != nil {
				return base, applyID, nil, nil, fmt.Errorf("observe retained schema committed cut: %w", committedErr)
			}
			if len(sourceOpening) == 0 {
				sourceOpening = []sqldriver.ReplicatedOpenOptions{{}}
			}
			if len(sourceOpening) != 1 {
				return base, applyID, nil, nil, fmt.Errorf("retained schema source opening options: %w", schemainstall.ErrConflict)
			}
			lastIndex, lastErr := wal.LastIndex()
			if lastErr != nil {
				return base, applyID, nil, nil, fmt.Errorf("inspect retained schema WAL upper bound: %w", lastErr)
			}
			if lastIndex >= committedApplied {
				entries, entryErr := wal.Entries(committedApplied, committedApplied+1, 1<<20)
				// LastIndex is only the WAL's retained upper bound. A prepared
				// transition may reserve that bound without making the exact
				// command readable. Absence therefore proves neither commit nor
				// corruption; leave the committed alias empty and let source
				// recovery classify the authenticated prepared cut. Every other
				// storage error remains fail-closed.
				if errors.Is(entryErr, raft.ErrUnavailable) {
					entries = nil
					entryErr = nil
				}
				if entryErr != nil || len(entries) > 1 {
					return base, applyID, nil, nil, fmt.Errorf("inspect retained schema command: %w", entryErr)
				}
				if len(entries) == 1 {
					sourceOpening[0].SchemaCommittedTransition = string(entries[0].GetData())
				}
			}
			database, source, err := raftmember.OpenBoundSQLWithApplyForSchemaSourceTransition(path, wal, base.Binding.Authority, base, applyID, transition.Bytes(), sourceOpening...)
			if err != nil && !errors.Is(err, sqldriver.ErrSchemaSourceNotCommitted) {
				return base, applyID, nil, nil, fmt.Errorf("open retained schema source transition: %w", err)
			}
			if err == nil {
				if err := settleRF3SchemaSourceBeforeServing(source, database, transition.Bytes()); err != nil {
					return base, applyID, nil, nil, fmt.Errorf("settle retained schema source transition: %w", err)
				}
				targetBase, targetApply, selected, err := sqldriver.PublishedReplicatedSchemaActivationIdentity(path)
				if err != nil || !selected {
					return base, applyID, nil, nil, errors.Join(err, fmt.Errorf("settled schema activation is not selectable: selected=%t", selected), schemainstall.ErrConflict)
				}
				if mismatch := rf3SchemaSuccessorMismatch(base, applyID, targetBase, targetApply); mismatch != nil {
					return base, applyID, nil, nil, errors.Join(mismatch, schemainstall.ErrConflict)
				}
				base, applyID = targetBase, targetApply
				targetSelected = true
			}
		}
	}
	if targetSelected {
		opening, err = rf3SchemaTargetRecoveryOpening(path, wal, opening)
		if err != nil {
			return base, applyID, nil, nil, err
		}
	}
	database, apply, err := raftmember.OpenBoundSQLWithApplyRecoveringGeneration(path, wal, base.Binding.Authority, base, applyID, opening...)
	if err != nil {
		err = fmt.Errorf("open selected schema generation %d: %w", base.Binding.Authority.SchemaGeneration, err)
	}
	return base, applyID, database, apply, err
}

func rf3SchemaTargetRecoveryOpening(
	path string, wal *raftstore.Store, opening []sqldriver.ReplicatedOpenOptions,
) ([]sqldriver.ReplicatedOpenOptions, error) {
	transition, found, err := sqldriver.ObservePersistedReplicatedSchemaTransition(path)
	if err != nil {
		return opening, fmt.Errorf("observe selected schema transition: %w", err)
	}
	if !found {
		return opening, nil
	}
	preparedApplied, preCommandApplied, emptySuffix, err :=
		sqldriver.ObservePersistedReplicatedSchemaEmptySuffix(path, transition.Bytes())
	if err != nil {
		return opening, fmt.Errorf("observe selected schema empty suffix: %w", err)
	}
	committedApplied, err := sqldriver.ObservePersistedReplicatedSchemaCommittedApplied(path, transition.Bytes())
	if err != nil {
		return nil, fmt.Errorf("observe selected schema committed cut: %w", err)
	}
	committed, err := rf3SchemaCommittedTransitionAlias(wal, committedApplied)
	if err != nil {
		return nil, fmt.Errorf("inspect selected schema committed command: %w", err)
	}
	// When the committed entry itself remains in the suffix, independently
	// prove every leader no-op between preparation and commit. If that entry is
	// compacted, all earlier suffix entries are behind the same certified
	// snapshot; target open's finalized membership proof is the retained
	// authority and rereading the discarded no-ops is neither possible nor
	// necessary.
	if emptySuffix && len(committed) != 0 {
		if err := rf3SchemaReplayNeutralSuffix(wal, preparedApplied, preCommandApplied,
			transition.RequestDigest, rf3SchemaTransitionGroup(transition.From)); err != nil {
			return nil, fmt.Errorf("verify selected schema empty suffix: %w", err)
		}
	}
	result := append([]sqldriver.ReplicatedOpenOptions(nil), opening...)
	if len(result) == 0 {
		result = []sqldriver.ReplicatedOpenOptions{{}}
	}
	if len(result) != 1 {
		return nil, schemainstall.ErrConflict
	}
	if len(committed) != 0 {
		result[0].SchemaCommittedTransition = string(committed)
	}
	return result, nil
}

// rf3SchemaCommittedTransitionAlias returns the exact RF3-wide command while
// its log entry is retained. Once a certified snapshot has compacted that
// entry, target open instead authenticates the replica-local activation command
// against the finalized checkpoint membership transition. ErrUnavailable is
// deliberately different: it cannot prove commit or compaction and fails
// closed, as do gaps and malformed reads.
func rf3SchemaCommittedTransitionAlias(wal rf3SchemaWALReader, committedApplied uint64) ([]byte, error) {
	if wal == nil || committedApplied == 0 {
		return nil, schemainstall.ErrConflict
	}
	entries, err := wal.Entries(committedApplied, committedApplied+1, 1<<20)
	if errors.Is(err, raft.ErrCompacted) {
		return nil, nil
	}
	if err != nil || len(entries) != 1 || entries[0].GetIndex() != committedApplied ||
		entries[0].GetType() != raftpb.EntryNormal || len(entries[0].GetData()) == 0 {
		return nil, errors.Join(err, schemainstall.ErrConflict)
	}
	return append([]byte(nil), entries[0].GetData()...), nil
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
		return errors.Join(fmt.Errorf("observe committed transition: applied=%d committed=%t: %w", applied, committed, err), schemainstall.ErrConflict)
	}
	published, err := source.PublishReplicatedSchemaCatalog()
	if err != nil || !published {
		return errors.Join(fmt.Errorf("publish retained target catalog: published=%t: %w", published, err), schemainstall.ErrOutcomeUnknown)
	}
	return nil
}
