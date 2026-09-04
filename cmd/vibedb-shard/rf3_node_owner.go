package main

import (
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftstore"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

// One process owns the physical log, append worker and checkpoint worker.
// Member runtimes own only their SQL/apply handles. They must drain before the
// owner closes; closing one group never closes another group's durability lane.
type rf3NodeOwner struct {
	store       *raftstore.NodeStore
	sequencer   *raftstore.NodeSubmissionSequencer
	checkpoints *raftmember.NodeCheckpointCoordinator
	controlMu   sync.Mutex
}

func newRF3NodeOwner(store *raftstore.NodeStore) (*rf3NodeOwner, error) {
	sequencer, err := raftstore.NewNodeSubmissionSequencer(store, 4*maxRF3ManifestGroups)
	if err != nil {
		return nil, err
	}
	checkpoints, err := raftmember.NewNodeCheckpointCoordinator(sequencer, maxRF3ManifestGroups)
	if err != nil {
		return nil, errors.Join(err, sequencer.Close())
	}
	return &rf3NodeOwner{store: store, sequencer: sequencer, checkpoints: checkpoints}, nil
}

func (owner *rf3NodeOwner) Close() error {
	if owner == nil {
		return nil
	}
	return errors.Join(owner.checkpoints.Close(), owner.sequencer.Close(), owner.store.Close())
}

func (owner *rf3NodeOwner) group(binding sqldriver.ReplicatedShardStoreBinding) (*raftstore.GroupView, error) {
	if owner == nil {
		return nil, raftmember.ErrWALUnavailable
	}
	group, found := owner.store.GroupByID(binding.GroupID)
	if !found {
		return nil, raftmember.ErrBindingMismatch
	}
	actual, err := raftmember.BindingFromNodeGroup(group, binding.Authority)
	if err != nil || actual != binding {
		return nil, errors.Join(raftmember.ErrBindingMismatch, err)
	}
	return group, nil
}

func (owner *rf3NodeOwner) adopt(group *raftstore.GroupView, database *sqldriver.Database, apply *sqldriver.ReplicatedApply) (*raftmember.Runtime, error) {
	if owner == nil || group == nil || database == nil || apply == nil {
		return nil, raftmember.ErrRuntimeOwnership
	}
	// Prove the exact SQL/group binding before durably allocating an incarnation.
	profile, err := apply.CapacityQualificationProfile()
	if err != nil {
		return nil, err
	}
	bound, err := owner.group(profile.Binding)
	if err != nil {
		return nil, err
	}
	expected, err := bound.Descriptor()
	if err != nil {
		return nil, err
	}
	actual, err := group.Descriptor()
	if err != nil || actual != expected {
		return nil, errors.Join(raftmember.ErrBindingMismatch, err)
	}
	if err := raftmember.ValidateNodeApplyCapacity(bound, apply); err != nil {
		return nil, err
	}
	// This cold control operation uses the same sequencer as live group appends.
	// No direct NodeStore write can race it during hot group enrollment.
	owner.controlMu.Lock()
	var submission raftstore.Submission
	err = submission.Initialize()
	if err == nil {
		err = submission.PrepareBeginIncarnations([]uint64{expected.LogKey})
	}
	if err == nil {
		_, err = owner.sequencer.TrySubmit(&submission)
	}
	if err == nil {
		_, err = submission.Wait()
	}
	owner.controlMu.Unlock()
	if err != nil {
		return nil, err
	}
	identity, err := raftmember.RuntimeIdentityFromNodeGroup(bound, apply)
	if err != nil {
		return nil, err
	}
	persistence, err := raftmember.BindNodeRuntimePersistence(owner.store, owner.sequencer, identity)
	if err != nil {
		return nil, err
	}
	runtime, err := raftmember.AdoptNodeRuntime(persistence, database, apply)
	if err != nil {
		return runtime, err
	}
	if err := runtime.ConfigureNodeCheckpointing(owner.checkpoints, raftmember.NodeCheckpointOptions{IntervalTicks: rf3WALGenerationIntervalTicks, OnError: func(err error) { fmt.Fprintf(os.Stderr, "RF3 node checkpoint deferred: %v\n", err) }}); err != nil {
		return runtime, err
	}
	return runtime, nil
}

func (group *preparedRF3Group) recoveryLog() rf3RecoveryLog {
	if group.nodeLog != nil {
		return group.nodeLog
	}
	return group.wal
}

func (group *preparedRF3Group) adoptRuntime() (*raftmember.Runtime, error) {
	if group.nodeOwner != nil {
		return group.nodeOwner.adopt(group.nodeLog, group.database, group.apply)
	}
	runtime, err := raftmember.AdoptPipelinedRuntime(group.wal, group.database, group.apply)
	if err != nil {
		return runtime, err
	}
	err = runtime.ConfigureWALGeneration(raftmember.WALGenerationDriverOptions{IntervalTicks: rf3WALGenerationIntervalTicks, Key: group.key, OnError: func(err error) { fmt.Fprintf(os.Stderr, "RF3 WAL generation deferred: %v\n", err) }})
	clear(group.key.Material[:])
	return runtime, err
}
