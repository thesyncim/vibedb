package main

import (
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/thesyncim/vibedb/internal/migrationbudget"
	"github.com/thesyncim/vibedb/internal/nodecontrol"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftstore"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	pb "go.etcd.io/raft/v3/raftpb"
)

// One process owns the physical log, append worker and checkpoint worker.
// Member runtimes own only their SQL/apply handles. They must drain before the
// owner closes; closing one group never closes another group's durability lane.
type rf3NodeOwner struct {
	store            *raftstore.NodeStore
	sequencer        *raftstore.NodeSubmissionSequencer
	checkpoints      *raftmember.NodeCheckpointCoordinator
	controlMu        sync.Mutex
	emptyRuntime     *rf3EmptyNodeRuntime
	migrationBudget  *migrationbudget.Budget
	pressureStop     chan struct{}
	pressureDone     chan struct{}
	pressureStopOnce sync.Once
}

// bindEmptyRuntime is the process-local handoff used by a bootstrap-directory
// adapter. It never publishes authority; it only exposes the already-created
// fail-closed reader slot and dynamic receiver registry while this owner is
// alive. A second runtime cannot replace a live physical-node owner.
func (owner *rf3NodeOwner) bindEmptyRuntime(runtime *rf3EmptyNodeRuntime) error {
	if owner == nil || runtime == nil {
		return raftmember.ErrRuntimeOwnership
	}
	owner.controlMu.Lock()
	defer owner.controlMu.Unlock()
	if owner.emptyRuntime != nil && owner.emptyRuntime != runtime {
		return raftmember.ErrRuntimeOwnership
	}
	owner.emptyRuntime = runtime
	return nil
}

func (owner *rf3NodeOwner) emptyIntentReader() *nodecontrol.IntentReaderSlot {
	if owner == nil {
		return nil
	}
	owner.controlMu.Lock()
	runtime := owner.emptyRuntime
	owner.controlMu.Unlock()
	if runtime == nil {
		return nil
	}
	return runtime.IntentReaderSlot()
}

// emptyRuntimeHandle returns the live physical-node runtime while it is
// owned by this process. Callers use it only to complete a certified learner
// install; the runtime itself still performs the serialized publication.
func (owner *rf3NodeOwner) emptyRuntimeHandle() *rf3EmptyNodeRuntime {
	if owner == nil {
		return nil
	}
	owner.controlMu.Lock()
	runtime := owner.emptyRuntime
	owner.controlMu.Unlock()
	return runtime
}

func (owner *rf3NodeOwner) unbindEmptyRuntime(runtime *rf3EmptyNodeRuntime) {
	if owner == nil || runtime == nil {
		return
	}
	owner.controlMu.Lock()
	if owner.emptyRuntime == runtime {
		owner.emptyRuntime = nil
	}
	owner.controlMu.Unlock()
}

func newRF3NodeOwner(store *raftstore.NodeStore, budgets ...*migrationbudget.Budget) (*rf3NodeOwner, error) {
	sequencer, err := raftstore.NewNodeSubmissionSequencer(store, 4*maxRF3ManifestGroups)
	if err != nil {
		return nil, err
	}
	checkpoints, err := raftmember.NewNodeCheckpointCoordinator(sequencer, maxRF3ManifestGroups)
	if err != nil {
		return nil, errors.Join(err, sequencer.Close())
	}
	owner := &rf3NodeOwner{store: store, sequencer: sequencer, checkpoints: checkpoints}
	if len(budgets) != 0 && budgets[0] != nil {
		owner.startMigrationPressureSampler(budgets[0])
	}
	return owner, nil
}

func (owner *rf3NodeOwner) Close() error {
	if owner == nil {
		return nil
	}
	owner.stopMigrationPressureSampler()
	owner.controlMu.Lock()
	owner.emptyRuntime = nil
	owner.controlMu.Unlock()
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

// registerAndAdoptDynamic publishes the certified post-transfer snapshot in
// the node-wide log and adopts its SQL/apply handles against that exact
// checkpoint. Registration and the requested incarnation are one sequenced
// durability operation; the node never infers a target identity from a local
// counter after the directory has committed it.
func (owner *rf3NodeOwner) registerAndAdoptDynamic(
	descriptor raftstore.GroupDescriptor, snapshot *pb.Snapshot, expectedIncarnation uint64,
	database *sqldriver.Database, apply *sqldriver.ReplicatedApply,
) (*raftmember.Runtime, error) {
	if owner == nil || owner.store == nil || owner.sequencer == nil || database == nil || apply == nil ||
		snapshot == nil || expectedIncarnation == 0 || descriptor.LogKey != 0 {
		return nil, raftmember.ErrRuntimeOwnership
	}
	if !owner.sequencer.Owns(owner.store) {
		return nil, raftmember.ErrNodePersistenceBinding
	}
	var submission raftstore.Submission
	if err := submission.Initialize(); err != nil {
		return nil, err
	}
	if err := submission.PrepareRegisterGroupWithSnapshotAt(descriptor, snapshot, expectedIncarnation); err != nil {
		return nil, err
	}
	if _, err := owner.sequencer.TrySubmit(&submission); err != nil {
		return nil, err
	}
	if _, err := submission.Wait(); err != nil {
		return nil, err
	}
	registered, incarnation, ok := submission.RegisteredGroup()
	wantRegistered := descriptor
	wantRegistered.LogKey = registered.LogKey
	if !ok || registered != wantRegistered || incarnation.Incarnation != expectedIncarnation {
		return nil, raftmember.ErrNodePersistenceBinding
	}
	group := owner.store.Group(registered.LogKey)
	if group == nil {
		return nil, raftmember.ErrNodePersistenceBinding
	}
	return owner.adoptRegistered(group, database, apply)
}

// adoptRegistered is the node-log counterpart to adopt. The group registration
// already durably contains the certified snapshot and exact incarnation, so
// this method must not mint another incarnation while recovering a retry.
func (owner *rf3NodeOwner) adoptRegistered(
	group *raftstore.GroupView, database *sqldriver.Database, apply *sqldriver.ReplicatedApply,
) (*raftmember.Runtime, error) {
	if owner == nil || group == nil || database == nil || apply == nil {
		return nil, raftmember.ErrRuntimeOwnership
	}
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
	incarnation, err := group.NodeIncarnation()
	if err != nil || incarnation == 0 {
		return nil, errors.Join(raftmember.ErrRuntimeOwnership, err)
	}
	persistence, err := raftmember.BindNodeRuntimePersistence(owner.store, owner.sequencer,
		raftmember.RuntimeIdentity{Group: groupFromBinding(profile.Binding), Distribution: profile.Binding.Distribution,
			Shard: profile.Binding.Shard, AllocationGeneration: profile.Binding.AllocationGeneration,
			MemberID: profile.Binding.MemberID, StoreID: profile.Binding.StoreID, NodeIncarnation: incarnation,
			RelationManifestDigest: profile.RelationManifestDigest})
	if err != nil {
		return nil, err
	}
	runtime, err := raftmember.AdoptNodeRuntime(persistence, database, apply)
	if err != nil {
		return runtime, err
	}
	if err := runtime.ConfigureNodeCheckpointing(owner.checkpoints, raftmember.NodeCheckpointOptions{
		IntervalTicks: rf3WALGenerationIntervalTicks,
		OnError:       func(err error) { fmt.Fprintf(os.Stderr, "RF3 node checkpoint deferred: %v\n", err) },
	}); err != nil {
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
