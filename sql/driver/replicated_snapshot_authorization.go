package driver

import (
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
)

// SnapshotAuthorizationFence reads one coherent durable source generation
// without checkpointing or pinning the source's data and system collections.
func (a *ReplicatedApply) SnapshotAuthorizationFence() (replicatedstate.SnapshotFence, error) {
	if a == nil || a.database == nil {
		return replicatedstate.SnapshotFence{}, ErrReplicatedApplyClosed
	}
	a.database.mu.RLock()
	defer a.database.mu.RUnlock()
	if err := a.checkLocked(); err != nil {
		return replicatedstate.SnapshotFence{}, err
	}
	if err := a.checkActivationBaseLocked(); err != nil {
		return replicatedstate.SnapshotFence{}, err
	}
	return a.machine.SnapshotAuthorizationFence()
}

// PublishedWithSnapshotAuthorizationFence returns the publication and its
// durable authorization fence under one apply read lock. Callers that need
// both values can therefore reject a membership or ownership advance between
// the two observations.
func (a *ReplicatedApply) PublishedWithSnapshotAuthorizationFence() (
	raftmodel.Publication, replicatedstate.SnapshotFence, error,
) {
	if a == nil || a.database == nil {
		return raftmodel.Publication{}, replicatedstate.SnapshotFence{}, ErrReplicatedApplyClosed
	}
	a.database.mu.RLock()
	defer a.database.mu.RUnlock()
	if err := a.checkLocked(); err != nil {
		return raftmodel.Publication{}, replicatedstate.SnapshotFence{}, err
	}
	if err := a.checkActivationBaseLocked(); err != nil {
		return raftmodel.Publication{}, replicatedstate.SnapshotFence{}, err
	}
	fence, err := a.machine.SnapshotAuthorizationFence()
	if err != nil {
		return raftmodel.Publication{}, replicatedstate.SnapshotFence{}, err
	}
	return a.machine.Published(), fence, nil
}

// PublishedLogicalEpochs returns the applied command epochs without taking a
// state snapshot. A closed apply reports ok false.
func (a *ReplicatedApply) PublishedLogicalEpochs() (policy, protection, ownership, schema, routing, generation uint64, ok bool) {
	if a == nil || a.database == nil {
		return 0, 0, 0, 0, 0, 0, false
	}
	a.database.mu.RLock()
	defer a.database.mu.RUnlock()
	if err := a.checkLocked(); err != nil {
		return 0, 0, 0, 0, 0, 0, false
	}
	return a.machine.PublishedLogicalEpochs()
}
