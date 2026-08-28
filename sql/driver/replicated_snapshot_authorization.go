package driver

import "github.com/thesyncim/vibedb/internal/replicatedstate"

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
