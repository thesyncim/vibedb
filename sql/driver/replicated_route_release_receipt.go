package driver

import "github.com/thesyncim/vibedb/internal/replicatedstate"

// RouteReleaseReceiptReadInto resolves one exact retained route-session
// release completion while holding the ReplicatedApply publication read lock.
// The narrow state-machine method performs command-shape, immutable-binding,
// and result-proof validation; this adapter exposes no generic collection
// reader to the owner lane.
func (a *ReplicatedApply) RouteReleaseReceiptReadInto(
	data []byte,
	dst []byte,
) (replicatedstate.CompletionLookup, error) {
	if a == nil || a.database == nil {
		return replicatedstate.CompletionLookup{}, ErrReplicatedApplyClosed
	}
	a.database.mu.RLock()
	defer a.database.mu.RUnlock()
	if err := a.checkLocked(); err != nil {
		return replicatedstate.CompletionLookup{}, err
	}
	if err := a.checkActivationBaseLocked(); err != nil {
		return replicatedstate.CompletionLookup{}, err
	}
	return a.machine.RouteReleaseReceiptReadInto(data, dst)
}
