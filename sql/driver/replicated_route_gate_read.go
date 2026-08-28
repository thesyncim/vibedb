package driver

import (
	"github.com/thesyncim/vibedb/internal/replicatedstate"
)

// RouteGateRead exposes the committed gate at the caller's ReadIndex
// floor, under the same live apply and activation fences as transaction reads.
// The fixed-width result owns its bytes; no SQL planning or row scan is needed.
func (a *ReplicatedApply) RouteGateRead(
	minimumApplied uint64,
) (replicatedstate.RouteGateReadResult, error) {
	if a == nil || a.database == nil {
		return replicatedstate.RouteGateReadResult{}, ErrReplicatedApplyClosed
	}
	a.database.mu.RLock()
	defer a.database.mu.RUnlock()
	if err := a.checkLocked(); err != nil {
		return replicatedstate.RouteGateReadResult{}, err
	}
	if err := a.checkActivationBaseLocked(); err != nil {
		return replicatedstate.RouteGateReadResult{}, err
	}
	return a.machine.RouteGateRead(minimumApplied)
}
