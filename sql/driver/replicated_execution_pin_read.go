package driver

import (
	"github.com/thesyncim/vibedb/internal/executionpin"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
)

// ExecutionPinRead exposes the exact hidden pin at the caller's ReadIndex
// floor, under the same live apply and activation fences as transaction reads.
// The fixed-width result owns its bytes; no SQL planning or row scan is needed.
func (a *ReplicatedApply) ExecutionPinRead(
	pin executionpin.PinID,
	minimumApplied uint64,
) (replicatedstate.ExecutionPinReadResult, error) {
	if a == nil || a.database == nil {
		return replicatedstate.ExecutionPinReadResult{}, ErrReplicatedApplyClosed
	}
	a.database.mu.RLock()
	defer a.database.mu.RUnlock()
	if err := a.checkLocked(); err != nil {
		return replicatedstate.ExecutionPinReadResult{}, err
	}
	if err := a.checkActivationBaseLocked(); err != nil {
		return replicatedstate.ExecutionPinReadResult{}, err
	}
	return a.machine.ExecutionPinRead(pin, minimumApplied)
}
