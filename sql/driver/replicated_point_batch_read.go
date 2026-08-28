package driver

import "github.com/thesyncim/vibedb/internal/replicatedstate"

// PointReadBatchInto reads one coherent replicated relation cut at the explicit
// quorum-applied floor, under the same live ownership and activation checks as
// PointReadInto. Packed input and output remain bounded by the machine codec.
func (a *ReplicatedApply) PointReadBatchInto(
	packed []byte, minimumApplied uint64, maxResultBytes int, dst []byte,
) (replicatedstate.PointReadBatchResult, error) {
	if a == nil || a.database == nil {
		return replicatedstate.PointReadBatchResult{}, ErrReplicatedApplyClosed
	}
	a.database.mu.RLock()
	defer a.database.mu.RUnlock()
	if err := a.checkLocked(); err != nil {
		return replicatedstate.PointReadBatchResult{}, err
	}
	if err := a.checkActivationBaseLocked(); err != nil {
		return replicatedstate.PointReadBatchResult{}, err
	}
	return a.machine.PointReadBatchInto(packed, minimumApplied, maxResultBytes, dst)
}
