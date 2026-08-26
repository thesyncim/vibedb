package gateway

import (
	"context"
)

// RecoverReplicatedTransactionRequests redrives every same-process RF3 request
// that still owns exact outcome-unknown bytes. The request registry performs
// the actual bounded snapshot and never holds its directory lock during I/O.
func (executor *Executor) RecoverReplicatedTransactionRequests(
	ctx context.Context,
) (int, error) {
	if executor == nil || executor.replicatedTransactionRequests == nil {
		return 0, nil
	}
	return executor.replicatedTransactionRequests.RecoverPending(
		executor.recoveryContext(ctx),
	)
}
