package raftservice

import "context"

// ReadPointBatch routes the complete batch to its exact published group owner.
// The owner retains the same single ReadIndex, generation lease and coherent
// relation-cut checks as a direct batch call; this layer does not split it.
func (owners *ExecutionOwners) ReadPointBatch(ctx context.Context, request PointReadBatchRequest) (PointReadBatchResult, PointReadLease, error) {
	owner, err := owners.owner(request.Fence.Group)
	if err != nil {
		return PointReadBatchResult{}, nil, err
	}
	return owner.ReadPointBatch(ctx, request)
}
