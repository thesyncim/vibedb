package splitcontroller

import (
	"context"
	"errors"

	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/rangesplit"
)

// RF3TailSinkResolver fans each child-local tail batch to every exact prepared
// replica before acknowledging it to the source capture. The retained child is
// already applied by the source group and therefore needs only a local ack.
type RF3TailSinkResolver struct{ Client *TailStreamClient }

func (resolver RF3TailSinkResolver) ResolveSplitTailSinks(
	ctx context.Context, plan *Plan, observed Observation,
) ([]rangesplit.TailSink, error) {
	if resolver.Client == nil || ctx == nil || plan == nil || observed.Artifacts == nil ||
		plan.partitioner.ValidateChildArtifactSet(*observed.Artifacts) != nil {
		return nil, ErrTailStreamControl
	}
	result := make([]rangesplit.TailSink, plan.childCount)
	for child := uint8(0); child < plan.childCount; child++ {
		if child == plan.retained {
			result[child] = func(rangesplit.TailBatch) error { return nil }
			continue
		}
		cursor := observed.Stages[child]
		target, ok := plan.Target(child)
		if cursor == nil || !ok || len(target.Replicas) == 0 {
			return nil, ErrTailStreamControl
		}
		binding, err := rangesplit.NewTailStreamBinding(
			[32]byte(plan.OperationID()), observed.Artifacts.Children[child],
		)
		if err != nil {
			return nil, err
		}
		trust := rafttransport.TrustDomain{
			ClusterID: target.WAL.ClusterID, ClusterIncarnation: target.WAL.ClusterIncarnation,
		}
		remotes := make([]*RemoteTailSink, len(target.Replicas))
		for index, replica := range target.Replicas {
			remotes[index], err = NewRemoteTailSink(
				ctx, resolver.Client, replica.Node, trust, binding, *cursor,
			)
			if err != nil {
				return nil, err
			}
		}
		result[child] = func(batch rangesplit.TailBatch) error {
			var joined error
			for _, sink := range remotes {
				if joined == nil {
					joined = sink.Apply(batch)
				}
			}
			return joined
		}
	}
	for _, sink := range result {
		if sink == nil {
			return nil, errors.New("splitcontroller: incomplete RF3 tail sink set")
		}
	}
	return result, nil
}

var _ SplitTailSinkResolver = RF3TailSinkResolver{}
