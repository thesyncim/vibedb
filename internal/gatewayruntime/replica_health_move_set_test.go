package gatewayruntime

import (
	"context"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/rebalance"
)

type captureFailedReplicaMoveSetSink struct {
	singles []rebalance.FailedReplicaMoveIntent
	sets    [][]rebalance.FailedReplicaMoveIntent
}

func (sink *captureFailedReplicaMoveSetSink) SubmitFailedReplicaMove(
	_ context.Context, intent rebalance.FailedReplicaMoveIntent,
) error {
	sink.singles = append(sink.singles, intent)
	return nil
}

func (sink *captureFailedReplicaMoveSetSink) SubmitFailedReplicaMoves(
	_ context.Context, intents []rebalance.FailedReplicaMoveIntent,
) error {
	copySet := append([]rebalance.FailedReplicaMoveIntent(nil), intents...)
	sink.sets = append(sink.sets, copySet)
	return nil
}

func TestReplicaHealthControllerAtomicallySubmitsOneCertifiedMoveSet(t *testing.T) {
	snapshot := testReplicaHealthSnapshot(t)
	sink := new(captureFailedReplicaMoveSetSink)
	controller, err := newGatewayReplicaHealthController(
		testReplicaHealthCatalog{snapshot},
		testFailureAuthority{certificates: []rebalance.FailureQuorumCertificate{
			{Shard: "a"}, {Shard: "b"},
		}},
		testHealthObserver{}, testCandidateInventory{}, sink,
	)
	if err != nil {
		t.Fatal(err)
	}
	controller.schedule = func(
		ctx context.Context, cut rebalance.FailedReplicaPlanningCut,
		out rebalance.FailedReplicaMoveSink,
	) (rebalance.FailedReplicaMoveIntent, error) {
		var operation rebalance.OperationID
		operation[0] = byte(cut.Certificate.Shard[0])
		intent := rebalance.FailedReplicaMoveIntent{
			Operation: operation,
		}
		return intent, out.SubmitFailedReplicaMove(ctx, intent)
	}

	pass, err := controller.RunPass(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if pass.Certificates != 2 || pass.Eligible != 2 || pass.Submitted != 2 {
		t.Fatalf("pass=%+v", pass)
	}
	if len(sink.singles) != 0 || len(sink.sets) != 1 || len(sink.sets[0]) != 2 {
		t.Fatalf("singles=%d sets=%d set-width=%d", len(sink.singles), len(sink.sets), len(sink.sets[0]))
	}
	if sink.sets[0][0].Operation[0] != byte(distribution.ShardID("a")[0]) ||
		sink.sets[0][1].Operation[0] != byte(distribution.ShardID("b")[0]) {
		t.Fatalf("move set lost certificate order: %+v", sink.sets[0])
	}
}
