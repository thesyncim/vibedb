package splitcontroller

import (
	"crypto/sha256"
	"testing"

	"github.com/thesyncim/vibedb/gateway"
)

func TestPreparedPlanObservationDirectoryResolvesUnpublishedRF3Exactly(t *testing.T) {
	plan, snapshot, target, _ := testPlanWithChildLeaders(t, rf3ChildLeaders())
	intent, err := AppendPlanIntent(nil, snapshot, plan)
	if err != nil {
		t.Fatal(err)
	}
	record := gateway.ReplicatedOperationRecord{
		ID: [32]byte(plan.OperationID()), Kind: gateway.ReplicatedOperationSplit,
		State: gateway.ReplicatedOperationPlanned, Revision: 1,
		CatalogGeneration: snapshot.Generation(), IntentDigest: sha256.Sum256(intent), Intent: intent,
	}
	catalog := &testControllerCatalog{
		memoryReplicatedOperationJournal: &memoryReplicatedOperationJournal{record: record, present: true},
		catalog:                          snapshot,
	}
	digest, err := gateway.CatalogSnapshotDigest(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	request := newPlanObservationRequest(
		plan, snapshot, digest, plan.source.Distribution, plan.children[1].Shard,
		plan.children[1].AllocationGeneration, 1, rf3ChildLeaders(),
	)
	directory, err := NewPreparedPlanObservationPeerDirectory(catalog)
	if err != nil {
		t.Fatal(err)
	}
	for index, endpoint := range request.ControlEndpoints {
		peer, resolveErr := directory.ResolvePlanObservationPeer(t.Context(), request, endpoint)
		if resolveErr != nil || peer.Node != target.Replicas[index].Node ||
			peer.MemberID != target.Replicas[index].Member {
			t.Fatalf("endpoint=%q peer=%+v err=%v", endpoint, peer, resolveErr)
		}
	}
	wrong := request
	wrong.Command.ReplicaSetVersion++
	wrong.RequestDigest = planObservationRequestDigest(wrong)
	if _, err = directory.ResolvePlanObservationPeer(t.Context(), wrong, request.ControlEndpoints[0]); err == nil {
		t.Fatal("wrong unpublished RF3 version resolved")
	}
}
