package rebalance

import (
	"bytes"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	pb "go.etcd.io/raft/v3/raftpb"
)

func TestPlanIntentCanonicalRestartRoundTripAndBounds(t *testing.T) {
	plan, catalog := moveTestPlan(t)
	publication := raftmodel.Publication{
		Applied: 5, ReplicaSetVersion: 4,
		ConfState: &pb.ConfState{Voters: []uint64{1, 3}},
	}
	prefix := []byte("prefix")
	raw, err := AppendPlanIntent(append([]byte(nil), prefix...), catalog, plan)
	if err != nil {
		t.Fatal(err)
	}
	intent := raw[len(prefix):]
	if len(intent) == 0 || len(intent) > MaxPlanIntentBytes {
		t.Fatalf("intent bytes = %d", len(intent))
	}
	recovered, err := OpenPlanIntent(intent, catalog, publication)
	if err != nil || recovered.OperationID() != plan.OperationID() {
		t.Fatalf("recovered=%v err=%v", recovered, err)
	}
	again, err := AppendPlanIntent(nil, catalog, recovered)
	if err != nil || !bytes.Equal(intent, again) {
		t.Fatalf("canonical replay changed: equal=%v err=%v", bytes.Equal(intent, again), err)
	}

	nonUniqueEmpty := bytes.Replace(intent, []byte(`"certificate":null`), []byte(`"certificate":""`), 1)
	if bytes.Equal(nonUniqueEmpty, intent) {
		t.Fatal("certificate representation not found")
	}
	for _, invalid := range [][]byte{
		append(append([]byte(nil), intent...), ' '),
		intent[:len(intent)-1],
		[]byte("{"),
		nonUniqueEmpty,
		make([]byte, MaxPlanIntentBytes+1),
	} {
		if _, err = OpenPlanIntent(invalid, catalog, publication); !errors.Is(err, ErrPlanIntent) {
			t.Fatalf("invalid intent error = %v", err)
		}
	}
}

func TestReplicaMoveOperationIdentityStableAndExact(t *testing.T) {
	plan, catalog := moveTestPlan(t)
	if plan.OperationID() == (OperationID{}) {
		t.Fatal("zero operation identity")
	}
	recovered, err := PlanReplicaMove(catalog, raftmodel.Publication{
		Applied: 6, ReplicaSetVersion: 6, ConfState: plan.learnerConf,
	}, moveTestRequest())
	if err != nil || recovered.OperationID() != plan.OperationID() {
		t.Fatalf("learner recovery identity=%x want=%x err=%v",
			recovered.OperationID(), plan.OperationID(), err)
	}

	request := moveTestRequest()
	request.Target = "another-target"
	changed := *plan
	changed.request = request
	changed.operation = replicaMoveOperationID(&changed)
	if changed.OperationID() == plan.OperationID() || changed.OperationID() == (OperationID{}) {
		t.Fatalf("request change did not produce an exact identity: %x", changed.OperationID())
	}

	// Identity hashing is length-delimited and streams the entire participant
	// set. It intentionally has no control-plane-specific member-count ceiling.
	many := *plan
	many.initialConf = &pb.ConfState{Voters: make([]uint64, 4096)}
	for index := range many.initialConf.Voters {
		many.initialConf.Voters[index] = uint64(index + 1)
	}
	first := replicaMoveOperationID(&many)
	many.initialConf.Voters[len(many.initialConf.Voters)-1]++
	second := replicaMoveOperationID(&many)
	if first == (OperationID{}) || second == (OperationID{}) || first == second {
		t.Fatalf("full participant identity not retained: first=%x second=%x", first, second)
	}
}

func TestReplicaMoveJournalIntentRemainsSmallAndImmutableAfterSnapshotBinding(t *testing.T) {
	plan, catalog := moveTestPlan(t)
	bound := bindMoveTestPlan(plan)
	unboundIntent, err := AppendReplicaMoveIntent(nil, catalog, plan)
	if err != nil {
		t.Fatal(err)
	}
	boundIntent, err := AppendReplicaMoveIntent(nil, catalog, bound)
	if err != nil || !bytes.Equal(unboundIntent, boundIntent) {
		t.Fatalf("journal intent changed after binding: bytes=%d equal=%v err=%v",
			len(boundIntent), bytes.Equal(unboundIntent, boundIntent), err)
	}
	if len(boundIntent) >= 40<<10 {
		t.Fatalf("immutable move intent exceeds replicated operation cell: %d", len(boundIntent))
	}
	certificate := &replicatedstate.SnapshotBaseCertificate{
		Manifest: replicatedstate.SnapshotArtifactManifest{State: bound.baseState},
		Digest:   bound.baseDigest,
	}
	recovered, err := OpenReplicaMoveIntent(boundIntent, catalog, raftmodel.Publication{
		Applied: 9, ReplicaSetVersion: 9, ConfState: plan.voterConf,
	}, certificate)
	if err != nil || !recovered.SnapshotBaseBound() ||
		recovered.OperationID() != plan.OperationID() {
		t.Fatalf("bound journal recovery=%+v err=%v", recovered, err)
	}
}
