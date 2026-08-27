package splitcontroller

import (
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/rangesplit"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"go.etcd.io/raft/v3"
)

func TestChildReplicaObservationsBindIndependentStoresAndLifecycle(t *testing.T) {
	plan, _, target, _ := testPlanWithChildLeaders(t, []distribution.EndpointID{"node-b", "node-c", "node-d"})
	request, _, _ := networkPlanObservationFixture(t)
	request.Child = target.Child
	request.RequestDigest = planObservationRequestDigest(request)
	results := make([]planObservationMemberResult, len(target.Replicas))
	for index, replica := range target.Replicas {
		results[index].cut = ChildPlanObservation{RequestDigest: request.RequestDigest, Runtime: &ChildObservation{
			Child: target.Child, Phase: ChildPhaseActivated, ApplyIdentity: replica.Apply,
			ApplyProfile: sqldriver.ReplicatedApplyCapacityProfile{Binding: replica.SQL.Binding,
				Initialized: true, RelationManifestDigest: target.RelationManifestDigest,
				MaxSessions: replica.Apply.MaxSessions, RetryWindow: replica.Apply.RetryWindow},
		}}
		var wire bytesBuffer
		response := planObservationWireResponse{Format: planObservationWireFormat, Kind: planObservationChild,
			RequestDigest: request.RequestDigest, Runtime: appendWireChildObservation(results[index].cut.Runtime)}
		if err := writePlanObservationResponse(&wire, response, MaxPlanObservationResponseBytes); err != nil {
			t.Fatalf("activated member response encoding: %v", err)
		}
		decoded, err := readPlanObservationResponse(&wire, MaxPlanObservationResponseBytes)
		if err != nil || decoded.Runtime == nil {
			t.Fatalf("activated member response roundtrip: %v", err)
		}
		opened, err := openChildPlanObservation(decoded)
		if err != nil || !sameChildObservationBase(*opened.Runtime, *results[index].cut.Runtime) {
			t.Fatalf("activated member response lost local proof: %v", err)
		}
	}
	check := func(want ActionKind) *ChildObservation {
		t.Helper()
		merged, err := mergeChildPlanObservations(request, results)
		if err != nil || merged.Runtime == nil || len(merged.Runtime.Members) != len(results) {
			t.Fatalf("independent member merge: %v", err)
		}
		var observed Observation
		observed.Children[target.Child] = merged.Runtime
		action, needed, err := plan.childAction(observed, rangesplit.CutoverCertificate{})
		if err != nil || needed != (want != 0) || action.Kind != want {
			t.Fatalf("child action=%+v needed=%t want=%d err=%v", action, needed, want, err)
		}
		return merged.Runtime
	}
	check(ActionCreateChildWAL)
	// A partial wave may legitimately expose different lifecycle phases. The
	// earliest member determines the remaining action, not equality of stores.
	results[0].cut.Runtime.Phase = ChildPhaseWALCreated
	results[0].cut.Runtime.WALBinding = target.Replicas[0].SQL.Binding
	check(ActionCreateChildWAL)
	for index := range results {
		results[index].cut.Runtime.Phase = ChildPhaseWALCreated
		results[index].cut.Runtime.WALBinding = target.Replicas[index].SQL.Binding
	}
	check(ActionAdoptChildRuntime)
	for index, replica := range target.Replicas {
		local := target
		local.WAL, local.SQL = replica.WAL, replica.SQL
		runtime := results[index].cut.Runtime
		runtime.Phase = ChildPhaseRuntimeAdopted
		runtime.RuntimeIdentity = testRuntimeIdentity(local)
	}
	check(ActionAwaitChildReady)
	ready := testReadyServingStates(target, results[0].cut.Runtime.ApplyProfile, 3, 1)
	for index := range results {
		ready[index].Identity = results[index].cut.Runtime.RuntimeIdentity
		results[index].cut.Runtime.ReadyReplicas = ready[index : index+1]
	}
	valid := check(0)
	for _, elected := range []uint64{2, 3} {
		for index := range results {
			serving := &results[index].cut.Runtime.ReadyReplicas[0]
			serving.Status.LeaderID = elected
			serving.Status.RaftState = raft.StateFollower
			if serving.Identity.MemberID == elected {
				serving.Status.RaftState = raft.StateLeader
			}
		}
		check(0)
	}
	for _, mutate := range []func(*ChildReplicaObservation){
		func(m *ChildReplicaObservation) { m.Member++ },
		func(m *ChildReplicaObservation) { m.ApplyIdentity.Storage = target.Replicas[1].Apply.Storage },
		func(m *ChildReplicaObservation) { m.ApplyProfile.Binding = target.Replicas[1].SQL.Binding },
		func(m *ChildReplicaObservation) { m.WALBinding = target.Replicas[1].SQL.Binding },
		func(m *ChildReplicaObservation) { m.RuntimeIdentity.StoreID = target.Replicas[1].StoreID },
		func(m *ChildReplicaObservation) { m.ApplyProfile.RelationManifestDigest[0]++ },
	} {
		forged := cloneChildPlanRuntime(valid)
		mutate(&forged.Members[0])
		if _, _, err := replicatedChildAction(target, *forged, rangesplit.CutoverCertificate{}); !errors.Is(err, ErrTopologyConflict) {
			t.Fatalf("substituted member evidence accepted: %v", err)
		}
	}
	results[2].cut.Runtime = results[1].cut.Runtime
	if _, err := mergeChildPlanObservations(request, results); !errors.Is(err, ErrPlanObservation) {
		t.Fatalf("duplicate physical member accepted: %v", err)
	}
}
