package splitcontroller

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rangesplit"
)

type localObservationOwnerStub struct {
	observation raftservice.ReplicaObservation
}

func (stub localObservationOwnerStub) ObserveReplica(
	_ context.Context, _ raftmember.GroupKey, target uint64,
) (raftservice.ReplicaObservation, error) {
	if stub.observation.Status.MemberID != target {
		return raftservice.ReplicaObservation{}, ErrPlanObservation
	}
	return stub.observation, nil
}

func TestLocalPlanObservationProviderReadsDurableBoundedSourceState(t *testing.T) {
	request, expected, _ := networkPlanObservationFixture(t)
	plan, _, _, _ := testPlan(t)
	source := newFlowSource(t, plan)
	if _, err := source.machine.InstallSnapshot(source.bootstrap); err != nil {
		t.Fatal(err)
	}
	// Use the state actually owned by the read snapshot while retaining the
	// request's exact routing identity.
	cut, err := source.machine.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	actual := cut.State()
	if err = cut.Close(); err != nil {
		t.Fatal(err)
	}
	request.Group = raftmember.GroupKey{
		ClusterID:             [16]byte(actual.Binding.ClusterID),
		ClusterIncarnation:    [16]byte(actual.Binding.ClusterIncarnation),
		TopologyRecoveryEpoch: actual.Binding.TopologyRecoveryEpoch,
		ShardIncarnation:      [16]byte(actual.Binding.ShardIncarnation),
		GroupID:               [16]byte(actual.Binding.GroupID),
	}
	request.Command.ReplicaSetVersion = actual.ReplicaSetVersion
	request.Command.ActivePolicyGeneration = actual.Binding.ActivePolicyGeneration
	request.Command.ProtectionEpoch = actual.Binding.ProtectionEpoch
	request.Command.OwnershipEpoch = actual.Binding.OwnershipEpoch
	request.Command.SchemaGeneration = actual.Binding.SchemaGeneration
	request.Command.RoutingVersion = actual.Binding.RoutingVersion
	request.Command.RouteGeneration = actual.Binding.RouteGeneration
	request.RequestDigest = planObservationRequestDigest(request)

	runtimeRoot := filepath.Join(t.TempDir(), "split-runtime")
	if err = os.Mkdir(runtimeRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	manifest := [32]byte{1}
	registry, err := OpenRuntimeStoreRegistry(runtimeRoot, manifest, 2, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = registry.Close() })
	lease, err := registry.Acquire(request.Operation)
	if err != nil {
		t.Fatal(err)
	}
	descriptor := rangesplit.SourceCaptureDescriptor{
		PlanDigest: [32]byte{2}, PlacementDigest: [32]byte{3}, Collection: "docs",
		Base:        rangesplit.ChildArtifactSourceCut{Applied: 1, Term: 1, DataChainDigest: [32]byte{4}, EntryDigest: [32]byte{5}, BaseDigest: [32]byte{6}, RouteGeneration: 1},
		Head:        rangesplit.ChildArtifactSourceCut{Applied: 9, Term: 2, DataChainDigest: [32]byte{7}, EntryDigest: [32]byte{8}, BaseDigest: [32]byte{6}, RouteGeneration: 1},
		Coordinates: rangesplit.TailSourceCoordinates{OwnershipEpoch: 1, RoutingVersion: 1, RouteGeneration: 1},
	}
	raw, err := rangesplit.AppendSourceCaptureDescriptor(nil, descriptor)
	if err != nil {
		t.Fatal(err)
	}
	if err = lease.Persist(RuntimeStateCapture, 0, 1, raw); err != nil {
		t.Fatal(err)
	}
	if err = lease.Release(); err != nil {
		t.Fatal(err)
	}
	serving := raftservice.ServingState{
		Identity: raftmember.RuntimeIdentity{Group: request.Group, MemberID: 1,
			AllocationGeneration: uint64(request.Allocation), StoreID: [16]byte{1}, NodeIncarnation: 1},
		Command: request.Command,
		Status: raftmember.RuntimeStatus{MemberID: 1, LeaderID: 1, Term: 2,
			Commit: actual.Applied, Applied: actual.Applied, CheckpointApplied: actual.Applied},
	}
	provider, err := NewLocalPlanObservationProvider(localObservationOwnerStub{raftservice.ReplicaObservation{
		State: actual, Status: serving.Status,
	}}, []LocalObservationGroup{{
		Identity: serving.Identity, Command: request.Command, Registry: registry,
	}})
	if err != nil {
		t.Fatal(err)
	}
	observed, err := provider.ObserveSplitSource(t.Context(), request, 1)
	if err != nil || observed.RequestDigest != request.RequestDigest ||
		observed.State.Applied != actual.Applied || observed.CaptureHead != descriptor.Head.Applied ||
		observed.Serving.Identity != serving.Identity {
		t.Fatalf("observed=%+v expected fixture=%d err=%v", observed, expected.Applied, err)
	}
	wrong := request
	wrong.Command.RouteGeneration++
	wrong.RequestDigest = planObservationRequestDigest(wrong)
	if _, err = provider.ObserveSplitSource(t.Context(), wrong, 1); err == nil {
		t.Fatal("wrong command reached durable runtime registry")
	}
}
