package splitcontroller

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rangesplit"
)

type localObservationOwnerStub struct {
	observation raftservice.ReplicaObservation
}

type localChildObservationStub struct{ child ChildObservation }

func (stub localChildObservationStub) ObserveLocalSplitChild(context.Context, PlanObservationRequest, uint64) (*ChildObservation, error) {
	return cloneChildPlanRuntime(&stub.child), nil
}

func TestLocalChildReadinessComesFromExactLiveOwnerCut(t *testing.T) {
	request, state, _ := networkPlanObservationFixture(t)
	request.Child = 1
	request.RequestDigest = planObservationRequestDigest(request)
	serving := servingForPlanObservation(request, 1, state.Applied)
	runtimeRoot := filepath.Join(t.TempDir(), "split-runtime")
	if err := os.Mkdir(runtimeRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	registry, err := OpenRuntimeStoreRegistry(runtimeRoot, [32]byte{1}, 2, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = registry.Close() })
	ownerCut := raftservice.ReplicaObservation{Identity: serving.Identity, State: state, Status: serving.Status}
	provider, err := NewLocalPlanObservationProvider(localObservationOwnerStub{ownerCut}, []LocalObservationGroup{{
		Identity: serving.Identity, Command: request.Command, Registry: registry,
		Children: localChildObservationStub{ChildObservation{Child: 1, Phase: ChildPhaseRuntimeAdopted, RuntimeIdentity: serving.Identity}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	got, err := provider.ObserveSplitChild(t.Context(), request, 1)
	if err != nil || got.Runtime == nil || len(got.Runtime.ReadyReplicas) != 1 || got.Runtime.ReadyReplicas[0].Identity != serving.Identity || got.Runtime.ReadyReplicas[0].Status.Applied != state.Applied {
		t.Fatalf("member-local readiness missing: %v", err)
	}
	for _, mutate := range []func(*raftservice.ReplicaObservation){
		func(o *raftservice.ReplicaObservation) { o.Identity.StoreID[0]++ },
		func(o *raftservice.ReplicaObservation) { o.Identity.NodeIncarnation++ },
		func(o *raftservice.ReplicaObservation) { o.State.Binding.OwnershipEpoch++ },
		func(o *raftservice.ReplicaObservation) { o.Status.Applied++ },
	} {
		forged := ownerCut
		mutate(&forged)
		provider.owners = localObservationOwnerStub{forged}
		if _, err := provider.ObserveSplitChild(t.Context(), request, 1); err == nil {
			t.Fatal("substituted local owner cut counted as a ready member")
		}
	}
	ownerCut.Status.LeaderID = 0
	provider.owners = localObservationOwnerStub{ownerCut}
	got, err = provider.ObserveSplitChild(t.Context(), request, 1)
	if err != nil || got.Runtime == nil || len(got.Runtime.ReadyReplicas) != 0 {
		t.Fatalf("leaderless member counted as ready: %v", err)
	}
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
	// The source commits its narrowed ownership before the catalog CAS. Both
	// a still-parent catalog and a reopened source must observe that exact cut.
	transition := PlanObservationSourceTransition{From: request.Command, To: request.Command,
		Range: distribution.KeyRange{End: distribution.KeyspaceEnd{Point: distribution.KeyspacePoint{128}}}}
	transition.To.OwnershipEpoch++
	transition.To.RoutingVersion++
	transition.To.RouteGeneration++
	actual.Binding.OwnershipEpoch = transition.To.OwnershipEpoch
	actual.Binding.RoutingVersion = transition.To.RoutingVersion
	actual.Binding.RouteGeneration = transition.To.RouteGeneration
	actual.Binding.OwnedRange = transition.Range
	provider.owners = localObservationOwnerStub{raftservice.ReplicaObservation{State: actual, Status: serving.Status}}
	if _, err := provider.ObserveSplitSource(t.Context(), request, 1); err == nil {
		t.Fatal("an unnamed ownership successor was accepted")
	}
	request.SourceTransition = &transition
	for _, startup := range []raftservice.CommandFence{transition.From, transition.To} {
		provider.groups[0].Command = startup
		for _, catalogCommand := range []raftservice.CommandFence{transition.From, transition.To} {
			request.Command = catalogCommand
			request.RequestDigest = planObservationRequestDigest(request)
			got, err := provider.ObserveSplitSource(t.Context(), request, 1)
			if err != nil || got.Serving.Command != transition.To || got.State.Binding != actual.Binding {
				t.Fatalf("sealed source observation startup=%+v catalog=%+v got=%+v err=%v", startup, catalogCommand, got, err)
			}
		}
	}
}

func TestLocalPlanObservationProviderRegistersOnlyExactBoundedGroups(t *testing.T) {
	request, _, _ := networkPlanObservationFixture(t)
	runtimeRoot := filepath.Join(t.TempDir(), "split-runtime")
	if err := os.Mkdir(runtimeRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	registry, err := OpenRuntimeStoreRegistry(runtimeRoot, [32]byte{1}, 2, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = registry.Close() })
	identity := raftmember.RuntimeIdentity{
		Group: request.Group, MemberID: 1, AllocationGeneration: uint64(request.Allocation),
		StoreID: [16]byte{1}, NodeIncarnation: 1,
	}
	provider, err := NewLocalPlanObservationProvider(
		localObservationOwnerStub{}, []LocalObservationGroup{{
			Identity: identity, Command: request.Command, Registry: registry,
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	child := LocalObservationGroup{
		Identity: identity, Command: request.Command, Registry: registry,
	}
	child.Identity.Group.GroupID[0]++
	child.Identity.StoreID[0]++
	if err = provider.RegisterGroups([]LocalObservationGroup{child}); err != nil {
		t.Fatal(err)
	}
	if err = provider.RegisterGroups([]LocalObservationGroup{child}); err != nil {
		t.Fatalf("exact registration is not idempotent: %v", err)
	}
	substitution := child
	substitution.Identity.MemberID++
	if err = provider.RegisterGroups([]LocalObservationGroup{substitution}); err == nil {
		t.Fatal("accepted identity substitution for registered group")
	}
	childRequest := request
	childRequest.Group = child.Identity.Group
	childRequest.RequestDigest = planObservationRequestDigest(childRequest)
	if resolved, ok := provider.resolve(childRequest, child.Identity.MemberID); !ok || resolved.Identity != child.Identity {
		t.Fatalf("dynamic child group unavailable: resolved=%+v ok=%v", resolved, ok)
	}
}
