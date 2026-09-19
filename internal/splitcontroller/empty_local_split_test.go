package splitcontroller

import (
	"testing"

	"github.com/thesyncim/vibedb/internal/raftservice"
)

func TestEmptyLocalSplitObservationRequiresExactHostedRegistration(t *testing.T) {
	request, state, _ := networkPlanObservationFixture(t)
	serving := servingForPlanObservation(request, 1, state.Applied)
	owner := localObservationOwnerStub{raftservice.ReplicaObservation{Identity: serving.Identity, State: state, Status: serving.Status}}
	provider, err := NewEmptyLocalPlanObservationProvider(owner)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.ObserveSplitSource(t.Context(), request, 1); err == nil {
		t.Fatal("empty node observed an unregistered group")
	}
	registry, err := OpenRuntimeStoreRegistry(t.TempDir(), [32]byte{1}, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Close()
	group := LocalObservationGroup{Identity: serving.Identity, Command: request.Command, Registry: registry}
	if err := provider.RegisterGroups([]LocalObservationGroup{group}); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.ObserveSplitSource(t.Context(), request, 1); err != nil {
		t.Fatalf("registered hosted source cannot be observed: %v", err)
	}
	if _, err := provider.ObserveSplitSource(t.Context(), request, 2); err == nil {
		t.Fatal("registration authorized a different local member")
	}
	if err := provider.UnregisterGroups([]LocalObservationGroup{group}); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.ObserveSplitSource(t.Context(), request, 1); err == nil {
		t.Fatal("withdrawn hosted group remained observable")
	}
}

func TestEmptyLocalSplitAdmissionRequiresExactHostedRegistration(t *testing.T) {
	plan, _, _, _ := testPlan(t)
	set, err := NewEmptyLocalPlanAdmissionRegistries([16]byte{0xfe}, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer set.Close()
	if _, err := set.ResolveLocalPlanAdmissionStores(t.Context(), plan); err == nil {
		t.Fatal("empty node resolved an unhosted source")
	}
	registry, err := OpenRuntimeStoreRegistry(t.TempDir(), [32]byte{2}, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Close()
	if err := set.RegisterRetained(RetainedPlanRuntimeRegistry{Distribution: plan.source.Distribution,
		Shard: plan.source.Shard, Allocation: plan.source.AllocationGeneration, Registry: registry}); err != nil {
		t.Fatal(err)
	}
	resolved, err := set.ResolveLocalPlanAdmissionStores(t.Context(), plan)
	if err != nil || len(resolved) != 1 || resolved[0] != registry {
		t.Fatalf("registered source stores=%v err=%v", resolved, err)
	}
}
