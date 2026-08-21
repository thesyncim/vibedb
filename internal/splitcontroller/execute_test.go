package splitcontroller

import (
	"bytes"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/rangesplit"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
)

func TestAppendSourceSealBuildsExactAllocationFreeBinaryTransition(t *testing.T) {
	plan, _, _, _ := testPlan(t)
	state := testSourceState(plan)
	state.ReplicaSetVersion = 2
	buffer := make([]byte, 0, replicatedstate.MaxOwnershipTransitionBytes)
	encoded, err := plan.AppendSourceSeal(buffer[:0], state, 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	view, err := replicatedstate.OpenOwnershipTransition(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if view.ExpectedReplicaSetVersion != state.ReplicaSetVersion ||
		view.SourceMember != 1 || view.TargetMember != 2 ||
		view.ToOwnershipEpoch != state.Binding.OwnershipEpoch+1 ||
		view.ToRoutingVersion != state.Binding.RoutingVersion+1 ||
		view.ToRouteGeneration != state.Binding.RouteGeneration+1 {
		t.Fatalf("seal view = %+v", view)
	}
	if !raceDetectorEnabled {
		if allocations := testing.AllocsPerRun(1_000, func() {
			var appendErr error
			encoded, appendErr = plan.AppendSourceSeal(buffer[:0], state, 1, 2)
			if appendErr != nil {
				panic(appendErr)
			}
		}); allocations != 0 {
			t.Fatalf("warm seal encode allocations = %v, want 0", allocations)
		}
	}

	prefix := []byte("unchanged")
	stale := state
	stale.Binding.RouteGeneration--
	got, err := plan.AppendSourceSeal(prefix, stale, 1, 2)
	if !errors.Is(err, ErrTopologyConflict) || !bytes.Equal(got, prefix) {
		t.Fatalf("stale seal = %q, %v", got, err)
	}
	got, err = plan.AppendSourceSeal(prefix, state, 1, 1)
	if !errors.Is(err, ErrTopologyConflict) || !bytes.Equal(got, prefix) {
		t.Fatalf("same-member seal = %q, %v", got, err)
	}
}

func TestBuildCatalogTransitionRefusesMissingProofs(t *testing.T) {
	plan, current, _, _ := testPlan(t)
	if _, err := plan.BuildCatalogTransition(
		current, rangesplit.CutoverCertificate{}, rangesplit.RetainedPruneCursor{},
	); !errors.Is(err, ErrTopologyConflict) {
		t.Fatalf("missing proof error = %v", err)
	}
}
