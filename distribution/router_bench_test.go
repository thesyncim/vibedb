package distribution

// The single-key write/lookup route is the promised hot path, so it gets the
// same allocation-gated benchmark treatment as PR 1a's append APIs: one reused
// Router driving a fully bound single-column key must resolve to one shard
// without allocating beyond the two allocations inherent to the frozen
// contracts — the DestinationSet slice a Mapper.MapPrefix must return, and the
// immutable Route.Targets slice handed back to the caller. All intermediate
// expansion, dedup, and resolution work reuses the Router's scratch buffers.
// TestSingleKeyRouteAllocation pins that ceiling as a regression gate;
// BenchmarkRouteSingleKey reports ns/op and allocs/op for observability.

import "testing"

// benchSinkRoute keeps the benchmarked route live so the call is not elided.
var benchSinkRoute Route

// singleKeyRouteInputs builds the reusable single-key routing inputs: a native
// arity-1 mapper, an 8-shard manifest, one fully bound finite ordinal, and the
// fail-closed default policy (a single-shard route needs no scatter admission).
func singleKeyRouteInputs(tb testing.TB) (*Router, Mapper, *Manifest, BoundConstraints, RoutePolicy) {
	mapper := NewNativeMapper(1)
	man := manifestFromBounds(tb, evenlySpacedBounds(8)...)
	cons := BoundConstraints{{Kind: DomainFinite, Values: []Scalar{mustNumber("42")}}}
	policy := NewRoutePolicy(AdmissionTargetedOnly, RouteLimits{})
	return NewRouter(), mapper, man, cons, policy
}

// BenchmarkRouteSingleKey measures the single-key hot path with a reused Router.
func BenchmarkRouteSingleKey(b *testing.B) {
	r, mapper, man, cons, policy := singleKeyRouteInputs(b)
	if _, err := r.Route(cons, mapper, man, policy); err != nil { // warm the scratch buffers
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		route, err := r.Route(cons, mapper, man, policy)
		if err != nil {
			b.Fatal(err)
		}
		benchSinkRoute = route
	}
}

// TestSingleKeyRouteAllocation gates the single-key route at no more than the two
// contract-bound allocations. Exceeding two means an intermediate buffer stopped
// being reused; the two are the Mapper's returned DestinationSet.Points slice and
// the immutable Route.Targets slice, neither of which the fixed contracts allow
// the router to elide.
func TestSingleKeyRouteAllocation(t *testing.T) {
	r, mapper, man, cons, policy := singleKeyRouteInputs(t)
	if _, err := r.Route(cons, mapper, man, policy); err != nil { // warm the scratch buffers
		t.Fatal(err)
	}
	allocs := testing.AllocsPerRun(1000, func() {
		route, err := r.Route(cons, mapper, man, policy)
		if err != nil {
			t.Fatal(err)
		}
		if route.Kind != RouteSingle {
			t.Fatalf("route kind = %v, want RouteSingle", route.Kind)
		}
	})
	if allocs > 2 {
		t.Fatalf("single-key route allocations = %v, want <= 2 (MapPrefix DestinationSet slice + immutable Route.Targets slice)", allocs)
	}
}
