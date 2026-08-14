package distribution

// The single-key write/lookup route is the promised hot path, so it gets the
// same allocation-gated benchmark treatment as the append APIs: one reused
// Router driving a fully bound single-column key must resolve to one shard
// without allocating beyond Route's immutable Targets result. NativeMapper's
// MapperInto extension reuses router stack scratch for its DestinationSet. All
// intermediate expansion, dedup, and resolution work reuses Router storage.
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

func BenchmarkRouteSingleKeyInto(b *testing.B) {
	r, mapper, man, cons, policy := singleKeyRouteInputs(b)
	var targets [1]Target
	if _, err := r.RouteInto(
		cons, mapper, man, policy, targets[:0],
	); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		route, err := r.RouteInto(
			cons, mapper, man, policy, targets[:0],
		)
		if err != nil {
			b.Fatal(err)
		}
		benchSinkRoute = route
	}
}

// TestSingleKeyRouteAllocation gates the ordinary immutable-result API at its
// one contract-bound allocation: Route.Targets.
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
	if allocs > 1 {
		t.Fatalf("single-key route allocations = %v, want <= 1 immutable Route.Targets slice", allocs)
	}
}

func TestSingleKeyRouteIntoReusesCallerTargets(t *testing.T) {
	r, mapper, man, cons, policy := singleKeyRouteInputs(t)
	var targets [1]Target
	if _, err := r.RouteInto(
		cons, mapper, man, policy, targets[:0],
	); err != nil {
		t.Fatal(err)
	}
	allocs := testing.AllocsPerRun(1000, func() {
		route, err := r.RouteInto(
			cons, mapper, man, policy, targets[:0],
		)
		if err != nil {
			t.Fatal(err)
		}
		if route.Kind != RouteSingle || len(route.Targets) != 1 ||
			&route.Targets[0] != &targets[0] {
			t.Fatal("RouteInto did not return the caller target storage")
		}
	})
	if allocs != 0 {
		t.Fatalf("single-key RouteInto allocations = %v, want 0", allocs)
	}
}
