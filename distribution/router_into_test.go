package distribution

import "testing"

// contractIntoMapper proves Router consumes the optional MapperInto contract
// without depending on NativeMapper. Each call overwrites the one-point caller
// scratch, which also detects a router that retained mapper scratch across
// Cartesian candidates instead of copying each result immediately.
type contractIntoMapper struct {
	points   []KeyspacePoint
	mapCalls int
	intoCall int
}

func (m *contractIntoMapper) Arity() int                   { return 1 }
func (m *contractIntoMapper) SupportedPrefixes() PrefixSet { return NewPrefixSet(1) }
func (m *contractIntoMapper) Version() MapperVersion       { return 1 }
func (m *contractIntoMapper) Admits(int, []Scalar) error   { return nil }
func (m *contractIntoMapper) MapPrefix([]Scalar) (DestinationSet, error) {
	m.mapCalls++
	return DestinationSet{Points: []KeyspacePoint{m.points[0]}}, nil
}
func (m *contractIntoMapper) MapPrefixInto(
	_ []Scalar,
	pointScratch []KeyspacePoint,
	_ []KeyRange,
) (DestinationSet, error) {
	p := m.points[m.intoCall%len(m.points)]
	m.intoCall++
	pointScratch = append(pointScratch[:0], p)
	return DestinationSet{Points: pointScratch}, nil
}

func TestRouterUsesGenericMapperIntoAndCopiesEachCandidate(t *testing.T) {
	man := manifestFromBounds(t, hb(0x40), hb(0x80))
	mapper := &contractIntoMapper{points: []KeyspacePoint{
		pt(hb(0x10)), pt(hb(0x50)),
	}}
	cons := BoundConstraints{finite(NewString("a"), NewString("b"))}
	policy := NewRoutePolicy(AdmissionAllowScatter, RouteLimits{})
	var targets [2]Target

	route, err := NewRouter().RouteInto(
		cons, mapper, man, policy, targets[:0],
	)
	if err != nil {
		t.Fatal(err)
	}
	if mapper.mapCalls != 0 || mapper.intoCall != 2 {
		t.Fatalf("mapper calls: MapPrefix=%d MapPrefixInto=%d, want 0/2", mapper.mapCalls, mapper.intoCall)
	}
	if route.Kind != RouteTargeted || len(route.Targets) != 2 ||
		route.Targets[0].Shard != "s0" || route.Targets[1].Shard != "s1" {
		t.Fatalf("route = %+v, want targeted s0/s1", route)
	}
	if &route.Targets[0] != &targets[0] {
		t.Fatal("RouteInto did not reuse caller target storage")
	}
}

func TestRouteIntoPreservesGenericMapperFallback(t *testing.T) {
	man := manifestFromBounds(t, hb(0x80))
	mapper := &programMapper{
		arity: 1, prefixes: NewPrefixSet(1), ver: 1,
		mapFn: fixedPoint(pt(hb(0x10))),
	}
	var targets [1]Target
	route, err := NewRouter().RouteInto(
		BoundConstraints{finite(NewString("a"))}, mapper, man,
		NewRoutePolicy(AdmissionTargetedOnly, RouteLimits{}), targets[:0],
	)
	if err != nil {
		t.Fatal(err)
	}
	if mapper.mapCalls != 1 {
		t.Fatalf("generic MapPrefix calls = %d, want 1", mapper.mapCalls)
	}
	if route.Kind != RouteSingle || len(route.Targets) != 1 ||
		&route.Targets[0] != &targets[0] {
		t.Fatalf("RouteInto generic fallback = %+v, want caller-owned single target", route)
	}
}
