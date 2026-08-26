package distribution

// The router is where domains, the mapper contract, admission, and the immutable
// manifest meet, so this file exercises the whole classification pipeline: a
// contradiction short-circuits before any mapper work, an unknown ordinal never
// narrows, dedup happens before the Cartesian product, a shorter leading prefix
// widens rather than skips ahead, an all-active-shards result is scatter even
// from finite exact values, the candidate and target limits raise their exact
// typed errors at the boundary, all three admission modes obey the truth table,
// and leader-only targets carry the manifest's shard id, endpoint, ownership
// epoch, and role. The instrumentation is a programmable Mapper: it records the
// combinations the router hands it (cloning the reused buffer) so call counts
// and prefix lengths become assertable, and it lets a test force exact
// destinations to drive every branch deterministically.

import (
	"errors"
	"fmt"
	"slices"
	"testing"
)

// programMapper is a fully programmable, self-instrumenting test Mapper. It
// records every Admits/MapPrefix call and the (defensively cloned) combinations
// it was handed, and it defers destination and admission decisions to optional
// closures so a test can both count expansion and force exact geometry.
type programMapper struct {
	arity    int
	prefixes PrefixSet
	ver      MapperVersion
	mapFn    func(values []Scalar) (DestinationSet, error)
	admitFn  func(prefixLen int, values []Scalar) error

	admitCalls int
	mapCalls   int
	combos     [][]Scalar
}

func (m *programMapper) Arity() int                   { return m.arity }
func (m *programMapper) SupportedPrefixes() PrefixSet { return m.prefixes }
func (m *programMapper) Version() MapperVersion       { return m.ver }

func (m *programMapper) Admits(prefixLen int, values []Scalar) error {
	m.admitCalls++
	if m.admitFn != nil {
		return m.admitFn(prefixLen, values)
	}
	return nil
}

func (m *programMapper) MapPrefix(values []Scalar) (DestinationSet, error) {
	m.mapCalls++
	m.combos = append(m.combos, slices.Clone(values)) // the router reuses its combo buffer
	if m.mapFn != nil {
		return m.mapFn(values)
	}
	return DestinationSet{}, nil
}

// spyMapper wraps a real mapper, delegating behavior while recording calls.
func spyMapper(delegate Mapper) *programMapper {
	return &programMapper{
		arity:    delegate.Arity(),
		prefixes: delegate.SupportedPrefixes(),
		ver:      delegate.Version(),
		mapFn:    delegate.MapPrefix,
		admitFn:  delegate.Admits,
	}
}

// fixedPoint is a mapFn returning one fixed keyspace point for any input.
func fixedPoint(p KeyspacePoint) func([]Scalar) (DestinationSet, error) {
	return func([]Scalar) (DestinationSet, error) {
		return DestinationSet{Points: []KeyspacePoint{p}}, nil
	}
}

// fixedRange is a mapFn returning one fixed keyspace range for any input.
func fixedRange(r KeyRange) func([]Scalar) (DestinationSet, error) {
	return func([]Scalar) (DestinationSet, error) {
		return DestinationSet{Ranges: []KeyRange{r}}, nil
	}
}

// finite builds a finite ValueDomain over the given values verbatim, without the
// deduplication FiniteDomain performs, so tests can feed the router raw
// duplicates and observe that it deduplicates before the product.
func finite(values ...Scalar) ValueDomain {
	return ValueDomain{Kind: DomainFinite, Values: values}
}

// kr and krMax build concrete and Max-ended keyspace ranges on the uint64 axis.
func kr(start, end uint64) KeyRange { return KeyRange{Start: pt(start), End: kend(end)} }
func krMax(start uint64) KeyRange   { return KeyRange{Start: pt(start), End: maxKeyEnd} }

// mustManifest constructs a manifest from explicit shards or fails the test.
func mustManifest(tb testing.TB, shards []Shard) *Manifest {
	tb.Helper()
	m, err := NewManifest("test", 1, shards)
	if err != nil {
		tb.Fatalf("NewManifest: %v", err)
	}
	return m
}

// targetIDs returns a route's target shard ids in order.
func targetIDs(r Route) []ShardID {
	out := make([]ShardID, len(r.Targets))
	for i, t := range r.Targets {
		out[i] = t.Shard
	}
	return out
}

// isShardValueError reports whether err matches ErrInvalidShardValue and is a
// *ShardValueError.
func isShardValueError(err error) bool {
	var sve *ShardValueError
	return errors.Is(err, ErrInvalidShardValue) && errors.As(err, &sve)
}

// admName renders an admission mode for sub-test names.
func admName(a RouteAdmission) string {
	switch a {
	case AdmissionTargetedOnly:
		return "targeted-only"
	case AdmissionAllowScatter:
		return "allow-scatter"
	case AdmissionAllowScatterOnOverflow:
		return "allow-scatter-on-overflow"
	default:
		return "unknown"
	}
}

// allModes is the full admission set the truth table ranges over.
var allModes = []RouteAdmission{AdmissionTargetedOnly, AdmissionAllowScatter, AdmissionAllowScatterOnOverflow}

// TestRouteContradictionShortCircuits asserts an empty ordinal collapses to
// RouteEmpty with no mapper work under every admission mode, whether the empty
// ordinal is bound directly, derived from contradictory predicates, or sits past
// the mapper's arity — and that the empty route still carries the manifest's
// distribution and routing version.
func TestRouteContradictionShortCircuits(t *testing.T) {
	man := manifestFromBounds(t, hb(0x80))

	newSpy := func() *programMapper {
		return &programMapper{arity: 1, prefixes: NewPrefixSet(1), ver: 1, mapFn: fixedPoint(pt(hb(0x10)))}
	}

	consCases := map[string]BoundConstraints{
		"direct empty ordinal":     {EmptyDomain()},
		"empty ordinal past arity": {finite(mustNumber("5")), EmptyDomain()},
	}
	for name, cons := range consCases {
		for _, adm := range allModes {
			spy := newSpy()
			route, err := NewRouter().Route(cons, spy, man, NewRoutePolicy(adm, RouteLimits{}))
			if err != nil {
				t.Fatalf("%s/%s: unexpected error %v", name, admName(adm), err)
			}
			if route.Kind != RouteEmpty || len(route.Targets) != 0 {
				t.Fatalf("%s/%s: route = %+v, want RouteEmpty with no targets", name, admName(adm), route)
			}
			if route.Distribution != man.Distribution() || route.RoutingVersion != man.Version() {
				t.Fatalf("%s/%s: empty route dropped manifest identity: %q/%d", name, admName(adm), route.Distribution, route.RoutingVersion)
			}
			if spy.mapCalls != 0 || spy.admitCalls != 0 {
				t.Fatalf("%s/%s: mapper invoked on a contradiction (admits=%d maps=%d)", name, admName(adm), spy.admitCalls, spy.mapCalls)
			}
		}
	}

	// A contradiction derived from the constraint layer behaves identically, and
	// still does no mapper work under the fail-closed default admission.
	b := NewConstraintBuilder()
	must(t, b.AddEquality(mustNumber("5")))
	must(t, b.AddMembership([]Scalar{mustNumber("6"), mustNumber("7")}))
	dom := b.Domain()
	if dom.Kind != DomainEmpty {
		t.Fatalf("derived domain kind = %v, want DomainEmpty", dom.Kind)
	}
	spy := newSpy()
	route, err := NewRouter().Route(BoundConstraints{dom}, spy, man, NewRoutePolicy(AdmissionTargetedOnly, RouteLimits{}))
	if err != nil || route.Kind != RouteEmpty {
		t.Fatalf("derived contradiction: route=%+v err=%v, want RouteEmpty", route, err)
	}
	if spy.mapCalls != 0 {
		t.Fatalf("derived contradiction invoked the mapper %d times", spy.mapCalls)
	}
}

// TestRouteUnknownLeadingScatters pins that a leading unknown ordinal produces an
// unknown route: rejected under targeted-only, and otherwise a scatter over every
// active shard (never a narrower subset), with no mapper work in either case.
func TestRouteUnknownLeadingScatters(t *testing.T) {
	man := manifestFromBounds(t, hb(0x55), hb(0xaa)) // 3 shards
	newSpy := func() *programMapper {
		return &programMapper{arity: 1, prefixes: NewPrefixSet(1), ver: 1, mapFn: fixedPoint(pt(hb(0x10)))}
	}
	cons := BoundConstraints{UnknownDomain()}

	spy := newSpy()
	if _, err := NewRouter().Route(cons, spy, man, NewRoutePolicy(AdmissionTargetedOnly, RouteLimits{})); !errors.Is(err, ErrScatterRejected) {
		t.Fatalf("unknown route under targeted-only: err = %v, want ErrScatterRejected", err)
	}
	if spy.mapCalls != 0 {
		t.Fatalf("unknown route did mapper work (%d calls); it must not touch the mapper", spy.mapCalls)
	}

	for _, adm := range []RouteAdmission{AdmissionAllowScatter, AdmissionAllowScatterOnOverflow} {
		spy = newSpy()
		route, err := NewRouter().Route(cons, spy, man, NewRoutePolicy(adm, RouteLimits{}))
		if err != nil {
			t.Fatalf("%s: unexpected error %v", admName(adm), err)
		}
		if route.Kind != RouteScatter {
			t.Fatalf("%s: kind = %v, want RouteScatter", admName(adm), route.Kind)
		}
		if len(route.Targets) != man.ShardCount() {
			t.Fatalf("%s: unknown route selected %d of %d shards; it must never narrow to a subset",
				admName(adm), len(route.Targets), man.ShardCount())
		}
		if spy.mapCalls != 0 {
			t.Fatalf("%s: unknown route did mapper work (%d calls)", admName(adm), spy.mapCalls)
		}
	}
}

// TestRouteUnknownNonLeadingDoesNotSkipAhead pins that a component after an
// unknown ordinal is never used: with [finite, unknown, finite] the router maps
// only the length-1 leading prefix, so the mapper only ever sees length-1
// combinations and the trailing finite value cannot let routing skip ahead.
func TestRouteUnknownNonLeadingDoesNotSkipAhead(t *testing.T) {
	man := manifestFromBounds(t, evenlySpacedBounds(4)...)
	spy := spyMapper(NewNativeMapper(3))
	cons := BoundConstraints{finite(mustNumber("7")), UnknownDomain(), finite(mustNumber("9"))}

	if _, err := NewRouter().Route(cons, spy, man, NewRoutePolicy(AdmissionAllowScatter, RouteLimits{})); err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if spy.mapCalls == 0 {
		t.Fatal("mapper was never called; the leading finite prefix should still map")
	}
	for i, c := range spy.combos {
		if len(c) != 1 {
			t.Fatalf("combo %d has length %d; only the length-1 leading prefix may map (a component after an unknown must not be used)", i, len(c))
		}
	}
}

// TestRouteDedupBeforeProduct asserts the router deduplicates each finite domain
// by canonical bytes before forming the Cartesian product: three equal spellings
// collapse to one candidate, and duplicate-bearing domains 2x2 expand to 1x2
// candidates rather than 4.
func TestRouteDedupBeforeProduct(t *testing.T) {
	man := manifestFromBounds(t, hb(0x80))
	n5, n50, n5e0 := mustNumber("5"), mustNumber("5.0"), mustNumber("5e0")

	single := &programMapper{arity: 1, prefixes: NewPrefixSet(1), ver: 1, mapFn: fixedPoint(pt(hb(0x10)))}
	if _, err := NewRouter().Route(BoundConstraints{finite(n5, n50, n5e0)}, single, man, NewRoutePolicy(AdmissionAllowScatter, RouteLimits{})); err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if single.mapCalls != 1 {
		t.Fatalf("single-column: mapper called %d times, want 1 (three equal spellings dedup to one candidate)", single.mapCalls)
	}

	multi := &programMapper{arity: 2, prefixes: NewPrefixSet(1, 2), ver: 1, mapFn: fixedPoint(pt(hb(0x10)))}
	cons := BoundConstraints{finite(n5, n50), finite(mustNumber("1"), mustNumber("2"))}
	if _, err := NewRouter().Route(cons, multi, man, NewRoutePolicy(AdmissionAllowScatter, RouteLimits{})); err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if multi.mapCalls != 2 {
		t.Fatalf("multi-column: mapper called %d times, want 2 (deduped 1x2); a post-product dedup would show the raw 2x2 = 4", multi.mapCalls)
	}
	for i, c := range multi.combos {
		if len(c) != 2 {
			t.Fatalf("combo %d has length %d, want 2", i, len(c))
		}
	}
}

// TestRoutePrefixWidensSubset pins that a shorter supported leading prefix maps
// to a wider subset than the full key: the full key resolves to one shard, and
// with its trailing component unknown the same leading value maps to a range
// covering a superset of shards — proving a prefix widens and never skips to a
// wrong narrower target.
func TestRoutePrefixWidensSubset(t *testing.T) {
	man := mustManifest(t, []Shard{
		{ID: "s0", AllocationGeneration: 1, Range: kr(0, hb(0x40)), Leaders: leader("0")},
		{ID: "s1", AllocationGeneration: 2, Range: kr(hb(0x40), hb(0x80)), Leaders: leader("1")},
		{ID: "s2", AllocationGeneration: 3, Range: krMax(hb(0x80)), Leaders: leader("2")},
	})
	fullPoint := pt(hb(0x50))                                        // inside s1
	prefixSpan := KeyRange{Start: pt(hb(0x50)), End: kend(hb(0xc0))} // covers s1 and s2, and contains fullPoint

	m := &programMapper{arity: 2, prefixes: NewPrefixSet(1, 2), ver: 1,
		mapFn: func(v []Scalar) (DestinationSet, error) {
			if len(v) == 2 {
				return DestinationSet{Points: []KeyspacePoint{fullPoint}}, nil
			}
			return DestinationSet{Ranges: []KeyRange{prefixSpan}}, nil
		}}
	policy := NewRoutePolicy(AdmissionAllowScatter, RouteLimits{})

	full, err := NewRouter().Route(BoundConstraints{finite(mustNumber("1")), finite(mustNumber("2"))}, m, man, policy)
	must(t, err)
	if full.Kind != RouteSingle || !slices.Equal(targetIDs(full), []ShardID{"s1"}) {
		t.Fatalf("full key: kind=%v targets=%v, want RouteSingle [s1]", full.Kind, targetIDs(full))
	}

	prefix, err := NewRouter().Route(BoundConstraints{finite(mustNumber("1")), UnknownDomain()}, m, man, policy)
	must(t, err)
	if prefix.Kind != RouteTargeted {
		t.Fatalf("prefix route: kind = %v, want RouteTargeted", prefix.Kind)
	}
	if got := targetIDs(prefix); !slices.Equal(got, []ShardID{"s1", "s2"}) {
		t.Fatalf("prefix route targets = %v, want [s1 s2]", got)
	}
	for _, tg := range full.Targets {
		if !slices.Contains(targetIDs(prefix), tg.Shard) {
			t.Fatalf("prefix route dropped full-key shard %q; a prefix must widen, never narrow", tg.Shard)
		}
	}
}

// TestRouteAllShardsClassifiedScatter asserts a route physically covering every
// active shard is classified RouteScatter even when a single finite exact value
// derived it: rejected under targeted-only, and a full-fan-out scatter otherwise.
func TestRouteAllShardsClassifiedScatter(t *testing.T) {
	man := manifestFromBounds(t, hb(0x55), hb(0xaa)) // 3 shards
	m := &programMapper{arity: 1, prefixes: NewPrefixSet(1), ver: 1, mapFn: fixedRange(KeyRange{Start: pt(0), End: maxKeyEnd})}
	cons := BoundConstraints{finite(mustNumber("42"))}

	if _, err := NewRouter().Route(cons, m, man, NewRoutePolicy(AdmissionTargetedOnly, RouteLimits{})); !errors.Is(err, ErrScatterRejected) {
		t.Fatalf("all-shard route under targeted-only: err = %v, want ErrScatterRejected", err)
	}
	route, err := NewRouter().Route(cons, m, man, NewRoutePolicy(AdmissionAllowScatter, RouteLimits{}))
	must(t, err)
	if route.Kind != RouteScatter {
		t.Fatalf("kind = %v, want RouteScatter (covering every active shard is scatter regardless of a finite derivation)", route.Kind)
	}
	if len(route.Targets) != man.ShardCount() {
		t.Fatalf("targets = %d, want all %d shards", len(route.Targets), man.ShardCount())
	}
}

// TestRouteCandidateLimit asserts the candidate-mapping limit trips before any
// expansion, with the exact *ExpansionLimitError, under both non-degrading modes.
func TestRouteCandidateLimit(t *testing.T) {
	man := manifestFromBounds(t, hb(0x80))
	m := &programMapper{arity: 2, prefixes: NewPrefixSet(1, 2), ver: 1, mapFn: fixedPoint(pt(hb(0x10)))}
	cons := BoundConstraints{
		finite(mustNumber("1"), mustNumber("2"), mustNumber("3")),
		finite(mustNumber("4"), mustNumber("5"), mustNumber("6")),
	} // 3 x 3 = 9 candidates
	limits := RouteLimits{MaxCandidateMappings: 8, MaxTargetShards: 64}

	for _, adm := range []RouteAdmission{AdmissionTargetedOnly, AdmissionAllowScatter} {
		_, err := NewRouter().Route(cons, m, man, NewRoutePolicy(adm, limits))
		if !errors.Is(err, ErrRouteExpansionLimit) {
			t.Fatalf("%s: err = %v, want ErrRouteExpansionLimit", admName(adm), err)
		}
		var ee *ExpansionLimitError
		if !errors.As(err, &ee) || ee.Limit != 8 {
			t.Fatalf("%s: want *ExpansionLimitError{Limit:8}, got %v", admName(adm), err)
		}
	}
	if m.mapCalls != 0 {
		t.Fatalf("candidate overflow expanded %d combos; the limit must trip before expansion", m.mapCalls)
	}
}

// TestRouteTargetLimit asserts the target-shard limit raises the exact
// *TargetLimitError (carrying both the limit and the selected count) for a
// selection above the limit but below full fan-out, under both non-degrading modes.
func TestRouteTargetLimit(t *testing.T) {
	man := manifestFromBounds(t, hb(0x20), hb(0x40), hb(0x60), hb(0x80))                                        // 5 shards s0..s4
	m := &programMapper{arity: 1, prefixes: NewPrefixSet(1), ver: 1, mapFn: fixedRange(kr(hb(0x20), hb(0x80)))} // covers s1,s2,s3
	cons := BoundConstraints{finite(mustNumber("1"))}
	limits := RouteLimits{MaxCandidateMappings: 256, MaxTargetShards: 2}

	for _, adm := range []RouteAdmission{AdmissionTargetedOnly, AdmissionAllowScatter} {
		_, err := NewRouter().Route(cons, m, man, NewRoutePolicy(adm, limits))
		if !errors.Is(err, ErrTargetShardLimit) {
			t.Fatalf("%s: err = %v, want ErrTargetShardLimit", admName(adm), err)
		}
		var te *TargetLimitError
		if !errors.As(err, &te) || te.Limit != 2 || te.Count != 3 {
			t.Fatalf("%s: want *TargetLimitError{Limit:2, Count:3}, got %v", admName(adm), err)
		}
	}
}

// TestRouteAdmissionTruthTable exercises every category of admission decision
// across all three modes: an unknown or all-shard scatter is rejected under
// targeted-only and permitted otherwise; a candidate or target overflow raises
// its typed error under targeted-only and allow-scatter, and degrades to a
// full-fan-out scatter under allow-scatter-on-overflow.
func TestRouteAdmissionTruthTable(t *testing.T) {
	man3 := manifestFromBounds(t, hb(0x55), hb(0xaa))                     // 3 shards
	man5 := manifestFromBounds(t, hb(0x20), hb(0x40), hb(0x60), hb(0x80)) // 5 shards

	oneFinite := BoundConstraints{finite(mustNumber("1"))}
	nineCandidates := BoundConstraints{
		finite(mustNumber("1"), mustNumber("2"), mustNumber("3")),
		finite(mustNumber("4"), mustNumber("5"), mustNumber("6")),
	}

	// wantErr maps a mode to the sentinel it must return; a mode absent from
	// wantErr must instead succeed as a full-fan-out scatter over man's shards.
	scenarios := []struct {
		name    string
		cons    BoundConstraints
		newMap  func() *programMapper
		man     *Manifest
		limits  RouteLimits
		wantErr map[RouteAdmission]error
	}{
		{
			name:    "unknown scatter",
			cons:    BoundConstraints{UnknownDomain()},
			newMap:  func() *programMapper { return &programMapper{arity: 1, prefixes: NewPrefixSet(1), ver: 1} },
			man:     man3,
			wantErr: map[RouteAdmission]error{AdmissionTargetedOnly: ErrScatterRejected},
		},
		{
			name: "all-shard scatter from finite",
			cons: oneFinite,
			newMap: func() *programMapper {
				return &programMapper{arity: 1, prefixes: NewPrefixSet(1), ver: 1, mapFn: fixedRange(KeyRange{Start: pt(0), End: maxKeyEnd})}
			},
			man:     man3,
			wantErr: map[RouteAdmission]error{AdmissionTargetedOnly: ErrScatterRejected},
		},
		{
			name: "candidate overflow",
			cons: nineCandidates,
			newMap: func() *programMapper {
				return &programMapper{arity: 2, prefixes: NewPrefixSet(1, 2), ver: 1, mapFn: fixedPoint(pt(hb(0x10)))}
			},
			man:     man3,
			limits:  RouteLimits{MaxCandidateMappings: 8, MaxTargetShards: 64},
			wantErr: map[RouteAdmission]error{AdmissionTargetedOnly: ErrRouteExpansionLimit, AdmissionAllowScatter: ErrRouteExpansionLimit},
		},
		{
			name: "target overflow",
			cons: oneFinite,
			newMap: func() *programMapper {
				return &programMapper{arity: 1, prefixes: NewPrefixSet(1), ver: 1, mapFn: fixedRange(kr(hb(0x20), hb(0x80)))}
			},
			man:     man5,
			limits:  RouteLimits{MaxCandidateMappings: 256, MaxTargetShards: 2},
			wantErr: map[RouteAdmission]error{AdmissionTargetedOnly: ErrTargetShardLimit, AdmissionAllowScatter: ErrTargetShardLimit},
		},
	}

	for _, sc := range scenarios {
		for _, adm := range allModes {
			t.Run(fmt.Sprintf("%s/%s", sc.name, admName(adm)), func(t *testing.T) {
				route, err := NewRouter().Route(sc.cons, sc.newMap(), sc.man, NewRoutePolicy(adm, sc.limits))
				if want, ok := sc.wantErr[adm]; ok {
					if !errors.Is(err, want) {
						t.Fatalf("err = %v, want match %v", err, want)
					}
					return
				}
				// No error expected: a full-fan-out scatter over every shard.
				if err != nil {
					t.Fatalf("unexpected error %v", err)
				}
				if route.Kind != RouteScatter {
					t.Fatalf("kind = %v, want RouteScatter", route.Kind)
				}
				if len(route.Targets) != sc.man.ShardCount() {
					t.Fatalf("targets = %d, want all %d shards", len(route.Targets), sc.man.ShardCount())
				}
			})
		}
	}
}

// TestRouteClassificationAndTargets pins RouteEmpty/RouteSingle/RouteTargeted
// classification and the exact target records: leader-only selection takes the
// first of a shard's leaders, each target carries that shard's id and ownership
// epoch with RoleLeader, targets come back in keyspace order, and the route
// carries the manifest's distribution and routing version.
func TestRouteClassificationAndTargets(t *testing.T) {
	man := mustManifest(t, []Shard{
		{ID: "s0", AllocationGeneration: 1, Range: kr(0, hb(0x80)), Leaders: []EndpointID{"ep-0a", "ep-0b"}, Epoch: 11},
		{ID: "s1", AllocationGeneration: 2, Range: kr(hb(0x80), hb(0xc0)), Leaders: []EndpointID{"ep-1"}, Epoch: 22},
		{ID: "s2", AllocationGeneration: 3, Range: krMax(hb(0xc0)), Leaders: []EndpointID{"ep-2"}, Epoch: 33},
	})
	policy := NewRoutePolicy(AdmissionAllowScatter, RouteLimits{})

	// RouteEmpty carries manifest identity but no targets.
	empty, err := NewRouter().Route(BoundConstraints{EmptyDomain()}, &programMapper{arity: 1, prefixes: NewPrefixSet(1), ver: 1}, man, policy)
	must(t, err)
	if empty.Kind != RouteEmpty || len(empty.Targets) != 0 {
		t.Fatalf("empty: route = %+v, want RouteEmpty with no targets", empty)
	}
	if empty.Distribution != "test" || empty.RoutingVersion != 1 {
		t.Fatalf("empty route identity = %q/%d, want test/1", empty.Distribution, empty.RoutingVersion)
	}

	// RouteSingle: a point in s0 selects the first of its two leaders, epoch 11.
	single, err := NewRouter().Route(BoundConstraints{finite(mustNumber("1"))}, &programMapper{arity: 1, prefixes: NewPrefixSet(1), ver: 1, mapFn: fixedPoint(pt(hb(0x10)))}, man, policy)
	must(t, err)
	if single.Kind != RouteSingle || len(single.Targets) != 1 {
		t.Fatalf("single: route = %+v, want one RouteSingle target", single)
	}
	if got, want := single.Targets[0], (Target{Shard: "s0", AllocationGeneration: 1, ManifestOrdinal: 0, Endpoint: "ep-0a", OwnershipEpoch: 11, Role: RoleLeader}); got != want {
		t.Fatalf("single target = %+v, want %+v", got, want)
	}

	// RouteTargeted: a range covering s0 and s1 (2 of 3), in keyspace order.
	targeted, err := NewRouter().Route(BoundConstraints{finite(mustNumber("1"))}, &programMapper{arity: 1, prefixes: NewPrefixSet(1), ver: 1, mapFn: fixedRange(kr(0, hb(0xc0)))}, man, policy)
	must(t, err)
	if targeted.Kind != RouteTargeted || len(targeted.Targets) != 2 {
		t.Fatalf("targeted: route = %+v, want two RouteTargeted targets", targeted)
	}
	want := []Target{
		{Shard: "s0", AllocationGeneration: 1, ManifestOrdinal: 0, Endpoint: "ep-0a", OwnershipEpoch: 11, Role: RoleLeader},
		{Shard: "s1", AllocationGeneration: 2, ManifestOrdinal: 1, Endpoint: "ep-1", OwnershipEpoch: 22, Role: RoleLeader},
	}
	for i := range want {
		if targeted.Targets[i] != want[i] {
			t.Fatalf("targeted target %d = %+v, want %+v", i, targeted.Targets[i], want[i])
		}
	}
	if targeted.Distribution != "test" || targeted.RoutingVersion != 1 {
		t.Fatalf("targeted route identity = %q/%d, want test/1", targeted.Distribution, targeted.RoutingVersion)
	}
}

// TestRouteMapperErrorsPropagate asserts a mapper that refuses a value or rejects
// a prefix surfaces its typed error through the router unchanged, exercising the
// per-value Admits rejection branch a value-agnostic native mapper cannot reach.
func TestRouteMapperErrorsPropagate(t *testing.T) {
	man := manifestFromBounds(t, hb(0x80))
	cons := BoundConstraints{finite(mustNumber("1"))}
	policy := NewRoutePolicy(AdmissionAllowScatter, RouteLimits{})

	refuseValue := &programMapper{arity: 1, prefixes: NewPrefixSet(1), ver: 1,
		admitFn: func(int, []Scalar) error { return &ShardValueError{Reason: "mapper refuses this value type"} }}
	if _, err := NewRouter().Route(cons, refuseValue, man, policy); !errors.Is(err, ErrInvalidShardValue) {
		t.Fatalf("value refusal: err = %v, want ErrInvalidShardValue", err)
	}

	refuseMap := &programMapper{arity: 1, prefixes: NewPrefixSet(1), ver: 1,
		mapFn: func([]Scalar) (DestinationSet, error) { return DestinationSet{}, &MapperError{Reason: "boom"} }}
	if _, err := NewRouter().Route(cons, refuseMap, man, policy); !errors.Is(err, ErrUnsupportedMapper) {
		t.Fatalf("map refusal: err = %v, want ErrUnsupportedMapper", err)
	}
}

// TestRouteNilMapperAndManifest asserts a nil mapper or nil manifest fails closed
// with a typed *MapperError once past the empty-ordinal short-circuit.
func TestRouteNilMapperAndManifest(t *testing.T) {
	man := manifestFromBounds(t, hb(0x80))
	cons := BoundConstraints{finite(mustNumber("1"))}
	policy := NewRoutePolicy(AdmissionAllowScatter, RouteLimits{})

	if _, err := NewRouter().Route(cons, nil, man, policy); !errors.Is(err, ErrUnsupportedMapper) {
		t.Fatalf("nil mapper: err = %v, want ErrUnsupportedMapper", err)
	}
	if _, err := NewRouter().Route(cons, NewNativeMapper(1), nil, policy); !errors.Is(err, ErrUnsupportedMapper) {
		t.Fatalf("nil manifest: err = %v, want ErrUnsupportedMapper", err)
	}
}

// TestRouterReuseIsStable pins the reusable-scratch contract: one Router driven
// through empty, single, targeted, and scatter routes in sequence produces the
// same result each time as a fresh Router, so no per-call buffer leaks state.
func TestRouterReuseIsStable(t *testing.T) {
	man := manifestFromBounds(t, hb(0x40), hb(0x80), hb(0xc0)) // 4 shards
	policy := NewRoutePolicy(AdmissionAllowScatter, RouteLimits{})

	steps := []struct {
		name string
		cons BoundConstraints
		m    Mapper
	}{
		{"empty", BoundConstraints{EmptyDomain()}, &programMapper{arity: 1, prefixes: NewPrefixSet(1), ver: 1}},
		{"single", BoundConstraints{finite(mustNumber("1"))}, &programMapper{arity: 1, prefixes: NewPrefixSet(1), ver: 1, mapFn: fixedPoint(pt(hb(0x10)))}},
		{"targeted", BoundConstraints{finite(mustNumber("1"))}, &programMapper{arity: 1, prefixes: NewPrefixSet(1), ver: 1, mapFn: fixedRange(kr(0, hb(0x80)))}},
		{"scatter", BoundConstraints{finite(mustNumber("1"))}, &programMapper{arity: 1, prefixes: NewPrefixSet(1), ver: 1, mapFn: fixedRange(KeyRange{Start: pt(0), End: maxKeyEnd})}},
	}

	reused := NewRouter()
	// Run the whole sequence twice through the reused router to expose any
	// contamination that only appears after the buffers have been grown.
	for pass := 0; pass < 2; pass++ {
		for _, s := range steps {
			want, err := NewRouter().Route(s.cons, s.m, man, policy) // a fresh router is the reference
			must(t, err)
			got, err := reused.Route(s.cons, s.m, man, policy)
			must(t, err)
			if got.Kind != want.Kind || !slices.Equal(targetIDs(got), targetIDs(want)) {
				t.Fatalf("pass %d step %q: reused router gave {%v %v}, fresh gave {%v %v}",
					pass, s.name, got.Kind, targetIDs(got), want.Kind, targetIDs(want))
			}
		}
	}
}
