package driver

import (
	"errors"
	"fmt"
	"sort"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	sqlast "github.com/thesyncim/vibedb/sql"
)

// threeShardFixture builds a binding whose three-shard manifest boundaries are
// derived from the reference mapper's own routed points, so vals[0], vals[1],
// and vals[2] land on shards 0, 1, and 2 respectively. An IN over two of the
// three values therefore produces a genuine targeted multi-shard route rather
// than a full scatter.
func threeShardFixture(t *testing.T) (binding *placementBinding, router *distribution.Router, vals [3]string) {
	t.Helper()
	mapper := distribution.NewNativeMapper(1)
	type valuePoint struct {
		value string
		point distribution.KeyspacePoint
	}
	points := make([]valuePoint, 0, 96)
	for i := 0; i < 96; i++ {
		v := fmt.Sprintf("tenant-%d", i)
		p, err := mapper.PointFor([]distribution.Scalar{distribution.NewString(v)})
		if err != nil {
			t.Fatalf("PointFor(%q): %v", v, err)
		}
		points = append(points, valuePoint{v, p})
	}
	sort.Slice(points, func(i, j int) bool {
		return distribution.ComparePoints(points[i].point, points[j].point) < 0
	})
	// gapAtOrAfter returns the first index at or after from whose point differs
	// from its predecessor, so a manifest boundary placed there splits real
	// values on both sides.
	gapAtOrAfter := func(from int) int {
		if from < 1 {
			from = 1
		}
		for k := from; k < len(points); k++ {
			if distribution.ComparePoints(points[k-1].point, points[k].point) < 0 {
				return k
			}
		}
		return -1
	}
	a := gapAtOrAfter(len(points) / 3)
	b := gapAtOrAfter(2 * len(points) / 3)
	if a < 1 || b <= a {
		t.Fatalf("probe values did not yield two usable boundaries (a=%d b=%d)", a, b)
	}
	b0 := points[a].point
	b1 := points[b].point
	manifest, err := distribution.NewManifest("d", 1, []distribution.Shard{
		{ID: "s0", Range: distribution.KeyRange{Start: distribution.KeyspacePoint{}, End: distribution.KeyspaceEnd{Point: b0}}, Leaders: []distribution.EndpointID{"e0"}},
		{ID: "s1", Range: distribution.KeyRange{Start: b0, End: distribution.KeyspaceEnd{Point: b1}}, Leaders: []distribution.EndpointID{"e1"}},
		{ID: "s2", Range: distribution.KeyRange{Start: b1, End: distribution.KeyspaceEnd{Max: true}}, Leaders: []distribution.EndpointID{"e2"}},
	})
	if err != nil {
		t.Fatalf("NewManifest three-shard: %v", err)
	}
	binding = newTestBinding(t, []string{"/tenant_id"}, manifest)
	router = distribution.NewRouter()
	// points[0] < b0 (shard 0); points[a] == b0 (shard 1); points[b] == b1 (shard 2).
	vals = [3]string{points[0].value, points[a].value, points[b].value}
	return binding, router, vals
}

// TestSingleShardRouteRejectsTargetedMulti proves a placed write whose predicate
// resolves to more than one but not every shard is a genuine targeted route and
// is still refused before dispatch, distinct from an unknown or full-scatter
// route.
func TestSingleShardRouteRejectsTargetedMulti(t *testing.T) {
	binding, router, vals := threeShardFixture(t)
	where := inExpr("tenant_id", strOp(vals[0]), strOp(vals[1]))

	// The underlying route is targeted (two of three shards), not a full scatter.
	route, err := compileConstraintProgram(binding, where).Route(router, nil)
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	if route.Kind != distribution.RouteTargeted {
		t.Fatalf("route kind = %v, want RouteTargeted", route.Kind)
	}
	if len(route.Targets) != 2 {
		t.Fatalf("targeted route has %d targets, want 2", len(route.Targets))
	}

	// The write preflight refuses it before dispatch.
	if _, err := singleShardRoute(compileConstraintProgram(binding, where), router, nil); !errors.Is(err, ErrScatterWrite) {
		t.Fatalf("targeted multi-shard write error = %v, want ErrScatterWrite", err)
	}
}

// TestUnbindableMembershipNeverNarrows proves a deferred or otherwise unbindable
// predicate keeps its ordinal unknown until a value is available, and that it
// suppresses any bound predicate sharing the same ordinal, so a not-yet-known
// value can never narrow routing unsafely.
func TestUnbindableMembershipNeverNarrows(t *testing.T) {
	binding := newTestBinding(t, []string{"/tenant_id"}, splitManifest(t))
	router := distribution.NewRouter()

	subqueryIn := func(col string) *sqlast.Expr {
		return &sqlast.Expr{Kind: sqlast.ExprIn, Path: pathExpr(col), Subquery: &sqlast.SelectStmt{}}
	}

	t.Run("subquery-only ordinal stays unknown and scatters", func(t *testing.T) {
		prog := compileConstraintProgram(binding, subqueryIn("tenant_id"))
		cons, err := prog.Bind(nil)
		if err != nil {
			t.Fatalf("Bind: %v", err)
		}
		if cons[0].Kind != distribution.DomainUnknown {
			t.Fatalf("domain kind = %v, want DomainUnknown", cons[0].Kind)
		}
		if _, err := prog.Route(router, nil); !errors.Is(err, distribution.ErrScatterRejected) {
			t.Fatalf("route error = %v, want ErrScatterRejected", err)
		}
	})

	t.Run("unbindable predicate suppresses a bound equality on the same ordinal", func(t *testing.T) {
		// The bound equality alone pins exactly one shard.
		single, err := compileConstraintProgram(binding, eqExpr("tenant_id", strOp("acme"))).Route(router, nil)
		if err != nil {
			t.Fatalf("bound-only Route: %v", err)
		}
		if single.Kind != distribution.RouteSingle {
			t.Fatalf("bound-only route kind = %v, want RouteSingle", single.Kind)
		}
		// Adding a deferred subquery set on that same ordinal must keep it unknown:
		// the known value must not be allowed to narrow routing while another
		// predicate over the ordinal is still unresolved.
		mixed := compileConstraintProgram(binding,
			andExpr(eqExpr("tenant_id", strOp("acme")), subqueryIn("tenant_id")))
		cons, err := mixed.Bind(nil)
		if err != nil {
			t.Fatalf("mixed Bind: %v", err)
		}
		if cons[0].Kind != distribution.DomainUnknown {
			t.Fatalf("mixed domain kind = %v, want DomainUnknown", cons[0].Kind)
		}
		if _, err := mixed.Route(router, nil); !errors.Is(err, distribution.ErrScatterRejected) {
			t.Fatalf("mixed route error = %v, want ErrScatterRejected", err)
		}
	})
}

// TestConstraintProgramPredicateOrderIndependent proves reordering the conjuncts
// of a placed predicate does not change the bound domain or the resolved route.
func TestConstraintProgramPredicateOrderIndependent(t *testing.T) {
	binding := newTestBinding(t, []string{"/tenant_id"}, splitManifest(t))
	router := distribution.NewRouter()

	forward := andExpr(eqExpr("tenant_id", numOp("5")), inExpr("tenant_id", numOp("4"), numOp("5")))
	reverse := andExpr(inExpr("tenant_id", numOp("4"), numOp("5")), eqExpr("tenant_id", numOp("5")))

	fCons, err := compileConstraintProgram(binding, forward).Bind(nil)
	if err != nil {
		t.Fatalf("forward Bind: %v", err)
	}
	rCons, err := compileConstraintProgram(binding, reverse).Bind(nil)
	if err != nil {
		t.Fatalf("reverse Bind: %v", err)
	}
	if fCons[0].Kind != distribution.DomainFinite || len(fCons[0].Values) != 1 {
		t.Fatalf("forward domain = %+v, want finite with one value", fCons[0])
	}
	if rCons[0].Kind != fCons[0].Kind || len(rCons[0].Values) != len(fCons[0].Values) {
		t.Fatalf("reverse domain = %+v, want %+v", rCons[0], fCons[0])
	}

	fRoute, err := compileConstraintProgram(binding, forward).Route(router, nil)
	if err != nil {
		t.Fatalf("forward Route: %v", err)
	}
	rRoute, err := compileConstraintProgram(binding, reverse).Route(router, nil)
	if err != nil {
		t.Fatalf("reverse Route: %v", err)
	}
	if !sameShardSet(fRoute, rRoute) {
		t.Fatalf("predicate order changed routing: %v vs %v", fRoute.Targets, rRoute.Targets)
	}
}
