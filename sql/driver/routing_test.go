package driver

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/query"
	sqlast "github.com/thesyncim/vibedb/sql"
	vibejson "github.com/thesyncim/vibejson"
)

// fullManifest builds a single-shard manifest covering the whole keyspace.
func fullManifest(t *testing.T) *distribution.Manifest {
	t.Helper()
	m, err := distribution.NewManifest("d", 1, []distribution.Shard{{
		ID:                   "s0",
		AllocationGeneration: 1,
		Range:                distribution.KeyRange{Start: distribution.KeyspacePoint{}, End: distribution.KeyspaceEnd{Max: true}},
		Leaders:              []distribution.EndpointID{"e0"},
	}})
	if err != nil {
		t.Fatalf("NewManifest full: %v", err)
	}
	return m
}

// splitManifest builds a two-shard manifest split at the keyspace midpoint, so
// distinct shard-key values can land on distinct shards.
func splitManifest(t *testing.T) *distribution.Manifest {
	t.Helper()
	mid := distribution.KeyspacePoint{0x80}
	m, err := distribution.NewManifest("d", 1, []distribution.Shard{
		{ID: "s0", AllocationGeneration: 1, Range: distribution.KeyRange{Start: distribution.KeyspacePoint{}, End: distribution.KeyspaceEnd{Point: mid}}, Leaders: []distribution.EndpointID{"e0"}},
		{ID: "s1", AllocationGeneration: 2, Range: distribution.KeyRange{Start: mid, End: distribution.KeyspaceEnd{Max: true}}, Leaders: []distribution.EndpointID{"e1"}},
	})
	if err != nil {
		t.Fatalf("NewManifest split: %v", err)
	}
	return m
}

// newTestBinding builds a placementBinding over columns routed by manifest.
func newTestBinding(t *testing.T, columns []string, manifest *distribution.Manifest) *placementBinding {
	t.Helper()
	pointers := make([]vibejson.CompiledPointer, len(columns))
	for i, col := range columns {
		p, err := vibejson.CompilePointer(col)
		if err != nil {
			t.Fatalf("CompilePointer(%q): %v", col, err)
		}
		pointers[i] = p
	}
	native := distribution.NewNativeMapper(len(columns))
	return &placementBinding{
		placement: distribution.TablePlacement{Table: "t", Distribution: "d", Columns: columns},
		spec:      distribution.DistributionSpec{Name: "d", Arity: len(columns), MapperVersion: distribution.NativeMapperVersion},
		mapper:    native,
		native:    native,
		manifest:  manifest,
		pointers:  pointers,
	}
}

func pathExpr(name string) *sqlast.PathExpr {
	return &sqlast.PathExpr{Source: 0, Segments: []sqlast.Segment{{Key: name}}}
}

func strOp(s string) sqlast.Operand  { return sqlast.Operand{Kind: sqlast.OperandString, Text: s} }
func numOp(s string) sqlast.Operand  { return sqlast.Operand{Kind: sqlast.OperandNumber, Text: s} }
func paramOp(ord int) sqlast.Operand { return sqlast.Operand{Kind: sqlast.OperandParam, Ordinal: ord} }

func eqExpr(name string, v sqlast.Operand) *sqlast.Expr {
	return &sqlast.Expr{Kind: sqlast.ExprCompare, Op: sqlast.OpEq, Path: pathExpr(name), Value: v}
}

func neExpr(name string, v sqlast.Operand) *sqlast.Expr {
	return &sqlast.Expr{Kind: sqlast.ExprCompare, Op: sqlast.OpNe, Path: pathExpr(name), Value: v}
}

func pathEqExpr(left, right string) *sqlast.Expr {
	return &sqlast.Expr{
		Kind: sqlast.ExprCompare, Op: sqlast.OpEq,
		Path: pathExpr(left), RightPath: pathExpr(right),
	}
}

func inExpr(name string, vs ...sqlast.Operand) *sqlast.Expr {
	return &sqlast.Expr{Kind: sqlast.ExprIn, Path: pathExpr(name), List: vs}
}

func orExpr(kids ...*sqlast.Expr) *sqlast.Expr {
	return &sqlast.Expr{Kind: sqlast.ExprOr, Kids: kids}
}

func andExpr(kids ...*sqlast.Expr) *sqlast.Expr {
	return &sqlast.Expr{Kind: sqlast.ExprAnd, Kids: kids}
}

func TestConstraintProgramBind(t *testing.T) {
	binding := newTestBinding(t, []string{"/tenant_id"}, fullManifest(t))
	compound := newTestBinding(t, []string{"/tenant_id", "/channel_id"}, fullManifest(t))
	constant := func(value bool) *sqlast.Expr {
		return &sqlast.Expr{Kind: sqlast.ExprConstant, Value: sqlast.Operand{Kind: sqlast.OperandBool, Bool: value}}
	}

	cases := []struct {
		name    string
		binding *placementBinding
		where   *sqlast.Expr
		args    []any
		want    []distribution.DomainKind
		wantLen []int // finite value count per ordinal, -1 to skip
	}{
		{
			name: "false empties all key ordinals", binding: compound, where: constant(false),
			want: []distribution.DomainKind{distribution.DomainEmpty, distribution.DomainEmpty}, wantLen: []int{-1, -1},
		},
		{
			name: "true retains all owners", binding: binding, where: constant(true),
			want: []distribution.DomainKind{distribution.DomainUnknown}, wantLen: []int{-1},
		},
		{
			name: "not true empties key domain", binding: binding, where: &sqlast.Expr{Kind: sqlast.ExprNot, Kids: []*sqlast.Expr{constant(true)}},
			want: []distribution.DomainKind{distribution.DomainEmpty}, wantLen: []int{-1},
		},
		{
			name: "false contributes no disjunction owner", binding: binding, where: orExpr(constant(false), eqExpr("tenant_id", strOp("a"))),
			want: []distribution.DomainKind{distribution.DomainFinite}, wantLen: []int{1},
		},
		{
			name: "false conjunct empties finite domain", binding: binding, where: andExpr(eqExpr("tenant_id", strOp("a")), constant(false)),
			want: []distribution.DomainKind{distribution.DomainEmpty}, wantLen: []int{-1},
		},
		{
			name: "false does not bypass runtime path analysis", binding: binding, where: andExpr(constant(false), pathEqExpr("value", "other_value")),
			want: []distribution.DomainKind{distribution.DomainUnknown}, wantLen: []int{-1},
		},
		{
			name: "disjunction of key values", binding: binding,
			where: orExpr(eqExpr("tenant_id", strOp("a")), eqExpr("tenant_id", paramOp(0))), args: []any{"b"},
			want: []distribution.DomainKind{distribution.DomainFinite}, wantLen: []int{2},
		},
		{
			name: "disjunction intersects sibling", binding: binding,
			where: andExpr(orExpr(eqExpr("tenant_id", strOp("a")), eqExpr("tenant_id", strOp("b"))), eqExpr("tenant_id", strOp("b"))),
			want:  []distribution.DomainKind{distribution.DomainFinite}, wantLen: []int{1},
		},
		{
			name: "unconstrained disjunction arm widens", binding: binding,
			where: orExpr(eqExpr("tenant_id", strOp("a")), eqExpr("other", strOp("b"))),
			want:  []distribution.DomainKind{distribution.DomainUnknown}, wantLen: []int{-1},
		},
		{
			name: "disjunction canonical numeric deduplication", binding: binding,
			where: orExpr(eqExpr("tenant_id", numOp("5")), eqExpr("tenant_id", numOp("5.0"))),
			want:  []distribution.DomainKind{distribution.DomainFinite}, wantLen: []int{1},
		},
		{
			name: "null disjunction arm contributes no key", binding: binding,
			where: orExpr(eqExpr("tenant_id", paramOp(0)), eqExpr("tenant_id", strOp("a"))), args: []any{nil},
			want: []distribution.DomainKind{distribution.DomainFinite}, wantLen: []int{1},
		},
		{
			name: "path comparison prevents disjunction pruning", binding: binding,
			where: andExpr(orExpr(eqExpr("tenant_id", strOp("a")), eqExpr("tenant_id", strOp("b"))), pathEqExpr("value", "other_value")),
			want:  []distribution.DomainKind{distribution.DomainUnknown}, wantLen: []int{-1},
		},
		{
			name: "compound disjunction keeps both ordinals complete", binding: compound,
			where: orExpr(andExpr(eqExpr("tenant_id", strOp("a")), eqExpr("channel_id", numOp("1"))), andExpr(eqExpr("tenant_id", strOp("b")), eqExpr("channel_id", numOp("2")))),
			want:  []distribution.DomainKind{distribution.DomainFinite, distribution.DomainFinite}, wantLen: []int{2, 2},
		},
		{
			name:    "equality literal",
			binding: binding,
			where:   eqExpr("tenant_id", strOp("a")),
			want:    []distribution.DomainKind{distribution.DomainFinite},
			wantLen: []int{1},
		},
		{
			name:    "equality parameter",
			binding: binding,
			where:   eqExpr("tenant_id", paramOp(0)),
			args:    []any{"a"},
			want:    []distribution.DomainKind{distribution.DomainFinite},
			wantLen: []int{1},
		},
		{
			name:    "path equality is unbindable",
			binding: binding,
			where:   pathEqExpr("tenant_id", "other_id"),
			want:    []distribution.DomainKind{distribution.DomainUnknown},
			wantLen: []int{-1},
		},
		{
			name:    "path equality disables sibling shard constraint",
			binding: binding,
			where: &sqlast.Expr{Kind: sqlast.ExprAnd, Kids: []*sqlast.Expr{
				eqExpr("tenant_id", strOp("tenant-a")),
				pathEqExpr("value", "other_value"),
			}},
			want:    []distribution.DomainKind{distribution.DomainUnknown},
			wantLen: []int{-1},
		},
		{
			name:    "equality raw number parameter",
			binding: binding,
			where:   eqExpr("tenant_id", paramOp(0)),
			args:    []any{vibejson.RawValue{Src: []byte("777")}},
			want:    []distribution.DomainKind{distribution.DomainFinite},
			wantLen: []int{1},
		},
		{
			name:    "membership two values",
			binding: binding,
			where:   inExpr("tenant_id", strOp("a"), strOp("b")),
			want:    []distribution.DomainKind{distribution.DomainFinite},
			wantLen: []int{2},
		},
		{
			name:    "equality intersect membership finite",
			binding: binding,
			where:   andExpr(eqExpr("tenant_id", numOp("5")), inExpr("tenant_id", numOp("4"), numOp("5"), numOp("5.0"))),
			want:    []distribution.DomainKind{distribution.DomainFinite},
			wantLen: []int{1},
		},
		{
			name:    "equality intersect membership contradiction",
			binding: binding,
			where:   andExpr(eqExpr("tenant_id", numOp("5")), inExpr("tenant_id", numOp("6"), numOp("7"))),
			want:    []distribution.DomainKind{distribution.DomainEmpty},
			wantLen: []int{-1},
		},
		{
			name:    "exact number spelling equivalence",
			binding: binding,
			where:   inExpr("tenant_id", numOp("5"), numOp("5.0"), numOp("5e0")),
			want:    []distribution.DomainKind{distribution.DomainFinite},
			wantLen: []int{1},
		},
		{
			name:    "no predicate",
			binding: binding,
			where:   nil,
			want:    []distribution.DomainKind{distribution.DomainUnknown},
			wantLen: []int{-1},
		},
		{
			name:    "non shard-key column",
			binding: binding,
			where:   eqExpr("other", strOp("a")),
			want:    []distribution.DomainKind{distribution.DomainUnknown},
			wantLen: []int{-1},
		},
		{
			name:    "inequality is not narrowing",
			binding: binding,
			where:   neExpr("tenant_id", strOp("a")),
			want:    []distribution.DomainKind{distribution.DomainUnknown},
			wantLen: []int{-1},
		},
		{
			name:    "equality with null parameter is empty",
			binding: binding,
			where:   eqExpr("tenant_id", paramOp(0)),
			args:    []any{nil},
			want:    []distribution.DomainKind{distribution.DomainEmpty},
			wantLen: []int{-1},
		},
		{
			name:    "membership skips null alternative",
			binding: binding,
			where:   inExpr("tenant_id", paramOp(0), strOp("b")),
			args:    []any{nil},
			want:    []distribution.DomainKind{distribution.DomainFinite},
			wantLen: []int{1},
		},
		{
			name:    "bool value is unbindable",
			binding: binding,
			where:   eqExpr("tenant_id", paramOp(0)),
			args:    []any{true},
			want:    []distribution.DomainKind{distribution.DomainUnknown},
			wantLen: []int{-1},
		},
		{
			name:    "subquery membership stays unknown",
			binding: binding,
			where:   &sqlast.Expr{Kind: sqlast.ExprIn, Path: pathExpr("tenant_id"), Subquery: &sqlast.SelectStmt{}},
			want:    []distribution.DomainKind{distribution.DomainUnknown},
			wantLen: []int{-1},
		},
		{
			name:    "compound both bound",
			binding: compound,
			where:   andExpr(eqExpr("tenant_id", strOp("a")), eqExpr("channel_id", numOp("7"))),
			want:    []distribution.DomainKind{distribution.DomainFinite, distribution.DomainFinite},
			wantLen: []int{1, 1},
		},
		{
			name:    "compound one bound one unknown",
			binding: compound,
			where:   eqExpr("tenant_id", strOp("a")),
			want:    []distribution.DomainKind{distribution.DomainFinite, distribution.DomainUnknown},
			wantLen: []int{1, -1},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			prog := compileConstraintProgram(c.binding, c.where)
			cons, err := prog.Bind(c.args)
			if err != nil {
				t.Fatalf("Bind: %v", err)
			}
			if len(cons) != len(c.want) {
				t.Fatalf("Bind produced %d ordinals, want %d", len(cons), len(c.want))
			}
			for i := range c.want {
				if cons[i].Kind != c.want[i] {
					t.Fatalf("ordinal %d kind = %v, want %v", i, cons[i].Kind, c.want[i])
				}
				if c.wantLen[i] >= 0 && len(cons[i].Values) != c.wantLen[i] {
					t.Fatalf("ordinal %d has %d values, want %d", i, len(cons[i].Values), c.wantLen[i])
				}
			}
		})
	}
}

// TestConstraintProgramConstantsAndParamsRouteIdentically proves a literal and a
// parameter carrying the same value, and different exact spellings of one number,
// resolve to the same shard.
func TestConstraintProgramConstantsAndParamsRouteIdentically(t *testing.T) {
	binding := newTestBinding(t, []string{"/tenant_id"}, splitManifest(t))
	router := distribution.NewRouter()

	route := func(where *sqlast.Expr, args []any) distribution.Route {
		t.Helper()
		r, err := compileConstraintProgram(binding, where).Route(router, args)
		if err != nil {
			t.Fatalf("Route: %v", err)
		}
		return r
	}

	stringLit := route(eqExpr("tenant_id", strOp("acme")), nil)
	stringParam := route(eqExpr("tenant_id", paramOp(0)), []any{"acme"})
	if !sameShardSet(stringLit, stringParam) {
		t.Fatalf("string literal and parameter routed differently: %v vs %v", stringLit.Targets, stringParam.Targets)
	}

	numLit := route(eqExpr("tenant_id", numOp("5")), nil)
	numParamInt := route(eqExpr("tenant_id", paramOp(0)), []any{int64(5)})
	numParamSpelled := route(eqExpr("tenant_id", paramOp(0)), []any{query.Number("5.0")})
	numLitExp := route(eqExpr("tenant_id", numOp("5e0")), nil)
	for _, other := range []distribution.Route{numParamInt, numParamSpelled, numLitExp} {
		if !sameShardSet(numLit, other) {
			t.Fatalf("number spellings routed differently: %v vs %v", numLit.Targets, other.Targets)
		}
	}
}

func TestValidateShardKeyLocality(t *testing.T) {
	cases := []struct {
		name         string
		shardColumns []string
		primaryKey   string
		wantErr      bool
	}{
		{name: "single key equals shard key", shardColumns: []string{"/tenant_id"}, primaryKey: "/tenant_id"},
		{name: "single key differs from shard key", shardColumns: []string{"/tenant_id"}, primaryKey: "/id", wantErr: true},
		{name: "compound shard key cannot fit single key", shardColumns: []string{"/tenant_id", "/channel_id"}, primaryKey: "/tenant_id", wantErr: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := validateShardKeyLocality("t", c.shardColumns, c.primaryKey)
			if c.wantErr {
				if !errors.Is(err, ErrShardKeyNotLocal) {
					t.Fatalf("err = %v, want ErrShardKeyNotLocal", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("err = %v, want nil", err)
			}
		})
	}
}

func doc(t *testing.T, value string) []byte {
	t.Helper()
	return []byte(fmt.Sprintf(`{"tenant_id":%s}`, strconv.Quote(value)))
}

// twoShardFixture builds a binding whose two-shard manifest boundary is derived
// from the reference mapper's own routed points, so the returned same-shard pair
// and cross-shard pair are partitioned deterministically regardless of the
// mapper's hash distribution.
func twoShardFixture(t *testing.T) (binding *placementBinding, router *distribution.Router, same, diff [2]string) {
	t.Helper()
	mapper := distribution.NewNativeMapper(1)
	type valuePoint struct {
		value string
		point distribution.KeyspacePoint
	}
	points := make([]valuePoint, 0, 64)
	for i := 0; i < 64; i++ {
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
	// Split at the first real point gap at or past the middle, so at least two
	// values fall strictly below the boundary and one falls at or above it.
	mid := -1
	for k := len(points) / 2; k < len(points); k++ {
		if distribution.ComparePoints(points[k-1].point, points[k].point) < 0 {
			mid = k
			break
		}
	}
	if mid < 2 {
		t.Fatalf("probe values did not yield a usable shard boundary")
	}
	boundary := points[mid].point
	manifest, err := distribution.NewManifest("d", 1, []distribution.Shard{
		{ID: "s0", AllocationGeneration: 1, Range: distribution.KeyRange{Start: distribution.KeyspacePoint{}, End: distribution.KeyspaceEnd{Point: boundary}}, Leaders: []distribution.EndpointID{"e0"}},
		{ID: "s1", AllocationGeneration: 2, Range: distribution.KeyRange{Start: boundary, End: distribution.KeyspaceEnd{Max: true}}, Leaders: []distribution.EndpointID{"e1"}},
	})
	if err != nil {
		t.Fatalf("NewManifest two-shard: %v", err)
	}
	binding = newTestBinding(t, []string{"/tenant_id"}, manifest)
	router = distribution.NewRouter()
	same = [2]string{points[0].value, points[1].value}
	diff = [2]string{points[0].value, points[mid].value}
	return binding, router, same, diff
}

func TestInsertPreflight(t *testing.T) {
	binding, _, same, diff := twoShardFixture(t)

	t.Run("single row", func(t *testing.T) {
		route, err := insertPreflight(binding, [][]byte{doc(t, same[0])})
		if err != nil {
			t.Fatalf("insertPreflight: %v", err)
		}
		if route.Kind != distribution.RouteSingle {
			t.Fatalf("route kind = %v, want RouteSingle", route.Kind)
		}
	})

	t.Run("same-shard multi row accepted", func(t *testing.T) {
		route, err := insertPreflight(binding, [][]byte{doc(t, same[0]), doc(t, same[1])})
		if err != nil {
			t.Fatalf("insertPreflight: %v", err)
		}
		if route.Kind != distribution.RouteSingle {
			t.Fatalf("route kind = %v, want RouteSingle", route.Kind)
		}
	})

	t.Run("cross-shard multi row rejected", func(t *testing.T) {
		_, err := insertPreflight(binding, [][]byte{doc(t, diff[0]), doc(t, diff[1])})
		if !errors.Is(err, ErrCrossShardWrite) {
			t.Fatalf("err = %v, want ErrCrossShardWrite", err)
		}
	})

	t.Run("missing shard-key column", func(t *testing.T) {
		_, err := insertPreflight(binding, [][]byte{[]byte(`{"other":1}`)})
		if err == nil {
			t.Fatalf("expected an error for a missing shard-key column")
		}
	})

	t.Run("null shard-key column", func(t *testing.T) {
		_, err := insertPreflight(binding, [][]byte{[]byte(`{"tenant_id":null}`)})
		if err == nil {
			t.Fatalf("expected an error for a null shard-key column")
		}
	})

	t.Run("bool shard-key value", func(t *testing.T) {
		_, err := insertPreflight(binding, [][]byte{[]byte(`{"tenant_id":true}`)})
		if err == nil {
			t.Fatalf("expected an error for a non-scalar shard-key value")
		}
	})
}

func TestSingleShardRoute(t *testing.T) {
	binding, router, _, diff := twoShardFixture(t)

	t.Run("equality single shard", func(t *testing.T) {
		route, err := singleShardRoute(compileConstraintProgram(binding, eqExpr("tenant_id", strOp(diff[0]))), router, nil)
		if err != nil {
			t.Fatalf("singleShardRoute: %v", err)
		}
		if route.Kind != distribution.RouteSingle {
			t.Fatalf("route kind = %v, want RouteSingle", route.Kind)
		}
	})

	t.Run("membership across shards rejected", func(t *testing.T) {
		where := inExpr("tenant_id", strOp(diff[0]), strOp(diff[1]))
		_, err := singleShardRoute(compileConstraintProgram(binding, where), router, nil)
		if !errors.Is(err, ErrScatterWrite) {
			t.Fatalf("err = %v, want ErrScatterWrite", err)
		}
	})

	t.Run("unknown predicate rejected", func(t *testing.T) {
		_, err := singleShardRoute(compileConstraintProgram(binding, nil), router, nil)
		if !errors.Is(err, ErrScatterWrite) {
			t.Fatalf("err = %v, want ErrScatterWrite", err)
		}
	})

	t.Run("contradiction is an accepted empty route", func(t *testing.T) {
		where := andExpr(eqExpr("tenant_id", strOp(diff[0])), eqExpr("tenant_id", strOp(diff[1])))
		route, err := singleShardRoute(compileConstraintProgram(binding, where), router, nil)
		if err != nil {
			t.Fatalf("singleShardRoute: %v", err)
		}
		if route.Kind != distribution.RouteEmpty {
			t.Fatalf("route kind = %v, want RouteEmpty", route.Kind)
		}
	})
}

func TestCheckShardKeyImmutable(t *testing.T) {
	binding, router, _, diff := twoShardFixture(t)

	target, err := insertPreflight(binding, [][]byte{doc(t, diff[0])})
	if err != nil {
		t.Fatalf("insertPreflight: %v", err)
	}

	t.Run("same shard key unchanged", func(t *testing.T) {
		if err := checkShardKeyImmutable(binding, router, target, doc(t, diff[0])); err != nil {
			t.Fatalf("checkShardKeyImmutable: %v", err)
		}
	})

	t.Run("shard-key move rejected", func(t *testing.T) {
		err := checkShardKeyImmutable(binding, router, target, doc(t, diff[1]))
		if !errors.Is(err, ErrShardKeyImmutable) {
			t.Fatalf("err = %v, want ErrShardKeyImmutable", err)
		}
	})

	t.Run("empty target is a no-op", func(t *testing.T) {
		empty := distribution.Route{Kind: distribution.RouteEmpty}
		if err := checkShardKeyImmutable(binding, router, empty, doc(t, diff[1])); err != nil {
			t.Fatalf("checkShardKeyImmutable(empty target): %v", err)
		}
	})
}

// TestInsertAndPredicateRouteConsistently proves an inserted document and an
// equality predicate over the same value route to the same shard, so a row can be
// found where it was written.
func TestInsertAndPredicateRouteConsistently(t *testing.T) {
	binding := newTestBinding(t, []string{"/tenant_id"}, splitManifest(t))
	router := distribution.NewRouter()

	for _, v := range []string{"acme", "globex", "initech"} {
		insertRoute, err := insertPreflight(binding, [][]byte{doc(t, v)})
		if err != nil {
			t.Fatalf("insertPreflight(%q): %v", v, err)
		}
		predRoute, err := compileConstraintProgram(binding, eqExpr("tenant_id", strOp(v))).Route(router, nil)
		if err != nil {
			t.Fatalf("predicate Route(%q): %v", v, err)
		}
		if !sameShardSet(insertRoute, predRoute) {
			t.Fatalf("value %q: insert routed to %v but predicate routed to %v", v, insertRoute.Targets, predRoute.Targets)
		}
	}
}
