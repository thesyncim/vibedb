package gateway

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"unsafe"

	"github.com/thesyncim/vibedb/distribution"
	sqlast "github.com/thesyncim/vibedb/sql"
	vibejson "github.com/thesyncim/vibejson"
)

func routeBoundPlan(t testing.TB, plan *BoundPlan) distribution.Route {
	t.Helper()
	route, err := distribution.NewRouter().Route(
		plan.constraints,
		distribution.NewNativeMapperWithBucketBits(plan.spec.Arity, plan.spec.EffectiveBucketBits()),
		plan.manifest,
		distribution.NewRoutePolicy(distribution.AdmissionAllowScatter, distribution.RouteLimits{}),
	)
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	return route
}

// TestPreparedPlanCompileBind proves SQL, placement metadata, and typed
// parameters—not client-supplied routing facts—produce the shard constraints,
// merge order, and global limit.
func TestPreparedPlanCompileBind(t *testing.T) {
	snap := testSnapshot(t, 7)
	prepared, err := snap.Prepare(context.Background(),
		`SELECT n FROM messages WHERE tenant_id = ? ORDER BY n DESC LIMIT ?`)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	bound, err := prepared.Bind([]any{
		"acme", vibejson.RawValue{Src: []byte("17")},
	})
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if bound.generation != 7 || bound.table != "messages" || bound.distribution != "tenant_data" {
		t.Fatalf("bound identity = %+v", bound)
	}
	if len(bound.constraints) != 1 || bound.constraints[0].Kind != distribution.DomainFinite ||
		len(bound.constraints[0].Values) != 1 {
		t.Fatalf("constraints = %+v, want one finite shard key", bound.constraints)
	}
	if len(bound.order) != 1 || bound.order[0] != (OrderKey{Column: 0, Desc: true}) {
		t.Fatalf("order = %+v, want column 0 descending", bound.order)
	}
	if bound.limit != 17 {
		t.Fatalf("limit = %d, want 17", bound.limit)
	}
	if route := routeBoundPlan(t, bound); route.Kind != distribution.RouteSingle {
		t.Fatalf("route kind = %s, want Single", route.Kind)
	}
}

// TestPreparedPlanLiteralParameterParity proves the shared driver constraint
// compiler keeps distributed and embedded exact-value routing semantics equal.
func TestPreparedPlanLiteralParameterParity(t *testing.T) {
	snap := testSnapshot(t, 1)
	literal, err := snap.Prepare(context.Background(),
		`SELECT n FROM messages WHERE tenant_id = 'acme'`)
	if err != nil {
		t.Fatalf("Prepare literal: %v", err)
	}
	parameter, err := snap.Prepare(context.Background(),
		`SELECT n FROM messages WHERE tenant_id = ?`)
	if err != nil {
		t.Fatalf("Prepare parameter: %v", err)
	}
	litBound, err := literal.Bind(nil)
	if err != nil {
		t.Fatalf("Bind literal: %v", err)
	}
	paramBound, err := parameter.Bind([]any{"acme"})
	if err != nil {
		t.Fatalf("Bind parameter: %v", err)
	}
	litRoute := routeBoundPlan(t, litBound)
	paramRoute := routeBoundPlan(t, paramBound)
	if len(litRoute.Targets) != 1 || len(paramRoute.Targets) != 1 ||
		litRoute.Targets[0].Shard != paramRoute.Targets[0].Shard {
		t.Fatalf("literal/parameter targets = %v / %v", litRoute.Targets, paramRoute.Targets)
	}
}

// TestPreparedPlanDistributedAggregateBoundary proves algebraic aggregates get
// an exact final combiner while aggregate expressions without merge state still
// fail before dispatch.
func TestPreparedPlanDistributedAggregateBoundary(t *testing.T) {
	snap := testSnapshot(t, 1)

	scatter, err := snap.Prepare(context.Background(), `SELECT COUNT(*) FROM messages`)
	if err != nil {
		t.Fatalf("Prepare scatter aggregate: %v", err)
	}
	scatterBound, err := scatter.Bind(nil)
	if err != nil {
		t.Fatalf("Bind scatter aggregate: %v", err)
	}
	scatterRoute := routeBoundPlan(t, scatterBound)
	if err := scatterBound.ValidateRoute(scatterRoute); err != nil {
		t.Fatalf("scatter COUNT validation: %v", err)
	}
	if len(scatterBound.aggregates) != 1 || scatterBound.aggregates[0] != sqlast.AggCount {
		t.Fatalf("aggregate program = %v, want COUNT", scatterBound.aggregates)
	}

	grouped, err := snap.Prepare(context.Background(),
		`SELECT tenant_id, COUNT(*), SUM(n), MIN(n), MAX(n) FROM messages GROUP BY tenant_id`)
	if err != nil {
		t.Fatalf("Prepare grouped aggregate: %v", err)
	}
	groupedBound, err := grouped.Bind(nil)
	if err != nil {
		t.Fatalf("Bind grouped aggregate: %v", err)
	}
	if err := groupedBound.ValidateRoute(routeBoundPlan(t, groupedBound)); err != nil {
		t.Fatalf("scatter GROUP BY validation: %v", err)
	}
	wantKinds := []sqlast.AggKind{
		sqlast.AggNone, sqlast.AggCount, sqlast.AggSum, sqlast.AggMin, sqlast.AggMax,
	}
	if !slices.Equal(groupedBound.aggregates, wantKinds) || !slices.Equal(groupedBound.groupKeys, []int{0}) {
		t.Fatalf("grouped program = %v keys %v, want %v keys [0]",
			groupedBound.aggregates, groupedBound.groupKeys, wantKinds)
	}

	groupedOrder, err := snap.Prepare(context.Background(),
		`SELECT tenant_id, COUNT(*) FROM messages GROUP BY tenant_id ORDER BY tenant_id`)
	if err != nil {
		t.Fatalf("Prepare grouped ORDER BY: %v", err)
	}
	groupedOrderBound, err := groupedOrder.Bind(nil)
	if err != nil {
		t.Fatalf("Bind grouped ORDER BY: %v", err)
	}
	if err := groupedOrderBound.ValidateRoute(routeBoundPlan(t, groupedOrderBound)); err != nil {
		t.Fatalf("grouped ORDER BY validation = %v, want post-finalization sort", err)
	}

	nonlocalLimit, err := snap.Prepare(context.Background(),
		`SELECT n, COUNT(*) FROM messages GROUP BY n ORDER BY n LIMIT 2`)
	if err != nil {
		t.Fatalf("Prepare nonlocal grouped LIMIT: %v", err)
	}
	nonlocalLimitBound, err := nonlocalLimit.Bind(nil)
	if err != nil {
		t.Fatalf("Bind nonlocal grouped LIMIT: %v", err)
	}
	if err := nonlocalLimitBound.ValidateRoute(routeBoundPlan(t, nonlocalLimitBound)); err != nil {
		t.Fatalf("nonlocal grouped LIMIT validation = %v, want partial-fragment admission", err)
	}

	distinct, err := snap.Prepare(context.Background(),
		`SELECT DISTINCT n FROM messages ORDER BY n DESC LIMIT 2`)
	if err != nil {
		t.Fatalf("Prepare DISTINCT: %v", err)
	}
	distinctBound, err := distinct.Bind(nil)
	if err != nil {
		t.Fatalf("Bind DISTINCT: %v", err)
	}
	if !slices.Equal(distinctBound.aggregates, []sqlast.AggKind{sqlast.AggNone}) ||
		!slices.Equal(distinctBound.groupKeys, []int{0}) {
		t.Fatalf("DISTINCT program = %v keys %v", distinctBound.aggregates, distinctBound.groupKeys)
	}
	if err := distinctBound.ValidateRoute(routeBoundPlan(t, distinctBound)); err != nil {
		t.Fatalf("distributed DISTINCT validation = %v", err)
	}
	windowedDistinct, err := snap.Prepare(context.Background(),
		`SELECT DISTINCT n FROM messages ORDER BY n DESC LIMIT ? OFFSET ?`)
	if err != nil {
		t.Fatalf("Prepare windowed DISTINCT: %v", err)
	}
	windowedDistinctBound, err := windowedDistinct.Bind([]any{2, 1})
	if err != nil || !windowedDistinctBound.hasLimit || windowedDistinctBound.limit != 2 ||
		windowedDistinctBound.offset != 1 {
		t.Fatalf("windowed DISTINCT bind = %+v, %v", windowedDistinctBound, err)
	}
	if err := windowedDistinctBound.ValidateRoute(routeBoundPlan(t, windowedDistinctBound)); err != nil {
		t.Fatalf("windowed DISTINCT validation = %v", err)
	}

	localLimit, err := snap.Prepare(context.Background(),
		`SELECT tenant_id, COUNT(*) FROM messages GROUP BY tenant_id ORDER BY tenant_id LIMIT 2`)
	if err != nil {
		t.Fatalf("Prepare shard-local grouped LIMIT: %v", err)
	}
	localLimitBound, err := localLimit.Bind(nil)
	if err != nil {
		t.Fatalf("Bind shard-local grouped LIMIT: %v", err)
	}
	if err := localLimitBound.ValidateRoute(routeBoundPlan(t, localLimitBound)); err != nil {
		t.Fatalf("shard-local grouped LIMIT validation = %v", err)
	}

	missingKey, err := snap.Prepare(context.Background(),
		`SELECT COUNT(*) FROM messages GROUP BY tenant_id`)
	if err != nil {
		t.Fatalf("Prepare omitted grouped key: %v", err)
	}
	missingKeyBound, err := missingKey.Bind(nil)
	if err != nil {
		t.Fatalf("Bind omitted grouped key: %v", err)
	}
	if err := missingKeyBound.ValidateRoute(routeBoundPlan(t, missingKeyBound)); !errors.Is(err, ErrDistributedPlanUnsupported) {
		t.Fatalf("omitted grouped key validation = %v, want unsupported finalization", err)
	}

	single, err := snap.Prepare(context.Background(),
		`SELECT COUNT(*) FROM messages WHERE tenant_id = 'acme'`)
	if err != nil {
		t.Fatalf("Prepare single aggregate: %v", err)
	}
	singleBound, err := single.Bind(nil)
	if err != nil {
		t.Fatalf("Bind single aggregate: %v", err)
	}
	if err := singleBound.ValidateRoute(routeBoundPlan(t, singleBound)); err != nil {
		t.Fatalf("single-shard aggregate validation: %v", err)
	}

	scalar, err := snap.Prepare(context.Background(), `SELECT SUM(n) + 1 FROM messages`)
	if err != nil {
		t.Fatalf("Prepare scalar aggregate: %v", err)
	}
	scalarBound, err := scalar.Bind(nil)
	if err != nil {
		t.Fatalf("Bind scalar aggregate: %v", err)
	}
	if err := scalarBound.ValidateRoute(routeBoundPlan(t, scalarBound)); !errors.Is(err, ErrDistributedPlanUnsupported) {
		t.Fatalf("scalar aggregate validation = %v, want unsupported", err)
	}

	avg, err := snap.Prepare(context.Background(), `SELECT AVG(n) FROM messages`)
	if err != nil {
		t.Fatalf("Prepare AVG: %v", err)
	}
	avgBound, err := avg.Bind(nil)
	if err != nil {
		t.Fatalf("Bind AVG: %v", err)
	}
	if err := avgBound.ValidateRoute(routeBoundPlan(t, avgBound)); !errors.Is(err, ErrDistributedPlanUnsupported) {
		t.Fatalf("AVG validation = %v, want unsupported until SUM+COUNT projection", err)
	}

	emptyAVG, err := snap.Prepare(context.Background(),
		`SELECT AVG(n) FROM messages WHERE tenant_id = 'a' AND tenant_id = 'b'`)
	if err != nil {
		t.Fatalf("Prepare empty AVG: %v", err)
	}
	emptyAVGBound, err := emptyAVG.Bind(nil)
	if err != nil {
		t.Fatalf("Bind empty AVG: %v", err)
	}
	if route := routeBoundPlan(t, emptyAVGBound); route.Kind != distribution.RouteEmpty {
		t.Fatalf("empty AVG route = %s", route.Kind)
	} else if err := emptyAVGBound.ValidateRoute(route); !errors.Is(err, ErrDistributedPlanUnsupported) {
		t.Fatalf("empty AVG validation = %v, want refusal rather than erased aggregate row", err)
	}
}

func TestPreparedPlanJoinColocationProof(t *testing.T) {
	config := testConfig(t)
	config.Placements = append(config.Placements, distribution.TablePlacement{
		Table: "users", Distribution: "tenant_data", Columns: []string{"/tenant_id"},
	})
	snap, err := NewSnapshot(config, testEndpoints(), 1)
	if err != nil {
		t.Fatalf("NewSnapshot: %v", err)
	}

	colocated, err := snap.Prepare(t.Context(), `
		SELECT messages.n
		FROM messages JOIN users ON messages.tenant_id = users.tenant_id
		WHERE messages.tenant_id = 'acme'`)
	if err != nil {
		t.Fatalf("Prepare colocated join: %v", err)
	}
	bound, err := colocated.Bind(nil)
	if err != nil {
		t.Fatalf("Bind colocated join: %v", err)
	}
	if err := bound.ValidateRoute(routeBoundPlan(t, bound)); err != nil {
		t.Fatalf("colocated join validation: %v", err)
	}

	noncolocated, err := snap.Prepare(t.Context(), `
		SELECT messages.n
		FROM messages JOIN users ON messages.n = users.n
		WHERE messages.tenant_id = 'acme'`)
	if err != nil {
		t.Fatalf("Prepare non-colocated join: %v", err)
	}
	noncolocatedBound, err := noncolocated.Bind(nil)
	if err != nil {
		t.Fatalf("Bind non-colocated join: %v", err)
	}
	if err := noncolocatedBound.ValidateRoute(routeBoundPlan(t, noncolocatedBound)); !errors.Is(err, ErrDistributedPlanUnsupported) {
		t.Fatalf("non-colocated join validation = %v, want unsupported", err)
	}

	config.Placements[0].AffinityGroup = "messages"
	config.Placements[1].AffinityGroup = "users"
	nonaffine, err := NewSnapshot(config, testEndpoints(), 2)
	if err != nil {
		t.Fatalf("NewSnapshot nonaffine: %v", err)
	}
	if _, err := nonaffine.Prepare(t.Context(), `
		SELECT messages.n
		FROM messages JOIN users ON messages.tenant_id = users.tenant_id
		WHERE messages.tenant_id = 'acme'`); err != nil {
		t.Fatalf("nonaffine coordinator join prepare = %v", err)
	}
}

func TestPreparedPlanNeverPrunesAlwaysUnsafeSemantics(t *testing.T) {
	config := testConfig(t)
	config.Placements = append(config.Placements, distribution.TablePlacement{
		Table: "users", Distribution: "tenant_data", Columns: []string{"/tenant_id"},
	})
	snap, err := NewSnapshot(config, testEndpoints(), 1)
	if err != nil {
		t.Fatalf("NewSnapshot: %v", err)
	}
	prepared, err := snap.Prepare(t.Context(), `
		SELECT users.tenant_id
		FROM messages RIGHT JOIN users ON messages.tenant_id = users.tenant_id
		WHERE messages.tenant_id = 'a' AND messages.tenant_id = 'b'`)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	bound, err := prepared.Bind(nil)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	route := routeBoundPlan(t, bound)
	if route.Kind != distribution.RouteEmpty {
		t.Fatalf("physical route = %s, want Empty", route.Kind)
	}
	if err := bound.ValidateRoute(route); !errors.Is(err, ErrDistributedPlanUnsupported) {
		t.Fatalf("empty unsafe route validation = %v, want unsupported", err)
	}
}

func TestPreparedPlanRejectsPhysicalNestedReadsOnTargetedRoute(t *testing.T) {
	config := testConfig(t)
	config.Placements = append(config.Placements, distribution.TablePlacement{
		Table: "users", Distribution: "tenant_data", Columns: []string{"/tenant_id"},
	})
	snap, err := NewSnapshot(config, testEndpoints(), 1)
	if err != nil {
		t.Fatalf("NewSnapshot: %v", err)
	}
	prepared, err := snap.Prepare(t.Context(), `
		SELECT n FROM messages
		WHERE tenant_id = 'acme' AND EXISTS (SELECT tenant_id FROM users)`)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	bound, err := prepared.Bind(nil)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	route := routeBoundPlan(t, bound)
	if route.Kind != distribution.RouteSingle {
		t.Fatalf("route = %s, want targeted Single before semantic validation", route.Kind)
	}
	if err := bound.ValidateRoute(route); !errors.Is(err, ErrDistributedPlanUnsupported) {
		t.Fatalf("nested physical read validation = %v, want unsupported", err)
	}
}

func TestPreparedPlanRefusals(t *testing.T) {
	snap := testSnapshot(t, 1)
	if _, err := snap.Prepare(context.Background(), `SELECT id FROM absent`); !errors.Is(err, ErrTableNotPlaced) {
		t.Fatalf("unplaced table err = %v, want ErrTableNotPlaced", err)
	}
	prepared, err := snap.Prepare(context.Background(), `SELECT n FROM messages WHERE tenant_id = ?`)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	if _, err := prepared.Bind(nil); !errors.Is(err, ErrPlanParameters) {
		t.Fatalf("missing param err = %v, want ErrPlanParameters", err)
	}
	// An unbounded DELETE prepares as a write plan but is refused at bind: it has
	// no shard-key predicate, so it cannot be proven single-shard for any values.
	deletePlan, err := snap.Prepare(context.Background(), `DELETE FROM messages`)
	if err != nil {
		t.Fatalf("DELETE Prepare: %v", err)
	}
	if _, err := deletePlan.BindWrite(nil); !errors.Is(err, ErrDistributedWriteUnsupported) {
		t.Fatalf("DELETE bind err = %v, want ErrDistributedWriteUnsupported", err)
	}
	if _, err := snap.Prepare(context.Background(), `
		WITH dormant AS (SELECT id FROM absent)
		SELECT n FROM messages WHERE tenant_id = 'a' AND tenant_id = 'b'`); !errors.Is(err, ErrTableNotPlaced) {
		t.Fatalf("dormant CTE table err = %v, want ErrTableNotPlaced", err)
	}
	if _, err := snap.Prepare(context.Background(), `
		SELECT n FROM messages
		WHERE tenant_id = 'a' AND tenant_id = 'b'
		  AND EXISTS (SELECT id FROM absent)`); !errors.Is(err, ErrTableNotPlaced) {
		t.Fatalf("unreachable subquery table err = %v, want ErrTableNotPlaced", err)
	}
}

// TestPreparedPlanCache proves successful plans are reused only inside their
// immutable catalog generation, remain safe for concurrent binding, and never
// let a cache hit bypass request cancellation.
func TestPreparedPlanCache(t *testing.T) {
	snap := testSnapshot(t, 1)
	const text = `SELECT n FROM messages WHERE tenant_id = ? ORDER BY n LIMIT 5`
	first, err := snap.Prepare(t.Context(), text)
	if err != nil {
		t.Fatalf("first Prepare: %v", err)
	}
	second, err := snap.Prepare(t.Context(), text)
	if err != nil {
		t.Fatalf("cached Prepare: %v", err)
	}
	if first != second {
		t.Fatal("cache returned a distinct prepared plan")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := snap.Prepare(ctx, text); !errors.Is(err, context.Canceled) {
		t.Fatalf("cached canceled Prepare = %v, want context.Canceled", err)
	}

	const workers = 32
	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			plan, err := snap.Prepare(context.Background(), text)
			if err == nil && plan != first {
				err = errors.New("concurrent cache hit returned a distinct plan")
			}
			if err == nil {
				_, err = plan.Bind([]any{"tenant"})
			}
			errCh <- err
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestPreparedPlanCacheSkipsLargeSQL(t *testing.T) {
	snap := testSnapshot(t, 1)
	text := `SELECT n FROM messages WHERE tenant_id = 'acme'` +
		strings.Repeat(" ", maxCachedSQLBytes)
	first, err := snap.Prepare(t.Context(), text)
	if err != nil {
		t.Fatalf("first Prepare: %v", err)
	}
	second, err := snap.Prepare(t.Context(), text)
	if err != nil {
		t.Fatalf("second Prepare: %v", err)
	}
	if first == second {
		t.Fatal("oversized SQL unexpectedly became a generation-lifetime cache resident")
	}
	if snap.planCache.Load() != nil {
		t.Fatal("oversized SQL allocated the lazy plan-cache directory")
	}
}

func TestPreparedPlanCacheOwnsBoundedSQLBeforeParsing(t *testing.T) {
	const text = `SELECT n FROM messages WHERE tenant_id = ? ORDER BY n LIMIT 5`
	for _, offset := range []int{0, 64 << 10} {
		snap := testSnapshot(t, 1)
		input := make([]byte, 1<<20)
		copy(input[offset:], text)
		borrowed := unsafe.String(unsafe.SliceData(input[offset:]), len(text))
		plan, err := snap.Prepare(t.Context(), borrowed)
		if err != nil {
			t.Fatal(err)
		}
		_, hash := snap.cachedPreparedPlan(text)
		entry := snap.planCache.Load().entries[hash&(preparedPlanCacheSlots-1)].Load()
		// Inspect the parser's actual retained lexer source: checking only the
		// cloned cache key would miss a full ingress buffer retained by lx.src.
		source := reflect.ValueOf(&plan.parser).Elem().FieldByName("lx").FieldByName("src").String()
		if source != text || entry.sql != text {
			t.Fatal("cache/parser SQL mismatch")
		}
		if unsafe.StringData(source) == unsafe.StringData(borrowed) {
			t.Fatal("cached parser retained borrowed ingress source")
		}
		if unsafe.StringData(source) != unsafe.StringData(entry.sql) {
			t.Fatal("parser source and cache key do not share one owned SQL allocation")
		}
		clear(input)
		if source != text || entry.sql != text {
			t.Fatal("input reuse mutated cached SQL")
		}
		if _, err := plan.Bind([]any{"tenant"}); err != nil {
			t.Fatal(err)
		}
	}
}

// TestCompactPlannerDirectory proves large table catalogs use a sorted compact
// directory rather than a retained hash map or one allocation per key column.
func TestCompactPlannerDirectory(t *testing.T) {
	const tables = 10_000
	placements := make([]distribution.TablePlacement, tables)
	for i := range placements {
		placements[i] = distribution.TablePlacement{
			Table:        fmt.Sprintf("table_%05d", tables-1-i),
			Distribution: "tenant_data",
			Columns:      []string{"/tenant_id"},
		}
	}
	config := testConfig(t)
	config.Placements = placements
	snap, err := NewSnapshot(config, testEndpoints(), 1)
	if err != nil {
		t.Fatalf("NewSnapshot: %v", err)
	}
	if got := snap.PlannerMetadataBytes(); got > tables*12 {
		t.Fatalf("planner directory = %d bytes (%0.2f/table), want <= 12/table",
			got, float64(got)/tables)
	}
	for _, name := range []string{"table_00000", "table_05000", "table_09999"} {
		placement, spec, manifest, ok := snap.plannerTableFor(name)
		if !ok || placement.Table != name || spec.Name != "tenant_data" || manifest == nil {
			t.Fatalf("lookup %q = %+v/%+v/%v/%v", name, placement, spec, manifest, ok)
		}
	}
	if allocs := testing.AllocsPerRun(1000, func() {
		_, _, _, _ = snap.plannerTableFor("table_05000")
	}); allocs != 0 {
		t.Fatalf("planner lookup allocations = %v, want 0", allocs)
	}

	// Every placement key is a view into one flat backing arena and shares the
	// interned column spelling. Adjacent one-column slices therefore have
	// adjacent element addresses.
	if &snap.config.Placements[0].Columns[0] == &snap.config.Placements[1].Columns[0] {
		t.Fatal("distinct column slots unexpectedly alias")
	}
	if snap.config.Placements[0].Columns[0] != snap.config.Placements[1].Columns[0] {
		t.Fatal("interned placement column spellings differ")
	}
}

func BenchmarkPlannerDirectoryLookup(b *testing.B) {
	for _, tables := range []int{1, 1_000, 100_000} {
		b.Run(fmt.Sprintf("tables=%d", tables), func(b *testing.B) {
			placements := make([]distribution.TablePlacement, tables)
			for i := range placements {
				placements[i] = distribution.TablePlacement{
					Table:        fmt.Sprintf("table_%06d", i),
					Distribution: "tenant_data",
					Columns:      []string{"/tenant_id"},
				}
			}
			config := testConfig(b)
			config.Placements = placements
			snap, err := NewSnapshot(config, testEndpoints(), 1)
			if err != nil {
				b.Fatalf("NewSnapshot: %v", err)
			}
			name := fmt.Sprintf("table_%06d", tables/2)
			b.ReportAllocs()
			b.ResetTimer()
			b.ReportMetric(float64(snap.PlannerMetadataBytes())/float64(tables), "planner-B/table")
			for i := 0; i < b.N; i++ {
				_, _, _, ok := snap.plannerTableFor(name)
				if !ok {
					b.Fatal("lookup missed")
				}
			}
		})
	}
}

func BenchmarkPreparedPlanCacheHit(b *testing.B) {
	snap := testSnapshot(b, 1)
	const text = `SELECT n FROM messages WHERE tenant_id = ? ORDER BY n LIMIT 5`
	if _, err := snap.Prepare(context.Background(), text); err != nil {
		b.Fatalf("Prepare: %v", err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := snap.Prepare(context.Background(), text); err != nil {
			b.Fatal(err)
		}
	}
}
