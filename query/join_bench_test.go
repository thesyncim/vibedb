package query

import (
	"fmt"
	"testing"

	"github.com/thesyncim/vibedb/store"
)

// The evidence behind every constant in join.go and join_bloom.go, and the
// evidence that a semi-join is worth having at all.
//
// Four things are measured against each other:
//
//   - MembershipPath: the threshold forced high, so the inner side's join keys
//     are collected and pushed into the outer predicate as a membership.
//   - LookupPath: the threshold forced to one, so the collection is abandoned
//     and every outer row probes the inner collection by key.
//   - BloomPrefilter: the same lookup with and without the semi-join reduction
//     filter in front of it.
//   - TwoQueryFilter: the hand-written equivalent a caller would write without
//     a join — run the inner query, collect its keys, build an In predicate
//     over them, run the outer query — which is the baseline the feature has to
//     beat to justify existing.
//
// The sweeps are what fix the constants: each holds everything constant except
// the quantity its constant is a threshold on, so the crossing point is a
// number rather than a judgement.

// joinBenchDatabase builds an orders/customers pair. matching is how many of
// the customers pass the inner filter, and therefore how large a membership
// would have to be; indexed declares an exact index on the outer join path so
// the membership can lower to a mask intersection.
func joinBenchDatabase(b testing.TB, outerRows, customers, matching int, indexed bool) *store.Database {
	db := &store.Database{}
	orders, err := db.CreateCollection("orders", store.Options{})
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < outerRows; i++ {
		doc := fmt.Sprintf(`{"id":%d,"customer":"c%d","total":%d,"region":"r%d"}`,
			i, i%customers, i%997, i%8)
		if _, err := orders.Put(fmt.Sprintf("o%d", i), []byte(doc)); err != nil {
			b.Fatal(err)
		}
	}
	buyers, err := db.CreateCollection("customers", store.Options{})
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < customers; i++ {
		// Spread the matching customers evenly rather than putting them in a
		// prefix. A prefix is not a neutral layout: it makes any sample of the
		// first rows scanned unrepresentative of the whole, which is precisely
		// what the mid-scan abandon check reads. Benchmarking a sampler against
		// data laid out to defeat sampling measures the layout, not the
		// sampler — the clustered case gets its own test instead.
		tier := "free"
		if stride := customers / max(matching, 1); stride > 0 && i%stride == 0 {
			tier = "pro"
		}
		doc := fmt.Sprintf(`{"tier":%q,"seat":%d,"name":"c%d"}`, tier, i, i)
		if _, err := buyers.Put(fmt.Sprintf("c%d", i), []byte(doc)); err != nil {
			b.Fatal(err)
		}
	}
	if indexed {
		if _, err := orders.CreateIndex(store.IndexDefinition{
			Name: "customer", Paths: []string{"/customer"},
		}); err != nil {
			b.Fatal(err)
		}
		if info, err := orders.BackfillIndex("customer", 0); err != nil || info.State != store.IndexReady {
			b.Fatalf("BackfillIndex(customer) = (%+v, %v)", info, err)
		}
	}
	return db
}

// joinBenchShapes are the points the membership/lookup sweep runs. The outer
// size is held constant so the only thing moving is how many join-key values a
// membership would have to hold.
var joinBenchShapes = []struct {
	name                           string
	outerRows, customers, matching int
}{
	{"matching=16", 20000, 20000, 16},
	{"matching=256", 20000, 20000, 256},
	{"matching=2048", 20000, 20000, 2048},
	{"matching=16384", 20000, 20000, 16384},
}

func benchmarkJoin(b *testing.B, membershipMax int, indexed bool) {
	for _, shape := range joinBenchShapes {
		b.Run(shape.name, func(b *testing.B) {
			db := joinBenchDatabase(b, shape.outerRows, shape.customers, shape.matching, indexed)
			catalog := db.Snapshot()
			q := Select(Count()).
				Join(JoinOn("customers", "customer", JoinKey).Where(Cmp("tier", Eq, "pro")))
			var e Exec
			e.Options.JoinMembershipMax = membershipMax
			// The prefilter is a separate measurement with its own benchmark;
			// leaving it on here would make this sweep report two decisions.
			e.Options.JoinFilterScanRatio = -1
			warmExec(b, q, &e, FromDatabase(catalog, "orders"))
			assertBenchStrategy(b, e.Stats, membershipMax)
			matched, _ := e.Result.Columns[0].Cells[0].Float64()
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if err := q.RunInto(&e, FromDatabase(catalog, "orders")); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*shape.outerRows), "ns/outer-row")
			// Reported so a run that silently stopped matching anything cannot
			// look like a speedup.
			b.ReportMetric(matched, "rows-kept")
		})
	}
}

// assertBenchStrategy fails a benchmark that did not take the strategy it is
// named for. Without it, a change that made one path unreachable would show up
// as the other path's numbers under both names, which is worse than no
// measurement at all.
func assertBenchStrategy(b *testing.B, stats ExecStats, membershipMax int) {
	b.Helper()
	if membershipMax == 1 {
		if stats.JoinLookups != 1 || stats.JoinProbes == 0 {
			b.Fatalf("wanted the lookup path, got %+v", stats)
		}
		return
	}
	if stats.JoinMemberships != 1 || stats.JoinProbes != 0 {
		b.Fatalf("wanted the membership path, got %+v", stats)
	}
}

// BenchmarkJoinMembershipPath is the collected-set strategy: one inner scan,
// then a search per outer row.
func BenchmarkJoinMembershipPath(b *testing.B) { benchmarkJoin(b, 1<<20, false) }

// BenchmarkJoinMembershipPathIndexed is the same strategy with a declared exact
// index on the outer join path, where the membership stops being a per-row
// search and becomes one probe per collected value plus a mask intersection.
func BenchmarkJoinMembershipPathIndexed(b *testing.B) { benchmarkJoin(b, 1<<20, true) }

// BenchmarkJoinLookupPath is the per-row keyed probe with no prefilter: no
// inner scan, no collected set, and a hash lookup plus an inner predicate
// evaluation on every outer row.
func BenchmarkJoinLookupPath(b *testing.B) { benchmarkJoin(b, 1, false) }

// BenchmarkJoinDefaultThreshold runs the shipped policy end to end, so the
// sweep shows what a caller who sets nothing actually gets at each point.
func BenchmarkJoinDefaultThreshold(b *testing.B) {
	for _, shape := range joinBenchShapes {
		b.Run(shape.name, func(b *testing.B) {
			db := joinBenchDatabase(b, shape.outerRows, shape.customers, shape.matching, false)
			catalog := db.Snapshot()
			q := Select(Count()).
				Join(JoinOn("customers", "customer", JoinKey).Where(Cmp("tier", Eq, "pro")))
			var e Exec
			warmExec(b, q, &e, FromDatabase(catalog, "orders"))
			chosen := "membership"
			if e.Stats.JoinLookups == 1 {
				chosen = "lookup"
				if e.Stats.JoinFilters == 1 {
					chosen = "lookup+filter"
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if err := q.RunInto(&e, FromDatabase(catalog, "orders")); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*shape.outerRows), "ns/outer-row")
			b.Log("chose the", chosen, "strategy")
		})
	}
}

// BenchmarkJoinTwoQueryFilter is the baseline: the same answer assembled by
// hand out of the two single-collection queries a caller has without a join. It
// runs the inner query, materializes its keys, builds an In predicate over
// them, and runs the outer query — which also means it pays a compile per
// execution, because the predicate is not known until the first query has
// answered.
//
// It is the honest comparison rather than a strawman: it uses the same
// membership primitive the join's own fast path uses, over the same snapshot,
// with the same warmed Exec storage. What the join saves is the materialization
// of the key list, the per-execution compile, and — past the threshold — the
// inner scan entirely.
func BenchmarkJoinTwoQueryFilter(b *testing.B) {
	for _, shape := range joinBenchShapes {
		b.Run(shape.name, func(b *testing.B) {
			db := joinBenchDatabase(b, shape.outerRows, shape.customers, shape.matching, false)
			catalog := db.Snapshot()
			customers, _ := catalog.Collection("customers")
			orders, _ := catalog.Collection("orders")

			innerQuery := Select(Path("name")).Where(Cmp("tier", Eq, "pro"))
			var innerExec, outerExec Exec
			keys := make([]any, 0, shape.customers)

			run := func() int {
				if err := innerQuery.RunInto(&innerExec, FromSnapshot(customers)); err != nil {
					b.Fatal(err)
				}
				keys = keys[:0]
				for r := 0; r < innerExec.Result.RowCount; r++ {
					text, _ := innerExec.Result.Columns[0].Cells[r].Text()
					keys = append(keys, text)
				}
				outerQuery := Select(Count()).Where(In("customer", keys...))
				if err := outerQuery.RunInto(&outerExec, FromSnapshot(orders)); err != nil {
					b.Fatal(err)
				}
				matched, _ := outerExec.Result.Columns[0].Cells[0].Float64()
				return int(matched)
			}

			matched := run()
			if matched == 0 {
				b.Fatal("the baseline must match rows, or it is measuring nothing")
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				run()
			}
			b.StopTimer()
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*shape.outerRows), "ns/outer-row")
			b.ReportMetric(float64(matched), "rows-kept")
		})
	}
}

// BenchmarkJoinThresholdSweep is the measurement joinMembershipMax's comment
// cites. It holds the collected set at a fixed size and walks the threshold
// across it, so each pair of adjacent points differs only in which strategy the
// binder chose for identical work. The prefilter is disabled throughout, so
// this sweep reports one decision rather than two.
func BenchmarkJoinThresholdSweep(b *testing.B) {
	for _, indexed := range []bool{false, true} {
		for _, matching := range []int{16, 64, 256, 512, 1024, 1536, 2048, 3072, 4096, 8192, 16384, 65536} {
			const outerRows = 20000
			customers := matching
			if customers < 1024 {
				customers = 1024
			}
			db := joinBenchDatabase(b, outerRows, customers, matching, indexed)
			catalog := db.Snapshot()
			q := Select(Count()).
				Join(JoinOn("customers", "customer", JoinKey).Where(Cmp("tier", Eq, "pro")))
			for _, strategy := range []struct {
				name          string
				membershipMax int
			}{{"membership", 1 << 20}, {"lookup", 1}} {
				name := fmt.Sprintf("indexed=%v/matching=%d/%s", indexed, matching, strategy.name)
				b.Run(name, func(b *testing.B) {
					var e Exec
					e.Options.JoinMembershipMax = strategy.membershipMax
					e.Options.JoinFilterScanRatio = -1
					warmExec(b, q, &e, FromDatabase(catalog, "orders"))
					assertBenchStrategy(b, e.Stats, strategy.membershipMax)
					b.ReportAllocs()
					b.ResetTimer()
					for b.Loop() {
						if err := q.RunInto(&e, FromDatabase(catalog, "orders")); err != nil {
							b.Fatal(err)
						}
					}
					b.StopTimer()
					b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*outerRows), "ns/outer-row")
				})
			}
		}
	}
}

// --- the semi-join reduction filter ----------------------------------------

// joinSelectiveDatabase builds the shape a semi-join reduction filter exists
// for: an outer collection whose keys are spread over a large inner collection
// of which only a fraction passes the inner filter. Most outer rows therefore
// name a customer that exists and is rejected — the population that costs a
// full lookup, decode, and predicate evaluation to say no to, and the one the
// filter answers from a single cache line.
func joinSelectiveDatabase(b testing.TB, outerRows, customers, matching int) *store.Database {
	db := &store.Database{}
	orders, err := db.CreateCollection("orders", store.Options{})
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < outerRows; i++ {
		// Spread over the whole customer space with a stride coprime to it, so
		// the references are scattered across chunks rather than clustered.
		doc := fmt.Sprintf(`{"id":%d,"customer":"c%d","total":%d}`, i, (i*7919)%customers, i%997)
		if _, err := orders.Put(fmt.Sprintf("o%d", i), []byte(doc)); err != nil {
			b.Fatal(err)
		}
	}
	buyers, err := db.CreateCollection("customers", store.Options{})
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < customers; i++ {
		tier := "free"
		if stride := customers / max(matching, 1); stride > 0 && i%stride == 0 {
			tier = "pro"
		}
		doc := fmt.Sprintf(`{"tier":%q,"seat":%d,"name":"c%d"}`, tier, i, i)
		if _, err := buyers.Put(fmt.Sprintf("c%d", i), []byte(doc)); err != nil {
			b.Fatal(err)
		}
	}
	return db
}

// BenchmarkJoinCostModel measures the two per-row costs joinBloomScanRatio
// compares. Nothing here is a join: each sub-benchmark isolates one of the two
// operations so the ratio is a measurement rather than a subtraction of two
// numbers that also contain everything else. It is swept over the inner
// collection's size because the probe is the cache-hostile half and does not
// stay flat as that grows.
func BenchmarkJoinCostModel(b *testing.B) {
	const outerRows = 20000
	for _, customers := range []int{1000, 10000, 100000, 400000} {
		db := joinSelectiveDatabase(b, outerRows, customers, customers/10)
		catalog := db.Snapshot()
		orders, _ := catalog.Collection("orders")
		buyers, _ := catalog.Collection("customers")

		// The inner side's own scan, which is what building a filter costs:
		// read every candidate row, evaluate the inner predicate, and reduce.
		// The filter's insert is one hash and eight ORs on top of this.
		b.Run(fmt.Sprintf("customers=%d/inner-row-scanned", customers), func(b *testing.B) {
			q := Select(Count()).Where(Cmp("tier", Eq, "pro"))
			var e Exec
			warmExec(b, q, &e, FromSnapshot(buyers))
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if err := q.RunInto(&e, FromSnapshot(buyers)); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*customers), "ns/inner-row")
		})

		// The outer scan with no join at all: the floor every joined execution
		// pays, and the baseline the probe cost is measured above.
		b.Run(fmt.Sprintf("customers=%d/outer-row-scanned", customers), func(b *testing.B) {
			q := Select(Count()).Where(Cmp("total", Ge, 0))
			var e Exec
			warmExec(b, q, &e, FromSnapshot(orders))
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if err := q.RunInto(&e, FromSnapshot(orders)); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*outerRows), "ns/outer-row")
		})

		// The same outer scan with an unfiltered lookup join on top. The
		// difference between this and outer-row-scanned is one probe.
		b.Run(fmt.Sprintf("customers=%d/outer-row-probed", customers), func(b *testing.B) {
			q := Select(Count()).Where(Cmp("total", Ge, 0)).
				Join(JoinOn("customers", "customer", JoinKey).Where(Cmp("tier", Eq, "pro")))
			var e Exec
			e.Options.JoinMembershipMax = 1
			e.Options.JoinFilterScanRatio = -1
			warmExec(b, q, &e, FromDatabase(catalog, "orders"))
			if e.Stats.JoinFilters != 0 {
				b.Fatalf("this measurement needs an unfiltered lookup, got %+v", e.Stats)
			}
			if e.Stats.JoinProbes != outerRows {
				b.Fatalf("wanted a probe per outer row, got %d", e.Stats.JoinProbes)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if err := q.RunInto(&e, FromDatabase(catalog, "orders")); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*outerRows), "ns/outer-row")
		})
	}
}

// BenchmarkJoinBloomPrefilter is the measurement the filter has to justify
// itself with: the same selective join under the shipped policy and with the
// filter forced off, swept over how much of the inner collection passes the
// inner filter.
//
// The "policy" arm is deliberately not forced. What is being measured is the
// adaptive system's actual output, including its decision to abandon a filter
// it has already started building — an arm that forced the filter on would
// report a cost the engine never pays and would hide the abandon entirely. Each
// arm reports whether a filter survived, so a run where the policy changed its
// mind is visible rather than inferred.
func BenchmarkJoinBloomPrefilter(b *testing.B) {
	const outerRows = 20000
	for _, shape := range []struct {
		name                string
		customers, matching int
	}{
		{"selectivity=1%", 40000, 400},
		{"selectivity=10%", 40000, 4000},
		{"selectivity=50%", 40000, 20000},
		{"selectivity=100%", 40000, 40000},
	} {
		db := joinSelectiveDatabase(b, outerRows, shape.customers, shape.matching)
		catalog := db.Snapshot()
		q := Select(Count()).
			Join(JoinOn("customers", "customer", JoinKey).Where(Cmp("tier", Eq, "pro")))
		for _, arm := range []struct {
			name  string
			ratio int
		}{{"policy", 0}, {"filter-disabled", -1}} {
			b.Run(shape.name+"/"+arm.name, func(b *testing.B) {
				var e Exec
				e.Options.JoinMembershipMax = 1 // force the lookup strategy
				e.Options.JoinFilterScanRatio = arm.ratio
				warmExec(b, q, &e, FromDatabase(catalog, "orders"))
				if arm.ratio < 0 && e.Stats.JoinFilters != 0 {
					b.Fatalf("the disabled arm built a filter anyway: %+v", e.Stats)
				}
				matched, _ := e.Result.Columns[0].Cells[0].Float64()
				filters, probes := e.Stats.JoinFilters, e.Stats.JoinProbes
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					if err := q.RunInto(&e, FromDatabase(catalog, "orders")); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*outerRows), "ns/outer-row")
				b.ReportMetric(matched, "rows-kept")
				b.ReportMetric(float64(probes), "probes")
				b.ReportMetric(float64(filters), "filters")
			})
		}
	}
}

// warmExec runs q twice so the Exec reaches its high-water capacity before a
// benchmark starts timing. Without it the first iterations measure the growth
// of every column, posting, and result buffer rather than the operation, which
// on a large fixture is the whole measurement.
func warmExec(b *testing.B, q *Query, e *Exec, src Source) {
	b.Helper()
	for range 2 {
		if err := q.RunInto(e, src); err != nil {
			b.Fatal(err)
		}
	}
}

// --- fan-out: the inner join ------------------------------------------------

// benchFanOutDatabase builds a users/orders pair with a controlled fan-out
// ratio: orders spread evenly over users, so each user has exactly
// orderRows/users partners and the result is that many times the driving
// collection's size.
func benchFanOutDatabase(b testing.TB, orderRows, users int) *store.Database {
	db := &store.Database{}
	people, err := db.CreateCollection("users", store.Options{})
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < users; i++ {
		if _, err := people.Put(fmt.Sprintf("u%d", i),
			[]byte(fmt.Sprintf(`{"id":%d,"name":"user-%d"}`, i, i))); err != nil {
			b.Fatal(err)
		}
	}
	orders, err := db.CreateCollection("orders", store.Options{})
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < orderRows; i++ {
		if _, err := orders.Put(fmt.Sprintf("o%d", i), []byte(fmt.Sprintf(
			`{"user_id":%d,"seat":%d,"total":%d}`, i%users, i, i%997))); err != nil {
			b.Fatal(err)
		}
	}
	return db
}

// BenchmarkFanOutJoin is the cost curve: the same 20,000 joined rows spread
// over fewer and fewer driving rows, so the fan-out ratio rises while the total
// work the build side does stays fixed.
//
// The per-pair figure is the one to read. It is what an inner join costs per
// row it returns, and holding it roughly flat as the ratio climbs is the claim
// the expansion has to support: the build is paid once whatever the ratio, and
// what scales is the gather, which is one sparse read per output row.
func BenchmarkFanOutJoin(b *testing.B) {
	const orderRows = 20000
	for _, fan := range []int{1, 4, 16} {
		users := orderRows / fan
		db := benchFanOutDatabase(b, orderRows, users)
		catalog := db.Snapshot()
		for _, shape := range []struct {
			name  string
			query *Query
		}{
			{"project both sides", Select(Path("name"), Path("o.total")).
				Join(JoinOn("orders", "id", "user_id").As("o"))},
			{"count pairs", Select(Count()).
				Join(JoinOn("orders", "id", "user_id").As("o"))},
			{"aggregate a joined column", Select(Count(), Sum("o.total")).
				Join(JoinOn("orders", "id", "user_id").As("o"))},
		} {
			b.Run(fmt.Sprintf("fan=%d/%s", fan, shape.name), func(b *testing.B) {
				var e Exec
				warmExec(b, shape.query, &e, FromDatabase(catalog, "users"))
				if e.Stats.JoinPairs != orderRows {
					b.Fatalf("want one pair per joined row (%d), got %+v", orderRows, e.Stats)
				}
				pairs := e.Stats.JoinPairs
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					if err := shape.query.RunInto(&e, FromDatabase(catalog, "users")); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(pairs), "ns/pair")
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(users), "ns/driving-row")
			})
		}
	}
}

// BenchmarkLeftJoinFanOut measures the exact incremental operator cost against
// the established inner-join path, both when every driving row matches and
// when half of them require null extension.
func BenchmarkLeftJoinFanOut(b *testing.B) {
	for _, shape := range []struct {
		name   string
		orders int
		users  int
	}{
		{name: "all-matched", orders: 20_000, users: 10_000},
		{name: "half-unmatched", orders: 5_000, users: 10_000},
	} {
		db := joinFanOutDatabase(b, shape.orders, shape.users)
		catalog := db.Snapshot()
		for _, operator := range []struct {
			name string
			join Join
		}{
			{name: "inner", join: JoinOn("orders", "id", "user_id").As("o")},
			{name: "left", join: LeftJoinOn("orders", "id", "user_id").As("o")},
		} {
			b.Run(shape.name+"/"+operator.name, func(b *testing.B) {
				q := Select(Path("id"), Path("o.seat")).Join(operator.join)
				var e Exec
				if err := q.RunInto(&e, FromDatabase(catalog, "users")); err != nil {
					b.Fatal(err)
				}
				rows := e.Result.RowCount
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					if err := q.RunInto(&e, FromDatabase(catalog, "users")); err != nil {
						b.Fatal(err)
					}
				}
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*rows), "ns/output-row")
			})
		}
	}
}

// BenchmarkFanOutAgainstSemiJoin is the regression guard the planner rule
// exists for: the same clause and the same collections answered as a filter and
// as an inner join, at a fan-out ratio of one so both return the same number of
// rows and the difference is the operator rather than the output.
//
// The joined side's size is swept because the two operators have different
// weak points and one point would misrepresent both. A semi-join on a joined
// FIELD cannot fall back to a keyed probe, so it collects the whole matching
// set and searches it per driving row — cheap while that set is small, and the
// membership's unfavourable regime once it is not. A fan-out always builds a
// hash multimap, which costs the same at either size. So the filter is much
// the cheaper operator on a small joined side, and on a large one the build
// overtakes it; that crossing is a property of the semi-join's strategy choice
// rather than of fan-out, and it is measured here rather than asserted.
func BenchmarkFanOutAgainstSemiJoin(b *testing.B) {
	const rows = 20000
	for _, joined := range []int{256, 20000} {
		db := benchFanOutDatabase(b, joined, rows)
		catalog := db.Snapshot()
		for _, arm := range []struct {
			name  string
			query *Query
		}{
			{"semi-join, filter only", Select(Count()).
				Join(JoinOn("orders", "id", "user_id"))},
			{"inner join, same clause aliased", Select(Count()).
				Join(JoinOn("orders", "id", "user_id").As("o"))},
			{"inner join projecting the joined side", Select(Path("name"), Path("o.total")).
				Join(JoinOn("orders", "id", "user_id").As("o"))},
		} {
			b.Run(fmt.Sprintf("joined=%d/%s", joined, arm.name), func(b *testing.B) {
				var e Exec
				warmExec(b, arm.query, &e, FromDatabase(catalog, "users"))
				built := e.Stats.JoinBuilds
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					if err := arm.query.RunInto(&e, FromDatabase(catalog, "users")); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(rows), "ns/driving-row")
				b.ReportMetric(float64(built), "builds")
			})
		}
	}
}
