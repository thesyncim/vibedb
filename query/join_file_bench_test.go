package query

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/collectionname"
	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
)

// Durable join measurement.
//
// Every threshold in join.go and join_bloom.go was measured against the heap
// backend, where an inner row is a pointer dereference into a live Go object
// and a probe is a hash lookup plus a document decode. None of that transfers
// unexamined to a page-backed collection, where a probe can fault a page and
// the scan reads through the page cache. These benchmarks re-derive the two
// quantities the strategy choice compares — the cost of scanning one inner row
// and the cost of probing one outer row — against a durable inner side, so
// joinBloomScanRatio can be checked rather than assumed.
//
// They deliberately measure warm and cold. A warm run reads a resident page
// cache and is the steady state a served query sees; a cold run reopens the
// database so every read is real I/O, and is where the Bloom filter's whole
// argument lives — it converts a page read into a cache-line test.

// durableBenchOptions is the geometry the join benchmarks measure over.
// ResidentBytes is generous so a warm arm really is warm; a cold arm gets its
// cold cache from reopening the database, not from starving it.
func durableBenchOptions() durable.Options {
	return durable.Options{
		Collection:    store.Options{ChunkDocuments: 64},
		ResidentBytes: 64 << 20,
	}
}

// joinBenchDurableDatabase writes joinBenchDatabase's corpus into a durable
// database directory, so the two backends are measured over identical data.
//
// Each collection is bulk-loaded from a heap one through CreateFrom rather than
// written a document at a time. That is one commit per collection instead of
// one per document, which is the difference between a corpus that takes a
// second to build and one that takes minutes — and the resulting file is the
// same file, so nothing about what is being measured changes.
func joinBenchDurableDatabase(b testing.TB, dir string, outerRows, customers, matching int) {
	b.Helper()
	options := durableBenchOptions()
	load := func(name string, fill func(*store.Collection)) {
		source := &store.Collection{}
		fill(source)
		filename, ok := collectionname.Encode(name)
		if !ok {
			b.Fatalf("invalid collection name %q", name)
		}
		file, err := os.Create(filepath.Join(dir, filename))
		if err != nil {
			b.Fatal(err)
		}
		defer func() { _ = file.Close() }()
		if _, err := durable.CreateFromPrimary(source, file, options); err != nil {
			b.Fatal(err)
		}
	}
	load("orders", func(c *store.Collection) {
		for i := 0; i < outerRows; i++ {
			doc := fmt.Appendf(nil, `{"id":%d,"customer":"c%d","total":%d,"region":"r%d"}`,
				i, i%customers, i%997, i%8)
			if _, err := c.Put(fmt.Sprintf("o%d", i), doc); err != nil {
				b.Fatal(err)
			}
		}
	})
	load("customers", func(c *store.Collection) {
		for i := 0; i < customers; i++ {
			// Spread the matching customers evenly rather than putting them in a
			// prefix, for the reason joinBenchDatabase gives: a prefix makes any
			// sample of the first rows scanned unrepresentative of the whole,
			// which is precisely what the mid-scan abandon check reads.
			tier := "free"
			if stride := customers / max(matching, 1); stride > 0 && i%stride == 0 {
				tier = "pro"
			}
			doc := fmt.Appendf(nil, `{"tier":%q,"seat":%d,"name":"c%d"}`, tier, i, i)
			if _, err := c.Put(fmt.Sprintf("c%d", i), doc); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// durableJoinCase is one prepared corpus, reusable across sub-benchmarks. The
// build is far more expensive than the measurement here, so a case is built
// once and reopened per cold run rather than rewritten.
type durableJoinCase struct {
	dir       string
	outerRows int
}

func prepareDurableJoinCase(b testing.TB, outerRows, customers, matching int) durableJoinCase {
	b.Helper()
	dir, err := os.MkdirTemp("", "vibejson-join-bench")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = os.RemoveAll(dir) })
	joinBenchDurableDatabase(b, dir, outerRows, customers, matching)
	return durableJoinCase{dir: dir, outerRows: outerRows}
}

// open reopens the case's directory. A freshly opened database has an empty
// page cache, which is what makes the first execution over it a cold one.
func (c durableJoinCase) open(b testing.TB) *durable.Database {
	b.Helper()
	db, err := durable.OpenDatabase(c.dir, durable.DatabaseOptions{Options: durableBenchOptions()})
	if err != nil {
		b.Fatal(err)
	}
	return db
}

func durableJoinQuery() *Query {
	return Select(Path("id"), Path("total")).
		Join(JoinOn("customers", "customer", JoinKey).Where(Cmp("tier", Eq, "pro")))
}

// runDurableJoin executes q once against a fresh cut of db and returns the row
// count, so a benchmark loop cannot be optimized away and a misconfigured case
// that selects nothing is visible rather than fast.
func runDurableJoin(b testing.TB, db *durable.Database, e *Exec, q *Query) int {
	b.Helper()
	catalog, err := db.Snapshot()
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = catalog.Close() }()
	if err := q.RunInto(e, FromFileDatabase(catalog, "orders")); err != nil {
		b.Fatal(err)
	}
	return e.Result.RowCount
}

// BenchmarkDurableJoinWarm measures the steady state: a resident page cache and
// a reused Exec, which is the shape a served query loop has.
func BenchmarkDurableJoinWarm(b *testing.B) {
	for _, shape := range joinBenchShapes {
		for _, strategy := range joinStrategies {
			name := fmt.Sprintf("%s/%s", shape.name, strategy.name)
			b.Run(name, func(b *testing.B) {
				c := prepareDurableJoinCase(b, shape.outerRows, shape.customers, shape.matching)
				db := c.open(b)
				defer func() { _ = db.Close() }()
				q := durableJoinQuery()
				var e Exec
				e.Options.JoinMembershipMax = strategy.membershipMax
				rows := runDurableJoin(b, db, &e, q)
				b.ReportMetric(float64(rows), "rows")
				b.ResetTimer()
				for range b.N {
					runDurableJoin(b, db, &e, q)
				}
				b.StopTimer()
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(shape.outerRows),
					"ns/outer-row")
			})
		}
	}
}

// BenchmarkDurableJoinCold reopens the database inside the timed region, so
// every page the join touches is read from the device.
//
// This is the measurement the Bloom filter exists for. A cold probe is a key
// tree walk plus a chunk directory walk plus a document page read; a filter
// rejection is a hash and two cache-line tests. The ratio between those is what
// decides whether the filter's scan can repay itself, and it is nothing like
// the heap's.
func BenchmarkDurableJoinCold(b *testing.B) {
	for _, shape := range joinBenchShapes {
		for _, strategy := range joinStrategies {
			name := fmt.Sprintf("%s/%s", shape.name, strategy.name)
			b.Run(name, func(b *testing.B) {
				c := prepareDurableJoinCase(b, shape.outerRows, shape.customers, shape.matching)
				q := durableJoinQuery()
				b.ResetTimer()
				for range b.N {
					db := c.open(b)
					var e Exec
					e.Options.JoinMembershipMax = strategy.membershipMax
					runDurableJoin(b, db, &e, q)
					if err := db.Close(); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(shape.outerRows),
					"ns/outer-row")
			})
		}
	}
}

// BenchmarkDurableJoinCostModel is BenchmarkJoinCostModel against durable
// collections: it separates the two per-row costs joinBloomScanRatio compares,
// using the same three-arm method, so the constant can be re-derived rather
// than inherited.
//
// The arms are the heap benchmark's, and the arithmetic is the same. The inner
// scan is the inner collection queried alone, which is what building a filter
// costs per candidate row. The outer scan with no join is the floor every
// joined execution pays. The outer scan with an unfiltered lookup join on top
// differs from that floor by exactly one probe per row, so the difference is
// the probe cost. The break-even ratio is probe cost divided by scan cost.
//
// Every arm runs warm, over a resident page cache, because that is the regime
// the shipped threshold has to be right in — a cold execution reads the inner
// side once either way and the strategy choice moves much less.
func BenchmarkDurableJoinCostModel(b *testing.B) {
	const outerRows = 20000
	for _, customers := range []int{1000, 10000, 100000} {
		c := prepareDurableJoinCase(b, outerRows, customers, customers/10)
		db := c.open(b)
		orders, _ := db.Collection("orders")
		buyers, _ := db.Collection("customers")

		b.Run(fmt.Sprintf("customers=%d/inner-row-scanned", customers), func(b *testing.B) {
			q := Select(Count()).Where(Cmp("tier", Eq, "pro"))
			snapshot, err := buyers.Snapshot()
			if err != nil {
				b.Fatal(err)
			}
			defer func() { _ = snapshot.Close() }()
			var e Exec
			if err := q.RunInto(&e, FromFile(snapshot)); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if err := q.RunInto(&e, FromFile(snapshot)); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*customers), "ns/inner-row")
		})

		b.Run(fmt.Sprintf("customers=%d/outer-row-scanned", customers), func(b *testing.B) {
			q := Select(Count()).Where(Cmp("total", Ge, 0))
			snapshot, err := orders.Snapshot()
			if err != nil {
				b.Fatal(err)
			}
			defer func() { _ = snapshot.Close() }()
			var e Exec
			if err := q.RunInto(&e, FromFile(snapshot)); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if err := q.RunInto(&e, FromFile(snapshot)); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*outerRows), "ns/outer-row")
		})

		b.Run(fmt.Sprintf("customers=%d/outer-row-probed", customers), func(b *testing.B) {
			q := Select(Count()).Where(Cmp("total", Ge, 0)).
				Join(JoinOn("customers", "customer", JoinKey).Where(Cmp("tier", Eq, "pro")))
			catalog, err := db.Snapshot()
			if err != nil {
				b.Fatal(err)
			}
			defer func() { _ = catalog.Close() }()
			var e Exec
			e.Options.JoinMembershipMax = 1
			e.Options.JoinFilterScanRatio = -1
			if err := q.RunInto(&e, FromFileDatabase(catalog, "orders")); err != nil {
				b.Fatal(err)
			}
			if e.Stats.JoinFilters != 0 {
				b.Fatalf("this measurement needs an unfiltered lookup, got %+v", e.Stats)
			}
			if e.Stats.JoinProbes != outerRows {
				b.Fatalf("wanted a probe per outer row, got %d", e.Stats.JoinProbes)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if err := q.RunInto(&e, FromFileDatabase(catalog, "orders")); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*outerRows), "ns/outer-row")
		})
		if err := db.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDurableJoinBloomPrefilter measures the filter against not building
// one, on a lookup binding, warm and cold. It is the direct test of the claim
// that on a durable backend the filtered path should dominate: a rejection that
// costs a cache line replaces a probe that costs page reads.
func BenchmarkDurableJoinBloomPrefilter(b *testing.B) {
	for _, selectivity := range []int{1, 10, 50} {
		inner := 50000
		c := prepareDurableJoinCase(b, 20000, inner, inner*selectivity/100)
		q := durableJoinQuery()
		for _, filtered := range []bool{false, true} {
			mode := "unfiltered"
			ratio := -1
			if filtered {
				mode, ratio = "filtered", 0
			}
			b.Run(fmt.Sprintf("pro=%d%%/%s/warm", selectivity, mode), func(b *testing.B) {
				db := c.open(b)
				defer func() { _ = db.Close() }()
				var e Exec
				e.Options.JoinMembershipMax = 1
				e.Options.JoinFilterScanRatio = ratio
				runDurableJoin(b, db, &e, q)
				b.ResetTimer()
				for range b.N {
					runDurableJoin(b, db, &e, q)
				}
				b.StopTimer()
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/20000.0, "ns/outer-row")
				// Whether a filter actually survived is reported rather than
				// assumed. The policy may build one and then abandon it on
				// observed selectivity, in which case the arm paid for the scan
				// and probed every row anyway — a cost profile identical to a
				// filter that simply did not help, and one that would be
				// invisible if only the timing were reported.
				b.ReportMetric(float64(e.Stats.JoinFilters), "filters")
				b.ReportMetric(float64(e.Stats.JoinFilterRejected), "rejected")
			})
			b.Run(fmt.Sprintf("pro=%d%%/%s/cold", selectivity, mode), func(b *testing.B) {
				var filters int
				var rejected uint64
				b.ResetTimer()
				for range b.N {
					db := c.open(b)
					var e Exec
					e.Options.JoinMembershipMax = 1
					e.Options.JoinFilterScanRatio = ratio
					runDurableJoin(b, db, &e, q)
					filters, rejected = e.Stats.JoinFilters, e.Stats.JoinFilterRejected
					if err := db.Close(); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/20000.0, "ns/outer-row")
				b.ReportMetric(float64(filters), "filters")
				b.ReportMetric(float64(rejected), "rejected")
			})
		}
	}
}

// BenchmarkDurableJoinFilterCrossover sweeps the inner candidate count against
// a fixed driving collection with the filter forced on and forced off, which is
// the crossover joinFileBloomScanRatio is a floor of, measured directly rather
// than derived from two separately measured per-row costs.
//
// The ratio the constant expresses is inner candidates per driving row, so the
// sweep is exactly that quantity: 20,000 driving rows against inner sides from
// 10,000 to 80,000, which is a ratio of 0.5 to 4. The arm that wins changes
// somewhere in that range and the point where it changes is the number the
// constant has to sit below.
//
// Inner selectivity is held at 1%, the case most favourable to the filter,
// because a filter that cannot pay for itself there cannot pay for itself
// anywhere: it is the selectivity at which a filter rejects the most probes.
// Measuring the crossover at the filter's best case is what makes a floor taken
// from it safe at every other.
func BenchmarkDurableJoinFilterCrossover(b *testing.B) {
	const outerRows = 20000
	for _, inner := range []int{10000, 20000, 40000, 80000} {
		c := prepareDurableJoinCase(b, outerRows, inner, max(inner/100, 1))
		q := durableJoinQuery()
		for _, forced := range []bool{false, true} {
			mode, ratio := "unfiltered", -1
			if forced {
				// A ratio wide enough that the gate always admits, so this arm
				// measures the filter's cost rather than the policy's decision.
				mode, ratio = "forced-filter", 1<<20
			}
			b.Run(fmt.Sprintf("ratio=%.1f/%s", float64(inner)/outerRows, mode), func(b *testing.B) {
				db := c.open(b)
				defer func() { _ = db.Close() }()
				var e Exec
				e.Options.JoinMembershipMax = 1
				e.Options.JoinFilterScanRatio = ratio
				runDurableJoin(b, db, &e, q)
				if forced && e.Stats.JoinFilters != 1 {
					b.Fatalf("the forced arm built no filter: %+v", e.Stats)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					runDurableJoin(b, db, &e, q)
				}
				b.StopTimer()
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(outerRows),
					"ns/outer-row")
			})
		}
	}
}
