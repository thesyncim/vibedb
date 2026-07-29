package competitive

import (
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"runtime"
	"slices"
	"strconv"
	"testing"
)

var (
	corpusSize   = flag.Int("corpus", CorpusSize, "documents in the shared corpus")
	corpusCard   = flag.String("cardinality", "low", "corpus variant: low (the shipped, ~92% redundant one) or high")
	keepCoreside = flag.Bool("coresident", false,
		"keep every engine's fixture loaded at once, the old behaviour. Off by default, because the bias it "+
			"introduces has opposite signs for the two sides of the comparison: releasing foreign fixtures speeds "+
			"up both vibejson engines, whose working sets the GC has to scan, and slows down all four competitors, "+
			"whose working sets live in mmaps and off-heap arenas it never touches. Keep it on to measure that")
)

var (
	docs        []Doc
	probeIdx    []int
	cardinality Cardinality
)

func TestMain(m *testing.M) {
	flag.Parse()
	var err error
	if cardinality, err = ParseCardinality(*corpusCard); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	docs = CorpusOf(*corpusSize, cardinality)
	// A fixed permutation of key ordinals, so point operations do not walk the
	// corpus in storage order and get an unrepresentative cache hit rate.
	rng := rand.New(rand.NewPCG(7, 11))
	probeIdx = rng.Perm(len(docs))
	code := m.Run()
	closeFixtures()
	os.Exit(code)
}

// fixtureKey identifies a loaded engine instance shared across benchmark
// iterations and -count repetitions.
type fixtureKey struct {
	name       string
	durability DurabilityMode
	indexed    bool
	purpose    string
}

type fixture struct {
	engine Engine
	dir    string
}

var fixtures = map[fixtureKey]*fixture{}

func closeFixtures() {
	for k, f := range fixtures {
		_ = f.engine.Close()
		_ = os.RemoveAll(f.dir)
		delete(fixtures, k)
	}
	runtime.GC()
}

// closeForeignFixtures releases every loaded engine that is not the one about
// to be measured.
//
// This is measurement correctness, not tidiness. Fixtures used to accumulate in
// Factories() order and were never released, so by the time the last engine in
// a row ran, five other engines' working sets were resident, the Go heap was
// several hundred megabytes larger, and the GC had more to scan. The effect was
// not uniform and therefore did not cancel: vibejson-heap's point read moved by
// tens of percent between an isolated run and a full one, while bbolt — whose
// working set is an mmap the Go heap never scans — barely moved. The smallest
// number in the table was the most run-mode-sensitive one, which is the worst
// possible place for a bias to sit. `-coresident` restores the old behaviour so
// the effect stays measurable; RESULTS.md reports the current pair.
//
// This makes a full `-bench=.` run close to the isolated figure. It does not
// make it identical: allocator state and page-cache history still carry over.
// Published figures come from one process per engine — see the README.
func closeForeignFixtures(name string) {
	if *keepCoreside {
		return
	}
	released := false
	for k, f := range fixtures {
		if k.name == name {
			continue
		}
		_ = f.engine.Close()
		_ = os.RemoveAll(f.dir)
		delete(fixtures, k)
		released = true
	}
	if released {
		runtime.GC()
		runtime.GC()
	}
}

// newLoaded builds one engine over a private directory and loads the shared
// package-global corpus. The caller owns the returned cleanup.
func newLoaded(tb testing.TB, factory Factory, cfg Config) (Engine, string, func()) {
	tb.Helper()
	return newLoadedCorpus(tb, factory, cfg, docs)
}

// newLoadedCorpus is newLoaded over an explicitly supplied corpus rather than
// the package-global docs. It exists for the one arm that cannot use the shared
// 100k fixture: the indexed vibejson-durable engine, whose on-graph exact index
// caps one physical index's postings at a single term leaf and must be sized
// below that ceiling (see TestFullEquivalenceIndexedDurable).
func newLoadedCorpus(tb testing.TB, factory Factory, cfg Config, corpus []Doc) (Engine, string, func()) {
	tb.Helper()
	dir, err := tempDir(factory.Name)
	if err != nil {
		tb.Fatal(err)
	}
	cfg.Dir = dir
	if cfg.CacheBytes == 0 {
		cfg.CacheBytes = DefaultCacheBytes
	}
	e, err := factory.New(cfg)
	if err != nil {
		_ = os.RemoveAll(dir)
		tb.Fatal(err)
	}
	if err := e.Load(corpus); err != nil {
		_ = e.Close()
		_ = os.RemoveAll(dir)
		tb.Fatal(err)
	}
	return e, dir, func() {
		_ = e.Close()
		_ = os.RemoveAll(dir)
	}
}

// loadedEngine returns a corpus-loaded engine, building it on first use. The
// "purpose" field keeps the mutating workloads off the instances the read-only
// workloads measure.
//
// Only read-only workloads may share a fixture. A workload that writes gets a
// fresh one per repetition — see BenchmarkPointWrite.
func loadedEngine(
	tb testing.TB,
	name string,
	durability DurabilityMode,
	indexed bool,
	purpose string,
) Engine {
	tb.Helper()
	closeForeignFixtures(name)
	key := fixtureKey{
		name: name, durability: durability, indexed: indexed, purpose: purpose,
	}
	if f, ok := fixtures[key]; ok {
		return f.engine
	}
	factory, ok := FactoryNamed(name)
	if !ok {
		tb.Fatalf("unknown engine %q", name)
	}
	e, dir, _ := newLoaded(tb, factory, Config{
		Durability: durability,
		Indexed:    indexed,
	})
	fixtures[key] = &fixture{engine: e, dir: dir}
	return e
}

// BenchmarkBulkLoad measures loading the whole corpus into an empty engine
// through that engine's bulk path: one bbolt write transaction, one Badger
// WriteBatch, one Pebble batch, one SQLite transaction over a prepared
// INSERT, and durable.CreateFrom for store/durable — which has to materialise
// the whole heap collection through store.Builder first, and pays for that
// inside the measurement.
//
// It runs with and without a secondary index over the filter field. The
// unindexed row alone was the whole bulk-load report for a while, which means
// the cost of building the index that BenchmarkIndexedFilter then measures the
// benefit of was charged to nobody. An engine's indexed-filter win has to be
// read against its own indexed-load cost, not against its unindexed one.
//
// One b.N iteration is one full corpus load, so ns/op is the total wall time.
func BenchmarkBulkLoad(b *testing.B) {
	for _, indexed := range []bool{false, true} {
		for _, factory := range Factories() {
			for _, durability := range BenchmarkDurabilityModes(factory.Name) {
				if indexed && !IndexCapable(factory.Name) {
					continue
				}
				b.Run(fmt.Sprintf(
					"%s/durability=%s/indexed=%v",
					factory.Name, durability, indexed,
				), func(b *testing.B) {
					closeForeignFixtures("")
					b.ReportAllocs()
					for b.Loop() {
						b.StopTimer()
						dir, err := tempDir(factory.Name)
						if err != nil {
							b.Fatal(err)
						}
						e, err := factory.New(Config{
							Dir: dir, Durability: durability, Indexed: indexed,
							CacheBytes: DefaultCacheBytes,
						})
						if err != nil {
							b.Fatal(err)
						}
						b.StartTimer()

						if err := e.Load(docs); err != nil {
							b.Fatal(err)
						}

						b.StopTimer()
						_ = e.Close()
						_ = os.RemoveAll(dir)
						b.StartTimer()
					}
					b.ReportMetric(float64(len(docs)), "docs/op")
				})
			}
		}
	}
}

// BenchmarkBulkLoadVariants isolates three choices that are easy to conflate
// with store/durable: verbatim versus compact bulk output, mutation replay,
// and leaving BufferCount at its default.
//
// Every variant is measured at more than one corpus size, because the previous
// version of this benchmark ran a 5,000-document build and then invited
// comparison with BenchmarkBulkLoad's 100,000-document row on the strength of a
// comment asserting that ns/doc was stable at 5,000. That was asserted, never
// measured, and it is exactly the kind of assertion a bulk builder with
// per-build fixed costs (a shape template, a value dictionary, a key
// directory) is most likely to violate. The sizes are now a measured column:
// read them against each other before comparing any of them with anything else.
//
// putloop-defaults runs only at the smallest size on purpose. At the default
// BufferCount a single Put costs milliseconds, so a full-corpus replay is
// minutes per iteration; the tuned variants are measured at that size too, so
// the defaults comparison is like-for-like.
func BenchmarkBulkLoadVariants(b *testing.B) {
	sizes := []int{1000, 5000, len(docs)}
	// -corpus can coincide with a fixed size; a duplicated b.Run name would
	// silently become "n=5000#01" and be read as a second sample.
	sizes = slices.Compact(slices.Sorted(slices.Values(sizes)))
	variants := []struct {
		name    string
		cfg     Config
		maxSize int // 0 means every size
	}{
		{name: "bulk-verbatim-tuned", cfg: Config{}},
		{name: "bulk-compact-tuned", cfg: Config{Compact: true}},
		{name: "putloop-tuned", cfg: Config{PutLoop: true}},
		{name: "putloop-defaults", cfg: Config{PutLoop: true, Untuned: true}, maxSize: 1000},
	}
	for _, v := range variants {
		for _, size := range sizes {
			if size > len(docs) {
				continue
			}
			if v.maxSize != 0 && size > v.maxSize {
				continue
			}
			subset := docs[:size]
			b.Run(fmt.Sprintf("vibejson-durable/%s/n=%d", v.name, size), func(b *testing.B) {
				closeForeignFixtures("")
				b.ReportAllocs()
				for b.Loop() {
					b.StopTimer()
					dir, err := tempDir("durable")
					if err != nil {
						b.Fatal(err)
					}
					cfg := v.cfg
					cfg.Dir = dir
					cfg.CacheBytes = DefaultCacheBytes
					e, err := newVibeDurable(cfg)
					if err != nil {
						b.Fatal(err)
					}
					b.StartTimer()

					if err := e.Load(subset); err != nil {
						b.Fatal(err)
					}

					b.StopTimer()
					_ = e.Close()
					_ = os.RemoveAll(dir)
					b.StartTimer()
				}
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*size), "ns/doc")
			})
		}
	}
}

// BenchmarkPointRead reads one document by key. Reads carry no durability
// setting, so every engine runs in its buffered configuration.
func BenchmarkPointRead(b *testing.B) {
	for _, factory := range Factories() {
		b.Run(factory.Name, func(b *testing.B) {
			e := loadedEngine(
				b, factory.Name, DurabilityDefault, false, "read",
			)
			buf := make([]byte, 0, 512)
			i := 0
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				out, err := e.Get(buf[:0], docs[probeIdx[i]].Key)
				if err != nil {
					b.Fatal(err)
				}
				if len(out) == 0 {
					b.Fatal("empty document")
				}
				buf = out
				i++
				if i == len(probeIdx) {
					i = 0
				}
			}
		})
	}
}

// BenchmarkVibeDurableReadFormat keeps the space-saving compact bulk format
// honest by pairing it with the same point and all-byte scan loops as the
// default verbatim format. Compact is not a competitor engine and must not be
// mixed into the cross-engine table as though it were the mutable default.
// This row exists specifically to prevent a smaller bulk artifact from being
// promoted without paying its measured read cost.
func BenchmarkVibeDurableReadFormat(b *testing.B) {
	for _, format := range []struct {
		name    string
		compact bool
	}{
		{name: "verbatim"},
		{name: "compact", compact: true},
	} {
		b.Run(format.name, func(b *testing.B) {
			closeForeignFixtures("")
			engine, err := newVibeDurable(Config{
				Dir:        b.TempDir(),
				CacheBytes: DefaultCacheBytes,
				Compact:    format.compact,
			})
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = engine.Close() })
			if err := engine.Load(docs); err != nil {
				b.Fatal(err)
			}

			b.Run("point", func(b *testing.B) {
				buf := make([]byte, 0, 512)
				index := 0
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					out, getErr := engine.Get(
						buf[:0], docs[probeIdx[index]].Key,
					)
					if getErr != nil {
						b.Fatal(getErr)
					}
					if len(out) == 0 {
						b.Fatal("empty document")
					}
					buf = out
					index++
					if index == len(probeIdx) {
						index = 0
					}
				}
			})

			b.Run("scan-all-bytes", func(b *testing.B) {
				totalBytes, _, _, _ := CorpusStats(docs)
				b.ReportAllocs()
				b.SetBytes(int64(totalBytes))
				b.ResetTimer()
				var count int
				for b.Loop() {
					var scanErr error
					count, scanErr = engine.ScanAllBytes()
					if scanErr != nil {
						b.Fatal(scanErr)
					}
				}
				b.StopTimer()
				if count != len(docs) {
					b.Fatalf(
						"scanned %d documents, want %d",
						count, len(docs),
					)
				}
				b.ReportMetric(
					float64(b.Elapsed().Nanoseconds())/
						float64(b.N*len(docs)),
					"ns/doc",
				)
			})
		})
	}
}

// BenchmarkPointWrite replaces one existing document. This is the workload
// where durability dominates, so it runs in each engine's explicitly named
// historical benchmark configurations. Equal slice positions are not claims
// of equivalent guarantees.
//
// Each repetition builds its own store and throws it away. The shared fixture
// this used to use accumulated every earlier repetition's writes, and the drift
// was large and monotonic: SQLite's sync=true row measured 4.10, then 5.41,
// then 6.72 ms across three repetitions of one `-count=3` run, +64% end to end.
// The median across repetitions hid it, and it penalised the engines that got
// through the most writes per repetition — i.e. the fastest ones — hardest.
func BenchmarkPointWrite(b *testing.B) {
	for _, factory := range Factories() {
		for _, durability := range BenchmarkDurabilityModes(factory.Name) {
			b.Run(fmt.Sprintf(
				"%s/durability=%s", factory.Name, durability,
			), func(b *testing.B) {
				closeForeignFixtures("")
				e, _, cleanup := newLoaded(b, factory, Config{
					Durability: durability,
				})
				defer cleanup()
				i := 0
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					idx := probeIdx[i]
					if err := e.Put(docs[idx].Key, UpdatedJSON(docs, idx)); err != nil {
						b.Fatal(err)
					}
					i++
					if i == len(probeIdx) {
						i = 0
					}
				}
			})
		}
	}
}

// BenchmarkPointWriteDurableDefaults shows what store/durable's own default
// commit-buffer pool costs a serial writer, against the tuned configuration
// every other row in this report uses.
func BenchmarkPointWriteDurableDefaults(b *testing.B) {
	durableFactory, _ := FactoryNamed("vibejson-durable")
	for _, untuned := range []bool{false, true} {
		name := "tuned"
		if untuned {
			name = "defaults"
		}
		b.Run("vibejson-durable/"+name, func(b *testing.B) {
			closeForeignFixtures("")
			e, _, cleanup := newLoaded(b, durableFactory, Config{Untuned: untuned})
			defer cleanup()
			i := 0
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				idx := probeIdx[i]
				if err := e.Put(docs[idx].Key, UpdatedJSON(docs, idx)); err != nil {
					b.Fatal(err)
				}
				i++
				if i == len(probeIdx) {
					i = 0
				}
			}
		})
	}
}

// BenchmarkScan visits every stored document once and touches the first byte of
// each value. It reports ns/doc so the number is independent of corpus size.
//
// This measures ITERATION ONLY and must be labelled that way wherever it is
// published. It was reported as a scan throughput figure and it cannot be one:
// vibejson's heap engine measured 248-byte documents at 5.41 ns/doc, which is
// 46 GB/s, above this machine's memory bandwidth. Nothing read the documents.
// BenchmarkScanAllBytes is the throughput measurement.
func BenchmarkScan(b *testing.B) {
	for _, factory := range Factories() {
		b.Run(factory.Name, func(b *testing.B) {
			e := loadedEngine(
				b, factory.Name, DurabilityDefault, false, "read",
			)
			b.ReportAllocs()
			b.ResetTimer()
			var n int
			for b.Loop() {
				var err error
				n, err = e.Scan()
				if err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			if n != len(docs) {
				b.Fatalf("scanned %d documents, want %d", n, len(docs))
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*len(docs)), "ns/doc")
		})
	}
}

// BenchmarkScanAllBytes is BenchmarkScan with every byte of every value
// actually read, through the one shared touchAll fold so the per-byte cost is
// identical across the table. It is the column to quote for "how fast can this
// engine hand me the corpus"; BenchmarkScan is the column for "how fast can it
// walk its own index". The two differ by more than an order of magnitude for
// the engines whose values are already materialised in memory.
func BenchmarkScanAllBytes(b *testing.B) {
	totalBytes, _, _, _ := CorpusStats(docs)
	for _, factory := range Factories() {
		b.Run(factory.Name, func(b *testing.B) {
			e := loadedEngine(
				b, factory.Name, DurabilityDefault, false, "read",
			)
			b.ReportAllocs()
			b.SetBytes(int64(totalBytes))
			b.ResetTimer()
			var n int
			for b.Loop() {
				var err error
				n, err = e.ScanAllBytes()
				if err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			if n != len(docs) {
				b.Fatalf("scanned %d documents, want %d", n, len(docs))
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*len(docs)), "ns/doc")
		})
	}
}

// BenchmarkFilter counts the ~1% of documents matching the scalar predicate
// with no secondary index available. For the key/value stores this is a full
// scan with a JSON extraction per document, which is precisely what they force
// an application to do. For SQLite it is a JSON1 expression over the stored
// text. For vibejson it is the query engine over an index-free collection.
func BenchmarkFilter(b *testing.B) {
	for _, factory := range Factories() {
		b.Run(factory.Name, func(b *testing.B) {
			e := loadedEngine(
				b, factory.Name, DurabilityDefault, false, "read",
			)
			b.ReportAllocs()
			b.ResetTimer()
			var n int
			for b.Loop() {
				var err error
				n, err = e.FilterCount(FilterValue)
				if err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			if n == 0 {
				b.Fatal("filter matched nothing")
			}
			b.ReportMetric(float64(n), "matches")
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*len(docs)), "ns/doc")
		})
	}
}

// BenchmarkIndexedFilter answers the same question with the engine's index.
// The key/value stores are skipped with an explicit reason rather than
// silently omitted: they have no such capability at all. Read every row here
// against the same engine's indexed row in BenchmarkBulkLoad, which is what
// the index cost to build.
func BenchmarkIndexedFilter(b *testing.B) {
	for _, factory := range Factories() {
		b.Run(factory.Name, func(b *testing.B) {
			if !IndexCapable(factory.Name) {
				b.Skipf("%s: %v", factory.Name, ErrNoIndex)
			}
			e := loadedEngine(
				b, factory.Name, DurabilityDefault, true, "indexed",
			)
			b.ReportAllocs()
			b.ResetTimer()
			var n int
			for b.Loop() {
				var err error
				n, err = e.IndexedCount(FilterValue)
				if err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			if n == 0 {
				b.Fatal("indexed probe matched nothing")
			}
			b.ReportMetric(float64(n), "matches")
		})
	}
}

// BenchmarkTuning measures every call-shape tuning this harness applies to a
// competitor against that competitor's own default spelling.
//
// It exists because "we tuned the competitors too" is the easiest claim in a
// competitive benchmark to make and the hardest to check. store/durable is
// tuned here — BufferCount=1024 buys it roughly 30 MiB of off-heap write
// buffers and a ~35x faster Put — and two competitors were left with symmetric
// pathologies on their defaults: Badger prefetching values it did not need on
// every scan, and bbolt opening and rolling back a read transaction on every
// point read that store/durable's transaction-free AppendRaw never opens.
// Every one of those is now a row here rather than a sentence in a Tuning()
// string, so the reader can check the size of each correction instead of
// trusting it.
//
// Only the engine/workload pairs where a tuning exists are listed. An engine
// absent from this table has no call-shape tuning to revert; what it does have
// is in its Tuning() string, and those are the settings that exist to make the
// engines comparable at all and cannot be flipped without changing the
// benchmark.
func BenchmarkTuning(b *testing.B) {
	cases := []struct {
		engine   string
		workload string
	}{
		{"bbolt", "pointread"},
		{"badger", "pointread"},
		{"badger", "scan"},
		{"sqlite", "scan"},
	}
	for _, c := range cases {
		factory, ok := FactoryNamed(c.engine)
		if !ok {
			b.Fatalf("unknown engine %q", c.engine)
		}
		for _, untuned := range []bool{false, true} {
			label := "tuned"
			if untuned {
				label = "defaults"
			}
			b.Run(fmt.Sprintf("%s/%s/%s", c.engine, c.workload, label), func(b *testing.B) {
				closeForeignFixtures("")
				e, _, cleanup := newLoaded(b, factory, Config{Untuned: untuned})
				defer cleanup()
				b.ReportAllocs()
				b.ResetTimer()
				switch c.workload {
				case "pointread":
					buf := make([]byte, 0, 512)
					i := 0
					for b.Loop() {
						out, err := e.Get(buf[:0], docs[probeIdx[i]].Key)
						if err != nil {
							b.Fatal(err)
						}
						if len(out) == 0 {
							b.Fatal("empty document")
						}
						buf = out
						i++
						if i == len(probeIdx) {
							i = 0
						}
					}
				case "scan":
					for b.Loop() {
						n, err := e.Scan()
						if err != nil {
							b.Fatal(err)
						}
						if n != len(docs) {
							b.Fatalf("scanned %d documents, want %d", n, len(docs))
						}
					}
					b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*len(docs)), "ns/doc")
				}
			})
		}
	}
}

// BenchmarkParse isolates the JSON extraction the key/value stores are forced
// into, with no storage engine underneath. It separates "what storage costs"
// from "what parsing costs" in the filter rows, and shows how much the
// key/value engines were flattered by being handed vibejson's own parser
// instead of the standard library they would realistically use.
func BenchmarkParse(b *testing.B) {
	needle := jsonScalarNeedle(FilterValue)
	b.Run("vibejson-pointer", func(b *testing.B) {
		b.ReportAllocs()
		i, n := 0, 0
		for b.Loop() {
			ok, err := matchesCountry(docs[i].JSON, needle)
			if err != nil {
				b.Fatal(err)
			}
			if ok {
				n++
			}
			i++
			if i == len(docs) {
				i = 0
			}
		}
		runtime.KeepAlive(n)
	})
	b.Run("encoding-json", func(b *testing.B) {
		b.ReportAllocs()
		i, n := 0, 0
		for b.Loop() {
			ok, err := matchesCountryStdlib(docs[i].JSON, FilterValue)
			if err != nil {
				b.Fatal(err)
			}
			if ok {
				n++
			}
			i++
			if i == len(docs) {
				i = 0
			}
		}
		runtime.KeepAlive(n)
	})
}

// TestFullEquivalence is the test that licenses every performance number in
// this harness. Every engine must return the exact stored bytes for every one
// of the corpus's keys, and must visit every key exactly once with those same
// bytes during a scan.
//
// It replaced a check that verified one document — docs[42] — and validated
// Scan by its count alone. An engine that returned correct bytes for docs[42]
// and garbage for the other 99,999, or that returned the right number of wrong
// documents, passed that check and its benchmark numbers would have been
// published. Nothing about a storage benchmark is meaningful without this.
//
// It is not a benchmark and it is allowed to be slow.
func TestFullEquivalence(t *testing.T) {
	if len(docs) == 0 {
		t.Fatal("empty corpus")
	}
	total, gz, err := CorpusRedundancy(docs)
	if err != nil {
		t.Fatal(err)
	}
	_, minBytes, maxBytes, want := CorpusStats(docs)
	t.Logf("corpus: cardinality=%s, %d docs, %d..%d bytes each, %.2f MiB total, "+
		"gzip -9 %.2f MiB (%.1f%% of raw), %d match %s=%s",
		cardinality, len(docs), minBytes, maxBytes, float64(total)/(1<<20),
		float64(gz)/(1<<20), 100*float64(gz)/float64(total), want, FilterField, FilterValue)

	byKey := make(map[string][]byte, len(docs))
	for i := range docs {
		byKey[docs[i].Key] = docs[i].JSON
	}

	for _, factory := range Factories() {
		t.Run(factory.Name, func(t *testing.T) {
			e := loadedEngine(
				t, factory.Name, DurabilityDefault, false, "read",
			)

			// 1. Every key, by Get, byte-identical. Not one key: all of them.
			var buf []byte
			for i := range docs {
				buf, err = e.Get(buf[:0], docs[i].Key)
				if err != nil {
					t.Fatalf("Get(%q): %v", docs[i].Key, err)
				}
				if string(buf) != string(docs[i].JSON) {
					t.Fatalf("Get(%q) mismatch:\n got %s\nwant %s",
						docs[i].Key, buf, docs[i].JSON)
				}
			}

			// 2. A scan visits every key exactly once, with exact bytes.
			seen := make([]bool, len(docs))
			n := 0
			err := e.Visit(func(key string, value []byte) error {
				n++
				want, ok := byKey[key]
				if !ok {
					return fmt.Errorf("scan produced unknown key %q", key)
				}
				if string(value) != string(want) {
					return fmt.Errorf("scan value mismatch for %q:\n got %s\nwant %s", key, value, want)
				}
				ord, err := keyOrdinal(key)
				if err != nil {
					return err
				}
				if seen[ord] {
					return fmt.Errorf("scan produced key %q twice", key)
				}
				seen[ord] = true
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if n != len(docs) {
				t.Fatalf("scan visited %d documents, want %d", n, len(docs))
			}
			for i, ok := range seen {
				if !ok {
					t.Fatalf("scan never visited %q", Key(i))
				}
			}

			// 3. Scan and ScanAllBytes agree with each other and with the corpus.
			for name, fn := range map[string]func() (int, error){
				"Scan": e.Scan, "ScanAllBytes": e.ScanAllBytes,
			} {
				got, err := fn()
				if err != nil {
					t.Fatalf("%s: %v", name, err)
				}
				if got != len(docs) {
					t.Fatalf("%s = %d, want %d", name, got, len(docs))
				}
			}

			// 4. Both filter paths agree with the corpus, and IndexCapable
			// agrees with what the engine actually does.
			c, err := e.FilterCount(FilterValue)
			if err != nil {
				t.Fatal(err)
			}
			if c != want {
				t.Fatalf("FilterCount = %d, want %d", c, want)
			}

			// The indexed vibejson-durable arm cannot run at the default corpus.
			// Its on-graph exact index serialises one physical index's whole
			// posting set into a single term leaf, bounded to 65,535 postings /
			// 64 KiB, and that ceiling binds both the bulk cutover and the Put
			// mutation path. The shared 100k corpus overruns it (every term's
			// postings share one leaf), so that arm's byte-exact oracle lives in
			// TestFullEquivalenceIndexedDurable, sized below the ceiling. Multi-leaf
			// term indexes are the future work that lifts the cap and folds this
			// arm back here. Every other engine's indexed arm stays at 100k.
			if factory.Name == "vibejson-durable" {
				return
			}

			idx := loadedEngine(
				t, factory.Name, DurabilityDefault, true, "indexed",
			)
			ic, err := idx.IndexedCount(FilterValue)
			switch {
			case errors.Is(err, ErrNoIndex):
				if IndexCapable(factory.Name) {
					t.Fatalf("IndexCapable(%q) is true but IndexedCount returned ErrNoIndex", factory.Name)
				}
			case err != nil:
				t.Fatal(err)
			default:
				if !IndexCapable(factory.Name) {
					t.Fatalf("IndexCapable(%q) is false but IndexedCount answered", factory.Name)
				}
				if ic != want {
					t.Fatalf("IndexedCount = %d, want %d", ic, want)
				}
			}
		})
	}
}

// TestFullEquivalenceIndexedDurable extends TestFullEquivalence's byte-exact
// oracle to the one arm TestFullEquivalence cannot cover at the default corpus:
// the indexed vibejson-durable engine.
//
// Its on-graph exact index serialises one physical index's whole posting set
// into a single term leaf, bounded to 65,535 postings / 64 KiB. That ceiling
// binds both the bulk cutover (AppendIndexTermLeaf) and the Put mutation path
// (the incremental exact-tile rebuild), so a low-cardinality corpus spread
// across enough documents overruns it. This test sizes the indexed durable arm
// below the ceiling and applies the identical oracle: every key byte-identical
// by Get, every key visited exactly once with exact bytes, Scan and
// ScanAllBytes counts, and IndexedCount against the corpus oracle. Multi-leaf
// term indexes are the future work that lifts the cap and folds this arm back
// into TestFullEquivalence at the shared corpus.
//
// It is not a benchmark and it is allowed to be slow.
func TestFullEquivalenceIndexedDurable(t *testing.T) {
	// 10,000 keeps the whole index's posting set inside the single term leaf
	// (65,535 postings / 64 KiB). It is the largest round size that passes; the
	// cap, not the test, sets the ceiling.
	const size = 10_000
	corpus := CorpusOf(size, cardinality)

	byKey := make(map[string][]byte, len(corpus))
	for i := range corpus {
		byKey[corpus[i].Key] = corpus[i].JSON
	}
	_, _, _, want := CorpusStats(corpus)

	factory, ok := FactoryNamed("vibejson-durable")
	if !ok {
		t.Fatal("vibejson-durable factory missing")
	}
	if !IndexCapable(factory.Name) {
		t.Fatalf("IndexCapable(%q) is false; this test asserts the indexed arm", factory.Name)
	}
	// Built directly rather than through loadedEngine, whose shared fixtures load
	// the package-global 100k docs this arm cannot index.
	e, _, cleanup := newLoadedCorpus(t, factory, Config{
		Durability: DurabilityDefault,
		Indexed:    true,
	}, corpus)
	defer cleanup()

	// 1. Every key, by Get, byte-identical.
	var buf []byte
	var err error
	for i := range corpus {
		buf, err = e.Get(buf[:0], corpus[i].Key)
		if err != nil {
			t.Fatalf("Get(%q): %v", corpus[i].Key, err)
		}
		if string(buf) != string(corpus[i].JSON) {
			t.Fatalf("Get(%q) mismatch:\n got %s\nwant %s",
				corpus[i].Key, buf, corpus[i].JSON)
		}
	}

	// 2. A scan visits every key exactly once, with exact bytes.
	seen := make([]bool, len(corpus))
	n := 0
	err = e.Visit(func(key string, value []byte) error {
		n++
		wantJSON, ok := byKey[key]
		if !ok {
			return fmt.Errorf("scan produced unknown key %q", key)
		}
		if string(value) != string(wantJSON) {
			return fmt.Errorf("scan value mismatch for %q:\n got %s\nwant %s", key, value, wantJSON)
		}
		ord, err := keyOrdinal(key)
		if err != nil {
			return err
		}
		if ord < 0 || ord >= len(seen) {
			return fmt.Errorf("scan produced out-of-range key %q", key)
		}
		if seen[ord] {
			return fmt.Errorf("scan produced key %q twice", key)
		}
		seen[ord] = true
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if n != len(corpus) {
		t.Fatalf("scan visited %d documents, want %d", n, len(corpus))
	}
	for i, ok := range seen {
		if !ok {
			t.Fatalf("scan never visited %q", Key(i))
		}
	}

	// 3. Scan and ScanAllBytes agree with each other and with the corpus.
	for name, fn := range map[string]func() (int, error){
		"Scan": e.Scan, "ScanAllBytes": e.ScanAllBytes,
	} {
		got, err := fn()
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if got != len(corpus) {
			t.Fatalf("%s = %d, want %d", name, got, len(corpus))
		}
	}

	// 4. The index answers the filter with the corpus's exact population.
	ic, err := e.IndexedCount(FilterValue)
	if err != nil {
		t.Fatalf("IndexedCount: %v", err)
	}
	if ic != want {
		t.Fatalf("IndexedCount = %d, want %d", ic, want)
	}
}

// keyOrdinal recovers i from Key(i).
func keyOrdinal(key string) (int, error) {
	const prefix = "doc:"
	if len(key) <= len(prefix) || key[:len(prefix)] != prefix {
		return 0, fmt.Errorf("malformed corpus key %q", key)
	}
	return strconv.Atoi(key[len(prefix):])
}

// TestCorpusVariantsAreShapeMatched proves the claim the high-cardinality disk
// column rests on: the two corpus variants differ in value entropy and in
// nothing else. If they differed in size, a disk difference between them would
// be unattributable.
func TestCorpusVariantsAreShapeMatched(t *testing.T) {
	const n = 20000
	low := CorpusOf(n, LowCardinality)
	high := CorpusOf(n, HighCardinality)
	for i := range low {
		if low[i].Key != high[i].Key {
			t.Fatalf("doc %d: key %q vs %q", i, low[i].Key, high[i].Key)
		}
		if len(low[i].JSON) != len(high[i].JSON) {
			t.Fatalf("doc %d: %d bytes vs %d bytes:\n%s\n%s",
				i, len(low[i].JSON), len(high[i].JSON), low[i].JSON, high[i].JSON)
		}
	}
	// Selectivity must be identical too, or the filter rows are not comparable.
	_, _, _, lowMatches := CorpusStats(low)
	_, _, _, highMatches := CorpusStats(high)
	if lowMatches != highMatches {
		t.Fatalf("filter selectivity differs: %d vs %d matches", lowMatches, highMatches)
	}

	lowTotal, lowGz, err := CorpusRedundancy(low)
	if err != nil {
		t.Fatal(err)
	}
	highTotal, highGz, err := CorpusRedundancy(high)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("low:  %d bytes, gzip -9 %d (%.1f%%)", lowTotal, lowGz, 100*float64(lowGz)/float64(lowTotal))
	t.Logf("high: %d bytes, gzip -9 %d (%.1f%%)", highTotal, highGz, 100*float64(highGz)/float64(highTotal))
	if lowGz >= highGz {
		t.Fatalf("the high-cardinality corpus must be less compressible: %d vs %d", highGz, lowGz)
	}
}
