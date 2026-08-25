package competitive

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"runtime"
	"slices"
	"strconv"
	"testing"

	"github.com/thesyncim/vibejson"
)

func TestCorpusDocumentShapesAreExactAndValid(t *testing.T) {
	for _, test := range []struct {
		shape DocumentShape
		large func(int) bool
		want  int
	}{
		{MixedDocuments, func(i int) bool { return i&1 != 0 }, mixedOverflowDocumentBytes},
		{OverflowHeavyDocuments, func(i int) bool { return i&7 != 0 }, heavyOverflowDocumentBytes},
	} {
		first := CorpusOfShape(32, HighCardinality, test.shape)
		second := CorpusOfShape(32, HighCardinality, test.shape)
		for i := range first {
			if !bytes.Equal(first[i].JSON, second[i].JSON) {
				t.Fatalf("%s document %d is not deterministic", test.shape, i)
			}
			if test.large(i) {
				if len(first[i].JSON) != test.want {
					t.Fatalf("%s document %d bytes=%d want=%d", test.shape, i, len(first[i].JSON), test.want)
				}
			} else if len(first[i].JSON) >= 512 {
				t.Fatalf("%s inline document %d bytes=%d", test.shape, i, len(first[i].JSON))
			}
		}
		if test.shape.MaxDocumentBytes() != test.want {
			t.Fatalf("%s admission bound=%d want=%d", test.shape, test.shape.MaxDocumentBytes(), test.want)
		}
	}
}

func TestNonIndexAdaptersRejectExactIndexConfiguration(t *testing.T) {
	for _, factory := range Factories() {
		if IndexCapable(factory.Name) {
			continue
		}
		if _, err := factory.New(Config{Dir: t.TempDir(), ExactIndexes: 1}); err == nil {
			t.Fatalf("%s accepted an exact-index configuration", factory.Name)
		}
	}
}

func TestExactIndexParametricPostingEquivalence(t *testing.T) {
	const targetOrdinal = 1 // mixed-shape overflow row
	corpus := CorpusOfShape(256, LowCardinality, MixedDocuments)
	original := corpus[targetOrdinal]
	wantValues := make([]string, MaximumExactIndexes)
	wantCounts := make([]int, MaximumExactIndexes)
	for index := range int(MaximumExactIndexes) {
		wantValues[index] = exactIndexText(t, original.JSON, uint8(index))
		for i := range corpus {
			if exactIndexText(t, corpus[i].JSON, uint8(index)) == wantValues[index] {
				wantCounts[index]++
			}
		}
	}

	for _, engineName := range []string{"vibedb", "sqlite"} {
		t.Run(engineName, func(t *testing.T) {
			factory, ok := FactoryNamed(engineName)
			if !ok {
				t.Fatalf("factory %q missing", engineName)
			}
			engine, _, cleanup := newLoadedCorpus(t, factory, Config{
				Durability: DurabilityBufferedVisible, ExactIndexes: MaximumExactIndexes,
				MaxDocumentBytes: MixedDocuments.MaxDocumentBytes(),
			}, corpus)
			defer cleanup()

			assert := func(
				stage string, changed int, documentAbsent bool,
				alternate string, alternateCount int,
			) {
				t.Helper()
				for index := range int(MaximumExactIndexes) {
					want := wantCounts[index]
					if documentAbsent || index == changed {
						want--
					}
					probe, err := engine.ProbeExactIndex(uint8(index), wantValues[index])
					if err != nil || probe.Count != want || !probe.IndexBounded || probe.IndexLookups == 0 {
						t.Fatalf("%s %s old probe %s = %+v, %v; want count=%d indexed",
							engineName, stage, ExactIndexDefinitions[index].Name, probe, err, want)
					}
				}
				if changed >= 0 {
					probe, err := engine.ProbeExactIndex(uint8(changed), alternate)
					if err != nil || probe.Count != alternateCount ||
						!probe.IndexBounded || probe.IndexLookups == 0 {
						t.Fatalf("%s %s alternate probe %s = %+v, %v; want count=%d indexed",
							engineName, stage, ExactIndexDefinitions[changed].Name,
							probe, err, alternateCount)
					}
				}
			}

			assert("loaded", -1, false, "", 0)
			for changed := range int(MaximumExactIndexes) {
				alternate := string(bytes.Repeat([]byte{'z'}, len(wantValues[changed])))
				updated := replaceExactIndexText(
					t, original.JSON, ExactIndexDefinitions[changed].Name,
					wantValues[changed], alternate,
				)
				if err := engine.Put(original.Key, updated); err != nil {
					t.Fatalf("%s update %s: %v", engineName, ExactIndexDefinitions[changed].Name, err)
				}
				assert("updated", changed, false, alternate, 1)
				if err := engine.Delete(original.Key); err != nil {
					t.Fatalf("%s delete %s: %v", engineName, ExactIndexDefinitions[changed].Name, err)
				}
				assert("deleted", changed, true, alternate, 0)
				if err := engine.Upsert(original.Key, original.JSON); err != nil {
					t.Fatalf("%s restore %s: %v", engineName, ExactIndexDefinitions[changed].Name, err)
				}
				if err := engine.Checkpoint(); err != nil {
					t.Fatalf("%s checkpoint %s: %v", engineName, ExactIndexDefinitions[changed].Name, err)
				}
				assert("restored-checkpoint", -1, false, "", 0)
			}
		})
	}
}

func exactIndexText(t testing.TB, document []byte, index uint8) string {
	t.Helper()
	if index >= MaximumExactIndexes {
		t.Fatalf("exact index %d out of range", index)
	}
	value, err := vibejson.Parse(document)
	if err != nil {
		t.Fatal(err)
	}
	pointer := vibejson.MustCompilePointer(ExactIndexDefinitions[index].JSONPointer)
	target, found, err := value.PointerCompiled(pointer)
	if err != nil || !found {
		t.Fatalf("exact index %d target found=%t err=%v", index, found, err)
	}
	text, ok := target.Text()
	if !ok {
		t.Fatalf("exact index %d target is not text", index)
	}
	return text
}

func replaceExactIndexText(
	t testing.TB,
	document []byte,
	field, old, replacement string,
) []byte {
	t.Helper()
	needle := []byte(`"` + field + `":"` + old + `"`)
	next := []byte(`"` + field + `":"` + replacement + `"`)
	result := bytes.Replace(document, needle, next, 1)
	if bytes.Equal(result, document) || !vibejson.Valid(result) {
		t.Fatalf("replace exact index field %q failed", field)
	}
	return result
}

var (
	corpusSize = flag.Int("corpus", CorpusSize, "documents in the shared corpus")
	corpusCard = flag.String("cardinality", "low", "corpus variant: low (the shipped, ~92% redundant one) or high")
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
// Keeping foreign engines resident biases GC-visible and mmap-backed working
// sets differently. A full -bench=. run releases them to stay close to the
// isolated process-per-engine publication protocol in README.md.
func closeForeignFixtures(name string) {
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
// the package-global docs. It is used by focused equivalence tests that want a
// smaller fixture than the shared benchmark corpus.
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
		Durability:   durability,
		ExactIndexes: exactIndexCount(indexed),
	})
	fixtures[key] = &fixture{engine: e, dir: dir}
	return e
}

// BenchmarkBulkLoad measures loading the whole corpus into an empty engine
// through that engine's bulk path: one bbolt write transaction, one Badger
// WriteBatch, one Pebble batch, one SQLite transaction over a prepared
// INSERT, and durable.CreateFromRecords for store/durable. The durable lane
// borrows the same corpus rows and charges descriptor planning,
// canonicalization, graph construction, publication, and reopen inside the
// measurement; it does not build a redundant in-memory database first.
//
// It runs with and without a secondary index over the filter field. The
// indexed-filter result must be read against its own indexed-load cost, not
// against the engine's unindexed load.
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
							Dir: dir, Durability: durability, ExactIndexes: exactIndexCount(indexed),
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

// BenchmarkBulkLoadVariants compares the sole unified bulk output with
// mutation replay and with leaving BufferCount at its default.
//
// Every variant is measured at more than one corpus size so per-build fixed
// costs (shape templates, value dictionaries, and key directories) cannot be
// mistaken for stable per-document scaling. Read sizes against each other
// before comparing any variant with another benchmark.
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
		{name: "bulk-unified-tuned", cfg: Config{}},
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
			b.Run(fmt.Sprintf("vibedb/%s/n=%d", v.name, size), func(b *testing.B) {
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
					e, err := newVibeDB(cfg)
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
				b, factory.Name, DurabilityBufferedVisible, false, "read",
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

// BenchmarkPointWrite alternates one existing document between its original
// value and a longer structural replacement. Every iteration therefore changes
// bytes and length; after one corpus pass it does not silently become an
// idempotent Put benchmark. This is the workload where durability dominates,
// so it runs in each engine's explicitly named durability configurations.
// Equal slice positions are not claims of equivalent guarantees. Each
// repetition builds its own store so writes from one repetition cannot affect
// another.
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
				before, vibeDB := vibeDBWriteCountersOf(e)
				i := 0
				var replacement []byte
				updated := make([]bool, len(docs))
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					idx := probeIdx[i]
					if updated[idx] {
						replacement = append(replacement[:0], docs[idx].JSON...)
					} else {
						replacement = AppendUpdatedJSON(replacement[:0], docs, idx)
					}
					if err := e.Put(docs[idx].Key, replacement); err != nil {
						b.Fatal(err)
					}
					updated[idx] = !updated[idx]
					i++
					if i == len(probeIdx) {
						i = 0
					}
				}
				b.StopTimer()
				if vibeDB {
					reportVibeDBWriteCounters(b, before, e, false)
				}
			})
		}
	}
}

func reportVibeDBWriteCounters(
	b *testing.B,
	before vibeDBWriteCounters,
	engine Engine,
	requirePatch bool,
) {
	b.Helper()
	after, ok := vibeDBWriteCountersOf(engine)
	if !ok {
		return
	}
	attempts := after.patchAttempts - before.patchAttempts
	patches := after.patches - before.patches
	folds := after.folds - before.folds
	replacements := after.replacements - before.replacements
	if requirePatch && patches == 0 {
		b.Fatalf("claimed compact-patch workload accepted no patches (attempts=%d)", attempts)
	}
	operations := float64(max(1, b.N))
	b.ReportMetric(float64(attempts)/operations, "patch-attempts/op")
	if attempts != 0 {
		b.ReportMetric(100*float64(patches)/float64(attempts), "patch-accept-%")
	}
	b.ReportMetric(float64(folds)/operations, "overlay-folds/op")
	b.ReportMetric(float64(replacements)/operations, "concurrent-replaces/op")
}

// BenchmarkPointWriteDurableDefaults shows what store/durable's own default
// commit-buffer pool costs a serial writer, against the tuned configuration
// every other row in this report uses.
func BenchmarkPointWriteDurableDefaults(b *testing.B) {
	durableFactory, _ := FactoryNamed("vibedb")
	for _, untuned := range []bool{false, true} {
		name := "tuned"
		if untuned {
			name = "defaults"
		}
		b.Run("vibedb/"+name, func(b *testing.B) {
			closeForeignFixtures("")
			e, _, cleanup := newLoaded(b, durableFactory, Config{Untuned: untuned})
			defer cleanup()
			i := 0
			var replacement []byte
			updated := make([]bool, len(docs))
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				idx := probeIdx[i]
				if updated[idx] {
					replacement = append(replacement[:0], docs[idx].JSON...)
				} else {
					replacement = AppendUpdatedJSON(replacement[:0], docs, idx)
				}
				if err := e.Put(docs[idx].Key, replacement); err != nil {
					b.Fatal(err)
				}
				updated[idx] = !updated[idx]
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
// VibeDB's heap engine measured 248-byte documents at 5.41 ns/doc, which is
// 46 GB/s, above this machine's memory bandwidth. Nothing read the documents.
// BenchmarkScanAllBytes is the throughput measurement.
func BenchmarkScan(b *testing.B) {
	for _, factory := range Factories() {
		b.Run(factory.Name, func(b *testing.B) {
			e := loadedEngine(
				b, factory.Name, DurabilityBufferedVisible, false, "read",
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
				b, factory.Name, DurabilityBufferedVisible, false, "read",
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
// text. For VibeDB it is the query engine over an index-free collection.
func BenchmarkFilter(b *testing.B) {
	for _, factory := range Factories() {
		b.Run(factory.Name, func(b *testing.B) {
			e := loadedEngine(
				b, factory.Name, DurabilityBufferedVisible, false, "read",
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
				b, factory.Name, DurabilityBufferedVisible, true, "indexed",
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
// key/value engines were flattered by being handed a trusted raw pointer seeker
// after admission. The second cell measures the same operation with complete
// document validation, retaining a single byte-native implementation.
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
	b.Run("vibejson-pointer-validated", func(b *testing.B) {
		b.ReportAllocs()
		i, n := 0, 0
		for b.Loop() {
			ok, err := matchesCountryValidated(docs[i].JSON, needle)
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
// this harness. Every engine must return its documented stored representation
// for every one of the corpus's keys, and must visit every key exactly once
// with those same bytes during a scan. Byte-preserving engines return the
// submitted spelling; vibedb returns the sole compact-stripe canonical form.
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

	for _, factory := range Factories() {
		t.Run(factory.Name, func(t *testing.T) {
			e := loadedEngine(
				t, factory.Name, DurabilityBufferedVisible, false, "read",
			)

			byKey := make(map[string][]byte, len(docs))
			var expected []byte
			for i := range docs {
				expected, err = AppendExpectedStoredJSON(expected[:0], factory.Name, docs[i].JSON)
				if err != nil {
					t.Fatalf("canonicalize %q: %v", docs[i].Key, err)
				}
				byKey[docs[i].Key] = append([]byte(nil), expected...)
			}

			// 1. Every key, by Get, representation-identical. Not one key: all
			// of them.
			var buf []byte
			for i := range docs {
				buf, err = e.Get(buf[:0], docs[i].Key)
				if err != nil {
					t.Fatalf("Get(%q): %v", docs[i].Key, err)
				}
				wantJSON := byKey[docs[i].Key]
				if string(buf) != string(wantJSON) {
					t.Fatalf("Get(%q) mismatch:\n got %s\nwant %s",
						docs[i].Key, buf, wantJSON)
				}
			}

			// 2. A scan visits every key exactly once, with the expected bytes.
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

			idx := loadedEngine(
				t, factory.Name, DurabilityBufferedVisible, IndexCapable(factory.Name), "indexed",
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

// TestFullEquivalenceIndexedDurable exercises the indexed durable path on a
// compact independent fixture in addition to the shared-corpus equivalence
// test. It applies the identical oracle: every key canonical by Get, every key
// visited exactly once with canonical bytes, Scan and ScanAllBytes counts, and
// IndexedCount against the corpus oracle.
//
// It is not a benchmark and it is allowed to be slow.
func TestFullEquivalenceIndexedDurable(t *testing.T) {
	// Keep this focused oracle smaller than the shared benchmark to avoid
	// duplicating its full indexed-load cost in the test suite.
	const size = 10_000
	corpus := CorpusOf(size, cardinality)

	byKey := make(map[string][]byte, len(corpus))
	var expected []byte
	var err error
	for i := range corpus {
		expected, err = AppendExpectedStoredJSON(expected[:0], "vibedb", corpus[i].JSON)
		if err != nil {
			t.Fatalf("canonicalize %q: %v", corpus[i].Key, err)
		}
		byKey[corpus[i].Key] = append([]byte(nil), expected...)
	}
	_, _, _, want := CorpusStats(corpus)

	factory, ok := FactoryNamed("vibedb")
	if !ok {
		t.Fatal("vibedb factory missing")
	}
	if !IndexCapable(factory.Name) {
		t.Fatalf("IndexCapable(%q) is false; this test asserts the indexed arm", factory.Name)
	}
	// Built directly rather than through loadedEngine, keeping this focused
	// oracle independent of the package-global benchmark fixture.
	e, _, cleanup := newLoadedCorpus(t, factory, Config{
		Durability:   DurabilityBufferedVisible,
		ExactIndexes: 1,
	}, corpus)
	defer cleanup()

	// 1. Every key, by Get, canonical-byte-identical.
	var buf []byte
	for i := range corpus {
		buf, err = e.Get(buf[:0], corpus[i].Key)
		if err != nil {
			t.Fatalf("Get(%q): %v", corpus[i].Key, err)
		}
		wantJSON := byKey[corpus[i].Key]
		if string(buf) != string(wantJSON) {
			t.Fatalf("Get(%q) mismatch:\n got %s\nwant %s",
				corpus[i].Key, buf, wantJSON)
		}
	}

	// 2. A scan visits every key exactly once, with canonical bytes.
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
