package durable

import (
	"bytes"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibejson"
)

// The U1 read-gate benchmark battery (unified-leaf design §10, §11 U1 row).
// Every benchmark builds its store from the exact competitive corpus the
// design's baselines were measured on (100k ~249 B documents) and reports
// the gate quantity explicitly: p50 ns for point pipelines (the gates are
// p50 gates), ns/doc for scan-shaped lanes.

func unifiedBenchSource(b *testing.B, keys []string, docs [][]byte) *store.Collection {
	b.Helper()
	builder, err := store.NewBuilder(store.Options{})
	if err != nil {
		b.Fatal(err)
	}
	for i := range keys {
		if err := builder.Append(keys[i], docs[i]); err != nil {
			b.Fatal(err)
		}
	}
	built, err := builder.Build()
	if err != nil {
		b.Fatal(err)
	}
	return built
}

func unifiedBenchStore(b *testing.B, keys []string, docs [][]byte, options Options) *Collection {
	return unifiedBenchStoreWith(b, keys, docs, options, Options{
		ResidentBytes: 256 << 20, Backend: BackendPortable,
	})
}

func unifiedBenchStoreWith(
	b *testing.B, keys []string, docs [][]byte, createOptions, openOptions Options,
) *Collection {
	b.Helper()
	dir := b.TempDir()
	path := filepath.Join(dir, "bench.vibe")
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		b.Fatal(err)
	}
	if _, err := CreateFromPrimary(unifiedBenchSource(b, keys, docs), file, createOptions); err != nil {
		b.Fatalf("CreateFromPrimary: %v", err)
	}
	if err := file.Close(); err != nil {
		b.Fatal(err)
	}
	reopened, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		b.Fatal(err)
	}
	collection, err := Open(reopened, openOptions)
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	b.Cleanup(func() {
		_ = collection.Close()
		_ = reopened.Close()
	})
	return collection
}

func unifiedBenchOptions() Options {
	return Options{
		ResidentBytes: 256 << 20, Backend: BackendPortable,
		Durability: DurabilityAsyncVisible,
	}
}

// benchCorpus returns the 100k low-cardinality competitive corpus (the GetRaw
// and filter gates' corpus) unless -short trims it for smoke runs.
func benchCorpus(b *testing.B) ([]string, [][]byte, int) {
	n := 100_000
	if testing.Short() {
		n = 10_000
	}
	keys, docs := unifiedCompetitiveCorpus(n, false)
	return keys, docs, n
}

func shuffledKeyBytes(keys []string) [][]byte {
	rng := rand.New(rand.NewPCG(42, 43))
	out := make([][]byte, len(keys))
	for i := range keys {
		out[i] = []byte(keys[i])
	}
	rng.Shuffle(len(out), func(i, j int) { out[i], out[j] = out[j], out[i] })
	return out
}

// reportP50 reports the median and p99 of the recorded per-op latencies,
// measured the way the competitive harness measures them (a time.Now/Since
// pair per operation, cmd/mixed/main.go:291). On this platform the monotonic
// clock ticks at ~41.7 ns, so single-op samples quantize to tick multiples;
// reportBatchP50 complements this with tick-free batch sampling.
func reportP50(b *testing.B, lat []int64) {
	if len(lat) == 0 {
		return
	}
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	b.ReportMetric(float64(lat[len(lat)/2]), "p50-ns/op")
	b.ReportMetric(float64(lat[len(lat)*99/100]), "p99-ns/op")
}

// batchSpan is the operations-per-clock-pair of the batched sampler: enough
// to dissolve the 41.7 ns tick and the clock-pair overhead below 3 ns/op,
// small enough that a p50 over batch means still reflects typical cost.
const batchSpan = 16

func reportBatchP50(b *testing.B, batch []int64) {
	if len(batch) == 0 {
		return
	}
	sort.Slice(batch, func(i, j int) bool { return batch[i] < batch[j] })
	b.ReportMetric(float64(batch[len(batch)/2])/batchSpan, "p50-batch16-ns/op")
}

// measureBatchP50 runs an untimed batched-sampling pass (16 ops per clock
// pair) and reports its tick-free p50; callers run it before ResetTimer.
func measureBatchP50(b *testing.B, ops int, op func(i int)) {
	b.Helper()
	batches := ops / batchSpan
	if batches == 0 {
		return
	}
	samples := make([]int64, batches)
	at := 0
	for s := 0; s < batches; s++ {
		start := time.Now()
		for range batchSpan {
			op(at)
			at++
		}
		samples[s] = time.Since(start).Nanoseconds()
	}
	reportBatchP50(b, samples)
}

func benchmarkPointGetRaw(b *testing.B) {
	keys, docs, _ := benchCorpus(b)
	collection := unifiedBenchStore(b, keys, docs, unifiedBenchOptions())
	snapshot, err := collection.Snapshot()
	if err != nil {
		b.Fatal(err)
	}
	defer snapshot.Close()
	probeKeys := shuffledKeyBytes(keys)
	dst := make([]byte, 0, 4096)
	lat := make([]int64, b.N)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := probeKeys[i%len(probeKeys)]
		start := time.Now()
		out, found, err := snapshot.AppendRaw(dst[:0], key)
		lat[i] = time.Since(start).Nanoseconds()
		if err != nil || !found {
			b.Fatalf("AppendRaw(%q) = (%v,%v)", key, found, err)
		}
		dst = out[:0]
	}
	b.StopTimer()
	measureBatchP50(b, min(b.N, 320_000), func(i int) {
		out, found, err := snapshot.AppendRaw(dst[:0], probeKeys[i%len(probeKeys)])
		if err != nil || !found {
			b.Fatal("AppendRaw")
		}
		dst = out[:0]
	})
	reportP50(b, lat)
}

// BenchmarkUnifiedGetRaw is the §10.1 gate: point-read p50 ≤ 0.50 µs
// (kill-switch ceiling 0.70 µs), 0 allocs.
func BenchmarkUnifiedGetRaw(b *testing.B) { benchmarkPointGetRaw(b) }

// BenchmarkUnifiedPrimaryReplace is the U2 mutation gate. It drives uniform
// equal-size replacements across the competitive corpus on the same
// buffered-visible acknowledgement contract as the 4.6 us pre-unification
// baseline.
// Median latency measures O(document) overlay publication; p99 exposes the
// bounded checkpoint that folds a full dirty-bucket window.
func BenchmarkUnifiedPrimaryReplace(b *testing.B) {
	const documents = 10_000
	keys, docs := unifiedPrimaryCorpus(documents, false)
	options := Options{
		ResidentBytes: 64 << 20, Backend: BackendPortable,
		Durability: DurabilityBufferedVisible,
	}
	collection := unifiedBenchStoreWith(b, keys, docs, options, options)
	if collection.primaryUnifiedOverlay == nil {
		b.Fatal("unified row overlay was not budgeted")
	}
	canonical := make([][]byte, documents)
	replacement := make([][]byte, documents)
	keyBytes := make([][]byte, documents)
	for i := range documents {
		var err error
		canonical[i], err = vibejson.AppendCanonicalize(nil, docs[i])
		if err != nil {
			b.Fatal(err)
		}
		replacement[i] = append([]byte(nil), canonical[i]...)
		if bytes.Contains(replacement[i], []byte(`"active":true`)) {
			replacement[i] = bytes.Replace(
				replacement[i], []byte(`"active":true`),
				[]byte(`"active":null`), 1,
			)
		} else {
			replacement[i] = bytes.Replace(
				replacement[i], []byte(`"active":false`),
				[]byte(`"active":10e+0`), 1,
			)
		}
		if len(replacement[i]) != len(canonical[i]) ||
			bytes.Equal(replacement[i], canonical[i]) {
			b.Fatalf("replacement %d does not preserve canonical size", i)
		}
		keyBytes[i] = []byte(keys[i])
	}
	latency := make([]int64, b.N)
	at := 0
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		doc := replacement[at%documents]
		if at/documents&1 != 0 {
			doc = canonical[at%documents]
		}
		start := time.Now()
		if _, err := collection.Put(keyBytes[at%documents], doc); err != nil {
			b.Fatal(err)
		}
		latency[i] = time.Since(start).Nanoseconds()
		at++
	}
	b.StopTimer()
	reportP50(b, latency)
}

func benchmarkOverflowGetRaw(b *testing.B) {
	// Overflow chains cannot enter through the bulk path (inline-only
	// constraint), so build small and rewrite every document through Put; the
	// reads then all take the chain-reassembly path in both representations.
	n := 4000
	keys, docs := unifiedCompetitiveCorpus(n, false)
	options := unifiedBenchOptions()
	// The overflow rewrite loop below runs through the buffered mutation lane
	// (the harness's profile), which owns the overflow chain scratch; the
	// bulk-only async lane is not built for thousands of chain mints.
	options.Durability = DurabilityBufferedVisible
	collection := unifiedBenchStoreWith(b, keys, docs, options, options)
	for i := range keys {
		if _, err := collection.Put([]byte(keys[i]), unifiedOverflowDoc(i)); err != nil {
			b.Fatalf("Put overflow: %v", err)
		}
		// Chain mints stage pages fast; checkpoint on a cadence the buffered
		// lane's bounded staging accepts (setup cost, untimed).
		if i%128 == 127 {
			if err := collection.Flush(); err != nil {
				b.Fatal(err)
			}
		}
	}
	if err := collection.Flush(); err != nil {
		b.Fatal(err)
	}
	snapshot, err := collection.Snapshot()
	if err != nil {
		b.Fatal(err)
	}
	defer snapshot.Close()
	probeKeys := shuffledKeyBytes(keys)
	dst := make([]byte, 0, 8192)
	lat := make([]int64, b.N)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := probeKeys[i%len(probeKeys)]
		start := time.Now()
		out, found, err := snapshot.AppendRaw(dst[:0], key)
		lat[i] = time.Since(start).Nanoseconds()
		if err != nil || !found {
			b.Fatalf("AppendRaw(%q) = (%v,%v)", key, found, err)
		}
		dst = out[:0]
	}
	b.StopTimer()
	reportP50(b, lat)
}

// BenchmarkUnifiedGetRawOverflow is the §10.1 chain-reassembly gate.
func BenchmarkUnifiedGetRawOverflow(b *testing.B) { benchmarkOverflowGetRaw(b) }

// BenchmarkUnifiedFieldProbe is the §10.2 gate: point field probe p50
// ≤ 0.30 µs end-to-end (pipeline floor + hole read), 0 allocs.
func BenchmarkUnifiedFieldProbe(b *testing.B) {
	keys, docs, _ := benchCorpus(b)
	collection := unifiedBenchStore(b, keys, docs, unifiedBenchOptions())
	snapshot, err := collection.Snapshot()
	if err != nil {
		b.Fatal(err)
	}
	defer snapshot.Close()
	probe, err := NewFieldProbe("/country")
	if err != nil {
		b.Fatal(err)
	}
	// Warm the per-(leaf, template) resolution cache: population is a
	// boundary cost amortized over the snapshot's lifetime (§10.2).
	dst := make([]byte, 0, 256)
	for i := range keys {
		if _, _, err := snapshot.AppendField(dst[:0], probe, []byte(keys[i])); err != nil {
			b.Fatal(err)
		}
	}
	probeKeys := shuffledKeyBytes(keys)
	lat := make([]int64, b.N)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := probeKeys[i%len(probeKeys)]
		start := time.Now()
		out, found, err := snapshot.AppendField(dst[:0], probe, key)
		lat[i] = time.Since(start).Nanoseconds()
		if err != nil || !found {
			b.Fatalf("AppendField(%q) = (%v,%v)", key, found, err)
		}
		dst = out[:0]
	}
	b.StopTimer()
	measureBatchP50(b, min(b.N, 320_000), func(i int) {
		out, found, err := snapshot.AppendField(dst[:0], probe, probeKeys[i%len(probeKeys)])
		if err != nil || !found {
			b.Fatal("AppendField")
		}
		dst = out[:0]
	})
	reportP50(b, lat)
}

// BenchmarkUnifiedCopyThenParseProbe is the pattern the probe replaces
// (§10.2's baseline): whole-document AppendRaw plus a path walk over the
// copy, measured through the same resolver for a like-for-like parse cost.
func BenchmarkUnifiedCopyThenParseProbe(b *testing.B) {
	keys, docs, _ := benchCorpus(b)
	collection := unifiedBenchStore(b, keys, docs, unifiedBenchOptions())
	snapshot, err := collection.Snapshot()
	if err != nil {
		b.Fatal(err)
	}
	defer snapshot.Close()
	probe, err := NewFieldProbe("/country")
	if err != nil {
		b.Fatal(err)
	}
	probeKeys := shuffledKeyBytes(keys)
	dst := make([]byte, 0, 4096)
	field := make([]byte, 0, 256)
	lat := make([]int64, b.N)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := probeKeys[i%len(probeKeys)]
		start := time.Now()
		doc, found, err := snapshot.AppendRaw(dst[:0], key)
		if err != nil || !found {
			b.Fatal("AppendRaw")
		}
		dst = doc
		out, found, err := probe.appendPathOf(field[:0], dst)
		lat[i] = time.Since(start).Nanoseconds()
		if err != nil || !found {
			b.Fatal("appendPathOf")
		}
		field = out[:0]
	}
	b.StopTimer()
	reportP50(b, lat)
}

func benchmarkScanAll(b *testing.B, highCardinality bool) {
	n := 100_000
	if testing.Short() {
		n = 10_000
	}
	keys, docs := unifiedCompetitiveCorpus(n, highCardinality)
	collection := unifiedBenchStore(b, keys, docs, unifiedBenchOptions())
	snapshot, err := collection.Snapshot()
	if err != nil {
		b.Fatal(err)
	}
	defer snapshot.Close()
	var sink byte
	rows := 0
	visit := func(key, value []byte) error {
		sink ^= value[0] ^ value[len(value)-1]
		rows++
		return nil
	}
	// Prime the snapshot-owned splice scratch. Cold growth is bounded setup;
	// repeated scans are the steady-state lane and must allocate nothing.
	if _, err := snapshot.RangeRawBuffer(nil, visit); err != nil {
		b.Fatal(err)
	}
	if rows != n {
		b.Fatalf("warm scan visited %d want %d", rows, n)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows = 0
		if _, err := snapshot.RangeRawBuffer(nil, visit); err != nil {
			b.Fatal(err)
		}
		if rows != n {
			b.Fatalf("scanned %d want %d", rows, n)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*n), "ns/doc")
	benchScanSink = sink
}

var benchScanSink byte

// The two scan gates exercise the sole durable format at both dictionary
// extremes. Low cardinality stresses templated/dictionary reconstruction;
// high cardinality prevents a fast shared-value path from hiding row-render
// regressions.
func BenchmarkUnifiedScanAllLowCardinality(b *testing.B) {
	benchmarkScanAll(b, false)
}

func BenchmarkUnifiedScanAllHighCardinality(b *testing.B) {
	benchmarkScanAll(b, true)
}

func benchmarkFilterEq(b *testing.B, path, needle string) {
	keys, docs, n := benchCorpus(b)
	collection := unifiedBenchStore(b, keys, docs, unifiedBenchOptions())
	snapshot, err := collection.Snapshot()
	if err != nil {
		b.Fatal(err)
	}
	defer snapshot.Close()
	filter, err := NewEqFilter(path, []byte(needle))
	if err != nil {
		b.Fatal(err)
	}
	warm, err := snapshot.FilterEqCount(filter)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	var result FilterEqResult
	for i := 0; i < b.N; i++ {
		result, err = snapshot.FilterEqCount(filter)
		if err != nil {
			b.Fatal(err)
		}
		if result != warm || result.Scanned != n {
			b.Fatalf("filter drift: %+v vs %+v", result, warm)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*n), "ns/doc")
	b.ReportMetric(float64(result.Matched), "matched")
	b.ReportMetric(float64(result.Fallback), "fallback-rows")
}

// BenchmarkUnifiedFilterEq is the §10.3 gate lane: the harness filter shape
// (country == "PT") over the token lane, gate ≤ 40 ns/doc, target ≤ 20.
func BenchmarkUnifiedFilterEq(b *testing.B) {
	benchmarkFilterEq(b, "/country", `"PT"`)
}

// BenchmarkUnifiedFilterEqFallback drives the recorded fallback lane: a
// container-valued path resolves on no template as a token compare, so every
// row renders and evaluates (§10.3's render-then-filter number).
func BenchmarkUnifiedFilterEqFallback(b *testing.B) {
	benchmarkFilterEq(b, "/profile", `{"joined":"2020-01-02","region":"eu-west-1","tier":"pro"}`)
}
