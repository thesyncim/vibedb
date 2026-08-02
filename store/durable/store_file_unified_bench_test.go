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

// The unified-read benchmark battery builds every store from the same
// competitive corpus (100k ~249 B documents) and reports p50 nanoseconds for
// point pipelines and nanoseconds per document for scan-shaped lanes.

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

// BenchmarkUnifiedGetRaw targets point-read p50 ≤ 0.50 µs
// (kill-switch ceiling 0.70 µs), 0 allocs.
func BenchmarkUnifiedGetRaw(b *testing.B) { benchmarkPointGetRaw(b) }

// BenchmarkUnifiedPrimaryReplace drives uniform
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

// BenchmarkUnifiedGetRawOverflow measures overflow-chain reassembly.
func BenchmarkUnifiedGetRawOverflow(b *testing.B) { benchmarkOverflowGetRaw(b) }

// BenchmarkUnifiedFieldProbe targets point field-probe p50
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
	// boundary cost amortized over the snapshot's lifetime.
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

// BenchmarkUnifiedCopyThenParseProbe is the pattern the probe replaces:
// whole-document AppendRaw plus a path walk over the
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

func benchmarkUnifiedRenderedScan(b *testing.B, highCardinality, consumeAllBytes bool) {
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

	// Prime the snapshot-owned splice scratch and count the actual canonical
	// bytes returned by this corpus. Cold growth is bounded setup; every gate
	// below measures and enforces the allocation-free steady state.
	renderedBytes := int64(0)
	warmRows := 0
	if _, err := snapshot.RangeRawBuffer(nil, func(_ []byte, value []byte) error {
		renderedBytes += int64(len(value))
		warmRows++
		return nil
	}); err != nil {
		b.Fatal(err)
	}
	if warmRows != n {
		b.Fatalf("warm scan visited %d want %d", warmRows, n)
	}

	var sink byte
	rows := 0
	var visit func(key, value []byte) error
	if consumeAllBytes {
		visit = func(_ []byte, value []byte) error {
			sink ^= touchUnifiedScanAllBytes(value)
			rows++
			return nil
		}
	} else {
		visit = func(_ []byte, value []byte) error {
			sink ^= value[0] ^ value[len(value)-1]
			rows++
			return nil
		}
	}

	var allocationErr error
	allocs := testing.AllocsPerRun(5, func() {
		rows = 0
		_, allocationErr = snapshot.RangeRawBuffer(nil, visit)
	})
	if allocationErr != nil {
		b.Fatalf("allocation probe: %v", allocationErr)
	}
	if rows != n {
		b.Fatalf("allocation probe visited %d want %d", rows, n)
	}
	if allocs != 0 {
		b.Fatalf("warmed scan allocated %.2f times, want 0", allocs)
	}
	if consumeAllBytes {
		b.SetBytes(renderedBytes)
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
	if consumeAllBytes {
		b.ReportMetric(float64(renderedBytes)/float64(n), "bytes/doc")
	}
	benchScanSink = sink
}

func touchUnifiedScanAllBytes(value []byte) byte {
	var sink byte
	for _, b := range value {
		sink ^= b
	}
	return sink
}

var benchScanSink byte

// The canonical-render gates exercise exact JSON reconstruction at both
// dictionary extremes. Their first+last-byte sink prevents dead-code removal
// without pretending to measure consumption of the whole returned document.
func BenchmarkUnifiedCanonicalRenderLowCardinality(b *testing.B) {
	benchmarkUnifiedRenderedScan(b, false, false)
}

func BenchmarkUnifiedCanonicalRenderHighCardinality(b *testing.B) {
	benchmarkUnifiedRenderedScan(b, true, false)
}

// The all-bytes gates include the same canonical renderer, then serially touch
// every returned byte. They report the actual bytes per document and fail if a
// warmed scan allocates, making this the honest local analogue of the
// cross-engine BenchmarkScanAllBytes lane.
func BenchmarkUnifiedScanAllBytesLowCardinality(b *testing.B) {
	benchmarkUnifiedRenderedScan(b, false, true)
}

func BenchmarkUnifiedScanAllBytesHighCardinality(b *testing.B) {
	benchmarkUnifiedRenderedScan(b, true, true)
}

func benchmarkFilterEq(b *testing.B, path, needle string) {
	benchmarkFilterEqMode(b, path, needle, false)
}

func benchmarkScalarFilterEq(b *testing.B, path, needle string) {
	benchmarkFilterEqMode(b, path, needle, true)
}

func benchmarkFilterEqMode(b *testing.B, path, needle string, scalar bool) {
	benchmarkFilterEqCardinality(b, path, needle, scalar, false)
}

func benchmarkFilterEqCardinality(
	b *testing.B,
	path, needle string,
	scalar, highCardinality bool,
) {
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
	var filter *EqFilter
	if scalar {
		filter, err = NewScalarEqFilter(path, []byte(needle))
	} else {
		filter, err = NewEqFilter(path, []byte(needle))
	}
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

// BenchmarkUnifiedFilterEq measures the harness filter shape
// (country == "PT") over the token lane, gate ≤ 40 ns/doc, target ≤ 20.
func BenchmarkUnifiedFilterEq(b *testing.B) {
	benchmarkFilterEq(b, "/country", `"PT"`)
}

// BenchmarkUnifiedFilterEqNested measures the same token scan through a
// nested template resolution, which is still performed once per leaf shape.
func BenchmarkUnifiedFilterEqNested(b *testing.B) {
	benchmarkScalarFilterEq(b, "/profile/tier", `"pro"`)
}

// BenchmarkUnifiedFilterEqNumber measures exact-decimal value equality. The
// decimal needle intentionally differs from the corpus's integer spelling.
func BenchmarkUnifiedFilterEqNumber(b *testing.B) {
	benchmarkScalarFilterEq(b, "/score", `500.0`)
}

// BenchmarkUnifiedFilterEqDate scans the packed date ordinal lane without
// reconstructing its quoted Gregorian spelling per row.
func BenchmarkUnifiedFilterEqDate(b *testing.B) {
	benchmarkFilterEq(b, "/profile/joined", `"2020-01-02"`)
}

// BenchmarkUnifiedFilterEqHighCardinalityString exercises a non-dictionary
// scalar stream. The missing needle forces a complete front-coded value scan
// while keeping selectivity from dominating the measurement.
func BenchmarkUnifiedFilterEqHighCardinalityString(b *testing.B) {
	benchmarkFilterEqCardinality(
		b, "/note", `"not-present-in-the-corpus"`, false, true,
	)
}

// BenchmarkUnifiedFilterEqFallback drives the recorded fallback lane: a
// container-valued path resolves on no template as a token compare, so every
// row renders and evaluates.
func BenchmarkUnifiedFilterEqFallback(b *testing.B) {
	benchmarkFilterEq(b, "/profile", `{"joined":"2020-01-02","region":"eu-west-1","tier":"pro"}`)
}
