package durable

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
)

func openBenchPrimaryPutCollection(
	tb testing.TB,
	primary bool,
	count int,
) (*Collection, func()) {
	tb.Helper()
	path := filepath.Join(tb.TempDir(), "primary-put.vibe")
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		tb.Fatal(err)
	}
	builder, err := store.NewBuilder(store.Options{})
	if err != nil {
		tb.Fatal(err)
	}
	for at := range count {
		if err := builder.Append(
			benchReadKey(at), benchReadDocument(at),
		); err != nil {
			tb.Fatal(err)
		}
	}
	built, err := builder.Build()
	if err != nil {
		tb.Fatal(err)
	}
	options := Options{
		ResidentBytes: 64 << 20, Backend: BackendPortable,
		Durability: DurabilityBufferedVisible,
	}
	if primary {
		_, err = CreateFromPrimary(built, file, options)
	} else {
		_, err = CreateFromPrimary(built, file, options)
	}
	if err != nil {
		tb.Fatal(err)
	}
	collection, err := Open(file, options)
	if err != nil {
		tb.Fatal(err)
	}
	return collection, func() {
		if closeErr := collection.Close(); closeErr != nil {
			tb.Fatal(closeErr)
		}
		_ = file.Close()
	}
}

// BenchmarkFilePrimaryPut compares the ordered-primary leaf path with the
// legacy 20 KiB chunk path under the same buffered-visible acknowledgement
// contract. Uniform alternates value sizes so every operation is an honest
// COW; hot-repeat alternates equal-sized values after the lazy first touch so
// it measures canonical-frame acknowledgement.
func BenchmarkFilePrimaryPut(b *testing.B) {
	const corpus = 10_000
	for _, implementation := range []struct {
		name    string
		primary bool
	}{
		{name: "primary", primary: true},
		{name: "legacy-chunk", primary: false},
	} {
		b.Run(implementation.name, func(b *testing.B) {
			b.Run("uniform-cow", func(b *testing.B) {
				collection, done := openBenchPrimaryPutCollection(
					b, implementation.primary, corpus,
				)
				defer done()
				short := []byte(`{"value":"short"}`)
				long := []byte(`{"value":"a deliberately longer value"}`)
				// Keys are precomputed as []byte outside the timed loop: the
				// store speaks []byte, so the loop must not pay a per-op
				// fmt.Sprintf or string->[]byte conversion. That conversion was
				// one of the two allocs/op this bench used to report.
				keys := make([][]byte, corpus)
				for j := range keys {
					keys[j] = []byte(benchReadKey(j))
				}
				base := collection.Stats()
				at := 0
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					value := short
					if at/corpus&1 != 0 {
						value = long
					}
					if _, err := collection.Put(
						keys[at%corpus], value,
					); err != nil {
						b.Fatal(err)
					}
					at++
				}
				b.StopTimer()
				if err := collection.Flush(); err != nil {
					b.Fatal(err)
				}
				reportPrimaryPutMetrics(
					b, collection.Stats(), base,
				)
			})
			b.Run("hot-repeat-same-size", func(b *testing.B) {
				collection, done := openBenchPrimaryPutCollection(
					b, implementation.primary, corpus,
				)
				defer done()
				first := []byte(`{"value":"hot-a"}`)
				second := []byte(`{"value":"hot-b"}`)
				key := []byte(benchReadKey(corpus / 2))
				// Size-changing COW, same-size lazy first touch, then one
				// canonical update leave the timed loop on its hot lane.
				for _, value := range [][]byte{first, second, first} {
					if _, err := collection.Put(key, value); err != nil {
						b.Fatal(err)
					}
				}
				base := collection.Stats()
				at := 0
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					value := first
					if at&1 != 0 {
						value = second
					}
					if _, err := collection.Put(key, value); err != nil {
						b.Fatal(err)
					}
					at++
				}
				b.StopTimer()
				if err := collection.Flush(); err != nil {
					b.Fatal(err)
				}
				reportPrimaryPutMetrics(
					b, collection.Stats(), base,
				)
			})
		})
	}
}

// BenchmarkFilePrimaryPointReadUnifiedShape measures class-5 point reads over
// a shape-redundant corpus, including row-body reconstruction.
func BenchmarkFilePrimaryPointReadUnifiedShape(b *testing.B) {
	if testing.Short() {
		b.Skip("100k-document corpus is too slow for -short")
	}
	const count = 100_000
	built, keys, _ := buildRedundantPrimaryCorpus(b, count)
	options := Options{ResidentBytes: 64 << 20, Backend: BackendPortable}
	file, err := os.OpenFile(
		filepath.Join(b.TempDir(), "primary-unified-read.vibe"),
		os.O_RDWR|os.O_CREATE, 0o600,
	)
	if err != nil {
		b.Fatal(err)
	}
	defer file.Close()
	if _, err := CreateFromPrimary(built, file, options); err != nil {
		b.Fatal(err)
	}
	collection, err := Open(file, options)
	if err != nil {
		b.Fatal(err)
	}
	defer collection.Close()
	classCounts := filePrimaryLeafClassCounts(
		b, file, collection.state.Load().root,
	)
	if classCounts[storeio.CommonPrimaryLeafUnified] == 0 {
		b.Fatalf("no unified leaves selected; split = %v", classCounts)
	}
	b.Logf("unified leaves=%d", classCounts[storeio.CommonPrimaryLeafUnified])

	order := benchReadProbeOrder(count)
	// Precompute probe keys as []byte outside the timed loop to hold the
	// zero-alloc read pin (the store's AppendRaw takes []byte).
	probes := make([][]byte, len(order))
	for at := range probes {
		probes[at] = []byte(keys[order[at]])
	}
	buffer := make([]byte, 0, 512)
	for _, key := range probes {
		out, ok, err := collection.AppendRaw(buffer[:0], key)
		if err != nil || !ok {
			b.Fatalf("warm AppendRaw: ok=%v err=%v", ok, err)
		}
		buffer = out
	}
	at := 0
	b.ReportAllocs()
	for b.Loop() {
		out, ok, err := collection.AppendRaw(buffer[:0], probes[at])
		if err != nil || !ok {
			b.Fatalf("AppendRaw: ok=%v err=%v", ok, err)
		}
		buffer = out
		if at++; at == len(probes) {
			at = 0
		}
	}
}

func reportPrimaryPutMetrics(
	b *testing.B,
	after, before Stats,
) {
	if b.N == 0 {
		return
	}
	b.ReportMetric(
		float64(after.DeviceBytes-before.DeviceBytes)/float64(b.N),
		"device-B/op",
	)
	b.ReportMetric(
		float64(
			after.BufferedInplaceUpdates-
				before.BufferedInplaceUpdates,
		)/float64(b.N),
		"inplace/op",
	)
}

func openBenchPrimaryReadCollection(
	tb testing.TB, count int,
) (*Collection, func()) {
	tb.Helper()
	path := filepath.Join(tb.TempDir(), "primary-read.vibe")
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		tb.Fatal(err)
	}
	builder, err := store.NewBuilder(store.Options{})
	if err != nil {
		tb.Fatal(err)
	}
	for at := range count {
		if err := builder.Append(
			benchReadKey(at), benchReadDocument(at),
		); err != nil {
			tb.Fatal(err)
		}
	}
	built, err := builder.Build()
	if err != nil {
		tb.Fatal(err)
	}
	options := Options{
		ResidentBytes: 64 << 20, Backend: BackendPortable,
	}
	if _, err := CreateFromPrimary(built, file, options); err != nil {
		tb.Fatal(err)
	}
	collection, err := Open(file, options)
	if err != nil {
		tb.Fatal(err)
	}
	return collection, func() {
		if closeErr := collection.Close(); closeErr != nil {
			tb.Fatal(closeErr)
		}
		_ = file.Close()
	}
}

// BenchmarkFilePrimaryPointRead mirrors BenchmarkFileStorePointRead over the
// same 100k documents and fixed random permutation. It reports the collection
// convenience form and the caller-pinned Snapshot separately, with allocation
// and residency metrics kept visible. On an Apple M4 Max (darwin/arm64), the
// phase-I correctness-first seqlock run measured 425 ns/op for Collection and
// 423 ns/op for Snapshot, both at 0 B/op and 0 allocs/op. These honest
// end-to-end results still do not meet the 300 ns promotion gate.
func BenchmarkFilePrimaryPointRead(b *testing.B) {
	if testing.Short() {
		b.Skip("100k-document corpus is too slow for -short")
	}
	collection, done := openBenchPrimaryReadCollection(
		b, benchReadCorpusSize,
	)
	defer done()
	order := benchReadProbeOrder(benchReadCorpusSize)
	// Precompute probe keys as []byte outside the timed loop to hold the
	// zero-alloc read pin (the store's AppendRaw takes []byte).
	keys := make([][]byte, len(order))
	for at := range keys {
		keys[at] = []byte(benchReadKey(order[at]))
	}
	warm := make([]byte, 0, 512)
	for _, key := range keys {
		out, ok, err := collection.AppendRaw(warm[:0], key)
		if err != nil || !ok {
			b.Fatalf("warm AppendRaw: ok=%v err=%v", ok, err)
		}
		warm = out
	}

	b.Run("collection", func(b *testing.B) {
		buffer := make([]byte, 0, 512)
		at := 0
		base := collection.Stats()
		b.ReportAllocs()
		for b.Loop() {
			out, ok, err := collection.AppendRaw(
				buffer[:0], keys[at],
			)
			if err != nil || !ok {
				b.Fatalf("AppendRaw: ok=%v err=%v", ok, err)
			}
			buffer = out
			if at++; at == len(keys) {
				at = 0
			}
		}
		b.StopTimer()
		reportReadResidency(b, collection.Stats(), base)
		b.ReportMetric(
			float64(collection.primaryRouter.Load().ResidentBytes())/
				float64(benchReadCorpusSize),
			"router-B/doc",
		)
		b.ReportMetric(
			float64(collection.primaryRouter.Load().BuildDuration().Nanoseconds()),
			"router-build-ns",
		)
	})

	b.Run("snapshot", func(b *testing.B) {
		snapshot, err := collection.Snapshot()
		if err != nil {
			b.Fatal(err)
		}
		defer snapshot.Close()
		buffer := make([]byte, 0, 512)
		at := 0
		base := collection.Stats()
		b.ReportAllocs()
		for b.Loop() {
			out, ok, err := snapshot.AppendRaw(
				buffer[:0], keys[at],
			)
			if err != nil || !ok {
				b.Fatalf("AppendRaw: ok=%v err=%v", ok, err)
			}
			buffer = out
			if at++; at == len(keys) {
				at = 0
			}
		}
		b.StopTimer()
		reportReadResidency(b, collection.Stats(), base)
		b.ReportMetric(
			float64(collection.primaryRouter.Load().ResidentBytes())/
				float64(benchReadCorpusSize),
			"router-B/doc",
		)
	})
}
