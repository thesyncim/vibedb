package durable

import (
	"bytes"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
)

// pointReadCostFixture peels the warm point-read path into independently
// measurable pieces. The complete locations and document references are
// prepared outside the timer only so each arm can isolate one layer; the
// resolver and AppendRaw arms still perform the production path end to end.
type pointReadCostFixture struct {
	collection *Collection
	snapshot   *Snapshot
	close      func()
	keys       []string
	hashes     []uint64
	locations  []storeio.PageKeyLocation
	documents  []storeio.PageRef
	random     []int
	ordered    []int
}

func openPointReadCostFixture(b *testing.B) *pointReadCostFixture {
	b.Helper()
	collection, closeCollection := openBenchReadCollection(
		b, benchReadCorpusSize, DocumentFormatVerbatim,
	)
	snapshot, err := collection.Snapshot()
	if err != nil {
		closeCollection()
		b.Fatal(err)
	}
	fixture := &pointReadCostFixture{
		collection: collection,
		snapshot:   snapshot,
		close: func() {
			_ = snapshot.Close()
			closeCollection()
		},
		keys:      make([]string, benchReadCorpusSize),
		hashes:    make([]uint64, benchReadCorpusSize),
		locations: make([]storeio.PageKeyLocation, benchReadCorpusSize),
		documents: make([]storeio.PageRef, benchReadCorpusSize),
		random:    benchReadProbeOrder(benchReadCorpusSize),
		ordered:   make([]int, benchReadCorpusSize),
	}
	for i := range benchReadCorpusSize {
		fixture.keys[i] = benchReadKey(i)
		fixture.hashes[i] = storeio.KeyHash(collection.storeID, fixture.keys[i])
		fixture.ordered[i] = i
		match, ok, resolveErr := collection.resolveFileFingerprint(
			snapshot.state, []byte(fixture.keys[i]),
		)
		if resolveErr != nil || !ok {
			fixture.close()
			b.Fatalf("prepare key %d: ok=%v err=%v", i, ok, resolveErr)
		}
		fixture.locations[i] = match.location
		fixture.documents[i] = match.documentRef
		match.Release()
	}
	return fixture
}

var (
	pointReadCostHashSink     uint64
	pointReadCostLocationSink storeio.PageKeyLocation
	pointReadCostRefSink      storeio.PageRef
	pointReadCostLengthSink   int
)

func reportPointReadPins(b *testing.B, collection *Collection, base Stats) {
	b.Helper()
	if b.N == 0 {
		return
	}
	stats := collection.Stats()
	acquires := (stats.CacheHits + stats.CacheMisses) - (base.CacheHits + base.CacheMisses)
	b.ReportMetric(float64(acquires)/float64(b.N), "pins/op")
	b.ReportMetric(float64(stats.CacheMisses-base.CacheMisses)/float64(b.N), "miss/op")
}

// BenchmarkFileStorePointReadCosts exposes the dominant warm-read costs
// separately and in composition. Both random and insertion-ordered probes are
// hard measurement lanes: a routing change that wins random lookups by
// damaging locality must show that trade here rather than hiding it in one
// aggregate number.
func BenchmarkFileStorePointReadCosts(b *testing.B) {
	if testing.Short() {
		b.Skip("100k-document corpus is too slow for -short")
	}
	fixture := openPointReadCostFixture(b)
	defer fixture.close()

	orders := []struct {
		name  string
		order []int
	}{
		{name: "random", order: fixture.random},
		{name: "ordered", order: fixture.ordered},
	}
	for _, probes := range orders {
		b.Run(probes.name, func(b *testing.B) {
			b.Run("key-hash", func(b *testing.B) {
				at := 0
				var hash uint64
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					index := probes.order[at]
					hash = storeio.KeyHash(fixture.collection.storeID, fixture.keys[index])
					if at++; at == len(probes.order) {
						at = 0
					}
				}
				pointReadCostHashSink = hash
			})

			b.Run("fingerprint-tree", func(b *testing.B) {
				at := 0
				var location storeio.PageKeyLocation
				base := fixture.collection.Stats()
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					index := probes.order[at]
					cursor, err := storeio.OpenPageKeyTreeCursor(
						fixture.collection.cache,
						fixture.snapshot.state.keyRoot,
						fixture.hashes[index],
						filePageKeyTreeBounds(fixture.snapshot.state),
					)
					if err != nil {
						b.Fatal(err)
					}
					var ok bool
					location, ok, err = cursor.Next()
					cursor.Close()
					if err != nil || !ok || location != fixture.locations[index] {
						b.Fatalf("fingerprint: location=%+v ok=%v err=%v", location, ok, err)
					}
					if at++; at == len(probes.order) {
						at = 0
					}
				}
				b.StopTimer()
				pointReadCostLocationSink = location
				reportPointReadPins(b, fixture.collection, base)
			})

			b.Run("fingerprint-first", func(b *testing.B) {
				at := 0
				var location storeio.PageKeyLocation
				base := fixture.collection.Stats()
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					index := probes.order[at]
					var ok bool
					var err error
					location, ok, err = storeio.FirstPageKeyTreeCandidate(
						fixture.collection.cache,
						fixture.snapshot.state.keyRoot,
						fixture.hashes[index],
						filePageKeyTreeBounds(fixture.snapshot.state),
					)
					if err != nil || !ok || location != fixture.locations[index] {
						b.Fatalf("fingerprint first: location=%+v ok=%v err=%v", location, ok, err)
					}
					if at++; at == len(probes.order) {
						at = 0
					}
				}
				b.StopTimer()
				pointReadCostLocationSink = location
				reportPointReadPins(b, fixture.collection, base)
			})

			b.Run("chunk-tree", func(b *testing.B) {
				at := 0
				var document storeio.PageRef
				base := fixture.collection.Stats()
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					index := probes.order[at]
					var ok bool
					var err error
					document, ok, err = storeio.LookupChunkTree(
						fixture.collection.cache,
						fixture.snapshot.state.chunkRoot,
						fixture.locations[index].Chunk,
						storeio.ChunkTreeBounds{
							FileEnd:       fixture.snapshot.state.super.FileEnd,
							NextLogicalID: fixture.snapshot.state.root.NextLogicalID,
						},
					)
					if err != nil || !ok || document != fixture.documents[index] {
						b.Fatalf("chunk: document=%+v ok=%v err=%v", document, ok, err)
					}
					if at++; at == len(probes.order) {
						at = 0
					}
				}
				b.StopTimer()
				pointReadCostRefSink = document
				reportPointReadPins(b, fixture.collection, base)
			})

			b.Run("document-pin-key-value", func(b *testing.B) {
				at := 0
				length := 0
				base := fixture.collection.Stats()
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					index := probes.order[at]
					lease, err := fixture.collection.cache.Acquire(fixture.documents[index])
					if err != nil {
						b.Fatal(err)
					}
					view, err := admittedFileDocumentChunk(
						lease.Page(), fixture.documents[index], fixture.locations[index].Chunk,
					)
					if err != nil {
						lease.Release()
						b.Fatal(err)
					}
					record, ok := view.lookup(fixture.locations[index].Slot)
					if !ok || !bytes.Equal(record.key, []byte(fixture.keys[index])) {
						lease.Release()
						b.Fatal("document candidate did not verify")
					}
					length += int(record.value.value.Length)
					lease.Release()
					if at++; at == len(probes.order) {
						at = 0
					}
				}
				b.StopTimer()
				pointReadCostLengthSink = length
				reportPointReadPins(b, fixture.collection, base)
			})

			b.Run("resolver", func(b *testing.B) {
				at := 0
				length := 0
				base := fixture.collection.Stats()
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					index := probes.order[at]
					match, ok, err := fixture.collection.resolveFileFingerprint(
						fixture.snapshot.state, []byte(fixture.keys[index]),
					)
					if err != nil || !ok {
						b.Fatalf("resolve: ok=%v err=%v", ok, err)
					}
					length += int(match.value.value.Length)
					match.Release()
					if at++; at == len(probes.order) {
						at = 0
					}
				}
				b.StopTimer()
				pointReadCostLengthSink = length
				reportPointReadPins(b, fixture.collection, base)
			})

			b.Run("append-raw", func(b *testing.B) {
				at := 0
				buffer := make([]byte, 0, 512)
				base := fixture.collection.Stats()
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					index := probes.order[at]
					out, ok, err := fixture.snapshot.AppendRaw(buffer[:0], fixture.keys[index])
					if err != nil || !ok {
						b.Fatalf("AppendRaw: ok=%v err=%v", ok, err)
					}
					buffer = out
					if at++; at == len(probes.order) {
						at = 0
					}
				}
				b.StopTimer()
				pointReadCostLengthSink = len(buffer)
				reportPointReadPins(b, fixture.collection, base)
			})
		})
	}
}
