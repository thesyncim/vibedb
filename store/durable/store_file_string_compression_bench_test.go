package durable

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
)

const (
	stringCompressionBenchRows      = 20_000
	stringCompressionBenchBatchSize = 64
)

var stringCompressionBenchDistinct = [...]int{2, 4, 8, 16, 32, 64, 128}

var (
	stringCompressionBenchBytes []byte
	stringCompressionBenchFound bool
	stringCompressionBenchErr   error
)

// stringCompressionMix is SplitMix64. It gives the fixture deterministic,
// nontrivial tokens without making setup depend on a random source.
func stringCompressionMix(value uint64) uint64 {
	value += 0x9e3779b97f4a7c15
	value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9
	value = (value ^ (value >> 27)) * 0x94d049bb133111eb
	return value ^ (value >> 31)
}

func stringCompressionValues() [][]byte {
	const alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz-_=+,.!?@#$%^&*()[]{}:;<>/|~"
	values := make([][]byte, 128)
	for id := range values {
		state := stringCompressionMix(uint64(id) + 91)
		token := make([]byte, 96)
		for i := range token {
			state = stringCompressionMix(state + uint64(i))
			token[i] = alphabet[state%uint64(len(alphabet))]
		}
		value := make([]byte, 0, len(token)*2+12)
		value = append(value, token...)
		value = append(value, "/checkpoint/"...)
		value = append(value, token...)
		values[id] = value
	}
	return values
}

// The control values are long and intentionally have no repeated fragment
// within an entry. Repetition exists only across documents through the small
// dictionary, so dictionary encoding remains useful while byte compression of
// each unique entry should be rejected.
func stringCompressionIncompressibleValues() [][]byte {
	const alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz-_=+,.!?@#$%^&*()[]{}:;<>/|~"
	values := make([][]byte, 32)
	for id := range values {
		state := stringCompressionMix(uint64(id) + 0x5eed)
		value := make([]byte, 384)
		for i := range value {
			state = stringCompressionMix(state + uint64(i))
			value[i] = alphabet[state%uint64(len(alphabet))]
		}
		values[id] = value
	}
	return values
}

func stringCompressionDocument(dst []byte, row, distinct int, values [][]byte) []byte {
	return stringCompressionDocumentVariant(dst, row, row%distinct, values)
}

func stringCompressionDocumentVariant(dst []byte, row, variant int, values [][]byte) []byte {
	dst = fmt.Appendf(dst[:0], `{"id":%d,"url":"`, row)
	dst = append(dst, values[variant]...)
	return append(dst, `"}`...)
}

func stringCompressionCorpus(tb testing.TB, rows, distinct int) ([]string, [][]byte) {
	tb.Helper()
	return stringCompressionCorpusWithValues(tb, rows, distinct, stringCompressionValues())
}

func stringCompressionCorpusWithValues(
	tb testing.TB, rows, distinct int, values [][]byte,
) ([]string, [][]byte) {
	tb.Helper()
	keys := make([]string, rows)
	documents := make([][]byte, rows)
	for row := range rows {
		keys[row] = fmt.Sprintf("url-%09d", row)
		documents[row] = stringCompressionDocument(nil, row, distinct, values)
	}
	return keys, documents
}

func stringCompressionConsume(value []byte) byte {
	var sink byte
	for _, current := range value {
		sink ^= current
	}
	return sink
}

func stringCompressionSource(tb testing.TB, keys []string, documents [][]byte) *store.Collection {
	tb.Helper()
	builder, err := store.NewBuilder(store.Options{ChunkDocuments: 64})
	if err != nil {
		tb.Fatal(err)
	}
	for i := range keys {
		if err := builder.Append(keys[i], documents[i]); err != nil {
			tb.Fatal(err)
		}
	}
	source, err := builder.Build()
	if err != nil {
		tb.Fatal(err)
	}
	return source
}

func stringCompressionOptions() Options {
	return Options{
		Collection:        store.Options{ChunkDocuments: 64},
		ResidentBytes:     128 << 20,
		Backend:           BackendPortable,
		Durability:        DurabilityBufferedVisible,
		MaxBatchDocuments: stringCompressionBenchBatchSize,
		MaxRetiredExtents: 1 << 16,
	}
}

func stringCompressionCreate(
	tb testing.TB, path string, keys []string, documents [][]byte,
) int64 {
	tb.Helper()
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		tb.Fatal(err)
	}
	size, err := CreateFromPrimary(
		stringCompressionSource(tb, keys, documents), file, stringCompressionOptions(),
	)
	if err != nil {
		_ = file.Close()
		tb.Fatal(err)
	}
	if err := file.Close(); err != nil {
		tb.Fatal(err)
	}
	return size
}

func stringCompressionOpen(tb testing.TB, path string) (*Collection, *os.File) {
	tb.Helper()
	file, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		tb.Fatal(err)
	}
	collection, err := Open(file, stringCompressionOptions())
	if err != nil {
		_ = file.Close()
		tb.Fatal(err)
	}
	return collection, file
}

func stringCompressionClose(tb testing.TB, collection *Collection, file *os.File) {
	tb.Helper()
	if err := collection.Close(); err != nil {
		tb.Errorf("close collection: %v", err)
	}
	if err := file.Close(); err != nil {
		tb.Errorf("close file: %v", err)
	}
}

// This is both a correctness guard for the benchmark fixture and a reopen
// check for every cardinality used by the paired space/performance run.
func TestStringCompressionCheckpointedReopenCorrectness(t *testing.T) {
	const rows = 1_024
	for _, distinct := range stringCompressionBenchDistinct {
		t.Run(fmt.Sprintf("distinct=%d", distinct), func(t *testing.T) {
			keys, documents := stringCompressionCorpus(t, rows, distinct)
			path := filepath.Join(t.TempDir(), "strings.vibe")
			size := stringCompressionCreate(t, path, keys, documents)
			if size <= 0 {
				t.Fatalf("checkpointed file bytes = %d", size)
			}
			collection, file := stringCompressionOpen(t, path)
			defer stringCompressionClose(t, collection, file)
			for row := range rows {
				got, found, err := collection.AppendRaw(nil, []byte(keys[row]))
				if err != nil || !found || !bytes.Equal(got, documents[row]) {
					t.Fatalf("row %d after reopen: found=%v err=%v got=%q want=%q", row, found, err, got, documents[row])
				}
			}
		})
	}
}

// TestStringCompressionIncompressibleControlRepresentation proves the control
// remains in inline compact leaves and that /url is represented by a
// dictionary stream. Combined with the deterministic high-entropy entries,
// this keeps the control on the raw-dictionary candidate rather than overflow.
func TestStringCompressionIncompressibleControlRepresentation(t *testing.T) {
	const rows = 1_024
	values := stringCompressionIncompressibleValues()
	rawDictionaryPayloadBytes := map[int]int{8: 3698, 32: 13266}
	for _, distinct := range []int{8, 32} {
		t.Run(fmt.Sprintf("distinct=%d", distinct), func(t *testing.T) {
			keys, documents := stringCompressionCorpusWithValues(t, rows, distinct, values)
			path := filepath.Join(t.TempDir(), "incompressible.vibe")
			stringCompressionCreate(t, path, keys, documents)
			collection, file := stringCompressionOpen(t, path)
			defer stringCompressionClose(t, collection, file)
			census := collectUnifiedLeafCensus(t, collection)
			if census.payloadBytes != rawDictionaryPayloadBytes[distinct] {
				t.Fatalf(
					"compact payload bytes=%d want raw-dictionary baseline=%d",
					census.payloadBytes, rawDictionaryPayloadBytes[distinct],
				)
			}

			var resolver storeio.UnifiedHoleResolver
			if err := resolver.SetPath([]byte("/url")); err != nil {
				t.Fatal(err)
			}
			needle := append([]byte{'"'}, values[0]...)
			needle = append(needle, '"')
			router := collection.primaryRouter.Load()
			if router == nil {
				t.Fatal("missing primary router")
			}
			seenRows := 0
			for rank := 0; rank < router.Len(); rank++ {
				route, ok := router.RouteAtRank(rank)
				if !ok {
					t.Fatalf("route at rank %d", rank)
				}
				lease, err := router.AcquireLeaf(collection.cache, route)
				if err != nil {
					t.Fatal(err)
				}
				view, ok := storeio.AdmittedCompactPrimaryStripe(
					lease.Page(), collection.state.Load().root.StoreID, route.Bucket,
				)
				if !ok {
					lease.Release()
					t.Fatalf("leaf %d is not a compact primary stripe", rank)
				}
				if view.HasOverflowRows() {
					lease.Release()
					t.Fatalf("leaf %d placed control values in overflow", rank)
				}
				if _, supported := view.CountResolvedDictionaryEqual(&resolver, needle); !supported {
					lease.Release()
					t.Fatalf("leaf %d /url is not dictionary represented", rank)
				}
				seenRows += view.Len()
				lease.Release()
			}
			if seenRows != rows {
				t.Fatalf("represented rows=%d want=%d", seenRows, rows)
			}
		})
	}
}

// BenchmarkStringCompressionBulkCheckpoint reports the complete checkpointed
// file size as bytes per document while timing the public bulk-build path.
func BenchmarkStringCompressionBulkCheckpoint(b *testing.B) {
	for _, distinct := range stringCompressionBenchDistinct {
		b.Run(fmt.Sprintf("distinct=%d", distinct), func(b *testing.B) {
			keys, documents := stringCompressionCorpus(b, stringCompressionBenchRows, distinct)
			var totalBytes int64
			b.ResetTimer()
			for i := 0; b.Loop(); i++ {
				path := filepath.Join(b.TempDir(), fmt.Sprintf("bulk-%d.vibe", i))
				totalBytes += stringCompressionCreate(b, path, keys, documents)
			}
			b.StopTimer()
			b.ReportMetric(float64(totalBytes)/float64(b.N*stringCompressionBenchRows), "fileB/doc")
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*stringCompressionBenchRows), "ns/doc")
		})
	}
}

// BenchmarkStringCompressionGetRaw measures full-row reconstruction from a
// reopened, resident collection. Setup and a complete warmup are untimed.
func BenchmarkStringCompressionGetRaw(b *testing.B) {
	for _, distinct := range stringCompressionBenchDistinct {
		b.Run(fmt.Sprintf("distinct=%d", distinct), func(b *testing.B) {
			keys, documents := stringCompressionCorpus(b, stringCompressionBenchRows, distinct)
			path := filepath.Join(b.TempDir(), "read.vibe")
			stringCompressionCreate(b, path, keys, documents)
			collection, file := stringCompressionOpen(b, path)
			defer stringCompressionClose(b, collection, file)
			var dst []byte
			for row := range keys {
				out, found, err := collection.AppendRaw(dst[:0], []byte(keys[row]))
				if err != nil || !found || !bytes.Equal(out, documents[row]) {
					b.Fatalf("warmup row %d: found=%v err=%v", row, found, err)
				}
				dst = out[:0]
			}
			keyBytes := make([][]byte, len(keys))
			for i := range keys {
				keyBytes[i] = []byte(keys[(i*8191)%len(keys)])
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; b.Loop(); i++ {
				stringCompressionBenchBytes, stringCompressionBenchFound, stringCompressionBenchErr =
					collection.AppendRaw(dst[:0], keyBytes[i%len(keyBytes)])
				if stringCompressionBenchErr != nil || !stringCompressionBenchFound {
					b.Fatalf("resident read: found=%v err=%v", stringCompressionBenchFound, stringCompressionBenchErr)
				}
				dst = stringCompressionBenchBytes[:0]
			}
		})
	}
}

// BenchmarkStringCompressionScanAllBytes exercises RangeRawBuffer's compact
// primary scan decoder and consumes every reconstructed document byte. This is
// deliberately separate from the point path because scan decoding retains
// dictionary fragments across adjacent rows.
func BenchmarkStringCompressionScanAllBytes(b *testing.B) {
	for _, distinct := range stringCompressionBenchDistinct {
		b.Run(fmt.Sprintf("distinct=%d", distinct), func(b *testing.B) {
			keys, documents := stringCompressionCorpus(b, stringCompressionBenchRows, distinct)
			path := filepath.Join(b.TempDir(), "scan.vibe")
			stringCompressionCreate(b, path, keys, documents)
			collection, file := stringCompressionOpen(b, path)
			defer stringCompressionClose(b, collection, file)
			snapshot, err := collection.Snapshot()
			if err != nil {
				b.Fatal(err)
			}
			defer snapshot.Close()
			var sink byte
			rows := 0
			bytesPerScan := int64(0)
			visit := func(_ []byte, value []byte) error {
				sink ^= stringCompressionConsume(value)
				rows++
				return nil
			}
			if _, err := snapshot.RangeRawBuffer(nil, func(_ []byte, value []byte) error {
				bytesPerScan += int64(len(value))
				rows++
				return nil
			}); err != nil {
				b.Fatal(err)
			}
			if rows != stringCompressionBenchRows {
				b.Fatalf("warm scan rows=%d want=%d", rows, stringCompressionBenchRows)
			}
			b.SetBytes(bytesPerScan)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				rows = 0
				if _, err := snapshot.RangeRawBuffer(nil, visit); err != nil {
					b.Fatal(err)
				}
				if rows != stringCompressionBenchRows {
					b.Fatalf("scan rows=%d want=%d", rows, stringCompressionBenchRows)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*stringCompressionBenchRows), "ns/doc")
			stringCompressionBenchBytes = []byte{sink}
		})
	}
}

func BenchmarkStringCompressionIncompressibleBulkCheckpoint(b *testing.B) {
	for _, distinct := range []int{8, 32} {
		b.Run(fmt.Sprintf("distinct=%d", distinct), func(b *testing.B) {
			keys, documents := stringCompressionCorpusWithValues(
				b, stringCompressionBenchRows, distinct, stringCompressionIncompressibleValues(),
			)
			var totalBytes int64
			b.ResetTimer()
			for i := 0; b.Loop(); i++ {
				path := filepath.Join(b.TempDir(), fmt.Sprintf("incompressible-%d.vibe", i))
				totalBytes += stringCompressionCreate(b, path, keys, documents)
			}
			b.StopTimer()
			b.ReportMetric(float64(totalBytes)/float64(b.N*stringCompressionBenchRows), "fileB/doc")
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*stringCompressionBenchRows), "ns/doc")
		})
	}
}

func BenchmarkStringCompressionIncompressibleGetRaw(b *testing.B) {
	for _, distinct := range []int{8, 32} {
		b.Run(fmt.Sprintf("distinct=%d", distinct), func(b *testing.B) {
			keys, documents := stringCompressionCorpusWithValues(
				b, stringCompressionBenchRows, distinct, stringCompressionIncompressibleValues(),
			)
			path := filepath.Join(b.TempDir(), "incompressible-read.vibe")
			stringCompressionCreate(b, path, keys, documents)
			collection, file := stringCompressionOpen(b, path)
			defer stringCompressionClose(b, collection, file)
			var dst []byte
			for row := range keys {
				out, found, err := collection.AppendRaw(dst[:0], []byte(keys[row]))
				if err != nil || !found || !bytes.Equal(out, documents[row]) {
					b.Fatalf("warmup row %d: found=%v err=%v", row, found, err)
				}
				dst = out[:0]
			}
			keyBytes := make([][]byte, len(keys))
			for i := range keys {
				keyBytes[i] = []byte(keys[(i*8191)%len(keys)])
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; b.Loop(); i++ {
				stringCompressionBenchBytes, stringCompressionBenchFound, stringCompressionBenchErr =
					collection.AppendRaw(dst[:0], keyBytes[i%len(keyBytes)])
				if stringCompressionBenchErr != nil || !stringCompressionBenchFound {
					b.Fatalf("resident read: found=%v err=%v", stringCompressionBenchFound, stringCompressionBenchErr)
				}
				dst = stringCompressionBenchBytes[:0]
			}
		})
	}
}

func BenchmarkStringCompressionBatchInsert(b *testing.B) {
	values := stringCompressionValues()
	for _, distinct := range stringCompressionBenchDistinct {
		b.Run(fmt.Sprintf("distinct=%d", distinct), func(b *testing.B) {
			path := filepath.Join(b.TempDir(), "insert.vibe")
			seedKeys, seedDocuments := stringCompressionCorpus(b, 1, distinct)
			stringCompressionCreate(b, path, seedKeys, seedDocuments)
			collection, file := stringCompressionOpen(b, path)
			defer stringCompressionClose(b, collection, file)
			var key, document []byte
			b.ResetTimer()
			for iteration := 0; b.Loop(); iteration++ {
				start := iteration*stringCompressionBenchBatchSize + 1
				if err := collection.Update(func(batch *WriteBatch) error {
					for offset := range stringCompressionBenchBatchSize {
						row := start + offset
						key = fmt.Appendf(key[:0], "url-%09d", row)
						document = stringCompressionDocument(document[:0], row, distinct, values)
						if err := batch.Put(key, document); err != nil {
							return err
						}
					}
					return nil
				}); err != nil {
					b.Fatal(err)
				}
			}
			if err := collection.Flush(); err != nil {
				b.Fatal(err)
			}
			b.StopTimer()
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*stringCompressionBenchBatchSize), "e2e-ns/doc")
		})
	}
}

func BenchmarkStringCompressionExistingValueUpdate(b *testing.B) {
	const rows = 10_000
	values := stringCompressionValues()
	for _, distinct := range stringCompressionBenchDistinct {
		b.Run(fmt.Sprintf("distinct=%d", distinct), func(b *testing.B) {
			keys, documents := stringCompressionCorpus(b, rows, distinct)
			path := filepath.Join(b.TempDir(), "replace.vibe")
			stringCompressionCreate(b, path, keys, documents)
			collection, file := stringCompressionOpen(b, path)
			defer stringCompressionClose(b, collection, file)
			keyBytes := make([][]byte, rows)
			for row := range rows {
				keyBytes[row] = []byte(keys[row])
			}
			var document []byte
			b.ResetTimer()
			for iteration := 0; b.Loop(); iteration++ {
				start := iteration * stringCompressionBenchBatchSize
				if err := collection.Update(func(batch *WriteBatch) error {
					for offset := range stringCompressionBenchBatchSize {
						row := (start + offset*8191) % rows
						document = stringCompressionDocumentVariant(
							document[:0], row, (row+iteration+1)%distinct, values,
						)
						if err := batch.Put(keyBytes[row], document); err != nil {
							return err
						}
					}
					return nil
				}); err != nil {
					b.Fatal(err)
				}
			}
			if err := collection.Flush(); err != nil {
				b.Fatal(err)
			}
			b.StopTimer()
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*stringCompressionBenchBatchSize), "e2e-ns/doc")
		})
	}
}
