package durable

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/store"
)

func openInsertScalingBulkFixture(tb testing.TB, rows int) (*Collection, string, Options, func()) {
	tb.Helper()
	path := filepath.Join(tb.TempDir(), "insert-tail.vibe")
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		tb.Fatal(err)
	}
	builder, err := store.NewBuilder(store.Options{})
	if err != nil {
		tb.Fatal(err)
	}
	key := make([]byte, 0, 32)
	document := make([]byte, 0, batch64DocumentCapacity)
	for row := range rows {
		key = fmt.Appendf(key[:0], "user-%09d", row)
		document = fmt.Appendf(document[:0], `{"id":%d,"value":"`, row)
		document = appendBatch64VariedPayload(document, uint64(row))
		document = append(document, `"}`...)
		if err := builder.Append(string(key), document); err != nil {
			tb.Fatal(err)
		}
	}
	built, err := builder.Build()
	if err != nil {
		tb.Fatal(err)
	}
	options := Options{ResidentBytes: 512 << 20, Backend: BackendPortable,
		Durability: DurabilityBufferedVisible}
	if _, err := CreateFromPrimary(built, file, options); err != nil {
		tb.Fatal(err)
	}
	collection, err := Open(file, options)
	if err != nil {
		tb.Fatal(err)
	}
	return collection, path, options, func() {
		if err := collection.Close(); err != nil {
			tb.Fatal(err)
		}
		_ = file.Close()
	}
}

// BenchmarkFilePrimaryBatch64AppendTailScaling measures only buffered Update
// calls plus one final Flush after an untimed bulk-built varied-256B preseed.
// Use -benchtime=8x..32x; all measured tail documents are prepared beforehand.
func BenchmarkFilePrimaryBatch64AppendTailScaling(b *testing.B) {
	const maxBatches = 32
	for _, preseed := range []int{16_384, 262_144, 1_048_576} {
		b.Run(fmt.Sprintf("preseed=%d", preseed), func(b *testing.B) {
			collection, path, options, done := openInsertScalingBulkFixture(b, preseed)
			keys := make([][]byte, maxBatches*batch64Rows)
			documents := make([][]byte, len(keys))
			for at := range keys {
				ordinal := uint64(preseed + at)
				keys[at] = fmt.Appendf(nil, "user-%09d", ordinal)
				documents[at] = fmt.Appendf(nil, `{"id":%d,"value":"`, ordinal)
				documents[at] = appendBatch64VariedPayload(documents[at], ordinal)
				documents[at] = append(documents[at], `"}`...)
			}
			base := collection.Stats()
			b.ReportAllocs()
			b.ResetTimer()
			if b.N > maxBatches {
				b.Fatalf("run with -benchtime at most %dx", maxBatches)
			}
			for batchIndex := 0; batchIndex < b.N; batchIndex++ {
				first := batchIndex * batch64Rows
				if err := collection.Update(func(batch *WriteBatch) error {
					for row := range batch64Rows {
						at := first + row
						if err := batch.Put(keys[at], documents[at]); err != nil {
							return err
						}
					}
					return nil
				}); err != nil {
					b.Fatal(err)
				}
			}
			flushStart := time.Now()
			if err := collection.Flush(); err != nil {
				b.Fatal(err)
			}
			flushElapsed := time.Since(flushStart)
			b.StopTimer()
			after := collection.Stats()
			written := b.N * batch64Rows
			verify := func(label string, reader *Collection) {
				b.Helper()
				for at := range written {
					raw, found, err := reader.AppendRaw(nil, keys[at])
					if err != nil || !found || !bytes.Equal(raw, documents[at]) {
						b.Fatalf("%s tail row %d = %q,%v,%v, want %q",
							label, at, raw, found, err, documents[at])
					}
				}
			}
			verify("live", collection)
			done()
			reopenFile, err := os.OpenFile(path, os.O_RDWR, 0o600)
			if err != nil {
				b.Fatal(err)
			}
			reopened, err := Open(reopenFile, options)
			if err != nil {
				_ = reopenFile.Close()
				b.Fatal(err)
			}
			verify("reopened", reopened)
			if err := reopened.Close(); err != nil {
				b.Fatal(err)
			}
			if err := reopenFile.Close(); err != nil {
				b.Fatal(err)
			}
			rows := float64(b.N * batch64Rows)
			if rows == 0 {
				return
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/rows, "final-flush-ns/row")
			b.ReportMetric(float64(flushElapsed.Nanoseconds()), "flush-ns")
			b.ReportMetric(float64(after.PrimaryLeafSplits-base.PrimaryLeafSplits), "splits")
			b.ReportMetric(float64(after.DeviceBytes-base.DeviceBytes)/rows, "device-B/row")
		})
	}
}
