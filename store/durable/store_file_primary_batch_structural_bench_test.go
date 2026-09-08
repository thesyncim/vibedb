package durable

import (
	"strconv"
	"testing"
	"time"
)

const (
	batch64Rows             = 64
	batch64Payload          = 256
	batch64DocumentCapacity = batch64Payload + 64
	batch64KeyDigits        = 20
)

// BenchmarkFilePrimaryBatch64SequentialInsert measures the durable cost of
// appending one bounded batch of ascending primary keys. Each benchmark
// iteration is exactly 64 rows, so -benchtime=Nx gives an exact N*64-row
// workload without allocating a corpus proportional to N.
//
// The final Flush remains inside the benchmark timer. Its whole-call latency is
// also reported separately because a single final fence is amortised across
// all rows and would otherwise be hidden by the ordinary ns/op number.
// This is a storage-local buffered-visible workload; the final fence makes the
// cumulative image durable, but the benchmark does not model a replicated SQL
// or Raft acknowledgement.
func BenchmarkFilePrimaryBatch64SequentialInsert(b *testing.B) {
	const batchRows = batch64Rows
	options := benchBatchOptions(batchRows)
	collection, done := openBenchCollection(b, options)
	defer done()

	// Fixed storage keeps the input working set at one batch. Keys are rebuilt
	// per iteration to stay lexicographically ascending while the document
	// buffers retain their capacity. The value field in each document is exactly
	// 256 bytes; the JSON envelope is intentionally additional document data.
	var (
		keyStorage   [batchRows][32]byte
		keyLengths   [batchRows]int
		documentData [batchRows][batch64DocumentCapacity]byte
		documentLens [batchRows]int
	)
	base := collection.Stats()
	// SetBytes reports only the logical bytes in each 256-byte value field;
	// JSON envelopes and primary keys are included in the work but not this
	// throughput denominator.
	b.SetBytes(int64(batchRows * batch64Payload))
	b.ReportAllocs()
	b.ResetTimer()
	// Keep an explicit b.N loop so the final Flush below remains part of the
	// measured durability scope. With -benchtime=Nx this executes exactly N
	// batches; b.Loop would stop the timer before the flush.
	for batchIndex := uint64(0); batchIndex < uint64(b.N); batchIndex++ {
		firstRow := batchIndex * batchRows
		for row := range batchRows {
			key := keyStorage[row][:0]
			key = append(key, "row-"...)
			key = appendBatch64FixedUint(key, firstRow+uint64(row))
			keyLengths[row] = len(key)

			document := documentData[row][:0]
			document = append(document, `{"id":`...)
			document = strconv.AppendUint(document, firstRow+uint64(row), 10)
			// This is the same row-seeded splitmix64+xorshift64-v1 stream used by
			// the retained 10M SQL workload, emitted directly into the fixed batch
			// buffer.
			document = append(document, `,"value":"`...)
			document = appendBatch64VariedPayload(document, firstRow+uint64(row))
			document = append(document, `"}`...)
			documentLens[row] = len(document)
		}

		if err := collection.Update(func(batch *WriteBatch) error {
			for row := range batchRows {
				if err := batch.Put(
					keyStorage[row][:keyLengths[row]],
					documentData[row][:documentLens[row]],
				); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			b.Fatal(err)
		}
	}

	flushStarted := time.Now()
	if err := collection.Flush(); err != nil {
		b.Fatal(err)
	}
	finalFlush := time.Since(flushStarted)
	b.StopTimer()

	after := collection.Stats()
	rows := uint64(b.N) * batchRows
	if rows == 0 {
		return
	}
	// These are deltas from the empty collection. DeviceBytes is the committer's
	// submitted device-payload counter, not host physical-write amplification;
	// it excludes SQL, Raft, and recovery-journal traffic. CommittedBatches is
	// committer-generation count, not the number of 64-row Update calls.
	// PrimaryLeafSplits counts structural publication events (a K-way split is
	// one event), and DeviceCommits counts device-fence submissions. The input
	// byte rate above counts only the 256-byte value fields; key and JSON-envelope
	// bytes remain part of the storage operation.
	metricPerRow := func(value uint64, name string) {
		b.ReportMetric(float64(value)/float64(rows), name)
	}
	metricPerRow(
		after.PrimaryStructuralRoutingStagedBytes-
			base.PrimaryStructuralRoutingStagedBytes,
		"primary-structural-routing-staged-B/row",
	)
	metricPerRow(
		after.PrimaryStructuralRoutingRetiredBytes-
			base.PrimaryStructuralRoutingRetiredBytes,
		"primary-structural-routing-retired-B/row",
	)
	metricPerRow(
		after.PrimaryTabletRoutingRebuilds-base.PrimaryTabletRoutingRebuilds,
		"primary-tablet-routing-rebuilds/row",
	)
	metricPerRow(
		after.PrimaryLeafSplits-base.PrimaryLeafSplits,
		"primary-leaf-splits/row",
	)
	metricPerRow(after.DeviceBytes-base.DeviceBytes, "device-submitted-B/row")
	metricPerRow(
		after.CommittedBatches-base.CommittedBatches,
		"committer-generations/row",
	)
	metricPerRow(after.DeviceCommits-base.DeviceCommits, "device-commit-fences/row")
	b.ReportMetric(float64(finalFlush.Nanoseconds()), "final-flush-ns")
	// This includes all buffered Update work plus the single final Flush fence
	// amortized per row; it is not a replicated SQL acknowledgement latency.
	b.ReportMetric(
		float64(b.Elapsed().Nanoseconds())/float64(rows),
		"buffered-final-flush-ns/row",
	)
}

const batch64PayloadAlphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz-_"

func batch64MixOrdinal(value uint64) uint64 {
	value += 0x9e3779b97f4a7c15
	value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9
	value = (value ^ (value >> 27)) * 0x94d049bb133111eb
	return value ^ (value >> 31)
}

func appendBatch64VariedPayload(dst []byte, row uint64) []byte {
	x := batch64MixOrdinal(row + 0x1d2b79f5aa33cc77)
	for range batch64Payload {
		x ^= x >> 12
		x ^= x << 25
		x ^= x >> 27
		x *= 0x2545f4914f6cdd1d
		dst = append(dst, batch64PayloadAlphabet[(x>>58)&63])
	}
	return dst
}

func appendBatch64FixedUint(dst []byte, value uint64) []byte {
	const width = batch64KeyDigits
	start := len(dst)
	dst = dst[:start+width]
	for at := len(dst) - 1; at >= start; at-- {
		dst[at] = byte('0' + value%10)
		value /= 10
	}
	return dst
}
