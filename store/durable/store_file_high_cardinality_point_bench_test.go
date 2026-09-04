package durable

import (
	"bytes"
	"testing"

	"github.com/thesyncim/vibejson"
)

// Shuffled point reads reconstruct and consume complete high-cardinality JSON.
// Fixture creation, reopen, and an exact-byte warmup are outside the timer.
func BenchmarkUnifiedGetRawHighCardinalityAllBytes(b *testing.B) {
	keys, docs := unifiedCompetitiveCorpus(100_000, true)
	collection := unifiedBenchStore(b, keys, docs, unifiedBenchOptions())
	snapshot, err := collection.Snapshot()
	if err != nil {
		b.Fatal(err)
	}
	defer snapshot.Close()
	dst := make([]byte, 0, 4096)
	var expected []byte
	for i, key := range keys {
		var err error
		expected, err = vibejson.AppendCanonicalize(expected[:0], docs[i])
		if err != nil {
			b.Fatal(err)
		}
		out, found, err := snapshot.AppendRaw(dst[:0], []byte(key))
		if err != nil || !found || !bytes.Equal(out, expected) {
			b.Fatalf("warmup row=%d found=%v err=%v", i, found, err)
		}
	}
	probes := shuffledKeyBytes(keys)
	var sink byte
	at := 0
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		out, found, err := snapshot.AppendRaw(dst[:0], probes[at])
		if err != nil || !found {
			b.Fatalf("point read found=%v err=%v", found, err)
		}
		sink ^= touchUnifiedScanAllBytes(out)
		dst = out[:0]
		at++
		if at == len(probes) {
			at = 0
		}
	}
	benchScanSink = sink
}
