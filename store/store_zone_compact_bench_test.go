package store

import (
	"fmt"
	"testing"
)

// The durable summary's whole write-path cost is this fold: one linear pass
// over the document being written, one hash and one three-entry scan per
// top-level member. It is measured on its own because the durable write path it
// sits in is dominated by page encoding and fsync, where a sub-microsecond fold
// is not distinguishable from noise — which is itself the result, but not one a
// benchmark of Put can state honestly.
func BenchmarkZoneSummaryFold(b *testing.B) {
	documents := make([][]byte, 0, 1024)
	for i := range 1024 {
		documents = append(documents, fmt.Appendf(nil,
			`{"v":%d,"bucket":%d,"label":"row-%06d","note":"lorem ipsum dolor sit amet consectetur adipiscing elit %d"}`,
			i, i%97, i, i))
	}
	bytesPerDocument := 0
	for _, d := range documents {
		bytesPerDocument += len(d)
	}
	bytesPerDocument /= len(documents)
	b.SetBytes(int64(bytesPerDocument))
	b.ReportAllocs()
	var summary ZoneSummary
	summary.Reset()
	i := 0
	for b.Loop() {
		if i%64 == 0 {
			summary.Reset()
		}
		summary.Fold(documents[i%len(documents)], i%64)
		i++
	}
}

// BenchmarkZoneSummaryCodec is the cost of moving a summary between its packed
// 30-byte durable form and the decoded value a probe reads. A chunk-summary
// probe pays one decode per chunk, so this is what the pruning walk costs per
// chunk on top of the page read it shares with the scan.
func BenchmarkZoneSummaryCodec(b *testing.B) {
	var summary ZoneSummary
	summary.Reset()
	summary.Fold([]byte(`{"v":12345,"bucket":7,"label":"row-000042"}`), 0)
	var encoded [ZoneCompactBytes]byte
	summary.Encode(&encoded)
	probe := ZoneProbe{Path: ZonePathHash("v"), Lo: 1, Hi: 1, Op: ZoneOpEq}
	b.ReportAllocs()
	var out ZoneSummary
	keep := false
	for b.Loop() {
		out.Decode(&encoded)
		keep = out.Keep(probe)
	}
	if keep {
		b.SetBytes(0)
	}
}
