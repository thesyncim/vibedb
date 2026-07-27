package durable

import (
	"fmt"
	"os"
	"testing"

	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibedb/store"
)

// The multi-column exact probe is the one read path that opens a candidate's
// stored JSON and indexes it per row, so it is where a sizing pass in front of
// the build is paid per row rather than per query. This benchmark isolates it:
// the corpus is built so every candidate collides on the probe's tuple hash
// and therefore reaches the document recheck, and the reported metric is per
// rechecked row so the number is independent of corpus size.
//
// The caveat this exists to settle is whether the recheck is I/O bound. It
// reports DocumentRecheckRows so a run can be read against the physical work
// it did, and the documents are small and few enough to sit in the page cache,
// which is the arm where a per-row CPU saving is visible at all.

// benchExactProbeFile builds a two-column exactly indexed file whose rows all
// share one indexed tuple, so a probe for that tuple admits every row as a
// candidate and rechecks each one against its stored document. The second
// indexed column alternates between two numbers too wide to compare by value,
// so they share a hash bucket and the leaf is marked collided: that is what
// withholds the tuple certificate the probe would otherwise decide from, and
// with a certificate no document is ever opened and this path is not reached.
func benchExactProbeFile(b *testing.B, rows int) *Snapshot {
	b.Helper()
	file, err := os.CreateTemp(b.TempDir(), "file-exact-probe-*")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { file.Close() })
	fs, err := Create(file, Options{
		Collection: store.Options{ChunkDocuments: 64},
		Durability: DurabilityAsyncVisible,
		Indexes: []store.IndexDefinition{{
			Name: "kv", Paths: []string{"/k", "/v"},
		}},
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { fs.Close() })
	for row := range rows {
		document := fmt.Sprintf(
			`{"k":"shared","v":%de100000,"id":%d,"name":"item-%06d","score":%d,`+
				`"tags":["alpha","beta","gamma"],"obj":{"x":%d,"live":true},"note":"padding text %d"}`,
			1+row%2, row, row, row*3, row%5, row)
		if _, err := fs.Put(fmt.Sprintf("k%06d", row), []byte(document)); err != nil {
			b.Fatal(err)
		}
	}
	snapshot, err := fs.Snapshot()
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { snapshot.Close() })
	return snapshot
}

// BenchmarkFileExactProbeRecheck times the per-row document recheck of a
// multi-column exact probe.
func BenchmarkFileExactProbeRecheck(b *testing.B) {
	const rows = 4096
	snapshot := benchExactProbeFile(b, rows)

	key, value := benchNeedle(b, `"shared"`), benchNeedle(b, `1e100000`)

	var workspace IndexWorkspace
	var masks []store.Mask
	masks, err := snapshot.AppendIndexMasksInto(masks[:0], &workspace, "kv", key, value)
	if err != nil {
		b.Fatal(err)
	}
	rechecked := workspace.LastProbeStats().DocumentRecheckRows
	if rechecked == 0 {
		b.Fatal("probe decided every row from certificates; the recheck path was never entered")
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		masks, err = snapshot.AppendIndexMasksInto(masks[:0], &workspace, "kv", key, value)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(rechecked), "ns/recheck")
	b.ReportMetric(float64(rechecked), "rechecks")
}

// benchNeedle indexes one scalar probe needle onto storage of its own.
func benchNeedle(b *testing.B, src string) vibejson.Index {
	b.Helper()
	needed, err := vibejson.RequiredIndexEntries([]byte(src))
	if err != nil {
		b.Fatal(err)
	}
	index, err := vibejson.BuildIndex([]byte(src), make([]vibejson.IndexEntry, needed))
	if err != nil {
		b.Fatal(err)
	}
	return index
}
