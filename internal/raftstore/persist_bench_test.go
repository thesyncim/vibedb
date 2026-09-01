package raftstore

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	pb "go.etcd.io/raft/v3/raftpb"
)

func BenchmarkPersistReadyDurability(b *testing.B) {
	for _, entryCount := range []int{1, 64} {
		b.Run(fmt.Sprintf("entries-%d", entryCount), func(b *testing.B) {
			options := testOptions()
			options.MaxFileBytes = 512 << 20
			options.MaxRecords = 64 << 10
			path := filepath.Join(b.TempDir(), "benchmark.wal")
			store, err := Create(path, testIdentity(), testKey(), testBootstrap(), options)
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = store.Close() })
			incarnation, err := store.BeginIncarnation()
			if err != nil {
				b.Fatal(err)
			}

			const payload = "benchmark-payload"
			entries := make([]*pb.Entry, entryCount)
			for index := range entries {
				entries[index] = entry(uint64(index+2), 2, payload)
			}
			hardState := hard(2, 1)
			batch := raftmodel.PersistBatch{
				NodeIncarnation: incarnation,
				HardState:       hardState,
				Entries:         entries,
				MustSync:        true,
			}

			originalRead := store.options.ops.readAt
			originalWrite := store.options.ops.writeAt
			originalBarrier := testRecordBarrier(store)
			originalSync := store.options.ops.sync
			var reads, writes, recordBarriers, finalSyncs, proofs uint64
			store.options.ops.observeNamespaceProof = func() { proofs++ }
			store.options.ops.readAt = func(file *os.File, data []byte, offset int64) (int, error) {
				reads++
				return originalRead(file, data, offset)
			}
			store.options.ops.writeAt = func(file *os.File, data []byte, offset int64) (int, error) {
				writes++
				return originalWrite(file, data, offset)
			}
			store.options.ops.recordBarrier = func(file *os.File) error {
				recordBarriers++
				return originalBarrier(file)
			}
			store.options.ops.sync = func(file *os.File) error {
				finalSyncs++
				return originalSync(file)
			}
			beforeSyncs := store.SyncCount()

			b.ReportAllocs()
			b.SetBytes(int64(entryCount * len(payload)))
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				term := uint64(iteration + 2)
				*hardState.Term = term
				for _, item := range entries {
					*item.Term = term
				}
				batch.ReadyID = uint64(iteration + 1)
				if err := store.Persist(batch); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()

			ready := float64(b.N)
			b.ReportMetric(float64(recordBarriers)/ready, "record-barriers/ready")
			b.ReportMetric(float64(finalSyncs)/ready, "final-syncs/ready")
			b.ReportMetric(float64(proofs)/ready, "namespace-proofs/ready")
			b.ReportMetric(float64(reads)/ready, "slot-reads/ready")
			b.ReportMetric(float64(writes)/ready, "writes/ready")
			b.ReportMetric(float64(store.SyncCount()-beforeSyncs)/ready, "sync-phases/ready")
			b.ReportMetric(float64(recordBarriers+finalSyncs)/(ready*float64(entryCount)), "sync-phases/entry")
		})
	}
}
