package raftstore

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	pb "go.etcd.io/raft/v3/raftpb"
)

// BenchmarkNodeStorePersistDurability measures the canonical node-wide log
// with its real platform durability primitive. Setup, incarnation allocation,
// and shutdown are outside the timer. The multi-group case is one durability
// wave, not a loop of independent group writes.
func BenchmarkNodeStorePersistDurability(b *testing.B) {
	for _, groups := range []int{1, 8} {
		b.Run(fmt.Sprintf("groups=%d", groups), func(b *testing.B) {
			b.StopTimer()
			options := NodeStoreOptions{
				Store:           testOptions(),
				FrameBytes:      1 << 20,
				Events:          128 << 10,
				WaveIDs:         32 << 10,
				EntriesPerGroup: 32 << 10,
				CachedSegments:  1,
				Groups:          64,
			}
			bootstraps := make([]NodeBootstrap, groups)
			groupKeys := make([]uint64, groups)
			ready := make([]NodeReady, groups)
			entries := make([]pb.Entry, groups)
			entryPointers := make([]*pb.Entry, groups)
			hard := make([]pb.HardState, groups)
			indexes := make([]uint64, groups)
			terms := make([]uint64, groups)
			payloads := make([][1]byte, groups)
			for i := range groups {
				key := uint64(i + 1)
				bootstraps[i] = NodeBootstrap{
					Descriptor: testGroupDescriptor(key),
					Snapshot:   nodeSnapshot(key, 1, 1),
				}
				groupKeys[i] = key
				payloads[i][0] = 'x'
				entries[i].Index = &indexes[i]
				entries[i].Term = &terms[i]
				entries[i].Data = payloads[i][:]
				hard[i].Term = &terms[i]
				hard[i].Commit = &indexes[i]
				entryPointers[i] = &entries[i]
				ready[i] = NodeReady{
					GroupID: key,
					Batch: raftmodel.PersistBatch{
						Entries:   entryPointers[i : i+1],
						HardState: &hard[i],
					},
				}
			}
			store, err := CreateNodeStore(filepath.Join(b.TempDir(), "node"), testNodeIdentity(), testKey(), bootstraps, options)
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = store.Close() })
			incarnations, err := store.BeginIncarnations(groupKeys)
			if err != nil {
				b.Fatal(err)
			}
			for i := range ready {
				ready[i].Batch.NodeIncarnation = incarnations[i].Incarnation
			}

			b.ReportAllocs()
			b.SetBytes(int64(groups))
			b.ResetTimer()
			b.StartTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				for i := range ready {
					indexes[i] = uint64(iteration + 2)
					terms[i] = uint64(iteration + 2)
					ready[i].Batch.ReadyID = uint64(iteration + 1)
				}
				if err = store.PersistWave(ready); err != nil {
					b.Fatalf("iteration=%d: %v", iteration, err)
				}
			}
		})
	}
}
