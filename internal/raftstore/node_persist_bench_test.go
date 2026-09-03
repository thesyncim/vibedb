package raftstore

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	pb "go.etcd.io/raft/v3/raftpb"
)

func BenchmarkNodeStoreOpenDescriptorCatalog256Groups(b *testing.B) {
	dir := filepath.Join(b.TempDir(), "node")
	options := NodeStoreOptions{MaxWaveBytes: 1 << 20, MaxSegmentEvents: 4096, RecentWaves: 1024, MaxEntriesPerGroup: DefaultNodeMaxEntriesPerGroup, ReaderSlots: 2, MaxGroups: 512}
	store, err := CreateNodeStore(dir, testNodeIdentity(), testKey(), []NodeBootstrap{{Descriptor: testGroupDescriptor(1), Snapshot: nodeSnapshot(1, 1, 1)}}, options)
	if err != nil {
		b.Fatal(err)
	}
	store.SetDataSyncForTesting(func(*os.File) error { return nil })
	for group := uint64(2); group <= 256; group++ {
		if _, err = store.RegisterGroup(testGroupDescriptor(group)); err != nil {
			b.Fatal(err)
		}
		if group%32 == 0 {
			if err = store.engine.Rotate(nil); err != nil {
				b.Fatal(err)
			}
			if err = store.engine.WaitSeal(); err != nil {
				b.Fatal(err)
			}
		}
	}
	if err = store.CheckpointDescriptorCatalog(); err != nil {
		b.Fatal(err)
	}
	if err = store.engine.Rotate(nil); err != nil {
		b.Fatal(err)
	}
	if err = store.engine.WaitSeal(); err != nil {
		b.Fatal(err)
	}
	if err = store.ReclaimDeadNodeLogPrefix(); err != nil {
		b.Fatal(err)
	}
	if err = store.Close(); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		store, err = OpenNodeStore(dir, testNodeIdentity(), testKey(), options)
		if err != nil {
			b.Fatal(err)
		}
		if err = store.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkNodeStoreOpenDefaultGeometry(b *testing.B) {
	dir := filepath.Join(b.TempDir(), "node")
	store, err := CreateNodeStore(dir, testNodeIdentity(), testKey(), []NodeBootstrap{{Descriptor: testGroupDescriptor(1), Snapshot: nodeSnapshot(1, 1, 1)}}, NodeStoreOptions{})
	if err != nil {
		b.Fatal(err)
	}
	if err = store.Close(); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		store, err = OpenNodeStore(dir, testNodeIdentity(), testKey(), NodeStoreOptions{})
		if err != nil {
			b.Fatal(err)
		}
		if err = store.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkNodeStorePersistDurability measures the canonical node-wide log
// with its real platform durability primitive. Setup, incarnation allocation,
// and shutdown are outside the timer. The multi-group case is one durability
// wave, not a loop of independent group writes.
func BenchmarkNodeStorePersistDurability(b *testing.B) {
	for _, groups := range []int{1, 8} {
		b.Run(fmt.Sprintf("groups=%d", groups), func(b *testing.B) {
			b.StopTimer()
			options := NodeStoreOptions{
				MaxWaveBytes:       1 << 20,
				MaxSegmentEvents:   128 << 10,
				RecentWaves:        32 << 10,
				MaxEntriesPerGroup: raftmodel.MaxMessageEntries,
				ReaderSlots:        1,
				MaxGroups:          64,
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
