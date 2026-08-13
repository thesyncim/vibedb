package raftstore

import (
	"math"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	pb "go.etcd.io/raft/v3/raftpb"
)

func TestTermHotReadAllocations(t *testing.T) {
	_, store, _ := createTestStore(t)
	allocations := testing.AllocsPerRun(1000, func() {
		term, err := store.Term(1)
		if err != nil || term != 1 {
			panic("Term failed")
		}
	})
	if allocations != 0 {
		t.Fatalf("Term allocations = %.2f", allocations)
	}
}

func BenchmarkEmptyPersistZeroWriteZeroSync(b *testing.B) {
	_, store, _ := createTestStore(b)
	incarnation, err := store.BeginIncarnation()
	if err != nil {
		b.Fatal(err)
	}
	batch := raftmodel.PersistBatch{NodeIncarnation: incarnation, ReadyID: 1}
	if err := store.Persist(batch); err != nil {
		b.Fatal(err)
	}
	before := store.SyncCount()
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		if err := store.Persist(batch); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if store.SyncCount() != before {
		b.Fatalf("empty retries synced %d times", store.SyncCount()-before)
	}
}

func BenchmarkReadyCodec4096EmptyEntries(b *testing.B) {
	entries := make([]*pb.Entry, MaxReadyEntries)
	for index := range entries {
		entries[index] = entry(uint64(index+2), 2, "")
	}
	batch := raftmodel.PersistBatch{NodeIncarnation: 1, ReadyID: 1, HardState: hard(2, uint64(len(entries)+1)), Entries: entries}
	b.ReportAllocs()
	b.SetBytes(int64(len(entries) * 32))
	for iteration := 0; iteration < b.N; iteration++ {
		encoded, err := marshalReadyPayload(batch)
		if err != nil || len(encoded) == 0 {
			b.Fatal(err)
		}
	}
}

func BenchmarkDetachedEntriesRead(b *testing.B) {
	_, store, _ := createTestStore(b)
	incarnation, err := store.BeginIncarnation()
	if err != nil {
		b.Fatal(err)
	}
	entries := make([]*pb.Entry, 128)
	for index := range entries {
		entries[index] = entry(uint64(index+2), 2, "payload")
	}
	if err := store.Persist(raftmodel.PersistBatch{NodeIncarnation: incarnation, ReadyID: 1, HardState: hard(2, 129), Entries: entries}); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		result, err := store.Entries(2, 130, math.MaxUint64)
		if err != nil || len(result) != len(entries) {
			b.Fatal(err)
		}
	}
}
