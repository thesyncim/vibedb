package raftstore

import (
	"crypto/sha256"
	"math"
	"runtime"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	pb "go.etcd.io/raft/v3/raftpb"
)

func TestObjectCryptoWorkspaceHotOperationsAllocateZero(t *testing.T) {
	workspace := newObjectCryptoWorkspace([sha256.Size]byte{1}, [sha256.Size]byte{2})
	digest := [sha256.Size]byte{3}
	context := []byte("context")
	payload := []byte("payload")
	// Initialize any implementation-owned lazy state before the measured run.
	_ = workspace.deriveObjectKey("wal-record", 1, digest)
	_ = workspace.makeObjectTag("wal-record", 1, context, payload)
	_ = workspace.deriveObjectNonce("wal-record", 1, digest)
	allocations := testing.AllocsPerRun(1000, func() {
		_ = workspace.deriveObjectKey("wal-record", 2, digest)
		_ = workspace.makeObjectTag("wal-record", 2, context, payload)
		_ = workspace.deriveObjectNonce("wal-record", 2, digest)
	})
	if allocations != 0 {
		t.Fatalf("object crypto workspace allocations = %.2f", allocations)
	}
}

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

func TestEntriesHotReadAllocations(t *testing.T) {
	_, store, _ := createTestStore(t)
	incarnation, err := store.BeginIncarnation()
	if err != nil {
		t.Fatal(err)
	}
	entries := make([]*pb.Entry, 128)
	for index := range entries {
		entries[index] = entry(uint64(index+2), 2, "payload")
	}
	if err := store.Persist(raftmodel.PersistBatch{
		NodeIncarnation: incarnation,
		ReadyID:         1,
		HardState:       hard(2, 129),
		Entries:         entries,
		MustSync:        true,
	}); err != nil {
		t.Fatal(err)
	}
	allocations := testing.AllocsPerRun(1000, func() {
		result, err := store.Entries(2, 130, math.MaxUint64)
		if err != nil || len(result) != len(entries) || cap(result) != len(result) {
			panic("Entries failed")
		}
	})
	if allocations != 0 {
		t.Fatalf("Entries allocations = %.2f", allocations)
	}
}

func TestEmptyReadyNamespaceProofHotPathAllocations(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("raw zero-allocation namespace syscalls are Linux-specific")
	}
	_, store, _ := createTestStore(t)
	incarnation, err := store.BeginIncarnation()
	if err != nil {
		t.Fatal(err)
	}
	readyID := uint64(0)
	batch := raftmodel.PersistBatch{NodeIncarnation: incarnation}
	exercise := func() {
		readyID++
		batch.ReadyID = readyID
		if err := store.Persist(batch); err != nil {
			panic(err)
		}
	}
	// Pin the directory and build the cached syscall paths.
	exercise()
	if allocations := testing.AllocsPerRun(1000, exercise); allocations != 0 {
		t.Fatalf("empty Ready namespace proof allocations = %.2f", allocations)
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

func BenchmarkBorrowedEntriesRead(b *testing.B) {
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
