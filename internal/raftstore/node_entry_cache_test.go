package raftstore

import (
	"bytes"
	"errors"
	"math"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestNodeEntryCacheReadsDuringUnrelatedSyncAndOwnsPayload(t *testing.T) {
	store, _, _ := coordinateFixture(t)
	view := store.Group(1)
	if _, err := view.FirstIndex(); err != nil {
		t.Fatal(err)
	}
	entry := typedEntry(2, 2, pb.EntryNormal, "original")
	if err := view.Persist(raftmodel.PersistBatch{NodeIncarnation: 1, ReadyID: 1, Entries: []*pb.Entry{entry}, HardState: hard(2, 2), MustSync: true}); err != nil {
		t.Fatal(err)
	}
	entry.Data[0] = 'X'
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	store.SetDataSyncForTesting(func(file *os.File) error { close(entered); <-release; return file.Sync() })
	done := make(chan error, 1)
	go func() {
		done <- store.Group(2).Persist(raftmodel.PersistBatch{NodeIncarnation: 1, ReadyID: 1, Entries: []*pb.Entry{typedEntry(2, 2, pb.EntryNormal, "other")}, HardState: hard(2, 2), MustSync: true})
	}()
	<-entered
	read := make(chan error, 1)
	go func() {
		entries, err := view.Entries(2, 3, math.MaxUint64)
		if err == nil && (len(entries) != 1 || string(entries[0].Data) != "original") {
			err = errors.New("cached payload was not detached from persisted input")
		}
		if err == nil {
			entries[0].Data[0] = 'Y'
			*entries[0].Term = 99
		}
		again, nextErr := view.Entries(2, 3, math.MaxUint64)
		if nextErr != nil {
			err = nextErr
		} else if len(again) != 1 || string(again[0].Data) != "original" || again[0].GetTerm() != 2 {
			err = errors.New("caller mutation changed the cache")
		}
		read <- err
	}()
	select {
	case err := <-read:
		if err != nil {
			t.Error(err)
		}
	case <-time.After(time.Second):
		t.Error("committed entry retrieval blocked behind unrelated sync")
	}
	unblock()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	store.SetDataSyncForTesting(func(file *os.File) error { return file.Sync() })
}

func TestNodeEntryCacheDoesNotPublishStagedReplacementBeforeSync(t *testing.T) {
	store, _, _ := coordinateFixture(t)
	view := store.Group(1)
	if _, err := view.FirstIndex(); err != nil {
		t.Fatal(err)
	}
	if err := view.Persist(raftmodel.PersistBatch{NodeIncarnation: 1, ReadyID: 1, Entries: []*pb.Entry{typedEntry(2, 2, pb.EntryNormal, "old")}, HardState: hard(2, 1), MustSync: true}); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	injected := errors.New("uncertain replacement sync")
	store.SetDataSyncForTesting(func(*os.File) error { close(entered); <-release; return injected })
	done := make(chan error, 1)
	go func() {
		done <- view.Persist(raftmodel.PersistBatch{NodeIncarnation: 1, ReadyID: 2, Entries: []*pb.Entry{typedEntry(2, 3, pb.EntryNormal, "new")}, HardState: hard(3, 1), MustSync: true})
	}()
	<-entered
	entries, found, err := view.cachedEntries(2, 3, math.MaxUint64)
	if err != nil || !found || len(entries) != 1 || string(entries[0].Data) != "old" || entries[0].GetTerm() != 2 {
		t.Errorf("staged replacement became visible: found=%v err=%v", found, err)
	}
	unblock()
	if err := <-done; !errors.Is(err, ErrPersistenceUnknown) {
		t.Fatal(err)
	}
	if _, err := view.Entries(2, 3, math.MaxUint64); !errors.Is(err, ErrPersistenceUnknown) {
		t.Fatal(err)
	}
}

func TestNodeEntryCacheBoundsEvictionLargeEntriesAndReadLimit(t *testing.T) {
	store, _, _ := coordinateFixture(t)
	view := store.Group(1)
	if _, err := view.FirstIndex(); err != nil {
		t.Fatal(err)
	}
	entries := []*pb.Entry{typedEntry(2, 2, pb.EntryNormal, "small"), typedEntry(3, 2, pb.EntryNormal, ""), typedEntry(4, 2, pb.EntryNormal, string(bytes.Repeat([]byte{'z'}, nodeEntryCacheSlotBytes+1)))}
	if err := view.Persist(raftmodel.PersistBatch{NodeIncarnation: 1, ReadyID: 1, Entries: entries, HardState: hard(2, 4), MustSync: true}); err != nil {
		t.Fatal(err)
	}
	for _, limit := range []uint64{0, uint64(proto.Size(entries[0])), math.MaxUint64} {
		got, err := view.Entries(2, 4, limit)
		want := 2
		if limit != math.MaxUint64 {
			want = 1
		}
		if err != nil || len(got) != want || !proto.Equal(got[0], entries[0]) {
			t.Fatalf("limit=%d entries=%d err=%v", limit, len(got), err)
		}
	}
	if _, found, err := view.cachedEntries(4, 5, math.MaxUint64); err != nil || found {
		t.Fatal("oversized payload entered the cache")
	}
	if got, err := view.Entries(2, 5, math.MaxUint64); err != nil || len(got) != 3 || !proto.Equal(got[2], entries[2]) {
		t.Fatalf("large-entry fallback: %v", err)
	}
	if got, err := view.Entries(3, 3, 0); err != nil || len(got) != 0 {
		t.Fatalf("empty range: %v", err)
	}
	if _, err := view.Entries(1, 2, 0); !errors.Is(err, raft.ErrCompacted) {
		t.Fatal(err)
	}
	if _, err := view.Entries(4, 6, 0); !errors.Is(err, raft.ErrUnavailable) {
		t.Fatal(err)
	}
	// Force a full physical-arena eviction without publishing any new group
	// references. A stale per-group reference must become a storage miss.
	store.coordinateMu.Lock()
	for i := 0; i < nodeEntryCacheSlots; i++ {
		var staged nodeTermCoordinate
		store.cacheEntryPayloadLocked(&staged, []byte("evict"))
	}
	store.coordinateMu.Unlock()
	if len(store.entryCacheArena) != 8<<20 {
		t.Fatal("payload arena grew")
	}
	if _, found, err := view.cachedEntries(2, 3, math.MaxUint64); err != nil || found {
		t.Fatal("evicted slot remained addressable")
	}
	if got, err := view.Entries(2, 3, math.MaxUint64); err != nil || len(got) != 1 || !proto.Equal(got[0], entries[0]) {
		t.Fatalf("eviction fallback: %v", err)
	}
}

func TestNodeEntryCacheWireSizeMatchesProtobuf(t *testing.T) {
	for _, index := range []uint64{1, 127, 128, 16383, 16384, math.MaxUint64} {
		for _, kind := range []pb.EntryType{pb.EntryNormal, pb.EntryConfChange, pb.EntryConfChangeV2} {
			for _, length := range []int{0, 1, 127, 128, nodeEntryCacheSlotBytes} {
				term := index
				entry := &pb.Entry{Index: &index, Term: &term, Type: &kind}
				if length != 0 {
					entry.Data = make([]byte, length)
				}
				cached := nodeTermCoordinate{index: index, term: term, kind: kind, payloadBytes: uint32(length)}
				if got, want := cachedEntryWireBytes(cached), uint64(proto.Size(entry)); got != want {
					t.Fatalf("index=%d kind=%v len=%d size=%d want=%d", index, kind, length, got, want)
				}
			}
		}
	}
}

func TestNodeEntryCacheWarmPersistenceDoesNotAllocate(t *testing.T) {
	store, _, _ := coordinateFixture(t)
	view := store.Group(1)
	if _, err := view.FirstIndex(); err != nil {
		t.Fatal(err)
	}
	index, term, commit := uint64(1), uint64(2), uint64(1)
	kind := pb.EntryNormal
	entry := &pb.Entry{Index: &index, Term: &term, Type: &kind, Data: []byte("value")}
	batch := raftmodel.PersistBatch{NodeIncarnation: 1, Entries: []*pb.Entry{entry}, HardState: &pb.HardState{Term: &term, Commit: &commit}, MustSync: true}
	if allocations := testing.AllocsPerRun(10, func() {
		index++
		commit = index
		batch.ReadyID++
		if err := view.Persist(batch); err != nil {
			panic(err)
		}
	}); allocations != 0 {
		t.Fatalf("warm persistence allocations=%v", allocations)
	}
	if got, found, err := view.cachedEntries(index, index+1, math.MaxUint64); err != nil || !found || len(got) != 1 || string(got[0].Data) != "value" {
		t.Fatalf("warm cache found=%v err=%v", found, err)
	}
}

func TestNodeEntryCachePublishesSeriesAndSharedExtentPayloads(t *testing.T) {
	store, _, _ := coordinateFixture(t)
	for _, group := range []uint64{1, 2} {
		if _, err := store.Group(group).FirstIndex(); err != nil {
			t.Fatal(err)
		}
	}
	a, b := string(bytes.Repeat([]byte{'a'}, 2048)), string(bytes.Repeat([]byte{'b'}, 4096))
	if err := store.PersistWave([]NodeReady{
		{GroupID: 1, Batch: raftmodel.PersistBatch{NodeIncarnation: 1, ReadyID: 1, Entries: []*pb.Entry{typedEntry(2, 2, pb.EntryNormal, a)}, HardState: hard(2, 2), MustSync: true}},
		{GroupID: 2, Batch: raftmodel.PersistBatch{NodeIncarnation: 1, ReadyID: 1, Entries: []*pb.Entry{typedEntry(2, 2, pb.EntryNormal, b)}, HardState: hard(2, 2), MustSync: true}},
	}); err != nil {
		t.Fatal(err)
	}
	for group, want := range map[uint64]string{1: a, 2: b} {
		got, found, err := store.Group(group).cachedEntries(2, 3, math.MaxUint64)
		if err != nil || !found || len(got) != 1 || string(got[0].Data) != want {
			t.Fatalf("group=%d found=%v err=%v", group, found, err)
		}
	}
	if err := store.PersistReadySeries(1, []raftmodel.PersistBatch{
		{NodeIncarnation: 1, ReadyID: 2, Entries: []*pb.Entry{typedEntry(3, 2, pb.EntryNormal, "series-a")}, HardState: hard(2, 3), MustSync: true},
		{NodeIncarnation: 1, ReadyID: 3, Entries: []*pb.Entry{typedEntry(4, 2, pb.EntryNormal, "series-b")}, HardState: hard(2, 4), MustSync: true},
	}); err != nil {
		t.Fatal(err)
	}
	got, found, err := store.Group(1).cachedEntries(3, 5, math.MaxUint64)
	if err != nil || !found || len(got) != 2 || string(got[0].Data) != "series-a" || string(got[1].Data) != "series-b" {
		t.Fatalf("series found=%v err=%v", found, err)
	}
}
