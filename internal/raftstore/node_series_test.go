package raftstore

import (
	"bytes"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftstore/seglog"
	pb "go.etcd.io/raft/v3/raftpb"
)

func TestNodeStoreReadySeriesRetryAndReopen(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "node")
	options := NodeStoreOptions{
		MaxWaveBytes: 1 << 20, MaxSegmentEvents: 128,
		RecentWaves: 32, MaxEntriesPerGroup: 32, ReaderSlots: 1, MaxGroups: 8,
	}
	store, err := CreateNodeStore(dir, testNodeIdentity(), testKey(), []NodeBootstrap{{
		Descriptor: testGroupDescriptor(10), Snapshot: nodeSnapshot(10, 1, 1),
	}}, options)
	if err != nil {
		t.Fatal(err)
	}
	incarnations, err := store.BeginIncarnations([]uint64{1})
	if err != nil {
		t.Fatal(err)
	}
	incarnation := incarnations[0].Incarnation
	var calls atomic.Int32
	var observed seglog.Wave
	store.persistWaveTest = func(wave seglog.Wave) error {
		calls.Add(1)
		observed = wave
		return store.engine.PersistWave(wave)
	}
	batches := []raftmodel.PersistBatch{
		{NodeIncarnation: incarnation, ReadyID: 1, HardState: hard(2, 2), Entries: []*pb.Entry{typedEntry(2, 2, pb.EntryNormal, "a")}},
		{NodeIncarnation: incarnation, ReadyID: 2, HardState: hard(2, 3), Entries: []*pb.Entry{typedEntry(3, 2, pb.EntryNormal, "b")}},
		{NodeIncarnation: incarnation, ReadyID: 3, HardState: hard(2, 4), Entries: []*pb.Entry{typedEntry(4, 2, pb.EntryNormal, "c")}},
	}
	if err = store.PersistReadySeries(1, batches); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 || len(observed.Batches) != 1 {
		t.Fatalf("persist calls=%d batches=%d", calls.Load(), len(observed.Batches))
	}
	physical := observed.Batches[0]
	if physical.ReadyID != 3 || physical.ReadySpan != 3 || len(physical.Entries) != 3 || physical.ReadyDigest == ([16]byte{}) {
		t.Fatalf("physical series=%+v", physical)
	}
	if err = store.PersistReadySeries(1, batches); err != nil {
		t.Fatalf("exact series retry: %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("exact retry reached engine %d times", calls.Load())
	}
	changed := append([]raftmodel.PersistBatch(nil), batches...)
	changed[1].Entries = []*pb.Entry{typedEntry(3, 2, pb.EntryNormal, "changed")}
	if err = store.PersistReadySeries(1, changed); !errors.Is(err, ErrRetryConflict) {
		t.Fatalf("changed series retry=%v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("conflicting retry reached engine %d times", calls.Load())
	}
	if summary, ok := store.engine.Summary(1); !ok || summary.ReadyID != 3 {
		t.Fatalf("summary after series=%+v ok=%v", summary, ok)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenNodeStore(dir, testNodeIdentity(), testKey(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if summary, ok := reopened.engine.Summary(1); !ok || summary.ReadyID != 3 {
		t.Fatalf("reopened summary=%+v ok=%v", summary, ok)
	}
	entries, err := reopened.Group(1).Entries(2, 5, ^uint64(0))
	if err != nil || len(entries) != 3 || !bytes.Equal(entries[2].GetData(), []byte("c")) {
		t.Fatalf("reopened entries=%d err=%v", len(entries), err)
	}
}

func TestNodeStoreReadySeriesRejectsBeforeMutation(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "node")
	options := NodeStoreOptions{MaxWaveBytes: 1 << 20, MaxSegmentEvents: 128, RecentWaves: 32, MaxEntriesPerGroup: 32, ReaderSlots: 1, MaxGroups: 8}
	store, err := CreateNodeStore(dir, testNodeIdentity(), testKey(), []NodeBootstrap{{
		Descriptor: testGroupDescriptor(10), Snapshot: nodeSnapshot(10, 1, 1),
	}}, options)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	incarnations, err := store.BeginIncarnations([]uint64{1})
	if err != nil {
		t.Fatal(err)
	}
	incarnation := incarnations[0].Incarnation
	var calls atomic.Int32
	store.persistWaveTest = func(seglog.Wave) error {
		calls.Add(1)
		return nil
	}
	base := raftmodel.PersistBatch{NodeIncarnation: incarnation, ReadyID: 1, Entries: []*pb.Entry{typedEntry(2, 2, pb.EntryNormal, "x")}}
	bad := []raftmodel.PersistBatch{base, {NodeIncarnation: incarnation + 1, ReadyID: 2}}
	if err = store.PersistReadySeries(1, bad); !errors.Is(err, ErrInvalid) {
		t.Fatalf("incarnation mismatch=%v", err)
	}
	withSnapshot := []raftmodel.PersistBatch{base, {NodeIncarnation: incarnation, ReadyID: 2, Snapshot: nodeSnapshot(10, 1, 1)}}
	if err = store.PersistReadySeries(1, withSnapshot); !errors.Is(err, ErrUnsupportedSnapshot) {
		t.Fatalf("multi snapshot=%v", err)
	}
	gap := []raftmodel.PersistBatch{base, {NodeIncarnation: incarnation, ReadyID: 3}}
	if err = store.PersistReadySeries(1, gap); !errors.Is(err, ErrInvalid) {
		t.Fatalf("ready gap=%v", err)
	}
	committedThenReplaced := []raftmodel.PersistBatch{
		{NodeIncarnation: incarnation, ReadyID: 1, HardState: hard(2, 3), Entries: []*pb.Entry{
			typedEntry(2, 2, pb.EntryNormal, "a"), typedEntry(3, 2, pb.EntryNormal, "committed"),
		}},
		{NodeIncarnation: incarnation, ReadyID: 2, HardState: hard(2, 3), Entries: []*pb.Entry{
			typedEntry(3, 2, pb.EntryNormal, "replacement"),
		}},
	}
	if err = store.PersistReadySeries(1, committedThenReplaced); !errors.Is(err, ErrInvalid) {
		t.Fatalf("replacement of intermediate committed entry=%v", err)
	}
	commitBeforeEntry := []raftmodel.PersistBatch{
		{NodeIncarnation: incarnation, ReadyID: 1, HardState: hard(1, 2), Entries: []*pb.Entry{
			typedEntry(2, 1, pb.EntryNormal, "a"),
		}},
		{NodeIncarnation: incarnation, ReadyID: 2, HardState: hard(2, 3)},
		{NodeIncarnation: incarnation, ReadyID: 3, HardState: hard(2, 3), Entries: []*pb.Entry{
			typedEntry(3, 2, pb.EntryNormal, "b"),
		}},
	}
	if err = store.PersistReadySeries(1, commitBeforeEntry); !errors.Is(err, ErrInvalid) {
		t.Fatalf("intermediate commit beyond durable last=%v", err)
	}
	tooMany := make([]raftmodel.PersistBatch, MaxReadySeries+1)
	for i := range tooMany {
		tooMany[i] = raftmodel.PersistBatch{NodeIncarnation: incarnation, ReadyID: uint64(i + 1)}
	}
	if err = store.PersistReadySeries(1, tooMany); !errors.Is(err, ErrBounds) {
		t.Fatalf("series bound=%v", err)
	}
	if calls.Load() != 0 {
		t.Fatalf("rejected series reached persistence %d times", calls.Load())
	}
	if summary, ok := store.engine.Summary(1); !ok || summary.ReadyID != 0 {
		t.Fatalf("rejected series changed summary=%+v ok=%v", summary, ok)
	}
}

func TestNodeStoreReadySeriesAggregatesOverlappingSuffixSafely(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "node")
	options := NodeStoreOptions{MaxWaveBytes: 1 << 20, MaxSegmentEvents: 128, RecentWaves: 32, MaxEntriesPerGroup: 32, ReaderSlots: 1, MaxGroups: 8}
	store, err := CreateNodeStore(dir, testNodeIdentity(), testKey(), []NodeBootstrap{{
		Descriptor: testGroupDescriptor(10), Snapshot: nodeSnapshot(10, 1, 1),
	}}, options)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	incarnations, err := store.BeginIncarnations([]uint64{1})
	if err != nil {
		t.Fatal(err)
	}
	incarnation := incarnations[0].Incarnation
	batches := []raftmodel.PersistBatch{
		{NodeIncarnation: incarnation, ReadyID: 1, HardState: hard(2, 2), Entries: []*pb.Entry{
			typedEntry(2, 2, pb.EntryNormal, "a"), typedEntry(3, 2, pb.EntryNormal, "old"),
		}},
		{NodeIncarnation: incarnation, ReadyID: 2, HardState: hard(2, 4), Entries: []*pb.Entry{
			typedEntry(3, 2, pb.EntryNormal, "new"), typedEntry(4, 2, pb.EntryNormal, "c"),
		}},
	}
	if err = store.PersistReadySeries(1, batches); err != nil {
		t.Fatal(err)
	}
	entries, err := store.Group(1).Entries(2, 5, ^uint64(0))
	if err != nil || len(entries) != 3 || !bytes.Equal(entries[1].GetData(), []byte("new")) {
		t.Fatalf("aggregated replacement entries=%d err=%v", len(entries), err)
	}
}

func TestNodeStoreMultiGroupSeriesRetainsDistinctEntries(t *testing.T) {
	store, dir, options := coordinateFixture(t)
	series := func(group uint64, first, second string) NodeReady {
		return NodeReady{GroupID: group, seriesCount: 2, series: [MaxReadySeries]raftmodel.PersistBatch{
			{NodeIncarnation: 1, ReadyID: 1, HardState: hard(2, 2), Entries: []*pb.Entry{typedEntry(2, 2, pb.EntryNormal, first)}, MustSync: true},
			{NodeIncarnation: 1, ReadyID: 2, HardState: hard(2, 3), Entries: []*pb.Entry{typedEntry(3, 2, pb.EntryNormal, second)}, MustSync: true},
		}}
	}
	if err := store.PersistWave([]NodeReady{
		series(1, "group-one-first", "group-one-second"),
		series(2, "group-two-first", "group-two-second"),
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenNodeStore(dir, testNodeIdentity(), testKey(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	for group, want := range map[uint64][]string{
		1: {"group-one-first", "group-one-second"},
		2: {"group-two-first", "group-two-second"},
	} {
		entries, err := reopened.Group(group).Entries(2, 4, ^uint64(0))
		if err != nil || len(entries) != len(want) {
			t.Fatalf("group=%d entries=%d err=%v", group, len(entries), err)
		}
		for index := range want {
			if got := string(entries[index].GetData()); got != want[index] {
				t.Fatalf("group=%d entry=%d data=%q want=%q", group, index, got, want[index])
			}
		}
	}
}

func assertNodeSeriesScratchCleared(t *testing.T, store *NodeStore) {
	t.Helper()
	for index, entry := range store.seriesEntryPtrs {
		if entry != nil {
			t.Fatalf("scratch pointer %d retained %p", index, entry)
		}
	}
	for index, entry := range store.seriesEntryArena {
		if entry != nil {
			t.Fatalf("arena pointer %d retained %p", index, entry)
		}
	}
}

func TestNodeStoreSeriesScratchClearsAfterFailedPreflight(t *testing.T) {
	store, _, _ := coordinateFixture(t)
	valid := NodeReady{GroupID: 1, seriesCount: 2, series: [MaxReadySeries]raftmodel.PersistBatch{
		{NodeIncarnation: 1, ReadyID: 1, Entries: []*pb.Entry{typedEntry(2, 2, pb.EntryNormal, "retained")}, HardState: hard(2, 2), MustSync: true},
		{NodeIncarnation: 1, ReadyID: 2, Entries: []*pb.Entry{typedEntry(3, 2, pb.EntryNormal, "retained")}, HardState: hard(2, 3), MustSync: true},
	}}
	invalid := NodeReady{GroupID: 2, seriesCount: 2, series: [MaxReadySeries]raftmodel.PersistBatch{
		{NodeIncarnation: 2, ReadyID: 1}, {NodeIncarnation: 2, ReadyID: 2},
	}}
	if err := store.PersistWave([]NodeReady{valid, invalid}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid preflight=%v", err)
	}
	assertNodeSeriesScratchCleared(t, store)
}

func TestNodeStoreSeriesScratchClearsAfterMultiGroupSuccess(t *testing.T) {
	store, _, _ := coordinateFixture(t)
	long := NodeReady{GroupID: 1, seriesCount: 2, series: [MaxReadySeries]raftmodel.PersistBatch{
		{NodeIncarnation: 1, ReadyID: 1, Entries: []*pb.Entry{
			typedEntry(2, 2, pb.EntryNormal, "long-2"),
			typedEntry(3, 2, pb.EntryNormal, "long-3"),
			typedEntry(4, 2, pb.EntryNormal, "long-4"),
		}, HardState: hard(2, 2), MustSync: true},
		{NodeIncarnation: 1, ReadyID: 2, HardState: hard(2, 2), MustSync: true},
	}}
	short := NodeReady{GroupID: 2, seriesCount: 2, series: [MaxReadySeries]raftmodel.PersistBatch{
		{NodeIncarnation: 1, ReadyID: 1, Entries: []*pb.Entry{typedEntry(2, 2, pb.EntryNormal, "short-2")}, HardState: hard(2, 2), MustSync: true},
		{NodeIncarnation: 1, ReadyID: 2, HardState: hard(2, 2), MustSync: true},
	}}
	if err := store.PersistWave([]NodeReady{long, short}); err != nil {
		t.Fatal(err)
	}
	assertNodeSeriesScratchCleared(t, store)
}

func TestNodeStoreSeriesScratchClearsAfterSuffixReplacement(t *testing.T) {
	store, _, _ := coordinateFixture(t)
	ready := NodeReady{GroupID: 1, seriesCount: 2, series: [MaxReadySeries]raftmodel.PersistBatch{
		{NodeIncarnation: 1, ReadyID: 1, Entries: []*pb.Entry{
			typedEntry(2, 2, pb.EntryNormal, "long-2"),
			typedEntry(3, 2, pb.EntryNormal, "long-3"),
			typedEntry(4, 2, pb.EntryNormal, "long-4"),
		}, HardState: hard(2, 2), MustSync: true},
		{NodeIncarnation: 1, ReadyID: 2, Entries: []*pb.Entry{typedEntry(3, 2, pb.EntryNormal, "replacement-3")}, HardState: hard(2, 3), MustSync: true},
	}}
	if err := store.PersistWave([]NodeReady{ready}); err != nil {
		t.Fatal(err)
	}
	entries, err := store.Group(1).Entries(2, 4, ^uint64(0))
	if err != nil || len(entries) != 2 || string(entries[0].GetData()) != "long-2" || string(entries[1].GetData()) != "replacement-3" {
		t.Fatalf("replacement entries=%d err=%v", len(entries), err)
	}
	assertNodeSeriesScratchCleared(t, store)
}

func TestNodeStoreSeriesScratchClearsAfterPersistenceFailure(t *testing.T) {
	store, _, _ := coordinateFixture(t)
	injected := errors.New("injected persistence failure")
	store.persistWaveTest = func(seglog.Wave) error {
		return injected
	}
	ready := NodeReady{GroupID: 1, seriesCount: 2, series: [MaxReadySeries]raftmodel.PersistBatch{
		{NodeIncarnation: 1, ReadyID: 1, Entries: []*pb.Entry{
			typedEntry(2, 2, pb.EntryNormal, "long-2"),
			typedEntry(3, 2, pb.EntryNormal, "long-3"),
			typedEntry(4, 2, pb.EntryNormal, "long-4"),
		}, HardState: hard(2, 2), MustSync: true},
		{NodeIncarnation: 1, ReadyID: 2, Entries: []*pb.Entry{typedEntry(3, 2, pb.EntryNormal, "replacement-3")}, HardState: hard(2, 3), MustSync: true},
	}}
	if err := store.PersistWave([]NodeReady{ready}); !errors.Is(err, ErrInvalid) || !errors.Is(err, injected) {
		t.Fatalf("persistence failure=%v", err)
	}
	assertNodeSeriesScratchCleared(t, store)
}

func TestNodeSubmissionSequencerSeriesHistogramAndWaveFusion(t *testing.T) {
	var observed []NodeReady
	q := newTestSequencer(t, 16, func(ready []NodeReady) error {
		observed = append(observed[:0], ready...)
		return nil
	})
	first := new(Submission)
	second := new(Submission)
	if err := first.Initialize(); err != nil {
		t.Fatal(err)
	}
	if err := second.Initialize(); err != nil {
		t.Fatal(err)
	}
	series := []raftmodel.PersistBatch{
		{NodeIncarnation: 1, ReadyID: 1}, {NodeIncarnation: 1, ReadyID: 2},
	}
	if err := first.PrepareReadySeries(1, series); err != nil {
		t.Fatal(err)
	}
	if err := second.PrepareReadySeries(2, []raftmodel.PersistBatch{{NodeIncarnation: 1, ReadyID: 1}}); err != nil {
		t.Fatal(err)
	}
	firstTicket, err := q.TrySubmit(first)
	if err != nil {
		t.Fatal(err)
	}
	secondTicket, err := q.TrySubmit(second)
	if err != nil {
		t.Fatal(err)
	}
	if ticket, err := first.Wait(); err != nil || ticket != firstTicket {
		t.Fatalf("first completion=%d err=%v", ticket, err)
	}
	if ticket, err := second.Wait(); err != nil || ticket != secondTicket {
		t.Fatalf("second completion=%d err=%v", ticket, err)
	}
	if len(observed) != 2 || observed[0].GroupID != 1 || observed[1].GroupID != 2 || nodeReadySeriesCount(observed[0]) != 2 || nodeReadySeriesCount(observed[1]) != 1 {
		t.Fatalf("observed fused wave=%+v", observed)
	}
	stats := q.Stats()
	if stats.ReadyLogicalBatches != 3 || stats.ReadySeriesSubmissions != 2 ||
		stats.ReadyMultiSeriesSubmissions != 1 || stats.ReadySingletonSeriesSubmissions != 1 ||
		stats.ReadySeriesHistogram[1] != 1 || stats.ReadySeriesHistogram[2] != 1 {
		t.Fatalf("series stats=%+v", stats)
	}
}

func TestNodeSubmissionSequencerDurableLogicalSeriesCounters(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "node")
	options := NodeStoreOptions{MaxWaveBytes: 1 << 20, MaxSegmentEvents: 128, RecentWaves: 32, MaxEntriesPerGroup: 32, ReaderSlots: 1, MaxGroups: 8}
	store, err := CreateNodeStore(dir, testNodeIdentity(), testKey(), []NodeBootstrap{{
		Descriptor: testGroupDescriptor(10), Snapshot: nodeSnapshot(10, 1, 1),
	}}, options)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	incarnations, err := store.BeginIncarnations([]uint64{1})
	if err != nil {
		t.Fatal(err)
	}
	sequencer, err := NewNodeSubmissionSequencer(store, 8)
	if err != nil {
		t.Fatal(err)
	}
	defer sequencer.Close()
	var cell Submission
	if err = cell.Initialize(); err != nil {
		t.Fatal(err)
	}
	incarnation := incarnations[0].Incarnation
	batches := []raftmodel.PersistBatch{
		{NodeIncarnation: incarnation, ReadyID: 1, HardState: hard(2, 2), Entries: []*pb.Entry{typedEntry(2, 2, pb.EntryNormal, "a")}},
		{NodeIncarnation: incarnation, ReadyID: 2, HardState: hard(2, 3), Entries: []*pb.Entry{typedEntry(3, 2, pb.EntryNormal, "b")}},
		{NodeIncarnation: incarnation, ReadyID: 3, HardState: hard(2, 4), Entries: []*pb.Entry{typedEntry(4, 2, pb.EntryNormal, "c")}},
	}
	if err = cell.PrepareReadySeries(1, batches); err != nil {
		t.Fatal(err)
	}
	ticket, err := sequencer.TrySubmit(&cell)
	if err != nil {
		t.Fatal(err)
	}
	if completed, waitErr := cell.Wait(); waitErr != nil || completed != ticket {
		t.Fatalf("completion=%d err=%v", completed, waitErr)
	}
	stats := sequencer.Stats()
	if stats.ReadyDurableLogicalBatches != 3 || stats.ReadyDurableSeriesSubmissions != 1 || stats.ReadyDurableSeriesHistogram[3] != 1 {
		t.Fatalf("durable series stats=%+v", stats)
	}
}

func TestNodeSubmissionSequencerDoesNotAttributeMixedRetryAsNewDurability(t *testing.T) {
	var q NodeSubmissionSequencer
	first := &Submission{kind: submissionReady, seriesCount: 3}
	second := &Submission{kind: submissionReady, seriesCount: 2}
	items := [MaxPersistGroupBatches]*Submission{first, second}
	q.observeWaveResult(&items, 2, nil, 1, 1)
	stats := q.Stats()
	if stats.ReadyDurableLogicalBatches != 0 || stats.ReadyDurableSeriesSubmissions != 0 {
		t.Fatalf("mixed duplicate/new wave attributed all series as newly durable: %+v", stats)
	}
	if stats.ReadyDurableWaves != 1 || stats.ReadyWaveGroupHistogram[1] != 1 {
		t.Fatalf("physical append witness lost: %+v", stats)
	}
}
