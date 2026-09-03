package raftstore

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftstore/seglog"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func nodeSnapshot(group, index, term uint64) *pb.Snapshot {
	return &pb.Snapshot{
		Data: []byte{byte(group), byte(index), byte(term)},
		Metadata: &pb.SnapshotMetadata{
			Index: &index, Term: &term,
			ConfState: &pb.ConfState{Voters: []uint64{1}},
		},
	}
}

func testNodeIdentity() NodeIdentity {
	legacy := testIdentity()
	var nodeID [16]byte
	copy(nodeID[:], []byte("physical-node-01"))
	return NodeIdentity{ClusterID: legacy.ClusterID, ClusterIncarnation: legacy.ClusterIncarnation, NodeID: nodeID}
}

func testGroupDescriptor(key uint64) GroupDescriptor {
	legacy := testIdentity()
	binary.BigEndian.PutUint64(legacy.GroupID[8:], key)
	binary.BigEndian.PutUint64(legacy.StoreID[8:], key)
	return GroupDescriptor{TopologyRecoveryEpoch: 1, AllocationGeneration: legacy.AllocationGeneration, MemberID: legacy.MemberID, GroupID: legacy.GroupID, ShardIncarnation: legacy.ShardIncarnation, StoreID: legacy.StoreID, Distribution: legacy.Distribution, Shard: legacy.Shard}
}

func TestNodeStoreUsesOnlyCanonicalNodeMetadataName(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "node")
	options := NodeStoreOptions{MaxWaveBytes: 1 << 20, MaxSegmentEvents: 64, RecentWaves: 16, MaxEntriesPerGroup: 16, ReaderSlots: 1}
	store, err := CreateNodeStore(dir, testNodeIdentity(), testKey(), []NodeBootstrap{{Descriptor: testGroupDescriptor(10), Snapshot: nodeSnapshot(10, 1, 1)}}, options)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	canonical := filepath.Join(dir, "NODEMETA")
	if _, err = os.Stat(canonical); err != nil {
		t.Fatalf("canonical node metadata: %v", err)
	}
	obsolete := filepath.Join(dir, nodeMetaName+".v1")
	if _, err = os.Stat(obsolete); !os.IsNotExist(err) {
		t.Fatalf("obsolete node metadata exists: %v", err)
	}
	if err = os.Rename(canonical, obsolete); err != nil {
		t.Fatal(err)
	}
	if reopened, openErr := OpenNodeStore(dir, testNodeIdentity(), testKey(), options); openErr == nil {
		_ = reopened.Close()
		t.Fatal("Open accepted obsolete node metadata filename")
	}
}

func TestNodeStoreAuthenticatesCompleteCapacityGeometry(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "node")
	options := NodeStoreOptions{SegmentBytes: DefaultNodeSegmentBytes, MaxWaveBytes: 1 << 20, MaxSegmentEvents: 64, RecentWaves: 16, MaxEntriesPerGroup: 16, ReaderSlots: 1, MaxGroups: 8}
	store, err := CreateNodeStore(dir, testNodeIdentity(), testKey(), []NodeBootstrap{{Descriptor: testGroupDescriptor(10), Snapshot: nodeSnapshot(10, 1, 1)}}, options)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		mutate func(*NodeStoreOptions)
	}{
		{"segment-bytes", func(o *NodeStoreOptions) { o.SegmentBytes++ }},
		{"max-wave-bytes", func(o *NodeStoreOptions) { o.MaxWaveBytes++ }},
		{"max-segment-events", func(o *NodeStoreOptions) { o.MaxSegmentEvents++ }},
		{"recent-waves", func(o *NodeStoreOptions) { o.RecentWaves++ }},
		{"max-entries-per-group", func(o *NodeStoreOptions) { o.MaxEntriesPerGroup++ }},
		{"reader-slots", func(o *NodeStoreOptions) { o.ReaderSlots++ }},
		{"max-groups", func(o *NodeStoreOptions) { o.MaxGroups++ }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			changed := options
			test.mutate(&changed)
			reopened, openErr := OpenNodeStore(dir, testNodeIdentity(), testKey(), changed)
			if reopened != nil {
				_ = reopened.Close()
			}
			if !errors.Is(openErr, ErrIdentityMismatch) {
				t.Fatalf("OpenNodeStore mismatch = %v", openErr)
			}
		})
	}
	reopened, err := OpenNodeStore(dir, testNodeIdentity(), testKey(), options)
	if err != nil {
		t.Fatalf("exact reopen: %v", err)
	}
	_ = reopened.Close()
}

func TestDefaultNodeGeometryIsUsableAndSpaceBounded(t *testing.T) {
	options, err := defaultNodeOptions(NodeStoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if options.SegmentBytes != DefaultNodeSegmentBytes || options.MaxWaveBytes != DefaultNodeMaxWaveBytes ||
		options.MaxSegmentEvents != DefaultNodeMaxSegmentEvents || options.RecentWaves != DefaultNodeRecentWaves ||
		options.MaxEntriesPerGroup != DefaultNodeMaxEntriesPerGroup || options.ReaderSlots != DefaultNodeReaderSlots ||
		options.MaxGroups != DefaultNodeMaxGroups {
		t.Fatalf("default node geometry=%+v", options)
	}
	if options.SegmentBytes*3 != 96<<20 {
		t.Fatalf("active plus two reserves=%d, want %d", options.SegmentBytes*3, 96<<20)
	}
	dir := filepath.Join(t.TempDir(), "node")
	store, err := CreateNodeStore(dir, testNodeIdentity(), testKey(), []NodeBootstrap{{Descriptor: testGroupDescriptor(10), Snapshot: nodeSnapshot(10, 1, 1)}}, NodeStoreOptions{})
	if err != nil {
		t.Fatalf("zero-value create: %v", err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = OpenNodeStore(dir, testNodeIdentity(), testKey(), NodeStoreOptions{})
	if err != nil {
		t.Fatalf("zero-value reopen: %v", err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestNodeSequencerAutomaticallyRotatesAndReopens(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "node")
	options := NodeStoreOptions{
		SegmentBytes: 1 << 20, MaxWaveBytes: 64 << 10, MaxSegmentEvents: 128,
		RecentWaves: 64, MaxEntriesPerGroup: 16, ReaderSlots: 2, MaxGroups: 8,
	}
	store, err := CreateNodeStore(dir, testNodeIdentity(), testKey(), []NodeBootstrap{{Descriptor: testGroupDescriptor(10), Snapshot: nodeSnapshot(10, 1, 1)}}, options)
	if err != nil {
		t.Fatal(err)
	}
	incarnations, err := store.BeginIncarnations([]uint64{1})
	if err != nil {
		t.Fatal(err)
	}
	sequencer, err := NewNodeSubmissionSequencer(store, 8)
	if err != nil {
		t.Fatal(err)
	}
	var submission Submission
	if err = submission.Initialize(); err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte{'r'}, 48<<10)
	for readyID := uint64(1); readyID <= 40; readyID++ {
		index := readyID + 1
		entry := typedEntry(index, 2, pb.EntryNormal, string(payload))
		batch := raftmodel.PersistBatch{NodeIncarnation: incarnations[0].Incarnation, ReadyID: readyID, Entries: []*pb.Entry{entry}, HardState: hard(2, index)}
		if err = submission.Prepare(NodeReady{GroupID: 1, Batch: batch}); err != nil {
			t.Fatal(err)
		}
		if _, err = sequencer.TrySubmit(&submission); err != nil {
			t.Fatal(err)
		}
		if _, err = submission.Wait(); err != nil {
			t.Fatalf("Ready %d: %v", readyID, err)
		}
	}
	logFiles, err := os.ReadDir(filepath.Join(dir, nodeLogDir))
	if err != nil {
		t.Fatal(err)
	}
	segmentFiles := 0
	for _, file := range logFiles {
		if len(file.Name()) >= len("segment-") && file.Name()[:len("segment-")] == "segment-" {
			segmentFiles++
		}
	}
	if segmentFiles < 5 {
		t.Fatalf("segment files=%d, want active, two reserves, and at least two sealed", segmentFiles)
	}
	if err = sequencer.Close(); err != nil {
		t.Fatal(err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenNodeStore(dir, testNodeIdentity(), testKey(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	entries, err := reopened.Group(1).Entries(2, 42, ^uint64(0))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 40 {
		t.Fatalf("reopened entries=%d, want 40", len(entries))
	}
	if entries[0].GetIndex() != 2 || entries[39].GetIndex() != 41 || !bytes.Equal(entries[39].GetData(), payload) {
		t.Fatalf("reopened first/last=%d/%d", entries[0].GetIndex(), entries[39].GetIndex())
	}
}

func TestNodeStoreDescriptorEnrollmentIsAtomicMonotonicAndReopens(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "node")
	options := NodeStoreOptions{MaxWaveBytes: 1 << 20, MaxSegmentEvents: 128, RecentWaves: 32, MaxEntriesPerGroup: 32, ReaderSlots: 1, MaxGroups: 8}
	initial := testGroupDescriptor(20)
	store, err := CreateNodeStore(dir, testNodeIdentity(), testKey(), []NodeBootstrap{{Descriptor: initial, Snapshot: nodeSnapshot(1, 1, 1)}}, options)
	if err != nil {
		t.Fatal(err)
	}
	first, ok := store.GroupByID(initial.GroupID)
	if !ok {
		t.Fatal("initial portable group missing")
	}
	firstDescriptor, err := first.Descriptor()
	if err != nil || firstDescriptor.LogKey != 1 {
		t.Fatalf("initial descriptor=%+v err=%v", firstDescriptor, err)
	}
	inserted := testGroupDescriptor(10)
	syncs := 0
	store.engine.SetDataSyncForTesting(func(file *os.File) error { syncs++; return file.Sync() })
	incarnation, err := store.RegisterGroup(inserted)
	if err != nil || incarnation != (GroupIncarnation{GroupID: 2, Incarnation: 1}) {
		t.Fatalf("register=%+v err=%v", incarnation, err)
	}
	if syncs != 1 {
		t.Fatalf("descriptor+incarnation durability syncs=%d want 1", syncs)
	}
	if again, retryErr := store.RegisterGroup(inserted); retryErr != nil || again != incarnation {
		t.Fatalf("exact retry=%+v err=%v", again, retryErr)
	}
	if firstAfter, found := store.GroupByID(initial.GroupID); !found || firstAfter.group != 1 {
		t.Fatalf("existing key renumbered: found=%v view=%+v", found, firstAfter)
	}
	if second, found := store.GroupByID(inserted.GroupID); !found || second.group != 2 {
		t.Fatalf("inserted lookup: found=%v view=%+v", found, second)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = OpenNodeStore(dir, testNodeIdentity(), testKey(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for _, tc := range []struct {
		id  [16]byte
		key uint64
	}{{initial.GroupID, 1}, {inserted.GroupID, 2}} {
		view, found := store.GroupByID(tc.id)
		if !found || view == nil || view.group != tc.key {
			var got uint64
			if view != nil {
				got = view.group
			}
			t.Fatalf("reopen id=%x found=%v key=%d", tc.id, found, got)
		}
		descriptor, descriptorErr := view.Descriptor()
		if descriptorErr != nil || descriptor.StoreID == ([16]byte{}) || descriptor.LogKey != tc.key {
			t.Fatalf("reopen descriptor=%+v err=%v", descriptor, descriptorErr)
		}
	}
}

func TestNodeStoreRejectsCheckpointCollisionAndNamespaceReplacement(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "node")
	options := NodeStoreOptions{MaxWaveBytes: 1 << 20, MaxSegmentEvents: 64, RecentWaves: 16, MaxEntriesPerGroup: 16, ReaderSlots: 1}
	snapshot := nodeSnapshot(10, 1, 1)
	store, err := CreateNodeStore(dir, testNodeIdentity(), testKey(), []NodeBootstrap{{Descriptor: testGroupDescriptor(10), Snapshot: snapshot}}, options)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.BeginIncarnations([]uint64{1}); err != nil {
		t.Fatal(err)
	}
	state, _ := store.engine.Group(1)
	checkpointPath := filepath.Join(dir, nodeCheckpointDir, fmt.Sprintf("%x.chk", state.Checkpoint.ID))
	original, err := os.ReadFile(checkpointPath)
	if err != nil {
		t.Fatal(err)
	}
	corrupt := append([]byte(nil), original...)
	corrupt[len(corrupt)-1] ^= 1
	if err = os.WriteFile(checkpointPath, corrupt, 0o600); err != nil {
		t.Fatal(err)
	}
	batch := raftmodel.PersistBatch{NodeIncarnation: 1, ReadyID: 1, Snapshot: snapshot, HardState: hard(1, 1)}
	if err = store.Group(1).Persist(batch); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("checkpoint collision = %v", err)
	}
	after, _ := os.ReadFile(checkpointPath)
	if !bytes.Equal(after, corrupt) {
		t.Fatal("conflicting checkpoint was overwritten")
	}
	if err = os.WriteFile(checkpointPath, original, 0o600); err != nil {
		t.Fatal(err)
	}
	moved := dir + ".old"
	if err = os.Rename(dir, moved); err != nil {
		t.Fatal(err)
	}
	if err = os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(dir, nodeMetaName), []byte("replacement"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err = store.Group(1).Persist(raftmodel.PersistBatch{NodeIncarnation: 1, ReadyID: 1}); !errors.Is(err, ErrNamespaceChanged) {
		t.Fatalf("namespace replacement = %v", err)
	}
	if _, err = store.Group(1).Term(1); !errors.Is(err, ErrPersistenceUnknown) {
		t.Fatalf("poisoned read = %v", err)
	}
	_ = store.Close()
}

func typedEntry(index, term uint64, kind pb.EntryType, data string) *pb.Entry {
	return &pb.Entry{Index: &index, Term: &term, Type: &kind, Data: []byte(data)}
}

func TestNodeStoreMultiGroupRetryAndReopen(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "node")
	options := NodeStoreOptions{MaxWaveBytes: 1 << 20, MaxSegmentEvents: 256, RecentWaves: 64, MaxEntriesPerGroup: 64, ReaderSlots: 2}
	store, err := CreateNodeStore(dir, testNodeIdentity(), testKey(), []NodeBootstrap{
		{Descriptor: testGroupDescriptor(10), Snapshot: nodeSnapshot(10, 1, 1)},
		{Descriptor: testGroupDescriptor(20), Snapshot: nodeSnapshot(20, 1, 1)},
	}, options)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.BeginIncarnations([]uint64{1, 2}); err != nil {
		t.Fatal(err)
	}
	persistCalls := 0
	store.persistWaveTest = func(wave seglog.Wave) error {
		persistCalls++
		return store.engine.PersistWave(wave)
	}
	conf := typedEntry(2, 2, pb.EntryConfChange, "conf")
	confV2 := typedEntry(2, 2, pb.EntryConfChangeV2, "conf-v2")
	wave := []NodeReady{
		{GroupID: 1, Batch: raftmodel.PersistBatch{NodeIncarnation: 1, ReadyID: 1, Entries: []*pb.Entry{conf}, HardState: hard(2, 2)}},
		{GroupID: 2, Batch: raftmodel.PersistBatch{NodeIncarnation: 1, ReadyID: 1, Entries: []*pb.Entry{confV2}, HardState: hard(2, 2)}},
	}
	if err = store.PersistWave(wave); err != nil {
		t.Fatal(err)
	}
	if persistCalls != 1 {
		t.Fatalf("multi-group Engine calls = %d, want 1", persistCalls)
	}
	second := NodeReady{GroupID: 2, Batch: raftmodel.PersistBatch{NodeIncarnation: 1, ReadyID: 2, Entries: []*pb.Entry{typedEntry(3, 2, pb.EntryNormal, "next")}, HardState: hard(2, 3)}}
	if err = store.PersistWave([]NodeReady{wave[0], second}); err != nil {
		t.Fatalf("mixed duplicate/new: %v", err)
	}
	if persistCalls != 2 {
		t.Fatalf("mixed retry Engine calls = %d, want 2", persistCalls)
	}
	if err = store.PersistWave([]NodeReady{second}); err != nil {
		t.Fatalf("all duplicate: %v", err)
	}
	if persistCalls != 2 {
		t.Fatalf("all-duplicate Engine calls = %d, want 2", persistCalls)
	}
	changed := wave[0]
	changed.Batch.Entries = []*pb.Entry{typedEntry(2, 2, pb.EntryConfChange, "changed")}
	if err = store.PersistWave([]NodeReady{changed}); !errors.Is(err, ErrRetryConflict) {
		t.Fatalf("changed duplicate = %v", err)
	}
	if err = store.engine.Rotate(nil); err != nil {
		t.Fatal(err)
	}
	third := NodeReady{GroupID: 2, Batch: raftmodel.PersistBatch{NodeIncarnation: 1, ReadyID: 3, Entries: []*pb.Entry{typedEntry(4, 3, pb.EntryNormal, "after-rotate")}, HardState: hard(3, 4)}}
	if err = store.PersistWave([]NodeReady{third}); err != nil {
		t.Fatalf("persist after rotate: %v", err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenNodeStore(dir, testNodeIdentity(), testKey(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	entries, err := reopened.Group(1).Entries(2, 3, ^uint64(0))
	if err != nil || len(entries) != 1 || entries[0].GetType() != pb.EntryConfChange || !bytes.Equal(entries[0].GetData(), []byte("conf")) {
		t.Fatalf("group 10 entries = %#v, %v", entries, err)
	}
	entries, err = reopened.Group(2).Entries(2, 5, ^uint64(0))
	if err != nil || len(entries) != 3 || entries[0].GetType() != pb.EntryConfChangeV2 || !bytes.Equal(entries[1].GetData(), []byte("next")) || !bytes.Equal(entries[2].GetData(), []byte("after-rotate")) {
		t.Fatalf("group 20 entries = %#v, %v", entries, err)
	}
	if hardState, confState, err := reopened.Group(2).InitialState(); err != nil || hardState.GetCommit() != 4 || len(confState.GetVoters()) != 1 {
		t.Fatalf("initial state = %#v %#v, %v", hardState, confState, err)
	}
}

func TestNodeStoreSharedBoundedExtentsRotateReopen(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "node")
	options := NodeStoreOptions{MaxWaveBytes: 1 << 20, MaxSegmentEvents: 64, RecentWaves: 16, MaxEntriesPerGroup: 16, ReaderSlots: 2}
	store, err := CreateNodeStore(dir, testNodeIdentity(), testKey(), []NodeBootstrap{{Descriptor: testGroupDescriptor(10), Snapshot: nodeSnapshot(10, 1, 1)}, {Descriptor: testGroupDescriptor(20), Snapshot: nodeSnapshot(20, 1, 1)}}, options)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.BeginIncarnations([]uint64{1, 2}); err != nil {
		t.Fatal(err)
	}
	a, b, c := string(bytes.Repeat([]byte{'a'}, 10<<10)), string(bytes.Repeat([]byte{'b'}, 10<<10)), string(bytes.Repeat([]byte{'c'}, 20<<10))
	ready := []NodeReady{
		{GroupID: 1, Batch: raftmodel.PersistBatch{NodeIncarnation: 1, ReadyID: 1, Entries: []*pb.Entry{typedEntry(2, 2, pb.EntryNormal, a)}, HardState: hard(2, 2)}},
		{GroupID: 2, Batch: raftmodel.PersistBatch{NodeIncarnation: 1, ReadyID: 1, Entries: []*pb.Entry{typedEntry(2, 2, pb.EntryNormal, b), typedEntry(3, 2, pb.EntryNormal, c)}, HardState: hard(2, 3)}},
	}
	if err = store.PersistWave(ready); err != nil {
		t.Fatal(err)
	}
	first, _, _, _, err := store.engine.LookupExact(1, 2)
	if err != nil {
		t.Fatal(err)
	}
	shared, _, _, _, err := store.engine.LookupExact(2, 2)
	if err != nil {
		t.Fatal(err)
	}
	second, _, _, _, err := store.engine.LookupExact(2, 3)
	if err != nil {
		t.Fatal(err)
	}
	if first.Offset != shared.Offset || first.ExtentID != shared.ExtentID || second.Offset == first.Offset || second.ExtentID <= first.ExtentID || first.Bytes > nodeDataExtentBytes+uint64(store.crypto.aead.Overhead()) || second.Bytes > nodeDataExtentBytes+uint64(store.crypto.aead.Overhead()) {
		t.Fatalf("extent geometry first=%+v shared=%+v second=%+v", first, shared, second)
	}
	if err = store.engine.Rotate(nil); err != nil {
		t.Fatal(err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = OpenNodeStore(dir, testNodeIdentity(), testKey(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for _, tc := range []struct {
		group, index uint64
		want         string
	}{{1, 2, a}, {2, 2, b}, {2, 3, c}} {
		entries, readErr := store.Group(tc.group).Entries(tc.index, tc.index+1, ^uint64(0))
		if readErr != nil || len(entries) != 1 || string(entries[0].GetData()) != tc.want {
			t.Fatalf("group=%d index=%d entries=%d err=%v", tc.group, tc.index, len(entries), readErr)
		}
	}
	firstIndex, err := store.Group(2).FirstIndex()
	if err != nil || firstIndex != 2 {
		t.Fatalf("first index=%d err=%v", firstIndex, err)
	}
	lastIndex, err := store.Group(2).LastIndex()
	if err != nil || lastIndex != 3 {
		t.Fatalf("last index=%d err=%v", lastIndex, err)
	}
}

func TestNodeStoreEmptyAndOversizeExtentPacking(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "node")
	options := NodeStoreOptions{MaxWaveBytes: 1 << 20, MaxSegmentEvents: 32, RecentWaves: 16, MaxEntriesPerGroup: 16, ReaderSlots: 1}
	store, err := CreateNodeStore(dir, testNodeIdentity(), testKey(), []NodeBootstrap{{Descriptor: testGroupDescriptor(10), Snapshot: nodeSnapshot(10, 1, 1)}, {Descriptor: testGroupDescriptor(20), Snapshot: nodeSnapshot(20, 1, 1)}}, options)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err = store.BeginIncarnations([]uint64{1, 2}); err != nil {
		t.Fatal(err)
	}
	calls := 0
	store.persistWaveTest = func(wave seglog.Wave) error {
		calls++
		if calls == 1 && (len(wave.Blob) != 0 || wave.Batches[0].Entries[0].ExtentBytes != 0) {
			t.Fatalf("all-empty wave paid an extent: %+v blob=%d", wave.Batches[0].Entries[0], len(wave.Blob))
		}
		if calls == 2 {
			want := 40<<10 + store.crypto.aead.Overhead()
			if len(wave.Blob) != want || wave.Batches[0].Entries[0].ExtentBytes != uint64(want) || wave.Batches[0].Entries[1].ExtentBytes != uint64(want) || wave.Batches[1].Entries[0].ExtentBytes != 0 {
				t.Fatalf("oversize/empty packing blob=%d batches=%+v", len(wave.Blob), wave.Batches)
			}
		}
		return store.engine.PersistWave(wave)
	}
	if err = store.PersistWave([]NodeReady{{GroupID: 1, Batch: raftmodel.PersistBatch{NodeIncarnation: 1, ReadyID: 1, Entries: []*pb.Entry{typedEntry(2, 2, pb.EntryNormal, "")}, HardState: hard(2, 2)}}}); err != nil {
		t.Fatal(err)
	}
	large := string(bytes.Repeat([]byte{'x'}, 40<<10))
	if err = store.PersistWave([]NodeReady{
		{GroupID: 1, Batch: raftmodel.PersistBatch{NodeIncarnation: 1, ReadyID: 2, Entries: []*pb.Entry{typedEntry(3, 2, pb.EntryNormal, large), typedEntry(4, 2, pb.EntryNormal, "")}, HardState: hard(2, 4)}},
		{GroupID: 2, Batch: raftmodel.PersistBatch{NodeIncarnation: 1, ReadyID: 1, Entries: []*pb.Entry{typedEntry(2, 2, pb.EntryNormal, "")}, HardState: hard(2, 2)}},
	}); err != nil {
		t.Fatal(err)
	}
}

func TestNodeStorePreparedReadAndTermZeroAlloc(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "node")
	options := NodeStoreOptions{MaxWaveBytes: 1 << 20, MaxSegmentEvents: 64, RecentWaves: 16, MaxEntriesPerGroup: 16, ReaderSlots: 1}
	store, err := CreateNodeStore(dir, testNodeIdentity(), testKey(), []NodeBootstrap{{Descriptor: testGroupDescriptor(10), Snapshot: nodeSnapshot(10, 1, 1)}}, options)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.BeginIncarnations([]uint64{1}); err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	batch := raftmodel.PersistBatch{NodeIncarnation: 1, ReadyID: 1, Entries: []*pb.Entry{typedEntry(2, 2, pb.EntryNormal, "value")}, HardState: hard(2, 2)}
	if err = store.Group(1).Persist(batch); err != nil {
		t.Fatal(err)
	}
	view := store.Group(1)
	ciphertext := make([]byte, options.MaxWaveBytes)
	plaintext := make([]byte, options.MaxWaveBytes)
	if _, err = view.ReadEntryInto(2, ciphertext, plaintext); err != nil {
		t.Fatal(err)
	}
	if got := testing.AllocsPerRun(1000, func() {
		entry, readErr := view.ReadEntryInto(2, ciphertext, plaintext)
		if readErr != nil || len(entry.Data) != 5 {
			panic("prepared read failed")
		}
	}); got != 0 {
		t.Fatalf("ReadEntryInto allocs/run = %v", got)
	}
	if got := testing.AllocsPerRun(1000, func() {
		term, termErr := view.Term(2)
		if termErr != nil || term != 2 {
			panic("term lookup failed")
		}
	}); got != 0 {
		t.Fatalf("Term allocs/run = %v", got)
	}
}

func TestNodeStorePreparedPersistZeroAlloc(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "node")
	options := NodeStoreOptions{MaxWaveBytes: 1 << 20, MaxSegmentEvents: 128, RecentWaves: 64, MaxEntriesPerGroup: 64, ReaderSlots: 1}
	store, err := CreateNodeStore(dir, testNodeIdentity(), testKey(), []NodeBootstrap{{Descriptor: testGroupDescriptor(10), Snapshot: nodeSnapshot(10, 1, 1)}}, options)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.BeginIncarnations([]uint64{1}); err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	index, term, entryType := uint64(1), uint64(2), pb.EntryNormal
	vote, commit := uint64(1), uint64(1)
	entry := &pb.Entry{Index: &index, Term: &term, Type: &entryType, Data: []byte("x")}
	hardState := &pb.HardState{Term: &term, Vote: &vote, Commit: &commit}
	batch := raftmodel.PersistBatch{NodeIncarnation: 1, Entries: []*pb.Entry{entry}, HardState: hardState}
	view := store.Group(1)
	readyID := uint64(0)
	if got := testing.AllocsPerRun(10, func() {
		readyID++
		index++
		commit = index
		batch.ReadyID = readyID
		if persistErr := view.Persist(batch); persistErr != nil {
			panic(persistErr)
		}
	}); got != 0 {
		t.Fatalf("prepared Persist allocs/run = %v", got)
	}
}

func TestNodeStoreIncarnationWaveRecoveryAndFence(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "node")
	options := NodeStoreOptions{MaxWaveBytes: 1 << 20, MaxSegmentEvents: 64, RecentWaves: 32, MaxEntriesPerGroup: 16, ReaderSlots: 1}
	store, err := CreateNodeStore(dir, testNodeIdentity(), testKey(), []NodeBootstrap{{Descriptor: testGroupDescriptor(10), Snapshot: nodeSnapshot(10, 1, 1)}, {Descriptor: testGroupDescriptor(20), Snapshot: nodeSnapshot(20, 1, 1)}}, options)
	if err != nil {
		t.Fatal(err)
	}
	allocated, err := store.BeginIncarnations([]uint64{1, 2})
	if err != nil || len(allocated) != 2 || allocated[0].Incarnation != 1 || allocated[1].Incarnation != 1 {
		t.Fatalf("allocate = %#v, %v", allocated, err)
	}
	if err = store.PersistIncarnations(allocated); err != nil {
		t.Fatalf("exact allocation retry: %v", err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = OpenNodeStore(dir, testNodeIdentity(), testKey(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err = store.Group(1).Persist(raftmodel.PersistBatch{NodeIncarnation: 2, ReadyID: 1}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("unallocated incarnation = %v", err)
	}
	if err = store.Group(1).Persist(raftmodel.PersistBatch{NodeIncarnation: 1, ReadyID: 1}); err != nil {
		t.Fatalf("first Ready in allocated incarnation: %v", err)
	}
	if _, err = store.BeginIncarnations([]uint64{1}); err != nil {
		t.Fatal(err)
	}
	if err = store.Group(1).Persist(raftmodel.PersistBatch{NodeIncarnation: 1, ReadyID: 2}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("stale incarnation = %v", err)
	}
	if err = store.Group(1).Persist(raftmodel.PersistBatch{NodeIncarnation: 2, ReadyID: 1}); err != nil {
		t.Fatalf("new incarnation Ready 1: %v", err)
	}
}

func TestNodeStoreCheckpointPublicationCrashPhases(t *testing.T) {
	for _, phase := range []CheckpointPhase{CheckpointTempWritten, CheckpointFileSynced, CheckpointRenamed, CheckpointDirectorySynced, CheckpointBeforeLogReference} {
		t.Run(fmt.Sprint(phase), func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "node")
			options := NodeStoreOptions{MaxWaveBytes: 1 << 20, MaxSegmentEvents: 64, RecentWaves: 16, MaxEntriesPerGroup: 16, ReaderSlots: 1}
			store, err := CreateNodeStore(dir, testNodeIdentity(), testKey(), []NodeBootstrap{{Descriptor: testGroupDescriptor(10), Snapshot: nodeSnapshot(10, 1, 1)}}, options)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = store.BeginIncarnations([]uint64{1}); err != nil {
				t.Fatal(err)
			}
			injected := errors.New("checkpoint crash")
			store.checkpointHookTest = func(got CheckpointPhase) error {
				if got == phase {
					return injected
				}
				return nil
			}
			store.checkpointLeaveTempTest = phase == CheckpointTempWritten || phase == CheckpointFileSynced
			snapshot := nodeSnapshot(10, 2, 2)
			snapshot.Data = []byte("exact snapshot bytes")
			snapshot.Metadata.ConfState = &pb.ConfState{Voters: []uint64{1, 2}, Learners: []uint64{3}}
			batch := raftmodel.PersistBatch{NodeIncarnation: 1, ReadyID: 1, Snapshot: snapshot, HardState: hard(2, 2)}
			if err = store.Group(1).Persist(batch); !errors.Is(err, injected) {
				t.Fatalf("phase error = %v", err)
			}
			_ = store.Close()
			store, err = OpenNodeStore(dir, testNodeIdentity(), testKey(), options)
			if err != nil {
				t.Fatal(err)
			}
			state, _ := store.engine.Group(1)
			if state.Checkpoint.Index != 1 {
				t.Fatalf("failed publication referenced checkpoint %d", state.Checkpoint.Index)
			}
			if err = store.Group(1).Persist(batch); err != nil {
				t.Fatalf("safe retry: %v", err)
			}
			got, err := store.Group(1).Snapshot()
			if err != nil || !proto.Equal(got, snapshot) {
				t.Fatalf("snapshot roundtrip = %#v, %v", got, err)
			}
			_ = store.Close()
		})
	}
}

func TestNodeStoreCheckpointUnknownLogSyncRecoversReference(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "node")
	options := NodeStoreOptions{MaxWaveBytes: 1 << 20, MaxSegmentEvents: 64, RecentWaves: 16, MaxEntriesPerGroup: 16, ReaderSlots: 1}
	store, err := CreateNodeStore(dir, testNodeIdentity(), testKey(), []NodeBootstrap{{Descriptor: testGroupDescriptor(10), Snapshot: nodeSnapshot(10, 1, 1)}}, options)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.BeginIncarnations([]uint64{1}); err != nil {
		t.Fatal(err)
	}
	injected := errors.New("unknown data sync")
	store.engine.SetDataSyncForTesting(func(*os.File) error { return injected })
	snapshot := nodeSnapshot(10, 2, 2)
	snapshot.Data = []byte("durable-before-reference")
	batch := raftmodel.PersistBatch{NodeIncarnation: 1, ReadyID: 1, Snapshot: snapshot, HardState: hard(2, 2)}
	if err = store.Group(1).Persist(batch); !errors.Is(err, ErrPersistenceUnknown) || !errors.Is(err, injected) {
		t.Fatalf("unknown sync = %v", err)
	}
	_ = store.Close()
	store, err = OpenNodeStore(dir, testNodeIdentity(), testKey(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	got, err := store.Group(1).Snapshot()
	if err != nil || !proto.Equal(got, snapshot) {
		t.Fatalf("recovered checkpoint = %#v, %v", got, err)
	}
}
