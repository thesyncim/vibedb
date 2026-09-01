package raftstore

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftstore/seglog"
	pb "go.etcd.io/raft/v3/raftpb"
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

func TestNodeStoreRejectsCheckpointCollisionAndNamespaceReplacement(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "node")
	options := NodeStoreOptions{Store: testOptions(), FrameBytes: 1 << 20, Events: 64, WaveIDs: 16, EntriesPerGroup: 16, CachedSegments: 1}
	snapshot := nodeSnapshot(10, 1, 1)
	store, err := CreateNodeStore(dir, testIdentity(), testKey(), []NodeBootstrap{{GroupID: 10, Snapshot: snapshot}}, options)
	if err != nil {
		t.Fatal(err)
	}
	state, _ := store.engine.Group(10)
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
	if err = store.Group(10).Persist(batch); !errors.Is(err, ErrCorrupt) {
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
	if err = store.Group(10).Persist(raftmodel.PersistBatch{NodeIncarnation: 1, ReadyID: 1}); !errors.Is(err, ErrNamespaceChanged) {
		t.Fatalf("namespace replacement = %v", err)
	}
	if _, err = store.Group(10).Term(1); !errors.Is(err, ErrPersistenceUnknown) {
		t.Fatalf("poisoned read = %v", err)
	}
	_ = store.Close()
}

func typedEntry(index, term uint64, kind pb.EntryType, data string) *pb.Entry {
	return &pb.Entry{Index: &index, Term: &term, Type: &kind, Data: []byte(data)}
}

func TestNodeStoreMultiGroupRetryAndReopen(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "node")
	options := NodeStoreOptions{Store: testOptions(), FrameBytes: 1 << 20, Events: 256, WaveIDs: 64, EntriesPerGroup: 64, CachedSegments: 2}
	store, err := CreateNodeStore(dir, testIdentity(), testKey(), []NodeBootstrap{
		{GroupID: 10, Snapshot: nodeSnapshot(10, 1, 1)},
		{GroupID: 20, Snapshot: nodeSnapshot(20, 1, 1)},
	}, options)
	if err != nil {
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
		{GroupID: 10, Batch: raftmodel.PersistBatch{NodeIncarnation: 1, ReadyID: 1, Entries: []*pb.Entry{conf}, HardState: hard(2, 2)}},
		{GroupID: 20, Batch: raftmodel.PersistBatch{NodeIncarnation: 1, ReadyID: 1, Entries: []*pb.Entry{confV2}, HardState: hard(2, 2)}},
	}
	if err = store.PersistWave(wave); err != nil {
		t.Fatal(err)
	}
	if persistCalls != 1 {
		t.Fatalf("multi-group Engine calls = %d, want 1", persistCalls)
	}
	second := NodeReady{GroupID: 20, Batch: raftmodel.PersistBatch{NodeIncarnation: 1, ReadyID: 2, Entries: []*pb.Entry{typedEntry(3, 2, pb.EntryNormal, "next")}, HardState: hard(2, 3)}}
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
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenNodeStore(dir, testIdentity(), testKey(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	entries, err := reopened.Group(10).Entries(2, 3, ^uint64(0))
	if err != nil || len(entries) != 1 || entries[0].GetType() != pb.EntryConfChange || !bytes.Equal(entries[0].GetData(), []byte("conf")) {
		t.Fatalf("group 10 entries = %#v, %v", entries, err)
	}
	entries, err = reopened.Group(20).Entries(2, 4, ^uint64(0))
	if err != nil || len(entries) != 2 || entries[0].GetType() != pb.EntryConfChangeV2 || !bytes.Equal(entries[1].GetData(), []byte("next")) {
		t.Fatalf("group 20 entries = %#v, %v", entries, err)
	}
	if hardState, confState, err := reopened.Group(20).InitialState(); err != nil || hardState.GetCommit() != 3 || len(confState.GetVoters()) != 1 {
		t.Fatalf("initial state = %#v %#v, %v", hardState, confState, err)
	}
}

func TestNodeStorePreparedReadAndTermZeroAlloc(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "node")
	options := NodeStoreOptions{Store: testOptions(), FrameBytes: 1 << 20, Events: 64, WaveIDs: 16, EntriesPerGroup: 16, CachedSegments: 1}
	store, err := CreateNodeStore(dir, testIdentity(), testKey(), []NodeBootstrap{{GroupID: 10, Snapshot: nodeSnapshot(10, 1, 1)}}, options)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	batch := raftmodel.PersistBatch{NodeIncarnation: 1, ReadyID: 1, Entries: []*pb.Entry{typedEntry(2, 2, pb.EntryNormal, "value")}, HardState: hard(2, 2)}
	if err = store.Group(10).Persist(batch); err != nil {
		t.Fatal(err)
	}
	view := store.Group(10)
	ciphertext := make([]byte, options.FrameBytes)
	plaintext := make([]byte, options.FrameBytes)
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
	options := NodeStoreOptions{Store: testOptions(), FrameBytes: 1 << 20, Events: 128, WaveIDs: 64, EntriesPerGroup: 64, CachedSegments: 1}
	store, err := CreateNodeStore(dir, testIdentity(), testKey(), []NodeBootstrap{{GroupID: 10, Snapshot: nodeSnapshot(10, 1, 1)}}, options)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	index, term, entryType := uint64(2), uint64(2), pb.EntryNormal
	vote, commit := uint64(1), uint64(2)
	entry := &pb.Entry{Index: &index, Term: &term, Type: &entryType, Data: []byte("x")}
	hardState := &pb.HardState{Term: &term, Vote: &vote, Commit: &commit}
	batch := raftmodel.PersistBatch{NodeIncarnation: 1, ReadyID: 1, Entries: []*pb.Entry{entry}, HardState: hardState}
	view := store.Group(10)
	store.persistWaveTest = func(seglog.Wave) error { return nil }
	store.namespaceProofTest = func() error { return nil }
	if got := testing.AllocsPerRun(10, func() {
		if persistErr := view.Persist(batch); persistErr != nil {
			panic(persistErr)
		}
	}); got != 0 {
		t.Fatalf("prepared Persist allocs/run = %v", got)
	}
}
