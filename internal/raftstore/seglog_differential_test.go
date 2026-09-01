package raftstore

import (
	"encoding/binary"
	"math"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftstore/seglog"
	pb "go.etcd.io/raft/v3/raftpb"
)

// This keeps the new node-wide state machine aligned with the existing
// single-group Store while the production adapter is still intentionally
// absent. The sequence covers append, legal suffix replacement, HardState,
// close, and recovery on both implementations.
func TestSeglogEngineDifferentialReadySemantics(t *testing.T) {
	path, store, options := createTestStore(t)
	incarnation, err := store.BeginIncarnation()
	if err != nil {
		t.Fatal(err)
	}
	engineDir := filepath.Join(t.TempDir(), "seglog")
	engine, err := seglog.CreateEngine(engineDir)
	if err != nil {
		t.Fatal(err)
	}
	if err = engine.Reserve(1<<20, 128, 32); err != nil {
		t.Fatal(err)
	}
	if err = engine.ReserveGroup(1, 128); err != nil {
		t.Fatal(err)
	}
	baseHard := seglog.HardState{Term: 1, Vote: 1, Commit: 1}
	base := seglog.Checkpoint{ID: [16]byte{1}, Index: 1, Term: 1}
	if err = engine.PersistWave(seglog.Wave{ID: differentialWaveID(1), Batches: []seglog.ReadyBatch{{GroupID: 1, Checkpoint: &base, Hard: &baseHard}}}); err != nil {
		t.Fatal(err)
	}
	batches := []raftmodel.PersistBatch{
		{NodeIncarnation: incarnation, ReadyID: 1, HardState: hard(2, 2), Entries: []*pb.Entry{entry(2, 2, "a"), entry(3, 2, "b"), entry(4, 2, "c")}},
		{NodeIncarnation: incarnation, ReadyID: 2, HardState: hard(3, 3), Entries: []*pb.Entry{entry(3, 3, "B"), entry(4, 3, "C")}},
		{NodeIncarnation: incarnation, ReadyID: 3, HardState: hard(4, 5), Entries: []*pb.Entry{entry(5, 4, "d")}},
	}
	confType, confV2Type := pb.EntryConfChange, pb.EntryConfChangeV2
	batches[0].Entries[1].Type = &confType
	batches[0].Entries[2].Type = &confV2Type
	for _, batch := range batches {
		if err = store.Persist(batch); err != nil {
			t.Fatal(err)
		}
		entries := make([]seglog.Entry, len(batch.Entries))
		for i, entry := range batch.Entries {
			entries[i] = seglog.Entry{Index: entry.GetIndex(), Term: entry.GetTerm(), Type: entry.GetType(), Data: entry.Data}
		}
		hard := seglog.HardState{Term: batch.HardState.GetTerm(), Vote: batch.HardState.GetVote(), Commit: batch.HardState.GetCommit()}
		replace := uint64(0)
		state, _ := engine.Group(1)
		engineLast := state.Checkpoint.Index
		if len(state.Entries) != 0 {
			engineLast = state.Entries[len(state.Entries)-1].Index
		}
		if len(entries) != 0 && entries[0].Index <= engineLast {
			replace = entries[0].Index
		}
		if err = engine.PersistWave(seglog.Wave{ID: differentialWaveID(batch.ReadyID + 1), Batches: []seglog.ReadyBatch{{GroupID: 1, ReplaceFrom: replace, Entries: entries, Hard: &hard}}}); err != nil {
			t.Fatal(err)
		}
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	if err = engine.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(path, testIdentity(), testBootstrap().TopologyRecoveryEpoch, testKey(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	engine, err = seglog.OpenEngine(engineDir)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	storeHard, _, err := store.InitialState()
	if err != nil {
		t.Fatal(err)
	}
	state, ok := engine.Group(1)
	if !ok || state.Hard.Term != storeHard.GetTerm() || state.Hard.Vote != storeHard.GetVote() || state.Hard.Commit != storeHard.GetCommit() {
		t.Fatalf("HardState differs: engine=%+v store=%v", state.Hard, storeHard)
	}
	first, _ := store.FirstIndex()
	last, _ := store.LastIndex()
	storeEntries, err := store.Entries(first, last+1, math.MaxUint64)
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Entries) != len(storeEntries) {
		t.Fatalf("entry lengths differ: engine=%d store=%d", len(state.Entries), len(storeEntries))
	}
	for i := range storeEntries {
		if state.Entries[i].Index != storeEntries[i].GetIndex() || state.Entries[i].Term != storeEntries[i].GetTerm() || state.Entries[i].Type != storeEntries[i].GetType() {
			t.Fatalf("entry %d differs: engine=%+v store=%v", i, state.Entries[i], storeEntries[i])
		}
	}
}

func differentialWaveID(n uint64) seglog.WaveID {
	var id seglog.WaveID
	binary.LittleEndian.PutUint64(id[:8], n)
	id[15] = 0xd1
	return id
}
