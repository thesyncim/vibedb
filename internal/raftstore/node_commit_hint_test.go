package raftstore

import (
	"errors"
	"os"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftstore/seglog"
	pb "go.etcd.io/raft/v3/raftpb"
)

func hintFixture(t *testing.T) (*NodeStore, string, NodeStoreOptions) {
	t.Helper()
	s, dir, options := coordinateFixture(t)
	if err := s.Group(1).Persist(raftmodel.PersistBatch{NodeIncarnation: 1, ReadyID: 1, HardState: hard(2, 1), Entries: []*pb.Entry{typedEntry(2, 2, pb.EntryNormal, "durable")}, MustSync: true}); err != nil {
		t.Fatal(err)
	}
	return s, dir, options
}
func commitHint(id, commit uint64) raftmodel.PersistBatch {
	return raftmodel.PersistBatch{NodeIncarnation: 1, ReadyID: id, HardState: hard(2, commit)}
}

func TestNodeCommitHintIsVolatileDetachedAndRetryExact(t *testing.T) {
	s, dir, options := hintFixture(t)
	if _, _, err := s.Group(1).LogBounds(); err != nil {
		t.Fatal(err)
	}
	syncs := 0
	s.SetDataSyncForTesting(func(f *os.File) error { syncs++; return f.Sync() })
	batch := commitHint(2, 2)
	if err := s.Group(1).Persist(batch); err != nil {
		t.Fatal(err)
	}
	if err := s.Group(1).Persist(batch); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if syncs != 0 {
		t.Fatalf("hint performed %d syncs", syncs)
	}
	durable, _ := s.engine.Metadata(1)
	if durable.Hard.Commit != 1 || durable.ReadyID != 1 {
		t.Fatalf("hint advanced durable metadata: %+v", durable)
	}
	*batch.HardState.Commit = 1
	if err := s.Group(1).Persist(batch); !errors.Is(err, ErrRetryConflict) {
		t.Fatalf("changed retry: %v", err)
	}
	live, _, err := s.Group(1).InitialState()
	if err != nil || live.GetCommit() != 2 {
		t.Fatalf("detached live commit: %v %v", live, err)
	}
	if last, commit, err := s.Group(1).LogBounds(); err != nil || last != 2 || commit != 2 {
		t.Fatalf("bounds %d %d %v", last, commit, err)
	}
	// There were no writes or barriers after the durable seed. Discard only
	// volatile process state to inspect the exact WAL-only recovery cut.
	s.commitHints = nil
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenNodeStore(dir, testNodeIdentity(), testKey(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	live, _, err = reopened.Group(1).InitialState()
	if err != nil || live.GetCommit() != 1 {
		t.Fatalf("WAL-only commit: %v %v", live, err)
	}
	entries, err := reopened.Group(1).Entries(2, 3, ^uint64(0))
	if err != nil || len(entries) != 1 || string(entries[0].GetData()) != "durable" {
		t.Fatalf("durable data lost: %v %v", entries, err)
	}
}

func TestNodeCommitHintFoldsIntoNextEntryAndRetainsRetryAcrossReopen(t *testing.T) {
	s, dir, options := hintFixture(t)
	if err := s.Group(1).Persist(commitHint(2, 2)); err != nil {
		t.Fatal(err)
	}
	var span uint64
	s.persistWaveTest = func(w seglog.Wave) error { span = w.Batches[0].ReadySpan; return s.engine.PersistWave(w) }
	next := raftmodel.PersistBatch{NodeIncarnation: 1, ReadyID: 3, Entries: []*pb.Entry{typedEntry(3, 2, pb.EntryNormal, "next")}, MustSync: true}
	if err := s.Group(1).Persist(next); err != nil {
		t.Fatal(err)
	}
	state, _ := s.engine.Metadata(1)
	if span != 2 || state.ReadyID != 3 || state.Hard.Commit != 2 || len(s.commitHints) != 0 {
		t.Fatalf("fold span=%d state=%+v hints=%d", span, state, len(s.commitHints))
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenNodeStore(dir, testNodeIdentity(), testKey(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if err := reopened.Group(1).Persist(next); err != nil {
		t.Fatalf("exact original retry after reopen: %v", err)
	}
	next.Entries = []*pb.Entry{typedEntry(3, 2, pb.EntryNormal, "different")}
	if err := reopened.Group(1).Persist(next); !errors.Is(err, ErrRetryConflict) {
		t.Fatalf("conflicting recovered retry: %v", err)
	}
}

func TestNodeCommitHintRejectsInvalidTransitionAndProtectsCommittedSuffix(t *testing.T) {
	s, _, _ := hintFixture(t)
	if err := s.Group(1).Persist(commitHint(2, 3)); !errors.Is(err, ErrInvalid) {
		t.Fatalf("commit past durable log: %v", err)
	}
	if err := s.Group(1).Persist(commitHint(2, 2)); err != nil {
		t.Fatal(err)
	}
	for _, batch := range []raftmodel.PersistBatch{
		commitHint(3, 1), commitHint(4, 2),
		{NodeIncarnation: 1, ReadyID: 3, Entries: []*pb.Entry{typedEntry(2, 2, pb.EntryNormal, "overwrite")}, MustSync: true},
	} {
		if err := s.Group(1).Persist(batch); !errors.Is(err, ErrInvalid) {
			t.Fatalf("invalid transition accepted: %v", err)
		}
	}
	state, _ := s.engine.Metadata(1)
	if state.ReadyID != 1 {
		t.Fatal("invalid transition flushed hints")
	}
}

func TestNodeCommitHintBoundsPendingSpanAndLargeSeries(t *testing.T) {
	s, _, _ := hintFixture(t)
	syncs := 0
	s.SetDataSyncForTesting(func(f *os.File) error { syncs++; return f.Sync() })
	for id := uint64(2); id <= 16; id++ {
		if err := s.Group(1).Persist(commitHint(id, 2)); err != nil {
			t.Fatal(err)
		}
	}
	if syncs != 0 {
		t.Fatalf("early sync: %d", syncs)
	}
	if err := s.Group(1).Persist(commitHint(17, 2)); err != nil {
		t.Fatal(err)
	}
	if syncs != 1 || len(s.commitHints) != 0 {
		t.Fatalf("bounded fold syncs=%d hints=%d", syncs, len(s.commitHints))
	}
	if err := s.Group(1).Persist(commitHint(18, 2)); err != nil {
		t.Fatal(err)
	}
	batches := make([]raftmodel.PersistBatch, MaxReadySeries)
	for i := range batches {
		batches[i] = commitHint(uint64(19+i), 2)
	}
	if err := s.PersistReadySeries(1, batches); err != nil {
		t.Fatal(err)
	}
	if syncs != 3 {
		t.Fatalf("overflow requires one pending flush plus caller series: %d", syncs)
	}
	if err := s.PersistReadySeries(1, batches); err != nil {
		t.Fatalf("large series retry: %v", err)
	}
}

func TestNodeCommitHintFlushesCheckpointAndIncarnationBoundaries(t *testing.T) {
	for _, boundary := range []string{"checkpoint", "incarnation", "close"} {
		t.Run(boundary, func(t *testing.T) {
			s, dir, options := hintFixture(t)
			if err := s.Group(1).Persist(commitHint(2, 2)); err != nil {
				t.Fatal(err)
			}
			var err error
			switch boundary {
			case "checkpoint":
				err = s.publishGroupCheckpointSequenced(1, nodeSnapshot(10, 2, 2))
			case "incarnation":
				_, err = s.BeginIncarnations([]uint64{1})
			case "close":
				err = s.Close()
			}
			if err != nil {
				t.Fatal(err)
			}
			if err = s.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := OpenNodeStore(dir, testNodeIdentity(), testKey(), options)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			state, _ := reopened.engine.Metadata(1)
			if state.Hard.Commit != 2 {
				t.Fatalf("boundary lost commit: %+v", state)
			}
		})
	}
}

func TestNodeCommitHintDoesNotPublishMixedWaveOnSyncFailure(t *testing.T) {
	s, _, _ := hintFixture(t)
	boom := errors.New("injected sync failure")
	s.SetDataSyncForTesting(func(f *os.File) error { return boom })
	err := s.PersistWave([]NodeReady{{GroupID: 1, Batch: commitHint(2, 2)}, {GroupID: 2, Batch: raftmodel.PersistBatch{NodeIncarnation: 1, ReadyID: 1, HardState: hard(2, 2), Entries: []*pb.Entry{typedEntry(2, 2, pb.EntryNormal, "other")}, MustSync: true}}})
	if !errors.Is(err, ErrPersistenceUnknown) || len(s.commitHints) != 0 {
		t.Fatalf("mixed wave published on failure: %v %d", err, len(s.commitHints))
	}
	if err = s.Group(1).Persist(commitHint(2, 2)); !errors.Is(err, ErrPersistenceUnknown) {
		t.Fatalf("hint accepted on poisoned engine: %v", err)
	}
}

func TestNodeCommitHintWarmAdmissionAndFoldAllocateZero(t *testing.T) {
	s, _, _ := hintFixture(t)
	// Warm the bounded map once. Every measured pair has one volatile hint and
	// one required empty barrier, folding the hint without borrowing its fields.
	if err := s.Group(1).Persist(commitHint(2, 2)); err != nil {
		t.Fatal(err)
	}
	id := uint64(3)
	batch := commitHint(id, 2)
	batch.MustSync = true
	if err := s.Group(1).Persist(batch); err != nil {
		t.Fatal(err)
	}
	view := s.Group(1)
	allocs := testing.AllocsPerRun(10, func() {
		id++
		batch.ReadyID = id
		batch.MustSync = false
		if err := view.Persist(batch); err != nil {
			panic(err)
		}
		id++
		batch.ReadyID = id
		batch.MustSync = true
		if err := view.Persist(batch); err != nil {
			panic(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("hint/fold allocations=%v", allocs)
	}
}

func TestNodeCommitHintTermVoteAndRequiredBarrierStillSync(t *testing.T) {
	for _, kind := range []string{"term", "vote", "barrier"} {
		t.Run(kind, func(t *testing.T) {
			s, _, _ := hintFixture(t)
			batch := commitHint(2, 2)
			switch kind {
			case "term":
				*batch.HardState.Term = 3
			case "vote":
				seed := commitHint(2, 1)
				*seed.HardState.Term = 3
				*seed.HardState.Vote = 0
				seed.MustSync = true
				if err := s.Group(1).Persist(seed); err != nil {
					t.Fatal(err)
				}
				batch.ReadyID = 3
				*batch.HardState.Term = 3
				batch.HardState.Vote = uint64Pointer(10)
			case "barrier":
				batch.MustSync = true
			}
			syncs := 0
			s.SetDataSyncForTesting(func(f *os.File) error { syncs++; return f.Sync() })
			if err := s.Group(1).Persist(batch); err != nil {
				t.Fatal(err)
			}
			state, _ := s.engine.Metadata(1)
			if syncs != 1 || state.ReadyID != batch.ReadyID || len(s.commitHints) != 0 {
				t.Fatalf("required metadata deferred: syncs=%d state=%+v", syncs, state)
			}
		})
	}
}

func TestNodeCommitHintPreflightIsAtomicAcrossGroupsAndSeries(t *testing.T) {
	s, _, _ := hintFixture(t)
	err := s.PersistWave([]NodeReady{{GroupID: 1, Batch: commitHint(2, 2)}, {GroupID: 2, Batch: raftmodel.PersistBatch{NodeIncarnation: 1, ReadyID: 1, HardState: hard(1, 99)}}})
	if !errors.Is(err, ErrInvalid) || len(s.commitHints) != 0 {
		t.Fatalf("failed wave published partial hint: %v", err)
	}
	err = s.PersistReadySeries(1, []raftmodel.PersistBatch{commitHint(2, 3), commitHint(3, 3)})
	if !errors.Is(err, ErrInvalid) || len(s.commitHints) != 0 {
		t.Fatalf("invalid series admitted: %v", err)
	}
	s.namespaceProofTest = func() error { return errors.New("missing namespace") }
	if err = s.Group(1).Persist(commitHint(2, 2)); err == nil || len(s.commitHints) != 0 {
		t.Fatalf("hint bypassed namespace proof: %v", err)
	}
}
