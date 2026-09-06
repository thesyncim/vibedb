package raftstore

import (
	"errors"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftstore/seglog"
	pb "go.etcd.io/raft/v3/raftpb"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Freeze collection behind the first wave, then deliberately block the durable
// wave. The hint must complete while that durable wave is still blocked.
func TestNodeSubmissionSequencerHintCompletesBeforeIndependentDurableWave(t *testing.T) {
	for _, hintErr := range []error{nil, ErrInvalid, ErrPersistenceUnknown} {
		t.Run("hint="+errorName(hintErr), func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			durableEntered, durableRelease := make(chan struct{}), make(chan struct{})
			var releaseOnce, durableOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }); durableOnce.Do(func() { close(durableRelease) }) }
			var calls atomic.Int32
			var mu sync.Mutex
			var waves [][]uint64
			q := newTestSequencer(t, 8, func(ready []NodeReady) error {
				ids := make([]uint64, len(ready))
				for i := range ready {
					ids[i] = ready[i].GroupID
				}
				mu.Lock()
				waves = append(waves, ids)
				mu.Unlock()
				if calls.Add(1) == 1 {
					close(entered)
					<-release
					return nil
				}
				if ready[0].GroupID == 3 {
					return hintErr
				}
				close(durableEntered)
				<-durableRelease
				return nil
			})
			// Registered after q.Close's cleanup, so failures unblock the worker first.
			t.Cleanup(unblock)
			first := preparedSubmission(t, 1, 1)
			if _, err := q.TrySubmit(first); err != nil {
				t.Fatal(err)
			}
			<-entered
			durableA, hint, durableB := preparedSubmission(t, 2, 1), preparedSubmission(t, 3, 1), preparedSubmission(t, 4, 1)
			durableA.Ready.Batch.MustSync = true
			durableB.Ready.Batch.MustSync = true
			for _, s := range []*Submission{durableA, hint, durableB} {
				if _, err := q.TrySubmit(s); err != nil {
					t.Fatal(err)
				}
			}
			releaseOnce.Do(func() { close(release) })
			if errors.Is(hintErr, ErrPersistenceUnknown) {
				for _, s := range []*Submission{hint, durableA, durableB} {
					if _, err := s.Wait(); !errors.Is(err, ErrPersistenceUnknown) {
						t.Fatalf("completion: %v", err)
					}
				}
			} else {
				select {
				case <-durableEntered:
				case <-time.After(5 * time.Second):
					t.Fatal("durable wave never entered")
				}
				if _, done, err := hint.Poll(); !done || !errors.Is(err, hintErr) {
					t.Fatalf("hint done=%v err=%v", done, err)
				}
				for _, s := range []*Submission{durableA, durableB} {
					if _, done, _ := s.Poll(); done {
						t.Fatal("durable completion before persistence")
					}
				}
				durableOnce.Do(func() { close(durableRelease) })
				for _, s := range []*Submission{durableA, durableB} {
					if _, err := s.Wait(); err != nil {
						t.Fatal(err)
					}
				}
			}
			mu.Lock()
			defer mu.Unlock()
			want := [][]uint64{{1}, {3}, {2, 4}}
			if errors.Is(hintErr, ErrPersistenceUnknown) {
				want = want[:2]
			}
			if !reflect.DeepEqual(waves, want) {
				t.Fatalf("waves=%v want=%v", waves, want)
			}
		})
	}
}

func errorName(err error) string {
	if err == nil {
		return "success"
	}
	return err.Error()
}

func TestSubmissionHintCandidateIncludesEveryLogicalBatch(t *testing.T) {
	for _, durable := range []struct {
		name   string
		change func(*raftmodel.PersistBatch)
	}{
		{"must-sync", func(b *raftmodel.PersistBatch) { b.MustSync = true }},
		{"entries", func(b *raftmodel.PersistBatch) { b.Entries = []*pb.Entry{typedEntry(2, 2, pb.EntryNormal, "value")} }},
		{"snapshot", func(b *raftmodel.PersistBatch) { b.Snapshot = nodeSnapshot(1, 2, 2) }},
	} {
		t.Run(durable.name, func(t *testing.T) {
			s := preparedSubmission(t, 1, 1)
			batches := []raftmodel.PersistBatch{{NodeIncarnation: 1, ReadyID: 1}, {NodeIncarnation: 1, ReadyID: 2}}
			if err := s.PrepareReadySeries(1, batches); err != nil {
				t.Fatal(err)
			}
			if !submissionHintCandidate(s) {
				t.Fatal("metadata-only series rejected")
			}
			// Exercise the classifier defensively even for snapshots, which
			// Prepare currently rejects before admission.
			durable.change(&s.readySeries[1])
			if submissionHintCandidate(s) {
				t.Fatal("durable logical tail classified as hint")
			}
		})
	}
	s := preparedSubmission(t, 1, 1)
	if err := s.PrepareBeginIncarnations([]uint64{1}); err != nil {
		t.Fatal(err)
	}
	if submissionHintCandidate(s) || submissionHintCandidate(nil) {
		t.Fatal("control or nil classified as hint")
	}
}

// Exercise the real persistence classifier, sync boundary, and WAL recovery,
// not just a callback that declares a candidate to be metadata-only.
func TestNodeSubmissionSequencerHintEarlyCompletionPreservesDurableRecovery(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "node")
	options := NodeStoreOptions{MaxWaveBytes: 1 << 20, MaxSegmentEvents: 256, RecentWaves: 64, MaxEntriesPerGroup: 64, ReaderSlots: 1}
	var bootstraps []NodeBootstrap
	for group := uint64(1); group <= 4; group++ {
		bootstraps = append(bootstraps, NodeBootstrap{Descriptor: testGroupDescriptor(group), Snapshot: nodeSnapshot(group, 1, 1)})
	}
	store, err := CreateNodeStore(dir, testNodeIdentity(), testKey(), bootstraps, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if _, err = store.BeginIncarnations([]uint64{1, 2, 3, 4}); err != nil {
		t.Fatal(err)
	}
	for group := uint64(1); group <= 4; group++ {
		err = store.Group(group).Persist(raftmodel.PersistBatch{NodeIncarnation: 1, ReadyID: 1, HardState: hard(2, 1), Entries: []*pb.Entry{typedEntry(2, 2, pb.EntryNormal, "seed")}, MustSync: true})
		if err != nil {
			t.Fatal(err)
		}
	}
	entered, release := make(chan struct{}), make(chan struct{})
	durableEntered, durableRelease := make(chan struct{}), make(chan struct{})
	var firstOnce, durableOnce sync.Once
	var calls, syncs atomic.Int32
	store.SetDataSyncForTesting(func(file *os.File) error { syncs.Add(1); return file.Sync() })
	store.persistWaveTest = func(wave seglog.Wave) error {
		switch calls.Add(1) {
		case 1:
			close(entered)
			<-release
		case 2:
			close(durableEntered)
			<-durableRelease
		}
		return store.engine.PersistWave(wave)
	}
	q, err := NewNodeSubmissionSequencer(store, 8)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = q.Close() })
	t.Cleanup(func() { firstOnce.Do(func() { close(release) }); durableOnce.Do(func() { close(durableRelease) }) })
	var items [4]*Submission
	for i := range items {
		items[i] = preparedSubmission(t, uint64(i+1), 2)
		items[i].Ready.Batch.HardState = hard(2, 2)
		if i != 2 {
			items[i].Ready.Batch.Entries = []*pb.Entry{typedEntry(3, 2, pb.EntryNormal, "next")}
			items[i].Ready.Batch.MustSync = true
		}
		if _, err = q.TrySubmit(items[i]); err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			<-entered
		}
	}
	firstOnce.Do(func() { close(release) })
	select {
	case <-durableEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("durable wave never entered")
	}
	if _, done, err := items[2].Poll(); !done || err != nil {
		t.Fatalf("hint done=%v err=%v", done, err)
	}
	for _, i := range []int{1, 3} {
		if _, done, _ := items[i].Poll(); done {
			t.Fatal("durable completion preceded sync")
		}
	}
	if syncs.Load() != 1 {
		t.Fatalf("hint added a sync: %d", syncs.Load())
	}
	durableOnce.Do(func() { close(durableRelease) })
	for _, item := range items {
		if _, err = item.Wait(); err != nil {
			t.Fatal(err)
		}
	}
	if calls.Load() != 2 || syncs.Load() != 2 {
		t.Fatalf("durable batching lost: calls=%d syncs=%d", calls.Load(), syncs.Load())
	}
	if err = q.Close(); err != nil {
		t.Fatal(err)
	}
	// Lose only volatile hint knowledge, reproducing the WAL-only crash cut.
	// No required persistence is bypassed; all acknowledged entries were synced.
	store.commitHints = nil
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenNodeStore(dir, testNodeIdentity(), testKey(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	for group := uint64(1); group <= 4; group++ {
		high, wantCommit := uint64(4), uint64(2)
		if group == 3 {
			high, wantCommit = 3, 1
		}
		entries, err := reopened.Group(group).Entries(2, high, ^uint64(0))
		if err != nil || len(entries) != int(high-2) || string(entries[0].GetData()) != "seed" {
			t.Fatalf("group %d recovered entries=%v err=%v", group, entries, err)
		}
		if high == 4 && string(entries[1].GetData()) != "next" {
			t.Fatalf("group %d lost acknowledged append", group)
		}
		state, _, err := reopened.Group(group).InitialState()
		if err != nil || state.GetCommit() != wantCommit {
			t.Fatalf("group %d commit=%d want=%d err=%v", group, state.GetCommit(), wantCommit, err)
		}
	}
}
