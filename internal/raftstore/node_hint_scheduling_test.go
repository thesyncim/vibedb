package raftstore

import (
	"errors"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	pb "go.etcd.io/raft/v3/raftpb"
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
