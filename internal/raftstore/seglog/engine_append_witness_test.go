package seglog

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

func TestEngineAppendWitnessConcurrentSealerFailure(t *testing.T) {
	engine := newReservedEngine(t, 1, 2)
	wave := Wave{ID: waveID(1), Batches: []ReadyBatch{
		{GroupID: 1, Entries: []Entry{{Index: 1, Term: 1}}},
		{GroupID: 2, Entries: []Entry{{Index: 1, Term: 1}}},
	}}
	if err := engine.PersistWave(wave); err != nil {
		t.Fatal(err)
	}
	if sequence, groups := engine.AppendWitness(); sequence != 1 || groups != 2 {
		t.Fatalf("initial append witness=%d/%d", sequence, groups)
	}
	if err := engine.PersistWave(wave); err != nil {
		t.Fatal(err)
	}
	if sequence, groups := engine.AppendWitness(); sequence != 1 || groups != 2 {
		t.Fatalf("exact retry changed append witness=%d/%d", sequence, groups)
	}

	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	failure := errors.New("sealer failed after sync")
	if err := engine.Rotate(func(phase RotationPhase) error {
		if phase == RotationSealedSynced {
			close(entered)
			<-release
			return failure
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	<-entered
	if sequence, groups := engine.AppendWitness(); sequence != 1 || groups != 2 {
		t.Fatalf("rotation sync changed append witness=%d/%d", sequence, groups)
	}
	started := make(chan struct{})
	var stop atomic.Bool
	var samples atomic.Uint64
	var sampler sync.WaitGroup
	sampler.Add(1)
	go func() {
		defer sampler.Done()
		for !stop.Load() {
			// Sequence keeps its original zero-on-unusable contract while
			// synchronizing with the sealer's mutation of the poison state.
			if sequence := engine.Sequence(); sequence > 1 {
				t.Errorf("unexpected sequence=%d", sequence)
			}
			sequence, groups := engine.AppendWitness()
			if sequence != 1 || groups != 2 {
				if sequence != 0 || groups != 0 {
					t.Errorf("inconsistent append witness=%d/%d", sequence, groups)
				}
			}
			if samples.Add(1) == 1 {
				close(started)
			}
		}
	}()
	defer func() { stop.Store(true); sampler.Wait() }()
	<-started
	releaseOnce.Do(func() { close(release) })
	if err := engine.WaitSeal(); !errors.Is(err, failure) {
		t.Fatalf("sealer result=%v", err)
	}
	if sequence, groups := engine.AppendWitness(); sequence != 0 || groups != 0 || engine.Sequence() != 0 {
		t.Fatalf("poisoned engine exposed append witness=%d/%d", sequence, groups)
	}
	if samples.Load() == 0 {
		t.Fatal("concurrent append witness sampler did not run")
	}
}
