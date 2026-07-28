package storeio

import (
	"errors"
	"sync"
	"testing"
)

func TestReadEpochsEnterExit(t *testing.T) {
	epochs := NewReadEpochs()
	if epochs.AnyActive() {
		t.Fatal("fresh table reports active readers")
	}
	if !epochs.SafeFrom(7) {
		t.Fatal("idle table must be safe from every generation")
	}
	if got := epochs.Minimum(9); got != 10 {
		t.Fatalf("idle minimum = %d, want successor 10", got)
	}

	epoch, ok := epochs.Enter(5)
	if !ok {
		t.Fatal("Enter on an idle table failed")
	}
	if !epochs.AnyActive() {
		t.Fatal("active slot not visible")
	}
	if epochs.SafeFrom(5) {
		t.Fatal("generation equal to an active slot must be unsafe")
	}
	if epochs.SafeFrom(3) {
		t.Fatal("generation below an active slot must be unsafe")
	}
	if !epochs.SafeFrom(6) {
		t.Fatal("generation above every active slot must be safe")
	}
	if got := epochs.Minimum(9); got != 5 {
		t.Fatalf("minimum with active slot = %d, want 5", got)
	}
	if !epoch.Update(8) {
		t.Fatal("Update failed")
	}
	if got := epochs.Minimum(9); got != 8 {
		t.Fatalf("minimum after update = %d, want 8", got)
	}
	epoch.Exit()
	if epochs.AnyActive() {
		t.Fatal("slot still active after Exit")
	}

	if _, ok := epochs.Enter(0); ok {
		t.Fatal("generation zero must be rejected")
	}
	if _, ok := epochs.Enter(readEpochActive); ok {
		t.Fatal("generation aliasing the active bit must be rejected")
	}
}

func TestReadEpochsCapacityFallsBack(t *testing.T) {
	epochs := NewReadEpochs()
	held := make([]ReadEpoch, 0, readEpochSlots)
	for range readEpochSlots {
		epoch, ok := epochs.Enter(3)
		if !ok {
			t.Fatalf("Enter failed with %d slots held", len(held))
		}
		held = append(held, epoch)
	}
	if _, ok := epochs.Enter(3); ok {
		t.Fatal("full table must decline entry")
	}
	held[0].Exit()
	if _, ok := epochs.Enter(4); !ok {
		t.Fatal("released slot must be claimable")
	}
	for _, epoch := range held[1:] {
		epoch.Exit()
	}
}

func TestReadEpochsWriterFenceDiverts(t *testing.T) {
	epochs := NewReadEpochs()
	epochs.BeginWriterFence()
	if _, ok := epochs.Enter(2); ok {
		t.Fatal("fenced table must divert new entries")
	}
	if !epochs.Diverted() {
		t.Fatal("fence not visible to Diverted")
	}
	epochs.BeginWriterFence()
	epochs.EndWriterFence()
	if !epochs.Diverted() {
		t.Fatal("nested fence released too early")
	}
	epochs.EndWriterFence()
	if epochs.Diverted() {
		t.Fatal("fence still raised after final EndWriterFence")
	}
	if _, ok := epochs.Enter(2); !ok {
		t.Fatal("entry after fence release failed")
	}
}

func TestReadEpochsClose(t *testing.T) {
	epochs := NewReadEpochs()
	epoch, ok := epochs.Enter(2)
	if !ok {
		t.Fatal("Enter failed")
	}
	if err := epochs.Close(); !errors.Is(err, ErrLeasesActive) {
		t.Fatalf("Close with an active reader = %v, want ErrLeasesActive", err)
	}
	epoch.Exit()
	if err := epochs.Close(); err != nil {
		t.Fatalf("Close after quiescence = %v", err)
	}
	if _, ok := epochs.Enter(2); ok {
		t.Fatal("closed table must divert entries")
	}
}

// TestReadEpochsConcurrentSlots exercises claim/release contention across
// more goroutines than slots under the race detector; overflow must decline,
// never corrupt a neighbouring claim.
func TestReadEpochsConcurrentSlots(t *testing.T) {
	epochs := NewReadEpochs()
	var group sync.WaitGroup
	for worker := range 4 * readEpochSlots {
		group.Add(1)
		go func(generation uint64) {
			defer group.Done()
			for range 10_000 {
				epoch, ok := epochs.Enter(generation)
				if !ok {
					continue
				}
				if minimum := epochs.Minimum(1 << 40); minimum > generation {
					t.Errorf("minimum %d ignored active generation %d", minimum, generation)
					epoch.Exit()
					return
				}
				epoch.Exit()
			}
		}(uint64(worker) + 1)
	}
	group.Wait()
	if epochs.AnyActive() {
		t.Fatal("slots leaked after concurrent churn")
	}
}
