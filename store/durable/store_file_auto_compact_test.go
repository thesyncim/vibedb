package durable

import (
	"errors"
	"os"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/storeio"
)

func automaticCompactionTestCollection(t *testing.T, policy AutomaticCompactionPolicy) *Collection {
	t.Helper()
	file, err := os.CreateTemp(t.TempDir(), "auto-compact-*.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })
	collection, err := Create(file, Options{
		Durability:          DurabilityAsyncVisible,
		AutomaticCompaction: policy,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = collection.Close() })
	return collection
}

func waitAutomaticCompaction(t *testing.T, collection *Collection) AutomaticCompactionStatus {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		status := collection.AutomaticCompactionStatus()
		if status.Successes+status.Failures != 0 && !status.InFlight {
			return status
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("automatic compaction did not settle: %+v", collection.AutomaticCompactionStatus())
	return AutomaticCompactionStatus{}
}

func TestAutomaticCompactionDisabledByDefault(t *testing.T) {
	collection := automaticCompactionTestCollection(t, AutomaticCompactionPolicy{})
	for i := 0; i < 8; i++ {
		key := []byte{byte('a' + i)}
		if _, err := collection.Put(key, []byte(`{"v":1}`)); err != nil {
			t.Fatal(err)
		}
	}
	status := collection.AutomaticCompactionStatus()
	if status.Enabled || status.Checks != 0 || status.Starts != 0 || status.InFlight {
		t.Fatalf("disabled automatic compaction did work: %+v", status)
	}
}

func TestAutomaticCompactionDefersPinnedSnapshotThenRuns(t *testing.T) {
	collection := automaticCompactionTestCollection(t, AutomaticCompactionPolicy{
		Enabled: true, TriggerBytes: 1, MinGenerationInterval: 1,
		MaxRecoveryLagGenerations: 64,
	})
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := collection.Put([]byte("a"), []byte(`{"v":1}`)); err != nil {
		t.Fatal(err)
	}
	status := collection.AutomaticCompactionStatus()
	if status.ReaderSkips == 0 || status.Starts != 0 || status.DebtBytes == 0 {
		t.Fatalf("snapshot did not defer debt: %+v", status)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := collection.Put([]byte("b"), []byte(`{"v":2}`)); err != nil {
		t.Fatal(err)
	}
	status = waitAutomaticCompaction(t, collection)
	if status.Starts != 1 || status.Successes != 1 || status.Failures != 0 ||
		status.DebtBytes != 0 || !status.Armed {
		t.Fatalf("automatic compaction result = %+v", status)
	}
}

func TestCompactOnlineSingleFlight(t *testing.T) {
	collection := automaticCompactionTestCollection(t, AutomaticCompactionPolicy{})
	collection.autoCompactionFlight.Store(true)
	_, err := collection.CompactOnline()
	collection.autoCompactionFlight.Store(false)
	if !errors.Is(err, storeio.ErrQueueFull) {
		t.Fatalf("concurrent compaction error = %v", err)
	}
}

func TestAutomaticCompactionPolicyRejectsInvalidHysteresis(t *testing.T) {
	err := ValidateOptions(Options{AutomaticCompaction: AutomaticCompactionPolicy{
		Enabled: true, TriggerBytes: 4096, RearmBytes: 4096,
	}})
	if err == nil {
		t.Fatal("equal trigger/rearm watermarks accepted")
	}
}

func TestAutomaticCompactionGenerationGateAllocatesNothing(t *testing.T) {
	collection := &Collection{options: normalizedFileStoreOptions{Options: Options{
		AutomaticCompaction: AutomaticCompactionPolicy{
			Enabled: true, TriggerBytes: 1, RearmBytes: 0,
			MinGenerationInterval: 1024, MaxRecoveryLagGenerations: 2,
		},
	}}}
	collection.autoCompactionLastCheck.Store(7)
	state := &fileStoreState{}
	state.root.Generation = 8
	if allocations := testing.AllocsPerRun(1000, func() {
		collection.considerAutomaticCompaction(state)
	}); allocations != 0 {
		t.Fatalf("cold admission allocations = %v", allocations)
	}
	if collection.autoCompactionChecks.Load() != 0 ||
		collection.autoCompactionWorker.Load() {
		t.Fatalf("generation gate admitted work")
	}
}
