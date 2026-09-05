package durable

import (
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
)

func TestEffectiveRecoveryFloorIncludesExactRootBanksAndReaders(t *testing.T) {
	leases, err := storeio.NewGenerationLeases(storeio.GenerationLeaseOptions{MaxLeases: 2})
	if err != nil {
		t.Fatal(err)
	}
	collection := &Collection{
		leases:     leases,
		readEpochs: storeio.NewReadEpochs(),
	}
	if err := collection.installExactRootRecoveryFloor(5); err != nil {
		t.Fatal(err)
	}
	if got := collection.effectiveRecoveryFloor(12); got != 5 {
		t.Fatalf("exact-root floor = %d, want 5", got)
	}
	if err := collection.installExactRootRecoveryFloor(4); err == nil {
		t.Fatal("accepted exact-root floor regression")
	}
	if err := collection.installExactRootRecoveryFloor(7); err != nil {
		t.Fatal(err)
	}
	if got := collection.effectiveRecoveryFloor(12); got != 7 {
		t.Fatalf("advanced exact-root floor = %d, want 7", got)
	}
	lease, err := leases.Acquire(3)
	if err != nil {
		t.Fatal(err)
	}
	if got := collection.effectiveRecoveryFloor(12); got != 3 {
		t.Fatalf("snapshot floor = %d, want 3", got)
	}
	lease.Release()
	epoch, ok := collection.readEpochs.Enter(4)
	if !ok {
		t.Fatal("failed to enter direct-read epoch")
	}
	if got := collection.effectiveRecoveryFloor(12); got != 4 {
		t.Fatalf("direct-read floor = %d, want 4", got)
	}
	epoch.Exit()
	collection.clearExactRootRecoveryFloor()
	if got := collection.effectiveRecoveryFloor(12); got != 12 {
		t.Fatalf("ordinary floor = %d, want 12", got)
	}
}
