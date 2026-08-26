package splitcontroller

import (
	"os"
	"path/filepath"
	"testing"
)

func TestActionWitnessLedgerRejectsSkipAndReplaySubstitution(t *testing.T) {
	manifest := [32]byte{9}
	root := filepath.Join(t.TempDir(), "runtime")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	registry, err := OpenRuntimeStoreRegistry(root, manifest, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := registry.Acquire(OperationID{1})
	if err != nil {
		t.Fatal(err)
	}
	witness := ActionWitness{
		Operation: OperationID{1}, PlanDigest: [32]byte{2}, CatalogGeneration: 7,
		CatalogDigest: [32]byte{3}, Sequence: 0x0200,
		PredecessorDigest: [32]byte{4}, Step: [32]byte{5},
	}
	if err = BeginActionWitness([]*RuntimeStoreLease{lease}, witness); err != nil {
		t.Fatal(err)
	}
	next := witness
	next.Sequence, next.Step = 0x0300, [32]byte{6}
	if err = BeginActionWitness([]*RuntimeStoreLease{lease}, next); err == nil {
		t.Fatal("advanced past an unsettled witness")
	}
	forged := witness
	forged.PredecessorDigest[0] ^= 0xff
	if err = BeginActionWitness([]*RuntimeStoreLease{lease}, forged); err == nil {
		t.Fatal("accepted same-sequence predecessor substitution")
	}
	if err = SettleActionWitness([]*RuntimeStoreLease{lease}, witness); err != nil {
		t.Fatal(err)
	}
	if err = BeginActionWitness([]*RuntimeStoreLease{lease}, next); err != nil {
		t.Fatal(err)
	}
	if err = lease.Release(); err != nil {
		t.Fatal(err)
	}
	if err = registry.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenRuntimeStoreRegistry(root, manifest, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	recovered, err := reopened.Acquire(OperationID{1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = recovered.Release() })
	if err = BeginActionWitness([]*RuntimeStoreLease{recovered}, next); err != nil {
		t.Fatalf("exact outcome-unknown intent did not survive restart: %v", err)
	}
}

func TestActionWitnessCodecCanonical(t *testing.T) {
	witness := ActionWitness{
		Operation: OperationID{1}, PlanDigest: [32]byte{2}, CatalogGeneration: 3,
		CatalogDigest: [32]byte{4}, Sequence: 5, PredecessorDigest: [32]byte{6}, Step: [32]byte{7},
		Settled: true,
	}
	raw, err := AppendActionWitness(nil, witness)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := OpenActionWitness(raw)
	if err != nil || opened != witness {
		t.Fatalf("opened=%+v err=%v", opened, err)
	}
	raw[193] = 1
	if _, err = OpenActionWitness(raw); err == nil {
		t.Fatal("accepted noncanonical reserved bytes")
	}
}
