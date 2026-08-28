package splitcontroller

import (
	"bytes"
	"errors"
	"testing"
)

func TestTerminalRetirementCanonicalRoundTripRejectsForgery(t *testing.T) {
	want := TerminalRetirement{
		Operation: OperationID{1}, PlanDigest: [32]byte{2}, CatalogGeneration: 3,
		CatalogDigest: [32]byte{4}, Proof: [32]byte{5},
	}
	raw := appendTerminalRetirement(nil, want)
	got, err := openTerminalRetirement(raw)
	if err != nil || got != want || len(raw) != terminalRetirementRequestBytes {
		t.Fatalf("got=%+v bytes=%d err=%v", got, len(raw), err)
	}
	if reencoded := appendTerminalRetirement(nil, got); !bytes.Equal(raw, reencoded) {
		t.Fatal("terminal retirement encoding is not canonical")
	}
	for _, offset := range []int{8, 40, 72, 80, 112, 144, len(raw) - 1} {
		corrupt := append([]byte(nil), raw...)
		corrupt[offset] ^= 0x80
		if _, openErr := openTerminalRetirement(corrupt); openErr == nil {
			t.Fatalf("forgery at byte %d accepted", offset)
		}
	}
	response := appendTerminalRetirementResponse(nil, want)
	if !validTerminalRetirementResponse(response, want) {
		t.Fatal("exact terminal response rejected")
	}
	wrong := want
	wrong.Proof[0]++
	if validTerminalRetirementResponse(response, wrong) {
		t.Fatal("terminal response replayed across proof")
	}
}

func TestLocalTerminalRetirementValidatesBeforeRevocationAndCollectsOnce(t *testing.T) {
	operation := runtimeOperation(17)
	planDigest := testManifestDigest("terminal plan")
	catalogDigest := testManifestDigest("terminal catalog")
	proof := testManifestDigest("terminal proof")
	manifest := testManifestDigest("terminal manifest")
	registry, err := OpenRuntimeStoreRegistry(preparedRuntimeRoot(t), manifest, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Close()
	lease, err := registry.Acquire(operation)
	if err != nil {
		t.Fatal(err)
	}
	grants, _ := NewDynamicShardActionGrants(1)
	binder, _ := NewBoundPlanAdmissionBinder(&planAdmissionGrantFactoryStub{}, grants)
	binder.active = map[OperationID]boundPlanAdmission{operation: {
		digest: planDigest, catalogGeneration: 9, catalogDigest: catalogDigest,
		leases: []*RuntimeStoreLease{lease}, registries: []*RuntimeStoreRegistry{registry},
	}}
	target := ShardActionTarget{}
	key := shardActionGrantKey{operation: operation, digest: planDigest, target: target}
	grants.grants[key] = ShardActionGrant{Operation: operation, PlanDigest: planDigest, Target: target}
	data, _ := NewDynamicSplitData(1)
	retirer, err := NewLocalTerminalRetirer(binder, grants, data)
	if err != nil {
		t.Fatal(err)
	}
	retirement := TerminalRetirement{
		Operation: operation, PlanDigest: planDigest, CatalogGeneration: 9,
		CatalogDigest: catalogDigest, Proof: proof,
	}
	forged := retirement
	forged.CatalogGeneration++
	if err = retirer.RetireTerminal(forged); err == nil {
		t.Fatal("mismatched catalog generation retired a live admission")
	}
	if len(grants.grants) != 1 {
		t.Fatal("capability was revoked before exact admission validation")
	}
	if _, err = lease.PinnedStore(); err != nil {
		t.Fatalf("lease was released before exact admission validation: %v", err)
	}
	if err = retirer.RetireTerminal(retirement); err != nil {
		t.Fatal(err)
	}
	if len(grants.grants) != 0 || len(binder.active) != 0 {
		t.Fatalf("grants=%d admissions=%d", len(grants.grants), len(binder.active))
	}
	if _, err = registry.Acquire(operation); !errors.Is(err, ErrRuntimeTerminal) {
		t.Fatalf("collected operation remained acquirable: %v", err)
	}
	if err = retirer.RetireTerminal(retirement); err != nil {
		t.Fatalf("terminal retirement replay: %v", err)
	}
}
