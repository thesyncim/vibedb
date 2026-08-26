package splitcontroller

import (
	"bytes"
	"errors"
	"testing"
)

func TestPlanAdmissionAuthenticatesCatalogAndPersistsCompactRestartWitness(t *testing.T) {
	plan, catalog, _, _ := testPlan(t)
	admission, err := NewPlanAdmission(catalog, plan)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := AppendPlanAdmission(nil, admission)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := OpenPlanAdmission(raw)
	if err != nil || opened.Operation != admission.Operation ||
		opened.CatalogDigest != admission.CatalogDigest ||
		opened.PlanDigest != admission.PlanDigest || !bytes.Equal(opened.Intent, admission.Intent) {
		t.Fatalf("opened=%+v err=%v", opened, err)
	}
	recovered, err := opened.Open(catalog)
	if err != nil || recovered.OperationID() != plan.OperationID() {
		t.Fatalf("plan=%v err=%v", recovered, err)
	}

	root := t.TempDir()
	manifest := testManifestDigest("plan-admission-runtime")
	store, err := OpenDurableRuntimeStore(root, plan.OperationID(), manifest)
	if err != nil {
		t.Fatal(err)
	}
	lease := &RuntimeStoreLease{store: store}
	if err = PersistPlanAdmission(lease, admission); err != nil {
		t.Fatal(err)
	}
	if err = PersistPlanAdmission(lease, admission); err != nil {
		t.Fatalf("exact retry: %v", err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = OpenDurableRuntimeStore(root, plan.OperationID(), manifest)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	state, present, err := store.Load(RuntimeStatePlanAdmission, 0)
	if err != nil || !present || state.Revision != 1 || !bytes.Equal(state.Payload, raw) {
		t.Fatalf("state=%+v present=%v err=%v", state, present, err)
	}
}

func TestPlanAdmissionRejectsSubstitutionAndNonCanonicalIntent(t *testing.T) {
	plan, catalog, _, _ := testPlan(t)
	admission, err := NewPlanAdmission(catalog, plan)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := AppendPlanAdmission(nil, admission)
	if err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func([]byte){
		func(value []byte) { value[56] ^= 1 },
		func(value []byte) { value[88] ^= 1 },
		func(value []byte) { value[len(value)-1] ^= 1 },
	} {
		corrupt := bytes.Clone(raw)
		mutate(corrupt)
		if _, openErr := OpenPlanAdmission(corrupt); !errors.Is(openErr, ErrPlanAdmission) {
			t.Fatalf("corrupt admission err=%v", openErr)
		}
	}
	wrong := admission
	wrong.CatalogDigest[0] ^= 1
	if _, openErr := wrong.Open(catalog); !errors.Is(openErr, ErrPlanAdmission) {
		t.Fatalf("wrong catalog digest err=%v", openErr)
	}
}
