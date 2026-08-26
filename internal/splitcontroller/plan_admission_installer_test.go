package splitcontroller

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

var errPlanAdmissionBind = errors.New("injected plan admission bind failure")

type planAdmissionStoresStub struct {
	stores []*RuntimeStoreRegistry
}

func (stub planAdmissionStoresStub) ResolveLocalPlanAdmissionStores(
	context.Context, *Plan,
) ([]*RuntimeStoreRegistry, error) {
	return stub.stores, nil
}

type planAdmissionBinderStub struct {
	fail  bool
	calls int
}

func (stub *planAdmissionBinderStub) BindPlanAdmission(
	_ context.Context, _ *Plan, _ PlanAdmission, leases []*RuntimeStoreLease,
) error {
	stub.calls++
	if stub.fail {
		return errPlanAdmissionBind
	}
	for _, lease := range leases {
		if err := lease.Release(); err != nil {
			return err
		}
	}
	return nil
}

func TestPlanAdmissionInstallerPersistsAllLocalStoresBeforeBinding(t *testing.T) {
	plan, catalog, _, _ := testPlan(t)
	admission, err := NewPlanAdmission(catalog, plan)
	if err != nil {
		t.Fatal(err)
	}
	registries := make([]*RuntimeStoreRegistry, 2)
	for index := range registries {
		root := filepath.Join(t.TempDir(), "split-runtime")
		if err = os.Mkdir(root, 0o700); err != nil {
			t.Fatal(err)
		}
		registries[index], err = OpenRuntimeStoreRegistry(
			root, testManifestDigest(string(rune('a'+index))), 2, nil,
		)
		if err != nil {
			t.Fatal(err)
		}
		defer registries[index].Close()
	}
	binder := &planAdmissionBinderStub{fail: true}
	installer, err := NewPlanAdmissionInstaller(
		planAdmissionStoresStub{stores: registries}, binder, len(registries),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err = installer.Install(t.Context(), catalog, admission); !errors.Is(err, errPlanAdmissionBind) || binder.calls != 1 {
		t.Fatalf("first install calls=%d err=%v", binder.calls, err)
	}
	for _, registry := range registries {
		lease, acquireErr := registry.Acquire(plan.OperationID())
		if acquireErr != nil {
			t.Fatal(acquireErr)
		}
		state, present, loadErr := lease.Load(RuntimeStatePlanAdmission, 0)
		if releaseErr := lease.Release(); loadErr != nil || releaseErr != nil || !present || state.Revision != 1 {
			t.Fatalf("state=%+v present=%v load=%v release=%v", state, present, loadErr, releaseErr)
		}
	}
	binder.fail = false
	if err = installer.Install(t.Context(), catalog, admission); err != nil || binder.calls != 2 {
		t.Fatalf("retry calls=%d err=%v", binder.calls, err)
	}
}

func TestPlanAdmissionInstallerRejectsDuplicateRegistryBeforeMutation(t *testing.T) {
	plan, catalog, _, _ := testPlan(t)
	admission, err := NewPlanAdmission(catalog, plan)
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(t.TempDir(), "split-runtime")
	if err = os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	registry, err := OpenRuntimeStoreRegistry(root, testManifestDigest("duplicate"), 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Close()
	binder := new(planAdmissionBinderStub)
	installer, err := NewPlanAdmissionInstaller(
		planAdmissionStoresStub{stores: []*RuntimeStoreRegistry{registry, registry}}, binder, 2,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err = installer.Install(t.Context(), catalog, admission); !errors.Is(err, ErrPlanAdmission) || binder.calls != 0 {
		t.Fatalf("calls=%d err=%v", binder.calls, err)
	}
}
