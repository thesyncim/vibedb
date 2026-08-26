package splitcontroller

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"
)

type splitOperationTerminalAuthorityStub struct {
	operation OperationID
	digest    [sha256.Size]byte
	proof     [sha256.Size]byte
	terminal  bool
	err       error
	calls     int
}

func (authority *splitOperationTerminalAuthorityStub) CertifySplitOperationTerminal(
	_ context.Context, operation OperationID, digest [sha256.Size]byte,
) ([sha256.Size]byte, bool, error) {
	authority.calls++
	if operation != authority.operation || digest != authority.digest {
		return [sha256.Size]byte{}, false, ErrSplitOperationRetirement
	}
	return authority.proof, authority.terminal, authority.err
}

func TestTerminalSplitOperationRetirementRevokesAllLiveStateAndIsIdempotent(t *testing.T) {
	plan, catalog, _, _ := testPlan(t)
	admission, err := NewPlanAdmission(catalog, plan)
	if err != nil {
		t.Fatal(err)
	}
	operation := plan.OperationID()
	grants, err := NewDynamicShardActionGrants(1)
	if err != nil {
		t.Fatal(err)
	}
	binder, err := NewBoundPlanAdmissionBinder(&planAdmissionGrantFactoryStub{}, grants)
	if err != nil {
		t.Fatal(err)
	}
	_, manifest := runtimeStoreIdentity()
	registry, err := OpenRuntimeStoreRegistry(preparedRuntimeRoot(t), manifest, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = registry.Close() })
	lease, err := registry.Acquire(operation)
	if err != nil {
		t.Fatal(err)
	}
	binder.limit = 1
	binder.active = map[OperationID]boundPlanAdmission{
		operation: {digest: admission.PlanDigest, leases: []*RuntimeStoreLease{lease}},
	}
	grantKey := shardActionGrantKey{
		operation: operation, digest: admission.PlanDigest,
		// The exact target value is irrelevant to retirement's operation-wide scan.
		target: ShardActionTarget{},
	}
	grants.grants[grantKey] = ShardActionGrant{Operation: operation, PlanDigest: admission.PlanDigest}
	routes, err := NewDynamicShardActionRoutes(1)
	if err != nil {
		t.Fatal(err)
	}
	routeKey := admittedPlanRouteKey{operation: operation, digest: admission.PlanDigest}
	routes.plans[routeKey] = admittedPlanRoutes{}
	authority := &splitOperationTerminalAuthorityStub{
		operation: operation, digest: admission.PlanDigest,
		proof: sha256.Sum256([]byte("replicated catalog terminal proof")), terminal: true,
	}
	retirer, err := NewTerminalSplitOperationRetirer(authority, binder, grants, routes)
	if err != nil {
		t.Fatal(err)
	}
	if err = retirer.RetireTerminalOperation(t.Context(), operation, admission.PlanDigest); err != nil {
		t.Fatal(err)
	}
	if len(grants.grants) != 0 || len(routes.plans) != 0 || len(binder.active) != 0 {
		t.Fatalf("grants=%d routes=%d admissions=%d", len(grants.grants), len(routes.plans), len(binder.active))
	}
	if _, err = lease.PinnedStore(); !errors.Is(err, ErrRuntimeStore) {
		t.Fatalf("retained lease remained usable: %v", err)
	}
	if err = retirer.RetireTerminalOperation(t.Context(), operation, admission.PlanDigest); err != nil {
		t.Fatalf("idempotent retirement: %v", err)
	}
	if authority.calls != 2 {
		t.Fatalf("terminal certifications=%d", authority.calls)
	}
	// Terminal cleanup returns both bounded indexes to full capacity.
	grants.grants[grantKey] = ShardActionGrant{}
	routes.plans[routeKey] = admittedPlanRoutes{}
	if len(grants.grants) != grants.limit || len(routes.plans) != routes.limit {
		t.Fatal("retired index capacity was not reusable")
	}
}

func TestTerminalSplitOperationRetirementRequiresExternalTerminalProof(t *testing.T) {
	operation := runtimeOperation(9)
	digest := sha256.Sum256([]byte("admitted plan"))
	grants, err := NewDynamicShardActionGrants(1)
	if err != nil {
		t.Fatal(err)
	}
	key := shardActionGrantKey{
		operation: operation, digest: digest,
		target: ShardActionTarget{},
	}
	grants.grants[key] = ShardActionGrant{Operation: operation, PlanDigest: digest}
	authority := &splitOperationTerminalAuthorityStub{
		operation: operation, digest: digest,
		proof: sha256.Sum256([]byte("not authoritative yet")), terminal: false,
	}
	retirer, err := NewTerminalSplitOperationRetirer(authority, nil, grants, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = retirer.RetireTerminalOperation(t.Context(), operation, digest); !errors.Is(err, ErrSplitOperationRetirement) {
		t.Fatalf("uncertified retirement err=%v", err)
	}
	if len(grants.grants) != 1 {
		t.Fatal("capability was revoked without terminal authority")
	}
	authority.terminal = true
	authority.proof = [sha256.Size]byte{}
	if err = retirer.RetireTerminalOperation(t.Context(), operation, digest); !errors.Is(err, ErrSplitOperationRetirement) {
		t.Fatalf("zero-proof retirement err=%v", err)
	}
	if len(grants.grants) != 1 {
		t.Fatal("capability was revoked with an empty terminal proof")
	}
}
