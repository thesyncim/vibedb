package splitcontroller

import (
	"context"
	"os"
	"testing"
)

func TestLocalAdmittedGrantFactoryBindsExactPreparedChildLease(t *testing.T) {
	plan, catalog, target, _ := testPlan(t)
	admission, err := NewPlanAdmission(catalog, plan)
	if err != nil {
		t.Fatal(err)
	}
	replica := target.Replicas[0]
	if err = os.Mkdir(replica.RuntimeRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	registry, err := OpenRuntimeStoreRegistry(replica.RuntimeRoot, replica.CertificateDigest, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = registry.Close() })
	lease, err := registry.Acquire(plan.OperationID())
	if err != nil {
		t.Fatal(err)
	}
	executor := new(recordingShardActionExecutor)
	factory, err := NewLocalAdmittedGrantFactory(LocalAdmittedGrantFactoryOptions{
		Node: replica.Node,
		Children: func(
			_ context.Context, _ *Plan, _ PlanAdmission, child uint8,
			gotReplica ChildReplicaTarget, gotLease *RuntimeStoreLease,
		) (ShardActionExecutor, error) {
			if child != target.Child || !targetMatchesPreparedReplica(target, gotReplica) || gotLease != lease {
				t.Fatal("factory received substituted prepared child authority")
			}
			return executor, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	grants, err := factory.BuildAdmittedShardActionGrants(
		t.Context(), catalog, plan, admission, []*RuntimeStoreLease{lease},
	)
	wantTarget, targetErr := remoteActionTargetForChildReplica(plan, target.Child, replica)
	if targetErr != nil || err != nil || len(grants) != 1 || grants[0].Target != wantTarget ||
		grants[0].Actions != childSplitActionMask() || len(grants[0].Leases) != 1 ||
		grants[0].Leases[0] != lease {
		t.Fatalf("grants=%+v targetErr=%v err=%v", grants, targetErr, err)
	}
}

func TestExactAdmissionLeaseRejectsCertificateSubstitution(t *testing.T) {
	operation := OperationID{1}
	firstDigest, secondDigest := [32]byte{1}, [32]byte{2}
	first, err := OpenRuntimeStoreRegistry(t.TempDir(), firstDigest, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	second, err := OpenRuntimeStoreRegistry(t.TempDir(), secondDigest, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = first.Close(); _ = second.Close() })
	firstLease, _ := first.Acquire(operation)
	secondLease, _ := second.Acquire(operation)
	leases := []*RuntimeStoreLease{firstLease, secondLease}
	used := []bool{false, false}
	got, index, err := exactAdmissionLease(leases, used, secondDigest)
	if err != nil || got != secondLease || index != 1 {
		t.Fatalf("lease=%p index=%d err=%v", got, index, err)
	}
	if _, _, err = exactAdmissionLease(leases, used, [32]byte{3}); err == nil {
		t.Fatal("unknown certificate digest selected a lease")
	}
}
