package splitcontroller

import (
	"testing"

	"github.com/thesyncim/vibedb/internal/rangesplit"
)

func TestLazyChildAdmissionRestoresDurableTailEndpoint(t *testing.T) {
	plan, artifacts, _, before, _ := testTailStreamTransportFixture(t)
	_, manifest := runtimeStoreIdentity()
	registry, err := OpenRuntimeStoreRegistry(preparedRuntimeRoot(t), manifest, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = registry.Close() })
	lease, err := registry.Acquire(plan.OperationID())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lease.Release() })
	raw, err := rangesplit.AppendChildArtifactSet(nil, artifacts)
	if err != nil {
		t.Fatal(err)
	}
	if err = lease.Persist(RuntimeStateArtifacts, 0, 1, raw); err != nil {
		t.Fatal(err)
	}
	raw, err = rangesplit.AppendChildStageCursor(nil, &before)
	if err != nil {
		t.Fatal(err)
	}
	if err = lease.Persist(RuntimeStateStage, 1, 1, raw); err != nil {
		t.Fatal(err)
	}
	data, err := NewDynamicSplitData(1)
	if err != nil {
		t.Fatal(err)
	}
	executor := &LazyReplicatedChildExecutor{options: LazyReplicatedChildExecutorOptions{
		Plan: plan, PlanDigest: [32]byte{1}, Child: 1, Lease: lease, Data: data,
	}}
	if err = executor.PublishTailTarget(); err != nil {
		t.Fatal(err)
	}
	resolved, err := data.ResolveSplitTail(t.Context(), plan.OperationID(), 1)
	if err != nil || resolved.Target != executor {
		t.Fatalf("reopened admission did not restore endpoint: %v", err)
	}
	cursor, found, err := resolved.Target.ObserveTail(t.Context())
	if err != nil || !found || cursor != before || executor.stage != nil {
		t.Fatalf("durable cursor was not observable before lazy SQL open: found=%t err=%v", found, err)
	}
}
