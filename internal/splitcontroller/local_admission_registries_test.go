package splitcontroller

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLocalPlanAdmissionRegistriesResolveExactSourceAndPreparedChild(t *testing.T) {
	plan, _, target, _ := testPlan(t)
	sourceRoot := filepath.Join(t.TempDir(), "source-runtime")
	if err := os.Mkdir(sourceRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	source, err := OpenRuntimeStoreRegistry(sourceRoot, [32]byte{1}, 2, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = source.Close() })
	replica := target.Replicas[1]
	if err = os.Mkdir(replica.RuntimeRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	set, err := NewLocalPlanAdmissionRegistries(replica.Node, []RetainedPlanRuntimeRegistry{{
		Distribution: plan.source.Distribution, Shard: plan.source.Shard,
		Allocation: plan.source.AllocationGeneration, Registry: source,
	}}, 2, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = set.Close() })
	resolved, err := set.ResolveLocalPlanAdmissionStores(t.Context(), plan)
	if err != nil || len(resolved) != 2 || resolved[0] != source || resolved[1] == source {
		t.Fatalf("resolved=%v err=%v", resolved, err)
	}
	again, err := set.ResolveLocalPlanAdmissionStores(t.Context(), plan)
	if err != nil || len(again) != 2 || again[1] != resolved[1] {
		t.Fatalf("exact retry reopened a second child registry: resolved=%v err=%v", again, err)
	}
	wrong := replica.CertificateDigest
	wrong[0] ^= 0xff
	if _, err = set.openChild(replica.RuntimeRoot, wrong); err == nil {
		t.Fatal("accepted a different certificate digest for the same prepared root")
	}
}

func TestLocalPlanAdmissionRegistriesRejectUnrelatedNode(t *testing.T) {
	plan, _, _, _ := testPlan(t)
	sourceRoot := filepath.Join(t.TempDir(), "source-runtime")
	if err := os.Mkdir(sourceRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	source, err := OpenRuntimeStoreRegistry(sourceRoot, [32]byte{2}, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = source.Close() })
	set, err := NewLocalPlanAdmissionRegistries([16]byte{0xfe}, []RetainedPlanRuntimeRegistry{{
		Distribution: plan.source.Distribution, Shard: "other", Allocation: 1, Registry: source,
	}}, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = set.Close() })
	if _, err = set.ResolveLocalPlanAdmissionStores(t.Context(), plan); err == nil {
		t.Fatal("unrelated node accepted an admission without a local participant")
	}
}
