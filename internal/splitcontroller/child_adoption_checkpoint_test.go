package splitcontroller

import (
	"context"
	"testing"

	"github.com/thesyncim/vibedb/internal/rangesplit"
)

type recordingChildAdoptionCheckpoint struct {
	calls int
	proof CertifiedChildAdoption
}

func (checkpoint *recordingChildAdoptionCheckpoint) CheckpointChildAdoption(_ context.Context, proof CertifiedChildAdoption, _ PreparedChildRuntime) error {
	checkpoint.calls++
	checkpoint.proof = proof
	return nil
}

// Called by the real capture->tail->source seal certificate test; no synthetic
// certificate or weakened verifier is used to exercise the checkpoint seam.
func testChildAdoptionCheckpointWithCertificate(t *testing.T, plan *Plan, certificate rangesplit.CutoverCertificate) {
	t.Helper()
	registry, err := OpenRuntimeStoreRegistry(t.TempDir(), [32]byte{1}, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Close()
	lease, err := registry.Acquire(plan.OperationID())
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	checkpoint := new(recordingChildAdoptionCheckpoint)
	target, _ := plan.Target(1)
	binding := &childAdoptionCheckpointBinding{plan: plan, digest: [32]byte{2}, child: 1, replica: target.Replicas[0], lease: lease, checkpoint: checkpoint}
	if err = binding.record(t.Context(), PreparedChildRuntime{}); err == nil || checkpoint.calls != 0 {
		t.Fatal("checkpoint preceded durable cutover proof")
	}
	store, err := lease.PinnedStore()
	if err != nil {
		t.Fatal(err)
	}
	if err = store.PersistCutoverCertificate(1, certificate); err != nil {
		t.Fatal(err)
	}
	if err = binding.record(t.Context(), PreparedChildRuntime{}); err != nil {
		t.Fatal(err)
	}
	proof := checkpoint.proof
	if checkpoint.calls != 1 || proof.OperationID() != plan.OperationID() || proof.Child() != 1 || proof.PlanDigest() != binding.digest || proof.CutoverDigest() != certificate.Digest() || proof.ReplicaTarget().CertificateDigest != target.Replicas[0].CertificateDigest {
		t.Fatal("checkpoint witness lost certified identity")
	}
}
