package gateway

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/rafttransport"
)

func testRetainedPruneDrain(t testing.TB, operation [32]byte, generation uint64) ClusterCatalogDrainCertificate {
	t.Helper()
	members := []ClusterCatalogDrainMember{{Node: rafttransport.NodeID{3}, Incarnation: 9}}
	trust := rafttransport.TrustDomain{ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2}}
	request := ClusterCatalogDrainRequest{
		Operation: operation, Step: sha256.Sum256([]byte("step")),
		Generation: generation, CatalogDigest: sha256.Sum256([]byte("catalog")),
	}
	fence, err := NewClusterCatalogDrainFence(request.fenceOperation(), generation, request.CatalogDigest, trust, members)
	if err != nil {
		t.Fatal(err)
	}
	machine, err := NewClusterCatalogDrainMachine(fence)
	if err != nil {
		t.Fatal(err)
	}
	ack := ClusterCatalogDrainAck{FenceDigest: fence.Digest(), Member: members[0], CurrentGeneration: generation}
	peer := rafttransport.PeerIdentity{Node: members[0].Node, TrustDomain: trust}
	if _, err = machine.ApplyAuthenticated(peer, ack); err != nil {
		t.Fatal(err)
	}
	proof, ok := machine.Certificate()
	if !ok {
		t.Fatal("missing drain proof")
	}
	return ClusterCatalogDrainCertificate{
		Request:     request,
		FenceDigest: fence.Digest(), RosterDigest: fence.RosterDigest(), Proof: proof,
	}
}

func TestRetainedPruneCertificateCanonicalForgeryAndGenerationReplay(t *testing.T) {
	manifest := testSnapshot(t, 2).config.Manifests[0]
	manifestDigest, err := DistributionManifestDigest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	retained, _ := manifest.ShardInfo(0)
	binding := RetainedPruneCertificateBinding{
		Generation: 2, Operation: sha256.Sum256([]byte("split-operation")),
		PlanDigest: sha256.Sum256([]byte("split-plan")), CutoverDigest: sha256.Sum256([]byte("cutover")),
		TargetManifestDigest: manifestDigest, RetainedRange: retained.Range,
		RetainedRangeLineage: RetainedRangeLineageDigest(manifestDigest, retained.Range),
	}
	certificate, err := NewRetainedPruneCertificate(
		binding, testRetainedPruneDrain(t, binding.Operation, binding.Generation),
	)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := AppendRetainedPruneCertificate([]byte{7}, certificate)
	if err != nil || len(raw) != 1+RetainedPruneCertificateBytes {
		t.Fatalf("append bytes=%d err=%v", len(raw), err)
	}
	opened, err := OpenRetainedPruneCertificate(raw[1:])
	if err != nil || !opened.ValidFor(binding) || opened.Digest() != certificate.Digest() {
		t.Fatalf("opened=%+v err=%v", opened, err)
	}
	reencoded, err := AppendRetainedPruneCertificate(nil, opened)
	if err != nil || !bytes.Equal(reencoded, raw[1:]) {
		t.Fatalf("non-canonical reencode err=%v", err)
	}

	forged := bytes.Clone(raw[1:])
	forged[60] ^= 1
	if _, err = OpenRetainedPruneCertificate(forged); !errors.Is(err, ErrRetainedPruneCertificate) {
		t.Fatalf("forgery err=%v", err)
	}
	replay := binding
	replay.Generation++
	if opened.ValidFor(replay) {
		t.Fatal("certificate replayed across catalog generation")
	}
	replay = binding
	replay.RetainedRange.End = distribution.KeyspaceEnd{Max: true}
	replay.RetainedRangeLineage = RetainedRangeLineageDigest(replay.TargetManifestDigest, replay.RetainedRange)
	if opened.ValidFor(replay) {
		t.Fatal("certificate replayed across retained lineage")
	}
}
