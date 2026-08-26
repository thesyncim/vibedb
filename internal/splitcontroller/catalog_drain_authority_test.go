package splitcontroller

import (
	"context"
	"testing"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/rangesplit"
)

type localCatalogDrainCollector struct {
	holder *gateway.CatalogHolder
	member gateway.ClusterCatalogDrainMember
}

func testRetainedPruneCertificate(
	t testing.TB, plan *Plan, published *gateway.Snapshot, cutover rangesplit.CutoverCertificate,
) gateway.RetainedPruneCertificate {
	t.Helper()
	member := gateway.ClusterCatalogDrainMember{Node: rafttransport.NodeID{3}, Incarnation: 4}
	trust := rafttransport.TrustDomain{ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2}}
	coordinator, err := gateway.NewClusterCatalogDrainCoordinator(
		trust, []gateway.ClusterCatalogDrainMember{member},
		localCatalogDrainCollector{holder: gateway.NewCatalogHolder(published), member: member},
	)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := gateway.CatalogSnapshotDigest(published)
	if err != nil {
		t.Fatal(err)
	}
	drain, err := coordinator.CertifyClusterCatalogDrain(
		context.Background(), clusterPlanCatalogDrainRequest(planCatalogDrainRequest(plan, digest)),
	)
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := deriveRetainedPruneCertificate(plan, published, cutover, drain)
	if err != nil {
		t.Fatal(err)
	}
	return certificate
}

func (collector localCatalogDrainCollector) CollectClusterCatalogDrain(
	ctx context.Context,
	fence gateway.ClusterCatalogDrainFence,
	accept func(rafttransport.PeerIdentity, gateway.ClusterCatalogDrainAck) error,
) error {
	ack, err := gateway.CollectClusterCatalogDrainAck(ctx, collector.holder, fence, collector.member)
	if err != nil {
		return err
	}
	return accept(rafttransport.PeerIdentity{
		Node:        collector.member.Node,
		TrustDomain: rafttransport.TrustDomain{ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2}},
	}, ack)
}

func TestClusterPlanCatalogDrainAuthorityCarriesExternallyVerifiableCertificate(t *testing.T) {
	plan, _, _, _ := testPlan(t)
	published := plan.targetSnapshotForTest(t)
	digest, err := gateway.CatalogSnapshotDigest(published)
	if err != nil {
		t.Fatal(err)
	}
	member := gateway.ClusterCatalogDrainMember{Node: rafttransport.NodeID{3}, Incarnation: 4}
	trust := rafttransport.TrustDomain{ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2}}
	coordinator, err := gateway.NewClusterCatalogDrainCoordinator(
		trust, []gateway.ClusterCatalogDrainMember{member},
		localCatalogDrainCollector{holder: gateway.NewCatalogHolder(published), member: member},
	)
	if err != nil {
		t.Fatal(err)
	}
	request := planCatalogDrainRequest(plan, digest)
	proof, err := (ClusterPlanCatalogDrainAuthority{Certifier: coordinator}).
		ObservePlanCatalogDrain(t.Context(), request)
	if err != nil || proof.RequestDigest != request.RequestDigest ||
		!proof.Certificate.ValidFor(clusterPlanCatalogDrainRequest(request)) ||
		!validCatalogDrainCertificate(plan, published, proof.Certificate) {
		t.Fatalf("proof=%+v err=%v", proof, err)
	}
	tampered := proof.Certificate
	tampered.Request.CatalogDigest[0]++
	if validCatalogDrainCertificate(plan, published, tampered) {
		t.Fatal("tampered external drain certificate accepted")
	}
}
