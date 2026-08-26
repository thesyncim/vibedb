package splitcontroller

import (
	"context"
	"crypto/sha256"
	"errors"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/rangesplit"
)

type ClusterCatalogDrainCertifier interface {
	CertifyClusterCatalogDrain(
		context.Context, gateway.ClusterCatalogDrainRequest,
	) (gateway.ClusterCatalogDrainCertificate, error)
}

// ClusterPlanCatalogDrainAuthority adapts the gateway roster's authenticated
// drain protocol to split reconciliation. The complete certificate, rather
// than a process-local boolean, crosses the gateway-to-shard authority boundary.
type ClusterPlanCatalogDrainAuthority struct{ Certifier ClusterCatalogDrainCertifier }

func (authority ClusterPlanCatalogDrainAuthority) ObservePlanCatalogDrain(
	ctx context.Context, request PlanCatalogDrainRequest,
) (PlanCatalogDrainProof, error) {
	if authority.Certifier == nil || ctx == nil || request.RequestDigest == ([sha256.Size]byte{}) {
		return PlanCatalogDrainProof{}, ErrPlanObservation
	}
	clusterRequest := clusterPlanCatalogDrainRequest(request)
	certificate, err := authority.Certifier.CertifyClusterCatalogDrain(ctx, clusterRequest)
	if err != nil || !certificate.ValidFor(clusterRequest) {
		return PlanCatalogDrainProof{}, errors.Join(ErrPlanObservation, err)
	}
	return PlanCatalogDrainProof{RequestDigest: request.RequestDigest, Certificate: certificate}, nil
}

func validCatalogDrainCertificate(
	plan *Plan, catalog *gateway.Snapshot, certificate gateway.ClusterCatalogDrainCertificate,
) bool {
	if plan == nil || catalog == nil || catalog.Generation() != plan.next {
		return false
	}
	digest, err := gateway.CatalogSnapshotDigest(catalog)
	if err != nil {
		return false
	}
	request := planCatalogDrainRequest(plan, digest)
	return certificate.ValidFor(clusterPlanCatalogDrainRequest(request))
}

func retainedPruneCertificateBinding(
	plan *Plan, catalog *gateway.Snapshot, cutover rangesplit.CutoverCertificate,
) (gateway.RetainedPruneCertificateBinding, error) {
	if plan == nil || catalog == nil || catalog.Generation() != plan.next ||
		plan.partitioner == nil || plan.partitioner.VerifyCutoverCertificate(cutover) != nil {
		return gateway.RetainedPruneCertificateBinding{}, ErrTopologyConflict
	}
	manifest, ok := catalog.Manifest(plan.source.Distribution)
	if !ok || plan.partitioner.ValidateRetainedPruneAuthority(manifest, plan.next, cutover) != nil {
		return gateway.RetainedPruneCertificateBinding{}, ErrTopologyConflict
	}
	manifestDigest, err := gateway.DistributionManifestDigest(manifest)
	if err != nil {
		return gateway.RetainedPruneCertificateBinding{}, errors.Join(ErrTopologyConflict, err)
	}
	retained := plan.children[plan.retained].Range
	return gateway.RetainedPruneCertificateBinding{
		Generation: plan.next, Operation: [sha256.Size]byte(plan.operation),
		PlanDigest: plan.partitioner.Digest(), CutoverDigest: cutover.Digest(),
		TargetManifestDigest: manifestDigest, RetainedRange: retained,
		RetainedRangeLineage: gateway.RetainedRangeLineageDigest(manifestDigest, retained),
	}, nil
}

func deriveRetainedPruneCertificate(
	plan *Plan, catalog *gateway.Snapshot, cutover rangesplit.CutoverCertificate,
	drain gateway.ClusterCatalogDrainCertificate,
) (gateway.RetainedPruneCertificate, error) {
	if !validCatalogDrainCertificate(plan, catalog, drain) {
		return gateway.RetainedPruneCertificate{}, ErrTopologyConflict
	}
	binding, err := retainedPruneCertificateBinding(plan, catalog, cutover)
	if err != nil {
		return gateway.RetainedPruneCertificate{}, err
	}
	certificate, err := gateway.NewRetainedPruneCertificate(binding, drain)
	if err != nil {
		return gateway.RetainedPruneCertificate{}, errors.Join(ErrTopologyConflict, err)
	}
	return certificate, nil
}

func validRetainedPruneCertificate(
	plan *Plan, catalog *gateway.Snapshot, cutover rangesplit.CutoverCertificate,
	certificate gateway.RetainedPruneCertificate,
) bool {
	binding, err := retainedPruneCertificateBinding(plan, catalog, cutover)
	if err != nil || !certificate.ValidFor(binding) ||
		!validCatalogDrainCertificate(plan, catalog, certificate.CatalogDrain()) {
		return false
	}
	raw, err := gateway.AppendRetainedPruneCertificate(nil, certificate)
	if err != nil {
		return false
	}
	opened, err := gateway.OpenRetainedPruneCertificate(raw)
	return err == nil && opened.ValidFor(binding) && opened.Digest() == certificate.Digest()
}
