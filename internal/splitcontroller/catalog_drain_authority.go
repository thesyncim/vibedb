package splitcontroller

import (
	"context"
	"crypto/sha256"
	"errors"

	"github.com/thesyncim/vibedb/gateway"
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
