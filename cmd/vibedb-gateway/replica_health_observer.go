package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"slices"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/rebalance"
	"github.com/thesyncim/vibedb/internal/replicacontrol"
)

type gatewayAuthenticatedHealthObserver struct {
	client gatewayReplicaObservationClient
}

type gatewayHealthObservationResult struct {
	member      uint64
	observation replicacontrol.Observation
	err         error
}

// ObserveReplicaHealth concurrently obtains a current authenticated cut from
// the serving RF3 members. The failed member may be unavailable, but a quorum
// and the certificate-term leader must answer. A successful probe contributes
// donor liveness only; it never creates or advances failure evidence.
func (observer gatewayAuthenticatedHealthObserver) ObserveReplicaHealth(
	ctx context.Context,
	catalog *gateway.Snapshot,
	certificate rebalance.FailureQuorumCertificate,
) (gatewayReplicaHealthObservation, error) {
	if ctx == nil || catalog == nil || observer.client == nil ||
		certificate.CatalogGeneration != catalog.Generation() {
		return gatewayReplicaHealthObservation{}, errGatewayReplicaHealth
	}
	var workspace [gateway.ServingReplicaCount]gateway.ReplicatedEndpoint
	route, found := catalog.ResolveReplicatedRoute(
		certificate.Distribution, certificate.Shard, workspace[:0],
	)
	if !found || route.Group != certificate.Group ||
		route.Command.ReplicaSetVersion != certificate.ReplicaSetVersion ||
		len(route.Replicas) != gateway.ServingReplicaCount {
		return gatewayReplicaHealthObservation{}, errGatewayReplicaHealth
	}
	operation := rebalance.FailureCertificateDigest(certificate)
	stepHash := sha256.New()
	_, _ = stepHash.Write([]byte("vibedb/replica-health-observation\x00"))
	_, _ = stepHash.Write(operation[:])
	var step [sha256.Size]byte
	_ = stepHash.Sum(step[:0])
	results := make(chan gatewayHealthObservationResult, len(route.Replicas))
	for _, endpoint := range route.Replicas {
		endpoint := endpoint
		go func() {
			request := replicacontrol.Request{
				Operation: operation, Step: step, Group: certificate.Group,
				TargetMember:              endpoint.Member,
				ExpectedReplicaSetVersion: certificate.ReplicaSetVersion,
			}
			cut, err := observer.client.Observe(ctx, endpoint.Node, request)
			results <- gatewayHealthObservationResult{
				member: endpoint.Member, observation: cut, err: err,
			}
		}()
	}
	var result gatewayReplicaHealthObservation
	var failures error
	successes := 0
	for range route.Replicas {
		observed := <-results
		if observed.err != nil {
			failures = errors.Join(failures, observed.err)
			continue
		}
		cut := observed.observation
		if cut.Status.MemberID != observed.member || cut.Status.Term != certificate.LeaderTerm ||
			cut.Publication.ReplicaSetVersion != certificate.ReplicaSetVersion ||
			cut.Publication.Applied < certificate.CommitIndex {
			failures = errors.Join(failures, errGatewayReplicaHealth)
			continue
		}
		successes++
		result.Healthy = append(result.Healthy, rebalance.HealthyReplica{
			Member: observed.member, LeaderTerm: cut.Status.Term,
			ReplicaSetVersion: cut.Publication.ReplicaSetVersion,
			HealthyThrough:    certificate.ConfirmedEpoch, Applied: cut.Publication.Applied,
			RecentActive: true,
		})
		if cut.Status.MemberID == cut.Status.LeaderID {
			if result.Leader.MemberID != 0 && result.Leader.MemberID != cut.Status.MemberID {
				return gatewayReplicaHealthObservation{}, errGatewayReplicaHealth
			}
			result.Leader, result.Publication = cut.Status, cut.Publication
		}
	}
	if successes < len(route.Replicas)/2+1 || result.Leader.MemberID == 0 {
		return gatewayReplicaHealthObservation{}, errors.Join(failures, errGatewayReplicaHealth)
	}
	slices.SortFunc(result.Healthy, func(left, right rebalance.HealthyReplica) int {
		switch {
		case left.Member < right.Member:
			return -1
		case left.Member > right.Member:
			return 1
		default:
			return 0
		}
	})
	return result, nil
}
