package splitcontroller

import (
	"context"
	"crypto/sha256"
	"errors"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
)

// PreparedPlanObservationPeerDirectory resolves both published groups and the
// complete pre-published RF3 child roster carried by an admitted PlanIntent.
// It performs no DNS inference: endpoint -> node/member comes from the exact
// immutable operation record, and the caller's stream opener pins node TLS.
type PreparedPlanObservationPeerDirectory struct {
	catalog   ControllerCatalog
	published *CatalogPlanObservationPeerDirectory
}

func NewPreparedPlanObservationPeerDirectory(
	catalog ControllerCatalog,
) (*PreparedPlanObservationPeerDirectory, error) {
	published, err := NewCatalogPlanObservationPeerDirectory(catalog)
	if err != nil {
		return nil, err
	}
	return &PreparedPlanObservationPeerDirectory{catalog: catalog, published: published}, nil
}

func (directory *PreparedPlanObservationPeerDirectory) ResolvePlanObservationPeer(
	ctx context.Context,
	request PlanObservationRequest,
	endpoint distribution.EndpointID,
) (PlanObservationPeer, error) {
	if directory == nil || directory.catalog == nil || directory.published == nil ||
		ctx == nil || endpoint == "" || !validNetworkPlanObservationRequest(request) {
		return PlanObservationPeer{}, ErrPlanObservation
	}
	if peer, err := directory.published.ResolvePlanObservationPeer(ctx, request, endpoint); err == nil {
		return peer, nil
	}
	snapshot, err := directory.catalog.Read(ctx)
	if err != nil || snapshot == nil || snapshot.Generation() != request.CatalogGeneration {
		return PlanObservationPeer{}, errors.Join(ErrPlanObservation, err)
	}
	digest, err := gateway.CatalogSnapshotDigest(snapshot)
	if err != nil || digest != request.CatalogDigest {
		return PlanObservationPeer{}, errors.Join(ErrPlanObservation, err)
	}
	record, err := directory.catalog.ReadOperation(ctx, [32]byte(request.Operation))
	if err != nil || record.Kind != gateway.ReplicatedOperationSplit ||
		record.IntentDigest != sha256.Sum256(record.Intent) {
		return PlanObservationPeer{}, errors.Join(ErrPlanObservation, err)
	}
	plan, err := OpenPlanIntent(record.Intent, snapshot)
	if err != nil || plan.OperationID() != request.Operation {
		return PlanObservationPeer{}, errors.Join(ErrPlanObservation, err)
	}
	target, ok := plan.Target(request.Child)
	if !ok || !preparedObservationTargetMatches(request, target) {
		return PlanObservationPeer{}, ErrPlanObservation
	}
	for _, replica := range target.Replicas {
		if replica.NativeEndpoint == endpoint {
			return PlanObservationPeer{Node: replica.Node, MemberID: replica.Member}, nil
		}
	}
	return PlanObservationPeer{}, ErrPlanObservation
}

func preparedObservationTargetMatches(
	request PlanObservationRequest,
	target ChildTarget,
) bool {
	binding := target.SQL.Binding
	group := raftmember.GroupKey{
		ClusterID: target.WAL.ClusterID, ClusterIncarnation: target.WAL.ClusterIncarnation,
		TopologyRecoveryEpoch: target.TopologyRecoveryEpoch,
		ShardIncarnation:      target.WAL.ShardIncarnation, GroupID: target.WAL.GroupID,
	}
	command := raftservice.CommandFence{
		ReplicaSetVersion:      target.ReplicaSetVersion,
		ActivePolicyGeneration: target.Authority.ActivePolicyGeneration,
		ProtectionEpoch:        target.Authority.ProtectionEpoch,
		OwnershipEpoch:         target.Authority.OwnershipEpoch,
		SchemaGeneration:       target.Authority.SchemaGeneration,
		RelationManifestDigest: target.RelationManifestDigest,
		RoutingVersion:         target.Authority.RoutingVersion,
		RouteGeneration:        target.Authority.RouteGeneration,
	}
	return request.Group == group && request.Command == command &&
		request.Distribution == distribution.DistributionName(binding.Distribution) &&
		request.Shard == distribution.ShardID(binding.Shard) &&
		request.Allocation == distribution.ShardAllocationGeneration(binding.AllocationGeneration) &&
		len(request.ControlEndpoints) == len(target.Replicas)
}
