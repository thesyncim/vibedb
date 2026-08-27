package splitcontroller

import (
	"crypto/sha256"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rangesplit"
	"github.com/thesyncim/vibedb/internal/replication"
)

func (p *Plan) buildCertifiedReplicatedCatalogTransition(current *gateway.Snapshot, certificate rangesplit.CutoverCertificate) (*gateway.Snapshot, error) {
	if err := p.partitioner.ValidatePublicationTransition(p.sourceManifest, p.targetManifest, current.Generation(), p.next, certificate); err != nil {
		return nil, err
	}
	descriptors, err := p.projectReplicatedSplitDescriptors(current, certificate.Digest())
	if err != nil {
		return nil, err
	}
	if descriptors == nil {
		return BuildCertifiedRangeSplitTransition(current, p.targetManifest, p.next, p.partitioner, certificate)
	}
	return gateway.BuildManifestTransitionsWithReplicatedMetadata(current, []*distribution.Manifest{p.targetManifest}, p.next, descriptors)
}

// The caller must first validate the complete cutover certificate. Keeping
// this projection separate lets tests verify exact serving metadata without
// claiming that a fabricated digest is a cutover proof.
func (p *Plan) projectReplicatedSplitDescriptors(current *gateway.Snapshot, cutoverDigest [32]byte) ([]gateway.ReplicatedShardDescriptor, error) {
	if p == nil || current == nil || cutoverDigest == ([32]byte{}) {
		return nil, ErrTopologyConflict
	}
	descriptors := current.ReplicatedShardDescriptors()
	sourceIndex := -1
	for i := range descriptors {
		if descriptors[i].Distribution == p.source.Distribution && descriptors[i].Shard == p.source.Shard && descriptors[i].AllocationGeneration == p.source.AllocationGeneration {
			sourceIndex = i
			break
		}
	}
	if sourceIndex < 0 {
		return nil, nil
	}
	source := descriptors[sourceIndex]
	if len(source.Replicas) != gateway.ServingReplicaCount || len(source.RequestLedgerRanges) != 0 {
		return nil, ErrTopologyConflict
	}
	intent, err := AppendPlanIntent(nil, current, p)
	if err != nil {
		return nil, err
	}
	planDigest := sha256.Sum256(intent)
	rootGroup := source.Group
	if source.SplitOrigin != nil {
		rootGroup = source.SplitOrigin.RootGroup
	}
	retained := &descriptors[sourceIndex]
	retained.Command.OwnershipEpoch = uint64(p.children[p.retained].OwnershipEpoch)
	retained.Command.RoutingVersion, retained.Command.RouteGeneration = uint64(p.targetManifest.Version()), p.next
	for child := uint8(0); child < p.childCount; child++ {
		if child == p.retained {
			continue
		}
		target := p.targets[child]
		if len(target.Replicas) != gateway.ServingReplicaCount || target.RelationManifestDigest != source.Command.RelationManifestDigest {
			return nil, ErrTopologyConflict
		}
		group := raftmember.GroupKey{ClusterID: target.WAL.ClusterID, ClusterIncarnation: target.WAL.ClusterIncarnation,
			TopologyRecoveryEpoch: target.TopologyRecoveryEpoch, ShardIncarnation: target.WAL.ShardIncarnation, GroupID: target.WAL.GroupID}
		descriptor := gateway.ReplicatedShardDescriptor{Distribution: p.source.Distribution, Shard: p.children[child].Shard,
			Group: group, AllocationGeneration: p.children[child].AllocationGeneration,
			Command: raftservice.CommandFence{ReplicaSetVersion: target.ReplicaSetVersion,
				ActivePolicyGeneration: target.Authority.ActivePolicyGeneration, ProtectionEpoch: target.Authority.ProtectionEpoch,
				OwnershipEpoch: target.Authority.OwnershipEpoch, SchemaGeneration: target.Authority.SchemaGeneration,
				RelationManifestDigest: target.RelationManifestDigest, RoutingVersion: target.Authority.RoutingVersion, RouteGeneration: target.Authority.RouteGeneration},
			SplitOrigin: &gateway.ReplicatedSplitOrigin{RootGroup: rootGroup, ParentGroup: source.Group,
				Operation: [32]byte(p.operation), PlanDigest: planDigest, CutoverDigest: cutoverDigest,
				SchemaGeneration: source.Command.SchemaGeneration, RelationManifestDigest: source.Command.RelationManifestDigest, Child: child},
			Replicas: make([]gateway.ReplicatedReplicaDescriptor, len(target.Replicas))}
		descriptor.RangeIdentity = splitChildLogicalDigest("range", source.RangeIdentity, p.operation, cutoverDigest, child)
		descriptor.LineageDigest = splitChildLogicalDigest("lineage", source.LineageDigest, p.operation, cutoverDigest, child)
		descriptor.ForwardingRuleDigest = splitChildLogicalDigest("forwarding", source.ForwardingRuleDigest, p.operation, cutoverDigest, child)
		for i, replica := range target.Replicas {
			descriptor.Replicas[i] = gateway.ReplicatedReplicaDescriptor{Member: replica.Member, Node: replica.Node,
				StoreID: replica.StoreID, NodeIncarnation: replica.NodeIncarnation, Endpoint: replica.Endpoint,
				NativeEndpoint: replica.NativeEndpoint, ControlEndpoint: replica.ControlEndpoint}
		}
		descriptors = append(descriptors, descriptor)
	}
	return descriptors, nil
}

func splitChildLogicalDigest(domain string, parent replication.Digest, operation OperationID, certificate [32]byte, child uint8) replication.Digest {
	hash := sha256.New()
	_, _ = hash.Write([]byte("vibedb/certified-split-child/" + domain + "\x00"))
	_, _ = hash.Write(parent[:])
	_, _ = hash.Write(operation[:])
	_, _ = hash.Write(certificate[:])
	_, _ = hash.Write([]byte{child})
	var result replication.Digest
	_ = hash.Sum(result[:0])
	return result
}
