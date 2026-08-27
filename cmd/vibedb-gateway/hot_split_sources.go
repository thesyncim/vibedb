package main

import (
	"bytes"
	"slices"
	"strings"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/hotshard"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibedb/store"
)

// This is a startup inventory bound, not a per-request participant limit.
// The canonical manifest also has an independent four MiB byte bound.
const maxGatewaySplitSources = 4096

type persistedGatewaySplitSource struct {
	ClusterID              [16]byte                               `json:"cluster_id"`
	ClusterIncarnation     [16]byte                               `json:"cluster_incarnation"`
	TopologyRecoveryEpoch  uint64                                 `json:"topology_recovery_epoch"`
	ShardIncarnation       [16]byte                               `json:"shard_incarnation"`
	GroupID                [16]byte                               `json:"group_id"`
	SchemaGeneration       uint64                                 `json:"schema_generation"`
	RelationManifestDigest [32]byte                               `json:"relation_manifest_digest"`
	Table                  string                                 `json:"table"`
	SQL                    sqldriver.ReplicatedShardStoreIdentity `json:"sql"`
	LocalIndexes           []persistedGatewaySplitIndex           `json:"local_indexes,omitempty"`
	Template               persistedGatewaySplitTemplate          `json:"template"`
	Replicas               []persistedGatewaySplitReplica         `json:"replicas"`
}

type persistedGatewaySplitIndex struct {
	Name  string   `json:"name"`
	Paths []string `json:"paths"`
}

type persistedGatewaySplitReplica struct {
	Node      string `json:"node"`
	ChildRoot string `json:"child_root"`
}

type gatewaySplitSource struct {
	Group                  raftmember.GroupKey
	SchemaGeneration       uint64
	RelationManifestDigest [32]byte
	LogicalSchemaDigest    replication.Digest
	Table                  string
	SQL                    sqldriver.ReplicatedShardStoreIdentity
	LocalIndexes           []store.IndexDefinition
	Placement              sqldriver.ReplicatedPlacementProfile
	Template               persistedGatewaySplitTemplate
	Replicas               [gateway.ServingReplicaCount]gatewaySplitReplica
}

type gatewaySplitReplica struct {
	Node     rafttransport.NodeID
	Root     string
	Snapshot string
}

func openGatewaySplitSources(encoded []persistedGatewaySplitSource, endpoints []gateway.ReplicatedEndpoint) ([]gatewaySplitSource, error) {
	if len(encoded) > maxGatewaySplitSources {
		return nil, errGatewayReplicaControlManifest
	}
	sources := make([]gatewaySplitSource, len(encoded))
	seen := make(map[raftmember.GroupKey]struct{}, len(encoded))
	for i, entry := range encoded {
		source := gatewaySplitSource{Group: raftmember.GroupKey{
			ClusterID: entry.ClusterID, ClusterIncarnation: entry.ClusterIncarnation,
			TopologyRecoveryEpoch: entry.TopologyRecoveryEpoch, ShardIncarnation: entry.ShardIncarnation, GroupID: entry.GroupID},
			SchemaGeneration: entry.SchemaGeneration, RelationManifestDigest: entry.RelationManifestDigest,
			Table: entry.Table, Template: entry.Template, SQL: entry.SQL}
		if entry.ClusterID == ([16]byte{}) || entry.ClusterIncarnation == ([16]byte{}) ||
			entry.TopologyRecoveryEpoch == 0 || entry.ShardIncarnation == ([16]byte{}) || entry.GroupID == ([16]byte{}) ||
			entry.SchemaGeneration == 0 || entry.RelationManifestDigest == ([32]byte{}) || entry.Table == "" ||
			!validGatewaySplitTemplate(entry.Template) || len(entry.Replicas) != gateway.ServingReplicaCount {
			return nil, errGatewayReplicaControlManifest
		}
		if len(entry.LocalIndexes) > 4096 || !gatewaySplitSourceSQLIdentityMatches(source) {
			return nil, errGatewayReplicaControlManifest
		}
		source.SQL = source.SQL.Clone()
		logical, err := sqldriver.ReplicatedRelationManifestDigest(source.SQL)
		if err != nil {
			return nil, errGatewayReplicaControlManifest
		}
		source.LogicalSchemaDigest = replication.Digest(logical)
		source.LocalIndexes = make([]store.IndexDefinition, len(entry.LocalIndexes))
		for j, index := range entry.LocalIndexes {
			source.LocalIndexes[j] = store.IndexDefinition{Name: strings.Clone(index.Name), Paths: slices.Clone(index.Paths)}
			if _, err := store.CompileExactIndex(source.LocalIndexes[j]); err != nil {
				return nil, errGatewayReplicaControlManifest
			}
		}
		if _, duplicate := seen[source.Group]; duplicate {
			return nil, errGatewayReplicaControlManifest
		}
		seen[source.Group] = struct{}{}
		for j, replica := range entry.Replicas {
			node, err := parseGatewayReplicaNode(replica.Node)
			if err != nil || !validGatewaySplitRoot(replica.ChildRoot) ||
				j > 0 && bytes.Compare(source.Replicas[j-1].Node[:], node[:]) >= 0 {
				return nil, errGatewayReplicaControlManifest
			}
			_, found := slices.BinarySearchFunc(endpoints, node, func(endpoint gateway.ReplicatedEndpoint, node rafttransport.NodeID) int {
				return bytes.Compare(endpoint.Node[:], node[:])
			})
			if !found {
				return nil, errGatewayReplicaControlManifest
			}
			source.Replicas[j] = gatewaySplitReplica{Node: node, Root: replica.ChildRoot}
		}
		sources[i] = source
	}
	return sources, nil
}

// Explicit inventories select exact group/schema authorities. No
// first-request learning, local authority synthesis, or fallback is permitted.
func gatewayHotSplitSources(manifest gatewayReplicaControlManifest, catalog *gateway.Snapshot) (map[raftmember.GroupKey]gatewaySplitSource, error) {
	if catalog == nil || len(manifest.Shards) == 0 || len(manifest.Shards) != len(manifest.SplitSnapshots) ||
		len(manifest.SplitSources) > maxGatewaySplitSources {
		return nil, hotshard.ErrInvalidPressureCut
	}
	endpoints := make(map[rafttransport.NodeID]gatewaySplitReplica, len(manifest.Shards))
	for i, endpoint := range manifest.Shards {
		if _, duplicate := endpoints[endpoint.Node]; duplicate {
			return nil, hotshard.ErrInvalidPressureCut
		}
		replica := gatewaySplitReplica{Node: endpoint.Node, Snapshot: manifest.SplitSnapshots[i]}
		endpoints[endpoint.Node] = replica
	}
	sources := manifest.SplitSources
	descriptors := catalog.ReplicatedShardDescriptors()
	profiles := catalog.ReplicatedTableProfiles()
	byGroup := make(map[raftmember.GroupKey]gateway.ReplicatedShardDescriptor, len(descriptors))
	for _, descriptor := range descriptors {
		byGroup[descriptor.Group] = descriptor
	}
	byTable := make(map[string]gateway.ReplicatedTableProfile, len(profiles))
	for _, profile := range profiles {
		byTable[profile.Table] = profile
	}
	result := make(map[raftmember.GroupKey]gatewaySplitSource, len(sources))
	// A node-local root cannot authorize two independent source groups.
	roots := make(map[gatewaySplitReplica]struct{}, len(sources)*gateway.ServingReplicaCount)
	for _, source := range sources {
		logical, err := sqldriver.ReplicatedRelationManifestDigest(source.SQL)
		if err != nil {
			return nil, hotshard.ErrInvalidPressureCut
		}
		source.LogicalSchemaDigest = replication.Digest(logical)
		descriptor, found := byGroup[source.Group]
		profile, tableFound := byTable[source.Table]
		placement, placed := catalog.Placement(source.Table)
		if !found || !tableFound || !placed || placement.Distribution != descriptor.Distribution ||
			len(placement.Columns) != 1 || placement.Columns[0] != source.Template.ShardKey ||
			!gatewaySplitSourceMatches(source, descriptor, profile) || !validGatewaySplitTemplate(source.Template) ||
			len(descriptor.Replicas) != gateway.ServingReplicaCount {
			return nil, hotshard.ErrInvalidPressureCut
		}
		if !gatewaySplitSourceSQLIdentityMatches(source) || source.SQL.Binding.Distribution != string(descriptor.Distribution) ||
			source.SQL.Binding.Shard != string(descriptor.Shard) || source.SQL.Binding.AllocationGeneration != uint64(descriptor.AllocationGeneration) ||
			source.SQL.UserPrimaryKey != profile.PrimaryKey || source.SQL.UserLimits.MaxKeyBytes != int(profile.MaxKeyBytes) ||
			source.SQL.UserLimits.MaxDocumentBytes != int(profile.MaxDocumentBytes) ||
			source.SQL.UserLimits.MaxBatchDocuments != source.Template.MaxBatchDocuments || source.SQL.UserLimits.MaxBatchBytes != source.Template.MaxBatchBytes {
			return nil, hotshard.ErrInvalidPressureCut
		}
		matchedReplica := false
		for _, replica := range descriptor.Replicas {
			if replica.Member == source.SQL.Binding.MemberID && replica.StoreID == source.SQL.Binding.StoreID {
				matchedReplica = true
			}
		}
		manifest, manifestFound := catalog.Manifest(descriptor.Distribution)
		var sourceRange distribution.KeyRange
		matchedRange := false
		if manifestFound {
			for i := 0; i < manifest.ShardCount(); i++ {
				shard, _ := manifest.ShardMetadataAt(i)
				if shard.ID == descriptor.Shard && shard.AllocationGeneration == descriptor.AllocationGeneration {
					sourceRange, matchedRange = shard.Range, true
					break
				}
			}
		}
		if !matchedReplica || !matchedRange {
			return nil, hotshard.ErrInvalidPressureCut
		}
		source.Placement = sqldriver.ReplicatedPlacementProfile{Format: source.Template.Format, ShardKey: source.Template.ShardKey,
			TupleVersion: distribution.TupleVersion(source.Template.TupleVersion), MapperVersion: distribution.MapperVersion(source.Template.MapperVersion), Range: sourceRange}
		digest, err := sqldriver.ReplicatedSchemaManifest(source.SQL, source.Placement, source.LocalIndexes)
		if err != nil || digest != source.RelationManifestDigest {
			return nil, hotshard.ErrInvalidPressureCut
		}
		source.SQL = source.SQL.Clone()
		source.LocalIndexes = cloneGatewaySplitIndexes(source.LocalIndexes)
		if _, duplicate := result[source.Group]; duplicate {
			return nil, hotshard.ErrInvalidPressureCut
		}
		for i, replica := range source.Replicas {
			for j := 0; j < i; j++ {
				if source.Replicas[j].Node == replica.Node {
					return nil, hotshard.ErrInvalidPressureCut
				}
			}
			endpoint, enrolled := endpoints[replica.Node]
			if !enrolled || !validGatewaySplitRoot(replica.Root) || !validGatewayReplicaAddress(endpoint.Snapshot) {
				return nil, hotshard.ErrInvalidPressureCut
			}
			foundNode := false
			for _, member := range descriptor.Replicas {
				if member.Node == replica.Node {
					foundNode = true
				}
			}
			if !foundNode {
				return nil, hotshard.ErrInvalidPressureCut
			}
			key := gatewaySplitReplica{Node: replica.Node, Root: replica.Root}
			if _, duplicate := roots[key]; duplicate {
				return nil, hotshard.ErrInvalidPressureCut
			}
			roots[key] = struct{}{}
			source.Replicas[i].Snapshot = endpoint.Snapshot
		}
		result[source.Group] = source
	}
	return result, nil
}

func cloneGatewaySplitIndexes(indexes []store.IndexDefinition) []store.IndexDefinition {
	owned := slices.Clone(indexes)
	for i := range owned {
		owned[i].Paths = slices.Clone(owned[i].Paths)
	}
	return owned
}

func gatewaySplitSourceSQLIdentityMatches(source gatewaySplitSource) bool {
	if _, err := sqldriver.ReplicatedRelationManifestDigest(source.SQL); err != nil {
		return false
	}
	binding := source.SQL.Binding
	return source.Group == (raftmember.GroupKey{ClusterID: binding.ClusterID, ClusterIncarnation: binding.ClusterIncarnation,
		TopologyRecoveryEpoch: binding.TopologyRecoveryEpoch, ShardIncarnation: binding.ShardIncarnation, GroupID: binding.GroupID}) &&
		source.SQL.UserTable == source.Table && source.SQL.RelationSchemaGeneration == source.SchemaGeneration
}

func gatewaySplitSourceMatches(source gatewaySplitSource, descriptor gateway.ReplicatedShardDescriptor, profile gateway.ReplicatedTableProfile) bool {
	return source.Group == descriptor.Group && source.SchemaGeneration == descriptor.Command.SchemaGeneration &&
		source.SchemaGeneration == profile.SchemaGeneration && source.RelationManifestDigest == descriptor.Command.RelationManifestDigest &&
		source.LogicalSchemaDigest == descriptor.LogicalSchemaDigest && source.LogicalSchemaDigest == profile.LogicalSchemaDigest &&
		source.Table == profile.Table && profile.Relation == 1
}
