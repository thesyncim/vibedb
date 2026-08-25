package gateway

import (
	"encoding/hex"
	"fmt"
	"slices"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	queryplanner "github.com/thesyncim/vibedb/planner"
)

// ServingReplicaCount is the only replicated topology served by this first
// current native vertical. Learners and joint membership need their own completed
// lifecycle before this catalog accepts any transitional shape.
const ServingReplicaCount = 3

// ReplicatedReplicaDescriptor binds one Raft member ID to the endpoint whose
// address is already authenticated by the catalog endpoint directory.
type ReplicatedReplicaDescriptor struct {
	Member          uint64
	Node            rafttransport.NodeID
	StoreID         [16]byte
	NodeIncarnation uint64
	Endpoint        distribution.EndpointID
	NativeEndpoint  distribution.EndpointID
	ControlEndpoint distribution.EndpointID
}

// ReplicatedShardDescriptor is the cold control-plane identity for one RF3
// allocation. Distribution and Shard are used only while constructing or
// persisting the catalog; serving resolves them to compact manifest ordinals.
type ReplicatedShardDescriptor struct {
	Distribution         distribution.DistributionName
	Shard                distribution.ShardID
	Group                raftmember.GroupKey
	AllocationGeneration distribution.ShardAllocationGeneration
	Command              raftservice.CommandFence
	Replicas             []ReplicatedReplicaDescriptor
}

type replicatedCatalogShard struct {
	group        raftmember.GroupKey
	allocation   distribution.ShardAllocationGeneration
	command      raftservice.CommandFence
	replicaBase  uint32
	manifest     uint32
	shard        uint32
	replicaCount uint8
}

type unresolvedReplicatedCatalogShard struct {
	descriptor ReplicatedShardDescriptor
	manifest   uint32
	shard      uint32
}

// NewSnapshotWithReplicatedMetadata constructs one immutable generation with
// optional RF3 serving coordinates. It uses the same current catalog format;
// replicated_shards is an optional field, not a second protocol generation.
func NewSnapshotWithReplicatedMetadata(
	config distribution.ClusterConfig,
	endpoints map[distribution.EndpointID]string,
	generation uint64,
	indexes []IndexDescriptor,
	statistics []queryplanner.TableStatistics,
	replicated []ReplicatedShardDescriptor,
) (*Snapshot, error) {
	snapshot, err := NewSnapshotWithPlannerMetadata(
		config, endpoints, generation, indexes, statistics,
	)
	if err != nil {
		return nil, err
	}
	if err := snapshot.attachReplicatedMetadata(replicated); err != nil {
		return nil, err
	}
	return snapshot, nil
}

func (snapshot *Snapshot) attachReplicatedMetadata(
	descriptors []ReplicatedShardDescriptor,
) error {
	if snapshot == nil || uint64(len(descriptors)) > uint64(^uint32(0)) ||
		len(descriptors) > maxCatalogBytes/ServingReplicaCount {
		return &CatalogError{Reason: "replicated shard directory exceeds its bound"}
	}
	if len(descriptors) == 0 {
		return nil
	}
	unresolved := make([]unresolvedReplicatedCatalogShard, len(descriptors))
	groups := make(map[raftmember.GroupKey]struct{}, len(descriptors))
	positions := make(map[replicatedCatalogPosition]struct{}, len(descriptors))
	for ordinal := range descriptors {
		descriptor := descriptors[ordinal]
		if !validReplicatedCatalogGroup(descriptor.Group) ||
			descriptor.Distribution == "" || descriptor.Shard == "" ||
			descriptor.AllocationGeneration == 0 || !descriptor.Command.Valid() ||
			len(descriptor.Replicas) != ServingReplicaCount {
			return &CatalogError{Reason: "replicated shard has an invalid RF3 identity"}
		}
		manifestOrdinal, manifest := snapshot.manifestOrdinal(descriptor.Distribution)
		if manifest == nil {
			return &CatalogError{Reason: fmt.Sprintf(
				"replicated shard references unknown distribution %q", descriptor.Distribution,
			)}
		}
		shardOrdinal, metadata := manifestShardOrdinal(manifest, descriptor.Shard)
		if shardOrdinal < 0 || metadata.AllocationGeneration != descriptor.AllocationGeneration ||
			metadata.LeaderCount != ServingReplicaCount ||
			descriptor.Command.OwnershipEpoch != uint64(metadata.Epoch) ||
			descriptor.Command.RoutingVersion != uint64(manifest.Version()) {
			return &CatalogError{Reason: fmt.Sprintf(
				"replicated shard %q/%q does not match one RF3 manifest allocation",
				descriptor.Distribution, descriptor.Shard,
			)}
		}
		position := replicatedCatalogPosition{
			distribution: descriptor.Distribution, shard: descriptor.Shard,
		}
		if _, duplicate := positions[position]; duplicate {
			return &CatalogError{Reason: "duplicate replicated shard position"}
		}
		positions[position] = struct{}{}
		if _, duplicate := groups[descriptor.Group]; duplicate {
			return &CatalogError{Reason: "duplicate replicated Raft group identity"}
		}
		groups[descriptor.Group] = struct{}{}
		for replicaOrdinal, replica := range descriptor.Replicas {
			manifestEndpoint, _ := manifest.ShardLeaderAt(shardOrdinal, replicaOrdinal)
			address, endpointExists := snapshot.endpoints[replica.Endpoint]
			nativeAddress, nativeExists := snapshot.endpoints[replica.NativeEndpoint]
			controlAddress, controlExists := snapshot.endpoints[replica.ControlEndpoint]
			if replica.Member == 0 || replica.Node == (rafttransport.NodeID{}) ||
				replica.StoreID == ([16]byte{}) || replica.NodeIncarnation == 0 ||
				replica.Endpoint == "" || replica.NativeEndpoint == "" || replica.ControlEndpoint == "" ||
				replica.NativeEndpoint == replica.Endpoint || replica.ControlEndpoint == replica.Endpoint ||
				replica.ControlEndpoint == replica.NativeEndpoint || !endpointExists || !nativeExists ||
				!controlExists || address == "" || nativeAddress == "" || controlAddress == "" ||
				address == nativeAddress || address == controlAddress || nativeAddress == controlAddress ||
				replica.Endpoint != manifestEndpoint {
				return &CatalogError{Reason: fmt.Sprintf(
					"replicated shard %q/%q replica %d does not match its manifest endpoint",
					descriptor.Distribution, descriptor.Shard, replicaOrdinal,
				)}
			}
			for prior := 0; prior < replicaOrdinal; prior++ {
				if descriptor.Replicas[prior].Member == replica.Member ||
					descriptor.Replicas[prior].Endpoint == replica.Endpoint ||
					descriptor.Replicas[prior].NativeEndpoint == replica.NativeEndpoint ||
					descriptor.Replicas[prior].ControlEndpoint == replica.ControlEndpoint {
					return &CatalogError{Reason: "replicated shard repeats a member or endpoint"}
				}
			}
		}
		unresolved[ordinal] = unresolvedReplicatedCatalogShard{
			descriptor: descriptor, manifest: uint32(manifestOrdinal), shard: uint32(shardOrdinal),
		}
	}
	slices.SortFunc(unresolved, func(left, right unresolvedReplicatedCatalogShard) int {
		if left.manifest != right.manifest {
			return int(left.manifest) - int(right.manifest)
		}
		return int(left.shard) - int(right.shard)
	})
	replicas := make([]ReplicatedEndpoint, 0, len(unresolved)*ServingReplicaCount)
	shards := make([]replicatedCatalogShard, len(unresolved))
	for ordinal := range unresolved {
		entry := &unresolved[ordinal]
		base := len(replicas)
		for _, replica := range entry.descriptor.Replicas {
			replicas = append(replicas, ReplicatedEndpoint{
				Member: replica.Member, Node: replica.Node, StoreID: replica.StoreID,
				NodeIncarnation: replica.NodeIncarnation,
				NativeEndpoint:  string(replica.NativeEndpoint),
				Address:         snapshot.endpoints[replica.NativeEndpoint],
				ControlEndpoint: string(replica.ControlEndpoint),
				ControlAddress:  snapshot.endpoints[replica.ControlEndpoint],
			})
		}
		shards[ordinal] = replicatedCatalogShard{
			group: entry.descriptor.Group, allocation: entry.descriptor.AllocationGeneration,
			command:     entry.descriptor.Command,
			replicaBase: uint32(base), manifest: entry.manifest, shard: entry.shard,
			replicaCount: uint8(len(entry.descriptor.Replicas)),
		}
	}
	snapshot.replicatedShards = shards
	snapshot.replicatedReplicas = replicas
	return nil
}

type replicatedCatalogPosition struct {
	distribution distribution.DistributionName
	shard        distribution.ShardID
}

func validReplicatedCatalogGroup(group raftmember.GroupKey) bool {
	return group.ClusterID != ([16]byte{}) && group.ClusterIncarnation != ([16]byte{}) &&
		group.TopologyRecoveryEpoch != 0 && group.ShardIncarnation != ([16]byte{}) &&
		group.GroupID != ([16]byte{})
}

func (snapshot *Snapshot) manifestOrdinal(
	distributionName distribution.DistributionName,
) (int, *distribution.Manifest) {
	if snapshot == nil {
		return -1, nil
	}
	for ordinal, manifest := range snapshot.config.Manifests {
		if manifest.Distribution() == distributionName {
			return ordinal, manifest
		}
	}
	return -1, nil
}

func manifestShardOrdinal(
	manifest *distribution.Manifest,
	shard distribution.ShardID,
) (int, distribution.ShardMetadata) {
	if manifest == nil {
		return -1, distribution.ShardMetadata{}
	}
	for ordinal := 0; ordinal < manifest.ShardCount(); ordinal++ {
		metadata, _ := manifest.ShardMetadataAt(ordinal)
		if metadata.ID == shard {
			return ordinal, metadata
		}
	}
	return -1, distribution.ShardMetadata{}
}

func (snapshot *Snapshot) replicatedShardAt(
	distributionName distribution.DistributionName,
	shardID distribution.ShardID,
) (replicatedCatalogShard, bool) {
	manifestOrdinal, manifest := snapshot.manifestOrdinal(distributionName)
	shardOrdinal, _ := manifestShardOrdinal(manifest, shardID)
	if manifestOrdinal < 0 || shardOrdinal < 0 {
		return replicatedCatalogShard{}, false
	}
	targetManifest, targetShard := uint32(manifestOrdinal), uint32(shardOrdinal)
	ordinal, found := slices.BinarySearchFunc(
		snapshot.replicatedShards,
		replicatedCatalogShard{manifest: targetManifest, shard: targetShard},
		func(left, right replicatedCatalogShard) int {
			if left.manifest != right.manifest {
				return int(left.manifest) - int(right.manifest)
			}
			return int(left.shard) - int(right.shard)
		},
	)
	if !found {
		return replicatedCatalogShard{}, false
	}
	return snapshot.replicatedShards[ordinal], true
}

// ResolveReplicatedRoute resolves one RF3 shard into caller-owned workspace.
// Supplying capacity for ServingReplicaCount makes the lookup allocation-free.
func (snapshot *Snapshot) ResolveReplicatedRoute(
	distributionName distribution.DistributionName,
	shardID distribution.ShardID,
	dst []ReplicatedEndpoint,
) (ReplicatedRoute, bool) {
	entry, ok := snapshot.replicatedShardAt(distributionName, shardID)
	if !ok || int(entry.replicaCount) != ServingReplicaCount ||
		int(entry.replicaBase)+int(entry.replicaCount) > len(snapshot.replicatedReplicas) {
		return ReplicatedRoute{}, false
	}
	dst = append(dst[:0], snapshot.replicatedReplicas[int(entry.replicaBase):int(entry.replicaBase)+int(entry.replicaCount)]...)
	return ReplicatedRoute{
		Group: entry.group, AllocationGeneration: uint64(entry.allocation),
		Command: entry.command, Replicas: dst,
	}, true
}

// ReplicatedMetadataBytes reports the retained compact RF3 directory and
// endpoint arena. Endpoint string bytes are owned by Snapshot.endpoints and
// are therefore not counted twice.
func (snapshot *Snapshot) ReplicatedMetadataBytes() uint64 {
	if snapshot == nil {
		return 0
	}
	return retainedReplicatedMetadataBytes(
		snapshot.replicatedShards,
		snapshot.replicatedReplicas,
	)
}

func (snapshot *Snapshot) replicatedDescriptors() []ReplicatedShardDescriptor {
	if snapshot == nil || len(snapshot.replicatedShards) == 0 {
		return nil
	}
	descriptors := make([]ReplicatedShardDescriptor, len(snapshot.replicatedShards))
	for ordinal, entry := range snapshot.replicatedShards {
		manifest := snapshot.config.Manifests[entry.manifest]
		metadata, _ := manifest.ShardMetadataAt(int(entry.shard))
		descriptor := ReplicatedShardDescriptor{
			Distribution: manifest.Distribution(), Shard: metadata.ID,
			Group: entry.group, AllocationGeneration: entry.allocation,
			Command:  entry.command,
			Replicas: make([]ReplicatedReplicaDescriptor, entry.replicaCount),
		}
		for replicaOrdinal := range descriptor.Replicas {
			endpoint, _ := manifest.ShardLeaderAt(int(entry.shard), replicaOrdinal)
			descriptor.Replicas[replicaOrdinal] = ReplicatedReplicaDescriptor{
				Member:          snapshot.replicatedReplicas[int(entry.replicaBase)+replicaOrdinal].Member,
				Node:            snapshot.replicatedReplicas[int(entry.replicaBase)+replicaOrdinal].Node,
				StoreID:         snapshot.replicatedReplicas[int(entry.replicaBase)+replicaOrdinal].StoreID,
				NodeIncarnation: snapshot.replicatedReplicas[int(entry.replicaBase)+replicaOrdinal].NodeIncarnation,
				Endpoint:        endpoint,
				NativeEndpoint: distribution.EndpointID(
					snapshot.replicatedReplicas[int(entry.replicaBase)+replicaOrdinal].NativeEndpoint,
				),
				ControlEndpoint: distribution.EndpointID(
					snapshot.replicatedReplicas[int(entry.replicaBase)+replicaOrdinal].ControlEndpoint,
				),
			}
		}
		descriptors[ordinal] = descriptor
	}
	return descriptors
}

func validateReplicatedCatalogTransition(current, next *Snapshot) error {
	if current == nil || len(current.replicatedShards) == 0 {
		return nil
	}
	for _, old := range current.replicatedShards {
		oldManifest := current.config.Manifests[old.manifest]
		oldMetadata, _ := oldManifest.ShardMetadataAt(int(old.shard))
		_, nextManifest := next.manifestOrdinal(oldManifest.Distribution())
		nextShardOrdinal, nextMetadata := manifestShardOrdinal(nextManifest, oldMetadata.ID)
		if nextShardOrdinal < 0 {
			continue
		}
		if nextMetadata.AllocationGeneration != oldMetadata.AllocationGeneration {
			continue
		}
		candidate, found := next.replicatedShardAt(oldManifest.Distribution(), oldMetadata.ID)
		if !found {
			return &CatalogError{Reason: fmt.Sprintf(
				"replicated shard %q/%q lost serving coordinates without an allocation transition",
				oldManifest.Distribution(), oldMetadata.ID,
			)}
		}
		if candidate.group != old.group {
			return &CatalogError{Reason: fmt.Sprintf(
				"replicated shard %q/%q changed Raft group within one allocation",
				oldManifest.Distribution(), oldMetadata.ID,
			)}
		}
		if replicatedCommandFenceRegresses(old.command, candidate.command) {
			return &CatalogError{Reason: fmt.Sprintf(
				"replicated shard %q/%q regressed a serving generation",
				oldManifest.Distribution(), oldMetadata.ID,
			)}
		}
		// This first RF3 vertical has no learner/joint-consensus executor. Do
		// not let a catalog reload imply that membership changed safely. A
		// later membership state machine can replace this exact equality with
		// a certified ReplicaSetVersion transition.
		if candidate.command.ReplicaSetVersion != old.command.ReplicaSetVersion ||
			!sameReplicatedCatalogRoster(current, old, next, candidate) {
			return &CatalogError{Reason: fmt.Sprintf(
				"replicated shard %q/%q changed its roster without a membership transition",
				oldManifest.Distribution(), oldMetadata.ID,
			)}
		}
	}
	return nil
}

func replicatedCommandFenceRegresses(old, next raftservice.CommandFence) bool {
	return next.ReplicaSetVersion < old.ReplicaSetVersion ||
		next.ActivePolicyGeneration < old.ActivePolicyGeneration ||
		next.ProtectionEpoch < old.ProtectionEpoch ||
		next.OwnershipEpoch < old.OwnershipEpoch ||
		next.SchemaGeneration < old.SchemaGeneration ||
		(next.SchemaGeneration == old.SchemaGeneration &&
			next.RelationManifestDigest != old.RelationManifestDigest) ||
		(next.SchemaGeneration != old.SchemaGeneration &&
			next.RelationManifestDigest == old.RelationManifestDigest) ||
		next.RoutingVersion < old.RoutingVersion ||
		next.RouteGeneration < old.RouteGeneration
}

func sameReplicatedCatalogRoster(
	oldSnapshot *Snapshot,
	old replicatedCatalogShard,
	nextSnapshot *Snapshot,
	next replicatedCatalogShard,
) bool {
	if old.replicaCount != next.replicaCount ||
		int(old.replicaBase)+int(old.replicaCount) > len(oldSnapshot.replicatedReplicas) ||
		int(next.replicaBase)+int(next.replicaCount) > len(nextSnapshot.replicatedReplicas) {
		return false
	}
	for ordinal := 0; ordinal < int(old.replicaCount); ordinal++ {
		oldReplica := oldSnapshot.replicatedReplicas[int(old.replicaBase)+ordinal]
		nextReplica := nextSnapshot.replicatedReplicas[int(next.replicaBase)+ordinal]
		if oldReplica != nextReplica {
			return false
		}
	}
	return true
}

type persistedReplicatedShard struct {
	Distribution           string                       `json:"distribution"`
	Shard                  string                       `json:"shard"`
	AllocationGeneration   uint64                       `json:"allocation_generation"`
	ClusterID              string                       `json:"cluster_id"`
	ClusterIncarnation     string                       `json:"cluster_incarnation"`
	TopologyRecoveryEpoch  uint64                       `json:"topology_recovery_epoch"`
	ShardIncarnation       string                       `json:"shard_incarnation"`
	GroupID                string                       `json:"group_id"`
	ReplicaSetVersion      uint64                       `json:"replica_set_version"`
	ActivePolicyGeneration uint64                       `json:"active_policy_generation"`
	ProtectionEpoch        uint64                       `json:"protection_epoch"`
	OwnershipEpoch         uint64                       `json:"ownership_epoch"`
	SchemaGeneration       uint64                       `json:"schema_generation"`
	RelationManifestDigest string                       `json:"relation_manifest_digest"`
	RoutingVersion         uint64                       `json:"routing_version"`
	RouteGeneration        uint64                       `json:"route_generation"`
	Replicas               []persistedReplicatedReplica `json:"replicas"`
}

type persistedReplicatedReplica struct {
	Member          uint64 `json:"member"`
	Node            string `json:"node"`
	StoreID         string `json:"store_id"`
	NodeIncarnation uint64 `json:"node_incarnation"`
	Endpoint        string `json:"endpoint"`
	NativeEndpoint  string `json:"native_endpoint"`
	ControlEndpoint string `json:"control_endpoint"`
}

func persistedReplicatedDescriptors(
	descriptors []ReplicatedShardDescriptor,
) []persistedReplicatedShard {
	if len(descriptors) == 0 {
		return nil
	}
	persisted := make([]persistedReplicatedShard, len(descriptors))
	for ordinal, descriptor := range descriptors {
		entry := persistedReplicatedShard{
			Distribution: string(descriptor.Distribution), Shard: string(descriptor.Shard),
			AllocationGeneration:   uint64(descriptor.AllocationGeneration),
			ClusterID:              hex.EncodeToString(descriptor.Group.ClusterID[:]),
			ClusterIncarnation:     hex.EncodeToString(descriptor.Group.ClusterIncarnation[:]),
			TopologyRecoveryEpoch:  descriptor.Group.TopologyRecoveryEpoch,
			ShardIncarnation:       hex.EncodeToString(descriptor.Group.ShardIncarnation[:]),
			GroupID:                hex.EncodeToString(descriptor.Group.GroupID[:]),
			ReplicaSetVersion:      descriptor.Command.ReplicaSetVersion,
			ActivePolicyGeneration: descriptor.Command.ActivePolicyGeneration,
			ProtectionEpoch:        descriptor.Command.ProtectionEpoch,
			OwnershipEpoch:         descriptor.Command.OwnershipEpoch,
			SchemaGeneration:       descriptor.Command.SchemaGeneration,
			RelationManifestDigest: hex.EncodeToString(
				descriptor.Command.RelationManifestDigest[:],
			),
			RoutingVersion:  descriptor.Command.RoutingVersion,
			RouteGeneration: descriptor.Command.RouteGeneration,
			Replicas:        make([]persistedReplicatedReplica, len(descriptor.Replicas)),
		}
		for replicaOrdinal, replica := range descriptor.Replicas {
			entry.Replicas[replicaOrdinal] = persistedReplicatedReplica{
				Member: replica.Member, Node: hex.EncodeToString(replica.Node[:]),
				StoreID:         hex.EncodeToString(replica.StoreID[:]),
				NodeIncarnation: replica.NodeIncarnation, Endpoint: string(replica.Endpoint),
				NativeEndpoint:  string(replica.NativeEndpoint),
				ControlEndpoint: string(replica.ControlEndpoint),
			}
		}
		persisted[ordinal] = entry
	}
	return persisted
}

func (pc persistedCatalog) replicatedDescriptors() ([]ReplicatedShardDescriptor, error) {
	if len(pc.ReplicatedShards) == 0 {
		return nil, nil
	}
	descriptors := make([]ReplicatedShardDescriptor, len(pc.ReplicatedShards))
	for ordinal, persisted := range pc.ReplicatedShards {
		group := raftmember.GroupKey{TopologyRecoveryEpoch: persisted.TopologyRecoveryEpoch}
		if err := decodeFixed16Hex(persisted.ClusterID, &group.ClusterID); err != nil {
			return nil, &CatalogError{Reason: "replicated shard cluster id: " + err.Error()}
		}
		if err := decodeFixed16Hex(persisted.ClusterIncarnation, &group.ClusterIncarnation); err != nil {
			return nil, &CatalogError{Reason: "replicated shard cluster incarnation: " + err.Error()}
		}
		if err := decodeFixed16Hex(persisted.ShardIncarnation, &group.ShardIncarnation); err != nil {
			return nil, &CatalogError{Reason: "replicated shard incarnation: " + err.Error()}
		}
		if err := decodeFixed16Hex(persisted.GroupID, &group.GroupID); err != nil {
			return nil, &CatalogError{Reason: "replicated shard group id: " + err.Error()}
		}
		var relationManifestDigest [32]byte
		if err := decodeFixed32Hex(
			persisted.RelationManifestDigest,
			&relationManifestDigest,
		); err != nil {
			return nil, &CatalogError{
				Reason: "replicated shard relation manifest digest: " + err.Error(),
			}
		}
		descriptor := ReplicatedShardDescriptor{
			Distribution: distribution.DistributionName(persisted.Distribution),
			Shard:        distribution.ShardID(persisted.Shard), Group: group,
			AllocationGeneration: distribution.ShardAllocationGeneration(persisted.AllocationGeneration),
			Command: raftservice.CommandFence{
				ReplicaSetVersion:      persisted.ReplicaSetVersion,
				ActivePolicyGeneration: persisted.ActivePolicyGeneration,
				ProtectionEpoch:        persisted.ProtectionEpoch,
				OwnershipEpoch:         persisted.OwnershipEpoch,
				SchemaGeneration:       persisted.SchemaGeneration,
				RelationManifestDigest: relationManifestDigest,
				RoutingVersion:         persisted.RoutingVersion,
				RouteGeneration:        persisted.RouteGeneration,
			},
			Replicas: make([]ReplicatedReplicaDescriptor, len(persisted.Replicas)),
		}
		for replicaOrdinal, replica := range persisted.Replicas {
			var node rafttransport.NodeID
			var storeID [16]byte
			if err := decodeFixed16Hex(replica.Node, (*[16]byte)(&node)); err != nil {
				return nil, &CatalogError{Reason: "replicated replica node: " + err.Error()}
			}
			if err := decodeFixed16Hex(replica.StoreID, &storeID); err != nil {
				return nil, &CatalogError{Reason: "replicated replica store: " + err.Error()}
			}
			descriptor.Replicas[replicaOrdinal] = ReplicatedReplicaDescriptor{
				Member: replica.Member, Node: node, StoreID: storeID,
				NodeIncarnation: replica.NodeIncarnation,
				Endpoint:        distribution.EndpointID(replica.Endpoint),
				NativeEndpoint:  distribution.EndpointID(replica.NativeEndpoint),
				ControlEndpoint: distribution.EndpointID(replica.ControlEndpoint),
			}
		}
		descriptors[ordinal] = descriptor
	}
	return descriptors, nil
}

func decodeFixed16Hex(encoded string, destination *[16]byte) error {
	if destination == nil || len(encoded) != hex.EncodedLen(len(destination)) {
		return fmt.Errorf("must contain exactly %d hexadecimal bytes", len(destination))
	}
	decoded, err := hex.Decode(destination[:], []byte(encoded))
	if err != nil || decoded != len(destination) {
		return fmt.Errorf("must contain exactly %d hexadecimal bytes", len(destination))
	}
	return nil
}

func decodeFixed32Hex(encoded string, destination *[32]byte) error {
	if destination == nil || len(encoded) != hex.EncodedLen(len(destination)) {
		return fmt.Errorf("must contain exactly %d hexadecimal bytes", len(destination))
	}
	decoded, err := hex.Decode(destination[:], []byte(encoded))
	if err != nil || decoded != len(destination) {
		return fmt.Errorf("must contain exactly %d hexadecimal bytes", len(destination))
	}
	return nil
}
