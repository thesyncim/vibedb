package gateway

import (
	"encoding/hex"
	"fmt"
	"slices"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/membershipgrant"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	queryplanner "github.com/thesyncim/vibedb/planner"
)

// ServingReplicaCount is the only public data topology served by this native
// vertical. A single enrolled membership target is retained separately and
// never widens the serving route.
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
	// RangeIdentity is the immutable logical range named by durable requests.
	// LineageDigest and ForwardingRuleDigest authenticate how that logical
	// range may be resolved after split or movement without allowing a retry to
	// substitute an unrelated allocation.
	RangeIdentity        replication.Digest
	LineageDigest        replication.Digest
	ForwardingRuleDigest replication.Digest
	// RequestLedgerRanges explicitly assigns authenticated request-home ranges
	// to this shard. Empty means this shard carries no ledger range; identities
	// and boundaries are provisioned control-plane input, never synthesized.
	RequestLedgerRanges []DurableRequestLedgerRangeDescriptor
	Replicas            []ReplicatedReplicaDescriptor
	// EnrolledTarget is the one authenticated replacement endpoint that may
	// participate in membership control. It is never included in the public
	// data route until a later catalog cut makes it one of Replicas.
	EnrolledTarget *ReplicatedReplicaDescriptor
}

type replicatedCatalogShard struct {
	group             raftmember.GroupKey
	allocation        distribution.ShardAllocationGeneration
	command           raftservice.CommandFence
	rangeIdentity     replication.Digest
	lineageDigest     replication.Digest
	forwardingDigest  replication.Digest
	replicaBase       uint32
	manifest          uint32
	shard             uint32
	replicaCount      uint8
	hasEnrolledTarget bool
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
	return NewSnapshotWithReplicatedTableMetadata(
		config, endpoints, generation, indexes, statistics, replicated, nil,
	)
}

// NewSnapshotWithReplicatedTableMetadata constructs one immutable catalog
// generation with exact RF3 coordinates and optional base-table profiles.
// There is one current grammar: replicated_tables is an optional field in the
// same catalog document, not a parallel format or compatibility mode.
func NewSnapshotWithReplicatedTableMetadata(
	config distribution.ClusterConfig,
	endpoints map[distribution.EndpointID]string,
	generation uint64,
	indexes []IndexDescriptor,
	statistics []queryplanner.TableStatistics,
	replicated []ReplicatedShardDescriptor,
	tables []ReplicatedTableProfile,
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
	if err := snapshot.attachReplicatedTableProfiles(tables); err != nil {
		return nil, err
	}
	if err := snapshot.attachDurableRequestLedgerRangesFromDescriptors(replicated); err != nil {
		return nil, err
	}
	return snapshot, nil
}

func (snapshot *Snapshot) attachReplicatedMetadata(
	descriptors []ReplicatedShardDescriptor,
) error {
	if snapshot == nil || uint64(len(descriptors)) > uint64(^uint32(0)) ||
		len(descriptors) > maxCatalogBytes/(ServingReplicaCount+1) {
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
			descriptor.RangeIdentity == (replication.Digest{}) ||
			descriptor.LineageDigest == (replication.Digest{}) ||
			descriptor.ForwardingRuleDigest == (replication.Digest{}) ||
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
		if target := descriptor.EnrolledTarget; target != nil {
			address, endpointExists := snapshot.endpoints[target.Endpoint]
			nativeAddress, nativeExists := snapshot.endpoints[target.NativeEndpoint]
			controlAddress, controlExists := snapshot.endpoints[target.ControlEndpoint]
			if target.Member == 0 || target.Node == (rafttransport.NodeID{}) ||
				target.StoreID == ([16]byte{}) || target.NodeIncarnation == 0 ||
				target.Endpoint == "" || target.NativeEndpoint == "" || target.ControlEndpoint == "" ||
				target.NativeEndpoint == target.Endpoint || target.ControlEndpoint == target.Endpoint ||
				target.ControlEndpoint == target.NativeEndpoint || !endpointExists || !nativeExists ||
				!controlExists || address == "" || nativeAddress == "" || controlAddress == "" ||
				address == nativeAddress || address == controlAddress || nativeAddress == controlAddress {
				return &CatalogError{Reason: fmt.Sprintf(
					"replicated shard %q/%q has an invalid enrolled membership target",
					descriptor.Distribution, descriptor.Shard,
				)}
			}
			for _, replica := range descriptor.Replicas {
				if replica.Member == target.Member || replica.Node == target.Node ||
					replica.StoreID == target.StoreID || replica.Endpoint == target.Endpoint ||
					replica.NativeEndpoint == target.NativeEndpoint ||
					replica.ControlEndpoint == target.ControlEndpoint {
					return &CatalogError{Reason: "enrolled membership target repeats a serving identity or endpoint"}
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
	replicas := make([]ReplicatedEndpoint, 0, len(unresolved)*(ServingReplicaCount+1))
	shards := make([]replicatedCatalogShard, len(unresolved))
	for ordinal := range unresolved {
		entry := &unresolved[ordinal]
		base := len(replicas)
		for _, replica := range entry.descriptor.Replicas {
			replicas = append(replicas, ReplicatedEndpoint{
				Member: replica.Member, Node: replica.Node, StoreID: replica.StoreID,
				NodeIncarnation: replica.NodeIncarnation,
				Endpoint:        string(replica.Endpoint),
				DataAddress:     snapshot.endpoints[replica.Endpoint],
				NativeEndpoint:  string(replica.NativeEndpoint),
				Address:         snapshot.endpoints[replica.NativeEndpoint],
				ControlEndpoint: string(replica.ControlEndpoint),
				ControlAddress:  snapshot.endpoints[replica.ControlEndpoint],
			})
		}
		if target := entry.descriptor.EnrolledTarget; target != nil {
			replicas = append(replicas, ReplicatedEndpoint{
				Member: target.Member, Node: target.Node, StoreID: target.StoreID,
				NodeIncarnation: target.NodeIncarnation,
				Endpoint:        string(target.Endpoint),
				DataAddress:     snapshot.endpoints[target.Endpoint],
				NativeEndpoint:  string(target.NativeEndpoint),
				Address:         snapshot.endpoints[target.NativeEndpoint],
				ControlEndpoint: string(target.ControlEndpoint),
				ControlAddress:  snapshot.endpoints[target.ControlEndpoint],
			})
		}
		shards[ordinal] = replicatedCatalogShard{
			group: entry.descriptor.Group, allocation: entry.descriptor.AllocationGeneration,
			command:          entry.descriptor.Command,
			rangeIdentity:    entry.descriptor.RangeIdentity,
			lineageDigest:    entry.descriptor.LineageDigest,
			forwardingDigest: entry.descriptor.ForwardingRuleDigest,
			replicaBase:      uint32(base), manifest: entry.manifest, shard: entry.shard,
			replicaCount:      uint8(len(entry.descriptor.Replicas)),
			hasEnrolledTarget: entry.descriptor.EnrolledTarget != nil,
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
	dst = dst[:len(dst):len(dst)]
	return ReplicatedRoute{
		Distribution: distributionName, Shard: shardID,
		Group: entry.group, AllocationGeneration: uint64(entry.allocation),
		Command: entry.command, RangeIdentity: entry.rangeIdentity,
		LineageDigest: entry.lineageDigest, ForwardingRuleDigest: entry.forwardingDigest,
		Replicas: dst,
	}, true
}

// ReplicatedRouteCount reports the complete catalog RF3 inventory, including
// reserved control relations that need not appear in a user routing manifest.
func (snapshot *Snapshot) ReplicatedRouteCount() int {
	if snapshot == nil {
		return 0
	}
	return len(snapshot.replicatedShards)
}

// ReplicatedRouteAt returns route i into caller-owned replica workspace. It is
// the bounded inventory seam used by cluster backup; callers must consume
// every ordinal rather than selecting user-visible tables.
func (snapshot *Snapshot) ReplicatedRouteAt(index int, dst []ReplicatedEndpoint) (ReplicatedRoute, bool) {
	if snapshot == nil || index < 0 || index >= len(snapshot.replicatedShards) {
		return ReplicatedRoute{}, false
	}
	entry := snapshot.replicatedShards[index]
	if int(entry.manifest) >= len(snapshot.config.Manifests) ||
		int(entry.replicaCount) != ServingReplicaCount ||
		int(entry.replicaBase)+int(entry.replicaCount) > len(snapshot.replicatedReplicas) {
		return ReplicatedRoute{}, false
	}
	manifest := snapshot.config.Manifests[entry.manifest]
	metadata, ok := manifest.ShardMetadataAt(int(entry.shard))
	if !ok {
		return ReplicatedRoute{}, false
	}
	dst = append(dst[:0], snapshot.replicatedReplicas[int(entry.replicaBase):int(entry.replicaBase)+int(entry.replicaCount)]...)
	dst = dst[:len(dst):len(dst)]
	return ReplicatedRoute{Distribution: manifest.Distribution(), Shard: metadata.ID, Group: entry.group,
		AllocationGeneration: uint64(entry.allocation), Command: entry.command,
		RangeIdentity: entry.rangeIdentity, LineageDigest: entry.lineageDigest,
		ForwardingRuleDigest: entry.forwardingDigest, Replicas: dst}, true
}

// ResolveReplicatedMembershipRoute resolves the active serving RF3 together
// with its optional enrolled replacement. The replacement is kept outside the
// serving slice, so ordinary reads, writes, and transactions cannot select it.
// Supplying capacity for ServingReplicaCount keeps a route without a target
// allocation-free; the target itself is returned by value.
func (snapshot *Snapshot) ResolveReplicatedMembershipRoute(
	distributionName distribution.DistributionName,
	shardID distribution.ShardID,
	dst []ReplicatedEndpoint,
) (ReplicatedMembershipRoute, bool) {
	entry, ok := snapshot.replicatedShardAt(distributionName, shardID)
	if !ok || int(entry.replicaCount) != ServingReplicaCount {
		return ReplicatedMembershipRoute{}, false
	}
	end := int(entry.replicaBase) + int(entry.replicaCount)
	targets := 0
	if entry.hasEnrolledTarget {
		targets = 1
	}
	if end+targets > len(snapshot.replicatedReplicas) {
		return ReplicatedMembershipRoute{}, false
	}
	dst = append(dst[:0], snapshot.replicatedReplicas[int(entry.replicaBase):end]...)
	dst = dst[:len(dst):len(dst)]
	result := ReplicatedMembershipRoute{Serving: ReplicatedRoute{
		Distribution: distributionName, Shard: shardID,
		Group: entry.group, AllocationGeneration: uint64(entry.allocation),
		Command: entry.command, RangeIdentity: entry.rangeIdentity,
		LineageDigest: entry.lineageDigest, ForwardingRuleDigest: entry.forwardingDigest,
		Replicas: dst,
	}}
	if targets != 0 {
		result.EnrolledTarget = snapshot.replicatedReplicas[end]
		result.HasEnrolledTarget = true
	}
	if !validReplicatedMembershipRoute(result) {
		return ReplicatedMembershipRoute{}, false
	}
	return result, true
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
		snapshot.replicatedTables,
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
			Command: entry.command, RangeIdentity: entry.rangeIdentity,
			LineageDigest: entry.lineageDigest, ForwardingRuleDigest: entry.forwardingDigest,
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
		if entry.hasEnrolledTarget {
			targetOrdinal := int(entry.replicaBase) + int(entry.replicaCount)
			if targetOrdinal >= len(snapshot.replicatedReplicas) {
				return nil
			}
			target := snapshot.replicatedReplicas[targetOrdinal]
			descriptor.EnrolledTarget = &ReplicatedReplicaDescriptor{
				Member: target.Member, Node: target.Node, StoreID: target.StoreID,
				NodeIncarnation: target.NodeIncarnation,
				Endpoint:        distribution.EndpointID(target.Endpoint),
				NativeEndpoint:  distribution.EndpointID(target.NativeEndpoint),
				ControlEndpoint: distribution.EndpointID(target.ControlEndpoint),
			}
		}
		descriptors[ordinal] = descriptor
	}
	if snapshot.durableRequestLedgerTopology != nil {
		for _, value := range snapshot.durableRequestLedgerTopology.Ranges {
			for ordinal := range descriptors {
				if descriptors[ordinal].Distribution == value.Route.Distribution &&
					descriptors[ordinal].Shard == value.Route.Shard {
					descriptors[ordinal].RequestLedgerRanges = append(
						descriptors[ordinal].RequestLedgerRanges,
						DurableRequestLedgerRangeDescriptor{
							Start: value.Start, End: value.End, Identity: value.Identity,
						},
					)
					break
				}
			}
		}
	}
	return descriptors
}

// ReplicatedShardDescriptors returns a detached cold control-plane directory.
// It is intended for startup/configuration certification, never hot routing.
func (snapshot *Snapshot) ReplicatedShardDescriptors() []ReplicatedShardDescriptor {
	if snapshot == nil {
		return nil
	}
	return snapshot.replicatedDescriptors()
}

func validateReplicatedCatalogTransition(current, next *Snapshot) error {
	if err := validateReplicatedTableTransition(current, next); err != nil {
		return err
	}
	if err := validateDurableRequestLedgerCatalogTransition(current, next); err != nil {
		return err
	}
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
		if candidate.rangeIdentity != old.rangeIdentity ||
			candidate.lineageDigest != old.lineageDigest ||
			candidate.forwardingDigest != old.forwardingDigest {
			return &CatalogError{Reason: fmt.Sprintf(
				"replicated shard %q/%q changed logical range authority within one allocation",
				oldManifest.Distribution(), oldMetadata.ID,
			)}
		}
		if replicatedCommandFenceRegresses(old.command, candidate.command) {
			return &CatalogError{Reason: fmt.Sprintf(
				"replicated shard %q/%q regressed a serving generation",
				oldManifest.Distribution(), oldMetadata.ID,
			)}
		}
		// Enrolling a non-serving target does not authorize a serving-roster
		// change. Do not let a catalog reload imply that membership completed:
		// an installed ReplicaSetVersion transition must certify that cut.
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

func advanceCatalogStateReplicaReplacement(
	current, next *Snapshot, grant membershipgrant.Grant,
) (*Snapshot, error) {
	if current == nil || next == nil || !grant.Valid() ||
		grant.CatalogGeneration != current.Generation() ||
		next.Generation() != current.Generation()+1 {
		return nil, &CatalogError{Reason: "invalid certified replica replacement cut"}
	}
	if err := validateRoutingTransition(current, next); err != nil {
		return nil, err
	}
	if err := validateCertifiedReplicaReplacement(current, next, grant); err != nil {
		return nil, err
	}
	indexHighWater, err := advanceIndexIDHighWater(current, next)
	if err != nil {
		return nil, err
	}
	shardHighWaters, err := advanceShardGenerationHighWaters(current, next)
	if err != nil {
		return nil, err
	}
	return snapshotWithCatalogLineage(next, indexHighWater, shardHighWaters), nil
}

func validateCertifiedReplicaReplacement(
	current, next *Snapshot, grant membershipgrant.Grant,
) error {
	if err := validateReplicatedTableTransition(current, next); err != nil {
		return err
	}
	if current == nil || next == nil || !grant.Valid() ||
		len(current.replicatedShards) != len(next.replicatedShards) ||
		!replicatedCatalogCertifiesInitialGrant(current, grant) {
		return &CatalogError{Reason: "membership grant does not certify the current RF3 catalog"}
	}
	replaced := false
	for _, old := range current.replicatedShards {
		oldManifest := current.config.Manifests[old.manifest]
		oldMetadata, _ := oldManifest.ShardMetadataAt(int(old.shard))
		candidate, found := next.replicatedShardAt(oldManifest.Distribution(), oldMetadata.ID)
		if !found || candidate.group != old.group || candidate.allocation != old.allocation {
			return &CatalogError{Reason: "replica replacement changed allocation identity"}
		}
		if old.group != grant.Group {
			if candidate.command != old.command ||
				!sameReplicatedCatalogRoster(current, old, next, candidate) {
				return &CatalogError{Reason: "replica replacement changed an unrelated RF3 group"}
			}
			continue
		}
		if replaced || !exactCertifiedReplicaReplacement(current, old, next, candidate, grant) {
			return &CatalogError{Reason: "replica replacement final roster is not exact"}
		}
		replaced = true
	}
	if !replaced {
		return &CatalogError{Reason: "replica replacement group is absent"}
	}
	return nil
}

func exactCertifiedReplicaReplacement(
	current *Snapshot,
	old replicatedCatalogShard,
	next *Snapshot,
	candidate replicatedCatalogShard,
	grant membershipgrant.Grant,
) bool {
	if old.replicaCount != ServingReplicaCount || candidate.replicaCount != ServingReplicaCount ||
		old.command.ReplicaSetVersion != grant.InitialReplicaSetVersion ||
		candidate.command.ReplicaSetVersion <= old.command.ReplicaSetVersion ||
		candidate.command.ActivePolicyGeneration != old.command.ActivePolicyGeneration ||
		candidate.command.ProtectionEpoch != old.command.ProtectionEpoch ||
		candidate.command.OwnershipEpoch != old.command.OwnershipEpoch+1 ||
		candidate.command.SchemaGeneration != old.command.SchemaGeneration ||
		candidate.command.RelationManifestDigest != old.command.RelationManifestDigest ||
		candidate.command.RoutingVersion != old.command.RoutingVersion+1 ||
		candidate.command.RouteGeneration != old.command.RouteGeneration+1 ||
		int(old.replicaBase)+ServingReplicaCount > len(current.replicatedReplicas) ||
		int(candidate.replicaBase)+ServingReplicaCount > len(next.replicatedReplicas) {
		return false
	}
	changes := 0
	changedOrdinal := -1
	for ordinal := 0; ordinal < ServingReplicaCount; ordinal++ {
		before := current.replicatedReplicas[int(old.replicaBase)+ordinal]
		after := next.replicatedReplicas[int(candidate.replicaBase)+ordinal]
		if before == after {
			continue
		}
		if before.Member != grant.SourceMember || after.Member != grant.TargetMember ||
			[16]byte(after.Node) != grant.TargetNode {
			return false
		}
		changes++
		changedOrdinal = ordinal
	}
	if changes != 1 {
		return false
	}
	oldManifest := current.config.Manifests[old.manifest]
	nextManifest := next.config.Manifests[candidate.manifest]
	oldMetadata, ok := oldManifest.ShardMetadataAt(int(old.shard))
	if !ok || oldMetadata.Epoch == ^distribution.OwnershipEpoch(0) ||
		oldManifest.Version() == ^distribution.RoutingVersion(0) {
		return false
	}
	targetEndpoint, ok := nextManifest.ShardLeaderAt(int(candidate.shard), changedOrdinal)
	if !ok {
		return false
	}
	expectedManifest, err := oldManifest.ReplaceShardLeader(
		int(old.shard), oldManifest.Version()+1, changedOrdinal,
		targetEndpoint, oldMetadata.Epoch+1,
	)
	return err == nil && nextManifest.Equal(expectedManifest)
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
	RangeIdentity          string                       `json:"range_identity"`
	LineageDigest          string                       `json:"lineage_digest"`
	ForwardingRuleDigest   string                       `json:"forwarding_rule_digest"`
	RoutingVersion         uint64                       `json:"routing_version"`
	RouteGeneration        uint64                       `json:"route_generation"`
	Replicas               []persistedReplicatedReplica `json:"replicas"`
	EnrolledTarget         *persistedReplicatedReplica  `json:"enrolled_target,omitempty"`
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
			RangeIdentity:        hex.EncodeToString(descriptor.RangeIdentity[:]),
			LineageDigest:        hex.EncodeToString(descriptor.LineageDigest[:]),
			ForwardingRuleDigest: hex.EncodeToString(descriptor.ForwardingRuleDigest[:]),
			RoutingVersion:       descriptor.Command.RoutingVersion,
			RouteGeneration:      descriptor.Command.RouteGeneration,
			Replicas:             make([]persistedReplicatedReplica, len(descriptor.Replicas)),
		}
		for replicaOrdinal, replica := range descriptor.Replicas {
			entry.Replicas[replicaOrdinal] = persistReplicatedReplica(replica)
		}
		if descriptor.EnrolledTarget != nil {
			target := persistReplicatedReplica(*descriptor.EnrolledTarget)
			entry.EnrolledTarget = &target
		}
		persisted[ordinal] = entry
	}
	return persisted
}

func persistReplicatedReplica(replica ReplicatedReplicaDescriptor) persistedReplicatedReplica {
	return persistedReplicatedReplica{
		Member: replica.Member, Node: hex.EncodeToString(replica.Node[:]),
		StoreID: hex.EncodeToString(replica.StoreID[:]), NodeIncarnation: replica.NodeIncarnation,
		Endpoint: string(replica.Endpoint), NativeEndpoint: string(replica.NativeEndpoint),
		ControlEndpoint: string(replica.ControlEndpoint),
	}
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
		var rangeIdentity, lineageDigest, forwardingRuleDigest [32]byte
		for _, item := range []struct {
			name string
			raw  string
			dst  *[32]byte
		}{
			{"range identity", persisted.RangeIdentity, &rangeIdentity},
			{"lineage digest", persisted.LineageDigest, &lineageDigest},
			{"forwarding rule digest", persisted.ForwardingRuleDigest, &forwardingRuleDigest},
		} {
			if err := decodeFixed32Hex(item.raw, item.dst); err != nil {
				return nil, &CatalogError{Reason: "replicated shard " + item.name + ": " + err.Error()}
			}
		}
		descriptor := ReplicatedShardDescriptor{
			Distribution: distribution.DistributionName(persisted.Distribution),
			Shard:        distribution.ShardID(persisted.Shard), Group: group,
			AllocationGeneration: distribution.ShardAllocationGeneration(persisted.AllocationGeneration),
			RangeIdentity:        replication.Digest(rangeIdentity),
			LineageDigest:        replication.Digest(lineageDigest),
			ForwardingRuleDigest: replication.Digest(forwardingRuleDigest),
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
			decoded, err := openPersistedReplicatedReplica(replica)
			if err != nil {
				return nil, err
			}
			descriptor.Replicas[replicaOrdinal] = decoded
		}
		if persisted.EnrolledTarget != nil {
			target, err := openPersistedReplicatedReplica(*persisted.EnrolledTarget)
			if err != nil {
				return nil, err
			}
			descriptor.EnrolledTarget = &target
		}
		descriptors[ordinal] = descriptor
	}
	return descriptors, nil
}

func openPersistedReplicatedReplica(
	replica persistedReplicatedReplica,
) (ReplicatedReplicaDescriptor, error) {
	var node rafttransport.NodeID
	var storeID [16]byte
	if err := decodeFixed16Hex(replica.Node, (*[16]byte)(&node)); err != nil {
		return ReplicatedReplicaDescriptor{}, &CatalogError{Reason: "replicated replica node: " + err.Error()}
	}
	if err := decodeFixed16Hex(replica.StoreID, &storeID); err != nil {
		return ReplicatedReplicaDescriptor{}, &CatalogError{Reason: "replicated replica store: " + err.Error()}
	}
	return ReplicatedReplicaDescriptor{
		Member: replica.Member, Node: node, StoreID: storeID,
		NodeIncarnation: replica.NodeIncarnation,
		Endpoint:        distribution.EndpointID(replica.Endpoint),
		NativeEndpoint:  distribution.EndpointID(replica.NativeEndpoint),
		ControlEndpoint: distribution.EndpointID(replica.ControlEndpoint),
	}, nil
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
