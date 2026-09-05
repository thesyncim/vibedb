package gateway

// This file contains the value-only contract shared by the rebalance planner,
// the catalog authority, and the execution controller.  It deliberately does
// not contain persistence or an owner implementation: those belong to the
// catalog authority.  Keeping the contract in gateway avoids making the
// authoritative catalog depend on internal/rebalance.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash"
	"slices"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	vibejson "github.com/thesyncim/vibejson"
)

const (
	// These are control-plane records, not a second catalog.  Their bounds are
	// intentionally independent of the catalog bound so an authority can
	// reject a hostile record before retaining a large decoded value.
	MaxGroupTransitionIntentBytes   = 1 << 20
	MaxGroupPublicationReceiptBytes = 128 << 10
	maxTransitionRouteEndpoints     = 64
)

var (
	ErrGroupTransition      = errors.New("gateway: invalid group transition")
	ErrTransitionOwnerBusy  = errors.New("gateway: distribution transition owner is held")
	ErrTransitionOwnerStale = errors.New("gateway: stale distribution transition owner")
)

// TransitionPhase identifies one durable publication boundary.  Membership
// changes are intentionally separate from catalog phases: a receipt records
// what the authority actually observed, rather than a generation guessed by
// the caller.
type TransitionPhase uint8

const (
	TransitionPhaseUnknown TransitionPhase = iota
	TransitionPhaseLearner
	TransitionPhaseVoter
	TransitionPhasePreRemove
	TransitionPhasePostRemove
	TransitionPhaseRetired
	TransitionPhaseComplete
)

func (phase TransitionPhase) Valid() bool {
	return phase >= TransitionPhaseLearner && phase <= TransitionPhaseComplete
}

// GroupTransitionKey is the immutable ABA-resistant identity of one exact
// group movement.  Distribution and shard are both present because Manifest's
// Version is distribution-wide, while SourceDescriptorDigest and
// SourceCommandFenceDigest bind the source cut that was actually planned.
type GroupTransitionKey struct {
	OperationID                [32]byte
	Distribution               distribution.DistributionName
	Shard                      distribution.ShardID
	Group                      raftmember.GroupKey
	SourceAllocationGeneration uint64
	SourceDescriptorDigest     [32]byte
	SourceCommandFenceDigest   [32]byte
}

func (key GroupTransitionKey) Valid() bool {
	return key.OperationID != ([32]byte{}) && key.Distribution != "" && key.Shard != "" &&
		validTransitionGroup(key.Group) && key.SourceAllocationGeneration != 0 &&
		key.SourceDescriptorDigest != ([32]byte{}) &&
		key.SourceCommandFenceDigest != ([32]byte{})
}

// GroupTransitionIntent is the durable source provenance for a move.  The
// complete source descriptor is retained, including every serving ordinal;
// source route and roster digests remain distinct so a receipt cannot be
// replayed after a route-only or member-identity change.
type GroupTransitionIntent struct {
	Key                       GroupTransitionKey
	SourceMember              uint64
	TargetMember              uint64
	SourceHeadGeneration      uint64
	SourceHeadDigest          [32]byte
	SourceDistributionVersion distribution.RoutingVersion
	SourceGroupDigest         [32]byte
	SourceRosterDigest        [32]byte
	SourceRouteDigest         [32]byte
	SourceCommandFenceDigest  [32]byte
	SourceDescriptor          ReplicatedShardDescriptor
	SourceRoute               []distribution.EndpointID
	Replacement               ReplicatedReplicaDescriptor
	TargetDistributionVersion distribution.RoutingVersion
}

func (intent GroupTransitionIntent) Valid() bool {
	if !intent.Key.Valid() || intent.SourceMember == 0 || intent.TargetMember == 0 ||
		intent.SourceMember == intent.TargetMember || intent.SourceHeadGeneration == 0 ||
		intent.SourceHeadDigest == ([32]byte{}) ||
		intent.SourceDistributionVersion == ^distribution.RoutingVersion(0) ||
		intent.SourceDistributionVersion == 0 ||
		intent.SourceGroupDigest == ([32]byte{}) || intent.SourceRosterDigest == ([32]byte{}) ||
		intent.SourceRouteDigest == ([32]byte{}) ||
		intent.SourceCommandFenceDigest == ([32]byte{}) ||
		intent.TargetDistributionVersion != intent.SourceDistributionVersion+1 ||
		!validTransitionDescriptor(intent.SourceDescriptor) ||
		intent.SourceDescriptor.Distribution != intent.Key.Distribution ||
		intent.SourceDescriptor.Shard != intent.Key.Shard ||
		intent.SourceDescriptor.Group != intent.Key.Group ||
		DigestReplicatedShardDescriptor(intent.SourceDescriptor) != intent.Key.SourceDescriptorDigest ||
		DigestCommandFence(intent.SourceDescriptor.Command) != intent.Key.SourceCommandFenceDigest ||
		!validTransitionReplica(intent.Replacement) || intent.Replacement.Member != intent.TargetMember {
		return false
	}
	if len(intent.SourceRoute) == 0 || len(intent.SourceRoute) > maxTransitionRouteEndpoints {
		return false
	}
	for _, endpoint := range intent.SourceRoute {
		if endpoint == "" {
			return false
		}
	}
	return true
}

// GroupPublicationReceipt is an authority-produced observation.  The
// committed head and group values are copied from the publication result; a
// caller-provided expected generation is never accepted as a receipt.
type GroupPublicationReceipt struct {
	Key                          GroupTransitionKey
	Phase                        TransitionPhase
	PredecessorReceiptDigest     [32]byte
	PredecessorGroupDigest       [32]byte
	PredecessorHeadGeneration    uint64
	PredecessorHeadDigest        [32]byte
	PredecessorGroupGeneration   uint64
	PredecessorGroupHeadDigest   [32]byte
	PredecessorRosterDigest      [32]byte
	PredecessorRouteDigest       [32]byte
	CommittedHeadGeneration      uint64
	CommittedHeadDigest          [32]byte
	CommittedGroupGeneration     uint64
	CommittedGroupDigest         [32]byte
	CommittedRosterDigest        [32]byte
	CommittedRouteDigest         [32]byte
	CommittedCommandFenceDigest  [32]byte
	CommittedDistributionVersion distribution.RoutingVersion
	SourceRouteDigest            [32]byte
	SourceRosterDigest           [32]byte
}

func (receipt GroupPublicationReceipt) Valid() bool {
	return receipt.Key.Valid() && receipt.Phase.Valid() &&
		receipt.PredecessorHeadGeneration != 0 && receipt.PredecessorHeadDigest != ([32]byte{}) &&
		receipt.PredecessorGroupGeneration != 0 && receipt.PredecessorGroupDigest != ([32]byte{}) &&
		receipt.PredecessorGroupHeadDigest != ([32]byte{}) && receipt.PredecessorRosterDigest != ([32]byte{}) &&
		receipt.PredecessorRouteDigest != ([32]byte{}) && receipt.CommittedHeadGeneration != 0 &&
		receipt.CommittedHeadDigest != ([32]byte{}) && receipt.CommittedGroupGeneration != 0 &&
		receipt.CommittedGroupDigest != ([32]byte{}) && receipt.CommittedRosterDigest != ([32]byte{}) &&
		receipt.CommittedRouteDigest != ([32]byte{}) && receipt.CommittedCommandFenceDigest != ([32]byte{}) &&
		receipt.CommittedDistributionVersion != 0 && receipt.SourceRouteDigest != ([32]byte{}) &&
		receipt.SourceRosterDigest != ([32]byte{})
}

// ReceiptDigest is the canonical identity chained by the next publication.
func (receipt GroupPublicationReceipt) ReceiptDigest() ([32]byte, error) {
	raw, err := AppendGroupPublicationReceipt(nil, receipt)
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(raw), nil
}

// GroupTransitionOwnerLease is the durable per-distribution ownership fence.
// Revision and FenceDigest are returned by the authority's CAS and must be
// carried through every publication.  A lease is idempotent for OperationID.
type GroupTransitionOwnerLease struct {
	Distribution distribution.DistributionName
	OperationID  [32]byte
	Revision     uint64
	FenceDigest  [32]byte
}

func (lease GroupTransitionOwnerLease) Valid() bool {
	return lease.Distribution != "" && lease.OperationID != ([32]byte{}) &&
		lease.Revision != 0 && lease.FenceDigest != ([32]byte{})
}

// DistributionTransitionOwner is implemented by the durable catalog
// authority.  Ownership is per distribution, not a process-global mutex, so
// unrelated manifests can publish concurrently.
type DistributionTransitionOwner interface {
	AcquireDistributionTransition(context.Context, GroupTransitionKey) (GroupTransitionOwnerLease, error)
	ReleaseDistributionTransition(context.Context, GroupTransitionOwnerLease, GroupPublicationReceipt) error
}

// GroupTransitionPublisher is an optional receipt-aware catalog surface.  The
// existing CatalogAuthority remains usable during migration; once installed,
// execution must use this method and journal its returned receipt.
type GroupTransitionPublisher interface {
	PublishGroupTransition(context.Context, GroupTransitionOwnerLease, GroupTransitionIntent, TransitionPhase, *Snapshot, [32]byte) (GroupPublicationReceipt, error)
}

// GroupTransitionReceiptReader is the restart seam for a controller that has
// already published one or more phases. Receipts are authoritative durable
// observations and are never reconstructed by decrementing the current head.
type GroupTransitionReceiptReader interface {
	ReadGroupPublicationReceipt(context.Context, GroupTransitionKey) (GroupPublicationReceipt, bool, error)
}

// AppendGroupTransitionIntent appends the canonical bounded intent encoding.
func AppendGroupTransitionIntent(dst []byte, intent GroupTransitionIntent) ([]byte, error) {
	if !intent.Valid() {
		return dst, ErrGroupTransition
	}
	raw, err := vibejson.Marshal(&intent)
	if err != nil {
		return dst, errors.Join(err, ErrGroupTransition)
	}
	start := len(dst)
	dst, err = vibejson.AppendCanonicalize(dst, raw)
	if err != nil || len(dst)-start == 0 || len(dst)-start > MaxGroupTransitionIntentBytes {
		return dst[:start], errors.Join(err, ErrGroupTransition)
	}
	return dst, nil
}

// OpenGroupTransitionIntent validates canonical uniqueness and all source
// provenance fields before returning an owned value.
func OpenGroupTransitionIntent(raw []byte) (GroupTransitionIntent, error) {
	var intent GroupTransitionIntent
	if len(raw) == 0 || len(raw) > MaxGroupTransitionIntentBytes || vibejson.Unmarshal(raw, &intent) != nil {
		return intent, ErrGroupTransition
	}
	canonical, err := vibejson.Marshal(&intent)
	if err != nil {
		return GroupTransitionIntent{}, errors.Join(err, ErrGroupTransition)
	}
	canonical, err = vibejson.AppendCanonicalize(nil, canonical)
	if err != nil || !bytes.Equal(raw, canonical) || !intent.Valid() {
		return GroupTransitionIntent{}, errors.Join(err, ErrGroupTransition)
	}
	intent.SourceDescriptor = cloneTransitionDescriptor(intent.SourceDescriptor)
	intent.SourceRoute = slices.Clone(intent.SourceRoute)
	return intent, nil
}

// AppendGroupPublicationReceipt appends one canonical bounded receipt.
func AppendGroupPublicationReceipt(dst []byte, receipt GroupPublicationReceipt) ([]byte, error) {
	if !receipt.Valid() {
		return dst, ErrGroupTransition
	}
	raw, err := vibejson.Marshal(&receipt)
	if err != nil {
		return dst, errors.Join(err, ErrGroupTransition)
	}
	start := len(dst)
	dst, err = vibejson.AppendCanonicalize(dst, raw)
	if err != nil || len(dst)-start == 0 || len(dst)-start > MaxGroupPublicationReceiptBytes {
		return dst[:start], errors.Join(err, ErrGroupTransition)
	}
	return dst, nil
}

func OpenGroupPublicationReceipt(raw []byte) (GroupPublicationReceipt, error) {
	var receipt GroupPublicationReceipt
	if len(raw) == 0 || len(raw) > MaxGroupPublicationReceiptBytes || vibejson.Unmarshal(raw, &receipt) != nil {
		return receipt, ErrGroupTransition
	}
	canonical, err := vibejson.Marshal(&receipt)
	if err != nil {
		return GroupPublicationReceipt{}, errors.Join(err, ErrGroupTransition)
	}
	canonical, err = vibejson.AppendCanonicalize(nil, canonical)
	if err != nil || !bytes.Equal(raw, canonical) || !receipt.Valid() {
		return GroupPublicationReceipt{}, errors.Join(err, ErrGroupTransition)
	}
	return receipt, nil
}

// ValidateSuccessor checks the receipt chain and the exact phase progression.
// It does not compare the live catalog; the authority does that in its CAS.
func (receipt GroupPublicationReceipt) ValidateSuccessor(intent GroupTransitionIntent, prior *GroupPublicationReceipt) error {
	if !intent.Valid() || !receipt.Valid() || receipt.Key != intent.Key ||
		receipt.SourceRouteDigest != intent.SourceRouteDigest ||
		receipt.SourceRosterDigest != intent.SourceRosterDigest {
		return ErrGroupTransition
	}
	if prior == nil {
		if receipt.PredecessorReceiptDigest != ([32]byte{}) || receipt.Phase != TransitionPhaseLearner {
			return ErrGroupTransition
		}
		if receipt.PredecessorHeadGeneration != intent.SourceHeadGeneration ||
			receipt.PredecessorHeadDigest != intent.SourceHeadDigest ||
			receipt.PredecessorGroupDigest != intent.SourceGroupDigest ||
			receipt.PredecessorRosterDigest != intent.SourceRosterDigest ||
			receipt.PredecessorRouteDigest != intent.SourceRouteDigest {
			return ErrGroupTransition
		}
		return nil
	}
	if !prior.Valid() || prior.Key != receipt.Key || receipt.PredecessorReceiptDigest == ([32]byte{}) {
		return ErrGroupTransition
	}
	digest, err := prior.ReceiptDigest()
	if err != nil || digest != receipt.PredecessorReceiptDigest ||
		receipt.PredecessorHeadGeneration != prior.CommittedHeadGeneration ||
		receipt.PredecessorHeadDigest != prior.CommittedHeadDigest ||
		receipt.PredecessorGroupDigest != prior.CommittedGroupDigest ||
		receipt.PredecessorGroupGeneration != prior.CommittedGroupGeneration ||
		receipt.PredecessorGroupHeadDigest != prior.CommittedHeadDigest ||
		receipt.PredecessorRosterDigest != prior.CommittedRosterDigest ||
		receipt.PredecessorRouteDigest != prior.CommittedRouteDigest ||
		!validPhaseSuccessor(prior.Phase, receipt.Phase) {
		return ErrGroupTransition
	}
	return nil
}

// BuildGroupOwnedShardTransition applies one owned shard endpoint delta to the
// current catalog head. It is intentionally distribution scoped: the caller
// must serialize siblings in the same distribution, while unrelated
// distributions may use their own current heads concurrently. The returned
// snapshot carries current.Generation()+1 as the actual catalog head and only
// increments the distribution manifest version for the pre-remove endpoint
// cut.
func BuildGroupOwnedShardTransition(
	current *Snapshot,
	intent GroupTransitionIntent,
	phase TransitionPhase,
	replacement ReplicatedReplicaDescriptor,
	command raftservice.CommandFence,
) (*Snapshot, error) {
	if replacement == (ReplicatedReplicaDescriptor{}) {
		replacement = intent.Replacement
	}
	if current == nil || !intent.Valid() || !phase.Valid() ||
		replacement != intent.Replacement || !validTransitionReplica(replacement) ||
		replacement.Member != intent.TargetMember || !command.Valid() || current.Generation() == ^uint64(0) {
		return nil, ErrGroupTransition
	}
	manifest, ok := current.Manifest(intent.Key.Distribution)
	if !ok || manifest == nil {
		return nil, ErrGroupTransition
	}
	if phase == TransitionPhasePreRemove && manifest.Version() != intent.SourceDistributionVersion ||
		phase == TransitionPhasePostRemove && manifest.Version() != intent.TargetDistributionVersion {
		return nil, ErrGroupTransition
	}
	descriptors := current.replicatedDescriptors()
	changed := false
	for index := range descriptors {
		descriptor := &descriptors[index]
		if descriptor.Group != intent.Key.Group {
			continue
		}
		if changed || descriptor.Distribution != intent.Key.Distribution || descriptor.Shard != intent.Key.Shard {
			return nil, ErrGroupTransition
		}
		if phase == TransitionPhasePreRemove {
			if DigestReplicatedShardDescriptor(*descriptor) != intent.Key.SourceDescriptorDigest ||
				descriptor.Command != intent.SourceDescriptor.Command || len(descriptor.Replicas) != ServingReplicaCount {
				return nil, ErrGroupTransition
			}
			changedOrdinal := -1
			for replicaOrdinal := range descriptor.Replicas {
				if descriptor.Replicas[replicaOrdinal].Member == intent.SourceMember {
					changedOrdinal = replicaOrdinal
					break
				}
			}
			if changedOrdinal < 0 || !validTransitionReplica(descriptor.Replicas[changedOrdinal]) {
				return nil, ErrGroupTransition
			}
			source := descriptor.Replicas[changedOrdinal]
			descriptor.Replicas[changedOrdinal] = replacement
			descriptor.EnrolledTarget = nil
			// Route leaders can deliberately use NativeEndpoint. Preserve the
			// chosen alias while replacing the same ordered source position.
			if source.Endpoint == intent.SourceRoute[0] {
				// public route alias
			} else if source.NativeEndpoint == intent.SourceRoute[0] {
				replacementEndpoint := replacement.NativeEndpoint
				if replacementEndpoint == "" {
					return nil, ErrGroupTransition
				}
				descriptor.Replicas[changedOrdinal].Endpoint = replacementEndpoint
			} else if source.ControlEndpoint == intent.SourceRoute[0] {
				descriptor.Replicas[changedOrdinal].Endpoint = replacement.ControlEndpoint
			} else {
				return nil, ErrGroupTransition
			}
			descriptor.Command = command
			changed = true
			continue
		}
		if phase == TransitionPhasePostRemove {
			foundTarget := false
			for _, replica := range descriptor.Replicas {
				if replica.Member == replacement.Member {
					foundTarget = replica == replacement
					break
				}
			}
			if !foundTarget {
				return nil, ErrGroupTransition
			}
			descriptor.Command = command
			changed = true
		}
	}
	if !changed {
		return nil, ErrGroupTransition
	}
	var nextManifest *distribution.Manifest = manifest
	if phase == TransitionPhasePreRemove {
		ordinal, metadata := manifestShardOrdinal(manifest, intent.Key.Shard)
		if ordinal < 0 {
			return nil, ErrGroupTransition
		}
		var found bool
		metadata, found = manifest.ShardMetadataAt(ordinal)
		if !found || len(intent.SourceRoute) != metadata.LeaderCount || metadata.Epoch == ^distribution.OwnershipEpoch(0) {
			return nil, ErrGroupTransition
		}
		leader, found := manifest.ShardLeaderAt(ordinal, 0)
		if !found || leader != intent.SourceRoute[0] {
			return nil, ErrGroupTransition
		}
		targetEndpoint := replacement.Endpoint
		if len(intent.SourceRoute) > 0 {
			if len(intent.SourceDescriptor.Replicas) == metadata.LeaderCount &&
				intent.SourceDescriptor.Replicas[0].NativeEndpoint == intent.SourceRoute[0] {
				targetEndpoint = replacement.NativeEndpoint
			} else if len(intent.SourceDescriptor.Replicas) == metadata.LeaderCount &&
				intent.SourceDescriptor.Replicas[0].ControlEndpoint == intent.SourceRoute[0] {
				targetEndpoint = replacement.ControlEndpoint
			}
		}
		var err error
		nextManifest, err = manifest.ReplaceShardLeader(ordinal, intent.TargetDistributionVersion, 0, targetEndpoint, metadata.Epoch+1)
		if err != nil {
			return nil, errors.Join(err, ErrGroupTransition)
		}
	}
	config := cloneConfig(current.config)
	replacedManifest := false
	for index := range config.Manifests {
		if config.Manifests[index].Distribution() == nextManifest.Distribution() {
			config.Manifests[index] = nextManifest
			replacedManifest = true
			break
		}
	}
	if !replacedManifest {
		return nil, ErrGroupTransition
	}
	next, err := NewSnapshotWithReplicatedTableMetadata(
		config, current.endpoints, current.Generation()+1, current.indexDescriptors(),
		current.statistics.Descriptors(), descriptors, current.replicatedTableProfiles(),
		current.ReplicatedTableDeclarations(),
	)
	if err != nil {
		return nil, errors.Join(err, ErrGroupTransition)
	}
	indexHighWater, err := advanceIndexIDHighWater(current, next)
	if err != nil {
		return nil, errors.Join(err, ErrGroupTransition)
	}
	shardHighWaters, err := advanceShardGenerationHighWaters(current, next)
	if err != nil {
		return nil, errors.Join(err, ErrGroupTransition)
	}
	return snapshotWithCatalogLineage(next, indexHighWater, shardHighWaters), nil
}

func validPhaseSuccessor(prior, next TransitionPhase) bool {
	return next == prior+1 || prior == TransitionPhaseRetired && next == TransitionPhaseComplete
}

// DigestCommandFence is independent of JSON field ordering and therefore
// remains stable if the wire representation gains optional metadata.
func DigestCommandFence(fence raftservice.CommandFence) [32]byte {
	var hash digestWriter
	hash.u64(fence.ReplicaSetVersion)
	hash.u64(fence.ActivePolicyGeneration)
	hash.u64(fence.ProtectionEpoch)
	hash.u64(fence.OwnershipEpoch)
	hash.u64(fence.SchemaGeneration)
	hash.bytes(fence.RelationManifestDigest[:])
	hash.u64(fence.RoutingVersion)
	hash.u64(fence.RouteGeneration)
	return hash.sum()
}

// DigestReplicatedReplicaDescriptor includes all authenticated identities and
// aliases. Native and public endpoints must remain distinguishable.
func DigestReplicatedReplicaDescriptor(replica ReplicatedReplicaDescriptor) [32]byte {
	var hash digestWriter
	hash.u64(replica.Member)
	hash.bytes(replica.Node[:])
	hash.bytes(replica.StoreID[:])
	hash.u64(replica.NodeIncarnation)
	hash.string(string(replica.Endpoint))
	hash.string(string(replica.NativeEndpoint))
	hash.string(string(replica.ControlEndpoint))
	return hash.sum()
}

// ValidForTransition reports whether a target descriptor contains every
// authenticated identity required by a receipt-aware publication.
func (replica ReplicatedReplicaDescriptor) ValidForTransition() bool {
	return validTransitionReplica(replica)
}

func DigestReplicaRoster(roster []ReplicatedReplicaDescriptor) [32]byte {
	var hash digestWriter
	hash.u64(uint64(len(roster)))
	for _, replica := range roster {
		digest := DigestReplicatedReplicaDescriptor(replica)
		hash.bytes(digest[:])
	}
	return hash.sum()
}

// DigestReplicatedShardDescriptor commits the complete cold group descriptor,
// including the ordered RF3 roster and all schema/lineage fences.
func DigestReplicatedShardDescriptor(descriptor ReplicatedShardDescriptor) [32]byte {
	var hash digestWriter
	hash.string(string(descriptor.Distribution))
	hash.string(string(descriptor.Shard))
	hash.group(descriptor.Group)
	hash.u64(uint64(descriptor.AllocationGeneration))
	commandDigest := DigestCommandFence(descriptor.Command)
	hash.bytes(commandDigest[:])
	hash.bytes(descriptor.LogicalSchemaDigest[:])
	hash.bytes(descriptor.RangeIdentity[:])
	hash.bytes(descriptor.LineageDigest[:])
	hash.bytes(descriptor.ForwardingRuleDigest[:])
	rosterDigest := DigestReplicaRoster(descriptor.Replicas)
	hash.bytes(rosterDigest[:])
	if descriptor.EnrolledTarget == nil {
		hash.u64(0)
	} else {
		hash.u64(1)
		targetDigest := DigestReplicatedReplicaDescriptor(*descriptor.EnrolledTarget)
		hash.bytes(targetDigest[:])
	}
	return hash.sum()
}

// DigestRoute computes the ordered route identity for one manifest shard.
func DigestRoute(manifest *distribution.Manifest, shard distribution.ShardID) [32]byte {
	var hash digestWriter
	if manifest == nil {
		return hash.sum()
	}
	hash.string(string(manifest.Distribution()))
	hash.u64(uint64(manifest.Version()))
	ordinal, ok := -1, false
	for index := 0; index < manifest.ShardCount(); index++ {
		metadata, found := manifest.ShardMetadataAt(index)
		if found && metadata.ID == shard {
			ordinal = index
			ok = true
			break
		}
	}
	if !ok {
		return [32]byte{}
	}
	metadata, ok := manifest.ShardMetadataAt(ordinal)
	if !ok {
		return [32]byte{}
	}
	hash.string(string(metadata.ID))
	hash.u64(uint64(metadata.AllocationGeneration))
	hash.u64(uint64(metadata.Epoch))
	hash.u64(uint64(metadata.LeaderCount))
	for index := 0; index < metadata.LeaderCount; index++ {
		leader, found := manifest.ShardLeaderAt(ordinal, index)
		if !found {
			return [32]byte{}
		}
		hash.string(string(leader))
	}
	return hash.sum()
}

// DigestRouteFromLeaders is useful when the planner already has the bounded
// ordered route and avoids a second manifest lookup.
func DigestRouteFromLeaders(distributionName distribution.DistributionName, shard distribution.ShardID, version distribution.RoutingVersion, allocation uint64, epoch uint64, leaders []distribution.EndpointID) [32]byte {
	var hash digestWriter
	hash.string(string(distributionName))
	hash.string(string(shard))
	hash.u64(uint64(version))
	hash.u64(allocation)
	hash.u64(epoch)
	hash.u64(uint64(len(leaders)))
	for _, leader := range leaders {
		hash.string(string(leader))
	}
	return hash.sum()
}

// DigestRouteFor computes the route digest directly from one immutable
// catalog head.
func DigestRouteFor(snapshot *Snapshot, distributionName distribution.DistributionName, shard distribution.ShardID) [32]byte {
	if snapshot == nil {
		return [32]byte{}
	}
	manifest, ok := snapshot.Manifest(distributionName)
	if !ok {
		return [32]byte{}
	}
	return DigestRoute(manifest, shard)
}

type digestWriter struct{ h hash.Hash }

func (writer *digestWriter) ensure() {
	if writer.h == nil {
		writer.h = sha256.New()
	}
}

func (writer *digestWriter) bytes(value []byte) {
	writer.ensure()
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = writer.h.Write(length[:])
	_, _ = writer.h.Write(value)
}

func (writer *digestWriter) string(value string) { writer.bytes([]byte(value)) }

func (writer *digestWriter) u64(value uint64) {
	var raw [8]byte
	binary.BigEndian.PutUint64(raw[:], value)
	_, _ = writer.h.Write(raw[:])
}

func (writer *digestWriter) group(group raftmember.GroupKey) {
	writer.bytes(group.ClusterID[:])
	writer.bytes(group.ClusterIncarnation[:])
	writer.u64(group.TopologyRecoveryEpoch)
	writer.bytes(group.ShardIncarnation[:])
	writer.bytes(group.GroupID[:])
}

func (writer *digestWriter) sum() [32]byte {
	writer.ensure()
	var result [32]byte
	copy(result[:], writer.h.Sum(nil))
	return result
}

func validTransitionGroup(group raftmember.GroupKey) bool {
	return group.ClusterID != ([16]byte{}) && group.ClusterIncarnation != ([16]byte{}) &&
		group.TopologyRecoveryEpoch != 0 && group.ShardIncarnation != ([16]byte{}) &&
		group.GroupID != ([16]byte{})
}

func validTransitionReplica(replica ReplicatedReplicaDescriptor) bool {
	return replica.Member != 0 && replica.Node != ([16]byte{}) &&
		replica.StoreID != ([16]byte{}) && replica.NodeIncarnation != 0 &&
		replica.Endpoint != "" && replica.NativeEndpoint != "" && replica.ControlEndpoint != ""
}

func validTransitionDescriptor(descriptor ReplicatedShardDescriptor) bool {
	if descriptor.Distribution == "" || descriptor.Shard == "" || !validTransitionGroup(descriptor.Group) ||
		descriptor.AllocationGeneration == 0 || !descriptor.Command.Valid() ||
		descriptor.LogicalSchemaDigest == ([32]byte{}) || descriptor.RangeIdentity == ([32]byte{}) ||
		descriptor.LineageDigest == ([32]byte{}) || descriptor.ForwardingRuleDigest == ([32]byte{}) ||
		len(descriptor.Replicas) != ServingReplicaCount {
		return false
	}
	for _, replica := range descriptor.Replicas {
		if !validTransitionReplica(replica) {
			return false
		}
	}
	return true
}

func cloneTransitionDescriptor(descriptor ReplicatedShardDescriptor) ReplicatedShardDescriptor {
	descriptor.Replicas = slices.Clone(descriptor.Replicas)
	descriptor.RequestLedgerRanges = slices.Clone(descriptor.RequestLedgerRanges)
	if descriptor.EnrolledTarget != nil {
		target := *descriptor.EnrolledTarget
		descriptor.EnrolledTarget = &target
	}
	if descriptor.SplitOrigin != nil {
		origin := *descriptor.SplitOrigin
		descriptor.SplitOrigin = &origin
	}
	return descriptor
}
