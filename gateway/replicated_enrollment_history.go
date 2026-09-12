package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"slices"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/replication"
)

// The directory's encoded native value bounds its input even after a catalog
// shrink makes formerly live recovery records collectible. Every entry carries
// two full SHA-256 values, so this is a conservative grammar-derived read bound.
// Retained output has the stricter live-inventory plus inactive-history bound.
const maxEnrollmentRecoveryHistoryEntries = maxEnrollmentHistoryBytes / (2 * sha256.Size)

type enrollmentHistoryRecord struct {
	entry  scalingIDDirectoryEntry
	intent GroupEnrollmentIntent
	raw    []byte
}

type enrollmentHistoryPlan struct {
	entries []scalingIDDirectoryEntry
	evicted []enrollmentHistoryRecord
}

type enrollmentRecoveryOwner struct {
	group        raftmember.GroupKey
	distribution distribution.DistributionName
	shard        distribution.ShardID
	allocation   distribution.ShardAllocationGeneration
	target       ReplicaIdentity
}

func enrollmentRecoveryInventory(snapshot *Snapshot) (map[enrollmentRecoveryOwner]struct{}, error) {
	if snapshot == nil || snapshot.Generation() == 0 {
		return nil, ErrReplicatedCatalogConflict
	}
	owners := make(map[enrollmentRecoveryOwner]struct{})
	var replicas [ServingReplicaCount]ReplicatedEndpoint
	for index := 0; index < snapshot.ReplicatedRouteCount(); index++ {
		route, ok := snapshot.ReplicatedRouteAt(index, replicas[:0])
		if !ok {
			return nil, ErrReplicatedCatalogConflict
		}
		membership, ok := snapshot.ResolveReplicatedMembershipRoute(route.Distribution, route.Shard, replicas[:0])
		if !ok || membership.Serving.Group != route.Group || len(membership.Serving.Replicas) != ServingReplicaCount {
			return nil, ErrReplicatedCatalogConflict
		}
		add := func(replica ReplicatedEndpoint) error {
			identity := ReplicaIdentity{
				Member: replica.Member, Node: replica.Node, NodeIncarnation: replica.NodeIncarnation, StoreID: replica.StoreID,
				Endpoint: distribution.EndpointID(replica.Endpoint), NativeEndpoint: distribution.EndpointID(replica.NativeEndpoint),
				ControlEndpoint: distribution.EndpointID(replica.ControlEndpoint),
			}
			if !identity.Valid() {
				return ErrReplicatedCatalogConflict
			}
			owner := enrollmentRecoveryOwner{group: route.Group, distribution: route.Distribution,
				shard: route.Shard, allocation: distribution.ShardAllocationGeneration(route.AllocationGeneration), target: identity}
			if _, duplicate := owners[owner]; duplicate {
				return ErrReplicatedCatalogConflict
			}
			owners[owner] = struct{}{}
			return nil
		}
		for _, replica := range membership.Serving.Replicas {
			if err := add(replica); err != nil {
				return nil, err
			}
		}
		if membership.HasEnrolledTarget {
			if err := add(membership.EnrolledTarget); err != nil {
				return nil, err
			}
		}
	}
	return owners, nil
}

func planEnrollmentHistory(snapshot *Snapshot, history []enrollmentHistoryRecord, terminal GroupEnrollmentIntent, digest replication.Digest) (enrollmentHistoryPlan, error) {
	if !terminal.Valid() || terminal.State < EnrollmentComplete || digest == (replication.Digest{}) ||
		len(history) > maxEnrollmentRecoveryHistoryEntries {
		return enrollmentHistoryPlan{}, ErrInvalidScalingMetadata
	}
	owners, err := enrollmentRecoveryInventory(snapshot)
	if err != nil {
		return enrollmentHistoryPlan{}, err
	}
	records := make([]enrollmentHistoryRecord, 0, len(history)+1)
	for _, record := range history {
		if !record.intent.Valid() || record.intent.State < EnrollmentComplete ||
			!bytes.Equal(record.entry.ID, record.intent.IntentID[:]) || record.entry.Revision != record.intent.Revision ||
			len(record.entry.Digest) != sha256.Size {
			return enrollmentHistoryPlan{}, ErrInvalidScalingMetadata
		}
		if record.intent.IntentID != terminal.IntentID {
			records = append(records, record)
		}
	}
	records = append(records, enrollmentHistoryRecord{intent: terminal, entry: scalingIDDirectoryEntry{
		ID: bytes.Clone(terminal.IntentID[:]), Revision: terminal.Revision, Digest: bytes.Clone(digest[:]),
	}})
	slices.SortFunc(records, func(left, right enrollmentHistoryRecord) int { return bytes.Compare(left.entry.ID, right.entry.ID) })
	pinned := make([]bool, len(records))
	claimed := make(map[enrollmentRecoveryOwner]struct{}, len(owners))
	inactive := 0
	for index, record := range records {
		if index != 0 && bytes.Equal(records[index-1].entry.ID, record.entry.ID) {
			return enrollmentHistoryPlan{}, ErrInvalidScalingMetadata
		}
		intent := record.intent
		owner := enrollmentRecoveryOwner{group: intent.Group, distribution: intent.Distribution,
			shard: intent.Shard, allocation: intent.AllocationGeneration, target: intent.Target}
		_, resident := owners[owner]
		if intent.State == EnrollmentComplete && resident {
			// One exact installed store has one original enrollment provenance.
			// Reject conflicting claims instead of growing history independently
			// of the finite serving and enrolled catalog inventory.
			if _, duplicate := claimed[owner]; duplicate {
				return enrollmentHistoryPlan{}, ErrScalingIdentity
			}
			claimed[owner] = struct{}{}
			pinned[index] = true
		} else {
			inactive++
		}
	}
	evict := max(0, inactive-maxEnrollmentHistory)
	plan := enrollmentHistoryPlan{entries: make([]scalingIDDirectoryEntry, 0, len(records)-evict)}
	for index, record := range records {
		if evict != 0 && !pinned[index] && record.intent.IntentID != terminal.IntentID {
			plan.evicted = append(plan.evicted, record)
			evict--
			continue
		}
		plan.entries = append(plan.entries, record.entry)
	}
	if evict != 0 || len(plan.entries) > len(owners)+maxEnrollmentHistory {
		return enrollmentHistoryPlan{}, ErrScalingMetadataBound
	}
	return plan, nil
}

// enrollmentHistoryMutations runs under authority.mu. Recovery provenance is
// retained for every exact live target; only inactive history is collectible.
// Every collection batch compares the same certified catalog cut used to prove
// absence. A large catalog shrink is collected in bounded batches before the
// caller atomically commits its new terminal row and parent progress.
func (authority *ReplicatedCatalogAuthority) enrollmentHistoryMutations(ctx context.Context, intent GroupEnrollmentIntent, recordDigest replication.Digest, remainingMutations int) ([]NativeMutation, error) {
	if authority == nil || authority.session == nil || ctx == nil || remainingMutations < 3 {
		return nil, ErrNativeBundleBound
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		cut, err := authority.readCatalogCut(ctx)
		if err != nil {
			return nil, err
		}
		entries, historyResult, err := authority.readTerminalHistory(ctx, enrollmentHistoryKey, enrollmentHistoryDocumentID[:],
			maxEnrollmentHistoryBytes, maxEnrollmentRecoveryHistoryEntries)
		if err != nil {
			return nil, err
		}
		history := make([]enrollmentHistoryRecord, 0, len(entries))
		for _, entry := range entries {
			var id [32]byte
			copy(id[:], entry.ID)
			raw, readErr := authority.readRaw(ctx, enrollmentIntentKey(id), maxEnrollmentRecordBytes)
			if readErr != nil || !raw.Found || scalingDigest(raw.Value) != replication.Digest(entry.Digest) {
				return nil, errors.Join(readErr, ErrReplicatedCatalogConflict)
			}
			record, readErr := openEnrollmentIntentRecord(raw.Value, id)
			if readErr != nil || record.Revision != entry.Revision {
				return nil, errors.Join(readErr, ErrReplicatedCatalogConflict)
			}
			history = append(history, enrollmentHistoryRecord{entry: entry, intent: record, raw: raw.Value})
		}
		plan, err := planEnrollmentHistory(cut.snapshot, history, intent, recordDigest)
		if err != nil {
			return nil, err
		}
		if len(plan.evicted)+3 <= remainingMutations {
			return enrollmentHistoryBatch(cut, historyResult, plan.entries, plan.evicted)
		}
		deleteCount := min(len(plan.evicted), authority.session.bundle.maxMutations-3)
		if deleteCount <= 0 {
			return nil, ErrNativeBundleBound
		}
		removed := make(map[[32]byte]struct{}, deleteCount)
		for _, record := range plan.evicted[:deleteCount] {
			removed[record.intent.IntentID] = struct{}{}
		}
		retained := make([]scalingIDDirectoryEntry, 0, len(entries)-deleteCount)
		for _, entry := range entries {
			var id [32]byte
			copy(id[:], entry.ID)
			if _, evicted := removed[id]; !evicted {
				retained = append(retained, entry)
			}
		}
		mutations, err := enrollmentHistoryBatch(cut, historyResult, retained, plan.evicted[:deleteCount])
		if err != nil {
			return nil, err
		}
		result, err := authority.session.MutateBatch(ctx, mutations)
		if err = scalingMutationError(result, err, authority.session); err != nil {
			return nil, err
		}
		// The catalog and history may have changed alongside this successful
		// chunk. Re-read both before selecting another victim or the final CAS.
	}
}

func enrollmentHistoryBatch(cut replicatedCatalogCut, historyResult ReplicatedPointResult, entries []scalingIDDirectoryEntry, evicted []enrollmentHistoryRecord) ([]NativeMutation, error) {
	historyBytes, err := appendScalingIDDirectory(nil, enrollmentHistoryDocumentID[:], entries, maxEnrollmentHistoryBytes)
	if err != nil {
		return nil, err
	}
	mutations := make([]NativeMutation, 0, 3+len(evicted))
	mutations = append(mutations,
		NativeMutation{Kind: replication.MutationPutDigestEqual, Key: replicatedCatalogHeadKey, Value: cut.head,
			ExpectedValueLength: uint64(len(cut.head)), ExpectedValueDigest: scalingDigest(cut.head)},
		NativeMutation{Kind: replication.MutationPutDigestEqual, Key: replicatedCatalogHeadWitnessKey, Value: cut.witness,
			ExpectedValueLength: uint64(len(cut.witness)), ExpectedValueDigest: scalingDigest(cut.witness)},
		scalingDirectoryMutation(historyResult, enrollmentHistoryKey, historyBytes),
	)
	for _, record := range evicted {
		mutations = append(mutations, NativeMutation{Kind: replication.MutationDeleteDigestEqual,
			Key: enrollmentIntentKey(record.intent.IntentID), ExpectedValueLength: uint64(len(record.raw)),
			ExpectedValueDigest: scalingDigest(record.raw)})
	}
	return mutations, nil
}
