package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"slices"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/membershipgrant"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
)

// MaxReplicatedCatalogBatchMutations is workspace for the existing bounded
// operation directory: head+witness, one receipt per move, and one occupancy
// page per receipt. Command-byte, storage, and directory limits are unchanged.
const MaxReplicatedCatalogBatchMutations = 2 + 2*maxReplicatedOperations

// ReplicaReplacementChange describes one independently certified member of
// an atomically admitted move set. All members publish the same G+1 and G+2
// cuts: publishing one sibling first would invalidate every other G intent.
type ReplicaReplacementChange struct {
	Grant    membershipgrant.Grant
	Manifest *distribution.Manifest
	Target   ReplicatedReplicaDescriptor
	Command  raftservice.CommandFence
}

// BuildReplicaReplacementSetTransition validates every change through the
// ordinary single-group builder, then combines only those exact projections.
// No unrelated schema, roster, endpoint, or serving fence can be smuggled into
// the combined cut. Multiple changes in one distribution require a separate
// manifest planner and are deliberately refused here.
func BuildReplicaReplacementSetTransition(current *Snapshot, changes []ReplicaReplacementChange, postRemove bool) (*Snapshot, error) {
	if current == nil || len(changes) == 0 || len(changes) > maxReplicatedOperations {
		return nil, ErrReplicatedCatalogConflict
	}
	config := cloneConfig(current.config)
	descriptors := current.replicatedDescriptors()
	groups := make(map[raftmember.GroupKey]bool, len(changes))
	distributions := make(map[distribution.DistributionName]bool, len(changes))
	for _, change := range changes {
		if groups[change.Grant.Group] {
			return nil, ErrReplicatedCatalogConflict
		}
		groups[change.Grant.Group] = true
		var projected *Snapshot
		var err error
		if postRemove {
			projected, err = BuildReplicaReplacementPostRemoveTransition(current, current.Generation()+1, change.Grant, change.Command.ReplicaSetVersion)
		} else {
			projected, err = BuildReplicaReplacementTransition(current, change.Manifest, current.Generation()+1, change.Grant, change.Target, change.Command)
		}
		if err != nil {
			return nil, err
		}
		found := false
		for _, replacement := range projected.replicatedDescriptors() {
			if replacement.Group != change.Grant.Group {
				continue
			}
			if distributions[replacement.Distribution] {
				return nil, ErrReplicatedCatalogConflict
			}
			distributions[replacement.Distribution] = true
			for i := range descriptors {
				if descriptors[i].Group == replacement.Group {
					descriptors[i] = replacement
					found = true
				}
			}
			manifest, ok := projected.Manifest(replacement.Distribution)
			if !ok {
				return nil, ErrReplicatedCatalogConflict
			}
			for i := range config.Manifests {
				if config.Manifests[i].Distribution() == replacement.Distribution {
					config.Manifests[i] = manifest
				}
			}
		}
		if !found {
			return nil, ErrReplicatedCatalogConflict
		}
	}
	next, err := NewSnapshotWithReplicatedTableMetadata(config, current.endpoints, current.Generation()+1,
		current.indexDescriptors(), current.statistics.Descriptors(), descriptors, current.replicatedTableProfiles(), current.ReplicatedTableDeclarations())
	if err != nil {
		return nil, err
	}
	indexes, err := advanceIndexIDHighWater(current, next)
	if err != nil {
		return nil, err
	}
	shards, err := advanceShardGenerationHighWaters(current, next)
	if err != nil {
		return nil, err
	}
	return snapshotWithCatalogLineage(next, indexes, shards), nil
}

func validateReplicaReplacementSet(current, next *Snapshot, grants []membershipgrant.Grant, postRemove bool) (*Snapshot, error) {
	if current == nil || next == nil || next.Generation() != current.Generation()+1 {
		return nil, ErrReplicatedCatalogConflict
	}
	changes := make([]ReplicaReplacementChange, 0, len(grants))
	for _, grant := range grants {
		change := ReplicaReplacementChange{Grant: grant}
		found := false
		for _, descriptor := range next.replicatedDescriptors() {
			if descriptor.Group != grant.Group {
				continue
			}
			change.Command = descriptor.Command
			change.Manifest, found = next.Manifest(descriptor.Distribution)
			for _, replica := range descriptor.Replicas {
				if replica.Member == grant.TargetMember {
					change.Target = replica
				}
			}
		}
		if !found {
			return nil, ErrReplicatedCatalogConflict
		}
		changes = append(changes, change)
	}
	certified, err := BuildReplicaReplacementSetTransition(current, changes, postRemove)
	if err != nil {
		return nil, err
	}
	want, err := appendReplicatedCatalogDocument(nil, certified, maxReplicatedCatalogBytes)
	if err != nil {
		return nil, err
	}
	got, err := appendReplicatedCatalogDocument(nil, next, maxReplicatedCatalogBytes)
	if err != nil || !bytes.Equal(want, got) {
		return nil, errors.Join(err, ErrReplicatedCatalogConflict)
	}
	return certified, nil
}

func (h *CatalogHolder) publishReplicaReplacementSetAfter(expected uint64, next *Snapshot, grants []membershipgrant.Grant, postRemove bool) error {
	h.leaseMu.Lock()
	defer h.leaseMu.Unlock()
	h.initLeaseTrackerLocked()
	current := h.ptr.Load()
	if current == nil || current.Generation() != expected {
		return ErrCatalogGenerationMismatch
	}
	certified, err := validateReplicaReplacementSet(current, next, grants, postRemove)
	if err != nil {
		return err
	}
	h.ptr.Store(certified)
	h.signalLeaseChangeLocked()
	return nil
}

type replacementSetPage struct {
	key    []byte
	prior  []byte
	groups []raftmember.GroupKey
}

// PublishReplicaReplacementSet commits every receipt, the complete catalog
// head, and its witness in one relation transaction. It retains the exact
// grants through G+2, just like the single-group lifecycle.
func (authority *ReplicatedCatalogAuthority) PublishReplicaReplacementSet(ctx context.Context, expectedGeneration uint64, next *Snapshot, grants []membershipgrant.Grant, postRemove bool) error {
	if authority == nil || authority.session == nil || ctx == nil || len(grants) < 2 || len(grants) > maxReplicatedOperations {
		return ErrReplicatedCatalog
	}
	ordered := slices.Clone(grants)
	slices.SortFunc(ordered, func(a, b membershipgrant.Grant) int { return compareMembershipGrantGroup(a.Group, b.Group) })
	for i, grant := range ordered {
		if !grant.Valid() || i > 0 && ordered[i-1].Group == grant.Group {
			return ErrReplicatedCatalog
		}
	}
	ctx, err := authority.authorizedContext(ctx)
	if err != nil {
		return err
	}
	authority.mu.Lock()
	defer authority.mu.Unlock()
	if err := authority.requireRouteSeedServingLocked(); err != nil {
		return err
	}
	if authority.session.Status().Pending {
		return ErrReplicatedCatalogPending
	}
	cut, err := authority.readCatalogCut(ctx)
	if err != nil {
		return err
	}
	if cut.snapshot.Generation() != expectedGeneration {
		return ErrCatalogGenerationMismatch
	}
	certified, err := validateReplicaReplacementSet(cut.snapshot, next, ordered, postRemove)
	if err != nil {
		return err
	}
	head, err := appendReplicatedCatalogDocument(nil, certified, maxReplicatedCatalogBytes)
	if err != nil {
		return err
	}
	witness, err := appendReplicatedCatalogHeadWitness(nil, certified.Generation(), head)
	if err != nil {
		return err
	}
	mutations := []NativeMutation{replacementSetCAS(replicatedCatalogHeadKey, cut.head, head), replacementSetCAS(replicatedCatalogHeadWitnessKey[:], cut.witness, witness)}
	pages := make(map[byte]*replacementSetPage)
	for _, grant := range ordered {
		grantKey, grantPageKey := replicatedMembershipGrantKeys(grant.Group)
		stored, err := authority.readRaw(ctx, grantKey[:], maxReplicatedMembershipGrantBytes)
		if err != nil || !stored.Found {
			return errors.Join(err, ErrReplicatedCatalogConflict)
		}
		exact, err := openReplicatedMembershipGrant(stored.Value)
		if err != nil || exact != grant {
			return errors.Join(err, ErrReplicatedCatalogConflict)
		}
		grantPage, err := authority.readRaw(ctx, grantPageKey[:], maxReplicatedMembershipGrantPageBytes)
		if err != nil || !grantPage.Found {
			return errors.Join(err, ErrReplicatedCatalogConflict)
		}
		grantGroups, err := openReplicatedMembershipGrantPage(grantPageKey.bucket(), grantPage.Value)
		if err != nil {
			return err
		}
		if _, found := findReplicatedMembershipGrantGroup(grantGroups, grant.Group); !found {
			return ErrReplicatedCatalogConflict
		}
		receiptKey, pageKey := replicatedReplicaReplacementReceiptKeys(grant.Group)
		prior, err := authority.readRaw(ctx, receiptKey[:], maxReplicatedReplicaReplacementReceiptBytes)
		if err != nil {
			return err
		}
		version, found := replicaSetVersionForGroup(next, grant.Group)
		if !found {
			return ErrReplicatedCatalogConflict
		}
		var receipt []byte
		if postRemove {
			old, err := openReplicaReplacementReceipt(prior.Value)
			previousVersion, found := replicaSetVersionForGroup(cut.snapshot, grant.Group)
			if err != nil || !prior.Found || old.Grant != grant || old.NewGeneration != expectedGeneration || old.PostRemoveGeneration != 0 ||
				old.NewHeadBytes != uint64(len(cut.head)) || old.NewHeadDigest != sha256.Sum256(cut.head) || !found || old.PublishedReplicaSetVersion != previousVersion {
				return errors.Join(err, ErrReplicatedCatalogConflict)
			}
			old.PostRemoveGeneration, old.PostRemoveReplicaSetVersion = next.Generation(), version
			old.PostRemoveHeadBytes, old.PostRemoveHeadDigest = uint64(len(head)), sha256.Sum256(head)
			receipt, err = appendReplicaReplacementReceiptRecord(nil, old)
			if err != nil {
				return err
			}
		} else {
			page := pages[pageKey.bucket()]
			if page == nil {
				result, err := authority.readRaw(ctx, pageKey[:], maxReplicatedMembershipGrantPageBytes)
				if err != nil {
					return err
				}
				page = &replacementSetPage{key: bytes.Clone(pageKey[:]), prior: bytes.Clone(result.Value)}
				if result.Found {
					page.groups, err = openReplicaReplacementReceiptPage(pageKey.bucket(), result.Value)
					if err != nil {
						return err
					}
				}
				pages[pageKey.bucket()] = page
			}
			index, found := findReplicatedMembershipGrantGroup(page.groups, grant.Group)
			if prior.Found != found {
				return ErrReplicatedCatalogConflict
			}
			if prior.Found {
				old, err := openReplicaReplacementReceipt(prior.Value)
				oldMatches := old.NewGeneration == expectedGeneration && old.NewHeadBytes == uint64(len(cut.head)) && old.NewHeadDigest == sha256.Sum256(cut.head)
				postMatches := old.PostRemoveGeneration == expectedGeneration && old.PostRemoveHeadBytes == uint64(len(cut.head)) && old.PostRemoveHeadDigest == sha256.Sum256(cut.head)
				if err != nil || old.Grant.Group != grant.Group || !oldMatches && !postMatches {
					return errors.Join(err, ErrReplicatedCatalogConflict)
				}
			} else {
				if len(page.groups) >= maxReplicatedMembershipGrantsPerPage {
					return ErrReplicatedCatalogConflict
				}
				page.groups = append(page.groups, raftmember.GroupKey{})
				copy(page.groups[index+1:], page.groups[index:])
				page.groups[index] = grant.Group
			}
			receipt, err = appendReplicaReplacementReceipt(nil, grant, cut.head, head, version)
			if err != nil {
				return err
			}
		}
		mutations = append(mutations, replacementSetCAS(bytes.Clone(receiptKey[:]), prior.Value, receipt))
	}
	// Canonical page order also makes outcome-unknown retries byte-identical
	// when two groups hash to the same receipt directory page.
	for bucket := 0; bucket < replicatedMembershipGrantPages; bucket++ {
		page := pages[byte(bucket)]
		if page == nil {
			continue
		}
		raw, err := appendReplicaReplacementReceiptPage(nil, byte(bucket), page.groups)
		if err != nil {
			return err
		}
		mutations = append(mutations, replacementSetCAS(page.key, page.prior, raw))
	}
	if len(mutations) > authority.session.bundle.maxMutations {
		return ErrReplicatedCatalog
	}
	result, err := authority.session.MutateBatch(ctx, mutations)
	if err != nil {
		if authority.session.Status().Pending {
			authority.pendingCatalog, authority.pendingExpected = certified, expectedGeneration
			authority.pendingReplacementSet, authority.pendingReplacementSetPostRemove = ordered, postRemove
			return errors.Join(err, ErrReplicatedCatalogPending)
		}
		return err
	}
	if result.Completion.ResultCode == replicatedstate.ResultIndexConflict {
		return ErrReplicatedCatalogConflict
	}
	if result.Completion.ResultCode != replicatedstate.ResultApplied {
		return ErrReplicatedCatalog
	}
	if err := authority.observePublishedCatalog(certified); err != nil {
		return err
	}
	return authority.holder.publishReplicaReplacementSetAfter(expectedGeneration, certified, ordered, postRemove)
}

func replacementSetCAS(key, prior, next []byte) NativeMutation {
	mutation := NativeMutation{Kind: replication.MutationPutAbsentOrEqual, Key: key, Value: next}
	if len(prior) != 0 {
		mutation.Kind = replication.MutationPutDigestEqual
		mutation.ExpectedValueLength = uint64(len(prior))
		mutation.ExpectedValueDigest = replication.Digest(sha256.Sum256(prior))
	}
	return mutation
}

func (authority *ReplicatedCatalogAuthority) prepareCertifiedReplicaReplacementSetRead(ctx context.Context, current, next *Snapshot, nextRaw []byte) (*Snapshot, func() error, error) {
	if current == nil || next == nil || next.Generation() != current.Generation()+1 || len(current.replicatedShards) != len(next.replicatedShards) {
		return nil, nil, ErrReplicatedCatalogConflict
	}
	currentRaw, err := appendReplicatedCatalogDocument(nil, current, maxReplicatedCatalogBytes)
	if err != nil {
		return nil, nil, err
	}
	var grants []membershipgrant.Grant
	var postRemove bool
	for _, old := range current.replicatedShards {
		manifest := current.config.Manifests[old.manifest]
		metadata, _ := manifest.ShardMetadataAt(int(old.shard))
		candidate, found := next.replicatedShardAt(manifest.Distribution(), metadata.ID)
		if !found || candidate.group != old.group {
			return nil, nil, ErrReplicatedCatalogConflict
		}
		if candidate.command == old.command && sameReplicatedCatalogRoster(current, old, next, candidate) {
			continue
		}
		key, _ := replicatedReplicaReplacementReceiptKeys(old.group)
		result, err := authority.readRaw(ctx, key[:], maxReplicatedReplicaReplacementReceiptBytes)
		if err != nil || !result.Found {
			return nil, nil, errors.Join(err, ErrReplicatedCatalogConflict)
		}
		receipt, err := openReplicaReplacementReceipt(result.Value)
		if err != nil || receipt.Grant.Group != old.group {
			return nil, nil, errors.Join(err, ErrReplicatedCatalogConflict)
		}
		post := current.Generation() == receipt.NewGeneration && next.Generation() == receipt.PostRemoveGeneration
		if len(grants) != 0 && post != postRemove {
			return nil, nil, ErrReplicatedCatalogConflict
		}
		postRemove = post
		if post {
			if receipt.NewHeadBytes != uint64(len(currentRaw)) || receipt.NewHeadDigest != sha256.Sum256(currentRaw) ||
				receipt.PostRemoveHeadBytes != uint64(len(nextRaw)) || receipt.PostRemoveHeadDigest != sha256.Sum256(nextRaw) ||
				receipt.PublishedReplicaSetVersion != old.command.ReplicaSetVersion || receipt.PostRemoveReplicaSetVersion != candidate.command.ReplicaSetVersion {
				return nil, nil, ErrReplicatedCatalogConflict
			}
		} else {
			if _, err := validateReplicaReplacementReceipt(result.Value, currentRaw, nextRaw, current.Generation(), next.Generation()); err != nil {
				return nil, nil, err
			}
			if receipt.PublishedReplicaSetVersion != candidate.command.ReplicaSetVersion {
				return nil, nil, ErrReplicatedCatalogConflict
			}
		}
		grants = append(grants, receipt.Grant)
	}
	if len(grants) < 2 {
		return nil, nil, ErrReplicatedCatalogConflict
	}
	certified, err := validateReplicaReplacementSet(current, next, grants, postRemove)
	if err != nil {
		return nil, nil, err
	}
	return certified, func() error {
		return authority.holder.publishReplicaReplacementSetAfter(current.Generation(), certified, grants, postRemove)
	}, nil
}
