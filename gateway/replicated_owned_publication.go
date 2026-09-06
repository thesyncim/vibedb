package gateway

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/thesyncim/vibedb/internal/membershipgrant"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
)

func validOwnedTransitionRecord(record groupTransitionRecord) bool {
	return record.Grant.Valid() && transitionMatchesGrant(record.Intent, record.Grant) &&
		record.Command.Valid() && DigestCommandFence(record.Command) == record.Receipt.CommittedCommandFenceDigest &&
		record.Receipt.ValidateSuccessor(record.Intent, record.Previous) == nil &&
		(record.Previous == nil || record.Previous.ValidateSuccessor(record.Intent, nil) == nil)
}

// ownedReceiptMatchesCurrent accepts unrelated head advances only while the
// entire owned group, roster, route, command and distribution version remain
// byte-identical to the durable publication. It is not a legacy grant fallback.
func ownedReceiptMatchesCurrent(current *Snapshot, record groupTransitionRecord) bool {
	if current == nil || !validOwnedTransitionRecord(record) || current.Generation() < record.Receipt.CommittedHeadGeneration {
		return false
	}
	if record.Receipt.ValidateSuccessor(record.Intent, record.Previous) != nil {
		return false
	}
	manifest, found := current.Manifest(record.Intent.Key.Distribution)
	if !found || manifest.Version() != record.Receipt.CommittedDistributionVersion {
		return false
	}
	for _, descriptor := range current.replicatedDescriptors() {
		if descriptor.Group != record.Intent.Key.Group {
			continue
		}
		return DigestReplicatedShardDescriptor(descriptor) == record.Receipt.CommittedGroupDigest &&
			DigestReplicaRoster(descriptor.Replicas) == record.Receipt.CommittedRosterDigest &&
			DigestCommandFence(descriptor.Command) == record.Receipt.CommittedCommandFenceDigest &&
			DigestRouteFor(current, descriptor.Distribution, descriptor.Shard) == record.Receipt.CommittedRouteDigest
	}
	return false
}

func (authority *ReplicatedCatalogAuthority) ownedMembershipGrantAuthorization(ctx context.Context, current *Snapshot, grant membershipgrant.Grant) (bool, error) {
	if current == nil || current.Generation() < grant.CatalogGeneration {
		return false, nil
	}
	for _, descriptor := range current.replicatedDescriptors() {
		if descriptor.Group != grant.Group {
			continue
		}
		id := transitionDocumentID("move-own/", descriptor.Distribution)
		raw, err := authority.readRaw(ctx, fixedControlPlaneKey(id), 8192)
		if err != nil || !raw.Found {
			return false, err
		}
		var owner distributionTransitionOwnerRecord
		if err = decodeTransitionDocument(raw.Value, id, &owner, 8192); err != nil {
			return false, err
		}
		if owner.Released || owner.Revision == 0 || !owner.Key.Valid() || owner.Key.Group != grant.Group || owner.Key.Distribution != descriptor.Distribution {
			return false, nil
		}
		if owner.Key.SourceDescriptorDigest == DigestReplicatedShardDescriptor(descriptor) &&
			owner.Key.SourceCommandFenceDigest == DigestCommandFence(descriptor.Command) && replicatedCatalogCertifiesInitialGrant(current, grant) {
			return true, nil
		}
		record, result, err := authority.readTransitionRecord(ctx, owner.Key)
		if err != nil || !result.Found {
			return false, err
		}
		return record.Intent.Key == owner.Key && record.Grant == grant && ownedReceiptMatchesCurrent(current, record), nil
	}
	return false, nil
}

// certifyOwnedPublication validates the same certified roster change as the
// legacy publisher, but requires a separate durable group receipt and keeps
// its original source head distinct from the actual CAS predecessor head.
func certifyOwnedPublication(current, next *Snapshot, record groupTransitionRecord) (*Snapshot, error) {
	if current == nil || next == nil || !validOwnedTransitionRecord(record) ||
		record.Receipt.ValidateSuccessor(record.Intent, record.Previous) != nil ||
		record.Receipt.PredecessorHeadGeneration != current.Generation() || record.Receipt.CommittedHeadGeneration != next.Generation() {
		return nil, ErrGroupTransition
	}
	beforeDigest, err := CatalogSnapshotDigest(current)
	if err != nil || beforeDigest != record.Receipt.PredecessorHeadDigest {
		return nil, errors.Join(err, ErrGroupTransition)
	}
	nextDigest, err := CatalogSnapshotDigest(next)
	if err != nil || nextDigest != record.Receipt.CommittedHeadDigest || !ownedReceiptMatchesCurrent(next, record) {
		return nil, errors.Join(err, ErrGroupTransition)
	}
	if record.Receipt.Phase == TransitionPhasePreRemove {
		if err = validateRoutingTransition(current, next); err != nil {
			return nil, err
		}
		if err = validateCertifiedReplicaReplacement(current, next, record.Grant); err != nil {
			return nil, err
		}
	} else {
		if record.Previous == nil {
			return nil, ErrGroupTransition
		}
		prior := record
		prior.Receipt, prior.Previous = *record.Previous, nil
		// The prior command is checked against the current exact group below.
		for _, descriptor := range current.replicatedDescriptors() {
			if descriptor.Group == record.Intent.Key.Group {
				prior.Command = descriptor.Command
			}
		}
		if !ownedReceiptMatchesCurrent(current, prior) {
			return nil, ErrGroupTransition
		}
		var before, after ReplicatedShardDescriptor
		for _, descriptor := range current.replicatedDescriptors() {
			if descriptor.Group == record.Intent.Key.Group {
				before = descriptor
			}
		}
		for _, descriptor := range next.replicatedDescriptors() {
			if descriptor.Group == record.Intent.Key.Group {
				after = descriptor
			}
		}
		if after.Command.ReplicaSetVersion <= before.Command.ReplicaSetVersion {
			return nil, ErrGroupTransition
		}
		command := after.Command
		command.ReplicaSetVersion = before.Command.ReplicaSetVersion
		if command != before.Command {
			return nil, ErrGroupTransition
		}
	}
	var commandSource ReplicatedShardDescriptor
	for _, descriptor := range next.replicatedDescriptors() {
		if descriptor.Group == record.Intent.Key.Group {
			commandSource = descriptor
		}
	}
	expected, err := BuildGroupOwnedShardTransition(current, record.Intent, record.Receipt.Phase, record.Intent.Replacement, commandSource.Command)
	if err != nil {
		return nil, err
	}
	expectedDigest, err := CatalogSnapshotDigest(expected)
	if err != nil || expectedDigest != nextDigest {
		return nil, errors.Join(err, ErrGroupTransition)
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

func (authority *ReplicatedCatalogAuthority) publishOwnedGroupTransition(ctx context.Context, publication *groupTransitionPublication, next *Snapshot, grant membershipgrant.Grant) error {
	ctx, err := authority.authorizedContext(ctx)
	if err != nil {
		return err
	}
	authority.mu.Lock()
	defer authority.mu.Unlock()
	if err = authority.requireRouteSeedServingLocked(); err != nil {
		return err
	}
	if authority.session.Status().Pending {
		return ErrReplicatedCatalogPending
	}
	cut, err := authority.readCatalogCut(ctx)
	if err != nil {
		return err
	}
	grantKey, _ := replicatedMembershipGrantKeys(grant.Group)
	grantRaw, err := authority.readRaw(ctx, grantKey[:], maxReplicatedMembershipGrantBytes)
	if err != nil || !grantRaw.Found {
		return errors.Join(err, ErrGroupTransition)
	}
	stored, err := openReplicatedMembershipGrant(grantRaw.Value)
	if err != nil || stored != grant {
		return errors.Join(err, ErrGroupTransition)
	}
	publication.grant = grant
	extra, err := authority.groupTransitionMutations(ctx, publication, cut.snapshot, next)
	if err != nil {
		return err
	}
	var record groupTransitionRecord
	id := transitionDocumentID("move-rec/", grant.Group)
	if err = decodeTransitionDocument(extra[1].Value, id, &record, maxGroupTransitionRecordBytes); err != nil {
		return err
	}
	certified, err := certifyOwnedPublication(cut.snapshot, next, record)
	if err != nil {
		return err
	}
	raw, err := appendReplicatedCatalogDocument(nil, certified, maxReplicatedCatalogBytes)
	if err != nil {
		return err
	}
	witness, err := appendReplicatedCatalogHeadWitness(nil, certified.Generation(), raw)
	if err != nil {
		return err
	}
	headDigest, witnessDigest := sha256.Sum256(cut.head), sha256.Sum256(cut.witness)
	mutations := []NativeMutation{
		{Kind: replication.MutationPutDigestEqual, Key: replicatedCatalogHeadKey, Value: raw, ExpectedValueLength: uint64(len(cut.head)), ExpectedValueDigest: replication.Digest(headDigest)},
		{Kind: replication.MutationPutDigestEqual, Key: replicatedCatalogHeadWitnessKey, Value: witness, ExpectedValueLength: uint64(len(cut.witness)), ExpectedValueDigest: replication.Digest(witnessDigest)},
		scalingDirectoryMutation(grantRaw, grantKey[:], grantRaw.Value),
	}
	mutations = append(mutations, extra...)
	// Retain each exact publication so a lagging gateway can certify every
	// missed head, even after the per-group latest receipt is overwritten.
	eventID := transitionDocumentID("move-head/", next.Generation())
	eventRaw, err := encodeTransitionDocument(eventID, record, maxGroupTransitionRecordBytes)
	if err != nil {
		return err
	}
	mutations = append(mutations, NativeMutation{Kind: replication.MutationPutAbsentOrEqual, Key: fixedControlPlaneKey(eventID), Value: eventRaw})

	if len(mutations) > authority.session.bundle.maxMutations {
		return ErrReplicatedCatalog
	}
	result, err := authority.session.MutateBatch(ctx, mutations)
	if err != nil {
		if errors.Is(err, ErrNativeCommandPending) || authority.session.Status().Pending {
			authority.pendingCatalog, authority.pendingExpected, authority.pendingOwned = certified, cut.snapshot.Generation(), true
			return errors.Join(err, ErrReplicatedCatalogPending)
		}
		return err
	}
	if result.Completion.ResultCode == replicatedstate.ResultIndexConflict {
		return fmt.Errorf("%w: owned publication CAS group=%x phase=%d head=%d", ErrReplicatedCatalogConflict, grant.Group.GroupID, publication.phase, next.Generation())
	}
	if result.Completion.ResultCode != replicatedstate.ResultApplied {
		return ErrReplicatedCatalog
	}
	if err = authority.observePublishedCatalog(ctx, certified); err != nil {
		return err
	}
	_, err = authority.publishReadCatalogCut(ctx, certified, raw)
	return err
}

// prepareOwnedGroupRead replays authenticated, atomically retained head events.
// Missing history or any unrelated change that cannot be reconstructed is denied.
func (authority *ReplicatedCatalogAuthority) prepareOwnedGroupRead(ctx context.Context, current, next *Snapshot, nextRaw []byte) (*Snapshot, func() error, bool, error) {
	if current == nil || next == nil || next.Generation() <= current.Generation() {
		return nil, nil, false, nil
	}
	certified := current
	for generation := current.Generation() + 1; ; generation++ {
		id := transitionDocumentID("move-head/", generation)
		raw, err := authority.readRaw(ctx, fixedControlPlaneKey(id), maxGroupTransitionRecordBytes)
		if err != nil {
			return nil, nil, true, err
		}
		if !raw.Found {
			if generation == current.Generation()+1 {
				return nil, nil, false, nil
			}
			return nil, nil, true, ErrGroupTransition
		}
		var record groupTransitionRecord
		if err = decodeTransitionDocument(raw.Value, id, &record, maxGroupTransitionRecordBytes); err != nil {
			return nil, nil, true, err
		}
		candidate, err := BuildGroupOwnedShardTransition(certified, record.Intent, record.Receipt.Phase, record.Intent.Replacement, record.Command)
		if err != nil {
			return nil, nil, true, err
		}
		certified, err = certifyOwnedPublication(certified, candidate, record)
		if err != nil {
			return nil, nil, true, err
		}
		if generation == next.Generation() {
			break
		}
	}
	canonical, err := appendReplicatedCatalogDocument(nil, certified, maxReplicatedCatalogBytes)
	if err != nil || sha256.Sum256(canonical) != sha256.Sum256(nextRaw) {
		return nil, nil, true, errors.Join(err, ErrGroupTransition)
	}
	sourceDigest, err := CatalogSnapshotDigest(current)
	if err != nil {
		return nil, nil, true, err
	}
	return certified, func() error {
		h := authority.holder
		h.leaseMu.Lock()
		defer h.leaseMu.Unlock()
		h.initLeaseTrackerLocked()
		installed := h.ptr.Load()
		if installed == nil {
			return ErrCatalogGenerationMismatch
		}
		digest, err := CatalogSnapshotDigest(installed)
		if err != nil {
			return err
		}
		if installed.Generation() == next.Generation() {
			nextDigest, err := CatalogSnapshotDigest(certified)
			if err != nil || digest != nextDigest {
				return errors.Join(err, ErrGroupTransition)
			}
			return nil
		}
		if installed.Generation() != current.Generation() || digest != sourceDigest {
			return ErrCatalogGenerationMismatch
		}
		h.ptr.Store(certified)
		h.signalLeaseChangeLocked()
		return nil
	}, true, nil
}
