package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"

	"github.com/thesyncim/vibedb/internal/membershipgrant"
	vibejson "github.com/thesyncim/vibejson"
)

const maxGroupTransitionRecordBytes = MaxGroupTransitionIntentBytes + MaxGroupPublicationReceiptBytes + 1024

type distributionTransitionOwnerRecord struct {
	Key      GroupTransitionKey
	Revision uint64
	Released bool
}

type groupTransitionRecord struct {
	Intent  GroupTransitionIntent
	Receipt GroupPublicationReceipt
}

type groupTransitionPublication struct {
	lease       GroupTransitionOwnerLease
	intent      GroupTransitionIntent
	phase       TransitionPhase
	predecessor [32]byte
}

func transitionDocumentID[T any](prefix string, value T) []byte {
	raw, _ := vibejson.Marshal(&value)
	digest := sha256.Sum256(raw)
	return []byte(prefix + hex.EncodeToString(digest[:]))
}

func encodeTransitionDocument[T any](id []byte, value T, limit int) ([]byte, error) {
	raw, err := vibejson.Marshal(&value)
	if err != nil {
		return nil, err
	}
	raw, err = vibejson.AppendCanonicalize(nil, raw)
	if err != nil {
		return nil, err
	}
	return appendControlPlaneDocument(nil, id, raw, limit)
}

func decodeTransitionDocument[T any](raw, id []byte, value *T, limit int) error {
	payload, err := openTypedControlPlaneDocument(raw, id, limit)
	if err != nil {
		return err
	}
	if err = vibejson.Unmarshal(payload, value); err != nil {
		return err
	}
	canonical, err := encodeTransitionDocument(id, *value, limit)
	if err != nil || !bytes.Equal(canonical, raw) {
		return errors.Join(err, ErrGroupTransition)
	}
	return nil
}

func ownerLease(record distributionTransitionOwnerRecord, raw []byte) GroupTransitionOwnerLease {
	return GroupTransitionOwnerLease{Distribution: record.Key.Distribution, OperationID: record.Key.OperationID,
		Revision: record.Revision, FenceDigest: sha256.Sum256(raw)}
}

// AcquireDistributionTransition retains a revisioned tombstone after release.
// A delayed publisher can never reuse an earlier owner's fence, even when the
// same operation identifier is presented again.
func (authority *ReplicatedCatalogAuthority) AcquireDistributionTransition(ctx context.Context, key GroupTransitionKey) (GroupTransitionOwnerLease, error) {
	if authority == nil || ctx == nil || !key.Valid() {
		return GroupTransitionOwnerLease{}, ErrGroupTransition
	}
	ctx, err := authority.authorizedContext(ctx)
	if err != nil {
		return GroupTransitionOwnerLease{}, err
	}
	authority.mu.Lock()
	defer authority.mu.Unlock()
	if err = authority.requireRouteSeedServingLocked(); err != nil {
		return GroupTransitionOwnerLease{}, err
	}
	if authority.session.Status().Pending {
		return GroupTransitionOwnerLease{}, ErrReplicatedCatalogPending
	}
	id := transitionDocumentID("move-own/", key.Distribution)
	result, err := authority.readRaw(ctx, fixedControlPlaneKey(id), 8192)
	if err != nil {
		return GroupTransitionOwnerLease{}, err
	}
	record := distributionTransitionOwnerRecord{Key: key, Revision: 1}
	if result.Found {
		var prior distributionTransitionOwnerRecord
		if err = decodeTransitionDocument(result.Value, id, &prior, 8192); err != nil || !prior.Key.Valid() || prior.Revision == 0 || prior.Key.Distribution != key.Distribution {
			return GroupTransitionOwnerLease{}, errors.Join(err, ErrGroupTransition)
		}
		if !prior.Released {
			if prior.Key != key {
				return GroupTransitionOwnerLease{}, ErrTransitionOwnerBusy
			}
			return ownerLease(prior, result.Value), nil
		}
		if prior.Key == key || prior.Revision == ^uint64(0) {
			return GroupTransitionOwnerLease{}, ErrTransitionOwnerStale
		}
		record.Revision = prior.Revision + 1
	}
	raw, err := encodeTransitionDocument(id, record, 8192)
	if err != nil {
		return GroupTransitionOwnerLease{}, err
	}
	native, err := authority.session.MutateBatch(ctx, []NativeMutation{scalingDirectoryMutation(result, fixedControlPlaneKey(id), raw)})
	if err = scalingMutationError(native, err, authority.session); err != nil {
		return GroupTransitionOwnerLease{}, err
	}
	return ownerLease(record, raw), nil
}

func (authority *ReplicatedCatalogAuthority) readTransitionRecord(ctx context.Context, key GroupTransitionKey) (groupTransitionRecord, ReplicatedPointResult, error) {
	id := transitionDocumentID("move-rec/", key.Group)
	result, err := authority.readRaw(ctx, fixedControlPlaneKey(id), maxGroupTransitionRecordBytes)
	if err != nil || !result.Found {
		return groupTransitionRecord{}, result, err
	}
	var record groupTransitionRecord
	if err = decodeTransitionDocument(result.Value, id, &record, maxGroupTransitionRecordBytes); err != nil ||
		!record.Intent.Valid() || !record.Receipt.Valid() || record.Intent.Key != record.Receipt.Key || record.Intent.Key.Group != key.Group {
		return groupTransitionRecord{}, result, errors.Join(err, ErrGroupTransition)
	}
	return record, result, nil
}

func (authority *ReplicatedCatalogAuthority) ReadGroupPublicationReceipt(ctx context.Context, key GroupTransitionKey) (GroupPublicationReceipt, bool, error) {
	if authority == nil || ctx == nil || !key.Valid() {
		return GroupPublicationReceipt{}, false, ErrGroupTransition
	}
	record, result, err := authority.readTransitionRecord(ctx, key)
	if err != nil || !result.Found || record.Intent.Key != key {
		return GroupPublicationReceipt{}, false, err
	}
	return record.Receipt, true, nil
}

func (authority *ReplicatedCatalogAuthority) ReleaseDistributionTransition(ctx context.Context, lease GroupTransitionOwnerLease, receipt GroupPublicationReceipt) error {
	if authority == nil || ctx == nil || !lease.Valid() || !receipt.Valid() || receipt.Phase != TransitionPhasePostRemove ||
		receipt.Key.Distribution != lease.Distribution || receipt.Key.OperationID != lease.OperationID {
		return ErrGroupTransition
	}
	ctx, err := authority.authorizedContext(ctx)
	if err != nil {
		return err
	}
	authority.mu.Lock()
	defer authority.mu.Unlock()
	if authority.session.Status().Pending {
		return ErrReplicatedCatalogPending
	}
	id := transitionDocumentID("move-own/", lease.Distribution)
	result, err := authority.readRaw(ctx, fixedControlPlaneKey(id), 8192)
	if err != nil || !result.Found {
		return errors.Join(err, ErrTransitionOwnerStale)
	}
	var record distributionTransitionOwnerRecord
	if err = decodeTransitionDocument(result.Value, id, &record, 8192); err != nil {
		return err
	}
	// Release is retryable, but the retained revision and complete key must match.
	if record.Key != receipt.Key || record.Revision != lease.Revision {
		return ErrTransitionOwnerStale
	}
	if record.Released {
		return nil
	}
	if ownerLease(record, result.Value) != lease {
		return ErrTransitionOwnerStale
	}
	durable, receiptResult, err := authority.readTransitionRecord(ctx, receipt.Key)
	if err != nil || !receiptResult.Found || durable.Receipt != receipt {
		return errors.Join(err, ErrGroupTransition)
	}
	grantKey, _ := replicatedMembershipGrantKeys(receipt.Key.Group)
	grant, err := authority.readRaw(ctx, grantKey[:], maxReplicatedMembershipGrantBytes)
	if err != nil {
		return err
	}
	if grant.Found {
		return ErrGroupTransition
	} // Retirement must have durably revoked the grant.
	record.Released = true
	raw, err := encodeTransitionDocument(id, record, 8192)
	if err != nil {
		return err
	}
	// The membership page changes on grant insertion/removal. Fence it in
	// the release batch so a concurrent new grant cannot cross the absence check.
	_, pageKey := replicatedMembershipGrantKeys(receipt.Key.Group)
	page, err := authority.readRaw(ctx, pageKey[:], maxReplicatedMembershipGrantPageBytes)
	if err != nil {
		return err
	}
	pageValue := page.Value
	if !page.Found {
		pageValue, err = appendReplicatedMembershipGrantPage(nil, pageKey.bucket(), nil)
		if err != nil {
			return err
		}
	}
	// Re-read absence after the page: a concurrent insert must either be seen
	// here or conflict with the page witness at apply.
	grant, err = authority.readRaw(ctx, grantKey[:], maxReplicatedMembershipGrantBytes)
	if err != nil || grant.Found {
		return errors.Join(err, ErrGroupTransition)
	}
	receiptID := transitionDocumentID("move-rec/", receipt.Key.Group)
	native, err := authority.session.MutateBatch(ctx, []NativeMutation{
		scalingDirectoryMutation(result, fixedControlPlaneKey(id), raw),
		scalingDirectoryMutation(page, pageKey[:], pageValue),
		scalingDirectoryMutation(receiptResult, fixedControlPlaneKey(receiptID), receiptResult.Value),
	})
	return scalingMutationError(native, err, authority.session)
}

// PublishGroupTransition uses the membership publisher's head, witness and
// certified-membership transaction. The owner fence and group receipt are
// appended to that same batch; there is no second-write recovery window.
func (authority *ReplicatedCatalogAuthority) PublishGroupTransition(ctx context.Context, lease GroupTransitionOwnerLease, intent GroupTransitionIntent, phase TransitionPhase, next *Snapshot, predecessor [32]byte) (GroupPublicationReceipt, error) {
	if authority == nil || ctx == nil || !lease.Valid() || !intent.Valid() || next == nil ||
		lease.Distribution != intent.Key.Distribution || lease.OperationID != intent.Key.OperationID ||
		(phase != TransitionPhasePreRemove && phase != TransitionPhasePostRemove) {
		return GroupPublicationReceipt{}, ErrGroupTransition
	}
	if receipt, found, err := authority.ReadGroupPublicationReceipt(ctx, intent.Key); err != nil {
		return GroupPublicationReceipt{}, err
	} else if found && receipt.Phase == phase {
		digest, err := CatalogSnapshotDigest(next)
		if err != nil || receipt.PredecessorReceiptDigest != predecessor || receipt.CommittedHeadDigest != digest {
			return GroupPublicationReceipt{}, errors.Join(err, ErrGroupTransition)
		}
		return receipt, nil
	}
	grant, found, err := authority.ReadMembershipGrant(ctx, intent.Key.Group)
	if err != nil || !found || !transitionMatchesGrant(intent, grant) {
		return GroupPublicationReceipt{}, errors.Join(err, ErrGroupTransition)
	}
	publication := &groupTransitionPublication{lease: lease, intent: intent, phase: phase, predecessor: predecessor}
	if phase == TransitionPhasePreRemove {
		err = authority.publishReplicaReplacement(ctx, next.Generation()-1, next, grant, publication)
	} else {
		version, found := replicaSetVersionForGroup(next, intent.Key.Group)
		if !found {
			return GroupPublicationReceipt{}, ErrGroupTransition
		}
		err = authority.publishReplicaReplacementPostRemove(ctx, next.Generation()-1, next, grant, version, publication)
	}
	if err != nil {
		return GroupPublicationReceipt{}, err
	}
	receipt, found, err := authority.ReadGroupPublicationReceipt(ctx, intent.Key)
	if err != nil || !found {
		return GroupPublicationReceipt{}, errors.Join(err, ErrGroupTransition)
	}
	return receipt, nil
}

func transitionMatchesGrant(intent GroupTransitionIntent, grant membershipgrant.Grant) bool {
	return grant.Group == intent.Key.Group && grant.CatalogGeneration == intent.SourceHeadGeneration &&
		grant.SourceMember == intent.SourceMember && grant.TargetMember == intent.TargetMember && grant.TargetNode == intent.Replacement.Node
}

func (authority *ReplicatedCatalogAuthority) groupTransitionMutations(ctx context.Context, publication *groupTransitionPublication, current, next *Snapshot) ([]NativeMutation, error) {
	if publication == nil {
		return nil, nil
	}
	intent := publication.intent
	id := transitionDocumentID("move-own/", intent.Key.Distribution)
	owner, err := authority.readRaw(ctx, fixedControlPlaneKey(id), 8192)
	if err != nil || !owner.Found {
		return nil, errors.Join(err, ErrTransitionOwnerStale)
	}
	var record distributionTransitionOwnerRecord
	if err = decodeTransitionDocument(owner.Value, id, &record, 8192); err != nil || record.Released || record.Key != intent.Key || ownerLease(record, owner.Value) != publication.lease {
		return nil, errors.Join(err, ErrTransitionOwnerStale)
	}
	var before, after ReplicatedShardDescriptor
	for _, descriptor := range current.replicatedDescriptors() {
		if descriptor.Group == intent.Key.Group {
			before = descriptor
		}
	}
	for _, descriptor := range next.replicatedDescriptors() {
		if descriptor.Group == intent.Key.Group {
			after = descriptor
		}
	}
	expected, err := BuildGroupOwnedShardTransition(current, intent, publication.phase, intent.Replacement, after.Command)
	if err != nil {
		return nil, err
	}
	expectedDigest, err := CatalogSnapshotDigest(expected)
	if err != nil {
		return nil, err
	}
	nextDigest, err := CatalogSnapshotDigest(next)
	if err != nil || nextDigest != expectedDigest {
		return nil, errors.Join(err, ErrGroupTransition)
	}
	currentDigest, err := CatalogSnapshotDigest(current)
	if err != nil {
		return nil, err
	}
	prior, priorRaw, err := authority.readTransitionRecord(ctx, intent.Key)
	if err != nil {
		return nil, err
	}
	var priorReceipt *GroupPublicationReceipt
	if priorRaw.Found && prior.Intent.Key == intent.Key {
		encoded, _ := AppendGroupTransitionIntent(nil, intent)
		priorEncoded, _ := AppendGroupTransitionIntent(nil, prior.Intent)
		if !bytes.Equal(encoded, priorEncoded) {
			return nil, ErrGroupTransition
		}
		priorReceipt = &prior.Receipt
	}
	manifest, found := next.Manifest(intent.Key.Distribution)
	if !found {
		return nil, ErrGroupTransition
	}
	receipt := GroupPublicationReceipt{Key: intent.Key, Phase: publication.phase, PredecessorReceiptDigest: publication.predecessor,
		PredecessorHeadGeneration: current.Generation(), PredecessorHeadDigest: currentDigest,
		PredecessorGroupGeneration: current.Generation(), PredecessorGroupHeadDigest: currentDigest,
		PredecessorGroupDigest: DigestReplicatedShardDescriptor(before), PredecessorRosterDigest: DigestReplicaRoster(before.Replicas),
		PredecessorRouteDigest:  DigestRouteFor(current, intent.Key.Distribution, intent.Key.Shard),
		CommittedHeadGeneration: next.Generation(), CommittedHeadDigest: nextDigest, CommittedGroupGeneration: next.Generation(),
		CommittedGroupDigest: DigestReplicatedShardDescriptor(after), CommittedRosterDigest: DigestReplicaRoster(after.Replicas),
		CommittedRouteDigest: DigestRouteFor(next, intent.Key.Distribution, intent.Key.Shard), CommittedCommandFenceDigest: DigestCommandFence(after.Command),
		CommittedDistributionVersion: manifest.Version(), SourceRouteDigest: intent.SourceRouteDigest, SourceRosterDigest: intent.SourceRosterDigest}
	if err = receipt.ValidateSuccessor(intent, priorReceipt); err != nil {
		return nil, err
	}
	receiptID := transitionDocumentID("move-rec/", intent.Key.Group)
	raw, err := encodeTransitionDocument(receiptID, groupTransitionRecord{Intent: intent, Receipt: receipt}, maxGroupTransitionRecordBytes)
	if err != nil {
		return nil, err
	}
	return []NativeMutation{scalingDirectoryMutation(owner, fixedControlPlaneKey(id), owner.Value), scalingDirectoryMutation(priorRaw, fixedControlPlaneKey(receiptID), raw)}, nil
}
