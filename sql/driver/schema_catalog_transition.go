package driver

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"reflect"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
)

// ReplicatedSchemaTransitionAuthority is the three independently authenticated
// control-plane witnesses carried by the ordered Raft entry. RequestDigest
// identifies the prepared rollout and checkpoint membership; AuthorizationDigest
// proves the catalog authority crossed its no-return boundary; CatalogCASDigest
// binds the exact old->new catalog compare-and-swap.
type ReplicatedSchemaTransitionAuthority struct {
	RequestDigest       [sha256.Size]byte
	AuthorizationDigest [sha256.Size]byte
	CatalogCASDigest    [sha256.Size]byte
	// Coordination* is identical for every physical replica in the Raft
	// group. Replica-local checkpoint membership remains in the authenticated
	// stage marker and must never make the replicated command bytes diverge.
	CoordinationSequence uint64
	CoordinationSource   [sha256.Size]byte
	CoordinationTarget   [sha256.Size]byte
}

// AppendReplicatedSchemaTransition appends the canonical Raft command for one
// durably prepared target. It is allocation-free when dst has sufficient
// capacity and does not propose or publish anything by itself.
func (a *ReplicatedApply) AppendReplicatedSchemaTransition(
	dst []byte,
	proof ReplicatedSchemaTargetProof,
	authority ReplicatedSchemaTransitionAuthority,
) ([]byte, error) {
	return a.appendReplicatedSchemaTransition(dst, proof, authority, 0)
}

// AppendReplicatedSchemaTransitionAfterEmptySuffix builds against a later
// applied index only after the shard owner has proved that the complete suffix
// after proof.SourceApplied contains empty normal Raft entries. The exact bound
// is persisted with the activation before proposal and rechecked at publish.
func (a *ReplicatedApply) AppendReplicatedSchemaTransitionAfterEmptySuffix(
	dst []byte, proof ReplicatedSchemaTargetProof,
	authority ReplicatedSchemaTransitionAuthority, preCommandApplied uint64,
) ([]byte, error) {
	if preCommandApplied <= proof.SourceApplied {
		return dst, ErrReplicatedSchemaCatalogImage
	}
	return a.appendReplicatedSchemaTransition(dst, proof, authority, preCommandApplied)
}

func (a *ReplicatedApply) appendReplicatedSchemaTransition(
	dst []byte, proof ReplicatedSchemaTargetProof,
	authority ReplicatedSchemaTransitionAuthority, preCommandApplied uint64,
) ([]byte, error) {
	if a == nil || a.database == nil || proof.SourceApplied == 0 ||
		proof.Catalog.SchemaGeneration == 0 || proof.Catalog.RelationManifestDigest == ([32]byte{}) ||
		proof.ApplyContract == ([32]byte{}) || proof.Membership.Sequence == 0 ||
		!proof.Relations.Valid() ||
		authority.RequestDigest == ([32]byte{}) ||
		authority.AuthorizationDigest == ([32]byte{}) ||
		authority.CatalogCASDigest == ([32]byte{}) {
		return dst, ErrReplicatedSchemaCatalogImage
	}
	marker, found, err := readReplicatedSchemaStageMarker(a.database.dataDir)
	if err != nil || !found || marker.schemaGeneration != proof.Catalog.SchemaGeneration ||
		marker.sourceApplied != proof.SourceApplied || marker.membership != proof.Membership ||
		marker.catalogDigest != proof.Catalog.Digest ||
		marker.relationWitness != proof.Relations.Witness ||
		marker.placementDigest != proof.Relations.PlacementDigest ||
		marker.applyContract != proof.ApplyContract || marker.authorization != authority.RequestDigest {
		return dst, errors.Join(err, ErrReplicatedSchemaCatalogImage)
	}
	a.database.mu.RLock()
	defer a.database.mu.RUnlock()
	wantApplied := proof.SourceApplied
	if preCommandApplied != 0 {
		wantApplied = preCommandApplied
	}
	if err := a.checkLocked(); err != nil || a.machine.Applied() != wantApplied {
		return dst, errors.Join(err, ErrReplicatedSchemaCatalogImage)
	}
	base := a.database.catalog.ReplicatedShardStore
	apply := a.database.catalog.ReplicatedApply
	if base == nil || apply == nil ||
		proof.Catalog.SchemaGeneration != base.RelationSchemaGeneration+1 {
		return dst, ErrReplicatedSchemaCatalogImage
	}
	fromManifest, err := a.machine.RelationManifestDigest()
	if err != nil {
		return dst, err
	}
	fromContract, err := a.machine.ApplyContractDigest()
	if err != nil {
		return dst, err
	}
	fromPlacement, err := a.machine.RelationPlacementDigest()
	if err != nil {
		return dst, err
	}
	publication := a.machine.Published()
	if publication.ReplicaSetVersion == 0 {
		return dst, ErrReplicatedSchemaCatalogImage
	}
	from := replicatedStateBindingAt(*base, apply.Placement.Range)
	membershipSequence := proof.Membership.Sequence
	membershipSource, membershipTarget := proof.Membership.Source, proof.Membership.Target
	if authority.CoordinationSequence != 0 || authority.CoordinationSource != ([sha256.Size]byte{}) ||
		authority.CoordinationTarget != ([sha256.Size]byte{}) {
		if authority.CoordinationSequence == 0 || authority.CoordinationSource == ([sha256.Size]byte{}) ||
			authority.CoordinationTarget == ([sha256.Size]byte{}) || authority.CoordinationSource == authority.CoordinationTarget {
			return dst, ErrReplicatedSchemaCatalogImage
		}
		membershipSequence = authority.CoordinationSequence
		membershipSource, membershipTarget = authority.CoordinationSource, authority.CoordinationTarget
	}
	return replicatedstate.AppendSchemaTransition(dst, replicatedstate.SchemaTransition{
		From: from, ToSchemaGeneration: proof.Catalog.SchemaGeneration,
		ExpectedReplicaSetVersion: publication.ReplicaSetVersion,
		MembershipSequence:        membershipSequence,
		MembershipSource:          membershipSource, MembershipTarget: membershipTarget,
		FromManifest: fromManifest, FromApplyContract: fromContract,
		ToManifest: proof.Catalog.RelationManifestDigest, ToApplyContract: proof.ApplyContract,
		FromPlacementDigest: fromPlacement, ToPlacementDigest: proof.Relations.PlacementDigest,
		RequestDigest:       authority.RequestDigest,
		AuthorizationDigest: authority.AuthorizationDigest,
		CatalogCASDigest:    authority.CatalogCASDigest,
	})
}

// ObserveReplicatedSchemaTransition proves command is the exact durable final
// entry of this source generation without reopening relation snapshots.
func (a *ReplicatedApply) ObserveReplicatedSchemaTransition(
	command []byte,
) (uint64, bool, error) {
	if a == nil || a.database == nil {
		return 0, false, ErrReplicatedApplyClosed
	}
	a.database.mu.RLock()
	defer a.database.mu.RUnlock()
	if err := a.checkLocked(); err != nil {
		return 0, false, err
	}
	return a.machine.ObserveSchemaTransition(command)
}

// ObserveReplicatedSchemaTransitionAlias proves an exact committed RF3 command
// against its replica-local catalog-CAS alias.
func (a *ReplicatedApply) ObserveReplicatedSchemaTransitionAlias(
	local, committed []byte,
) (uint64, bool, error) {
	if a == nil || a.database == nil {
		return 0, false, ErrReplicatedApplyClosed
	}
	a.database.mu.RLock()
	defer a.database.mu.RUnlock()
	if err := a.checkLocked(); err != nil {
		return 0, false, err
	}
	return a.machine.ObserveSchemaTransitionAlias(local, committed)
}

// AppendReplicatedSchemaTransitionAlias builds this replica's canonical
// activation envelope for an already-applied RF3 transition. The committed
// command is authoritative for every replicated field; only the local
// CatalogCASDigest is recomputed from this replica's exact source catalog and
// prepared target. Before returning, the method proves the exact committed
// command/index against the live machine through ObserveSchemaTransitionAlias.
// It does not persist, propose, or publish the alias.
func (a *ReplicatedApply) AppendReplicatedSchemaTransitionAlias(
	dst []byte,
	proof ReplicatedSchemaTargetProof,
	authority ReplicatedSchemaTransitionAuthority,
	committed []byte,
	expectedApplied uint64,
) ([]byte, error) {
	if a == nil || a.database == nil || expectedApplied == 0 ||
		proof.SourceApplied == 0 || expectedApplied <= proof.SourceApplied ||
		proof.Catalog.Digest == ([sha256.Size]byte{}) ||
		proof.Catalog.SchemaGeneration == 0 ||
		proof.Catalog.RelationManifestDigest == ([sha256.Size]byte{}) ||
		proof.ApplyContract == ([sha256.Size]byte{}) ||
		proof.Membership.Sequence == 0 || !proof.Relations.Valid() ||
		proof.Witness == ([sha256.Size]byte{}) ||
		authority.RequestDigest == ([sha256.Size]byte{}) ||
		authority.AuthorizationDigest == ([sha256.Size]byte{}) {
		return dst, ErrReplicatedSchemaCatalogImage
	}

	marker, found, err := readReplicatedSchemaStageMarker(a.database.dataDir)
	if err != nil || !found || marker.schemaGeneration != proof.Catalog.SchemaGeneration ||
		marker.sourceApplied != proof.SourceApplied || marker.membership != proof.Membership ||
		marker.catalogDigest != proof.Catalog.Digest || marker.relationWitness != proof.Relations.Witness ||
		marker.placementDigest != proof.Relations.PlacementDigest ||
		marker.applyContract != proof.ApplyContract || marker.authorization != authority.RequestDigest ||
		marker.targetWitness != proof.Witness || proof.Witness != replicatedSchemaTargetProofDigest(proof) {
		return dst, errors.Join(err, ErrReplicatedSchemaCatalogImage)
	}

	committedView, err := replicatedstate.OpenSchemaTransition(committed)
	if err != nil {
		return dst, errors.Join(err, ErrReplicatedSchemaCatalogImage)
	}
	transition := committedView.SchemaTransition
	if transition.RequestDigest != authority.RequestDigest ||
		transition.AuthorizationDigest != authority.AuthorizationDigest ||
		transition.ToSchemaGeneration != proof.Catalog.SchemaGeneration ||
		transition.ToManifest != proof.Catalog.RelationManifestDigest ||
		transition.ToApplyContract != proof.ApplyContract ||
		transition.ToPlacementDigest != proof.Relations.PlacementDigest {
		return dst, ErrReplicatedSchemaCatalogImage
	}
	membershipSequence := proof.Membership.Sequence
	membershipSource, membershipTarget := proof.Membership.Source, proof.Membership.Target
	if authority.CoordinationSequence != 0 || authority.CoordinationSource != ([sha256.Size]byte{}) ||
		authority.CoordinationTarget != ([sha256.Size]byte{}) {
		if authority.CoordinationSequence == 0 || authority.CoordinationSource == ([sha256.Size]byte{}) ||
			authority.CoordinationTarget == ([sha256.Size]byte{}) || authority.CoordinationSource == authority.CoordinationTarget {
			return dst, ErrReplicatedSchemaCatalogImage
		}
		membershipSequence = authority.CoordinationSequence
		membershipSource, membershipTarget = authority.CoordinationSource, authority.CoordinationTarget
	}
	if transition.MembershipSequence != membershipSequence ||
		transition.MembershipSource != membershipSource || transition.MembershipTarget != membershipTarget {
		return dst, ErrReplicatedSchemaCatalogImage
	}

	// Capture a bounded canonical image of the complete in-memory catalog while
	// it is read-locked. The persisted image must match this byte-for-byte before
	// it can supply the replica-local CAS input. ObserveSchemaTransitionAlias
	// does the final locked live-machine proof immediately before returning.
	d := a.database
	d.mu.RLock()
	if err := a.checkLocked(); err != nil || a.machine.Applied() != expectedApplied ||
		d.catalog.ReplicatedShardStore == nil || d.catalog.ReplicatedApply == nil {
		d.mu.RUnlock()
		return dst, errors.Join(err, ErrReplicatedSchemaCatalogImage)
	}
	canonicalBound, err := catalogSizeUpperBound(d.catalog)
	if err != nil {
		d.mu.RUnlock()
		return dst, errors.Join(err, ErrReplicatedSchemaCatalogImage)
	}
	sourceCanonical, err := appendCatalogJSON(make([]byte, 0, canonicalBound), d.catalog)
	if err != nil {
		d.mu.RUnlock()
		return dst, errors.Join(err, ErrReplicatedSchemaCatalogImage)
	}
	dataDir, path := d.dataDir, d.path
	base := *d.catalog.ReplicatedShardStore
	applyProfile := *d.catalog.ReplicatedApply
	from := replicatedStateBindingAt(base, applyProfile.Placement.Range)
	d.mu.RUnlock()

	sourceRaw, sourceFound, err := readCatalogFile(path)
	if err != nil || !sourceFound || !bytes.Equal(sourceRaw, sourceCanonical) {
		return dst, errors.Join(err, ErrReplicatedSchemaCatalogImage)
	}
	sourceCatalog, sourceImage, err := openReplicatedSchemaCatalogImage(sourceRaw)
	if err != nil || sourceImage.SchemaGeneration != transition.From.SchemaGeneration ||
		sourceImage.RelationManifestDigest != transition.FromManifest || transition.From != from ||
		!sourceCatalog.ReplicatedShardStore.Equal(base) ||
		!reflect.DeepEqual(sourceCatalog.ReplicatedApply, &applyProfile) {
		return dst, errors.Join(err, ErrReplicatedSchemaCatalogImage)
	}
	targetRaw, err := readReplicatedSchemaTargetCatalog(dataDir, proof.Catalog)
	if err != nil {
		return dst, errors.Join(err, ErrReplicatedSchemaCatalogImage)
	}
	_, targetImage, err := openReplicatedSchemaCatalogImage(targetRaw)
	if err != nil || targetImage != proof.Catalog {
		return dst, errors.Join(err, ErrReplicatedSchemaCatalogImage)
	}
	sourceDigest := sha256.Sum256(sourceCanonical)
	if sourceImage.Digest != sourceDigest {
		return dst, ErrReplicatedSchemaCatalogImage
	}
	localCAS := replicatedSchemaCatalogCASDigest(
		sourceDigest, targetImage.Digest, authority.RequestDigest, authority.AuthorizationDigest,
	)
	if localCAS == ([sha256.Size]byte{}) {
		return dst, ErrReplicatedSchemaCatalogImage
	}
	transition.CatalogCASDigest = localCAS
	start := len(dst)
	local, err := replicatedstate.AppendSchemaTransition(dst, transition)
	if err != nil {
		return dst, err
	}
	command := local[start:]
	applied, observed, err := a.ObserveReplicatedSchemaTransitionAlias(command, committed)
	if err != nil || !observed || applied != expectedApplied {
		return dst, errors.Join(err, ErrReplicatedSchemaCatalogImage)
	}
	return local, nil
}
