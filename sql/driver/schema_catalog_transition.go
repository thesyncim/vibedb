package driver

import (
	"crypto/sha256"
	"errors"

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
}

// AppendReplicatedSchemaTransition appends the canonical Raft command for one
// durably prepared target. It is allocation-free when dst has sufficient
// capacity and does not propose or publish anything by itself.
func (a *ReplicatedApply) AppendReplicatedSchemaTransition(
	dst []byte,
	proof ReplicatedSchemaTargetProof,
	authority ReplicatedSchemaTransitionAuthority,
) ([]byte, error) {
	if a == nil || a.database == nil || proof.SourceApplied == 0 ||
		proof.Catalog.SchemaGeneration == 0 || proof.Catalog.RelationManifestDigest == ([32]byte{}) ||
		proof.ApplyContract == ([32]byte{}) || proof.Membership.Sequence == 0 ||
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
		marker.applyContract != proof.ApplyContract || marker.authorization != authority.RequestDigest {
		return dst, errors.Join(err, ErrReplicatedSchemaCatalogImage)
	}
	a.database.mu.RLock()
	defer a.database.mu.RUnlock()
	if err := a.checkLocked(); err != nil || a.machine.Applied() != proof.SourceApplied {
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
	publication := a.machine.Published()
	if publication.ReplicaSetVersion == 0 {
		return dst, ErrReplicatedSchemaCatalogImage
	}
	from := replicatedStateBindingAt(*base, apply.Placement.Range)
	return replicatedstate.AppendSchemaTransition(dst, replicatedstate.SchemaTransition{
		From: from, ToSchemaGeneration: proof.Catalog.SchemaGeneration,
		ExpectedReplicaSetVersion: publication.ReplicaSetVersion,
		MembershipSequence:        proof.Membership.Sequence,
		MembershipSource:          proof.Membership.Source, MembershipTarget: proof.Membership.Target,
		FromManifest: fromManifest, FromApplyContract: fromContract,
		ToManifest: proof.Catalog.RelationManifestDigest, ToApplyContract: proof.ApplyContract,
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
