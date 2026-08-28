package driver

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/store/durable"
	"github.com/thesyncim/vibejson"
)

var replicatedSchemaTargetProofDomain = []byte(
	"vibedb/sql/replicated-schema-target-proof\x00",
)

// ReplicatedSchemaTargetProof binds the canonical catalog image to the exact
// immutable relation images and deterministic apply contract prepared for it.
type ReplicatedSchemaTargetProof struct {
	Catalog       ReplicatedSchemaCatalogImage
	Relations     replicatedstate.RelationImageCertificate
	SourceApplied uint64
	Membership    durable.CheckpointMembershipWitness
	ApplyContract [sha256.Size]byte
	Witness       [sha256.Size]byte
}

// CertifyReplicatedSchemaTarget opens only non-serving target storage names
// from ReplicatedSchemaTargetDirectory and computes their bounded
// content/cardinality/placement certificate. It does not publish the catalog,
// change checkpoint membership, or alter serving handles.
//
// Every target relation must use a fresh physical identity. This is both the
// immutable-generation fence and the prerequisite for crash-safe checkpoint
// membership replacement. A newly introduced global-index relation must be
// nonempty; its content is expected to have been produced by the distributed
// backfill path, never synthesized as an empty shortcut here.
func (a *ReplicatedApply) CertifyReplicatedSchemaTarget(
	raw []byte,
) (proof ReplicatedSchemaTargetProof, resultErr error) {
	return a.certifyReplicatedSchemaTarget(raw, nil)
}

// PrepareReplicatedSchemaTarget performs the same immutable image audit and,
// only if the live machine still equals expectedApplied, prepares the exact
// checkpoint replacement membership. The final database lock covers only the
// O(relation-count) membership certificate; the O(row-count) audit remains off
// the serving path. expectedApplied must come from the backfill/capture barrier
// that produced the target images.
func (a *ReplicatedApply) PrepareReplicatedSchemaTarget(
	raw []byte,
	expectedApplied uint64,
	authorization [sha256.Size]byte,
) (ReplicatedSchemaTargetProof, error) {
	if expectedApplied == 0 || authorization == ([sha256.Size]byte{}) {
		return ReplicatedSchemaTargetProof{}, ErrReplicatedSchemaCatalogImage
	}
	return a.certifyReplicatedSchemaTarget(raw, func(
		staged *database,
		target ReplicatedShardStoreIdentity,
		proof *ReplicatedSchemaTargetProof,
	) error {
		a.database.mu.Lock()
		defer a.database.mu.Unlock()
		if err := a.checkLocked(); err != nil || a.machine.Applied() != expectedApplied ||
			a.database.checkpointGroup == nil {
			return errors.Join(err, ErrReplicatedSchemaCatalogImage)
		}
		if prior, found, err := readReplicatedSchemaStageMarker(a.database.dataDir); err != nil {
			return err
		} else if found && prior.catalogDigest == proof.Catalog.Digest {
			if prior.authorization != authorization || prior.sourceApplied != expectedApplied {
				return ErrReplicatedSchemaCatalogImage
			}
		} else if found && prior.catalogDigest != proof.Catalog.Digest {
			retired, err := schemaProofRetired(a.database.dataDir, prior.catalogDigest, prior.schemaGeneration)
			if err != nil || !retired {
				return errors.Join(err, ErrReplicatedSchemaCatalogImage)
			}
		}
		if err := ensureReplicatedSchemaOrigin(a.database.path); err != nil {
			return err
		}
		staged.replicatedApplyCollection = a.database.replicatedApplyCollection
		staged.replicatedCaptureCollection = a.database.replicatedCaptureCollection
		members, err := replicatedApplyCheckpointMembers(target, staged)
		if err != nil {
			return err
		}
		witness, err := a.database.checkpointGroup.PrepareMembershipTransition(
			members, authorization,
		)
		if err != nil {
			return err
		}
		proof.SourceApplied = expectedApplied
		proof.Membership = witness
		proof.Witness = replicatedSchemaTargetProofDigest(*proof)
		if err := writeReplicatedSchemaTargetCatalog(
			a.database.dataDir, raw, proof.Catalog,
		); err != nil {
			return err
		}
		storages, err := schemaStageStorageIDs(target)
		if err != nil {
			return err
		}
		if a.database.catalog.ReplicatedShardStore == nil {
			return ErrReplicatedSchemaCatalogImage
		}
		sourceStorages, err := schemaStageStorageIDs(*a.database.catalog.ReplicatedShardStore)
		if err != nil {
			return err
		}
		return writeReplicatedSchemaStageMarker(a.database.dataDir, replicatedSchemaStageMarker{
			schemaGeneration: target.RelationSchemaGeneration,
			sourceApplied:    expectedApplied, membership: witness,
			catalogDigest:   proof.Catalog.Digest,
			relationWitness: proof.Relations.Witness,
			placementDigest: proof.Relations.PlacementDigest,
			applyContract:   proof.ApplyContract,
			authorization:   authorization, targetWitness: proof.Witness,
			storages: storages, sourceStorages: sourceStorages,
		})
	})
}

// RecoverPreparedReplicatedSchemaTarget reconstructs the bounded target proof
// from the exact canonical catalog plus the durable membership marker. The
// relation images are recertified off the serving path; no row material is
// retained after return.
func (a *ReplicatedApply) RecoverPreparedReplicatedSchemaTarget(
	raw []byte,
	requestDigest [sha256.Size]byte,
) (ReplicatedSchemaTargetProof, error) {
	if a == nil || a.database == nil || requestDigest == ([32]byte{}) {
		return ReplicatedSchemaTargetProof{}, ErrReplicatedSchemaCatalogImage
	}
	proof, err := a.CertifyReplicatedSchemaTarget(raw)
	if err != nil {
		return proof, err
	}
	marker, found, err := readReplicatedSchemaStageMarker(a.database.dataDir)
	if err != nil || !found || marker.authorization != requestDigest ||
		marker.catalogDigest != proof.Catalog.Digest ||
		marker.relationWitness != proof.Relations.Witness ||
		marker.placementDigest != proof.Relations.PlacementDigest ||
		marker.applyContract != proof.ApplyContract {
		return proof, errors.Join(err, ErrReplicatedSchemaCatalogImage)
	}
	proof.SourceApplied = marker.sourceApplied
	proof.Membership = marker.membership
	proof.Witness = replicatedSchemaTargetProofDigest(proof)
	if proof.Witness != marker.targetWitness {
		return proof, ErrReplicatedSchemaCatalogImage
	}
	if _, err := readReplicatedSchemaTargetCatalog(a.database.dataDir, proof.Catalog); err != nil {
		return ReplicatedSchemaTargetProof{}, err
	}
	if err := fenceReplicatedSchemaFiles(a.database.dataDir,
		replicatedSchemaStageMarkerName, replicatedSchemaTargetCatalogName); err != nil {
		return ReplicatedSchemaTargetProof{}, err
	}
	return proof, nil
}

// ObservePreparedReplicatedSchemaTarget settles a durable prepare after a
// restart without opening the serving apply machine or rescanning relation
// rows. The returned witness is covered by the stage marker checksum.
func ObservePreparedReplicatedSchemaTarget(
	path string, raw []byte, requestDigest [sha256.Size]byte,
) ([sha256.Size]byte, bool, error) {
	if path == "" || requestDigest == ([sha256.Size]byte{}) {
		return [sha256.Size]byte{}, false, ErrReplicatedSchemaCatalogImage
	}
	image, err := ValidateReplicatedSchemaCatalogImage(raw)
	if err != nil {
		return [sha256.Size]byte{}, false, err
	}
	absolute, err := canonicalCatalogPath(path)
	if err != nil {
		return [sha256.Size]byte{}, false, err
	}
	marker, found, err := readReplicatedSchemaStageMarker(absolute + ".tables")
	if err != nil || !found {
		return [sha256.Size]byte{}, found, err
	}
	if marker.catalogDigest != image.Digest {
		retired, err := schemaProofRetired(absolute+".tables", marker.catalogDigest, marker.schemaGeneration)
		if err != nil || retired {
			return [sha256.Size]byte{}, false, err
		}
	}
	if marker.authorization != requestDigest || marker.catalogDigest != image.Digest ||
		marker.schemaGeneration != image.SchemaGeneration || marker.targetWitness == ([32]byte{}) {
		return [sha256.Size]byte{}, false, ErrReplicatedSchemaCatalogImage
	}
	if _, err := readReplicatedSchemaTargetCatalog(absolute+".tables", image); err != nil {
		return [sha256.Size]byte{}, false, err
	}
	if err := fenceReplicatedSchemaFiles(absolute+".tables",
		replicatedSchemaStageMarkerName, replicatedSchemaTargetCatalogName); err != nil {
		return [sha256.Size]byte{}, false, err
	}
	return marker.targetWitness, true, nil
}

func (a *ReplicatedApply) certifyReplicatedSchemaTarget(
	raw []byte,
	settle func(*database, ReplicatedShardStoreIdentity, *ReplicatedSchemaTargetProof) error,
) (proof ReplicatedSchemaTargetProof, resultErr error) {
	if a == nil || a.database == nil || a.owner == nil {
		return proof, ErrReplicatedApplyClosed
	}
	// Mutable online targets cannot enter the immutable rollout path. The
	// online coordinator must first freeze/certify them at its exact cut.
	if err := rejectMutableSchemaShadow(a.database.dataDir); err != nil {
		return proof, err
	}
	targetCatalog, image, err := openReplicatedSchemaCatalogImage(raw)
	if err != nil {
		return proof, err
	}
	a.database.mu.RLock()
	if err = a.checkLocked(); err != nil {
		a.database.mu.RUnlock()
		return proof, err
	}
	current := a.database.catalog.ReplicatedShardStore
	if current == nil || targetCatalog.ReplicatedShardStore == nil ||
		targetCatalog.ReplicatedApply == nil || a.database.catalog.ShardStore == nil {
		a.database.mu.RUnlock()
		return proof, ErrReplicatedSchemaCatalogImage
	}
	wantBinding := current.Binding
	wantBinding.Authority.SchemaGeneration++
	targetIdentity := targetCatalog.ReplicatedShardStore.Clone()
	wantApply := *a.database.catalog.ReplicatedApply
	wantApply.ValidationDigest = targetCatalog.ReplicatedApply.ValidationDigest
	if targetIdentity.Binding != wantBinding ||
		targetIdentity.RelationSchemaGeneration != wantBinding.Authority.SchemaGeneration ||
		targetIdentity.LogID != current.LogID || targetIdentity.UserTable != current.UserTable ||
		targetCatalog.ShardStore == nil ||
		*targetCatalog.ShardStore != *a.database.catalog.ShardStore ||
		*targetCatalog.ReplicatedApply != wantApply {
		a.database.mu.RUnlock()
		return proof, ErrReplicatedSchemaCatalogImage
	}
	currentStorages := make(map[string]struct{}, len(current.Relations)+2)
	for ordinal := range current.Relations {
		currentStorages[current.Relations[ordinal].Storage] = struct{}{}
	}
	currentStorages[a.database.catalog.ReplicatedApply.Storage] = struct{}{}
	currentStorages[a.database.catalog.ReplicatedApply.CaptureStorage] = struct{}{}
	dataDir := filepath.Join(a.database.dataDir, replicatedSchemaTargetsDirectory)
	a.database.mu.RUnlock()
	directory, err := os.Lstat(dataDir)
	if err != nil || !directory.IsDir() || directory.Mode()&os.ModeSymlink != 0 {
		return proof, errors.Join(ErrReplicatedSchemaCatalogImage, err)
	}

	staged := &database{catalog: targetCatalog, tables: make(map[string]*table, len(targetIdentity.Relations))}
	opened := make([]*table, 0, len(targetIdentity.Relations))
	defer func() {
		for i := len(opened) - 1; i >= 0; i-- {
			resultErr = errors.Join(resultErr, opened[i].collection.Close(), opened[i].file.Close())
		}
	}()
	for ordinal := range targetIdentity.Relations {
		relation := targetIdentity.Relations[ordinal]
		if _, aliasesServing := currentStorages[relation.Storage]; aliasesServing {
			return proof, fmt.Errorf(
				"%w: target relation %q reuses serving storage",
				ErrReplicatedSchemaCatalogImage, relation.Table,
			)
		}
		meta := targetCatalog.Tables[relation.Table]
		if meta == nil || meta.Storage != relation.Storage || !meta.Materialized {
			return proof, ErrReplicatedSchemaCatalogImage
		}
		staged.dataDir = dataDir
		path := staged.tablePathForMeta(meta)
		file, openErr := os.OpenFile(path, os.O_RDWR, 0)
		if openErr != nil {
			return proof, openErr
		}
		candidate := &table{meta: meta, file: file}
		candidate.schema, openErr = compileSchemaMeta(meta.Schema)
		if openErr != nil {
			_ = file.Close()
			return proof, openErr
		}
		options := durableOptions(candidate)
		options.Indexes = nil
		candidate.collection, openErr = durable.Open(file, options)
		if openErr != nil {
			_ = file.Close()
			return proof, openErr
		}
		if relation.Kind == ReplicatedShardRelationJSON {
			candidate.primary, openErr = vibejson.CompilePointer(meta.PrimaryKey)
			if openErr != nil {
				_ = candidate.collection.Close()
				_ = file.Close()
				return proof, openErr
			}
		}
		staged.tables[relation.Table] = candidate
		opened = append(opened, candidate)
	}
	applyIdentity := targetCatalog.ReplicatedApply.identity()
	relations, err := replicatedApplyRelations(targetIdentity, applyIdentity, staged, a)
	if err != nil {
		return proof, err
	}
	binding := replicatedStateBindingAt(targetIdentity, applyIdentity.Placement.Range)
	certificate, err := replicatedstate.CertifyRelationImages(binding, relations)
	if err != nil || certificate.ManifestDigest != image.RelationManifestDigest {
		return proof, errors.Join(err, ErrReplicatedSchemaCatalogImage)
	}
	for ordinal := 1; ordinal < len(targetIdentity.Relations); ordinal++ {
		relation := targetIdentity.Relations[ordinal]
		if relation.Kind != ReplicatedShardRelationGlobalIndex ||
			replicatedGlobalIndexExists(current, relation) {
			continue
		}
		if certificate.NonEmptyRelations&(uint64(1)<<uint(ordinal)) == 0 {
			return proof, fmt.Errorf(
				"%w: new global-index relation %q has no certified backfill image",
				ErrReplicatedSchemaCatalogImage, relation.Table,
			)
		}
	}
	contract, err := replicatedstate.RelationBundleApplyContractDigest(
		binding, relations, replicatedstate.BundleApplyContractOptions{
			MaxSessions: applyIdentity.MaxSessions, RetryWindow: applyIdentity.RetryWindow,
			RequestLedgerCapacityBytes:       applyIdentity.RequestLedgerCapacityBytes,
			RequestLedgerCleanupReserveBytes: applyIdentity.RequestLedgerCleanupReserveBytes,
			RequestLedgerRange: replicatedstate.RequestLedgerRange{
				Start:    requestledger.LedgerHome(applyIdentity.RequestLedgerRangeStart),
				End:      requestledger.LedgerHome(applyIdentity.RequestLedgerRangeEnd),
				Identity: requestledger.Digest(applyIdentity.RequestLedgerRangeIdentity),
			},
		},
	)
	if err != nil {
		return proof, err
	}
	proof = ReplicatedSchemaTargetProof{
		Catalog: image, Relations: certificate, ApplyContract: contract,
	}
	if settle != nil {
		if err := settle(staged, targetIdentity, &proof); err != nil {
			return proof, err
		}
	} else {
		a.database.mu.RLock()
		if err := a.checkLocked(); err == nil {
			proof.SourceApplied = a.machine.Applied()
		}
		a.database.mu.RUnlock()
	}
	proof.Witness = replicatedSchemaTargetProofDigest(proof)
	return proof, nil
}

func replicatedSchemaTargetProofDigest(proof ReplicatedSchemaTargetProof) [sha256.Size]byte {
	h := sha256.New()
	_, _ = h.Write(replicatedSchemaTargetProofDomain)
	_, _ = h.Write(proof.Catalog.Digest[:])
	_, _ = h.Write(proof.Catalog.LocalRelationManifestDigest[:])
	_, _ = h.Write(proof.Catalog.ApplyProfileDigest[:])
	_, _ = h.Write(proof.Relations.Witness[:])
	_, _ = h.Write(proof.ApplyContract[:])
	var fixed [24]byte
	binary.LittleEndian.PutUint64(fixed[0:8], proof.SourceApplied)
	binary.LittleEndian.PutUint64(fixed[8:16], proof.Membership.Sequence)
	_, _ = h.Write(fixed[:16])
	_, _ = h.Write(proof.Membership.Source[:])
	_, _ = h.Write(proof.Membership.Target[:])
	var result [sha256.Size]byte
	_ = h.Sum(result[:0])
	return result
}

func replicatedGlobalIndexExists(
	current *ReplicatedShardStoreIdentity,
	target ReplicatedShardRelationIdentity,
) bool {
	if current == nil || target.Kind != ReplicatedShardRelationGlobalIndex {
		return false
	}
	for ordinal := 1; ordinal < len(current.Relations); ordinal++ {
		relation := current.Relations[ordinal]
		if relation.Kind == target.Kind && relation.IndexID == target.IndexID &&
			relation.Incarnation == target.Incarnation && relation.Table == target.Table {
			return true
		}
	}
	return false
}
