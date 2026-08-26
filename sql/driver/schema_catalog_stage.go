package driver

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"os"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/store/durable"
	"github.com/thesyncim/vibejson"
)

var replicatedSchemaTargetProofDomain = []byte(
	"vibedb/sql/replicated-schema-target-proof\x00",
)

// ReplicatedSchemaTargetProof binds the canonical catalog image to the exact
// immutable relation images already prepared for it. ApplyContract is settled
// separately when the target machine opens after the ordered Raft transition.
type ReplicatedSchemaTargetProof struct {
	Catalog   ReplicatedSchemaCatalogImage
	Relations replicatedstate.RelationImageCertificate
	Witness   [sha256.Size]byte
}

// CertifyReplicatedSchemaTarget opens only non-serving target storage names
// from the current shard's pinned data namespace and computes their bounded
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
	if a == nil || a.database == nil || a.owner == nil {
		return proof, ErrReplicatedApplyClosed
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
	dataDir := a.database.dataDir
	a.database.mu.RUnlock()

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
	h := sha256.New()
	_, _ = h.Write(replicatedSchemaTargetProofDomain)
	_, _ = h.Write(image.Digest[:])
	_, _ = h.Write(image.LocalRelationManifestDigest[:])
	_, _ = h.Write(image.ApplyProfileDigest[:])
	_, _ = h.Write(certificate.Witness[:])
	proof = ReplicatedSchemaTargetProof{Catalog: image, Relations: certificate}
	_ = h.Sum(proof.Witness[:0])
	return proof, nil
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
