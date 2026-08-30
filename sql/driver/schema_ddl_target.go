package driver

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/schemachange"
	"github.com/thesyncim/vibedb/query"
	sqlast "github.com/thesyncim/vibedb/sql"
	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
)

// ReplicatedSchemaDDLTarget is an unpublished, certified schema successor.
// The coordinator must retain Catalog and SourceApplied in its operation
// journal before preparing the existing replicated schema rollout. Building
// this artifact alone never authorizes activation or changes serving data.
type ReplicatedSchemaDDLTarget struct {
	Catalog []byte
	Proof   ReplicatedSchemaTargetProof
	NoOp    bool
}

// ValidateReplicatedSchemaDDLTarget checks a detached build receipt. It does
// not open images or grant serving authority; prepare still verifies the files.
func ValidateReplicatedSchemaDDLTarget(target ReplicatedSchemaDDLTarget, applied, sourceSchema uint64) error {
	if applied == 0 || sourceSchema == 0 || sourceSchema == ^uint64(0) {
		return ErrReplicatedSchemaCatalogImage
	}
	if target.NoOp {
		if len(target.Catalog) != 0 || target.Proof != (ReplicatedSchemaTargetProof{}) {
			return ErrReplicatedSchemaCatalogImage
		}
		return nil
	}
	image, err := ValidateReplicatedSchemaCatalogImage(target.Catalog)
	proof := target.Proof
	if err != nil || image.SchemaGeneration != sourceSchema+1 || proof.Catalog != image ||
		proof.SourceApplied != applied || proof.Membership != (durable.CheckpointMembershipWitness{}) ||
		proof.ApplyContract == ([32]byte{}) || proof.Relations.Witness == ([32]byte{}) ||
		proof.Witness != replicatedSchemaTargetProofDigest(proof) {
		return errors.Join(err, ErrReplicatedSchemaCatalogImage)
	}
	return nil
}

// BuildReplicatedSchemaDDLTarget builds CREATE/DROP INDEX and TRUNCATE images
// from one exact data cut. The caller must first fence new distributed writes
// and obtain expectedApplied from a quorum barrier. A concurrent publication
// causes a conflict, never a target that silently omits an acknowledged write.
// This is a maintenance/offline primitive, NOT an online DDL implementation.
// Do not expose it as online SQL by holding a route gate across the build.
// An online coordinator must reconcile writes made after its snapshot before
// certifying the target, and bound the final cutover independently of row count.
//
// All row work is outside the serving catalog lock. Only one bounded batch of
// documents is retained; every relation gets a fresh physical identity, while
// the WAL, member, logical relation IDs, retry state and capture storage stay
// unchanged. The returned files belong to the rollout operation, not a local
// DDL commit. ALTER and DROP TABLE require their own schema/namespace lowering.
func (a *ReplicatedApply) BuildReplicatedSchemaDDLTarget(
	ctx context.Context, expectedApplied uint64, text string,
) (ReplicatedSchemaDDLTarget, error) {
	return a.buildReplicatedSchemaDDLTarget(ctx, expectedApplied, text, nil)
}

func (a *ReplicatedApply) buildReplicatedSchemaDDLTarget(
	ctx context.Context, expectedApplied uint64, text string, reserve func(*catalogFile) error,
) (result ReplicatedSchemaDDLTarget, resultErr error) {
	return a.buildReplicatedSchemaDDLImage(ctx, expectedApplied, text, reserve, nil)
}

type schemaDDLOnlineCopy struct {
	capture *schemachange.SourceCapture
	source  []byte
	cursor  schemachange.Cursor
}

func (a *ReplicatedApply) buildReplicatedSchemaDDLImage(
	ctx context.Context, expectedApplied uint64, text string, reserve func(*catalogFile) error, online *schemaDDLOnlineCopy,
) (result ReplicatedSchemaDDLTarget, resultErr error) {
	if ctx == nil || expectedApplied == 0 || a == nil || a.database == nil ||
		len(text) == 0 || len(text) > ReplicatedChildSchemaMaxBytes {
		return result, ErrReplicatedSchemaCatalogImage
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	statement, err := query.PrepareDML(text)
	if err != nil {
		return result, err
	}
	defer statement.Release()
	var cut replicatedstate.DataReadCut
	core := a.database
	core.mu.RLock()
	err = a.checkLocked()
	if err == nil {
		err = a.checkActivationBaseLocked()
	}
	if err == nil && online == nil && a.machine.Applied() != expectedApplied {
		err = ErrTransactionConflict
	}
	var raw []byte
	if err == nil {
		raw, err = appendCatalogJSON(nil, core.catalog)
	}
	if err == nil {
		err = a.machine.DataReadCutInto(nil, expectedApplied, &cut)
	}
	core.mu.RUnlock()
	if err != nil {
		return result, err
	}
	defer func() { resultErr = errors.Join(resultErr, cut.Close()) }()
	if online == nil && cut.Fence().Applied != expectedApplied {
		return result, ErrTransactionConflict
	}
	if online != nil {
		online.cursor, err = online.capture.CursorAt(cut.Fence())
		if err != nil || !bytes.Equal(raw, online.source) {
			return result, errors.Join(err, ErrTransactionConflict)
		}
	}
	var decoded catalogFileVibe
	if err := decodeCatalogJSON(raw, &decoded); err != nil {
		return result, err
	}
	target := catalogFile(decoded)
	truncate, noOp, err := lowerReplicatedSchemaDDL(&target, statement)
	if err != nil || noOp {
		return ReplicatedSchemaDDLTarget{NoOp: noOp}, err
	}
	identity := target.ReplicatedShardStore
	if identity.RelationSchemaGeneration == ^uint64(0) || identity.Binding.Authority.SchemaGeneration == ^uint64(0) {
		return result, ErrReplicatedSchemaCatalogImage
	}
	identity.RelationSchemaGeneration++
	identity.Binding.Authority.SchemaGeneration++
	directory, err := a.ReplicatedSchemaTargetDirectory()
	if err != nil {
		return result, err
	}
	// This private catalog is only an identity allocator; it is never serving.
	staged := &database{dataDir: directory, catalog: target}
	for ordinal := range identity.Relations {
		relation := &identity.Relations[ordinal]
		storage, err := staged.newStorageIdentityLocked()
		if err != nil {
			return result, err
		}
		meta := target.Tables[relation.Table]
		meta.Storage, relation.Storage, meta.Materialized = storage, storage, true
	}
	refreshSchemaDDLTargetIdentity(&target)
	if reserve != nil {
		if err := reserve(&target); err != nil {
			return result, err
		}
	}
	created := make([]string, 0, len(identity.Relations))
	defer func() {
		if resultErr == nil {
			return
		}
		// No membership or rollout record can refer to this failed build: its
		// catalog has not escaped the method. Remove only files we created.
		for _, path := range created {
			for _, name := range []string{durable.RecoveryJournalPath(path), path} {
				if err := os.Remove(name); err != nil && !os.IsNotExist(err) {
					resultErr = errors.Join(resultErr, err)
				}
			}
		}
		file, err := os.Open(directory)
		if err == nil {
			err = errors.Join(file.Sync(), file.Close())
		}
		resultErr = errors.Join(resultErr, err)
	}()
	for ordinal := range identity.Relations {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		relation := &identity.Relations[ordinal]
		meta := target.Tables[relation.Table]
		candidate := &table{meta: meta}
		candidate.schema, err = compileSchemaMeta(meta.Schema)
		if err != nil {
			return result, err
		}
		path := filepath.Join(directory, relation.Storage+".vjc")
		file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
		if err != nil {
			return result, err
		}
		created = append(created, path)
		collection, err := durable.Create(file, durableOptions(candidate))
		if err == nil && !truncate {
			err = copyReplicatedSchemaRelation(ctx, &cut, replication.RelationID(ordinal+1), collection)
		}
		if collection != nil {
			err = errors.Join(err, collection.Close())
		}
		err = errors.Join(err, file.Close())
		if err != nil {
			return result, err
		}
	}
	raw, err = appendCatalogJSON(nil, target)
	if err != nil {
		return result, err
	}
	if online != nil {
		// A mutable shadow is not a certified rollout target. Its distinct
		// public receipt carries the snapshot cursor, never a target proof.
		d, err := online.capture.Descriptor()
		if err != nil || d.Abort != schemachange.NotAborted {
			return result, errors.Join(err, ErrTransactionConflict)
		}
		file, err := os.Open(directory)
		if err == nil {
			err = errors.Join(file.Sync(), file.Close())
		}
		return ReplicatedSchemaDDLTarget{Catalog: raw}, err
	}
	proof, err := a.CertifyReplicatedSchemaTarget(raw)
	if err != nil {
		return result, err
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	core.mu.RLock()
	err = a.checkLocked()
	if err == nil && (a.machine.Applied() != expectedApplied || proof.SourceApplied != expectedApplied) {
		err = ErrTransactionConflict
	}
	core.mu.RUnlock()
	if err != nil {
		return result, err
	}
	file, err := os.Open(directory)
	if err == nil {
		err = errors.Join(file.Sync(), file.Close())
	}
	if err != nil {
		return result, err
	}
	return ReplicatedSchemaDDLTarget{Catalog: raw, Proof: proof}, nil
}

func refreshSchemaDDLTargetIdentity(target *catalogFile) {
	identity := target.ReplicatedShardStore
	identity.UserStorage = identity.Relations[0].Storage
	meta := target.Tables[identity.UserTable]
	identity.Relations[0].LocalIndexDigest = replicatedLocalIndexDigest(meta.Indexes)
	identity.Relations[0].SchemaDigest = replicatedSchemaDigest(meta.Schema)
	identity.RelationManifestDigest = replicatedRelationManifestDigest(*identity)
	target.ReplicatedApply.ValidationDigest = replicatedApplyProfileDigest(*identity, target.ReplicatedApply.Placement)
}

func lowerReplicatedSchemaDDL(target *catalogFile, statement *query.DMLStatement) (truncate, noOp bool, err error) {
	if target.ReplicatedShardStore == nil || target.ReplicatedApply == nil {
		return false, false, ErrReplicatedSchemaCatalogImage
	}
	name := target.ReplicatedShardStore.UserTable
	meta := target.Tables[name]
	if meta == nil {
		return false, false, ErrReplicatedSchemaCatalogImage
	}
	tree := statement.Tree()
	switch tree.Kind {
	case sqlast.KindAlterTable:
		definition, err := statement.LowerAlterTable()
		if err != nil {
			return false, false, err
		}
		if definition.Table != name {
			return false, false, ErrTableNotFound
		}
		if meta.Schema == nil {
			// A schemaless SQL table still derives physical identity from its
			// declared primary-key path. The first additive field makes the store
			// schema explicit, so carry that previously driver-enforced invariant
			// into the compiled schema rather than accidentally weakening it.
			meta.Schema = &schemaMeta{Root: uint16(store.SchemaObject), Fields: []schemaFieldMeta{{
				Path: meta.PrimaryKey, Types: uint16(store.SchemaBool | store.SchemaNumber | store.SchemaString), Required: true,
			}}}
		}
		for _, existing := range meta.Schema.Fields {
			if existing.Path == definition.Field.Path {
				if definition.IfNotExists {
					return false, true, nil
				}
				return false, false, fmt.Errorf("vibedb: column already exists: %s", tree.AlterTable.Column.Path.Spec())
			}
		}
		meta.Schema.Fields = append(meta.Schema.Fields, schemaFieldMeta{
			Path: definition.Field.Path, Types: uint16(definition.Field.Types), Required: definition.Field.Required,
		})
	case sqlast.KindCreateIndex:
		definition, err := statement.LowerIndex()
		if err != nil {
			return false, false, err
		}
		if definition.Table != name {
			return false, false, ErrTableNotFound
		}
		if _, err := store.CompileExactIndex(definition.Definition); err != nil {
			return false, false, err
		}
		for _, existing := range meta.Indexes {
			if existing.Name == definition.Definition.Name {
				if definition.IfNotExists {
					return false, true, nil
				}
				return false, false, ErrIndexExists
			}
		}
		meta.Indexes = append(meta.Indexes, indexMeta{Name: definition.Definition.Name, Paths: definition.Definition.Paths})
	case sqlast.KindDropIndex:
		if tree.DropIndex.HasTable && tree.DropIndex.Table != name {
			return false, false, ErrTableNotFound
		}
		for i, index := range meta.Indexes {
			if index.Name == tree.DropIndex.Name {
				meta.Indexes = append(meta.Indexes[:i], meta.Indexes[i+1:]...)
				return false, false, nil
			}
		}
		if tree.DropIndex.IfExists {
			return false, true, nil
		}
		return false, false, ErrIndexNotFound
	case sqlast.KindTruncate:
		if tree.Truncate.Table != name {
			return false, false, ErrTableNotFound
		}
		return true, false, nil
	default:
		return false, false, fmt.Errorf("%w: DDL target requires ALTER TABLE ADD COLUMN, CREATE INDEX, DROP INDEX or TRUNCATE", ErrReplicatedSchemaCatalogImage)
	}
	return false, false, nil
}

func copyReplicatedSchemaRelation(ctx context.Context, cut *replicatedstate.DataReadCut, id replication.RelationID, target *durable.Collection) error {
	snapshot, ok := cut.Relation(id)
	if !ok {
		return ErrReplicatedSchemaCatalogImage
	}
	batch := make([]seedDocument, 0, target.MaxBatchDocuments())
	batchBytes := 0
	flush := func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if len(batch) == 0 {
			return nil
		}
		if err := target.Update(func(write *durable.WriteBatch) error {
			for _, row := range batch {
				if err := write.Put([]byte(row.key), row.document); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
		clear(batch)
		batch, batchBytes = batch[:0], 0
		return nil
	}
	err := snapshot.RangeRaw(func(key, value []byte) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !cut.OwnsKey(id, key) {
			return nil
		}
		size := len(key) + len(value)
		if size > target.MaxBatchBytes() {
			return durable.ErrBatchTooLarge
		}
		if len(batch) == cap(batch) || batchBytes > target.MaxBatchBytes()-size {
			if err := flush(); err != nil {
				return err
			}
		}
		batch = append(batch, seedDocument{key: string(key), document: append([]byte(nil), value...)})
		batchBytes += size
		return nil
	})
	if err == nil {
		err = flush()
	}
	return err
}
