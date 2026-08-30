package driver

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"

	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/schemachange"
	"github.com/thesyncim/vibedb/store/durable"
)

// ReplayReplicatedSchemaDDLShadow applies at most maxEntries (1..1024) captured
// publications, never chasing beyond the head observed at entry. Per-relation
// commits may survive independently; the cursor is published only after all
// effects of an entry are durable. Exact after-value comparisons make retries
// safe after a crash between those commits and cursor publication. Otherwise
// the fixed before witness must match, or the build fails without touching the
// source. Source serving locks are not held during target I/O.
func (a *ReplicatedApply) ReplayReplicatedSchemaDDLShadow(ctx context.Context, operation [32]byte, maxEntries int) (result ReplicatedSchemaDDLShadow, resultErr error) {
	if operation == [32]byte{} || maxEntries < 1 || maxEntries > 1024 {
		return result, ErrReplicatedSchemaDDLConflict
	}
	root, unlock, err := a.lockSchemaDDLShadow(ctx)
	if err != nil {
		return result, err
	}
	defer unlock()
	record, found, err := readSchemaDDLShadowRecord(root)
	if err != nil || !found || !record.Ready || record.Shadow.Operation != operation {
		return result, errors.Join(err, ErrReplicatedSchemaDDLConflict)
	}
	source, generation, err := a.schemaDDLShadowSource()
	if err != nil || generation != record.SourceGeneration || sha256.Sum256(source) != record.SourceDigest {
		return result, errors.Join(err, ErrReplicatedSchemaDDLConflict)
	}
	d, err := a.ReplicatedSchemaCaptureDescriptor(operation)
	if err != nil || d.Config != record.Capture || d.Abort != schemachange.NotAborted ||
		record.Shadow.Cursor.Publication.Applied > d.Head.Publication.Applied ||
		record.Shadow.Cursor.Publication.Applied == d.Head.Publication.Applied && record.Shadow.Cursor != d.Head {
		return result, errors.Join(err, ErrReplicatedSchemaDDLConflict)
	}
	catalog, image, err := openReplicatedSchemaCatalogImage(record.Shadow.Catalog)
	if err != nil {
		return result, err
	}
	marker, staged, err := readReplicatedSchemaStageMarker(a.database.dataDir)
	if err != nil || staged && marker.schemaGeneration >= image.SchemaGeneration {
		return result, errors.Join(err, ErrReplicatedSchemaDDLConflict)
	}
	targetRoot, err := os.OpenRoot(filepath.Join(a.database.dataDir, replicatedSchemaTargetsDirectory))
	if err != nil {
		return result, err
	}
	defer targetRoot.Close()
	collections := make(map[replication.RelationID]*durable.Collection)
	var files []*os.File
	defer func() {
		for _, c := range collections {
			resultErr = errors.Join(resultErr, c.Close())
		}
		for _, f := range files {
			resultErr = errors.Join(resultErr, f.Close())
		}
	}()
	open := func(id replication.RelationID) (*durable.Collection, error) {
		if c := collections[id]; c != nil {
			return c, nil
		}
		if id == 0 || int(id) > len(catalog.ReplicatedShardStore.Relations) {
			return nil, ErrReplicatedSchemaDDLConflict
		}
		relation := catalog.ReplicatedShardStore.Relations[id-1]
		meta := catalog.Tables[relation.Table]
		candidate := &table{meta: meta}
		candidate.schema, err = compileSchemaMeta(meta.Schema)
		if err != nil {
			return nil, err
		}
		file, err := openSchemaDDLRegular(targetRoot, relation.Storage+".vjc", os.O_RDWR)
		if err != nil {
			return nil, err
		}
		files = append(files, file)
		c, err := durable.Open(file, durableOptions(candidate))
		if err != nil {
			return nil, err
		}
		collections[id] = c
		return c, nil
	}
	var workspace schemachange.CaptureWorkspace
	for n := 0; n < maxEntries && record.Shadow.Cursor.Publication.Applied < d.Head.Publication.Applied; n++ {
		entry, found, err := a.ReadReplicatedSchemaCapture(ctx, operation, record.Shadow.Cursor, &workspace)
		if err != nil || !found || entry.Abort != schemachange.NotAborted {
			return result, errors.Join(err, ErrReplicatedSchemaDDLConflict)
		}
		if !record.Truncate {
			for first := 0; first < len(entry.Mutations); {
				end := first + 1
				for end < len(entry.Mutations) && entry.Mutations[end].Relation == entry.Mutations[first].Relation {
					end++
				}
				c, err := open(entry.Mutations[first].Relation)
				if err == nil {
					err = replaySchemaDDLShadowRelation(ctx, c, entry.Mutations[first:end])
				}
				if err != nil {
					return result, err
				}
				if err := schemaDDLShadowFault("replay-relation"); err != nil {
					return result, err
				}
				first = end
			}
		}
		latest, err := a.ReplicatedSchemaCaptureDescriptor(operation)
		if err != nil || latest.Config != record.Capture || latest.Abort != schemachange.NotAborted {
			return result, errors.Join(err, ErrReplicatedSchemaDDLConflict)
		}
		record.Shadow.Cursor = schemachange.Cursor{Publication: entry.After, Digest: entry.Digest}
		if err := schemaDDLShadowFault("replay-cursor"); err != nil {
			return result, err
		}
		if err := writeSchemaDDLShadowRecord(root, record); err != nil {
			return result, err
		}
	}
	return record.Shadow, nil
}

// FinalizeReplicatedSchemaDDLShadow catches an online target up to the exact
// externally fenced source cut, seals capture, audits the target and publishes
// the ordinary detached build receipt used by schema installation. Row work
// happened before the fence; this path is bounded by retained capture entries,
// catalog size and relation count. The caller must hold the distributed write
// fence from expectedApplied selection through return.
func (a *ReplicatedApply) FinalizeReplicatedSchemaDDLShadow(
	ctx context.Context, operation [32]byte, expectedApplied uint64,
) (result ReplicatedSchemaDDLTarget, resultErr error) {
	if a == nil || a.database == nil || ctx == nil || operation == ([32]byte{}) || expectedApplied == 0 {
		return result, ErrReplicatedSchemaDDLConflict
	}
	for {
		shadow, err := a.ReplayReplicatedSchemaDDLShadow(ctx, operation, 1024)
		if err != nil {
			return result, err
		}
		if shadow.NoOp {
			return ReplicatedSchemaDDLTarget{NoOp: true}, nil
		}
		if shadow.Cursor.Publication.Applied > expectedApplied {
			return result, ErrTransactionConflict
		}
		if shadow.Cursor.Publication.Applied == expectedApplied {
			break
		}
	}
	root, unlock, err := a.lockSchemaDDLShadow(ctx)
	if err != nil {
		return result, err
	}
	record, found, err := readSchemaDDLShadowRecord(root)
	prior, priorFound, priorErr := readSchemaDDLBuildRecord(root)
	unlock()
	if err != nil || priorErr != nil || !found || !record.Ready || record.Shadow.Operation != operation ||
		record.Shadow.Cursor.Publication.Applied != expectedApplied {
		return result, errors.Join(err, priorErr, ErrReplicatedSchemaDDLConflict)
	}
	if priorFound && prior.Operation != operation && (!prior.Ready || prior.Target.NoOp ||
		prior.Target.Proof.Catalog.SchemaGeneration != record.SourceGeneration ||
		prior.Target.Proof.Catalog.Digest != record.SourceDigest) {
		return result, ErrReplicatedSchemaDDLConflict
	}
	if _, err := a.FinishReplicatedSchemaCapture(ctx, operation, expectedApplied); err != nil {
		return result, err
	}
	verified, err := a.PreflightReplicatedSchemaTarget(ctx, record.Shadow.Catalog, expectedApplied)
	if err != nil {
		return result, err
	}
	result, err = verified.DetachedTarget()
	resultErr = errors.Join(err, verified.Close())
	if resultErr != nil {
		return ReplicatedSchemaDDLTarget{}, resultErr
	}
	root, unlock, err = a.lockSchemaDDLShadow(ctx)
	if err != nil {
		return ReplicatedSchemaDDLTarget{}, err
	}
	defer unlock()
	current, found, err := readSchemaDDLShadowRecord(root)
	if err != nil || !found || current.SourceGeneration != record.SourceGeneration ||
		current.SourceDigest != record.SourceDigest || current.SQL != record.SQL ||
		current.Shadow.Cursor != record.Shadow.Cursor || !bytes.Equal(current.Shadow.Catalog, result.Catalog) {
		return ReplicatedSchemaDDLTarget{}, errors.Join(err, ErrReplicatedSchemaDDLConflict)
	}
	build := schemaDDLBuildRecord{Version: 1, Operation: operation, Applied: expectedApplied,
		SourceGeneration: record.SourceGeneration, SourceDigest: record.SourceDigest,
		SQL: record.SQL, Ready: true, Target: result}
	if prior, exists, readErr := readSchemaDDLBuildRecord(root); readErr != nil {
		return ReplicatedSchemaDDLTarget{}, readErr
	} else if exists {
		if prior.Operation == operation {
			if prior.Applied != expectedApplied || prior.SQL != record.SQL || !prior.Ready ||
				!bytes.Equal(prior.Target.Catalog, result.Catalog) || prior.Target.Proof != result.Proof {
				return ReplicatedSchemaDDLTarget{}, ErrReplicatedSchemaDDLConflict
			}
			return prior.Target, nil
		}
		// A completed predecessor may occupy the one-slot recovery journal.
		// Replace it only when its authenticated target is byte-for-byte this
		// online build's source generation; no absence or generation number
		// alone authorizes rollover.
		if !prior.Ready || prior.Target.NoOp ||
			prior.Target.Proof.Catalog.SchemaGeneration != record.SourceGeneration ||
			prior.Target.Proof.Catalog.Digest != record.SourceDigest {
			return ReplicatedSchemaDDLTarget{}, ErrReplicatedSchemaDDLConflict
		}
	}
	if err := writeSchemaDDLBuildRecord(root, build); err != nil {
		return ReplicatedSchemaDDLTarget{}, err
	}
	return result, nil
}

func replaySchemaDDLShadowRelation(ctx context.Context, target *durable.Collection, mutations []schemachange.Mutation) (resultErr error) {
	if len(mutations) > target.MaxBatchDocuments() {
		return durable.ErrBatchTooLarge
	}
	snapshot, err := target.Snapshot()
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, snapshot.Close()) }()
	pending := make([]schemachange.Mutation, 0, len(mutations))
	var value []byte
	for _, mutation := range mutations {
		if err := ctx.Err(); err != nil {
			return err
		}
		var found bool
		value, found, err = snapshot.AppendRaw(value[:0], mutation.Key)
		if err != nil {
			return err
		}
		if found == mutation.AfterPresent && (!found || bytes.Equal(value, mutation.After)) {
			continue // durable effect from a partially completed prior attempt
		}
		if !mutation.MatchesBefore(value, found) {
			return ErrReplicatedSchemaDDLConflict
		}
		pending = append(pending, mutation)
	}
	if len(pending) == 0 {
		return nil
	}
	return target.Update(func(batch *durable.WriteBatch) error {
		for _, mutation := range pending {
			var err error
			if mutation.AfterPresent {
				err = batch.Put(mutation.Key, mutation.After)
			} else {
				err = batch.Delete(mutation.Key)
			}
			if err != nil {
				return err
			}
		}
		return nil
	})
}
