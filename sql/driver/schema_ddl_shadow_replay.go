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
