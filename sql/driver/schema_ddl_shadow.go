package driver

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"os"

	"github.com/thesyncim/vibedb/internal/schemachange"
	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/query"
)

// ReplicatedSchemaDDLShadow is a mutable, non-serving build receipt, not a
// rollout certificate. Snapshot is the copied source cut; Cursor includes all
// durably replayed entries. Neither permits activation or bypasses a read/write
// fence. In particular a caught-up receipt can become stale immediately.
type ReplicatedSchemaDDLShadow struct {
	Operation [32]byte
	Catalog   []byte
	Snapshot  schemachange.Cursor
	Cursor    schemachange.Cursor
	NoOp      bool
}

type schemaDDLShadowRecord struct {
	Version          uint32
	SQL              string
	SourceDigest     [32]byte
	SourceGeneration uint64
	Capture          schemachange.CaptureConfig
	Ready            bool
	Truncate         bool
	Shadow           ReplicatedSchemaDDLShadow
}

const schemaDDLShadowName = ".schema-ddl-shadow"

var schemaDDLShadowDomain = []byte("vibedb/schema-ddl-shadow/1\x00")

// BuildReplicatedSchemaDDLShadow copies an immutable snapshot outside serving
// locks while capture retains intervening writes. It reserves private image
// identities durably before creating any files. An interrupted copy restarts
// at a fresh captured cut using only its own unprepared target names. A ready
// copy is reused across process replacement, not rescanned or recopied.
//
// CREATE/DROP INDEX copies all source relations. TRUNCATE builds empty images:
// source writes before its eventual cutover belong to the truncated generation
// and must not be resurrected by replay. The caller must still coordinate an
// exact, bounded cutover; this method never acquires a serving route gate.
func (a *ReplicatedApply) BuildReplicatedSchemaDDLShadow(ctx context.Context, operation [32]byte, text string, maxRecords, maxBytes uint64) (ReplicatedSchemaDDLShadow, error) {
	if operation == [32]byte{} || len(text) == 0 || len(text) > ReplicatedChildSchemaMaxBytes {
		return ReplicatedSchemaDDLShadow{}, ErrReplicatedSchemaDDLConflict
	}
	root, unlock, err := a.lockSchemaDDLShadow(ctx)
	if err != nil {
		return ReplicatedSchemaDDLShadow{}, err
	}
	defer unlock()
	source, generation, err := a.schemaDDLShadowSource()
	if err != nil {
		return ReplicatedSchemaDDLShadow{}, err
	}
	sourceDigest := sha256.Sum256(source)
	previous, found, err := readSchemaDDLShadowRecord(root)
	if err != nil {
		return ReplicatedSchemaDDLShadow{}, err
	}
	if found && (previous.Shadow.Operation != operation || previous.SQL != text || previous.SourceDigest != sourceDigest || previous.SourceGeneration != generation) {
		return ReplicatedSchemaDDLShadow{}, ErrReplicatedSchemaDDLConflict
	}
	if old, exists, err := readSchemaDDLBuildRecord(root); err != nil || exists && (!old.Ready || !old.Target.NoOp && old.Target.Proof.Catalog.SchemaGeneration > generation) {
		return ReplicatedSchemaDDLShadow{}, errors.Join(err, ErrReplicatedSchemaDDLConflict)
	}
	// Reject invalid SQL before installing capture or reserving storage.
	statement, err := query.PrepareDML(text)
	if err != nil {
		return ReplicatedSchemaDDLShadow{}, err
	}
	defer statement.Release()
	var decoded catalogFileVibe
	if err := decodeCatalogJSON(source, &decoded); err != nil {
		return ReplicatedSchemaDDLShadow{}, err
	}
	target := catalogFile(decoded)
	truncate, noOp, err := lowerReplicatedSchemaDDL(&target, statement)
	if err != nil || noOp {
		return ReplicatedSchemaDDLShadow{Operation: operation, NoOp: noOp}, err
	}
	plan := schemaDDLShadowPlan(text, sourceDigest)
	d, err := a.BeginReplicatedSchemaCapture(ctx, operation, plan, maxRecords, maxBytes)
	if err != nil || d.Abort != schemachange.NotAborted {
		return ReplicatedSchemaDDLShadow{}, errors.Join(err, ErrReplicatedSchemaDDLConflict)
	}
	if found && previous.Capture != d.Config {
		return ReplicatedSchemaDDLShadow{}, ErrReplicatedSchemaDDLConflict
	}
	if found && previous.Ready {
		return previous.Shadow, nil
	}
	a.database.mu.RLock()
	capture := a.schemaCapture
	a.database.mu.RUnlock()
	online := &schemaDDLOnlineCopy{capture: capture, source: source}
	record := schemaDDLShadowRecord{Version: 1, SQL: text, SourceDigest: sourceDigest, SourceGeneration: generation,
		Capture: d.Config, Truncate: truncate, Shadow: ReplicatedSchemaDDLShadow{Operation: operation}}
	reserve := func(target *catalogFile) error {
		if found {
			retained, image, err := openReplicatedSchemaCatalogImage(previous.Shadow.Catalog)
			if err != nil || len(retained.ReplicatedShardStore.Relations) != len(target.ReplicatedShardStore.Relations) {
				return errors.Join(err, ErrReplicatedSchemaDDLConflict)
			}
			for i := range target.ReplicatedShardStore.Relations {
				relation := &target.ReplicatedShardStore.Relations[i]
				old := retained.ReplicatedShardStore.Relations[i]
				if old.Table != relation.Table || validateStorageIdentity(old.Storage) != nil {
					return ErrReplicatedSchemaDDLConflict
				}
				relation.Storage, target.Tables[relation.Table].Storage = old.Storage, old.Storage
			}
			refreshSchemaDDLTargetIdentity(target)
			raw, err := appendCatalogJSON(nil, *target)
			if err != nil || !bytes.Equal(raw, previous.Shadow.Catalog) {
				return errors.Join(err, ErrReplicatedSchemaDDLConflict)
			}
			marker, staged, err := readReplicatedSchemaStageMarker(a.database.dataDir)
			if err != nil || staged && marker.schemaGeneration >= image.SchemaGeneration {
				return errors.Join(err, ErrReplicatedSchemaDDLConflict)
			}
			if err := removeReservedSchemaDDLImages(a.database.dataDir, retained); err != nil {
				return err
			}
		}
		raw, err := appendCatalogJSON(nil, *target)
		if err != nil {
			return err
		}
		record.Shadow.Catalog, record.Shadow.Snapshot, record.Shadow.Cursor = raw, online.cursor, online.cursor
		if err := writeSchemaDDLShadowRecord(root, record); err != nil {
			return err
		}
		return schemaDDLShadowFault("copy-reserved")
	}
	image, err := a.buildReplicatedSchemaDDLImage(ctx, d.Base.Publication.Applied, text, reserve, online)
	if err != nil {
		return ReplicatedSchemaDDLShadow{}, err
	}
	if image.NoOp || !bytes.Equal(image.Catalog, record.Shadow.Catalog) {
		return ReplicatedSchemaDDLShadow{}, ErrReplicatedSchemaDDLConflict
	}
	record.Ready = true
	if err := schemaDDLShadowFault("copy-ready"); err != nil {
		return ReplicatedSchemaDDLShadow{}, err
	}
	if err := writeSchemaDDLShadowRecord(root, record); err != nil {
		// Preserve the reserved files: pending or ready may be the durable
		// winner. Retrying a pending copy never overwrites prepared images.
		return ReplicatedSchemaDDLShadow{}, err
	}
	return record.Shadow, nil
}

func schemaDDLShadowPlan(text string, source [32]byte) [32]byte {
	h := sha256.New()
	_, _ = h.Write(schemaDDLShadowDomain)
	_, _ = h.Write(source[:])
	_, _ = h.Write([]byte(text))
	var digest [32]byte
	_ = h.Sum(digest[:0])
	return digest
}

func (a *ReplicatedApply) schemaDDLShadowSource() ([]byte, uint64, error) {
	a.database.mu.RLock()
	defer a.database.mu.RUnlock()
	if err := a.checkLocked(); err != nil {
		return nil, 0, err
	}
	if err := a.checkActivationBaseLocked(); err != nil {
		return nil, 0, err
	}
	raw, err := appendCatalogJSON(nil, a.database.catalog)
	return raw, a.database.catalog.ReplicatedShardStore.RelationSchemaGeneration, err
}

func (a *ReplicatedApply) lockSchemaDDLShadow(ctx context.Context) (*os.Root, func(), error) {
	if a == nil || a.database == nil || ctx == nil {
		return nil, nil, ErrReplicatedApplyClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	root, err := os.OpenRoot(a.database.dataDir)
	if err != nil {
		return nil, nil, err
	}
	lock, err := openSchemaDDLRegular(root, schemaDDLJournalName+".lock", os.O_RDWR|os.O_CREATE)
	if err == nil {
		err = storeio.LockWriter(lock)
	}
	if err != nil {
		if lock != nil {
			_ = lock.Close()
		}
		_ = root.Close()
		return nil, nil, err
	}
	// Nonblocking, shard-local DDL ownership; never a serving/write lock.
	return root, func() { storeio.UnlockWriter(lock); _ = lock.Close(); _ = root.Close() }, nil
}
