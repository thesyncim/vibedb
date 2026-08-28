package driver

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"path/filepath"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store/durable"
	"github.com/thesyncim/vibejson"
)

var ErrReplicatedSchemaDDLConflict = errors.New("vibedb: another schema DDL build is retained or its identity differs")

const schemaDDLJournalName = ".schema-ddl-build"
const schemaDDLJournalMaxBytes = 32 << 20

var schemaDDLJournalDomain = []byte("vibedb/schema-ddl-build/1\x00")

// ReplicatedSchemaDDLSourceApplied returns the exact source cut of a retained
// build, if this catalog was produced by it. Installers must not substitute
// their current applied index: that would certify stale target rows after a
// concurrent source write. The caller serializes this read with build/prepare.
func (a *ReplicatedApply) ReplicatedSchemaDDLSourceApplied(raw []byte) (uint64, bool, error) {
	if a == nil || a.database == nil {
		return 0, false, ErrReplicatedApplyClosed
	}
	a.database.mu.RLock()
	err := a.checkLocked()
	directory := a.database.dataDir
	a.database.mu.RUnlock()
	if err != nil {
		return 0, false, err
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		return 0, false, err
	}
	defer root.Close()
	record, found, err := readSchemaDDLBuildRecord(root)
	if err != nil || !found {
		return 0, false, err
	}
	if !bytes.Equal(record.Target.Catalog, raw) {
		return 0, false, nil
	}
	if !record.Ready || record.Target.NoOp {
		return 0, false, ErrReplicatedSchemaDDLConflict
	}
	return record.Applied, true, nil
}

type schemaDDLBuildRecord struct {
	Version          uint32
	Operation        [32]byte
	Applied          uint64
	SourceGeneration uint64
	SourceDigest     [32]byte
	SQL              string
	Ready            bool
	Target           ReplicatedSchemaDDLTarget
}

// BuildJournaledReplicatedSchemaDDLTarget reserves image identities before
// materialization and persists the exact certified result before returning.
// One bounded slot per shard survives process replacement. Retries cannot
// change SQL, source cut, operation identity or immutable target names.
//
// The schema coordinator must hold its exclusive route gate, serialize this
// builder with schema prepare/activation, and retain the returned result in
// its own operation record. This method grants no schema serving authority.
func (a *ReplicatedApply) BuildJournaledReplicatedSchemaDDLTarget(
	ctx context.Context, operation [32]byte, expectedApplied uint64, text string,
) (ReplicatedSchemaDDLTarget, error) {
	if a == nil || a.database == nil || ctx == nil || operation == ([32]byte{}) ||
		expectedApplied == 0 || len(text) == 0 || len(text) > ReplicatedChildSchemaMaxBytes {
		return ReplicatedSchemaDDLTarget{}, ErrReplicatedSchemaDDLConflict
	}
	if err := ctx.Err(); err != nil {
		return ReplicatedSchemaDDLTarget{}, err
	}
	core := a.database
	core.mu.RLock()
	err := a.checkLocked()
	var source []byte
	var generation uint64
	if err == nil {
		source, err = appendCatalogJSON(nil, core.catalog)
		generation = core.catalog.ReplicatedShardStore.RelationSchemaGeneration
	}
	directory := core.dataDir
	core.mu.RUnlock()
	if err != nil {
		return ReplicatedSchemaDDLTarget{}, err
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		return ReplicatedSchemaDDLTarget{}, err
	}
	defer root.Close()
	lock, err := openSchemaDDLRegular(root, schemaDDLJournalName+".lock", os.O_RDWR|os.O_CREATE)
	if err != nil {
		return ReplicatedSchemaDDLTarget{}, err
	}
	defer lock.Close()
	if err = storeio.LockWriter(lock); err != nil {
		return ReplicatedSchemaDDLTarget{}, err
	}
	defer storeio.UnlockWriter(lock)
	if err := rejectMutableSchemaShadow(directory); err != nil {
		return ReplicatedSchemaDDLTarget{}, err
	}
	previous, found, err := readSchemaDDLBuildRecord(root)
	if err != nil {
		return ReplicatedSchemaDDLTarget{}, err
	}
	sourceDigest := sha256.Sum256(source)
	if found && previous.Operation == operation {
		if previous.Applied != expectedApplied || previous.SQL != text {
			return ReplicatedSchemaDDLTarget{}, ErrReplicatedSchemaDDLConflict
		}
		if previous.Ready {
			// This durable certificate is a build receipt, not a serving fence.
			// It remains replayable after the installer has moved the images.
			return previous.Target, nil
		}
		if previous.SourceGeneration != generation || previous.SourceDigest != sourceDigest {
			return ReplicatedSchemaDDLTarget{}, ErrTransactionConflict
		}
	} else if found && (!previous.Ready || !previous.Target.NoOp &&
		(generation < previous.Target.Proof.Catalog.SchemaGeneration ||
			generation == previous.Target.Proof.Catalog.SchemaGeneration && sourceDigest != previous.Target.Proof.Catalog.Digest)) {
		return ReplicatedSchemaDDLTarget{}, ErrReplicatedSchemaDDLConflict
	}
	record := schemaDDLBuildRecord{Version: 1, Operation: operation, Applied: expectedApplied,
		SourceGeneration: generation, SourceDigest: sourceDigest, SQL: text}
	reserve := func(target *catalogFile) error {
		if found && previous.Operation == operation {
			retained, image, err := openReplicatedSchemaCatalogImage(previous.Target.Catalog)
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
			if err != nil || !bytes.Equal(raw, previous.Target.Catalog) {
				return errors.Join(err, ErrReplicatedSchemaDDLConflict)
			}
			// A pending build has never returned a receipt. Nevertheless refuse
			// deletion if any installer has already certified these exact files.
			marker, staged, err := readReplicatedSchemaStageMarker(directory)
			if err != nil || staged && marker.schemaGeneration >= image.SchemaGeneration {
				return errors.Join(err, ErrReplicatedSchemaDDLConflict)
			}
			if err := removeReservedSchemaDDLImages(directory, retained); err != nil {
				return err
			}
		}
		raw, err := appendCatalogJSON(nil, *target)
		if err != nil {
			return err
		}
		record.Target.Catalog = raw
		return writeSchemaDDLBuildRecord(root, record)
	}
	target, err := a.buildReplicatedSchemaDDLTarget(ctx, expectedApplied, text, reserve)
	if err != nil {
		return ReplicatedSchemaDDLTarget{}, err
	}
	record.Ready, record.Target = true, target
	if err := writeSchemaDDLBuildRecord(root, record); err != nil {
		// Preserve reserved files: either the pending intent or the exact ready
		// receipt can be the durable winner after an unknown publication.
		return ReplicatedSchemaDDLTarget{}, err
	}
	return target, nil
}

func removeReservedSchemaDDLImages(directory string, catalog catalogFile) error {
	root, err := os.OpenRoot(filepath.Join(directory, replicatedSchemaTargetsDirectory))
	if err != nil {
		return err
	}
	defer root.Close()
	for _, relation := range catalog.ReplicatedShardStore.Relations {
		if validateStorageIdentity(relation.Storage) != nil {
			return ErrReplicatedSchemaDDLConflict
		}
		for _, name := range []string{relation.Storage + ".vjc.rjournal", relation.Storage + ".vjc"} {
			info, err := root.Lstat(name)
			if os.IsNotExist(err) {
				continue
			}
			if err != nil || !info.Mode().IsRegular() {
				return errors.Join(err, ErrReplicatedSchemaDDLConflict)
			}
			if err := root.Remove(name); err != nil {
				return err
			}
		}
	}
	return syncSchemaNamespaceDirectory(root, ".")
}

func openSchemaDDLRegular(root *os.Root, name string, flags int) (*os.File, error) {
	info, err := root.Lstat(name)
	if err == nil && !info.Mode().IsRegular() {
		return nil, ErrReplicatedSchemaDDLConflict
	}
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	return root.OpenFile(name, flags, 0600)
}

func validateSchemaDDLBuildRecord(record schemaDDLBuildRecord) error {
	if record.Version != 1 || record.Operation == ([32]byte{}) || record.Applied == 0 ||
		record.SourceGeneration == 0 || record.SourceGeneration == ^uint64(0) || record.SourceDigest == ([32]byte{}) ||
		len(record.SQL) == 0 || len(record.SQL) > ReplicatedChildSchemaMaxBytes {
		return ErrReplicatedSchemaDDLConflict
	}
	if record.Target.NoOp {
		if !record.Ready || len(record.Target.Catalog) != 0 || record.Target.Proof != (ReplicatedSchemaTargetProof{}) {
			return ErrReplicatedSchemaDDLConflict
		}
		return nil
	}
	image, err := ValidateReplicatedSchemaCatalogImage(record.Target.Catalog)
	if err != nil || image.SchemaGeneration != record.SourceGeneration+1 {
		return errors.Join(err, ErrReplicatedSchemaDDLConflict)
	}
	if !record.Ready {
		if record.Target.Proof != (ReplicatedSchemaTargetProof{}) {
			return ErrReplicatedSchemaDDLConflict
		}
		return nil
	}
	return ValidateReplicatedSchemaDDLTarget(record.Target, record.Applied, record.SourceGeneration)
}

func readSchemaDDLBuildRecord(root *os.Root) (schemaDDLBuildRecord, bool, error) {
	var record schemaDDLBuildRecord
	file, err := openSchemaDDLRegular(root, schemaDDLJournalName, os.O_RDONLY)
	if os.IsNotExist(err) {
		return record, false, nil
	}
	if err != nil {
		return record, false, err
	}
	raw, err := io.ReadAll(io.LimitReader(file, schemaDDLJournalMaxBytes+1))
	if err == nil {
		err = file.Sync()
	}
	err = errors.Join(err, file.Close())
	if err != nil {
		return record, false, err
	}
	if len(raw) <= sha256.Size || len(raw) > schemaDDLJournalMaxBytes {
		return record, false, ErrReplicatedSchemaDDLConflict
	}
	h := sha256.New()
	_, _ = h.Write(schemaDDLJournalDomain)
	_, _ = h.Write(raw[sha256.Size:])
	if !bytes.Equal(raw[:sha256.Size], h.Sum(nil)) {
		return record, false, ErrReplicatedSchemaDDLConflict
	}
	if err := vibejson.Unmarshal(raw[sha256.Size:], &record); err != nil {
		return record, false, err
	}
	if err := validateSchemaDDLBuildRecord(record); err != nil {
		return record, false, err
	}
	canonical, err := vibejson.Marshal(&record)
	if err != nil || !bytes.Equal(canonical, raw[sha256.Size:]) {
		return record, false, errors.Join(err, ErrReplicatedSchemaDDLConflict)
	}
	if err := syncSchemaNamespaceDirectory(root, "."); err != nil {
		return record, false, err
	}
	return record, true, nil
}

func writeSchemaDDLBuildRecord(root *os.Root, record schemaDDLBuildRecord) error {
	if err := validateSchemaDDLBuildRecord(record); err != nil {
		return err
	}
	body, err := vibejson.Marshal(&record)
	if err != nil {
		return err
	}
	if len(body)+sha256.Size > schemaDDLJournalMaxBytes {
		return ErrReplicatedSchemaDDLConflict
	}
	h := sha256.New()
	_, _ = h.Write(schemaDDLJournalDomain)
	_, _ = h.Write(body)
	raw := append(h.Sum(nil), body...)
	const pending = schemaDDLJournalName + ".tmp"
	if err := root.Remove(pending); err != nil && !os.IsNotExist(err) {
		return err
	}
	file, err := openSchemaDDLRegular(root, pending, os.O_WRONLY|os.O_CREATE|os.O_EXCL)
	if err == nil {
		var written int
		written, err = file.Write(raw)
		if err == nil && written != len(raw) {
			err = io.ErrShortWrite
		}
	}
	if err == nil {
		err = file.Sync()
	}
	if file != nil {
		err = errors.Join(err, file.Close())
	}
	if err == nil {
		err = root.Rename(pending, schemaDDLJournalName)
	}
	if err == nil {
		err = syncSchemaNamespaceDirectory(root, ".")
	}
	if err != nil {
		return errors.Join(durable.ErrCommitOutcomeUnknown, err)
	}
	return nil
}
