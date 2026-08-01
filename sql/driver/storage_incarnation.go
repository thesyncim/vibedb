package driver

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	sqlast "github.com/thesyncim/vibedb/sql"
	"github.com/thesyncim/vibedb/store/durable"
)

const (
	storageIdentityBytes      = 32
	maxStorageIdentityTries   = 32
	maxStorageRecoveryEntries = 4096
	maxRetiredTables          = 64
)

func validateStorageIdentity(identity string) error {
	if identity == "" {
		// Catalog version 0 derived the path from the SQL table name. Empty is
		// therefore the explicit, backward-compatible legacy representation.
		return nil
	}
	if len(identity) != storageIdentityBytes*2 {
		return fmt.Errorf(
			"identity is %d bytes, want %d lowercase hexadecimal bytes",
			len(identity), storageIdentityBytes*2,
		)
	}
	for _, c := range []byte(identity) {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return errors.New("identity must be lowercase hexadecimal")
		}
	}
	return nil
}

func (d *database) tablePathForMeta(name string, meta *tableMeta) string {
	if meta != nil && meta.Storage != "" {
		return filepath.Join(d.dataDir, meta.Storage+".vjc")
	}
	return d.legacyTablePath(name)
}

func (d *database) newStorageIdentityLocked() (string, error) {
	var random [storageIdentityBytes]byte
	for attempt := 0; attempt < maxStorageIdentityTries; attempt++ {
		if _, err := rand.Read(random[:]); err != nil {
			return "", fmt.Errorf("vibedb: allocate SQL storage identity: %w", err)
		}
		identity := hex.EncodeToString(random[:])
		path := filepath.Join(d.dataDir, identity+".vjc")
		if d.storagePathInUseLocked(path) {
			continue
		}
		if _, err := os.Lstat(path); err == nil || !os.IsNotExist(err) {
			if err != nil {
				return "", err
			}
			continue
		}
		journal := durable.RecoveryJournalPath(path)
		if _, err := os.Lstat(journal); err == nil || !os.IsNotExist(err) {
			if err != nil {
				return "", err
			}
			continue
		}
		return identity, nil
	}
	return "", errors.New("vibedb: could not allocate a unique SQL storage identity")
}

func (d *database) storagePathInUseLocked(path string) bool {
	for name, meta := range d.catalog.Tables {
		if d.tablePathForMeta(name, meta) == path {
			return true
		}
	}
	for i := range d.retired {
		if d.retired[i].path == path {
			return true
		}
	}
	return false
}

// recoverOrphanedTableStorage removes only names owned by the private SQL
// table namespace and absent from the fully validated catalog. Such files are
// the bounded recovery record for a crash before or after a catalog cutover.
func (d *database) recoverOrphanedTableStorage(protected map[string]string) error {
	directory, err := os.Open(d.dataDir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	defer directory.Close()

	protectedNames := make(map[string]struct{}, len(protected)*2)
	for path := range protected {
		base := filepath.Base(path)
		protectedNames[base] = struct{}{}
		protectedNames[base+".rjournal"] = struct{}{}
	}
	removed := false
	var result error
	entries := 0
	for {
		names, readErr := directory.Readdirnames(128)
		for _, name := range names {
			entries++
			if entries > maxStorageRecoveryEntries {
				result = fmt.Errorf(
					"%w: SQL table directory has more than %d entries",
					ErrTooManyStorageFiles, maxStorageRecoveryEntries,
				)
				break
			}
			if _, keep := protectedNames[name]; keep || !managedStorageName(name) {
				continue
			}
			if removeErr := os.Remove(filepath.Join(d.dataDir, name)); removeErr != nil &&
				!os.IsNotExist(removeErr) {
				result = fmt.Errorf(
					"%w: remove orphaned SQL storage %q: %w",
					durable.ErrCommitOutcomeUnknown, name, removeErr,
				)
				break
			}
			removed = true
		}
		if result != nil || errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			result = readErr
			break
		}
	}
	if removed {
		if syncErr := d.directorySync(d.dataDir); syncErr != nil {
			result = errors.Join(result, fmt.Errorf(
				"%w: fence orphaned SQL storage cleanup: %w",
				durable.ErrCommitOutcomeUnknown, syncErr,
			))
		}
	}
	return result
}

func managedStorageName(name string) bool {
	name = strings.TrimSuffix(name, ".rjournal")
	if strings.HasSuffix(name, ".vjc") {
		return storageIdentityValid(strings.TrimSuffix(name, ".vjc"))
	}
	if !strings.HasPrefix(name, ".") {
		return false
	}
	name = strings.TrimPrefix(name, ".")
	const marker = ".vjc.tmp-"
	at := strings.Index(name, marker)
	return at == storageIdentityBytes*2 &&
		len(name) > at+len(marker) &&
		storageIdentityValid(name[:at])
}

func storageIdentityValid(identity string) bool {
	return identity != "" && validateStorageIdentity(identity) == nil
}

func cloneTableMeta(source *tableMeta) *tableMeta {
	clone := *source
	if source.Schema != nil {
		schema := *source.Schema
		schema.Fields = append([]schemaFieldMeta(nil), source.Schema.Fields...)
		clone.Schema = &schema
	}
	clone.Indexes = make([]indexMeta, len(source.Indexes))
	for i := range source.Indexes {
		clone.Indexes[i] = indexMeta{
			Name:  source.Indexes[i].Name,
			Paths: append([]string(nil), source.Indexes[i].Paths...),
		}
	}
	return &clone
}

func (d *database) checkRetirementCapacityLocked(add int) error {
	if add <= maxRetiredTables-len(d.retired) {
		return nil
	}
	return fmt.Errorf(
		"%w: %d retired table incarnations, maximum is %d",
		ErrTooManyRetiredTables, len(d.retired)+add, maxRetiredTables,
	)
}

// truncateTableStorageLockedContext is the storage cutover seam for TRUNCATE.
// SQL parsing remains deliberately outside this change.
func (d *database) truncateTableStorageLockedContext(
	ctx context.Context,
	name string,
) error {
	t, ok := d.tables[name]
	if !ok {
		return fmt.Errorf("%w: %q", ErrTableNotFound, name)
	}
	return d.replaceTableStorageLockedContext(ctx, name, t.meta.Indexes, false)
}

// dropIndexStorageLockedContext rebuilds a table without one exact index and
// atomically publishes the resulting storage incarnation.
func (d *database) dropIndexStorageLockedContext(
	ctx context.Context,
	tableName string,
	indexName string,
) error {
	t, ok := d.tables[tableName]
	if !ok {
		return fmt.Errorf("%w: %q", ErrTableNotFound, tableName)
	}
	indexes := make([]indexMeta, 0, len(t.meta.Indexes))
	found := false
	for i := range t.meta.Indexes {
		if t.meta.Indexes[i].Name == indexName {
			found = true
			continue
		}
		indexes = append(indexes, t.meta.Indexes[i])
	}
	if !found {
		return fmt.Errorf("%w: %q", ErrIndexNotFound, indexName)
	}
	return d.replaceTableStorageLockedContext(ctx, tableName, indexes, true)
}

// resolveDropIndexLocked applies DROP INDEX's optional ON qualification. Index
// names are table-local today, so an unqualified duplicate is an error even
// with IF EXISTS: existence does not choose between two destructive targets.
func (d *database) resolveDropIndexLocked(
	drop *sqlast.DropIndexStmt,
) (tableName string, found bool, err error) {
	if drop == nil {
		return "", false, errors.New("vibedb: DROP INDEX has no parsed definition")
	}
	if drop.HasTable {
		t, exists := d.tables[drop.Table]
		if !exists {
			if drop.IfExists {
				return "", false, nil
			}
			return "", false, fmt.Errorf("%w: %q", ErrTableNotFound, drop.Table)
		}
		for i := range t.meta.Indexes {
			if t.meta.Indexes[i].Name == drop.Name {
				return drop.Table, true, nil
			}
		}
		if drop.IfExists {
			return "", false, nil
		}
		return "", false, fmt.Errorf("%w: %q on table %q",
			ErrIndexNotFound, drop.Name, drop.Table)
	}
	for name, t := range d.tables {
		for i := range t.meta.Indexes {
			if t.meta.Indexes[i].Name != drop.Name {
				continue
			}
			if found {
				return "", false, fmt.Errorf(
					"%w: %q exists on tables %q and %q; add ON table",
					ErrIndexAmbiguous, drop.Name, tableName, name,
				)
			}
			tableName, found = name, true
		}
	}
	if found || drop.IfExists {
		return tableName, found, nil
	}
	return "", false, fmt.Errorf("%w: %q", ErrIndexNotFound, drop.Name)
}

func (d *database) replaceTableStorageLockedContext(
	ctx context.Context,
	name string,
	indexes []indexMeta,
	copyDocuments bool,
) error {
	if err := d.settleCatalogLocked(); err != nil {
		return err
	}
	old, ok := d.tables[name]
	if !ok {
		return fmt.Errorf("%w: %q", ErrTableNotFound, name)
	}
	if err := contextCheckpoint(ctx); err != nil {
		return err
	}
	retire := old.collection != nil || old.file != nil
	if retire {
		if err := d.checkRetirementCapacityLocked(1); err != nil {
			return err
		}
	}
	identity, err := d.newStorageIdentityLocked()
	if err != nil {
		return err
	}
	meta := cloneTableMeta(old.meta)
	meta.Storage = identity
	meta.Indexes = cloneIndexMeta(indexes)
	meta.Materialized = false
	candidate := &table{
		meta: meta, schema: old.schema, primary: old.primary,
	}
	if err := durable.ValidateOptions(durableOptions(candidate)); err != nil {
		return fmt.Errorf("vibedb: replacement table %q: %w", name, err)
	}
	if old.collection != nil {
		if err := d.buildReplacementStorageLocked(
			ctx, name, old, candidate, copyDocuments,
		); err != nil {
			return err
		}
	}
	if err := contextCheckpoint(ctx); err != nil {
		return errors.Join(err, d.discardTableStorageLocked(name, candidate))
	}

	oldMeta := d.catalog.Tables[name]
	oldPath := d.tablePathForMeta(name, oldMeta)
	previousPending := d.catalogWritePending
	retiredBefore := len(d.retired)
	d.catalog.Tables[name] = meta
	d.tables[name] = candidate
	if retire {
		d.retired = append(d.retired, retiredTable{
			name: name, path: oldPath,
			journal: durable.RecoveryJournalPath(oldPath),
			file:    old.file, collection: old.collection,
		})
	}
	published, persistErr := d.persistCatalogLocked()
	if persistErr != nil && !published {
		d.catalog.Tables[name] = oldMeta
		d.tables[name] = old
		d.retired = d.retired[:retiredBefore]
		d.catalogWritePending = previousPending
		return errors.Join(persistErr, d.discardTableStorageLocked(name, candidate))
	}
	if persistErr != nil {
		return persistErr
	}
	return d.settleDroppedTablesLocked()
}

func cloneIndexMeta(source []indexMeta) []indexMeta {
	clone := make([]indexMeta, len(source))
	for i := range source {
		clone[i] = indexMeta{
			Name:  source[i].Name,
			Paths: append([]string(nil), source[i].Paths...),
		}
	}
	return clone
}

func (d *database) buildReplacementStorageLocked(
	ctx context.Context,
	name string,
	old *table,
	candidate *table,
	copyDocuments bool,
) error {
	if err := d.ensureDataDir(); err != nil {
		return err
	}
	path := d.tablePathForMeta(name, candidate.meta)
	file, err := createPublishableTableTemp(
		d.dataDir, "."+filepath.Base(path)+".tmp-",
	)
	if err != nil {
		return err
	}
	tmpPath := file.Name()
	collection, err := durable.Create(file, durableOptions(candidate))
	if err == nil && copyDocuments {
		var snapshot *durable.Snapshot
		snapshot, err = old.collection.Snapshot()
		if err == nil {
			batch := make([]seedDocument, 0, collection.MaxBatchDocuments())
			batchBytes := 0
			flush := func() error {
				if len(batch) == 0 {
					return nil
				}
				if updateErr := collection.Update(func(write *durable.WriteBatch) error {
					for i := range batch {
						if putErr := write.Put(
							[]byte(batch[i].key), batch[i].document,
						); putErr != nil {
							return putErr
						}
					}
					return nil
				}); updateErr != nil {
					return updateErr
				}
				for i := range batch {
					batch[i] = seedDocument{}
				}
				batch = batch[:0]
				batchBytes = 0
				return nil
			}
			err = snapshot.RangeRaw(func(key, document []byte) error {
				if checkpointErr := contextCheckpoint(ctx); checkpointErr != nil {
					return checkpointErr
				}
				entryBytes := len(key) + len(document)
				if entryBytes > collection.MaxBatchBytes() {
					return durable.ErrBatchTooLarge
				}
				if len(batch) == collection.MaxBatchDocuments() ||
					batchBytes > collection.MaxBatchBytes()-entryBytes {
					if flushErr := flush(); flushErr != nil {
						return flushErr
					}
				}
				batch = append(batch, seedDocument{
					key: string(key), document: append([]byte(nil), document...),
				})
				batchBytes += entryBytes
				return nil
			})
			if err == nil {
				err = flush()
			}
			if closeErr := snapshot.Close(); closeErr != nil {
				err = errors.Join(err, closeErr)
			}
		}
	}
	if err != nil {
		return errors.Join(err, d.discardUnpublishedStorageLocked(
			collection, file, tmpPath,
		))
	}
	if _, statErr := os.Lstat(path); statErr == nil || !os.IsNotExist(statErr) {
		if statErr == nil {
			statErr = errors.New("storage identity path already exists")
		}
		return errors.Join(statErr, d.discardUnpublishedStorageLocked(
			collection, file, tmpPath,
		))
	}
	if err := publishNewPath(tmpPath, path); err != nil {
		return errors.Join(err, d.discardUnpublishedStorageLocked(
			collection, file, tmpPath, path,
		))
	}
	if err := publishJournalSibling(tmpPath, path); err != nil {
		return errors.Join(err, d.discardUnpublishedStorageLocked(
			collection, file, tmpPath, path,
		))
	}
	d.tableDirFencePending = true
	if err := d.directorySync(d.dataDir); err != nil {
		return errors.Join(fmt.Errorf(
			"%w: fence replacement SQL storage: %w",
			durable.ErrCommitOutcomeUnknown, err,
		), d.discardUnpublishedStorageLocked(collection, file, path))
	}
	d.tableDirFencePending = false
	candidate.file = file
	candidate.collection = collection
	candidate.meta.Materialized = true
	return nil
}

func (d *database) discardTableStorageLocked(name string, t *table) error {
	if t == nil || (t.collection == nil && t.file == nil) {
		return nil
	}
	path := d.tablePathForMeta(name, t.meta)
	err := d.discardUnpublishedStorageLocked(t.collection, t.file, path)
	t.collection = nil
	t.file = nil
	return err
}

func (d *database) discardUnpublishedStorageLocked(
	collection *durable.Collection,
	file *os.File,
	paths ...string,
) error {
	var result error
	if collection != nil {
		result = errors.Join(result, d.collectionClose(collection))
	}
	if file != nil {
		result = errors.Join(result, file.Close())
	}
	removed := false
	seen := make(map[string]struct{}, len(paths)*2)
	for _, path := range paths {
		for _, candidate := range []string{path, durable.RecoveryJournalPath(path)} {
			if _, duplicate := seen[candidate]; duplicate {
				continue
			}
			seen[candidate] = struct{}{}
			if err := os.Remove(candidate); err != nil && !os.IsNotExist(err) {
				result = errors.Join(result, err)
			} else if err == nil {
				removed = true
			}
		}
	}
	if removed {
		d.tableDirFencePending = true
		if err := d.directorySync(d.dataDir); err != nil {
			result = errors.Join(result, fmt.Errorf(
				"%w: fence unpublished SQL storage cleanup: %w",
				durable.ErrCommitOutcomeUnknown, err,
			))
		} else {
			d.tableDirFencePending = false
		}
	}
	return result
}
