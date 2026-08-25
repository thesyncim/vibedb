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
		return errors.New("identity is empty")
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

func (d *database) tablePathForMeta(meta *tableMeta) string {
	if meta == nil || meta.Storage == "" {
		return ""
	}
	return filepath.Join(d.dataDir, meta.Storage+".vjc")
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
	if d.catalog.ReplicatedApply != nil &&
		(d.replicatedApplyPath(d.catalog.ReplicatedApply) == path ||
			d.replicatedCapturePath(d.catalog.ReplicatedApply) == path) {
		return true
	}
	for _, meta := range d.catalog.Tables {
		if d.tablePathForMeta(meta) == path {
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
	for path, name := range protected {
		base := filepath.Base(path)
		protectedNames[base] = struct{}{}
		meta := d.catalog.Tables[name]
		protectJournal := meta != nil && meta.Materialized
		if apply := d.catalog.ReplicatedApply; apply != nil &&
			(d.replicatedApplyPath(apply) == path || d.replicatedCapturePath(apply) == path) {
			protectJournal = true
		}
		if !protectJournal {
			if _, statErr := os.Lstat(path); statErr == nil {
				protectJournal = true
			} else if !os.IsNotExist(statErr) {
				return statErr
			}
		}
		if protectJournal {
			protectedNames[base+".rjournal"] = struct{}{}
		}
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

	detachedOld := false
	adoptedCandidate := false
	if old.collection != nil {
		if d.txnLog == nil {
			return errors.Join(
				errors.New("vibedb: database transaction log is not open"),
				d.discardTableStorageLocked(name, candidate),
			)
		}
		if err := d.txnLog.DetachCollection(old.collection); err != nil {
			return errors.Join(
				fmt.Errorf(
					"vibedb: detach replaced table %q from transaction log: %w",
					name, err,
				),
				d.discardTableStorageLocked(name, candidate),
			)
		}
		detachedOld = true
		if candidate.collection == nil {
			adoptErr := d.txnLog.AdoptCollection(old.collection)
			if adoptErr != nil {
				d.retainTxnReattachLocked(old.collection)
			}
			return errors.Join(
				errors.New("vibedb: replacement table has no durable collection"),
				adoptErr,
				d.discardTableStorageLocked(name, candidate),
			)
		}
		if err := d.txnLog.AdoptCollection(candidate.collection); err != nil {
			adoptErr := d.txnLog.AdoptCollection(old.collection)
			if adoptErr != nil {
				d.retainTxnReattachLocked(old.collection)
			}
			return errors.Join(
				fmt.Errorf(
					"vibedb: attach replacement table %q to transaction log: %w",
					name, err,
				),
				adoptErr,
				d.discardTableStorageLocked(name, candidate),
			)
		}
		adoptedCandidate = true
	}

	oldMeta := d.catalog.Tables[name]
	oldPath := d.tablePathForMeta(oldMeta)
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
		var candidateDetachErr error
		if adoptedCandidate {
			candidateDetachErr = d.txnLog.DetachCollection(candidate.collection)
		}
		var oldAdoptErr error
		if detachedOld {
			oldAdoptErr = d.txnLog.AdoptCollection(old.collection)
			if oldAdoptErr != nil {
				d.retainTxnReattachLocked(old.collection)
			}
		}
		if candidateDetachErr != nil {
			// A failed detach keeps the candidate registered by contract. Preserve
			// its complete resource/path ownership in the retirement registry;
			// closing or unlinking it here would strand a stale TxnLog handle. A
			// later catalog settlement or terminal close can retry cleanup after
			// transaction-log recovery has become possible.
			d.retired = append(d.retired, retiredTable{
				name: name + " (unpublished replacement)",
				path: d.tablePathForMeta(candidate.meta),
				journal: durable.RecoveryJournalPath(
					d.tablePathForMeta(candidate.meta),
				),
				file: candidate.file, collection: candidate.collection,
			})
			candidate.file = nil
			candidate.collection = nil
			return errors.Join(persistErr, candidateDetachErr, oldAdoptErr)
		}
		return errors.Join(
			persistErr,
			oldAdoptErr,
			d.discardTableStorageLocked(name, candidate),
		)
	}
	// The new incarnation remains authoritative on success and on a catalog
	// publication with unknown durability outcome.
	d.advanceLayoutEpochLocked()
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
	path := d.tablePathForMeta(candidate.meta)
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
	file, collection, err = d.publishTableStorageLocked(
		tmpPath, path, file, collection, durableOptions(candidate),
	)
	if err != nil {
		return fmt.Errorf("vibedb: publish replacement SQL storage: %w", err)
	}
	candidate.file = file
	candidate.collection = collection
	candidate.meta.Materialized = true
	return nil
}

// publishTableStorageLocked publishes a fully built durable collection without
// retaining any open store or recovery-journal handle across the namespace
// moves. Windows rename semantics require that stronger ownership boundary for
// the journal; using it on every platform also makes the publication protocol
// uniform and easy to audit:
//
//  1. checkpoint and close the unpublished collection and descriptor,
//  2. publish and fence the recovery journal,
//  3. publish and fence the store file,
//  4. reopen the final name before any catalog cutover.
//
// A crash before the catalog cutover leaves either catalog-owned first-table
// storage (adopted by openDatabase) or an unreferenced replacement identity
// (removed by bounded orphan recovery). An in-process failure attempts a
// fenced cleanup and never returns a live half-published candidate.
func (d *database) publishTableStorageLocked(
	tmpPath string,
	path string,
	file *os.File,
	collection *durable.Collection,
	options durable.Options,
) (*os.File, *durable.Collection, error) {
	if err, completed := d.collectionCloseState(collection); err != nil {
		if !completed {
			return nil, nil, errors.Join(err, d.retainUnpublishedStorageLocked(
				collection, file, tmpPath,
			))
		}
		// A completed Close may retain a sticky persistence error. Ownership
		// nevertheless ended, so the candidate is not publishable and its private
		// descriptor and names can be removed safely.
		return nil, nil, errors.Join(err, d.discardUnpublishedStorageLocked(
			nil, file, tmpPath,
		))
	}
	collection = nil
	if err := file.Close(); err != nil {
		return nil, nil, errors.Join(err, d.discardUnpublishedStorageLocked(
			nil, nil, tmpPath,
		))
	}
	file = nil

	cleanup := func(err error) (*os.File, *durable.Collection, error) {
		return nil, nil, errors.Join(err, d.discardUnpublishedStorageLocked(
			nil, nil, tmpPath, path,
		))
	}
	if err := publishJournalSibling(tmpPath, path); err != nil {
		return cleanup(err)
	}
	d.tableDirFencePending = true
	if err := d.directorySync(d.dataDir); err != nil {
		return cleanup(fmt.Errorf(
			"%w: fence published SQL recovery journal: %w",
			durable.ErrCommitOutcomeUnknown, err,
		))
	}
	d.tableDirFencePending = false
	if err := publishNewPath(tmpPath, path); err != nil {
		return cleanup(err)
	}
	d.tableDirFencePending = true
	if err := d.directorySync(d.dataDir); err != nil {
		return cleanup(fmt.Errorf(
			"%w: fence published SQL store: %w",
			durable.ErrCommitOutcomeUnknown, err,
		))
	}
	d.tableDirFencePending = false

	finalFile, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return cleanup(err)
	}
	// The durable page catalog is authoritative for exact-index definitions;
	// this is the same recovery contract used by openDatabase after an online
	// index publication races the SQL metadata mirror.
	options.Indexes = nil
	finalCollection, err := durable.Open(finalFile, options)
	if err != nil {
		closeErr := finalFile.Close()
		_, _, cleanupErr := cleanup(errors.Join(err, closeErr))
		return nil, nil, cleanupErr
	}
	return finalFile, finalCollection, nil
}

func (d *database) discardTableStorageLocked(name string, t *table) error {
	if t == nil || (t.collection == nil && t.file == nil) {
		return nil
	}
	path := d.tablePathForMeta(t.meta)
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
		closeErr, completed := d.collectionCloseState(collection)
		if !completed {
			return errors.Join(closeErr, d.retainUnpublishedStorageLocked(
				collection, file, paths...,
			))
		}
		result = errors.Join(result, closeErr)
		collection = nil
	}
	if file != nil {
		result = errors.Join(result, file.Close())
	}
	removed := false
	removeFailed := false
	seen := make(map[string]struct{}, len(paths)*2)
	for _, path := range paths {
		for _, candidate := range []string{path, durable.RecoveryJournalPath(path)} {
			if _, duplicate := seen[candidate]; duplicate {
				continue
			}
			seen[candidate] = struct{}{}
			if err := os.Remove(candidate); err != nil && !os.IsNotExist(err) {
				result = errors.Join(result, err)
				removeFailed = true
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
	if removeFailed {
		// Close completion ends engine and descriptor ownership, but it does not
		// end ownership of names that could not be unlinked. Keep those names in
		// the same bounded retirement slot that callers reserve before creating an
		// unpublished store. Catalog settlement must drain this entry before a
		// later mutation can allocate another candidate.
		result = errors.Join(result, d.retainUnpublishedNamespaceLocked(paths...))
	}
	return result
}

func (d *database) retainUnpublishedNamespaceLocked(paths ...string) error {
	if len(paths) == 0 || len(paths) > 2 {
		return errors.New("vibedb: invalid unpublished storage cleanup path count")
	}
	entry := retiredTable{
		name: "unpublished SQL storage", path: paths[0],
		journal: durable.RecoveryJournalPath(paths[0]),
	}
	if len(paths) == 2 {
		entry.extraPath = paths[1]
		entry.extraJournal = durable.RecoveryJournalPath(paths[1])
	}
	d.retired = append(d.retired, entry)
	if len(d.retired) > maxRetiredTables {
		// Production callers pre-reserve this slot. Preserve the only retry
		// ownership record if an internal caller violates that invariant, while
		// ensuring every later catalog mutation fails at the typed bound.
		return ErrTooManyRetiredTables
	}
	return fmt.Errorf(
		"%w: unpublished SQL storage namespace cleanup is pending retry",
		durable.ErrCommitOutcomeUnknown,
	)
}

// retainUnpublishedStorageLocked transfers a candidate whose engine teardown
// is incomplete into the bounded retry queue used by replaced table
// incarnations. Callers reserve one retirement slot before constructing the
// candidate. At most two store names exist around publication (temporary and
// final), so the queue entry remains fixed-size.
func (d *database) retainUnpublishedStorageLocked(
	collection *durable.Collection,
	file *os.File,
	paths ...string,
) error {
	if collection == nil || collection.CloseCompleted() {
		return errors.New("vibedb: cannot retain completed unpublished storage")
	}
	if len(paths) == 0 || len(paths) > 2 {
		return errors.New("vibedb: invalid unpublished storage cleanup path count")
	}
	entry := retiredTable{
		name: "unpublished SQL storage", path: paths[0],
		journal: durable.RecoveryJournalPath(paths[0]),
		file:    file, collection: collection,
	}
	if len(paths) == 2 {
		entry.extraPath = paths[1]
		entry.extraJournal = durable.RecoveryJournalPath(paths[1])
	}
	d.retired = append(d.retired, entry)
	if len(d.retired) > maxRetiredTables {
		// Candidate construction pre-reserves this slot. Preserve ownership even
		// if an internal caller violated that invariant, but stop later DDL with
		// the typed resource bound.
		return ErrTooManyRetiredTables
	}
	return fmt.Errorf(
		"%w: unpublished SQL storage cleanup is pending retry",
		durable.ErrCommitOutcomeUnknown,
	)
}
