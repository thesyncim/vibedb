package driver

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/store/durable"
	"github.com/thesyncim/vibejson/x/byteview"
)

const (
	distributedTransactionStateFile = "distributed-transaction-state.vjc"
	distributedTransactionMember    = "\x00distributed-transaction-state"
)

var (
	ErrDistributedTransactionConflict = errors.New("vibedb: distributed transaction state conflicts with durable state")
	errDistributedAlreadyApplied      = errors.New("vibedb: distributed participant was already applied")
)

type distributedParticipantCommit struct {
	id           distributedtxn.ID
	revision     uint64
	rowsAffected int64
	document     []byte
}

func (t *tx) checkDistributedParticipantLocked() error {
	commit := t.distributedParticipant
	if commit == nil {
		return nil
	}
	collection := t.conn.db.distributedTxnCollection
	if collection == nil {
		return errors.New("vibedb: distributed transaction state collection is not open")
	}
	raw, found, err := collection.AppendRaw(nil, commit.id[:])
	if err != nil {
		return err
	}
	if !found {
		return nil
	}
	revision, rowsAffected, err := openDistributedParticipantState(raw)
	if err != nil || revision != commit.revision {
		return ErrDistributedTransactionConflict
	}
	commit.rowsAffected = rowsAffected
	return errDistributedAlreadyApplied
}

func distributedTransactionStateOptions() durable.Options {
	return durable.Options{
		Durability:  durable.DurabilitySync,
		MaxKeyBytes: 16, InlineValueBytes: 64, MaxDocumentBytes: 64,
		MaxBatchDocuments: 1, MaxBatchBytes: 80,
	}
}

func validateDistributedTransactionStateCollection(collection *durable.Collection) error {
	if collection == nil || collection.HasSchema() || collection.HasIndexes() ||
		!collection.HasSynchronousDurability() || !collection.SupportsUpdate() ||
		collection.MaxKeyBytes() != 16 || collection.MaxDocumentBytes() != 64 ||
		collection.MaxBatchDocuments() != 1 || collection.MaxBatchBytes() != 80 {
		return errors.New("vibedb: distributed transaction state collection profile mismatch")
	}
	return nil
}

func (d *database) distributedTransactionStatePath() string {
	if d == nil || d.catalog.ShardStore == nil {
		return ""
	}
	return filepath.Join(d.dataDir, distributedTransactionStateFile)
}

func (d *database) ensureDistributedTransactionStateLocked() error {
	if d.distributedTxnCollection != nil {
		return validateDistributedTransactionStateCollection(d.distributedTxnCollection)
	}
	path := d.distributedTransactionStatePath()
	if path == "" {
		return errors.New("vibedb: distributed transactions require a direct shard store")
	}
	if _, err := os.Stat(path); err == nil {
		return errors.New("vibedb: distributed transaction state exists but was not recovered")
	} else if !os.IsNotExist(err) {
		return err
	}
	if err := d.ensureDataDir(); err != nil {
		return err
	}
	file, err := createPublishableTableTemp(d.dataDir, ".distributed-transaction-state.tmp-")
	if err != nil {
		return err
	}
	tmpPath := file.Name()
	options := distributedTransactionStateOptions()
	collection, err := durable.Create(file, options)
	if err != nil {
		return errors.Join(err, d.discardUnpublishedStorageLocked(collection, file, tmpPath))
	}
	file, collection, err = d.publishTableStorageLocked(tmpPath, path, file, collection, options)
	if err != nil {
		return fmt.Errorf("vibedb: publish distributed transaction state: %w", err)
	}
	if err := validateDistributedTransactionStateCollection(collection); err != nil {
		return errors.Join(err, d.discardUnpublishedStorageLocked(collection, file, path))
	}
	if err := d.txnLog.AdoptCollection(collection); err != nil {
		return errors.Join(err, d.discardUnpublishedStorageLocked(collection, file, path))
	}
	d.distributedTxnFile = file
	d.distributedTxnCollection = collection
	return nil
}

func appendDistributedParticipantState(dst []byte, revision uint64, rowsAffected int64) []byte {
	dst = append(dst, '[', byte('0'+distributedtxn.ParticipantApplied), ',')
	dst = strconv.AppendUint(dst, revision, 10)
	dst = append(dst, ',')
	dst = strconv.AppendInt(dst, rowsAffected, 10)
	return append(dst, ']')
}

func openDistributedParticipantState(src []byte) (revision uint64, rowsAffected int64, err error) {
	if len(src) < 7 || src[0] != '[' || src[1] != byte('0'+distributedtxn.ParticipantApplied) ||
		src[2] != ',' || src[len(src)-1] != ']' {
		return 0, 0, ErrDistributedTransactionConflict
	}
	comma := -1
	for i := 3; i < len(src)-1; i++ {
		if src[i] == ',' {
			comma = i
			break
		}
	}
	if comma < 0 {
		return 0, 0, ErrDistributedTransactionConflict
	}
	revision, err = strconv.ParseUint(byteview.String(src[3:comma]), 10, 64)
	if err != nil || revision == 0 {
		return 0, 0, ErrDistributedTransactionConflict
	}
	rowsAffected, err = strconv.ParseInt(byteview.String(src[comma+1:len(src)-1]), 10, 64)
	if err != nil || rowsAffected < 0 {
		return 0, 0, ErrDistributedTransactionConflict
	}
	return revision, rowsAffected, nil
}

// OpenDistributedTransactionJournal opens the shard-local transaction journal
// next to this catalog's owned table storage. ShardStoreServingClaim provides
// the single live writer exclusion; the journal remains separate from SQL data
// until participant apply joins both through the catalog transaction log.
func (d *Database) OpenDistributedTransactionJournal() (*distributedtxn.Journal, error) {
	if d == nil || d.connector == nil {
		return nil, ErrDatabaseClosed
	}
	d.connector.mu.Lock()
	if d.connector.closed || d.connector.db == nil {
		d.connector.mu.Unlock()
		return nil, ErrDatabaseClosed
	}
	core := d.connector.db
	d.connector.mu.Unlock()

	core.mu.Lock()
	if core.closed {
		core.mu.Unlock()
		return nil, ErrDatabaseClosed
	}
	if err := core.ensureDistributedTransactionStateLocked(); err != nil {
		core.mu.Unlock()
		return nil, err
	}
	path := filepath.Join(core.dataDir, "distributed-transactions.vtj")
	core.mu.Unlock()
	journal, err := distributedtxn.OpenJournal(path)
	if err != nil {
		return nil, errors.Join(errors.New("vibedb: open distributed transaction journal"), err)
	}
	return journal, nil
}

// DistributedParticipantStatus reads the SQL-atomic participant state by raw
// transaction ID. It allocates only the tiny state document copy-out.
func (d *Database) DistributedParticipantStatus(id distributedtxn.ID) (revision uint64, rowsAffected int64, found bool, err error) {
	if d == nil || d.connector == nil {
		return 0, 0, false, ErrDatabaseClosed
	}
	d.connector.mu.Lock()
	if d.connector.closed || d.connector.db == nil {
		d.connector.mu.Unlock()
		return 0, 0, false, ErrDatabaseClosed
	}
	core := d.connector.db
	d.connector.mu.Unlock()
	core.mu.RLock()
	defer core.mu.RUnlock()
	if core.closed || core.distributedTxnCollection == nil {
		return 0, 0, false, ErrDatabaseClosed
	}
	raw, found, err := core.distributedTxnCollection.AppendRaw(nil, id[:])
	if err != nil || !found {
		return 0, 0, found, err
	}
	revision, rowsAffected, err = openDistributedParticipantState(raw)
	return revision, rowsAffected, true, err
}

// CommitDistributedParticipant publishes the active SQL transaction and its
// APPLIED participant state in one database transaction. An exact retry returns
// the retained affected-row count without republishing the user mutation.
func (s *Session) CommitDistributedParticipant(
	ctx context.Context,
	id distributedtxn.ID,
	expectedRevision uint64,
	rowsAffected int64,
) (int64, error) {
	if err := s.live(); err != nil {
		return 0, err
	}
	if id.IsZero() || expectedRevision == 0 || rowsAffected < 0 {
		return 0, ErrDistributedTransactionConflict
	}
	if s.current != nil {
		return 0, ErrCursorOpen
	}
	if s.state != SessionInTransaction || s.conn.tx == nil {
		return 0, ErrNoTransaction
	}
	if err := contextCheckpoint(ctx); err != nil {
		s.state = SessionFailedTransaction
		return 0, err
	}
	commit := &distributedParticipantCommit{
		id: id, revision: expectedRevision + 1, rowsAffected: rowsAffected,
	}
	commit.document = appendDistributedParticipantState(nil, commit.revision, rowsAffected)
	tx := s.conn.tx
	tx.distributedParticipant = commit
	err := tx.Commit()
	s.state = SessionIdle
	if errors.Is(err, errDistributedAlreadyApplied) {
		return commit.rowsAffected, nil
	}
	if err != nil {
		return 0, err
	}
	return rowsAffected, nil
}
