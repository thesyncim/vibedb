package driver

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/bits"
	"os"
	"path/filepath"

	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/store/durable"
)

const (
	distributedTransactionStateFile = "distributed-transaction-state.vjc"
	distributedTransactionMember    = "\x00distributed-transaction-state"

	// The target apply record belongs to the repository's single
	// unreleased format-0 image. The codec byte selects its sole current
	// grammar; it is a corruption sentinel, not a released format version.
	distributedTargetStateCodecSentinel = uint8(0)
	distributedTargetStateHeaderBytes   = 8
	distributedTargetStateMaxBytes      = distributedTargetStateHeaderBytes +
		binary.MaxVarintLen64 + 9 // uint64 revision plus MaxInt64 rows: 8+10+9 = 27.
	distributedTransactionStateKeyBytes   = len(distributedtxn.ID{})
	distributedTransactionStateBatchBytes = distributedTransactionStateKeyBytes +
		distributedTargetStateMaxBytes
)

var (
	ErrDistributedTransactionConflict = errors.New("vibedb: distributed transaction state conflicts with durable state")
	errDistributedAlreadyApplied      = errors.New("vibedb: distributed participant was already applied")
	distributedTargetStateMagic       = [4]byte{'V', 'D', 'P', 'A'}
)

type distributedTargetCommit struct {
	id           distributedtxn.ID
	revision     uint64
	rowsAffected int64
	document     []byte
	documentBuf  [distributedTargetStateMaxBytes]byte
}

func (t *tx) checkDistributedTargetLocked() error {
	commit := t.distributedTarget
	if commit == nil {
		return nil
	}
	collection := t.conn.db.distributedTxnCollection
	if collection == nil {
		return errors.New("vibedb: distributed transaction state collection is not open")
	}
	var rawBuf [distributedTargetStateMaxBytes]byte
	raw, found, err := collection.AppendRaw(rawBuf[:0], commit.id[:])
	if err != nil {
		return err
	}
	if !found {
		return nil
	}
	revision, rowsAffected, err := openDistributedTargetState(raw)
	if err != nil || revision != commit.revision {
		return ErrDistributedTransactionConflict
	}
	commit.rowsAffected = rowsAffected
	return errDistributedAlreadyApplied
}

func distributedTransactionStateOptions() durable.Options {
	return durable.Options{
		Durability:        durable.DurabilitySync,
		OpaqueValues:      true,
		MaxKeyBytes:       distributedTransactionStateKeyBytes,
		InlineValueBytes:  distributedTargetStateMaxBytes,
		MaxDocumentBytes:  distributedTargetStateMaxBytes,
		MaxBatchDocuments: 1,
		MaxBatchBytes:     distributedTransactionStateBatchBytes,
	}
}

func validateDistributedTransactionStateCollection(collection *durable.Collection) error {
	if collection == nil || !collection.HasOpaqueValues() ||
		collection.HasSchema() || collection.HasIndexes() ||
		!collection.HasSynchronousDurability() || !collection.SupportsUpdate() ||
		collection.MaxKeyBytes() != distributedTransactionStateKeyBytes ||
		collection.MaxDocumentBytes() != distributedTargetStateMaxBytes ||
		collection.MaxBatchDocuments() != 1 ||
		collection.MaxBatchBytes() != distributedTransactionStateBatchBytes {
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

func appendDistributedTargetState(dst []byte, revision uint64, rowsAffected int64) []byte {
	dst = append(dst, distributedTargetStateMagic[:]...)
	dst = append(dst,
		distributedTargetStateCodecSentinel,
		byte(distributedtxn.TargetApplied),
		0, 0,
	)
	dst = binary.AppendUvarint(dst, revision)
	return binary.AppendUvarint(dst, uint64(rowsAffected))
}

func openDistributedTargetState(src []byte) (revision uint64, rowsAffected int64, err error) {
	if len(src) < distributedTargetStateHeaderBytes+2 ||
		len(src) > distributedTargetStateMaxBytes ||
		src[0] != distributedTargetStateMagic[0] ||
		src[1] != distributedTargetStateMagic[1] ||
		src[2] != distributedTargetStateMagic[2] ||
		src[3] != distributedTargetStateMagic[3] ||
		src[4] != distributedTargetStateCodecSentinel ||
		src[5] != byte(distributedtxn.TargetApplied) ||
		src[6] != 0 || src[7] != 0 {
		return 0, 0, ErrDistributedTransactionConflict
	}
	revision, n := binary.Uvarint(src[distributedTargetStateHeaderBytes:])
	if n <= 0 || revision == 0 || n != distributedTargetUvarintBytes(revision) {
		return 0, 0, ErrDistributedTransactionConflict
	}
	position := distributedTargetStateHeaderBytes + n
	rows, n := binary.Uvarint(src[position:])
	if n <= 0 || rows > math.MaxInt64 || n != distributedTargetUvarintBytes(rows) {
		return 0, 0, ErrDistributedTransactionConflict
	}
	if position+n != len(src) {
		return 0, 0, ErrDistributedTransactionConflict
	}
	return revision, int64(rows), nil
}

func distributedTargetUvarintBytes(value uint64) int {
	return max(1, (bits.Len64(value)+6)/7)
}

// OpenDistributedTransactionJournal opens the shard-local transaction journal
// next to this catalog's owned table storage. ShardStoreServingClaim provides
// the single live writer exclusion; the journal remains separate from SQL data
// until target apply joins both through the catalog transaction log.
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

// DistributedTargetStatus reads the SQL-atomic target state by raw
// transaction ID. Warm inline reads decode through bounded stack scratch and
// do not allocate a payload copy.
func (d *Database) DistributedTargetStatus(id distributedtxn.ID) (revision uint64, rowsAffected int64, found bool, err error) {
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
	var rawBuf [distributedTargetStateMaxBytes]byte
	raw, found, err := core.distributedTxnCollection.AppendRaw(rawBuf[:0], id[:])
	if err != nil || !found {
		return 0, 0, found, err
	}
	revision, rowsAffected, err = openDistributedTargetState(raw)
	return revision, rowsAffected, true, err
}

// CommitDistributedTarget publishes the active SQL transaction and its
// APPLIED target state in one database transaction. An exact retry returns
// the retained affected-row count without republishing the user mutation.
func (s *Session) CommitDistributedTarget(
	ctx context.Context,
	id distributedtxn.ID,
	expectedRevision uint64,
	rowsAffected int64,
) (int64, error) {
	if err := s.live(); err != nil {
		return 0, err
	}
	if id.IsZero() || expectedRevision == 0 || expectedRevision == math.MaxUint64 || rowsAffected < 0 {
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
	commit := &distributedTargetCommit{
		id: id, revision: expectedRevision + 1, rowsAffected: rowsAffected,
	}
	commit.document = appendDistributedTargetState(
		commit.documentBuf[:0], commit.revision, rowsAffected,
	)
	tx := s.conn.tx
	tx.distributedTarget = commit
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
