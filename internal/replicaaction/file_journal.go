package replicaaction

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"hash/crc32"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"

	"github.com/thesyncim/vibedb/internal/storeio"
)

const (
	replicaActionJournalHeaderBytes   = 32
	replicaActionJournalChecksumBytes = 4
	MaxReplicaActionJournalBytes      = replicaActionJournalHeaderBytes + MaxRequestBytes + replicaActionJournalChecksumBytes
	AbsoluteMaxReplicaActionRecords   = 4096
)

var replicaActionJournalMagic = [8]byte{'V', 'B', 'R', 'A', 'J', 'R', 'N', 0}
var replicaActionJournalCRC = crc32.MakeTable(crc32.Castagnoli)

// FileJournal retains one atomically replaceable, checksummed record per
// operation. It fails closed at maxRecords instead of evicting a completed
// identity: forgetting a topology action could make a later retry execute it
// twice. Live space is bounded by maxRecords * MaxReplicaActionJournalBytes.
type FileJournal struct {
	mu         sync.Mutex
	root       *os.Root
	lock       *os.File
	maxRecords int
	records    map[[32]byte]Record
	closed     bool
}

// OpenFileJournal opens or creates a single-writer replica-action journal.
// Recovery rejects unknown names, symlinks, noncanonical bytes, corruption,
// duplicate identities, and a record count above the configured bound.
func OpenFileJournal(path string, maxRecords int) (*FileJournal, error) {
	if path == "" || maxRecords <= 0 || maxRecords > AbsoluteMaxReplicaActionRecords {
		return nil, ErrBound
	}
	path = filepath.Clean(path)
	if err := os.MkdirAll(path, 0o700); err != nil {
		return nil, err
	}
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.Join(ErrControl, err)
	}
	root, err := os.OpenRoot(path)
	if err != nil {
		return nil, err
	}
	lock, err := openReplicaActionRegular(root, "journal.lock", os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		_ = root.Close()
		return nil, err
	}
	if err = storeio.LockWriter(lock); err != nil {
		_ = lock.Close()
		_ = root.Close()
		return nil, err
	}
	journal := &FileJournal{root: root, lock: lock, maxRecords: maxRecords,
		records: make(map[[32]byte]Record, maxRecords)}
	if err = journal.recover(); err != nil {
		_ = journal.Close()
		return nil, err
	}
	return journal, nil
}

func (journal *FileJournal) ReadReplicaAction(ctx context.Context, operation [32]byte) (Record, error) {
	if journal == nil || ctx == nil || operation == ([32]byte{}) {
		return Record{}, ErrControl
	}
	if cause := context.Cause(ctx); cause != nil {
		return Record{}, cause
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if journal.closed {
		return Record{}, ErrControl
	}
	record, ok := journal.records[operation]
	if !ok {
		return Record{}, ErrMissing
	}
	record.Request = cloneRequest(record.Request)
	return record, nil
}

func (journal *FileJournal) PublishReplicaAction(ctx context.Context, expected uint64, record Record) error {
	if journal == nil || ctx == nil || !validRecord(record) {
		return ErrControl
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if journal.closed {
		return ErrControl
	}
	operation := record.Request.Operation
	current, found := journal.records[operation]
	if expected == 0 && found || expected != 0 && (!found || current.Revision != expected) {
		return ErrConflict
	}
	if expected == ^uint64(0) || record.Revision != expected+1 ||
		!found && (record.State != Running || record.Revision != 1) ||
		found && (!equalRequest(current.Request, record.Request) || current.State != Running || record.State != Complete) {
		return ErrConflict
	}
	if !found && len(journal.records) == journal.maxRecords {
		return ErrBound
	}
	var storage [MaxReplicaActionJournalBytes]byte
	raw, err := appendReplicaActionJournalRecord(storage[:0], record)
	if err != nil {
		return err
	}
	name := replicaActionJournalName(operation)
	temporary := "." + name + ".tmp"
	file, err := journal.createTemporary(temporary)
	if err != nil {
		return err
	}
	renamed := false
	defer func() {
		_ = file.Close()
		if !renamed {
			_ = journal.root.Remove(temporary)
		}
	}()
	if err = writeReplicaActionBytes(file, raw); err == nil {
		err = file.Sync()
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = journal.root.Rename(temporary, name); err != nil {
		return err
	}
	renamed = true
	journal.records[operation] = cloneRecord(record)
	if err = syncReplicaActionRoot(journal.root); err != nil {
		return errors.Join(ErrOutcomeUnknown, err)
	}
	return nil
}

func (journal *FileJournal) createTemporary(name string) (*os.File, error) {
	file, err := journal.root.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if !errors.Is(err, os.ErrExist) {
		return file, err
	}
	info, statErr := journal.root.Lstat(name)
	if statErr != nil || !info.Mode().IsRegular() {
		return nil, errors.Join(ErrControl, statErr)
	}
	if removeErr := journal.root.Remove(name); removeErr != nil {
		return nil, errors.Join(ErrOutcomeUnknown, err, removeErr)
	}
	return journal.root.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
}

func (journal *FileJournal) Close() error {
	if journal == nil {
		return nil
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if journal.closed {
		return nil
	}
	journal.closed = true
	err := errors.Join(storeio.UnlockWriter(journal.lock), journal.lock.Close(), journal.root.Close())
	journal.lock, journal.root, journal.records = nil, nil, nil
	return err
}

func (journal *FileJournal) recover() error {
	entries, err := fs.ReadDir(journal.root.FS(), ".")
	if err != nil {
		return err
	}
	removedTemporary := false
	for _, entry := range entries {
		name := entry.Name()
		if name == "journal.lock" {
			continue
		}
		if len(name) > 1 && name[0] == '.' && filepath.Ext(name) == ".tmp" {
			if entry.Type()&fs.ModeType != 0 {
				return ErrControl
			}
			if err = journal.root.Remove(name); err != nil {
				return err
			}
			removedTemporary = true
			continue
		}
		operation, ok := openReplicaActionJournalName(name)
		if !ok || entry.Type()&fs.ModeType != 0 || len(journal.records) == journal.maxRecords {
			return ErrControl
		}
		file, openErr := openReplicaActionRegular(journal.root, name, os.O_RDONLY, 0)
		if openErr != nil {
			return openErr
		}
		info, statErr := file.Stat()
		if statErr != nil || !info.Mode().IsRegular() || info.Size() < replicaActionJournalHeaderBytes+requestHeaderBytes+replicaActionJournalChecksumBytes || info.Size() > MaxReplicaActionJournalBytes {
			_ = file.Close()
			return errors.Join(ErrControl, statErr)
		}
		raw := make([]byte, int(info.Size()))
		_, readErr := io.ReadFull(file, raw)
		closeErr := file.Close()
		if readErr != nil || closeErr != nil {
			return errors.Join(readErr, closeErr)
		}
		record, decodeErr := openReplicaActionJournalRecord(raw)
		if decodeErr != nil || record.Request.Operation != operation {
			return errors.Join(ErrControl, decodeErr)
		}
		if _, duplicate := journal.records[operation]; duplicate {
			return ErrControl
		}
		journal.records[operation] = record
	}
	if removedTemporary {
		return syncReplicaActionRoot(journal.root)
	}
	return nil
}

func appendReplicaActionJournalRecord(dst []byte, record Record) ([]byte, error) {
	if !validRecord(record) {
		return dst, ErrControl
	}
	start := len(dst)
	dst = append(dst, make([]byte, replicaActionJournalHeaderBytes)...)
	out := dst[start:]
	copy(out[:8], replicaActionJournalMagic[:])
	out[8] = byte(record.State)
	binary.LittleEndian.PutUint64(out[16:24], record.Revision)
	requestStart := len(dst)
	var err error
	dst, err = AppendRequest(dst, record.Request)
	if err != nil {
		return dst[:start], err
	}
	binary.LittleEndian.PutUint32(dst[start+24:start+28], uint32(len(dst)-requestStart))
	checksum := crc32.Checksum(dst[start:], replicaActionJournalCRC)
	dst = binary.LittleEndian.AppendUint32(dst, checksum)
	return dst, nil
}

func openReplicaActionJournalRecord(raw []byte) (Record, error) {
	if len(raw) < replicaActionJournalHeaderBytes+requestHeaderBytes+replicaActionJournalChecksumBytes ||
		len(raw) > MaxReplicaActionJournalBytes || !bytes.Equal(raw[:8], replicaActionJournalMagic[:]) ||
		!zero(raw[9:16]) || !zero(raw[28:32]) {
		return Record{}, ErrControl
	}
	body := raw[:len(raw)-replicaActionJournalChecksumBytes]
	if binary.LittleEndian.Uint32(raw[len(body):]) != crc32.Checksum(body, replicaActionJournalCRC) {
		return Record{}, ErrControl
	}
	requestBytes := int(binary.LittleEndian.Uint32(raw[24:28]))
	if requestBytes != len(raw)-replicaActionJournalHeaderBytes-replicaActionJournalChecksumBytes {
		return Record{}, ErrControl
	}
	request, err := OpenRequest(raw[replicaActionJournalHeaderBytes:len(body)])
	if err != nil {
		return Record{}, err
	}
	record := Record{Request: request, Revision: binary.LittleEndian.Uint64(raw[16:24]), State: State(raw[8])}
	if !validRecord(record) {
		return Record{}, ErrControl
	}
	canonical, err := appendReplicaActionJournalRecord(nil, record)
	if err != nil || !bytes.Equal(canonical, raw) {
		return Record{}, errors.Join(ErrControl, err)
	}
	return record, nil
}

func replicaActionJournalName(operation [32]byte) string {
	var encoded [64]byte
	hex.Encode(encoded[:], operation[:])
	return "a-" + string(encoded[:])
}

func openReplicaActionJournalName(name string) ([32]byte, bool) {
	var operation [32]byte
	if len(name) != 66 || name[:2] != "a-" {
		return operation, false
	}
	n, err := hex.Decode(operation[:], []byte(name[2:]))
	return operation, err == nil && n == len(operation) && operation != ([32]byte{}) &&
		replicaActionJournalName(operation) == name
}

func cloneRecord(record Record) Record {
	record.Request = cloneRequest(record.Request)
	return record
}

func openReplicaActionRegular(root *os.Root, name string, flags int, mode os.FileMode) (*os.File, error) {
	before, beforeErr := root.Lstat(name)
	if beforeErr != nil && !errors.Is(beforeErr, os.ErrNotExist) {
		return nil, beforeErr
	}
	if beforeErr == nil && !before.Mode().IsRegular() {
		return nil, ErrControl
	}
	file, err := root.OpenFile(name, flags, mode)
	if err != nil {
		return nil, err
	}
	after, afterErr := root.Lstat(name)
	opened, statErr := file.Stat()
	if afterErr != nil || statErr != nil || !after.Mode().IsRegular() || !opened.Mode().IsRegular() ||
		!os.SameFile(after, opened) || beforeErr == nil && !os.SameFile(before, after) {
		_ = file.Close()
		return nil, errors.Join(ErrControl, afterErr, statErr)
	}
	return file, nil
}

func writeReplicaActionBytes(writer io.Writer, raw []byte) error {
	for len(raw) != 0 {
		n, err := writer.Write(raw)
		if n > 0 {
			raw = raw[n:]
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

func syncReplicaActionRoot(root *os.Root) error {
	directory, err := root.Open(".")
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}

var _ Journal = (*FileJournal)(nil)
