package snapshottransfer

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
	sourceJournalHeaderBytes   = 32
	sourceJournalChecksumBytes = 4
	MaxSourceJournalBytes      = sourceJournalHeaderBytes + SourceControlRequestBytes + DescriptorBytes + sourceJournalChecksumBytes
	AbsoluteMaxSourceRecords   = 4096
)

var sourceJournalMagic = [8]byte{'V', 'B', 'S', 'J', 'R', 'N', 0, 0}
var sourceJournalCRC = crc32.MakeTable(crc32.Castagnoli)

// SourceFileJournal retains one atomically replaceable, checksummed record per
// snapshot export operation. Records are never evicted automatically: losing a
// completed identity could make a retried replica move export a different
// snapshot. Live space is bounded by maxRecords * MaxSourceJournalBytes.
type SourceFileJournal struct {
	mu         sync.Mutex
	root       *os.Root
	lock       *os.File
	maxRecords int
	records    map[[32]byte]SourceControlRecord
	closed     bool
}

// OpenSourceFileJournal opens or creates a single-writer durable source-export
// journal. Recovery fails closed on unknown entries, symlinks, corruption,
// noncanonical records, and retention-bound reductions.
func OpenSourceFileJournal(path string, maxRecords int) (*SourceFileJournal, error) {
	if path == "" || maxRecords <= 0 || maxRecords > AbsoluteMaxSourceRecords {
		return nil, ErrBound
	}
	path = filepath.Clean(path)
	if err := os.MkdirAll(path, 0o700); err != nil {
		return nil, err
	}
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.Join(ErrSourceControl, err)
	}
	root, err := os.OpenRoot(path)
	if err != nil {
		return nil, err
	}
	lock, err := openSourceJournalRegular(root, "journal.lock", os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		_ = root.Close()
		return nil, err
	}
	if err = storeio.LockWriter(lock); err != nil {
		_ = lock.Close()
		_ = root.Close()
		return nil, err
	}
	journal := &SourceFileJournal{
		root: root, lock: lock, maxRecords: maxRecords,
		records: make(map[[32]byte]SourceControlRecord, maxRecords),
	}
	if err = journal.recover(); err != nil {
		_ = journal.Close()
		return nil, err
	}
	return journal, nil
}

func (journal *SourceFileJournal) ReadSourceExport(
	ctx context.Context,
	operation [32]byte,
) (SourceControlRecord, error) {
	if journal == nil || ctx == nil || operation == ([32]byte{}) {
		return SourceControlRecord{}, ErrSourceControl
	}
	if cause := context.Cause(ctx); cause != nil {
		return SourceControlRecord{}, cause
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if journal.closed {
		return SourceControlRecord{}, ErrSourceControl
	}
	record, found := journal.records[operation]
	if !found {
		return SourceControlRecord{}, ErrSourceMissing
	}
	return record, nil
}

func (journal *SourceFileJournal) PublishSourceExport(
	ctx context.Context,
	expected uint64,
	record SourceControlRecord,
) error {
	if journal == nil || ctx == nil || !validSourceControlRecord(record) {
		return ErrSourceControl
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if journal.closed {
		return ErrSourceControl
	}
	operation := record.Request.Operation
	current, found := journal.records[operation]
	if expected == 0 && found || expected != 0 && (!found || current.Revision != expected) {
		return ErrSourceConflict
	}
	if expected == ^uint64(0) || record.Revision != expected+1 ||
		!found && (record.State != SourceControlRunning || record.Revision != 1) ||
		found && (current.Request != record.Request || current.State != SourceControlRunning ||
			record.State != SourceControlComplete) {
		return ErrSourceConflict
	}
	if !found && len(journal.records) == journal.maxRecords {
		return ErrBound
	}
	var storage [MaxSourceJournalBytes]byte
	raw, err := appendSourceJournalRecord(storage[:0], record)
	if err != nil {
		return err
	}
	name := sourceJournalName(operation)
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
	if err = writeSourceJournalBytes(file, raw); err == nil {
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
	journal.records[operation] = record
	if err = syncSourceJournalRoot(journal.root); err != nil {
		return errors.Join(ErrSourceOutcomeUnknown, err)
	}
	return nil
}

func (journal *SourceFileJournal) createTemporary(name string) (*os.File, error) {
	file, err := journal.root.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if !errors.Is(err, os.ErrExist) {
		return file, err
	}
	info, statErr := journal.root.Lstat(name)
	if statErr != nil || !info.Mode().IsRegular() {
		return nil, errors.Join(ErrSourceControl, statErr)
	}
	if removeErr := journal.root.Remove(name); removeErr != nil {
		return nil, errors.Join(ErrSourceOutcomeUnknown, err, removeErr)
	}
	return journal.root.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
}

func (journal *SourceFileJournal) Close() error {
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

func (journal *SourceFileJournal) recover() error {
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
		if _, temporary := openSourceJournalTemporaryName(name); temporary {
			if entry.Type()&fs.ModeType != 0 {
				return ErrSourceControl
			}
			if err = journal.root.Remove(name); err != nil {
				return err
			}
			removedTemporary = true
			continue
		}
		operation, ok := openSourceJournalName(name)
		if !ok || entry.Type()&fs.ModeType != 0 || len(journal.records) == journal.maxRecords {
			return ErrSourceControl
		}
		file, openErr := openSourceJournalRegular(journal.root, name, os.O_RDONLY, 0)
		if openErr != nil {
			return openErr
		}
		info, statErr := file.Stat()
		minimum := sourceJournalHeaderBytes + SourceControlRequestBytes + sourceJournalChecksumBytes
		if statErr != nil || !info.Mode().IsRegular() || info.Size() < int64(minimum) ||
			info.Size() > int64(MaxSourceJournalBytes) {
			_ = file.Close()
			return errors.Join(ErrSourceControl, statErr)
		}
		raw := make([]byte, int(info.Size()))
		_, readErr := io.ReadFull(file, raw)
		closeErr := file.Close()
		if readErr != nil || closeErr != nil {
			return errors.Join(readErr, closeErr)
		}
		record, decodeErr := openSourceJournalRecord(raw)
		if decodeErr != nil || record.Request.Operation != operation {
			return errors.Join(ErrSourceControl, decodeErr)
		}
		if _, duplicate := journal.records[operation]; duplicate {
			return ErrSourceControl
		}
		journal.records[operation] = record
	}
	if removedTemporary {
		return syncSourceJournalRoot(journal.root)
	}
	return nil
}

func appendSourceJournalRecord(dst []byte, record SourceControlRecord) ([]byte, error) {
	if !validSourceControlRecord(record) {
		return dst, ErrSourceControl
	}
	start := len(dst)
	dst = append(dst, make([]byte, sourceJournalHeaderBytes)...)
	out := dst[start:]
	copy(out[:8], sourceJournalMagic[:])
	out[8] = byte(record.State)
	binary.BigEndian.PutUint64(out[16:24], record.Revision)
	payloadStart := len(dst)
	var err error
	dst, err = AppendSourceControlRequest(dst, record.Request)
	if err != nil {
		return dst[:start], err
	}
	if record.State == SourceControlComplete {
		dst, err = AppendDescriptor(dst, record.Descriptor)
		if err != nil {
			return dst[:start], err
		}
	}
	binary.BigEndian.PutUint32(dst[start+24:start+28], uint32(len(dst)-payloadStart))
	checksum := crc32.Checksum(dst[start:], sourceJournalCRC)
	dst = binary.BigEndian.AppendUint32(dst, checksum)
	return dst, nil
}

func openSourceJournalRecord(raw []byte) (SourceControlRecord, error) {
	minimum := sourceJournalHeaderBytes + SourceControlRequestBytes + sourceJournalChecksumBytes
	if len(raw) < minimum || len(raw) > MaxSourceJournalBytes ||
		!bytes.Equal(raw[:8], sourceJournalMagic[:]) || !allZero(raw[9:16]) || !allZero(raw[28:32]) {
		return SourceControlRecord{}, ErrSourceControl
	}
	body := raw[:len(raw)-sourceJournalChecksumBytes]
	if binary.BigEndian.Uint32(raw[len(body):]) != crc32.Checksum(body, sourceJournalCRC) {
		return SourceControlRecord{}, ErrSourceControl
	}
	payloadBytes := int(binary.BigEndian.Uint32(raw[24:28]))
	if payloadBytes != len(raw)-sourceJournalHeaderBytes-sourceJournalChecksumBytes ||
		(payloadBytes != SourceControlRequestBytes && payloadBytes != SourceControlRequestBytes+DescriptorBytes) {
		return SourceControlRecord{}, ErrSourceControl
	}
	request, err := OpenSourceControlRequest(raw[sourceJournalHeaderBytes : sourceJournalHeaderBytes+SourceControlRequestBytes])
	if err != nil {
		return SourceControlRecord{}, err
	}
	record := SourceControlRecord{
		Request: request, Revision: binary.BigEndian.Uint64(raw[16:24]), State: SourceControlState(raw[8]),
	}
	if payloadBytes == SourceControlRequestBytes+DescriptorBytes {
		record.Descriptor, err = OpenDescriptor(body[sourceJournalHeaderBytes+SourceControlRequestBytes:])
		if err != nil {
			return SourceControlRecord{}, err
		}
	}
	if !validSourceControlRecord(record) {
		return SourceControlRecord{}, ErrSourceControl
	}
	canonical, err := appendSourceJournalRecord(nil, record)
	if err != nil || !bytes.Equal(canonical, raw) {
		return SourceControlRecord{}, errors.Join(ErrSourceControl, err)
	}
	return record, nil
}

func sourceJournalName(operation [32]byte) string {
	var encoded [64]byte
	hex.Encode(encoded[:], operation[:])
	return "s-" + string(encoded[:])
}

func openSourceJournalName(name string) ([32]byte, bool) {
	var operation [32]byte
	if len(name) != 66 || name[:2] != "s-" {
		return operation, false
	}
	n, err := hex.Decode(operation[:], []byte(name[2:]))
	return operation, err == nil && n == len(operation) && operation != ([32]byte{}) &&
		sourceJournalName(operation) == name
}

func openSourceJournalTemporaryName(name string) ([32]byte, bool) {
	if len(name) != 71 || name[0] != '.' || name[67:] != ".tmp" {
		return [32]byte{}, false
	}
	return openSourceJournalName(name[1:67])
}

func openSourceJournalRegular(root *os.Root, name string, flags int, mode os.FileMode) (*os.File, error) {
	before, beforeErr := root.Lstat(name)
	if beforeErr != nil && !errors.Is(beforeErr, os.ErrNotExist) {
		return nil, beforeErr
	}
	if beforeErr == nil && !before.Mode().IsRegular() {
		return nil, ErrSourceControl
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
		return nil, errors.Join(ErrSourceControl, afterErr, statErr)
	}
	return file, nil
}

func writeSourceJournalBytes(writer io.Writer, raw []byte) error {
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

func syncSourceJournalRoot(root *os.Root) error {
	directory, err := root.Open(".")
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}

var _ SourceControlJournal = (*SourceFileJournal)(nil)
