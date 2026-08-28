package snapshottransfer

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"

	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/storeio"
)

const (
	bootstrapJournalPrefixBytes = 328
	bootstrapJournalBaseBytes   = bootstrapJournalPrefixBytes + 144 + 4
	MaxBootstrapJournalBytes    = bootstrapJournalBaseBytes + 2*replication.MaxIdentityBytes
	AbsoluteMaxBootstrapRecords = 4096
)

var bootstrapJournalMagic = [8]byte{'V', 'B', 'B', 'J', 'R', 'N', 0, 0}

// BootstrapFileJournal retains one atomically replaceable file per operation.
// Bootstrap is an infrequent topology transition, so avoiding an append log
// also avoids a separate compaction/recovery protocol and bounds live space by
// MaxRecords * MaxBootstrapJournalBytes.
type BootstrapFileJournal struct {
	mu         sync.Mutex
	root       *os.Root
	lock       *os.File
	maxRecords int
	records    map[[32]byte]BootstrapRecord
	closed     bool
}

// OpenBootstrapFileJournal opens or creates a single-writer durable journal.
// Recovery rejects noncanonical, duplicate, symlinked, or excess records.
func OpenBootstrapFileJournal(path string, maxRecords int) (*BootstrapFileJournal, error) {
	if path == "" || maxRecords <= 0 || maxRecords > AbsoluteMaxBootstrapRecords {
		return nil, ErrBound
	}
	if err := os.MkdirAll(path, 0o700); err != nil {
		return nil, err
	}
	root, err := os.OpenRoot(path)
	if err != nil {
		return nil, err
	}
	lock, err := openBootstrapJournalRegular(root, "journal.lock", os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		_ = root.Close()
		return nil, err
	}
	if info, statErr := lock.Stat(); statErr != nil || !info.Mode().IsRegular() {
		_ = lock.Close()
		_ = root.Close()
		return nil, errors.Join(ErrBootstrapControl, statErr)
	}
	if err = storeio.LockWriter(lock); err != nil {
		_ = lock.Close()
		_ = root.Close()
		return nil, err
	}
	journal := &BootstrapFileJournal{
		root: root, lock: lock, maxRecords: maxRecords,
		records: make(map[[32]byte]BootstrapRecord, maxRecords),
	}
	if err = journal.recover(); err != nil {
		_ = journal.Close()
		return nil, err
	}
	return journal, nil
}

func (journal *BootstrapFileJournal) ReadBootstrap(
	ctx context.Context,
	operation [32]byte,
) (BootstrapRecord, error) {
	if journal == nil || ctx == nil || operation == ([32]byte{}) {
		return BootstrapRecord{}, ErrBootstrapControl
	}
	if cause := context.Cause(ctx); cause != nil {
		return BootstrapRecord{}, cause
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if journal.closed {
		return BootstrapRecord{}, ErrBootstrapControl
	}
	record, found := journal.records[operation]
	if !found {
		return BootstrapRecord{}, ErrBootstrapMissing
	}
	return record, nil
}

func (journal *BootstrapFileJournal) PublishBootstrap(
	ctx context.Context,
	expected uint64,
	record BootstrapRecord,
) error {
	if journal == nil || ctx == nil || !validBootstrapRecord(record) {
		return ErrBootstrapControl
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if journal.closed {
		return ErrBootstrapControl
	}
	operation := record.Request.Operation
	current, found := journal.records[operation]
	if expected == 0 && found || expected != 0 && (!found || current.Revision != expected) {
		return ErrBootstrapConflict
	}
	if !found && len(journal.records) == journal.maxRecords {
		return ErrBound
	}
	var raw [MaxBootstrapJournalBytes]byte
	encoded, err := appendBootstrapJournalRecord(raw[:0], record)
	if err != nil {
		return err
	}
	name := bootstrapJournalRecordName(operation)
	temporary := "." + name + ".tmp"
	file, err := journal.root.OpenFile(temporary, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if errors.Is(err, os.ErrExist) {
		if removeErr := journal.root.Remove(temporary); removeErr != nil {
			return errors.Join(ErrBootstrapOutcomeUnknown, err, removeErr)
		}
		file, err = journal.root.OpenFile(temporary, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	}
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
	if err = writeBootstrapJournalBytes(file, encoded); err == nil {
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
	if err = syncBootstrapJournalRoot(journal.root); err != nil {
		return errors.Join(ErrBootstrapOutcomeUnknown, err)
	}
	return nil
}

func (journal *BootstrapFileJournal) Close() error {
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
	journal.lock, journal.root = nil, nil
	journal.records = nil
	return err
}

func (journal *BootstrapFileJournal) recover() error {
	entries, err := fs.ReadDir(journal.root.FS(), ".")
	if err != nil {
		return err
	}
	for _, entry := range entries {
		name := entry.Name()
		if name == "journal.lock" {
			continue
		}
		if len(name) > 1 && name[0] == '.' && filepath.Ext(name) == ".tmp" {
			if entry.Type()&fs.ModeType != 0 {
				return ErrBootstrapControl
			}
			if err := journal.root.Remove(name); err != nil {
				return err
			}
			continue
		}
		operation, ok := openBootstrapJournalRecordName(name)
		if !ok || entry.Type()&fs.ModeType != 0 || len(journal.records) == journal.maxRecords {
			return ErrBootstrapControl
		}
		file, err := openBootstrapJournalRegular(journal.root, name, os.O_RDONLY, 0)
		if err != nil {
			return err
		}
		info, statErr := file.Stat()
		if statErr != nil || !info.Mode().IsRegular() || info.Size() < bootstrapJournalPrefixBytes ||
			info.Size() > MaxBootstrapJournalBytes {
			_ = file.Close()
			return errors.Join(ErrBootstrapControl, statErr)
		}
		raw := make([]byte, int(info.Size()))
		_, readErr := io.ReadFull(file, raw)
		closeErr := file.Close()
		if readErr != nil || closeErr != nil {
			return errors.Join(readErr, closeErr)
		}
		record, err := openBootstrapJournalRecord(raw)
		if err != nil || record.Request.Operation != operation {
			return errors.Join(ErrBootstrapControl, err)
		}
		if _, duplicate := journal.records[operation]; duplicate {
			return ErrBootstrapControl
		}
		journal.records[operation] = record
	}
	return nil
}

func bootstrapJournalRecordName(operation [32]byte) string {
	var encoded [64]byte
	hex.Encode(encoded[:], operation[:])
	return "b-" + string(encoded[:])
}

func openBootstrapJournalRecordName(name string) ([32]byte, bool) {
	var operation [32]byte
	if len(name) != 66 || name[0] != 'b' || name[1] != '-' {
		return operation, false
	}
	decoded, err := hex.Decode(operation[:], []byte(name[2:]))
	return operation, err == nil && decoded == len(operation) && operation != ([32]byte{})
}

func appendBootstrapJournalRecord(dst []byte, record BootstrapRecord) ([]byte, error) {
	if !validBootstrapRecord(record) {
		return dst, ErrBootstrapControl
	}
	identityBytes := 0
	if record.State == BootstrapComplete {
		identityBytes = 144 + 4 + len(record.Identity.Distribution) + len(record.Identity.Shard)
	}
	length := bootstrapJournalPrefixBytes + identityBytes
	start := len(dst)
	dst = append(dst, make([]byte, length)...)
	b := dst[start:]
	copy(b[:8], bootstrapJournalMagic[:])
	b[8] = byte(record.State)
	binary.BigEndian.PutUint64(b[16:24], record.Revision)
	request, err := AppendBootstrapRequest(b[24:24], record.Request)
	if err != nil || len(request) != BootstrapRequestBytes {
		return dst[:start], errors.Join(ErrBootstrapControl, err)
	}
	copy(b[24:bootstrapJournalPrefixBytes], request)
	if record.State == BootstrapComplete {
		appendRuntimeIdentity(b[bootstrapJournalPrefixBytes:bootstrapJournalPrefixBytes+144], record.Identity)
		cursor := bootstrapJournalPrefixBytes + 144
		binary.BigEndian.PutUint16(b[cursor:cursor+2], uint16(len(record.Identity.Distribution)))
		binary.BigEndian.PutUint16(b[cursor+2:cursor+4], uint16(len(record.Identity.Shard)))
		cursor += 4
		cursor += copy(b[cursor:], record.Identity.Distribution)
		copy(b[cursor:], record.Identity.Shard)
	}
	return dst, nil
}

func openBootstrapJournalRecord(raw []byte) (BootstrapRecord, error) {
	if len(raw) < bootstrapJournalPrefixBytes || len(raw) > MaxBootstrapJournalBytes ||
		!bytes.Equal(raw[:8], bootstrapJournalMagic[:]) || !allZero(raw[9:16]) {
		return BootstrapRecord{}, ErrBootstrapControl
	}
	state := BootstrapState(raw[8])
	request, err := OpenBootstrapRequest(raw[24:bootstrapJournalPrefixBytes])
	if err != nil {
		return BootstrapRecord{}, err
	}
	record := BootstrapRecord{
		Request: request, Revision: binary.BigEndian.Uint64(raw[16:24]), State: state,
	}
	if state == BootstrapRunning {
		if len(raw) != bootstrapJournalPrefixBytes || !validBootstrapRecord(record) {
			return BootstrapRecord{}, ErrBootstrapControl
		}
		return record, nil
	}
	if state != BootstrapComplete || len(raw) < bootstrapJournalBaseBytes {
		return BootstrapRecord{}, ErrBootstrapControl
	}
	identity := openRuntimeIdentity(raw[bootstrapJournalPrefixBytes : bootstrapJournalPrefixBytes+144])
	cursor := bootstrapJournalPrefixBytes + 144
	distributionBytes := int(binary.BigEndian.Uint16(raw[cursor : cursor+2]))
	shardBytes := int(binary.BigEndian.Uint16(raw[cursor+2 : cursor+4]))
	cursor += 4
	if distributionBytes == 0 || shardBytes == 0 ||
		distributionBytes > replication.MaxIdentityBytes || shardBytes > replication.MaxIdentityBytes ||
		len(raw) != cursor+distributionBytes+shardBytes {
		return BootstrapRecord{}, ErrBootstrapControl
	}
	identity.Distribution = string(raw[cursor : cursor+distributionBytes])
	identity.Shard = string(raw[cursor+distributionBytes:])
	record.Identity = identity
	if !validBootstrapRecord(record) {
		return BootstrapRecord{}, ErrBootstrapControl
	}
	return record, nil
}

func writeBootstrapJournalBytes(writer io.Writer, raw []byte) error {
	for len(raw) != 0 {
		written, err := writer.Write(raw)
		if written > 0 {
			raw = raw[written:]
		}
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

func openBootstrapJournalRegular(
	root *os.Root,
	name string,
	flags int,
	mode os.FileMode,
) (*os.File, error) {
	before, beforeErr := root.Lstat(name)
	if beforeErr != nil && !errors.Is(beforeErr, os.ErrNotExist) {
		return nil, beforeErr
	}
	if beforeErr == nil && !before.Mode().IsRegular() {
		return nil, ErrBootstrapControl
	}
	file, err := root.OpenFile(name, flags, mode)
	if err != nil {
		return nil, err
	}
	after, afterErr := root.Lstat(name)
	opened, statErr := file.Stat()
	if afterErr != nil || statErr != nil || !after.Mode().IsRegular() ||
		!opened.Mode().IsRegular() || !os.SameFile(after, opened) ||
		beforeErr == nil && !os.SameFile(before, after) {
		_ = file.Close()
		return nil, errors.Join(ErrBootstrapControl, afterErr, statErr)
	}
	return file, nil
}

func syncBootstrapJournalRoot(root *os.Root) error {
	directory, err := root.Open(".")
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}

var _ BootstrapJournal = (*BootstrapFileJournal)(nil)
