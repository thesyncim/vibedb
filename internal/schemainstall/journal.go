package schemainstall

import (
	"context"
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"

	"github.com/thesyncim/vibedb/internal/storeio"
)

// Journal is the minimal durable CAS boundary used by Installer. Publication
// must be durable before returning nil; errors may be outcome-unknown and are
// always settled by a subsequent exact Read.
type Journal interface {
	Read(context.Context, [32]byte) (Record, error)
	Publish(context.Context, uint64, Record) error
}

type FileJournal struct {
	mu         sync.Mutex
	root       *os.Root
	lock       *os.File
	maxRecords int
	records    map[[32]byte]Record
	closed     bool
}

func OpenFileJournal(path string, maxRecords int) (*FileJournal, error) {
	if path == "" || maxRecords <= 0 || maxRecords > AbsoluteMaxRecords {
		return nil, ErrBound
	}
	path = filepath.Clean(path)
	if err := os.MkdirAll(path, 0o700); err != nil {
		return nil, err
	}
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.Join(ErrInvalid, err)
	}
	root, err := os.OpenRoot(path)
	if err != nil {
		return nil, err
	}
	lock, err := openRegular(root, "journal.lock", os.O_RDWR|os.O_CREATE, 0o600)
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

func (journal *FileJournal) Read(ctx context.Context, operation [32]byte) (Record, error) {
	if journal == nil || ctx == nil || operation == ([32]byte{}) {
		return Record{}, ErrInvalid
	}
	if cause := context.Cause(ctx); cause != nil {
		return Record{}, cause
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if journal.closed {
		return Record{}, ErrClosed
	}
	record, found := journal.records[operation]
	if !found {
		return Record{}, ErrMissing
	}
	return record, nil
}

func (journal *FileJournal) Publish(ctx context.Context, expected uint64, next Record) error {
	if journal == nil || ctx == nil || !validRecord(next) {
		return ErrInvalid
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if journal.closed {
		return ErrClosed
	}
	current, found := journal.records[next.Request.Operation]
	if expected == 0 && found || expected != 0 && (!found || current.Revision != expected) {
		return ErrConflict
	}
	if next.Revision != expected+1 || !validTransition(current, found, next) {
		return ErrConflict
	}
	if !found && len(journal.records) == journal.maxRecords {
		return ErrBound
	}
	raw, err := appendRecord(make([]byte, 0, recordBytes), next)
	if err != nil {
		return err
	}
	name := recordName(next.Request.Operation)
	temporary := "." + name + ".tmp"
	file, err := openRegular(journal.root, temporary, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	landed := false
	defer func() {
		_ = file.Close()
		if !landed {
			_ = journal.root.Remove(temporary)
		}
	}()
	if err = writeAll(file, raw); err == nil {
		err = file.Sync()
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err == nil {
		err = journal.root.Rename(temporary, name)
	}
	if err == nil {
		err = syncRoot(journal.root)
	}
	if err != nil {
		return err
	}
	landed = true
	journal.records[next.Request.Operation] = next
	return nil
}

func validTransition(current Record, found bool, next Record) bool {
	if !found {
		return next.State == StatePrepared && next.Revision == 1
	}
	if current.Request != next.Request || current.Installation != next.Installation ||
		current.State == StateDrained {
		return false
	}
	switch current.State {
	case StatePrepared:
		return next.State == StateAuthorized && validAuthorization(next.Authorization, next.Request.Operation)
	case StateAuthorized:
		return next.State == StateActive && next.Authorization == current.Authorization
	case StateActive:
		return next.State == StateDrained && next.Authorization == current.Authorization &&
			validDrainProof(next.DrainProof, current.Authorization)
	default:
		return false
	}
}

func (journal *FileJournal) recover() error {
	entries, err := fs.ReadDir(journal.root.FS(), ".")
	if err != nil {
		return err
	}
	for _, entry := range entries {
		name := entry.Name()
		if name == "journal.lock" {
			continue
		}
		if len(name) > 1 && name[0] == '.' {
			operation, ok := openTemporaryName(name)
			if !ok || entry.Type()&os.ModeSymlink != 0 {
				return ErrInvalid
			}
			_ = operation
			if err = journal.root.Remove(name); err != nil {
				return err
			}
			continue
		}
		operation, ok := openRecordName(name)
		if !ok || entry.Type()&os.ModeSymlink != 0 || entry.IsDir() {
			return ErrInvalid
		}
		if len(journal.records) == journal.maxRecords {
			return ErrBound
		}
		file, openErr := openRegular(journal.root, name, os.O_RDONLY, 0)
		if openErr != nil {
			return openErr
		}
		info, statErr := file.Stat()
		if statErr != nil || !info.Mode().IsRegular() || info.Size() != recordBytes {
			_ = file.Close()
			return errors.Join(ErrInvalid, statErr)
		}
		raw := make([]byte, recordBytes)
		_, readErr := io.ReadFull(file, raw)
		var trailing [1]byte
		count, trailingErr := file.Read(trailing[:])
		closeErr := file.Close()
		if readErr != nil || count != 0 || !errors.Is(trailingErr, io.EOF) || closeErr != nil {
			return errors.Join(ErrInvalid, readErr, trailingErr, closeErr)
		}
		record, decodeErr := openRecord(raw)
		if decodeErr != nil || record.Request.Operation != operation {
			return errors.Join(ErrInvalid, decodeErr)
		}
		if _, duplicate := journal.records[operation]; duplicate {
			return ErrInvalid
		}
		journal.records[operation] = record
	}
	return syncRoot(journal.root)
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
	err := storeio.UnlockWriter(journal.lock)
	err = errors.Join(err, journal.lock.Close(), journal.root.Close())
	return err
}

func recordName(operation [32]byte) string {
	var encoded [64]byte
	hex.Encode(encoded[:], operation[:])
	return "s-" + string(encoded[:])
}

func openRecordName(name string) ([32]byte, bool) {
	var operation [32]byte
	if len(name) != 66 || name[:2] != "s-" {
		return operation, false
	}
	n, err := hex.Decode(operation[:], []byte(name[2:]))
	return operation, err == nil && n == len(operation) && operation != ([32]byte{}) && recordName(operation) == name
}

func openTemporaryName(name string) ([32]byte, bool) {
	var operation [32]byte
	if len(name) != 72 || name[0] != '.' || name[len(name)-4:] != ".tmp" {
		return operation, false
	}
	return openRecordName(name[1 : len(name)-4])
}

func openRegular(root *os.Root, name string, flags int, mode os.FileMode) (*os.File, error) {
	before, beforeErr := root.Lstat(name)
	if beforeErr != nil && !errors.Is(beforeErr, os.ErrNotExist) {
		return nil, beforeErr
	}
	if beforeErr == nil && !before.Mode().IsRegular() {
		return nil, ErrInvalid
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
		return nil, errors.Join(ErrInvalid, afterErr, statErr)
	}
	return file, nil
}

func writeAll(writer io.Writer, raw []byte) error {
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

func syncRoot(root *os.Root) error {
	directory, err := root.Open(".")
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}

var _ Journal = (*FileJournal)(nil)
