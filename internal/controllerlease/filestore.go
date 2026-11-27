package controllerlease

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// FileStore is a crash-safe, cross-process durable Store backed by a directory
// of checksummed canonical binary lease files.
//
// Each key maps to <dir>/<sha256(key)>.lease guarded by an exclusive advisory
// lock file (<dir>/<sha256(key)>.lock) plus an in-process mutex. Writes use
// temp-file + fsync + atomic rename + directory fsync, and every file carries a
// trailing 32-byte SHA-256 digest so torn or corrupted files fail closed with
// ErrInvalidRecord rather than silently resetting Epoch to 0.
type FileStore struct {
	dir string
	mu  sync.Mutex
}

// NewFileStore opens or creates a FileStore rooted at dir.
func NewFileStore(dir string) (*FileStore, error) {
	if dir == "" {
		return nil, fmt.Errorf("%w: empty FileStore directory", ErrInvalidConfig)
	}
	clean := filepath.Clean(dir)
	if err := os.MkdirAll(clean, 0o700); err != nil {
		return nil, fmt.Errorf("controllerlease: create store dir %q: %w", clean, err)
	}
	return &FileStore{dir: clean}, nil
}

func (s *FileStore) keyPaths(key string) (dataPath, lockPath string) {
	sum := sha256.Sum256([]byte(key))
	name := hex.EncodeToString(sum[:16])
	return filepath.Join(s.dir, name+".lease"), filepath.Join(s.dir, name+".lock")
}

func (s *FileStore) withKeyLock(key string, fn func(dataPath string) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	dataPath, lockPath := s.keyPaths(key)
	lf, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("controllerlease: open lock %q: %w", lockPath, err)
	}
	defer lf.Close()

	if err := lockFileExclusive(lf); err != nil {
		return fmt.Errorf("controllerlease: lock %q: %w", lockPath, err)
	}
	defer func() { _ = unlockFile(lf) }()

	return fn(dataPath)
}

func readCheckedLeaseFile(path string) (LeaseRecord, bool, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return LeaseRecord{}, false, nil
		}
		return LeaseRecord{}, false, err
	}
	if len(raw) < sha256.Size+19 {
		return LeaseRecord{}, false, fmt.Errorf("%w: truncated lease file %q", ErrInvalidRecord, path)
	}
	payload := raw[:len(raw)-sha256.Size]
	trailer := raw[len(raw)-sha256.Size:]
	sum := sha256.Sum256(payload)
	if !bytes.Equal(sum[:], trailer) {
		return LeaseRecord{}, false, fmt.Errorf("%w: checksum mismatch in %q", ErrInvalidRecord, path)
	}
	rec, err := UnmarshalBinary(payload)
	if err != nil {
		return LeaseRecord{}, false, err
	}
	return rec, true, nil
}

func writeCheckedLeaseFile(dir, path string, rec LeaseRecord) error {
	payload, err := rec.MarshalBinary()
	if err != nil {
		return err
	}
	sum := sha256.Sum256(payload)
	frame := make([]byte, len(payload)+len(sum))
	copy(frame, payload)
	copy(frame[len(payload):], sum[:])

	tmp, err := os.CreateTemp(dir, ".lease-*.tmp")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmpPath)
		}
	}()
	if _, err := tmp.Write(frame); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return err
	}
	cleanup = false
	return syncDir(dir)
}

// Read implements Store.
func (s *FileStore) Read(ctx context.Context, key string) (LeaseRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return LeaseRecord{}, false, err
	}
	if key == "" {
		return LeaseRecord{}, false, ErrInvalidConfig
	}
	var (
		rec    LeaseRecord
		exists bool
	)
	err := s.withKeyLock(key, func(dataPath string) error {
		var rerr error
		rec, exists, rerr = readCheckedLeaseFile(dataPath)
		return rerr
	})
	return rec, exists, err
}

// CompareAndSwap implements Store.
func (s *FileStore) CompareAndSwap(
	ctx context.Context,
	key string,
	expected *LeaseRecord,
	next LeaseRecord,
) (bool, LeaseRecord, error) {
	if err := ctx.Err(); err != nil {
		return false, LeaseRecord{}, err
	}
	if key == "" {
		return false, LeaseRecord{}, ErrInvalidConfig
	}
	if !next.Valid() {
		return false, LeaseRecord{}, ErrInvalidRecord
	}
	var (
		swapped  bool
		observed LeaseRecord
	)
	err := s.withKeyLock(key, func(dataPath string) error {
		current, exists, rerr := readCheckedLeaseFile(dataPath)
		if rerr != nil {
			return rerr
		}
		if expected == nil {
			if exists {
				observed = current
				return nil
			}
			if werr := writeCheckedLeaseFile(s.dir, dataPath, next); werr != nil {
				return werr
			}
			swapped = true
			observed = next
			return nil
		}
		if !exists || current != *expected {
			observed = current
			return nil
		}
		if next.Epoch < current.Epoch {
			observed = current
			return ErrStaleEpoch
		}
		if werr := writeCheckedLeaseFile(s.dir, dataPath, next); werr != nil {
			return werr
		}
		swapped = true
		observed = next
		return nil
	})
	return swapped, observed, err
}
