package rangesplit

import (
	"bytes"
	cryptorand "crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/thesyncim/vibedb/internal/storeio"
)

// ChildStageCursorStore owns one crash-durable atomically replaced cursor and
// one advisory writer lease in a pinned directory namespace.
type ChildStageCursorStore struct {
	mu sync.Mutex

	root     *os.Root
	base     string
	lockFile *os.File
	raw      [childStageCursorBytes]byte
	cursor   ChildStageCursor
	codec    ChildStageCursorWorkspace
	has      bool
	closed   bool
}

// OpenChildStageCursorStore opens or creates the writer lease for path and
// strictly recovers its current fixed-size cursor when present.
func OpenChildStageCursorStore(path string) (*ChildStageCursorStore, error) {
	if path == "" {
		return nil, fmt.Errorf("%w: empty cursor path", ErrChildStage)
	}
	parent, base := filepath.Dir(path), filepath.Base(path)
	if base == "" || base == "." || base == ".." ||
		filepath.Join(parent, base) != filepath.Clean(path) {
		return nil, fmt.Errorf("%w: cursor path", ErrChildStage)
	}
	root, err := os.OpenRoot(parent)
	if err != nil {
		return nil, err
	}
	lockFile, err := openChildStageCursorEntry(
		root, base+".lock", os.O_RDWR|os.O_CREATE, 0o600,
	)
	if err != nil {
		_ = root.Close()
		return nil, err
	}
	if err := storeio.LockWriter(lockFile); err != nil {
		_ = lockFile.Close()
		_ = root.Close()
		return nil, fmt.Errorf("%w: cursor writer: %v", ErrChildStage, err)
	}
	store := &ChildStageCursorStore{root: root, base: base, lockFile: lockFile}
	if err := store.readCurrent(); err != nil {
		_ = store.Close()
		return nil, err
	}
	return store, nil
}

// Load appends a detached current cursor to dst. A false result means that no
// cursor exists. Reusing dst avoids an allocation.
func (s *ChildStageCursorStore) Load(dst []byte) ([]byte, bool, error) {
	if s == nil {
		return dst, false, ErrChildStage
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return dst, false, ErrChildStage
	}
	if !s.has {
		return dst, false, nil
	}
	return append(dst, s.raw[:]...), true, nil
}

// Persist atomically advances the cursor. Exact retries are idempotent. A
// regression or cursor from another child artifact fails closed.
func (s *ChildStageCursorStore) Persist(raw []byte) error {
	if s == nil {
		return ErrChildStage
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrChildStage
	}
	next, err := decodeChildStageCursor(raw, &s.codec)
	if err != nil {
		return err
	}
	if s.has && bytes.Equal(raw, s.raw[:]) {
		return nil
	}
	if s.has && !validChildStageCursorAdvance(s.cursor, next) {
		return fmt.Errorf("%w: cursor does not monotonically advance", ErrChildStage)
	}
	temporary, err := s.createTemporary()
	if err != nil {
		return err
	}
	temporaryBase := filepath.Base(temporary.Name())
	renamed := false
	defer func() {
		if !renamed {
			_ = s.root.Remove(temporaryBase)
		}
	}()
	if err := writeChildArtifactBytes(temporary, raw); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := replaceChildStageCursorEntry(s.root, temporaryBase, s.base); err != nil {
		return err
	}
	renamed = true
	if err := syncChildStageCursorRoot(s.root); err != nil {
		return errors.Join(ErrChildStageOutcomeUnknown, err)
	}
	copy(s.raw[:], raw)
	s.cursor = next
	s.has = true
	return nil
}

func (s *ChildStageCursorStore) readCurrent() error {
	file, err := openChildStageCursorEntry(s.root, s.base, os.O_RDONLY, 0)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if info.Size() != childStageCursorBytes {
		return fmt.Errorf("%w: cursor file size", ErrChildStage)
	}
	if _, err := io.ReadFull(file, s.raw[:]); err != nil {
		return err
	}
	cursor, err := decodeChildStageCursor(s.raw[:], &s.codec)
	if err != nil {
		return err
	}
	s.cursor = cursor
	s.has = true
	return nil
}

func (s *ChildStageCursorStore) createTemporary() (*os.File, error) {
	var nonce [16]byte
	for range 100 {
		if _, err := cryptorand.Read(nonce[:]); err != nil {
			return nil, err
		}
		name := "." + s.base + ".tmp-" + hex.EncodeToString(nonce[:])
		file, err := s.root.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
		if err == nil {
			return file, nil
		}
		if !errors.Is(err, os.ErrExist) {
			return nil, err
		}
	}
	return nil, fmt.Errorf("%w: cannot allocate cursor temporary", ErrChildStage)
}

// Close releases the writer lease and pinned namespace. It is idempotent.
func (s *ChildStageCursorStore) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	err := errors.Join(storeio.UnlockWriter(s.lockFile), s.lockFile.Close(), s.root.Close())
	s.lockFile = nil
	s.root = nil
	return err
}

func openChildStageCursorEntry(
	root *os.Root,
	name string,
	flags int,
	mode os.FileMode,
) (*os.File, error) {
	var before os.FileInfo
	var err error
	if flags&os.O_CREATE == 0 {
		before, err = root.Lstat(name)
		if err != nil {
			return nil, err
		}
		if !before.Mode().IsRegular() {
			return nil, fmt.Errorf("%w: cursor entry is not regular", ErrChildStage)
		}
	}
	file, err := root.OpenFile(name, flags, mode)
	if err != nil {
		return nil, err
	}
	opened, openErr := file.Stat()
	entry, entryErr := root.Lstat(name)
	stable := openErr == nil && entryErr == nil && opened.Mode().IsRegular() &&
		entry.Mode().IsRegular() && os.SameFile(opened, entry)
	if before != nil {
		stable = stable && os.SameFile(before, entry)
	}
	if !stable {
		_ = file.Close()
		if openErr != nil {
			return nil, openErr
		}
		if entryErr != nil {
			return nil, entryErr
		}
		return nil, fmt.Errorf("%w: unstable cursor entry", ErrChildStage)
	}
	return file, nil
}

func validChildStageCursorAdvance(current, next ChildStageCursor) bool {
	if current.child != next.child || current.planDigest != next.planDigest ||
		current.placementDigest != next.placementDigest ||
		current.artifactDigest != next.artifactDigest ||
		current.headerDigest != next.headerDigest ||
		current.baseDigest != next.baseDigest {
		return false
	}
	switch current.phase {
	case ChildStageArtifact:
		if next.routeGeneration != current.routeGeneration {
			return false
		}
		if next.phase == ChildStageArtifact {
			return next.applied == current.applied && next.term == current.term &&
				next.dataChainDigest == current.dataChainDigest &&
				next.entryDigest == current.entryDigest &&
				next.artifactChunks > current.artifactChunks &&
				next.artifactRows >= current.artifactRows &&
				next.artifactPayload >= current.artifactPayload &&
				next.artifactOffset > current.artifactOffset
		}
		return next.phase == ChildStageTail && next.applied == current.applied &&
			next.term == current.term && next.dataChainDigest == current.dataChainDigest &&
			next.entryDigest == current.entryDigest &&
			next.lastBatchDigest == ([32]byte{}) &&
			next.artifactChunks >= current.artifactChunks &&
			next.artifactRows >= current.artifactRows &&
			next.artifactPayload >= current.artifactPayload &&
			next.artifactOffset > current.artifactOffset
	case ChildStageTail:
		if current.applied == ^uint64(0) ||
			(next.phase != ChildStageTail && next.phase != ChildStageSealed) ||
			next.applied != current.applied+1 || next.term < current.term ||
			next.artifactChunks != current.artifactChunks ||
			next.artifactRows != current.artifactRows ||
			next.artifactPayload != current.artifactPayload ||
			next.artifactOffset != current.artifactOffset ||
			next.lastChunkDigest != current.lastChunkDigest ||
			next.lastBatchDigest == ([32]byte{}) {
			return false
		}
		if next.phase == ChildStageTail {
			return next.routeGeneration == current.routeGeneration &&
				next.imageRows == 0 && next.imageBytes == 0 &&
				next.imageDigest == ([32]byte{})
		}
		return current.routeGeneration != ^uint64(0) &&
			next.routeGeneration == current.routeGeneration+1 &&
			next.imageDigest != ([32]byte{}) &&
			(next.imageRows != 0 || next.imageBytes == 0)
	case ChildStageSealed:
		return false
	default:
		return false
	}
}
