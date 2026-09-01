package seglog

import (
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
)

type RotationPhase uint8

const (
	RotationSealedSynced RotationPhase = iota + 1
	RotationSealedRenamed
	RotationNextPublished
	RotationManifestPublished
)

// Log owns one directory. Append writes only the active segment; Sync is the
// publication boundary that makes its current offset recoverable.
type Log struct {
	dir           string
	active        *os.File
	activeOffset  uint64
	activeHash    hash.Hash
	digestScratch [32]byte
	manifest      Manifest
	records       uint64
	events        []segmentEvent
	eventSpare    []segmentEvent
	poisoned      error
	publishHook   func(Manifest) error
	authKey       [32]byte
}

func activeName(id uint64) string { return fmt.Sprintf("%020d.active", id) }
func sealedName(id uint64) string { return fmt.Sprintf("%020d.seg", id) }

func createLog(dir string) (*Log, error) {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, err
	}
	if _, err := os.Stat(filepath.Join(dir, ManifestName)); err == nil {
		return nil, os.ErrExist
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	var logID [16]byte
	if _, err := rand.Read(logID[:]); err != nil {
		return nil, err
	}
	h := segmentHeader{ID: 1, Generation: 1, LogID: logID}
	f, err := createSegment(dir, h)
	if err != nil {
		return nil, err
	}
	m := Manifest{Generation: 1, ActiveID: 1, ActiveGeneration: 1, DurableSegmentID: 1, DurableOffset: segmentHeaderBytes, LogID: logID}
	if err = publishManifest(dir, m); err != nil {
		_ = f.Close()
		return nil, err
	}
	activeHash := sha256.New()
	_, _ = activeHash.Write(marshalSegmentHeader(h))
	return &Log{dir: dir, active: f, activeOffset: segmentHeaderBytes, activeHash: activeHash, manifest: m}, nil
}

func (l *Log) Close() error {
	if l.active == nil {
		return nil
	}
	err := l.active.Close()
	l.active = nil
	return err
}

func (l *Log) usable() error {
	if l.poisoned != nil {
		return l.poisoned
	}
	if l.active == nil {
		return os.ErrClosed
	}
	return nil
}
func (l *Log) poison(err error) error {
	if err == nil {
		err = errors.New("ambiguous storage mutation")
	}
	if l.poisoned == nil {
		l.poisoned = errors.Join(ErrPoisoned, err)
	}
	return l.poisoned
}
func (l *Log) publish(m Manifest) error {
	if l.publishHook != nil {
		return l.publishHook(m)
	}
	return publishManifest(l.dir, m)
}
func (l *Log) ReserveEvents(capacity int) error {
	if err := l.usable(); err != nil {
		return err
	}
	if capacity < len(l.events) {
		return ErrBounds
	}
	if capacity <= cap(l.events) && capacity <= cap(l.eventSpare) {
		return nil
	}
	events := make([]segmentEvent, len(l.events), capacity)
	copy(events, l.events)
	l.events = events
	if capacity > cap(l.eventSpare) {
		l.eventSpare = make([]segmentEvent, 0, capacity)
	}
	return nil
}

func (l *Log) Sync() error {
	if err := l.usable(); err != nil {
		return err
	}
	if err := l.active.Sync(); err != nil {
		return l.poison(err)
	}
	next := l.manifest
	next.Generation++
	next.DurableOffset = l.activeOffset
	if err := l.publish(next); err != nil {
		return l.poison(err)
	}
	l.manifest = next
	return nil
}

// SetTruncate records a durable, logical prefix boundary. Existing locations
// at or below index disappear from the in-memory index; segment bytes remain
// immutable until a future background reclaimer deletes whole segments.
func createSegment(dir string, h segmentHeader) (*os.File, error) {
	tmp := filepath.Join(dir, fmt.Sprintf(".%020d.tmp", h.ID))
	final := filepath.Join(dir, activeName(h.ID))
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o640)
	if err != nil {
		return nil, err
	}
	ok := false
	defer func() {
		if !ok {
			_ = f.Close()
			_ = os.Remove(tmp)
		}
	}()
	headerBytes := marshalSegmentHeader(h)
	if n, writeErr := f.Write(headerBytes); writeErr != nil {
		return nil, writeErr
	} else if n != len(headerBytes) {
		return nil, io.ErrShortWrite
	}
	if err = f.Sync(); err != nil {
		return nil, err
	}
	if err = os.Rename(tmp, final); err != nil {
		return nil, err
	}
	if err = syncDir(dir); err != nil {
		return nil, err
	}
	ok = true
	return f, nil
}

func publishManifest(dir string, m Manifest) error {
	b, err := marshalManifest(m)
	if err != nil {
		return err
	}
	tmp := filepath.Join(dir, ".MANIFEST.tmp")
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o640)
	if err != nil {
		return err
	}
	if n, writeErr := f.Write(b); writeErr != nil {
		err = writeErr
	} else if n != len(b) {
		err = io.ErrShortWrite
	} else {
		err = f.Sync()
	}
	err = errors.Join(err, f.Close())
	if err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err = os.Rename(tmp, filepath.Join(dir, ManifestName)); err != nil {
		return err
	}
	return syncDir(dir)
}

func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	return errors.Join(f.Sync(), f.Close())
}
func hashPrefix(f *os.File, n uint64) ([32]byte, error) {
	h := sha256.New()
	if _, err := io.CopyN(h, io.NewSectionReader(f, 0, int64(n)), int64(n)); err != nil {
		return [32]byte{}, err
	}
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out, nil
}
