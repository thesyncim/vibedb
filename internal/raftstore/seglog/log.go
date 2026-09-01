package seglog

import (
	"crypto/sha256"
	"errors"
	"hash"
	"io"
	"os"
	"path/filepath"
)

type RotationPhase uint8

const (
	RotationSealedSynced RotationPhase = iota + 1
	RotationSealFileClosed
	RotationNextPublished
	RotationPendingMetadataPublished
	RotationSealedMetadataPublished
)

// Log owns one directory. Append writes only the active segment; Sync is the
// publication boundary that makes its current offset recoverable.
type Log struct {
	dir           string
	active        *os.File
	activeOffset  uint64
	activeHash    hash.Hash
	digestScratch [32]byte
	state         metadataState
	metadata      *metadataStore
	reserveFiles  [2]*os.File
	records       uint64
	events        []segmentEvent
	eventSpare    []segmentEvent
	poisoned      error
	publishHook   func(metadataState) error
	authKey       [32]byte
}

func segmentPath(dir string, id fileID) string { return filepath.Join(dir, segmentFileName(id)) }

func createLog(dir string, logID [16]byte, authKey [32]byte, segmentCapacity uint64) (*Log, error) {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, err
	}
	if _, err := os.Stat(filepath.Join(dir, metadataName)); err == nil {
		return nil, os.ErrExist
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	f, activeReserve, err := prepareReserve(dir, segmentCapacity)
	if err != nil {
		return nil, err
	}
	h := segmentHeader{ID: 1, Generation: 1, LogID: logID, FileID: activeReserve.FileID, Capacity: activeReserve.Capacity}
	if err = activateReserve(f, h); err != nil {
		_ = f.Close()
		return nil, err
	}
	var reserves [2]reserveDescriptor
	var reserveFiles [2]*os.File
	for i := range reserves {
		reserveFiles[i], reserves[i], err = prepareReserve(dir, activeReserve.Capacity)
		if err != nil {
			_ = f.Close()
			for j := 0; j < i; j++ {
				_ = reserveFiles[j].Close()
			}
			return nil, err
		}
	}
	slot := metadataSlot{Generation: 1, LogID: logID, CatalogTail: metadataCatalogStart, Active: activeDescriptor{FileID: activeReserve.FileID, ID: 1, Generation: 1, Capacity: activeReserve.Capacity}, Reserves: reserves}
	metadata, err := createMetadataStore(dir, slot, authKey)
	if err != nil {
		_ = f.Close()
		for i := range reserveFiles {
			if reserveFiles[i] != nil {
				_ = reserveFiles[i].Close()
			}
		}
		return nil, err
	}
	m := stateFromMetadata(slot, nil)
	activeHash := sha256.New()
	_, _ = activeHash.Write(marshalSegmentHeader(h))
	return &Log{dir: dir, active: f, activeOffset: segmentHeaderBytes, activeHash: activeHash, state: m, metadata: metadata, reserveFiles: reserveFiles, authKey: authKey}, nil
}

func (l *Log) Close() error {
	if l.active == nil {
		return nil
	}
	err := l.active.Close()
	for i := range l.reserveFiles {
		if l.reserveFiles[i] != nil {
			err = errors.Join(err, l.reserveFiles[i].Close())
			l.reserveFiles[i] = nil
		}
	}
	err = errors.Join(err, l.metadata.Close())
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
func (l *Log) publish(m metadataState) error {
	if l.publishHook != nil {
		return l.publishHook(m)
	}
	if l.metadata == nil {
		return ErrCorrupt
	}
	slot := metadataSlotFromState(l.metadata.slot, m)
	var record *catalogRecord
	if len(m.Segments) != 0 {
		last := m.Segments[len(m.Segments)-1]
		oldSealed := len(l.state.Segments)
		if oldSealed != 0 && pendingSegment(l.state.Segments[oldSealed-1]) {
			oldSealed--
		}
		if last.State == SegmentSealed && len(m.Segments) > oldSealed {
			copyMeta := last
			copyMeta.PreviousHash = [32]byte{}
			copyMeta.FileID = fileID{}
			record = &catalogRecord{Kind: catalogSeal, Segment: copyMeta, FileID: last.FileID}
		}
	}
	return l.metadata.publish(slot, record)
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
	l.state.DurableOffset = l.activeOffset
	return nil
}

// replenishReserve runs only on the serial background sealer. The file is
// physically allocated and durable before its ownership appears in a slot.
func activateReserve(file *os.File, header segmentHeader) error {
	if stat, err := file.Stat(); err != nil || stat.Size() != 0 {
		return errors.Join(ErrCorrupt, err)
	}
	bytes := marshalSegmentHeader(header)
	if err := writeFullAt(file, bytes, 0); err != nil {
		return err
	}
	return file.Sync()
}

// SetTruncate records a durable, logical prefix boundary. Existing locations
// at or below index disappear from the in-memory index; segment bytes remain
// immutable until a future background reclaimer deletes whole segments.
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
