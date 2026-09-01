package seglog

import (
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sort"
)

type Location struct {
	SegmentID, Offset, Bytes, Index, Term uint64
}

type GroupIndex struct {
	TruncateIndex, TruncateTerm uint64
	Entries                     []Location
}

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
	dir          string
	active       *os.File
	activeOffset uint64
	manifest     Manifest
	index        map[uint64]*GroupIndex
	last         map[uint64]uint64
	lastTerm     map[uint64]uint64
	records      uint64
	buf          []byte
}

func activeName(id uint64) string { return fmt.Sprintf("%020d.active", id) }
func sealedName(id uint64) string { return fmt.Sprintf("%020d.seg", id) }

func Create(dir string) (*Log, error) {
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
	return &Log{dir: dir, active: f, activeOffset: segmentHeaderBytes, manifest: m, index: make(map[uint64]*GroupIndex), last: make(map[uint64]uint64), lastTerm: make(map[uint64]uint64), buf: make([]byte, 0, 4096)}, nil
}

func Open(dir string) (*Log, error) {
	b, err := os.ReadFile(filepath.Join(dir, ManifestName))
	if err != nil {
		return nil, err
	}
	m, err := unmarshalManifest(b)
	if err != nil {
		return nil, err
	}
	l := &Log{dir: dir, manifest: m, index: make(map[uint64]*GroupIndex), last: make(map[uint64]uint64), lastTerm: make(map[uint64]uint64), buf: make([]byte, 0, 4096)}
	if err = l.reconcileRotation(); err != nil {
		return nil, err
	}
	if err = l.rebuild(); err != nil {
		if l.active != nil {
			_ = l.active.Close()
		}
		return nil, err
	}
	return l, nil
}

func (l *Log) Close() error {
	if l.active == nil {
		return nil
	}
	err := l.active.Close()
	l.active = nil
	return err
}
func (l *Log) Manifest() Manifest {
	m := l.manifest
	m.Segments = slices.Clone(m.Segments)
	m.Groups = slices.Clone(m.Groups)
	return m
}
func (l *Log) Group(group uint64) (GroupIndex, bool) {
	g, ok := l.index[group]
	if !ok {
		return GroupIndex{}, false
	}
	return GroupIndex{TruncateIndex: g.TruncateIndex, TruncateTerm: g.TruncateTerm, Entries: slices.Clone(g.Entries)}, true
}

func (l *Log) Append(r Record) (Location, error) {
	if l.active == nil {
		return Location{}, os.ErrClosed
	}
	g := l.index[r.GroupID]
	if r.Index <= l.last[r.GroupID] || g != nil && r.Index <= g.TruncateIndex {
		return Location{}, fmt.Errorf("%w: group index regression", ErrCorrupt)
	}
	encoded, err := marshalRecord(r, l.buf)
	if err != nil {
		return Location{}, err
	}
	l.buf = encoded
	off := l.activeOffset
	n, err := l.active.WriteAt(encoded, int64(off))
	if err != nil {
		return Location{}, err
	}
	if n != len(encoded) {
		return Location{}, io.ErrShortWrite
	}
	l.activeOffset += uint64(n)
	loc := Location{SegmentID: l.manifest.ActiveID, Offset: off, Bytes: uint64(len(encoded)), Index: r.Index, Term: r.Term}
	if g == nil {
		g = &GroupIndex{}
		l.index[r.GroupID] = g
	}
	g.Entries = append(g.Entries, loc)
	l.last[r.GroupID] = r.Index
	l.lastTerm[r.GroupID] = r.Term
	l.records++
	return loc, nil
}

// Sync first makes active bytes durable, then atomically publishes the exact
// recoverable offset. Bytes beyond that offset are never interpreted on Open.
func (l *Log) Sync() error {
	if l.active == nil {
		return os.ErrClosed
	}
	if err := l.active.Sync(); err != nil {
		return err
	}
	next := l.manifest
	next.Generation++
	next.DurableOffset = l.activeOffset
	next.Groups = l.groupMetadata(l.activeOffset)
	if err := publishManifest(l.dir, next); err != nil {
		return err
	}
	l.manifest = next
	return nil
}

// SetTruncate records a durable, logical prefix boundary. Existing locations
// at or below index disappear from the in-memory index; segment bytes remain
// immutable until a future background reclaimer deletes whole segments.
func (l *Log) SetTruncate(group, index, term uint64) error {
	if group == 0 || index == 0 {
		return ErrBounds
	}
	g := l.index[group]
	if g == nil {
		g = &GroupIndex{}
		l.index[group] = g
	}
	if index < g.TruncateIndex || index == g.TruncateIndex && term != g.TruncateTerm {
		return fmt.Errorf("%w: truncation regression", ErrCorrupt)
	}
	if index > g.TruncateIndex {
		at := sort.Search(len(g.Entries), func(i int) bool { return g.Entries[i].Index >= index })
		if at == len(g.Entries) || g.Entries[at].Index != index || g.Entries[at].Term != term || (g.Entries[at].SegmentID == l.manifest.ActiveID && g.Entries[at].Offset+g.Entries[at].Bytes > l.manifest.DurableOffset) {
			return fmt.Errorf("%w: truncation boundary is not durable", ErrCorrupt)
		}
	}
	cut := sort.Search(len(g.Entries), func(i int) bool { return g.Entries[i].Index > index })
	nextGroups := l.groupMetadata(l.manifest.DurableOffset)
	found := false
	for i := range nextGroups {
		if nextGroups[i].GroupID == group {
			nextGroups[i].TruncateIndex, nextGroups[i].TruncateTerm, found = index, term, true
			break
		}
	}
	if !found {
		nextGroups = append(nextGroups, GroupMeta{GroupID: group, TruncateIndex: index, TruncateTerm: term, DurableLastIndex: l.last[group], DurableLastTerm: l.lastTerm[group]})
		sort.Slice(nextGroups, func(i, j int) bool { return nextGroups[i].GroupID < nextGroups[j].GroupID })
	}
	next := l.manifest
	next.Generation++
	next.Groups = nextGroups
	if err := publishManifest(l.dir, next); err != nil {
		return err
	}
	g.TruncateIndex, g.TruncateTerm = index, term
	g.Entries = slices.Delete(g.Entries, 0, cut)
	l.manifest = next
	return nil
}

func (l *Log) groupMetadata(durableOffset uint64) []GroupMeta {
	groups := make([]GroupMeta, 0, len(l.index))
	for id, g := range l.index {
		lastIndex, lastTerm := uint64(0), uint64(0)
		for i := len(g.Entries) - 1; i >= 0; i-- {
			loc := g.Entries[i]
			if loc.SegmentID < l.manifest.ActiveID || loc.Offset+loc.Bytes <= durableOffset {
				lastIndex, lastTerm = loc.Index, loc.Term
				break
			}
		}
		if lastIndex == 0 && g.TruncateIndex != 0 {
			lastIndex, lastTerm = g.TruncateIndex, g.TruncateTerm
		}
		if lastIndex != 0 {
			groups = append(groups, GroupMeta{GroupID: id, TruncateIndex: g.TruncateIndex, TruncateTerm: g.TruncateTerm, DurableLastIndex: lastIndex, DurableLastTerm: lastTerm})
		}
	}
	sort.Slice(groups, func(i, j int) bool { return groups[i].GroupID < groups[j].GroupID })
	return groups
}

// Rotate performs no old-log replay: it hashes the current file once while
// sealing, publishes immutable names with renames, and swaps a small manifest.
func (l *Log) Rotate(hook func(RotationPhase) error) error {
	if err := l.Sync(); err != nil {
		return err
	}
	dataBytes := l.manifest.DurableOffset
	sum, err := hashPrefix(l.active, dataBytes)
	if err != nil {
		return err
	}
	previous := [32]byte{}
	if n := len(l.manifest.Segments); n != 0 {
		previous = l.manifest.Segments[n-1].Hash
	}
	ftr := segmentFooter{ID: l.manifest.ActiveID, Generation: l.manifest.ActiveGeneration, Records: l.records, DataBytes: dataBytes, PreviousHash: previous, Hash: sum}
	if _, err = l.active.WriteAt(marshalSegmentFooter(ftr), int64(dataBytes)); err != nil {
		return err
	}
	if err = l.active.Sync(); err != nil {
		return err
	}
	if hook != nil {
		if err = hook(RotationSealedSynced); err != nil {
			return err
		}
	}
	if err = l.active.Close(); err != nil {
		return err
	}
	l.active = nil
	if err = os.Rename(filepath.Join(l.dir, activeName(ftr.ID)), filepath.Join(l.dir, sealedName(ftr.ID))); err != nil {
		return err
	}
	if err = syncDir(l.dir); err != nil {
		return err
	}
	if hook != nil {
		if err = hook(RotationSealedRenamed); err != nil {
			return err
		}
	}
	nextID := ftr.ID + 1
	nextGeneration := ftr.Generation + 1
	nextFile, err := createSegment(l.dir, segmentHeader{ID: nextID, Generation: nextGeneration, PreviousID: ftr.ID, PreviousHash: sum, LogID: l.manifest.LogID})
	if err != nil {
		return err
	}
	l.active = nextFile
	l.activeOffset = segmentHeaderBytes
	if hook != nil {
		if err = hook(RotationNextPublished); err != nil {
			return err
		}
	}
	next := l.manifest
	next.Generation++
	next.ActiveID = nextID
	next.ActiveGeneration = nextGeneration
	next.DurableSegmentID = nextID
	next.DurableOffset = segmentHeaderBytes
	next.Segments = append(slices.Clone(next.Segments), SegmentMeta{ID: ftr.ID, Generation: ftr.Generation, Bytes: dataBytes + segmentFooterBytes, Records: ftr.Records, PreviousHash: previous, Hash: sum})
	if err = publishManifest(l.dir, next); err != nil {
		return err
	}
	l.manifest = next
	l.records = 0
	if hook != nil {
		if err = hook(RotationManifestPublished); err != nil {
			return err
		}
	}
	return nil
}

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
	if _, err = f.Write(marshalSegmentHeader(h)); err != nil {
		return nil, err
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
	tmp := filepath.Join(dir, ".MANIFEST.v3.tmp")
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o640)
	if err != nil {
		return err
	}
	if _, err = f.Write(b); err == nil {
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
