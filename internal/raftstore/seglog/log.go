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
	dir           string
	active        *os.File
	activeOffset  uint64
	activeHash    hash.Hash
	digestScratch [32]byte
	manifest      Manifest
	index         map[uint64]*GroupIndex
	last          map[uint64]uint64
	lastTerm      map[uint64]uint64
	records       uint64
	buf           []byte
	events        []segmentEvent
	poisoned      error
	publishHook   func(Manifest) error
	authKey       [32]byte
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
	activeHash := sha256.New()
	_, _ = activeHash.Write(marshalSegmentHeader(h))
	return &Log{dir: dir, active: f, activeOffset: segmentHeaderBytes, activeHash: activeHash, manifest: m, index: make(map[uint64]*GroupIndex), last: make(map[uint64]uint64), lastTerm: make(map[uint64]uint64), buf: make([]byte, 0, 4096)}, nil
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

// ReserveGroup moves index allocation to a control-plane boundary. Append
// never grows the slice: the production adapter must size this from its
// configured segment/index admission bound and rotate or reserve again before
// exhaustion.
func (l *Log) ReserveGroup(group uint64, capacity int) error {
	if err := l.usable(); err != nil {
		return err
	}
	if group == 0 || capacity < 0 {
		return ErrBounds
	}
	g := l.index[group]
	if g == nil {
		l.index[group] = &GroupIndex{Entries: make([]Location, 0, capacity)}
		return nil
	}
	if capacity < len(g.Entries) {
		return ErrBounds
	}
	if capacity <= cap(g.Entries) {
		return nil
	}
	entries := make([]Location, len(g.Entries), capacity)
	copy(entries, g.Entries)
	g.Entries = entries
	return nil
}

func (l *Log) ReserveEvents(capacity int) error {
	if err := l.usable(); err != nil {
		return err
	}
	if capacity < len(l.events) {
		return ErrBounds
	}
	if capacity <= cap(l.events) {
		return nil
	}
	events := make([]segmentEvent, len(l.events), capacity)
	copy(events, l.events)
	l.events = events
	return nil
}

func (l *Log) Append(r Record) (Location, error) {
	if err := l.usable(); err != nil {
		return Location{}, err
	}
	if r.Kind != RecordEntry {
		return Location{}, fmt.Errorf("%w: Append requires entry record", ErrBounds)
	}
	g := l.index[r.GroupID]
	if g != nil && (r.Index <= l.last[r.GroupID] || r.Index <= g.TruncateIndex) {
		return Location{}, fmt.Errorf("%w: group index regression", ErrCorrupt)
	}
	if g == nil || len(g.Entries) == cap(g.Entries) {
		return Location{}, fmt.Errorf("%w: group index reservation", ErrBounds)
	}
	if len(l.events) == cap(l.events) {
		return Location{}, fmt.Errorf("%w: segment event reservation", ErrBounds)
	}
	encoded, err := marshalRecord(r, l.buf)
	if err != nil {
		return Location{}, err
	}
	l.buf = encoded
	off := l.activeOffset
	n, err := l.active.WriteAt(encoded, int64(off))
	if err != nil {
		return Location{}, l.poison(err)
	}
	if n != len(encoded) {
		return Location{}, l.poison(io.ErrShortWrite)
	}
	if _, err = l.activeHash.Write(encoded); err != nil {
		return Location{}, l.poison(err)
	}
	l.activeOffset += uint64(n)
	loc := Location{SegmentID: l.manifest.ActiveID, Offset: off, Bytes: uint64(len(encoded)), Index: r.Index, Term: r.Term}
	g.Entries = append(g.Entries, loc)
	l.last[r.GroupID] = r.Index
	l.lastTerm[r.GroupID] = r.Term
	l.events = append(l.events, segmentEvent{Kind: RecordEntry, GroupID: r.GroupID, Index: r.Index, Term: r.Term, Offset: off, Bytes: uint64(n)})
	l.records++
	return loc, nil
}

// TruncateSuffix appends an explicit logical record before changing the live
// index. Recovery permits a group-index regression only through this record,
// so a torn or missing marker can never make replacement entries appear.
// The marker's term binds the surviving predecessor (zero when from is one).
func (l *Log) TruncateSuffix(group, from uint64) error {
	if err := l.usable(); err != nil {
		return err
	}
	g := l.index[group]
	if group == 0 || from == 0 || g == nil || from <= g.TruncateIndex {
		return ErrBounds
	}
	cut := sort.Search(len(g.Entries), func(i int) bool { return g.Entries[i].Index >= from })
	if cut == len(g.Entries) && from != l.last[group]+1 {
		return fmt.Errorf("%w: suffix truncation gap", ErrCorrupt)
	}
	if len(l.events) == cap(l.events) {
		return fmt.Errorf("%w: segment event reservation", ErrBounds)
	}
	term := g.TruncateTerm
	if cut != 0 {
		term = g.Entries[cut-1].Term
	}
	encoded, err := marshalRecord(Record{GroupID: group, Index: from, Term: term, Kind: RecordTruncateSuffix}, l.buf)
	if err != nil {
		return err
	}
	l.buf = encoded
	n, err := l.active.WriteAt(encoded, int64(l.activeOffset))
	if err != nil {
		return l.poison(err)
	}
	if n != len(encoded) {
		return l.poison(io.ErrShortWrite)
	}
	if _, err = l.activeHash.Write(encoded); err != nil {
		return l.poison(err)
	}
	l.activeOffset += uint64(n)
	l.records++
	g.Entries = slices.Delete(g.Entries, cut, len(g.Entries))
	l.last[group] = from - 1
	l.lastTerm[group] = term
	l.events = append(l.events, segmentEvent{Kind: RecordTruncateSuffix, GroupID: group, Index: from, Term: term})
	return nil
}

// Sync first makes active bytes durable, then atomically publishes the exact
// recoverable offset. Bytes beyond that offset are never interpreted on Open.
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
	next.Groups = l.groupMetadata(l.activeOffset)
	if err := l.publish(next); err != nil {
		return l.poison(err)
	}
	l.manifest = next
	return nil
}

// SetTruncate records a durable, logical prefix boundary. Existing locations
// at or below index disappear from the in-memory index; segment bytes remain
// immutable until a future background reclaimer deletes whole segments.
func (l *Log) SetTruncate(group, index, term uint64) error {
	if err := l.usable(); err != nil {
		return err
	}
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
	if err := l.publish(next); err != nil {
		return l.poison(err)
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
		if _, seen := l.last[id]; lastIndex != 0 || seen {
			groups = append(groups, GroupMeta{GroupID: id, TruncateIndex: g.TruncateIndex, TruncateTerm: g.TruncateTerm, DurableLastIndex: lastIndex, DurableLastTerm: lastTerm})
		}
	}
	sort.Slice(groups, func(i, j int) bool { return groups[i].GroupID < groups[j].GroupID })
	return groups
}

// Rotate performs no log read or replay: Append maintains the active SHA-256,
// so sealing snapshots fixed-size state before rename and manifest swap.
func (l *Log) Rotate(hook func(RotationPhase) error) error {
	if err := l.usable(); err != nil {
		return err
	}
	if err := l.Sync(); err != nil {
		return err
	}
	dataBytes := l.manifest.DurableOffset
	var err error
	digest := l.activeHash.Sum(l.digestScratch[:0])
	var sum [32]byte
	copy(sum[:], digest)
	previous := l.manifest.AnchorHash
	if n := len(l.manifest.Segments); n != 0 {
		previous = l.manifest.Segments[n-1].Hash
	}
	indexBytes, encodeErr := marshalSegmentIndex(l.events, dataBytes)
	if encodeErr != nil {
		return l.poison(encodeErr)
	}
	if n, writeErr := l.active.WriteAt(indexBytes, int64(dataBytes)); writeErr != nil {
		return l.poison(writeErr)
	} else if n != len(indexBytes) {
		return l.poison(io.ErrShortWrite)
	}
	ftr := segmentFooter{ID: l.manifest.ActiveID, Generation: l.manifest.ActiveGeneration, Records: l.records, DataBytes: dataBytes, Hash: sum, IndexOffset: dataBytes, IndexBytes: uint64(len(indexBytes)), Events: uint64(len(l.events))}
	header := segmentHeader{ID: l.manifest.ActiveID, Generation: l.manifest.ActiveGeneration, PreviousID: l.expectedPreviousID(), PreviousHash: previous, LogID: l.manifest.LogID}
	ftr.Auth = segmentMetadataMAC(l.authKey, header, indexBytes, ftr)
	footerBytes := marshalSegmentFooter(ftr)
	if n, writeErr := l.active.WriteAt(footerBytes, int64(dataBytes)+int64(len(indexBytes))); writeErr != nil {
		return l.poison(writeErr)
	} else if n != len(footerBytes) {
		return l.poison(io.ErrShortWrite)
	}
	if err = l.active.Sync(); err != nil {
		return l.poison(err)
	}
	if hook != nil {
		if err = hook(RotationSealedSynced); err != nil {
			return l.poison(err)
		}
	}
	if err = l.active.Close(); err != nil {
		return l.poison(err)
	}
	l.active = nil
	if err = os.Rename(filepath.Join(l.dir, activeName(ftr.ID)), filepath.Join(l.dir, sealedName(ftr.ID))); err != nil {
		return l.poison(err)
	}
	if err = syncDir(l.dir); err != nil {
		return l.poison(err)
	}
	if hook != nil {
		if err = hook(RotationSealedRenamed); err != nil {
			return l.poison(err)
		}
	}
	nextID := ftr.ID + 1
	nextGeneration := ftr.Generation + 1
	nextFile, err := createSegment(l.dir, segmentHeader{ID: nextID, Generation: nextGeneration, PreviousID: ftr.ID, PreviousHash: sum, LogID: l.manifest.LogID})
	if err != nil {
		return l.poison(err)
	}
	l.active = nextFile
	l.activeOffset = segmentHeaderBytes
	l.activeHash = sha256.New()
	_, _ = l.activeHash.Write(marshalSegmentHeader(segmentHeader{ID: nextID, Generation: nextGeneration, PreviousID: ftr.ID, PreviousHash: sum, LogID: l.manifest.LogID}))
	l.events = l.events[:0]
	if hook != nil {
		if err = hook(RotationNextPublished); err != nil {
			return l.poison(err)
		}
	}
	next := l.manifest
	next.Generation++
	next.ActiveID = nextID
	next.ActiveGeneration = nextGeneration
	next.DurableSegmentID = nextID
	next.DurableOffset = segmentHeaderBytes
	next.Segments = append(slices.Clone(next.Segments), SegmentMeta{ID: ftr.ID, Generation: ftr.Generation, Bytes: dataBytes + uint64(len(indexBytes)) + segmentFooterBytes, Records: ftr.Records, IndexOffset: dataBytes, IndexBytes: uint64(len(indexBytes)), PreviousHash: previous, Hash: sum})
	if err = l.publish(next); err != nil {
		return l.poison(err)
	}
	l.manifest = next
	l.records = 0
	if hook != nil {
		if err = hook(RotationManifestPublished); err != nil {
			return l.poison(err)
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
	tmp := filepath.Join(dir, ".MANIFEST.v3.tmp")
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
