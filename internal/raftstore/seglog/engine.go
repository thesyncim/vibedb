package seglog

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sort"
)

var (
	ErrWaveConflict = errors.New("seglog: wave ID reused with different payload")
	ErrRaftState    = errors.New("seglog: illegal Raft durable state")
)

const engineWaveGroup = ^uint64(0)

type WaveID [16]byte

type Entry struct {
	Index, Term uint64
	Data        []byte
	dataOffset  uint64
}
type HardState struct{ Term, Vote, Commit uint64 }
type Checkpoint struct {
	ID          [16]byte
	Index, Term uint64
}
type ReadyBatch struct {
	GroupID                     uint64
	ReplaceFrom                 uint64
	Entries                     []Entry
	Hard                        *HardState
	TruncateIndex, TruncateTerm uint64
	Checkpoint                  *Checkpoint
}
type Wave struct {
	ID      WaveID
	Batches []ReadyBatch
}

type EntryLocation struct{ SegmentID, Offset, Bytes, Index, Term uint64 }
type GroupState struct {
	Hard                        HardState
	TruncateIndex, TruncateTerm uint64
	Checkpoint                  Checkpoint
	Entries                     []EntryLocation
}

type engineGroup struct{ GroupState }

type Engine struct {
	log          *Log
	groups       map[uint64]*engineGroup
	waves        map[WaveID][32]byte
	waveLimit    int
	sequence     uint64
	frameBuf     []byte
	eventScratch []segmentEvent
	syncData     func(*os.File) error
	writeAt      func(*os.File, []byte, int64) (int, error)
}

func CreateEngine(dir string) (*Engine, error) {
	l, err := Create(dir)
	if err != nil {
		return nil, err
	}
	return &Engine{log: l, groups: make(map[uint64]*engineGroup), waves: make(map[WaveID][32]byte), syncData: syncActiveData, writeAt: func(f *os.File, b []byte, off int64) (int, error) { return f.WriteAt(b, off) }}, nil
}

func (e *Engine) Close() error { return e.log.Close() }

// Reserve moves every unbounded allocation off the steady PersistWave path.
func (e *Engine) Reserve(frameBytes, events, waveIDs int) error {
	if err := e.log.usable(); err != nil {
		return err
	}
	if frameBytes < 72 || events < len(e.eventScratch) || waveIDs < len(e.waves) {
		return ErrBounds
	}
	if frameBytes > cap(e.frameBuf) {
		e.frameBuf = make([]byte, 0, frameBytes)
	}
	if events > cap(e.eventScratch) {
		e.eventScratch = make([]segmentEvent, 0, events)
	}
	if events > cap(e.log.events) {
		if err := e.log.ReserveEvents(events); err != nil {
			return err
		}
	}
	if waveIDs > len(e.waves) {
		replacement := make(map[WaveID][32]byte, waveIDs)
		for id, digest := range e.waves {
			replacement[id] = digest
		}
		e.waves = replacement
	}
	e.waveLimit = waveIDs
	return nil
}

func (e *Engine) ReserveGroup(group uint64, entries int) error {
	if err := e.log.usable(); err != nil {
		return err
	}
	if group == 0 || group == engineWaveGroup || entries < 0 {
		return ErrBounds
	}
	g := e.groups[group]
	if g == nil {
		e.groups[group] = &engineGroup{GroupState: GroupState{Entries: make([]EntryLocation, 0, entries)}}
		return nil
	}
	if entries < len(g.Entries) {
		return ErrBounds
	}
	if entries > cap(g.Entries) {
		grown := make([]EntryLocation, len(g.Entries), entries)
		copy(grown, g.Entries)
		g.Entries = grown
	}
	return nil
}

func (e *Engine) Group(group uint64) (GroupState, bool) {
	if e == nil || e.log.usable() != nil {
		return GroupState{}, false
	}
	g, ok := e.groups[group]
	if !ok {
		return GroupState{}, false
	}
	result := g.GroupState
	result.Entries = slices.Clone(result.Entries)
	return result, true
}
func (e *Engine) Sequence() uint64 {
	if e == nil || e.log.usable() != nil {
		return 0
	}
	return e.sequence
}

// PersistWave makes a caller-sorted, multi-group Ready wave durable with one
// append and one data-sync. The frame is the acknowledgement boundary: its
// checksum and canonical payload digest cover every batch, so recovery applies
// all batches or none. A sync error poisons this handle because the complete
// frame may nevertheless be durable; OpenEngine recovers it, after which an
// exact WaveID+payload retry is a no-op. Reusing the ID for other bytes is fatal.
//
// Callers must reserve buffers, group entry capacity, and WaveID map capacity
// before entering the steady path. They must also supply collision-resistant
// nonzero WaveIDs and strictly increasing GroupIDs within each wave. Success is
// the fence before dependent messages or Ready completion may be published.
func (e *Engine) PersistWave(w Wave) error {
	if err := e.log.usable(); err != nil {
		return err
	}
	if w.ID == (WaveID{}) || len(w.Batches) == 0 {
		return ErrBounds
	}
	if previous, ok := e.waves[w.ID]; ok {
		_, digest, _, err := e.prepareWave(w, false)
		if err != nil {
			return e.log.poison(ErrWaveConflict)
		}
		if previous == digest {
			return nil
		}
		return e.log.poison(ErrWaveConflict)
	}
	if len(e.waves) >= e.waveLimit {
		return ErrBounds
	}
	frame, _, events, err := e.prepareWave(w, true)
	if err != nil {
		return err
	}
	if len(e.log.events)+len(events) > cap(e.log.events) {
		return ErrBounds
	}
	offset := e.log.activeOffset
	sequence := e.sequence + 1
	sealWaveHeader(frame, sequence, w.ID)
	n, err := e.writeAt(e.log.active, frame, int64(offset))
	if err != nil {
		return e.log.poison(err)
	}
	if n != len(frame) {
		return e.log.poison(io.ErrShortWrite)
	}
	if _, err = e.log.activeHash.Write(frame); err != nil {
		return e.log.poison(err)
	}
	if err = e.syncData(e.log.active); err != nil {
		return e.log.poison(err)
	}
	e.log.activeOffset += uint64(n)
	e.log.records++
	for i := range events {
		if events[i].Kind == eventWaveEntry {
			events[i].Offset += offset
		}
	}
	e.log.events = append(e.log.events, events...)
	for _, event := range events {
		if err = e.applyEvent(event, e.log.manifest.ActiveID); err != nil {
			return e.log.poison(err)
		}
	}
	return nil
}

func (e *Engine) prepareWave(w Wave, validateState bool) ([]byte, [32]byte, []segmentEvent, error) {
	e.frameBuf = e.frameBuf[:0]
	encodedBytes, eventCount, err := waveSize(w)
	if err != nil {
		return nil, [32]byte{}, nil, err
	}
	if encodedBytes > cap(e.frameBuf) || eventCount > cap(e.eventScratch) {
		return nil, [32]byte{}, nil, ErrBounds
	}
	e.frameBuf = e.frameBuf[:72]
	clear(e.frameBuf)
	e.eventScratch = e.eventScratch[:0]
	previousGroup := uint64(0)
	e.frameBuf = appendUvarint(e.frameBuf, uint64(len(w.Batches)))
	for batchIndex := range w.Batches {
		batch := &w.Batches[batchIndex]
		if batch.GroupID == 0 || batch.GroupID == engineWaveGroup || batch.GroupID <= previousGroup {
			return nil, [32]byte{}, nil, ErrRaftState
		}
		group := e.groups[batch.GroupID]
		if validateState && group == nil {
			return nil, [32]byte{}, nil, ErrBounds
		}
		flags := batchFlags(batch)
		if validateState {
			var err error
			flags, err = validateBatch(group, batch)
			if err != nil {
				return nil, [32]byte{}, nil, err
			}
		}
		needed := len(batch.Entries)
		if batch.ReplaceFrom != 0 {
			needed++
		}
		if batch.TruncateIndex != 0 {
			needed++
		}
		if batch.Checkpoint != nil {
			needed++
		}
		if batch.Hard != nil {
			needed++
		}
		if len(e.eventScratch)+needed+1 > cap(e.eventScratch) || validateState && len(group.Entries)-replacementCount(group.Entries, batch.ReplaceFrom)+len(batch.Entries) > cap(group.Entries) {
			return nil, [32]byte{}, nil, ErrBounds
		}
		e.frameBuf = appendUvarint(e.frameBuf, batch.GroupID-previousGroup)
		e.frameBuf = append(e.frameBuf, flags)
		if batch.ReplaceFrom != 0 {
			e.frameBuf = appendUvarint(e.frameBuf, batch.ReplaceFrom)
			if validateState {
				e.eventScratch = append(e.eventScratch, segmentEvent{Kind: RecordTruncateSuffix, GroupID: batch.GroupID, Index: batch.ReplaceFrom, Term: predecessorTerm(group, batch.ReplaceFrom)})
			}
		}
		if batch.TruncateIndex != 0 {
			e.frameBuf = appendUvarint(e.frameBuf, batch.TruncateIndex)
			e.frameBuf = appendUvarint(e.frameBuf, batch.TruncateTerm)
		}
		if batch.Checkpoint != nil {
			e.frameBuf = append(e.frameBuf, batch.Checkpoint.ID[:]...)
			e.frameBuf = appendUvarint(e.frameBuf, batch.Checkpoint.Index)
			e.frameBuf = appendUvarint(e.frameBuf, batch.Checkpoint.Term)
		}
		if batch.Hard != nil {
			e.frameBuf = appendUvarint(e.frameBuf, batch.Hard.Term)
			e.frameBuf = appendUvarint(e.frameBuf, batch.Hard.Vote)
			e.frameBuf = appendUvarint(e.frameBuf, batch.Hard.Commit)
		}
		e.frameBuf = appendUvarint(e.frameBuf, uint64(len(batch.Entries)))
		for _, entry := range batch.Entries {
			e.frameBuf = appendUvarint(e.frameBuf, entry.Index)
			e.frameBuf = appendUvarint(e.frameBuf, entry.Term)
			e.frameBuf = appendUvarint(e.frameBuf, uint64(len(entry.Data)))
			dataOffset := uint64(len(e.frameBuf))
			e.frameBuf = append(e.frameBuf, entry.Data...)
			if len(e.frameBuf) > cap(e.frameBuf) {
				return nil, [32]byte{}, nil, ErrBounds
			}
			if validateState {
				e.eventScratch = append(e.eventScratch, segmentEvent{Kind: eventWaveEntry, GroupID: batch.GroupID, Index: entry.Index, Term: entry.Term, Offset: dataOffset, Bytes: uint64(len(entry.Data))})
			}
		}
		if validateState && batch.TruncateIndex != 0 {
			e.eventScratch = append(e.eventScratch, segmentEvent{Kind: eventPrefix, GroupID: batch.GroupID, Index: batch.TruncateIndex, Term: batch.TruncateTerm})
		}
		if validateState && batch.Checkpoint != nil {
			e.eventScratch = append(e.eventScratch, segmentEvent{Kind: eventCheckpoint, GroupID: batch.GroupID, Index: batch.Checkpoint.Index, Term: batch.Checkpoint.Term, Reference: batch.Checkpoint.ID})
		}
		if validateState && batch.Hard != nil {
			e.eventScratch = append(e.eventScratch, segmentEvent{Kind: eventHardState, GroupID: batch.GroupID, Term: batch.Hard.Term, Vote: batch.Hard.Vote, Commit: batch.Hard.Commit})
		}
		previousGroup = batch.GroupID
	}
	if len(e.frameBuf) > maxRecordBytes || len(e.frameBuf) > cap(e.frameBuf) {
		return nil, [32]byte{}, nil, ErrBounds
	}
	digest := sha256.Sum256(e.frameBuf[72:])
	copy(e.frameBuf[40:72], digest[:])
	if validateState {
		e.eventScratch = append(e.eventScratch, segmentEvent{Kind: eventWave, GroupID: engineWaveGroup, Index: e.sequence + 1, Reference: w.ID, Digest: digest})
	}
	return e.frameBuf, digest, e.eventScratch, nil
}

// waveSize is deliberately run before the encoder mutates its reusable slices.
// Insufficient reservations therefore fail without a hidden growth allocation.
func waveSize(w Wave) (int, int, error) {
	bytes, events := 72+uvarintBytes(uint64(len(w.Batches))), 1
	previousGroup := uint64(0)
	for i := range w.Batches {
		b := &w.Batches[i]
		if b.GroupID <= previousGroup {
			return 0, 0, ErrRaftState
		}
		bytes += uvarintBytes(b.GroupID-previousGroup) + 1 + uvarintBytes(uint64(len(b.Entries)))
		if b.ReplaceFrom != 0 {
			bytes += uvarintBytes(b.ReplaceFrom)
			events++
		}
		if b.TruncateIndex != 0 {
			bytes += uvarintBytes(b.TruncateIndex) + uvarintBytes(b.TruncateTerm)
			events++
		}
		if b.Checkpoint != nil {
			bytes += 16 + uvarintBytes(b.Checkpoint.Index) + uvarintBytes(b.Checkpoint.Term)
			events++
		}
		if b.Hard != nil {
			bytes += uvarintBytes(b.Hard.Term) + uvarintBytes(b.Hard.Vote) + uvarintBytes(b.Hard.Commit)
			events++
		}
		for j := range b.Entries {
			entry := &b.Entries[j]
			if len(entry.Data) > maxRecordBytes || bytes > maxRecordBytes-len(entry.Data) {
				return 0, 0, ErrBounds
			}
			bytes += uvarintBytes(entry.Index) + uvarintBytes(entry.Term) + uvarintBytes(uint64(len(entry.Data))) + len(entry.Data)
			events++
		}
		previousGroup = b.GroupID
	}
	if bytes > maxRecordBytes {
		return 0, 0, ErrBounds
	}
	return bytes, events, nil
}

func uvarintBytes(value uint64) int {
	bytes := 1
	for value >= 0x80 {
		value >>= 7
		bytes++
	}
	return bytes
}

const (
	batchReplace    = 1 << 0
	batchPrefix     = 1 << 1
	batchCheckpoint = 1 << 2
	batchHard       = 1 << 3
)

func batchFlags(b *ReadyBatch) byte {
	var flags byte
	if b.ReplaceFrom != 0 {
		flags |= batchReplace
	}
	if b.TruncateIndex != 0 {
		flags |= batchPrefix
	}
	if b.Checkpoint != nil {
		flags |= batchCheckpoint
	}
	if b.Hard != nil {
		flags |= batchHard
	}
	return flags
}

func validateBatch(g *engineGroup, b *ReadyBatch) (byte, error) {
	flags := batchFlags(b)
	last := durableLast(g)
	effectiveCommit := g.Hard.Commit
	if b.Hard != nil {
		effectiveCommit = b.Hard.Commit
	}
	if len(b.Entries) != 0 {
		first := b.Entries[0].Index
		if first <= last {
			if b.ReplaceFrom != first || b.ReplaceFrom <= g.Hard.Commit {
				return 0, ErrRaftState
			}
		} else if first != last+1 || b.ReplaceFrom != 0 {
			return 0, ErrRaftState
		}
		for i, entry := range b.Entries {
			if entry.Index != first+uint64(i) || entry.Term == 0 {
				return 0, ErrRaftState
			}
		}
		last = b.Entries[len(b.Entries)-1].Index
	} else if b.ReplaceFrom != 0 {
		return 0, ErrRaftState
	}
	if b.Checkpoint != nil {
		if b.Checkpoint.ID == ([16]byte{}) || b.Checkpoint.Index == 0 || b.Checkpoint.Term == 0 || b.Checkpoint.Index < g.Checkpoint.Index || b.Checkpoint.Index > effectiveCommit || (b.Checkpoint.Index <= last && termAt(g, b, b.Checkpoint.Index) != b.Checkpoint.Term) {
			return 0, ErrRaftState
		}
		if b.Checkpoint.Index > last {
			last = b.Checkpoint.Index
		}
	}
	if b.TruncateIndex != 0 {
		if b.TruncateTerm == 0 || b.TruncateIndex < g.TruncateIndex || b.TruncateIndex > last || b.TruncateIndex > effectiveCommit || (b.Checkpoint == nil || b.TruncateIndex != b.Checkpoint.Index) && termAt(g, b, b.TruncateIndex) != b.TruncateTerm {
			return 0, ErrRaftState
		}
	}
	if b.Hard != nil {
		h := b.Hard
		if h.Term < g.Hard.Term || h.Commit < g.Hard.Commit || h.Commit > last || h.Term == 0 && h.Vote != 0 {
			return 0, ErrRaftState
		}
		if h.Term == g.Hard.Term && g.Hard.Vote != 0 && h.Vote != g.Hard.Vote {
			return 0, ErrRaftState
		}
		if len(b.Entries) != 0 && h.Term < b.Entries[len(b.Entries)-1].Term {
			return 0, ErrRaftState
		}
	}
	return flags, nil
}

func durableLast(g *engineGroup) uint64 {
	if len(g.Entries) != 0 {
		return g.Entries[len(g.Entries)-1].Index
	}
	if g.Checkpoint.Index > g.TruncateIndex {
		return g.Checkpoint.Index
	}
	return g.TruncateIndex
}
func replacementCount(entries []EntryLocation, from uint64) int {
	if from == 0 {
		return 0
	}
	cut := sort.Search(len(entries), func(i int) bool { return entries[i].Index >= from })
	return len(entries) - cut
}
func predecessorTerm(g *engineGroup, from uint64) uint64 {
	if from <= 1 {
		return 0
	}
	for i := len(g.Entries) - 1; i >= 0; i-- {
		if g.Entries[i].Index == from-1 {
			return g.Entries[i].Term
		}
	}
	if g.Checkpoint.Index == from-1 {
		return g.Checkpoint.Term
	}
	if g.TruncateIndex == from-1 {
		return g.TruncateTerm
	}
	return 0
}
func termAt(g *engineGroup, b *ReadyBatch, index uint64) uint64 {
	if b != nil {
		for _, entry := range b.Entries {
			if entry.Index == index {
				return entry.Term
			}
		}
	}
	for i := len(g.Entries) - 1; i >= 0; i-- {
		if g.Entries[i].Index == index {
			return g.Entries[i].Term
		}
	}
	if g.Checkpoint.Index == index {
		return g.Checkpoint.Term
	}
	if g.TruncateIndex == index {
		return g.TruncateTerm
	}
	return 0
}

func sealWaveHeader(frame []byte, sequence uint64, id WaveID) {
	binary.LittleEndian.PutUint32(frame[0:4], uint32(len(frame)))
	binary.LittleEndian.PutUint16(frame[4:6], RecordWave)
	binary.LittleEndian.PutUint16(frame[6:8], FormatVersion)
	binary.LittleEndian.PutUint64(frame[8:16], sequence)
	copy(frame[16:32], id[:])
	binary.LittleEndian.PutUint32(frame[32:36], crc32.Checksum(frame[40:], crcTable))
	binary.LittleEndian.PutUint32(frame[36:40], crc32.Checksum(frame[:36], crcTable))
}

func (e *Engine) applyEvent(event segmentEvent, segmentID uint64) error {
	if event.Kind == eventWave {
		id := WaveID(event.Reference)
		if previous, ok := e.waves[id]; ok && previous != event.Digest {
			return ErrWaveConflict
		}
		e.waves[id] = event.Digest
		if event.Index != e.sequence+1 {
			return ErrCorrupt
		}
		e.sequence = event.Index
		return nil
	}
	g := e.groups[event.GroupID]
	if g == nil {
		g = &engineGroup{}
		e.groups[event.GroupID] = g
	}
	switch event.Kind {
	case RecordTruncateSuffix:
		if event.Index == 0 || event.Index <= g.Hard.Commit || event.Index > durableLast(g)+1 || predecessorTerm(g, event.Index) != event.Term {
			return ErrRaftState
		}
		cut := sort.Search(len(g.Entries), func(i int) bool { return g.Entries[i].Index >= event.Index })
		g.Entries = slices.Delete(g.Entries, cut, len(g.Entries))
	case eventWaveEntry:
		if event.Index != durableLast(g)+1 || event.Term == 0 {
			return ErrRaftState
		}
		g.Entries = append(g.Entries, EntryLocation{SegmentID: segmentID, Offset: event.Offset, Bytes: event.Bytes, Index: event.Index, Term: event.Term})
	case eventPrefix:
		if event.Index < g.TruncateIndex || event.Index > durableLast(g) || termAt(g, nil, event.Index) != event.Term {
			return ErrRaftState
		}
		cut := sort.Search(len(g.Entries), func(i int) bool { return g.Entries[i].Index > event.Index })
		g.Entries = slices.Delete(g.Entries, 0, cut)
		g.TruncateIndex, g.TruncateTerm = event.Index, event.Term
	case eventCheckpoint:
		if event.Reference == ([16]byte{}) || event.Index < g.Checkpoint.Index || event.Term == 0 {
			return ErrRaftState
		}
		cut := sort.Search(len(g.Entries), func(i int) bool { return g.Entries[i].Index > event.Index })
		g.Entries = slices.Delete(g.Entries, 0, cut)
		g.TruncateIndex, g.TruncateTerm = event.Index, event.Term
		g.Checkpoint = Checkpoint{ID: event.Reference, Index: event.Index, Term: event.Term}
	case eventHardState:
		if event.Term < g.Hard.Term || event.Commit < g.Hard.Commit || event.Commit > durableLast(g) || event.Term == g.Hard.Term && g.Hard.Vote != 0 && event.Vote != g.Hard.Vote {
			return ErrRaftState
		}
		g.Hard = HardState{Term: event.Term, Vote: event.Vote, Commit: event.Commit}
	default:
		return ErrCorrupt
	}
	return nil
}

func (e *Engine) Rotate(hook func(RotationPhase) error) error { return e.log.Rotate(hook) }

func OpenEngine(dir string) (*Engine, error) {
	return openEngine(dir, syncActiveData)
}

func openEngine(dir string, startupSync func(*os.File) error) (*Engine, error) {
	b, err := os.ReadFile(filepath.Join(dir, ManifestName))
	if err != nil {
		return nil, err
	}
	manifest, err := unmarshalManifest(b)
	if err != nil {
		return nil, err
	}
	l := &Log{dir: dir, manifest: manifest, index: make(map[uint64]*GroupIndex), last: make(map[uint64]uint64), lastTerm: make(map[uint64]uint64), buf: make([]byte, 0, 4096)}
	if err = l.reconcileRotation(); err != nil {
		return nil, err
	}
	e := &Engine{log: l, groups: make(map[uint64]*engineGroup), waves: make(map[WaveID][32]byte), syncData: startupSync, writeAt: func(f *os.File, b []byte, off int64) (int, error) { return f.WriteAt(b, off) }}
	if err = e.rebuild(); err != nil {
		_ = l.Close()
		return nil, err
	}
	e.waveLimit = len(e.waves)
	return e, nil
}

func (e *Engine) rebuild() error {
	previousID, previousHash := e.log.manifest.AnchorID, e.log.manifest.AnchorHash
	for _, want := range e.log.manifest.Segments {
		got, _, events, err := readSealedMetadata(filepath.Join(e.log.dir, sealedName(want.ID)), e.log.manifest.LogID, previousID, previousHash)
		if err != nil {
			return err
		}
		if got != want {
			return ErrCorrupt
		}
		for _, event := range events {
			if err = e.applyEvent(event, want.ID); err != nil {
				return err
			}
		}
		previousID, previousHash = want.ID, want.Hash
	}
	headerBytes := make([]byte, segmentHeaderBytes)
	if _, err := e.log.active.ReadAt(headerBytes, 0); err != nil {
		return err
	}
	header, err := unmarshalSegmentHeader(headerBytes)
	if err != nil {
		return err
	}
	if header.ID != e.log.manifest.ActiveID || header.Generation != e.log.manifest.ActiveGeneration || header.PreviousID != previousID || header.PreviousHash != previousHash || header.LogID != e.log.manifest.LogID {
		return ErrCorrupt
	}
	stat, err := e.log.active.Stat()
	if err != nil {
		return err
	}
	end := uint64(stat.Size())
	off := uint64(segmentHeaderBytes)
	frameHeader := make([]byte, recordHeaderBytes)
	for off < end {
		remaining := end - off
		if remaining < recordHeaderBytes {
			break
		}
		if _, err = e.log.active.ReadAt(frameHeader, int64(off)); err != nil {
			return err
		}
		total, headerErr := inspectWaveHeader(frameHeader)
		if headerErr != nil {
			return headerErr
		}
		if total > remaining {
			break
		}
		frame := make([]byte, total)
		copy(frame, frameHeader)
		if _, err = e.log.active.ReadAt(frame[recordHeaderBytes:], int64(off+recordHeaderBytes)); err != nil {
			return err
		}
		id, digest, batches, parseErr := decodeWaveFrame(frame, e.sequence+1)
		if parseErr != nil {
			return parseErr
		}
		events, applyErr := e.eventsForDecoded(batches, off, e.sequence+1, id, digest)
		if applyErr != nil {
			return applyErr
		}
		for _, event := range events {
			if applyErr = e.applyEvent(event, header.ID); applyErr != nil {
				return applyErr
			}
		}
		e.log.events = append(e.log.events, events...)
		e.log.records++
		off += total
	}
	if off != end {
		if err = e.log.active.Truncate(int64(off)); err != nil {
			return err
		}
	}
	// A process crash can leave a checksum-valid frame only in the page cache.
	// Before recovered state becomes observable, make the accepted prefix a
	// durable startup boundary (also persisting any torn-tail truncation).
	if err = e.syncData(e.log.active); err != nil {
		return err
	}
	e.log.activeOffset = off
	e.log.activeHash = sha256.New()
	if _, err = io.CopyN(e.log.activeHash, io.NewSectionReader(e.log.active, 0, int64(off)), int64(off)); err != nil {
		return err
	}
	return nil
}

func inspectWaveHeader(header []byte) (uint64, error) {
	if len(header) != recordHeaderBytes || binary.LittleEndian.Uint16(header[4:6]) != RecordWave || binary.LittleEndian.Uint16(header[6:8]) != FormatVersion || binary.LittleEndian.Uint32(header[36:40]) != crc32.Checksum(header[:36], crcTable) {
		return 0, ErrCorrupt
	}
	total := uint64(binary.LittleEndian.Uint32(header[:4]))
	if total < 72 || total > maxRecordBytes {
		return 0, ErrCorrupt
	}
	return total, nil
}

func decodeWaveFrame(frame []byte, expectedSequence uint64) (WaveID, [32]byte, []ReadyBatch, error) {
	if len(frame) < 72 {
		return WaveID{}, [32]byte{}, nil, ErrCorrupt
	}
	total, err := inspectWaveHeader(frame[:40])
	if err != nil || total != uint64(len(frame)) || binary.LittleEndian.Uint64(frame[8:16]) != expectedSequence || binary.LittleEndian.Uint32(frame[32:36]) != crc32.Checksum(frame[40:], crcTable) {
		return WaveID{}, [32]byte{}, nil, ErrCorrupt
	}
	var id WaveID
	copy(id[:], frame[16:32])
	if id == (WaveID{}) {
		return id, [32]byte{}, nil, ErrCorrupt
	}
	var stored [32]byte
	copy(stored[:], frame[40:72])
	digest := sha256.Sum256(frame[72:])
	if stored != digest {
		return id, digest, nil, ErrCorrupt
	}
	cursor := canonicalCursor{data: frame[72:]}
	count, err := cursor.uvarint()
	if err != nil || count == 0 {
		return id, digest, nil, ErrCorrupt
	}
	batches := make([]ReadyBatch, 0, count)
	previous := uint64(0)
	for range count {
		delta, err := cursor.uvarint()
		if err != nil || delta == 0 || previous > ^uint64(0)-delta {
			return id, digest, nil, ErrCorrupt
		}
		group := previous + delta
		if group == engineWaveGroup {
			return id, digest, nil, ErrCorrupt
		}
		flags, err := cursor.byte()
		if err != nil || flags & ^byte(batchReplace|batchPrefix|batchCheckpoint|batchHard) != 0 {
			return id, digest, nil, ErrCorrupt
		}
		batch := ReadyBatch{GroupID: group}
		if flags&batchReplace != 0 {
			batch.ReplaceFrom, err = cursor.uvarint()
			if err != nil || batch.ReplaceFrom == 0 {
				return id, digest, nil, ErrCorrupt
			}
		}
		if flags&batchPrefix != 0 {
			batch.TruncateIndex, err = cursor.uvarint()
			if err != nil {
				return id, digest, nil, ErrCorrupt
			}
			batch.TruncateTerm, err = cursor.uvarint()
			if err != nil {
				return id, digest, nil, ErrCorrupt
			}
		}
		if flags&batchCheckpoint != 0 {
			reference, readErr := cursor.take(16)
			if readErr != nil {
				return id, digest, nil, readErr
			}
			checkpoint := Checkpoint{}
			copy(checkpoint.ID[:], reference)
			checkpoint.Index, err = cursor.uvarint()
			if err != nil {
				return id, digest, nil, ErrCorrupt
			}
			checkpoint.Term, err = cursor.uvarint()
			if err != nil {
				return id, digest, nil, ErrCorrupt
			}
			batch.Checkpoint = &checkpoint
		}
		if flags&batchHard != 0 {
			hard := HardState{}
			hard.Term, err = cursor.uvarint()
			if err != nil {
				return id, digest, nil, ErrCorrupt
			}
			hard.Vote, err = cursor.uvarint()
			if err != nil {
				return id, digest, nil, ErrCorrupt
			}
			hard.Commit, err = cursor.uvarint()
			if err != nil {
				return id, digest, nil, ErrCorrupt
			}
			batch.Hard = &hard
		}
		entryCount, err := cursor.uvarint()
		if err != nil || entryCount > uint64(len(cursor.data)-cursor.off)/3+1 {
			return id, digest, nil, ErrCorrupt
		}
		batch.Entries = make([]Entry, 0, entryCount)
		for range entryCount {
			entry := Entry{}
			entry.Index, err = cursor.uvarint()
			if err != nil {
				return id, digest, nil, ErrCorrupt
			}
			entry.Term, err = cursor.uvarint()
			if err != nil {
				return id, digest, nil, ErrCorrupt
			}
			size, readErr := cursor.uvarint()
			if readErr != nil || size > uint64(len(cursor.data)-cursor.off) {
				return id, digest, nil, ErrCorrupt
			}
			entry.dataOffset = uint64(72 + cursor.off)
			data, readErr := cursor.take(int(size))
			if readErr != nil {
				return id, digest, nil, readErr
			}
			entry.Data = data
			batch.Entries = append(batch.Entries, entry)
		}
		batches = append(batches, batch)
		previous = group
	}
	if !cursor.empty() {
		return id, digest, nil, ErrCorrupt
	}
	return id, digest, batches, nil
}

func (e *Engine) eventsForDecoded(batches []ReadyBatch, frameOffset, sequence uint64, id WaveID, digest [32]byte) ([]segmentEvent, error) {
	events := make([]segmentEvent, 0)
	for i := range batches {
		batch := &batches[i]
		g := e.groups[batch.GroupID]
		if g == nil {
			g = &engineGroup{}
			e.groups[batch.GroupID] = g
		}
		if _, err := validateBatch(g, batch); err != nil {
			return nil, err
		}
		if batch.ReplaceFrom != 0 {
			events = append(events, segmentEvent{Kind: RecordTruncateSuffix, GroupID: batch.GroupID, Index: batch.ReplaceFrom, Term: predecessorTerm(g, batch.ReplaceFrom)})
		}
		for _, entry := range batch.Entries {
			events = append(events, segmentEvent{Kind: eventWaveEntry, GroupID: batch.GroupID, Index: entry.Index, Term: entry.Term, Offset: frameOffset + entry.dataOffset, Bytes: uint64(len(entry.Data))})
		}
		if batch.TruncateIndex != 0 {
			events = append(events, segmentEvent{Kind: eventPrefix, GroupID: batch.GroupID, Index: batch.TruncateIndex, Term: batch.TruncateTerm})
		}
		if batch.Checkpoint != nil {
			events = append(events, segmentEvent{Kind: eventCheckpoint, GroupID: batch.GroupID, Index: batch.Checkpoint.Index, Term: batch.Checkpoint.Term, Reference: batch.Checkpoint.ID})
		}
		if batch.Hard != nil {
			events = append(events, segmentEvent{Kind: eventHardState, GroupID: batch.GroupID, Term: batch.Hard.Term, Vote: batch.Hard.Vote, Commit: batch.Hard.Commit})
		}
	}
	events = append(events, segmentEvent{Kind: eventWave, GroupID: engineWaveGroup, Index: sequence, Reference: id, Digest: digest})
	return events, nil
}

func (e *Engine) DeepVerify() error {
	previousID, previousHash := e.log.manifest.AnchorID, e.log.manifest.AnchorHash
	for _, want := range e.log.manifest.Segments {
		path := filepath.Join(e.log.dir, sealedName(want.ID))
		got, footer, _, err := readSealedMetadata(path, e.log.manifest.LogID, previousID, previousHash)
		if err != nil {
			return err
		}
		if got != want {
			return ErrCorrupt
		}
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		sum, err := hashPrefix(f, footer.DataBytes)
		if err == nil && sum != footer.Hash {
			err = ErrCorrupt
		}
		if err == nil {
			off := uint64(segmentHeaderBytes)
			header := make([]byte, recordHeaderBytes)
			for off < footer.DataBytes {
				if footer.DataBytes-off < recordHeaderBytes {
					err = ErrCorrupt
					break
				}
				if _, err = f.ReadAt(header, int64(off)); err != nil {
					break
				}
				total, inspectErr := inspectWaveHeader(header)
				if inspectErr != nil || total > footer.DataBytes-off {
					err = ErrCorrupt
					break
				}
				frame := make([]byte, total)
				copy(frame, header)
				if _, err = f.ReadAt(frame[recordHeaderBytes:], int64(off+recordHeaderBytes)); err != nil {
					break
				}
				if _, _, _, inspectErr = decodeWaveFrame(frame, binary.LittleEndian.Uint64(header[8:16])); inspectErr != nil {
					err = inspectErr
					break
				}
				off += total
			}
		}
		closeErr := f.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
		previousID, previousHash = want.ID, want.Hash
	}
	return nil
}
