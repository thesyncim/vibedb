package seglog

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sort"
)

func (l *Log) reconcileRotation() error {
	activePath := filepath.Join(l.dir, activeName(l.manifest.ActiveID))
	f, err := os.OpenFile(activePath, os.O_RDWR, 0)
	if err == nil {
		l.active = f
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	// A missing manifest-selected active is recoverable only when that exact
	// segment has a complete sealed header/footer/index publication. This is the
	// rename-before-manifest crash state; payload scrubbing remains explicit.
	meta, footer, _, err := readSealedMetadataAuthenticated(filepath.Join(l.dir, sealedName(l.manifest.ActiveID)), l.manifest.LogID, l.expectedPreviousID(), l.expectedPreviousHash(), l.authKey)
	if err != nil {
		return fmt.Errorf("%w: selected active missing: %v", ErrCorrupt, err)
	}
	nextID := footer.ID + 1
	nextGeneration := footer.Generation + 1
	nextPath := filepath.Join(l.dir, activeName(nextID))
	next, openErr := os.OpenFile(nextPath, os.O_RDWR, 0)
	if errors.Is(openErr, os.ErrNotExist) {
		// A create crash can leave only the unpublished staging inode. It is
		// never a recovery input and is safe to unlink under the single-writer
		// directory lock expected by the eventual production adapter.
		_ = os.Remove(filepath.Join(l.dir, fmt.Sprintf(".%020d.tmp", nextID)))
		next, openErr = createSegment(l.dir, segmentHeader{ID: nextID, Generation: nextGeneration, PreviousID: footer.ID, PreviousHash: footer.Hash, LogID: l.manifest.LogID})
	}
	if openErr != nil {
		return openErr
	}
	hb := make([]byte, segmentHeaderBytes)
	if _, err = next.ReadAt(hb, 0); err != nil {
		_ = next.Close()
		return err
	}
	h, err := unmarshalSegmentHeader(hb)
	if err != nil || h.ID != nextID || h.Generation != nextGeneration || h.PreviousID != footer.ID || h.PreviousHash != footer.Hash || h.LogID != l.manifest.LogID {
		_ = next.Close()
		return fmt.Errorf("%w: next active does not continue sealed chain", ErrCorrupt)
	}
	st, err := next.Stat()
	if err != nil {
		_ = next.Close()
		return err
	}
	if st.Size() != segmentHeaderBytes {
		_ = next.Close()
		return fmt.Errorf("%w: unpublished next active contains records", ErrCorrupt)
	}
	nm := l.manifest
	nm.Generation++
	nm.ActiveID = nextID
	nm.ActiveGeneration = nextGeneration
	nm.DurableSegmentID = nextID
	nm.DurableOffset = segmentHeaderBytes
	nm.Segments = append(slices.Clone(nm.Segments), meta)
	if err = publishManifest(l.dir, nm); err != nil {
		_ = next.Close()
		return err
	}
	l.manifest, l.active = nm, next
	return nil
}

func (l *Log) expectedPreviousID() uint64 {
	if n := len(l.manifest.Segments); n != 0 {
		return l.manifest.Segments[n-1].ID
	}
	return l.manifest.AnchorID
}
func (l *Log) expectedPreviousHash() [32]byte {
	if n := len(l.manifest.Segments); n != 0 {
		return l.manifest.Segments[n-1].Hash
	}
	return l.manifest.AnchorHash
}

func (l *Log) rebuild() error {
	for _, gm := range l.manifest.Groups {
		l.index[gm.GroupID] = &GroupIndex{TruncateIndex: gm.TruncateIndex, TruncateTerm: gm.TruncateTerm}
		l.last[gm.GroupID], l.lastTerm[gm.GroupID] = gm.TruncateIndex, gm.TruncateTerm
	}
	previousID, previousHash := l.manifest.AnchorID, l.manifest.AnchorHash
	for _, want := range l.manifest.Segments {
		got, _, events, err := readSealedMetadataAuthenticated(filepath.Join(l.dir, sealedName(want.ID)), l.manifest.LogID, previousID, previousHash, l.authKey)
		if err != nil {
			return err
		}
		if got != want {
			return fmt.Errorf("%w: sealed segment %d differs from manifest", ErrCorrupt, want.ID)
		}
		for _, event := range events {
			if err = l.applyEvent(event, want.ID); err != nil {
				return err
			}
		}
		previousID, previousHash = want.ID, want.Hash
	}
	hb := make([]byte, segmentHeaderBytes)
	if _, err := l.active.ReadAt(hb, 0); err != nil {
		return err
	}
	h, err := unmarshalSegmentHeader(hb)
	if err != nil || h.ID != l.manifest.ActiveID || h.Generation != l.manifest.ActiveGeneration || h.PreviousID != previousID || h.PreviousHash != previousHash || h.LogID != l.manifest.LogID {
		return fmt.Errorf("%w: active segment chain", ErrCorrupt)
	}
	st, err := l.active.Stat()
	if err != nil {
		return err
	}
	if uint64(st.Size()) < l.manifest.DurableOffset {
		return fmt.Errorf("%w: active shorter than durable offset", ErrCorrupt)
	}
	if _, err = l.scanRecords(l.active, h.ID, segmentHeaderBytes, l.manifest.DurableOffset, true); err != nil {
		return err
	}
	if len(l.last) != len(l.manifest.Groups) {
		return fmt.Errorf("%w: per-group durable index set", ErrCorrupt)
	}
	for _, gm := range l.manifest.Groups {
		if l.last[gm.GroupID] != gm.DurableLastIndex || l.lastTerm[gm.GroupID] != gm.DurableLastTerm {
			return fmt.Errorf("%w: group %d durable index metadata", ErrCorrupt, gm.GroupID)
		}
		g := l.index[gm.GroupID]
		cut := sort.Search(len(g.Entries), func(i int) bool { return g.Entries[i].Index > gm.TruncateIndex })
		g.Entries = slices.Delete(g.Entries, 0, cut)
		g.TruncateIndex, g.TruncateTerm = gm.TruncateIndex, gm.TruncateTerm
	}
	// The manifest is the anti-invention boundary. Complete-looking records,
	// partial envelopes, and a footer beyond it are all discarded identically.
	if uint64(st.Size()) != l.manifest.DurableOffset {
		if err = l.active.Truncate(int64(l.manifest.DurableOffset)); err != nil {
			return err
		}
		if err = l.active.Sync(); err != nil {
			return err
		}
	}
	l.activeOffset = l.manifest.DurableOffset
	l.activeHash = sha256.New()
	if _, err = io.CopyN(l.activeHash, io.NewSectionReader(l.active, 0, int64(l.activeOffset)), int64(l.activeOffset)); err != nil {
		return err
	}
	return nil
}

func (l *Log) scanRecords(f *os.File, segmentID, start, end uint64, active bool) (uint64, error) {
	off := start
	var count uint64
	header := make([]byte, recordHeaderBytes)
	var recordBuf []byte
	for off < end {
		if end-off < recordHeaderBytes {
			return 0, fmt.Errorf("%w: committed partial record header", ErrCorrupt)
		}
		if _, err := f.ReadAt(header, int64(off)); err != nil {
			return 0, fmt.Errorf("%w: read record header: %v", ErrCorrupt, err)
		}
		n := uint64(binaryUint32(header[0:4]))
		if n < recordHeaderBytes || n > maxRecordBytes+recordHeaderBytes || n > end-off {
			return 0, fmt.Errorf("%w: committed partial record", ErrCorrupt)
		}
		if uint64(cap(recordBuf)) < n {
			recordBuf = make([]byte, n)
		} else {
			recordBuf = recordBuf[:n]
		}
		buf := recordBuf
		copy(buf, header)
		if _, err := f.ReadAt(buf[recordHeaderBytes:], int64(off+recordHeaderBytes)); err != nil && !errors.Is(err, io.EOF) {
			return 0, err
		}
		r, consumed, err := inspectRecord(buf)
		if err != nil || consumed != n {
			return 0, fmt.Errorf("segment %d offset %d: %w", segmentID, off, errors.Join(ErrCorrupt, err))
		}
		event := segmentEvent{Kind: r.Kind, GroupID: r.GroupID, Index: r.Index, Term: r.Term}
		if r.Kind == RecordEntry {
			event.Offset, event.Bytes = off, n
		}
		if err = l.applyEvent(event, segmentID); err != nil {
			return 0, err
		}
		if active {
			l.events = append(l.events, event)
			l.records++
		}
		off += n
		count++
	}
	if off != end {
		return 0, fmt.Errorf("%w: segment record boundary", ErrCorrupt)
	}
	return count, nil
}

func (l *Log) applyEvent(event segmentEvent, segmentID uint64) error {
	g := l.index[event.GroupID]
	if g == nil {
		g = &GroupIndex{}
		l.index[event.GroupID] = g
	}
	switch event.Kind {
	case RecordEntry:
		if event.Index <= g.TruncateIndex {
			return nil
		}
		if event.Index <= l.last[event.GroupID] {
			return fmt.Errorf("%w: group %d record index regression", ErrCorrupt, event.GroupID)
		}
		g.Entries = append(g.Entries, Location{SegmentID: segmentID, Offset: event.Offset, Bytes: event.Bytes, Index: event.Index, Term: event.Term})
		l.last[event.GroupID], l.lastTerm[event.GroupID] = event.Index, event.Term
	case RecordTruncateSuffix:
		if event.Index <= g.TruncateIndex {
			return nil
		}
		if event.Index > l.last[event.GroupID]+1 {
			return fmt.Errorf("%w: group %d suffix truncation", ErrCorrupt, event.GroupID)
		}
		cut := sort.Search(len(g.Entries), func(i int) bool { return g.Entries[i].Index >= event.Index })
		wantTerm := g.TruncateTerm
		if cut != 0 {
			wantTerm = g.Entries[cut-1].Term
		}
		if event.Term != wantTerm {
			return fmt.Errorf("%w: group %d suffix predecessor term", ErrCorrupt, event.GroupID)
		}
		g.Entries = slices.Delete(g.Entries, cut, len(g.Entries))
		l.last[event.GroupID], l.lastTerm[event.GroupID] = event.Index-1, event.Term
	default:
		return fmt.Errorf("%w: unknown segment event", ErrCorrupt)
	}
	return nil
}

func readSealedMetadata(path string, logID [16]byte, previousID uint64, previousHash [32]byte) (SegmentMeta, segmentFooter, []segmentEvent, error) {
	return readSealedMetadataAuthenticated(path, logID, previousID, previousHash, [32]byte{})
}

func readSealedMetadataAuthenticated(path string, logID [16]byte, previousID uint64, previousHash, authKey [32]byte) (SegmentMeta, segmentFooter, []segmentEvent, error) {
	f, err := os.Open(path)
	if err != nil {
		return SegmentMeta{}, segmentFooter{}, nil, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return SegmentMeta{}, segmentFooter{}, nil, err
	}
	if st.Size() < segmentHeaderBytes+segmentIndexHeaderBytes+4+segmentFooterBytes {
		return SegmentMeta{}, segmentFooter{}, nil, fmt.Errorf("%w: short sealed segment", ErrCorrupt)
	}
	hb := make([]byte, segmentHeaderBytes)
	if _, err = f.ReadAt(hb, 0); err != nil {
		return SegmentMeta{}, segmentFooter{}, nil, err
	}
	h, err := unmarshalSegmentHeader(hb)
	if err != nil {
		return SegmentMeta{}, segmentFooter{}, nil, err
	}
	if h.LogID != logID || h.PreviousID != previousID || h.PreviousHash != previousHash {
		return SegmentMeta{}, segmentFooter{}, nil, fmt.Errorf("%w: segment chain mismatch", ErrCorrupt)
	}
	fb := make([]byte, segmentFooterBytes)
	if _, err = f.ReadAt(fb, st.Size()-segmentFooterBytes); err != nil {
		return SegmentMeta{}, segmentFooter{}, nil, err
	}
	footer, err := unmarshalSegmentFooter(fb)
	if err != nil {
		return SegmentMeta{}, segmentFooter{}, nil, err
	}
	fileBytes := uint64(st.Size())
	if footer.ID != h.ID || footer.Generation != h.Generation || footer.IndexOffset > fileBytes-segmentFooterBytes || footer.IndexBytes != fileBytes-segmentFooterBytes-footer.IndexOffset {
		return SegmentMeta{}, segmentFooter{}, nil, fmt.Errorf("%w: footer/header identity", ErrCorrupt)
	}
	indexBytes := make([]byte, footer.IndexBytes)
	if _, err = f.ReadAt(indexBytes, int64(footer.IndexOffset)); err != nil {
		return SegmentMeta{}, segmentFooter{}, nil, err
	}
	if authKey != ([32]byte{}) && footer.Auth != segmentMetadataMAC(authKey, h, indexBytes, footer) {
		return SegmentMeta{}, segmentFooter{}, nil, fmt.Errorf("%w: sealed metadata authentication", ErrCorrupt)
	}
	events, err := unmarshalSegmentIndex(indexBytes, footer.DataBytes, footer.Events)
	if err != nil {
		return SegmentMeta{}, segmentFooter{}, nil, err
	}
	meta := SegmentMeta{ID: h.ID, Generation: h.Generation, Bytes: uint64(st.Size()), Records: footer.Records, IndexOffset: footer.IndexOffset, IndexBytes: footer.IndexBytes, PreviousHash: h.PreviousHash, Hash: footer.Hash}
	return meta, footer, events, nil
}

// DeepVerify is the explicit bit-rot scrub path. Normal Open intentionally
// reads only sealed headers, footers, and compact event indexes; this method
// hashes and decodes every retained sealed payload byte.
func (l *Log) DeepVerify() error {
	previousID, previousHash := l.manifest.AnchorID, l.manifest.AnchorHash
	for _, want := range l.manifest.Segments {
		path := filepath.Join(l.dir, sealedName(want.ID))
		got, footer, indexed, err := readSealedMetadataAuthenticated(path, l.manifest.LogID, previousID, previousHash, l.authKey)
		if err != nil {
			return err
		}
		if got != want {
			return fmt.Errorf("%w: sealed segment metadata", ErrCorrupt)
		}
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		sum, hashErr := hashPrefix(f, footer.DataBytes)
		var scanned []segmentEvent
		if hashErr == nil {
			scanned, hashErr = readRecordEvents(f, footer.DataBytes)
		}
		closeErr := f.Close()
		if hashErr != nil {
			return hashErr
		}
		if closeErr != nil {
			return closeErr
		}
		if !bytes.Equal(sum[:], footer.Hash[:]) {
			return fmt.Errorf("%w: sealed segment hash", ErrCorrupt)
		}
		if !slices.Equal(canonicalEventOrder(scanned), indexed) {
			return fmt.Errorf("%w: sealed index differs from records", ErrCorrupt)
		}
		previousID, previousHash = want.ID, want.Hash
	}
	return nil
}

func readRecordEvents(f *os.File, end uint64) ([]segmentEvent, error) {
	off := uint64(segmentHeaderBytes)
	result := make([]segmentEvent, 0)
	header := make([]byte, recordHeaderBytes)
	var recordBuf []byte
	for off < end {
		if end-off < recordHeaderBytes {
			return nil, fmt.Errorf("%w: partial record header", ErrCorrupt)
		}
		if _, err := f.ReadAt(header, int64(off)); err != nil {
			return nil, err
		}
		n := uint64(binaryUint32(header[0:4]))
		if n < recordHeaderBytes || n > maxRecordBytes+recordHeaderBytes || n > end-off {
			return nil, fmt.Errorf("%w: record geometry", ErrCorrupt)
		}
		if uint64(cap(recordBuf)) < n {
			recordBuf = make([]byte, n)
		} else {
			recordBuf = recordBuf[:n]
		}
		copy(recordBuf, header)
		if _, err := f.ReadAt(recordBuf[recordHeaderBytes:], int64(off+recordHeaderBytes)); err != nil {
			return nil, err
		}
		r, consumed, err := inspectRecord(recordBuf)
		if err != nil || consumed != n {
			return nil, errors.Join(ErrCorrupt, err)
		}
		event := segmentEvent{Kind: r.Kind, GroupID: r.GroupID, Index: r.Index, Term: r.Term}
		if r.Kind == RecordEntry {
			event.Offset, event.Bytes = off, n
		}
		result = append(result, event)
		off += n
	}
	if off != end {
		return nil, fmt.Errorf("%w: record boundary", ErrCorrupt)
	}
	return result, nil
}

func binaryUint32(b []byte) uint32 {
	return uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
}
