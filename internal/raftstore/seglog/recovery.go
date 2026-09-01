package seglog

import (
	"bytes"
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
	// segment is a fully valid sealed file. This is the rename-before-manifest
	// crash state; no other absent-active state is guessed.
	meta, footer, err := verifySealed(filepath.Join(l.dir, sealedName(l.manifest.ActiveID)), l.manifest.LogID, l.expectedPreviousID(), l.expectedPreviousHash())
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
	return 0
}
func (l *Log) expectedPreviousHash() [32]byte {
	if n := len(l.manifest.Segments); n != 0 {
		return l.manifest.Segments[n-1].Hash
	}
	return [32]byte{}
}

func (l *Log) rebuild() error {
	var previousID uint64
	var previousHash [32]byte
	for _, want := range l.manifest.Segments {
		got, _, err := verifySealed(filepath.Join(l.dir, sealedName(want.ID)), l.manifest.LogID, previousID, previousHash)
		if err != nil {
			return err
		}
		if got != want {
			return fmt.Errorf("%w: sealed segment %d differs from manifest", ErrCorrupt, want.ID)
		}
		f, err := os.Open(filepath.Join(l.dir, sealedName(want.ID)))
		if err != nil {
			return err
		}
		var count uint64
		count, err = l.scanRecords(f, want.ID, segmentHeaderBytes, want.Bytes-segmentFooterBytes, true)
		closeErr := f.Close()
		if err != nil {
			return err
		}
		if count != want.Records {
			return fmt.Errorf("%w: sealed segment %d record count", ErrCorrupt, want.ID)
		}
		if closeErr != nil {
			return closeErr
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
	if _, err = l.scanRecords(l.active, h.ID, segmentHeaderBytes, l.manifest.DurableOffset, false); err != nil {
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
	return nil
}

func (l *Log) scanRecords(f *os.File, segmentID, start, end uint64, sealed bool) (uint64, error) {
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
		n := uint64(binaryUint32(header[8:12]))
		if n < recordHeaderBytes+4 || n > maxRecordBytes+recordHeaderBytes+4 || n > end-off {
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
		g := l.index[r.GroupID]
		if g == nil {
			g = &GroupIndex{}
			l.index[r.GroupID] = g
		}
		switch r.Kind {
		case RecordEntry:
			if r.Index <= l.last[r.GroupID] || r.Index <= g.TruncateIndex {
				return 0, fmt.Errorf("%w: group %d record index regression", ErrCorrupt, r.GroupID)
			}
			g.Entries = append(g.Entries, Location{SegmentID: segmentID, Offset: off, Bytes: n, Index: r.Index, Term: r.Term})
			l.last[r.GroupID], l.lastTerm[r.GroupID] = r.Index, r.Term
		case RecordTruncateSuffix:
			if r.Index <= g.TruncateIndex || r.Index > l.last[r.GroupID]+1 {
				return 0, fmt.Errorf("%w: group %d suffix truncation", ErrCorrupt, r.GroupID)
			}
			cut := sort.Search(len(g.Entries), func(i int) bool { return g.Entries[i].Index >= r.Index })
			wantTerm := g.TruncateTerm
			if cut != 0 {
				wantTerm = g.Entries[cut-1].Term
			}
			if r.Term != wantTerm {
				return 0, fmt.Errorf("%w: group %d suffix predecessor term", ErrCorrupt, r.GroupID)
			}
			g.Entries = slices.Delete(g.Entries, cut, len(g.Entries))
			l.last[r.GroupID], l.lastTerm[r.GroupID] = r.Index-1, r.Term
		}
		if !sealed {
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

func verifySealed(path string, logID [16]byte, previousID uint64, previousHash [32]byte) (SegmentMeta, segmentFooter, error) {
	f, err := os.Open(path)
	if err != nil {
		return SegmentMeta{}, segmentFooter{}, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return SegmentMeta{}, segmentFooter{}, err
	}
	if st.Size() < segmentHeaderBytes+segmentFooterBytes {
		return SegmentMeta{}, segmentFooter{}, fmt.Errorf("%w: short sealed segment", ErrCorrupt)
	}
	hb := make([]byte, segmentHeaderBytes)
	if _, err = f.ReadAt(hb, 0); err != nil {
		return SegmentMeta{}, segmentFooter{}, err
	}
	h, err := unmarshalSegmentHeader(hb)
	if err != nil {
		return SegmentMeta{}, segmentFooter{}, err
	}
	if h.LogID != logID || h.PreviousID != previousID || h.PreviousHash != previousHash {
		return SegmentMeta{}, segmentFooter{}, fmt.Errorf("%w: segment chain mismatch", ErrCorrupt)
	}
	fb := make([]byte, segmentFooterBytes)
	if _, err = f.ReadAt(fb, st.Size()-segmentFooterBytes); err != nil {
		return SegmentMeta{}, segmentFooter{}, err
	}
	footer, err := unmarshalSegmentFooter(fb)
	if err != nil {
		return SegmentMeta{}, segmentFooter{}, err
	}
	if footer.ID != h.ID || footer.Generation != h.Generation || footer.PreviousHash != h.PreviousHash || footer.DataBytes+segmentFooterBytes != uint64(st.Size()) {
		return SegmentMeta{}, segmentFooter{}, fmt.Errorf("%w: footer/header identity", ErrCorrupt)
	}
	sum, err := hashPrefix(f, footer.DataBytes)
	if err != nil {
		return SegmentMeta{}, segmentFooter{}, err
	}
	if !bytes.Equal(sum[:], footer.Hash[:]) {
		return SegmentMeta{}, segmentFooter{}, fmt.Errorf("%w: sealed segment hash", ErrCorrupt)
	}
	return SegmentMeta{ID: h.ID, Generation: h.Generation, Bytes: uint64(st.Size()), Records: footer.Records, PreviousHash: h.PreviousHash, Hash: sum}, footer, nil
}

func binaryUint32(b []byte) uint32 {
	return uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
}
