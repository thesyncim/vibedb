// Package seglog implements the version 3, node-wide segmented Raft log
// format. It is intentionally not wired into raftstore yet: the package is the
// recovery and indexing foundation for that cutover.
package seglog

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

const (
	FormatVersion uint16 = 3
	ManifestName         = "MANIFEST.v3"

	manifestHeaderBytes  = 80
	manifestSegmentBytes = 96
	manifestGroupBytes   = 48
	segmentHeaderBytes   = 128
	recordHeaderBytes    = 64
	segmentFooterBytes   = 128
	maxManifestBytes     = 64 << 20
	maxRecordBytes       = 64 << 20
)

var (
	ErrCorrupt    = errors.New("seglog: corrupt")
	ErrBounds     = errors.New("seglog: bounds exceeded")
	crcTable      = crc32.MakeTable(crc32.Castagnoli)
	manifestMagic = [8]byte{'V', 'D', 'B', 'S', 'M', 'A', 'N', 0}
	headerMagic   = [8]byte{'V', 'D', 'B', 'S', 'E', 'G', 'H', 0}
	recordMagic   = [8]byte{'V', 'D', 'B', 'S', 'R', 'E', 'C', 0}
	footerMagic   = [8]byte{'V', 'D', 'B', 'S', 'E', 'G', 'F', 0}
)

type SegmentMeta struct {
	ID, Generation, Bytes, Records uint64
	PreviousHash, Hash             [32]byte
}

type GroupMeta struct {
	GroupID, TruncateIndex, TruncateTerm uint64
	DurableLastIndex, DurableLastTerm    uint64
}

type Manifest struct {
	Generation, ActiveID, ActiveGeneration uint64
	DurableSegmentID, DurableOffset        uint64
	LogID                                  [16]byte
	Segments                               []SegmentMeta
	Groups                                 []GroupMeta
}

type segmentHeader struct {
	ID, Generation, PreviousID uint64
	PreviousHash               [32]byte
	LogID                      [16]byte
}

type segmentFooter struct {
	ID, Generation, Records, DataBytes uint64
	PreviousHash, Hash                 [32]byte
}

type Record struct {
	GroupID, Index, Term uint64
	Kind                 uint16
	Flags                uint16
	Payload              []byte
}

func putCRC(dst []byte) {
	binary.LittleEndian.PutUint32(dst[len(dst)-4:], crc32.Checksum(dst[:len(dst)-4], crcTable))
}
func validCRC(src []byte) bool {
	return len(src) >= 4 && binary.LittleEndian.Uint32(src[len(src)-4:]) == crc32.Checksum(src[:len(src)-4], crcTable)
}

func marshalManifest(m Manifest) ([]byte, error) {
	if m.Generation == 0 || m.ActiveID == 0 || m.ActiveGeneration == 0 || m.DurableSegmentID != m.ActiveID || m.DurableOffset < segmentHeaderBytes {
		return nil, fmt.Errorf("%w: invalid manifest state", ErrCorrupt)
	}
	length := manifestHeaderBytes + len(m.Segments)*manifestSegmentBytes + len(m.Groups)*manifestGroupBytes + 4
	if length > maxManifestBytes {
		return nil, ErrBounds
	}
	b := make([]byte, length)
	copy(b, manifestMagic[:])
	binary.LittleEndian.PutUint16(b[8:10], FormatVersion)
	binary.LittleEndian.PutUint16(b[10:12], manifestHeaderBytes)
	binary.LittleEndian.PutUint32(b[12:16], uint32(length))
	binary.LittleEndian.PutUint64(b[16:24], m.Generation)
	binary.LittleEndian.PutUint64(b[24:32], m.ActiveID)
	binary.LittleEndian.PutUint64(b[32:40], m.ActiveGeneration)
	binary.LittleEndian.PutUint64(b[40:48], m.DurableSegmentID)
	binary.LittleEndian.PutUint64(b[48:56], m.DurableOffset)
	binary.LittleEndian.PutUint32(b[56:60], uint32(len(m.Segments)))
	binary.LittleEndian.PutUint32(b[60:64], uint32(len(m.Groups)))
	copy(b[64:80], m.LogID[:])
	off := manifestHeaderBytes
	for _, s := range m.Segments {
		binary.LittleEndian.PutUint64(b[off:off+8], s.ID)
		binary.LittleEndian.PutUint64(b[off+8:off+16], s.Generation)
		binary.LittleEndian.PutUint64(b[off+16:off+24], s.Bytes)
		binary.LittleEndian.PutUint64(b[off+24:off+32], s.Records)
		copy(b[off+32:off+64], s.PreviousHash[:])
		copy(b[off+64:off+96], s.Hash[:])
		off += manifestSegmentBytes
	}
	for _, g := range m.Groups {
		binary.LittleEndian.PutUint64(b[off:off+8], g.GroupID)
		binary.LittleEndian.PutUint64(b[off+8:off+16], g.TruncateIndex)
		binary.LittleEndian.PutUint64(b[off+16:off+24], g.TruncateTerm)
		binary.LittleEndian.PutUint64(b[off+24:off+32], g.DurableLastIndex)
		binary.LittleEndian.PutUint64(b[off+32:off+40], g.DurableLastTerm)
		off += manifestGroupBytes
	}
	putCRC(b)
	return b, nil
}

func unmarshalManifest(b []byte) (Manifest, error) {
	if len(b) < manifestHeaderBytes+4 || len(b) > maxManifestBytes || !validCRC(b) || string(b[:8]) != string(manifestMagic[:]) || binary.LittleEndian.Uint16(b[8:10]) != FormatVersion || binary.LittleEndian.Uint16(b[10:12]) != manifestHeaderBytes || int(binary.LittleEndian.Uint32(b[12:16])) != len(b) {
		return Manifest{}, fmt.Errorf("%w: manifest envelope", ErrCorrupt)
	}
	ns, ng := int(binary.LittleEndian.Uint32(b[56:60])), int(binary.LittleEndian.Uint32(b[60:64]))
	if ns > (len(b)-manifestHeaderBytes-4)/manifestSegmentBytes || ng > (len(b)-manifestHeaderBytes-4-ns*manifestSegmentBytes)/manifestGroupBytes || manifestHeaderBytes+ns*manifestSegmentBytes+ng*manifestGroupBytes+4 != len(b) {
		return Manifest{}, fmt.Errorf("%w: manifest geometry", ErrCorrupt)
	}
	m := Manifest{Generation: binary.LittleEndian.Uint64(b[16:24]), ActiveID: binary.LittleEndian.Uint64(b[24:32]), ActiveGeneration: binary.LittleEndian.Uint64(b[32:40]), DurableSegmentID: binary.LittleEndian.Uint64(b[40:48]), DurableOffset: binary.LittleEndian.Uint64(b[48:56]), Segments: make([]SegmentMeta, 0, ns), Groups: make([]GroupMeta, 0, ng)}
	copy(m.LogID[:], b[64:80])
	off := manifestHeaderBytes
	var last uint64
	var lastGeneration uint64
	var lastHash [32]byte
	for range ns {
		s := SegmentMeta{ID: binary.LittleEndian.Uint64(b[off : off+8]), Generation: binary.LittleEndian.Uint64(b[off+8 : off+16]), Bytes: binary.LittleEndian.Uint64(b[off+16 : off+24]), Records: binary.LittleEndian.Uint64(b[off+24 : off+32])}
		copy(s.PreviousHash[:], b[off+32:off+64])
		copy(s.Hash[:], b[off+64:off+96])
		if s.ID == 0 || s.ID != last+1 || s.ID >= m.ActiveID || s.Generation != lastGeneration+1 || s.Bytes < segmentHeaderBytes+segmentFooterBytes || s.PreviousHash != lastHash || s.Hash == ([32]byte{}) {
			return Manifest{}, fmt.Errorf("%w: segment IDs not monotonic", ErrCorrupt)
		}
		last, lastGeneration, lastHash = s.ID, s.Generation, s.Hash
		m.Segments = append(m.Segments, s)
		off += manifestSegmentBytes
	}
	last = 0
	for range ng {
		g := GroupMeta{GroupID: binary.LittleEndian.Uint64(b[off : off+8]), TruncateIndex: binary.LittleEndian.Uint64(b[off+8 : off+16]), TruncateTerm: binary.LittleEndian.Uint64(b[off+16 : off+24]), DurableLastIndex: binary.LittleEndian.Uint64(b[off+24 : off+32]), DurableLastTerm: binary.LittleEndian.Uint64(b[off+32 : off+40])}
		if g.GroupID == 0 || g.GroupID <= last || (g.TruncateIndex == 0) != (g.TruncateTerm == 0) || g.DurableLastIndex == 0 || g.DurableLastTerm == 0 || g.DurableLastIndex < g.TruncateIndex || !allZero(b[off+40:off+manifestGroupBytes]) {
			return Manifest{}, fmt.Errorf("%w: group IDs not monotonic", ErrCorrupt)
		}
		last = g.GroupID
		m.Groups = append(m.Groups, g)
		off += manifestGroupBytes
	}
	if m.Generation == 0 || m.ActiveID == 0 || m.ActiveGeneration != uint64(ns)+1 || m.DurableSegmentID != m.ActiveID || m.DurableOffset < segmentHeaderBytes || m.ActiveID != uint64(ns)+1 || m.LogID == ([16]byte{}) {
		return Manifest{}, fmt.Errorf("%w: manifest state", ErrCorrupt)
	}
	return m, nil
}

func marshalSegmentHeader(h segmentHeader) []byte {
	b := make([]byte, segmentHeaderBytes)
	copy(b, headerMagic[:])
	binary.LittleEndian.PutUint16(b[8:10], FormatVersion)
	binary.LittleEndian.PutUint16(b[10:12], segmentHeaderBytes)
	binary.LittleEndian.PutUint64(b[16:24], h.ID)
	binary.LittleEndian.PutUint64(b[24:32], h.Generation)
	binary.LittleEndian.PutUint64(b[32:40], h.PreviousID)
	copy(b[40:72], h.PreviousHash[:])
	copy(b[72:88], h.LogID[:])
	putCRC(b)
	return b
}

func unmarshalSegmentHeader(b []byte) (segmentHeader, error) {
	if len(b) != segmentHeaderBytes || !validCRC(b) || string(b[:8]) != string(headerMagic[:]) || binary.LittleEndian.Uint16(b[8:10]) != FormatVersion || binary.LittleEndian.Uint16(b[10:12]) != segmentHeaderBytes {
		return segmentHeader{}, fmt.Errorf("%w: segment header", ErrCorrupt)
	}
	h := segmentHeader{ID: binary.LittleEndian.Uint64(b[16:24]), Generation: binary.LittleEndian.Uint64(b[24:32]), PreviousID: binary.LittleEndian.Uint64(b[32:40])}
	copy(h.PreviousHash[:], b[40:72])
	copy(h.LogID[:], b[72:88])
	if h.ID == 0 || h.Generation == 0 || h.ID != h.Generation || (h.ID == 1) != (h.PreviousID == 0) || h.PreviousID+1 != h.ID || h.LogID == ([16]byte{}) || !allZero(b[88:segmentHeaderBytes-4]) {
		return segmentHeader{}, fmt.Errorf("%w: segment header identity", ErrCorrupt)
	}
	return h, nil
}

func recordSize(payload int) uint64 { return uint64(recordHeaderBytes + payload + 4) }

func marshalRecord(r Record, dst []byte) ([]byte, error) {
	if r.GroupID == 0 || r.Index == 0 || r.Term == 0 || len(r.Payload) > maxRecordBytes {
		return nil, ErrBounds
	}
	n := int(recordSize(len(r.Payload)))
	if cap(dst) < n {
		dst = make([]byte, n)
	} else {
		dst = dst[:n]
		clear(dst)
	}
	copy(dst, recordMagic[:])
	binary.LittleEndian.PutUint32(dst[8:12], uint32(n))
	binary.LittleEndian.PutUint16(dst[12:14], r.Kind)
	binary.LittleEndian.PutUint16(dst[14:16], r.Flags)
	binary.LittleEndian.PutUint64(dst[16:24], r.GroupID)
	binary.LittleEndian.PutUint64(dst[24:32], r.Index)
	binary.LittleEndian.PutUint64(dst[32:40], r.Term)
	binary.LittleEndian.PutUint32(dst[40:44], uint32(len(r.Payload)))
	binary.LittleEndian.PutUint32(dst[44:48], crc32.Checksum(r.Payload, crcTable))
	binary.LittleEndian.PutUint32(dst[60:64], crc32.Checksum(dst[:60], crcTable))
	copy(dst[64:], r.Payload)
	putCRC(dst)
	return dst, nil
}

func inspectRecord(b []byte) (Record, uint64, error) {
	if len(b) < recordHeaderBytes || string(b[:8]) != string(recordMagic[:]) || binary.LittleEndian.Uint32(b[60:64]) != crc32.Checksum(b[:60], crcTable) {
		return Record{}, 0, fmt.Errorf("%w: record header", ErrCorrupt)
	}
	n := uint64(binary.LittleEndian.Uint32(b[8:12]))
	plen := uint64(binary.LittleEndian.Uint32(b[40:44]))
	if n != recordSize(int(plen)) || n > maxRecordBytes+recordHeaderBytes+4 || n < recordHeaderBytes+4 {
		return Record{}, 0, fmt.Errorf("%w: record geometry", ErrCorrupt)
	}
	if uint64(len(b)) < n {
		return Record{}, n, errors.New("seglog: incomplete record")
	}
	r := Record{Kind: binary.LittleEndian.Uint16(b[12:14]), Flags: binary.LittleEndian.Uint16(b[14:16]), GroupID: binary.LittleEndian.Uint64(b[16:24]), Index: binary.LittleEndian.Uint64(b[24:32]), Term: binary.LittleEndian.Uint64(b[32:40]), Payload: b[64 : n-4]}
	if r.GroupID == 0 || r.Index == 0 || r.Term == 0 || !allZero(b[48:60]) || crc32.Checksum(r.Payload, crcTable) != binary.LittleEndian.Uint32(b[44:48]) || !validCRC(b[:n]) {
		return Record{}, n, fmt.Errorf("%w: record checksum", ErrCorrupt)
	}
	return r, n, nil
}

func marshalSegmentFooter(f segmentFooter) []byte {
	b := make([]byte, segmentFooterBytes)
	copy(b, footerMagic[:])
	binary.LittleEndian.PutUint16(b[8:10], FormatVersion)
	binary.LittleEndian.PutUint16(b[10:12], segmentFooterBytes)
	binary.LittleEndian.PutUint64(b[16:24], f.ID)
	binary.LittleEndian.PutUint64(b[24:32], f.Generation)
	binary.LittleEndian.PutUint64(b[32:40], f.Records)
	binary.LittleEndian.PutUint64(b[40:48], f.DataBytes)
	copy(b[48:80], f.PreviousHash[:])
	copy(b[80:112], f.Hash[:])
	putCRC(b)
	return b
}

func unmarshalSegmentFooter(b []byte) (segmentFooter, error) {
	if len(b) != segmentFooterBytes || !validCRC(b) || string(b[:8]) != string(footerMagic[:]) || binary.LittleEndian.Uint16(b[8:10]) != FormatVersion || binary.LittleEndian.Uint16(b[10:12]) != segmentFooterBytes {
		return segmentFooter{}, fmt.Errorf("%w: segment footer", ErrCorrupt)
	}
	f := segmentFooter{ID: binary.LittleEndian.Uint64(b[16:24]), Generation: binary.LittleEndian.Uint64(b[24:32]), Records: binary.LittleEndian.Uint64(b[32:40]), DataBytes: binary.LittleEndian.Uint64(b[40:48])}
	copy(f.PreviousHash[:], b[48:80])
	copy(f.Hash[:], b[80:112])
	if f.ID == 0 || f.Generation == 0 || f.ID != f.Generation || f.DataBytes < segmentHeaderBytes || !allZero(b[112:segmentFooterBytes-4]) {
		return segmentFooter{}, fmt.Errorf("%w: footer state", ErrCorrupt)
	}
	return f, nil
}

func allZero(b []byte) bool {
	for _, v := range b {
		if v != 0 {
			return false
		}
	}
	return true
}
