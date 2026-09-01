// Package seglog implements the canonical node-wide segmented Raft log.
package seglog

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"slices"
	"sort"
)

const (
	FormatVersion        uint16 = 1
	ManifestName                = "MANIFEST"
	RecordTruncateSuffix uint16 = 2
	RecordWave           uint16 = 3
	eventHardState       uint16 = 4
	eventPrefix          uint16 = 5
	eventCheckpoint      uint16 = 6
	eventWave            uint16 = 7
	eventWaveEntry       uint16 = 8
	eventWaveEntryConf   uint16 = 9
	eventWaveEntryConfV2 uint16 = 10
	eventReadyState      uint16 = 11
	eventBlobEntry       uint16 = 12
	eventBlobEntryConf   uint16 = 13
	eventBlobEntryConfV2 uint16 = 14
	eventIncarnation     uint16 = 15

	manifestHeaderBytes  = 128
	manifestSegmentBytes = 120
	segmentHeaderBytes   = 128
	recordHeaderBytes    = 40
	segmentFooterBytes   = 160
	maxManifestBytes     = 64 << 20
	maxSegmentIndexBytes = 64 << 20
	maxRecordBytes       = 64 << 20
)

var _ [40 - recordHeaderBytes]byte
var _ [recordHeaderBytes - 40]byte

var (
	ErrCorrupt    = errors.New("seglog: corrupt")
	ErrBounds     = errors.New("seglog: bounds exceeded")
	ErrPoisoned   = errors.New("seglog: poisoned handle")
	crcTable      = crc32.MakeTable(crc32.Castagnoli)
	manifestMagic = [8]byte{'V', 'D', 'B', 'S', 'M', 'A', 'N', 0}
	headerMagic   = [8]byte{'V', 'D', 'B', 'S', 'E', 'G', 'H', 0}
	footerMagic   = [8]byte{'V', 'D', 'B', 'S', 'E', 'G', 'F', 0}
)

type SegmentMeta struct {
	ID, Generation, Bytes, Records uint64
	IndexOffset, IndexBytes        uint64
	PreviousHash, Hash             [32]byte
	State                          SegmentState
}

type SegmentState uint8

const (
	SegmentSealed SegmentState = iota + 1
	SegmentFrozenPending
)

type Manifest struct {
	Generation, ActiveID, ActiveGeneration uint64
	DurableSegmentID, DurableOffset        uint64
	LogID                                  [16]byte
	AnchorID, AnchorGeneration             uint64
	AnchorHash                             [32]byte
	Segments                               []SegmentMeta
}

type segmentHeader struct {
	ID, Generation, PreviousID uint64
	PreviousHash               [32]byte
	LogID                      [16]byte
}

type segmentFooter struct {
	ID, Generation, Records, DataBytes uint64
	Auth, Hash                         [32]byte
	IndexOffset, IndexBytes, Events    uint64
}

type segmentEvent struct {
	Kind                                uint16
	GroupID, Index, Term, Offset, Bytes uint64
	Vote, Commit                        uint64
	Incarnation, ReadyID                uint64
	DataOffset, DataBytes               uint64
	Reference                           [16]byte
	ReadyDigest                         [16]byte
	Digest                              [32]byte
}

func putCRC(dst []byte) {
	binary.LittleEndian.PutUint32(dst[len(dst)-4:], crc32.Checksum(dst[:len(dst)-4], crcTable))
}
func validCRC(src []byte) bool {
	return len(src) >= 4 && binary.LittleEndian.Uint32(src[len(src)-4:]) == crc32.Checksum(src[:len(src)-4], crcTable)
}

func marshalManifest(m Manifest) ([]byte, error) {
	if err := validateManifestForMarshal(m); err != nil {
		return nil, err
	}
	length := manifestHeaderBytes + len(m.Segments)*manifestSegmentBytes + 4
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
	copy(b[64:80], m.LogID[:])
	binary.LittleEndian.PutUint64(b[80:88], m.AnchorID)
	binary.LittleEndian.PutUint64(b[88:96], m.AnchorGeneration)
	copy(b[96:128], m.AnchorHash[:])
	off := manifestHeaderBytes
	for _, s := range m.Segments {
		binary.LittleEndian.PutUint64(b[off:off+8], s.ID)
		binary.LittleEndian.PutUint64(b[off+8:off+16], s.Generation)
		binary.LittleEndian.PutUint64(b[off+16:off+24], s.Bytes)
		binary.LittleEndian.PutUint64(b[off+24:off+32], s.Records)
		binary.LittleEndian.PutUint64(b[off+32:off+40], s.IndexOffset)
		binary.LittleEndian.PutUint64(b[off+40:off+48], s.IndexBytes)
		copy(b[off+48:off+80], s.PreviousHash[:])
		copy(b[off+80:off+112], s.Hash[:])
		b[off+112] = byte(s.State)
		off += manifestSegmentBytes
	}
	putCRC(b)
	return b, nil
}

func validateManifestForMarshal(m Manifest) error {
	anchorValid := m.AnchorID == 0 && m.AnchorGeneration == 0 && m.AnchorHash == ([32]byte{}) || m.AnchorID != 0 && m.AnchorGeneration != 0 && m.AnchorHash != ([32]byte{})
	if m.Generation == 0 || m.ActiveID == 0 || !anchorValid || m.DurableSegmentID != m.ActiveID || m.DurableOffset < segmentHeaderBytes || m.ActiveID != m.AnchorID+uint64(len(m.Segments))+1 || m.ActiveGeneration != m.AnchorGeneration+uint64(len(m.Segments))+1 || m.LogID == ([16]byte{}) {
		return fmt.Errorf("%w: invalid manifest state", ErrCorrupt)
	}
	lastID, lastGeneration, lastHash := m.AnchorID, m.AnchorGeneration, m.AnchorHash
	for i, s := range m.Segments {
		pending := pendingSegment(s)
		if s.ID != lastID+1 || s.Generation != lastGeneration+1 || !(validSegmentGeometry(s) || pending && i == len(m.Segments)-1) || s.PreviousHash != lastHash || s.Hash == ([32]byte{}) {
			return fmt.Errorf("%w: invalid segment metadata", ErrCorrupt)
		}
		lastID, lastGeneration, lastHash = s.ID, s.Generation, s.Hash
	}
	return nil
}

func unmarshalManifest(b []byte) (Manifest, error) {
	if len(b) < manifestHeaderBytes+4 || len(b) > maxManifestBytes || !validCRC(b) || string(b[:8]) != string(manifestMagic[:]) || binary.LittleEndian.Uint16(b[8:10]) != FormatVersion || binary.LittleEndian.Uint16(b[10:12]) != manifestHeaderBytes || int(binary.LittleEndian.Uint32(b[12:16])) != len(b) {
		return Manifest{}, fmt.Errorf("%w: manifest envelope", ErrCorrupt)
	}
	ns := int(binary.LittleEndian.Uint32(b[56:60]))
	if binary.LittleEndian.Uint32(b[60:64]) != 0 || ns > (len(b)-manifestHeaderBytes-4)/manifestSegmentBytes || manifestHeaderBytes+ns*manifestSegmentBytes+4 != len(b) {
		return Manifest{}, fmt.Errorf("%w: manifest geometry", ErrCorrupt)
	}
	m := Manifest{Generation: binary.LittleEndian.Uint64(b[16:24]), ActiveID: binary.LittleEndian.Uint64(b[24:32]), ActiveGeneration: binary.LittleEndian.Uint64(b[32:40]), DurableSegmentID: binary.LittleEndian.Uint64(b[40:48]), DurableOffset: binary.LittleEndian.Uint64(b[48:56]), Segments: make([]SegmentMeta, 0, ns)}
	copy(m.LogID[:], b[64:80])
	m.AnchorID = binary.LittleEndian.Uint64(b[80:88])
	m.AnchorGeneration = binary.LittleEndian.Uint64(b[88:96])
	copy(m.AnchorHash[:], b[96:128])
	off := manifestHeaderBytes
	last, lastGeneration, lastHash := m.AnchorID, m.AnchorGeneration, m.AnchorHash
	for i := range ns {
		s := SegmentMeta{ID: binary.LittleEndian.Uint64(b[off : off+8]), Generation: binary.LittleEndian.Uint64(b[off+8 : off+16]), Bytes: binary.LittleEndian.Uint64(b[off+16 : off+24]), Records: binary.LittleEndian.Uint64(b[off+24 : off+32]), IndexOffset: binary.LittleEndian.Uint64(b[off+32 : off+40]), IndexBytes: binary.LittleEndian.Uint64(b[off+40 : off+48])}
		copy(s.PreviousHash[:], b[off+48:off+80])
		copy(s.Hash[:], b[off+80:off+112])
		s.State = SegmentState(b[off+112])
		if !allZero(b[off+113 : off+manifestSegmentBytes]) {
			return Manifest{}, fmt.Errorf("%w: segment descriptor reserved bytes", ErrCorrupt)
		}
		if s.ID == 0 || s.ID != last+1 || s.ID >= m.ActiveID || s.Generation != lastGeneration+1 || !(validSegmentGeometry(s) || pendingSegment(s) && i == ns-1) || s.PreviousHash != lastHash || s.Hash == ([32]byte{}) {
			return Manifest{}, fmt.Errorf("%w: segment IDs not monotonic", ErrCorrupt)
		}
		last, lastGeneration, lastHash = s.ID, s.Generation, s.Hash
		m.Segments = append(m.Segments, s)
		off += manifestSegmentBytes
	}
	anchorValid := m.AnchorID == 0 && m.AnchorGeneration == 0 && m.AnchorHash == ([32]byte{}) || m.AnchorID != 0 && m.AnchorGeneration != 0 && m.AnchorHash != ([32]byte{})
	if m.Generation == 0 || m.ActiveID == 0 || !anchorValid || m.ActiveGeneration != m.AnchorGeneration+uint64(ns)+1 || m.DurableSegmentID != m.ActiveID || m.DurableOffset < segmentHeaderBytes || m.ActiveID != m.AnchorID+uint64(ns)+1 || m.LogID == ([16]byte{}) {
		return Manifest{}, fmt.Errorf("%w: manifest state", ErrCorrupt)
	}
	return m, nil
}

func validSegmentGeometry(s SegmentMeta) bool {
	return s.State == SegmentSealed && s.Bytes >= segmentFooterBytes && s.IndexOffset >= segmentHeaderBytes && s.IndexBytes >= sealedIndexHeaderBytes && s.IndexBytes <= maxSegmentIndexBytes && s.IndexOffset <= s.Bytes-segmentFooterBytes && s.IndexBytes == s.Bytes-segmentFooterBytes-s.IndexOffset
}

func pendingSegment(s SegmentMeta) bool {
	return s.State == SegmentFrozenPending && s.Bytes >= segmentHeaderBytes && s.IndexOffset == 0 && s.IndexBytes == 0 && s.Hash != ([32]byte{})
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
	if h.ID == 0 || h.Generation == 0 || (h.ID == 1) != (h.PreviousID == 0) || h.PreviousID+1 != h.ID || h.LogID == ([16]byte{}) || !allZero(b[88:segmentHeaderBytes-4]) {
		return segmentHeader{}, fmt.Errorf("%w: segment header identity", ErrCorrupt)
	}
	return h, nil
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
	copy(b[48:80], f.Auth[:])
	copy(b[80:112], f.Hash[:])
	binary.LittleEndian.PutUint64(b[112:120], f.IndexOffset)
	binary.LittleEndian.PutUint64(b[120:128], f.IndexBytes)
	binary.LittleEndian.PutUint64(b[128:136], f.Events)
	putCRC(b)
	return b
}

func unmarshalSegmentFooter(b []byte) (segmentFooter, error) {
	if len(b) != segmentFooterBytes || !validCRC(b) || string(b[:8]) != string(footerMagic[:]) || binary.LittleEndian.Uint16(b[8:10]) != FormatVersion || binary.LittleEndian.Uint16(b[10:12]) != segmentFooterBytes {
		return segmentFooter{}, fmt.Errorf("%w: segment footer", ErrCorrupt)
	}
	f := segmentFooter{ID: binary.LittleEndian.Uint64(b[16:24]), Generation: binary.LittleEndian.Uint64(b[24:32]), Records: binary.LittleEndian.Uint64(b[32:40]), DataBytes: binary.LittleEndian.Uint64(b[40:48])}
	copy(f.Auth[:], b[48:80])
	copy(f.Hash[:], b[80:112])
	f.IndexOffset = binary.LittleEndian.Uint64(b[112:120])
	f.IndexBytes = binary.LittleEndian.Uint64(b[120:128])
	f.Events = binary.LittleEndian.Uint64(b[128:136])
	if f.ID == 0 || f.Generation == 0 || f.DataBytes < segmentHeaderBytes || f.IndexOffset != f.DataBytes || f.IndexBytes < sealedIndexHeaderBytes || f.IndexBytes > maxSegmentIndexBytes || !allZero(b[136:segmentFooterBytes-4]) {
		return segmentFooter{}, fmt.Errorf("%w: footer state", ErrCorrupt)
	}
	return f, nil
}

func canonicalEventOrder(events []segmentEvent) []segmentEvent {
	ordered := slices.Clone(events)
	sort.SliceStable(ordered, func(i, j int) bool { return ordered[i].GroupID < ordered[j].GroupID })
	return ordered
}

func appendUvarint(dst []byte, value uint64) []byte {
	var buf [10]byte
	n := binary.PutUvarint(buf[:], value)
	return append(dst, buf[:n]...)
}

type canonicalCursor struct {
	data []byte
	off  int
}

func (c *canonicalCursor) byte() (byte, error) {
	if c.off >= len(c.data) {
		return 0, io.ErrUnexpectedEOF
	}
	v := c.data[c.off]
	c.off++
	return v, nil
}
func (c *canonicalCursor) take(n int) ([]byte, error) {
	if n < 0 || n > len(c.data)-c.off {
		return nil, io.ErrUnexpectedEOF
	}
	result := c.data[c.off : c.off+n]
	c.off += n
	return result, nil
}
func (c *canonicalCursor) uvarint() (uint64, error) {
	if c.off >= len(c.data) {
		return 0, io.ErrUnexpectedEOF
	}
	value, n := binary.Uvarint(c.data[c.off:])
	if n <= 0 {
		return 0, ErrCorrupt
	}
	var canonical [10]byte
	if binary.PutUvarint(canonical[:], value) != n {
		return 0, ErrCorrupt
	}
	c.off += n
	return value, nil
}
func (c *canonicalCursor) empty() bool { return c.off == len(c.data) }

func allZero(b []byte) bool {
	for _, v := range b {
		if v != 0 {
			return false
		}
	}
	return true
}
