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
	canonicalFormatMarker uint16 = 1
	RecordTruncateSuffix  uint16 = 2
	RecordWave            uint16 = 3
	eventHardState        uint16 = 4
	eventPrefix           uint16 = 5
	eventCheckpoint       uint16 = 6
	eventWave             uint16 = 7
	eventWaveEntry        uint16 = 8
	eventWaveEntryConf    uint16 = 9
	eventWaveEntryConfV2  uint16 = 10
	eventReadyState       uint16 = 11
	eventBlobEntry        uint16 = 12
	eventBlobEntryConf    uint16 = 13
	eventBlobEntryConfV2  uint16 = 14
	eventIncarnation      uint16 = 15
	eventWaveRef          uint16 = 16

	segmentIdentityBytes = 128
	segmentHeaderBytes   = segmentIdentityBytes + reserveHeaderBytes
	recordHeaderBytes    = 40
	segmentFooterBytes   = 160
	maxSegmentIndexBytes = 64 << 20
	maxRecordBytes       = 64 << 20
)

var _ [40 - recordHeaderBytes]byte
var _ [recordHeaderBytes - 40]byte

var (
	ErrCorrupt  = errors.New("seglog: corrupt")
	ErrBounds   = errors.New("seglog: bounds exceeded")
	ErrPoisoned = errors.New("seglog: poisoned handle")
	crcTable    = crc32.MakeTable(crc32.Castagnoli)
	headerMagic = [8]byte{'V', 'D', 'B', 'S', 'E', 'G', 'H', 0}
	footerMagic = [8]byte{'V', 'D', 'B', 'S', 'E', 'G', 'F', 0}
)

type SegmentMeta struct {
	ID, Generation, Bytes, Records uint64
	IndexOffset, IndexBytes        uint64
	PreviousHash, Hash             [32]byte
	FileID                         fileID
	State                          SegmentState
}

type SegmentState uint8

const (
	SegmentSealed SegmentState = iota + 1
	SegmentFrozenPending
)

type metadataState struct {
	Generation, ActiveID, ActiveGeneration uint64
	DurableSegmentID, DurableOffset        uint64
	LogID                                  [16]byte
	ActiveFileID                           fileID
	SegmentCapacity                        uint64
	AnchorID, AnchorGeneration             uint64
	AnchorHash                             [32]byte
	Reserves                               [2]reserveDescriptor
	Segments                               []SegmentMeta
}

type segmentHeader struct {
	ID, Generation, PreviousID uint64
	PreviousHash               [32]byte
	LogID                      [16]byte
	FileID                     fileID
	Capacity                   uint64
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

func validSegmentGeometry(s SegmentMeta) bool {
	return s.State == SegmentSealed && s.Bytes >= segmentFooterBytes && s.IndexOffset >= segmentHeaderBytes && s.IndexBytes >= sealedIndexHeaderBytes && s.IndexBytes <= maxSegmentIndexBytes && s.IndexOffset <= s.Bytes-segmentFooterBytes && s.IndexBytes == s.Bytes-segmentFooterBytes-s.IndexOffset
}

func pendingSegment(s SegmentMeta) bool {
	return s.State == SegmentFrozenPending && s.Bytes >= segmentHeaderBytes && s.IndexOffset == 0 && s.IndexBytes == 0 && s.Hash != ([32]byte{})
}

func marshalSegmentHeader(h segmentHeader) []byte {
	b := make([]byte, segmentIdentityBytes)
	copy(b, headerMagic[:])
	binary.LittleEndian.PutUint16(b[8:10], canonicalFormatMarker)
	binary.LittleEndian.PutUint16(b[10:12], segmentIdentityBytes)
	binary.LittleEndian.PutUint64(b[16:24], h.ID)
	binary.LittleEndian.PutUint64(b[24:32], h.Generation)
	binary.LittleEndian.PutUint64(b[32:40], h.PreviousID)
	copy(b[40:72], h.PreviousHash[:])
	copy(b[72:88], h.LogID[:])
	copy(b[88:104], h.FileID[:])
	binary.LittleEndian.PutUint64(b[104:112], h.Capacity)
	putCRC(b)
	return b
}

func unmarshalSegmentHeader(b []byte) (segmentHeader, error) {
	if len(b) < segmentIdentityBytes {
		return segmentHeader{}, fmt.Errorf("%w: segment header", ErrCorrupt)
	}
	b = b[:segmentIdentityBytes]
	if !validCRC(b) || string(b[:8]) != string(headerMagic[:]) || binary.LittleEndian.Uint16(b[8:10]) != canonicalFormatMarker || binary.LittleEndian.Uint16(b[10:12]) != segmentIdentityBytes {
		return segmentHeader{}, fmt.Errorf("%w: segment header", ErrCorrupt)
	}
	h := segmentHeader{ID: binary.LittleEndian.Uint64(b[16:24]), Generation: binary.LittleEndian.Uint64(b[24:32]), PreviousID: binary.LittleEndian.Uint64(b[32:40])}
	copy(h.PreviousHash[:], b[40:72])
	copy(h.LogID[:], b[72:88])
	copy(h.FileID[:], b[88:104])
	h.Capacity = binary.LittleEndian.Uint64(b[104:112])
	if h.ID == 0 || h.Generation == 0 || (h.ID == 1) != (h.PreviousID == 0) || h.PreviousID+1 != h.ID || h.LogID == ([16]byte{}) || h.FileID == (fileID{}) || h.Capacity < segmentHeaderBytes || h.Capacity >= 1<<32 || !allZero(b[112:segmentIdentityBytes-4]) {
		return segmentHeader{}, fmt.Errorf("%w: segment header identity", ErrCorrupt)
	}
	return h, nil
}

func marshalSegmentFooter(f segmentFooter) []byte {
	b := make([]byte, segmentFooterBytes)
	copy(b, footerMagic[:])
	binary.LittleEndian.PutUint16(b[8:10], canonicalFormatMarker)
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
	if len(b) != segmentFooterBytes || !validCRC(b) || string(b[:8]) != string(footerMagic[:]) || binary.LittleEndian.Uint16(b[8:10]) != canonicalFormatMarker || binary.LittleEndian.Uint16(b[10:12]) != segmentFooterBytes {
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
