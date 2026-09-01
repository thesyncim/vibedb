// Package seglog implements the version 3, node-wide segmented Raft log
// format. It is intentionally not wired into raftstore yet: the package is the
// recovery and indexing foundation for that cutover.
package seglog

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"slices"
	"sort"
)

const (
	FormatVersion        uint16 = 3
	ManifestName                = "MANIFEST.v3"
	RecordEntry          uint16 = 1
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

	manifestHeaderBytes     = 128
	manifestSegmentBytes    = 112
	manifestGroupBytes      = 48
	segmentHeaderBytes      = 128
	recordHeaderBytes       = 40
	segmentFooterBytes      = 160
	segmentIndexHeaderBytes = 40
	maxManifestBytes        = 64 << 20
	maxSegmentIndexBytes    = 64 << 20
	maxRecordBytes          = 64 << 20
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
	indexMagic    = [8]byte{'V', 'D', 'B', 'S', 'I', 'D', 'X', 0}
)

type SegmentMeta struct {
	ID, Generation, Bytes, Records uint64
	IndexOffset, IndexBytes        uint64
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
	AnchorID, AnchorGeneration             uint64
	AnchorHash                             [32]byte
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
	if err := validateManifestForMarshal(m); err != nil {
		return nil, err
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

func validateManifestForMarshal(m Manifest) error {
	anchorValid := m.AnchorID == 0 && m.AnchorGeneration == 0 && m.AnchorHash == ([32]byte{}) || m.AnchorID != 0 && m.AnchorGeneration != 0 && m.AnchorHash != ([32]byte{})
	if m.Generation == 0 || m.ActiveID == 0 || !anchorValid || m.DurableSegmentID != m.ActiveID || m.DurableOffset < segmentHeaderBytes || m.ActiveID != m.AnchorID+uint64(len(m.Segments))+1 || m.ActiveGeneration != m.AnchorGeneration+uint64(len(m.Segments))+1 || m.LogID == ([16]byte{}) {
		return fmt.Errorf("%w: invalid manifest state", ErrCorrupt)
	}
	lastID, lastGeneration, lastHash := m.AnchorID, m.AnchorGeneration, m.AnchorHash
	for _, s := range m.Segments {
		if s.ID != lastID+1 || s.Generation != lastGeneration+1 || !validSegmentGeometry(s) || s.PreviousHash != lastHash || s.Hash == ([32]byte{}) {
			return fmt.Errorf("%w: invalid segment metadata", ErrCorrupt)
		}
		lastID, lastGeneration, lastHash = s.ID, s.Generation, s.Hash
	}
	lastGroup := uint64(0)
	for _, g := range m.Groups {
		if g.GroupID == 0 || g.GroupID <= lastGroup || (g.TruncateIndex == 0) != (g.TruncateTerm == 0) || (g.DurableLastIndex == 0) != (g.DurableLastTerm == 0) || g.DurableLastIndex < g.TruncateIndex {
			return fmt.Errorf("%w: invalid group metadata", ErrCorrupt)
		}
		lastGroup = g.GroupID
	}
	return nil
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
	m.AnchorID = binary.LittleEndian.Uint64(b[80:88])
	m.AnchorGeneration = binary.LittleEndian.Uint64(b[88:96])
	copy(m.AnchorHash[:], b[96:128])
	off := manifestHeaderBytes
	last, lastGeneration, lastHash := m.AnchorID, m.AnchorGeneration, m.AnchorHash
	for range ns {
		s := SegmentMeta{ID: binary.LittleEndian.Uint64(b[off : off+8]), Generation: binary.LittleEndian.Uint64(b[off+8 : off+16]), Bytes: binary.LittleEndian.Uint64(b[off+16 : off+24]), Records: binary.LittleEndian.Uint64(b[off+24 : off+32]), IndexOffset: binary.LittleEndian.Uint64(b[off+32 : off+40]), IndexBytes: binary.LittleEndian.Uint64(b[off+40 : off+48])}
		copy(s.PreviousHash[:], b[off+48:off+80])
		copy(s.Hash[:], b[off+80:off+112])
		if s.ID == 0 || s.ID != last+1 || s.ID >= m.ActiveID || s.Generation != lastGeneration+1 || !validSegmentGeometry(s) || s.PreviousHash != lastHash || s.Hash == ([32]byte{}) {
			return Manifest{}, fmt.Errorf("%w: segment IDs not monotonic", ErrCorrupt)
		}
		last, lastGeneration, lastHash = s.ID, s.Generation, s.Hash
		m.Segments = append(m.Segments, s)
		off += manifestSegmentBytes
	}
	last = 0
	for range ng {
		g := GroupMeta{GroupID: binary.LittleEndian.Uint64(b[off : off+8]), TruncateIndex: binary.LittleEndian.Uint64(b[off+8 : off+16]), TruncateTerm: binary.LittleEndian.Uint64(b[off+16 : off+24]), DurableLastIndex: binary.LittleEndian.Uint64(b[off+24 : off+32]), DurableLastTerm: binary.LittleEndian.Uint64(b[off+32 : off+40])}
		if g.GroupID == 0 || g.GroupID <= last || (g.TruncateIndex == 0) != (g.TruncateTerm == 0) || (g.DurableLastIndex == 0) != (g.DurableLastTerm == 0) || g.DurableLastIndex < g.TruncateIndex || !allZero(b[off+40:off+manifestGroupBytes]) {
			return Manifest{}, fmt.Errorf("%w: group IDs not monotonic", ErrCorrupt)
		}
		last = g.GroupID
		m.Groups = append(m.Groups, g)
		off += manifestGroupBytes
	}
	anchorValid := m.AnchorID == 0 && m.AnchorGeneration == 0 && m.AnchorHash == ([32]byte{}) || m.AnchorID != 0 && m.AnchorGeneration != 0 && m.AnchorHash != ([32]byte{})
	if m.Generation == 0 || m.ActiveID == 0 || !anchorValid || m.ActiveGeneration != m.AnchorGeneration+uint64(ns)+1 || m.DurableSegmentID != m.ActiveID || m.DurableOffset < segmentHeaderBytes || m.ActiveID != m.AnchorID+uint64(ns)+1 || m.LogID == ([16]byte{}) {
		return Manifest{}, fmt.Errorf("%w: manifest state", ErrCorrupt)
	}
	return m, nil
}

func validSegmentGeometry(s SegmentMeta) bool {
	return s.Bytes >= segmentFooterBytes && s.IndexOffset >= segmentHeaderBytes && s.IndexBytes >= segmentIndexHeaderBytes+4 && s.IndexBytes <= maxSegmentIndexBytes && s.IndexOffset <= s.Bytes-segmentFooterBytes && s.IndexBytes == s.Bytes-segmentFooterBytes-s.IndexOffset
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

func recordSize(payload int) uint64 { return uint64(recordHeaderBytes + payload) }

func marshalRecord(r Record, dst []byte) ([]byte, error) {
	if r.GroupID == 0 || r.Index == 0 || (r.Kind != RecordEntry && r.Kind != RecordTruncateSuffix) || (r.Kind == RecordEntry && r.Term == 0) || (r.Kind == RecordTruncateSuffix && len(r.Payload) != 0) || len(r.Payload) > maxRecordBytes {
		return nil, ErrBounds
	}
	n := int(recordSize(len(r.Payload)))
	if cap(dst) < n {
		dst = make([]byte, n)
	} else {
		dst = dst[:n]
		clear(dst)
	}
	binary.LittleEndian.PutUint32(dst[0:4], uint32(n))
	binary.LittleEndian.PutUint16(dst[4:6], r.Kind)
	binary.LittleEndian.PutUint16(dst[6:8], r.Flags)
	binary.LittleEndian.PutUint64(dst[8:16], r.GroupID)
	binary.LittleEndian.PutUint64(dst[16:24], r.Index)
	binary.LittleEndian.PutUint64(dst[24:32], r.Term)
	binary.LittleEndian.PutUint32(dst[32:36], crc32.Checksum(r.Payload, crcTable))
	binary.LittleEndian.PutUint32(dst[36:40], crc32.Checksum(dst[:36], crcTable))
	copy(dst[40:], r.Payload)
	return dst, nil
}

func inspectRecord(b []byte) (Record, uint64, error) {
	if len(b) < recordHeaderBytes || binary.LittleEndian.Uint32(b[36:40]) != crc32.Checksum(b[:36], crcTable) {
		return Record{}, 0, fmt.Errorf("%w: record header", ErrCorrupt)
	}
	n := uint64(binary.LittleEndian.Uint32(b[0:4]))
	if n > maxRecordBytes+recordHeaderBytes || n < recordHeaderBytes {
		return Record{}, 0, fmt.Errorf("%w: record geometry", ErrCorrupt)
	}
	if uint64(len(b)) < n {
		return Record{}, n, errors.New("seglog: incomplete record")
	}
	r := Record{Kind: binary.LittleEndian.Uint16(b[4:6]), Flags: binary.LittleEndian.Uint16(b[6:8]), GroupID: binary.LittleEndian.Uint64(b[8:16]), Index: binary.LittleEndian.Uint64(b[16:24]), Term: binary.LittleEndian.Uint64(b[24:32]), Payload: b[40:n]}
	if r.GroupID == 0 || r.Index == 0 || (r.Kind != RecordEntry && r.Kind != RecordTruncateSuffix) || (r.Kind == RecordEntry && r.Term == 0) || (r.Kind == RecordTruncateSuffix && len(r.Payload) != 0) || crc32.Checksum(r.Payload, crcTable) != binary.LittleEndian.Uint32(b[32:36]) {
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
	if f.ID == 0 || f.Generation == 0 || f.DataBytes < segmentHeaderBytes || f.IndexOffset != f.DataBytes || f.IndexBytes < segmentIndexHeaderBytes+4 || f.IndexBytes > maxSegmentIndexBytes || !allZero(b[136:segmentFooterBytes-4]) {
		return segmentFooter{}, fmt.Errorf("%w: footer state", ErrCorrupt)
	}
	return f, nil
}

func segmentMetadataMAC(key [32]byte, header segmentHeader, index []byte, footer segmentFooter) [32]byte {
	if key == ([32]byte{}) {
		return [32]byte{}
	}
	footer.Auth = [32]byte{}
	encoded := marshalSegmentFooter(footer)
	mac := hmac.New(sha256.New, key[:])
	_, _ = mac.Write([]byte("vibedb/seglog-v3/sealed-metadata\x00"))
	encodedHeader := marshalSegmentHeader(header)
	_, _ = mac.Write(encodedHeader)
	_, _ = mac.Write(index)
	_, _ = mac.Write(encoded)
	var result [32]byte
	copy(result[:], mac.Sum(nil))
	return result
}

func marshalSegmentIndex(events []segmentEvent, dataBytes uint64) ([]byte, error) {
	ordered := canonicalEventOrder(events)
	b := make([]byte, segmentIndexHeaderBytes, segmentIndexHeaderBytes+len(events)*8+4)
	groups := uint64(0)
	previousGroup := uint64(0)
	for first := 0; first < len(ordered); {
		last := first + 1
		for last < len(ordered) && ordered[last].GroupID == ordered[first].GroupID {
			last++
		}
		group := ordered[first].GroupID
		if group == 0 || group <= previousGroup {
			return nil, ErrCorrupt
		}
		b = appendUvarint(b, group-previousGroup)
		b = appendUvarint(b, uint64(last-first))
		previousOffset := uint64(0)
		previousBlobBytes := uint64(0)
		for _, event := range ordered[first:last] {
			b = append(b, byte(event.Kind))
			switch event.Kind {
			case RecordEntry, eventWaveEntry, eventWaveEntryConf, eventWaveEntryConfV2:
				b = appendUvarint(b, event.Index)
				b = appendUvarint(b, event.Term)
				minimum := uint64(recordHeaderBytes)
				if event.Kind == eventWaveEntry || event.Kind == eventWaveEntryConf || event.Kind == eventWaveEntryConfV2 {
					minimum = 0
				}
				if event.Offset <= previousOffset || event.Bytes < minimum || event.Offset > dataBytes || event.Bytes > dataBytes-event.Offset {
					return nil, ErrCorrupt
				}
				b = appendUvarint(b, event.Offset-previousOffset)
				b = appendUvarint(b, event.Bytes)
				previousOffset = event.Offset
			case eventBlobEntry, eventBlobEntryConf, eventBlobEntryConfV2:
				b = appendUvarint(b, event.Index)
				b = appendUvarint(b, event.Term)
				if event.Offset < previousOffset || event.Offset > dataBytes || event.Bytes < 16 || event.Bytes > dataBytes-event.Offset {
					return nil, ErrCorrupt
				}
				offsetDelta := event.Offset - previousOffset
				b = appendUvarint(b, offsetDelta)
				if offsetDelta != 0 {
					b = appendUvarint(b, event.Bytes)
					previousBlobBytes = event.Bytes
				} else if event.Bytes != previousBlobBytes {
					return nil, ErrCorrupt
				}
				b = appendUvarint(b, event.DataOffset)
				b = appendUvarint(b, event.DataBytes)
				previousOffset = event.Offset
			case RecordTruncateSuffix, eventPrefix:
				b = appendUvarint(b, event.Index)
				b = appendUvarint(b, event.Term)
			case eventHardState:
				b = appendUvarint(b, event.Term)
				b = appendUvarint(b, event.Vote)
				b = appendUvarint(b, event.Commit)
			case eventCheckpoint:
				b = appendUvarint(b, event.Index)
				b = appendUvarint(b, event.Term)
				b = append(b, event.Reference[:]...)
			case eventWave:
				b = appendUvarint(b, event.Index)
				b = append(b, event.Reference[:]...)
				b = append(b, event.Digest[:]...)
			case eventReadyState:
				b = appendUvarint(b, event.Incarnation)
				b = appendUvarint(b, event.ReadyID)
				b = append(b, event.ReadyDigest[:]...)
				b = append(b, event.Reference[:]...)
			case eventIncarnation:
				b = appendUvarint(b, event.Incarnation)
			default:
				return nil, ErrCorrupt
			}
		}
		groups++
		previousGroup = group
		first = last
	}
	if uint64(len(b)+4) > maxSegmentIndexBytes || uint64(len(b)+4) > uint64(^uint32(0)) {
		return nil, ErrBounds
	}
	b = append(b, 0, 0, 0, 0)
	copy(b, indexMagic[:])
	binary.LittleEndian.PutUint16(b[8:10], FormatVersion)
	binary.LittleEndian.PutUint16(b[10:12], segmentIndexHeaderBytes)
	binary.LittleEndian.PutUint32(b[12:16], uint32(len(b)))
	binary.LittleEndian.PutUint64(b[16:24], uint64(len(events)))
	binary.LittleEndian.PutUint64(b[24:32], dataBytes)
	binary.LittleEndian.PutUint64(b[32:40], groups)
	putCRC(b)
	return b, nil
}

func canonicalEventOrder(events []segmentEvent) []segmentEvent {
	ordered := slices.Clone(events)
	sort.SliceStable(ordered, func(i, j int) bool { return ordered[i].GroupID < ordered[j].GroupID })
	return ordered
}

func unmarshalSegmentIndex(b []byte, dataBytes, eventCount uint64) ([]segmentEvent, error) {
	if len(b) < segmentIndexHeaderBytes+4 || !validCRC(b) || string(b[:8]) != string(indexMagic[:]) || binary.LittleEndian.Uint16(b[8:10]) != FormatVersion || binary.LittleEndian.Uint16(b[10:12]) != segmentIndexHeaderBytes || int(binary.LittleEndian.Uint32(b[12:16])) != len(b) || binary.LittleEndian.Uint64(b[16:24]) != eventCount || binary.LittleEndian.Uint64(b[24:32]) != dataBytes {
		return nil, fmt.Errorf("%w: segment index envelope", ErrCorrupt)
	}
	groups := binary.LittleEndian.Uint64(b[32:40])
	bodyBytes := uint64(len(b) - segmentIndexHeaderBytes - 4)
	if (groups == 0) != (eventCount == 0) || groups > eventCount || eventCount > bodyBytes/3 {
		return nil, fmt.Errorf("%w: segment index groups", ErrCorrupt)
	}
	result := make([]segmentEvent, 0, eventCount)
	cursor := canonicalCursor{data: b[segmentIndexHeaderBytes : len(b)-4]}
	previousGroup := uint64(0)
	for range groups {
		delta, err := cursor.uvarint()
		if err != nil || delta == 0 || previousGroup > ^uint64(0)-delta {
			return nil, fmt.Errorf("%w: group delta", ErrCorrupt)
		}
		group := previousGroup + delta
		count, err := cursor.uvarint()
		if err != nil || count == 0 || count > eventCount-uint64(len(result)) {
			return nil, fmt.Errorf("%w: group event count", ErrCorrupt)
		}
		previousOffset := uint64(0)
		previousBlobBytes := uint64(0)
		for range count {
			tag, err := cursor.byte()
			if err != nil || tag < byte(RecordEntry) || tag > byte(eventIncarnation) || tag == byte(RecordWave) {
				return nil, fmt.Errorf("%w: event kind", ErrCorrupt)
			}
			event := segmentEvent{Kind: uint16(tag), GroupID: group}
			switch event.Kind {
			case RecordEntry, eventWaveEntry, eventWaveEntryConf, eventWaveEntryConfV2:
				event.Index, err = cursor.uvarint()
				if err != nil || event.Index == 0 {
					return nil, fmt.Errorf("%w: event index", ErrCorrupt)
				}
				event.Term, err = cursor.uvarint()
				if err != nil || event.Term == 0 {
					return nil, fmt.Errorf("%w: event term", ErrCorrupt)
				}
				delta, err := cursor.uvarint()
				if err != nil || delta == 0 || previousOffset > ^uint64(0)-delta {
					return nil, fmt.Errorf("%w: event offset", ErrCorrupt)
				}
				event.Offset = previousOffset + delta
				event.Bytes, err = cursor.uvarint()
				minimum := uint64(recordHeaderBytes)
				if event.Kind == eventWaveEntry || event.Kind == eventWaveEntryConf || event.Kind == eventWaveEntryConfV2 {
					minimum = 0
				}
				if err != nil || event.Bytes < minimum || event.Offset < segmentHeaderBytes || event.Offset > dataBytes || event.Bytes > dataBytes-event.Offset {
					return nil, fmt.Errorf("%w: event geometry", ErrCorrupt)
				}
				previousOffset = event.Offset
			case eventBlobEntry, eventBlobEntryConf, eventBlobEntryConfV2:
				event.Index, err = cursor.uvarint()
				if err != nil || event.Index == 0 {
					return nil, ErrCorrupt
				}
				event.Term, err = cursor.uvarint()
				if err != nil || event.Term == 0 {
					return nil, ErrCorrupt
				}
				delta, readErr := cursor.uvarint()
				if readErr != nil || previousOffset > ^uint64(0)-delta {
					return nil, ErrCorrupt
				}
				event.Offset = previousOffset + delta
				if delta != 0 {
					event.Bytes, err = cursor.uvarint()
					previousBlobBytes = event.Bytes
				} else {
					event.Bytes = previousBlobBytes
				}
				if err != nil || event.Bytes < 16 || event.Offset < segmentHeaderBytes || event.Offset > dataBytes || event.Bytes > dataBytes-event.Offset {
					return nil, ErrCorrupt
				}
				event.DataOffset, err = cursor.uvarint()
				if err != nil {
					return nil, ErrCorrupt
				}
				event.DataBytes, err = cursor.uvarint()
				if err != nil || event.DataOffset > ^uint64(0)-event.DataBytes {
					return nil, ErrCorrupt
				}
				previousOffset = event.Offset
			case RecordTruncateSuffix, eventPrefix:
				event.Index, err = cursor.uvarint()
				if err != nil || event.Index == 0 {
					return nil, fmt.Errorf("%w: event index", ErrCorrupt)
				}
				event.Term, err = cursor.uvarint()
				if err != nil {
					return nil, fmt.Errorf("%w: event term", ErrCorrupt)
				}
			case eventHardState:
				event.Term, err = cursor.uvarint()
				if err != nil {
					return nil, ErrCorrupt
				}
				event.Vote, err = cursor.uvarint()
				if err != nil {
					return nil, ErrCorrupt
				}
				event.Commit, err = cursor.uvarint()
				if err != nil {
					return nil, ErrCorrupt
				}
			case eventCheckpoint:
				event.Index, err = cursor.uvarint()
				if err != nil || event.Index == 0 {
					return nil, ErrCorrupt
				}
				event.Term, err = cursor.uvarint()
				if err != nil || event.Term == 0 {
					return nil, ErrCorrupt
				}
				reference, err := cursor.take(16)
				if err != nil {
					return nil, err
				}
				copy(event.Reference[:], reference)
			case eventWave:
				event.Index, err = cursor.uvarint()
				if err != nil || event.Index == 0 {
					return nil, ErrCorrupt
				}
				reference, err := cursor.take(16)
				if err != nil {
					return nil, err
				}
				copy(event.Reference[:], reference)
				digest, err := cursor.take(32)
				if err != nil {
					return nil, err
				}
				copy(event.Digest[:], digest)
			case eventReadyState:
				event.Incarnation, err = cursor.uvarint()
				if err != nil || event.Incarnation == 0 {
					return nil, ErrCorrupt
				}
				event.ReadyID, err = cursor.uvarint()
				if err != nil || event.ReadyID == 0 {
					return nil, ErrCorrupt
				}
				digest, takeErr := cursor.take(16)
				if takeErr != nil {
					return nil, ErrCorrupt
				}
				copy(event.ReadyDigest[:], digest)
				if event.ReadyDigest == ([16]byte{}) {
					return nil, ErrCorrupt
				}
				waveID, takeErr := cursor.take(16)
				if takeErr != nil {
					return nil, ErrCorrupt
				}
				copy(event.Reference[:], waveID)
			case eventIncarnation:
				event.Incarnation, err = cursor.uvarint()
				if err != nil || event.Incarnation == 0 {
					return nil, ErrCorrupt
				}
			}
			result = append(result, event)
		}
		previousGroup = group
	}
	if len(result) != int(eventCount) || !cursor.empty() {
		return nil, fmt.Errorf("%w: segment index trailing data", ErrCorrupt)
	}
	return result, nil
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
