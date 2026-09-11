package storeio

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"slices"

	"github.com/pierrec/lz4/v4"
)

const (
	PrimaryExactPackMaxDecodedBytes = 64 << 10
	PrimaryExactPackHeaderBytes     = 32
	PrimaryExactPackMemberBytes     = 16
	PrimaryExactPackEnvelopeBytes   = PageHeaderSize + PageTrailerSize
	PrimaryExactPackMaxPayloadBytes = PrimaryExactPackMaxDecodedBytes - PrimaryExactPackEnvelopeBytes
	PrimaryExactPackMaxMembers      = (PrimaryExactPackMaxPayloadBytes - PrimaryExactPackHeaderBytes) / (PrimaryExactPackMemberBytes + 1)
	primaryExactPackMagic           = "VXP1"
	primaryExactPackVersion         = 1
	primaryExactPackRaw             = 0
	primaryExactPackLZ4             = 1
)

var (
	ErrPrimaryExactPackBounds  = errors.New("storeio: primary exact pack bounds")
	ErrPrimaryExactPackCorrupt = errors.New("storeio: corrupt primary exact pack")
	ErrPrimaryExactPackIndex   = errors.New("storeio: primary exact pack index mismatch")
	primaryExactPackCRCTable   = crc32.MakeTable(crc32.Castagnoli)
)

type PrimaryExactPackMember struct {
	IndexID uint32
	Leaf    []byte
}
type primaryExactPackMemberMeta struct{ indexID, offset, length uint32 }

// PrimaryExactPackEncoder builds a page-enveloped pack bounded to 64 KiB.
// The header remains plain; the directory and opaque canonical leaves form one
// raw or LZ4 body. Prepared member and leaf capacities are independent.
type PrimaryExactPackEncoder struct {
	members                   []primaryExactPackMemberMeta
	leaves, frame, compressed []byte
	compressor                lz4.Compressor
	memberCap, leafCap        int
}

func (e *PrimaryExactPackEncoder) Prepare(memberCapacity, leafCapacity int) error {
	maxLeaf := PrimaryExactPackMaxPayloadBytes - PrimaryExactPackHeaderBytes - PrimaryExactPackMemberBytes
	if e == nil || memberCapacity < 1 || memberCapacity > PrimaryExactPackMaxMembers || leafCapacity < 1 || leafCapacity > maxLeaf {
		return ErrPrimaryExactPackBounds
	}
	e.members = slices.Grow(e.members[:0], memberCapacity)
	e.leaves = slices.Grow(e.leaves[:0], leafCapacity)
	maxBody := PrimaryExactPackMaxPayloadBytes - PrimaryExactPackHeaderBytes
	e.frame = slices.Grow(e.frame[:0], maxBody)
	e.compressed = slices.Grow(e.compressed[:0], lz4.CompressBlockBound(maxBody))
	e.memberCap, e.leafCap = memberCapacity, leafCapacity
	return nil
}

func (e *PrimaryExactPackEncoder) Reset() {
	if e != nil {
		e.members = e.members[:0]
		e.leaves = e.leaves[:0]
	}
}

func (e *PrimaryExactPackEncoder) Append(indexID uint32, leaf []byte) error {
	if e == nil {
		return ErrPrimaryExactPackBounds
	}
	used := PrimaryExactPackEnvelopeBytes + PrimaryExactPackHeaderBytes + (len(e.members)+1)*PrimaryExactPackMemberBytes + len(e.leaves) + len(leaf)
	if len(leaf) == 0 || len(e.members) >= e.memberCap || len(leaf) > e.leafCap-len(e.leaves) || used > PrimaryExactPackMaxDecodedBytes {
		return ErrPrimaryExactPackBounds
	}
	offset := len(e.leaves)
	e.leaves = append(e.leaves, leaf...)
	e.members = append(e.members, primaryExactPackMemberMeta{indexID, uint32(offset), uint32(len(leaf))})
	return nil
}

func (e *PrimaryExactPackEncoder) AppendMember(m PrimaryExactPackMember) error {
	return e.Append(m.IndexID, m.Leaf)
}

// Encode appends a complete payload. Admission uses the configured physical
// quantum and common page envelope, and preserves raw when extents tie. A
// durable caller with a configured maximum below 64 KiB must impose that lower
// append budget before Encode.
func (e *PrimaryExactPackEncoder) Encode(dst []byte, physicalQuantum int) ([]byte, error) {
	return e.encode(dst, physicalQuantum, true)
}

// EncodeRaw appends the canonical raw representation without running LZ4.
// It is the first durable integration stage and shares all framing and bounds
// with Encode.
func (e *PrimaryExactPackEncoder) EncodeRaw(dst []byte, physicalQuantum int) ([]byte, error) {
	return e.encode(dst, physicalQuantum, false)
}

func validPrimaryExactPackFraming(pack []byte) bool {
	if len(pack) < PrimaryExactPackHeaderBytes || string(pack[:4]) != primaryExactPackMagic ||
		pack[5] != primaryExactPackVersion || pack[4] > primaryExactPackLZ4 {
		return false
	}
	for _, v := range pack[28:32] {
		if v != 0 {
			return false
		}
	}
	count := int(binary.LittleEndian.Uint16(pack[6:]))
	decoded := int(binary.LittleEndian.Uint32(pack[8:]))
	stored := int(binary.LittleEndian.Uint32(pack[12:]))
	dirBytes := int(binary.LittleEndian.Uint32(pack[16:]))
	total := int(binary.LittleEndian.Uint32(pack[20:]))
	if count < 1 || count > PrimaryExactPackMaxMembers ||
		dirBytes != count*PrimaryExactPackMemberBytes || dirBytes >= decoded ||
		PrimaryExactPackEnvelopeBytes+PrimaryExactPackHeaderBytes+decoded > PrimaryExactPackMaxDecodedBytes ||
		stored < 1 || total != len(pack) ||
		uint64(PrimaryExactPackHeaderBytes)+uint64(stored) != uint64(total) {
		return false
	}
	if pack[4] == primaryExactPackRaw {
		return stored == decoded
	}
	return stored < decoded
}

func (e *PrimaryExactPackEncoder) encode(dst []byte, physicalQuantum int, allowLZ4 bool) ([]byte, error) {
	if e == nil || len(e.members) == 0 || physicalQuantum < 1 || physicalQuantum > PrimaryExactPackMaxDecodedBytes || physicalQuantum&(physicalQuantum-1) != 0 {
		return dst, ErrPrimaryExactPackBounds
	}
	dirBytes := len(e.members) * PrimaryExactPackMemberBytes
	decodedBytes := dirBytes + len(e.leaves)
	if PrimaryExactPackEnvelopeBytes+PrimaryExactPackHeaderBytes+decodedBytes > PrimaryExactPackMaxDecodedBytes || cap(e.frame) < decodedBytes {
		return dst, ErrPrimaryExactPackBounds
	}
	e.frame = e.frame[:decodedBytes]
	clear(e.frame[:dirBytes])
	for i, m := range e.members {
		x := e.frame[i*PrimaryExactPackMemberBytes:]
		binary.LittleEndian.PutUint32(x, m.indexID)
		binary.LittleEndian.PutUint32(x[4:], m.offset)
		binary.LittleEndian.PutUint32(x[8:], m.length)
	}
	copy(e.frame[dirBytes:], e.leaves)
	codec := byte(primaryExactPackRaw)
	body := e.frame
	if allowLZ4 {
		bound := lz4.CompressBlockBound(decodedBytes)
		if cap(e.compressed) < bound {
			return dst, ErrPrimaryExactPackBounds
		}
		e.compressed = e.compressed[:bound]
		n, err := e.compressor.CompressBlock(e.frame, e.compressed)
		if err != nil {
			return dst, ErrPrimaryExactPackBounds
		}
		if n > 0 {
			raw, rawOK := primaryExactPackRoundUp(PrimaryExactPackEnvelopeBytes+PrimaryExactPackHeaderBytes+decodedBytes, physicalQuantum)
			compressed, compressedOK := primaryExactPackRoundUp(PrimaryExactPackEnvelopeBytes+PrimaryExactPackHeaderBytes+n, physicalQuantum)
			if !rawOK || !compressedOK {
				return dst, ErrPrimaryExactPackBounds
			}
			if compressed < raw {
				codec, body = primaryExactPackLZ4, e.compressed[:n]
			}
		}
	}
	total := PrimaryExactPackHeaderBytes + len(body)
	if cap(dst)-len(dst) < total {
		return dst, ErrPrimaryExactPackBounds
	}
	start := len(dst)
	dst = dst[:start+total]
	pack := dst[start:]
	clear(pack[:PrimaryExactPackHeaderBytes])
	copy(pack, primaryExactPackMagic)
	pack[4], pack[5] = codec, primaryExactPackVersion
	binary.LittleEndian.PutUint16(pack[6:], uint16(len(e.members)))
	binary.LittleEndian.PutUint32(pack[8:], uint32(decodedBytes))
	binary.LittleEndian.PutUint32(pack[12:], uint32(len(body)))
	binary.LittleEndian.PutUint32(pack[16:], uint32(dirBytes))
	binary.LittleEndian.PutUint32(pack[20:], uint32(total))
	binary.LittleEndian.PutUint32(pack[24:], crc32.Checksum(e.frame, primaryExactPackCRCTable))
	copy(pack[PrimaryExactPackHeaderBytes:], body)
	return dst, nil
}

func primaryExactPackRoundUp(n, q int) (int, bool) {
	if n < 0 || q < 1 || q&(q-1) != 0 || n > int(^uint(0)>>1)-(q-1) {
		return 0, false
	}
	return (n + q - 1) &^ (q - 1), true
}

// PrimaryExactPackDecoder opens a raw body by borrowing src or decodes an LZ4
// body once into its prepared workspace. Raw member views require src to remain
// immutable and alive. Every member view is invalidated by Reset, Prepare, or
// the next Open call, including an Open that returns an error.
type PrimaryExactPackDecoder struct {
	directory, leaves, workspace []byte
	count                        int
	codec                        byte
}

func (d *PrimaryExactPackDecoder) Prepare(decodedBodyCapacity int) error {
	max := PrimaryExactPackMaxPayloadBytes - PrimaryExactPackHeaderBytes
	if d == nil || decodedBodyCapacity < PrimaryExactPackMemberBytes+1 || decodedBodyCapacity > max {
		return ErrPrimaryExactPackBounds
	}
	d.workspace = slices.Grow(d.workspace[:0], decodedBodyCapacity)
	d.Reset()
	return nil
}

func (d *PrimaryExactPackDecoder) Reset() {
	if d != nil {
		d.directory = nil
		d.leaves = nil
		d.count = 0
		d.codec = 0
	}
}

func (d *PrimaryExactPackDecoder) Open(src []byte) error {
	if d == nil {
		return ErrPrimaryExactPackBounds
	}
	d.Reset()
	bad := func() error { return ErrPrimaryExactPackCorrupt }
	if len(src) < PrimaryExactPackHeaderBytes || string(src[:4]) != primaryExactPackMagic || src[5] != primaryExactPackVersion || src[4] > primaryExactPackLZ4 {
		return bad()
	}
	for _, v := range src[28:32] {
		if v != 0 {
			return bad()
		}
	}
	count := int(binary.LittleEndian.Uint16(src[6:]))
	decoded := int(binary.LittleEndian.Uint32(src[8:]))
	stored := int(binary.LittleEndian.Uint32(src[12:]))
	dirBytes := int(binary.LittleEndian.Uint32(src[16:]))
	total := int(binary.LittleEndian.Uint32(src[20:]))
	if count < 1 || count > PrimaryExactPackMaxMembers || dirBytes != count*PrimaryExactPackMemberBytes || dirBytes >= decoded || PrimaryExactPackEnvelopeBytes+PrimaryExactPackHeaderBytes+decoded > PrimaryExactPackMaxDecodedBytes || stored < 1 || total != len(src) || uint64(PrimaryExactPackHeaderBytes)+uint64(stored) != uint64(total) {
		return bad()
	}
	body, frame := src[PrimaryExactPackHeaderBytes:], src[PrimaryExactPackHeaderBytes:]
	if src[4] == primaryExactPackRaw {
		if stored != decoded {
			return bad()
		}
	} else {
		if stored >= decoded || cap(d.workspace) < decoded {
			return bad()
		}
		d.workspace = d.workspace[:decoded]
		n, err := lz4.UncompressBlock(body, d.workspace)
		if err != nil || n != decoded {
			return bad()
		}
		frame = d.workspace
	}
	if crc32.Checksum(frame, primaryExactPackCRCTable) != binary.LittleEndian.Uint32(src[24:]) {
		return bad()
	}
	dir := frame[:dirBytes]
	previous, leafBytes := uint32(0), decoded-dirBytes
	for i := 0; i < count; i++ {
		x := dir[i*PrimaryExactPackMemberBytes:]
		offset, length := binary.LittleEndian.Uint32(x[4:]), binary.LittleEndian.Uint32(x[8:])
		if offset != previous || length == 0 || uint64(offset)+uint64(length) > uint64(leafBytes) || binary.LittleEndian.Uint32(x[12:]) != 0 {
			return bad()
		}
		previous = offset + length
	}
	if previous != uint32(leafBytes) {
		return bad()
	}
	d.directory, d.leaves, d.count, d.codec = dir, frame[dirBytes:], count, src[4]
	return nil
}

func (d *PrimaryExactPackDecoder) MemberCount() int {
	if d == nil {
		return 0
	}
	return d.count
}
func (d *PrimaryExactPackDecoder) Member(ordinal int, expectedIndexID uint32) ([]byte, error) {
	if d == nil || ordinal < 0 || ordinal >= d.count {
		return nil, ErrPrimaryExactPackBounds
	}
	x := d.directory[ordinal*PrimaryExactPackMemberBytes:]
	if binary.LittleEndian.Uint32(x) != expectedIndexID {
		return nil, ErrPrimaryExactPackIndex
	}
	offset, length := int(binary.LittleEndian.Uint32(x[4:])), int(binary.LittleEndian.Uint32(x[8:]))
	return d.leaves[offset : offset+length : offset+length], nil
}
func (d *PrimaryExactPackDecoder) Codec() string {
	if d == nil || d.count == 0 {
		return ""
	}
	if d.codec == primaryExactPackLZ4 {
		return "lz4"
	}
	return "raw"
}
