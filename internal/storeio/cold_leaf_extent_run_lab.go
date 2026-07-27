package storeio

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// Cold leaf extent runs are an isolated experiment for removing the 4 KiB
// allocation discontinuity from immutable ordered leaves without weakening
// the Store's 4 KiB root, direct-I/O, or crash-damage geometry.
//
// A run is written once, in full, at a 4 KiB-aligned physical offset. Leaves
// inside it are packed on eight-byte boundaries and independently checksummed
// by their leaf codec. The unified router's anchor leaf handle can therefore
// point directly at the absolute leaf byte range: a point read rounds that
// range out to the device's direct-I/O alignment and returns the inner slice.
// It does not read or descend through the run header.
//
// The production handle schema is one shared 18-byte value for hot and cold
// leaves: u48(offset>>3), u48 generation, u16(length-1), and a four-byte zone.
// It covers 1..65536 byte leaves and two PiB at eight-byte granularity. That is
// one byte/leaf above the previous 17-byte route estimate, while avoiding a
// packed-leaf-only reference shape or an extra point-read indirection.
//
// Packed leaves are cold and immutable. A mutation escapes the leaf to an
// ordinary 4 KiB-aligned hot extent; a later cold compaction packs it again.
// This is physical extent cleaning, not a logical read overlay or delta chain.
// It keeps point and lexical readers on one authoritative leaf.
//
// The complementary narrow-leaf experiment has a separate churn rule. Its
// 217 slots are a prefix of a 256-slot hot class: slots 0..191 retain the same
// 12 candidate groups and slots 192..255 become a larger exceptional area.
// Reclassifying upward before the narrow stash reaches eight entries therefore
// preserves BucketID, stable slots, and every posting bit; only the immutable
// leaf image and its unified anchor leaf handle change. Returning to the narrow
// class is optional cold work and is legal without posting repair only when
// slots 217..255 are empty. No narrow format is production-ready until the
// reclass transaction and its p99 are measured end to end.
const (
	ColdLeafExtentRunLabIOQuantum        = 4 << 10
	ColdLeafExtentRunLabMinimumLeafClass = 4 << 10
	ColdLeafExtentRunLabMaximumLeafBytes = 64 << 10
	ColdLeafExtentRunLabTargetLeaves     = 64
	ColdLeafExtentRunLabHeaderSize       = 32
	ColdLeafExtentRunLabRecordSize       = 8
	ColdLeafExtentRunLabTrailerSize      = 16
	ColdLeafExtentRunLabLeafAlignment    = 8

	coldLeafExtentRunLabMagic = "CLRUN001"
)

var (
	ErrColdLeafExtentRunLabCorrupt = errors.New(
		"vibejson: corrupt cold leaf extent run lab",
	)
	ErrColdLeafExtentRunLabFull = errors.New(
		"vibejson: cold leaf extent run lab is full",
	)
)

// ColdLeafExtentRunLabRecord is one independently checksummed leaf image.
// BucketID order is the lexical leaf order selected by the tablet router.
type ColdLeafExtentRunLabRecord struct {
	BucketID uint32
	Leaf     []byte
}

// ColdLeafExtentRunLabRef is exactly the routing information the unified anchor
// leaf handle needs. Offset is deliberately eight-byte granular. Length is the
// leaf's exact self-framed extent, rather than its outward-rounded direct-I/O
// window. BucketID remains expanded here only to make the isolated lab
// self-checking; the production 18-byte handle does not repeat it.
type ColdLeafExtentRunLabRef struct {
	Offset   uint64
	Length   uint32
	BucketID uint32
}

// ColdLeafExtentRunLabView is a fully validated, cleaner-only view. Point and
// lexical reads use their unified anchor leaf handle directly and never
// construct it.
type ColdLeafExtentRunLabView struct {
	run        []byte
	baseOffset uint64
	generation uint64
	usedEnd    uint32
	count      uint16
}

// ColdLeafExtentRunLabIterator walks a validated run without allocation.
type ColdLeafExtentRunLabIterator struct {
	run        []byte
	baseOffset uint64
	cursor     uint32
	remaining  uint16
}

// ColdLeafExtentRunLabClass returns the power-of-two size class for an exact
// self-framed leaf. Classes are planning hints only; records remain
// byte-packed and pay no per-leaf class or sector rounding.
func ColdLeafExtentRunLabClass(leafBytes int) int {
	if leafBytes <= 0 || leafBytes > ColdLeafExtentRunLabMaximumLeafBytes {
		return 0
	}
	class := ColdLeafExtentRunLabMinimumLeafClass
	for class < leafBytes {
		class <<= 1
	}
	return class
}

// ColdLeafExtentRunLabBytes reserves room for at least 64 class-sized leaves
// plus one 4 KiB bookkeeping/slack allowance. The result is always a complete
// direct-I/O quantum. Smaller leaves can fill the same run more densely.
func ColdLeafExtentRunLabBytes(leafBytes int) int {
	class := ColdLeafExtentRunLabClass(leafBytes)
	if class == 0 {
		return 0
	}
	return class*ColdLeafExtentRunLabTargetLeaves +
		ColdLeafExtentRunLabIOQuantum
}

// EncodeColdLeafExtentRunLab byte-packs as many ordered records as fit in dst.
// dst must be exactly the class run size chosen by the caller, baseOffset must
// be 4 KiB aligned, and refs must have room for every input. A short final run
// still writes its whole immutable extent; production compaction should merge
// adjacent cold candidates rather than publish a sparsely populated run.
//
// The record envelope is cleaner-only reverse metadata. A point reference
// starts after it, directly at the self-framed leaf.
func EncodeColdLeafExtentRunLab(
	dst []byte,
	baseOffset uint64,
	generation uint64,
	records []ColdLeafExtentRunLabRecord,
	refs []ColdLeafExtentRunLabRef,
) ([]ColdLeafExtentRunLabRef, error) {
	if len(dst) < ColdLeafExtentRunLabIOQuantum ||
		len(dst)%ColdLeafExtentRunLabIOQuantum != 0 ||
		baseOffset%ColdLeafExtentRunLabIOQuantum != 0 ||
		generation == 0 || len(records) == 0 || len(records) > int(^uint16(0)) ||
		len(refs) < len(records) {
		return nil, fmt.Errorf("%w: run identity or bounds", ErrInvalidWrite)
	}
	clear(dst)
	cursor := ColdLeafExtentRunLabHeaderSize
	trailer := len(dst) - ColdLeafExtentRunLabTrailerSize
	var previousBucket uint32
	for rank, record := range records {
		if record.BucketID >= 1<<30 || len(record.Leaf) == 0 ||
			len(record.Leaf) > ColdLeafExtentRunLabMaximumLeafBytes ||
			rank != 0 && record.BucketID <= previousBucket {
			return nil, fmt.Errorf("%w: leaf identity or order", ErrInvalidWrite)
		}
		leafStart := cursor + ColdLeafExtentRunLabRecordSize
		exactEnd := leafStart + len(record.Leaf)
		next := coldLeafExtentRunLabAlign(exactEnd)
		if next > trailer {
			return nil, ErrColdLeafExtentRunLabFull
		}
		binary.LittleEndian.PutUint32(dst[cursor:cursor+4], record.BucketID)
		binary.LittleEndian.PutUint32(
			dst[cursor+4:cursor+ColdLeafExtentRunLabRecordSize],
			uint32(len(record.Leaf)),
		)
		copy(dst[leafStart:], record.Leaf)
		refs[rank] = ColdLeafExtentRunLabRef{
			Offset:   baseOffset + uint64(leafStart),
			Length:   uint32(len(record.Leaf)),
			BucketID: record.BucketID,
		}
		cursor = next
		previousBucket = record.BucketID
	}

	copy(dst[:8], coldLeafExtentRunLabMagic)
	binary.LittleEndian.PutUint32(dst[8:12], DevelopmentFormatVersion)
	binary.LittleEndian.PutUint32(dst[12:16], uint32(len(dst)))
	binary.LittleEndian.PutUint64(dst[16:24], generation)
	binary.LittleEndian.PutUint32(dst[24:28], uint32(cursor))
	binary.LittleEndian.PutUint16(dst[28:30], uint16(len(records)))
	checksum := PageChecksum(dst[:trailer])
	binary.LittleEndian.PutUint32(dst[trailer:trailer+4], checksum)
	binary.LittleEndian.PutUint32(dst[trailer+4:trailer+8], ^checksum)
	headerChecksum := PageChecksum(dst[:ColdLeafExtentRunLabHeaderSize])
	binary.LittleEndian.PutUint32(dst[trailer+8:trailer+12], headerChecksum)
	binary.LittleEndian.PutUint32(dst[trailer+12:], ^headerChecksum)
	return refs[:len(records)], nil
}

// OpenColdLeafExtentRunLab verifies the immutable run and every cleaner
// envelope. Leaf-specific checksum and structure validation remains the leaf
// codec's responsibility.
func OpenColdLeafExtentRunLab(
	src []byte,
	baseOffset uint64,
) (ColdLeafExtentRunLabView, error) {
	if len(src) < ColdLeafExtentRunLabIOQuantum ||
		len(src)%ColdLeafExtentRunLabIOQuantum != 0 ||
		baseOffset%ColdLeafExtentRunLabIOQuantum != 0 {
		return ColdLeafExtentRunLabView{}, fmt.Errorf(
			"%w: run geometry", ErrColdLeafExtentRunLabCorrupt,
		)
	}
	trailer := len(src) - ColdLeafExtentRunLabTrailerSize
	checksum := binary.LittleEndian.Uint32(src[trailer : trailer+4])
	headerChecksum := binary.LittleEndian.Uint32(src[trailer+8 : trailer+12])
	if string(src[:8]) != coldLeafExtentRunLabMagic ||
		binary.LittleEndian.Uint32(src[8:12]) != DevelopmentFormatVersion ||
		binary.LittleEndian.Uint32(src[12:16]) != uint32(len(src)) ||
		binary.LittleEndian.Uint64(src[16:24]) == 0 ||
		binary.LittleEndian.Uint32(src[24:28]) < ColdLeafExtentRunLabHeaderSize ||
		int(binary.LittleEndian.Uint32(src[24:28])) > trailer ||
		binary.LittleEndian.Uint16(src[28:30]) == 0 ||
		!allZero(src[30:ColdLeafExtentRunLabHeaderSize]) ||
		binary.LittleEndian.Uint32(src[trailer+4:trailer+8]) != ^checksum ||
		binary.LittleEndian.Uint32(src[trailer+12:]) != ^headerChecksum ||
		PageChecksum(src[:trailer]) != checksum ||
		PageChecksum(src[:ColdLeafExtentRunLabHeaderSize]) != headerChecksum {
		return ColdLeafExtentRunLabView{}, fmt.Errorf(
			"%w: framing or checksum", ErrColdLeafExtentRunLabCorrupt,
		)
	}
	usedEnd := binary.LittleEndian.Uint32(src[24:28])
	count := binary.LittleEndian.Uint16(src[28:30])
	cursor := uint32(ColdLeafExtentRunLabHeaderSize)
	var previousBucket uint32
	for rank := uint16(0); rank < count; rank++ {
		if uint64(cursor)+ColdLeafExtentRunLabRecordSize > uint64(usedEnd) {
			return ColdLeafExtentRunLabView{}, fmt.Errorf(
				"%w: truncated reverse record", ErrColdLeafExtentRunLabCorrupt,
			)
		}
		bucketID := binary.LittleEndian.Uint32(src[cursor : cursor+4])
		length := binary.LittleEndian.Uint32(
			src[cursor+4 : cursor+ColdLeafExtentRunLabRecordSize],
		)
		exactEnd := uint64(cursor) + ColdLeafExtentRunLabRecordSize +
			uint64(length)
		next := uint64(coldLeafExtentRunLabAlign(int(exactEnd)))
		if bucketID >= 1<<30 || length == 0 ||
			length > ColdLeafExtentRunLabMaximumLeafBytes ||
			rank != 0 && bucketID <= previousBucket ||
			next > uint64(usedEnd) || !allZero(src[exactEnd:next]) {
			return ColdLeafExtentRunLabView{}, fmt.Errorf(
				"%w: reverse record", ErrColdLeafExtentRunLabCorrupt,
			)
		}
		cursor = uint32(next)
		previousBucket = bucketID
	}
	if cursor != usedEnd || !allZero(src[usedEnd:trailer]) {
		return ColdLeafExtentRunLabView{}, fmt.Errorf(
			"%w: used extent or padding", ErrColdLeafExtentRunLabCorrupt,
		)
	}
	return ColdLeafExtentRunLabView{
		run: src, baseOffset: baseOffset,
		generation: binary.LittleEndian.Uint64(src[16:24]),
		usedEnd:    usedEnd, count: count,
	}, nil
}

// Generation returns the immutable packing generation.
func (v ColdLeafExtentRunLabView) Generation() uint64 { return v.generation }

// Len returns the number of leaf images in the run.
func (v ColdLeafExtentRunLabView) Len() int { return int(v.count) }

// PhysicalBytes returns the complete durable run length.
func (v ColdLeafExtentRunLabView) PhysicalBytes() int { return len(v.run) }

// UsedBytes returns header, envelopes, and live leaf bytes, excluding the
// zero tail and fixed checksum trailer.
func (v ColdLeafExtentRunLabView) UsedBytes() int { return int(v.usedEnd) }

// Iterator returns a zero-allocation cleaner walk.
func (v ColdLeafExtentRunLabView) Iterator() ColdLeafExtentRunLabIterator {
	return ColdLeafExtentRunLabIterator{
		run: v.run, baseOffset: v.baseOffset,
		cursor: ColdLeafExtentRunLabHeaderSize, remaining: v.count,
	}
}

// Next returns one direct point reference and borrowed leaf image.
func (it *ColdLeafExtentRunLabIterator) Next() (
	ColdLeafExtentRunLabRef,
	[]byte,
	bool,
) {
	if it == nil || it.remaining == 0 {
		return ColdLeafExtentRunLabRef{}, nil, false
	}
	bucketID := binary.LittleEndian.Uint32(it.run[it.cursor : it.cursor+4])
	length := binary.LittleEndian.Uint32(
		it.run[it.cursor+4 : it.cursor+ColdLeafExtentRunLabRecordSize],
	)
	start := it.cursor + ColdLeafExtentRunLabRecordSize
	end := start + length
	ref := ColdLeafExtentRunLabRef{
		Offset: it.baseOffset + uint64(start),
		Length: length, BucketID: bucketID,
	}
	leaf := it.run[start:end:end]
	it.cursor = uint32(coldLeafExtentRunLabAlign(int(end)))
	it.remaining--
	return ref, leaf, true
}

// ResolveColdLeafExtentRunLab is the point/scan hot-path primitive. It does
// not consult or validate the run header; the unified anchor leaf handle
// selects the exact self-validating leaf bytes directly.
func ResolveColdLeafExtentRunLab(
	run []byte,
	baseOffset uint64,
	ref ColdLeafExtentRunLabRef,
) ([]byte, bool) {
	if ref.BucketID >= 1<<30 || ref.Length == 0 ||
		ref.Length > ColdLeafExtentRunLabMaximumLeafBytes ||
		ref.Offset < baseOffset ||
		ref.Offset&(ColdLeafExtentRunLabLeafAlignment-1) != 0 {
		return nil, false
	}
	start := ref.Offset - baseOffset
	end := start + uint64(ref.Length)
	if end < start || end > uint64(len(run)) {
		return nil, false
	}
	return run[start:end:end], true
}

// ColdLeafExtentRunLabReadWindow rounds one byte-packed leaf outward to the
// actual direct-I/O alignment. innerOffset identifies the exact leaf inside
// the returned window. The cache can retain the ordinary aligned window while
// typed readers borrow only the exact inner slice.
func ColdLeafExtentRunLabReadWindow(
	run []byte,
	baseOffset uint64,
	ref ColdLeafExtentRunLabRef,
	ioQuantum uint32,
) (window []byte, innerOffset uint32, ok bool) {
	if ioQuantum == 0 || ioQuantum&(ioQuantum-1) != 0 ||
		baseOffset%uint64(ioQuantum) != 0 {
		return nil, 0, false
	}
	leaf, ok := ResolveColdLeafExtentRunLab(run, baseOffset, ref)
	if !ok {
		return nil, 0, false
	}
	_ = leaf
	relative := ref.Offset - baseOffset
	windowStart := relative &^ uint64(ioQuantum-1)
	leafEnd := relative + uint64(ref.Length)
	windowEnd := (leafEnd + uint64(ioQuantum) - 1) &
		^uint64(ioQuantum-1)
	if windowEnd < leafEnd || windowEnd > uint64(len(run)) {
		return nil, 0, false
	}
	return run[windowStart:windowEnd:windowEnd],
		uint32(relative - windowStart), true
}

// EscapeColdLeafExtentRunLab copies one packed cold image into a dedicated
// hot extent. It models the only ordinary mutation transition: readers never
// consult a delta or retain the packed image as an overlay. dst is
// caller-owned staging memory and the returned checksum prevents the copy from
// being optimized out in benchmarks. The leaf codec performs the real update
// and sealing after this transition.
func EscapeColdLeafExtentRunLab(
	dst []byte,
	run []byte,
	baseOffset uint64,
	ref ColdLeafExtentRunLabRef,
) (hot []byte, checksum uint32, ok bool) {
	leaf, ok := ResolveColdLeafExtentRunLab(run, baseOffset, ref)
	if !ok {
		return nil, 0, false
	}
	length := (len(leaf) + ColdLeafExtentRunLabIOQuantum - 1) &
		^(ColdLeafExtentRunLabIOQuantum - 1)
	if len(dst) < length {
		return nil, 0, false
	}
	hot = dst[:length]
	clear(hot)
	copy(hot, leaf)
	return hot, PageChecksum(hot), true
}

func coldLeafExtentRunLabAlign(value int) int {
	return (value + ColdLeafExtentRunLabLeafAlignment - 1) &
		^(ColdLeafExtentRunLabLeafAlignment - 1)
}
