package store

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"unsafe"

	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/document"
	"github.com/thesyncim/vibejson/x/byteview"
)

// Log-structured Segment persistence: a versioned, mmap-friendly serialization
// of a segment's core so a corpus reopens with zero re-parse.
//
// A built Segment is source arenas plus a structural tape per document (index.go),
// with the shape-deduplicated storage (segment_shape.go) folding the recurring
// keys into a shared shape table. WriteTo lays that state down as a byte image
// and Open reconstructs a Segment whose arenas view straight back into it, so a
// process that has indexed a corpus once can memory-map the image and answer
// Doc(i) at full speed without re-validating a byte. The format is EXPLICITLY
// UNSTABLE before v1: the header and every generation carry the format version
// and Open rejects a version or magic it does not recognize rather than
// misreading it.
//
// # Layout
//
// The image is a header, an append-only log of self-describing document
// records, a shared shape table, and a manifest that indexes them, closed by a
// fixed footer that locates the manifest from the end:
//
//	  offset 0
//	+----------------------------------------------------------------+
//	| header      magic "SJDOCSET", format version                    |
//	+----------------------------------------------------------------+
//	| doc record 0   [hdr | source bytes | tape entries]  (8-aligned) |
//	| doc record 1   [hdr | source bytes | tape entries]              |
//	|   ...            << the write path appends new versions here >>  |
//	| doc record N-1                                                  |
//	+----------------------------------------------------------------+
//	| shape table    the deduplicated key spellings, once per shape   |
//	+----------------------------------------------------------------+
//	| manifest       magic, flags/options, shape-table span, and the  |
//	|                offsets index: the absolute offset of every live  |
//	|                document record — this is the snapshot            |
//	+----------------------------------------------------------------+
//	| footer         magic, manifest span, manifest checksum, version |
//	+----------------------------------------------------------------+
//	  end of image
//
// A document record is self-describing — its header carries the source length,
// the entry count and width, and (for a shape-taped document) the shape id and
// root span — so the record is the unit the log appends and the manifest merely
// points at. The manifest is the snapshot: it lists the offset of every
// document record the snapshot sees plus the shape table those records resolve
// against. The footer is fixed-size and read from the end, so a reader locates
// the latest manifest in one seek.
//
// # Alignment to the write path (ADR 0002)
//
// This shape is deliberately the substrate the planned MVCC-snapshot write path
// drops into. Mutation there is an append: a writer lays a new document version
// down as a fresh record, appends any new shapes, and publishes a new snapshot
// by appending a new manifest generation and footer at the tail — never
// rewriting a live byte, exactly the never-move arena discipline the in-memory
// segment already keeps (segment.go). A manifest is an immutable snapshot of
// pointers into immutable records, so an update reuses every unchanged record
// and only the changed document costs new bytes; older manifests remain in the
// image as the older snapshots an MVCC reader time-travels to, and the newest
// footer names the current one. Reads mmap a manifest and resolve handles
// straight into its records, blocking never and paying no indirection. This
// first cut writes one generation; the format needs no change to grow more.
//
// # Zero-copy and portability
//
// All integers are little-endian. On the overwhelmingly common little-endian
// host the on-disk words already match the in-memory representation, so a
// document's source bytes and its 16-byte entry tape (or 16-byte shape-taped
// value array) are handed to the reconstructed Index as sub-slices that view
// straight into the mapped image — no copy, no parse. Two sections are copied
// rather than viewed, and the reasons are recorded honestly:
//
//   - The narrow (8-byte) shape-taped value arrays are consolidated into the
//     segment's single Segment.narrow slab, which is addressed by a per-document
//     offset; because each record carries its own values, they cannot all be
//     one contiguous view and are copied into the slab on Open.
//   - The shape table is small (keys live once per shape) and is rebuilt into a
//     fresh, fully functional ShapeCache so the reopened segment resolves and
//     continues to Append; its bytes are copied there.
//
// On a big-endian host, or when a section's mapped address is not 4-byte
// aligned for an entry view, Open decodes the words individually instead of
// aliasing them; the result is byte-identical, only slower. WriteTo emits the
// canonical little-endian form on every host.
//
// # What is serialized, what is rebuilt
//
// The core is serialized: every document's source and tape (classic, wide
// shape-taped, and narrow shape-taped), the shape table, and the interned key
// spellings the shapes carry. The two opt-in accelerators layered over the core
// — the inverted postings (segment_postings.go) and the value dictionary
// (segment_valuedict.go) — are rebuilt on Open from the reconstructed documents
// when their flags were set, deterministically reproducing the original
// structures; a first cut is free to rebuild them because they never change
// what a read returns, only its cost and at-rest space (both are pure functions
// of the committed documents).
//
// # Lifetime
//
// A segment returned by Open borrows the image: its document sources and entry
// tapes view into the bytes passed to Open, which the segment pins (Segment.source)
// so a Go-owned image stays alive for the segment's lifetime. When the image is a
// memory map the caller owns the mapping — every borrowed view is valid only
// while it stays mapped, exactly the contract a borrowed arena keeps.
//
// # Bounds
//
// One document's source and entry storage stay within the index's 32-bit
// coordinate space (index.go), so a record is bounded like any built document.
// Open requires the whole image addressable as one byte slice; a memory map
// satisfies this lazily, the OS paging in only the records a reader touches, so
// a corpus larger than RAM reopens without an eager read. WriteTo streams to an
// io.Writer, buffering only the O(documents) manifest, so writing never holds a
// second copy of the corpus.

// The format identifiers and fixed sizes. The version is bumped on any layout
// change; Open rejects a mismatch (the format is unstable before v1).
const (
	persistVersion = 1

	// A collection writes one bounded Segment image per at-most-64-row micro-page.
	// Keep
	// that manifest and its offsets in fixed scratch so persistence allocation
	// is page-granular rather than row-granular.
	persistSmallManifestDocuments = 64

	persistHeaderMagic   = "SJDOCSET"
	persistManifestMagic = "SJDSMANI"
	persistFooterMagic   = "SJDSFOOT"

	persistRecordHeaderLen = 24 // see the record header layout in writeDocRecord
	segmentManifestFixed   = 56 // manifest bytes before the offsets index
)

// Both serialized images in this package — the segment image written by
// [Segment.WriteTo] and the collection checkpoint written by
// [Collection.WriteTo] — carry the same fixed envelope around a
// self-describing manifest. Only the envelope is shared: the two formats keep
// independent magics, version lineages, and error taxonomies, because a
// checkpoint's payload region is a concatenation of segment images and the two
// must be able to move apart.
const (
	// ImageHeaderLen is magic(8) + version(4) + reserved(4).
	ImageHeaderLen = 16
	// ImageFooterLen is magic(8) + manifestOffset(8) + manifestLength(8) +
	// checksum(8) + version(4) + reserved(4).
	ImageFooterLen = 40
)

// A persistFrame is the envelope codec for one of the two image formats. It
// owns the header and footer bytes and the fail-closed validation that
// recovers a manifest from an untrusted image; everything inside the manifest
// belongs to the format that declared the frame.
type persistFrame struct {
	headerMagic   string
	manifestMagic string
	footerMagic   string
	version       uint32
	manifestFixed uint64
	errMagic      error
	errVersion    error
	errCorrupt    error
}

// writeHeader lays down the fixed image header.
func (f persistFrame) writeHeader(pw *persistWriter) {
	var header [ImageHeaderLen]byte
	copy(header[0:8], f.headerMagic)
	binary.LittleEndian.PutUint32(header[8:12], f.version)
	pw.writeSmall(header[:])
}

// writeFooter seals the image: it records where the manifest starts, how long
// it is, and its checksum, so a reader can find and verify the manifest
// without scanning the payload.
func (f persistFrame) writeFooter(pw *persistWriter, manifestOffset uint64, manifest []byte) {
	var footer [ImageFooterLen]byte
	copy(footer[0:8], f.footerMagic)
	binary.LittleEndian.PutUint64(footer[8:16], manifestOffset)
	binary.LittleEndian.PutUint64(footer[16:24], uint64(len(manifest)))
	binary.LittleEndian.PutUint64(footer[24:32], ManifestChecksum(manifest))
	binary.LittleEndian.PutUint32(footer[32:36], f.version)
	pw.writeSmall(footer[:])
}

// openManifest validates the envelope of an untrusted image and returns the
// manifest it frames, along with the manifest's offset. Every check is
// fail-closed and ordered cheapest-first: length, header magic and version,
// reserved bytes, footer magic and version, then the manifest span against
// real bytes before any of those bytes are read, then the checksum, and only
// then the manifest's own magic and version. The returned slice aliases data.
func (f persistFrame) openManifest(data []byte) ([]byte, uint64, error) {
	if uint64(len(data)) < ImageHeaderLen+ImageFooterLen {
		return nil, 0, fmt.Errorf("%w: image shorter than its framing", f.errCorrupt)
	}
	if string(data[0:8]) != f.headerMagic {
		return nil, 0, fmt.Errorf("%w: header magic", f.errMagic)
	}
	if v := binary.LittleEndian.Uint32(data[8:12]); v != f.version {
		return nil, 0, fmt.Errorf("%w: header version %d != %d", f.errVersion, v, f.version)
	}
	if binary.LittleEndian.Uint32(data[12:16]) != 0 {
		return nil, 0, fmt.Errorf("%w: header reserved field", f.errCorrupt)
	}
	footer := data[uint64(len(data))-ImageFooterLen:]
	if string(footer[0:8]) != f.footerMagic {
		return nil, 0, fmt.Errorf("%w: footer magic", f.errMagic)
	}
	if v := binary.LittleEndian.Uint32(footer[32:36]); v != f.version {
		return nil, 0, fmt.Errorf("%w: footer version %d != %d", f.errVersion, v, f.version)
	}
	if binary.LittleEndian.Uint32(footer[36:40]) != 0 {
		return nil, 0, fmt.Errorf("%w: footer reserved field", f.errCorrupt)
	}
	offset := binary.LittleEndian.Uint64(footer[8:16])
	length := binary.LittleEndian.Uint64(footer[16:24])
	checksum := binary.LittleEndian.Uint64(footer[24:32])
	limit := uint64(len(data)) - ImageFooterLen
	if offset < ImageHeaderLen || length < f.manifestFixed ||
		offset > limit || length > limit-offset {
		return nil, 0, fmt.Errorf("%w: manifest span out of range", f.errCorrupt)
	}
	manifest := data[offset : offset+length]
	if ManifestChecksum(manifest) != checksum {
		return nil, 0, fmt.Errorf("%w: manifest checksum", f.errCorrupt)
	}
	if string(manifest[0:8]) != f.manifestMagic {
		return nil, 0, fmt.Errorf("%w: manifest magic", f.errCorrupt)
	}
	if v := binary.LittleEndian.Uint32(manifest[8:12]); v != f.version {
		return nil, 0, fmt.Errorf("%w: manifest version %d != %d", f.errCorrupt, v, f.version)
	}
	return manifest, offset, nil
}

// segmentFrame is the envelope of a standalone segment image.
var segmentFrame = persistFrame{
	headerMagic:   persistHeaderMagic,
	manifestMagic: persistManifestMagic,
	footerMagic:   persistFooterMagic,
	version:       persistVersion,
	manifestFixed: segmentManifestFixed,
	errMagic:      ErrPersistMagic,
	errVersion:    ErrPersistVersion,
	errCorrupt:    ErrPersistCorrupt,
}

// The manifest flag bits record the segment's opt-in modes and the enrichment
// option, so Open restores the same configuration and rebuilds the same
// accelerators.
const (
	persistFlagShapeTapes = 1 << iota
	persistFlagPostings
	persistFlagValueDict
	persistFlagHashKeys
	persistFlagWideValueTapes
)

// A document record's storage class, in its header: a classic tape, a 16-byte
// shape-taped value array, or an 8-byte narrow value array.
const (
	persistDocClassic uint8 = iota
	persistDocWide
	persistDocNarrow
)

// persistNoShape marks a classic record's absent shape id.
const persistNoShape = ^uint32(0)

// Open and WriteTo report these on a malformed or unrecognized image. They own
// no storage and are safe to compare concurrently.
var (
	// ErrPersistMagic means the image is not a Segment serialization: a header or
	// footer magic did not match.
	ErrPersistMagic = errors.New("vibejson: not a Segment image")
	// ErrPersistVersion means the image's format version differs from this
	// build's; the pre-v1 format is unstable and mismatches are rejected rather
	// than misread.
	ErrPersistVersion = errors.New("vibejson: unsupported Segment image version")
	// ErrPersistCorrupt means the image is structurally invalid: a truncated or
	// out-of-range section, a failed manifest checksum, or an inconsistent
	// record. It is the fail-closed verdict on any input Open cannot trust.
	ErrPersistCorrupt = errors.New("vibejson: corrupt Segment image")
)

// persistNativeLittleEndian reports whether the host stores integers
// little-endian, so the bulk entry sections can be aliased (native) or must be
// decoded word by word (big-endian). Determined once at init.
var persistNativeLittleEndian = func() bool {
	x := uint16(1)
	return *(*byte)(unsafe.Pointer(&x)) == 1
}()

// persistAlign8 rounds n up to the next multiple of eight, the alignment the
// entry sections take so an aliased IndexEntry view meets its 4-byte load
// requirement and the record after it starts aligned.
func persistAlign8(n uint64) uint64 { return (n + 7) &^ 7 }

// ManifestChecksum is the FNV-1a 64-bit fold used to seal the manifest. It gates
// only structural trust — a mismatch rejects the image — so a non-cryptographic
// fold is sufficient.
func ManifestChecksum(b []byte) uint64 {
	const (
		offset uint64 = 1469598103934665603
		prime  uint64 = 1099511628211
	)
	h := offset
	for _, c := range b {
		h = (h ^ uint64(c)) * prime
	}
	return h
}

// WriteTo serializes the segment to w in the log-structured image this file
// documents and returns the number of bytes written, satisfying io.WriterTo.
// It streams a header, one self-describing record per document in ordinal
// order, the shared shape table, the manifest indexing them, and a locating
// footer, buffering only the O(documents) manifest so a large corpus never
// costs a second copy. The image reopens through Open into a Segment whose every
// Doc and accessor is byte-identical to this one's.
func (s *Segment) WriteTo(w io.Writer) (int64, error) {
	pw := &persistWriter{w: w}
	s.writeToPersistWriter(pw, 0)
	return pw.off, pw.err
}

// writeToNested writes a self-contained image through parent while keeping
// all offsets relative to the nested image. Reusing the outer writer avoids
// allocating one io.Writer adapter and scratch block per collection micro-page.
func (s *Segment) writeToNested(pw *persistWriter) (int64, error) {
	base := pw.off
	s.writeToPersistWriter(pw, base)
	return pw.off - base, pw.err
}

func (s *Segment) writeToPersistWriter(pw *persistWriter, base int64) {

	segmentFrame.writeHeader(pw)

	// Shape records are addressed by their compiled id — their position in the
	// cache's shape list, which is stable — so a record names its shape by that
	// index and the shape table is written in the same order.
	shapeRecords := s.persistShapeRecords()
	shapeID := make(map[*ShapeRecord]uint32, len(shapeRecords))
	for id, rec := range shapeRecords {
		shapeID[rec] = uint32(id)
	}

	var smallOffsets [persistSmallManifestDocuments]uint64
	var docOffsets []uint64
	docCount := s.Len()
	if docCount <= len(smallOffsets) {
		docOffsets = smallOffsets[:docCount]
	} else {
		docOffsets = make([]uint64, docCount)
	}
	var narrowTotal uint64
	for i := range docCount {
		docOffsets[i] = pw.writeDocRecord(s, i, shapeID, &narrowTotal, base)
	}

	pw.pad8()
	shapeTableOffset := uint64(pw.off - base)
	pw.writeShapeTable(shapeRecords)
	shapeTableLength := uint64(pw.off-base) - shapeTableOffset

	pw.pad8()
	manifestOffset := uint64(pw.off - base)
	manifest := s.buildManifest(pw.small[:0], docOffsets, narrowTotal, shapeTableOffset, shapeTableLength)
	pw.write(manifest)

	segmentFrame.writeFooter(pw, manifestOffset, manifest)
}

// writeDocRecord lays down document i's self-describing record and returns its
// absolute offset. The 24-byte header is followed by the source bytes and, once
// realigned to eight, the tape: the classic or wide value entries at 16 bytes,
// or the narrow value array at 8. It accumulates the narrow value total so the
// reader can size the consolidated slab in one allocation.
//
//	 0        4        8        12       16       20  21  22   24
//	+--------+--------+--------+--------+--------+---+---+-----+
//	| srcLen | nEntry | start  |  end   | shape  | k | e | pad |
//	+--------+--------+--------+--------+--------+---+---+-----+
//	 srcLen  source byte length            start/end  shape-taped root span
//	 nEntry  entry/value count             shape      shape id (classic: ^0)
//	 k       storage class (persistDoc*)   e          key-hash enrichment flag
func (pw *persistWriter) writeDocRecord(s *Segment, i int, shapeID map[*ShapeRecord]uint32, narrowTotal *uint64, base int64) uint64 {
	pw.pad8()
	offset := uint64(pw.off - base)

	idx := s.DocAt(i)
	ref := s.ShapeTapeRefAt(i)
	template, templateOK := s.TemplateAt(i)
	if ref.Rec != nil {
		ref.Start, ref.End = s.shapeTapeRootSpan(idx, ref)
	}

	var (
		kind    uint8
		entries uint32
		sid     = persistNoShape
	)
	switch {
	case templateOK:
		kind = persistDocClassic
		entries = uint32(len(template.Index.Entries))
	case ref.Rec == nil:
		kind = persistDocClassic
		entries = uint32(len(idx.Entries))
	case ref.Narrow:
		kind = persistDocNarrow
		entries = uint32(len(ref.Rec.Fields))
		sid = shapeID[ref.Rec]
		*narrowTotal += uint64(entries)
	default:
		kind = persistDocWide
		entries = uint32(len(idx.Entries))
		sid = shapeID[ref.Rec]
	}

	var header [persistRecordHeaderLen]byte
	binary.LittleEndian.PutUint32(header[0:4], uint32(len(idx.Src)))
	binary.LittleEndian.PutUint32(header[4:8], entries)
	binary.LittleEndian.PutUint32(header[8:12], ref.Start)
	binary.LittleEndian.PutUint32(header[12:16], ref.End)
	binary.LittleEndian.PutUint32(header[16:20], sid)
	header[20] = kind
	if ref.enriched {
		header[21] = 1
	}
	pw.writeSmall(header[:])
	pw.write(idx.Src)
	pw.pad8()
	if templateOK {
		pw.writeTemplateDoc(s, i, template)
	} else if kind == persistDocNarrow {
		pw.writeNarrowDoc(s, i, ref, entries)
	} else {
		pw.writeEntries(idx.Entries)
	}
	return offset
}

// writeTemplateDoc expands a builder-only repeated-layout template directly
// into the stable checkpoint format. It uses fixed scratch and never creates
// a second in-memory tape, so checkpointing a compact collection remains bounded.
func (pw *persistWriter) writeTemplateDoc(s *Segment, doc int, template *DocumentTemplate) {
	var raw [16]byte
	for ordinal := range template.Index.Entries {
		entry := template.Index.Entries[ordinal]
		if ordinal == 0 {
			span := s.TemplateSpan(doc, template, ordinal)
			entry.Start, entry.End = span&0xffff, span>>16
		} else if template.spanIndex[ordinal] == ^uint16(0) {
			entry.Start, entry.End = s.storeTemplateKeySpan(doc, template, ordinal)
		} else {
			span := s.TemplateSpan(doc, template, ordinal)
			entry.Start, entry.End = span&0xffff, span>>16
		}
		binary.LittleEndian.PutUint32(raw[0:4], entry.Start)
		binary.LittleEndian.PutUint32(raw[4:8], entry.End)
		binary.LittleEndian.PutUint32(raw[8:12], entry.Next)
		binary.LittleEndian.PutUint32(raw[12:16], entry.Info)
		pw.writeSmall(raw[:])
	}
}

// writeNarrowDoc streams one compact tape from either the ordinary Go slab or
// a collection page image. The fixed eight-byte scratch keeps re-checkpointing an
// Open zero-allocation per row and avoids materializing a second tape.
func (pw *persistWriter) writeNarrowDoc(s *Segment, doc int, ref ShapeTapeRef, entries uint32) {
	var raw [8]byte
	for i := range entries {
		value := s.NarrowAt(doc, ref, int(i))
		binary.LittleEndian.PutUint32(raw[0:4], value.Span)
		binary.LittleEndian.PutUint32(raw[4:8], value.Info)
		pw.writeSmall(raw[:])
	}
}

// writeShapeTable serializes the shared shapes: a count, then per shape a field
// count and each field's raw key spelling (the content between the quotes,
// escapes included). Only the raw spellings are stored; Open reconstructs the
// decoded names, the info words, the name table, and the fingerprint by
// resolving each shape back through a fresh ShapeCache, so the table is the
// interned key store and nothing about a shape is duplicated on disk.
func (s *Segment) persistShapeRecords() []*ShapeRecord {
	if len(s.mappedShapes) != 0 {
		return s.mappedShapes
	}
	return s.shapes.shapes
}

func (pw *persistWriter) writeShapeTable(shapes []*ShapeRecord) {
	var scratch [4]byte
	binary.LittleEndian.PutUint32(scratch[:], uint32(len(shapes)))
	pw.writeSmall(scratch[:])
	for _, rec := range shapes {
		binary.LittleEndian.PutUint32(scratch[:], uint32(len(rec.Fields)))
		pw.writeSmall(scratch[:])
		for f := range rec.Fields {
			raw := rec.Fields[f].raw
			binary.LittleEndian.PutUint32(scratch[:], uint32(len(raw)))
			pw.writeSmall(scratch[:])
			if len(raw) > 0 {
				pw.write(byteview.Bytes(raw))
			}
		}
	}
}

// buildManifest assembles the manifest bytes: the fixed prologue (magic,
// version, flags, options, counts, and the shape table span) followed by the
// offsets index — the absolute offset of every document record, which is the
// snapshot's live segment. It is buffered whole so WriteTo can checksum it.
func (s *Segment) buildManifest(dst []byte, docOffsets []uint64, narrowTotal, shapeTableOffset, shapeTableLength uint64) []byte {
	need := segmentManifestFixed + 8*len(docOffsets)
	if cap(dst) < need {
		dst = make([]byte, need)
	} else {
		dst = dst[:need]
		clear(dst)
	}
	buf := dst
	copy(buf[0:8], persistManifestMagic)
	binary.LittleEndian.PutUint32(buf[8:12], persistVersion)
	binary.LittleEndian.PutUint32(buf[12:16], s.persistFlags())
	binary.LittleEndian.PutUint64(buf[16:24], uint64(s.Options.MaxDepth))
	binary.LittleEndian.PutUint32(buf[24:28], s.valueFloor)
	// buf[28:32] reserved
	binary.LittleEndian.PutUint32(buf[32:36], uint32(len(docOffsets)))
	binary.LittleEndian.PutUint32(buf[36:40], uint32(narrowTotal))
	binary.LittleEndian.PutUint64(buf[40:48], shapeTableOffset)
	binary.LittleEndian.PutUint64(buf[48:56], shapeTableLength)
	for i, off := range docOffsets {
		binary.LittleEndian.PutUint64(buf[segmentManifestFixed+8*i:], off)
	}
	return buf
}

// persistFlags packs the segment's modes and enrichment option for the manifest.
func (s *Segment) persistFlags() uint32 {
	var f uint32
	if s.ShapeTapes {
		f |= persistFlagShapeTapes
	}
	if s.Postings {
		f |= persistFlagPostings
	}
	if s.ValueDict {
		f |= persistFlagValueDict
	}
	if s.Options.HashKeys {
		f |= persistFlagHashKeys
	}
	if s.wideValueTapes {
		f |= persistFlagWideValueTapes
	}
	return f
}

// A persistWriter threads a running offset and the first write error through
// WriteTo so callers stream without tracking either. Once an error is latched
// every later write is a no-op and WriteTo returns it.
type persistWriter struct {
	w     io.Writer
	off   int64
	err   error
	small [segmentManifestFixed + 8*persistSmallManifestDocuments]byte
}

// Write lets nested persistence formats stream through one offset/error
// tracker. It also upgrades a short write with no explicit error to
// io.ErrShortWrite, as io.WriterTo requires.
func (pw *persistWriter) Write(b []byte) (int, error) {
	if pw.err != nil || len(b) == 0 {
		return 0, pw.err
	}
	n, err := pw.w.Write(b)
	pw.off += int64(n)
	if err == nil && n != len(b) {
		err = io.ErrShortWrite
	}
	if err != nil {
		pw.err = err
	}
	return n, err
}

func (pw *persistWriter) write(b []byte) {
	_, _ = pw.Write(b)
}

// writeSmall copies a short stack-backed field into writer-owned storage
// before crossing the io.Writer interface. Without this boundary, record
// headers and endian scratch arrays escape once per document. All callers are
// synchronous and len(b) is at most the fixed footer width.
func (pw *persistWriter) writeSmall(b []byte) {
	if len(b) > len(pw.small) {
		panic("vibejson: internal persistence field exceeds scratch")
	}
	copy(pw.small[:], b)
	pw.write(pw.small[:len(b)])
}

// pad8 writes zero bytes up to the next eight-byte boundary.
func (pw *persistWriter) pad8() {
	if r := pw.off & 7; r != 0 {
		clear(pw.small[:8])
		pw.write(pw.small[:8-r])
	}
}

// writeEntries emits a 16-byte entry array little-endian: a raw copy on a
// little-endian host, where the words already match, and a per-word encode
// otherwise.
func (pw *persistWriter) writeEntries(e []vibejson.IndexEntry) {
	if len(e) == 0 {
		return
	}
	if persistNativeLittleEndian {
		pw.write(unsafe.Slice((*byte)(unsafe.Pointer(&e[0])), len(e)*int(unsafe.Sizeof(vibejson.IndexEntry{}))))
		return
	}
	for i := range e {
		binary.LittleEndian.PutUint32(pw.small[0:4], e[i].Start)
		binary.LittleEndian.PutUint32(pw.small[4:8], e[i].End)
		binary.LittleEndian.PutUint32(pw.small[8:12], e[i].Next)
		binary.LittleEndian.PutUint32(pw.small[12:16], e[i].Info)
		pw.write(pw.small[:16])
	}
}

// Open reconstructs a Segment from an image WriteTo produced. The returned segment
// borrows data: its document sources and entry tapes view into it (a memory map
// pages in only what a reader touches), so data must stay valid — and a memory
// map stay mapped — for the segment's lifetime. Every Doc and accessor is
// byte-identical to the segment that was written. Open validates the header,
// footer, manifest checksum, and every section bound, returning ErrPersistMagic,
// ErrPersistVersion, or ErrPersistCorrupt without panicking on any truncated or
// malformed input.
func OpenSegment(data []byte) (*Segment, error) {
	s := new(Segment)
	if err := openSegmentInto(s, data); err != nil {
		return nil, err
	}
	return s, nil
}

// openSegmentInto reconstructs an image directly into caller-owned storage.
// Collection persistence uses it to initialize an embedded chunk Segment without
// copying a value that contains a synchronization primitive.
func openSegmentInto(s *Segment, data []byte) error {
	return openSegmentIntoMode(s, data, nil, 0)
}

// openSegmentIntoCollection reconstructs one collection micro-page with its per-document
// slice/shape headers in the collection-wide pointer-free external descriptor
// block. Public Open deliberately keeps its existing append-capable layout.
func openSegmentIntoCollection(s *Segment, data []byte, mapped *storeMappedDocs, base uint64) error {
	return openSegmentIntoMode(s, data, mapped, base)
}

func openSegmentIntoMode(s *Segment, data []byte, mapped *storeMappedDocs, mappedBase uint64) error {
	manifest, manifestOff, err := segmentFrame.openManifest(data)
	if err != nil {
		return err
	}
	manifestLen := uint64(len(manifest))
	// limit is the last byte the payload region can reach: everything before
	// the fixed footer.
	limit := uint64(len(data)) - ImageFooterLen
	flags := binary.LittleEndian.Uint32(manifest[12:16])
	maxDepth := int64(binary.LittleEndian.Uint64(manifest[16:24]))
	valueFloor := binary.LittleEndian.Uint32(manifest[24:28])
	docCount := binary.LittleEndian.Uint32(manifest[32:36])
	narrowTotal := binary.LittleEndian.Uint32(manifest[36:40])
	shapeTableOffset := binary.LittleEndian.Uint64(manifest[40:48])
	shapeTableLength := binary.LittleEndian.Uint64(manifest[48:56])

	// The offsets index must fit the manifest, and the shape table must lie in
	// the records region before the manifest; both bound the untrusted counts
	// against real bytes so no allocation trusts the header. The span check is
	// unconditional so even a zero-length table cannot slice past the image.
	if uint64(docCount) > (manifestLen-segmentManifestFixed)/8 {
		return fmt.Errorf("%w: document count exceeds manifest", ErrPersistCorrupt)
	}
	if shapeTableOffset > manifestOff || shapeTableLength > manifestOff-shapeTableOffset ||
		(shapeTableLength != 0 && shapeTableOffset < ImageHeaderLen) {
		return fmt.Errorf("%w: shape table span out of range", ErrPersistCorrupt)
	}
	if uint64(narrowTotal) > limit/uint64(unsafe.Sizeof(ShapeNarrowValue{})) {
		return fmt.Errorf("%w: narrow value total exceeds image", ErrPersistCorrupt)
	}

	compact := mapped != nil && persistNativeLittleEndian
	*s = Segment{source: data}
	if compact {
		s.mappedDocs = mapped
		s.mappedBase = mappedBase
		s.mappedCount = int(docCount)
		if mappedBase+uint64(docCount) > uint64(len(mapped.refs)) {
			return fmt.Errorf("%w: collection document directory span", ErrPersistCorrupt)
		}
	}
	s.ShapeTapes = flags&persistFlagShapeTapes != 0
	s.wideValueTapes = flags&persistFlagWideValueTapes != 0
	s.Options = document.IndexOptions{HashKeys: flags&persistFlagHashKeys != 0}
	if maxDepth > 0 {
		s.Options.MaxDepth = int(maxDepth)
	}
	s.valueFloor = valueFloor

	shapeRecs, err := s.openShapes(data[shapeTableOffset : shapeTableOffset+shapeTableLength])
	if err != nil {
		return err
	}

	if compact {
		s.mappedShapes = shapeRecs
	} else {
		s.docs = make([]vibejson.Index, docCount)
		s.narrow = make([]ShapeNarrowValue, 0, narrowTotal)
	}
	var tapeRefs []ShapeTapeRef
	if !compact {
		tapeRefs = make([]ShapeTapeRef, docCount)
	}
	hasShape := false
	for i := 0; i < int(docCount); i++ {
		recOff := binary.LittleEndian.Uint64(manifest[segmentManifestFixed+8*i:])
		ref, err := s.openDocRecord(data, recOff, manifestOff, shapeRecs, i, compact)
		if err != nil {
			return err
		}
		if ref.Rec != nil && !compact {
			tapeRefs[i] = ref
			hasShape = true
		}
	}
	// tapeRefs stays empty unless some document is shape-taped, matching the
	// commit-time invariant that it is either empty or exactly docs-aligned.
	if hasShape {
		s.tapeRefs = tapeRefs
	}

	s.rebuildAccelerators(flags)
	return nil
}

// openDocRecord reconstructs document i from the record at recOff, storing its
// Index in s.docs, appending any narrow values to the shared slab, and
// returning its shape header (the zero ref for a classic document). It bounds
// every span against the image so a malformed record fails closed rather than
// aliasing out of range.
func (s *Segment) openDocRecord(data []byte, recOff, recLimit uint64, shapeRecs []*ShapeRecord, i int, compact bool) (ShapeTapeRef, error) {
	if recOff < ImageHeaderLen || recOff&7 != 0 || recOff > recLimit || recLimit-recOff < persistRecordHeaderLen {
		return ShapeTapeRef{}, fmt.Errorf("%w: record %d header out of range", ErrPersistCorrupt, i)
	}
	h := data[recOff : recOff+persistRecordHeaderLen]
	srcLen := uint64(binary.LittleEndian.Uint32(h[0:4]))
	entryCount := uint64(binary.LittleEndian.Uint32(h[4:8]))
	start := binary.LittleEndian.Uint32(h[8:12])
	end := binary.LittleEndian.Uint32(h[12:16])
	sid := binary.LittleEndian.Uint32(h[16:20])
	kind := h[20]
	enriched := h[21] != 0

	srcOff := recOff + persistRecordHeaderLen
	if srcLen > recLimit-srcOff {
		return ShapeTapeRef{}, fmt.Errorf("%w: record %d source out of range", ErrPersistCorrupt, i)
	}
	src := data[srcOff : srcOff+srcLen : srcOff+srcLen]

	entriesOff := persistAlign8(srcOff + srcLen)
	var width uint64
	switch kind {
	case persistDocNarrow:
		width = uint64(unsafe.Sizeof(ShapeNarrowValue{}))
	default:
		width = uint64(unsafe.Sizeof(vibejson.IndexEntry{}))
	}
	if entriesOff > recLimit || entryCount > (recLimit-entriesOff)/width {
		return ShapeTapeRef{}, fmt.Errorf("%w: record %d entries out of range", ErrPersistCorrupt, i)
	}

	switch kind {
	case persistDocClassic:
		if compact {
			s.mappedDocs.refs[s.mappedBase+uint64(i)] = storeMappedDocRef{
				sourceOff: srcOff, srcLen: uint32(srcLen),
				entryCount: uint32(entryCount), shapeID: storeMappedNoShape, kind: kind,
			}
			return ShapeTapeRef{}, nil
		}
		s.docs[i] = vibejson.Index{Src: src, Entries: openEntries(data, entriesOff, entryCount)}
		return ShapeTapeRef{}, nil
	case persistDocWide:
		rec, err := shapeAt(shapeRecs, sid, i)
		if err != nil {
			return ShapeTapeRef{}, err
		}
		if entryCount != uint64(len(rec.Fields)) {
			return ShapeTapeRef{}, fmt.Errorf("%w: record %d value count != shape width", ErrPersistCorrupt, i)
		}
		if compact {
			s.mappedDocs.refs[s.mappedBase+uint64(i)] = storeMappedDocRef{
				sourceOff: srcOff, srcLen: uint32(srcLen),
				entryCount: uint32(entryCount), start: start, end: end, shapeID: sid,
				kind: kind, enriched: enriched,
			}
			return ShapeTapeRef{Rec: rec, Start: start, End: end, enriched: enriched}, nil
		}
		s.docs[i] = vibejson.Index{Src: src, Entries: openEntries(data, entriesOff, entryCount)}
		return ShapeTapeRef{Rec: rec, Start: start, End: end, enriched: enriched}, nil
	case persistDocNarrow:
		rec, err := shapeAt(shapeRecs, sid, i)
		if err != nil {
			return ShapeTapeRef{}, err
		}
		if entryCount != uint64(len(rec.Fields)) {
			return ShapeTapeRef{}, fmt.Errorf("%w: record %d narrow count != shape width", ErrPersistCorrupt, i)
		}
		if compact {
			s.mappedDocs.refs[s.mappedBase+uint64(i)] = storeMappedDocRef{
				sourceOff: srcOff, srcLen: uint32(srcLen),
				entryCount: uint32(entryCount), start: start, end: end, shapeID: sid,
				kind: kind, enriched: enriched,
			}
			s.mappedNarrow += int(entryCount)
			return ShapeTapeRef{Rec: rec, Start: start, End: end, Narrow: true, enriched: enriched}, nil
		}
		slabOff := uint32(len(s.narrow))
		s.narrow = appendNarrow(s.narrow, data, entriesOff, entryCount)
		s.docs[i] = vibejson.Index{Src: src}
		return ShapeTapeRef{Rec: rec, Start: start, End: end, off: slabOff, Narrow: true, enriched: enriched}, nil
	default:
		return ShapeTapeRef{}, fmt.Errorf("%w: record %d unknown storage class %d", ErrPersistCorrupt, i, kind)
	}
}

// shapeAt resolves a record's shape id against the reconstructed table.
func shapeAt(shapeRecs []*ShapeRecord, sid uint32, doc int) (*ShapeRecord, error) {
	if uint64(sid) >= uint64(len(shapeRecs)) {
		return nil, fmt.Errorf("%w: record %d shape id %d out of range", ErrPersistCorrupt, doc, sid)
	}
	return shapeRecs[sid], nil
}

// openShapes rebuilds the shared shapes from the serialized key spellings. Each
// shape is reconstructed by resolving a synthetic flat object of its keys back
// through the segment's ShapeCache — the identical machinery that compiled it —
// which reproduces the fingerprint, decoded names, info words, name table, and
// duplicate-key flag exactly, so the reopened cache resolves and continues to
// Append against the same shapes. Records are reconstructed in id order, so a
// record's stored shape id indexes the returned slice directly.
func (s *Segment) openShapes(table []byte) ([]*ShapeRecord, error) {
	if len(table) == 0 {
		return nil, nil
	}
	r := persistReader{b: table, ok: true}
	shapeCount := r.u32()
	if !r.ok || uint64(shapeCount) > uint64(len(table))/8 {
		return nil, fmt.Errorf("%w: shape count exceeds table", ErrPersistCorrupt)
	}
	recs := make([]*ShapeRecord, 0, shapeCount)
	var synth []byte
	for k := range shapeCount {
		fieldCount := r.u32()
		if !r.ok || fieldCount == 0 || fieldCount > shapeMaxFields {
			return nil, fmt.Errorf("%w: shape %d field count %d", ErrPersistCorrupt, k, fieldCount)
		}
		synth = append(synth[:0], '{')
		for m := range fieldCount {
			rawLen := r.u32()
			raw := r.bytes(uint64(rawLen))
			if !r.ok {
				return nil, fmt.Errorf("%w: shape %d key %d out of range", ErrPersistCorrupt, k, m)
			}
			if m > 0 {
				synth = append(synth, ',')
			}
			synth = append(synth, '"')
			synth = append(synth, raw...)
			synth = append(synth, '"', ':', '0')
		}
		synth = append(synth, '}')
		rec, err := s.rebuildShape(synth, int(fieldCount))
		if err != nil {
			return nil, err
		}
		recs = append(recs, rec)
	}
	return recs, nil
}

// rebuildShape compiles one shape from a synthetic object of its keys. The
// values are placeholders — the fingerprint and fields depend only on the keys
// — and the object is resolved twice to clear the cache's sighting gate, so the
// second resolve compiles it; a key sequence already compiled (a duplicate in a
// malformed table) resolves on the first probe and is returned as is, never
// panicking.
func (s *Segment) rebuildShape(synth []byte, fieldCount int) (*ShapeRecord, error) {
	idx, err := vibejson.BuildIndexOptions(synth, make([]vibejson.IndexEntry, 2*fieldCount+2), document.IndexOptions{})
	if err != nil {
		return nil, fmt.Errorf("%w: shape rebuild: %v", ErrPersistCorrupt, err)
	}
	node := idx.Root()
	if shape, ok := s.shapes.Resolve(node); ok {
		return shape.rec, nil
	}
	shape, ok := s.shapes.Resolve(node)
	if !ok || shape.rec == nil {
		return nil, fmt.Errorf("%w: shape did not compile", ErrPersistCorrupt)
	}
	return shape.rec, nil
}

// rebuildAccelerators reconstructs the opt-in postings and value dictionary
// from the loaded documents when their flags were set, replaying each
// document's ingest hook in ordinal order. Both are pure functions of the
// committed documents (and, for the dictionary, the length floor already
// restored), so the replay reproduces the original structures and the reopened
// segment answers WhereExists, WhereContains, DocValue, and Stats identically.
func (s *Segment) rebuildAccelerators(flags uint32) {
	if flags&persistFlagPostings != 0 {
		s.Postings = true
		for i := 0; i < s.Len(); i++ {
			s.indexPostings(i, s.DocAt(i), s.ShapeTapeRefAt(i))
		}
	}
	if flags&persistFlagValueDict != 0 {
		s.ValueDict = true
		for i := 0; i < s.Len(); i++ {
			s.valueDictAppend(i, s.ShapeTapeRefAt(i))
		}
	}
}

// openEntries returns a 16-byte entry array from the image: a zero-copy view
// aliasing the mapped bytes when the host is little-endian and the address is
// 4-byte aligned, otherwise a decoded copy. The caller has bounded [off, off +
// count*16) within data.
func openEntries(data []byte, off, count uint64) []vibejson.IndexEntry {
	if count == 0 {
		return nil
	}
	if persistNativeLittleEndian {
		p := unsafe.Pointer(&data[off])
		if uintptr(p)%unsafe.Alignof(vibejson.IndexEntry{}) == 0 {
			return unsafe.Slice((*vibejson.IndexEntry)(p), int(count))
		}
	}
	out := make([]vibejson.IndexEntry, count)
	for i := range out {
		b := data[off+uint64(i)*16:]
		out[i] = vibejson.IndexEntry{
			Start: binary.LittleEndian.Uint32(b[0:4]),
			End:   binary.LittleEndian.Uint32(b[4:8]),
			Next:  binary.LittleEndian.Uint32(b[8:12]),
			Info:  binary.LittleEndian.Uint32(b[12:16]),
		}
	}
	return out
}

// appendNarrow appends count 8-byte narrow values from the image to dst. The
// consolidated slab cannot alias the scattered records, so the values are
// always copied — natively when the host and alignment allow, else per word.
// The caller has bounded [off, off + count*8) within data.
func appendNarrow(dst []ShapeNarrowValue, data []byte, off, count uint64) []ShapeNarrowValue {
	if count == 0 {
		return dst
	}
	if persistNativeLittleEndian {
		p := unsafe.Pointer(&data[off])
		if uintptr(p)%unsafe.Alignof(ShapeNarrowValue{}) == 0 {
			return append(dst, unsafe.Slice((*ShapeNarrowValue)(p), int(count))...)
		}
	}
	for i := range count {
		b := data[off+i*8:]
		dst = append(dst, ShapeNarrowValue{
			Span: binary.LittleEndian.Uint32(b[0:4]),
			Info: binary.LittleEndian.Uint32(b[4:8]),
		})
	}
	return dst
}

// A persistReader is a bounds-checked cursor over a section, valid while ok is
// set. A read that would exceed the section clears ok and yields a zero, and a
// read on a cleared cursor is a no-op, so once any read fails every later one
// does too and a caller checks ok once after a run of reads — a truncated
// section can never panic. ok must start true.
type persistReader struct {
	b   []byte
	pos uint64
	ok  bool
}

func (r *persistReader) u32() uint32 {
	if !r.ok || r.pos+4 > uint64(len(r.b)) {
		r.ok = false
		return 0
	}
	v := binary.LittleEndian.Uint32(r.b[r.pos:])
	r.pos += 4
	return v
}

func (r *persistReader) bytes(n uint64) []byte {
	if !r.ok || n > uint64(len(r.b))-r.pos {
		r.ok = false
		return nil
	}
	v := r.b[r.pos : r.pos+n]
	r.pos += n
	return v
}
