package tin

import (
	"encoding/binary"
	"fmt"
	"slices"
)

// Distributed segments: the shippable queryable unit.
//
// A Segment is one immutable snapshot slice — sealed term lists plus the
// vocabulary, lengths, and statistics a receiver needs to answer Match
// and Score with no re-indexing. Per-term bytes ride the sealed wire
// codec, so this file only frames the snapshot around them. Lengths ship
// as (id, length) pairs, which stays exact for sparse heap DocIDs where
// a dense array would not. Maps marshal in sorted key order, so
// remarshal is byte-stable.
//
// The distributed contract is per-segment scoring (Lucene-segment
// semantics): the receiver answers with the segment's own document
// frequencies, and the gatherer merges across segments. Open (unsealed)
// lists never ship — the sender seals first, as durable generation
// builds already do. Id gaps past 2^32 cannot seal (delta-coded ids)
// and stay open, so only dense or generation-local id spaces ship;
// sparse heap DocIDs beyond that range are a documented non-goal. An
// opened segment answers queries exactly like the sender; it is built
// for the query-only replica case (a later Add transparently unseals,
// as usual).
type Segment struct {
	Dict   map[uint64]string
	Lens   map[DocID]uint32
	NDocs  int
	Tokens uint64
	Lists  map[uint64]*sealedPostings
}

// ExportSegment collects the index's sealed lists with the vocabulary,
// lengths, and statistics a receiver needs. It takes the index lock like
// any other reader.
func (ix *Index) ExportSegment() *Segment {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	seg := &Segment{
		Dict:   make(map[uint64]string, len(ix.dict)),
		Lens:   make(map[DocID]uint32, len(ix.docs)),
		NDocs:  ix.nDocs,
		Tokens: ix.tokens,
		Lists:  make(map[uint64]*sealedPostings),
	}
	for h, spell := range ix.dict {
		seg.Dict[h] = spell
	}
	for id, meta := range ix.docs {
		seg.Lens[id] = meta.length
	}
	for term, p := range ix.post {
		if p.sealed != nil {
			seg.Lists[term] = p.sealed
		}
	}
	return seg
}

// OpenSegment builds a queryable index from a shipped segment, failing
// closed on accounting the wire codec cannot see (document counts,
// list sizes). Vocabulary relearns through learnLocked, so the spelling
// index arrives sorted and parse-ready.
func OpenSegment(seg *Segment) (*Index, error) {
	if seg == nil {
		return nil, fmt.Errorf("tin segment: nil")
	}
	if seg.NDocs != len(seg.Lens) {
		return nil, fmt.Errorf("tin segment: %d lengths for %d docs", len(seg.Lens), seg.NDocs)
	}
	ix := NewIndex()
	ix.nDocs = seg.NDocs
	ix.tokens = seg.Tokens
	minLen := uint32(0)
	first := true
	for id, length := range seg.Lens {
		ix.docs[id] = docMeta{length: length}
		if first || length < minLen {
			minLen, first = length, false
		}
	}
	ix.minDocLen = minLen
	for term, s := range seg.Lists {
		if s == nil || uint64(s.n) > uint64(seg.NDocs) {
			return nil, fmt.Errorf("tin segment: bad list for term %d", term)
		}
		ix.post[term] = &postings{sealed: s}
	}
	for h, spell := range seg.Dict {
		ix.learnLocked(h, []byte(spell))
	}
	ix.sorted = true
	return ix, nil
}

const (
	segmentWireMagic   = "tinG"
	segmentWireVersion = 1
)

// MarshalSegment encodes a segment for shipment: header, vocabulary,
// lengths, then each sealed list framed by its own length.
func MarshalSegment(seg *Segment) ([]byte, error) {
	if seg == nil {
		return nil, fmt.Errorf("tin segment: nothing to marshal")
	}
	out := make([]byte, 0, 1024)
	out = append(out, segmentWireMagic...)
	out = append(out, segmentWireVersion)
	putU32 := func(v uint32) {
		out = append(out, byte(v), byte(v>>8), byte(v>>16), byte(v>>24))
	}
	putU64 := func(v uint64) {
		putU32(uint32(v))
		putU32(uint32(v >> 32))
	}
	putU64(uint64(seg.NDocs))
	putU64(seg.Tokens)
	hashes := make([]uint64, 0, len(seg.Dict))
	for h := range seg.Dict {
		hashes = append(hashes, h)
	}
	slices.Sort(hashes)
	putU32(uint32(len(hashes)))
	for _, h := range hashes {
		putU64(h)
		spell := seg.Dict[h]
		putU32(uint32(len(spell)))
		out = append(out, spell...)
	}
	ids := make([]DocID, 0, len(seg.Lens))
	for id := range seg.Lens {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	out = appendLensSection(out, ids, seg.Lens)
	terms := make([]uint64, 0, len(seg.Lists))
	for term := range seg.Lists {
		terms = append(terms, term)
	}
	slices.Sort(terms)
	putU32(uint32(len(terms)))
	for _, term := range terms {
		w, err := MarshalSealed(seg.Lists[term])
		if err != nil {
			return nil, err
		}
		putU64(term)
		putU32(uint32(len(w)))
		out = append(out, w...)
	}
	return out, nil
}

// Lens section layout (gaps and lengths uvarint): count u32, checked
// against the header count by the caller, then mode u8 (0 = contiguous
// ids, 1 = gapped) | base u64 |
// gaps (mode 1 only, N-1 uvarints, each >= 1) | lengths (N uvarints).
// Build ordinals and durable generations are contiguous, so the common
// case stores ~2-3 bytes per document instead of 12; sparse heaps fall
// back to delta gaps, which stay compact while ids cluster.
func appendLensSection(out []byte, ids []DocID, lens map[DocID]uint32) []byte {
	putU32 := func(v uint32) {
		out = append(out, byte(v), byte(v>>8), byte(v>>16), byte(v>>24))
	}
	putU64 := func(v uint64) {
		putU32(uint32(v))
		putU32(uint32(v >> 32))
	}
	putU32(uint32(len(ids)))
	if len(ids) == 0 {
		return out
	}
	contig := true
	for i, id := range ids {
		if uint64(id) != uint64(ids[0])+uint64(i) {
			contig = false
			break
		}
	}
	if contig {
		out = append(out, 0)
	} else {
		out = append(out, 1)
	}
	putU64(uint64(ids[0]))
	prev := uint64(ids[0])
	for _, id := range ids[1:] {
		if !contig {
			out = binary.AppendUvarint(out, uint64(id)-prev)
		}
		prev = uint64(id)
	}
	for _, id := range ids {
		out = binary.AppendUvarint(out, uint64(lens[id]))
	}
	return out
}

// readLensSection decodes count lengths, failing closed on truncation,
// bad modes, zero gaps, duplicate ids, or count mismatch. Decoded ids
// feed a map sized exactly count, so corrupt values cannot over-allocate.
func readLensSection(r *sealReader, count int) (map[DocID]uint32, error) {
	lens := make(map[DocID]uint32, count)
	if count == 0 {
		return lens, nil
	}
	mode := r.byte()
	if r.err != nil || mode > 1 {
		return nil, fmt.Errorf("tin segment: bad lens mode")
	}
	base := r.u64()
	if r.err != nil {
		return nil, r.err
	}
	ids := make([]DocID, count)
	ids[0] = DocID(base)
	prev := base
	for i := 1; i < count; i++ {
		if mode == 0 {
			prev++
		} else {
			gap, n := binary.Uvarint(r.b)
			if n <= 0 {
				return nil, fmt.Errorf("tin segment: bad lens gap")
			}
			r.b = r.b[n:]
			if gap == 0 {
				return nil, fmt.Errorf("tin segment: zero lens gap")
			}
			prev += gap
		}
		ids[i] = DocID(prev)
	}
	for _, id := range ids {
		v, n := binary.Uvarint(r.b)
		if n <= 0 || v > 0xffffffff {
			return nil, fmt.Errorf("tin segment: bad lens length")
		}
		r.b = r.b[n:]
		lens[id] = uint32(v)
	}
	if len(lens) != count {
		return nil, fmt.Errorf("tin segment: duplicate lens ids")
	}
	return lens, nil
}

// UnmarshalSegment decodes a shipped segment, failing closed before any
// make the remaining bytes cannot back.
func UnmarshalSegment(b []byte) (*Segment, error) {
	r := &sealReader{b: b}
	if string(r.bytes(4)) != segmentWireMagic {
		return nil, fmt.Errorf("tin segment: bad magic")
	}
	if r.byte() != segmentWireVersion {
		return nil, fmt.Errorf("tin segment: bad version")
	}
	seg := &Segment{}
	seg.NDocs = int(r.u64())
	seg.Tokens = r.u64()
	if r.err != nil {
		return nil, r.err
	}
	// Every document costs at least one lens byte (its uvarint length),
	// so a count above the remaining bytes is impossible. A tighter
	// per-doc floor would false-reject tightly packed small-doc shards,
	// whose whole wire runs near five bytes per document.
	if seg.NDocs < 0 || seg.NDocs > len(r.b) {
		return nil, fmt.Errorf("tin segment: bad doc count %d", seg.NDocs)
	}
	n := int(r.u32())
	if r.err != nil || n < 0 || n > len(r.b)/13 {
		return nil, fmt.Errorf("tin segment: bad dict length %d", n)
	}
	seg.Dict = make(map[uint64]string, n)
	for range n {
		h := r.u64()
		sn := int(r.u32())
		if r.err != nil || sn < 0 || sn > len(r.b) {
			return nil, fmt.Errorf("tin segment: bad spelling length %d", sn)
		}
		spell := r.bytes(sn)
		if r.err != nil {
			return nil, r.err
		}
		seg.Dict[h] = string(spell)
	}
	n = int(r.u32())
	if r.err != nil || n != seg.NDocs {
		return nil, fmt.Errorf("tin segment: %d lengths for %d docs", n, seg.NDocs)
	}
	lens, err := readLensSection(r, n)
	if err != nil {
		return nil, err
	}
	seg.Lens = lens
	n = int(r.u32())
	if r.err != nil || n < 0 || n > len(r.b)/13 {
		return nil, fmt.Errorf("tin segment: bad list count %d", n)
	}
	seg.Lists = make(map[uint64]*sealedPostings, n)
	for range n {
		term := r.u64()
		ln := int(r.u32())
		if r.err != nil || ln < 0 || ln > len(r.b) {
			return nil, fmt.Errorf("tin segment: bad list length %d", ln)
		}
		w := r.bytes(ln)
		if r.err != nil {
			return nil, r.err
		}
		s, err := UnmarshalSealed(w)
		if err != nil {
			return nil, err
		}
		seg.Lists[term] = s
	}
	return seg, nil
}
