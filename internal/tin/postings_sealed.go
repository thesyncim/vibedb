package tin

import "math/bits"

// Sealed postings: bit-packed, delta-coded posting lists for immutable
// indexes.
//
// An open posting list costs 8 bytes per id (DocID), 4 per offset boundary,
// and 4 per position. Sealed lists pack each 128-row block with its own bit
// widths: sorted ids become small deltas, frequencies (derived from the
// offsets, so no frequency array exists in either layout) pack beside them,
// and positions become one raw base per document plus narrow deltas. Dense
// durable builds seal after their single scan, so every generation-pinned
// query reads packed lists; heap indexes stay open (mutable) until Seal.
//
// Decode is streaming: a bitReader pulls one value at a time with scalar
// locals, so boolean matching, scoring gathers, and span expansion decode
// straight into the caller's output with no temporary buffers. Random access
// (phrase lookups) binary-searches the per-block first-id index, then seeks
// through per-16-row checkpoints and scans at most 15 documents. All sealed
// readers run under the index lock and stage in index-owned scratch, so warm
// queries allocate nothing beyond output growth — the same contract the open
// readers keep.

// sealedBlockRows is the packing quantum: 128 rows per block, the SIMD-BP128
// convention, so directory entries amortize to fractions of a byte per
// document while one block still fits L1.
const sealedBlockRows = 128

// sealedCkptEvery checkpoints stream bit offsets every 16 rows: 8
// checkpoints per stream per block (0.5 bytes per document across both
// streams) bound every random seek to a walk of at most 15 codes.
const sealedCkptEvery = 16

const sealedCkptPerBlock = sealedBlockRows / sealedCkptEvery

// sealedPostings is one term's compressed posting list. first holds each
// block's first document id raw (the block index); blk directories address
// the packed streams. bases holds one raw position base per document,
// indexed by global row.
type sealedPostings struct {
	n     uint32
	occ   uint64
	first []uint64
	blk   []sealedBlock
	ids   []uint32
	cnts  []uint32
	bases []uint32
	pos   []uint32
}

// sealedBlock directories one 128-row block. idW, cntW, posW are the packed
// bit widths (1..32); idOff, cntOff, posOff are word offsets into the
// streams; posCkpt holds the delta-stream bit offsets (relative to posOff)
// of rows 0, 16, ..., 112. Counts need no checkpoints: fixed-width codes
// seek arithmetically.
type sealedBlock struct {
	rows            uint32
	idW, cntW, posW uint8
	idOff, cntOff   uint32
	posOff          uint32
	posCkpt         [sealedCkptPerBlock]uint32
}

// bitReader streams fixed-width codes LSB-first out of packed words. The
// zero value is unusable; build with packReaderAt/packReaderSeek.
type bitReader struct {
	w    []uint32
	i    int
	acc  uint64
	fill uint
}

// packReaderAt positions a reader at word offset off.
func packReaderAt(words []uint32, off uint32) bitReader {
	return bitReader{w: words, i: int(off)}
}

// packReaderSeek positions a reader at a bit offset relative to word offset
// off. Random seeks use this; sequential scans never do.
func packReaderSeek(words []uint32, off, bit uint32) bitReader {
	i := int(off) + int(bit>>5)
	shift := bit & 31
	return bitReader{w: words, i: i + 1, acc: uint64(words[i]) >> shift, fill: 32 - uint(shift)}
}

// next pulls one w-bit code (1 <= w <= 32). Callers must pull exactly the
// packed count: the stream carries no terminator.
func (r *bitReader) next(w uint8) uint32 {
	for r.fill < uint(w) {
		r.acc |= uint64(r.w[r.i]) << r.fill
		r.i++
		r.fill += 32
	}
	v := uint32(r.acc & (uint64(1)<<w - 1))
	r.acc >>= w
	r.fill -= uint(w)
	return v
}

// packWidth returns the bits needed to store x (minimum 1: a zero-width
// stream cannot advance a reader).
func packWidth(x uint32) uint8 {
	if w := packWidthOf(x); w > 1 {
		return w
	}
	return 1
}

// packWidthOf returns the exact bits needed to store x (0 for x == 0).
func packWidthOf(x uint32) uint8 {
	return uint8(bits.Len32(x))
}

// packAppend appends vals at width w to out.
func packAppend(vals []uint32, w uint8, out []uint32) []uint32 {
	acc := uint64(0)
	fill := uint(0)
	for _, v := range vals {
		acc |= uint64(v) << fill
		fill += uint(w)
		for fill >= 32 {
			out = append(out, uint32(acc))
			acc >>= 32
			fill -= 32
		}
	}
	if fill > 0 {
		out = append(out, uint32(acc))
	}
	return out
}

// sealPostings compresses p, which must be sorted. It returns nil — leaving
// the list open — when an id gap escapes 32 bits (heap DocIDs are arbitrary
// uint64; dense build ordinals never do).
func sealPostings(p *postings) *sealedPostings {
	n := len(p.ids)
	if n == 0 || p.sealed != nil {
		return nil
	}
	nb := (n + sealedBlockRows - 1) / sealedBlockRows
	s := &sealedPostings{
		n:     uint32(n),
		occ:   uint64(len(p.pos)),
		first: make([]uint64, nb),
		blk:   make([]sealedBlock, nb),
	}
	var deltas [sealedBlockRows]uint32
	var freqs [sealedBlockRows]uint32
	gapsBuf := make([]uint32, 0, 1024)
	for b := range nb {
		start := b * sealedBlockRows
		end := start + sealedBlockRows
		if end > n {
			end = n
		}
		rows := end - start
		bl := &s.blk[b]
		bl.rows = uint32(rows)
		s.first[b] = uint64(p.ids[start])
		// Widths pass over id deltas (block-first ids stay raw in first),
		// frequencies, and position gaps (per-document bases excluded).
		idW, cntW, posW := uint8(1), uint8(1), uint8(1)
		for i := range rows {
			g := i + start
			c := p.off[g+1] - p.off[g]
			freqs[i] = c
			if w := packWidth(c); w > cntW {
				cntW = w
			}
			if i > 0 {
				d := uint64(p.ids[g]) - uint64(p.ids[g-1])
				if d >= 1<<32 {
					return nil
				}
				deltas[i-1] = uint32(d)
				if w := packWidth(uint32(d)); w > idW {
					idW = w
				}
			}
			base := p.off[g]
			for k := base + 1; k < base+c; k++ {
				if w := packWidth(p.pos[k] - p.pos[k-1]); w > posW {
					posW = w
				}
			}
		}
		bl.idW, bl.cntW, bl.posW = idW, cntW, posW
		// Pack pass: id deltas skip the block-first raw id; counts stream
		// whole; bases stay raw; gaps pack at posW with checkpoints.
		bl.idOff = uint32(len(s.ids))
		s.ids = packAppend(deltas[:rows-1], idW, s.ids)
		bl.cntOff = uint32(len(s.cnts))
		s.cnts = packAppend(freqs[:rows], cntW, s.cnts)
		bl.posOff = uint32(len(s.pos))
		// Gaps pack continuously at posW, so gap j sits at bit j*posW;
		// checkpoints record the running bit total every 16 rows.
		gapsBuf = gapsBuf[:0]
		posBits := uint32(0)
		for i := range rows {
			g := i + start
			c := freqs[i]
			s.bases = append(s.bases, p.pos[p.off[g]])
			if i%sealedCkptEvery == 0 {
				bl.posCkpt[i/sealedCkptEvery] = posBits
			}
			base := p.off[g]
			for k := base + 1; k < base+c; k++ {
				gapsBuf = append(gapsBuf, p.pos[k]-p.pos[k-1])
			}
			posBits += (c - 1) * uint32(posW)
		}
		s.pos = packAppend(gapsBuf, posW, s.pos)
	}
	return s
}

// sealedBytes reports the sealed footprint in bytes: blocks index, packed
// streams, raw bases, and directories. Tests pin space savings against it.
func (s *sealedPostings) sealedBytes() uint64 {
	if s == nil {
		return 0
	}
	// rows(4) + widths(4) + offsets(12) + checkpoints(8x4).
	const blockDir = 4 + 4 + 12 + sealedCkptPerBlock*4
	return uint64(8*len(s.first) +
		4*(len(s.ids)+len(s.cnts)+len(s.bases)+len(s.pos)) +
		blockDir*len(s.blk))
}

// docCount reports the posting list's document count in either layout.
func (p *postings) docCount() int {
	if p.sealed != nil {
		return int(p.sealed.n)
	}
	return len(p.ids)
}

// openSealed decodes a sealed list back to the open layout exactly, so Add
// and Remove can mutate a sealed list transparently.
func (p *postings) openSealed() {
	s := p.sealed
	if s == nil {
		return
	}
	n := int(s.n)
	ids := make([]DocID, 0, n)
	off := make([]uint32, 0, n+1)
	pos := make([]uint32, 0, s.occ)
	off = append(off, 0)
	for b := range s.blk {
		bl := &s.blk[b]
		idR := packReaderAt(s.ids, bl.idOff)
		cntR := packReaderAt(s.cnts, bl.cntOff)
		id := DocID(s.first[b])
		posBit := uint32(0)
		for k := uint32(0); k < bl.rows; k++ {
			if k > 0 {
				id += DocID(idR.next(bl.idW))
			}
			c := cntR.next(bl.cntW)
			ids = append(ids, id)
			row := len(ids) - 1
			cur := s.bases[row]
			pos = append(pos, cur)
			// A block of single-occurrence documents packs no gaps at
			// all; seeking an empty stream would read out of bounds.
			if c > 1 {
				posR := packReaderSeek(s.pos, bl.posOff, posBit)
				for t := uint32(1); t < c; t++ {
					cur += posR.next(bl.posW)
					pos = append(pos, cur)
				}
			}
			posBit += (c - 1) * uint32(bl.posW)
			off = append(off, uint32(len(pos)))
		}
	}
	p.ids, p.off, p.pos, p.sealed = ids, off, pos, nil
}

// sealedAppendIDs streams every document id into out with no temporary
// buffer: the running id plus one pulled delta per document. The document
// count is known, so out grows once instead of per doubling.
func (s *sealedPostings) sealedAppendIDs(out []DocID) []DocID {
	if need := len(out) + int(s.n); cap(out) < need {
		grown := make([]DocID, len(out), need)
		copy(grown, out)
		out = grown
	}
	for b := range s.blk {
		bl := &s.blk[b]
		idR := packReaderAt(s.ids, bl.idOff)
		id := DocID(s.first[b])
		out = append(out, id)
		for k := uint32(1); k < bl.rows; k++ {
			id += DocID(idR.next(bl.idW))
			out = append(out, id)
		}
	}
	return out
}

// sealedBlkCacheEntries bounds the decoded-block cache: phrase lookups
// alternate terms per document (one entry thrashes), while consecutive
// documents usually share the block. Four entries cover typical phrases;
// longer ones still bound every miss to one block decode.
const sealedBlkCacheEntries = 4

// sealedBlkCache is one cached block decode: the sealed object plus block
// number tag decoded ids and frequencies. Tags hold the sealed object
// pointer: sealed objects are immutable and unsealing swaps in a new one,
// so a tag hit can never read stale rows.
type sealedBlkCache struct {
	src *sealedPostings
	num int
	ids []DocID
	cnt []uint32
}

// sealedBlock decodes one block's ids and frequencies into index-owned
// staging, returning them. Phrase lookups visit candidate documents in
// ascending order, so same-term consecutive lookups usually hit the cached
// block and pay a binary search instead of a fresh prefix decode.
func (ix *Index) sealedBlock(s *sealedPostings, b int) ([]DocID, []uint32) {
	for i := range ix.blk {
		if ix.blk[i].src == s && ix.blk[i].num == b {
			return ix.blk[i].ids, ix.blk[i].cnt
		}
	}
	v := ix.blkVictim % sealedBlkCacheEntries
	ix.blkVictim++
	e := &ix.blk[v]
	bl := &s.blk[b]
	idR := packReaderAt(s.ids, bl.idOff)
	cntR := packReaderAt(s.cnts, bl.cntOff)
	ids := e.ids[:0]
	cnts := e.cnt[:0]
	id := DocID(s.first[b])
	for k := uint32(0); k < bl.rows; k++ {
		if k > 0 {
			id += DocID(idR.next(bl.idW))
		}
		ids = append(ids, id)
		cnts = append(cnts, cntR.next(bl.cntW))
	}
	e.ids, e.cnt = ids, cnts
	e.src, e.num = s, b
	return ids, cnts
}

// sealedFindRow locates doc's global row: binary search over the block
// index, then binary search over the cached block's ids. The cache fills
// on the second same-block miss: single probes (skewed conjunctions
// stepping through distinct blocks) scan linearly with early exit instead
// of paying a full block decode nobody reuses, while repeated lookups
// (phrase slots, gallop runs) decode once and search from then on.
func (ix *Index) sealedFindRow(s *sealedPostings, doc DocID) (int, bool) {
	lo, hi := 0, len(s.first)
	for lo < hi {
		m := lo + (hi-lo)/2
		if s.first[m] <= uint64(doc) {
			lo = m + 1
		} else {
			hi = m
		}
	}
	if lo == 0 {
		return 0, false
	}
	b := lo - 1
	for i := range ix.blk {
		if ix.blk[i].src == s && ix.blk[i].num == b {
			return binaryBlockRow(b, ix.blk[i].ids, doc)
		}
	}
	if ix.missSrc == s && ix.missBlk == b {
		ix.missSrc = nil
		ids, _ := ix.sealedBlock(s, b)
		return binaryBlockRow(b, ids, doc)
	}
	ix.missSrc, ix.missBlk = s, b
	return linearBlockRow(s, b, doc)
}

// binaryBlockRow searches decoded block ids for doc.
func binaryBlockRow(b int, ids []DocID, doc DocID) (int, bool) {
	lo, hi := 0, len(ids)
	for lo < hi {
		m := lo + (hi-lo)/2
		if ids[m] < doc {
			lo = m + 1
		} else {
			hi = m
		}
	}
	if lo < len(ids) && ids[lo] == doc {
		return b*sealedBlockRows + lo, true
	}
	return 0, false
}

// linearBlockRow scans one block's id deltas with early exit: prefix
// decoding forces a scan from the block start, but absence past doc stops
// it — usually well before the 128th row.
func linearBlockRow(s *sealedPostings, b int, doc DocID) (int, bool) {
	bl := &s.blk[b]
	idR := packReaderAt(s.ids, bl.idOff)
	id := DocID(s.first[b])
	for k := uint32(0); k < bl.rows; k++ {
		if k > 0 {
			id += DocID(idR.next(bl.idW))
		}
		if id == doc {
			return b*sealedBlockRows + int(k), true
		}
		if id > doc {
			return 0, false
		}
	}
	return 0, false
}

// sealedRowAt appends cached block (b, wrow)'s positions to out: the
// frequency comes from cached counts and the delta bit offset accumulates
// from the checkpoint over cached prefix counts, so only the row's own
// gaps decode.
func (ix *Index) sealedRowAt(s *sealedPostings, b, wrow int, out []uint32) []uint32 {
	bl := &s.blk[b]
	_, cnts := ix.sealedBlock(s, b)
	c := cnts[wrow]
	base := s.bases[b*sealedBlockRows+wrow]
	anchor := wrow / sealedCkptEvery * sealedCkptEvery
	posBit := bl.posCkpt[anchor/sealedCkptEvery]
	for i := anchor; i < wrow; i++ {
		posBit += (cnts[i] - 1) * uint32(bl.posW)
	}
	out = append(out, base)
	return s.decodeGaps(bl, posBit, base, c, out)
}

// decodeGaps appends (c-1) delta-decoded positions after base. A block of
// single-occurrence documents packs no gaps at all; seeking an empty
// stream would read out of bounds, hence the guard.
func (s *sealedPostings) decodeGaps(bl *sealedBlock, posBit, base, c uint32, out []uint32) []uint32 {
	if c > 1 {
		posR := packReaderSeek(s.pos, bl.posOff, posBit)
		cur := base
		for t := uint32(1); t < c; t++ {
			cur += posR.next(bl.posW)
			out = append(out, cur)
		}
	}
	return out
}

// sealedRowPositions appends doc's positions to stage and returns the new
// tail plus the extended stage, or false when doc is absent: one
// block-index search, one cache probe, one binary search, one checkpointed
// delta decode. It fuses sealedFindRow plus sealedRowAt so phrase lookups
// pay the index and cache exactly once. Callers must use the extended
// stage from here on: the tail aliases it.
func (ix *Index) sealedRowPositions(s *sealedPostings, doc DocID, stage []uint32) ([]uint32, []uint32, bool) {
	lo, hi := 0, len(s.first)
	for lo < hi {
		m := lo + (hi-lo)/2
		if s.first[m] <= uint64(doc) {
			lo = m + 1
		} else {
			hi = m
		}
	}
	if lo == 0 {
		return nil, stage, false
	}
	b := lo - 1
	for i := range ix.blk {
		if ix.blk[i].src == s && ix.blk[i].num == b {
			row, ok := binaryBlockRow(b, ix.blk[i].ids, doc)
			if !ok {
				return nil, stage, false
			}
			base := len(stage)
			out := ix.sealedRowAt(s, b, row%sealedBlockRows, stage)
			return out[base:], out, true
		}
	}
	// Miss: decode the block unconditionally. Unlike bare membership
	// probes, a positions lookup always wants the block's counts next,
	// so the fill always pays; alternating terms would defeat a
	// miss-twice heuristic here.
	ids, _ := ix.sealedBlock(s, b)
	row, ok := binaryBlockRow(b, ids, doc)
	if !ok {
		return nil, stage, false
	}
	base := len(stage)
	out := ix.sealedRowAt(s, b, row%sealedBlockRows, stage)
	return out[base:], out, true
}

// sealedTermSpans expands a sealed term posting list to span hits, one
// block at a time: id and count streams decode in lockstep while the delta
// bit offset accumulates sequentially, so no checkpoint seeks fire. posBuf
// is index-owned staging reused per row.
func (ix *Index) sealedTermSpans(s *sealedPostings, out []spanHit, posBuf []uint32) []spanHit {
	for b := range s.blk {
		bl := &s.blk[b]
		idR := packReaderAt(s.ids, bl.idOff)
		cntR := packReaderAt(s.cnts, bl.cntOff)
		id := DocID(s.first[b])
		posBit := uint32(0)
		for k := uint32(0); k < bl.rows; k++ {
			if k > 0 {
				id += DocID(idR.next(bl.idW))
			}
			c := cntR.next(bl.cntW)
			row := b*sealedBlockRows + int(k)
			pos := posBuf[:0]
			cur := s.bases[row]
			pos = append(pos, cur)
			if c > 1 {
				posR := packReaderSeek(s.pos, bl.posOff, posBit)
				for t := uint32(1); t < c; t++ {
					cur += posR.next(bl.posW)
					pos = append(pos, cur)
				}
			}
			for _, pp := range pos {
				out = append(out, spanHit{doc: id, start: pp, end: pp + 1})
			}
			posBit += (c - 1) * uint32(bl.posW)
		}
	}
	return out
}

// sealedScoreGather stages a sealed term's document ids into the index-owned
// decIDs buffer while gathering frequencies and lengths into tf/dl, so the
// shared BM25 tail runs unchanged. Callers must not retain decIDs past the
// drain: later sealed calls reuse it.
func (ix *Index) sealedScoreGather(
	s *sealedPostings,
	tf, dl []float64,
) ([]DocID, []float64, []float64) {
	ids := ix.decIDs[:0]
	for b := range s.blk {
		bl := &s.blk[b]
		idR := packReaderAt(s.ids, bl.idOff)
		cntR := packReaderAt(s.cnts, bl.cntOff)
		id := DocID(s.first[b])
		for k := uint32(0); k < bl.rows; k++ {
			if k > 0 {
				id += DocID(idR.next(bl.idW))
			}
			c := cntR.next(bl.cntW)
			ids = append(ids, id)
			tf = append(tf, float64(c))
			dl = append(dl, float64(ix.docLength(id)))
		}
	}
	ix.decIDs = ids
	return ids, tf, dl
}
