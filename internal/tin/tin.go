// Package tin owns VibeDB's full-text search core: a zero-allocation
// tokenizer with PlanetScale-TIN-compatible normalization (case and accent
// folding, 0-based token positions), a positional inverted index, a TINQL
// query evaluator, and BM25 top-K scoring.
//
// The hot paths are allocation-free in steady state: the tokenizer streams
// token hashes to a callback, Match and Score reuse index-owned scratch, and
// the bulk ASCII fold has a GOEXPERIMENT=simd vector kernel with a scalar
// fallback (see fold_wide.go, selected per arch by fold_enable_amd64.go and
// fold_enable_arm64.go). The package depends only on the standard library.
//
// Landed surface: terms, phrases with slop, AND/OR/AND NOT, match-all,
// BM25, fuzzy, wildcard, ranges, MATCHES, proximity, span relations,
// positional filters, SQL DDL (CREATE INDEX ... USING tin) with the ==>
// operator, heap and durable store persistence, the Go Collection API, and
// pgwire exposure.
package tin

import "sync"

// DocID identifies an indexed document. It is opaque to the index: callers
// map it to their row identity.
type DocID uint64

// Scored is one scored hit, sorted by descending Score (ties by DocID).
type Scored struct {
	Doc   DocID
	Score float64
}

// BM25。小: standard Okapi parameters.
const (
	bm25K1 = 1.2
	bm25B  = 0.75
)

// Index is an in-memory positional inverted index over folded term hashes.
//
// The zero value is not usable; build with NewIndex. An Index is safe for
// concurrent use: Add and Remove take the write lock, Match and Score take
// the same lock because they may lazily sort postings first.
type Index struct {
	mu sync.Mutex

	docs     map[DocID]docMeta
	post     map[uint64]*postings
	nDocs    int
	tokens   uint64
	sorted   bool
	foldBuf  []byte
	spellBuf []byte

	// dict maps term hashes to their first-seen folded spelling. spellIdx
	// orders the same vocabulary by spelling for wildcard, fuzzy, range,
	// and regex expansion. Spellings are never pruned on Remove: a removed
	// term keeps its spelling but its posting list is gone, so expansions
	// over it simply match nothing.
	dict     map[uint64]string
	spellIdx []spellEntry
	// maxSpell bounds fuzzy DP scratch; it only grows with the vocabulary.
	maxSpell int

	// scratchS stages one Score call's hits; scoring only appends, so
	// sharing it across the query tree is safe.
	scratchS []Scored
	// scoreTF/scoreDL/scoreOut stage term frequencies, document lengths, and
	// kernel outputs for BM25; gathered scalar, computed wide, drained
	// immediately, so sequential scoring calls safely share them.
	scoreTF  []float64
	scoreDL  []float64
	scoreOut []float64
	// topHeap stages the bounded worst-first heap for single-term top-K
	// scoring; capacity persists across calls under the index lock.
	topHeap []Scored
	// andKeep/andSums/andDL stage a selective AND's intersection, its
	// accumulated scores, and its lengths; andPTF/andPDL/andPIdx stage
	// one kid's paired-slot frequencies, lengths, and keep indexes.
	andKeep []DocID
	andSums []float64
	andDL   []float64
	andPTF  []float64
	andPDL  []float64
	andPIdx []int
	// decIDs stages sealed document ids for scoring gathers; decPos stages
	// one sealed row's positions for span expansion. Both are sequential
	// staging under the index lock, never retained across calls.
	decIDs []DocID
	decPos []uint32
	// blk caches decoded sealed blocks for random lookups (phrase slots)
	// with a round-robin victim; blkVictim counts evictions. Same lock
	// discipline as the decode staging above.
	blk       [sealedBlkCacheEntries]sealedBlkCache
	blkVictim uint64
	// termLists stages resolved postings for layout-aware conjunctions,
	// reused across calls under the same lock.
	termLists []*postings
	// missSrc/missBlk remembers the last uncached block probe so the
	// second same-block lookup fills the block cache (see sealedFindRow).
	missSrc *sealedPostings
	missBlk int
	// docBase/docLens index document lengths densely while ids arrive
	// dense (append for the next id, overwrite on replace); the first
	// sparse jump drops the array and the docs map stays source of
	// truth. Lengths are only read for live posting-derived documents,
	// so a zeroed removal slot is never observed.
	docBase uint64
	docLens []uint32
	// minDocLen is the shortest length ever noted. Removals can only
	// leave it stale-low, which keeps block score upper bounds valid
	// (conservative); clearing every document re-establishes it.
	minDocLen uint32
}

// noteDocLength tracks id's length in the dense array. Call with the
// document not yet inserted so an empty map plus a nil array means the
// first document ever (which establishes the base).
func (ix *Index) noteDocLength(id DocID, length uint32) {
	if len(ix.docs) == 0 || length < ix.minDocLen {
		ix.minDocLen = length
	}
	if ix.docLens == nil {
		if len(ix.docs) != 0 {
			return
		}
		ix.docBase = uint64(id)
		ix.docLens = make([]uint32, 0, 64)
	}
	if d := uint64(id) - ix.docBase; d < uint64(len(ix.docLens)) {
		ix.docLens[d] = length
	} else if d == uint64(len(ix.docLens)) {
		ix.docLens = append(ix.docLens, length)
	} else {
		ix.docLens = nil
	}
}

// docLength returns id's length without hashing: array probe while dense,
// map fallback once sparse.
func (ix *Index) docLength(id DocID) uint32 {
	if ix.docLens != nil {
		if d := uint64(id) - ix.docBase; d < uint64(len(ix.docLens)) {
			return ix.docLens[d]
		}
	}
	return ix.docs[id].length
}

// spellEntry is one vocabulary row ordered by spelling.
type spellEntry struct {
	spell string
	hash  uint64
}

// docMeta tracks per-document statistics for BM25 and the term list needed
// for exact Remove.
type docMeta struct {
	length uint32
	terms  []uint64
}

// postings is one term's posting list. ids is sorted exactly when the
// index's sorted flag holds; pos holds per-document positions with offsets
// in off (off has len(ids)+1), so the term frequency of entry i is always
// off[i+1]-off[i] with no separate frequency array. A sealed list packs the
// same rows bit-packed and delta-coded (see postings_sealed.go) with the
// open arrays released; readers branch on sealed, mutators unseal first.
type postings struct {
	ids []DocID
	off []uint32
	pos []uint32
	sealed *sealedPostings
}

// NewIndex returns an empty Index.
func NewIndex() *Index {
	return &Index{
		docs: make(map[DocID]docMeta),
		post: make(map[uint64]*postings),
		dict: make(map[uint64]string),
	}
}

// Docs returns the number of indexed documents.
func (ix *Index) Docs() int {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	return ix.nDocs
}

// Add indexes text under id, replacing any previous document with the same
// id. Text is normalized exactly like query terms (case and accent folding),
// so index and query sides agree by construction.
//
// Documents of 256 bytes or more fold through the vector kernel into a reused
// scratch buffer; shorter documents scan directly. Either way the steady
// state allocates only for genuinely new terms, documents, and posting
// growth — never per byte or per token.
func (ix *Index) Add(id DocID, text string) {
	ix.mu.Lock()
	defer ix.mu.Unlock()

	if old, ok := ix.docs[id]; ok {
		ix.removeLocked(id, old)
	}

	var length uint32
	var terms []uint64
	ix.spellBuf = ix.spellBuf[:0]
	spell := &ix.spellBuf
	emit := func(hash uint64, pos uint32, spelling []byte) {
		length++
		terms = ix.appendLocked(id, hash, pos, terms)
		ix.learnLocked(hash, spelling)
	}
	if len(text) >= simdFoldThreshold && len(text) <= maxFoldDoc {
		if cap(ix.foldBuf) < len(text) {
			ix.foldBuf = make([]byte, len(text))
		}
		buf := ix.foldBuf[:len(text)]
		foldASCII(buf, text)
		scanFoldedRec(buf, spell, emit)
	} else {
		scanStringRec(text, spell, emit)
	}
	ix.noteDocLength(id, length)
	ix.docs[id] = docMeta{length: length, terms: terms}
	ix.nDocs++
	ix.tokens += uint64(length)
	ix.sorted = false
}

// appendLocked appends one token occurrence and returns the document's term
// list (extended when hash is new to the document, for exact Remove).
func (ix *Index) appendLocked(id DocID, hash uint64, pos uint32, terms []uint64) []uint64 {
	p := ix.post[hash]
	if p == nil {
		// off holds one boundary per entry plus the initial zero, so
		// positionsOf(row) is always pos[off[row]:off[row+1]].
		p = &postings{off: []uint32{0}}
		ix.post[hash] = p
	}
	// Mutating a sealed list decodes it in place first; Seal is a
	// build-final optimization, not a lock.
	p.openSealed()
	if n := len(p.ids); n > 0 && p.ids[n-1] == id {
		p.pos = append(p.pos, pos)
		p.off[n] = uint32(len(p.pos))
		return terms
	}
	p.ids = append(p.ids, id)
	p.pos = append(p.pos, pos)
	p.off = append(p.off, uint32(len(p.pos)))
	return append(terms, hash)
}

// Remove drops id from the index, reporting whether it was present.
func (ix *Index) Remove(id DocID) bool {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	old, ok := ix.docs[id]
	if !ok {
		return false
	}
	ix.removeLocked(id, old)
	return true
}

func (ix *Index) removeLocked(id DocID, old docMeta) {
	// Removal binary-searches posting lists, so an index that never
	// matched (never sorted) must sort first: out-of-order Adds leave
	// lists unsorted, and searching those misses live rows, ghosting
	// replaced documents. The sort is amortized — the next Match would
	// pay it anyway.
	ix.ensureSorted()
	for _, hash := range old.terms {
		p := ix.post[hash]
		if p == nil {
			continue
		}
		p.openSealed()
		n := len(p.ids)
		lo, hi := 0, n
		for lo < hi {
			m := lo + (hi-lo)/2
			if p.ids[m] < id {
				lo = m + 1
			} else {
				hi = m
			}
		}
		if lo >= n || p.ids[lo] != id {
			continue
		}
		start, end := p.off[lo], p.off[lo+1]
		p.pos = append(p.pos[:start], p.pos[end:]...)
		width := end - start
		for i := lo + 1; i <= n; i++ {
			p.off[i] -= width
		}
		p.ids = append(p.ids[:lo], p.ids[lo+1:]...)
		p.off = append(p.off[:lo], p.off[lo+1:]...)
		if len(p.ids) == 0 {
			delete(ix.post, hash)
		}
	}
	delete(ix.docs, id)
	if ix.docLens != nil {
		if d := uint64(id) - ix.docBase; d < uint64(len(ix.docLens)) {
			ix.docLens[d] = 0
		}
	}
	ix.nDocs--
	ix.tokens -= uint64(old.length)
}

// ensureSorted orders every posting list by document. Adds append in
// document-id order only by luck, so the first Match or Score after a batch
// of Adds pays one amortized sort.
func (ix *Index) ensureSorted() {
	if ix.sorted {
		return
	}
	for _, p := range ix.post {
		// Sealed lists are sorted by construction; mutators unseal.
		if p.sealed != nil || len(p.ids) < 2 {
			continue
		}
		sortPostings(p)
	}
	ix.sorted = true
}

// Seal compresses every posting list bit-packed and delta-coded (see
// postings_sealed.go) and releases the open arrays. Seal is terminal for a
// build-final index: later Adds transparently unseal the lists they touch.
// A list whose id gap escapes 32 bits stays open. Durable generation builds
// seal after their single scan; heap callers seal explicitly.
func (ix *Index) Seal() {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	ix.ensureSorted()
	for _, p := range ix.post {
		if p.sealed != nil {
			continue
		}
		// Adopt the packed form only when it shrinks: the block
		// directory costs ~52 bytes, so rare-term lists stay open.
		// Compression never regresses space, by construction.
		if s := sealPostings(p); s != nil && s.sealedBytes() < openBytesOf(p) {
			p.sealed = s
			p.ids, p.off, p.pos = nil, nil, nil
		}
	}
	ix.sorted = true
}

// openBytesOf tallies one open list: 8 bytes per id, 4 per offset
// boundary, 4 per position.
func openBytesOf(p *postings) uint64 {
	return 8*uint64(len(p.ids)) + 4*uint64(len(p.off)) + 4*uint64(len(p.pos))
}

// sortPostings orders one posting list by document. Adds usually arrive in
// document order already, so insertion sort is effectively linear; the
// general case stays quadratic but runs only on the amortized sort path,
// never per query.
func sortPostings(p *postings) {
	n := len(p.ids)
	ord := make([]int, n)
	for i := range ord {
		ord[i] = i
	}
	for i := 1; i < n; i++ {
		j := i
		for j > 0 && p.ids[ord[j-1]] > p.ids[ord[j]] {
			ord[j-1], ord[j] = ord[j], ord[j-1]
			j--
		}
	}
	if isIdentity(ord) {
		return
	}
	ids := make([]DocID, n)
	widths := make([]uint32, n)
	total := uint32(0)
	for i, o := range ord {
		ids[i] = p.ids[o]
		widths[i] = p.off[o+1] - p.off[o]
		total += widths[i]
	}
	pos := make([]uint32, 0, total)
	off := make([]uint32, n+1)
	for i, o := range ord {
		off[i] = uint32(len(pos))
		pos = append(pos, p.pos[p.off[o]:p.off[o+1]]...)
	}
	off[n] = uint32(len(pos))
	p.ids, p.pos, p.off = ids, pos, off
}

// isIdentity reports whether ord is the identity permutation.
func isIdentity(ord []int) bool {
	for i, o := range ord {
		if o != i {
			return false
		}
	}
	return true
}
