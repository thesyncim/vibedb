// Package tin owns VibeDB's full-text search core: a zero-allocation
// tokenizer with PlanetScale-TIN-compatible normalization (case and accent
// folding, 0-based token positions), a positional inverted index, a TINQL
// query evaluator, and BM25 top-K scoring.
//
// The hot paths are allocation-free in steady state: the tokenizer streams
// token hashes to a callback, Match and Score reuse index-owned scratch, and
// the bulk ASCII fold has a GOEXPERIMENT=simd vector kernel with a scalar
// fallback (see fold_simd.go and fold_nosimd.go). The package depends only on
// the standard library.
//
// Roadmap: this slice lands the core (terms, phrases with slop, AND/OR/AND
// NOT, match-all, BM25). Later slices add the TINQL surface (fuzzy,
// wildcard, ranges, MATCHES, proximity, span relations, positional filters),
// SQL DDL (CREATE INDEX ... USING tin) with the ==> operator, store
// persistence, the Go Collection API, and pgwire exposure.
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

	docs    map[DocID]*docMeta
	post    map[uint64]*postings
	nDocs   int
	tokens  uint64
	sorted  bool
	foldBuf []byte

	// scratchS stages one Score call's hits; scoring only appends, so
	// sharing it across the query tree is safe.
	scratchS []Scored
}

// docMeta tracks per-document statistics for BM25 and the term list needed
// for exact Remove.
type docMeta struct {
	length uint32
	terms  []uint64
}

// postings is one term's posting list. ids is sorted exactly when the
// index's sorted flag holds; freq parallels ids; pos holds per-document
// positions with offsets in off (off has len(ids)+1).
type postings struct {
	ids  []DocID
	freq []uint32
	off  []uint32
	pos  []uint32
}

// NewIndex returns an empty Index.
func NewIndex() *Index {
	return &Index{
		docs: make(map[DocID]*docMeta),
		post: make(map[uint64]*postings),
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
	emit := func(hash uint64, pos uint32) {
		length++
		terms = ix.appendLocked(id, hash, pos, terms)
	}
	if len(text) >= simdFoldThreshold && len(text) <= maxFoldDoc {
		if cap(ix.foldBuf) < len(text) {
			ix.foldBuf = make([]byte, len(text))
		}
		buf := ix.foldBuf[:len(text)]
		foldASCII(buf, text)
		scanFolded(buf, emit)
	} else {
		scanString(text, emit)
	}
	ix.docs[id] = &docMeta{length: length, terms: terms}
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
	if n := len(p.ids); n > 0 && p.ids[n-1] == id {
		p.freq[n-1]++
		p.pos = append(p.pos, pos)
		p.off[n] = uint32(len(p.pos))
		return terms
	}
	p.ids = append(p.ids, id)
	p.freq = append(p.freq, 1)
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

func (ix *Index) removeLocked(id DocID, old *docMeta) {
	for _, hash := range old.terms {
		p := ix.post[hash]
		if p == nil {
			continue
		}
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
		p.freq = append(p.freq[:lo], p.freq[lo+1:]...)
		p.off = append(p.off[:lo], p.off[lo+1:]...)
		if len(p.ids) == 0 {
			delete(ix.post, hash)
		}
	}
	delete(ix.docs, id)
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
		if len(p.ids) < 2 {
			continue
		}
		sortPostings(p)
	}
	ix.sorted = true
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
	freq := make([]uint32, n)
	widths := make([]uint32, n)
	total := uint32(0)
	for i, o := range ord {
		ids[i] = p.ids[o]
		freq[i] = p.freq[o]
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
	p.ids, p.freq, p.pos, p.off = ids, freq, pos, off
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
