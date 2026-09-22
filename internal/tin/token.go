package tin

import (
	"slices"
	"unicode"
	"unicode/utf8"
)

// Hashing is FNV-1a/64 streamed per folded byte: no per-token buffer, no
// per-token string, one integer of state.
const (
	fnvOffset = 14695981039346656037
	fnvPrime  = 1099511628211
)

func mix(h uint64, b byte) uint64 {
	return (h ^ uint64(b)) * fnvPrime
}

// scanString streams (hash, 0-based position) pairs for text's tokens,
// folding on the fly. It allocates nothing: substrings share text's backing
// and runes decode into stack values.
func scanString(text string, emit func(hash uint64, pos uint32)) {
	scanStringRec(text, nil, func(h uint64, p uint32, _ []byte) { emit(h, p) })
}

// scanStringRec is scanString with optional spelling capture: when spell is
// non-nil, each emit also carries the token's folded bytes, valid only for
// the call's duration (they alias *spell, which the caller reuses).
func scanStringRec(text string, spell *[]byte, emit func(hash uint64, pos uint32, spelling []byte)) {
	var s scanner
	s.emit = emit
	s.spell = spell
	for i := 0; i < len(text); {
		c := text[i]
		if c < utf8.RuneSelf {
			b := foldByte(c)
			if wordClass[b] {
				s.take(b)
			} else {
				s.flush()
			}
			i++
			continue
		}
		i += s.runeString(text[i:])
	}
	s.flush()
}

// scanFolded streams tokens from buf, whose ASCII bytes are already folded
// (see foldASCII). Multibyte sequences pass the fold through, so the rune
// path decodes them identically to scanString.
func scanFolded(buf []byte, emit func(hash uint64, pos uint32)) {
	scanFoldedRec(buf, nil, func(h uint64, p uint32, _ []byte) { emit(h, p) })
}

// scanFoldedRec is scanFolded with optional spelling capture.
func scanFoldedRec(buf []byte, spell *[]byte, emit func(hash uint64, pos uint32, spelling []byte)) {
	var s scanner
	s.emit = emit
	s.spell = spell
	for i := 0; i < len(buf); {
		c := buf[i]
		if c < utf8.RuneSelf {
			if wordClass[c] {
				s.take(c)
			} else {
				s.flush()
			}
			i++
			continue
		}
		i += s.runeBytes(buf[i:])
	}
	s.flush()
}

// FoldTerm normalizes one query term to its index hash and reports how many
// tokens the input scanned as. Inputs that analyze to no tokens (empty,
// whitespace, bare punctuation) return n == 0 and match nothing per TIN's
// rule; the hash is meaningless then and must not be used.
func FoldTerm(term string) (hash uint64, n int) {
	scanString(term, func(h uint64, _ uint32) {
		if n == 0 {
			hash = h
		}
		n++
	})
	return hash, n
}

// scanner holds one scan's running state.
type scanner struct {
	emit   func(hash uint64, pos uint32, spelling []byte)
	spell  *[]byte
	mark   int
	h      uint64
	pos    uint32
	inWord bool
}

// scanPairs appends text's (hash, 0-based position) pairs to out, folding on
// the fly exactly like scanString but with no emit callback. A func argument
// stored in the scanner escapes to the heap — one closure plus its captured
// slice per call — while this threads the slice by value through a plain
// data struct, so warm callers allocate only when out must grow.
// TestScanPairsAgreesWithScanString locks the two together, including the
// rune fold path.
// scanTab fuses the ASCII fold and word class into one lookup: 0 means
// separator, anything else is the folded byte to hash. It builds in
// init from foldByte/wordClass so the two can never disagree; a var
// initializer would run before wordClass's own init and read it empty.
// Multibyte sequences never consult the table (their bytes are not
// independently foldable).
var scanTab [256]byte

func init() {
	for c := 0; c < 256; c++ {
		if b := foldByte(byte(c)); wordClass[b] {
			scanTab[c] = b
		}
	}
}

func scanPairs(text string, out []tokPos) []tokPos {
	s := pairScanner{out: out, h: fnvOffset}
	i, n := 0, len(text)
	for i < n {
		c := text[i]
		if c >= utf8.RuneSelf {
			r, size := utf8.DecodeRuneInString(text[i:])
			s.feed(r, size)
			i += size
			continue
		}
		if b := scanTab[c]; b == 0 {
			s.flush()
			i++
			continue
		}
		// ASCII word run: the per-byte in-word branch hoists out, and
		// the run may still continue through a multibyte letter, so
		// the token stays open in s (flushed by the next separator,
		// a breaking rune, or the trailing flush) exactly as before.
		if !s.inWord {
			s.h = fnvOffset
			s.inWord = true
		}
		h := s.h
		for i < n {
			c := text[i]
			if c >= utf8.RuneSelf {
				break
			}
			b := scanTab[c]
			if b == 0 {
				break
			}
			h = mix(h, b)
			i++
		}
		s.h = h
	}
	s.flush()
	return s.out
}

// sortTokPos orders pairs by (hash, pos), the document order transient
// matching and scoring both require.
func sortTokPos(pairs []tokPos) {
	slices.SortFunc(pairs, func(a, b tokPos) int {
		if a.hash != b.hash {
			if a.hash < b.hash {
				return -1
			}
			return 1
		}
		if a.pos < b.pos {
			return -1
		}
		if a.pos > b.pos {
			return 1
		}
		return 0
	})
}

// pairScanner is scanString's state machine with the emit callback replaced
// by an appended slice. It holds data only, so callers keep it on the stack.
type pairScanner struct {
	out    []tokPos
	h      uint64
	pos    uint32
	inWord bool
}

// take feeds one folded byte into the open token, starting one first.
func (s *pairScanner) take(b byte) {
	if !s.inWord {
		s.h = fnvOffset
		s.inWord = true
	}
	s.h = mix(s.h, b)
}

// flush closes the open token, if any.
func (s *pairScanner) flush() {
	if !s.inWord {
		return
	}
	s.out = append(s.out, tokPos{hash: s.h, pos: s.pos})
	s.pos++
	s.inWord = false
	s.h = fnvOffset
}

// feed folds one decoded rune into the running hash. Bytes the fold cannot
// represent start or continue a token by Unicode letter/digit property;
// anything else separates. Mirrors scanner.feed exactly.
func (s *pairScanner) feed(r rune, size int) {
	if r < utf8.RuneSelf {
		if b := foldByte(byte(r)); wordClass[b] {
			s.take(b)
		} else {
			s.flush()
		}
		return
	}
	if r < 256 {
		if f := latinFold[byte(r)]; f != 0 {
			s.take(f)
			return
		}
	}
	if r == utf8.RuneError && size <= 1 {
		s.flush()
		return
	}
	l := unicode.ToLower(r)
	if !unicode.IsLetter(l) && !unicode.IsDigit(l) {
		s.flush()
		return
	}
	var enc [utf8.UTFMax]byte
	n := utf8.EncodeRune(enc[:], l)
	for _, b := range enc[:n] {
		s.take(b)
	}
}

// take feeds one folded byte into the open token, starting one first.
func (s *scanner) take(b byte) {
	if !s.inWord {
		s.h = fnvOffset
		s.inWord = true
		if s.spell != nil {
			s.mark = len(*s.spell)
		}
	}
	if s.spell != nil {
		*s.spell = append(*s.spell, b)
	}
	s.h = mix(s.h, b)
}

// flush closes the open token, if any.
func (s *scanner) flush() {
	if !s.inWord {
		return
	}
	var spelling []byte
	if s.spell != nil {
		spelling = (*s.spell)[s.mark:]
	}
	s.emit(s.h, s.pos, spelling)
	s.pos++
	s.inWord = false
	s.h = fnvOffset
}

// feed folds one decoded rune into the running hash, returning the rune's
// byte size. Bytes the fold cannot represent start or continue a token by
// Unicode letter/digit property; anything else separates.
func (s *scanner) feed(r rune, size int) int {
	if r < utf8.RuneSelf {
		// Unreachable from the multibyte call sites (ASCII never reaches
		// here); kept symmetric so each path owns its fold rule.
		if b := foldByte(byte(r)); wordClass[b] {
			s.take(b)
		} else {
			s.flush()
		}
		return size
	}
	if r < 256 {
		if f := latinFold[byte(r)]; f != 0 {
			s.take(f)
			return size
		}
	}
	if r == utf8.RuneError && size <= 1 {
		s.flush()
		return size
	}
	l := unicode.ToLower(r)
	if !unicode.IsLetter(l) && !unicode.IsDigit(l) {
		s.flush()
		return size
	}
	var enc [utf8.UTFMax]byte
	n := utf8.EncodeRune(enc[:], l)
	for _, b := range enc[:n] {
		s.take(b)
	}
	return size
}

// runeString decodes one rune at text and feeds it, returning bytes consumed.
func (s *scanner) runeString(text string) int {
	r, size := utf8.DecodeRuneInString(text)
	return s.feed(r, size)
}

// runeBytes decodes one rune at buf and feeds it, returning bytes consumed.
func (s *scanner) runeBytes(buf []byte) int {
	r, size := utf8.DecodeRune(buf)
	return s.feed(r, size)
}
