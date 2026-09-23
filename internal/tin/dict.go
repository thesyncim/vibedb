package tin

import (
	"regexp"
	"sort"
	"unicode"
	"unicode/utf8"
)

// Term dictionary and query-time expansion. The dictionary maps folded term
// hashes to spellings for wildcard, fuzzy, range, and regex matching. All
// expansion runs at parse time (see ParseTINQL): it allocates per query
// shape, while evaluation over the expanded hashes stays steady-state.

// learnLocked records hash's spelling on first sight: a string copy plus one
// sorted vocabulary insert. Steady-state indexing (known terms) does neither.
func (ix *Index) learnLocked(hash uint64, spelling []byte) {
	if _, ok := ix.dict[hash]; ok {
		return
	}
	spell := string(spelling)
	ix.dict[hash] = spell
	at := sort.Search(len(ix.spellIdx), func(i int) bool {
		return ix.spellIdx[i].spell >= spell
	})
	ix.spellIdx = append(ix.spellIdx, spellEntry{})
	copy(ix.spellIdx[at+1:], ix.spellIdx[at:])
	ix.spellIdx[at] = spellEntry{spell: spell, hash: hash}
	if len(spell) > ix.maxSpell {
		ix.maxSpell = len(spell)
	}
}

// spellOf folds raw (unescaped) term text to its first token's spelling,
// reporting the token count exactly like FoldTerm.
func spellOf(raw string) (spell []byte, n int) {
	var buf []byte
	var first []byte
	scanStringRec(raw, &buf, func(_ uint64, _ uint32, spelling []byte) {
		if n == 0 {
			first = append(first[:0], spelling...)
		}
		n++
	})
	return first, n
}

// foldPattern folds an (already unescaped) wildcard, fuzzy, or regex pattern
// the way index spellings are folded, without tokenizing: every rune passes
// through, so `*`, `?`, and regex metacharacters survive.
func foldPattern(pat string) string {
	var out []byte
	for _, r := range pat {
		switch {
		case r < utf8.RuneSelf:
			b := byte(r)
			out = append(out, foldByte(b))
		case r < 256:
			if f := latinFold[byte(r)]; f != 0 {
				out = append(out, f)
			} else {
				out = append(out, string(r)...)
			}
		default:
			out = append(out, string(unicode.ToLower(r))...)
		}
	}
	return string(out)
}

// posted reports whether hash has a live posting list.
func (ix *Index) posted(hash uint64) bool {
	p := ix.post[hash]
	return p != nil && p.docCount() > 0
}

// expandWildcard returns hashes of posted terms matching a folded `*`/`?`
// pattern. The pattern compiles once per call; matching is a full-term
// anchored match over spellings.
func (ix *Index) expandWildcard(folded string) ([]uint64, error) {
	re, err := globToRegexp(folded)
	if err != nil {
		return nil, err
	}
	return ix.expandRegexp(re), nil
}

// globToRegexp translates a folded glob to an anchored regular expression.
// The pattern is already folded, so literal bytes match spellings exactly;
// regex metacharacters in literals are quoted.
func globToRegexp(folded string) (*regexp.Regexp, error) {
	var sb []byte
	sb = append(sb, '^')
	for i := 0; i < len(folded); {
		switch c := folded[i]; c {
		case '*':
			sb = append(sb, '.', '*')
			i++
		case '?':
			sb = append(sb, '.')
			i++
		case '\\', '.', '+', '(', ')', '|', '^', '$', '[', ']', '{', '}':
			sb = append(sb, '\\', c)
			i++
		default:
			r, size := utf8.DecodeRuneInString(folded[i:])
			sb = append(sb, folded[i:i+size]...)
			_ = r
			i += size
		}
	}
	sb = append(sb, '$')
	return regexp.Compile(string(sb))
}

// expandRegexp returns hashes of posted terms fully matched by re.
func (ix *Index) expandRegexp(re *regexp.Regexp) []uint64 {
	var out []uint64
	for _, e := range ix.spellIdx {
		if re.MatchString(e.spell) && ix.posted(e.hash) {
			out = append(out, e.hash)
		}
	}
	return out
}

// expandFuzzy returns hashes of posted terms within edit distance dist of
// spell, sharing its first prefix bytes (the stable prefix, default 1).
func (ix *Index) expandFuzzy(spell string, prefix, dist int) []uint64 {
	if prefix > len(spell) {
		prefix = len(spell)
	}
	prev := make([]int, len(spell)+1)
	cur := make([]int, len(spell)+1)
	var out []uint64
	for _, e := range ix.spellIdx {
		if len(e.spell) < prefix || e.spell[:prefix] != spell[:prefix] {
			continue
		}
		if d := len(e.spell) - len(spell); d < -dist || d > dist {
			continue
		}
		if levenshteinBounded(spell, e.spell, dist, prev, cur) <= dist && ix.posted(e.hash) {
			out = append(out, e.hash)
		}
	}
	return out
}

// levenshteinBounded is byte-wise Levenshtein capped at max: it returns a
// value > max when the distance exceeds it, without finishing the matrix.
// prev and cur are caller scratch of len(a)+1.
func levenshteinBounded(a, b string, max int, prev, cur []int) int {
	for i := range prev[:len(a)+1] {
		prev[i] = i
	}
	for j := 1; j <= len(b); j++ {
		cur[0] = j
		rowMin := j
		for i := 1; i <= len(a); i++ {
			cost := 0
			if a[i-1] != b[j-1] {
				cost = 1
			}
			del := prev[i] + 1
			ins := cur[i-1] + 1
			sub := prev[i-1] + cost
			m := del
			if ins < m {
				m = ins
			}
			if sub < m {
				m = sub
			}
			cur[i] = m
			if m < rowMin {
				rowMin = m
			}
		}
		if rowMin > max {
			return max + 1
		}
		prev, cur = cur, prev
	}
	return prev[len(a)]
}

// expandRange returns hashes of posted terms with lo <= spell <= hi in
// spelling order; either bound may be open (nil).
func (ix *Index) expandRange(lo, hi *string) []uint64 {
	start := 0
	if lo != nil {
		start = sort.Search(len(ix.spellIdx), func(i int) bool {
			return ix.spellIdx[i].spell >= *lo
		})
	}
	end := len(ix.spellIdx)
	if hi != nil {
		end = sort.Search(len(ix.spellIdx), func(i int) bool {
			return ix.spellIdx[i].spell > *hi
		})
	}
	var out []uint64
	for _, e := range ix.spellIdx[start:end] {
		if ix.posted(e.hash) {
			out = append(out, e.hash)
		}
	}
	return out
}
