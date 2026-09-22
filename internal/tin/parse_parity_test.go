package tin

import (
	"testing"
)

// PlanetScale parity: positions are 1-based in the query language.
// IN WORDS x TO y keeps 1-based positions x..y; WITHIN n keeps spans of at
// most n positions including endpoints; FIRST/LAST count tokens from the
// edges. Every expectation below mirrors the TINQL reference examples.
func parityPositionCorpus(t *testing.T) *Index {
	t.Helper()
	ix := NewIndex()
	ix.Add(1, "w0 w1 w2 w3 w4 w5")
	return ix
}

func parityMatch(t *testing.T, ix *Index, input string) []DocID {
	t.Helper()
	q, err := ix.ParseTINQL(input)
	if err != nil {
		t.Fatalf("%s: unexpected error %v", input, err)
	}
	return ix.Match(q, nil)
}

func parityMatchWant(t *testing.T, ix *Index, input string, want []DocID) {
	t.Helper()
	got := parityMatch(t, ix, input)
	if len(got) != len(want) {
		t.Fatalf("%s: matched %v, want %v", input, got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("%s: matched %v, want %v", input, got, want)
		}
	}
}

func TestParityInWordsOneBased(t *testing.T) {
	ix := parityPositionCorpus(t)
	parityMatchWant(t, ix, `w0 IN WORDS 1 TO 1`, []DocID{1})
	parityMatchWant(t, ix, `w2 IN WORDS 2 TO 3`, []DocID{1})
	parityMatchWant(t, ix, `w3 IN WORDS 2 TO 3`, nil)
	parityMatchWant(t, ix, `w0 IN WORDS 1 TO 6`, []DocID{1})
	parityMatchWant(t, ix, `w5 IN WORDS 6 TO 6`, []DocID{1})
	parityMatchWant(t, ix, `w5 IN WORDS 1 TO 5`, nil)
}

func TestParityWithinWidth(t *testing.T) {
	ix := parityPositionCorpus(t)
	parityMatchWant(t, ix, `(w0 NEAR/0 w1) WITHIN 2`, []DocID{1})
	parityMatchWant(t, ix, `(w0 NEAR/0 w1) WITHIN 1`, nil)
	parityMatchWant(t, ix, `(w0 NEAR/1 w2) WITHIN 3`, []DocID{1})
	parityMatchWant(t, ix, `(w0 NEAR/1 w2) WITHIN 2`, nil)
	parityMatchWant(t, ix, `w0 WITHIN 1`, []DocID{1})
	parityMatchWant(t, ix, `w0 WITHIN 0`, nil)
}

func parityMustFail(t *testing.T, ix *Index, input string) {
	t.Helper()
	if q, err := ix.ParseTINQL(input); err == nil {
		t.Fatalf("%s: parsed as op %d, want an error", input, q.Op)
	}
}

// TestParitySpanStarRejected pins the match-all operand ban: * is a
// document-level query and cannot appear inside span, relation, or
// positional operators, while document-level uses stay valid.
func TestParitySpanStarRejected(t *testing.T) {
	ix := NewIndex()
	for _, in := range []string{
		`* NOT ENCLOSES spam`,
		`spam NOT ENCLOSES *`,
		`* THEN/0 spam`,
		`spam NEAR/0 *`,
		`(*) WITHIN 5`,
		`(spam NEAR/0 *) WITHIN 5`,
		`* IN FIRST 5 WORDS`,
		`spam IN WORDS 1 TO 2 NOT ENCLOSES *`,
	} {
		parityMustFail(t, ix, in)
	}
	for _, in := range []string{`*`, `(*)`, `* AND NOT spam`, `* AND beer`, `beer AND *`} {
		if _, err := ix.ParseTINQL(in); err != nil {
			t.Fatalf("%s: unexpected error %v", in, err)
		}
	}
}

// TestParityColonTerm pins the leading-colon rule: a term starting with
// : needs an explicit operator after another expression.
// TestParityComma pins comma handling: separator inside [...], numeric
// comma between digits, and rejection elsewhere. Matching follows the
// index's own tokenization on both sides.
func TestParityComma(t *testing.T) {
	ix := NewIndex()
	ix.Add(1, "beer")
	ix.Add(2, "ale")
	ix.Add(3, "lager")
	ix.Add(4, "I owe 47,000 dollars")
	ix.Add(5, "I owe 48,000 dollars")
	parityMatchWant(t, ix, `[beer, ale, lager]`, []DocID{1, 2, 3})
	parityMatchWant(t, ix, `[beer,ale]`, []DocID{1, 2})
	parityMatchWant(t, ix, `[beer ale, lager]`, []DocID{1, 2, 3})
	parityMatchWant(t, ix, `[47,000 48,000]`, []DocID{4, 5})
	parityMatchWant(t, ix, `1,000 TO 2,000`, nil)
	for _, in := range []string{`[beer,]`, `a,b`, `;`, `beer;`} {
		parityMustFail(t, ix, in)
	}
	if _, err := ix.ParseTINQL(`47,000`); err != nil {
		t.Fatalf("47,000: unexpected error %v", err)
	}
}

// TestParityBackslash pins backslash handling: a backslash before a
// non-special stays literal in bare terms, while \" \\ _ [ ] and \X
// resolve inside phrases.
func TestParityBackslash(t *testing.T) {
	ix := NewIndex()
	ix.Add(1, `foo\bar docs here`)
	ix.Add(2, `hello world`)
	if _, err := ix.ParseTINQL(`foo\bar`); err != nil {
		t.Fatalf(`foo\bar: unexpected error %v`, err)
	}
	if _, err := ix.ParseTINQL(`C:\Users\docs`); err != nil {
		t.Fatalf(`C:\Users\docs: unexpected error %v`, err)
	}
	if _, err := ix.ParseTINQL(`abc\`); err != nil {
		t.Fatalf(`abc\: unexpected error %v`, err)
	}
	parityMatchWant(t, ix, `"hello world"`, []DocID{2})
	parityMatchWant(t, ix, `"a\xb"`, nil)
}

// TestParityByTerm pins BY as an ordinary term outside ENCLOSED BY.
func TestParityByTerm(t *testing.T) {
	ix := NewIndex()
	ix.Add(1, "x by y")
	ix.Add(2, "x y")
	parityMatchWant(t, ix, `x BY y`, []DocID{1})
	parityMatchWant(t, ix, `x by y`, []DocID{1})
	parityMatchWant(t, ix, `100%`, nil)
}

// TestParityMatchesClosers pins regex-pattern termination: unescaped
// whitespace ends the pattern, while ) and ] close groups and classes
// opened inside and end the pattern otherwise.
func TestParityMatchesClosers(t *testing.T) {
	ix := NewIndex()
	ix.Add(1, "hops and dreams")
	ix.Add(2, "hop scotch")
	ix.Add(3, "abc day")
	ix.Add(4, "xbc day")
	parityMatchWant(t, ix, `(MATCHES hop.*s)`, []DocID{1})
	parityMatchWant(t, ix, `MATCHES [abc]x`, nil)
	parityMatchWant(t, ix, `MATCHES [abc]`, nil)
	parityMatchWant(t, ix, `MATCHES [ab]c`, nil)
	parityMatchWant(t, ix, `MATCHES (ho)+ps`, []DocID{1})
	parityMustFail(t, ix, `MATCHES a(b`)
	parityMustFail(t, ix, `MATCHES`)
}

func TestParityColonTerm(t *testing.T) {
	ix := NewIndex()
	parityMustFail(t, ix, `beer :tag`)
	if _, err := ix.ParseTINQL(`beer AND :tag`); err != nil {
		t.Fatalf("beer AND :tag: unexpected error %v", err)
	}
}

func TestParityFirstLastEdges(t *testing.T) {
	ix := parityPositionCorpus(t)
	parityMatchWant(t, ix, `w0 IN FIRST 1 WORDS`, []DocID{1})
	parityMatchWant(t, ix, `w1 IN FIRST 1 WORDS`, nil)
	parityMatchWant(t, ix, `w5 IN LAST 1 WORDS`, []DocID{1})
	parityMatchWant(t, ix, `w4 IN LAST 1 WORDS`, nil)
	parityMatchWant(t, ix, `w0 IN FIRST 100 WORDS`, []DocID{1})
	parityMatchWant(t, ix, `w5 IN LAST 100 WORDS`, []DocID{1})
}

// PlanetScale parity: a wildcard pattern spanning tokenizer boundaries
// rewrites to an adjacent phrase of exact and glob slots: `e-mail*` is
// `"e [MATCHES mail.*]"`. A pattern without boundaries stays whole-pattern
// alternatives, so `ma*il` never becomes a phrase of `ma` and `il`.
func TestParitySplitWildcard(t *testing.T) {
	ix := NewIndex()
	ix.Add(1, "reach me at e-mail lists")
	ix.Add(2, "email lists here")
	ix.Add(3, "e-mailx marks the spot")
	ix.Add(4, "e xyz mail lists")
	parityMatchWant(t, ix, `e-mail*`, []DocID{1, 3})
	parityMatchWant(t, ix, `e\,mail*`, []DocID{1, 3})
	parityMatchWant(t, ix, `mail*`, []DocID{1, 3, 4})
	parityMatchWant(t, ix, `ma*il`, []DocID{1, 4})
}

// PlanetScale parity: fuzzy splits a hyphenated word first, so `e-mail~1`
// phrases exact `e` with the edit neighborhood of `mail`.
func TestParitySplitFuzzy(t *testing.T) {
	ix := NewIndex()
	ix.Add(1, "send e-mail now")
	ix.Add(2, "send e-maul now")
	ix.Add(3, "send e-xyz now")
	ix.Add(4, "send email now")
	parityMatchWant(t, ix, `e-mail`, []DocID{1})
	parityMatchWant(t, ix, `e-mail~1`, []DocID{1, 2})
	if _, err := ix.ParseTINQL(`e-mail~`); err == nil {
		t.Fatalf("e-mail~ parsed without an edit distance, want error")
	}
}

// PlanetScale parity: CONTAINS takes wildcards and fuzzy but never creates
// a phrase — a multi-token word stays a single term with only the spelling
// match, which the token dictionary cannot hold.
func TestParityContainsShapes(t *testing.T) {
	ix := NewIndex()
	ix.Add(1, "wi-fi network")
	ix.Add(2, "mail server")
	ix.Add(3, "email server")
	ix.Add(4, "wimax network")
	parityMatchWant(t, ix, `wi-fi`, []DocID{1})
	parityMatchWant(t, ix, `CONTAINS wi-fi`, nil)
	parityMatchWant(t, ix, `CONTAINS mail`, []DocID{2})
	parityMatchWant(t, ix, `CONTAINS mail*`, []DocID{2})
	parityMatchWant(t, ix, `CONTAINS ma?l`, []DocID{2})
	parityMatchWant(t, ix, `CONTAINS e-mail*`, nil)
	parityMatchWant(t, ix, `CONTAINS mail~1`, []DocID{2})
	parityMatchWant(t, ix, `CONTAINS wi-fi~1`, nil)
	if _, err := ix.ParseTINQL(`CONTAINS ...`); err == nil {
		t.Fatalf("CONTAINS ... parsed without a term, want error")
	}
}

// PlanetScale parity: every numeric argument fits in an unsigned 32-bit
// integer; larger values are parse errors on all platforms.
func TestParityU32Bounds(t *testing.T) {
	ix := NewIndex()
	ix.Add(1, "beer wine")
	for _, input := range []string{
		`beer THEN/4294967296 wine`,
		`beer NEAR/4294967296 wine`,
		`beer~4294967296`,
		`beer~1:4294967296`,
		`beer IN FIRST 4294967296 WORDS`,
		`beer IN LAST 4294967296 WORDS`,
		`beer IN MIDDLE 4294967296%`,
		`beer IN WORDS 1 TO 4294967296`,
		`(beer NEAR/1 wine) WITHIN 4294967296`,
		`"beer wine"~4294967296`,
		`AT LEAST 4294967296 OF [beer wine]`,
		`AT LEAST 18446744073709551615 OF [beer wine]`,
	} {
		if _, err := ix.ParseTINQL(input); err == nil {
			t.Fatalf("%s parsed past the 32-bit bound, want error", input)
		}
	}
	// The bound itself still parses.
	for _, input := range []string{
		`beer THEN/4294967295 wine`,
		`beer IN FIRST 4294967295 WORDS`,
		`AT LEAST 4294967295 OF [beer wine]`,
	} {
		if _, err := ix.ParseTINQL(input); err != nil {
			t.Fatalf("%s: unexpected error %v", input, err)
		}
	}
}

// PlanetScale parity: % sits immediately after the number. The percentage
// rounds up: 50% of 5 items means at least 3 must match.
func TestParityPercentPlacement(t *testing.T) {
	ix := NewIndex()
	ix.Add(1, "a")
	ix.Add(2, "a b")
	ix.Add(3, "a b c")
	ix.Add(4, "a b c d")
	ix.Add(5, "a b c d e")
	parityMatchWant(t, ix, `AT LEAST 50% OF [a b c d]`, []DocID{2, 3, 4, 5})
	parityMatchWant(t, ix, `AT LEAST 50% OF [a b c d e]`, []DocID{3, 4, 5})
	for _, input := range []string{
		`AT LEAST 50 % OF [a b]`,
		`a IN FIRST 25 %`,
		`a IN LAST 25 %`,
		`a IN MIDDLE 50 %`,
	} {
		if _, err := ix.ParseTINQL(input); err == nil {
			t.Fatalf("%s parsed with a spaced %%, want error", input)
		}
	}
}

// PlanetScale parity: boost factors accept scientific notation within
// [0, 10000]; anything outside is a parse error.
func TestParityBoostScientific(t *testing.T) {
	ix := NewIndex()
	ix.Add(1, "beer")
	q, err := ix.ParseTINQL(`beer^1e3`)
	if err != nil {
		t.Fatalf("beer^1e3: %v", err)
	}
	if q.Boost != 1000 {
		t.Fatalf("beer^1e3 boost = %v, want 1000", q.Boost)
	}
	for _, input := range []string{`beer^1e5`, `beer^1e999`, `beer^10000.1`} {
		if _, err := ix.ParseTINQL(input); err == nil {
			t.Fatalf("%s parsed past the boost bound, want error", input)
		}
	}
}

// PlanetScale parity: inside a phrase `*?~^` are literal text; leading and
// trailing gaps anchor to nothing and are ignored; an all-gap phrase
// matches nothing; a quote inside [...] is a parse error.
func TestParityPhraseLiterals(t *testing.T) {
	ix := NewIndex()
	ix.Add(1, "a b")
	ix.Add(2, "axb")
	ix.Add(3, "wolf")
	ix.Add(4, "big bad wolf")
	parityMatchWant(t, ix, `"a * b"`, []DocID{1})
	parityMatchWant(t, ix, `"a^b"`, []DocID{1})
	parityMatchWant(t, ix, `"a~b"`, []DocID{1})
	parityMatchWant(t, ix, `"_ wolf"`, []DocID{3, 4})
	parityMatchWant(t, ix, `"big bad _"`, []DocID{4})
	parityMatchWant(t, ix, `"_"`, nil)
	parityMatchWant(t, ix, `"_"~2`, nil)
	parityMatchWant(t, ix, `"she said \"hi\""`, nil)
	for _, input := range []string{`"[\"a\" b]"`, `"[A AND B] x"`, `"[A NEAR/3 B] x"`} {
		if _, err := ix.ParseTINQL(input); err == nil {
			t.Fatalf("%s parsed, want error", input)
		}
	}
}

// PlanetScale parity: a written token the tokenizer splits occupies
// consecutive positions at its phrase slot; commas inside phrase brackets
// are literal term characters.
func TestParityPhraseMultiTokenAlts(t *testing.T) {
	ix := NewIndex()
	ix.Add(1, "e-mail now")
	ix.Add(2, "e-maul now")
	ix.Add(3, "e-xyz now")
	ix.Add(4, "47 000 48 000 number")
	parityMatchWant(t, ix, `"[e-mail e-maul] now"`, []DocID{1, 2})
	parityMatchWant(t, ix, `"[47,000 48,000]"`, []DocID{4})
	if _, err := ix.ParseTINQL(`"[wi-fi wimax] x"`); err == nil {
		t.Fatalf(`"[wi-fi wimax] x" parsed with mixed-width alternatives, want error`)
	}
}

// Locked reference behaviors: digit-commas never split the alternative
// list, MATCHES keeps its pattern case exactly, and the reserved words
// stay reserved.
func TestParityLockedReference(t *testing.T) {
	ix := NewIndex()
	ix.Add(1, "47 000 x")
	ix.Add(2, "48 000 x")
	ix.Add(3, "beer")
	parityMatchWant(t, ix, `47,000`, []DocID{1})
	parityMatchWant(t, ix, `[47,000 48,000]`, []DocID{1, 2})
	parityMatchWant(t, ix, `MATCHES beer.*`, []DocID{3})
	parityMatchWant(t, ix, `MATCHES Beer.*`, nil)
	for _, input := range []string{
		`AT`, `ALL`, `ENCLOSED`, `beer IN bar`, `beer IN MIDDLE 5 WORDS`,
	} {
		if _, err := ix.ParseTINQL(input); err == nil {
			t.Fatalf("%s parsed, want error", input)
		}
	}
}
