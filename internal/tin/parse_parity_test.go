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
