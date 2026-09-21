package tin

import (
	"fmt"
	"strings"
	"testing"
)

// TINQL surface tests: parse the documented language, then match. Each case
// builds its own index so expectations read locally.

func parseMatch(t *testing.T, docs map[DocID]string, input string) []DocID {
	t.Helper()
	ix := NewIndex()
	for id, text := range docs {
		ix.Add(id, text)
	}
	q, err := ix.ParseTINQL(input)
	if err != nil {
		t.Fatalf("ParseTINQL(%q): %v", input, err)
	}
	return ix.Match(q, nil)
}

func expectDocs(t *testing.T, input string, got []DocID, want ...DocID) {
	t.Helper()
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("%q matched %v, want %v", input, got, want)
	}
}

func TestTINQLTermsAndBooleans(t *testing.T) {
	docs := map[DocID]string{
		1: "fuji apple juicy",
		2: "apple fuji",
		3: "apple pie",
		4: "fish and chips",
	}
	expectDocs(t, `apple AND "fuji apple"`, parseMatch(t, docs, `apple AND "fuji apple"`), 1)
	expectDocs(t, `juicy AND apple`, parseMatch(t, docs, `juicy AND apple`), 1)
	expectDocs(t, `apple OR pie`, parseMatch(t, docs, `apple OR pie`), 1, 2, 3)
	expectDocs(t, `apple AND NOT pie`, parseMatch(t, docs, `apple AND NOT pie`), 1, 2)
	expectDocs(t, `apple pie`, parseMatch(t, docs, `apple pie`), 3) // implicit AND
	expectDocs(t, `and`, parseMatch(t, docs, `and`), 4)             // lowercase: a term
	expectDocs(t, `CONTAINS apple`, parseMatch(t, docs, `CONTAINS apple`), 1, 2, 3)
	expectDocs(t, `*`, parseMatch(t, docs, `*`), 1, 2, 3, 4)
	// No-token inputs match nothing.
	expectDocs(t, ``, parseMatch(t, docs, ``))
	expectDocs(t, `   `, parseMatch(t, docs, `   `))
	expectDocs(t, `...`, parseMatch(t, docs, `...`))
}

func TestTINQLPrecedence(t *testing.T) {
	// A OR B THEN/0 C AND D  ==  A OR ((B THEN/0 C) AND D)
	docs := map[DocID]string{
		1: "a x",     // A only
		2: "b c d",   // (B THEN C) AND D
		3: "b c",     // B THEN C, no D
		4: "b d c",   // B AND D but not THEN
		5: "c d b x", // D AND ... no A/B-adjacency
	}
	got := parseMatch(t, docs, `a OR b THEN/0 c AND d`)
	expectDocs(t, `a OR b THEN/0 c AND d`, got, 1, 2)
}

func TestTINQLPhrases(t *testing.T) {
	docs := map[DocID]string{
		1: "fuji apple pie",
		2: "apple fuji pie",
		3: "big bad wolf",
		4: "big huge wolf",
		5: "big bad huge wolf",
		6: "fuji big apple",
	}
	expectDocs(t, `"fuji apple"`, parseMatch(t, docs, `"fuji apple"`), 1)
	expectDocs(t, `"fuji apple"~0`, parseMatch(t, docs, `"fuji apple"~0`), 1)
	expectDocs(t, `"fuji apple"~1`, parseMatch(t, docs, `"fuji apple"~1`), 1, 6)
	expectDocs(t, `"fuji apple"~2`, parseMatch(t, docs, `"fuji apple"~2`), 1, 6)
	expectDocs(t, `"big _ wolf"`, parseMatch(t, docs, `"big _ wolf"`), 3, 4)
	expectDocs(t, `"big [bad huge] wolf"`, parseMatch(t, docs, `"big [bad huge] wolf"`), 3, 4)
	expectDocs(t, `wi-fi`, parseMatch(t, map[DocID]string{1: "wi-fi access", 2: "wireless fidelity"}, `wi-fi`), 1)
}

func TestTINQLAlternatives(t *testing.T) {
	docs := map[DocID]string{
		1: "mango lassi",
		2: "plum cake",
		3: "pear tart",
		4: "mango plum pear pie",
		5: "banana bread",
	}
	expectDocs(t, `[mango plum pear]`, parseMatch(t, docs, `[mango plum pear]`), 1, 2, 3, 4)
	expectDocs(t, `AT LEAST 2 OF [mango plum pear pie]`, parseMatch(t, docs, `AT LEAST 2 OF [mango plum pear pie]`), 4)
	expectDocs(t, `AT LEAST 50% OF [mango plum pear pie]`, parseMatch(t, docs, `AT LEAST 50% OF [mango plum pear pie]`), 4)
	expectDocs(t, `ALL OF [mango plum pear]`, parseMatch(t, docs, `ALL OF [mango plum pear]`), 4)
}

func TestTINQLProximity(t *testing.T) {
	docs := map[DocID]string{
		1: "fuji apple",
		2: "apple fuji",
		3: "peach xx blossom",
		4: "peach xx yy blossom",
		5: "citrus melon",
		6: "citrus xx melon",
	}
	expectDocs(t, `fuji THEN/0 apple`, parseMatch(t, docs, `fuji THEN/0 apple`), 1)
	expectDocs(t, `apple THEN/0 fuji`, parseMatch(t, docs, `apple THEN/0 fuji`), 2)
	expectDocs(t, `peach NEAR/1 blossom`, parseMatch(t, docs, `peach NEAR/1 blossom`), 3)
	expectDocs(t, `peach NEAR/2 blossom`, parseMatch(t, docs, `peach NEAR/2 blossom`), 3, 4)
	expectDocs(t, `(citrus NEAR/5 melon) WITHIN 6`, parseMatch(t, docs, `(citrus NEAR/5 melon) WITHIN 6`), 5, 6)
	expectDocs(t, `(citrus NEAR/5 melon) WITHIN 2`, parseMatch(t, docs, `(citrus NEAR/5 melon) WITHIN 2`), 5)
	expectDocs(t, `(citrus NEAR/5 melon) WITHIN 1`, parseMatch(t, docs, `(citrus NEAR/5 melon) WITHIN 1`))
}

func TestTINQLRelations(t *testing.T) {
	docs := map[DocID]string{
		1: "security critical threat",
		2: "critical failure",
		3: "spam spam spam",
		4: "title here disclaimer there",
		5: "alpha beta gamma",
	}
	expectDocs(t,
		`(security NEAR/10 threat) ENCLOSES critical`,
		parseMatch(t, docs, `(security NEAR/10 threat) ENCLOSES critical`), 1)
	expectDocs(t, `* NOT ENCLOSES spam`, parseMatch(t, docs, `* NOT ENCLOSES spam`), 1, 2, 4, 5)
	expectDocs(t,
		`critical ENCLOSED BY (security NEAR/10 threat)`,
		parseMatch(t, docs, `critical ENCLOSED BY (security NEAR/10 threat)`), 1)
	expectDocs(t,
		`title NOT OVERLAPPING disclaimer`,
		parseMatch(t, docs, `title NOT OVERLAPPING disclaimer`), 4)
	expectDocs(t, `alpha BEFORE gamma`, parseMatch(t, docs, `alpha BEFORE gamma`), 5)
	expectDocs(t, `gamma AFTER alpha`, parseMatch(t, docs, `gamma AFTER alpha`), 5)
	expectDocs(t, `gamma BEFORE alpha`, parseMatch(t, docs, `gamma BEFORE alpha`))
}

func TestTINQLPositionalFilters(t *testing.T) {
	docs := map[DocID]string{
		1: "apple " + strings.Repeat("word ", 98) + "tail",
		2: "head " + strings.Repeat("word ", 98) + "apple",
	}
	expectDocs(t, `apple IN FIRST 1 WORDS`, parseMatch(t, docs, `apple IN FIRST 1 WORDS`), 1)
	expectDocs(t, `apple IN LAST 1 WORDS`, parseMatch(t, docs, `apple IN LAST 1 WORDS`), 2)
	expectDocs(t, `apple IN FIRST 2%`, parseMatch(t, docs, `apple IN FIRST 2%`), 1)
	expectDocs(t, `apple IN LAST 2%`, parseMatch(t, docs, `apple IN LAST 2%`), 2)
	expectDocs(t, `apple IN MIDDLE 50%`, parseMatch(t, docs, `apple IN MIDDLE 50%`))
	expectDocs(t, `apple IN WORDS 0 TO 0`, parseMatch(t, docs, `apple IN WORDS 0 TO 0`), 1)
	expectDocs(t, `apple IN WORDS 99 TO 99`, parseMatch(t, docs, `apple IN WORDS 99 TO 99`), 2)
}

func TestTINQLExpansions(t *testing.T) {
	docs := map[DocID]string{
		1: "apple application",
		2: "peach",
		3: "aardvark",
		4: "cat",
		5: "apply",
	}
	expectDocs(t, `appl*`, parseMatch(t, docs, `appl*`), 1, 5)
	expectDocs(t, `p?ach`, parseMatch(t, docs, `p?ach`), 2)
	expectDocs(t, `apple~1`, parseMatch(t, docs, `apple~1`), 1, 5)
	expectDocs(t, `apple~0:1`, parseMatch(t, docs, `apple~0:1`), 1, 5)
	expectDocs(t, `apply~0`, parseMatch(t, docs, `apply~0`), 5)
	expectDocs(t, `aardvark TO cat`, parseMatch(t, docs, `aardvark TO cat`), 1, 3, 4, 5)
	expectDocs(t, `* TO cat`, parseMatch(t, docs, `* TO cat`), 1, 3, 4, 5)
	expectDocs(t, `MATCHES peach.*`, parseMatch(t, docs, `MATCHES peach.*`), 2)
	expectDocs(t, `MATCHES appl.*`, parseMatch(t, docs, `MATCHES appl.*`), 1, 5)
}

func TestTINQLBoosts(t *testing.T) {
	ix := NewIndex()
	ix.Add(1, "apple banana")
	ix.Add(2, "apple apple banana")
	q, err := ix.ParseTINQL(`apple^2`)
	if err != nil {
		t.Fatal(err)
	}
	got := ix.Score(q, 10, nil)
	if len(got) != 2 || got[0].Doc != 2 {
		t.Fatalf("boosted order = %v", got)
	}
	if _, err := ix.ParseTINQL(`apple^10000.1`); err == nil {
		t.Fatal("out-of-range boost parsed without error")
	}
	if _, err := ix.ParseTINQL(`"a b"^1.5`); err != nil {
		t.Fatalf("phrase boost: %v", err)
	}
}

func TestTINQLErrors(t *testing.T) {
	ix := NewIndex()
	ix.Add(1, "wi fi apple")
	for _, input := range []string{
		`""`, `[]`, `["a"`, `"abc`, `(apple`, `apple)`, `* TO`,
		`apple~`, `apple~x`, `apple^`, `apple^10001`, `wi-fi~2`,
		`apple NOT peel`, `NOT apple`, `THEN/2 apple`, `apple THEN apple`,
		`apple THEN/ apple`, `apple IN NOWHERE 5`, `apple IN WORDS 1`,
		`AT MOST 2 OF [a b]`, `ALL [a b]`, `MATCHES`, `[a, b]`, `a,b`,
		`apple ENCLOSED apple`, `A TO * TO B`, `"a * b"`,
	} {
		if q, err := ix.ParseTINQL(input); err == nil {
			t.Fatalf("ParseTINQL(%q) = %+v, want error", input, q)
		}
	}
}

func TestTINQLNoTokenInputs(t *testing.T) {
	ix := NewIndex()
	ix.Add(1, "apple")
	for _, input := range []string{"", "   ", "..."} {
		q, err := ix.ParseTINQL(input)
		if err != nil {
			t.Fatalf("ParseTINQL(%q): %v", input, err)
		}
		if got := ix.Match(q, nil); len(got) != 0 {
			t.Fatalf("ParseTINQL(%q) matched %v", input, got)
		}
	}
}

func TestTINQLScoreEndToEnd(t *testing.T) {
	docs := map[DocID]string{
		1: "apple banana",
		2: "apple apple apple banana",
		3: "banana cherry",
	}
	ix := NewIndex()
	for id, text := range docs {
		ix.Add(id, text)
	}
	q, err := ix.ParseTINQL(`apple AND banana`)
	if err != nil {
		t.Fatal(err)
	}
	got := ix.Score(q, 10, nil)
	if len(got) != 2 || got[0].Doc != 2 || got[1].Doc != 1 {
		t.Fatalf("AND score = %v", got)
	}
	q, err = ix.ParseTINQL(`"apple banana"`)
	if err != nil {
		t.Fatal(err)
	}
	got = ix.Score(q, 10, nil)
	// Doc 2 holds an adjacent occurrence at positions 2,3, so both match
	// with the shorter document first.
	if len(got) != 2 || got[0].Doc != 1 || got[1].Doc != 2 {
		t.Fatalf("phrase score = %v", got)
	}
}
