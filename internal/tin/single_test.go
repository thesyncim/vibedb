package tin

import (
	"fmt"
	"testing"
)

// TestMatchSingleAgreesWithIndex is the transient evaluator's contract:
// for every document and every query, single-document matching equals index
// membership. The query engine's ==> recheck rests on this.
func TestMatchSingleAgreesWithIndex(t *testing.T) {
	docs := map[DocID]string{
		1:  "fuji apple juicy red",
		2:  "apple fuji pie",
		3:  "apple apple apple banana",
		4:  "lazy dog sleeps all day",
		5:  "security critical threat level",
		6:  "peach blossom spring rain",
		7:  "wi-fi access point here",
		8:  "Jalapeño café con leche",
		9:  "title alpha beta gamma disclaimer",
		10: "citrus melon orange tang",
	}
	ix := NewIndex()
	for id, text := range docs {
		ix.Add(id, text)
	}
	inputs := []string{
		`apple`, `fuji`, `apple AND fuji`, `apple OR dog`, `apple AND NOT pie`,
		`"fuji apple"`, `"fuji apple"~2`, `"big _ wolf"`, `"apple [pie banana]"`,
		`[apple dog peach]`, `AT LEAST 2 OF [apple dog peach pie]`, `ALL OF [apple fuji]`,
		`apple THEN/0 fuji`, `peach NEAR/3 rain`, `fuji NEAR/1 apple`,
		`(security NEAR/5 threat) ENCLOSES critical`,
		`critical ENCLOSED BY (security NEAR/5 threat)`,
		`title NOT OVERLAPPING disclaimer`, `alpha BEFORE gamma`, `gamma AFTER alpha`,
		`apple IN FIRST 2 WORDS`, `day IN LAST 2 WORDS`, `*`, `* NOT ENCLOSES apple`,
		`appl*`, `p?ach`, `apple~1`, `aardvark TO cat`, `MATCHES fuji`,
		`CONTAINS banana`, `wi-fi`, `jalapeno`, `apple^2`, `"apple banana"^1.5`,
		`(apple OR dog) WITHIN 4`, `apple IN WORDS 0 TO 2`,
	}
	var scratch TextScratch
	for _, input := range inputs {
		q, err := ix.ParseTINQL(input)
		if err != nil {
			t.Fatalf("ParseTINQL(%q): %v", input, err)
		}
		matched := ix.Match(q, nil)
		inSet := make(map[DocID]bool, len(matched))
		for _, id := range matched {
			inSet[id] = true
		}
		for id, text := range docs {
			if got := MatchSingle(text, q, &scratch); got != inSet[id] {
				t.Fatalf("%q doc %d: single=%v index=%v", input, id, got, inSet[id])
			}
		}
	}
}

// TestMatchSingleAllocs bounds transient matching garbage: warmed scratch
// must hold steady state at a small constant (phrase slot lists), never
// scaling with the text.
func TestMatchSingleAllocs(t *testing.T) {
	q := Query{Op: OpPhrase, Phrase: []PhrasePos{
		{Alts: []uint64{mustHash(t, "quick")}},
		{Alts: []uint64{mustHash(t, "brown")}},
		{Alts: []uint64{mustHash(t, "fox")}},
	}}
	text := "the quick brown fox jumps over the lazy dog near the river bank"
	var scratch TextScratch
	if !MatchSingle(text, q, &scratch) {
		t.Fatal("expected match")
	}
	const maxAllocs = 4
	if n := testing.AllocsPerRun(50, func() {
		MatchSingle(text, q, &scratch)
	}); n > maxAllocs {
		t.Fatalf("MatchSingle allocated %v per run, want <= %d", n, maxAllocs)
	}
}

func TestMatchSingleBasics(t *testing.T) {
	var scratch TextScratch
	if !MatchSingle("Fuji APPLE", Query{Op: OpTerm, Term: mustHash(t, "fuji")}, &scratch) {
		t.Fatal("case folding")
	}
	if MatchSingle("", Query{Op: OpAll}, &scratch) != true {
		t.Fatal("match-all on empty doc")
	}
	if MatchSingle("apple", Query{Op: OpOr}, &scratch) {
		t.Fatal("empty expansion matches nothing")
	}
	got := fmt.Sprint(MatchSingle("a b", Query{Op: OpTerm, Term: mustHash(t, "zzz")}, &scratch))
	if got != "false" {
		t.Fatalf("unknown term: %s", got)
	}
}
