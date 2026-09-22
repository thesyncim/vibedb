package tin

import (
	"math"
	"testing"
)

// scoreCorpusInputs exercises every scoring arm: terms, phrases, boolean
// combinations, proximity and relations, filters, thresholds, wildcards,
// and boosts.
var scoreCorpusInputs = []string{
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

var scoreCorpusDocs = []string{
	"fuji apple juicy red pie",
	"lazy dog sleeps all day near the river bank",
	"security critical threat level alpha beta gamma",
	"title alpha beta gamma disclaimer",
	"the quick brown fox jumps over the lazy dog",
	"",
}

// TestScoreSingleAgreesWithScore proves the transient scorer mirrors the
// index scorer: for every document and every query shape, ScoreSingle
// equals the document's ix.Score entry (0 when absent). The 1e-12 relative
// tolerance is the documented scalar/vector kernel rounding gap, never a
// ranking flip; NaN equals NaN.
func TestScoreSingleAgreesWithScore(t *testing.T) {
	ix := NewIndex()
	for id, text := range scoreCorpusDocs {
		ix.Add(DocID(id+1), text)
	}
	var scratch TextScratch
	var stats ScoreStats
	for _, input := range scoreCorpusInputs {
		q, err := ix.ParseTINQL(input)
		if err != nil {
			t.Fatalf("ParseTINQL(%q): %v", input, err)
		}
		ix.RefreshScoreStats(q, &stats)
		ranked := ix.Score(q, 0, nil)
		for id, text := range scoreCorpusDocs {
			got := ScoreSingle(text, q, &stats, &scratch)
			var want float64
			found := false
			for _, s := range ranked {
				if s.Doc == DocID(id+1) {
					want, found = s.Score, true
					break
				}
			}
			if !found && got != 0 {
				t.Fatalf("%q doc %d: transient %v, index absent", input, id+1, got)
			}
			if found && !scoresClose(got, want) {
				t.Fatalf("%q doc %d: transient %v != index %v", input, id+1, got, want)
			}
		}
	}
}

func scoresClose(a, b float64) bool {
	if math.IsNaN(a) && math.IsNaN(b) {
		return true
	}
	if a == b {
		return true
	}
	den := math.Max(math.Abs(a), math.Abs(b))
	if den == 0 {
		return true
	}
	return math.Abs(a-b)/den <= 1e-12
}

// TestScoreSingleZeroAlloc pins the transient scorer's warm budget: a term
// query over a short document must not touch the heap once statistics and
// scratch fit.
func TestScoreSingleZeroAlloc(t *testing.T) {
	ix := NewIndex()
	ix.Add(1, "luxury vintage watches")
	q, err := ix.ParseTINQL("luxury AND vintage")
	if err != nil {
		t.Fatal(err)
	}
	var scratch TextScratch
	stats := ix.RefreshScoreStats(q, nil)
	if got := ScoreSingle("luxury vintage watches", q, stats, &scratch); got <= 0 {
		t.Fatalf("score = %v, want positive", got)
	}
	if n := testing.AllocsPerRun(50, func() {
		ScoreSingle("luxury vintage watches", q, stats, &scratch)
	}); n != 0 {
		t.Fatalf("ScoreSingle allocated %v per run, want 0", n)
	}
}
