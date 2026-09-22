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

// TestScoreSingleUnsortedExact pins the sort-free path bit-identical to
// the sorted one: same counts, same idf order, same formula, same
// summation order — only the sort is gone.
func TestScoreSingleUnsortedExact(t *testing.T) {
	ix := NewIndex()
	for id, text := range scoreCorpusDocs {
		ix.Add(DocID(id+1), text)
	}
	inputs := []string{
		`apple`, `fuji`, `apple AND fuji`, `apple OR dog`, `apple AND NOT pie`,
		`[apple dog peach]`, `AT LEAST 2 OF [apple dog peach pie]`, `ALL OF [apple fuji]`,
		`appl*`, `p?ach`, `apple~1`, `aardvark TO cat`, `MATCHES fuji`,
		`CONTAINS banana`, `jalapeno`, `apple^2`, `*`,
		`(apple OR dog) AND (fuji OR peach) AND NOT pie`,
	}
	var stats ScoreStats
	for _, input := range inputs {
		q, err := ix.ParseTINQL(input)
		if err != nil {
			t.Fatalf("ParseTINQL(%q): %v", input, err)
		}
		if needsPositions(q) {
			t.Fatalf("%q needs positions, want sort-free", input)
		}
		ix.RefreshScoreStats(q, &stats)
		for id, text := range scoreCorpusDocs {
			var scratch TextScratch
			pairs := scanPairs(text, scratch.pairs[:0])
			sorted := append([]tokPos(nil), pairs...)
			sortTokPos(sorted)
			av := docView{pairs: sorted, length: uint32(len(sorted)), scratch: &scratch}
			acur := scoreCursor{st: &stats}
			want, wok := av.scoreSingleInto(q, &acur)
			bv := docView{pairs: pairs, length: uint32(len(pairs)), scratch: &scratch}
			bcur := scoreCursor{st: &stats}
			got, gok := bv.scoreSingleUnsorted(q, &bcur)
			if got != want || gok != wok {
				t.Fatalf("%q doc %d: unsorted (%v,%v) != sorted (%v,%v)",
					input, id+1, got, gok, want, wok)
			}
		}
	}
}

// TestNeedsPositions classifies every operator: only phrases, proximity,
// relations, and positional filters observe order.
func TestNeedsPositions(t *testing.T) {
	ix := NewIndex()
	ix.Add(1, "apple fuji pie")
	inputs := map[string]bool{
		`apple`: false, `apple AND fuji`: false, `apple OR fuji`: false,
		`apple AND NOT fuji`: false, `AT LEAST 1 OF [apple fuji]`: false,
		`*`: false, `appl*`: false, `apple^2`: false,
		`"apple fuji"`: true, `"apple fuji"~1`: true,
		`apple THEN/0 fuji`: true, `apple NEAR/1 fuji`: true,
		`(apple OR fuji) WITHIN 3`:              true,
		`(apple NEAR/1 fuji) ENCLOSES apple`:    true,
		`apple ENCLOSED BY (apple NEAR/1 fuji)`: true,
		`apple NOT OVERLAPPING fuji`:            true,
		`apple BEFORE fuji`:                     true, `apple AFTER fuji`: true,
		`apple IN FIRST 1 WORDS`:           true,
		`(apple AND fuji) OR "apple fuji"`: true,
	}
	for input, want := range inputs {
		q, err := ix.ParseTINQL(input)
		if err != nil {
			t.Fatalf("ParseTINQL(%q): %v", input, err)
		}
		if got := needsPositions(q); got != want {
			t.Fatalf("%q: needsPositions = %v, want %v", input, got, want)
		}
	}
}

var transientBenchDoc = "fuji apple juicy red pie apple apple banana " +
	"lazy dog sleeps all day near the river bank security critical"

// benchmarkTransient runs MatchSingle/ScoreSingle over one document so
// the sort-free term lane pins against the sorted phrase lane.
func benchmarkTransient(b *testing.B, input string, score bool) {
	b.Helper()
	ix := NewIndex()
	ix.Add(1, transientBenchDoc)
	q, err := ix.ParseTINQL(input)
	if err != nil {
		b.Fatal(err)
	}
	var scratch TextScratch
	var stats ScoreStats
	ix.RefreshScoreStats(q, &stats)
	b.ReportAllocs()
	b.ResetTimer()
	if score {
		for i := 0; i < b.N; i++ {
			_ = ScoreSingle(transientBenchDoc, q, &stats, &scratch)
		}
		return
	}
	for i := 0; i < b.N; i++ {
		_ = MatchSingle(transientBenchDoc, q, &scratch)
	}
}

func BenchmarkTransientMatchTerm(b *testing.B)   { benchmarkTransient(b, `apple AND banana`, false) }
func BenchmarkTransientMatchPhrase(b *testing.B) { benchmarkTransient(b, `"apple banana"`, false) }
func BenchmarkTransientScoreTerm(b *testing.B)   { benchmarkTransient(b, `apple AND banana`, true) }
func BenchmarkTransientScorePhrase(b *testing.B) { benchmarkTransient(b, `"apple banana"`, true) }

// benchmarkMatchStop runs MatchSingle over a long document with the term
// at the given position, pinning early-exit behavior: a leading hit
// scans a prefix, a missing term scans everything.
func benchmarkMatchStop(b *testing.B, doc, input string, want bool) {
	b.Helper()
	ix := NewIndex()
	ix.Add(1, doc)
	q, err := ix.ParseTINQL(input)
	if err != nil {
		b.Fatal(err)
	}
	var scratch TextScratch
	if got := MatchSingle(doc, q, &scratch); got != want {
		b.Fatalf("match = %v, want %v", got, want)
	}
	b.SetBytes(int64(len(doc)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = MatchSingle(doc, q, &scratch)
	}
}

var matchStopBase = transientBenchDoc + " " + transientBenchDoc + " " +
	transientBenchDoc + " " + transientBenchDoc + " trailing words here"

func BenchmarkMatchStopEarly(b *testing.B) {
	benchmarkMatchStop(b, "needle "+matchStopBase, `needle`, true)
}
func BenchmarkMatchStopLate(b *testing.B) {
	benchmarkMatchStop(b, matchStopBase+" needle", `needle`, true)
}
func BenchmarkMatchStopAbsent(b *testing.B) {
	benchmarkMatchStop(b, matchStopBase, `absentterm`, false)
}
func BenchmarkMatchStopAll(b *testing.B) { benchmarkMatchStop(b, matchStopBase, `*`, true) }
