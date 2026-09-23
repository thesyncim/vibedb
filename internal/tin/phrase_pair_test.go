package tin

import (
	"math/rand"
	"reflect"
	"testing"
)

// pairCorpus builds a randomized open index plus the token stream per
// document, so the pair fast path answers against a brute-force oracle that
// shares no code with the query lanes. The vocabulary is tiny to force
// collisions, reversals, gaps, and self-adjacency; planted bigrams make
// every pair shape (hit, miss, reversed, self) occur.
func pairCorpus(seed int64, n int) (*Index, [][]string) {
	ix := NewIndex()
	vocab := []string{"alpha", "beta", "gamma", "delta"}
	rng := rand.New(rand.NewSource(seed))
	toks := make([][]string, 0, n)
	for i := 0; i < n; i++ {
		var doc []string
		switch i % 8 {
		case 0:
			doc = []string{"alpha", "beta"}
		case 1:
			doc = []string{"beta", "alpha"}
		case 2:
			doc = []string{"alpha", "gamma", "beta"}
		case 3:
			doc = []string{"alpha", "alpha"}
		case 4:
			doc = []string{"alpha"}
		default:
			for k, m := 0, 1+rng.Intn(10); k < m; k++ {
				doc = append(doc, vocab[rng.Intn(len(vocab))])
			}
		}
		text := ""
		for k, w := range doc {
			if k > 0 {
				text += " "
			}
			text += w
		}
		ix.Add(DocID(i+1), text)
		toks = append(toks, doc)
	}
	return ix, toks
}

// brutePair is the independent oracle: doc matches when b immediately
// follows a in its token stream. live marks documents still present;
// removed documents never match even when their tokens would.
func brutePair(toks [][]string, live []bool, a, b string) []DocID {
	var out []DocID
	for i, doc := range toks {
		if !live[i] {
			continue
		}
		for k := 0; k+1 < len(doc); k++ {
			if doc[k] == a && doc[k+1] == b {
				out = append(out, DocID(i+1))
				break
			}
		}
	}
	return out
}

func checkPairsAgainstBrute(t *testing.T, ix *Index, toks [][]string, live []bool, pairs [][2]string) {
	t.Helper()
	for _, pr := range pairs {
		ha, hb := mustHash(t, pr[0]), mustHash(t, pr[1])
		got := ix.matchExactPairDocs(ha, hb, nil)
		want := brutePair(toks, live, pr[0], pr[1])
		if len(want) == 0 {
			if len(got) != 0 {
				t.Fatalf("pair %q %q: want empty, got %v", pr[0], pr[1], got)
			}
			continue
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("pair %q %q: got %v want %v", pr[0], pr[1], got, want)
		}
		// Ascending order is part of the contract (same as the generic lane).
		for i := 1; i < len(got); i++ {
			if got[i-1] >= got[i] {
				t.Fatalf("pair %q %q: not ascending: %v", pr[0], pr[1], got)
			}
		}
	}
}

// TestExactPairMatchesBruteForce runs the fast path against the oracle on
// open and sealed twins, including self-pairs, missing terms, reversals,
// and documents removed after indexing (which the postings still name but
// the document table no longer holds, so they must not match).
func TestExactPairMatchesBruteForce(t *testing.T) {
	ix, toks := pairCorpus(42, 300)
	live := make([]bool, len(toks))
	for i := range live {
		live[i] = true
	}
	// Remove a planted hitter (doc 1: "alpha beta"), a self-hitter
	// (doc 4: "alpha alpha"), and a random doc.
	for _, id := range []DocID{1, 4, 17} {
		if !ix.Remove(id) {
			t.Fatalf("remove %d failed", id)
		}
		live[id-1] = false
	}
	pairs := [][2]string{
		{"alpha", "beta"}, {"beta", "alpha"},
		{"alpha", "alpha"}, {"beta", "beta"}, {"delta", "delta"},
		{"alpha", "gamma"}, {"gamma", "beta"}, {"delta", "alpha"},
		{"alpha", "missing"}, {"missing", "beta"}, {"missing", "missing"},
	}
	checkPairsAgainstBrute(t, ix, toks, live, pairs)

	sealed := NewIndex()
	for i, doc := range toks {
		text := ""
		for k, w := range doc {
			if k > 0 {
				text += " "
			}
			text += w
		}
		sealed.Add(DocID(i+1), text)
	}
	for _, id := range []DocID{1, 4, 17} {
		sealed.Remove(id)
	}
	sealed.Seal()
	checkPairsAgainstBrute(t, sealed, toks, live, pairs)
}

// TestExactPairMatchesMatch runs the public Match entry (which dispatches
// to the fast path for this shape) against the oracle, proving the
// dispatch preserves verdicts end to end on both layouts.
func TestExactPairMatchesMatch(t *testing.T) {
	ix, toks := pairCorpus(7, 120)
	live := make([]bool, len(toks))
	for i := range live {
		live[i] = true
	}
	sealed := NewIndex()
	for i, doc := range toks {
		text := ""
		for k, w := range doc {
			if k > 0 {
				text += " "
			}
			text += w
		}
		sealed.Add(DocID(i+1), text)
	}
	sealed.Seal()
	for _, x := range []*Index{ix, sealed} {
		for _, pr := range [][2]string{{"alpha", "beta"}, {"beta", "alpha"}, {"alpha", "alpha"}, {"gamma", "delta"}} {
			q := Query{Op: OpPhrase, Phrase: []PhrasePos{
				{Alts: []uint64{mustHash(t, pr[0])}},
				{Alts: []uint64{mustHash(t, pr[1])}},
			}}
			got := x.Match(q, nil)
			want := brutePair(toks, live, pr[0], pr[1])
			if len(want) == 0 && len(got) != 0 {
				t.Fatalf("pair %q %q: want empty, got %v", pr[0], pr[1], got)
			}
			if len(want) > 0 && !reflect.DeepEqual(got, want) {
				t.Fatalf("pair %q %q: got %v want %v", pr[0], pr[1], got, want)
			}
		}
	}
}

// TestExactPairZeroAlloc pins the steady-state allocation budget: open/open
// stages nothing, so a warmed call with caller-owned out allocates zero;
// sealed twins reuse the match position stage and block cache after warmup.
func TestExactPairZeroAlloc(t *testing.T) {
	for _, seal := range []bool{false, true} {
		ix := sealedBenchIndex(seal)
		ha, hb := mustHash(t, "filler"), mustHash(t, "words")
		out := ix.matchExactPairDocs(ha, hb, nil)
		if len(out) == 0 {
			t.Fatalf("seal=%v: bench bigram matched nothing", seal)
		}
		for i := 0; i < 5; i++ {
			out = ix.matchExactPairDocs(ha, hb, out[:0])
		}
		if n := testing.AllocsPerRun(100, func() {
			out = ix.matchExactPairDocs(ha, hb, out[:0])
		}); n != 0 {
			t.Fatalf("seal=%v: %v allocs per warmed pair match, want 0", seal, n)
		}
		_ = out
	}
}
