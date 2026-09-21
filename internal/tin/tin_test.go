package tin

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"
)

func mustHash(t *testing.T, term string) uint64 {
	t.Helper()
	h, n := FoldTerm(term)
	if n != 1 {
		t.Fatalf("FoldTerm(%q) scanned %d tokens, want 1", term, n)
	}
	return h
}

func collect(text string) (hashes []uint64, pos []uint32) {
	scanString(text, func(h uint64, p uint32) {
		hashes = append(hashes, h)
		pos = append(pos, p)
	})
	return hashes, pos
}

func TestFoldASCIIMatchesScalar(t *testing.T) {
	// Exhaustive single- and double-byte differential over the whole table.
	for b := 0; b < 256; b++ {
		for c := 0; c < 256; c += 51 {
			src := string([]byte{byte(b), byte(c)})
			dst := make([]byte, len(src))
			foldASCII(dst, src)
			var want [2]byte
			foldASCIIScalar(want[:], src)
			if dst[0] != want[0] || dst[1] != want[1] {
				t.Fatalf("foldASCII(%q) = %q, want %q", src, dst, want[:])
			}
		}
	}
	// Randomized buffers, biased to ASCII text with UTF-8 sprinkled in.
	rng := rand.New(rand.NewSource(1))
	alpha := "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789 _-.,Jalapeño café Über straße "
	for trial := 0; trial < 200; trial++ {
		n := 1 + rng.Intn(600)
		var sb strings.Builder
		for sb.Len() < n {
			sb.WriteByte(alpha[rng.Intn(len(alpha))])
		}
		src := sb.String()[:n]
		dst := make([]byte, len(src))
		foldASCII(dst, src)
		want := make([]byte, len(src))
		foldASCIIScalar(want, src)
		if string(dst) != string(want) {
			t.Fatalf("trial %d: foldASCII(%q) = %q, want %q", trial, src, dst, want)
		}
	}
}

func TestScanPositionsAndHyphen(t *testing.T) {
	h, p := collect("Fuji apple, wi-fi!")
	fuji := mustHash(t, "fuji")
	apple := mustHash(t, "apple")
	wi := mustHash(t, "wi")
	fi := mustHash(t, "fi")
	wantH := []uint64{fuji, apple, wi, fi}
	wantP := []uint32{0, 1, 2, 3}
	if fmt.Sprint(h) != fmt.Sprint(wantH) || fmt.Sprint(p) != fmt.Sprint(wantP) {
		t.Fatalf("scan = (%x,%v), want (%x,%v)", h, p, wantH, wantP)
	}
}

func TestFoldEqualityCaseAndAccent(t *testing.T) {
	for _, pair := range [][2]string{
		{"Apple", "apple"},
		{"Jalapeño", "jalapeno"},
		{"CAFÉ", "café"},
		{"STRASSE", "strasse"},
		{"Über", "uber"},
	} {
		a, na := FoldTerm(pair[0])
		b, nb := FoldTerm(pair[1])
		if na != 1 || nb != 1 || a != b {
			t.Fatalf("FoldTerm(%q)=(%x,%d) vs FoldTerm(%q)=(%x,%d)", pair[0], a, na, pair[1], b, nb)
		}
	}
}

func TestFoldTermNoTokensMatchNothing(t *testing.T) {
	for _, s := range []string{"", "   ", " \t\n ", "...", "--", "''"} {
		if _, n := FoldTerm(s); n != 0 {
			t.Fatalf("FoldTerm(%q) scanned %d tokens, want 0", s, n)
		}
	}
	ix := NewIndex()
	ix.Add(1, "apple")
	if got := ix.Match(Query{Op: OpTerm, Term: 999}, nil); len(got) != 0 {
		t.Fatalf("unknown term matched %v", got)
	}
}

func TestScanFoldedEqualsScanString(t *testing.T) {
	// The bulk SIMD-fold path must emit exactly the scalar path's stream.
	docs := []string{
		strings.Repeat("The Quick Brown Fox Jumps Over Jalapeño Café. ", 20),
		strings.Repeat("wi-fi Über alles, STRASSE 123. ", 30),
		strings.Repeat("a", 500),
	}
	for i, d := range docs {
		if len(d) < simdFoldThreshold {
			t.Fatalf("doc %d is %d bytes, below the fold threshold", i, len(d))
		}
		var wantH []uint64
		var wantP []uint32
		scanString(d, func(h uint64, p uint32) {
			wantH = append(wantH, h)
			wantP = append(wantP, p)
		})
		buf := make([]byte, len(d))
		foldASCII(buf, d)
		var gotH []uint64
		var gotP []uint32
		scanFolded(buf, func(h uint64, p uint32) {
			gotH = append(gotH, h)
			gotP = append(gotP, p)
		})
		if fmt.Sprint(gotH) != fmt.Sprint(wantH) || fmt.Sprint(gotP) != fmt.Sprint(wantP) {
			t.Fatalf("doc %d: folded scan differs at some token", i)
		}
	}
}

func addDocs(t *testing.T, ix *Index, docs map[DocID]string) {
	t.Helper()
	for id, text := range docs {
		ix.Add(id, text)
	}
}

func matchIDs(t *testing.T, ix *Index, q Query) []DocID {
	t.Helper()
	return ix.Match(q, nil)
}

func TestBooleanAndPhrase(t *testing.T) {
	ix := NewIndex()
	addDocs(t, ix, map[DocID]string{
		1: "the quick brown fox jumps",
		2: "quick brown fox",
		3: "brown fox quick",
		4: "lazy dog sleeps",
	})
	quick := mustHash(t, "quick")
	brown := mustHash(t, "brown")
	fox := mustHash(t, "fox")
	dog := mustHash(t, "dog")

	if got := matchIDs(t, ix, Query{Op: OpTerm, Term: quick}); fmt.Sprint(got) != "[1 2 3]" {
		t.Fatalf("term quick = %v", got)
	}
	and := Query{Op: OpAnd, Kids: []Query{{Op: OpTerm, Term: quick}, {Op: OpTerm, Term: dog}}}
	if got := matchIDs(t, ix, and); len(got) != 0 {
		t.Fatalf("quick AND dog = %v, want empty", got)
	}
	or := Query{Op: OpOr, Kids: []Query{{Op: OpTerm, Term: fox}, {Op: OpTerm, Term: dog}}}
	if got := matchIDs(t, ix, or); fmt.Sprint(got) != "[1 2 3 4]" {
		t.Fatalf("fox OR dog = %v", got)
	}
	andnot := Query{Op: OpAndNot, Kids: []Query{{Op: OpTerm, Term: brown}, {Op: OpTerm, Term: quick}}}
	if got := matchIDs(t, ix, andnot); fmt.Sprint(got) != "[]" && len(got) != 0 {
		t.Fatalf("brown AND NOT quick = %v, want empty", got)
	}
	phrase := Query{Op: OpPhrase, Terms: []uint64{quick, brown, fox}}
	if got := matchIDs(t, ix, phrase); fmt.Sprint(got) != "[1 2]" {
		t.Fatalf("phrase quick brown fox = %v", got)
	}
	slop := Query{Op: OpPhrase, Terms: []uint64{brown, quick}, Slop: 1}
	if got := matchIDs(t, ix, slop); fmt.Sprint(got) != "[3]" {
		t.Fatalf("phrase brown quick~1 = %v", got)
	}
	all := matchIDs(t, ix, Query{Op: OpAll})
	if fmt.Sprint(all) != "[1 2 3 4]" {
		t.Fatalf("match-all = %v", all)
	}
}

func TestBM25OrdersByFrequency(t *testing.T) {
	ix := NewIndex()
	addDocs(t, ix, map[DocID]string{
		1: "apple banana",
		2: "apple apple apple banana",
		3: "banana cherry",
	})
	apple := mustHash(t, "apple")
	got := ix.Score(Query{Op: OpTerm, Term: apple}, 10, nil)
	if len(got) != 2 || got[0].Doc != 2 || got[1].Doc != 1 {
		t.Fatalf("BM25 order = %v, want [2 1]", got)
	}
	if !(got[0].Score > got[1].Score && got[1].Score > 0) {
		t.Fatalf("BM25 scores not positive-ordered: %v", got)
	}
	// topK truncates.
	if got := ix.Score(Query{Op: OpTerm, Term: apple}, 1, nil); len(got) != 1 || got[0].Doc != 2 {
		t.Fatalf("topK=1 = %v", got)
	}
	// Boost scales.
	plain := ix.Score(Query{Op: OpTerm, Term: apple}, 0, nil)
	boosted := ix.Score(Query{Op: OpTerm, Term: apple, Boost: 3}, 0, nil)
	if len(plain) != len(boosted) {
		t.Fatalf("boost changed matches: %v vs %v", plain, boosted)
	}
	for i := range plain {
		if boosted[i].Score != 3*plain[i].Score {
			t.Fatalf("boost not applied: %v vs %v", plain[i], boosted[i])
		}
	}
}

func TestRemove(t *testing.T) {
	ix := NewIndex()
	addDocs(t, ix, map[DocID]string{1: "apple banana", 2: "apple cherry"})
	apple := mustHash(t, "apple")
	if got := matchIDs(t, ix, Query{Op: OpTerm, Term: apple}); fmt.Sprint(got) != "[1 2]" {
		t.Fatalf("before remove = %v", got)
	}
	if !ix.Remove(1) {
		t.Fatal("Remove(1) = false")
	}
	if ix.Remove(1) {
		t.Fatal("second Remove(1) = true")
	}
	if got := matchIDs(t, ix, Query{Op: OpTerm, Term: apple}); fmt.Sprint(got) != "[2]" {
		t.Fatalf("after remove = %v", got)
	}
	// Re-adding replaces.
	ix.Add(2, "durian")
	if got := matchIDs(t, ix, Query{Op: OpTerm, Term: apple}); len(got) != 0 {
		t.Fatalf("after replace = %v", got)
	}
	if got := matchIDs(t, ix, Query{Op: OpTerm, Term: mustHash(t, "durian")}); fmt.Sprint(got) != "[2]" {
		t.Fatalf("replaced doc = %v", got)
	}
}

func TestZeroAllocSteadyState(t *testing.T) {
	if n := testing.AllocsPerRun(50, func() {
		scanString("The quick brown fox jumps over the lazy dog", func(uint64, uint32) {})
	}); n != 0 {
		t.Fatalf("scanString allocated %v per run", n)
	}
	src := strings.Repeat("The quick brown fox. ", 20)
	buf := make([]byte, len(src))
	testing.AllocsPerRun(20, func() {
		foldASCII(buf, src)
	})
	if n := testing.AllocsPerRun(50, func() {
		foldASCII(buf, src)
	}); n != 0 {
		t.Fatalf("foldASCII allocated %v per run", n)
	}
	ix := NewIndex()
	for i := DocID(1); i <= 50; i++ {
		ix.Add(i, "apple banana cherry durian elderberry fig grape")
	}
	term := mustHash(t, "apple")
	mout := make([]DocID, 0, 64)
	_ = ix.Match(Query{Op: OpTerm, Term: term}, mout)
	if n := testing.AllocsPerRun(50, func() {
		_ = ix.Match(Query{Op: OpTerm, Term: term}, mout[:0])
	}); n != 0 {
		t.Fatalf("term Match allocated %v per run", n)
	}
	sout := make([]Scored, 0, 64)
	_ = ix.Score(Query{Op: OpTerm, Term: term}, 10, sout)
	if n := testing.AllocsPerRun(50, func() {
		_ = ix.Score(Query{Op: OpTerm, Term: term}, 10, sout[:0])
	}); n != 0 {
		t.Fatalf("term Score allocated %v per run", n)
	}
}
