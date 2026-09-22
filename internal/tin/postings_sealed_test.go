package tin

import (
	"fmt"
	"strings"
	"testing"
)

// Sealed postings must read back exactly what the open layout holds, in
// every operator, at every block size, while costing a fraction of the
// space and no per-query allocations.

// TestPackRoundTripWidths pins the bit codec directly: every width 1..32,
// lengths straddling word boundaries, and extreme values including the
// uint32 maximum.
func TestPackRoundTripWidths(t *testing.T) {
	for w := uint8(1); w <= 32; w++ {
		for _, n := range []int{1, 2, 3, 4, 5, 7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 65, 100} {
			vals := make([]uint32, n)
			var max uint32
			if w == 32 {
				max = ^uint32(0)
			} else {
				max = uint32(1)<<w - 1
			}
			for i := range vals {
				// Span the code space: extremes plus a striding middle.
				switch i % 4 {
				case 0:
					vals[i] = 0
				case 1:
					vals[i] = max
				case 2:
					vals[i] = max >> 1
				default:
					vals[i] = uint32(i*2654435761) & max
				}
			}
			words := packAppend(vals, w, nil)
			if want := (n*int(w) + 31) / 32; len(words) != want {
				t.Fatalf("width %d n %d: %d words, want %d", w, n, len(words), want)
			}
			r := packReaderAt(words, 0)
			for i, want := range vals {
				if got := r.next(w); got != want {
					t.Fatalf("width %d n %d val %d: got %d, want %d", w, n, i, got, want)
				}
			}
			// Seek to the midpoint and resume: random access must agree.
			if n > 2 {
				mid := n / 2
				r := packReaderSeek(words, 0, uint32(mid)*uint32(w))
				for i := mid; i < n; i++ {
					if got := r.next(w); got != vals[i] {
						t.Fatalf("width %d seek %d val %d: got %d, want %d", w, mid, i, got, vals[i])
					}
				}
			}
		}
	}
}

// sealedCorpusDocs mixes document shapes across block and checkpoint
// boundaries: a shared term over 300 documents (three blocks), high
// frequencies, sparse heap ids (wide id deltas), and a long filler run
// (wide position gaps).
func sealedCorpusDocs() map[DocID]string {
	docs := map[DocID]string{
		1:     "fuji apple juicy red pie",
		2:     "apple fuji pie",
		3:     "apple apple apple banana",
		4:     "lazy dog sleeps all day",
		5:     "security critical threat level",
		10000: "sparse apple orchard",
		1 << 20: "wider apple gap",
		1 << 30: "widest apple delta",
	}
	for i := 0; i < 300; i++ {
		id := DocID(50 + i)
		text := "common filler words here"
		if i%7 == 0 {
			text += " apple apple apple"
		}
		if i%29 == 0 {
			text += " " + strings.Repeat("repeat ", 40)
		}
		docs[id] = text
	}
	docs[1000] = strings.Repeat("pad ", 70000) + "needle apple"
	return docs
}

func sealTwin(t *testing.T, docs map[DocID]string) (*Index, *Index) {
	t.Helper()
	open := NewIndex()
	sealed := NewIndex()
	for id, text := range docs {
		open.Add(id, text)
		sealed.Add(id, text)
	}
	sealed.Seal()
	// Rare terms may stay open when the directory would cost more than
	// the rows (pinned in TestSealedSpace); callers assert their own
	// must-seal terms.
	for _, p := range sealed.post {
		if p.sealed != nil && (p.ids != nil || p.off != nil || p.pos != nil) {
			t.Fatal("sealed list retains open arrays")
		}
	}
	return open, sealed
}

func matchString(ix *Index, q Query) string {
	return fmt.Sprint(ix.Match(q, nil))
}

func scoreString(ix *Index, q Query) string {
	return fmt.Sprint(ix.Score(q, 0, nil))
}

// TestSealedAgreesWithOpen runs the whole operator corpus against sealed
// and open twins: matches and BM25 scores must agree exactly (identical
// frequencies feed the same kernel, so bit-identity is the bar).
func TestSealedAgreesWithOpen(t *testing.T) {
	docs := sealedCorpusDocs()
	open, sealed := sealTwin(t, docs)
	if p := sealed.post[mustHash(t, "common")]; p == nil || p.sealed == nil {
		t.Fatal("common term did not seal")
	}
	for _, input := range scoreCorpusInputs {
		q, err := open.ParseTINQL(input)
		if err != nil {
			t.Fatalf("ParseTINQL(%q): %v", input, err)
		}
		if got, want := matchString(sealed, q), matchString(open, q); got != want {
			t.Fatalf("%q match:\n sealed %s\n open   %s", input, got, want)
		}
		if got, want := scoreString(sealed, q), scoreString(open, q); got != want {
			t.Fatalf("%q score:\n sealed %s\n open   %s", input, got, want)
		}
	}
}

// TestSealedBlockBoundaries pins exact agreement at every structural edge:
// empty-adjacent sizes, block multiples, and checkpoints, plus unseal
// restoring the open rows bit-for-bit.
func TestSealedBlockBoundaries(t *testing.T) {
	for _, n := range []int{1, 2, 15, 16, 17, 127, 128, 129, 255, 256, 300} {
		docs := make(map[DocID]string, n+1)
		for i := 0; i < n; i++ {
			docs[DocID(i+1)] = "shared term plus unique"
		}
		docs[DocID(n+1)] = "shared shared shared"
		open, sealed := sealTwin(t, docs)
		// Small lists stay open under the adoption policy; force-seal
		// them so the boundary sizes prove the packed readers, not the
		// open ones.
		for hash, sp := range sealed.post {
			if sp.sealed == nil {
				if s := sealPostings(open.post[hash]); s != nil {
					sp.sealed = s
					sp.ids, sp.off, sp.pos = nil, nil, nil
				}
			}
		}
		q := Query{Op: OpTerm, Term: mustHash(t, "shared")}
		if got, want := matchString(sealed, q), matchString(open, q); got != want {
			t.Fatalf("n=%d match:\n sealed %s\n open   %s", n, got, want)
		}
		if got, want := scoreString(sealed, q), scoreString(open, q); got != want {
			t.Fatalf("n=%d score:\n sealed %s\n open   %s", n, got, want)
		}
		pq, err := open.ParseTINQL(`"shared term"`)
		if err != nil {
			t.Fatal(err)
		}
		if got, want := matchString(sealed, pq), matchString(open, pq); got != want {
			t.Fatalf("n=%d phrase:\n sealed %s\n open   %s", n, got, want)
		}
		// Unseal restores the open rows exactly.
		for hash, sp := range sealed.post {
			op := open.post[hash]
			sp.openSealed()
			if fmt.Sprint(sp.ids) != fmt.Sprint(op.ids) ||
				fmt.Sprint(sp.off) != fmt.Sprint(op.off) ||
				fmt.Sprint(sp.pos) != fmt.Sprint(op.pos) {
				t.Fatalf("n=%d term %x: unseal differs", n, hash)
			}
		}
	}
}

// TestSealedUnsealMutate proves Seal is an optimization, not a lock:
// Adds (replace and new) and Removes on a sealed index agree with a
// freshly built index holding the same final documents.
func TestSealedUnsealMutate(t *testing.T) {
	docs := sealedCorpusDocs()
	fresh := NewIndex()
	sealed := NewIndex()
	for id, text := range docs {
		fresh.Add(id, text)
		sealed.Add(id, text)
	}
	sealed.Seal()
	sealed.Add(1, "fuji apple replaced")
	fresh.Add(1, "fuji apple replaced")
	sealed.Add(5000, "brand new apple document")
	fresh.Add(5000, "brand new apple document")
	if !sealed.Remove(2) || !fresh.Remove(2) {
		t.Fatal("remove reported absent")
	}
	for _, input := range scoreCorpusInputs {
		q, err := fresh.ParseTINQL(input)
		if err != nil {
			t.Fatalf("ParseTINQL(%q): %v", input, err)
		}
		if got, want := matchString(sealed, q), matchString(fresh, q); got != want {
			t.Fatalf("%q match after mutate:\n sealed %s\n fresh  %s", input, got, want)
		}
		if got, want := scoreString(sealed, q), scoreString(fresh, q); got != want {
			t.Fatalf("%q score after mutate:\n sealed %s\n fresh  %s", input, got, want)
		}
	}
}

// TestSealedZeroAlloc pins the sealed readers' warm budget: term Match and
// Score over a sealed multi-block index must not touch the heap once
// outputs fit — the same contract the open readers keep.
func TestSealedZeroAlloc(t *testing.T) {
	ix := NewIndex()
	for id, text := range sealedCorpusDocs() {
		ix.Add(id, text)
	}
	ix.Seal()
	q, err := ix.ParseTINQL("apple")
	if err != nil {
		t.Fatal(err)
	}
	out := ix.Match(q, nil)
	out = out[:0]
	ix.Match(q, out)
	if n := testing.AllocsPerRun(50, func() {
		ix.Match(q, out)
	}); n != 0 {
		t.Fatalf("sealed Match allocated %v per run, want 0", n)
	}
	scored := ix.Score(q, 0, nil)
	scored = scored[:0]
	ix.Score(q, 0, scored)
	if n := testing.AllocsPerRun(50, func() {
		ix.Score(q, 0, scored)
	}); n != 0 {
		t.Fatalf("sealed Score allocated %v per run, want 0", n)
	}
}

// openBytes tallies the open footprint the space test pins: 8 bytes per id,
// 4 per offset boundary, 4 per position.
func openBytes(ix *Index) uint64 {
	var total uint64
	for _, p := range ix.post {
		total += 8*uint64(len(p.ids)) + 4*uint64(len(p.off)) + 4*uint64(len(p.pos))
	}
	return total
}

// sealedBenchIndex builds a 2000-document corpus twins: open and sealed.
// Every document holds the common term; every seventh holds the selective
// term with triple frequency; every twenty-ninth repeats one word forty
// times for wide counts and positions.
func sealedBenchIndex(seal bool) *Index {
	ix := NewIndex()
	for i := 0; i < 2000; i++ {
		text := "common filler words here"
		if i%7 == 0 {
			text += " selective selective selective"
		}
		if i%29 == 0 {
			text += " " + strings.Repeat("repeat ", 40)
		}
		ix.Add(DocID(i+1), text)
	}
	if seal {
		ix.Seal()
	}
	return ix
}

func benchmarkMatchTerm(b *testing.B, ix *Index, term string) {
	q := Query{Op: OpTerm, Term: mustHash(b, term)}
	out := ix.Match(q, nil)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out = ix.Match(q, out[:0])
	}
	_ = out
}

func BenchmarkMatchTermOpen(b *testing.B) { benchmarkMatchTerm(b, sealedBenchIndex(false), "common") }
func BenchmarkMatchTermSealed(b *testing.B) { benchmarkMatchTerm(b, sealedBenchIndex(true), "common") }

func benchmarkScoreTerm(b *testing.B, ix *Index, term string) {
	q := Query{Op: OpTerm, Term: mustHash(b, term)}
	out := ix.Score(q, 0, nil)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out = ix.Score(q, 0, out[:0])
	}
	_ = out
}

func BenchmarkScoreTermOpen(b *testing.B)   { benchmarkScoreTerm(b, sealedBenchIndex(false), "common") }
func BenchmarkScoreTermSealed(b *testing.B) { benchmarkScoreTerm(b, sealedBenchIndex(true), "common") }

func benchmarkMatchAnd(b *testing.B, ix *Index) {
	q, err := ix.ParseTINQL("common AND selective")
	if err != nil {
		b.Fatal(err)
	}
	out := ix.Match(q, nil)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out = ix.Match(q, out[:0])
	}
	_ = out
}

func BenchmarkMatchAndOpen(b *testing.B)   { benchmarkMatchAnd(b, sealedBenchIndex(false)) }
func BenchmarkMatchAndSealed(b *testing.B) { benchmarkMatchAnd(b, sealedBenchIndex(true)) }

// skewedBenchIndex holds 20000 documents sharing one term with a rare term
// in twelve: the canonical selective-conjunction shape, where probing must
// skip the long list's decode.
func skewedBenchIndex(seal bool) *Index {
	ix := NewIndex()
	for i := 0; i < 20000; i++ {
		text := "common filler words here"
		if i%1700 == 0 {
			text += " needle"
		}
		ix.Add(DocID(i+1), text)
	}
	if seal {
		ix.Seal()
	}
	return ix
}

func benchmarkMatchSkewed(b *testing.B, ix *Index) {
	q, err := ix.ParseTINQL("needle AND common")
	if err != nil {
		b.Fatal(err)
	}
	out := ix.Match(q, nil)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out = ix.Match(q, out[:0])
	}
	_ = out
}

func BenchmarkMatchSkewedOpen(b *testing.B)   { benchmarkMatchSkewed(b, skewedBenchIndex(false)) }
func BenchmarkMatchSkewedSealed(b *testing.B) { benchmarkMatchSkewed(b, skewedBenchIndex(true)) }

func benchmarkMatchPhrase(b *testing.B, ix *Index) {
	q, err := ix.ParseTINQL(`"filler words"`)
	if err != nil {
		b.Fatal(err)
	}
	out := ix.Match(q, nil)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out = ix.Match(q, out[:0])
	}
	_ = out
}

func BenchmarkMatchPhraseOpen(b *testing.B)   { benchmarkMatchPhrase(b, sealedBenchIndex(false)) }
func BenchmarkMatchPhraseSealed(b *testing.B) { benchmarkMatchPhrase(b, sealedBenchIndex(true)) }

func sealedBytes(ix *Index) uint64 {
	var total uint64
	for _, p := range ix.post {
		// Open leftovers (rare terms the directory would bloat) count
		// at open rates: the tally stays honest.
		if p.sealed == nil {
			total += openBytesOf(p)
			continue
		}
		total += p.sealed.sealedBytes()
	}
	return total
}

// TestSealedSpace proves the space win on a mixed corpus: sealed costs at
// most half the open footprint overall, every list shrinks, and a list
// whose id gap escapes 32 bits stays open (correctness over compression).
func TestSealedSpace(t *testing.T) {
	docs := sealedCorpusDocs()
	open := NewIndex()
	for id, text := range docs {
		open.Add(id, text)
	}
	wantOpen := openBytes(open)
	sealed := NewIndex()
	for id, text := range docs {
		sealed.Add(id, text)
	}
	sealed.Seal()
	gotSealed := sealedBytes(sealed)
	if gotSealed*2 > wantOpen {
		t.Fatalf("sealed %d bytes, want at most half of open %d", gotSealed, wantOpen)
	}
	// The adoption policy, pinned per list: sealed means smaller, open
	// means the packed form was nil (unsealable gap) or no smaller.
	for hash, sp := range sealed.post {
		openList := openBytesOf(open.post[hash])
		if sp.sealed != nil {
			if sb := sp.sealed.sealedBytes(); sb >= openList {
				t.Fatalf("term %x: sealed %d bytes >= open %d", hash, sb, openList)
			}
			continue
		}
		if s := sealPostings(open.post[hash]); s != nil && s.sealedBytes() < openList {
			t.Fatalf("term %x: open at %d bytes but packed would cost %d", hash, openList, s.sealedBytes())
		}
	}
	t.Logf("sealed %d bytes vs open %d (%.2fx)", gotSealed, wantOpen, float64(wantOpen)/float64(gotSealed))

	// A 40-bit id gap cannot delta-code: the list stays open and exact.
	ix := NewIndex()
	ix.Add(1, "gap term")
	ix.Add(DocID(1)<<40, "gap term")
	ix.Seal()
	p := ix.post[mustHash(t, "gap")]
	if p == nil || p.sealed != nil {
		t.Fatal("wide-gap list should stay open")
	}
	if got := matchString(ix, Query{Op: OpTerm, Term: mustHash(t, "gap")}); got != "[1 1099511627776]" {
		t.Fatalf("wide-gap match = %s", got)
	}
}
