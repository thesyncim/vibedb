package tin

import (
	"reflect"
	"testing"
)

// gatherShardCorpus splits docs across n shards with disjoint DocIDs:
// every doc carries "common", every 5th adds "zipf", and one shard in
// four holds an exact-tie group ("tied tie tie") so ties span shards.
// Shard 0 holds a near-neighbor ("zipp") so fuzzy expansions stay
// multi-term and leave the pinned fast path.
func gatherShardCorpus(n, per int) []Shard {
	shards := make([]Shard, n)
	for s := range shards {
		ix := NewIndex()
		for i := 0; i < per; i++ {
			id := DocID(uint64(s*per+i) + 1)
			body := "common"
			for k := 0; k < (i*13)%24; k++ {
				body += " filler"
			}
			if i%5 == 0 {
				body += " zipf"
			}
			if s == 0 && i == 1 {
				body += " zipp"
			}
			if s%2 == 0 && i%7 == 0 {
				body = "tied tie tie"
			}
			ix.Add(id, body)
		}
		shards[s].Ix = ix
	}
	return shards
}

func gatherQuery(t *testing.T, shards []Shard, pattern string) Query {
	t.Helper()
	q, err := shards[0].Ix.ParseTINQL(pattern)
	if err != nil {
		t.Fatal(err)
	}
	return q
}

// TestScoreGatheredMatchesSequentialMerge proves the parallel gather
// changes nothing: gathering equals merging the sequential per-shard
// top-K runs, for terms, ANDs, phrases, ties, and full rankings.
func TestScoreGatheredMatchesSequentialMerge(t *testing.T) {
	shards := gatherShardCorpus(4, 2000)
	for _, pattern := range []string{"common", "zipf AND common", `"tied tie"`, "zipf"} {
		q := gatherQuery(t, shards, pattern)
		for _, topK := range []int{1, 10, 100, 0} {
			got := ScoreGathered(shards, q, topK, nil)
			var runs [][]Scored
			for i := range shards {
				runs = append(runs, shards[i].Ix.Score(q, topK, nil))
			}
			want := mergeScored(runs, topK, nil)
			if len(got) != len(want) {
				t.Fatalf("%s topK=%d: %d hits, want %d", pattern, topK, len(got), len(want))
			}
			for i := range got {
				if got[i] != want[i] {
					t.Fatalf("%s topK=%d hit %d: %+v != %+v", pattern, topK, i, got[i], want[i])
				}
			}
		}
	}
}

// TestGatherPoolMatchesSpawnGather proves the pool changes nothing:
// every pool size (fewer, equal, and more workers than shards) returns
// bit-identical rankings to the spawn-per-call gather over terms, ANDs,
// phrases, ties, and full rankings, with nil and empty shard sets
// covered. Shard buffers are fresh per size so no run aliases another.
func TestGatherPoolMatchesSpawnGather(t *testing.T) {
	for _, workers := range []int{1, 2, 6, 7, 32} {
		shards := gatherShardCorpus(6, 500)
		p := NewGatherPool(workers)
		for _, pattern := range []string{"common", "zipf AND common", `"tied tie"`, "zipf"} {
			q := gatherQuery(t, shards, pattern)
			for _, topK := range []int{1, 10, 0} {
				want := ScoreGathered(shards, q, topK, nil)
				for i := range shards {
					shards[i].Out = nil
				}
				got := p.Score(shards, q, topK, nil)
				if len(got) != len(want) {
					t.Fatalf("workers=%d %s topK=%d: %d hits, want %d", workers, pattern, topK, len(got), len(want))
				}
				for i := range got {
					if got[i] != want[i] {
						t.Fatalf("workers=%d %s topK=%d hit %d: %+v != %+v", workers, pattern, topK, i, got[i], want[i])
					}
				}
			}
		}
		p.Close()
	}
}

// TestGatherPoolPinnedMatchesSpawn proves the pinned pool twins change
// nothing: top-K and full rankings over terms and conjunctions equal the
// spawn-per-call segmented paths bit for bit (terms) and Doc order plus
// 1e-12 (conjunctions), including declines.
func TestGatherPoolPinnedMatchesSpawn(t *testing.T) {
	p := NewGatherPool(3)
	defer p.Close()
	shards := gatherShardCorpus(4, 2000)
	for _, pattern := range []string{
		"common", "zipf", "tied", "missing",
		`zipf AND common`, `tied AND common`, `common AND missing`,
		`zipf OR common`, `"tied tie"`, `*`,
	} {
		q := gatherQuery(t, shards, pattern)
		terms, shaped := segmentedTerms(q)
		var gs SegGlobals
		if shaped {
			gs = gatherSegGlobals(shards, terms)
		}
		for _, topK := range []int{1, 10, 100} {
			want, wantOK := ScoreSegmented(shards, q, topK, nil)
			for i := range shards {
				shards[i].Out = nil
			}
			got, gotOK := p.ScorePinned(shards, q, topK, &gs, nil)
			if gotOK != wantOK {
				t.Fatalf("%s topK=%d: pool ok=%v, spawn ok=%v", pattern, topK, gotOK, wantOK)
			}
			if !wantOK {
				continue
			}
			if len(got) != len(want) {
				t.Fatalf("%s topK=%d: %d hits, want %d", pattern, topK, len(got), len(want))
			}
			for i := range got {
				if got[i] != want[i] {
					t.Fatalf("%s topK=%d hit %d = %+v, want %+v", pattern, topK, i, got[i], want[i])
				}
			}
		}
		wantF, wantFOK := ScoreSegmentedFull(shards, q, nil)
		for i := range shards {
			shards[i].Out = nil
		}
		gotF, gotFOK := p.ScorePinnedFull(shards, q, &gs, nil)
		if gotFOK != wantFOK {
			t.Fatalf("%s full: pool ok=%v, spawn ok=%v", pattern, gotFOK, wantFOK)
		}
		if !wantFOK {
			continue
		}
		if len(gotF) != len(wantF) {
			t.Fatalf("%s full: %d hits, want %d", pattern, len(gotF), len(wantF))
		}
		for i := range gotF {
			if gotF[i].Doc != wantF[i].Doc {
				t.Fatalf("%s full hit %d doc = %v, want %v", pattern, i, gotF[i].Doc, wantF[i].Doc)
			}
			if !scoresClose(gotF[i].Score, wantF[i].Score) {
				t.Fatalf("%s full hit %d score = %v, want %v", pattern, i, gotF[i].Score, wantF[i].Score)
			}
		}
	}
}

// TestGatherPoolEdges covers degenerate inputs: no shards, all-nil
// shards, and a pool larger than the shard set on a single worker.
func TestGatherPoolEdges(t *testing.T) {
	p := NewGatherPool(4)
	defer p.Close()
	if got := p.Score(nil, Query{Op: OpTerm, Term: 1}, 10, nil); len(got) != 0 {
		t.Fatalf("empty shards: %v", got)
	}
	nils := []Shard{{}, {}, {}}
	if got := p.Score(nils, Query{Op: OpTerm, Term: 1}, 10, nil); len(got) != 0 {
		t.Fatalf("all-nil shards: %v", got)
	}
	one := NewGatherPool(1)
	defer one.Close()
	shards := gatherShardCorpus(1, 100)
	q := gatherQuery(t, shards, "common")
	want := ScoreGathered(shards, q, 5, nil)
	shards[0].Out = nil
	if got := one.Score(shards, q, 5, nil); len(got) != len(want) {
		t.Fatalf("single: %d hits, want %d", len(got), len(want))
	} else {
		for i := range got {
			if got[i] != want[i] {
				t.Fatalf("single hit %d: %+v != %+v", i, got[i], want[i])
			}
		}
	}
}

// TestGatherPoolSteadyAllocs pins the pool's warm budget at exactly three,
// independent of shard count: the runs slice, the pre-sized merge heap,
// and the shared WaitGroup (its address crosses goroutines, so it escapes
// by construction). The spawn-per-call path spends ~2 per shard on top.
func TestGatherPoolSteadyAllocs(t *testing.T) {
	shards := gatherShardCorpus(16, 500)
	p := NewGatherPool(8)
	defer p.Close()
	q := gatherQuery(t, shards, "common")
	var out []Scored
	for i := 0; i < 10; i++ {
		out = p.Score(shards, q, 10, out[:0])
	}
	if n := testing.AllocsPerRun(20, func() {
		out = p.Score(shards, q, 10, out[:0])
	}); n != 3 {
		t.Fatalf("pool Score allocated %v per run, want 3 (runs, heap, WaitGroup)", n)
	}
	_ = out
}

// TestScoreGatheredTopKSufficiency proves per-shard top-K suffices: the
// merged top-K over full per-shard rankings is identical, even with ties
// spanning every shard.
func TestScoreGatheredTopKSufficiency(t *testing.T) {
	shards := gatherShardCorpus(4, 2000)
	for _, pattern := range []string{"common", `"tied tie"`, "zipf AND common"} {
		q := gatherQuery(t, shards, pattern)
		const topK = 10
		narrow := ScoreGathered(shards, q, topK, nil)
		var full [][]Scored
		for i := range shards {
			full = append(full, shards[i].Ix.Score(q, 0, nil))
		}
		wide := mergeScored(full, topK, nil)
		if len(narrow) != len(wide) {
			t.Fatalf("%s: narrow %d hits, wide %d", pattern, len(narrow), len(wide))
		}
		for i := range narrow {
			if narrow[i] != wide[i] {
				t.Fatalf("%s hit %d: %+v != %+v", pattern, i, narrow[i], wide[i])
			}
		}
	}
}

// TestMatchGatheredUnion proves union semantics with dedupe: an
// overlapping shard contributes no duplicates, nil shards contribute
// nothing.
func TestMatchGatheredUnion(t *testing.T) {
	shards := gatherShardCorpus(3, 1000)
	overlap := NewIndex()
	overlap.Add(7, "common overlap")
	overlap.Add(1001, "common overlap")
	shards = append(shards, Shard{Ix: overlap}, Shard{})
	q := gatherQuery(t, shards, "common OR overlap")
	got := MatchGathered(shards, q, nil)
	seen := make(map[DocID]int)
	for _, id := range got {
		seen[id]++
		if seen[id] > 1 {
			t.Fatalf("duplicate DocID %d in union", id)
		}
	}
	var want int
	seenShard := make(map[DocID]bool)
	for i := range shards {
		if shards[i].Ix == nil {
			continue
		}
		for _, id := range shards[i].Ix.Match(q, nil) {
			if !seenShard[id] {
				seenShard[id] = true
				want++
			}
		}
	}
	if len(got) != want {
		t.Fatalf("union %d docs, want %d", len(got), want)
	}
	for i := 1; i < len(got); i++ {
		if got[i-1] >= got[i] {
			t.Fatalf("union not DocID-sorted at %d", i)
		}
	}
}

// TestMergeScoredUnit pins the merge contract directly: order, truncation,
// cross-run ties by DocID, dedupe, empties, and unbounded merge.
func TestMergeScoredUnit(t *testing.T) {
	a := []Scored{{Doc: 1, Score: 5}, {Doc: 3, Score: 3}, {Doc: 5, Score: 1}}
	b := []Scored{{Doc: 2, Score: 5}, {Doc: 3, Score: 2}, {Doc: 9, Score: 0.5}}
	got := mergeScored([][]Scored{a, b, nil, {}}, 4, nil)
	want := []Scored{{Doc: 1, Score: 5}, {Doc: 2, Score: 5}, {Doc: 3, Score: 3}, {Doc: 5, Score: 1}}
	if len(got) != len(want) {
		t.Fatalf("merged %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("hit %d: %+v != %+v", i, got[i], want[i])
		}
	}
	all := mergeScored([][]Scored{a, b}, 0, nil)
	if len(all) != 5 {
		t.Fatalf("unbounded merge %d hits, want 5 (3 + 3, one duplicate dropped)", len(all))
	}
	if len(mergeScored(nil, 10, nil)) != 0 {
		t.Fatal("no runs must merge empty")
	}
}

// gatherSingleCorpus rebuilds gatherShardCorpus's documents (same DocIDs,
// same texts) in one index: the single-index oracle for segmented search.
func gatherSingleCorpus(n, per int) *Index {
	ix := NewIndex()
	for s := 0; s < n; s++ {
		for i := 0; i < per; i++ {
			id := DocID(uint64(s*per+i) + 1)
			body := "common"
			for k := 0; k < (i*13)%24; k++ {
				body += " filler"
			}
			if i%5 == 0 {
				body += " zipf"
			}
			if s == 0 && i == 1 {
				body += " zipp"
			}
			if s%2 == 0 && i%7 == 0 {
				body = "tied tie tie"
			}
			ix.Add(id, body)
		}
	}
	return ix
}

// TestScoreSegmentedExact proves the pinned segmented top-K is the single
// index's top-K bit for bit: terms, ANDs, boosts, missing terms, terms
// absent from whole shards, duplicate kids, cross-shard ties, and empty
// and nil shards. Shapes outside the pinned fast path decline.
func TestScoreSegmentedExact(t *testing.T) {
	shards := gatherShardCorpus(4, 2000)
	shards = append(shards, Shard{Ix: NewIndex()}, Shard{})
	single := gatherSingleCorpus(4, 2000)
	exact := []string{
		"common", "zipf", "tied", "missing",
		"zipf AND common", "tied AND common", "common AND missing",
		"common^2", "zipf^0.5 AND common",
	}
	for _, pattern := range exact {
		q := gatherQuery(t, shards, pattern)
		for _, topK := range []int{1, 10, 100} {
			got, ok := ScoreSegmented(shards, q, topK, nil)
			if !ok {
				t.Fatalf("%s topK=%d declined, want exact", pattern, topK)
			}
			want := single.Score(q, topK, nil)
			if len(got) != len(want) {
				t.Fatalf("%s topK=%d: %d hits, want %d", pattern, topK, len(got), len(want))
			}
			for i := range got {
				if got[i] != want[i] {
					t.Fatalf("%s topK=%d hit %d = %+v, want %+v", pattern, topK, i, got[i], want[i])
				}
			}
		}
	}
	// `zipf AND zipf` declines too: its intersection is the list itself,
	// which trips the unselective fast gate in the single index as well.
	decline := []string{`"tied tie"`, `zipf OR common`, `zipf~1`, `zip*`, `zipf THEN/0 common`, `*`, `zipf AND zipf`}
	for _, pattern := range decline {
		q := gatherQuery(t, shards, pattern)
		if _, ok := ScoreSegmented(shards, q, 10, nil); ok {
			t.Fatalf("%s segmented without pinned support, want decline", pattern)
		}
	}
}

// TestScoreSegmentedFullExact proves ScoreSegmentedFull merges the exact
// full ranking over open and sealed shards, empty and nil shards, and
// missing terms. Lone terms merge bit-identically; all-term conjunctions
// accumulate shortest-first by the shared df on every shard, so the
// merged ranking carries the same Doc order with scores agreeing to
// 1e-12 (the AND contract in score_and.go) — the corpus below exhibits
// no near-tie reorder, and any future one fails loudly here.
func TestScoreSegmentedFullExact(t *testing.T) {
	for _, sealed := range []bool{false, true} {
		shards := gatherShardCorpus(4, 2000)
		shards = append(shards, Shard{Ix: NewIndex()}, Shard{})
		if sealed {
			for i := range shards {
				if shards[i].Ix != nil {
					shards[i].Ix.Seal()
				}
			}
		}
		single := gatherSingleCorpus(4, 2000)
		for _, pattern := range []string{
			"common", "zipf", "tied", "missing", "common^2", "zipf^0.5",
		} {
			q := gatherQuery(t, shards, pattern)
			got, ok := ScoreSegmentedFull(shards, q, nil)
			if !ok {
				t.Fatalf("sealed=%v %s declined, want exact", sealed, pattern)
			}
			want := single.Score(q, 0, nil)
			if len(got) != len(want) {
				t.Fatalf("sealed=%v %s: %d hits, want %d", sealed, pattern, len(got), len(want))
			}
			for i := range got {
				if got[i] != want[i] {
					t.Fatalf("sealed=%v %s hit %d = %+v, want %+v", sealed, pattern, i, got[i], want[i])
				}
			}
		}
		for _, pattern := range []string{
			`zipf AND common`, `tied AND common`, `common AND missing`,
			`zipf^0.5 AND common`,
		} {
			q := gatherQuery(t, shards, pattern)
			got, ok := ScoreSegmentedFull(shards, q, nil)
			if !ok {
				t.Fatalf("sealed=%v %s declined, want full AND", sealed, pattern)
			}
			want := single.Score(q, 0, nil)
			if len(got) != len(want) {
				t.Fatalf("sealed=%v %s: %d hits, want %d", sealed, pattern, len(got), len(want))
			}
			for i := range got {
				if got[i].Doc != want[i].Doc {
					t.Fatalf("sealed=%v %s hit %d doc = %v, want %v", sealed, pattern, i, got[i].Doc, want[i].Doc)
				}
				if !scoresClose(got[i].Score, want[i].Score) {
					t.Fatalf("sealed=%v %s hit %d score = %v, want %v", sealed, pattern, i, got[i].Score, want[i].Score)
				}
			}
		}
		for _, pattern := range []string{
			`"tied tie"`, `zipf OR common`, `*`,
		} {
			q := gatherQuery(t, shards, pattern)
			if _, ok := ScoreSegmentedFull(shards, q, nil); ok {
				t.Fatalf("sealed=%v %s segmented without full support, want decline", sealed, pattern)
			}
		}
	}
}

// TestScoreSegmentedShippedExact proves the distributed query path: shards
// shipped as wire bundles and reopened on the receiving side serve
// ScoreSegmented, ScoreSegmentedFull, and MatchGathered bit-identically to
// the single index. Shipped indexes arrive fully sealed with cold caches,
// which live-built shards never exercise. The small-doc shards also pin
// the segment doc-count floor: their wire runs near five bytes per
// document, which the old nine-byte floor false-rejected.
func TestScoreSegmentedShippedExact(t *testing.T) {
	live := gatherShardCorpus(4, 2000)
	var shards []Shard
	for _, sh := range live {
		sh.Ix.Seal()
		wire, err := MarshalSegment(sh.Ix.ExportSegment())
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		seg, err := UnmarshalSegment(wire)
		if err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		rx, err := OpenSegment(seg)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		shards = append(shards, Shard{Ix: rx})
	}
	shards = append(shards, Shard{Ix: NewIndex()}, Shard{})
	single := gatherSingleCorpus(4, 2000)
	for _, pattern := range []string{"common", "zipf", "tied", "missing", "zipf AND common"} {
		q := gatherQuery(t, shards, pattern)
		got, ok := ScoreSegmented(shards, q, 10, nil)
		if !ok {
			t.Fatalf("%s declined, want exact", pattern)
		}
		if want := single.Score(q, 10, nil); !reflect.DeepEqual(got, want) {
			t.Fatalf("%s top-10 differs", pattern)
		}
	}
	for _, pattern := range []string{"common", "zipf AND common", `"tied tie"`, `zipf OR common`} {
		q := gatherQuery(t, shards, pattern)
		if got := MatchGathered(shards, q, nil); !reflect.DeepEqual(got, single.Match(q, nil)) {
			t.Fatalf("%s match differs", pattern)
		}
	}
	for _, pattern := range []string{"common", "zipf", "tied", "missing"} {
		q := gatherQuery(t, shards, pattern)
		got, ok := ScoreSegmentedFull(shards, q, nil)
		if !ok {
			t.Fatalf("%s full declined, want exact", pattern)
		}
		if want := single.Score(q, 0, nil); !reflect.DeepEqual(got, want) {
			t.Fatalf("%s full ranking differs", pattern)
		}
	}
	// Full conjunctions over shipped bundles: same Doc order, 1e-12
	// scores (the AND contract), exercising the sealed import paths.
	for _, pattern := range []string{`zipf AND common`, `tied AND common`} {
		q := gatherQuery(t, shards, pattern)
		got, ok := ScoreSegmentedFull(shards, q, nil)
		if !ok {
			t.Fatalf("%s full declined, want full AND", pattern)
		}
		want := single.Score(q, 0, nil)
		if len(got) != len(want) {
			t.Fatalf("%s: %d hits, want %d", pattern, len(got), len(want))
		}
		for i := range got {
			if got[i].Doc != want[i].Doc {
				t.Fatalf("%s hit %d doc = %v, want %v", pattern, i, got[i].Doc, want[i].Doc)
			}
			if !scoresClose(got[i].Score, want[i].Score) {
				t.Fatalf("%s hit %d score = %v, want %v", pattern, i, got[i].Score, want[i].Score)
			}
		}
	}
}

// TestScoreSegmentedMasksExact proves MatchGathered unions every shape
// exactly, including the ones ScoreSegmented declines: masks never score.
func TestScoreSegmentedMasksExact(t *testing.T) {
	shards := gatherShardCorpus(4, 2000)
	shards = append(shards, Shard{Ix: NewIndex()}, Shard{})
	single := gatherSingleCorpus(4, 2000)
	for _, pattern := range []string{
		"common", "zipf AND common", `"tied tie"`, `zipf OR common`,
		`zipf~1`, `zip*`, `zipf THEN/0 common`, `common^2`,
	} {
		q := gatherQuery(t, shards, pattern)
		got := MatchGathered(shards, q, nil)
		want := single.Match(q, nil)
		if len(got) != len(want) {
			t.Fatalf("%s: %d docs, want %d", pattern, len(got), len(want))
		}
		for i := range got {
			if got[i] != want[i] {
				t.Fatalf("%s doc %d = %d, want %d", pattern, i, got[i], want[i])
			}
		}
	}
}

// TestRefreshSegmentedStatsExact proves segmented statistics are the
// single index's bit for bit: totals, term frequencies, and span-node
// document frequencies across terms, booleans, phrases, proximity,
// filters, and boosts.
func TestRefreshSegmentedStatsExact(t *testing.T) {
	shards := gatherShardCorpus(4, 2000)
	single := gatherSingleCorpus(4, 2000)
	for _, pattern := range []string{
		"common", "zipf AND common", `"tied tie"`, `zipf OR common`,
		`common NEAR/3 zipf`, `tied THEN/0 tie`, `zipf~1`, `zip*`,
		`common IN FIRST 100 WORDS`, `common^2`,
		`AT LEAST 2 OF [common zipf tied]`, `missing`,
	} {
		q := gatherQuery(t, shards, pattern)
		got := RefreshSegmentedStats(shards, q, nil)
		want := single.RefreshScoreStats(q, nil)
		if got.nDocs != want.nDocs || got.avg != want.avg {
			t.Fatalf("%s: totals (%d, %v), want (%d, %v)",
				pattern, got.nDocs, got.avg, want.nDocs, want.avg)
		}
		if len(got.idfs) != len(want.idfs) {
			t.Fatalf("%s: %d idfs, want %d", pattern, len(got.idfs), len(want.idfs))
		}
		for i := range got.idfs {
			if got.idfs[i] != want.idfs[i] {
				t.Fatalf("%s idf %d = %v, want %v", pattern, i, got.idfs[i], want.idfs[i])
			}
		}
	}
}

// TestGatherPoolMatchMatchesSpawn proves the pooled match twins change
// nothing: Match and MatchQueries equal the spawn-per-call gathers DocID
// for DocID over terms, ANDs, ORs, and phrases, at every pool size, with
// a nil shard and a short queries slice covered. DocOut buffers reset
// between runs so no run aliases another.
func TestGatherPoolMatchMatchesSpawn(t *testing.T) {
	for _, workers := range []int{1, 2, 6, 7} {
		shards := gatherShardCorpus(6, 500)
		shards = append(shards, Shard{})
		p := NewGatherPool(workers)
		for _, pattern := range []string{"common", "zipf AND common", "common OR zipf", `"tied tie"`} {
			q := gatherQuery(t, shards, pattern)
			want := MatchGathered(shards, q, nil)
			for i := range shards {
				shards[i].DocOut = nil
			}
			if got := p.Match(shards, q, nil); !reflect.DeepEqual(got, want) {
				t.Fatalf("workers=%d %s: %d ids, want %d", workers, pattern, len(got), len(want))
			}
			queries := make([]Query, len(shards))
			for i := range shards {
				if shards[i].Ix == nil {
					continue
				}
				var err error
				queries[i], err = shards[i].Ix.ParseTINQL(pattern)
				if err != nil {
					t.Fatal(err)
				}
			}
			wantQ := MatchGatheredQueries(shards, queries, nil)
			for i := range shards {
				shards[i].DocOut = nil
			}
			if got := p.MatchQueries(shards, queries, nil); !reflect.DeepEqual(got, wantQ) {
				t.Fatalf("workers=%d %s queries: %d ids, want %d", workers, pattern, len(got), len(wantQ))
			}
			// A queries slice shorter than shards serves the prefix.
			short := queries[:len(shards)-2]
			wantS := MatchGatheredQueries(shards, short, nil)
			for i := range shards {
				shards[i].DocOut = nil
			}
			if got := p.MatchQueries(shards, short, nil); !reflect.DeepEqual(got, wantS) {
				t.Fatalf("workers=%d %s short: %d ids, want %d", workers, pattern, len(got), len(wantS))
			}
		}
		p.Close()
	}
}
