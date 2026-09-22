package tin

import (
	"testing"
)

// gatherShardCorpus splits docs across n shards with disjoint DocIDs:
// every doc carries "common", every 5th adds "zipf", and one shard in
// four holds an exact-tie group ("tied tie tie") so ties span shards.
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
