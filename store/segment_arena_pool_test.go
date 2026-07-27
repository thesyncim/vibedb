package store

import (
	"fmt"
	"runtime"
	"testing"
)

// oversizeResetCorpus is a batch that does not fit one generation of either
// arena. Eighteen members index to thirty-seven entries, so four thousand
// documents need about 151,000 of them against the 65,536-entry ceiling, and
// their three hundred-odd bytes each need about 1.2 MB of source against the
// one-mebibyte ceiling. Both arenas therefore supersede a generation mid-fill,
// which is the case the recycling exists for and the case a narrower fixture
// silently never reaches.
func oversizeResetCorpus(documents int) [][]byte {
	docs := make([][]byte, documents)
	for i := range docs {
		docs[i] = fmt.Appendf(nil,
			`{"id":%d,"bucket":%d,"score":%d,"label":"row-%06d","active":%t,`+
				`"note":"lorem ipsum dolor sit amet consectetur adipiscing elit sed do %d",`+
				`"a1":%d,"a2":%d,"a3":%d,"a4":%d,"a5":%d,"a6":%d,"a7":%d,"a8":%d,`+
				`"b1":"tag-%06d","b2":"tag-%06d","b3":"tag-%06d","b4":"tag-%06d"}`,
			i, i%128, i*3, i, i%3 != 0, i,
			i, i+1, i+2, i+3, i+4, i+5, i+6, i+7, i, i%97, i%31, i%7)
	}
	return docs
}

// refillSegment empties the segment and fills it again, which is the cycle
// Reset exists for and the one the recycling has to survive.
func refillSegment(tb testing.TB, s *Segment, docs [][]byte) {
	tb.Helper()
	s.Reset()
	fillSegment(tb, s, docs)
}

// Given a batch too large for one arena generation, when a Segment is Reset and
// refilled with it, then the refill must allocate nothing.
//
// This is Reset's whole contract, stated for the case that used to break it.
// Arena chunks stop doubling at a ceiling, so a fill needing more than one
// chunk's worth supersedes the installed chunk and installs a successor —
// and Reset used to keep only the newest generation, so the very next fill
// superseded it at the same document and discarded the successor again. That is
// a megabyte of arena allocated and thrown away per batch, forever, which is
// what the byte budget here is for: it is thirty-seven allocations, a number no
// count budget would ever flag, and 37 MB, which is impossible to miss.
//
// Both budgets are asserted for that reason. Neither alone would have caught it.
func TestSegmentResetRecyclesOversizeArenasAlloc(t *testing.T) {
	const byteBudget = 4 << 10
	docs := oversizeResetCorpus(4096)
	var s Segment
	// Enough fills to converge: the first grows both arenas through every
	// generation, and the free list only holds a superseded generation from the
	// fill after the one that made it.
	for range 4 {
		refillSegment(t, &s, docs)
	}
	if len(s.entryPool.retired) == 0 {
		t.Fatal("the fixture never superseded an entry arena generation, so it " +
			"cannot show whether superseded generations are recycled")
	}
	if len(s.srcPool.retired) == 0 {
		t.Fatal("the fixture never superseded a source arena generation")
	}
	const passes = 4
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for range passes {
		refillSegment(t, &s, docs)
	}
	runtime.ReadMemStats(&after)
	allocs := (after.Mallocs - before.Mallocs) / passes
	bytes := (after.TotalAlloc - before.TotalAlloc) / passes
	t.Logf("refill of %d oversize documents: %d allocs, %d B", len(docs), allocs, bytes)
	if allocs != 0 {
		t.Fatalf("refilling a Reset Segment allocated %d times (%d B); Reset must "+
			"leave every superseded arena generation available to the next fill",
			allocs, bytes)
	}
	if bytes > byteBudget {
		t.Fatalf("refilling a Reset Segment allocated %d B, budget %d: an arena "+
			"generation is being re-bought behind a zero allocation count",
			bytes, byteBudget)
	}
}

// Given arena generations recycled from an earlier fill, when the documents
// built into them are read back, then every one must be byte-identical to a
// standalone build.
//
// A recycled chunk is handed out without its tail being cleared, and a
// superseded generation is handed back only because Reset has invalidated
// everything that pointed into it. Both are exactly the kind of reasoning that
// is right until it is not, so this reads every document of every fill rather
// than trusting either. Successive fills use different documents so that a chunk
// serving a second fill cannot accidentally agree with what the first left in it.
func TestSegmentResetRecycledArenaKeepsDocuments(t *testing.T) {
	var s Segment
	for pass := range 4 {
		docs := oversizeResetCorpus(2048)
		for i := range docs {
			docs[i] = fmt.Appendf(docs[i][:len(docs[i])-1], `,"pass":%d}`, pass)
		}
		refillSegment(t, &s, docs)
		strs := make([]string, len(docs))
		for i, doc := range docs {
			strs[i] = string(doc)
		}
		checkSegmentDifferential(t, &s, strs, fmt.Sprintf("recycled pass %d", pass))
	}
}

// Given many fills of varying size, when they have all run, then the free lists
// must stay bounded by the widest single fill rather than growing with the
// number of fills.
//
// This is the bound that keeps recycling from becoming a leak. It is a
// high-water mark and is deliberately not decayed: every decaying variant that
// was tried threw away, on some schedule or other, exactly the generations the
// next wide fill was about to ask for, because a batch consumer's fills are not
// uniform and the variation is not periodic. What holding the high-water costs
// is what this pins — one fill's worth of spare generations, which is the same
// order as the arena the Segment already has installed, and no more however long
// it runs.
func TestSegmentResetArenaFreeListStaysBounded(t *testing.T) {
	var s Segment
	wide := oversizeResetCorpus(8192)
	refillSegment(t, &s, wide)
	spike := len(s.entryPool.retired)
	if spike < 2 {
		t.Fatalf("the widest fill superseded %d entry generations, too few to "+
			"distinguish a bounded free list from an unbounded one", spike)
	}
	// Alternating wide and narrow fills is what a batch consumer does: every scan
	// ends in a short batch, and batches go to whichever worker is free. Sixty
	// fills is far more than any bound that could be expressed in fills.
	small := oversizeResetCorpus(8)
	for i := range 60 {
		if i%3 == 2 {
			refillSegment(t, &s, small)
		} else {
			refillSegment(t, &s, wide)
		}
	}
	for _, pool := range []struct {
		name string
		held int
	}{
		{"entry", len(s.entryPool.free) + len(s.entryPool.retired)},
		{"source", len(s.srcPool.free) + len(s.srcPool.retired)},
	} {
		if pool.held > spike+1 {
			t.Fatalf("after sixty fills the %s free list holds %d generations "+
				"against a widest fill of %d; the bound must be one fill's demand, "+
				"not the number of fills", pool.name, pool.held, spike)
		}
	}
}
