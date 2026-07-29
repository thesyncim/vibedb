package store

import (
	"bytes"
	"fmt"
	"runtime"
	"testing"

	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/document"
)

// resetCorpus builds one deterministic batch of documents. tag varies the
// content so a refilled segment cannot pass a differential by accidentally
// re-storing the bytes it already held, and members varies the layout so the
// shape cache sees more than one.
func resetCorpus(tag string, count, members int) [][]byte {
	docs := make([][]byte, count)
	for i := range docs {
		doc := fmt.Appendf(nil, `{"tag":%q,"id":%d,"label":"%s-%04d"`, tag, i, tag, i)
		for m := range members {
			doc = fmt.Appendf(doc, `,"f%d":%d`, m, i*(m+1))
		}
		if i%3 == 0 {
			// Every third document carries a nested member, so the corpus is a
			// mix of shape-conforming and classic documents in both storage
			// modes rather than uniformly one of them.
			doc = fmt.Appendf(doc, `,"nested":{"a":%d}`, i)
		}
		docs[i] = append(doc, '}')
	}
	return docs
}

func fillSegment(tb testing.TB, s *Segment, docs [][]byte) {
	tb.Helper()
	for i, doc := range docs {
		if _, err := s.Append(doc); err != nil {
			tb.Fatalf("append %d: %v", i, err)
		}
	}
}

// readSegment renders everything a caller can observe about a segment's
// contents: each document's source, its full classic tape (via Doc, which also
// exercises the shape-tape widening path), and the values a compiled pointer
// resolves for the batch. Two segments holding the same documents must produce
// byte-identical renderings whatever storage form each chose internally.
func readSegment(tb testing.TB, s *Segment) []byte {
	tb.Helper()
	var out []byte
	for i := 0; i < s.Len(); i++ {
		doc := s.Doc(i)
		out = fmt.Appendf(out, "doc %d src %s\n", i, doc.Src)
		for e := range doc.Entries {
			entry := doc.Entries[e]
			out = fmt.Appendf(out, "  e%d %d:%d next=%d info=%d\n",
				e, entry.Start, entry.End, entry.Next, entry.Info)
		}
	}
	for _, path := range []string{"/id", "/label", "/f0", "/nested/a", "/missing"} {
		pointer, err := vibejson.CompilePointer(path)
		if err != nil {
			tb.Fatal(err)
		}
		values, err := s.AppendPointer(nil, pointer)
		if err != nil {
			tb.Fatalf("pointer %s: %v", path, err)
		}
		for i, v := range values {
			out = fmt.Appendf(out, "%s[%d] = %s\n", path, i, v.Bytes())
		}
	}
	return out
}

// Given a segment filled, read to completion, and Reset, when it is refilled
// with a different corpus, then every read must be byte-identical to a segment
// that only ever held the second corpus.
//
// This is Reset's whole correctness claim, and it is asserted as a differential
// rather than as a list of field checks because the failure modes are all
// "some retained table still describes the documents that are gone". The
// segment is deliberately read to completion before the Reset — Doc on every
// document, which under ShapeTapes populates the widened-tape cache keyed by
// ordinal — so a Reset that kept that cache would answer Doc(0) after the
// refill with the previous corpus's first document. The corpora differ in
// length as well as content, in both directions, so a stale document table, a
// stale shape-header table, or an arena whose length was not rewound shows up
// as a wrong span rather than merely a wrong count.
func TestSegmentResetRefillMatchesFreshSegment(t *testing.T) {
	for _, mode := range []struct {
		name    string
		segment func() *Segment
	}{
		{"classic", func() *Segment { return &Segment{} }},
		{"hashed", func() *Segment {
			return &Segment{Options: document.IndexOptions{HashKeys: true}}
		}},
		{"shape", func() *Segment { return &Segment{ShapeTapes: true} }},
		{"shape+postings", func() *Segment { return &Segment{ShapeTapes: true, Postings: true} }},
		{"valuedict", func() *Segment { return &Segment{ShapeTapes: true, ValueDict: true, valueFloor: 1} }},
	} {
		t.Run(mode.name, func(t *testing.T) {
			reused := mode.segment()
			// Three fills of decreasing then increasing size. Shrinking is the
			// case that exposes an unrewound length; growing is the case that
			// exposes an arena generation the previous fill retired.
			corpora := [][][]byte{
				resetCorpus("first", 40, 6),
				resetCorpus("second", 7, 3),
				resetCorpus("third", 90, 9),
			}
			for round, docs := range corpora {
				reused.Reset()
				fillSegment(t, reused, docs)
				// Reading the whole segment before the next Reset is what makes
				// the widened-tape cache non-empty at the moment it is cleared.
				got := readSegment(t, reused)

				fresh := mode.segment()
				fillSegment(t, fresh, docs)
				want := readSegment(t, fresh)
				if !bytes.Equal(got, want) {
					t.Fatalf("round %d: refilled segment reads differently from a fresh one\n got %d bytes\nwant %d bytes",
						round, len(got), len(want))
				}
				if reused.Len() != len(docs) {
					t.Fatalf("round %d: Len = %d, want %d", round, reused.Len(), len(docs))
				}
				gotStats, wantStats := reused.Stats(), fresh.Stats()
				if gotStats.Docs != wantStats.Docs {
					t.Fatalf("round %d: Docs = %d, want %d", round, gotStats.Docs, wantStats.Docs)
				}
				// The retained shape cache legitimately makes the refilled
				// segment store at least as many documents in dedup form as a
				// fresh one: a layout it compiled in an earlier round resolves on
				// its first sighting in this one. Storing fewer would mean the
				// cache lost records it should have kept.
				if gotStats.ShapeTaped < wantStats.ShapeTaped {
					t.Fatalf("round %d: ShapeTaped = %d, want at least %d",
						round, gotStats.ShapeTaped, wantStats.ShapeTaped)
				}
				// The dictionary is dropped by Reset, so a refilled segment must
				// intern exactly what a fresh one does — a retained arena would
				// show up here as extra distinct values carried over.
				if gotStats.DictValues != wantStats.DictValues ||
					gotStats.DictBytes != wantStats.DictBytes {
					t.Fatalf("round %d: dictionary = %d values / %d B, want %d / %d",
						round, gotStats.DictValues, gotStats.DictBytes,
						wantStats.DictValues, wantStats.DictBytes)
				}
				// Non-vacuity: each mode must actually have exercised the state
				// Reset is responsible for clearing. A corpus that never
				// shape-tapes or never interns a value would make the comparisons
				// above pass without touching the tables in question.
				switch mode.name {
				case "shape", "shape+postings", "valuedict":
					if gotStats.ShapeTaped == 0 {
						t.Fatalf("round %d: no document was shape-taped, so the "+
							"widened-tape cache and header table are untested", round)
					}
				}
				if mode.name == "valuedict" && gotStats.DictValues == 0 {
					t.Fatalf("round %d: the value dictionary interned nothing, so its "+
						"reset is untested", round)
				}
				if mode.name == "shape+postings" {
					gotRows := reused.WhereExists("nested")
					wantRows := fresh.WhereExists("nested")
					if fmt.Sprint(gotRows) != fmt.Sprint(wantRows) {
						t.Fatalf("round %d: WhereExists = %v, want %v", round, gotRows, wantRows)
					}
					if len(gotRows) == 0 {
						t.Fatalf("round %d: WhereExists matched nothing; the postings "+
							"this check means to observe were never built", round)
					}
				}
			}
		})
	}
}

// Given a view obtained from a segment, when the segment is Reset and refilled,
// then the view must still address the segment's own arena and must show the
// new contents there.
//
// This pins the exact ownership contract Reset documents, in both directions.
// The retained slice must remain readable — the arena is retained, not freed, so
// a stale view can never fault or address memory the segment gave back — and it
// must report the refilled document's bytes, which is precisely why holding one
// across a Reset is a silent wrong-answer bug rather than a crash. Pinning the
// aliasing exactly is what makes the contract testable: a Reset that quietly
// replaced an arena instead of rewinding it would leave the stale view showing
// the old bytes and would pass any test that only asked "does it still read".
//
// The corpus is sized to fit the minimum source chunk, so the refill provably
// reuses the same arena generation rather than allocating a fresh one, and the
// forced GC between the two fills gives the collector every opportunity to
// invalidate a view the segment no longer references.
func TestSegmentResetInvalidatesRetainedViews(t *testing.T) {
	const count = 20
	first := resetCorpus("alpha", count, 2)
	second := resetCorpus("bravo", count, 2)
	if len(first[0]) != len(second[0]) {
		t.Fatalf("corpora must be span-identical for the aliasing check: %d vs %d",
			len(first[0]), len(second[0]))
	}
	total := 0
	for _, doc := range first {
		total += len(doc)
	}
	if total > segmentMinSrcChunk {
		t.Fatalf("corpus of %d bytes exceeds the %d-byte minimum source chunk, so the "+
			"refill would not provably reuse the same arena", total, segmentMinSrcChunk)
	}

	s := &Segment{}
	fillSegment(t, s, first)
	retainedSrc := s.Doc(0).Src
	retainedEntries := s.Doc(0).Entries
	if !bytes.Equal(retainedSrc, first[0]) {
		t.Fatalf("retained source = %s, want %s", retainedSrc, first[0])
	}

	s.Reset()
	// A forced collection with the retained views alive: nothing the segment
	// still owns may be reclaimed under them.
	runtime.GC()
	runtime.GC()
	fillSegment(t, s, second)
	runtime.GC()

	if got := s.Doc(0).Src; !bytes.Equal(got, second[0]) {
		t.Fatalf("refilled document 0 = %s, want %s", got, second[0])
	}
	// The contract, stated as an assertion: the stale view is readable and now
	// shows the refilled corpus, not the one it was taken from.
	if bytes.Equal(retainedSrc, first[0]) {
		t.Fatalf("view retained across Reset still shows the old document: the arena " +
			"was replaced rather than rewound, and the documented invalidation " +
			"contract no longer describes what Reset does")
	}
	if !bytes.Equal(retainedSrc, second[0]) {
		t.Fatalf("view retained across Reset shows %s, want the refilled document %s: "+
			"the invalidation is wider than the documented arena reuse",
			retainedSrc, second[0])
	}
	if len(retainedEntries) == 0 {
		t.Fatal("retained tape was empty; the entry-arena half of the contract is untested")
	}
	// Reading the stale tape must stay in-bounds of the refilled source. This is
	// the property that makes the hazard a wrong answer rather than a panic.
	for _, e := range retainedEntries {
		if int(e.End) > len(s.Doc(0).Src) {
			t.Fatalf("stale entry %d:%d escapes the refilled document of %d bytes",
				e.Start, e.End, len(s.Doc(0).Src))
		}
	}
	runtime.KeepAlive(retainedSrc)
	runtime.KeepAlive(retainedEntries)
}

// Given a segment whose arenas have reached a batch's high-water mark, when it
// is Reset and refilled with an equivalent batch, then the refill must allocate
// nothing.
//
// This is the property the method exists for, asserted directly rather than
// inferred from a benchmark: a consumer that indexes a bounded batch, reads it,
// and discards it converges on arenas that hold a whole batch, after which the
// steady-state cost of a batch is zero allocations. Without Reset the same loop
// pays full geometric growth of both arenas per batch.
func TestSegmentResetRefillIsAllocationFree(t *testing.T) {
	docs := resetCorpus("steady", 200, 5)
	s := &Segment{}
	// Warm-up: enough fills for both arenas to reach a chunk that holds the
	// whole batch. Growth is geometric, so this converges in a handful.
	for range 8 {
		s.Reset()
		fillSegment(t, s, docs)
	}
	allocs := testing.AllocsPerRun(20, func() {
		s.Reset()
		for _, doc := range docs {
			if _, err := s.Append(doc); err != nil {
				t.Fatal(err)
			}
		}
	})
	if allocs != 0 {
		t.Fatalf("a warm Reset and refill of %d documents allocated %.1f times, want 0",
			len(docs), allocs)
	}
	if s.Len() != len(docs) {
		t.Fatalf("Len = %d, want %d", s.Len(), len(docs))
	}
}

// Given a segment whose contents are published or whose arenas are borrowed,
// when Reset is called, then it must panic rather than rewind storage it does
// not own.
//
// Reset is for a scratch segment being refilled, and the distinction is enforced
// in the type rather than left to the caller because both failure modes are
// silent. Rewinding a collection chunk would rewrite documents that live
// snapshots are still reading; rewinding a segment opened from an image would
// claim length over bytes belonging to a mapping the segment does not own. The
// sealed-chunk invariant TestStoreChunkRetainsNoIngestScratch pins is safe from
// this method for exactly this reason: a sealed chunk cannot reach it.
func TestSegmentResetRefusesPublishedSegments(t *testing.T) {
	mustPanic := func(t *testing.T, what string, f func()) {
		t.Helper()
		defer func() {
			if recover() == nil {
				t.Fatalf("Reset on %s did not panic", what)
			}
		}()
		f()
	}

	t.Run("collection chunk", func(t *testing.T) {
		collection := &Collection{Options: Options{ChunkDocuments: 4}}
		for i := range 6 {
			if _, err := collection.Put(fmt.Sprintf("k%d", i),
				fmt.Appendf(nil, `{"id":%d,"v":"x"}`, i)); err != nil {
				t.Fatal(err)
			}
		}
		state := collection.state.Load()
		checked := 0
		for id := uint32(0); id < state.Chunks.Count; id++ {
			chunk := state.Chunks.Get(id)
			if chunk == nil {
				continue
			}
			mustPanic(t, "a published collection chunk", func() { chunk.Docs.Reset() })
			checked++
		}
		if checked == 0 {
			t.Fatal("no published chunk was examined")
		}
	})

}
