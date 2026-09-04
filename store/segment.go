package store

import (
	"errors"
	"sync"
	"unsafe"

	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/document"
)

// A Segment indexes a batch of JSON documents into shared storage. Append
// copies each document into a chunked source arena and builds its index in a
// chunked entry arena, so a batch of N documents costs a handful of arena
// allocations instead of two per document, and consecutive documents' bytes
// and entries stay adjacent for batch scans. ReadFrom ingests an entire
// stream of documents in bulk, reading straight into the arena.
//
// The two arenas follow the interner's never-moving discipline: a chunk is
// appended to only within its capacity and retired in place when full, so
// nothing handed out is ever invalidated.
//
//	source arena   chunk: [ doc0 | doc1 | doc2 |  spare )
//	entry arena    chunk: [ e(0) | e(1) | e(2) |  spare )
//	docs[i] = Index{src, entries}: subslices of the two arenas
//
// Under the opt-in ShapeTapes mode a conforming document's e(i) holds only
// its value entries — the classic tape's keys live once in a compiled shape
// — with a per-document header alongside; segment_shape.go owns that
// representation and its contracts.
//
// Growth within an Append is transactional: the document's bytes land in the
// source chunk's uncommitted tail first, the index builds into the entry
// chunk's tail, and only success extends the committed lengths over both.
// On failure the tails are truncated, which is a length rollback with no
// copying — the uncommitted bytes were never visible.
//
// Doc returns the ordinary Index over a stored document, with the full
// per-document API. Arena chunks are append-only and never moved, so every
// Index, Node, and RawValue obtained from the segment remains valid across later
// Appends. A failed Append leaves the segment unchanged. The zero Segment is empty
// and ready to use. A Segment is not safe for concurrent use; concurrent reads
// are safe once appending stops.
type Segment struct {
	// Options configures indexing, read at each Append. Set HashKeys before
	// the first Append for lookup-heavy engines: enrichment costs one linear
	// pass over the entries at build time and accelerates every Get after it.
	Options document.IndexOptions

	// ShapeTapes opts the segment into shape-deduplicated tapes, read at each
	// Append like Options: a conforming document stores one entry per member
	// value instead of the classic tape, roughly halving tape storage on
	// shape-clustered corpora and letting the batch extractors index value
	// arrays directly. Value entries are themselves dual-width: a document
	// whose root span fits 16-bit offsets (under 64 KiB) stores 8-byte
	// entries, halving the value array again; wider documents keep 16-byte
	// entries. Semantics never change — every lookup, extractor, and Doc
	// result is identical to classic storage — but Doc's cost does: its first
	// call on a shape-taped document materializes and permanently caches the
	// classic tape (see Doc). Set it before the first Append.
	//
	// Conformance decides whether the mode helps, and it is strict, so read it
	// before enabling: a document conforms only if its root is a non-empty
	// object whose every member value is a single tape entry — that is, a
	// scalar or an empty container. One nested object or array anywhere at the
	// root disqualifies the whole document, and its members are not considered
	// separately. The root's key sequence must also byte-match a shape the
	// segment's cache has already compiled, which takes a second sighting of that
	// layout, and the layout must not repeat a decoded key name. Whatever
	// fails is stored classic, unchanged and correct.
	//
	// The consequence is that the option is a lever for flat corpora, not a
	// general one. On a flat corpus it is a large win; on a corpus whose
	// documents all carry a nested value it can store nothing at all, and
	// enabling it there buys a per-document conformance check that always
	// fails. Measure rather than assume: Segment.Stats reports ShapeTaped, so a
	// short ingest tells you which corpus you have. See segment_shape.go for
	// the representation and its proof obligations.
	ShapeTapes bool

	// Postings opts the segment into the inverted existence and containment layer,
	// read at each Append like Options: a document's top-level keys and scalar
	// values are folded into Segment-owned postings so WhereExists and
	// WhereContains answer selective predicates by probing a candidate set
	// rather than scanning every document. Key existence resolves through the
	// shape index, so it is most effective paired with ShapeTapes — with shapes
	// off every document is the non-conforming remainder and existence stays
	// correct but scans; value containment prunes regardless. Set it before the
	// first Append: enabling it later leaves earlier documents unindexed, and
	// the query paths detect the gap and fall back to a full scan. Ingest pays
	// one pass over each document's top-level members. See segment_postings.go
	// for the representation and its proof obligations.
	Postings bool

	docs []vibejson.Index
	// mappedDocs is collection-only compact metadata for a Segment page reopened
	// from a validated image. It replaces per-document slice and shape pointers
	// with pointer-free external descriptors; mappedShapes scales with distinct
	// layouts, not documents. Public Open keeps the ordinary representation.
	mappedDocs   *storeMappedDocs
	mappedShapes []*ShapeRecord
	mappedBase   uint64
	mappedCount  int
	mappedNarrow int
	srcChunk     []byte                // current source arena chunk
	entryChunk   []vibejson.IndexEntry // current entry arena chunk
	scratch      []vibejson.IndexEntry // spill tape for documents the entry chunk cannot hold

	// The two arena recyclers. Chunks grow geometrically only up to a fixed
	// maximum (segmentMaxSrcChunk, segmentMaxEntryChunk), so a fill whose
	// documents need more than one chunk's worth necessarily supersedes the
	// installed chunk and installs another. Retaining every chunk needed at the
	// fill high-water mark prevents repeated arena growth for batches larger than
	// one chunk. Reset's ownership contract makes recycling sound: every Index
	// the segment handed out is invalid afterwards, so no superseded generation
	// remains observable.
	srcPool   arenaPool[byte]
	entryPool arenaPool[vibejson.IndexEntry]

	// Shape-tape state (segment_shape.go): tapeRefs is empty or docs-aligned
	// and holds each document's dedup header; narrow is the slab of 8-byte
	// value entries for narrow-width documents, addressed by each ref's
	// offset (it relocates freely as it grows: no pointer into it ever
	// leaves a call); shapes is the ingest conformance cache, allocated on
	// first use and released by sealIngest — reads resolve through each
	// ref's Rec pointer and mappedShapes, never through this cache, so a
	// published chunk carries no ingest tables; widened caches the classic
	// tapes Doc has re-materialized, under widenMu so concurrent reads stay
	// safe once appending stops. wideValueTapes is the width test seam: it
	// forces 16-byte value entries for narrow-eligible documents so the
	// differential tests can hold the two widths against each other on
	// identical documents; nothing outside tests sets it.
	tapeRefs       []ShapeTapeRef
	narrow         []ShapeNarrowValue
	shapes         *ShapeCache
	widened        map[int][]vibejson.IndexEntry
	widenMu        sync.Mutex
	wideValueTapes bool

	// postings is the inverted existence/containment layer (segment_postings.go),
	// built at commit under the Postings opt-in and nil until the first indexed
	// document. It owns its structures and is read by WhereExists and
	// WhereContains; a partial index (Postings enabled late) is detected and
	// bypassed, so the pointer being non-nil never forces a stale answer.
	postings *docPostings

	// Arena minima are internal construction hints. Zero preserves the bulk
	// Segment policy below; bounded immutable collection chunks select smaller first
	// allocations so a one-document rewrite does not buy stream-sized arenas.
	// The branch is paid only when a new arena chunk is allocated, never on a
	// read or on an Append that fits the current chunk.
	arenaMinSrc     int
	arenaMinEntries int
	// dropEmptySpill is the bounded-collection policy: if shape compaction removes
	// every entry from a spill-built document, retain no empty entry arena.
	// A bulk Segment keeps that arena for its next Append; one collection rebuild has
	// exactly one replacement Append, so the capacity has no future consumer.
	dropEmptySpill bool
	// singleAppend states that at most one document is ever indexed into this
	// segment through buildDoc, so the entry arena's growth policy has nothing left
	// to amortize. A bulk Segment sizes a spilled document's replacement chunk
	// for the appends that follow it; a chunk rebuilt by buildStoreChunk takes
	// its other documents by reference through appendStoreDoc and parses only
	// the one replacement, so spare entries bought here would be retained
	// unwritten for the published chunk's whole live tenure. Only
	// prepareStoreSegment sets it: a Builder page shares initChunkSegment
	// but fills up to ChunkDocuments documents out of one entry arena, where
	// the geometric policy still pays for itself.
	singleAppend bool

	// ValueDict opts the segment into the corpus-wide value dictionary, read at
	// each Append like ShapeTapes: a value span that recurs across the segment —
	// an enum string, a label, a repeated sub-object — is interned once into
	// the shared arena, and each later occurrence records a compact reference
	// in place of the bytes, from which a compacting store drops the repeated
	// source while every value still resolves to a stable arena view in O(1)
	// (no decompression). Shape tapes remove key redundancy; the dictionary
	// removes value redundancy, and the two compose. Semantics never change:
	// every read is byte-identical to classic storage, the arena holding bytes
	// identical to the source they stand in for. Set it before the first
	// Append.
	//
	// It costs memory rather than saving it, and this is structural, not a
	// tuning failure. A live segment retains every document's verbatim source so
	// that reads stay zero-copy, so enabling the dictionary removes nothing
	// from the source: the arena, the splice records, and the sighting set are
	// all additions on top of bytes that stay resident. Its payoff is entirely
	// at rest: the repeated source it lets a compacting or persisting writer
	// drop, which SegmentStats.DictSavedBytes models. Enable it when you are
	// writing the segment out or compacting it, not to shrink a live segment.
	// Measured tradeoffs live in docs/performance.md; see segment_valuedict.go
	// for the representation, its read contract, and the read==source invariant.
	ValueDict bool

	// Value-dictionary state (segment_valuedict.go), populated only under
	// ValueDict. values is the corpus-wide interner arena, never-moving like
	// the ShapeCache; valueSplices is the segment-wide slab holding one record per
	// dictionary-backed occurrence, in per-document source order; valueRefs is
	// empty or docs-aligned and windows each document's records within the
	// slab; valueSeen gates interning on a value's second sighting, so a
	// singleton never costs an entry it cannot amortize. valueFloor is the
	// interning length floor (zero selects valueDictMinSpan) — a test seam the
	// exhaustive suite lowers to one so every repeated span is dictionary-
	// backed and the arena read path is exercised on every value shape; nothing
	// outside tests sets it.
	values       ValueInterner
	valueRefs    []valueDictRef
	valueSplices []valueSplice
	valueSeen    map[uint64]struct{}
	valueFloor   uint32

	// source is the serialized image an Open'd segment borrows its arenas from
	// (segment_persist.go): a segment reconstructed by Open holds the bytes here so
	// the zero-copy document sources and entry tapes that view into them stay
	// alive for the segment's lifetime, and it is nil for a segment built by Append.
	// The field pins the mapping; the caller owns keeping an underlying mmap
	// mapped, and every borrowed view is invalid once it is unmapped.
	source []byte
}

// Arena chunks grow geometrically between fixed bounds, like the interner's:
// small sets stay small, large ones amortize allocation, and a document larger
// than the maximum still gets a chunk of its own.
const (
	segmentMinSrcChunk   = 8 << 10
	segmentMaxSrcChunk   = 1 << 20
	segmentMinEntryChunk = 512
	segmentMaxEntryChunk = 64 << 10
)

func (s *Segment) sourceChunkMinimum() int {
	if s.arenaMinSrc > 0 {
		return s.arenaMinSrc
	}
	return segmentMinSrcChunk
}

func (s *Segment) entryChunkMinimum() int {
	if s.arenaMinEntries > 0 {
		return s.arenaMinEntries
	}
	return segmentMinEntryChunk
}

// Len returns the number of stored documents.
func (s *Segment) Len() int {
	if s.mappedDocs != nil {
		return s.mappedCount
	}
	return len(s.docs)
}

// Doc returns the Index over the ith document. The Index borrows the segment's
// arenas and remains valid across later Appends. An out-of-range ordinal
// panics like an out-of-range slice index.
//
// Under ShapeTapes, a shape-taped document's classic tape no longer exists,
// so Doc's first call on it synthesizes one — an allocation and one pass
// over the members — and caches it for the segment's lifetime: later calls
// return the same storage, handles stay stable, and concurrent Doc calls
// remain safe once appending stops. The result is identical to the Index
// classic storage would have returned. The space cost is the honest flip
// side: widening a document re-buys the classic tape the mode dropped, so
// engines wanting the space win extract through the batch primitives, which
// read the deduplicated form directly.
func (s *Segment) Doc(i int) vibejson.Index {
	if template, ok := s.TemplateAt(i); ok {
		return s.widenStoreTemplate(i, template)
	}
	if r := s.ShapeTapeRefAt(i); r.Rec != nil {
		return s.widenShapeTape(i, r)
	}
	return s.DocAt(i)
}

// Append copies src into the segment, validates and indexes the copy, and returns
// the new document's ordinal. src may be reused or discarded after the call.
// Invalid input returns the same error BuildIndexOptions reports and leaves
// the segment unchanged: no partial document is ever visible.
func (s *Segment) Append(src []byte) (int, error) {
	// The copy lands first: the index must alias arena bytes, not the
	// caller's buffer. Appending within capacity never moves a chunk, so
	// previously returned views survive; on failure the copied bytes are
	// still uncommitted arena tail and restoring the length removes them.
	if len(s.srcChunk)+len(src) > cap(s.srcChunk) {
		s.srcPool.retire(s.srcChunk)
		s.srcChunk = s.srcPool.take(
			len(src), cap(s.srcChunk),
			s.sourceChunkMinimum(), segmentMaxSrcChunk,
		)
	}
	mark := len(s.srcChunk)
	s.srcChunk = append(s.srcChunk, src...)
	index, ref, err := s.buildDoc(s.srcChunk[mark:len(s.srcChunk):len(s.srcChunk)])
	if err != nil {
		s.srcChunk = s.srcChunk[:mark]
		return 0, err
	}
	return s.commitDoc(index, ref), nil
}

// appendStoreSchema is collection's fused parse-and-schema path. The schema sees
// the complete structural index before optional shape compaction, so a valid
// write is parsed once and a rejected write commits neither source nor tape.
func (s *Segment) appendStoreSchema(
	src []byte,
	schema *Schema,
) (int, error) {
	if len(s.srcChunk)+len(src) > cap(s.srcChunk) {
		s.srcPool.retire(s.srcChunk)
		s.srcChunk = s.srcPool.take(
			len(src), cap(s.srcChunk),
			s.sourceChunkMinimum(), segmentMaxSrcChunk,
		)
	}
	mark := len(s.srcChunk)
	s.srcChunk = append(s.srcChunk, src...)
	index, ref, err := s.buildDocSchema(
		s.srcChunk[mark:len(s.srcChunk):len(s.srcChunk)],
		schema,
	)
	if err != nil {
		s.srcChunk = s.srcChunk[:mark]
		return 0, err
	}
	return s.commitDoc(index, ref), nil
}

// buildDoc indexes one arena-resident document into the entry arena. It first
// builds directly into the current chunk's free tail — the common case, one
// validation pass and no copy. A document that outgrows the tail builds once
// into the spill tape and moves its exact entry count into a fresh chunk while
// the entries are cache-hot; a precount pass (`RequiredIndexEntries`) would
// instead rescan every document's source on the common path. Under ShapeTapes
// a conforming document is compacted
// to its value entries between build and commit (shapeTapeCompact), so key
// entries never reach committed storage; the returned ref is its dedup header,
// zero for classic documents. The spill path avoids a mandatory source
// precount and therefore preserves the one-pass common case.
func (s *Segment) buildDoc(src []byte) (vibejson.Index, ShapeTapeRef, error) {
	if cap(s.entryChunk) == 0 {
		s.entryChunk = make([]vibejson.IndexEntry, 0, s.entryChunkMinimum())
	}
	used := len(s.entryChunk)
	free := s.entryChunk[used:]
	index, err := vibejson.BuildIndexOptions(src, free, s.Options)
	if err == nil {
		n := len(index.Entries)
		if n > 0 &&
			unsafe.SliceData(index.Entries) != unsafe.SliceData(free) {
			// Both builders write into the storage they are handed; the
			// commit below extends the chunk over exactly those entries. If
			// the invariant ever broke, extending would expose garbage, so
			// fail closed: the document keeps the storage it was built in and
			// the chunk stays unchanged.
			return index, ShapeTapeRef{}, nil
		}
		index.Entries = index.Entries[:n:n]
		index, ref := s.shapeTapeCompact(index)
		if out, ok := s.exactSingleTape(src, index, ref); ok {
			return out, ref, nil
		}
		s.entryChunk = s.entryChunk[:used+len(index.Entries)]
		return index, ref, nil
	}
	if !errors.Is(err, document.ErrIndexFull) {
		return vibejson.Index{}, ShapeTapeRef{}, err
	}
	// One entry is recorded per value or key, and each spans at least one
	// distinct source byte, so len(src)+2 entries always suffice.
	if cap(s.scratch) < len(src)+2 {
		s.scratch = make([]vibejson.IndexEntry, 0, len(src)+2)
	}
	index, err = vibejson.BuildIndexOptions(src, s.scratch[:0], s.Options)
	if err != nil {
		return vibejson.Index{}, ShapeTapeRef{}, err
	}
	index, ref := s.shapeTapeCompact(index)
	n := len(index.Entries)
	if n == 0 && s.dropEmptySpill {
		return vibejson.Index{Src: src}, ref, nil
	}
	if out, ok := s.exactSingleTape(src, index, ref); ok {
		return out, ref, nil
	}
	s.entryPool.retire(s.entryChunk)
	chunk := s.entryPool.take(
		n, cap(s.entryChunk),
		s.entryChunkMinimum(), segmentMaxEntryChunk,
	)[:n]
	copy(chunk, index.Entries)
	s.entryChunk = chunk
	return vibejson.Index{Src: src, Entries: chunk[:n:n]}, ref, nil
}

// exactSingleTape gives the one document a singleAppend segment parses storage
// sized to that document, and reports whether it applied. It is the shared tail
// of both build routes, and the reason a rebuilt collection chunk retains no
// entry arena at all.
//
// A bulk segment leaves a document's tape in the arena it was built in, because
// the arena's remaining capacity has later appends to amortize it against.
// Under singleAppend those appends do not exist: the tape pins whatever array
// it lands in for the published chunk's whole live tenure, so an arena's
// surplus — max(2*prev, min, n) entries on the growth route, or the
// deliberately over-reserved estimate prepareStoreSegment installs so the build
// need not spill at all — would be retained forever and never written. Copying
// the exact entry count out while they are still cache-hot converts that
// surplus into one bounded transient allocation.
//
// The arena is deliberately left unadvanced rather than replaced or shrunk: its
// free tail is the capacity prepareStoreSegment reserved for the carried-over
// template tapes appendStoreDoc has still to synthesize, and installing an
// exact-fit chunk over it would force those appends to grow — copying this
// document's entries into the replacement and re-buying every byte just
// released. sealIngest drops it at publication if they never came.
func (s *Segment) exactSingleTape(
	src []byte, index vibejson.Index, ref ShapeTapeRef,
) (vibejson.Index, bool) {
	if !s.singleAppend {
		return vibejson.Index{}, false
	}
	if len(index.Entries) == 0 && s.dropEmptySpill {
		return vibejson.Index{Src: src}, true
	}
	tape := make([]vibejson.IndexEntry, len(index.Entries))
	copy(tape, index.Entries)
	return vibejson.Index{Src: src, Entries: tape}, true
}

// committedEntries totals the structural entries the segment's documents own.
// It is not len(entryChunk): that is one arena generation, and after any growth
// the earlier generations still back the tapes of the documents built into
// them. Summing the tapes is the only count that survives growth, and it is
// bounded by the segment's document count rather than by its bytes.
func (s *Segment) committedEntries() int {
	total := 0
	for i := range s.docs {
		total += len(s.docs[i].Entries)
	}
	return total
}

// sealIngest releases the ingest-only working state of a completed immutable
// chunk. A collection chunk is published once and every later edit rebuilds it by
// copy into a new Segment, so after its final commitDoc no document can be
// indexed into this one again and two fields become pure debt: scratch, the
// spill build buffer, which is one entry per source byte of the widest
// document that overflowed the entry arena and whose contents are always
// copied into the document's own tape rather than handed out; and valueSeen,
// the value dictionary's first-sighting gate, which is one entry per distinct
// value hash and is read only by valueDictScan while ingesting. Neither is
// reachable from a read: the documents, tapes, narrow slab, splice slab, and
// interner arena are untouched, so a sealed chunk answers byte-identically.
//
// The entry arena goes only when it holds no committed entries, which is
// exactly the condition under which no document's tape aliases it: it is then
// a build target — for a spilled document that outgrew it, for one whose tape
// shape compaction removed entirely, or for template capacity a rebuild
// reserved and did not spend — and this set indexes nothing further. An arena
// with committed entries backs live tapes and must outlive the seal.
// ensureShapeCache returns the ingest conformance cache, allocating a
// bulk-policy cache on first use. Collection chunks arrive with an
// exact-minima cache from initChunkSegment when ShapeTapes is enabled;
// raw scratch Segments grow one here on their first conforming document.
// Reads never touch the cache — Doc widens through each ref's Rec pointer —
// so sealIngest releases it and no read path pays this branch.
func (s *Segment) ensureShapeCache() *ShapeCache {
	if s.shapes == nil {
		s.shapes = &ShapeCache{}
	}
	return s.shapes
}

func (s *Segment) sealIngest() {
	s.scratch = nil
	s.valueSeen = nil
	// The conformance cache compiled shapes for the ingest this seal closes.
	// Rebuilds re-seed from the surviving rows' Rec pointers and reads never
	// consult the cache, so retaining its table and arenas would pin
	// page-local compilation memory for the chunk's whole published lifetime.
	s.shapes = nil
	// The arena free lists go unconditionally. They exist only to serve a later
	// fill, and a sealed chunk takes no further document; the retired list in
	// particular would otherwise keep a slice header per superseded generation
	// alive for the chunk's whole published lifetime, for storage nothing will
	// ever build into again.
	s.srcPool.drop()
	s.entryPool.drop()
	if len(s.entryChunk) == 0 {
		s.entryChunk = nil
	}
}

// Reset empties a scratch segment for refilling, retaining the source and entry
// arenas, the document and shape-header tables, the spill build buffer, and the
// compiled shape cache so the next fill reuses that storage instead of growing
// it again from nothing. It is the counterpart of sealIngest: sealIngest
// releases ingest state a published chunk will never use again, while Reset
// keeps exactly that state and drops the documents.
//
// It supports allocation-stable consumers that repeatedly index, read, and
// discard bounded batches, such as batched query executors, streaming reducers,
// and fixed-window aggregators. The arenas reach the batch high-water mark once
// and stay there.
//
// # Ownership contract
//
// Reset invalidates every view the segment has handed out. An Index from Doc or
// DocAt, a Node or RawValue derived from one, a tape from any batch primitive, a
// key or value slice, a ShapeTapeRef, and anything reachable from them all point
// into arenas Reset keeps and the next Append overwrites in place. Reading any
// of them after a Reset is a use-after-free that Go's memory safety cannot
// catch: the memory is still mapped and still owned by this segment, so a stale
// Index does not fault — it silently reports a later document's bytes as an
// earlier document's value. This is the same destination-reuse boundary as
// KeyInterner.Reset and the append-style APIs, and it is the whole reason the
// method is opt-in rather than automatic: the ordinary Segment contract promises
// that an Index outlives later Appends, and a caller who has published views to
// anyone else must not call Reset. A caller who detaches what it keeps — copying
// the bytes out before the next batch, as the query executor's ownScalars does —
// is safe, and TestSegmentResetInvalidatesRetainedViews pins that the
// invalidation is exactly the arena reuse and nothing wider.
//
// # Sealed segments are refused
//
// Reset is for a scratch segment being refilled and never for one whose contents
// have been published, so it panics rather than trusting the caller when the
// segment is not scratch. Two families are refused. A segment reopened from a
// serialized image (Open) or reconstructed as a collection page borrows its
// arenas from storage it does not own, so truncating them would claim length
// over bytes belonging to a mapping. A collection chunk — every segment built
// through initChunkSegment, identified here by the arena-minimum and
// single-append construction hints only that path sets — is published once and
// then read by snapshots this segment cannot see; refilling it in place would
// rewrite live readers' documents under them. That is also what keeps
// TestStoreChunkRetainsNoIngestScratch's sealed-chunk invariant safe from this
// method: a sealed chunk can never reach it.
func (s *Segment) Reset() {
	if s.source != nil || s.mappedDocs != nil {
		panic("vibedb: Segment.Reset on a segment borrowing storage it does not own")
	}
	if s.arenaMinSrc != 0 || s.arenaMinEntries != 0 || s.dropEmptySpill || s.singleAppend {
		panic("vibedb: Segment.Reset on a published collection chunk")
	}
	// The document and header tables are cleared before truncation, not merely
	// truncated. Each Index holds slice headers into the arena generation it was
	// built in, and an arena generation is dropped from the segment the moment a
	// document outgrows it, so a stale header past the new length would pin every
	// superseded generation for as long as the segment lives — turning the
	// reuse this method exists for into an unbounded leak. Clearing at length
	// each time keeps the whole retained capacity clean inductively: everything
	// past the current length was already cleared by the previous Reset.
	clear(s.docs)
	s.docs = s.docs[:0]
	clear(s.tapeRefs)
	s.tapeRefs = s.tapeRefs[:0]
	// The arenas keep their capacity: this is the allocation the method exists
	// to remove. The newest generation of each stays installed, and every
	// generation this fill superseded joins the free list the next fill draws
	// from — which is what makes the removal hold for a batch too large for one
	// chunk. Chunks stop doubling at segmentMaxSrcChunk and segmentMaxEntryChunk,
	// so such a batch supersedes its chunk at the same point every time; keeping
	// only the newest generation meant re-buying the replacement on every single
	// fill. See srcFree/entryFree for the measurement.
	s.srcPool.recycle(cap(s.srcChunk))
	s.entryPool.recycle(cap(s.entryChunk))
	s.srcChunk = s.srcChunk[:0]
	s.entryChunk = s.entryChunk[:0]
	// The narrow value slab holds no pointers, so truncation alone releases
	// everything; ShapeTapeRef.off readdresses it from zero on the next fill.
	s.narrow = s.narrow[:0]
	// The widened tape cache is keyed by document ordinal, and ordinals restart
	// at zero. Left in place it would answer Doc(0) on the next batch with the
	// previous batch's document — a wrong-answer bug, not a space one. Clearing
	// keeps the map's buckets.
	clear(s.widened)
	// The postings index is dropped rather than emptied. Its shape-to-id map is
	// keyed by ShapeRecord pointers and its key interner assigns dense ids in
	// sighting order, so reusing it across fills would need every one of those
	// tables reset in lockstep with the documents they describe; a nil pointer is
	// the only state that cannot be subtly stale. Segment.Postings is documented
	// as an ingest-time cost that only selective repeated probes amortize, and a
	// batch indexed once and dropped is exactly the case it does not pay for, so
	// nothing here is on a path worth complicating.
	s.postings = nil
	// The value dictionary is dropped for space, not correctness: its arena
	// interns value bytes copied out of documents this Reset discards, so
	// retaining it would accumulate the bytes of every batch ever indexed while
	// the splice records that reference them are truncated away. The
	// first-sighting gate is cleared rather than dropped because its buckets cost
	// nothing to keep and it must not outlive the arena it gates.
	s.values = ValueInterner{}
	clear(s.valueRefs)
	s.valueRefs = s.valueRefs[:0]
	s.valueSplices = s.valueSplices[:0]
	clear(s.valueSeen)
	// scratch and shapes deliberately survive. scratch is the spill build buffer,
	// whose contents are always copied into a document's own tape and never
	// handed out, so it is pure reusable capacity. The ShapeCache owns its own
	// arena — key spellings are interned into it, never viewed out of document
	// source — so its compiled records stay valid across a Reset, and keeping
	// them means a corpus whose layouts repeat across batches compiles each
	// exactly once instead of once per batch. The one visible consequence is a
	// storage choice, not a semantic one: a layout the cache compiled before the
	// Reset is resolved on its first sighting after it, so a refilled segment can
	// store in dedup form a document a freshly built segment would still have
	// stored classic. Every read is identical either way — the ingest-time key
	// proof runs per document regardless — so only Stats can tell.
}

// buildDocSchema intentionally specializes buildDoc rather than adding a
// validator callback or branch to Segment's public hot path. The duplicated
// arena choreography is small and mechanically parallel; keeping it here
// preserves identical schemaless code generation while placing validation
// between the one structural parse and shape-tape compaction.
func (s *Segment) buildDocSchema(
	src []byte,
	schema *Schema,
) (vibejson.Index, ShapeTapeRef, error) {
	if cap(s.entryChunk) == 0 {
		s.entryChunk = make([]vibejson.IndexEntry, 0, s.entryChunkMinimum())
	}
	used := len(s.entryChunk)
	free := s.entryChunk[used:]
	index, err := vibejson.BuildIndexOptions(src, free, s.Options)
	if err == nil {
		n := len(index.Entries)
		if n > 0 && unsafe.SliceData(index.Entries) != unsafe.SliceData(free) {
			// Both builders write into the storage they are handed; the
			// commit below extends the chunk over exactly those entries. If
			// the invariant ever broke, extending would expose garbage, so
			// fail closed: the document keeps the storage it was built in and
			// the chunk stays unchanged.
			if err := schema.ValidateIndex(index); err != nil {
				return vibejson.Index{}, ShapeTapeRef{}, err
			}
			return index, ShapeTapeRef{}, nil
		}
		index.Entries = index.Entries[:n:n]
		if err := schema.ValidateIndex(index); err != nil {
			return vibejson.Index{}, ShapeTapeRef{}, err
		}
		index, ref := s.shapeTapeCompact(index)
		if out, ok := s.exactSingleTape(src, index, ref); ok {
			return out, ref, nil
		}
		s.entryChunk = s.entryChunk[:used+len(index.Entries)]
		return index, ref, nil
	}
	if !errors.Is(err, document.ErrIndexFull) {
		return vibejson.Index{}, ShapeTapeRef{}, err
	}
	// One entry is recorded per value or key, and each spans at least one
	// distinct source byte, so len(src)+2 entries always suffice.
	if cap(s.scratch) < len(src)+2 {
		s.scratch = make([]vibejson.IndexEntry, 0, len(src)+2)
	}
	index, err = vibejson.BuildIndexOptions(src, s.scratch[:0], s.Options)
	if err != nil {
		return vibejson.Index{}, ShapeTapeRef{}, err
	}
	if err := schema.ValidateIndex(index); err != nil {
		return vibejson.Index{}, ShapeTapeRef{}, err
	}
	index, ref := s.shapeTapeCompact(index)
	n := len(index.Entries)
	if n == 0 && s.dropEmptySpill {
		return vibejson.Index{Src: src}, ref, nil
	}
	if out, ok := s.exactSingleTape(src, index, ref); ok {
		return out, ref, nil
	}
	s.entryPool.retire(s.entryChunk)
	chunk := s.entryPool.take(
		n, cap(s.entryChunk),
		s.entryChunkMinimum(), segmentMaxEntryChunk,
	)[:n]
	copy(chunk, index.Entries)
	s.entryChunk = chunk
	return vibejson.Index{Src: src, Entries: chunk[:n:n]}, ref, nil
}

// An arenaPool recycles the arena generations a Segment's fills supersede, so
// that a batch consumer refilling through Reset re-buys none of them. See the
// srcPool and entryPool fields for what it costs not to have one.
//
// The two lists are not one because a superseded generation is not immediately
// reusable: it is still aliased by the sources and tapes of the documents built
// into it, and only Reset — which invalidates every view the segment handed out
// — makes it dead storage. retired collects the current fill's; free holds what
// earlier fills left, and is what take draws from.
type arenaPool[T any] struct {
	free    [][]T
	retired [][]T
	// peak is how many generations the widest fill so far superseded, and it
	// bounds how many free chunks recycle keeps. It is a high-water mark that is
	// never lowered, which is the same convention every other retained buffer in
	// this package follows — the installed arena chunk itself is kept at its
	// high-water by Reset, and so are the document and shape tables.
	//
	// Every bound that decays was tried and none works, because a batch
	// consumer's fills are not uniform and the variation is not periodic. The
	// last batch of a scan is a short one that supersedes fewer generations, and
	// batches go to whichever worker is free, so one worker's fills can be
	// mostly short for a long stretch and then not be. Under "the last fill's
	// demand", "the larger of the last two", and "the widest of the last N" for
	// N from eight to a hundred and twenty-eight, the same intermittent
	// two-megabyte re-buy showed up — just at different times. The demand a
	// Segment's arenas can be asked for is set by the batch size its caller
	// configured, so holding that high-water is holding one batch's arena, which
	// is the same order as the arena the Segment already has installed.
	//
	// It is released where every other ingest buffer is: sealIngest for a
	// published chunk, the owned-document transfer, and dropping the Segment.
	peak int
}

// retire records the generation a spill is about to supersede.
//
// A generation with nothing committed in it aliases nothing — the fill's very
// first document was simply too large for it — so it goes straight back on the
// free list and may serve a later, smaller document in this same fill, rather
// than waiting for a Reset that would then count it against the bound.
func (p *arenaPool[T]) retire(chunk []T) {
	switch {
	case cap(chunk) == 0:
	case len(chunk) == 0:
		p.free = append(p.free, chunk)
	default:
		p.retired = append(p.retired, chunk)
	}
}

// take returns the next arena generation, preferring a recycled chunk over a
// fresh allocation. need is the element count the document forcing the spill
// requires, prevCap the capacity of the generation being superseded.
//
// The size a recycled chunk must meet is the size this generation would have
// been allocated at, not merely need. Accepting anything smaller would
// reinstate an early, small generation as the current chunk and restart
// geometric growth from there: the next document would spill again immediately,
// and a fill that spilled once while growing would thrash through the whole
// free list and then re-grow from the bottom — measured as a 1.3 MB
// per-execution regression on a corpus that had been allocating nothing.
// Chunks at the ceiling, the only ones a steady batched fill supersedes, always
// satisfy it, so the common case still recycles.
//
// A recycled chunk's tail is not cleared. Both arena element types are
// pointer-free — a source byte and a four-word IndexEntry — so stale contents
// pin nothing, and every element a document commits is written before it is
// read.
func (p *arenaPool[T]) take(need, prevCap, min, max int) []T {
	want := segmentChunkCap(prevCap, need, min, max)
	// Newest first: sizes are non-decreasing along the list under geometric
	// growth, so the newest entry is the likeliest to be big enough.
	for i := len(p.free) - 1; i >= 0; i-- {
		if cap(p.free[i]) < want {
			continue
		}
		chunk := p.free[i]
		last := len(p.free) - 1
		p.free[i] = p.free[last]
		p.free[last] = nil
		p.free = p.free[:last]
		return chunk[:0]
	}
	return make([]T, 0, want)
}

// recycle promotes the generations this fill superseded onto the free list and
// trims it to the bound. installed is the capacity of the chunk that stays
// installed. It is called by Reset, which is the moment the superseded
// generations stop being readable.
func (p *arenaPool[T]) recycle(installed int) {
	p.peak = max(p.peak, len(p.retired))
	// Chunks smaller than the installed one are dropped rather than promoted.
	// The next generation is sized from the installed chunk's capacity and that
	// only ever grows, so such a chunk can never satisfy a future request: they
	// are the early rungs of the growth ladder a cold Segment climbed once, and
	// keeping them would pin a megabyte of storage nothing can take while also
	// counting against the bound.
	kept := 0
	for _, chunk := range p.free {
		if cap(chunk) >= installed {
			p.free[kept] = chunk
			kept++
		}
	}
	// Clearing before truncating: a dropped chunk left past the new length would
	// stay reachable through the retained capacity, which is the whole storage
	// this filter exists to release.
	clear(p.free[kept:])
	p.free = p.free[:kept]
	for _, chunk := range p.retired {
		if cap(chunk) >= installed {
			p.free = append(p.free, chunk)
		}
	}
	clear(p.retired)
	p.retired = p.retired[:0]
	// Trimming keeps the tail, which is the newly retired end: those chunks are
	// the largest under geometric growth and are exactly the ones the next fill
	// of the same size will ask for.
	if len(p.free) > p.peak {
		n := copy(p.free, p.free[len(p.free)-p.peak:])
		clear(p.free[n:])
		p.free = p.free[:n]
	}
}

// drop releases everything the pool holds. It is for a segment that will never
// be filled again, where the lists are pure debt.
func (p *arenaPool[T]) drop() { *p = arenaPool[T]{} }

// segmentChunkCap sizes the next arena chunk: double the previous within
// [min, max], then at least need.
func segmentChunkCap(prev, need, min, max int) int {
	size := 2 * prev
	if size < min {
		size = min
	}
	if size > max {
		size = max
	}
	if size < need {
		size = need
	}
	return size
}

// AppendPointer resolves one compiled pointer against every document in the
// segment, in ordinal order, appending one RawValue per document to dst: the
// target's exact source bytes when present, the zero RawValue when absent. It
// returns the extended slice. The zero RawValue is the library's standing
// invalid value — no bytes, Kind Invalid — and a present target always has at
// least one byte, so absence needs no side channel and dst[i] stays aligned
// with document i. Appended values borrow the segment's arenas under the usual
// RawValue lifetime rules.
//
// Each pointer token carries its content hash, precomputed once by
// CompilePointer, so per-document resolution rehashes nothing on
// key-hash-enriched objects (see document.IndexOptions.HashKeys).
// Resolution semantics per document are exactly Index.PointerCompiled's: an
// invalid array-index token for an array target stops the batch, returning
// dst truncated to its original length and the token's error.
//
// A shape-taped document resolves the first token against its stored shape —
// one memoized ordinal per shape, no key bytes touched — and descends any
// remaining tokens from the value through the ordinary compiled-pointer
// loop, so both storage forms share one resolution semantics.
func (s *Segment) AppendPointer(dst []vibejson.RawValue, pointer vibejson.CompiledPointer) ([]vibejson.RawValue, error) {
	mark := len(dst)
	var hint shapeTapeHint
	var templateHint storeTemplatePointerHint
	var key0 vibejson.CompiledKey
	if len(pointer.Tokens) > 0 {
		key0 = vibejson.CompiledKey{Key: pointer.Tokens[0].Text, Hash: pointer.Tokens[0].Hash}
	}
	for i := 0; i < s.Len(); i++ {
		if template, templateOK := s.TemplateAt(i); templateOK {
			ordinal, ok, err := templateHint.resolve(template, pointer)
			if err != nil {
				return dst[:mark], err
			}
			if !ok {
				dst = append(dst, vibejson.RawValue{})
				continue
			}
			span := s.TemplateSpan(i, template, ordinal)
			doc := s.DocAt(i)
			raw := vibejson.RawValue{Src: doc.Src[span&0xffff : span>>16]}
			if s.ValueDict {
				raw = s.valueRaw(i, span&0xffff, raw)
			}
			dst = append(dst, raw)
			continue
		}
		if r := s.ShapeTapeRefAt(i); r.Rec != nil {
			doc := s.DocAt(i)
			if len(pointer.Tokens) == 0 {
				// The empty pointer selects the root. Compact collection rows recover
				// this otherwise-cold span from their validated source.
				start, end := s.shapeTapeRootSpan(doc, r)
				dst = append(dst, vibejson.RawValue{Src: doc.Src[start:end]})
				continue
			}
			ord := hint.lookup(r.Rec, key0)
			if ord < 0 {
				dst = append(dst, vibejson.RawValue{})
				continue
			}
			rest := pointer.Tokens[1:]
			if len(rest) == 0 {
				// The common single-token pointer names the value itself: its
				// span is already in hand at both entry widths, so the raw
				// slice is taken with no node to reconstitute or escape. Under
				// ValueDict a dictionary-backed value reads its interned span
				// from the shared arena instead — byte-identical.
				if r.Narrow {
					nv := s.NarrowAt(i, r, int(ord))
					raw := vibejson.RawValue{Src: doc.Src[nv.Span&0xFFFF : nv.Span>>16]}
					if s.ValueDict {
						raw = s.valueRaw(i, nv.Span&0xFFFF, raw)
					}
					dst = append(dst, raw)
				} else {
					e := &doc.Entries[ord]
					raw := vibejson.RawValue{Src: doc.Src[e.Start:e.End]}
					if s.ValueDict {
						raw = s.valueRaw(i, e.Start, raw)
					}
					dst = append(dst, raw)
				}
				continue
			}
			// Deeper tokens descend into the value. A flat value has no
			// children, so the descent resolves to absence except for an
			// array-index token error on an empty-array value; only this
			// uncommon path reconstitutes a narrow entry, which the descent
			// never lets outlive the iteration.
			var wide vibejson.IndexEntry
			var node vibejson.Node
			if r.Narrow {
				wide = s.NarrowAt(i, r, int(ord)).widen()
				node = vibejson.Node{Src: &doc.Src[0], Entry: &wide}
			} else {
				node = vibejson.Node{Src: &doc.Src[0], Entry: &doc.Entries[ord]}
			}
			next, ok, err := node.PointerTokens(rest)
			if err != nil {
				return dst[:mark], err
			}
			if !ok {
				dst = append(dst, vibejson.RawValue{})
				continue
			}
			raw := next.Raw()
			if s.ValueDict {
				raw = s.valueRaw(i, next.Entry.Start, raw)
			}
			dst = append(dst, raw)
			continue
		}
		node, ok, err := s.DocAt(i).PointerCompiled(pointer)
		if err != nil {
			return dst[:mark], err
		}
		if !ok {
			dst = append(dst, vibejson.RawValue{})
			continue
		}
		raw := node.Raw()
		if s.ValueDict {
			raw = s.valueRaw(i, node.Entry.Start, raw)
		}
		dst = append(dst, raw)
	}
	return dst, nil
}

// AppendPointerRows is the sparse-gather form of [Segment.AppendPointer]. It
// resolves pointer only for the document ordinals in rows, in the order
// supplied, and appends one RawValue per ordinal to dst. Duplicate ordinals
// produce duplicate values; an out-of-range ordinal panics like [Segment.Doc].
// Absence, error rollback, borrowing, compiled-token, duplicate-key, and value
// dictionary semantics are exactly AppendPointer's.
//
// A shape-taped document resolves the first token against its proven shape and
// reads its narrow or wide value entry directly, so gathering selected rows
// never widens their compact tapes. This makes the method suitable for query
// engines applying an inverted posting list before materializing projected or
// aggregate columns: its work is O(len(rows)), not O(s.Len()).
func (s *Segment) AppendPointerRows(dst []vibejson.RawValue, rows []int, pointer vibejson.CompiledPointer) ([]vibejson.RawValue, error) {
	mark := len(dst)
	var hint shapeTapeHint
	var templateHint storeTemplatePointerHint
	var key0 vibejson.CompiledKey
	if len(pointer.Tokens) > 0 {
		key0 = vibejson.CompiledKey{Key: pointer.Tokens[0].Text, Hash: pointer.Tokens[0].Hash}
	}
	for _, i := range rows {
		if template, templateOK := s.TemplateAt(i); templateOK {
			ordinal, ok, err := templateHint.resolve(template, pointer)
			if err != nil {
				return dst[:mark], err
			}
			if !ok {
				dst = append(dst, vibejson.RawValue{})
				continue
			}
			span := s.TemplateSpan(i, template, ordinal)
			doc := s.DocAt(i)
			raw := vibejson.RawValue{Src: doc.Src[span&0xffff : span>>16]}
			if s.ValueDict {
				raw = s.valueRaw(i, span&0xffff, raw)
			}
			dst = append(dst, raw)
			continue
		}
		if r := s.ShapeTapeRefAt(i); r.Rec != nil {
			doc := s.DocAt(i)
			if len(pointer.Tokens) == 0 {
				start, end := s.shapeTapeRootSpan(doc, r)
				dst = append(dst, vibejson.RawValue{Src: doc.Src[start:end]})
				continue
			}
			ord := hint.lookup(r.Rec, key0)
			if ord < 0 {
				dst = append(dst, vibejson.RawValue{})
				continue
			}
			rest := pointer.Tokens[1:]
			if len(rest) == 0 {
				if r.Narrow {
					nv := s.NarrowAt(i, r, int(ord))
					raw := vibejson.RawValue{Src: doc.Src[nv.Span&0xFFFF : nv.Span>>16]}
					if s.ValueDict {
						raw = s.valueRaw(i, nv.Span&0xFFFF, raw)
					}
					dst = append(dst, raw)
				} else {
					e := &doc.Entries[ord]
					raw := vibejson.RawValue{Src: doc.Src[e.Start:e.End]}
					if s.ValueDict {
						raw = s.valueRaw(i, e.Start, raw)
					}
					dst = append(dst, raw)
				}
				continue
			}
			var wide vibejson.IndexEntry
			var node vibejson.Node
			if r.Narrow {
				wide = s.NarrowAt(i, r, int(ord)).widen()
				node = vibejson.Node{Src: &doc.Src[0], Entry: &wide}
			} else {
				node = vibejson.Node{Src: &doc.Src[0], Entry: &doc.Entries[ord]}
			}
			next, ok, err := node.PointerTokens(rest)
			if err != nil {
				return dst[:mark], err
			}
			if !ok {
				dst = append(dst, vibejson.RawValue{})
				continue
			}
			raw := next.Raw()
			if s.ValueDict {
				raw = s.valueRaw(i, next.Entry.Start, raw)
			}
			dst = append(dst, raw)
			continue
		}
		node, ok, err := s.DocAt(i).PointerCompiled(pointer)
		if err != nil {
			return dst[:mark], err
		}
		if !ok {
			dst = append(dst, vibejson.RawValue{})
			continue
		}
		raw := node.Raw()
		if s.ValueDict {
			raw = s.valueRaw(i, node.Entry.Start, raw)
		}
		dst = append(dst, raw)
	}
	return dst, nil
}
