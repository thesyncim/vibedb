package store

import (
	"errors"
	"fmt"
	"hash/maphash"
	"math/bits"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/thesyncim/vibedb/internal/storekey"
	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/document"
)

// Options fixes the representation of chunks created by a collection. The
// zero value selects 64-document chunks with the ordinary Segment layout.
// ShapeTapes, Postings, ValueDict, and IndexOptions have the same semantics as
// their Segment counterparts. Options are frozen by the first operation that
// initializes the collection (currently Put or CreateIndex).
type Options struct {
	// ChunkDocuments bounds documents rebuilt by one ordinary mutation. Zero
	// selects 64; valid explicit values are 1 through 64.
	//
	// 64 is the default because it is the only value at which a chunk's slot
	// mask and an index posting word are the same object: both are one uint64.
	// Smaller chunks leave posting bits unused, spread postings over more chunk
	// ids, and make a full scan visit more chunks.
	//
	// Lowering it can reduce replacement latency because a mutation rebuilds its
	// whole chunk, at the cost of higher metadata and scan overhead. Bulk loads
	// should use [Builder], which does not rebuild existing chunks. Benchmark the
	// intended corpus before changing this representation bound; measurements
	// belong in the performance documentation rather than this API contract.
	ChunkDocuments int
	// IndexOptions configures each bounded Segment's structural index.
	IndexOptions document.IndexOptions
	// ShapeTapes enables per-chunk shape-deduplicated tapes. It stores a
	// document compactly only if that document conforms — a non-empty root
	// object whose every member value is one tape entry, so a single nested
	// object or array disqualifies it — which makes this a lever for flat
	// corpora rather than a general one. See [Segment.ShapeTapes] for the full
	// conformance rule and how to tell which corpus you have.
	ShapeTapes bool
	// Postings builds the physical posting layer from the first Put.
	Postings bool
	// ValueDict enables a value dictionary scoped to each immutable chunk. It
	// increases the collection's live footprint — documents keep their verbatim
	// source for zero-copy reads, so the dictionary is purely additive — and
	// pays only at rest, in repeated source a compacting or persisting writer can
	// then drop. See [Segment.ValueDict].
	ValueDict bool
	// Schema optionally enforces compiled root and RFC 6901 field constraints
	// on every insert or replacement. Nil preserves the schemaless fast path.
	Schema *Schema
}

// StateOptions is the pointer-free subset copied into every immutable
// collection generation. Collection-wide schema belongs to the collection, not
// to each
// publication; keeping it out of this value preserves the schemaless state
// size class and avoids bytes/op growth on every mutation.
type StateOptions struct {
	IndexOptions   document.IndexOptions
	ChunkDocuments int
	ShapeTapes     bool
	Postings       bool
	ValueDict      bool
}

func (o Options) stateOptions() StateOptions {
	return StateOptions{
		IndexOptions: o.IndexOptions, ChunkDocuments: o.ChunkDocuments,
		ShapeTapes: o.ShapeTapes, Postings: o.Postings,
		ValueDict: o.ValueDict,
	}
}

// MaxChunkDocuments is the fixed number of documents a single chunk addresses.
// It bounds the per-chunk slot arrays and the eight-bit slot index, and it is
// the ceiling and default for Options.ChunkDocuments.
const MaxChunkDocuments = 64

// ErrTooLarge reports that the persistent chunk address space is full.
// The limit is 2^32-1 chunks (at most 274 billion documents with the default
// chunk size), so reaching it indicates a caller architecture error rather
// than an ordinary capacity event. The guard prevents uint32 wraparound.
var ErrTooLarge = errors.New("vibedb: collection chunk address space exhausted")

// Normalized returns o with zero fields resolved to their defaults and every
// bound validated, or an error describing the first invalid setting. Callers
// pass the result to collection construction so later code can assume settled,
// in-range values.
func (o Options) Normalized() (Options, error) {
	if o.ChunkDocuments == 0 {
		o.ChunkDocuments = MaxChunkDocuments
	}
	if o.ChunkDocuments < 1 || o.ChunkDocuments > MaxChunkDocuments {
		return Options{}, fmt.Errorf("vibedb: collection ChunkDocuments must be in [1,%d]", MaxChunkDocuments)
	}
	if o.Schema != nil && !o.Schema.Valid() {
		return Options{}, fmt.Errorf(
			"%w: uninitialized compiled schema",
			ErrSchemaDefinition,
		)
	}
	return o, nil
}

// A Collection is a keyed set of JSON documents with immutable snapshots and a
// lock-free raw read path. It is the in-memory source model the query engine
// reads and durable collections are built from. Put and Delete rebuild at most
// one bounded document chunk, path-copy only
// bounded-radix metadata, and publish one new state through an atomic pointer. A
// replacement parses only its new document; unchanged source and structural-tape
// storage is immutable and shared into the new chunk.
//
// The zero Collection is ready to use: set Options before the first Put or
// CreateIndex, or use [New]. Invalid chunk bounds in a directly
// constructed Collection are reported by the first operation that initializes
// it, so only [New] reports configuration errors up front. A Collection is safe
// for concurrent use. Snapshot readers take no writer lock; GetRaw and Range
// take no lock at all. Get may enter the synchronized shape-tape widening cache
// described on [Snapshot.Get]. A Collection must not be copied after first use.
type Collection struct {
	Options Options

	// name is the catalog name a [Database] published this collection under.
	// It is empty for a standalone collection and immutable afterwards, so
	// Name needs no lock.
	name string

	mu      sync.Mutex
	state   atomic.Pointer[State]
	options Options

	// Writer-only chunk-id sets make allocation and physical-index tracking
	// O(1). Empty chunk ids are reused, so insert/delete churn cannot grow the
	// chunk address space; reclamation takes indexed ids directly rather than
	// rescanning the entire vector after every bounded batch.
	free          storeIDSet
	postingChunks storeIDSet

	indexes      map[string]*storeIndexBuild
	indexVisit   uint32
	exactAliases uint32
}

// WithBulkSnapshot runs fn with c's current State (materializing an
// empty one from c.Options if the collection has never been written to),
// holding c's writer lock for fn's duration. It exists so
// package store/durable can bulk-serialize a collection as a durable collection
// without exposing the mutex, atomic state pointer, or option-normalization
// internals that a direct field read would require.
func (c *Collection) WithBulkSnapshot(fn func(state *State) error) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	state := c.state.Load()
	if state == nil {
		normalized, err := c.Options.Normalized()
		if err != nil {
			return err
		}
		state = &State{StateOptions: normalized.stateOptions()}
	}
	return fn(state)
}

// State is one immutable generation of a collection: its documents, keys, and
// exact-index snapshots at a single point in time. Mutations path-copy the
// structures they touch and publish a new State, so a captured pointer keeps
// naming the generation it was taken from even as writers advance. Exported
// fields describe that generation; the unexported fields track how much of it
// is borrowed from a mapped source versus owned Go heap.
type State struct {
	Generation   uint64
	Count        int
	ChunkCount   uint32
	seed         maphash.Seed
	StateOptions StateOptions
	keys         *storekey.Node
	// baseKeys is the compact immutable directory created by Builder. keys is
	// then only the path-copied overlay for later insertions and moved keys.
	baseKeys *storeMappedKeys
	// mappedDocs pins the off-heap source block for the lifetime of every
	// state, exactly like baseKeys. It must never be dropped early: a chunk
	// rebuild carries surviving rows by reference out of the mapped block
	// (appendStoreDoc copies nothing), and those borrowed pointers are
	// invisible to the garbage collector, so this edge is the only thing
	// standing between a rebuilt chunk and the block's finalizer munmap.
	mappedDocs *storeMappedDocs
	Chunks     storeChunkVector
	Indexes    []IndexInfo
	secondary  []storeIndexSnapshot
}

// Chunk is the unit of document storage and copy-on-write publication: up to
// MaxChunkDocuments documents addressed by an eight-bit slot, plus the ordering
// permutation, liveness mask, and zone-pruning summary that let scans and index
// probes work without decoding rows. A chunk may borrow its keys and bytes from
// a mapped source or own them on the Go heap; a mutation rebuilds and republishes
// the whole chunk rather than editing it in place.
type Chunk struct {
	Docs       Segment
	keys       []string
	keyBytes   []byte
	mappedKeys *storeMappedKeys
	mappedBase uint64
	Ord        [MaxChunkDocuments]uint8
	Live       uint64
	Count      uint8
}

type storeIDSet struct {
	ids []uint32
	pos map[uint32]int
}

func (s *storeIDSet) add(id uint32) {
	if s.pos == nil {
		s.pos = make(map[uint32]int)
	}
	if _, exists := s.pos[id]; exists {
		return
	}
	s.pos[id] = len(s.ids)
	s.ids = append(s.ids, id)
}

func (s *storeIDSet) remove(id uint32) {
	pos, exists := s.pos[id]
	if !exists {
		return
	}
	last := len(s.ids) - 1
	other := s.ids[last]
	s.ids[pos] = other
	s.ids = s.ids[:last]
	delete(s.pos, id)
	if pos != last {
		s.pos[other] = pos
	}
}

func initChunkSegment(
	docs *Segment,
	options StateOptions,
	postings bool,
) {
	*docs = Segment{
		Options:    options.IndexOptions,
		ShapeTapes: options.ShapeTapes,
		Postings:   postings,
		ValueDict:  options.ValueDict,
		// A collection carries unchanged document sources directly into the next
		// immutable chunk. Exact first source chunks prevent a short document
		// from pinning stream-sized spare capacity for its whole live tenure.
		arenaMinSrc:     1,
		arenaMinEntries: 16,
		dropEmptySpill:  true,
	}
	// ShapeCache's default arenas amortize compilation across an unbounded
	// Segment. A collection chunk is capped at 64 documents and is rebuilt by copy;
	// exact minima prevent one page-local shape from pinning bulk-sized field,
	// table, record, and spelling slabs. The compiler and read representation
	// stay identical, so this policy change has no query-path branch.
	docs.shapes.arenaMinRecords = 1
	docs.shapes.arenaMinFields = 1
	docs.shapes.arenaMinSlots = 1
	docs.shapes.arenaMinBytes = 1
}

// prepareStoreSegment reserves the dense per-chunk tables and seeds its shape
// cache with exactly the immutable shape records referenced by surviving rows.
// Source bytes and classic tapes remain independently immutable and may be
// shared; row tables and the narrow-value slab are copied because their offsets
// are chunk-local. Excluding replaceSlot is what prevents an updated-away
// shape from becoming historical cache debt.
func prepareStoreSegment(
	docs *Segment,
	options StateOptions,
	postings bool,
	old *Chunk,
	live uint64,
	replaceSlot int,
	replaceBytes int,
) {
	initChunkSegment(docs, options, postings)
	// Every surviving row is carried in by reference through appendStoreDoc;
	// only the replacement slot is parsed, so this set indexes at most one
	// document through buildDoc. The entry arena's growth headroom therefore
	// has no future writer and would be retained unwritten for the published
	// chunk's live tenure. The Builder shares initChunkSegment but parses
	// every row of its page, so the flag belongs here rather than there.
	docs.singleAppend = true
	count := bits.OnesCount64(live)
	docs.docs = make([]vibejson.Index, 0, count)
	// The replacement's own tape is the one thing buildDoc allocates here, and
	// without a reservation it always allocates it twice over: arenaMinEntries
	// is 16, no real document fits, and the ErrIndexFull spill path then buys a
	// len(src)+2 scratch tape — an order of magnitude more entries than any
	// document actually has — builds into it, copies the exact tape out through
	// exactSpillTape, and sealIngest discards the scratch at the end of this
	// same rebuild. Reserving from the chunk's own observed entries-per-byte
	// removes both transient growth allocations while keeping the retained slack
	// bounded. The allocation gate and measured provenance live in
	// TestStoreCollectionReplaceAllocation and docs/performance.md.
	entryCap := storeReplacementEntries(old, replaceSlot, replaceBytes)
	if !options.ShapeTapes || old == nil {
		if entryCap != 0 {
			docs.entryChunk = make([]vibejson.IndexEntry, 0, entryCap)
		}
		return
	}
	narrowCap := 0
	shaped := false
	for bitsLeft := live; bitsLeft != 0; bitsLeft &= bitsLeft - 1 {
		slot := bits.TrailingZeros64(bitsLeft)
		if slot == replaceSlot || old.Live&(uint64(1)<<uint(slot)) == 0 {
			continue
		}
		if template, ok := old.Docs.TemplateAt(int(old.Ord[slot])); ok {
			entryCap += len(template.Index.Entries)
			continue
		}
		ref := old.Docs.ShapeTapeRefAt(int(old.Ord[slot]))
		if ref.Rec != nil {
			shaped = true
		}
		if ref.Narrow {
			narrowCap += len(ref.Rec.Fields)
		}
		docs.shapes.seedRecord(ref.Rec)
	}
	if replaceSlot >= 0 {
		if old.Live&(uint64(1)<<uint(replaceSlot)) != 0 {
			oldOrd := int(old.Ord[replaceSlot])
			if template, ok := old.Docs.TemplateAt(oldOrd); ok {
				entryCap += len(template.Index.Entries)
			} else {
				ref := old.Docs.ShapeTapeRefAt(oldOrd)
				if ref.Narrow {
					narrowCap += len(ref.Rec.Fields)
				}
			}
		} else if old.Count != 0 {
			// An insertion has no old row to size from. Reserve the current
			// average so a same-shape replacement—the common case when a freed
			// slot is reused—does not grow and recopy the whole narrow slab.
			narrowCap += old.Docs.narrowLen() / int(old.Count)
		}
	}
	// Reserve the header slab only on evidence that this chunk produces
	// headers, because conformance is a property of the corpus and not of the
	// option: shapeTapeConforms requires a flat root object, so one nested
	// member anywhere in a document disqualifies that whole document, and a
	// corpus that always has one disqualifies every document forever. A
	// speculative reservation is not free — it is non-nil, so commitDoc's
	// alignment rule then stores a zero header for every document — and it
	// measured 26 B per document of pure loss on a corpus where nothing could
	// ever conform. A surviving row that already carries a header is the exact
	// evidence that this chunk's corpus does conform, so pre-sizing pays for
	// itself; the replaced row is deliberately not evidence, since reserving
	// from a shape that is being written away would reinstate the same
	// speculative slab. Where there is no evidence, commitDoc allocates on the
	// first document that actually conforms, so a corpus that starts
	// conforming loses one reservation, never a header. A one-row rebuild is
	// left to that same lazy path, which keeps the ChunkDocuments=1 update
	// path at its original allocation count.
	if count > 1 && shaped {
		docs.tapeRefs = make([]ShapeTapeRef, 0, count)
	}
	if narrowCap != 0 {
		docs.narrow = make([]ShapeNarrowValue, 0, narrowCap)
	}
	if entryCap != 0 {
		docs.entryChunk = make([]vibejson.IndexEntry, 0, entryCap)
	}
}

// storeReplacementEntries estimates how many structural entries the one
// document a rebuild parses will produce, from a document the outgoing chunk
// already holds. Entries per source byte is a property of the corpus's shape,
// not of any single row, so one live row of the same chunk predicts the
// replacement closely enough to keep the build inside its arena; scaling by the
// two source lengths is what keeps that true when the new body is much longer
// or shorter than the row it is measured against.
//
// It is a hint in the strict sense: too small only falls back to the spill path
// that would have run unconditionally before, and too large is capped at
// len(src)+2, the count that always suffices, so no caller can be made worse
// off than the unreserved build. Zero means there is no evidence — an insert
// into a fresh chunk, or a chunk whose rows are all stored in a compact shape
// or narrow form that owns no classic tape to measure.
func storeReplacementEntries(old *Chunk, replaceSlot, replaceBytes int) int {
	if old == nil || replaceSlot < 0 || replaceBytes <= 0 || old.Live == 0 {
		return 0
	}
	// An insert lands on a slot with no predecessor, so measure any surviving
	// row of the same chunk instead. Chunk-local rows are the closest evidence
	// available and cost one indexed read either way.
	slot := replaceSlot
	if old.Live&(uint64(1)<<uint(replaceSlot)) == 0 {
		slot = bits.TrailingZeros64(old.Live)
	}
	ord := int(old.Ord[slot])
	index := old.Docs.DocAt(ord)
	entries := len(index.Entries)
	if template, ok := old.Docs.TemplateAt(ord); ok {
		// A templated row's classic tape is not stored, but the template holds
		// exactly the tape appendStoreDoc synthesizes for it, so it measures the
		// same corpus shape.
		entries = len(template.Index.Entries)
	}
	if entries == 0 || len(index.Src) == 0 {
		return 0
	}
	// Deliberately 64-bit: both factors are bounded only by a document's size,
	// so their product overflows a 32-bit int on GOARCH=386 well inside the
	// sizes this store accepts. The len(src)+2 ceiling below returns the result
	// to int range on every platform.
	scaled := max(
		// Entry count tracks structure, not length, so a body that is merely
		// shorter than its predecessor usually has exactly as many members. Taking
		// the measured row as a floor covers that case — the ordinary shape of a
		// field-level edit — where pure length scaling would under-reserve and
		// spill on every write that shortens a string.
		(uint64(replaceBytes)*uint64(entries)+uint64(len(index.Src))-1)/
			uint64(len(index.Src)), uint64(entries))
	// Two entries of slack, not a percentage: the arena outlives the rebuild
	// that fills it — the new row's tape is carried by reference into every
	// later generation of this chunk — so every reserved entry the document
	// does not use is retained for as long as the row is. A proportional
	// margin would buy insurance against spilling with permanent bytes, and a
	// spill is only the unreserved path this reservation replaced.
	scaled += 2
	if scaled > uint64(replaceBytes)+2 {
		return replaceBytes + 2
	}
	return int(scaled)
}

// appendStoreDoc carries one validated immutable document into a new dense
// Segment without copying its source or rebuilding its structural tape. Narrow
// shape values are the sole set-relative storage: copy them into the new slab
// and rewrite the private offset before commit. commitDoc then rebuilds any
// enabled chunk-local postings or value dictionary against the new ordinal.
func appendStoreDoc(dst *Segment, old *Segment, oldOrd int) int {
	if template, ok := old.TemplateAt(oldOrd); ok {
		used := len(dst.entryChunk)
		dst.entryChunk = old.synthStoreTemplate(oldOrd, template, dst.entryChunk)
		index := vibejson.Index{Src: old.DocAt(oldOrd).Src, Entries: dst.entryChunk[used:]}
		return dst.commitDoc(index, ShapeTapeRef{})
	}
	index := old.DocAt(oldOrd)
	ref := old.ShapeTapeRefAt(oldOrd)
	promoted := false
	if ref.Rec == nil && dst.ShapeTapes {
		index, ref, promoted = copyStoreShapeTape(dst, index)
	}
	if ref.Narrow && !promoted {
		n := uint32(len(ref.Rec.Fields))
		oldRef := ref
		ref.off = uint32(len(dst.narrow))
		for i := range n {
			dst.narrow = append(dst.narrow, old.NarrowAt(oldOrd, oldRef, int(i)))
		}
	}
	return dst.commitDoc(index, ref)
}

// copyStoreShapeTape promotes one reused classic flat object into the new
// chunk's compact representation without modifying its immutable old tape.
// Resolve preserves the ordinary repeat-sighting economics, and the exact key
// comparison preserves shapeTapeCompact's collision-proof trust boundary.
func copyStoreShapeTape(dst *Segment, index vibejson.Index) (vibejson.Index, ShapeTapeRef, bool) {
	entries := index.Entries
	if len(entries) == 0 {
		return index, ShapeTapeRef{}, false
	}
	root := &entries[0]
	count := int(root.Count())
	if root.Kind() != document.Object || count == 0 {
		return index, ShapeTapeRef{}, false
	}
	shape, ok := dst.shapes.Resolve(vibejson.NodeFromEntries(index.Src, entries))
	if !ok || shape.rec.dupKeys {
		return index, ShapeTapeRef{}, false
	}
	rec := shape.rec
	if !shapeTapeConforms(index, rec) {
		return index, ShapeTapeRef{}, false
	}
	ref := ShapeTapeRef{
		Rec:      rec,
		Start:    root.Start,
		End:      root.End,
		enriched: root.KeysHashed(),
	}
	if root.End <= ShapeNarrowMaxEnd && !dst.wideValueTapes &&
		uint64(len(dst.narrow))+uint64(count) <= uint64(^uint32(0)) {
		ref.Narrow = true
		ref.off = dst.appendNarrowShapeValues(entries, count)
		return vibejson.Index{Src: index.Src}, ref, true
	}
	values := make([]vibejson.IndexEntry, count)
	for m := range values {
		values[m] = entries[2*m+2]
	}
	return vibejson.Index{Src: index.Src, Entries: values}, ref, true
}

// chunkKeysCap sizes a rebuilt chunk's key directory to its highest
// addressable slot instead of the chunk capacity. Slots address by position,
// so sparse chunks (the common small-collection case) stop paying a full
// capacity directory per rebuild. The bound covers every slot any reader can
// observe: live slots (point lookups resolve through the key directory to
// live slots; scans, indexes, and the compiled-key fast path all iterate or
// verify the live mask first) plus removed slots being cleared and an
// insert's replacement slot. Readers never address beyond it.
func chunkKeysCap(old *Chunk, live uint64, replaceSlot int) int {
	var oldLive uint64
	if old != nil {
		oldLive = old.Live
	}
	n := bits.Len64(live | oldLive)
	if replaceSlot+1 > n {
		n = replaceSlot + 1
	}
	return n
}

// buildStoreChunk is the single bounded rebuild primitive used by inserts,
// replacements, deletes, index backfill, and index reclaim.
// live is the exact post-edit slot mask. replaceSlot selects one slot whose
// bytes come from src; -1 means every remaining document comes from old.
func buildStoreChunk(
	options StateOptions,
	postings bool,
	old *Chunk,
	live uint64,
	replaceSlot int,
	key string,
	src []byte,
) (*Chunk, error) {
	if live == 0 {
		// Deleting or expiring the final row removes the vector leaf outright.
		// There is no replacement to validate and no empty chunk object to
		// publish, so avoid constructing tables that would be discarded below.
		return nil, nil
	}
	chunk := &Chunk{
		keys: make([]string, chunkKeysCap(old, live, replaceSlot)),
	}
	prepareStoreSegment(&chunk.Docs, options, postings, old, live, replaceSlot, len(src))
	if old != nil {
		for bitsLeft := old.Live; bitsLeft != 0; bitsLeft &= bitsLeft - 1 {
			slot := bits.TrailingZeros64(bitsLeft)
			chunk.keys[slot] = old.Key(slot)
		}
	}
	chunk.Live = live
	if old != nil {
		for removed := old.Live &^ live; removed != 0; removed &= removed - 1 {
			chunk.keys[bits.TrailingZeros64(removed)] = ""
		}
	}
	if replaceSlot >= 0 {
		chunk.keys[replaceSlot] = key
	}
	for bitsLeft := chunk.Live; bitsLeft != 0; bitsLeft &= bitsLeft - 1 {
		i := bits.TrailingZeros64(bitsLeft)
		var ord int
		if i == replaceSlot {
			var err error
			ord, err = chunk.Docs.Append(src)
			if err != nil {
				return nil, err
			}
		} else {
			ord = appendStoreDoc(&chunk.Docs, &old.Docs, int(old.Ord[i]))
		}
		chunk.Ord[i] = uint8(ord)
		chunk.Count++
	}
	if chunk.Count == 0 {
		return nil, nil
	}
	// The chunk is complete and about to be published immutable: no further
	// document can be indexed into it, so its ingest-only working state is
	// released before it acquires the lifetime of the generation holding it.
	chunk.Docs.sealIngest()
	return chunk, nil
}

// buildStoreChunkSchema is the schema-on specialization of buildStoreChunk.
// Keeping the replacement append explicit gives the much more common
// schemaless build a direct Segment.Append call; a callback or inner-loop mode
// branch would add work to every schemaless mutation.
func buildStoreChunkSchema(
	options StateOptions,
	schema *Schema,
	postings bool,
	old *Chunk,
	live uint64,
	replaceSlot int,
	key string,
	src []byte,
) (*Chunk, error) {
	if live == 0 {
		return nil, nil
	}
	chunk := &Chunk{
		keys: make([]string, chunkKeysCap(old, live, replaceSlot)),
	}
	prepareStoreSegment(
		&chunk.Docs, options, postings, old, live, replaceSlot, len(src),
	)
	if old != nil {
		for bitsLeft := old.Live; bitsLeft != 0; bitsLeft &= bitsLeft - 1 {
			slot := bits.TrailingZeros64(bitsLeft)
			chunk.keys[slot] = old.Key(slot)
		}
	}
	chunk.Live = live
	if old != nil {
		for removed := old.Live &^ live; removed != 0; removed &= removed - 1 {
			chunk.keys[bits.TrailingZeros64(removed)] = ""
		}
	}
	if replaceSlot >= 0 {
		chunk.keys[replaceSlot] = key
	}
	for bitsLeft := chunk.Live; bitsLeft != 0; bitsLeft &= bitsLeft - 1 {
		i := bits.TrailingZeros64(bitsLeft)
		var ord int
		if i == replaceSlot {
			var err error
			ord, err = chunk.Docs.appendStoreSchema(src, schema)
			if err != nil {
				return nil, err
			}
		} else {
			ord = appendStoreDoc(
				&chunk.Docs, &old.Docs, int(old.Ord[i]),
			)
		}
		chunk.Ord[i] = uint8(ord)
		chunk.Count++
	}
	if chunk.Count == 0 {
		return nil, nil
	}
	chunk.Docs.sealIngest()
	return chunk, nil
}

func rebuildStoreChunk(
	options StateOptions,
	postings bool,
	old *Chunk,
	slot int,
	key string,
	src []byte,
	keep bool,
) (*Chunk, error) {
	var live uint64
	if old != nil {
		live = old.Live
	}
	mask := uint64(1) << uint(slot)
	if keep {
		live |= mask
		return buildStoreChunk(
			options, postings, old, live, slot, key, src,
		)
	}
	return buildStoreChunk(
		options, postings, old, live&^mask, -1, "", nil,
	)
}

func rebuildStoreChunkSchema(
	options StateOptions,
	schema *Schema,
	postings bool,
	old *Chunk,
	slot int,
	key string,
	src []byte,
) (*Chunk, error) {
	var live uint64
	if old != nil {
		live = old.Live
	}
	live |= uint64(1) << uint(slot)
	return buildStoreChunkSchema(
		options, schema, postings, old, live, slot, key, src,
	)
}

// Key returns the document key stored at the given physical slot. The result
// is borrowed: it aliases the chunk's owned key slice or the underlying mapped
// source and stays valid only while this chunk generation is reachable.
func (c *Chunk) Key(slot int) string {
	if c.keys != nil {
		return c.keys[slot]
	}
	return c.mappedKeys.keyAt(c.mappedBase, c.Ord[slot])
}

func (c *Collection) initLocked() (*State, error) {
	if state := c.state.Load(); state != nil {
		return state, nil
	}
	options, err := c.Options.Normalized()
	if err != nil {
		return nil, err
	}
	c.options = options
	c.free.pos = make(map[uint32]int)
	state := &State{
		seed: maphash.MakeSeed(), StateOptions: options.stateOptions(),
	}
	c.state.Store(state)
	return state, nil
}

// Put validates src and atomically inserts or replaces key. It copies src and
// a newly inserted key; callers may reuse them after return. created reports
// whether key was absent.
//
// A failed validation leaves the collection and every Snapshot unchanged.
//
// One Put rebuilds one whole chunk: it parses the new document and carries
// every other row of that chunk into a fresh immutable chunk by reference, then
// publishes it. The cost is therefore O(ChunkDocuments) copying per write, not
// O(1), which is the price of chunks that are immutable once published,
// snapshots that never see a partial edit, and deletes that leave no tombstone.
// Loading a corpus through a loop of Put pays that factor on every row; use
// [Builder], which does not rebuild at all.
func (c *Collection) Put(key string, src []byte) (created bool, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	state, err := c.initLocked()
	if err != nil {
		return false, err
	}
	hash := maphash.String(state.seed, key)
	old, loc, found := storeStateKeyLookupChunk(state, hash, key)
	if found {
		storedKey := old.Key(int(loc.Slot))
		var chunk *Chunk
		if schema := c.options.Schema; schema != nil {
			chunk, err = rebuildStoreChunkSchema(
				state.StateOptions, schema,
				c.options.Postings, old, int(loc.Slot),
				storedKey, src,
			)
		} else {
			chunk, err = rebuildStoreChunk(
				state.StateOptions, c.options.Postings, old,
				int(loc.Slot), storedKey, src, true,
			)
		}
		if err != nil {
			return false, err
		}
		next := *state
		next.Generation++
		next.Chunks = state.Chunks.set(loc.Chunk, chunk)
		c.noteChunkPostingsLocked(loc.Chunk, old, chunk)
		catalogChanged, secondaryChanged := c.noteIndexesForChunkLocked(loc.Chunk, old, chunk, uint64(1)<<loc.Slot)
		if catalogChanged {
			next.Indexes = c.indexInfosLocked()
		}
		if secondaryChanged {
			next.secondary = c.indexSnapshotsLocked()
		}
		c.state.Store(&next)
		return false, nil
	}

	if len(c.free.ids) == 0 && state.Chunks.Count == ^uint32(0) {
		return false, ErrTooLarge
	}
	chunkID, slot, old := c.allocateSlotLocked(state)
	var chunk *Chunk
	if schema := c.options.Schema; schema != nil {
		chunk, err = rebuildStoreChunkSchema(
			state.StateOptions, schema, c.options.Postings,
			old, slot, key, src,
		)
	} else {
		chunk, err = rebuildStoreChunk(
			state.StateOptions, c.options.Postings, old,
			slot, key, src, true,
		)
	}
	if err != nil {
		return false, err
	}
	// Validation is complete and publication cannot fail. Only now take key
	// ownership, so malformed JSON and schema violations do not allocate a
	// transient key that is immediately discarded.
	key = strings.Clone(key)
	chunk.keys[slot] = key
	next := *state
	next.Generation++
	next.Count++
	loc = Location{Chunk: chunkID, Slot: uint8(slot)}
	next.keys = storekey.Insert(state.keys, hash, key, loc)
	if chunkID == state.Chunks.Count {
		next.Chunks, _ = state.Chunks.append(chunk)
	} else {
		next.Chunks = state.Chunks.set(chunkID, chunk)
	}
	if old == nil {
		next.ChunkCount++
	}
	c.noteChunkPostingsLocked(chunkID, old, chunk)
	if int(chunk.Count) == state.StateOptions.ChunkDocuments {
		c.removeFreeLocked(chunkID)
	} else {
		c.addFreeLocked(chunkID)
	}
	catalogChanged, secondaryChanged := c.noteIndexesForChunkLocked(chunkID, old, chunk, uint64(1)<<uint(slot))
	if catalogChanged {
		next.Indexes = c.indexInfosLocked()
	}
	if secondaryChanged {
		next.secondary = c.indexSnapshotsLocked()
	}
	c.state.Store(&next)
	return true, nil
}

// Delete atomically removes key. It reports whether key was present. A miss is
// a no-op and does not advance the generation. Existing snapshots retain the
// removed row through their immutable chunk and key-directory roots.
func (c *Collection) Delete(key string) (deleted bool, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	state, err := c.initLocked()
	if err != nil {
		return false, err
	}
	hash := maphash.String(state.seed, key)
	old, loc, found := storeStateKeyLookupChunk(state, hash, key)
	if !found {
		return false, nil
	}
	nextChunk, err := rebuildStoreChunk(
		state.StateOptions, c.options.Postings, old,
		int(loc.Slot), "", nil, false,
	)
	if err != nil {
		return false, err
	}
	next := *state
	next.Generation++
	next.Count--
	next.keys = storekey.Delete(state.keys, hash, key)
	next.Chunks = state.Chunks.set(loc.Chunk, nextChunk)
	if nextChunk == nil {
		next.ChunkCount--
	}
	c.noteChunkPostingsLocked(loc.Chunk, old, nextChunk)
	c.addFreeLocked(loc.Chunk)
	catalogChanged, secondaryChanged := c.noteIndexesForChunkLocked(
		loc.Chunk, old, nextChunk, uint64(1)<<loc.Slot,
	)
	if catalogChanged {
		next.Indexes = c.indexInfosLocked()
	}
	if secondaryChanged {
		next.secondary = c.indexSnapshotsLocked()
	}
	c.state.Store(&next)
	return true, nil
}

func (c *Collection) allocateSlotLocked(state *State) (uint32, int, *Chunk) {
	if len(c.free.ids) == 0 {
		return state.Chunks.Count, 0, nil
	}
	id := c.free.ids[len(c.free.ids)-1]
	chunk := state.Chunks.Get(id)
	if chunk == nil {
		return id, 0, nil
	}
	limitMask := ^uint64(0)
	if state.StateOptions.ChunkDocuments < 64 {
		limitMask = uint64(1)<<uint(state.StateOptions.ChunkDocuments) - 1
	}
	free := ^chunk.Live & limitMask
	if free == 0 {
		panic("vibedb: full collection chunk in free set")
	}
	return id, bits.TrailingZeros64(free), chunk
}

func (c *Collection) addFreeLocked(id uint32) {
	c.free.add(id)
}

func (c *Collection) removeFreeLocked(id uint32) {
	c.free.remove(id)
}

func (c *Collection) noteChunkPostingsLocked(id uint32, old, next *Chunk) {
	oldIndexed := old != nil && old.Docs.Postings
	nextIndexed := next != nil && next.Docs.Postings
	if oldIndexed == nextIndexed {
		return
	}
	if nextIndexed {
		c.postingChunks.add(id)
	} else {
		c.postingChunks.remove(id)
	}
}

// Snapshot returns this in-memory collection's current immutable view. This
// heap-only operation is O(1), never blocks a writer, and remains valid while
// later writes publish new views. Durable collection snapshots have a distinct
// bounded-fold contract. The error return always reports nil; it exists so
// Collection satisfies the same [Mutable] shape as durable.Collection, whose
// Snapshot can fail on I/O.
func (c *Collection) Snapshot() (Snapshot, error) {
	return Snapshot{state: c.state.Load()}, nil
}

// Len returns the number of keys in the current snapshot.
func (c *Collection) Len() uint64 {
	state := c.state.Load()
	if state == nil {
		return 0
	}
	return uint64(state.Count)
}

// Generation returns the monotonically increasing publication number. Zero is
// the empty initial state; every successful mutation publishes the next value.
func (c *Collection) Generation() uint64 {
	state := c.state.Load()
	if state == nil {
		return 0
	}
	return state.Generation
}

// StateMetrics is one coherent, constant-size observation of an in-memory
// collection generation.
type StateMetrics struct {
	Documents  uint64
	Generation uint64
}

// StateMetrics captures the current immutable state pointer once, so its
// document count and generation cannot come from different publications.
func (c *Collection) StateMetrics() StateMetrics {
	if c == nil {
		return StateMetrics{}
	}
	state := c.state.Load()
	if state == nil {
		return StateMetrics{}
	}
	return StateMetrics{
		Documents: uint64(state.Count), Generation: state.Generation,
	}
}

// A Snapshot is a logically immutable collection view. Its zero value is an empty
// snapshot. It is safe for concurrent use and remains valid independently of
// later collection mutations. GetRaw takes no lock, clock call, or
// allocation; Get may populate an equivalent memoized shape-tape widening.
type Snapshot struct {
	state *State
}

// Chunks reports the snapshot's live chunk count, the unit the query work
// budget charges scan admission by. The vector's Count is a high-water figure
// that includes recycled chunks, so this method counts the live set directly.
func (s Snapshot) Chunks() int {
	if s.state == nil {
		return 0
	}
	chunks := 0
	s.state.Chunks.Each(func(_ uint32, _ *Chunk) bool {
		chunks++
		return true
	})
	return chunks
}

// Len returns the number of keys visible in s.
func (s Snapshot) Len() int {
	if s.state == nil {
		return 0
	}
	return s.state.Count
}

// Generation returns the publication generation captured by s.
func (s Snapshot) Generation() uint64 {
	if s.state == nil {
		return 0
	}
	return s.state.Generation
}

// GetRaw returns key's exact JSON bytes as a read-only borrowed RawValue.
func (s Snapshot) GetRaw(key string) (vibejson.RawValue, bool) {
	if s.state == nil {
		return vibejson.RawValue{}, false
	}
	hash := maphash.String(s.state.seed, key)
	// An untouched mapped directory has no heap overlay. Replacements keep a
	// base key at its stable slot and deletes clear the live bit; inserting any
	// non-base location creates the overlay. This dominant reopen/read path can
	// therefore avoid the general overlay router and a duplicate chunk walk.
	if s.state.baseKeys != nil && s.state.keys == nil {
		loc, ok := s.state.baseKeys.lookup(hash, key)
		if !ok {
			return vibejson.RawValue{}, false
		}
		chunk := s.state.Chunks.Get(loc.Chunk)
		if chunk == nil || chunk.Live&(uint64(1)<<loc.Slot) == 0 {
			return vibejson.RawValue{}, false
		}
		return vibejson.RawValue{Src: chunk.Docs.RawAt(int(chunk.Ord[loc.Slot]))}, true
	}
	chunk, loc, ok := storeStateKeyLookupChunk(s.state, hash, key)
	if !ok {
		return vibejson.RawValue{}, false
	}
	return vibejson.RawValue{Src: chunk.Docs.RawAt(int(chunk.Ord[loc.Slot]))}, true
}

// Get returns key's navigable Index. Shape-taped chunks may take their widening
// mutex and allocate once to memoize this document's equivalent classic tape,
// exactly like Segment.Doc; GetRaw is the lock- and allocation-free path when
// exact JSON bytes are sufficient.
func (s Snapshot) Get(key string) (vibejson.Index, bool) {
	if s.state == nil {
		return vibejson.Index{}, false
	}
	hash := maphash.String(s.state.seed, key)
	if s.state.baseKeys != nil && s.state.keys == nil {
		loc, ok := s.state.baseKeys.lookup(hash, key)
		if !ok {
			return vibejson.Index{}, false
		}
		chunk := s.state.Chunks.Get(loc.Chunk)
		if chunk == nil || chunk.Live&(uint64(1)<<loc.Slot) == 0 {
			return vibejson.Index{}, false
		}
		return chunk.Docs.Doc(int(chunk.Ord[loc.Slot])), true
	}
	chunk, loc, ok := storeStateKeyLookupChunk(s.state, hash, key)
	if !ok {
		return vibejson.Index{}, false
	}
	return chunk.Docs.Doc(int(chunk.Ord[loc.Slot])), true
}

// Range visits live keys in stable chunk/slot order until fn returns false.
// Values borrow the Snapshot. Range itself allocates nothing.
func (s Snapshot) Range(fn func(key string, value vibejson.RawValue) bool) {
	if s.state == nil {
		return
	}
	s.state.Chunks.Each(func(_ uint32, chunk *Chunk) bool {
		for live := chunk.Live; live != 0; live &= live - 1 {
			slot := bits.TrailingZeros64(live)
			if !fn(chunk.Key(slot), vibejson.RawValue{Src: chunk.Docs.RawAt(int(chunk.Ord[slot]))}) {
				return false
			}
		}
		return true
	})
}

// GetRaw is the current-snapshot convenience form of Snapshot.GetRaw.
func (c *Collection) GetRaw(key string) (vibejson.RawValue, bool) {
	snap, _ := c.Snapshot()
	return snap.GetRaw(key)
}

// Get is the current-snapshot convenience form of Snapshot.Get.
func (c *Collection) Get(key string) (vibejson.Index, bool) {
	snap, _ := c.Snapshot()
	return snap.Get(key)
}
