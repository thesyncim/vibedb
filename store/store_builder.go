package store

import (
	"errors"
	"fmt"
	"hash/maphash"
	"math"
	"math/bits"

	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/x/byteview"
)

var (
	// ErrDuplicateKey reports that Builder.Append received a key it
	// already owns. Bulk construction requires unique keys so every document is
	// written exactly once into its final micro-page.
	ErrDuplicateKey = errors.New("vibejson: duplicate Builder key")
	// ErrBuilderClosed reports use after Build transferred the builder's
	// immutable graph into a collection.
	ErrBuilderClosed = errors.New("vibejson: Builder is closed")
)

// Builder constructs a keyed collection without publishing and path-copying
// persistent metadata for every input row. It is the bulk-load complement to
// Collection.Put: Append validates and copies one unique key/document into its final
// bounded micro-page, mutating only builder-owned key and chunk radix nodes;
// Build freezes that graph and publishes it once.
//
// A builder belongs to one goroutine. Append errors leave all previously
// appended rows intact and do not consume a key or slot. Build may be called
// once; the returned collection has ordinary snapshot, mutation, and index
// semantics. Builder is intentionally not an update API: online changes belong
// to Collection.Put.
//
// Use it for any load of more than a chunk's worth of rows. Put rebuilds its
// whole chunk per write — that is what makes a published chunk immutable and a
// delete tombstone-free — so a loop of Put over N rows does O(N*ChunkDocuments)
// copying and allocates roughly an order of magnitude more per row than Append
// does. Measured on a 100,000-row, 24 MiB corpus: Append plus Build allocated
// 821 B and 0.19 allocations per row, where the same rows through Put allocated
// about 7.8 kB and 14 allocations each.
//
// Build is where the load's memory peaks, and by a wide margin. Every appended
// page stages its source and structural tapes on the Go heap and holds them
// until compactDocuments copies the whole graph into one off-heap block, so the
// builder's Go-heap working set grows with the corpus and is at its largest at
// the moment that block is allocated — the same corpus measured 77 MiB of live
// Go heap and 119 MiB of MemStats.HeapSys against 3.9 MiB of steady-state
// HeapAlloc afterwards. Size a process for the load, not for the result, and
// see the package documentation on why HeapAlloc shows neither.
type Builder struct {
	options  Options
	seed     maphash.Seed
	keyTable storeBuilderKeyTable
	chunks   storeChunkVector
	current  *Chunk
	count    int
	keyBytes int
	closed   bool
	shapes   []*ShapeRecord
	shapeSet map[*ShapeRecord]struct{}
	// sourceHint is the exact JSON bytes in the preceding full chunk. It lets
	// the next unpublished chunk reserve one bounded arena instead of retaining
	// every geometric growth generation through already-built Index slices.
	sourceHint int
	// entryHint is the exact structural-entry count the preceding full chunk
	// committed, the tape-side counterpart of sourceHint. Without it a page
	// starts its entry arena at arenaMinEntries — a policy sized for the
	// one-document rebuild in prepareStoreSegment, not for a page that goes on
	// to take ChunkDocuments of them — and then reaches page size by geometric
	// growth. That growth is not amortized here the way it is in an ordinary
	// append-only arena: every superseded generation still backs the tapes of
	// the documents built into it, so each one stays live for the builder's
	// whole run instead of becoming collectable, and the page ends up retaining
	// roughly twice the entries it committed. Worse, each growth step is
	// reached through buildDoc's ErrIndexFull spill path, which buys a
	// len(src)+2 scratch tape — an order of magnitude more entries than the
	// document actually needs — once per page. Reserving the whole page's tape
	// up front removes both.
	entryHint       int
	currentDocBytes int
}

// NewBuilder returns an empty bulk builder. It validates Options up
// front so Append cannot discover a configuration error after consuming rows.
func NewBuilder(options Options) (*Builder, error) {
	normalized, err := options.Normalized()
	if err != nil {
		return nil, err
	}
	return &Builder{options: normalized, seed: maphash.MakeSeed()}, nil
}

// Len returns the number of documents successfully appended so far.
func (b *Builder) Len() int {
	if b == nil {
		return 0
	}
	return b.count
}

// Append validates and copies one unique keyed document. The caller may reuse
// key and src after return. A duplicate key, invalid JSON, closed builder, or
// exhausted chunk address space changes no committed row.
func (b *Builder) Append(key string, src []byte) error {
	if b == nil || b.closed {
		return ErrBuilderClosed
	}
	if uint64(len(key)) > uint64(^uint32(0)) || len(key) > math.MaxInt-b.keyBytes {
		return ErrCheckpointTooLarge
	}
	hash := maphash.String(b.seed, key)
	if b.keyTable.contains(b, hash, key) {
		return fmt.Errorf("%w %q", ErrDuplicateKey, key)
	}
	if uint64(b.count) >= storeBuilderKeyOrdinalMask {
		return ErrTooLarge
	}
	if b.current == nil {
		if b.chunks.Count == ^uint32(0) {
			return ErrTooLarge
		}
		capacity := storeBuilderSourceCapacity(b.options.ChunkDocuments, len(src), b.sourceHint)
		entries := storeBuilderEntryCapacity(capacity, b.sourceHint, b.entryHint)
		b.current = newStoreBuilderChunk(b.options, b.shapes, capacity, entries)
	}

	// Segment.Append owns and validates src before any key or directory state is
	// committed. Its rollback contract leaves the page unchanged on error.
	var (
		ord int
		err error
	)
	if b.options.Schema == nil {
		ord, err = b.current.Docs.Append(src)
	} else {
		ord, err = b.current.Docs.appendStoreSchema(
			src, b.options.Schema,
		)
	}
	if err != nil {
		return err
	}
	// Fold the summary before the slot counter moves: the count of documents
	// already in this page is exactly what a newly created zone entry needs to
	// deduce that none of them carried the path (see ZonePaths). Builder never
	// rebuilds a page, so this is the only place a bulk-built chunk is
	// summarized, and it costs one linear pass over source bytes already in
	// cache from Segment.Append.
	// Grow before publishing the new key into the table. Rehashing sees only
	// preceding rows, and every subsequent operation is bounded and infallible.
	b.keyTable.reserve(b, b.count+1)
	b.currentDocBytes += len(src)
	keyStart := len(b.current.keyBytes)
	b.current.keyBytes = append(b.current.keyBytes, key...)
	storedKey := byteview.String(b.current.keyBytes[keyStart:])
	b.keyBytes += len(key)
	slot := int(b.current.Count)
	b.current.keys[slot] = storedKey
	b.current.Ord[slot] = uint8(ord)
	b.current.Live |= uint64(1) << uint(slot)
	b.current.Count++
	b.keyTable.insert(hash, uint64(b.count))
	b.count++

	if int(b.current.Count) == b.options.ChunkDocuments {
		b.flush()
	}
	return nil
}

func newStoreBuilderChunk(options Options, shapes []*ShapeRecord, sourceCapacity, entryCapacity int) *Chunk {
	chunk := &Chunk{
		keys: make([]string, options.ChunkDocuments),
	}
	initChunkSegment(
		&chunk.Docs, options.stateOptions(), options.Postings,
	)
	if sourceCapacity > 0 {
		chunk.Docs.srcChunk = make([]byte, 0, sourceCapacity)
	}
	if entryCapacity > 0 {
		chunk.Docs.entryChunk = make([]vibejson.IndexEntry, 0, entryCapacity)
	}
	// A builder page takes exactly ChunkDocuments rows before flush hands it
	// over, so its row table has a known final length. Letting append discover
	// it geometrically instead costs both the superseded generations, which are
	// garbage the moment they are copied, and a final capacity rounded past the
	// row count by the allocator's size classes, which is retained until
	// compactDocuments drops the whole table.
	if options.ChunkDocuments > 0 {
		chunk.Docs.docs = make([]vibejson.Index, 0, options.ChunkDocuments)
	}
	for _, rec := range shapes {
		chunk.Docs.shapes.seedRecord(rec)
	}
	return chunk
}

// storeBuilderEntryCapacity converts a page's source reservation into a tape
// reservation using the preceding full page's measured entries-per-byte, so the
// two arenas are sized from the same evidence and the source reservation's own
// headroom carries over into the tape. Scaling rather than reusing the previous
// entry count directly is what keeps the reservation honest when
// storeBuilderSourceCapacity detects a size phase change and resamples: a page
// reserved for larger documents must reserve proportionally more tape or it
// spills on the row that crosses the old page's size.
//
// It returns zero — leaving buildDoc's geometric growth in place — whenever
// there is no evidence yet, which covers the first page and any corpus whose
// documents commit no structural entries at all because shape compaction
// replaced them. Overestimating is bounded to one arena and never wrong;
// underestimating only falls back to the growth path, so no caller has to treat
// this as anything but a hint.
func storeBuilderEntryCapacity(sourceCapacity, previousBytes, previousEntries int) int {
	if sourceCapacity <= 0 || previousBytes <= 0 || previousEntries <= 0 {
		return 0
	}
	// Deliberately 64-bit: sourceCapacity reaches segmentMaxSrcChunk (2^20) and
	// previousEntries is of the same order, so the product overflows a 32-bit
	// int on GOARCH=386 long before the quotient could. The ceiling below is
	// what brings the result back into int range on every platform.
	scaled := (uint64(sourceCapacity)*uint64(previousEntries) + uint64(previousBytes) - 1) /
		uint64(previousBytes)
	if scaled > uint64(segmentMaxEntryChunk) {
		return segmentMaxEntryChunk
	}
	return int(scaled)
}

func storeBuilderSourceCapacity(chunkDocuments, firstDocumentBytes, previousBytes int) int {
	if chunkDocuments <= 0 || firstDocumentBytes <= 0 {
		return 0
	}
	sample := segmentMaxSrcChunk
	if firstDocumentBytes <= segmentMaxSrcChunk/chunkDocuments {
		sample = firstDocumentBytes * chunkDocuments
	}
	if previousBytes <= 0 {
		return storeBuilderSourceHeadroom(sample, chunkDocuments)
	}
	previousBytes = min(previousBytes, segmentMaxSrcChunk)
	average := (previousBytes + chunkDocuments - 1) / chunkDocuments
	// Reuse the exact preceding-page size only while the new first row is a
	// plausible member of the same size distribution. A phase change switches
	// immediately to the current sample instead of pinning stale capacity.
	if firstDocumentBytes >= max(average/2, 1) && firstDocumentBytes <= average*2 {
		return storeBuilderSourceHeadroom(previousBytes, chunkDocuments)
	}
	return storeBuilderSourceHeadroom(sample, chunkDocuments)
}

func storeBuilderSourceHeadroom(size, chunkDocuments int) int {
	if size >= segmentMaxSrcChunk {
		return segmentMaxSrcChunk
	}
	if chunkDocuments <= 1 {
		return size
	}
	// One average row absorbs ordinary page-to-page variance without the
	// 2x retained cost of crossing an arena boundary by a few bytes.
	headroom := max(size/chunkDocuments, 256)
	if size > segmentMaxSrcChunk-headroom {
		return segmentMaxSrcChunk
	}
	return size + headroom
}

func (b *Builder) flush() {
	if b.options.ShapeTapes {
		compactStoreBuilderShapes(&b.current.Docs)
		if b.shapeSet == nil {
			b.shapeSet = make(map[*ShapeRecord]struct{})
		}
		for _, rec := range b.current.Docs.shapes.shapes {
			if _, exists := b.shapeSet[rec]; exists {
				continue
			}
			b.shapeSet[rec] = struct{}{}
			b.shapes = append(b.shapes, rec)
		}
	}
	// The page takes no further row: b.current is cleared below and every later
	// edit rebuilds it by copy, so its ingest-only working state is released
	// here. compactStoreBuilderShapes above already drops the entry arena on
	// the all-compact page; this covers the spill buffer and the value
	// dictionary's sighting gate on every page, compact remainder or not.
	b.current.Docs.sealIngest()
	b.chunks.appendTransient(b.current)
	if int(b.current.Count) == b.options.ChunkDocuments {
		b.sourceHint = b.currentDocBytes
		// Read after compactStoreBuilderShapes above, so a shape-compacted page
		// reports the entries it actually kept. A short page is not evidence:
		// its arena was sized for ChunkDocuments rows and only Build can end
		// one, so recording it would halve every following reservation.
		b.entryHint = b.current.Docs.committedEntries()
	}
	b.currentDocBytes = 0
	b.current = nil
}

// compactStoreBuilderShapes revisits the first sighting of every shape after
// its page-local repeat has compiled the immutable record. Ordinary Segment
// append cannot rewrite an already returned Index, but an unpublished builder
// owns every row and can safely drop those redundant classic key tapes before
// publication. Value postings and dictionaries remain exact because the
// document bytes do not change. Key-existence postings also encode the
// document's physical storage class, so a successful transition moves its
// ordinal from the classic remainder to the compiled shape.
func compactStoreBuilderShapes(docs *Segment) {
	if docs == nil || len(docs.shapes.shapes) == 0 || len(docs.tapeRefs) == 0 {
		return
	}
	allCompact := true
	for i := range docs.docs {
		if docs.ShapeTapeRefAt(i).Rec != nil {
			continue
		}
		index, ref, ok := copyStoreShapeTape(docs, docs.docs[i])
		if !ok {
			allCompact = false
			continue
		}
		docs.docs[i] = index
		docs.tapeRefs[i] = ref
		if docs.postings != nil {
			docs.postings.promoteShapeDoc(ref.Rec, int32(i))
		}
	}
	if allCompact {
		docs.entryChunk = nil
		docs.entryPool.drop()
		docs.scratch = nil
	}
}

// Build freezes the accumulated graph, compacts immutable keys and document
// source/tapes into pointer-free owned blocks, and transfers them to a new
// collection. Empty input produces an initialized empty collection. The builder closes
// even when it is empty, preventing accidental aliasing through later Append
// calls. Once key compaction succeeds, Build is terminal: any later index or
// document-compaction failure releases all unpublished external blocks and a
// subsequent call returns ErrBuilderClosed.
func (b *Builder) Build() (*Collection, error) {
	return b.build(storeBuilderBuildSteps{
		compactDocuments: (*Builder).compactDocuments,
	})
}

// storeBuilderBuildSteps keeps failure injection local to one unpublished
// builder. Tests can stop after a specific ownership transfer without a
// process-wide hook that could race an unrelated Build.
type storeBuilderBuildSteps struct {
	compactDocuments func(*Builder, *State) error
}

func (b *Builder) build(steps storeBuilderBuildSteps) (*Collection, error) {
	if b == nil || b.closed {
		return nil, ErrBuilderClosed
	}
	if b.current != nil {
		b.flush()
	}
	baseKeys, err := b.compactBaseKeys()
	if err != nil {
		return nil, err
	}
	b.closed = true

	state := &State{
		Count:        b.count,
		ChunkCount:   b.chunks.Count,
		seed:         b.seed,
		StateOptions: b.options.stateOptions(),
		baseKeys:     baseKeys,
		Chunks:       b.chunks,
	}
	if b.count != 0 {
		state.Generation = 1
	}
	collection := &Collection{Options: b.options, options: b.options}
	collection.free.pos = make(map[uint32]int)
	collection.postingChunks.pos = make(map[uint32]int)
	for id := uint32(0); id < b.chunks.Count; id++ {
		chunk := b.chunks.Get(id)
		if chunk != nil && int(chunk.Count) < b.options.ChunkDocuments {
			collection.free.add(id)
		}
		if chunk != nil && chunk.Docs.Postings {
			collection.postingChunks.add(id)
		}
	}
	if err := steps.compactDocuments(b, state); err != nil {
		releaseStoreBuilderBuild(state)
		return nil, err
	}
	collection.state.Store(state)
	b.keyTable = storeBuilderKeyTable{}
	b.chunks = storeChunkVector{}
	b.current = nil
	b.shapes = nil
	b.shapeSet = nil
	b.sourceHint = 0
	b.currentDocBytes = 0
	return collection, nil
}

// releaseStoreBuilderBuild closes every external block that can become owned
// after key compaction but before the new collection is published. A failed Build
// is terminal once compactBaseKeys has rewritten the chunks, so no subsequent
// builder operation can observe these released unpublished representations.
func releaseStoreBuilderBuild(state *State) {
	if state == nil {
		return
	}
	if state.mappedDocs != nil {
		state.mappedDocs.release()
		state.mappedDocs = nil
	}
	if state.baseKeys != nil {
		state.baseKeys.release()
		state.baseKeys = nil
	}
}

// compactBaseKeys replaces the builder-only HAMT and its leaf objects with one
// immutable Swiss-style table plus packed key bytes. On common Unix systems
// both regions are outside the Go heap. The published collection therefore retains
// neither a string allocation nor a directory leaf per input row.
func (b *Builder) compactBaseKeys() (*storeMappedKeys, error) {
	if b.count == 0 {
		return nil, nil
	}
	base, err := newStoreOwnedKeys(b.count, b.keyBytes, b.chunks.Count >= storeMappedLocationMaxChunk, b.options.ChunkDocuments)
	if err != nil {
		return nil, fmt.Errorf("vibejson: compact Builder keys: %w", err)
	}
	position := 0
	refBase := uint64(0)
	valid := true
	b.chunks.Each(func(id uint32, chunk *Chunk) bool {
		for live := chunk.Live; live != 0; live &= live - 1 {
			slot := bits.TrailingZeros64(live)
			key := chunk.keys[slot]
			start := position
			position += copy(base.source[position:], key)
			ref := refBase + uint64(chunk.Ord[slot])
			if ref >= uint64(base.keyRefCount()) {
				valid = false
				return false
			}
			base.setKeySpan(ref, uint64(start), uint32(len(key)))
			base.setLocation(ref, Location{Chunk: id, Slot: uint8(slot)})
			if !base.insert(maphash.String(b.seed, key), ref) {
				valid = false
				return false
			}
		}
		refBase += uint64(chunk.Count)
		return true
	})
	if !valid || position != len(base.source) || refBase != uint64(b.count) {
		base.release()
		return nil, errors.New("vibejson: Builder compact key invariant")
	}
	refBase = 0
	b.chunks.Each(func(_ uint32, chunk *Chunk) bool {
		chunk.keys = nil
		chunk.keyBytes = nil
		chunk.mappedKeys = base
		chunk.mappedBase = refBase
		refBase += uint64(chunk.Count)
		return true
	})
	return base, nil
}

const (
	// A collection can address fewer than 2^38 rows: 2^32 chunk ids times at most
	// 64 rows. Forty ordinal bits therefore leave a 24-bit hash fingerprint in
	// one pointer-free word without narrowing the public address space.
	storeBuilderKeyOrdinalBits = 40
	storeBuilderKeyOrdinalMask = uint64(1)<<storeBuilderKeyOrdinalBits - 1
	storeBuilderKeyMinSlots    = 16
)

// storeBuilderKeyTable is the unpublished builder's duplicate-key guard.
//
// Each occupied slot packs a 24-bit hash fingerprint and row ordinal+1 into
// one uint64. The ordinal resolves the exact key from builder-owned chunk
// bytes, so even a full hash collision is compared byte-for-byte. Unlike the
// online persistent HAMT, this append-only table needs no node or leaf per key,
// contains no pointers for the garbage collector to scan, and grows only
// geometrically. Build discards it after publishing the compact mapped key
// directory.
type storeBuilderKeyTable struct {
	slots []uint64
}

func (t *storeBuilderKeyTable) contains(b *Builder, hash uint64, key string) bool {
	if len(t.slots) == 0 {
		return false
	}
	fingerprint := hash >> storeBuilderKeyOrdinalBits
	mask := uint64(len(t.slots) - 1)
	for slot := hash & mask; ; slot = (slot + 1) & mask {
		packed := t.slots[slot]
		if packed == 0 {
			return false
		}
		if packed>>storeBuilderKeyOrdinalBits != fingerprint {
			continue
		}
		stored, ok := b.keyAt((packed & storeBuilderKeyOrdinalMask) - 1)
		if ok && stored == key {
			return true
		}
	}
}

func (t *storeBuilderKeyTable) reserve(b *Builder, entries int) {
	capacity := len(t.slots)
	if capacity != 0 && entries <= capacity-capacity/8 {
		return
	}
	if capacity == 0 {
		capacity = storeBuilderKeyMinSlots
	}
	for entries > capacity-capacity/8 {
		capacity *= 2
	}
	previous := t.slots
	t.slots = make([]uint64, capacity)
	for _, packed := range previous {
		if packed == 0 {
			continue
		}
		row := (packed & storeBuilderKeyOrdinalMask) - 1
		key, ok := b.keyAt(row)
		if !ok {
			panic("vibejson: Builder key table ordinal invariant")
		}
		t.insert(maphash.String(b.seed, key), row)
	}
}

func (t *storeBuilderKeyTable) insert(hash, row uint64) {
	packed := hash&^storeBuilderKeyOrdinalMask | (row + 1)
	mask := uint64(len(t.slots) - 1)
	for slot := hash & mask; ; slot = (slot + 1) & mask {
		if t.slots[slot] == 0 {
			t.slots[slot] = packed
			return
		}
	}
}

func (b *Builder) keyAt(row uint64) (string, bool) {
	chunkDocuments := uint64(b.options.ChunkDocuments)
	chunkID := row / chunkDocuments
	if chunkID > uint64(^uint32(0)) {
		return "", false
	}
	var chunk *Chunk
	if uint32(chunkID) < b.chunks.Count {
		chunk = b.chunks.Get(uint32(chunkID))
	} else if uint32(chunkID) == b.chunks.Count {
		chunk = b.current
	}
	slot := int(row % chunkDocuments)
	if chunk == nil || slot >= int(chunk.Count) {
		return "", false
	}
	return chunk.keys[slot], true
}
