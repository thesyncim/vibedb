package store

import (
	"errors"
	"fmt"
	"hash/maphash"
	"math/bits"
	"slices"
	"strings"

	"github.com/thesyncim/vibedb/internal/storekey"
)

// Multi-collection write (Memory / heap profile).
//
// A single [Collection.Put] or [Collection.Delete] publishes through one atomic
// state pointer under that collection's writer lock. Applying the same idea to
// several collections one after another does not compose into one database
// transaction: a [Database.Snapshot] that interleaves the stores can observe a
// partial member set. UpdateCollections closes that window by staging every
// participant's entry set first, then holding every participant writer in the
// catalog's global name order while flipping all published-state pointers
// inside the hold — the write dual of [Database.Snapshot]'s lock protocol.
//
// There is no durability dimension. Bounded admission mirrors the durable
// engine's zero-value batch defaults (per-participant document and byte
// ceilings, and a hard collection-count cap).

var (
	// ErrBatchTooLarge reports a per-collection staged set that exceeds the
	// heap transaction admission bound. Nothing was published.
	ErrBatchTooLarge = errors.New("vibedb: collection write batch exceeds configured bound")
	// ErrBatchClosed reports use of a WriteBatch after the UpdateCollections
	// that owns it returned.
	ErrBatchClosed = errors.New("vibedb: collection write batch is no longer active")
	// ErrTxnTooLarge reports a multi-collection apply that exceeds a
	// cross-participant bound (collection count). Nothing was staged.
	ErrTxnTooLarge = errors.New("vibedb: database transaction exceeds a bounded limit")
	// ErrTxnParticipant reports a nil participant, an unnamed collection, a
	// duplicate name, or a DatabaseBatch.Collection name outside the
	// participant set.
	ErrTxnParticipant = errors.New("vibedb: invalid database transaction participant")
)

const (
	// defaultHeapMaxBatchDocuments mirrors durable's zero-value
	// MaxBatchDocuments (store.MaxChunkDocuments).
	defaultHeapMaxBatchDocuments = MaxChunkDocuments
	// defaultHeapMaxKeyBytes mirrors the durable / facade zero-value key bound
	// used only to size the default batch-byte budget.
	defaultHeapMaxKeyBytes = 256
	// defaultHeapBatchValueBytes mirrors durable's defaultBatchValueBytes.
	defaultHeapBatchValueBytes = 16 << 20
	// defaultHeapMaxBatchBytes mirrors durable's zero-value MaxBatchBytes:
	// one maximum-size key per batch document plus the default value budget.
	defaultHeapMaxBatchBytes = defaultHeapMaxBatchDocuments*defaultHeapMaxKeyBytes +
		defaultHeapBatchValueBytes
	// defaultHeapMaxTxnCollections mirrors durable.TxnLimits' default
	// MaxCollections.
	defaultHeapMaxTxnCollections = 16
	// defaultHeapBatchPositionHint sizes the per-batch key-position index.
	// The batch bound (maxDocuments) is enforced independently, so the hint
	// only trades one regrowth on large batches against a max-sized map on
	// every small commit.
	defaultHeapBatchPositionHint = 8
)

// WriteBatch accumulates the mutations one participant contributes to an
// UpdateCollections apply. Keys are deduplicated as they arrive; keys and
// documents are copied into the batch so the caller may reuse its buffers as
// soon as a method returns. Document syntax and schema are validated when the
// batch is applied under the writer lock, matching [Collection.Put].
type WriteBatch struct {
	collection   *Collection
	entries      []writeBatchEntry
	position     map[string]int
	keys         []byte
	values       []byte
	active       bool
	maxDocuments int
	maxBytes     int
}

type writeBatchEntry struct {
	keyOffset, keyLength     int
	valueOffset, valueLength int
	remove                   bool
}

func (b *WriteBatch) key(entry writeBatchEntry) string {
	return string(b.keys[entry.keyOffset : entry.keyOffset+entry.keyLength])
}

func (b *WriteBatch) keyBytes(entry writeBatchEntry) []byte {
	return b.keys[entry.keyOffset : entry.keyOffset+entry.keyLength]
}

func (b *WriteBatch) value(entry writeBatchEntry) []byte {
	return b.values[entry.valueOffset : entry.valueOffset+entry.valueLength]
}

func (b *WriteBatch) reset() {
	b.entries = b.entries[:0]
	b.keys = b.keys[:0]
	b.values = b.values[:0]
	clear(b.position)
}

// Len reports how many distinct keys the batch will mutate.
func (b *WriteBatch) Len() int {
	if b == nil {
		return 0
	}
	return len(b.entries)
}

// Put records key with src. It reports ErrBatchTooLarge or ErrBatchClosed
// immediately; malformed JSON and schema violations are reported when the
// owning UpdateCollections applies the batch.
func (b *WriteBatch) Put(key string, src []byte) error {
	if b == nil || !b.active {
		return ErrBatchClosed
	}
	return b.record(key, src, false)
}

// Delete records the removal of key. Removing a key the collection does not
// hold is not an error and publishes nothing for it.
func (b *WriteBatch) Delete(key string) error {
	if b == nil || !b.active {
		return ErrBatchClosed
	}
	return b.record(key, nil, true)
}

func (b *WriteBatch) record(key string, src []byte, remove bool) error {
	if at, exists := b.position[key]; exists {
		old := b.entries[at]
		nextBytes := len(b.keys) + len(b.values) - old.valueLength
		if len(src) > b.maxBytes-nextBytes {
			return ErrBatchTooLarge
		}
		b.replaceValue(at, src)
		b.entries[at].remove = remove
		return nil
	}
	if len(b.entries) >= b.maxDocuments {
		return ErrBatchTooLarge
	}
	nextBytes := len(b.keys) + len(key) + len(b.values)
	if len(src) > b.maxBytes-nextBytes {
		return ErrBatchTooLarge
	}
	entry := writeBatchEntry{
		keyOffset: len(b.keys), keyLength: len(key),
		valueOffset: len(b.values), valueLength: len(src), remove: remove,
	}
	b.keys = append(b.keys, key...)
	b.values = append(b.values, src...)
	b.entries = append(b.entries, entry)
	b.position[string(b.keyBytes(entry))] = len(b.entries) - 1
	return nil
}

func (b *WriteBatch) replaceValue(at int, src []byte) {
	entry := &b.entries[at]
	start := entry.valueOffset
	oldEnd := start + entry.valueLength
	delta := len(src) - entry.valueLength
	if delta > 0 {
		oldLength := len(b.values)
		b.values = append(b.values, make([]byte, delta)...)
		copy(b.values[oldEnd+delta:], b.values[oldEnd:oldLength])
	} else if delta < 0 {
		copy(b.values[start+len(src):], b.values[oldEnd:])
		clear(b.values[len(b.values)+delta:])
		b.values = b.values[:len(b.values)+delta]
	}
	copy(b.values[start:start+len(src)], src)
	for i := range b.entries {
		if i != at && b.entries[i].valueOffset >= oldEnd {
			b.entries[i].valueOffset += delta
		}
	}
	entry.valueLength = len(src)
}

// DatabaseBatch is the per-apply staging handle passed to UpdateCollections.
// Collection returns the WriteBatch for a participant name.
type DatabaseBatch struct {
	byName map[string]*WriteBatch
}

// Collection returns the participant WriteBatch for name.
func (b *DatabaseBatch) Collection(name string) (*WriteBatch, error) {
	if b == nil {
		return nil, ErrTxnParticipant
	}
	batch, ok := b.byName[name]
	if !ok || batch == nil {
		return nil, fmt.Errorf("%w: %q", ErrTxnParticipant, name)
	}
	return batch, nil
}

// UpdateCollections stages per-collection entry sets via fn, then holds every
// participant writer in catalog name order and flips all published-state
// pointers inside that hold. A concurrent [Database.Snapshot] cut therefore
// observes each transaction all-or-nothing across the participant set.
// Single-collection [Collection.Snapshot] readers may observe an individual
// collection before or after its flip — the same promise [Collection.Put]
// always made.
//
// participants must be non-nil, catalog-named collections with distinct names.
// An empty participant list runs fn against an empty batch set and publishes
// nothing. fn nil is refused. Exceeding the per-participant batch bound or the
// participant-count cap refuses before any writer is taken.
func UpdateCollections(participants []*Collection, fn func(*DatabaseBatch) error) error {
	if fn == nil {
		return errors.New("vibedb: UpdateCollections requires a function")
	}
	ordered, err := orderTxnParticipants(participants)
	if err != nil {
		return err
	}
	if len(ordered) > defaultHeapMaxTxnCollections {
		return fmt.Errorf(
			"%w: more than %d collections",
			ErrTxnTooLarge, defaultHeapMaxTxnCollections,
		)
	}

	batch := &DatabaseBatch{byName: make(map[string]*WriteBatch, len(ordered))}
	batches := make([]*WriteBatch, len(ordered))
	for i, collection := range ordered {
		wb := &WriteBatch{
			collection: collection,
			// Size the position index for the common small batch and let it
			// grow: a max-sized index on every commit costs ~2KB per
			// single-key transaction (a top allocator in commit profiles)
			// while regrowth on genuinely large batches amortizes.
			position:     make(map[string]int, defaultHeapBatchPositionHint),
			active:       true,
			maxDocuments: defaultHeapMaxBatchDocuments,
			maxBytes:     defaultHeapMaxBatchBytes,
		}
		batches[i] = wb
		batch.byName[collection.name] = wb
	}
	defer closeHeapWriteBatches(batches)

	if err := fn(batch); err != nil {
		return err
	}

	dirty := false
	for _, wb := range batches {
		if wb.Len() > 0 {
			dirty = true
			break
		}
	}
	if !dirty {
		return nil
	}

	for _, collection := range ordered {
		collection.mu.Lock()
	}
	defer func() {
		for i := len(ordered) - 1; i >= 0; i-- {
			ordered[i].mu.Unlock()
		}
	}()

	planned := make([]heapTxnPublish, 0, len(batches))
	for _, wb := range batches {
		if wb.Len() == 0 {
			continue
		}
		pub, err := wb.collection.planWriteBatchLocked(wb)
		if err != nil {
			return err
		}
		if pub.next == nil {
			continue
		}
		planned = append(planned, pub)
	}
	// Publication is infallible once every participant has a planned next
	// state: side effects and pointer flips run under the held writers.
	for i := range planned {
		planned[i].collection.publishWriteBatchLocked(planned[i])
	}
	return nil
}

// Update is the catalog-owned convenience form of UpdateCollections. It copies
// the currently cataloged collection set under the catalog read lock, releases
// that lock before calling fn, and uses the captured set as the participants in
// the same staging and publication protocol.
func (d *Database) Update(fn func(*DatabaseBatch) error) error {
	if d == nil {
		return ErrTxnParticipant
	}
	if fn == nil {
		return errors.New("vibedb: Database.Update requires a function")
	}
	d.mu.RLock()
	participants := make([]*Collection, 0, len(d.collections))
	for _, collection := range d.collections {
		participants = append(participants, collection)
	}
	d.mu.RUnlock()
	if len(participants) == 0 {
		batch := &DatabaseBatch{byName: map[string]*WriteBatch{}}
		return fn(batch)
	}
	return UpdateCollections(participants, fn)
}

func orderTxnParticipants(participants []*Collection) ([]*Collection, error) {
	if len(participants) == 0 {
		return nil, nil
	}
	ordered := make([]*Collection, len(participants))
	copy(ordered, participants)
	slices.SortFunc(ordered, func(a, b *Collection) int {
		if a == nil && b == nil {
			return 0
		}
		if a == nil {
			return -1
		}
		if b == nil {
			return 1
		}
		return strings.Compare(a.name, b.name)
	})
	seen := make(map[string]struct{}, len(ordered))
	for _, collection := range ordered {
		if collection == nil {
			return nil, fmt.Errorf("%w: nil collection", ErrTxnParticipant)
		}
		if collection.name == "" {
			return nil, fmt.Errorf("%w: unnamed collection", ErrTxnParticipant)
		}
		if _, dup := seen[collection.name]; dup {
			return nil, fmt.Errorf("%w: duplicate name %q", ErrTxnParticipant, collection.name)
		}
		seen[collection.name] = struct{}{}
	}
	return ordered, nil
}

func closeHeapWriteBatches(batches []*WriteBatch) {
	for _, batch := range batches {
		if batch == nil {
			continue
		}
		batch.active = false
		batch.reset()
	}
}

type heapChunkEdit struct {
	id      uint32
	old     *Chunk
	next    *Chunk
	changed uint64
}

type heapTxnPublish struct {
	collection *Collection
	next       *State
	edits      []heapChunkEdit
	free       storeIDSet
}

func (c *Collection) planWriteBatchLocked(batch *WriteBatch) (heapTxnPublish, error) {
	state, err := c.initLocked()
	if err != nil {
		return heapTxnPublish{}, err
	}
	working := *state
	free := cloneStoreIDSet(c.free)
	edits := make([]heapChunkEdit, 0, len(batch.entries))
	for _, entry := range batch.entries {
		key := batch.key(entry)
		if entry.remove {
			edit, ok, err := planDeleteEntry(c, &working, &free, key)
			if err != nil {
				return heapTxnPublish{}, err
			}
			if ok {
				edits = append(edits, edit)
			}
			continue
		}
		edit, err := planPutEntry(c, &working, &free, key, batch.value(entry))
		if err != nil {
			return heapTxnPublish{}, err
		}
		edits = append(edits, edit)
	}
	if len(edits) == 0 {
		// Every entry was a delete-miss; publish nothing for this participant.
		return heapTxnPublish{}, nil
	}
	working.Generation = state.Generation + 1
	return heapTxnPublish{
		collection: c,
		next:       &working,
		edits:      edits,
		free:       free,
	}, nil
}

func (c *Collection) publishWriteBatchLocked(pub heapTxnPublish) {
	if pub.next == nil {
		return
	}
	c.free = pub.free
	catalogChanged, secondaryChanged := false, false
	for _, edit := range pub.edits {
		c.noteChunkPostingsLocked(edit.id, edit.old, edit.next)
		cat, sec := c.noteIndexesForChunkLocked(edit.id, edit.old, edit.next, edit.changed)
		catalogChanged = catalogChanged || cat
		secondaryChanged = secondaryChanged || sec
	}
	if catalogChanged {
		pub.next.Indexes = c.indexInfosLocked()
	}
	if secondaryChanged {
		pub.next.secondary = c.indexSnapshotsLocked()
	}
	c.state.Store(pub.next)
}

func planPutEntry(
	c *Collection,
	state *State,
	free *storeIDSet,
	key string,
	src []byte,
) (heapChunkEdit, error) {
	hash := maphash.String(state.seed, key)
	old, loc, found := storeStateKeyLookupChunk(state, hash, key)
	if found {
		storedKey := old.Key(int(loc.Slot))
		var chunk *Chunk
		var err error
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
			return heapChunkEdit{}, err
		}
		state.Chunks = state.Chunks.set(loc.Chunk, chunk)
		return heapChunkEdit{
			id: loc.Chunk, old: old, next: chunk,
			changed: uint64(1) << loc.Slot,
		}, nil
	}

	if len(free.ids) == 0 && state.Chunks.Count == ^uint32(0) {
		return heapChunkEdit{}, ErrTooLarge
	}
	chunkID, slot, old := allocateSlotFromFree(free, state)
	var chunk *Chunk
	var err error
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
		return heapChunkEdit{}, err
	}
	key = strings.Clone(key)
	chunk.keys[slot] = key
	state.Count++
	loc = Location{Chunk: chunkID, Slot: uint8(slot)}
	state.keys = storekey.Insert(state.keys, hash, key, loc)
	if chunkID == state.Chunks.Count {
		state.Chunks, _ = state.Chunks.append(chunk)
	} else {
		state.Chunks = state.Chunks.set(chunkID, chunk)
	}
	if old == nil {
		state.ChunkCount++
	}
	if int(chunk.Count) == state.StateOptions.ChunkDocuments {
		free.remove(chunkID)
	} else {
		free.add(chunkID)
	}
	return heapChunkEdit{
		id: chunkID, old: old, next: chunk,
		changed: uint64(1) << uint(slot),
	}, nil
}

func planDeleteEntry(
	c *Collection,
	state *State,
	free *storeIDSet,
	key string,
) (heapChunkEdit, bool, error) {
	hash := maphash.String(state.seed, key)
	old, loc, found := storeStateKeyLookupChunk(state, hash, key)
	if !found {
		return heapChunkEdit{}, false, nil
	}
	nextChunk, err := rebuildStoreChunk(
		state.StateOptions, c.options.Postings, old,
		int(loc.Slot), "", nil, false,
	)
	if err != nil {
		return heapChunkEdit{}, false, err
	}
	state.Count--
	state.keys = storekey.Delete(state.keys, hash, key)
	state.Chunks = state.Chunks.set(loc.Chunk, nextChunk)
	if nextChunk == nil {
		state.ChunkCount--
	}
	free.add(loc.Chunk)
	return heapChunkEdit{
		id: loc.Chunk, old: old, next: nextChunk,
		changed: uint64(1) << loc.Slot,
	}, true, nil
}

func cloneStoreIDSet(s storeIDSet) storeIDSet {
	if len(s.ids) == 0 {
		return storeIDSet{}
	}
	out := storeIDSet{
		ids: append([]uint32(nil), s.ids...),
		pos: make(map[uint32]int, len(s.pos)),
	}
	for id, at := range s.pos {
		out.pos[id] = at
	}
	return out
}

func allocateSlotFromFree(free *storeIDSet, state *State) (uint32, int, *Chunk) {
	if len(free.ids) == 0 {
		return state.Chunks.Count, 0, nil
	}
	id := free.ids[len(free.ids)-1]
	chunk := state.Chunks.Get(id)
	if chunk == nil {
		return id, 0, nil
	}
	limitMask := ^uint64(0)
	if state.StateOptions.ChunkDocuments < 64 {
		limitMask = uint64(1)<<uint(state.StateOptions.ChunkDocuments) - 1
	}
	freeMask := ^chunk.Live & limitMask
	if freeMask == 0 {
		panic("vibedb: full collection chunk in free set")
	}
	return id, bits.TrailingZeros64(freeMask), chunk
}
