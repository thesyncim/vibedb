package durable

import (
	"errors"
	"math/bits"
	"slices"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
	vibejson "github.com/thesyncim/vibejson"
)

// ErrBatchTooLarge reports a batch whose mutations do not fit the reservation
// Options.MaxBatchDocuments sized. Nothing was published; the caller may retry
// with fewer mutations.
var ErrBatchTooLarge = errors.New("vibejson: collection write batch exceeds configured bound")

// ErrBatchClosed reports use of a WriteBatch after the Update that owns it
// returned. Batches are pooled per collection, so a retained one would write
// into a later caller's mutations.
var ErrBatchClosed = errors.New("vibejson: collection write batch is no longer active")

// WriteBatch accumulates the mutations one Update publishes as a single
// generation.
//
// Keys are deduplicated as they arrive: mutating the same key twice keeps only
// the second mutation, so the published generation contains exactly one row
// state per key. Keys and documents are copied into the batch, so the caller
// may reuse its buffers as soon as a method returns.
//
// Deduplication is what makes the batch expressible at all — two rows for one
// key inside a single chunk rebuild would corrupt the page — but it is also
// visible in one place. Deleting a key and then putting it back inside one
// batch keeps the row at its existing {chunk, slot}, where the same pair of
// single-document calls would free the slot and append a new one. Both publish
// the same documents under the same keys; only the coordinates differ, and no
// coordinate is part of the collection's contract.
//
// Document syntax is validated when Update applies the batch, not when Put
// records it. Validation needs the same parse the commit needs, and doing it
// twice would double the only per-document CPU cost a batched write has left.
type WriteBatch struct {
	collection *Collection
	entries    []writeBatchEntry
	position   map[string]int
	keys       []byte
	values     []byte
	active     bool
}

type writeBatchEntry struct {
	keyOffset, keyLength     int
	valueOffset, valueLength int
	remove                   bool
}

func (b *WriteBatch) key(entry writeBatchEntry) []byte {
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

// Put records key with src. It reports ErrKeyTooLarge, ErrDocumentTooLarge, or
// ErrBatchTooLarge immediately; malformed JSON is reported by Update.
func (b *WriteBatch) Put(key string, src []byte) error {
	if b == nil || !b.active {
		return ErrBatchClosed
	}
	if len(key) > b.collection.options.MaxKeyBytes {
		return ErrKeyTooLarge
	}
	if len(src) > b.collection.options.MaxDocumentBytes {
		return ErrDocumentTooLarge
	}
	return b.record(key, src, false)
}

// Delete records the removal of key. Removing a key the collection does not
// hold is not an error and publishes nothing for it.
func (b *WriteBatch) Delete(key string) error {
	if b == nil || !b.active {
		return ErrBatchClosed
	}
	if len(key) > b.collection.options.MaxKeyBytes {
		return ErrKeyTooLarge
	}
	return b.record(key, nil, true)
}

func (b *WriteBatch) record(key string, src []byte, remove bool) error {
	if at, exists := b.position[key]; exists {
		old := b.entries[at]
		nextBytes := len(b.keys) + len(b.values) - old.valueLength
		if len(src) > b.collection.options.MaxBatchBytes-nextBytes {
			return ErrBatchTooLarge
		}
		b.replaceValue(at, src)
		b.entries[at].remove = remove
		return nil
	}
	if len(b.entries) >= b.collection.options.MaxBatchDocuments {
		return ErrBatchTooLarge
	}
	nextBytes := len(b.keys) + len(key) + len(b.values)
	if len(src) > b.collection.options.MaxBatchBytes-nextBytes {
		return ErrBatchTooLarge
	}
	entry := writeBatchEntry{
		keyOffset: len(b.keys), keyLength: len(key),
		valueOffset: len(b.values), valueLength: len(src), remove: remove,
	}
	b.keys = append(b.keys, key...)
	b.values = append(b.values, src...)
	b.entries = append(b.entries, entry)
	// The map key is the arena's copy, so it neither retains the caller's
	// string nor allocates a second one.
	b.position[string(b.key(entry))] = len(b.entries) - 1
	return nil
}

// replaceValue compacts a superseded value in place and repairs later offsets.
// Callback history therefore cannot grow the arena beyond MaxBatchBytes even
// when one key alternates between Put and Delete indefinitely.
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

// Update applies every mutation fn records as one failure-atomic generation:
// one document-page rebuild per touched chunk, one batched descent per
// directory, one publication root, and one durability fence.
//
// The batch either publishes whole or publishes nothing. An error returned by
// fn, or by any mutation the batch stages, aborts the transaction and leaves
// the collection exactly as it was.
func (c *Collection) Update(fn func(*WriteBatch) error) (err error) {
	if c == nil {
		return ErrClosed
	}
	if fn == nil {
		return errors.New("vibejson: collection Update requires a function")
	}
	if c.primaryGraphReadOnly() {
		return ErrPrimaryReadOnly
	}
	c.writer.Lock()
	var generation uint64
	defer func() {
		wait := generation != 0 && c.synchronous()
		if wait {
			c.durabilityWait.Add(1)
		}
		c.writer.Unlock()
		if wait {
			err = errors.Join(err, c.waitPublished(generation))
			c.durabilityWait.Done()
		}
	}()
	if c.closed {
		return ErrClosed
	}
	if failure := c.PersistenceError(); failure != nil {
		return failure
	}
	batch := c.fileWriteBatch()
	defer c.releaseFileWriteBatch(batch)
	if err := fn(batch); err != nil {
		return err
	}
	if len(batch.entries) == 0 {
		return nil
	}
	state := c.state.Load()
	if state == nil {
		return ErrClosed
	}
	if err := c.ensureDirtyCapacity(); err != nil {
		return err
	}
	published, err := c.applyFileBatch(state, batch)
	if err != nil {
		return err
	}
	// A batch whose every mutation resolved to nothing — deletes of keys the
	// collection does not hold — publishes no generation, and a synchronous
	// caller must not then be made to wait for one. Waiting on a generation the
	// committer was never handed reports a generation-order failure for what was
	// a perfectly successful no-op.
	if published {
		generation = state.root.Generation + 1
	}
	return nil
}

// fileBatchMutation is one resolved key: where its row lives, whether the batch
// creates or removes it, and the document bytes that replace it.
type fileBatchMutation struct {
	key         []byte
	value       []byte
	location    storeio.KeyLocation
	fingerprint storeio.PageKeyLocation
	created     bool
	remove      bool
}

// fileBatchPlacement is writer-only occupancy evidence for a chunk whose live
// word this batch changes before materialization. It stays bounded by the batch
// document limit: full chunks skipped while searching are not retained.
type fileBatchPlacement struct {
	chunk uint32
	live  uint64
}

// fileIndexBatchEdit is one posting-mask change a batch owes an index. The
// certificate lives in the collection's arena because a batch holds many at
// once and every one of them outlives the parsed document it came from.
type fileIndexBatchEdit struct {
	tupleHash  uint64
	certOffset int
	certLength int
	indexID    uint32
	chunk      uint32
	slot       uint8
	present    bool
}

func (c *Collection) applyFileBatch(state *fileStoreState, batch *WriteBatch) (bool, error) {
	generation := state.root.Generation + 1
	if generation == 0 {
		return false, storeio.ErrGenerationOrder
	}
	highWater, freeChunkHint, err := c.resolveFileBatch(state, batch)
	if err != nil {
		return false, err
	}
	if len(c.batchMutations) == 0 {
		return false, nil
	}
	if err := c.refreshReusable(state); err != nil {
		return false, err
	}
	tx, err := c.beginWriteTransaction(c.options.maxTransactionPages, storeio.WriteTransactionOptions{
		StoreID: c.storeID, Generation: generation, PageSize: uint32(c.options.PageSize),
		FileEnd: state.super.FileEnd, NextLogicalID: state.root.NextLogicalID,
		Reusable: c.reusable, ReuseJournal: c.reuseJournal,
		ReusableIndex:    &c.freeExtentIndex,
		ReusablePromoter: c.reusableExtentPromoter(),
	})
	if err != nil {
		return false, err
	}
	abort := true
	retirementReserved := false
	defer func() {
		if abort {
			if retirementReserved {
				_ = c.reclaimer.CancelRetiredGeneration(state.root.Generation)
			}
			_ = tx.Abort()
		}
	}()
	c.retireScratch = c.retireScratch[:0]
	c.retireRefScratch = c.retireRefScratch[:0]
	c.batchChunkTreeEdits = c.batchChunkTreeEdits[:0]
	c.batchPageKeyEdits = c.batchPageKeyEdits[:0]
	c.batchIndexEdits = c.batchIndexEdits[:0]
	c.batchCertArena = c.batchCertArena[:0]
	c.batchRetired = c.batchRetired[:0]

	result, err := c.applyFileBatchChunks(tx, state)
	if err != nil {
		return false, err
	}
	keyRoot, err := c.applyFileBatchKeys(tx, state, highWater)
	if err != nil {
		return false, err
	}
	indexRoot, err := c.applyFileBatchIndexes(tx, state)
	if err != nil {
		return false, err
	}
	// A collection built by CreateFrom carries two read accelerators that this
	// path cannot maintain incrementally: the compact index-group catalog and
	// the dense float64 scan projection. Both are strictly redundant — exact
	// postings and the chunk-tree scan remain authoritative and this batch keeps
	// them current — so releasing them costs query speed, never an answer. The
	// single-document path already releases the projection on the first
	// appending Put after a bulk build, for the same reason: neither structure
	// has an incremental form that can absorb new chunks.
	retireIndexGroup := state.root.IndexGroupHead != (storeio.PageRef{})
	retireFloat64Scan := state.root.Float64ScanHead != (storeio.PageRef{})
	// Listing the retirements has to precede syncFreeLog, because syncFreeLog is
	// what writes them down: it reads c.retireScratch to mark the owning segments
	// dirty and to emit a FreeOpSet delta per extent. Listing them afterwards left
	// the old state root — and, on the first batch after a bulk build, the entire
	// index-group catalog and float64 projection — recorded nowhere but the
	// reclaimer's memory, so they reached disk only once the generation fence
	// lifted and were abandoned outright by a Close or a crash before it did. That
	// is the exact loss "a retirement is durable in the commit that makes it" was
	// introduced to stop, and the single-document path has always ordered these
	// two calls this way.
	if err := c.collectFileBatchRetirements(state, retireFloat64Scan, retireIndexGroup); err != nil {
		return false, err
	}
	freeLog, err := c.syncFreeLog(tx, state)
	if err != nil {
		return false, err
	}
	nextState, nextInline, err := c.stageFileState(
		tx, state, generation, highWater, freeChunkHint,
		result.documentCount,
		result.liveChunks, result.chunkRoot, keyRoot, indexRoot,
		storeio.PageRef{}, storeio.PageRef{}, freeLog.head, freeLog.checksum,
		freeLog.inline,
	)
	if err != nil {
		return false, err
	}
	if err := c.reserveFileBatchRetirements(); err != nil {
		return false, err
	}
	retirementReserved = true
	if err := c.publishStagedFileMutation(
		tx, nextState, nextInline, freeLog,
	); err != nil {
		return false, err
	}
	abort = false
	c.appendChunk = result.appendChunk
	c.appendLive = result.appendLive
	if c.appendLive == fileStoreLiveMask(state.root.ChunkDocuments) {
		c.appendChunk = highWater
		c.appendLive = 0
	}
	return true, nil
}

// resolveFileBatch turns recorded mutations into placed rows. Every key is
// resolved against the published key directory before anything is staged, so
// slot assignment and the create/replace decision are settled while the batch
// can still be rejected without writing a page.
func (c *Collection) resolveFileBatch(
	state *fileStoreState, batch *WriteBatch,
) (uint32, uint32, error) {
	return c.resolveFileBatchWith(
		state, batch,
		func(key []byte) uint64 {
			return storeio.KeyHashBytes(state.root.StoreID, key)
		},
		func(key []byte, hash uint64) (storeio.PageKeyLocation, bool, error) {
			if state.keyRoot == (storeio.PageRef{}) {
				return storeio.PageKeyLocation{}, false, nil
			}
			match, found, err := c.resolveFileFingerprintHash(state, key, hash)
			if err != nil || !found {
				match.Release()
				return storeio.PageKeyLocation{}, found, err
			}
			location := match.location
			match.Release()
			return location, true, nil
		},
	)
}

// resolveFileBatchWith keeps hash injection confined to planning. Production
// always passes the StoreID-keyed hash and the exact document-backed resolver
// above; tests can force a collision without weakening the durable hash.
func (c *Collection) resolveFileBatchWith(
	state *fileStoreState,
	batch *WriteBatch,
	hashKey func([]byte) uint64,
	resolve func([]byte, uint64) (storeio.PageKeyLocation, bool, error),
) (uint32, uint32, error) {
	mutations := c.batchMutations[:0]
	for _, entry := range batch.entries {
		key := batch.key(entry)
		hash := hashKey(key)
		fingerprint, found, err := resolve(key, hash)
		if err != nil {
			return 0, 0, err
		}
		location := storeio.KeyLocation{
			Chunk: fingerprint.Chunk, Slot: fingerprint.Slot,
		}
		if !found && entry.remove {
			continue
		}
		mutation := fileBatchMutation{
			key: key, location: location, fingerprint: fingerprint,
			created: !found, remove: entry.remove,
		}
		if !found {
			mutation.fingerprint.Hash = hash
		}
		if !entry.remove {
			mutation.value = batch.value(entry)
		}
		mutations = append(mutations, mutation)
	}
	c.batchMutations = mutations

	// Record the lowest chunk this publication frees, but do not hand a slot
	// retired by this same atomic publication back to another key. The chunk
	// materializer deliberately has one edit per stable slot; cross-key
	// replacement in a single slot would otherwise make the create observe the
	// deleted key in the old immutable page and conflate their index history.
	// The next publication starts from the lowered hint and reuses the hole.
	c.batchPlacement = c.batchPlacement[:0]
	deletedChunkHint := state.root.ChunkHighWater
	for i := range mutations {
		mutation := &mutations[i]
		if mutation.created || !mutation.remove {
			continue
		}
		deletedChunkHint = min(deletedChunkHint, mutation.location.Chunk)
	}

	highWater := state.root.ChunkHighWater
	limit := fileStoreLiveMask(state.root.ChunkDocuments)
	freeChunkHint := state.root.FreeChunkHint
	for i := range mutations {
		mutation := &mutations[i]
		if !mutation.created {
			continue
		}
		for {
			chunk := freeChunkHint
			if chunk > highWater {
				return 0, 0, storeio.ErrDocumentPageCorrupt
			}
			index := c.findFileBatchPlacement(chunk)
			live := uint64(0)
			if index >= 0 {
				live = c.batchPlacement[index].live
			} else if chunk < state.root.ChunkHighWater {
				var err error
				live, err = c.loadFileChunkLive(state, chunk)
				if err != nil {
					return 0, 0, err
				}
			}
			free := ^live & limit
			if free == 0 {
				freeChunkHint++
				continue
			}
			if chunk == highWater {
				if highWater == ^uint32(0) {
					return 0, 0, store.ErrTooLarge
				}
				highWater++
			}
			if index < 0 {
				c.batchPlacement = append(c.batchPlacement, fileBatchPlacement{
					chunk: chunk, live: live,
				})
				index = len(c.batchPlacement) - 1
			}
			slot := uint8(bits.TrailingZeros64(free))
			mutation.location = storeio.KeyLocation{Chunk: chunk, Slot: slot}
			mutation.fingerprint.Chunk = chunk
			mutation.fingerprint.Slot = slot
			c.batchPlacement[index].live |= uint64(1) << slot
			if c.batchPlacement[index].live == limit {
				freeChunkHint++
			}
			break
		}
	}
	return highWater, min(freeChunkHint, deletedChunkHint), nil
}

func (c *Collection) findFileBatchPlacement(chunk uint32) int {
	for i := range c.batchPlacement {
		if c.batchPlacement[i].chunk == chunk {
			return i
		}
	}
	return -1
}

func (c *Collection) loadFileChunkLive(
	state *fileStoreState, chunk uint32,
) (uint64, error) {
	_, view, leases, err := c.loadFileChunk(state, chunk)
	if err != nil {
		return 0, err
	}
	if leases != nil {
		defer leases.Release()
	}
	if view == nil {
		return 0, nil
	}
	return view.live(), nil
}

type fileBatchChunkResult struct {
	chunkRoot     storeio.PageRef
	documentCount uint64
	liveChunks    uint32
	appendChunk   uint32
	appendLive    uint64
}

// applyFileBatchChunks rebuilds every chunk page the batch touches exactly
// once, no matter how many of its documents land in that chunk. This is the
// write-amplification fix: a Put that adds one row to a sixty-four-row chunk
// rewrites all sixty-four, so sixty-four separate Puts into one chunk rewrote
// 2,080 rows where this rewrites 64.
func (c *Collection) applyFileBatchChunks(
	tx *storeio.WriteTransaction, state *fileStoreState,
) (fileBatchChunkResult, error) {
	result := fileBatchChunkResult{
		chunkRoot: state.chunkRoot, documentCount: state.root.DocumentCount,
		liveChunks: state.root.LiveChunks, appendChunk: c.appendChunk, appendLive: c.appendLive,
	}
	mutations := c.batchMutations
	slices.SortFunc(mutations, func(a, b fileBatchMutation) int {
		if a.location.Chunk != b.location.Chunk {
			if a.location.Chunk < b.location.Chunk {
				return -1
			}
			return 1
		}
		return int(a.location.Slot) - int(b.location.Slot)
	})
	for first := 0; first < len(mutations); {
		chunk := mutations[first].location.Chunk
		last := first + 1
		for last < len(mutations) && mutations[last].location.Chunk == chunk {
			last++
		}
		group := mutations[first:last]
		first = last
		if err := c.applyFileBatchChunk(tx, state, chunk, group, &result); err != nil {
			return result, err
		}
	}
	if len(c.batchChunkTreeEdits) != 0 {
		mutation, err := storeio.MutateChunkTreeBatch(
			c.cache, tx, state.chunkRoot, c.batchChunkTreeEdits,
			storeio.ChunkTreeBounds{
				FileEnd: state.super.FileEnd, NextLogicalID: state.root.NextLogicalID,
			},
			c.batchRetired[:0],
		)
		c.batchRetired = mutation.Retired
		if err != nil {
			return result, batchAllocationError(err)
		}
		result.chunkRoot = mutation.Root
		for _, ref := range mutation.Retired {
			if err := c.appendIndexRetiredRef(state, ref); err != nil {
				return result, err
			}
		}
	}
	return result, nil
}

func (c *Collection) applyFileBatchChunk(
	tx *storeio.WriteTransaction, state *fileStoreState, chunk uint32,
	group []fileBatchMutation, result *fileBatchChunkResult,
) error {
	oldRef, oldZone, oldView, oldLease, err := c.loadFileChunkZone(state, chunk)
	if err != nil {
		return err
	}
	if oldLease != nil {
		defer oldLease.Release()
	}
	edited := uint64(0)
	for i := range group {
		bit := uint64(1) << group[i].location.Slot
		if edited&bit != 0 {
			return storeio.ErrDocumentPageCorrupt
		}
		edited |= bit
	}
	if err := c.seedFileFloat64Columns(state, oldView, edited); err != nil {
		return err
	}
	edits := c.batchChunkEdits[:0]
	for i := range group {
		mutation := &group[i]
		edit, editErr := c.stageFileBatchRow(tx, state, oldView, mutation)
		if editErr != nil {
			return editErr
		}
		edits = append(edits, edit)
		if mutation.created {
			result.documentCount++
		}
		if mutation.remove {
			result.documentCount--
		}
	}
	c.batchChunkEdits = edits
	rows, live, err := c.buildFileRows(state, oldView, edits)
	if err != nil {
		return err
	}
	if live == 0 {
		c.batchChunkTreeEdits = append(c.batchChunkTreeEdits, storeio.ChunkTreeEdit{
			Chunk: chunk, Delete: true,
		})
		result.liveChunks--
	} else {
		columns := c.fileFloat64Columns(state)
		documentSize, sizeErr := c.fileDocumentPageSize(rows, columns)
		if sizeErr != nil {
			return sizeErr
		}
		documentLogicalID := uint64(0)
		if oldRef.Kind == storeio.PageDocument {
			documentLogicalID = oldRef.LogicalID
		}
		page, allocateErr := tx.Allocate(storeio.PageDocument, documentSize, documentLogicalID)
		if allocateErr != nil {
			return batchAllocationError(allocateErr)
		}
		if _, encodeErr := storeio.EncodeDocumentPageWithColumns(page.Bytes(), storeio.DocumentPageHeader{
			StoreID: c.storeID, Generation: tx.Generation(), LogicalID: page.Ref().LogicalID,
			PageSize: page.Ref().Length, ChunkID: chunk, Live: live,
		}, rows, columns, tx.NextLogicalID(), tx.FileEnd(), uint32(c.options.PageSize)); encodeErr != nil {
			return encodeErr
		}
		if stageErr := page.Stage(); stageErr != nil {
			return stageErr
		}
		zone := storeio.ChunkZone{}
		hasZone := false
		if merger := c.zoneMerger(
			rows, edits, zonePriorDocs(oldView),
		); merger != nil {
			zone = merger.MergeChunkZone(oldZone)
			hasZone = true
		}
		c.batchChunkTreeEdits = append(c.batchChunkTreeEdits, storeio.ChunkTreeEdit{
			Chunk: chunk, Document: page.Ref(), HasZone: hasZone, Zone: zone,
		})
		if oldRef == (storeio.PageRef{}) {
			result.liveChunks++
		}
	}
	if err := c.appendDocumentRetirement(state, oldRef, oldView); err != nil {
		return err
	}
	if chunk == result.appendChunk || chunk >= state.root.ChunkHighWater {
		result.appendChunk = chunk
		result.appendLive = live
	}
	return nil
}

// stageFileBatchRow prepares one document's contribution to its chunk: the old
// row's index tuples and overflow retirement, then the new row's staged value,
// index tuples, and typed covering values.
//
// Both parsed indexes are consumed here rather than being retained by the
// caller. A batch that kept every replacement document's index alive would hold
// one parse arena per document, turning a bounded writer into one that scales
// its peak memory with the batch size.
func (c *Collection) stageFileBatchRow(
	tx *storeio.WriteTransaction, state *fileStoreState,
	oldView *fileDocumentChunk, mutation *fileBatchMutation,
) (fileChunkEdit, error) {
	edit := fileChunkEdit{slot: mutation.location.Slot, keep: !mutation.remove}
	var oldIndex vibejson.Index
	hasOldIndex := false
	if mutation.created {
		if oldView != nil {
			if _, occupied := oldView.lookup(mutation.location.Slot); occupied {
				return edit, storeio.ErrDocumentPageCorrupt
			}
		}
	} else {
		if oldView == nil {
			return edit, storeio.ErrDocumentPageCorrupt
		}
		oldValue, ok := oldView.lookupKey(mutation.location.Slot, mutation.key)
		if !ok {
			return edit, storeio.ErrDocumentPageCorrupt
		}
		if !oldValue.grouped {
			if err := c.appendOverflowRetirements(state, oldValue.value, mutation.location); err != nil {
				return edit, err
			}
		}
		// Only the exact indexes need the replaced document. The single-document
		// path also re-parses it for configured float64 columns, because its
		// projection maintainer asks whether the covered values changed; this
		// path releases the projection outright, and the typed columns it writes
		// into the chunk page are seeded from the old page's own column values
		// rather than from a re-parse. Re-reading and re-parsing every replaced
		// document to answer a question nobody asks is the most expensive thing
		// a batch of updates could do.
		if len(c.options.indexes) != 0 {
			raw, valueErr := c.appendFileDocumentValue(
				c.indexValueScratch[:0], state, *oldView, oldValue, mutation.location,
			)
			if valueErr != nil {
				return edit, valueErr
			}
			c.indexValueScratch = raw
			oldIndex, valueErr = c.buildOldFileIndex(raw)
			if valueErr != nil {
				return edit, valueErr
			}
			hasOldIndex = true
		}
	}
	if mutation.remove {
		var oldPointer *vibejson.Index
		if hasOldIndex {
			oldPointer = &oldIndex
		}
		return edit, c.appendFileBatchIndexEdits(mutation.location, oldPointer, nil)
	}
	newIndex, err := c.validateDocument(mutation.value)
	if err != nil {
		return edit, err
	}
	var oldPointer *vibejson.Index
	if hasOldIndex {
		oldPointer = &oldIndex
	}
	if err := c.appendFileBatchIndexEdits(mutation.location, oldPointer, &newIndex); err != nil {
		return edit, err
	}
	if err := c.setFileFloat64Column(state, mutation.location.Slot, &newIndex); err != nil {
		return edit, err
	}
	record, err := c.stageFileValue(tx, mutation.location, mutation.key, mutation.value)
	if err != nil {
		return edit, batchAllocationError(err)
	}
	edit.record = record
	return edit, nil
}

// appendFileBatchIndexEdits records the posting changes one document owes,
// applying the same unchanged-tuple short circuit the single-document path
// uses so a replacement that leaves every indexed value alone writes nothing.
func (c *Collection) appendFileBatchIndexEdits(
	location storeio.KeyLocation, oldIndex, newIndex *vibejson.Index,
) error {
	for indexID, exact := range c.options.indexes {
		var oldHash, newHash uint64
		var oldOK, newOK bool
		var err error
		if oldIndex != nil {
			oldHash, oldOK, err = fileIndexTupleHash(exact, *oldIndex)
			if err != nil {
				return err
			}
		}
		if newIndex != nil {
			newHash, newOK, err = fileIndexTupleHash(exact, *newIndex)
			if err != nil {
				return err
			}
		}
		if oldOK && newOK && oldHash == newHash {
			equal, equalErr := fileIndexTuplesEqual(exact, *oldIndex, *newIndex)
			if equalErr != nil {
				return equalErr
			}
			if equal {
				continue
			}
			if err := c.appendFileBatchIndexEdit(uint32(indexID), newHash, location, true, exact, newIndex); err != nil {
				return err
			}
			continue
		}
		if oldOK {
			if err := c.appendFileBatchIndexEdit(uint32(indexID), oldHash, location, false, exact, nil); err != nil {
				return err
			}
		}
		if newOK {
			if err := c.appendFileBatchIndexEdit(uint32(indexID), newHash, location, true, exact, newIndex); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *Collection) appendFileBatchIndexEdit(
	indexID uint32, tupleHash uint64, location storeio.KeyLocation,
	present bool, exact *store.ExactIndex, index *vibejson.Index,
) error {
	edit := fileIndexBatchEdit{
		indexID: indexID, tupleHash: tupleHash, chunk: location.Chunk,
		slot: location.Slot, present: present, certOffset: len(c.batchCertArena),
	}
	if present {
		// fileIndexCertificate reports "this value has no representative" by
		// returning a nil slice rather than the arena it was handed, so the
		// arena must only be adopted when it actually grew. Assigning the nil
		// back would truncate the arena and silently repoint every certificate
		// span the batch had already recorded.
		arena, err := c.fileIndexCertificate(c.batchCertArena, exact, *index)
		if err != nil {
			return err
		}
		if arena != nil {
			c.batchCertArena = arena
			edit.certLength = len(arena) - edit.certOffset
		}
	}
	c.batchIndexEdits = append(c.batchIndexEdits, edit)
	return nil
}

func (c *Collection) applyFileBatchKeys(
	tx *storeio.WriteTransaction, state *fileStoreState, highWater uint32,
) (storeio.PageRef, error) {
	edits := c.batchPageKeyEdits[:0]
	edits = appendFileBatchKeyEdits(edits, c.batchMutations)
	c.batchPageKeyEdits = edits
	if len(edits) == 0 {
		return state.keyRoot, nil
	}
	mutation, err := storeio.MutatePageKeyTreeBatch(
		c.cache, tx, state.keyRoot, edits, storeio.PageKeyTreeBounds{
			FileEnd: tx.FileEnd(), NextLogicalID: tx.NextLogicalID(),
			ChunkHighWater: highWater, ChunkDocuments: state.root.ChunkDocuments,
		}, c.batchRetired[:0])
	c.batchRetired = mutation.Retired
	if err != nil {
		return storeio.PageRef{}, batchAllocationError(err)
	}
	// Every edit was derived from an exact full-key resolution before the
	// transaction allocated anything. A missing delete or duplicate insert
	// therefore means the primary directory disagreed with the document pages.
	// Publishing the chunk changes in that case would make the split permanent,
	// so fail closed while the transaction is still abortable.
	if err := validateFileBatchKeyMutation(mutation, len(edits)); err != nil {
		return storeio.PageRef{}, err
	}
	for _, ref := range mutation.Retired {
		if err := c.appendIndexRetiredRef(state, ref); err != nil {
			return storeio.PageRef{}, err
		}
	}
	return mutation.Root, nil
}

func appendFileBatchKeyEdits(
	edits []storeio.PageKeyTreeEdit, mutations []fileBatchMutation,
) []storeio.PageKeyTreeEdit {
	for i := range mutations {
		mutation := &mutations[i]
		if !mutation.created && !mutation.remove {
			continue
		}
		edit := storeio.PageKeyTreeEdit{
			Location:  mutation.fingerprint,
			Operation: storeio.PageKeyTreeInsert,
		}
		if mutation.remove {
			edit.Operation = storeio.PageKeyTreeDelete
		}
		edits = append(edits, edit)
	}
	slices.SortFunc(edits, func(a, b storeio.PageKeyTreeEdit) int {
		return compareFileBatchFingerprint(a.Location, b.Location)
	})
	return edits
}

func validateFileBatchKeyMutation(
	mutation storeio.PageKeyTreeBatchMutation, expected int,
) error {
	if mutation.Applied != expected || expected != 0 && !mutation.Changed {
		return storeio.ErrKeyDirectoryCorrupt
	}
	return nil
}

func compareFileBatchFingerprint(a, b storeio.PageKeyLocation) int {
	if a.Hash < b.Hash {
		return -1
	}
	if a.Hash > b.Hash {
		return 1
	}
	if a.Chunk < b.Chunk {
		return -1
	}
	if a.Chunk > b.Chunk {
		return 1
	}
	return int(a.Slot) - int(b.Slot)
}

// applyFileBatchIndexes resolves every collected posting change against the
// published directory, coalesces the changes that share a routing key, and
// applies the result in one descent per index tree.
//
// Resolving before writing is what lets the descent be purely mechanical: the
// mask a routing key keeps depends on the exact-value certificate already
// stored under its tuple hash, and that comparison is semantic JSON equality
// rather than byte equality, which the page layer cannot perform.
func (c *Collection) applyFileBatchIndexes(
	tx *storeio.WriteTransaction, state *fileStoreState,
) (storeio.PageRef, error) {
	if len(c.batchIndexEdits) == 0 {
		return state.indexRoot, nil
	}
	edits := c.batchIndexEdits
	slices.SortFunc(edits, func(a, b fileIndexBatchEdit) int {
		if a.indexID != b.indexID {
			return int(a.indexID) - int(b.indexID)
		}
		if a.tupleHash != b.tupleHash {
			if a.tupleHash < b.tupleHash {
				return -1
			}
			return 1
		}
		if a.chunk != b.chunk {
			if a.chunk < b.chunk {
				return -1
			}
			return 1
		}
		return int(a.slot) - int(b.slot)
	})
	bounds := storeio.IndexTreeBounds{
		FileEnd: tx.FileEnd(), NextLogicalID: tx.NextLogicalID(),
		IndexHighWater: uint32(len(c.options.indexes)),
	}
	c.batchTreeEdits = c.batchTreeEdits[:0]
	c.batchTreeCerts = c.batchTreeCerts[:0]
	for first := 0; first < len(edits); {
		last := first + 1
		for last < len(edits) && edits[last].indexID == edits[first].indexID &&
			edits[last].tupleHash == edits[first].tupleHash &&
			edits[last].chunk == edits[first].chunk {
			last++
		}
		if err := c.resolveFileBatchPosting(state, edits[first:last], bounds); err != nil {
			return storeio.PageRef{}, err
		}
		first = last
	}
	if len(c.batchTreeEdits) == 0 {
		return state.indexRoot, nil
	}
	mutation, err := storeio.MutateIndexTreeBatch(
		c.cache, tx, state.indexRoot, c.batchTreeEdits, c.batchTreeCerts, bounds, c.batchRetired[:0],
	)
	c.batchRetired = mutation.Retired
	if err != nil {
		return storeio.PageRef{}, batchAllocationError(err)
	}
	for _, ref := range mutation.Retired {
		if err := c.appendIndexRetiredRef(state, ref); err != nil {
			return storeio.PageRef{}, err
		}
	}
	return mutation.Root, nil
}

// resolveFileBatchPosting folds one routing key's changes into the mask the
// published directory already holds and emits at most one tree edit for it.
func (c *Collection) resolveFileBatchPosting(
	state *fileStoreState, group []fileIndexBatchEdit, bounds storeio.IndexTreeBounds,
) error {
	key := storeio.IndexDirectoryKey{
		IndexID: group[0].indexID, TupleHash: group[0].tupleHash, Chunk: group[0].chunk,
	}
	// LookupIndexTree copies the stored representative out before it drops the
	// leaf lease, so the scratch below never aliases an evictable page frame.
	entry, certificate, found, err := storeio.LookupIndexTree(
		c.cache, state.indexRoot, key, bounds, c.indexCertificateScratch[:0],
	)
	c.indexCertificateScratch = certificate
	if err != nil {
		return err
	}
	columns := int(c.options.indexes[key.IndexID].N)
	mask := uint64(0)
	collision := false
	if found {
		if len(c.indexCertificateScratch) != 0 &&
			!fileIndexCertificateValid(c.indexCertificateScratch, columns) {
			return storeio.ErrIndexDirectoryCorrupt
		}
		collision = entry.Flags&storeio.IndexEntryCollision != 0
		mask = entry.Bits
	}
	for _, edit := range group {
		bit := uint64(1) << edit.slot
		if !edit.present {
			mask &^= bit
			continue
		}
		incoming := c.batchCertArena[edit.certOffset : edit.certOffset+edit.certLength]
		if edit.certLength == 0 {
			// A value with no representative makes the whole routing key
			// uncertifiable: readers must recheck every row behind the mask.
			c.indexCertificateScratch = c.indexCertificateScratch[:0]
			collision = false
		} else if len(c.indexCertificateScratch) == 0 {
			if found || mask != 0 {
				collision = true
			}
			c.indexCertificateScratch = append(c.indexCertificateScratch, incoming...)
		} else if !fileIndexCertificatesEqual(c.indexCertificateScratch, incoming, columns) {
			collision = true
		}
		mask |= bit
	}
	if mask == 0 {
		if !found {
			return nil
		}
		c.batchTreeEdits = append(c.batchTreeEdits, storeio.IndexTreeEdit{
			Entry: storeio.IndexDirectoryEntry{Key: key}, Delete: true,
		})
		return nil
	}
	flags := uint16(0)
	if collision {
		flags |= storeio.IndexEntryCollision
	}
	// A zero-length span is only well formed at offset zero, because that is how
	// the leaf format spells "this posting has no representative". Stamping the
	// arena's current end into an empty span instead made the whole batch
	// unpublishable the moment it met an indexed value too long to certify — a
	// value the single-document path routes and records without complaint.
	span := storeio.CertSpan{}
	if len(c.indexCertificateScratch) != 0 {
		span.Offset = uint32(len(c.batchTreeCerts))
		span.Length = uint16(len(c.indexCertificateScratch))
		c.batchTreeCerts = append(c.batchTreeCerts, c.indexCertificateScratch...)
	}
	c.batchTreeEdits = append(c.batchTreeEdits, storeio.IndexTreeEdit{
		Entry: storeio.IndexDirectoryEntry{
			Key: key, Bits: mask, Flags: flags,
			Kind: storeio.IndexEntryInlineMask, Cert: span,
		},
	})
	return nil
}

// collectFileBatchRetirements lists the bulk-built read accelerators this path
// releases rather than maintains. State is carried in the fixed root and owns
// no allocator extent. This reserves nothing, so it is safe to run before
// syncFreeLog, which is what makes the list durable.
func (c *Collection) collectFileBatchRetirements(
	state *fileStoreState, retireFloat64Scan, retireIndexGroup bool,
) error {
	if retireFloat64Scan {
		if err := c.appendFloat64ScanRetirements(state); err != nil {
			return err
		}
	}
	if retireIndexGroup {
		if err := c.appendIndexGroupRetirements(state); err != nil {
			return err
		}
	}
	return nil
}

// reserveFileBatchRetirements hands the complete list to the reclaimer. It runs
// after syncFreeLog for the same reason reserveFileRetirements does: a fold adds
// its own superseded pages to the same list, and it only knows which once it has
// decided to fold.
func (c *Collection) reserveFileBatchRetirements() error {
	if err := c.reclaimer.RetireBatch(c.retireScratch); err != nil {
		if errors.Is(err, storeio.ErrRetiredExtentCapacity) {
			if retryErr := c.retryRetirementAfterPressure(); retryErr == nil {
				return nil
			} else if !errors.Is(retryErr, storeio.ErrRetiredExtentCapacity) {
				return retryErr
			}
		}
		return c.absorbRetirementPressure(err)
	}
	return nil
}

// batchAllocationError translates the transaction allocator's exhaustion into
// the batch-shaped error a caller can act on. Nothing is published either way;
// only the advice differs.
func batchAllocationError(err error) error {
	if errors.Is(err, storeio.ErrTooManyPages) {
		return errors.Join(ErrBatchTooLarge, err)
	}
	return err
}
