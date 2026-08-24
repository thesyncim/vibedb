package durable

import (
	"errors"
)

// ErrBatchTooLarge reports a batch whose mutations do not fit the reservation
// Options.MaxBatchDocuments sized. Nothing was published; the caller may retry
// with fewer mutations.
var ErrBatchTooLarge = errors.New("vibedb: collection write batch exceeds configured bound")

// ErrBatchClosed reports use of a WriteBatch after the Update that owns it
// returned. Batches are pooled per collection, so a retained one would write
// into a later caller's mutations.
var ErrBatchClosed = errors.New("vibedb: collection write batch is no longer active")

// WriteBatch accumulates the mutations one Update applies as one logical atomic
// publication. Most batches need one generation. When the final rows cannot fit
// the current leaf topology, Update may first publish a content-equivalent
// topology generation and publish the logical batch in the following generation.
//
// Keys are deduplicated as they arrive: mutating the same key twice keeps only
// the second mutation, so the published generation contains exactly one row
// state per key. Keys and values are copied into the batch, so the caller
// may reuse its buffers as soon as a method returns.
//
// Deduplication also makes last-write-wins semantics explicit before routing:
// deleting a key and then putting it back in one batch publishes the final
// document without staging an intermediate primary mutation. Physical row
// coordinates are not part of the collection contract.
//
// JSON syntax is validated when Update applies a JSON collection's batch, not
// when Put records it. Opaque collections do no syntax validation.
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
// ErrBatchTooLarge immediately; malformed JSON is reported by Update for JSON
// collections. Opaque collections retain src byte-for-byte.
//
// key is borrowed for the duration of the call and not retained after it
// returns: the batch copies it into its own arena. The caller may reuse or
// mutate the backing array as soon as Put returns.
func (b *WriteBatch) Put(key []byte, src []byte) error {
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
// hold is not an error and publishes nothing for it. key is borrowed for the
// call only; the batch copies it into its own arena.
func (b *WriteBatch) Delete(key []byte) error {
	if b == nil || !b.active {
		return ErrBatchClosed
	}
	if len(key) > b.collection.options.MaxKeyBytes {
		return ErrKeyTooLarge
	}
	return b.record(key, nil, true)
}

func (b *WriteBatch) record(key []byte, src []byte, remove bool) error {
	// m[string(b)] is the compiler's non-allocating map-read pattern: the string
	// conversion is used only as the index expression and never escapes.
	if at, exists := b.position[string(key)]; exists {
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

// appendRecovery records one already-validated atomic journal entry without
// consulting the caller-facing MaxBatchDocuments/MaxBatchBytes admission
// policy. Those limits are process options and may be smaller on reopen than
// when the acknowledged record was written; recovery must honor the durable
// record. The ordinary apply path still validates persisted key/document/schema
// semantics before publication.
func (b *WriteBatch) appendRecovery(key, src []byte, remove bool) error {
	if b == nil || !b.active {
		return ErrBatchClosed
	}
	if _, exists := b.position[string(key)]; exists {
		return errors.New("vibedb: duplicate key in recovery batch")
	}
	entry := writeBatchEntry{
		keyOffset: len(b.keys), keyLength: len(key),
		valueOffset: len(b.values), valueLength: len(src), remove: remove,
	}
	b.keys = append(b.keys, key...)
	b.values = append(b.values, src...)
	b.entries = append(b.entries, entry)
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

// Update applies every mutation fn records as one logical failure-atomic
// publication.
//
// The logical batch either publishes whole or publishes nothing: an error
// returned by fn, or by any mutation the batch stages, exposes no subset of its
// primary rows or exact-index postings. If the final rows cannot fit the current
// leaf topology, Update may first publish one content-equivalent topology
// generation. A later logical or durability error can therefore leave the rows
// and postings unchanged while Generation may advance. A prepare-time rejection
// is ordinary and never poisons; only a durability fence failure does.
//
// The logical commit is one rewritten leaf frame per touched leaf, one batch
// journal record synced once, and every logical leaf change published in one
// generation (see updatePrimaryBatch); the batch is carried only by the
// buffered-visible and sync-journal lanes.
func (c *Collection) Update(fn func(*WriteBatch) error) (err error) {
	if c == nil {
		return ErrClosed
	}
	if err := c.rejectCheckpointGroupOwner(); err != nil {
		return err
	}
	if fn == nil {
		return errors.New("vibedb: collection Update requires a function")
	}
	// Every collection is an ordered primary graph, so a batch is always applied
	// as one routed transaction over the graph (see updatePrimaryBatch).
	return c.updatePrimaryBatch(fn)
}

// fileWriteBatch borrows the reusable WriteBatch handle, resetting it for a
// fresh Update. The handle and its dedup map are pooled on the Collection so a
// steady-state batch allocates nothing.
func (c *Collection) fileWriteBatch() *WriteBatch {
	if c.batch == nil {
		c.batch = &WriteBatch{
			collection: c,
			position:   make(map[string]int, c.options.MaxBatchDocuments),
		}
	}
	batch := c.batch
	batch.reset()
	batch.active = true
	return batch
}

func (c *Collection) releaseFileWriteBatch(batch *WriteBatch) {
	batch.active = false
	batch.reset()
}
