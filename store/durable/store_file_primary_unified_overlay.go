package durable

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/document"
)

const (
	// The record/hash window is deliberately wider than the distinct-bucket
	// directory: replacement-heavy workloads can accumulate many generations
	// across the same bounded set of dirty leaves before paying one fold. The
	// hash table remains at 50% load at the record ceiling.
	// Keep enough logical replacements resident to amortize one foreground
	// physical fold across a useful write window.  The former 4,096-record
	// ceiling forced a whole dirty-tree checkpoint roughly every four thousand
	// updates even when the byte and resident-memory budgets still had ample
	// room.  Thirty-two thousand entries remain representable by the uint16
	// fold indexes and keep all metadata and redo storage statically bounded.
	// The 8 MiB arena is still charged against Options.ResidentBytes: it trades
	// cache residency for a longer, predictable foreground fold interval rather
	// than introducing hidden memory.
	primaryUnifiedOverlayRecords = 32768
	primaryUnifiedOverlayBuckets = filePrimaryPendingParentLimit
	primaryUnifiedOverlayTable   = 65536
	// The bucket directory stays at <=50% load at the overlay's hard
	// distinct-bucket limit. Open addressing therefore gives the mutation and
	// scan lanes a bounded, allocation-free bucket lookup without the previous
	// record-window rescans (including the quadratic distinct-bucket census).
	primaryUnifiedOverlayBucketTable = 2048
	primaryUnifiedOverlayMax         = 8 << 20

	primaryUnifiedOverlayPut    = 1
	primaryUnifiedOverlayDelete = 2
)

type primaryUnifiedOverlayDisposition uint8

const (
	primaryUnifiedOverlayMissing primaryUnifiedOverlayDisposition = iota
	primaryUnifiedOverlayValue
	primaryUnifiedOverlayDeleted
)

// primaryUnifiedOverlayRecord is one immutable replacement linked into the
// fixed overlay table. Offsets address overlay.arena; previous is a one-based
// record index. Fields are written completely before the head's atomic Store
// publishes the record to lock-free point readers.
type primaryUnifiedOverlayRecord struct {
	generation uint64
	hash       uint64
	previous   uint32
	// bucketPrevious links only records for this bucket. The global previous
	// chain remains the point-lookup hash chain; this second chain makes
	// per-bucket deltas, fold replay, scans, and version checks independent of
	// unrelated mutations in the same overlay window.
	bucketPrevious uint32
	bucket         uint32
	keyOffset      uint32
	valueOff       uint32
	valueLen       uint32
	rawDelta       int32
	keyLen         uint16
	countDelta     int8
	kind           uint8
	slot           uint8
	// reservationWide describes this key's final fold shape at generation.
	// Old records remain immutable for readers and recovery; the bucket-wide
	// aggregate is updated from the newest record per key, so a later certified
	// restore can retire an older tombstone's conservative wide reservation.
	reservationWide uint8
	// scalarPatch occupies the record's former six-byte tail padding. It is an
	// opaque, fail-closed certificate against the routed admitted leaf base.
	scalarPatch storeio.CommonPrimaryUnifiedScalarPatch
}

const primaryUnifiedOverlayRecordBytes = 56

var (
	_ [primaryUnifiedOverlayRecordBytes - int(unsafe.Sizeof(primaryUnifiedOverlayRecord{}))]byte
	_ [int(unsafe.Sizeof(primaryUnifiedOverlayRecord{})) - primaryUnifiedOverlayRecordBytes]byte
	_ [6 - int(unsafe.Sizeof(storeio.CommonPrimaryUnifiedScalarPatch{}))]byte
	_ [int(unsafe.Sizeof(storeio.CommonPrimaryUnifiedScalarPatch{})) - 6]byte
)

// primaryUnifiedOverlayBucket is one open-addressed directory entry. head is
// the publication word: head==0 means the slot is empty, and publish writes
// bucket plus the aggregates before storing head. The serialized writer owns
// updates, while atomics keep lock-free scans and online-index reconciliation
// race-free.
type primaryUnifiedOverlayBucket struct {
	bucket        atomic.Uint32
	head          atomic.Uint32
	reservedBytes atomic.Uint32
	fixedBytes    atomic.Uint32
	wideKeys      atomic.Int32
	rawBytes      atomic.Int64
	rows          atomic.Int32
	insertSlots   [4]atomic.Uint64
	// scalarBase/shape/hole bind the conservative compact fold envelope to one
	// immutable source leaf and one numeric column for the current pending epoch.
	// They are writer-owned admission metadata; no durable record grammar or
	// lock-free reader state depends on them.
	scalarBase       storeio.PageRef
	scalarShape      uint16
	scalarHole       uint16
	scalarFixedBytes uint32
	scalarSet        bool
}

type primaryUnifiedOverlay struct {
	records []primaryUnifiedOverlayRecord
	heads   []atomic.Uint32
	arena   []byte
	// backingBytes is the configured arena capacity even before a read-only
	// collection performs its first mutation. backingOnce keeps the sizeable
	// record/hash/byte backing pay-for-use while preserving one immutable,
	// bounded allocation shape after publication starts.
	backingBytes int
	backingOnce  sync.Once
	buckets      [primaryUnifiedOverlayBucketTable]primaryUnifiedOverlayBucket
	// bucketCount belongs to the writer, but an atomic keeps the directory's
	// reset/publication protocol explicit alongside count and used.
	bucketCount atomic.Uint32
	// bucketLimit is the collection's normalized runtime dirty-leaf bound. The
	// fixed directory above is the 1,024-bucket maximum; explicit smaller
	// BufferCount or ResidentBytes configurations use only this prefix budget.
	bucketLimit uint32
	// dirtyByteLimit bounds the physical leaf extents plus conservative parent
	// allowance that one fold may stage inside ResidentBytes. Same-shape native
	// patch certificates reserve the routed extent's exact length; every other
	// mutation reserves maxLeafBytes and upgrades an existing bucket if needed.
	dirtyByteLimit uint64
	dirtyBytes     atomic.Uint64
	maxLeafBytes   uint32
	parentBytes    uint32

	// count and used publish initialized record/arena prefixes for stats and
	// fold-side traversal. The serialized writer owns reservations; readers
	// reach records only through heads.
	count atomic.Uint32
	used  atomic.Uint32
	// folded is the newest generation already represented by the durable leaf
	// base. Records remain readable until a reader-free publication can recycle
	// the arena; generations above folded are the next checkpoint's dirty set.
	folded atomic.Uint64
}

type primaryUnifiedOverlayPrepared struct {
	index         uint32
	usedAfter     uint32
	headSlot      uint32
	bucketSlot    uint32
	reservedBytes uint32
	fixedBytes    uint32
	wideKeys      int32
	dirtyAfter    uint64
	newBucket     bool
}

// primaryUnifiedOverlayBatchMutation is the fully resolved input to one
// checkpoint-group row-overlay preparation. The caller has already proved that
// the key is an existing inline row and that the final leaf shape remains
// within the compact geometry envelope. Every mutation in a batch receives the
// same generation; the private cursors below are what make that safe.
type primaryUnifiedOverlayBatchMutation struct {
	bucket          storeio.BucketID
	hash            uint64
	key             []byte
	value           []byte
	rawDelta        int
	countDelta      int
	kind            uint8
	stableSlot      uint8
	fixedLeafBytes  uint32
	reservationWide bool
	scalarPatch     storeio.CommonPrimaryUnifiedScalarPatch
	scalarBase      storeio.PageRef
	scalarShape     int
	scalarHole      int
}

type primaryUnifiedOverlayBatchRecord struct {
	headSlot   uint32
	bucketSlot uint32
}

type primaryUnifiedOverlayBatchHash struct {
	slot uint32
	head uint32
}

type primaryUnifiedOverlayBatchKey struct {
	bucket storeio.BucketID
	hash   uint64
	key    []byte
	wide   bool
}

type primaryUnifiedOverlayBatchBucket struct {
	bucket           uint32
	slot             uint32
	head             uint32
	baseReserved     uint32
	reservedBytes    uint32
	fixedBytes       uint32
	wideKeys         int32
	rawBytes         int64
	rows             int32
	insertSlots      [4]uint64
	newBucket        bool
	scalarBase       storeio.PageRef
	scalarShape      uint16
	scalarHole       uint16
	scalarFixedBytes uint32
	scalarSet        bool
}

// primaryUnifiedOverlayBatchPrepared owns a contiguous, unpublished record and
// arena suffix. No reader can reach either suffix until publishBatch installs
// the final count, bucket heads, and hash heads. abortBatch simply abandons
// those private cursors; a later prepare may overwrite the suffix safely.
type primaryUnifiedOverlayBatchPrepared struct {
	records    []primaryUnifiedOverlayBatchRecord
	buckets    []primaryUnifiedOverlayBatchBucket
	hashes     []primaryUnifiedOverlayBatchHash
	countAfter uint32
	usedAfter  uint32
	dirtyAfter uint64
	generation uint64
	live       bool
}

const (
	primaryUnifiedOverlayMaxInt64 = int64(^uint64(0) >> 1)
	primaryUnifiedOverlayMinInt64 = -primaryUnifiedOverlayMaxInt64 - 1
	primaryUnifiedOverlayMaxInt32 = int64(^uint32(0) >> 1)
	primaryUnifiedOverlayMinInt32 = -primaryUnifiedOverlayMaxInt32 - 1
)

func addPrimaryUnifiedOverlayInt64(value int64, delta int) (int64, bool) {
	d := int64(delta)
	if d > 0 && value > primaryUnifiedOverlayMaxInt64-d {
		return 0, false
	}
	if d < 0 && value < primaryUnifiedOverlayMinInt64-d {
		return 0, false
	}
	return value + d, true
}

func addPrimaryUnifiedOverlayInt32(value int32, delta int) (int32, bool) {
	d := int64(delta)
	if d > 0 && int64(value) > primaryUnifiedOverlayMaxInt32-d {
		return 0, false
	}
	if d < 0 && int64(value) < primaryUnifiedOverlayMinInt32-d {
		return 0, false
	}
	return int32(int64(value) + d), true
}

func newPrimaryUnifiedOverlay(
	bytes, bucketLimit int,
	dirtyByteLimit uint64,
	maxLeafBytes, parentBytes uint32,
) *primaryUnifiedOverlay {
	o := newLazyPrimaryUnifiedOverlay(
		bytes, bucketLimit, dirtyByteLimit, maxLeafBytes, parentBytes,
	)
	if o != nil {
		o.ensureBacking()
	}
	return o
}

func newLazyPrimaryUnifiedOverlay(
	bytes, bucketLimit int,
	dirtyByteLimit uint64,
	maxLeafBytes, parentBytes uint32,
) *primaryUnifiedOverlay {
	if bytes <= 0 || bucketLimit <= 0 ||
		bucketLimit > primaryUnifiedOverlayBuckets ||
		dirtyByteLimit == 0 || maxLeafBytes == 0 || parentBytes == 0 ||
		uint64(maxLeafBytes)+uint64(parentBytes) > uint64(^uint32(0)) {
		return nil
	}
	return &primaryUnifiedOverlay{
		backingBytes:   bytes,
		bucketLimit:    uint32(bucketLimit),
		dirtyByteLimit: dirtyByteLimit,
		maxLeafBytes:   maxLeafBytes,
		parentBytes:    parentBytes,
	}
}

func (o *primaryUnifiedOverlay) ensureBacking() {
	o.backingOnce.Do(func() {
		o.records = make([]primaryUnifiedOverlayRecord, primaryUnifiedOverlayRecords)
		o.heads = make([]atomic.Uint32, primaryUnifiedOverlayTable)
		o.arena = make([]byte, o.backingBytes)
	})
}

func (o *primaryUnifiedOverlay) capacityBytes() int {
	if o == nil {
		return 0
	}
	return o.backingBytes
}

func primaryUnifiedOverlayTargetBytes(
	pageSize, maxKeyBytes, inlineValueBytes int,
) int {
	if pageSize <= 0 || maxKeyBytes <= 0 || inlineValueBytes <= 0 {
		return 0
	}
	target := primaryUnifiedOverlayRecords * (maxKeyBytes + inlineValueBytes)
	target = min(target, primaryUnifiedOverlayMax)
	target = max(target, 64<<10)
	return (target + pageSize - 1) / pageSize * pageSize
}

func (o *primaryUnifiedOverlay) hasPending() bool {
	if o == nil {
		return false
	}
	count := o.count.Load()
	return count != 0 && o.records[count-1].generation > o.folded.Load()
}

func primaryUnifiedOverlayBucketHash(bucket storeio.BucketID) uint32 {
	// Bucket identities carry tablet bits above the local-id field, so masking
	// their low bits directly would collide corresponding leaves from every
	// tablet. This integer finalizer folds all 32 bits before the power-of-two
	// table mask.
	value := uint32(bucket)
	value ^= value >> 16
	value *= 0x7feb352d
	value ^= value >> 15
	value *= 0x846ca68b
	value ^= value >> 16
	return value
}

// bucketSlot returns the matching directory slot, or the first empty slot.
// Entries are never deleted individually: markFolded clears the whole table,
// so encountering an empty slot proves the bucket is absent.
func (o *primaryUnifiedOverlay) bucketSlot(
	bucket storeio.BucketID,
) (slot uint32, found bool) {
	if o == nil {
		return 0, false
	}
	mask := uint32(primaryUnifiedOverlayBucketTable - 1)
	slot = primaryUnifiedOverlayBucketHash(bucket) & mask
	for range primaryUnifiedOverlayBucketTable {
		entry := &o.buckets[slot]
		if entry.head.Load() == 0 {
			return slot, false
		}
		if entry.bucket.Load() == uint32(bucket) {
			return slot, true
		}
		slot = (slot + 1) & mask
	}
	return 0, false
}

func (o *primaryUnifiedOverlay) pendingBucketHead(
	bucket storeio.BucketID, folded uint64,
) uint32 {
	slot, found := o.bucketSlot(bucket)
	if !found {
		return 0
	}
	head := o.buckets[slot].head.Load()
	if head == 0 || int(head) > len(o.records) ||
		o.records[head-1].generation <= folded {
		return 0
	}
	return head
}

func (o *primaryUnifiedOverlay) pendingBucket(bucket storeio.BucketID) bool {
	if o == nil {
		return false
	}
	return o.pendingBucketHead(bucket, o.folded.Load()) != 0
}

// bucketVersion is the generation of the newest visible row-overlay record
// for bucket. It is a per-leaf reconciliation token: an unchanged durable
// PageRef is not enough to prove an online-index scan is current because
// VCS1 replaces rows in this append-only overlay between checkpoints.
func (o *primaryUnifiedOverlay) bucketVersion(
	bucket storeio.BucketID, generation uint64,
) uint64 {
	if o == nil {
		return 0
	}
	folded := o.folded.Load()
	head := o.pendingBucketHead(bucket, folded)
	for head != 0 {
		record := &o.records[head-1]
		if record.generation <= folded {
			break
		}
		if record.generation <= generation {
			return record.generation
		}
		head = record.bucketPrevious
	}
	return 0
}

func (o *primaryUnifiedOverlay) pendingBucketDeltas(
	bucket storeio.BucketID,
) (rawBytes, rows int) {
	if o == nil {
		return 0, 0
	}
	folded := o.folded.Load()
	slot, found := o.bucketSlot(bucket)
	if !found {
		return 0, 0
	}
	entry := &o.buckets[slot]
	head := entry.head.Load()
	if head == 0 || int(head) > len(o.records) ||
		o.records[head-1].generation <= folded {
		return 0, 0
	}
	return int(entry.rawBytes.Load()), int(entry.rows.Load())
}

// scalarColumnForBatch returns the one numeric shape/hole bound retained for a
// pending bucket. A base-ref mismatch means the routed immutable leaf changed
// underneath the pending epoch and the caller must re-enter the complete COW
// path; it is never safe to reuse the old envelope against a new PageRef.
func (o *primaryUnifiedOverlay) scalarColumnForBatch(
	bucket storeio.BucketID, base storeio.PageRef,
) (shape, hole int, fixedBytes uint32, bound, compatible bool) {
	if o == nil {
		return 0, 0, 0, false, true
	}
	slot, found := o.bucketSlot(bucket)
	if !found {
		return 0, 0, 0, false, true
	}
	entry := &o.buckets[slot]
	head := entry.head.Load()
	if head == 0 || int(head) > len(o.records) ||
		o.records[head-1].generation <= o.folded.Load() {
		return 0, 0, 0, false, true
	}
	if !entry.scalarSet {
		return 0, 0, 0, false, true
	}
	if entry.scalarBase != base || entry.scalarFixedBytes == 0 {
		return 0, 0, 0, false, false
	}
	return int(entry.scalarShape), int(entry.scalarHole),
		entry.scalarFixedBytes, true, true
}

// pendingInsertSlots returns every slot claimed by an overlay-native insert
// since the last fold. Deleted insert slots deliberately remain reserved until
// the fold so a different key cannot claim the same stable slot while an old
// generation may still resolve the first key through the overlay.
func (o *primaryUnifiedOverlay) pendingInsertSlots(
	bucket storeio.BucketID,
) (occupied [4]uint64) {
	if o == nil {
		return occupied
	}
	folded := o.folded.Load()
	slot, found := o.bucketSlot(bucket)
	if !found {
		return occupied
	}
	entry := &o.buckets[slot]
	head := entry.head.Load()
	if head == 0 || int(head) > len(o.records) ||
		o.records[head-1].generation <= folded {
		return occupied
	}
	for i := range occupied {
		occupied[i] = entry.insertSlots[i].Load()
	}
	return occupied
}

// chooseLargeUnindexedSlot assigns a collision-free overlay-local identity.
// Large unindexed compact stripes have no persisted uint8 posting geometry;
// the slot exists only so the bounded overlay can coalesce repeated keys.
func (o *primaryUnifiedOverlay) chooseLargeUnindexedSlot(
	bucket storeio.BucketID, hash uint64,
) (uint8, bool) {
	return o.chooseLargeUnindexedSlotWithAdditional(
		bucket, hash, [4]uint64{},
	)
}

// chooseLargeUnindexedSlotWithAdditional is the batch form of the large
// unindexed identity allocator. additional contains slots selected earlier in
// the same unpublished batch; the published overlay history is still scanned
// so collisions remain stable across retries and folds.
func (o *primaryUnifiedOverlay) chooseLargeUnindexedSlotWithAdditional(
	bucket storeio.BucketID, hash uint64, additional [4]uint64,
) (uint8, bool) {
	if o == nil {
		return 0, false
	}
	used := additional
	folded := o.folded.Load()
	bucketSlot, found := o.bucketSlot(bucket)
	if found {
		for head := o.buckets[bucketSlot].head.Load(); head != 0; {
			if int(head) > len(o.records) {
				return 0, false
			}
			record := &o.records[head-1]
			if record.bucket != uint32(bucket) || record.generation <= folded {
				break
			}
			used[record.slot>>6] |= uint64(1) << uint(record.slot&63)
			if record.bucketPrevious >= head {
				return 0, false
			}
			head = record.bucketPrevious
		}
	}
	start := uint8(hash)
	for step := 0; step < storeio.CommonPrimaryLeafWideSlots; step++ {
		slot := start + uint8(step)
		if used[slot>>6]&(uint64(1)<<uint(slot&63)) == 0 {
			return slot, true
		}
	}
	return 0, false
}

func (o *primaryUnifiedOverlay) lookup(
	bucket storeio.BucketID, hash uint64, key []byte, generation uint64,
) ([]byte, primaryUnifiedOverlayDisposition, uint8) {
	if o == nil || o.count.Load() == 0 {
		return nil, primaryUnifiedOverlayMissing, 0
	}
	folded := o.folded.Load()
	head := o.heads[hash&(primaryUnifiedOverlayTable-1)].Load()
	for head != 0 {
		record := &o.records[head-1]
		// A reader at the fold generation may still hold the pre-fold route and
		// needs the overlay that completed that generation. Later generations
		// resolve against the new durable base and must ignore those stale
		// records; this also lets a writer fall back structurally when an
		// in-flight old reader temporarily prevents arena recycling.
		if folded != 0 && generation > folded &&
			record.generation <= folded {
			head = record.previous
			continue
		}
		if record.generation <= generation &&
			record.bucket == uint32(bucket) &&
			record.hash == hash &&
			int(record.keyOffset)+int(record.keyLen) <= len(o.arena) &&
			bytes.Equal(
				o.arena[record.keyOffset:uint32(record.keyOffset)+uint32(record.keyLen)],
				key,
			) {
			end := uint64(record.valueOff) + uint64(record.valueLen)
			if end > uint64(len(o.arena)) {
				return nil, primaryUnifiedOverlayMissing, 0
			}
			if record.kind == primaryUnifiedOverlayDelete {
				return nil, primaryUnifiedOverlayDeleted, record.slot
			}
			if record.kind != primaryUnifiedOverlayPut {
				return nil, primaryUnifiedOverlayMissing, 0
			}
			return o.arena[record.valueOff:uint32(end):uint32(end)],
				primaryUnifiedOverlayValue, record.slot
		}
		head = record.previous
	}
	return nil, primaryUnifiedOverlayMissing, 0
}

// pendingKeyReservationWide reports the newest pending fold shape for key.
// Absence means the immutable base is still the final shape and therefore has
// no conservative wide charge. This intentionally ignores folded generations:
// their shape is already represented by the routed physical extent.
func (o *primaryUnifiedOverlay) pendingKeyReservationWide(
	bucket storeio.BucketID, hash uint64, key []byte,
) (bool, error) {
	if o == nil || o.count.Load() == 0 {
		return false, nil
	}
	folded := o.folded.Load()
	used := o.used.Load()
	head := o.heads[hash&(primaryUnifiedOverlayTable-1)].Load()
	for head != 0 {
		if int(head) > len(o.records) {
			return false, storeio.ErrCommonPrimaryLeafCorrupt
		}
		record := &o.records[head-1]
		keyEnd := uint64(record.keyOffset) + uint64(record.keyLen)
		if keyEnd > uint64(used) {
			return false, storeio.ErrCommonPrimaryLeafCorrupt
		}
		if record.generation > folded &&
			record.bucket == uint32(bucket) && record.hash == hash &&
			bytes.Equal(
				o.arena[record.keyOffset:uint32(keyEnd):uint32(keyEnd)], key,
			) {
			return record.reservationWide != 0, nil
		}
		head = record.previous
	}
	return false, nil
}

// sameSizeReusableRecord returns the newest pending record for key only when
// its arena bytes are no longer needed to construct a future recovery-journal
// batch. The caller still has to exclude readers before overwriting the value:
// the durable journal protects crash recovery, while snapshot/epoch exclusion
// protects an in-process reader of the older visible generation.
func (o *primaryUnifiedOverlay) sameSizeReusableRecord(
	bucket storeio.BucketID,
	hash uint64,
	key []byte,
	valueLen int,
	stableSlot uint8,
	journalDurableGeneration uint64,
) (*primaryUnifiedOverlayRecord, bool) {
	if o == nil || len(key) == 0 || valueLen <= 0 ||
		journalDurableGeneration == 0 ||
		o.count.Load() >= uint32(len(o.records)) {
		return nil, false
	}
	folded := o.folded.Load()
	used := o.used.Load()
	head := o.heads[hash&(primaryUnifiedOverlayTable-1)].Load()
	for head != 0 {
		if int(head) > len(o.records) {
			return nil, false
		}
		record := &o.records[head-1]
		keyEnd := uint64(record.keyOffset) + uint64(record.keyLen)
		if record.bucket == uint32(bucket) && record.hash == hash &&
			keyEnd <= uint64(used) &&
			bytes.Equal(o.arena[record.keyOffset:uint32(keyEnd):uint32(keyEnd)], key) {
			// This is the newest record for the key. Never search through an
			// ineligible latest record and alias an older version underneath it.
			valueEnd := uint64(record.valueOff) + uint64(record.valueLen)
			return record,
				record.generation > folded &&
					record.generation <= journalDurableGeneration &&
					record.kind == primaryUnifiedOverlayPut &&
					record.countDelta == 0 &&
					record.slot == stableSlot &&
					int(record.valueLen) == valueLen &&
					keyEnd == uint64(record.valueOff) &&
					valueEnd <= uint64(used)
		}
		head = record.previous
	}
	return nil, false
}

func (o *primaryUnifiedOverlay) canReuseSameSizeArena(
	bucket storeio.BucketID,
	hash uint64,
	key, value []byte,
	stableSlot uint8,
	journalDurableGeneration uint64,
) bool {
	_, ok := o.sameSizeReusableRecord(
		bucket, hash, key, len(value), stableSlot,
		journalDurableGeneration,
	)
	return ok
}

// prepareSameSizeArenaReuse appends a fresh generation-stamped metadata record
// while retaining the newest journal-covered record's key/value offsets. It is
// deliberately non-fallible after the value copy: every capacity, identity,
// generation, and bounds check completes first, so a caller holding the reader
// exclusion fence can overwrite and immediately publish without leaving an old
// visible generation pointing at uncommitted bytes.
func (o *primaryUnifiedOverlay) prepareSameSizeArenaReuse(
	bucket storeio.BucketID,
	hash uint64,
	generation uint64,
	key, value []byte,
	stableSlot uint8,
	journalDurableGeneration uint64,
	leafBytes uint32,
	scalarPatch storeio.CommonPrimaryUnifiedScalarPatch,
) (primaryUnifiedOverlayPrepared, bool) {
	if generation == 0 || len(value) == 0 || o == nil ||
		leafBytes == 0 || leafBytes > o.maxLeafBytes {
		return primaryUnifiedOverlayPrepared{}, false
	}
	previous, ok := o.sameSizeReusableRecord(
		bucket, hash, key, len(value), stableSlot,
		journalDurableGeneration,
	)
	if !ok {
		return primaryUnifiedOverlayPrepared{}, false
	}
	bucketSlot, bucketFound := o.bucketSlot(bucket)
	if !bucketFound {
		return primaryUnifiedOverlayPrepared{}, false
	}
	entry := &o.buckets[bucketSlot]
	currentReserved := entry.reservedBytes.Load()
	fixedBytes := uint32(uint64(leafBytes) + uint64(o.parentBytes))
	fixedBytes = max(fixedBytes, entry.fixedBytes.Load())
	wideKeys := entry.wideKeys.Load()
	if wideKeys < 0 {
		return primaryUnifiedOverlayPrepared{}, false
	}
	reservationWide := leafBytes == o.maxLeafBytes
	previousWide := previous.reservationWide != 0
	if reservationWide && !previousWide {
		wideKeys++
	} else if !reservationWide && previousWide {
		wideKeys--
	}
	if wideKeys < 0 {
		return primaryUnifiedOverlayPrepared{}, false
	}
	reservedBytes := fixedBytes
	if wideKeys != 0 {
		reservedBytes = uint32(uint64(o.maxLeafBytes) + uint64(o.parentBytes))
	}
	currentDirty := o.dirtyBytes.Load()
	if currentDirty < uint64(currentReserved) {
		return primaryUnifiedOverlayPrepared{}, false
	}
	dirtyAfter := currentDirty - uint64(currentReserved) + uint64(reservedBytes)
	if dirtyAfter > o.dirtyByteLimit {
		return primaryUnifiedOverlayPrepared{}, false
	}
	index := o.count.Load()
	used := o.used.Load()
	headSlot := uint32(hash & (primaryUnifiedOverlayTable - 1))
	o.records[index] = primaryUnifiedOverlayRecord{
		generation:     generation,
		hash:           hash,
		previous:       o.heads[headSlot].Load(),
		bucketPrevious: o.buckets[bucketSlot].head.Load(),
		bucket:         uint32(bucket),
		keyOffset:      previous.keyOffset,
		valueOff:       previous.valueOff,
		valueLen:       previous.valueLen,
		keyLen:         previous.keyLen,
		kind:           primaryUnifiedOverlayPut,
		slot:           stableSlot,
		scalarPatch:    scalarPatch,
		reservationWide: func() uint8 {
			if reservationWide {
				return 1
			}
			return 0
		}(),
	}
	copy(
		o.arena[previous.valueOff:previous.valueOff+previous.valueLen],
		value,
	)
	return primaryUnifiedOverlayPrepared{
		index: index, usedAfter: used,
		headSlot: headSlot, bucketSlot: bucketSlot,
		reservedBytes: reservedBytes, fixedBytes: fixedBytes,
		wideKeys: wideKeys, dirtyAfter: dirtyAfter,
	}, true
}

// prepare reserves and fills one record without publishing it. The caller may
// abandon the reservation by doing nothing: the writer-owned count/used words
// are advanced only by publish.
func (o *primaryUnifiedOverlay) prepare(
	bucket storeio.BucketID,
	hash uint64,
	generation uint64,
	key, value []byte,
	rawDelta, countDelta int,
	kind, stableSlot uint8,
) (primaryUnifiedOverlayPrepared, error) {
	if o == nil {
		return primaryUnifiedOverlayPrepared{}, fmt.Errorf(
			"%w: unified row overlay disabled", storeio.ErrPageCachePinned,
		)
	}
	return o.prepareWithLeafBytes(
		bucket, hash, generation, key, value,
		rawDelta, countDelta, kind, stableSlot, o.maxLeafBytes,
	)
}

// prepareWithLeafBytes is the certified fixed-extent admission path. Only the
// concurrent existing-key lane may pass less than maxLeafBytes, and only after
// the fused scalar/generic admission proof certifies that exact routed extent.
// All structural/shape-changing callers use prepare above.
func (o *primaryUnifiedOverlay) prepareWithLeafBytes(
	bucket storeio.BucketID,
	hash uint64,
	generation uint64,
	key, value []byte,
	rawDelta, countDelta int,
	kind, stableSlot uint8,
	leafBytes uint32,
) (primaryUnifiedOverlayPrepared, error) {
	return o.prepareWithLeafReservation(
		bucket, hash, generation, key, value,
		rawDelta, countDelta, kind, stableSlot,
		leafBytes, leafBytes == o.maxLeafBytes,
		storeio.CommonPrimaryUnifiedScalarPatch{},
	)
}

// prepareWithLeafReservation records both the routed fixed extent and whether
// this key's newest final shape needs the conservative maximum extent. The
// distinction lets a Delete -> certified Put transition remove only that key's
// wide charge without discarding the immutable tombstone record needed by old
// readers or recovery-journal construction.
func (o *primaryUnifiedOverlay) prepareWithLeafReservation(
	bucket storeio.BucketID,
	hash uint64,
	generation uint64,
	key, value []byte,
	rawDelta, countDelta int,
	kind, stableSlot uint8,
	fixedLeafBytes uint32,
	reservationWide bool,
	scalarPatch storeio.CommonPrimaryUnifiedScalarPatch,
) (primaryUnifiedOverlayPrepared, error) {
	if o == nil || generation == 0 || len(key) == 0 ||
		kind != primaryUnifiedOverlayPut &&
			kind != primaryUnifiedOverlayDelete ||
		kind == primaryUnifiedOverlayPut && len(value) == 0 ||
		kind == primaryUnifiedOverlayDelete && len(value) != 0 {
		return primaryUnifiedOverlayPrepared{}, fmt.Errorf(
			"%w: unified row overlay disabled", storeio.ErrPageCachePinned,
		)
	}
	if int(int32(rawDelta)) != rawDelta ||
		countDelta < -1 || countDelta > 1 ||
		fixedLeafBytes == 0 || fixedLeafBytes > o.maxLeafBytes {
		return primaryUnifiedOverlayPrepared{}, fmt.Errorf(
			"%w: unified row overlay delta", storeio.ErrInvalidWrite,
		)
	}
	o.ensureBacking()
	index := o.count.Load()
	used := o.used.Load()
	needed := uint64(len(key)) + uint64(len(value))
	if index >= uint32(len(o.records)) ||
		uint64(used)+needed > uint64(len(o.arena)) {
		return primaryUnifiedOverlayPrepared{}, fmt.Errorf(
			"%w: unified row overlay pressure", storeio.ErrPageCachePinned,
		)
	}
	bucketSlot, bucketFound := o.bucketSlot(bucket)
	if !bucketFound &&
		o.bucketCount.Load() >= o.bucketLimit {
		return primaryUnifiedOverlayPrepared{}, fmt.Errorf(
			"%w: unified row overlay bucket pressure",
			storeio.ErrPageCachePinned,
		)
	}
	currentReserved := uint32(0)
	currentFixed := uint32(0)
	currentWide := int32(0)
	if bucketFound {
		entry := &o.buckets[bucketSlot]
		currentReserved = entry.reservedBytes.Load()
		currentFixed = entry.fixedBytes.Load()
		currentWide = entry.wideKeys.Load()
		if currentWide < 0 {
			return primaryUnifiedOverlayPrepared{}, storeio.ErrCommonPrimaryLeafCorrupt
		}
	}
	fixedBytes := uint32(uint64(fixedLeafBytes) + uint64(o.parentBytes))
	fixedBytes = max(fixedBytes, currentFixed)
	previousWide, err := o.pendingKeyReservationWide(bucket, hash, key)
	if err != nil {
		return primaryUnifiedOverlayPrepared{}, err
	}
	wideKeys := currentWide
	if reservationWide && !previousWide {
		wideKeys++
	} else if !reservationWide && previousWide {
		wideKeys--
	}
	if wideKeys < 0 {
		return primaryUnifiedOverlayPrepared{}, storeio.ErrCommonPrimaryLeafCorrupt
	}
	reservedBytes := fixedBytes
	if wideKeys != 0 {
		reservedBytes = uint32(uint64(o.maxLeafBytes) + uint64(o.parentBytes))
	}
	currentDirty := o.dirtyBytes.Load()
	if currentDirty < uint64(currentReserved) {
		return primaryUnifiedOverlayPrepared{}, storeio.ErrCommonPrimaryLeafCorrupt
	}
	dirtyAfter := currentDirty - uint64(currentReserved) + uint64(reservedBytes)
	if dirtyAfter > o.dirtyByteLimit {
		return primaryUnifiedOverlayPrepared{}, fmt.Errorf(
			"%w: unified row overlay dirty-byte pressure",
			storeio.ErrPageCachePinned,
		)
	}
	keyOff := used
	valueOff := keyOff + uint32(len(key))
	copy(o.arena[keyOff:valueOff], key)
	copy(o.arena[valueOff:valueOff+uint32(len(value))], value)
	slot := uint32(hash & (primaryUnifiedOverlayTable - 1))
	o.records[index] = primaryUnifiedOverlayRecord{
		generation:     generation,
		hash:           hash,
		previous:       o.heads[slot].Load(),
		bucketPrevious: o.buckets[bucketSlot].head.Load(),
		bucket:         uint32(bucket),
		keyOffset:      keyOff,
		valueOff:       valueOff,
		valueLen:       uint32(len(value)),
		rawDelta:       int32(rawDelta),
		keyLen:         uint16(len(key)),
		countDelta:     int8(countDelta),
		kind:           kind,
		slot:           stableSlot,
		scalarPatch:    scalarPatch,
		reservationWide: func() uint8 {
			if reservationWide {
				return 1
			}
			return 0
		}(),
	}
	return primaryUnifiedOverlayPrepared{
		index: index, usedAfter: valueOff + uint32(len(value)),
		headSlot: slot, bucketSlot: bucketSlot,
		reservedBytes: reservedBytes, fixedBytes: fixedBytes,
		wideKeys: wideKeys, dirtyAfter: dirtyAfter,
		newBucket: !bucketFound,
	}, nil
}

// batchBucketSlot resolves a bucket against both the published directory and
// the private bucket set being prepared. The published directory cannot be
// touched until the whole batch is ready, so ordinary bucketSlot would return
// the same first empty slot for two distinct new buckets in one batch.
func (o *primaryUnifiedOverlay) batchBucketSlot(
	bucket storeio.BucketID,
	private []primaryUnifiedOverlayBatchBucket,
) (slot uint32, found bool, ok bool) {
	if o == nil {
		return 0, false, false
	}
	mask := uint32(primaryUnifiedOverlayBucketTable - 1)
	slot = primaryUnifiedOverlayBucketHash(bucket) & mask
	for range primaryUnifiedOverlayBucketTable {
		privateSlot := -1
		for index := range private {
			if private[index].slot == slot {
				privateSlot = index
				break
			}
		}
		if privateSlot >= 0 {
			if private[privateSlot].bucket == uint32(bucket) {
				return slot, true, true
			}
			slot = (slot + 1) & mask
			continue
		}
		entry := &o.buckets[slot]
		if entry.head.Load() == 0 {
			return slot, false, true
		}
		if entry.bucket.Load() == uint32(bucket) {
			return slot, true, true
		}
		slot = (slot + 1) & mask
	}
	return 0, false, false
}

// prepareBatch reserves and fills a complete same-generation row-overlay
// batch. Every record and arena byte is written into an unpublished suffix;
// count, used, heads, bucket aggregates, and dirtyBytes remain unchanged until
// publishBatch. This is intentionally a separate implementation from
// prepare: repeatedly calling the single-record method would reread the same
// unpublished count and arena cursor and overwrite the first record.
func (o *primaryUnifiedOverlay) prepareBatch(
	generation uint64,
	mutations []primaryUnifiedOverlayBatchMutation,
) (primaryUnifiedOverlayBatchPrepared, error) {
	if o == nil {
		return primaryUnifiedOverlayBatchPrepared{}, fmt.Errorf(
			"%w: unified row overlay disabled", storeio.ErrPageCachePinned,
		)
	}
	if generation == 0 || len(mutations) == 0 {
		return primaryUnifiedOverlayBatchPrepared{}, fmt.Errorf(
			"%w: unified row overlay batch", storeio.ErrInvalidWrite,
		)
	}
	o.ensureBacking()
	count := o.count.Load()
	used := o.used.Load()
	if count > uint32(len(o.records)) || used > uint32(len(o.arena)) {
		return primaryUnifiedOverlayBatchPrepared{}, storeio.ErrCommonPrimaryLeafCorrupt
	}
	if uint64(count)+uint64(len(mutations)) > uint64(len(o.records)) {
		return primaryUnifiedOverlayBatchPrepared{}, fmt.Errorf(
			"%w: unified row overlay record pressure", storeio.ErrPageCachePinned,
		)
	}
	var needed uint64
	for index := range mutations {
		mutation := &mutations[index]
		if len(mutation.key) == 0 ||
			len(mutation.key) > int(^uint16(0)) ||
			uint64(len(mutation.key)) > uint64(^uint32(0)) ||
			uint64(len(mutation.value)) > uint64(^uint32(0)) ||
			mutation.kind != primaryUnifiedOverlayPut &&
				mutation.kind != primaryUnifiedOverlayDelete ||
			mutation.kind == primaryUnifiedOverlayPut && len(mutation.value) == 0 ||
			mutation.kind == primaryUnifiedOverlayDelete && len(mutation.value) != 0 ||
			int64(int32(mutation.rawDelta)) != int64(mutation.rawDelta) ||
			mutation.countDelta < -1 || mutation.countDelta > 1 ||
			mutation.fixedLeafBytes == 0 ||
			mutation.fixedLeafBytes > o.maxLeafBytes {
			return primaryUnifiedOverlayBatchPrepared{}, fmt.Errorf(
				"%w: unified row overlay batch mutation", storeio.ErrInvalidWrite,
			)
		}
		if needed > ^uint64(0)-uint64(len(mutation.key))-
			uint64(len(mutation.value)) {
			return primaryUnifiedOverlayBatchPrepared{}, storeio.ErrInvalidWrite
		}
		needed += uint64(len(mutation.key)) + uint64(len(mutation.value))
	}
	if uint64(used)+needed > uint64(len(o.arena)) {
		return primaryUnifiedOverlayBatchPrepared{}, fmt.Errorf(
			"%w: unified row overlay arena pressure", storeio.ErrPageCachePinned,
		)
	}
	currentDirty := o.dirtyBytes.Load()
	if currentDirty > o.dirtyByteLimit {
		return primaryUnifiedOverlayBatchPrepared{}, storeio.ErrCommonPrimaryLeafCorrupt
	}
	if o.bucketCount.Load() > o.bucketLimit {
		return primaryUnifiedOverlayBatchPrepared{}, storeio.ErrCommonPrimaryLeafCorrupt
	}

	prepared := primaryUnifiedOverlayBatchPrepared{
		records:    make([]primaryUnifiedOverlayBatchRecord, len(mutations)),
		buckets:    make([]primaryUnifiedOverlayBatchBucket, 0, len(mutations)),
		hashes:     make([]primaryUnifiedOverlayBatchHash, 0, len(mutations)),
		countAfter: count,
		usedAfter:  used,
		generation: generation,
	}
	keys := make([]primaryUnifiedOverlayBatchKey, 0, len(mutations))
	for index := range mutations {
		mutation := &mutations[index]
		bucketSlot, bucketFound, slotOK := o.batchBucketSlot(
			mutation.bucket, prepared.buckets,
		)
		if !slotOK {
			return primaryUnifiedOverlayBatchPrepared{}, fmt.Errorf(
				"%w: unified row overlay bucket pressure", storeio.ErrPageCachePinned,
			)
		}
		bucketIndex := -1
		for candidate := range prepared.buckets {
			if prepared.buckets[candidate].slot == bucketSlot {
				bucketIndex = candidate
				break
			}
		}
		if bucketIndex < 0 {
			if !bucketFound {
				newBuckets := 0
				for candidate := range prepared.buckets {
					if prepared.buckets[candidate].newBucket {
						newBuckets++
					}
				}
				if uint64(o.bucketCount.Load())+uint64(newBuckets)+1 >
					uint64(o.bucketLimit) {
					return primaryUnifiedOverlayBatchPrepared{}, fmt.Errorf(
						"%w: unified row overlay bucket pressure",
						storeio.ErrPageCachePinned,
					)
				}
			}
			bucket := primaryUnifiedOverlayBatchBucket{
				bucket:    uint32(mutation.bucket),
				slot:      bucketSlot,
				newBucket: !bucketFound,
			}
			if bucketFound {
				entry := &o.buckets[bucketSlot]
				bucket.head = entry.head.Load()
				bucket.baseReserved = entry.reservedBytes.Load()
				bucket.fixedBytes = entry.fixedBytes.Load()
				bucket.wideKeys = entry.wideKeys.Load()
				bucket.rawBytes = entry.rawBytes.Load()
				bucket.rows = entry.rows.Load()
				for slot := range bucket.insertSlots {
					bucket.insertSlots[slot] = entry.insertSlots[slot].Load()
				}
				bucket.scalarBase = entry.scalarBase
				bucket.scalarShape = entry.scalarShape
				bucket.scalarHole = entry.scalarHole
				bucket.scalarFixedBytes = entry.scalarFixedBytes
				bucket.scalarSet = entry.scalarSet
				if bucket.head != 0 && int(bucket.head) > len(o.records) {
					return primaryUnifiedOverlayBatchPrepared{},
						storeio.ErrCommonPrimaryLeafCorrupt
				}
			}
			bucketIndex = len(prepared.buckets)
			prepared.buckets = append(prepared.buckets, bucket)
		}
		bucket := &prepared.buckets[bucketIndex]
		fixedBytes64 := uint64(mutation.fixedLeafBytes) + uint64(o.parentBytes)
		if fixedBytes64 > uint64(^uint32(0)) {
			return primaryUnifiedOverlayBatchPrepared{}, storeio.ErrInvalidWrite
		}
		if uint32(fixedBytes64) > bucket.fixedBytes {
			bucket.fixedBytes = uint32(fixedBytes64)
		}
		if mutation.scalarPatch !=
			(storeio.CommonPrimaryUnifiedScalarPatch{}) &&
			mutation.scalarShape >= 0 {
			if mutation.scalarShape > int(^uint16(0)) ||
				mutation.scalarHole < 0 ||
				mutation.scalarHole > int(^uint16(0)) {
				return primaryUnifiedOverlayBatchPrepared{},
					storeio.ErrInvalidWrite
			}
			if bucket.scalarSet {
				if bucket.scalarBase != mutation.scalarBase ||
					bucket.scalarShape != uint16(mutation.scalarShape) ||
					bucket.scalarHole != uint16(mutation.scalarHole) ||
					bucket.scalarFixedBytes != mutation.fixedLeafBytes {
					return primaryUnifiedOverlayBatchPrepared{},
						storeio.ErrInvalidWrite
				}
			} else {
				if mutation.fixedLeafBytes == 0 {
					return primaryUnifiedOverlayBatchPrepared{},
						storeio.ErrInvalidWrite
				}
				bucket.scalarBase = mutation.scalarBase
				bucket.scalarShape = uint16(mutation.scalarShape)
				bucket.scalarHole = uint16(mutation.scalarHole)
				bucket.scalarFixedBytes = mutation.fixedLeafBytes
				bucket.scalarSet = true
			}
		}

		previousWide, foundKey := false, false
		for keyIndex := range keys {
			candidate := &keys[keyIndex]
			if bytes.Equal(candidate.key, mutation.key) {
				previousWide, foundKey = candidate.wide, true
				break
			}
		}
		if !foundKey {
			var err error
			previousWide, err = o.pendingKeyReservationWide(
				mutation.bucket, mutation.hash, mutation.key,
			)
			if err != nil {
				return primaryUnifiedOverlayBatchPrepared{}, err
			}
		} else {
			// WriteBatch canonicalization removes duplicate final keys before
			// staging. Rejecting a duplicate here keeps direct callers from
			// creating ambiguous same-generation slot/aggregate deltas after
			// private preparation has begun.
			return primaryUnifiedOverlayBatchPrepared{}, storeio.ErrInvalidWrite
		}
		if mutation.reservationWide && !previousWide {
			bucket.wideKeys++
		} else if !mutation.reservationWide && previousWide {
			bucket.wideKeys--
		}
		if bucket.wideKeys < 0 {
			return primaryUnifiedOverlayBatchPrepared{},
				storeio.ErrCommonPrimaryLeafCorrupt
		}
		rawBytes, rawOK := addPrimaryUnifiedOverlayInt64(
			bucket.rawBytes, mutation.rawDelta,
		)
		if !rawOK {
			return primaryUnifiedOverlayBatchPrepared{}, storeio.ErrInvalidWrite
		}
		bucket.rawBytes = rawBytes
		rows, rowsOK := addPrimaryUnifiedOverlayInt32(
			bucket.rows, mutation.countDelta,
		)
		if !rowsOK {
			return primaryUnifiedOverlayBatchPrepared{}, storeio.ErrInvalidWrite
		}
		bucket.rows = rows
		if mutation.countDelta > 0 {
			bucket.insertSlots[mutation.stableSlot>>6] |=
				uint64(1) << uint(mutation.stableSlot&63)
		}

		headSlot := uint32(mutation.hash & (primaryUnifiedOverlayTable - 1))
		previous := o.heads[headSlot].Load()
		for hashIndex := range prepared.hashes {
			if prepared.hashes[hashIndex].slot == headSlot {
				previous = prepared.hashes[hashIndex].head
				break
			}
		}
		keyOffset := prepared.usedAfter
		valueOffset := keyOffset + uint32(len(mutation.key))
		valueEnd := valueOffset + uint32(len(mutation.value))
		recordIndex := prepared.countAfter
		o.records[recordIndex] = primaryUnifiedOverlayRecord{
			generation:     generation,
			hash:           mutation.hash,
			previous:       previous,
			bucketPrevious: bucket.head,
			bucket:         uint32(mutation.bucket),
			keyOffset:      keyOffset,
			valueOff:       valueOffset,
			valueLen:       uint32(len(mutation.value)),
			rawDelta:       int32(mutation.rawDelta),
			keyLen:         uint16(len(mutation.key)),
			countDelta:     int8(mutation.countDelta),
			kind:           mutation.kind,
			slot:           mutation.stableSlot,
			scalarPatch:    mutation.scalarPatch,
			reservationWide: func() uint8 {
				if mutation.reservationWide {
					return 1
				}
				return 0
			}(),
		}
		copy(o.arena[keyOffset:valueOffset], mutation.key)
		copy(o.arena[valueOffset:valueEnd], mutation.value)
		prepared.records[index] = primaryUnifiedOverlayBatchRecord{
			headSlot: headSlot, bucketSlot: bucket.slot,
		}
		prepared.countAfter++
		prepared.usedAfter = valueEnd
		bucket.head = recordIndex + 1
		foundHash := false
		for hashIndex := range prepared.hashes {
			if prepared.hashes[hashIndex].slot == headSlot {
				prepared.hashes[hashIndex].head = recordIndex + 1
				foundHash = true
				break
			}
		}
		if !foundHash {
			prepared.hashes = append(prepared.hashes, primaryUnifiedOverlayBatchHash{
				slot: headSlot, head: recordIndex + 1,
			})
		}
		keys = append(keys, primaryUnifiedOverlayBatchKey{
			bucket: mutation.bucket, hash: mutation.hash,
			key: mutation.key, wide: mutation.reservationWide,
		})
	}
	for index := range prepared.buckets {
		bucket := &prepared.buckets[index]
		if bucket.wideKeys != 0 {
			bucket.reservedBytes = uint32(uint64(o.maxLeafBytes) + uint64(o.parentBytes))
		} else {
			bucket.reservedBytes = bucket.fixedBytes
		}
		if currentDirty < uint64(bucket.baseReserved) {
			return primaryUnifiedOverlayBatchPrepared{},
				storeio.ErrCommonPrimaryLeafCorrupt
		}
		remaining := currentDirty - uint64(bucket.baseReserved)
		if remaining > ^uint64(0)-uint64(bucket.reservedBytes) {
			return primaryUnifiedOverlayBatchPrepared{}, storeio.ErrInvalidWrite
		}
		currentDirty = remaining + uint64(bucket.reservedBytes)
	}
	if currentDirty > o.dirtyByteLimit {
		return primaryUnifiedOverlayBatchPrepared{}, fmt.Errorf(
			"%w: unified row overlay dirty-byte pressure",
			storeio.ErrPageCachePinned,
		)
	}
	prepared.dirtyAfter = currentDirty
	prepared.live = true
	return prepared, nil
}

// abortBatch abandons a private record/arena suffix. The published prefixes
// remain untouched, so no reader, recovery journal, or later fold can observe
// the abandoned records. The next writer may reuse the exact suffix.
func (o *primaryUnifiedOverlay) abortBatch(
	prepared *primaryUnifiedOverlayBatchPrepared,
) {
	if prepared == nil || !prepared.live {
		return
	}
	*prepared = primaryUnifiedOverlayBatchPrepared{}
}

// publishBatch installs one prepared same-generation batch. All bucket
// aggregates and the global count/arena cursor are written before their bucket
// heads, and the hash heads are last. This keeps both bucket-chain and hash-chain
// readers from reaching a partially linked batch.
func (o *primaryUnifiedOverlay) publishBatch(
	prepared *primaryUnifiedOverlayBatchPrepared,
) {
	if o == nil || prepared == nil || !prepared.live ||
		len(prepared.records) == 0 {
		return
	}
	for index := range prepared.buckets {
		state := &prepared.buckets[index]
		bucket := &o.buckets[state.slot]
		if state.newBucket {
			bucket.bucket.Store(state.bucket)
			for slot := range bucket.insertSlots {
				bucket.insertSlots[slot].Store(0)
			}
			o.bucketCount.Add(1)
		}
		bucket.fixedBytes.Store(state.fixedBytes)
		bucket.wideKeys.Store(state.wideKeys)
		bucket.reservedBytes.Store(state.reservedBytes)
		bucket.rawBytes.Store(state.rawBytes)
		bucket.rows.Store(state.rows)
		for slot := range bucket.insertSlots {
			bucket.insertSlots[slot].Store(state.insertSlots[slot])
		}
		bucket.scalarBase = state.scalarBase
		bucket.scalarShape = state.scalarShape
		bucket.scalarHole = state.scalarHole
		bucket.scalarFixedBytes = state.scalarFixedBytes
		bucket.scalarSet = state.scalarSet
	}
	o.dirtyBytes.Store(prepared.dirtyAfter)
	o.used.Store(prepared.usedAfter)
	o.count.Store(prepared.countAfter)
	// Count and arena usage become visible before any bucket head points at the
	// new suffix. Bucket-chain readers validate their record keys against this
	// published prefix, so a head cannot expose a record while the prefix still
	// advertises the previous generation's length.
	for index := range prepared.buckets {
		state := &prepared.buckets[index]
		o.buckets[state.slot].head.Store(state.head)
	}
	for index := range prepared.hashes {
		hash := &prepared.hashes[index]
		o.heads[hash.slot].Store(hash.head)
	}
	prepared.live = false
}

func (o *primaryUnifiedOverlay) publish(prepared primaryUnifiedOverlayPrepared) {
	record := &o.records[prepared.index]
	bucket := &o.buckets[prepared.bucketSlot]
	if prepared.newBucket {
		bucket.bucket.Store(record.bucket)
		bucket.reservedBytes.Store(0)
		bucket.fixedBytes.Store(0)
		bucket.wideKeys.Store(0)
		bucket.rawBytes.Store(0)
		bucket.rows.Store(0)
		for i := range bucket.insertSlots {
			bucket.insertSlots[i].Store(0)
		}
		o.bucketCount.Add(1)
	}
	// prepared was computed from the immediately preceding published state by
	// the serialized overlay publisher. Store the exact final-state reservation,
	// including a safe downgrade when the last wide key is restored.
	bucket.fixedBytes.Store(prepared.fixedBytes)
	bucket.wideKeys.Store(prepared.wideKeys)
	bucket.reservedBytes.Store(prepared.reservedBytes)
	o.dirtyBytes.Store(prepared.dirtyAfter)
	bucket.rawBytes.Add(int64(record.rawDelta))
	bucket.rows.Add(int32(record.countDelta))
	if record.countDelta > 0 {
		bucket.insertSlots[record.slot>>6].Or(
			uint64(1) << uint(record.slot&63),
		)
	}
	// Publish the bucket chain before the global count/hash words. A reader
	// that reaches it before the collection state advances filters the future
	// generation; after state publication every directory aggregate is ready.
	bucket.head.Store(prepared.index + 1)
	o.used.Store(prepared.usedAfter)
	o.count.Store(prepared.index + 1)
	o.heads[prepared.headSlot].Store(prepared.index + 1)
}

// pendingBuckets returns one representative key for each bucket dirtied since
// the last fold. The returned slices borrow the overlay arena.
func (o *primaryUnifiedOverlay) pendingBuckets(
	buckets *[primaryUnifiedOverlayBuckets]storeio.BucketID,
	keys *[primaryUnifiedOverlayBuckets][]byte,
) (int, error) {
	if o == nil {
		return 0, nil
	}
	folded := o.folded.Load()
	n := 0
	for i := range o.buckets {
		head := o.buckets[i].head.Load()
		if head == 0 || int(head) > len(o.records) {
			continue
		}
		record := &o.records[head-1]
		if record.generation <= folded {
			continue
		}
		if n == len(buckets) {
			return 0, fmt.Errorf(
				"%w: unified row overlay bucket pressure",
				storeio.ErrPageCachePinned,
			)
		}
		end := record.keyOffset + uint32(record.keyLen)
		if end > uint32(len(o.arena)) {
			return 0, storeio.ErrCommonPrimaryLeafCorrupt
		}
		buckets[n] = storeio.BucketID(record.bucket)
		keys[n] = o.arena[record.keyOffset:end:end]
		n++
	}
	return n, nil
}

// applyBucket coalesces this bucket to its newest visible record per key, then
// applies that final state to the lexical base rows. Deletes run first so an
// otherwise-full base can make room for same-window inserts without retaining
// the raw generation history on the stack.
func (o *primaryUnifiedOverlay) applyBucket(
	records []storeio.CommonPrimaryLeafRecord,
	bucket storeio.BucketID,
	generation uint64,
) ([]storeio.CommonPrimaryLeafRecord, error) {
	var indexes [storeio.CommonPrimaryLeafWideSlots]uint16
	n, err := o.latestBucketRecords(&indexes, bucket, generation)
	if err != nil {
		return records, err
	}
	for _, phase := range [...]uint8{
		primaryUnifiedOverlayDelete,
		primaryUnifiedOverlayPut,
	} {
		for _, index := range indexes[:n] {
			record := &o.records[index]
			if record.kind != phase {
				continue
			}
			keyEnd := record.keyOffset + uint32(record.keyLen)
			if keyEnd > uint32(len(o.arena)) {
				return records, storeio.ErrCommonPrimaryLeafCorrupt
			}
			key := o.arena[record.keyOffset:keyEnd:keyEnd]
			lo, hi := 0, len(records)
			for lo < hi {
				mid := int(uint(lo+hi) >> 1)
				if bytes.Compare(records[mid].Key, key) < 0 {
					lo = mid + 1
				} else {
					hi = mid
				}
			}
			found := lo < len(records) && bytes.Equal(records[lo].Key, key)
			switch record.kind {
			case primaryUnifiedOverlayPut:
				valueEnd := record.valueOff + record.valueLen
				if valueEnd > uint32(len(o.arena)) || record.valueLen == 0 {
					return records, storeio.ErrCommonPrimaryLeafCorrupt
				}
				row := storeio.CommonPrimaryLeafRecord{
					Slot: record.slot,
					Key:  key,
					Value: storeio.CommonPrimaryLeafValue{
						Inline: o.arena[record.valueOff:valueEnd:valueEnd],
					},
				}
				if found {
					records[lo] = row
					continue
				}
				if len(records) == cap(records) {
					return records, storeio.ErrCommonPrimaryLeafFull
				}
				records = records[:len(records)+1]
				copy(records[lo+1:], records[lo:len(records)-1])
				records[lo] = row
			case primaryUnifiedOverlayDelete:
				// Insert -> Delete has no base row but is a valid coalesced final
				// absence. A base-backed tombstone removes the existing row.
				if !found {
					continue
				}
				copy(records[lo:], records[lo+1:])
				records[len(records)-1] = storeio.CommonPrimaryLeafRecord{}
				records = records[:len(records)-1]
			default:
				return records, storeio.ErrCommonPrimaryLeafCorrupt
			}
		}
	}
	return records, nil
}

// latestBucketRecordsUnordered collects the newest visible overlay record per
// stable slot for one bucket. Its compact output is in slot order, not lexical
// order. A stable slot belongs to one key throughout an overlay window, so the
// caller-owned 256-entry dst doubles as an O(1) slot-index table during the
// history walk. The native replacement patcher accepts arbitrary input order
// and independently proves each key's admitted rank and slot, avoiding an
// unnecessary quadratic sort on its hot fold path.
func (o *primaryUnifiedOverlay) latestBucketRecordsUnordered(
	dst *[storeio.CommonPrimaryLeafWideSlots]uint16,
	bucket storeio.BucketID,
	generation uint64,
) (int, error) {
	if o == nil || dst == nil {
		return 0, nil
	}
	folded := o.folded.Load()
	bucketSlot, found := o.bucketSlot(bucket)
	if !found {
		return 0, nil
	}
	return o.latestBucketRecordsUnorderedFromHead(
		dst, bucket, o.buckets[bucketSlot].head.Load(), folded, generation,
	)
}

// latestBucketRecordsUnorderedFromHead is the immutable-history form used by
// a current scan after it releases the structural fence. The scan captures
// head and folded with its visible generation; a later fold may clear the live
// bucket directory, but an active reader pin prevents recycling these records
// and their arena while this captured chain is traversed.
func (o *primaryUnifiedOverlay) latestBucketRecordsUnorderedFromHead(
	dst *[storeio.CommonPrimaryLeafWideSlots]uint16,
	bucket storeio.BucketID,
	head uint32,
	afterGeneration, generation uint64,
) (int, error) {
	if o == nil || dst == nil || head == 0 || generation <= afterGeneration {
		return 0, nil
	}
	count := int(o.count.Load())
	if count > len(o.records) {
		return 0, storeio.ErrCommonPrimaryLeafCorrupt
	}
	used := o.used.Load()
	if used > uint32(len(o.arena)) {
		return 0, storeio.ErrCommonPrimaryLeafCorrupt
	}
	var seenSlots [storeio.CommonPrimaryLeafWideSlots / 64]uint64
	n := 0
	newerGeneration := uint64(0)
	for head != 0 {
		// publish exposes the bucket head before count/used, while the owning
		// collection generation remains old. A snapshot can therefore encounter
		// one completely initialized future record beyond those two watermarks.
		// Validate it against immutable backing storage, but require the tighter
		// published watermarks before accepting any record visible at generation.
		if head > uint32(len(o.records)) {
			return 0, storeio.ErrCommonPrimaryLeafCorrupt
		}
		index := head - 1
		record := &o.records[index]
		if record.bucket != uint32(bucket) {
			return 0, storeio.ErrCommonPrimaryLeafCorrupt
		}
		// A checkpoint-group batch publishes several immutable records at one
		// generation. Equal generations are valid while record indexes in the
		// bucket chain must still decrease strictly.
		if record.generation == 0 ||
			newerGeneration != 0 && record.generation > newerGeneration {
			return 0, storeio.ErrCommonPrimaryLeafCorrupt
		}
		newerGeneration = record.generation
		if record.generation <= afterGeneration {
			break
		}
		next := record.bucketPrevious
		if next >= head && next != 0 {
			return 0, storeio.ErrCommonPrimaryLeafCorrupt
		}
		head = next
		keyEnd := uint64(record.keyOffset) + uint64(record.keyLen)
		valueEnd := uint64(record.valueOff) + uint64(record.valueLen)
		if record.keyLen == 0 || keyEnd > uint64(len(o.arena)) ||
			uint64(record.valueOff) != keyEnd ||
			valueEnd > uint64(len(o.arena)) ||
			record.kind == primaryUnifiedOverlayPut &&
				record.valueLen == 0 ||
			record.kind == primaryUnifiedOverlayDelete && record.valueLen != 0 ||
			record.kind != primaryUnifiedOverlayPut &&
				record.kind != primaryUnifiedOverlayDelete {
			return 0, storeio.ErrCommonPrimaryLeafCorrupt
		}
		if record.generation > generation {
			continue
		}
		if index >= uint32(count) || valueEnd > uint64(used) {
			return 0, storeio.ErrCommonPrimaryLeafCorrupt
		}
		key := o.arena[record.keyOffset:uint32(keyEnd):uint32(keyEnd)]
		slot := int(record.slot)
		word := slot >> 6
		bit := uint64(1) << uint(slot&63)
		if seenSlots[word]&bit != 0 {
			other := &o.records[dst[slot]]
			otherEnd := uint64(other.keyOffset) + uint64(other.keyLen)
			if other.keyLen == 0 || otherEnd > uint64(used) {
				return 0, storeio.ErrCommonPrimaryLeafCorrupt
			}
			if !bytes.Equal(
				key,
				o.arena[other.keyOffset:uint32(otherEnd):uint32(otherEnd)],
			) {
				return 0, storeio.ErrCommonPrimaryLeafCorrupt
			}
			continue
		}
		if n == len(dst) {
			return 0, storeio.ErrCommonPrimaryLeafCorrupt
		}
		seenSlots[word] |= bit
		dst[slot] = uint16(index)
		n++
	}
	// Compact the slot-index table in place. The output index never advances
	// beyond the slot currently being read, so writing the dense prefix cannot
	// overwrite an unseen slot's saved record index.
	n = 0
	for slot := range len(dst) {
		if seenSlots[slot>>6]&(uint64(1)<<uint(slot&63)) == 0 {
			continue
		}
		index := dst[slot]
		dst[n] = index
		n++
	}
	return n, nil
}

// latestBucketRecords returns the unordered slot census in lexical key order
// for merge/scan callers. Sorting also makes duplicate keys assigned to
// distinct slots adjacent, preserving the full corruption check for every
// structural consumer.
func (o *primaryUnifiedOverlay) latestBucketRecords(
	dst *[storeio.CommonPrimaryLeafWideSlots]uint16,
	bucket storeio.BucketID,
	generation uint64,
) (int, error) {
	n, err := o.latestBucketRecordsUnordered(dst, bucket, generation)
	if err != nil || n == 0 {
		return n, err
	}
	return o.sortLatestBucketRecords(dst, n)
}

func (o *primaryUnifiedOverlay) latestBucketRecordsFromHead(
	dst *[storeio.CommonPrimaryLeafWideSlots]uint16,
	bucket storeio.BucketID,
	head uint32,
	afterGeneration, generation uint64,
) (int, error) {
	n, err := o.latestBucketRecordsUnorderedFromHead(
		dst, bucket, head, afterGeneration, generation,
	)
	if err != nil || n == 0 {
		return n, err
	}
	return o.sortLatestBucketRecords(dst, n)
}

func (o *primaryUnifiedOverlay) sortLatestBucketRecords(
	dst *[storeio.CommonPrimaryLeafWideSlots]uint16,
	n int,
) (int, error) {
	const insertionSortLimit = 12
	if n <= insertionSortLimit {
		for i := 1; i < n; i++ {
			index := dst[i]
			record := &o.records[index]
			end := record.keyOffset + uint32(record.keyLen)
			key := o.arena[record.keyOffset:end:end]
			at := i
			for at > 0 {
				previous := &o.records[dst[at-1]]
				previousEnd := previous.keyOffset + uint32(previous.keyLen)
				previousKey :=
					o.arena[previous.keyOffset:previousEnd:previousEnd]
				if bytes.Compare(previousKey, key) <= 0 {
					break
				}
				dst[at] = dst[at-1]
				at--
			}
			dst[at] = index
		}
	} else {
		slices.SortFunc(dst[:n], func(a, b uint16) int {
			left := &o.records[a]
			leftEnd := left.keyOffset + uint32(left.keyLen)
			right := &o.records[b]
			rightEnd := right.keyOffset + uint32(right.keyLen)
			return bytes.Compare(
				o.arena[left.keyOffset:leftEnd:leftEnd],
				o.arena[right.keyOffset:rightEnd:rightEnd],
			)
		})
	}
	// Stable-slot identity catches the common corruption case during the history
	// walk. Sorting makes the converse invariant equally cheap to verify: the
	// same key may not be assigned to two distinct stable slots.
	for i := 1; i < n; i++ {
		previous := &o.records[dst[i-1]]
		previousEnd := previous.keyOffset + uint32(previous.keyLen)
		current := &o.records[dst[i]]
		currentEnd := current.keyOffset + uint32(current.keyLen)
		if bytes.Equal(
			o.arena[previous.keyOffset:previousEnd:previousEnd],
			o.arena[current.keyOffset:currentEnd:currentEnd],
		) {
			return 0, storeio.ErrCommonPrimaryLeafCorrupt
		}
	}
	return n, nil
}

// checkpointEntries exposes every raw overlay publication in
// (afterGeneration, targetGeneration] in generation order as one recovery
// journal batch. It deliberately does not coalesce repeated keys: the ordinary
// mutation path advances the public generation once per entry during replay, so
// retaining one entry per logical generation makes recovery land exactly on the
// flushed target even for Put→Delete→Put windows.
//
// complete is true only when the interval contains exactly one overlay record
// for each consecutive generation. A structural, overflow, batch, snapshot-COW
// or online-index publication therefore creates a gap (or duplicate generation)
// and forces the caller onto the physical root-fold checkpoint.
func (o *primaryUnifiedOverlay) checkpointEntries(
	dst []storeio.RecoveryBatchEntry,
	afterGeneration, targetGeneration uint64,
) (entries []storeio.RecoveryBatchEntry, complete bool, err error) {
	// Compact fold certificates are
	// relative to the immutable routed leaf, whereas journal entries replay
	// sequentially against the preceding logical value. Reusing a base-relative
	// certificate after another same-key overlay mutation would splice the wrong
	// preimage, so the current journal delta always emits complete Put/Delete
	// entries.
	dst = dst[:0]
	if o == nil || targetGeneration <= afterGeneration {
		return dst, targetGeneration == afterGeneration, nil
	}
	count := min(int(o.count.Load()), len(o.records))
	expected := afterGeneration + 1
	for i := 0; i < count; i++ {
		record := &o.records[i]
		if record.generation <= afterGeneration {
			continue
		}
		if record.generation > targetGeneration {
			break
		}
		if record.generation != expected || len(dst) == cap(dst) {
			return dst[:0], false, nil
		}
		keyEnd := record.keyOffset + uint32(record.keyLen)
		valueEnd := record.valueOff + record.valueLen
		if record.keyLen == 0 ||
			keyEnd > uint32(len(o.arena)) ||
			valueEnd > uint32(len(o.arena)) {
			return dst[:0], false, storeio.ErrCommonPrimaryLeafCorrupt
		}
		entry := storeio.RecoveryBatchEntry{
			Key: o.arena[record.keyOffset:keyEnd:keyEnd],
		}
		switch record.kind {
		case primaryUnifiedOverlayPut:
			if record.valueLen == 0 {
				return dst[:0], false, storeio.ErrCommonPrimaryLeafCorrupt
			}
			entry.Kind = storeio.RecoveryRecordKindPut
			entry.Value = o.arena[record.valueOff:valueEnd:valueEnd]
		case primaryUnifiedOverlayDelete:
			if record.valueLen != 0 {
				return dst[:0], false, storeio.ErrCommonPrimaryLeafCorrupt
			}
			entry.Kind = storeio.RecoveryRecordKindDelete
		default:
			return dst[:0], false, storeio.ErrCommonPrimaryLeafCorrupt
		}
		dst = append(dst, entry)
		expected++
	}
	return dst, expected == targetGeneration+1, nil
}

func (o *primaryUnifiedOverlay) markFolded(generation uint64, recycle bool) {
	if o == nil {
		return
	}
	o.folded.Store(generation)
	// The directory describes only records newer than folded. Clear it even
	// when an old epoch reader pins the global record/hash chains; those chains
	// retain the old reader's history while a later window starts from an empty
	// per-bucket directory.
	for i := range o.buckets {
		bucket := &o.buckets[i]
		if bucket.head.Load() == 0 {
			continue
		}
		bucket.head.Store(0)
		bucket.reservedBytes.Store(0)
		bucket.fixedBytes.Store(0)
		bucket.wideKeys.Store(0)
		bucket.rawBytes.Store(0)
		bucket.rows.Store(0)
		for slot := range bucket.insertSlots {
			bucket.insertSlots[slot].Store(0)
		}
		bucket.scalarBase = storeio.PageRef{}
		bucket.scalarShape = 0
		bucket.scalarHole = 0
		bucket.scalarFixedBytes = 0
		bucket.scalarSet = false
	}
	o.bucketCount.Store(0)
	o.dirtyBytes.Store(0)
	if !recycle {
		return
	}
	count := o.count.Load()
	// Only hash slots reached by a sparse record window can be nonzero. Avoid
	// making a small CP64 recycle pay 65,536 unrelated atomic stores; above the
	// measured locality crossover, retain the bounded sequential sweep rather
	// than scattering one store per history record. Duplicate sparse slots are
	// harmless. Records are pointer-free values overwritten completely before
	// publication, so their stale bytes never need a bulk clear.
	if count <= primaryUnifiedOverlayRecords/4 {
		for i := range count {
			record := &o.records[i]
			o.heads[record.hash&(primaryUnifiedOverlayTable-1)].Store(0)
		}
	} else {
		for i := range o.heads {
			o.heads[i].Store(0)
		}
	}
	o.count.Store(0)
	o.used.Store(0)
	o.folded.Store(0)
}

func (c *Collection) recyclePrimaryUnifiedOverlayIfSafe() {
	overlay := c.primaryUnifiedOverlay
	if overlay == nil || overlay.count.Load() == 0 || overlay.hasPending() {
		return
	}
	c.snapshotGate.Lock()
	c.beginReaderFence()
	if !c.anyActiveReaders() {
		overlay.markFolded(overlay.folded.Load(), true)
	}
	c.endReaderFence()
	c.snapshotGate.Unlock()
}

type primaryUnifiedOverlayStats struct {
	capacityBytes     uint64
	arenaBytes        uint64
	logicalDirtyBytes uint64
	reservedFoldBytes uint64
	retainedRecords   uint64
	dirtyBuckets      uint64
	dirtyBucketLimit  uint64
	dirtyByteLimit    uint64
}

func (o *primaryUnifiedOverlay) stats() primaryUnifiedOverlayStats {
	if o == nil {
		return primaryUnifiedOverlayStats{}
	}
	resident := uint64(o.used.Load())
	result := primaryUnifiedOverlayStats{
		capacityBytes:     uint64(o.capacityBytes()),
		arenaBytes:        resident,
		retainedRecords:   uint64(o.count.Load()),
		dirtyBuckets:      uint64(o.bucketCount.Load()),
		reservedFoldBytes: o.dirtyBytes.Load(),
		dirtyBucketLimit:  uint64(o.bucketLimit),
		dirtyByteLimit:    o.dirtyByteLimit,
	}
	if o.hasPending() {
		// DirtyBytes is cache-style logical residency. The conservative physical
		// fold reservation is exported separately as dirtyBytes.
		result.logicalDirtyBytes = resident
	}
	return result
}

// canonicalPrimaryMutationValue returns the one logical representation exposed
// by compact VCS1 reads. It is shared by the overlay and exceptional structural
// lanes so an overflow row, reader-forced COW, or async mutation cannot retain
// a different byte spelling from the ordinary inline fast path.
func (c *Collection) canonicalPrimaryMutationValue(src []byte) ([]byte, error) {
	if c != nil && c.options.OpaqueValues {
		return src, nil
	}
	estimate := max(64, len(src)/8+8)
	for cap(c.primaryUnifiedIndexScratch) < estimate {
		c.primaryUnifiedIndexScratch = make([]vibejson.IndexEntry, 0, estimate)
	}
	var index vibejson.Index
	for {
		var err error
		index, err = vibejson.BuildIndexOptions(
			src, c.primaryUnifiedIndexScratch[:cap(c.primaryUnifiedIndexScratch)],
			c.options.Collection.IndexOptions,
		)
		if !errors.Is(err, document.ErrIndexFull) {
			if err != nil {
				return nil, err
			}
			break
		}
		estimate = 2 * cap(c.primaryUnifiedIndexScratch)
		c.primaryUnifiedIndexScratch = make([]vibejson.IndexEntry, 0, estimate)
	}
	c.primaryUnifiedIndexScratch = index.Entries
	if storeio.IndexIsCanonical(index, &c.primaryUnifiedCanonicalWS) {
		return src, nil
	}
	if cap(c.primaryUnifiedCanonical) < len(src) {
		c.primaryUnifiedCanonical = make([]byte, 0, len(src))
	}
	canonicalScratch := c.primaryUnifiedCanonical[:0]
	out, err := storeio.AppendCanonicalIndexed(
		canonicalScratch, index,
		&c.primaryUnifiedCanonicalWS,
	)
	if err != nil {
		return nil, err
	}
	c.primaryUnifiedCanonical = out
	return out, nil
}

// canonicalPrimaryUnifiedOverlayValue applies the overlay's bounded inline
// admission. canonicalReady is the schemaless Put fast path's proof that src
// already came from canonicalPrimaryMutationValue; schema collections retain
// the old late-canonicalization path. Tape-dense or overflow values decline to
// the structural lane without repeating a parse for canonical-ready input.
func (c *Collection) canonicalPrimaryUnifiedOverlayValue(
	src []byte, canonicalReady bool,
) ([]byte, bool, error) {
	if c == nil || c.primaryUnifiedOverlay == nil ||
		len(src) > c.options.InlineValueBytes {
		return nil, false, nil
	}
	if canonicalReady {
		return src, true, nil
	}
	canonical, err := c.canonicalPrimaryMutationValue(src)
	if err != nil {
		return nil, false, err
	}
	if len(canonical) > c.options.InlineValueBytes {
		return nil, false, nil
	}
	return canonical, true, nil
}

// tryPrimaryUnifiedOverlayPut publishes one inline compact-stripe insert or
// replacement as a bounded delta record.
//
// pressure asks the caller to checkpoint and retry; handled means the logical
// mutation is already published.
func (c *Collection) tryPrimaryUnifiedOverlayPut(
	state *fileStoreState,
	route storeio.ResidentPrimaryRoute,
	page []byte,
	key, src []byte,
	canonicalReady bool,
) (handled, created, pressure bool, err error) {
	overlay := c.primaryUnifiedOverlay
	if overlay == nil ||
		storeio.PrimaryLeafClass(page) != storeio.CommonPrimaryLeafCompact {
		return false, false, false, nil
	}
	c.primaryUnifiedSeen = true
	c.recyclePrimaryUnifiedOverlayIfSafe()
	stripe, ok := storeio.AdmittedCompactPrimaryStripe(
		page, c.storeID, route.Bucket,
	)
	if !ok {
		return false, false, false, storeio.ErrCommonPrimaryLeafCorrupt
	}
	baseRank, baseFound := stripe.FindKey(key)
	largeUnindexed := stripe.Len() > storeio.CommonPrimaryLeafWideSlots ||
		!baseFound && stripe.Len() == storeio.CommonPrimaryLeafWideSlots &&
			state.root.IndexCount == 0
	if largeUnindexed && state.root.IndexCount != 0 {
		return false, false, false, nil
	}
	baseSlot, slotOK := uint8(0), largeUnindexed
	if !largeUnindexed {
		baseSlot, slotOK = stripe.PostingSlot(baseRank)
	}
	if baseFound && !slotOK {
		return false, false, false, storeio.ErrCommonPrimaryLeafCorrupt
	}
	current, disposition, overlaySlot := overlay.lookup(
		route.Bucket, route.Hash, key, state.root.Generation,
	)
	found := baseFound
	stableSlot := baseSlot
	oldLen := 0
	var oldRaw []byte
	switch disposition {
	case primaryUnifiedOverlayValue:
		found = true
		stableSlot = overlaySlot
		oldLen = len(current)
		oldRaw = current
	case primaryUnifiedOverlayDeleted:
		found = false
		stableSlot = overlaySlot
	case primaryUnifiedOverlayMissing:
		if baseFound {
			if _, overflow := stripe.OverflowRef(baseRank); overflow {
				return false, false, false, nil
			}
			var decoded bool
			c.overflowValueScratch, decoded = stripe.AppendValue(
				c.overflowValueScratch[:0], baseRank,
			)
			if !decoded {
				return false, false, false, storeio.ErrCommonPrimaryLeafCorrupt
			}
			oldLen = len(c.overflowValueScratch)
			oldRaw = c.overflowValueScratch
		}
	default:
		return false, false, false, storeio.ErrCommonPrimaryLeafCorrupt
	}
	if largeUnindexed && disposition == primaryUnifiedOverlayMissing {
		stableSlot, slotOK = overlay.chooseLargeUnindexedSlot(
			route.Bucket, route.Hash,
		)
		if !slotOK {
			return false, false, true, nil
		}
	}
	// The leaf header carries its exact no-compression envelope. Accumulating
	// each pending mutation's exact delta lets growing values and inserts stay
	// on the O(document) lane whenever an all-trivial fold remains bounded.
	canonical, eligible, err :=
		c.canonicalPrimaryUnifiedOverlayValue(src, canonicalReady)
	if err != nil || !eligible {
		return false, false, false, err
	}
	// The maximum physical extent cannot absorb an adverse change in compact
	// stream or dictionary encoding. Only a byte-identical immutable-base Put is
	// provably safe there. Route every other Put through the exact COW encoder
	// while this bucket has no pending delta; it can return the structural split
	// signal immediately instead of leaving Flush with an unsplittable overlay.
	if route.Ref.Length == storeio.CommonPrimaryLeafMaxExtentBytes &&
		(!baseFound || disposition != primaryUnifiedOverlayMissing ||
			!bytes.Equal(canonical, oldRaw)) {
		return false, false, false, nil
	}
	pendingRaw, pendingRows :=
		overlay.pendingBucketDeltas(route.Bucket)
	leafWasEmpty := stripe.Len()+pendingRows == 0
	countDelta := 0
	rawDelta := len(canonical) - oldLen
	if !found {
		countDelta = 1
		rawDelta = storeio.CommonPrimaryUnifiedInsertedTrivialBytes(
			key, len(canonical),
		)
		if rawDelta == 0 {
			return false, false, false, storeio.ErrInvalidWrite
		}
		if disposition == primaryUnifiedOverlayMissing && !largeUnindexed {
			var slotOK bool
			stableSlot, slotOK = stripe.ChooseInsertSlotHashed(
				route.Hash, overlay.pendingInsertSlots(route.Bucket),
			)
			if !slotOK {
				return false, false, false, nil
			}
		}
	}
	rowLimit := storeio.CommonPrimaryLeafWideSlots
	if largeUnindexed {
		rowLimit = storeio.CompactPrimaryStripeMaxRows
	}
	if stripe.Len()+pendingRows+countDelta > rowLimit ||
		stripe.EncodedPayloadBytes()+pendingRaw+rawDelta >
			storeio.CommonPrimaryLeafMaxExtentBytes-storeio.PageHeaderSize-storeio.PageTrailerSize {
		return false, false, false, nil
	}
	if state.root.Generation >= fileLogicalCutGenerationMask {
		return false, false, false, storeio.ErrGenerationOrder
	}
	if err := c.validatePrimaryUniquePut(
		canonical, found, route, stableSlot,
	); err != nil {
		return false, false, false, err
	}
	generation := state.root.Generation + 1
	prepared, err := overlay.prepare(
		route.Bucket, route.Hash, generation, key, canonical,
		rawDelta, countDelta, primaryUnifiedOverlayPut, stableSlot,
	)
	if err != nil {
		if !overlay.hasPending() {
			// Folded records may still be pinned by an old epoch reader. The
			// generation-aware lookup above makes them irrelevant to the current
			// base, so use the structural mutation lane rather than spinning on
			// an arena that cannot yet recycle.
			return false, false, false, nil
		}
		return false, false, true, nil
	}
	preparedExact, exactPressure, err :=
		c.preparePrimaryExactUnifiedMutation(
			route, oldRaw, canonical, false, found,
			stableSlot, stableSlot, generation,
		)
	if err != nil {
		return false, false, false, err
	}
	if exactPressure {
		return false, false, true, nil
	}
	// The journal-backed synchronous lane must make the canonical spelling
	// durable before the record becomes reader-visible. Buffered-visible adds
	// its grouped journal record after this function returns, exactly like the
	// existing COW publication.
	if err := c.journalBeforePublishLocked(
		false, generation, key, canonical,
	); err != nil {
		c.unwindPrimaryExactPrepared(&preparedExact)
		return false, false, false, err
	}
	nextRoot := state.root
	nextRoot.Generation = generation
	if !found {
		nextRoot.DocumentCount++
	}
	nextState := &fileStoreState{
		root: nextRoot, fileEnd: state.fileEnd,
		freeHead: state.freeHead,
	}

	c.snapshotGate.Lock()
	c.beginReaderFence()
	overlay.publish(prepared)
	c.installPrimaryExactResidentLocked(preparedExact)
	router := c.primaryRouter.Load()
	router.AdvanceGeneration(generation)
	if !found && leafWasEmpty && router.ClearEmpty(route) {
		c.removePrimaryEmptyLeaf()
	}
	c.pageValidator.update(nextState)
	c.publishFileState(nextState)
	c.endReaderFence()
	c.snapshotGate.Unlock()
	return true, !found, false, nil
}

// tryPrimaryUnifiedOverlayDelete publishes a compact-stripe tombstone as one
// bounded delta record.
func (c *Collection) tryPrimaryUnifiedOverlayDelete(
	state *fileStoreState,
	route storeio.ResidentPrimaryRoute,
	page []byte,
	key []byte,
) (handled, deleted, pressure bool, err error) {
	overlay := c.primaryUnifiedOverlay
	if overlay == nil ||
		storeio.PrimaryLeafClass(page) != storeio.CommonPrimaryLeafCompact {
		return false, false, false, nil
	}
	c.primaryUnifiedSeen = true
	c.recyclePrimaryUnifiedOverlayIfSafe()
	stripe, ok := storeio.AdmittedCompactPrimaryStripe(
		page, c.storeID, route.Bucket,
	)
	if !ok {
		return false, false, false, storeio.ErrCommonPrimaryLeafCorrupt
	}
	largeUnindexed := stripe.Len() > storeio.CommonPrimaryLeafWideSlots
	if largeUnindexed && state.root.IndexCount != 0 {
		return false, false, false, nil
	}
	baseRank, baseFound := stripe.FindKey(key)
	baseSlot, slotOK := uint8(0), largeUnindexed
	if !largeUnindexed {
		baseSlot, slotOK = stripe.PostingSlot(baseRank)
	}
	if baseFound && !slotOK {
		return false, false, false, storeio.ErrCommonPrimaryLeafCorrupt
	}
	current, disposition, overlaySlot := overlay.lookup(
		route.Bucket, route.Hash, key, state.root.Generation,
	)
	stableSlot := baseSlot
	oldLen := 0
	var oldRaw []byte
	switch disposition {
	case primaryUnifiedOverlayValue:
		stableSlot = overlaySlot
		oldLen = len(current)
		oldRaw = current
	case primaryUnifiedOverlayDeleted:
		return true, false, false, nil
	case primaryUnifiedOverlayMissing:
		if !baseFound {
			return true, false, false, nil
		}
		if _, overflow := stripe.OverflowRef(baseRank); overflow {
			return false, false, false, nil
		}
		var decoded bool
		c.overflowValueScratch, decoded = stripe.AppendValue(
			c.overflowValueScratch[:0], baseRank,
		)
		if !decoded {
			return false, false, false, storeio.ErrCommonPrimaryLeafCorrupt
		}
		oldLen = len(c.overflowValueScratch)
		oldRaw = c.overflowValueScratch
	default:
		return false, false, false, storeio.ErrCommonPrimaryLeafCorrupt
	}
	if largeUnindexed && disposition == primaryUnifiedOverlayMissing {
		stableSlot, slotOK = overlay.chooseLargeUnindexedSlot(
			route.Bucket, route.Hash,
		)
		if !slotOK {
			return false, false, true, nil
		}
	}
	pendingRaw, pendingRows :=
		overlay.pendingBucketDeltas(route.Bucket)
	rawDelta := -storeio.CommonPrimaryUnifiedInsertedTrivialBytes(key, oldLen)
	if rawDelta == 0 ||
		stripe.Len()+pendingRows-1 < 0 ||
		stripe.EncodedPayloadBytes()+pendingRaw+rawDelta < 0 {
		return false, false, false, nil
	}
	becomesEmpty := stripe.Len()+pendingRows-1 == 0
	if state.root.Generation >= fileLogicalCutGenerationMask {
		return false, false, false, storeio.ErrGenerationOrder
	}
	generation := state.root.Generation + 1
	prepared, err := overlay.prepare(
		route.Bucket, route.Hash, generation, key, nil,
		rawDelta, -1, primaryUnifiedOverlayDelete, stableSlot,
	)
	if err != nil {
		if !overlay.hasPending() {
			return false, false, false, nil
		}
		return false, false, true, nil
	}
	preparedExact, exactPressure, err :=
		c.preparePrimaryExactUnifiedMutation(
			route, oldRaw, nil, true, true,
			stableSlot, stableSlot, generation,
		)
	if err != nil {
		return false, false, false, err
	}
	if exactPressure {
		return false, false, true, nil
	}
	if state.root.DocumentCount == 0 {
		c.unwindPrimaryExactPrepared(&preparedExact)
		return false, false, false, storeio.ErrInvalidWrite
	}
	if err := c.journalBeforePublishLocked(
		true, generation, key, nil,
	); err != nil {
		c.unwindPrimaryExactPrepared(&preparedExact)
		return false, false, false, err
	}
	nextRoot := state.root
	nextRoot.Generation = generation
	nextRoot.DocumentCount--
	nextState := &fileStoreState{
		root: nextRoot, fileEnd: state.fileEnd,
		freeHead: state.freeHead,
	}

	c.snapshotGate.Lock()
	c.beginReaderFence()
	overlay.publish(prepared)
	c.installPrimaryExactResidentLocked(preparedExact)
	router := c.primaryRouter.Load()
	router.AdvanceGeneration(generation)
	if becomesEmpty && router.MarkEmpty(route) {
		c.primaryEmptyLeaves.Add(1)
	}
	c.pageValidator.update(nextState)
	c.publishFileState(nextState)
	c.endReaderFence()
	c.snapshotGate.Unlock()
	return true, true, false, nil
}

// preparePrimaryUnifiedOverlayParentsLocked converts the overlay's dirty
// bucket set into the parent-route descriptors the existing checkpoint
// materializer already knows how to rewrite. The leaf handle remains the
// durable base; the materializer detects the dirty bucket and encodes its
// merged compact VCS1 image directly into the transaction.
func (c *Collection) preparePrimaryUnifiedOverlayParentsLocked(
	state *fileStoreState,
) error {
	overlay := c.primaryUnifiedOverlay
	if overlay == nil || !overlay.hasPending() {
		return nil
	}
	var buckets [primaryUnifiedOverlayBuckets]storeio.BucketID
	var keys [primaryUnifiedOverlayBuckets][]byte
	count, err := overlay.pendingBuckets(&buckets, &keys)
	if err != nil {
		return err
	}
	router := c.primaryRouter.Load()
	if router == nil || router.Generation() != state.root.Generation {
		return storeio.ErrSegmentedTabletRouterCorrupt
	}
	for i := 0; i < count; i++ {
		if c.primaryPendingParentIndex(buckets[i]) >= 0 {
			continue
		}
		resident, ok := router.Route(keys[i])
		if !ok || resident.Bucket != buckets[i] {
			return storeio.ErrSegmentedTabletRouterCorrupt
		}
		var path filePrimaryMutationPath
		if err := c.acquirePrimaryRoutingPath(
			&path, state, keys[i], resident,
		); err != nil {
			return err
		}
		pending := filePrimaryPendingParentFromPath(resident, &path)
		pending.volatileRef = resident.Ref
		path.Release()
		c.primaryPendingParents = append(c.primaryPendingParents, pending)
	}
	return nil
}
