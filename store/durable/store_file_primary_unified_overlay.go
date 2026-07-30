package durable

import (
	"bytes"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/document"
)

const (
	primaryUnifiedOverlayRecords = 256
	primaryUnifiedOverlayBuckets = filePrimaryPendingParentLimit
	primaryUnifiedOverlayTable   = 512
	primaryUnifiedOverlayMax     = 1 << 20

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
	bucket     uint32
	keyOffset  uint32
	valueOff   uint32
	valueLen   uint32
	rawDelta   int32
	keyLen     uint16
	countDelta int8
	kind       uint8
	slot       uint8
	_          uint8
}

type primaryUnifiedOverlay struct {
	records []primaryUnifiedOverlayRecord
	heads   []atomic.Uint32
	arena   []byte

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
	index     uint32
	usedAfter uint32
	headSlot  uint32
}

func newPrimaryUnifiedOverlay(bytes int) *primaryUnifiedOverlay {
	if bytes <= 0 {
		return nil
	}
	return &primaryUnifiedOverlay{
		records: make([]primaryUnifiedOverlayRecord, primaryUnifiedOverlayRecords),
		heads:   make([]atomic.Uint32, primaryUnifiedOverlayTable),
		arena:   make([]byte, bytes),
	}
}

func primaryUnifiedOverlayBudget(
	residentBytes, transactionBytes uint64,
	pageSize, maxPageSize, maxKeyBytes, inlineValueBytes int,
) int {
	if residentBytes <= transactionBytes || pageSize <= 0 ||
		maxPageSize <= 0 || maxKeyBytes <= 0 || inlineValueBytes <= 0 {
		return 0
	}
	target := primaryUnifiedOverlayRecords * (maxKeyBytes + inlineValueBytes)
	target = min(target, primaryUnifiedOverlayMax)
	target = max(target, 64<<10)
	target = (target + pageSize - 1) / pageSize * pageSize
	// The physical PageCache must still retain the complete worst-case dirty
	// transaction plus one maximum extent. The overlay is carved only from
	// genuine surplus, so Options.ResidentBytes remains the total owned data
	// budget rather than becoming cache + hidden heap.
	if uint64(target)+transactionBytes+uint64(maxPageSize) > residentBytes {
		return 0
	}
	return target
}

func (o *primaryUnifiedOverlay) hasPending() bool {
	if o == nil {
		return false
	}
	count := o.count.Load()
	return count != 0 && o.records[count-1].generation > o.folded.Load()
}

func (o *primaryUnifiedOverlay) pendingBucket(bucket storeio.BucketID) bool {
	if o == nil {
		return false
	}
	folded := o.folded.Load()
	count := int(o.count.Load())
	for i := count - 1; i >= 0; i-- {
		record := &o.records[i]
		if record.generation <= folded {
			break
		}
		if record.bucket == uint32(bucket) {
			return true
		}
	}
	return false
}

// bucketVersion is the generation of the newest visible row-overlay record
// for bucket. It is a per-leaf reconciliation token: an unchanged durable
// PageRef is not enough to prove an online-index scan is current because
// class-5 replaces rows in this append-only overlay between checkpoints.
func (o *primaryUnifiedOverlay) bucketVersion(
	bucket storeio.BucketID, generation uint64,
) uint64 {
	if o == nil {
		return 0
	}
	folded := o.folded.Load()
	count := min(int(o.count.Load()), len(o.records))
	for i := count - 1; i >= 0; i-- {
		record := &o.records[i]
		if record.generation <= folded {
			break
		}
		if record.generation <= generation &&
			record.bucket == uint32(bucket) {
			return record.generation
		}
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
	count := int(o.count.Load())
	for i := count - 1; i >= 0; i-- {
		record := &o.records[i]
		if record.generation <= folded {
			break
		}
		if record.bucket == uint32(bucket) {
			rawBytes += int(record.rawDelta)
			rows += int(record.countDelta)
		}
	}
	return rawBytes, rows
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
	count := int(o.count.Load())
	for i := count - 1; i >= 0; i-- {
		record := &o.records[i]
		if record.generation <= folded {
			break
		}
		if record.bucket == uint32(bucket) && record.countDelta > 0 {
			occupied[record.slot>>6] |= uint64(1) << uint(record.slot&63)
		}
	}
	return occupied
}

func (o *primaryUnifiedOverlay) lookup(
	bucket storeio.BucketID, hash uint64, key []byte, generation uint64,
) ([]byte, primaryUnifiedOverlayDisposition, uint8) {
	if o == nil {
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
		countDelta < -1 || countDelta > 1 {
		return primaryUnifiedOverlayPrepared{}, fmt.Errorf(
			"%w: unified row overlay delta", storeio.ErrInvalidWrite,
		)
	}
	index := o.count.Load()
	used := o.used.Load()
	needed := uint64(len(key)) + uint64(len(value))
	if index >= uint32(len(o.records)) ||
		uint64(used)+needed > uint64(len(o.arena)) {
		return primaryUnifiedOverlayPrepared{}, fmt.Errorf(
			"%w: unified row overlay pressure", storeio.ErrPageCachePinned,
		)
	}
	if !o.pendingBucket(bucket) {
		var distinct int
		folded := o.folded.Load()
		for i := int(index) - 1; i >= 0; i-- {
			record := &o.records[i]
			if record.generation <= folded {
				break
			}
			first := true
			for j := i + 1; j < int(index); j++ {
				if o.records[j].generation > folded &&
					o.records[j].bucket == record.bucket {
					first = false
					break
				}
			}
			if first {
				distinct++
			}
		}
		if distinct >= primaryUnifiedOverlayBuckets {
			return primaryUnifiedOverlayPrepared{}, fmt.Errorf(
				"%w: unified row overlay bucket pressure",
				storeio.ErrPageCachePinned,
			)
		}
	}
	keyOff := used
	valueOff := keyOff + uint32(len(key))
	copy(o.arena[keyOff:valueOff], key)
	copy(o.arena[valueOff:valueOff+uint32(len(value))], value)
	slot := uint32(hash & (primaryUnifiedOverlayTable - 1))
	o.records[index] = primaryUnifiedOverlayRecord{
		generation: generation,
		hash:       hash,
		previous:   o.heads[slot].Load(),
		bucket:     uint32(bucket),
		keyOffset:  keyOff,
		valueOff:   valueOff,
		valueLen:   uint32(len(value)),
		rawDelta:   int32(rawDelta),
		keyLen:     uint16(len(key)),
		countDelta: int8(countDelta),
		kind:       kind,
		slot:       stableSlot,
	}
	return primaryUnifiedOverlayPrepared{
		index: index, usedAfter: valueOff + uint32(len(value)), headSlot: slot,
	}, nil
}

func (o *primaryUnifiedOverlay) publish(prepared primaryUnifiedOverlayPrepared) {
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
	count := int(o.count.Load())
	n := 0
	for i := 0; i < count; i++ {
		record := &o.records[i]
		if record.generation <= folded {
			continue
		}
		bucket := storeio.BucketID(record.bucket)
		seen := false
		for j := 0; j < n; j++ {
			if buckets[j] == bucket {
				seen = true
				break
			}
		}
		if seen {
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
		buckets[n] = bucket
		keys[n] = o.arena[record.keyOffset:end:end]
		n++
	}
	return n, nil
}

// applyBucket replays this bucket's overlay records in generation order onto
// the lexical base rows. Inserts and re-inserts carry their preselected stable
// slot; replacements retain it; deletes remove the logical row.
func (o *primaryUnifiedOverlay) applyBucket(
	records []storeio.CommonPrimaryLeafRecord,
	bucket storeio.BucketID,
	generation uint64,
) ([]storeio.CommonPrimaryLeafRecord, error) {
	folded := o.folded.Load()
	count := int(o.count.Load())
	for i := 0; i < count; i++ {
		record := &o.records[i]
		if record.generation <= folded ||
			record.generation > generation ||
			record.bucket != uint32(bucket) {
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
			if !found {
				return records, storeio.ErrCommonPrimaryLeafCorrupt
			}
			copy(records[lo:], records[lo+1:])
			records[len(records)-1] = storeio.CommonPrimaryLeafRecord{}
			records = records[:len(records)-1]
		default:
			return records, storeio.ErrCommonPrimaryLeafCorrupt
		}
	}
	return records, nil
}

// latestBucketRecords collects the newest visible overlay record per key for
// one bucket, sorted lexically by key. dst is caller-owned fixed storage so a
// masked scan can merge inserted rows with the base leaf without allocating.
func (o *primaryUnifiedOverlay) latestBucketRecords(
	dst *[primaryUnifiedOverlayRecords]uint16,
	bucket storeio.BucketID,
	generation uint64,
) int {
	if o == nil || dst == nil {
		return 0
	}
	folded := o.folded.Load()
	count := min(int(o.count.Load()), len(o.records))
	n := 0
	for i := count - 1; i >= 0; i-- {
		record := &o.records[i]
		if record.generation <= folded ||
			record.generation > generation ||
			record.bucket != uint32(bucket) {
			continue
		}
		keyEnd := record.keyOffset + uint32(record.keyLen)
		if keyEnd > uint32(len(o.arena)) {
			continue
		}
		key := o.arena[record.keyOffset:keyEnd:keyEnd]
		duplicate := false
		for at := 0; at < n; at++ {
			other := &o.records[dst[at]]
			otherEnd := other.keyOffset + uint32(other.keyLen)
			if otherEnd <= uint32(len(o.arena)) &&
				bytes.Equal(key, o.arena[other.keyOffset:otherEnd:otherEnd]) {
				duplicate = true
				break
			}
		}
		if duplicate {
			continue
		}
		dst[n] = uint16(i)
		n++
	}
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
	return n
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
	if !recycle {
		return
	}
	for i := range o.heads {
		o.heads[i].Store(0)
	}
	count := o.count.Load()
	clear(o.records[:count])
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

func (o *primaryUnifiedOverlay) stats() (capacity, resident, dirty uint64) {
	if o == nil {
		return 0, 0, 0
	}
	capacity = uint64(len(o.arena))
	resident = uint64(o.used.Load())
	if o.hasPending() {
		dirty = resident
	}
	return capacity, resident, dirty
}

// canonicalPrimaryMutationValue returns the one logical representation exposed
// by class-5 reads. It is shared by the overlay and exceptional structural
// lanes so an overflow row, reader-forced COW, or async mutation cannot retain
// a different byte spelling from the ordinary inline fast path.
func (c *Collection) canonicalPrimaryMutationValue(src []byte) ([]byte, error) {
	estimate := max(64, len(src)/8+8)
	for cap(c.primaryUnifiedIndexScratch) < estimate {
		c.primaryUnifiedIndexScratch = make([]vibejson.IndexEntry, 0, estimate)
	}
	var index vibejson.Index
	for {
		var err error
		index, err = vibejson.BuildIndex(
			src, c.primaryUnifiedIndexScratch[:cap(c.primaryUnifiedIndexScratch)],
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
// admission after canonicalization. Tape-dense or overflow values decline to
// the structural lane, which calls canonicalPrimaryMutationValue again and
// therefore preserves the same class-5 byte contract.
func (c *Collection) canonicalPrimaryUnifiedOverlayValue(
	src []byte,
) ([]byte, bool, error) {
	if c == nil || c.primaryUnifiedOverlay == nil ||
		len(src) > c.options.InlineValueBytes {
		return nil, false, nil
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

// tryPrimaryUnifiedOverlayPut publishes one inline class-5 insert or replacement
// as an O(row) overlay record. A new row claims its final stable envelope slot
// up front, so a later fold preserves the same placement without a leaf-wide
// re-placement pass.
//
// pressure asks the caller to checkpoint and retry; handled means the logical
// mutation is already published.
func (c *Collection) tryPrimaryUnifiedOverlayPut(
	state *fileStoreState,
	route storeio.ResidentPrimaryRoute,
	page []byte,
	key, src []byte,
) (handled, created, pressure bool, err error) {
	overlay := c.primaryUnifiedOverlay
	if overlay == nil ||
		storeio.PrimaryLeafClass(page) != storeio.CommonPrimaryLeafUnified {
		return false, false, false, nil
	}
	c.primaryUnifiedSeen = true
	c.recyclePrimaryUnifiedOverlayIfSafe()
	uv, ok := storeio.AdmittedCommonPrimaryUnifiedLeaf(
		page, c.storeID, route.Bucket, c.primaryLeafBounds(state),
	)
	if !ok {
		return false, false, false, storeio.ErrCommonPrimaryLeafCorrupt
	}
	baseSlot, body, overflow, baseFound :=
		uv.LookupBodySlotHashed(route.Hash, key)
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
		overflow = false
	case primaryUnifiedOverlayMissing:
		if baseFound {
			if overflow {
				return false, false, false, nil
			}
			old := uv.AppendAdmittedRowBody(c.primaryUnifiedCanonical[:0], body)
			oldLen = len(old)
			c.overflowValueScratch = append(
				c.overflowValueScratch[:0], old...,
			)
			oldRaw = c.overflowValueScratch
		}
	default:
		return false, false, false, storeio.ErrCommonPrimaryLeafCorrupt
	}
	// The leaf header carries its exact no-compression envelope. Accumulating
	// each pending mutation's exact delta lets growing values and inserts stay
	// on the O(document) lane whenever an all-trivial fold remains bounded.
	canonical, eligible, err := c.canonicalPrimaryUnifiedOverlayValue(src)
	if err != nil || !eligible {
		return false, false, false, err
	}
	pendingRaw, pendingRows :=
		overlay.pendingBucketDeltas(route.Bucket)
	leafWasEmpty := uv.Len()+pendingRows == 0
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
		if disposition == primaryUnifiedOverlayMissing {
			var slotOK bool
			stableSlot, slotOK = uv.ChooseInsertSlotHashed(
				route.Hash, overlay.pendingInsertSlots(route.Bucket),
			)
			if !slotOK {
				return false, false, false, nil
			}
		}
	}
	if !storeio.CommonPrimaryUnifiedTrivialFits(
		uv.Len()+pendingRows+countDelta,
		uv.TrivialContentBytes()+pendingRaw+rawDelta,
	) {
		return false, false, false, nil
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
	nextSuper := state.super
	nextSuper.Generation = generation
	nextState := &fileStoreState{
		root: nextRoot, super: nextSuper, freeHead: state.freeHead,
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

// tryPrimaryUnifiedOverlayDelete publishes an inline class-5 tombstone as one
// O(key) overlay record. Tombstones keep the removed row's stable slot so a
// same-window reinsert can reclaim it without leaf-wide placement work.
func (c *Collection) tryPrimaryUnifiedOverlayDelete(
	state *fileStoreState,
	route storeio.ResidentPrimaryRoute,
	page []byte,
	key []byte,
) (handled, deleted, pressure bool, err error) {
	overlay := c.primaryUnifiedOverlay
	if overlay == nil ||
		storeio.PrimaryLeafClass(page) != storeio.CommonPrimaryLeafUnified {
		return false, false, false, nil
	}
	c.primaryUnifiedSeen = true
	c.recyclePrimaryUnifiedOverlayIfSafe()
	uv, ok := storeio.AdmittedCommonPrimaryUnifiedLeaf(
		page, c.storeID, route.Bucket, c.primaryLeafBounds(state),
	)
	if !ok {
		return false, false, false, storeio.ErrCommonPrimaryLeafCorrupt
	}
	baseSlot, body, overflow, baseFound :=
		uv.LookupBodySlotHashed(route.Hash, key)
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
		if overflow {
			// Overflow retirement remains on the structural lane.
			return false, false, false, nil
		}
		old := uv.AppendAdmittedRowBody(c.primaryUnifiedCanonical[:0], body)
		oldLen = len(old)
		c.overflowValueScratch = append(
			c.overflowValueScratch[:0], old...,
		)
		oldRaw = c.overflowValueScratch
	default:
		return false, false, false, storeio.ErrCommonPrimaryLeafCorrupt
	}
	pendingRaw, pendingRows :=
		overlay.pendingBucketDeltas(route.Bucket)
	rawDelta := -storeio.CommonPrimaryUnifiedInsertedTrivialBytes(key, oldLen)
	if rawDelta == 0 ||
		!storeio.CommonPrimaryUnifiedTrivialFits(
			uv.Len()+pendingRows-1,
			uv.TrivialContentBytes()+pendingRaw+rawDelta,
		) {
		return false, false, false, nil
	}
	becomesEmpty := uv.Len()+pendingRows-1 == 0
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
	nextSuper := state.super
	nextSuper.Generation = generation
	nextState := &fileStoreState{
		root: nextRoot, super: nextSuper, freeHead: state.freeHead,
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
// merged class-5 image directly into the transaction.
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
