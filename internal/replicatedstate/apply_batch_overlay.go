package replicatedstate

import (
	"bytes"
	"crypto/sha256"
	"hash/maphash"
	"slices"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/store/durable"
)

const (
	// A large base document is useful only while planning the current run. Keep
	// the warmed small-command shape, but do not pin command-independent table
	// bytes in every Machine after one cold large-row transition.
	maxNormalBatchRetainedBufferBytes = 64 << 10
	// Structural scratch is independent of the final durable mutation count: a
	// hostile run can churn thousands of logical keys back to their base values.
	// Retain enough for ordinary warmed batches, but do not permanently charge a
	// shard for a one-off maximum command run.
	// The logical bound is 2*MaxNormalApplyBatchEntries+1. Go's slice growth
	// rounds that ordinary 257-entry system shape to a 512-entry backing array,
	// so the retention gate is expressed in capacity classes.
	maxNormalBatchRetainedOverlayEntries = 4 * raftmodel.MaxNormalApplyBatchEntries
	maxNormalBatchRetainedOverlaySlots   = 4 * raftmodel.MaxNormalApplyBatchEntries
)

// One process-secret seed protects every binary overlay from adversarial
// replicated keys constructing deterministic probe chains. Fingerprint
// collisions are still resolved by exact binary-key comparison.
var logicalOverlayHashSeed = maphash.MakeSeed()

// logicalOverlay is the bounded binary-key planning view for one committed
// apply run. It uses open addressing over offsets into one reusable byte arena:
// no []byte-to-string conversion participates in lookup or ownership. The
// first mutation of a key retains base existence plus equality metadata. One
// reusable probe compares a final put with the pinned base so the durable batch
// can omit a sequence of logical writes whose net image equals that base.
type logicalOverlay struct {
	base    *durable.Snapshot
	live    *durable.Collection
	slots   []uint32
	entries []logicalOverlayEntry
	arena   []byte
	probe   []byte
	order   []int
	undo    []logicalOverlayUndo

	netDocuments int
	netBytes     int
}

type logicalOverlayEntry struct {
	hash uint64

	keyOffset int
	keyLength int

	baseFound  bool
	baseEqual  bool
	baseDigest [sha256.Size]byte
	baseLength uint64

	valueOffset int
	valueLength int
	deleted     bool
}

type logicalOverlayUndo struct {
	index int
	entry logicalOverlayEntry
}

type logicalOverlayMark struct {
	entries      int
	arena        int
	undo         int
	netDocuments int
	netBytes     int
}

func overlayKeyHash(key []byte) uint64 {
	return maphash.Bytes(logicalOverlayHashSeed, key)
}

func (o *logicalOverlay) reset(base *durable.Snapshot) {
	o.base = base
	o.live = nil
	o.resetContents()
}

func (o *logicalOverlay) resetPoint(base pointSnapshot) {
	o.base = base.value
	o.live = base.live
	o.resetContents()
}

func (o *logicalOverlay) resetContents() {
	clear(o.slots)
	clear(o.entries)
	clear(o.arena)
	clear(o.probe)
	clear(o.order)
	clear(o.undo)
	o.entries = o.entries[:0]
	o.arena = o.arena[:0]
	o.probe = o.probe[:0]
	o.order = o.order[:0]
	o.undo = o.undo[:0]
	o.netDocuments = 0
	o.netBytes = 0
}

func (o *logicalOverlay) release() {
	coldEntries := cap(o.entries) > maxNormalBatchRetainedOverlayEntries
	coldSlots := cap(o.slots) > maxNormalBatchRetainedOverlaySlots
	coldOrder := cap(o.order) > maxNormalBatchRetainedOverlayEntries
	coldUndo := cap(o.undo) > maxNormalBatchRetainedOverlayEntries
	o.reset(nil)
	if coldEntries {
		o.entries = nil
	}
	if coldSlots {
		o.slots = nil
	}
	if coldOrder {
		o.order = nil
	}
	if coldUndo {
		o.undo = nil
	}
	if cap(o.arena) > maxNormalBatchRetainedBufferBytes {
		o.arena = nil
	}
	if cap(o.probe) > maxNormalBatchRetainedBufferBytes {
		o.probe = nil
	}
}

func (o *logicalOverlay) mark() logicalOverlayMark {
	return logicalOverlayMark{
		entries: len(o.entries), arena: len(o.arena), undo: len(o.undo),
		netDocuments: o.netDocuments, netBytes: o.netBytes,
	}
}

func (o *logicalOverlay) commit(mark logicalOverlayMark) {
	if mark.undo <= len(o.undo) {
		o.undo = o.undo[:mark.undo]
	}
}

func (o *logicalOverlay) rollback(mark logicalOverlayMark) {
	for i := len(o.undo) - 1; i >= mark.undo; i-- {
		undo := o.undo[i]
		o.entries[undo.index] = undo.entry
	}
	o.undo = o.undo[:mark.undo]
	o.entries = o.entries[:mark.entries]
	o.arena = o.arena[:mark.arena]
	o.netDocuments = mark.netDocuments
	o.netBytes = mark.netBytes
	o.rebuildSlots()
}

func (o *logicalOverlay) rebuildSlots() {
	clear(o.slots)
	for index := range o.entries {
		entry := &o.entries[index]
		slot := int(entry.hash & uint64(len(o.slots)-1))
		for o.slots[slot] != 0 {
			slot = (slot + 1) & (len(o.slots) - 1)
		}
		o.slots[slot] = uint32(index + 1)
	}
}

func (o *logicalOverlay) ensureInsertRoom() {
	if len(o.slots) != 0 && (len(o.entries)+1)*4 <= len(o.slots)*3 {
		return
	}
	size := 8
	if len(o.slots) != 0 {
		size = len(o.slots) * 2
	}
	for (len(o.entries)+1)*4 > size*3 {
		size *= 2
	}
	if cap(o.slots) >= size {
		o.slots = o.slots[:size]
		clear(o.slots)
	} else {
		o.slots = make([]uint32, size)
	}
	o.rebuildSlots()
}

func (o *logicalOverlay) find(key []byte, hash uint64) (index, slot int, found bool) {
	if len(o.slots) == 0 {
		return 0, 0, false
	}
	slot = int(hash & uint64(len(o.slots)-1))
	for {
		encoded := o.slots[slot]
		if encoded == 0 {
			return 0, slot, false
		}
		index = int(encoded - 1)
		entry := &o.entries[index]
		if entry.hash == hash && entry.keyLength == len(key) &&
			bytes.Equal(o.key(entry), key) {
			return index, slot, true
		}
		slot = (slot + 1) & (len(o.slots) - 1)
	}
}

func (o *logicalOverlay) key(entry *logicalOverlayEntry) []byte {
	return o.arena[entry.keyOffset : entry.keyOffset+entry.keyLength]
}

func (o *logicalOverlay) value(entry *logicalOverlayEntry) []byte {
	return o.arena[entry.valueOffset : entry.valueOffset+entry.valueLength]
}

func (o *logicalOverlay) contributes(entry *logicalOverlayEntry) bool {
	if entry.deleted {
		return entry.baseFound
	}
	return !entry.baseFound || !entry.baseEqual
}

func (o *logicalOverlay) contribution(entry *logicalOverlayEntry) (documents, bytes int) {
	if !o.contributes(entry) {
		return 0, 0
	}
	return 1, entry.keyLength + entry.valueLength
}

func (o *logicalOverlay) record(key, value []byte, deleted bool) error {
	return o.recordResolved(
		key, value, deleted, false, false, 0, [sha256.Size]byte{},
		false, 0, [sha256.Size]byte{},
	)
}

// recordMutation consumes descriptors produced by the single logical planning
// read. The first mutation of a key necessarily read the base snapshot because
// no overlay entry existed yet. Subsequent mutations reuse the original base
// descriptor retained in the entry. No user snapshot read or value hash occurs
// here, including when a later command restores the original base value.
func (o *logicalOverlay) recordMutation(
	mutation finalMutation,
	descriptors []mutationValueDescriptor,
) error {
	if !mutation.described || int(mutation.descriptorIndex) >= len(descriptors) {
		return ErrInvalidCollection
	}
	descriptor := &descriptors[mutation.descriptorIndex]
	if !mutation.beforeFound && (descriptor.beforeLength != 0 ||
		descriptor.beforeDigest != ([sha256.Size]byte{})) ||
		mutation.delete && (descriptor.afterLength != 0 ||
			descriptor.afterDigest != ([sha256.Size]byte{})) {
		return ErrInvalidCollection
	}
	return o.recordResolved(
		mutation.key, mutation.value, mutation.delete,
		true, mutation.beforeFound, descriptor.beforeLength, descriptor.beforeDigest,
		!mutation.delete, descriptor.afterLength, descriptor.afterDigest,
	)
}

func (o *logicalOverlay) recordResolved(
	key, value []byte,
	deleted bool,
	baseKnown, knownBaseFound bool,
	knownBaseLength uint64,
	knownBaseDigest [sha256.Size]byte,
	afterKnown bool,
	afterLength uint64,
	afterDigest [sha256.Size]byte,
) error {
	hash := overlayKeyHash(key)
	index, _, found := o.find(key, hash)
	baseLoaded := false
	if !found {
		o.ensureInsertRoom()
		_, slot, _ := o.find(key, hash)
		entry := logicalOverlayEntry{
			hash: hash, keyOffset: len(o.arena), keyLength: len(key),
		}
		o.arena = append(o.arena, key...)
		if baseKnown {
			entry.baseFound = knownBaseFound
			entry.baseLength = knownBaseLength
			entry.baseDigest = knownBaseDigest
		} else if o.base != nil {
			var err error
			if deleted {
				entry.baseFound, err = o.base.ContainsKey(key)
			} else {
				o.probe, entry.baseFound, err = o.base.AppendRaw(o.probe[:0], key)
				baseLoaded = err == nil
			}
			if err != nil {
				return err
			}
		}
		o.entries = append(o.entries, entry)
		index = len(o.entries) - 1
		o.slots[slot] = uint32(index + 1)
	} else {
		oldDocuments, oldBytes := o.contribution(&o.entries[index])
		o.netDocuments -= oldDocuments
		o.netBytes -= oldBytes
		o.undo = append(o.undo, logicalOverlayUndo{index: index, entry: o.entries[index]})
	}
	entry := &o.entries[index]
	entry.baseEqual = false
	if !deleted && entry.baseFound {
		if afterKnown && (baseKnown || entry.baseDigest != ([sha256.Size]byte{})) {
			entry.baseEqual = entry.baseLength == afterLength &&
				entry.baseDigest == afterDigest
		} else {
			var baseFound bool
			var err error
			if baseLoaded {
				baseFound = entry.baseFound
			} else {
				o.probe, baseFound, err = o.base.AppendRaw(o.probe[:0], key)
				if err != nil {
					return err
				}
			}
			if !baseFound {
				return ErrInconsistentSnapshot
			}
			entry.baseEqual = bytes.Equal(o.probe, value)
		}
	}
	entry.valueOffset = len(o.arena)
	entry.valueLength = len(value)
	entry.deleted = deleted
	o.arena = append(o.arena, value...)
	documents, bytes := o.contribution(entry)
	o.netDocuments += documents
	o.netBytes += bytes
	return nil
}

func (o *logicalOverlay) appendRaw(dst, key []byte) ([]byte, bool, error) {
	return o.appendRawTracked(dst, key, nil)
}

func (o *logicalOverlay) appendRawTracked(
	dst, key []byte,
	physicalBaseReads *uint32,
) ([]byte, bool, error) {
	hash := overlayKeyHash(key)
	if index, _, found := o.find(key, hash); found {
		entry := &o.entries[index]
		if entry.deleted {
			return dst, false, nil
		}
		return append(dst, o.value(entry)...), true, nil
	}
	if o.base == nil && o.live == nil {
		return dst, false, nil
	}
	if physicalBaseReads != nil {
		*physicalBaseReads++
	}
	if o.live != nil {
		return o.live.AppendRaw(dst, key)
	}
	return o.base.AppendRaw(dst, key)
}

func (o *logicalOverlay) sorted(matching func(*logicalOverlayEntry) bool) []int {
	o.order = o.order[:0]
	for index := range o.entries {
		if matching == nil || matching(&o.entries[index]) {
			o.order = append(o.order, index)
		}
	}
	slices.SortFunc(o.order, func(left, right int) int {
		return bytes.Compare(o.key(&o.entries[left]), o.key(&o.entries[right]))
	})
	return o.order
}

func (o *logicalOverlay) rangePrefixRaw(
	prefix []byte,
	visit func(key, value []byte) error,
) error {
	ordered := o.sorted(func(entry *logicalOverlayEntry) bool {
		return bytes.HasPrefix(o.key(entry), prefix)
	})
	next := 0
	emitOverlay := func(index int) error {
		entry := &o.entries[index]
		if entry.deleted {
			return nil
		}
		return visit(o.key(entry), o.value(entry))
	}
	if o.base != nil || o.live != nil {
		err := pointSnapshot{value: o.base, live: o.live}.rangePrefixRaw(prefix, func(key, value []byte) error {
			for next < len(ordered) {
				entry := &o.entries[ordered[next]]
				comparison := bytes.Compare(o.key(entry), key)
				if comparison > 0 {
					break
				}
				next++
				if comparison == 0 {
					return emitOverlay(ordered[next-1])
				}
				if err := emitOverlay(ordered[next-1]); err != nil {
					return err
				}
			}
			return visit(key, value)
		})
		if err != nil {
			return err
		}
	}
	for next < len(ordered) {
		if err := emitOverlay(ordered[next]); err != nil {
			return err
		}
		next++
	}
	return nil
}

func (o *logicalOverlay) writeNet(batch *durable.WriteBatch) error {
	if batch == nil {
		return durable.ErrBatchClosed
	}
	ordered := o.sorted(o.contributes)
	for _, index := range ordered {
		entry := &o.entries[index]
		var err error
		if entry.deleted {
			err = batch.Delete(o.key(entry))
		} else {
			err = batch.Put(o.key(entry), o.value(entry))
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (o *logicalOverlay) appendAttempted(dst []finalMutation) []finalMutation {
	ordered := o.sorted(nil)
	for _, index := range ordered {
		dst = append(dst, finalMutation{key: o.key(&o.entries[index])})
	}
	return dst
}
