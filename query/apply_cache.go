package query

import (
	"hash/maphash"
	"math"
	"unsafe"
)

const applyEmptyKeyHash = uint64(0x6a09e667f3bcc909)

type applyMemoEntry struct {
	hash       uint64
	keyRow     int
	valueStart int
	valueRows  int
}

// applyMemoCache owns exact parameter tuples and their complete right-side
// relations. Empty right relations are entries too, which is required for
// OUTER APPLY and for zero-parameter decorrelation.
type applyMemoCache struct {
	seed      maphash.Seed
	seedReady bool
	keyCols   int
	rightCols int
	keys      recursiveSpool
	values    recursiveSpool
	slots     []uint32
	entries   []applyMemoEntry
}

func (c *applyMemoCache) begin(keyColumns, rightColumns int) {
	if !c.seedReady {
		c.seed = maphash.MakeSeed()
		c.seedReady = true
	}
	c.reset()
	c.keyCols = keyColumns
	c.rightCols = rightColumns
	c.keys.columns = keyColumns
	c.values.columns = rightColumns
}

func (c *applyMemoCache) reset() {
	if c == nil {
		return
	}
	c.keys.reset()
	c.values.reset()
	c.slots = c.slots[:0]
	c.entries = c.entries[:0]
}

func (c *applyMemoCache) release() {
	if c == nil {
		return
	}
	*c = applyMemoCache{}
}

func (c *applyMemoCache) hashKey(key *recursiveSpool, cancel *CancelFlag) (uint64, error) {
	if c.keyCols == 0 {
		return applyEmptyKeyHash, cancellationError(cancel)
	}
	if key == nil || key.rows != 1 || key.columns != c.keyCols {
		return 0, ErrApplySize
	}
	return hashRecursiveRow(c.seed, key, 0, c.keyCols, cancel)
}

func (c *applyMemoCache) find(
	hash uint64,
	key *recursiveSpool,
	cancel *CancelFlag,
) (*applyMemoEntry, bool, error) {
	if len(c.slots) == 0 {
		return nil, false, cancellationError(cancel)
	}
	mask := uint64(len(c.slots) - 1)
	for slot, probes := hash&mask, 0; probes < len(c.slots); probes++ {
		if err := cancellationCheckpoint(cancel, probes); err != nil {
			return nil, false, err
		}
		stored := c.slots[slot]
		if stored == 0 {
			return nil, false, nil
		}
		entry := &c.entries[stored-1]
		if entry.hash == hash {
			equal := c.keyCols == 0
			var err error
			if !equal {
				equal, err = recursiveRowsEqual(
					&c.keys, entry.keyRow, key, 0, c.keyCols, cancel,
				)
			}
			if err != nil {
				return nil, false, err
			}
			if equal {
				return entry, true, nil
			}
		}
		slot = (slot + 1) & mask
	}
	return nil, false, ErrApplySize
}

func (c *applyMemoCache) admitEntry(options applyOptions) error {
	entries, ok := checkedRecursiveAdd(len(c.entries), 1)
	if !ok || uint64(entries) >= uint64(math.MaxUint32) {
		return ErrApplySize
	}
	if options.maxCacheEntries >= 0 && entries > options.maxCacheEntries {
		return &ApplyCacheBudgetError{
			Entries: entries, EntryLimit: options.maxCacheEntries,
			ByteLimit: options.maxCacheBytes,
		}
	}
	return nil
}

func (c *applyMemoCache) store(
	hash uint64,
	key, values *recursiveSpool,
	options applyOptions,
) error {
	entries, ok := checkedRecursiveAdd(len(c.entries), 1)
	if !ok || uint64(entries) >= uint64(math.MaxUint32) {
		return ErrApplySize
	}
	capacity, err := applyMemoCapacity(entries)
	if err != nil {
		return err
	}
	keyBytes := int64(0)
	if c.keyCols != 0 {
		if key == nil || key.rows != 1 || key.columns != c.keyCols {
			return ErrApplySize
		}
		keyBytes = key.retainedBytes()
	}
	valueBytes := int64(0)
	if values != nil {
		if values.columns != c.rightCols {
			return ErrApplySize
		}
		valueBytes = values.retainedBytes()
	}
	projected := saturatedBytes(
		saturatedBytes(c.keys.retainedBytes(), keyBytes),
		saturatedBytes(
			saturatedBytes(c.values.retainedBytes(), valueBytes),
			applyMemoIndexBytes(capacity, entries),
		),
	)
	if projected == math.MaxInt64 {
		return ErrApplySize
	}
	if options.maxCacheBytes >= 0 && projected > options.maxCacheBytes {
		return &ApplyCacheBudgetError{
			Entries: entries, EntryLimit: options.maxCacheEntries,
			Bytes: projected, ByteLimit: options.maxCacheBytes,
		}
	}
	if err := c.ensureTable(entries); err != nil {
		return err
	}
	entry := applyMemoEntry{
		hash:       hash,
		keyRow:     -1,
		valueStart: c.values.rows,
	}
	if c.keyCols != 0 {
		entry.keyRow = c.keys.rows
		if err := c.keys.appendSpool(key, options.cancel); err != nil {
			return err
		}
	}
	if values != nil && values.rows != 0 {
		entry.valueRows = values.rows
		if err := c.values.appendSpool(values, options.cancel); err != nil {
			return err
		}
	}
	var reserveErr error
	c.entries, reserveErr = reserveRecursiveSlice(c.entries, entries)
	if reserveErr != nil {
		return ErrApplySize
	}
	entryIndex := len(c.entries)
	c.entries = append(c.entries, entry)
	mask := uint64(len(c.slots) - 1)
	for slot, probes := hash&mask, 0; probes < len(c.slots); probes++ {
		if err := cancellationCheckpoint(options.cancel, probes); err != nil {
			return err
		}
		if c.slots[slot] == 0 {
			c.slots[slot] = uint32(entryIndex + 1)
			return cancellationError(options.cancel)
		}
		slot = (slot + 1) & mask
	}
	return ErrApplySize
}

func (c *applyMemoCache) ensureTable(entries int) error {
	capacity, err := applyMemoCapacity(entries)
	if err != nil {
		return err
	}
	if len(c.slots) >= capacity {
		return nil
	}
	if cap(c.slots) < capacity {
		c.slots = make([]uint32, capacity)
	} else {
		c.slots = c.slots[:capacity]
		clear(c.slots)
	}
	mask := uint64(capacity - 1)
	for entry := range c.entries {
		slot := c.entries[entry].hash & mask
		for c.slots[slot] != 0 {
			slot = (slot + 1) & mask
		}
		c.slots[slot] = uint32(entry + 1)
	}
	return nil
}

func applyMemoCapacity(entries int) (int, error) {
	if entries < 0 || uint64(entries) >= uint64(math.MaxUint32) || entries > math.MaxInt/2 {
		return 0, ErrApplySize
	}
	if entries == 0 {
		return 0, nil
	}
	need := entries * 2
	capacity := 8
	for capacity < need {
		if capacity > math.MaxInt/2 {
			return 0, ErrApplySize
		}
		capacity <<= 1
	}
	return capacity, nil
}

func applyMemoIndexBytes(slots, entries int) int64 {
	if slots < 0 || entries < 0 {
		return math.MaxInt64
	}
	return saturatedBytes(
		saturatedProduct(int64(slots), int64(unsafe.Sizeof(uint32(0)))),
		saturatedProduct(int64(entries), int64(unsafe.Sizeof(applyMemoEntry{}))),
	)
}
