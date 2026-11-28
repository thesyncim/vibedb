package query

import "github.com/thesyncim/vibejson/x/keyhash"

const fileGroupIndexInitCap = 64

// fileGroupIndexSlot is one probe entry in a fileGroupIndex. It contains no Go
// pointers so its backing slice is allocated in noscan spans and never scanned
// by the garbage collector.
type fileGroupIndexSlot struct {
	gen     uint32
	hash    uint32
	groupID int
}

// fileGroupIndex is a generation-stamped, open-addressed hash table mapping
// encoded group keys (stored in an external []fileGroup slice) to their group
// index.
//
// Resetting the table between batches increments the generation stamp in O(1)
// without zeroing the slot array; the array is only cleared on uint32
// wraparound (once every 2^32-1 resets).
type fileGroupIndex struct {
	slots []fileGroupIndexSlot
	gen   uint32
	count int
	limit int // max count before grow: floor(len(slots)*7/10)
}

func (t *fileGroupIndex) len() int {
	return t.count
}

// reset invalidates all entries in O(1) while retaining the backing slice.
// Only uint32 generation wraparound zeroes the slice.
func (t *fileGroupIndex) reset() {
	t.count = 0
	if len(t.slots) == 0 {
		return
	}
	t.gen++
	if t.gen == 0 {
		clear(t.slots)
		t.gen = 1
	}
}

// release drops the backing slice and resets all metadata.
func (t *fileGroupIndex) release() {
	t.slots = nil
	t.gen = 0
	t.count = 0
	t.limit = 0
}

func (t *fileGroupIndex) lookupBytes(key []byte, groups []fileGroup) (int, bool) {
	if t.count == 0 {
		return 0, false
	}
	id, _, ok := t.lookupBytesSlot(key, keyhash.Bytes(key), groups)
	return id, ok
}

func (t *fileGroupIndex) lookupBytesHash(
	key []byte, hash uint32, groups []fileGroup,
) (int, bool) {
	id, _, ok := t.lookupBytesSlot(key, hash, groups)
	return id, ok
}

// lookupBytesSlot probes for key using its precomputed keyhash. On a miss, pos
// is the first unoccupied slot in the current table (valid for insertAt while
// t.count < t.limit and no other mutation occurs).
func (t *fileGroupIndex) lookupBytesSlot(
	key []byte, hash uint32, groups []fileGroup,
) (id int, pos uint, ok bool) {
	slots := t.slots
	if t.count == 0 {
		if len(slots) == 0 {
			return 0, 0, false
		}
		return 0, uint(hash) & uint(len(slots)-1), false
	}
	mask := uint(len(slots) - 1)
	gen := t.gen
	pos = uint(hash) & mask
	for {
		s := slots[pos]
		if s.gen != gen {
			return 0, pos, false
		}
		if s.hash == hash && string(key) == groups[s.groupID].key {
			return s.groupID, pos, true
		}
		pos = (pos + 1) & mask
	}
}

func (t *fileGroupIndex) lookupString(key string, groups []fileGroup) (int, bool) {
	if t.count == 0 {
		return 0, false
	}
	id, _, ok := t.lookupStringSlot(key, keyhash.String(key), groups)
	return id, ok
}

func (t *fileGroupIndex) lookupStringHash(
	key string, hash uint32, groups []fileGroup,
) (int, bool) {
	id, _, ok := t.lookupStringSlot(key, hash, groups)
	return id, ok
}

// lookupStringSlot probes for key using its precomputed keyhash. On a miss, pos
// is the first unoccupied slot in the current table (valid for insertAt while
// t.count < t.limit and no other mutation occurs).
func (t *fileGroupIndex) lookupStringSlot(
	key string, hash uint32, groups []fileGroup,
) (id int, pos uint, ok bool) {
	slots := t.slots
	if t.count == 0 {
		if len(slots) == 0 {
			return 0, 0, false
		}
		return 0, uint(hash) & uint(len(slots)-1), false
	}
	mask := uint(len(slots) - 1)
	gen := t.gen
	pos = uint(hash) & mask
	for {
		s := slots[pos]
		if s.gen != gen {
			return 0, pos, false
		}
		if s.hash == hash && key == groups[s.groupID].key {
			return s.groupID, pos, true
		}
		pos = (pos + 1) & mask
	}
}

// insertAt inserts a key known to be absent at the empty slot pos returned by
// an immediately preceding lookup*Slot miss, avoiding a second probe when no
// growth is required.
func (t *fileGroupIndex) insertAt(pos uint, hash uint32, groupID int) {
	if t.count >= t.limit {
		t.insertHash(hash, groupID)
		return
	}
	t.slots[pos] = fileGroupIndexSlot{
		gen:     t.gen,
		hash:    hash,
		groupID: groupID,
	}
	t.count++
}

// insertHash inserts a key known to be absent from t using its precomputed
// keyhash. Callers must check lookup* first so duplicate keys are never
// inserted into the table.
func (t *fileGroupIndex) insertHash(hash uint32, groupID int) {
	if t.count >= t.limit {
		t.grow()
	}
	slots := t.slots
	mask := uint(len(slots) - 1)
	gen := t.gen
	pos := uint(hash) & mask
	for slots[pos].gen == gen {
		pos = (pos + 1) & mask
	}
	slots[pos] = fileGroupIndexSlot{
		gen:     gen,
		hash:    hash,
		groupID: groupID,
	}
	t.count++
}

func (t *fileGroupIndex) grow() {
	const maxInt = int(^uint(0) >> 1)
	var newCap int
	if len(t.slots) == 0 {
		newCap = fileGroupIndexInitCap
	} else {
		if len(t.slots) > maxInt/2 {
			panic("query: file group index capacity overflow")
		}
		newCap = len(t.slots) * 2
	}
	newSlots := make([]fileGroupIndexSlot, newCap)
	const newGen = uint32(1)
	if t.count > 0 {
		oldGen := t.gen
		mask := uint(newCap - 1)
		for _, s := range t.slots {
			if s.gen != oldGen {
				continue
			}
			pos := uint(s.hash) & mask
			for newSlots[pos].gen == newGen {
				pos = (pos + 1) & mask
			}
			newSlots[pos] = fileGroupIndexSlot{
				gen:     newGen,
				hash:    s.hash,
				groupID: s.groupID,
			}
		}
	}
	t.slots = newSlots
	t.gen = newGen
	t.limit = newCap/10*7 + (newCap%10)*7/10
}
