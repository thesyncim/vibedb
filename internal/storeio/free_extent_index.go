package storeio

// freeExtentIndexFanout is deliberately large enough that the hierarchy costs
// little memory while each level still fits in a small, predictable scan.
const freeExtentIndexFanout = 64

// An int cannot describe enough extents to require more levels: 64^11 is
// greater than the largest positive 64-bit int.
const freeExtentIndexMaxLevels = 11

// FreeExtentIndex is a caller-backed max-length hierarchy over an
// offset-ordered FreeExtent slice. It preserves exact lowest-offset first-fit:
// maxima only discard groups that cannot satisfy a request, and every descent
// selects the first remaining group.
//
// Rebuild, FirstFit, and Update do not allocate. The caller owns both the
// extents and the maxima storage and must not reorder or resize the extent
// slice without rebuilding the index.
type FreeExtentIndex struct {
	maxima       []uint64
	extentCount  int
	levelCount   uint8
	levelOffsets [freeExtentIndexMaxLevels]int
}

// FreeExtentIndexCapacity returns the number of uint64 maxima required to
// index extentCount extents. Multiplying the result by eight gives the exact
// storage requirement in bytes.
func FreeExtentIndexCapacity(extentCount int) int {
	if extentCount <= 0 {
		return 0
	}
	capacity := 0
	for {
		extentCount = (extentCount-1)/freeExtentIndexFanout + 1
		capacity += extentCount
		if extentCount == 1 {
			return capacity
		}
	}
}

// Rebuild replaces x with an index of extents using the prefix of storage
// reported by FreeExtentIndexCapacity. It returns false, without retaining
// either input, when storage is too small.
func (x *FreeExtentIndex) Rebuild(extents []FreeExtent, storage []uint64) bool {
	if x == nil {
		return false
	}
	*x = FreeExtentIndex{}
	required := FreeExtentIndexCapacity(len(extents))
	if len(storage) < required {
		return false
	}
	if len(extents) == 0 {
		return true
	}

	x.maxima = storage[:required]
	x.extentCount = len(extents)
	inputCount := len(extents)
	offset := 0
	for {
		levelLength := (inputCount-1)/freeExtentIndexFanout + 1
		level := int(x.levelCount)
		x.levelOffsets[level] = offset
		x.levelCount++
		offset += levelLength
		if levelLength == 1 {
			break
		}
		inputCount = levelLength
	}

	leaf := x.level(0)
	for group := range leaf {
		first := group * freeExtentIndexFanout
		last := min(first+freeExtentIndexFanout, len(extents))
		leaf[group] = maxFreeExtentLength(extents[first:last])
	}
	for level := 1; level < int(x.levelCount); level++ {
		children := x.level(level - 1)
		parents := x.level(level)
		for group := range parents {
			first := group * freeExtentIndexFanout
			last := min(first+freeExtentIndexFanout, len(children))
			parents[group] = maxUint64(children[first:last])
		}
	}
	return true
}

// FirstFit returns the index of the lowest-offset extent whose length is at
// least want. A zero-length request, a resized extent slice, or an unbuilt
// index has no match.
func (x *FreeExtentIndex) FirstFit(extents []FreeExtent, want uint64) (int, bool) {
	if x == nil || want == 0 || len(extents) != x.extentCount || x.levelCount == 0 {
		return 0, false
	}
	if extents[0].Length >= want {
		return 0, true
	}
	return x.firstFitAfterFirst(extents, want)
}

// Keeping the hierarchy walk out of FirstFit lets the compiler inline its
// overwhelmingly common first-extent fast path into allocator callers.
func (x *FreeExtentIndex) firstFitAfterFirst(extents []FreeExtent, want uint64) (int, bool) {
	root := x.level(int(x.levelCount) - 1)
	if root[0] < want {
		return 0, false
	}

	group := 0
	for level := int(x.levelCount) - 2; level >= 0; level-- {
		nodes := x.level(level)
		first := group * freeExtentIndexFanout
		last := min(first+freeExtentIndexFanout, len(nodes))
		child := firstMaxAtLeast(nodes[first:last], want)
		if child < 0 {
			// The caller changed a length without Update.
			return 0, false
		}
		group = first + child
	}

	first := group * freeExtentIndexFanout
	last := min(first+freeExtentIndexFanout, len(extents))
	for rank := first; rank < last; rank++ {
		if extents[rank].Length >= want {
			return rank, true
		}
	}
	// Reaching this point means the caller changed a length without Update.
	return 0, false
}

// NearestFit returns the index of the extent whose length is at least want and
// whose offset is nearest hint, measured as |extent.Offset - hint|. It is the
// placement-hint counterpart of FirstFit: FirstFit concentrates allocation at
// the low end of the file, NearestFit keeps a copy-on-write page physically near
// the extent it retires, so churn recycles a tablet's neighbourhood instead of
// marching every rewrite to the file tail.
//
// It returns the same extent a linear nearest scan would, but stays O(levels)
// by reusing the maxima hierarchy: a binary search locates hint's offset
// neighbourhood, then one bounded descent on each side finds the nearest
// qualifying extent below and at-or-above hint. Ties favour the lower offset, so
// placement stays deterministic and biased toward compaction. A zero want, a
// resized extent slice, or an unbuilt index has no match. Extents must be offset
// ordered, which the reusable set always is; consumed zero-length entries keep
// their offset and are skipped because their length cannot satisfy a request.
func (x *FreeExtentIndex) NearestFit(extents []FreeExtent, want, hint uint64) (int, bool) {
	if x == nil || want == 0 || len(extents) != x.extentCount || x.levelCount == 0 {
		return 0, false
	}
	pivot := freeExtentLowerBound(extents, hint)
	below, belowOK := x.lastFitBefore(extents, want, pivot)
	above, aboveOK := x.firstFitFrom(extents, want, pivot)
	switch {
	case !belowOK && !aboveOK:
		return 0, false
	case !belowOK:
		return above, true
	case !aboveOK:
		return below, true
	}
	// pivot is the first offset >= hint, so below sits strictly under hint and
	// above at or over it: the two candidates never share a direction, and the
	// nearer wins. On a tie prefer the lower offset for a deterministic, compact
	// choice.
	if extents[above].Offset-hint < hint-extents[below].Offset {
		return above, true
	}
	return below, true
}

// freeExtentLowerBound returns the first index whose offset is at least offset,
// over an offset-ordered extent slice.
func freeExtentLowerBound(extents []FreeExtent, offset uint64) int {
	lo, hi := 0, len(extents)
	for lo < hi {
		middle := int(uint(lo+hi) >> 1)
		if extents[middle].Offset < offset {
			lo = middle + 1
		} else {
			hi = middle
		}
	}
	return lo
}

// firstFitFrom returns the least index at or after from whose length is at least
// want. It scans the remainder of from's leaf group directly, then asks the
// hierarchy for the next group that can hold want and scans that. Both scans are
// bounded by the fan-out and the ascent by the level count, so the whole query
// is O(levels).
func (x *FreeExtentIndex) firstFitFrom(extents []FreeExtent, want uint64, from int) (int, bool) {
	if from < 0 {
		from = 0
	}
	if from >= len(extents) {
		return 0, false
	}
	group := from / freeExtentIndexFanout
	last := min((group+1)*freeExtentIndexFanout, len(extents))
	for rank := from; rank < last; rank++ {
		if extents[rank].Length >= want {
			return rank, true
		}
	}
	next, ok := x.nextGroupWithMax(group+1, want)
	if !ok {
		return 0, false
	}
	first := next * freeExtentIndexFanout
	last = min(first+freeExtentIndexFanout, len(extents))
	for rank := first; rank < last; rank++ {
		if extents[rank].Length >= want {
			return rank, true
		}
	}
	return 0, false
}

// lastFitBefore returns the greatest index strictly below before whose length is
// at least want. It mirrors firstFitFrom in the descending direction.
func (x *FreeExtentIndex) lastFitBefore(extents []FreeExtent, want uint64, before int) (int, bool) {
	if before > len(extents) {
		before = len(extents)
	}
	if before <= 0 {
		return 0, false
	}
	high := before - 1
	group := high / freeExtentIndexFanout
	start := group * freeExtentIndexFanout
	for rank := high; rank >= start; rank-- {
		if extents[rank].Length >= want {
			return rank, true
		}
	}
	prev, ok := x.prevGroupWithMax(group-1, want)
	if !ok {
		return 0, false
	}
	first := prev * freeExtentIndexFanout
	last := min(first+freeExtentIndexFanout, len(extents))
	for rank := last - 1; rank >= first; rank-- {
		if extents[rank].Length >= want {
			return rank, true
		}
	}
	return 0, false
}

// nextGroupWithMax returns the least leaf-group index at or after startGroup
// whose maximum length is at least want. It ascends from startGroup, scanning
// each node's remaining siblings within its own parent block, then climbing to
// the parent's next sibling; a hit descends leftward to the first qualifying
// leaf group. It is the successor-search that keeps firstFitFrom O(levels).
func (x *FreeExtentIndex) nextGroupWithMax(startGroup int, want uint64) (int, bool) {
	if startGroup < 0 {
		startGroup = 0
	}
	level := 0
	index := startGroup
	for {
		nodes := x.level(level)
		if index < len(nodes) {
			blockEnd := min(
				(index/freeExtentIndexFanout+1)*freeExtentIndexFanout,
				len(nodes),
			)
			for at := index; at < blockEnd; at++ {
				if nodes[at] >= want {
					if leaf := x.descendFirst(level, at, want); leaf >= 0 {
						return leaf, true
					}
				}
			}
		}
		if level+1 >= int(x.levelCount) {
			return 0, false
		}
		index = index/freeExtentIndexFanout + 1
		level++
	}
}

// prevGroupWithMax returns the greatest leaf-group index at or before startGroup
// whose maximum length is at least want, the descending mirror of
// nextGroupWithMax.
func (x *FreeExtentIndex) prevGroupWithMax(startGroup int, want uint64) (int, bool) {
	level := 0
	index := startGroup
	for {
		if index < 0 {
			return 0, false
		}
		nodes := x.level(level)
		if index >= len(nodes) {
			index = len(nodes) - 1
		}
		if index >= 0 {
			blockStart := (index / freeExtentIndexFanout) * freeExtentIndexFanout
			for at := index; at >= blockStart; at-- {
				if nodes[at] >= want {
					if leaf := x.descendLast(level, at, want); leaf >= 0 {
						return leaf, true
					}
				}
			}
		}
		if level+1 >= int(x.levelCount) {
			return 0, false
		}
		index = index/freeExtentIndexFanout - 1
		level++
	}
}

// descendFirst walks from a node known to satisfy want down to the leftmost
// leaf group that still satisfies it. A negative return means the caller changed
// a length without Update, and the caller keeps searching.
func (x *FreeExtentIndex) descendFirst(level, node int, want uint64) int {
	for level > 0 {
		child := level - 1
		children := x.level(child)
		first := node * freeExtentIndexFanout
		last := min(first+freeExtentIndexFanout, len(children))
		found := -1
		for at := first; at < last; at++ {
			if children[at] >= want {
				found = at
				break
			}
		}
		if found < 0 {
			return -1
		}
		node = found
		level = child
	}
	return node
}

// descendLast walks from a qualifying node down to the rightmost leaf group that
// still satisfies want, the mirror of descendFirst.
func (x *FreeExtentIndex) descendLast(level, node int, want uint64) int {
	for level > 0 {
		child := level - 1
		children := x.level(child)
		first := node * freeExtentIndexFanout
		last := min(first+freeExtentIndexFanout, len(children))
		found := -1
		for at := last - 1; at >= first; at-- {
			if children[at] >= want {
				found = at
				break
			}
		}
		if found < 0 {
			return -1
		}
		node = found
		level = child
	}
	return node
}

// Update refreshes the one leaf containing extentIndex and its ancestors after
// the caller changes that extent's Length. It does not allocate. Update
// returns false if x does not describe extents or extentIndex is out of range.
func (x *FreeExtentIndex) Update(extents []FreeExtent, extentIndex int) bool {
	if x == nil || len(extents) != x.extentCount || extentIndex < 0 ||
		extentIndex >= len(extents) || x.levelCount == 0 {
		return false
	}

	group := extentIndex / freeExtentIndexFanout
	leaf := x.level(0)
	first := group * freeExtentIndexFanout
	last := min(first+freeExtentIndexFanout, len(extents))
	leaf[group] = maxFreeExtentLength(extents[first:last])

	for level := 1; level < int(x.levelCount); level++ {
		group /= freeExtentIndexFanout
		children := x.level(level - 1)
		parents := x.level(level)
		first = group * freeExtentIndexFanout
		last = min(first+freeExtentIndexFanout, len(children))
		parents[group] = maxUint64(children[first:last])
	}
	return true
}

// Len reports the number of extents described by x.
func (x *FreeExtentIndex) Len() int {
	if x == nil {
		return 0
	}
	return x.extentCount
}

// StorageBytes reports the exact caller-owned storage retained by x.
func (x *FreeExtentIndex) StorageBytes() int {
	if x == nil {
		return 0
	}
	return len(x.maxima) * 8
}

func (x *FreeExtentIndex) level(level int) []uint64 {
	first := x.levelOffsets[level]
	last := len(x.maxima)
	if level+1 < int(x.levelCount) {
		last = x.levelOffsets[level+1]
	}
	return x.maxima[first:last]
}

func maxFreeExtentLength(extents []FreeExtent) uint64 {
	var largest uint64
	for i := range extents {
		largest = max(largest, extents[i].Length)
	}
	return largest
}

func maxUint64(values []uint64) uint64 {
	var largest uint64
	for _, value := range values {
		largest = max(largest, value)
	}
	return largest
}

func firstMaxAtLeast(values []uint64, want uint64) int {
	for i, value := range values {
		if value >= want {
			return i
		}
	}
	return -1
}
