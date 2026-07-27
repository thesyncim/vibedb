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
