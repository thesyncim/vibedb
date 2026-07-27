package storeio

import "unsafe"

// retiredIntervalNone is the nil link for the fixed-capacity interval AVL.
// int32 keeps every node compact while still covering the reclaimer's maximum
// supported 1<<24 extents.
const retiredIntervalNone = int32(-1)

// An AVL containing at most 1<<24 nodes is less than 36 levels high. Keep the
// path on the goroutine stack so insertion and deletion remain allocation-free
// and malformed internal state fails closed instead of recursing until the
// process stack is exhausted.
const retiredIntervalPathCapacity = 40

// retiredIntervalNode is deliberately pointer-free and exactly 24 bytes.
// Parent links are omitted: mutations retain their bounded search path on the
// stack and reconnect rotations while walking it upward.
type retiredIntervalNode struct {
	offset     uint64
	length     uint64
	leftHeight uint32
	right      int32
}

// MaxRetiredExtents needs 24 bits of node rank. Encoding rank+1 in 25 bits
// leaves zero as nil and the upper seven bits for an AVL height (the maximum
// possible configured tree is under 36 levels).
const (
	retiredIntervalLinkBits = 25
	retiredIntervalLinkMask = uint32(1<<retiredIntervalLinkBits) - 1
)

// retiredIntervalIndex is a fixed-capacity, allocation-free interval set used
// only while the reclaimer mutex is held. The generation-ordered pending slice
// remains authoritative for reclamation; this secondary ordering exists so a
// retirement can prove non-overlap without scanning every snapshot-fenced
// extent.
//
// Free nodes reuse their left link as an intrusive free list. The AVL balance
// invariant gives worst-case O(log n) lookup and mutation for every insertion
// order; caller-controlled extent offsets cannot create a linear tree.
//
// Links and the intrusive free list are trusted volatile invariants, not a
// decoder boundary: every rank is created by this implementation while the
// reclaimer mutex is held. Per-hop bounds/cycle checks would tax every lookup
// to diagnose only out-of-contract mutation of an externally supplied arena.
// ExtentReclaimerOptions therefore grants the reclaimer exclusive mutable
// ownership of that arena for its full lifetime. Persistent or untrusted bytes
// must never be installed here without a separate complete validator.
type retiredIntervalIndex struct {
	nodes       []retiredIntervalNode
	root        int32
	free        int32
	count       int
	high        int32
	initialized bool
}

func newRetiredIntervalIndex(capacity int) retiredIntervalIndex {
	return retiredIntervalIndex{
		nodes: make([]retiredIntervalNode, capacity),
		root:  retiredIntervalNone,
		free:  retiredIntervalNone,
	}
}

// RetiredIntervalIndexStorageBytes returns the exact pointer-free backing
// storage the reclaimer needs for its large-set overlap index.
func RetiredIntervalIndexStorageBytes(capacity int) int {
	if capacity > 1<<24 {
		return 0
	}
	return fixedStorageBytes(capacity, unsafe.Sizeof(retiredIntervalNode{}))
}

// RetiredExtentStorageBytes returns the exact pointer-free backing storage the
// reclaimer needs for its generation-ordered pending-retirement arena.
func RetiredExtentStorageBytes(capacity int) int {
	if capacity > 1<<24 {
		return 0
	}
	return fixedStorageBytes(capacity, unsafe.Sizeof(FreeExtent{}))
}

func fixedStorageBytes(capacity int, elementSize uintptr) int {
	if capacity <= 0 ||
		uint64(capacity) > uint64(^uint(0)>>1)/uint64(elementSize) {
		return 0
	}
	return capacity * int(elementSize)
}

func newRetiredIntervalIndexIn(
	capacity int, storage []byte,
) (retiredIntervalIndex, bool) {
	required := RetiredIntervalIndexStorageBytes(capacity)
	if required == 0 || len(storage) < required ||
		uintptr(unsafe.Pointer(unsafe.SliceData(storage)))%
			unsafe.Alignof(retiredIntervalNode{}) != 0 {
		return retiredIntervalIndex{}, false
	}
	nodes := unsafe.Slice(
		(*retiredIntervalNode)(unsafe.Pointer(unsafe.SliceData(storage))),
		capacity,
	)
	return retiredIntervalIndex{
		nodes: nodes, root: retiredIntervalNone, free: retiredIntervalNone,
	}, true
}

func newRetiredExtentArenaIn(
	capacity int, storage []byte,
) ([]FreeExtent, bool) {
	required := RetiredExtentStorageBytes(capacity)
	if required == 0 || len(storage) < required ||
		uintptr(unsafe.Pointer(unsafe.SliceData(storage)))%
			unsafe.Alignof(FreeExtent{}) != 0 {
		return nil, false
	}
	arena := unsafe.Slice(
		(*FreeExtent)(unsafe.Pointer(unsafe.SliceData(storage))),
		capacity,
	)
	return arena[:0], true
}

func fixedStorageRangesOverlap(
	left []byte, leftBytes int,
	right []byte, rightBytes int,
) bool {
	if leftBytes <= 0 || rightBytes <= 0 {
		return false
	}
	leftStart := uintptr(unsafe.Pointer(unsafe.SliceData(left)))
	rightStart := uintptr(unsafe.Pointer(unsafe.SliceData(right)))
	leftEnd := leftStart + uintptr(leftBytes)
	rightEnd := rightStart + uintptr(rightBytes)
	return leftStart < rightEnd && rightStart < leftEnd
}

func (x *retiredIntervalIndex) ensureInitialized() {
	if x != nil && !x.initialized {
		x.reset()
	}
}

func (x *retiredIntervalIndex) reset() {
	if x == nil {
		return
	}
	x.root = retiredIntervalNone
	x.free = retiredIntervalNone
	x.count = 0
	x.high = 0
	x.initialized = true
}

func (x *retiredIntervalIndex) len() int {
	if x == nil {
		return 0
	}
	return x.count
}

// overlaps reports whether [offset, offset+length) intersects the indexed set.
// Callers validate overflow and zero length before reaching this method.
func (x *retiredIntervalIndex) overlaps(offset, length uint64) bool {
	if x == nil || !x.initialized || length == 0 {
		return false
	}
	end := offset + length
	at := x.root
	predecessor := retiredIntervalNone
	successor := retiredIntervalNone
	for at != retiredIntervalNone {
		node := &x.nodes[at]
		if offset < node.offset {
			successor = at
			at = x.nodeLeft(at)
		} else {
			predecessor = at
			at = node.right
		}
	}
	if predecessor != retiredIntervalNone {
		node := &x.nodes[predecessor]
		if offset < node.offset+node.length {
			return true
		}
	}
	return successor != retiredIntervalNone && x.nodes[successor].offset < end
}

func (x *retiredIntervalIndex) contains(offset, length uint64) bool {
	if x == nil || !x.initialized {
		return false
	}
	at := x.root
	for at != retiredIntervalNone {
		node := &x.nodes[at]
		switch {
		case offset < node.offset:
			at = x.nodeLeft(at)
		case offset > node.offset:
			at = node.right
		default:
			return node.length == length
		}
	}
	return false
}

func (x *retiredIntervalIndex) insert(offset, length uint64) bool {
	if x == nil || length == 0 || offset > ^uint64(0)-length {
		return false
	}
	x.ensureInitialized()
	if x.overlaps(offset, length) {
		return false
	}

	var path [retiredIntervalPathCapacity]int32
	depth := 0
	at := x.root
	for at != retiredIntervalNone {
		if depth == len(path) {
			return false
		}
		path[depth] = at
		depth++
		if offset < x.nodes[at].offset {
			at = x.nodeLeft(at)
		} else {
			at = x.nodes[at].right
		}
	}

	rank := x.takeNode()
	if rank == retiredIntervalNone {
		return false
	}
	x.nodes[rank] = retiredIntervalNode{
		offset:     offset,
		length:     length,
		leftHeight: uint32(1) << retiredIntervalLinkBits,
		right:      retiredIntervalNone,
	}
	if depth == 0 {
		x.root = rank
		x.count++
		return true
	}
	parent := path[depth-1]
	if offset < x.nodes[parent].offset {
		x.setNodeLeft(parent, rank)
	} else {
		x.nodes[parent].right = rank
	}
	x.rebalancePath(path[:depth])
	x.count++
	return true
}

func (x *retiredIntervalIndex) remove(offset, length uint64) bool {
	if x == nil || !x.initialized {
		return false
	}
	var path [retiredIntervalPathCapacity]int32
	depth := 0
	removed := x.root
	for removed != retiredIntervalNone {
		switch {
		case offset < x.nodes[removed].offset:
			if depth == len(path) {
				return false
			}
			path[depth] = removed
			depth++
			removed = x.nodeLeft(removed)
		case offset > x.nodes[removed].offset:
			if depth == len(path) {
				return false
			}
			path[depth] = removed
			depth++
			removed = x.nodes[removed].right
		default:
			goto found
		}
	}
	return false

found:
	if x.nodes[removed].length != length {
		return false
	}
	left := x.nodeLeft(removed)
	right := x.nodes[removed].right
	if left == retiredIntervalNone || right == retiredIntervalNone {
		child := left
		if child == retiredIntervalNone {
			child = right
		}
		x.replacePathChild(path[:depth], removed, child)
		x.releaseNode(removed)
		x.rebalancePath(path[:depth])
		x.count--
		return true
	}

	// Copy the in-order successor's interval into the node being retained, then
	// remove the successor (which has no left child). The path contains every
	// ancestor whose height may have changed, including the retained node.
	if depth == len(path) {
		return false
	}
	path[depth] = removed
	depth++
	successor := right
	for x.nodeLeft(successor) != retiredIntervalNone {
		if depth == len(path) {
			return false
		}
		path[depth] = successor
		depth++
		successor = x.nodeLeft(successor)
	}
	x.nodes[removed].offset = x.nodes[successor].offset
	x.nodes[removed].length = x.nodes[successor].length
	parent := path[depth-1]
	if x.nodeLeft(parent) == successor {
		x.setNodeLeft(parent, x.nodes[successor].right)
	} else if x.nodes[parent].right == successor {
		x.nodes[parent].right = x.nodes[successor].right
	} else {
		return false
	}
	x.releaseNode(successor)
	x.rebalancePath(path[:depth])
	x.count--
	return true
}

func (x *retiredIntervalIndex) takeNode() int32 {
	if x.free != retiredIntervalNone {
		rank := x.free
		x.free = x.nodeLeft(rank)
		return rank
	}
	if int(x.high) >= len(x.nodes) {
		return retiredIntervalNone
	}
	rank := x.high
	x.high++
	return rank
}

func (x *retiredIntervalIndex) releaseNode(rank int32) {
	x.nodes[rank] = retiredIntervalNode{
		leftHeight: encodeRetiredIntervalLink(x.free),
	}
	x.free = rank
}

func (x *retiredIntervalIndex) replacePathChild(
	path []int32, old, replacement int32,
) {
	if len(path) == 0 {
		x.root = replacement
		return
	}
	parent := path[len(path)-1]
	if x.nodeLeft(parent) == old {
		x.setNodeLeft(parent, replacement)
		return
	}
	if x.nodes[parent].right == old {
		x.nodes[parent].right = replacement
	}
}

func (x *retiredIntervalIndex) rebalancePath(path []int32) {
	for position := len(path) - 1; position >= 0; position-- {
		oldRoot := path[position]
		oldHeight := x.nodeHeight(oldRoot)
		newRoot := x.rebalance(oldRoot)
		if position == 0 {
			x.root = newRoot
		} else {
			parent := path[position-1]
			if x.nodeLeft(parent) == oldRoot {
				x.setNodeLeft(parent, newRoot)
			} else if x.nodes[parent].right == oldRoot {
				x.nodes[parent].right = newRoot
			}
		}
		// Once the rebalanced subtree kept its previous height, no ancestor's
		// balance or height can have changed. This is the ordinary case for a
		// mutation in a large steady tree and avoids walking to the root merely
		// to rewrite identical height bytes.
		if x.nodeHeight(newRoot) == oldHeight {
			break
		}
	}
}

func (x *retiredIntervalIndex) rebalance(root int32) int32 {
	x.updateHeight(root)
	balance := x.nodeHeight(x.nodeLeft(root)) -
		x.nodeHeight(x.nodes[root].right)
	switch {
	case balance > 1:
		left := x.nodeLeft(root)
		if x.nodeHeight(x.nodeLeft(left)) <
			x.nodeHeight(x.nodes[left].right) {
			x.setNodeLeft(root, x.rotateLeft(left))
		}
		return x.rotateRight(root)
	case balance < -1:
		right := x.nodes[root].right
		if x.nodeHeight(x.nodes[right].right) <
			x.nodeHeight(x.nodeLeft(right)) {
			x.nodes[root].right = x.rotateRight(right)
		}
		return x.rotateLeft(root)
	default:
		return root
	}
}

func (x *retiredIntervalIndex) rotateLeft(root int32) int32 {
	right := x.nodes[root].right
	middle := x.nodeLeft(right)
	x.nodes[root].right = middle
	x.setNodeLeft(right, root)
	x.updateHeight(root)
	x.updateHeight(right)
	return right
}

func (x *retiredIntervalIndex) rotateRight(root int32) int32 {
	left := x.nodeLeft(root)
	middle := x.nodes[left].right
	x.setNodeLeft(root, middle)
	x.nodes[left].right = root
	x.updateHeight(root)
	x.updateHeight(left)
	return left
}

func (x *retiredIntervalIndex) updateHeight(rank int32) {
	if rank == retiredIntervalNone {
		return
	}
	x.setNodeHeight(rank, uint8(
		max(x.nodeHeight(x.nodeLeft(rank)),
			x.nodeHeight(x.nodes[rank].right))+1,
	))
}

func (x *retiredIntervalIndex) nodeHeight(rank int32) int {
	if rank == retiredIntervalNone {
		return 0
	}
	return int(x.nodes[rank].leftHeight >> retiredIntervalLinkBits)
}

func (x *retiredIntervalIndex) nodeLeft(rank int32) int32 {
	return int32(x.nodes[rank].leftHeight&retiredIntervalLinkMask) - 1
}

func (x *retiredIntervalIndex) setNodeLeft(rank, left int32) {
	x.nodes[rank].leftHeight =
		x.nodes[rank].leftHeight&^retiredIntervalLinkMask |
			encodeRetiredIntervalLink(left)
}

func (x *retiredIntervalIndex) setNodeHeight(rank int32, height uint8) {
	x.nodes[rank].leftHeight =
		x.nodes[rank].leftHeight&retiredIntervalLinkMask |
			uint32(height)<<retiredIntervalLinkBits
}

func encodeRetiredIntervalLink(rank int32) uint32 {
	return uint32(rank + 1)
}
