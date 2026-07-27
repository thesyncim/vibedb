package store

// Persistent bitmap postings for declared collection indexes.
//
// Collection slots are stable for the lifetime of a key and a chunk holds at
// most
// 64 slots, so one uint64 is the native posting unit. A posting starts inline
// for the overwhelmingly common one-to-four-chunk case. Wider postings promote
// to a sparse persistent radix vector: an update path-copies only the nodes
// leading to one chunk word, old snapshots keep their words, and an absent
// address range costs no leaf storage. Shrinking postings demote immediately.

const storeIndexInlineMasks = 4

type storeIndexChunkMask struct {
	chunk uint32
	mask  uint64
}

type storeIndexMasks struct {
	// Split ids from words to avoid the four bytes of alignment padding that
	// an array-of-structs would pay for every inline entry.
	chunks [storeIndexInlineMasks]uint32
	masks  [storeIndexInlineMasks]uint64
	n      uint8
	wide   storeIndexMaskVector
}

func (m storeIndexMasks) get(chunk uint32) uint64 {
	if m.wide.root != nil {
		return m.wide.get(chunk)
	}
	for i := 0; i < int(m.n); i++ {
		if m.chunks[i] == chunk {
			return m.masks[i]
		}
	}
	return 0
}

func (m storeIndexMasks) set(chunk uint32, mask uint64) storeIndexMasks {
	if m.wide.root != nil {
		m.wide = m.wide.set(chunk, mask)
		if m.wide.words <= storeIndexInlineMasks {
			var compact storeIndexMasks
			m.wide.each(func(id uint32, word uint64) bool {
				compact = compact.set(id, word)
				return true
			})
			return compact
		}
		return m
	}
	for i := 0; i < int(m.n); i++ {
		if m.chunks[i] != chunk {
			continue
		}
		if mask == 0 {
			copy(m.chunks[i:], m.chunks[i+1:int(m.n)])
			copy(m.masks[i:], m.masks[i+1:int(m.n)])
			m.n--
			m.chunks[m.n] = 0
			m.masks[m.n] = 0
		} else {
			m.masks[i] = mask
		}
		return m
	}
	if mask == 0 {
		return m
	}
	if m.n < storeIndexInlineMasks {
		i := int(m.n)
		for i > 0 && m.chunks[i-1] > chunk {
			m.chunks[i] = m.chunks[i-1]
			m.masks[i] = m.masks[i-1]
			i--
		}
		m.chunks[i] = chunk
		m.masks[i] = mask
		m.n++
		return m
	}
	for i := 0; i < int(m.n); i++ {
		m.wide = m.wide.set(m.chunks[i], m.masks[i])
	}
	m.chunks = [storeIndexInlineMasks]uint32{}
	m.masks = [storeIndexInlineMasks]uint64{}
	m.n = 0
	m.wide = m.wide.set(chunk, mask)
	return m
}

func (m storeIndexMasks) empty() bool {
	return m.n == 0 && m.wide.words == 0
}

func (m storeIndexMasks) each(fn func(uint32, uint64) bool) {
	if m.wide.root != nil {
		m.wide.each(fn)
		return
	}
	for i := 0; i < int(m.n); i++ {
		if !fn(m.chunks[i], m.masks[i]) {
			return
		}
	}
}

// storeIndexMaskIterator is a reusable cursor over one immutable posting.
// The radix has at most seven levels for a uint32 chunk id, so its complete
// traversal state fits inline. Advancing never allocates and never restarts at
// the root; this matters when a packed base is merged with a wide mutation
// delta one word at a time. The branch stack and the resident leaf are
// separate because the node types are: the stack holds levels depth..1 and the
// leaf is level zero, whose scan position must survive the calls between two
// words of the same leaf.
type storeIndexMaskIterator struct {
	inlineChunks [storeIndexInlineMasks]uint32
	inlineMasks  [storeIndexInlineMasks]uint64
	nodes        [8]*storeIndexMaskBranch
	prefixes     [8]uint32
	positions    [8]uint8
	leaf         *storeIndexMaskLeaf
	leafPrefix   uint32
	leafPos      uint8
	inlineN      uint8
	inlinePos    uint8
	depth        uint8
	top          int8
	wide         bool
}

func (m storeIndexMasks) iterator() storeIndexMaskIterator {
	if m.wide.root == nil {
		return storeIndexMaskIterator{
			inlineChunks: m.chunks,
			inlineMasks:  m.masks,
			inlineN:      m.n,
			top:          -1,
		}
	}
	it := storeIndexMaskIterator{depth: m.wide.depth, wide: true}
	it.nodes[0] = m.wide.root
	return it
}

func (it *storeIndexMaskIterator) next() (uint32, uint64, bool) {
	if it == nil {
		return 0, 0, false
	}
	if !it.wide {
		if it.inlinePos >= it.inlineN {
			return 0, 0, false
		}
		i := it.inlinePos
		it.inlinePos++
		return it.inlineChunks[i], it.inlineMasks[i], true
	}
	for {
		if it.leaf != nil {
			for i := int(it.leafPos); i < len(it.leaf.masks); i++ {
				it.leafPos = uint8(i + 1)
				if it.leaf.masks[i] != 0 {
					return it.leafPrefix | uint32(i), it.leaf.masks[i], true
				}
			}
			it.leaf = nil
		}
		if it.top < 0 {
			return 0, 0, false
		}
		top := int(it.top)
		node := it.nodes[top]
		level := int(it.depth) - top
		start := int(it.positions[top])
		descended := false
		if level == 1 {
			for i := start; i < len(node.leaves); i++ {
				it.positions[top] = uint8(i + 1)
				if leaf := node.leaves[i]; leaf != nil {
					it.leaf, it.leafPrefix, it.leafPos = leaf, it.prefixes[top]|uint32(i)<<5, 0
					descended = true
					break
				}
			}
		} else {
			for i := start; i < len(node.children); i++ {
				it.positions[top] = uint8(i + 1)
				child := node.children[i]
				if child == nil {
					continue
				}
				next := top + 1
				it.nodes[next] = child
				it.prefixes[next] = it.prefixes[top] | uint32(i)<<(uint(level)*5)
				it.positions[next] = 0
				it.top = int8(next)
				descended = true
				break
			}
		}
		if !descended {
			it.top--
		}
	}
}

func (m storeIndexMasks) next(from uint64) (uint32, uint64, bool) {
	if from > uint64(^uint32(0)) {
		return 0, 0, false
	}
	if m.wide.root != nil {
		return m.wide.next(from)
	}
	for i := 0; i < int(m.n); i++ {
		if uint64(m.chunks[i]) >= from {
			return m.chunks[i], m.masks[i], true
		}
	}
	return 0, 0, false
}

// storeIndexMasksFromSorted builds one immutable posting without the
// intermediate path copies used by online single-word updates. entries must be
// strictly ascending by chunk and contain no zero masks.
func storeIndexMasksFromSorted(entries []storeIndexChunkMask) storeIndexMasks {
	if len(entries) <= storeIndexInlineMasks {
		var out storeIndexMasks
		out.n = uint8(len(entries))
		for i, entry := range entries {
			out.chunks[i], out.masks[i] = entry.chunk, entry.mask
		}
		return out
	}
	maxID := entries[len(entries)-1].chunk
	depth := uint8(storeIndexMaskMinDepth)
	for uint64(maxID) >= storeIndexMaskCapacity(depth) {
		depth++
	}
	return storeIndexMasks{wide: storeIndexMaskVector{
		root:  storeIndexMaskBranchBuild(entries, depth),
		depth: depth,
		words: uint32(len(entries)),
	}}
}

// The radix node is split by level, because a level-tagged node that carries
// both arrays pays for both at every level while the level decides which one
// is live: half of every node is storage no read can ever reach. That waste is
// not a rounding error. On a 1000-distinct-value index over 50k documents the
// vector holds 1000 branches and 24848 leaves, and the dead arrays measured
// 6.62 MB — 49.1% of the entire index, 132 B per stored document. Splitting on
// the level every walk already computes removes it at the cost of one compare
// the loops already had.
//
// A branch still carries a child array and a leaf array because Go cannot type
// a pointer to "either", but that residue is bounded rather than proportional:
// a branch exists only to fan out to 32 children, so branches are at most one
// node per 32 below them, and the dead array they keep costs 1/32 of the array
// it replaces instead of 1/2. The maximum path is six branches plus a leaf for
// a uint32 chunk id.
type storeIndexMaskLeaf struct {
	masks [32]uint64
}

type storeIndexMaskBranch struct {
	children [32]*storeIndexMaskBranch // live at level >= 2
	leaves   [32]*storeIndexMaskLeaf   // live at level 1
}

// A wide vector is never shallower than one branch over its leaves, so the
// root always has the branch type and one pointer still describes it. The
// alternative — letting a posting confined to 32 chunks be a bare leaf —
// needs a second typed pointer in every vector, and because a vector is
// embedded in every posting that word is charged per distinct indexed value:
// it pushed storeIndexPostingLeaf from the 80-byte size class into the
// 96-byte one and cost 16 B per document on a unique-value index, to save one
// branch on postings that only exist in stores below 32 chunks.
type storeIndexMaskVector struct {
	root  *storeIndexMaskBranch
	depth uint8 // always >= 1 when root is non-nil
	words uint32
}

func (v storeIndexMaskVector) get(id uint32) uint64 {
	if v.root == nil || uint64(id) >= storeIndexMaskCapacity(v.depth) {
		return 0
	}
	node := v.root
	for level := v.depth; level > 1; level-- {
		node = node.children[(id>>(uint(level)*5))&31]
		if node == nil {
			return 0
		}
	}
	leaf := node.leaves[(id>>5)&31]
	if leaf == nil {
		return 0
	}
	return leaf.masks[id&31]
}

func (v storeIndexMaskVector) set(id uint32, mask uint64) storeIndexMaskVector {
	old := v.get(id)
	if old == mask {
		return v
	}
	if v.depth == 0 {
		v.depth = storeIndexMaskMinDepth
	}
	for uint64(id) >= storeIndexMaskCapacity(v.depth) {
		v.root = &storeIndexMaskBranch{children: [32]*storeIndexMaskBranch{v.root}}
		v.depth++
	}
	v.root = storeIndexMaskBranchSet(v.root, v.depth, id, mask)
	if old == 0 {
		v.words++
	} else if mask == 0 {
		v.words--
	}
	if v.words == 0 {
		return storeIndexMaskVector{}
	}
	for v.depth > storeIndexMaskMinDepth && storeIndexMaskOnlyFirstChild(v.root, v.depth) {
		v.root = v.root.children[0]
		v.depth--
	}
	return v
}

func storeIndexMaskOnlyFirstChild(node *storeIndexMaskBranch, level uint8) bool {
	if node == nil {
		return false
	}
	if level == 1 {
		if node.leaves[0] == nil {
			return false
		}
		for _, leaf := range node.leaves[1:] {
			if leaf != nil {
				return false
			}
		}
		return true
	}
	if node.children[0] == nil {
		return false
	}
	for _, child := range node.children[1:] {
		if child != nil {
			return false
		}
	}
	return true
}

// storeIndexMaskMinDepth is the shallowest wide vector: one branch of leaves,
// addressing 1024 chunks. See storeIndexMaskVector for why zero is excluded.
const storeIndexMaskMinDepth = 1

func storeIndexMaskCapacity(depth uint8) uint64 {
	return uint64(32) << (uint(depth) * 5)
}

func storeIndexMaskLeafSet(leaf *storeIndexMaskLeaf, id uint32, mask uint64) *storeIndexMaskLeaf {
	var out storeIndexMaskLeaf
	if leaf != nil {
		out = *leaf
	}
	out.masks[id&31] = mask
	if mask == 0 && storeIndexMaskLeafEmpty(&out) {
		return nil
	}
	return &out
}

func storeIndexMaskBranchSet(node *storeIndexMaskBranch, level uint8, id uint32, mask uint64) *storeIndexMaskBranch {
	var out storeIndexMaskBranch
	if node != nil {
		out = *node
	}
	if level == 1 {
		i := (id >> 5) & 31
		out.leaves[i] = storeIndexMaskLeafSet(out.leaves[i], id, mask)
	} else {
		i := (id >> (uint(level) * 5)) & 31
		out.children[i] = storeIndexMaskBranchSet(out.children[i], level-1, id, mask)
	}
	if mask == 0 && storeIndexMaskBranchEmpty(&out, level) {
		return nil
	}
	return &out
}

func storeIndexMaskLeafBuild(entries []storeIndexChunkMask) *storeIndexMaskLeaf {
	if len(entries) == 0 {
		return nil
	}
	leaf := new(storeIndexMaskLeaf)
	for _, entry := range entries {
		leaf.masks[entry.chunk&31] = entry.mask
	}
	return leaf
}

func storeIndexMaskBranchBuild(entries []storeIndexChunkMask, level uint8) *storeIndexMaskBranch {
	if len(entries) == 0 {
		return nil
	}
	node := new(storeIndexMaskBranch)
	shift := uint(level) * 5
	for first := 0; first < len(entries); {
		i := (entries[first].chunk >> shift) & 31
		last := first + 1
		for last < len(entries) && (entries[last].chunk>>shift)&31 == i {
			last++
		}
		if level == 1 {
			node.leaves[i] = storeIndexMaskLeafBuild(entries[first:last])
		} else {
			node.children[i] = storeIndexMaskBranchBuild(entries[first:last], level-1)
		}
		first = last
	}
	return node
}

func storeIndexMaskLeafEmpty(leaf *storeIndexMaskLeaf) bool {
	for _, mask := range leaf.masks {
		if mask != 0 {
			return false
		}
	}
	return true
}

func storeIndexMaskBranchEmpty(node *storeIndexMaskBranch, level uint8) bool {
	if level == 1 {
		for _, leaf := range node.leaves {
			if leaf != nil {
				return false
			}
		}
		return true
	}
	for _, child := range node.children {
		if child != nil {
			return false
		}
	}
	return true
}

func (v storeIndexMaskVector) each(fn func(uint32, uint64) bool) {
	storeIndexMaskBranchEach(v.root, v.depth, 0, fn)
}

func (v storeIndexMaskVector) next(from uint64) (uint32, uint64, bool) {
	if v.root == nil || from > uint64(^uint32(0)) || from >= storeIndexMaskCapacity(v.depth) {
		return 0, 0, false
	}
	return storeIndexMaskBranchNext(v.root, v.depth, 0, from)
}

func storeIndexMaskLeafNext(leaf *storeIndexMaskLeaf, prefix, from uint64) (uint32, uint64, bool) {
	if leaf == nil {
		return 0, 0, false
	}
	start := 0
	if from > prefix {
		// Compare in uint64 before narrowing: a 32-bit int cannot hold every
		// chunk-id difference, and this package has broken the 386 build twice
		// on exactly that narrowing.
		skip := from - prefix
		if skip >= uint64(len(leaf.masks)) {
			return 0, 0, false
		}
		start = int(skip)
	}
	for i := start; i < len(leaf.masks); i++ {
		if leaf.masks[i] != 0 {
			return uint32(prefix + uint64(i)), leaf.masks[i], true
		}
	}
	return 0, 0, false
}

func storeIndexMaskBranchNext(node *storeIndexMaskBranch, level uint8, prefix, from uint64) (uint32, uint64, bool) {
	if node == nil {
		return 0, 0, false
	}
	shift := uint(level) * 5
	start := 0
	if from > prefix {
		skip := (from - prefix) >> shift
		if skip >= 32 {
			return 0, 0, false
		}
		start = int(skip)
	}
	for i := start; i < 32; i++ {
		childPrefix := prefix + uint64(i)<<shift
		childFrom := max(from, childPrefix)
		var id uint32
		var mask uint64
		var ok bool
		if level == 1 {
			id, mask, ok = storeIndexMaskLeafNext(node.leaves[i], childPrefix, childFrom)
		} else {
			id, mask, ok = storeIndexMaskBranchNext(node.children[i], level-1, childPrefix, childFrom)
		}
		if ok {
			return id, mask, true
		}
	}
	return 0, 0, false
}

func storeIndexMaskLeafEach(leaf *storeIndexMaskLeaf, prefix uint32, fn func(uint32, uint64) bool) bool {
	if leaf == nil {
		return true
	}
	for i, mask := range leaf.masks {
		if mask != 0 && !fn(prefix|uint32(i), mask) {
			return false
		}
	}
	return true
}

func storeIndexMaskBranchEach(node *storeIndexMaskBranch, level uint8, prefix uint32, fn func(uint32, uint64) bool) bool {
	if node == nil {
		return true
	}
	shift := uint(level) * 5
	if level == 1 {
		for i, leaf := range node.leaves {
			if leaf != nil && !storeIndexMaskLeafEach(leaf, prefix|uint32(i)<<shift, fn) {
				return false
			}
		}
		return true
	}
	for i, child := range node.children {
		if child != nil && !storeIndexMaskBranchEach(child, level-1, prefix|uint32(i)<<shift, fn) {
			return false
		}
	}
	return true
}

// A separate persistent HAMT maps a composite scalar fingerprint to its
// posting. Fingerprints are candidate routing only: a full-hash collision
// deliberately shares one posting and the lookup's exact JSON recheck removes
// false positives. That removes stored composite-key copies without weakening
// correctness.
type storeIndexPostingLeaf struct {
	hash  uint64
	masks storeIndexMasks
}

type storeIndexPostingSlot struct {
	child *storeIndexPostingNode
	leaf  *storeIndexPostingLeaf
}

const storeIndexTrieBits = 5

type storeIndexPostingNode struct {
	slots [1 << storeIndexTrieBits]storeIndexPostingSlot
}

func storeIndexPostingLookup(root *storeIndexPostingNode, hash uint64) (storeIndexMasks, bool) {
	for shift := uint(0); root != nil; shift += storeIndexTrieBits {
		slot := root.slots[(hash>>shift)&31]
		if slot.leaf != nil {
			if slot.leaf.hash == hash {
				return slot.leaf.masks, true
			}
			return storeIndexMasks{}, false
		}
		root = slot.child
	}
	return storeIndexMasks{}, false
}

func storeIndexPostingSet(root *storeIndexPostingNode, hash uint64, chunk uint32, bit uint64, present bool) *storeIndexPostingNode {
	return storeIndexPostingSetMask(root, hash, chunk, bit, present)
}

func storeIndexPostingSetMask(root *storeIndexPostingNode, hash uint64, chunk uint32, change uint64, present bool) *storeIndexPostingNode {
	masks, _ := storeIndexPostingLookup(root, hash)
	word := masks.get(chunk)
	if present {
		word |= change
	} else {
		word &^= change
	}
	masks = masks.set(chunk, word)
	if masks.empty() {
		return storeIndexPostingDelete(root, 0, hash)
	}
	return storeIndexPostingInsert(root, 0, &storeIndexPostingLeaf{hash: hash, masks: masks})
}

// storeIndexPostingBuild creates a complete directory from unique leaves in
// low-radix-first order. It allocates each reachable HAMT node exactly once.
func storeIndexPostingBuild(leaves []*storeIndexPostingLeaf, shift uint) *storeIndexPostingNode {
	if len(leaves) == 0 {
		return nil
	}
	node := new(storeIndexPostingNode)
	for first := 0; first < len(leaves); {
		i := (leaves[first].hash >> shift) & 31
		last := first + 1
		for last < len(leaves) && (leaves[last].hash>>shift)&31 == i {
			last++
		}
		if last-first == 1 {
			node.slots[i].leaf = leaves[first]
		} else {
			node.slots[i].child = storeIndexPostingBuild(leaves[first:last], shift+storeIndexTrieBits)
		}
		first = last
	}
	return node
}

func storeIndexPostingInsert(root *storeIndexPostingNode, shift uint, add *storeIndexPostingLeaf) *storeIndexPostingNode {
	var out storeIndexPostingNode
	if root != nil {
		out = *root
	}
	i := (add.hash >> shift) & 31
	slot := out.slots[i]
	if slot.child != nil {
		slot.child = storeIndexPostingInsert(slot.child, shift+storeIndexTrieBits, add)
		out.slots[i] = slot
		return &out
	}
	if slot.leaf == nil || slot.leaf.hash == add.hash {
		slot.leaf = add
		out.slots[i] = slot
		return &out
	}
	child := storeIndexPostingInsert(nil, shift+storeIndexTrieBits, slot.leaf)
	child = storeIndexPostingInsert(child, shift+storeIndexTrieBits, add)
	out.slots[i] = storeIndexPostingSlot{child: child}
	return &out
}

func storeIndexPostingDelete(root *storeIndexPostingNode, shift uint, hash uint64) *storeIndexPostingNode {
	if root == nil {
		return nil
	}
	i := (hash >> shift) & 31
	slot := root.slots[i]
	if slot.child != nil {
		next := storeIndexPostingDelete(slot.child, shift+storeIndexTrieBits, hash)
		if next == slot.child {
			return root
		}
		slot.child = next
		if leaf, ok := storeIndexPostingSingleton(next); ok {
			slot = storeIndexPostingSlot{leaf: leaf}
		}
	} else {
		if slot.leaf == nil || slot.leaf.hash != hash {
			return root
		}
		slot.leaf = nil
	}
	out := *root
	out.slots[i] = slot
	if storeIndexPostingNodeEmpty(&out) {
		return nil
	}
	return &out
}

func storeIndexPostingNodeEmpty(node *storeIndexPostingNode) bool {
	for i := range node.slots {
		if node.slots[i].child != nil || node.slots[i].leaf != nil {
			return false
		}
	}
	return true
}

func storeIndexPostingSingleton(node *storeIndexPostingNode) (*storeIndexPostingLeaf, bool) {
	if node == nil {
		return nil, false
	}
	var one *storeIndexPostingLeaf
	for i := range node.slots {
		slot := node.slots[i]
		if slot.child != nil {
			return nil, false
		}
		if slot.leaf != nil {
			if one != nil {
				return nil, false
			}
			one = slot.leaf
		}
	}
	return one, one != nil
}
