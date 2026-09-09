package storeio

import (
	"bytes"
	"sync/atomic"
	"unsafe"
)

const residentRouteBlockSize = 128
const residentRouteFanout = 64

type residentRouteCell struct {
	seq        atomic.Uint64
	offset     atomic.Uint64
	generation atomic.Uint64
	meta       atomic.Uint64
	hint       pageCacheFrameHint
	empty      atomic.Uint32
}

type residentRouteEntry struct {
	fence []byte
	cell  *residentRouteCell
}
type residentRouteNode struct {
	leaves       []residentRouteEntry
	children     []*residentRouteNode
	counts       []int
	first        []byte
	total        int
	bytes        int
	firstPacked  uint64
	searchKeys   []uint64
	searchPrefix []byte
	searchSkip   int
	cellArena    []residentRouteCell
	fenceArena   []byte
}

func newResidentRouteLeaf(entries []residentRouteEntry) *residentRouteNode {
	owned := append([]residentRouteEntry(nil), entries...)
	n := &residentRouteNode{leaves: owned, total: len(owned)}
	if len(entries) > 0 {
		n.first = entries[0].fence
		n.firstPacked = packLexicalWindow(n.first)
	}
	n.initSearch(len(owned), func(i int) []byte { return owned[i].fence })
	n.bytes = int(unsafe.Sizeof(*n)) + cap(n.leaves)*int(unsafe.Sizeof(residentRouteEntry{})) + cap(n.searchKeys)*8
	// Structural images may share these immutable fences and coherent cells.
	// Count their logical reachable size per image, which is conservative when
	// multiple live snapshots share the same allocation.
	for _, entry := range owned {
		n.bytes += len(entry.fence) + int(unsafe.Sizeof(residentRouteCell{}))
	}
	return n
}

func newResidentRouteBranch(children []*residentRouteNode) *residentRouteNode {
	owned := append([]*residentRouteNode(nil), children...)
	n := &residentRouteNode{children: owned, counts: make([]int, len(owned))}
	if len(children) > 0 {
		n.first = children[0].first
		n.firstPacked = children[0].firstPacked
	}
	n.initSearch(len(owned), func(i int) []byte { return owned[i].first })
	for i, c := range children {
		n.total += c.total
		n.counts[i] = n.total
		n.bytes += c.bytes
	}
	n.bytes += int(unsafe.Sizeof(*n)) + cap(n.children)*int(unsafe.Sizeof((*residentRouteNode)(nil))) + cap(n.counts)*int(unsafe.Sizeof(int(0))) + cap(n.searchKeys)*8
	return n
}

func (n *residentRouteNode) initSearch(count int, fence func(int) []byte) {
	n.searchKeys = make([]uint64, count)
	first := 0
	for first < count && len(fence(first)) == 0 {
		first++
	}
	if first == count {
		return
	}
	a, b := fence(first), fence(count-1)
	for n.searchSkip < len(a) && n.searchSkip < len(b) && a[n.searchSkip] == b[n.searchSkip] {
		n.searchSkip++
	}
	n.searchPrefix = a[:n.searchSkip]
	for i := range count {
		f := fence(i)
		if len(f) >= n.searchSkip {
			n.searchKeys[i] = packLexicalWindow(f[n.searchSkip:])
		}
	}
}

func (n *residentRouteNode) searchFloor(key []byte, count int, fence func(int) []byte) int {
	if n.searchSkip != 0 {
		head := key
		if len(head) > n.searchSkip {
			head = head[:n.searchSkip]
		}
		if c := bytes.Compare(head, n.searchPrefix); c != 0 {
			if c < 0 {
				return 0
			}
			return count - 1
		}
		if len(key) < n.searchSkip {
			return 0
		}
	}
	window := packLexicalWindow(key[n.searchSkip:])
	lo, hi := 0, count
	for lo < hi {
		m := int(uint(lo+hi) >> 1)
		packed := n.searchKeys[m]
		if packed < window || packed == window && bytes.Compare(fence(m), key) <= 0 {
			lo = m + 1
		} else {
			hi = m
		}
	}
	return max(0, lo-1)
}

func buildResidentRouteTree(entries []residentRouteEntry) *residentRouteNode {
	level := make([]*residentRouteNode, 0, (len(entries)+residentRouteBlockSize-1)/residentRouteBlockSize)
	for i := 0; i < len(entries); i += residentRouteBlockSize {
		j := min(i+residentRouteBlockSize, len(entries))
		level = append(level, newResidentRouteLeaf(entries[i:j]))
	}
	for len(level) > 1 {
		next := make([]*residentRouteNode, 0, (len(level)+residentRouteFanout-1)/residentRouteFanout)
		for i := 0; i < len(level); i += residentRouteFanout {
			j := min(i+residentRouteFanout, len(level))
			next = append(next, newResidentRouteBranch(level[i:j]))
		}
		level = next
	}
	if len(level) == 0 {
		return nil
	}
	return level[0]
}

func (n *residentRouteNode) at(rank int) (residentRouteEntry, bool) {
	if n == nil || rank < 0 || rank >= n.total {
		return residentRouteEntry{}, false
	}
	for len(n.children) != 0 {
		lo, hi := 0, len(n.counts)
		for lo < hi {
			m := int(uint(lo+hi) >> 1)
			if n.counts[m] <= rank {
				lo = m + 1
			} else {
				hi = m
			}
		}
		i := lo
		if i > 0 {
			rank -= n.counts[i-1]
		}
		n = n.children[i]
	}
	return n.leaves[rank], true
}

func (n *residentRouteNode) floor(key []byte) (residentRouteEntry, int, bool) {
	if n == nil {
		return residentRouteEntry{}, 0, false
	}
	base := 0
	for len(n.children) != 0 {
		i := n.searchFloor(key, len(n.children), func(i int) []byte { return n.children[i].first })
		if i > 0 {
			base += n.counts[i-1]
		}
		n = n.children[i]
	}
	i := n.searchFloor(key, len(n.leaves), func(i int) []byte { return n.leaves[i].fence })
	return n.leaves[i], base + i, true
}

func residentCellRoute(e residentRouteEntry, rank int, hash uint64) (ResidentPrimaryRoute, bool) {
	c := e.cell
	for {
		before := c.seq.Load()
		if before&1 != 0 {
			continue
		}
		off := c.offset.Load()
		gen := c.generation.Load()
		meta := c.meta.Load()
		if before != c.seq.Load() {
			continue
		}
		bucket := BucketID(uint32(meta >> 32))
		id, ok := CommonPrimaryLeafLogicalID(bucket)
		if !ok {
			return ResidentPrimaryRoute{}, false
		}
		return ResidentPrimaryRoute{Ref: PageRef{Offset: off, LogicalID: id, Generation: gen, Length: uint32(meta), Kind: PagePrimaryLeaf}, Bucket: bucket, Hash: hash, rank: uint32(rank), cell: c}, true
	}
}

func (r *ResidentPrimaryRouter) buildPersistentTree() {
	count := len(r.rows) / residentPrimaryRouterWords
	leaves := make([]*residentRouteNode, 0, (count+residentRouteBlockSize-1)/residentRouteBlockSize)
	for base := 0; base < count; base += residentRouteBlockSize {
		end := min(base+residentRouteBlockSize, count)
		n := &residentRouteNode{leaves: make([]residentRouteEntry, end-base), cellArena: make([]residentRouteCell, end-base), total: end - base}
		fenceBytes := 0
		for rank := base; rank < end; rank++ {
			fenceBytes += len(r.flatFence(rank))
		}
		n.fenceArena = make([]byte, 0, fenceBytes)
		for rank := base; rank < end; rank++ {
			at := rank * residentPrimaryRouterWords
			word := r.rows[at]
			f := r.fences[uint32(word):uint32(word>>32)]
			start := len(n.fenceArena)
			n.fenceArena = append(n.fenceArena, f...)
			cell := &n.cellArena[rank-base]
			cell.offset.Store(r.rows[at+1])
			cell.generation.Store(r.rows[at+2])
			cell.meta.Store(r.rows[at+3])
			cell.empty.Store(r.empty[rank].Load())
			n.leaves[rank-base] = residentRouteEntry{fence: n.fenceArena[start:len(n.fenceArena):len(n.fenceArena)], cell: cell}
		}
		n.first = n.leaves[0].fence
		n.firstPacked = packLexicalWindow(n.first)
		n.initSearch(len(n.leaves), func(i int) []byte { return n.leaves[i].fence })
		n.bytes = int(unsafe.Sizeof(*n)) + cap(n.leaves)*int(unsafe.Sizeof(residentRouteEntry{})) + cap(n.cellArena)*int(unsafe.Sizeof(residentRouteCell{})) + cap(n.fenceArena) + cap(n.searchKeys)*8
		leaves = append(leaves, n)
	}
	r.tree = residentTreeRoot(leaves)
	r.treeBytes = r.tree.bytes
	r.buckets, _ = residentBucketBuild(r.treeEntries())
	r.refreshTreeFloor()
}

func (r *ResidentPrimaryRouter) refreshTreeFloor() {
	if r.tree == nil {
		r.floorEntry, r.firstRealFence, r.firstRealPacked = residentRouteEntry{}, nil, 0
		return
	}
	r.floorEntry, _ = r.tree.at(0)
	if r.tree.total > 1 {
		e, _ := r.tree.at(1)
		r.firstRealFence, r.firstRealPacked = e.fence, packLexicalWindow(e.fence)
	}
}

func (r *ResidentPrimaryRouter) flatFence(rank int) []byte {
	word := r.rows[rank*residentPrimaryRouterWords]
	return r.fences[uint32(word):uint32(word>>32)]
}

func (r *ResidentPrimaryRouter) treeEntries() []residentRouteEntry {
	entries := make([]residentRouteEntry, r.Len())
	for i := range entries {
		entries[i], _ = r.tree.at(i)
	}
	return entries
}

func replaceResidentTree(root *residentRouteNode, rank int, repl []residentRouteEntry) []*residentRouteNode {
	return replaceResidentTreeRange(root, rank, 1, repl)
}

func replaceResidentTreeRange(root *residentRouteNode, rank, remove int, repl []residentRouteEntry) []*residentRouteNode {
	if len(root.children) == 0 {
		out := make([]residentRouteEntry, 0, len(root.leaves)-remove+len(repl))
		out = append(out, root.leaves[:rank]...)
		out = append(out, repl...)
		out = append(out, root.leaves[rank+remove:]...)
		nodes := make([]*residentRouteNode, 0, (len(out)+residentRouteBlockSize-1)/residentRouteBlockSize)
		for len(out) > 0 {
			parts := (len(out) + residentRouteBlockSize - 1) / residentRouteBlockSize
			n := (len(out) + parts - 1) / parts
			nodes = append(nodes, newResidentRouteLeaf(out[:n]))
			out = out[n:]
		}
		return nodes
	}
	kids := make([]*residentRouteNode, 0, len(root.children)+1)
	remaining, cursor, inserted := remove, 0, false
	for _, child := range root.children {
		childEnd := cursor + child.total
		if remaining == 0 || childEnd <= rank {
			kids = append(kids, child)
			cursor = childEnd
			continue
		}
		local := max(0, rank-cursor)
		take := min(remaining, child.total-local)
		var add []residentRouteEntry
		if !inserted {
			add, inserted = repl, true
		}
		kids = append(kids, replaceResidentTreeRange(child, local, take, add)...)
		remaining -= take
		cursor = childEnd
	}
	kids = coalesceResidentNodes(kids)
	nodes := make([]*residentRouteNode, 0, (len(kids)+residentRouteFanout-1)/residentRouteFanout)
	for len(kids) > 0 {
		parts := (len(kids) + residentRouteFanout - 1) / residentRouteFanout
		n := (len(kids) + parts - 1) / parts
		nodes = append(nodes, newResidentRouteBranch(kids[:n]))
		kids = kids[n:]
	}
	return nodes
}

func (r *ResidentPrimaryRouter) replacePersistentRange(rank, remove int, repl []residentRouteEntry, generation uint64) *ResidentPrimaryRouter {
	old := make([]residentRouteEntry, remove)
	for i := range old {
		old[i], _ = r.tree.at(rank + i)
	}
	root := residentTreeRoot(replaceResidentTreeRange(r.tree, rank, remove, repl))
	buckets := r.buckets
	for _, entry := range old {
		buckets, _ = buckets.delete(mustResidentCellBucket(entry.cell))
	}
	for _, entry := range repl {
		buckets, _ = buckets.set(mustResidentCellBucket(entry.cell), entry)
	}
	next := &ResidentPrimaryRouter{storeID: r.storeID, tree: root, buckets: buckets}
	if root != nil {
		next.treeBytes = root.bytes
	}
	next.generation.Store(generation)
	next.refreshTreeFloor()
	return next
}

func coalesceResidentNodes(nodes []*residentRouteNode) []*residentRouteNode {
	for i := 0; i+1 < len(nodes); {
		a, b := nodes[i], nodes[i+1]
		var merged *residentRouteNode
		if len(a.children) == 0 && len(b.children) == 0 && len(a.leaves)+len(b.leaves) <= residentRouteBlockSize {
			x := append(append(make([]residentRouteEntry, 0, len(a.leaves)+len(b.leaves)), a.leaves...), b.leaves...)
			merged = newResidentRouteLeaf(x)
		}
		if len(a.children) != 0 && len(b.children) != 0 && len(a.children)+len(b.children) <= residentRouteFanout {
			x := append(append(make([]*residentRouteNode, 0, len(a.children)+len(b.children)), a.children...), b.children...)
			merged = newResidentRouteBranch(x)
		}
		if merged == nil {
			i++
			continue
		}
		nodes = append(nodes[:i], append([]*residentRouteNode{merged}, nodes[i+2:]...)...)
	}
	return nodes
}

func residentTreeRoot(nodes []*residentRouteNode) *residentRouteNode {
	for len(nodes) > 1 {
		next := make([]*residentRouteNode, 0, (len(nodes)+residentRouteFanout-1)/residentRouteFanout)
		for len(nodes) > 0 {
			n := min(len(nodes), residentRouteFanout)
			next = append(next, newResidentRouteBranch(nodes[:n]))
			nodes = nodes[n:]
		}
		nodes = next
	}
	if len(nodes) == 0 {
		return nil
	}
	root := nodes[0]
	for root != nil && len(root.children) == 1 {
		root = root.children[0]
	}
	return root
}

func newResidentRouteEntry(fence []byte, ref PageRef, bucket BucketID) residentRouteEntry {
	c := &residentRouteCell{}
	c.offset.Store(ref.Offset)
	c.generation.Store(ref.Generation)
	c.meta.Store(uint64(ref.Length) | uint64(uint32(bucket))<<32)
	return residentRouteEntry{fence: append([]byte(nil), fence...), cell: c}
}

func (r *ResidentPrimaryRouter) replacePersistent(rank int, repl []residentRouteEntry, generation uint64) *ResidentPrimaryRouter {
	old, _ := r.tree.at(rank)
	root := residentTreeRoot(replaceResidentTree(r.tree, rank, repl))
	buckets, _ := r.buckets.delete(mustResidentCellBucket(old.cell))
	for _, entry := range repl {
		buckets, _ = buckets.set(mustResidentCellBucket(entry.cell), entry)
	}
	next := &ResidentPrimaryRouter{storeID: r.storeID, tree: root, treeBytes: root.bytes, buckets: buckets}
	next.generation.Store(generation)
	next.refreshTreeFloor()
	return next
}

func mustResidentCellRef(c *residentRouteCell) PageRef {
	meta := c.meta.Load()
	bucket := BucketID(uint32(meta >> 32))
	id, _ := CommonPrimaryLeafLogicalID(bucket)
	return PageRef{Offset: c.offset.Load(), LogicalID: id, Generation: c.generation.Load(), Length: uint32(meta), Kind: PagePrimaryLeaf}
}
func mustResidentCellBucket(c *residentRouteCell) BucketID {
	return BucketID(uint32(c.meta.Load() >> 32))
}
