// Package storekey owns the persistent key directory used by keyed stores.
//
// Its compact row location stays pointer-free and has the same concrete
// register representation at every call site; no interface or conversion is
// retained in a node.
package storekey

// Persistent hash trie for a mutable keyed store.
//
// The first 15 hash bits use cache-hot fixed 32-way nodes. Their terminal
// slots keep up to two distinct hashes in a short leaf bucket; only the rarer
// third collision allocates another node. Every mutation path-copies its route.
// Full-hash collisions remain an immutable leaf chain and compare complete
// keys.

const (
	trieBits        = 5
	keyBucketShift  = 10 // The third fixed level consumes bits 10..14.
	keyLeafBucket   = 2
	keyBranchFactor = 1 << trieBits
)

// Location is the compact stable row address stored in each key leaf.
type Location struct {
	Chunk uint32
	Slot  uint8
}

type keyLeaf struct {
	hash uint64
	key  string
	loc  Location
	next *keyLeaf
}

type keySlot struct {
	child *Node
	leaf  *keyLeaf
}

// Node is one immutable directory node. Its representation is intentionally
// opaque; callers retain only the root pointer published in their snapshot.
type Node struct {
	slots [keyBranchFactor]keySlot
}

// Lookup resolves the exact key selected by hash.
func Lookup(root *Node, hash uint64, key string) (Location, bool) {
	remaining := hash
	for root != nil {
		slot := &root.slots[remaining&31]
		for leaf := slot.leaf; leaf != nil; leaf = leaf.next {
			if leaf.hash == hash && leaf.key == key {
				return leaf.loc, true
			}
		}
		root = slot.child
		remaining >>= trieBits
	}
	var zero Location
	return zero, false
}

// Insert returns a new root containing key. The caller has already established
// whether this is an insert or a location replacement.
func Insert(root *Node, hash uint64, key string, loc Location) *Node {
	return insertAt(root, 0, &keyLeaf{hash: hash, key: key, loc: loc})
}

func insertAt(root *Node, shift uint, add *keyLeaf) *Node {
	var out Node
	if root != nil {
		out = *root
	}
	i := (add.hash >> shift) & 31
	slot := out.slots[i]
	if slot.child != nil {
		slot.child = insertAt(slot.child, shift+trieBits, add)
		out.slots[i] = slot
		return &out
	}
	if slot.leaf == nil {
		slot.leaf = add
		out.slots[i] = slot
		return &out
	}

	// Complete-hash collisions stay in one leaf chain. Copying the chain also
	// permits an existing key's location to change without mutating a snapshot.
	if leafHasHash(slot.leaf, add.hash) {
		slot.leaf = leafInsert(slot.leaf, add)
		out.slots[i] = slot
		return &out
	}

	// After the cache-hot 15-bit prefix, a two-leaf bucket is cheaper in both
	// bytes and dependent loads than another 512-byte node. Promote only the
	// third distinct hash. The same policy in rare deeper nodes bounds skewed
	// tries without adding a branch to lookup.
	if shift >= keyBucketShift && leafCount(slot.leaf) < keyLeafBucket {
		slot.leaf = leafInsert(slot.leaf, add)
		out.slots[i] = slot
		return &out
	}

	var child *Node
	for leaf := slot.leaf; leaf != nil; leaf = leaf.next {
		child = insertAt(child, shift+trieBits, &keyLeaf{
			hash: leaf.hash, key: leaf.key, loc: leaf.loc,
		})
	}
	child = insertAt(child, shift+trieBits, add)
	out.slots[i] = keySlot{child: child}
	return &out
}

func leafInsert(head, add *keyLeaf) *keyLeaf {
	if head == nil {
		return add
	}
	if head.key == add.key {
		return &keyLeaf{hash: add.hash, key: add.key, loc: add.loc, next: head.next}
	}
	return &keyLeaf{
		hash: head.hash, key: head.key, loc: head.loc,
		next: leafInsert(head.next, add),
	}
}

func leafHasHash(leaf *keyLeaf, hash uint64) bool {
	for ; leaf != nil; leaf = leaf.next {
		if leaf.hash == hash {
			return true
		}
	}
	return false
}

func leafCount(leaf *keyLeaf) int {
	n := 0
	for ; leaf != nil && n < keyLeafBucket; leaf = leaf.next {
		n++
	}
	return n
}

// Delete returns a root without key. Missing keys preserve the original root.
func Delete(root *Node, hash uint64, key string) *Node {
	out, _ := deleteAt(root, 0, hash, key)
	return out
}

func deleteAt(root *Node, shift uint, hash uint64, key string) (*Node, bool) {
	if root == nil {
		return nil, false
	}
	i := (hash >> shift) & 31
	slot := root.slots[i]
	var changed bool
	if slot.child != nil {
		slot.child, changed = deleteAt(slot.child, shift+trieBits, hash, key)
		if !changed {
			return root, false
		}
		if shift >= keyBucketShift {
			if leaf, ok := nodeLeafBucket(slot.child); ok {
				slot = keySlot{leaf: leaf}
			}
		} else if leaf, ok := singleton(slot.child); ok {
			slot = keySlot{leaf: leaf}
		}
	} else {
		slot.leaf, changed = leafDelete(slot.leaf, hash, key)
		if !changed {
			return root, false
		}
	}
	out := *root
	out.slots[i] = slot
	if nodeEmpty(&out) {
		return nil, true
	}
	return &out, true
}

func leafDelete(head *keyLeaf, hash uint64, key string) (*keyLeaf, bool) {
	if head == nil {
		return nil, false
	}
	if head.hash == hash && head.key == key {
		return head.next, true
	}
	next, changed := leafDelete(head.next, hash, key)
	if !changed {
		return head, false
	}
	return &keyLeaf{hash: head.hash, key: head.key, loc: head.loc, next: next}, true
}

func nodeEmpty(node *Node) bool {
	for i := range node.slots {
		if node.slots[i].child != nil || node.slots[i].leaf != nil {
			return false
		}
	}
	return true
}

// singleton permits deletion to collapse a child with one occupied leaf slot
// back into its parent. A full-hash collision chain is one leaf slot and can be
// shared there unchanged.
func singleton(node *Node) (*keyLeaf, bool) {
	if node == nil {
		return nil, false
	}
	var one *keyLeaf
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

// nodeLeafBucket flattens a promoted subtree as soon as deletion brings it
// back within the leaf-bucket bound. Counting uses a fixed stack buffer, so
// failed collapse checks do not allocate during churn.
func nodeLeafBucket(node *Node) (*keyLeaf, bool) {
	if node == nil {
		return nil, false
	}
	var leaves [keyLeafBucket]*keyLeaf
	n := 0
	if !collectLeaves(node, &leaves, &n) || n == 0 {
		return nil, false
	}
	var head *keyLeaf
	for i := n - 1; i >= 0; i-- {
		leaf := leaves[i]
		head = &keyLeaf{hash: leaf.hash, key: leaf.key, loc: leaf.loc, next: head}
	}
	return head, true
}

func collectLeaves(node *Node, leaves *[keyLeafBucket]*keyLeaf, n *int) bool {
	if node == nil {
		return true
	}
	for i := range node.slots {
		slot := node.slots[i]
		if slot.child != nil && !collectLeaves(slot.child, leaves, n) {
			return false
		}
		for leaf := slot.leaf; leaf != nil; leaf = leaf.next {
			if *n == len(leaves) {
				return false
			}
			leaves[*n] = leaf
			(*n)++
		}
	}
	return true
}
