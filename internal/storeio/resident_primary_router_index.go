package storeio

import (
	"fmt"
	"math/bits"
	"unsafe"
)

// residentBucketIndex is an immutable fixed-depth radix index over the full
// 32-bit BucketID. Path-copy edits share untouched nodes and route entries.
type residentBucketIndex struct {
	root    *residentBucketBranch
	count   int
	maximum BucketID
	hasMax  bool
	bytes   int
}

type residentBucketBranch struct {
	child  [16]*residentBucketBranch
	leaf   *residentBucketLeaf     // populated only after seven radix nibbles
	locals *residentTabletLocalSet // populated at the 20-bit tablet prefix
	bytes  int                     // owned bytes in this immutable subtree
}

type residentBucketLeaf struct {
	present uint16
	entry   [16]residentRouteEntry
}

type residentTabletLocalSet struct {
	words [TabletLocalIdentityLocalCount / 64]uint64
}

func (s *residentTabletLocalSet) contains(localID uint32) bool {
	return s != nil && localID < TabletLocalIdentityLocalCount &&
		s.words[localID>>6]&(uint64(1)<<(localID&63)) != 0
}

func residentBucketEntryID(entry residentRouteEntry) (BucketID, bool) {
	if entry.cell == nil {
		return 0, false
	}
	bucket := BucketID(uint32(entry.cell.meta.Load() >> 32))
	_, _, ok := SplitTabletLocalIdentityBucket(uint32(bucket))
	return bucket, ok
}

func residentBucketNibble(bucket BucketID, depth uint8) uint8 {
	return uint8(uint32(bucket) >> (28 - 4*depth) & 15)
}

// residentBucketBuild constructs mutable private nodes once, then publishes
// the completed immutable image. Input order is irrelevant and duplicates are
// rejected rather than silently replacing an entry.
func residentBucketBuild(entries []residentRouteEntry) (*residentBucketIndex, error) {
	index := &residentBucketIndex{}
	if len(entries) == 0 {
		return index, nil
	}
	index.root = &residentBucketBranch{}
	for _, entry := range entries {
		bucket, ok := residentBucketEntryID(entry)
		if !ok {
			return nil, fmt.Errorf("%w: resident bucket entry", ErrInvalidWrite)
		}
		node := index.root
		for depth := uint8(0); depth < 7; depth++ {
			nibble := residentBucketNibble(bucket, depth)
			if node.child[nibble] == nil {
				node.child[nibble] = &residentBucketBranch{}
			}
			node = node.child[nibble]
			if depth == 4 {
				if node.locals == nil {
					node.locals = &residentTabletLocalSet{}
				}
				_, localID, _ := SplitTabletLocalIdentityBucket(uint32(bucket))
				node.locals.words[localID>>6] |= uint64(1) << (localID & 63)
			}
		}
		if node.leaf == nil {
			node.leaf = &residentBucketLeaf{}
		}
		slot := residentBucketNibble(bucket, 7)
		bit := uint16(1) << slot
		if node.leaf.present&bit != 0 {
			return nil, fmt.Errorf("%w: duplicate resident bucket %d", ErrInvalidWrite, bucket)
		}
		node.leaf.present |= bit
		node.leaf.entry[slot] = entry
		index.count++
		if !index.hasMax || bucket > index.maximum {
			index.maximum, index.hasMax = bucket, true
		}
	}
	index.bytes = residentBucketFinalizeBytes(index.root) + int(unsafe.Sizeof(*index))
	return index, nil
}

func (index *residentBucketIndex) lookup(bucket BucketID) (residentRouteEntry, bool) {
	if index == nil || index.root == nil {
		return residentRouteEntry{}, false
	}
	node := index.root
	for depth := uint8(0); depth < 7; depth++ {
		node = node.child[residentBucketNibble(bucket, depth)]
		if node == nil {
			return residentRouteEntry{}, false
		}
	}
	if node.leaf == nil {
		return residentRouteEntry{}, false
	}
	slot := residentBucketNibble(bucket, 7)
	if node.leaf.present&(uint16(1)<<slot) == 0 {
		return residentRouteEntry{}, false
	}
	return node.leaf.entry[slot], true
}

func (index *residentBucketIndex) tabletLocals(tabletID uint32) (*residentTabletLocalSet, bool) {
	if index == nil || index.root == nil || tabletID >= TabletLocalIdentityTabletCount {
		return nil, false
	}
	node := index.root
	prefix := BucketID(tabletID << TabletLocalIdentityLocalBits)
	for depth := uint8(0); depth < 5; depth++ {
		node = node.child[residentBucketNibble(prefix, depth)]
		if node == nil {
			return nil, false
		}
	}
	return node.locals, node.locals != nil
}

func (index *residentBucketIndex) set(bucket BucketID, entry residentRouteEntry) (*residentBucketIndex, error) {
	entryBucket, ok := residentBucketEntryID(entry)
	if !ok || entryBucket != bucket {
		return nil, fmt.Errorf("%w: resident bucket set identity", ErrInvalidWrite)
	}
	old, existed := index.lookup(bucket)
	_ = old
	root := residentBucketSet(indexRoot(index), bucket, entry, 0)
	next := &residentBucketIndex{root: root, count: indexCount(index), maximum: bucket, hasMax: true}
	if existed {
		next.maximum, next.hasMax = index.maximum, index.hasMax
	} else if index != nil && index.hasMax && index.maximum > bucket {
		next.maximum = index.maximum
	}
	if !existed {
		next.count++
	}
	next.bytes = residentBucketOwnedBytes(root) + int(unsafe.Sizeof(*next))
	return next, nil
}

func indexRoot(index *residentBucketIndex) *residentBucketBranch {
	if index == nil {
		return nil
	}
	return index.root
}
func indexCount(index *residentBucketIndex) int {
	if index == nil {
		return 0
	}
	return index.count
}

func residentBucketSet(node *residentBucketBranch, bucket BucketID, entry residentRouteEntry, depth uint8) *residentBucketBranch {
	next := &residentBucketBranch{}
	if node != nil {
		*next = *node
	}
	if depth == 5 {
		set := &residentTabletLocalSet{}
		if next.locals != nil {
			*set = *next.locals
		}
		_, localID, _ := SplitTabletLocalIdentityBucket(uint32(bucket))
		set.words[localID>>6] |= uint64(1) << (localID & 63)
		next.locals = set
	}
	if depth == 7 {
		leaf := &residentBucketLeaf{}
		if next.leaf != nil {
			*leaf = *next.leaf
		}
		slot := residentBucketNibble(bucket, 7)
		leaf.present |= uint16(1) << slot
		leaf.entry[slot] = entry
		next.leaf = leaf
		residentBucketRecountBytes(next)
		return next
	}
	nibble := residentBucketNibble(bucket, depth)
	next.child[nibble] = residentBucketSet(next.child[nibble], bucket, entry, depth+1)
	residentBucketRecountBytes(next)
	return next
}

func (index *residentBucketIndex) delete(bucket BucketID) (*residentBucketIndex, bool) {
	if _, ok := index.lookup(bucket); !ok {
		return index, false
	}
	root := residentBucketDelete(index.root, bucket, 0)
	next := &residentBucketIndex{root: root, count: index.count - 1, maximum: index.maximum, hasMax: index.hasMax}
	if next.count == 0 {
		next.maximum, next.hasMax = 0, false
	} else if bucket == index.maximum {
		next.maximum, next.hasMax = residentBucketMaximum(root, 0, 0)
	}
	next.bytes = residentBucketOwnedBytes(root) + int(unsafe.Sizeof(*next))
	return next, true
}

func residentBucketDelete(node *residentBucketBranch, bucket BucketID, depth uint8) *residentBucketBranch {
	if node == nil {
		return nil
	}
	next := *node
	if depth == 5 && next.locals != nil {
		set := *next.locals
		_, localID, _ := SplitTabletLocalIdentityBucket(uint32(bucket))
		set.words[localID>>6] &^= uint64(1) << (localID & 63)
		if residentTabletLocalEmpty(&set) {
			next.locals = nil
		} else {
			next.locals = &set
		}
	}
	if depth == 7 {
		leaf := *next.leaf
		slot := residentBucketNibble(bucket, 7)
		leaf.present &^= uint16(1) << slot
		leaf.entry[slot] = residentRouteEntry{}
		if leaf.present == 0 {
			return nil
		}
		next.leaf = &leaf
		residentBucketRecountBytes(&next)
		return &next
	}
	nibble := residentBucketNibble(bucket, depth)
	next.child[nibble] = residentBucketDelete(next.child[nibble], bucket, depth+1)
	if next.leaf == nil && next.locals == nil {
		for _, child := range next.child {
			if child != nil {
				residentBucketRecountBytes(&next)
				return &next
			}
		}
		return nil
	}
	residentBucketRecountBytes(&next)
	return &next
}

func residentTabletLocalEmpty(set *residentTabletLocalSet) bool {
	for _, word := range set.words {
		if word != 0 {
			return false
		}
	}
	return true
}

func residentBucketMaximum(node *residentBucketBranch, depth uint8, prefix uint32) (BucketID, bool) {
	if node == nil {
		return 0, false
	}
	if depth == 7 {
		if node.leaf == nil || node.leaf.present == 0 {
			return 0, false
		}
		slot := 15 - bits.LeadingZeros16(node.leaf.present)
		return BucketID(prefix | uint32(slot)), true
	}
	for nibble := 15; nibble >= 0; nibble-- {
		if node.child[nibble] != nil {
			return residentBucketMaximum(node.child[nibble], depth+1,
				prefix|uint32(nibble)<<uint(28-4*depth))
		}
	}
	return 0, false
}

func (index *residentBucketIndex) max() (BucketID, bool) {
	if index == nil {
		return 0, false
	}
	return index.maximum, index.hasMax
}

func (index *residentBucketIndex) retainedBytes() int {
	if index == nil {
		return 0
	}
	return index.bytes
}

func residentBucketOwnedBytes(node *residentBucketBranch) int {
	if node == nil {
		return 0
	}
	return node.bytes
}

// residentBucketFinalizeBytes visits the private bulk-built tree once before
// publication. Persistent edits recount only their fixed-depth copied path.
func residentBucketFinalizeBytes(node *residentBucketBranch) int {
	if node == nil {
		return 0
	}
	for _, child := range node.child {
		residentBucketFinalizeBytes(child)
	}
	return residentBucketRecountBytes(node)
}

func residentBucketRecountBytes(node *residentBucketBranch) int {
	total := int(unsafe.Sizeof(*node))
	if node.leaf != nil {
		total += int(unsafe.Sizeof(*node.leaf))
	}
	if node.locals != nil {
		total += int(unsafe.Sizeof(*node.locals))
	}
	for _, child := range node.child {
		if child != nil {
			total += child.bytes
		}
	}
	node.bytes = total
	return total
}
