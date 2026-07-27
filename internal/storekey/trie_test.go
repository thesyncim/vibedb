package storekey

import (
	"fmt"
	"testing"
)

func TestSharedPrefixAndFullHashCollision(t *testing.T) {
	var root *Node
	root = Insert(root, 7, "collision-a", Location{Chunk: 1, Slot: 2})
	root = Insert(root, 7, "collision-b", Location{Chunk: 3, Slot: 4})
	root = Insert(root, 0, "low", Location{Chunk: 5, Slot: 6})
	root = Insert(root, uint64(1)<<63, "high", Location{Chunk: 7, Slot: 8})
	for _, test := range []struct {
		key  string
		hash uint64
		loc  Location
	}{
		{"collision-a", 7, Location{Chunk: 1, Slot: 2}},
		{"collision-b", 7, Location{Chunk: 3, Slot: 4}},
		{"low", 0, Location{Chunk: 5, Slot: 6}},
		{"high", uint64(1) << 63, Location{Chunk: 7, Slot: 8}},
	} {
		if got, ok := Lookup(root, test.hash, test.key); !ok || got != test.loc {
			t.Fatalf("lookup %q = (%+v,%v), want (%+v,true)", test.key, got, ok, test.loc)
		}
	}
	old := root
	root = Delete(root, 7, "collision-a")
	if _, ok := Lookup(root, 7, "collision-a"); ok {
		t.Fatal("deleted collision remains")
	}
	if _, ok := Lookup(root, 7, "collision-b"); !ok {
		t.Fatal("sibling collision was deleted")
	}
	if _, ok := Lookup(old, 7, "collision-a"); !ok {
		t.Fatal("delete changed retained root")
	}
}

func TestTailPromotionAndDeleteDemotion(t *testing.T) {
	var root *Node
	for i := 0; i < keyLeafBucket+1; i++ {
		hash := uint64(i) << FixedBits
		root = Insert(root, hash, fmt.Sprintf("k%d", i), Location{Chunk: uint32(i), Slot: uint8(i)})
	}
	boundary := root.slots[0].child.slots[0].child.slots[0]
	if boundary.child == nil || boundary.leaf != nil {
		t.Fatalf("third key did not promote terminal bucket: %+v", boundary)
	}
	if _, ok := nodeLeafBucket(boundary.child); ok {
		t.Fatal("promoted tail incorrectly fits the leaf bucket")
	}
	old := root
	last := keyLeafBucket
	root = Delete(root, uint64(last)<<FixedBits, fmt.Sprintf("k%d", last))
	if root.slots[0].child != nil || leafCount(root.slots[0].leaf) != keyLeafBucket {
		t.Fatalf("tail did not flatten and collapse: %+v", root.slots[0])
	}
	for i := 0; i < keyLeafBucket; i++ {
		loc, ok := Lookup(root, uint64(i)<<FixedBits, fmt.Sprintf("k%d", i))
		if !ok || loc.Chunk != uint32(i) {
			t.Fatalf("lookup k%d after demotion = (%+v,%v)", i, loc, ok)
		}
	}
	if _, ok := Lookup(old, uint64(last)<<FixedBits, fmt.Sprintf("k%d", last)); !ok {
		t.Fatal("delete changed retained promoted root")
	}
}
