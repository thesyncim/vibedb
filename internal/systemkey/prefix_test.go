package systemkey

import "testing"

func TestBlocksAreCompleteDisjointAndReservedAboveExecutionPin(t *testing.T) {
	want := [...]Block{
		{0x10, 0x1f}, {0x20, 0x2f}, {0x30, 0x3f},
		{0x40, 0x4f}, {0x50, 0x5f}, {0x60, 0xff},
	}
	var claimed [256]Owner
	for index, expected := range want {
		owner := Owner(index + 1)
		block, ok := ForOwner(owner)
		if !ok || block != expected {
			t.Fatalf("owner %d block = %+v,%v, want %+v", owner, block, ok, expected)
		}
		for prefix := int(block.First); prefix <= int(block.Last); prefix++ {
			if claimed[prefix] != 0 {
				t.Fatalf("prefix %02x claimed by %d and %d", prefix, claimed[prefix], owner)
			}
			claimed[prefix] = owner
			if !block.Contains(byte(prefix)) {
				t.Fatalf("block %+v rejected %02x", block, prefix)
			}
		}
	}
	for prefix := 0x10; prefix <= 0xff; prefix++ {
		if claimed[prefix] == 0 {
			t.Fatalf("hidden prefix %02x has no explicit owner or reservation", prefix)
		}
	}
	if _, ok := ForOwner(0); ok {
		t.Fatal("zero owner was accepted")
	}
}
