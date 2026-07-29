package store

import (
	"fmt"
	"hash/maphash"
	"testing"
)

func TestStoreMappedKeysGroupProbeCollisionDifferential(t *testing.T) {
	const count = 257
	source := make([]byte, 0, count*12)
	mapped, err := newStoreMappedKeys(nil, count, false)
	if err != nil {
		t.Fatal(err)
	}
	defer mapped.release()
	seed := maphash.MakeSeed()
	want := make(map[string]Location, count)
	for i := range count {
		key := fmt.Sprintf("collision-key-%03d", i)
		off := len(source)
		source = append(source, key...)
		loc := Location{Chunk: uint32(i / 64), Slot: uint8(i % 64)}
		mapped.refs[i] = storeMappedKeyRef{off: uint64(off), length: uint32(len(key))}
		mapped.setLocation(uint64(i), loc)
		want[key] = loc
	}
	mapped.source = source
	for i := range count {
		key := fmt.Sprintf("collision-key-%03d", i)
		// Retain only three initial hash bits to force long, wrapping clusters;
		// exact spelling must still distinguish every key.
		hash := maphash.String(seed, key) & 7
		if !mapped.insert(hash, uint64(i)) {
			t.Fatalf("insert %q reported duplicate", key)
		}
	}
	for key, loc := range want {
		hash := maphash.String(seed, key) & 7
		if got, ok := mapped.lookup(hash, key); !ok || got != loc {
			t.Fatalf("lookup %q = (%+v, %v), want %+v", key, got, ok, loc)
		}
	}
	if _, ok := mapped.lookup(0, "absent"); ok {
		t.Fatal("absent collision lookup hit")
	}
	if mapped.insert(maphash.String(seed, "collision-key-100")&7, 100) {
		t.Fatal("duplicate insertion succeeded")
	}
}

