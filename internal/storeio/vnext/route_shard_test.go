package vnext

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"testing"
)

func routeShardTestEntries(count int) []RouteShardEntry {
	entries := make([]RouteShardEntry, count)
	for i := range entries {
		entries[i] = RouteShardEntry{
			Hash:     KeyedFingerprint(testIdentity.StoreID, fmt.Sprintf("route-shard-%09d", i)),
			Location: uint32(i + 1),
		}
	}
	return entries
}

func TestRouteShardExactVerificationAndBlockOwnership(t *testing.T) {
	entries := routeShardTestEntries(100)
	// An equal full hash is legal: exact verification chooses the intended key.
	entries = append(entries, RouteShardEntry{Hash: entries[7].Hash, Location: 999})
	shard, err := BuildRouteShard(entries)
	if err != nil {
		t.Fatal(err)
	}
	defer shard.Close()
	if shard.Len() != len(entries) || shard.Capacity()*15 < len(entries)*16 || !shard.OutsideHeap() {
		t.Fatalf("shard geometry len=%d cap=%d external=%v", shard.Len(), shard.Capacity(), shard.OutsideHeap())
	}
	for _, entry := range entries {
		location, found := shard.Lookup(entry.Hash, func(location uint32) bool { return location == entry.Location })
		if !found || location != entry.Location {
			t.Fatalf("lookup %#x = (%d,%v), want %d", entry.Hash, location, found, entry.Location)
		}
	}
	if _, found := shard.Lookup(entries[7].Hash, nil); found {
		t.Fatal("nil verifier accepted a fingerprint")
	}
	if _, found := shard.Lookup(KeyedFingerprint(testIdentity.StoreID, "absent"), func(uint32) bool { return true }); found {
		t.Fatal("absent hash found")
	}
	borrowed, err := OpenRouteShard(shard.Bytes())
	if err != nil || borrowed.OutsideHeap() || borrowed.Len() != shard.Len() {
		t.Fatalf("borrowed = (%+v,%v)", borrowed, err)
	}
}

func TestRouteShardRejectsCorruptionAndBounds(t *testing.T) {
	shard, err := BuildRouteShard(routeShardTestEntries(32))
	if err != nil {
		t.Fatal(err)
	}
	defer shard.Close()
	image := shard.Bytes()
	for _, test := range []struct {
		name   string
		mutate func([]byte)
	}{
		{"checksum", func(data []byte) { data[len(data)-1] ^= 1 }},
		{"capacity", func(data []byte) { binary.LittleEndian.PutUint32(data[8:12], 3) }},
		{"slot bytes", func(data []byte) { binary.LittleEndian.PutUint32(data[16:20], 1) }},
		{"reserved", func(data []byte) { data[24] = 1 }},
		{"reserved state", func(data []byte) {
			routeShardPutBits(data[routeShardHeaderBytes:], 0, routeShardSlotBits, 3)
			binary.LittleEndian.PutUint32(data[20:24], routeShardChecksum(data))
		}},
		{"live count", func(data []byte) {
			binary.LittleEndian.PutUint32(data[12:16], 1)
			binary.LittleEndian.PutUint32(data[20:24], routeShardChecksum(data))
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			corrupt := bytes.Clone(image)
			test.mutate(corrupt)
			if _, err := OpenRouteShard(corrupt); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("OpenRouteShard = %v, want corruption", err)
			}
		})
	}

	empty, err := BuildRouteShard(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer empty.Close()
	tail := bytes.Clone(empty.Bytes())
	tail[len(tail)-1] |= 0xf0
	binary.LittleEndian.PutUint32(tail[20:24], routeShardChecksum(tail))
	if _, err := OpenRouteShard(tail); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("OpenRouteShard(nonzero tail) = %v, want corruption", err)
	}
}

func TestRouteShardCanonicalTombstonePreservesProbeChain(t *testing.T) {
	const capacity = uint32(2)
	slotBytes, ok := routeShardSlotBytes(capacity)
	if !ok {
		t.Fatal("invalid test capacity")
	}
	image := make([]byte, routeShardHeaderBytes+slotBytes)
	binary.LittleEndian.PutUint32(image[0:4], routeShardMagic)
	binary.LittleEndian.PutUint16(image[4:6], routeShardVersion)
	binary.LittleEndian.PutUint16(image[6:8], routeShardHeaderBytes)
	binary.LittleEndian.PutUint32(image[8:12], capacity)
	binary.LittleEndian.PutUint32(image[12:16], 1)
	binary.LittleEndian.PutUint32(image[16:20], uint32(slotBytes))

	// Force the first home bucket to be a canonical tombstone and place the
	// live candidate in the next bucket. Lookup must continue rather than
	// spelling a tombstone as end-of-chain.
	hash := uint64(0)
	slots := image[routeShardHeaderBytes:]
	routeShardPutBits(slots, 0, routeShardSlotBits, routeShardTombstone)
	routeShardPutBits(
		slots, routeShardSlotBits, routeShardSlotBits,
		routeShardLive|uint64(77)<<2|uint64(routeShardTag(hash))<<34,
	)
	binary.LittleEndian.PutUint32(image[20:24], routeShardChecksum(image))
	shard, err := OpenRouteShard(image)
	if err != nil {
		t.Fatal(err)
	}
	if location, found := shard.Lookup(hash, func(location uint32) bool {
		return location == 77
	}); !found || location != 77 {
		t.Fatalf("lookup through tombstone = (%d,%v), want (77,true)", location, found)
	}

	noncanonical := bytes.Clone(image)
	routeShardPutBits(
		noncanonical[routeShardHeaderBytes:], 0, routeShardSlotBits,
		routeShardTombstone|uint64(1)<<2,
	)
	binary.LittleEndian.PutUint32(
		noncanonical[20:24], routeShardChecksum(noncanonical),
	)
	if _, err := OpenRouteShard(noncanonical); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("OpenRouteShard(noncanonical tombstone) = %v, want corruption", err)
	}
}

func TestRouteShardZeroAllocationLookup(t *testing.T) {
	entries := routeShardTestEntries(1024)
	shard, err := BuildRouteShard(entries)
	if err != nil {
		t.Fatal(err)
	}
	defer shard.Close()
	if allocs := testing.AllocsPerRun(1000, func() {
		location, found := shard.Lookup(entries[511].Hash, func(location uint32) bool { return location == entries[511].Location })
		if !found || location != entries[511].Location {
			panic("route shard lookup")
		}
	}); allocs != 0 {
		t.Fatalf("lookup allocations = %.2f, want 0", allocs)
	}
}

// The shard makes concurrent immutable reads safe; it intentionally does not
// test or claim publication safety. An owner must finish BuildRouteShard and
// publish its pointer through a generation/root protocol before readers use it.
func TestRouteShardConcurrentImmutableLookup(t *testing.T) {
	entries := routeShardTestEntries(1024)
	shard, err := BuildRouteShard(entries)
	if err != nil {
		t.Fatal(err)
	}
	defer shard.Close()
	var group sync.WaitGroup
	for worker := range 8 {
		group.Add(1)
		go func() {
			defer group.Done()
			for round := range 100 {
				entry := entries[(worker*101+round)%len(entries)]
				location, found := shard.Lookup(entry.Hash, func(location uint32) bool {
					return location == entry.Location
				})
				if !found || location != entry.Location {
					t.Errorf("lookup = (%d,%v), want %d", location, found, entry.Location)
					return
				}
			}
		}()
	}
	group.Wait()
}

func BenchmarkRouteShardLookup(b *testing.B) {
	entries := routeShardTestEntries(1 << 15)
	shard, err := BuildRouteShard(entries)
	if err != nil {
		b.Fatal(err)
	}
	defer shard.Close()
	at := 0
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		entry := entries[at]
		location, found := shard.Lookup(entry.Hash, func(location uint32) bool { return location == entry.Location })
		if !found || location != entry.Location {
			b.Fatal("route shard lookup")
		}
		at++
		if at == len(entries) {
			at = 0
		}
	}
}
