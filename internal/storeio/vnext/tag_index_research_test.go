package vnext

// This file is a research-only laboratory for a possible replacement of the
// durable fingerprint tree. It deliberately has no production callers and no
// durable encoding. In particular, it exposes an important accounting fact:
// a Go atomic slot is an eight-byte word. A 16-bit tag + uint32 location +
// two-bit state can be bit-packed into 50 bits, but cannot be independently
// atomically updated in 6.25 bytes. Publication would therefore have to be a
// copy-on-write immutable byte slab, atomically swapped at its root.

import (
	"fmt"
	"math/bits"
	"sort"
	"sync/atomic"
	"testing"
	"time"
)

const (
	tagResearchEmpty uint64 = iota
	tagResearchLive
	tagResearchTombstone
)

// tagResearchTable packs [tag:30 | location:32 | state:2] in one atomic word.
// The home bucket is derived from the complete keyed hash. A tag only prunes:
// lookup always calls verify before reporting a hit, just as the durable
// fingerprint tree compares the complete document key.
type tagResearchTable struct {
	slots []atomic.Uint64
	mask  uint64
	bits  uint
}

func newTagResearchTable(capacity int, tagBits uint) *tagResearchTable {
	if capacity < 2 || capacity&(capacity-1) != 0 || tagBits == 0 || tagBits > 30 {
		panic("invalid tag research geometry")
	}
	return &tagResearchTable{slots: make([]atomic.Uint64, capacity), mask: uint64(capacity - 1), bits: tagBits}
}

func (t *tagResearchTable) tag(hash uint64) uint32 {
	// Use bits from a separately mixed view of the full keyed hash rather than
	// the low home-bucket bits. SipHash's output is pseudorandom, but this makes
	// the independence requirement explicit and preserves it if home changes.
	mixed := bits.RotateLeft64(hash, 23) ^ hash>>17 ^ hash<<11
	return uint32(mixed & ((uint64(1) << t.bits) - 1))
}

func tagResearchPack(tag, location uint32, state uint64) uint64 {
	return state | uint64(location)<<2 | uint64(tag)<<34
}

func tagResearchUnpack(word uint64) (tag, location uint32, state uint64) {
	return uint32(word >> 34), uint32(word >> 2), word & 3
}

// insert is builder-only: a published table is immutable. It makes no attempt
// to establish key identity because that authority intentionally remains in a
// document block; callers must deduplicate keys before building the table.
func (t *tagResearchTable) insert(hash uint64, location uint32) {
	if location == 0 {
		panic("zero location")
	}
	tag := t.tag(hash)
	firstTombstone := -1
	for probe := uint64(0); probe <= t.mask; probe++ {
		index := (hash + probe) & t.mask
		word := t.slots[index].Load()
		_, _, state := tagResearchUnpack(word)
		if state == tagResearchTombstone && firstTombstone < 0 {
			firstTombstone = int(index)
			continue
		}
		if state != tagResearchEmpty {
			continue
		}
		if firstTombstone >= 0 {
			index = uint64(firstTombstone)
		}
		t.slots[index].Store(tagResearchPack(tag, location, tagResearchLive))
		return
	}
	panic("full tag research table")
}

// lookup returns candidate-check count in addition to the exact result. It is
// useful because a tag collision costs a document-block pin/key compare, not
// merely another CPU comparison.
func (t *tagResearchTable) lookup(hash uint64, verify func(uint32) bool) (location uint32, found bool, checks int) {
	tag := t.tag(hash)
	for probe := uint64(0); probe <= t.mask; probe++ {
		word := t.slots[(hash+probe)&t.mask].Load()
		candidateTag, candidateLocation, state := tagResearchUnpack(word)
		if state == tagResearchEmpty {
			return 0, false, checks
		}
		if state == tagResearchLive && candidateTag == tag {
			checks++
			if verify(candidateLocation) {
				return candidateLocation, true, checks
			}
		}
	}
	return 0, false, checks
}

func tagResearchHash(i int) uint64 {
	return KeyedFingerprint(testIdentity.StoreID, fmt.Sprintf("tag-research-%09d", i))
}

func tagResearchLatency(t *tagResearchTable, hashes []uint64, hit bool) (p50, p99 time.Duration, checks float64) {
	// A single lookup is below the wall-clock resolution on some machines. Time
	// a short fixed batch, then normalize; this is a screening distribution,
	// not an end-to-end latency claim.
	const batch = 16
	durations := make([]time.Duration, len(hashes)/batch)
	operations := 0
	for sample := range durations {
		start := time.Now()
		for offset := range batch {
			i := sample*batch + offset
			_, found, count := t.lookup(hashes[i], func(location uint32) bool {
				return hit && location == uint32(i+1)
			})
			checks += float64(count)
			operations++
			if found != hit {
				panic("research lookup parity")
			}
		}
		durations[sample] = time.Since(start) / batch
	}
	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
	return durations[len(durations)/2], durations[len(durations)*99/100], checks / float64(operations)
}

// TestTagResearchMatrix reports the RAM-versus-false-document-check trade for
// 8/16/24/30-bit tags at 15/16 load. The timing uses time.Now and is therefore
// a diagnostic p50/p99, not a release benchmark; the Benchmark below is the
// stable CPU measurement. A production design must additionally measure the
// end-to-end document pin/cache-miss p99.
func TestTagResearchMatrix(t *testing.T) {
	const capacity = 1 << 16
	const count = capacity * 15 / 16
	hits := make([]uint64, count)
	misses := make([]uint64, count)
	for i := range hits {
		hits[i] = tagResearchHash(i)
		misses[i] = tagResearchHash(i + count)
	}
	for _, width := range []uint{8, 16, 24, 30} {
		table := newTagResearchTable(capacity, width)
		for i, hash := range hits {
			table.insert(hash, uint32(i+1))
		}
		hit50, hit99, hitChecks := tagResearchLatency(table, hits, true)
		// A miss verifier must never accept a matching tag by itself.
		miss50, miss99, missChecks := tagResearchLatency(table, misses, false)
		t.Logf("tag=%d slot=8.00B live=%.2fB hit p50/p99=%s/%s checks=%.6f miss p50/p99=%s/%s checks=%.6f",
			width, 8.0/(15.0/16.0), hit50, hit99, hitChecks, miss50, miss99, missChecks)
	}
}

// TestTagResearchForcedTagAndFullHashCollisions proves that neither an equal
// tag nor even an equal complete hash becomes identity: both candidates are
// checked against the full key in their document record.
func TestTagResearchForcedTagAndFullHashCollisions(t *testing.T) {
	table := newTagResearchTable(16, 16)
	const hash = uint64(0x9a11223344556677)
	table.insert(hash, 11)
	table.insert(hash, 22) // deliberate full-hash collision, distinct full keys
	location, found, checks := table.lookup(hash, func(location uint32) bool { return location == 22 })
	if !found || location != 22 || checks != 2 {
		t.Fatalf("full collision = (%d,%v,%d), want (22,true,2)", location, found, checks)
	}
	// Find a distinct hash sharing the home bucket and 16-bit tag, then prove a
	// tag collision does not produce a false positive.
	var sameTag uint64
	for candidate := uint64(1); ; candidate++ {
		if candidate != hash && candidate&table.mask == hash&table.mask && table.tag(candidate) == table.tag(hash) {
			sameTag = candidate
			break
		}
	}
	if _, found, checks = table.lookup(sameTag, func(uint32) bool { return false }); found || checks == 0 {
		t.Fatalf("tag collision = (found=%v checks=%d), want verified miss", found, checks)
	}
}

func BenchmarkTagResearchLookup(b *testing.B) {
	const capacity = 1 << 16
	const count = capacity * 15 / 16
	hashes := make([]uint64, count)
	for i := range hashes {
		hashes[i] = tagResearchHash(i)
	}
	for _, width := range []uint{8, 16, 24, 30} {
		b.Run(fmt.Sprintf("tag=%d/hit", width), func(b *testing.B) {
			table := newTagResearchTable(capacity, width)
			for i, hash := range hashes {
				table.insert(hash, uint32(i+1))
			}
			at, checks := 0, 0
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				_, found, count := table.lookup(hashes[at], func(location uint32) bool { return location == uint32(at+1) })
				if !found {
					b.Fatal("miss")
				}
				checks += count
				at++
				if at == len(hashes) {
					at = 0
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(checks)/float64(b.N), "docchecks/op")
		})
	}
}
