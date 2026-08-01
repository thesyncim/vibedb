package store

import (
	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/x/byteview"
)

// The value dictionary primitive: corpus-wide deduplication of repeated value
// spans, the value counterpart of the KeyInterner.
//
// A shape-clustered corpus stores the same key sequence millions of times, and
// the shape-deduplicated tape (segment_shape.go) stops paying for it by moving
// the keys into a compiled shape. Real corpora repeat their *values* just as
// hard: a ticketing feed names the same handful of venues, seat categories, and
// area identifiers across every performance, and a social feed repeats the same
// language tags, source strings, and boilerplate across every post. A general
// compressor removes this byte-wise and pays whole-value decompression on every
// read. A ValueInterner removes it structurally: it maps each distinct value
// span to a dense uint32 identifier, so a document that repeats a value stores a
// four-byte reference instead of the bytes, and a read resolves the reference to
// a stable arena view in O(1) — no decompression.
//
// This is the KeyInterner discipline (intern.go) applied to value content, with
// one deliberate difference: values intern by their *raw* span, verbatim,
// including a string's surrounding quotes and a number's exact spelling. Keys
// intern by decoded content because two spellings of one key must group as one
// field; values intern by raw bytes because the value contract is exact —
// RawValue.Bytes, NumberText, and the round trip through AppendJSON all return
// the original bytes, and a number's spelling ("1e3" versus "1000") is
// significant. The consequence is that two raw spellings that decode equal — an
// escaped string and its unescaped twin — receive distinct identifiers. That is
// correct (their Raw bytes differ) and costs only a missed dedup on the rare
// repeated-escape case; the common repeated value, a bare string or number, is
// one entry.
//
// The interner owns its storage under the arena discipline the whole layer
// shares: value bytes are copied into append-only chunks that are never moved or
// reallocated in place, so a slice returned by Value stays valid as the interner
// grows and interned values outlive the documents they were interned from.
//
//	retired chunks (full)             current chunk
//	+---------------+  +-----------+  +-----------+----------+
//	| val | val | v |  | val | val |  | val | val |  spare   |
//	+---------------+  +-----------+  +-----------+----------+
//	   ^ values[id] views point into chunks; a chunk that cannot
//	     hold the next value is retired in place, never grown, so
//	     no view is ever invalidated.
//
// The zero ValueInterner is empty and ready to use. A ValueInterner is not safe
// for concurrent use; a multi-document engine shards one per worker exactly as
// it shards a ShapeCache.
type ValueInterner struct {
	table  []uint32 // open-addressing slots holding id+1; zero marks empty
	hashes []uint32 // id -> content hash, reused when the table rehashes
	values [][]byte // id -> arena-backed raw span; the bytes never move
	counts []uint32 // id -> occurrences interned so far, saturating at max uint32
	chunk  []byte   // current arena chunk, appended to only within capacity
	bytes  int64    // total distinct value bytes held across all chunks
}

// The arena and table bounds mirror the key interner's: geometric chunk growth
// between fixed limits so small dictionaries stay small and large ones amortize
// allocation, a value longer than the maximum still getting a chunk of its own;
// the table starts small and doubles at three-quarters load to keep probe
// sequences short.
const (
	valueDictMinChunk = 1 << 10
	valueDictMaxChunk = 64 << 10
	valueDictMinTable = 64
)

// Len returns the number of distinct interned values. Identifiers are dense:
// every id in [0, Len) is assigned. This is the dictionary's entry count, the
// numerator of its deduplication ratio against the occurrences it replaced.
func (in *ValueInterner) Len() int {
	return len(in.values)
}

// Bytes returns the total distinct value bytes the dictionary holds — the sum
// of len(Value(id)) over every id, accumulated as values are interned so the
// accounting never rescans the arena.
func (in *ValueInterner) Bytes() int64 {
	return in.bytes
}

// Value returns the raw span of an interned value. The slice borrows the
// interner's arena: it remains valid for the interner's lifetime and must not be
// modified. An unassigned id panics like an out-of-range slice index.
//
// This is the dictionary's read contract and the invariant every value-dict
// read rests on: Value(Intern(v)) is byte-identical to v for every v, and stays
// so for the interner's lifetime because the arena never moves an interned span.
func (in *ValueInterner) Value(id uint32) []byte {
	return in.values[id]
}

// Count returns how many times an interned value has been interned — its
// occurrence count across the corpus, saturating at the uint32 maximum. A
// document store gates dictionary storage on this: a value seen once costs a
// dictionary entry with no saving. An unassigned id panics like an out-of-range
// slice index.
func (in *ValueInterner) Count(id uint32) uint32 {
	return in.counts[id]
}

// Intern returns value's identifier, assigning the next dense one on first
// appearance and bumping the occurrence count on every appearance. The raw span
// is copied, so value may be reused or discarded after the call.
func (in *ValueInterner) Intern(value []byte) uint32 {
	return in.intern(vibejson.HashKeyContent(value), byteview.String(value))
}

// InternString is Intern for a value already held as a string.
func (in *ValueInterner) InternString(value string) uint32 {
	return in.intern(vibejson.HashKeyContent(byteview.Bytes(value)), value)
}

// Lookup returns value's identifier without assigning one or bumping its count.
func (in *ValueInterner) Lookup(value []byte) (uint32, bool) {
	return in.lookup(vibejson.HashKeyContent(value), byteview.String(value))
}

// lookup probes the table for a value with the given content hash. Linear
// probing from the hash's home slot ends at the first empty slot, which an
// insertion never leaves between a value's home and its slot. A slot matches
// only after the stored hash and then the raw bytes agree, so a hash collision
// costs a comparison but never misleads — the interner's trust boundary is its
// own, exactly the key interner's.
func (in *ValueInterner) lookup(hash uint32, value string) (uint32, bool) {
	if len(in.table) == 0 {
		return 0, false
	}
	mask := uint32(len(in.table) - 1)
	for slot := hash & mask; ; slot = (slot + 1) & mask {
		stored := in.table[slot]
		if stored == 0 {
			return 0, false
		}
		id := stored - 1
		if in.hashes[id] == hash && byteview.String(in.values[id]) == value {
			return id, true
		}
	}
}

// intern is the shared byte/string implementation. hash is value's content hash
// under the tape's key-hash family, reused so a lookup and an insert probe
// identically.
func (in *ValueInterner) intern(hash uint32, value string) uint32 {
	if id, ok := in.lookup(hash, value); ok {
		if in.counts[id] != ^uint32(0) {
			in.counts[id]++
		}
		return id
	}
	return in.insert(hash, value)
}

// insert assigns the next identifier to a value lookup just missed. It copies
// the raw span into the arena — never extending a chunk beyond its capacity, so
// existing value bytes stay put — and records the id in the table, growing the
// table first when the insertion would cross three-quarters load.
func (in *ValueInterner) insert(hash uint32, value string) uint32 {
	if uint64(len(in.values)) >= 1<<32-1 {
		// Table slots store id+1 in a uint32; the arena alone makes this
		// unreachable in practice, so the guard only pins the invariant that a
		// dictionary identifier round-trips through a uint32 value reference.
		panic("vibedb: ValueInterner exceeds 32-bit identifiers")
	}
	if (len(in.values)+1)*4 >= len(in.table)*3 {
		in.grow()
	}
	if len(in.chunk)+len(value) > cap(in.chunk) {
		size := max(2*cap(in.chunk), valueDictMinChunk)
		if size > valueDictMaxChunk {
			size = valueDictMaxChunk
		}
		if size < len(value) {
			size = len(value)
		}
		in.chunk = make([]byte, 0, size)
	}
	start := len(in.chunk)
	in.chunk = append(in.chunk, value...)
	id := uint32(len(in.values))
	in.values = append(in.values, in.chunk[start:len(in.chunk):len(in.chunk)])
	in.hashes = append(in.hashes, hash)
	in.counts = append(in.counts, 1)
	in.bytes += int64(len(value))
	mask := uint32(len(in.table) - 1)
	slot := hash & mask
	for in.table[slot] != 0 {
		slot = (slot + 1) & mask
	}
	in.table[slot] = id + 1
	return id
}

// grow doubles the table and reinserts every id from its stored hash. Only slots
// move; value bytes, identifiers, and counts are untouched.
func (in *ValueInterner) grow() {
	size := max(2*len(in.table), valueDictMinTable)
	table := make([]uint32, size)
	mask := uint32(size - 1)
	for id, hash := range in.hashes {
		slot := hash & mask
		for table[slot] != 0 {
			slot = (slot + 1) & mask
		}
		table[slot] = uint32(id) + 1
	}
	in.table = table
}
