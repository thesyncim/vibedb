package store

import (
	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/document"
	"github.com/thesyncim/vibejson/x/byteview"
)

// A KeyInterner maps object-key content to dense uint32 identifiers, assigning
// each distinct key the next identifier from zero in first-appearance order.
// Multi-document engines group fields across documents by comparing these
// identifiers instead of key bytes, so interning is always by decoded content:
// two spellings of one key — raw, escaped, or unicode-escaped — receive one
// identifier, exactly as two spellings match one query in Node.Get.
//
// The interner owns its storage. Key bytes are copied into append-only arena
// chunks that are never moved or reallocated in place, so slices returned by
// Key stay valid as the interner grows and interned keys outlive their source
// documents, until an explicit Reset reuses the arena:
//
//	retired chunks (full)             current chunk
//	+---------------+  +-----------+  +-----------+----------+
//	| key | key | k |  | key | key |  | key | key |  spare   |
//	+---------------+  +-----------+  +-----------+----------+
//	   ^ keys[id] views point into chunks; a chunk that cannot
//	     hold the next key is retired in place, never grown, so
//	     no view is ever invalidated.
//
// The zero KeyInterner is empty and ready to use. A KeyInterner is
// not safe for concurrent use.
type KeyInterner struct {
	table   []uint32 // open-addressing slots holding id+1; zero marks empty
	hashes  []uint32 // id -> content hash, reused when the table rehashes
	keys    [][]byte // id -> arena-backed content; the bytes never move
	chunk   []byte   // current arena chunk, appended to only within capacity
	chunks  [][]byte // every arena chunk, retained so Reset can reuse it
	used    int      // chunks selected since construction or the last Reset
	scratch []byte   // decoded spelling of an escaped key, reused per key
	stack   []uint64 // open-object state for AppendKeyIDs, reused per call
}

// The arena grows geometrically between the chunk bounds so small interners
// stay small and large ones amortize allocation; a key longer than the maximum
// still gets a chunk of its own. The table starts small for the same reason
// and doubles at three-quarters load, keeping probe sequences short.
const (
	internMinChunk = 1 << 10
	internMaxChunk = 64 << 10
	internMinTable = 64
)

// Len returns the number of distinct interned keys. Identifiers are dense:
// every id in [0, Len) is assigned.
func (in *KeyInterner) Len() int {
	return len(in.keys)
}

// Key returns the content of an interned key. The slice borrows the
// interner's arena: it remains valid until Reset and must not be modified. An
// unassigned id panics like an out-of-range slice index.
func (in *KeyInterner) Key(id uint32) []byte {
	return in.keys[id]
}

// Reset removes every interned key while retaining the table, identifier
// arrays, decode scratch, and arena chunks for reuse. IDs assigned after Reset
// start again at zero. Keys and byte slices returned before Reset become
// invalid and must not be read afterward; this is the same destination-reuse
// boundary used by append-style APIs. Reset is allocation-free and makes a
// repeated same-sized interning pass allocation-free after warm-up.
func (in *KeyInterner) Reset() {
	clear(in.table)
	in.hashes = in.hashes[:0]
	in.keys = in.keys[:0]
	in.chunk = nil
	in.used = 0
	in.scratch = in.scratch[:0]
	in.stack = in.stack[:0]
}

// Intern returns key's identifier, assigning the next dense one on first
// appearance. The content is copied, so key may be reused or discarded.
func (in *KeyInterner) Intern(key []byte) uint32 {
	return in.intern(vibejson.HashKeyContent(key), byteview.String(key))
}

// InternString is Intern for a string key.
func (in *KeyInterner) InternString(key string) uint32 {
	return in.intern(vibejson.HashKey(key), key)
}

// Lookup returns key's identifier without assigning one.
func (in *KeyInterner) Lookup(key []byte) (uint32, bool) {
	return in.lookup(vibejson.HashKeyContent(key), byteview.String(key))
}

// LookupString is Lookup for a string key.
func (in *KeyInterner) LookupString(key string) (uint32, bool) {
	return in.lookup(vibejson.HashKey(key), key)
}

// lookup probes the table for a key with the given content hash. Linear
// probing from the hash's home slot ends at the first empty slot, which an
// insertion never leaves between a key's home and its slot. A slot matches
// only after the stored hash and then the content bytes agree, so hash
// collisions cost a comparison but never mislead.
func (in *KeyInterner) lookup(hash uint32, key string) (uint32, bool) {
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
		if in.hashes[id] == hash && byteview.String(in.keys[id]) == key {
			return id, true
		}
	}
}

// intern is the shared byte/string implementation: hash is key's content hash
// under the tape's key-hash family, so a stored enrichment hash and a freshly
// computed one probe identically.
func (in *KeyInterner) intern(hash uint32, key string) uint32 {
	if id, ok := in.lookup(hash, key); ok {
		return id
	}
	return in.insert(hash, key)
}

// insert assigns the next identifier to a key lookup just missed. It copies
// the content into the arena — never extending a chunk beyond its capacity, so
// existing key bytes stay put — and records the id in the table, growing the
// table first when the insertion would cross three-quarters load.
func (in *KeyInterner) insert(hash uint32, key string) uint32 {
	if uint64(len(in.keys)) >= 1<<32-1 {
		// Table slots store id+1 in a uint32; the arena alone makes this
		// unreachable in practice, so the guard only pins the invariant.
		panic("vibejson: KeyInterner exceeds 32-bit identifiers")
	}
	if (len(in.keys)+1)*4 >= len(in.table)*3 {
		in.grow()
	}
	if len(in.chunk)+len(key) > cap(in.chunk) {
		in.nextChunk(len(key))
	}
	start := len(in.chunk)
	in.chunk = append(in.chunk, key...)
	id := uint32(len(in.keys))
	in.keys = append(in.keys, in.chunk[start:len(in.chunk):len(in.chunk)])
	in.hashes = append(in.hashes, hash)
	mask := uint32(len(in.table) - 1)
	slot := hash & mask
	for in.table[slot] != 0 {
		slot = (slot + 1) & mask
	}
	in.table[slot] = id + 1
	return id
}

// nextChunk selects reusable arena capacity or allocates one geometrically.
// The scan runs only at chunk boundaries. Swapping an unused chunk forward is
// safe because Reset invalidated every old Key view before chunks are reused.
func (in *KeyInterner) nextChunk(need int) {
	best := -1
	for i := in.used; i < len(in.chunks); i++ {
		if cap(in.chunks[i]) >= need && (best < 0 || cap(in.chunks[i]) < cap(in.chunks[best])) {
			best = i
		}
	}
	if best >= 0 {
		in.chunks[in.used], in.chunks[best] = in.chunks[best], in.chunks[in.used]
		in.chunk = in.chunks[in.used][:0]
		in.used++
		return
	}

	size := max(2*cap(in.chunk), internMinChunk)
	if size > internMaxChunk {
		size = internMaxChunk
	}
	if size < need {
		size = need
	}
	in.chunk = make([]byte, 0, size)
	in.chunks = append(in.chunks, in.chunk)
	in.used++
}

// grow doubles the table and reinserts every id from its stored hash. Only
// slots move; key bytes and identifiers are untouched.
func (in *KeyInterner) grow() {
	size := max(2*len(in.table), internMinTable)
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

// AppendKeyIDs interns every object key in t and appends the identifiers to
// dst in tape order — the order ObjectIter yields members, outer keys before
// the keys nested under them. It returns the extended slice.
//
// Keys intern by decoded content. An unescaped key's content bytes are its
// decoded form and intern directly; an escaped key decodes into a reusable
// scratch buffer first, so both spellings of one key receive one identifier.
//
// When a key's parent object carries the keys-hashed marker (see
// document.IndexOptions.HashKeys), the key entry's next word already holds
// the content hash enrichment computed, and the walk reuses it: stored hash,
// table probe, one byte comparison, no rehash. Enrichment hashes an escaped
// key's raw spelling, so escaped keys always take the decode path, exactly as
// they always byte-compare in Node.Get. The walk tracks the marker per parent
// through a stack of open object extents because a key entry's own words
// cannot distinguish a stored hash from the default next of an unenriched
// build. Each stack word packs one open object:
//
//	| entry index one past the object's subtree (63 bits) | enriched (1 bit) |
//
// so popping is one shift-and-compare against the walk position, and the
// stack's top is always the current entry's innermost enclosing object.
func (in *KeyInterner) AppendKeyIDs(dst []uint32, t vibejson.Index) []uint32 {
	src := t.Src
	stack := in.stack[:0]
	for i := range t.Entries {
		e := &t.Entries[i]
		// Containers cover the entry range [header, header+next); pop every
		// object whose extent ended so the top is the innermost open object,
		// which for a key entry is exactly its parent.
		for len(stack) > 0 && stack[len(stack)-1]>>1 <= uint64(i) {
			stack = stack[:len(stack)-1]
		}
		flags := e.Flags()
		if flags&vibejson.TapeFlagKey != 0 {
			content := src[e.Start+1 : e.End-1]
			var id uint32
			switch {
			case flags&vibejson.TapeFlagEscaped != 0:
				in.scratch = vibejson.AppendDecodedJSONString(in.scratch[:0], content)
				id = in.intern(vibejson.HashKeyContent(in.scratch), byteview.String(in.scratch))
			case len(stack) > 0 && stack[len(stack)-1]&1 != 0:
				id = in.intern(e.Next, byteview.String(content))
			default:
				id = in.intern(vibejson.HashKeyContent(content), byteview.String(content))
			}
			dst = append(dst, id)
			continue
		}
		if e.Kind() == document.Object {
			enriched := uint64(0)
			if e.KeysHashed() {
				enriched = 1
			}
			stack = append(stack, (uint64(i)+uint64(e.Next))<<1|enriched)
		}
	}
	in.stack = stack
	return dst
}
