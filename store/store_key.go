package store

import (
	"hash/maphash"

	"github.com/thesyncim/vibejson"
)

// CompiledKey is a reusable collection lookup compiled against one
// collection's hash seed and current stable slot. It is a precompiled
// accessor, not a key: [CompiledKey.String] returns the spelling it was
// compiled from. Compile it once with [Collection.CompileKey] or
// [Snapshot.CompileKey], then use [Snapshot.GetRawKey] or [Snapshot.GetKey] on
// repeated reads.
//
// A CompiledKey remains correct across updates and snapshots. When its key is
// still live at the cached stable slot, lookup bypasses both string hashing and
// the key directories. Delete/reinsert movement, an initially absent key, or
// use with another collection falls back to a complete seeded-hash and full-key
// lookup. The key spelling is borrowed like [vibejson.CompiledKey]; keep that string
// immutable.
// The zero value names the empty key and is safe to use.
type CompiledKey struct {
	key  string
	seed maphash.Seed
	hash uint64
	loc  Location
	// generation proves that loc was resolved against the exact current
	// publication. It adds no GC edge and lets repeated reads skip a redundant
	// full-key comparison until any mutation publishes a newer state.
	generation uint64
	located    bool
}

// String returns the key spelling.
func (k CompiledKey) String() string { return k.key }

// CompileKey returns a reusable lookup for key in this snapshot. Compilation
// performs one ordinary key lookup so later hits can take the verified stable-
// slot fast path. Compiling against an empty snapshot is valid; later use on a
// non-empty collection falls back to its ordinary lookup.
func (s Snapshot) CompileKey(key string) CompiledKey {
	compiled := CompiledKey{key: key}
	if s.state == nil {
		return compiled
	}
	compiled.seed = s.state.seed
	compiled.hash = maphash.String(compiled.seed, key)
	compiled.loc, compiled.located = storeStateKeyLookup(s.state, compiled.hash, key)
	compiled.generation = s.state.Generation
	return compiled
}

// CompileKey is the current-snapshot convenience form of Snapshot.CompileKey.
func (c *Collection) CompileKey(key string) CompiledKey {
	snap, _ := c.Snapshot()
	return snap.CompileKey(key)
}

// storeKeyCompiledFallback resolves a compiled-key cache miss. Callers check
// the stable slot inline so a hit pays no extra helper call. Fingerprints and
// cached addresses are never authoritative without a complete-key check.
func storeKeyCompiledFallback(state *State, key CompiledKey) (*Chunk, Location, bool) {
	if state == nil {
		return nil, Location{}, false
	}
	hash := key.hash
	if key.seed != state.seed {
		hash = maphash.String(state.seed, key.key)
	}
	chunk, loc, ok := storeStateKeyLookupChunk(state, hash, key.key)
	if !ok {
		return nil, Location{}, false
	}
	return chunk, loc, true
}

// GetRawKey returns key's exact JSON bytes through a compiled collection lookup.
// The returned value has the same borrowing and lifetime contract as GetRaw.
func (s Snapshot) GetRawKey(key CompiledKey) (vibejson.RawValue, bool) {
	state := s.state
	if state != nil && key.located && key.seed == state.seed {
		loc := key.loc
		chunk := state.Chunks.Get(loc.Chunk)
		if chunk != nil && chunk.Live&(uint64(1)<<loc.Slot) != 0 &&
			(key.generation == state.Generation || chunk.Key(int(loc.Slot)) == key.key) {
			return vibejson.RawValue{Src: chunk.Docs.RawAt(int(chunk.Ord[loc.Slot]))}, true
		}
	}
	chunk, loc, ok := storeKeyCompiledFallback(state, key)
	if !ok {
		return vibejson.RawValue{}, false
	}
	return vibejson.RawValue{Src: chunk.Docs.RawAt(int(chunk.Ord[loc.Slot]))}, true
}

// GetKey returns key's navigable Index through a compiled collection lookup. Shape-
// tape widening has the same one-time allocation behavior as Get.
func (s Snapshot) GetKey(key CompiledKey) (vibejson.Index, bool) {
	state := s.state
	if state != nil && key.located && key.seed == state.seed {
		loc := key.loc
		chunk := state.Chunks.Get(loc.Chunk)
		if chunk != nil && chunk.Live&(uint64(1)<<loc.Slot) != 0 &&
			(key.generation == state.Generation || chunk.Key(int(loc.Slot)) == key.key) {
			return chunk.Docs.Doc(int(chunk.Ord[loc.Slot])), true
		}
	}
	chunk, loc, ok := storeKeyCompiledFallback(state, key)
	if !ok {
		return vibejson.Index{}, false
	}
	return chunk.Docs.Doc(int(chunk.Ord[loc.Slot])), true
}

// GetRawKey is the current-snapshot convenience form of Snapshot.GetRawKey.
func (c *Collection) GetRawKey(key CompiledKey) (vibejson.RawValue, bool) {
	snap4, _ := c.Snapshot()
	return snap4.GetRawKey(key)
}

// GetKey is the current-snapshot convenience form of Snapshot.GetKey.
func (c *Collection) GetKey(key CompiledKey) (vibejson.Index, bool) {
	snap, _ := c.Snapshot()
	return snap.GetKey(key)
}

// AppendRaw appends key's exact JSON spelling to caller-owned storage. It is
// the lifetime-independent counterpart to GetRaw: with sufficient capacity it
// allocates nothing, and the returned bytes remain valid after the Snapshot or
// a caller-owned mapped collection image is released. A miss leaves dst unchanged.
func (s Snapshot) AppendRaw(dst []byte, key string) ([]byte, bool) {
	raw, ok := s.GetRaw(key)
	if !ok {
		return dst, false
	}
	return append(dst, raw.Bytes()...), true
}

// AppendRawKey is AppendRaw through a reusable compiled collection key.
func (s Snapshot) AppendRawKey(dst []byte, key CompiledKey) ([]byte, bool) {
	raw, ok := s.GetRawKey(key)
	if !ok {
		return dst, false
	}
	return append(dst, raw.Bytes()...), true
}

// AppendRaw is the current-snapshot convenience form of Snapshot.AppendRaw.
func (c *Collection) AppendRaw(dst []byte, key string) ([]byte, bool) {
	snap2, _ := c.Snapshot()
	return snap2.AppendRaw(dst, key)
}

// AppendRawKey is the current-snapshot convenience form of
// Snapshot.AppendRawKey.
func (c *Collection) AppendRawKey(dst []byte, key CompiledKey) ([]byte, bool) {
	snap1, _ := c.Snapshot()
	return snap1.AppendRawKey(dst, key)
}
