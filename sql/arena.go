package sql

// The parse arena.
//
// This is query/compiler.go's chunkArena, applied to the same problem one layer
// earlier. It is duplicated rather than shared because it is unexported there
// and because exporting an allocator from query purely to let the parser borrow
// it would make an internal storage strategy part of that package's API. The
// invariant it exists to hold is identical and worth restating: an AST is a
// graph of interior pointers — a *PathExpr inside a ResultColumn, a []*Expr
// inside an Expr, a string viewing interned bytes — and an append-grown backing
// array moves out from under every one of them when it reallocates, leaving a
// tree whose nodes alias storage the parser has since handed to someone else. A
// chunk is never resized once allocated, so growth only appends a chunk and
// every pointer already issued stays valid until the arena is rewound.
//
// Unlike query's compiler this has no one-shot "allocate each object exactly"
// mode. There, the one-shot path is a package-level front end whose plan
// outlives the compiler, so the tail of every partly filled chunk is waste that
// is never recovered. Here the one-shot [Parse] is a convenience wrapper whose
// arena dies with the statement, and interning identifiers one heap allocation
// at a time would be strictly worse than filling a chunk. One strategy is
// therefore enough, and one strategy is one thing to get right.

// defaultFirstChunk is the element count of an arena's first chunk when its
// owner did not size it. Later chunks double, so the chunk count stays
// logarithmic in statement size and allocDirty's linear scan over chunks stays
// trivial.
//
// The count is per element rather than derived from a byte budget through
// unsafe.Sizeof, which is how query's arenas size themselves. This package has
// six arenas over known element types, so its owner can simply say how many of
// each a statement needs (see newParser); reaching for unsafe to recompute what
// the author already knows would add an entry to the repository's audited
// unsafe inventory to save six numbers.
const defaultFirstChunk = 8

// A chunkArena hands out sub-slices of T that stay valid as the arena grows.
// The zero value is ready to use; set first to size the initial chunk.
type chunkArena[T any] struct {
	chunks [][]T
	at     int // index of the chunk being carved from
	used   int // elements carved from chunks[at]
	first  int // elements in the first chunk, or 0 for defaultFirstChunk
}

// firstChunk is the element count of this arena's first chunk.
func (a *chunkArena[T]) firstChunk() int {
	if a.first > 0 {
		return a.first
	}
	return defaultFirstChunk
}

// alloc reserves n zeroed elements. Zeroing matters: chunk storage is reused
// across parses, and a field left behind from the previous statement — a
// negated flag, say — would produce a tree that reads as valid and means
// something else.
func (a *chunkArena[T]) alloc(n int) []T {
	out := a.allocDirty(n)
	clear(out)
	return out
}

// allocDirty reserves n elements without zeroing, for the callers that
// overwrite every element before anyone can read it.
func (a *chunkArena[T]) allocDirty(n int) []T {
	if n <= 0 {
		return nil
	}
	for a.at < len(a.chunks) {
		chunk := a.chunks[a.at]
		if a.used+n <= len(chunk) {
			out := chunk[a.used : a.used+n : a.used+n]
			a.used += n
			return out
		}
		// The tail of this chunk is too small. Skip to the next rather than
		// growing this one, because growing it would move storage earlier
		// reservations still point at. The skip is deterministic, so a
		// repeated parse of the same statement skips identically and the
		// steady state still allocates nothing.
		a.at++
		a.used = 0
	}
	size := a.firstChunk()
	if len(a.chunks) > 0 {
		size = len(a.chunks[len(a.chunks)-1]) * 2
	}
	for size < n {
		size *= 2
	}
	chunk := make([]T, size)
	a.chunks = append(a.chunks, chunk)
	a.at = len(a.chunks) - 1
	a.used = n
	return chunk[0:n:n]
}

// one reserves a single zeroed element and returns a pointer to it.
func (a *chunkArena[T]) one() *T {
	return &a.alloc(1)[0]
}

// rewind returns every chunk to unused without freeing any of it. Every slice
// and pointer previously handed out is invalid from here on; that invalidation
// is the Parser's documented contract.
func (a *chunkArena[T]) rewind() {
	a.at = 0
	a.used = 0
}
