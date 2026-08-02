package query

import (
	"slices"
	"unsafe"

	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/x/byteview"
)

// The reusable plan compiler behind prepared SQL statements.
//
// Every arena here is a chunkArena rather than an append-grown slice. A plan
// is a graph of interior pointers — a *compiledPredicate points at a node, a
// scalar's num field points at a literal's digits, a string points at interned
// bytes — and append's reallocation would move the backing array out from
// under every one of them, leaving a plan whose nodes alias storage the
// compiler has since handed to someone else. Chunks are never resized once
// allocated, so an issued pointer stays valid for as long as the compiler is
// not rewound.

// A compiler owns the reusable storage a [Statement] refills when SQL
// placeholders are rebound. Its zero value is ready to use. It is
// single-consumer and not safe for concurrent use.
//
// A plan compiled in this storage remains valid until the next compile or
// release. This is the same borrowed-lifetime rule [Result] and
// [Workspace] carry: retaining high-water storage is what makes repeated
// bind-and-execute allocation-free.
type compiler struct {
	// tmp builds headers, exact numeric spellings, and derived pointer paths.
	tmp []byte

	// Arenas whose storage a compiled plan retains.
	text  chunkArena[byte]                // interned strings and literal digits
	tape  chunkArena[vibejson.IndexEntry] // containment and equality needle tapes
	nodes chunkArena[compiledPredicate]   // compiled predicate nodes
	kids  chunkArena[*compiledPredicate]  // compiled predicate child lists
	lits  chunkArena[scalar]              // membership alternatives
	idxs  chunkArena[vibejson.Index]      // membership needles
	boxes chunkArena[any]                 // In alternatives before typing
	nums  chunkArena[Number]              // pointer-boxed number literals
	strs  chunkArena[string]              // pointer-boxed string literals
	tree  chunkArena[Predicate]           // builder-tree operand lists

	// Reused whole-plan slices. Unlike the arenas these hold no interior
	// pointers into themselves, so a plain reslice-to-zero is enough.
	columns    []Column
	groupBy    []string
	orderBy    []orderSpec
	joins      []Join
	marks      []correlatedMark
	headers    []string
	planCols   []planColumn
	planJoins  []planJoin
	planMarks  []planMark
	planOrder  []planOrder
	groupCols  []int
	filterCols []int
	lateCols   []int
	lateNums   []int
	values     pathRegistry
	numbers    pathRegistry

	// joinRegs and joinPlans are one inner path registry and one inner plan per
	// join position. They are held behind pointers rather than inline so that
	// growing either list cannot move a plan or registry that the join being
	// compiled already points at.
	joinRegs  []*pathRegistry
	joinPlans []*plan
	markRegs  []*pathRegistry
	markPlans []*plan
	// joinAliases resolves a qualified path spec to the clause it names. It is
	// rebuilt per compile, before any path is compiled, because a path's
	// meaning depends on it.
	joinAliases []joinAlias

	// leaves and members are the scalar-containment lowering's scratch.
	// members is used with stack discipline because the walk that fills it
	// recurses, and one flat reused slice would let an inner object's members
	// overwrite the outer object's.
	leaves  []scalarContainmentLeaf
	members []effectiveMember

	// counts is coalesceEqualityDisjuncts' per-column tally. It is a slice
	// scanned linearly rather than a map: a disjunction names a couple of
	// columns, and the map cost one allocation plus a hash per operand.
	counts []columnCount

	// plan and result are the single reused plan and its cached compile
	// outcome a reusable compiler writes into. Reusing them is exactly what
	// the borrowed-lifetime contract above is about: the previous Query's plan
	// pointer is this one. A one-shot compiler leaves both nil and allocates
	// per compile instead.
	//
	// They are pointers rather than embedded values for an escape-analysis
	// reason worth stating, because it is invisible and easy to undo:
	// returning &c.plan from any method makes that method leak its receiver.
	// Handing back a pointer the compiler merely holds leaks nothing.
	plan   *plan
	result *compileResult

	// oneShot marks the compiler a programmatic builder Query creates for its lazy
	// first compile: it compiles exactly once and is then unreachable except
	// through the plan it produced, so nothing can ever rewind it.
	//
	// It is a genuinely different allocation strategy, not a tuning knob. A
	// reusable compiler wants chunks, because a chunk amortizes over every
	// later compile; a compiler that will never compile again wants each
	// object sized exactly, because every unused byte of a chunk is waste it
	// has no second compile to recover. The split is confined to where storage
	// comes from — see chunkArena.direct — and the lowering above it is one
	// implementation either way, which is the only reason having two
	// strategies is safe at all.
	oneShot bool

	// paths memoizes compiled paths across compiles. It is deliberately not
	// rewound, because compilePath is a pure function of its spec and a
	// compiledPath is immutable, which makes the pointer tokens
	// vibejson.CompilePointer allocates the one part of a plan that can safely
	// outlive the compile that produced it.
	paths pathCache
}

// forOneShot puts c in the strategy a programmatic builder Query needs: exact
// allocation instead of chunking, no path memo, and a heap-allocated plan.
//
// The flag is pushed down into each arena rather than consulted through c on
// every reservation, so the arenas stay self-contained and the branch is one
// predictable load next to the storage it decides about.
func (c *compiler) forOneShot() {
	c.oneShot = true
	c.text.direct = true
	c.tape.direct = true
	c.nodes.direct = true
	c.kids.direct = true
	c.lits.direct = true
	c.idxs.direct = true
	c.boxes.direct = true
	c.nums.direct = true
	c.strs.direct = true
	c.tree.direct = true
}

// release drops every chunk c retains, returning it to its zero state and
// invalidating its current plan. Reusing a warm compiler is the point of
// having one, so release is for after one unusually large statement whose
// arenas should not stay pinned.
func (c *compiler) release() {
	if c == nil {
		return
	}
	*c = compiler{}
}

// outcome publishes a compile result. A reusable compiler overwrites its own,
// which is the pointer the previous Query is still holding and precisely what
// the invalidation rule describes; a one-shot compiler allocates, so that the
// Query it returns holds nothing that points back into it.
// The halves are passed separately rather than as one compileResult: taking
// the address of a by-value parameter in either branch would make it escape in
// both, putting a heap allocation back on the reusable path that has none.
func (c *compiler) outcome(p *plan, err error) *compileResult {
	if c.oneShot {
		return &compileResult{plan: p, err: err}
	}
	if c.result == nil {
		c.result = new(compileResult)
	}
	*c.result = compileResult{plan: p, err: err}
	return c.result
}

// planStorage returns the plan this compile writes into: the compiler's own
// for a reusable compiler, a fresh one otherwise.
func (c *compiler) planStorage() *plan {
	if c.oneShot {
		return new(plan)
	}
	if c.plan == nil {
		c.plan = new(plan)
	}
	return c.plan
}

// fail installs a compile failure in dst. The failure is installed rather than
// only returned so a caller that ignores the error and runs dst anyway sees
// that same error instead of executing whichever half-lowered clauses survived
// into dst before the failure.
func (c *compiler) fail(dst *Query, err error) error {
	c.prepare(dst)
	dst.built = c.outcome(nil, err)
	return err
}

// prepare resets dst onto c's retained builder storage, so lowering appends
// into capacity the previous compile already grew.
func (c *compiler) prepare(dst *Query) {
	dst.built = nil
	dst.columns = c.columns[:0]
	dst.groupBy = c.groupBy[:0]
	dst.orderBy = c.orderBy[:0]
	dst.joins = c.joins[:0]
	dst.marks = c.marks[:0]
	dst.where = Predicate{}
	dst.hasWhere = false
	dst.limit = 0
	dst.hasLimit = false
}

// keep takes back the builder storage dst grew, so the next compile starts
// from this compile's high-water capacity.
func (c *compiler) keep(dst *Query) {
	c.columns = dst.columns
	c.groupBy = dst.groupBy
	c.orderBy = dst.orderBy
	c.joins = dst.joins
	c.marks = dst.marks
}

// rewind returns every arena and reused slice to empty while keeping the
// storage behind it. Only the path memo survives, because it is the one thing
// here that is not per-compile state.
func (c *compiler) rewind() {
	c.text.rewind()
	c.tape.rewind()
	c.nodes.rewind()
	c.kids.rewind()
	c.lits.rewind()
	c.idxs.rewind()
	c.boxes.rewind()
	c.nums.rewind()
	c.strs.rewind()
	c.tree.rewind()
	c.values.reset()
	c.numbers.reset()
	c.leaves = c.leaves[:0]
	c.members = c.members[:0]
	c.counts = c.counts[:0]
}

// reserve grows s so that n more elements fit without reallocating, and
// returns it with its length unchanged.
//
// It matters most on the Go builder's one-shot path, where a slice starts
// empty and every doubling is an allocation the compile never recovers. A warm
// prepared-statement compiler is already past its high-water mark and reserve
// is a no-op there.
func reserve[T any](s []T, n int) []T {
	if n <= cap(s)-len(s) {
		return s
	}
	return slices.Grow(s, n)
}

// --- string and byte interning --------------------------------------------

// intern copies b into the text arena and returns it as a string. The copy is
// what lets the plan retain a name read out of a caller's src bytes or out of
// the shared unescaping scratch: both are gone or overwritten by the time the
// query runs.
func (c *compiler) intern(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return byteview.String(c.bytes(b))
}

// internString is intern for a string source.
func (c *compiler) internString(s string) string {
	if len(s) == 0 {
		return ""
	}
	return byteview.String(c.bytes(byteview.Bytes(s)))
}

// bytes copies b into the text arena and returns the arena's copy, the form a
// literal's decimal spelling and a needle's source text are retained in.
func (c *compiler) bytes(b []byte) []byte {
	dst := c.text.allocDirty(len(b))
	copy(dst, b)
	return dst
}

// --- compiled-path memo ----------------------------------------------------

// pathCacheMax bounds the memo. A compiler driven by one query never
// approaches it; one driven by an unbounded stream of distinct queries would
// otherwise retain a compiled pointer per distinct path forever, which is a
// leak rather than a cache. Overflow drops the whole memo rather than evicting
// one entry, because a compiled path is small and an eviction policy would buy
// nothing a fresh start does not.
const pathCacheMax = 256

// A pathCache memoizes compilePath across compiles. compilePath is a pure
// function of its spec and the compiledPath it returns is immutable, so the
// token slice vibejson.CompilePointer allocates is the one piece of plan
// storage that can be shared across a statement's successive bindings.
type pathCache struct {
	entries []cachedPath
}

// A cachedPath is one memoized compilation, including a failure: an invalid
// pointer spec fails identically every time, and remembering that keeps a
// repeatedly bound bad statement from re-running the pointer parser.
type cachedPath struct {
	spec string
	path compiledPath
	err  error
}

// compilePath returns spec's compiled path, compiling it on the first sighting.
//
// The memo key is a copy of spec rather than spec itself. A spec handed in by
// the lowering points into the text arena, and vibejson.CompilePointer's
// pointer string and token texts would then alias that arena too — so the next
// compile's rewind would rewrite the tokens of every previously compiled path.
func (c *compiler) compilePath(spec string) (compiledPath, error) {
	if c.oneShot {
		// Nothing will rewind this compiler, so the arena spec outlives the
		// plan and the compiled pointer may borrow it.
		return compilePath(spec)
	}
	for i := range c.paths.entries {
		if c.paths.entries[i].spec == spec {
			return c.paths.entries[i].path, c.paths.entries[i].err
		}
	}
	if len(c.paths.entries) >= pathCacheMax {
		c.paths.entries = c.paths.entries[:0]
	}
	owned := string(byteview.Bytes(spec))
	path, err := compilePath(owned)
	c.paths.entries = append(c.paths.entries, cachedPath{spec: owned, path: path, err: err})
	return path, err
}

// --- arena-backed plan constructors ---------------------------------------

// operands reserves an arena-backed operand list of exactly n predicates. The
// callers all know their operand count up front, so the returned slice is
// appended into and never outgrows its reservation.
func (c *compiler) operands(n int) []Predicate {
	return c.tree.alloc(n)[:0]
}

// pair is the two-operand list SQL's IN-with-NULL rewrite needs.
func (c *compiler) pair(a, b Predicate) []Predicate {
	kids := c.tree.alloc(2)
	kids[0], kids[1] = a, b
	return kids
}

// not negates a predicate without the one-element slice literal [Not] builds,
// which would be a heap allocation per negation on the arena path.
func (c *compiler) not(inner Predicate) Predicate {
	kids := c.tree.alloc(1)
	kids[0] = inner
	return Predicate{kind: predNot, kids: kids}
}

// --- chunked arenas --------------------------------------------------------

// arenaChunkBytes is the byte budget of an arena's first chunk. It is a byte
// budget rather than an element count because these arenas hold everything
// from single bytes to quarter-kilobyte predicate nodes: one element count for
// all of them either over-provisions the wide types by tens of kilobytes on a
// builder's one-shot compile, or splits the interned-text arena across a dozen
// chunks. Later chunks double,
// so the chunk count stays logarithmic in the query's size whichever type it
// is, and alloc's linear scan over them stays trivial.
//
// The budget is deliberately small. A warm compiler reaches its high-water
// chunk set within a compile or two whatever the budget is, so the only thing
// a larger one buys is a bigger bill for the throwaway compiler behind a
// builder Query, which never gets to reuse a byte of it.
const arenaChunkBytes = 128

// arenaMinElements floors the first chunk for the wide element types, whose
// byte budget would otherwise buy a single element and turn a four-node
// predicate tree into four separately allocated chunks.
const arenaMinElements = 4

// firstChunk is the element count arenaChunkBytes buys for this arena's
// element type, never fewer than arenaMinElements. A zero-sized element type
// would divide by zero rather than answer a size, so it takes the floor too.
func (a *chunkArena[T]) firstChunk() int {
	size := int(unsafe.Sizeof(*new(T)))
	if size > 0 {
		if n := arenaChunkBytes / size; n > arenaMinElements {
			return n
		}
	}
	return arenaMinElements
}

// A chunkArena hands out sub-slices of T that stay valid as the arena grows.
// An append-grown slice cannot promise that: reallocating its backing array
// leaves every pointer and sub-slice already issued aliasing storage the
// garbage collector is free to reuse, which is precisely the corruption a
// compiled plan full of interior pointers would suffer. A chunk is never
// resized once allocated, so growth only ever appends a new chunk, and
// rewinding keeps every chunk for the next compile.
//
// The zero chunkArena is ready to use.
type chunkArena[T any] struct {
	chunks [][]T
	at     int // index of the chunk being carved from
	used   int // elements carved from chunks[at]

	// direct switches off chunking entirely: every reservation is its own
	// exactly sized allocation. Chunking is a bet that a later compile will
	// use the rest of the chunk, and a one-shot compiler is the case where
	// that bet is known to lose — the leftover of every partly filled chunk is
	// pure waste on a one-shot builder compile's bill. Both strategies satisfy
	// the same contract, an issued slice stays valid until rewind, which is
	// why the callers above cannot tell them apart.
	direct bool
}

// alloc reserves n zeroed elements. Zeroing matters: chunk storage is reused
// across compiles, and a stale field left behind — a needle from the previous
// query, say — would compile into a plan that silently answers the wrong
// question rather than failing.
func (a *chunkArena[T]) alloc(n int) []T {
	if a.direct {
		if n <= 0 {
			return nil
		}
		return make([]T, n) // already zeroed
	}
	out := a.allocDirty(n)
	clear(out)
	return out
}

// allocDirty reserves n elements without zeroing them, for the callers that
// overwrite every element before anyone can read it.
func (a *chunkArena[T]) allocDirty(n int) []T {
	if n <= 0 {
		return nil
	}
	if a.direct {
		return make([]T, n)
	}
	for a.at < len(a.chunks) {
		chunk := a.chunks[a.at]
		if a.used+n <= len(chunk) {
			out := chunk[a.used : a.used+n : a.used+n]
			a.used += n
			return out
		}
		// The tail of this chunk is too small for the request. Skip to the
		// next chunk rather than growing this one, because growing it would
		// move storage that earlier reservations still point at. The skip is
		// deterministic, so a repeated compile skips identically and the
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

// one reserves a single zeroed element and returns a pointer to it, the
// spelling every arena-allocated node uses.
func (a *chunkArena[T]) one() *T {
	return &a.alloc(1)[0]
}

// rewind returns every chunk to unused without freeing any of it. Every slice
// and pointer previously handed out is invalid from here on; that invalidation
// is the compiler's documented contract.
func (a *chunkArena[T]) rewind() {
	a.at = 0
	a.used = 0
}
