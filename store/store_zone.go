package store

import (
	"encoding/binary"
	"math"
	"math/bits"
	"strconv"
	"sync/atomic"
	"unsafe"

	"github.com/thesyncim/vibejson/x/byteview"
	"github.com/thesyncim/vibejson/x/scanner"
)

// Per-chunk zone maps: the block-pruning tier.
//
// Before this file the collection had pruning at exactly two granularities —
// the declared-index catalog (whole-index) and postings/index masks (single
// row) — and nothing in between. A predicate with no declared index therefore
// read and materialized every chunk in the collection no matter how selective
// it was. This file adds the tier every serious column store has between those
// two: a small, fixed-size summary of each immutable chunk that the planner
// consults to decide whether the chunk can be skipped without being read.
//
// # The conservative-pruning argument
//
// A chunk may be skipped only when it provably contains no matching row.
// Scanning a chunk that holds no match costs time; skipping a chunk that holds
// one is silent data loss, so every decision here is built so that the *only*
// error a summary can make is to be too wide.
//
// Soundness rests on one property: phi (zoneCode below) is a monotone map from
// the query's total order over JSON scalars (query/scalar.go's compareScalar)
// into uint32, and it is applied by the identical code to stored values and to
// query literals. Monotone means a <= b implies phi(a) <= phi(b); it does not
// have to be injective, and every place it loses information (a 29-bit number
// mantissa, a four-byte string prefix) loses it in a way that only merges
// distinct values onto one code, never reorders them.
//
// Given monotonicity, with mn = min over the chunk of phi(x) and mx = max:
//
//	x  = L  implies phi(x) == phi(L), so prune when phi(L) < mn or phi(L) > mx
//	x  > L  implies phi(x) >= phi(L), so prune when mx < phi(L)
//	x  < L  implies phi(x) <= phi(L), so prune when mn > phi(L)
//
// and symmetrically for >= and <=. Each implication is one-directional, which
// is exactly the direction pruning needs: a chunk survives whenever a match is
// still possible.
//
// Folding is allowed to be even more conservative than phi: a value is folded
// as a closed interval [lo, hi] known to contain phi(v), and mn/mx take the
// min of lo and the max of hi. That is what lets an escaped string, whose
// decoded prefix is not known without decoding it, be folded as "some string
// in this range" instead of forcing a decode on the write path.
//
// # Exact-decimal numbers
//
// query/scalar.go deliberately keeps 9007199254740992 and 9007199254740993
// distinct, so a naive float64 zone bound would be a false-negative generator:
// a chunk holding only ...93 has float64 max ...92, and `x > 9007199254740992`
// would prune a chunk that matches. The bound here is safe for a subtler
// reason than rounding slack. strconv.ParseFloat is correctly rounded, and
// correct rounding is a *monotone* function of the exact decimal value, so
// phi(number) = tag | top-29-bits-of-monotone-float-order(ParseFloat(s)) is
// monotone with respect to exact decimal order even though it is massively
// non-injective. The two integers above collapse onto one code; the pruning
// rules above tolerate that (they only ever conclude "may match"), and the
// per-row evaluator still separates them exactly. -0 is normalized to +0
// because the two are one value to compareScalar but two distinct bit
// patterns, and a phi that separated them would not be monotone.
//
// # Nulls
//
// Absence and explicit null are tracked as two separate bits, not one. They
// are genuinely different facts about a schemaless document, and EXISTS
// already distinguishes them (query/predicate.go's present()); only IS NULL
// unions them. Neither participates in min/max: evalCmp rejects a null cell
// before any comparison, so excluding nulls from the bounds is not merely
// space-saving — it lets a chunk in which a path is always null or always
// absent be pruned outright for every ordered predicate.

// ZonePaths bounds the number of distinct document paths one chunk keeps
// statistics for. This is the answer to the schemaless-metadata problem, and
// it is deliberately a hard cap rather than a policy that adapts:
//
// A schemaless collection has an unbounded path set, so any summary keyed by
// "every path ever seen" grows with the corpus's vocabulary rather than with
// its size, and metadata that can outgrow the data is a worse bug than the
// missing optimization it was meant to fix. The policy here is therefore
// bounded and first-come: the first ZonePaths *top-level* member names
// observed in a chunk get an entry, and once the table is full it never
// changes again for that chunk's lifetime.
//
// First-come and never-evict is not a convenience. It is what makes an entry
// sound. An entry for path A is created by the first document in the chunk
// that carries A; because the table only ever grows, no earlier document in
// that chunk can have carried A without having created the entry itself. That
// is the invariant that lets a late-created entry record "absent in the
// documents before me" instead of having to rescan them. An eviction policy —
// LRU, frequency, query-history-adaptive — would break it: a re-admitted path
// would have a summary covering only the documents seen since re-admission,
// and would prune chunks that match. Adaptivity was rejected for that reason
// first and because it would make on-disk content depend on query history
// second.
//
// Only top-level members are tracked. Nested paths are the other unbounded
// axis (a document of depth d with f fields per level has f^d paths), and
// enumerating them costs a full recursive walk per write, against a write path
// that is already the collection's bottleneck. Top-level enumeration is one
// linear pass over source bytes the writer has in hand.
const ZonePaths = 8

// Zone statistic flags. Absent and Null are separate bits by design; see the
// file header.
const (
	// zoneFlagAbsent records that at least one document in the chunk does not
	// carry this path at all.
	zoneFlagAbsent uint8 = 1 << iota
	// zoneFlagNull records an explicit JSON null at this path.
	zoneFlagNull
	// zoneFlagValue records at least one non-null value, and is therefore the
	// bit that makes min and max meaningful. Without it a chunk holds nothing
	// any ordered comparison can match, and every Cmp prunes it.
	zoneFlagValue
)

// A chunkZone is one immutable chunk's complete summary. It is a value
// embedded in Chunk rather than a pointer to heap state, so publishing a
// rebuilt chunk costs one fixed-size copy and no allocation.
//
// Space: 8*8 + 8*4 + 8*4 + 8 + 3 bytes, padded to 144 bytes per chunk. At the
// default 64 documents per chunk that is 2.25 bytes per document, or 0.88% of
// the 257 bytes per document of the benchmark corpus. The bound is structural,
// not statistical: the arrays are fixed-size, so 144 bytes per chunk is the
// worst case as well as the typical one, whatever the corpus's path vocabulary
// looks like.
type chunkZone struct {
	// hash holds ZonePathHash of each tracked member name. The name itself is
	// deliberately not stored: it would cost a string header plus bytes per
	// entry and more than double the summary. A 64-bit hash makes an
	// intra-chunk collision (at most 8 entries) a ~2e-18 event, and the fold
	// path merges on hash rather than replacing, so were one to happen the
	// colliding entry would describe the union of two paths — wider, and
	// therefore still sound for range and equality probes.
	hash [ZonePaths]uint64
	min  [ZonePaths]uint32
	max  [ZonePaths]uint32
	flag [ZonePaths]uint8
	n    uint8
	// overflow records that the per-chunk path budget turned a path away. It
	// is what lets a *missing* entry mean something: while no path has been
	// turned away, a path with no entry was carried by no document in the
	// chunk, so EXISTS and every comparison prune the chunk outright and IS
	// NULL matches all of it. That is the single most valuable case in a
	// heterogeneous schemaless corpus, where most chunks simply do not have
	// most paths — and it is exactly the deduction that stops being sound the
	// moment the budget starts dropping paths on the floor.
	overflow bool
	// state distinguishes a usable summary from the two ways one can be
	// unusable. Both unusable states make every probe report "no statistics",
	// so every chunk survives — the safe direction — but they differ in what
	// the write path should do about it, and collapsing them into one bool
	// made every write to a poisoned chunk pay for a rebuild that could only
	// poison it again.
	state uint8
}

const (
	// zoneStateOK is a summary that covers every live document in its chunk.
	zoneStateOK uint8 = iota
	// zoneStateStale is a chunk whose documents were never folded — today only
	// a chunk materialized from a persisted image. No entry may be created
	// against it (the first-sighting deduction in ZonePaths would be false),
	// so the next rebuild recomputes the whole summary instead.
	zoneStateStale
	// zoneStatePoisoned is a chunk holding a document the fold path cannot
	// summarize — an escaped member name. Recomputing would meet the same
	// document again, so the state is permanent for that chunk's lineage and
	// no further work is spent on it.
	zoneStatePoisoned
)

// Zone value codes. The three-bit tag reproduces compareScalar's cross-kind
// order (bool < number < string < container) so one uint32 orders values of
// mixed kinds, which is what a schemaless zone map needs: a path can hold a
// number in one document and a string in the next, and `x > 5` must not prune
// a chunk whose only value at x is "abc" (strings sort above numbers, so it
// matches).
const (
	zoneTagShift            = 29
	zonePayloadMask  uint32 = 1<<zoneTagShift - 1
	zoneTagBool      uint32 = 1 << zoneTagShift
	zoneTagNumber    uint32 = 2 << zoneTagShift
	zoneTagString    uint32 = 3 << zoneTagShift
	zoneTagContainer uint32 = 4 << zoneTagShift
)

// zonePruningEnabled is the global kill switch. It exists so a differential
// test can run one query body twice — once pruned, once not — and assert the
// two results are identical, which is the only practical way to make a
// pruning bug impossible to miss. It is deliberately process-wide and
// deliberately not an Option: an Option would change what is stored, and the
// differential needs the same stored bytes read two ways.
var zonePruningEnabled atomic.Bool

func init() { zonePruningEnabled.Store(true) }

// SetZonePruning enables or disables chunk-summary pruning process-wide and
// returns the previous setting. Disabling it does not change what is stored or
// what any query returns; it only forces the planner back onto the unpruned
// scan, which is what makes it a usable differential control. It is safe to
// call concurrently with queries, though a query already in flight may observe
// either setting.
func SetZonePruning(enabled bool) bool { return zonePruningEnabled.Swap(enabled) }

// ZonePruning reports whether chunk-summary pruning is currently enabled.
func ZonePruning() bool { return zonePruningEnabled.Load() }

// ZonePathHash hashes a decoded top-level member name into the key a
// [ZoneProbe] carries. It is FNV-1a rather than maphash so the same name
// hashes identically in every process, which keeps summaries reproducible in
// tests and leaves the door open to persisting them.
func ZonePathHash(name string) uint64 {
	// Eight bytes per multiply rather than one. Member names are short, so the
	// byte-at-a-time form spent as much of the write-path summary's budget on
	// hashing four-character names as the string scan spent on the document.
	// The trailing partial word folds in its own length so that two names
	// differing only in trailing NUL-equivalent padding cannot collide.
	h := uint64(14695981039346656037)
	b := byteview.Bytes(name)
	for len(b) >= 8 {
		h = (h ^ binary.LittleEndian.Uint64(b)) * 1099511628211
		b = b[8:]
	}
	if len(b) != 0 {
		// The trailing partial word is assembled register-side rather than
		// through a zero-filled array, because copy() of two to seven bytes
		// compiles to a memmove call, and a call was measurably the most
		// expensive thing about hashing a four-character member name.
		var tail uint64
		for i := len(b) - 1; i >= 0; i-- {
			tail = tail<<8 | uint64(b[i])
		}
		h = (h ^ (tail | uint64(len(b))<<56)) * 1099511628211
	}
	// A collection's member names share long prefixes far more often than
	// random strings do, and FNV's low bits stay correlated for inputs that
	// differ late. The entry scan compares whole words, so this only matters
	// if a caller ever buckets by low bits; the avalanche is one multiply and
	// two shifts and removes the question.
	h ^= h >> 33
	h *= 0xff51afd7ed558ccd
	h ^= h >> 33
	return h
}

func zonePathHashBytes(name []byte) uint64 {
	return ZonePathHash(byteview.String(name))
}

// ZoneCodeBool returns the zone code for a boolean literal.
func ZoneCodeBool(v bool) uint32 {
	if v {
		return zoneTagBool | 1
	}
	return zoneTagBool
}

// ZoneCodeNumber returns the zone code for a JSON number in its exact source
// spelling, and false when the spelling cannot be converted. Callers that get
// false must not build a probe from it: an arbitrary substitute code would
// break the monotonicity the pruning rules depend on.
func ZoneCodeNumber(num []byte) (uint32, bool) {
	if n, ok := zoneSmallInteger(num); ok {
		return zoneTagNumber | uint32(zoneMonotoneFloat(float64(n))>>35), true
	}
	f, err := strconv.ParseFloat(byteview.String(num), 64)
	if err != nil {
		// Range errors are the only failure a syntactically valid JSON number
		// can produce, and ParseFloat still returns the correctly signed
		// infinity (or zero on underflow), which is the correctly rounded — and
		// therefore still monotone — result. Anything else means the caller
		// handed over something that is not a JSON number.
		if numErr, ok := err.(*strconv.NumError); !ok || numErr.Err != strconv.ErrRange {
			return 0, false
		}
	}
	return zoneTagNumber | uint32(zoneMonotoneFloat(f)>>35), true
}

// zoneSmallInteger parses a plain integer of at most 15 digits, the widest
// decimal integer float64 represents exactly. It is not an approximation of
// ParseFloat, it is the same answer: int64 to float64 is exact below 2^53, and
// ParseFloat is correctly rounded, so both produce the identical float64 and
// therefore the identical code. Anything with a sign of a fraction, an
// exponent, or a sixteenth digit falls through to ParseFloat unchanged.
//
// It exists because folding is on the write path and ParseFloat is not cheap:
// a document with four integer members was spending more time converting them
// than scanning its whole source.
func zoneSmallInteger(num []byte) (int64, bool) {
	if len(num) == 0 {
		return 0, false
	}
	i, negative := 0, false
	if num[0] == '-' {
		negative, i = true, 1
	}
	if i == len(num) || len(num)-i > 15 {
		return 0, false
	}
	value := int64(0)
	for ; i < len(num); i++ {
		digit := num[i] - '0'
		if digit > 9 {
			return 0, false
		}
		value = value*10 + int64(digit)
	}
	if negative {
		value = -value
	}
	return value, true
}

// ZoneCodeString returns the zone code for an already-decoded string.
func ZoneCodeString(s string) uint32 {
	var b [4]byte
	n := copy(b[:], s)
	return zoneTagString | zonePackPrefix(&b, n, 0)
}

// zoneMonotoneFloat maps a float64 onto the uint64 whose unsigned order is the
// float's numeric order. -0 is folded onto +0 first: compareScalar treats them
// as one value, and a code that separated them would not be monotone.
func zoneMonotoneFloat(f float64) uint64 {
	if f == 0 {
		f = 0
	}
	b := math.Float64bits(f)
	if b>>63 != 0 {
		return ^b
	}
	return b | 1<<63
}

// zonePackPrefix packs the first four bytes of a byte string into a 29-bit
// payload, padding positions n..3 with pad. Truncating a byte string to a
// fixed-width zero-padded prefix is monotone under lexicographic order, which
// is what strings and containers are compared by; padding with 0xff instead
// gives the upper end of the interval an unknown suffix could reach.
func zonePackPrefix(b *[4]byte, n int, pad byte) uint32 {
	var v [4]byte = *b
	for i := n; i < 4; i++ {
		v[i] = pad
	}
	return uint32(v[0])<<21 | uint32(v[1])<<13 | uint32(v[2])<<5 | uint32(v[3])>>3
}

// zoneValueCodes classifies one raw JSON value and returns the closed interval
// [lo, hi] that provably contains its zone code, plus the flag bit the value
// contributes. A null contributes zoneFlagNull and no interval.
func zoneValueCodes(v []byte) (lo, hi uint32, flag uint8) {
	if len(v) == 0 {
		return 0, 0, 0
	}
	switch v[0] {
	case 'n':
		return 0, 0, zoneFlagNull
	case 't':
		c := ZoneCodeBool(true)
		return c, c, zoneFlagValue
	case 'f':
		c := ZoneCodeBool(false)
		return c, c, zoneFlagValue
	case '"':
		if len(v) < 2 {
			// Unreachable: the scanner only reports a string span once it has
			// found both quotes. Widening rather than slicing keeps a
			// malformed span from panicking inside a write.
			return zoneTagString, zoneTagString | zonePayloadMask, zoneFlagValue
		}
		lo, hi = zoneStringCodes(v[1 : len(v)-1])
		return lo, hi, zoneFlagValue
	case '{', '[':
		var b [4]byte
		n := copy(b[:], v)
		c := zoneTagContainer | zonePackPrefix(&b, n, 0)
		return c, c, zoneFlagValue
	default:
		c, ok := ZoneCodeNumber(v)
		if !ok {
			// Unreachable for a validated document. Widen to the whole number
			// range rather than invent a code: a wrong code would prune a
			// matching chunk, a maximally wide one only costs a scan.
			return zoneTagNumber, zoneTagNumber | zonePayloadMask, zoneFlagValue
		}
		return c, c, zoneFlagValue
	}
}

// zoneStringCodes bounds the zone code of a JSON string from its raw content
// bytes (the span inside the quotes). Unescaped bytes decode to themselves, so
// a prefix free of backslashes is exact. A backslash inside the first four
// bytes makes the decoded prefix unknown from here, and rather than decode on
// the write path the value is folded as the interval every string sharing the
// known prefix could occupy.
func zoneStringCodes(content []byte) (uint32, uint32) {
	var b [4]byte
	n := 0
	for i := 0; i < len(content) && n < 4; i++ {
		if content[i] == '\\' {
			return zoneTagString | zonePackPrefix(&b, n, 0x00),
				zoneTagString | zonePackPrefix(&b, n, 0xff)
		}
		b[n] = content[i]
		n++
	}
	c := zoneTagString | zonePackPrefix(&b, n, 0)
	return c, c
}

// find returns the index of the entry for hash, or -1.
func (z *chunkZone) find(hash uint64) int {
	for i := 0; i < int(z.n); i++ {
		if z.hash[i] == hash {
			return i
		}
	}
	return -1
}

// fold folds one document's source into z. priorDocs is the number of live
// documents already summarized by z; it is what a newly created entry needs to
// know that those documents did not carry the path (see ZonePaths for why that
// deduction is sound).
//
// Cost is one linear pass over src plus one hash and one 8-entry scan per
// top-level member — no parse, no tape, no allocation, and nothing proportional
// to the chunk's other 63 documents. That last property is the whole reason
// folding is merge-only: a chunk rebuild already copies 64 slots, and a summary
// that had to re-read all of them on every Put would multiply the write path's
// dominant cost instead of adding to it.
//
// Merge-only means the summary widens and never narrows. A replaced or deleted
// document's values stay in the bounds. That is a precision loss under heavy
// churn, never a correctness one, and it is the price of an O(1)-per-write
// maintenance cost.
func (z *chunkZone) fold(src []byte, priorDocs int) {
	if z.state != zoneStateOK {
		return
	}
	// seen marks the entries this document touched, so the entries it did not
	// touch can be marked absent. ZonePaths <= 32 keeps it one word.
	var seen uint32
	i := zoneSkipSpace(src, 0)
	if i >= len(src) || src[i] != '{' {
		// A root that is not an object carries no top-level member, so every
		// tracked path is absent in it. That is a complete summary, not a
		// failure.
		z.markAbsent(seen)
		return
	}
	i = zoneSkipSpace(src, i+1)
	if i < len(src) && src[i] == '}' {
		z.markAbsent(seen)
		return
	}
	for {
		if i >= len(src) || src[i] != '"' {
			z.state = zoneStatePoisoned
			return
		}
		keyStart := i + 1
		end, escaped, ok := zoneScanString(src, i)
		if !ok || escaped {
			// An escaped member name cannot be keyed without decoding it, and
			// silently skipping it would let a later document create an entry
			// for the same decoded name that does not cover this document —
			// a false negative. Poisoning the chunk costs pruning on a rare
			// corpus and costs nothing else.
			z.state = zoneStatePoisoned
			return
		}
		name := src[keyStart : end-1]
		i = zoneSkipSpace(src, end)
		if i >= len(src) || src[i] != ':' {
			z.state = zoneStatePoisoned
			return
		}
		i = zoneSkipSpace(src, i+1)
		valueStart := i
		valueEnd, ok := zoneSkipValue(src, i)
		if !ok {
			z.state = zoneStatePoisoned
			return
		}
		if slot := z.observe(name, priorDocs); slot >= 0 {
			seen |= 1 << uint(slot)
			lo, hi, flag := zoneValueCodes(src[valueStart:valueEnd])
			z.flag[slot] |= flag
			if flag == zoneFlagValue {
				if z.min[slot] > lo {
					z.min[slot] = lo
				}
				if z.max[slot] < hi {
					z.max[slot] = hi
				}
			}
		}
		i = zoneSkipSpace(src, valueEnd)
		if i >= len(src) {
			z.state = zoneStatePoisoned
			return
		}
		switch src[i] {
		case ',':
			i = zoneSkipSpace(src, i+1)
		case '}':
			z.markAbsent(seen)
			return
		default:
			z.state = zoneStatePoisoned
			return
		}
	}
}

// observe returns the entry slot for name, creating it if there is room, or -1
// when the per-chunk budget is exhausted. A path the budget turned away is
// turned away for the rest of the chunk's life, which is exactly what keeps a
// later entry from claiming coverage of documents it never saw.
func (z *chunkZone) observe(name []byte, priorDocs int) int {
	hash := zonePathHashBytes(name)
	if slot := z.find(hash); slot >= 0 {
		return slot
	}
	if int(z.n) == ZonePaths {
		z.overflow = true
		return -1
	}
	slot := int(z.n)
	z.n++
	z.hash[slot] = hash
	// An empty interval: the first folded value sets both ends.
	z.min[slot] = ^uint32(0)
	z.max[slot] = 0
	z.flag[slot] = 0
	if priorDocs > 0 {
		// Every document folded before this one lacked the path, or it would
		// already have an entry: the table only grows, so nothing can have
		// been turned away earlier and admitted now.
		z.flag[slot] = zoneFlagAbsent
	}
	return slot
}

func (z *chunkZone) markAbsent(seen uint32) {
	for i := 0; i < int(z.n); i++ {
		if seen&(1<<uint(i)) == 0 {
			z.flag[i] |= zoneFlagAbsent
		}
	}
}

// rebuild recomputes a chunk's summary from scratch by folding every document
// in ordinal order. It is the recovery path for a stale zone — today only a
// chunk restored from a persisted image, whose documents were never folded —
// and it runs at most once per such chunk, on the first write that rebuilds
// it. Folding by ordinal is what makes priorDocs correct: a
// rebuilt chunk's ordinals are dense and assigned in slot order, so ordinal i
// has exactly i documents before it.
func (z *chunkZone) rebuild(docs *Segment) {
	*z = chunkZone{}
	for ord, count := 0, docs.Len(); ord < count; ord++ {
		z.fold(docs.RawAt(ord), ord)
	}
}

// zoneMaintain brings the rebuilt chunk's summary up to date from the chunk it
// replaces. It is the single maintenance seam every mutation funnels through
// (buildStoreChunk and its schema twin), so an insert, a replacement, a
// delete, an index backfill, and an index reclaim cannot
// disagree about what a chunk's summary means.
//
// src is the one document this rebuild parsed, or nil when every document came
// from old by reference. That asymmetry is the point: carrying the old
// summary forward and folding only the new document keeps maintenance O(1) in
// the chunk's size, which is the only shape that does not multiply a write
// path whose dominant cost is already the 64-slot rebuild.
func zoneMaintain(chunk *Chunk, old *Chunk, src []byte) {
	priorDocs := 0
	if old != nil {
		chunk.zone = old.zone
		priorDocs = bits.OnesCount64(old.Live)
	}
	if chunk.zone.state == zoneStateStale {
		chunk.zone.rebuild(&chunk.Docs)
		return
	}
	if src != nil {
		chunk.zone.fold(src, priorDocs)
	}
}

// ZoneOp is the predicate shape a [ZoneProbe] asks a chunk summary about.
type ZoneOp uint8

const (
	// ZoneOpNone is a predicate the zone tier cannot answer. A zero ZoneProbe
	// is inert.
	ZoneOpNone ZoneOp = iota
	// ZoneOpEq covers equality and membership alike: Lo and Hi bound the
	// candidate literal codes, and for a single equality they are equal.
	ZoneOpEq
	ZoneOpNe
	ZoneOpLt
	ZoneOpLe
	ZoneOpGt
	ZoneOpGe
	// ZoneOpIsNull matches an absent path or an explicit null, the two the
	// query treats as one value.
	ZoneOpIsNull
	// ZoneOpExists matches any present path, explicit null included.
	ZoneOpExists
)

// A ZoneProbe is one compiled leaf predicate expressed against chunk
// summaries. Query compiles it once, so probing costs no per-execution work
// beyond the chunk walk itself and allocates nothing.
type ZoneProbe struct {
	// Path is ZonePathHash of the top-level member name.
	Path uint64
	// Lo and Hi are the zone codes bounding the predicate's literals. They are
	// unused by ZoneOpIsNull, ZoneOpExists, and ZoneOpNe.
	Lo, Hi uint32
	Op     ZoneOp
}

// keep reports whether a chunk whose summary is z may contain a row matching
// probe. It is the single place the conservative-pruning rules from the file
// header are applied, and every path that is not certain returns true.
func (z *chunkZone) keep(probe ZoneProbe) bool {
	if z.state != zoneStateOK {
		return true
	}
	slot := z.find(probe.Path)
	if slot < 0 {
		if z.overflow {
			// The budget dropped at least one path, so a missing entry proves
			// nothing about this one and the chunk may hold anything.
			return true
		}
		// No document in this chunk carries the path. Every cell is therefore
		// absent, which satisfies IS NULL, fails EXISTS, and fails every
		// comparison (evalCmp rejects a null cell before comparing).
		return probe.Op == ZoneOpIsNull
	}
	flag := z.flag[slot]
	switch probe.Op {
	case ZoneOpIsNull:
		return flag&(zoneFlagAbsent|zoneFlagNull) != 0
	case ZoneOpExists:
		return flag&(zoneFlagNull|zoneFlagValue) != 0
	}
	// Every remaining operator is a comparison, and evalCmp rejects a null or
	// absent cell before comparing, so a chunk with no comparable value at
	// this path cannot match any of them.
	if flag&zoneFlagValue == 0 {
		return false
	}
	mn, mx := z.min[slot], z.max[slot]
	switch probe.Op {
	case ZoneOpEq:
		// A match needs a code in both the literal interval and the chunk
		// interval; disjoint intervals prove there is none.
		return probe.Lo <= mx && probe.Hi >= mn
	case ZoneOpLt, ZoneOpLe:
		return mn <= probe.Hi
	case ZoneOpGt, ZoneOpGe:
		return mx >= probe.Lo
	default:
		// ZoneOpNe is satisfied by any value other than the literal, which
		// min/max cannot rule out; reaching here already proved the chunk has
		// a comparable value, which is all the pruning inequality allows.
		return true
	}
}

// AppendZoneMasks appends one mask per chunk that may contain a row matching
// probe, and reports how many chunks it skipped and whether the result is a
// real bound. It reports unbounded when pruning is disabled, when the probe is
// inert, or when no chunk was pruned — an unpruned mask set is a sound
// superset but a useless one, and reporting it as unbounded lets the caller
// skip the downstream mask intersection and the sparse-row gather it would
// otherwise trigger.
//
// Masks are emitted in strictly ascending chunk order with each chunk's exact
// live-slot word, which is what [Mask]'s ordering and liveness contract
// requires. With sufficient dst capacity the operation allocates nothing.
func (s Snapshot) AppendZoneMasks(dst []Mask, probe ZoneProbe) ([]Mask, int, bool) {
	if probe.Op == ZoneOpNone || s.state == nil || !zonePruningEnabled.Load() {
		return dst, 0, false
	}
	pruned := 0
	s.state.Chunks.Each(func(id uint32, chunk *Chunk) bool {
		if !chunk.zone.keep(probe) {
			pruned++
			return true
		}
		dst = append(dst, Mask{Chunk: id, Bits: chunk.Live})
		return true
	})
	if pruned == 0 {
		return dst[:0], 0, false
	}
	return dst, pruned, true
}

var _ ZoneSource = Snapshot{}

// ZoneSource is the optional block-pruning capability a snapshot may offer.
// It is deliberately separate from [IndexSource]: a backend without chunk
// summaries is complete without it, and query type-asserts for it exactly as
// it does for [LiveMaskSource]. Both shipped backends offer it — this file for
// heap chunks, store/durable/store_file_zone.go for chunks on disk, whose
// summaries are a narrower encoding of the same statistics (see
// store_zone_compact.go) probed by the identical [ZoneProbe].
type ZoneSource interface {
	AppendZoneMasks(dst []Mask, probe ZoneProbe) ([]Mask, int, bool)
}

// ZoneStats reports how many chunks this snapshot has, how many carry usable
// statistics, and how many distinct paths are tracked in total. It exists for
// tests and for benchmark reporting; it is not on any execution path.
func (s Snapshot) ZoneStats() (chunks, sound, paths int) {
	if s.state == nil {
		return 0, 0, 0
	}
	s.state.Chunks.Each(func(_ uint32, chunk *Chunk) bool {
		chunks++
		if chunk.zone.state == zoneStateOK {
			sound++
			paths += int(chunk.zone.n)
		}
		return true
	})
	return chunks, sound, paths
}

// ZoneChunksScanned reports how many of this snapshot's chunks survive probe.
// It is the measurement hook the pruning benchmarks report chunks-skipped
// from, and it performs no document I/O.
func (s Snapshot) ZoneChunksScanned(probe ZoneProbe) (kept, total int) {
	if s.state == nil {
		return 0, 0
	}
	s.state.Chunks.Each(func(_ uint32, chunk *Chunk) bool {
		total++
		if chunk.zone.keep(probe) {
			kept++
		}
		return true
	})
	return kept, total
}

// zoneSkipSpace advances past JSON insignificant whitespace.
func zoneSkipSpace(src []byte, i int) int {
	// Compact JSON is the overwhelming case and the fold path calls this four
	// times per member, so the no-whitespace answer is one comparison: every
	// byte JSON treats as insignificant whitespace is at or below space.
	if i < len(src) && src[i] > ' ' {
		return i
	}
	for i < len(src) {
		switch src[i] {
		case ' ', '\t', '\n', '\r':
			i++
		default:
			return i
		}
	}
	return i
}

// zoneScanString returns the offset just past the JSON string starting at
// src[i], whether it contained any escape, and whether it terminated. Skipping
// two bytes at a backslash is correct for every JSON escape including \uXXXX,
// whose four hex digits are ordinary characters.
func zoneScanString(src []byte, i int) (int, bool, bool) {
	// bytes.IndexByte is vectorized and a byte-at-a-time loop is not, which
	// matters because the longest thing the fold path walks is usually a
	// string value it has no interest in. Escapes are handled by resuming the
	// search past the escaped byte rather than by inspecting every byte.
	// The shared JSON string scanner stops at the first quote, backslash, or
	// control byte in one vectorized pass. Folding walks far more string bytes
	// than it inspects — a long text member is skipped whole — so this is the
	// single hottest thing the write-path summary does, and a byte-at-a-time
	// loop or a pair of IndexByte passes measured roughly twice its cost.
	escaped := false
	for i++; i < len(src); {
		j := scanner.IndexStringSyntax(src, i)
		if j >= len(src) {
			return len(src), escaped, false
		}
		switch src[j] {
		case '"':
			return j + 1, escaped, true
		case '\\':
			// Two bytes is right for every JSON escape including \uXXXX,
			// whose four hex digits are ordinary characters.
			escaped = true
			i = j + 2
		default:
			// A raw control byte inside a string is not valid JSON, so the
			// document this came from was never validated. Refuse rather than
			// guess at its extent.
			return j, escaped, false
		}
	}
	return len(src), escaped, false
}

// zoneSkipValue returns the offset just past the JSON value starting at
// src[i]. The input is a document the segment already validated, so the scan
// only has to find the value's extent, not re-check its grammar; it still
// reports failure rather than reading out of range if it ever meets input it
// cannot account for.
func zoneSkipValue(src []byte, i int) (int, bool) {
	if i >= len(src) {
		return i, false
	}
	switch src[i] {
	case '"':
		end, _, ok := zoneScanString(src, i)
		return end, ok
	case '{', '[':
		depth := 0
		for i < len(src) {
			switch src[i] {
			case '"':
				end, _, ok := zoneScanString(src, i)
				if !ok {
					return i, false
				}
				i = end
				continue
			case '{', '[':
				depth++
			case '}', ']':
				depth--
				if depth == 0 {
					return i + 1, true
				}
			}
			i++
		}
		return i, false
	default:
		for i < len(src) {
			switch src[i] {
			case ',', '}', ']', ' ', '\t', '\n', '\r':
				return i, true
			}
			i++
		}
		return i, true
	}
}

// ZoneChunkBytes is the fixed per-chunk cost of the summary. It is exported so
// tests and benchmarks check the space claim in this file's documentation
// rather than repeating it.
func ZoneChunkBytes() int { return int(unsafe.Sizeof(chunkZone{})) }
