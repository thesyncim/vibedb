package query

import (
	"fmt"
	"hash/maphash"

	"github.com/thesyncim/vibedb/store"
)

// The fan-out build side.
//
// A semi-join asks whether a driving row has a partner, and this package
// answers that without ever producing one: a collected set searched per row, or
// a keyed probe behind a Bloom filter. An inner join asks which partners, and
// that question has no answer that avoids materializing them — the result
// carries one row per matching pair, so the pairs have to exist.
//
// So a fan-out clause abandons the strategy choice entirely and builds the one
// structure that can answer it: every surviving joined row, addressed by its
// join value. It is a hash multimap rather than a sorted set because the same
// structure has to serve two questions — "is there a partner" for the filter
// phase and "which partners" for the expansion — and a chain walk answers both
// from one lookup.
//
// # Why the entries are not deduplicated
//
// The semi-join's set is deduplicated, and it has to be: two joined documents
// with one key must not select the driving row twice. Here the opposite is
// required. Two joined documents with one key are two output rows, and that is
// the whole difference between the two operators. The build therefore keeps one
// entry per surviving joined row, and the dedup that makes a membership cheap
// is exactly what would make this wrong.
//
// # Ordering
//
// Pairs are produced in driving-ordinal order, and within one driving row in
// ascending joined-row address order. Both halves are structural rather than
// incidental. The outer half is the expansion walking the selection, which the
// filter phase already produced in ascending scan order — including across
// workers, whose ranges are concatenated in range order. The inner half is
// bucket chains built back to front (see index), so walking a chain forward
// yields the entries in the order the inner scan appended them, which is the
// joined collection's own stable chunk and slot order.
//
// That rule is what makes a fan-out result reproducible, and what lets the
// exhaustive differential compare it against a naive nested loop that simply
// walks both collections in order.

// joinBuildEmpty marks the end of a bucket chain and an empty bucket. It is
// -1 rather than 0 because 0 is a valid entry index, and a sentinel that
// collides with a real one would silently truncate the first entry's chain.
const joinBuildEmpty = int32(-1)

// A joinBuild is one fan-out clause's materialized joined side: every row that
// survived the clause's own filter, in the joined collection's scan order,
// indexed by join value.
//
// Its storage lives in the executing [Workspace] and is reused across
// executions like every other buffer here. That reuse is what keeps a fan-out
// join allocation-free in the steady state, which matters more here than
// anywhere else in this package: a fan-out produces more rows than the driving
// collection has, so anything allocated per pair would scale with the output.
type joinBuild struct {
	// values, rows, and next are parallel per-entry arrays. Keeping them
	// separate rather than as one slice of structs is what lets the probe walk
	// a chain through next and touch values only for the entries it reaches,
	// instead of straddling a cache line per hop over fields it does not read.
	values []scalar
	rows   []store.Location
	next   []int32
	// buckets is the open hash directory: a power-of-two table of chain heads.
	buckets []int32
	mask    uint32
	// text owns every byte values views, copied out of the joined side's
	// extraction because that extraction's buffers are refilled batch to batch.
	text []byte
	// seed keys client-controlled join values before they reach the bucket
	// directory. It is minted once per retained Workspace and survives reset,
	// so a warmed join pays neither randomness nor allocation while collision
	// construction remains impractical.
	seed   maphash.Seed
	seeded bool
	// active reports whether this clause built a side at all. A clause that
	// only filters never does, and reading a build that was never filled must
	// fail closed rather than report no matches for a reason it cannot name.
	active bool
}

// reset empties b for a new execution while keeping its storage. The scalars
// are cleared rather than truncated because they view the previous execution's
// joined collection, and leaving them reachable through the retained capacity
// would pin it for the Workspace's lifetime.
func (b *joinBuild) reset() {
	clear(b.values)
	b.values = b.values[:0]
	b.rows = b.rows[:0]
	b.next = b.next[:0]
	b.text = b.text[:0]
	b.active = false
}

// add records one surviving joined row. cell is the row's join value, already
// copied into b's own storage by the caller.
func (b *joinBuild) add(cell scalar, row store.Location) error {
	if len(b.values) >= int64ToInt(maxJoinBuildEntries) {
		return fmt.Errorf(
			"query: join build exceeds %d entries addressable by its int32 chains",
			maxJoinBuildEntries,
		)
	}
	b.values = append(b.values, cell)
	b.rows = append(b.rows, row)
	return nil
}

// index builds the bucket directory over the entries collected so far. It is a
// separate pass rather than an insert per entry because the entry count is only
// known at the end, and sizing the directory to it is what keeps the chains
// short without either over-allocating for a small joined side or rehashing a
// large one.
//
// The entries are linked back to front. That is the ordering rule made
// structural: prepending in reverse leaves every chain holding its entries in
// ascending entry order, which is ascending joined-row address order, which is
// the order the expansion must emit them in.
func (b *joinBuild) index() error {
	if !b.seeded {
		b.seed = maphash.MakeSeed()
		b.seeded = true
	}
	buckets, err := joinBuildBuckets(len(b.values))
	if err != nil {
		return err
	}
	if cap(b.buckets) < buckets {
		b.buckets = make([]int32, buckets)
	} else {
		b.buckets = b.buckets[:buckets]
	}
	for i := range b.buckets {
		b.buckets[i] = joinBuildEmpty
	}
	b.mask = uint32(buckets - 1)
	b.next = resize(b.next, len(b.values))
	for i := len(b.values) - 1; i >= 0; i-- {
		slot := uint32(hashJoinValue(b.seed, b.values[i])) & b.mask
		b.next[i] = b.buckets[slot]
		b.buckets[slot] = int32(i)
	}
	b.active = true
	return nil
}

// joinBuildBuckets is the power-of-two directory size for n entries. Two
// buckets per entry keeps the average chain at half a hop for distinct values,
// which is what makes a probe's cost the exact-comparison of the matches it
// finds rather than the hops it took to reach them.
const maxJoinBuildEntries int64 = 1<<31 - 1

func joinBuildBuckets(n int) (int, error) {
	maxInt := int(^uint(0) >> 1)
	if n < 0 || int64(n) > maxJoinBuildEntries || n > maxInt/2 {
		return 0, fmt.Errorf(
			"query: join build size %d is not representable by its int32 chains",
			n,
		)
	}
	buckets := 1
	for buckets < n*2 {
		if buckets > maxInt/2 {
			return 0, fmt.Errorf(
				"query: join build bucket directory overflows int for %d entries",
				n,
			)
		}
		buckets *= 2
	}
	return buckets, nil
}

func int64ToInt(n int64) int {
	maxInt := int64(^uint(0) >> 1)
	if n > maxInt {
		return int(maxInt)
	}
	return int(n)
}

// appendMatches appends the joined rows whose join value equals cell, in
// ascending joined-row address order, and reports how many it appended.
//
// The hash only chooses a chain; membership is decided by the same
// compareScalar every other equality in this engine uses, so a fan-out join
// matches exactly the pairs a nested loop would and exactly the driving rows
// the semi-join would keep.
func (b *joinBuild) appendMatches(dst []store.Location, cell scalar, limit int) ([]store.Location, int, bool) {
	if !b.active || cell.kind == kindNull {
		// A null or absent driving key joins to nothing, the rule every other
		// comparison in this engine applies.
		return dst, 0, false
	}
	found := 0
	for i := b.buckets[uint32(hashJoinValue(b.seed, cell))&b.mask]; i != joinBuildEmpty; i = b.next[i] {
		if compareScalar(b.values[i], cell) == 0 {
			if limit >= 0 && len(dst) >= limit {
				return dst, found, true
			}
			dst = append(dst, b.rows[i])
			found++
		}
	}
	return dst, found, false
}

// has reports whether cell has any partner. It is appendMatches without the
// append, for the filter phase, which asks only the existential question and
// would otherwise pay to build a list it discards.
func (b *joinBuild) has(cell scalar) bool {
	if !b.active || cell.kind == kindNull {
		return false
	}
	for i := b.buckets[uint32(hashJoinValue(b.seed, cell))&b.mask]; i != joinBuildEmpty; i = b.next[i] {
		if compareScalar(b.values[i], cell) == 0 {
			return true
		}
	}
	return false
}

// hashJoinValue hashes a join value so that two values compareScalar reports
// equal always hash equal.
//
// Numbers hash their canonical exact-decimal decomposition rather than either
// their spelling or a float64 image. Thus 1, 1.0, and 10e-1 share a hash while
// adjacent integers beyond 2^53 and arbitrarily wide exponents still spread
// across buckets. Containers hash their source bytes, which is exactly what
// compareScalar orders them by.
func hashJoinValue(seed maphash.Seed, cell scalar) uint64 {
	switch cell.kind {
	case kindBool:
		if cell.bval {
			return 0x9e3779b97f4a7c15
		}
		return 0xbf58476d1ce4e5b9
	case kindNumber:
		return hashExactJoinNumber(seed, cell.num)
	case kindString:
		return maphash.String(seed, cell.sval)
	default:
		return maphash.Bytes(seed, cell.raw)
	}
}

// hashExactJoinNumber streams the same canonical fields appendNumberKey
// encodes, but never materializes the key. maphash supplies a keyed bucket
// hash, while writing each field directly keeps the path allocation-free even
// for exponent spellings whose adjusted weight is wider than an int64.
func hashExactJoinNumber(seed maphash.Seed, num []byte) uint64 {
	d := parseDecimal(num)
	var h maphash.Hash
	h.SetSeed(seed)
	if d.zero {
		_ = h.WriteByte(0)
		return h.Sum64()
	}

	_ = h.WriteByte(1)
	if d.neg {
		_ = h.WriteByte(1)
	} else {
		_ = h.WriteByte(0)
	}

	weight := &d.weight
	var compact decimalWeight
	if !weight.wide {
		compact = wideWeightFromInt64(weight.compact)
		weight = &compact
	}
	if weight.neg {
		_ = h.WriteByte(1)
	} else {
		_ = h.WriteByte(0)
	}
	weightLen := weightMagnitudeLen(weight)
	hashJoinUint(&h, uint64(weightLen))
	for i := 0; i < weightLen; i++ {
		_ = h.WriteByte(weightMagnitudeDigit(weight, i))
	}

	hashJoinUint(&h, uint64(len(d.intDigits)+len(d.fracDigits)))
	_, _ = h.Write(d.intDigits)
	_, _ = h.Write(d.fracDigits)
	return h.Sum64()
}

func hashJoinUint(h *maphash.Hash, value uint64) {
	for range 8 {
		_ = h.WriteByte(byte(value))
		value >>= 8
	}
}

// mixJoinHash is a bijective avalanche, so adjacent integers do not land in
// adjacent buckets and an ordinary sequence of identifiers spreads instead of
// clustering into one chain.
func mixJoinHash(v uint64) uint64 {
	v ^= v >> 30
	v *= 0xbf58476d1ce4e5b9
	v ^= v >> 27
	v *= 0x94d049bb133111eb
	v ^= v >> 31
	return v
}
