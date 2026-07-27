package query

import (
	"hash/maphash"
	"math/bits"
)

// The semi-join reduction filter.
//
// A lookup-bound join probes the inner collection once per outer row and prunes
// nothing. That is the right shape when most outer rows match, and the wrong one
// when most do not: a probe that ends in "no partner" costs a key lookup, a
// document decode, and an inner predicate evaluation, all to answer no.
//
// A Bloom filter over the inner side's surviving join keys answers that same no
// from one cache line. It is the classic Bloomjoin / semi-join reduction, and it
// is strictly additive here because it can only produce false positives: a row
// the filter admits is still decided by the exact probe behind it, so the filter
// changes how many probes run and never what any of them concludes. That
// one-sided error is what lets it be adopted without a second correctness
// argument — there is no configuration of it that returns a wrong row, only
// configurations that waste time.
//
// # Where the win actually is
//
// It is worth being precise, because the obvious intuition is wrong. An outer
// row whose key is simply absent from the inner collection is already cheap to
// reject: the key lookup misses in the hash directory and returns. The filter
// saves little there. The rows it saves real work on are the ones whose key is
// PRESENT but whose inner document fails the inner filter — those pay a full
// lookup, a document decode, and a predicate evaluation before answering no.
// The filter holds only the keys that passed the inner predicate, so it rejects
// exactly that population, and that is the population a selective join is made
// of.
//
// # What it costs, and what that forced
//
// It is not free and it is not always right. Building it means scanning the
// whole inner side, which is exactly the work the lookup strategy exists to
// avoid. So the filter is a third strategy with its own cost profile rather
// than an improvement to the second one.
//
// The first version of this gated the scan on cardinalities alone
// (joinBloomScanRatio) and was measured to be 1.56x SLOWER than no filter when
// the inner predicate kept everything: the scan was paid in full and the filter
// then admitted every row. Bounding that loss by halving the ratio did not work
// either — it declined filters measured to be 1.55x wins. What fixed it was
// measuring instead of bounding: joinBinding.keepFiltering re-reads the inner
// predicate's observed selectivity after every batch and abandons a scan that
// can no longer pay, which costs one batch rather than a whole collection.
// A second measurement, after the executor grew its late-materialization
// phase, then overturned how that check charges for the scan; keepFiltering
// records what changed and why.
//
// With both gates, BenchmarkJoinBloomPrefilter over a 20,000-row driving
// collection and 40,000 inner rows (ns per outer row, Apple M4 Max, Go 1.26):
//
//	inner filter keeps   policy   filter off   filter kept
//	                1%    155.7        282.0           yes
//	               10%    194.7        293.5           yes
//	               50%    283.9        280.6       no, abandoned
//	              100%    285.1        280.5       no, abandoned
//
// so the filter is worth 1.5x to 1.8x where it applies and costs nothing
// measurable where it does not.

// joinBloomBlock is one 32-byte block: eight 32-bit words, each carrying
// exactly one of the filter's eight bits per key. Confining a key's whole
// signature to one block is the point of the design — a classic Bloom filter's
// k probes are k independent cache misses, while this is one, and testing the
// block is eight loads and eight ANDs over a single cache line.
//
// Provenance: ALGO-BLOOM-BLOCKED-001.
type joinBloomBlock [8]uint32

// joinBloomSalt spreads one 32-bit hash into eight independent in-word bit
// positions. Each salt is odd, so multiplication by it is a bijection on
// uint32 and the eight derived positions stay independent for any input.
//
// Provenance: ALGO-BLOOM-BLOCKED-001.
var joinBloomSalt = [8]uint32{
	0x47b6137b, 0x44974d91, 0x8824ad5b, 0xa2b7289d,
	0x705495c7, 0x2df1424b, 0x9efc4947, 0x5c6bfb31,
}

// joinBloomBits is how many bits the filter allocates per expected key. Eight
// of them are set per key, so this is the classic m/n.
//
// TestJoinBloomFalsePositiveRate measures what 16 buys: 0.01% over 20,000 keys,
// an order of magnitude better than the textbook figure for this layout because
// rounding the block count up to a power of two hands back extra headroom on
// average — that case gets 26 bits per key rather than 16. The measurement is
// the number to trust; the constant is what it was produced from.
//
// A lower ratio would still work: the filter's error is one-sided, so a worse
// rate costs probes and never rows. 16 is chosen because the memory is capped
// by joinBloomMaxBytes regardless, which makes accuracy the only thing left to
// spend the budget on.
const joinBloomBits = 16

// joinBloomMaxBytes caps one binding's filter. A filter is a summary, and a
// summary that scales without bound is just a copy of what it summarizes — the
// thing the lookup strategy exists to avoid materializing. Past this size the
// false-positive rate degrades and the filter admits more rows, which costs
// probes and never correctness.
const joinBloomMaxBytes = 1 << 20

// A joinBloom is a blocked Bloom filter over one inner side's join keys. The
// zero value is an unsized, inactive filter that admits everything.
type joinBloom struct {
	blocks []joinBloomBlock
	// mask selects a block. The block count is a power of two so this is an
	// AND rather than a division on the per-row path.
	mask uint32
	// active reports whether the filter was built and may be consulted. A
	// filter that was never sized admits everything, so the probe path can read
	// this one bool instead of branching on a nil slice and a mode.
	active bool

	// inserted is evidence, not control: it is what the benchmarks and the
	// non-vacuity tests read to confirm the filter was built over the whole
	// inner side. It is written only while binding, on the calling goroutine.
	//
	// The test and admit tallies are deliberately not here. Those are written
	// once per outer row, by whichever goroutine is evaluating that row, and a
	// counter on the shared filter would be a data race rather than a slow
	// path; they live on the per-evaluator joinProbe instead.
	inserted uint64
}

// joinBloomSeed is the hash seed. It is a package value rather than a
// per-binding one because insert and test must agree within an execution and
// nothing outside one execution ever reads a filter; one seed for the process
// is the cheapest way to satisfy that, and maphash's seeded hash is already
// the fastest string hash available here.
var joinBloomSeed = maphash.MakeSeed()

// reset sizes b for keys and empties it, keeping the block storage. keys is an
// exact upper bound on the number of insertions — the inner side's candidate
// count — not an estimate, which is what lets the sizing be decided before the
// first insert instead of discovered after the last one.
func (b *joinBloom) reset(keys int) {
	blocks := joinBloomBlocks(keys)
	if cap(b.blocks) < blocks {
		b.blocks = make([]joinBloomBlock, blocks)
	} else {
		b.blocks = b.blocks[:blocks]
		clear(b.blocks)
	}
	b.mask = uint32(blocks - 1)
	b.active = true
	b.inserted = 0
}

// disable turns the filter off without freeing it, the state every join that
// did not build one leaves it in.
func (b *joinBloom) disable() {
	b.active = false
	b.inserted = 0
}

// joinBloomBlocks is the power-of-two block count for keys, floored at one
// block and capped at joinBloomMaxBytes.
func joinBloomBlocks(keys int) int {
	if keys < 1 {
		keys = 1
	}
	// Each block holds 256 bits, and the rounding up to a power of two below
	// only ever adds capacity, so this cannot under-size the filter.
	blocks := (keys*joinBloomBits + 255) / 256
	const maxBlocks = joinBloomMaxBytes / 32
	if blocks > maxBlocks {
		return maxBlocks
	}
	if blocks < 1 {
		return 1
	}
	return 1 << uint(bits.Len(uint(blocks-1)))
}

// signature derives a key's block index and its eight in-block bits.
//
// The block comes from the high half of the hash and the bits from the low
// half, which is what the reference layout does so that a collision in one is
// independent of a collision in the other. That reason was tested here and did
// not hold up: deriving both from the low half measures an identical
// false-positive rate (TestJoinBloomFalsePositiveRate, 0.0001 either way),
// because maphash already avalanches and the odd-salt multiplication
// decorrelates the in-block bits from the index by itself. The split is kept
// anyway — it costs one shift, and it is the property the published
// false-positive arithmetic assumes — but it is kept on the strength of the
// reference rather than of a measurement, and a reader should not go looking
// for the win it does not have.
func (b *joinBloom) signature(hash uint64) (int, joinBloomBlock) {
	var word joinBloomBlock
	low := uint32(hash)
	for i := range word {
		word[i] = uint32(1) << ((low * joinBloomSalt[i]) >> 27)
	}
	return int(uint32(hash>>32) & b.mask), word
}

// insert records one inner join key.
func (b *joinBloom) insert(hash uint64) {
	index, word := b.signature(hash)
	block := &b.blocks[index]
	for i := range word {
		block[i] |= word[i]
	}
	b.inserted++
}

// admits reports whether the inner side may hold this key. False is certain;
// true is not, and the exact probe behind it is what decides.
//
// The filter itself is read-only here — several filter-phase workers test the
// same blocks concurrently — so the tallies go to the caller's own scratch.
func (b *joinBloom) admits(hash uint64, pr *joinProbe) bool {
	index, word := b.signature(hash)
	block := &b.blocks[index]
	pr.tested++
	for i := range word {
		if block[i]&word[i] == 0 {
			return false
		}
	}
	pr.admitted++
	return true
}

// hashJoinKey hashes a collection key.
//
// The filter is built and tested over collection keys only, because it exists
// solely for the lookup strategy and the lookup strategy exists solely for
// joins against a primary key. That is what keeps this one line rather than a
// case per scalar kind: extending the filter to an arbitrary join value would
// have to hash numbers by their float64 image rather than by their spelling, so
// that 1 and 1.0 — one value under compareScalar — could not land in two
// buckets and turn a real match into a rejection the exact probe never gets to
// overturn.
func hashJoinKey(key string) uint64 {
	return maphash.String(joinBloomSeed, key)
}
