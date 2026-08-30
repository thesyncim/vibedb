package store

import (
	"bytes"
	"errors"
	"fmt"
	"hash/maphash"
	"math"
	"math/bits"
	"slices"
	"strings"

	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/document"
)

// MaxIndexColumns bounds compound exact indexes. Each indexed document
// is extracted with fixed stack storage, so the bound is part of the no-
// transient-allocation maintenance contract rather than an arbitrary parser
// limit. Wider predicates can combine independent indexes with bitmap AND.
const MaxIndexColumns = 4

// IndexDefinition declares one exact scalar index. Paths are RFC 6901
// JSON Pointers. One path creates a column index; two or more create an
// order-sensitive compound key. Missing, unresolvable, and container values
// are omitted; null, booleans, exact JSON numbers, and decoded strings are
// indexed.
type IndexDefinition struct {
	Name  string
	Paths []string
	// Unique marks a logical exact-index alias as a uniqueness constraint.
	// Durable collections validate existing rows before publication, persist
	// this bit, and enforce it on later point, batch, bulk, and replay writes.
	Unique bool
}

var (
	// ErrIndexDefinition reports an empty name, invalid path, or invalid
	// compound width.
	ErrIndexDefinition = errors.New("vibedb: invalid collection index definition")
	// ErrIndexArity reports a lookup whose value count does not match the
	// declared column count.
	ErrIndexArity = errors.New("vibedb: collection index lookup arity mismatch")
	// ErrIndexScalar reports a lookup value that is absent, invalid, or a
	// JSON container. Exact indexes deliberately accept scalars only.
	ErrIndexScalar = errors.New("vibedb: collection exact index requires scalar values")
	// ErrUniqueIndexViolation reports existing live rows that map to the same
	// non-NULL canonical exact-index term. Callers may use errors.Is to retain
	// constraint identity across SQL and wire-protocol error translation.
	ErrUniqueIndexViolation = errors.New("vibedb: unique index constraint violated")
	// ErrMaskOrder reports a sparse bitmap stream whose chunk ids are not
	// strictly increasing. Ordered masks permit allocation-free merge, lookup,
	// and range execution without copying or sorting caller storage.
	ErrMaskOrder = errors.New("vibedb: collection masks are not strictly ordered")
	// ErrMaskChunk reports a non-zero mask for a chunk absent from the
	// selected snapshot. Failing closed prevents stale or cross-snapshot masks
	// from silently dropping rows.
	ErrMaskChunk = errors.New("vibedb: collection mask chunk is not live")
)

// ExactIndex is the compiled, allocation-free form of one exact-index
// definition: the ordered set of column pointers extracted from each document,
// the source path spellings, and the hash seed that turns a column tuple into a
// posting key. It is immutable after CompileExactIndex and shared by every
// snapshot that maintains the index. N is the number of active columns.
type ExactIndex struct {
	Paths [MaxIndexColumns]vibejson.CompiledPointer
	Specs [MaxIndexColumns]string
	seed  maphash.Seed
	N     uint8
}

type storeIndexSnapshot struct {
	name  string
	state IndexState
	exact *ExactIndex
	root  *storeIndexPostingNode
	base  *storePackedIndex
	dirty storeIndexMaskVector
}

// Mask is one chunk's stable-slot candidate bitmap. Exact index result
// producers return strictly ordered live bits. Range consumers also accept
// dead candidate bits and ignore them, which lets complement plans remain
// metadata-only until the selected document page is admitted.
type Mask struct {
	Chunk uint32
	Bits  uint64
}

// CompileExactIndex validates one IndexDefinition and compiles its paths into
// an ExactIndex ready for extraction and posting maintenance. It rejects empty,
// oversized, or malformed definitions so the returned index can be used without
// re-checking, and derives a deterministic seed from the paths so the same
// definition always hashes tuples identically.
func CompileExactIndex(def IndexDefinition) (*ExactIndex, error) {
	if def.Name == "" {
		return nil, fmt.Errorf("%w: name is empty", ErrIndexDefinition)
	}
	if len(def.Paths) == 0 || len(def.Paths) > MaxIndexColumns {
		return nil, fmt.Errorf("%w: path count must be in [1,%d]", ErrIndexDefinition, MaxIndexColumns)
	}
	out := &ExactIndex{N: uint8(len(def.Paths))}
	for i, spec := range def.Paths {
		owned := strings.Clone(spec)
		pointer, err := vibejson.CompilePointer(owned)
		if err != nil {
			return nil, fmt.Errorf("%w: path %d: %v", ErrIndexDefinition, i, err)
		}
		for previous := range i {
			if out.Specs[previous] == owned {
				return nil, fmt.Errorf(
					"%w: duplicate path %q at columns %d and %d",
					ErrIndexDefinition, owned, previous, i,
				)
			}
		}
		out.Paths[i] = pointer
		out.Specs[i] = owned
	}
	return out, nil
}

func storeIndexTupleHash(seed maphash.Seed, values []vibejson.RawValue) (uint64, bool) {
	h := uint64(postFNVOffset)
	for _, value := range values {
		part, ok := storeIndexRawValueHash(seed, value)
		if !ok {
			return 0, false
		}
		// The delimiter makes tuple composition order-sensitive even when one
		// component's ending state resembles another component's start.
		h = (h ^ 0x9e3779b97f4a7c15) * postFNVPrime
		h = (h ^ part) * postFNVPrime
	}
	// The posting directory consumes low-order radix digits first. FNV is a
	// good streaming content fold but its adjacent small-integer buckets have
	// correlated low bits, which would create needlessly deep HAMT paths for
	// ordinary enum columns. A final bijective avalanche preserves equality
	// while spreading every input bit across the directory address.
	h ^= h >> 30
	h *= 0xbf58476d1ce4e5b9
	h ^= h >> 27
	h *= 0x94d049bb133111eb
	h ^= h >> 31
	return h, true
}

func storeIndexRawValueHash(seed maphash.Seed, v vibejson.RawValue) (uint64, bool) {
	switch v.Kind() {
	case document.Null:
		return postScalarBucket(postTagNull, 0), true
	case document.Bool:
		value, _ := v.Bool()
		tag := uint64(postTagFalse)
		if value {
			tag = postTagTrue
		}
		return postScalarBucket(tag, 0), true
	case document.Number:
		value, ok := v.Float64()
		if !ok {
			return postScalarBucket(postTagNumberWide, 0), true
		}
		if value == 0 {
			value = 0
		}
		return postScalarBucket(postTagNumber, math.Float64bits(value)), true
	case document.String:
		if content, clean := v.StringBytes(); clean {
			return postScalarBucket(postTagString, maphash.Bytes(seed, content)), true
		}
		var h maphash.Hash
		h.SetSeed(seed)
		it := vibejson.JSONStringByteIter{Raw: v.Bytes()[1 : len(v.Bytes())-1]}
		for {
			b, ok := it.Next()
			if !ok {
				return postScalarBucket(postTagString, h.Sum64()), true
			}
			_ = h.WriteByte(b)
		}
	default:
		return 0, false
	}
}

func storeIndexExtract(chunk *Chunk, slot int, exact *ExactIndex, out *[MaxIndexColumns]vibejson.RawValue) (uint64, bool) {
	if !ExtractIndexColumns(chunk, slot, exact, out) {
		return 0, false
	}
	return storeIndexTupleHash(exact.seed, out[:exact.N])
}

// ExtractIndexColumns fills out with the indexed column values for one live
// document slot and reports whether the row is indexable. It returns false when
// the slot is dead or any indexed path is absent or empty, which is how a
// document is correctly excluded from an exact index rather than stored under a
// null tuple. out is caller-owned scratch; the written values borrow the
// chunk's document bytes.
func ExtractIndexColumns(chunk *Chunk, slot int, exact *ExactIndex, out *[MaxIndexColumns]vibejson.RawValue) bool {
	if chunk == nil || chunk.Live&(uint64(1)<<uint(slot)) == 0 {
		return false
	}
	row := [1]int{int(chunk.Ord[slot])}
	for i := 0; i < int(exact.N); i++ {
		var one [1]vibejson.RawValue
		values, err := chunk.Docs.AppendPointerRows(one[:0], row[:], exact.Paths[i])
		if err != nil || len(values) != 1 || len(values[0].Bytes()) == 0 {
			return false
		}
		out[i] = values[0]
	}
	return true
}

// storeIndexUpdateSlot moves one stable slot between tuple postings. Equal
// fingerprints require no physical change: exact verification reads the new
// immutable chunk, and even the deliberately coarse wide-number bucket stays
// correct. This makes an update outside the indexed paths metadata-free.
func storeIndexUpdateSlot(root *storeIndexPostingNode, exact *ExactIndex, chunkID uint32, old, next *Chunk, slot int) *storeIndexPostingNode {
	var oldValues, nextValues [MaxIndexColumns]vibejson.RawValue
	oldHash, oldOK := storeIndexExtract(old, slot, exact, &oldValues)
	nextHash, nextOK := storeIndexExtract(next, slot, exact, &nextValues)
	if oldOK && nextOK && oldHash == nextHash {
		return root
	}
	bit := uint64(1) << uint(slot)
	if oldOK {
		root = storeIndexPostingSet(root, oldHash, chunkID, bit, false)
	}
	if nextOK {
		root = storeIndexPostingSet(root, nextHash, chunkID, bit, true)
	}
	return root
}

func storeIndexSetChunk(root *storeIndexPostingNode, exact *ExactIndex, chunkID uint32, chunk *Chunk, present bool) *storeIndexPostingNode {
	var storage [MaxChunkDocuments]storeIndexHashMask
	entries := storeIndexCollectChunk(storage[:0], exact, chunk)
	for _, entry := range entries {
		root = storeIndexPostingSetMask(root, entry.hash, chunkID, entry.mask, present)
	}
	return root
}

type storeIndexHashMask struct {
	hash uint64
	mask uint64
}

func storeIndexCollectChunk(dst []storeIndexHashMask, exact *ExactIndex, chunk *Chunk) []storeIndexHashMask {
	if chunk == nil {
		return dst
	}
	for live := chunk.Live; live != 0; live &= live - 1 {
		slot := bits.TrailingZeros64(live)
		var values [MaxIndexColumns]vibejson.RawValue
		hash, ok := storeIndexExtract(chunk, slot, exact, &values)
		if ok {
			dst = append(dst, storeIndexHashMask{hash: hash, mask: uint64(1) << uint(slot)})
		}
	}
	slices.SortFunc(dst, func(a, b storeIndexHashMask) int {
		switch {
		case a.hash < b.hash:
			return -1
		case a.hash > b.hash:
			return 1
		default:
			return 0
		}
	})
	out := dst[:0]
	for first := 0; first < len(dst); {
		last := first + 1
		mask := dst[first].mask
		for last < len(dst) && dst[last].hash == dst[first].hash {
			mask |= dst[last].mask
			last++
		}
		out = append(out, storeIndexHashMask{hash: dst[first].hash, mask: mask})
		first = last
	}
	return out
}

func storeIndexBuildBulk(pending map[uint64][]storeIndexChunkMask) *storeIndexPostingNode {
	leaves := make([]*storeIndexPostingLeaf, 0, len(pending))
	for hash, entries := range pending {
		leaves = append(leaves, &storeIndexPostingLeaf{
			hash:  hash,
			masks: storeIndexMasksFromSorted(entries),
		})
	}
	slices.SortFunc(leaves, func(a, b *storeIndexPostingLeaf) int {
		// The HAMT consumes low radix digits first. Bit-reversed order makes
		// every successive low-bit group contiguous for the one-allocation
		// recursive builder below.
		ah, bh := bits.Reverse64(a.hash), bits.Reverse64(b.hash)
		switch {
		case ah < bh:
			return -1
		case ah > bh:
			return 1
		default:
			return 0
		}
	})
	return storeIndexPostingBuild(leaves, 0)
}

func storeIndexApplyBulk(root *storeIndexPostingNode, pending map[uint64][]storeIndexChunkMask) *storeIndexPostingNode {
	for hash, entries := range pending {
		masks, _ := storeIndexPostingLookup(root, hash)
		masks = storeIndexMergeBulkMasks(masks, entries)
		root = storeIndexPostingInsert(root, 0, &storeIndexPostingLeaf{hash: hash, masks: masks})
	}
	return root
}

// storeIndexMergeBulkMasks merges an ordered backfill suffix into one current
// posting in a single forward pass. Backfill visits chunks in ascending order
// and storeIndexCollectChunk emits at most one word per tuple and chunk, so
// changes is strictly ordered. Preserving that invariant avoids both an
// O(N log N) resort and the repeated path copies of applying one chunk at a
// time. The fixed local buffer covers a complete ordinary collection chunk batch.
// Provenance: ALGO-ROARING-001.
func storeIndexMergeBulkMasks(current storeIndexMasks, changes []storeIndexChunkMask) storeIndexMasks {
	if len(changes) == 0 {
		return current
	}
	n := int(current.n) + int(current.wide.words) + len(changes)
	var local [MaxChunkDocuments]storeIndexChunkMask
	out := local[:0]
	if n > len(local) {
		out = make([]storeIndexChunkMask, 0, n)
	}
	it := current.iterator()
	leftChunk, leftMask, leftOK := it.next()
	right := 0
	for leftOK && right < len(changes) {
		switch {
		case leftChunk < changes[right].chunk:
			out = append(out, storeIndexChunkMask{chunk: leftChunk, mask: leftMask})
			leftChunk, leftMask, leftOK = it.next()
		case leftChunk > changes[right].chunk:
			out = append(out, changes[right])
			right++
		default:
			out = append(out, storeIndexChunkMask{
				chunk: leftChunk,
				mask:  leftMask | changes[right].mask,
			})
			leftChunk, leftMask, leftOK = it.next()
			right++
		}
	}
	for leftOK {
		out = append(out, storeIndexChunkMask{chunk: leftChunk, mask: leftMask})
		leftChunk, leftMask, leftOK = it.next()
	}
	out = append(out, changes[right:]...)
	return storeIndexMasksFromSorted(out)
}

func storeIndexScalarEqual(a, b vibejson.RawValue) bool {
	kind := a.Kind()
	if kind != b.Kind() {
		return false
	}
	switch kind {
	case document.Null:
		return true
	case document.Bool:
		av, _ := a.Bool()
		bv, _ := b.Bool()
		return av == bv
	case document.Number:
		return vibejson.JSONNumberEqual(a.Bytes(), b.Bytes())
	case document.String:
		aRaw, bRaw := a.Bytes(), b.Bytes()
		var af, bf uint8
		if bytes.IndexByte(aRaw, '\\') >= 0 {
			af = vibejson.TapeFlagEscaped
		}
		if bytes.IndexByte(bRaw, '\\') >= 0 {
			bf = vibejson.TapeFlagEscaped
		}
		return vibejson.RawJSONStringEqual(aRaw, af, bRaw, bf)
	default:
		return false
	}
}

func storeIndexSlotEqual(chunk *Chunk, slot int, exact *ExactIndex, want []vibejson.RawValue) bool {
	var got [MaxIndexColumns]vibejson.RawValue
	if !ExtractIndexColumns(chunk, slot, exact, &got) {
		return false
	}
	for i := 0; i < int(exact.N); i++ {
		if !storeIndexScalarEqual(got[i], want[i]) {
			return false
		}
	}
	return true
}

func (s Snapshot) exactIndex(name string) (storeIndexSnapshot, bool) {
	if s.state == nil {
		return storeIndexSnapshot{}, false
	}
	lo, hi := 0, len(s.state.secondary)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if s.state.secondary[mid].name < name {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo == len(s.state.secondary) || s.state.secondary[lo].name != name {
		return storeIndexSnapshot{}, false
	}
	return s.state.secondary[lo], true
}

func (s Snapshot) visitIndexMatches(name string, values []vibejson.Index, visit func(uint32, *Chunk, int)) error {
	index, ok := s.exactIndex(name)
	if !ok {
		return ErrIndexNotFound
	}
	if len(values) != int(index.exact.N) {
		return ErrIndexArity
	}
	var want [MaxIndexColumns]vibejson.RawValue
	for i := range values {
		root := values[i].Root()
		if _, scalar := postValueHash(root); !scalar {
			return ErrIndexScalar
		}
		want[i] = root.Raw()
	}
	hash, _ := storeIndexTupleHash(index.exact.seed, want[:index.exact.N])
	if index.state != IndexReady {
		s.state.Chunks.Each(func(chunkID uint32, chunk *Chunk) bool {
			for live := chunk.Live; live != 0; live &= live - 1 {
				slot := bits.TrailingZeros64(live)
				if storeIndexSlotEqual(chunk, slot, index.exact, want[:index.exact.N]) {
					visit(chunkID, chunk, slot)
				}
			}
			return true
		})
		return nil
	}
	storeIndexEachCandidate(index, hash, func(chunkID uint32, candidates uint64) {
		chunk := s.state.Chunks.Get(chunkID)
		if chunk == nil {
			return
		}
		for live := candidates & chunk.Live; live != 0; live &= live - 1 {
			slot := bits.TrailingZeros64(live)
			if storeIndexSlotEqual(chunk, slot, index.exact, want[:index.exact.N]) {
				visit(chunkID, chunk, slot)
			}
		}
	})
	return nil
}

// AppendIndexCandidateMasks appends the ready index's ordered stable-slot
// candidate words without exact scalar rechecks. A returned word is a sound
// superset: hash collisions and deliberately coarse numeric buckets can add
// rows, never remove a matching row. Callers must evaluate the predicate
// against every returned document before accepting it.
//
// This is the query-planner lane. It avoids decoding the same indexed JSON
// once during every leaf probe and again while evaluating the final predicate.
// A building index falls back to AppendIndexMasks and therefore remains exact.
// With sufficient dst capacity the operation allocates nothing.
func (s Snapshot) AppendIndexCandidateMasks(dst []Mask, name string, values ...vibejson.Index) ([]Mask, error) {
	index, ok := s.exactIndex(name)
	if !ok {
		return dst, ErrIndexNotFound
	}
	if len(values) != int(index.exact.N) {
		return dst, ErrIndexArity
	}
	var want [MaxIndexColumns]vibejson.RawValue
	for i := range values {
		root := values[i].Root()
		if _, scalar := postValueHash(root); !scalar {
			return dst, ErrIndexScalar
		}
		want[i] = root.Raw()
	}
	hash, _ := storeIndexTupleHash(index.exact.seed, want[:index.exact.N])
	if index.state != IndexReady {
		return s.AppendIndexMasks(dst, name, values...)
	}
	storeIndexEachCandidate(index, hash, func(chunkID uint32, candidates uint64) {
		chunk := s.state.Chunks.Get(chunkID)
		if chunk == nil {
			return
		}
		if candidates &= chunk.Live; candidates != 0 {
			dst = append(dst, Mask{Chunk: chunkID, Bits: candidates})
		}
	})
	return dst, nil
}

// storeIndexEachCandidate merges the immutable packed base with the
// path-copied mutation delta in chunk order. A dirty chunk is wholly shadowed:
// its complete current postings live in root, while every old base word for
// that chunk is skipped. This keeps writes O(one bounded chunk) and avoids a
// corpus rebuild on the first mutation.
func storeIndexEachCandidate(index storeIndexSnapshot, hash uint64, visit func(uint32, uint64)) {
	delta, _ := storeIndexPostingLookup(index.root, hash)
	deltaIterator := delta.iterator()
	deltaChunk, deltaMask, deltaOK := deltaIterator.next()
	// The lookup-hit flag is deliberately dropped: a miss yields the zero
	// iterator, whose next reports exhaustion on its first call, so priming the
	// merge below already folds "hash absent from the base" into baseOK. Keeping
	// the flag would only duplicate that state and let the two disagree.
	baseIterator, _ := index.base.iterator(hash)
	baseChunk, baseMask, baseOK := baseIterator.next()
	for baseOK {
		for deltaOK && deltaChunk < baseChunk {
			visit(deltaChunk, deltaMask)
			deltaChunk, deltaMask, deltaOK = deltaIterator.next()
		}
		if deltaOK && deltaChunk == baseChunk {
			visit(deltaChunk, deltaMask)
			deltaChunk, deltaMask, deltaOK = deltaIterator.next()
		} else if index.dirty.get(baseChunk) == 0 {
			visit(baseChunk, baseMask)
		}
		baseChunk, baseMask, baseOK = baseIterator.next()
	}
	for deltaOK {
		visit(deltaChunk, deltaMask)
		deltaChunk, deltaMask, deltaOK = deltaIterator.next()
	}
}

// AppendIndexRows appends immutable row addresses exactly matching the scalar
// values of a declared single-column or compound index. Each Index must have a
// scalar root. With sufficient dst capacity, the lookup and exact collision
// recheck allocate nothing. A Building index remains correct by scanning the
// snapshot; a Ready index visits only its stable-slot bitmap candidates.
func (s Snapshot) AppendIndexRows(dst []Location, name string, values ...vibejson.Index) ([]Location, error) {
	err := s.visitIndexMatches(name, values, func(chunkID uint32, _ *Chunk, slot int) {
		dst = append(dst, Location{Chunk: chunkID, Slot: uint8(slot)})
	})
	return dst, err
}

// AppendIndexMasks appends exact matches in their native chunk bitmap form.
// Adjacent matches in one chunk coalesce into one word. With sufficient dst
// capacity the complete lookup allocates nothing.
func (s Snapshot) AppendIndexMasks(dst []Mask, name string, values ...vibejson.Index) ([]Mask, error) {
	err := s.visitIndexMatches(name, values, func(chunkID uint32, _ *Chunk, slot int) {
		bit := uint64(1) << uint(slot)
		if len(dst) != 0 && dst[len(dst)-1].Chunk == chunkID {
			dst[len(dst)-1].Bits |= bit
		} else {
			dst = append(dst, Mask{Chunk: chunkID, Bits: bit})
		}
	})
	return dst, err
}

// AppendLiveMasks appends one stable-slot word per live chunk. It is the
// universe used to complement an exactly indexed predicate.
func (s Snapshot) AppendLiveMasks(dst []Mask) []Mask {
	if s.state == nil {
		return dst
	}
	s.state.Chunks.Each(func(chunkID uint32, chunk *Chunk) bool {
		dst = append(dst, Mask{Chunk: chunkID, Bits: chunk.Live})
		return true
	})
	return dst
}

// AppendIndexKeys is [Snapshot.AppendIndexRows] with key materialization.
func (s Snapshot) AppendIndexKeys(dst []string, name string, values ...vibejson.Index) ([]string, error) {
	err := s.visitIndexMatches(name, values, func(_ uint32, chunk *Chunk, slot int) {
		dst = append(dst, chunk.Key(slot))
	})
	return dst, err
}

// IndexKeys is the allocating convenience form of [Snapshot.AppendIndexKeys].
func (s Snapshot) IndexKeys(name string, values ...vibejson.Index) ([]string, error) {
	return s.AppendIndexKeys(nil, name, values...)
}

// AppendIndexRawKeys validates scalar JSON values and probes a declared index.
// Scalar needles use fixed stack tape storage; with sufficient dst capacity,
// the complete operation allocates nothing.
func (s Snapshot) AppendIndexRawKeys(dst []string, name string, values ...[]byte) ([]string, error) {
	if len(values) > MaxIndexColumns {
		return dst, ErrIndexArity
	}
	var indexes [MaxIndexColumns]vibejson.Index
	var entries [MaxIndexColumns]vibejson.IndexEntry
	for i, src := range values {
		// The one-entry storage is the scalar test, not just a buffer: a tape
		// wider than one entry is by definition not a lone scalar, so the
		// build reports ErrIndexFull exactly where a preceding
		// RequiredIndexEntries pass would have reported a count above one.
		// Dropping that pass halves the work on the accepting path — it is a
		// second complete validation of the same bytes — and the rejecting
		// path re-validates below only to recover the precise error, which is
		// the one case where its cost does not matter.
		index, err := vibejson.BuildIndex(src, entries[i:i+1:i+1])
		if err == document.ErrIndexFull {
			// Either a well-formed non-scalar or malformed input that ran out
			// of storage before the builder could diagnose it. Only a full
			// validation pass separates the two, and only this path pays it.
			if _, validateErr := vibejson.RequiredIndexEntries(src); validateErr != nil {
				return dst, validateErr
			}
			return dst, ErrIndexScalar
		}
		if err != nil {
			return dst, err
		}
		indexes[i] = index
	}
	return s.AppendIndexKeys(dst, name, indexes[:len(values)]...)
}

// IndexRawKeys is the allocating convenience form of
// [Snapshot.AppendIndexRawKeys].
func (s Snapshot) IndexRawKeys(name string, values ...[]byte) ([]string, error) {
	return s.AppendIndexRawKeys(nil, name, values...)
}

// AppendIndexKeys probes the current Snapshot; see
// [Snapshot.AppendIndexKeys].
func (c *Collection) AppendIndexKeys(dst []string, name string, values ...vibejson.Index) ([]string, error) {
	snap9, _ := c.Snapshot()
	return snap9.AppendIndexKeys(dst, name, values...)
}

// AppendIndexRows probes the current Snapshot; see
// [Snapshot.AppendIndexRows].
func (c *Collection) AppendIndexRows(dst []Location, name string, values ...vibejson.Index) ([]Location, error) {
	snap8, _ := c.Snapshot()
	return snap8.AppendIndexRows(dst, name, values...)
}

// AppendIndexMasks probes the current Snapshot; see
// [Snapshot.AppendIndexMasks].
func (c *Collection) AppendIndexMasks(dst []Mask, name string, values ...vibejson.Index) ([]Mask, error) {
	snap7, _ := c.Snapshot()
	return snap7.AppendIndexMasks(dst, name, values...)
}

// AppendIndexCandidateMasks probes the current Snapshot; see
// [Snapshot.AppendIndexCandidateMasks].
func (c *Collection) AppendIndexCandidateMasks(dst []Mask, name string, values ...vibejson.Index) ([]Mask, error) {
	snap6, _ := c.Snapshot()
	return snap6.AppendIndexCandidateMasks(dst, name, values...)
}

// IndexKeys probes the current Snapshot; see [Snapshot.IndexKeys].
func (c *Collection) IndexKeys(name string, values ...vibejson.Index) ([]string, error) {
	snap5, _ := c.Snapshot()
	return snap5.IndexKeys(name, values...)
}

// AppendIndexRawKeys probes the current Snapshot; see
// [Snapshot.AppendIndexRawKeys].
func (c *Collection) AppendIndexRawKeys(dst []string, name string, values ...[]byte) ([]string, error) {
	snap4, _ := c.Snapshot()
	return snap4.AppendIndexRawKeys(dst, name, values...)
}

// IndexRawKeys probes the current Snapshot; see [Snapshot.IndexRawKeys].
func (c *Collection) IndexRawKeys(name string, values ...[]byte) ([]string, error) {
	snap3, _ := c.Snapshot()
	return snap3.IndexRawKeys(name, values...)
}
