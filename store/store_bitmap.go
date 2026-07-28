package store

import (
	"math"
	"math/bits"

	"github.com/thesyncim/vibedb/internal/bitset"
	"github.com/thesyncim/vibejson"
)

// Collection bitmap workspaces use one uint64 per logical micro-page and one
// bit
// per stable slot. Dense words are the execution form for repeated Boolean
// plans: their page id is the slice index, so native SIMD can combine them
// without decoding sparse (page, mask) pairs. Sparse Mask remains the
// better interchange form when only a few pages match.

// BitmapWords returns the number of uint64 words required to address this
// snapshot's logical page high-water mark. Empty historical page ids occupy a
// zero word but require no document or index page to remain resident. It
// panics before integer wrap if the span cannot fit a slice on this platform.
func (s Snapshot) BitmapWords() int {
	if s.state == nil {
		return 0
	}
	if uint64(s.state.Chunks.Count) > uint64(math.MaxInt) {
		panic("vibejson: collection bitmap exceeds addressable slice length")
	}
	return int(s.state.Chunks.Count)
}

func appendStoreBitmapWords(dst []uint64, n int) ([]uint64, []uint64) {
	mark := len(dst)
	if n <= cap(dst)-mark {
		dst = dst[:mark+n]
	} else {
		dst = append(dst, make([]uint64, n)...)
	}
	words := dst[mark:]
	clear(words)
	return dst, words
}

// AppendIndexBitmap appends the dense, exact stable-slot bitmap for one ready
// or building declared index lookup. The appended length is BitmapWords;
// given that much spare capacity, the complete operation allocates nothing.
// A building index remains exact through its ordinary scan fallback.
func (s Snapshot) AppendIndexBitmap(dst []uint64, name string, values ...vibejson.Index) ([]uint64, error) {
	out, words := appendStoreBitmapWords(dst, s.BitmapWords())
	err := s.visitIndexMatches(name, values, func(chunkID uint32, _ *Chunk, slot int) {
		words[chunkID] |= uint64(1) << uint(slot)
	})
	if err != nil {
		return dst, err
	}
	return out, nil
}

// AppendLiveBitmap appends the snapshot universe in dense page-word form.
// AND-NOT against this universe implements exact NOT without inventing rows in
// empty pages or dead stable slots.
func (s Snapshot) AppendLiveBitmap(dst []uint64) []uint64 {
	out, words := appendStoreBitmapWords(dst, s.BitmapWords())
	if s.state != nil {
		s.state.Chunks.Each(func(chunkID uint32, chunk *Chunk) bool {
			words[chunkID] = chunk.Live
			return true
		})
	}
	return out
}

// AppendBitmapRows decodes a dense bitmap into ordered immutable row
// addresses, masking caller-supplied words against this snapshot's live slots.
// With sufficient destination capacity it allocates nothing.
func (s Snapshot) AppendBitmapRows(dst []Location, words []uint64) []Location {
	if s.state == nil {
		return dst
	}
	n := min(len(words), s.BitmapWords())
	for chunkID := range n {
		chunk := s.state.Chunks.Get(uint32(chunkID))
		if chunk == nil {
			continue
		}
		for live := words[chunkID] & chunk.Live; live != 0; live &= live - 1 {
			dst = append(dst, Location{Chunk: uint32(chunkID), Slot: uint8(bits.TrailingZeros64(live))})
		}
	}
	return dst
}

// AppendBitmapKeys is AppendBitmapRows with ordered key materialization.
func (s Snapshot) AppendBitmapKeys(dst []string, words []uint64) []string {
	if s.state == nil {
		return dst
	}
	n := min(len(words), s.BitmapWords())
	for chunkID := range n {
		chunk := s.state.Chunks.Get(uint32(chunkID))
		if chunk == nil {
			continue
		}
		for live := words[chunkID] & chunk.Live; live != 0; live &= live - 1 {
			slot := bits.TrailingZeros64(live)
			dst = append(dst, chunk.Key(slot))
		}
	}
	return dst
}

// AppendBitmapAnd appends a & b. The shorter input fixes the result
// length. dst may be exactly a[:0] or b[:0] for in-place execution; other
// writable overlap is unsupported.
func AppendBitmapAnd(dst, a, b []uint64) []uint64 { return bitset.And(dst, a, b) }

// AppendBitmapAnd3 appends the fused a & b & c result. Fusion avoids an
// intermediate bitmap pass and supports exact in-place execution with any
// input; other writable overlap is unsupported.
func AppendBitmapAnd3(dst, a, b, c []uint64) []uint64 {
	return bitset.And3(dst, a, b, c)
}

// AppendBitmapOr appends a | b, treating absent words as zero. Exact
// in-place execution with either input is supported; other overlap is not.
func AppendBitmapOr(dst, a, b []uint64) []uint64 { return bitset.Or(dst, a, b) }

// AppendBitmapAndNot appends a &^ b, treating absent b words as zero.
// Exact in-place execution with either input is supported; other overlap is
// not.
func AppendBitmapAndNot(dst, a, b []uint64) []uint64 { return bitset.AndNot(dst, a, b) }
