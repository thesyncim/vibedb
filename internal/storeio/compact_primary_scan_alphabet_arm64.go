//go:build go1.27 && !go1.28 && goexperiment.simd && arm64 && darwin

package storeio

import "simd/archsimd"

type compactAlphabetShuffle struct {
	lo, hi      [16]byte
	right, left [16]int8
}

var compactAlphabetShuffles = func() (plans [7][8]compactAlphabetShuffle) {
	for width := 1; width <= 6; width++ {
		for bit := range 8 {
			p := &plans[width][bit]
			for lane := range 16 {
				at := bit + lane*width
				p.lo[lane], p.hi[lane] = byte(at/8), byte(at/8+1)
				p.right[lane], p.left[lane] = -int8(at&7), int8(8-at&7)
			}
		}
	}
	return
}()

// compactScanAlphabetSIMD decodes complete 16-symbol batches. The caller keeps
// the scalar tail; all loads are bounded by the source slice's length.
func compactScanAlphabetSIMD(dst, data []byte, bit, width, cardinality int, base byte) (int, bool) {
	if width < 1 || width > 6 || len(dst) < 32 || bit < 0 {
		return 0, true
	}
	p := &compactAlphabetShuffles[width][bit&7]
	lo := archsimd.LoadUint8x16Array(&p.lo)
	hi := archsimd.LoadUint8x16Array(&p.hi)
	right := archsimd.LoadInt8x16Array(&p.right)
	left := archsimd.LoadInt8x16Array(&p.left)
	mask := archsimd.BroadcastUint8x16(byte(1<<width - 1))
	first := archsimd.BroadcastUint8x16(base)
	char, at := 0, bit/8
	for len(dst)-char >= 16 && len(data)-at >= 16 {
		packed := archsimd.LoadUint8x16Array((*[16]byte)(data[at:]))
		codes := packed.LookupOrZero(lo).Shift(right).Or(packed.LookupOrZero(hi).Shift(left)).And(mask)
		if int(codes.ReduceMax()) >= cardinality {
			return char, false
		}
		codes.Add(first).StoreArray((*[16]byte)(dst[char:]))
		char += 16
		at += 2 * width
	}
	return char, true
}
