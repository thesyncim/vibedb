//go:build go1.27 && !go1.28 && goexperiment.simd && amd64

package storeio

import "simd/archsimd"

type compactAlphabetAVX2Plan struct {
	indices0 [16]int8
	indices1 [16]int8
	factors  [8]uint16
}

var (
	compactAlphabetAVX2Plans = func() (plans [7][8]compactAlphabetAVX2Plan) {
		for width := 1; width <= 6; width++ {
			for bit := range 8 {
				p := &plans[width][bit]
				for lane := range 8 {
					at0 := bit + lane*width
					at1 := bit + (lane+8)*width
					p.indices0[lane*2], p.indices0[lane*2+1] = int8(at0/8), int8(at0/8+1)
					p.indices1[lane*2], p.indices1[lane*2+1] = int8(at1/8), int8(at1/8+1)
					p.factors[lane] = uint16(1) << uint(8-at0&7)
				}
			}
		}
		return
	}()
	compactAlphabetAVX2Pack = [16]int8{0, 2, 4, 6, 8, 10, 12, 14, -1, -1, -1, -1, -1, -1, -1, -1}
)

// compactScanAlphabetSIMD decodes 16 admitted packed symbols per AVX2
// iteration. Byte shuffles form eight little-endian words per half;
// multiplication aligns each field at bit eight, avoiding AVX-512-only
// variable word shifts. Its caller has already validated every code.
func compactScanAlphabetSIMD(dst, data []byte, bit, width, _ int, base byte) (int, bool) {
	if !archsimd.X86.AVX2() || width < 1 || width > 6 || len(dst) < 32 || bit < 0 {
		return 0, true
	}
	p := &compactAlphabetAVX2Plans[width][bit&7]
	indices0 := archsimd.LoadInt8x16Array(&p.indices0)
	indices1 := archsimd.LoadInt8x16Array(&p.indices1)
	factors := archsimd.LoadUint16x8Array(&p.factors)
	mask := archsimd.BroadcastUint16x8(uint16(1<<width - 1))
	first := archsimd.BroadcastUint16x8(uint16(base))
	pack := archsimd.LoadInt8x16Array(&compactAlphabetAVX2Pack)
	var zero archsimd.Uint8x16
	char, at := 0, bit/8
	for len(dst)-char >= 16 && len(data)-at >= 16 {
		packed := archsimd.LoadUint8x16Array((*[16]byte)(data[at:]))
		codes0 := packed.PermuteOrZero(indices0).ReshapeToUint16s().Mul(factors).ShiftAllRight(8).And(mask)
		codes1 := packed.PermuteOrZero(indices1).ReshapeToUint16s().Mul(factors).ShiftAllRight(8).And(mask)
		bytes0 := codes0.Add(first).ReshapeToUint8s().PermuteOrZero(pack)
		bytes1 := codes1.Add(first).ReshapeToUint8s().PermuteOrZero(pack)
		bytes0.Or(bytes1.ConcatShiftBytesRight(zero, 8)).StoreArray((*[16]byte)(dst[char:]))
		char += 16
		at += 2 * width
	}
	return char, true
}
