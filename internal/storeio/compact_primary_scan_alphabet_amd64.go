//go:build amd64

package storeio

import xsyscpu "golang.org/x/sys/cpu"

// compactScanAlphabetSIMD expands the dominant admitted five-bit contiguous
// alphabet with BMI2. PDEP deposits forty packed source bits directly into the
// low five bits of eight byte lanes, avoiding the scalar mask/shift network.
// The caller keeps the bounded scalar tail and has already admitted every code.
func compactScanAlphabetSIMD(dst, data []byte, bit, width, _ int, base byte) (int, bool) {
	if !xsyscpu.X86.HasBMI2 || width != 5 || len(dst) < 32 || bit < 0 || bit >= len(data)*8 {
		return 0, true
	}
	return compactScanAlphabet5BMI2(&dst[0], len(dst), &data[0], len(data), bit, base), true
}

//go:noescape
func compactScanAlphabet5BMI2(dst *byte, dstLen int, data *byte, dataLen, bit int, base byte) int
