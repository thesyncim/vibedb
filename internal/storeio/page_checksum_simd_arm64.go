//go:build go1.27 && !go1.28 && goexperiment.simd && arm64 && darwin

package storeio

import (
	"hash/crc32"
	"simd/archsimd"
	"unsafe"
)

// The folding schedule and constants are generated from the CRC32C polynomial
// using the MIT/zlib-licensed fast-crc32 formulation:
// https://github.com/corsix/fast-crc32
var (
	crc32cInitialPMULL          = [2]uint64{0xffffffff, 0}
	crc32cFold144PMULL          = [2]uint64{0x7e908048, 0xc96cfdc0}
	crc32cFold64PMULL           = [2]uint64{0x740eef02, 0x9e4addf8}
	crc32cFold16PMULL           = [2]uint64{0xf20c0dfe, 0x493c7d27}
	crc32cFold32PMULL           = [2]uint64{0x3da6d0cb, 0xba4fc28e}
	crc32cReduce64QuotientPMULL = [2]uint64{0x4869ec38dea713f1, 0}
	crc32cReduce64PolyPMULL     = [2]uint64{0x105ec76f1, 0}
)

func pageChecksum(data []byte) uint32 {
	if archsimd.ARM64.PMULL() {
		if len(data) >= 1024 {
			return pageChecksumPMULL9(data)
		}
		if len(data) >= 256 {
			return pageChecksumPMULL4(data)
		}
	}
	return crc32.Checksum(data, pageChecksumTable)
}

// pageChecksumPMULL4 folds four independent 128-bit streams with PMULL. A
// second PMULL stage reduces the final residue; only a sub-64-byte tail uses
// the standard CRC32C updater. Everything is Go SIMD intrinsics and stack
// values: this package owns no assembly implementation.
func pageChecksumPMULL4(data []byte) uint32 {
	base := unsafe.SliceData(data)
	x0 := loadCRC32CBlock128(base, 0).
		Xor(archsimd.LoadUint64x2Array(&crc32cInitialPMULL))
	x1 := loadCRC32CBlock128(base, 16)
	x2 := loadCRC32CBlock128(base, 32)
	x3 := loadCRC32CBlock128(base, 48)

	fold := archsimd.LoadUint64x2Array(&crc32cFold64PMULL)
	i := 64
	for ; i+64 <= len(data); i += 64 {
		y0 := x0.CarrylessMultiplyEven(fold).Xor(loadCRC32CBlock128(base, i))
		x0 = x0.CarrylessMultiplyOdd(fold).Xor(y0)
		y1 := x1.CarrylessMultiplyEven(fold).Xor(loadCRC32CBlock128(base, i+16))
		x1 = x1.CarrylessMultiplyOdd(fold).Xor(y1)
		y2 := x2.CarrylessMultiplyEven(fold).Xor(loadCRC32CBlock128(base, i+32))
		x2 = x2.CarrylessMultiplyOdd(fold).Xor(y2)
		y3 := x3.CarrylessMultiplyEven(fold).Xor(loadCRC32CBlock128(base, i+48))
		x3 = x3.CarrylessMultiplyOdd(fold).Xor(y3)
	}

	fold = archsimd.LoadUint64x2Array(&crc32cFold16PMULL)
	y0 := x0.CarrylessMultiplyEven(fold).Xor(x1)
	x0 = x0.CarrylessMultiplyOdd(fold).Xor(y0)
	y2 := x2.CarrylessMultiplyEven(fold).Xor(x3)
	x2 = x2.CarrylessMultiplyOdd(fold).Xor(y2)

	fold = archsimd.LoadUint64x2Array(&crc32cFold32PMULL)
	y0 = x0.CarrylessMultiplyEven(fold).Xor(x2)
	x0 = x0.CarrylessMultiplyOdd(fold).Xor(y0)

	crc := reduceCRC32C128PMULL(x0)
	if i == len(data) {
		return ^crc
	}
	return crc32.Update(^crc, pageChecksumTable, data[i:])
}

// pageChecksumPMULL9 keeps nine independent polynomial streams in the arm64
// vector register file. Besides hiding PMULL latency, its 144-byte stride makes
// the vector body cover 4032 of every 4096 page bytes.
func pageChecksumPMULL9(data []byte) uint32 {
	base := unsafe.SliceData(data)
	x0 := loadCRC32CBlock128(base, 0).
		Xor(archsimd.LoadUint64x2Array(&crc32cInitialPMULL))
	x1 := loadCRC32CBlock128(base, 16)
	x2 := loadCRC32CBlock128(base, 32)
	x3 := loadCRC32CBlock128(base, 48)
	x4 := loadCRC32CBlock128(base, 64)
	x5 := loadCRC32CBlock128(base, 80)
	x6 := loadCRC32CBlock128(base, 96)
	x7 := loadCRC32CBlock128(base, 112)
	x8 := loadCRC32CBlock128(base, 128)

	fold := archsimd.LoadUint64x2Array(&crc32cFold144PMULL)
	i := 144
	for ; i+144 <= len(data); i += 144 {
		y0 := x0.CarrylessMultiplyEven(fold)
		x0 = x0.CarrylessMultiplyOdd(fold).Xor(y0).Xor(loadCRC32CBlock128(base, i))
		y1 := x1.CarrylessMultiplyEven(fold)
		x1 = x1.CarrylessMultiplyOdd(fold).Xor(y1).Xor(loadCRC32CBlock128(base, i+16))
		y2 := x2.CarrylessMultiplyEven(fold)
		x2 = x2.CarrylessMultiplyOdd(fold).Xor(y2).Xor(loadCRC32CBlock128(base, i+32))
		y3 := x3.CarrylessMultiplyEven(fold)
		x3 = x3.CarrylessMultiplyOdd(fold).Xor(y3).Xor(loadCRC32CBlock128(base, i+48))
		y4 := x4.CarrylessMultiplyEven(fold)
		x4 = x4.CarrylessMultiplyOdd(fold).Xor(y4).Xor(loadCRC32CBlock128(base, i+64))
		y5 := x5.CarrylessMultiplyEven(fold)
		x5 = x5.CarrylessMultiplyOdd(fold).Xor(y5).Xor(loadCRC32CBlock128(base, i+80))
		y6 := x6.CarrylessMultiplyEven(fold)
		x6 = x6.CarrylessMultiplyOdd(fold).Xor(y6).Xor(loadCRC32CBlock128(base, i+96))
		y7 := x7.CarrylessMultiplyEven(fold)
		x7 = x7.CarrylessMultiplyOdd(fold).Xor(y7).Xor(loadCRC32CBlock128(base, i+112))
		y8 := x8.CarrylessMultiplyEven(fold)
		x8 = x8.CarrylessMultiplyOdd(fold).Xor(y8).Xor(loadCRC32CBlock128(base, i+128))
	}

	fold = archsimd.LoadUint64x2Array(&crc32cFold16PMULL)
	y0 := x0.CarrylessMultiplyEven(fold)
	x0 = x0.CarrylessMultiplyOdd(fold).Xor(y0).Xor(x1)
	x1, x2, x3, x4, x5, x6, x7 = x2, x3, x4, x5, x6, x7, x8
	y0 = x0.CarrylessMultiplyEven(fold)
	x0 = x0.CarrylessMultiplyOdd(fold).Xor(y0).Xor(x1)
	y2 := x2.CarrylessMultiplyEven(fold)
	x2 = x2.CarrylessMultiplyOdd(fold).Xor(y2).Xor(x3)
	y4 := x4.CarrylessMultiplyEven(fold)
	x4 = x4.CarrylessMultiplyOdd(fold).Xor(y4).Xor(x5)
	y6 := x6.CarrylessMultiplyEven(fold)
	x6 = x6.CarrylessMultiplyOdd(fold).Xor(y6).Xor(x7)

	fold = archsimd.LoadUint64x2Array(&crc32cFold32PMULL)
	y0 = x0.CarrylessMultiplyEven(fold)
	x0 = x0.CarrylessMultiplyOdd(fold).Xor(y0).Xor(x2)
	y4 = x4.CarrylessMultiplyEven(fold)
	x4 = x4.CarrylessMultiplyOdd(fold).Xor(y4).Xor(x6)

	fold = archsimd.LoadUint64x2Array(&crc32cFold64PMULL)
	y0 = x0.CarrylessMultiplyEven(fold)
	x0 = x0.CarrylessMultiplyOdd(fold).Xor(y0).Xor(x4)

	crc := reduceCRC32C128PMULL(x0)
	if i == len(data) {
		return ^crc
	}
	return crc32.Update(^crc, pageChecksumTable, data[i:])
}

// loadCRC32CBlock128 loads one complete 16-byte window. The PMULL callers prove
// offset+16 <= len(data). ARM64 permits unaligned vector loads. The returned
// vector owns no pointer and the helper retains no storage.
func loadCRC32CBlock128(base *byte, offset int) archsimd.Uint64x2 {
	return archsimd.LoadUint64x2Array((*[2]uint64)(unsafe.Add(unsafe.Pointer(base), offset)))
}

func reduceCRC32C128PMULL(value archsimd.Uint64x2) uint32 {
	crc := reduceCRC32C64PMULL(0, value.GetElem(0))
	return reduceCRC32C64PMULL(crc, value.GetElem(1))
}

func reduceCRC32C64PMULL(crc uint32, value uint64) uint32 {
	var lane archsimd.Uint64x2
	lane = lane.SetElem(0, uint64(crc)^value)
	lane = lane.CarrylessMultiplyEven(archsimd.LoadUint64x2Array(&crc32cReduce64QuotientPMULL))
	lane = lane.CarrylessMultiplyEven(archsimd.LoadUint64x2Array(&crc32cReduce64PolyPMULL))
	return uint32(lane.GetElem(1))
}
