package store

import (
	"strconv"
	"testing"
)

var (
	storeIndexBenchmarkMasks storeIndexMasks
	storeIndexBenchmarkSum   uint64
)

func benchmarkStoreIndexInterleaved(words int) (storeIndexMasks, []storeIndexChunkMask) {
	current := make([]storeIndexChunkMask, words)
	changes := make([]storeIndexChunkMask, words)
	for i := range words {
		current[i] = storeIndexChunkMask{chunk: uint32(i * 2), mask: 1}
		changes[i] = storeIndexChunkMask{chunk: uint32(i*2 + 1), mask: 2}
	}
	return storeIndexMasksFromSorted(current), changes
}

func BenchmarkStoreIndexBulkMergeInterleaved(b *testing.B) {
	for _, words := range []int{64, 1024, 16384} {
		b.Run(strconv.Itoa(words), func(b *testing.B) {
			current, changes := benchmarkStoreIndexInterleaved(words)
			b.ReportAllocs()
			for b.Loop() {
				storeIndexBenchmarkMasks = storeIndexMergeBulkMasks(current, changes)
			}
		})
	}
}

func BenchmarkStoreIndexWideDeltaIteration(b *testing.B) {
	for _, words := range []int{64, 1024, 16384} {
		b.Run(strconv.Itoa(words), func(b *testing.B) {
			entries := make([]storeIndexChunkMask, words)
			for i := range entries {
				entries[i] = storeIndexChunkMask{
					chunk: uint32(i * 97),
					mask:  uint64(1) << uint(i&63),
				}
			}
			const hash = 0x123456789abcdef0
			index := storeIndexSnapshot{
				root: storeIndexPostingInsert(nil, 0, &storeIndexPostingLeaf{
					hash: hash, masks: storeIndexMasksFromSorted(entries),
				}),
			}
			b.ReportAllocs()
			for b.Loop() {
				sum := uint64(0)
				storeIndexEachCandidate(index, hash, func(chunk uint32, mask uint64) {
					sum += uint64(chunk) + mask
				})
				storeIndexBenchmarkSum = sum
			}
		})
	}
}
