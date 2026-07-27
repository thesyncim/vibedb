package storeio

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
	"testing"
)

var (
	coldLeafExtentRunLabBenchmarkBytes    []byte
	coldLeafExtentRunLabBenchmarkChecksum uint32
	coldLeafExtentRunLabBenchmarkBool     bool
)

// The capacity experiment spends the 32 bytes after an eight-byte magic on
// four independent 64-bit Bloom lanes. Its remaining eight header bytes carry
// count/stash/heap/class framing after identity moves to the unified handle.
const coldLeafCapacityLabHeaderSize = 48

type coldLeafCapacityPlacementLab struct {
	hashes   [256]uint64
	owner    [240]int16
	slot     [256]uint8
	visited  [240]uint16
	epoch    uint16
	groups   int
	normal   int
	stash    int
	live     int
	assigned int
}

type coldLeafCapacity208ReadLab struct {
	page         []byte
	layout       orderedHashLeafLabLayout
	count        int
	stashCount   int
	slots        int
	normal       int
	groups       int
	controlStart int
	stashBloom   [4]uint64
}

type coldLeafCapacityLookupCost struct {
	rank int
	cost int
}

func BenchmarkColdLeafExtentRunLabSpace(b *testing.B) {
	const liveKeys = 218
	for _, leafBytes := range []int{4 << 10, 8 << 10, 16 << 10} {
		b.Run(fmt.Sprintf("%dKiB", leafBytes>>10), func(b *testing.B) {
			reportColdLeafExtentRunLabSpace(b, liveKeys, leafBytes)
		})
	}
	shapes := []struct {
		name, live, keyBytes, valueBytes int
	}{
		{name: 128, live: 128, keyBytes: 8, valueBytes: 8},
		{name: 218, live: 218, keyBytes: 8, valueBytes: 8},
		{name: 230, live: 230, keyBytes: 8, valueBytes: 48},
	}
	for _, shape := range shapes {
		leafBytes := OrderedHashLeafLabMetadataBytes(shape.live) +
			shape.live*(shape.keyBytes+shape.valueBytes)
		b.Run(fmt.Sprintf(
			"Ordered/%drows-%dBkey-%dBvalue",
			shape.live, shape.keyBytes, shape.valueBytes,
		), func(b *testing.B) {
			reportColdLeafExtentRunLabSpace(b, shape.live, leafBytes)
			dedicated := (leafBytes + ColdLeafExtentRunLabIOQuantum - 1) &
				^(ColdLeafExtentRunLabIOQuantum - 1)
			b.ReportMetric(
				float64(dedicated-leafBytes)/float64(shape.live),
				"dedicated-slack-B/key",
			)
			b.ReportMetric(float64(leafBytes), "exact-leaf-B")
		})
	}
}

func reportColdLeafExtentRunLabSpace(
	b *testing.B,
	liveKeys int,
	leafBytes int,
) {
	runBytes := ColdLeafExtentRunLabBytes(leafBytes)
	recordBytes := coldLeafExtentRunLabAlign(
		leafBytes + ColdLeafExtentRunLabRecordSize,
	)
	count := (runBytes - ColdLeafExtentRunLabHeaderSize -
		ColdLeafExtentRunLabTrailerSize) / recordBytes
	physicalSlack := runBytes - count*leafBytes
	cursor := ColdLeafExtentRunLabHeaderSize
	var windowBytes int64
	for range count {
		leafStart := cursor + ColdLeafExtentRunLabRecordSize
		windowStart := leafStart &^ (ColdLeafExtentRunLabIOQuantum - 1)
		windowEnd := (leafStart + leafBytes +
			ColdLeafExtentRunLabIOQuantum - 1) &
			^(ColdLeafExtentRunLabIOQuantum - 1)
		windowBytes += int64(windowEnd - windowStart)
		cursor += recordBytes
	}
	dedicatedWindow := (leafBytes + ColdLeafExtentRunLabIOQuantum - 1) &
		^(ColdLeafExtentRunLabIOQuantum - 1)
	b.ReportMetric(float64(runBytes)/float64(count*liveKeys), "physical-B/key")
	b.ReportMetric(float64(physicalSlack)/float64(count*liveKeys), "slack-B/key")
	b.ReportMetric(
		float64(windowBytes)/float64(count),
		"expected-window-B/random-miss",
	)
	b.ReportMetric(
		float64(windowBytes)/float64(count*dedicatedWindow),
		"random-miss-read-amplification/x",
	)
	b.ReportMetric(float64(count), "leaves/run")
}

func BenchmarkColdLeafExtentRunLabHotLookup(b *testing.B) {
	for _, leafBytes := range []int{4 << 10, 8 << 10, 16 << 10} {
		records := coldLeafExtentRunLabTestRecords(64, leafBytes)
		run := make([]byte, ColdLeafExtentRunLabBytes(leafBytes))
		refs := make([]ColdLeafExtentRunLabRef, len(records))
		refs, err := EncodeColdLeafExtentRunLab(
			run, coldLeafExtentRunLabTestBase, 1, records, refs,
		)
		if err != nil {
			b.Fatal(err)
		}
		ref := refs[len(refs)/2]
		direct := records[len(records)/2].Leaf
		b.Run(fmt.Sprintf("%dKiB/DirectSlice", leafBytes>>10), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				coldLeafExtentRunLabBenchmarkBytes = direct
				coldLeafExtentRunLabBenchmarkChecksum =
					uint32(direct[0]) + uint32(direct[len(direct)-1])
			}
		})
		b.Run(fmt.Sprintf("%dKiB/PackedResolve", leafBytes>>10), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				leaf, ok := ResolveColdLeafExtentRunLab(
					run, coldLeafExtentRunLabTestBase, ref,
				)
				coldLeafExtentRunLabBenchmarkBytes = leaf
				coldLeafExtentRunLabBenchmarkBool = ok
				coldLeafExtentRunLabBenchmarkChecksum =
					uint32(leaf[0]) + uint32(leaf[len(leaf)-1])
			}
		})
		b.Run(fmt.Sprintf("%dKiB/RoundedWindow", leafBytes>>10), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				window, inner, ok := ColdLeafExtentRunLabReadWindow(
					run, coldLeafExtentRunLabTestBase, ref,
					ColdLeafExtentRunLabIOQuantum,
				)
				leaf := window[inner : inner+uint32(ref.Length)]
				coldLeafExtentRunLabBenchmarkBytes = leaf
				coldLeafExtentRunLabBenchmarkBool = ok
				coldLeafExtentRunLabBenchmarkChecksum =
					uint32(leaf[0]) + uint32(leaf[len(leaf)-1])
			}
		})
	}
}

func BenchmarkColdLeafExtentRunLabEscapeUpdate(b *testing.B) {
	for _, leafBytes := range []int{4 << 10, 8 << 10, 16 << 10} {
		records := coldLeafExtentRunLabTestRecords(64, leafBytes)
		run := make([]byte, ColdLeafExtentRunLabBytes(leafBytes))
		refs := make([]ColdLeafExtentRunLabRef, len(records))
		refs, err := EncodeColdLeafExtentRunLab(
			run, coldLeafExtentRunLabTestBase, 1, records, refs,
		)
		if err != nil {
			b.Fatal(err)
		}
		ref := refs[len(refs)/2]
		direct := records[len(records)/2].Leaf
		dst := make([]byte, leafBytes)
		b.SetBytes(int64(leafBytes))
		b.Run(fmt.Sprintf("%dKiB/DedicatedCopySeal", leafBytes>>10), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				clear(dst)
				copy(dst, direct)
				coldLeafExtentRunLabBenchmarkChecksum = PageChecksum(dst)
			}
		})
		b.Run(fmt.Sprintf("%dKiB/PackedEscapeCopySeal", leafBytes>>10), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				hot, checksum, ok := EscapeColdLeafExtentRunLab(
					dst, run, coldLeafExtentRunLabTestBase, ref,
				)
				coldLeafExtentRunLabBenchmarkBytes = hot
				coldLeafExtentRunLabBenchmarkChecksum = checksum
				coldLeafExtentRunLabBenchmarkBool = ok
			}
		})
	}
}

func BenchmarkColdLeafExtentRunLabPackBuild(b *testing.B) {
	for _, leafBytes := range []int{4 << 10, 8 << 10, 16 << 10} {
		records := coldLeafExtentRunLabTestRecords(64, leafBytes)
		run := make([]byte, ColdLeafExtentRunLabBytes(leafBytes))
		refs := make([]ColdLeafExtentRunLabRef, len(records))
		b.Run(fmt.Sprintf("%dKiB", leafBytes>>10), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(run)))
			for range b.N {
				got, err := EncodeColdLeafExtentRunLab(
					run, coldLeafExtentRunLabTestBase, 1, records, refs,
				)
				if err != nil {
					b.Fatal(err)
				}
				coldLeafExtentRunLabBenchmarkChecksum =
					uint32(got[len(got)-1].Offset)
			}
		})
	}
}

func BenchmarkColdLeafCapacityPlacementLab(b *testing.B) {
	for _, shape := range []struct {
		name                string
		normal, stash, live int
	}{
		{name: "208slots-187live", normal: 192, stash: 16, live: 187},
		{name: "217slots-195live", normal: 192, stash: 25, live: 195},
		{name: "218slots-196live", normal: 192, stash: 26, live: 196},
		{name: "256slots-218live", normal: 240, stash: 16, live: 218},
		{name: "256slots-230live", normal: 240, stash: 16, live: 230},
	} {
		b.Run(shape.name, func(b *testing.B) {
			var placer coldLeafCapacityPlacementLab
			placer.normal, placer.stash, placer.live =
				shape.normal, shape.stash, shape.live
			placer.groups = shape.normal / orderedHashLeafLabGroupSize
			random := uint64(0x9e3779b97f4a7c15)
			totalStash, maximumStash, failures := uint64(0), 0, uint64(0)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				for rank := 0; rank < placer.live; rank++ {
					random += 0x9e3779b97f4a7c15
					value := random
					value = (value ^ value>>30) * 0xbf58476d1ce4e5b9
					value = (value ^ value>>27) * 0x94d049bb133111eb
					placer.hashes[rank] = value ^ value>>31
				}
				stash, ok := placer.placeAll()
				totalStash += uint64(stash)
				maximumStash = max(maximumStash, stash)
				if !ok {
					failures++
				}
			}
			b.StopTimer()
			b.ReportMetric(
				float64(totalStash)/float64(b.N), "mean-stash/trial",
			)
			b.ReportMetric(float64(maximumStash), "max-stash")
			b.ReportMetric(
				float64(failures)/float64(b.N), "split-probability",
			)
		})
	}
}

func BenchmarkColdLeafCapacityStableRestoreLab(b *testing.B) {
	for _, shape := range []struct {
		name                string
		slots, normal, live int
	}{
		{name: "217x195", slots: 217, normal: 192, live: 195},
		{name: "256x195-preserved-groups", slots: 256, normal: 192, live: 195},
		{name: "256x218", slots: 256, normal: 240, live: 218},
	} {
		b.Run(shape.name, func(b *testing.B) {
			var placer coldLeafCapacityPlacementLab
			placer.normal = shape.normal
			placer.stash = shape.slots - shape.normal
			placer.live = shape.live
			placer.groups = shape.normal / orderedHashLeafLabGroupSize
			random := uint64(0x243f6a8885a308d3)
			for rank := 0; rank < shape.live; rank++ {
				placer.hashes[rank] = coldLeafCapacitySplitMix(&random)
			}
			if _, ok := placer.placeAll(); !ok {
				b.Fatal("initial placement")
			}
			var occupied [256]bool
			for rank := 0; rank < shape.live; rank++ {
				occupied[placer.slot[rank]] = true
			}
			moved, splits := uint64(0), uint64(0)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				victim := int(coldLeafCapacitySplitMix(&random) %
					uint64(shape.live))
				oldSlot := int(placer.slot[victim])
				occupied[oldSlot] = false
				slot, ok := coldLeafCapacityRuntimeInsertionSlot(
					&occupied, shape.slots, shape.normal,
					placer.hashes[victim],
				)
				if !ok {
					splits++
					// Exact-slot restore is always legal. Keep the benchmark
					// progressing so it reports the unexpected failure.
					slot = oldSlot
				}
				occupied[slot] = true
				placer.slot[victim] = uint8(slot)
				if slot != oldSlot {
					moved++
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(moved)/float64(b.N), "moved-slot/op")
			b.ReportMetric(float64(splits)/float64(b.N), "split/op")
		})
	}
}

func BenchmarkColdLeafCapacityRuntimeReplacementLab(b *testing.B) {
	for _, shape := range []struct {
		name                string
		slots, normal, live int
	}{
		{name: "217x195", slots: 217, normal: 192, live: 195},
		{name: "256x195-preserved-groups", slots: 256, normal: 192, live: 195},
		{name: "256x218", slots: 256, normal: 240, live: 218},
	} {
		b.Run(shape.name, func(b *testing.B) {
			var placer coldLeafCapacityPlacementLab
			placer.normal = shape.normal
			placer.stash = shape.slots - shape.normal
			placer.live = shape.live
			placer.groups = shape.normal / orderedHashLeafLabGroupSize
			random := uint64(0x13198a2e03707344)
			totalObserved, splits, censored := uint64(0), uint64(0), uint64(0)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				for rank := 0; rank < shape.live; rank++ {
					placer.hashes[rank] = coldLeafCapacitySplitMix(&random)
				}
				if _, ok := placer.placeAll(); !ok {
					b.Fatal("initial placement")
				}
				var occupied [256]bool
				for rank := 0; rank < shape.live; rank++ {
					occupied[placer.slot[rank]] = true
				}
				for replacements := 0; replacements < 1_000_000; replacements++ {
					victim := int(coldLeafCapacitySplitMix(&random) %
						uint64(shape.live))
					oldSlot := int(placer.slot[victim])
					occupied[oldSlot] = false
					hash := coldLeafCapacitySplitMix(&random)
					slot, ok := coldLeafCapacityRuntimeInsertionSlot(
						&occupied, shape.slots, shape.normal, hash,
					)
					if !ok {
						occupied[oldSlot] = true
						totalObserved += uint64(replacements)
						splits++
						break
					}
					occupied[slot] = true
					placer.slot[victim] = uint8(slot)
					placer.hashes[victim] = hash
					if replacements == 1_000_000-1 {
						totalObserved += 1_000_000
						censored++
					}
				}
			}
			b.StopTimer()
			b.ReportMetric(
				float64(totalObserved)/float64(b.N),
				"observed-replacements/trial",
			)
			b.ReportMetric(float64(splits)/float64(b.N), "split/trial")
			b.ReportMetric(float64(censored)/float64(b.N), "censored/trial")
		})
	}
}

func coldLeafCapacityRuntimeInsertionSlot(
	occupied *[256]bool,
	slots, normal int,
	hash uint64,
) (int, bool) {
	groups := normal / orderedHashLeafLabGroupSize
	first := int(uint8(hash)) % groups
	second := int(uint8(hash>>8)) % groups
	if second == first {
		second++
		if second == groups {
			second = 0
		}
	}
	for groupIndex := 0; groupIndex < 2; groupIndex++ {
		group := first
		home := int(uint8(hash>>16) & 0xf)
		if groupIndex != 0 {
			group = second
			home = int(uint8(hash>>20) & 0xf)
		}
		for ordinal := 0; ordinal < orderedHashLeafLabGroupSize; ordinal++ {
			slot := group*orderedHashLeafLabGroupSize +
				(home+ordinal)&(orderedHashLeafLabGroupSize-1)
			if !occupied[slot] {
				return slot, true
			}
		}
	}
	for slot := normal; slot < slots; slot++ {
		if !occupied[slot] {
			return slot, true
		}
	}
	return 0, false
}

func coldLeafCapacitySplitMix(state *uint64) uint64 {
	*state += 0x9e3779b97f4a7c15
	value := *state
	value = (value ^ value>>30) * 0xbf58476d1ce4e5b9
	value = (value ^ value>>27) * 0x94d049bb133111eb
	return value ^ value>>31
}

func BenchmarkColdLeafCapacity208ReadLab(b *testing.B) {
	records, compact := buildColdLeafCapacity208ReadLab(b)
	records217, compact217 := buildColdLeafCapacity217ReadLab(b)
	currentRecords, current := openOrderedHashLeafLabBenchmark(b, 218, 8)
	compactHashes := make([]uint64, len(records))
	for rank := range records {
		compactHashes[rank] = orderedHashLeafLabKeyHash(
			orderedHashLeafLabTestSeed, records[rank].Key,
		)
	}
	currentHashes := make([]uint64, len(currentRecords))
	for rank := range currentRecords {
		currentHashes[rank] = orderedHashLeafLabKeyHash(
			orderedHashLeafLabTestSeed, currentRecords[rank].Key,
		)
	}
	hashes217 := make([]uint64, len(records217))
	for rank := range records217 {
		hashes217[rank] = orderedHashLeafLabKeyHash(
			orderedHashLeafLabTestSeed, records217[rank].Key,
		)
	}
	compactHit := records[len(records)/2].Key
	compactHash := orderedHashLeafLabKeyHash(
		orderedHashLeafLabTestSeed, compactHit,
	)
	currentHit := []byte("key-0109")
	currentHash := orderedHashLeafLabKeyHash(
		orderedHashLeafLabTestSeed, currentHit,
	)
	miss := []byte("missing!")
	missHash := orderedHashLeafLabKeyHash(orderedHashLeafLabTestSeed, miss)

	b.Run("PointHit/208x187", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			coldLeafExtentRunLabBenchmarkBytes,
				coldLeafExtentRunLabBenchmarkBool =
				compact.lookupHashed(compactHash, compactHit)
		}
	})
	b.Run("PointHit/256x218", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_, coldLeafExtentRunLabBenchmarkBytes, _,
				coldLeafExtentRunLabBenchmarkBool =
				current.LookupHashed(currentHash, currentHit)
		}
	})
	b.Run("PointMiss/208x187", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			coldLeafExtentRunLabBenchmarkBytes,
				coldLeafExtentRunLabBenchmarkBool =
				compact.lookupHashed(missHash, miss)
		}
	})
	b.Run("PointMiss/256x218", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_, coldLeafExtentRunLabBenchmarkBytes, _,
				coldLeafExtentRunLabBenchmarkBool =
				current.LookupHashed(missHash, miss)
		}
	})
	b.Run("PointMiss/217x195Bloom", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			coldLeafExtentRunLabBenchmarkBytes,
				coldLeafExtentRunLabBenchmarkBool =
				compact217.lookupHashed(missHash, miss)
		}
	})
	b.Run("PointHitAll/208x187", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			for rank := range records {
				coldLeafExtentRunLabBenchmarkBytes,
					coldLeafExtentRunLabBenchmarkBool =
					compact.lookupHashed(
						compactHashes[rank], records[rank].Key,
					)
			}
		}
		b.ReportMetric(
			float64(b.Elapsed().Nanoseconds())/
				float64(int64(b.N)*int64(len(records))),
			"ns/lookup",
		)
	})
	b.Run("PointHitAll/256x218", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			for rank := range currentRecords {
				_, coldLeafExtentRunLabBenchmarkBytes, _,
					coldLeafExtentRunLabBenchmarkBool =
					current.LookupHashed(
						currentHashes[rank], currentRecords[rank].Key,
					)
			}
		}
		b.ReportMetric(
			float64(b.Elapsed().Nanoseconds())/
				float64(int64(b.N)*int64(len(currentRecords))),
			"ns/lookup",
		)
	})
	b.Run("PointHitAll/217x195Bloom", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			for rank := range records217 {
				coldLeafExtentRunLabBenchmarkBytes,
					coldLeafExtentRunLabBenchmarkBool =
					compact217.lookupHashed(
						hashes217[rank], records217[rank].Key,
					)
			}
		}
		b.ReportMetric(
			float64(b.Elapsed().Nanoseconds())/
				float64(int64(b.N)*int64(len(records217))),
			"ns/lookup",
		)
	})
	b.Run("LexicalScan/208x187", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			coldLeafExtentRunLabBenchmarkChecksum = compact.scan()
		}
		b.ReportMetric(
			float64(b.Elapsed().Nanoseconds())/
				float64(int64(b.N)*int64(compact.count)),
			"ns/doc",
		)
	})
	b.Run("LexicalScan/256x218", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			coldLeafExtentRunLabBenchmarkChecksum =
				scanColdLeafCapacityCurrentLab(&current)
		}
		b.ReportMetric(
			float64(b.Elapsed().Nanoseconds())/
				float64(int64(b.N)*int64(current.Len())),
			"ns/doc",
		)
	})
}

func BenchmarkColdLeafCapacityWideReadLab(b *testing.B) {
	records, wide := buildColdLeafCapacityWideHighStashReadLab(b, 38)
	hashes := make([]uint64, len(records))
	normalRanks := make([]int, 0, len(records))
	stashCosts := make([]coldLeafCapacityLookupCost, 0, wide.stashCount)
	allCosts := coldLeafCapacityCosts(
		wide.page, wide.layout, wide.count, wide.controlStart,
		wide.slots, wide.normal, wide.groups, records,
	)
	for rank := range records {
		hashes[rank] = orderedHashLeafLabKeyHash(
			orderedHashLeafLabTestSeed, records[rank].Key,
		)
		if int(records[rank].Slot) < wide.normal {
			normalRanks = append(normalRanks, rank)
		}
	}
	for _, cost := range allCosts {
		if int(records[cost.rank].Slot) >= wide.normal {
			stashCosts = append(stashCosts, cost)
		}
	}
	filteredMiss, filteredHash := coldLeafCapacityFindMiss(b, wide, false)
	falsePositiveMiss, falsePositiveHash := coldLeafCapacityFindMiss(b, wide, true)

	b.Run("NormalHitAll/256x195-38stash", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			for _, rank := range normalRanks {
				coldLeafExtentRunLabBenchmarkBytes,
					coldLeafExtentRunLabBenchmarkBool =
					wide.lookupHashed(hashes[rank], records[rank].Key)
			}
		}
		b.ReportMetric(
			float64(b.Elapsed().Nanoseconds())/
				float64(int64(b.N)*int64(len(normalRanks))),
			"ns/lookup",
		)
	})
	for _, selected := range []struct {
		name string
		cost coldLeafCapacityLookupCost
	}{
		{name: "StashHit/ProxyP50", cost: stashCosts[len(stashCosts)/2]},
		{name: "StashHit/ProxyWorst", cost: stashCosts[len(stashCosts)-1]},
	} {
		rank := selected.cost.rank
		b.Run(selected.name, func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				coldLeafExtentRunLabBenchmarkBytes,
					coldLeafExtentRunLabBenchmarkBool =
					wide.lookupHashed(hashes[rank], records[rank].Key)
			}
		})
	}
	b.Run("PointMiss/FilterReject", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			coldLeafExtentRunLabBenchmarkBytes,
				coldLeafExtentRunLabBenchmarkBool =
				wide.lookupHashed(filteredHash, filteredMiss)
		}
	})
	b.Run("PointMiss/FilterFalsePositive", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			coldLeafExtentRunLabBenchmarkBytes,
				coldLeafExtentRunLabBenchmarkBool =
				wide.lookupHashed(falsePositiveHash, falsePositiveMiss)
		}
	})
	b.Run("FilterFalsePositiveRate", func(b *testing.B) {
		random, positives := uint64(0x6a09e667f3bcc909), uint64(0)
		b.ReportAllocs()
		for range b.N {
			hash := coldLeafCapacitySplitMix(&random)
			if coldLeafCapacityBloomMayContain(wide.stashBloom, hash) {
				positives++
			}
		}
		b.ReportMetric(
			100*float64(positives)/float64(b.N),
			"filter-positive-%",
		)
	})
	b.Run("LexicalScan/256x195-38stash", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			coldLeafExtentRunLabBenchmarkChecksum = wide.scan()
		}
		b.ReportMetric(
			float64(b.Elapsed().Nanoseconds())/
				float64(int64(b.N)*int64(wide.count)),
			"ns/doc",
		)
	})
}

func BenchmarkColdLeafCapacityPromotionEncodeLab(b *testing.B) {
	records, _ := buildColdLeafCapacity217ReadLab(b)
	dst := make([]byte, 2*ColdLeafExtentRunLabIOQuantum)
	b.ReportAllocs()
	b.SetBytes(int64(len(dst)))
	for range b.N {
		used, ok := encodeColdLeafCapacityPromotionLab(dst, records)
		if !ok {
			b.Fatal("promotion encode")
		}
		coldLeafExtentRunLabBenchmarkChecksum =
			uint32(used) + binary.LittleEndian.Uint32(dst[len(dst)-16:])
	}
	b.ReportMetric(float64(len(dst)), "durable-B/promotion")
}

func BenchmarkColdLeafCapacity208CollisionTailLab(b *testing.B) {
	records208, compact := buildColdLeafCapacity208ReadLab(b)
	records256, current := openOrderedHashLeafLabBenchmark(b, 218, 8)
	costs208 := coldLeafCapacityCosts(
		compact.page, compact.layout, compact.count,
		compact.controlStart, compact.slots, compact.normal,
		compact.groups, records208,
	)
	costs256 := coldLeafCapacityCosts(
		current.page, current.layout, current.Len(),
		OrderedHashLeafLabHeaderSize, OrderedHashLeafLabSlotCount,
		OrderedHashLeafLabNormalSlotCount,
		orderedHashLeafLabGroupCount, records256,
	)
	for _, quantile := range []struct {
		name        string
		numerator   int
		denominator int
	}{
		{name: "P50", numerator: 50, denominator: 100},
		{name: "P95", numerator: 95, denominator: 100},
		{name: "P99", numerator: 99, denominator: 100},
		{name: "Worst", numerator: 1, denominator: 1},
	} {
		rank208 := coldLeafCapacityCostQuantile(
			costs208, quantile.numerator, quantile.denominator,
		)
		key208 := records208[rank208].Key
		hash208 := orderedHashLeafLabKeyHash(
			orderedHashLeafLabTestSeed, key208,
		)
		b.Run(quantile.name+"/208x187", func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				coldLeafExtentRunLabBenchmarkBytes,
					coldLeafExtentRunLabBenchmarkBool =
					compact.lookupHashed(hash208, key208)
			}
		})

		rank256 := coldLeafCapacityCostQuantile(
			costs256, quantile.numerator, quantile.denominator,
		)
		key256 := records256[rank256].Key
		hash256 := orderedHashLeafLabKeyHash(
			orderedHashLeafLabTestSeed, key256,
		)
		b.Run(quantile.name+"/256x218", func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				_, coldLeafExtentRunLabBenchmarkBytes, _,
					coldLeafExtentRunLabBenchmarkBool =
					current.LookupHashed(hash256, key256)
			}
		})
	}
}

func coldLeafCapacityCosts(
	page []byte,
	layout orderedHashLeafLabLayout,
	live, controlStart, slots, normal, groups int,
	records []OrderedHashLeafLabRecord,
) []coldLeafCapacityLookupCost {
	costs := make([]coldLeafCapacityLookupCost, live)
	for target := 0; target < live; target++ {
		hash := orderedHashLeafLabKeyHash(
			orderedHashLeafLabTestSeed, records[target].Key,
		)
		want := orderedHashLeafLabControlLive | byte(hash>>57)
		first := int(uint8(hash)) % groups
		second := int(uint8(hash>>8)) % groups
		if second == first {
			second++
			if second == groups {
				second = 0
			}
		}
		cost, found := 0, false
		for groupIndex := 0; groupIndex < 2 && !found; groupIndex++ {
			group := first
			home := int(uint8(hash>>16) & 0xf)
			if groupIndex != 0 {
				group = second
				home = int(uint8(hash>>20) & 0xf)
			}
			for ordinal := 0; ordinal < orderedHashLeafLabGroupSize; ordinal++ {
				slot := group*orderedHashLeafLabGroupSize +
					(home+ordinal)&(orderedHashLeafLabGroupSize-1)
				cost++
				if page[controlStart+slot] != want {
					continue
				}
				rank := int(page[controlStart+slots+slot])
				cost += 2 + rank%layout.checkpointStride
				if rank == target {
					found = true
					break
				}
			}
		}
		for slot := normal; slot < slots && !found; slot++ {
			cost++
			if page[controlStart+slot] != want {
				continue
			}
			rank := int(page[controlStart+slots+slot])
			cost += 2 + rank%layout.checkpointStride
			if rank == target {
				found = true
			}
		}
		costs[target] = coldLeafCapacityLookupCost{
			rank: target, cost: cost,
		}
	}
	sort.Slice(costs, func(i, j int) bool {
		return costs[i].cost < costs[j].cost
	})
	return costs
}

func coldLeafCapacityCostQuantile(
	costs []coldLeafCapacityLookupCost,
	numerator, denominator int,
) int {
	index := (len(costs)*numerator + denominator - 1) / denominator
	if index > 0 {
		index--
	}
	return costs[index].rank
}

func (p *coldLeafCapacityPlacementLab) placeAll() (int, bool) {
	for rank := 0; rank < p.normal; rank++ {
		p.owner[rank] = -1
	}
	clear(p.visited[:p.normal])
	p.epoch = 0
	p.assigned = 0
	for rank := 0; rank < p.live; rank++ {
		p.epoch++
		if p.place(rank) {
			p.assigned++
			continue
		}
		stash := rank - p.assigned
		if stash >= p.stash {
			return stash + 1, false
		}
		p.slot[rank] = uint8(p.normal + stash)
	}
	return p.live - p.assigned, true
}

func (p *coldLeafCapacityPlacementLab) place(index int) bool {
	hash := p.hashes[index]
	first := int(uint8(hash)) % p.groups
	second := int(uint8(hash>>8)) % p.groups
	if second == first {
		second++
		if second == p.groups {
			second = 0
		}
	}
	for ordinal := 0; ordinal < 2*orderedHashLeafLabGroupSize; ordinal++ {
		group := first
		home := int(uint8(hash>>16) & 0xf)
		local := ordinal
		if ordinal >= orderedHashLeafLabGroupSize {
			group = second
			home = int(uint8(hash>>20) & 0xf)
			local -= orderedHashLeafLabGroupSize
		}
		slot := group*orderedHashLeafLabGroupSize +
			(home+local)&(orderedHashLeafLabGroupSize-1)
		if p.visited[slot] == p.epoch {
			continue
		}
		p.visited[slot] = p.epoch
		previous := p.owner[slot]
		if previous < 0 || p.place(int(previous)) {
			p.owner[slot] = int16(index)
			p.slot[index] = uint8(slot)
			return true
		}
	}
	return false
}

func buildColdLeafCapacity208ReadLab(
	b testing.TB,
) ([]OrderedHashLeafLabRecord, coldLeafCapacity208ReadLab) {
	b.Helper()
	return buildColdLeafCapacityReadLab(b, 208, 192, 187)
}

func buildColdLeafCapacity217ReadLab(
	b testing.TB,
) ([]OrderedHashLeafLabRecord, coldLeafCapacity208ReadLab) {
	b.Helper()
	return buildColdLeafCapacityReadLab(b, 217, 192, 195)
}

func buildColdLeafCapacityWideReadLab(
	b testing.TB,
) ([]OrderedHashLeafLabRecord, coldLeafCapacity208ReadLab) {
	b.Helper()
	return buildColdLeafCapacityReadLab(b, 256, 192, 195)
}

func buildColdLeafCapacityWideHighStashReadLab(
	b testing.TB,
	target int,
) ([]OrderedHashLeafLabRecord, coldLeafCapacity208ReadLab) {
	b.Helper()
	records, wide := buildColdLeafCapacityWideReadLab(b)
	for wide.stashCount < target {
		source, destination := -1, -1
		for slot := wide.normal - 1; slot >= 0; slot-- {
			if wide.page[wide.controlStart+slot] != 0 {
				source = slot
				break
			}
		}
		for slot := wide.normal; slot < wide.slots; slot++ {
			if wide.page[wide.controlStart+slot] == 0 {
				destination = slot
				break
			}
		}
		if source < 0 || destination < 0 {
			b.Fatal("cannot construct high-stash wide fixture")
		}
		rank := int(wide.page[wide.controlStart+wide.slots+source])
		wide.page[wide.controlStart+destination] =
			wide.page[wide.controlStart+source]
		wide.page[wide.controlStart+wide.slots+destination] = byte(rank)
		wide.page[wide.controlStart+source] = 0
		wide.page[wide.controlStart+wide.slots+source] =
			orderedHashLeafLabEmptyRank
		records[rank].Slot = uint8(destination)
		wide.stashCount++
	}
	wide.stashBloom = [4]uint64{}
	for rank := range records {
		if int(records[rank].Slot) < wide.normal {
			continue
		}
		hash := orderedHashLeafLabKeyHash(
			orderedHashLeafLabTestSeed, records[rank].Key,
		)
		coldLeafCapacityBloomAdd(&wide.stashBloom, hash)
	}
	coldLeafCapacityStoreBloom(wide.page, wide.stashBloom)
	return records, wide
}

func coldLeafCapacityFindMiss(
	b testing.TB,
	view coldLeafCapacity208ReadLab,
	filterPositive bool,
) ([]byte, uint64) {
	b.Helper()
	for candidate := 0; candidate < 10_000_000; candidate++ {
		key := []byte(fmt.Sprintf("miss-%08d", candidate))
		hash := orderedHashLeafLabKeyHash(orderedHashLeafLabTestSeed, key)
		if coldLeafCapacityBloomMayContain(
			view.stashBloom, hash,
		) != filterPositive {
			continue
		}
		if _, ok := view.lookupHashed(hash, key); !ok {
			return key, hash
		}
	}
	b.Fatal("cannot find requested filter miss shape")
	return nil, 0
}

func buildColdLeafCapacityReadLab(
	b testing.TB,
	slots, normal, live int,
) ([]OrderedHashLeafLabRecord, coldLeafCapacity208ReadLab) {
	b.Helper()
	stash := slots - normal
	records := orderedHashLeafLabRecords(live, 8)
	var placer coldLeafCapacityPlacementLab
	placer.normal, placer.stash, placer.live = normal, stash, live
	placer.groups = normal / orderedHashLeafLabGroupSize
	for rank := range records {
		placer.hashes[rank] = orderedHashLeafLabKeyHash(
			orderedHashLeafLabTestSeed, records[rank].Key,
		)
	}
	stashCount, ok := placer.placeAll()
	if !ok {
		b.Fatalf("%d-slot fixture placement split", slots)
	}

	layout := coldLeafCapacityReadLabLayout(slots, live)
	pageBytes := ColdLeafExtentRunLabIOQuantum
	if slots > 217 {
		pageBytes = 2 * ColdLeafExtentRunLabIOQuantum
	}
	page := make([]byte, pageBytes)
	for slot := 0; slot < slots; slot++ {
		page[coldLeafCapacityLabHeaderSize+slots+slot] =
			orderedHashLeafLabEmptyRank
	}
	cursor := layout.heapStart
	var stashBloom [4]uint64
	for rank := range records {
		record := &records[rank]
		record.Slot = placer.slot[rank]
		hash := placer.hashes[rank]
		slot := int(record.Slot)
		page[coldLeafCapacityLabHeaderSize+slot] =
			orderedHashLeafLabControlLive | byte(hash>>57)
		page[coldLeafCapacityLabHeaderSize+slots+slot] = byte(rank)
		if slot >= normal {
			coldLeafCapacityBloomAdd(&stashBloom, hash)
		}
		putOrderedHashLeafLabKeyLength(
			page, layout, rank, uint8(len(record.Key)),
		)
		putOrderedHashLeafLabBoundary(page, layout, rank, uint16(cursor))
		copy(page[cursor:], record.Key)
		cursor += len(record.Key)
		copy(page[cursor:], record.Value)
		cursor += len(record.Value)
	}
	putOrderedHashLeafLabBoundary(page, layout, live, uint16(cursor))
	buildOrderedHashLeafLabCheckpoints(page, layout, live+1)
	if cursor+OrderedHashLeafLabTrailerSize >
		pageBytes {
		b.Fatalf("%d-slot image uses %d bytes",
			slots, cursor+OrderedHashLeafLabTrailerSize)
	}
	coldLeafCapacityStoreBloom(page, stashBloom)
	return records, coldLeafCapacity208ReadLab{
		page: page, layout: layout, count: live, stashCount: stashCount,
		slots: slots, normal: normal,
		groups:       normal / orderedHashLeafLabGroupSize,
		controlStart: coldLeafCapacityLabHeaderSize,
		stashBloom:   stashBloom,
	}
}

func encodeColdLeafCapacityPromotionLab(
	page []byte,
	records []OrderedHashLeafLabRecord,
) (int, bool) {
	const slots, normal = 256, 192
	if len(page) != 2*ColdLeafExtentRunLabIOQuantum {
		return 0, false
	}
	layout := coldLeafCapacityReadLabLayout(slots, len(records))
	clear(page)
	copy(page[:8], "OHWIDE01")
	for slot := 0; slot < slots; slot++ {
		page[coldLeafCapacityLabHeaderSize+slots+slot] =
			orderedHashLeafLabEmptyRank
	}
	var filter [4]uint64
	cursor, stashCount := layout.heapStart, 0
	for rank := range records {
		record := &records[rank]
		slot := int(record.Slot)
		if slot >= slots || page[coldLeafCapacityLabHeaderSize+slot] != 0 {
			return 0, false
		}
		hash := orderedHashLeafLabKeyHash(
			orderedHashLeafLabTestSeed, record.Key,
		)
		page[coldLeafCapacityLabHeaderSize+slot] =
			orderedHashLeafLabControlLive | byte(hash>>57)
		page[coldLeafCapacityLabHeaderSize+slots+slot] = byte(rank)
		if slot >= normal {
			stashCount++
			coldLeafCapacityBloomAdd(&filter, hash)
		}
		putOrderedHashLeafLabKeyLength(
			page, layout, rank, uint8(len(record.Key)),
		)
		putOrderedHashLeafLabBoundary(page, layout, rank, uint16(cursor))
		copy(page[cursor:], record.Key)
		cursor += len(record.Key)
		copy(page[cursor:], record.Value)
		cursor += len(record.Value)
	}
	if cursor+OrderedHashLeafLabTrailerSize > len(page) {
		return 0, false
	}
	putOrderedHashLeafLabBoundary(page, layout, len(records), uint16(cursor))
	buildOrderedHashLeafLabCheckpoints(page, layout, len(records)+1)
	coldLeafCapacityStoreBloom(page, filter)
	binary.LittleEndian.PutUint16(page[40:42], uint16(len(records)))
	binary.LittleEndian.PutUint16(page[42:44], uint16(stashCount))
	binary.LittleEndian.PutUint16(page[44:46], uint16(cursor))
	binary.LittleEndian.PutUint16(page[46:48], slots)
	sealOrderedHashLeafLab(page)
	return cursor, true
}

func coldLeafCapacityReadLabLayout(slots, live int) orderedHashLeafLabLayout {
	boundaries := live + 1
	keyBytes := (live*7 + 7) / 8
	overflowBytes := (live + 7) / 8
	highBytes := (boundaries + orderedHashLeafLabEFLowerUniverse + 7) / 8
	checkpointStride := 2 * orderedHashLeafLabEFCheckpoint
	checkpointCount := (boundaries + checkpointStride - 1) /
		checkpointStride
	layout := orderedHashLeafLabLayout{
		keyLengthStart:   coldLeafCapacityLabHeaderSize + 2*slots,
		highBytes:        highBytes,
		checkpointCount:  checkpointCount,
		checkpointStride: checkpointStride,
	}
	layout.overflowStart = layout.keyLengthStart + keyBytes
	layout.lowStart = layout.overflowStart + overflowBytes
	layout.highStart = layout.lowStart + boundaries
	layout.checkpointStart = layout.highStart + highBytes
	layout.heapStart = layout.checkpointStart + 2*checkpointCount
	return layout
}

func (v *coldLeafCapacity208ReadLab) lookupHashed(
	hash uint64,
	key []byte,
) ([]byte, bool) {
	wantControl := orderedHashLeafLabControlLive | byte(hash>>57)
	first := int(uint8(hash)) % v.groups
	second := int(uint8(hash>>8)) % v.groups
	if second == first {
		second++
		if second == v.groups {
			second = 0
		}
	}
	for groupIndex := 0; groupIndex < 2; groupIndex++ {
		group := first
		home := int(uint8(hash>>16) & 0xf)
		if groupIndex != 0 {
			group = second
			home = int(uint8(hash>>20) & 0xf)
		}
		base := group * orderedHashLeafLabGroupSize
		for ordinal := 0; ordinal < orderedHashLeafLabGroupSize; ordinal++ {
			slot := base + (home+ordinal)&
				(orderedHashLeafLabGroupSize-1)
			if v.page[v.controlStart+slot] != wantControl {
				continue
			}
			if value, ok := v.lookupCandidate(slot, key); ok {
				return value, true
			}
		}
	}
	if v.stashCount == 0 {
		return nil, false
	}
	if !coldLeafCapacityBloomMayContain(v.stashBloom, hash) {
		return nil, false
	}
	for slot := v.normal; slot < v.slots; slot++ {
		if v.page[v.controlStart+slot] != wantControl {
			continue
		}
		if value, ok := v.lookupCandidate(slot, key); ok {
			return value, true
		}
	}
	return nil, false
}

func coldLeafCapacityBloomAdd(filter *[4]uint64, hash uint64) {
	filter[0] |= uint64(1) << uint(hash>>32&63)
	filter[1] |= uint64(1) << uint(hash>>38&63)
	filter[2] |= uint64(1) << uint(hash>>44&63)
	filter[3] |= uint64(1) << uint(hash>>50&63)
}

func coldLeafCapacityBloomMayContain(filter [4]uint64, hash uint64) bool {
	return filter[0]&(uint64(1)<<uint(hash>>32&63)) != 0 &&
		filter[1]&(uint64(1)<<uint(hash>>38&63)) != 0 &&
		filter[2]&(uint64(1)<<uint(hash>>44&63)) != 0 &&
		filter[3]&(uint64(1)<<uint(hash>>50&63)) != 0
}

func coldLeafCapacityStoreBloom(page []byte, filter [4]uint64) {
	for lane := range filter {
		binary.LittleEndian.PutUint64(page[8+lane*8:], filter[lane])
	}
}

func (v *coldLeafCapacity208ReadLab) lookupCandidate(
	slot int,
	key []byte,
) ([]byte, bool) {
	rank := int(v.page[v.controlStart+v.slots+slot])
	if rank >= v.count ||
		orderedHashLeafLabKeyLength(v.page, v.layout, rank) != len(key) {
		return nil, false
	}
	start, end, ok := v.recordBounds(rank)
	if !ok || !bytes.Equal(v.page[start:start+len(key)], key) {
		return nil, false
	}
	return v.page[start+len(key) : end : end], true
}

func (v *coldLeafCapacity208ReadLab) boundary(index int) (uint16, bool) {
	if index < 0 || index > v.count {
		return 0, false
	}
	checkpoint := index / v.layout.checkpointStride
	baseIndex := checkpoint * v.layout.checkpointStride
	position := int(binary.LittleEndian.Uint16(
		v.page[v.layout.checkpointStart+checkpoint*2:],
	))
	if !orderedHashLeafLabHighBit(v.page, v.layout, position) {
		return 0, false
	}
	for current := baseIndex; current < index; current++ {
		next, ok := orderedHashLeafLabNextHighBit(
			v.page, v.layout, position+1,
		)
		if !ok {
			return 0, false
		}
		position = next
	}
	high := position - index
	if high < 0 || high >= orderedHashLeafLabEFLowerUniverse {
		return 0, false
	}
	return uint16(high<<orderedHashLeafLabEFLowerBits) |
		uint16(v.page[v.layout.lowStart+index]), true
}

func (v *coldLeafCapacity208ReadLab) recordBounds(
	rank int,
) (int, int, bool) {
	if rank < 0 || rank >= v.count {
		return 0, 0, false
	}
	offset, ok := v.boundary(rank)
	if !ok {
		return 0, 0, false
	}
	position := int(offset>>orderedHashLeafLabEFLowerBits) + rank
	nextPosition, ok := orderedHashLeafLabNextHighBit(
		v.page, v.layout, position+1,
	)
	if !ok {
		return 0, 0, false
	}
	start := int(offset)
	nextRank := rank + 1
	end := int(uint16(
		(nextPosition-nextRank)<<orderedHashLeafLabEFLowerBits,
	) | uint16(v.page[v.layout.lowStart+nextRank]))
	return start, end, end > start
}

func (v *coldLeafCapacity208ReadLab) scan() uint32 {
	offset, ok := v.boundary(0)
	if !ok {
		return 0
	}
	position := int(offset >> orderedHashLeafLabEFLowerBits)
	checksum := uint32(0)
	for rank := 0; rank < v.count; rank++ {
		nextPosition, found := orderedHashLeafLabNextHighBit(
			v.page, v.layout, position+1,
		)
		if !found {
			return 0
		}
		nextRank := rank + 1
		end := int(uint16(
			(nextPosition-nextRank)<<orderedHashLeafLabEFLowerBits,
		) | uint16(v.page[v.layout.lowStart+nextRank]))
		keyLength := orderedHashLeafLabKeyLength(
			v.page, v.layout, rank,
		)
		start := int(offset)
		checksum += uint32(v.page[start]) +
			uint32(v.page[start+keyLength])
		offset = uint16(end)
		position = nextPosition
	}
	return checksum
}

func scanColdLeafCapacityCurrentLab(v *OrderedHashLeafLabView) uint32 {
	offset, ok := v.boundary(0)
	if !ok {
		return 0
	}
	position := int(offset >> orderedHashLeafLabEFLowerBits)
	checksum := uint32(0)
	for rank := 0; rank < int(v.count); rank++ {
		nextPosition, found := orderedHashLeafLabNextHighBit(
			v.page, v.layout, position+1,
		)
		if !found {
			return 0
		}
		nextRank := rank + 1
		end := int(uint16(
			(nextPosition-nextRank)<<orderedHashLeafLabEFLowerBits,
		) | uint16(v.page[v.layout.lowStart+nextRank]))
		keyLength := orderedHashLeafLabKeyLength(
			v.page, v.layout, rank,
		)
		start := int(offset)
		checksum += uint32(v.page[start]) +
			uint32(v.page[start+keyLength])
		offset = uint16(end)
		position = nextPosition
	}
	return checksum
}
