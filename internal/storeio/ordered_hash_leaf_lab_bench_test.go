package storeio

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
)

var (
	orderedHashLeafLabBenchmarkSlot  uint8
	orderedHashLeafLabBenchmarkBytes []byte
	orderedHashLeafLabBenchmarkInt   int
	orderedHashLeafLabBenchmarkBool  bool
)

func reportOrderedHashLeafLabMutationIO(
	b *testing.B, extent, copied, checksummed int,
) {
	b.Cleanup(func() {
		b.ReportMetric(float64(extent), "durable_extent_B/op")
		b.ReportMetric(float64(copied), "copied_B/op")
		b.ReportMetric(float64(checksummed), "checksummed_B/op")
	})
}

func orderedHashLeafLabFullChecksumBytes(page []byte) int {
	// Two data regions plus the eight-byte checksum table.
	return orderedHashLeafLabTrailerStart(page) + 8
}

func orderedHashLeafLabChangedChecksumBytes(
	page []byte, changedStart, changedEnd int,
) int {
	checksumSplit := orderedHashLeafLabChecksumSplit(page)
	bytes := 8
	if changedStart < checksumSplit && changedEnd > 0 {
		bytes += checksumSplit
	}
	if changedStart < orderedHashLeafLabTrailerStart(page) &&
		changedEnd > checksumSplit {
		bytes += orderedHashLeafLabTrailerStart(page) - checksumSplit
	}
	return bytes
}

func openOrderedHashLeafLabBenchmark(
	b testing.TB, count, valueLength int,
) ([]OrderedHashLeafLabRecord, OrderedHashLeafLabView) {
	b.Helper()
	records := orderedHashLeafLabRecords(count, valueLength)
	return records, openOrderedHashLeafLabTest(b, records)
}

func orderedHashLeafLabBenchmarkFalseTagMiss(
	view *OrderedHashLeafLabView,
) ([]byte, uint64, int) {
	var best []byte
	var bestHash uint64
	bestMatches := -1
	for candidate := 0; candidate < 100000; candidate++ {
		key := []byte(fmt.Sprintf("m%07d", candidate))
		hash := KeyHashBytes(orderedHashLeafLabTestSeed, key)
		want := orderedHashLeafLabControlLive | byte(hash>>57)
		matches := 0
		first, second := orderedHashLeafLabGroups(hash)
		firstHome := uint8(hash>>16) & 0x0f
		secondHome := uint8(hash>>20) & 0x0f
		for groupIndex := 0; groupIndex < 2; groupIndex++ {
			group, home := first, firstHome
			if groupIndex != 0 {
				group, home = second, secondHome
			}
			base := group * orderedHashLeafLabGroupSize
			for ordinal := uint8(0); ordinal < orderedHashLeafLabGroupSize; ordinal++ {
				slot := base + (home+ordinal)&(orderedHashLeafLabGroupSize-1)
				if view.page[orderedHashLeafLabControlStart+int(slot)] != want {
					continue
				}
				rank := int(view.page[orderedHashLeafLabRankStart+int(slot)])
				if rank < view.Len() &&
					orderedHashLeafLabKeyLength(view.page, view.layout, rank) == len(key) {
					matches++
				}
			}
		}
		if matches > bestMatches {
			best = key
			bestHash = hash
			bestMatches = matches
		}
	}
	return best, bestHash, bestMatches
}

func BenchmarkOrderedHashLeafLabPointLookup(b *testing.B) {
	records, view := openOrderedHashLeafLabBenchmark(b, 230, 32)
	hit := records[len(records)/2].Key
	hitSlot := records[len(records)/2].Slot
	hitHash := KeyHashBytes(orderedHashLeafLabTestSeed, hit)
	miss := []byte("missing-key")
	missHash := KeyHashBytes(orderedHashLeafLabTestSeed, miss)
	falseTagMiss, falseTagHash, falseTagMatches :=
		orderedHashLeafLabBenchmarkFalseTagMiss(&view)

	b.Run("HashOnly", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			orderedHashLeafLabBenchmarkInt =
				int(orderedHashLeafLabKeyHash(orderedHashLeafLabTestSeed, hit))
		}
	})
	b.Run("Hit/Prehashed", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			orderedHashLeafLabBenchmarkSlot,
				orderedHashLeafLabBenchmarkBytes, _,
				orderedHashLeafLabBenchmarkBool = view.LookupHashed(hitHash, hit)
		}
	})
	b.Run("Hit/HashIncluded", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			orderedHashLeafLabBenchmarkSlot,
				orderedHashLeafLabBenchmarkBytes, _,
				orderedHashLeafLabBenchmarkBool = view.Lookup(hit)
		}
	})
	b.Run("Hit/StableSlotHint", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			orderedHashLeafLabBenchmarkBytes, _,
				orderedHashLeafLabBenchmarkBool = view.LookupSlot(hitSlot, hit)
		}
	})
	b.Run("Miss/Prehashed", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			orderedHashLeafLabBenchmarkSlot,
				orderedHashLeafLabBenchmarkBytes, _,
				orderedHashLeafLabBenchmarkBool = view.LookupHashed(missHash, miss)
		}
	})
	b.Run("Miss/HashIncluded", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			orderedHashLeafLabBenchmarkSlot,
				orderedHashLeafLabBenchmarkBytes, _,
				orderedHashLeafLabBenchmarkBool = view.Lookup(miss)
		}
	})
	b.Run(fmt.Sprintf("Miss/ExactConfirmation%d", falseTagMatches), func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			orderedHashLeafLabBenchmarkSlot,
				orderedHashLeafLabBenchmarkBytes, _,
				orderedHashLeafLabBenchmarkBool =
				view.LookupHashed(falseTagHash, falseTagMiss)
		}
	})
}

func BenchmarkOrderedHashLeafLabLexicalIteration(b *testing.B) {
	records, view := openOrderedHashLeafLabBenchmark(b, 230, 32)
	b.ReportAllocs()
	b.SetBytes(int64(len(records)))
	b.ResetTimer()
	for range b.N {
		iterator := view.AllRows()
		for {
			key, value, _, ok := iterator.NextBorrowed()
			if !ok {
				break
			}
			orderedHashLeafLabBenchmarkBytes = key
			orderedHashLeafLabBenchmarkBytes = value
		}
	}
	b.StopTimer()
	b.ReportMetric(
		float64(b.Elapsed().Nanoseconds())/float64(int64(b.N)*int64(len(records))),
		"ns/doc",
	)
}

func BenchmarkOrderedHashLeafLabLowerBoundAndRange(b *testing.B) {
	_, view := openOrderedHashLeafLabBenchmark(b, 230, 32)
	lower := []byte("key-0100")
	upper := []byte("key-0200")
	b.Run("LowerBound", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			orderedHashLeafLabBenchmarkInt = view.LowerBound(lower)
		}
	})
	b.Run("Range100", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			iterator := view.Range(lower, upper)
			for {
				_, value, _, ok := iterator.NextBorrowed()
				if !ok {
					break
				}
				orderedHashLeafLabBenchmarkBytes = value
			}
		}
	})
}

func BenchmarkOrderedHashLeafLabCanonicalMutation(b *testing.B) {
	records, original := openOrderedHashLeafLabBenchmark(b, 230, 32)
	target := records[len(records)/2]
	first := bytes.Repeat([]byte{0xa5}, 32)
	second := bytes.Repeat([]byte{0x5a}, 32)
	work := make([]byte, OrderedHashLeafLabPageSize)
	targetRank := int(original.page[orderedHashLeafLabRankStart+int(target.Slot)])
	targetStart, targetEnd, ok := original.recordBounds(targetRank)
	if !ok {
		b.Fatal("target bounds")
	}
	targetKeyLength := orderedHashLeafLabKeyLength(
		original.page, original.layout, targetRank,
	)
	extent := original.ExtentBytes()
	fullChecksumBytes := orderedHashLeafLabFullChecksumBytes(original.page)
	payloadBytes := int(original.heapEnd) - original.layout.heapStart

	b.Run("COW/UpdateSameLength", func(b *testing.B) {
		reportOrderedHashLeafLabMutationIO(
			b, extent, int(original.heapEnd), fullChecksumBytes,
		)
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			value := first
			if index&1 != 0 {
				value = second
			}
			page, err := original.UpdateTo(
				work, uint64(index)+8, target.Slot, target.Key, value, false,
			)
			if err != nil {
				b.Fatal(err)
			}
			orderedHashLeafLabBenchmarkBytes = page
		}
	})
	b.Run("COW/UpdateGrow", func(b *testing.B) {
		value := bytes.Repeat([]byte{0x3c}, 48)
		reportOrderedHashLeafLabMutationIO(
			b, extent, int(original.heapEnd), fullChecksumBytes,
		)
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			page, err := original.UpdateTo(
				work, uint64(index)+8, target.Slot, target.Key, value, false,
			)
			if err != nil {
				b.Fatal(err)
			}
			orderedHashLeafLabBenchmarkBytes = page
		}
	})
	b.Run("COW/UpdateShrink", func(b *testing.B) {
		value := bytes.Repeat([]byte{0xc3}, 16)
		reportOrderedHashLeafLabMutationIO(
			b, extent, int(original.heapEnd), fullChecksumBytes,
		)
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			page, err := original.UpdateTo(
				work, uint64(index)+8, target.Slot, target.Key, value, false,
			)
			if err != nil {
				b.Fatal(err)
			}
			orderedHashLeafLabBenchmarkBytes = page
		}
	})
	b.Run("COW/UpdateGrowExtent", func(b *testing.B) {
		smallRecords := orderedHashLeafLabRecords(1, 1)
		small := openOrderedHashLeafLabTest(b, smallRecords)
		value := bytes.Repeat([]byte{0x77}, 4<<10)
		work := make([]byte, OrderedHashLeafLabPageSize)
		wantExtent := 8 << 10
		reportOrderedHashLeafLabMutationIO(
			b, wantExtent, int(small.heapEnd), wantExtent-8,
		)
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			page, err := small.UpdateTo(
				work, uint64(index)+8, smallRecords[0].Slot,
				smallRecords[0].Key, value, false,
			)
			if err != nil {
				b.Fatal(err)
			}
			orderedHashLeafLabBenchmarkBytes = page
		}
	})
	b.Run("COW/Delete", func(b *testing.B) {
		work := make([]byte, OrderedHashLeafLabPageSize)
		reportOrderedHashLeafLabMutationIO(
			b, extent, payloadBytes-(targetEnd-targetStart), fullChecksumBytes,
		)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			page, err := original.DeleteTo(work, 8, target.Slot, target.Key)
			if err != nil {
				b.Fatal(err)
			}
			orderedHashLeafLabBenchmarkBytes = page
		}
	})
	b.Run("COW/Insert", func(b *testing.B) {
		work := make([]byte, OrderedHashLeafLabPageSize)
		reportOrderedHashLeafLabMutationIO(
			b, extent, payloadBytes+len("key-0115a")+len("inserted"),
			fullChecksumBytes,
		)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			page, slot, err := original.InsertTo(
				work, 8, []byte("key-0115a"), []byte("inserted"), false,
			)
			if err != nil {
				b.Fatal(err)
			}
			orderedHashLeafLabBenchmarkBytes = page
			orderedHashLeafLabBenchmarkSlot = slot
		}
	})
	b.Run("Owned/UpdateSameLength", func(b *testing.B) {
		page := append([]byte(nil), original.page...)
		owned, err := OpenOrderedHashLeafLab(page, orderedHashLeafLabTestSeed)
		if err != nil {
			b.Fatal(err)
		}
		reportOrderedHashLeafLabMutationIO(
			b, extent, len(first),
			orderedHashLeafLabChangedChecksumBytes(
				original.page, targetStart+targetKeyLength, targetEnd,
			),
		)
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			value := first
			if index&1 != 0 {
				value = second
			}
			if err := owned.MaterializeOwnedUpdate(
				target.Slot, target.Key, value, false,
			); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("Owned/UpdateGrowExtentSignal", func(b *testing.B) {
		smallRecords := orderedHashLeafLabRecords(1, 1)
		page := encodeOrderedHashLeafLabTest(b, smallRecords)
		owned, err := OpenOrderedHashLeafLab(page, orderedHashLeafLabTestSeed)
		if err != nil {
			b.Fatal(err)
		}
		value := bytes.Repeat([]byte{0x77}, 4<<10)
		reportOrderedHashLeafLabMutationIO(
			b, owned.ExtentBytes(), 0, 0,
		)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			err := owned.MaterializeOwnedUpdate(
				smallRecords[0].Slot, smallRecords[0].Key, value, false,
			)
			orderedHashLeafLabBenchmarkBool =
				errors.Is(err, ErrOrderedHashLeafLabResize)
		}
	})
	b.Run("Owned/UpdateGrow", func(b *testing.B) {
		page := append([]byte(nil), original.page...)
		owned, err := OpenOrderedHashLeafLab(page, orderedHashLeafLabTestSeed)
		if err != nil {
			b.Fatal(err)
		}
		grown := bytes.Repeat([]byte{0x3c}, 48)
		reportOrderedHashLeafLabMutationIO(
			b, extent, int(original.heapEnd)-targetEnd+len(grown),
			orderedHashLeafLabChangedChecksumBytes(
				original.page, targetStart+targetKeyLength,
				int(original.heapEnd)+len(grown)-len(target.Value),
			),
		)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if err := owned.MaterializeOwnedUpdate(
				target.Slot, target.Key, grown, false,
			); err != nil {
				b.Fatal(err)
			}
			b.StopTimer()
			if err := owned.MaterializeOwnedUpdate(
				target.Slot, target.Key, first, false,
			); err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
		}
	})
	b.Run("Owned/UpdateShrink", func(b *testing.B) {
		page := append([]byte(nil), original.page...)
		owned, err := OpenOrderedHashLeafLab(page, orderedHashLeafLabTestSeed)
		if err != nil {
			b.Fatal(err)
		}
		grown := bytes.Repeat([]byte{0x3c}, 48)
		shrunk := bytes.Repeat([]byte{0xc3}, 16)
		reportOrderedHashLeafLabMutationIO(
			b, extent, int(original.heapEnd)-targetEnd+len(shrunk),
			orderedHashLeafLabChangedChecksumBytes(
				original.page, targetStart+targetKeyLength,
				int(original.heapEnd),
			),
		)
		if err := owned.MaterializeOwnedUpdate(
			target.Slot, target.Key, grown, false,
		); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if err := owned.MaterializeOwnedUpdate(
				target.Slot, target.Key, shrunk, false,
			); err != nil {
				b.Fatal(err)
			}
			b.StopTimer()
			if err := owned.MaterializeOwnedUpdate(
				target.Slot, target.Key, grown, false,
			); err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
		}
	})
	b.Run("Owned/Delete", func(b *testing.B) {
		page := append([]byte(nil), original.page...)
		owned, err := OpenOrderedHashLeafLab(page, orderedHashLeafLabTestSeed)
		if err != nil {
			b.Fatal(err)
		}
		currentSlot := target.Slot
		reportOrderedHashLeafLabMutationIO(
			b, extent, payloadBytes-(targetEnd-targetStart),
			orderedHashLeafLabChangedChecksumBytes(
				original.page, orderedHashLeafLabVariableStart,
				int(original.heapEnd),
			),
		)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if err := owned.MaterializeOwnedDelete(
				currentSlot, target.Key,
			); err != nil {
				b.Fatal(err)
			}
			b.StopTimer()
			currentSlot, err = owned.MaterializeOwnedInsert(
				target.Key, target.Value, false,
			)
			if err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
		}
	})
	b.Run("Owned/Insert", func(b *testing.B) {
		page := append([]byte(nil), original.page...)
		owned, err := OpenOrderedHashLeafLab(page, orderedHashLeafLabTestSeed)
		if err != nil {
			b.Fatal(err)
		}
		reportOrderedHashLeafLabMutationIO(
			b, extent, payloadBytes,
			orderedHashLeafLabChangedChecksumBytes(
				original.page, orderedHashLeafLabVariableStart,
				int(original.heapEnd),
			),
		)
		if err := owned.MaterializeOwnedDelete(
			target.Slot, target.Key,
		); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			slot, err := owned.MaterializeOwnedInsert(
				target.Key, target.Value, false,
			)
			if err != nil {
				b.Fatal(err)
			}
			b.StopTimer()
			if err := owned.MaterializeOwnedDelete(
				slot, target.Key,
			); err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
		}
	})
	b.Run("OpenAdmission", func(b *testing.B) {
		reportOrderedHashLeafLabMutationIO(
			b, extent, 0, fullChecksumBytes,
		)
		b.ReportAllocs()
		for range b.N {
			view, err := OpenOrderedHashLeafLab(
				original.page, orderedHashLeafLabTestSeed,
			)
			if err != nil {
				b.Fatal(err)
			}
			orderedHashLeafLabBenchmarkInt = view.Len()
		}
	})
	b.Run("SplitSignalFull", func(b *testing.B) {
		_, full := openOrderedHashLeafLabBenchmark(b, 256, 1)
		reportOrderedHashLeafLabMutationIO(
			b, full.ExtentBytes(), 0, 0,
		)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			_, _, err := full.InsertTo(
				work, 8, []byte("zzzz"), []byte("x"), false,
			)
			orderedHashLeafLabBenchmarkBool =
				errors.Is(err, ErrOrderedHashLeafLabSplit)
		}
	})
}

func BenchmarkOrderedHashLeafLabSpaceByOccupancy(b *testing.B) {
	for _, live := range []int{128, 192, 218, 225, 230, 244} {
		b.Run(fmt.Sprintf("Live%d", live), func(b *testing.B) {
			records := orderedHashLeafLabRecords(live, 8)
			page := make([]byte, OrderedHashLeafLabPageSize)
			var encoded []byte
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				var err error
				encoded, err = EncodeOrderedHashLeafLab(
					page, orderedHashLeafLabTestHeader(1),
					orderedHashLeafLabTestSeed, records,
				)
				if err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			b.ReportMetric(
				OrderedHashLeafLabMetadataBytesPerLiveKey(live),
				"structural_B/live_key",
			)
			b.ReportMetric(
				float64(len(encoded))/float64(live),
				"durable_extent_B/live_key",
			)
			b.ReportMetric(
				float64(int(binaryLittleEndianUint16OrderedHashLeafLab(
					encoded[38:40],
				))-orderedHashLeafLabMakeLayout(live).heapStart)/float64(live),
				"key_value_B/live_key",
			)
			b.ReportMetric(
				100*float64(live)/OrderedHashLeafLabSlotCount,
				"occupancy_pct",
			)
		})
	}
}

func BenchmarkOrderedHashLeafLabExtentMutationScaling(b *testing.B) {
	tests := []struct {
		name        string
		count       int
		valueLength int
		wantExtent  int
	}{
		{name: "4KiB", count: 128, valueLength: 8, wantExtent: 4 << 10},
		{name: "8KiB", count: 230, valueLength: 8, wantExtent: 8 << 10},
		{name: "16KiB", count: 230, valueLength: 48, wantExtent: 16 << 10},
		{name: "32KiB", count: 230, valueLength: 112, wantExtent: 32 << 10},
		{name: "64KiB", count: 230, valueLength: 255, wantExtent: 64 << 10},
	}
	for _, test := range tests {
		records := orderedHashLeafLabRecords(test.count, test.valueLength)
		original := openOrderedHashLeafLabTest(b, records)
		if original.ExtentBytes() != test.wantExtent {
			b.Fatalf(
				"%s fixture extent = %d, want %d",
				test.name, original.ExtentBytes(), test.wantExtent,
			)
		}
		target := records[len(records)/4]
		rank := int(original.page[orderedHashLeafLabRankStart+int(target.Slot)])
		start, end, ok := original.recordBounds(rank)
		if !ok {
			b.Fatal("target bounds")
		}
		keyLength := orderedHashLeafLabKeyLength(
			original.page, original.layout, rank,
		)
		replacement := bytes.Repeat([]byte{0x6d}, test.valueLength)

		b.Run(test.name+"/COWSameLength", func(b *testing.B) {
			work := make([]byte, OrderedHashLeafLabPageSize)
			reportOrderedHashLeafLabMutationIO(
				b, test.wantExtent, int(original.heapEnd),
				orderedHashLeafLabFullChecksumBytes(original.page),
			)
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				page, err := original.UpdateTo(
					work, uint64(index)+8, target.Slot, target.Key,
					replacement, false,
				)
				if err != nil {
					b.Fatal(err)
				}
				orderedHashLeafLabBenchmarkBytes = page
			}
		})

		b.Run(test.name+"/OwnedSameLength", func(b *testing.B) {
			page := append([]byte(nil), original.page...)
			owned, err := OpenOrderedHashLeafLab(page, orderedHashLeafLabTestSeed)
			if err != nil {
				b.Fatal(err)
			}
			reportOrderedHashLeafLabMutationIO(
				b, test.wantExtent, len(replacement),
				orderedHashLeafLabChangedChecksumBytes(
					original.page, start+keyLength, end,
				),
			)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if err := owned.MaterializeOwnedUpdate(
					target.Slot, target.Key, replacement, false,
				); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
