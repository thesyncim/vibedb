package storeio

import (
	"bytes"
	"fmt"
	"math/bits"
	"testing"
)

var (
	adaptiveOrderedLeafLabBenchmarkBytes []byte
	adaptiveOrderedLeafLabBenchmarkSlot  uint8
	adaptiveOrderedLeafLabBenchmarkInt   int
	adaptiveOrderedLeafLabBenchmarkBool  bool
)

func openAdaptiveOrderedLeafLabBenchmark(
	b testing.TB,
	class AdaptiveOrderedLeafLabClass,
	count, valueLength int,
) ([]AdaptiveOrderedLeafLabRecord, AdaptiveOrderedLeafLabView) {
	b.Helper()
	records := adaptiveOrderedLeafLabTestRecords(b, class, count, valueLength)
	return records, openAdaptiveOrderedLeafLabTest(b, class, records)
}

func BenchmarkAdaptiveOrderedLeafLabGroupSelection(b *testing.B) {
	hashes := [256]uint64{}
	var state uint64 = 0x9e3779b97f4a7c15
	for index := range hashes {
		state ^= state >> 12
		state ^= state << 25
		state ^= state >> 27
		hashes[index] = state * 0x2545f4914f6cdd1d
	}
	b.Run("Adaptive12x16", func(b *testing.B) {
		b.ReportAllocs()
		for index := range b.N {
			first, second := adaptiveOrderedLeafLabGroups(hashes[index&255])
			adaptiveOrderedLeafLabBenchmarkSlot = first ^ second
		}
	})
	b.Run("Current15x16", func(b *testing.B) {
		b.ReportAllocs()
		for index := range b.N {
			first, second := orderedHashLeafLabGroups(hashes[index&255])
			adaptiveOrderedLeafLabBenchmarkSlot = first ^ second
		}
	})
}

func BenchmarkAdaptiveOrderedLeafLabExactCandidate(b *testing.B) {
	records, adaptive := openAdaptiveOrderedLeafLabBenchmark(
		b, AdaptiveOrderedLeafLabWide, 256, 1,
	)
	currentRecords, current :=
		openCurrentOrderedLeafFromAdaptiveBenchmark(b, records)
	adaptiveNormal := records[:0]
	for _, record := range records {
		if int(record.Slot) < AdaptiveOrderedLeafLabNormalSlots {
			adaptiveNormal = append(adaptiveNormal, record)
		}
	}
	currentNormal := currentRecords[:0]
	for _, record := range currentRecords {
		if int(record.Slot) < OrderedHashLeafLabNormalSlotCount {
			currentNormal = append(currentNormal, record)
		}
	}
	b.Run("Adaptive", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			for index := range adaptiveNormal {
				adaptiveOrderedLeafLabBenchmarkBytes, _,
					adaptiveOrderedLeafLabBenchmarkBool =
					adaptive.lookupNormalCandidate(
						adaptiveNormal[index].Slot,
						adaptiveNormal[index].Key,
					)
			}
		}
		b.ReportMetric(
			float64(b.Elapsed().Nanoseconds())/
				float64(int64(b.N)*int64(len(adaptiveNormal))),
			"ns/key",
		)
	})
	b.Run("Current", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			for index := range currentNormal {
				adaptiveOrderedLeafLabBenchmarkBytes, _,
					adaptiveOrderedLeafLabBenchmarkBool =
					current.lookupCandidate(
						currentNormal[index].Slot,
						currentNormal[index].Key,
					)
			}
		}
		b.ReportMetric(
			float64(b.Elapsed().Nanoseconds())/
				float64(int64(b.N)*int64(len(currentNormal))),
			"ns/key",
		)
	})
}

func openCurrentOrderedLeafFromAdaptiveBenchmark(
	b testing.TB, records []AdaptiveOrderedLeafLabRecord,
) ([]OrderedHashLeafLabRecord, OrderedHashLeafLabView) {
	b.Helper()
	currentRecords := make([]OrderedHashLeafLabRecord, len(records))
	for index := range records {
		currentRecords[index] = OrderedHashLeafLabRecord{
			Key:      records[index].Key,
			Value:    records[index].Value,
			Overflow: records[index].Overflow,
		}
	}
	if err := PlaceOrderedHashLeafLabRecords(
		adaptiveOrderedLeafLabTestSeed, currentRecords,
	); err != nil {
		b.Fatal(err)
	}
	page, err := EncodeOrderedHashLeafLab(
		make([]byte, OrderedHashLeafLabPageSize),
		orderedHashLeafLabTestHeader(7),
		adaptiveOrderedLeafLabTestSeed,
		currentRecords,
	)
	if err != nil {
		b.Fatal(err)
	}
	current, err := OpenOrderedHashLeafLab(
		page, adaptiveOrderedLeafLabTestSeed,
	)
	if err != nil {
		b.Fatal(err)
	}
	return currentRecords, current
}

func adaptiveOrderedLeafLabNormalAndStash(
	b testing.TB, records []AdaptiveOrderedLeafLabRecord,
) (normal, stashMedian, stashWorst AdaptiveOrderedLeafLabRecord) {
	b.Helper()
	stash := make([]AdaptiveOrderedLeafLabRecord, 0, 64)
	for _, record := range records {
		if int(record.Slot) < AdaptiveOrderedLeafLabNormalSlots {
			normal = record
		} else {
			stash = append(stash, record)
		}
	}
	if len(stash) == 0 {
		b.Fatal("benchmark fixture has no stash")
	}
	return normal, stash[len(stash)/2], stash[len(stash)-1]
}

func BenchmarkAdaptiveOrderedLeafLabPointLookup(b *testing.B) {
	records, view := openAdaptiveOrderedLeafLabBenchmark(
		b, AdaptiveOrderedLeafLabNarrow,
		AdaptiveOrderedLeafLabNarrowLiveTarget, 8,
	)
	normal, stashMedian, stashWorst :=
		adaptiveOrderedLeafLabNormalAndStash(b, records)
	miss := []byte("not-here")
	normalHash := adaptiveOrderedLeafLabKeyHash(
		adaptiveOrderedLeafLabTestSeed, normal.Key,
	)
	stashMedianHash := adaptiveOrderedLeafLabKeyHash(
		adaptiveOrderedLeafLabTestSeed, stashMedian.Key,
	)
	stashWorstHash := adaptiveOrderedLeafLabKeyHash(
		adaptiveOrderedLeafLabTestSeed, stashWorst.Key,
	)
	missHash := adaptiveOrderedLeafLabKeyHash(
		adaptiveOrderedLeafLabTestSeed, miss,
	)

	b.Run("Narrow/NormalHit/Prehashed", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			adaptiveOrderedLeafLabBenchmarkSlot,
				adaptiveOrderedLeafLabBenchmarkBytes, _,
				adaptiveOrderedLeafLabBenchmarkBool =
				view.LookupHashed(normalHash, normal.Key)
		}
	})
	b.Run("Narrow/NormalHit/HashIncluded", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			adaptiveOrderedLeafLabBenchmarkSlot,
				adaptiveOrderedLeafLabBenchmarkBytes, _,
				adaptiveOrderedLeafLabBenchmarkBool = view.Lookup(normal.Key)
		}
	})
	b.Run("Narrow/StashHit/P50/Prehashed", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			adaptiveOrderedLeafLabBenchmarkSlot,
				adaptiveOrderedLeafLabBenchmarkBytes, _,
				adaptiveOrderedLeafLabBenchmarkBool =
				view.LookupHashed(stashMedianHash, stashMedian.Key)
		}
	})
	b.Run("Narrow/StashHit/P50/HashIncluded", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			adaptiveOrderedLeafLabBenchmarkSlot,
				adaptiveOrderedLeafLabBenchmarkBytes, _,
				adaptiveOrderedLeafLabBenchmarkBool =
				view.Lookup(stashMedian.Key)
		}
	})
	b.Run("Narrow/StashHit/Worst/Prehashed", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			adaptiveOrderedLeafLabBenchmarkSlot,
				adaptiveOrderedLeafLabBenchmarkBytes, _,
				adaptiveOrderedLeafLabBenchmarkBool =
				view.LookupHashed(stashWorstHash, stashWorst.Key)
		}
	})
	b.Run("Narrow/StashHit/Worst/HashIncluded", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			adaptiveOrderedLeafLabBenchmarkSlot,
				adaptiveOrderedLeafLabBenchmarkBytes, _,
				adaptiveOrderedLeafLabBenchmarkBool =
				view.Lookup(stashWorst.Key)
		}
	})
	b.Run("Narrow/Miss/Prehashed", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			adaptiveOrderedLeafLabBenchmarkSlot,
				adaptiveOrderedLeafLabBenchmarkBytes, _,
				adaptiveOrderedLeafLabBenchmarkBool =
				view.LookupHashed(missHash, miss)
		}
	})
	b.Run("Narrow/Miss/HashIncluded", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			adaptiveOrderedLeafLabBenchmarkSlot,
				adaptiveOrderedLeafLabBenchmarkBytes, _,
				adaptiveOrderedLeafLabBenchmarkBool = view.Lookup(miss)
		}
	})
	b.Run("Narrow/AllKeyAverage/Prehashed", func(b *testing.B) {
		hashes := make([]uint64, len(records))
		for index := range records {
			hashes[index] = adaptiveOrderedLeafLabKeyHash(
				adaptiveOrderedLeafLabTestSeed, records[index].Key,
			)
		}
		b.ReportAllocs()
		b.SetBytes(int64(len(records)))
		b.ResetTimer()
		for range b.N {
			for index := range records {
				adaptiveOrderedLeafLabBenchmarkSlot,
					adaptiveOrderedLeafLabBenchmarkBytes, _,
					adaptiveOrderedLeafLabBenchmarkBool =
					view.LookupHashed(hashes[index], records[index].Key)
			}
		}
		b.StopTimer()
		b.ReportMetric(
			float64(b.Elapsed().Nanoseconds())/
				float64(int64(b.N)*int64(len(records))),
			"ns/key",
		)
	})
	b.Run("Narrow/AllKeyAverage/HashIncluded", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(records)))
		b.ResetTimer()
		for range b.N {
			for index := range records {
				adaptiveOrderedLeafLabBenchmarkSlot,
					adaptiveOrderedLeafLabBenchmarkBytes, _,
					adaptiveOrderedLeafLabBenchmarkBool =
					view.Lookup(records[index].Key)
			}
		}
		b.StopTimer()
		b.ReportMetric(
			float64(b.Elapsed().Nanoseconds())/
				float64(int64(b.N)*int64(len(records))),
			"ns/key",
		)
	})

	currentRecords, current :=
		openCurrentOrderedLeafFromAdaptiveBenchmark(
			b, records,
		)
	currentHashes := make([]uint64, len(currentRecords))
	for index := range currentRecords {
		currentHashes[index] = orderedHashLeafLabKeyHash(
			adaptiveOrderedLeafLabTestSeed, currentRecords[index].Key,
		)
	}
	b.Run("Current/AllKeyAverage/Prehashed", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(currentRecords)))
		b.ResetTimer()
		for range b.N {
			for index := range currentRecords {
				adaptiveOrderedLeafLabBenchmarkSlot,
					adaptiveOrderedLeafLabBenchmarkBytes, _,
					adaptiveOrderedLeafLabBenchmarkBool =
					current.LookupHashed(currentHashes[index], currentRecords[index].Key)
			}
		}
		b.StopTimer()
		b.ReportMetric(
			float64(b.Elapsed().Nanoseconds())/
				float64(int64(b.N)*int64(len(currentRecords))),
			"ns/key",
		)
	})
	b.Run("Current/AllKeyAverage/HashIncluded", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(currentRecords)))
		b.ResetTimer()
		for range b.N {
			for index := range currentRecords {
				adaptiveOrderedLeafLabBenchmarkSlot,
					adaptiveOrderedLeafLabBenchmarkBytes, _,
					adaptiveOrderedLeafLabBenchmarkBool =
					current.Lookup(currentRecords[index].Key)
			}
		}
		b.StopTimer()
		b.ReportMetric(
			float64(b.Elapsed().Nanoseconds())/
				float64(int64(b.N)*int64(len(currentRecords))),
			"ns/key",
		)
	})
}

func BenchmarkAdaptiveOrderedLeafLabLexicalScan(b *testing.B) {
	records, view := openAdaptiveOrderedLeafLabBenchmark(
		b, AdaptiveOrderedLeafLabNarrow,
		AdaptiveOrderedLeafLabNarrowLiveTarget, 8,
	)
	b.Run("Narrow", func(b *testing.B) {
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
				adaptiveOrderedLeafLabBenchmarkBytes = key
				adaptiveOrderedLeafLabBenchmarkBytes = value
			}
		}
		b.StopTimer()
		b.ReportMetric(
			float64(b.Elapsed().Nanoseconds())/
				float64(int64(b.N)*int64(len(records))),
			"ns/doc",
		)
	})

	currentRecords, current :=
		openCurrentOrderedLeafFromAdaptiveBenchmark(b, records)
	b.Run("Current", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(currentRecords)))
		b.ResetTimer()
		for range b.N {
			iterator := current.AllRows()
			for {
				key, value, _, ok := iterator.NextBorrowed()
				if !ok {
					break
				}
				adaptiveOrderedLeafLabBenchmarkBytes = key
				adaptiveOrderedLeafLabBenchmarkBytes = value
			}
		}
		b.StopTimer()
		b.ReportMetric(
			float64(b.Elapsed().Nanoseconds())/
				float64(int64(b.N)*int64(len(currentRecords))),
			"ns/doc",
		)
	})
}

func BenchmarkAdaptiveOrderedLeafLabMutations(b *testing.B) {
	records, original := openAdaptiveOrderedLeafLabBenchmark(
		b, AdaptiveOrderedLeafLabNarrow,
		AdaptiveOrderedLeafLabNarrowLiveTarget, 8,
	)
	target := records[len(records)/2]
	first := []byte("AAAAAAAA")
	second := []byte("BBBBBBBB")
	adaptiveDeletedPage, err := original.DeleteTo(
		make([]byte, AdaptiveOrderedLeafLabNarrowBytes),
		8, target.Slot, target.Key,
	)
	if err != nil {
		b.Fatal(err)
	}
	adaptiveDeleted, err := OpenAdaptiveOrderedLeafLab(
		adaptiveDeletedPage, adaptiveOrderedLeafLabTestSeed,
	)
	if err != nil {
		b.Fatal(err)
	}
	currentRecords, current :=
		openCurrentOrderedLeafFromAdaptiveBenchmark(b, records)
	var currentTarget OrderedHashLeafLabRecord
	for _, record := range currentRecords {
		if bytes.Equal(record.Key, target.Key) {
			currentTarget = record
			break
		}
	}
	currentDeletedPage, err := current.DeleteTo(
		make([]byte, OrderedHashLeafLabPageSize),
		8, currentTarget.Slot, currentTarget.Key,
	)
	if err != nil {
		b.Fatal(err)
	}
	currentDeleted, err := OpenOrderedHashLeafLab(
		currentDeletedPage, adaptiveOrderedLeafLabTestSeed,
	)
	if err != nil {
		b.Fatal(err)
	}

	b.Run("Owned/SameLengthUpdate", func(b *testing.B) {
		page := append([]byte(nil), original.page...)
		owned, err := OpenAdaptiveOrderedLeafLab(
			page, adaptiveOrderedLeafLabTestSeed,
		)
		if err != nil {
			b.Fatal(err)
		}
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

	b.Run("COW/DeleteSameKeyRestore", func(b *testing.B) {
		deletedBuffer := make([]byte, AdaptiveOrderedLeafLabNarrowBytes)
		restoredBuffer := make([]byte, AdaptiveOrderedLeafLabNarrowBytes)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			deletedPage, err := original.DeleteTo(
				deletedBuffer, 8, target.Slot, target.Key,
			)
			if err != nil {
				b.Fatal(err)
			}
			deleted, err := OpenAdaptiveOrderedLeafLab(
				deletedPage, adaptiveOrderedLeafLabTestSeed,
			)
			if err != nil {
				b.Fatal(err)
			}
			restoredPage, err := deleted.RestoreTo(
				restoredBuffer, 9,
				target.Slot, target.Key, target.Value, false,
			)
			if err != nil {
				b.Fatal(err)
			}
			adaptiveOrderedLeafLabBenchmarkBytes = restoredPage
		}
	})

	b.Run("Fair/Adaptive/Delete", func(b *testing.B) {
		work := make([]byte, AdaptiveOrderedLeafLabNarrowBytes)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			page, err := original.DeleteTo(
				work, 8, target.Slot, target.Key,
			)
			if err != nil {
				b.Fatal(err)
			}
			adaptiveOrderedLeafLabBenchmarkBytes = page
		}
	})
	b.Run("Fair/Current/Delete", func(b *testing.B) {
		work := make([]byte, OrderedHashLeafLabPageSize)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			page, err := current.DeleteTo(
				work, 8, currentTarget.Slot, currentTarget.Key,
			)
			if err != nil {
				b.Fatal(err)
			}
			adaptiveOrderedLeafLabBenchmarkBytes = page
		}
	})
	b.Run("Fair/Adaptive/Insert", func(b *testing.B) {
		work := make([]byte, AdaptiveOrderedLeafLabNarrowBytes)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			page, err := adaptiveDeleted.RestoreTo(
				work, 9, target.Slot, target.Key, target.Value, false,
			)
			if err != nil {
				b.Fatal(err)
			}
			adaptiveOrderedLeafLabBenchmarkBytes = page
		}
	})
	b.Run("Fair/Current/Insert", func(b *testing.B) {
		work := make([]byte, OrderedHashLeafLabPageSize)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			page, slot, err := currentDeleted.InsertTo(
				work, 9, currentTarget.Key, currentTarget.Value, false,
			)
			if err != nil {
				b.Fatal(err)
			}
			adaptiveOrderedLeafLabBenchmarkBytes = page
			adaptiveOrderedLeafLabBenchmarkSlot = slot
		}
	})
	b.Run("Fair/Adaptive/DeleteRestoreNoAdmission", func(b *testing.B) {
		deletedWork := make([]byte, AdaptiveOrderedLeafLabNarrowBytes)
		restoredWork := make([]byte, AdaptiveOrderedLeafLabNarrowBytes)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			page, err := original.DeleteTo(
				deletedWork, 8, target.Slot, target.Key,
			)
			if err != nil {
				b.Fatal(err)
			}
			restored, err := adaptiveDeleted.RestoreTo(
				restoredWork, 9,
				target.Slot, target.Key, target.Value, false,
			)
			if err != nil {
				b.Fatal(err)
			}
			adaptiveOrderedLeafLabBenchmarkBytes = page
			adaptiveOrderedLeafLabBenchmarkBytes = restored
		}
	})
	b.Run("Admission/Adaptive4KiB", func(b *testing.B) {
		page := original.PersistentBytes()
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			view, err := OpenAdaptiveOrderedLeafLab(
				page, adaptiveOrderedLeafLabTestSeed,
			)
			if err != nil {
				b.Fatal(err)
			}
			adaptiveOrderedLeafLabBenchmarkInt = view.Len()
		}
	})
	b.Run("Admission/Current", func(b *testing.B) {
		page := current.PersistentBytes()
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			view, err := OpenOrderedHashLeafLab(
				page, adaptiveOrderedLeafLabTestSeed,
			)
			if err != nil {
				b.Fatal(err)
			}
			adaptiveOrderedLeafLabBenchmarkInt = view.Len()
		}
	})

	b.Run("Promotion/NarrowToWide", func(b *testing.B) {
		work := make([]byte, AdaptiveOrderedLeafLabWideBytes)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			page, err := PromoteAdaptiveOrderedLeafLab(work, 8, &original)
			if err != nil {
				b.Fatal(err)
			}
			adaptiveOrderedLeafLabBenchmarkBytes = page
		}
	})

	b.Run("COW/RandomReplacementChurn", func(b *testing.B) {
		deletedBuffer := make([]byte, AdaptiveOrderedLeafLabNarrowBytes)
		replacedBuffer := make([]byte, AdaptiveOrderedLeafLabNarrowBytes)
		const scheduleSize = 1024
		var targets [scheduleSize]int
		var replacementKeys [scheduleSize][8]byte
		for index := 0; index < scheduleSize; index++ {
			targets[index] = (index*137 + 41) % len(records)
			copy(replacementKeys[index][:], fmt.Sprintf("r%07d", index))
		}
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			schedule := index & (scheduleSize - 1)
			record := records[targets[schedule]]
			deletedPage, err := original.DeleteTo(
				deletedBuffer, 8, record.Slot, record.Key,
			)
			if err != nil {
				b.Fatal(err)
			}
			deleted, err := OpenAdaptiveOrderedLeafLab(
				deletedPage, adaptiveOrderedLeafLabTestSeed,
			)
			if err != nil {
				b.Fatal(err)
			}
			page, slot, err := deleted.InsertTo(
				replacedBuffer, 9,
				replacementKeys[schedule][:], record.Value, false,
			)
			if err != nil {
				b.Fatal(err)
			}
			adaptiveOrderedLeafLabBenchmarkBytes = page
			adaptiveOrderedLeafLabBenchmarkSlot = slot
		}
	})
}

func BenchmarkAdaptiveOrderedLeafLabSpace(b *testing.B) {
	for _, test := range []struct {
		name  string
		class AdaptiveOrderedLeafLabClass
		live  int
	}{
		{name: "Narrow195", class: AdaptiveOrderedLeafLabNarrow, live: 195},
		{name: "Wide195", class: AdaptiveOrderedLeafLabWide, live: 195},
		{name: "Wide256", class: AdaptiveOrderedLeafLabWide, live: 256},
	} {
		b.Run(test.name, func(b *testing.B) {
			records := adaptiveOrderedLeafLabTestRecords(
				b, test.class, test.live, 8,
			)
			dst := make([]byte, test.class.extentBytes())
			var page []byte
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				var err error
				page, err = EncodeAdaptiveOrderedLeafLab(
					dst, test.class,
					adaptiveOrderedLeafLabTestHeader(7),
					adaptiveOrderedLeafLabTestSeed,
					records,
				)
				if err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			view, err := OpenAdaptiveOrderedLeafLab(
				page, adaptiveOrderedLeafLabTestSeed,
			)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportMetric(
				AdaptiveOrderedLeafLabMetadataBytesPerLive(
					test.class, test.live,
				),
				"structural_B/live",
			)
			b.ReportMetric(
				float64(view.PhysicalSlackBytes())/float64(test.live),
				"physical_slack_B/live",
			)
			b.ReportMetric(float64(view.StashLen()), "stash_rows")
			b.ReportMetric(
				float64(len(page))/float64(test.live),
				"extent_B/live",
			)
		})
	}
}

func BenchmarkAdaptiveOrderedLeafLabWideStashWorst(b *testing.B) {
	records, view := openAdaptiveOrderedLeafLabBenchmark(
		b, AdaptiveOrderedLeafLabWide, 256, 1,
	)
	_, _, worst := adaptiveOrderedLeafLabNormalAndStash(b, records)
	hash := adaptiveOrderedLeafLabKeyHash(adaptiveOrderedLeafLabTestSeed, worst.Key)
	stashMask := view.stashMask()
	exactTags := 0
	tag := adaptiveOrderedLeafLabSaltedStashTag12(hash, view.page[21])
	for mask := stashMask; mask != 0; mask &= mask - 1 {
		index := bits.TrailingZeros64(mask)
		rank, _ := view.stashRankAt(index)
		start, _, _ := view.recordBounds(int(rank))
		keyLength := adaptiveOrderedLeafLabKeyLength(
			view.page, &view.layout, int(rank),
		)
		if adaptiveOrderedLeafLabSaltedStashTag12(
			adaptiveOrderedLeafLabKeyHash(
				adaptiveOrderedLeafLabTestSeed,
				view.page[start:start+keyLength],
			),
			view.page[21],
		) == tag {
			exactTags++
		}
	}
	b.Run("Hashed", func(b *testing.B) {
		b.ReportMetric(float64(bits.OnesCount64(stashMask)), "stash_rows")
		b.ReportMetric(6, "max_binary_probes")
		b.ReportMetric(float64(exactTags), "exact_tag_candidates")
		b.ReportAllocs()
		for range b.N {
			adaptiveOrderedLeafLabBenchmarkSlot,
				adaptiveOrderedLeafLabBenchmarkBytes, _,
				adaptiveOrderedLeafLabBenchmarkBool =
				view.LookupHashed(hash, worst.Key)
		}
	})
	b.Run("StableSlotHint", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			adaptiveOrderedLeafLabBenchmarkBytes, _,
				adaptiveOrderedLeafLabBenchmarkBool =
				view.LookupSlot(worst.Slot, worst.Key)
		}
	})
}

func BenchmarkAdaptiveOrderedLeafLabWidePointDistribution(b *testing.B) {
	records, view := openAdaptiveOrderedLeafLabBenchmark(
		b, AdaptiveOrderedLeafLabWide, 256, 1,
	)
	normal := make([]AdaptiveOrderedLeafLabRecord, 0, AdaptiveOrderedLeafLabNormalSlots)
	stash := make([]AdaptiveOrderedLeafLabRecord, 0, AdaptiveOrderedLeafLabWideStash)
	for _, record := range records {
		if int(record.Slot) < AdaptiveOrderedLeafLabNormalSlots {
			normal = append(normal, record)
		} else {
			stash = append(stash, record)
		}
	}
	normalHashes := make([]uint64, len(normal))
	normalProbeTotal := 0
	for index := range normal {
		normalHashes[index] = adaptiveOrderedLeafLabKeyHash(
			adaptiveOrderedLeafLabTestSeed, normal[index].Key,
		)
		for ordinal := 0; ordinal < adaptiveOrderedLeafLabGroupSize*2; ordinal++ {
			normalProbeTotal++
			if adaptiveOrderedLeafLabCandidate(
				normalHashes[index], ordinal,
			) == normal[index].Slot {
				break
			}
		}
	}
	b.Run("NormalAverage/Prehashed", func(b *testing.B) {
		b.Cleanup(func() {
			b.ReportMetric(
				float64(normalProbeTotal)/float64(len(normal)), "probes/key",
			)
		})
		b.ReportAllocs()
		b.SetBytes(int64(len(normal)))
		b.ResetTimer()
		for range b.N {
			for index := range normal {
				adaptiveOrderedLeafLabBenchmarkSlot,
					adaptiveOrderedLeafLabBenchmarkBytes, _,
					adaptiveOrderedLeafLabBenchmarkBool =
					view.LookupHashed(normalHashes[index], normal[index].Key)
			}
		}
		b.StopTimer()
		b.ReportMetric(
			float64(b.Elapsed().Nanoseconds())/
				float64(int64(b.N)*int64(len(normal))),
			"ns/key",
		)
	})
	b.Run("NormalAverage/HashIncluded", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(normal)))
		b.ResetTimer()
		for range b.N {
			for index := range normal {
				adaptiveOrderedLeafLabBenchmarkSlot,
					adaptiveOrderedLeafLabBenchmarkBytes, _,
					adaptiveOrderedLeafLabBenchmarkBool =
					view.Lookup(normal[index].Key)
			}
		}
		b.StopTimer()
		b.ReportMetric(
			float64(b.Elapsed().Nanoseconds())/
				float64(int64(b.N)*int64(len(normal))),
			"ns/key",
		)
	})
	for _, test := range []struct {
		name   string
		record AdaptiveOrderedLeafLabRecord
	}{
		{name: "StashP50", record: stash[len(stash)/2]},
		{name: "StashWorstRank", record: stash[len(stash)-1]},
	} {
		hash := adaptiveOrderedLeafLabKeyHash(
			adaptiveOrderedLeafLabTestSeed, test.record.Key,
		)
		b.Run(test.name+"/Prehashed", func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				adaptiveOrderedLeafLabBenchmarkSlot,
					adaptiveOrderedLeafLabBenchmarkBytes, _,
					adaptiveOrderedLeafLabBenchmarkBool =
					view.LookupHashed(hash, test.record.Key)
			}
		})
		b.Run(test.name+"/HashIncluded", func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				adaptiveOrderedLeafLabBenchmarkSlot,
					adaptiveOrderedLeafLabBenchmarkBytes, _,
					adaptiveOrderedLeafLabBenchmarkBool =
					view.Lookup(test.record.Key)
			}
		})
	}

	miss := []byte("not-here")
	missHash := adaptiveOrderedLeafLabKeyHash(
		adaptiveOrderedLeafLabTestSeed, miss,
	)
	b.Run("Miss/Prehashed", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			adaptiveOrderedLeafLabBenchmarkSlot,
				adaptiveOrderedLeafLabBenchmarkBytes, _,
				adaptiveOrderedLeafLabBenchmarkBool =
				view.LookupHashed(missHash, miss)
		}
	})

	currentRecords, current :=
		openCurrentOrderedLeafFromAdaptiveBenchmark(b, records)
	currentNormal := make([]OrderedHashLeafLabRecord, 0, 240)
	currentHashes := make([]uint64, 0, 240)
	currentProbeTotal := 0
	for _, record := range currentRecords {
		if int(record.Slot) >= OrderedHashLeafLabNormalSlotCount {
			continue
		}
		currentNormal = append(currentNormal, record)
		currentHashes = append(
			currentHashes,
			orderedHashLeafLabKeyHash(adaptiveOrderedLeafLabTestSeed, record.Key),
		)
		hash := currentHashes[len(currentHashes)-1]
		for ordinal := 0; ordinal < orderedHashLeafLabGroupSize*2; ordinal++ {
			currentProbeTotal++
			if orderedHashLeafLabCandidate(hash, ordinal) == record.Slot {
				break
			}
		}
	}
	b.Run("CurrentNormalAverage/Prehashed", func(b *testing.B) {
		b.Cleanup(func() {
			b.ReportMetric(
				float64(currentProbeTotal)/float64(len(currentNormal)), "probes/key",
			)
		})
		b.ReportAllocs()
		b.SetBytes(int64(len(currentNormal)))
		b.ResetTimer()
		for range b.N {
			for index := range currentNormal {
				adaptiveOrderedLeafLabBenchmarkSlot,
					adaptiveOrderedLeafLabBenchmarkBytes, _,
					adaptiveOrderedLeafLabBenchmarkBool =
					current.LookupHashed(currentHashes[index], currentNormal[index].Key)
			}
		}
		b.StopTimer()
		b.ReportMetric(
			float64(b.Elapsed().Nanoseconds())/
				float64(int64(b.N)*int64(len(currentNormal))),
			"ns/key",
		)
	})
}

func BenchmarkAdaptiveOrderedLeafLabWideSparseWorstDistribution(b *testing.B) {
	full := adaptiveOrderedLeafLabTestRecords(
		b, AdaptiveOrderedLeafLabWide, AdaptiveOrderedLeafLabWideSlots, 1,
	)
	records := make([]AdaptiveOrderedLeafLabRecord, 0,
		AdaptiveOrderedLeafLabNarrowLiveTarget)
	normalCount := 0
	for _, record := range full {
		if int(record.Slot) >= AdaptiveOrderedLeafLabNormalSlots {
			records = append(records, record)
			continue
		}
		if normalCount < AdaptiveOrderedLeafLabNarrowLiveTarget-
			AdaptiveOrderedLeafLabWideStash {
			records = append(records, record)
			normalCount++
		}
	}
	view := openAdaptiveOrderedLeafLabTest(b, AdaptiveOrderedLeafLabWide, records)
	normal := make([]AdaptiveOrderedLeafLabRecord, 0, normalCount)
	stash := make([]AdaptiveOrderedLeafLabRecord, 0,
		AdaptiveOrderedLeafLabWideStash)
	for _, record := range records {
		if int(record.Slot) < AdaptiveOrderedLeafLabNormalSlots {
			normal = append(normal, record)
		} else {
			stash = append(stash, record)
		}
	}
	normalHashes := make([]uint64, len(normal))
	for index := range normal {
		normalHashes[index] = adaptiveOrderedLeafLabKeyHash(
			adaptiveOrderedLeafLabTestSeed, normal[index].Key,
		)
	}
	b.Run("NormalAverage/Prehashed", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			for index := range normal {
				adaptiveOrderedLeafLabBenchmarkSlot,
					adaptiveOrderedLeafLabBenchmarkBytes, _,
					adaptiveOrderedLeafLabBenchmarkBool =
					view.LookupHashed(normalHashes[index], normal[index].Key)
			}
		}
		b.ReportMetric(
			float64(b.Elapsed().Nanoseconds())/
				float64(int64(b.N)*int64(len(normal))),
			"ns/key",
		)
	})
	for _, test := range []struct {
		name   string
		record AdaptiveOrderedLeafLabRecord
	}{
		{name: "StashP50", record: stash[len(stash)/2]},
		{name: "StashWorstSlot", record: stash[len(stash)-1]},
	} {
		hash := adaptiveOrderedLeafLabKeyHash(
			adaptiveOrderedLeafLabTestSeed, test.record.Key,
		)
		b.Run(test.name+"/Prehashed", func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				adaptiveOrderedLeafLabBenchmarkSlot,
					adaptiveOrderedLeafLabBenchmarkBytes, _,
					adaptiveOrderedLeafLabBenchmarkBool =
					view.LookupHashed(hash, test.record.Key)
			}
		})
	}
	miss := []byte("not-here")
	missHash := adaptiveOrderedLeafLabKeyHash(
		adaptiveOrderedLeafLabTestSeed, miss,
	)
	b.Run("Miss/Prehashed", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			adaptiveOrderedLeafLabBenchmarkSlot,
				adaptiveOrderedLeafLabBenchmarkBytes, _,
				adaptiveOrderedLeafLabBenchmarkBool =
				view.LookupHashed(missHash, miss)
		}
	})
	currentRecords, current :=
		openCurrentOrderedLeafFromAdaptiveBenchmark(b, records)
	currentNormal := currentRecords[:0]
	currentHashes := make([]uint64, 0, len(currentRecords))
	for _, record := range currentRecords {
		if int(record.Slot) >= OrderedHashLeafLabNormalSlotCount {
			continue
		}
		currentNormal = append(currentNormal, record)
		currentHashes = append(currentHashes,
			orderedHashLeafLabKeyHash(adaptiveOrderedLeafLabTestSeed, record.Key))
	}
	b.Run("CurrentNormalAverage/Prehashed", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			for index := range currentNormal {
				adaptiveOrderedLeafLabBenchmarkSlot,
					adaptiveOrderedLeafLabBenchmarkBytes, _,
					adaptiveOrderedLeafLabBenchmarkBool =
					current.LookupHashed(currentHashes[index], currentNormal[index].Key)
			}
		}
		b.ReportMetric(
			float64(b.Elapsed().Nanoseconds())/
				float64(int64(b.N)*int64(len(currentNormal))),
			"ns/key",
		)
	})
	b.Run("Space", func(b *testing.B) {
		for range b.N {
		}
		b.ReportMetric(
			float64(view.layout.heapStart+AdaptiveOrderedLeafLabTrailerBytes)/
				float64(len(records)),
			"structural_B/live",
		)
		b.ReportMetric(
			float64(len(view.page))/float64(len(records)),
			"physical_B/live",
		)
	})
}

func BenchmarkAdaptiveOrderedLeafLabWideSparseSteadyDistribution(b *testing.B) {
	const stashRows = 23
	full := adaptiveOrderedLeafLabTestRecords(
		b, AdaptiveOrderedLeafLabWide, AdaptiveOrderedLeafLabWideSlots, 1,
	)
	records := make([]AdaptiveOrderedLeafLabRecord, 0,
		AdaptiveOrderedLeafLabNarrowLiveTarget)
	normalNeeded := AdaptiveOrderedLeafLabNarrowLiveTarget - stashRows
	normalCount, stashCount := 0, 0
	for _, record := range full {
		if int(record.Slot) < AdaptiveOrderedLeafLabNormalSlots {
			if normalCount < normalNeeded {
				records = append(records, record)
				normalCount++
			}
		} else if stashCount < stashRows {
			records = append(records, record)
			stashCount++
		}
	}
	view := openAdaptiveOrderedLeafLabTest(b, AdaptiveOrderedLeafLabWide, records)
	normal := make([]AdaptiveOrderedLeafLabRecord, 0, normalNeeded)
	stash := make([]AdaptiveOrderedLeafLabRecord, 0, stashRows)
	for _, record := range records {
		if int(record.Slot) < AdaptiveOrderedLeafLabNormalSlots {
			normal = append(normal, record)
		} else {
			stash = append(stash, record)
		}
	}
	hashes := make([]uint64, len(normal))
	for index := range normal {
		hashes[index] = adaptiveOrderedLeafLabKeyHash(
			adaptiveOrderedLeafLabTestSeed, normal[index].Key,
		)
	}
	b.Run("NormalAverage/Prehashed", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			for index := range normal {
				adaptiveOrderedLeafLabBenchmarkSlot,
					adaptiveOrderedLeafLabBenchmarkBytes, _,
					adaptiveOrderedLeafLabBenchmarkBool =
					view.LookupHashed(hashes[index], normal[index].Key)
			}
		}
		b.ReportMetric(
			float64(b.Elapsed().Nanoseconds())/
				float64(int64(b.N)*int64(len(normal))),
			"ns/key",
		)
	})
	for _, test := range []struct {
		name   string
		record AdaptiveOrderedLeafLabRecord
	}{
		{name: "StashP50", record: stash[len(stash)/2]},
		{name: "StashWorstSlot", record: stash[len(stash)-1]},
	} {
		hash := adaptiveOrderedLeafLabKeyHash(
			adaptiveOrderedLeafLabTestSeed, test.record.Key,
		)
		b.Run(test.name+"/Prehashed", func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				adaptiveOrderedLeafLabBenchmarkSlot,
					adaptiveOrderedLeafLabBenchmarkBytes, _,
					adaptiveOrderedLeafLabBenchmarkBool =
					view.LookupHashed(hash, test.record.Key)
			}
		})
	}
	miss := []byte("not-here")
	missHash := adaptiveOrderedLeafLabKeyHash(
		adaptiveOrderedLeafLabTestSeed, miss,
	)
	b.Run("Miss/Prehashed", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			adaptiveOrderedLeafLabBenchmarkSlot,
				adaptiveOrderedLeafLabBenchmarkBytes, _,
				adaptiveOrderedLeafLabBenchmarkBool =
				view.LookupHashed(missHash, miss)
		}
	})
}

func BenchmarkAdaptiveOrderedLeafLabPrefix100(b *testing.B) {
	_, view := openAdaptiveOrderedLeafLabBenchmark(
		b, AdaptiveOrderedLeafLabWide, 256, 8,
	)
	prefix := []byte("k00001")
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		iterator := view.Prefix(prefix)
		for {
			_, value, _, ok := iterator.NextBorrowed()
			if !ok {
				break
			}
			adaptiveOrderedLeafLabBenchmarkBytes = value
		}
	}
}

func BenchmarkAdaptiveOrderedLeafLabTagNegativeMiss(b *testing.B) {
	_, view := openAdaptiveOrderedLeafLabBenchmark(
		b, AdaptiveOrderedLeafLabNarrow,
		AdaptiveOrderedLeafLabNarrowLiveTarget, 1,
	)
	var key []byte
	var hash uint64
	for candidate := 0; ; candidate++ {
		key = []byte(fmt.Sprintf("m%07d", candidate))
		hash = adaptiveOrderedLeafLabKeyHash(adaptiveOrderedLeafLabTestSeed, key)
		want := adaptiveOrderedLeafLabSaltedStashTag(hash, view.page[21], 8)
		if adaptiveOrderedLeafLabTagMask(
			view.page[view.layout.stashTagStart:view.layout.stashTagStart+
				view.layout.stashTagLen],
			want,
		)&view.stashMask() == 0 {
			break
		}
	}
	b.ReportAllocs()
	for range b.N {
		adaptiveOrderedLeafLabBenchmarkSlot,
			adaptiveOrderedLeafLabBenchmarkBytes, _,
			adaptiveOrderedLeafLabBenchmarkBool =
			view.LookupHashed(hash, key)
	}
}

func BenchmarkAdaptiveOrderedLeafLabExactConfirmationMiss(b *testing.B) {
	_, view := openAdaptiveOrderedLeafLabBenchmark(
		b, AdaptiveOrderedLeafLabNarrow,
		AdaptiveOrderedLeafLabNarrowLiveTarget, 1,
	)
	var key []byte
	var hash uint64
	stashTags := make(map[byte]struct{}, view.StashLen())
	for mask := view.stashMask(); mask != 0; mask &= mask - 1 {
		index := bits.TrailingZeros64(mask)
		stashTags[adaptiveOrderedLeafLabGetStashTag(
			view.page[view.layout.stashTagStart:view.layout.stashTagStart+view.layout.stashTagLen],
			index,
		)] = struct{}{}
	}
	for candidate := 0; ; candidate++ {
		key = []byte(fmt.Sprintf("m%07d", candidate))
		hash = adaptiveOrderedLeafLabKeyHash(adaptiveOrderedLeafLabTestSeed, key)
		if _, ok := stashTags[adaptiveOrderedLeafLabSaltedStashTag(
			hash, view.page[21], 8,
		)]; ok {
			break
		}
	}
	b.ReportAllocs()
	for range b.N {
		adaptiveOrderedLeafLabBenchmarkSlot,
			adaptiveOrderedLeafLabBenchmarkBytes, _,
			adaptiveOrderedLeafLabBenchmarkBool =
			view.LookupHashed(hash, key)
	}
	if adaptiveOrderedLeafLabBenchmarkBool {
		b.Fatal("false positive escaped exact confirmation")
	}
}

func BenchmarkAdaptiveOrderedLeafLabUpdatePayloadEquality(b *testing.B) {
	records, view := openAdaptiveOrderedLeafLabBenchmark(
		b, AdaptiveOrderedLeafLabNarrow,
		AdaptiveOrderedLeafLabNarrowLiveTarget, 8,
	)
	target := records[len(records)/2]
	replacement := bytes.Repeat([]byte{0xa5}, len(target.Value))
	b.ReportAllocs()
	for range b.N {
		if err := view.MaterializeOwnedUpdate(
			target.Slot, target.Key, replacement, false,
		); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAdaptiveOrderedLeafLabFairInterleavedAbsent(b *testing.B) {
	full := adaptiveOrderedLeafLabTestRecords(
		b, AdaptiveOrderedLeafLabWide, AdaptiveOrderedLeafLabWideSlots, 1,
	)
	steady := make([]AdaptiveOrderedLeafLabRecord, 0, 195)
	normal, stash := 0, 0
	for _, record := range full {
		if int(record.Slot) < AdaptiveOrderedLeafLabNormalSlots {
			if normal < 175 {
				steady = append(steady, record)
				normal++
			}
		} else if stash < 20 {
			steady = append(steady, record)
			stash++
		}
	}
	for _, fixture := range []struct {
		name    string
		records []AdaptiveOrderedLeafLabRecord
	}{
		{name: "SteadySparse20", records: steady},
		{name: "MaximumStash64", records: full},
	} {
		b.Run(fixture.name, func(b *testing.B) {
			adaptive := openAdaptiveOrderedLeafLabTest(
				b, AdaptiveOrderedLeafLabWide, fixture.records,
			)
			currentRecords, current :=
				openCurrentOrderedLeafFromAdaptiveBenchmark(b, fixture.records)
			var keys [256][8]byte
			var hashes [256]uint64
			for index := range keys {
				copy(keys[index][:], fmt.Sprintf("m%07d", index+1000000))
				hashes[index] = adaptiveOrderedLeafLabKeyHash(
					adaptiveOrderedLeafLabTestSeed, keys[index][:],
				)
				if _, _, _, ok := adaptive.LookupHashed(
					hashes[index], keys[index][:],
				); ok {
					b.Fatal("adaptive absent fixture collided with a live key")
				}
				if _, _, _, ok := current.LookupHashed(
					hashes[index], keys[index][:],
				); ok {
					b.Fatal("current absent fixture collided with a live key")
				}
			}
			b.Run("Adaptive/Prehashed", func(b *testing.B) {
				b.ReportMetric(float64(adaptive.StashLen()), "stash_rows")
				b.ReportAllocs()
				for index := range b.N {
					at := index & 255
					adaptiveOrderedLeafLabBenchmarkSlot,
						adaptiveOrderedLeafLabBenchmarkBytes, _,
						adaptiveOrderedLeafLabBenchmarkBool =
						adaptive.LookupHashed(hashes[at], keys[at][:])
				}
			})
			b.Run("Current/Prehashed", func(b *testing.B) {
				b.ReportMetric(float64(len(currentRecords)), "live_rows")
				b.ReportAllocs()
				for index := range b.N {
					at := index & 255
					adaptiveOrderedLeafLabBenchmarkSlot,
						adaptiveOrderedLeafLabBenchmarkBytes, _,
						adaptiveOrderedLeafLabBenchmarkBool =
						current.LookupHashed(hashes[at], keys[at][:])
				}
			})
		})
	}
}

func BenchmarkAdaptiveOrderedLeafLabLowerBound(b *testing.B) {
	_, view := openAdaptiveOrderedLeafLabBenchmark(
		b, AdaptiveOrderedLeafLabNarrow,
		AdaptiveOrderedLeafLabNarrowLiveTarget, 8,
	)
	key := []byte("k0000100")
	b.ReportAllocs()
	for range b.N {
		adaptiveOrderedLeafLabBenchmarkInt = view.LowerBound(key)
	}
}
