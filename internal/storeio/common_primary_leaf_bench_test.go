package storeio

import (
	"testing"
)

var (
	commonPrimaryLeafBenchmarkSlot     uint8
	commonPrimaryLeafBenchmarkBytes    []byte
	commonPrimaryLeafBenchmarkValue    CommonPrimaryLeafValue
	commonPrimaryLeafBenchmarkRow      CommonPrimaryLeafRow
	commonPrimaryLeafBenchmarkView     CommonPrimaryLeafView
	commonPrimaryLeafBenchmarkPage     []byte
	commonPrimaryLeafBenchmarkBool     bool
	commonPrimaryLeafBenchmarkOverflow bool
	commonPrimaryLeafBenchmarkInt      int
)

func commonPrimaryLeafBenchmarkFixture(
	b testing.TB,
) ([]CommonPrimaryLeafRecord, []byte, CommonPrimaryLeafView, PageRef, CommonPrimaryLeafBounds) {
	b.Helper()
	records := commonPrimaryLeafTestRecords(
		b, CommonPrimaryLeafNarrow,
		CommonPrimaryLeafNarrowLive, 8,
	)
	page, view, ref, bounds := commonPrimaryLeafOpenTest(
		b, CommonPrimaryLeafNarrow, 4<<10, records,
	)
	return records, page, view, ref, bounds
}

func commonPrimaryLeafAdaptiveBenchmarkFixture(
	b testing.TB,
	records []CommonPrimaryLeafRecord,
) AdaptiveOrderedLeafLabView {
	b.Helper()
	adaptive := make([]AdaptiveOrderedLeafLabRecord, len(records))
	for index := range records {
		adaptive[index] = AdaptiveOrderedLeafLabRecord{
			Key: records[index].Key, Value: records[index].Value.Inline,
		}
	}
	if err := PlaceAdaptiveOrderedLeafLabRecords(
		AdaptiveOrderedLeafLabNarrow,
		commonPrimaryLeafTestSeed, adaptive,
	); err != nil {
		b.Fatal(err)
	}
	page, err := EncodeAdaptiveOrderedLeafLab(
		make([]byte, AdaptiveOrderedLeafLabNarrowBytes),
		AdaptiveOrderedLeafLabNarrow,
		AdaptiveOrderedLeafLabHeader{BucketID: 991, Generation: 17},
		commonPrimaryLeafTestSeed, adaptive,
	)
	if err != nil {
		b.Fatal(err)
	}
	view, err := OpenAdaptiveOrderedLeafLab(
		page, commonPrimaryLeafTestSeed,
	)
	if err != nil {
		b.Fatal(err)
	}
	return view
}

func BenchmarkCommonPrimaryLeafPointLookup(b *testing.B) {
	records, _, view, _, _ := commonPrimaryLeafBenchmarkFixture(b)
	var normal, stash CommonPrimaryLeafRecord
	for _, record := range records {
		if int(record.Slot) < CommonPrimaryLeafNormalSlots {
			normal = record
		} else {
			stash = record
			break
		}
	}
	if normal.Key == nil || stash.Key == nil {
		b.Fatal("fixture lacks normal/stash")
	}
	miss := []byte("not-here")
	for _, test := range []struct {
		name string
		key  []byte
	}{
		{"NormalHit", normal.Key},
		{"StashHit", stash.Key},
		{"Miss", miss},
	} {
		hash := commonPrimaryLeafHash(
			commonPrimaryLeafTestSeed, test.key,
		)
		b.Run(test.name+"/Prehashed", func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				commonPrimaryLeafBenchmarkSlot,
					commonPrimaryLeafBenchmarkBytes,
					commonPrimaryLeafBenchmarkOverflow,
					commonPrimaryLeafBenchmarkBool =
					view.LookupRawHashed(hash, test.key)
			}
		})
		b.Run(test.name+"/HashIncluded", func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				commonPrimaryLeafBenchmarkSlot,
					commonPrimaryLeafBenchmarkBytes,
					commonPrimaryLeafBenchmarkOverflow,
					commonPrimaryLeafBenchmarkBool =
					view.LookupRaw(test.key)
			}
		})
	}

	hashes := make([]uint64, len(records))
	for index := range records {
		hashes[index] = commonPrimaryLeafHash(
			commonPrimaryLeafTestSeed, records[index].Key,
		)
	}
	b.Run("AllKeyAverage/Prehashed", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(records)))
		b.ResetTimer()
		for range b.N {
			for index := range records {
				commonPrimaryLeafBenchmarkSlot,
					commonPrimaryLeafBenchmarkBytes,
					commonPrimaryLeafBenchmarkOverflow,
					commonPrimaryLeafBenchmarkBool =
					view.LookupRawHashed(hashes[index], records[index].Key)
			}
		}
		b.StopTimer()
		b.ReportMetric(
			float64(b.Elapsed().Nanoseconds())/
				float64(int64(b.N)*int64(len(records))),
			"ns/key",
		)
	})
	adaptive := commonPrimaryLeafAdaptiveBenchmarkFixture(b, records)
	adaptiveMissHash := adaptiveOrderedLeafLabKeyHash(
		commonPrimaryLeafTestSeed, miss,
	)
	b.Run("FairAdaptive/Miss/Prehashed", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			commonPrimaryLeafBenchmarkSlot,
				commonPrimaryLeafBenchmarkBytes,
				commonPrimaryLeafBenchmarkOverflow,
				commonPrimaryLeafBenchmarkBool =
				adaptive.LookupHashed(adaptiveMissHash, miss)
		}
	})
}

func BenchmarkCommonPrimaryLeafScan(b *testing.B) {
	records, _, view, _, _ := commonPrimaryLeafBenchmarkFixture(b)
	adaptive := commonPrimaryLeafAdaptiveBenchmarkFixture(b, records)
	b.Run("CommonPage", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(records)))
		b.ResetTimer()
		for range b.N {
			it := view.AllRows()
			for {
				key, value, overflow, ok := it.NextRawBorrowed()
				if !ok {
					break
				}
				commonPrimaryLeafBenchmarkBytes = value
				commonPrimaryLeafBenchmarkOverflow = overflow
				commonPrimaryLeafBenchmarkInt = len(key)
			}
		}
		b.StopTimer()
		b.ReportMetric(
			float64(b.Elapsed().Nanoseconds())/
				float64(int64(b.N)*int64(len(records))),
			"ns/doc",
		)
	})
	b.Run("AdaptiveLab", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(records)))
		b.ResetTimer()
		for range b.N {
			it := adaptive.AllRows()
			for {
				key, value, overflow, ok := it.NextBorrowed()
				if !ok {
					break
				}
				commonPrimaryLeafBenchmarkValue =
					CommonPrimaryLeafValue{Inline: value}
				commonPrimaryLeafBenchmarkBool = overflow
				commonPrimaryLeafBenchmarkInt = len(key)
			}
		}
		b.StopTimer()
		b.ReportMetric(
			float64(b.Elapsed().Nanoseconds())/
				float64(int64(b.N)*int64(len(records))),
			"ns/doc",
		)
	})
}

func BenchmarkCommonPrimaryLeafOpen(b *testing.B) {
	_, page, _, ref, bounds := commonPrimaryLeafBenchmarkFixture(b)
	b.Run("4KiB/Narrow195", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(page)))
		for range b.N {
			var err error
			commonPrimaryLeafBenchmarkView, err =
				OpenCommonPrimaryLeaf(
					page, commonPrimaryLeafTestSeed, 991,
					ref, ref.Generation, bounds,
				)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	representative := commonPrimaryLeafTestRecords(
		b, CommonPrimaryLeafNarrow, 195, 249,
	)
	largePage, _, largeRef, largeBounds :=
		commonPrimaryLeafOpenTest(
			b, CommonPrimaryLeafNarrow, 64<<10, representative,
		)
	b.Run("64KiB/Narrow195/249BValue", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(largePage)))
		for range b.N {
			var err error
			commonPrimaryLeafBenchmarkView, err =
				OpenCommonPrimaryLeaf(
					largePage, commonPrimaryLeafTestSeed, 991,
					largeRef, largeRef.Generation, largeBounds,
				)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkCommonPrimaryLeafMutation(b *testing.B) {
	records, _, view, ref, _ := commonPrimaryLeafBenchmarkFixture(b)
	target := records[len(records)/2]
	replacement := CommonPrimaryLeafValue{Inline: []byte("changed!")}
	b.Run("EqualLengthUpdate", func(b *testing.B) {
		dst := make([]byte, 4<<10)
		b.ReportAllocs()
		b.SetBytes(4 << 10)
		b.ResetTimer()
		for range b.N {
			var err error
			commonPrimaryLeafBenchmarkPage, err = view.UpdateTo(
				dst, ref.Generation+1, target.Slot, target.Key, replacement,
			)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("Delete", func(b *testing.B) {
		dst := make([]byte, 4<<10)
		b.ReportAllocs()
		b.SetBytes(4 << 10)
		b.ResetTimer()
		for range b.N {
			var err error
			commonPrimaryLeafBenchmarkPage, err = view.DeleteTo(
				dst, ref.Generation+1, target.Slot, target.Key,
			)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	deletedPage, err := view.DeleteTo(
		make([]byte, 4<<10), ref.Generation+1, target.Slot, target.Key,
	)
	if err != nil {
		b.Fatal(err)
	}
	deletedRef := ref
	deletedRef.Generation++
	deleted, err := OpenCommonPrimaryLeaf(
		deletedPage, commonPrimaryLeafTestSeed, 991,
		deletedRef, deletedRef.Generation, view.bounds,
	)
	if err != nil {
		b.Fatal(err)
	}
	b.Run("Insert", func(b *testing.B) {
		dst := make([]byte, 4<<10)
		b.ReportAllocs()
		b.SetBytes(4 << 10)
		b.ResetTimer()
		for range b.N {
			var err error
			commonPrimaryLeafBenchmarkPage,
				commonPrimaryLeafBenchmarkSlot, err =
				deleted.InsertTo(
					dst, deletedRef.Generation+1,
					target.Key, target.Value,
				)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("StableSlotRestore", func(b *testing.B) {
		dst := make([]byte, 4<<10)
		b.ReportAllocs()
		b.SetBytes(4 << 10)
		b.ResetTimer()
		for range b.N {
			var err error
			commonPrimaryLeafBenchmarkPage, err =
				deleted.InsertSlotTo(
					dst, deletedRef.Generation+1, target.Slot,
					target.Key, target.Value,
				)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
