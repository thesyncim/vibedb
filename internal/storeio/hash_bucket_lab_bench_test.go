package storeio

import (
	"bytes"
	"fmt"
	"testing"
)

func hashBucketLabBenchmarkRecords(count, valueLength int) []HashBucketLabRecord {
	records := make([]HashBucketLabRecord, 0, count)
	for index := 0; index < count; index++ {
		key := []byte(fmt.Sprintf("key-%03d", index))
		value := bytes.Repeat([]byte{byte(index), byte(index >> 8)}, (valueLength+1)/2)
		value = value[:valueLength]
		records = append(records, HashBucketLabRecord{
			Key:   key,
			Value: value,
		})
	}
	if err := PlaceHashBucketLabRecords(hashBucketLabTestSeed, records); err != nil {
		panic(err)
	}
	return records
}

func hashBucketLabBenchmarkFalseTagMiss(view HashBucketLabView) ([]byte, uint64, int) {
	var best []byte
	var bestHash uint64
	bestMatches := -1
	for candidate := 0; candidate < 10000; candidate++ {
		key := []byte(fmt.Sprintf("mis%04d", candidate))
		hash := KeyHashBytes(hashBucketLabTestSeed, key)
		matches := 0
		tagAndLive := uint32(hash>>58)<<22 | hashBucketLabLiveFlag
		for ordinal := 0; ordinal < 32; ordinal++ {
			slot := hashBucketLabCandidate(hash, ordinal)
			descriptor := hashBucketLabDescriptor(view.page, slot)
			if descriptor&(hashBucketLabTagMask|hashBucketLabLiveFlag) == tagAndLive &&
				int(descriptor&hashBucketLabKeyMask)>>14 == len(key) {
				matches++
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

func openHashBucketLabBenchmark(b *testing.B, records []HashBucketLabRecord) HashBucketLabView {
	b.Helper()
	page := encodeHashBucketLabTestPage(b, records)
	view, err := OpenHashBucketLab(page)
	if err != nil {
		b.Fatal(err)
	}
	return view
}

func BenchmarkHashBucketLabPointLookup(b *testing.B) {
	records := hashBucketLabBenchmarkRecords(244, 32)
	view := openHashBucketLabBenchmark(b, records)
	hit := records[len(records)/2].Key
	hitHash := KeyHashBytes(hashBucketLabTestSeed, hit)
	miss := []byte("missing-key")
	missHash := KeyHashBytes(hashBucketLabTestSeed, miss)
	falseTagMiss, falseTagHash, falseTagMatches :=
		hashBucketLabBenchmarkFalseTagMiss(view)

	b.Run("Hit/Prehashed", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			hashBucketLabBenchmarkSlot, hashBucketLabBenchmarkBytes, _, _ =
				view.LookupHashed(hitHash, hit)
		}
	})
	b.Run("Hit/HashIncluded", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			hashBucketLabBenchmarkSlot, hashBucketLabBenchmarkBytes, _, _ =
				view.Lookup(hit)
		}
	})
	b.Run("Miss/Prehashed", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			hashBucketLabBenchmarkSlot, hashBucketLabBenchmarkBytes, _, _ =
				view.LookupHashed(missHash, miss)
		}
	})
	b.Run("Miss/HashIncluded", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			hashBucketLabBenchmarkSlot, hashBucketLabBenchmarkBytes, _, _ =
				view.Lookup(miss)
		}
	})
	b.Run(fmt.Sprintf("Miss/FalseTagCollisions%d", falseTagMatches), func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			hashBucketLabBenchmarkSlot, hashBucketLabBenchmarkBytes, _, _ =
				view.LookupHashed(falseTagHash, falseTagMiss)
		}
	})
}

func BenchmarkHashBucketLabStableSlotScan(b *testing.B) {
	records := hashBucketLabBenchmarkRecords(244, 32)
	view := openHashBucketLabBenchmark(b, records)
	b.ReportAllocs()
	b.SetBytes(int64(len(records)))
	b.ResetTimer()
	for range b.N {
		iterator := view.AllRows()
		for {
			_, slot, _, value, _, ok := iterator.NextBorrowed()
			if !ok {
				break
			}
			hashBucketLabBenchmarkSlot = slot
			hashBucketLabBenchmarkBytes = value
		}
	}
	b.StopTimer()
	b.ReportMetric(
		float64(b.Elapsed().Nanoseconds())/float64(int64(b.N)*int64(len(records))),
		"ns/doc",
	)
}

func BenchmarkHashBucketLabPageLocalUpdate(b *testing.B) {
	records := hashBucketLabBenchmarkRecords(244, 32)
	target := &records[len(records)/2]

	b.Run("SameLength", func(b *testing.B) {
		view := openHashBucketLabBenchmark(b, records)
		first := bytes.Repeat([]byte{0xa5}, 32)
		second := bytes.Repeat([]byte{0x5a}, 32)
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			value := first
			if index&1 != 0 {
				value = second
			}
			if err := view.Update(target.Slot, target.Key, value, false); err != nil {
				b.Fatal(err)
			}
		}
	})
	for _, test := range []struct {
		name  string
		value []byte
	}{
		{name: "Grow", value: bytes.Repeat([]byte{0x5a}, 48)},
		{name: "Shrink", value: bytes.Repeat([]byte{0xa5}, 16)},
	} {
		b.Run(test.name, func(b *testing.B) {
			original := encodeHashBucketLabTestPage(b, records)
			work := make([]byte, HashBucketLabPageSize)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				b.StopTimer()
				copy(work, original)
				view, err := OpenHashBucketLab(work)
				if err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
				if err := view.Update(
					target.Slot, target.Key, test.value, false,
				); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkHashBucketLabTombstoneFreeDelete(b *testing.B) {
	records := hashBucketLabBenchmarkRecords(244, 32)
	target := &records[len(records)/2]
	original := encodeHashBucketLabTestPage(b, records)
	work := make([]byte, HashBucketLabPageSize)

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		b.StopTimer()
		copy(work, original)
		view, err := OpenHashBucketLab(work)
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		if err := view.Delete(target.Slot, target.Key); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkHashBucketLabMetadataByOccupancy(b *testing.B) {
	tests := []struct {
		name  string
		count int
	}{
		{name: "50pct", count: 128},
		{name: "75pct", count: 192},
		{name: "90pct", count: 231},
		{name: "95pct", count: 244},
		{name: "100pct", count: 256},
	}
	for _, test := range tests {
		b.Run(test.name, func(b *testing.B) {
			records := hashBucketLabBenchmarkRecords(test.count, 8)
			page := make([]byte, HashBucketLabPageSize)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, err := EncodeHashBucketLab(
					page, hashBucketLabTestHeader(), records,
				); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			b.ReportMetric(
				HashBucketLabMetadataBytesPerLiveKey(test.count),
				"metadata_B/live_key",
			)
			b.ReportMetric(
				float64(HashBucketLabPageSize)/float64(test.count),
				"page_B/live_key",
			)
			b.ReportMetric(
				float64(hashBucketLabTrailerStart-HashBucketLabHeapStart)/
					float64(test.count),
				"inline_budget_B/live_key",
			)
			b.ReportMetric(
				HashBucketLabOverflowReferenceSize,
				"overflow_ref_B/key",
			)
			b.ReportMetric(
				100*float64(test.count)/HashBucketLabSlotCount,
				"occupancy_pct",
			)
		})
	}
}
