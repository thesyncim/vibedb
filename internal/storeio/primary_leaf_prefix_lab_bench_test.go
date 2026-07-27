package storeio

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"testing"
)

// Run with:
//   go test ./internal/storeio -run '^$' -bench '^BenchmarkPrimaryLeafPrefixLab' -benchmem -benchtime=150ms -count=5
//
// M4 Max, Go 1.26.5, 195 rows, 8 KiB narrow-slot-class extent; medians of
// five runs. Space is structural plus key bytes and excludes values.
//
// family   R   whole B/key   prefix B/key   saved
// dense    4       17.07          9.882      42.10%
// dense    8       17.07          8.559      49.85%
// dense   16       17.07          7.892      53.76%
// uuidv7   4       21.07         18.32       13.02%
// uuidv7   8       21.07         17.71       15.94%
// uuidv7  16       21.07         17.40       17.41%
// random   4       21.07         21.83       -3.627%
// random   8       21.07         21.81       -3.530%
// random  16       21.07         21.79       -3.432%
//
// Latency columns are prefix/whole: hit ns, miss ns, iteration ns/doc, and
// lower-bound ns.
//
// family   R       hit        miss       iteration       lower-bound
// dense    4   52.29/42.12  68.87/69.51  6.267/4.752   366.8/178.0
// dense    8   51.46/41.51  68.98/68.86  6.299/4.844   509.8/184.3
// dense   16   51.77/41.85  70.93/70.12  6.731/4.865   833.0/183.1
// uuidv7   4   62.21/52.20  69.70/70.58  5.581/4.836   383.7/187.3
// uuidv7   8   64.98/55.45  71.67/72.71  5.883/4.733   513.4/192.2
// uuidv7  16   64.87/53.57  74.22/69.31  5.393/4.652   800.1/179.8
// random   4   63.07/53.95  70.64/68.89  5.599/4.776   377.9/186.9
// random   8   65.40/55.88  71.94/70.95  5.793/4.826   507.5/185.8
// random  16   65.31/55.07  71.36/70.70  5.783/4.758   830.8/187.5
//
// Verdict against hit <=45 ns, miss <=55 ns, iteration <=6 ns/doc: no
// combination promotes. Dense and uuidv7 save bytes, and uuidv7/random pass
// iteration, but every prefix combination fails both point-operation gates.
// Random keys also lose 3.4-3.6% space, so truncation does not pay there.

var (
	primaryLeafPrefixLabBenchBytes    []byte
	primaryLeafPrefixLabBenchSlot     uint8
	primaryLeafPrefixLabBenchBool     bool
	primaryLeafPrefixLabBenchInt      int
	primaryLeafPrefixLabBenchCommon   CommonPrimaryLeafView
	primaryLeafPrefixLabBenchIterator PrimaryLeafPrefixLabIterator
)

type primaryLeafPrefixLabFamily struct {
	name string
	keys [][]byte
}

func primaryLeafPrefixLabFamilies(count int) []primaryLeafPrefixLabFamily {
	dense := primaryLeafPrefixLabDenseKeys(count)
	mixed := make([][]byte, count)
	randomKeys := make([][]byte, count)
	rng := rand.New(rand.NewSource(0x5eed))
	for i := 0; i < count; i++ {
		// UUID-v7-shaped, time-ordered 16-byte binary keys: a common 48-bit
		// millisecond region followed by deterministic mixed entropy.
		key := make([]byte, 16)
		binary.BigEndian.PutUint64(key[:8], uint64(0x019800000000+i)<<16)
		binary.BigEndian.PutUint64(key[8:], uint64(i)*0x9e3779b97f4a7c15)
		mixed[i] = key
		key = make([]byte, 16)
		_, _ = rng.Read(key)
		randomKeys[i] = key
	}
	// The random family must be lexical before placement/encoding.
	slicesSortBytes(randomKeys)
	return []primaryLeafPrefixLabFamily{
		{"Dense", dense}, {"UUIDv7", mixed}, {"Random16", randomKeys},
	}
}

func slicesSortBytes(keys [][]byte) {
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && string(keys[j]) < string(keys[j-1]); j-- {
			keys[j], keys[j-1] = keys[j-1], keys[j]
		}
	}
}

func primaryLeafPrefixLabBenchFixture(
	b testing.TB, keys [][]byte, restart int,
) ([]CommonPrimaryLeafRecord, PrimaryLeafPrefixLabView, CommonPrimaryLeafView) {
	b.Helper()
	records := primaryLeafPrefixLabRecords(b, keys)
	image, err := EncodePrimaryLeafPrefixLab(
		commonPrimaryLeafTestSeed, restart, records,
	)
	if err != nil {
		b.Fatal(err)
	}
	view, err := OpenPrimaryLeafPrefixLab(image, commonPrimaryLeafTestSeed)
	if err != nil {
		b.Fatal(err)
	}
	_, common, _, _ := commonPrimaryLeafOpenTest(
		b, CommonPrimaryLeafNarrow, primaryLeafPrefixLabExtent, records,
	)
	return records, view, common
}

func BenchmarkPrimaryLeafPrefixLabSpace(b *testing.B) {
	const count = CommonPrimaryLeafNarrowLive
	for _, family := range primaryLeafPrefixLabFamilies(count) {
		wholeKeyBytes := 0
		for _, key := range family.keys {
			wholeKeyBytes += len(key)
		}
		wholeStructural := CommonPrimaryLeafStructuralBytes(
			CommonPrimaryLeafNarrow, count, primaryLeafPrefixLabExtent,
		)
		whole := wholeStructural + wholeKeyBytes
		for _, restart := range []int{4, 8, 16} {
			records := primaryLeafPrefixLabRecords(b, family.keys)
			image, err := EncodePrimaryLeafPrefixLab(
				commonPrimaryLeafTestSeed, restart, records,
			)
			if err != nil {
				b.Fatal(err)
			}
			valueBytes := 3 * count
			prefixStructuralAndKeys :=
				PageHeaderSize + len(image) - valueBytes + PageTrailerSize
			saved := float64(whole-prefixStructuralAndKeys) /
				float64(whole) * 100
			b.Run(fmt.Sprintf("%s/R%d", family.name, restart), func(b *testing.B) {
				b.ReportMetric(float64(whole)/count, "whole-B/key")
				b.ReportMetric(
					float64(prefixStructuralAndKeys)/count,
					"prefix-B/key",
				)
				b.ReportMetric(saved, "saved-%")
			})
		}
	}
}

func BenchmarkPrimaryLeafPrefixLab(b *testing.B) {
	const count = CommonPrimaryLeafNarrowLive
	miss := []byte("not-present-prefix-lab-key")
	for _, family := range primaryLeafPrefixLabFamilies(count) {
		for _, restart := range []int{4, 8, 16} {
			records, view, common := primaryLeafPrefixLabBenchFixture(
				b, family.keys, restart,
			)
			target := records[len(records)/2].Key
			lower := records[len(records)/2].Key
			name := fmt.Sprintf("%s/R%d", family.name, restart)
			b.Run(name+"/Prefix/Hit", func(b *testing.B) {
				b.ReportAllocs()
				for range b.N {
					primaryLeafPrefixLabBenchSlot,
						primaryLeafPrefixLabBenchBytes,
						primaryLeafPrefixLabBenchBool, _ = view.Lookup(target)
				}
			})
			b.Run(name+"/Whole/Hit", func(b *testing.B) {
				b.ReportAllocs()
				for range b.N {
					primaryLeafPrefixLabBenchSlot,
						primaryLeafPrefixLabBenchBytes,
						primaryLeafPrefixLabBenchBool, _ = common.LookupRaw(target)
				}
			})
			b.Run(name+"/Prefix/Miss", func(b *testing.B) {
				b.ReportAllocs()
				for range b.N {
					primaryLeafPrefixLabBenchSlot,
						primaryLeafPrefixLabBenchBytes,
						primaryLeafPrefixLabBenchBool, _ = view.Lookup(miss)
				}
			})
			b.Run(name+"/Whole/Miss", func(b *testing.B) {
				b.ReportAllocs()
				for range b.N {
					primaryLeafPrefixLabBenchSlot,
						primaryLeafPrefixLabBenchBytes,
						primaryLeafPrefixLabBenchBool, _ = common.LookupRaw(miss)
				}
			})
			b.Run(name+"/Prefix/Iteration", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					it := view.AllRows()
					for {
						key, value, overflow, ok := it.NextRawBorrowed()
						if !ok {
							break
						}
						primaryLeafPrefixLabBenchInt = len(key) + len(value)
						primaryLeafPrefixLabBenchBool = overflow
					}
				}
				b.ReportMetric(
					float64(b.Elapsed().Nanoseconds())/
						float64(int64(b.N)*count),
					"ns/doc",
				)
			})
			b.Run(name+"/Whole/Iteration", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					it := common.AllRows()
					for {
						key, value, overflow, ok := it.NextRawBorrowed()
						if !ok {
							break
						}
						primaryLeafPrefixLabBenchInt = len(key) + len(value)
						primaryLeafPrefixLabBenchBool = overflow
					}
				}
				b.ReportMetric(
					float64(b.Elapsed().Nanoseconds())/
						float64(int64(b.N)*count),
					"ns/doc",
				)
			})
			b.Run(name+"/Prefix/LowerBound", func(b *testing.B) {
				b.ReportAllocs()
				for range b.N {
					primaryLeafPrefixLabBenchInt = view.LowerBound(lower)
				}
			})
			b.Run(name+"/Whole/LowerBound", func(b *testing.B) {
				b.ReportAllocs()
				for range b.N {
					primaryLeafPrefixLabBenchInt = common.LowerBound(lower)
				}
			})
		}
	}
}
