package vnext

import (
	"fmt"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
)

var postingTileBenchmarkSink uint64

func BenchmarkTermPostingTileIteration(b *testing.B) {
	for _, test := range postingTileCases() {
		if test.name == "empty" {
			continue
		}
		b.Run(test.name, func(b *testing.B) {
			var component [storeio.TermPostingMaxPayloadBytes]byte
			record, n, err := storeio.BuildTermPosting(
				component[:], 77, &test.posting, &test.live,
			)
			if err != nil {
				b.Fatal(err)
			}
			view, err := storeio.OpenTermPosting(
				record, component[:n:n], &test.live,
			)
			if err != nil {
				b.Fatal(err)
			}
			currentBytes := nonemptyPostingChunks(&test.posting) * 32
			nextBytes := record.ReachableBytes()
			b.ReportAllocs()
			b.SetBytes(int64(nextBytes))
			b.ResetTimer()
			for b.Loop() {
				checksum := uint64(0)
				iterator := view.Iterator()
				for {
					mask, ok := iterator.Next()
					if !ok {
						break
					}
					checksum ^= uint64(mask.Chunk)<<56 ^ mask.Bits
				}
				postingTileBenchmarkSink = checksum
			}
			b.ReportMetric(float64(currentBytes), "current-B")
			b.ReportMetric(float64(nextBytes), "vnext-B")
			b.ReportMetric(
				100*(1-float64(nextBytes)/float64(currentBytes)),
				"space-saved-%",
			)
		})
	}
}

func BenchmarkTermPostingTileBuildOpen(b *testing.B) {
	for _, test := range postingTileCases() {
		b.Run(fmt.Sprintf("%s/%v", test.name, test.codec), func(b *testing.B) {
			var component [storeio.TermPostingMaxPayloadBytes]byte
			var record storeio.TermPosting
			var n int
			var view storeio.TermPostingView
			var err error
			b.ReportAllocs()
			for b.Loop() {
				record, n, err = storeio.BuildTermPosting(
					component[:], 77, &test.posting, &test.live,
				)
				if err != nil {
					b.Fatal(err)
				}
				view, err = storeio.OpenTermPosting(
					record, component[:n:n], &test.live,
				)
				if err != nil || view.Rows() != record.Rows {
					b.Fatal("round trip")
				}
			}
		})
	}
}

// BenchmarkCurrentPostingChunkWords isolates the best possible resident loop
// over the current durable representation. It deliberately charges no B+tree
// or 32-byte record decode cost; the vNext iterator must be interpreted
// against this lower bound and the separately reported page-space reduction.
func BenchmarkCurrentPostingChunkWords(b *testing.B) {
	for _, test := range postingTileCases() {
		if test.name == "empty" {
			continue
		}
		b.Run(test.name, func(b *testing.B) {
			currentBytes := nonemptyPostingChunks(&test.posting) * 32
			b.ReportMetric(float64(currentBytes), "current-B")
			b.ReportAllocs()
			for b.Loop() {
				checksum := uint64(0)
				for chunk, mask := range test.posting {
					if mask != 0 {
						checksum ^= uint64(chunk)<<56 ^ mask
					}
				}
				postingTileBenchmarkSink = checksum
			}
		})
	}
}

// BenchmarkCurrentIndexLeafIteration measures the actual admitted 32-byte
// durable leaf records for the same masks. It still excludes B+tree descent,
// certificate copying, and cross-leaf continuation, so it is a conservative
// resident baseline for the adaptive iterator.
func BenchmarkCurrentIndexLeafIteration(b *testing.B) {
	for _, test := range postingTileCases() {
		if test.name == "empty" {
			continue
		}
		b.Run(test.name, func(b *testing.B) {
			entries := make(
				[]storeio.IndexDirectoryEntry, 0,
				nonemptyPostingChunks(&test.posting),
			)
			for chunk, mask := range test.posting {
				if mask == 0 {
					continue
				}
				entries = append(entries, storeio.IndexDirectoryEntry{
					Key: storeio.IndexDirectoryKey{
						IndexID: 0, TupleHash: 1, Chunk: uint32(chunk),
					},
					Bits: mask, Kind: storeio.IndexEntryInlineMask,
				})
			}
			page, err := storeio.EncodeIndexDirectoryLeaf(
				make([]byte, 4096),
				storeio.IndexDirectoryHeader{
					StoreID:    testIdentity.StoreID,
					Generation: testIdentity.Generation,
					LogicalID:  testIdentity.LogicalID,
					PageSize:   4096,
				},
				entries, nil, testIdentity.LogicalID+1, 1,
			)
			if err != nil {
				b.Fatal(err)
			}
			view, err := storeio.OpenIndexDirectoryPage(
				page, 1<<20, testIdentity.LogicalID+1, 1,
			)
			if err != nil {
				b.Fatal(err)
			}
			currentBytes := len(entries) * storeio.IndexDirectoryLeafRecordSize
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				checksum := uint64(0)
				for rank := 0; rank < view.Len(); rank++ {
					entry, ok := view.EntryAt(rank)
					if !ok {
						b.Fatal("entry")
					}
					checksum ^= uint64(entry.Key.Chunk)<<56 ^ entry.Bits
				}
				postingTileBenchmarkSink = checksum
			}
			b.ReportMetric(float64(currentBytes), "current-B")
		})
	}
}
