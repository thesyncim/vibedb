package storeio

import (
	"os"
	"testing"
)

var inlineCodecSink uint64

func BenchmarkStateRootPublication(b *testing.B) {
	for _, inline := range []bool{false, true} {
		name := "external-page"
		if inline {
			name = "inline"
		}
		b.Run(name, func(b *testing.B) {
			file, err := os.CreateTemp(b.TempDir(), "state-root-publication-*")
			if err != nil {
				b.Fatal(err)
			}
			defer file.Close()
			pageSize := uint32(physicalPageQuantum)
			dataStart := testMutableStoreDataStart(pageSize)
			if err := file.Truncate(int64(dataStart)); err != nil {
				b.Fatal(err)
			}
			committer, err := NewCommitter(file, DeviceOptions{
				Backend: BackendPortable, BufferCount: 4,
				BufferSize: max(os.Getpagesize(), int(pageSize)),
			}, CommitterOptions{
				QueueSlots: 4, MaxPagesPerBatch: 1, GroupLimit: 1,
			})
			if err != nil {
				b.Fatal(err)
			}
			defer committer.Close()
			fileEnd := dataStart
			state := StateRoot{
				StoreID: testStoreID, PageSize: pageSize,
				MaxPageSize: pageSize, NextLogicalID: 2, ChunkDocuments: 64,
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				generation := uint64(i + 1)
				tx, err := BeginWriteTransaction(
					committer, nil, btoi(!inline), WriteTransactionOptions{
						StoreID: testStoreID, Generation: generation, PageSize: pageSize,
						FileEnd: fileEnd, NextLogicalID: 2,
					},
				)
				if err != nil {
					b.Fatal(err)
				}
				state.Generation = generation
				if inline {
					if err := tx.PublishInline(state, InlineFreeDelta{}); err != nil {
						b.Fatal(err)
					}
				} else {
					statePage, err := tx.Allocate(PageStateRoot, pageSize, StateRootLogicalID)
					if err != nil {
						b.Fatal(err)
					}
					if _, err := EncodeStateRootPage(statePage.Bytes(), state, tx.FileEnd()); err != nil {
						b.Fatal(err)
					}
					if err := statePage.Stage(); err != nil {
						b.Fatal(err)
					}
					if err := tx.Publish(
						statePage.Ref(), PageChecksum(statePage.Bytes()), 0, 0, 0,
					); err != nil {
						b.Fatal(err)
					}
					fileEnd = tx.FileEnd()
				}
				if err := committer.Wait(generation); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			stats := committer.Stats()
			b.ReportMetric(float64(stats.DeviceBytes)/float64(b.N), "devB/op")
			b.ReportMetric(
				float64(stats.DeviceBytes)/float64(pageSize)/float64(b.N), "pages/op",
			)
		})
	}
}

func btoi(value bool) int {
	if value {
		return 1
	}
	return 0
}

func BenchmarkInlineFreeDeltaCodec(b *testing.B) {
	root := testInlineFreeSuperblock(7, InlineFreeDeltaCapacity+4)
	pageSize := uint64(root.PageSize)
	records := make([]FreeDelta, InlineFreeDeltaCapacity)
	for rank := range records {
		records[rank] = FreeDelta{
			Op:     FreeOpDelete,
			Extent: FreeExtent{Offset: uint64(rank+4) * pageSize},
		}
	}
	if err := root.FreeDelta.Append(records, root.PageSize, root.FileEnd); err != nil {
		b.Fatal(err)
	}
	var encoded [InlineSuperblockSize]byte
	if _, err := EncodeInlineSuperblock(encoded[:], root); err != nil {
		b.Fatal(err)
	}

	b.Run("encode/full", func(b *testing.B) {
		b.ReportAllocs()
		b.ReportMetric(InlineFreeDeltaCapacity, "records/op")
		for i := 0; i < b.N; i++ {
			if _, err := EncodeInlineSuperblock(encoded[:], root); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("decode/full", func(b *testing.B) {
		var generation uint64
		b.ReportAllocs()
		b.ReportMetric(InlineFreeDeltaCapacity, "records/op")
		for i := 0; i < b.N; i++ {
			decoded, err := DecodeInlineSuperblock(encoded[:])
			if err != nil {
				b.Fatal(err)
			}
			generation += decoded.Generation
		}
		inlineCodecSink = generation
	})
	b.Run("append/latest", func(b *testing.B) {
		base := root.FreeDelta
		change := []FreeDelta{
			{Op: FreeOpSet, Extent: FreeExtent{
				Offset: 4 * pageSize, Length: pageSize, RetiredGeneration: 6,
			}},
			{Op: FreeOpDelete, Extent: FreeExtent{Offset: 9 * pageSize}},
		}
		var working InlineFreeDelta
		b.ReportAllocs()
		b.ReportMetric(float64(base.Len()), "base-records/op")
		for i := 0; i < b.N; i++ {
			working = base
			if err := working.Append(change, root.PageSize, root.FileEnd); err != nil {
				b.Fatal(err)
			}
		}
		inlineCodecSink = uint64(working.Len())
	})
}
