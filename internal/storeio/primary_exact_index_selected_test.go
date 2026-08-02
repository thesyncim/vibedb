package storeio

import (
	"bytes"
	"fmt"
	"testing"
)

func selectedPostingTestMask(
	t testing.TB,
	page []byte,
	ranks []int,
) [4]uint64 {
	t.Helper()
	view, ok := AdmittedCompactPrimaryStripe(
		page, unifiedTestStoreID(), 0,
	)
	if !ok {
		t.Fatal("AdmittedCompactPrimaryStripe")
	}
	var selected [4]uint64
	for _, rank := range ranks {
		if rank < 0 || rank >= view.Len() {
			t.Fatalf("rank %d outside [0,%d)", rank, view.Len())
		}
		slot, ok := view.PostingSlot(rank)
		if !ok {
			t.Fatalf("posting slot at rank %d", rank)
		}
		selected[slot>>6] |= uint64(1) << uint(slot&63)
	}
	return selected
}

func encodeSelectedPostingTestLeaf(
	t testing.TB,
	records []CommonPrimaryLeafRecord,
) ([]byte, int) {
	t.Helper()
	storeID := unifiedTestStoreID()
	builder := NewUnifiedPrimaryLeafBuilder()
	count := min(len(records), CommonPrimaryLeafWideSlots)
	if err := PlaceCommonPrimaryLeafRecords(CommonPrimaryLeafWide, storeID, records[:count]); err != nil {
		t.Fatal(err)
	}
	payload, err := BuildCompactPrimaryStripePayload(records[:count], builder)
	if err != nil {
		t.Fatal(err)
	}
	need := PageHeaderSize + len(payload) + PageTrailerSize
	extent := (need + int(physicalPageQuantum) - 1) &^ (int(physicalPageQuantum) - 1)
	page, err := EncodeCompactPrimaryStripe(
		make([]byte, extent),
		CommonPrimaryLeafHeader{
			StoreID: storeID, Generation: 1, Bucket: 0,
			PageSize: uint32(extent),
		},
		records[:count], builder,
	)
	if err != nil {
		t.Fatal(err)
	}
	return page, count
}

func TestVisitPrimaryLeafSelectedPostingRowsMatchesFilteredFullVisit(t *testing.T) {
	records, _ := unifiedTestCorpus(200)
	page, count := encodeSelectedPostingTestLeaf(t, records)
	for _, wanted := range []int{0, 1, 4, 16, count} {
		t.Run(fmt.Sprintf("%d_rows", wanted), func(t *testing.T) {
			ranks := make([]int, 0, wanted)
			for i := 0; i < wanted; i++ {
				ranks = append(ranks, i*count/max(wanted, 1))
			}
			selected := selectedPostingTestMask(t, page, ranks)
			// Dead directory bits must remain harmless even when a caller has no
			// exact-index epoch available to pre-validate liveness.
			view, ok := AdmittedCompactPrimaryStripe(
				page, unifiedTestStoreID(), 0,
			)
			if !ok {
				t.Fatal("AdmittedCommonPrimaryUnifiedLeaf")
			}
			for slot := 0; slot < CommonPrimaryLeafWideSlots; slot++ {
				if _, occupied := view.RankAtSlot(uint8(slot)); !occupied {
					selected[slot>>6] |= uint64(1) << uint(slot&63)
					break
				}
			}

			var want, got [][]byte
			fullScratch := make([]byte, 0, 4096)
			var err error
			_, err = VisitPrimaryLeafPostingRows(
				page, unifiedTestStoreID(), 0, unifiedTestBounds(),
				fullScratch,
				func(slot uint8, key, raw []byte, _ bool) error {
					if selected[slot>>6]&(uint64(1)<<uint(slot&63)) != 0 {
						want = append(want, bytes.Clone(append(key, raw...)))
					}
					return nil
				},
			)
			if err != nil {
				t.Fatal(err)
			}
			selectedScratch := make([]byte, 0, 4096)
			_, err = VisitPrimaryLeafSelectedPostingRows(
				page, unifiedTestStoreID(), 0, unifiedTestBounds(),
				selected, selectedScratch,
				func(_ uint8, key, raw []byte, _ bool) error {
					got = append(got, bytes.Clone(append(key, raw...)))
					return nil
				},
			)
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != len(want) {
				t.Fatalf("selected rows = %d, want %d", len(got), len(want))
			}
			for i := range want {
				if !bytes.Equal(got[i], want[i]) {
					t.Fatalf("row %d differs", i)
				}
			}
		})
	}
}

var selectedPostingBenchmarkBytes int

func BenchmarkVisitPrimaryLeafSelectedPostingRows(b *testing.B) {
	records, _ := unifiedTestCorpus(200)
	page, count := encodeSelectedPostingTestLeaf(b, records)
	for _, wanted := range []int{1, 4, 16, count} {
		ranks := make([]int, wanted)
		for i := range wanted {
			ranks[i] = i * count / wanted
		}
		selected := selectedPostingTestMask(b, page, ranks)
		b.Run(fmt.Sprintf("selected_%d", wanted), func(b *testing.B) {
			scratch := make([]byte, 0, 4096)
			total := 0
			visit := func(_ uint8, key, raw []byte, _ bool) error {
				total += len(key) + len(raw)
				return nil
			}
			b.ReportAllocs()
			for b.Loop() {
				var err error
				scratch, err = VisitPrimaryLeafSelectedPostingRows(
					page, unifiedTestStoreID(), 0, unifiedTestBounds(),
					selected, scratch[:0], visit,
				)
				if err != nil {
					b.Fatal(err)
				}
			}
			selectedPostingBenchmarkBytes = total
		})
		b.Run(fmt.Sprintf("render_all_filter_%d", wanted), func(b *testing.B) {
			scratch := make([]byte, 0, 4096)
			total := 0
			visit := func(slot uint8, key, raw []byte, _ bool) error {
				if selected[slot>>6]&(uint64(1)<<uint(slot&63)) != 0 {
					total += len(key) + len(raw)
				}
				return nil
			}
			b.ReportAllocs()
			for b.Loop() {
				var err error
				scratch, err = VisitPrimaryLeafPostingRows(
					page, unifiedTestStoreID(), 0, unifiedTestBounds(),
					scratch[:0], visit,
				)
				if err != nil {
					b.Fatal(err)
				}
			}
			selectedPostingBenchmarkBytes = total
		})
	}
}
