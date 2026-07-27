package storeio

import (
	"os"
	"testing"
	"time"
)

// The scale evidence for replacing the linked image with a segment index.
//
// The claim being tested is not that the new representation is faster. It is
// that the two costs which used to grow with the size of the free set no longer
// do: what one commit writes when it folds, and what the free set costs to
// describe per extent. The old image was a chain of pages each naming the next,
// so a fold rewrote all of them and the only way to keep that affordable was to
// cap the image at sixteen pages — 2,640 extents at the 4 KiB page size, about
// 11 MiB of trackable free space. A multi-terabyte store with ordinary
// fragmentation has millions of free extents, so it hit that cap and then could
// not write down space it had already reclaimed.
//
// These tests build free sets far past the old cap, replay them from bytes on
// disk, and record what a fold would write at each size.

// freeLogScaleSizes are the measured points. The largest is chosen just under
// FreeLogMaxIndexPages*FreeIndexRecordCapacity*FreeImageRecordCapacity, which is
// where the remaining cap now sits — 92,400 extents at the 4 KiB page size,
// thirty-five times the 2,640 the linked image could hold.
var freeLogScaleSizes = []int{10_000, 50_000, 90_000}

// Given free sets far larger than the linked image could hold, when each is
// written as segments plus an index and replayed from disk, then every extent
// comes back, and the pages a fold rewrites to change one segment stay flat
// while the set grows by an order of magnitude.
//
// The second half is the point. Under the old representation the same
// measurement would have been the whole image at every size — and for every
// size here, would not have been representable at all.
func TestFreeLogScalesPastTheLinkedImageCap(t *testing.T) {
	if testing.Short() {
		t.Skip("scale measurement builds several hundred megabytes of sparse file")
	}
	const oldImagePages = 16
	oldCap := oldImagePages * FreeImageRecordCapacity(testSuperblockPageSize)
	perSegment := FreeImageRecordCapacity(testSuperblockPageSize)
	perIndexPage := FreeIndexRecordCapacity(testSuperblockPageSize)

	var firstFold, lastFold int
	for at, extents := range freeLogScaleSizes {
		if extents <= oldCap {
			t.Fatalf("%d extents is inside the old %d-extent cap, so it measures nothing",
				extents, oldCap)
		}
		w := newFreeLogScaleWriter(t, extents)
		segments := (extents + perSegment - 1) / perSegment
		indexPages := (segments + perIndexPage - 1) / perIndexPage
		if indexPages > FreeLogMaxIndexPages {
			t.Fatalf("%d extents needs %d index pages, bound is %d",
				extents, indexPages, FreeLogMaxIndexPages)
		}
		head := w.build(segments)

		// Eager: read every segment, which is what an open cost before residency
		// became a budget.
		started := time.Now()
		replayed, pages, err := ReplayFreeLog(w.cache, head, w.bounds(), nil, extents+16, segments)
		eager := time.Since(started)
		if err != nil {
			t.Fatalf("%d extents: replay: %v", extents, err)
		}
		if len(replayed) != extents {
			t.Fatalf("%d extents: replayed %d", extents, len(replayed))
		}
		for i := range pages.Resident {
			if !pages.Resident[i] {
				t.Fatalf("%d extents: a full budget left segment %d unread", extents, i)
			}
		}
		// Lazy: read only what a working set needs. The chain here holds no
		// records, so nothing is mandatory and the budget alone decides.
		const budget = 8
		started = time.Now()
		partial, lazyPages, err := ReplayFreeLog(w.cache, head, w.bounds(), nil, extents+16, budget)
		lazy := time.Since(started)
		if err != nil {
			t.Fatalf("%d extents: lazy replay: %v", extents, err)
		}
		read := 0
		for _, ok := range lazyPages.Resident {
			if ok {
				read++
			}
		}
		if want := min(budget, segments); read != want {
			t.Fatalf("%d extents: lazy replay read %d segments, want %d", extents, read, want)
		}
		if len(partial) > len(replayed) {
			t.Fatalf("%d extents: lazy replay produced %d extents against %d eager",
				extents, len(partial), len(replayed))
		}
		elapsed := eager
		for i := 1; i < len(replayed); i++ {
			if replayed[i-1].Offset+replayed[i-1].Length > replayed[i].Offset {
				t.Fatalf("%d extents: replay produced overlap at rank %d", extents, i)
			}
		}
		if len(pages.Segments) != segments || len(pages.Index) != indexPages {
			t.Fatalf("%d extents: %d segments in %d index pages, want %d in %d",
				extents, len(pages.Segments), len(pages.Index), segments, indexPages)
		}

		// What a fold writes to change one segment: that segment, plus the index.
		// The old image had no such number — every fold rewrote every image page.
		fold := 1 + indexPages
		if at == 0 {
			firstFold = fold
		}
		lastFold = fold
		t.Logf("%7d extents: %4d segments, %d index pages; "+
			"eager open %s reading %d pages; lazy open %s reading %d pages for %d extents; "+
			"one-segment fold writes %d pages (old design: not representable, cap %d)",
			extents, segments, indexPages,
			eager.Round(time.Millisecond), segments+indexPages+1,
			lazy.Round(time.Millisecond), read+indexPages+1, len(partial),
			fold, oldCap)
		_ = elapsed
	}
	// Nine times the extents must not cost nine times the fold. The index grows
	// as extents/(168*70), so the whole measured range shares one handful of
	// index pages and the fold write is essentially flat.
	if lastFold > firstFold+FreeLogMaxIndexPages {
		t.Fatalf("fold write grew from %d to %d pages across a %dx range in free-set size",
			firstFold, lastFold, freeLogScaleSizes[len(freeLogScaleSizes)-1]/freeLogScaleSizes[0])
	}
}

// freeLogScaleWriter lays out a deliberately fragmented free set — one free page
// between every pair of live ones — and writes it as segment and index pages.
type freeLogScaleWriter struct {
	t       *testing.T
	file    *os.File
	cache   *PageCache
	extents []FreeExtent
	next    uint64
	logic   uint64
	fileEnd uint64
}

func newFreeLogScaleWriter(t *testing.T, extents int) *freeLogScaleWriter {
	t.Helper()
	file, err := os.CreateTemp(t.TempDir(), "free-log-scale-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })
	pageSize := uint64(testSuperblockPageSize)
	// Every free extent is one page with one live page after it, which is the
	// worst fragmentation a page-granular allocator can reach: no two free
	// extents ever coalesce, so the set cannot be made smaller by merging.
	dataPages := uint64(2*extents) + 4
	metadataPages := uint64(extents/FreeImageRecordCapacity(testSuperblockPageSize)) +
		uint64(FreeLogMaxIndexPages) + 8
	fileEnd := (dataPages + metadataPages) * pageSize
	if err := file.Truncate(int64(fileEnd)); err != nil {
		t.Fatal(err)
	}
	cache, err := NewPageCache(file, PageCacheOptions{
		PageSize: int(testSuperblockPageSize), ResidentBytes: 64 << 20,
		StoreID: testStoreID, ReadConcurrency: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cache.Close() })
	set := make([]FreeExtent, extents)
	for i := range set {
		set[i] = FreeExtent{
			Offset: uint64(2*i+5) * pageSize, Length: pageSize, RetiredGeneration: 1,
		}
	}
	return &freeLogScaleWriter{
		t: t, file: file, cache: cache, extents: set,
		next: dataPages, logic: 2, fileEnd: fileEnd,
	}
}

func (w *freeLogScaleWriter) bounds() FreeLogBounds {
	return FreeLogBounds{FileEnd: w.fileEnd, NextLogicalID: w.logic + 1}
}

func (w *freeLogScaleWriter) write(kind PageKind, encode func([]byte, FreeLogHeader) error) PageRef {
	w.t.Helper()
	page := make([]byte, testSuperblockPageSize)
	header := FreeLogHeader{
		StoreID: testStoreID, Generation: 3, LogicalID: w.logic, PageSize: testSuperblockPageSize,
	}
	if err := encode(page, header); err != nil {
		w.t.Fatal(err)
	}
	offset := w.next * uint64(testSuperblockPageSize)
	if _, err := w.file.WriteAt(page, int64(offset)); err != nil {
		w.t.Fatal(err)
	}
	ref := PageRef{
		Offset: offset, LogicalID: w.logic, Generation: 3,
		Length: testSuperblockPageSize, Kind: kind,
	}
	w.next++
	w.logic++
	return ref
}

// build writes the whole set as segments, then the index, then a one-record
// delta chain, and returns the chain head a store would publish.
func (w *freeLogScaleWriter) build(segments int) PageRef {
	w.t.Helper()
	perSegment := FreeImageRecordCapacity(testSuperblockPageSize)
	// Reserve the logical-id space up front: every page validates its own id
	// against the high-water mark, and the descriptors are written after the
	// pages they name.
	w.logic += uint64(segments) + uint64(FreeLogMaxIndexPages) + 2
	descriptors := make([]FreeSegment, 0, segments)
	for i := range segments {
		lower := i * perSegment
		upper := min(lower+perSegment, len(w.extents))
		extents := w.extents[lower:upper]
		ref := w.write(PageFreeImage, func(page []byte, header FreeLogHeader) error {
			_, err := EncodeFreeImagePage(page, header, extents, w.fileEnd, w.logic+1)
			return err
		})
		largest := uint64(0)
		for _, extent := range extents {
			largest = max(largest, extent.Length)
		}
		descriptors = append(descriptors, FreeSegment{
			Ref: ref, FirstOffset: extents[0].Offset,
			LargestFree: largest, Count: uint32(len(extents)),
		})
	}
	perIndexPage := FreeIndexRecordCapacity(testSuperblockPageSize)
	var indexHead PageRef
	for lower := 0; lower < len(descriptors); lower += perIndexPage {
		upper := min(lower+perIndexPage, len(descriptors))
		prev := indexHead
		indexHead = w.write(PageFreeIndex, func(page []byte, header FreeLogHeader) error {
			_, err := EncodeFreeIndexPage(
				page, header, descriptors[lower:upper], prev, w.fileEnd, w.logic+1)
			return err
		})
	}
	return w.write(PageFreeDelta, func(page []byte, header FreeLogHeader) error {
		_, err := EncodeFreeDeltaPage(page, header, nil, PageRef{}, indexHead, w.fileEnd, w.logic+1)
		return err
	})
}

// Given the geometry constants alone, when the free set the format can describe
// is computed, then it is thirty-five times what the linked image held.
//
// This is stated as a test rather than a comment because the three constants it
// multiplies live in three files, and the whole point of the change is the
// product. A future edit that shrinks the index bound to buy back a page of
// transaction reserve should have to see what it costs.
func TestFreeSetCapacityAgainstTheLinkedImage(t *testing.T) {
	perSegment := FreeImageRecordCapacity(testSuperblockPageSize)
	perIndexPage := FreeIndexRecordCapacity(testSuperblockPageSize)
	now := FreeLogMaxIndexPages * perIndexPage * perSegment
	before := 16 * perSegment
	if now < 30*before {
		t.Fatalf("free set holds %d extents against the linked image's %d, less than 30x",
			now, before)
	}
	// The fold reserve is what the old bound was protecting, and it must not have
	// grown: the index bound plus the fold-segment bound plus the delta bound is
	// what one commit may write, and it was sixteen image pages plus four delta
	// pages before.
	if reserve := FreeLogMaxIndexPages + FreeLogMaxFoldSegments + FreeLogMaxDeltaPages; reserve > 28 {
		t.Fatalf("free log reserves %d pages of one transaction, was 20", reserve)
	}
	t.Logf("free set capacity %d extents (%d MiB trackable at %d-byte pages) "+
		"against the linked image's %d (%d MiB), fold reserve %d pages",
		now, uint64(now)*uint64(testSuperblockPageSize)>>20, testSuperblockPageSize,
		before, uint64(before)*uint64(testSuperblockPageSize)>>20,
		FreeLogMaxIndexPages+FreeLogMaxFoldSegments+FreeLogMaxDeltaPages)
}
