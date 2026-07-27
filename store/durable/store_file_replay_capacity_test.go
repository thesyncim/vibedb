package durable

import (
	"errors"
	"fmt"
	"os"
	"slices"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
)

// TestFileStorePointReplayDoesNotExhaustRetirementCapacity matches the
// point-only transaction geometry used by the competitive adapter. A replay
// has no retained snapshots, so retirement pressure must remain bounded by
// the physical committer's fallback window rather than growing with the
// collection's document count.
func TestFileStorePointReplayDoesNotExhaustRetirementCapacity(t *testing.T) {
	const documents = 12_000
	file, err := os.CreateTemp(t.TempDir(), "file-store-point-replay-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	collection, err := Create(file, Options{
		ResidentBytes:     64 << 20,
		MaxBatchDocuments: 1,
		BufferCount:       1024,
		QueueSlots:        1024,
		GroupLimit:        64,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()

	for i := range documents {
		key := fmt.Sprintf("doc:%08d", i)
		value := fmt.Appendf(
			nil,
			`{"id":%d,"name":"user-%d","country":"PT","score":%d,"active":true,`+
				`"profile":{"tier":"team","region":"eu-west-1"},"tags":["alpha","delta"],`+
				`"note":"steady state, no anomalies observed in the last reporting window"}`,
			i, i, i%1000,
		)
		if created, putErr := collection.Put(key, value); putErr != nil || !created {
			stats := collection.Stats()
			retired := collection.reclaimer.Stats()
			t.Fatalf(
				"Put(%d) = (%v,%v); generation=%d durable=%d pending=%d/%d "+
					"pending_bytes=%d oldest_retired=%d fallback=%d reusable=%d "+
					"reusable_bytes=%d segments=%d dirty=%d dirty_all=%v "+
					"fold_limit=%d retire_scratch=%d file_end=%d",
				i, created, putErr, stats.PublishedGeneration, stats.DurableGeneration,
				stats.PendingRetiredExtents, stats.RetiredExtentCapacity,
				stats.PendingRetiredBytes, retired.OldestRetired,
				collection.committer.FallbackGeneration(),
				stats.ReusableExtents, stats.ReusableBytes, len(collection.freeSegments),
				collection.freeDirtyCount, collection.freeDirtyAll,
				collection.freeFoldLimit, len(collection.retireScratch), stats.FileEnd,
			)
		}
	}
	if err := collection.Flush(); err != nil {
		t.Fatal(err)
	}
	stats := collection.Stats()
	if stats.PendingRetiredExtents == stats.RetiredExtentCapacity {
		t.Fatalf("replay ended at retirement capacity: %+v", stats)
	}
	t.Logf(
		"generation=%d pending=%d/%d reusable=%d reusable_bytes=%d file_end=%d",
		stats.PublishedGeneration, stats.PendingRetiredExtents,
		stats.RetiredExtentCapacity, stats.ReusableExtents,
		stats.ReusableBytes, stats.FileEnd,
	)
}

// TestFreeFoldPlannerCoversMutationAndSelfRetirementClosure drives the planner
// at the complete format limit. Point-operation admission uses the
// single-document batch geometry; the planner still has to remain correct when
// a wider configuration reaches the complete segment-index capacity.
//
// Point updates and deletes can dirty segments through every mutable page
// family: state, document and overflow, chunk, fingerprint, indexes,
// float64 and grouping accelerators, reusable allocations, and the free log
// itself. This image starts with half the format limit in full segments and
// adds one independently placed retirement to every segment, forcing every old
// segment to split and consuming the exact complete reservation.
func TestFreeFoldPlannerCoversMutationAndSelfRetirementClosure(t *testing.T) {
	normalized, err := (Options{
		ResidentBytes:     64 << 20,
		MaxBatchDocuments: 1,
		BufferCount:       1024,
	}).normalized()
	if err != nil {
		t.Fatal(err)
	}
	perPage := storeio.FreeImageRecordCapacity(4096)
	hardLimit := storeio.FreeLogMaxIndexPages *
		storeio.FreeIndexRecordCapacity(4096)
	singleDocumentOptions := normalized.Options
	singleDocumentOptions.MaxBatchDocuments = 1
	wantSingle := batchFreeFoldLimit(singleDocumentOptions, 0)
	if got := normalized.singleDocumentFreeFoldLimit; got != wantSingle ||
		got <= storeio.FreeLogMaxFoldSegments ||
		normalized.freeFoldLimit != got || got >= hardLimit {
		t.Fatalf(
			"single-document fold limit = %d batch limit %d, want one-document geometry %d above baseline %d and below format limit %d",
			got, normalized.freeFoldLimit, wantSingle,
			storeio.FreeLogMaxFoldSegments, hardLimit,
		)
	}
	if hardLimit%2 != 0 {
		t.Fatalf("test requires an even point fold limit, got %d", hardLimit)
	}
	segments := hardLimit / 2
	collection := &Collection{
		freeFoldLimit:    hardLimit,
		freeImagePerPage: perPage,
		freeIndexPerPage: storeio.FreeIndexRecordCapacity(4096),
		freeSegments:     make([]storeio.FreeSegment, segments),
		freeDirty:        make([]bool, segments),
		freeReadBack:     make([]bool, segments),
		freeResident:     make([]bool, segments),
		freeFoldRanges:   make([][2]int, 0, hardLimit),
		freeFoldOrder:    make([]freeFoldSlot, 0, hardLimit),
	}
	live := make([]storeio.FreeExtent, 0, segments*(perPage+1))
	for segment := range segments {
		first := uint64(segment+1) << 30
		collection.freeSegments[segment] = storeio.FreeSegment{
			FirstOffset: first,
			Count:       uint32(perPage),
		}
		collection.freeDirty[segment] = true
		collection.freeResident[segment] = true
		for extent := range perPage + 1 {
			live = append(live, storeio.FreeExtent{
				Offset:            first + uint64(extent)*4096,
				Length:            4096,
				RetiredGeneration: 1,
			})
		}
	}
	for segment := range segments {
		lo, hi := collection.segmentSpan(live, segment)
		if got, want := hi-lo, perPage+1; got != want {
			t.Fatalf("segment %d span = %d, want %d", segment, got, want)
		}
	}
	plan, err := collection.planFreeFold(live)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(plan.rebuilt), 2*segments; got != want {
		t.Fatalf("rebuilt segments = %d, want %d", got, want)
	}
	if len(plan.rebuilt) != hardLimit {
		t.Fatalf("complete fold limit %d cannot hold %d split outputs",
			hardLimit, len(plan.rebuilt))
	}
}

// TestPointFreeFoldSkewRepacksWholeImageBeforeRefusing covers the case where
// the free set fits comfortably in a compact image but an incremental layout
// does not. Full dirty segments split while hundreds of clean one-record
// segments would otherwise be carried, overflowing the segment index.
//
// The extra record in every dirty segment is held by the reclaimer rather than
// reusable, so the assertion also proves that the repack includes fenced
// extents when it reconstructs the complete image.
func TestPointFreeFoldSkewRepacksWholeImageBeforeRefusing(t *testing.T) {
	const (
		pageSize        = 4096
		dirtySegment    = 278
		segmentPageBase = uint64(1) << 60
	)
	perPage := storeio.FreeImageRecordCapacity(pageSize)
	indexPerPage := storeio.FreeIndexRecordCapacity(pageSize)
	hardLimit := storeio.FreeLogMaxIndexPages * indexPerPage
	if dirtySegment >= hardLimit/2 {
		t.Fatalf("dirty test geometry %d leaves no sparse carried tail", dirtySegment)
	}

	leases, err := storeio.NewGenerationLeases(
		storeio.GenerationLeaseOptions{MaxLeases: 1},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer leases.Close()
	reclaimer, err := storeio.NewExtentReclaimer(
		leases,
		storeio.ExtentReclaimerOptions{MaxRetiredExtents: hardLimit},
	)
	if err != nil {
		t.Fatal(err)
	}

	collection := &Collection{
		reclaimer:        reclaimer,
		freeFoldLimit:    hardLimit,
		freeImagePerPage: perPage,
		freeIndexPerPage: indexPerPage,
		freeSegments:     make([]storeio.FreeSegment, hardLimit),
		freeDirty:        make([]bool, hardLimit),
		freeResident:     make([]bool, hardLimit),
		freeReadBack:     make([]bool, hardLimit),
		freeRetired:      make([]bool, hardLimit),
		freeFoldRanges:   make([][2]int, 0, hardLimit),
		freeFoldOrder:    make([]freeFoldSlot, 0, hardLimit),
		retireScratch:    make([]storeio.FreeExtent, 0, 2*hardLimit),
	}
	reusable := make([]storeio.FreeExtent, 0, dirtySegment*perPage+hardLimit-dirtySegment)
	fenced := make([]storeio.FreeExtent, 0, dirtySegment)
	segmentPages := make([]storeio.FreeExtent, 0, hardLimit)
	for segment := range hardLimit {
		first := uint64(segment+1) << 24
		// Segment pages are live metadata until the repack supersedes them.
		// Keep their synthetic address range disjoint from the free extents:
		// the format-wide index holds enough pages for low sequential refs to
		// reach the first reusable extent at 1<<24.
		segmentPageOffset := segmentPageBase +
			uint64(segment)*uint64(pageSize)
		count := 1
		if segment < dirtySegment {
			count = perPage
			collection.freeDirty[segment] = true
			collection.freeDirtyCount++
			fenced = append(fenced, storeio.FreeExtent{
				Offset:            first + pageSize,
				Length:            pageSize,
				RetiredGeneration: 1,
			})
		}
		collection.freeSegments[segment] = storeio.FreeSegment{
			Ref: storeio.PageRef{
				Offset: segmentPageOffset,
				Length: pageSize,
			},
			FirstOffset: first,
			Count:       uint32(count),
		}
		segmentPages = append(segmentPages, storeio.FreeExtent{
			Offset:            segmentPageOffset,
			Length:            pageSize,
			RetiredGeneration: 1,
		})
		collection.freeResident[segment] = true
		for extent := range count {
			reusable = append(reusable, storeio.FreeExtent{
				Offset:            first + uint64(extent)*2*pageSize,
				Length:            pageSize,
				RetiredGeneration: 1,
			})
		}
	}
	collection.reusable = reusable
	if err := reclaimer.RetireBatch(fenced); err != nil {
		t.Fatal(err)
	}

	bounds := storeio.FreeLogBounds{}
	live, err := collection.buildFoldImage(bounds)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(live), len(reusable)+len(fenced); got != want {
		t.Fatalf("complete fold image has %d extents, want reusable+fenced %d",
			got, want)
	}
	if _, err := collection.planFreeFold(live); !errors.Is(err, storeio.ErrRetiredExtentCapacity) {
		t.Fatalf("skewed incremental plan = %v, want segment-index overflow", err)
	}

	state := &fileStoreState{root: storeio.StateRoot{Generation: 1}}
	plan, repacked, err := collection.planFreeFoldWithRepack(live, bounds, state)
	if err != nil {
		t.Fatal(err)
	}
	wantExtents := append(make([]storeio.FreeExtent, 0,
		len(reusable)+len(fenced)+len(segmentPages)), reusable...)
	wantExtents = append(wantExtents, fenced...)
	wantExtents = append(wantExtents, segmentPages...)
	slices.SortFunc(wantExtents, compareFreeExtentOffset)
	if len(repacked) != len(wantExtents) {
		t.Fatalf("whole-image repack preserved %d extents, want %d",
			len(repacked), len(wantExtents))
	}
	for i := range wantExtents {
		if repacked[i] != wantExtents[i] {
			t.Fatalf("whole-image repack extent %d = %+v, want %+v",
				i, repacked[i], wantExtents[i])
		}
	}
	wantPages := (len(repacked) + perPage - 1) / perPage
	if got := len(plan.rebuilt); got != wantPages {
		t.Fatalf("whole-image repack rebuilt %d pages, want compact %d", got, wantPages)
	}
	if got := len(plan.order); got != wantPages || got > hardLimit {
		t.Fatalf("whole-image repack order has %d slots, want %d within %d",
			got, wantPages, hardLimit)
	}
	if !collection.freeDirtyAll {
		t.Fatal("skew fallback did not force the complete image through repacking")
	}
}

func TestPointFreeFoldFailedRepackKeepsConservativeDirtyState(t *testing.T) {
	const (
		pageSize = 4096
		segments = storeio.FreeLogMaxFoldSegments + 1
	)
	perPage := storeio.FreeImageRecordCapacity(pageSize)
	leases, err := storeio.NewGenerationLeases(
		storeio.GenerationLeaseOptions{MaxLeases: 1},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer leases.Close()
	reclaimer, err := storeio.NewExtentReclaimer(
		leases,
		storeio.ExtentReclaimerOptions{MaxRetiredExtents: 64},
	)
	if err != nil {
		t.Fatal(err)
	}
	collection := &Collection{
		reclaimer:        reclaimer,
		freeFoldLimit:    storeio.FreeLogMaxFoldSegments,
		freeImagePerPage: perPage,
		freeIndexPerPage: storeio.FreeIndexRecordCapacity(pageSize),
		freeSegments:     make([]storeio.FreeSegment, segments),
		freeDirty:        make([]bool, segments),
		freeResident:     make([]bool, segments),
		freeReadBack:     make([]bool, segments),
		freeRetired:      make([]bool, segments),
		freeFoldRanges:   make([][2]int, 0, storeio.FreeLogMaxFoldSegments),
		freeFoldOrder:    make([]freeFoldSlot, 0, storeio.FreeLogMaxFoldSegments),
		retireScratch:    make([]storeio.FreeExtent, 0, 64),
	}
	for segment := range segments {
		first := uint64(segment+1) << 24
		collection.freeSegments[segment] = storeio.FreeSegment{
			Ref: storeio.PageRef{
				Offset: uint64(segment+1) * uint64(pageSize),
				Length: pageSize,
			},
			FirstOffset: first,
			Count:       uint32(perPage),
		}
		collection.freeDirty[segment] = true
		collection.freeResident[segment] = true
		collection.freeDirtyCount++
		for extent := range perPage {
			collection.reusable = append(collection.reusable, storeio.FreeExtent{
				Offset:            first + uint64(extent)*2*pageSize,
				Length:            pageSize,
				RetiredGeneration: 1,
			})
		}
	}
	bounds := storeio.FreeLogBounds{}
	live, err := collection.buildFoldImage(bounds)
	if err != nil {
		t.Fatal(err)
	}
	state := &fileStoreState{root: storeio.StateRoot{Generation: 1}}
	if _, _, err := collection.planFreeFoldWithRepack(live, bounds, state); !errors.Is(
		err, storeio.ErrRetiredExtentCapacity,
	) {
		t.Fatalf("unrepresentable repack = %v, want capacity error", err)
	}
	if !collection.freeDirtyAll {
		t.Fatal("failed repack relaxed dirty state and could carry stale segments later")
	}

	// An abort leaves the next commit conservative: rebuilding the image again
	// still treats every old segment as dirty and refuses rather than carrying
	// a stale page by reference.
	live, err = collection.buildFoldImage(bounds)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := collection.planFreeFold(live); !errors.Is(err, storeio.ErrRetiredExtentCapacity) {
		t.Fatalf("next plan after failed repack = %v, want the same safe refusal", err)
	}
}
