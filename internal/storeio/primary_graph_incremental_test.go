package storeio

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	vibejson "github.com/thesyncim/vibejson"
)

type incrementalPrimaryTestSink struct {
	pages [][]byte
	refs  []PageRef
	next  uint64
}

type incrementalPrimaryTestPage struct {
	owner *incrementalPrimaryTestSink
	ref   PageRef
	image []byte
}

func (p *incrementalPrimaryTestPage) Bytes() []byte { return p.image }
func (p *incrementalPrimaryTestPage) Ref() PageRef  { return p.ref }
func (p *incrementalPrimaryTestPage) Stage() error {
	p.owner.pages = append(p.owner.pages, p.image)
	p.owner.refs = append(p.owner.refs, p.ref)
	return nil
}

func (s *incrementalPrimaryTestSink) AllocatePage(
	kind PageKind, length uint32, logicalID uint64,
) (PrimaryGraphBuildPage, error) {
	ref := PageRef{
		Offset: s.next, LogicalID: logicalID, Generation: 7,
		Length: length, Kind: kind,
	}
	s.next += uint64(length)
	return &incrementalPrimaryTestPage{
		owner: s, ref: ref, image: make([]byte, length),
	}, nil
}
func (*incrementalPrimaryTestSink) StoreIdentity() [16]byte { return testStoreID }
func (*incrementalPrimaryTestSink) BuildGeneration() uint64 { return 7 }
func (s *incrementalPrimaryTestSink) BuildFileEnd() uint64  { return s.next }
func (*incrementalPrimaryTestSink) BuildNextLogicalID() uint64 {
	return PrimaryFirstDynamicLogicalID
}
func (*incrementalPrimaryTestSink) MaxBuildPageBytes() int {
	return CommonPrimaryLeafMaxExtentBytes
}

func TestPrimaryGraphLeafWindowPlannerMatchesBulkBoundary(t *testing.T) {
	pointer, err := vibejson.CompilePointer("/score")
	if err != nil {
		t.Fatal(err)
	}
	for _, placed := range []bool{false, true} {
		rows := 1200
		if placed {
			rows = CommonPrimaryLeafWideSlots
		}
		records := make([]PrimaryGraphRecord, rows)
		for row := range records {
			key := []byte(fmt.Sprintf("key-%08d", row))
			value := []byte(fmt.Sprintf(
				`{"score":%d,"label":"value-%08d-%s"}`,
				row, row, bytes.Repeat([]byte{'x'}, row%47),
			))
			records[row] = BorrowPrimaryGraphRecord(key, value)
		}
		for _, maxExtent := range []int{16 << 10, CommonPrimaryLeafMaxExtentBytes} {
			planner, err := NewPrimaryGraphLeafWindowPlanner(
				placed, []vibejson.CompiledPointer{pointer},
			)
			if err != nil {
				t.Fatal(err)
			}
			count, extent, payload, err := planner.Plan(records, maxExtent)
			if err != nil {
				t.Fatal(err)
			}
			maxRows := CompactPrimaryStripeMaxRows
			if placed {
				maxRows = CommonPrimaryLeafWideSlots
			}
			bulk, err := planCompactPrimaryLeavesSummarized(
				testStoreID, records, maxRows, maxExtent,
				[]vibejson.CompiledPointer{pointer},
			)
			if err != nil {
				t.Fatal(err)
			}
			first := bulk[0]
			if count != first.last-first.first || extent != first.extent {
				t.Fatalf(
					"placed=%v extent=%d got count/extent %d/%d, want %d/%d",
					placed, maxExtent, count, extent,
					first.last-first.first, first.extent,
				)
			}
			builder := NewUnifiedPrimaryLeafBuilder()
			if err := builder.SetCompactPrimarySummaries(
				[]vibejson.CompiledPointer{pointer},
			); err != nil {
				t.Fatal(err)
			}
			if err := prepareCompactPrimaryGraphStripe(records, placed, builder); err != nil {
				t.Fatal(err)
			}
			want, err := buildPreparedCompactPrimaryGraphStripePayload(
				records[:count], builder,
			)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(payload, want) {
				t.Fatalf("placed=%v extent=%d payload differs", placed, maxExtent)
			}
		}
	}
}

func TestPrimaryGraphLeafWindowPlannerWarmAllocationBound(t *testing.T) {
	records := make([]PrimaryGraphRecord, CommonPrimaryLeafWideSlots)
	for row := range records {
		records[row] = BorrowPrimaryGraphRecord(
			[]byte(fmt.Sprintf("key-%08d", row)),
			[]byte(fmt.Sprintf(`{"score":%d,"enabled":true}`, row)),
		)
	}
	planner, err := NewPrimaryGraphLeafWindowPlanner(true, nil)
	if err != nil {
		t.Fatal(err)
	}
	for range 4 {
		if _, _, _, err := planner.Plan(records, CommonPrimaryLeafMaxExtentBytes); err != nil {
			t.Fatal(err)
		}
	}
	allocs := testing.AllocsPerRun(100, func() {
		if _, _, _, err := planner.Plan(records, CommonPrimaryLeafMaxExtentBytes); err != nil {
			panic(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("warm planner allocs/run = %.2f, want 0", allocs)
	}
}

func TestPrimaryGraphLeafWindowPlannerStagesDirectly(t *testing.T) {
	records := make([]PrimaryGraphRecord, CommonPrimaryLeafWideSlots)
	for row := range records {
		records[row] = BorrowPrimaryGraphRecord(
			[]byte(fmt.Sprintf("key-%08d", row)),
			[]byte(fmt.Sprintf(`{"rank":%d,"payload":"%s"}`,
				row, bytes.Repeat([]byte{'z'}, row%31))),
		)
	}
	planner, err := NewPrimaryGraphLeafWindowPlanner(true, nil)
	if err != nil {
		t.Fatal(err)
	}
	sink := &incrementalPrimaryTestSink{next: 64 << 10}
	placements := make([]PrimaryGraphPlacement, len(records))
	emission, err := planner.Stage(
		sink, 3, 17, records, 16<<10, placements,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(sink.pages) != 1 || emission.Count == 0 ||
		emission.Count > len(records) {
		t.Fatalf("bad emission: %#v pages=%d", emission, len(sink.pages))
	}
	wantBucket, _ := MakeTabletLocalIdentityBucket(3, 17)
	if emission.Bucket != BucketID(wantBucket) ||
		!bytes.Equal(emission.FirstKey, records[0].keyBytes()) ||
		!bytes.Equal(emission.LastKey, records[emission.Count-1].keyBytes()) {
		t.Fatalf("bad routing witness: %#v", emission)
	}
	for row := range emission.Count {
		if placements[row].Bucket != emission.Bucket ||
			placements[row].Slot != uint8(row) {
			t.Fatalf("placement %d = %#v", row, placements[row])
		}
	}
	view, err := OpenCompactPrimaryStripe(
		sink.pages[0], testStoreID, emission.Bucket, emission.Ref, 7,
		CommonPrimaryLeafBounds{
			FileEnd:           incrementalPrimaryFileEnd(sink),
			NextLogicalID:     PrimaryFirstDynamicLogicalID,
			AllocationQuantum: format0PageSize,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if view.Len() != emission.Count {
		t.Fatalf("decoded rows=%d, want %d", view.Len(), emission.Count)
	}
}

func TestStagePrimaryTabletWindowUsesBoundedLeafWitnesses(t *testing.T) {
	records := make([]PrimaryGraphRecord, 64)
	for row := range records {
		records[row] = BorrowPrimaryGraphRecord(
			[]byte(fmt.Sprintf("key-%08d", row)),
			[]byte(fmt.Sprintf(`{"rank":%d}`, row)),
		)
	}
	planner, err := NewPrimaryGraphLeafWindowPlanner(true, nil)
	if err != nil {
		t.Fatal(err)
	}
	sink := &incrementalPrimaryTestSink{next: 64 << 10}
	leaves := make([]primaryBuiltLeaf, 0, 2)
	for leaf := range 2 {
		window := records[leaf*32 : (leaf+1)*32]
		emission, err := planner.Stage(
			sink, 5, uint16(leaf), window,
			CommonPrimaryLeafMaxExtentBytes, nil,
		)
		if err != nil {
			t.Fatal(err)
		}
		leaves = append(leaves, primaryBuiltLeaf{
			firstKey: emission.FirstKey, lastKey: emission.LastKey,
			ref: emission.Ref,
		})
	}
	child, err := stagePrimaryTabletWindow(sink, 5, leaves, []byte("before"))
	if err != nil {
		t.Fatal(err)
	}
	if child.id != 5 || child.ref.Kind != PageTabletRoute || len(child.floor) == 0 {
		t.Fatalf("bad tablet child: %#v", child)
	}
	var routeImage []byte
	for at := range sink.refs {
		if sink.refs[at] == child.ref {
			routeImage = sink.pages[at]
			break
		}
	}
	if routeImage == nil {
		t.Fatal("tablet route was not staged")
	}
	view, err := OpenGlobalTabletCatalogTabletRoot(
		routeImage, child.ref,
		GlobalTabletCatalogBounds{
			StoreID: testStoreID, SelectedRootGeneration: 7,
			FileEnd:       incrementalPrimaryFileEnd(sink),
			NextLogicalID: PrimaryFirstDynamicLogicalID,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if view.TabletID() != 5 || view.AnchorCount() != 1 {
		t.Fatalf("tablet id/anchors = %d/%d", view.TabletID(), view.AnchorCount())
	}
}

func TestPrimaryGraphCatalogFolderBoundsDirectAndBranchShapes(t *testing.T) {
	leafFanout := GlobalTabletCatalogWorstCaseFanout(
		GlobalTabletCatalogNodeBytes, CommonPrimaryLeafMaxKeyBytes,
	)
	rootFanout := GlobalTabletCatalogWorstCaseFanout(
		GlobalTabletCatalogRootBytes, CommonPrimaryLeafMaxKeyBytes,
	)
	for _, test := range []struct {
		name       string
		tablets    int
		childLevel GlobalTabletCatalogNodeLevel
	}{
		{name: "direct", tablets: 2 * leafFanout, childLevel: GlobalTabletCatalogLeaf},
		{
			name: "branch", tablets: (rootFanout + 1) * leafFanout,
			childLevel: GlobalTabletCatalogBranch,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			const base = uint64(64 << 10)
			const stride = uint64(GlobalTabletCatalogTabletBytes)
			sink := &incrementalPrimaryTestSink{
				next: base + uint64(test.tablets)*stride,
			}
			folder, err := NewPrimaryGraphCatalogFolder(sink)
			if err != nil {
				t.Fatal(err)
			}
			for tabletID := range test.tablets {
				logicalID, ok := GlobalTabletCatalogTabletRootLogicalID(uint32(tabletID))
				if !ok {
					t.Fatal("tablet logical ID")
				}
				var floor []byte
				if tabletID != 0 {
					floor = []byte(fmt.Sprintf("f-%08d", tabletID))
				}
				if err := folder.AddTablet(primaryCatalogChild{
					floor: floor, id: uint32(tabletID),
					ref: PageRef{
						Offset:    base + uint64(tabletID)*stride,
						LogicalID: logicalID, Generation: 7,
						Length: GlobalTabletCatalogTabletBytes,
						Kind:   PageTabletRoute,
					},
				}); err != nil {
					t.Fatal(err)
				}
			}
			root, err := folder.Finish()
			if err != nil {
				t.Fatal(err)
			}
			var image []byte
			for at := range sink.refs {
				if sink.refs[at] == root {
					image = sink.pages[at]
					break
				}
			}
			if image == nil {
				t.Fatal("catalog root not staged")
			}
			view, err := OpenGlobalTabletCatalogNode(
				image, root,
				GlobalTabletCatalogBounds{
					StoreID: testStoreID, SelectedRootGeneration: 7,
					FileEnd:       sink.next,
					NextLogicalID: PrimaryFirstDynamicLogicalID,
				},
			)
			if err != nil {
				t.Fatal(err)
			}
			if view.Level() != GlobalTabletCatalogRoot ||
				view.ChildLevel() != test.childLevel {
				t.Fatalf(
					"root level/child=%d/%d, want %d/%d",
					view.Level(), view.ChildLevel(),
					GlobalTabletCatalogRoot, test.childLevel,
				)
			}
			if cap(folder.tablets) != leafFanout ||
				cap(folder.leaves) > rootFanout+1 ||
				cap(folder.branches) > rootFanout {
				t.Fatalf(
					"unbounded caps tablets/leaves/branches=%d/%d/%d",
					cap(folder.tablets), cap(folder.leaves), cap(folder.branches),
				)
			}
		})
	}
}

func TestPrimaryGraphStreamBuilderConsumesPinnedWindowsAndReopens(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "primary-stream-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	reservation := UnrootedGenerationReservation{
		Offset: 64 << 10, Length: 64 << 20,
		FirstLogicalID: PrimaryFirstDynamicLogicalID,
		LogicalIDCount: 1 << 20,
	}
	writer, err := NewUnrootedGenerationWriter(
		file, reservation, testStoreID, 11, 0,
	)
	if err != nil {
		t.Fatal(err)
	}
	sink, err := NewUnrootedPrimaryGraphSink(
		writer, testStoreID, 11, PrimaryFirstDynamicLogicalID,
		reservation.Offset+reservation.Length, make([]byte, 512<<10),
	)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := NewPrimaryGraphStreamBuilder(sink, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	const rows = 1024
	for first := 0; first < rows; {
		count := min(17+(first%61), rows-first)
		keys := make([][]byte, count)
		values := make([][]byte, count)
		window := make([]PrimaryGraphRecord, count)
		placements := make([]PrimaryGraphPlacement, count)
		for row := range count {
			rank := first + row
			keys[row] = []byte(fmt.Sprintf("key-%08d", rank))
			values[row] = []byte(fmt.Sprintf(`{"rank":%d}`, rank))
			window[row] = BorrowPrimaryGraphRecord(keys[row], values[row])
		}
		if err := stream.StageWindow(window, placements); err != nil {
			t.Fatal(err)
		}
		for row := range count {
			if placements[row].Bucket == 0 && placements[row].Slot == 0 && first+row != 0 {
				t.Fatalf("placement %d was not populated", first+row)
			}
			clear(keys[row])
			clear(values[row])
		}
		first += count
	}
	root, err := stream.Finish()
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Sync(); err != nil {
		t.Fatal(err)
	}
	if cap(stream.leaves) != TabletLocalIdentityLocalCount ||
		cap(stream.keyArena) != 2*TabletLocalIdentityLocalCount*CommonPrimaryLeafMaxKeyBytes {
		t.Fatalf(
			"stream bounds leaves/keys=%d/%d", cap(stream.leaves), cap(stream.keyArena),
		)
	}
	cache, err := NewPageCache(file, PageCacheOptions{
		PageSize: int(format0PageSize), MaxPageSize: CommonPrimaryLeafMaxExtentBytes,
		ResidentBytes: 4 << 20, StoreID: testStoreID, ReadConcurrency: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()
	bounds := GlobalTabletCatalogBounds{
		StoreID: testStoreID, SelectedRootGeneration: 11,
		FileEnd:       reservation.Offset + reservation.Length,
		NextLogicalID: sink.BuildNextLogicalID(),
	}
	router, err := BuildResidentPrimaryRouter(cache, root, bounds)
	if err != nil {
		t.Fatal(err)
	}
	if router.Len() == 0 {
		t.Fatal("streamed graph has no leaves")
	}
	for _, rank := range []int{0, rows / 2, rows - 1} {
		key := []byte(fmt.Sprintf("key-%08d", rank))
		route, ok := router.Route(key)
		if !ok {
			t.Fatalf("route %q", key)
		}
		lease, err := cache.Acquire(route.Ref)
		if err != nil {
			t.Fatal(err)
		}
		view, err := OpenCompactPrimaryStripe(
			lease.Page(), testStoreID, route.Bucket, route.Ref, 11,
			CommonPrimaryLeafBounds{
				FileEnd: bounds.FileEnd, NextLogicalID: bounds.NextLogicalID,
				AllocationQuantum: format0PageSize,
			},
		)
		if err != nil {
			lease.Release()
			t.Fatal(err)
		}
		row, ok := view.FindKey(key)
		if !ok {
			lease.Release()
			t.Fatalf("missing key %q", key)
		}
		value, ok := view.AppendValue(nil, row)
		lease.Release()
		want := []byte(fmt.Sprintf(`{"rank":%d}`, rank))
		if !ok || !bytes.Equal(value, want) {
			t.Fatalf("value %q = %q,%v want %q", key, value, ok, want)
		}
	}
}

func incrementalPrimaryFileEnd(s *incrementalPrimaryTestSink) uint64 {
	return max(s.next, uint64(GlobalTabletCatalogRootBytes))
}
