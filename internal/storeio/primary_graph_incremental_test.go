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

func TestPrimaryValueLeafWindowPlannerStagesMixedOverflow(t *testing.T) {
	overflow := PageRef{
		Offset: 1 << 20, LogicalID: PrimaryFirstDynamicLogicalID, Generation: 7,
		Length: format0PageSize, Kind: PageOverflow,
	}
	records := []CommonPrimaryLeafRecord{
		{Slot: 0, Key: []byte("a"), Value: CommonPrimaryLeafValue{Inline: []byte(`{"v":1}`)}},
		{Slot: 1, Key: []byte("b"), Value: CommonPrimaryLeafValue{Overflow: overflow}},
		{Slot: 2, Key: []byte("c"), Value: CommonPrimaryLeafValue{Inline: []byte(`{"v":3}`)}},
	}
	planner, err := NewPrimaryValueLeafWindowPlanner(nil)
	if err != nil {
		t.Fatal(err)
	}
	sink := &incrementalPrimaryTestSink{next: 2 << 20}
	emission, err := planner.Stage(
		sink, 0, 0, records, CommonPrimaryLeafMaxExtentBytes,
	)
	if err != nil {
		t.Fatal(err)
	}
	view, err := OpenCompactPrimaryStripe(
		sink.pages[0], testStoreID, emission.Bucket, emission.Ref, 7,
		CommonPrimaryLeafBounds{
			FileEnd: sink.next, NextLogicalID: PrimaryFirstDynamicLogicalID + 100,
			AllocationQuantum: format0PageSize,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if view.Len() != len(records) || !view.HasOverflowRows() {
		t.Fatalf("mixed leaf rows/overflow=%d/%v", view.Len(), view.HasOverflowRows())
	}
	got, ok := view.OverflowRef(1)
	if !ok || got != overflow {
		t.Fatalf("overflow ref=%+v,%v want %+v", got, ok, overflow)
	}
}

func TestPrimaryValueUnplacedPlannerIgnoresAndDoesNotLeakSlots(t *testing.T) {
	const rows = 300
	records := make([]CommonPrimaryLeafRecord, rows)
	wantSlots := make([]uint8, rows)
	for row := range records {
		records[row] = CommonPrimaryLeafRecord{
			Slot:  uint8(row % 7),
			Key:   fmt.Appendf(nil, "unplaced-%04d", row),
			Value: CommonPrimaryLeafValue{Inline: []byte(`{"shared":"value"}`)},
		}
		wantSlots[row] = records[row].Slot
	}
	builder := NewUnifiedPrimaryLeafBuilder()
	planner := &PrimaryValueLeafWindowPlanner{builder: builder}
	starts, err := AppendCommonPrimaryCompactLeafStarts(
		nil, builder, testStoreID, records,
	)
	if err != nil || len(starts) != 1 || starts[0] != 0 {
		t.Fatalf("unplaced starts = %v, %v; want one range", starts, err)
	}
	for row := range records {
		if records[row].Slot != wantSlots[row] {
			t.Fatalf("topology planning changed source row %d slot to %d", row, records[row].Slot)
		}
	}
	count, extent, _, err := planner.PlanUnplaced(
		records, CommonPrimaryLeafMaxExtentBytes,
	)
	if err != nil || count != rows || extent > CommonPrimaryLeafMaxExtentBytes {
		t.Fatalf("unplaced plan = %d rows/%d bytes, %v", count, extent, err)
	}
	for row := range records {
		if records[row].Slot != wantSlots[row] {
			t.Fatalf("PlanUnplaced changed source row %d slot to %d", row, records[row].Slot)
		}
	}

	placed := make([]CommonPrimaryLeafRecord, 64)
	for row := range placed {
		placed[row] = CommonPrimaryLeafRecord{
			Key:   fmt.Appendf(nil, "placed-%03d", row),
			Value: CommonPrimaryLeafValue{Inline: []byte(`{"v":1}`)},
		}
	}
	if err := PlaceCommonPrimaryLeafRecords(
		CommonPrimaryLeafWide, testStoreID, placed,
	); err != nil {
		t.Fatalf("place follow-up indexed window: %v", err)
	}
	if _, err := BuildCompactPrimaryStripePayload(placed, builder); err != nil {
		t.Fatalf("placed encode after unplaced plan: %v", err)
	}
	for row := range placed {
		if got := builder.slotAt(row); got != placed[row].Slot {
			t.Fatalf("placed slot %d = %d, want %d after unplaced planning", row, got, placed[row].Slot)
		}
	}
}

func TestPrimaryValueUnplacedPlannerBoundaryRangesRoundTrip(t *testing.T) {
	cases := []struct {
		name          string
		opaque        bool
		mixedOverflow bool
		inlineBytes   int
	}{
		{name: "varied-inline", inlineBytes: 360},
		{name: "opaque", opaque: true, inlineBytes: 360},
		{name: "mixed-overflow", mixedOverflow: true, inlineBytes: 520},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			const rows = 700
			overflow := PageRef{
				Offset: 1 << 20, LogicalID: PrimaryFirstDynamicLogicalID + 1,
				Generation: 7, Length: format0PageSize, Kind: PageOverflow,
			}
			records := make([]CommonPrimaryLeafRecord, rows)
			sourceSlots := make([]uint8, rows)
			for row := range records {
				value := primaryBoundaryPayload(row, tc.inlineBytes)
				if tc.opaque {
					records[row].Value = CommonPrimaryLeafValue{Inline: value}
				} else {
					records[row].Value = CommonPrimaryLeafValue{
						Inline: fmt.Appendf(nil, `{"payload":%q,"rank":%d}`, value, row),
					}
				}
				if tc.mixedOverflow && row%5 == 0 {
					records[row].Value = CommonPrimaryLeafValue{Overflow: overflow}
				}
				records[row].Key = fmt.Appendf(nil, "boundary-%04d", row)
				records[row].Slot = uint8((row*37 + 11) % CommonPrimaryLeafWideSlots)
				sourceSlots[row] = records[row].Slot
			}

			builder := NewUnifiedPrimaryLeafBuilder()
			if err := builder.SetOpaqueValues(tc.opaque); err != nil {
				t.Fatalf("set planner opaque mode: %v", err)
			}
			starts, err := AppendCommonPrimaryCompactLeafStarts(
				nil, builder, testStoreID, records,
			)
			if err != nil {
				t.Fatalf("plan 64 KiB compact ranges: %v", err)
			}
			if len(starts) < 2 || starts[0] != 0 {
				t.Fatalf("compact starts = %v, want byte-bounded ranges", starts)
			}
			for row := range records {
				if records[row].Slot != sourceSlots[row] {
					t.Fatalf("planning changed source row %d slot to %d", row, records[row].Slot)
				}
			}

			const bucket BucketID = 0
			logicalID, ok := CommonPrimaryLeafLogicalID(bucket)
			if !ok {
				t.Fatal("common primary leaf logical ID")
			}
			bounds := CommonPrimaryLeafBounds{
				FileEnd: 2 << 20, NextLogicalID: PrimaryFirstDynamicLogicalID + 16,
				AllocationQuantum: format0PageSize,
			}
			for span, first := range starts {
				end := rows
				if span+1 < len(starts) {
					end = starts[span+1]
				}
				if first < 0 || first >= end || end > rows {
					t.Fatalf("span %d bounds = [%d,%d), rows=%d", span, first, end, rows)
				}
				window := append([]CommonPrimaryLeafRecord(nil), records[first:end]...)
				if len(window) > CommonPrimaryLeafWideSlots {
					t.Fatalf("byte-bounded span %d has %d rows; test must exercise placement", span, len(window))
				}
				if err := PlaceCommonPrimaryLeafRecords(
					CommonPrimaryLeafWide, testStoreID, window,
				); err != nil {
					t.Fatalf("place compact span %d with %d rows: %v", span, len(window), err)
				}

				stripeBuilder := NewUnifiedPrimaryLeafBuilder()
				if err := stripeBuilder.SetOpaqueValues(tc.opaque); err != nil {
					t.Fatalf("set stripe opaque mode: %v", err)
				}
				payload, err := BuildCompactPrimaryStripePayload(window, stripeBuilder)
				if err != nil {
					t.Fatalf("build compact span %d: %v", span, err)
				}
				need := PageHeaderSize + len(payload) + PageTrailerSize
				quantum := int(physicalPageQuantum)
				extent := (need + quantum - 1) &^ (quantum - 1)
				if extent > CommonPrimaryLeafMaxExtentBytes {
					t.Fatalf("span %d encoded extent=%d, over 64 KiB", span, extent)
				}
				header := CommonPrimaryLeafHeader{
					StoreID: testStoreID, Generation: 7, Bucket: bucket,
					PageSize: uint32(extent),
				}
				page, err := EncodeCompactPrimaryStripe(
					make([]byte, extent), header, window, stripeBuilder,
				)
				if err != nil {
					t.Fatalf("encode real compact span %d: %v", span, err)
				}
				ref := PageRef{
					Offset: uint64(format0PageSize), Length: uint32(extent),
					LogicalID: logicalID, Generation: 7, Kind: PagePrimaryLeaf,
				}
				view, err := OpenCompactPrimaryStripe(
					page, testStoreID, bucket, ref, 7, bounds,
				)
				if err != nil {
					t.Fatalf("decode real compact span %d: %v", span, err)
				}
				if view.Len() != len(window) || len(page) > CommonPrimaryLeafMaxExtentBytes {
					t.Fatalf("span %d decoded rows/extent=%d/%d, want %d and <=64 KiB",
						span, view.Len(), len(page), len(window))
				}
				for offset, source := range records[first:end] {
					key, ok := view.AppendKey(nil, offset)
					if !ok || !bytes.Equal(key, source.Key) {
						t.Fatalf("span %d row %d key=%q/%v, want %q",
							span, offset, key, ok, source.Key)
					}
					if source.Value.IsOverflow() {
						got, ok := view.OverflowRef(offset)
						if !ok || got != source.Value.Overflow {
							t.Fatalf("span %d row %d overflow=%+v/%v, want %+v",
								span, offset, got, ok, source.Value.Overflow)
						}
						continue
					}
					got, ok := view.AppendValue(nil, offset)
					if !ok || !bytes.Equal(got, source.Value.Inline) {
						t.Fatalf("span %d row %d value=%q/%v, want %q",
							span, offset, got, ok, source.Value.Inline)
					}
				}
			}
			for row := range records {
				if records[row].Slot != sourceSlots[row] {
					t.Fatalf("encoding changed source row %d slot to %d", row, records[row].Slot)
				}
			}
		})
	}
}

func primaryBoundaryPayload(row, size int) []byte {
	const alphabet = "0123456789abcdefghijklmnopqrstuvwxyz"
	payload := make([]byte, size)
	state := uint64(row+1) * 0x9e3779b97f4a7c15
	for index := range payload {
		state ^= state >> 12
		state ^= state << 25
		state ^= state >> 27
		state *= 0x2545f4914f6cdd1d
		payload[index] = alphabet[state%uint64(len(alphabet))]
	}
	return payload
}

func incrementalPrimaryFileEnd(s *incrementalPrimaryTestSink) uint64 {
	return max(s.next, uint64(GlobalTabletCatalogRootBytes))
}
