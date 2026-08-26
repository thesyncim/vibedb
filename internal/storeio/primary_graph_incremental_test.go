package storeio

import (
	"bytes"
	"fmt"
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

func incrementalPrimaryFileEnd(s *incrementalPrimaryTestSink) uint64 {
	return max(s.next, uint64(GlobalTabletCatalogRootBytes))
}
