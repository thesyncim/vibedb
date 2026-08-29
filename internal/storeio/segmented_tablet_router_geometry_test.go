package storeio

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"testing"
)

func TestPrimaryGraphBytePackedReservation(t *testing.T) {
	records := make([]PrimaryGraphRecord, 3000)
	prefix := bytes.Repeat([]byte("p"), 254)
	for i := range records {
		key := append(append([]byte(nil), prefix...), byte(i>>8), byte(i))
		word := fmt.Sprintf("%08x", uint32(i)*2654435761)
		records[i] = PrimaryGraphRecord{Key: string(key), Value: fmt.Sprintf(`{"n":%d,"v":"%s"}`, i, bytes.Repeat([]byte(word), 55))}
	}
	plan, err := planPrimaryGraph(testStoreID, records, true, 4096, nil)
	if err != nil {
		t.Fatal(err)
	}
	rowBased, err := primaryGraphPageCountForLeaves(len(plan.leaves))
	if err != nil || plan.PageCount() <= rowBased {
		t.Fatalf("fixture must require byte-packed anchors: pages=%d row-based=%d err=%v", plan.PageCount(), rowBased, err)
	}
}

func TestSegmentedTabletRouterBytePackedAnchorLayout(t *testing.T) {
	header, leaves, _ := segmentedTabletRouterTestInputs(t, 512)
	rng := rand.New(rand.NewSource(913))
	for rank := 1; rank < len(leaves); rank++ {
		fence := make([]byte, 64)
		_, _ = rng.Read(fence)
		binary.BigEndian.PutUint16(fence, uint16(rank))
		leaves[rank].Fence = fence
	}
	ends, count, err := PlanSegmentedTabletRouterAnchors(leaves)
	if err != nil || count <= 2 || ends[0] >= SegmentedTabletRouterRowsPerPage {
		t.Fatalf("byte-packed layout ends=%v count=%d err=%v", ends, count, err)
	}
	_, _, refs := segmentedTabletRouterTestInputs(t, count*SegmentedTabletRouterRowsPerPage)
	root, locator, anchors, gotCount, err := EncodeSegmentedTabletRouter(
		make([]byte, SegmentedTabletRouterRootBytes), make([]byte, SegmentedTabletRouterLocatorBytes),
		make([]byte, count*SegmentedTabletRouterAnchorPageBytes), header, refs, leaves,
	)
	if err != nil || gotCount != count {
		t.Fatalf("encode count=%d err=%v", gotCount, err)
	}
	view, err := OpenSegmentedTabletRouter(root, locator, anchors)
	if err != nil {
		t.Fatal(err)
	}
	for rank, first, page := 0, 0, 0; rank < len(leaves); rank++ {
		if rank == ends[page] {
			first = rank
			page++
		}
		leaf := leaves[rank]
		route := view.Route(segmentedTabletRouterTestSeed, leaf.Fence)
		if route.Ref != leaf.Ref || int(route.PageID) != page || int(route.RowSlot) != rank-first {
			t.Fatalf("rank=%d route=%+v", rank, route)
		}
		ref, zone, found := view.ResolveBucketID(route.Bucket)
		if !found || ref != leaf.Ref || zone != leaf.Zone {
			t.Fatalf("locator rank=%d found=%v", rank, found)
		}
	}
	if allocations := testing.AllocsPerRun(100, func() {
		_ = view.Route(segmentedTabletRouterTestSeed, leaves[257].Fence)
	}); allocations != 0 {
		t.Fatalf("read allocations=%v", allocations)
	}
}

func TestSegmentedTabletRouterBytePackedAnchorCapacity(t *testing.T) {
	_, leaves, _ := segmentedTabletRouterTestInputs(t, 4096)
	rng := rand.New(rand.NewSource(914))
	for rank := 1; rank < len(leaves); rank++ {
		fence := make([]byte, 128)
		_, _ = rng.Read(fence)
		binary.BigEndian.PutUint16(fence, uint16(rank))
		leaves[rank].Fence = fence
	}
	if _, _, err := PlanSegmentedTabletRouterAnchors(leaves); !errors.Is(err, ErrSegmentedTabletRouterNoSpace) {
		t.Fatalf("true tablet capacity: %v", err)
	}
}
