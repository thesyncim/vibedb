package storeio

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"testing"
)

var segmentedTabletRouterTestSeed = [16]byte{
	0x73, 0x65, 0x67, 0x6d, 0x65, 0x6e, 0x74, 0x65,
	0x64, 0x2d, 0x72, 0x6f, 0x75, 0x74, 0x65, 0x72,
}

func segmentedTabletRouterTestLeafLogicalID(
	t testing.TB, bucket BucketID,
) uint64 {
	t.Helper()
	logicalID, ok := SegmentedTabletRouterLeafLogicalID(bucket)
	if !ok {
		t.Fatalf("derive leaf logical ID for %d", bucket)
	}
	return logicalID
}

func segmentedTabletRouterTestAnchorLogicalID(
	t testing.TB, tabletID uint32, pageID uint8,
) uint64 {
	t.Helper()
	logicalID, ok := SegmentedTabletRouterAnchorLogicalID(
		tabletID, pageID,
	)
	if !ok {
		t.Fatalf("derive anchor logical ID for %d/%d", tabletID, pageID)
	}
	return logicalID
}

func segmentedTabletRouterTestBucket(ref PageRef) BucketID {
	return BucketID(ref.LogicalID - SegmentedTabletRouterLeafLogicalIDBase)
}

func segmentedTabletRouterTestInputs(
	t testing.TB, leafCount int,
) (
	SegmentedTabletRouterHeader,
	[]SegmentedTabletRouterLeaf,
	[]PageRef,
) {
	t.Helper()
	const tabletID = uint32(42)
	header := SegmentedTabletRouterHeader{
		StoreID:  segmentedTabletRouterTestSeed,
		TabletID: tabletID, Generation: 10_000,
		AnchorKind: PagePrimaryAnchor, LeafKind: PagePrimaryLeaf,
	}
	leaves := make([]SegmentedTabletRouterLeaf, leafCount)
	for rank := range leaves {
		localID := uint16((rank*2053 + 17) & 4095)
		bucket, ok := MakeTabletLocalIdentityBucket(
			tabletID, uint32(localID),
		)
		if !ok {
			t.Fatal("compose bucket")
		}
		var fence []byte
		if rank != 0 {
			fence = []byte(fmt.Sprintf(
				"tenant/0042/document/%010d", rank*230,
			))
		}
		var zone BucketZone
		binary.LittleEndian.PutUint32(
			zone[:], uint32(rank)^0xa5a55a5a,
		)
		length := uint32(1 + rank%65536)
		if rank%5 == 0 {
			length = uint32(4096 << ((rank / 5) % 5))
		}
		leaves[rank] = SegmentedTabletRouterLeaf{
			LocalID: localID,
			Fence:   fence,
			Ref: PageRef{
				Offset: uint64(rank+1) * 24,
				LogicalID: segmentedTabletRouterTestLeafLogicalID(
					t, BucketID(bucket),
				),
				Generation: uint64(1000 + rank),
				Length:     length,
				Kind:       header.LeafKind,
			},
			Zone: zone,
		}
	}
	pageCount := (leafCount + SegmentedTabletRouterRowsPerPage - 1) /
		SegmentedTabletRouterRowsPerPage
	anchorRefs := make([]PageRef, pageCount)
	for pageID := range anchorRefs {
		anchorRefs[pageID] = PageRef{
			Offset: uint64(pageID+1) * SegmentedTabletRouterAnchorPageBytes,
			LogicalID: segmentedTabletRouterTestAnchorLogicalID(
				t, tabletID, uint8(pageID),
			),
			Generation: header.Generation,
			Length:     SegmentedTabletRouterAnchorPageBytes,
			Kind:       header.AnchorKind,
		}
	}
	return header, leaves, anchorRefs
}

func segmentedTabletRouterTestView(
	t testing.TB, leafCount int,
) (
	SegmentedTabletRouterView,
	[]SegmentedTabletRouterLeaf,
	[]PageRef,
) {
	t.Helper()
	header, leaves, refs := segmentedTabletRouterTestInputs(t, leafCount)
	pageCount := (leafCount + SegmentedTabletRouterRowsPerPage - 1) /
		SegmentedTabletRouterRowsPerPage
	root, locator, pages, gotPageCount, err :=
		EncodeSegmentedTabletRouter(
			make([]byte, SegmentedTabletRouterRootBytes),
			make([]byte, SegmentedTabletRouterLocatorBytes),
			make([]byte, pageCount*SegmentedTabletRouterAnchorPageBytes),
			header, refs, leaves,
		)
	if err != nil {
		t.Fatal(err)
	}
	if gotPageCount != pageCount {
		t.Fatalf("pageCount = %d, want %d", gotPageCount, pageCount)
	}
	view, err := OpenSegmentedTabletRouter(root, locator, pages)
	if err != nil {
		t.Fatal(err)
	}
	return view, leaves, refs
}

func TestSegmentedTabletRouterRouteResolveAndOrderedCursor(
	t *testing.T,
) {
	view, leaves, _ := segmentedTabletRouterTestView(t, 3072)
	for _, rank := range []int{0, 1, 255, 256, 1536, 3071} {
		key := leaves[rank].Fence
		hash := KeyHashBytes(segmentedTabletRouterTestSeed, key)
		route := view.RouteHashed(hash, key)
		if route.Bucket != segmentedTabletRouterTestBucket(leaves[rank].Ref) ||
			route.Hash != hash ||
			int(route.PageID) != rank/SegmentedTabletRouterRowsPerPage ||
			int(route.RowSlot) != rank%SegmentedTabletRouterRowsPerPage ||
			route.Ref != leaves[rank].Ref ||
			route.Zone != leaves[rank].Zone {
			t.Fatalf("route rank %d = %+v", rank, route)
		}
		included := view.Route(segmentedTabletRouterTestSeed, key)
		if included != route {
			t.Fatalf("included route rank %d = %+v, want %+v",
				rank, included, route)
		}
		ref, zone, ok := view.ResolveBucketID(route.Bucket)
		if !ok || ref != leaves[rank].Ref || zone != leaves[rank].Zone {
			t.Fatalf("resolve rank %d = %+v/%x/%v", rank, ref, zone, ok)
		}
	}
	foreign, _ := MakeTabletLocalIdentityBucket(43, 17)
	if _, _, ok := view.ResolveBucketID(BucketID(foreign)); ok {
		t.Fatal("foreign bucket resolved")
	}
	missingLocalID := uint16(0)
	for binary.LittleEndian.Uint16(
		view.locator[int(missingLocalID)*2:],
	) != segmentedTabletRouterEmpty {
		missingLocalID++
	}
	missing, _ := MakeTabletLocalIdentityBucket(
		42, uint32(missingLocalID),
	)
	if _, _, ok := view.ResolveBucketID(BucketID(missing)); ok {
		t.Fatal("unused bucket resolved")
	}

	cursor := view.LowerBound(leaves[253].Fence)
	for rank := 253; rank < len(leaves); rank++ {
		route, ok := cursor.Route(uint64(rank))
		if !ok || route.Bucket != segmentedTabletRouterTestBucket(leaves[rank].Ref) ||
			route.Hash != uint64(rank) {
			t.Fatalf("cursor rank %d = %+v,%v", rank, route, ok)
		}
		if rank+1 < len(leaves) {
			if !cursor.Next() {
				t.Fatalf("cursor stopped at rank %d", rank)
			}
		} else if cursor.Next() {
			t.Fatal("cursor advanced past end")
		}
	}
}

func TestSegmentedTabletRouterUsesExactCommonAnchorEnvelope(t *testing.T) {
	header, leaves, refs := segmentedTabletRouterTestInputs(
		t, SegmentedTabletRouterRowsPerPage,
	)
	root, locator, pages, pageCount, err := EncodeSegmentedTabletRouter(
		make([]byte, SegmentedTabletRouterRootBytes),
		make([]byte, SegmentedTabletRouterLocatorBytes),
		make([]byte, SegmentedTabletRouterAnchorPageBytes),
		header, refs, leaves,
	)
	if err != nil {
		t.Fatal(err)
	}
	if pageCount != 1 {
		t.Fatalf("page count = %d, want 1", pageCount)
	}
	pageHeader, payload, err := OpenPage(pages)
	if err != nil {
		t.Fatal(err)
	}
	if pageHeader.StoreID != header.StoreID ||
		pageHeader.Generation != refs[0].Generation ||
		pageHeader.LogicalID != refs[0].LogicalID ||
		pageHeader.PageSize != SegmentedTabletRouterAnchorPageBytes ||
		pageHeader.PayloadLength != segmentedTabletRouterAnchorPayloadBytes ||
		pageHeader.Kind != PagePrimaryAnchor ||
		pageHeader.Flags != 0 ||
		len(payload) != segmentedTabletRouterAnchorPayloadBytes {
		t.Fatalf("common anchor header = %+v, payload=%d", pageHeader, len(payload))
	}
	if got := binary.LittleEndian.Uint16(payload[0:2]); got !=
		SegmentedTabletRouterRowsPerPage {
		t.Fatalf("anchor count = %d", got)
	}
	if !allZero(payload[6:segmentedTabletRouterAnchorPayloadHeaderBytes]) {
		t.Fatal("non-zero anchor payload reserved bytes")
	}
	if _, err := OpenSegmentedTabletRouter(root, locator, pages); err != nil {
		t.Fatalf("open full 256-row anchor: %v", err)
	}
}

func TestSegmentedTabletRouterLogicalIDNamespace(t *testing.T) {
	if SegmentedTabletRouterStateRootLogicalID != 1 ||
		SegmentedTabletRouterLeafLogicalIDBase != 2 ||
		SegmentedTabletRouterLeafLogicalIDLimit !=
			SegmentedTabletRouterAnchorLogicalIDBase ||
		SegmentedTabletRouterAnchorLogicalIDLimit !=
			PrimaryAnchorLogicalIDLimit ||
		SegmentedTabletRouterFirstDynamicLogicalID !=
			PrimaryFirstDynamicLogicalID ||
		SegmentedTabletRouterAnchorLogicalIDLimit >=
			SegmentedTabletRouterFirstDynamicLogicalID {
		t.Fatalf(
			"namespace boundaries state=%d leaf=[%d,%d) anchor=[%d,%d) dynamic=%d",
			SegmentedTabletRouterStateRootLogicalID,
			SegmentedTabletRouterLeafLogicalIDBase,
			SegmentedTabletRouterLeafLogicalIDLimit,
			SegmentedTabletRouterAnchorLogicalIDBase,
			SegmentedTabletRouterAnchorLogicalIDLimit,
			SegmentedTabletRouterFirstDynamicLogicalID,
		)
	}
	leafZero, ok := SegmentedTabletRouterLeafLogicalID(0)
	if !ok || leafZero != SegmentedTabletRouterLeafLogicalIDBase {
		t.Fatalf("bucket zero logical ID = %d,%v", leafZero, ok)
	}
	leafMax, ok := SegmentedTabletRouterLeafLogicalID(
		BucketID(PrimaryBucketIDLimit - 1),
	)
	if !ok || leafMax != SegmentedTabletRouterLeafLogicalIDLimit-1 {
		t.Fatalf("max bucket logical ID = %d,%v", leafMax, ok)
	}
	if _, ok := SegmentedTabletRouterLeafLogicalID(
		BucketID(PrimaryBucketIDLimit),
	); ok {
		t.Fatal("out-of-range BucketID accepted")
	}
	anchorZero, ok := SegmentedTabletRouterAnchorLogicalID(0, 0)
	if !ok || anchorZero != SegmentedTabletRouterAnchorLogicalIDBase {
		t.Fatalf("first anchor logical ID = %d,%v", anchorZero, ok)
	}
	anchorMax, ok := SegmentedTabletRouterAnchorLogicalID(
		TabletLocalIdentityTabletCount-1,
		SegmentedTabletRouterMaxPages-1,
	)
	if !ok || anchorMax != SegmentedTabletRouterAnchorLogicalIDLimit-1 {
		t.Fatalf("last anchor logical ID = %d,%v", anchorMax, ok)
	}
	if _, ok := SegmentedTabletRouterAnchorLogicalID(
		TabletLocalIdentityTabletCount, 0,
	); ok {
		t.Fatal("out-of-range tablet accepted")
	}
	if _, ok := SegmentedTabletRouterAnchorLogicalID(
		0, SegmentedTabletRouterMaxPages,
	); ok {
		t.Fatal("out-of-range anchor page accepted")
	}
	ids := []uint64{
		SegmentedTabletRouterStateRootLogicalID,
		leafZero, leafMax, anchorZero, anchorMax,
		SegmentedTabletRouterFirstDynamicLogicalID,
	}
	for left := range ids {
		for right := left + 1; right < len(ids); right++ {
			if ids[left] == ids[right] {
				t.Fatalf("namespace collision at logical ID %d", ids[left])
			}
		}
	}
	if SegmentedTabletRouterIsDynamicLogicalID(
		SegmentedTabletRouterFirstDynamicLogicalID-1,
	) || !SegmentedTabletRouterIsDynamicLogicalID(
		SegmentedTabletRouterFirstDynamicLogicalID,
	) {
		t.Fatal("dynamic logical-ID boundary is not exact")
	}
}

func TestSegmentedTabletRouterRejectsCollidingLogicalIDs(t *testing.T) {
	header, leaves, refs := segmentedTabletRouterTestInputs(t, 1)
	validLeafID := leaves[0].Ref.LogicalID
	validAnchorID := refs[0].LogicalID
	encode := func() error {
		_, _, _, _, err := EncodeSegmentedTabletRouter(
			make([]byte, SegmentedTabletRouterRootBytes),
			make([]byte, SegmentedTabletRouterLocatorBytes),
			make([]byte, SegmentedTabletRouterAnchorPageBytes),
			header, refs, leaves,
		)
		return err
	}
	bucket := segmentedTabletRouterTestBucket(leaves[0].Ref)
	for name, logicalID := range map[string]uint64{
		"state-root-as-leaf":     SegmentedTabletRouterStateRootLogicalID,
		"legacy-bucket-plus-one": uint64(bucket) + 1,
		"anchor-range-as-leaf":   SegmentedTabletRouterAnchorLogicalIDBase,
	} {
		t.Run(name, func(t *testing.T) {
			leaves[0].Ref.LogicalID = logicalID
			if err := encode(); err == nil {
				t.Fatalf("leaf logical ID %d accepted", logicalID)
			}
			leaves[0].Ref.LogicalID = validLeafID
		})
	}
	for name, logicalID := range map[string]uint64{
		"state-root-as-anchor": SegmentedTabletRouterStateRootLogicalID,
		"legacy-tablet-page": uint64(header.TabletID)*
			SegmentedTabletRouterMaxPages + 1,
		"leaf-range-as-anchor": SegmentedTabletRouterLeafLogicalIDBase,
	} {
		t.Run(name, func(t *testing.T) {
			refs[0].LogicalID = logicalID
			if err := encode(); err == nil {
				t.Fatalf("anchor logical ID %d accepted", logicalID)
			}
			refs[0].LogicalID = validAnchorID
		})
	}
}

func TestSegmentedTabletRouterRandomizedRouting(t *testing.T) {
	view, leaves, _ := segmentedTabletRouterTestView(t, 3072)
	random := rand.New(rand.NewSource(7))
	for iteration := range 20_000 {
		value := random.Intn(3072*230 + 230)
		key := []byte(fmt.Sprintf(
			"tenant/0042/document/%010d", value,
		))
		want := max(sort.Search(len(leaves), func(rank int) bool {
			return bytes.Compare(leaves[rank].Fence, key) > 0
		})-1, 0)
		got := view.RouteHashed(123, key)
		if got.Bucket != segmentedTabletRouterTestBucket(leaves[want].Ref) {
			t.Fatalf(
				"iteration %d route(%q) = %d, want rank %d bucket %d",
				iteration, key, got.Bucket, want,
				segmentedTabletRouterTestBucket(leaves[want].Ref),
			)
		}
	}
}

func TestSegmentedTabletRouterBinaryShortAndTiedHeads(t *testing.T) {
	header := SegmentedTabletRouterHeader{
		StoreID:  segmentedTabletRouterTestSeed,
		TabletID: 7, Generation: 100,
		AnchorKind: PagePrimaryAnchor, LeafKind: PagePrimaryLeaf,
	}
	fences := [][]byte{
		nil,
		{0},
		{0, 0},
		{0, 0, 0},
		{0, 0, 0, 0},
		{0, 0, 0, 0, 0},
		{0, 0, 0, 0, 1},
		{0, 0, 0, 1},
		{0, 0, 1},
		{0, 1},
		{1},
	}
	leaves := make([]SegmentedTabletRouterLeaf, len(fences))
	for rank, fence := range fences {
		bucket, _ := MakeTabletLocalIdentityBucket(
			header.TabletID, uint32(rank),
		)
		leaves[rank] = SegmentedTabletRouterLeaf{
			LocalID: uint16(rank), Fence: fence,
			Ref: PageRef{
				Offset: uint64(rank+1) * 8,
				LogicalID: segmentedTabletRouterTestLeafLogicalID(
					t, BucketID(bucket),
				),
				Generation: uint64(rank) + 1,
				Length:     uint32(rank) + 1,
				Kind:       header.LeafKind,
			},
		}
	}
	anchorRef := PageRef{
		Offset: SegmentedTabletRouterAnchorPageBytes,
		LogicalID: segmentedTabletRouterAnchorLogicalID(
			header.TabletID, 0,
		),
		Generation: header.Generation,
		Length:     SegmentedTabletRouterAnchorPageBytes,
		Kind:       header.AnchorKind,
	}
	root, locator, pages, _, err := EncodeSegmentedTabletRouter(
		make([]byte, SegmentedTabletRouterRootBytes),
		make([]byte, SegmentedTabletRouterLocatorBytes),
		make([]byte, SegmentedTabletRouterAnchorPageBytes),
		header, []PageRef{anchorRef}, leaves,
	)
	if err != nil {
		t.Fatal(err)
	}
	view, err := OpenSegmentedTabletRouter(root, locator, pages)
	if err != nil {
		t.Fatal(err)
	}
	if view.pages[0].headBytes != 4 {
		t.Fatalf("head width = %d, want 4", view.pages[0].headBytes)
	}
	queries := append([][]byte(nil), fences...)
	queries = append(queries,
		[]byte{0, 0, 0, 0, 0, 1},
		[]byte{0, 0, 0, 0, 2},
		[]byte{0, 0, 2},
		[]byte{2},
	)
	for _, key := range queries {
		want := max(sort.Search(len(fences), func(rank int) bool {
			return bytes.Compare(fences[rank], key) > 0
		})-1, 0)
		route := view.RouteHashed(9, key)
		if route.Bucket !=
			segmentedTabletRouterTestBucket(leaves[want].Ref) {
			t.Fatalf("route(%v) = %d, want rank %d bucket %d",
				key, route.Bucket, want,
				segmentedTabletRouterTestBucket(leaves[want].Ref))
		}
	}
}

func TestSegmentedTabletRouterExactLengthHandle(t *testing.T) {
	view, leaves, _ := segmentedTabletRouterTestView(t, 1024)
	for _, rank := range []int{0, 1, 127, 511, 1023} {
		bucket := segmentedTabletRouterTestBucket(leaves[rank].Ref)
		ref, _, ok := view.ResolveBucketID(bucket)
		if !ok || ref.Offset != leaves[rank].Ref.Offset ||
			ref.Length != leaves[rank].Ref.Length {
			t.Fatalf("exact handle rank %d = %+v,%v, want %+v",
				rank, ref, ok, leaves[rank].Ref)
		}
	}

	header, one, refs := segmentedTabletRouterTestInputs(t, 1)
	one[0].Ref.Offset = (uint64(1)<<48 - 1) << 3
	one[0].Ref.Length = 65536
	root, locator, pages, _, err := EncodeSegmentedTabletRouter(
		make([]byte, SegmentedTabletRouterRootBytes),
		make([]byte, SegmentedTabletRouterLocatorBytes),
		make([]byte, SegmentedTabletRouterAnchorPageBytes),
		header, refs, one,
	)
	if err != nil {
		t.Fatal(err)
	}
	maxView, err := OpenSegmentedTabletRouter(root, locator, pages)
	if err != nil {
		t.Fatal(err)
	}
	ref, _, ok := maxView.ResolveBucketID(
		segmentedTabletRouterTestBucket(one[0].Ref),
	)
	if !ok || ref.Offset != one[0].Ref.Offset || ref.Length != 65536 {
		t.Fatalf("maximum exact handle = %+v,%v", ref, ok)
	}
}

func TestSegmentedTabletRouterOrdinaryCOWIsLocalized(t *testing.T) {
	view, leaves, refs := segmentedTabletRouterTestView(t, 3072)
	target := 1537
	bucket := segmentedTabletRouterTestBucket(leaves[target].Ref)
	oldLocator := append([]byte(nil), view.locator...)
	oldPages := make([]byte, len(view.pages[0].image)*int(view.pageCount))
	for pageID := 0; pageID < int(view.pageCount); pageID++ {
		copy(oldPages[pageID*SegmentedTabletRouterAnchorPageBytes:],
			view.pages[pageID].image)
	}
	newLeaf := leaves[target].Ref
	newLeaf.Offset += 8
	newLeaf.Generation = 10_001
	newLeaf.Length = 317
	var newZone BucketZone
	copy(newZone[:], []byte{9, 8, 7, 6})
	pageID := uint8(target / SegmentedTabletRouterRowsPerPage)
	newAnchor := refs[pageID]
	newAnchor.Offset += 1 << 20
	newAnchor.Generation = 10_001
	result, err := view.RewriteHandle(
		make([]byte, SegmentedTabletRouterRootBytes),
		make([]byte, SegmentedTabletRouterAnchorPageBytes),
		10_001, bucket, newLeaf, newZone, newAnchor,
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.Bytes != SegmentedTabletRouterRootBytes+
		SegmentedTabletRouterAnchorPageBytes ||
		result.PageID != pageID ||
		!bytes.Equal(view.locator, oldLocator) {
		t.Fatalf("ordinary COW result = %+v, locator changed=%v",
			result, !bytes.Equal(view.locator, oldLocator))
	}
	nextPages := append([]byte(nil), oldPages...)
	copy(nextPages[int(pageID)*SegmentedTabletRouterAnchorPageBytes:],
		result.AnchorPage)
	next, err := OpenSegmentedTabletRouter(
		result.Root, view.locator, nextPages,
	)
	if err != nil {
		t.Fatal(err)
	}
	gotRef, gotZone, ok := next.ResolveBucketID(bucket)
	if !ok || gotRef != newLeaf || gotZone != newZone {
		t.Fatalf("rewritten handle = %+v/%x/%v", gotRef, gotZone, ok)
	}
	other := target - 1
	otherRef, otherZone, ok := next.ResolveBucketID(
		segmentedTabletRouterTestBucket(leaves[other].Ref),
	)
	if !ok || otherRef != leaves[other].Ref ||
		otherZone != leaves[other].Zone {
		t.Fatalf("neighbor changed = %+v/%x/%v", otherRef, otherZone, ok)
	}
}

func TestSegmentedTabletRouterStructuralSplitIsAtomic(t *testing.T) {
	view, leaves, refs := segmentedTabletRouterTestView(t, 3072)
	const sourcePageID = uint8(5)
	newPageID := view.pageCount
	leftRef := refs[sourcePageID]
	leftRef.Offset += 2 << 20
	leftRef.Generation = 10_001
	rightRef := PageRef{
		Offset: 3 << 20,
		LogicalID: segmentedTabletRouterAnchorLogicalID(
			view.tabletID, newPageID,
		),
		Generation: 10_001,
		Length:     SegmentedTabletRouterAnchorPageBytes,
		Kind:       view.anchorKind,
	}
	result, err := view.SplitAnchorPage(
		make([]byte, SegmentedTabletRouterRootBytes),
		make([]byte, SegmentedTabletRouterLocatorBytes),
		make([]byte, SegmentedTabletRouterAnchorPageBytes),
		make([]byte, SegmentedTabletRouterAnchorPageBytes),
		10_001, sourcePageID, newPageID, 128, leftRef, rightRef,
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.Bytes != SegmentedTabletRouterRootBytes+
		SegmentedTabletRouterLocatorBytes+
		2*SegmentedTabletRouterAnchorPageBytes {
		t.Fatalf("split bytes = %d", result.Bytes)
	}
	nextPages := make([]byte,
		(int(view.pageCount)+1)*SegmentedTabletRouterAnchorPageBytes)
	for pageID := 0; pageID < int(view.pageCount); pageID++ {
		copy(nextPages[pageID*SegmentedTabletRouterAnchorPageBytes:],
			view.pages[pageID].image)
	}
	copy(nextPages[int(sourcePageID)*SegmentedTabletRouterAnchorPageBytes:],
		result.LeftPage)
	copy(nextPages[int(newPageID)*SegmentedTabletRouterAnchorPageBytes:],
		result.RightPage)
	next, err := OpenSegmentedTabletRouter(
		result.Root, result.Locator, nextPages,
	)
	if err != nil {
		t.Fatal(err)
	}
	first := int(sourcePageID) * SegmentedTabletRouterRowsPerPage
	for _, rank := range []int{first, first + 127, first + 128, first + 255} {
		route := next.RouteHashed(7, leaves[rank].Fence)
		wantPage := sourcePageID
		if rank >= first+128 {
			wantPage = newPageID
		}
		if route.PageID != wantPage ||
			route.Bucket != segmentedTabletRouterTestBucket(leaves[rank].Ref) ||
			route.RowSlot != uint8(rank-first) {
			t.Fatalf("split route rank %d = %+v, want page %d",
				rank, route, wantPage)
		}
		ref, zone, ok := next.ResolveBucketID(route.Bucket)
		if !ok || ref != leaves[rank].Ref || zone != leaves[rank].Zone {
			t.Fatalf("split resolve rank %d = %+v/%x/%v",
				rank, ref, zone, ok)
		}
	}
}

func TestSegmentedTabletRouterReadPathsAllocateNothing(t *testing.T) {
	view, leaves, _ := segmentedTabletRouterTestView(t, 3072)
	key := leaves[1536].Fence
	hash := KeyHashBytes(segmentedTabletRouterTestSeed, key)
	bucket := segmentedTabletRouterTestBucket(leaves[1536].Ref)
	pages := make([]byte, int(view.pageCount)*
		SegmentedTabletRouterAnchorPageBytes)
	for pageID := 0; pageID < int(view.pageCount); pageID++ {
		copy(
			pages[pageID*SegmentedTabletRouterAnchorPageBytes:],
			view.pages[pageID].image,
		)
	}
	if got := testing.AllocsPerRun(1000, func() {
		_, _ = OpenSegmentedTabletRouter(view.root, view.locator, pages)
	}); got != 0 {
		t.Fatalf("Open allocations = %v", got)
	}
	if got := testing.AllocsPerRun(1000, func() {
		_ = view.RouteHashed(hash, key)
	}); got != 0 {
		t.Fatalf("RouteHashed allocations = %v", got)
	}
	if got := testing.AllocsPerRun(1000, func() {
		_, _, _ = view.ResolveBucketID(bucket)
	}); got != 0 {
		t.Fatalf("ResolveBucketID allocations = %v", got)
	}
	if got := testing.AllocsPerRun(1000, func() {
		cursor := view.LowerBound(key)
		_, _ = cursor.Route(hash)
		_ = cursor.Next()
	}); got != 0 {
		t.Fatalf("cursor allocations = %v", got)
	}
}

func TestSegmentedTabletRouterSpaceProjection(t *testing.T) {
	const leafCount = TabletLocalIdentityLocalCount
	view, _, _ := segmentedTabletRouterTestView(t, leafCount)
	if view.pageCount != SegmentedTabletRouterMaxPages {
		t.Fatalf("full tablet pages = %d", view.pageCount)
	}
	const persistentBytes = SegmentedTabletRouterRootBytes +
		SegmentedTabletRouterLocatorBytes +
		SegmentedTabletRouterMaxPages*
			SegmentedTabletRouterAnchorPageBytes
	if persistentBytes != 143360 ||
		float64(persistentBytes)/leafCount != 35 {
		t.Fatalf("full tablet = %d bytes, %.3f B/leaf",
			persistentBytes, float64(persistentBytes)/leafCount)
	}
	wants := map[int]float64{
		187: float64(persistentBytes) / float64(leafCount*187),
		195: float64(persistentBytes) / float64(leafCount*195),
		230: float64(persistentBytes) / float64(leafCount*230),
	}
	for rows, want := range wants {
		got := SegmentedTabletRouterRoutingBytesPerDocument(
			leafCount, rows,
		)
		if got != want || got >= 0.19 {
			t.Fatalf("%d rows = %.9f, want %.9f", rows, got, want)
		}
	}
}

func TestSegmentedTabletRouterRejectsCorruption(t *testing.T) {
	view, _, _ := segmentedTabletRouterTestView(t, 1024)
	root := append([]byte(nil), view.root...)
	locator := append([]byte(nil), view.locator...)
	pages := make([]byte, int(view.pageCount)*
		SegmentedTabletRouterAnchorPageBytes)
	for pageID := 0; pageID < int(view.pageCount); pageID++ {
		copy(pages[pageID*SegmentedTabletRouterAnchorPageBytes:],
			view.pages[pageID].image)
	}
	tests := []struct {
		name     string
		edit     func(root, locator, pages []byte)
		sealRoot bool
		sealPage int
	}{
		{
			name:     "root-checksum",
			edit:     func(root, _, _ []byte) { root[100] ^= 1 },
			sealPage: -1,
		},
		{
			name: "legacy-root-version",
			edit: func(root, _, _ []byte) {
				binary.LittleEndian.PutUint32(root[8:12], 2)
			},
			sealRoot: true, sealPage: -1,
		},
		{
			name: "root-store-id",
			edit: func(root, _, _ []byte) {
				root[44] ^= 0xff
			},
			sealRoot: true, sealPage: -1,
		},
		{
			name: "root-anchor-kind",
			edit: func(root, _, _ []byte) {
				root[15] = byte(PageTabletRoute)
			},
			sealRoot: true, sealPage: -1,
		},
		{
			name: "root-leaf-kind",
			edit: func(root, _, _ []byte) {
				root[16] = byte(PageDocument)
			},
			sealRoot: true, sealPage: -1,
		},
		{
			name: "root-generation-overflow",
			edit: func(root, _, _ []byte) {
				binary.LittleEndian.PutUint64(
					root[24:32], uint64(1)<<48,
				)
			},
			sealRoot: true, sealPage: -1,
		},
		{
			name: "future-anchor-generation",
			edit: func(root, _, _ []byte) {
				segmentedTabletRouterPutUint48(
					root[segmentedTabletRouterRootRefsAt+6:],
					view.generation+1,
				)
			},
			sealRoot: true, sealPage: -1,
		},
		{
			name:     "locator-binding",
			edit:     func(_, locator, _ []byte) { locator[0] ^= 1 },
			sealPage: -1,
		},
		{
			name: "duplicate-page-rank",
			edit: func(root, _, _ []byte) {
				root[segmentedTabletRouterRootRanksAt+1] =
					root[segmentedTabletRouterRootRanksAt]
			},
			sealRoot: true, sealPage: -1,
		},
		{
			name:     "anchor-checksum",
			edit:     func(_, _, pages []byte) { pages[100] ^= 1 },
			sealPage: -1,
		},
		{
			name: "legacy-common-anchor-version",
			edit: func(_, _, pages []byte) {
				binary.LittleEndian.PutUint16(pages[8:10], pageVersion-1)
			},
			sealPage: 0,
		},
		{
			name: "legacy-private-anchor-envelope",
			edit: func(_, _, pages []byte) {
				copy(pages[:8], "STRPAGE1")
			},
			sealPage: 0,
		},
		{
			name: "wrong-common-anchor-kind",
			edit: func(_, _, pages []byte) {
				pages[12] = byte(PageTabletRoute)
			},
			sealPage: 0,
		},
		{
			name: "cross-store-anchor",
			edit: func(_, _, pages []byte) {
				pages[40] ^= 0xff
			},
			sealPage: 0,
		},
		{
			name: "future-leaf-generation",
			edit: func(_, _, pages []byte) {
				slot := pages[segmentedTabletRouterAnchorRanksAt]
				segmentedTabletRouterPutUint48(
					pages[segmentedTabletRouterAnchorHandlesAt+
						int(slot)*SegmentedTabletRouterHandleBytes+6:],
					view.pages[0].generation+1,
				)
			},
			sealPage: 0,
		},
		{
			name: "duplicate-row-rank",
			edit: func(_, _, pages []byte) {
				pages[segmentedTabletRouterAnchorRanksAt+1] =
					pages[segmentedTabletRouterAnchorRanksAt]
			},
			sealPage: 0,
		},
		{
			name: "bad-compressed-offset",
			edit: func(_, _, pages []byte) {
				binary.LittleEndian.PutUint16(
					pages[segmentedTabletRouterAnchorOffsetsAt+2:],
					65535,
				)
			},
			sealPage: 0,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			nextRoot := append([]byte(nil), root...)
			nextLocator := append([]byte(nil), locator...)
			nextPages := append([]byte(nil), pages...)
			test.edit(nextRoot, nextLocator, nextPages)
			if test.sealRoot {
				segmentedTabletRouterSeal(
					nextRoot, segmentedTabletRouterRootTrailerAt,
				)
			}
			if test.sealPage >= 0 {
				image := nextPages[test.sealPage*
					SegmentedTabletRouterAnchorPageBytes:]
				segmentedTabletRouterSeal(
					image[:SegmentedTabletRouterAnchorPageBytes],
					segmentedTabletRouterAnchorTrailerAt,
				)
			}
			if _, err := OpenSegmentedTabletRouter(
				nextRoot, nextLocator, nextPages,
			); !errors.Is(err, ErrSegmentedTabletRouterCorrupt) {
				t.Fatalf("Open error = %v", err)
			}
		})
	}
}

func TestSegmentedTabletRouterGenerationBounds(t *testing.T) {
	header, leaves, refs := segmentedTabletRouterTestInputs(t, 1)
	encode := func() ([]byte, []byte, []byte, error) {
		root, locator, pages, _, err := EncodeSegmentedTabletRouter(
			make([]byte, SegmentedTabletRouterRootBytes),
			make([]byte, SegmentedTabletRouterLocatorBytes),
			make([]byte, SegmentedTabletRouterAnchorPageBytes),
			header, refs, leaves,
		)
		return root, locator, pages, err
	}

	t.Run("future-leaf-encode", func(t *testing.T) {
		old := leaves[0].Ref.Generation
		leaves[0].Ref.Generation = header.Generation + 1
		if _, _, _, err := encode(); err == nil {
			t.Fatal("future leaf generation accepted")
		}
		leaves[0].Ref.Generation = old
	})
	t.Run("future-anchor-encode", func(t *testing.T) {
		refs[0].Generation = header.Generation + 1
		if _, _, _, err := encode(); err == nil {
			t.Fatal("future anchor generation accepted")
		}
		refs[0].Generation = header.Generation
	})
	t.Run("maximum-generation", func(t *testing.T) {
		const maximum = uint64(1)<<48 - 1
		header.Generation = maximum
		refs[0].Generation = maximum
		root, locator, pages, err := encode()
		if err != nil {
			t.Fatal(err)
		}
		view, err := OpenSegmentedTabletRouter(root, locator, pages)
		if err != nil || view.generation != maximum {
			t.Fatalf("open maximum generation = %d,%v",
				view.generation, err)
		}
	})
	t.Run("overflow-generation", func(t *testing.T) {
		header.Generation = uint64(1) << 48
		refs[0].Generation = uint64(1)<<48 - 1
		if _, _, _, err := encode(); err == nil {
			t.Fatal("48-bit-overflow root generation accepted")
		}
	})

	view, currentLeaves, currentRefs :=
		segmentedTabletRouterTestView(t, 1)
	bucket := segmentedTabletRouterTestBucket(currentLeaves[0].Ref)
	const maximum = uint64(1)<<48 - 1
	leafRef := currentLeaves[0].Ref
	leafRef.Generation = maximum
	anchorRef := currentRefs[0]
	anchorRef.Generation = maximum
	if _, err := view.RewriteHandle(
		make([]byte, SegmentedTabletRouterRootBytes),
		make([]byte, SegmentedTabletRouterAnchorPageBytes),
		maximum, bucket, leafRef, BucketZone{}, anchorRef,
	); err != nil {
		t.Fatalf("maximum-generation COW: %v", err)
	}
	leafRef.Generation = uint64(1) << 48
	anchorRef.Generation = uint64(1) << 48
	if _, err := view.RewriteHandle(
		make([]byte, SegmentedTabletRouterRootBytes),
		make([]byte, SegmentedTabletRouterAnchorPageBytes),
		uint64(1)<<48, bucket, leafRef, BucketZone{}, anchorRef,
	); err == nil {
		t.Fatal("48-bit-overflow COW generation accepted")
	}
}

func TestSegmentedTabletRouterRejectsInvalidWrites(t *testing.T) {
	header, leaves, refs := segmentedTabletRouterTestInputs(t, 256)
	encode := func() error {
		_, _, _, _, err := EncodeSegmentedTabletRouter(
			make([]byte, SegmentedTabletRouterRootBytes),
			make([]byte, SegmentedTabletRouterLocatorBytes),
			make([]byte, SegmentedTabletRouterAnchorPageBytes),
			header, refs, leaves,
		)
		return err
	}
	t.Run("duplicate-local-id", func(t *testing.T) {
		old := leaves[1].LocalID
		leaves[1].LocalID = leaves[0].LocalID
		leaves[1].Ref.LogicalID = leaves[0].Ref.LogicalID
		if err := encode(); err == nil {
			t.Fatal("duplicate LocalID accepted")
		}
		leaves[1].LocalID = old
		bucket, _ := MakeTabletLocalIdentityBucket(
			header.TabletID, uint32(old),
		)
		leaves[1].Ref.LogicalID =
			segmentedTabletRouterTestLeafLogicalID(t, BucketID(bucket))
	})
	t.Run("unaligned-cold-offset", func(t *testing.T) {
		leaves[1].Ref.Offset++
		if err := encode(); err == nil {
			t.Fatal("unaligned leaf accepted")
		}
		leaves[1].Ref.Offset--
	})
	t.Run("zero-length", func(t *testing.T) {
		old := leaves[1].Ref.Length
		leaves[1].Ref.Length = 0
		if err := encode(); err == nil {
			t.Fatal("zero length accepted")
		}
		leaves[1].Ref.Length = old
	})
	t.Run("zero-store-id", func(t *testing.T) {
		old := header.StoreID
		header.StoreID = [16]byte{}
		if err := encode(); err == nil {
			t.Fatal("zero StoreID accepted")
		}
		header.StoreID = old
	})
	t.Run("wrong-anchor-kind", func(t *testing.T) {
		header.AnchorKind = PageTabletRoute
		refs[0].Kind = PageTabletRoute
		if err := encode(); err == nil {
			t.Fatal("non-primary anchor kind accepted")
		}
		header.AnchorKind = PagePrimaryAnchor
		refs[0].Kind = PagePrimaryAnchor
	})
	t.Run("wrong-leaf-kind", func(t *testing.T) {
		header.LeafKind = PageDocument
		for rank := range leaves {
			leaves[rank].Ref.Kind = PageDocument
		}
		if err := encode(); err == nil {
			t.Fatal("non-primary leaf kind accepted")
		}
		header.LeafKind = PagePrimaryLeaf
		for rank := range leaves {
			leaves[rank].Ref.Kind = PagePrimaryLeaf
		}
	})
}

func FuzzOpenSegmentedTabletRouter(f *testing.F) {
	view, _, _ := segmentedTabletRouterTestView(f, 32)
	pages := append([]byte(nil), view.pages[0].image...)
	f.Add(
		append([]byte(nil), view.root...),
		append([]byte(nil), view.locator...),
		pages,
	)
	f.Fuzz(func(t *testing.T, root, locator, pages []byte) {
		_, _ = OpenSegmentedTabletRouter(root, locator, pages)
	})
}
