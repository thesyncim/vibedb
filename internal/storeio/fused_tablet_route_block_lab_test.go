package storeio

import (
	"bytes"
	"encoding/binary"
	"errors"
	"sort"
	"testing"
)

var fusedTabletRouteBlockLabTestStoreID = [16]byte{
	'f', 'u', 's', 'e', 'd', '-', 'r', 'o',
	'u', 't', 'e', '-', 'b', 'l', 'o', 'k',
}

var fusedTabletRouteBlockLabTestBounds = GlobalTabletCatalogBounds{
	StoreID:                fusedTabletRouteBlockLabTestStoreID,
	SelectedRootGeneration: 100,
	FileEnd:                1 << 44,
	NextLogicalID:          FusedTabletRouteBlockLabFirstDynamicID + 1<<20,
}

func fusedTabletRouteBlockLabTestRef(
	offset, logicalID, generation uint64, length uint32, kind PageKind,
) PageRef {
	return PageRef{
		Offset: offset, LogicalID: logicalID, Generation: generation,
		Length: length, Kind: kind,
	}
}

func fusedTabletRouteBlockLabTestTablets(
	t testing.TB,
) []FusedTabletRouteBlockLabTablet {
	t.Helper()
	ids := []uint32{7, 19, 11}
	floors := [][]byte{nil, []byte("m"), []byte("z")}
	pageIDs := []uint8{2, 0, 1}
	anchorFloors := [][]byte{nil, []byte("g"), []byte("t")}
	tablets := make([]FusedTabletRouteBlockLabTablet, len(ids))
	for at, tabletID := range ids {
		locatorLogical, _ := GlobalTabletCatalogLocatorLogicalID(tabletID)
		tablet := FusedTabletRouteBlockLabTablet{
			Floor: floors[at], TabletID: tabletID,
			StableSlot: uint8((at*5 + 2) & 0xff),
			Locator: fusedTabletRouteBlockLabTestRef(
				uint64(100+at)*GlobalTabletCatalogLocatorBytes,
				locatorLogical, uint64(70+at),
				GlobalTabletCatalogLocatorBytes,
				PagePrimaryLocator,
			),
			Anchors: make([]FusedTabletRouteBlockLabAnchor, len(pageIDs)),
		}
		for rank, pageID := range pageIDs {
			logicalID, _ := GlobalTabletCatalogAnchorLogicalID(
				tabletID, pageID,
			)
			tablet.Anchors[rank] = FusedTabletRouteBlockLabAnchor{
				Floor: anchorFloors[rank], PageID: pageID,
				Ref: fusedTabletRouteBlockLabTestRef(
					uint64(1000+at*16+rank)*
						SegmentedTabletRouterAnchorPageBytes,
					logicalID, uint64(80+at*3+rank),
					SegmentedTabletRouterAnchorPageBytes,
					PagePrimaryAnchor,
				),
			}
		}
		tablets[at] = tablet
	}
	return tablets
}

func fusedTabletRouteBlockLabTestView(
	t testing.TB,
) (
	FusedTabletRouteBlockLabView,
	[]byte,
	PageRef,
	[]FusedTabletRouteBlockLabTablet,
) {
	t.Helper()
	const blockID = uint32(3)
	logicalID, _ := FusedTabletRouteBlockLabLogicalID(blockID)
	tablets := fusedTabletRouteBlockLabTestTablets(t)
	image, err := EncodeFusedTabletRouteBlockLab(
		make([]byte, FusedTabletRouteBlockLabBytes),
		FusedTabletRouteBlockLabHeader{
			StoreID:    fusedTabletRouteBlockLabTestStoreID,
			Generation: 10, SelectedRootGeneration: 100,
			LogicalID: logicalID, BlockID: blockID,
			Kind:        PageTabletRoute,
			LocatorKind: PagePrimaryLocator,
			AnchorKind:  PagePrimaryAnchor, LeafKind: PagePrimaryLeaf,
			Bounds: fusedTabletRouteBlockLabTestBounds,
		},
		tablets,
	)
	if err != nil {
		t.Fatal(err)
	}
	ref := fusedTabletRouteBlockLabTestRef(
		8<<20, logicalID, 10,
		FusedTabletRouteBlockLabBytes, PageTabletRoute,
	)
	view, err := OpenFusedTabletRouteBlockLab(
		image, ref, 100, fusedTabletRouteBlockLabTestBounds,
	)
	if err != nil {
		t.Fatal(err)
	}
	return view, image, ref, tablets
}

func TestFusedTabletRouteBlockLabRejectsCrossStoreAndStaleSnapshotGrafts(
	t *testing.T,
) {
	_, image, ref, _ := fusedTabletRouteBlockLabTestView(t)
	crossStore := fusedTabletRouteBlockLabTestBounds
	crossStore.StoreID[0] ^= 1
	if _, err := OpenFusedTabletRouteBlockLab(
		image, ref, crossStore.SelectedRootGeneration, crossStore,
	); !errors.Is(err, ErrFusedTabletRouteBlockLabCorrupt) {
		t.Fatalf("cross-Store fused-block graft error = %v", err)
	}
	staleSnapshot := fusedTabletRouteBlockLabTestBounds
	staleSnapshot.SelectedRootGeneration = ref.Generation - 1
	if _, err := OpenFusedTabletRouteBlockLab(
		image, ref, staleSnapshot.SelectedRootGeneration, staleSnapshot,
	); !errors.Is(err, ErrFusedTabletRouteBlockLabCorrupt) {
		t.Fatalf("stale-generation fused-block graft error = %v", err)
	}
	if _, err := OpenFusedTabletRouteBlockLab(
		image, ref, fusedTabletRouteBlockLabTestBounds.SelectedRootGeneration-1,
		fusedTabletRouteBlockLabTestBounds,
	); !errors.Is(err, ErrFusedTabletRouteBlockLabCorrupt) {
		t.Fatalf("split snapshot context error = %v", err)
	}
}

func TestFusedTabletRouteBlockLabExactRoutePostingScanAndCOW(
	t *testing.T,
) {
	view, image, ref, tablets := fusedTabletRouteBlockLabTestView(t)
	for _, test := range []struct {
		key    string
		tablet int
		anchor int
	}{
		{"", 0, 0},
		{"h", 0, 1},
		{"n", 1, 1},
		{"u", 1, 2},
		{"z", 2, 2},
		{"zz", 2, 2},
	} {
		route, ok := view.RouteAnchor([]byte(test.key))
		wantTablet := tablets[test.tablet]
		wantAnchor := wantTablet.Anchors[test.anchor]
		if !ok || route.TabletID != wantTablet.TabletID ||
			route.TabletOrdinal != uint16(test.tablet) ||
			route.TabletSlot != wantTablet.StableSlot ||
			route.LocatorRef != wantTablet.Locator ||
			route.PageID != wantAnchor.PageID ||
			route.AnchorRef != wantAnchor.Ref {
			t.Fatalf("route %q = %+v,%v", test.key, route, ok)
		}
	}
	bucket, _ := MakeTabletLocalIdentityBucket(tablets[1].TabletID, 97)
	resolved, ok := view.ResolveTablet(BucketID(bucket))
	locator, locatorOK := resolved.LocatorRef()
	if !ok || !locatorOK || resolved.TabletID() != tablets[1].TabletID ||
		resolved.StableSlot() != tablets[1].StableSlot ||
		locator != tablets[1].Locator {
		t.Fatalf("posting tablet = %d/%+v,%v,%v",
			resolved.TabletID(), locator, ok, locatorOK)
	}
	resolved, ok = view.ResolveStableSlot(tablets[1].StableSlot)
	inverseID, inverseOK := view.TabletIDAt(tablets[1].StableSlot)
	if !ok || !inverseOK || resolved.TabletID() != tablets[1].TabletID ||
		inverseID != tablets[1].TabletID {
		t.Fatalf("stable inverse = %d/%d,%v,%v",
			resolved.TabletID(), inverseID, ok, inverseOK)
	}
	if _, ok := view.ResolveStableSlot(255); ok {
		t.Fatal("resolved absent stable slot")
	}

	cursor := view.LowerBound([]byte("n"))
	for want := 1; want < len(tablets); want++ {
		tablet, ok := cursor.Tablet()
		if !ok || tablet.TabletID() != tablets[want].TabletID {
			t.Fatalf("tablet cursor %d = %d,%v", want, tablet.TabletID(), ok)
		}
		anchors := tablet.LowerBound(nil)
		for rank := 0; rank < len(tablets[want].Anchors); rank++ {
			route, ok := anchors.Route()
			if !ok || route.PageID != tablets[want].Anchors[rank].PageID {
				t.Fatalf("anchor cursor %d/%d = %+v,%v",
					want, rank, route, ok)
			}
			if rank+1 < len(tablets[want].Anchors) && !anchors.Next() {
				t.Fatalf("anchor cursor stopped at %d/%d", want, rank)
			}
		}
		if want+1 < len(tablets) && !cursor.Next() {
			t.Fatalf("tablet cursor stopped at %d", want)
		}
	}
	ordered := view.OrderedLowerBound([]byte("n"))
	for tabletAt := 1; tabletAt < len(tablets); tabletAt++ {
		firstAnchor := 0
		if tabletAt == 1 {
			firstAnchor = 1
		}
		for anchorAt := firstAnchor; anchorAt < len(tablets[tabletAt].Anchors); anchorAt++ {
			tabletID, pageID, anchorRef, ok := ordered.AnchorRef()
			if !ok || tabletID != tablets[tabletAt].TabletID ||
				pageID != tablets[tabletAt].Anchors[anchorAt].PageID ||
				anchorRef != tablets[tabletAt].Anchors[anchorAt].Ref {
				t.Fatalf("ordered cursor %d/%d = %d/%d/%+v,%v",
					tabletAt, anchorAt, tabletID, pageID, anchorRef, ok)
			}
			last := tabletAt+1 == len(tablets) &&
				anchorAt+1 == len(tablets[tabletAt].Anchors)
			if !last {
				if !ordered.Next() {
					t.Fatalf("ordered cursor stopped at %d/%d",
						tabletAt, anchorAt)
				}
			}
		}
	}

	replacement := tablets[1].Anchors[1].Ref
	replacement.Offset += 1 << 20
	replacement.Generation = 99
	before := bytes.Clone(image)
	if _, err := view.RewriteReferences(
		image, 20, 100,
		fusedTabletRouteBlockLabTestBounds,
		FusedTabletRouteBlockLabRefMutation{
			TabletID: tablets[1].TabletID, ReplaceAnchor: true,
			AnchorPageID: tablets[1].Anchors[1].PageID,
			Anchor:       replacement,
		},
	); err == nil {
		t.Fatal("accepted overlapping route-block COW")
	}
	if !bytes.Equal(image, before) {
		t.Fatal("overlap rejection changed old snapshot")
	}
	next, err := view.RewriteReferences(
		make([]byte, FusedTabletRouteBlockLabBytes), 20, 100,
		fusedTabletRouteBlockLabTestBounds,
		FusedTabletRouteBlockLabRefMutation{
			TabletID: tablets[1].TabletID, ReplaceAnchor: true,
			AnchorPageID: tablets[1].Anchors[1].PageID,
			Anchor:       replacement,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	nextRef := ref
	nextRef.Generation = 20
	nextView, err := OpenFusedTabletRouteBlockLab(
		next, nextRef, 100, fusedTabletRouteBlockLabTestBounds,
	)
	if err != nil {
		t.Fatal(err)
	}
	if nextView.floors.Header().Generation != nextRef.Generation {
		t.Fatalf(
			"embedded map birth = %d, want enclosing birth %d",
			nextView.floors.Header().Generation, nextRef.Generation,
		)
	}
	nextRoute, ok := nextView.RouteAnchor([]byte("n"))
	if !ok || nextRoute.AnchorRef != replacement {
		t.Fatalf("COW route = %+v,%v", nextRoute, ok)
	}
	oldRoute, _ := view.RouteAnchor([]byte("n"))
	if oldRoute.AnchorRef != tablets[1].Anchors[1].Ref {
		t.Fatal("COW changed old snapshot")
	}
}

func TestFusedTabletRouteBlockLabStructuralInsertRemovePreservesStableSlots(
	t *testing.T,
) {
	view, _, ref, tablets := fusedTabletRouteBlockLabTestView(t)
	inserted := tablets[1]
	inserted.Floor = []byte("s")
	inserted.TabletID = 23
	inserted.StableSlot = 99
	inserted.Anchors = append(
		[]FusedTabletRouteBlockLabAnchor(nil), inserted.Anchors...,
	)
	locatorLogical, _ := GlobalTabletCatalogLocatorLogicalID(
		inserted.TabletID,
	)
	inserted.Locator.LogicalID = locatorLogical
	for rank := range inserted.Anchors {
		logicalID, _ := GlobalTabletCatalogAnchorLogicalID(
			inserted.TabletID, inserted.Anchors[rank].PageID,
		)
		inserted.Anchors[rank].Ref.LogicalID = logicalID
	}
	scratch := new(FusedTabletRouteBlockLabStructuralScratch)
	image, err := view.InsertTablet(
		make([]byte, FusedTabletRouteBlockLabBytes),
		20, 100, fusedTabletRouteBlockLabTestBounds, inserted, scratch,
	)
	if err != nil {
		t.Fatal(err)
	}
	ref.Generation = 20
	next, err := OpenFusedTabletRouteBlockLab(
		image, ref, 100, fusedTabletRouteBlockLabTestBounds,
	)
	if err != nil {
		t.Fatal(err)
	}
	route, ok := next.RouteAnchor([]byte("t"))
	moved, movedOK := next.ResolveStableSlot(tablets[2].StableSlot)
	if !ok || route.TabletID != inserted.TabletID ||
		route.TabletOrdinal != 2 || route.TabletSlot != inserted.StableSlot ||
		!movedOK || moved.TabletID() != tablets[2].TabletID ||
		moved.ordinal != 3 {
		t.Fatalf("insert route=%+v,%v moved=%+v,%v",
			route, ok, moved, movedOK)
	}
	removedImage, err := next.RemoveTablet(
		make([]byte, FusedTabletRouteBlockLabBytes),
		30, 100, fusedTabletRouteBlockLabTestBounds,
		tablets[0].StableSlot, scratch,
	)
	if err != nil {
		t.Fatal(err)
	}
	ref.Generation = 30
	removed, err := OpenFusedTabletRouteBlockLab(
		removedImage, ref, 100, fusedTabletRouteBlockLabTestBounds,
	)
	if err != nil {
		t.Fatal(err)
	}
	first, ok := removed.RouteTablet(nil)
	if !ok || first.TabletID() != tablets[1].TabletID ||
		first.StableSlot() != tablets[1].StableSlot {
		t.Fatalf("promoted first = %+v,%v", first, ok)
	}
	if _, ok := removed.ResolveStableSlot(tablets[0].StableSlot); ok {
		t.Fatal("removed stable slot remained present")
	}
}

func TestFusedTabletRouteBlockLabCOWIsCanonicalAcrossHistories(t *testing.T) {
	const blockID = uint32(12)
	logicalID, _ := FusedTabletRouteBlockLabLogicalID(blockID)
	tablets := fusedTabletRouteBlockLabTestTablets(t)
	encodeAt := func(generation uint64) ([]byte, PageRef) {
		t.Helper()
		image, err := EncodeFusedTabletRouteBlockLab(
			make([]byte, FusedTabletRouteBlockLabBytes),
			FusedTabletRouteBlockLabHeader{
				StoreID:    fusedTabletRouteBlockLabTestStoreID,
				Generation: generation, SelectedRootGeneration: 100,
				LogicalID: logicalID, BlockID: blockID,
				Kind:        PageTabletRoute,
				LocatorKind: PagePrimaryLocator,
				AnchorKind:  PagePrimaryAnchor, LeafKind: PagePrimaryLeaf,
				Bounds: fusedTabletRouteBlockLabTestBounds,
			},
			tablets,
		)
		if err != nil {
			t.Fatal(err)
		}
		return image, fusedTabletRouteBlockLabTestRef(
			12<<20, logicalID, generation,
			FusedTabletRouteBlockLabBytes, PageTabletRoute,
		)
	}
	replacement := tablets[1].Anchors[1].Ref
	replacement.Offset += 2 << 20
	replacement.Generation = 99
	mutation := FusedTabletRouteBlockLabRefMutation{
		TabletID: tablets[1].TabletID, ReplaceAnchor: true,
		AnchorPageID: tablets[1].Anchors[1].PageID,
		Anchor:       replacement,
	}
	rewrite := func(ancestor uint64) []byte {
		t.Helper()
		image, ref := encodeAt(ancestor)
		view, err := OpenFusedTabletRouteBlockLab(
			image, ref, 100, fusedTabletRouteBlockLabTestBounds,
		)
		if err != nil {
			t.Fatal(err)
		}
		next, err := view.RewriteReferences(
			make([]byte, FusedTabletRouteBlockLabBytes),
			20, 100, fusedTabletRouteBlockLabTestBounds, mutation,
		)
		if err != nil {
			t.Fatal(err)
		}
		return next
	}
	fromFive := rewrite(5)
	fromSeven := rewrite(7)
	if !bytes.Equal(fromFive, fromSeven) {
		t.Fatal("equivalent fused COW depends on ancestor generation")
	}

	corrupt := bytes.Clone(fromFive)
	payload := corrupt[PageHeaderSize:]
	mapStart := FusedTabletRouteBlockLabHeaderBytes
	binary.LittleEndian.PutUint64(payload[mapStart+24:], 19)
	tabletAnchorMapLabSeal(
		payload[mapStart : mapStart+
			int(binary.LittleEndian.Uint16(payload[10:12]))],
	)
	if _, err := sealInitializedPage(corrupt); err != nil {
		t.Fatal(err)
	}
	expected := fusedTabletRouteBlockLabTestRef(
		12<<20, logicalID, 20,
		FusedTabletRouteBlockLabBytes, PageTabletRoute,
	)
	if _, err := OpenFusedTabletRouteBlockLab(
		corrupt, expected, 100, fusedTabletRouteBlockLabTestBounds,
	); err == nil {
		t.Fatal("accepted embedded map born before its enclosing block")
	}
}

func TestFusedTabletRouteBlockLabCOWBatchAccumulatesEveryEdit(
	t *testing.T,
) {
	view, _, ref, tablets := fusedTabletRouteBlockLabTestView(t)
	locator := tablets[0].Locator
	locator.Offset += 3 << 20
	locator.Generation = 99
	lastAnchor := tablets[2].Anchors[2].Ref
	lastAnchor.Offset += 4 << 20
	lastAnchor.Generation = 99
	middleAnchor := tablets[1].Anchors[1].Ref
	middleAnchor.Offset += 5 << 20
	middleAnchor.Generation = 99

	// TabletIDs are intentionally not lexical in the fixture. The batch order
	// is the exact stable-ID order required by RewriteReferenceBatch.
	mutations := []FusedTabletRouteBlockLabRefMutation{
		{
			TabletID: tablets[0].TabletID, ReplaceLocator: true,
			Locator: locator,
		},
		{
			TabletID: tablets[2].TabletID, ReplaceAnchor: true,
			AnchorPageID: tablets[2].Anchors[2].PageID,
			Anchor:       lastAnchor,
		},
		{
			TabletID: tablets[1].TabletID, ReplaceAnchor: true,
			AnchorPageID: tablets[1].Anchors[1].PageID,
			Anchor:       middleAnchor,
		},
	}
	next, err := view.RewriteReferenceBatch(
		make([]byte, FusedTabletRouteBlockLabBytes),
		20, 100, fusedTabletRouteBlockLabTestBounds, mutations,
	)
	if err != nil {
		t.Fatal(err)
	}
	nextRef := ref
	nextRef.Generation = 20
	nextView, err := OpenFusedTabletRouteBlockLab(
		next, nextRef, 100, fusedTabletRouteBlockLabTestBounds,
	)
	if err != nil {
		t.Fatal(err)
	}
	bucket, _ := MakeTabletLocalIdentityBucket(
		tablets[0].TabletID, 1,
	)
	first, ok := nextView.ResolveTablet(BucketID(bucket))
	gotLocator, locatorOK := first.LocatorRef()
	if !ok || !locatorOK || gotLocator != locator {
		t.Fatalf(
			"batched locator = %+v,%v,%v, want %+v",
			gotLocator, ok, locatorOK, locator,
		)
	}
	last, ok := nextView.RouteAnchor([]byte("z"))
	if !ok || last.TabletID != tablets[2].TabletID ||
		last.AnchorRef != lastAnchor {
		t.Fatalf("batched last route = %+v,%v", last, ok)
	}
	middle, ok := nextView.RouteAnchor([]byte("n"))
	if !ok || middle.TabletID != tablets[1].TabletID ||
		middle.AnchorRef != middleAnchor {
		t.Fatalf("batched middle route = %+v,%v", middle, ok)
	}

	oldBucket, _ := MakeTabletLocalIdentityBucket(
		tablets[0].TabletID, 1,
	)
	oldFirst, _ := view.ResolveTablet(BucketID(oldBucket))
	oldLocator, _ := oldFirst.LocatorRef()
	oldLast, _ := view.RouteAnchor([]byte("z"))
	oldMiddle, _ := view.RouteAnchor([]byte("n"))
	if oldLocator != tablets[0].Locator ||
		oldLast.AnchorRef != tablets[2].Anchors[2].Ref ||
		oldMiddle.AnchorRef != tablets[1].Anchors[1].Ref {
		t.Fatal("batched COW changed the admitted ancestor")
	}
}

func TestFusedTabletRouteBlockLabCOWBatchPrevalidatesWithoutMutation(
	t *testing.T,
) {
	view, _, _, tablets := fusedTabletRouteBlockLabTestView(t)
	replacement := tablets[0].Locator
	replacement.Offset += 6 << 20
	replacement.Generation = 99
	dst := bytes.Repeat([]byte{0xa5}, FusedTabletRouteBlockLabBytes)
	before := bytes.Clone(dst)
	valid := FusedTabletRouteBlockLabRefMutation{
		TabletID: tablets[0].TabletID, ReplaceLocator: true,
		Locator: replacement,
	}
	for _, mutations := range [][]FusedTabletRouteBlockLabRefMutation{
		{
			valid,
			{
				TabletID:       tablets[2].TabletID,
				ReplaceLocator: true, Locator: replacement,
			},
			valid,
		},
		{valid, valid},
	} {
		if _, err := view.RewriteReferenceBatch(
			dst, 20, 100, fusedTabletRouteBlockLabTestBounds, mutations,
		); err == nil {
			t.Fatalf("accepted invalid mutation batch: %+v", mutations)
		}
		if !bytes.Equal(dst, before) {
			t.Fatal("batch prevalidation failure changed destination")
		}
	}
}

func TestFusedTabletRouteBlockLabCOWRequiresMonotonicSnapshot(t *testing.T) {
	view, _, ref, tablets := fusedTabletRouteBlockLabTestView(t)
	replacement := tablets[1].Anchors[1].Ref
	replacement.Generation = 99
	mutation := FusedTabletRouteBlockLabRefMutation{
		TabletID: tablets[1].TabletID, ReplaceAnchor: true,
		AnchorPageID: tablets[1].Anchors[1].PageID, Anchor: replacement,
	}
	dst := make([]byte, FusedTabletRouteBlockLabBytes)
	before := bytes.Clone(dst)

	if _, err := view.RewriteReferences(
		dst, 20, 99, fusedTabletRouteBlockLabTestBounds, mutation,
	); err == nil {
		t.Fatal("accepted lower selected-root generation")
	}
	if !bytes.Equal(dst, before) {
		t.Fatal("generation rejection changed destination")
	}

	replacement.Offset = fusedTabletRouteBlockLabTestBounds.FileEnd
	replacement.Generation = 101
	mutation.Anchor = replacement
	if _, err := view.RewriteReferences(
		dst, 20, 101, fusedTabletRouteBlockLabTestBounds, mutation,
	); err == nil {
		t.Fatal("accepted appended child under source file bounds")
	}
	if !bytes.Equal(dst, before) {
		t.Fatal("bounds rejection changed destination")
	}

	expanded := fusedTabletRouteBlockLabTestBounds
	expanded.FileEnd += SegmentedTabletRouterAnchorPageBytes
	expanded.SelectedRootGeneration = 101
	next, err := view.RewriteReferences(
		dst, 20, 101, expanded, mutation,
	)
	if err != nil {
		t.Fatal(err)
	}
	nextRef := ref
	nextRef.Generation = 20
	nextView, err := OpenFusedTabletRouteBlockLab(next, nextRef, 101, expanded)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := nextView.RouteAnchor([]byte("n"))
	if !ok || got.AnchorRef != replacement {
		t.Fatalf("expanded-bounds route = %+v,%v, want %+v", got, ok, replacement)
	}
}

func TestFusedTabletRouteBlockLabPhysicalBirthUsesSnapshotGeneration(
	t *testing.T,
) {
	view, image, ref, tablets := fusedTabletRouteBlockLabTestView(t)
	if view.header.Generation != 10 ||
		tablets[2].Locator.Generation <= view.header.Generation ||
		tablets[2].Anchors[2].Ref.Generation <= view.header.Generation {
		t.Fatal("test does not exercise child newer than parent birth")
	}
	if _, err := OpenFusedTabletRouteBlockLab(
		image, ref, 87, fusedTabletRouteBlockLabTestBounds,
	); err == nil {
		t.Fatal("accepted child newer than selected snapshot root")
	}
	if _, err := OpenFusedTabletRouteBlockLab(
		image, ref, 9, fusedTabletRouteBlockLabTestBounds,
	); err == nil {
		t.Fatal("accepted block newer than selected snapshot root")
	}
	bad := fusedTabletRouteBlockLabTestTablets(t)
	bad[0].Anchors[0].Ref.Generation = 101
	logicalID, _ := FusedTabletRouteBlockLabLogicalID(4)
	if _, err := EncodeFusedTabletRouteBlockLab(
		make([]byte, FusedTabletRouteBlockLabBytes),
		FusedTabletRouteBlockLabHeader{
			StoreID:    fusedTabletRouteBlockLabTestStoreID,
			Generation: 10, SelectedRootGeneration: 100,
			LogicalID: logicalID, BlockID: 4, Kind: PageTabletRoute,
			LocatorKind: PagePrimaryLocator,
			AnchorKind:  PagePrimaryAnchor, LeafKind: PagePrimaryLeaf,
			Bounds: fusedTabletRouteBlockLabTestBounds,
		},
		bad,
	); err == nil {
		t.Fatal("encoded child newer than selected snapshot root")
	}
}

func TestFusedTabletRouteBlockLabBoundsAndGraftFailClosed(t *testing.T) {
	view, image, ref, tablets := fusedTabletRouteBlockLabTestView(t)
	bounds := fusedTabletRouteBlockLabTestBounds
	bounds.FileEnd = tablets[2].Anchors[2].Ref.Offset +
		uint64(tablets[2].Anchors[2].Ref.Length) - 1
	if _, err := OpenFusedTabletRouteBlockLab(
		image, ref, 100, bounds,
	); err == nil {
		t.Fatal("accepted child crossing snapshot file end")
	}
	bounds = fusedTabletRouteBlockLabTestBounds
	bounds.NextLogicalID = FusedTabletRouteBlockLabLogicalIDBase
	if _, err := OpenFusedTabletRouteBlockLab(
		image, ref, 100, bounds,
	); err == nil {
		t.Fatal("accepted route block outside logical namespace")
	}
	corrupt := bytes.Clone(image)
	corrupt[PageHeaderSize+FusedTabletRouteBlockLabHeaderBytes+7] ^= 1
	if _, err := OpenFusedTabletRouteBlockLab(
		corrupt, ref, 100, fusedTabletRouteBlockLabTestBounds,
	); err == nil {
		t.Fatal("accepted checksum-invalid route block")
	}
	route, _ := view.RouteAnchor([]byte("n"))
	route.AnchorRef.Offset += 4096
	if _, err := OpenFusedTabletRouteBlockLabAnchor(
		make([]byte, SegmentedTabletRouterAnchorPageBytes), route,
	); !errors.Is(err, ErrFusedTabletRouteBlockLabCorrupt) {
		t.Fatalf("grafted route error = %v", err)
	}
	duplicate := fusedTabletRouteBlockLabTestTablets(t)
	duplicate[2].TabletID = duplicate[0].TabletID
	logicalID, _ := FusedTabletRouteBlockLabLogicalID(5)
	if _, err := EncodeFusedTabletRouteBlockLab(
		make([]byte, FusedTabletRouteBlockLabBytes),
		FusedTabletRouteBlockLabHeader{
			StoreID:    fusedTabletRouteBlockLabTestStoreID,
			Generation: 10, SelectedRootGeneration: 100,
			LogicalID: logicalID, BlockID: 5, Kind: PageTabletRoute,
			LocatorKind: PagePrimaryLocator,
			AnchorKind:  PagePrimaryAnchor, LeafKind: PagePrimaryLeaf,
			Bounds: fusedTabletRouteBlockLabTestBounds,
		},
		duplicate,
	); err == nil {
		t.Fatal("accepted duplicate stable TabletID")
	}
	duplicate = fusedTabletRouteBlockLabTestTablets(t)
	duplicate[2].StableSlot = duplicate[0].StableSlot
	if _, err := EncodeFusedTabletRouteBlockLab(
		make([]byte, FusedTabletRouteBlockLabBytes),
		FusedTabletRouteBlockLabHeader{
			StoreID:    fusedTabletRouteBlockLabTestStoreID,
			Generation: 10, SelectedRootGeneration: 100,
			LogicalID: logicalID, BlockID: 5, Kind: PageTabletRoute,
			LocatorKind: PagePrimaryLocator,
			AnchorKind:  PagePrimaryAnchor, LeafKind: PagePrimaryLeaf,
			Bounds: fusedTabletRouteBlockLabTestBounds,
		},
		duplicate,
	); err == nil {
		t.Fatal("accepted duplicate stable slot")
	}
}

func fusedTabletRouteBlockLabTabletFromSegmentedRoot(
	t testing.TB, floor []byte, root []byte, locator PageRef,
) FusedTabletRouteBlockLabTablet {
	t.Helper()
	inner, err := globalTabletCatalogOpenSegmentedRootOnly(root)
	if err != nil {
		t.Fatal(err)
	}
	tablet := FusedTabletRouteBlockLabTablet{
		Floor: floor, TabletID: inner.tabletID, StableSlot: 17,
		Locator: locator,
		Anchors: make([]FusedTabletRouteBlockLabAnchor, inner.pageCount),
	}
	for rank := 0; rank < int(inner.pageCount); rank++ {
		pageID := inner.rootRanks[rank]
		ref, ok := inner.anchorRef(pageID)
		if !ok {
			t.Fatalf("anchor ref %d", pageID)
		}
		fence := inner.rootFence(rank)
		tablet.Anchors[rank] = FusedTabletRouteBlockLabAnchor{
			Floor: bytes.Clone(fence.a), PageID: pageID, Ref: ref,
		}
	}
	return tablet
}

func TestFusedTabletRouteBlockLabAnchorAndExactLocatorBinding(t *testing.T) {
	header, leaves, anchorRefs := segmentedTabletRouterTestInputs(t, 512)
	header.StoreID = fusedTabletRouteBlockLabTestStoreID
	header.AnchorKind = PagePrimaryAnchor
	header.LeafKind = PagePrimaryLeaf
	for rank := range leaves {
		leaves[rank].Ref.Kind = header.LeafKind
	}
	for rank := range anchorRefs {
		anchorRefs[rank].Kind = header.AnchorKind
	}
	root, rawLocator, pages, pageCount, err := EncodeSegmentedTabletRouter(
		make([]byte, SegmentedTabletRouterRootBytes),
		make([]byte, SegmentedTabletRouterLocatorBytes),
		make([]byte, 2*SegmentedTabletRouterAnchorPageBytes),
		header, anchorRefs, leaves,
	)
	if err != nil || pageCount != 2 {
		t.Fatalf("segmented encode = %d,%v", pageCount, err)
	}
	// Rebuild one checksum-valid anchor whose child was born after the anchor
	// itself. Physical-birth generations are not ancestry clocks.
	const target = 300
	selectedRootGeneration := header.Generation + 1
	bounds := fusedTabletRouteBlockLabTestBounds
	bounds.SelectedRootGeneration = selectedRootGeneration
	targetCode := binary.LittleEndian.Uint16(
		rawLocator[int(leaves[target].LocalID)*2:],
	)
	targetPageID, targetSlot := uint8(targetCode>>8), uint8(targetCode)
	targetPage := pages[int(targetPageID)*SegmentedTabletRouterAnchorPageBytes : (int(targetPageID)+1)*SegmentedTabletRouterAnchorPageBytes]
	segmentedTabletRouterPutUint48(
		targetPage[segmentedTabletRouterAnchorHandlesAt+
			int(targetSlot)*SegmentedTabletRouterHandleBytes+6:],
		selectedRootGeneration,
	)
	segmentedTabletRouterSeal(
		targetPage, segmentedTabletRouterAnchorTrailerAt,
	)
	locatorLogical, _ := GlobalTabletCatalogLocatorLogicalID(header.TabletID)
	locatorRef := fusedTabletRouteBlockLabTestRef(
		4<<20, locatorLogical, header.Generation,
		GlobalTabletCatalogLocatorBytes, PagePrimaryLocator,
	)
	tablet := fusedTabletRouteBlockLabTabletFromSegmentedRoot(
		t, nil, root, locatorRef,
	)
	logicalID, _ := FusedTabletRouteBlockLabLogicalID(6)
	image, err := EncodeFusedTabletRouteBlockLab(
		make([]byte, FusedTabletRouteBlockLabBytes),
		FusedTabletRouteBlockLabHeader{
			StoreID:                fusedTabletRouteBlockLabTestStoreID,
			Generation:             9_000,
			SelectedRootGeneration: selectedRootGeneration,
			LogicalID:              logicalID, BlockID: 6, Kind: PageTabletRoute,
			LocatorKind: PagePrimaryLocator,
			AnchorKind:  header.AnchorKind, LeafKind: header.LeafKind,
			Bounds: bounds,
		},
		[]FusedTabletRouteBlockLabTablet{tablet},
	)
	if err != nil {
		t.Fatal(err)
	}
	blockRef := fusedTabletRouteBlockLabTestRef(
		12<<20, logicalID, 9_000,
		FusedTabletRouteBlockLabBytes, PageTabletRoute,
	)
	view, err := OpenFusedTabletRouteBlockLab(
		image, blockRef, selectedRootGeneration,
		bounds,
	)
	if err != nil {
		t.Fatal(err)
	}
	key := leaves[target].Fence
	route, ok := view.RouteAnchor(key)
	if !ok || route.PageID != 1 {
		t.Fatalf("fused anchor route = %+v,%v", route, ok)
	}
	start := int(route.PageID) * SegmentedTabletRouterAnchorPageBytes
	anchor, err := OpenFusedTabletRouteBlockLabAnchor(
		pages[start:start+SegmentedTabletRouterAnchorPageBytes], route,
	)
	if err != nil {
		t.Fatal(err)
	}
	crossStoreOwner := view
	crossStoreOwner.bounds.StoreID[0] ^= 1
	crossStoreRoute := route
	crossStoreRoute.owner = &crossStoreOwner
	if _, err := OpenFusedTabletRouteBlockLabAnchor(
		pages[start:start+SegmentedTabletRouterAnchorPageBytes],
		crossStoreRoute,
	); !errors.Is(err, ErrFusedTabletRouteBlockLabCorrupt) {
		t.Fatalf("cross-Store fused-anchor owner error = %v", err)
	}
	staleOwner := view
	staleOwner.selectedRootGeneration = view.header.Generation - 1
	staleOwner.bounds.SelectedRootGeneration = staleOwner.selectedRootGeneration
	staleRoute := route
	staleRoute.owner = &staleOwner
	if _, err := OpenFusedTabletRouteBlockLabAnchor(
		pages[start:start+SegmentedTabletRouterAnchorPageBytes],
		staleRoute,
	); !errors.Is(err, ErrFusedTabletRouteBlockLabCorrupt) {
		t.Fatalf("stale-generation fused-anchor owner error = %v", err)
	}
	hash := KeyHashBytes(segmentedTabletRouterTestSeed, key)
	got, ok := anchor.RouteHashed(hash, key)
	wantLeaf := leaves[target].Ref
	wantLeaf.Generation = selectedRootGeneration
	if !ok || got.Ref != wantLeaf ||
		got.Bucket != segmentedTabletRouterTestBucket(leaves[target].Ref) {
		t.Fatalf("fused point route = %+v,%v", got, ok)
	}

	locatorEntries := make([]GlobalTabletCatalogLocatorEntry, len(leaves))
	for rank, leaf := range leaves {
		code := binary.LittleEndian.Uint16(rawLocator[int(leaf.LocalID)*2:])
		locatorEntries[rank] = GlobalTabletCatalogLocatorEntry{
			LocalID: leaf.LocalID, PageID: uint8(code >> 8),
			RowSlot: uint8(code), State: GlobalTabletCatalogLocatorLive,
		}
	}
	sort.Slice(locatorEntries, func(left, right int) bool {
		return locatorEntries[left].LocalID < locatorEntries[right].LocalID
	})
	locatorImage, err := EncodeGlobalTabletCatalogLocator(
		make([]byte, GlobalTabletCatalogLocatorBytes),
		PageHeader{
			StoreID:    fusedTabletRouteBlockLabTestStoreID,
			Generation: header.Generation, LogicalID: locatorLogical,
			PageSize: GlobalTabletCatalogLocatorBytes,
			PayloadLength: GlobalTabletCatalogLocatorHeader +
				globalTabletCatalogPackedBytes,
			Kind: locatorRef.Kind,
		},
		bounds, header.TabletID,
		header.Generation, locatorEntries,
	)
	if err != nil {
		t.Fatal(err)
	}
	locator, err := OpenGlobalTabletCatalogLocator(
		locatorImage, locatorRef, bounds,
	)
	if err != nil {
		t.Fatal(err)
	}
	resolvedRef, resolvedZone, ok := anchor.ResolveBucket(
		&locator, got.Bucket,
	)
	if !ok || resolvedRef != got.Ref || resolvedZone != got.Zone {
		t.Fatalf("posting resolve = %+v/%x/%v", resolvedRef, resolvedZone, ok)
	}
	graft := locator
	graft.ref.Offset += 4096
	if _, _, ok := anchor.ResolveBucket(&graft, got.Bucket); ok {
		t.Fatal("accepted same-tablet locator PageRef graft")
	}
}

func TestFusedTabletRouteBlockLabGeometryAndSpace(t *testing.T) {
	geometry, ok := FusedTabletRouteBlockLabCatalogGeometry(
		174_000, FusedTabletRouteBlockLabMaxFence,
	)
	if !ok || geometry.RouteBlockFanout != 1 ||
		geometry.MaximumTablets < 174_000 ||
		geometry.BranchLevels != 2 ||
		geometry.ColdPointCachePages != 4 ||
		geometry.CurrentColdPointPages != 4 ||
		geometry.ResidentBytes != FusedTabletRouteBlockLabRootBytes ||
		geometry.FirstAnchorUpdateBytes != 272<<10 ||
		geometry.CurrentFirstAnchorUpdateBytes != 96<<10 ||
		geometry.OwnedAnchorUpdateBytes != 16<<10 ||
		geometry.CurrentOwnedAnchorUpdateBytes != 24<<10 ||
		geometry.SplitBytes != 288<<10 ||
		geometry.CurrentSplitBytes != 112<<10 {
		t.Fatalf("geometry = %+v,%v", geometry, ok)
	}
	full, ok := FusedTabletRouteBlockLabRoutingSpace(
		100_000_000_000, 195, 4096, 40,
	)
	if !ok {
		t.Fatal("full space")
	}
	threeQuarter, ok := FusedTabletRouteBlockLabRoutingSpace(
		100_000_000_000, 195, 3072, 40,
	)
	if !ok {
		t.Fatal("75% space")
	}
	if full.AnchorsPerTablet != 16 ||
		threeQuarter.AnchorsPerTablet != 12 ||
		full.BytesPerDoc >= 0.185 ||
		threeQuarter.BytesPerDoc >= 0.19 ||
		full.ResidentBytes != 128<<10 ||
		threeQuarter.ColdScanPages <= full.ColdScanPages {
		t.Fatalf("space full=%+v 75%%=%+v", full, threeQuarter)
	}
	t.Logf(
		"full %.6f B/doc %.3f GiB; 75%% %.6f B/doc %.3f GiB; geometry %+v",
		full.BytesPerDoc, float64(full.TotalBytes)/(1<<30),
		threeQuarter.BytesPerDoc,
		float64(threeQuarter.TotalBytes)/(1<<30), geometry,
	)
}

func TestFusedTabletRouteBlockLabWorstCaseAdmitsOne(t *testing.T) {
	const tablets = 1
	input := make([]FusedTabletRouteBlockLabTablet, tablets)
	for at := range input {
		tabletID := uint32((at*7 + 3) & (TabletLocalIdentityTabletCount - 1))
		floor := make([]byte, 0)
		if at != 0 {
			floor = bytes.Repeat([]byte{byte('a' + at)}, 256)
			floor[0] = byte('a' + at)
		}
		locatorLogical, _ := GlobalTabletCatalogLocatorLogicalID(tabletID)
		input[at] = FusedTabletRouteBlockLabTablet{
			Floor: floor, TabletID: tabletID,
			StableSlot: uint8((at*11 + 5) & 0xff),
			Locator: fusedTabletRouteBlockLabTestRef(
				uint64(100+at)*8192, locatorLogical, 50,
				GlobalTabletCatalogLocatorBytes,
				PagePrimaryLocator,
			),
			Anchors: make([]FusedTabletRouteBlockLabAnchor, 16),
		}
		for rank := 0; rank < 16; rank++ {
			var anchorFloor []byte
			if rank != 0 {
				anchorFloor = bytes.Repeat(
					[]byte{byte(rank), byte(at), 0x5a, 0xa5}, 64,
				)
			}
			pageID := uint8((rank*5 + 3) & 15)
			logicalID, _ := GlobalTabletCatalogAnchorLogicalID(
				tabletID, pageID,
			)
			input[at].Anchors[rank] = FusedTabletRouteBlockLabAnchor{
				Floor: anchorFloor, PageID: pageID,
				Ref: fusedTabletRouteBlockLabTestRef(
					uint64(1000+at*16+rank)*8192,
					logicalID, 50,
					SegmentedTabletRouterAnchorPageBytes,
					PagePrimaryAnchor,
				),
			}
		}
	}
	logicalID, _ := FusedTabletRouteBlockLabLogicalID(7)
	bounds := fusedTabletRouteBlockLabTestBounds
	bounds.SelectedRootGeneration = 50
	image, err := EncodeFusedTabletRouteBlockLab(
		make([]byte, FusedTabletRouteBlockLabBytes),
		FusedTabletRouteBlockLabHeader{
			StoreID:    fusedTabletRouteBlockLabTestStoreID,
			Generation: 50, SelectedRootGeneration: 50,
			LogicalID: logicalID, BlockID: 7, Kind: PageTabletRoute,
			LocatorKind: PagePrimaryLocator,
			AnchorKind:  PagePrimaryAnchor, LeafKind: PagePrimaryLeaf,
			Bounds: bounds,
		},
		input,
	)
	if err != nil {
		t.Fatal(err)
	}
	ref := fusedTabletRouteBlockLabTestRef(
		20<<20, logicalID, 50,
		FusedTabletRouteBlockLabBytes, PageTabletRoute,
	)
	if _, err := OpenFusedTabletRouteBlockLab(
		image, ref, 50, bounds,
	); err != nil {
		t.Fatal(err)
	}
}

func FuzzFusedTabletRouteBlockLabAdmission(f *testing.F) {
	_, image, ref, _ := fusedTabletRouteBlockLabTestView(f)
	f.Add(bytes.Clone(image), uint64(100))
	f.Add([]byte("short"), uint64(100))
	f.Fuzz(func(t *testing.T, src []byte, selected uint64) {
		if len(src) > FusedTabletRouteBlockLabBytes*2 {
			t.Skip()
		}
		_, _ = OpenFusedTabletRouteBlockLab(
			src, ref, selected, fusedTabletRouteBlockLabTestBounds,
		)
	})
}
