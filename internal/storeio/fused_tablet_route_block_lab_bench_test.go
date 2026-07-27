package storeio

import (
	"bytes"
	"fmt"
	"testing"
)

var (
	fusedTabletRouteBlockLabBenchmarkRoute  FusedTabletRouteBlockLabRoute
	fusedTabletRouteBlockLabBenchmarkTablet FusedTabletRouteBlockLabTabletView
	fusedTabletRouteBlockLabBenchmarkLeaf   SegmentedTabletRouterRoute
	fusedTabletRouteBlockLabBenchmarkImage  []byte
	fusedTabletRouteBlockLabBenchmarkCount  int
)

func fusedTabletRouteBlockLabBenchmarkFixture(
	b testing.TB,
) (
	FusedTabletRouteBlockLabView,
	FusedTabletRouteBlockLabAnchorView,
	GlobalTabletCatalogNodeView,
	GlobalTabletCatalogTabletRootView,
	GlobalTabletCatalogAnchorView,
	[]byte,
	[]SegmentedTabletRouterLeaf,
) {
	b.Helper()
	header, leaves, anchorRefs := segmentedTabletRouterTestInputs(b, 3072)
	header.AnchorKind = PagePrimaryAnchor
	header.LeafKind = PagePrimaryLeaf
	for rank := range leaves {
		leaves[rank].Ref.Kind = header.LeafKind
	}
	for rank := range anchorRefs {
		anchorRefs[rank].Kind = header.AnchorKind
	}
	bounds := fusedTabletRouteBlockLabTestBounds
	bounds.SelectedRootGeneration = header.Generation
	root, _, pages, pageCount, err := EncodeSegmentedTabletRouter(
		make([]byte, SegmentedTabletRouterRootBytes),
		make([]byte, SegmentedTabletRouterLocatorBytes),
		make([]byte, 12*SegmentedTabletRouterAnchorPageBytes),
		header, anchorRefs, leaves,
	)
	if err != nil || pageCount != 12 {
		b.Fatalf("segmented fixture = %d,%v", pageCount, err)
	}
	locatorLogical, _ := GlobalTabletCatalogLocatorLogicalID(header.TabletID)
	locatorRef := fusedTabletRouteBlockLabTestRef(
		4<<20, locatorLogical, header.Generation,
		GlobalTabletCatalogLocatorBytes, PagePrimaryLocator,
	)
	actual := fusedTabletRouteBlockLabTabletFromSegmentedRoot(
		b, []byte("tenant/0042"), root, locatorRef,
	)

	const (
		tabletCount     = 16
		selectedOrdinal = 8
	)
	actual.StableSlot = uint8((selectedOrdinal*17 + 3) & 0xff)
	tablets := make([]FusedTabletRouteBlockLabTablet, tabletCount)
	catalogEntries := make([]GlobalTabletCatalogNodeEntry, tabletCount)
	for ordinal := 0; ordinal < tabletCount; ordinal++ {
		tabletID := uint32(ordinal*37 + 7)
		var floor []byte
		switch {
		case ordinal == 0:
		case ordinal < selectedOrdinal:
			floor = []byte(fmt.Sprintf("tenant/%04d", ordinal))
		case ordinal == selectedOrdinal:
			tabletID = header.TabletID
			floor = []byte("tenant/0042")
		default:
			floor = []byte(fmt.Sprintf(
				"tenant/%04d", ordinal+42-selectedOrdinal,
			))
		}
		if ordinal == selectedOrdinal {
			tablets[ordinal] = actual
		} else {
			locatorLogical, _ := GlobalTabletCatalogLocatorLogicalID(tabletID)
			tablet := FusedTabletRouteBlockLabTablet{
				Floor: floor, TabletID: tabletID,
				StableSlot: uint8((ordinal*17 + 3) & 0xff),
				Locator: fusedTabletRouteBlockLabTestRef(
					uint64(2000+ordinal)*8192, locatorLogical, 9000,
					GlobalTabletCatalogLocatorBytes,
					PagePrimaryLocator,
				),
				Anchors: make([]FusedTabletRouteBlockLabAnchor, 12),
			}
			for rank := range tablet.Anchors {
				var anchorFloor []byte
				if rank != 0 {
					anchorFloor = []byte(fmt.Sprintf(
						"%s/document/%010d", floor, rank*256*230,
					))
				}
				pageID := uint8((rank*5 + 3) & 15)
				logicalID, _ := GlobalTabletCatalogAnchorLogicalID(
					tabletID, pageID,
				)
				tablet.Anchors[rank] = FusedTabletRouteBlockLabAnchor{
					Floor: anchorFloor, PageID: pageID,
					Ref: fusedTabletRouteBlockLabTestRef(
						uint64(4000+ordinal*16+rank)*8192,
						logicalID, 9000,
						SegmentedTabletRouterAnchorPageBytes,
						header.AnchorKind,
					),
				}
			}
			tablets[ordinal] = tablet
		}
		tabletLogical, _ := GlobalTabletCatalogTabletRootLogicalID(tabletID)
		catalogEntries[ordinal] = GlobalTabletCatalogNodeEntry{
			Floor: floor, ID: tabletID,
			Ref: fusedTabletRouteBlockLabTestRef(
				uint64(8000+ordinal)*GlobalTabletCatalogTabletBytes,
				tabletLogical, 9000,
				GlobalTabletCatalogTabletBytes, PageChunkDirectory,
			),
		}
	}

	blockLogical, _ := FusedTabletRouteBlockLabLogicalID(99)
	blockImage, err := EncodeFusedTabletRouteBlockLab(
		make([]byte, FusedTabletRouteBlockLabBytes),
		FusedTabletRouteBlockLabHeader{
			StoreID:    fusedTabletRouteBlockLabTestStoreID,
			Generation: 9000, SelectedRootGeneration: header.Generation,
			LogicalID: blockLogical, BlockID: 99, Kind: PageTabletRoute,
			LocatorKind: PagePrimaryLocator,
			AnchorKind:  header.AnchorKind, LeafKind: header.LeafKind,
			Bounds: bounds,
		},
		tablets,
	)
	if err != nil {
		b.Fatal(err)
	}
	blockRef := fusedTabletRouteBlockLabTestRef(
		96<<20, blockLogical, 9000,
		FusedTabletRouteBlockLabBytes, PageTabletRoute,
	)
	block, err := OpenFusedTabletRouteBlockLab(
		blockImage, blockRef, header.Generation, bounds,
	)
	if err != nil {
		b.Fatal(err)
	}
	key := leaves[1536].Fence
	fusedRoute, ok := block.RouteAnchor(key)
	if !ok {
		b.Fatal("fused route")
	}
	start := int(fusedRoute.PageID) * SegmentedTabletRouterAnchorPageBytes
	fusedAnchor, err := OpenFusedTabletRouteBlockLabAnchor(
		pages[start:start+SegmentedTabletRouterAnchorPageBytes], fusedRoute,
	)
	if err != nil {
		b.Fatal(err)
	}

	tabletLogical, _ := GlobalTabletCatalogTabletRootLogicalID(header.TabletID)
	tabletRef := catalogEntries[selectedOrdinal].Ref
	tabletImage, err := EncodeGlobalTabletCatalogTabletRoot(
		make([]byte, GlobalTabletCatalogTabletBytes),
		PageHeader{
			StoreID:    fusedTabletRouteBlockLabTestStoreID,
			Generation: header.Generation, LogicalID: tabletLogical,
			PageSize: GlobalTabletCatalogTabletBytes,
			PayloadLength: GlobalTabletCatalogRootHeader +
				SegmentedTabletRouterRootBytes,
			Kind: tabletRef.Kind,
		},
		bounds, locatorRef, root,
	)
	if err != nil {
		b.Fatal(err)
	}
	// The selected catalog handle must bind the wrapper's physical birth.
	catalogEntries[selectedOrdinal].Ref.Generation = header.Generation
	tabletRef.Generation = header.Generation
	tablet, err := OpenGlobalTabletCatalogTabletRoot(
		tabletImage, tabletRef, bounds,
	)
	if err != nil {
		b.Fatal(err)
	}
	selected, ok := tablet.RouteAnchor(key)
	if !ok {
		b.Fatal("current tablet route")
	}
	start = int(selected.PageID) * SegmentedTabletRouterAnchorPageBytes
	currentAnchor, err := OpenGlobalTabletCatalogAnchor(
		pages[start:start+SegmentedTabletRouterAnchorPageBytes],
		&tablet, selected.PageID,
	)
	if err != nil {
		b.Fatal(err)
	}

	catalogLogical, _ := GlobalTabletCatalogCatalogLeafLogicalID(88)
	catalogImage, err := EncodeGlobalTabletCatalogNode(
		make([]byte, GlobalTabletCatalogNodeBytes),
		GlobalTabletCatalogNodeHeader{
			StoreID:    fusedTabletRouteBlockLabTestStoreID,
			Generation: header.Generation, LogicalID: catalogLogical,
			PageID: 88, Level: GlobalTabletCatalogLeaf,
			Kind: PageIndexDirectory, ChildKind: PageChunkDirectory,
			ChildLength: GlobalTabletCatalogTabletBytes,
			Bounds:      bounds,
		},
		catalogEntries,
	)
	if err != nil {
		b.Fatal(err)
	}
	catalogRef := fusedTabletRouteBlockLabTestRef(
		112<<20, catalogLogical, header.Generation,
		GlobalTabletCatalogNodeBytes, PageIndexDirectory,
	)
	catalog, err := OpenGlobalTabletCatalogNode(
		catalogImage, catalogRef, bounds,
	)
	if err != nil {
		b.Fatal(err)
	}
	return block, fusedAnchor, catalog, tablet, currentAnchor, key, leaves
}

func BenchmarkFusedTabletRouteBlockLabPoint(b *testing.B) {
	block, fusedAnchor, catalog, tablet, currentAnchor, key, _ :=
		fusedTabletRouteBlockLabBenchmarkFixture(b)
	hash := KeyHashBytes(segmentedTabletRouterTestSeed, key)
	selectedTablet, _ := block.RouteTablet(key)

	b.Run("fused-tablet-floor-only", func(b *testing.B) {
		b.ReportAllocs()
		var tabletView FusedTabletRouteBlockLabTabletView
		for b.Loop() {
			tabletView, _ = block.RouteTablet(key)
		}
		fusedTabletRouteBlockLabBenchmarkTablet = tabletView
	})
	b.Run("fused-anchor-root-only", func(b *testing.B) {
		b.ReportAllocs()
		var route FusedTabletRouteBlockLabRoute
		for b.Loop() {
			route, _ = selectedTablet.RouteAnchor(key)
		}
		fusedTabletRouteBlockLabBenchmarkRoute = route
	})
	b.Run("fused-route-block-only", func(b *testing.B) {
		b.ReportAllocs()
		var route FusedTabletRouteBlockLabRoute
		for b.Loop() {
			route, _ = block.RouteAnchor(key)
		}
		fusedTabletRouteBlockLabBenchmarkRoute = route
	})
	b.Run("current-catalog-leaf-only", func(b *testing.B) {
		b.ReportAllocs()
		var route GlobalTabletCatalogNodeRoute
		for b.Loop() {
			route = catalog.Route(key)
		}
		globalTabletCatalogBenchmarkNodeRoute = route
	})
	b.Run("anchor-only", func(b *testing.B) {
		b.ReportAllocs()
		var leaf SegmentedTabletRouterRoute
		for b.Loop() {
			leaf, _ = fusedAnchor.RouteHashed(hash, key)
		}
		fusedTabletRouteBlockLabBenchmarkLeaf = leaf
	})
	b.Run("current-tablet-root+anchor", func(b *testing.B) {
		b.ReportAllocs()
		var selected GlobalTabletCatalogAnchorRoute
		var leaf SegmentedTabletRouterRoute
		for b.Loop() {
			selected, _ = tablet.RouteAnchor(key)
			leaf, _ = currentAnchor.RouteHashed(hash, key)
		}
		globalTabletCatalogBenchmarkRef = selected.Ref
		fusedTabletRouteBlockLabBenchmarkLeaf = leaf
	})
	b.Run("fused-block+anchor", func(b *testing.B) {
		b.ReportAllocs()
		var route FusedTabletRouteBlockLabRoute
		var leaf SegmentedTabletRouterRoute
		for b.Loop() {
			route, _ = block.RouteAnchor(key)
			leaf, _ = fusedAnchor.RouteHashed(hash, key)
		}
		fusedTabletRouteBlockLabBenchmarkRoute = route
		fusedTabletRouteBlockLabBenchmarkLeaf = leaf
	})
	b.Run("current-catalog-leaf+tablet-root+anchor", func(b *testing.B) {
		b.ReportAllocs()
		var catalogRoute GlobalTabletCatalogNodeRoute
		var selected GlobalTabletCatalogAnchorRoute
		var leaf SegmentedTabletRouterRoute
		for b.Loop() {
			catalogRoute = catalog.Route(key)
			selected, _ = tablet.RouteAnchor(key)
			leaf, _ = currentAnchor.RouteHashed(hash, key)
		}
		globalTabletCatalogBenchmarkNodeRoute = catalogRoute
		globalTabletCatalogBenchmarkRef = selected.Ref
		fusedTabletRouteBlockLabBenchmarkLeaf = leaf
	})
}

func BenchmarkFusedTabletRouteBlockLabPostingAndScan(b *testing.B) {
	block, _, catalog, tablet, _, _, leaves :=
		fusedTabletRouteBlockLabBenchmarkFixture(b)
	bucket := segmentedTabletRouterTestBucket(leaves[1536].Ref)
	directTablet, _ := block.ResolveTablet(bucket)
	stableSlot := directTablet.StableSlot()
	b.Run("posting-tablet+locator", func(b *testing.B) {
		b.ReportAllocs()
		var tablet FusedTabletRouteBlockLabTabletView
		for b.Loop() {
			tablet, _ = block.ResolveStableSlot(stableSlot)
			_, _ = tablet.LocatorRef()
		}
		fusedTabletRouteBlockLabBenchmarkTablet = tablet
	})
	b.Run("ordered-route-metadata", func(b *testing.B) {
		b.ReportAllocs()
		var count int
		for b.Loop() {
			count = 0
			cursor := block.OrderedLowerBound(nil)
			for {
				if _, _, _, ok := cursor.AnchorRef(); !ok {
					break
				}
				count++
				if !cursor.Next() {
					break
				}
			}
		}
		b.ReportMetric(
			float64(b.Elapsed().Nanoseconds())/float64(b.N*count),
			"ns/anchor",
		)
		fusedTabletRouteBlockLabBenchmarkCount = count
	})
	b.Run("current-ordered-route-metadata", func(b *testing.B) {
		b.ReportAllocs()
		var count int
		var ref PageRef
		for b.Loop() {
			count = 0
			tablets := catalog.LowerBound(nil)
			for {
				if _, ok := tablets.Route(); !ok {
					break
				}
				for rank := 0; rank < int(tablet.inner.pageCount); rank++ {
					pageID := tablet.inner.rootRanks[rank]
					ref, _ = tablet.inner.anchorRef(pageID)
					count++
				}
				if !tablets.Next() {
					break
				}
			}
		}
		b.ReportMetric(
			float64(b.Elapsed().Nanoseconds())/float64(b.N*count),
			"ns/anchor",
		)
		globalTabletCatalogBenchmarkRef = ref
		fusedTabletRouteBlockLabBenchmarkCount = count
	})
}

func BenchmarkFusedTabletRouteBlockLabCOW(b *testing.B) {
	block, _, _, _, _, key, _ :=
		fusedTabletRouteBlockLabBenchmarkFixture(b)
	route, _ := block.RouteAnchor(key)
	replacement := route.AnchorRef
	replacement.Offset += 1 << 30
	replacement.Generation = 10_000
	dst := make([]byte, FusedTabletRouteBlockLabBytes)
	bounds := fusedTabletRouteBlockLabTestBounds
	bounds.SelectedRootGeneration = 10_000
	b.SetBytes(FusedTabletRouteBlockLabBytes)
	b.ReportAllocs()
	var image []byte
	for b.Loop() {
		var err error
		image, err = block.RewriteReferences(
			dst, 9500, 10_000, bounds,
			FusedTabletRouteBlockLabRefMutation{
				TabletID: route.TabletID, ReplaceAnchor: true,
				AnchorPageID: route.PageID, Anchor: replacement,
			},
		)
		if err != nil {
			b.Fatal(err)
		}
	}
	fusedTabletRouteBlockLabBenchmarkImage = image
}

func BenchmarkFusedTabletRouteBlockLabCOWBatch(b *testing.B) {
	block, _, _, tablets := fusedTabletRouteBlockLabTestView(b)
	locator := tablets[0].Locator
	locator.Offset += 3 << 20
	locator.Generation = 99
	lastAnchor := tablets[2].Anchors[2].Ref
	lastAnchor.Offset += 4 << 20
	lastAnchor.Generation = 99
	middleAnchor := tablets[1].Anchors[1].Ref
	middleAnchor.Offset += 5 << 20
	middleAnchor.Generation = 99
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
	dst := make([]byte, FusedTabletRouteBlockLabBytes)
	b.SetBytes(FusedTabletRouteBlockLabBytes)
	b.ReportAllocs()
	var image []byte
	for b.Loop() {
		var err error
		image, err = block.RewriteReferenceBatch(
			dst, 20, 100, fusedTabletRouteBlockLabTestBounds, mutations,
		)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(
		float64(b.Elapsed().Nanoseconds())/float64(b.N*len(mutations)),
		"ns/edit",
	)
	fusedTabletRouteBlockLabBenchmarkImage = image
}

func BenchmarkFusedTabletRouteBlockLabSpace(b *testing.B) {
	for _, occupied := range []uint64{4096, 3072} {
		b.Run(fmt.Sprintf("%d-leaves", occupied), func(b *testing.B) {
			space, ok := FusedTabletRouteBlockLabRoutingSpace(
				100_000_000_000, 195, occupied, 40,
			)
			if !ok {
				b.Fatal("space")
			}
			currentCatalog, ok := GlobalTabletCatalogCatalogGeometry(
				space.Tablets, FusedTabletRouteBlockLabMaxFence,
			)
			if !ok {
				b.Fatal("current catalog geometry")
			}
			currentBytes := currentCatalog.DiskBytes +
				space.Tablets*(GlobalTabletCatalogTabletBytes+
					GlobalTabletCatalogLocatorBytes+
					space.AnchorsPerTablet*
						SegmentedTabletRouterAnchorPageBytes)
			currentBytesPerDoc := float64(currentBytes) /
				float64(space.Documents)
			b.ReportMetric(space.BytesPerDoc, "B/doc")
			b.ReportMetric(currentBytesPerDoc, "current-B/doc")
			b.ReportMetric(
				(currentBytesPerDoc-space.BytesPerDoc)/
					currentBytesPerDoc*100,
				"space-saving-%",
			)
			b.ReportMetric(float64(space.TotalBytes)/(1<<30), "GiB")
			b.ReportMetric(float64(currentBytes)/(1<<30), "current-GiB")
			b.ReportMetric(float64(space.CatalogBytes)/(1<<20), "catalog-MiB")
			b.ReportMetric(float64(space.Tablets), "tablets")
			b.ReportMetric(float64(space.ColdScanPages), "cold-scan-pages")
		})
	}
}

func BenchmarkFusedTabletRouteBlockLabDescriptorDensity(b *testing.B) {
	block, _, _, _, _, _, _ := fusedTabletRouteBlockLabBenchmarkFixture(b)
	for b.Loop() {
		_ = bytes.Equal(block.image, block.image)
	}
	b.ReportMetric(
		float64(block.header.PayloadLength)/float64(block.Count()),
		"B/tablet-route-block",
	)
	b.ReportMetric(
		float64(FusedTabletRouteBlockLabWorstCaseFanout(256)),
		"worst-tablets/block",
	)
	b.ReportMetric(
		float64(block.header.PayloadLength)/
			float64(FusedTabletRouteBlockLabBytes-PageHeaderSize-PageTrailerSize)*
			100,
		"payload-fill-%",
	)
}
