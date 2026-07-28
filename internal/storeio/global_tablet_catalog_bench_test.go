package storeio

import (
	"encoding/binary"
	"fmt"
	"sort"
	"testing"
)

var (
	globalTabletCatalogBenchmarkNodeRoute GlobalTabletCatalogNodeRoute
	globalTabletCatalogBenchmarkRef       PageRef
	globalTabletCatalogBenchmarkState     GlobalTabletCatalogLocatorState
	globalTabletCatalogBenchmarkPageID    uint8
	globalTabletCatalogBenchmarkRowSlot   uint8
	globalTabletCatalogBenchmarkImage     []byte
	globalTabletCatalogBenchmarkCount     int
)

func globalTabletCatalogBenchmarkNode(
	b testing.TB, level, rootChild GlobalTabletCatalogNodeLevel, count int,
) GlobalTabletCatalogNodeView {
	return globalTabletCatalogBenchmarkNodeShape(
		b, level, rootChild, count, false,
	)
}

func globalTabletCatalogBenchmarkNodeShape(
	b testing.TB, level, rootChild GlobalTabletCatalogNodeLevel, count int,
	sharedHead bool,
) GlobalTabletCatalogNodeView {
	b.Helper()
	entries := make([]GlobalTabletCatalogNodeEntry, count)
	for at := range entries {
		var floor []byte
		if at != 0 {
			if sharedHead {
				floor = []byte(fmt.Sprintf("tenant/%08x", at))
			} else {
				floor = make([]byte, 4)
				binary.BigEndian.PutUint32(floor, uint32(at))
			}
		}
		id := uint32(at)
		childLogical, ok := globalTabletCatalogChildLogicalID(
			level,
			func() GlobalTabletCatalogNodeLevel {
				if level == GlobalTabletCatalogRoot {
					return rootChild
				}
				return GlobalTabletCatalogLeaf
			}(),
			id,
		)
		if !ok {
			b.Fatalf("child logical ID %d", at)
		}
		childKind := PagePrimaryCatalog
		childLength := uint32(GlobalTabletCatalogNodeBytes)
		if level == GlobalTabletCatalogLeaf {
			childKind = PageTabletRoute
			childLength = GlobalTabletCatalogTabletBytes
		}
		entries[at] = GlobalTabletCatalogNodeEntry{
			Floor: floor, ID: id,
			Ref: globalTabletCatalogTestRef(
				uint64(at+1)*8192, childLogical, 100,
				childLength, childKind,
			),
		}
	}
	pageBytes, _ := globalTabletCatalogNodePageBytes(level)
	var logicalID uint64
	var pageID uint32
	switch level {
	case GlobalTabletCatalogLeaf:
		logicalID, _ = GlobalTabletCatalogCatalogLeafLogicalID(0)
	case GlobalTabletCatalogBranch:
		logicalID, _ = GlobalTabletCatalogCatalogBranchLogicalID(0)
	case GlobalTabletCatalogRoot:
		logicalID = GlobalTabletCatalogRootLogicalID
	}
	childKind := PagePrimaryCatalog
	childLength := uint32(GlobalTabletCatalogNodeBytes)
	if level == GlobalTabletCatalogLeaf {
		childKind = PageTabletRoute
		childLength = GlobalTabletCatalogTabletBytes
	}
	image, err := EncodeGlobalTabletCatalogNode(
		make([]byte, pageBytes),
		GlobalTabletCatalogNodeHeader{
			StoreID:    globalTabletCatalogTestStoreID,
			Bounds:     globalTabletCatalogTestBounds,
			Generation: 100, LogicalID: logicalID, PageID: pageID,
			Level: level, RootChildLevel: rootChild,
			Kind: PagePrimaryCatalog, ChildKind: childKind,
			ChildLength: childLength,
		},
		entries,
	)
	if err != nil {
		b.Fatal(err)
	}
	ref := globalTabletCatalogTestRef(
		1<<30, logicalID, 100, uint32(pageBytes), PagePrimaryCatalog,
	)
	view, err := OpenGlobalTabletCatalogNode(
		image, ref, globalTabletCatalogTestBounds,
	)
	if err != nil {
		b.Fatal(err)
	}
	return view
}

func BenchmarkGlobalTabletCatalogResidentRoute(b *testing.B) {
	leaf := globalTabletCatalogBenchmarkNode(
		b, GlobalTabletCatalogLeaf, 0, 256,
	)
	root := globalTabletCatalogBenchmarkNode(
		b, GlobalTabletCatalogRoot, GlobalTabletCatalogLeaf, 256,
	)
	rootRare := globalTabletCatalogBenchmarkNode(
		b, GlobalTabletCatalogRoot, GlobalTabletCatalogBranch, 256,
	)
	branch := globalTabletCatalogBenchmarkNode(
		b, GlobalTabletCatalogBranch, 0, 256,
	)
	sharedHead := globalTabletCatalogBenchmarkNodeShape(
		b, GlobalTabletCatalogLeaf, 0, 256, true,
	)
	key := make([]byte, 4)
	binary.BigEndian.PutUint32(key, 128)
	b.Run("catalog-leaf", func(b *testing.B) {
		b.ReportAllocs()
		var route GlobalTabletCatalogNodeRoute
		for b.Loop() {
			route = leaf.Route(key)
		}
		globalTabletCatalogBenchmarkNodeRoute = route
	})
	b.Run("normal-two-level", func(b *testing.B) {
		b.ReportAllocs()
		var rootRoute, leafRoute GlobalTabletCatalogNodeRoute
		for b.Loop() {
			rootRoute = root.Route(key)
			leafRoute = leaf.Route(key)
		}
		leafRoute.ID ^= rootRoute.ID
		leafRoute.Ordinal ^= rootRoute.Ordinal
		globalTabletCatalogBenchmarkNodeRoute = leafRoute
	})
	b.Run("shared-head-leaf", func(b *testing.B) {
		key := []byte("tenant/00000080")
		b.ReportAllocs()
		var route GlobalTabletCatalogNodeRoute
		for b.Loop() {
			route = sharedHead.Route(key)
		}
		globalTabletCatalogBenchmarkNodeRoute = route
	})
	b.Run("rare-three-level", func(b *testing.B) {
		b.ReportAllocs()
		var rootRoute, branchRoute, leafRoute GlobalTabletCatalogNodeRoute
		for b.Loop() {
			rootRoute = rootRare.Route(key)
			branchRoute = branch.Route(key)
			leafRoute = leaf.Route(key)
		}
		leafRoute.ID ^= rootRoute.ID ^ branchRoute.ID
		leafRoute.Ordinal ^= rootRoute.Ordinal ^ branchRoute.Ordinal
		globalTabletCatalogBenchmarkNodeRoute = leafRoute
	})
	b.Run("ordered-256", func(b *testing.B) {
		b.ReportAllocs()
		var count int
		for b.Loop() {
			cursor := leaf.LowerBound(nil)
			count = 0
			for {
				if _, ok := cursor.Route(); !ok {
					break
				}
				count++
				if !cursor.Next() {
					break
				}
			}
		}
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/
			float64(b.N*leaf.Count()), "ns/tablet")
		globalTabletCatalogBenchmarkCount = count
	})
}

func BenchmarkGlobalTabletCatalogCompactLocator(b *testing.B) {
	const tabletID = uint32(42)
	logicalID, _ := GlobalTabletCatalogLocatorLogicalID(tabletID)
	entries := make([]GlobalTabletCatalogLocatorEntry, TabletLocalIdentityLocalCount)
	for localID := range entries {
		entries[localID] = GlobalTabletCatalogLocatorEntry{
			LocalID: uint16(localID),
			PageID:  uint8(localID / SegmentedTabletRouterRowsPerPage),
			RowSlot: uint8(localID),
			State:   GlobalTabletCatalogLocatorLive,
		}
	}
	image, err := EncodeGlobalTabletCatalogLocator(
		make([]byte, GlobalTabletCatalogLocatorBytes),
		PageHeader{
			StoreID:    globalTabletCatalogTestStoreID,
			Generation: 100, LogicalID: logicalID,
			PageSize: GlobalTabletCatalogLocatorBytes,
			PayloadLength: GlobalTabletCatalogLocatorHeader +
				globalTabletCatalogPackedBytes,
			Kind: PagePrimaryLocator,
		},
		globalTabletCatalogTestBounds,
		tabletID, 100, entries,
	)
	if err != nil {
		b.Fatal(err)
	}
	ref := globalTabletCatalogTestRef(
		1<<20, logicalID, 100,
		GlobalTabletCatalogLocatorBytes, PagePrimaryLocator,
	)
	view, err := OpenGlobalTabletCatalogLocator(
		image, ref, globalTabletCatalogTestBounds,
	)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	var pageID, rowSlot uint8
	var state GlobalTabletCatalogLocatorState
	for b.Loop() {
		pageID, rowSlot, state = view.Resolve(3073)
	}
	globalTabletCatalogBenchmarkPageID = pageID
	globalTabletCatalogBenchmarkRowSlot = rowSlot
	globalTabletCatalogBenchmarkState = state
}

func BenchmarkGlobalTabletCatalogCatalogCOW(b *testing.B) {
	for _, test := range []struct {
		name  string
		level GlobalTabletCatalogNodeLevel
		child GlobalTabletCatalogNodeLevel
		count int
	}{
		{"leaf-8K", GlobalTabletCatalogLeaf, 0, 256},
		{"branch-8K", GlobalTabletCatalogBranch, 0, 256},
		{"root-64K", GlobalTabletCatalogRoot, GlobalTabletCatalogLeaf, 256},
	} {
		b.Run(test.name, func(b *testing.B) {
			view := globalTabletCatalogBenchmarkNode(
				b, test.level, test.child, test.count,
			)
			id := uint32(test.count / 2)
			key := make([]byte, 4)
			binary.BigEndian.PutUint32(key, id)
			route := view.Route(key)
			replacement := route.Ref
			replacement.Offset += 1 << 30
			replacement.Generation = 101
			dst := make([]byte, len(view.image))
			b.SetBytes(int64(len(dst)))
			b.ReportAllocs()
			var image []byte
			for b.Loop() {
				var err error
				image, err = view.RewriteHandle(
					dst, 101, globalTabletCatalogTestBounds,
					id, replacement,
				)
				if err != nil {
					b.Fatal(err)
				}
			}
			globalTabletCatalogBenchmarkImage = image
		})
	}
}

func BenchmarkGlobalTabletCatalogRoutingSpace(b *testing.B) {
	for _, rows := range []uint64{187, 195, 230} {
		b.Run(fmt.Sprintf("%d-rows", rows), func(b *testing.B) {
			space, ok := GlobalTabletCatalogRoutingSpace(
				100_000_000_000, rows, 4096, 6<<20,
			)
			if !ok {
				b.Fatal("space")
			}
			b.ReportMetric(space.BytesPerDoc, "B/doc")
			b.ReportMetric(float64(space.Tablets), "tablets")
			b.ReportMetric(float64(space.TotalBytes)/(1<<30), "GiB")
		})
	}
}

func BenchmarkGlobalTabletCatalogRootAndSelectedAnchor(b *testing.B) {
	header, leaves, anchorRefs := segmentedTabletRouterTestInputs(b, 3072)
	header.StoreID = globalTabletCatalogTestStoreID
	bounds := globalTabletCatalogTestBounds
	bounds.SelectedRootGeneration = header.Generation
	root, rawLocator, anchors, _, err := EncodeSegmentedTabletRouter(
		make([]byte, SegmentedTabletRouterRootBytes),
		make([]byte, SegmentedTabletRouterLocatorBytes),
		make([]byte, 12*SegmentedTabletRouterAnchorPageBytes),
		header, anchorRefs, leaves,
	)
	if err != nil {
		b.Fatal(err)
	}
	locatorLogical, _ := GlobalTabletCatalogLocatorLogicalID(header.TabletID)
	locatorRef := globalTabletCatalogTestRef(
		1<<20, locatorLogical, header.Generation,
		GlobalTabletCatalogLocatorBytes, PagePrimaryLocator,
	)
	tabletLogical, _ := GlobalTabletCatalogTabletRootLogicalID(header.TabletID)
	tabletRef := globalTabletCatalogTestRef(
		2<<20, tabletLogical, header.Generation,
		GlobalTabletCatalogTabletBytes, PageTabletRoute,
	)
	tabletImage, err := EncodeGlobalTabletCatalogTabletRoot(
		make([]byte, GlobalTabletCatalogTabletBytes),
		PageHeader{
			StoreID:    globalTabletCatalogTestStoreID,
			Generation: header.Generation, LogicalID: tabletLogical,
			PageSize: GlobalTabletCatalogTabletBytes,
			PayloadLength: GlobalTabletCatalogRootHeader +
				SegmentedTabletRouterRootBytes,
			Kind: tabletRef.Kind,
		},
		bounds,
		locatorRef, root,
	)
	if err != nil {
		b.Fatal(err)
	}
	tablet, err := OpenGlobalTabletCatalogTabletRoot(
		tabletImage, tabletRef, bounds,
	)
	if err != nil {
		b.Fatal(err)
	}
	key := leaves[1536].Fence
	selected, ok := tablet.RouteAnchor(key)
	if !ok {
		b.Fatal("select anchor")
	}
	start := int(selected.PageID) * SegmentedTabletRouterAnchorPageBytes
	anchor, err := OpenGlobalTabletCatalogAnchor(
		anchors[start:start+SegmentedTabletRouterAnchorPageBytes],
		&tablet, selected.PageID,
	)
	if err != nil {
		b.Fatal(err)
	}
	hash := KeyHashBytes(segmentedTabletRouterTestSeed, key)
	b.Run("point", func(b *testing.B) {
		b.ReportAllocs()
		var route SegmentedTabletRouterRoute
		for b.Loop() {
			selected, _ = tablet.RouteAnchor(key)
			route, _ = anchor.RouteHashed(hash, key)
		}
		globalTabletCatalogBenchmarkRef = selected.Ref
		segmentedTabletRouterBenchmarkRoute = route
	})

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
			StoreID:    globalTabletCatalogTestStoreID,
			Generation: header.Generation, LogicalID: locatorLogical,
			PageSize: GlobalTabletCatalogLocatorBytes,
			PayloadLength: GlobalTabletCatalogLocatorHeader +
				globalTabletCatalogPackedBytes,
			Kind: locatorRef.Kind,
		},
		bounds,
		header.TabletID, header.Generation, locatorEntries,
	)
	if err != nil {
		b.Fatal(err)
	}
	locator, err := OpenGlobalTabletCatalogLocator(
		locatorImage, locatorRef, bounds,
	)
	if err != nil {
		b.Fatal(err)
	}
	bucket := segmentedTabletRouterTestBucket(leaves[1536].Ref)
	b.Run("posting-bound-locator", func(b *testing.B) {
		b.ReportAllocs()
		var ref PageRef
		var zone BucketZone
		var ok bool
		for b.Loop() {
			ref, zone, ok = anchor.ResolveBucket(&locator, bucket)
		}
		if !ok {
			b.Fatal("posting resolve")
		}
		globalTabletCatalogBenchmarkRef = ref
		segmentedTabletRouterBenchmarkZone = zone
	})
}

func BenchmarkGlobalTabletCatalogPacked14Decode(b *testing.B) {
	var packed [globalTabletCatalogPackedBytes]byte
	for id := range uint16(TabletLocalIdentityLocalCount) {
		code := uint16(GlobalTabletCatalogLocatorLive)<<12 |
			uint16(id&0x0fff)
		globalTabletCatalogPut14(packed[:], id, code)
	}
	b.ReportAllocs()
	var code uint16
	for b.Loop() {
		code = globalTabletCatalogGet14(packed[:], 4095)
	}
	binary.LittleEndian.PutUint16(packed[:2], code)
}
