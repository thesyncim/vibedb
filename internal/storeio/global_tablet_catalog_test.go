package storeio

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"testing"
)

var globalTabletCatalogTestStoreID = [16]byte{
	'g', 'l', 'o', 'b', 'a', 'l', '-', 'c',
	'a', 't', 'a', 'l', 'o', 'g', '0', '1',
}

var globalTabletCatalogTestBounds = GlobalTabletCatalogBounds{
	StoreID:                globalTabletCatalogTestStoreID,
	SelectedRootGeneration: 200,
	FileEnd:                1 << 44, NextLogicalID: GlobalTabletCatalogFirstDynamicLogicalID + 1<<20,
}

func globalTabletCatalogTestRef(
	offset, logicalID, generation uint64, length uint32, kind PageKind,
) PageRef {
	return PageRef{
		Offset: offset, LogicalID: logicalID, Generation: generation,
		Length: length, Kind: kind,
	}
}

func globalTabletCatalogTestNode(
	t testing.TB,
) (
	GlobalTabletCatalogNodeView,
	[]byte,
	PageRef,
	[]GlobalTabletCatalogNodeEntry,
) {
	t.Helper()
	const generation = uint64(100)
	// Stable IDs are deliberately non-monotonic in lexical floor order. A
	// middle split allocates a fresh ID without renumbering either neighbor.
	tablets := []uint32{7, 19, 11}
	floors := [][]byte{nil, []byte("m"), []byte("z")}
	entries := make([]GlobalTabletCatalogNodeEntry, len(tablets))
	for at, tabletID := range tablets {
		logicalID, _ := GlobalTabletCatalogTabletRootLogicalID(tabletID)
		entries[at] = GlobalTabletCatalogNodeEntry{
			Floor: floors[at], ID: tabletID,
			Ref: globalTabletCatalogTestRef(
				uint64(at+1)*GlobalTabletCatalogTabletBytes,
				logicalID, generation, GlobalTabletCatalogTabletBytes,
				PageTabletRoute,
			),
		}
	}
	logicalID, _ := GlobalTabletCatalogCatalogLeafLogicalID(3)
	image, err := EncodeGlobalTabletCatalogNode(
		make([]byte, GlobalTabletCatalogNodeBytes),
		GlobalTabletCatalogNodeHeader{
			StoreID:    globalTabletCatalogTestStoreID,
			Bounds:     globalTabletCatalogTestBounds,
			Generation: generation, LogicalID: logicalID, PageID: 3,
			Level: GlobalTabletCatalogLeaf,
			Kind:  PagePrimaryCatalog, ChildKind: PageTabletRoute,
			ChildLength: GlobalTabletCatalogTabletBytes,
		},
		entries,
	)
	if err != nil {
		t.Fatal(err)
	}
	ref := globalTabletCatalogTestRef(
		128<<10, logicalID, generation,
		GlobalTabletCatalogNodeBytes, PagePrimaryCatalog,
	)
	view, err := OpenGlobalTabletCatalogNode(
		image, ref, globalTabletCatalogTestBounds,
	)
	if err != nil {
		t.Fatal(err)
	}
	return view, image, ref, entries
}

func TestGlobalTabletCatalogExactRouteCursorAndCOW(t *testing.T) {
	view, image, ref, entries := globalTabletCatalogTestNode(t)
	for _, test := range []struct {
		key  string
		want int
	}{
		{"", 0},
		{"a", 0},
		{"m", 1},
		{"y", 1},
		{"z", 2},
		{"zz", 2},
	} {
		got := view.Route([]byte(test.key))
		if got.ID != entries[test.want].ID ||
			got.Ref != entries[test.want].Ref ||
			int(got.Ordinal) != test.want {
			t.Fatalf("route %q = %+v, want entry %d", test.key, got, test.want)
		}
	}
	cursor := view.LowerBound([]byte("n"))
	for want := 1; want < len(entries); want++ {
		got, ok := cursor.Route()
		if !ok || got.ID != entries[want].ID {
			t.Fatalf("cursor %d = %+v,%v", want, got, ok)
		}
		if want+1 < len(entries) && !cursor.Next() {
			t.Fatalf("cursor ended at %d", want)
		}
	}
	replacement := entries[1].Ref
	replacement.Offset += 64 << 10
	replacement.Generation++
	before := bytes.Clone(image)
	if _, err := view.RewriteHandle(
		image, replacement.Generation, globalTabletCatalogTestBounds,
		entries[1].ID, replacement,
	); err == nil {
		t.Fatal("accepted overlapping COW destination")
	}
	if !bytes.Equal(image, before) {
		t.Fatal("overlap rejection changed admitted source")
	}
	backing := make([]byte, len(image)+1)
	copy(backing[1:], image)
	shiftedView, err := OpenGlobalTabletCatalogNode(
		backing[1:], ref, globalTabletCatalogTestBounds,
	)
	if err != nil {
		t.Fatal(err)
	}
	shiftedBefore := bytes.Clone(backing[1:])
	if _, err := shiftedView.RewriteHandle(
		backing[:len(image)], replacement.Generation,
		globalTabletCatalogTestBounds,
		entries[1].ID, replacement,
	); err == nil {
		t.Fatal("accepted partially overlapping COW destination")
	}
	if !bytes.Equal(backing[1:], shiftedBefore) {
		t.Fatal("partial-overlap rejection changed admitted source")
	}
	next, err := view.RewriteHandle(
		make([]byte, len(image)), replacement.Generation,
		globalTabletCatalogTestBounds,
		entries[1].ID, replacement,
	)
	if err != nil {
		t.Fatal(err)
	}
	nextRef := ref
	nextRef.Generation = replacement.Generation
	nextView, err := OpenGlobalTabletCatalogNode(
		next, nextRef, globalTabletCatalogTestBounds,
	)
	if err != nil {
		t.Fatal(err)
	}
	if nextView.floors.Header().Generation != nextRef.Generation {
		t.Fatalf(
			"embedded floor-map birth = %d, want node birth %d",
			nextView.floors.Header().Generation, nextRef.Generation,
		)
	}
	if got := nextView.Route([]byte("n")); got.Ref != replacement {
		t.Fatalf("COW route = %+v, want %+v", got.Ref, replacement)
	}
	if got := view.Route([]byte("n")); got.Ref != entries[1].Ref {
		t.Fatal("COW changed old snapshot")
	}
}

func TestGlobalTabletCatalogCOWIsCanonicalAcrossHistories(t *testing.T) {
	view100, _, ref100, entries := globalTabletCatalogTestNode(t)
	replacement := entries[1].Ref
	replacement.Offset += 128 << 10
	replacement.Generation = 102
	from100, err := view100.RewriteHandle(
		make([]byte, len(view100.image)), 102,
		globalTabletCatalogTestBounds,
		entries[1].ID, replacement,
	)
	if err != nil {
		t.Fatal(err)
	}

	image101, err := EncodeGlobalTabletCatalogNode(
		make([]byte, GlobalTabletCatalogNodeBytes),
		GlobalTabletCatalogNodeHeader{
			StoreID:    globalTabletCatalogTestStoreID,
			Bounds:     globalTabletCatalogTestBounds,
			Generation: 101, LogicalID: ref100.LogicalID, PageID: 3,
			Level: GlobalTabletCatalogLeaf,
			Kind:  PagePrimaryCatalog, ChildKind: PageTabletRoute,
			ChildLength: GlobalTabletCatalogTabletBytes,
		},
		entries,
	)
	if err != nil {
		t.Fatal(err)
	}
	ref101 := ref100
	ref101.Generation = 101
	view101, err := OpenGlobalTabletCatalogNode(
		image101, ref101, globalTabletCatalogTestBounds,
	)
	if err != nil {
		t.Fatal(err)
	}
	from101, err := view101.RewriteHandle(
		make([]byte, len(view101.image)), 102,
		globalTabletCatalogTestBounds,
		entries[1].ID, replacement,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(from100, from101) {
		t.Fatal("equivalent catalog COW depends on ancestor generation")
	}

	corrupt := bytes.Clone(from100)
	payload := corrupt[PageHeaderSize:]
	mapStart := GlobalTabletCatalogNodePayloadHeaderBytes
	binary.LittleEndian.PutUint64(payload[mapStart+24:], 101)
	tabletAnchorMapSeal(
		payload[mapStart : mapStart+
			int(binary.LittleEndian.Uint32(payload[12:16]))],
	)
	if _, err := sealInitializedPage(corrupt); err != nil {
		t.Fatal(err)
	}
	expected := ref100
	expected.Generation = 102
	if _, err := OpenGlobalTabletCatalogNode(
		corrupt, expected, globalTabletCatalogTestBounds,
	); err == nil {
		t.Fatal("accepted floor map born before its enclosing node")
	}
}

func TestGlobalTabletCatalogCOWUsesMonotonicDestinationBounds(t *testing.T) {
	view, _, ref, entries := globalTabletCatalogTestNode(t)
	replacement := entries[1].Ref
	replacement.Offset = globalTabletCatalogTestBounds.FileEnd
	replacement.Generation = 101
	dst := make([]byte, len(view.image))
	before := bytes.Clone(dst)

	if _, err := view.RewriteHandle(
		dst, 101, globalTabletCatalogTestBounds,
		entries[1].ID, replacement,
	); err == nil {
		t.Fatal("accepted appended child under source file bounds")
	}
	if !bytes.Equal(dst, before) {
		t.Fatal("bounds rejection changed destination")
	}

	shrunk := globalTabletCatalogTestBounds
	shrunk.FileEnd--
	if _, err := view.RewriteHandle(
		dst, 101, shrunk, entries[1].ID, entries[1].Ref,
	); err == nil {
		t.Fatal("accepted shrinking destination bounds")
	}

	expanded := globalTabletCatalogTestBounds
	expanded.FileEnd += GlobalTabletCatalogTabletBytes
	next, err := view.RewriteHandle(
		dst, 101, expanded, entries[1].ID, replacement,
	)
	if err != nil {
		t.Fatal(err)
	}
	nextRef := ref
	nextRef.Generation = 101
	nextView, err := OpenGlobalTabletCatalogNode(next, nextRef, expanded)
	if err != nil {
		t.Fatal(err)
	}
	if got := nextView.Route([]byte("n")); got.Ref != replacement {
		t.Fatalf("expanded-bounds route = %+v, want %+v", got.Ref, replacement)
	}
}

func TestGlobalTabletCatalogNonMonotonicStableIDsAllLevels(t *testing.T) {
	for _, level := range []GlobalTabletCatalogNodeLevel{
		GlobalTabletCatalogLeaf,
		GlobalTabletCatalogBranch,
		GlobalTabletCatalogRoot,
	} {
		t.Run(fmt.Sprintf("level-%d", level), func(t *testing.T) {
			ids := []uint32{0, 2, 1}
			floors := [][]byte{nil, []byte("m"), []byte("z")}
			childLevel := GlobalTabletCatalogLeaf
			childKind := PagePrimaryCatalog
			childLength := uint32(GlobalTabletCatalogNodeBytes)
			pageID := uint32(3)
			var logicalID uint64
			switch level {
			case GlobalTabletCatalogLeaf:
				childKind = PageTabletRoute
				childLength = GlobalTabletCatalogTabletBytes
				logicalID, _ = GlobalTabletCatalogCatalogLeafLogicalID(pageID)
			case GlobalTabletCatalogBranch:
				logicalID, _ = GlobalTabletCatalogCatalogBranchLogicalID(pageID)
			case GlobalTabletCatalogRoot:
				pageID = 0
				childLevel = GlobalTabletCatalogBranch
				logicalID = GlobalTabletCatalogRootLogicalID
			}
			entries := make([]GlobalTabletCatalogNodeEntry, len(ids))
			for at, id := range ids {
				childLogical, ok := globalTabletCatalogChildLogicalID(
					level, childLevel, id,
				)
				if !ok {
					t.Fatal("derive non-monotonic child")
				}
				entries[at] = GlobalTabletCatalogNodeEntry{
					Floor: floors[at], ID: id,
					Ref: globalTabletCatalogTestRef(
						uint64(at+1)*8192, childLogical, 50,
						childLength, childKind,
					),
				}
			}
			pageBytes, _ := globalTabletCatalogNodePageBytes(level)
			image, err := EncodeGlobalTabletCatalogNode(
				make([]byte, pageBytes),
				GlobalTabletCatalogNodeHeader{
					StoreID:    globalTabletCatalogTestStoreID,
					Bounds:     globalTabletCatalogTestBounds,
					Generation: 50, LogicalID: logicalID, PageID: pageID,
					Level: level, RootChildLevel: childLevel,
					Kind: PagePrimaryCatalog, ChildKind: childKind,
					ChildLength: childLength,
				},
				entries,
			)
			if err != nil {
				t.Fatal(err)
			}
			ref := globalTabletCatalogTestRef(
				1<<30, logicalID, 50, uint32(pageBytes), PagePrimaryCatalog,
			)
			view, err := OpenGlobalTabletCatalogNode(
				image, ref, globalTabletCatalogTestBounds,
			)
			if err != nil {
				t.Fatal(err)
			}
			crossStore := globalTabletCatalogTestBounds
			crossStore.StoreID[0] ^= 1
			if _, err := OpenGlobalTabletCatalogNode(
				image, ref, crossStore,
			); !errors.Is(err, ErrGlobalTabletCatalogCorrupt) {
				t.Fatalf("cross-Store node graft error = %v", err)
			}
			staleSnapshot := globalTabletCatalogTestBounds
			staleSnapshot.SelectedRootGeneration = ref.Generation - 1
			if _, err := OpenGlobalTabletCatalogNode(
				image, ref, staleSnapshot,
			); !errors.Is(err, ErrGlobalTabletCatalogCorrupt) {
				t.Fatalf("stale-generation node graft error = %v", err)
			}
			for at, key := range [][]byte{nil, []byte("n"), []byte("zz")} {
				if got := view.Route(key); got.ID != ids[at] ||
					got.Ref != entries[at].Ref {
					t.Fatalf("route %q = %+v, want %+v", key, got, entries[at])
				}
			}
			cursor := view.LowerBound(nil)
			for at, want := range ids {
				got, ok := cursor.Route()
				if !ok || got.ID != want {
					t.Fatalf("cursor %d = %+v,%v", at, got, ok)
				}
				if at+1 < len(ids) && !cursor.Next() {
					t.Fatal("early cursor end")
				}
			}
			replacement := entries[1].Ref
			replacement.Offset += 64 << 10
			replacement.Generation++
			next, err := view.RewriteHandle(
				make([]byte, pageBytes), replacement.Generation,
				globalTabletCatalogTestBounds,
				ids[1], replacement,
			)
			if err != nil {
				t.Fatal(err)
			}
			nextRef := ref
			nextRef.Generation++
			nextView, err := OpenGlobalTabletCatalogNode(
				next, nextRef, globalTabletCatalogTestBounds,
			)
			if err != nil {
				t.Fatal(err)
			}
			if got := nextView.Route([]byte("n")); got.ID != ids[1] ||
				got.Ref != replacement {
				t.Fatalf("COW route = %+v", got)
			}
		})
	}
}

func TestGlobalTabletCatalogAdaptiveDepthAndWorstCaseCapacity(t *testing.T) {
	leaf := GlobalTabletCatalogWorstCaseFanout(
		GlobalTabletCatalogNodeBytes, 256,
	)
	root := GlobalTabletCatalogWorstCaseFanout(
		GlobalTabletCatalogRootBytes, 256,
	)
	if leaf < 28 || root < 235 {
		t.Fatalf("worst-case fanout leaf/root = %d/%d, want >=28/>=235", leaf, root)
	}
	if capacity := uint64(leaf) * uint64(leaf) * uint64(root); capacity < 174_000 {
		t.Fatalf("three-level adversarial capacity = %d", capacity)
	}
	if twoLevel := uint64(leaf) * uint64(root); twoLevel >= 174_000 {
		t.Fatalf("test no longer exercises rare third level: %d", twoLevel)
	}
	bounds, ok := GlobalTabletCatalogCatalogGeometry(174_000, 256)
	if !ok || bounds.Levels != 3 || bounds.PointPages != 3 ||
		bounds.LeafPages != 6215 || bounds.BranchPages != 222 ||
		bounds.MaximumTablets < 174_000 ||
		bounds.COWBytes != 80<<10 ||
		bounds.ResidentBytes != 64<<10 {
		t.Fatalf("174k adversarial bounds = %+v,%v", bounds, ok)
	}
	if typical, ok := GlobalTabletCatalogCatalogGeometry(174_000, 8); !ok ||
		typical.Levels != 2 || typical.PointPages != 2 {
		t.Fatalf("short-fence bounds = %+v,%v", typical, ok)
	}

	// Exercise the actual prefix/restart encoder near the universal leaf
	// bound with valid, strictly ordered 256-byte binary separators.
	count := leaf
	entries := make([]GlobalTabletCatalogNodeEntry, count)
	for at := range entries {
		var floor []byte
		if at != 0 {
			floor = make([]byte, 256)
			binary.BigEndian.PutUint32(floor, uint32(at))
			for i := 4; i < len(floor); i++ {
				floor[i] = byte(uint32(at)*0x9e3779b9 + uint32(i)*0x85ebca6b)
			}
		}
		logicalID, _ := GlobalTabletCatalogTabletRootLogicalID(uint32(at))
		entries[at] = GlobalTabletCatalogNodeEntry{
			Floor: floor, ID: uint32(at),
			Ref: globalTabletCatalogTestRef(
				uint64(at+1)*8192, logicalID, 10,
				GlobalTabletCatalogTabletBytes, PageTabletRoute,
			),
		}
	}
	logicalID, _ := GlobalTabletCatalogCatalogLeafLogicalID(0)
	if _, err := EncodeGlobalTabletCatalogNode(
		make([]byte, GlobalTabletCatalogNodeBytes),
		GlobalTabletCatalogNodeHeader{
			StoreID:    globalTabletCatalogTestStoreID,
			Bounds:     globalTabletCatalogTestBounds,
			Generation: 10, LogicalID: logicalID,
			Level: GlobalTabletCatalogLeaf,
			Kind:  PagePrimaryCatalog, ChildKind: PageTabletRoute,
			ChildLength: GlobalTabletCatalogTabletBytes,
		},
		entries,
	); err != nil {
		t.Fatalf("actual worst-case-bound corpus: %v", err)
	}
}

func TestGlobalTabletCatalogTwoAndThreeLevelRootTyping(t *testing.T) {
	for _, childLevel := range []GlobalTabletCatalogNodeLevel{
		GlobalTabletCatalogLeaf,
		GlobalTabletCatalogBranch,
	} {
		childID := uint32(5)
		var childLogical uint64
		var ok bool
		if childLevel == GlobalTabletCatalogLeaf {
			childLogical, ok = GlobalTabletCatalogCatalogLeafLogicalID(childID)
		} else {
			childLogical, ok = GlobalTabletCatalogCatalogBranchLogicalID(childID)
		}
		if !ok {
			t.Fatal("child logical ID")
		}
		entry := GlobalTabletCatalogNodeEntry{
			ID: childID,
			Ref: globalTabletCatalogTestRef(
				8192, childLogical, 20,
				GlobalTabletCatalogNodeBytes, PagePrimaryCatalog,
			),
		}
		image, err := EncodeGlobalTabletCatalogNode(
			make([]byte, GlobalTabletCatalogRootBytes),
			GlobalTabletCatalogNodeHeader{
				StoreID:    globalTabletCatalogTestStoreID,
				Bounds:     globalTabletCatalogTestBounds,
				Generation: 20, LogicalID: GlobalTabletCatalogRootLogicalID,
				Level: GlobalTabletCatalogRoot, RootChildLevel: childLevel,
				Kind: PagePrimaryCatalog, ChildKind: PagePrimaryCatalog,
				ChildLength: GlobalTabletCatalogNodeBytes,
			},
			[]GlobalTabletCatalogNodeEntry{entry},
		)
		if err != nil {
			t.Fatal(err)
		}
		rootRef := globalTabletCatalogTestRef(
			256<<10, GlobalTabletCatalogRootLogicalID, 20,
			GlobalTabletCatalogRootBytes, PagePrimaryCatalog,
		)
		view, err := OpenGlobalTabletCatalogNode(
			image, rootRef, globalTabletCatalogTestBounds,
		)
		if err != nil {
			t.Fatal(err)
		}
		if got := view.Route(nil); got.ID != childID || got.Ref != entry.Ref {
			t.Fatalf("root child level %d = %+v", childLevel, got)
		}
	}
}

func TestGlobalTabletCatalogCompactLocator(t *testing.T) {
	const tabletID = uint32(42)
	logicalID, _ := GlobalTabletCatalogLocatorLogicalID(tabletID)
	header := PageHeader{
		StoreID:    globalTabletCatalogTestStoreID,
		Generation: 77, LogicalID: logicalID,
		PageSize: GlobalTabletCatalogLocatorBytes,
		PayloadLength: GlobalTabletCatalogLocatorHeader +
			globalTabletCatalogPackedBytes,
		Kind: PagePrimaryLocator,
	}
	entries := []GlobalTabletCatalogLocatorEntry{
		{LocalID: 0, PageID: 0, RowSlot: 7, State: GlobalTabletCatalogLocatorLive},
		{LocalID: 1, PageID: 15, RowSlot: 255, State: GlobalTabletCatalogLocatorRetired},
		{LocalID: 2047, PageID: 8, RowSlot: 9, State: GlobalTabletCatalogLocatorLive},
		{LocalID: 4095, PageID: 14, RowSlot: 3, State: GlobalTabletCatalogLocatorLive},
	}
	image, err := EncodeGlobalTabletCatalogLocator(
		make([]byte, GlobalTabletCatalogLocatorBytes),
		header, globalTabletCatalogTestBounds, tabletID, 70, entries,
	)
	if err != nil {
		t.Fatal(err)
	}
	ref := globalTabletCatalogTestRef(
		64<<10, logicalID, header.Generation,
		GlobalTabletCatalogLocatorBytes, header.Kind,
	)
	view, err := OpenGlobalTabletCatalogLocator(
		image, ref, globalTabletCatalogTestBounds,
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		pageID, rowSlot, state := view.Resolve(entry.LocalID)
		if pageID != entry.PageID || rowSlot != entry.RowSlot ||
			state != entry.State {
			t.Fatalf("resolve %d = %d/%d/%d, want %+v",
				entry.LocalID, pageID, rowSlot, state, entry)
		}
	}
	if _, _, state := view.Resolve(99); state != GlobalTabletCatalogLocatorEmpty {
		t.Fatalf("empty state = %d", state)
	}
	if len(view.packed) != 7168 {
		t.Fatalf("packed locator bytes = %d", len(view.packed))
	}
}

func TestGlobalTabletCatalogCacheableTabletReadPaths(t *testing.T) {
	header, leaves, anchorRefs := segmentedTabletRouterTestInputs(t, 1024)
	header.StoreID = globalTabletCatalogTestStoreID
	bounds := globalTabletCatalogTestBounds
	bounds.SelectedRootGeneration = header.Generation
	root, rawLocator, anchors, _, err := EncodeSegmentedTabletRouter(
		make([]byte, SegmentedTabletRouterRootBytes),
		make([]byte, SegmentedTabletRouterLocatorBytes),
		make([]byte, 4*SegmentedTabletRouterAnchorPageBytes),
		header, anchorRefs, leaves,
	)
	if err != nil {
		t.Fatal(err)
	}
	locatorLogical, _ := GlobalTabletCatalogLocatorLogicalID(header.TabletID)
	locatorRef := globalTabletCatalogTestRef(
		1<<20, locatorLogical, header.Generation,
		GlobalTabletCatalogLocatorBytes, PagePrimaryLocator,
	)
	locatorEntries := make([]GlobalTabletCatalogLocatorEntry, len(leaves))
	for rank, leaf := range leaves {
		code := binary.LittleEndian.Uint16(rawLocator[int(leaf.LocalID)*2:])
		locatorEntries[rank] = GlobalTabletCatalogLocatorEntry{
			LocalID: leaf.LocalID, PageID: uint8(code >> 8),
			RowSlot: uint8(code), State: GlobalTabletCatalogLocatorLive,
		}
	}
	// Encoder requires LocalID order; physical locator identity is independent
	// of lexical anchor order.
	for i := 1; i < len(locatorEntries); i++ {
		for j := i; j > 0 && locatorEntries[j].LocalID < locatorEntries[j-1].LocalID; j-- {
			locatorEntries[j], locatorEntries[j-1] = locatorEntries[j-1], locatorEntries[j]
		}
	}
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
		t.Fatal(err)
	}
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
		t.Fatal(err)
	}
	tablet, err := OpenGlobalTabletCatalogTabletRoot(
		tabletImage, tabletRef, bounds,
	)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := tablet.LocatorRef(); !ok || got != locatorRef {
		t.Fatalf("locator ref = %+v,%v", got, ok)
	}
	locator, err := OpenGlobalTabletCatalogLocator(
		locatorImage, locatorRef, bounds,
	)
	if err != nil {
		t.Fatal(err)
	}
	crossStore := bounds
	crossStore.StoreID[0] ^= 1
	if _, err := OpenGlobalTabletCatalogTabletRoot(
		tabletImage, tabletRef, crossStore,
	); !errors.Is(err, ErrGlobalTabletCatalogCorrupt) {
		t.Fatalf("cross-Store tablet graft error = %v", err)
	}
	if _, err := OpenGlobalTabletCatalogLocator(
		locatorImage, locatorRef, crossStore,
	); !errors.Is(err, ErrGlobalTabletCatalogCorrupt) {
		t.Fatalf("cross-Store locator graft error = %v", err)
	}
	staleSnapshot := bounds
	staleSnapshot.SelectedRootGeneration = header.Generation - 1
	if _, err := OpenGlobalTabletCatalogTabletRoot(
		tabletImage, tabletRef, staleSnapshot,
	); !errors.Is(err, ErrGlobalTabletCatalogCorrupt) {
		t.Fatalf("stale-generation tablet graft error = %v", err)
	}
	if _, err := OpenGlobalTabletCatalogLocator(
		locatorImage, locatorRef, staleSnapshot,
	); !errors.Is(err, ErrGlobalTabletCatalogCorrupt) {
		t.Fatalf("stale-generation locator graft error = %v", err)
	}
	for _, rank := range []int{0, 255, 256, 511, 512, 1023} {
		key := leaves[rank].Fence
		next, ok := tablet.RouteAnchor(key)
		if !ok {
			t.Fatalf("route anchor %d", rank)
		}
		pageImage := anchors[int(next.PageID)*SegmentedTabletRouterAnchorPageBytes:]
		anchor, err := OpenGlobalTabletCatalogAnchor(
			pageImage[:SegmentedTabletRouterAnchorPageBytes],
			&tablet, next.PageID,
		)
		if err != nil {
			t.Fatal(err)
		}
		hash := KeyHashBytes(segmentedTabletRouterTestSeed, key)
		point, ok := anchor.RouteHashed(hash, key)
		if !ok || point.Bucket != segmentedTabletRouterTestBucket(leaves[rank].Ref) ||
			point.Ref != leaves[rank].Ref {
			t.Fatalf("point %d = %+v,%v", rank, point, ok)
		}
		ref, zone, ok := anchor.ResolveBucket(&locator, point.Bucket)
		if !ok || ref != point.Ref || zone != point.Zone {
			t.Fatalf("posting %d = %+v/%x/%v", rank, ref, zone, ok)
		}
	}
	target := 512
	selected, _ := tablet.RouteAnchor(leaves[target].Fence)
	start := int(selected.PageID) * SegmentedTabletRouterAnchorPageBytes
	anchor, err := OpenGlobalTabletCatalogAnchor(
		anchors[start:start+SegmentedTabletRouterAnchorPageBytes],
		&tablet, selected.PageID,
	)
	if err != nil {
		t.Fatal(err)
	}
	wantFence := leaves[target].Fence
	prefix := []byte("owned-prefix:")
	fenceScratch := make([]byte, len(prefix), len(prefix)+len(wantFence))
	copy(fenceScratch, prefix)
	appended, ok := anchor.AppendFenceAt(fenceScratch, 0)
	if !ok || !bytes.Equal(appended[:len(prefix)], prefix) ||
		!bytes.Equal(appended[len(prefix):], wantFence) {
		t.Fatalf("appended fence = %q,%v, want prefix %q fence %q",
			appended, ok, prefix, wantFence)
	}
	if got := testing.AllocsPerRun(1000, func() {
		var appendOK bool
		fenceScratch, appendOK = anchor.AppendFenceAt(
			fenceScratch[:len(prefix)], 0,
		)
		if !appendOK {
			panic("AppendFenceAt failed on admitted rank")
		}
	}); got != 0 {
		t.Fatalf("AppendFenceAt allocations = %v, want 0", got)
	}
	crossStoreRoot := tablet
	crossStoreRoot.bounds.StoreID[0] ^= 1
	if _, err := OpenGlobalTabletCatalogAnchor(
		anchors[start:start+SegmentedTabletRouterAnchorPageBytes],
		&crossStoreRoot, selected.PageID,
	); !errors.Is(err, ErrGlobalTabletCatalogCorrupt) {
		t.Fatalf("cross-Store anchor owner error = %v", err)
	}
	staleRoot := tablet
	staleRoot.bounds.SelectedRootGeneration = tablet.header.Generation - 1
	if _, err := OpenGlobalTabletCatalogAnchor(
		anchors[start:start+SegmentedTabletRouterAnchorPageBytes],
		&staleRoot, selected.PageID,
	); !errors.Is(err, ErrGlobalTabletCatalogCorrupt) {
		t.Fatalf("stale-generation anchor owner error = %v", err)
	}
	bucket := segmentedTabletRouterTestBucket(leaves[target].Ref)
	graft := locator
	graft.ref.Offset += 8192
	if _, _, ok := anchor.ResolveBucket(&graft, bucket); ok {
		t.Fatal("accepted same-tablet locator under another PageRef")
	}
	staleRef := locatorRef
	staleRef.Offset += 16 << 10
	staleRef.Generation--
	staleImage, err := EncodeGlobalTabletCatalogLocator(
		make([]byte, GlobalTabletCatalogLocatorBytes),
		PageHeader{
			StoreID:    globalTabletCatalogTestStoreID,
			Generation: staleRef.Generation, LogicalID: locatorLogical,
			PageSize: GlobalTabletCatalogLocatorBytes,
			PayloadLength: GlobalTabletCatalogLocatorHeader +
				globalTabletCatalogPackedBytes,
			Kind: staleRef.Kind,
		},
		bounds,
		header.TabletID, staleRef.Generation, locatorEntries,
	)
	if err != nil {
		t.Fatal(err)
	}
	stale, err := OpenGlobalTabletCatalogLocator(
		staleImage, staleRef, bounds,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, ok := anchor.ResolveBucket(&stale, bucket); ok {
		t.Fatal("grafted stale same-tablet locator")
	}

	// The selected anchor is full. A localized leaf split must preserve all
	// three unaffected anchor references, replace this anchor, append exactly
	// one stable anchor, and publish a locator that resolves both halves.
	point, ok := anchor.RouteHashed(
		KeyHashBytes(segmentedTabletRouterTestSeed, leaves[target].Fence),
		leaves[target].Fence,
	)
	if !ok {
		t.Fatal("localized split source route")
	}
	used := make([]bool, TabletLocalIdentityLocalCount)
	for _, leaf := range leaves {
		used[leaf.LocalID] = true
	}
	rightLocalID := uint16(0)
	for used[rightLocalID] {
		rightLocalID++
	}
	rightBucketU, _ := MakeTabletLocalIdentityBucket(header.TabletID, uint32(rightLocalID))
	rightBucket := BucketID(rightBucketU)
	rightFence, err := ShortestPrimaryFence(
		make([]byte, len(leaves[target+1].Fence)),
		leaves[target].Fence, leaves[target+1].Fence,
	)
	if err != nil {
		t.Fatal(err)
	}
	nextGeneration := header.Generation + 1
	leftRef := point.Ref
	leftRef.Offset = 3 << 20
	leftRef.Generation = nextGeneration
	rightLogical, _ := CommonPrimaryLeafLogicalID(rightBucket)
	rightRef := globalTabletCatalogTestRef(
		4<<20, rightLogical, nextGeneration, point.Ref.Length, point.Ref.Kind,
	)
	leftAnchorLogical, _ := GlobalTabletCatalogAnchorLogicalID(header.TabletID, selected.PageID)
	leftAnchorRef := globalTabletCatalogTestRef(
		5<<20, leftAnchorLogical, nextGeneration,
		SegmentedTabletRouterAnchorPageBytes, PagePrimaryAnchor,
	)
	rightPageID := uint8(tablet.AnchorCount())
	rightAnchorLogical, _ := GlobalTabletCatalogAnchorLogicalID(header.TabletID, rightPageID)
	rightAnchorRef := globalTabletCatalogTestRef(
		6<<20, rightAnchorLogical, nextGeneration,
		SegmentedTabletRouterAnchorPageBytes, PagePrimaryAnchor,
	)
	localized, err := tablet.InsertSplitLeaf(
		make([]byte, SegmentedTabletRouterRootBytes),
		make([]byte, GlobalTabletCatalogLocatorBytes),
		make([]byte, SegmentedTabletRouterAnchorPageBytes),
		make([]byte, SegmentedTabletRouterAnchorPageBytes),
		nextGeneration, point, leftRef, rightLocalID, rightFence, rightRef,
		leftAnchorRef, rightAnchorRef, &locator, &anchor,
	)
	if err != nil {
		t.Fatal(err)
	}
	if localized.PageCount != 5 || localized.Bytes !=
		SegmentedTabletRouterRootBytes+GlobalTabletCatalogLocatorBytes+
			2*SegmentedTabletRouterAnchorPageBytes {
		t.Fatalf("localized split geometry = pages %d bytes %d", localized.PageCount, localized.Bytes)
	}
	nextBounds := bounds
	nextBounds.SelectedRootGeneration = nextGeneration
	nextLocatorRef := locatorRef
	nextLocatorRef.Offset = 7 << 20
	nextLocatorRef.Generation = nextGeneration
	nextTabletRef := tabletRef
	nextTabletRef.Offset = 8 << 20
	nextTabletRef.Generation = nextGeneration
	nextTabletImage, err := EncodeGlobalTabletCatalogTabletRoot(
		make([]byte, GlobalTabletCatalogTabletBytes),
		PageHeader{StoreID: header.StoreID, Generation: nextGeneration,
			LogicalID: tabletLogical, PageSize: GlobalTabletCatalogTabletBytes,
			PayloadLength: GlobalTabletCatalogRootHeader + SegmentedTabletRouterRootBytes,
			Kind:          PageTabletRoute},
		nextBounds, nextLocatorRef, localized.Root,
	)
	if err != nil {
		t.Fatal(err)
	}
	nextTablet, err := OpenGlobalTabletCatalogTabletRoot(nextTabletImage, nextTabletRef, nextBounds)
	if err != nil {
		t.Fatal(err)
	}
	for rank := 0; rank < tablet.AnchorCount(); rank++ {
		oldRoute, _ := tablet.AnchorAt(rank)
		if oldRoute.PageID == selected.PageID {
			continue
		}
		found := false
		for nextRank := 0; nextRank < nextTablet.AnchorCount(); nextRank++ {
			nextRoute, _ := nextTablet.AnchorAt(nextRank)
			if nextRoute.PageID == oldRoute.PageID {
				found = nextRoute.Ref == oldRoute.Ref
				break
			}
		}
		if !found {
			t.Fatalf("unaffected anchor page %d was not preserved", oldRoute.PageID)
		}
	}
	nextLocator, err := OpenGlobalTabletCatalogLocator(localized.Locator, nextLocatorRef, nextBounds)
	if err != nil {
		t.Fatal(err)
	}
	pageID, _, state := nextLocator.Resolve(rightLocalID)
	rightRoute, routeOK := nextTablet.RouteAnchor(rightFence)
	if !routeOK || pageID != rightRoute.PageID || state != GlobalTabletCatalogLocatorLive {
		t.Fatalf("right locator = page %d state %d", pageID, state)
	}
}

func TestGlobalTabletCatalogFailClosed(t *testing.T) {
	_, image, ref, _ := globalTabletCatalogTestNode(t)
	tests := map[string]func([]byte){
		"child state": func(page []byte) {
			header, payload, err := OpenPage(page)
			if err != nil {
				panic(err)
			}
			mapBytes := int(binary.LittleEndian.Uint32(payload[12:16]))
			handle := PageHeaderSize + GlobalTabletCatalogNodePayloadHeaderBytes + mapBytes
			clear(page[handle : handle+GlobalTabletCatalogHandleBytes])
			if _, err := SealPage(page[:header.PageSize]); err != nil {
				panic(err)
			}
		},
		"child length": func(page []byte) {
			binary.LittleEndian.PutUint32(
				page[PageHeaderSize+16:PageHeaderSize+20],
				GlobalTabletCatalogTabletBytes*2,
			)
			if _, err := SealPage(page); err != nil {
				panic(err)
			}
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			corrupt := bytes.Clone(image)
			mutate(corrupt)
			if _, err := OpenGlobalTabletCatalogNode(
				corrupt, ref, globalTabletCatalogTestBounds,
			); !errors.Is(
				err, ErrGlobalTabletCatalogCorrupt,
			) {
				t.Fatalf("open error = %v", err)
			}
		})
	}
}

func TestGlobalTabletCatalogRootAndLocatorFailClosed(t *testing.T) {
	header, leaves, anchorRefs := segmentedTabletRouterTestInputs(t, 1)
	header.StoreID = globalTabletCatalogTestStoreID
	bounds := globalTabletCatalogTestBounds
	bounds.SelectedRootGeneration = header.Generation
	rawRoot, _, _, _, err := EncodeSegmentedTabletRouter(
		make([]byte, SegmentedTabletRouterRootBytes),
		make([]byte, SegmentedTabletRouterLocatorBytes),
		make([]byte, SegmentedTabletRouterAnchorPageBytes),
		header, anchorRefs, leaves,
	)
	if err != nil {
		t.Fatal(err)
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
		locatorRef, rawRoot,
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Run("discoverable locator binding", func(t *testing.T) {
		corrupt := bytes.Clone(tabletImage)
		payload := corrupt[PageHeaderSize:]
		ref := decodePageRef(payload[16 : 16+PageRefSize])
		ref.LogicalID++
		encodePageRef(payload[16:16+PageRefSize], ref)
		if _, err := SealPage(corrupt); err != nil {
			t.Fatal(err)
		}
		if _, err := OpenGlobalTabletCatalogTabletRoot(
			corrupt, tabletRef, bounds,
		); !errors.Is(err, ErrGlobalTabletCatalogCorrupt) {
			t.Fatalf("open error = %v", err)
		}
	})
	t.Run("embedded root semantics", func(t *testing.T) {
		corrupt := bytes.Clone(tabletImage)
		inner := corrupt[PageHeaderSize+GlobalTabletCatalogRootHeader:]
		inner[14] = SegmentedTabletRouterMaxPages + 1
		segmentedTabletRouterSeal(
			inner[:SegmentedTabletRouterRootBytes],
			segmentedTabletRouterRootTrailerAt,
		)
		if _, err := SealPage(corrupt); err != nil {
			t.Fatal(err)
		}
		if _, err := OpenGlobalTabletCatalogTabletRoot(
			corrupt, tabletRef, bounds,
		); !errors.Is(err, ErrGlobalTabletCatalogCorrupt) {
			t.Fatalf("open error = %v", err)
		}
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
		header.TabletID, header.Generation,
		[]GlobalTabletCatalogLocatorEntry{{
			LocalID: leaves[0].LocalID, State: GlobalTabletCatalogLocatorLive,
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	binary.LittleEndian.PutUint16(locatorImage[PageHeaderSize+8:], 2)
	if _, err := SealPage(locatorImage); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenGlobalTabletCatalogLocator(
		locatorImage, locatorRef, bounds,
	); !errors.Is(err, ErrGlobalTabletCatalogCorrupt) {
		t.Fatalf("locator open error = %v", err)
	}
}

func TestGlobalTabletCatalogNamespaceAndSpace(t *testing.T) {
	ranges := [][2]uint64{
		{GlobalTabletCatalogLeafLogicalIDBase, GlobalTabletCatalogLeafLogicalIDLimit},
		{GlobalTabletCatalogAnchorLogicalIDBase, GlobalTabletCatalogAnchorLogicalIDLimit},
		{GlobalTabletCatalogTabletRootLogicalIDBase, GlobalTabletCatalogTabletRootLogicalIDLimit},
		{GlobalTabletCatalogLocatorLogicalIDBase, GlobalTabletCatalogLocatorLogicalIDLimit},
		{GlobalTabletCatalogLeafPageLogicalIDBase, GlobalTabletCatalogLeafPageLogicalIDLimit},
		{GlobalTabletCatalogBranchPageLogicalIDBase, GlobalTabletCatalogBranchPageLogicalIDLimit},
		{GlobalTabletCatalogRootLogicalID, GlobalTabletCatalogRootLogicalID + 1},
		{PrimaryTabletRouteLogicalIDBase, PrimaryTabletRouteLogicalIDLimit},
	}
	for at := 1; at < len(ranges); at++ {
		if ranges[at-1][1] != ranges[at][0] {
			t.Fatalf("namespace gap/collision %d: %v then %v", at, ranges[at-1], ranges[at])
		}
	}
	if GlobalTabletCatalogFirstDynamicLogicalID != ranges[len(ranges)-1][1] {
		t.Fatal("dynamic namespace boundary")
	}
	view, _, _, entries := globalTabletCatalogTestNode(t)
	if _, err := view.RewriteHandle(
		make([]byte, len(view.image)), uint64(1)<<48,
		globalTabletCatalogTestBounds,
		entries[0].ID, entries[0].Ref,
	); err == nil {
		t.Fatal("accepted 48-bit generation overflow")
	}
	space, ok := GlobalTabletCatalogRoutingSpace(
		100_000_000_000, 195, 4096, 6<<20,
	)
	if !ok {
		t.Fatal("routing space")
	}
	if space.Tablets != 125_201 ||
		space.BytesPerDoc < 0.184 || space.BytesPerDoc > 0.185 {
		t.Fatalf("100B space = %+v", space)
	}
	t.Logf(
		"100B: tablets=%d tablet-routing=%0.3fGiB catalog=%0.3fMiB B/doc=%0.6f",
		space.Tablets, float64(space.TabletBytes)/(1<<30),
		float64(space.CatalogBytes)/(1<<20), space.BytesPerDoc,
	)
}

func TestGlobalTabletCatalogReferenceBounds(t *testing.T) {
	_, image, ref, entries := globalTabletCatalogTestNode(t)
	outOfFile := entries
	outOfFile[0].Ref.Offset = globalTabletCatalogTestBounds.FileEnd - 4096
	logicalID, _ := GlobalTabletCatalogCatalogLeafLogicalID(3)
	if _, err := EncodeGlobalTabletCatalogNode(
		make([]byte, GlobalTabletCatalogNodeBytes),
		GlobalTabletCatalogNodeHeader{
			StoreID:    globalTabletCatalogTestStoreID,
			Bounds:     globalTabletCatalogTestBounds,
			Generation: 100, LogicalID: logicalID, PageID: 3,
			Level: GlobalTabletCatalogLeaf,
			Kind:  PagePrimaryCatalog, ChildKind: PageTabletRoute,
			ChildLength: GlobalTabletCatalogTabletBytes,
		},
		outOfFile,
	); err == nil {
		t.Fatal("accepted child extent crossing FileEnd")
	}
	huge := entries
	huge[0].Ref.Offset = ^uint64(0) &^ 4095
	if _, err := EncodeGlobalTabletCatalogNode(
		make([]byte, GlobalTabletCatalogNodeBytes),
		GlobalTabletCatalogNodeHeader{
			StoreID:    globalTabletCatalogTestStoreID,
			Bounds:     globalTabletCatalogTestBounds,
			Generation: 100, LogicalID: logicalID, PageID: 3,
			Level: GlobalTabletCatalogLeaf,
			Kind:  PagePrimaryCatalog, ChildKind: PageTabletRoute,
			ChildLength: GlobalTabletCatalogTabletBytes,
		},
		huge,
	); err == nil {
		t.Fatal("accepted huge packed child offset")
	}
	badExpected := ref
	badExpected.Offset = globalTabletCatalogTestBounds.FileEnd
	if _, err := OpenGlobalTabletCatalogNode(
		image, badExpected, globalTabletCatalogTestBounds,
	); !errors.Is(err, ErrGlobalTabletCatalogCorrupt) {
		t.Fatalf("out-of-file node error = %v", err)
	}
	tooSmallNamespace := globalTabletCatalogTestBounds
	tooSmallNamespace.NextLogicalID = entries[0].Ref.LogicalID
	if _, err := OpenGlobalTabletCatalogNode(
		image, ref, tooSmallNamespace,
	); !errors.Is(err, ErrGlobalTabletCatalogCorrupt) {
		t.Fatalf("namespace-bound node error = %v", err)
	}
}

func FuzzGlobalTabletCatalogNodeAdmission(f *testing.F) {
	_, image, ref, _ := globalTabletCatalogTestNode(f)
	f.Add(uint16(0), byte(1))
	f.Add(uint16(len(image)-1), byte(0x80))
	f.Fuzz(func(t *testing.T, offset uint16, value byte) {
		corrupt := bytes.Clone(image)
		at := int(offset) % len(corrupt)
		corrupt[at] ^= value
		view, err := OpenGlobalTabletCatalogNode(
			corrupt, ref, globalTabletCatalogTestBounds,
		)
		if value == 0 {
			if err != nil || view.Count() != 3 {
				t.Fatalf("unchanged image: %v", err)
			}
			return
		}
		if err == nil {
			t.Fatalf("admitted mutation at %d xor %02x", at, value)
		}
	})
}

func ExampleGlobalTabletCatalogRoutingSpace() {
	space, _ := GlobalTabletCatalogRoutingSpace(
		100_000_000_000, 195, 4096, 6<<20,
	)
	fmt.Printf("%.4f bytes/document\n", space.BytesPerDoc)
	// Output:
	// 0.1847 bytes/document
}
