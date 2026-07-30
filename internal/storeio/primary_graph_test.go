package storeio

import (
	"bytes"
	"fmt"
	"os"
	"testing"
)

type primaryGraphTestImage struct {
	file    *os.File
	root    StateRoot
	bounds  GlobalTabletCatalogBounds
	pages   map[PageRef][]byte
	nodes   map[PageRef]*GlobalTabletCatalogNodeView
	tablets map[PageRef]*GlobalTabletCatalogTabletRootView
	anchors map[PageRef]*GlobalTabletCatalogAnchorView
	leaves  map[PageRef]*CommonPrimaryUnifiedLeafView
}

func buildPrimaryGraphTestImage(
	t testing.TB, count int,
) (*primaryGraphTestImage, []PrimaryGraphRecord) {
	t.Helper()
	records := make([]PrimaryGraphRecord, count)
	for at := range records {
		records[at] = PrimaryGraphRecord{
			Key:   []byte(fmt.Sprintf("key-%08d", at)),
			Value: []byte(fmt.Sprintf(`{"rank":%d}`, at)),
		}
	}
	file, err := os.CreateTemp(t.TempDir(), "primary-graph-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })
	const maxPages = 600
	committer, err := NewCommitter(file, DeviceOptions{
		Backend: BackendPortable, BufferCount: 1024,
		BufferSize: GlobalTabletCatalogRootBytes,
	}, CommitterOptions{
		QueueSlots: 2, MaxPagesPerBatch: maxPages, GroupLimit: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = committer.Close() })
	layout, err := MutableStoreLayout(format0PageSize)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := BeginWriteTransaction(
		committer, nil, maxPages,
		WriteTransactionOptions{
			StoreID: format0StoreID, Generation: 1, PageSize: format0PageSize,
			FileEnd:       layout.DataStart,
			NextLogicalID: PrimaryFirstDynamicLogicalID,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	primaryRoot, err := BuildPrimaryGraph(tx, records)
	if err != nil {
		_ = tx.Abort()
		t.Fatal(err)
	}
	root := StateRoot{
		StoreID: format0StoreID, Generation: 1, PageSize: format0PageSize,
		MaxPageSize:   GlobalTabletCatalogRootBytes,
		NextLogicalID: tx.NextLogicalID(), ChunkDocuments: 64,
		PrimaryRoot: primaryRoot,
	}
	if err := tx.PublishInline(root, InlineFreeDelta{}); err != nil {
		t.Fatal(err)
	}
	if err := committer.Wait(root.Generation); err != nil {
		t.Fatal(err)
	}
	return &primaryGraphTestImage{
		file: file, root: root,
		bounds: GlobalTabletCatalogBounds{
			StoreID: format0StoreID, SelectedRootGeneration: root.Generation,
			FileEnd: tx.FileEnd(), NextLogicalID: root.NextLogicalID,
		},
		pages:   make(map[PageRef][]byte),
		nodes:   make(map[PageRef]*GlobalTabletCatalogNodeView),
		tablets: make(map[PageRef]*GlobalTabletCatalogTabletRootView),
		anchors: make(map[PageRef]*GlobalTabletCatalogAnchorView),
		leaves:  make(map[PageRef]*CommonPrimaryUnifiedLeafView),
	}, records
}

func (image *primaryGraphTestImage) page(
	t testing.TB, ref PageRef,
) []byte {
	t.Helper()
	if page := image.pages[ref]; page != nil {
		return page
	}
	page := make([]byte, ref.Length)
	if _, err := image.file.ReadAt(page, int64(ref.Offset)); err != nil {
		t.Fatal(err)
	}
	image.pages[ref] = page
	return page
}

func (image *primaryGraphTestImage) node(
	t testing.TB, ref PageRef,
) *GlobalTabletCatalogNodeView {
	t.Helper()
	if view := image.nodes[ref]; view != nil {
		return view
	}
	view, err := OpenGlobalTabletCatalogNode(
		image.page(t, ref), ref, image.bounds,
	)
	if err != nil {
		t.Fatal(err)
	}
	image.nodes[ref] = &view
	return &view
}

func (image *primaryGraphTestImage) tablet(
	t testing.TB, ref PageRef,
) *GlobalTabletCatalogTabletRootView {
	t.Helper()
	if view := image.tablets[ref]; view != nil {
		return view
	}
	view, err := OpenGlobalTabletCatalogTabletRoot(
		image.page(t, ref), ref, image.bounds,
	)
	if err != nil {
		t.Fatal(err)
	}
	image.tablets[ref] = &view
	return &view
}

func (image *primaryGraphTestImage) anchor(
	t testing.TB,
	tablet *GlobalTabletCatalogTabletRootView,
	route GlobalTabletCatalogAnchorRoute,
) *GlobalTabletCatalogAnchorView {
	t.Helper()
	if view := image.anchors[route.Ref]; view != nil {
		return view
	}
	view, err := OpenGlobalTabletCatalogAnchor(
		image.page(t, route.Ref), tablet, route.PageID,
	)
	if err != nil {
		t.Fatal(err)
	}
	image.anchors[route.Ref] = &view
	return &view
}

func (image *primaryGraphTestImage) leaf(
	t testing.TB, route SegmentedTabletRouterRoute,
) *CommonPrimaryUnifiedLeafView {
	t.Helper()
	if view := image.leaves[route.Ref]; view != nil {
		return view
	}
	view, err := OpenCommonPrimaryUnifiedLeaf(
		image.page(t, route.Ref), image.root.StoreID,
		route.Bucket, route.Ref, image.root.Generation,
		CommonPrimaryLeafBounds{
			FileEnd: image.bounds.FileEnd, NextLogicalID: image.root.NextLogicalID,
			AllocationQuantum: image.root.PageSize,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	image.leaves[route.Ref] = &view
	return &view
}

func (image *primaryGraphTestImage) catalogLeaf(
	t testing.TB, key []byte,
) *GlobalTabletCatalogNodeView {
	t.Helper()
	root := image.node(t, image.root.PrimaryRoot)
	next := root.Route(key)
	child := image.node(t, next.Ref)
	if root.ChildLevel() == GlobalTabletCatalogBranch {
		next = child.Route(key)
		child = image.node(t, next.Ref)
	}
	if child.Level() != GlobalTabletCatalogLeaf {
		t.Fatalf("catalog terminal level = %d", child.Level())
	}
	return child
}

func (image *primaryGraphTestImage) lookup(
	t testing.TB, key []byte,
) ([]byte, bool) {
	t.Helper()
	catalog := image.catalogLeaf(t, key)
	tabletRoute := catalog.Route(key)
	tablet := image.tablet(t, tabletRoute.Ref)
	anchorRoute, ok := tablet.RouteAnchor(key)
	if !ok {
		t.Fatal("tablet anchor route")
	}
	anchor := image.anchor(t, tablet, anchorRoute)
	hash := KeyHashBytes(image.root.StoreID, key)
	leafRoute, ok := anchor.RouteHashed(hash, key)
	if !ok {
		t.Fatal("anchor leaf route")
	}
	leaf := image.leaf(t, leafRoute)
	body, overflow, found := leaf.LookupBodyHashed(leafRoute.Hash, key)
	if !found || overflow {
		return nil, false
	}
	value, ok := leaf.AppendRowBody(nil, body)
	return value, ok
}

func (image *primaryGraphTestImage) tabletRefs(
	t testing.TB,
) []PageRef {
	t.Helper()
	root, err := OpenGlobalTabletCatalogNode(
		image.page(t, image.root.PrimaryRoot),
		image.root.PrimaryRoot, image.bounds,
	)
	if err != nil {
		t.Fatal(err)
	}
	var leaves []*GlobalTabletCatalogNodeView
	rootCursor := root.LowerBound(nil)
	for {
		route, ok := rootCursor.Route()
		if !ok {
			break
		}
		node := image.node(t, route.Ref)
		if root.ChildLevel() == GlobalTabletCatalogBranch {
			branchCursor := node.LowerBound(nil)
			for {
				leafRoute, childOK := branchCursor.Route()
				if !childOK {
					break
				}
				leaves = append(leaves, image.node(t, leafRoute.Ref))
				if !branchCursor.Next() {
					break
				}
			}
		} else {
			leaves = append(leaves, node)
		}
		if !rootCursor.Next() {
			break
		}
	}
	var refs []PageRef
	for at := range leaves {
		cursor := leaves[at].LowerBound(nil)
		for {
			route, ok := cursor.Route()
			if !ok {
				break
			}
			refs = append(refs, route.Ref)
			if !cursor.Next() {
				break
			}
		}
	}
	return refs
}

func TestBuildPrimaryGraphRoundTrip(t *testing.T) {
	for _, count := range []int{1, 1_000, 100_000} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			image, records := buildPrimaryGraphTestImage(t, count)
			for at := range records {
				value, ok := image.lookup(t, records[at].Key)
				if !ok || !bytes.Equal(value, records[at].Value) {
					t.Fatalf("lookup %d = %q,%v", at, value, ok)
				}
			}
			for _, key := range [][]byte{
				[]byte(""),
				[]byte("key-00000000\x00"),
				[]byte(fmt.Sprintf("key-%08d-missing", count/2)),
				[]byte("zzzz"),
			} {
				if value, ok := image.lookup(t, key); ok {
					t.Fatalf("absent lookup %q = %q", key, value)
				}
			}

			at := 0
			leafBounds := CommonPrimaryLeafBounds{
				FileEnd:           image.bounds.FileEnd,
				NextLogicalID:     image.root.NextLogicalID,
				AllocationQuantum: image.root.PageSize,
			}
			for _, tabletRef := range image.tabletRefs(t) {
				tablet, err := OpenGlobalTabletCatalogTabletRoot(
					image.page(t, tabletRef), tabletRef, image.bounds,
				)
				if err != nil {
					t.Fatal(err)
				}
				for anchorRank := 0; anchorRank < tablet.AnchorCount(); anchorRank++ {
					anchorRoute, ok := tablet.AnchorAt(anchorRank)
					if !ok {
						t.Fatal("anchor iteration route")
					}
					anchor, err := OpenGlobalTabletCatalogAnchor(
						image.page(t, anchorRoute.Ref),
						&tablet, anchorRoute.PageID,
					)
					if err != nil {
						t.Fatal(err)
					}
					for leafRank := 0; leafRank < anchor.Count(); leafRank++ {
						route, ok := anchor.RouteAt(leafRank, 0)
						if !ok {
							t.Fatal("leaf iteration route")
						}
						leaf, err := OpenCommonPrimaryUnifiedLeaf(
							image.page(t, route.Ref), image.root.StoreID,
							route.Bucket, route.Ref, image.root.Generation,
							leafBounds,
						)
						if err != nil {
							t.Fatal(err)
						}
						for rank := 0; rank < leaf.Len(); rank++ {
							key, body, overflow, ok := leaf.RowRawAt(rank)
							value, rendered := leaf.AppendRowBody(nil, body)
							if !ok || overflow || !rendered {
								t.Fatal("unified leaf iteration")
							}
							if at >= len(records) ||
								!bytes.Equal(key, records[at].Key) ||
								!bytes.Equal(value, records[at].Value) {
								t.Fatalf("iteration row %d = %q/%q", at, key, value)
							}
							at++
						}
					}
				}
			}
			if at != len(records) {
				t.Fatalf("iteration count = %d, want %d", at, len(records))
			}
		})
	}
}

func TestBuildPrimaryGraphDeterministic(t *testing.T) {
	first, records := buildPrimaryGraphTestImage(t, 1_000)
	second, _ := buildPrimaryGraphTestImage(t, 1_000)
	if first.bounds.FileEnd != second.bounds.FileEnd {
		t.Fatalf("file ends = %d/%d", first.bounds.FileEnd, second.bounds.FileEnd)
	}
	left := make([]byte, first.bounds.FileEnd)
	right := make([]byte, second.bounds.FileEnd)
	if _, err := first.file.ReadAt(left, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := second.file.ReadAt(right, 0); err != nil {
		t.Fatal(err)
	}
	if len(records) == 0 || !bytes.Equal(left, right) {
		t.Fatal("same primary input produced different durable bytes")
	}
}
