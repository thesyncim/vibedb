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

var tabletAnchorMapTestSeed = [16]byte{
	0x10, 0x32, 0x54, 0x76, 0x98, 0xba, 0xdc, 0xfe,
	0xef, 0xcd, 0xab, 0x89, 0x67, 0x45, 0x23, 0x01,
}

func tabletAnchorMapTestAnchors(count int) []TabletAnchorMapAnchor {
	anchors := make([]TabletAnchorMapAnchor, count)
	for rank := range anchors {
		anchors[rank] = TabletAnchorMapAnchor{
			Fence: []byte(fmt.Sprintf(
				"tenant/0042/document/%08d", (rank+1)*230,
			)),
			Bucket: BucketID(rank + 2),
		}
	}
	return anchors
}

func encodeTabletAnchorMapTest(
	t testing.TB, anchors []TabletAnchorMapAnchor,
) []byte {
	t.Helper()
	image, err := EncodeTabletAnchorMap(
		make([]byte, 1<<20),
		TabletAnchorMapHeader{TabletID: 42, Generation: 7},
		1,
		anchors,
	)
	if err != nil {
		t.Fatal(err)
	}
	return image
}

func openTabletAnchorMapTest(
	t testing.TB, anchors []TabletAnchorMapAnchor,
) TabletAnchorMapView {
	t.Helper()
	view, err := OpenTabletAnchorMap(
		encodeTabletAnchorMapTest(t, anchors),
	)
	if err != nil {
		t.Fatal(err)
	}
	return view
}

func tabletAnchorMapFenceBytes(
	t testing.TB, view TabletAnchorMapView, rank int,
) []byte {
	t.Helper()
	common, prefix, suffix, ok := view.FenceAt(rank)
	if !ok {
		t.Fatalf("FenceAt(%d) failed", rank)
	}
	fence := make([]byte, 0, len(common)+len(prefix)+len(suffix))
	fence = append(fence, common...)
	fence = append(fence, prefix...)
	return append(fence, suffix...)
}

func TestTabletAnchorMapRouteAndLexicalWalk(t *testing.T) {
	anchors := tabletAnchorMapTestAnchors(4300)
	view := openTabletAnchorMapTest(t, anchors)
	if view.FenceCount() != len(anchors) ||
		view.BucketCount() != len(anchors)+1 ||
		view.Header() !=
			(TabletAnchorMapHeader{TabletID: 42, Generation: 7}) {
		t.Fatalf(
			"header/counts = %+v/%d/%d",
			view.Header(), view.FenceCount(), view.BucketCount(),
		)
	}
	for rank, anchor := range anchors {
		if got := tabletAnchorMapFenceBytes(t, view, rank); !bytes.Equal(got, anchor.Fence) {
			t.Fatalf("fence %d = %q, want %q", rank, got, anchor.Fence)
		}
	}

	targets := []struct {
		key    string
		bucket BucketID
	}{
		{"", 1},
		{"tenant/0042/document/00000000", 1},
		{"tenant/0042/document/00000229", 1},
		{"tenant/0042/document/00000230", 2},
		{"tenant/0042/document/00000230\x00", 2},
		{"tenant/0042/document/00000459", 2},
		{"tenant/0042/document/99999999", 4301},
		{"tenant/0041", 1},
		{"tenant/0043", 4301},
	}
	for _, test := range targets {
		key := []byte(test.key)
		hash := KeyHashBytes(tabletAnchorMapTestSeed, key)
		reused := view.RouteHashed(hash, key)
		included := view.Route(tabletAnchorMapTestSeed, key)
		if reused.Bucket != test.bucket || included != reused ||
			reused.Hash != hash {
			t.Fatalf("route(%q) = %+v / %+v", key, reused, included)
		}
		cursor := view.LowerBound(key)
		if got, ok := cursor.Bucket(); !ok || got != test.bucket {
			t.Fatalf("LowerBound(%q) = %d,%v", key, got, ok)
		}
	}

	cursor := view.LowerBound(anchors[1023].Fence)
	for want := 1024; want < view.BucketCount(); want++ {
		got, ok := cursor.Bucket()
		if !ok || got != BucketID(want+1) {
			t.Fatalf("walk %d = %d,%v", want, got, ok)
		}
		if want+1 < view.BucketCount() && !cursor.Next() {
			t.Fatalf("walk stopped at %d", want)
		}
	}
	if cursor.Next() {
		t.Fatal("cursor advanced past final bucket")
	}
}

func TestTabletAnchorMapShortestSplitFence(t *testing.T) {
	tests := []struct {
		left, right string
	}{
		{"a", "aa"},
		{"aa", "abz"},
		{"ab\xff", "ac\x00"},
		{"tenant/0042/0099", "tenant/0042/0100"},
		{"\x00\xff", "\x01"},
	}
	for _, test := range tests {
		fence, err := ShortestTabletAnchorMapFence(
			make([]byte, len(test.right)),
			[]byte(test.left),
			[]byte(test.right),
		)
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Compare([]byte(test.left), fence) >= 0 ||
			bytes.Compare(fence, []byte(test.right)) > 0 {
			t.Fatalf(
				"fence(%q,%q) = %q", test.left, test.right, fence,
			)
		}
		for length := 1; length < len(fence); length++ {
			if bytes.Compare([]byte(test.left), fence[:length]) < 0 {
				t.Fatalf(
					"fence %q is not shortest; %q also separates",
					fence, fence[:length],
				)
			}
		}
	}
	if _, err := ShortestTabletAnchorMapFence(
		make([]byte, 1), []byte("b"), []byte("a"),
	); err == nil {
		t.Fatal("overlapping ranges accepted")
	}
}

func TestTabletAnchorMapBatchRewrite(t *testing.T) {
	anchors := []TabletAnchorMapAnchor{
		{Fence: []byte("b"), Bucket: 2},
		{Fence: []byte("d"), Bucket: 3},
		{Fence: []byte("f"), Bucket: 4},
	}
	view := openTabletAnchorMapTest(t, anchors)
	edits := []TabletAnchorMapEdit{
		{Operation: TabletAnchorMapInsert, Fence: []byte("c"), Bucket: 5},
		{Operation: TabletAnchorMapDelete, Fence: []byte("d")},
		{Operation: TabletAnchorMapReplace, Fence: []byte("f"), Bucket: 6},
	}
	image, err := view.ApplyBatch(
		make([]byte, 4096), make([]byte, 128), 8, edits,
	)
	if err != nil {
		t.Fatal(err)
	}
	after, err := OpenTabletAnchorMap(image)
	if err != nil {
		t.Fatal(err)
	}
	if after.Header().Generation != 8 || after.FenceCount() != 3 {
		t.Fatalf("after header/count = %+v/%d", after.Header(), after.FenceCount())
	}
	wantFences := []string{"b", "c", "f"}
	wantBuckets := []BucketID{1, 2, 5, 6}
	for rank, want := range wantFences {
		if got := string(tabletAnchorMapFenceBytes(t, after, rank)); got != want {
			t.Fatalf("fence %d = %q, want %q", rank, got, want)
		}
	}
	cursor := after.LowerBound(nil)
	for rank, want := range wantBuckets {
		got, ok := cursor.Bucket()
		if !ok || got != want {
			t.Fatalf("bucket %d = %d,%v, want %d", rank, got, ok, want)
		}
		if rank+1 < len(wantBuckets) {
			cursor.Next()
		}
	}
	if got := after.RouteHashed(99, []byte("d")); got.Bucket != 5 {
		t.Fatalf("route(d) = %+v", got)
	}
	// The immutable source remains admitted and unchanged.
	if got := view.RouteHashed(99, []byte("d")); got.Bucket != 3 {
		t.Fatalf("source route(d) changed = %+v", got)
	}
}

func TestTabletAnchorMapBatchRejectsInvalidEdits(t *testing.T) {
	view := openTabletAnchorMapTest(
		t,
		[]TabletAnchorMapAnchor{
			{Fence: []byte("b"), Bucket: 2},
			{Fence: []byte("d"), Bucket: 3},
		},
	)
	tests := []struct {
		name  string
		gen   uint64
		edit  TabletAnchorMapEdit
		short bool
	}{
		{
			name: "insert-existing", gen: 8,
			edit: TabletAnchorMapEdit{
				Operation: TabletAnchorMapInsert,
				Fence:     []byte("b"), Bucket: 7,
			},
		},
		{
			name: "delete-absent", gen: 8,
			edit: TabletAnchorMapEdit{
				Operation: TabletAnchorMapDelete, Fence: []byte("c"),
			},
		},
		{
			name: "replace-absent", gen: 8,
			edit: TabletAnchorMapEdit{
				Operation: TabletAnchorMapReplace,
				Fence:     []byte("c"), Bucket: 7,
			},
		},
		{
			name: "stale-generation", gen: 7,
			edit: TabletAnchorMapEdit{
				Operation: TabletAnchorMapInsert,
				Fence:     []byte("c"), Bucket: 7,
			},
		},
		{
			name: "scratch", gen: 8, short: true,
			edit: TabletAnchorMapEdit{
				Operation: TabletAnchorMapInsert,
				Fence:     []byte("c"), Bucket: 7,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scratch := make([]byte, 32)
			if test.short {
				scratch = scratch[:1]
			}
			_, err := view.ApplyBatch(
				make([]byte, 4096), scratch, test.gen,
				[]TabletAnchorMapEdit{test.edit},
			)
			if err == nil {
				t.Fatal("invalid edit accepted")
			}
			if test.short &&
				!errors.Is(err, ErrTabletAnchorMapScratch) {
				t.Fatalf("scratch error = %v", err)
			}
		})
	}
}

func TestTabletAnchorMapSingleBucketAndLongSharedPrefix(t *testing.T) {
	single := openTabletAnchorMapTest(t, nil)
	for _, key := range [][]byte{nil, []byte("a"), {0xff}} {
		if got := single.RouteHashed(1, key); got.Bucket != 1 ||
			got.Ordinal != 0 {
			t.Fatalf("single route(%x) = %+v", key, got)
		}
	}
	anchors := []TabletAnchorMapAnchor{
		{
			Fence:  append(bytes.Repeat([]byte("x"), 300), 'a'),
			Bucket: 2,
		},
		{
			Fence:  append(bytes.Repeat([]byte("x"), 300), 'z'),
			Bucket: 3,
		},
	}
	view := openTabletAnchorMapTest(t, anchors)
	if got := view.RouteHashed(
		0, append(bytes.Repeat([]byte("x"), 300), 'm'),
	); got.Bucket != 2 {
		t.Fatalf("long shared-prefix route = %+v", got)
	}
}

func TestTabletAnchorMapReadPathsAllocateNothing(t *testing.T) {
	view := openTabletAnchorMapTest(
		t, tabletAnchorMapTestAnchors(4300),
	)
	key := []byte("tenant/0042/document/00054321")
	hash := KeyHashBytes(tabletAnchorMapTestSeed, key)
	if got := testing.AllocsPerRun(1000, func() {
		_ = view.RouteHashed(hash, key)
	}); got != 0 {
		t.Fatalf("RouteHashed allocations = %v", got)
	}
	if got := testing.AllocsPerRun(1000, func() {
		_ = view.Route(tabletAnchorMapTestSeed, key)
	}); got != 0 {
		t.Fatalf("Route allocations = %v", got)
	}
	if got := testing.AllocsPerRun(1000, func() {
		cursor := view.LowerBound(key)
		_, _ = cursor.Bucket()
		_ = cursor.Next()
	}); got != 0 {
		t.Fatalf("LowerBound/walk allocations = %v", got)
	}
}

func TestTabletAnchorMapRejectsCorruption(t *testing.T) {
	original := encodeTabletAnchorMapTest(
		t, tabletAnchorMapTestAnchors(64),
	)
	type corruption struct {
		name string
		edit func([]byte)
		seal bool
	}
	corruptions := []corruption{
		{
			name: "checksum",
			edit: func(image []byte) { image[100] ^= 1 },
		},
		{
			name: "first-offset",
			edit: func(image []byte) {
				common := int(binary.LittleEndian.Uint16(image[34:36]))
				at := TabletAnchorMapHeaderSize +
					tabletAnchorMapAcceleratorBytes + common
				binary.LittleEndian.PutUint16(image[at:], 1)
			},
			seal: true,
		},
		{
			name: "accelerator",
			edit: func(image []byte) {
				binary.LittleEndian.PutUint16(
					image[TabletAnchorMapHeaderSize:], 9,
				)
			},
			seal: true,
		},
		{
			name: "bucket-overflow",
			edit: func(image []byte) {
				count := int(binary.LittleEndian.Uint16(image[32:34]))
				common := int(binary.LittleEndian.Uint16(image[34:36]))
				at := TabletAnchorMapHeaderSize +
					tabletAnchorMapAcceleratorBytes + common +
					(count+1)*2
				binary.LittleEndian.PutUint32(image[at:], PrimaryBucketIDLimit)
			},
			seal: true,
		},
		{
			name: "restart-sharing",
			edit: func(image []byte) {
				count := int(binary.LittleEndian.Uint16(image[32:34]))
				common := int(binary.LittleEndian.Uint16(image[34:36]))
				keyAt := TabletAnchorMapHeaderSize +
					tabletAnchorMapAcceleratorBytes + common +
					(count+1)*2 + (count+1)*4
				binary.LittleEndian.PutUint16(image[keyAt:], 1)
			},
			seal: true,
		},
		{
			name: "maximum-fence",
			edit: func(image []byte) {
				binary.LittleEndian.PutUint16(image[36:38], 1)
			},
			seal: true,
		},
	}
	for _, corruption := range corruptions {
		t.Run(corruption.name, func(t *testing.T) {
			image := append([]byte(nil), original...)
			corruption.edit(image)
			if corruption.seal {
				tabletAnchorMapSeal(image)
			}
			if _, err := OpenTabletAnchorMap(image); !errors.Is(err, ErrTabletAnchorMapCorrupt) {
				t.Fatalf("Open error = %v", err)
			}
		})
	}
}

func TestTabletAnchorMapRandomizedRouting(t *testing.T) {
	random := rand.New(rand.NewSource(91))
	for iteration := range 50 {
		count := 1 + random.Intn(1000)
		values := make([]uint32, count)
		for rank := range values {
			values[rank] = random.Uint32()
		}
		sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
		anchors := make([]TabletAnchorMapAnchor, 0, count)
		for _, value := range values {
			fence := []byte(fmt.Sprintf("prefix/%08x", value))
			if len(anchors) != 0 &&
				bytes.Equal(anchors[len(anchors)-1].Fence, fence) {
				continue
			}
			anchors = append(anchors, TabletAnchorMapAnchor{
				Fence: fence, Bucket: BucketID(len(anchors) + 2),
			})
		}
		view := openTabletAnchorMapTest(t, anchors)
		for range 1000 {
			key := []byte(fmt.Sprintf("prefix/%08x", random.Uint32()))
			want := sort.Search(len(anchors), func(rank int) bool {
				return bytes.Compare(anchors[rank].Fence, key) > 0
			})
			got := view.RouteHashed(0, key)
			if int(got.Ordinal) != want ||
				got.Bucket != BucketID(want+1) {
				t.Fatalf(
					"iteration %d route(%q) = %+v, want ordinal %d",
					iteration, key, got, want,
				)
			}
		}
	}
}

func tabletAnchorHandleTest(
	t testing.TB, localBits uint8, leafCount int,
) (TabletAnchorHandleView, []TabletAnchorHandleLeaf) {
	t.Helper()
	const tabletID = uint32(42)
	first, ok := ComposeTabletAnchorBucketID(tabletID, 0, localBits)
	if !ok {
		t.Fatal("compose first bucket")
	}
	anchors := make([]TabletAnchorMapAnchor, leafCount-1)
	leaves := make([]TabletAnchorHandleLeaf, leafCount)
	for ordinal := range leafCount {
		bucket, ok := ComposeTabletAnchorBucketID(
			tabletID, uint16(ordinal), localBits,
		)
		if !ok {
			t.Fatalf("compose bucket %d", ordinal)
		}
		if ordinal != 0 {
			anchors[ordinal-1] = TabletAnchorMapAnchor{
				Fence: []byte(fmt.Sprintf(
					"tenant/0042/document/%08d", ordinal*230,
				)),
				Bucket: bucket,
			}
		}
		var zone BucketZone
		binary.LittleEndian.PutUint32(zone[:], uint32(ordinal)^0xa5a5a5a5)
		leaves[ordinal] = TabletAnchorHandleLeaf{
			Bucket: bucket,
			Ref: PageRef{
				Offset:     uint64(ordinal+1) * 4096,
				LogicalID:  uint64(bucket) + 1,
				Generation: uint64(100 + ordinal),
				Length:     uint32(4096 << (ordinal % 5)),
				Kind:       PagePrimaryLeaf,
			},
			Zone: zone,
		}
	}
	anchorImage, err := EncodeTabletAnchorMap(
		make([]byte, 1<<20),
		TabletAnchorMapHeader{
			TabletID: uint64(tabletID), Generation: 77,
		},
		first,
		anchors,
	)
	if err != nil {
		t.Fatal(err)
	}
	anchorView, err := OpenTabletAnchorMap(anchorImage)
	if err != nil {
		t.Fatal(err)
	}
	handleImage, err := EncodeTabletAnchorHandles(
		make([]byte, 1<<20),
		anchorView,
		localBits,
		PagePrimaryLeaf,
		leaves,
	)
	if err != nil {
		t.Fatal(err)
	}
	view, err := OpenTabletAnchorHandles(handleImage, anchorView)
	if err != nil {
		t.Fatal(err)
	}
	return view, leaves
}

func TestTabletAnchorHandleRouteAndResolve(t *testing.T) {
	for _, geometry := range []struct {
		bits, count int
	}{
		{bits: 12, count: 3072},
		{bits: 13, count: 4300},
	} {
		t.Run(fmt.Sprintf("%d-local-bits", geometry.bits), func(t *testing.T) {
			view, leaves := tabletAnchorHandleTest(
				t, uint8(geometry.bits), geometry.count,
			)
			targetOrdinal := geometry.count / 2
			key := []byte(fmt.Sprintf(
				"tenant/0042/document/%08d", targetOrdinal*230,
			))
			hash := KeyHashBytes(tabletAnchorMapTestSeed, key)
			route := view.RouteHashed(hash, key)
			want := leaves[targetOrdinal]
			if route.Bucket != want.Bucket || route.Ref != want.Ref ||
				route.Zone != want.Zone || route.Hash != hash ||
				int(route.Ordinal) != targetOrdinal {
				t.Fatalf("route = %+v, want %+v", route, want)
			}
			included := view.Route(tabletAnchorMapTestSeed, key)
			if included != route {
				t.Fatalf("included route = %+v, want %+v", included, route)
			}
			for _, ordinal := range []int{
				0, 1, targetOrdinal, geometry.count - 1,
			} {
				gotRef, gotZone, ok := view.ResolveBucketID(
					leaves[ordinal].Bucket,
				)
				if !ok || gotRef != leaves[ordinal].Ref ||
					gotZone != leaves[ordinal].Zone {
					t.Fatalf(
						"resolve %d = %+v/%x/%v",
						ordinal, gotRef, gotZone, ok,
					)
				}
			}
			foreign, _ := ComposeTabletAnchorBucketID(
				43, 1, uint8(geometry.bits),
			)
			if _, _, ok := view.ResolveBucketID(foreign); ok {
				t.Fatal("foreign tablet resolved")
			}
			missing, _ := ComposeTabletAnchorBucketID(
				42, uint16(geometry.count), uint8(geometry.bits),
			)
			if _, _, ok := view.ResolveBucketID(missing); ok {
				t.Fatal("missing local leaf resolved")
			}
			if bytesPerAnchor := view.CombinedBytesPerAnchor(); bytesPerAnchor <= TabletAnchorHandleHandleSize {
				t.Fatalf(
					"combined bytes/anchor = %.3f", bytesPerAnchor,
				)
			}
		})
	}
}

func TestTabletAnchorHandleReadPathsAllocateNothing(t *testing.T) {
	view, leaves := tabletAnchorHandleTest(t, 12, 3072)
	key := []byte("tenant/0042/document/00353280")
	hash := KeyHashBytes(tabletAnchorMapTestSeed, key)
	if got := testing.AllocsPerRun(1000, func() {
		_ = view.RouteHashed(hash, key)
	}); got != 0 {
		t.Fatalf("combined RouteHashed allocations = %v", got)
	}
	if got := testing.AllocsPerRun(1000, func() {
		_, _, _ = view.ResolveBucketID(leaves[1536].Bucket)
	}); got != 0 {
		t.Fatalf("ResolveBucketID allocations = %v", got)
	}
}

func TestTabletAnchorHandleRejectsCorruptionAndWrongBinding(t *testing.T) {
	view, _ := tabletAnchorHandleTest(t, 12, 256)
	original := append([]byte(nil), view.image...)
	corruptions := []struct {
		name string
		edit func([]byte)
		seal bool
	}{
		{
			name: "checksum",
			edit: func(image []byte) { image[100] ^= 1 },
		},
		{
			name: "locator",
			edit: func(image []byte) {
				binary.LittleEndian.PutUint16(
					image[TabletAnchorHandleHeaderSize:], 500,
				)
			},
			seal: true,
		},
		{
			name: "extent-class",
			edit: func(image []byte) {
				at := TabletAnchorHandleHeaderSize + (1<<12)*2
				image[at+12] = 7
			},
			seal: true,
		},
		{
			name: "anchor-binding",
			edit: func(image []byte) { image[36] ^= 1 },
			seal: true,
		},
	}
	for _, corruption := range corruptions {
		t.Run(corruption.name, func(t *testing.T) {
			image := append([]byte(nil), original...)
			corruption.edit(image)
			if corruption.seal {
				tabletAnchorHandleSeal(image)
			}
			if _, err := OpenTabletAnchorHandles(
				image, view.anchors,
			); !errors.Is(err, ErrTabletAnchorHandleCorrupt) {
				t.Fatalf("Open error = %v", err)
			}
		})
	}
}

func FuzzOpenTabletAnchorMap(f *testing.F) {
	image, err := EncodeTabletAnchorMap(
		make([]byte, 4096),
		TabletAnchorMapHeader{TabletID: 1, Generation: 1},
		1,
		tabletAnchorMapTestAnchors(32),
	)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(append([]byte(nil), image...))
	f.Fuzz(func(t *testing.T, candidate []byte) {
		_, _ = OpenTabletAnchorMap(candidate)
	})
}

func FuzzOpenTabletAnchorHandle(f *testing.F) {
	view, _ := tabletAnchorHandleTest(f, 12, 32)
	anchorView := view.anchors
	f.Add(append([]byte(nil), view.image...))
	f.Fuzz(func(t *testing.T, candidate []byte) {
		_, _ = OpenTabletAnchorHandles(candidate, anchorView)
	})
}
