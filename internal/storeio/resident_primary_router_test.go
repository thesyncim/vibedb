package storeio

import (
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
)

var residentPrimaryRouterSplitBenchmarkSink *ResidentPrimaryRouter

func residentPrimaryRouterGenerationTestFixture(
	t testing.TB,
) *ResidentPrimaryRouter {
	t.Helper()
	fences := []byte("mt")
	rows := make([]uint64, 3*residentPrimaryRouterWords)
	for rank := range 3 {
		start, end := uint32(0), uint32(0)
		switch rank {
		case 1:
			end = 1
		case 2:
			start, end = 1, 2
		}
		bucket := BucketID(100 + rank)
		logicalID, ok := CommonPrimaryLeafLogicalID(bucket)
		if !ok {
			t.Fatalf("leaf logical ID for bucket %d", bucket)
		}
		at := rank * residentPrimaryRouterWords
		rows[at] = uint64(start) | uint64(end)<<32
		rows[at+1] = uint64(4096 * (rank + 1))
		rows[at+2] = uint64(90 + rank)
		rows[at+3] = uint64(4096) | uint64(uint32(bucket))<<32
		if logicalID != CommonPrimaryLeafLeafLogicalIDBase+uint64(bucket) {
			t.Fatalf("leaf logical ID = %d", logicalID)
		}
	}
	router := &ResidentPrimaryRouter{
		storeID: [16]byte{0x72, 0x6f, 0x75, 0x74, 0x65, 0x72},
		fences:  fences,
		rows:    rows,
		hints:   make([]pageCacheFrameHint, 3),
		empty:   make([]atomic.Uint32, 3),
	}
	router.buildSearchKeys()
	router.generation.Store(100)
	router.version.Store(24)
	return router
}

func TestResidentPrimaryRouterAdvanceGenerationLeavesRoutesUnchanged(
	t *testing.T,
) {
	router := residentPrimaryRouterGenerationTestFixture(t)
	keys := [][]byte{[]byte("alpha"), []byte("mango"), []byte("zulu")}
	beforeRoutes := make([]ResidentPrimaryRoute, len(keys))
	beforeRanks := make([]ResidentPrimaryRoute, router.Len())
	for i, key := range keys {
		var ok bool
		beforeRoutes[i], ok = router.Route(key)
		if !ok {
			t.Fatalf("route %q missing", key)
		}
	}
	for rank := range router.Len() {
		var ok bool
		beforeRanks[rank], ok = router.RouteAtRank(rank)
		if !ok {
			t.Fatalf("route rank %d missing", rank)
		}
	}
	beforeRows := slices.Clone(router.rows)
	beforeVersion := router.version.Load()

	router.AdvanceGeneration(101)

	if got := router.Generation(); got != 101 {
		t.Fatalf("generation = %d, want 101", got)
	}
	if got := router.version.Load(); got != beforeVersion {
		t.Fatalf("version = %d, want unchanged %d", got, beforeVersion)
	}
	if !slices.Equal(router.rows, beforeRows) {
		t.Fatal("generation-only advance changed packed route rows")
	}
	for i, key := range keys {
		got, ok := router.Route(key)
		if !ok || got != beforeRoutes[i] {
			t.Fatalf("route %q = %+v,%v, want unchanged %+v",
				key, got, ok, beforeRoutes[i])
		}
	}
	for rank := range router.Len() {
		got, ok := router.RouteAtRank(rank)
		if !ok || got != beforeRanks[rank] {
			t.Fatalf("route rank %d = %+v,%v, want unchanged %+v",
				rank, got, ok, beforeRanks[rank])
		}
	}
}

func TestResidentPrimaryRouterNextTabletIDFromAuthenticatedRoutes(t *testing.T) {
	router := residentPrimaryRouterGenerationTestFixture(t)
	for rank, tabletID := range []uint32{2, 0, 1} {
		bucket, ok := MakeTabletLocalIdentityBucket(tabletID, uint32(100+rank))
		if !ok {
			t.Fatal("make routed tablet bucket")
		}
		at := rank * residentPrimaryRouterWords
		router.rows[at+3] = uint64(uint32(router.rows[at+3])) |
			uint64(bucket)<<32
	}
	if got, ok := router.NextTabletID(); !ok || got != 3 {
		t.Fatalf("next tablet ID = %d,%v, want 3,true", got, ok)
	}
	last, _ := MakeTabletLocalIdentityBucket(
		TabletLocalIdentityTabletCount-1, 7,
	)
	router.rows[3] = uint64(uint32(router.rows[3])) | uint64(last)<<32
	if got, ok := router.NextTabletID(); ok || got != 0 {
		t.Fatalf("exhausted next tablet ID = %d,%v, want 0,false", got, ok)
	}
}

func TestResidentPrimaryRouterSplitLeafSplicesWithoutGraphWalk(t *testing.T) {
	router := residentPrimaryRouterGenerationTestFixture(t)
	route, ok := router.Route([]byte("mango"))
	if !ok || route.Bucket != 101 {
		t.Fatalf("source route = %+v,%v", route, ok)
	}
	left := route.Ref
	left.Offset += 64 << 10
	left.Generation = 101
	rightBucket := BucketID(409)
	rightLogical, ok := CommonPrimaryLeafLogicalID(rightBucket)
	if !ok {
		t.Fatal("right logical ID")
	}
	right := PageRef{Offset: left.Offset + 4096, LogicalID: rightLogical,
		Generation: 101, Length: 4096, Kind: PagePrimaryLeaf}
	next, err := router.SplitLeaf(
		route, left, rightBucket, []byte("s"), right, 101,
	)
	if err != nil {
		t.Fatal(err)
	}
	if next.Len() != router.Len()+1 || next.Generation() != 101 ||
		router.Len() != 3 || router.Generation() != 100 {
		t.Fatalf("router cardinality/generation = %d/%d old=%d/%d",
			next.Len(), next.Generation(), router.Len(), router.Generation())
	}
	tests := []struct {
		key    string
		bucket BucketID
		ref    PageRef
	}{
		{"alpha", 100, mustResidentRoute(t, router, 0).Ref},
		{"mango", 101, left},
		{"sun", rightBucket, right},
		{"zulu", 102, mustResidentRoute(t, router, 2).Ref},
	}
	for _, test := range tests {
		got, routeOK := next.Route([]byte(test.key))
		if !routeOK || got.Bucket != test.bucket || got.Ref != test.ref {
			t.Fatalf("route %q = %+v,%v", test.key, got, routeOK)
		}
	}
	if testing.AllocsPerRun(100, func() {
		spliced, splitErr := router.SplitLeaf(
			route, left, rightBucket, []byte("s"), right, 101,
		)
		if splitErr != nil || spliced.Len() != 4 {
			panic("split splice")
		}
	}) > 8 {
		t.Fatal("split splice exceeded fixed array allocation count")
	}
}

func TestResidentPrimaryRouterSplitLeafPartitionSplicesKWayRoutes(t *testing.T) {
	router := residentPrimaryRouterGenerationTestFixture(t)
	for _, test := range []struct {
		name   string
		source int
		fences []string
	}{
		{name: "first", source: 0, fences: []string{"", "a", "b", "c", "d", "e"}},
		{name: "middle", source: 1, fences: []string{"m", "n", "o", "p", "q", "r"}},
		{name: "last", source: 2, fences: []string{"t", "u", "v", "w", "x", "y"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			source, ok := router.RouteAtRank(test.source)
			if !ok {
				t.Fatalf("source rank %d", test.source)
			}
			const generation = uint64(101)
			replacements := make([]SegmentedTabletRouterLeaf, len(test.fences))
			for rank, rawFence := range test.fences {
				localID := uint16(200 + rank)
				if rank == 0 {
					_, local, localOK := SplitTabletLocalIdentityBucket(uint32(source.Bucket))
					if !localOK {
						t.Fatal("source local identity")
					}
					localID = uint16(local)
				}
				bucket, bucketOK := MakeTabletLocalIdentityBucket(0, uint32(localID))
				if !bucketOK {
					t.Fatalf("replacement bucket %d", localID)
				}
				logicalID, logicalOK := CommonPrimaryLeafLogicalID(BucketID(bucket))
				if !logicalOK {
					t.Fatalf("replacement logical ID %d", localID)
				}
				replacements[rank] = SegmentedTabletRouterLeaf{
					LocalID: localID, Fence: []byte(rawFence),
					Ref: PageRef{Offset: uint64(1+rank) * 64 << 10,
						LogicalID: logicalID, Generation: generation,
						Length: 4096, Kind: PagePrimaryLeaf},
				}
			}
			next, err := router.SplitLeafPartition(source, replacements, generation)
			if err != nil {
				t.Fatal(err)
			}
			if got, want := next.Len(), router.Len()-1+len(replacements); got != want {
				t.Fatalf("partition route count = %d, want %d", got, want)
			}
			if got := next.Generation(); got != generation {
				t.Fatalf("partition generation = %d, want %d", got, generation)
			}
			for rank, replacement := range replacements {
				got, routeOK := next.Route([]byte(test.fences[rank]))
				wantBucket, _ := MakeTabletLocalIdentityBucket(0, uint32(replacement.LocalID))
				if !routeOK || got.Ref != replacement.Ref ||
					got.Bucket != BucketID(wantBucket) {
					t.Fatalf("replacement rank %d route = %+v,%v", rank, got, routeOK)
				}
			}
			for rank := range router.Len() {
				if rank == test.source {
					continue
				}
				old, oldOK := router.RouteAtRank(rank)
				newRank := rank
				if rank > test.source {
					newRank += len(replacements) - 1
				}
				got, newOK := next.RouteAtRank(newRank)
				if !oldOK || !newOK || got.Ref != old.Ref || got.Bucket != old.Bucket {
					t.Fatalf("unaffected rank %d = %+v,%v, want %+v", rank, got, newOK, old)
				}
			}

			duplicate := slices.Clone(replacements)
			duplicate[2].LocalID = duplicate[1].LocalID
			duplicateBucket, duplicateOK := MakeTabletLocalIdentityBucket(
				0, uint32(duplicate[2].LocalID),
			)
			if !duplicateOK {
				t.Fatal("duplicate replacement bucket")
			}
			duplicate[2].Ref.LogicalID, duplicateOK =
				SegmentedTabletRouterLeafLogicalID(BucketID(duplicateBucket))
			if !duplicateOK {
				t.Fatal("duplicate replacement logical ID")
			}
			if _, err := router.SplitLeafPartition(source, duplicate, generation); !errors.Is(err, ErrInvalidWrite) {
				t.Fatalf("duplicate replacement error = %v, want %v", err, ErrInvalidWrite)
			}
			duplicate = slices.Clone(replacements)
			duplicate[1].LocalID = duplicate[0].LocalID
			duplicateBucket, duplicateOK = MakeTabletLocalIdentityBucket(
				0, uint32(duplicate[1].LocalID),
			)
			if !duplicateOK {
				t.Fatal("source duplicate bucket")
			}
			duplicate[1].Ref.LogicalID, duplicateOK =
				SegmentedTabletRouterLeafLogicalID(BucketID(duplicateBucket))
			if !duplicateOK {
				t.Fatal("source duplicate logical ID")
			}
			if _, err := router.SplitLeafPartition(source, duplicate, generation); !errors.Is(err, ErrInvalidWrite) {
				t.Fatalf("source duplicate error = %v, want %v", err, ErrInvalidWrite)
			}
		})
	}
}

func TestResidentPrimaryRouterSplitLeafPartitionPreservesGlobalTabletFloor(
	t *testing.T,
) {
	const (
		generation = uint64(100)
		tabletID   = uint32(7)
	)
	entries := []struct {
		tablet uint32
		local  uint16
		fence  string
	}{
		{tablet: 0, local: 100, fence: ""},
		{tablet: 0, local: 101, fence: "m"},
		{tablet: tabletID, local: 100, fence: "tablet/07"},
		{tablet: tabletID, local: 101, fence: "tablet/07/050"},
		{tablet: tabletID, local: 102, fence: "tablet/07/100"},
		{tablet: 9, local: 100, fence: "tablet/09"},
	}
	fences := make([]byte, 0, 128)
	rows := make([]uint64, len(entries)*residentPrimaryRouterWords)
	for rank, entry := range entries {
		bucket, ok := MakeTabletLocalIdentityBucket(entry.tablet, uint32(entry.local))
		if !ok {
			t.Fatalf("entry %d bucket", rank)
		}
		logicalID, ok := SegmentedTabletRouterLeafLogicalID(BucketID(bucket))
		if !ok {
			t.Fatalf("entry %d logical ID", rank)
		}
		ref := PageRef{
			Offset: uint64(rank+1) * 64 << 10, LogicalID: logicalID,
			Generation: generation, Length: 4096, Kind: PagePrimaryLeaf,
		}
		start := len(fences)
		fences = append(fences, entry.fence...)
		at := rank * residentPrimaryRouterWords
		rows[at] = uint64(uint32(start)) | uint64(uint32(len(fences)))<<32
		rows[at+1] = ref.Offset
		rows[at+2] = ref.Generation
		rows[at+3] = uint64(ref.Length) | uint64(bucket)<<32
	}
	router := &ResidentPrimaryRouter{
		storeID: [16]byte{0x72, 0x6f, 0x75, 0x72, 0x74, 0x65, 0x72},
		fences:  fences, rows: rows,
		hints: make([]pageCacheFrameHint, len(entries)),
		empty: make([]atomic.Uint32, len(entries)),
	}
	router.buildSearchKeys()
	router.generation.Store(generation)
	source, ok := router.Route([]byte("tablet/07"))
	if !ok || source.Bucket != BucketID(mustResidentTabletBucket(t, tabletID, 100)) {
		t.Fatalf("later-tablet source = %+v,%v", source, ok)
	}
	replacementFences := []string{
		"", "tablet/07/010", "tablet/07/020", "tablet/07/030",
	}
	const outputCount = 4
	replacements := make([]SegmentedTabletRouterLeaf, outputCount)
	for rank, fence := range replacementFences {
		localID := uint16(200 + rank)
		if rank == 0 {
			localID = 100
		}
		bucket, bucketOK := MakeTabletLocalIdentityBucket(tabletID, uint32(localID))
		if !bucketOK {
			t.Fatalf("replacement %d bucket", rank)
		}
		logicalID, logicalOK := SegmentedTabletRouterLeafLogicalID(BucketID(bucket))
		if !logicalOK {
			t.Fatalf("replacement %d logical ID", rank)
		}
		replacements[rank] = SegmentedTabletRouterLeaf{
			LocalID: localID, Fence: []byte(fence),
			Ref: PageRef{
				Offset: uint64(32+rank) * 64 << 10, LogicalID: logicalID,
				Generation: generation + 1, Length: 4096, Kind: PagePrimaryLeaf,
			},
		}
	}
	next, err := router.SplitLeafPartition(source, replacements, generation+1)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(next.fence(2)); got != "tablet/07" {
		t.Fatalf("preserved global tablet floor = %q", got)
	}
	for _, test := range []struct {
		key    string
		bucket uint32
	}{
		{key: "tablet/07", bucket: mustResidentTabletBucket(t, tabletID, 100)},
		{key: "tablet/07/015", bucket: mustResidentTabletBucket(t, tabletID, 201)},
		{key: "tablet/07/025", bucket: mustResidentTabletBucket(t, tabletID, 202)},
		{key: "tablet/07/035", bucket: mustResidentTabletBucket(t, tabletID, 203)},
		{key: "tablet/07/075", bucket: mustResidentTabletBucket(t, tabletID, 101)},
		{key: "tablet/09", bucket: mustResidentTabletBucket(t, 9, 100)},
	} {
		got, routeOK := next.Route([]byte(test.key))
		if !routeOK || got.Bucket != BucketID(test.bucket) {
			t.Fatalf("global route %q = %+v,%v, want bucket %d", test.key, got, routeOK, test.bucket)
		}
	}
	for _, key := range []string{"mango", "tablet/09"} {
		before, beforeOK := router.Route([]byte(key))
		after, afterOK := next.Route([]byte(key))
		if !beforeOK || !afterOK || before.Ref != after.Ref ||
			before.Bucket != after.Bucket {
			t.Fatalf("unaffected tablet route %q = before:%+v,%v after:%+v,%v",
				key, before, beforeOK, after, afterOK)
		}
	}
	duplicate := slices.Clone(replacements)
	duplicate[1].LocalID = 102
	duplicateBucket, duplicateOK := MakeTabletLocalIdentityBucket(
		tabletID, uint32(duplicate[1].LocalID),
	)
	if !duplicateOK {
		t.Fatal("selected-tablet duplicate bucket")
	}
	duplicate[1].Ref.LogicalID, duplicateOK =
		SegmentedTabletRouterLeafLogicalID(BucketID(duplicateBucket))
	if !duplicateOK {
		t.Fatal("selected-tablet duplicate logical ID")
	}
	if _, err := router.SplitLeafPartition(source, duplicate, generation+1); !errors.Is(err, ErrInvalidWrite) {
		t.Fatalf("selected-tablet duplicate error = %v, want %v", err, ErrInvalidWrite)
	}
	badFence := slices.Clone(replacements)
	badFence[1].Fence = []byte("a")
	if _, err := router.SplitLeafPartition(source, badFence, generation+1); !errors.Is(err, ErrInvalidWrite) {
		t.Fatalf("local/global fence mismatch error = %v, want %v", err, ErrInvalidWrite)
	}
}

func mustResidentTabletBucket(t testing.TB, tabletID uint32, localID uint16) uint32 {
	t.Helper()
	bucket, ok := MakeTabletLocalIdentityBucket(tabletID, uint32(localID))
	if !ok {
		t.Fatalf("resident tablet bucket tablet=%d local=%d", tabletID, localID)
	}
	return bucket
}

func TestResidentPrimaryRouterRemoveLeafSplicesWithoutGraphWalk(t *testing.T) {
	router := residentPrimaryRouterGenerationTestFixture(t)
	middle, ok := router.Route([]byte("mango"))
	if !ok || middle.Bucket != 101 {
		t.Fatalf("middle route = %+v,%v", middle, ok)
	}
	next, err := router.RemoveLeaf(middle, 101)
	if err != nil {
		t.Fatal(err)
	}
	if next.Len() != 2 || next.Generation() != 101 ||
		router.Len() != 3 || router.Generation() != 100 {
		t.Fatalf("router cardinality/generation = %d/%d old=%d/%d",
			next.Len(), next.Generation(), router.Len(), router.Generation())
	}
	for _, test := range []struct {
		key    string
		bucket BucketID
	}{{"alpha", 100}, {"mango", 100}, {"zulu", 102}} {
		got, routeOK := next.Route([]byte(test.key))
		if !routeOK || got.Bucket != test.bucket {
			t.Fatalf("route %q = %+v,%v", test.key, got, routeOK)
		}
	}
	if _, err := router.RemoveLeaf(middle, 100); !errors.Is(err, ErrInvalidWrite) {
		t.Fatalf("stale generation error = %v", err)
	}
	if got := testing.AllocsPerRun(100, func() {
		spliced, removeErr := router.RemoveLeaf(middle, 101)
		if removeErr != nil || spliced.Len() != 2 {
			panic("remove splice")
		}
	}); got > 8 {
		t.Fatalf("remove splice allocations = %v, want <= 8", got)
	}
}

func TestResidentPrimaryRouterRemoveFirstLeafPromotesEmptyFloor(t *testing.T) {
	router := residentPrimaryRouterGenerationTestFixture(t)
	first := mustResidentRoute(t, router, 0)
	next, err := router.RemoveLeaf(first, 101)
	if err != nil {
		t.Fatal(err)
	}
	if next.Len() != 2 || len(next.fence(0)) != 0 {
		t.Fatalf("next first floor/cardinality = %q/%d", next.fence(0), next.Len())
	}
	for _, key := range []string{"alpha", "mango"} {
		got, ok := next.Route([]byte(key))
		if !ok || got.Bucket != 101 {
			t.Fatalf("route %q = %+v,%v", key, got, ok)
		}
	}
	one := &ResidentPrimaryRouter{
		storeID: router.storeID,
		rows:    slices.Clone(router.rows[:residentPrimaryRouterWords]),
		hints:   make([]pageCacheFrameHint, 1),
		empty:   make([]atomic.Uint32, 1),
	}
	one.generation.Store(100)
	if _, err := one.RemoveLeaf(mustResidentRoute(t, one, 0), 101); !errors.Is(err, ErrInvalidWrite) {
		t.Fatalf("singleton removal error = %v", err)
	}
}

func mustResidentRoute(t testing.TB, router *ResidentPrimaryRouter, rank int) ResidentPrimaryRoute {
	t.Helper()
	route, ok := router.RouteAtRank(rank)
	if !ok {
		t.Fatalf("route rank %d", rank)
	}
	return route
}

func BenchmarkResidentPrimaryRouterSplitLeaf4096(b *testing.B) {
	const count = TabletLocalIdentityLocalCount - 1
	var fences []byte
	rows := make([]uint64, count*residentPrimaryRouterWords)
	for rank := range count {
		fence := fmt.Appendf(nil, "f%04d", rank)
		start := uint32(len(fences))
		fences = append(fences, fence...)
		end := uint32(len(fences))
		bucket := BucketID(rank)
		logicalID, ok := CommonPrimaryLeafLogicalID(bucket)
		if !ok {
			b.Fatalf("leaf logical ID for bucket %d", bucket)
		}
		at := rank * residentPrimaryRouterWords
		rows[at] = uint64(start) | uint64(end)<<32
		rows[at+1] = uint64(4096 * (rank + 1))
		rows[at+2] = logicalID
		rows[at+3] = uint64(4096) | uint64(uint32(bucket))<<32
	}
	router := &ResidentPrimaryRouter{
		fences: fences,
		rows:   rows,
		hints:  make([]pageCacheFrameHint, count),
		empty:  make([]atomic.Uint32, count),
	}
	router.buildSearchKeys()
	router.generation.Store(100)
	route, ok := router.Route([]byte("f2048"))
	if !ok {
		b.Fatal("source route")
	}
	left := route.Ref
	left.Offset += 64 << 20
	left.Generation = 101
	rightBucket := BucketID(count)
	rightLogical, ok := CommonPrimaryLeafLogicalID(rightBucket)
	if !ok {
		b.Fatal("right logical ID")
	}
	right := PageRef{Offset: left.Offset + 4096, LogicalID: rightLogical,
		Generation: 101, Length: 4096, Kind: PagePrimaryLeaf}
	b.ReportAllocs()
	b.ReportMetric(count+1, "routes/op")
	b.ResetTimer()
	for b.Loop() {
		var err error
		residentPrimaryRouterSplitBenchmarkSink, err = router.SplitLeaf(
			route, left, rightBucket, []byte("f2048z"), right, 101,
		)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestResidentPrimaryRouterRouteConcurrentWithAdvanceGeneration(
	t *testing.T,
) {
	router := residentPrimaryRouterGenerationTestFixture(t)
	keys := [][]byte{[]byte("alpha"), []byte("mango"), []byte("zulu")}
	want := make([]ResidentPrimaryRoute, len(keys))
	for i, key := range keys {
		var ok bool
		want[i], ok = router.Route(key)
		if !ok {
			t.Fatalf("route %q missing", key)
		}
	}

	const (
		readers  = 8
		advances = 20_000
	)
	start := make(chan struct{})
	var failed atomic.Bool
	var group sync.WaitGroup
	group.Add(readers + 1)
	for reader := range readers {
		go func() {
			defer group.Done()
			<-start
			for iteration := range advances {
				index := (reader + iteration) % len(keys)
				got, ok := router.Route(keys[index])
				if !ok || got != want[index] {
					failed.Store(true)
					return
				}
				byRank, rankOK := router.RouteAtRank(int(got.rank))
				if !rankOK || byRank.Ref != got.Ref ||
					byRank.Bucket != got.Bucket {
					failed.Store(true)
					return
				}
			}
		}()
	}
	go func() {
		defer group.Done()
		<-start
		for generation := uint64(101); generation <= 100+advances; generation++ {
			router.AdvanceGeneration(generation)
		}
	}()
	beforeVersion := router.version.Load()
	close(start)
	group.Wait()

	if failed.Load() {
		t.Fatal("concurrent generation advance changed a routed leaf")
	}
	if got := router.Generation(); got != 100+advances {
		t.Fatalf("generation = %d, want %d", got, 100+advances)
	}
	if got := router.version.Load(); got != beforeVersion {
		t.Fatalf("version = %d, want unchanged %d", got, beforeVersion)
	}
}
