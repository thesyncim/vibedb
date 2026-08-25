package storeio

import (
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
