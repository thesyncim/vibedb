package storeio

import (
	"slices"
	"sync"
	"sync/atomic"
	"testing"
)

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
