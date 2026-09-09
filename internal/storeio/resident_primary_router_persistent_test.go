package storeio

import (
	"errors"
	"fmt"
	"testing"
)

func persistentRouterTestRef(t testing.TB, bucket BucketID, generation uint64, page int) PageRef {
	t.Helper()
	logicalID, ok := CommonPrimaryLeafLogicalID(bucket)
	if !ok {
		t.Fatalf("logical ID for bucket %d", bucket)
	}
	return PageRef{
		Offset: uint64(page+1) * 4096, LogicalID: logicalID,
		Generation: generation, Length: 4096, Kind: PagePrimaryLeaf,
	}
}

func TestResidentPrimaryPersistentRouterRejectsForeignAndRetiredCells(t *testing.T) {
	router := residentPrimaryRouterGenerationTestFixture(t)
	foreign := residentPrimaryRouterGenerationTestFixture(t)
	foreignRoute, ok := foreign.Route([]byte("mango"))
	if !ok {
		t.Fatal("foreign route")
	}
	foreignNext := persistentRouterTestRef(t, foreignRoute.Bucket, 101, 100)
	if router.CanUpdateLeaf(foreignRoute, foreignNext, 101) {
		t.Fatal("router accepted an equal-valued route cell owned by another router")
	}

	source, ok := router.Route([]byte("mango"))
	if !ok {
		t.Fatal("split source route")
	}
	left := persistentRouterTestRef(t, source.Bucket, 101, 101)
	rightBucket := BucketID(409)
	right := persistentRouterTestRef(t, rightBucket, 101, 102)
	next, err := router.SplitLeaf(source, left, rightBucket, []byte("s"), right, 101)
	if err != nil {
		t.Fatal(err)
	}
	retiredNext := persistentRouterTestRef(t, source.Bucket, 102, 103)
	if next.CanUpdateLeaf(source, retiredNext, 102) {
		t.Fatal("new router accepted the retired pre-split source cell")
	}
}

func TestResidentPrimaryPersistentRouterRemoveFirstPreservesSuccessorEmpty(t *testing.T) {
	router := residentPrimaryRouterGenerationTestFixture(t)
	first, _ := router.RouteAtRank(0)
	successor, _ := router.RouteAtRank(1)
	if !router.MarkEmpty(successor) {
		t.Fatal("mark successor empty")
	}
	next, err := router.RemoveLeaf(first, 101)
	if err != nil {
		t.Fatal(err)
	}
	promoted, ok := next.RouteAtRank(0)
	if !ok || promoted.Bucket != successor.Bucket || promoted.Ref != successor.Ref {
		t.Fatalf("promoted successor = %+v,%v, want %+v", promoted, ok, successor)
	}
	if !next.ClearEmpty(promoted) {
		t.Fatal("promoted successor lost its empty marker")
	}
	if next.ClearEmpty(promoted) {
		t.Fatal("promoted successor empty marker cleared twice")
	}
}

func TestResidentPrimaryPersistentRouterNilSafety(t *testing.T) {
	var router *ResidentPrimaryRouter
	if router.MarkEmpty(ResidentPrimaryRoute{}) {
		t.Fatal("nil router marked a route empty")
	}
	if router.ClearEmpty(ResidentPrimaryRoute{}) {
		t.Fatal("nil router cleared a route empty")
	}

	real := residentPrimaryRouterGenerationTestFixture(t)
	route, ok := real.Route([]byte("mango"))
	if !ok || route.cell == nil {
		t.Fatal("real persistent route")
	}
	if _, err := real.AcquireLeaf(nil, route); !errors.Is(err, ErrPageCacheReference) {
		t.Fatalf("AcquireLeaf(nil) error = %v, want %v", err, ErrPageCacheReference)
	}
}

type persistentRouterModelEntry struct {
	fence  string
	bucket BucketID
	ref    PageRef
}

func assertPersistentRouterModel(t testing.TB, router *ResidentPrimaryRouter, model []persistentRouterModelEntry) {
	t.Helper()
	if router.Len() != len(model) {
		t.Fatalf("router length = %d, want %d", router.Len(), len(model))
	}
	for rank, want := range model {
		got, ok := router.RouteAtRank(rank)
		if !ok || got.Bucket != want.bucket || got.Ref != want.ref {
			t.Fatalf("rank %d = %+v,%v, want bucket=%d ref=%+v", rank, got, ok, want.bucket, want.ref)
		}
		probe, routeOK := router.Route([]byte(want.fence))
		if !routeOK || probe.Bucket != want.bucket || probe.Ref != want.ref {
			t.Fatalf("route floor %q = %+v,%v, want bucket=%d ref=%+v", want.fence, probe, routeOK, want.bucket, want.ref)
		}
	}
}

func TestResidentPrimaryPersistentRouterRepeatedInteriorSplitRemoveModel(t *testing.T) {
	router := residentPrimaryRouterGenerationTestFixture(t)
	model := make([]persistentRouterModelEntry, router.Len())
	for rank := range model {
		route, _ := router.RouteAtRank(rank)
		model[rank] = persistentRouterModelEntry{
			fence: string(router.fence(rank)), bucket: route.Bucket, ref: route.Ref,
		}
	}
	type retainedImage struct {
		router *ResidentPrimaryRouter
		model  []persistentRouterModelEntry
		gen    uint64
	}
	retained := []retainedImage{{
		router: router, model: append([]persistentRouterModelEntry(nil), model...),
		gen: router.Generation(),
	}}
	generation := router.Generation()

	// Repeatedly split the growing interior interval ["m", "t"). This crosses
	// several leaf-block boundaries while keeping every operation deterministic.
	for at := range 260 {
		sourceRank := 1 + at
		source, ok := router.RouteAtRank(sourceRank)
		if !ok {
			t.Fatalf("split %d source rank %d", at, sourceRank)
		}
		generation++
		left := persistentRouterTestRef(t, source.Bucket, generation, 1000+2*at)
		rightBucket := BucketID(1000 + at)
		right := persistentRouterTestRef(t, rightBucket, generation, 1001+2*at)
		fence := fmt.Sprintf("n%04d", at)
		next, err := router.SplitLeaf(source, left, rightBucket, []byte(fence), right, generation)
		if err != nil {
			t.Fatalf("split %d: %v", at, err)
		}
		model[sourceRank].ref = left
		inserted := persistentRouterModelEntry{fence: fence, bucket: rightBucket, ref: right}
		model = append(model, persistentRouterModelEntry{})
		copy(model[sourceRank+2:], model[sourceRank+1:])
		model[sourceRank+1] = inserted
		router = next
		if at%47 == 0 {
			retained = append(retained, retainedImage{
				router: router, model: append([]persistentRouterModelEntry(nil), model...),
				gen: router.Generation(),
			})
		}
	}
	assertPersistentRouterModel(t, router, model)

	// Remove interior rows in reverse order so model ranks remain simple while
	// exercising path copies and block coalescing throughout the tree.
	for rank := len(model) - 2; rank > 1; rank -= 3 {
		route, _ := router.RouteAtRank(rank)
		generation++
		next, err := router.RemoveLeaf(route, generation)
		if err != nil {
			t.Fatalf("remove rank %d: %v", rank, err)
		}
		model = append(model[:rank], model[rank+1:]...)
		router = next
	}
	assertPersistentRouterModel(t, router, model)
	for _, image := range retained {
		assertPersistentRouterModel(t, image.router, image.model)
		if got := image.router.Generation(); got != image.gen {
			t.Fatalf("retained router generation = %d, want %d", got, image.gen)
		}
	}
}
