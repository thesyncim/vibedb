package storeio

import "testing"

func TestVisitPrimaryGraphRefsStreamsEveryAuthenticatedExtentOnce(t *testing.T) {
	image, records := buildPrimaryGraphTestImage(t, 1000)
	plan, err := PlanPrimaryGraph(image.root.StoreID, records, false)
	if err != nil {
		t.Fatal(err)
	}
	cache, err := NewPageCache(image.file, PageCacheOptions{
		PageSize: int(format0PageSize), MaxPageSize: GlobalTabletCatalogRootBytes,
		ResidentBytes: 4 << 20, StoreID: image.root.StoreID, ReadConcurrency: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()
	seen := make(map[PageRef]struct{}, plan.PageCount())
	err = VisitPrimaryGraphRefs(
		cache, image.root.PrimaryRoot, image.bounds,
		func(ref PageRef) error {
			if _, exists := seen[ref]; exists {
				t.Fatalf("duplicate graph ref: %+v", ref)
			}
			seen[ref] = struct{}{}
			return nil
		},
	)
	if err != nil {
		t.Fatalf("visit after %d refs: %v", len(seen), err)
	}
	if len(seen) != plan.PageCount() {
		t.Fatalf("visited pages=%d, want %d", len(seen), plan.PageCount())
	}
}
