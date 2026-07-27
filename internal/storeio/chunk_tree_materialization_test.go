package storeio

import (
	"errors"
	"testing"
)

func TestPageCacheTryAcquireResidentNeverLoadsAndCanonicalAbsentIsBusy(
	t *testing.T,
) {
	file, storeID, refs := newPageCacheFixture(t, 1)
	cache, err := NewPageCache(file, PageCacheOptions{
		PageSize:      pageCacheTestPageSize,
		ResidentBytes: pageCacheTestPageSize,
		StoreID:       storeID,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()

	beforeStats := cache.Stats()
	if lease, resident, err := cache.TryAcquireResident(refs[0]); err != nil ||
		resident {
		lease.Release()
		t.Fatalf("cold TryAcquireResident = %v, %v", resident, err)
	}
	afterStats := cache.Stats()
	if afterStats.PageReads != beforeStats.PageReads ||
		afterStats.ReadBytes != beforeStats.ReadBytes {
		t.Fatalf("cold resident probe performed I/O: before %+v, after %+v",
			beforeStats, afterStats)
	}

	lease, err := cache.Acquire(refs[0])
	if err != nil {
		t.Fatal(err)
	}
	before, after := canonicalCacheImages(t, lease.Page(), refs[0], 29)
	lease.Release()
	if !cache.Invalidate(refs[0]) {
		t.Fatal("failed to evict clean canonical fixture")
	}
	if err := cache.ReplaceCanonicalDirty(
		refs[0], before, after, 2,
	); !errors.Is(err, ErrCanonicalPageBusy) {
		t.Fatalf("absent canonical replacement = %v, want %v",
			err, ErrCanonicalPageBusy)
	}
}
