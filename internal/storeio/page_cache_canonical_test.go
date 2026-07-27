package storeio

import (
	"bytes"
	"errors"
	"os"
	"testing"
)

func canonicalCacheImages(
	t testing.TB, filePage []byte, ref PageRef, value byte,
) (before, after []byte) {
	t.Helper()
	before = append([]byte(nil), filePage...)
	after = append([]byte(nil), filePage...)
	_, payload, err := OpenPage(after)
	if err != nil {
		t.Fatal(err)
	}
	payload[0] = value
	if _, err := SealPage(after); err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(before, after) {
		t.Fatal("canonical test images are equal")
	}
	header, _, err := OpenPage(after)
	if err != nil || !canonicalPageHeaderMatchesRef(header, ref, header.StoreID) {
		t.Fatalf("invalid after-image: %v", err)
	}
	return before, after
}

func TestPageCacheCanonicalReplaceRestoreAndDurability(t *testing.T) {
	file, storeID, refs := newPageCacheFixture(t, 1)
	cache, err := NewPageCache(file, PageCacheOptions{
		PageSize: pageCacheTestPageSize, ResidentBytes: pageCacheTestPageSize,
		StoreID: storeID,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()

	lease, err := cache.Acquire(refs[0])
	if err != nil {
		t.Fatal(err)
	}
	before, after := canonicalCacheImages(t, lease.Page(), refs[0], 99)
	lease.Release()

	if err := cache.ReplaceCanonicalDirty(refs[0], before, after, 2); err != nil {
		t.Fatal(err)
	}
	if len(cache.dirtyFrames) != 1 {
		t.Fatalf("canonical dirty queue = %+v", cache.dirtyFrames)
	}
	if stats := cache.Stats(); stats.DirtyBytes != pageCacheTestPageSize {
		t.Fatalf("dirty replacement stats = %+v", stats)
	}
	lease, err = cache.Acquire(refs[0])
	if err != nil || !bytes.Equal(lease.Page(), after) {
		t.Fatalf("replacement Acquire = (%v,equal=%v)", err, bytes.Equal(lease.Page(), after))
	}
	lease.Release()

	if err := cache.RestoreCanonicalDirty(refs[0], before, after, 2); err != nil {
		t.Fatal(err)
	}
	if len(cache.dirtyFrames) != 0 {
		t.Fatalf("canonical rollback retained queue = %+v", cache.dirtyFrames)
	}
	if stats := cache.Stats(); stats.DirtyBytes != 0 {
		t.Fatalf("rollback stats = %+v", stats)
	}
	lease, err = cache.Acquire(refs[0])
	if err != nil || !bytes.Equal(lease.Page(), before) {
		t.Fatalf("rollback Acquire = (%v,equal=%v)", err, bytes.Equal(lease.Page(), before))
	}
	lease.Release()

	if err := cache.ReplaceCanonicalDirty(refs[0], before, after, 3); err != nil {
		t.Fatal(err)
	}
	if len(cache.dirtyFrames) != 1 {
		t.Fatalf("second canonical dirty queue = %+v", cache.dirtyFrames)
	}
	cache.MarkDurable(2)
	if stats := cache.Stats(); stats.DirtyBytes != pageCacheTestPageSize {
		t.Fatalf("early durability fence cleared replacement: %+v", stats)
	}
	cache.MarkDurable(3)
	if len(cache.dirtyFrames) != 0 {
		t.Fatalf("canonical durability retained queue = %+v", cache.dirtyFrames)
	}
	if stats := cache.Stats(); stats.DirtyBytes != 0 {
		t.Fatalf("durability fence retained replacement: %+v", stats)
	}
}

func TestPageCacheCanonicalReplaceRejectsPinsChangesAndInvalidImages(t *testing.T) {
	file, storeID, refs := newPageCacheFixture(t, 1)
	cache, err := NewPageCache(file, PageCacheOptions{
		PageSize: pageCacheTestPageSize, ResidentBytes: pageCacheTestPageSize,
		StoreID: storeID,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()

	lease, err := cache.Acquire(refs[0])
	if err != nil {
		t.Fatal(err)
	}
	before, after := canonicalCacheImages(t, lease.Page(), refs[0], 77)
	if err := cache.ReplaceCanonicalDirty(refs[0], before, after, 2); !errors.Is(err, ErrCanonicalPageBusy) {
		t.Fatalf("pinned replacement = %v, want %v", err, ErrCanonicalPageBusy)
	}
	lease.Release()

	changed := append([]byte(nil), before...)
	_, payload, err := OpenPage(changed)
	if err != nil {
		t.Fatal(err)
	}
	payload[0] = 44
	if _, err := SealPage(changed); err != nil {
		t.Fatal(err)
	}
	if err := cache.ReplaceCanonicalDirty(refs[0], changed, after, 2); !errors.Is(err, ErrCanonicalPageChanged) {
		t.Fatalf("changed before-image = %v, want %v", err, ErrCanonicalPageChanged)
	}

	corrupt := append([]byte(nil), after...)
	corrupt[len(corrupt)-1] ^= 1
	if err := cache.ReplaceCanonicalDirty(refs[0], before, corrupt, 2); err == nil {
		t.Fatal("corrupt after-image accepted")
	}
	if err := cache.RestoreCanonicalDirty(refs[0], before, after, 2); !errors.Is(err, ErrCanonicalPageChanged) {
		t.Fatalf("rollback without replacement = %v, want %v", err, ErrCanonicalPageChanged)
	}

	if err := cache.ReplaceCanonicalDirty(refs[0], before, after, 2); err != nil {
		t.Fatal(err)
	}
	wrongAfter := append([]byte(nil), after...)
	_, payload, err = OpenPage(wrongAfter)
	if err != nil {
		t.Fatal(err)
	}
	payload[0] ^= 1
	if _, err := SealPage(wrongAfter); err != nil {
		t.Fatal(err)
	}
	if err := cache.RestoreCanonicalDirty(
		refs[0], before, wrongAfter, 2,
	); !errors.Is(err, ErrCanonicalPageChanged) {
		t.Fatalf("stale rollback = %v, want %v", err, ErrCanonicalPageChanged)
	}
	lease, err = cache.Acquire(refs[0])
	if err != nil || !bytes.Equal(lease.Page(), after) {
		t.Fatalf("stale rollback changed frame = (%v,equal=%v)", err, bytes.Equal(lease.Page(), after))
	}
	lease.Release()
	if err := cache.RestoreCanonicalDirty(refs[0], before, after, 2); err != nil {
		t.Fatal(err)
	}
}

func TestPageCacheCanonicalRollbackRunsTypedValidation(t *testing.T) {
	file, storeID, refs := newPageCacheFixture(t, 1)
	reject := false
	rejected := errors.New("typed rollback rejected")
	cache, err := NewPageCache(file, PageCacheOptions{
		PageSize: pageCacheTestPageSize, ResidentBytes: pageCacheTestPageSize,
		StoreID: storeID,
		Validate: func(_ []byte, _ PageRef) error {
			if reject {
				return rejected
			}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()
	lease, err := cache.Acquire(refs[0])
	if err != nil {
		t.Fatal(err)
	}
	before, after := canonicalCacheImages(t, lease.Page(), refs[0], 33)
	lease.Release()
	if err := cache.ReplaceCanonicalDirty(refs[0], before, after, 2); err != nil {
		t.Fatal(err)
	}
	reject = true
	if err := cache.RestoreCanonicalDirty(refs[0], before, after, 2); !errors.Is(err, rejected) {
		t.Fatalf("typed-invalid rollback = %v, want %v", err, rejected)
	}
	reject = false
	if err := cache.RestoreCanonicalDirty(refs[0], before, after, 2); err != nil {
		t.Fatal(err)
	}
}

func TestPageCacheCanonicalMultiPageDirtyReservationAccounting(t *testing.T) {
	const (
		quantum = 4096
		length  = 3 * quantum
	)
	file, err := os.CreateTemp(t.TempDir(), "canonical-cache-extent-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	storeID := [16]byte{9, 7, 5, 3, 1, 2, 4, 6, 8, 10, 12, 14, 16, 15, 13, 11}
	ref := PageRef{
		Offset: 4 * quantum, LogicalID: 2, Generation: 1,
		Length: length, Kind: PageDocument,
	}
	page := make([]byte, length)
	payload, err := InitPage(page, PageHeader{
		StoreID: storeID, Generation: ref.Generation, LogicalID: ref.LogicalID,
		PageSize: ref.Length, PayloadLength: 32, Kind: ref.Kind,
	})
	if err != nil {
		t.Fatal(err)
	}
	payload[0] = 1
	if _, err := SealPage(page); err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt(page, int64(ref.Offset)); err != nil {
		t.Fatal(err)
	}
	cache, err := NewPageCache(file, PageCacheOptions{
		PageSize: quantum, MaxPageSize: 4 * quantum,
		ResidentBytes: 4 * quantum, StoreID: storeID,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()
	lease, err := cache.Acquire(ref)
	if err != nil {
		t.Fatal(err)
	}
	before, after := canonicalCacheImages(t, lease.Page(), ref, 2)
	lease.Release()
	if err := cache.ReplaceCanonicalDirty(ref, before, after, 2); err != nil {
		t.Fatal(err)
	}
	if cache.dirtyBytes != length || cache.dirtyReservedBytes != 3*quantum {
		t.Fatalf("dirty accounting = (%d,%d), want (%d,%d)",
			cache.dirtyBytes, cache.dirtyReservedBytes, length, 3*quantum)
	}
	if err := cache.RestoreCanonicalDirty(ref, before, after, 2); err != nil {
		t.Fatal(err)
	}
	if cache.dirtyBytes != 0 || cache.dirtyReservedBytes != 0 {
		t.Fatalf("rollback dirty accounting = (%d,%d), want zero",
			cache.dirtyBytes, cache.dirtyReservedBytes)
	}
}

func TestPageCacheCanonicalReplaceRestoreWarmAllocations(t *testing.T) {
	file, storeID, refs := newPageCacheFixture(t, 1)
	cache, err := NewPageCache(file, PageCacheOptions{
		PageSize: pageCacheTestPageSize, ResidentBytes: pageCacheTestPageSize,
		StoreID: storeID,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()

	lease, err := cache.Acquire(refs[0])
	if err != nil {
		t.Fatal(err)
	}
	before, after := canonicalCacheImages(t, lease.Page(), refs[0], 55)
	lease.Release()
	if allocs := testing.AllocsPerRun(1000, func() {
		if err := cache.ReplaceCanonicalDirty(refs[0], before, after, 2); err != nil {
			panic(err)
		}
		if err := cache.RestoreCanonicalDirty(refs[0], before, after, 2); err != nil {
			panic(err)
		}
	}); allocs != 0 {
		t.Fatalf("canonical replace+restore allocations = %g, want 0", allocs)
	}
}
