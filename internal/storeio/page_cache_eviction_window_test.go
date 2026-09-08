package storeio

import (
	"bytes"
	"testing"
	"time"
)

func TestPageCacheEvictionWindowDefersFastPin(t *testing.T) {
	cache, refs := newTwoSlotReadyPageCache(t)

	keys := make([]pageCacheKey, len(refs))
	for index, ref := range refs {
		key, err := cache.validateRef(ref)
		if err != nil {
			t.Fatal(err)
		}
		keys[index] = key
	}
	cache.mu.Lock()
	cacheLocked := true
	prepared := false
	preparedCount := 0
	unlockHeld := func() {
		if prepared {
			for locked := preparedCount - 1; locked >= 0; locked-- {
				cache.frames[cache.evictionScratch[locked]].lock.Unlock()
			}
			prepared = false
		}
		if cacheLocked {
			cache.mu.Unlock()
			cacheLocked = false
		}
	}
	defer unlockHeld()
	indexes := make([]int, len(keys))
	for refIndex, key := range keys {
		index, ok := cache.lookupLocked(cacheKeyHash(key), key)
		if !ok {
			t.Fatalf("ready reference %d is not in cache", refIndex)
		}
		indexes[refIndex] = index
	}
	first, second := indexes[0], indexes[1]
	if first > second {
		first, second = second, first
	}
	if first != 0 || second != 1 {
		t.Fatalf("ready frame indexes = (%d,%d), want (0,1)", first, second)
	}

	count, ok := cache.prepareEvictWindowLocked(first, 2)
	prepared, preparedCount = ok, count
	if !ok || count != 2 {
		t.Fatalf("prepare eviction window = (%d,%v), want (2,true)", count, ok)
	}
	pinnedIndex := int(cache.evictionScratch[1])
	pinnedRefIndex := 0
	for index, frameIndex := range indexes {
		if frameIndex == pinnedIndex {
			pinnedRefIndex = index
			break
		}
	}
	pinnedRef := refs[pinnedRefIndex]
	pinnedKey := keys[pinnedRefIndex]

	type acquireResult struct {
		lease PageLease
		err   error
	}
	acquired := make(chan acquireResult, 1)
	cancel := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		lease, err := cache.Acquire(pinnedRef)
		select {
		case acquired <- acquireResult{lease: lease, err: err}:
		case <-cancel:
			lease.Release()
		}
	}()
	var result acquireResult
	select {
	case result = <-acquired:
	case <-time.After(5 * time.Second):
		close(cancel)
		unlockHeld()
		<-done
		select {
		case result = <-acquired:
			result.lease.Release()
		default:
		}
		t.Fatal("fast resident Acquire did not complete while eviction locks were held")
	}
	if result.err != nil {
		unlockHeld()
		t.Fatalf("fast resident Acquire = %v", result.err)
	}
	if result.lease.frame != pinnedIndex {
		unlockHeld()
		result.lease.Release()
		t.Fatalf("fast resident Acquire frame = %d, want %d", result.lease.frame, pinnedIndex)
	}
	var wantPagePrefix, wantPayloadPrefix [4]byte
	copy(wantPagePrefix[:], result.lease.Page()[:len(wantPagePrefix)])
	copy(wantPayloadPrefix[:], result.lease.Payload()[:len(wantPayloadPrefix)])

	beforeEvictions := cache.evictions
	complete := cache.finishEvictWindowLocked(count)
	prepared = false
	if complete {
		unlockHeld()
		result.lease.Release()
		t.Fatal("partially pinned eviction window reported complete")
	}
	if got := cache.evictions - beforeEvictions; got != 1 {
		unlockHeld()
		result.lease.Release()
		t.Fatalf("partial window evictions = %d, want 1", got)
	}
	if cache.frames[first].state.Load() != uint32(pageCacheEmpty) {
		unlockHeld()
		result.lease.Release()
		t.Fatalf("unpinned frame %d was not freed", first)
	}
	pinnedFrame := &cache.frames[pinnedIndex]
	if pinnedFrame.state.Load() != uint32(pageCacheReady) ||
		pinnedFrame.key != pinnedKey || pinnedFrame.pins.Load() != 1 ||
		pinnedFrame.flags.Load()&pageCacheFrameDoomed == 0 ||
		pinnedFrame.referenced.Load() {
		unlockHeld()
		result.lease.Release()
		t.Fatalf("deferred pinned frame = state %d key %+v pins %d flags %#x referenced %v",
			pinnedFrame.state.Load(), pinnedFrame.key, pinnedFrame.pins.Load(),
			pinnedFrame.flags.Load(), pinnedFrame.referenced.Load())
	}
	page := cache.extentBytes(pinnedIndex, pinnedKey.length)
	if !bytes.Equal(page[:len(wantPagePrefix)], wantPagePrefix[:]) ||
		!bytes.Equal(page[PageHeaderSize:PageHeaderSize+len(wantPayloadPrefix)], wantPayloadPrefix[:]) {
		unlockHeld()
		result.lease.Release()
		t.Fatal("deferred pinned frame changed its page or payload bytes")
	}
	if _, ok := cache.reserveLocked(2); ok {
		unlockHeld()
		result.lease.Release()
		t.Fatal("partial eviction exposed a two-slot reservation while one frame remained pinned")
	}
	unlockHeld()

	result.lease.Release()
	stats := cache.Stats()
	if stats.ResidentBytes != 0 || stats.ReservedBytes != 0 || stats.ReadyFrames != 0 ||
		stats.Pins != 0 {
		t.Fatalf("post-release partial eviction stats = %+v", stats)
	}
	cache.mu.Lock()
	start, available := cache.blocks.take(2)
	if available {
		cache.blocks.put(start, 2)
	}
	cache.mu.Unlock()
	if !available || start != 0 {
		t.Fatalf("released full reservation = (%d,%v), want (0,true)", start, available)
	}
}

func TestPageCacheEvictionWindowCompletesWithoutPin(t *testing.T) {
	cache, refs := newTwoSlotReadyPageCache(t)
	cache.mu.Lock()
	key, err := cache.validateRef(refs[0])
	if err != nil {
		cache.mu.Unlock()
		t.Fatal(err)
	}
	start, ok := cache.lookupLocked(cacheKeyHash(key), key)
	if !ok {
		cache.mu.Unlock()
		t.Fatal("first ready reference is not in cache")
	}
	beforeEvictions := cache.evictions
	complete := cache.evictWindowLocked(start, 2)
	if !complete {
		cache.mu.Unlock()
		t.Fatal("uncontended eviction window did not complete")
	}
	if got := cache.evictions - beforeEvictions; got != 2 {
		cache.mu.Unlock()
		t.Fatalf("complete window evictions = %d, want 2", got)
	}
	for index := range cache.frames {
		if state := cache.frames[index].state.Load(); state != uint32(pageCacheEmpty) {
			cache.mu.Unlock()
			t.Fatalf("frame %d state = %d after complete eviction, want empty", index, state)
		}
	}
	start, available := cache.blocks.take(2)
	if available {
		cache.blocks.put(start, 2)
	}
	cache.mu.Unlock()
	if !available || start != 0 {
		t.Fatalf("complete eviction reservation = (%d,%v), want (0,true)", start, available)
	}
	if stats := cache.Stats(); stats.ResidentBytes != 0 || stats.ReservedBytes != 0 ||
		stats.ReadyFrames != 0 || stats.Pins != 0 {
		t.Fatalf("complete eviction stats = %+v", stats)
	}
}

func newTwoSlotReadyPageCache(t *testing.T) (*PageCache, []PageRef) {
	t.Helper()
	file, storeID, refs := newPageCacheFixture(t, 2)
	cache, err := NewPageCache(file, PageCacheOptions{
		PageSize:        pageCacheTestPageSize,
		MaxPageSize:     pageCacheTestPageSize * 2,
		ResidentBytes:   pageCacheTestPageSize * 2,
		StoreID:         storeID,
		ReadConcurrency: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := cache.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	for _, ref := range refs {
		lease, err := cache.Acquire(ref)
		if err != nil {
			t.Fatal(err)
		}
		lease.Release()
	}
	return cache, refs
}
