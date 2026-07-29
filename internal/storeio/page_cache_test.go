package storeio

import (
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"
)

const pageCacheTestPageSize = 4096

func TestPageCacheBoundedAdmissionEvictionAndIdentity(t *testing.T) {
	file, storeID, refs := newPageCacheFixture(t, 3)
	cache, err := NewPageCache(file, PageCacheOptions{
		PageSize: pageCacheTestPageSize, ResidentBytes: 2 * pageCacheTestPageSize,
		StoreID: storeID, PrefetchQueue: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := cache.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})

	first, err := cache.Acquire(refs[0])
	if err != nil || len(first.Payload()) != 32 || first.Payload()[0] != 1 {
		t.Fatalf("first Acquire = payload %v, %v", first.Payload(), err)
	}
	if len(first.Page()) != pageCacheTestPageSize || cap(first.Page()) != pageCacheTestPageSize {
		t.Fatalf("first Page = %d/%d", len(first.Page()), cap(first.Page()))
	}
	second, err := cache.Acquire(refs[1])
	if err != nil || second.Payload()[0] != 2 {
		t.Fatalf("second Acquire = payload %v, %v", second.Payload(), err)
	}
	if _, err := cache.Acquire(refs[2]); !errors.Is(err, ErrPageCachePinned) {
		t.Fatalf("fully pinned Acquire error = %v, want %v", err, ErrPageCachePinned)
	}

	first.Release()
	third, err := cache.Acquire(refs[2])
	if err != nil || third.Payload()[0] != 3 {
		t.Fatalf("third Acquire = payload %v, %v", third.Payload(), err)
	}
	third.Release()
	second.Release()

	stats := cache.Stats()
	if stats.CapacityBytes != 2*pageCacheTestPageSize ||
		stats.ResidentBytes != stats.CapacityBytes || stats.PinnedPages != 0 ||
		stats.PageReads != 3 || stats.ReadBytes != 3*pageCacheTestPageSize || stats.Evictions != 1 {
		t.Fatalf("unexpected stats: %+v", stats)
	}

	wrong := refs[2]
	wrong.LogicalID++
	if _, err := cache.Acquire(wrong); !errors.Is(err, ErrPageCacheReference) {
		t.Fatalf("identity mismatch error = %v, want %v", err, ErrPageCacheReference)
	}
}

func TestPageCacheReadyHitIncludesRoutingMetadata(t *testing.T) {
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
	lease.Release()
	key, err := cache.validateRef(refs[0])
	if err != nil {
		t.Fatal(err)
	}
	wrong := key
	mask := uint64(len(cache.table) - 1)
	for aux := uint16(1); aux != 0; aux++ {
		wrong.aux = aux
		if cacheKeyHash(wrong)&mask == cacheKeyHash(key)&mask {
			break
		}
	}
	if wrong.aux == 0 {
		t.Fatal("could not construct colliding routing key")
	}
	var wrongLease PageLease
	if cache.tryPinReady(cacheKeyHash(wrong), wrong, &wrongLease) {
		wrongLease.Release()
		t.Fatal("ready hit ignored PageRef routing metadata")
	}
}

func TestPageCacheFrameHintBypassesTableAndChecksExactIdentity(t *testing.T) {
	file, storeID, refs := newPageCacheFixture(t, 2)
	cache, err := NewPageCache(file, PageCacheOptions{
		PageSize: pageCacheTestPageSize, ResidentBytes: 2 * pageCacheTestPageSize,
		StoreID: storeID,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()

	var firstHint pageCacheFrameHint
	first, err := cache.acquireFrameHinted(refs[0], &firstHint)
	if err != nil || first.Payload()[0] != 1 {
		t.Fatalf("first hinted acquire = payload %v, %v", first.Payload(), err)
	}
	firstFrame := first.frame
	first.Release()
	if firstHint.packed.Load() == 0 {
		t.Fatal("first hinted acquire did not publish a frame")
	}

	firstKey, err := cache.validateRef(refs[0])
	if err != nil {
		t.Fatal(err)
	}
	firstHash := cacheKeyHash(firstKey)
	cache.mu.Lock()
	cache.removeLocked(firstHash, firstKey)
	cache.mu.Unlock()
	first, err = cache.acquireFrameHinted(refs[0], &firstHint)
	if err != nil || first.frame != firstFrame || first.Payload()[0] != 1 {
		t.Fatalf(
			"table-free hinted acquire = frame %d payload %v, %v",
			first.frame, first.Payload(), err,
		)
	}
	first.Release()
	cache.mu.Lock()
	cache.insertLocked(firstHash, firstFrame)
	cache.mu.Unlock()

	second, err := cache.Acquire(refs[1])
	if err != nil {
		t.Fatal(err)
	}
	secondFrame := second.frame
	second.Release()
	secondKey, err := cache.validateRef(refs[1])
	if err != nil {
		t.Fatal(err)
	}
	var forged pageCacheFrameHint
	forged.packed.Store(
		uint64(pageCacheKeyStamp(secondKey))<<32 |
			uint64(uint32(firstFrame)+1),
	)
	second, err = cache.acquireFrameHinted(refs[1], &forged)
	if err != nil || second.frame != secondFrame || second.Payload()[0] != 2 {
		t.Fatalf(
			"forged hinted acquire = frame %d payload %v, %v",
			second.frame, second.Payload(), err,
		)
	}
	second.Release()
	if got := int(uint32(forged.packed.Load())) - 1; got != secondFrame {
		t.Fatalf("refreshed forged hint frame = %d, want %d", got, secondFrame)
	}
}

func TestPageCachePrefetchOrderingAndHit(t *testing.T) {
	file, storeID, refs := newPageCacheFixture(t, 3)
	cache, err := NewPageCache(file, PageCacheOptions{
		PageSize: pageCacheTestPageSize, ResidentBytes: 3 * pageCacheTestPageSize,
		StoreID: storeID, PrefetchQueue: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := cache.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})

	if queued, err := cache.Prefetch([]PageRef{refs[1], refs[0]}); queued != 0 ||
		!errors.Is(err, ErrPageCacheReference) {
		t.Fatalf("unordered Prefetch = %d, %v", queued, err)
	}
	if queued, err := cache.Prefetch(refs[:2]); err != nil || queued != 2 {
		t.Fatalf("Prefetch = %d, %v", queued, err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for cache.Stats().PageReads < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if stats := cache.Stats(); stats.PageReads != 2 {
		t.Fatalf("prefetch did not complete: %+v", stats)
	}
	lease, err := cache.Acquire(refs[0])
	if err != nil {
		t.Fatal(err)
	}
	lease.Release()
	stats := cache.Stats()
	if stats.CacheHits != 1 || stats.PrefetchHits != 1 || stats.PrefetchQueued != 2 {
		t.Fatalf("unexpected prefetch stats: %+v", stats)
	}
}

func TestPageCacheIOUringPrefetchBatch(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("io_uring requires Linux")
	}
	file, storeID, refs := newPageCacheFixture(t, 8)
	cache, err := NewPageCache(file, PageCacheOptions{
		PageSize: pageCacheTestPageSize, ResidentBytes: 8 * pageCacheTestPageSize,
		StoreID: storeID, PrefetchQueue: 8, ReadConcurrency: 4,
		Backend: BackendIOUring,
	})
	if errors.Is(err, ErrUnavailable) || errors.Is(err, ErrUnsupported) ||
		errors.Is(err, syscall.EPERM) || errors.Is(err, syscall.ENOMEM) {
		t.Skip(err)
	}
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()
	if queued, err := cache.Prefetch(refs); err != nil || queued != len(refs) {
		t.Fatalf("Prefetch = (%d,%v), want (%d,nil)", queued, err, len(refs))
	}
	deadline := time.Now().Add(2 * time.Second)
	for cache.Stats().PageReads < uint64(len(refs)) && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	stats := cache.Stats()
	if stats.ReadBackend != BackendIOUring || stats.PageReads != uint64(len(refs)) ||
		stats.AsyncReadBatches == 0 || stats.LargestReadBatch < 2 || stats.ReadErrors != 0 {
		t.Fatalf("io_uring prefetch stats = %+v", stats)
	}
	for index, ref := range refs {
		lease, err := cache.Acquire(ref)
		if err != nil || lease.Payload()[0] != byte(index+1) {
			t.Fatalf("Acquire(%d) = (%v,%v)", index, lease.Payload(), err)
		}
		lease.Release()
	}
}

func TestPageCacheRejectsCorruptionAndShortRead(t *testing.T) {
	file, storeID, refs := newPageCacheFixture(t, 2)
	cache, err := NewPageCache(file, PageCacheOptions{
		PageSize: pageCacheTestPageSize, ResidentBytes: pageCacheTestPageSize,
		StoreID: storeID,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := cache.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})

	if _, err := file.WriteAt([]byte{0xff}, int64(refs[0].Offset+PageHeaderSize+3)); err != nil {
		t.Fatal(err)
	}
	if _, err := cache.Acquire(refs[0]); !errors.Is(err, ErrPageCorrupt) {
		t.Fatalf("corrupt Acquire error = %v, want %v", err, ErrPageCorrupt)
	}
	if err := file.Truncate(int64(refs[1].Offset + uint64(refs[1].Length)/2)); err != nil {
		t.Fatal(err)
	}
	if _, err := cache.Acquire(refs[1]); !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("short Acquire error = %v, want EOF", err)
	}
}

func TestPageCacheCloseRequiresReleasedLeases(t *testing.T) {
	file, storeID, refs := newPageCacheFixture(t, 1)
	cache, err := NewPageCache(file, PageCacheOptions{
		PageSize: pageCacheTestPageSize, ResidentBytes: pageCacheTestPageSize,
		StoreID: storeID,
	})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := cache.Acquire(refs[0])
	if err != nil {
		t.Fatal(err)
	}
	if err := cache.Close(); !errors.Is(err, ErrPageCachePinned) {
		t.Fatalf("Close with lease = %v, want %v", err, ErrPageCachePinned)
	}
	if _, err := cache.Acquire(refs[0]); !errors.Is(err, ErrPageCacheClosed) {
		t.Fatalf("Acquire while closing = %v, want %v", err, ErrPageCacheClosed)
	}
	lease.Release()
	if err := cache.Close(); err != nil {
		t.Fatal(err)
	}
	if err := cache.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPageCacheWarmAcquireSteadyAllocation(t *testing.T) {
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
	lease.Release()
	allocs := testing.AllocsPerRun(1000, func() {
		lease, acquireErr := cache.Acquire(refs[0])
		if acquireErr != nil {
			panic(acquireErr)
		}
		if lease.Payload()[0] != 1 {
			panic("wrong page")
		}
		lease.Release()
	})
	if allocs != 0 {
		t.Fatalf("warm Acquire/Release allocations = %v, want 0", allocs)
	}
}

func TestPageCacheDemandWaitsForSpeculativeAdmission(t *testing.T) {
	file, storeID, refs := newPageCacheFixture(t, 2)
	cache, err := NewPageCache(file, PageCacheOptions{
		PageSize: pageCacheTestPageSize, ResidentBytes: pageCacheTestPageSize,
		StoreID: storeID, Backend: BackendPortable,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()

	key, err := cache.validateRef(refs[0])
	if err != nil {
		t.Fatal(err)
	}
	cache.mu.Lock()
	frameIndex, ok := cache.reserveLocked(1)
	if !ok {
		t.Fatal("reserve speculative frame")
	}
	frame := &cache.frames[frameIndex]
	frame.lock.Lock()
	cache.beginExtentLocked(frameIndex, 1, key, cacheKeyHash(key))
	frame.flags |= pageCacheFramePrefetched
	cache.activeLoads++
	frame.lock.Unlock()
	page := cache.extentBytes(frameIndex, refs[0].Length)
	cache.mu.Unlock()
	n, err := file.ReadAt(page, int64(refs[0].Offset))
	if err != nil {
		t.Fatal(err)
	}

	type result struct {
		lease PageLease
		err   error
	}
	done := make(chan result, 1)
	go func() {
		lease, acquireErr := cache.Acquire(refs[1])
		done <- result{lease: lease, err: acquireErr}
	}()
	select {
	case got := <-done:
		got.lease.Release()
		t.Fatalf("demand returned before speculative completion: %v", got.err)
	case <-time.After(10 * time.Millisecond):
	}
	cache.completePrefetch(pageCacheRingLoad{
		ref: refs[0], key: key, frame: frameIndex,
	}, n, nil)
	select {
	case got := <-done:
		if got.err != nil || got.lease.Payload()[0] != 2 {
			t.Fatalf("demand after speculative completion = (%v,%v)", got.lease.Payload(), got.err)
		}
		got.lease.Release()
	case <-time.After(2 * time.Second):
		t.Fatal("demand did not resume after speculative completion")
	}
}

func TestPageCacheSpanAwareReservationEvictsOneColdWindow(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "store-page-cache-span-aware-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	storeID := [16]byte{19, 17, 13, 11, 7, 5, 3, 2, 1, 4, 6, 8, 10, 12, 14, 16}
	const (
		span  = 4
		zones = 10
	)
	cache, err := NewPageCache(file, PageCacheOptions{
		PageSize: pageCacheTestPageSize, MaxPageSize: span * pageCacheTestPageSize,
		ResidentBytes: zones * span * pageCacheTestPageSize,
		StoreID:       storeID,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()

	cache.mu.Lock()
	defer cache.mu.Unlock()
	nextID := uint64(2)
	admitReserved := func(index, reservedSpan int) pageCacheKey {
		key := pageCacheKey{
			offset:     nextID * pageCacheTestPageSize,
			logicalID:  nextID,
			generation: 1,
			length:     uint32(reservedSpan * pageCacheTestPageSize),
			kind:       PageOverflow,
		}
		nextID++
		frame := &cache.frames[index]
		frame.lock.Lock()
		cache.beginExtentLocked(index, reservedSpan, key, cacheKeyHash(key))
		frame.state = pageCacheReady
		frame.lock.Unlock()
		return key
	}
	admit := func(reservedSpan int) (int, pageCacheKey) {
		index, ok := cache.reserveLocked(reservedSpan)
		if !ok {
			t.Fatalf("reserve %d-frame extent", reservedSpan)
		}
		return index, admitReserved(index, reservedSpan)
	}
	reset := func(index int) {
		frame := &cache.frames[index]
		frame.lock.Lock()
		cache.resetExtentLocked(index)
		frame.lock.Unlock()
	}

	for range zones * span {
		admit(1)
	}
	// Preserve two interleaved four-frame extents while the other zones
	// contain singles. Resetting only one complete zone at a time makes the
	// allocator return that exact aligned span.
	for _, zone := range []int{4, 6} {
		start := zone * span
		for index := start; index < start+span; index++ {
			reset(index)
		}
		index, _ := admit(span)
		if index != start {
			t.Fatalf("four-frame extent start = %d, want %d", index, start)
		}
	}
	// Leave 24/40 slots resident. Every four-frame window retains at least one
	// extent, so a four-frame take cannot succeed without eviction.
	for _, index := range []int{
		3, 7, 11, 15,
		21, 22, 23,
		29, 30, 31,
		33, 34, 35,
		37, 38, 39,
	} {
		reset(index)
	}
	residentSlots := 0
	for index := 0; index < len(cache.frames); {
		frame := &cache.frames[index]
		if frame.state == pageCacheReady {
			extentSpan := int(frame.reservationSpan)
			residentSlots += extentSpan
			index += extentSpan
		} else {
			index++
		}
	}
	if residentSlots != 24 {
		t.Fatalf("resident slots = %d, want 24/40", residentSlots)
	}

	hotKeys := make([]pageCacheKey, 0, 3)
	for index := range 3 {
		frame := &cache.frames[index]
		frame.referenced = true
		frame.hits = 100
		hotKeys = append(hotKeys, frame.key)
	}
	// The next two windows each expose one cold frame to the old frame-at-a-
	// time clock before two referenced frames. The fourth window is uniformly
	// cold. A linear clock walk evicts five frames before it coalesces a span;
	// window scoring chooses the three-frame cold window directly.
	for _, index := range []int{5, 6, 9, 10} {
		cache.frames[index].referenced = true
		cache.frames[index].hits = 10
	}
	cache.hand = 0

	for reservation := range 4 {
		before := cache.evictions
		index, ok := cache.reserveLocked(span)
		if !ok {
			t.Fatalf("reservation %d failed", reservation)
		}
		if evicted := cache.evictions - before; evicted > span {
			t.Fatalf("reservation %d evicted %d frames, want at most %d",
				reservation, evicted, span)
		}
		if reservation == 0 && index != 4*span {
			t.Fatalf("first reservation chose window %d, want matching large-class window %d",
				index/span, 4)
		}
		admitReserved(index, span)
	}
	for _, key := range hotKeys {
		index, ok := cache.lookupLocked(cacheKeyHash(key), key)
		if !ok || cache.frames[index].state != pageCacheReady {
			t.Fatalf("hot single-frame extent %+v was evicted", key)
		}
	}
}

func TestPageCacheExactSpanEvictionSlidesWithinZone(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "store-page-cache-sliding-window-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	const zoneSlots = 16
	cache, err := NewPageCache(file, PageCacheOptions{
		PageSize: pageCacheTestPageSize, MaxPageSize: zoneSlots * pageCacheTestPageSize,
		ResidentBytes: zoneSlots * pageCacheTestPageSize,
		StoreID:       [16]byte{31, 29, 23, 19, 17, 13, 11, 7, 5, 3, 2, 1, 4, 6, 8, 10},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()

	cache.mu.Lock()
	defer cache.mu.Unlock()
	spans := [...]int{3, 5, 3, 5}
	keys := [len(spans)]pageCacheKey{}
	starts := [len(spans)]int{}
	nextID := uint64(2)
	for i, span := range spans {
		index, ok := cache.reserveLocked(span)
		if !ok {
			t.Fatalf("reserve initial span %d", span)
		}
		starts[i] = index
		key := pageCacheKey{
			offset: nextID * pageCacheTestPageSize, logicalID: nextID, generation: 1,
			length: uint32(span * pageCacheTestPageSize), kind: PageOverflow,
		}
		nextID++
		keys[i] = key
		frame := &cache.frames[index]
		frame.lock.Lock()
		cache.beginExtentLocked(index, span, key, cacheKeyHash(key))
		frame.state = pageCacheReady
		frame.lock.Unlock()
	}
	if starts != [len(spans)]int{0, 3, 8, 11} {
		t.Fatalf("initial exact placement = %v, want [0 3 8 11]", starts)
	}
	cache.frames[starts[3]].referenced = true
	cache.frames[starts[3]].hits = 100
	cache.hand = 0

	before := cache.evictions
	index, ok := cache.reserveLocked(5)
	if !ok || index != 3 {
		t.Fatalf("sliding five-frame reserve = (%d,%v), want (3,true)", index, ok)
	}
	if got := cache.evictions - before; got != 1 {
		t.Fatalf("sliding five-frame reserve evicted %d extents, want one", got)
	}
	if _, ok := cache.lookupLocked(cacheKeyHash(keys[1]), keys[1]); ok {
		t.Fatal("cold five-frame extent remained resident")
	}
	for _, keyIndex := range []int{0, 2, 3} {
		if resident, ok := cache.lookupLocked(cacheKeyHash(keys[keyIndex]), keys[keyIndex]); !ok ||
			cache.frames[resident].state != pageCacheReady {
			t.Fatalf("extent %d was unexpectedly evicted", keyIndex)
		}
	}
}

func TestPageCacheSegregatedZonesRecycleLargeChurnWithoutEviction(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "store-page-cache-zone-classes-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	storeID := [16]byte{23, 19, 17, 13, 11, 7, 5, 3, 2, 1, 4, 6, 8, 10, 12, 14}
	const (
		zoneSlots     = 16
		zones         = 20
		smallExtents  = 192
		largeSpan     = 8
		largeExtents  = 16
		residentSlots = 192
		churnCycles   = 3000
	)
	cache, err := NewPageCache(file, PageCacheOptions{
		PageSize: pageCacheTestPageSize, MaxPageSize: zoneSlots * pageCacheTestPageSize,
		ResidentBytes: zones * zoneSlots * pageCacheTestPageSize,
		StoreID:       storeID, ReadConcurrency: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()

	type admittedExtent struct {
		ref   PageRef
		index int
	}
	nextID := uint64(2)
	nextOffset := cache.dataStart
	admit := func(span int) admittedExtent {
		t.Helper()
		ref := PageRef{
			Offset: nextOffset, LogicalID: nextID, Generation: 1,
			Length: uint32(span * pageCacheTestPageSize), Kind: PageOverflow,
		}
		nextID++
		nextOffset += uint64(ref.Length)
		key, validateErr := cache.validateRef(ref)
		if validateErr != nil {
			t.Fatalf("validate generated ref: %v", validateErr)
		}
		cache.mu.Lock()
		index, ok := cache.reserveLocked(span)
		if !ok {
			cache.mu.Unlock()
			t.Fatalf("reserve %d-frame extent", span)
		}
		frame := &cache.frames[index]
		frame.lock.Lock()
		cache.beginExtentLocked(index, span, key, cacheKeyHash(key))
		frame.state = pageCacheReady
		frame.lock.Unlock()
		cache.mu.Unlock()
		return admittedExtent{ref: ref, index: index}
	}

	small := make([]admittedExtent, 0, smallExtents)
	large := make([]admittedExtent, 0, largeExtents)
	// Alternate the two sizes while both remain, reproducing the placement
	// pressure that mixed size classes in one arena. Class lists still pack
	// each size into its own zones.
	for len(small) < smallExtents || len(large) < largeExtents {
		if len(small) < smallExtents {
			small = append(small, admit(1))
		}
		if len(large) < largeExtents {
			large = append(large, admit(largeSpan))
		}
	}

	smallByZone := make([][]admittedExtent, zones)
	for _, extent := range small {
		zone := extent.index / zoneSlots
		smallByZone[zone] = append(smallByZone[zone], extent)
	}
	largeByZone := make([][]admittedExtent, zones)
	for _, extent := range large {
		zone := extent.index / zoneSlots
		largeByZone[zone] = append(largeByZone[zone], extent)
	}
	smallZones, largeZones := 0, 0
	for zone, class := range cache.blocks.zoneClasses {
		switch class {
		case pageCacheBlockSmall:
			smallZones++
			if len(smallByZone[zone]) != zoneSlots || len(largeByZone[zone]) != 0 {
				t.Fatalf("small zone %d contains %d small and %d large extents",
					zone, len(smallByZone[zone]), len(largeByZone[zone]))
			}
		case pageCacheBlockLarge:
			largeZones++
			if len(smallByZone[zone]) != 0 || len(largeByZone[zone]) != 2 {
				t.Fatalf("large zone %d contains %d small and %d large extents",
					zone, len(smallByZone[zone]), len(largeByZone[zone]))
			}
		default:
			t.Fatalf("full arena left zone %d unassigned", zone)
		}
	}
	if smallZones != 12 || largeZones != 8 {
		t.Fatalf("zone classes = %d small, %d large; want 12 and 8",
			smallZones, largeZones)
	}

	// Retain at least one single-frame extent in every small zone while
	// reducing total residency to 60%. Retain one eight-frame extent in every
	// large zone, leaving one churn reservation per large zone.
	unreachable := make([]PageRef, 0, 72)
	longLivedSmall := make([]admittedExtent, 0, 128)
	smallToRelease := 64
	activeLarge := make([]admittedExtent, 0, largeZones)
	for zone := range zones {
		group := smallByZone[zone]
		release := min(smallToRelease, max(0, len(group)-1))
		for _, extent := range group[:release] {
			unreachable = append(unreachable, extent.ref)
		}
		longLivedSmall = append(longLivedSmall, group[release:]...)
		smallToRelease -= release
		if group = largeByZone[zone]; len(group) != 0 {
			unreachable = append(unreachable, group[0].ref)
			activeLarge = append(activeLarge, group[1])
		}
	}
	if smallToRelease != 0 || len(longLivedSmall) != 128 || len(activeLarge) != 8 {
		t.Fatalf("resident setup = release %d, %d small, %d large",
			smallToRelease, len(longLivedSmall), len(activeLarge))
	}
	cache.MarkUnreachable(unreachable)
	if stats := cache.Stats(); stats.ResidentBytes != residentSlots*pageCacheTestPageSize ||
		stats.Evictions != 0 {
		t.Fatalf("60%% resident setup stats = %+v", stats)
	}

	baselineEvictions := cache.Stats().Evictions
	oneUnreachable := make([]PageRef, 1)
	for cycle := range churnCycles {
		replacement := admit(largeSpan)
		slot := cycle % len(activeLarge)
		oneUnreachable[0] = activeLarge[slot].ref
		cache.MarkUnreachable(oneUnreachable)
		activeLarge[slot] = replacement
	}
	stats := cache.Stats()
	if got := stats.Evictions - baselineEvictions; got != 0 {
		t.Fatalf("%d large reservations caused %d evictions, want 0",
			churnCycles, got)
	}
	if stats.CrossClassTakes != 0 {
		t.Fatalf("steady segregated workload used %d cross-class takes",
			stats.CrossClassTakes)
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	for _, extent := range longLivedSmall {
		key, validateErr := cache.validateRef(extent.ref)
		if validateErr != nil {
			t.Fatal(validateErr)
		}
		index, ok := cache.lookupLocked(cacheKeyHash(key), key)
		if !ok || cache.frames[index].state != pageCacheReady {
			t.Fatalf("long-lived single-frame extent %+v was evicted", extent.ref)
		}
	}
}

func TestPageCacheFiveFrameResidencyHasNoReservationSlackOrChurnEvictions(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "store-page-cache-five-frame-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	const (
		zoneSlots   = 16
		zones       = 20
		span        = 5
		extentCount = 59
		churnCycles = 1000
	)
	storeID := [16]byte{29, 23, 19, 17, 13, 11, 7, 5, 3, 2, 1, 4, 6, 8, 10, 12}
	cache, err := NewPageCache(file, PageCacheOptions{
		PageSize: pageCacheTestPageSize, MaxPageSize: zoneSlots * pageCacheTestPageSize,
		ResidentBytes: zones * zoneSlots * pageCacheTestPageSize,
		StoreID:       storeID, ReadConcurrency: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()

	nextID := uint64(2)
	nextOffset := cache.dataStart
	admit := func() PageRef {
		t.Helper()
		ref := PageRef{
			Offset: nextOffset, LogicalID: nextID, Generation: 1,
			Length: span * pageCacheTestPageSize, Kind: PageOverflow,
		}
		nextID++
		nextOffset += uint64(ref.Length)
		key, validateErr := cache.validateRef(ref)
		if validateErr != nil {
			t.Fatalf("validate generated ref: %v", validateErr)
		}
		cache.mu.Lock()
		index, ok := cache.reserveLocked(span)
		if !ok {
			cache.mu.Unlock()
			t.Fatal("reserve five-frame extent")
		}
		frame := &cache.frames[index]
		frame.lock.Lock()
		cache.beginExtentLocked(index, span, key, cacheKeyHash(key))
		frame.state = pageCacheReady
		frame.lock.Unlock()
		cache.mu.Unlock()
		return ref
	}

	active := make([]PageRef, extentCount)
	for i := range active {
		active[i] = admit()
	}
	logicalBytes := uint64(extentCount * span * pageCacheTestPageSize)
	stats := cache.Stats()
	if logicalBytes*10 <= stats.CapacityBytes*9 {
		t.Fatalf("logical residency = %d/%d, want above 90%%",
			logicalBytes, stats.CapacityBytes)
	}
	if stats.ResidentBytes != logicalBytes || stats.ReservedBytes != logicalBytes {
		t.Fatalf("five-frame reservation accounting = %+v, want resident=reserved=%d",
			stats, logicalBytes)
	}

	baselineEvictions := stats.Evictions
	unreachable := make([]PageRef, 1)
	for cycle := range churnCycles {
		replacement := admit()
		slot := cycle % len(active)
		unreachable[0] = active[slot]
		cache.MarkUnreachable(unreachable)
		active[slot] = replacement
	}
	stats = cache.Stats()
	if stats.ResidentBytes != logicalBytes || stats.ReservedBytes != logicalBytes {
		t.Fatalf("post-churn reservation accounting = %+v, want resident=reserved=%d",
			stats, logicalBytes)
	}
	if got := stats.Evictions - baselineEvictions; got != 0 {
		t.Fatalf("%d five-frame churn cycles caused %d evictions, want zero",
			churnCycles, got)
	}
}

func TestPageCacheVariableDocumentExtent(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "store-page-cache-variable-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })
	storeID := [16]byte{9, 7, 5, 3, 1, 2, 4, 6, 8, 10, 12, 14, 16, 15, 13, 11}
	const extentSize = 2 * pageCacheTestPageSize
	page := make([]byte, extentSize)
	payload, err := InitPage(page, PageHeader{
		StoreID: storeID, Generation: 3, LogicalID: 7,
		PageSize: extentSize, PayloadLength: 17, Kind: PageOverflow,
	})
	if err != nil {
		t.Fatal(err)
	}
	copy(payload, "variable document")
	if _, err := SealPage(page); err != nil {
		t.Fatal(err)
	}
	ref := PageRef{
		Offset: 4 * pageCacheTestPageSize, LogicalID: 7, Generation: 3,
		Length: extentSize, Kind: PageOverflow,
	}
	if _, err := file.WriteAt(page, int64(ref.Offset)); err != nil {
		t.Fatal(err)
	}
	cache, err := NewPageCache(file, PageCacheOptions{
		PageSize: pageCacheTestPageSize, MaxPageSize: extentSize,
		ResidentBytes: extentSize, StoreID: storeID, ReadConcurrency: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := cache.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	lease, err := cache.Acquire(ref)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(lease.Payload()); got != "variable document" {
		t.Fatalf("Payload = %q", got)
	}
	if got := lease.Header().PageSize; got != extentSize {
		t.Fatalf("PageSize = %d, want %d", got, extentSize)
	}
	lease.Release()

	metadata := ref
	metadata.Kind = PageIndexPosting
	if _, err := cache.Acquire(metadata); !errors.Is(err, ErrPageCacheReference) {
		t.Fatalf("oversize metadata error = %v, want %v", err, ErrPageCacheReference)
	}
	for _, kind := range []PageKind{
		PagePrimaryCatalog,
		PageTabletDirectory,
		PagePrimaryLocator,
		PageTabletRoute,
		PagePrimaryAnchor,
		PagePrimaryLeaf,
	} {
		hybrid := ref
		hybrid.Kind = kind
		if _, err := cache.validateRef(hybrid); err != nil {
			t.Fatalf("variable hybrid kind %d rejected: %v", kind, err)
		}
	}
	stats := cache.Stats()
	if stats.CapacityBytes != extentSize || stats.ResidentBytes != extentSize ||
		stats.PageReads != 1 || stats.ReadBytes != extentSize {
		t.Fatalf("unexpected variable-extent stats: %+v", stats)
	}
}

func TestPageCacheNonPowerOfTwoDocumentExtentDemandAndEviction(t *testing.T) {
	tests := [...]struct {
		name         string
		logicalPages int
	}{
		{name: "12KiB", logicalPages: 3},
		{name: "20KiB", logicalPages: 5},
		{name: "28KiB", logicalPages: 7},
		{name: "60KiB", logicalPages: 15},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			file, err := os.CreateTemp(t.TempDir(), "store-page-cache-non-power-*")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = file.Close() })
			storeID := [16]byte{11, 7, 5, 3, 1, 2, 4, 6, 8, 10, 12, 14, 16, 15, 13, 9}
			length := uint32(test.logicalPages * pageCacheTestPageSize)
			zonePages := nextPowerOfTwo(test.logicalPages)
			if _, initErr := InitPage(make([]byte, length), PageHeader{
				StoreID: storeID, Generation: 1, LogicalID: 99,
				PageSize: length, PayloadLength: 32, Kind: PageIndexPosting,
			}); !errors.Is(initErr, ErrInvalidWrite) {
				t.Fatalf("non-power metadata InitPage = %v, want %v", initErr, ErrInvalidWrite)
			}
			refs := make([]PageRef, 3)
			offset := uint64(4 * pageCacheTestPageSize)
			for i := range refs {
				page := make([]byte, length)
				payload, initErr := InitPage(page, PageHeader{
					StoreID: storeID, Generation: 1, LogicalID: uint64(i + 2),
					PageSize: length, PayloadLength: 32, Kind: PageOverflow,
				})
				if initErr != nil {
					t.Fatal(initErr)
				}
				payload[0] = byte(i + 1)
				if _, sealErr := SealPage(page); sealErr != nil {
					t.Fatal(sealErr)
				}
				if _, writeErr := file.WriteAt(page, int64(offset)); writeErr != nil {
					t.Fatal(writeErr)
				}
				refs[i] = PageRef{
					Offset: offset, LogicalID: uint64(i + 2), Generation: 1,
					Length: length, Kind: PageOverflow,
				}
				offset += uint64(length)
			}

			zoneBytes := zonePages * pageCacheTestPageSize
			cache, err := NewPageCache(file, PageCacheOptions{
				PageSize: pageCacheTestPageSize, MaxPageSize: zoneBytes,
				ResidentBytes: int64(2 * zoneBytes), StoreID: storeID,
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

			first, err := cache.Acquire(refs[0])
			if err != nil || len(first.Page()) != int(length) || first.Payload()[0] != 1 {
				t.Fatalf("first Acquire = (%d,%v,%v)", len(first.Page()), first.Payload(), err)
			}
			second, err := cache.Acquire(refs[1])
			if err != nil || len(second.Page()) != int(length) || second.Payload()[0] != 2 {
				t.Fatalf("second Acquire = (%d,%v,%v)", len(second.Page()), second.Payload(), err)
			}
			if _, err := cache.Acquire(refs[2]); !errors.Is(err, ErrPageCachePinned) {
				t.Fatalf("fully pinned third Acquire = %v, want %v", err, ErrPageCachePinned)
			}
			first.Release()
			third, err := cache.Acquire(refs[2])
			if err != nil || len(third.Page()) != int(length) || third.Payload()[0] != 3 {
				t.Fatalf("third Acquire = (%d,%v,%v)", len(third.Page()), third.Payload(), err)
			}
			second.Release()
			third.Release()

			ready := 0
			cache.mu.Lock()
			for i := range cache.frames {
				frame := &cache.frames[i]
				if frame.state == pageCacheReady {
					ready++
					if int(frame.reservationSpan) != test.logicalPages ||
						frame.key.length != length {
						cache.mu.Unlock()
						t.Fatalf(
							"head reservation = %d pages for %d-byte extent",
							frame.reservationSpan, frame.key.length,
						)
					}
				}
			}
			cache.mu.Unlock()
			if ready != 2 {
				t.Fatalf("ready extents = %d, want 2", ready)
			}

			stats := cache.Stats()
			if stats.CapacityBytes != uint64(2*zoneBytes) ||
				stats.ResidentBytes != 2*uint64(length) ||
				stats.ReservedBytes != stats.ResidentBytes ||
				stats.ReadyFrames != 2 || stats.PinnedPages != 0 ||
				stats.PageReads != 3 || stats.ReadBytes != 3*uint64(length) ||
				stats.Evictions != 1 {
				t.Fatalf("non-power extent stats = %+v", stats)
			}

			metadata := refs[2]
			metadata.Kind = PageIndexPosting
			if _, err := cache.Acquire(metadata); !errors.Is(err, ErrPageCacheReference) {
				t.Fatalf("non-power metadata error = %v, want %v", err, ErrPageCacheReference)
			}
		})
	}
}

func TestPageCacheNonPowerOfTwoDirtyReservationAccounting(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "store-page-cache-non-power-dirty-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	storeID := [16]byte{13, 7, 5, 3, 1, 2, 4, 6, 8, 10, 12, 14, 16, 15, 11, 9}
	const length = 3 * pageCacheTestPageSize
	page := make([]byte, length)
	payload, err := InitPage(page, PageHeader{
		StoreID: storeID, Generation: 1, LogicalID: 2,
		PageSize: length, PayloadLength: 32, Kind: PageOverflow,
	})
	if err != nil {
		t.Fatal(err)
	}
	payload[0] = 1
	if _, err := SealPage(page); err != nil {
		t.Fatal(err)
	}
	ref := PageRef{
		Offset: 4 * pageCacheTestPageSize, LogicalID: 2, Generation: 1,
		Length: length, Kind: PageOverflow,
	}
	cache, err := NewPageCache(file, PageCacheOptions{
		PageSize: pageCacheTestPageSize, MaxPageSize: 4 * pageCacheTestPageSize,
		ResidentBytes: 8 * pageCacheTestPageSize, StoreID: storeID,
		ReadConcurrency: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()

	if err := cache.AdmitDirty(ref, page, 2); err != nil {
		t.Fatal(err)
	}
	stats := cache.Stats()
	if stats.ResidentBytes != length || stats.ReservedBytes != 3*pageCacheTestPageSize ||
		stats.DirtyBytes != length ||
		cache.DirtyCapacityAvailable() != 5*pageCacheTestPageSize {
		t.Fatalf("dirty non-power stats = %+v, available=%d", stats, cache.DirtyCapacityAvailable())
	}
	cache.MarkDurable(1)
	if stats := cache.Stats(); stats.DirtyBytes != length ||
		cache.DirtyCapacityAvailable() != 5*pageCacheTestPageSize {
		t.Fatalf("early durability stats = %+v, available=%d", stats, cache.DirtyCapacityAvailable())
	}
	cache.MarkDurable(2)
	if stats := cache.Stats(); stats.DirtyBytes != 0 ||
		cache.DirtyCapacityAvailable() != 8*pageCacheTestPageSize {
		t.Fatalf("durable stats = %+v, available=%d", stats, cache.DirtyCapacityAvailable())
	}
	if !cache.Invalidate(ref) {
		t.Fatal("Invalidate durable non-power extent")
	}
	if stats := cache.Stats(); stats.ResidentBytes != 0 || stats.ReservedBytes != 0 {
		t.Fatalf("invalidated non-power stats = %+v", stats)
	}
}

func TestPageCacheMixedNonPowerReservationSymmetry(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "store-page-cache-mixed-reservations-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })
	storeID := [16]byte{17, 7, 5, 3, 1, 2, 4, 6, 8, 10, 12, 14, 16, 15, 11, 9}
	logicalPages := [...]int{3, 5, 7, 15, 16}
	reservedPages := logicalPages
	refs := make([]PageRef, len(logicalPages))
	pages := make([][]byte, len(logicalPages))
	offset := uint64(4 * pageCacheTestPageSize)
	for i, span := range logicalPages {
		length := uint32(span * pageCacheTestPageSize)
		page := make([]byte, length)
		payload, initErr := InitPage(page, PageHeader{
			StoreID: storeID, Generation: 1, LogicalID: uint64(i + 2),
			PageSize: length, PayloadLength: 32, Kind: PageOverflow,
		})
		if initErr != nil {
			t.Fatal(initErr)
		}
		payload[0] = byte(i + 1)
		if _, sealErr := SealPage(page); sealErr != nil {
			t.Fatal(sealErr)
		}
		if _, writeErr := file.WriteAt(page, int64(offset)); writeErr != nil {
			t.Fatal(writeErr)
		}
		refs[i] = PageRef{
			Offset: offset, LogicalID: uint64(i + 2), Generation: 1,
			Length: length, Kind: PageOverflow,
		}
		pages[i] = page
		offset += uint64(length)
	}

	const capacityPages = 32
	cache, err := NewPageCache(file, PageCacheOptions{
		PageSize: pageCacheTestPageSize, MaxPageSize: 16 * pageCacheTestPageSize,
		ResidentBytes: capacityPages * pageCacheTestPageSize, StoreID: storeID,
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

	for i, ref := range refs {
		want := uint64(reservedPages[i] * pageCacheTestPageSize)
		if got := cache.ReservationBytes(ref.Length); got != want {
			t.Fatalf("ReservationBytes(%d pages) = %d, want %d", logicalPages[i], got, want)
		}
	}
	if cache.ReservationBytes(0) != 0 ||
		cache.ReservationBytes(3*pageCacheTestPageSize+1) != 0 ||
		cache.ReservationBytes(17*pageCacheTestPageSize) != 0 {
		t.Fatal("ReservationBytes accepted zero, unaligned, or oversized length")
	}
	var nilCache *PageCache
	if nilCache.ReservationBytes(3*pageCacheTestPageSize) != 0 {
		t.Fatal("nil cache returned a reservation")
	}
	if allocs := testing.AllocsPerRun(1000, func() {
		if cache.ReservationBytes(15*pageCacheTestPageSize) != 15*pageCacheTestPageSize {
			panic("wrong reservation")
		}
	}); allocs != 0 {
		t.Fatalf("ReservationBytes allocations = %v, want 0", allocs)
	}

	for i := range 3 {
		lease, acquireErr := cache.Acquire(refs[i])
		if acquireErr != nil || lease.Payload()[0] != byte(i+1) {
			t.Fatalf("mixed Acquire(%d) = (%v,%v)", i, lease.Payload(), acquireErr)
		}
		lease.Release()
	}
	if stats := cache.Stats(); stats.ResidentBytes != 15*pageCacheTestPageSize ||
		stats.ReservedBytes != 15*pageCacheTestPageSize {
		t.Fatalf("initial mixed stats = %+v", stats)
	}
	large, err := cache.Acquire(refs[3])
	if err != nil || large.Payload()[0] != 4 {
		t.Fatalf("evicting mixed Acquire = (%v,%v)", large.Payload(), err)
	}
	large.Release()
	if stats := cache.Stats(); stats.Evictions == 0 {
		t.Fatalf("15-page demand did not evict mixed reservations: %+v", stats)
	}
	for i := range 4 {
		cache.Invalidate(refs[i])
	}
	if stats := cache.Stats(); stats.ResidentBytes != 0 || stats.ReservedBytes != 0 {
		t.Fatalf("mixed demand reservations did not return to zero: %+v", stats)
	}

	for i := range 3 {
		if err := cache.AdmitDirty(refs[i], pages[i], 2); err != nil {
			t.Fatalf("AdmitDirty(%d): %v", i, err)
		}
	}
	if stats := cache.Stats(); stats.DirtyBytes != 15*pageCacheTestPageSize ||
		stats.ReservedBytes != 15*pageCacheTestPageSize ||
		cache.DirtyCapacityAvailable() != 17*pageCacheTestPageSize {
		t.Fatalf("mixed dirty stats = %+v, available=%d", stats, cache.DirtyCapacityAvailable())
	}
	cache.MarkDurable(2)
	for i := range 3 {
		if !cache.Invalidate(refs[i]) {
			t.Fatalf("Invalidate durable mixed extent %d", i)
		}
	}
	if stats := cache.Stats(); stats.DirtyBytes != 0 ||
		stats.ResidentBytes != 0 || stats.ReservedBytes != 0 {
		t.Fatalf("mixed dirty reservations did not return to zero: %+v", stats)
	}

	full, err := cache.Acquire(refs[4])
	if err != nil || len(full.Page()) != 16*pageCacheTestPageSize ||
		full.Payload()[0] != 5 {
		t.Fatalf("full-zone Acquire = (%d,%v,%v)", len(full.Page()), full.Payload(), err)
	}
	full.Release()
	if stats := cache.Stats(); stats.ReservedBytes != 16*pageCacheTestPageSize {
		t.Fatalf("full-zone reservation stats = %+v", stats)
	}
	if !cache.Invalidate(refs[4]) {
		t.Fatal("Invalidate full-zone extent")
	}
	if stats := cache.Stats(); stats.ResidentBytes != 0 || stats.ReservedBytes != 0 {
		t.Fatalf("full-zone reservation did not return to zero: %+v", stats)
	}
}

func TestPageCachePacksMetadataAndVariableExtentByQuantum(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "store-page-cache-packed-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })
	storeID := [16]byte{4, 2, 8, 1, 6, 3, 7, 5, 9, 10, 11, 12, 13, 14, 15, 16}
	refs := make([]PageRef, 0, 5)
	offset := uint64(4 * pageCacheTestPageSize)
	for pageID := range 5 {
		length := uint32(pageCacheTestPageSize)
		kind := PageIndexPosting
		if pageID == 4 {
			length = 4 * pageCacheTestPageSize
			kind = PageOverflow
		}
		page := make([]byte, length)
		payload, initErr := InitPage(page, PageHeader{
			StoreID: storeID, Generation: 1, LogicalID: uint64(pageID + 2),
			PageSize: length, PayloadLength: 32, Kind: kind,
		})
		if initErr != nil {
			t.Fatal(initErr)
		}
		payload[0] = byte(pageID + 1)
		if _, sealErr := SealPage(page); sealErr != nil {
			t.Fatal(sealErr)
		}
		if _, writeErr := file.WriteAt(page, int64(offset)); writeErr != nil {
			t.Fatal(writeErr)
		}
		refs = append(refs, PageRef{
			Offset: offset, LogicalID: uint64(pageID + 2), Generation: 1,
			Length: length, Kind: kind,
		})
		offset += uint64(length)
	}

	const resident = 8 * pageCacheTestPageSize
	cache, err := NewPageCache(file, PageCacheOptions{
		PageSize: pageCacheTestPageSize, MaxPageSize: 4 * pageCacheTestPageSize,
		ResidentBytes: resident, StoreID: storeID, ReadConcurrency: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := cache.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	leases := make([]PageLease, len(refs))
	for index, ref := range refs {
		leases[index], err = cache.Acquire(ref)
		if err != nil || leases[index].Payload()[0] != byte(index+1) {
			t.Fatalf("Acquire(%d) = (%v,%v)", index, leases[index].Payload(), err)
		}
	}
	stats := cache.Stats()
	if stats.CapacityBytes != resident || stats.ResidentBytes != resident ||
		stats.Frames != 8 || stats.ReadyFrames != 5 || stats.PinnedPages != 5 {
		t.Fatalf("packed stats = %+v", stats)
	}
	for index := range leases {
		leases[index].Release()
	}
}

func TestPageCacheFrameControlIsPointerFree(t *testing.T) {
	frameType := reflect.TypeFor[pageCacheFrame]()
	var visit func(reflect.Type) bool
	visit = func(typ reflect.Type) bool {
		switch typ.Kind() {
		case reflect.Array:
			return visit(typ.Elem())
		case reflect.Struct:
			for field := range typ.NumField() {
				if !visit(typ.Field(field).Type) {
					return false
				}
			}
			return true
		case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map,
			reflect.Pointer, reflect.Slice, reflect.String, reflect.UnsafePointer:
			return false
		default:
			return true
		}
	}
	if !visit(frameType) {
		t.Fatalf("pageCacheFrame contains GC-visible pointer-bearing state: %v", frameType)
	}
	if frameType.Size() > 64 {
		t.Fatalf("pageCacheFrame is %d bytes, want at most one cache line", frameType.Size())
	}
	loadType := reflect.TypeFor[pageCacheRingLoad]()
	if !visit(loadType) {
		t.Fatalf("pageCacheRingLoad contains GC-visible pointer-bearing state: %v", loadType)
	}
	linkType := reflect.TypeFor[pageCacheBlockLink]()
	if !visit(linkType) {
		t.Fatalf("pageCacheBlockLink contains GC-visible pointer-bearing state: %v", linkType)
	}
	if linkType.Size() != 12 {
		t.Fatalf("pageCacheBlockLink is %d bytes, want 12", linkType.Size())
	}
}

func TestPageCacheBlockAllocatorExactSpanRoundTrips(t *testing.T) {
	for span := 1; span <= 16; span++ {
		blocks := newPageCacheBlocks(16, 16)
		index, ok := blocks.take(span)
		if !ok || index != 0 {
			t.Fatalf("take(%d) = (%d,%v), want (0,true)", span, index, ok)
		}
		if got, want := blocks.zoneClasses[0], pageCacheBlockClassForSpan(span); got != want {
			t.Fatalf("take(%d) zone class = %d, want %d", span, got, want)
		}
		blocks.put(index, span)
		if got := blocks.zoneClasses[0]; got != pageCacheBlockUnassigned {
			t.Fatalf("put(%d) zone class = %d, want unassigned", span, got)
		}
		if got := blocks.links[0].span; got != 16 {
			t.Fatalf("put(%d) restored run length %d, want 16", span, got)
		}
	}

	partial := newPageCacheBlocks(37, 16)
	index, ok := partial.take(5)
	if !ok || index != 32 {
		t.Fatalf("partial-zone take = (%d,%v), want (32,true)", index, ok)
	}
	partial.put(index, 5)
	if got := partial.zoneClasses[2]; got != pageCacheBlockUnassigned {
		t.Fatalf("partial zone reset class = %d, want unassigned", got)
	}
	if got := partial.links[32].span; got != 5 {
		t.Fatalf("partial zone restored run length %d, want 5", got)
	}
}

func TestPageCacheBlockAllocatorFilesExactSplitRemainders(t *testing.T) {
	blocks := newPageCacheBlocks(16, 16)
	first, ok := blocks.take(5)
	if !ok || first != 0 {
		t.Fatalf("take(5) = (%d,%v), want (0,true)", first, ok)
	}
	if got := blocks.links[5].span; got != 11 {
		t.Fatalf("first remainder length = %d, want 11", got)
	}
	if got := blocks.head(pageCacheBlockLarge, 11); got != 5 {
		t.Fatalf("11-run head = %d, want 5", got)
	}
	second, ok := blocks.take(6)
	if !ok || second != 5 {
		t.Fatalf("take(6) = (%d,%v), want (5,true)", second, ok)
	}
	if got := blocks.links[11].span; got != 5 {
		t.Fatalf("second remainder length = %d, want 5", got)
	}
	third, ok := blocks.take(5)
	if !ok || third != 11 {
		t.Fatalf("take remainder = (%d,%v), want (11,true)", third, ok)
	}
	if _, ok := blocks.take(1); ok {
		t.Fatal("take from exhausted allocator succeeded")
	}
}

func TestPageCacheBlockAllocatorThreeWayCoalescing(t *testing.T) {
	blocks := newPageCacheBlocks(16, 16)
	allocated := [4]int{}
	for i := range allocated {
		var ok bool
		allocated[i], ok = blocks.take(4)
		if !ok || allocated[i] != i*4 {
			t.Fatalf("take four at %d = (%d,%v)", i, allocated[i], ok)
		}
	}
	blocks.put(allocated[0], 4)
	blocks.put(allocated[2], 4)
	blocks.put(allocated[1], 4)
	if got := blocks.links[0].span; got != 12 {
		t.Fatalf("three-way merged run length = %d, want 12", got)
	}
	index, ok := blocks.take(12)
	if !ok || index != 0 {
		t.Fatalf("take merged run = (%d,%v), want (0,true)", index, ok)
	}
	blocks.put(index, 12)
	blocks.put(allocated[3], 4)
	if got := blocks.zoneClasses[0]; got != pageCacheBlockUnassigned {
		t.Fatalf("fully coalesced zone class = %d, want unassigned", got)
	}
	if got := blocks.links[0].span; got != 16 {
		t.Fatalf("fully coalesced run length = %d, want 16", got)
	}
}

func TestPageCacheBlockAllocatorAssignsFallsBackAndResetsZones(t *testing.T) {
	blocks := newPageCacheBlocks(32, 16)
	index, ok := blocks.take(3)
	if !ok {
		t.Fatal("first small take failed")
	}
	zone := index / int(blocks.zoneSlots)
	if got := blocks.zoneClasses[zone]; got != pageCacheBlockSmall {
		t.Fatalf("first take zone class = %d, want small", got)
	}
	if other := zone ^ 1; blocks.zoneClasses[other] != pageCacheBlockUnassigned {
		t.Fatalf("untouched zone class = %d, want unassigned", blocks.zoneClasses[other])
	}
	blocks.put(index, 3)
	if got := blocks.zoneClasses[zone]; got != pageCacheBlockUnassigned {
		t.Fatalf("fully freed zone class = %d, want unassigned", got)
	}

	skewed := newPageCacheBlocks(16, 16)
	small, ok := skewed.take(3)
	if !ok {
		t.Fatal("skew small take failed")
	}
	large, ok := skewed.take(5)
	if !ok {
		t.Fatal("cross-class large take failed")
	}
	if skewed.crossClassTakes != 1 {
		t.Fatalf("cross-class takes = %d, want 1", skewed.crossClassTakes)
	}
	if got := skewed.zoneClasses[0]; got != pageCacheBlockSmall {
		t.Fatalf("fallback changed zone class to %d, want small", got)
	}
	skewed.put(large, 5)
	skewed.put(small, 3)
	if got := skewed.zoneClasses[0]; got != pageCacheBlockUnassigned {
		t.Fatalf("coalesced fallback zone class = %d, want unassigned", got)
	}
}

func TestPageCacheBlockAllocatorDeterministicExactPlacement(t *testing.T) {
	run := func() [8]int {
		blocks := newPageCacheBlocks(32, 16)
		spans := [...]int{5, 5, 5, 3, 3, 3, 3, 3}
		var starts [len(spans)]int
		for i, span := range spans {
			index, ok := blocks.take(span)
			if !ok {
				t.Fatalf("take sequence index %d span %d", i, span)
			}
			starts[i] = index
		}
		return starts
	}
	first := run()
	second := run()
	if first != second {
		t.Fatalf("placement changed between identical runs: %v != %v", first, second)
	}

	blocks := newPageCacheBlocks(16, 16)
	if allocs := testing.AllocsPerRun(1000, func() {
		index, ok := blocks.take(5)
		if !ok {
			panic("take")
		}
		blocks.put(index, 5)
	}); allocs != 0 {
		t.Fatalf("take/put allocations = %v, want 0", allocs)
	}
}

func TestPageCacheConcurrentHitsAndEvictions(t *testing.T) {
	file, storeID, refs := newPageCacheFixture(t, 32)
	cache, err := NewPageCache(file, PageCacheOptions{
		PageSize: pageCacheTestPageSize, ResidentBytes: 4 * pageCacheTestPageSize,
		StoreID: storeID, ReadConcurrency: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()

	const workers = 16
	start := make(chan struct{})
	errorsByWorker := make(chan error, workers)
	var group sync.WaitGroup
	group.Add(workers)
	for worker := range workers {
		go func() {
			defer group.Done()
			<-start
			for iteration := range 1000 {
				index := (worker*17 + iteration*13) & (len(refs) - 1)
				for {
					lease, acquireErr := cache.Acquire(refs[index])
					if errors.Is(acquireErr, ErrPageCachePinned) {
						runtime.Gosched()
						continue
					}
					if acquireErr != nil {
						errorsByWorker <- acquireErr
						return
					}
					if lease.Payload()[0] != byte(index+1) {
						lease.Release()
						errorsByWorker <- errors.New("wrong concurrent page payload")
						return
					}
					lease.Release()
					break
				}
			}
		}()
	}
	close(start)
	group.Wait()
	close(errorsByWorker)
	for workerErr := range errorsByWorker {
		t.Fatal(workerErr)
	}
	stats := cache.Stats()
	if stats.ResidentBytes > stats.CapacityBytes || stats.Evictions == 0 || stats.PinnedPages != 0 {
		t.Fatalf("concurrent stats = %+v", stats)
	}
}

func TestPageCacheConcurrentCloseDrainsResidentHits(t *testing.T) {
	file, storeID, refs := newPageCacheFixture(t, 8)
	cache, err := NewPageCache(file, PageCacheOptions{
		PageSize: pageCacheTestPageSize, ResidentBytes: 8 * pageCacheTestPageSize,
		StoreID: storeID, ReadConcurrency: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, ref := range refs {
		lease, acquireErr := cache.Acquire(ref)
		if acquireErr != nil {
			t.Fatal(acquireErr)
		}
		lease.Release()
	}
	const workers = 16
	var ready sync.WaitGroup
	var group sync.WaitGroup
	ready.Add(workers)
	group.Add(workers)
	workerErrors := make(chan error, workers)
	for worker := range workers {
		go func() {
			defer group.Done()
			announced := false
			for {
				lease, acquireErr := cache.Acquire(refs[worker&(len(refs)-1)])
				if errors.Is(acquireErr, ErrPageCacheClosed) {
					if !announced {
						ready.Done()
					}
					return
				}
				if acquireErr != nil {
					workerErrors <- acquireErr
					if !announced {
						ready.Done()
					}
					return
				}
				if !announced {
					announced = true
					ready.Done()
				}
				if lease.Payload()[0] != byte(worker&(len(refs)-1)+1) {
					lease.Release()
					workerErrors <- errors.New("wrong page during concurrent close")
					return
				}
				lease.Release()
			}
		}()
	}
	ready.Wait()
	closeErr := cache.Close()
	if closeErr != nil && !errors.Is(closeErr, ErrPageCachePinned) {
		t.Fatalf("concurrent Close = %v", closeErr)
	}
	group.Wait()
	close(workerErrors)
	for workerErr := range workerErrors {
		t.Fatal(workerErr)
	}
	if err := cache.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPageCacheDirtyAdmissionWaitsForDurability(t *testing.T) {
	file, storeID, refs := newPageCacheFixture(t, 2)
	page := make([]byte, pageCacheTestPageSize)
	if _, err := file.ReadAt(page, int64(refs[0].Offset)); err != nil {
		t.Fatal(err)
	}
	cache, err := NewPageCache(file, PageCacheOptions{
		PageSize: pageCacheTestPageSize, ResidentBytes: pageCacheTestPageSize,
		StoreID: storeID, ReadConcurrency: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := cache.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	if err := cache.AdmitDirty(refs[0], page, 2); err != nil {
		t.Fatal(err)
	}
	if len(cache.dirtyFrames) != 1 {
		t.Fatalf("dirty queue = %d, want 1", len(cache.dirtyFrames))
	}
	if _, err := file.WriteAt([]byte{0xff}, int64(refs[0].Offset+PageHeaderSize)); err != nil {
		t.Fatal(err)
	}
	lease, err := cache.Acquire(refs[0])
	if err != nil || lease.Payload()[0] != 1 {
		t.Fatalf("Acquire admitted = (%v,%v)", lease.Payload(), err)
	}
	lease.Release()
	if _, err := cache.Acquire(refs[1]); !errors.Is(err, ErrPageCachePinned) {
		t.Fatalf("Acquire with dirty cache = %v, want %v", err, ErrPageCachePinned)
	}
	stats := cache.Stats()
	if stats.DirtyBytes != pageCacheTestPageSize || stats.PageReads != 0 || stats.CacheHits != 1 {
		t.Fatalf("dirty Stats = %+v", stats)
	}
	cache.MarkDurable(1)
	if len(cache.dirtyFrames) != 1 {
		t.Fatalf("early durability queue = %d, want 1", len(cache.dirtyFrames))
	}
	if stats := cache.Stats(); stats.DirtyBytes != pageCacheTestPageSize {
		t.Fatalf("early MarkDurable cleared page: %+v", stats)
	}
	cache.MarkDurable(2)
	if len(cache.dirtyFrames) != 0 {
		t.Fatalf("durable queue = %d, want 0", len(cache.dirtyFrames))
	}
	second, err := cache.Acquire(refs[1])
	if err != nil {
		t.Fatal(err)
	}
	second.Release()
	if stats := cache.Stats(); stats.DirtyBytes != 0 || stats.PageReads != 1 || stats.Evictions != 1 {
		t.Fatalf("durable Stats = %+v", stats)
	}
}

func TestPageCacheSeparatesReusedOffsetGenerations(t *testing.T) {
	file, storeID, refs := newPageCacheFixture(t, 1)
	oldPage := make([]byte, pageCacheTestPageSize)
	if _, err := file.ReadAt(oldPage, int64(refs[0].Offset)); err != nil {
		t.Fatal(err)
	}
	newRef := refs[0]
	newRef.Generation++
	newPage := make([]byte, pageCacheTestPageSize)
	payload, err := InitPage(newPage, PageHeader{
		StoreID: storeID, Generation: newRef.Generation, LogicalID: newRef.LogicalID,
		PageSize: newRef.Length, PayloadLength: 32, Kind: newRef.Kind,
	})
	if err != nil {
		t.Fatal(err)
	}
	payload[0] = 9
	if _, err := SealPage(newPage); err != nil {
		t.Fatal(err)
	}
	cache, err := NewPageCache(file, PageCacheOptions{
		PageSize: pageCacheTestPageSize, ResidentBytes: 2 * pageCacheTestPageSize,
		StoreID: storeID, ReadConcurrency: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()
	if err := cache.AdmitDirty(refs[0], oldPage, 2); err != nil {
		t.Fatal(err)
	}
	if err := cache.AdmitDirty(newRef, newPage, 3); err != nil {
		t.Fatal(err)
	}
	if len(cache.dirtyFrames) != 2 {
		t.Fatalf("generation dirty queue = %d, want 2", len(cache.dirtyFrames))
	}
	cache.MarkDurable(2)
	if len(cache.dirtyFrames) != 1 || cache.dirtyFrames[0].generation != 3 {
		t.Fatalf("partial durability queue = %+v", cache.dirtyFrames)
	}
	oldLease, err := cache.Acquire(refs[0])
	if err != nil || oldLease.Payload()[0] != 1 {
		t.Fatalf("old generation = (%v,%v)", oldLease.Payload(), err)
	}
	newLease, err := cache.Acquire(newRef)
	if err != nil || newLease.Payload()[0] != 9 {
		t.Fatalf("new generation = (%v,%v)", newLease.Payload(), err)
	}
	oldLease.Release()
	newLease.Release()
	if stats := cache.Stats(); stats.ReadyFrames != 2 || stats.ResidentBytes != 2*pageCacheTestPageSize {
		t.Fatalf("generation collision stats = %+v", stats)
	}
}

func TestPageCacheMarkUnreachableIsExactAndReleasesDirtyCapacity(t *testing.T) {
	file, storeID, refs := newPageCacheFixture(t, 1)
	oldPage := make([]byte, pageCacheTestPageSize)
	if _, err := file.ReadAt(oldPage, int64(refs[0].Offset)); err != nil {
		t.Fatal(err)
	}
	newRef := refs[0]
	newRef.Generation++
	newPage := make([]byte, pageCacheTestPageSize)
	payload, err := InitPage(newPage, PageHeader{
		StoreID: storeID, Generation: newRef.Generation,
		LogicalID: newRef.LogicalID, PageSize: newRef.Length,
		PayloadLength: 32, Kind: newRef.Kind,
	})
	if err != nil {
		t.Fatal(err)
	}
	payload[0] = 9
	if _, err := SealPage(newPage); err != nil {
		t.Fatal(err)
	}
	cache, err := NewPageCache(file, PageCacheOptions{
		PageSize:      pageCacheTestPageSize,
		ResidentBytes: 2 * pageCacheTestPageSize,
		StoreID:       storeID, ReadConcurrency: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()
	if err := cache.AdmitDirty(refs[0], oldPage, 1); err != nil {
		t.Fatal(err)
	}
	if err := cache.AdmitDirty(newRef, newPage, 2); err != nil {
		t.Fatal(err)
	}
	pinned, err := cache.Acquire(refs[0])
	if err != nil {
		t.Fatal(err)
	}
	cache.MarkUnreachable([]PageRef{refs[0], refs[0]})
	stats := cache.Stats()
	if stats.DirtyBytes != pageCacheTestPageSize ||
		stats.PinnedPages != 1 || stats.ReadyFrames != 2 {
		t.Fatalf("pinned unreachable stats = %+v", stats)
	}
	if got := pinned.Payload()[0]; got != 1 {
		t.Fatalf("pinned unreachable payload = %d, want 1", got)
	}
	newLease, err := cache.Acquire(newRef)
	if err != nil || newLease.Payload()[0] != 9 {
		t.Fatalf("new exact generation = (%v, %v)", newLease.Payload(), err)
	}
	newLease.Release()
	pinned.Release()
	stats = cache.Stats()
	if stats.DirtyBytes != pageCacheTestPageSize ||
		stats.PinnedPages != 0 || stats.ReadyFrames != 1 {
		t.Fatalf("last-unpin unreachable stats = %+v", stats)
	}
}

func TestPageCacheDoomedExtentReleasedOnLastUnpin(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "store-page-cache-doomed-extent-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	storeID := [16]byte{29, 23, 19, 17, 13, 11, 7, 5, 3, 2, 1, 4, 6, 8, 10, 12}
	ref := PageRef{
		Offset: 4 * pageCacheTestPageSize, LogicalID: 2, Generation: 1,
		Length: 3 * pageCacheTestPageSize, Kind: PageOverflow,
	}
	page := newPageCacheTestPage(t, storeID, ref, 0x5a)
	if _, err := file.WriteAt(page, int64(ref.Offset)); err != nil {
		t.Fatal(err)
	}
	cache, err := NewPageCache(file, PageCacheOptions{
		PageSize: pageCacheTestPageSize, MaxPageSize: 4 * pageCacheTestPageSize,
		ResidentBytes: 4 * pageCacheTestPageSize, StoreID: storeID,
		ReadConcurrency: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()

	lease, err := cache.Acquire(ref)
	if err != nil {
		t.Fatal(err)
	}
	cache.MarkUnreachable([]PageRef{ref})
	if got := lease.Payload()[0]; got != 0x5a {
		t.Fatalf("doomed pinned payload = %x, want 5a", got)
	}
	key, err := cache.validateRef(ref)
	if err != nil {
		t.Fatal(err)
	}
	cache.mu.Lock()
	index, ok := cache.lookupLocked(cacheKeyHash(key), key)
	if ok {
		frame := &cache.frames[index]
		frame.lock.Lock()
		ok = frame.flags&pageCacheFrameDoomed != 0 && !frame.referenced
		frame.lock.Unlock()
	}
	cache.mu.Unlock()
	if !ok {
		t.Fatal("pinned unreachable extent was not doomed and dereferenced")
	}
	if stats := cache.Stats(); stats.ResidentBytes != uint64(ref.Length) ||
		stats.ReservedBytes != uint64(ref.Length) || stats.Pins != 1 {
		t.Fatalf("doomed pinned stats = %+v", stats)
	}

	lease.Release()
	if stats := cache.Stats(); stats.ResidentBytes != 0 || stats.ReservedBytes != 0 ||
		stats.ReadyFrames != 0 || stats.Pins != 0 {
		t.Fatalf("last-unpin stats = %+v", stats)
	}
	cache.mu.Lock()
	start, available := cache.blocks.take(3)
	if available {
		cache.blocks.put(start, 3)
	}
	cache.mu.Unlock()
	if !available || start != 0 {
		t.Fatalf("released exact-span reservation = (%d,%v), want (0,true)", start, available)
	}

	lease, err = cache.Acquire(ref)
	if err != nil {
		t.Fatal(err)
	}
	lease.Release()
	if stats := cache.Stats(); stats.PageReads != 2 || stats.Misses != 2 {
		t.Fatalf("reacquire did not miss after doomed release: %+v", stats)
	}
}

func TestPageCacheDoomedExtentSurvivesUntilSecondPinReleases(t *testing.T) {
	file, storeID, refs := newPageCacheFixture(t, 1)
	cache, err := NewPageCache(file, PageCacheOptions{
		PageSize: pageCacheTestPageSize, ResidentBytes: pageCacheTestPageSize,
		StoreID: storeID, ReadConcurrency: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()

	first, err := cache.Acquire(refs[0])
	if err != nil {
		t.Fatal(err)
	}
	second, err := cache.Acquire(refs[0])
	if err != nil {
		t.Fatal(err)
	}
	cache.MarkUnreachable(refs)
	first.Release()
	if got := second.Payload()[0]; got != 1 {
		t.Fatalf("remaining pin payload = %d, want 1", got)
	}
	if stats := cache.Stats(); stats.ResidentBytes != pageCacheTestPageSize ||
		stats.ReadyFrames != 1 || stats.Pins != 1 {
		t.Fatalf("first-release doomed stats = %+v", stats)
	}
	second.Release()
	if stats := cache.Stats(); stats.ResidentBytes != 0 ||
		stats.ReadyFrames != 0 || stats.Pins != 0 {
		t.Fatalf("second-release doomed stats = %+v", stats)
	}
}

func TestPageCacheDoomedDirtyAccountingClearedOnce(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "store-page-cache-doomed-dirty-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	storeID := [16]byte{31, 29, 23, 19, 17, 13, 11, 7, 5, 3, 2, 1, 4, 6, 8, 10}
	ref := PageRef{
		Offset: 4 * pageCacheTestPageSize, LogicalID: 2, Generation: 1,
		Length: 3 * pageCacheTestPageSize, Kind: PageOverflow,
	}
	page := newPageCacheTestPage(t, storeID, ref, 0x7b)
	cache, err := NewPageCache(file, PageCacheOptions{
		PageSize: pageCacheTestPageSize, MaxPageSize: 4 * pageCacheTestPageSize,
		ResidentBytes: 4 * pageCacheTestPageSize, StoreID: storeID,
		ReadConcurrency: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()
	if err := cache.AdmitDirty(ref, page, 2); err != nil {
		t.Fatal(err)
	}
	lease, err := cache.Acquire(ref)
	if err != nil {
		t.Fatal(err)
	}
	if stats := cache.Stats(); stats.DirtyBytes != uint64(ref.Length) ||
		cache.DirtyCapacityAvailable() != pageCacheTestPageSize {
		t.Fatalf("initial dirty accounting = %+v, available=%d",
			stats, cache.DirtyCapacityAvailable())
	}

	cache.MarkUnreachable([]PageRef{ref, ref})
	if stats := cache.Stats(); stats.DirtyBytes != 0 ||
		stats.ResidentBytes != uint64(ref.Length) ||
		cache.DirtyCapacityAvailable() != 4*pageCacheTestPageSize {
		t.Fatalf("doomed dirty accounting = %+v, available=%d",
			stats, cache.DirtyCapacityAvailable())
	}
	if len(cache.dirtyFrames) != 0 {
		t.Fatalf("doomed dirty queue = %+v, want empty", cache.dirtyFrames)
	}
	lease.Release()
	if stats := cache.Stats(); stats.DirtyBytes != 0 || stats.ResidentBytes != 0 ||
		cache.DirtyCapacityAvailable() != 4*pageCacheTestPageSize {
		t.Fatalf("released doomed dirty accounting = %+v, available=%d",
			stats, cache.DirtyCapacityAvailable())
	}
}

func TestPageCacheConcurrentAcquireReleaseAndMarkUnreachable(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "store-page-cache-doomed-race-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	storeID := [16]byte{37, 31, 29, 23, 19, 17, 13, 11, 7, 5, 3, 2, 1, 4, 6, 8}
	current := PageRef{
		Offset: 4 * pageCacheTestPageSize, LogicalID: 2, Generation: 1,
		Length: pageCacheTestPageSize, Kind: PageOverflow,
	}
	cache, err := NewPageCache(file, PageCacheOptions{
		PageSize: pageCacheTestPageSize, MaxPageSize: pageCacheTestPageSize,
		ResidentBytes: 64 * pageCacheTestPageSize, StoreID: storeID,
		ReadConcurrency: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()
	if err := cache.AdmitDirty(current, newPageCacheTestPage(t, storeID, current, 1), 1); err != nil {
		t.Fatal(err)
	}

	var rootMu sync.RWMutex
	stop := make(chan struct{})
	readerErrors := make(chan error, 4)
	var readers sync.WaitGroup
	for range 4 {
		readers.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				rootMu.RLock()
				ref := current
				lease, acquireErr := cache.Acquire(ref)
				rootMu.RUnlock()
				if acquireErr != nil {
					readerErrors <- acquireErr
					return
				}
				gotGeneration := lease.Header().Generation
				gotPayload := lease.Payload()[0]
				if gotGeneration != ref.Generation ||
					gotPayload != byte(ref.Generation) {
					lease.Release()
					readerErrors <- fmt.Errorf(
						"lease generation/payload = (%d,%d), want (%d,%d)",
						gotGeneration, gotPayload,
						ref.Generation, byte(ref.Generation),
					)
					return
				}
				runtime.Gosched()
				lease.Release()
			}
		})
	}

	var publishErr error
	for generation := uint64(2); generation <= 500; generation++ {
		next := current
		next.Generation = generation
		page := newPageCacheTestPage(t, storeID, next, byte(generation))
		if publishErr = cache.AdmitDirty(next, page, generation); publishErr != nil {
			break
		}
		rootMu.Lock()
		old := current
		current = next
		cache.MarkUnreachable([]PageRef{old})
		rootMu.Unlock()
		runtime.Gosched()
	}
	close(stop)
	readers.Wait()
	close(readerErrors)
	if publishErr != nil {
		t.Fatalf("publish dirty generation: %v", publishErr)
	}
	for readerErr := range readerErrors {
		t.Errorf("reader: %v", readerErr)
	}
	cache.MarkUnreachable([]PageRef{current})
	if stats := cache.Stats(); stats.ResidentBytes != 0 || stats.DirtyBytes != 0 ||
		stats.Pins != 0 {
		t.Fatalf("post-race stats = %+v", stats)
	}
}

func TestPageCacheDiscardDirtyGeneration(t *testing.T) {
	file, storeID, refs := newPageCacheFixture(t, 1)
	page := make([]byte, pageCacheTestPageSize)
	if _, err := file.ReadAt(page, int64(refs[0].Offset)); err != nil {
		t.Fatal(err)
	}
	cache, err := NewPageCache(file, PageCacheOptions{
		PageSize: pageCacheTestPageSize, ResidentBytes: pageCacheTestPageSize,
		StoreID: storeID, ReadConcurrency: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()
	if err := cache.AdmitDirty(refs[0], page, 3); err != nil {
		t.Fatal(err)
	}
	lease, err := cache.Acquire(refs[0])
	if err != nil {
		t.Fatal(err)
	}
	if err := cache.DiscardDirty(3); !errors.Is(err, ErrPageCachePinned) {
		t.Fatalf("pinned DiscardDirty = %v, want %v", err, ErrPageCachePinned)
	}
	if len(cache.dirtyFrames) != 1 {
		t.Fatalf("failed discard changed dirty queue: %+v", cache.dirtyFrames)
	}
	lease.Release()
	if err := cache.DiscardDirty(3); err != nil {
		t.Fatal(err)
	}
	if len(cache.dirtyFrames) != 0 {
		t.Fatalf("discard retained dirty queue: %+v", cache.dirtyFrames)
	}
	if stats := cache.Stats(); stats.ResidentBytes != 0 || stats.DirtyBytes != 0 {
		t.Fatalf("Stats after discard = %+v", stats)
	}
	lease, err = cache.Acquire(refs[0])
	if err != nil {
		t.Fatal(err)
	}
	lease.Release()
	if stats := cache.Stats(); stats.PageReads != 1 {
		t.Fatalf("discarded page did not read from file: %+v", stats)
	}
}

func newPageCacheFixture(t testing.TB, count int) (*os.File, [16]byte, []PageRef) {
	t.Helper()
	file, err := os.CreateTemp(t.TempDir(), "store-page-cache-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { file.Close() })
	storeID := [16]byte{1, 3, 5, 7, 9, 11, 13, 15, 2, 4, 6, 8, 10, 12, 14, 16}
	refs := make([]PageRef, count)
	for i := range refs {
		offset := uint64(4+i) * pageCacheTestPageSize
		logicalID := uint64(i + 2)
		page := make([]byte, pageCacheTestPageSize)
		payload, initErr := InitPage(page, PageHeader{
			StoreID: storeID, Generation: 1, LogicalID: logicalID,
			PageSize: pageCacheTestPageSize, PayloadLength: 32, Kind: PageOverflow,
		})
		if initErr != nil {
			t.Fatal(initErr)
		}
		payload[0] = byte(i + 1)
		if _, sealErr := SealPage(page); sealErr != nil {
			t.Fatal(sealErr)
		}
		if _, writeErr := file.WriteAt(page, int64(offset)); writeErr != nil {
			t.Fatal(writeErr)
		}
		refs[i] = PageRef{
			Offset: offset, LogicalID: logicalID, Generation: 1,
			Length: pageCacheTestPageSize, Kind: PageOverflow,
		}
	}
	return file, storeID, refs
}

func newPageCacheTestPage(
	t testing.TB, storeID [16]byte, ref PageRef, marker byte,
) []byte {
	t.Helper()
	page := make([]byte, ref.Length)
	payload, err := InitPage(page, PageHeader{
		StoreID: storeID, Generation: ref.Generation, LogicalID: ref.LogicalID,
		PageSize: ref.Length, PayloadLength: 32, Kind: ref.Kind, Flags: ref.Flags,
	})
	if err != nil {
		t.Fatal(err)
	}
	payload[0] = marker
	if _, err := SealPage(page); err != nil {
		t.Fatal(err)
	}
	return page
}
