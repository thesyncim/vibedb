package storeio

import (
	"bytes"
	"fmt"
	"unsafe"
)

// OffsetOf reports view's exact byte offset inside the leased complete page.
// Typed admitted views intentionally expose only borrowed slices; this keeps
// the one pointer-to-offset conversion beside the lease that owns the arena.
func (l *PageLease) OffsetOf(view []byte) (uint32, bool) {
	if l == nil || l.cache == nil || len(view) == 0 || len(l.page) == 0 {
		return 0, false
	}
	pageStart := uintptr(unsafe.Pointer(unsafe.SliceData(l.page)))
	viewStart := uintptr(unsafe.Pointer(unsafe.SliceData(view)))
	if viewStart < pageStart {
		return 0, false
	}
	offset := viewStart - pageStart
	if offset > uintptr(len(l.page)) ||
		uintptr(len(view)) > uintptr(len(l.page))-offset ||
		offset > uintptr(^uint32(0)) {
		return 0, false
	}
	return uint32(offset), true
}

// ReplaceLeasedCanonicalDirty edits one exclusively owned, write-pinned
// frame-native page without copying its complete extent. The caller's resolving
// lease must be the frame's only reader pin. While the row bytes move, the frame
// is in a brief exclusive state and the frame lock prevents a new reader from
// acquiring a view over torn bytes.
//
// The page keeps the immutable identity assigned by its first COW publication.
// Resealing is deferred to the checkpoint worker, which already has to acquire
// the frame lock before handing its bytes to Device.
func (c *PageCache) ReplaceLeasedCanonicalDirty(
	lease *PageLease,
	from, to PageRef,
	valueOffset uint32,
	before, after []byte,
	dirtyGeneration uint64,
) (previousDirty uint64, err error) {
	if c == nil || lease == nil || lease.cache != c ||
		len(before) == 0 || len(before) != len(after) {
		return 0, ErrCanonicalPageBusy
	}
	fromKey, err := c.validateRef(from)
	if err != nil {
		return 0, err
	}
	end := uint64(valueOffset) + uint64(len(after))
	if from != to || dirtyGeneration < to.Generation ||
		end > uint64(from.Length)-PageTrailerSize {
		return 0, fmt.Errorf("%w: canonical replacement shape", ErrPageCacheReference)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closing.Load() || c.closed {
		return 0, ErrPageCacheClosed
	}
	index := lease.frame
	if index < 0 || index >= len(c.frames) {
		return 0, ErrCanonicalPageBusy
	}
	frame := &c.frames[index]
	frame.lock.Lock()
	defer frame.lock.Unlock()
	if frame.state.Load() != uint32(pageCacheReady) || frame.key != fromKey ||
		lease.key != fromKey || frame.pins.Load() != 1 ||
		frame.flags.Load()&(pageCacheFrameWritePinned|
			pageCacheFrameBufferedOwned) == 0 {
		return 0, ErrCanonicalPageBusy
	}
	// Exclude lock-free validators before touching bytes: a reader that
	// validated just before this exclusion announced first, so the drain
	// below observes it (it aborts on the exclusive state). The caller's
	// own lease pin keeps continuity, so pins must settle back to exactly
	// one; anything else is a genuinely active reader and stays busy.
	frame.state.Store(pageCacheExclusive)
	if !drainFramePins(frame) || frame.pins.Load() != 1 {
		frame.state.Store(pageCacheReady)
		return 0, ErrCanonicalPageBusy
	}
	page := c.extentBytes(index, from.Length)
	target := page[int(valueOffset):int(end)]
	if !bytes.Equal(target, before) {
		return 0, ErrCanonicalPageChanged
	}

	previousDirty = frame.dirty
	frame.state.Store(pageCacheExclusive)
	copy(target, after)
	frame.flags.Or(pageCacheFrameNeedsReseal)

	if previousDirty == 0 {
		c.dirtyBytes += uint64(frame.key.length)
		c.dirtyReservedBytes +=
			uint64(frame.reservationSpan) * uint64(c.options.PageSize)
	} else if previousDirty != dirtyGeneration {
		c.removeDirtyFrameLocked(index, previousDirty)
	}
	frame.dirty = dirtyGeneration
	frame.referenced.Store(true)
	if previousDirty != dirtyGeneration {
		c.recordDirtyFrameLocked(index, dirtyGeneration)
	}
	frame.state.Store(pageCacheReady)
	return previousDirty, nil
}
