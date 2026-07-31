package storeio

import (
	"bytes"
	"errors"
	"fmt"
)

var (
	// ErrCanonicalPageBusy reports that a stable-reference materialization
	// target is absent, loading, dirty, or pinned. The caller must use its
	// ordinary copy-on-write path; it must never wait while holding the
	// publication gate.
	ErrCanonicalPageBusy = errors.New("vibejson: canonical Store page is not exclusively writable")
	// ErrCanonicalPageChanged reports that the resident page no longer equals
	// the before-image used to construct a materialization transaction.
	ErrCanonicalPageChanged = errors.New("vibejson: canonical Store page changed during materialization")
)

// ReplaceCanonicalDirty atomically swaps one clean, unpinned resident page to
// an encoded after-image without changing its PageRef. It is a writer-only
// operation for recovery-journaled canonical materialization. Ordinary reads
// never call or branch on it.
//
// before and after must be complete, independently owned page images. The
// caller must already have blocked snapshot acquisition and proved that no
// active snapshot or recovery root requires the before-image. On success the
// frame remains non-evictable until dirtyGeneration becomes durable.
func (c *PageCache) ReplaceCanonicalDirty(
	ref PageRef, before, after []byte, dirtyGeneration uint64,
) error {
	if c == nil {
		return ErrPageCacheClosed
	}
	key, err := c.validateRef(ref)
	if err != nil || dirtyGeneration <= ref.Generation ||
		len(before) < int(ref.Length) || len(after) < int(ref.Length) {
		return fmt.Errorf("%w: canonical reference, generation, or bytes", ErrPageCacheReference)
	}
	before = before[:int(ref.Length)]
	after = after[:int(ref.Length)]
	beforeHeader, _, err := OpenPage(before)
	if err != nil {
		return err
	}
	afterHeader, _, err := OpenPage(after)
	if err != nil {
		return err
	}
	if !canonicalPageHeaderMatchesRef(beforeHeader, ref, c.options.StoreID) ||
		!canonicalPageHeaderMatchesRef(afterHeader, ref, c.options.StoreID) {
		return fmt.Errorf("%w: canonical image identity does not match reference", ErrPageCacheReference)
	}
	if c.options.Validate != nil {
		if err := c.options.Validate(after, ref); err != nil {
			return err
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closing.Load() || c.closed {
		return ErrPageCacheClosed
	}
	index, ok := c.lookupLocked(cacheKeyHash(key), key)
	if !ok {
		return ErrCanonicalPageBusy
	}
	frame := &c.frames[index]
	frame.lock.Lock()
	defer frame.lock.Unlock()
	if frame.state != pageCacheReady || frame.dirty != 0 || frame.pins != 0 {
		return ErrCanonicalPageBusy
	}
	page := c.extentBytes(index, ref.Length)
	if !bytes.Equal(page, before) {
		return ErrCanonicalPageChanged
	}
	copy(page, after)
	frame.payloadLength = afterHeader.PayloadLength
	frame.dirty = dirtyGeneration
	frame.referenced = true
	c.dirtyBytes += uint64(frame.key.length)
	c.dirtyReservedBytes += uint64(frame.reservationSpan) * uint64(c.options.PageSize)
	c.recordDirtyFrameLocked(index, dirtyGeneration)
	return nil
}

// RestoreCanonicalDirty reverses the exact successful replacement identified
// by after before its publication has been accepted. Callers roll back a
// multi-page transaction in reverse replacement order. Once the batch is
// queued, recovery and the durability fence—not this method—own failures.
func (c *PageCache) RestoreCanonicalDirty(
	ref PageRef, before, after []byte, dirtyGeneration uint64,
) error {
	if c == nil {
		return ErrPageCacheClosed
	}
	key, err := c.validateRef(ref)
	if err != nil || dirtyGeneration <= ref.Generation ||
		len(before) < int(ref.Length) || len(after) < int(ref.Length) {
		return fmt.Errorf("%w: canonical rollback reference, generation, or bytes", ErrPageCacheReference)
	}
	before = before[:int(ref.Length)]
	after = after[:int(ref.Length)]
	header, _, err := OpenPage(before)
	if err != nil {
		return err
	}
	if !canonicalPageHeaderMatchesRef(header, ref, c.options.StoreID) {
		return fmt.Errorf("%w: canonical rollback identity does not match reference", ErrPageCacheReference)
	}
	afterHeader, _, err := OpenPage(after)
	if err != nil || !canonicalPageHeaderMatchesRef(afterHeader, ref, c.options.StoreID) {
		return fmt.Errorf("%w: canonical replacement identity does not match reference", ErrPageCacheReference)
	}
	if c.options.Validate != nil {
		if err := c.options.Validate(before, ref); err != nil {
			return err
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closing.Load() || c.closed {
		return ErrPageCacheClosed
	}
	index, ok := c.lookupLocked(cacheKeyHash(key), key)
	if !ok {
		return ErrCanonicalPageChanged
	}
	frame := &c.frames[index]
	frame.lock.Lock()
	defer frame.lock.Unlock()
	if frame.state != pageCacheReady || frame.dirty != dirtyGeneration || frame.pins != 0 {
		return ErrCanonicalPageChanged
	}
	if !bytes.Equal(c.extentBytes(index, ref.Length), after) {
		return ErrCanonicalPageChanged
	}
	copy(c.extentBytes(index, ref.Length), before)
	frame.payloadLength = header.PayloadLength
	frame.dirty = 0
	frame.referenced = true
	c.dirtyBytes -= uint64(frame.key.length)
	c.dirtyReservedBytes -= uint64(frame.reservationSpan) * uint64(c.options.PageSize)
	c.removeDirtyFrameLocked(index, dirtyGeneration)
	return nil
}

func canonicalPageHeaderMatchesRef(header PageHeader, ref PageRef, storeID [16]byte) bool {
	return header.StoreID == storeID &&
		header.PageSize == ref.Length &&
		header.LogicalID == ref.LogicalID &&
		header.Generation == ref.Generation && header.Kind == ref.Kind
}
