package durable

import (
	"bytes"
	"fmt"
	"slices"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
)

type primarySnapshotMaskGroup struct {
	bucket   storeio.BucketID
	selected [4]uint64
	floor    []byte
	route    storeio.ResidentPrimaryRoute
}

// RangeRawCurrent visits one current reader-visible generation in bytewise
// lexical key order. Unlike Snapshot.RangeRaw, it is deliberately ephemeral:
// the generation and its row-overlay history are pinned only for this call.
// That lets a mixed read/write loop scan pending inline mutations directly
// instead of first folding them into a new physical graph.
//
// The structural fence is held only long enough to pin a generation and copy
// the overlay's fixed bucket-head directory. Ordinary and structural writers,
// checkpoints, and Close may then proceed while the rooted graph and captured
// immutable record chains remain protected by the reader pin. key and value
// are borrowed only for the callback.
func (c *Collection) RangeRawCurrent(fn func(key, value []byte) error) error {
	_, err := c.RangeRawCurrentBuffer(nil, fn)
	return err
}

// RangeRawCurrentBuffer is RangeRawCurrent with caller-owned reconstruction
// storage. Reusing the returned slice makes warmed inline and overflow scans
// allocation-free.
func (c *Collection) RangeRawCurrentBuffer(
	scratch []byte,
	fn func(key, value []byte) error,
) ([]byte, error) {
	if c == nil {
		return scratch, ErrClosed
	}
	if fn == nil {
		return scratch, nil
	}

	var captured primaryCurrentScanOverlayCapture
	view, pin, rooted, err := c.capturePrimaryCurrentScan(&captured)
	if err != nil {
		return scratch, err
	}
	if !rooted {
		// A structural deferred lane has a router newer than its sealed root.
		// Preserve the mature Snapshot materialization path for this rare case;
		// the common inline overlay lane never reaches it.
		snapshot, snapshotErr := c.Snapshot()
		if snapshotErr != nil {
			return scratch, snapshotErr
		}
		defer snapshot.Close()
		return snapshot.RangeRawBuffer(scratch, fn)
	}
	defer pin.release()
	return c.rangeRawCurrentAt(view, &captured, scratch, fn)
}

// primaryCurrentScanOverlayCapture mirrors the overlay's fixed open-addressed
// bucket directory. Each packed word is bucket<<32|head; head zero is empty.
// Copying all 2,048 words under the short structural fence is bounded and
// avoids a map, allocation, or long writer exclusion during callbacks.
type primaryCurrentScanOverlayCapture struct {
	bucket [primaryUnifiedOverlayBucketTable]uint64
}

func (d *primaryCurrentScanOverlayCapture) capture(
	overlay *primaryUnifiedOverlay,
	baseGeneration, generation uint64,
) error {
	*d = primaryCurrentScanOverlayCapture{}
	if overlay == nil || generation <= baseGeneration {
		return nil
	}
	for index := range overlay.buckets {
		head := overlay.buckets[index].head.Load()
		if head == 0 {
			continue
		}
		if head > uint32(len(overlay.records)) {
			return storeio.ErrCommonPrimaryLeafCorrupt
		}
		// Head publication follows complete immutable record initialization.
		// Derive the identity from that record rather than a separately loaded
		// directory word, so a future publisher cannot produce a torn
		// {stale bucket,new head} observation in this captured cut.
		record := &overlay.records[head-1]
		if record.generation == 0 {
			return storeio.ErrCommonPrimaryLeafCorrupt
		}
		d.bucket[index] = uint64(record.bucket)<<32 | uint64(head)
	}
	return nil
}

func (d *primaryCurrentScanOverlayCapture) head(
	bucket storeio.BucketID,
) uint32 {
	mask := uint32(primaryUnifiedOverlayBucketTable - 1)
	slot := primaryUnifiedOverlayBucketHash(bucket) & mask
	for range primaryUnifiedOverlayBucketTable {
		packed := d.bucket[slot]
		head := uint32(packed)
		if head == 0 {
			return 0
		}
		if uint32(packed>>32) == uint32(bucket) {
			return head
		}
		slot = (slot + 1) & mask
	}
	return 0
}

type primaryCurrentScanPin struct {
	epoch  storeio.ReadEpoch
	lease  storeio.GenerationLease
	direct bool
}

func (p *primaryCurrentScanPin) release() {
	if p.direct {
		p.epoch.Exit()
	} else {
		p.lease.Release()
	}
}

func (c *Collection) capturePrimaryCurrentScan(
	captured *primaryCurrentScanOverlayCapture,
) (fileLogicalView, primaryCurrentScanPin, bool, error) {
	c.writer.RLock()
	defer c.writer.RUnlock()
	if c.closed {
		return fileLogicalView{}, primaryCurrentScanPin{}, false, ErrClosed
	}
	if len(c.primaryPendingParents) != 0 {
		return fileLogicalView{}, primaryCurrentScanPin{}, false, nil
	}
	if view, epoch, ok := c.enterReadEpoch(); ok {
		state := view.state
		if state.root.PrimaryRoot == (storeio.PageRef{}) {
			epoch.Exit()
			return fileLogicalView{}, primaryCurrentScanPin{}, false,
				storeio.ErrSegmentedTabletRouterCorrupt
		}
		if err := captured.capture(
			c.primaryUnifiedOverlay,
			state.root.PrimaryRoot.Generation,
			view.generation,
		); err != nil {
			epoch.Exit()
			return fileLogicalView{}, primaryCurrentScanPin{}, false, err
		}
		return view, primaryCurrentScanPin{
			epoch: epoch, direct: true,
		}, true, nil
	}

	c.snapshotGate.RLock()
	view, stateErr := c.visibleLogicalView()
	if stateErr != nil {
		c.snapshotGate.RUnlock()
		return fileLogicalView{}, primaryCurrentScanPin{}, false, stateErr
	}
	lease, leaseErr := c.leases.Acquire(view.retentionGeneration())
	c.snapshotGate.RUnlock()
	if leaseErr != nil {
		return fileLogicalView{}, primaryCurrentScanPin{}, false,
			publicReaderLeaseError(leaseErr)
	}
	state := view.state
	if state.root.PrimaryRoot == (storeio.PageRef{}) {
		lease.Release()
		return fileLogicalView{}, primaryCurrentScanPin{}, false,
			storeio.ErrSegmentedTabletRouterCorrupt
	}
	if err := captured.capture(
		c.primaryUnifiedOverlay,
		state.root.PrimaryRoot.Generation,
		view.generation,
	); err != nil {
		lease.Release()
		return fileLogicalView{}, primaryCurrentScanPin{}, false, err
	}
	return view, primaryCurrentScanPin{lease: lease}, true, nil
}

func (c *Collection) rangeRawCurrentAt(
	view fileLogicalView,
	captured *primaryCurrentScanOverlayCapture,
	scratch []byte,
	fn func(key, value []byte) error,
) ([]byte, error) {
	state := view.state
	catalogBounds := storeio.GlobalTabletCatalogBounds{
		StoreID:                state.root.StoreID,
		SelectedRootGeneration: state.root.PrimaryRoot.Generation,
		FileEnd:                state.fileEnd,
		NextLogicalID:          state.root.NextLogicalID,
	}
	bounds := c.primaryLeafBounds(state)
	var cursor storeio.PrimaryGraphCursor
	if err := storeio.InitPrimaryGraphCursor(
		&cursor, c.cache, state.root.PrimaryRoot,
		catalogBounds, bounds, nil, nil,
	); err != nil {
		return scratch, err
	}
	defer cursor.Close()
	if view.generation == state.root.PrimaryRoot.Generation {
		cursor.AdoptSpliceScratch(scratch)
		for {
			key, ref, err := cursor.VisitInline(fn)
			scratch = cursor.ReleaseSpliceScratch()
			if err != nil {
				return scratch, err
			}
			if ref == (storeio.PageRef{}) {
				return scratch, nil
			}
			scratch, err = c.appendPrimaryOverflowValue(
				scratch[:0], ref, bounds,
			)
			if err != nil {
				return scratch, err
			}
			if err = fn(key, scratch); err != nil {
				return scratch, err
			}
			cursor.AdoptSpliceScratch(scratch)
		}
	}

	overlay := c.primaryUnifiedOverlay
	var indexes [storeio.CommonPrimaryLeafWideSlots]uint16
	var visited uint64
	for {
		bucket, leaf, ok := cursor.CurrentUnifiedLeaf()
		if !ok {
			break
		}
		head := captured.head(bucket)
		overlayCount := 0
		var err error
		if head != 0 && overlay != nil {
			overlayCount, err = overlay.latestBucketRecordsFromHead(
				&indexes, bucket, head,
				state.root.PrimaryRoot.Generation,
				view.generation,
			)
			if err != nil {
				return scratch, err
			}
		}
		leafVisible := int64(leaf.Len())
		var previousKey []byte
		for at, index := range indexes[:overlayCount] {
			if int(index) >= len(overlay.records) {
				return scratch, storeio.ErrCommonPrimaryLeafCorrupt
			}
			record := &overlay.records[index]
			keyEnd64 := uint64(record.keyOffset) + uint64(record.keyLen)
			valueEnd64 := uint64(record.valueOff) + uint64(record.valueLen)
			if record.keyLen == 0 || keyEnd64 > uint64(len(overlay.arena)) ||
				record.valueOff != uint32(keyEnd64) ||
				valueEnd64 > uint64(len(overlay.arena)) {
				return scratch, storeio.ErrCommonPrimaryLeafCorrupt
			}
			keyEnd := uint32(keyEnd64)
			key := overlay.arena[record.keyOffset:keyEnd:keyEnd]
			if at != 0 && bytes.Compare(previousKey, key) >= 0 {
				return scratch, storeio.ErrCommonPrimaryLeafCorrupt
			}
			previousKey = key

			rank, matchesBase := leaf.RankAtSlot(record.slot)
			if matchesBase {
				baseKey, rowOK := leaf.RowAt(rank)
				if !rowOK || !bytes.Equal(baseKey, key) {
					return scratch, storeio.ErrCommonPrimaryLeafCorrupt
				}
			} else {
				rank = leaf.FirstRankFrom(key)
				if rank < leaf.Len() {
					baseKey, rowOK := leaf.RowAt(rank)
					if !rowOK || bytes.Equal(baseKey, key) {
						return scratch, storeio.ErrCommonPrimaryLeafCorrupt
					}
				}
			}

			switch record.kind {
			case primaryUnifiedOverlayPut:
				if record.valueLen == 0 {
					return scratch, storeio.ErrCommonPrimaryLeafCorrupt
				}
			case primaryUnifiedOverlayDelete:
				if record.valueLen != 0 {
					return scratch, storeio.ErrCommonPrimaryLeafCorrupt
				}
				// A final tombstone for an overlay-native key has no base row and
				// no visible output. It still participates in lexical validation.
				if !matchesBase {
					continue
				}
			default:
				return scratch, storeio.ErrCommonPrimaryLeafCorrupt
			}

			scratch, err = c.visitRangeRawCurrentBaseUntil(
				&cursor, rank, scratch, bounds, fn,
			)
			if err != nil {
				return scratch, err
			}
			if matchesBase {
				if err = cursor.ConsumeCurrentLeafBase(key); err != nil {
					return scratch, err
				}
				if record.kind == primaryUnifiedOverlayDelete {
					leafVisible--
					continue
				}
			} else {
				leafVisible++
			}
			value := overlay.arena[record.valueOff:uint32(valueEnd64):uint32(valueEnd64)]
			if err = fn(key, value); err != nil {
				return scratch, err
			}
		}
		scratch, err = c.visitRangeRawCurrentBaseUntil(
			&cursor, leaf.Len(), scratch, bounds, fn,
		)
		if err != nil {
			return scratch, err
		}
		if leafVisible < 0 {
			return scratch, storeio.ErrCommonPrimaryLeafCorrupt
		}
		visited += uint64(leafVisible)
		if err := cursor.NextLeaf(); err != nil {
			return scratch, err
		}
	}
	if visited != view.documentCount {
		return scratch, fmt.Errorf(
			"%w: current scan visited %d rows, want %d",
			storeio.ErrCommonPrimaryLeafCorrupt,
			visited,
			view.documentCount,
		)
	}
	return scratch, nil
}

// visitRangeRawCurrentBaseUntil drains one untouched base span, resolving any
// overflow rows encountered before limit and then re-entering at the advanced
// base rank. The cursor never retains scratch between calls.
func (c *Collection) visitRangeRawCurrentBaseUntil(
	cursor *storeio.PrimaryGraphCursor,
	limit int,
	scratch []byte,
	bounds storeio.CommonPrimaryLeafBounds,
	fn func(key, value []byte) error,
) ([]byte, error) {
	if cursor == nil || fn == nil {
		return scratch, storeio.ErrCommonPrimaryLeafCorrupt
	}
	for {
		cursor.AdoptSpliceScratch(scratch)
		key, ref, err := cursor.VisitCurrentLeafInlineUntil(limit, fn)
		scratch = cursor.ReleaseSpliceScratch()
		if err != nil {
			return scratch, err
		}
		if ref == (storeio.PageRef{}) {
			return scratch, nil
		}
		scratch, err = c.appendPrimaryOverflowValue(
			scratch[:0], ref, bounds,
		)
		if err != nil {
			return scratch, err
		}
		if err = fn(key, scratch); err != nil {
			return scratch, err
		}
	}
}

// RangeRaw visits live rows in bytewise lexical key order. key and value are
// borrowed only for the callback; overflow values reuse one bounded buffer.
// Returning an error stops the scan immediately.
func (s *Snapshot) RangeRaw(fn func(key, value []byte) error) error {
	_, err := s.RangeRawBuffer(nil, fn)
	return err
}

// RangeRawBuffer is RangeRaw with caller-owned overflow storage. The returned
// slice preserves any grown capacity for the next scan. Inline-only scans and
// warmed overflow scans allocate nothing when scratch has sufficient capacity.
func (s *Snapshot) RangeRawBuffer(scratch []byte, fn func(key, value []byte) error) ([]byte, error) {
	if s == nil || s.collection == nil || s.state == nil {
		return scratch, ErrClosed
	}
	if fn == nil {
		return scratch, nil
	}
	return scratch, s.rangePrimaryGraph(nil, nil, nil, fn)
}

// rangePrimaryGraph is the ordered-primary scan core. lower is inclusive,
// upper is exclusive, and a non-empty prefix additionally bounds the result.
func (s *Snapshot) rangePrimaryGraph(
	lower, upper, prefix []byte,
	fn func(key, value []byte) error,
) error {
	state := s.state
	catalogBounds := storeio.GlobalTabletCatalogBounds{
		StoreID:                state.root.StoreID,
		SelectedRootGeneration: state.root.Generation,
		FileEnd:                state.fileEnd,
		NextLogicalID:          state.root.NextLogicalID,
	}
	leafBounds := storeio.CommonPrimaryLeafBounds{
		FileEnd:           state.fileEnd,
		NextLogicalID:     state.root.NextLogicalID,
		AllocationQuantum: state.root.PageSize,
	}
	var (
		cursor storeio.PrimaryGraphCursor
		err    error
	)
	if len(prefix) != 0 {
		err = storeio.InitPrimaryGraphPrefixCursor(
			&cursor, s.collection.cache, state.root.PrimaryRoot,
			catalogBounds, leafBounds, prefix,
		)
	} else {
		err = storeio.InitPrimaryGraphCursor(
			&cursor, s.collection.cache, state.root.PrimaryRoot,
			catalogBounds, leafBounds, lower, upper,
		)
	}
	if err != nil {
		return err
	}
	// Seed the cursor's splice buffer from the Snapshot's retained scratch and
	// hand the grown buffer back when the scan ends, so class-5 row
	// reconstruction reuses one allocation across every scan of this snapshot
	// rather than growing a fresh buffer per scan.
	cursor.AdoptSpliceScratch(s.scanSpliceScratch)
	defer func() {
		s.scanSpliceScratch = cursor.ReleaseSpliceScratch()
		cursor.Close()
	}()
	// The fused drain stops at each out-of-line row and returns its key and chain
	// head; the resolved value is reassembled into one reused buffer and delivered
	// to fn exactly like an inline value, then the drain is re-entered. Passing fn
	// straight through (no wrapper closure) keeps an inline scan allocation-free.
	for {
		key, ref, err := cursor.VisitInline(fn)
		if err != nil {
			return err
		}
		if ref == (storeio.PageRef{}) {
			return nil
		}
		s.overflowScanValue, err = s.collection.appendPrimaryOverflowValue(
			s.overflowScanValue[:0], ref, leafBounds,
		)
		if err != nil {
			return err
		}
		if err := fn(key, s.overflowScanValue); err != nil {
			return err
		}
	}
}

// RangeMasksRaw visits only the live stable slots named by ordered masks.
// Masks must be strictly increasing by Chunk; zero and dead bits are ignored.
// The callback order is identical to filtering RangeRaw, so query execution
// can push an exact index bound into page reads without changing LIMIT,
// grouping, or stable tie semantics. Inline key/value slices borrow one cache
// lease for the callback. One overflow buffer is reused for the complete call.
func (s *Snapshot) RangeMasksRaw(masks []store.Mask, fn func(key, value []byte) error) error {
	if s == nil || s.collection == nil || s.state == nil {
		return ErrClosed
	}
	if fn == nil {
		return nil
	}
	scratch, err := s.rangePrimaryMasks(
		masks, s.scanSpliceScratch,
		func(_ store.Location, key, value []byte) error {
			return fn(key, value)
		},
	)
	s.scanSpliceScratch = scratch
	return err
}

// RangeMasksRawBuffer is RangeMasksRaw with caller-owned overflow storage.
// The returned slice preserves capacity even when iteration stops with an
// error, allowing a retry loop to remain allocation-free.
func (s *Snapshot) RangeMasksRawBuffer(masks []store.Mask, scratch []byte, fn func(key, value []byte) error) ([]byte, error) {
	if s == nil || s.collection == nil || s.state == nil {
		return scratch, ErrClosed
	}
	if fn == nil {
		return scratch, nil
	}
	return s.rangePrimaryMasks(
		masks, scratch,
		func(_ store.Location, key, value []byte) error {
			return fn(key, value)
		},
	)
}

// RangeMasksRawRowsBuffer is the location-aware form of
// [Snapshot.RangeMasksRawBuffer]. The callback receives the stable row
// address selected from this snapshot. It is useful when a covering index
// decides most rows and a query must preserve first-row ordering while
// rechecking only the residual candidates.
//
// Masks must be strictly increasing by Chunk. Zero and dead bits are ignored.
// key and value borrow one cache lease for the callback; overflow storage is
// returned for reuse.
func (s *Snapshot) RangeMasksRawRowsBuffer(
	masks []store.Mask,
	scratch []byte,
	fn func(row store.Location, key, value []byte) error,
) ([]byte, error) {
	if s == nil || s.collection == nil || s.state == nil {
		return scratch, ErrClosed
	}
	if fn == nil {
		return scratch, nil
	}
	return s.rangePrimaryMasks(masks, scratch, fn)
}

// rangePrimaryMasks materializes selected rows from the snapshot-rooted primary
// graph. Masks are grouped by bucket (four quadrant tiles per bucket), paired
// with the immutable floor retained by this snapshot's router, and sorted by
// that floor. A coherently sampled handle no newer than the snapshot visibility
// cut pins its immutable page directly; a handle advanced after capture is
// resolved by floor through state.root.PrimaryRoot. Any row-overlay suffix in
// the snapshot cut is merged at that exact generation. Thus old exact masks,
// old leaf slots, and old row images all come from one generation, and callbacks
// retain the lexical order of RangeRaw after non-contiguous bucket allocation.
// Every selected bit is rechecked against the pinned exact epoch's live-slot
// map, so a mask that names a dead or absent slot fails closed.
func (s *Snapshot) rangePrimaryMasks(
	masks []store.Mask,
	scratch []byte,
	fn func(row store.Location, key, value []byte) error,
) ([]byte, error) {
	state := s.state
	router := s.primaryRouter
	if router == nil {
		return scratch, store.ErrMaskChunk
	}
	bounds := s.collection.primaryLeafBounds(state)
	catalogBounds := storeio.GlobalTabletCatalogBounds{
		StoreID:                state.root.StoreID,
		SelectedRootGeneration: state.root.PrimaryRoot.Generation,
		FileEnd:                state.fileEnd,
		NextLogicalID:          state.root.NextLogicalID,
	}
	// Liveness enforcement resolves through the pinned epoch's read rule at
	// this snapshot's generation: newest tile record ≤ G, else the flat
	// table — the same fence the probe's posting recheck uses.
	epoch := s.epoch
	enforceLive := epoch != nil
	atGen := state.root.Generation
	overlayTiles := enforceLive && epoch.tileRecordCount.Load() != 0
	// The Snapshot-owned buffer reassembles each selected out-of-line value, kept
	// separate from the posting scratch that borrows the row's key and descriptor.
	// Using the field rather than a captured local keeps the per-bucket callback
	// free of a boxed-capture allocation (zero-GC scan hot path).
	s.overflowScanValue = s.overflowScanValue[:0]
	s.maskGroups = s.maskGroups[:0]
	floorsOrdered := true
	var previous uint32
	for at := 0; at < len(masks); {
		if at != 0 && masks[at].Chunk <= previous {
			return scratch, store.ErrMaskOrder
		}
		bucket := masks[at].Chunk >> 2
		if bucket >= storeio.PrimaryBucketIDLimit {
			return scratch, store.ErrMaskChunk
		}
		group := primarySnapshotMaskGroup{
			bucket: storeio.BucketID(bucket),
		}
		for at < len(masks) && masks[at].Chunk>>2 == bucket {
			mask := masks[at]
			if at != 0 && mask.Chunk <= previous {
				return scratch, store.ErrMaskOrder
			}
			previous = mask.Chunk
			quadrant := mask.Chunk & 3
			if mask.Bits != 0 && enforceLive &&
				mask.Bits&^epoch.resolveLiveWord(
					mask.Chunk, atGen, overlayTiles,
				) != 0 {
				return scratch, store.ErrMaskChunk
			}
			group.selected[quadrant] = mask.Bits
			at++
		}
		var ok bool
		group.route, group.floor, ok = router.ResolveBucketFloor(group.bucket)
		if !ok {
			return scratch, store.ErrMaskChunk
		}
		if len(s.maskGroups) != 0 {
			order := bytes.Compare(
				s.maskGroups[len(s.maskGroups)-1].floor, group.floor,
			)
			if order == 0 {
				return scratch, store.ErrMaskChunk
			}
			floorsOrdered = floorsOrdered && order < 0
		}
		s.maskGroups = append(s.maskGroups, group)
	}
	if !floorsOrdered {
		slices.SortFunc(
			s.maskGroups,
			func(a, b primarySnapshotMaskGroup) int {
				return bytes.Compare(a.floor, b.floor)
			},
		)
		for i := 1; i < len(s.maskGroups); i++ {
			if bytes.Equal(s.maskGroups[i-1].floor, s.maskGroups[i].floor) {
				return scratch, store.ErrMaskChunk
			}
		}
	}
	if len(s.maskGroups) == 0 {
		return scratch, nil
	}
	// A retained router handle whose leaf generation is not newer than the
	// snapshot visibility cut still names an immutable page that belongs to this
	// snapshot and takes the original one-pin fast path. A bucket rewritten after
	// capture falls back to
	// one lazy rooted cursor; multiple stale buckets share that descent and its
	// lexical successors. Sampling the handle and generation through the router
	// seqlock is race-safe even while a newer publisher flips it.
	var cursor storeio.PrimaryGraphCursor
	defer cursor.Close()
	rooted := false
	for groupAt := range s.maskGroups {
		group := &s.maskGroups[groupAt]
		var (
			page  []byte
			lease storeio.PageLease
			err   error
		)
		if group.route.Ref.Generation <= state.root.Generation {
			lease, err = router.AcquireLeaf(s.collection.cache, group.route)
			if err != nil {
				return scratch, err
			}
			page = lease.Page()
		} else {
			if !rooted {
				if err = storeio.InitPrimaryGraphCursor(
					&cursor, s.collection.cache, state.root.PrimaryRoot,
					catalogBounds, bounds, group.floor, nil,
				); err != nil {
					return scratch, err
				}
				rooted = true
			} else if err = cursor.NextLeaf(); err != nil {
				return scratch, err
			}
			for {
				bucket, rootedPage, pageOK := cursor.CurrentUnifiedLeafPage()
				if !pageOK {
					return scratch, store.ErrMaskChunk
				}
				if bucket == group.bucket {
					page = rootedPage
					break
				}
				if err = cursor.NextLeaf(); err != nil {
					return scratch, err
				}
			}
		}
		if state.root.Generation > state.root.PrimaryRoot.Generation &&
			s.collection.primaryUnifiedOverlay.pendingBucket(group.bucket) {
			scratch, err = s.rangePrimaryOverlayMaskedLeaf(
				page, group.bucket, bounds, group.selected, scratch, fn,
			)
		} else {
			scratch, err = storeio.VisitPrimaryLeafSelectedPostingRows(
				page, state.root.StoreID, group.bucket, bounds,
				group.selected, scratch,
				func(slot uint8, key, raw []byte, overflow bool) error {
					quadrant := slot >> 6
					value := raw
					if overflow {
						// The posting scratch already holds the borrowed
						// key/descriptor; keep the resolved value separate.
						resolved, rErr :=
							s.collection.appendPrimaryOverflowValue(
								s.overflowScanValue[:0],
								storeio.DecodePrimaryOverflowRef(raw), bounds,
							)
						if rErr != nil {
							return rErr
						}
						s.overflowScanValue = resolved
						value = resolved
					}
					return fn(store.Location{
						Chunk: uint32(group.bucket)<<2 | uint32(quadrant),
						Slot:  slot & 63,
					}, key, value)
				},
			)
		}
		lease.Release()
		if err != nil {
			return scratch, err
		}
	}
	return scratch, nil
}

// rangePrimaryOverlayMaskedLeaf merges one snapshot generation's row-overlay
// records with its immutable class-5 base in lexical order. The generation
// bound is essential: the shared overlay may already contain records from a
// newer publication, but an older snapshot must consume only records visible at
// its own cut.
func (s *Snapshot) rangePrimaryOverlayMaskedLeaf(
	page []byte,
	bucket storeio.BucketID,
	bounds storeio.CommonPrimaryLeafBounds,
	selected [4]uint64,
	scratch []byte,
	fn func(row store.Location, key, value []byte) error,
) ([]byte, error) {
	unified, ok := storeio.AdmittedCommonPrimaryUnifiedLeaf(
		page, s.state.root.StoreID, bucket, bounds,
	)
	if !ok {
		return scratch, storeio.ErrCommonPrimaryLeafCorrupt
	}
	slots, ok := unified.PostingSlots()
	if !ok {
		return scratch, storeio.ErrCommonPrimaryLeafCorrupt
	}
	overlay := s.collection.primaryUnifiedOverlay
	var indexes [storeio.CommonPrimaryLeafWideSlots]uint16
	overlayCount, overlayErr := overlay.latestBucketRecords(
		&indexes, bucket, s.state.root.Generation,
	)
	if overlayErr != nil {
		return scratch, overlayErr
	}
	emit := func(
		slot uint8, key, raw []byte, overflow, encoded bool,
	) error {
		quadrant := slot >> 6
		if selected[quadrant]&(uint64(1)<<uint(slot&63)) == 0 {
			return nil
		}
		value := raw
		if overflow {
			resolved, err := s.collection.appendPrimaryOverflowValue(
				s.overflowScanValue[:0],
				storeio.DecodePrimaryOverflowRef(raw), bounds,
			)
			if err != nil {
				return err
			}
			s.overflowScanValue = resolved
			value = resolved
		} else if encoded {
			scratch = unified.AppendAdmittedRowBody(scratch[:0], raw)
			value = scratch
		}
		return fn(store.Location{
			Chunk: uint32(bucket)<<2 | uint32(quadrant),
			Slot:  slot & 63,
		}, key, value)
	}

	baseRank, overlayAt := 0, 0
	for baseRank < unified.Len() || overlayAt < overlayCount {
		var baseKey, baseBody []byte
		var baseOverflow bool
		if baseRank < unified.Len() {
			var rowOK bool
			baseKey, baseBody, baseOverflow, rowOK =
				unified.RowRawAt(baseRank)
			if !rowOK {
				return scratch, storeio.ErrCommonPrimaryLeafCorrupt
			}
		}
		var record *primaryUnifiedOverlayRecord
		var overlayKey []byte
		if overlayAt < overlayCount {
			record = &overlay.records[indexes[overlayAt]]
			keyEnd := record.keyOffset + uint32(record.keyLen)
			if keyEnd > uint32(len(overlay.arena)) {
				return scratch, storeio.ErrCommonPrimaryLeafCorrupt
			}
			overlayKey = overlay.arena[record.keyOffset:keyEnd:keyEnd]
		}
		order := 0
		switch {
		case baseRank >= unified.Len():
			order = 1
		case overlayAt >= overlayCount:
			order = -1
		default:
			order = bytes.Compare(baseKey, overlayKey)
		}
		if order < 0 {
			if err := emit(
				slots[baseRank], baseKey, baseBody, baseOverflow, true,
			); err != nil {
				return scratch, err
			}
			baseRank++
			continue
		}
		if record.kind == primaryUnifiedOverlayPut {
			valueEnd := record.valueOff + record.valueLen
			if record.valueLen == 0 ||
				valueEnd > uint32(len(overlay.arena)) {
				return scratch, storeio.ErrCommonPrimaryLeafCorrupt
			}
			if err := emit(
				record.slot, overlayKey,
				overlay.arena[record.valueOff:valueEnd:valueEnd],
				false, false,
			); err != nil {
				return scratch, err
			}
		} else if record.kind != primaryUnifiedOverlayDelete {
			return scratch, storeio.ErrCommonPrimaryLeafCorrupt
		}
		overlayAt++
		if order == 0 {
			baseRank++
		}
	}
	return scratch, nil
}
