package durable

import (
	"bytes"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
)

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

// RangeRawReadAheadBuffer is currently an alias of RangeRawBuffer, retained
// for its callers in the query layer: the ordered-primary scan reads inline
// values from the leaves it already walks in order, so it has no separate
// document-extent read-ahead lane.
func (s *Snapshot) RangeRawReadAheadBuffer(scratch []byte, fn func(key, value []byte) error) ([]byte, error) {
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

// rangePrimaryMasks materializes selected rows from the ordered primary graph.
// Masks are grouped by bucket (four quadrant tiles per bucket), the bucket's
// leaf is resolved through the resident router, and its live rows are visited in
// lexical order with their posting-stable slot; a row whose tile/bit is selected
// is delivered. For an indexed primary every selected bit is rechecked against
// the live-slot map, so a mask that names a dead or absent slot fails closed.
func (s *Snapshot) rangePrimaryMasks(
	masks []store.Mask,
	scratch []byte,
	fn func(row store.Location, key, value []byte) error,
) ([]byte, error) {
	state := s.state
	router := s.collection.primaryRouter.Load()
	if router == nil {
		return scratch, store.ErrMaskChunk
	}
	bounds := s.collection.primaryLeafBounds(state)
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
	var previous uint32
	for at := 0; at < len(masks); {
		if at != 0 && masks[at].Chunk <= previous {
			return scratch, store.ErrMaskOrder
		}
		bucket := masks[at].Chunk >> 2
		if bucket >= storeio.PrimaryBucketIDLimit {
			return scratch, store.ErrMaskChunk
		}
		var selected [4]uint64
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
			selected[quadrant] = mask.Bits
			at++
		}
		route, ok := router.ResolveBucketID(storeio.BucketID(bucket))
		if !ok {
			return scratch, store.ErrMaskChunk
		}
		lease, err := router.AcquireLeaf(s.collection.cache, route)
		if err != nil {
			return scratch, err
		}
		if s.collection.primaryUnifiedOverlay.pendingBucket(route.Bucket) {
			scratch, err = s.rangePrimaryOverlayMaskedLeaf(
				lease.Page(), route.Bucket, bounds, selected,
				scratch, fn,
			)
		} else {
			scratch, err = storeio.VisitPrimaryLeafSelectedPostingRows(
				lease.Page(), state.root.StoreID, route.Bucket, bounds,
				selected, scratch,
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
						Chunk: uint32(route.Bucket)<<2 | uint32(quadrant),
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

// rangePrimaryOverlayMaskedLeaf merges a bucket's newest visible row-overlay
// records with its immutable class-5 base in lexical order. This is the masked
// counterpart of point reads' overlay-first rule: inserted rows are visible,
// replacements expose their new value, and tombstones suppress the base row.
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
			overlayKey =
				overlay.arena[record.keyOffset:keyEnd:keyEnd]
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
