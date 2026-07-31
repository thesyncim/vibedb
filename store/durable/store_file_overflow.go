package durable

import (
	"fmt"
	"math"

	"github.com/thesyncim/vibedb/internal/storeio"
)

// Overflow-on-Put stores a document value that exceeds the inline leaf budget
// (InlineValueBytes) out of line, as a forward-linked chain of PageOverflow
// extents. The leaf record then holds only the 32-byte PageRef of the chain
// head, so a leaf carrying large values stays small through the ordinary
// structural machinery.

// ErrOverflowChainCorrupt reports that a resolved overflow chain does not agree
// with its own length or link metadata. The individual page codec already
// checksum- and bounds-validates each extent; this is the cross-page invariant
// (contiguous offsets summing to Total) the chain walker enforces.
var ErrOverflowChainCorrupt = fmt.Errorf(
	"%w: overflow value chain", storeio.ErrOverflowPageCorrupt,
)

// primaryOverflowPageOverhead is the per-page framing an overflow extent spends
// before value bytes: common header, common trailer, and the overflow payload
// header.
const primaryOverflowPageOverhead = storeio.PageHeaderSize +
	storeio.PageTrailerSize + storeio.OverflowPagePayloadHeaderSize

// primaryOverflowValueIsInline reports whether a value of this length fits the
// collection's inline leaf budget and so never takes the overflow path.
func (c *Collection) primaryOverflowValueIsInline(length int) bool {
	return length <= c.options.InlineValueBytes
}

// primaryOverflowPageCount reports how many overflow extents a value of the
// given length occupies at this collection's geometry. It is the chain-length
// bound the resolver and retirement walkers cap against.
func (c *Collection) primaryOverflowPageCount(length int) int {
	perPage := c.options.MaxPageSize - primaryOverflowPageOverhead
	if perPage <= 0 || length <= 0 {
		return 0
	}
	return 1 + (length-1)/perPage
}

// stagePrimaryOverflowChain writes value out of line as a forward-linked chain
// of PageOverflow extents allocated from tx, and returns the head PageRef to
// embed in the leaf record. Each extent is sized to its own piece (rounded up to
// the allocation quantum, capped at MaxPageSize) so a value one byte past the
// inline threshold occupies a single quantum rather than a full MaxPageSize
// frame. Pieces are allocated in order so each extent's freshly minted LogicalID
// is strictly less than its successor's, which is exactly the forward-link rule
// the overflow codec enforces (Next.LogicalID > header.LogicalID).
func (c *Collection) stagePrimaryOverflowChain(
	tx *storeio.WriteTransaction, value []byte, generation uint64,
) (storeio.PageRef, error) {
	quantum := uint32(c.options.PageSize)
	maxExtent := uint32(c.options.MaxPageSize)
	perPage := int(maxExtent) - primaryOverflowPageOverhead
	if perPage <= 0 || len(value) == 0 ||
		len(value) > c.options.MaxDocumentBytes {
		return storeio.PageRef{}, fmt.Errorf(
			"%w: overflow value length", storeio.ErrInvalidWrite,
		)
	}
	total := uint64(len(value))
	c.overflowChainScratch = c.overflowChainScratch[:0]
	c.overflowOffsetScratch = c.overflowOffsetScratch[:0]
	// Reserve every extent first so each piece's Next can name a later, higher-
	// LogicalID extent that already exists.
	for offset := 0; offset < len(value); {
		n := min(perPage, len(value)-offset)
		raw := primaryOverflowPageOverhead + n
		extent := (uint32(raw) + quantum - 1) / quantum * quantum
		if extent < quantum {
			extent = quantum
		}
		page, err := tx.Allocate(storeio.PageOverflow, extent, 0)
		if err != nil {
			return storeio.PageRef{}, err
		}
		c.overflowChainScratch = append(c.overflowChainScratch, page)
		c.overflowOffsetScratch = append(c.overflowOffsetScratch, offset)
		offset += n
	}
	// Bounds are read after every allocation so a piece's Next reference resolves
	// against the transaction's advanced FileEnd and NextLogicalID.
	fileEnd := tx.FileEnd()
	nextLogicalID := tx.NextLogicalID()
	for i := range c.overflowChainScratch {
		page := c.overflowChainScratch[i]
		start := c.overflowOffsetScratch[i]
		end := len(value)
		var next storeio.PageRef
		if i+1 < len(c.overflowChainScratch) {
			end = c.overflowOffsetScratch[i+1]
			next = c.overflowChainScratch[i+1].Ref()
		}
		header := storeio.OverflowPageHeader{
			StoreID: c.storeID, Generation: generation,
			LogicalID: page.Ref().LogicalID, PageSize: page.Ref().Length,
			Total: total, Offset: uint64(start), Next: next,
		}
		if _, err := storeio.EncodeOverflowPage(
			page.Bytes(), header, value[start:end], fileEnd, nextLogicalID,
			quantum,
		); err != nil {
			return storeio.PageRef{}, err
		}
		if err := page.Stage(); err != nil {
			return storeio.PageRef{}, err
		}
	}
	return c.overflowChainScratch[0].Ref(), nil
}

// mintBufferedPrimaryOverflowChain stores value out of line as a forward-linked
// chain of PageOverflow extents admitted as VOLATILE, memory-only buffered-dirty
// frames — the deferred-canonical lane writes no device bytes at Put. It lays the
// chain out starting at baseOffset (a quantum-aligned offset in the collection's
// volatile file region) with fresh logical IDs from baseLogicalID, exactly as a
// write transaction would allocate them, so the returned head embeds in the leaf
// and every forward link validates once the caller advances the visible FileEnd
// and NextLogicalID to cover the chain. It returns the head PageRef, the total
// file bytes the chain occupies (so the caller places the leaf immediately past
// it and advances FileEnd), and the extent count (so the caller advances
// NextLogicalID). Extents ascend in both offset and logical ID, which is exactly
// the forward-link rule the overflow codec enforces (Next.LogicalID > header's).
func (c *Collection) mintBufferedPrimaryOverflowChain(
	value []byte, generation uint64,
	baseOffset, baseLogicalID uint64,
) (head storeio.PageRef, totalBytes uint64, pages int, err error) {
	quantum := uint32(c.options.PageSize)
	maxExtent := uint32(c.options.MaxPageSize)
	perPage := int(maxExtent) - primaryOverflowPageOverhead
	if perPage <= 0 || len(value) == 0 ||
		len(value) > c.options.MaxDocumentBytes {
		return storeio.PageRef{}, 0, 0, fmt.Errorf(
			"%w: overflow value length", storeio.ErrInvalidWrite,
		)
	}
	total := uint64(len(value))
	c.overflowRefScratch = c.overflowRefScratch[:0]
	c.overflowOffsetScratch = c.overflowOffsetScratch[:0]
	// Reserve every extent's identity first so each piece's Next names a later,
	// higher-logical-ID extent whose offset and length are already fixed.
	fileOffset := baseOffset
	logicalID := baseLogicalID
	for start := 0; start < len(value); {
		n := min(perPage, len(value)-start)
		raw := primaryOverflowPageOverhead + n
		extent := (uint32(raw) + quantum - 1) / quantum * quantum
		if extent < quantum {
			extent = quantum
		}
		if fileOffset > math.MaxUint64-uint64(extent) {
			return storeio.PageRef{}, 0, 0, storeio.ErrInvalidWrite
		}
		c.overflowRefScratch = append(c.overflowRefScratch, storeio.PageRef{
			Offset: fileOffset, LogicalID: logicalID,
			Generation: generation, Length: extent,
			Kind: storeio.PageOverflow,
		})
		c.overflowOffsetScratch = append(c.overflowOffsetScratch, start)
		fileOffset += uint64(extent)
		logicalID++
		start += n
	}
	pages = len(c.overflowRefScratch)
	totalBytes = fileOffset - baseOffset
	// Bounds are read after the whole layout is fixed so every Next reference
	// resolves against the ceilings the caller will publish for the visible state.
	fileEnd := fileOffset
	nextLogicalID := baseLogicalID + uint64(pages)
	if cap(c.overflowPageScratch) < int(maxExtent) {
		c.overflowPageScratch = make([]byte, maxExtent)
	}
	for i := range c.overflowRefScratch {
		ref := c.overflowRefScratch[i]
		start := c.overflowOffsetScratch[i]
		end := len(value)
		var next storeio.PageRef
		if i+1 < len(c.overflowRefScratch) {
			end = c.overflowOffsetScratch[i+1]
			next = c.overflowRefScratch[i+1]
		}
		header := storeio.OverflowPageHeader{
			StoreID: c.storeID, Generation: generation,
			LogicalID: ref.LogicalID, PageSize: ref.Length,
			Total: total, Offset: uint64(start), Next: next,
		}
		buf := c.overflowPageScratch[:ref.Length]
		if _, err := storeio.EncodeOverflowPage(
			buf, header, value[start:end], fileEnd, nextLogicalID,
			quantum,
		); err != nil {
			return storeio.PageRef{}, 0, 0, err
		}
		if err := c.cache.AdmitBufferedDirty(
			ref, buf, math.MaxUint64,
		); err != nil {
			return storeio.PageRef{}, 0, 0, err
		}
		// Record the admission immediately so a failure on a LATER extent (or any
		// fallible caller step after the whole chain is minted) can hand every
		// already-admitted frame back through unadmitPrimaryMutationFrames;
		// nothing references these frames until the caller publishes.
		c.primaryMutationAdmitted = append(c.primaryMutationAdmitted, ref)
	}
	return c.overflowRefScratch[0], totalBytes, pages, nil
}

// appendPrimaryOverflowValue walks the overflow chain rooted at first and
// appends the reassembled value to dst. Every extent is checksum- and
// bounds-validated by the page codec against the reader's snapshot bounds; the
// walk additionally proves the pieces are contiguous (each Offset equals the
// running length) and sum to a single agreed Total, so a grafted or reordered
// chain fails closed rather than yielding a silently wrong value.
func (c *Collection) appendPrimaryOverflowValue(
	dst []byte, first storeio.PageRef, bounds storeio.CommonPrimaryLeafBounds,
) ([]byte, error) {
	ref := first
	var total, have uint64
	for pages := 0; ref != (storeio.PageRef{}); pages++ {
		if pages > 0 && uint64(pages) > total {
			return dst, ErrOverflowChainCorrupt
		}
		lease, err := c.cache.Acquire(ref)
		if err != nil {
			return dst, err
		}
		view, err := storeio.OpenOverflowPage(
			lease.Page(), bounds.FileEnd, bounds.NextLogicalID,
			bounds.AllocationQuantum,
		)
		if err != nil {
			lease.Release()
			return dst, err
		}
		header := view.Header()
		if pages == 0 {
			total = header.Total
		} else if header.Total != total || header.Offset != have {
			lease.Release()
			return dst, ErrOverflowChainCorrupt
		}
		dst = append(dst, view.Data()...)
		have += uint64(len(view.Data()))
		ref = header.Next
		lease.Release()
	}
	if have != total {
		return dst, ErrOverflowChainCorrupt
	}
	return dst, nil
}

// collectPrimaryOverflowExtents appends every extent of the overflow chain
// rooted at first to dst, so a value replaced or deleted by a mutation can retire
// its old out-of-line pages through the ordinary retirement accounting. The walk
// trusts the same per-page codec validation as the resolver.
func (c *Collection) collectPrimaryOverflowExtents(
	dst []storeio.PageRef, first storeio.PageRef,
	bounds storeio.CommonPrimaryLeafBounds,
) ([]storeio.PageRef, error) {
	ref := first
	var total, have uint64
	for pages := 0; ref != (storeio.PageRef{}); pages++ {
		if pages > 0 && uint64(pages) > total {
			return dst, ErrOverflowChainCorrupt
		}
		lease, err := c.cache.Acquire(ref)
		if err != nil {
			return dst, err
		}
		view, err := storeio.OpenOverflowPage(
			lease.Page(), bounds.FileEnd, bounds.NextLogicalID,
			bounds.AllocationQuantum,
		)
		if err != nil {
			lease.Release()
			return dst, err
		}
		header := view.Header()
		if pages == 0 {
			total = header.Total
		}
		dst = append(dst, ref)
		have += uint64(len(view.Data()))
		ref = header.Next
		lease.Release()
	}
	if have != total {
		return dst, ErrOverflowChainCorrupt
	}
	return dst, nil
}
