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

// primaryOverflowExtentGeometry returns the value bytes and rounded extent
// length for the next page of an overflow chain. Keeping this arithmetic in one
// helper makes the pre-plan and encoder identical, and widening before the
// round-up keeps the calculation exact on 386.
func (c *Collection) primaryOverflowExtentGeometry(remaining int) (
	piece int, extent uint32, ok bool,
) {
	quantum := uint64(c.options.PageSize)
	maximum := uint64(c.options.MaxPageSize)
	if remaining <= 0 || quantum == 0 || maximum < primaryOverflowPageOverhead {
		return 0, 0, false
	}
	perPage := c.options.MaxPageSize - primaryOverflowPageOverhead
	piece = min(perPage, remaining)
	raw := uint64(primaryOverflowPageOverhead) + uint64(piece)
	if raw > math.MaxUint64-(quantum-1) {
		return 0, 0, false
	}
	rounded := (raw + quantum - 1) / quantum * quantum
	if rounded < quantum || rounded > maximum || rounded > math.MaxUint32 {
		return 0, 0, false
	}
	return piece, uint32(rounded), true
}

// planBufferedPrimaryOverflowChain assigns one volatile chain's complete
// physical and logical identity without touching the cache. Batch publication
// uses this read-only pass to prove FileEnd/NextLogicalID and dirty-capacity
// bounds for every chain before the first frame is admitted.
func (c *Collection) planBufferedPrimaryOverflowChain(
	value []byte, generation, baseOffset, baseLogicalID uint64,
) (head storeio.PageRef, totalBytes uint64, pages int, err error) {
	if len(value) == 0 || len(value) > c.options.MaxDocumentBytes ||
		generation == 0 || baseLogicalID == 0 || c.options.PageSize <= 0 ||
		baseOffset%uint64(c.options.PageSize) != 0 {
		return storeio.PageRef{}, 0, 0, fmt.Errorf(
			"%w: overflow value layout", storeio.ErrInvalidWrite,
		)
	}
	offset := baseOffset
	logicalID := baseLogicalID
	remaining := len(value)
	for remaining != 0 {
		piece, extent, ok := c.primaryOverflowExtentGeometry(remaining)
		if !ok || offset > math.MaxUint64-uint64(extent) ||
			logicalID == math.MaxUint64 {
			return storeio.PageRef{}, 0, 0, storeio.ErrInvalidWrite
		}
		ref := storeio.PageRef{
			Offset: offset, LogicalID: logicalID,
			Generation: generation, Length: extent,
			Kind: storeio.PageOverflow,
		}
		if pages == 0 {
			head = ref
		}
		offset += uint64(extent)
		logicalID++
		pages++
		remaining -= piece
	}
	return head, offset - baseOffset, pages, nil
}

// admitBufferedPrimaryOverflowChain encodes and admits a chain whose identities
// were fixed by planBufferedPrimaryOverflowChain. admitted is updated after
// each successful frame, so a later failure can discard the exact staged prefix
// without leaking dirty cache capacity.
func (c *Collection) admitBufferedPrimaryOverflowChain(
	value []byte, generation uint64, head storeio.PageRef,
	fileEnd, nextLogicalID uint64, admitted *[]storeio.PageRef,
) error {
	planned, totalBytes, pages, err := c.planBufferedPrimaryOverflowChain(
		value, generation, head.Offset, head.LogicalID,
	)
	if err != nil {
		return err
	}
	if planned != head || head.Offset > math.MaxUint64-totalBytes ||
		head.Offset+totalBytes > fileEnd ||
		uint64(pages) > math.MaxUint64-head.LogicalID ||
		head.LogicalID+uint64(pages) > nextLogicalID {
		return storeio.ErrInvalidWrite
	}
	if cap(c.overflowPageScratch) < c.options.MaxPageSize {
		c.overflowPageScratch = make([]byte, c.options.MaxPageSize)
	}
	offset := head.Offset
	logicalID := head.LogicalID
	valueOffset := 0
	for valueOffset < len(value) {
		piece, extent, ok := c.primaryOverflowExtentGeometry(
			len(value) - valueOffset,
		)
		if !ok {
			return storeio.ErrInvalidWrite
		}
		ref := storeio.PageRef{
			Offset: offset, LogicalID: logicalID,
			Generation: generation, Length: extent,
			Kind: storeio.PageOverflow,
		}
		var next storeio.PageRef
		if valueOffset+piece < len(value) {
			nextPiece, nextExtent, nextOK := c.primaryOverflowExtentGeometry(
				len(value) - valueOffset - piece,
			)
			if !nextOK || nextPiece == 0 ||
				offset > math.MaxUint64-uint64(extent) ||
				logicalID == math.MaxUint64 {
				return storeio.ErrInvalidWrite
			}
			next = storeio.PageRef{
				Offset: offset + uint64(extent), LogicalID: logicalID + 1,
				Generation: generation, Length: nextExtent,
				Kind: storeio.PageOverflow,
			}
		}
		buf := c.overflowPageScratch[:int(extent)]
		header := storeio.OverflowPageHeader{
			StoreID: c.storeID, Generation: generation,
			LogicalID: logicalID, PageSize: extent,
			Total: uint64(len(value)), Offset: uint64(valueOffset), Next: next,
		}
		if _, err := storeio.EncodeOverflowPage(
			buf, header, value[valueOffset:valueOffset+piece],
			fileEnd, nextLogicalID, uint32(c.options.PageSize),
		); err != nil {
			return err
		}
		if err := c.cache.AdmitBufferedDirty(ref, buf, math.MaxUint64); err != nil {
			return err
		}
		*admitted = append(*admitted, ref)
		offset += uint64(extent)
		logicalID++
		valueOffset += piece
	}
	return nil
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
	head, totalBytes, pages, err = c.planBufferedPrimaryOverflowChain(
		value, generation, baseOffset, baseLogicalID,
	)
	if err != nil {
		return storeio.PageRef{}, 0, 0, err
	}
	fileEnd := baseOffset + totalBytes
	nextLogicalID := baseLogicalID + uint64(pages)
	if err := c.admitBufferedPrimaryOverflowChain(
		value, generation, head, fileEnd, nextLogicalID,
		&c.primaryMutationAdmitted,
	); err != nil {
		return storeio.PageRef{}, 0, 0, err
	}
	return head, totalBytes, pages, nil
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
