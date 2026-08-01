package store

import (
	"crypto/rand"
	"fmt"
	"math"
	"math/bits"
	"runtime"
	"slices"
	"unsafe"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/internal/storemem"
)

const (
	storePackedIndexPageSize       = 4096
	storePackedIndexFirstLogicalID = uint64(2)
	storePackedIndexMaxSegments    = 82
	storePackedIndexMaxEntries     = 4096
)

type storePackedIndexRef struct {
	hash    uint64
	page    uint32
	segment uint16
	_       uint16
}

const storePackedIndexRefBytes = 16

var _ [storePackedIndexRefBytes - unsafe.Sizeof(storePackedIndexRef{})]byte
var _ [unsafe.Sizeof(storePackedIndexRef{}) - storePackedIndexRefBytes]byte

// storePackedIndex is an immutable exact-index base. Page bytes and the sorted
// hash directory share one pointer-free anonymous block; the sole Go slice of
// admitted views contributes one pointer per physical page, never per value,
// posting, or document.
type storePackedIndex struct {
	refs          []storePackedIndexRef
	views         []storeio.PostingPageView
	block         *storemem.Block
	fingerprints  uint64
	chunkWords    uint64
	candidateRows uint64
}

// storePackedIndexIterator walks one immutable tuple-hash stream across page
// segments. Keeping the cursor as a value lets callers merge it with a mutable
// posting in one loop, without closure capture or a second root lookup.
type storePackedIndexIterator struct {
	index    *storePackedIndex
	segment  storeio.PostingSegmentView
	entries  storeio.PostingIterator
	hash     uint64
	streamID uint32
}

type storePackedBuildStream struct {
	hash    uint64
	entries []storeIndexChunkMask
}

// storeIndexPending materializes a completed transient posting HAMT for the
// one-time packed-base fold. It is never retained or used by steady queries.
func storeIndexPending(root *storeIndexPostingNode) map[uint64][]storeIndexChunkMask {
	if root == nil {
		return nil
	}
	pending := make(map[uint64][]storeIndexChunkMask)
	var walk func(*storeIndexPostingNode)
	walk = func(node *storeIndexPostingNode) {
		if node == nil {
			return
		}
		for i := range node.slots {
			slot := node.slots[i]
			if slot.child != nil {
				walk(slot.child)
			}
			if slot.leaf == nil {
				continue
			}
			entries := make([]storeIndexChunkMask, 0, int(slot.leaf.masks.n)+int(slot.leaf.masks.wide.words))
			slot.leaf.masks.each(func(chunk uint32, mask uint64) bool {
				entries = append(entries, storeIndexChunkMask{chunk: chunk, mask: mask})
				return true
			})
			pending[slot.leaf.hash] = entries
		}
	}
	walk(root)
	return pending
}

func newStorePackedIndex(pending map[uint64][]storeIndexChunkMask) (*storePackedIndex, error) {
	if len(pending) == 0 {
		return nil, nil
	}
	if uint64(len(pending)) > uint64(^uint32(0)) {
		return nil, ErrCheckpointTooLarge
	}
	streams := make([]storePackedBuildStream, 0, len(pending))
	for hash, entries := range pending {
		streams = append(streams, storePackedBuildStream{hash: hash, entries: entries})
	}
	slices.SortFunc(streams, func(a, b storePackedBuildStream) int {
		switch {
		case a.hash < b.hash:
			return -1
		case a.hash > b.hash:
			return 1
		default:
			return 0
		}
	})
	pageCount, err := storePackedIndexPageCount(streams)
	if err != nil {
		return nil, err
	}
	if uint64(pageCount) > uint64(^uint32(0)) {
		return nil, ErrCheckpointTooLarge
	}
	if pageCount < 0 || pageCount > math.MaxInt/storePackedIndexPageSize ||
		len(streams) > math.MaxInt/storePackedIndexRefBytes {
		return nil, ErrCheckpointTooLarge
	}
	pageBytes := pageCount * storePackedIndexPageSize
	refBytes := len(streams) * storePackedIndexRefBytes
	if pageBytes > math.MaxInt-refBytes {
		return nil, ErrCheckpointTooLarge
	}
	block, err := storemem.Allocate(pageBytes + refBytes)
	if err != nil {
		return nil, err
	}
	data := block.Bytes()
	refs := unsafe.Slice((*storePackedIndexRef)(unsafe.Pointer(&data[pageBytes])), len(streams))
	packed := &storePackedIndex{
		refs: refs, views: make([]storeio.PostingPageView, pageCount), block: block,
		fingerprints: uint64(len(streams)),
	}
	runtime.SetFinalizer(packed, (*storePackedIndex).release)
	if err := packed.encode(data[:pageBytes], streams); err != nil {
		packed.release()
		return nil, err
	}
	return packed, nil
}

func (p *storePackedIndex) release() {
	if p == nil || p.block == nil {
		return
	}
	_ = p.block.Close()
	p.block = nil
	p.refs = nil
	p.views = nil
}

func (p *storePackedIndex) externalBytes() uint64 {
	if p == nil || p.block == nil || !p.block.OutsideHeap() {
		return 0
	}
	return uint64(p.block.Len())
}

func storePackedIndexPageCount(streams []storePackedBuildStream) (int, error) {
	pages := 0
	used := storeio.PostingPagePayloadHeaderSize
	flush := func() {
		if used != storeio.PostingPagePayloadHeaderSize {
			pages++
			used = storeio.PostingPagePayloadHeaderSize
		}
	}
	for _, stream := range streams {
		remaining := stream.entries
		for len(remaining) != 0 {
			capacity := storePackedIndexPageSize - storeio.PageHeaderSize - storeio.PageTrailerSize -
				used - storeio.PostingSegmentHeaderSize
			count, encoded, err := storePackedIndexPrefix(remaining, capacity)
			if err != nil {
				if used != storeio.PostingPagePayloadHeaderSize {
					flush()
					continue
				}
				return 0, err
			}
			used += storeio.PostingSegmentHeaderSize + encoded
			remaining = remaining[count:]
			if len(remaining) != 0 {
				flush()
			}
		}
	}
	flush()
	return pages, nil
}

func storePackedIndexPrefix(entries []storeIndexChunkMask, capacity int) (count, encoded int, err error) {
	if len(entries) == 0 || capacity <= 0 {
		return 0, 0, storeio.ErrInvalidWrite
	}
	previous := entries[0].chunk
	limit := min(len(entries), int(^uint16(0)))
	for i := range limit {
		entry := storeio.PostingEntry{Chunk: entries[i].chunk, Bits: entries[i].mask}
		size, sizeErr := storeio.PostingEntryEncodedSize(previous, entry, i == 0)
		if sizeErr != nil {
			return 0, 0, sizeErr
		}
		if encoded+size > capacity {
			break
		}
		encoded += size
		count++
		previous = entry.Chunk
	}
	if count == 0 {
		return 0, 0, storeio.ErrInvalidWrite
	}
	return count, encoded, nil
}

func (p *storePackedIndex) encode(pageBytes []byte, streams []storePackedBuildStream) error {
	var storeID [16]byte
	if _, err := rand.Read(storeID[:]); err != nil {
		return fmt.Errorf("vibedb: packed exact-index identity: %w", err)
	}
	segments := make([]storeio.PostingSegment, 0, storePackedIndexMaxSegments)
	entries := make([]storeio.PostingEntry, 0, storePackedIndexMaxEntries)
	pageIndex := 0
	used := storeio.PostingPagePayloadHeaderSize
	nextLogicalID := storePackedIndexFirstLogicalID + uint64(len(p.views))
	flush := func() error {
		if len(segments) == 0 {
			return nil
		}
		start := pageIndex * storePackedIndexPageSize
		page := pageBytes[start : start+storePackedIndexPageSize]
		if _, err := storeio.EncodePostingPage(page, storeio.PostingPageHeader{
			StoreID: storeID, Generation: 1,
			LogicalID: storePackedIndexFirstLogicalID + uint64(pageIndex),
			PageSize:  storePackedIndexPageSize, IndexID: 0,
		}, segments, nextLogicalID, 1); err != nil {
			return err
		}
		view, err := storeio.OpenPostingPage(page, nextLogicalID, 1)
		if err != nil {
			return err
		}
		p.views[pageIndex] = view
		pageIndex++
		segments = segments[:0]
		entries = entries[:0]
		used = storeio.PostingPagePayloadHeaderSize
		return nil
	}

	for streamIndex, stream := range streams {
		remaining := stream.entries
		first := true
		for len(remaining) != 0 {
			capacity := storePackedIndexPageSize - storeio.PageHeaderSize - storeio.PageTrailerSize -
				used - storeio.PostingSegmentHeaderSize
			count, encoded, err := storePackedIndexPrefix(remaining, capacity)
			if err != nil {
				if len(segments) != 0 {
					if err := flush(); err != nil {
						return err
					}
					continue
				}
				return err
			}
			if first {
				p.refs[streamIndex] = storePackedIndexRef{
					hash: stream.hash, page: uint32(pageIndex), segment: uint16(len(segments)),
				}
				first = false
			}
			entryStart := len(entries)
			for _, entry := range remaining[:count] {
				entries = append(entries, storeio.PostingEntry{Chunk: entry.chunk, Bits: entry.mask})
				p.chunkWords++
				p.candidateRows += uint64(bits.OnesCount64(entry.mask))
			}
			segment := storeio.PostingSegment{
				StreamID: uint32(streamIndex + 1), TupleHash: stream.hash,
				Entries: entries[entryStart:],
			}
			remaining = remaining[count:]
			if len(remaining) != 0 {
				segment.Next = storeio.PostingLink{
					LogicalID: storePackedIndexFirstLogicalID + uint64(pageIndex+1), Segment: 0,
				}
			}
			segments = append(segments, segment)
			used += storeio.PostingSegmentHeaderSize + encoded
			if len(remaining) != 0 {
				if err := flush(); err != nil {
					return err
				}
			}
		}
	}
	if err := flush(); err != nil {
		return err
	}
	if pageIndex != len(p.views) {
		return fmt.Errorf("vibedb: packed exact-index page-count invariant")
	}
	return nil
}

func (p *storePackedIndex) lookup(hash uint64) (storeio.PostingSegmentView, bool) {
	if p == nil || len(p.refs) == 0 {
		return storeio.PostingSegmentView{}, false
	}
	low, high := 0, len(p.refs)
	for low < high {
		middle := int(uint(low+high) >> 1)
		if p.refs[middle].hash < hash {
			low = middle + 1
		} else {
			high = middle
		}
	}
	if low == len(p.refs) || p.refs[low].hash != hash {
		runtime.KeepAlive(p)
		return storeio.PostingSegmentView{}, false
	}
	ref := p.refs[low]
	if int(ref.page) >= len(p.views) {
		return storeio.PostingSegmentView{}, false
	}
	segment, ok := p.views[ref.page].SegmentAt(int(ref.segment))
	if !ok || segment.Header().StreamID != uint32(low+1) || segment.Header().TupleHash != hash {
		return storeio.PostingSegmentView{}, false
	}
	runtime.KeepAlive(p)
	return segment, true
}

func (p *storePackedIndex) each(hash uint64, visit func(uint32, uint64) bool) bool {
	it, ok := p.iterator(hash)
	if !ok {
		return true
	}
	for {
		chunk, mask, next := it.next()
		if !next {
			return true
		}
		if !visit(chunk, mask) {
			runtime.KeepAlive(p)
			return false
		}
	}
}

func (p *storePackedIndex) iterator(hash uint64) (storePackedIndexIterator, bool) {
	segment, ok := p.lookup(hash)
	if !ok {
		return storePackedIndexIterator{}, false
	}
	return storePackedIndexIterator{
		index: p, segment: segment, entries: segment.Iterator(), hash: hash,
		streamID: segment.Header().StreamID,
	}, true
}

func (it *storePackedIndexIterator) next() (uint32, uint64, bool) {
	if it == nil || it.index == nil {
		return 0, 0, false
	}
	for {
		if entry, ok := it.entries.Next(); ok {
			return entry.Chunk, entry.Bits, true
		}
		link := it.segment.Header().Next
		if link == (storeio.PostingLink{}) ||
			link.LogicalID < storePackedIndexFirstLogicalID {
			runtime.KeepAlive(it.index)
			it.index = nil
			return 0, 0, false
		}
		page := link.LogicalID - storePackedIndexFirstLogicalID
		if page >= uint64(len(it.index.views)) {
			it.index = nil
			return 0, 0, false
		}
		segment, ok := it.index.views[page].SegmentAt(int(link.Segment))
		if !ok || segment.Header().StreamID != it.streamID || segment.Header().TupleHash != it.hash {
			it.index = nil
			return 0, 0, false
		}
		it.segment = segment
		it.entries = segment.Iterator()
	}
}
