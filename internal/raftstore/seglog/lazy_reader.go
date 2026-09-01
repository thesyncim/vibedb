package seglog

import (
	"encoding/binary"
	"io"
)

// sealedRunRef is the only per-segment/group entry routing state retained by
// Open. Final semantic summaries are streamed into the group's single current
// state and are deliberately absent here.
type sealedRunRef struct {
	SegmentID, GroupID, First, Last uint64
	RouteFirst, RouteLast           uint64
	DescriptorBase                  uint64
	DescriptorCount, BlockEntries   uint32
	ExtentOffset, ExtentBytes       uint64
	Inline                          routeEntry
}

type routeCacheSlot struct {
	segmentID, groupID uint64
	ordinal            uint32
	used               bool
	stamp              uint64
	entries            []routeEntry
}

// RouteCache charges both descriptors and decoded route entries at Reserve.
// It uses a bounded linear LRU because practical slot counts are deliberately
// small; no map grows with retained entries or access history.
type RouteCache struct {
	slots        []routeCacheSlot
	arena        []routeEntry
	entriesBlock int
	clock        uint64
}

func (c *RouteCache) Reserve(slots, entriesPerBlock int) error {
	if slots < 0 || entriesPerBlock <= 0 || slots > 1<<16 || entriesPerBlock > 1<<20 || slots != 0 && entriesPerBlock > int(^uint(0)>>1)/slots {
		return ErrBounds
	}
	c.slots = make([]routeCacheSlot, slots)
	c.arena = make([]routeEntry, slots*entriesPerBlock)
	c.entriesBlock = entriesPerBlock
	for i := range c.slots {
		c.slots[i].entries = c.arena[i*entriesPerBlock : i*entriesPerBlock]
	}
	return nil
}

func (c *RouteCache) get(segmentID, groupID uint64, ordinal uint32) ([]routeEntry, bool) {
	for i := range c.slots {
		slot := &c.slots[i]
		if slot.used && slot.segmentID == segmentID && slot.groupID == groupID && slot.ordinal == ordinal {
			slot.stamp = c.tick()
			return slot.entries, true
		}
	}
	return nil, false
}

func (c *RouteCache) victim(entries int) *routeCacheSlot {
	if entries > c.entriesBlock || len(c.slots) == 0 {
		return nil
	}
	victim := &c.slots[0]
	for i := range c.slots {
		if !c.slots[i].used {
			victim = &c.slots[i]
			break
		}
		if c.slots[i].stamp < victim.stamp {
			victim = &c.slots[i]
		}
	}
	victim.stamp = c.tick()
	return victim
}

func (c *RouteCache) tick() uint64 {
	if c.clock == ^uint64(0) {
		for i := range c.slots {
			c.slots[i].used = false
			c.slots[i].stamp = 0
		}
		c.clock = 0
	}
	c.clock++
	return c.clock
}

type RouteReadMetrics struct {
	MetadataCalls, MetadataBytes uint64
	CacheHits, CacheMisses       uint64
}

// LazyRouteReader performs exact metadata reads for one exact segment fd.
// It is intentionally single-lane: auth hash, scratch, cache, and metrics are
// mutable. Concurrent schedulers reserve one reader per lane instead of sharing
// a reader behind a lock. routeBuffer is caller-owned
// bounded scratch and must be at least sealedMaxBlockRouteBytes for every valid sealed
// block. Point returns a value copied from bounded cache or the run's inline
// route; it never returns scratch-backed data.
type LazyRouteReader struct {
	reader      io.ReaderAt
	segmentID   uint64
	logID       [16]byte
	auth        routeAuthWorkspace
	descriptor  [sealedRouteDescriptorBytes]byte
	routeBuffer []byte
	decode      []routeEntry
	cache       RouteCache
	metrics     RouteReadMetrics
}

func NewLazyRouteReader(reader io.ReaderAt, key [32]byte, logID [16]byte, segmentID uint64, cacheSlots int) (*LazyRouteReader, error) {
	return newLazyRouteReader(reader, key, logID, segmentID, cacheSlots, false)
}

func newLazyRouteReader(reader io.ReaderAt, key [32]byte, logID [16]byte, segmentID uint64, cacheSlots int, allowZeroKey bool) (*LazyRouteReader, error) {
	if reader == nil || !allowZeroKey && key == ([32]byte{}) || logID == ([16]byte{}) || segmentID == 0 {
		return nil, ErrBounds
	}
	r := &LazyRouteReader{reader: reader, segmentID: segmentID, logID: logID, auth: newRouteAuthWorkspace(key), routeBuffer: make([]byte, sealedMaxBlockRouteBytes), decode: make([]routeEntry, sealedDefaultBlockEntries)}
	if err := r.cache.Reserve(cacheSlots, sealedDefaultBlockEntries); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *LazyRouteReader) Metrics() RouteReadMetrics { return r.metrics }

func (r *LazyRouteReader) Point(run sealedRunRef, index uint64) (routeEntry, error) {
	routeFirst, routeLast := run.RouteFirst, run.RouteLast
	if routeFirst == 0 {
		routeFirst, routeLast = run.First, run.Last
	}
	if run.SegmentID != r.segmentID || index < run.First || index > run.Last || run.First == 0 {
		return routeEntry{}, ErrBounds
	}
	if run.DescriptorCount == 0 {
		if run.First != run.Last || !validRouteEntry(run.Inline) {
			return routeEntry{}, ErrCorrupt
		}
		return run.Inline, nil
	}
	if run.BlockEntries == 0 {
		return routeEntry{}, ErrCorrupt
	}
	if index < routeFirst || index > routeLast {
		return routeEntry{}, ErrCorrupt
	}
	ordinal64 := (index - routeFirst) / uint64(run.BlockEntries)
	if ordinal64 >= uint64(run.DescriptorCount) || ordinal64 > uint64(^uint32(0)) {
		return routeEntry{}, ErrCorrupt
	}
	ordinal := uint32(ordinal64)
	entries, ok := r.cache.get(run.SegmentID, run.GroupID, ordinal)
	if ok {
		r.metrics.CacheHits++
		position := index - (routeFirst + ordinal64*uint64(run.BlockEntries))
		if position >= uint64(len(entries)) {
			return routeEntry{}, ErrCorrupt
		}
		return entries[position], nil
	}
	r.metrics.CacheMisses++
	if run.DescriptorBase > sealedMaxSegmentBytes || uint64(ordinal)*sealedRouteDescriptorBytes > sealedMaxSegmentBytes-run.DescriptorBase {
		return routeEntry{}, ErrCorrupt
	}
	descriptorOffset := run.DescriptorBase + uint64(ordinal)*sealedRouteDescriptorBytes
	if err := r.readAt(r.descriptor[:], descriptorOffset); err != nil {
		return routeEntry{}, err
	}
	payloadBytes := binary.LittleEndian.Uint32(r.descriptor[4:8])
	if payloadBytes == 0 || payloadBytes > sealedMaxBlockRouteBytes {
		return routeEntry{}, ErrCorrupt
	}
	payloadOffset := binary.LittleEndian.Uint32(r.descriptor[0:4])
	if uint64(payloadOffset)+uint64(payloadBytes) > sealedMaxSegmentBytes {
		return routeEntry{}, ErrCorrupt
	}
	payload := r.routeBuffer[:payloadBytes]
	if err := r.readAt(payload, uint64(payloadOffset)); err != nil {
		return routeEntry{}, err
	}
	descriptor, err := r.auth.unmarshalRouteDescriptor(r.descriptor[:], payload, r.logID, run.SegmentID, run.GroupID, ordinal)
	if err != nil || descriptor.Entries > uint32(len(r.decode)) {
		return routeEntry{}, ErrCorrupt
	}
	first := routeFirst + ordinal64*uint64(run.BlockEntries)
	if first > routeLast {
		return routeEntry{}, ErrCorrupt
	}
	expected := min(uint64(run.BlockEntries), routeLast-first+1)
	descriptorExtentEnd, runExtentEnd := uint64(descriptor.ExtentOffset)+uint64(descriptor.ExtentBytes), run.ExtentOffset+run.ExtentBytes
	if uint64(descriptor.Entries) != expected || index-first >= expected || uint64(descriptor.ExtentOffset) < run.ExtentOffset || descriptorExtentEnd < uint64(descriptor.ExtentOffset) || runExtentEnd < run.ExtentOffset || descriptorExtentEnd > runExtentEnd {
		return routeEntry{}, ErrCorrupt
	}
	decoded, err := decodeRoutePayload(payload, descriptor.Entries, r.decode[:0])
	if err != nil {
		return routeEntry{}, err
	}
	for i := range decoded {
		end := decoded[i].ExtentOffset + decoded[i].ExtentBytes
		if decoded[i].ExtentOffset < uint64(descriptor.ExtentOffset) || end < decoded[i].ExtentOffset || end > descriptorExtentEnd {
			return routeEntry{}, ErrCorrupt
		}
	}
	slot := r.cache.victim(len(decoded))
	if slot == nil {
		return decoded[index-first], nil
	}
	slot.entries = slot.entries[:r.cache.entriesBlock]
	copy(slot.entries, decoded)
	slot.entries = slot.entries[:len(decoded)]
	slot.segmentID, slot.groupID, slot.ordinal, slot.used = run.SegmentID, run.GroupID, ordinal, true
	return slot.entries[index-first], nil
}

func (r *LazyRouteReader) readAt(dst []byte, offset uint64) error {
	if offset > uint64(^uint64(0)>>1) {
		return ErrCorrupt
	}
	n, err := r.reader.ReadAt(dst, int64(offset))
	r.metrics.MetadataCalls++
	r.metrics.MetadataBytes += uint64(n)
	if err != nil {
		return err
	}
	if n != len(dst) {
		return io.ErrUnexpectedEOF
	}
	return nil
}
