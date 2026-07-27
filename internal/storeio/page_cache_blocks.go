package storeio

const pageCacheNoBlock = ^uint32(0)

const smallLargeBoundary = 4

type pageCacheBlockClass uint8

const (
	pageCacheBlockUnassigned pageCacheBlockClass = iota
	pageCacheBlockSmall
	pageCacheBlockLarge
	pageCacheBlockClassCount
)

// pageCacheBlockLink is the intrusive control record for one free-run head.
// It contains no Go pointers and exists once per cache allocation quantum, not
// once per database page. Non-head slots and allocated heads use span zero.
type pageCacheBlockLink struct {
	next uint32
	prev uint32
	span uint8
	_    [3]byte
}

// pageCacheBlocks is a bounded exact-span run allocator for the cache arena.
// Free runs never cross max-page-sized zone boundaries. Each zone's runs live
// in exact-length lists belonging to the zone's current size class.
type pageCacheBlocks struct {
	// heads is class-major: each class owns one intrusive free-list head for
	// every exact run length from one through zoneSlots.
	heads           []uint32
	links           []pageCacheBlockLink
	zoneClasses     []pageCacheBlockClass
	zoneSlots       uint32
	crossClassTakes uint64
}

func newPageCacheBlocks(slots, maxSpan int) pageCacheBlocks {
	blocks := pageCacheBlocks{
		heads:       make([]uint32, int(pageCacheBlockClassCount)*maxSpan),
		links:       make([]pageCacheBlockLink, slots),
		zoneClasses: make([]pageCacheBlockClass, (slots+maxSpan-1)/maxSpan),
		zoneSlots:   uint32(maxSpan),
	}
	for index := range blocks.heads {
		blocks.heads[index] = pageCacheNoBlock
	}
	for index := range blocks.links {
		blocks.links[index] = pageCacheBlockLink{
			next: pageCacheNoBlock, prev: pageCacheNoBlock,
		}
	}
	for start := 0; start < slots; start += maxSpan {
		blocks.add(uint32(start), uint8(min(maxSpan, slots-start)))
	}
	return blocks
}

// take removes the lowest-address part of the first available exact-size run.
// Class order remains matching, unassigned, then the other assigned class.
func (b *pageCacheBlocks) take(span int) (int, bool) {
	if b == nil || span <= 0 || span > int(b.zoneSlots) {
		return 0, false
	}
	class := pageCacheBlockClassForSpan(span)
	index, runSpan, ok := b.takeClass(class, uint8(span))
	if !ok {
		index, runSpan, ok = b.takeClass(pageCacheBlockUnassigned, uint8(span))
		if ok {
			b.setZoneClass(index/b.zoneSlots, class)
		}
	}
	if !ok {
		fallback := pageCacheBlockSmall
		if class == pageCacheBlockSmall {
			fallback = pageCacheBlockLarge
		}
		index, runSpan, ok = b.takeClass(fallback, uint8(span))
		if !ok {
			return 0, false
		}
		b.crossClassTakes++
	}
	if remainder := runSpan - uint8(span); remainder != 0 {
		b.add(index+uint32(span), remainder)
	}
	return int(index), true
}

// put returns one allocated span and coalesces adjacent free runs within its
// max-page-sized zone. The bounded preceding-run scan is at most zoneSlots
// iterations and avoids a second per-frame metadata array.
func (b *pageCacheBlocks) put(index, span int) {
	current := uint32(index)
	mergedSpan := uint8(span)
	zone := current / b.zoneSlots
	zoneStart := zone * b.zoneSlots
	zoneEnd := min(zoneStart+b.zoneSlots, uint32(len(b.links)))

	var previous uint32
	hasPrevious := false
	for candidate := zoneStart; candidate < current; candidate++ {
		candidateSpan := b.links[candidate].span
		if candidateSpan != 0 && candidate+uint32(candidateSpan) == current {
			previous = candidate
			hasPrevious = true
			break
		}
	}
	if hasPrevious {
		previousSpan := b.links[previous].span
		b.remove(previous, previousSpan)
		current = previous
		mergedSpan += previousSpan
	}

	next := current + uint32(mergedSpan)
	if next < zoneEnd {
		if nextSpan := b.links[next].span; nextSpan != 0 {
			b.remove(next, nextSpan)
			mergedSpan += nextSpan
		}
	}

	if current == zoneStart && current+uint32(mergedSpan) == zoneEnd {
		b.setZoneClass(zone, pageCacheBlockUnassigned)
	}
	b.add(current, mergedSpan)
}

func pageCacheBlockClassForSpan(span int) pageCacheBlockClass {
	if span < smallLargeBoundary {
		return pageCacheBlockSmall
	}
	return pageCacheBlockLarge
}

func (b *pageCacheBlocks) takeClass(
	class pageCacheBlockClass, want uint8,
) (uint32, uint8, bool) {
	span := want
	for span <= uint8(b.zoneSlots) && b.head(class, span) == pageCacheNoBlock {
		span++
	}
	if span > uint8(b.zoneSlots) {
		return 0, 0, false
	}
	index := b.head(class, span)
	b.removeClass(index, span, class)
	return index, span, true
}

func (b *pageCacheBlocks) zoneClass(index uint32) pageCacheBlockClass {
	return b.zoneClasses[index/b.zoneSlots]
}

// setZoneClass relinks every free run in one bounded zone before publishing
// its new class. The removed allocation that triggered an assignment is no
// longer on a list, and a fully coalesced reset likewise has no linked
// remainder, but scanning all zone slots keeps both transitions uniform.
func (b *pageCacheBlocks) setZoneClass(zone uint32, class pageCacheBlockClass) {
	old := b.zoneClasses[zone]
	if old == class {
		return
	}
	start := zone * b.zoneSlots
	end := min(start+b.zoneSlots, uint32(len(b.links)))
	for index := start; index < end; index++ {
		span := b.links[index].span
		if span == 0 {
			continue
		}
		b.removeClass(index, span, old)
		b.addClass(index, span, class)
	}
	b.zoneClasses[zone] = class
}

func (b *pageCacheBlocks) head(class pageCacheBlockClass, span uint8) uint32 {
	return b.heads[int(class)*int(b.zoneSlots)+int(span)-1]
}

func (b *pageCacheBlocks) setHead(
	class pageCacheBlockClass, span uint8, index uint32,
) {
	b.heads[int(class)*int(b.zoneSlots)+int(span)-1] = index
}

func (b *pageCacheBlocks) add(index uint32, span uint8) {
	b.addClass(index, span, b.zoneClass(index))
}

func (b *pageCacheBlocks) addClass(
	index uint32, span uint8, class pageCacheBlockClass,
) {
	head := b.head(class, span)
	b.links[index] = pageCacheBlockLink{
		next: head, prev: pageCacheNoBlock, span: span,
	}
	if head != pageCacheNoBlock {
		b.links[head].prev = index
	}
	b.setHead(class, span, index)
}

func (b *pageCacheBlocks) remove(index uint32, span uint8) {
	b.removeClass(index, span, b.zoneClass(index))
}

func (b *pageCacheBlocks) removeClass(
	index uint32, span uint8, class pageCacheBlockClass,
) {
	link := &b.links[index]
	if link.span != span {
		panic("storeio: page-cache free-run invariant")
	}
	if link.prev == pageCacheNoBlock {
		b.setHead(class, span, link.next)
	} else {
		b.links[link.prev].next = link.next
	}
	if link.next != pageCacheNoBlock {
		b.links[link.next].prev = link.prev
	}
	*link = pageCacheBlockLink{
		next: pageCacheNoBlock, prev: pageCacheNoBlock,
	}
}
