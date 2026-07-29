package store

// Online builds need exact per-chunk coverage until they become Ready. A flat
// []uint64 is ideal for dense ids but makes a sparse historical high-water mark
// retain memory after deletes. Coverage is therefore paged by 4,096 chunk ids:
// dense pages still cost one bit per id, absent address ranges cost nothing,
// and an empty page is removed immediately. Ready builds discard the structure
// entirely and use storeIndexBuild.all.

const storeCoveragePageShift = 12

type storeCoveragePage struct {
	words [1 << (storeCoveragePageShift - 6)]uint64
	bits  uint16
}

type storeCoverage struct {
	pages map[uint32]*storeCoveragePage
}

func (c storeCoverage) has(id uint32) bool {
	page := c.pages[id>>storeCoveragePageShift]
	if page == nil {
		return false
	}
	return page.words[(id>>6)&(uint32(len(page.words))-1)]&(uint64(1)<<(id&63)) != 0
}

func (c *storeCoverage) mark(id uint32) bool {
	pageID := id >> storeCoveragePageShift
	page := c.pages[pageID]
	if page == nil {
		if c.pages == nil {
			c.pages = make(map[uint32]*storeCoveragePage)
		}
		page = new(storeCoveragePage)
		c.pages[pageID] = page
	}
	word := &page.words[(id>>6)&(uint32(len(page.words))-1)]
	mask := uint64(1) << (id & 63)
	if *word&mask != 0 {
		return false
	}
	*word |= mask
	page.bits++
	return true
}

func (c *storeCoverage) unmark(id uint32) bool {
	pageID := id >> storeCoveragePageShift
	page := c.pages[pageID]
	if page == nil {
		return false
	}
	word := &page.words[(id>>6)&(uint32(len(page.words))-1)]
	mask := uint64(1) << (id & 63)
	if *word&mask == 0 {
		return false
	}
	*word &^= mask
	page.bits--
	if page.bits == 0 {
		delete(c.pages, pageID)
		if len(c.pages) == 0 {
			c.pages = nil
		}
	}
	return true
}
