package storeio

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"unsafe"

	"github.com/thesyncim/vibedb/internal/storemem"
)

var ErrBlockSelectorCorrupt = errors.New("vibejson: corrupt Store block selector page")

// BlockSelectorBoundary describes one already lexically ordered immutable
// document block. First and Last are build-only borrowed complete keys; no
// complete boundary key is retained in the selector image. BlockID is the
// compact logical document-block identity used to acquire raw or packed bytes.
type BlockSelectorBoundary struct {
	First   []byte
	Last    []byte
	BlockID uint32
}

// BlockSelector is a multi-level immutable fence tree. Leaves select document
// blocks; upper pages select child pages. The selector never stores document
// keys, only shortest right-boundary separators and four-byte locators.
type BlockSelector struct {
	levels [][]*BlockSelectorPage // leaves first, exactly one root last
	// rootRef is the durable twelve-byte PageRef spelling: page ordinal plus
	// generation/reserved fields. Resident pointers are separate above.
	rootRef [12]byte
	blocks  int
}

// BlockSelectorPage is a checksummed exact 4-KiB immutable selector page.
// It owns a block only when built locally; OpenBlockSelectorPage borrows bytes.
type BlockSelectorPage struct {
	block     *storemem.Block
	data      []byte
	locators  []byte
	restarts  []byte
	prefix    []byte
	suffix    []byte
	entries   []byte
	base      uint32
	count     uint16
	keyLimit  uint16
	compact   bool
	entryHead int
}

// BlockSelectorCursor provides a bounded compact separator scratch. Longer
// separators require caller-provided scratch and fail closed without allocating.
type BlockSelectorCursor struct {
	compact  [255]byte
	extended []byte
	large    bool
}

func (c *BlockSelectorCursor) SetExtendedScratch(scratch []byte) {
	if c != nil {
		c.extended = scratch
	}
}
func (c *BlockSelectorCursor) prepare(limit int) []byte {
	if limit <= len(c.compact) {
		c.large = false
		return c.compact[:limit]
	}
	if len(c.extended) < limit {
		return nil
	}
	c.large = true
	return c.extended[:limit]
}

// BlockSelectorSpace charges every fixed selector/root page, including header,
// locators, restart offsets, padding, and all root levels. KeysPerBlock is the
// fixed stable-slot geometry assumed by the document blocks.
type BlockSelectorSpace struct {
	PhysicalBytes           int
	DurableRootRefBytes     int
	ResidentPageHandleBytes int
	ResidentPointerBytes    int
	ResidentHeaderBytes     int
	AllocatorPaddingBytes   int
	PageCount               int
	RootLevels              int
	Blocks                  int
	KeysPerBlock            int
}

func (s BlockSelectorSpace) BytesPerKey() float64 {
	if s.Blocks == 0 || s.KeysPerBlock == 0 {
		return 0
	}
	return float64(s.PhysicalBytes+s.DurableRootRefBytes+s.ResidentPageHandleBytes+s.ResidentPointerBytes+s.ResidentHeaderBytes) / float64(s.Blocks*s.KeysPerBlock)
}

// BlockSelectorHit is the exact lower-bound result after one candidate-block
// probe and, only for a shortest-separator gap, one adjacent-block fallback.
type BlockSelectorHit struct {
	BlockID  uint32
	Slot     uint8
	Fallback bool
}

const (
	BlockSelectorPageBytes    = 4 << 10
	blockSelectorHeaderBytes  = 32
	blockSelectorRestart      = 8
	blockSelectorCompactHead  = 2
	blockSelectorExtendedHead = 4
	blockSelectorHardKey      = BlockSelectorPageBytes - blockSelectorHeaderBytes - 4 - 2 - blockSelectorExtendedHead
	blockSelectorMagic        = uint32(0x534a56) // "VJS" little-endian
	blockSelectorVersion      = uint16(1)
)

// BuildBlockSelector builds leaves and the minimal immutable upper-page tree.
// It rejects non-contiguous lexical block ranges and never retains the supplied
// complete keys after construction.
func BuildBlockSelector(blocks []BlockSelectorBoundary) (*BlockSelector, error) {
	if len(blocks) == 0 {
		return nil, ErrInvalidWrite
	}
	if err := blockSelectorValidateBoundaries(blocks, false); err != nil {
		return nil, err
	}
	pages, next, err := blockSelectorBuildLevel(blocks, false)
	if err != nil {
		return nil, err
	}
	out := &BlockSelector{levels: [][]*BlockSelectorPage{pages}, blocks: len(blocks)}
	for len(pages) > 1 {
		pages, next, err = blockSelectorBuildLevel(next, true)
		if err != nil {
			out.Close()
			return nil, err
		}
		out.levels = append(out.levels, pages)
	}
	binary.LittleEndian.PutUint32(out.rootRef[0:4], uint32(len(pages)-1))
	binary.LittleEndian.PutUint32(out.rootRef[4:8], 1)
	return out, nil
}

// Select returns the candidate block and its only possible adjacent fallback.
// It allocates nothing. If long separator scratch was not supplied, it returns
// false rather than allocating a hidden large per-reader buffer.
func (s *BlockSelector) Select(target []byte, cursor *BlockSelectorCursor) (candidate, adjacent uint32, ok bool) {
	if s == nil || cursor == nil || len(s.levels) == 0 {
		return 0, 0, false
	}
	index := 0
	for level := len(s.levels) - 1; level > 0; level-- {
		page := s.levels[level][index]
		rank, ok := page.selectRank(target, cursor)
		if !ok {
			return 0, 0, false
		}
		index = int(page.locator(rank))
		if index < 0 || index >= len(s.levels[level-1]) {
			return 0, 0, false
		}
	}
	page := s.levels[0][index]
	rank, ok := page.selectRank(target, cursor)
	if !ok {
		return 0, 0, false
	}
	candidate = page.locator(rank)
	if rank < int(page.count) {
		adjacent = page.locator(rank + 1)
	} else if index+1 < len(s.levels[0]) {
		adjacent = s.levels[0][index+1].base
	}
	return candidate, adjacent, candidate != 0
}

// Seek adds exact document-block lower-bound verification to Select. lower may
// be RawBlockView.LowerBound, PackedBlockView.LowerBound, or a future cache
// adapter. It probes no more than the candidate and one adjacent gap block.
func (s *BlockSelector) Seek(target []byte, cursor *BlockSelectorCursor, lower func(uint32, []byte) (uint8, bool)) (BlockSelectorHit, bool) {
	if lower == nil {
		return BlockSelectorHit{}, false
	}
	candidate, adjacent, ok := s.Select(target, cursor)
	if !ok {
		return BlockSelectorHit{}, false
	}
	if slot, found := lower(candidate, target); found {
		return BlockSelectorHit{BlockID: candidate, Slot: slot}, true
	}
	if adjacent != 0 {
		if slot, found := lower(adjacent, target); found {
			return BlockSelectorHit{BlockID: adjacent, Slot: slot, Fallback: true}, true
		}
	}
	return BlockSelectorHit{}, false
}

// BlockSelectorRange walks adjacent block identities without allocating. A
// range consumer then uses the raw or packed lexical iterator in each block.
type BlockSelectorRange struct {
	selector   *BlockSelector
	leaf, rank int
	block      uint32
	valid      bool
}

func (s *BlockSelector) Range(target []byte, cursor *BlockSelectorCursor) BlockSelectorRange {
	if s == nil || cursor == nil || len(s.levels) == 0 {
		return BlockSelectorRange{}
	}
	index := 0
	for level := len(s.levels) - 1; level > 0; level-- {
		rank, ok := s.levels[level][index].selectRank(target, cursor)
		if !ok {
			return BlockSelectorRange{}
		}
		index = int(s.levels[level][index].locator(rank))
	}
	rank, ok := s.levels[0][index].selectRank(target, cursor)
	if !ok {
		return BlockSelectorRange{}
	}
	return BlockSelectorRange{selector: s, leaf: index, rank: rank, block: s.levels[0][index].locator(rank), valid: true}
}
func (r *BlockSelectorRange) BlockID() (uint32, bool) {
	if r == nil || !r.valid {
		return 0, false
	}
	return r.block, true
}
func (r *BlockSelectorRange) NextBlock() bool {
	if r == nil || !r.valid {
		return false
	}
	page := r.selector.levels[0][r.leaf]
	if r.rank < int(page.count) {
		r.rank++
		r.block = page.locator(r.rank)
		return true
	}
	if r.leaf+1 >= len(r.selector.levels[0]) {
		r.valid = false
		return false
	}
	r.leaf++
	r.rank = 0
	r.block = r.selector.levels[0][r.leaf].base
	return true
}

func (s *BlockSelector) Space(keysPerBlock int) BlockSelectorSpace {
	if s == nil || keysPerBlock <= 0 {
		return BlockSelectorSpace{}
	}
	pages := 0
	for _, level := range s.levels {
		pages += len(level)
	}
	pointersRaw := pages*int(unsafe.Sizeof(uintptr(0))) + len(s.levels)*int(unsafe.Sizeof([]*BlockSelectorPage{}))
	headerRaw := int(unsafe.Sizeof(BlockSelector{}))
	pointers, header := blockSelectorRound64(pointersRaw), blockSelectorRound64(headerRaw)
	return BlockSelectorSpace{PhysicalBytes: pages * BlockSelectorPageBytes, DurableRootRefBytes: 12, ResidentPageHandleBytes: pages * int(unsafe.Sizeof([3]uintptr{})), ResidentPointerBytes: pointers, ResidentHeaderBytes: header, AllocatorPaddingBytes: pointers - pointersRaw + header - headerRaw, PageCount: pages, RootLevels: len(s.levels) - 1, Blocks: s.blocks, KeysPerBlock: keysPerBlock}
}
func blockSelectorRound64(value int) int { return (value + 63) &^ 63 }
func (s *BlockSelector) Close() error {
	if s == nil {
		return nil
	}
	var first error
	for _, level := range s.levels {
		for _, page := range level {
			if err := page.Close(); err != nil && first == nil {
				first = err
			}
		}
	}
	s.levels = nil
	s.blocks = 0
	return first
}

func blockSelectorBuildLevel(input []BlockSelectorBoundary, allowZero bool) ([]*BlockSelectorPage, []BlockSelectorBoundary, error) {
	pages := make([]*BlockSelectorPage, 0, (len(input)+255)/256)
	next := make([]BlockSelectorBoundary, 0, cap(pages))
	for start := 0; start < len(input); {
		end := start + 1
		var best *BlockSelectorPage
		bestEnd := start
		for end <= len(input) {
			page, err := buildBlockSelectorPage(input[start:end], allowZero)
			if err != nil {
				break
			}
			if best != nil {
				_ = best.Close()
			}
			best, bestEnd = page, end
			end++
		}
		if best == nil {
			for _, page := range pages {
				_ = page.Close()
			}
			return nil, nil, ErrInvalidWrite
		}
		pages = append(pages, best)
		next = append(next, BlockSelectorBoundary{First: input[start].First, Last: input[bestEnd-1].Last, BlockID: uint32(len(pages) - 1)})
		start = bestEnd
	}
	return pages, next, nil
}

// BuildBlockSelectorPage creates one page over consecutive blocks. For leaf
// pages BlockID is a document logical block identity; for upper pages it is a
// child-page ordinal. Both are direct four-byte locators in the same codec.
func BuildBlockSelectorPage(blocks []BlockSelectorBoundary) (*BlockSelectorPage, error) {
	return buildBlockSelectorPage(blocks, false)
}

func buildBlockSelectorPage(blocks []BlockSelectorBoundary, allowZero bool) (*BlockSelectorPage, error) {
	if len(blocks) == 0 || blockSelectorValidateBoundaries(blocks, allowZero) != nil {
		return nil, ErrInvalidWrite
	}
	count := len(blocks) - 1
	separators := make([][]byte, count)
	maxKey := 0
	for i := range separators {
		separators[i] = blockSelectorSeparator(blocks[i].Last, blocks[i+1].First)
		if len(separators[i]) == 0 {
			return nil, ErrInvalidWrite
		}
		maxKey = max(maxKey, len(separators[i]))
	}
	compact := maxKey <= 255
	head := blockSelectorHead(compact)
	prefix, suffix := blockSelectorEdges(separators)
	restarts := (count + blockSelectorRestart - 1) / blockSelectorRestart
	entryBytes := 0
	previous := []byte(nil)
	for i, separator := range separators {
		if i%blockSelectorRestart == 0 {
			previous = nil
		}
		middle := separator[prefix : len(separator)-suffix]
		shared := blockSelectorPrefix(previous, middle)
		tail := len(middle) - shared
		if compact && (shared > 255 || tail > 255) {
			return nil, ErrInvalidWrite
		}
		entryBytes += head + tail
		previous = middle
	}
	locatorBytes := count * 4
	restartBytes := restarts * 2
	if blockSelectorHeaderBytes+locatorBytes+restartBytes+prefix+suffix+entryBytes > BlockSelectorPageBytes || entryBytes > int(^uint16(0)) {
		return nil, ErrInvalidWrite
	}
	block, err := storemem.Allocate(BlockSelectorPageBytes)
	if err != nil {
		return nil, err
	}
	data := block.Bytes()
	binary.LittleEndian.PutUint32(data[0:4], blockSelectorMagic)
	binary.LittleEndian.PutUint16(data[4:6], blockSelectorVersion)
	binary.LittleEndian.PutUint16(data[6:8], blockSelectorHeaderBytes)
	binary.LittleEndian.PutUint16(data[8:10], uint16(count))
	binary.LittleEndian.PutUint16(data[10:12], uint16(entryBytes))
	binary.LittleEndian.PutUint16(data[12:14], uint16(prefix))
	binary.LittleEndian.PutUint16(data[14:16], uint16(suffix))
	binary.LittleEndian.PutUint32(data[16:20], blocks[0].BlockID)
	binary.LittleEndian.PutUint16(data[20:22], uint16(maxKey))
	if compact {
		data[22] = 1
	}
	locatorAt := blockSelectorHeaderBytes
	restartAt := locatorAt + locatorBytes
	prefixAt := restartAt + restartBytes
	suffixAt := prefixAt + prefix
	entryAt := suffixAt + suffix
	if count > 0 {
		copy(data[prefixAt:suffixAt], separators[0][:prefix])
		copy(data[suffixAt:entryAt], separators[0][len(separators[0])-suffix:])
	}
	previous = nil
	encodedAt := 0
	for i, separator := range separators {
		binary.LittleEndian.PutUint32(data[locatorAt+i*4:], blocks[i+1].BlockID)
		if i%blockSelectorRestart == 0 {
			binary.LittleEndian.PutUint16(data[restartAt+i/blockSelectorRestart*2:], uint16(encodedAt))
			previous = nil
		}
		middle := separator[prefix : len(separator)-suffix]
		shared := blockSelectorPrefix(previous, middle)
		tail := middle[shared:]
		at := entryAt + encodedAt
		if compact {
			data[at], data[at+1] = byte(shared), byte(len(tail))
		} else {
			binary.LittleEndian.PutUint16(data[at:at+2], uint16(shared))
			binary.LittleEndian.PutUint16(data[at+2:at+4], uint16(len(tail)))
		}
		copy(data[at+head:], tail)
		encodedAt += head + len(tail)
		previous = middle
	}
	binary.LittleEndian.PutUint32(data[24:28], blockSelectorChecksum(data))
	page, err := openBlockSelectorPage(data)
	if err != nil {
		_ = block.Close()
		return nil, err
	}
	page.block = block
	return page, nil
}

func OpenBlockSelectorPage(src []byte) (*BlockSelectorPage, error) { return openBlockSelectorPage(src) }
func openBlockSelectorPage(src []byte) (*BlockSelectorPage, error) {
	if len(src) != BlockSelectorPageBytes || binary.LittleEndian.Uint32(src[0:4]) != blockSelectorMagic || binary.LittleEndian.Uint16(src[4:6]) != blockSelectorVersion || binary.LittleEndian.Uint16(src[6:8]) != blockSelectorHeaderBytes || !allZero(src[23:24]) || !allZero(src[28:32]) || binary.LittleEndian.Uint32(src[24:28]) != blockSelectorChecksum(src) {
		return nil, ErrBlockSelectorCorrupt
	}
	count := int(binary.LittleEndian.Uint16(src[8:10]))
	entryBytes := int(binary.LittleEndian.Uint16(src[10:12]))
	prefixLen := int(binary.LittleEndian.Uint16(src[12:14]))
	suffixLen := int(binary.LittleEndian.Uint16(src[14:16]))
	base := binary.LittleEndian.Uint32(src[16:20])
	keyLimit := int(binary.LittleEndian.Uint16(src[20:22]))
	compact := src[22] == 1
	if keyLimit < 0 || keyLimit > blockSelectorHardKey || prefixLen+suffixLen > keyLimit {
		return nil, ErrBlockSelectorCorrupt
	}
	restarts := (count + blockSelectorRestart - 1) / blockSelectorRestart
	locatorAt := blockSelectorHeaderBytes
	restartAt := locatorAt + count*4
	prefixAt := restartAt + restarts*2
	suffixAt := prefixAt + prefixLen
	entryAt := suffixAt + suffixLen
	if entryAt > BlockSelectorPageBytes || entryBytes > BlockSelectorPageBytes-entryAt || !allZero(src[entryAt+entryBytes:]) {
		return nil, ErrBlockSelectorCorrupt
	}
	page := &BlockSelectorPage{data: src, locators: src[locatorAt:restartAt], restarts: src[restartAt:prefixAt], prefix: src[prefixAt:suffixAt], suffix: src[suffixAt:entryAt], entries: src[entryAt : entryAt+entryBytes], base: base, count: uint16(count), keyLimit: uint16(keyLimit), compact: compact, entryHead: blockSelectorHead(compact)}
	if !page.canonical() {
		return nil, ErrBlockSelectorCorrupt
	}
	return page, nil
}

func (p *BlockSelectorPage) selectRank(target []byte, c *BlockSelectorCursor) (int, bool) {
	if p == nil || c == nil {
		return 0, false
	}
	key := c.prepare(int(p.keyLimit))
	if key == nil {
		return 0, false
	}
	lo, hi := 0, p.restartCount()
	for lo < hi {
		mid := lo + (hi-lo)/2
		if p.compareRestart(mid, target) > 0 {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	group := lo - 1
	if group < 0 {
		group = 0
	}
	if rank, ok := p.selectGroup(target, group, key); ok {
		return rank, true
	}
	if lo < p.restartCount() {
		return p.selectGroup(target, lo, key)
	}
	return int(p.count), true
}
func (p *BlockSelectorPage) selectGroup(target []byte, group int, key []byte) (int, bool) {
	if group < 0 || group >= p.restartCount() {
		return 0, false
	}
	rank := group * blockSelectorRestart
	at := p.restartOffset(group)
	previous := 0
	limit := min(rank+blockSelectorRestart, int(p.count))
	for rank < limit {
		next, length, ok := p.decode(at, previous, key)
		if !ok {
			return 0, false
		}
		if bytes.Compare(key[:length], target) >= 0 {
			return rank, true
		}
		at, previous, rank = next, length-len(p.prefix)-len(p.suffix), rank+1
	}
	return 0, false
}
func (p *BlockSelectorPage) locator(rank int) uint32 {
	if rank == 0 {
		return p.base
	}
	return binary.LittleEndian.Uint32(p.locators[(rank-1)*4:])
}
func (p *BlockSelectorPage) restartCount() int { return len(p.restarts) / 2 }
func (p *BlockSelectorPage) restartOffset(i int) int {
	return int(binary.LittleEndian.Uint16(p.restarts[i*2:]))
}
func (p *BlockSelectorPage) compareRestart(i int, target []byte) int {
	at := p.restartOffset(i)
	tail := p.tail(at)
	return blockSelectorCompare(p.prefix, p.entries[at+p.entryHead:at+p.entryHead+tail], p.suffix, target)
}
func (p *BlockSelectorPage) tail(at int) int {
	if p.compact {
		return int(p.entries[at+1])
	}
	return int(binary.LittleEndian.Uint16(p.entries[at+2 : at+4]))
}
func (p *BlockSelectorPage) decode(at, previous int, key []byte) (next, length int, ok bool) {
	if at < 0 || at+p.entryHead > len(p.entries) || previous < 0 {
		return 0, 0, false
	}
	shared, tail := 0, 0
	if p.compact {
		shared, tail = int(p.entries[at]), int(p.entries[at+1])
	} else {
		shared, tail = int(binary.LittleEndian.Uint16(p.entries[at:at+2])), int(binary.LittleEndian.Uint16(p.entries[at+2:at+4]))
	}
	middle := shared + tail
	length = len(p.prefix) + middle + len(p.suffix)
	next = at + p.entryHead + tail
	if shared > previous || length > int(p.keyLimit) || next > len(p.entries) || len(key) < int(p.keyLimit) {
		return 0, 0, false
	}
	copy(key[:len(p.prefix)], p.prefix)
	copy(key[len(p.prefix)+shared:len(p.prefix)+middle], p.entries[at+p.entryHead:next])
	copy(key[len(p.prefix)+middle:length], p.suffix)
	return next, length, true
}
func (p *BlockSelectorPage) canonical() bool {
	if p.count == 0 {
		return len(p.entries) == 0 && len(p.prefix) == 0 && len(p.suffix) == 0
	}
	var key, last, first [blockSelectorHardKey]byte
	at, previous, lastLen, firstLen := 0, 0, 0, 0
	for rank := 0; rank < int(p.count); rank++ {
		if rank%blockSelectorRestart == 0 {
			if p.restartOffset(rank/blockSelectorRestart) != at {
				return false
			}
			previous = 0
		}
		shared := 0
		if p.compact {
			shared = int(p.entries[at])
		} else {
			shared = int(binary.LittleEndian.Uint16(p.entries[at : at+2]))
		}
		if shared > previous || rank%blockSelectorRestart == 0 && shared != 0 {
			return false
		}
		if shared < previous && p.tail(at) > 0 && key[len(p.prefix)+shared] == p.entries[at+p.entryHead] {
			return false
		}
		next, length, ok := p.decode(at, previous, key[:])
		if !ok || rank > 0 && bytes.Compare(last[:lastLen], key[:length]) >= 0 {
			return false
		}
		if rank == 0 {
			copy(first[:length], key[:length])
			firstLen = length
		}
		copy(last[:length], key[:length])
		at, previous, lastLen = next, length-len(p.prefix)-len(p.suffix), length
	}
	if at != len(p.entries) {
		return false
	}
	prefix := firstLen
	at, previous = 0, 0
	for rank := 0; rank < int(p.count); rank++ {
		if rank%blockSelectorRestart == 0 {
			previous = 0
		}
		next, length, ok := p.decode(at, previous, key[:])
		if !ok {
			return false
		}
		prefix = blockSelectorPrefix(first[:prefix], key[:length])
		at, previous = next, length-len(p.prefix)-len(p.suffix)
	}
	suffix := firstLen - prefix
	at, previous = 0, 0
	for rank := 0; rank < int(p.count); rank++ {
		if rank%blockSelectorRestart == 0 {
			previous = 0
		}
		next, length, ok := p.decode(at, previous, key[:])
		if !ok {
			return false
		}
		suffix = min(suffix, blockSelectorSuffix(first[prefix:firstLen], key[prefix:length]))
		at, previous = next, length-len(p.prefix)-len(p.suffix)
	}
	return len(p.prefix) == prefix && len(p.suffix) == suffix && bytes.Equal(p.prefix, first[:prefix]) && bytes.Equal(p.suffix, first[firstLen-suffix:firstLen])
}
func (p *BlockSelectorPage) Close() error {
	if p == nil || p.block == nil {
		return nil
	}
	b := p.block
	p.block, p.data, p.locators, p.restarts, p.prefix, p.suffix, p.entries = nil, nil, nil, nil, nil, nil, nil
	return b.Close()
}

func blockSelectorValidateBoundaries(blocks []BlockSelectorBoundary, allowZero bool) error {
	for i, b := range blocks {
		if (!allowZero && b.BlockID == 0) || bytes.Compare(b.First, b.Last) > 0 || i > 0 && bytes.Compare(blocks[i-1].Last, b.First) >= 0 {
			return ErrInvalidWrite
		}
	}
	return nil
}
func blockSelectorSeparator(left, right []byte) []byte {
	if bytes.Compare(left, right) >= 0 {
		return nil
	}
	n := blockSelectorPrefix(left, right)
	return right[:n+1]
}
func blockSelectorEdges(keys [][]byte) (int, int) {
	if len(keys) == 0 {
		return 0, 0
	}
	first := keys[0]
	p := len(first)
	for _, key := range keys[1:] {
		p = blockSelectorPrefix(first[:p], key)
	}
	s := len(first) - p
	for _, key := range keys {
		s = min(s, blockSelectorSuffix(first[p:], key[p:]))
	}
	return p, s
}
func blockSelectorPrefix(a, b []byte) int {
	n := min(len(a), len(b))
	i := 0
	for i < n && a[i] == b[i] {
		i++
	}
	return i
}
func blockSelectorSuffix(a, b []byte) int {
	n := min(len(a), len(b))
	i := 0
	for i < n && a[len(a)-1-i] == b[len(b)-1-i] {
		i++
	}
	return i
}
func blockSelectorHead(compact bool) int {
	if compact {
		return blockSelectorCompactHead
	}
	return blockSelectorExtendedHead
}
func blockSelectorCompare(prefix, middle, suffix, target []byte) int {
	at := 0
	for _, part := range [3][]byte{prefix, middle, suffix} {
		left := len(target) - at
		if left <= 0 {
			if len(part) > 0 {
				return 1
			}
			continue
		}
		n := min(left, len(part))
		if c := bytes.Compare(part[:n], target[at:at+n]); c != 0 {
			return c
		}
		at += n
		if n < len(part) {
			return 1
		}
	}
	if at < len(target) {
		return -1
	}
	return 0
}
func blockSelectorChecksum(data []byte) uint32 {
	sum := crc32.Update(0, pageChecksumTable, data[:24])
	var zero [4]byte
	sum = crc32.Update(sum, pageChecksumTable, zero[:])
	return crc32.Update(sum, pageChecksumTable, data[28:])
}
