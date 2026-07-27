package storeio

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"unsafe"

	"github.com/thesyncim/vibedb/internal/storemem"
)

// RouteBucketTable uses aligned sixteen-slot (one cache line) buckets in one
// table-wide address space. The earlier eight-slot table control failed 11 of
// 12 random 65,535-row builds at exact 15/16 load even with K=32,768; this
// variant keeps the same two-choice semantics while supplying a credible
// failure bound. Pages are only physical 4-KiB COW units:
// Slots pack [row16 | tag16]. Both choices are
// calculated over every bucket in the table, so a lookup
// reads at most two pages rather than treating pages as independent shards.
// The table is immutable after Build and owns its page blocks until Close.
type RouteBucketTable struct {
	pages       []*storemem.Block
	root        [routeBucketTableRootBytes]byte
	pageRefs    []RouteBucketTablePageRef
	bucketCount uint32
	count       uint16
}

// RouteBucketTablePageRef is the exact twelve-byte durable COW locator.
// Resident Go page handles are kept in the separate pages array and accounted
// independently.
type RouteBucketTablePageRef [routeBucketTableBlockMapBytes]byte

func routeBucketTablePageRef(pageID uint64, generation uint32) RouteBucketTablePageRef {
	var ref RouteBucketTablePageRef
	binary.LittleEndian.PutUint64(ref[0:8], pageID)
	binary.LittleEndian.PutUint32(ref[8:12], generation)
	return ref
}

func (ref RouteBucketTablePageRef) valid() bool {
	return binary.LittleEndian.Uint64(ref[0:8]) != 0 &&
		binary.LittleEndian.Uint32(ref[8:12]) != 0
}

// RouteBucketTableBuilder has no retained heap scratch. Its Build method
// constructs directly in not-yet-published page blocks; bounded BFS never
// mutates before it has found an augmenting path, making failure atomic.
type RouteBucketTableBuilder struct{}

// RouteBucketTableEntry maps one complete keyed hash to its shard-local row.
// Equal hashes are legal because Lookup always delegates exact identity to the
// document-block verifier.
type RouteBucketTableEntry struct {
	Hash  uint64
	RowID uint16
}

type RouteBucketTableAccounting struct {
	PageBytes             int
	RootDirectoryBytes    int
	BlockMapBytes         int
	ResidentHandleBytes   int
	ResidentPointerBytes  int
	ResidentHeaderBytes   int
	AllocatorPaddingBytes int
	TotalBytes            int
}

const (
	RouteBucketTablePageBytes       = 4 << 10
	routeBucketTablePageMagic       = uint32(0x54524a56) // "VJRT"
	routeBucketTablePageVersion     = uint16(1)
	routeBucketTablePageHeaderBytes = 32
	routeBucketTableBucketSlots     = 16
	routeBucketTableBucketsPerPage  = (RouteBucketTablePageBytes - routeBucketTablePageHeaderBytes) / (routeBucketTableBucketSlots * 4)
	routeBucketTablePageCapacity    = routeBucketTableBucketsPerPage * routeBucketTableBucketSlots
	routeBucketTableRootBytes       = 32
	routeBucketTableBlockMapBytes   = 12
	routeBucketTableMaxRows         = 65_535
	routeBucketTableMaxBuckets      = 4_369
	routeBucketTableLoadNum         = 15
	routeBucketTableLoadDen         = 16
	// Counts bucket visits and slot reads across both initial choices.
	routeBucketTableSearchBudget = 32_768
)

const routeBucketTableBlockHandleBytes = int(unsafe.Sizeof([3]uintptr{}))
const routeBucketTableEmptyRow = ^uint16(0)

var (
	ErrRouteBucketTableIncomplete = errors.New("vibejson: route bucket table incomplete")
	ErrRouteBucketTableCorrupt    = errors.New("vibejson: corrupt route bucket table")
)

var routeBucketTableZeroChecksumWord [4]byte

// BuildRouteBucketTable builds a table-wide immutable route table. It accepts
// at most 65,535 local rows because that is the packed row-ID domain.
func BuildRouteBucketTable(entries []RouteBucketTableEntry) (*RouteBucketTable, error) {
	var builder RouteBucketTableBuilder
	return builder.Build(entries)
}

func (builder *RouteBucketTableBuilder) Build(entries []RouteBucketTableEntry) (*RouteBucketTable, error) {
	if builder == nil || len(entries) > routeBucketTableMaxRows {
		return nil, ErrInvalidWrite
	}
	var seen [1024]uint64
	for _, entry := range entries {
		if entry.RowID == routeBucketTableEmptyRow {
			return nil, ErrInvalidWrite
		}
		word, bit := entry.RowID>>6, uint(entry.RowID&63)
		if seen[word]&(uint64(1)<<bit) != 0 {
			return nil, ErrInvalidWrite
		}
		seen[word] |= uint64(1) << bit
	}
	if len(entries) == 0 {
		return &RouteBucketTable{}, nil
	}
	buckets := routeBucketTableBucketsFor(len(entries))
	if buckets == 0 || buckets > routeBucketTableMaxBuckets {
		return nil, ErrInvalidWrite
	}
	table, err := newRouteBucketTable(buckets, len(entries))
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		if placed, _, _, _ := routeBucketTableInsert(table, entry.Hash, routeBucketTableEntryWord(entry)); !placed {
			_ = table.Close()
			return nil, ErrRouteBucketTableIncomplete
		}
	}
	if err := table.seal(); err != nil {
		_ = table.Close()
		return nil, err
	}
	return table, nil
}

func routeBucketTableEntryWord(entry RouteBucketTableEntry) uint32 {
	return uint32(entry.RowID) | uint32(routeBucketTableTag(entry.Hash))<<16
}
func routeBucketTableTag(hash uint64) uint16 {
	return uint16(routeBucketMix(hash ^ 0x5bf03635d6f2f6a7))
}

func routeBucketTableBucketsFor(rows int) int {
	return (rows*routeBucketTableLoadDen + routeBucketTableBucketSlots*routeBucketTableLoadNum - 1) /
		(routeBucketTableBucketSlots * routeBucketTableLoadNum)
}

func newRouteBucketTable(buckets, rows int) (*RouteBucketTable, error) {
	pages := (buckets + routeBucketTableBucketsPerPage - 1) / routeBucketTableBucketsPerPage
	table := &RouteBucketTable{pages: make([]*storemem.Block, pages), pageRefs: make([]RouteBucketTablePageRef, pages), bucketCount: uint32(buckets), count: uint16(rows)}
	binary.LittleEndian.PutUint32(table.root[0:4], 0x524a56)
	binary.LittleEndian.PutUint16(table.root[4:6], 1)
	binary.LittleEndian.PutUint16(table.root[6:8], routeBucketTableRootBytes)
	binary.LittleEndian.PutUint32(table.root[8:12], uint32(buckets))
	binary.LittleEndian.PutUint32(table.root[12:16], uint32(rows))
	binary.LittleEndian.PutUint32(table.root[16:20], uint32(pages))
	for page := 0; page < pages; page++ {
		block, err := storemem.Allocate(RouteBucketTablePageBytes)
		if err != nil {
			_ = table.Close()
			return nil, err
		}
		table.pages[page] = block
		data := block.Bytes()
		first := page * routeBucketTableBucketsPerPage
		bucketCount := min(routeBucketTableBucketsPerPage, buckets-first)
		table.pageRefs[page] = routeBucketTablePageRef(uint64(page+1), 1)
		binary.LittleEndian.PutUint32(data[0:4], routeBucketTablePageMagic)
		binary.LittleEndian.PutUint16(data[4:6], routeBucketTablePageVersion)
		binary.LittleEndian.PutUint16(data[6:8], routeBucketTablePageHeaderBytes)
		binary.LittleEndian.PutUint32(data[8:12], uint32(first))
		binary.LittleEndian.PutUint16(data[12:14], uint16(bucketCount))
		binary.LittleEndian.PutUint32(data[16:20], uint32(buckets))
		binary.LittleEndian.PutUint16(data[20:22], uint16(page))
		binary.LittleEndian.PutUint16(data[22:24], uint16(pages))
		for slot := 0; slot < routeBucketTablePageCapacity; slot++ {
			binary.LittleEndian.PutUint32(data[routeBucketTablePageHeaderBytes+slot*4:], uint32(routeBucketTableEmptyRow))
		}
	}
	return table, nil
}

func (t *RouteBucketTable) seal() error {
	binary.LittleEndian.PutUint32(t.root[28:32], routeBucketTableRootChecksum(t.root[:]))
	if binary.LittleEndian.Uint32(t.root[0:4]) != 0x524a56 || binary.LittleEndian.Uint16(t.root[4:6]) != 1 || binary.LittleEndian.Uint16(t.root[6:8]) != routeBucketTableRootBytes || binary.LittleEndian.Uint32(t.root[8:12]) != t.bucketCount || binary.LittleEndian.Uint32(t.root[12:16]) != uint32(t.count) || binary.LittleEndian.Uint32(t.root[16:20]) != uint32(len(t.pages)) || binary.LittleEndian.Uint32(t.root[28:32]) != routeBucketTableRootChecksum(t.root[:]) || len(t.pageRefs) != len(t.pages) {
		return routeBucketTableCorrupt("root")
	}
	live := 0
	for page, block := range t.pages {
		data := block.Bytes()
		pageLive := 0
		for slot := 0; slot < routeBucketTablePageCapacity; slot++ {
			if uint16(binary.LittleEndian.Uint32(data[routeBucketTablePageHeaderBytes+slot*4:])) != routeBucketTableEmptyRow {
				pageLive++
			}
		}
		binary.LittleEndian.PutUint16(data[14:16], uint16(pageLive))
		binary.LittleEndian.PutUint32(data[24:28], routeBucketTablePageChecksum(data))
		if !t.pageRefs[page].valid() || !routeBucketTablePageValid(data, page, int(t.bucketCount), len(t.pages)) {
			return routeBucketTableCorrupt("page")
		}
		live += pageLive
	}
	if live != int(t.count) {
		return routeBucketTableCorrupt("count")
	}
	return nil
}

func routeBucketTableRootChecksum(root []byte) uint32 {
	checksum := crc32.Update(0, pageChecksumTable, root[:28])
	return crc32.Update(checksum, pageChecksumTable, routeBucketTableZeroChecksumWord[:])
}

func routeBucketTablePageChecksum(data []byte) uint32 {
	checksum := crc32.Update(0, pageChecksumTable, data[:24])
	checksum = crc32.Update(checksum, pageChecksumTable, routeBucketTableZeroChecksumWord[:])
	return crc32.Update(checksum, pageChecksumTable, data[28:])
}

func routeBucketTablePageValid(data []byte, page, buckets, pages int) bool {
	if len(data) != RouteBucketTablePageBytes ||
		binary.LittleEndian.Uint32(data[0:4]) != routeBucketTablePageMagic ||
		binary.LittleEndian.Uint16(data[4:6]) != routeBucketTablePageVersion ||
		binary.LittleEndian.Uint16(data[6:8]) != routeBucketTablePageHeaderBytes ||
		binary.LittleEndian.Uint32(data[8:12]) != uint32(page*routeBucketTableBucketsPerPage) ||
		binary.LittleEndian.Uint16(data[12:14]) != uint16(min(routeBucketTableBucketsPerPage, buckets-page*routeBucketTableBucketsPerPage)) ||
		binary.LittleEndian.Uint32(data[16:20]) != uint32(buckets) ||
		binary.LittleEndian.Uint16(data[20:22]) != uint16(page) ||
		binary.LittleEndian.Uint16(data[22:24]) != uint16(pages) ||
		!allZero(data[28:32]) || binary.LittleEndian.Uint32(data[24:28]) != routeBucketTablePageChecksum(data) {
		return false
	}
	live := 0
	activeSlots := int(binary.LittleEndian.Uint16(data[12:14])) * routeBucketTableBucketSlots
	for slot := 0; slot < routeBucketTablePageCapacity; slot++ {
		word := binary.LittleEndian.Uint32(data[routeBucketTablePageHeaderBytes+slot*4:])
		if slot >= activeSlots {
			if word != uint32(routeBucketTableEmptyRow) {
				return false
			}
			continue
		}
		if uint16(word) == routeBucketTableEmptyRow {
			if word != uint32(routeBucketTableEmptyRow) {
				return false
			}
			continue
		}
		live++
	}
	return live == int(binary.LittleEndian.Uint16(data[14:16])) &&
		allZero(data[routeBucketTablePageHeaderBytes+routeBucketTablePageCapacity*4:])
}

func (t *RouteBucketTable) word(bucket, slot int) uint32 {
	data := t.pages[bucket/routeBucketTableBucketsPerPage].Bytes()
	local := bucket % routeBucketTableBucketsPerPage
	return binary.LittleEndian.Uint32(data[routeBucketTablePageHeaderBytes+(local*routeBucketTableBucketSlots+slot)*4:])
}
func (t *RouteBucketTable) put(bucket, slot int, word uint32) {
	data := t.pages[bucket/routeBucketTableBucketsPerPage].Bytes()
	local := bucket % routeBucketTableBucketsPerPage
	binary.LittleEndian.PutUint32(data[routeBucketTablePageHeaderBytes+(local*routeBucketTableBucketSlots+slot)*4:], word)
}
func (t *RouteBucketTable) place(bucket int, word uint32) bool {
	for slot := 0; slot < routeBucketTableBucketSlots; slot++ {
		if uint16(t.word(bucket, slot)) == routeBucketTableEmptyRow {
			t.put(bucket, slot, word)
			return true
		}
	}
	return false
}

func routeBucketTableChoices(hash uint64, buckets int) (int, int) {
	first := int(hash % uint64(buckets))
	return first, routeBucketTableAlt(first, routeBucketTableTag(hash), buckets)
}
func routeBucketTableAlt(bucket int, tag uint16, buckets int) int {
	sum := int(routeBucketMix(uint64(tag)^0xd6e8feb86659fd93) % uint64(buckets))
	return (sum - bucket + buckets) % buckets
}

type routeBucketTableSearchNode struct {
	bucket uint16
	parent uint16
	slot   uint8
}

const routeBucketTableSearchRoot = ^uint16(0)

func routeBucketTableInsert(t *RouteBucketTable, hash uint64, word uint32) (bool, int, int, int) {
	first, second := routeBucketTableChoices(hash, int(t.bucketCount))
	if t.place(first, word) || (second != first && t.place(second, word)) {
		return true, 0, 0, 0
	}
	remaining := routeBucketTableSearchBudget
	for attempt, start := range [2]int{first, second} {
		if remaining == 0 || (attempt == 1 && second == first) {
			break
		}
		placed, path, work, pages := routeBucketTableRelocate(t, start, word, remaining)
		if placed {
			return true, path, routeBucketTableSearchBudget - remaining + work, pages
		}
		remaining -= work
	}
	return false, 0, routeBucketTableSearchBudget - remaining, 0
}

func routeBucketTableRelocate(t *RouteBucketTable, bucket int, pending uint32, budget int) (bool, int, int, int) {
	var queue [routeBucketTableMaxBuckets]routeBucketTableSearchNode
	var visited [routeBucketTableMaxBuckets/64 + 1]uint64
	var pageSeen [2]uint64 // max table has 70 pages
	queue[0] = routeBucketTableSearchNode{bucket: uint16(bucket), parent: routeBucketTableSearchRoot}
	visited[bucket>>6] |= uint64(1) << uint(bucket&63)
	head, tail, work, pages := 0, 1, 0, 0
	for head < tail {
		if work == budget {
			return false, 0, work, pages
		}
		work++
		node := queue[head]
		current := int(node.bucket)
		page := current / routeBucketTableBucketsPerPage
		if pageSeen[page>>6]&(uint64(1)<<uint(page&63)) == 0 {
			pageSeen[page>>6] |= uint64(1) << uint(page&63)
			pages++
		}
		for slot := 0; slot < routeBucketTableBucketSlots; slot++ {
			if work == budget {
				return false, 0, work, pages
			}
			work++
			victim := t.word(current, slot)
			if uint16(victim) == routeBucketTableEmptyRow {
				path := routeBucketTableCommit(t, queue[:tail], head, slot, pending)
				return true, path, work, pages
			}
			other := routeBucketTableAlt(current, uint16(victim>>16), int(t.bucketCount))
			if other == current || visited[other>>6]&(uint64(1)<<uint(other&63)) != 0 {
				continue
			}
			visited[other>>6] |= uint64(1) << uint(other&63)
			queue[tail] = routeBucketTableSearchNode{bucket: uint16(other), parent: uint16(head), slot: uint8(slot)}
			tail++
		}
		head++
	}
	return false, 0, work, pages
}

func routeBucketTableCommit(t *RouteBucketTable, queue []routeBucketTableSearchNode, leaf, emptySlot int, pending uint32) int {
	var path [routeBucketTableMaxBuckets]uint16
	length := 0
	for node := leaf; queue[node].parent != routeBucketTableSearchRoot; node = int(queue[node].parent) {
		path[length] = uint16(node)
		length++
	}
	for index := length - 1; index >= 0; index-- {
		node := queue[path[index]]
		parent := queue[node.parent]
		victim := t.word(int(parent.bucket), int(node.slot))
		t.put(int(parent.bucket), int(node.slot), pending)
		pending = victim
	}
	t.put(int(queue[leaf].bucket), emptySlot, pending)
	return length
}

// Lookup probes at most two globally selected buckets and consequently at
// most two pages. verify is mandatory and performs the authoritative identity
// check before a row is returned.
func (t *RouteBucketTable) Lookup(hash uint64, verify func(rowID uint16) bool) (rowID uint16, found bool) {
	if t == nil || t.bucketCount == 0 || verify == nil {
		return 0, false
	}
	tag := routeBucketTableTag(hash)
	first, second := routeBucketTableChoices(hash, int(t.bucketCount))
	for attempt, bucket := range [2]int{first, second} {
		if attempt == 1 && second == first {
			continue
		}
		for slot := 0; slot < routeBucketTableBucketSlots; slot++ {
			word := t.word(bucket, slot)
			if uint16(word) == routeBucketTableEmptyRow || uint16(word>>16) != tag {
				continue
			}
			row := uint16(word)
			if verify(row) {
				return row, true
			}
		}
	}
	return 0, false
}

func (t *RouteBucketTable) Len() int {
	if t == nil {
		return 0
	}
	return int(t.count)
}
func (t *RouteBucketTable) Buckets() int {
	if t == nil {
		return 0
	}
	return int(t.bucketCount)
}
func (t *RouteBucketTable) Pages() int {
	if t == nil {
		return 0
	}
	return len(t.pages)
}
func (t *RouteBucketTable) Accounting() RouteBucketTableAccounting {
	if t == nil {
		return RouteBucketTableAccounting{}
	}
	pages := len(t.pages) * RouteBucketTablePageBytes
	mapBytes := len(t.pageRefs) * routeBucketTableBlockMapBytes
	pointersRaw := cap(t.pages) * int(unsafe.Sizeof(uintptr(0)))
	refsRaw := cap(t.pageRefs) * routeBucketTableBlockMapBytes
	headerRaw := int(unsafe.Sizeof(RouteBucketTable{}))
	pointers, refs, header := routeBucketTableRound64(pointersRaw), routeBucketTableRound64(refsRaw), routeBucketTableRound64(headerRaw)
	padding := pointers - pointersRaw + refs - refsRaw + header - headerRaw
	return RouteBucketTableAccounting{PageBytes: pages, RootDirectoryBytes: routeBucketTableRootBytes, BlockMapBytes: mapBytes, ResidentHandleBytes: len(t.pages) * routeBucketTableBlockHandleBytes, ResidentPointerBytes: pointers, ResidentHeaderBytes: header, AllocatorPaddingBytes: padding, TotalBytes: pages + routeBucketTableRootBytes + refs + len(t.pages)*routeBucketTableBlockHandleBytes + pointers + header}
}
func routeBucketTableRound64(value int) int { return (value + 63) &^ 63 }
func (t *RouteBucketTable) Close() error {
	if t == nil {
		return nil
	}
	var first error
	for index, block := range t.pages {
		if block != nil {
			if err := block.Close(); err != nil && first == nil {
				first = err
			}
			t.pages[index] = nil
		}
	}
	t.pages = nil
	t.pageRefs = nil
	t.root = [routeBucketTableRootBytes]byte{}
	t.bucketCount = 0
	t.count = 0
	return first
}

func routeBucketMix(value uint64) uint64 {
	value ^= value >> 33
	value *= 0xff51afd7ed558ccd
	value ^= value >> 33
	value *= 0xc4ceb9fe1a85ec53
	return value ^ value>>33
}

func routeBucketTableCorrupt(what string) error {
	return fmt.Errorf("%w: %s", ErrRouteBucketTableCorrupt, what)
}
