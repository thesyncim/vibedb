package store

import (
	"math/bits"
	"reflect"

	"github.com/thesyncim/vibejson"
)

// IndexStats describes the physical footprint of one declared exact
// index in a Snapshot. The counts describe only the currently reachable
// immutable root; older Snapshots deliberately retain their own path-copied
// nodes until callers release them.
type IndexStats struct {
	Info IndexInfo

	// Fingerprints is the number of distinct composite hash buckets. A rare
	// full-hash collision shares a bucket and is separated by exact recheck.
	Fingerprints uint64
	// ChunkWords counts materialized stable-slot uint64 words. Empty chunk
	// ranges consume no posting storage.
	ChunkWords uint64
	// CandidateRows is the sum of set bits before collision rechecks.
	CandidateRows uint64
	// DirectoryNodes and BitmapNodes expose the persistent radix overhead.
	DirectoryNodes uint64
	BitmapNodes    uint64
	// EstimatedBytes accounts for reachable index-owned nodes, leaves,
	// compiled paths, and the published snapshot descriptor. It excludes Go
	// allocator size classes and collection documents, which the index only borrows.
	EstimatedBytes uint64
	// PackedBytes is the immutable base's physical page and hash-directory
	// footprint. ExternalBytes is the subset outside Go HeapAlloc on the
	// current platform.
	PackedBytes   uint64
	ExternalBytes uint64
}

// IndexStats returns allocation-free physical statistics for a declared exact
// index in this immutable Snapshot.
func (s Snapshot) IndexStats(name string) (IndexStats, error) {
	index, ok := s.exactIndex(name)
	if !ok {
		return IndexStats{}, ErrIndexNotFound
	}
	info, ok := s.indexInfo(name)
	if !ok {
		return IndexStats{}, ErrIndexNotFound
	}
	stats := IndexStats{Info: info}
	stats.EstimatedBytes = uint64(reflect.TypeFor[storeIndexSnapshot]().Size()) + uint64(reflect.TypeFor[ExactIndex]().Size())
	stats.EstimatedBytes += uint64(len(index.name))
	for i := 0; i < int(index.exact.N); i++ {
		stats.EstimatedBytes += uint64(len(index.exact.Specs[i]))
		stats.EstimatedBytes += uint64(len(index.exact.Paths[i].Tokens)) * uint64(reflect.TypeFor[vibejson.CompiledPointerToken]().Size())
	}
	if index.base != nil {
		stats.Fingerprints += index.base.fingerprints
		stats.ChunkWords += index.base.chunkWords
		stats.CandidateRows += index.base.candidateRows
		stats.PackedBytes = uint64(index.base.block.Len())
		stats.ExternalBytes = index.base.externalBytes()
		stats.EstimatedBytes += stats.PackedBytes
	}
	storeIndexAccumulatePostingStats(index.root, &stats)
	storeIndexAccumulateMaskStats(index.dirty, &stats)
	return stats, nil
}

func (s Snapshot) indexInfo(name string) (IndexInfo, bool) {
	if s.state == nil {
		return IndexInfo{}, false
	}
	lo, hi := 0, len(s.state.Indexes)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if s.state.Indexes[mid].Name < name {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo == len(s.state.Indexes) || s.state.Indexes[lo].Name != name {
		return IndexInfo{}, false
	}
	return s.state.Indexes[lo], true
}

// IndexStats returns statistics for the current Snapshot.
func (c *Collection) IndexStats(name string) (IndexStats, error) {
	snap10, _ := c.Snapshot()
	return snap10.IndexStats(name)
}

func storeIndexAccumulatePostingStats(node *storeIndexPostingNode, stats *IndexStats) {
	if node == nil {
		return
	}
	stats.DirectoryNodes++
	stats.EstimatedBytes += uint64(reflect.TypeFor[storeIndexPostingNode]().Size())
	for i := range node.slots {
		slot := node.slots[i]
		if slot.child != nil {
			storeIndexAccumulatePostingStats(slot.child, stats)
		}
		if slot.leaf == nil {
			continue
		}
		stats.Fingerprints++
		stats.EstimatedBytes += uint64(reflect.TypeFor[storeIndexPostingLeaf]().Size())
		stats.ChunkWords += uint64(slot.leaf.masks.n) + uint64(slot.leaf.masks.wide.words)
		slot.leaf.masks.each(func(_ uint32, mask uint64) bool {
			stats.CandidateRows += uint64(bits.OnesCount64(mask))
			return true
		})
		storeIndexAccumulateMaskStats(slot.leaf.masks.wide, stats)
	}
}

// storeIndexAccumulateMaskStats counts both radix shapes. BitmapNodes stays
// one count over leaves and branches alike — it measures the persistent radix
// overhead, which the split changed the size of, not the arity of — while
// EstimatedBytes charges each shape its own size so the figure tracks the
// halved leaf.
func storeIndexAccumulateMaskStats(v storeIndexMaskVector, stats *IndexStats) {
	storeIndexAccumulateMaskBranchStats(v.root, v.depth, stats)
}

func storeIndexAccumulateMaskLeafStats(leaf *storeIndexMaskLeaf, stats *IndexStats) {
	if leaf == nil {
		return
	}
	stats.BitmapNodes++
	stats.EstimatedBytes += uint64(reflect.TypeFor[storeIndexMaskLeaf]().Size())
}

func storeIndexAccumulateMaskBranchStats(node *storeIndexMaskBranch, level uint8, stats *IndexStats) {
	if node == nil {
		return
	}
	stats.BitmapNodes++
	stats.EstimatedBytes += uint64(reflect.TypeFor[storeIndexMaskBranch]().Size())
	if level == 1 {
		for _, leaf := range node.leaves {
			storeIndexAccumulateMaskLeafStats(leaf, stats)
		}
		return
	}
	for _, child := range node.children {
		storeIndexAccumulateMaskBranchStats(child, level-1, stats)
	}
}
