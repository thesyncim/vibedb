package store

// Stats is an allocation-free operational snapshot. It is O(number of
// index definitions), never O(keys or chunks). The Chunks field counts
// materialized immutable chunks; ChunkHighWater is the persistent
// vector's address span and may be larger after deletes. Sparse vector
// traversal skips absent branches, so that difference is metadata, not scan or
// compaction debt. ReusableChunks includes both partially filled and empty
// chunk ids.
type Stats struct {
	// Generation is the latest atomic publication number.
	Generation uint64
	// Keys is the current document count.
	Keys int
	// Chunks is the number of materialized immutable chunks.
	Chunks uint32
	// ChunkHighWater is the persistent vector's address span.
	ChunkHighWater uint32
	// ChunkDocuments is the configured per-chunk document bound.
	ChunkDocuments int
	// ReusableChunks counts partially filled and empty writer-side ids.
	ReusableChunks int
	// Indexes is the number of logical online index definitions.
	Indexes int
	// PhysicalIndexes is the number of distinct exact-index implementations.
	// Identical ordered-path aliases count once.
	PhysicalIndexes int
	// IndexedChunks counts chunks that physically retain postings.
	IndexedChunks int
	// ExternalKeyBytes is pointer-free mapped key-directory metadata outside
	// Go HeapAlloc on supported Unix platforms. It remains process RSS.
	ExternalKeyBytes uint64
	// ExternalDocumentBytes is pointer-free document descriptors plus any
	// collection-owned packed source/tape blocks outside Go HeapAlloc on supported
	// Unix platforms. Caller-owned mapped image bytes remain separate.
	ExternalDocumentBytes uint64
	// ExternalIndexBytes is immutable exact-index page/directory storage
	// outside Go HeapAlloc on supported Unix platforms. Mutation deltas remain
	// ordinary snapshot-owned heap nodes until folded into a later base.
	ExternalIndexBytes uint64
}

// Stats returns current writer and publication counters without traversing
// documents or allocating. It briefly takes the writer mutex so reclamation
// counters describe the same instant as the published state.
func (c *Collection) Stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	state := c.state.Load()
	if state == nil {
		chunkDocuments := c.Options.ChunkDocuments
		if chunkDocuments == 0 {
			chunkDocuments = MaxChunkDocuments
		}
		return Stats{ChunkDocuments: chunkDocuments}
	}
	stats := Stats{
		Generation:     state.Generation,
		Keys:           state.Count,
		Chunks:         state.ChunkCount,
		ChunkHighWater: state.Chunks.Count,
		ChunkDocuments: state.StateOptions.ChunkDocuments,
		ReusableChunks: len(c.free.ids),
		Indexes:        len(c.indexes),
		IndexedChunks:  len(c.postingChunks.ids),
	}
	stats.ExternalKeyBytes = state.baseKeys.externalBytes()
	stats.ExternalDocumentBytes = state.mappedDocs.externalBytes()
	visit := c.nextExactIndexVisitLocked()
	for _, index := range c.indexes {
		if index.visit == visit {
			continue
		}
		index.visit = visit
		stats.PhysicalIndexes++
		stats.ExternalIndexBytes += index.base.externalBytes()
	}
	return stats
}
