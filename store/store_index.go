package store

import (
	"errors"
	"math/bits"
	"slices"
	"strings"
)

// IndexKind names an online secondary-index family.
type IndexKind uint8

const (
	// IndexExact is a declared single-column or compound scalar index.
	// Create it with [Collection.CreateIndex]. Its physical directory maps one
	// order-sensitive typed fingerprint to persistent stable-slot bitmaps;
	// hashes only prune and every returned row is verified exactly.
	IndexExact IndexKind = iota + 1
)

// IndexState is an online index's publication state.
type IndexState uint8

const (
	// IndexBuilding means exact scan fallback is still required for some
	// live chunks.
	IndexBuilding IndexState = iota + 1
	// IndexReady means every live chunk has physical coverage.
	IndexReady
)

// IndexInfo is immutable index metadata published with a Snapshot.
// During Building, covered chunks already carry the index and uncovered
// chunks remain exact through scan fallback. Ready means every current chunk
// was covered; later writes dual-maintain it before publication.
type IndexInfo struct {
	// Name is the caller-assigned logical index name.
	Name string
	// Kind identifies the shared physical index family.
	Kind IndexKind
	// Columns contains the indexed RFC 6901 paths in compound-key order.
	Columns     [MaxIndexColumns]string
	ColumnCount uint8
	// State is Building until CoveredChunks equals TotalChunks.
	State IndexState
	// CoveredChunks is the count eligible for the physical accelerated path.
	CoveredChunks uint32
	// TotalChunks is the number of live chunks in this publication.
	TotalChunks uint32
}

var (
	// ErrIndexExists reports a CreateIndex name collision.
	ErrIndexExists = errors.New("vibedb: collection index already exists")
	// ErrIndexNotFound reports an unknown index name.
	ErrIndexNotFound = errors.New("vibedb: collection index not found")
)

type storeIndexBuild struct {
	info     IndexInfo
	coverage storeCoverage
	scan     storeChunkVector
	cursor   uint64
	all      bool
	exact    *ExactIndex
	root     *storeIndexPostingNode
	base     *storePackedIndex
	dirty    storeIndexMaskVector
	visit    uint32
	refs     uint32
}

func storeExactIndexEqual(a, b *ExactIndex) bool {
	if a == nil || b == nil || a.N != b.N {
		return false
	}
	for i := 0; i < int(a.N); i++ {
		if a.Specs[i] != b.Specs[i] {
			return false
		}
	}
	return true
}

func (c *Collection) equivalentExactIndexLocked(exact *ExactIndex) *storeIndexBuild {
	for _, existing := range c.indexes {
		if storeExactIndexEqual(existing.exact, exact) {
			return existing
		}
	}
	return nil
}

func storeLogicalIndexInfo(name string, b *storeIndexBuild) IndexInfo {
	info := b.info
	info.Name = name
	return info
}

func (c *Collection) nextExactIndexVisitLocked() uint32 {
	c.indexVisit++
	if c.indexVisit != 0 {
		return c.indexVisit
	}
	for _, b := range c.indexes {
		b.visit = 0
	}
	c.indexVisit = 1
	return c.indexVisit
}

// CreateIndex atomically publishes a path-specific exact scalar index. A
// single path indexes one column; multiple paths form an order-sensitive
// compound key. Existing chunks are processed by BackfillIndex while every
// later write dual-maintains the definition before publication. Until Ready,
// probes use an exact scan fallback.
func (c *Collection) CreateIndex(def IndexDefinition) (IndexInfo, error) {
	exact, err := CompileExactIndex(def)
	if err != nil {
		return IndexInfo{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	state, err := c.initLocked()
	if err != nil {
		return IndexInfo{}, err
	}
	if c.indexes == nil {
		c.indexes = make(map[string]*storeIndexBuild)
	}
	if _, exists := c.indexes[def.Name]; exists {
		return IndexInfo{}, ErrIndexExists
	}
	name := strings.Clone(def.Name)
	if shared := c.equivalentExactIndexLocked(exact); shared != nil {
		shared.refs++
		c.exactAliases++
		c.indexes[name] = shared
		next := *state
		next.Generation++
		next.Indexes = c.indexInfosLocked()
		next.secondary = c.indexSnapshotsLocked()
		c.state.Store(&next)
		return storeLogicalIndexInfo(name, shared), nil
	}
	exact.seed = state.seed
	b := &storeIndexBuild{exact: exact, refs: 1, info: IndexInfo{
		Name:        name,
		Kind:        IndexExact,
		State:       IndexBuilding,
		TotalChunks: state.ChunkCount,
	}, scan: state.Chunks}
	b.info.ColumnCount = exact.N
	copy(b.info.Columns[:], exact.Specs[:exact.N])
	b.updateState()
	c.indexes[name] = b
	next := *state
	next.Generation++
	next.Indexes = c.indexInfosLocked()
	next.secondary = c.indexSnapshotsLocked()
	c.state.Store(&next)
	return storeLogicalIndexInfo(name, b), nil
}

// BackfillIndex examines and rebuilds at most maxChunks chunks from the
// definition's immutable start snapshot, then publishes changed coverage
// atomically. maxChunks <= 0 means all remaining chunks. The resumable radix
// cursor skips deleted subtrees without scanning integer ids. Writes that
// touched a chunk after AddIndex already marked it covered and need no rebuild.
func (c *Collection) BackfillIndex(name string, maxChunks int) (IndexInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	b := c.indexes[name]
	if b == nil {
		return IndexInfo{}, ErrIndexNotFound
	}
	state := c.state.Load()
	if b.info.State == IndexReady || state == nil {
		return storeLogicalIndexInfo(name, b), nil
	}
	examined := 0
	changed := false
	bulkRoot := b.root
	var pending map[uint64][]storeIndexChunkMask
	for maxChunks <= 0 || examined < maxChunks {
		id, _, ok := b.scan.next(b.cursor)
		if !ok {
			break
		}
		b.cursor = uint64(id) + 1
		examined++
		if b.has(id) {
			continue
		}
		chunk := state.Chunks.Get(id)
		if chunk == nil {
			continue
		}
		var storage [MaxChunkDocuments]storeIndexHashMask
		for _, entry := range storeIndexCollectChunk(storage[:0], b.exact, chunk) {
			if pending == nil {
				pending = make(map[uint64][]storeIndexChunkMask)
			}
			pending[entry.hash] = append(pending[entry.hash], storeIndexChunkMask{chunk: id, mask: entry.mask})
		}
		b.mark(id)
		changed = true
	}
	if len(pending) != 0 {
		if bulkRoot == nil {
			b.root = storeIndexBuildBulk(pending)
		} else {
			b.root = storeIndexApplyBulk(bulkRoot, pending)
		}
	}
	if b.base == nil && b.info.CoveredChunks == b.info.TotalChunks {
		base, err := newStorePackedIndex(storeIndexPending(b.root))
		if err != nil {
			// Keep the complete heap root and Building state retryable. A later
			// BackfillIndex call can retry only the fold without rescanning rows.
			return storeLogicalIndexInfo(name, b), err
		}
		b.base = base
		b.root = nil
		b.dirty = storeIndexMaskVector{}
		changed = true
	}
	b.updateState()
	if changed {
		next := *state
		next.Generation++
		next.Indexes = c.indexInfosLocked()
		next.secondary = c.indexSnapshotsLocked()
		c.state.Store(&next)
	}
	return storeLogicalIndexInfo(name, b), nil
}

// DropIndex removes the logical definition from the next snapshot immediately.
// Old Snapshots keep their indexed chunks until ordinary garbage collection
// releases them. No postings reclamation follows: chunk-level postings exist
// only when Options.Postings requested them at build time (CreateIndex mints
// IndexExact definitions exclusively, so no logical index can be a postings
// consumer), and born-with postings are a standing option, not garbage a drop
// detaches.
func (c *Collection) DropIndex(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.indexes == nil || c.indexes[name] == nil {
		return ErrIndexNotFound
	}
	b := c.indexes[name]
	delete(c.indexes, name)
	if b.refs > 1 {
		b.refs--
		c.exactAliases--
	}
	state := c.state.Load()
	if state == nil {
		return nil
	}
	next := *state
	next.Generation++
	next.Indexes = c.indexInfosLocked()
	next.secondary = c.indexSnapshotsLocked()
	c.state.Store(&next)
	return nil
}

func (b *storeIndexBuild) has(id uint32) bool {
	if b.all {
		return true
	}
	return b.coverage.has(id)
}

func (b *storeIndexBuild) mark(id uint32) {
	if b.all {
		b.info.CoveredChunks = b.info.TotalChunks
		return
	}
	if b.coverage.mark(id) {
		b.info.CoveredChunks++
	}
}

func (b *storeIndexBuild) unmark(id uint32) {
	if b.all {
		b.info.CoveredChunks = b.info.TotalChunks
		return
	}
	if b.coverage.unmark(id) {
		b.info.CoveredChunks--
	}
}

// updateState collapses complete coverage to an implicit all-live-chunks
// representation. Readiness is monotonic because every later write builds the
// active physical family before publication. Discarding both the bitmap and
// CreateIndex snapshot prevents a completed logical index from pinning historical
// chunks or retaining one bit per chunk indefinitely.
func (b *storeIndexBuild) updateState() {
	if b.info.CoveredChunks == b.info.TotalChunks {
		b.info.State = IndexReady
		b.coverage = storeCoverage{}
		b.all = true
		b.scan = storeChunkVector{}
		b.cursor = 0
		return
	}
	b.info.State = IndexBuilding
}

// noteIndexesForChunkLocked updates logical coverage for a collection write. A
// newly materialized chunk joins every logical index already fully built;
// removing the last document removes that chunk from both total and covered
// counts. Rewrites dual-maintain before publication and therefore mark the
// chunk covered even while background backfill is still running elsewhere.
func (c *Collection) noteIndexesForChunkLocked(id uint32, old, next *Chunk, changed uint64) (catalogChanged, secondaryChanged bool) {
	oldLive, nextLive := old != nil, next != nil
	visit := uint32(0)
	if c.exactAliases != 0 {
		visit = c.nextExactIndexVisitLocked()
	}
	for _, b := range c.indexes {
		if visit != 0 {
			if b.visit == visit {
				continue
			}
			b.visit = visit
		}
		oldInfo, oldRoot, oldDirty := b.info, b.root, b.dirty.root
		covered := b.has(id)
		if b.base != nil && b.dirty.get(id) == 0 {
			// Shadow the whole chunk before changing one slot. Base postings for
			// every tuple in a dirty chunk are skipped by readers, so seed the
			// delta with the complete old image exactly once.
			b.dirty = b.dirty.set(id, 1)
			if oldLive {
				b.root = storeIndexSetChunk(b.root, b.exact, id, old, true)
			}
		}
		switch {
		case !oldLive && nextLive:
			b.info.TotalChunks++
			b.root = storeIndexSetChunk(b.root, b.exact, id, next, true)
			b.mark(id)
		case oldLive && !nextLive:
			if covered {
				b.root = storeIndexSetChunk(b.root, b.exact, id, old, false)
			}
			b.info.TotalChunks--
			b.unmark(id)
		case nextLive:
			if covered {
				for slots := changed; slots != 0; slots &= slots - 1 {
					slot := bits.TrailingZeros64(slots)
					b.root = storeIndexUpdateSlot(b.root, b.exact, id, old, next, slot)
				}
			} else {
				// A mutation of an uncovered chunk indexes its complete next
				// image, so the chunk can join coverage immediately.
				b.root = storeIndexSetChunk(b.root, b.exact, id, next, true)
			}
			b.mark(id)
		}
		b.updateState()
		if b.info != oldInfo {
			catalogChanged = true
		}
		if b.root != oldRoot || b.dirty.root != oldDirty || b.info != oldInfo {
			secondaryChanged = true
		}
	}
	return catalogChanged, secondaryChanged
}

func (c *Collection) indexSnapshotsLocked() []storeIndexSnapshot {
	if len(c.indexes) == 0 {
		return nil
	}
	out := make([]storeIndexSnapshot, 0, len(c.indexes))
	for name, b := range c.indexes {
		out = append(out, storeIndexSnapshot{
			name:  name,
			state: b.info.State,
			exact: b.exact, root: b.root, base: b.base, dirty: b.dirty,
		})
	}
	slices.SortFunc(out, func(a, b storeIndexSnapshot) int {
		if a.name < b.name {
			return -1
		}
		if a.name > b.name {
			return 1
		}
		return 0
	})
	return out
}

func (c *Collection) indexInfosLocked() []IndexInfo {
	if len(c.indexes) == 0 {
		return nil
	}
	out := make([]IndexInfo, 0, len(c.indexes))
	for name, b := range c.indexes {
		out = append(out, storeLogicalIndexInfo(name, b))
	}
	slices.SortFunc(out, func(a, b IndexInfo) int {
		if a.Name < b.Name {
			return -1
		}
		if a.Name > b.Name {
			return 1
		}
		return 0
	})
	return out
}

// AppendIndexes appends immutable index metadata to dst.
func (s Snapshot) AppendIndexes(dst []IndexInfo) []IndexInfo {
	if s.state == nil {
		return dst
	}
	return append(dst, s.state.Indexes...)
}
