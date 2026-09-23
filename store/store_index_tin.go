package store

import (
	"fmt"
	"math/bits"
	"strings"
	"sync"

	"github.com/thesyncim/vibedb/internal/tin"
	"github.com/thesyncim/vibejson"
)

// Full-text (tin) indexes on in-memory collections.
//
// A tin index is a text search structure over one JSON path, maintained as
// a state-pinned sidecar rather than inside the exact posting machinery:
// each snapshot State gets its tin indexes built once, on first use, by
// scanning that State's own chunks. Pinning the build to the State makes
// reads unconditionally sound — the index and the documents it describes
// can never skew — at the price of a rebuild scan when the collection
// changes between queries. Writes need no hooks at all: a write publishes a
// new State, and the next query builds for it.
//
// DocIDs pack the stable slot address the engine already guarantees:
// chunk in the high 32 bits, slot below. Slots stay stable within a State,
// so a build for State S answers queries over S exactly.

// TinDocID maps a stable (chunk, slot) address to a tin document identity.
func TinDocID(chunk uint32, slot int) tin.DocID {
	return tin.DocID(uint64(chunk)<<32 | uint64(slot))
}

// tinDefinition is one declared tin index: its catalog name, path spelling,
// and compiled pointer.
type tinDefinition struct {
	name    string
	path    string
	pointer vibejson.CompiledPointer
}

// CompileTinDefinition validates a tin index definition: exactly one path,
// no uniqueness, and a compilable RFC 6901 pointer.
func CompileTinDefinition(def IndexDefinition) (tinDefinition, error) {
	if def.Name == "" {
		return tinDefinition{}, fmt.Errorf("%w: name is empty", ErrIndexDefinition)
	}
	if len(def.Paths) != 1 {
		return tinDefinition{}, fmt.Errorf(
			"%w: tin indexes exactly one path, got %d",
			ErrIndexDefinition, len(def.Paths),
		)
	}
	if def.Unique {
		return tinDefinition{}, fmt.Errorf("%w: tin indexes do not support UNIQUE", ErrIndexDefinition)
	}
	owned := strings.Clone(def.Paths[0])
	pointer, err := vibejson.CompilePointer(owned)
	if err != nil {
		return tinDefinition{}, fmt.Errorf("%w: path: %v", ErrIndexDefinition, err)
	}
	return tinDefinition{name: strings.Clone(def.Name), path: owned, pointer: pointer}, nil
}

// createTinIndexLocked records a tin definition and publishes it in the next
// snapshot. Content builds lazily on first query use (or eagerly via
// BackfillIndex), so creation never scans.
func (c *Collection) createTinIndexLocked(def IndexDefinition) (IndexInfo, error) {
	tdef, err := CompileTinDefinition(def)
	if err != nil {
		return IndexInfo{}, err
	}
	state, err := c.initLocked()
	if err != nil {
		return IndexInfo{}, err
	}
	if c.tinDefs == nil {
		c.tinDefs = make(map[string]tinDefinition)
	}
	if _, exists := c.indexes[def.Name]; exists {
		return IndexInfo{}, ErrIndexExists
	}
	if _, exists := c.tinDefs[tdef.name]; exists {
		return IndexInfo{}, ErrIndexExists
	}
	c.tinDefs[tdef.name] = tdef
	next := *state
	next.Generation++
	next.Indexes = c.indexInfosLocked()
	next.secondary = c.indexSnapshotsLocked()
	c.state.Store(&next)
	return tinIndexInfo(tdef), nil
}

// tinIndexInfo reports a tin definition in catalog form.
func tinIndexInfo(tdef tinDefinition) IndexInfo {
	info := IndexInfo{Name: tdef.name, Kind: IndexTin, State: IndexReady, ColumnCount: 1}
	info.Columns[0] = tdef.path
	return info
}

// dropTinIndexLocked removes a tin definition. Cached builds for live
// snapshots stay valid through their own references; the garbage collector
// reclaims them with their states.
func (c *Collection) dropTinIndexLocked(name string) bool {
	if _, ok := c.tinDefs[name]; !ok {
		return false
	}
	delete(c.tinDefs, name)
	return true
}

// TinIndex returns the tin index built over snapshot s for the named index,
// building it on first use by scanning s's own chunks. The returned index
// must not be mutated: it is shared by every concurrent reader of s.
func (c *Collection) TinIndex(s Snapshot, name string) (*tin.Index, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	tdef, ok := c.tinDefs[name]
	if !ok {
		return nil, ErrIndexNotFound
	}
	state := s.state
	if state == nil {
		return nil, fmt.Errorf("%w: snapshot has no state", ErrIndexNotFound)
	}
	if entry, ok := c.tinCache[state]; ok {
		if ix, ok := entry[name]; ok {
			return ix, nil
		}
	} else if len(c.tinCache) >= maxTinCacheStates {
		// Bound retained builds: executing snapshots hold their own
		// references, so eviction never invalidates a live reader.
		for st := range c.tinCache {
			if st != c.state.Load() {
				delete(c.tinCache, st)
				break
			}
		}
		if len(c.tinCache) >= maxTinCacheStates {
			c.tinCache = make(map[*State]map[string]*tin.Index)
		}
	}
	ix, err := buildTinIndex(state, tdef)
	if err != nil {
		return nil, err
	}
	if c.tinCache == nil {
		c.tinCache = make(map[*State]map[string]*tin.Index)
	}
	entry := c.tinCache[state]
	if entry == nil {
		entry = make(map[string]*tin.Index)
		c.tinCache[state] = entry
	}
	entry[name] = ix
	return ix, nil
}

// maxTinCacheStates bounds state-pinned tin builds retained by a collection.
const maxTinCacheStates = 4

// tinSegKey names one cached segmented build: the index name plus the
// segment count it was partitioned into.
type tinSegKey struct {
	name string
	n    int
}

// TinSegments returns n segment indexes partitioning snapshot s's rows
// for the named tin index, building them in parallel on first use and
// caching per State like TinIndex. Segments share the global (chunk,
// slot) document identities over disjoint contiguous chunk sets, so
// gathered search unions exactly and per-segment top-K merges under one
// shared statistics view. n clamps into [1, chunk count]; the builds are
// never mutated, matching the single index's sharing contract.
func (c *Collection) TinSegments(s Snapshot, name string, n int) ([]*tin.Index, error) {
	if n <= 0 {
		return nil, fmt.Errorf("%w: segment count %d", ErrIndexDefinition, n)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	tdef, ok := c.tinDefs[name]
	if !ok {
		return nil, ErrIndexNotFound
	}
	state := s.state
	if state == nil {
		return nil, fmt.Errorf("%w: snapshot has no state", ErrIndexNotFound)
	}
	if byState, ok := c.tinSegCache[state]; ok {
		if segs, ok := byState[tinSegKey{name, n}]; ok {
			return segs, nil
		}
	} else {
		c.evictTinSegCacheLocked()
	}
	var ids []uint32
	state.Chunks.Each(func(id uint32, _ *Chunk) bool {
		ids = append(ids, id)
		return true
	})
	if n > len(ids) {
		n = len(ids)
	}
	if n == 0 {
		return nil, nil
	}
	segs := make([]*tin.Index, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range segs {
		lo := len(ids) * i / n
		hi := len(ids) * (i + 1) / n
		wg.Add(1)
		go func(i int, chunkIDs []uint32) {
			defer wg.Done()
			ix := tin.NewIndex()
			for _, id := range chunkIDs {
				if err := addChunkToIndex(ix, id, state.Chunks.Get(id), tdef.pointer); err != nil {
					errs[i] = err
					return
				}
			}
			segs[i] = ix
		}(i, ids[lo:hi])
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}
	if c.tinSegCache == nil {
		c.tinSegCache = make(map[*State]map[tinSegKey][]*tin.Index)
	}
	byState := c.tinSegCache[state]
	if byState == nil {
		byState = make(map[tinSegKey][]*tin.Index)
		c.tinSegCache[state] = byState
	}
	byState[tinSegKey{name, n}] = segs
	return segs, nil
}

// evictTinSegCacheLocked bounds retained segmented builds like the single
// builds: executing snapshots hold their own references, so eviction never
// invalidates a live reader.
func (c *Collection) evictTinSegCacheLocked() {
	if len(c.tinSegCache) < maxTinCacheStates {
		return
	}
	for st := range c.tinSegCache {
		if st != c.state.Load() {
			delete(c.tinSegCache, st)
			break
		}
	}
	if len(c.tinSegCache) >= maxTinCacheStates {
		c.tinSegCache = make(map[*State]map[tinSegKey][]*tin.Index)
	}
}

// TinSegmentsForPath resolves the definition covering path and returns the
// n-way segmented build TinSegments makes for s.
func (c *Collection) TinSegmentsForPath(s Snapshot, path string, n int) ([]*tin.Index, error) {
	c.mu.Lock()
	name := ""
	for candidate, tdef := range c.tinDefs {
		if tdef.path == path && (name == "" || candidate < name) {
			name = candidate
		}
	}
	c.mu.Unlock()
	if name == "" {
		return nil, ErrIndexNotFound
	}
	return c.TinSegments(s, name, n)
}

// TinSegmentsForPath returns the segmented build over snapshot s for the
// first definition covering path. A snapshot without a collection, or a
// collection with no tin index over path, reports ErrIndexNotFound.
func (s Snapshot) TinSegmentsForPath(path string, n int) ([]*tin.Index, error) {
	if s.coll == nil {
		return nil, ErrIndexNotFound
	}
	return s.coll.TinSegmentsForPath(s, path, n)
}

// buildTinIndex scans every live slot of state, indexing string values found
// at the definition's path. Non-string values are skipped: tin indexes text.
func buildTinIndex(state *State, tdef tinDefinition) (*tin.Index, error) {
	ix := tin.NewIndex()
	var err error
	state.Chunks.Each(func(id uint32, chunk *Chunk) bool {
		if err != nil {
			return false
		}
		if e := addChunkToIndex(ix, id, chunk, tdef.pointer); e != nil {
			err = e
			return false
		}
		return true
	})
	return ix, err
}

// addChunkToIndex indexes one chunk's live text slots into ix with global
// (chunk, slot) document identities, so segment builds over disjoint
// chunk sets stay mutually disjoint and union exactly.
func addChunkToIndex(ix *tin.Index, id uint32, chunk *Chunk, pointer vibejson.CompiledPointer) error {
	// One decode buffer serves the chunk's escaped strings; unescaped
	// strings alias the source and never touch it.
	var scratch []byte
	for live := chunk.Live; live != 0; live &= live - 1 {
		slot := bits.TrailingZeros64(live)
		text, next, ok := tinSlotBytes(chunk, slot, pointer, scratch)
		if !ok {
			continue
		}
		scratch = next
		ix.AddBytes(TinDocID(id, slot), text)
	}
	return nil
}

// tinSlotBytes extracts the decoded string bytes at pointer for one live
// slot: a source alias when the JSON string is unescaped (zero-copy),
// otherwise decoded into scratch, which the caller reuses across slots.
// The returned text is valid only until the next tinSlotBytes call reuses
// the same scratch; AddBytes consumes it synchronously. Malformed content
// and non-strings report ok == false, exactly like the string lane did.
func tinSlotBytes(chunk *Chunk, slot int, pointer vibejson.CompiledPointer, scratch []byte) (text, next []byte, ok bool) {
	if chunk == nil || chunk.Live&(uint64(1)<<uint(slot)) == 0 {
		return nil, scratch, false
	}
	row := [1]int{int(chunk.Ord[slot])}
	var one [1]vibejson.RawValue
	values, err := chunk.Docs.AppendPointerRows(one[:0], row[:], pointer)
	if err != nil || len(values) != 1 {
		return nil, scratch, false
	}
	if b, ok := values[0].StringBytes(); ok {
		return b, scratch, true
	}
	dec, ok, err := values[0].AppendText(scratch[:0])
	if err != nil || !ok {
		return nil, scratch, false
	}
	return dec, dec, true
}

// TinIndexForPath returns the tin index built over snapshot s for the first
// definition covering path, building it on first use by scanning s's own
// chunks. Definitions over one path hold identical content, so the first
// catalog hit answers for every name over that path. A snapshot taken from a
// collection (or database) carries its collection; a zero Snapshot or one
// whose collection defines no tin index over path reports ErrIndexNotFound.
func (s Snapshot) TinIndexForPath(path string) (*tin.Index, error) {
	if s.coll == nil {
		return nil, ErrIndexNotFound
	}
	return s.coll.TinIndexForPath(s, path)
}

// TinIndexForPath resolves the definition covering path to its catalog name
// and returns the index TinIndex builds for s. The definition scan holds the
// collection lock only for the map lookup; the build itself locks inside
// TinIndex.
func (c *Collection) TinIndexForPath(s Snapshot, path string) (*tin.Index, error) {
	c.mu.Lock()
	name := ""
	for candidate, tdef := range c.tinDefs {
		// Definitions over one path hold identical content, but the
		// winner must still be deterministic: Go map order is random,
		// so resolve to the smallest catalog name, matching the
		// name-sorted catalog indexInfosLocked publishes.
		if tdef.path == path && (name == "" || candidate < name) {
			name = candidate
		}
	}
	c.mu.Unlock()
	if name == "" {
		return nil, ErrIndexNotFound
	}
	return c.TinIndex(s, name)
}

// TinHit is one ranked full-text hit: the document key and its BM25 score.
// Hits arrive in index order: descending score, ties by document identity.
type TinHit struct {
	Key   string
	Score float64
}

// TinSearch is the Go API for full-text search over one path: it resolves
// the tin index covering path in s, parses tinql against that snapshot's
// index, and returns up to topK hits by BM25 score. DocIDs pack the stable
// slot addresses TinDocID documents, so keys resolve through the same state
// the index was built from and a hit can never name a document another
// snapshot's write moved. A missing index reports ErrIndexNotFound and an
// invalid query reports its TINQL parse error; topK <= 0 returns no hits.
func (c *Collection) TinSearch(s Snapshot, path, tinql string, topK int) ([]TinHit, error) {
	if topK <= 0 {
		return nil, nil
	}
	ix, err := s.TinIndexForPath(path)
	if err != nil {
		return nil, err
	}
	q, err := ix.ParseTINQL(tinql)
	if err != nil {
		return nil, err
	}
	state := s.state
	if state == nil {
		return nil, ErrIndexNotFound
	}
	scored := ix.Score(q, topK, nil)
	hits := make([]TinHit, 0, len(scored))
	for _, hit := range scored {
		chunk := state.Chunks.Get(uint32(hit.Doc >> 32))
		slot := int(hit.Doc & 0xffffffff)
		if chunk == nil || slot < 0 || slot >= MaxChunkDocuments ||
			chunk.Live&(uint64(1)<<uint(slot)) == 0 {
			continue
		}
		hits = append(hits, TinHit{Key: chunk.Key(slot), Score: hit.Score})
	}
	return hits, nil
}
