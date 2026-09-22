package store

import (
	"fmt"
	"math/bits"
	"strings"

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

// tinCacheEntry pins one State's built tin indexes.
type tinCacheEntry struct {
	state   *State
	indexes map[string]*tin.Index
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

// buildTinIndex scans every live slot of state, indexing string values found
// at the definition's path. Non-string values are skipped: tin indexes text.
func buildTinIndex(state *State, tdef tinDefinition) (*tin.Index, error) {
	ix := tin.NewIndex()
	var err error
	state.Chunks.Each(func(id uint32, chunk *Chunk) bool {
		if err != nil {
			return false
		}
		for live := chunk.Live; live != 0; live &= live - 1 {
			slot := bits.TrailingZeros64(live)
			text, ok := tinSlotText(chunk, slot, tdef.pointer)
			if !ok {
				continue
			}
			ix.Add(TinDocID(id, slot), text)
		}
		return true
	})
	return ix, err
}

// tinSlotText extracts the decoded string at pointer for one live slot.
func tinSlotText(chunk *Chunk, slot int, pointer vibejson.CompiledPointer) (string, bool) {
	if chunk == nil || chunk.Live&(uint64(1)<<uint(slot)) == 0 {
		return "", false
	}
	row := [1]int{int(chunk.Ord[slot])}
	var one [1]vibejson.RawValue
	values, err := chunk.Docs.AppendPointerRows(one[:0], row[:], pointer)
	if err != nil || len(values) != 1 {
		return "", false
	}
	text, ok, err := values[0].Text()
	if err != nil || !ok {
		return "", false
	}
	return text, true
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
