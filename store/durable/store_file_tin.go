package durable

import (
	"fmt"
	"strings"

	"github.com/thesyncim/vibedb/internal/tin"
	"github.com/thesyncim/vibedb/store"
	vibejson "github.com/thesyncim/vibejson"
)

// Generation-pinned full-text builds for durable collections.
//
// A tin build scans one snapshot generation once and indexes every declared
// tin path, so one scan serves every definition and every execution against
// that generation. Pinning the build to the generation makes reads
// unconditionally sound — the index and the documents it describes can never
// skew — at the price of a rebuild scan when the collection changes between
// queries. Writes need no hooks at all: a write publishes a new generation,
// and the next query builds for it.
//
// DocIDs are dense per-build ordinals; the build carries the ordinal-to-key
// table beside the indexes so hits resolve without touching the snapshot.
// Builds are immutable after publication: the snapshot may close while a
// caller still scores against its build.

// maxDurableTinCacheGenerations bounds generation-pinned tin builds retained
// by a collection. Executing callers hold their own build reference, so
// eviction never invalidates a live reader.
const maxDurableTinCacheGenerations = 4

// tinSnapshotBuild is one generation's full-text content: per-name indexes
// plus the shared ordinal-to-key table.
type tinSnapshotBuild struct {
	generation uint64
	indexes    map[string]*tin.Index
	keys       [][]byte
}

// TinHit is one ranked full-text hit: the document key and its BM25 score.
// Hits arrive in index order: descending score, ties by document identity.
type TinHit struct {
	Key   string
	Score float64
}

// TinIndexForPath resolves through the snapshot's owning collection. It
// exists for query binding, which holds only the snapshot.
func (s *Snapshot) TinIndexForPath(path string) (*tin.Index, error) {
	if s == nil || s.collection == nil {
		return nil, store.ErrIndexNotFound
	}
	return s.collection.TinIndexForPath(s, path)
}

// TinIndexForPath returns the tin index built over the snapshot's generation
// for the first definition covering path. The index is shared by every
// concurrent reader of the generation and must not be mutated. A snapshot
// whose pinned catalog defines no tin index over path reports
// store.ErrIndexNotFound.
func (c *Collection) TinIndexForPath(snap *Snapshot, path string) (*tin.Index, error) {
	build, name, err := c.tinBuildForPath(snap, path)
	if err != nil {
		return nil, err
	}
	ix, ok := build.indexes[name]
	if !ok {
		return nil, store.ErrIndexNotFound
	}
	return ix, nil
}

// tinBuildForPath resolves the definition covering path to its catalog name
// and returns the generation's build with it.
func (c *Collection) tinBuildForPath(
	snap *Snapshot,
	path string,
) (*tinSnapshotBuild, string, error) {
	if c == nil || snap == nil || snap.state == nil {
		return nil, "", store.ErrIndexNotFound
	}
	name := ""
	for _, definition := range snap.indexDefinitions {
		if definition.Kind == store.IndexTin &&
			len(definition.Paths) == 1 && definition.Paths[0] == path {
			name = definition.Name
			break
		}
	}
	if name == "" {
		return nil, "", store.ErrIndexNotFound
	}
	build, err := c.tinBuild(snap)
	if err != nil {
		return nil, "", err
	}
	return build, name, nil
}

// tinBuild returns the snapshot generation's build, scanning the
// generation's own documents on first use. Definitions come from the
// snapshot's pinned catalog, so an online index change between the snapshot
// and the build cannot smuggle a foreign declaration in.
func (c *Collection) tinBuild(snap *Snapshot) (*tinSnapshotBuild, error) {
	generation := snap.Generation()
	c.tinMu.Lock()
	defer c.tinMu.Unlock()
	if build, ok := c.tinBuilds[generation]; ok {
		return build, nil
	}
	build, err := buildTinSnapshot(snap)
	if err != nil {
		return nil, err
	}
	if c.tinBuilds == nil {
		c.tinBuilds = make(map[uint64]*tinSnapshotBuild)
	}
	c.tinBuilds[generation] = build
	for len(c.tinBuilds) > maxDurableTinCacheGenerations {
		evicted := false
		for gen := range c.tinBuilds {
			if gen != generation {
				delete(c.tinBuilds, gen)
				evicted = true
				break
			}
		}
		if !evicted {
			break
		}
	}
	return build, nil
}

// buildTinSnapshot scans every live document of the snapshot's generation,
// indexing string values found at each declared tin path. Non-string values
// are skipped: tin indexes text. Keys and spellings are copied: range
// buffers are transient, while the build outlives the scan.
func buildTinSnapshot(snap *Snapshot) (*tinSnapshotBuild, error) {
	type tinPath struct {
		name    string
		pointer vibejson.CompiledPointer
	}
	var paths []tinPath
	for _, definition := range snap.indexDefinitions {
		if definition.Kind != store.IndexTin || len(definition.Paths) != 1 {
			continue
		}
		pointer, err := vibejson.CompilePointer(definition.Paths[0])
		if err != nil {
			return nil, fmt.Errorf(
				"%w: tin index %q path: %v",
				store.ErrIndexDefinition, definition.Name, err,
			)
		}
		paths = append(paths, tinPath{
			name: definition.Name, pointer: pointer,
		})
	}
	build := &tinSnapshotBuild{
		generation: snap.Generation(),
		indexes:    make(map[string]*tin.Index, len(paths)),
	}
	for _, path := range paths {
		build.indexes[path.name] = tin.NewIndex()
	}
	ordinal := uint64(0)
	err := snap.RangeRaw(func(key, value []byte) error {
		docID := tin.DocID(ordinal)
		ordinal++
		if len(paths) == 0 {
			return nil
		}
		build.keys = append(build.keys, append([]byte(nil), key...))
		doc, err := vibejson.Parse(value)
		if err != nil {
			return fmt.Errorf("vibedb: tin build parses document: %v", err)
		}
		for _, path := range paths {
			cell, found, err := doc.PointerCompiled(path.pointer)
			if err != nil {
				return fmt.Errorf(
					"vibedb: tin build resolves tin index %q: %v",
					path.name, err,
				)
			}
			if !found {
				continue
			}
			text, ok := cell.Text()
			if !ok {
				continue
			}
			build.indexes[path.name].Add(docID, strings.Clone(text))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return build, nil
}

// TinSearch is the Go API for full-text search over one path: it resolves
// the tin index covering path in the snapshot's generation, parses tinql
// against that generation's index, and returns up to topK hits by BM25
// score. A missing index reports store.ErrIndexNotFound and an invalid
// query reports its TINQL parse error; topK <= 0 returns no hits.
func (c *Collection) TinSearch(
	snap *Snapshot,
	path, tinql string,
	topK int,
) ([]TinHit, error) {
	if topK <= 0 {
		return nil, nil
	}
	build, name, err := c.tinBuildForPath(snap, path)
	if err != nil {
		return nil, err
	}
	ix, ok := build.indexes[name]
	if !ok {
		return nil, store.ErrIndexNotFound
	}
	q, err := ix.ParseTINQL(tinql)
	if err != nil {
		return nil, err
	}
	scored := ix.Score(q, topK, nil)
	hits := make([]TinHit, 0, len(scored))
	for _, hit := range scored {
		if uint64(hit.Doc) >= uint64(len(build.keys)) {
			continue
		}
		hits = append(hits, TinHit{
			Key: string(build.keys[uint64(hit.Doc)]), Score: hit.Score,
		})
	}
	return hits, nil
}
