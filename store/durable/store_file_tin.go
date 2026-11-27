package durable

import (
	"fmt"
	"strings"

	"github.com/thesyncim/vibedb/internal/storeio"
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

// tinDefSpec is one compiled tin declaration pinned to a snapshot build.
type tinDefSpec struct {
	name    string
	path    string
	pointer vibejson.CompiledPointer
}

// tinSnapshotBuild is one generation's full-text content: per-name indexes,
// the shared ordinal-to-key table, and the ordinal-to-stable-slot map that
// prunes file scans to candidate masks. slots is nil when the build cannot
// prune; probes then decline to the full scan.
//
// When leafRefs is non-nil, the build also records the immutable per-bucket
// leaf PageRefs and key sets at its generation so later generations can
// maintain postings incrementally by diffing only changed leaves.
type tinSnapshotBuild struct {
	generation   uint64
	indexes      map[string]*tin.Index
	keys         [][]byte
	slots        *tinSlotMap
	paths        []tinDefSpec
	keyToOrdinal map[string]uint64
	leafRefs     map[storeio.BucketID]storeio.PageRef
	bucketKeys   map[storeio.BucketID][][]byte
	liveRows     int
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

// TinIndex returns the tin index built over the snapshot's generation for
// the named declaration, building it on first use by scanning the
// generation's own documents. The index is shared by every concurrent
// reader of the generation and must not be mutated. A snapshot whose
// pinned catalog declares no tin index under name reports
// store.ErrIndexNotFound.
func (c *Collection) TinIndex(snap *Snapshot, name string) (*tin.Index, error) {
	if c == nil || snap == nil || snap.state == nil {
		return nil, fmt.Errorf("%w: snapshot has no state", store.ErrIndexNotFound)
	}
	declared := false
	for _, definition := range snap.indexDefinitions {
		if definition.Kind == store.IndexTin && definition.Name == name {
			declared = true
			break
		}
	}
	if !declared {
		return nil, store.ErrIndexNotFound
	}
	build, err := c.tinBuild(snap)
	if err != nil {
		return nil, err
	}
	ix, ok := build.indexes[name]
	if !ok {
		return nil, store.ErrIndexNotFound
	}
	return ix, nil
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

// tinBuild returns the snapshot generation's build, maintaining postings
// incrementally from the closest cached generation's leaf diff when eligible
// and falling back to a full generation scan otherwise. Definitions come from
// the snapshot's pinned catalog, so an online index change between the
// snapshot and the build cannot smuggle a foreign declaration in.
func (c *Collection) tinBuild(snap *Snapshot) (*tinSnapshotBuild, error) {
	generation := snap.Generation()
	c.tinMu.Lock()
	defer c.tinMu.Unlock()
	if build, ok := c.tinBuilds[generation]; ok {
		return build, nil
	}
	paths, err := compileSnapshotTinPaths(snap)
	if err != nil {
		return nil, err
	}
	var (
		base     *tinSnapshotBuild
		baseDist uint64
	)
	for gen, candidate := range c.tinBuilds {
		if candidate == nil || candidate.leafRefs == nil || candidate.slots == nil {
			continue
		}
		if !tinDefSpecsEqual(candidate.paths, paths) {
			continue
		}
		var dist uint64
		if generation >= gen {
			dist = generation - gen
		} else {
			dist = gen - generation
		}
		if base == nil || dist < baseDist {
			base = candidate
			baseDist = dist
		}
	}
	var (
		build       *tinSnapshotBuild
		incremental bool
	)
	if base != nil {
		build, incremental, err = buildTinSnapshotIncremental(snap, base, paths)
		if err != nil {
			return nil, err
		}
	}
	if !incremental {
		build, err = buildTinSnapshot(snap, paths)
		if err != nil {
			return nil, err
		}
		c.tinFullBuilds++
	} else {
		c.tinIncrementalBuilds++
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

func compileSnapshotTinPaths(snap *Snapshot) ([]tinDefSpec, error) {
	var paths []tinDefSpec
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
		paths = append(paths, tinDefSpec{
			name: definition.Name, path: definition.Paths[0], pointer: pointer,
		})
	}
	return paths, nil
}

func tinDefSpecsEqual(a, b []tinDefSpec) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].name != b[i].name || a[i].path != b[i].path {
			return false
		}
	}
	return true
}

// buildTinSnapshot scans every live document of the snapshot's generation,
// indexing string values found at each declared tin path. Non-string values
// are skipped: tin indexes text. Keys and spellings are copied: range
// buffers are transient, while the build outlives the scan.
func buildTinSnapshot(snap *Snapshot, paths []tinDefSpec) (*tinSnapshotBuild, error) {
	build := &tinSnapshotBuild{
		generation: snap.Generation(),
		indexes:    make(map[string]*tin.Index, len(paths)),
		paths:      paths,
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
	// Builds are immutable after publication, so seal every index now: the
	// scan above is the list's only mutation, and all generation-pinned
	// queries read packed postings from here on.
	for _, ix := range build.indexes {
		ix.Seal()
	}
	// The slot map joins the leaf walk against the indexed keys and only
	// publishes when the join proves itself exact.
	build.slots, build.keyToOrdinal, build.leafRefs, build.bucketKeys =
		joinTinSlotsWithLeaves(build.keys, snap)
	build.liveRows = len(build.keys)
	return build, nil
}

// buildTinSnapshotIncremental derives snap's tin build from base by diffing
// immutable primary leaf PageRefs on snap.primaryRouter. Only leaves whose
// PageRef changed (or buckets added/removed) are read and re-indexed; all
// untouched sealed posting lists are shared directly via tin.Index.Clone.
//
// Soundness: router.Generation() must equal snap.Generation() both before
// collecting routes and after reading every changed leaf. Any concurrent
// router advance, overflow value, leaf read error, bulk change (>50% leaves),
// or ordinal bloat declines cleanly with (nil, false, nil) so the caller falls
// back to a full rooted RangeRaw scan.
func buildTinSnapshotIncremental(
	snap *Snapshot,
	base *tinSnapshotBuild,
	paths []tinDefSpec,
) (*tinSnapshotBuild, bool, error) {
	if snap == nil || snap.state == nil || base == nil ||
		base.leafRefs == nil || base.slots == nil || len(paths) == 0 {
		return nil, false, nil
	}
	// Periodically compact the ordinal table if churn left >50% dead slots.
	if len(base.keys) > 2*base.liveRows+1024 {
		return nil, false, nil
	}
	router := snap.primaryRouter
	generation := snap.Generation()
	if router == nil || router.Generation() != generation {
		return nil, false, nil
	}
	nLeaves := router.Len()
	curLeafRefs := make(map[storeio.BucketID]storeio.PageRef, nLeaves)
	var changedRoutes []storeio.ResidentPrimaryRoute
	for rank := 0; rank < nLeaves; rank++ {
		route, ok := router.RouteAtRank(rank)
		if !ok {
			return nil, false, nil
		}
		curLeafRefs[route.Bucket] = route.Ref
		if prevRef, existed := base.leafRefs[route.Bucket]; !existed || prevRef != route.Ref {
			changedRoutes = append(changedRoutes, route)
		}
	}
	var removedBuckets []storeio.BucketID
	for bucket := range base.leafRefs {
		if _, exists := curLeafRefs[bucket]; !exists {
			removedBuckets = append(removedBuckets, bucket)
		}
	}
	if router.Generation() != generation {
		return nil, false, nil
	}
	if len(changedRoutes) == 0 && len(removedBuckets) == 0 {
		// No leaf changed across this generation boundary: share the immutable
		// base build directly under the new generation stamp.
		return &tinSnapshotBuild{
			generation:   generation,
			indexes:      base.indexes,
			keys:         base.keys,
			slots:        base.slots,
			paths:        paths,
			keyToOrdinal: base.keyToOrdinal,
			leafRefs:     base.leafRefs,
			bucketKeys:   base.bucketKeys,
			liveRows:     base.liveRows,
		}, true, nil
	}
	if nLeaves > 2 && len(changedRoutes)+len(removedBuckets) > nLeaves/2 {
		return nil, false, nil
	}

	type updatedRow struct {
		key   []byte
		value []byte
		slot  TinSlotHit
	}
	var (
		updatedRows   []updatedRow
		newKeySet     = make(map[string]struct{}, len(changedRoutes)*32)
		newBucketKeys = make(map[storeio.BucketID][][]byte, len(changedRoutes))
		c             = snap.collection
		state         = snap.state
		bounds        = c.primaryLeafBounds(state)
		scratch       []byte
		declined      bool
	)
	for _, route := range changedRoutes {
		lease, err := router.AcquireLeaf(c.cache, route)
		if err != nil {
			return nil, false, nil
		}
		bucket := uint32(route.Bucket)
		var bKeys [][]byte
		scratch, err = storeio.VisitPrimaryLeafPostingRows(
			lease.Page(), state.root.StoreID, route.Bucket, bounds, scratch,
			func(slot uint8, key, value []byte, overflow bool) error {
				if overflow {
					declined = true
					return errTinSlotSkew
				}
				keyCopy := append([]byte(nil), key...)
				valCopy := append([]byte(nil), value...)
				bKeys = append(bKeys, keyCopy)
				newKeySet[string(keyCopy)] = struct{}{}
				updatedRows = append(updatedRows, updatedRow{
					key:   keyCopy,
					value: valCopy,
					slot:  TinSlotHit{Chunk: bucket<<2 | uint32(slot>>6), Bit: slot & 63},
				})
				return nil
			},
		)
		lease.Release()
		if err != nil || declined {
			return nil, false, nil
		}
		newBucketKeys[route.Bucket] = bKeys
	}
	if router.Generation() != generation {
		return nil, false, nil
	}

	// Phase 2: clone base indexes (sharing all untouched sealed postings) and
	// remove keys that disappeared from removed or changed buckets and did not
	// relocate into another changed bucket.
	indexes := make(map[string]*tin.Index, len(paths))
	for _, p := range paths {
		prevIx := base.indexes[p.name]
		if prevIx == nil {
			return nil, false, nil
		}
		indexes[p.name] = prevIx.Clone()
	}
	keys := make([][]byte, len(base.keys), len(base.keys)+len(updatedRows))
	copy(keys, base.keys)
	slots := make([]TinSlotHit, len(base.slots.slots), len(base.slots.slots)+len(updatedRows))
	copy(slots, base.slots.slots)
	keyToOrdinal := make(map[string]uint64, len(base.keyToOrdinal)+len(updatedRows))
	for k, ord := range base.keyToOrdinal {
		keyToOrdinal[k] = ord
	}
	bucketKeys := make(map[storeio.BucketID][][]byte, len(curLeafRefs))
	for b, ks := range base.bucketKeys {
		bucketKeys[b] = ks
	}
	liveRows := base.liveRows

	removeKey := func(k []byte) {
		ks := string(k)
		if _, stillPresent := newKeySet[ks]; stillPresent {
			return
		}
		ord, ok := keyToOrdinal[ks]
		if !ok {
			return
		}
		delete(keyToOrdinal, ks)
		if ord < uint64(len(keys)) {
			keys[ord] = nil
		}
		liveRows--
		docID := tin.DocID(ord)
		for _, ix := range indexes {
			ix.Remove(docID)
		}
	}
	for _, bucket := range removedBuckets {
		for _, k := range base.bucketKeys[bucket] {
			removeKey(k)
		}
		delete(bucketKeys, bucket)
	}
	for _, route := range changedRoutes {
		for _, k := range base.bucketKeys[route.Bucket] {
			removeKey(k)
		}
		bucketKeys[route.Bucket] = newBucketKeys[route.Bucket]
	}

	// Phase 3: upsert every live row from the changed leaves.
	for _, row := range updatedRows {
		ks := string(row.key)
		ord, existed := keyToOrdinal[ks]
		if !existed {
			ord = uint64(len(keys))
			keys = append(keys, row.key)
			slots = append(slots, row.slot)
			keyToOrdinal[ks] = ord
			liveRows++
		} else {
			slots[ord] = row.slot
		}
		doc, err := vibejson.Parse(row.value)
		if err != nil {
			return nil, false, fmt.Errorf("vibedb: tin build parses document: %v", err)
		}
		docID := tin.DocID(ord)
		for _, path := range paths {
			cell, found, err := doc.PointerCompiled(path.pointer)
			if err != nil {
				return nil, false, fmt.Errorf(
					"vibedb: tin build resolves tin index %q: %v",
					path.name, err,
				)
			}
			if found {
				if text, ok := cell.Text(); ok {
					indexes[path.name].Add(docID, strings.Clone(text))
					continue
				}
			}
			if existed {
				indexes[path.name].Remove(docID)
			}
		}
	}

	for _, ix := range indexes {
		ix.Seal()
	}
	return &tinSnapshotBuild{
		generation:   generation,
		indexes:      indexes,
		keys:         keys,
		slots:        &tinSlotMap{slots: slots, rows: liveRows},
		paths:        paths,
		keyToOrdinal: keyToOrdinal,
		leafRefs:     curLeafRefs,
		bucketKeys:   bucketKeys,
		liveRows:     liveRows,
	}, true, nil
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
