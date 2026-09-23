package durable

import (
	"errors"
	"slices"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/internal/tin"
	"github.com/thesyncim/vibedb/store"
)

// errTinSlotSkew fails the key join closed: a visited key the build never
// indexed, or one visited twice, means the leaf walk and the RangeRaw
// sequence disagree, so no table is published.
var errTinSlotSkew = errors.New("vibedb: tin slot join skew")

// Generation-pinned ordinal-to-stable-slot maps for tin pruning.
//
// A tin build indexes one snapshot generation's live rows in RangeRaw
// (bytewise lexical key) order with dense ordinals, while the file scan
// reads stable slots addressed by physical chunk. The slot map bridges
// them: one stable slot per indexed ordinal, joined BY KEY — never by
// position — so no order assumption can misalign it.
//
// Why a join: stable slots are hash-assigned, not lexical, so the Nth set
// bit of a live-word walk is not the Nth RangeRaw row once a bucket holds
// more than a trivial shape. The build instead walks the generation's
// leaves for (key, slot) pairs and joins them against the indexed key
// sequence. Document keys are unique, so the join validates itself: every
// indexed key resolves to exactly one visited slot and the visited set
// equals the indexed set, or the build declines every probe. A stale leaf
// (rewritten after the snapshot), a missed bucket, or any skew fails the
// equality and declines — recall can never silently narrow.
//
// The leaf walk needs readable (256-slot geometry) leaves: collections
// that keep slot geometry (exact or tin declared) maintain it on the
// write path, and tin declare repartitions pre-existing wide stripes.
// Anything else declines to the full scan.
//
// A TinBuild is immutable after publication: the snapshot may close while
// a caller still probes against its build. Query execution shares builds
// read-only across workers exactly like the tin indexes beside them.

// tinPruneDeclineFraction bounds the hit set a probe will mask: beyond
// one quarter of the indexed rows the masked reads cost more than the
// sequential scan they replace, so the probe declines and the scan runs
// whole. Full-match queries (`*`) always decline this way.
const tinPruneDeclineFraction = 4

// TinSlotHit is one match ordinal resolved to its stable slot.
type TinSlotHit struct {
	Chunk uint32
	Bit   uint8
}

// TinBuild is the query-facing full-text handle for one tin declaration:
// the generation-pinned index to parse and match against, plus the
// ordinal-to-stable-slot map. A nil slot map declines every probe to the
// full scan.
type TinBuild struct {
	index *tin.Index
	slots *tinSlotMap
}

// tinSlotMap is one generation's ordinal-to-stable-slot table in indexed
// (RangeRaw) order: slots[o] addresses the oth indexed row. rows is that
// row count. The table prunes only when the key join proved it exact.
type tinSlotMap struct {
	slots []TinSlotHit
	rows  int
}

// Index returns the generation-pinned tin index to parse TINQL against.
func (b *TinBuild) Index() *tin.Index {
	if b == nil {
		return nil
	}
	return b.index
}

// TinBuildForPath returns the query-facing full-text handle for the first
// definition covering path: the generation-pinned index plus the
// ordinal-to-stable-slot map. It shares the collection's cached
// per-generation build, so concurrent readers of one generation share one
// table. A snapshot whose pinned catalog defines no tin index over path
// reports store.ErrIndexNotFound.
func (s *Snapshot) TinBuildForPath(path string) (*TinBuild, error) {
	if s == nil || s.collection == nil {
		return nil, store.ErrIndexNotFound
	}
	build, name, err := s.collection.tinBuildForPath(s, path)
	if err != nil {
		return nil, err
	}
	ix, ok := build.indexes[name]
	if !ok || ix == nil {
		return nil, store.ErrIndexNotFound
	}
	return &TinBuild{index: ix, slots: build.slots}, nil
}

// PrimaryRouter exposes the snapshot's live primary router for probe-time
// bucket resolvability checks. The router object is collection-owned and
// versioned: retaining it across the execution is race-safe, and a stale
// resolution only ever declines a probe, never misroutes one.
func (s *Snapshot) PrimaryRouter() *storeio.ResidentPrimaryRouter {
	if s == nil {
		return nil
	}
	return s.primaryRouter
}

// tinSlotWalk visits every occupied stable slot of the snapshot's leaves
// in rank order, reporting (key, chunk, bit). It reads the leaves the
// snapshot's own router names; a rewritten leaf, a missing rank, or a
// non-compact stripe declines rather than misreads. Values are never
// decoded: the walk only needs keys to join on.
func tinSlotWalk(
	snap *Snapshot,
	fn func(key []byte, chunk uint32, bit uint8) error,
) error {
	router := snap.primaryRouter
	if router == nil || snap.state == nil {
		return storeio.ErrSegmentedTabletRouterCorrupt
	}
	c := snap.collection
	state := snap.state
	bounds := c.primaryLeafBounds(state)
	var scratch []byte
	for rank := 0; rank < router.Len(); rank++ {
		route, ok := router.RouteAtRank(rank)
		if !ok {
			return storeio.ErrSegmentedTabletRouterCorrupt
		}
		lease, err := router.AcquireLeaf(c.cache, route)
		if err != nil {
			return err
		}
		bucket := uint32(route.Bucket)
		scratch, err = storeio.VisitPrimaryLeafPostingRows(
			lease.Page(), state.root.StoreID, route.Bucket, bounds, scratch,
			func(slot uint8, key, _ []byte, _ bool) error {
				return fn(key, bucket<<2|uint32(slot>>6), slot&63)
			},
		)
		lease.Release()
		if err != nil {
			return err
		}
	}
	return nil
}

// joinTinSlots builds the ordinal-to-slot table by joining the leaf walk
// against the indexed key sequence. It returns nil unless the join proves
// itself exact: every indexed key resolves to exactly one visited slot.
// Document keys are unique, so no-duplicates plus a full count is the
// whole proof — a rewritten leaf, a missed bucket, or any skew fails it
// and declines every probe. All allocations here are build-time (once per
// generation); queries only index the retained table.
func joinTinSlots(keys [][]byte, snap *Snapshot) *tinSlotMap {
	slots := make([]TinSlotHit, len(keys))
	pos := make(map[string]int, len(keys))
	for o, key := range keys {
		s := string(key)
		if _, dup := pos[s]; dup {
			return nil
		}
		pos[s] = o
	}
	found := make([]bool, len(keys))
	seen := 0
	err := tinSlotWalk(snap, func(key []byte, chunk uint32, bit uint8) error {
		o, ok := pos[string(key)]
		if !ok {
			return errTinSlotSkew
		}
		if found[o] {
			return errTinSlotSkew
		}
		found[o] = true
		slots[o] = TinSlotHit{Chunk: chunk, Bit: bit}
		seen++
		return nil
	})
	if err != nil || seen != len(keys) {
		return nil
	}
	return &tinSlotMap{slots: slots, rows: len(keys)}
}

// AppendCandidateMasks maps ascending match ordinals to strictly ascending
// stable-slot masks, appending into dst and resolving through scratch.
// It reports ok=false — decline to the full scan — when the build cannot
// prune, an ordinal escapes the indexed rows, the ordinals arrive out of
// order, a candidate bucket no longer resolves, or the hit set is too
// large to beat a sequential scan. Empty ids match nothing and return dst
// unchanged with ok=true. scratch carries the resolved slots in chunk
// order for zero-alloc reuse.
//
// Masks name only live-as-of-the-build-generation slots; the scan still
// rechecks every candidate row, so the probe narrows but never decides.
func (b *TinBuild) AppendCandidateMasks(
	router *storeio.ResidentPrimaryRouter,
	ids []tin.DocID,
	dst []store.Mask,
	scratch []TinSlotHit,
) ([]store.Mask, []TinSlotHit, bool) {
	if len(ids) == 0 {
		return dst, scratch, true
	}
	if b == nil || b.slots == nil || router == nil {
		return dst, scratch, false
	}
	m := b.slots
	if m.rows <= 0 || len(ids)*tinPruneDeclineFraction > m.rows {
		return dst, scratch, false
	}
	// Direct table lookup per ordinal; duplicates re-emit the same slot
	// and the fold ORs them idempotently. A backward step breaks the
	// ascending contract: decline rather than misorder masks.
	scratch = scratch[:0]
	var prev uint64
	for i, id := range ids {
		ord := uint64(id)
		if i > 0 && ord < prev {
			return dst, scratch, false
		}
		if ord >= uint64(len(m.slots)) {
			return dst, scratch, false
		}
		scratch = append(scratch, m.slots[ord])
		prev = ord
	}
	dst, ok := foldSlotMasks(router, scratch, dst)
	if !ok {
		return dst, scratch, false
	}
	return dst, scratch, true
}

// foldSlotMasks sorts resolved slots into strictly ascending chunk order
// — chunk order need not match the indexed order — and folds bit runs
// into masks. A candidate bucket that no longer resolves declines the
// probe: delete-all plus compaction over an old snapshot stays exact by
// falling back to the full scan rather than erroring or skipping rows.
func foldSlotMasks(
	router *storeio.ResidentPrimaryRouter,
	scratch []TinSlotHit,
	dst []store.Mask,
) ([]store.Mask, bool) {
	slices.SortFunc(scratch, func(a, c TinSlotHit) int {
		if a.Chunk != c.Chunk {
			if a.Chunk < c.Chunk {
				return -1
			}
			return 1
		}
		if a.Bit != c.Bit {
			if a.Bit < c.Bit {
				return -1
			}
			return 1
		}
		return 0
	})
	var cur uint32
	var bitsAcc uint64
	started := false
	for _, hit := range scratch {
		if !started || hit.Chunk != cur {
			if started {
				dst = append(dst, store.Mask{Chunk: cur, Bits: bitsAcc})
			}
			if _, _, ok := router.ResolveBucketFloor(
				storeio.BucketID(hit.Chunk >> 2),
			); !ok {
				return dst, false
			}
			cur, bitsAcc, started = hit.Chunk, 0, true
		}
		bitsAcc |= uint64(1) << hit.Bit
	}
	if started {
		dst = append(dst, store.Mask{Chunk: cur, Bits: bitsAcc})
	}
	return dst, true
}
