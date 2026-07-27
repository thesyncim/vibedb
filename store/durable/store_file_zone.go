package durable

import (
	"math/bits"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
)

// Per-chunk zone maps for the durable backend.
//
// The heap backend has carried chunk summaries since store/store_zone.go; this
// file is the same tier for a collection whose chunks are on disk, which is
// where block pruning is actually worth something. On the heap a pruned chunk
// is a scan avoided. Here it is a page never faulted, and at a low enough
// selectivity that is the difference between a query that reads the collection
// and a query that reads its directory.
//
// Three questions had to be answered that the heap backend never faced.
//
// # Where the summary lives
//
// In the chunk directory's radix leaf, beside the reference it describes — not
// in the chunk page (which would have to be read to be pruned, defeating the
// purpose), and not in a parallel structure (which would cost its own pages to
// read and write, and its own commit ordering to get wrong).
//
// The leaf is the page a reader must open anyway to learn where a chunk lives,
// so probing costs no read the scan was not already going to pay. It is also
// the page a writer must rewrite anyway when a chunk moves, and every chunk
// mutation moves its chunk: the format is copy-on-write, so a Put allocates a
// new document extent and rewrites the root-to-leaf directory path to point at
// it. The summary rides along in bytes the commit was already writing.
//
// The price is 30 bytes a lane — all a full 64-lane leaf has left in a
// 4096-byte page — which buys three tracked paths rather than the heap's
// eight, at 24-bit rather than 32-bit resolution. store_zone_compact.go
// argues why each of those losses is sound.
//
// # What it costs the write path
//
// Nothing in pages and one fold in CPU. A Put already rebuilds its chunk page
// and already rewrites its directory path; the summary adds no allocation, no
// page, and no fsync. The CPU is one linear pass over the document being
// written — the same merge-only, O(1)-in-chunk-size discipline store_zone.go
// uses, for the same reason: a summary that re-read the chunk's other 63
// documents would multiply the dominant cost of the write path instead of
// adding to it.
//
// The one case that does re-read is a chunk whose carried-forward summary is
// stale — a chunk that has never had one, which today means a chunk a bulk
// build published. That rebuild happens once per chunk, on the first commit
// that touches it, out of rows the commit already has in hand.
//
// # How it survives a crash
//
// By not being a separate thing that can disagree. The summary is in the same
// page, under the same checksum, allocated in the same write transaction, and
// published by the same state-root commit as the chunk reference it describes.
// A torn write fails the leaf's checksum and the whole generation is
// discarded; a crash before the superblock switch leaves the previous state
// root, whose leaves carry the previous summaries beside the previous
// references. There is no ordering to get wrong because there is no second
// write to order.
//
// The remaining risk is not physical but logical: a summary that is sound for
// a chunk it no longer describes. That is prevented by construction too —
// every path that publishes a chunk extent goes through
// [storeio.UpsertChunkTreeZone] with a merger, or through it without one, and
// without one the lane's summary is reset to "no statistics" rather than left
// pointing at the previous chunk's values.

// fileZoneMerger folds a commit's chunk rebuild into the summary the rewritten
// directory leaf will carry. One lives on each Collection and is reused, so
// handing it to storeio as an interface costs no allocation.
type fileZoneMerger struct {
	// rows is the complete replacement row set for the chunk, ascending by
	// slot. It is only read when the carried-forward summary is stale and has
	// to be rebuilt from scratch.
	rows []storeio.DocumentRecord
	// edits are the rows this commit newly wrote into the chunk, which is all
	// an up-to-date summary has to fold.
	edits []fileChunkEdit
	// priorDocs is how many live documents the chunk held before this commit.
	// A path first seen now was carried by none of them; see store_zone.go's
	// ZonePaths for why that deduction needs the entry table to only grow.
	priorDocs int
	summary   store.ZoneSummary
}

var _ storeio.ChunkZoneMerger = (*fileZoneMerger)(nil)

// MergeChunkZone is called from inside the leaf rewrite, with the summary that
// lane carries today.
func (m *fileZoneMerger) MergeChunkZone(old storeio.ChunkZone) storeio.ChunkZone {
	m.summary.Decode((*[store.ZoneCompactBytes]byte)(&old))
	switch m.summary.State() {
	case store.ZoneCompactPoisoned:
		// Permanent for this chunk's lineage: whatever the fold could not
		// summarize is still in the chunk, and recomputing would meet it again.
		return old
	case store.ZoneCompactStale:
		m.rebuild()
	default:
		m.fold()
	}
	var out storeio.ChunkZone
	m.summary.Encode((*[store.ZoneCompactBytes]byte)(&out))
	return out
}

func (m *fileZoneMerger) rebuild() {
	m.summary.Reset()
	for i := range m.rows {
		if !zoneFoldRecord(&m.summary, &m.rows[i], i) {
			return
		}
	}
}

func (m *fileZoneMerger) fold() {
	prior := m.priorDocs
	for i := range m.edits {
		if !m.edits[i].keep {
			// Merge-only: a removed document's values stay in the bounds. That
			// is a precision loss under churn and never a correctness one, and
			// it is what keeps maintenance independent of chunk size.
			continue
		}
		if !zoneFoldRecord(&m.summary, &m.edits[i].record, prior) {
			return
		}
		prior++
	}
}

// zoneFoldRecord folds one row and reports whether folding may continue. A row
// whose JSON lives in an overflow chain has no inline spelling here, and
// summarizing it would mean reading the chain on the write path. Poisoning the
// chunk instead costs pruning for chunks that happen to hold an oversized
// document and costs nothing else; folding the row as absent would be a false
// negative, which is the one error a summary may never make.
func zoneFoldRecord(summary *store.ZoneSummary, row *storeio.DocumentRecord, priorDocs int) bool {
	if len(row.JSON) == 0 {
		summary.Poison()
		return false
	}
	summary.Fold(row.JSON, priorDocs)
	return summary.Sound()
}

// zoneMerger prepares the collection's reusable merger for one chunk rebuild
// and returns it, or nil when chunk summaries are switched off. Returning nil
// is what makes [store.SetZonePruning] a complete control: with pruning off
// nothing is folded and nothing is written, so a differential run compares two
// stores whose only difference is the summaries, not two query plans over one.
func (c *Collection) zoneMerger(rows []storeio.DocumentRecord, edits []fileChunkEdit, priorDocs int) storeio.ChunkZoneMerger {
	if !store.ZonePruning() {
		return nil
	}
	c.zoneScratch.rows = rows
	c.zoneScratch.edits = edits
	c.zoneScratch.priorDocs = priorDocs
	return &c.zoneScratch
}

// zonePriorDocs is the live-document count of the chunk a rebuild replaces.
func zonePriorDocs(old *fileDocumentChunk) int {
	if old == nil {
		return 0
	}
	return bits.OnesCount64(old.live())
}

// AppendZoneMasks appends one mask per chunk whose summary cannot rule out a
// match, and reports how many chunks it skipped and whether the result bounds
// anything. It reads directory pages only: a pruned chunk's extent is never
// acquired, so pruning here is avoided I/O rather than avoided CPU.
//
// The bits of a surviving chunk's mask are the chunk's full stable-slot
// universe rather than its exact live word, which lives in the document page
// header and would cost the very read this tier exists to avoid. Range
// consumers ignore dead candidate bits by contract ([store.Mask]), so a
// too-wide word costs nothing but the recheck the executor was going to
// perform anyway.
//
// An unpruned result is reported unbounded, exactly as the heap backend does:
// a mask set covering every chunk is a sound superset and a useless one, and
// saying so lets the caller skip a mask intersection and stay on its ordinary
// scan.
func (s *Snapshot) AppendZoneMasks(dst []store.Mask, probe store.ZoneProbe) ([]store.Mask, int, bool) {
	if s == nil || s.collection == nil || s.state == nil ||
		probe.Op == store.ZoneOpNone || !store.ZonePruning() {
		return dst, 0, false
	}
	state := s.state
	live := fileStoreLiveMask(state.root.ChunkDocuments)
	pruned := 0
	var summary store.ZoneSummary
	err := storeio.WalkChunkTreeZones(
		s.collection.cache, state.chunkRoot,
		storeio.ChunkTreeBounds{FileEnd: state.super.FileEnd, NextLogicalID: state.root.NextLogicalID},
		func(chunk uint32, zone storeio.ChunkZone) error {
			summary.Decode((*[store.ZoneCompactBytes]byte)(&zone))
			if !summary.Keep(probe) {
				pruned++
				return nil
			}
			dst = append(dst, store.Mask{Chunk: chunk, Bits: live})
			return nil
		},
	)
	if err != nil || pruned == 0 {
		// A directory read that failed here is not this tier's to report: the
		// scan is about to walk the same pages and will surface it with
		// context. Declining to bound is always safe.
		return dst[:0], 0, false
	}
	return dst, pruned, true
}

// ZoneStats reports how many chunks this snapshot has, how many carry a
// summary that can prune, and how many path entries those summaries hold in
// total. It exists for tests and benchmark reporting and reads directory pages
// only.
func (s *Snapshot) ZoneStats() (chunks, sound, paths int, err error) {
	if s == nil || s.collection == nil || s.state == nil {
		return 0, 0, 0, ErrClosed
	}
	state := s.state
	var summary store.ZoneSummary
	walkErr := storeio.WalkChunkTreeZones(
		s.collection.cache, state.chunkRoot,
		storeio.ChunkTreeBounds{FileEnd: state.super.FileEnd, NextLogicalID: state.root.NextLogicalID},
		func(_ uint32, zone storeio.ChunkZone) error {
			chunks++
			summary.Decode((*[store.ZoneCompactBytes]byte)(&zone))
			if summary.Sound() {
				sound++
				paths += summary.Paths()
			}
			return nil
		},
	)
	return chunks, sound, paths, walkErr
}

// ZoneChunksScanned reports how many of this snapshot's chunks survive probe.
// It is the measurement hook the pruning benchmarks report chunks-skipped
// from, and it performs no document I/O.
func (s *Snapshot) ZoneChunksScanned(probe store.ZoneProbe) (kept, total int, err error) {
	if s == nil || s.collection == nil || s.state == nil {
		return 0, 0, ErrClosed
	}
	state := s.state
	var summary store.ZoneSummary
	walkErr := storeio.WalkChunkTreeZones(
		s.collection.cache, state.chunkRoot,
		storeio.ChunkTreeBounds{FileEnd: state.super.FileEnd, NextLogicalID: state.root.NextLogicalID},
		func(_ uint32, zone storeio.ChunkZone) error {
			total++
			summary.Decode((*[store.ZoneCompactBytes]byte)(&zone))
			if summary.Keep(probe) {
				kept++
			}
			return nil
		},
	)
	return kept, total, walkErr
}

// The capability is offered on *Snapshot and deliberately not on
// QuerySnapshot. QuerySnapshot exists to carry an index workspace and three
// stats accumulators through repeated index probes, and a summary probe needs
// none of them — but it is five pointers wide, so converting one to a
// store.ZoneSource interface has to copy it to the heap, while a *Snapshot
// converts in place. The planner hands over the narrower type for exactly that
// reason (see query/file_candidates.go), and forwarding the method here would
// only offer a caller a version that costs 48 bytes an execution to reach the
// same code.
var _ store.ZoneSource = (*Snapshot)(nil)

// bulkChunkZone computes one chunk's summary during a bulk build.
//
// A bulk build is the one path where recomputing from scratch is the *cheaper*
// option: it has every row in hand, publishes each chunk exactly once, and has
// no previous summary to carry forward. It is also the path that matters most
// for whether pruning is available at all — CreateFrom is how a large
// collection is built, and a store whose chunks had no summaries until someone
// wrote to them would offer no pruning on exactly the workload that wants it.
//
// A document that will live in an overflow chain poisons its chunk for the
// same reason it does on the incremental path: the summary must never claim to
// cover a document it did not read.
func (b *fileStoreBulkBuild) bulkChunkZone(document int) storeio.ChunkZone {
	var out storeio.ChunkZone
	if !store.ZonePruning() {
		return out
	}
	plan := b.documents[document]
	var summary store.ZoneSummary
	summary.Reset()
	prior := 0
	for row := plan.first; row < plan.last; row++ {
		_, _, raw := b.sourceRow(row)
		if len(raw) == 0 {
			summary.Poison()
			break
		}
		summary.Fold(raw, prior)
		if !summary.Sound() {
			break
		}
		prior++
	}
	summary.Encode((*[store.ZoneCompactBytes]byte)(&out))
	return out
}
