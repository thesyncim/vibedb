package durable

import (
	"github.com/thesyncim/vibedb/store"
	vibejson "github.com/thesyncim/vibejson"
)

// *Collection and *Snapshot satisfy store's shared Mutable and IndexSource
// shapes with their existing exported methods — no adapter needed for basic
// conformance. Index-lifecycle mutation is deliberately absent: collection's
// indexes are frozen at construction (Options.Indexes), so it does
// not, and should not, implement store.IndexManager.
var (
	_ store.Mutable[*Snapshot] = (*Collection)(nil)
	_ store.IndexSource        = (*Snapshot)(nil)
)

// SupportsUpdate reports whether the collection's current immutable layout and
// durability lane can publish [Collection.Update] batches. It describes a
// structural capability, not a size guarantee: an individual batch may still
// exceed its configured document or byte bounds.
//
// Chunk-layout collections support Update, including exact-index maintenance.
// Ordered-primary collections support it only on a deferred-canonical lane and
// only without exact indexes; their single-document Put/Delete path maintains
// exact indexes, but their batch publisher deliberately rejects them.
func (c *Collection) SupportsUpdate() bool {
	if c == nil || c.state.Load() == nil {
		return false
	}
	if !c.primaryGraphReadOnly() {
		return true
	}
	return len(c.options.indexes) == 0 && c.deferredCanonicalLane()
}

// QuerySnapshot adapts a Snapshot for repeated, zero-allocation index
// probing: it satisfies store.IndexSource like Snapshot itself, but
// routes through the workspace-reusing AppendIndexMasksInto/
// AppendIndexCandidateMasksInto forms instead of allocating a fresh
// IndexWorkspace per call, and optionally accumulates probe statistics.
// Use it (rather than Snapshot directly) wherever a store.IndexSource is
// probed more than once against the same snapshot, such as query plan
// execution.
type QuerySnapshot struct {
	Snapshot     *Snapshot
	Workspace    *IndexWorkspace
	Rechecks     *uint64
	Certificates *uint64
	// PostingPages accumulates IndexProbeStats.PostingPages: the
	// index-directory leaf pages the probes read, which is their physical read
	// work now that stable-slot masks live inline in those leaves.
	PostingPages *int
}

var _ store.IndexSource = QuerySnapshot{}

// AppendIndexes appends the frozen exact-index catalog visible to the
// wrapped snapshot.
func (s QuerySnapshot) AppendIndexes(dst []store.IndexInfo) []store.IndexInfo {
	return s.Snapshot.AppendIndexes(dst)
}

// AppendIndexMasks appends exact stable-slot masks, reusing s.Workspace.
func (s QuerySnapshot) AppendIndexMasks(dst []store.Mask, name string, values ...vibejson.Index) ([]store.Mask, error) {
	out, err := s.Snapshot.AppendIndexMasksInto(dst, s.Workspace, name, values...)
	s.accumulate()
	return out, err
}

// AppendIndexCandidateMasks appends candidate stable-slot masks (not yet
// exact-rechecked), reusing s.Workspace.
func (s QuerySnapshot) AppendIndexCandidateMasks(dst []store.Mask, name string, values ...vibejson.Index) ([]store.Mask, error) {
	out, err := s.Snapshot.AppendIndexCandidateMasksInto(dst, s.Workspace, name, values...)
	s.accumulate()
	return out, err
}

func (s QuerySnapshot) accumulate() {
	if s.Rechecks == nil && s.Certificates == nil && s.PostingPages == nil {
		return
	}
	stats := s.Workspace.LastProbeStats()
	if s.Rechecks != nil {
		*s.Rechecks += stats.DocumentRecheckRows
	}
	if s.Certificates != nil {
		*s.Certificates += stats.CertificateRows
	}
	if s.PostingPages != nil {
		*s.PostingPages += stats.PostingPages
	}
}
