package durable

import (
	"github.com/thesyncim/vibedb/store"
	vibejson "github.com/thesyncim/vibejson"
)

// *Snapshot satisfies store's shared IndexSource shape with its existing
// exported methods — no adapter needed for basic conformance. Repeated probing
// should go through QuerySnapshot below, which reuses one workspace.
var _ store.IndexSource = (*Snapshot)(nil)

// SupportsUpdate reports whether the collection's durability lane and index
// configuration can publish [Collection.Update] batches. It describes a
// structural capability, not a size guarantee: an individual batch may still
// exceed its configured document or byte bounds.
//
// Update is available only on a deferred-canonical lane and only without exact
// indexes; the single-document Put/Delete path maintains exact indexes, but the
// batch publisher deliberately rejects them.
func (c *Collection) SupportsUpdate() bool {
	return c != nil && c.state.Load() != nil &&
		len(c.options.indexes) == 0 && c.deferredCanonicalLane()
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
