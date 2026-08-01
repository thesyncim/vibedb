package durable

import (
	"github.com/thesyncim/vibedb/store"
	vibejson "github.com/thesyncim/vibejson"
)

// *Snapshot satisfies store's shared IndexSource shape with its existing
// exported methods — no adapter needed for basic conformance. Repeated probing
// should go through IndexSession below, which owns reusable private scratch.
var _ store.IndexSource = (*Snapshot)(nil)

// SupportsUpdate reports whether the collection's durability lane can publish
// [Collection.Update] batches. It describes a structural capability, not a size
// guarantee: an individual batch may still exceed its configured document or
// byte bounds. Exact indexes participate in the same atomic publication as the
// primary leaves and therefore do not narrow this capability.
func (c *Collection) SupportsUpdate() bool {
	return c != nil && c.state.Load() != nil && c.deferredCanonicalLane()
}

// IndexSession binds one borrowed Snapshot to reusable exact-index probe
// storage. Its zero value is ready to Reset and use. The scratch workspace and
// live counters are private: callers cannot splice their own pointers into an
// execution or couple themselves to mutation-oriented engine fields.
//
// A session is single-consumer and must not be used concurrently. Reset binds
// another snapshot and clears accumulated metrics without discarding retained
// capacity. Release drops the capacity. The session does not own the Snapshot
// lease; close or replace the snapshot only after the current probe finishes.
type IndexSession struct {
	snapshot  *Snapshot
	workspace IndexWorkspace
	metrics   IndexSessionMetrics
}

var _ store.IndexSource = (*IndexSession)(nil)

// IndexSessionMetrics is a detached, stable summary accumulated since the
// session's last Reset. It reports logical candidate work and physical posting
// reads without exposing the workspace used to produce them.
type IndexSessionMetrics struct {
	Probes              uint64
	CandidateRows       uint64
	CertificateRows     uint64
	DocumentRecheckRows uint64
	MatchedRows         uint64
	CandidateChunks     uint64
	PostingPages        uint64
}

// NewIndexSession returns a session bound to snapshot. Callers that retain a
// session in another object can use its zero value plus Reset to avoid this one
// construction allocation.
func NewIndexSession(snapshot *Snapshot) *IndexSession {
	session := new(IndexSession)
	session.Reset(snapshot)
	return session
}

// Reset binds snapshot and starts a fresh metrics interval while preserving
// the session's high-water scratch capacity.
func (s *IndexSession) Reset(snapshot *Snapshot) {
	if s == nil {
		return
	}
	s.snapshot = snapshot
	s.metrics = IndexSessionMetrics{}
}

// Release unbinds the snapshot and drops all retained probe capacity.
func (s *IndexSession) Release() {
	if s == nil {
		return
	}
	s.snapshot = nil
	s.metrics = IndexSessionMetrics{}
	s.workspace.Release()
}

// Metrics returns a detached value snapshot of work accumulated since Reset.
func (s *IndexSession) Metrics() IndexSessionMetrics {
	if s == nil {
		return IndexSessionMetrics{}
	}
	return s.metrics
}

// AppendIndexes appends the immutable exact-index catalog visible to the
// bound snapshot. An unbound session behaves like an empty catalog.
func (s *IndexSession) AppendIndexes(dst []store.IndexInfo) []store.IndexInfo {
	if s == nil || s.snapshot == nil {
		return dst
	}
	return s.snapshot.AppendIndexes(dst)
}

// AppendIndexMasks appends exact stable-slot masks using retained private
// scratch and adds the probe's work to Metrics.
func (s *IndexSession) AppendIndexMasks(dst []store.Mask, name string, values ...vibejson.Index) ([]store.Mask, error) {
	if s == nil || s.snapshot == nil {
		return dst, ErrClosed
	}
	out, err := s.snapshot.AppendIndexMasksInto(dst, &s.workspace, name, values...)
	s.accumulate()
	return out, err
}

// AppendIndexCandidateMasks appends candidate stable-slot masks using retained
// private scratch and adds the probe's work to Metrics.
func (s *IndexSession) AppendIndexCandidateMasks(dst []store.Mask, name string, values ...vibejson.Index) ([]store.Mask, error) {
	if s == nil || s.snapshot == nil {
		return dst, ErrClosed
	}
	out, err := s.snapshot.AppendIndexCandidateMasksInto(dst, &s.workspace, name, values...)
	s.accumulate()
	return out, err
}

func (s *IndexSession) accumulate() {
	stats := s.workspace.LastProbeStats()
	s.metrics.Probes++
	s.metrics.CandidateRows += stats.CandidateRows
	s.metrics.CertificateRows += stats.CertificateRows
	s.metrics.DocumentRecheckRows += stats.DocumentRecheckRows
	s.metrics.MatchedRows += stats.MatchedRows
	if stats.CandidateChunks > 0 {
		s.metrics.CandidateChunks += uint64(stats.CandidateChunks)
	}
	if stats.PostingPages > 0 {
		s.metrics.PostingPages += uint64(stats.PostingPages)
	}
}
