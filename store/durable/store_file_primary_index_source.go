package durable

import (
	"github.com/thesyncim/vibedb/store"
	vibejson "github.com/thesyncim/vibejson"
)

// The ordered-primary Snapshot's store.IndexSource surface. Every collection is
// a primary-layout store, so a probe routes straight to the resident exact
// term-leaf index published beside the graph (appendPrimaryExactMasks). Those
// masks are rechecked against the live posting map on read, so they are already
// exact — candidate and exact probes coincide.

// AppendIndexes appends the frozen exact-index catalog visible to the snapshot.
func (s *Snapshot) AppendIndexes(dst []store.IndexInfo) []store.IndexInfo {
	if s == nil || s.collection == nil || s.state == nil {
		return dst
	}
	for _, definition := range s.collection.options.Indexes {
		info := store.IndexInfo{
			Name: definition.Name, Kind: store.IndexExact, State: store.IndexReady,
			// The ordered-primary exact index covers every document, so it is fully
			// covered by construction. The planner keys indexability off State and
			// ColumnCount; coverage is reported as one complete unit.
			TotalChunks: 1, CoveredChunks: 1,
			ColumnCount: uint8(len(definition.Paths)),
		}
		copy(info.Columns[:], definition.Paths)
		dst = append(dst, info)
	}
	return dst
}

// AppendIndexMasks appends exact stable-slot masks for the named index.
func (s *Snapshot) AppendIndexMasks(dst []store.Mask, name string, values ...vibejson.Index) ([]store.Mask, error) {
	var workspace IndexWorkspace
	return s.AppendIndexMasksInto(dst, &workspace, name, values...)
}

// AppendIndexMasksInto is AppendIndexMasks with reusable transient storage.
func (s *Snapshot) AppendIndexMasksInto(dst []store.Mask, workspace *IndexWorkspace, name string, values ...vibejson.Index) ([]store.Mask, error) {
	if s == nil || s.collection == nil || s.state == nil {
		return dst, ErrClosed
	}
	if workspace == nil {
		var local IndexWorkspace
		workspace = &local
	}
	return s.appendPrimaryExactMasks(dst, workspace, name, values)
}

// AppendIndexCandidateMasks appends stable-slot masks for the named index. On
// the ordered primary these are already exact, so it coincides with
// AppendIndexMasks.
func (s *Snapshot) AppendIndexCandidateMasks(dst []store.Mask, name string, values ...vibejson.Index) ([]store.Mask, error) {
	var workspace IndexWorkspace
	return s.AppendIndexCandidateMasksInto(dst, &workspace, name, values...)
}

// AppendIndexCandidateMasksInto is AppendIndexCandidateMasks with reusable
// transient storage.
func (s *Snapshot) AppendIndexCandidateMasksInto(dst []store.Mask, workspace *IndexWorkspace, name string, values ...vibejson.Index) ([]store.Mask, error) {
	return s.AppendIndexMasksInto(dst, workspace, name, values...)
}
