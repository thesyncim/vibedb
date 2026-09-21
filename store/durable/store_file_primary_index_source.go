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

// AppendIndexes appends the immutable index catalog visible to the snapshot:
// the exact aliases, then the declared tin definitions. Tin postings build
// lazily per generation on first query use, so a declaration is usable
// immediately and advertises IndexReady, exactly like the heap sidecar;
// readers still observe the published declaration exactly.
func (s *Snapshot) AppendIndexes(dst []store.IndexInfo) []store.IndexInfo {
	if s == nil || s.collection == nil || s.state == nil {
		return dst
	}
	for _, definition := range s.indexDefinitions {
		if definition.Kind == store.IndexTin {
			info := store.IndexInfo{
				Name: definition.Name, Kind: store.IndexTin,
				State:       store.IndexReady,
				ColumnCount: 1,
			}
			if len(definition.Paths) == 1 {
				info.Columns[0] = definition.Paths[0]
			}
			dst = append(dst, info)
			continue
		}
		info := store.IndexInfo{
			Name: definition.Name, Kind: store.IndexExact, State: store.IndexReady,
			Unique: definition.Unique,
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

// AppendIndexMasksInto is the expert, low-level form of AppendIndexMasks with
// caller-managed reusable transient storage. Normal query execution should use
// [IndexSession], whose workspace is private.
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

// AppendIndexCandidateMasksInto is the expert, low-level form of
// AppendIndexCandidateMasks with caller-managed reusable transient storage.
// Normal query execution should use [IndexSession].
func (s *Snapshot) AppendIndexCandidateMasksInto(dst []store.Mask, workspace *IndexWorkspace, name string, values ...vibejson.Index) ([]store.Mask, error) {
	return s.AppendIndexMasksInto(dst, workspace, name, values...)
}

// AppendIndexRangeCandidateMasks probes a single-column ordered exact index.
// The returned masks are a candidate superset; callers must recheck the full
// comparison. A false bound result leaves dst unchanged and requests a scan.
func (s *Snapshot) AppendIndexRangeCandidateMasks(
	dst []store.Mask,
	name string,
	span store.IndexRange,
) ([]store.Mask, bool, error) {
	var workspace IndexWorkspace
	return s.AppendIndexRangeCandidateMasksInto(dst, &workspace, name, span)
}

// AppendIndexRangeCandidateMasksInto is the reusable-workspace form of
// AppendIndexRangeCandidateMasks.
func (s *Snapshot) AppendIndexRangeCandidateMasksInto(
	dst []store.Mask,
	workspace *IndexWorkspace,
	name string,
	span store.IndexRange,
) ([]store.Mask, bool, error) {
	if s == nil || s.collection == nil || s.state == nil {
		return dst, false, ErrClosed
	}
	if workspace == nil {
		var local IndexWorkspace
		workspace = &local
	}
	return s.appendPrimaryExactRangeMasks(dst, workspace, name, span)
}
