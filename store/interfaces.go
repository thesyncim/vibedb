package store

import vibejson "github.com/thesyncim/vibejson"

// IndexSource is the shape query's candidate-generation code plans against: a
// declared-index catalog plus exact/candidate mask probes. *Snapshot and
// package store/durable's snapshot type (through a small adapter — durable's
// probes carry extra I/O-workspace and stats parameters an interface method
// can't take) both satisfy it, so query's mask-based planner has one
// implementation shared by both backends instead of two.
//
// Deliberately excluded: Snapshot.AppendLiveMasks and the Segment posting-
// ordinal path. A durable snapshot cannot materialize a full live-row mask
// without cost the way a heap Snapshot can, and Segment's candidate
// representation ([]int ordinals) is a different shape in kind, not just in
// data access — callers that need either fall outside this interface by
// design rather than forcing a fourth method onto it.
type IndexSource interface {
	AppendIndexes(dst []IndexInfo) []IndexInfo
	AppendIndexMasks(dst []Mask, name string, values ...vibejson.Index) ([]Mask, error)
	AppendIndexCandidateMasks(dst []Mask, name string, values ...vibejson.Index) ([]Mask, error)
}

var _ IndexSource = Snapshot{}

// IndexRange describes one scalar interval over a single-column ordered
// index. A missing endpoint is open. Endpoint indexes borrow their source
// bytes and must remain valid until the probe returns.
type IndexRange struct {
	Lower          vibejson.Index
	Upper          vibejson.Index
	HasLower       bool
	HasUpper       bool
	LowerInclusive bool
	UpperInclusive bool
}

// RangeIndexSource is an optional ordered-index capability. The bool result is
// false when the physical range is too broad for the source's bounded probe
// policy; callers must then use their ordinary scan. Returned masks are a
// candidate superset and the complete predicate remains authoritative.
type RangeIndexSource interface {
	AppendIndexRangeCandidateMasks(
		dst []Mask,
		name string,
		span IndexRange,
	) ([]Mask, bool, error)
}

// LiveMaskSource is an optional IndexSource capability: some backends (the
// heap Snapshot) can cheaply materialize the full live-row universe as
// masks, which lets query complement a candidate set for NOT without a
// second index probe. durable does not implement it — materializing that
// universe needs real page I/O, not a metadata-only operation — so query's
// NOT handling checks for this capability with a type assertion instead of
// requiring it on IndexSource.
type LiveMaskSource interface {
	AppendLiveMasks(dst []Mask) []Mask
}

var _ LiveMaskSource = Snapshot{}
