package distribution

import "slices"

// DestinationSet is a mapper's explicit output: zero or more first-class
// points and zero or more half-open ranges. Points are never encoded as
// [point, successor(point)), so the maximum keyspace point carries no overflow.
type DestinationSet struct {
	Points []KeyspacePoint
	Ranges []KeyRange
}

// Normalize returns a deterministic canonical form of d without mutating the
// receiver: points sorted and exact-deduplicated, ranges sorted and merged
// across overlap and adjacency, and points already covered by a range dropped.
// It reports a *DestinationError (matching ErrInvalidDestination) for a
// malformed range. The fixed 8-byte profile is inherent in the element types
// and preserved.
func (d DestinationSet) Normalize() (DestinationSet, error) {
	ranges := slices.Clone(d.Ranges)
	for i := range ranges {
		if !ranges[i].Valid() {
			return DestinationSet{}, &DestinationError{Reason: "range start is not below its end"}
		}
	}
	slices.SortFunc(ranges, func(a, b KeyRange) int {
		if c := ComparePoints(a.Start, b.Start); c != 0 {
			return c
		}
		return compareEnds(a.End, b.End)
	})

	var merged []KeyRange
	for i, r := range ranges {
		if i == 0 {
			merged = append(merged, r)
			continue
		}
		last := &merged[len(merged)-1]
		if pointAtOrBelowEnd(r.Start, last.End) {
			last.End = maxEnd(last.End, r.End)
			continue
		}
		merged = append(merged, r)
	}

	points := slices.Clone(d.Points)
	slices.SortFunc(points, ComparePoints)

	var kept []KeyspacePoint
	var prev KeyspacePoint
	havePrev := false
	ri := 0
	for _, p := range points {
		if havePrev && p == prev {
			continue // exact duplicate
		}
		prev, havePrev = p, true
		for ri < len(merged) && !pointBelowEnd(p, merged[ri].End) {
			ri++
		}
		if ri < len(merged) && ComparePoints(merged[ri].Start, p) <= 0 {
			continue // covered by a range
		}
		kept = append(kept, p)
	}

	return DestinationSet{Points: kept, Ranges: merged}, nil
}
