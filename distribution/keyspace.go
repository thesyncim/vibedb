package distribution

// KeyspaceWidth is the fixed byte width of a Vitess-compatible keyspace
// position. The compatibility profile admits no other width.
const KeyspaceWidth = 8

// KeyspacePoint is a fixed-width keyspace position ordered as an unsigned
// big-endian integer. The zero value is the minimum position, 0; the all-ones
// value 0xFF..FF is the maximum position and is a first-class point, never
// encoded as a successor.
type KeyspacePoint [KeyspaceWidth]byte

// KeyspaceEnd is the exclusive upper bound of a range. Max represents exactly
// 2^64, the one position no KeyspacePoint can hold, and is legal only on the
// final range of a manifest.
type KeyspaceEnd struct {
	Point KeyspacePoint
	Max   bool
}

// KeyRange is a half-open interval [Start, End) over the fixed-width keyspace.
type KeyRange struct {
	Start KeyspacePoint // inclusive
	End   KeyspaceEnd   // exclusive
}

// ComparePoints orders two positions as unsigned big-endian integers,
// returning -1, 0, or 1. It is equivalent to bytes.Compare on the arrays but
// allocation-free: it never slices the array values.
func ComparePoints(a, b KeyspacePoint) int {
	for i := 0; i < KeyspaceWidth; i++ {
		if a[i] != b[i] {
			if a[i] < b[i] {
				return -1
			}
			return 1
		}
	}
	return 0
}

// Valid reports whether r is a well-formed half-open interval, Start < End. A
// Max end is always above every point, so a Max-ended range is always valid.
func (r KeyRange) Valid() bool {
	if r.End.Max {
		return true
	}
	return ComparePoints(r.Start, r.End.Point) < 0
}

// Contains reports whether p lies in [Start, End). The maximum point falls
// only inside a Max-ended range, with no successor computed.
func (r KeyRange) Contains(p KeyspacePoint) bool {
	return ComparePoints(r.Start, p) <= 0 && pointBelowEnd(p, r.End)
}

// Overlaps reports whether r and other share any point.
func (r KeyRange) Overlaps(other KeyRange) bool {
	return pointBelowEnd(r.Start, other.End) && pointBelowEnd(other.Start, r.End)
}

// pointBelowEnd reports whether p < e. A Max end is above every point.
func pointBelowEnd(p KeyspacePoint, e KeyspaceEnd) bool {
	if e.Max {
		return true
	}
	return ComparePoints(p, e.Point) < 0
}

// pointAtOrBelowEnd reports whether p <= e, treating a concrete end point as an
// inclusive boundary. It is the merge predicate that also treats adjacency
// (p == e.Point) as touching.
func pointAtOrBelowEnd(p KeyspacePoint, e KeyspaceEnd) bool {
	if e.Max {
		return true
	}
	return ComparePoints(p, e.Point) <= 0
}

// compareEnds orders two exclusive ends, with Max above every concrete point.
func compareEnds(a, b KeyspaceEnd) int {
	switch {
	case a.Max && b.Max:
		return 0
	case a.Max:
		return 1
	case b.Max:
		return -1
	default:
		return ComparePoints(a.Point, b.Point)
	}
}

// maxEnd returns the higher of two ends.
func maxEnd(a, b KeyspaceEnd) KeyspaceEnd {
	if compareEnds(a, b) >= 0 {
		return a
	}
	return b
}
