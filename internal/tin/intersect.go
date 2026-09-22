package tin

// Sorted-list intersection. intersectInto is the AND hot path: every
// boolean conjunction merges posting runs through it.
//
// A 128-bit vector merge was measured against this loop and lost ~2.2x at
// every size (the scalar merge streams branch-predictably; the vector
// setup plus per-lane extraction costs more than it saves, and archsimd
// offers no wider integer lanes). So the scalar spelling stands alone —
// no dispatch, no dead kernel. See BenchmarkIntersectScalar; re-measure
// before reviving a wide path.
func intersectInto(a, b []DocID, out []DocID) []DocID {
	return intersectScalar(a, b, out)
}

// intersectScalar intersects two sorted lists into out. out may alias a
// (all call sites intersect in place); it must not alias b.
func intersectScalar(a, b []DocID, out []DocID) []DocID {
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i] < b[j]:
			i++
		case a[i] > b[j]:
			j++
		default:
			out = append(out, a[i])
			i++
			j++
		}
	}
	return out
}
