package store

type packedFloat64Summary struct {
	count int
	sum   float64
	min   float64
	max   float64
}

// addPackedFloat64LE merges one cache-admitted dense little-endian value run.
// Typed page admission has already rejected NaN and infinity. The reduction
// order is fixed by reducePackedFloat64LEReference and shared by portable and
// SIMD builds, so acceleration cannot change result bits.
func (a *Float64Aggregate) addPackedFloat64LE(values []byte) bool {
	if a == nil || len(values)&7 != 0 {
		return false
	}
	summary := reducePackedFloat64LE(values)
	if summary.count == 0 {
		return true
	}
	if a.Count == 0 {
		a.Min, a.Max = summary.min, summary.max
	} else {
		if summary.min < a.Min {
			a.Min = summary.min
		}
		if summary.max > a.Max {
			a.Max = summary.max
		}
	}
	a.Count += summary.count
	a.Sum += summary.sum
	return true
}
