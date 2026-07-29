package storeio

const maxIntValue = int(^uint(0) >> 1)

// checkedSizeAdd adds two non-negative byte counts without wrapping and keeps
// the result within limit. Callers choose the format or address-space limit
// appropriate for the value they are about to encode or allocate.
func checkedSizeAdd(total, addition, limit uint64) (uint64, bool) {
	if total > limit || addition > limit-total {
		return 0, false
	}
	return total + addition, true
}

// checkedSizeMul multiplies two non-negative byte counts without wrapping and
// keeps the result within limit.
func checkedSizeMul(count, width, limit uint64) (uint64, bool) {
	if count != 0 && width > limit/count {
		return 0, false
	}
	return count * width, true
}

// checkedSizeInt admits a byte count only when both the serialized format and
// the current architecture can represent it.
func checkedSizeInt(size, formatLimit uint64) (int, bool) {
	limit := formatLimit
	if intLimit := uint64(maxIntValue); intLimit < limit {
		limit = intLimit
	}
	if size > limit {
		return 0, false
	}
	return int(size), true
}
