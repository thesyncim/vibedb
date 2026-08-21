package query

// CompareValidatedJSONNumbers returns the sign of a - b for two strict JSON
// number spellings. It is exact for arbitrary-length mantissas and exponents,
// performs no allocation, and never rounds through float64. Callers must first
// validate externally sourced bytes as JSON numbers.
func CompareValidatedJSONNumbers(a, b []byte) int {
	return compareNumberBytes(a, b)
}
