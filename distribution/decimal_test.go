package distribution

// compareDigitsToUint decides how a JSON exponent literal combines with the
// mantissa-position adjustment, and the digit-run bounds on that adjustment
// keep its operands well inside uint64 for any spelling short enough to exist.
// The comparison is nonetheless written to stay correct at and past the 20-digit
// uint64 boundary, so a naive value-parse can never silently wrap. These cases
// exercise that boundary directly, since no reachable spelling can.

import "testing"

func TestCompareDigitsToUintBoundary(t *testing.T) {
	const maxUint64 = 18446744073709551615 // 1<<64 - 1, 20 digits

	cases := []struct {
		digits string
		v      uint64
		want   int
	}{
		{"1", 1, 0},
		{"9", 1, 1},
		{"1", 9, -1},
		{"10", 9, 1},
		{"9", 10, -1},
		{"123", 124, -1},
		{"124", 123, 1},
		// 19-digit magnitude is always below a 20-digit v.
		{"9999999999999999999", maxUint64, -1},
		// Exactly maxUint64, the widest value uintFromDigits could parse safely.
		{"18446744073709551615", maxUint64, 0},
		{"18446744073709551614", maxUint64, -1},
		{"18446744073709551616", maxUint64, 1},
		// 20 digits above maxUint64: parsing digits back into a uint64 would wrap
		// to a small value and mis-report -1; the string compare must return 1.
		{"99999999999999999999", maxUint64, 1},
		{"20000000000000000000", maxUint64, 1},
	}
	for _, c := range cases {
		if got := compareDigitsToUint(c.digits, c.v); got != c.want {
			t.Errorf("compareDigitsToUint(%q, %d) = %d, want %d", c.digits, c.v, got, c.want)
		}
	}
}
