package query

import (
	"math/big"
	"strings"
	"testing"
)

func TestWideDecimalAdjustedExponentRegressions(t *testing.T) {
	nines := strings.Repeat("9", 96)
	power := "1" + strings.Repeat("0", 96)
	almostPower := strings.Repeat("9", 96)

	tests := []struct {
		name string
		a    string
		b    string
		want int
	}{
		{
			name: "mantissa adjustment changes huge positive weight",
			a:    "1e" + power,
			b:    "10e" + power,
			want: -1,
		},
		{
			name: "positive carry across entire exponent",
			a:    "10e" + nines,
			b:    "1e1" + strings.Repeat("0", 96),
			want: 0,
		},
		{
			name: "positive borrow across entire exponent",
			a:    "0.1e" + power,
			b:    "1e" + almostPower,
			want: 0,
		},
		{
			name: "negative exponent plus positive adjustment",
			a:    "10e-" + power,
			b:    "1e-" + almostPower,
			want: 0,
		},
		{
			name: "negative exponent plus negative adjustment",
			a:    "0.1e-" + almostPower,
			b:    "1e-" + power,
			want: 0,
		},
		{
			name: "int64 positive boundary",
			a:    "0.1e9223372036854775808",
			b:    "1e9223372036854775807",
			want: 0,
		},
		{
			name: "int64 negative boundary",
			a:    "10e-9223372036854775808",
			b:    "1e-9223372036854775807",
			want: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := sign(compareNumberBytes([]byte(tt.a), []byte(tt.b)))
			if got != tt.want {
				t.Fatalf("compareNumberBytes(%q, %q) = %d, want %d", tt.a, tt.b, got, tt.want)
			}
			ka := appendNumberKey(nil, []byte(tt.a))
			kb := appendNumberKey(nil, []byte(tt.b))
			if equalBytes(ka, kb) != (tt.want == 0) {
				t.Fatalf("group-key equality = %v, value equality = %v", equalBytes(ka, kb), tt.want == 0)
			}
		})
	}
}

// TestWideDecimalOrderingMatchesBigInt cross-checks the production
// allocation-free exponent representation against an intentionally separate
// math/big test oracle. The oracle stores the exponent and adjustment as a
// big.Int but never expands 10^exponent.
func TestWideDecimalOrderingMatchesBigInt(t *testing.T) {
	exponents := []string{
		"9223372036854775807",
		"9223372036854775808",
		"9999999999999999999",
		"10000000000000000000",
		strings.Repeat("9", 40),
		"1" + strings.Repeat("0", 40),
		strings.Repeat("9", 257),
		"1" + strings.Repeat("0", 257),
	}
	mantissas := []string{
		"1",
		"10",
		"1000",
		"0.1",
		"0.001",
		"1.00000000000000000001",
		"123.4500",
		"-7.500",
		"1" + strings.Repeat("0", 37),
		"0." + strings.Repeat("0", 36) + "1",
	}

	spellings := []string{"0", "-0e" + exponents[len(exponents)-1]}
	for _, exponent := range exponents {
		for _, mantissa := range mantissas {
			spellings = append(spellings,
				mantissa+"e"+exponent,
				mantissa+"e-"+exponent,
			)
		}
	}

	reference := make([]referenceDecimal, len(spellings))
	for i, spelling := range spellings {
		reference[i] = parseReferenceDecimal(t, spelling)
	}
	for i, a := range spellings {
		for j, b := range spellings {
			want := compareReferenceDecimals(reference[i], reference[j])
			got := sign(compareNumberBytes([]byte(a), []byte(b)))
			if got != want {
				t.Fatalf("compareNumberBytes(%q, %q) = %d, math/big oracle %d", a, b, got, want)
			}

			ka := appendNumberKey(nil, []byte(a))
			kb := appendNumberKey(nil, []byte(b))
			if equalBytes(ka, kb) != (want == 0) {
				t.Fatalf(
					"group key equality for (%q, %q) = %v, oracle equality = %v",
					a, b, equalBytes(ka, kb), want == 0,
				)
			}
		}
	}
}

func TestWideWeightAdjustmentMatchesBigInt(t *testing.T) {
	const (
		maxInt64 = int64(^uint64(0) >> 1)
		minInt64 = -maxInt64 - 1
	)
	tests := []struct {
		expNeg     bool
		exp        string
		adjustment int64
	}{
		{exp: "9223372036854775808", adjustment: minInt64},
		{expNeg: true, exp: "9223372036854775808", adjustment: maxInt64},
		{exp: "1", adjustment: minInt64},
		{expNeg: true, exp: "1", adjustment: maxInt64},
		{exp: strings.Repeat("9", 128), adjustment: maxInt64},
		{exp: "1" + strings.Repeat("0", 128), adjustment: minInt64},
		{expNeg: true, exp: strings.Repeat("9", 128), adjustment: minInt64},
		{expNeg: true, exp: "1" + strings.Repeat("0", 128), adjustment: maxInt64},
	}
	for _, tt := range tests {
		base, ok := new(big.Int).SetString(tt.exp, 10)
		if !ok {
			t.Fatalf("invalid test exponent %q", tt.exp)
		}
		if tt.expNeg {
			base.Neg(base)
		}
		want := new(big.Int).Add(base, big.NewInt(tt.adjustment))
		got := bigIntFromDecimalWeight(t, makeWideWeight(tt.expNeg, []byte(tt.exp), tt.adjustment))
		if got.Cmp(want) != 0 {
			t.Fatalf("weight(%v%s, %+d) = %s, want %s", map[bool]string{true: "-"}[tt.expNeg], tt.exp, tt.adjustment, got, want)
		}
	}
}

var wideDecimalBenchmarkSink int

func TestWideDecimalHotPathsAllocateZero(t *testing.T) {
	exponent := []byte("1e" + strings.Repeat("9", 256))
	adjusted := []byte("10e" + strings.Repeat("9", 256))

	if got := testing.AllocsPerRun(1_000, func() {
		wideDecimalBenchmarkSink = compareNumberBytes(exponent, adjusted)
	}); got != 0 {
		t.Fatalf("wide compare allocated %.2f times per call", got)
	}

	key := make([]byte, 0, len(adjusted)+32)
	if got := testing.AllocsPerRun(1_000, func() {
		key = appendNumberKey(key[:0], adjusted)
		wideDecimalBenchmarkSink = len(key)
	}); got != 0 {
		t.Fatalf("wide group-key encoding allocated %.2f times per call", got)
	}
}

func BenchmarkWideDecimalCompare(b *testing.B) {
	exponent := []byte("1e" + strings.Repeat("9", 256))
	adjusted := []byte("10e" + strings.Repeat("9", 256))
	b.ReportAllocs()
	b.SetBytes(int64(len(exponent) + len(adjusted)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wideDecimalBenchmarkSink = compareNumberBytes(exponent, adjusted)
	}
}

func BenchmarkWideDecimalGroupKey(b *testing.B) {
	number := []byte("10e" + strings.Repeat("9", 256))
	key := make([]byte, 0, len(number)+32)
	b.ReportAllocs()
	b.SetBytes(int64(len(number)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key = appendNumberKey(key[:0], number)
	}
	wideDecimalBenchmarkSink = len(key)
}

type referenceDecimal struct {
	neg    bool
	zero   bool
	digits string
	weight big.Int
}

func parseReferenceDecimal(t *testing.T, spelling string) referenceDecimal {
	t.Helper()
	var out referenceDecimal
	i := 0
	if spelling[i] == '-' {
		out.neg = true
		i++
	}
	intStart := i
	for i < len(spelling) && spelling[i] >= '0' && spelling[i] <= '9' {
		i++
	}
	integer := spelling[intStart:i]
	fraction := ""
	if i < len(spelling) && spelling[i] == '.' {
		i++
		start := i
		for i < len(spelling) && spelling[i] >= '0' && spelling[i] <= '9' {
			i++
		}
		fraction = spelling[start:i]
	}

	exponent := new(big.Int)
	if i < len(spelling) {
		i++ // e or E
		exponentNeg := false
		if i < len(spelling) && (spelling[i] == '+' || spelling[i] == '-') {
			exponentNeg = spelling[i] == '-'
			i++
		}
		if _, ok := exponent.SetString(spelling[i:], 10); !ok {
			t.Fatalf("math/big rejected exponent in %q", spelling)
		}
		if exponentNeg {
			exponent.Neg(exponent)
		}
	}

	mantissa := integer + fraction
	lead := 0
	for lead < len(mantissa) && mantissa[lead] == '0' {
		lead++
	}
	if lead == len(mantissa) {
		out.zero = true
		return out
	}
	last := len(mantissa)
	for mantissa[last-1] == '0' {
		last--
	}
	out.digits = mantissa[lead:last]
	adjustment := int64(len(integer) - lead - 1)
	out.weight.Add(exponent, big.NewInt(adjustment))
	return out
}

func compareReferenceDecimals(a, b referenceDecimal) int {
	switch {
	case a.zero && b.zero:
		return 0
	case a.zero:
		if b.neg {
			return 1
		}
		return -1
	case b.zero:
		if a.neg {
			return -1
		}
		return 1
	}
	if a.neg != b.neg {
		if a.neg {
			return -1
		}
		return 1
	}
	cmp := a.weight.Cmp(&b.weight)
	if cmp == 0 {
		cmp = compareReferenceDigits(a.digits, b.digits)
	}
	if a.neg {
		return -cmp
	}
	return cmp
}

func compareReferenceDigits(a, b string) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if a[i] == b[i] {
			continue
		}
		if a[i] < b[i] {
			return -1
		}
		return 1
	}
	switch {
	case len(a) < len(b):
		return -1
	case len(a) > len(b):
		return 1
	default:
		return 0
	}
}

func bigIntFromDecimalWeight(t *testing.T, w decimalWeight) *big.Int {
	t.Helper()
	if !w.wide {
		return big.NewInt(w.compact)
	}
	n := weightMagnitudeLen(&w)
	if n == 0 {
		return new(big.Int)
	}
	digits := make([]byte, n)
	for i := range digits {
		digits[i] = weightMagnitudeDigit(&w, i)
	}
	out, ok := new(big.Int).SetString(string(digits), 10)
	if !ok {
		t.Fatalf("invalid production weight digits %q", digits)
	}
	if w.neg {
		out.Neg(out)
	}
	return out
}
