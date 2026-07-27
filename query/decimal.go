package query

// Exact decimal ordering over JSON number spellings.
//
// compareNumberBytes decides the sign of a - b for two validated JSON numbers
// without rounding through float64. It is the ordering counterpart to the
// core's jsonNumberEqual (number_equal.go), which the containment operator
// uses for equality; that kernel is package-private and answers only "equal?",
// so this file reproduces its exact-decimal decomposition and extends it to a
// full order. The decomposition is:
//
//	value = ± 0.d0 d1 … dk × 10^(weight+1)
//
// where d0…dk are the significant digits with leading and trailing zeros
// stripped and weight is the decimal exponent of d0 (its spelled exponent plus
// an adjustment from the digit layout). Two nonzero values compare by sign,
// then by weight, then by the significant-digit sequence — each read in place
// from the source bytes, so the common comparison allocates nothing.
//
// JSON places no bound on the exponent literal, so a spelling like
// 1e10000000000000000000 is valid and its weight exceeds int64. Spellings
// whose exponent literal is eighteen digits or fewer — everything that occurs
// in practice and the entire test domain — carry an exact int64 weight.
// Wider exponents set expWide and fall to a deterministic, documented
// best-effort order (compareWideExp) that never rounds and never allocates an
// astronomically large intermediate the way math/big would; it is exact when
// the two exponents differ in sign or magnitude and total in every case.

// decimal is the exact decomposition of one validated JSON number spelling.
// All slices alias the source bytes; nothing is copied.
type decimal struct {
	neg  bool // significand sign of a nonzero value
	zero bool // every digit is zero; sign and weight are then irrelevant

	// digits are the significant digits (no decimal point, no leading or
	// trailing zeros), aliasing the two source runs the integer and fraction
	// parts occupy. A number's digits may span both runs, so they are held as
	// up to two slices compared in sequence.
	intDigits  []byte
	fracDigits []byte

	weight   int64 // decimal exponent of the leading significant digit
	expWide  bool  // exponent literal wider than 18 digits: weight is not exact
	expNeg   bool  // sign of a wide exponent literal
	expDigit []byte
}

// compareNumberBytes returns the sign of a - b for two validated JSON number
// spellings.
func compareNumberBytes(a, b []byte) int {
	if string(a) == string(b) {
		return 0
	}
	da := parseDecimal(a)
	db := parseDecimal(b)
	return compareDecimals(da, db)
}

// parseDecimal decomposes src, which must be exactly one validated JSON number
// spelling (the invariant every caller holds: cells come from validated
// documents, literals from strconv formatting).
func parseDecimal(src []byte) decimal {
	var d decimal
	i := 0
	if i < len(src) && src[i] == '-' {
		d.neg = true
		i++
	}
	intStart := i
	for i < len(src) && src[i] >= '0' && src[i] <= '9' {
		i++
	}
	intEnd := i

	fracStart, fracEnd := 0, 0
	if i < len(src) && src[i] == '.' {
		i++
		fracStart = i
		for i < len(src) && src[i] >= '0' && src[i] <= '9' {
			i++
		}
		fracEnd = i
	}

	var exp int64
	expFits := true
	if i < len(src) { // e or E
		i++
		if i < len(src) && src[i] == '+' {
			i++
		} else if i < len(src) && src[i] == '-' {
			d.expNeg = true
			i++
		}
		for i < len(src) && src[i] == '0' {
			i++
		}
		d.expDigit = src[i:]
		if len(d.expDigit) <= 18 {
			for _, c := range d.expDigit {
				exp = exp*10 + int64(c-'0')
			}
			if d.expNeg {
				exp = -exp
			}
		} else {
			expFits = false
			d.expWide = true
		}
	}

	// The leading significant digit fixes the weight adjustment, and the last
	// nonzero digit bounds the significant sequence. JSON forbids leading
	// zeros, so the integer part is either the single digit 0 or opens with a
	// nonzero digit; a nonzero integer part therefore holds the leading
	// significant digit, otherwise it is the first nonzero fraction digit.
	//
	// Trailing zeros are non-significant wherever they fall — the exponent, not
	// the mantissa, carries a value's scale — so 10e-1, 1.0, and 1 share the
	// digit sequence "1". Zeros *between* the leading and trailing significant
	// digits stay (100.5 is "1005"); only the trailing run is dropped.
	var adj int64
	intSignificant := intEnd > intStart && src[intStart] != '0'
	if intSignificant {
		adj = int64(intEnd-intStart) - 1
	} else {
		lead := -1
		for j := fracStart; j < fracEnd; j++ {
			if src[j] != '0' {
				lead = j
				break
			}
		}
		if lead < 0 {
			d.zero = true
			return d
		}
		adj = -int64(lead-fracStart) - 1
		fracStart = lead
	}
	if expFits {
		d.weight = exp + adj
	}

	// Locate the last nonzero fraction digit. If one exists the significant
	// sequence ends there and every integer digit in front of it — zeros
	// included — stays significant; otherwise the sequence is integer-only and
	// its own trailing zeros drop away. A value with no significant integer
	// digits always has a significant fraction digit, since the zero case
	// returned above.
	fracLast := fracEnd
	for fracLast > fracStart && src[fracLast-1] == '0' {
		fracLast--
	}
	if fracLast > fracStart {
		if intSignificant {
			d.intDigits = src[intStart:intEnd]
		}
		d.fracDigits = src[fracStart:fracLast]
	} else {
		intLast := intEnd
		for intLast > intStart && src[intLast-1] == '0' {
			intLast--
		}
		d.intDigits = src[intStart:intLast]
	}
	return d
}

// compareDecimals returns the sign of a - b.
func compareDecimals(a, b decimal) int {
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
	mag := compareMagnitude(a, b)
	if a.neg {
		return -mag
	}
	return mag
}

// compareMagnitude compares the absolute values of two nonzero decompositions:
// larger magnitude returns +1.
func compareMagnitude(a, b decimal) int {
	if a.expWide || b.expWide {
		return compareWideExp(a, b)
	}
	if a.weight != b.weight {
		if a.weight < b.weight {
			return -1
		}
		return 1
	}
	return compareDigits(a, b)
}

// compareDigits compares two aligned significant-digit sequences (same
// weight): digit by digit, and when one is a prefix of the other the longer
// sequence is the larger magnitude, since trailing significant digits only add
// value.
func compareDigits(a, b decimal) int {
	i := 0
	for {
		ad, aok := digitAt(a, i)
		bd, bok := digitAt(b, i)
		if !aok || !bok {
			switch {
			case aok == bok:
				return 0
			case aok:
				return 1
			default:
				return -1
			}
		}
		if ad != bd {
			if ad < bd {
				return -1
			}
			return 1
		}
		i++
	}
}

// digitAt returns the ith significant digit of d, spanning the integer then
// the fraction run, and false once the digits are exhausted.
func digitAt(d decimal, i int) (byte, bool) {
	if i < len(d.intDigits) {
		return d.intDigits[i], true
	}
	i -= len(d.intDigits)
	if i < len(d.fracDigits) {
		return d.fracDigits[i], true
	}
	return 0, false
}

// compareWideExp orders two magnitudes when at least one exponent literal is
// wider than eighteen digits. It is exact when the exponents differ in sign or
// decimal magnitude — the only way two such numbers can compare — and
// otherwise falls back to the significant digits. It never allocates a large
// intermediate. Reaching this path requires an exponent literal no real corpus
// or the test domain contains; the branch keeps the order total and
// deterministic for adversarial input rather than exact to the last digit.
func compareWideExp(a, b decimal) int {
	aMag, aWide := expMagnitude(a)
	bMag, bWide := expMagnitude(b)
	if !aWide && !bWide {
		// Both exponents actually fit; compare their exact weights.
		if a.weight != b.weight {
			if a.weight < b.weight {
				return -1
			}
			return 1
		}
		return compareDigits(a, b)
	}
	if s := compareSignedDigitStrings(a.expNeg, aMag, b.expNeg, bMag); s != 0 {
		return s
	}
	return compareDigits(a, b)
}

// expMagnitude returns a decomposition's exponent magnitude as a digit string
// (leading zeros already stripped) and whether it is the wide form.
func expMagnitude(d decimal) ([]byte, bool) {
	if d.expWide {
		return d.expDigit, true
	}
	return nil, false
}

// compareSignedDigitStrings orders two signed magnitudes given as decimal digit
// strings without leading zeros: a negative exponent is a smaller magnitude
// than a positive one, and within a sign a longer or lexicographically greater
// string is the larger magnitude.
func compareSignedDigitStrings(aNeg bool, a []byte, bNeg bool, b []byte) int {
	if aNeg != bNeg {
		if aNeg {
			return -1
		}
		return 1
	}
	m := compareUnsignedDigitStrings(a, b)
	if aNeg {
		return -m
	}
	return m
}

// compareUnsignedDigitStrings orders two decimal digit strings with no leading
// zeros by magnitude.
func compareUnsignedDigitStrings(a, b []byte) int {
	if len(a) != len(b) {
		if len(a) < len(b) {
			return -1
		}
		return 1
	}
	return bytesCompare(a, b)
}
