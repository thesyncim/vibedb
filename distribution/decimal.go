package distribution

// Exact decimal decomposition of a validated JSON number spelling, frozen and
// kept independent of query/decimal.go by design: this package must never
// let local query/index evolution silently reshape placement bytes. Only the
// pieces this codec needs are kept — canonicalization for encoding — not the
// three-way comparison query/decimal.go also provides.
//
// Every well-formed spelling reduces to:
//
//	value = ± 0.d0 d1 … dk × 10^(weight+1)
//
// where d0…dk are the significant digits with leading and trailing zeros
// stripped and weight is the decimal exponent of d0, adjusted for the
// mantissa position. A JSON exponent literal is unbounded, so weight is
// tracked as a signed arbitrary-precision decimal integer built from small
// fixed-size views over the source spelling: an unchanged prefix, at most one
// changed digit, a carry/borrow fill run, and a fixed-size tail. This keeps
// canonicalization exact and allocation-free even for huge valid exponents,
// without constructing a big integer or an exponent-sized buffer.

const weightTailDigits = 20

// numberDecimal is the exact decomposition of one validated JSON number
// spelling. intDigits and fracDigits alias the source spelling; nothing is
// copied.
type numberDecimal struct {
	neg  bool // significand sign of a nonzero value
	zero bool // every digit is zero; sign and weight are then irrelevant

	intDigits  string
	fracDigits string

	weight numberWeight
}

// numberWeight is a signed arbitrary-precision decimal integer: the adjusted
// decimal exponent. Its magnitude is the canonical concatenation
// prefix + optional middle + fill repeated fillLen times + tail. A weight of
// exactly zero carries no magnitude digits.
type numberWeight struct {
	neg bool

	prefix    string
	middle    byte
	hasMiddle bool
	fill      byte
	fillLen   int

	tail      [weightTailDigits]byte
	tailStart uint8
	tailLen   uint8
}

// isASCIIDigit reports whether c is '0'..'9'. The single unsigned comparison
// folds the two range bounds into one branch, which matters on the digit-scan
// loops that walk every character of a long spelling.
func isASCIIDigit(c byte) bool {
	return c-'0' <= 9
}

// parseNumberSpelling validates that spelling is exactly one well-formed JSON
// number literal and fills d with its canonical decomposition. d must be the
// zero value on entry; on error d is left partially written and the caller
// must ignore it.
func parseNumberSpelling(spelling string, d *numberDecimal) error {
	s := spelling
	n := len(s)
	i := 0

	if i < n && s[i] == '-' {
		d.neg = true
		i++
	}

	intStart := i
	if i >= n || !isASCIIDigit(s[i]) {
		return &InvalidNumberError{Spelling: spelling}
	}
	if s[i] == '0' {
		i++
	} else {
		for i < n && isASCIIDigit(s[i]) {
			i++
		}
	}
	intEnd := i

	fracStart, fracEnd := 0, 0
	if i < n && s[i] == '.' {
		i++
		fracStart = i
		for i < n && isASCIIDigit(s[i]) {
			i++
		}
		fracEnd = i
		if fracEnd == fracStart {
			return &InvalidNumberError{Spelling: spelling}
		}
	}

	var (
		expDigits string
		expNeg    bool
	)
	if i < n && (s[i] == 'e' || s[i] == 'E') {
		i++
		if i < n && s[i] == '+' {
			i++
		} else if i < n && s[i] == '-' {
			expNeg = true
			i++
		}
		digitsStart := i
		for i < n && isASCIIDigit(s[i]) {
			i++
		}
		if i == digitsStart {
			return &InvalidNumberError{Spelling: spelling}
		}
		raw := s[digitsStart:i]
		for len(raw) > 0 && raw[0] == '0' {
			raw = raw[1:]
		}
		expDigits = raw
	}

	if i != n {
		return &InvalidNumberError{Spelling: spelling}
	}

	// The leading significant digit fixes the weight adjustment, and the
	// last nonzero digit bounds the significant sequence. JSON forbids
	// leading zeros, so the integer part is either the single digit 0 or
	// opens with a nonzero digit; a nonzero integer part therefore holds the
	// leading significant digit, otherwise it is the first nonzero fraction
	// digit.
	var adj int64
	intSignificant := intEnd > intStart && s[intStart] != '0'
	if intSignificant {
		adj = int64(intEnd-intStart) - 1
	} else {
		lead := -1
		for j := fracStart; j < fracEnd; j++ {
			if s[j] != '0' {
				lead = j
				break
			}
		}
		if lead < 0 {
			d.zero = true
			return nil
		}
		adj = -int64(lead-fracStart) - 1
		fracStart = lead
	}

	makeWeight(&d.weight, expNeg, expDigits, adj)

	// Trailing zeros are non-significant wherever they fall — the exponent,
	// not the mantissa, carries a value's scale — so 10e-1, 1.0, and 1 share
	// the digit sequence "1". Zeros between the leading and trailing
	// significant digits stay; only the trailing run is dropped.
	fracLast := fracEnd
	for fracLast > fracStart && s[fracLast-1] == '0' {
		fracLast--
	}
	if fracLast > fracStart {
		if intSignificant {
			d.intDigits = s[intStart:intEnd]
		}
		d.fracDigits = s[fracStart:fracLast]
	} else {
		intLast := intEnd
		for intLast > intStart && s[intLast-1] == '0' {
			intLast--
		}
		d.intDigits = s[intStart:intLast]
	}
	return nil
}

// makeWeight sets w to the signed exponent literal magnitude (expDigits,
// already stripped of leading zeroes) plus the signed mantissa-position
// adjustment. w must be the zero value on entry; a zero result leaves it
// untouched. Filling through the pointer keeps the fixed-size tail array off
// the return path, so no per-call copy of it happens. adjustment always fits
// int64: it is bounded by the digit-run lengths of a single spelling string.
func makeWeight(w *numberWeight, expNeg bool, expDigits string, adjustment int64) {
	if len(expDigits) == 0 {
		if adjustment == 0 {
			return
		}
		w.neg = adjustment < 0
		setWeightUint(w, absInt64(adjustment))
		return
	}
	if adjustment == 0 {
		w.neg = expNeg
		w.prefix = expDigits
		return
	}

	adjustmentNeg := adjustment < 0
	small := absInt64(adjustment)
	if expNeg == adjustmentNeg {
		w.neg = expNeg
		addWeightUint(w, expDigits, small)
		return
	}

	switch compareDigitsToUint(expDigits, small) {
	case 0:
		// Exponent literal and adjustment cancel exactly; w stays zero.
	case 1:
		w.neg = expNeg
		subtractWeightUint(w, expDigits, small)
	default:
		// expDigits < small, so the parse fits uint64 and the difference is positive.
		w.neg = adjustmentNeg
		setWeightUint(w, small-uintFromDigits(expDigits))
	}
}

func absInt64(v int64) uint64 {
	if v >= 0 {
		return uint64(v)
	}
	// -(MinInt64) is not representable as int64.
	return uint64(-(v + 1)) + 1
}

func uintDigits(v uint64) int {
	n := 1
	for v >= 10 {
		v /= 10
		n++
	}
	return n
}

func uintFromDigits(digits string) uint64 {
	var v uint64
	for i := 0; i < len(digits); i++ {
		v = v*10 + uint64(digits[i]-'0')
	}
	return v
}

// compareDigitsToUint compares a canonical (no leading zero) decimal magnitude
// against v. It renders v to digits and compares digit strings rather than
// parsing digits back into a uint64, so a magnitude of 20 or more digits — at
// the edge of or beyond the uint64 range — compares correctly instead of
// silently overflowing. (The digit-run bound on adjustment keeps that edge
// unreachable in practice, but the string compare is correct regardless and
// no more costly.)
func compareDigitsToUint(digits string, v uint64) int {
	var buf [20]byte // uint64 tops out at 20 decimal digits
	i := len(buf)
	for {
		i--
		buf[i] = byte(v%10) + '0'
		v /= 10
		if v == 0 {
			break
		}
	}
	vDigits := buf[i:]
	switch {
	case len(digits) < len(vDigits):
		return -1
	case len(digits) > len(vDigits):
		return 1
	}
	// Equal digit counts, neither with a leading zero: lexical order is
	// numeric order.
	for k := range len(digits) {
		if digits[k] != vDigits[k] {
			if digits[k] < vDigits[k] {
				return -1
			}
			return 1
		}
	}
	return 0
}

func setWeightUint(w *numberWeight, v uint64) {
	if v == 0 {
		return
	}
	i := len(w.tail)
	for v != 0 {
		i--
		w.tail[i] = byte(v%10) + '0'
		v /= 10
	}
	w.tailStart = uint8(i)
	w.tailLen = uint8(len(w.tail) - i)
}

// addWeightUint materializes only the exponent positions aligned with small.
// A carry through the untouched prefix becomes one changed digit and a run
// of zeroes, regardless of how long that prefix is.
func addWeightUint(w *numberWeight, magnitude string, small uint64) {
	k := uintDigits(small)
	prefixEnd := max(len(magnitude)-k, 0)
	tailStart := len(w.tail) - k
	rest := small
	carry := 0
	for pos := k - 1; pos >= 0; pos-- {
		sourcePos := len(magnitude) - k + pos
		sourceDigit := 0
		if sourcePos >= 0 {
			sourceDigit = int(magnitude[sourcePos] - '0')
		}
		sum := sourceDigit + int(rest%10) + carry
		rest /= 10
		w.tail[tailStart+pos] = byte(sum%10) + '0'
		carry = sum / 10
	}
	w.tailStart = uint8(tailStart)
	w.tailLen = uint8(k)
	if carry == 0 {
		w.prefix = magnitude[:prefixEnd]
		return
	}

	p := prefixEnd - 1
	for p >= 0 && magnitude[p] == '9' {
		p--
	}
	if p >= 0 {
		w.prefix = magnitude[:p]
		w.middle = magnitude[p] + 1
		w.hasMiddle = true
		w.fillLen = prefixEnd - p - 1
	} else {
		w.middle = '1'
		w.hasMiddle = true
		w.fillLen = prefixEnd
	}
	w.fill = '0'
}

// subtractWeightUint requires magnitude > small. A borrow through the
// untouched prefix becomes one changed digit and a run of nines.
func subtractWeightUint(w *numberWeight, magnitude string, small uint64) {
	k := uintDigits(small)
	prefixEnd := len(magnitude) - k
	tailStart := len(w.tail) - k
	rest := small
	borrow := 0
	for pos := k - 1; pos >= 0; pos-- {
		sourceDigit := int(magnitude[prefixEnd+pos] - '0')
		diff := sourceDigit - int(rest%10) - borrow
		rest /= 10
		if diff < 0 {
			diff += 10
			borrow = 1
		} else {
			borrow = 0
		}
		w.tail[tailStart+pos] = byte(diff) + '0'
	}
	w.tailStart = uint8(tailStart)
	w.tailLen = uint8(k)
	if borrow == 0 {
		w.prefix = magnitude[:prefixEnd]
		trimWeightTail(w)
		return
	}

	p := prefixEnd - 1
	for p >= 0 && magnitude[p] == '0' {
		p--
	}
	// magnitude starts nonzero and is larger than small, so a lender exists.
	w.prefix = magnitude[:p]
	w.middle = magnitude[p] - 1
	if p > 0 || w.middle != '0' {
		w.hasMiddle = true
	}
	w.fill = '9'
	w.fillLen = prefixEnd - p - 1
	trimWeightTail(w)
}

// trimWeightTail removes leading zeroes only when no earlier segment makes
// those zeroes positional.
func trimWeightTail(w *numberWeight) {
	if len(w.prefix) != 0 || w.hasMiddle || w.fillLen != 0 {
		return
	}
	start := int(w.tailStart)
	end := start + int(w.tailLen)
	for start < end && w.tail[start] == '0' {
		start++
	}
	w.tailStart = uint8(start)
	w.tailLen = uint8(end - start)
}

// weightMagnitudeLen reports the canonical digit-count of w's magnitude; a
// weight of exactly zero reports 0.
func weightMagnitudeLen(w *numberWeight) int {
	return len(w.prefix) + boolInt(w.hasMiddle) + w.fillLen + int(w.tailLen)
}

func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

// appendWeightMagnitude appends w's canonical magnitude digit sequence
// directly from its source view and fixed tail; no exponent-sized
// intermediate is allocated.
func appendWeightMagnitude(dst []byte, w *numberWeight) []byte {
	dst = append(dst, w.prefix...)
	if w.hasMiddle {
		dst = append(dst, w.middle)
	}
	for i := 0; i < w.fillLen; i++ {
		dst = append(dst, w.fill)
	}
	start := int(w.tailStart)
	return append(dst, w.tail[start:start+int(w.tailLen)]...)
}
