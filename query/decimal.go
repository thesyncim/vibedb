package query

// Exact decimal ordering over JSON number spellings.
//
// compareNumberBytes decides the sign of a - b for two validated JSON numbers
// without rounding through float64. The decomposition is:
//
//	value = ± 0.d0 d1 … dk × 10^(weight+1)
//
// where d0…dk are the significant digits with leading and trailing zeros
// stripped and weight is the decimal exponent of d0. JSON does not bound an
// exponent literal, so weight cannot in general fit any machine integer. A
// decimalWeight therefore has an int64 fast path and an exact wide form.
//
// The wide form represents:
//
//	signed exponent literal + signed mantissa-position adjustment
//
// as a small sequence of views: an unchanged prefix of the exponent, at most
// one changed digit, a run produced by carry or borrow, and a fixed-size tail.
// The adjustment comes from byte-slice positions and is at most int64-wide, so
// only its final 19 decimal positions need materializing. Carry and borrow may
// cross an arbitrarily long exponent, but those crossed digits form a run of
// zeroes or nines. This keeps comparison exact and allocation-free without
// constructing a math/big integer or a second exponent-sized buffer.

const decimalWeightTailDigits = 20

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

	weight decimalWeight
}

// decimalWeight is a signed arbitrary-precision decimal integer. compact is
// the common int64 representation. The exact wide magnitude is the canonical
// concatenation:
//
//	prefix, optional middle, fill repeated fillLen times, tail
//
// A wide zero has no magnitude digits and is normalized back to compact zero.
type decimalWeight struct {
	compact int64
	wide    bool
	neg     bool

	prefix    []byte
	middle    byte
	hasMiddle bool
	fill      byte
	fillLen   int

	tail      [decimalWeightTailDigits]byte
	tailStart uint8
	tailLen   uint8
}

// compareNumberBytes returns the sign of a - b for two validated JSON number
// spellings.
func compareNumberBytes(a, b []byte) int {
	if equalBytes(a, b) {
		return 0
	}
	da := parseDecimal(a)
	db := parseDecimal(b)
	return compareDecimals(&da, &db)
}

func equalBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
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

	var (
		exp       int64
		expDigits []byte
		expNeg    bool
		expFits   = true
	)
	if i < len(src) { // e or E
		i++
		if i < len(src) && src[i] == '+' {
			i++
		} else if i < len(src) && src[i] == '-' {
			expNeg = true
			i++
		}
		for i < len(src) && src[i] == '0' {
			i++
		}
		expDigits = src[i:]
		// Eighteen digits always fit int64, including the sign. Keeping the
		// common test branch this simple makes ordinary decimal parsing cheap;
		// the exact wide path handles every longer spelling, even when its
		// eventual adjusted weight would happen to fit int64.
		if len(expDigits) <= 18 {
			for _, c := range expDigits {
				exp = exp*10 + int64(c-'0')
			}
			if expNeg {
				exp = -exp
			}
		} else {
			expFits = false
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
		if weight, ok := checkedAddInt64(exp, adj); ok {
			d.weight.compact = weight
		} else {
			d.weight = makeWideWeight(expNeg, expDigits, adj)
		}
	} else {
		d.weight = makeWideWeight(expNeg, expDigits, adj)
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

func checkedAddInt64(a, b int64) (int64, bool) {
	const (
		maxInt64 = int64(^uint64(0) >> 1)
		minInt64 = -maxInt64 - 1
	)
	if b > 0 && a > maxInt64-b {
		return 0, false
	}
	if b < 0 && a < minInt64-b {
		return 0, false
	}
	return a + b, true
}

// makeWideWeight adds the signed adjustment to an arbitrary-length signed
// exponent magnitude. expDigits has no leading zeroes.
func makeWideWeight(expNeg bool, expDigits []byte, adjustment int64) decimalWeight {
	if len(expDigits) == 0 {
		if adjustment == 0 {
			return decimalWeight{}
		}
		w := decimalWeight{wide: true, neg: adjustment < 0}
		setWeightUint(&w, absInt64(adjustment))
		return w
	}
	if adjustment == 0 {
		return decimalWeight{wide: true, neg: expNeg, prefix: expDigits}
	}

	adjustmentNeg := adjustment < 0
	small := absInt64(adjustment)
	if expNeg == adjustmentNeg {
		w := decimalWeight{wide: true, neg: expNeg}
		addWeightUint(&w, expDigits, small)
		return w
	}

	switch compareDigitsToUint(expDigits, small) {
	case 0:
		return decimalWeight{}
	case 1:
		w := decimalWeight{wide: true, neg: expNeg}
		subtractWeightUint(&w, expDigits, small)
		return w
	default:
		w := decimalWeight{wide: true, neg: adjustmentNeg}
		setWeightUint(&w, small-uintFromDigits(expDigits))
		return w
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

func uintFromDigits(digits []byte) uint64 {
	var v uint64
	for _, c := range digits {
		v = v*10 + uint64(c-'0')
	}
	return v
}

// compareDigitsToUint compares a canonical decimal magnitude with v. Every
// path that parses digits into uint64 first proves they have at most 19
// digits; any 19-digit decimal value fits uint64.
func compareDigitsToUint(digits []byte, v uint64) int {
	n := uintDigits(v)
	if len(digits) != n {
		if len(digits) < n {
			return -1
		}
		return 1
	}
	dv := uintFromDigits(digits)
	switch {
	case dv < v:
		return -1
	case dv > v:
		return 1
	default:
		return 0
	}
}

func setWeightUint(w *decimalWeight, v uint64) {
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
// A carry through the untouched prefix becomes one changed digit and a run of
// zeroes, regardless of how long that prefix is.
func addWeightUint(w *decimalWeight, magnitude []byte, small uint64) {
	k := uintDigits(small)
	prefixEnd := len(magnitude) - k
	if prefixEnd < 0 {
		prefixEnd = 0
	}
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
func subtractWeightUint(w *decimalWeight, magnitude []byte, small uint64) {
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
func trimWeightTail(w *decimalWeight) {
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

// compareDecimals returns the sign of a - b.
func compareDecimals(a, b *decimal) int {
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
func compareMagnitude(a, b *decimal) int {
	if weight := compareDecimalWeights(&a.weight, &b.weight); weight != 0 {
		return weight
	}
	return compareSignificantDigits(a, b)
}

func compareDecimalWeights(a, b *decimalWeight) int {
	if !a.wide && !b.wide {
		switch {
		case a.compact < b.compact:
			return -1
		case a.compact > b.compact:
			return 1
		default:
			return 0
		}
	}
	var (
		aw decimalWeight
		bw decimalWeight
	)
	if !a.wide {
		aw = wideWeightFromInt64(a.compact)
		a = &aw
	}
	if !b.wide {
		bw = wideWeightFromInt64(b.compact)
		b = &bw
	}

	an := weightMagnitudeLen(a)
	bn := weightMagnitudeLen(b)
	switch {
	case an == 0 && bn == 0:
		return 0
	case an == 0:
		if b.neg {
			return 1
		}
		return -1
	case bn == 0:
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

	cmp := 0
	if an != bn {
		if an < bn {
			cmp = -1
		} else {
			cmp = 1
		}
	} else {
		for i := 0; i < an; i++ {
			ad := weightMagnitudeDigit(a, i)
			bd := weightMagnitudeDigit(b, i)
			if ad == bd {
				continue
			}
			if ad < bd {
				cmp = -1
			} else {
				cmp = 1
			}
			break
		}
	}
	if a.neg {
		return -cmp
	}
	return cmp
}

func wideWeightFromInt64(v int64) decimalWeight {
	w := decimalWeight{wide: true, neg: v < 0}
	setWeightUint(&w, absInt64(v))
	return w
}

func weightMagnitudeLen(w *decimalWeight) int {
	return len(w.prefix) + boolInt(w.hasMiddle) + w.fillLen + int(w.tailLen)
}

func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func weightMagnitudeDigit(w *decimalWeight, i int) byte {
	if i < len(w.prefix) {
		return w.prefix[i]
	}
	i -= len(w.prefix)
	if w.hasMiddle {
		if i == 0 {
			return w.middle
		}
		i--
	}
	if i < w.fillLen {
		return w.fill
	}
	i -= w.fillLen
	return w.tail[int(w.tailStart)+i]
}

// compareSignificantDigits compares two aligned significant-digit sequences
// (same weight): digit by digit, and when one is a prefix of the other the
// longer sequence is the larger magnitude, since trailing significant digits
// only add value.
func compareSignificantDigits(a, b *decimal) int {
	i := 0
	for {
		ad, aok := significantDigitAt(a, i)
		bd, bok := significantDigitAt(b, i)
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

// significantDigitAt returns the ith significant digit of d, spanning the
// integer then the fraction run, and false once the digits are exhausted.
func significantDigitAt(d *decimal, i int) (byte, bool) {
	if i < len(d.intDigits) {
		return d.intDigits[i], true
	}
	i -= len(d.intDigits)
	if i < len(d.fracDigits) {
		return d.fracDigits[i], true
	}
	return 0, false
}
