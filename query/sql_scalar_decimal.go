package query

import (
	"errors"
	"fmt"
	"math"
	"math/big"
	"strconv"
)

var (
	ErrScalarDivisionByZero = errors.New("query: scalar division by zero")
	ErrScalarNumericRange   = errors.New("query: scalar numeric range exceeded")
)

// ScalarDivisionByZeroError is the exact runtime error for / and % with a
// numeric zero divisor. Pos is the UTF-8 source byte position of the authored
// operator, retained for database/sql and pgwire SQLSTATE/position mapping.
type ScalarDivisionByZeroError struct{ Pos int }

func (e *ScalarDivisionByZeroError) Error() string {
	return fmt.Sprintf("query: division or modulo by zero at byte %d: %v", e.Pos, ErrScalarDivisionByZero)
}
func (e *ScalarDivisionByZeroError) Unwrap() error { return ErrScalarDivisionByZero }
func (e *ScalarDivisionByZeroError) Position() int { return e.Pos }

// ScalarNumericRangeError reports an exact decimal operation whose required
// coefficient/exponent workspace cannot be represented or admitted under the
// execution's exact-decimal budget. It never substitutes float64 or infinity.
type ScalarNumericRangeError struct {
	Pos       int
	Operation string
	Requested int64
	Limit     int64
}

func (e *ScalarNumericRangeError) Error() string {
	return fmt.Sprintf(
		"query: scalar %s at byte %d needs %d exact-decimal bytes with limit %d: %v",
		e.Operation, e.Pos, e.Requested, e.Limit, ErrScalarNumericRange,
	)
}
func (e *ScalarNumericRangeError) Unwrap() error { return ErrScalarNumericRange }
func (e *ScalarNumericRangeError) Position() int { return e.Pos }

// ScalarAggregateBudgetError positions an exact-decimal resource failure at
// the scalar operator that requested the storage. It deliberately unwraps the
// existing AggregateBudgetError: a caller-configured execution bound is a
// program/resource limit, not mathematical numeric overflow.
type ScalarAggregateBudgetError struct {
	Pos       int
	Operation string
	Err       error
}

func (e *ScalarAggregateBudgetError) Error() string {
	if e == nil {
		return ErrAggregateBudget.Error()
	}
	return fmt.Sprintf("query: scalar %s at byte %d: %v", e.Operation, e.Pos, e.Err)
}

func (e *ScalarAggregateBudgetError) Unwrap() error {
	if e == nil {
		return ErrAggregateBudget
	}
	return e.Err
}

func (e *ScalarAggregateBudgetError) Position() int {
	if e == nil {
		return 0
	}
	return e.Pos
}

const scalarDecimalBaseBytes = int64(768)

// sqlScalarDecimal is retained per prepared operation and execution state.
// Every big.Int receiver is reused, including quotient/remainder and power
// temporaries, so a warmed operation whose magnitude stays within its high
// water mark performs no heap allocation.
type sqlScalarDecimal struct {
	a, b       big.Int
	left       big.Int
	right      big.Int
	result     big.Int
	den        big.Int
	rem        big.Int
	aux        big.Int
	tmp        big.Int
	pow        big.Int
	exponent   big.Int
	chunk      big.Int
	digits     []byte
	parse      []byte
	chunks     []uint64
	lease      aggregateLease
	leftScale  int64
	rightScale int64
}

func (d *sqlScalarDecimal) binary(
	op sqlScalarArithmeticOp,
	left, right []byte,
	pos int,
	dst []byte,
	budget *aggregateBudget,
) ([]byte, int, error) {
	need := scalarDecimalBaseBytes + 12*int64(len(left)+len(right)+64)
	if err := d.reserve(need, pos, op.String(), budget); err != nil {
		return dst, 0, err
	}
	var err error
	d.leftScale, err = d.parseNumber(&d.a, left)
	if err != nil {
		return dst, 0, d.rangeError(pos, op.String(), math.MaxInt64, budget)
	}
	d.rightScale, err = d.parseNumber(&d.b, right)
	if err != nil {
		return dst, 0, d.rangeError(pos, op.String(), math.MaxInt64, budget)
	}

	var scale int64
	switch op {
	case sqlScalarAdd, sqlScalarSubtract:
		scale, err = d.addSub(op == sqlScalarSubtract, pos, budget)
	case sqlScalarMultiply:
		scale, err = d.multiply(pos, budget)
	case sqlScalarDivide:
		if d.b.Sign() == 0 {
			return dst, 0, &ScalarDivisionByZeroError{Pos: pos}
		}
		scale, err = d.divide(pos, budget)
	case sqlScalarModulo:
		if d.b.Sign() == 0 {
			return dst, 0, &ScalarDivisionByZeroError{Pos: pos}
		}
		scale, err = d.modulo(pos, budget)
	default:
		err = fmt.Errorf("query: invalid scalar arithmetic operation %d", op)
	}
	if err != nil {
		return dst, 0, err
	}
	start := len(dst)
	dst, err = d.appendNumber(dst, &d.result, scale, pos, op.String(), budget)
	if err != nil {
		return dst[:start], 0, err
	}
	return dst, start, nil
}

func (d *sqlScalarDecimal) parseNumber(dst *big.Int, raw []byte) (int64, error) {
	parsed := parseDecimal(raw)
	if parsed.zero {
		dst.SetInt64(0)
		return 0, nil
	}
	if parsed.weight.wide {
		return 0, ErrScalarNumericRange
	}
	d.parse = append(d.parse[:0], parsed.intDigits...)
	d.parse = append(d.parse, parsed.fracDigits...)
	if len(d.parse) == 0 {
		return 0, ErrScalarNumericRange
	}
	d.parseBigDigits(dst, d.parse)
	if parsed.neg {
		dst.Neg(dst)
	}
	return checkedScalarScale(
		parsed.weight.compact, -int64(len(d.parse)-1),
	)
}

func (d *sqlScalarDecimal) parseBigDigits(dst *big.Int, digits []byte) {
	first := true
	for len(digits) != 0 {
		width := min(len(digits), 18)
		var value, base uint64
		base = 1
		for _, digit := range digits[:width] {
			value = value*10 + uint64(digit-'0')
			base *= 10
		}
		if first {
			dst.SetUint64(value)
			first = false
		} else {
			d.chunk.SetUint64(base)
			dst.Mul(dst, &d.chunk)
			d.chunk.SetUint64(value)
			dst.Add(dst, &d.chunk)
		}
		digits = digits[width:]
	}
}

func checkedScalarScale(a, b int64) (int64, error) {
	value, ok := checkedAddInt64(a, b)
	if !ok {
		return 0, ErrScalarNumericRange
	}
	return value, nil
}

func (d *sqlScalarDecimal) addSub(
	subtract bool,
	pos int,
	budget *aggregateBudget,
) (int64, error) {
	scale := min(d.leftScale, d.rightScale)
	leftShift := d.leftScale - scale
	rightShift := d.rightScale - scale
	need, ok := scalarAlignedNeed(&d.a, &d.b, leftShift, rightShift)
	if !ok {
		return 0, d.rangeError(pos, "addition", math.MaxInt64, budget)
	}
	if err := d.reserve(need, pos, "addition", budget); err != nil {
		return 0, err
	}
	d.left.Set(&d.a)
	d.right.Set(&d.b)
	if err := d.mulPow10(&d.left, leftShift); err != nil {
		return 0, d.rangeError(pos, "addition", need, budget)
	}
	if err := d.mulPow10(&d.right, rightShift); err != nil {
		return 0, d.rangeError(pos, "addition", need, budget)
	}
	if subtract {
		d.result.Sub(&d.left, &d.right)
	} else {
		d.result.Add(&d.left, &d.right)
	}
	return scale, nil
}

func scalarAlignedNeed(a, b *big.Int, leftShift, rightShift int64) (int64, bool) {
	if leftShift < 0 || rightShift < 0 || leftShift > math.MaxInt32 || rightShift > math.MaxInt32 {
		return 0, false
	}
	digits := scalarDecimalDigitsUpper(a) + scalarDecimalDigitsUpper(b)
	if digits > math.MaxInt64-leftShift-rightShift-256 {
		return 0, false
	}
	return scalarDecimalBaseBytes + 8*(digits+leftShift+rightShift+256), true
}

// scalarDecimalDigitsUpper is an allocation-free conservative decimal width
// from a binary width. 1233/4096 is just above log10(2), so it never
// underestimates and overcharges by at most one digit.
func scalarDecimalDigitsUpper(value *big.Int) int64 {
	bits := int64(value.BitLen())
	if bits == 0 {
		return 1
	}
	return (bits*1233)/4096 + 1
}

func (d *sqlScalarDecimal) multiply(pos int, budget *aggregateBudget) (int64, error) {
	scale, err := checkedScalarScale(d.leftScale, d.rightScale)
	if err != nil {
		return 0, d.rangeError(pos, "multiplication", math.MaxInt64, budget)
	}
	need := scalarDecimalBaseBytes + 8*int64((d.a.BitLen()+d.b.BitLen())/3+256)
	if err := d.reserve(need, pos, "multiplication", budget); err != nil {
		return 0, err
	}
	d.result.Mul(&d.a, &d.b)
	return scale, nil
}

func (d *sqlScalarDecimal) modulo(pos int, budget *aggregateBudget) (int64, error) {
	scale := min(d.leftScale, d.rightScale)
	leftShift, rightShift := d.leftScale-scale, d.rightScale-scale
	need, ok := scalarAlignedNeed(&d.a, &d.b, leftShift, rightShift)
	if !ok {
		return 0, d.rangeError(pos, "modulo", math.MaxInt64, budget)
	}
	if err := d.reserve(need, pos, "modulo", budget); err != nil {
		return 0, err
	}
	d.left.Set(&d.a)
	d.right.Set(&d.b)
	if err := d.mulPow10(&d.left, leftShift); err != nil {
		return 0, d.rangeError(pos, "modulo", need, budget)
	}
	if err := d.mulPow10(&d.right, rightShift); err != nil {
		return 0, d.rangeError(pos, "modulo", need, budget)
	}
	d.result.Rem(&d.left, &d.right)
	return scale, nil
}

func (d *sqlScalarDecimal) divide(pos int, budget *aggregateBudget) (int64, error) {
	scale, err := checkedScalarScale(d.leftScale, -d.rightScale)
	if err != nil {
		return 0, d.rangeError(pos, "division", math.MaxInt64, budget)
	}
	// Zero has one canonical exact representation regardless of either
	// operand's authored exponent. More importantly, the binary-GCD reduction
	// below requires two non-zero magnitudes: subtracting zero from the
	// denominator cannot make progress. Keep this branch before reduceFraction
	// so 0/nonzero is constant-time and allocation-free.
	if d.a.Sign() == 0 {
		d.result.SetInt64(0)
		return 0, nil
	}
	sign := d.a.Sign() * d.b.Sign()
	d.left.Abs(&d.a)
	d.den.Abs(&d.b)
	d.reduceFraction()

	// Finite decimal expansion: denominator has no prime factors beside 2/5.
	d.right.Set(&d.den)
	twos, fives := int64(0), int64(0)
	for d.right.Bit(0) == 0 {
		d.right.Rsh(&d.right, 1)
		twos++
	}
	for {
		d.aux.QuoRem(&d.right, aggregateBigFive, &d.rem)
		if d.rem.Sign() != 0 {
			break
		}
		d.right.Set(&d.aux)
		fives++
	}
	if d.right.Cmp(aggregateBigOne) == 0 {
		down := max(twos, fives)
		need := scalarDecimalBaseBytes + 8*int64(d.left.BitLen()/3+int(down)+256)
		if err := d.reserve(need, pos, "division", budget); err != nil {
			return 0, err
		}
		for i := twos; i < down; i++ {
			d.left.Mul(&d.left, aggregateBigTwo)
		}
		for i := fives; i < down; i++ {
			d.left.Mul(&d.left, aggregateBigFive)
		}
		if sign < 0 {
			d.left.Neg(&d.left)
		}
		d.result.Set(&d.left)
		return checkedScalarScale(scale, -down)
	}

	// Non-terminating decimal: the engine's established exact-decimal division
	// policy is 34 significant digits, round-to-nearest ties-to-even (the same
	// contract AVG already exposes).
	numeratorDigits := d.bigDecimalDigits(&d.left)
	denominatorDigits := d.bigDecimalDigits(&d.den)
	order := numeratorDigits - denominatorDigits
	if order >= 0 {
		d.tmp.Set(&d.den)
		if err := d.mulPow10(&d.tmp, int64(order)); err != nil {
			return 0, d.rangeError(pos, "division", math.MaxInt64, budget)
		}
		if d.left.Cmp(&d.tmp) < 0 {
			order--
		}
	} else {
		d.tmp.Set(&d.left)
		if err := d.mulPow10(&d.tmp, int64(-order)); err != nil {
			return 0, d.rangeError(pos, "division", math.MaxInt64, budget)
		}
		if d.tmp.Cmp(&d.den) < 0 {
			order--
		}
	}
	shift := int64(averageDigits - 1 - order)
	need := scalarDecimalBaseBytes + 8*int64(numeratorDigits+denominatorDigits+absInt(int(shift))+256)
	if err := d.reserve(need, pos, "division", budget); err != nil {
		return 0, err
	}
	if shift >= 0 {
		d.tmp.Set(&d.left)
		if err := d.mulPow10(&d.tmp, shift); err != nil {
			return 0, d.rangeError(pos, "division", need, budget)
		}
		d.result.QuoRem(&d.tmp, &d.den, &d.rem)
		d.aux.Lsh(&d.rem, 1)
		d.roundHalfEven(&d.den)
	} else {
		d.tmp.Set(&d.den)
		if err := d.mulPow10(&d.tmp, -shift); err != nil {
			return 0, d.rangeError(pos, "division", need, budget)
		}
		d.result.QuoRem(&d.left, &d.tmp, &d.rem)
		d.aux.Lsh(&d.rem, 1)
		d.roundHalfEven(&d.tmp)
	}
	if sign < 0 {
		d.result.Neg(&d.result)
	}
	return checkedScalarScale(scale, -shift)
}

func (d *sqlScalarDecimal) reduceFraction() {
	// Stein's binary GCD uses only the retained receivers below. math/big.GCD
	// and the Euclidean Mod loop both allocate transient quotient storage on
	// every warmed division, even when all operand capacities are stable.
	d.aux.Set(&d.left)
	d.rem.Set(&d.den)
	leftZeros := d.aux.TrailingZeroBits()
	rightZeros := d.rem.TrailingZeroBits()
	common := min(leftZeros, rightZeros)
	d.aux.Rsh(&d.aux, leftZeros)
	d.rem.Rsh(&d.rem, rightZeros)
	for {
		switch d.aux.Cmp(&d.rem) {
		case 0:
			if common != 0 {
				d.aux.Lsh(&d.aux, common)
			}
			d.left.Quo(&d.left, &d.aux)
			d.den.Quo(&d.den, &d.aux)
			return
		case 1:
			d.aux.Sub(&d.aux, &d.rem)
			d.aux.Rsh(&d.aux, d.aux.TrailingZeroBits())
		default:
			d.rem.Sub(&d.rem, &d.aux)
			d.rem.Rsh(&d.rem, d.rem.TrailingZeroBits())
		}
	}
}

func (d *sqlScalarDecimal) roundHalfEven(denominator *big.Int) {
	cmp := d.aux.Cmp(denominator)
	if cmp > 0 || cmp == 0 && d.result.Bit(0) != 0 {
		d.result.Add(&d.result, aggregateBigOne)
	}
}

var scalarBigDecimalBase = big.NewInt(1_000_000_000_000_000_000)

func (d *sqlScalarDecimal) splitBigDecimal(value *big.Int) []uint64 {
	d.chunks = d.chunks[:0]
	d.tmp.Abs(value)
	for d.tmp.Sign() != 0 {
		d.tmp.QuoRem(&d.tmp, scalarBigDecimalBase, &d.rem)
		d.chunks = append(d.chunks, d.rem.Uint64())
	}
	return d.chunks
}

func (d *sqlScalarDecimal) bigDecimalDigits(value *big.Int) int {
	chunks := d.splitBigDecimal(value)
	if len(chunks) == 0 {
		return 1
	}
	top := chunks[len(chunks)-1]
	digits := 1
	for top >= 10 {
		top /= 10
		digits++
	}
	return (len(chunks)-1)*18 + digits
}

func (d *sqlScalarDecimal) mulPow10(value *big.Int, shift int64) error {
	if shift < 0 || shift > math.MaxInt32 {
		return ErrScalarNumericRange
	}
	if shift == 0 || value.Sign() == 0 {
		return nil
	}
	d.exponent.SetInt64(shift)
	d.pow.Exp(aggregateBigTen, &d.exponent, nil)
	value.Mul(value, &d.pow)
	return nil
}

func (d *sqlScalarDecimal) appendNumber(
	dst []byte,
	coefficient *big.Int,
	scale int64,
	pos int,
	operation string,
	budget *aggregateBudget,
) ([]byte, error) {
	if coefficient.Sign() == 0 {
		return append(dst, '0'), nil
	}
	d.digits = d.appendBigDecimal(d.digits[:0], coefficient)
	negative := d.digits[0] == '-'
	magnitude := d.digits
	if negative {
		magnitude = magnitude[1:]
	}
	exponent, ok := checkedAddInt64(scale, int64(len(magnitude)-1))
	if !ok {
		return dst, d.rangeError(pos, operation, math.MaxInt64, budget)
	}
	if negative {
		dst = append(dst, '-')
	}
	dst = append(dst, magnitude[0])
	if len(magnitude) > 1 {
		dst = append(dst, '.')
		dst = append(dst, magnitude[1:]...)
	}
	if exponent != 0 {
		dst = append(dst, 'e')
		dst = strconv.AppendInt(dst, exponent, 10)
	}
	return dst, nil
}

func (d *sqlScalarDecimal) appendBigDecimal(dst []byte, value *big.Int) []byte {
	negative := value.Sign() < 0
	chunks := d.splitBigDecimal(value)
	if negative {
		dst = append(dst, '-')
	}
	if len(chunks) == 0 {
		return append(dst, '0')
	}
	dst = strconv.AppendUint(dst, chunks[len(chunks)-1], 10)
	for index := len(chunks) - 2; index >= 0; index-- {
		value := chunks[index]
		start := len(dst)
		dst = append(dst, "000000000000000000"...)
		for pos := start + 17; pos >= start; pos-- {
			dst[pos] = byte(value%10) + '0'
			value /= 10
		}
	}
	return dst
}

func (d *sqlScalarDecimal) reserve(
	need int64,
	pos int,
	operation string,
	budget *aggregateBudget,
) error {
	if need < 0 {
		return d.rangeError(pos, operation, math.MaxInt64, budget)
	}
	if err := d.lease.reserve(budget, need); err != nil {
		return &ScalarAggregateBudgetError{
			Pos: pos, Operation: operation, Err: err,
		}
	}
	return nil
}

func (d *sqlScalarDecimal) rangeError(
	pos int,
	operation string,
	requested int64,
	budget *aggregateBudget,
) error {
	limit := int64(0)
	if budget != nil {
		limit = budget.limit
	}
	return &ScalarNumericRangeError{
		Pos: pos, Operation: operation, Requested: requested, Limit: limit,
	}
}

type sqlScalarArithmeticOp uint8

const (
	sqlScalarAdd sqlScalarArithmeticOp = iota
	sqlScalarSubtract
	sqlScalarMultiply
	sqlScalarDivide
	sqlScalarModulo
)

func (o sqlScalarArithmeticOp) String() string {
	switch o {
	case sqlScalarAdd:
		return "addition"
	case sqlScalarSubtract:
		return "subtraction"
	case sqlScalarMultiply:
		return "multiplication"
	case sqlScalarDivide:
		return "division"
	default:
		return "modulo"
	}
}
