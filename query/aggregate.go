package query

import (
	"errors"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"sync/atomic"

	"github.com/thesyncim/vibejson/x/byteview"
)

const (
	defaultAggregateBytes = int64(16 << 20)
	averageDigits         = 34
	aggregateAccBaseBytes = int64(512)
	maxFixedAggregateJSON = int64(4096)
)

var (
	aggregateBigTen  = big.NewInt(10)
	aggregateBigTwo  = big.NewInt(2)
	aggregateBigFive = big.NewInt(5)
	aggregateBigOne  = big.NewInt(1)
)

func normalizeAggregateBytes(opts ExecOptions) (int64, error) {
	n := opts.AggregateBytes
	if n == 0 {
		n = defaultAggregateBytes
	}
	if n < aggregateAccBaseBytes {
		return 0, fmt.Errorf(
			"query: AggregateBytes must be at least %d bytes", aggregateAccBaseBytes,
		)
	}
	return n, nil
}

// ErrAggregateBudget is matched by [AggregateBudgetError]. Exact decimal
// reduction returns this error instead of allocating a coefficient whose
// retained representation would exceed ExecOptions.AggregateBytes.
var ErrAggregateBudget = errors.New("query: exact aggregate budget exceeded")

// AggregateBudgetError reports a bounded exact-decimal reduction that could
// not reserve enough of the caller's whole-execution aggregate budget.
type AggregateBudgetError struct {
	Limit     int64
	Used      int64
	Requested int64
}

func (e *AggregateBudgetError) Error() string {
	return fmt.Sprintf(
		"query: exact aggregate needs %d additional bytes with %d of %d already retained: %v",
		e.Requested, e.Used, e.Limit, ErrAggregateBudget,
	)
}

func (e *AggregateBudgetError) Unwrap() error { return ErrAggregateBudget }

type aggregateBudget struct {
	limit int64
	epoch uint64
	used  atomic.Int64
}

func (b *aggregateBudget) begin(limit int64) {
	b.limit = limit
	b.epoch++
	if b.epoch == 0 {
		b.epoch = 1
	}
	b.used.Store(0)
}

type aggregateLease struct {
	epoch    uint64
	reserved int64
}

func (l *aggregateLease) reserve(b *aggregateBudget, need int64) error {
	if need < 0 {
		// A negative need is an upstream size overflow. Do not turn a
		// MaxInt64 budget into MinInt64 with limit+1 and accidentally admit it.
		return budgetError(b, math.MaxInt64)
	}
	if l.epoch != b.epoch {
		l.epoch = b.epoch
		l.reserved = 0
	}
	if need <= l.reserved {
		return nil
	}
	delta := need - l.reserved
	for {
		used := b.used.Load()
		if delta > b.limit-used {
			return &AggregateBudgetError{
				Limit: b.limit, Used: used, Requested: delta,
			}
		}
		if b.used.CompareAndSwap(used, used+delta) {
			l.reserved = need
			return nil
		}
	}
}

type aggAcc struct {
	count int
	num   *numberAcc
	lease aggregateLease
}

type numberAcc struct {
	n       int
	sum     decimalSum
	average decimalSum
	extreme scalar
	owned   []byte
}

// decimalSum is coefficient × 10^scale. The compact form covers the ordinary
// int64 and common fixed-decimal path without allocation. The big form is
// entered only when coefficient arithmetic or an exponent leaves int64.
// Coefficients are always normalized: nonzero coefficients have no trailing
// decimal zero.
type decimalSum struct {
	set bool
	big bool

	smallCoeff int64
	smallScale int64

	coeff big.Int
	scale big.Int

	termCoeff big.Int
	termScale big.Int
	pow       big.Int
	tmp       big.Int
	rem       big.Int
	aux       big.Int
	exp       big.Int

	parse  []byte
	digits int
}

func resetAggs(accs []aggAcc) {
	for i := range accs {
		accs[i].count = 0
		if accs[i].num != nil {
			accs[i].num.reset()
		}
	}
}

func (a *numberAcc) reset() {
	a.n = 0
	a.extreme = scalar{}
	a.owned = a.owned[:0]
	a.sum.reset()
	a.average.reset()
}

func (s *decimalSum) reset() {
	s.set = false
	s.big = false
	s.smallCoeff = 0
	s.smallScale = 0
	s.coeff.SetInt64(0)
	s.scale.SetInt64(0)
	s.termCoeff.SetInt64(0)
	s.termScale.SetInt64(0)
	s.pow.SetInt64(0)
	s.tmp.SetInt64(0)
	s.rem.SetInt64(0)
	s.aux.SetInt64(0)
	s.exp.SetInt64(0)
	s.parse = s.parse[:0]
	s.digits = 0
}

func (a *aggAcc) number(b *aggregateBudget) (*numberAcc, error) {
	if err := a.lease.reserve(b, aggregateAccBaseBytes); err != nil {
		return nil, err
	}
	if a.num == nil {
		a.num = new(numberAcc)
	}
	return a.num, nil
}

func (a *aggAcc) accumulateNumber(kind aggKind, value scalar, b *aggregateBudget) error {
	n, err := a.number(b)
	if err != nil {
		return err
	}
	switch kind {
	case aggSum, aggAvg:
		if err := n.sum.add(value, &a.lease, b); err != nil {
			return err
		}
	case aggMin:
		if n.n == 0 || compareScalar(value, n.extreme) < 0 {
			if err := a.lease.reserve(b, aggregateAccBaseBytes+int64(len(value.num))); err != nil {
				return err
			}
			n.extreme = value
		}
	case aggMax:
		if n.n == 0 || compareScalar(value, n.extreme) > 0 {
			if err := a.lease.reserve(b, aggregateAccBaseBytes+int64(len(value.num))); err != nil {
				return err
			}
			n.extreme = value
		}
	}
	n.n++
	return nil
}

func (s *decimalSum) add(value scalar, lease *aggregateLease, budget *aggregateBudget) error {
	// Before parsing a wide exponent or coefficient, reserve a conservative
	// multiple of its spelling. This bounds every math/big temporary as well as
	// the retained coefficient and parse buffer.
	inputNeed := aggregateAccBaseBytes + 8*int64(len(value.num)+32)
	if err := lease.reserve(budget, inputNeed); err != nil {
		return err
	}

	// JSON integers dominate SQL telemetry and counters. Keep their running
	// value at scale zero while it fits int64, regardless of source trailing
	// zeroes. Parsing 30 as coefficient 3 × 10^1 and then aligning it with 31
	// would otherwise promote an entirely machine-integer sum to math/big on
	// every change in decimal width.
	if value.isInt && (!s.set || !s.big) {
		current, ok := int64(0), true
		if s.set {
			if s.smallScale < 0 {
				ok = false
			} else {
				current, ok = multiplyPow10Int64(s.smallCoeff, s.smallScale)
			}
		}
		if ok {
			if sum, sumOK := checkedAddInt64(current, value.ival); sumOK {
				s.set = true
				s.smallCoeff = sum
				s.smallScale = 0
				s.digits = intDigits64(sum)
				return nil
			}
		}
	}

	d := parseDecimal(value.num)
	digitCount := len(d.intDigits) + len(d.fracDigits)
	termSmall := d.zero || digitCount <= 18 && !d.weight.wide
	var termCoeff int64
	var termScale int64
	if termSmall && !d.zero {
		for i := 0; i < digitCount; i++ {
			digit, _ := significantDigitAt(&d, i)
			termCoeff = termCoeff*10 + int64(digit-'0')
		}
		if d.neg {
			termCoeff = -termCoeff
		}
		var ok bool
		termScale, ok = checkedAddInt64(d.weight.compact, -int64(digitCount-1))
		termSmall = ok
	}
	if termSmall && (!s.set || !s.big) {
		if !s.set {
			s.set = true
			s.smallCoeff = termCoeff
			s.smallScale = termScale
			s.digits = intDigits64(termCoeff)
			return nil
		}
		if s.smallScale == termScale {
			if sum, ok := checkedAddInt64(s.smallCoeff, termCoeff); ok {
				s.smallCoeff = sum
				s.digits = intDigits64(sum)
				return nil
			}
		}
	}

	if err := s.parseBigTerm(&d, digitCount, termSmall, termCoeff, termScale); err != nil {
		return err
	}
	if !s.set {
		s.set = true
		s.big = true
		s.coeff.Set(&s.termCoeff)
		s.scale.Set(&s.termScale)
		s.normalizeBig()
		return nil
	}
	if !s.big {
		s.promote()
	}
	return s.addBigTerm(lease, budget)
}

func (s *decimalSum) addSum(other *decimalSum, lease *aggregateLease, budget *aggregateBudget) error {
	if !other.set {
		return nil
	}
	need := aggregateAccBaseBytes + 8*int64(max(s.digits, other.digits)+96)
	if err := lease.reserve(budget, need); err != nil {
		return err
	}
	if !s.set {
		s.set = true
		s.big = other.big
		s.smallCoeff = other.smallCoeff
		s.smallScale = other.smallScale
		s.digits = other.digits
		if other.big {
			s.coeff.Set(&other.coeff)
			s.scale.Set(&other.scale)
			s.parse = append(s.parse[:0], other.parse...)
		}
		return nil
	}
	if !s.big && !other.big && s.smallScale == other.smallScale {
		if sum, ok := checkedAddInt64(s.smallCoeff, other.smallCoeff); ok {
			s.smallCoeff = sum
			s.digits = intDigits64(sum)
			return nil
		}
	}
	if !s.big {
		s.promote()
	}
	if other.big {
		s.termCoeff.Set(&other.coeff)
		s.termScale.Set(&other.scale)
	} else {
		s.termCoeff.SetInt64(other.smallCoeff)
		s.termScale.SetInt64(other.smallScale)
	}
	return s.addBigTerm(lease, budget)
}

func detachAggregateExtremes(accs []aggAcc, columns []planColumn) {
	for i := range accs {
		if columns[i].agg != aggMin && columns[i].agg != aggMax {
			continue
		}
		n := accs[i].num
		if n == nil || n.n == 0 || len(n.extreme.num) == 0 {
			continue
		}
		n.owned = append(n.owned[:0], n.extreme.num...)
		n.extreme.num = n.owned
		n.extreme.raw = n.owned
	}
}

func copyAggregateExtreme(dst *numberAcc, src scalar) {
	dst.owned = append(dst.owned[:0], src.num...)
	dst.extreme = src
	dst.extreme.num = dst.owned
	dst.extreme.raw = dst.owned
}

func (s *decimalSum) parseBigTerm(
	d *decimal,
	digitCount int,
	termSmall bool,
	termCoeff int64,
	termScale int64,
) error {
	if termSmall {
		s.termCoeff.SetInt64(termCoeff)
		s.termScale.SetInt64(termScale)
		return nil
	}

	s.parse = s.parse[:0]
	if d.neg {
		s.parse = append(s.parse, '-')
	}
	s.parse = append(s.parse, d.intDigits...)
	s.parse = append(s.parse, d.fracDigits...)
	if _, ok := s.termCoeff.SetString(byteview.String(s.parse), 10); !ok {
		return fmt.Errorf("query: invalid exact aggregate coefficient")
	}

	if !d.weight.wide {
		s.termScale.SetInt64(d.weight.compact)
	} else {
		s.parse = s.parse[:0]
		if d.weight.neg {
			s.parse = append(s.parse, '-')
		}
		for i, n := 0, weightMagnitudeLen(&d.weight); i < n; i++ {
			s.parse = append(s.parse, weightMagnitudeDigit(&d.weight, i))
		}
		if _, ok := s.termScale.SetString(byteview.String(s.parse), 10); !ok {
			return fmt.Errorf("query: invalid exact aggregate exponent")
		}
	}
	s.tmp.SetInt64(int64(digitCount - 1))
	s.termScale.Sub(&s.termScale, &s.tmp)
	return nil
}

func (s *decimalSum) promote() {
	s.coeff.SetInt64(s.smallCoeff)
	s.scale.SetInt64(s.smallScale)
	s.big = true
}

func (s *decimalSum) addBigTerm(lease *aggregateLease, budget *aggregateBudget) error {
	cmp := s.scale.Cmp(&s.termScale)
	shift := int64(0)
	switch {
	case cmp > 0:
		s.tmp.Sub(&s.scale, &s.termScale)
		var ok bool
		shift, ok = boundedBigShift(&s.tmp, budget.limit)
		if !ok {
			return budgetError(budget, math.MaxInt64)
		}
	case cmp < 0:
		s.tmp.Sub(&s.termScale, &s.scale)
		var ok bool
		shift, ok = boundedBigShift(&s.tmp, budget.limit)
		if !ok {
			return budgetError(budget, math.MaxInt64)
		}
	}

	currentDigits := max(s.digits, 1)
	if currentDigits == 1 && s.coeff.Sign() != 0 {
		currentDigits = s.decimalDigits(&s.coeff)
	}
	termDigits := s.decimalDigits(&s.termCoeff)
	predicted := int64(max(currentDigits, termDigits)) + shift + 1
	need := aggregateAccBaseBytes + 8*(predicted+int64(len(s.parse))+64)
	if err := lease.reserve(budget, need); err != nil {
		return err
	}

	switch {
	case cmp > 0:
		s.mulPow10(&s.coeff, int(shift))
		s.scale.Set(&s.termScale)
	case cmp < 0:
		s.mulPow10(&s.termCoeff, int(shift))
	}
	s.coeff.Add(&s.coeff, &s.termCoeff)
	s.normalizeBig()
	return nil
}

func boundedBigShift(v *big.Int, limit int64) (int64, bool) {
	if !v.IsInt64() {
		return 0, false
	}
	n := v.Int64()
	return n, n >= 0 && n <= limit
}

func budgetError(b *aggregateBudget, need int64) error {
	used := b.used.Load()
	requested := int64(1)
	if need > used {
		requested = need - used
	}
	return &AggregateBudgetError{Limit: b.limit, Used: used, Requested: requested}
}

func (s *decimalSum) mulPow10(value *big.Int, n int) {
	if n == 0 || value.Sign() == 0 {
		return
	}
	s.exp.SetInt64(int64(n))
	s.pow.Exp(aggregateBigTen, &s.exp, nil)
	value.Mul(value, &s.pow)
}

func intDigits64(v int64) int {
	u := absInt64(v)
	return uintDigits(u)
}

func (s *decimalSum) normalizeBig() {
	if s.coeff.Sign() == 0 {
		s.big = false
		s.smallCoeff = 0
		s.smallScale = 0
		s.digits = 1
		return
	}
	if s.demoteIfCompact() {
		return
	}
	s.parse = s.coeff.Append(s.parse[:0], 10)
	start := 0
	if s.parse[0] == '-' {
		start = 1
	}
	end := len(s.parse)
	for end > start && s.parse[end-1] == '0' {
		end--
	}
	zeros := len(s.parse) - end
	if zeros != 0 {
		if _, ok := s.coeff.SetString(byteview.String(s.parse[:end]), 10); !ok {
			panic("query: normalized aggregate coefficient became invalid")
		}
		s.tmp.SetInt64(int64(zeros))
		s.scale.Add(&s.scale, &s.tmp)
	}
	s.digits = end - start
	s.demoteIfCompact()
}

// demoteIfCompact moves a big state back to the allocation-free int64 form
// when both components and the normalization adjustment fit. Checking the
// trailing-zero adjustment before mutating the state matters at MaxInt64:
// coefficient 10 × 10^MaxInt64 has a normalized exponent one larger than an
// int64 even though both unnormalized components individually fit.
func (s *decimalSum) demoteIfCompact() bool {
	if !s.coeff.IsInt64() || !s.scale.IsInt64() {
		return false
	}
	coefficient := s.coeff.Int64()
	scale := s.scale.Int64()
	if coefficient == 0 {
		s.smallCoeff, s.smallScale, s.digits, s.big = 0, 0, 1, false
		return true
	}
	zeros := int64(0)
	for coefficient%10 == 0 {
		coefficient /= 10
		zeros++
	}
	var ok bool
	scale, ok = checkedAddInt64(scale, zeros)
	if !ok {
		return false
	}
	s.smallCoeff = coefficient
	s.smallScale = scale
	s.digits = intDigits64(coefficient)
	s.big = false
	return true
}

// decimalDigits returns the exact base-10 digit count without formatting an
// unretained string. The logarithm is only an initial integer estimate; a
// power-of-ten comparison corrects it before the count is used.
func (s *decimalSum) decimalDigits(v *big.Int) int {
	if v.Sign() == 0 {
		return 1
	}
	estimate := int(float64(v.BitLen()-1)*0.3010299956639812) + 1
	if estimate < 1 {
		estimate = 1
	}
	s.exp.SetInt64(int64(estimate - 1))
	s.tmp.SetInt64(10)
	s.pow.Exp(&s.tmp, &s.exp, nil)
	for v.CmpAbs(&s.pow) < 0 {
		estimate--
		s.pow.Quo(&s.pow, aggregateBigTen)
	}
	s.pow.Mul(&s.pow, aggregateBigTen)
	for v.CmpAbs(&s.pow) >= 0 {
		estimate++
		s.pow.Mul(&s.pow, aggregateBigTen)
	}
	return estimate
}

func (s *decimalSum) appendJSON(dst []byte) []byte {
	if !s.big {
		coefficient, scale := s.smallCoeff, s.smallScale
		if coefficient == 0 {
			scale = 0
		} else {
			// The compact accumulation path deliberately postpones
			// normalization while same-scale terms keep arriving. Canonicalize
			// only the emitted view so serial, parallel, and spill merges
			// produce byte-identical encodings without adding work per row.
			for coefficient%10 == 0 {
				coefficient /= 10
				scale++
			}
		}
		dst = strconv.AppendInt(dst, coefficient, 10)
		if scale != 0 {
			dst = append(dst, 'e')
			dst = strconv.AppendInt(dst, scale, 10)
		}
		return dst
	}
	dst = append(dst, s.parse...)
	if s.scale.Sign() != 0 {
		dst = append(dst, 'e')
		dst = s.scale.Append(dst, 10)
	}
	return dst
}

// fixedJSONLen reports the ordinary-decimal encoding width after compact
// normalization. Extremely large positive or negative scales decline so the
// scientific form can represent them without manufacturing a zero run.
func (s *decimalSum) fixedJSONLen() (int64, bool) {
	digits, negative, scale := s.digits, s.sign() < 0, int64(0)
	if s.big {
		if !s.scale.IsInt64() {
			return 0, false
		}
		scale = s.scale.Int64()
	} else {
		coefficient := s.smallCoeff
		scale = s.smallScale
		if coefficient == 0 {
			return 1, true
		}
		for coefficient%10 == 0 {
			coefficient /= 10
			if scale == math.MaxInt64 {
				return 0, false
			}
			scale++
		}
		digits = intDigits64(coefficient)
	}
	sign := int64(0)
	if negative {
		sign = 1
	}
	width := int64(digits) + sign
	if scale == math.MinInt64 {
		return 0, false
	}
	switch {
	case scale >= 0:
		if scale > math.MaxInt64-width {
			return 0, false
		}
		width += scale
	case scale > -int64(digits):
		width++ // decimal point within the coefficient
	default:
		zeros := -scale - int64(digits)
		if zeros > math.MaxInt64-width-2 {
			return 0, false
		}
		width += 2 + zeros // "0.", leading zeroes, coefficient
	}
	return width, width <= maxFixedAggregateJSON
}

func (s *decimalSum) appendFixedJSON(dst []byte) []byte {
	start := len(dst)
	scale := int64(0)
	if s.big {
		dst = append(dst, s.parse...)
		scale = s.scale.Int64()
	} else {
		coefficient := s.smallCoeff
		scale = s.smallScale
		if coefficient == 0 {
			return append(dst, '0')
		}
		for coefficient%10 == 0 {
			coefficient /= 10
			scale++
		}
		dst = strconv.AppendInt(dst, coefficient, 10)
	}

	digitStart := start
	if dst[digitStart] == '-' {
		digitStart++
	}
	digits := len(dst) - digitStart
	if scale >= 0 {
		for range scale {
			dst = append(dst, '0')
		}
		return dst
	}
	point := int64(digits) + scale
	if point > 0 {
		at := digitStart + int(point)
		dst = append(dst, 0)
		copy(dst[at+1:], dst[at:len(dst)-1])
		dst[at] = '.'
		return dst
	}

	zeros := int(-point)
	extra := zeros + 2
	oldEnd := len(dst)
	for range extra {
		dst = append(dst, 0)
	}
	copy(dst[digitStart+extra:], dst[digitStart:oldEnd])
	dst[digitStart] = '0'
	dst[digitStart+1] = '.'
	for i := 0; i < zeros; i++ {
		dst[digitStart+2+i] = '0'
	}
	return dst
}

func (s *decimalSum) int64Value() (int64, bool) {
	if s.big {
		if s.scale.Sign() < 0 || !s.scale.IsInt64() || !s.coeff.IsInt64() {
			return 0, false
		}
		return multiplyPow10Int64(s.coeff.Int64(), s.scale.Int64())
	}
	if s.smallScale < 0 {
		return 0, false
	}
	return multiplyPow10Int64(s.smallCoeff, s.smallScale)
}

func multiplyPow10Int64(v, scale int64) (int64, bool) {
	if scale > 18 {
		return 0, v == 0
	}
	for range scale {
		next, ok := checkedMulInt64(v, 10)
		if !ok {
			return 0, false
		}
		v = next
	}
	return v, true
}

func checkedMulInt64(a, b int64) (int64, bool) {
	if a == 0 || b == 0 {
		return 0, true
	}
	if a == -1 && b == -1<<63 || b == -1 && a == -1<<63 {
		return 0, false
	}
	product := a * b
	return product, product/b == a
}

func (w *Workspace) exactDecimalCell(sum *decimalSum) (Cell, error) {
	fixedLen, fixed := sum.fixedJSONLen()
	encodedLen := int64(sum.digits + 48)
	if fixed {
		encodedLen = fixedLen
	}
	need := int64(len(w.aggregateOut)) + encodedLen
	if err := w.aggregateLease.reserve(&w.aggregateBudget, need); err != nil {
		return Cell{}, err
	}
	start := len(w.aggregateOut)
	if fixed {
		w.aggregateOut = sum.appendFixedJSON(w.aggregateOut)
	} else {
		w.aggregateOut = sum.appendJSON(w.aggregateOut)
	}
	raw := w.aggregateOut[start:len(w.aggregateOut):len(w.aggregateOut)]
	cell := Cell{kind: TypeNumber, flag: cellNumberRaw, raw: raw}
	if value, ok := sum.int64Value(); ok {
		cell.flag |= cellInteger
		cell.word = uint64(value)
	}
	return cell, nil
}

func (w *Workspace) exactAverageCell(a *aggAcc) (Cell, error) {
	if a.num == nil || a.num.n == 0 {
		return nullCell(), nil
	}
	// The n==1 path is both exact and the overwhelmingly common grouped-AVG
	// case. The general half-even quotient is completed below in this file.
	if a.num.n == 1 {
		return w.exactDecimalCell(&a.num.sum)
	}
	average, err := a.num.averageOf(
		&a.num.sum, a.num.n, &a.lease, &w.aggregateBudget,
	)
	if err != nil {
		return Cell{}, err
	}
	return w.exactDecimalCell(average)
}

func (w *Workspace) exactSumCell(a *aggAcc) (Cell, error) {
	if a.num == nil || a.num.n == 0 {
		return nullCell(), nil
	}
	return w.exactDecimalCell(&a.num.sum)
}

func (a *numberAcc) averageOf(
	sum *decimalSum,
	count int,
	lease *aggregateLease,
	budget *aggregateBudget,
) (*decimalSum, error) {
	out := &a.average
	out.reset()
	a.prepareAverageFraction(out, sum, count)

	// A denominator containing only factors 2 and 5 has a finite decimal
	// expansion. Emit it exactly when normalization leaves no more than the
	// policy's 34 significant digits.
	denominator := out.termCoeff.Uint64()
	twos, fives := 0, 0
	for denominator%2 == 0 {
		denominator /= 2
		twos++
	}
	for denominator%5 == 0 {
		denominator /= 5
		fives++
	}
	if denominator == 1 {
		scaleDown := max(twos, fives)
		for i := twos; i < scaleDown; i++ {
			out.coeff.Mul(&out.coeff, aggregateBigTwo)
		}
		for i := fives; i < scaleDown; i++ {
			out.coeff.Mul(&out.coeff, aggregateBigFive)
		}
		out.tmp.SetInt64(int64(scaleDown))
		out.scale.Sub(&out.scale, &out.tmp)
		if sum.sign() < 0 {
			out.coeff.Neg(&out.coeff)
		}
		out.set, out.big = true, true
		out.normalizeBig()
		if out.digits <= averageDigits {
			need := aggregateAccBaseBytes + 8*int64(out.digits+scaleDown+96)
			if err := lease.reserve(budget, need); err != nil {
				return nil, err
			}
			return out, nil
		}
		a.prepareAverageFraction(out, sum, count)
	}

	numeratorDigits := out.decimalDigits(&out.coeff)
	denominatorDigits := out.decimalDigits(&out.termCoeff)
	preflight := int64(numeratorDigits + denominatorDigits + averageDigits + 128)
	if err := lease.reserve(budget, aggregateAccBaseBytes+8*preflight); err != nil {
		return nil, err
	}
	order := numeratorDigits - denominatorDigits
	if order >= 0 {
		out.tmp.Set(&out.termCoeff)
		out.mulPow10(&out.tmp, order)
		if out.coeff.Cmp(&out.tmp) < 0 {
			order--
		}
	} else {
		out.tmp.Set(&out.coeff)
		out.mulPow10(&out.tmp, -order)
		if out.tmp.Cmp(&out.termCoeff) < 0 {
			order--
		}
	}
	shift := averageDigits - 1 - order
	predicted := int64(max(numeratorDigits, denominatorDigits) + absInt(shift) + averageDigits + 128)
	if err := lease.reserve(budget, aggregateAccBaseBytes+8*predicted); err != nil {
		return nil, err
	}

	if shift >= 0 {
		out.tmp.Set(&out.coeff)
		out.mulPow10(&out.tmp, shift)
		out.coeff.QuoRem(&out.tmp, &out.termCoeff, &out.rem)
		out.aux.Lsh(&out.rem, 1)
		switch cmp := out.aux.Cmp(&out.termCoeff); {
		case cmp > 0, cmp == 0 && out.coeff.Bit(0) != 0:
			out.coeff.Add(&out.coeff, aggregateBigOne)
		}
	} else {
		out.tmp.Set(&out.termCoeff)
		out.mulPow10(&out.tmp, -shift)
		out.coeff.QuoRem(&out.coeff, &out.tmp, &out.rem)
		out.aux.Lsh(&out.rem, 1)
		switch cmp := out.aux.Cmp(&out.tmp); {
		case cmp > 0, cmp == 0 && out.coeff.Bit(0) != 0:
			out.coeff.Add(&out.coeff, aggregateBigOne)
		}
	}
	if sum.sign() < 0 {
		out.coeff.Neg(&out.coeff)
	}
	out.tmp.SetInt64(int64(shift))
	out.scale.Sub(&out.scale, &out.tmp)
	out.set, out.big = true, true
	out.normalizeBig()
	return out, nil
}

func (a *numberAcc) prepareAverageFraction(out, sum *decimalSum, count int) {
	if !sum.big {
		numerator := absInt64(sum.smallCoeff)
		denominator := uint64(count)
		divisor := gcdUint64(numerator, denominator)
		out.coeff.SetUint64(numerator / divisor)
		out.termCoeff.SetUint64(denominator / divisor)
		out.scale.SetInt64(sum.smallScale)
		return
	}

	out.coeff.Abs(&sum.coeff)
	out.scale.Set(&sum.scale)
	out.termCoeff.SetInt64(int64(count))

	// big.Int.GCD creates private temporaries on every call. Keep Euclid's
	// working values in the accumulator instead, so even a wide AVG becomes
	// allocation-free once these retained integers have reached their
	// high-water capacities.
	out.aux.Set(&out.coeff)
	out.rem.Set(&out.termCoeff)
	for out.rem.Sign() != 0 {
		out.tmp.Mod(&out.aux, &out.rem)
		out.aux.Set(&out.rem)
		out.rem.Set(&out.tmp)
	}
	out.coeff.Quo(&out.coeff, &out.aux)
	out.termCoeff.Quo(&out.termCoeff, &out.aux)
}

func gcdUint64(a, b uint64) uint64 {
	for b != 0 {
		a, b = b, a%b
	}
	return a
}

func (s *decimalSum) sign() int {
	if s.big {
		return s.coeff.Sign()
	}
	switch {
	case s.smallCoeff < 0:
		return -1
	case s.smallCoeff > 0:
		return 1
	default:
		return 0
	}
}

func absInt(v int) int {
	if v < 0 {
		return -v
	}
	return v
}
