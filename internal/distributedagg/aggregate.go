// Package distributedagg combines algebraic aggregate fragments carried as
// canonical vibejson cells. It is shared by gateway fallback aggregation and
// worker-local exchange reducers so both paths have exactly the same numeric
// and grouping semantics.
package distributedagg

import (
	"bytes"
	"errors"
	"fmt"
	"math/big"
	"math/bits"
	"strconv"

	"github.com/thesyncim/vibedb/internal/exchange"
	queryplanner "github.com/thesyncim/vibedb/planner"
	queryengine "github.com/thesyncim/vibedb/query"
	vibejson "github.com/thesyncim/vibejson"
	jsondoc "github.com/thesyncim/vibejson/document"
	"github.com/thesyncim/vibejson/x/byteview"
)

var (
	// ErrAggregate reports a malformed or non-algebraic fragment.
	ErrAggregate = errors.New("distributed aggregate: fragments cannot be merged")
	// ErrLimit reports an exact state or output that exceeds its admitted bound.
	ErrLimit = errors.New("distributed aggregate: memory or output limit exceeded")
)

// Kind is the compact aggregate program carried between trusted workers.
type Kind uint8

const (
	None Kind = iota
	Count
	Sum
	Min
	Max
)

func (k Kind) Valid() bool { return k <= Max }

type extreme struct {
	cell   exchange.Cell
	number []byte
	ival   int64
	isInt  bool
	set    bool
}

// exactCount stores a 64-bit count inline (16 bytes) and lazily promotes to a
// heap-allocated big.Int only on uint64 overflow.
type exactCount struct {
	small uint64
	wide  *big.Int
}

func (c *exactCount) add(value uint64) {
	if c.wide == nil && ^uint64(0)-c.small >= value {
		c.small += value
		return
	}
	if c.wide == nil {
		c.wide = new(big.Int).SetUint64(c.small)
	}
	var term big.Int
	term.SetUint64(value)
	c.wide.Add(c.wide, &term)
}

func (c *exactCount) addBig(value *big.Int) {
	if c.wide == nil {
		c.wide = new(big.Int).SetUint64(c.small)
	}
	c.wide.Add(c.wide, value)
}

func (c *exactCount) appendTo(dst []byte) []byte {
	if c.wide != nil {
		return c.wide.Append(dst, 10)
	}
	return strconv.AppendUint(dst, c.small, 10)
}

func (c *exactCount) retainedBytes() uint64 {
	if c.wide == nil {
		return 0
	}
	return retainedBigIntBytes(c.wide)
}

type exactSumWide struct {
	integer  big.Int
	rational big.Rat
	fraction bool
}

// exactSum stores a 64-bit integer sum inline (24 bytes, down from 120 bytes)
// and lazily allocates exactSumWide only on int64 overflow or fractional input.
type exactSum struct {
	small int64
	wide  *exactSumWide
	set   bool
}

func (s *exactSum) addInt64(value int64) {
	const maxInt64 = int64(^uint64(0) >> 1)
	const minInt64 = -maxInt64 - 1
	if s.wide == nil {
		if !((value > 0 && s.small > maxInt64-value) ||
			(value < 0 && s.small < minInt64-value)) {
			s.small += value
			s.set = true
			return
		}
		w := &exactSumWide{}
		w.integer.SetInt64(s.small)
		s.wide = w
	}
	if s.wide.fraction {
		var term big.Rat
		term.SetInt64(value)
		s.wide.rational.Add(&s.wide.rational, &term)
	} else {
		var term big.Int
		term.SetInt64(value)
		s.wide.integer.Add(&s.wide.integer, &term)
	}
	s.set = true
}

func (s *exactSum) addBigInt(value *big.Int) {
	if s.wide == nil {
		w := &exactSumWide{}
		w.integer.SetInt64(s.small)
		s.wide = w
	}
	if s.wide.fraction {
		var term big.Rat
		term.SetInt(value)
		s.wide.rational.Add(&s.wide.rational, &term)
	} else {
		s.wide.integer.Add(&s.wide.integer, value)
	}
	s.set = true
}

func (s *exactSum) addRat(value *big.Rat) {
	if s.wide == nil {
		w := &exactSumWide{fraction: true}
		w.rational.SetInt64(s.small)
		s.wide = w
	} else if !s.wide.fraction {
		s.wide.rational.SetInt(&s.wide.integer)
		s.wide.integer = big.Int{}
		s.wide.fraction = true
	}
	s.wide.rational.Add(&s.wide.rational, value)
	s.set = true
}

func (s *exactSum) add(cell exchange.Cell, maxBytes uint64, scratch []byte) ([]byte, error) {
	if cell.Null {
		return scratch, nil
	}
	spelling := trimJSONSpace(cell.Bytes)
	if maxBytes != 0 && uint64(len(spelling)) > maxBytes {
		return scratch, fmt.Errorf("%w: SUM state exceeds exact numeric admission", ErrLimit)
	}
	if value, ok := parseCanonicalInt64(spelling); ok {
		s.addInt64(value)
		return scratch, nil
	}
	raw := vibejson.RawValue{Src: spelling}
	if value, ok := raw.Int64(); ok {
		s.addInt64(value)
		return scratch, nil
	}
	if isPlainJSONInteger(spelling) && vibejson.Valid(spelling) {
		var value big.Int
		// math/big exposes only a string parser; byteview makes this a borrowed,
		// allocation-free compatibility boundary on the promoted-number path.
		if _, ok := value.SetString(byteview.String(spelling), 10); !ok {
			return scratch, fmt.Errorf("%w: SUM state is not an exact number", ErrAggregate)
		}
		s.addBigInt(&value)
		return scratch, nil
	}

	canonical, canonicalErr := queryplanner.AppendCanonicalScalarJSON(scratch, spelling)
	if canonicalErr != nil || len(canonical) == 0 || canonical[0] == '"' ||
		canonical[0] == 't' || canonical[0] == 'f' || canonical[0] == 'n' ||
		!queryplanner.CanonicalNumberFitsDecimalBytes(byteview.String(canonical), maxBytes) {
		return canonical, fmt.Errorf("%w: SUM state exceeds exact numeric admission", ErrLimit)
	}
	var value big.Rat
	if _, ok := value.SetString(byteview.String(canonical)); !ok {
		return canonical, fmt.Errorf("%w: SUM state is not an exact number", ErrAggregate)
	}
	s.addRat(&value)
	return canonical, nil
}

func (s *exactSum) appendIntegerTo(dst []byte) []byte {
	if s.wide != nil {
		return s.wide.integer.Append(dst, 10)
	}
	return strconv.AppendInt(dst, s.small, 10)
}

func (s *exactSum) isFraction() bool {
	return s.wide != nil && s.wide.fraction
}

func (s *exactSum) retainedBytes() uint64 {
	if s.wide == nil {
		return 0
	}
	if s.wide.fraction {
		return retainedBigRatBytes(&s.wide.rational)
	}
	return retainedBigIntBytes(&s.wide.integer)
}

func trimJSONSpace(b []byte) []byte {
	if len(b) == 0 || (b[0] > ' ' && b[len(b)-1] > ' ') {
		return b
	}
	return bytes.TrimSpace(b)
}

// parseCanonicalUint64 validates and parses a canonical JSON non-negative
// integer (no leading zeros except "0", no decimals/exponents) in one pass.
func parseCanonicalUint64(b []byte) (uint64, bool) {
	if len(b) == 0 {
		return 0, false
	}
	if b[0] == '0' {
		return 0, len(b) == 1
	}
	if b[0] < '1' || b[0] > '9' {
		return 0, false
	}
	var val uint64
	for i := 0; i < len(b); i++ {
		c := b[i]
		if c < '0' || c > '9' {
			return 0, false
		}
		d := uint64(c - '0')
		if val > (^uint64(0)-d)/10 {
			return 0, false
		}
		val = val*10 + d
	}
	return val, true
}

// parseCanonicalInt64 validates and parses a canonical JSON signed integer
// (optional '-', no leading zeros except "0"/"-0", no decimals/exponents) in
// one pass.
func parseCanonicalInt64(b []byte) (int64, bool) {
	if len(b) == 0 {
		return 0, false
	}
	neg := false
	i := 0
	if b[0] == '-' {
		neg = true
		i = 1
		if len(b) == 1 {
			return 0, false
		}
	}
	if b[i] == '0' {
		return 0, len(b) == i+1
	}
	if b[i] < '1' || b[i] > '9' {
		return 0, false
	}
	const maxPos = uint64(1<<63 - 1)
	const maxNeg = uint64(1 << 63)
	limit := maxPos
	if neg {
		limit = maxNeg
	}
	var mag uint64
	for ; i < len(b); i++ {
		c := b[i]
		if c < '0' || c > '9' {
			return 0, false
		}
		d := uint64(c - '0')
		if mag > (limit-d)/10 {
			return 0, false
		}
		mag = mag*10 + d
	}
	if neg {
		return -int64(mag), true
	}
	return int64(mag), true
}

func addCount(total *exactCount, cell exchange.Cell, maxBytes uint64) error {
	if cell.Null {
		return fmt.Errorf("%w: COUNT state is null", ErrAggregate)
	}
	spelling := trimJSONSpace(cell.Bytes)
	if maxBytes != 0 && uint64(len(spelling)) > maxBytes {
		return fmt.Errorf("%w: COUNT state exceeds aggregate byte cap", ErrLimit)
	}
	if count, ok := parseCanonicalUint64(spelling); ok {
		total.add(count)
		return nil
	}
	if count, ok := (vibejson.RawValue{Src: spelling}).Uint64(); ok {
		total.add(count)
		return nil
	}
	var count big.Int
	if !isPlainJSONInteger(spelling) || !vibejson.Valid(spelling) {
		return fmt.Errorf("%w: COUNT state is not a non-negative integer", ErrAggregate)
	}
	if _, ok := count.SetString(byteview.String(spelling), 10); !ok || count.Sign() < 0 {
		return fmt.Errorf("%w: COUNT state is not a non-negative integer", ErrAggregate)
	}
	total.addBig(&count)
	return nil
}

func addExtreme(state *extreme, cell exchange.Cell, maximum bool) error {
	if cell.Null {
		return nil
	}
	spelling := trimJSONSpace(cell.Bytes)
	ival, isInt := parseCanonicalInt64(spelling)
	if !isInt {
		raw := vibejson.RawValue{Src: spelling}
		if raw.Kind() != jsondoc.Number || !vibejson.Valid(spelling) {
			return fmt.Errorf("%w: MIN/MAX state is not numeric", ErrAggregate)
		}
	}
	if !state.set {
		state.cell, state.number, state.ival, state.isInt, state.set = cell, spelling, ival, isInt, true
		return nil
	}
	var replace bool
	if isInt && state.isInt {
		if maximum {
			replace = ival > state.ival
		} else {
			replace = ival < state.ival
		}
	} else {
		comparison := queryengine.CompareValidatedJSONNumbers(spelling, state.number)
		replace = (maximum && comparison > 0) || (!maximum && comparison < 0)
	}
	if replace {
		state.cell, state.number, state.ival, state.isInt = cell, spelling, ival, isInt
	}
	return nil
}

// MergeCells combines one scalar aggregate column without decoding generic
// JSON or converting values to owned strings.
func MergeCells(kind Kind, cells []exchange.Cell, maxBytes uint64) (exchange.Cell, error) {
	switch kind {
	case Count:
		var total exactCount
		for i := range cells {
			if err := addCount(&total, cells[i], maxBytes); err != nil {
				return exchange.Cell{}, err
			}
		}
		spelling := total.appendTo(nil)
		if maxBytes != 0 && uint64(len(spelling)) > maxBytes {
			return exchange.Cell{}, fmt.Errorf("%w: COUNT output exceeds aggregate byte cap", ErrLimit)
		}
		return exchange.Cell{Bytes: spelling}, nil
	case Sum:
		var total exactSum
		var scratch []byte
		for i := range cells {
			var err error
			scratch, err = total.add(cells[i], maxBytes, scratch[:0])
			if err != nil {
				return exchange.Cell{}, err
			}
		}
		if !total.set {
			return exchange.Cell{Null: true}, nil
		}
		if !total.isFraction() {
			spelling := total.appendIntegerTo(nil)
			if maxBytes != 0 && uint64(len(spelling)) > maxBytes {
				return exchange.Cell{}, fmt.Errorf("%w: SUM output exceeds aggregate byte cap", ErrLimit)
			}
			return exchange.Cell{Bytes: spelling}, nil
		}
		spelling, err := appendExactDecimal(nil, &total.wide.rational, maxBytes)
		if err != nil {
			return exchange.Cell{}, err
		}
		return exchange.Cell{Bytes: spelling}, nil
	case Min, Max:
		var state extreme
		for i := range cells {
			if err := addExtreme(&state, cells[i], kind == Max); err != nil {
				return exchange.Cell{}, err
			}
		}
		if !state.set {
			return exchange.Cell{Null: true}, nil
		}
		return state.cell, nil
	default:
		return exchange.Cell{}, fmt.Errorf("%w: aggregate kind %d has no scalar combiner", ErrAggregate, kind)
	}
}

func appendExactDecimal(dst []byte, value *big.Rat, maxBytes uint64) ([]byte, error) {
	denominator := new(big.Int).Set(value.Denom())
	twos, fives := 0, 0
	for denominator.Bit(0) == 0 {
		denominator.Rsh(denominator, 1)
		twos++
	}
	five := big.NewInt(5)
	var remainder big.Int
	for {
		remainder.Mod(denominator, five)
		if remainder.Sign() != 0 {
			break
		}
		denominator.Quo(denominator, five)
		fives++
	}
	if denominator.Cmp(big.NewInt(1)) != 0 {
		return dst, fmt.Errorf("%w: SUM state is not a finite decimal", ErrAggregate)
	}
	scale := max(twos, fives)
	coefficient := new(big.Int).Set(value.Num())
	if twos < scale {
		coefficient.Mul(coefficient, new(big.Int).Exp(big.NewInt(2), big.NewInt(int64(scale-twos)), nil))
	}
	if fives < scale {
		coefficient.Mul(coefficient, new(big.Int).Exp(big.NewInt(5), big.NewInt(int64(scale-fives)), nil))
	}
	negative := coefficient.Sign() < 0
	coefficient.Abs(coefficient)
	digits := coefficient.Append(nil, 10)
	needed := len(digits)
	if scale >= len(digits) {
		needed = scale + 2
	} else if scale != 0 {
		needed++
	}
	if negative {
		needed++
	}
	if maxBytes != 0 && uint64(needed) > maxBytes {
		return dst, fmt.Errorf("%w: SUM output exceeds aggregate byte cap", ErrLimit)
	}
	start := len(dst)
	if negative && !(len(digits) == 1 && digits[0] == '0') {
		dst = append(dst, '-')
	}
	if scale == 0 {
		return append(dst, digits...), nil
	}
	if scale >= len(digits) {
		dst = append(dst, '0', '.')
		for range scale - len(digits) {
			dst = append(dst, '0')
		}
		dst = append(dst, digits...)
	} else {
		point := len(digits) - scale
		dst = append(dst, digits[:point]...)
		dst = append(dst, '.')
		dst = append(dst, digits[point:]...)
	}
	for len(dst) > start && dst[len(dst)-1] == '0' {
		dst = dst[:len(dst)-1]
	}
	if len(dst) > start && dst[len(dst)-1] == '.' {
		dst = dst[:len(dst)-1]
	}
	return dst, nil
}

func isPlainJSONInteger(value []byte) bool {
	if len(value) == 0 {
		return false
	}
	start := 0
	if value[0] == '-' {
		start = 1
	}
	if start == len(value) {
		return false
	}
	for _, char := range value[start:] {
		if char < '0' || char > '9' {
			return false
		}
	}
	return true
}

func retainedBigIntBytes(value *big.Int) uint64 {
	return uint64(cap(value.Bits())) * uint64(bits.UintSize/8)
}

func retainedBigRatBytes(value *big.Rat) uint64 {
	return retainedBigIntBytes(value.Num()) + retainedBigIntBytes(value.Denom())
}
