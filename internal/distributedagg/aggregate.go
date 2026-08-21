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
	set    bool
}

type exactCount struct {
	small uint64
	wide  big.Int
	large bool
}

func (c *exactCount) add(value uint64) {
	if !c.large && ^uint64(0)-c.small >= value {
		c.small += value
		return
	}
	if !c.large {
		c.wide.SetUint64(c.small)
		c.large = true
	}
	var term big.Int
	term.SetUint64(value)
	c.wide.Add(&c.wide, &term)
}

func (c *exactCount) addBig(value *big.Int) {
	if !c.large {
		c.wide.SetUint64(c.small)
		c.large = true
	}
	c.wide.Add(&c.wide, value)
}

func (c *exactCount) appendTo(dst []byte) []byte {
	if c.large {
		return c.wide.Append(dst, 10)
	}
	return strconv.AppendUint(dst, c.small, 10)
}

func (c *exactCount) retainedBytes() uint64 {
	if !c.large {
		return 0
	}
	return retainedBigIntBytes(&c.wide)
}

type exactSignedInteger struct {
	small int64
	wide  big.Int
	large bool
}

func (i *exactSignedInteger) add(value int64) {
	const maxInt64 = int64(^uint64(0) >> 1)
	const minInt64 = -maxInt64 - 1
	if !i.large && !((value > 0 && i.small > maxInt64-value) ||
		(value < 0 && i.small < minInt64-value)) {
		i.small += value
		return
	}
	if !i.large {
		i.wide.SetInt64(i.small)
		i.large = true
	}
	var term big.Int
	term.SetInt64(value)
	i.wide.Add(&i.wide, &term)
}

func (i *exactSignedInteger) addBig(value *big.Int) {
	if !i.large {
		i.wide.SetInt64(i.small)
		i.large = true
	}
	i.wide.Add(&i.wide, value)
}

func (i *exactSignedInteger) appendTo(dst []byte) []byte {
	if i.large {
		return i.wide.Append(dst, 10)
	}
	return strconv.AppendInt(dst, i.small, 10)
}

func (i *exactSignedInteger) setRat(dst *big.Rat) {
	if i.large {
		dst.SetInt(&i.wide)
		return
	}
	var value big.Int
	value.SetInt64(i.small)
	dst.SetInt(&value)
}

func (i *exactSignedInteger) retainedBytes() uint64 {
	if !i.large {
		return 0
	}
	return retainedBigIntBytes(&i.wide)
}

type exactSum struct {
	integer  exactSignedInteger
	rational big.Rat
	set      bool
	fraction bool
}

func (s *exactSum) add(cell exchange.Cell, maxBytes uint64, scratch []byte) ([]byte, error) {
	if cell.Null {
		return scratch, nil
	}
	spelling := bytes.TrimSpace(cell.Bytes)
	if maxBytes != 0 && uint64(len(spelling)) > maxBytes {
		return scratch, fmt.Errorf("%w: SUM state exceeds exact numeric admission", ErrLimit)
	}
	raw := vibejson.RawValue{Src: spelling}
	if value, ok := raw.Int64(); ok {
		if s.fraction {
			var term big.Rat
			term.SetInt64(value)
			s.rational.Add(&s.rational, &term)
		} else {
			s.integer.add(value)
		}
		s.set = true
		return scratch, nil
	}
	if isPlainJSONInteger(spelling) && vibejson.Valid(spelling) {
		var value big.Int
		// math/big exposes only a string parser; byteview makes this a borrowed,
		// allocation-free compatibility boundary on the promoted-number path.
		if _, ok := value.SetString(byteview.String(spelling), 10); !ok {
			return scratch, fmt.Errorf("%w: SUM state is not an exact number", ErrAggregate)
		}
		if s.fraction {
			var term big.Rat
			term.SetInt(&value)
			s.rational.Add(&s.rational, &term)
		} else {
			s.integer.addBig(&value)
		}
		s.set = true
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
	if !s.fraction {
		s.integer.setRat(&s.rational)
		s.fraction = true
	}
	s.rational.Add(&s.rational, &value)
	s.set = true
	return canonical, nil
}

func (s *exactSum) retainedBytes() uint64 {
	if s.fraction {
		return retainedBigRatBytes(&s.rational)
	}
	return s.integer.retainedBytes()
}

func addCount(total *exactCount, cell exchange.Cell, maxBytes uint64) error {
	if cell.Null {
		return fmt.Errorf("%w: COUNT state is null", ErrAggregate)
	}
	spelling := bytes.TrimSpace(cell.Bytes)
	if maxBytes != 0 && uint64(len(spelling)) > maxBytes {
		return fmt.Errorf("%w: COUNT state exceeds aggregate byte cap", ErrLimit)
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
	spelling := bytes.TrimSpace(cell.Bytes)
	raw := vibejson.RawValue{Src: spelling}
	if raw.Kind() != jsondoc.Number || !vibejson.Valid(spelling) {
		return fmt.Errorf("%w: MIN/MAX state is not numeric", ErrAggregate)
	}
	if !state.set {
		state.cell, state.number, state.set = cell, spelling, true
		return nil
	}
	comparison := queryengine.CompareValidatedJSONNumbers(spelling, state.number)
	if (maximum && comparison > 0) || (!maximum && comparison < 0) {
		state.cell, state.number = cell, spelling
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
		if !total.fraction {
			spelling := total.integer.appendTo(nil)
			if maxBytes != 0 && uint64(len(spelling)) > maxBytes {
				return exchange.Cell{}, fmt.Errorf("%w: SUM output exceeds aggregate byte cap", ErrLimit)
			}
			return exchange.Cell{Bytes: spelling}, nil
		}
		spelling, err := appendExactDecimal(nil, &total.rational, maxBytes)
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
