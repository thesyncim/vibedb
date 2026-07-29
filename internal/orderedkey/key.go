// Package orderedkey encodes typed JSON scalars so ordinary byte comparison is
// semantic query order. It is the shared storage primitive for ordered
// secondary indexes; readable query syntax and transport framing deliberately
// live elsewhere.
package orderedkey

import (
	"encoding/binary"
	"math"
	"unicode/utf16"
	"unicode/utf8"
)

// Direction selects one component's index order.
type Direction uint8

const (
	Ascending Direction = iota
	Descending
)

// Type tags leave intentional gaps. A future ordered scalar family can be
// assigned between existing families without renumbering every persisted key.
const (
	tagNull byte = 0x10

	tagFalse byte = 0x20
	tagTrue  byte = 0x21

	tagNumber         byte = 0x30
	tagNumberNegative byte = 0x10
	tagNumberZero     byte = 0x20
	tagNumberPositive byte = 0x30

	tagString byte = 0x40
)

// The adjusted exponent is stored in an order-preserving variable-length form.
// The common int64 range stays compact, so a small integer key does not pay for
// the worst case. JSON itself places no width limit on an exponent, however, so
// two escape headers extend that compact form with an arbitrary-width decimal
// magnitude. A plain varint is unusable here: byte order must equal numeric
// order, and every length-prefixed varint whose prefix is not itself ordered
// gets this wrong — a 2-byte 256 would sort before a 1-byte 255 if the payload
// were compared before the length.
//
// For compact exponents, the binary-magnitude length is made order-carrying by
// folding it into a single header byte centred on 0x80:
//
//	header = 0x80 + L   for a positive exponent of L significant bytes
//	header = 0x80       for exponent zero, with no payload
//	header = 0x80 - L   for a negative exponent of L significant bytes
//
// Positive exponents then sort after zero, which sorts after negatives, purely
// on the header. Within the positives a longer exponent is a larger one, and
// 0x80+L grows with L, so the header alone already orders across the length
// boundary; the big-endian payload only has to break ties between equal
// lengths. Within the negatives the relationship inverts — a longer exponent is
// a *smaller* number — which 0x80-L delivers, and the payload is stored
// complemented so that a larger magnitude sorts earlier.
//
// Headers 0x89 and 0x77 sort just outside the compact positive and negative
// ranges. They carry an 8-byte big-endian decimal-digit count followed by the
// magnitude's ASCII digits. The negative form complements both fields so that
// a larger magnitude sorts earlier. Fixed-width length bytes make digit count
// order-carrying while keeping the representation prefix-free.
//
// Every payload is minimal (no leading zero byte/digit). That is what makes the
// encoding canonical: were 1 storable in two forms, an equality probe could
// miss rows, so the decoder also rejects a wide encoding of an int64 exponent.
//
// The header also states its own length, so the exponent stays self-delimiting
// and a component can still be skipped in one forward pass.
const (
	expHeaderWideNegative byte = 0x77
	expHeaderZero         byte = 0x80
	expHeaderWidePositive byte = 0x89
	expWideLengthBytes    int  = 8
)

type decimalExponent struct {
	negative bool
	digits   []byte
}

type magnitudeOperation uint8

const (
	magnitudeDirect magnitudeOperation = iota
	magnitudeCopy
	magnitudeAdd
	magnitudeSubtract
)

// adjustedExponent is either the byte-compatible compact int64 form or a view
// describing how to materialize a wide decimal magnitude directly into dst.
// Keeping the operation rather than its result is what makes the wide path
// allocation-free once the caller has reserved destination capacity.
type adjustedExponent struct {
	compact   bool
	value     int64
	negative  bool
	digits    int
	source    []byte
	delta     uint64
	operation magnitudeOperation
}

// appendExponent writes a compact adjusted exponent. For a negative *number*
// the whole exponent is complemented, because a larger exponent then means a
// more negative value that must sort earlier.
func appendExponent(dst []byte, adjusted int64, negative bool) []byte {
	start := len(dst)
	switch {
	case adjusted == 0:
		dst = append(dst, expHeaderZero)
	case adjusted > 0:
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(adjusted))
		i := 0
		for buf[i] == 0 {
			i++
		}
		dst = append(dst, expHeaderZero+byte(8-i))
		dst = append(dst, buf[i:]...)
	default:
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(-adjusted))
		i := 0
		for buf[i] == 0 {
			i++
		}
		dst = append(dst, expHeaderZero-byte(8-i))
		for _, b := range buf[i:] {
			dst = append(dst, ^b)
		}
	}
	if negative {
		invert(dst[start:])
	}
	return dst
}

type decodedExponent struct {
	compact     int64
	wide        bool
	negative    bool
	digitsStart int
	digitsEnd   int
	digitMask   byte
}

// decodeExponent reads either exponent form. mask maps bytes back through the
// direction and negative-number complements. It rejects non-minimal payloads,
// including a wide spelling of a compact exponent, so one value has one key.
func decodeExponent(key []byte, i int, mask byte) (decodedExponent, int, error) {
	if i >= len(key) {
		return decodedExponent{}, 0, ErrKeyTruncated
	}
	header := key[i] ^ mask
	i++
	switch {
	case header == expHeaderZero:
		return decodedExponent{}, i, nil
	case header > expHeaderZero && header < expHeaderWidePositive:
		n := int(header - expHeaderZero)
		if i+n > len(key) {
			return decodedExponent{}, 0, ErrKeyTruncated
		}
		var v uint64
		for _, b := range key[i : i+n] {
			v = v<<8 | uint64(b^mask)
		}
		// Minimal length and int64 range: a leading zero byte or a value with
		// the sign bit set could not have been produced by appendExponent.
		if v>>((n-1)*8) == 0 || v > uint64(math.MaxInt64) {
			return decodedExponent{}, 0, ErrMalformedKey
		}
		return decodedExponent{compact: int64(v)}, i + n, nil
	case header < expHeaderZero && header > expHeaderWideNegative:
		n := int(expHeaderZero - header)
		if i+n > len(key) {
			return decodedExponent{}, 0, ErrKeyTruncated
		}
		var v uint64
		for _, b := range key[i : i+n] {
			v = v<<8 | uint64(^(b ^ mask))
		}
		if v>>((n-1)*8) == 0 || v > uint64(math.MaxInt64) {
			return decodedExponent{}, 0, ErrMalformedKey
		}
		return decodedExponent{compact: -int64(v)}, i + n, nil
	case header == expHeaderWidePositive || header == expHeaderWideNegative:
		payloadMask := mask
		negative := header == expHeaderWideNegative
		if negative {
			payloadMask ^= 0xff
		}
		if len(key)-i < expWideLengthBytes {
			return decodedExponent{}, 0, ErrKeyTruncated
		}
		var n uint64
		for _, b := range key[i : i+expWideLengthBytes] {
			n = n<<8 | uint64(b^payloadMask)
		}
		i += expWideLengthBytes
		if n == 0 {
			return decodedExponent{}, 0, ErrMalformedKey
		}
		if n > uint64(len(key)-i) {
			return decodedExponent{}, 0, ErrKeyTruncated
		}
		end := i + int(n)
		for j := i; j < end; j++ {
			digit := key[j] ^ payloadMask
			if digit < '0' || digit > '9' || j == i && digit == '0' {
				return decodedExponent{}, 0, ErrMalformedKey
			}
		}
		// Values within int64 must use the compact representation. A lexical
		// comparison suffices because the wide magnitude has no leading zero.
		const maxInt64Decimal = "9223372036854775807"
		if n < uint64(len(maxInt64Decimal)) {
			return decodedExponent{}, 0, ErrMalformedKey
		}
		if n == uint64(len(maxInt64Decimal)) {
			for j := range maxInt64Decimal {
				got := key[i+j] ^ payloadMask
				if got < maxInt64Decimal[j] {
					return decodedExponent{}, 0, ErrMalformedKey
				}
				if got > maxInt64Decimal[j] {
					break
				}
				if j == len(maxInt64Decimal)-1 {
					return decodedExponent{}, 0, ErrMalformedKey
				}
			}
		}
		return decodedExponent{
			wide:        true,
			negative:    negative,
			digitsStart: i,
			digitsEnd:   end,
			digitMask:   payloadMask,
		}, end, nil
	default:
		return decodedExponent{}, 0, ErrMalformedKey
	}
}

// AppendNull appends one prefix-free null component.
func AppendNull(dst []byte, direction Direction) ([]byte, bool) {
	if direction > Descending {
		return dst, false
	}
	start := len(dst)
	dst = append(dst, tagNull)
	invertDescending(dst[start:], direction)
	return dst, true
}

// AppendBool appends one prefix-free Boolean component.
func AppendBool(dst []byte, value bool, direction Direction) ([]byte, bool) {
	if direction > Descending {
		return dst, false
	}
	start := len(dst)
	tag := byte(tagFalse)
	if value {
		tag = tagTrue
	}
	dst = append(dst, tag)
	invertDescending(dst[start:], direction)
	return dst, true
}

// AppendNumber canonicalizes one validated JSON number into order-preserving
// bytes. Equivalent spellings such as 1, 1.0, and 1e0 produce identical keys;
// finite JSON decimals never pass through float64.
//
// False reports invalid JSON-number syntax or an invalid direction. Exponents
// have JSON's full lexical range rather than an implementation-sized subset.
func AppendNumber(dst, number []byte, direction Direction) ([]byte, bool) {
	if direction > Descending {
		return dst, false
	}
	start := len(dst)
	negative, integerDigits, first, last, explicitExponent, ok := numberParts(number)
	if !ok {
		return dst, false
	}
	dst = append(dst, tagNumber)
	if first < 0 {
		dst = append(dst, tagNumberZero)
		invertDescending(dst[start:], direction)
		return dst, true
	}
	delta := int64(integerDigits) - int64(first)
	adjusted := makeAdjustedExponent(explicitExponent, delta)
	sign := byte(tagNumberPositive)
	if negative {
		sign = tagNumberNegative
	}
	dst = append(dst, sign)
	dst = appendAdjustedExponent(dst, adjusted, negative)

	digitIndex := 0
	for _, b := range number {
		if b < '0' || b > '9' {
			if b == 'e' || b == 'E' {
				break
			}
			continue
		}
		if digitIndex >= first && digitIndex <= last {
			digit := b - '0'
			if negative {
				// Reverse magnitude: digits 10..1 and terminator 11.
				dst = append(dst, 10-digit)
			} else {
				// Forward magnitude: digits 1..10 and terminator 0.
				dst = append(dst, digit+1)
			}
		}
		digitIndex++
	}
	if negative {
		dst = append(dst, 11)
	} else {
		dst = append(dst, 0)
	}
	invertDescending(dst[start:], direction)
	return dst, true
}

// NumberEncodedSize reports the exact byte length AppendNumber would append.
// It validates and normalizes without materializing coefficient digits, so a
// caller can enforce a key-size bound before giving append room to grow.
func NumberEncodedSize(number []byte) (int, bool) {
	_, integerDigits, first, last, explicitExponent, ok := numberParts(number)
	if !ok {
		return 0, false
	}
	if first < 0 {
		return 2, true // number tag plus zero tag
	}
	delta := int64(integerDigits) - int64(first)
	adjusted := makeAdjustedExponent(explicitExponent, delta)
	exponentBytes, ok := adjustedEncodedSize(adjusted)
	if !ok {
		return 0, false
	}
	coefficientBytes := last - first + 1
	if exponentBytes > math.MaxInt-3 ||
		coefficientBytes > math.MaxInt-3-exponentBytes {
		return 0, false
	}
	return 3 + exponentBytes + coefficientBytes, true
}

func makeAdjustedExponent(explicit decimalExponent, delta int64) adjustedExponent {
	if len(explicit.digits) == 0 {
		if delta >= 0 {
			return finishAdjustedExponent(false, nil, uint64(delta), magnitudeDirect)
		}
		return finishAdjustedExponent(true, nil, absInt64(delta), magnitudeDirect)
	}
	if delta == 0 {
		return finishAdjustedExponent(explicit.negative, explicit.digits, 0, magnitudeCopy)
	}

	deltaNegative := delta < 0
	deltaMagnitude := uint64(delta)
	if deltaNegative {
		deltaMagnitude = absInt64(delta)
	}
	if explicit.negative == deltaNegative {
		return finishAdjustedExponent(
			explicit.negative, explicit.digits, deltaMagnitude, magnitudeAdd,
		)
	}

	switch compareDecimalMagnitude(explicit.digits, deltaMagnitude) {
	case 0:
		return adjustedExponent{compact: true}
	case -1:
		// The explicit magnitude is smaller, so it necessarily fits uint64.
		// The result inherits delta's sign and is already materialized as one
		// machine word; this path is compact for every realizable JSON slice,
		// but finishAdjustedExponent also handles the theoretical 2^63 case.
		explicitMagnitude := parseDecimalMagnitude(explicit.digits)
		return finishAdjustedExponent(
			deltaNegative, nil, deltaMagnitude-explicitMagnitude, magnitudeDirect,
		)
	default:
		return finishAdjustedExponent(
			explicit.negative, explicit.digits, deltaMagnitude, magnitudeSubtract,
		)
	}
}

func finishAdjustedExponent(
	negative bool,
	source []byte,
	delta uint64,
	operation magnitudeOperation,
) adjustedExponent {
	digits, magnitude, magnitudeOK := magnitudeMetadata(source, delta, operation)
	if digits == 0 {
		return adjustedExponent{compact: true}
	}
	if magnitudeOK && magnitude <= uint64(math.MaxInt64) {
		value := int64(magnitude)
		if negative {
			value = -value
		}
		return adjustedExponent{compact: true, value: value}
	}
	return adjustedExponent{
		negative:  negative,
		digits:    digits,
		source:    source,
		delta:     delta,
		operation: operation,
	}
}

func appendAdjustedExponent(dst []byte, adjusted adjustedExponent, negativeNumber bool) []byte {
	if adjusted.compact {
		return appendExponent(dst, adjusted.value, negativeNumber)
	}
	start := len(dst)
	header := byte(expHeaderWidePositive)
	if adjusted.negative {
		header = expHeaderWideNegative
	}
	dst = append(dst, header)
	payloadStart := len(dst)
	var length [expWideLengthBytes]byte
	binary.BigEndian.PutUint64(length[:], uint64(adjusted.digits))
	dst = append(dst, length[:]...)
	dst = appendMagnitude(dst, adjusted)
	if adjusted.negative {
		invert(dst[payloadStart:])
	}
	if negativeNumber {
		invert(dst[start:])
	}
	return dst
}

func adjustedEncodedSize(adjusted adjustedExponent) (int, bool) {
	if !adjusted.compact {
		if adjusted.digits > math.MaxInt-1-expWideLengthBytes {
			return 0, false
		}
		return 1 + expWideLengthBytes + adjusted.digits, true
	}
	if adjusted.value == 0 {
		return 1, true
	}
	magnitude := uint64(adjusted.value)
	if adjusted.value < 0 {
		magnitude = uint64(-adjusted.value)
	}
	size := 2 // header plus at least one byte
	for magnitude > 0xff {
		size++
		magnitude >>= 8
	}
	return size, true
}

func absInt64(value int64) uint64 {
	if value >= 0 {
		return uint64(value)
	}
	return uint64(-(value + 1)) + 1
}

func compareDecimalMagnitude(digits []byte, value uint64) int {
	valueDigits := 1
	for rest := value; rest >= 10; rest /= 10 {
		valueDigits++
	}
	if len(digits) < valueDigits {
		return -1
	}
	if len(digits) > valueDigits {
		return 1
	}
	parsed := parseDecimalMagnitude(digits)
	switch {
	case parsed < value:
		return -1
	case parsed > value:
		return 1
	default:
		return 0
	}
}

func parseDecimalMagnitude(digits []byte) uint64 {
	var value uint64
	for _, digit := range digits {
		value = value*10 + uint64(digit-'0')
	}
	return value
}

type magnitudeCursor struct {
	sourceIndex int
	source      []byte
	delta       uint64
	carry       int
	operation   magnitudeOperation
}

func newMagnitudeCursor(
	source []byte,
	delta uint64,
	operation magnitudeOperation,
) magnitudeCursor {
	return magnitudeCursor{
		sourceIndex: len(source) - 1,
		source:      source,
		delta:       delta,
		operation:   operation,
	}
}

// next returns result digits from least to most significant.
func (cursor *magnitudeCursor) next() (byte, bool) {
	switch cursor.operation {
	case magnitudeDirect:
		if cursor.delta == 0 {
			return 0, false
		}
		digit := byte(cursor.delta % 10)
		cursor.delta /= 10
		return digit, true
	case magnitudeCopy:
		if cursor.sourceIndex < 0 {
			return 0, false
		}
		digit := cursor.source[cursor.sourceIndex] - '0'
		cursor.sourceIndex--
		return digit, true
	case magnitudeAdd:
		if cursor.sourceIndex < 0 && cursor.delta == 0 && cursor.carry == 0 {
			return 0, false
		}
		sum := cursor.carry
		if cursor.sourceIndex >= 0 {
			sum += int(cursor.source[cursor.sourceIndex] - '0')
			cursor.sourceIndex--
		}
		if cursor.delta != 0 {
			sum += int(cursor.delta % 10)
			cursor.delta /= 10
		}
		cursor.carry = sum / 10
		return byte(sum % 10), true
	default:
		if cursor.sourceIndex < 0 {
			return 0, false
		}
		difference := int(cursor.source[cursor.sourceIndex]-'0') - cursor.carry
		cursor.sourceIndex--
		if cursor.delta != 0 {
			difference -= int(cursor.delta % 10)
			cursor.delta /= 10
		}
		if difference < 0 {
			difference += 10
			cursor.carry = 1
		} else {
			cursor.carry = 0
		}
		return byte(difference), true
	}
}

func magnitudeMetadata(
	source []byte,
	delta uint64,
	operation magnitudeOperation,
) (digits int, value uint64, valueOK bool) {
	cursor := newMagnitudeCursor(source, delta, operation)
	place := uint64(1)
	position := 0
	for {
		digit, ok := cursor.next()
		if !ok {
			break
		}
		if digit != 0 {
			digits = position + 1
		}
		if position < 19 {
			value += uint64(digit) * place
			if position < 18 {
				place *= 10
			}
		}
		position++
	}
	return digits, value, digits <= 19
}

func appendMagnitude(dst []byte, adjusted adjustedExponent) []byte {
	start := len(dst)
	for i := 0; i < adjusted.digits; i++ {
		dst = append(dst, 0)
	}
	cursor := newMagnitudeCursor(adjusted.source, adjusted.delta, adjusted.operation)
	for position := 0; ; position++ {
		digit, ok := cursor.next()
		if !ok {
			break
		}
		if position < adjusted.digits {
			dst[start+adjusted.digits-1-position] = '0' + digit
		}
	}
	return dst
}

// AppendString appends decoded UTF-8 string bytes. Zero-zero terminates the
// component; embedded zero is escaped as zero-0xff. This is prefix-free and
// preserves byte order.
func AppendString(dst, decoded []byte, direction Direction) ([]byte, bool) {
	if direction > Descending || !utf8.Valid(decoded) {
		return dst, false
	}
	start := len(dst)
	dst = append(dst, tagString)
	for _, b := range decoded {
		dst = appendStringByte(dst, b)
	}
	dst = append(dst, 0, 0)
	invertDescending(dst[start:], direction)
	return dst, true
}

// StringEncodedSize reports the exact byte length AppendString would append.
// It permits callers to reject an impossible durable key without first
// allocating an encoding proportional to attacker-controlled input.
func StringEncodedSize(decoded []byte) (int, bool) {
	if !utf8.Valid(decoded) || len(decoded) > math.MaxInt-3 {
		return 0, false
	}
	size := 3 + len(decoded) // tag and two-byte terminator
	for _, b := range decoded {
		if b == 0 {
			if size == math.MaxInt {
				return 0, false
			}
			size++
		}
	}
	return size, true
}

// AppendJSONString decodes and appends one complete validated JSON string
// without a transient text buffer. It also fails closed on malformed input,
// making the storage boundary safe if a future caller violates the validated
// input contract.
func AppendJSONString(dst, raw []byte, direction Direction) ([]byte, bool) {
	if direction > Descending {
		return dst, false
	}
	start := len(dst)
	dst = append(dst, tagString)
	var ok bool
	dst, _, ok = appendJSONStringContent(dst, raw, true)
	if !ok {
		return dst[:start], false
	}
	dst = append(dst, 0, 0)
	invertDescending(dst[start:], direction)
	return dst, true
}

// JSONStringEncodedSize reports the exact byte length AppendJSONString would
// append. It validates and decodes only enough to count the ordered payload, so
// a caller can reject a large escaped key without first allocating its decoded
// text or its proportional ordered encoding.
func JSONStringEncodedSize(raw []byte) (int, bool) {
	_, contentBytes, ok := appendJSONStringContent(nil, raw, false)
	if !ok || contentBytes > math.MaxInt-3 {
		return 0, false
	}
	return contentBytes + 3, true
}

func appendJSONStringContent(
	dst, raw []byte,
	emit bool,
) ([]byte, int, bool) {
	if len(raw) < 2 || raw[0] != '"' ||
		raw[len(raw)-1] != '"' || !utf8.Valid(raw) {
		return dst, 0, false
	}
	encodedBytes := 0
	for i := 1; i < len(raw)-1; {
		b := raw[i]
		if b != '\\' {
			if b < 0x20 || b == '"' {
				return dst, 0, false
			}
			dst, encodedBytes = appendJSONStringByte(
				dst, b, encodedBytes, emit,
			)
			i++
			continue
		}
		i++
		if i >= len(raw)-1 {
			return dst, 0, false
		}
		switch raw[i] {
		case '"', '\\', '/':
			dst, encodedBytes = appendJSONStringByte(
				dst, raw[i], encodedBytes, emit,
			)
			i++
		case 'b':
			dst, encodedBytes = appendJSONStringByte(
				dst, '\b', encodedBytes, emit,
			)
			i++
		case 'f':
			dst, encodedBytes = appendJSONStringByte(
				dst, '\f', encodedBytes, emit,
			)
			i++
		case 'n':
			dst, encodedBytes = appendJSONStringByte(
				dst, '\n', encodedBytes, emit,
			)
			i++
		case 'r':
			dst, encodedBytes = appendJSONStringByte(
				dst, '\r', encodedBytes, emit,
			)
			i++
		case 't':
			dst, encodedBytes = appendJSONStringByte(
				dst, '\t', encodedBytes, emit,
			)
			i++
		case 'u':
			high, next, ok := decodeHex4(raw, i+1)
			if !ok {
				return dst, 0, false
			}
			i = next
			r := rune(high)
			if 0xd800 <= high && high <= 0xdbff {
				if i+2 > len(raw)-1 || raw[i] != '\\' || raw[i+1] != 'u' {
					return dst, 0, false
				}
				low, lowNext, lowOK := decodeHex4(raw, i+2)
				if !lowOK || low < 0xdc00 || low > 0xdfff {
					return dst, 0, false
				}
				r = utf16.DecodeRune(r, rune(low))
				i = lowNext
			} else if 0xdc00 <= high && high <= 0xdfff {
				return dst, 0, false
			}
			var encoded [utf8.UTFMax]byte
			n := utf8.EncodeRune(encoded[:], r)
			for _, out := range encoded[:n] {
				dst, encodedBytes = appendJSONStringByte(
					dst, out, encodedBytes, emit,
				)
			}
		default:
			return dst, 0, false
		}
	}
	return dst, encodedBytes, true
}

func appendJSONStringByte(
	dst []byte,
	value byte,
	encodedBytes int,
	emit bool,
) ([]byte, int) {
	encodedBytes++
	if value == 0 {
		encodedBytes++
	}
	if emit {
		dst = appendStringByte(dst, value)
	}
	return dst, encodedBytes
}

func appendStringByte(dst []byte, b byte) []byte {
	if b == 0 {
		return append(dst, 0, 0xff)
	}
	return append(dst, b)
}

func invertDescending(component []byte, direction Direction) {
	if direction == Descending {
		invert(component)
	}
}

func invert(value []byte) {
	for i := range value {
		value[i] = ^value[i]
	}
}

// numberParts returns normalized decimal geometry without materializing a
// digit buffer. first and last address the combined integer+fraction sequence;
// first < 0 denotes zero.
func numberParts(number []byte) (
	negative bool,
	integerDigits, first, last int,
	exponent decimalExponent,
	ok bool,
) {
	if !validNumber(number) {
		return false, 0, 0, 0, decimalExponent{}, false
	}
	i := 0
	if number[i] == '-' {
		negative = true
		i++
	}
	integerStart := i
	for i < len(number) && number[i] >= '0' && number[i] <= '9' {
		i++
	}
	integerDigits = i - integerStart
	digitIndex := 0
	first, last = -1, -1
	for j := integerStart; j < i; j++ {
		if number[j] != '0' {
			if first < 0 {
				first = digitIndex
			}
			last = digitIndex
		}
		digitIndex++
	}
	if i < len(number) && number[i] == '.' {
		i++
		for i < len(number) && number[i] >= '0' && number[i] <= '9' {
			if number[i] != '0' {
				if first < 0 {
					first = digitIndex
				}
				last = digitIndex
			}
			digitIndex++
			i++
		}
	}
	if i == len(number) {
		return negative, integerDigits, first, last, decimalExponent{}, true
	}
	i++ // e/E
	if number[i] == '+' || number[i] == '-' {
		exponent.negative = number[i] == '-'
		i++
	}
	for i < len(number) && number[i] == '0' {
		i++
	}
	if i < len(number) {
		exponent.digits = number[i:]
	} else {
		exponent.negative = false
	}
	return negative, integerDigits, first, last, exponent, true
}

func validNumber(number []byte) bool {
	if len(number) == 0 {
		return false
	}
	i := 0
	if number[i] == '-' {
		i++
		if i == len(number) {
			return false
		}
	}
	if number[i] == '0' {
		i++
		if i < len(number) && number[i] >= '0' && number[i] <= '9' {
			return false
		}
	} else {
		if number[i] < '1' || number[i] > '9' {
			return false
		}
		for i < len(number) && number[i] >= '0' && number[i] <= '9' {
			i++
		}
	}
	if i < len(number) && number[i] == '.' {
		i++
		start := i
		for i < len(number) && number[i] >= '0' && number[i] <= '9' {
			i++
		}
		if i == start {
			return false
		}
	}
	if i < len(number) && (number[i] == 'e' || number[i] == 'E') {
		i++
		if i < len(number) && (number[i] == '+' || number[i] == '-') {
			i++
		}
		start := i
		for i < len(number) && number[i] >= '0' && number[i] <= '9' {
			i++
		}
		if i == start {
			return false
		}
	}
	return i == len(number)
}

func decodeHex4(raw []byte, start int) (uint16, int, bool) {
	if start < 0 || start+4 > len(raw)-1 {
		return 0, start, false
	}
	var value uint16
	for _, b := range raw[start : start+4] {
		value <<= 4
		switch {
		case b >= '0' && b <= '9':
			value |= uint16(b - '0')
		case b >= 'a' && b <= 'f':
			value |= uint16(b-'a') + 10
		case b >= 'A' && b <= 'F':
			value |= uint16(b-'A') + 10
		default:
			return 0, start, false
		}
	}
	return value, start + 4, true
}
