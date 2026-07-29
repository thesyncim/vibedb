package orderedkey

import (
	"errors"
	"strconv"
	"unicode/utf8"
)

// Decoding and level-wise component access.
//
// A composite key is the concatenation of independently encoded components.
// Every component is self-delimiting in a single forward pass: null and bool
// are one tag byte, a string ends at the unescaped 0x00,0x00 terminator, and a
// number ends at its digit terminator after a self-delimiting exponent. Nothing
// is back-loaded into a trailer, so finding component k costs a scan of
// components 0..k-1 and never requires decoding them into values. This is the
// property Leapfrog Triejoin needs: seek(x) and next() operate at one level by
// comparing that level's byte span, and the span is discoverable without
// interpreting the levels below it.
//
// Direction is recoverable from the component itself rather than from a schema
// the reader must carry. A descending component is the bitwise complement of
// the ascending one, so its tag byte is the complement of an ascending tag; the
// ascending tags {0x10,0x20,0x21,0x30,0x40} and their complements
// {0xef,0xdf,0xde,0xcf,0xbf} are disjoint sets, so one tag byte identifies both
// the family and the direction unambiguously. Scanning then proceeds in the
// ascending domain by XOR-ing every byte with a mask of 0x00 (ascending) or
// 0xff (descending), which is why the complement does not interact badly with
// terminator detection: the terminator and the escape are complemented along
// with the payload and stay distinguishable from each other, because they
// differ in their second byte (0x00,0x00 versus 0x00,0xff ascending, and
// 0xff,0xff versus 0xff,0x00 descending).

// Kind is a decoded component's scalar family. The values mirror the cross-type
// order the keys themselves encode, so comparing kinds compares families.
type Kind uint8

const (
	KindNull Kind = iota
	KindBool
	KindNumber
	KindString
)

var (
	// ErrMalformedKey reports a key that is not the output of this encoder.
	// Decoding is a storage boundary that may see torn or corrupted pages, so
	// every scan rejects rather than panics or reads out of bounds.
	ErrMalformedKey = errors.New("orderedkey: malformed key")
	// ErrKeyTruncated reports a component that runs past the end of the key.
	ErrKeyTruncated = errors.New("orderedkey: truncated key")
)

// Component describes one decoded component. Payload names the span appended to
// the caller's buffer: decoded UTF-8 content for a string, canonical decimal
// text for a number, and an empty span otherwise. Reporting a span instead of a
// slice keeps the decoder append-style and lets the caller keep one buffer for
// a whole key without a second copy of the data.
type Component struct {
	Kind         Kind
	Bool         bool
	Descending   bool
	PayloadStart int
	PayloadEnd   int
}

// maskAt classifies the tag at off, returning the ascending-domain tag and the
// XOR mask that maps this component's bytes into the ascending domain.
func maskAt(key []byte, off int) (tag, mask byte, err error) {
	if off < 0 || off >= len(key) {
		return 0, 0, ErrKeyTruncated
	}
	switch t := key[off]; t {
	case tagNull, tagFalse, tagTrue, tagNumber, tagString:
		return t, 0x00, nil
	case ^tagNull, ^tagFalse, ^tagTrue, ^tagNumber, ^tagString:
		return ^t, 0xff, nil
	default:
		return 0, 0, ErrMalformedKey
	}
}

// numberSpan locates a number's parts starting at the sign byte. digitsStart and
// digitsEnd bound the encoded digit run, excluding the terminator. The exponent
// is variable-length, so its extent is discovered by decoding its header rather
// than by skipping a fixed width.
func numberSpan(
	key []byte,
	i int,
	mask byte,
) (adjusted decodedExponent, digitsStart, digitsEnd, end int, err error) {
	if i >= len(key) {
		return decodedExponent{}, 0, 0, 0, ErrKeyTruncated
	}
	sign := key[i] ^ mask
	i++
	if sign == tagNumberZero {
		return decodedExponent{}, i, i, i, nil
	}
	if sign != tagNumberPositive && sign != tagNumberNegative {
		return decodedExponent{}, 0, 0, 0, ErrMalformedKey
	}
	negative := sign == tagNumberNegative
	// A negative number carries a complemented exponent on top of whatever the
	// direction already applied, so the two complements compose into one mask.
	expMask := mask
	term := byte(0)
	if negative {
		expMask ^= 0xff
		term = 11
	}
	adjusted, i, err = decodeExponent(key, i, expMask)
	if err != nil {
		return decodedExponent{}, 0, 0, 0, err
	}
	digitsStart = i
	for i < len(key) {
		b := key[i] ^ mask
		if b == term {
			return adjusted, digitsStart, i, i + 1, nil
		}
		if b < 1 || b > 10 {
			return decodedExponent{}, 0, 0, 0, ErrMalformedKey
		}
		i++
	}
	return decodedExponent{}, 0, 0, 0, ErrKeyTruncated
}

// DecodeComponent decodes the component at key[off:], appending its payload to
// dst. It returns the component, the grown buffer, and the offset of the next
// component.
//
// Numbers round-trip by value, not by spelling: the encoder canonicalizes, so
// decoding a key built from "1.0" or "1e0" yields "1". Re-encoding any decoded
// number reproduces the same key bytes, which is the property callers rely on.
func DecodeComponent(dst, key []byte, off int) (Component, []byte, int, error) {
	tag, mask, err := maskAt(key, off)
	if err != nil {
		return Component{}, dst, 0, err
	}
	c := Component{Descending: mask == 0xff, PayloadStart: len(dst), PayloadEnd: len(dst)}
	switch tag {
	case tagNull:
		return c, dst, off + 1, nil
	case tagFalse, tagTrue:
		c.Kind = KindBool
		c.Bool = tag == tagTrue
		return c, dst, off + 1, nil
	case tagString:
		c.Kind = KindString
		out, next, err := decodeString(dst, key, off+1, mask)
		if err != nil {
			return Component{}, dst[:len(dst)], 0, err
		}
		// AppendString only ever encodes valid UTF-8, so content that is not
		// valid UTF-8 did not come from this encoder. Rejecting it keeps the
		// set decode accepts equal to the set encode can produce, which is what
		// makes "anything that decodes re-encodes to the same bytes" true.
		if !utf8.Valid(out[c.PayloadStart:]) {
			return Component{}, dst[:len(dst)], 0, ErrMalformedKey
		}
		c.PayloadEnd = len(out)
		return c, out, next, nil
	default:
		c.Kind = KindNumber
		out, next, err := decodeNumber(dst, key, off+1, mask)
		if err != nil {
			return Component{}, dst[:len(dst)], 0, err
		}
		c.PayloadEnd = len(out)
		return c, out, next, nil
	}
}

func decodeString(dst, key []byte, i int, mask byte) ([]byte, int, error) {
	for i < len(key) {
		b := key[i] ^ mask
		if b != 0 {
			dst = append(dst, b)
			i++
			continue
		}
		if i+1 >= len(key) {
			return dst, 0, ErrKeyTruncated
		}
		switch key[i+1] ^ mask {
		case 0x00:
			return dst, i + 2, nil
		case 0xff:
			dst = append(dst, 0)
			i += 2
		default:
			return dst, 0, ErrMalformedKey
		}
	}
	return dst, 0, ErrKeyTruncated
}

// decodeNumber rebuilds canonical decimal text. The encoded form is a sign, an
// order-preserving adjusted exponent, and the significant digits with leading
// and trailing zeros already stripped, so the value is 0.<digits> * 10^adjusted.
func decodeNumber(dst, key []byte, i int, mask byte) ([]byte, int, error) {
	if i >= len(key) {
		return dst, 0, ErrKeyTruncated
	}
	sign := key[i] ^ mask
	if sign == tagNumberZero {
		return append(dst, '0'), i + 1, nil
	}
	exponent, digitsStart, digitsEnd, end, err := numberSpan(key, i, mask)
	if err != nil {
		return dst, 0, err
	}
	negative := sign == tagNumberNegative

	n := digitsEnd - digitsStart
	if n == 0 {
		// A non-zero sign with no digits has no decimal reading.
		return dst, 0, ErrMalformedKey
	}
	digit := func(j int) byte {
		b := key[digitsStart+j] ^ mask
		if negative {
			return '0' + (10 - b)
		}
		return '0' + (b - 1)
	}
	// The encoder strips leading and trailing zeros, so a canonical key never
	// carries them; rejecting them here keeps decode total on corrupted input
	// instead of emitting text that re-encodes to different bytes.
	if digit(0) == '0' || digit(n-1) == '0' {
		return dst, 0, ErrMalformedKey
	}

	if negative {
		dst = append(dst, '-')
	}
	if exponent.wide {
		// 0.<digits> contributes no exponent adjustment when it is encoded,
		// so this compact scientific spelling reproduces even an exponent far
		// outside any machine integer without arithmetic or a temporary copy.
		dst = append(dst, '0', '.')
		for j := 0; j < n; j++ {
			dst = append(dst, digit(j))
		}
		dst = append(dst, 'e')
		if exponent.negative {
			dst = append(dst, '-')
		}
		for j := exponent.digitsStart; j < exponent.digitsEnd; j++ {
			dst = append(dst, key[j]^exponent.digitMask)
		}
		return dst, end, nil
	}
	adjusted := exponent.compact
	switch {
	case adjusted >= int64(n) && adjusted <= 21:
		// Plain integer with trailing zeros: 0.125*10^5 is 12500.
		for j := 0; j < n; j++ {
			dst = append(dst, digit(j))
		}
		for j := int64(n); j < adjusted; j++ {
			dst = append(dst, '0')
		}
	case adjusted > 0 && adjusted < int64(n):
		a := int(adjusted)
		for j := 0; j < a; j++ {
			dst = append(dst, digit(j))
		}
		dst = append(dst, '.')
		for j := a; j < n; j++ {
			dst = append(dst, digit(j))
		}
	case adjusted <= 0 && adjusted >= -6:
		dst = append(dst, '0', '.')
		for j := int64(0); j < -adjusted; j++ {
			dst = append(dst, '0')
		}
		for j := 0; j < n; j++ {
			dst = append(dst, digit(j))
		}
	default:
		// Scientific notation keeps extreme exponents compact. The spelling is
		// "0.<digits>e<adjusted>" rather than the more usual
		// "<d0>.<rest>e<adjusted-1>" precisely so the exponent written out is
		// adjusted itself: re-encoding "0.D" contributes a delta of zero, so no
		// ±1 is needed, including at the machine-integer boundaries.
		dst = append(dst, '0', '.')
		for j := 0; j < n; j++ {
			dst = append(dst, digit(j))
		}
		dst = append(dst, 'e')
		dst = strconv.AppendInt(dst, adjusted, 10)
	}
	return dst, end, nil
}
