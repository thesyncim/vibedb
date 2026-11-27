// Package jsoncmp provides allocation-free RFC 8259 JSON number equality and
// streaming unescaped byte iteration over validated JSON string interiors.
package jsoncmp

import (
	"bytes"
	"unicode/utf16"
	"unicode/utf8"

	"github.com/thesyncim/vibejson/document"
)

// RawJSONStringEqual reports whether two validated raw JSON string tokens
// (including enclosing quotes) decode to identical UTF-8 byte sequences.
// When neither token has document.TapeFlagEscaped set, it compares raw bytes
// directly in one pass without decoding.
func RawJSONStringEqual(a []byte, aFlags uint8, b []byte, bFlags uint8) bool {
	if (aFlags|bFlags)&document.TapeFlagEscaped == 0 {
		return bytes.Equal(a, b)
	}
	if len(a) < 2 || len(b) < 2 {
		return false
	}
	ia := JSONStringByteIter{Raw: a[1 : len(a)-1]}
	ib := JSONStringByteIter{Raw: b[1 : len(b)-1]}
	for {
		ba, oka := ia.Next()
		bb, okb := ib.Next()
		if oka != okb {
			return false
		}
		if !oka {
			return true
		}
		if ba != bb {
			return false
		}
	}
}

// JSONStringByteIter streams the decoded UTF-8 bytes of a validated JSON
// string interior (without enclosing quotes) with zero heap allocations.
type JSONStringByteIter struct {
	Raw []byte
	pos int
	buf [4]byte
	bi  uint8
	bn  uint8
}

// Next returns the next decoded UTF-8 byte, or (0, false) at end of string.
func (it *JSONStringByteIter) Next() (byte, bool) {
	if it.bi < it.bn {
		b := it.buf[it.bi]
		it.bi++
		return b, true
	}
	if it.pos >= len(it.Raw) {
		return 0, false
	}
	c := it.Raw[it.pos]
	if c != '\\' {
		it.pos++
		return c, true
	}
	it.pos++
	if it.pos >= len(it.Raw) {
		return 0, false
	}
	esc := it.Raw[it.pos]
	it.pos++
	switch esc {
	case '"', '\\', '/':
		return esc, true
	case 'b':
		return '\b', true
	case 'f':
		return '\f', true
	case 'n':
		return '\n', true
	case 'r':
		return '\r', true
	case 't':
		return '\t', true
	case 'u':
		if it.pos+4 > len(it.Raw) {
			return 0, false
		}
		r1, ok := parseHex4(it.Raw[it.pos : it.pos+4])
		if !ok {
			return 0, false
		}
		it.pos += 4
		r := rune(r1)
		if utf16.IsSurrogate(r) && it.pos+6 <= len(it.Raw) &&
			it.Raw[it.pos] == '\\' && it.Raw[it.pos+1] == 'u' {
			if r2, ok2 := parseHex4(it.Raw[it.pos+2 : it.pos+6]); ok2 {
				dec := utf16.DecodeRune(r, rune(r2))
				if dec != utf8.RuneError {
					it.pos += 6
					r = dec
				}
			}
		}
		n := utf8.EncodeRune(it.buf[:], r)
		it.bi = 1
		it.bn = uint8(n)
		return it.buf[0], true
	default:
		return esc, true
	}
}

func parseHex4(b []byte) (uint16, bool) {
	var v uint16
	for i := 0; i < 4; i++ {
		c := b[i]
		var d uint16
		switch {
		case c >= '0' && c <= '9':
			d = uint16(c - '0')
		case c >= 'a' && c <= 'f':
			d = uint16(c-'a') + 10
		case c >= 'A' && c <= 'F':
			d = uint16(c-'A') + 10
		default:
			return 0, false
		}
		v = v<<4 | d
	}
	return v, true
}

// JSONNumberEqual reports whether two valid RFC 8259 JSON number tokens
// represent the exact same mathematical rational value, with zero allocations
// and arbitrary precision (e.g. -0 == 0, 1e2 == 100, 1.50 == 1.5).
func JSONNumberEqual(a, b []byte) bool {
	if len(a) == len(b) {
		same := true
		for i := range a {
			if a[i] != b[i] {
				same = false
				break
			}
		}
		if same {
			return len(a) > 0
		}
	}
	an, aok := parseCanonNum(a)
	bn, bok := parseCanonNum(b)
	if !aok || !bok {
		return false
	}
	if an.zero && bn.zero {
		return true
	}
	if an.zero != bn.zero || an.neg != bn.neg || an.exp != bn.exp || an.sigLen != bn.sigLen {
		return false
	}
	for i := 0; i < an.sigLen; i++ {
		if an.digitAt(a, i) != bn.digitAt(b, i) {
			return false
		}
	}
	return true
}

type canonNum struct {
	neg      bool
	zero     bool
	exp      int64 // position of most-significant non-zero digit (10^exp)
	sigLen   int   // count of significant digits from first to last non-zero digit inclusive
	firstIdx int   // index in raw of first non-zero digit
	dotIdx   int   // index of '.' in raw, or -1
}

func (c canonNum) digitAt(raw []byte, i int) byte {
	pos := c.firstIdx + i
	if c.dotIdx >= 0 && c.firstIdx < c.dotIdx && pos >= c.dotIdx {
		pos++
	}
	return raw[pos]
}

func parseCanonNum(raw []byte) (canonNum, bool) {
	if len(raw) == 0 {
		return canonNum{}, false
	}
	i := 0
	neg := false
	if raw[i] == '-' {
		neg = true
		i++
		if i >= len(raw) {
			return canonNum{}, false
		}
	}
	intStart := i
	for i < len(raw) && raw[i] >= '0' && raw[i] <= '9' {
		i++
	}
	intEnd := i
	if intEnd == intStart {
		return canonNum{}, false
	}
	dotIdx := -1
	fracStart, fracEnd := i, i
	if i < len(raw) && raw[i] == '.' {
		dotIdx = i
		i++
		fracStart = i
		for i < len(raw) && raw[i] >= '0' && raw[i] <= '9' {
			i++
		}
		fracEnd = i
		if fracEnd == fracStart {
			return canonNum{}, false
		}
	}
	var expVal int64
	if i < len(raw) && (raw[i] == 'e' || raw[i] == 'E') {
		i++
		if i >= len(raw) {
			return canonNum{}, false
		}
		expNeg := false
		if raw[i] == '+' {
			i++
		} else if raw[i] == '-' {
			expNeg = true
			i++
		}
		if i >= len(raw) || raw[i] < '0' || raw[i] > '9' {
			return canonNum{}, false
		}
		for i < len(raw) && raw[i] >= '0' && raw[i] <= '9' {
			d := int64(raw[i] - '0')
			if expVal < 1<<50 {
				expVal = expVal*10 + d
			}
			i++
		}
		if expNeg {
			expVal = -expVal
		}
	}
	if i != len(raw) {
		return canonNum{}, false
	}

	// Find first and last non-zero digit across int + frac.
	firstIdx := -1
	lastIdx := -1
	var firstPos int64 // relative to ones place (0 = units digit)
	for j := intStart; j < intEnd; j++ {
		if raw[j] != '0' {
			if firstIdx < 0 {
				firstIdx = j
				firstPos = int64(intEnd - 1 - j)
			}
			lastIdx = j
		}
	}
	for j := fracStart; j < fracEnd; j++ {
		if raw[j] != '0' {
			if firstIdx < 0 {
				firstIdx = j
				firstPos = -int64(j - fracStart + 1)
			}
			lastIdx = j
		}
	}
	if firstIdx < 0 {
		return canonNum{zero: true}, true
	}
	sigLen := lastIdx - firstIdx + 1
	if dotIdx >= 0 && firstIdx < dotIdx && lastIdx > dotIdx {
		sigLen--
	}
	return canonNum{
		neg:      neg,
		exp:      firstPos + expVal,
		sigLen:   sigLen,
		firstIdx: firstIdx,
		dotIdx:   dotIdx,
	}, true
}
