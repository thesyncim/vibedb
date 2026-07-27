package orderedkey

import (
	"bytes"
	"fmt"
	"math"
	"math/big"
	"math/rand"
	"testing"
)

// The order contract proved here is the reason the package exists:
//
//	bytes.Compare(encode(a), encode(b)) == referenceCompare(a, b)
//
// It is proved by an all-pairs sweep over a generated corpus rather than by
// walking a hand-sorted list, because a hand-sorted list only ever exercises
// adjacent pairs the author already believed were ordered. The quadratic sweep
// is what catches a transitivity break, a cross-type inversion, or a boundary
// value nobody thought to place next to its neighbour.
//
// The reference for numbers is math/big.Rat, i.e. exact decimal value. That
// matches query/scalar.go's compareScalar, which orders numbers by exact
// decimal and never through float64, so 1, 1.0 and 1e0 are one value while
// 9007199254740992 and 9007199254740993 are two. Encoding through float64 would
// silently merge that last pair; the corpus includes it so the sweep would fail
// if anyone ever introduced a float64 path.

type testValue struct {
	kind Kind
	b    bool
	num  string // JSON number text, for KindNumber
	str  string // decoded content, for KindString
	rat  *big.Rat
}

func (v testValue) String() string {
	switch v.kind {
	case KindNull:
		return "null"
	case KindBool:
		return fmt.Sprintf("%v", v.b)
	case KindNumber:
		return v.num
	default:
		return fmt.Sprintf("%q", v.str)
	}
}

func num(t *testing.T, text string) testValue {
	t.Helper()
	r, ok := new(big.Rat).SetString(text)
	if !ok {
		t.Fatalf("reference cannot parse %q", text)
	}
	return testValue{kind: KindNumber, num: text, rat: r}
}

func str(s string) testValue { return testValue{kind: KindString, str: s} }

// referenceCompare is the independent total order the encoding must reproduce:
// null < false < true < number < string across families, matching the kind
// order in query/scalar.go (null < bool < number < string), and by exact value
// within a family.
func referenceCompare(a, b testValue) int {
	ra, rb := familyRank(a), familyRank(b)
	if ra != rb {
		return sign(ra - rb)
	}
	switch a.kind {
	case KindNull, KindBool:
		return 0 // family rank already separated false from true
	case KindNumber:
		return a.rat.Cmp(b.rat)
	default:
		return bytes.Compare([]byte(a.str), []byte(b.str))
	}
}

func familyRank(v testValue) int {
	switch v.kind {
	case KindNull:
		return 0
	case KindBool:
		if v.b {
			return 2
		}
		return 1
	case KindNumber:
		return 3
	default:
		return 4
	}
}

func sign(x int) int {
	switch {
	case x < 0:
		return -1
	case x > 0:
		return 1
	default:
		return 0
	}
}

func encodeValue(t *testing.T, dst []byte, v testValue, d Direction) []byte {
	t.Helper()
	var ok bool
	switch v.kind {
	case KindNull:
		dst, ok = AppendNull(dst, d)
	case KindBool:
		dst, ok = AppendBool(dst, v.b, d)
	case KindNumber:
		dst, ok = AppendNumber(dst, []byte(v.num), d)
	default:
		dst, ok = AppendString(dst, []byte(v.str), d)
	}
	if !ok {
		t.Fatalf("encode %s", v)
	}
	return dst
}

// corpus spans every family and the boundary values where an ordering bug
// hides: the sign transition, the exponent transition, digit-count changes,
// magnitudes float64 cannot separate, the empty string, strings that are
// prefixes of one another, and strings differing only in the 0x00 byte the
// composite terminator and its escape are built from.
func corpus(t *testing.T) []testValue {
	t.Helper()
	values := []testValue{
		{kind: KindNull},
		{kind: KindBool, b: false},
		{kind: KindBool, b: true},
	}

	numbers := []string{
		"0", "-0", "0.0", "0e999", "0.000",
		"1", "1.0", "1e0", "0.1e1", "10e-1",
		"-1", "-1.0", "-1e0",
		"2", "-2", "9", "-9", "10", "-10", "11", "-11",
		"99", "100", "101", "-99", "-100", "-101",
		"0.1", "0.2", "0.9", "0.01", "0.09", "0.001",
		"-0.1", "-0.2", "-0.9", "-0.01", "-0.001",
		"1.5", "-1.5", "1.05", "-1.05", "1.4999", "1.5001",
		// int64 boundaries, and the neighbours that a signed-int encoding gets
		// wrong if it forgets to flip the sign bit.
		"9223372036854775807", "9223372036854775806", "9223372036854775808",
		"-9223372036854775808", "-9223372036854775807", "-9223372036854775809",
		// Adjacent integers that collide once rounded through float64. Their
		// separation is the proof the encoder stays exact-decimal.
		"9007199254740992", "9007199254740993", "9007199254740994",
		"99999999999999999", "100000000000000000", "100000000000000001",
		"12345678901234567", "12345678901234568",
		// Beyond any fixed-width integer entirely.
		"170141183460469231731687303715884105728",
		"-170141183460469231731687303715884105728",
		"1e100", "-1e100", "1e-100", "-1e-100",
		"1e308", "1e309", "1e-323", "-1e308", "-1e309",
		"1.7976931348623157e308", "5e-324", "2.5e-324",
		"0.30000000000000004", "0.3", "0.1000000000000000055511151231257827",
	}
	for _, n := range numbers {
		values = append(values, num(t, n))
	}

	strings := []string{
		"", "\x00", "\x00\x00", "\x00\x00\x00", "\x01", "\ufffd",
		"a", "a\x00", "a\x00\x00", "a\x00b", "a\x00\x01", "a\x01", "aa", "ab", "b",
		"abc", "abcd", "abcde", "abd",
		"A", "Z", "z", "~",
		"é", "€", "😀", "日本語",
		"tenant", "tenant\x00", "tenant0", "tenants",
	}
	for _, s := range strings {
		values = append(values, str(s))
	}

	// Deterministic fill so the sweep covers combinations nobody enumerated.
	rng := rand.New(rand.NewSource(20260725))
	for i := 0; i < 420; i++ {
		digits := 1 + rng.Intn(20)
		var sb []byte
		if rng.Intn(2) == 0 {
			sb = append(sb, '-')
		}
		sb = append(sb, byte('1'+rng.Intn(9)))
		for j := 1; j < digits; j++ {
			sb = append(sb, byte('0'+rng.Intn(10)))
		}
		if rng.Intn(2) == 0 {
			sb = append(sb, '.')
			for j := 0; j < 1+rng.Intn(8); j++ {
				sb = append(sb, byte('0'+rng.Intn(10)))
			}
		}
		if rng.Intn(3) == 0 {
			sb = append(sb, 'e')
			if rng.Intn(2) == 0 {
				sb = append(sb, '-')
			}
			sb = append(sb, []byte(fmt.Sprint(rng.Intn(200)))...)
		}
		values = append(values, num(t, string(sb)))
	}
	alphabet := []byte{0x00, 0x01, 'a', 'b', 0x7f, 0xc3}
	for i := 0; i < 260; i++ {
		n := rng.Intn(5)
		var sb []byte
		for j := 0; j < n; j++ {
			c := alphabet[rng.Intn(len(alphabet))]
			if c == 0xc3 { // keep the corpus valid UTF-8
				sb = append(sb, 0xc3, 0xa9)
				continue
			}
			sb = append(sb, c)
		}
		values = append(values, str(string(sb)))
	}
	return values
}

func TestScalarOrderMatchesReferenceAllPairs(t *testing.T) {
	values := corpus(t)
	for _, d := range []Direction{Ascending, Descending} {
		encoded := make([][]byte, len(values))
		for i, v := range values {
			encoded[i] = encodeValue(t, nil, v, d)
		}
		flip := 1
		if d == Descending {
			flip = -1
		}
		for i := range values {
			for j := range values {
				want := flip * sign(referenceCompare(values[i], values[j]))
				got := sign(bytes.Compare(encoded[i], encoded[j]))
				if got != want {
					t.Fatalf("direction=%v %s vs %s: got %d want %d\n%x\n%x",
						d, values[i], values[j], got, want, encoded[i], encoded[j])
				}
			}
		}
	}
}

// Equal values must encode to identical bytes, not merely to bytes that compare
// equal. Index lookup and GROUP BY both key on the encoded bytes, so distinct
// spellings of one value have to collapse to one key.
func TestEqualValuesEncodeIdenticalBytes(t *testing.T) {
	values := corpus(t)
	encoded := make([][]byte, len(values))
	for i, v := range values {
		encoded[i] = encodeValue(t, nil, v, Ascending)
	}
	for i := range values {
		for j := range values {
			if referenceCompare(values[i], values[j]) != 0 {
				continue
			}
			if !bytes.Equal(encoded[i], encoded[j]) {
				t.Fatalf("%s == %s but keys differ:\n%x\n%x",
					values[i], values[j], encoded[i], encoded[j])
			}
		}
	}
}

// compositeReference orders composites lexicographically by component, applying
// each component's own direction. This is the semantics a multi-column index
// with mixed ASC/DESC must deliver.
func compositeReference(a, b []testValue, dirs []Direction) int {
	for i := range a {
		c := sign(referenceCompare(a[i], b[i]))
		if dirs[i] == Descending {
			c = -c
		}
		if c != 0 {
			return c
		}
	}
	return 0
}

func compositeCorpus(t *testing.T) []testValue {
	t.Helper()
	return []testValue{
		{kind: KindNull},
		{kind: KindBool, b: false},
		{kind: KindBool, b: true},
		num(t, "-1e100"), num(t, "-1"), num(t, "0"), num(t, "1"), num(t, "1e100"),
		num(t, "9007199254740992"), num(t, "9007199254740993"),
		str(""), str("\x00"), str("a"), str("a\x00"), str("a\x00b"), str("aa"), str("ab"),
	}
}

// The composite sweep is where a terminator-free or length-prefixed scheme
// fails. ("a","bc") and ("ab","c") must not collide, and ("a", x) must sort
// before ("aa", y) for every x and y, which naive concatenation of unterminated
// strings gets wrong. Components that are prefixes of one another are therefore
// deliberately over-represented in the corpus.
func TestCompositeOrderMatchesReferenceAllPairs(t *testing.T) {
	values := compositeCorpus(t)
	dirSets := [][]Direction{
		{Ascending, Ascending},
		{Ascending, Descending},
		{Descending, Ascending},
		{Descending, Descending},
	}
	for _, dirs := range dirSets {
		var tuples [][]testValue
		var keys [][]byte
		for _, a := range values {
			for _, b := range values {
				tuple := []testValue{a, b}
				key := encodeValue(t, nil, a, dirs[0])
				key = encodeValue(t, key, b, dirs[1])
				tuples = append(tuples, tuple)
				keys = append(keys, key)
			}
		}
		for i := range tuples {
			for j := range tuples {
				want := compositeReference(tuples[i], tuples[j], dirs)
				got := sign(bytes.Compare(keys[i], keys[j]))
				if got != want {
					t.Fatalf("dirs=%v (%s,%s) vs (%s,%s): got %d want %d\n%x\n%x",
						dirs, tuples[i][0], tuples[i][1], tuples[j][0], tuples[j][1],
						got, want, keys[i], keys[j])
				}
			}
		}
	}
}

func TestThreeComponentOrderMatchesReference(t *testing.T) {
	values := []testValue{
		{kind: KindNull}, {kind: KindBool, b: true},
		num(t, "-1"), num(t, "0"), num(t, "10"),
		str(""), str("a"), str("a\x00"), str("aa"),
	}
	dirSets := [][]Direction{
		{Ascending, Ascending, Ascending},
		{Ascending, Descending, Ascending},
		{Descending, Ascending, Descending},
	}
	for _, dirs := range dirSets {
		var tuples [][]testValue
		var keys [][]byte
		for _, a := range values {
			for _, b := range values {
				for _, c := range values {
					key := encodeValue(t, nil, a, dirs[0])
					key = encodeValue(t, key, b, dirs[1])
					key = encodeValue(t, key, c, dirs[2])
					tuples = append(tuples, []testValue{a, b, c})
					keys = append(keys, key)
				}
			}
		}
		for i := range tuples {
			for j := range tuples {
				want := compositeReference(tuples[i], tuples[j], dirs)
				got := sign(bytes.Compare(keys[i], keys[j]))
				if got != want {
					t.Fatalf("dirs=%v %v vs %v: got %d want %d",
						dirs, tuples[i], tuples[j], got, want)
				}
			}
		}
	}
}

// A prefix range must contain exactly the keys carrying that prefix. This is
// simultaneously the range-query bound and the shard boundary, so an off-by-one
// here would both drop rows and misroute them.
func TestPrefixRangeContainsExactlyMatchingKeys(t *testing.T) {
	values := compositeCorpus(t)
	for _, p := range values {
		prefix := encodeValue(t, nil, p, Ascending)
		end, ok := AppendPrefixEnd(nil, prefix)
		if !ok {
			t.Fatalf("no successor for %s", p)
		}
		for _, a := range values {
			for _, b := range values {
				key := encodeValue(t, nil, a, Ascending)
				key = encodeValue(t, key, b, Ascending)
				inRange := bytes.Compare(key, prefix) >= 0 && bytes.Compare(key, end) < 0
				hasPrefix := bytes.HasPrefix(key, prefix)
				if inRange != hasPrefix {
					t.Fatalf("prefix %s key (%s,%s): inRange=%v hasPrefix=%v",
						p, a, b, inRange, hasPrefix)
				}
			}
		}
	}
}

// jsonSpelling renders a decoded string as JSON source, escaping the characters
// JSON requires plus every control byte, so the escaped and raw entry points can
// be compared on the same content.
func jsonSpelling(s string) []byte {
	out := []byte{'"'}
	for _, r := range s {
		switch {
		case r == '"':
			out = append(out, '\\', '"')
		case r == '\\':
			out = append(out, '\\', '\\')
		case r < 0x20:
			out = append(out, []byte(fmt.Sprintf(`\u%04x`, r))...)
		default:
			out = append(out, []byte(string(r))...)
		}
	}
	return append(out, '"')
}

// AppendJSONString must be indistinguishable from decoding the JSON text and
// calling AppendString. Two entry points that disagreed would put the same
// logical string at two places in the index, so the all-pairs order proof for
// AppendString carries over to AppendJSONString only because of this identity.
func TestJSONStringEncodingMatchesDecodedString(t *testing.T) {
	for _, v := range corpus(t) {
		if v.kind != KindString {
			continue
		}
		for _, d := range []Direction{Ascending, Descending} {
			want := encodeValue(t, nil, v, d)
			got, ok := AppendJSONString(nil, jsonSpelling(v.str), d)
			if !ok || !bytes.Equal(got, want) {
				t.Fatalf("%s (%s): got %x ok=%v want %x", v, jsonSpelling(v.str), got, ok, want)
			}
		}
	}
}

// The variable-length exponent changes length at every power-of-256 boundary,
// in both exponent signs, and independently for positive and negative numbers
// (which complement the whole exponent). Those boundaries are where a varint
// whose length is not order-carrying breaks: a 2-byte exponent must still sort
// after every 1-byte one for a positive number, and before every 1-byte one for
// a negative number.
//
// math/big cannot be the reference here — 10^(9e18) is not materializable — so
// the reference is structural instead. For a single-digit mantissa d and
// exponent E the value is d*10^E, so among positive numbers the order is
// exactly the order of the pair (E, d), and among negative numbers it is the
// reverse. That is an independent statement about the values, not a restatement
// of the encoding.
type expValue struct {
	negative bool
	exponent int64
	digit    int
	text     string
}

func expCorpus() []expValue {
	var boundaries []int64
	for _, b := range []int64{
		0, 1, 2, 9, 10, 255, 256, 257, 65535, 65536, 65537,
		1 << 23, 1<<24 - 1, 1 << 24, 1<<32 - 1, 1 << 32, 1<<40 - 1, 1 << 40,
		1<<48 - 1, 1 << 48, 1<<56 - 1, 1 << 56,
		math.MaxInt64 - 2, math.MaxInt64 - 1, math.MaxInt64,
	} {
		boundaries = append(boundaries, b)
		if b > 0 {
			boundaries = append(boundaries, -b)
		}
	}
	var out []expValue
	for _, e := range boundaries {
		for _, d := range []int{1, 5, 9} {
			for _, neg := range []bool{false, true} {
				text := fmt.Sprintf("%de%d", d, e)
				if neg {
					text = "-" + text
				}
				out = append(out, expValue{negative: neg, exponent: e, digit: d, text: text})
			}
		}
	}
	return out
}

// expReference orders by value: negatives below positives, positives ascending
// in (exponent, digit), negatives descending in the same pair.
func expReference(a, b expValue) int {
	if a.negative != b.negative {
		if a.negative {
			return -1
		}
		return 1
	}
	c := 0
	switch {
	case a.exponent < b.exponent:
		c = -1
	case a.exponent > b.exponent:
		c = 1
	default:
		c = sign(a.digit - b.digit)
	}
	if a.negative {
		c = -c
	}
	return c
}

func TestExponentLengthBoundariesOrderAllPairs(t *testing.T) {
	all := expCorpus()
	for _, d := range []Direction{Ascending, Descending} {
		var kept []expValue
		var keys [][]byte
		for _, v := range all {
			// Exponents near the int64 limit overflow the adjusted exponent and
			// are refused by design; they simply do not take part.
			key, ok := AppendNumber(nil, []byte(v.text), d)
			if !ok {
				continue
			}
			kept = append(kept, v)
			keys = append(keys, key)
		}
		if len(kept) < 100 {
			t.Fatalf("corpus collapsed to %d values", len(kept))
		}
		flip := 1
		if d == Descending {
			flip = -1
		}
		for i := range kept {
			for j := range kept {
				want := flip * sign(expReference(kept[i], kept[j]))
				got := sign(bytes.Compare(keys[i], keys[j]))
				if got != want {
					t.Fatalf("direction=%v %s vs %s: got %d want %d\n%x\n%x",
						d, kept[i].text, kept[j].text, got, want, keys[i], keys[j])
				}
			}
		}
	}
}

// Every exponent length must survive a round trip, including the negative-number
// case where the exponent is complemented on top of the direction.
func TestExponentLengthBoundariesRoundTrip(t *testing.T) {
	for _, d := range []Direction{Ascending, Descending} {
		for _, v := range expCorpus() {
			key, ok := AppendNumber(nil, []byte(v.text), d)
			if !ok {
				continue
			}
			c, out, next, err := DecodeComponent(nil, key, 0)
			if err != nil {
				t.Fatalf("%s: %v", v.text, err)
			}
			if next != len(key) || c.Kind != KindNumber {
				t.Fatalf("%s: next=%d len=%d kind=%v", v.text, next, len(key), c.Kind)
			}
			again, ok := AppendNumber(nil, out[c.PayloadStart:c.PayloadEnd], d)
			if !ok || !bytes.Equal(again, key) {
				t.Fatalf("%s: decoded %q did not re-encode", v.text, out[c.PayloadStart:c.PayloadEnd])
			}
		}
	}
}

// A non-minimal exponent payload would give one value two keys, so an equality
// probe built from the canonical key could miss rows stored under the padded
// one. The decoder must refuse it rather than silently accept both.
func TestDecodeRejectsNonMinimalExponent(t *testing.T) {
	cases := [][]byte{
		{tagNumber, tagNumberPositive, expHeaderZero + 2, 0x00, 0x01, 2, 0},
		{tagNumber, tagNumberPositive, expHeaderZero + 1, 0x00, 2, 0},
		{tagNumber, tagNumberPositive, expHeaderZero + 9, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 0},
		// Negative exponent with a padded (complemented) leading zero byte.
		{tagNumber, tagNumberPositive, expHeaderZero - 2, 0xff, 0xfe, 2, 0},
		// Positive exponent whose payload sets the int64 sign bit.
		{tagNumber, tagNumberPositive, expHeaderZero + 8, 0xff, 0, 0, 0, 0, 0, 0, 1, 2, 0},
	}
	for _, key := range cases {
		if _, _, _, err := DecodeComponent(nil, key, 0); err == nil {
			t.Fatalf("accepted non-canonical exponent %x", key)
		}
	}
}
