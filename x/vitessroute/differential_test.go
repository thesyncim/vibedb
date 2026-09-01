// Authoritative Vitess differential harness for the vitessroute adapter.
//
// These tests import real upstream Vitess (vitess.io/vitess, pinned in this
// module's go.mod) ONLY from _test.go files. No shipped adapter .go file imports
// Vitess; the root module carries no Vitess dependency. The design names v0.22.0
// as the reference release; the harness pins v0.24.2, whose xxhash/multicol/cfc
// vindex code is byte-identical to v0.22.0 (v0.22.0 cannot compile under this
// repo's Go 1.26 toolchain — the swissmap GOEXPERIMENT was removed). Each test
// computes the UPSTREAM destination by driving the genuine
// go/vt/vtgate/vindexes Vindex (via CreateVindex + Map/Hash over sqltypes
// values) and the ADAPTER destination via the shipped Stage 1 code, then
// asserts exact equality of points and ranges.
//
// A differential mismatch is a STOP CONDITION: it means the adapter diverged
// from the pinned upstream. Fix the adapter; never relax the assertion.

package vitessroute

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	"github.com/thesyncim/vibedb/distribution"
)

// ----------------------------------------------------------------------------
// Deterministic input corpus
// ----------------------------------------------------------------------------

// scalarInput is one corpus value with independent ground truth for both sides:
// a raw string, or an exact integer with a possibly non-canonical adapter
// spelling. The adapter consumes the spelling; the upstream side is built from
// the concrete integer, so a serialization bug cannot hide behind a shared
// derivation.
type scalarInput struct {
	isStr bool
	s     string // when isStr: raw bytes

	spell string // when number: JSON number spelling fed to the adapter
	useU  bool   // when number: value is u (unsigned, > int64 max)
	i     int64
	u     uint64
}

func strIn(s string) scalarInput              { return scalarInput{isStr: true, s: s} }
func intIn(spell string, i int64) scalarInput { return scalarInput{spell: spell, i: i} }
func uintIn(spell string, u uint64) scalarInput {
	return scalarInput{spell: spell, useU: true, u: u}
}

func (in scalarInput) adapterScalar(t *testing.T) distribution.Scalar {
	t.Helper()
	if in.isStr {
		return distribution.NewString(in.s)
	}
	s, err := distribution.NewNumber(in.spell)
	if err != nil {
		t.Fatalf("distribution.NewNumber(%q): %v", in.spell, err)
	}
	return s
}

func (in scalarInput) upstreamValue() sqltypes.Value {
	if in.isStr {
		return sqltypes.NewVarChar(in.s)
	}
	if in.useU {
		return sqltypes.NewUint64(in.u)
	}
	return sqltypes.NewInt64(in.i)
}

// canonicalDecimal is the minimal decimal ASCII the ground-truth integer must
// serialize to. It is computed with strconv, independent of the adapter, so it
// is a trustworthy expectation for the golden artifact.
func (in scalarInput) canonicalDecimal() string {
	if in.useU {
		return strconv.FormatUint(in.u, 10)
	}
	return strconv.FormatInt(in.i, 10)
}

// stringCorpus exercises the raw-UTF-8-bytes rule: empty, embedded NUL, unicode,
// invalid UTF-8, whitespace/control, long, and digit-lookalike strings (a string
// "12345" must hash identically to the integer 12345, since both serialize to
// the same bytes).
func stringCorpus() []string {
	return []string{
		"",
		"tenant-42",
		"tenant-0000123",
		"channel",
		"0",
		"12345",
		"18446744073709551615",
		"a\x00b",
		"\x00",
		"a\x00\x00c",
		"café",
		"日本語",
		"Ω≈ç√∫",
		"emoji-🍣-😀",
		"naïve",
		"tab\there",
		"line\nbreak",
		"\xff\xfe\x00\x80",
		strings.Repeat("x", 300),
		strings.Repeat("\x00", 8),
	}
}

// numberCorpus exercises the decimal-ASCII-for-integers rule and exact-number
// spelling equivalence: an integer never hashes as its binary form, and "12345",
// "1.2345e4", "12345.0", "123450e-1" all denote 12345 and must hash identically
// to the integer 12345.
func numberCorpus() []scalarInput {
	return []scalarInput{
		intIn("0", 0),
		intIn("-0", 0),
		intIn("0.0", 0),
		intIn("0e100", 0),
		intIn("1", 1),
		intIn("-1", -1),
		intIn("42", 42),
		intIn("12345", 12345),
		intIn("1.2345e4", 12345),
		intIn("12345.0", 12345),
		intIn("123450e-1", 12345),
		intIn("-12345", -12345),
		intIn("5", 5),
		intIn("5.0", 5),
		intIn("5e0", 5),
		intIn("50e-1", 5),
		intIn("10000000000", 10000000000),
		intIn("1000000000000", 1000000000000),
		intIn("9223372036854775807", math.MaxInt64),
		intIn("-9223372036854775808", math.MinInt64),
		intIn("9223372036854775806", math.MaxInt64-1),
		uintIn("9223372036854775808", uint64(math.MaxInt64)+1),
		uintIn("18446744073709551615", math.MaxUint64),
		uintIn("18446744073709551614", math.MaxUint64-1),
		uintIn("1.8446744073709551615e19", math.MaxUint64),
	}
}

// ----------------------------------------------------------------------------
// Upstream drivers (real Vitess)
// ----------------------------------------------------------------------------

func newUpstreamXXHash(t *testing.T) vindexes.SingleColumn {
	t.Helper()
	v, err := vindexes.CreateVindex("xxhash", "xxhash", nil)
	if err != nil {
		t.Fatalf("upstream CreateVindex(xxhash): %v", err)
	}
	sc, ok := v.(vindexes.SingleColumn)
	if !ok {
		t.Fatalf("upstream xxhash is not a SingleColumn: %T", v)
	}
	return sc
}

func upstreamXXHash(t *testing.T, sc vindexes.SingleColumn, val sqltypes.Value) distribution.DestinationSet {
	t.Helper()
	dests, err := sc.Map(context.Background(), nil, []sqltypes.Value{val})
	if err != nil {
		t.Fatalf("upstream xxhash Map(%v): %v", val, err)
	}
	if len(dests) != 1 {
		t.Fatalf("upstream xxhash Map returned %d destinations, want 1", len(dests))
	}
	return convertDest(t, dests[0])
}

func newUpstreamMultiCol(t *testing.T, count int, columnBytes string) vindexes.MultiColumn {
	t.Helper()
	params := map[string]string{
		"column_count":  strconv.Itoa(count),
		"column_vindex": strings.TrimRight(strings.Repeat("xxhash,", count), ","),
	}
	if columnBytes != "" {
		params["column_bytes"] = columnBytes
	}
	v, err := vindexes.CreateVindex("multicol", "multicol", params)
	if err != nil {
		t.Fatalf("upstream CreateVindex(multicol, count=%d, bytes=%q): %v", count, columnBytes, err)
	}
	mc, ok := v.(vindexes.MultiColumn)
	if !ok {
		t.Fatalf("upstream multicol is not a MultiColumn: %T", v)
	}
	return mc
}

func upstreamMultiCol(t *testing.T, mc vindexes.MultiColumn, row []sqltypes.Value) distribution.DestinationSet {
	t.Helper()
	dests, err := mc.Map(context.Background(), nil, [][]sqltypes.Value{row})
	if err != nil {
		t.Fatalf("upstream multicol Map(%v): %v", row, err)
	}
	if len(dests) != 1 {
		t.Fatalf("upstream multicol Map returned %d destinations, want 1", len(dests))
	}
	return convertDest(t, dests[0])
}

// convertDest normalizes an upstream key.ShardDestination into the adapter's
// distribution.DestinationSet vocabulary. A DestinationKeyspaceID is a point (it
// must be exactly 8 bytes in this fixed-width profile); a DestinationKeyRange is
// a half-open range whose variable-length prefix bytes are right-zero-padded to
// 8 bytes and whose empty End means the open maximum (KeyspaceEnd.Max).
func convertDest(t *testing.T, d key.ShardDestination) distribution.DestinationSet {
	t.Helper()
	switch v := d.(type) {
	case key.DestinationKeyspaceID:
		if len(v) != distribution.KeyspaceWidth {
			t.Fatalf("upstream keyspace id width = %d bytes (%x), want %d", len(v), []byte(v), distribution.KeyspaceWidth)
		}
		var p distribution.KeyspacePoint
		copy(p[:], v)
		return distribution.DestinationSet{Points: []distribution.KeyspacePoint{p}}
	case key.DestinationKeyRange:
		kr := v.KeyRange
		if len(kr.Start) == 0 || len(kr.Start) >= distribution.KeyspaceWidth {
			t.Fatalf("upstream range start width = %d bytes, want 1..%d", len(kr.Start), distribution.KeyspaceWidth-1)
		}
		var start distribution.KeyspacePoint
		copy(start[:], kr.Start)
		end := distribution.KeyspaceEnd{}
		if len(kr.End) == 0 {
			end.Max = true
		} else {
			var ep distribution.KeyspacePoint
			copy(ep[:], kr.End)
			end.Point = ep
		}
		return distribution.DestinationSet{Ranges: []distribution.KeyRange{{Start: start, End: end}}}
	default:
		t.Fatalf("unexpected upstream destination type %T (%v)", d, d)
		return distribution.DestinationSet{}
	}
}

// ----------------------------------------------------------------------------
// Adapter drivers
// ----------------------------------------------------------------------------

// adapterMultiColMapper builds the adapter's multicol mapper through the strict
// VSchema loader using the same params fed to upstream, so the differential also
// proves the config loader's column-byte distribution matches getColumnBytes.
func adapterMultiColMapper(t *testing.T, count int, columnBytes string) distribution.Mapper {
	t.Helper()
	cols := make([]string, count)
	for i := range cols {
		cols[i] = fmt.Sprintf("c%d", i)
	}
	params := map[string]any{
		"column_count":  strconv.Itoa(count),
		"column_vindex": strings.TrimRight(strings.Repeat("xxhash,", count), ","),
	}
	if columnBytes != "" {
		params["column_bytes"] = columnBytes
	}
	vs := map[string]any{
		"sharded": true,
		"vindexes": map[string]any{
			"mc": map[string]any{"type": "multicol", "params": params},
		},
		"tables": map[string]any{
			"t": map[string]any{
				"column_vindexes": []any{
					map[string]any{"columns": cols, "name": "mc"},
				},
			},
		},
	}
	data := mustJSON(t, vs)
	ks, err := LoadKeyspace(data)
	if err != nil {
		t.Fatalf("adapter LoadKeyspace(count=%d, bytes=%q): %v", count, columnBytes, err)
	}
	m, err := ks.Mapper("t")
	if err != nil {
		t.Fatalf("adapter Mapper(t): %v", err)
	}
	return m
}

func adapterScalars(t *testing.T, ins []scalarInput) []distribution.Scalar {
	t.Helper()
	out := make([]distribution.Scalar, len(ins))
	for i, in := range ins {
		out[i] = in.adapterScalar(t)
	}
	return out
}

func upstreamRow(ins []scalarInput) []sqltypes.Value {
	out := make([]sqltypes.Value, len(ins))
	for i, in := range ins {
		out[i] = in.upstreamValue()
	}
	return out
}

// ----------------------------------------------------------------------------
// Differential tests
// ----------------------------------------------------------------------------

func TestDifferentialXXHashStrings(t *testing.T) {
	up := newUpstreamXXHash(t)
	m := NewXXHashMapper()
	for _, s := range stringCorpus() {
		got, err := m.MapPrefix([]distribution.Scalar{distribution.NewString(s)})
		if err != nil {
			t.Fatalf("adapter MapPrefix(string %q): %v", s, err)
		}
		want := upstreamXXHash(t, up, sqltypes.NewVarChar(s))
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("xxhash mismatch for string %q:\n adapter=%s\nupstream=%s", s, showDest(got), showDest(want))
		}
	}
}

func TestDifferentialXXHashNumbers(t *testing.T) {
	up := newUpstreamXXHash(t)
	m := NewXXHashMapper()
	for _, in := range numberCorpus() {
		got, err := m.MapPrefix([]distribution.Scalar{in.adapterScalar(t)})
		if err != nil {
			t.Fatalf("adapter MapPrefix(number %q): %v", in.spell, err)
		}
		want := upstreamXXHash(t, up, in.upstreamValue())
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("xxhash mismatch for number spelling %q (=%s):\n adapter=%s\nupstream=%s",
				in.spell, in.canonicalDecimal(), showDest(got), showDest(want))
		}
	}
}

// multicolConfigs are the fixed-8-byte configurations this profile supports,
// including explicit and defaulted column_bytes across 1..8 columns.
func multicolConfigs() []struct {
	count int
	bytes string
} {
	return []struct {
		count int
		bytes string
	}{
		{1, "8"}, {1, ""},
		{2, "4,4"}, {2, "2,6"}, {2, "6,2"}, {2, "1,7"}, {2, "7,1"}, {2, "3,5"}, {2, ""},
		{3, "2,2,4"}, {3, "2,3,3"}, {3, "3,3,2"}, {3, "1,1,6"}, {3, ""},
		{4, "2,2,2,2"}, {4, "1,1,1,5"}, {4, ""},
		{5, ""}, {6, ""}, {7, ""}, {8, ""},
	}
}

// multicolRows returns deterministic full-key rows (length count) mixing
// strings, integers, empty, unicode, and 64-bit boundaries across columns.
func multicolRows(count int) [][]scalarInput {
	palette := []scalarInput{
		strIn("tenant-42"),
		intIn("12345", 12345),
		strIn("channel-7"),
		intIn("0", 0),
		strIn("日本語"),
		uintIn("18446744073709551615", math.MaxUint64),
		strIn(""),
		intIn("-9223372036854775808", math.MinInt64),
	}
	ints := []scalarInput{
		intIn("1", 1), intIn("67890", 67890), intIn("-1", -1), intIn("9223372036854775807", math.MaxInt64),
		intIn("1000000000000", 1000000000000), intIn("42", 42), intIn("0", 0), intIn("7", 7),
	}
	strs := []scalarInput{
		strIn("a"), strIn("bucket-3"), strIn("region-eu"), strIn("café"),
		strIn("\x00"), strIn("Ω≈ç√"), strIn("zone-1"), strIn("rack"),
	}
	rows := [][]scalarInput{
		palette[:count],
		ints[:count],
		strs[:count],
	}
	// One alternating string/int row and one empty/zero edge row.
	mixed := make([]scalarInput, count)
	edge := make([]scalarInput, count)
	for i := 0; i < count; i++ {
		if i%2 == 0 {
			mixed[i] = strs[i]
		} else {
			mixed[i] = ints[i]
		}
		if i == 0 {
			edge[i] = strIn("")
		} else {
			edge[i] = intIn("0", 0)
		}
	}
	rows = append(rows, mixed, edge)
	return rows
}

func TestDifferentialMultiCol(t *testing.T) {
	for _, cfg := range multicolConfigs() {
		cfg := cfg
		name := fmt.Sprintf("count=%d/bytes=%s", cfg.count, orDefault(cfg.bytes))
		t.Run(name, func(t *testing.T) {
			mc := newUpstreamMultiCol(t, cfg.count, cfg.bytes)
			m := adapterMultiColMapper(t, cfg.count, cfg.bytes)
			for _, full := range multicolRows(cfg.count) {
				// Every supported leading prefix length 1..count: full key -> point,
				// shorter prefix -> range.
				for l := 1; l <= cfg.count; l++ {
					prefix := full[:l]
					got, err := m.MapPrefix(adapterScalars(t, prefix))
					if err != nil {
						t.Fatalf("adapter MapPrefix(prefix len %d): %v", l, err)
					}
					want := upstreamMultiCol(t, mc, upstreamRow(prefix))
					if !reflect.DeepEqual(got, want) {
						t.Fatalf("multicol mismatch (%s, prefix len %d, row %v):\n adapter=%s\nupstream=%s",
							name, l, describeRow(prefix), showDest(got), showDest(want))
					}
				}
			}
		})
	}
}

// TestDifferentialPrefixRange authoritatively pins the prefix-to-range logic
// (NewKeyRangeFromPrefix/addOne) directly, independent of hashing, including the
// trailing-0xff carry and the all-0xff overflow to the open maximum end.
func TestDifferentialPrefixRange(t *testing.T) {
	var prefixes [][]byte
	// Crafted edge prefixes of every length 1..7.
	for n := 1; n <= distribution.KeyspaceWidth-1; n++ {
		allFF := make([]byte, n)
		for i := range allFF {
			allFF[i] = 0xff
		}
		prefixes = append(prefixes, allFF) // overflow -> Max

		trailFF := make([]byte, n)
		trailFF[0] = 0x40
		for i := 1; i < n; i++ {
			trailFF[i] = 0xff
		}
		prefixes = append(prefixes, trailFF) // carry across a trailing 0xff run

		mid := make([]byte, n)
		for i := range mid {
			mid[i] = byte(0x10 + i)
		}
		prefixes = append(prefixes, mid) // plain increment of the last byte

		zero := make([]byte, n) // all-zero prefix
		prefixes = append(prefixes, zero)
	}
	prefixes = append(prefixes,
		[]byte{0x00, 0xff},
		[]byte{0xff, 0x00},
		[]byte{0x12, 0x34, 0x56},
		[]byte{0xfe, 0xff, 0xff},
	)

	for _, prefix := range prefixes {
		got := distribution.DestinationSet{Ranges: []distribution.KeyRange{prefixRange(clone(prefix))}}
		want := convertDest(t, vindexes.NewKeyRangeFromPrefix(clone(prefix)))
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("prefixRange(%x) mismatch:\n adapter=%s\nupstream=%s", prefix, showDest(got), showDest(want))
		}
	}
}

// Authoritative probe anchors: the xxhash 8-byte keyspace id, dumped as hex.
const (
	anchorTenant42 = "404f5103a9b9fbf9"
	anchorInt12345 = "b64fd60addd2f2c6"
)

// TestXXHashAnchors verifies the two authoritative probe anchors directly: the
// keyspace id (the little-endian encoding of the xxhash Sum64, i.e. the point
// bytes in order) equals the reported hex, proving the decimal-ASCII-for-
// integers rule (the integer 12345 hashes exactly like the string "12345").
func TestXXHashAnchors(t *testing.T) {
	m := NewXXHashMapper()

	dsStr, err := m.MapPrefix([]distribution.Scalar{distribution.NewString("tenant-42")})
	if err != nil {
		t.Fatal(err)
	}
	if got := hex.EncodeToString(dsStr.Points[0][:]); got != anchorTenant42 {
		t.Fatalf(`xxhash("tenant-42") keyspace id = %s, want %s`, got, anchorTenant42)
	}

	n, err := distribution.NewNumber("12345")
	if err != nil {
		t.Fatal(err)
	}
	dsInt, err := m.MapPrefix([]distribution.Scalar{n})
	if err != nil {
		t.Fatal(err)
	}
	if got := hex.EncodeToString(dsInt.Points[0][:]); got != anchorInt12345 {
		t.Fatalf("xxhash(int 12345) keyspace id = %s, want %s", got, anchorInt12345)
	}

	// The integer 12345 and the string "12345" must hash identically (same bytes).
	dsStr12345, err := m.MapPrefix([]distribution.Scalar{distribution.NewString("12345")})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(dsInt, dsStr12345) {
		t.Fatalf("int 12345 and string \"12345\" hashed differently: %s vs %s", showDest(dsInt), showDest(dsStr12345))
	}
}

// TestMultiColProfileRejectsSubEightWidth confirms the profile narrowing: a
// column_bytes spec that upstream accepts but whose total is below 8 bytes (so
// upstream would emit a non-8-byte keyspace id) is rejected by the loader.
func TestMultiColProfileRejectsSubEightWidth(t *testing.T) {
	// Upstream accepts count=2, column_bytes="2,2" and produces a 4-byte ksid.
	mc := newUpstreamMultiCol(t, 2, "2,2")
	dests, err := mc.Map(context.Background(), nil, [][]sqltypes.Value{{sqltypes.NewVarChar("a"), sqltypes.NewVarChar("b")}})
	if err != nil {
		t.Fatalf("upstream multicol Map: %v", err)
	}
	ksid, ok := dests[0].(key.DestinationKeyspaceID)
	if !ok || len(ksid) != 4 {
		t.Fatalf("expected upstream to emit a 4-byte keyspace id, got %T len=%d", dests[0], len(ksid))
	}
	// The adapter loader must reject the same config: the fixed profile is 8 bytes.
	_, lerr := LoadKeyspace(mustJSON(t, map[string]any{
		"sharded": true,
		"vindexes": map[string]any{
			"mc": map[string]any{"type": "multicol", "params": map[string]any{
				"column_count": "2", "column_bytes": "2,2", "column_vindex": "xxhash,xxhash",
			}},
		},
		"tables": map[string]any{
			"t": map[string]any{"column_vindexes": []any{map[string]any{"columns": []string{"c0", "c1"}, "name": "mc"}}},
		},
	}))
	if !errors.Is(lerr, ErrUnsupportedVSchema) {
		t.Fatalf("loader accepted a sub-8-byte multicol config; err = %v, want ErrUnsupportedVSchema", lerr)
	}
}

// ----------------------------------------------------------------------------
// Fuzz: invalid numeric conversions and boundary widths
// ----------------------------------------------------------------------------

// FuzzXXHashString proves adapter/upstream parity for arbitrary raw string
// bytes, non-circularly (each side derives from the same string independently).
func FuzzXXHashString(f *testing.F) {
	for _, s := range stringCorpus() {
		f.Add(s)
	}
	m := NewXXHashMapper()
	f.Fuzz(func(t *testing.T, s string) {
		got, err := m.MapPrefix([]distribution.Scalar{distribution.NewString(s)})
		if err != nil {
			t.Fatalf("adapter rejected string %q: %v", s, err)
		}
		up := newUpstreamXXHash(t)
		want := upstreamXXHash(t, up, sqltypes.NewVarChar(s))
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("xxhash string mismatch for %q:\n adapter=%s\nupstream=%s", s, showDest(got), showDest(want))
		}
	})
}

// FuzzXXHashInt64 proves adapter/upstream parity for signed 64-bit integers: the
// adapter renders the value's canonical decimal spelling and upstream serializes
// an INT64, and the two hashes must agree.
func FuzzXXHashInt64(f *testing.F) {
	for _, v := range []int64{0, 1, -1, 42, 12345, -12345, math.MaxInt64, math.MinInt64, 1 << 40} {
		f.Add(v)
	}
	m := NewXXHashMapper()
	f.Fuzz(func(t *testing.T, v int64) {
		n, err := distribution.NewNumber(strconv.FormatInt(v, 10))
		if err != nil {
			t.Fatalf("NewNumber(%d): %v", v, err)
		}
		got, err := m.MapPrefix([]distribution.Scalar{n})
		if err != nil {
			t.Fatalf("adapter rejected int %d: %v", v, err)
		}
		up := newUpstreamXXHash(t)
		want := upstreamXXHash(t, up, sqltypes.NewInt64(v))
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("xxhash int mismatch for %d:\n adapter=%s\nupstream=%s", v, showDest(got), showDest(want))
		}
	})
}

// FuzzXXHashUint64 covers the unsigned range beyond int64 max.
func FuzzXXHashUint64(f *testing.F) {
	for _, v := range []uint64{0, 1, math.MaxInt64, uint64(math.MaxInt64) + 1, math.MaxUint64, 1 << 63} {
		f.Add(v)
	}
	m := NewXXHashMapper()
	f.Fuzz(func(t *testing.T, v uint64) {
		n, err := distribution.NewNumber(strconv.FormatUint(v, 10))
		if err != nil {
			t.Fatalf("NewNumber(%d): %v", v, err)
		}
		got, err := m.MapPrefix([]distribution.Scalar{n})
		if err != nil {
			t.Fatalf("adapter rejected uint %d: %v", v, err)
		}
		up := newUpstreamXXHash(t)
		want := upstreamXXHash(t, up, sqltypes.NewUint64(v))
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("xxhash uint mismatch for %d:\n adapter=%s\nupstream=%s", v, showDest(got), showDest(want))
		}
	})
}

// FuzzNumericAdmission fuzzes invalid numeric conversions: for any spelling that
// is a well-formed number scalar, the xxhash mapper must either return exactly
// one 8-byte point (admitted lossless integer) or a typed ErrInvalidShardValue
// (fractional / out-of-domain). It must never panic and must be deterministic.
func FuzzNumericAdmission(f *testing.F) {
	seeds := []string{
		"0", "-0", "12345", "1.2345e4", "5.0", "50e-1", "1e1000", "1e-1", "3.14",
		"-9223372036854775808", "9223372036854775807", "18446744073709551615",
		"18446744073709551616", "9999999999999999999999999", "0e100", "-1", "100",
		"1.5", "0.0001", "2e-3", "123456789012345678901234567890",
	}
	for _, s := range seeds {
		f.Add(s)
	}
	m := NewXXHashMapper()
	f.Fuzz(func(t *testing.T, spelling string) {
		num, err := distribution.NewNumber(spelling)
		if err != nil {
			return // not a well-formed number scalar at all
		}
		ds, err := m.MapPrefix([]distribution.Scalar{num})
		if err != nil {
			if !errors.Is(err, distribution.ErrInvalidShardValue) {
				t.Fatalf("MapPrefix(%q) error = %v, want ErrInvalidShardValue", spelling, err)
			}
			return
		}
		if len(ds.Points) != 1 || len(ds.Ranges) != 0 {
			t.Fatalf("admitted number %q produced %d points / %d ranges, want 1 point", spelling, len(ds.Points), len(ds.Ranges))
		}
		ds2, err2 := m.MapPrefix([]distribution.Scalar{num})
		if err2 != nil || !reflect.DeepEqual(ds, ds2) {
			t.Fatalf("non-deterministic mapping for %q", spelling)
		}
	})
}

// FuzzMultiColPrefixRange fuzzes boundary widths: for any prefix of length 1..7,
// the adapter's prefixRange must equal upstream NewKeyRangeFromPrefix, including
// every carry and the all-0xff overflow.
func FuzzMultiColPrefixRange(f *testing.F) {
	seeds := [][]byte{
		{0x00}, {0x40}, {0xff}, {0xff, 0xff}, {0x12, 0x34}, {0x00, 0xff}, {0xff, 0x00},
		{0x40, 0x80, 0xc0}, {0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07},
	}
	for _, s := range seeds {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, prefix []byte) {
		if len(prefix) < 1 || len(prefix) > distribution.KeyspaceWidth-1 {
			return // adapter prefixRange domain is a partial multicol key: 1..7 bytes
		}
		got := distribution.DestinationSet{Ranges: []distribution.KeyRange{prefixRange(clone(prefix))}}
		want := convertDest(t, vindexes.NewKeyRangeFromPrefix(clone(prefix)))
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("prefixRange(%x) mismatch:\n adapter=%s\nupstream=%s", prefix, showDest(got), showDest(want))
		}
	})
}

// ----------------------------------------------------------------------------
// Small shared helpers
// ----------------------------------------------------------------------------

func clone(b []byte) []byte { return append([]byte(nil), b...) }

func orDefault(s string) string {
	if s == "" {
		return "default"
	}
	return s
}

func describeRow(ins []scalarInput) []string {
	out := make([]string, len(ins))
	for i, in := range ins {
		if in.isStr {
			out[i] = fmt.Sprintf("str(%q)", in.s)
		} else {
			out[i] = fmt.Sprintf("int(%s)", in.canonicalDecimal())
		}
	}
	return out
}

func showDest(d distribution.DestinationSet) string {
	var b strings.Builder
	for _, p := range d.Points {
		fmt.Fprintf(&b, "point(%x) ", p[:])
	}
	for _, r := range d.Ranges {
		end := "MAX"
		if !r.End.Max {
			end = fmt.Sprintf("%x", r.End.Point[:])
		}
		fmt.Fprintf(&b, "range(%x-%s) ", r.Start[:], end)
	}
	if b.Len() == 0 {
		return "<empty>"
	}
	return strings.TrimSpace(b.String())
}
