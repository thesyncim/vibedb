package storeio

import (
	"encoding/binary"
	"math"
	"strconv"
	"testing"
)

func barePrefixIntegerDescriptor(
	t testing.TB, count int, first, step int64,
) compactStreamView {
	t.Helper()
	data := make([]byte, 18)
	data[0] = 2
	binary.LittleEndian.PutUint64(data[2:], uint64(first))
	binary.LittleEndian.PutUint64(data[10:], uint64(step))
	binaryStream, err := (compactStreamEncoding{
		kind: compactStreamPrefixInt, count: count, data: data,
		dict: [][]byte{nil, nil},
	}).appendBinary(nil)
	if err != nil {
		t.Fatalf("append bare descriptor: %v", err)
	}
	view, err := openCompactStream(binaryStream)
	if err != nil {
		t.Fatalf("open bare descriptor: %v", err)
	}
	return view
}

func prefixIntegerDescriptor(
	t testing.TB, count int, first, step int64,
	prefix, suffix string, width int,
) compactStreamView {
	t.Helper()
	data := make([]byte, 18)
	data[0] = 2
	if width != 0 {
		data[0], data[1] = 3, byte(width)
	}
	binary.LittleEndian.PutUint64(data[2:], uint64(first))
	binary.LittleEndian.PutUint64(data[10:], uint64(step))
	binaryStream, err := (compactStreamEncoding{
		kind: compactStreamPrefixInt, count: count, data: data,
		dict: [][]byte{[]byte(prefix), []byte(suffix)},
	}).appendBinary(nil)
	if err != nil {
		t.Fatalf("append prefix descriptor: %v", err)
	}
	view, err := openCompactStream(binaryStream)
	if err != nil {
		t.Fatalf("open prefix descriptor: %v", err)
	}
	return view
}

func prefixIntegerSpellingValues(
	count int, first, step int64, prefix, suffix string, width int,
) [][]byte {
	values := make([][]byte, count)
	for row := range values {
		digits := strconv.AppendInt(nil, first+int64(row)*step, 10)
		if width > len(digits) {
			padded := make([]byte, width)
			for at := range padded[:width-len(digits)] {
				padded[at] = '0'
			}
			copy(padded[width-len(digits):], digits)
			digits = padded
		}
		value := append([]byte(prefix), digits...)
		values[row] = append(value, suffix...)
	}
	return values
}

func TestCompactBarePrefixIntegerNumericEquality(t *testing.T) {
	values := make([][]byte, 4)
	for row, value := range []uint64{
		999999999999999997,
		999999999999999998,
		999999999999999999,
		1000000000000000000,
	} {
		values[row] = strconv.AppendUint(nil, value, 10)
	}
	encoded, ok := encodeCompactPrefixInt(values)
	if !ok {
		t.Fatal("bare arithmetic prefix integer rejected")
	}
	view := compactCodecRoundTrip(t, encoded, values)
	if view.kind != compactStreamPrefixInt || view.data[0] != 2 ||
		len(view.data) != 18 || len(view.dictData) != 0 {
		t.Fatalf("bare arithmetic descriptor = kind=%d flags=%d data=%d dict=%d",
			view.kind, view.data[0], len(view.data), len(view.dictData))
	}
	for _, test := range []struct {
		needle int64
		want   int
	}{
		{needle: 999999999999999997, want: 1},
		{needle: 999999999999999999, want: 1},
		{needle: 1000000000000000000, want: 1},
		{needle: 999999999999999996, want: 0},
		{needle: -1, want: 0},
		{needle: math.MaxInt64, want: 0},
	} {
		got, supported := view.countIntegerEqual(test.needle)
		if !supported || got != test.want {
			t.Fatalf("integer needle=%d count=%d supported=%v want=%d",
				test.needle, got, supported, test.want)
		}
	}
	for _, test := range []struct {
		needle      string
		needleInt   int64
		needleIsInt bool
		want        int
	}{
		{needle: "999999999999999999", needleInt: 999999999999999999, needleIsInt: true, want: 1},
		{needle: "1e18", needleInt: 1000000000000000000, needleIsInt: true, want: 1},
		{needle: "1000000000000000000.0", needleInt: 1000000000000000000, needleIsInt: true, want: 1},
		{needle: "-0", needleInt: 0, needleIsInt: true, want: 0},
		{needle: "-1", needleInt: -1, needleIsInt: true, want: 0},
		{needle: "999999999999999999.5", needleInt: 0, needleIsInt: false, want: 0},
		{needle: "1e100", needleInt: 0, needleIsInt: false, want: 0},
	} {
		got, _, _, supported := view.countNumberEqual(
			[]byte(test.needle), test.needleInt, test.needleIsInt, nil, nil,
		)
		if !supported || got != test.want {
			t.Fatalf("number needle=%s count=%d supported=%v want=%d",
				test.needle, got, supported, test.want)
		}
	}
	fixedValues := [][]byte{
		[]byte("10000"), []byte("10001"), []byte("10002"), []byte("10003"),
	}
	fixedEncoded, ok := encodeCompactPrefixInt(fixedValues)
	if !ok {
		t.Fatal("fixed-width bare prefix integer rejected")
	}
	fixedView := compactCodecRoundTrip(t, fixedEncoded, fixedValues)
	if fixedView.data[0] != 3 || fixedView.data[1] != 5 {
		t.Fatalf("fixed-width bare descriptor flags=%d width=%d", fixedView.data[0], fixedView.data[1])
	}
	if got, supported := fixedView.countIntegerEqual(10002); !supported || got != 1 {
		t.Fatalf("fixed-width integer count=%d supported=%v", got, supported)
	}
	if got, _, _, supported := fixedView.countNumberEqual(
		[]byte("1.0002e4"), 10002, true, nil, nil,
	); !supported || got != 1 {
		t.Fatalf("fixed-width number count=%d supported=%v", got, supported)
	}
}

func TestCompactBarePrefixIntegerEqualityRejectsOtherForms(t *testing.T) {
	forms := []struct {
		name   string
		values [][]byte
	}{
		{
			name: "quoted",
			values: [][]byte{
				[]byte(`"1"`), []byte(`"2"`), []byte(`"3"`), []byte(`"4"`),
			},
		},
		{
			name: "affixed",
			values: [][]byte{
				[]byte("id:1"), []byte("id:2"), []byte("id:3"), []byte("id:4"),
			},
		},
		{
			name: "padded",
			values: [][]byte{
				[]byte("001"), []byte("002"), []byte("003"), []byte("004"),
			},
		},
		{
			name: "packed",
			values: [][]byte{
				[]byte("0"), []byte("1"), []byte("3"), []byte("6"),
				[]byte("10"), []byte("15"), []byte("21"), []byte("28"),
			},
		},
	}
	for _, form := range forms {
		t.Run(form.name, func(t *testing.T) {
			encoded, ok := encodeCompactPrefixInt(form.values)
			if !ok {
				t.Fatal("prefix integer form rejected")
			}
			view := compactCodecRoundTrip(t, encoded, form.values)
			if got, supported := view.countIntegerEqual(2); supported || got != 0 {
				t.Fatalf("integer count=%d supported=%v for kind=%d flags=%d",
					got, supported, view.kind, view.data[0])
			}
			if got, _, _, supported := view.countNumberEqual(
				[]byte("2"), 2, true, nil, nil,
			); supported || got != 0 {
				t.Fatalf("number count=%d supported=%v for kind=%d flags=%d",
					got, supported, view.kind, view.data[0])
			}
		})
	}
}

func TestCompactBarePrefixIntegerEqualityDomainGuards(t *testing.T) {
	valid := []struct {
		name   string
		count  int
		first  int64
		step   int64
		needle int64
		want   int
	}{
		{name: "empty", count: 0, first: 7, step: math.MinInt64, needle: 7, want: 0},
		{name: "singleton", count: 1, first: math.MaxInt64, step: math.MinInt64, needle: math.MaxInt64, want: 1},
		{name: "constant", count: 4, first: 7, step: 0, needle: 7, want: 4},
		{name: "descending", count: 6, first: 10, step: -2, needle: 4, want: 1},
		{name: "upper-bound", count: 2, first: math.MaxInt64 - 1, step: 1, needle: math.MaxInt64, want: 1},
	}
	for _, test := range valid {
		t.Run(test.name, func(t *testing.T) {
			view := barePrefixIntegerDescriptor(t, test.count, test.first, test.step)
			got, supported := view.countIntegerEqual(test.needle)
			if !supported || got != test.want {
				t.Fatalf("count=%d first=%d step=%d got=%d supported=%v want=%d",
					test.count, test.first, test.step, got, supported, test.want)
			}
		})
	}
	for _, test := range []struct {
		name  string
		count int
		first int64
		step  int64
	}{
		{name: "negative-first", count: 4, first: -1, step: 1},
		{name: "positive-overflow", count: 3, first: math.MaxInt64 - 1, step: 1},
		{name: "negative-overflow", count: 3, first: 1, step: -1},
		{name: "minimum-step", count: 2, first: math.MaxInt64, step: math.MinInt64},
	} {
		t.Run(test.name, func(t *testing.T) {
			view := barePrefixIntegerDescriptor(t, test.count, test.first, test.step)
			if got, supported := view.countIntegerEqual(0); supported || got != 0 {
				t.Fatalf("malformed count=%d first=%d step=%d got=%d supported=%v",
					test.count, test.first, test.step, got, supported)
			}
			if got, _, _, supported := view.countNumberEqual(
				[]byte("1.5"), 0, false, nil, nil,
			); supported || got != 0 {
				t.Fatalf("malformed number count=%d supported=%v", got, supported)
			}
		})
	}
	paddedSingleton := barePrefixIntegerDescriptor(t, 1, 1, math.MinInt64)
	paddedSingleton.data[0], paddedSingleton.data[1] = 3, 3
	if got, supported := paddedSingleton.countIntegerEqual(1); supported || got != 0 {
		t.Fatalf("padded singleton count=%d supported=%v", got, supported)
	}
	zeroSingleton := barePrefixIntegerDescriptor(t, 1, 0, math.MinInt64)
	zeroSingleton.data[0], zeroSingleton.data[1] = 3, 1
	if got, supported := zeroSingleton.countIntegerEqual(0); !supported || got != 1 {
		t.Fatalf("zero singleton count=%d supported=%v", got, supported)
	}
	maxSingleton := barePrefixIntegerDescriptor(t, 1, math.MaxInt64, math.MinInt64)
	maxSingleton.data[0], maxSingleton.data[1] = 3, 19
	if got, supported := maxSingleton.countIntegerEqual(math.MaxInt64); !supported || got != 1 {
		t.Fatalf("maximum singleton count=%d supported=%v", got, supported)
	}
	mode2Width := barePrefixIntegerDescriptor(t, compactStreamRestart, 7, 1)
	mode2Width.data[1] = 9 // mode 2 ignores the historical width byte.
	if got, supported := mode2Width.countIntegerEqual(8); !supported || got != 1 {
		t.Fatalf("mode2 width byte count=%d supported=%v", got, supported)
	}
	view := barePrefixIntegerDescriptor(t, 4, 7, 1)
	view.data = view.data[:17]
	if got, supported := view.countIntegerEqual(7); supported || got != 0 {
		t.Fatalf("short arithmetic data count=%d supported=%v", got, supported)
	}
	view = barePrefixIntegerDescriptor(t, 4, 7, 1)
	view.dictDir = view.dictDir[:2]
	if got, supported := view.countIntegerEqual(7); supported || got != 0 {
		t.Fatalf("short dictionary directory count=%d supported=%v", got, supported)
	}
}

func TestCompactPrefixIntegerSpellingArithmeticShortcut(t *testing.T) {
	tests := []struct {
		name           string
		count          int
		first          int64
		step           int64
		width          int
		prefix, suffix string
		needle         string
		want           int
	}{
		{
			name:  "ascending-bare-threshold63",
			count: 63, first: 100, step: 1,
			prefix: "", suffix: "",
			needle: "130", want: 1,
		},
		{
			name:  "ascending-affixed-threshold64",
			count: 64, first: 100, step: 1,
			prefix: "id:", suffix: ":end",
			needle: "id:163:end", want: 1,
		},
		{
			name:  "descending-padded-affixed",
			count: 64, first: 200, step: -1, width: 3,
			prefix: "id:", suffix: ":end",
			needle: "id:137:end", want: 1,
		},
		{
			name:  "constant-padded-affixed",
			count: 64, first: 7, step: 0, width: 3,
			prefix: "id:", suffix: ":end",
			needle: "id:007:end", want: 64,
		},
		{
			name:  "affixed-miss",
			count: 64, first: 100, step: 1,
			prefix: "id:", suffix: ":end",
			needle: "id:164:end", want: 0,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			values := prefixIntegerSpellingValues(
				test.count, test.first, test.step,
				test.prefix, test.suffix, test.width,
			)
			encoded, ok := encodeCompactPrefixInt(values)
			if !ok {
				t.Fatal("prefix integer spelling fixture rejected")
			}
			view := compactCodecRoundTrip(t, encoded, values)
			if view.kind != compactStreamPrefixInt || view.data[0]&2 == 0 {
				t.Fatalf("fixture kind=%d flags=%d", view.kind, view.data[0])
			}
			got, supported := view.countPrefixIntegerEqual([]byte(test.needle))
			if !supported || got != test.want {
				t.Fatalf("needle=%q count=%d supported=%v want=%d",
					test.needle, got, supported, test.want)
			}
		})
	}

	values := prefixIntegerSpellingValues(64, 100, 1, "id:", ":end", 0)
	encoded, ok := encodeCompactPrefixInt(values)
	if !ok {
		t.Fatal("affixed mismatch fixture rejected")
	}
	view := compactCodecRoundTrip(t, encoded, values)
	mode2Values := prefixIntegerSpellingValues(64, 9, 1, "id:", ":end", 0)
	mode2Encoded, ok := encodeCompactPrefixInt(mode2Values)
	if !ok {
		t.Fatal("variable-width mode2 fixture rejected")
	}
	mode2View := compactCodecRoundTrip(t, mode2Encoded, mode2Values)
	if mode2View.data[0] != 2 {
		t.Fatalf("variable-width fixture flags=%d want mode2", mode2View.data[0])
	}
	mode2View.data[1] = 9 // mode 2 spelling admission ignores the width byte.
	if got, supported := mode2View.countPrefixIntegerEqual([]byte("id:42:end")); !supported || got != 1 {
		t.Fatalf("mode2 spelling width byte count=%d supported=%v", got, supported)
	}
	for _, needle := range []string{"other:163:end", "id:0100:end", "id:163:other"} {
		if got, supported := view.countPrefixIntegerEqual([]byte(needle)); supported || got != 0 {
			t.Fatalf("needle=%q count=%d supported=%v for mismatch", needle, got, supported)
		}
	}

	padded := prefixIntegerSpellingValues(64, 200, -1, "id:", ":end", 3)
	encoded, ok = encodeCompactPrefixInt(padded)
	if !ok {
		t.Fatal("padded width mismatch fixture rejected")
	}
	view = compactCodecRoundTrip(t, encoded, padded)
	if got, supported := view.countPrefixIntegerEqual([]byte("id:37:end")); supported || got != 0 {
		t.Fatalf("padded width mismatch count=%d supported=%v", got, supported)
	}
}

func TestCompactPrefixIntegerSpellingArithmeticOverflowFallback(t *testing.T) {
	view := prefixIntegerDescriptor(
		t, compactStreamRestart, math.MaxInt64, 1, "id:", ":end", 0,
	)
	got, supported := view.countPrefixIntegerEqual(
		[]byte("id:9223372036854775807:end"),
	)
	if !supported || got != 1 {
		t.Fatalf("overflow endpoint count=%d supported=%v want=1,true", got, supported)
	}

	view = prefixIntegerDescriptor(t, compactStreamRestart, 0, -1, "id:", ":end", 0)
	got, supported = view.countPrefixIntegerEqual([]byte("id:0:end"))
	if !supported || got != 1 {
		t.Fatalf("underflow endpoint count=%d supported=%v want=1,true", got, supported)
	}
}
