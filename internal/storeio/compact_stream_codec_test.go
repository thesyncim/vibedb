package storeio

import (
	"bytes"
	"strconv"
	"testing"
)

func compactCodecRoundTrip(t testing.TB, encoded compactStreamEncoding, values [][]byte) compactStreamView {
	t.Helper()
	binaryStream, err := encoded.appendBinary(nil)
	if err != nil {
		t.Fatal(err)
	}
	view, err := openCompactStream(binaryStream)
	if err != nil {
		t.Fatal(err)
	}
	if view.encoded != len(binaryStream) || view.count != len(values) {
		t.Fatalf("stream geometry encoded=%d/%d count=%d/%d",
			view.encoded, len(binaryStream), view.count, len(values))
	}
	buf := make([]byte, 0, 256)
	for row, want := range values {
		got, ok := view.appendValue(buf[:0], row)
		if !ok || !bytes.Equal(got, want) {
			t.Fatalf("row %d got=%q ok=%v want=%q", row, got, ok, want)
		}
		buf = got
	}
	return view
}

func TestCompactStreamCodecRoundTrip(t *testing.T) {
	dictionary := make([][]byte, 257)
	front := make([][]byte, 257)
	integers := make([]int64, 257)
	dates := make([]int32, 257)
	prefix := make([][]byte, 257)
	for i := range 257 {
		dictionary[i] = []byte([]string{`"PT"`, `"US"`, `"DE"`}[i%3])
		front[i] = []byte("value/" + strconv.Itoa(i) + "/" + string(rune('a'+i%23)))
		integers[i] = int64(i*3 - 200)
		value := []byte(`"2024-01-01"`)
		dates[i], _ = compactDateOrdinal(value)
		dates[i] += int32(i % 100)
		prefix[i] = []byte("doc:" + leftPadDecimal(i, 8))
	}
	integerSpellings := make([][]byte, len(integers))
	dateSpellings := make([][]byte, len(dates))
	for i := range integers {
		integerSpellings[i] = AppendCanonicalInt(nil, integers[i])
		dateSpellings[i] = appendCompactDate(nil, dates[i])
	}

	tests := []struct {
		name   string
		kind   uint8
		values [][]byte
		encode func() compactStreamEncoding
	}{
		{"dictionary", compactStreamDictionary, dictionary, func() compactStreamEncoding { return encodeCompactDictionary(dictionary) }},
		{"front", compactStreamFront, front, func() compactStreamEncoding { return encodeCompactFront(front) }},
		{"FOR", compactStreamFOR, integerSpellings, func() compactStreamEncoding { return encodeCompactFOR(integers) }},
		{"delta", compactStreamDelta, integerSpellings, func() compactStreamEncoding { return encodeCompactDelta(integers) }},
		{"packed-delta", compactStreamDeltaPack, integerSpellings, func() compactStreamEncoding {
			var scratch compactStreamScratch
			return scratch.encodeDeltaPack(0, integers)
		}},
		{"date", compactStreamDate, dateSpellings, func() compactStreamEncoding { return encodeCompactDate(dates) }},
		{"prefix-int", compactStreamPrefixInt, prefix, func() compactStreamEncoding {
			encoded, ok := encodeCompactPrefixInt(prefix)
			if !ok {
				t.Fatal("prefix-int rejected")
			}
			return encoded
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			encoded := test.encode()
			if encoded.kind != test.kind {
				t.Fatalf("kind=%d want=%d", encoded.kind, test.kind)
			}
			view := compactCodecRoundTrip(t, encoded, test.values)
			needle := test.values[len(test.values)/2]
			want := 0
			for _, value := range test.values {
				if bytes.Equal(value, needle) {
					want++
				}
			}
			got, _, supported := view.countSpellingEqual(needle, nil)
			if !supported || got != want {
				t.Fatalf("spelling count=%d supported=%v want=%d", got, supported, want)
			}
		})
	}
}

func TestCompactStreamAdaptiveSelectionAndCount(t *testing.T) {
	values := make([][]byte, 100_000)
	want := 0
	for i := range values {
		values[i] = []byte([]string{`"PT"`, `"US"`, `"DE"`, `"FR"`}[i%4])
		if bytes.Equal(values[i], []byte(`"PT"`)) {
			want++
		}
	}
	encoded := encodeCompactScalarStream(values)
	if encoded.kind != compactStreamDictionary {
		t.Fatalf("adaptive kind=%d want dictionary", encoded.kind)
	}
	view := compactCodecRoundTrip(t, encoded, values)
	matched, supported := view.countDictionaryEqual([]byte(`"PT"`))
	if !supported || matched != want {
		t.Fatalf("count=%d supported=%v want=%d", matched, supported, want)
	}
	if matched, supported := view.countDictionaryEqual([]byte(`"missing"`)); !supported || matched != 0 {
		t.Fatalf("missing count=%d supported=%v", matched, supported)
	}
}

func TestCountCompactPackedEqualMatchesRandomAccess(t *testing.T) {
	for width := 0; width <= 64; width++ {
		for _, count := range []int{0, 1, 7, 8, 9, 63, 64, 65, 257, 4096} {
			data := make([]byte, (count*width+7)/8)
			mask := uint64(0)
			if width != 0 {
				mask = uint64(1)<<uint(width) - 1
			}
			want := mask / 3
			expected := 0
			for row := 0; row < count; row++ {
				value := uint64(row*17+row/3) & mask
				compactPutBits(data, row*width, width, value)
				if value == want {
					expected++
				}
			}
			if got := countCompactPackedEqual(data, count, width, want); got != expected {
				t.Fatalf(
					"width=%d count=%d got=%d want=%d",
					width, count, got, expected,
				)
			}
		}
	}
}

func TestCompactStreamIntegerCountMatchesValues(t *testing.T) {
	values := make([]int64, 4097)
	for row := range values {
		values[row] = int64((row*37)%997 - 400)
	}
	for _, encoded := range []compactStreamEncoding{
		encodeCompactFOR(values),
		encodeCompactDelta(values),
		func() compactStreamEncoding {
			var scratch compactStreamScratch
			return scratch.encodeDeltaPack(0, values)
		}(),
	} {
		spellings := make([][]byte, len(values))
		for row, value := range values {
			spellings[row] = AppendCanonicalInt(nil, value)
		}
		view := compactCodecRoundTrip(t, encoded, spellings)
		for _, needle := range []int64{-401, -400, 0, 596, 597} {
			want := 0
			for _, value := range values {
				if value == needle {
					want++
				}
			}
			got, supported := view.countIntegerEqual(needle)
			if !supported || got != want {
				t.Fatalf(
					"kind=%d needle=%d count=%d supported=%v want=%d",
					view.kind, needle, got, supported, want,
				)
			}
		}
	}
}

func TestCompactStreamNumberCountExactDecimalSemantics(t *testing.T) {
	values := [][]byte{
		[]byte(`1`), []byte(`1.0`), []byte(`1e0`), []byte(`0.1e1`),
		[]byte(`2`), []byte(`"1"`), []byte(`-0`), []byte(`0.0`),
	}
	for _, encoded := range []compactStreamEncoding{
		encodeCompactDictionary(values),
		encodeCompactFront(values),
	} {
		view := compactCodecRoundTrip(t, encoded, values)
		for _, test := range []struct {
			needle      string
			needleInt   int64
			needleIsInt bool
			want        int
		}{
			{needle: `1.00e0`, needleInt: 1, needleIsInt: true, want: 4},
			{needle: `0`, needleInt: 0, needleIsInt: true, want: 2},
			{needle: `0.5`, want: 0},
		} {
			got, _, _, supported := view.countNumberEqual(
				[]byte(test.needle), test.needleInt, test.needleIsInt, nil, nil,
			)
			if !supported || got != test.want {
				t.Fatalf(
					"kind=%d needle=%s count=%d supported=%v want=%d",
					view.kind, test.needle, got, supported, test.want,
				)
			}
		}
	}
}

func TestCompactStreamPackedPrefixIntRoundTrip(t *testing.T) {
	values := make([][]byte, 4096)
	for row := range values {
		values[row] = []byte("user-" + strconv.Itoa(row*100+row%2))
	}
	encoded := encodeCompactScalarStream(values)
	if encoded.kind != compactStreamPrefixInt || encoded.data[0]&4 == 0 {
		t.Fatalf("codec kind=%d flags=%02x, want packed prefix-int", encoded.kind, encoded.data[0])
	}
	view := compactCodecRoundTrip(t, encoded, values)
	needle := values[997]
	want := 0
	for _, value := range values {
		if bytes.Equal(value, needle) {
			want++
		}
	}
	got, _, supported := view.countSpellingEqual(needle, nil)
	if !supported || got != want {
		t.Fatalf("packed prefix count=%d supported=%v want=%d", got, supported, want)
	}
}

func TestCompactStreamRejectsCorruptFraming(t *testing.T) {
	values := [][]byte{[]byte(`"PT"`), []byte(`"US"`), []byte(`"PT"`)}
	encoded, err := encodeCompactScalarStream(values).appendBinary(nil)
	if err != nil {
		t.Fatal(err)
	}
	tests := [][]byte{
		encoded[:compactStreamHeader-1],
		append([]byte(nil), encoded...),
		append([]byte(nil), encoded...),
		append([]byte(nil), encoded...),
	}
	tests[1][0] = compactStreamKindLimit
	tests[2][2] = 1
	tests[3][10] = 0xff
	tests[3][11] = 0xff
	for i, malformed := range tests {
		if _, err := openCompactStream(malformed); err == nil {
			t.Fatalf("corruption %d admitted", i)
		}
	}
}

func TestCompactStreamPointDecodeWarmAllocations(t *testing.T) {
	values := make([][]byte, 257)
	for i := range values {
		values[i] = []byte("doc:" + leftPadDecimal(i, 8))
	}
	view := compactCodecRoundTrip(t, encodeCompactScalarStream(values), values)
	buf := make([]byte, 0, 64)
	allocs := testing.AllocsPerRun(1000, func() {
		out, ok := view.appendValue(buf[:0], 191)
		if !ok || !bytes.Equal(out, values[191]) {
			panic("compact point decode")
		}
		buf = out
	})
	if allocs != 0 {
		t.Fatalf("warm point allocations=%v want 0", allocs)
	}
}

func leftPadDecimal(value, width int) string {
	raw := strconv.Itoa(value)
	if len(raw) >= width {
		return raw
	}
	buf := make([]byte, width)
	for i := range width - len(raw) {
		buf[i] = '0'
	}
	copy(buf[width-len(raw):], raw)
	return string(buf)
}
