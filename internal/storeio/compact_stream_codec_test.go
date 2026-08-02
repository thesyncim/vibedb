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
			compactCodecRoundTrip(t, encoded, test.values)
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
	tests[3][16] = 0xff
	tests[3][17] = 0xff
	tests[3][18] = 0xff
	tests[3][19] = 0xff
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
