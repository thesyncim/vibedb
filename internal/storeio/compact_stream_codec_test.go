package storeio

import (
	"bytes"
	"encoding/binary"
	"math/bits"
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
		{"alphabet", compactStreamAlphabet, front, func() compactStreamEncoding {
			var scratch compactStreamScratch
			encoded, ok := scratch.encodeAlphabet(0, front, 0)
			if !ok {
				t.Fatal("alphabet rejected")
			}
			return encoded
		}},
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
	gotBinary, err := encoded.appendBinary(nil)
	if err != nil {
		t.Fatal(err)
	}
	wantBinary, err := encodeCompactDictionary(values).appendBinary(nil)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(gotBinary, wantBinary) {
		t.Fatal("deferred dictionary bytes differ from eager encoding")
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

func TestMeasureCompactFrontMatchesEncoding(t *testing.T) {
	values := make([][]byte, 130)
	for row := range values {
		switch row % 4 {
		case 0:
			values[row] = []byte(`"short"`)
		case 1:
			values[row] = []byte(`"shared-prefix-0123456789-alpha"`)
		case 2:
			values[row] = []byte(`"shared-prefix-0123456789-beta-with-a-long-suffix"`)
		default:
			values[row] = []byte(`"x"`)
		}
	}
	for _, count := range []int{1, 2, 63, 64, 65, 127, 128, 129, 130} {
		got := measureCompactFront(values[:count])
		want := encodeCompactFront(values[:count]).encodedBytes()
		if got != want {
			t.Fatalf("count=%d measured=%d encoded=%d", count, got, want)
		}
	}
}

func TestMeasureCompactAlphabetMatchesEncoding(t *testing.T) {
	values := make([][]byte, 130)
	for row := range values {
		values[row] = []byte("value/" + leftPadDecimal(row, 5) +
			"/abcdefghijklmnopqrstuvwxyz")
	}
	for _, count := range []int{1, 2, 63, 64, 65, 127, 128, 129, 130} {
		var scratch compactStreamScratch
		plan, ok := scratch.measureAlphabet(2, values[:count], 0)
		if !ok {
			t.Fatalf("count=%d alphabet rejected", count)
		}
		encoded := scratch.finishAlphabet(2, values[:count], plan)
		if got := encoded.encodedBytes(); got != plan.encoded {
			t.Fatalf("count=%d measured=%d encoded=%d", count, plan.encoded, got)
		}
		compactCodecRoundTrip(t, encoded, values[:count])
	}
}

func TestCompactAlphabetPacksAcrossValuesWithinRestartBlock(t *testing.T) {
	const rows = compactStreamRestart + 1
	values := make([][]byte, rows)
	const alphabet = "abcdefghijklmnopq"
	for row := range values {
		values[row] = []byte{alphabet[row%len(alphabet)]}
	}

	var scratch compactStreamScratch
	plan, ok := scratch.measureAlphabet(2, values, 0)
	if !ok {
		t.Fatal("alphabet rejected")
	}
	if plan.width != 5 {
		t.Fatalf("width=%d, want 5", plan.width)
	}
	// Two offsets, a base and width byte for each fixed-length block,
	// 40 packed bytes for the full block, and one for the final row.
	const wantDataBytes = 2*4 + 2*2 + 40 + 1
	if plan.totalBytes != wantDataBytes {
		t.Fatalf("data bytes=%d, want %d", plan.totalBytes, wantDataBytes)
	}
	oldPerValuePadding := 2*4 + rows + rows
	if plan.totalBytes >= oldPerValuePadding {
		t.Fatalf("block packing=%d did not beat per-value padding=%d", plan.totalBytes, oldPerValuePadding)
	}

	encoded := scratch.finishAlphabet(2, values, plan)
	view := compactCodecRoundTrip(t, encoded, values)
	for _, row := range []int{0, compactStreamRestart - 1, compactStreamRestart} {
		got, valid := view.appendValue(nil, row)
		if !valid || !bytes.Equal(got, values[row]) {
			t.Fatalf("row=%d got=%q valid=%v want=%q", row, got, valid, values[row])
		}
	}
}

func TestCompactAlphabetBitPacksVariableLengths(t *testing.T) {
	values := make([][]byte, compactStreamRestart+1)
	seeds := []string{"abc", "bcde", "cdefg", "defghi"}
	characters := 0
	for row := range values {
		values[row] = []byte(seeds[row%len(seeds)])
		characters += len(values[row])
	}
	var scratch compactStreamScratch
	plan, ok := scratch.measureAlphabet(2, values, 0)
	if !ok || plan.prefix != 0 || plan.suffix != 0 {
		t.Fatalf("alphabet plan=%+v ok=%v", plan, ok)
	}
	encoded := scratch.finishAlphabet(2, values, plan)
	// Two offsets; the full block has a one-byte base, one-byte width, and
	// sixteen bytes of 2-bit deltas; the final block needs only base+width.
	wantDataBytes := 2*4 + (1 + 1 + 16) + (1 + 1) +
		(characters*plan.width+7)/8
	if plan.totalBytes != wantDataBytes {
		t.Fatalf("data bytes=%d, want %d", plan.totalBytes, wantDataBytes)
	}
	compactCodecRoundTrip(t, encoded, values)
}

func TestCompactAlphabetSequentialZeroWidthMiddle(t *testing.T) {
	values := make([][]byte, compactStreamRestart+1)
	for row := range values {
		values[row] = []byte(`"a"`)
		if row&1 != 0 {
			values[row] = []byte(`"aa"`)
		}
	}
	var scratch compactStreamScratch
	encoded, ok := scratch.encodeAlphabet(2, values, 0)
	if !ok || encoded.width != 0 || len(encoded.dict) != 3 {
		t.Fatalf("kind=%d width=%d dictionaries=%d", encoded.kind, encoded.width, len(encoded.dict))
	}
	view := compactCodecRoundTrip(t, encoded, values)
	var state compactStreamSequentialState
	for row := range values {
		got, valid := state.appendValue(nil, &view, row)
		if !valid || !bytes.Equal(got, values[row]) {
			t.Fatalf("row=%d got=%q valid=%v want=%q", row, got, valid, values[row])
		}
	}
}

func TestCompactAlphabetSequentialReservoirWidthsRestartsAndAllocations(t *testing.T) {
	for _, cardinality := range []int{2, 3, 5, 9, 17, 33, 64} {
		values := make([][]byte, 2*compactStreamRestart+1)
		// Seed every symbol so the encoder must select the target width even
		// when later rows happen to use a smaller subset.
		values[0] = make([]byte, cardinality)
		for symbol := range cardinality {
			values[0][symbol] = byte('A' + symbol)
		}
		for row := 1; row < len(values); row++ {
			length := 1 + row*7%17
			values[row] = make([]byte, length)
			for char := range length {
				values[row][char] = byte('A' + (row*5+char*3)%cardinality)
			}
		}

		var scratch compactStreamScratch
		encoded, ok := scratch.encodeAlphabet(0, values, 0)
		wantWidth := bits.Len(uint(cardinality - 1))
		if !ok || int(encoded.width) != wantWidth {
			t.Fatalf(
				"cardinality=%d kind=%d width=%d want=%d ok=%v",
				cardinality, encoded.kind, encoded.width, wantWidth, ok,
			)
		}
		view := compactCodecRoundTrip(t, encoded, values)
		backing := make([]byte, cardinality+17)
		decode := func(checkRandom bool) {
			var state compactStreamSequentialState
			for row, want := range values {
				for at := range backing {
					backing[at] = 0xff
				}
				got, valid := state.appendValue(backing[:0], &view, row)
				if !valid || !bytes.Equal(got, want) {
					panic("packed alphabet sequential decode")
				}
				if checkRandom {
					random, randomValid := view.appendValue(nil, row)
					if !randomValid || !bytes.Equal(got, random) {
						panic("packed alphabet random parity")
					}
				}
			}
		}
		decode(true)
		if allocs := testing.AllocsPerRun(1_000, func() { decode(false) }); allocs != 0 {
			t.Fatalf(
				"cardinality=%d width=%d allocations=%v want 0",
				cardinality, wantWidth, allocs,
			)
		}
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
		func() compactStreamEncoding {
			var scratch compactStreamScratch
			encoded, ok := scratch.encodeAlphabet(0, values, 0)
			if !ok {
				t.Fatal("numeric alphabet rejected")
			}
			return encoded
		}(),
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

func TestCompactStreamAdaptiveAlphabetSelection(t *testing.T) {
	values := make([][]byte, 1000)
	for row := range values {
		value := make([]byte, 66)
		value[0], value[len(value)-1] = '"', '"'
		state := uint64(row + 1)
		for at := 1; at < len(value)-1; at++ {
			state = state*6364136223846793005 + 1442695040888963407
			value[at] = byte('a' + state%26)
		}
		values[row] = value
	}
	encoded := encodeCompactScalarStream(values)
	if encoded.kind != compactStreamAlphabet {
		t.Fatalf("adaptive kind=%d want alphabet", encoded.kind)
	}
	if len(encoded.dict) != 3 || !bytes.Equal(encoded.dict[1], []byte{'"'}) ||
		!bytes.Equal(encoded.dict[2], []byte{'"'}) {
		t.Fatalf("alphabet affixes=%q, want shared quotes", encoded.dict[1:])
	}
	view := compactCodecRoundTrip(t, encoded, values)
	buf := make([]byte, 0, 80)
	allocs := testing.AllocsPerRun(1000, func() {
		out, ok := view.appendValue(buf[:0], 731)
		if !ok || !bytes.Equal(out, values[731]) {
			panic("alphabet point decode")
		}
		buf = out
	})
	if allocs != 0 {
		t.Fatalf("alphabet point allocations=%v want 0", allocs)
	}
	missing := []byte(`"not-present-in-this-alphabet-stream"`)
	if got, supported := view.countDictionaryEqual(missing); supported || got != 0 {
		t.Fatalf("alphabet exposed dictionary counter: %d %v", got, supported)
	}
	if got, _ := view.countAlphabetEqual(missing, nil); got != 0 {
		t.Fatalf("alphabet missing count=%d", got)
	}
}

func TestCompactDateOrdinalExhaustiveRoundTrip(t *testing.T) {
	limit := int32(compactDaysBeforeYear(10_000))
	var spelling [12]byte
	for ordinal := int32(0); ordinal < limit; ordinal++ {
		got := appendCompactDate(spelling[:0], ordinal)
		roundTrip, ok := compactDateOrdinal(got)
		if !ok || roundTrip != ordinal {
			t.Fatalf("ordinal=%d spelling=%q roundTrip=%d ok=%v", ordinal, got, roundTrip, ok)
		}
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

func TestCompactAlphabetRejectsCorruption(t *testing.T) {
	values := [][]byte{[]byte("ab"), []byte("ac"), []byte("ba")}
	var scratch compactStreamScratch
	encoding, ok := scratch.encodeAlphabet(0, values, 0)
	if !ok || len(encoding.dict) != 1 || len(encoding.dict[0]) != 3 {
		t.Fatal("alphabet fixture")
	}
	encoded, err := encoding.appendBinary(nil)
	if err != nil {
		t.Fatal(err)
	}
	dictStart := compactStreamHeader + 2
	dataStart := dictStart + len(encoding.dict[0])
	tests := [][]byte{
		append([]byte(nil), encoded...),
		append([]byte(nil), encoded...),
		append([]byte(nil), encoded...),
		append([]byte(nil), encoded...),
	}
	tests[0][dictStart+1] = tests[0][dictStart]
	binary.LittleEndian.PutUint32(tests[1][dataStart:], 0)
	cursor := dataStart + 4
	_, n, valid := readCompactUvarint(tests[2][cursor:])
	if !valid {
		t.Fatal("alphabet fixture length base")
	}
	cursor += n
	lengthWidth := int(tests[2][cursor])
	tests[3][cursor] = 65
	cursor++
	cursor += (len(values)*lengthWidth + 7) / 8
	tests[2][cursor] |= 3
	for i, malformed := range tests {
		if _, err := openCompactStream(malformed); err == nil {
			t.Fatalf("alphabet corruption %d admitted", i)
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

// TestCompactStreamPlannerEstimateExact pins the scalar planner's borrowed cost
// estimates to the bytes the finished encoding actually serializes.
// measureDictionary and measureAlphabet steer candidate selection and the
// alphabet size limit before the winning encoding is built; a drifting estimate
// would let leaf-capacity planning admit a stream image that does not match its
// reserved extent. Both the borrowed estimate and the serialized length are
// checked against the finished encoding across cardinality extremes.
func TestCompactStreamPlannerEstimateExact(t *testing.T) {
	sequential := make([][]byte, 4096)
	for i := range sequential {
		sequential[i] = []byte("primary-key-" + leftPadDecimal(i, 9))
	}
	lowCardinality := make([][]byte, 300)
	for i := range lowCardinality {
		lowCardinality[i] = []byte([]string{`"PT"`, `"US"`, `"DE"`, `"FR"`}[i%4])
	}
	empties := make([][]byte, 64)
	for i := range empties {
		empties[i] = []byte{}
	}
	cases := []struct {
		name   string
		values [][]byte
	}{
		{"unique-sequential-keys", sequential},
		{"low-cardinality", lowCardinality},
		{"single-value", [][]byte{[]byte(`"only"`)}},
		{"two-values", [][]byte{[]byte(`"a"`), []byte(`"b"`)}},
		{"empty-strings", empties},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// The dictionary estimate builds the census in insertion order; the
			// finished encoding sorts the same set. Neither the byte total nor the
			// packed id width depends on ordering, so the two must agree exactly.
			var dictScratch compactStreamScratch
			dictEstimate := dictScratch.measureDictionary(tc.values)
			dictionary := dictScratch.finishDictionary(tc.values)
			if got := dictionary.encodedBytes(); got != dictEstimate {
				t.Fatalf("dictionary estimate=%d encodedBytes=%d", dictEstimate, got)
			}
			// A dictionary whose entries overflow the u16 section bounds is
			// unserializable and never selected; the size invariant above still
			// holds, so only match the serialized length when it fits a page.
			if dictBinary, err := dictionary.appendBinary(nil); err == nil &&
				len(dictBinary) != dictEstimate {
				t.Fatalf("dictionary estimate=%d serialized=%d", dictEstimate, len(dictBinary))
			}
			// The alphabet estimate reserves its limit from the same borrowed pass
			// that finishAlphabet then packs; when a stream qualifies, its plan
			// figure must equal the finished image.
			var alphaScratch compactStreamScratch
			if plan, ok := alphaScratch.measureAlphabet(2, tc.values, 0); ok {
				alphabet := alphaScratch.finishAlphabet(2, tc.values, plan)
				if got := alphabet.encodedBytes(); got != plan.encoded {
					t.Fatalf("alphabet estimate=%d encodedBytes=%d", plan.encoded, got)
				}
				if alphaBinary, alphaErr := alphabet.appendBinary(nil); alphaErr == nil &&
					len(alphaBinary) != plan.encoded {
					t.Fatalf("alphabet estimate=%d serialized=%d", plan.encoded, len(alphaBinary))
				}
			}
			// Whichever candidate the planner selects, its reported encodedBytes is
			// the exact serialized length the enclosing page reserves.
			var pick compactStreamScratch
			winner := pick.encode(tc.values)
			winnerBinary, err := winner.appendBinary(nil)
			if err != nil {
				t.Fatal(err)
			}
			if len(winnerBinary) != winner.encodedBytes() {
				t.Fatalf("winner kind=%d encodedBytes=%d serialized=%d",
					winner.kind, winner.encodedBytes(), len(winnerBinary))
			}
			compactCodecRoundTrip(t, winner, tc.values)
		})
	}
}
