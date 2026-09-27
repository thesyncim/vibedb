package storeio

import (
	"bytes"
	"math/bits"
	"strconv"
	"testing"
)

func compactAlphabetUnionValues(cardinality int) [][]byte {
	values := make([][]byte, cardinality)
	for i := range cardinality {
		values[i] = []byte("key-" + leftPadDecimal(i, 4) + "-end")
	}
	return values
}

func compactAlphabetUnionRepeatedValues(cardinality int) [][]byte {
	unique := compactAlphabetUnionValues(cardinality)
	values := make([][]byte, 0, 2*cardinality+3)
	values = append(values, unique...)
	for i := range cardinality {
		values = append(values, unique[(i*37+11)%cardinality])
	}
	values = append(values, unique[0], unique[cardinality/2], unique[cardinality-1])
	return values
}

func measureCompactAlphabetBeforeUnionReuse(
	values [][]byte,
	limit int,
) (compactAlphabetPlan, []byte, bool) {
	blocks := (len(values) + compactStreamRestart - 1) / compactStreamRestart
	prefix, suffix := compactSharedAffixes(values)
	var present [256]bool
	count := 0
	for _, value := range values {
		middle := value[prefix : len(value)-suffix]
		for _, b := range middle {
			if !present[b] {
				present[b] = true
				count++
			}
		}
	}
	if count == 0 || count > 64 {
		return compactAlphabetPlan{}, nil, false
	}
	width := bits.Len(uint(count - 1))
	totalBytes := 4 * blocks
	for first := 0; first < len(values); first += compactStreamRestart {
		last := min(first+compactStreamRestart, len(values))
		lo, hi, characters := 0, 0, 0
		for row, value := range values[first:last] {
			middle := len(value) - prefix - suffix
			if row == 0 {
				lo, hi = middle, middle
			} else {
				lo, hi = min(lo, middle), max(hi, middle)
			}
			characters += middle
		}
		lengthWidth := bits.Len(uint(hi - lo))
		totalBytes += compactUvarintLen(uint64(lo)) + 1 +
			((last-first)*lengthWidth+7)/8
		totalBytes += (characters*width + 7) / 8
	}
	dictionaryEntries, affixBytes := 1, 0
	if prefix != 0 || suffix != 0 {
		dictionaryEntries = 3
		affixBytes = prefix + suffix
	}
	encodedBytes := compactStreamHeader + 2*dictionaryEntries + count + affixBytes + totalBytes
	if limit > 0 && encodedBytes >= limit {
		return compactAlphabetPlan{}, nil, false
	}
	alphabet := make([]byte, 0, count)
	for b := range present {
		if present[b] {
			alphabet = append(alphabet, byte(b))
		}
	}
	return compactAlphabetPlan{
		width: width, prefix: prefix, suffix: suffix,
		totalBytes: totalBytes, encoded: encodedBytes,
	}, alphabet, true
}

func assertCompactAlphabetPlanAndBytesEqual(
	t testing.TB,
	gotPlan compactAlphabetPlan,
	gotAlphabet []byte,
	gotOK bool,
	wantPlan compactAlphabetPlan,
	wantAlphabet []byte,
	wantOK bool,
) {
	t.Helper()
	if gotOK != wantOK || gotPlan != wantPlan ||
		gotOK && !bytes.Equal(gotAlphabet, wantAlphabet) {
		t.Fatalf("alphabet result plan=%+v ok=%v alphabet=%x; want plan=%+v ok=%v alphabet=%x",
			gotPlan, gotOK, gotAlphabet, wantPlan, wantOK, wantAlphabet)
	}
}

func TestCompactAlphabetUnionMatchesFullRowScan(t *testing.T) {
	longValues := make([][]byte, 129)
	for i := range longValues {
		longValues[i] = []byte("long-" + leftPadDecimal(i, 4) + "-" +
			string(bytes.Repeat([]byte{'x'}, 512)) + "-tail")
	}
	tooManySymbols := [][]byte{make([]byte, 67)}
	tooManySymbols[0][0] = '<'
	for i := 0; i < 65; i++ {
		tooManySymbols[0][i+1] = byte(i + 1)
	}
	tooManySymbols[0][66] = '>'

	cases := []struct {
		name   string
		values [][]byte
	}{
		{name: "empty-middle", values: [][]byte{[]byte(`"a"`), []byte(`"aa"`), []byte(`"a"`)}},
		{name: "single", values: [][]byte{[]byte(`"single"`)}},
		{name: "empty-spelling", values: [][]byte{nil, nil, []byte("x")}},
		{name: "variable-restart-affixes", values: compactAlphabetReservoirTestValues(64)},
		{name: "large-dictionary-bytes", values: longValues},
		{name: "more-than-64-symbols", values: tooManySymbols},
	}
	for _, cardinality := range []int{15, 16, 17, 127, 128, 129, 4096} {
		cases = append(cases, struct {
			name   string
			values [][]byte
		}{"unique-" + strconv.Itoa(cardinality), compactAlphabetUnionValues(cardinality)})
		cases = append(cases, struct {
			name   string
			values [][]byte
		}{"repeated-" + strconv.Itoa(cardinality), compactAlphabetUnionRepeatedValues(cardinality)})
	}

	var optimized, oldPath compactStreamScratch
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, limit := range []int{0, 1} {
				wantPlan, wantAlphabet, wantOK := measureCompactAlphabetBeforeUnionReuse(tc.values, limit)
				gotPlan, gotOK := optimized.measureAlphabet(2, tc.values, limit)
				gotAlphabet := optimized.alphabet[2]
				assertCompactAlphabetPlanAndBytesEqual(
					t, gotPlan, gotAlphabet, gotOK,
					wantPlan, wantAlphabet, wantOK,
				)

				optimized.measureDictionary(tc.values)
				spellings := tc.values
				if len(optimized.dict[0]) < len(tc.values) {
					spellings = optimized.dict[0]
				}
				unionPlan, unionOK := optimized.measureAlphabetWithSpellings(
					2, tc.values, spellings, limit,
				)
				unionAlphabet := optimized.alphabet[2]
				oldPlan, oldOK := oldPath.measureAlphabet(2, tc.values, limit)
				assertCompactAlphabetPlanAndBytesEqual(
					t, unionPlan, unionAlphabet, unionOK,
					oldPlan, oldPath.alphabet[2], oldOK,
				)
				assertCompactAlphabetPlanAndBytesEqual(
					t, unionPlan, unionAlphabet, unionOK,
					wantPlan, wantAlphabet, wantOK,
				)
			}
		})
	}
}

func TestCompactAlphabetUnionWidthsAndSerializedOracle(t *testing.T) {
	var optimized, oldPath compactStreamScratch
	for _, cardinality := range []int{1, 2, 3, 5, 9, 17, 33, 64} {
		t.Run(strconv.Itoa(cardinality), func(t *testing.T) {
			values := compactAlphabetReservoirTestValues(cardinality)
			optimized.measureDictionary(values)
			spellings := optimized.dict[0]
			if len(spellings) >= len(values) {
				t.Fatalf("fixture lacks duplicate spellings: dictionary=%d values=%d",
					len(spellings), len(values))
			}
			gotPlan, gotOK := optimized.measureAlphabetWithSpellings(2, values, spellings, 0)
			wantPlan, wantOK := oldPath.measureAlphabet(2, values, 0)
			wantWidth := bits.Len(uint(cardinality - 1))
			if !gotOK || !wantOK || gotPlan.width != wantWidth {
				t.Fatalf("union plan=%+v ok=%v old=%+v ok=%v want width=%d",
					gotPlan, gotOK, wantPlan, wantOK, wantWidth)
			}
			assertCompactAlphabetPlanAndBytesEqual(
				t, gotPlan, optimized.alphabet[2], gotOK,
				wantPlan, oldPath.alphabet[2], wantOK,
			)
			got := optimized.finishAlphabet(2, values, gotPlan)
			want := oldPath.finishAlphabet(2, values, wantPlan)
			assertCompactEncodingEqual(t, got, want)
		})
	}
}

func TestCompactAlphabetUnionPreservesExactLimitBoundary(t *testing.T) {
	values := compactAlphabetUnionRepeatedValues(17)
	var optimized, oldPath compactStreamScratch
	optimized.measureDictionary(values)
	spellings := optimized.dict[0]
	if len(spellings) >= len(values) {
		t.Fatalf("fixture lacks duplicate spellings: dictionary=%d values=%d",
			len(spellings), len(values))
	}
	basePlan, baseOK := optimized.measureAlphabetWithSpellings(2, values, spellings, 0)
	if !baseOK {
		t.Fatal("unlimited alphabet plan rejected")
	}
	for _, limit := range []int{0, basePlan.encoded - 1, basePlan.encoded, basePlan.encoded + 1} {
		gotPlan, gotOK := optimized.measureAlphabetWithSpellings(2, values, spellings, limit)
		wantPlan, wantAlphabet, wantOK := measureCompactAlphabetBeforeUnionReuse(values, limit)
		oldPlan, oldOK := oldPath.measureAlphabet(2, values, limit)
		assertCompactAlphabetPlanAndBytesEqual(
			t, gotPlan, optimized.alphabet[2], gotOK,
			oldPlan, oldPath.alphabet[2], oldOK,
		)
		assertCompactAlphabetPlanAndBytesEqual(
			t, gotPlan, optimized.alphabet[2], gotOK,
			wantPlan, wantAlphabet, wantOK,
		)
		wantAccepted := limit == 0 || limit > basePlan.encoded
		if gotOK != wantAccepted {
			t.Fatalf("limit=%d accepted=%v want=%v encoded=%d",
				limit, gotOK, wantAccepted, basePlan.encoded)
		}
	}
}

func assertCompactEncodingEqual(t testing.TB, got, want compactStreamEncoding) {
	t.Helper()
	if got.kind != want.kind || got.width != want.width || got.count != want.count ||
		got.encodedBytes() != want.encodedBytes() || !bytes.Equal(got.data, want.data) ||
		len(got.dict) != len(want.dict) {
		t.Fatalf("encoding shape differs: got kind=%d width=%d count=%d bytes=%d; want kind=%d width=%d count=%d bytes=%d",
			got.kind, got.width, got.count, got.encodedBytes(),
			want.kind, want.width, want.count, want.encodedBytes())
	}
	for i := range got.dict {
		if !bytes.Equal(got.dict[i], want.dict[i]) {
			t.Fatalf("dictionary[%d] differs", i)
		}
	}
	gotBinary, gotErr := got.appendBinary(nil)
	wantBinary, wantErr := want.appendBinary(nil)
	if (gotErr != nil) != (wantErr != nil) || !bytes.Equal(gotBinary, wantBinary) {
		t.Fatalf("serialized bytes differ: got len=%d err=%v; want len=%d err=%v",
			len(gotBinary), gotErr, len(wantBinary), wantErr)
	}
}

func encodeCompactShapeBeforeAlphabetUnionReuse(
	s *compactStreamScratch,
	values [][]byte,
	ranks []uint16,
	leafRows int,
	rankContext *compactRankContext,
	rankView *CompactPrimaryStripeView,
) compactStreamEncoding {
	if len(values) == 0 {
		return compactStreamEncoding{kind: compactStreamDictionary}
	}
	dictionaryBytes := s.measureDictionary(values)
	frontBytes := measureCompactFront(values)
	n := 3
	alphabetLimit := min(dictionaryBytes, frontBytes)
	alphabet, hasAlphabet := s.measureAlphabet(2, values, alphabetLimit)
	numeric, hasPrefix := s.encodePrefixIntShapeWithRankContext(
		7, values, ranks, leafRows, rankContext, rankView,
	)
	rankNumber := hasPrefix && numeric.kind == compactStreamRankAffine &&
		numeric.data[0] == 2 && len(numeric.dict[0]) == 0 && len(numeric.dict[1]) == 0
	allIntegers := !rankNumber
	if allIntegers {
		s.integers = append(s.integers[:0], make([]int64, len(values))...)
		for i := range values {
			s.integers[i], allIntegers = CanonicalIntValue(values[i])
			if !allIntegers {
				break
			}
		}
	}
	if allIntegers {
		s.candidates[n] = s.encodeFOR(n, s.integers)
		n++
		s.candidates[n] = s.encodeDelta(n, s.integers)
		n++
		s.candidates[n] = s.encodeDeltaPack(n, s.integers)
		n++
	}
	allDates := !rankNumber
	if allDates {
		s.dates = append(s.dates[:0], make([]int32, len(values))...)
		for i := range values {
			s.dates[i], allDates = compactDateOrdinal(values[i])
			if !allDates {
				break
			}
		}
	}
	if allDates {
		s.candidates[n] = s.encodeDate(n, s.dates)
		n++
	}
	if hasPrefix {
		s.candidates[n] = numeric
		n++
	}
	bestBytes, bestSlot := frontBytes, -1
	if hasAlphabet && alphabet.encoded < bestBytes {
		bestBytes, bestSlot = alphabet.encoded, 2
	}
	for slot := 3; slot < n; slot++ {
		if candidate := s.candidates[slot]; candidate.encodedBytes() < bestBytes {
			bestBytes, bestSlot = candidate.encodedBytes(), slot
		}
	}
	preferDictionaryScan := len(s.dict[0]) <= compactDictionaryScanPreferred &&
		dictionaryBytes <= bestBytes+bestBytes/4
	if dictionaryBytes <= bestBytes || preferDictionaryScan {
		return s.finishDictionary(values)
	}
	if bestSlot < 0 {
		return s.encodeFront(1, values)
	}
	if bestSlot == 2 {
		return s.finishAlphabet(2, values, alphabet)
	}
	return s.candidates[bestSlot]
}

func TestCompactStreamAlphabetUnionPreservesPlannerOutput(t *testing.T) {
	values := compactAlphabetUnionRepeatedValues(129)
	unique := compactAlphabetUnionValues(4096)
	cases := []struct {
		name         string
		values       [][]byte
		wantKind     uint8
		assertWinner bool
	}{
		{name: "repeated-dictionary-values", values: values},
		{name: "all-unique-large", values: unique},
		{name: "alphabet-winner", values: compactVariedV1Values(2*compactStreamRestart + 3),
			wantKind: compactStreamAlphabet, assertWinner: true},
		{name: "integer-alternative", values: [][]byte{[]byte("100"), []byte("102"), []byte("104"), []byte("106"), []byte("108")},
			wantKind: compactStreamFOR, assertWinner: true},
		{name: "empty-and-affix", values: [][]byte{[]byte(`"a"`), []byte(`"aa"`), []byte(`"a"`), []byte(`"abca"`)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var optimized, reference compactStreamScratch
			got := optimized.encodeShape(tc.values, nil, 0)
			want := encodeCompactShapeBeforeAlphabetUnionReuse(&reference, tc.values, nil, 0, nil, nil)
			if tc.assertWinner && got.kind != tc.wantKind {
				t.Fatalf("normal planner kind=%d want=%d", got.kind, tc.wantKind)
			}
			assertCompactEncodingEqual(t, got, want)
			compactCodecRoundTrip(t, got, tc.values)
		})
	}

	view, _ := compactRankAffineFixture(t, 7)
	shape, shapeRows := -1, 0
	for candidate := 0; candidate < view.shapeCount; candidate++ {
		entry, ok := view.shapeEntry(candidate)
		if ok && entry.rows >= compactStreamRestart && entry.rows < view.rows {
			shape, shapeRows = candidate, entry.rows
			break
		}
	}
	if shape < 0 {
		t.Fatal("rank fixture has no shape suitable for rank-context encoding")
	}
	rankedValues := make([][]byte, 0, shapeRows)
	for rank := 0; rank < view.rows; rank++ {
		if view.rowShape(rank) == shape {
			rankedValues = append(rankedValues, AppendCanonicalInt(nil, 10000+7*int64(rank)))
		}
	}
	newContext := &compactRankContext{shape: shape, shapeRows: shapeRows}
	oldContext := &compactRankContext{shape: shape, shapeRows: shapeRows}
	var optimized, reference compactStreamScratch
	got := optimized.encodeShapeWithRankContext(rankedValues, nil, view.rows, newContext, &view)
	want := encodeCompactShapeBeforeAlphabetUnionReuse(
		&reference, rankedValues, nil, view.rows, oldContext, &view,
	)
	if got.kind != compactStreamRankAffine {
		t.Fatalf("rank-context fixture kind=%d want rank-affine", got.kind)
	}
	assertCompactEncodingEqual(t, got, want)
}

func TestCompactStreamAlphabetUnionScratchReuse(t *testing.T) {
	cases := [][][]byte{
		compactAlphabetUnionRepeatedValues(17),
		compactAlphabetUnionValues(129),
		compactAlphabetReservoirTestValues(64),
		[][]byte{[]byte(""), []byte(""), []byte("x")},
		compactAlphabetUnionRepeatedValues(4096),
	}
	var got, want compactStreamScratch
	for round := 0; round < 3; round++ {
		for _, values := range cases {
			gotEncoding := got.encode(values)
			wantEncoding := encodeCompactShapeBeforeAlphabetUnionReuse(&want, values, nil, 0, nil, nil)
			assertCompactEncodingEqual(t, gotEncoding, wantEncoding)
		}
	}
}

var compactAlphabetUnionBenchmarkPlan compactAlphabetPlan

func compactAlphabetUnionBenchmarkValues(rows, cardinality, width int) [][]byte {
	unique := make([][]byte, cardinality)
	for i := range cardinality {
		value := bytes.Repeat([]byte{'x'}, width)
		if cardinality <= 8 {
			// Model repeated, moderately wide spellings with a 64-byte varying
			// middle and long shared affixes, while keeping row width realistic.
			copy(value[:96], bytes.Repeat([]byte{'p'}, 96))
			for offset := 96; offset < width-96; offset++ {
				value[offset] = byte('a' + (i+offset/8)%cardinality)
			}
			copy(value[width-96:], bytes.Repeat([]byte{'z'}, 96))
		} else {
			// The unique control keeps the alphabet under its symbol limit so it
			// exercises the production all-unique path rather than early rejection.
			copy(value, []byte("key:"))
			copy(value[4:], leftPadDecimal(i, 4))
		}
		unique[i] = value
	}
	values := make([][]byte, rows)
	for row := range rows {
		values[row] = unique[row%cardinality]
	}
	return values
}

func compactAlphabetUnionInputBytes(values [][]byte) int64 {
	var total int64
	for _, value := range values {
		total += int64(len(value))
	}
	return total
}

func BenchmarkCompactAlphabetUnionMeasurement(b *testing.B) {
	for _, tc := range []struct {
		name   string
		values [][]byte
	}{
		{name: "repeated-eight-spellings-256B", values: compactAlphabetUnionBenchmarkValues(4096, 8, 256)},
		{name: "all-unique-256B", values: compactAlphabetUnionBenchmarkValues(4096, 4096, 256)},
	} {
		b.Run(tc.name+"/full-values", func(b *testing.B) {
			var scratch compactStreamScratch
			for range 2 {
				scratch.measureDictionary(tc.values)
				scratch.measureAlphabet(2, tc.values, 0)
			}
			wantEntries := 4096
			if tc.name == "repeated-eight-spellings-256B" {
				wantEntries = 8
			}
			if gotEntries := len(scratch.dict[0]); gotEntries != wantEntries {
				b.Fatalf("benchmark dictionary entries=%d want=%d", gotEntries, wantEntries)
			}
			b.ReportAllocs()
			b.SetBytes(compactAlphabetUnionInputBytes(tc.values))
			b.ResetTimer()
			for range b.N {
				scratch.measureDictionary(tc.values)
				plan, ok := scratch.measureAlphabet(2, tc.values, 0)
				if !ok {
					b.Fatal("full-row alphabet scan rejected benchmark values")
				}
				compactAlphabetUnionBenchmarkPlan = plan
			}
		})
		b.Run(tc.name+"/dictionary-representatives", func(b *testing.B) {
			var scratch compactStreamScratch
			for range 2 {
				scratch.measureDictionary(tc.values)
				spellings := tc.values
				if len(scratch.dict[0]) < len(tc.values) {
					spellings = scratch.dict[0]
				}
				scratch.measureAlphabetWithSpellings(2, tc.values, spellings, 0)
			}
			wantEntries := 4096
			if tc.name == "repeated-eight-spellings-256B" {
				wantEntries = 8
			}
			if gotEntries := len(scratch.dict[0]); gotEntries != wantEntries {
				b.Fatalf("benchmark dictionary entries=%d want=%d", gotEntries, wantEntries)
			}
			b.ReportAllocs()
			b.SetBytes(compactAlphabetUnionInputBytes(tc.values))
			b.ResetTimer()
			for range b.N {
				scratch.measureDictionary(tc.values)
				spellings := tc.values
				if len(scratch.dict[0]) < len(tc.values) {
					spellings = scratch.dict[0]
				}
				plan, ok := scratch.measureAlphabetWithSpellings(2, tc.values, spellings, 0)
				if !ok {
					b.Fatal("dictionary-representative alphabet scan rejected benchmark values")
				}
				compactAlphabetUnionBenchmarkPlan = plan
			}
		})
	}
}
