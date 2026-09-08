package storeio

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"testing"
)

func compressedDictionaryTestValues(distinct, rows int) [][]byte {
	unique := make([][]byte, distinct)
	for id := range unique {
		token := bytes.Repeat([]byte(fmt.Sprintf("node-%02d/checkpoint/", id)), 8)
		unique[id] = append([]byte{'"'}, token...)
		unique[id] = append(unique[id], token...)
		unique[id] = append(unique[id], '"')
	}
	values := make([][]byte, rows)
	for row := range values {
		values[row] = unique[row%len(unique)]
	}
	return values
}

func TestCompactCompressedDictionaryRoundTrip(t *testing.T) {
	values := compressedDictionaryTestValues(8, 64)
	encoded := encodeCompactScalarStream(values)
	if encoded.kind != compactStreamCompressedDictionary {
		t.Fatalf("kind = %d, want compressed dictionary", encoded.kind)
	}
	binaryStream, err := encoded.appendBinary(nil)
	if err != nil {
		t.Fatal(err)
	}
	view, err := openCompactStream(binaryStream)
	if err != nil {
		t.Fatal(err)
	}
	for row, want := range values {
		prefix := []byte(`{"value":`)
		got, ok := view.appendValue(prefix, row)
		if !ok || !bytes.Equal(got, append(append([]byte(nil), prefix...), want...)) {
			t.Fatalf("row %d append = %q, %v", row, got, ok)
		}
		length, peak, ok := compactProjectionValueLen(view, row)
		if !ok || length != len(want) || peak != len(want) {
			t.Fatalf("row %d projection length = %d/%d/%v, want %d", row, length, peak, ok, len(want))
		}
	}
	needle := values[3]
	matched, scratch, supported := view.countSpellingEqual(needle, make([]byte, 0, len(needle)))
	if !supported || matched != 8 || !bytes.Equal(scratch, needle) {
		t.Fatalf("count = %d/%v scratch=%q", matched, supported, scratch)
	}
	dst := make([]byte, 0, len(values[0])+16)
	if allocs := testing.AllocsPerRun(100, func() {
		var ok bool
		dst, ok = view.appendValue(dst[:0], 17)
		if !ok {
			panic("decode")
		}
	}); allocs != 0 {
		t.Fatalf("warm point allocations = %.2f", allocs)
	}
	if allocs := testing.AllocsPerRun(100, func() {
		if _, err := openCompactStream(binaryStream); err != nil {
			panic(err)
		}
	}); allocs != 0 {
		t.Fatalf("validation allocations = %.2f", allocs)
	}
}

func TestCompactCompressedDictionaryRawEntry(t *testing.T) {
	value := []byte(`"raw dictionary spelling"`)
	encoded := compactStreamEncoding{
		kind: compactStreamCompressedDictionary, count: 2,
		dict: [][]byte{append([]byte{0}, value...)},
	}
	binaryStream, err := encoded.appendBinary(nil)
	if err != nil {
		t.Fatal(err)
	}
	view, err := openCompactStream(binaryStream)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := view.appendValue(nil, 1)
	if !ok || !bytes.Equal(got, value) {
		t.Fatalf("raw entry = %q, %v", got, ok)
	}
}

func TestCompactCompressedDictionaryRejectsMalformedEntries(t *testing.T) {
	values := compressedDictionaryTestValues(2, 8)
	encoded := encodeCompactScalarStream(values)
	binaryStream, err := encoded.appendBinary(nil)
	if err != nil {
		t.Fatal(err)
	}
	dictStart := compactStreamHeader + 2*len(encoded.dict)
	tests := map[string]func([]byte){
		"tag": func(src []byte) { src[dictStart] = 2 },
		"decoded length": func(src []byte) {
			src[dictStart] = 1
			for at := dictStart + 1; at < dictStart+10; at++ {
				src[at] = 0xff
			}
		},
		"body": func(src []byte) { src[dictStart+3] ^= 0xff },
	}
	for name, corrupt := range tests {
		t.Run(name, func(t *testing.T) {
			src := append([]byte(nil), binaryStream...)
			corrupt(src)
			if _, err := openCompactStream(src); !errors.Is(err, ErrCommonPrimaryLeafCorrupt) {
				t.Fatalf("error = %v", err)
			}
		})
	}
	oversizeEntry := append([]byte{1}, binary.AppendUvarint(nil, compactCompressedDictionaryMaxValueBytes+1)...)
	oversizeEntry = append(oversizeEntry, 0)
	oversize := compactStreamEncoding{
		kind: compactStreamCompressedDictionary, count: 1,
		dict: [][]byte{oversizeEntry},
	}
	src, err := oversize.appendBinary(nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := openCompactStream(src); !errors.Is(err, ErrCommonPrimaryLeafCorrupt) {
		t.Fatalf("oversize error = %v", err)
	}
}

func TestCompactCompressedDictionaryExcludesKeysAndLargeValues(t *testing.T) {
	values := compressedDictionaryTestValues(4, 64)
	var scratch compactStreamScratch
	keys := make([][]byte, len(values))
	for i := range values {
		keys[i] = append([]byte(nil), values[i]...)
	}
	if got := scratch.encodeKeys(keys); got.kind == compactStreamCompressedDictionary {
		t.Fatal("quoted opaque keys selected scalar compression")
	}
	if scratch.compressor != nil {
		t.Fatal("key planning allocated an LZ4 compressor")
	}
	large := bytes.Repeat([]byte("repeated-large-value"), compactCompressedDictionaryMaxValueBytes/20+2)
	large = append([]byte{'"'}, large...)
	large = append(large, '"')
	for i := range values {
		values[i] = large
	}
	if got := scratch.encode(values); got.kind == compactStreamCompressedDictionary {
		t.Fatal("value above decoded cap selected compression")
	}
	if scratch.compressor != nil {
		t.Fatal("large-value rejection allocated an LZ4 compressor")
	}
}

func TestCompactCompressedDictionaryWarmEncodeAllocations(t *testing.T) {
	accepted := compressedDictionaryTestValues(8, 64)
	const alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz-_=+,.!?@#$%^&*()[]{}:;<>/|~"
	rejected := make([][]byte, 64)
	state := uint64(0xdecafbad12345678)
	for row := range rejected {
		value := make([]byte, 202)
		value[0], value[len(value)-1] = '"', '"'
		for at := 1; at < len(value)-1; at++ {
			state += 0x9e3779b97f4a7c15
			state = (state ^ state>>30) * 0xbf58476d1ce4e5b9
			state = (state ^ state>>27) * 0x94d049bb133111eb
			state ^= state >> 31
			value[at] = alphabet[state%uint64(len(alphabet))]
		}
		rejected[row] = value
	}
	for name, values := range map[string][][]byte{
		"accepted": accepted,
		"rejected": rejected,
	} {
		t.Run(name, func(t *testing.T) {
			var scratch compactStreamScratch
			first := scratch.encode(values)
			if name == "accepted" && first.kind != compactStreamCompressedDictionary {
				t.Fatalf("warmup kind = %d, want compressed", first.kind)
			}
			if name == "rejected" && first.kind == compactStreamCompressedDictionary {
				t.Fatal("incompressible warmup unexpectedly compressed")
			}
			if scratch.compressor == nil {
				t.Fatal("warmup did not exercise compression candidate")
			}
			if allocs := testing.AllocsPerRun(100, func() {
				encoded := scratch.encode(values)
				if (name == "accepted") != (encoded.kind == compactStreamCompressedDictionary) {
					panic("admission changed")
				}
			}); allocs != 0 {
				t.Fatalf("warm encode allocations = %.2f", allocs)
			}
		})
	}
}

func TestCompactCompressedDictionaryPreparedScanFragments(t *testing.T) {
	values := compressedDictionaryTestValues(8, 64)
	records := make([]CommonPrimaryLeafRecord, len(values))
	for row := range records {
		records[row] = CommonPrimaryLeafRecord{
			Key:   []byte(fmt.Sprintf("row-%04d", row)),
			Value: CommonPrimaryLeafValue{Inline: append(append([]byte(`{"value":`), values[row]...), '}')},
		}
	}
	view := compactProjectionTestView(t, records)
	var decoder CompactPrimaryScanDecoder
	decoder.prepare(&view, 0)
	if !decoder.supported {
		t.Fatal("scan decoder declined ordinary compressed dictionary leaf")
	}
	streamAt := -1
	for at := range decoder.streamView {
		if decoder.streamView[at].kind == compactStreamCompressedDictionary {
			streamAt = at
			break
		}
	}
	if streamAt < 0 || decoder.streamPlan[streamAt].dictionaryCount == 0 {
		t.Fatal("compressed dictionary was not prepared into fragment pool")
	}
	for row := range records {
		got, ok := decoder.appendDictionaryFragment(nil, streamAt, row)
		if !ok || !bytes.Contains(got, values[row]) {
			t.Fatalf("row %d prepared fragment = %q, %v", row, got, ok)
		}
	}
}

func TestCompactCompressedDictionaryProjectionLifetimeAndBound(t *testing.T) {
	values := compressedDictionaryTestValues(8, 64)
	records := make([]CommonPrimaryLeafRecord, len(values))
	for row := range records {
		document := append([]byte(`{"first":`), values[row]...)
		document = append(document, `,"second":"tail"}`...)
		records[row] = CommonPrimaryLeafRecord{
			Key:   []byte(fmt.Sprintf("row-%04d", row)),
			Value: CommonPrimaryLeafValue{Inline: document},
		}
	}
	view := compactProjectionTestView(t, records)
	filter, err := NewUnifiedProjectionFilter([][]byte{[]byte("/first"), []byte("/second")})
	if err != nil {
		t.Fatal(err)
	}
	seen := make([]int, 1)
	shapes := make([]UnifiedProjectionShapeWorkspace, 1)
	streams := make([]UnifiedProjectionStreamWorkspace, 2)
	fields := make([]UnifiedProjectionField, 2)
	callbacks := 0
	supported, stopped, scratch, err := view.VisitResolvedProjection(
		filter.resolvers, seen, shapes, streams, fields,
		make([]byte, 0, len(values[0])+32), -1,
		func(row int, fields []UnifiedProjectionField) error {
			callbacks++
			if !bytes.Equal(fields[0].JSON, values[row]) || !bytes.Equal(fields[1].JSON, []byte(`"tail"`)) {
				t.Fatalf("row %d projection changed prior field: %q / %q", row, fields[0].JSON, fields[1].JSON)
			}
			return nil
		},
	)
	if err != nil || !supported || stopped || callbacks != len(records) {
		t.Fatalf("projection supported=%v stopped=%v callbacks=%d err=%v", supported, stopped, callbacks, err)
	}
	tooSmall := make([]byte, 0, len(values[0])-1)
	supported, stopped, tooSmall, err = view.VisitResolvedProjection(
		filter.resolvers, seen, shapes, streams, fields, tooSmall, -1,
		func(int, []UnifiedProjectionField) error {
			t.Fatal("insufficient scratch published a row")
			return nil
		},
	)
	if err != nil || supported || stopped || len(tooSmall) != 0 || cap(tooSmall) != len(values[0])-1 {
		t.Fatalf("small scratch supported=%v stopped=%v len/cap=%d/%d err=%v", supported, stopped, len(tooSmall), cap(tooSmall), err)
	}
	_ = scratch
}

func TestCompactCompressedDictionaryAlternatingWarmWorkspaceAllocations(t *testing.T) {
	cardinalities := []int{2, 8, 16, 64}
	accepted := make([][][]byte, len(cardinalities))
	for at, cardinality := range cardinalities {
		accepted[at] = compressedDictionaryTestValues(cardinality, 64)
	}
	rejected := make([][]byte, 64)
	for row := range rejected {
		value := make([]byte, 202)
		value[0], value[len(value)-1] = '"', '"'
		for at := 1; at < len(value)-1; at++ {
			value[at] = byte(33 + (row*79+at*43+row*at*17)%90)
			if value[at] == '"' || value[at] == '\\' {
				value[at] = '~'
			}
		}
		rejected[row] = value
	}
	var scratch compactStreamScratch
	wire := make([]byte, 0, CommonPrimaryLeafMaxExtentBytes)
	// Warm the largest cardinality and both admission outcomes before measuring.
	for _, set := range accepted {
		encoded := scratch.encode(set)
		var err error
		wire, err = encoded.appendBinary(wire[:0])
		if err != nil {
			t.Fatal(err)
		}
	}
	encoded := scratch.encode(rejected)
	var err error
	wire, err = encoded.appendBinary(wire[:0])
	if err != nil {
		t.Fatal(err)
	}
	if allocs := testing.AllocsPerRun(100, func() {
		for _, set := range accepted {
			encoded := scratch.encode(set)
			var err error
			wire, err = encoded.appendBinary(wire[:0])
			if err != nil {
				panic(err)
			}
		}
		encoded := scratch.encode(rejected)
		var err error
		wire, err = encoded.appendBinary(wire[:0])
		if err != nil {
			panic(err)
		}
	}); allocs != 0 {
		t.Fatalf("alternating warm encode+framing allocations = %.2f", allocs)
	}
}

func compressedDictionaryScanFixture(t testing.TB, distinct, fields int) (CompactPrimaryStripeView, [][]byte) {
	t.Helper()
	values := compressedDictionaryTestValues(distinct, 64)
	records := make([]CommonPrimaryLeafRecord, len(values))
	documents := make([][]byte, len(values))
	for row := range records {
		document := []byte{'{'}
		for field := range fields {
			if field != 0 {
				document = append(document, ',')
			}
			document = fmt.Appendf(document, `"value%d":`, field)
			document = append(document, values[row]...)
		}
		document = append(document, '}')
		documents[row] = document
		records[row] = CommonPrimaryLeafRecord{
			Key: []byte(fmt.Sprintf("row-%04d", row)), Value: CommonPrimaryLeafValue{Inline: document},
		}
	}
	return compactProjectionTestView(t, records), documents
}

func TestCompactCompressedDictionaryScanPoolFallbackAndReuse(t *testing.T) {
	fit, fitDocuments := compressedDictionaryScanFixture(t, 8, 1)
	var decoder CompactPrimaryScanDecoder
	decoder.prepare(&fit, 0)
	if !decoder.supported || decoder.streamPlan[0].dictionaryCount == 0 {
		t.Fatal("fit fixture did not prepare compressed fragments")
	}
	dst := make([]byte, 0, 1024)
	for row := range fitDocuments {
		got, ok := decoder.appendValue(dst[:0], &fit, 0, row, 0, row)
		if !ok || !bytes.Equal(got, fitDocuments[row]) {
			t.Fatalf("fit row %d = %q, %v", row, got, ok)
		}
	}
	if allocs := testing.AllocsPerRun(100, func() {
		var ok bool
		dst, ok = decoder.appendValue(dst[:0], &fit, 0, 0, 0, 0)
		if !ok {
			panic("scan")
		}
	}); allocs != 0 {
		t.Fatalf("warm prepared scan allocations = %.2f", allocs)
	}

	exhausted, exhaustedDocuments := compressedDictionaryScanFixture(t, 16, 2)
	decoder = CompactPrimaryScanDecoder{}
	decoder.prepare(&exhausted, 0)
	if !decoder.supported {
		t.Fatal("pool exhaustion disabled bounded scan fallback")
	}
	prepared, fallback := 0, 0
	for at := 0; at < 2; at++ {
		if decoder.streamPlan[at].dictionaryCount == 0 {
			fallback++
		} else {
			prepared++
		}
	}
	if prepared != 1 || fallback != 1 {
		t.Fatalf("two-hole pool plans prepared/fallback = %d/%d, want 1/1", prepared, fallback)
	}
	for row := range exhaustedDocuments {
		got, ok := decoder.appendValue(dst[:0], &exhausted, 0, row, 0, row)
		if !ok || !bytes.Equal(got, exhaustedDocuments[row]) {
			t.Fatalf("fallback row %d = %q, %v", row, got, ok)
		}
	}

	other, otherDocuments := compressedDictionaryScanFixture(t, 4, 1)
	other.header.Generation = fit.header.Generation + 1
	got, ok := decoder.appendValue(dst[:0], &fit, 7, 0, 0, 0)
	if !ok || !bytes.Equal(got, fitDocuments[0]) {
		t.Fatalf("bucket reset = %q, %v", got, ok)
	}
	got, ok = decoder.appendValue(dst[:0], &other, 7, 0, 0, 0)
	if !ok || !bytes.Equal(got, otherDocuments[0]) {
		t.Fatalf("generation reset reused stale plan = %q, %v", got, ok)
	}
}

func TestCompactCompressedDictionaryAdmissionLeavesOrdinaryValuesAlone(t *testing.T) {
	values := make([][]byte, 64)
	for row := range values {
		values[row] = []byte(fmt.Sprintf(`"user-%08d"`, row))
	}
	if got := encodeCompactScalarStream(values); got.kind == compactStreamCompressedDictionary {
		t.Fatal("ordinary short strings selected compression")
	}
}
