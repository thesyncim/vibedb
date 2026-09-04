package storeio

import (
	"encoding/binary"
	"fmt"
	"testing"
)

const compactPackedBenchmarkLabelSalt = uint64(0x9e3779b97f4a7c15)

func compactPackedBenchmarkLabel(row int) []byte {
	id := uint64((row*73)&127) + 1
	return fmt.Appendf(nil, `"c%016x"`, id*compactPackedBenchmarkLabelSalt)
}

func compactPackedBenchmarkNumber(row int) []byte {
	return fmt.Appendf(nil, "%d", ((row*73)&1023)-512)
}

func compactPackedBenchmarkStream(
	t testing.TB,
	count, width int, kind uint8,
) compactStreamView {
	t.Helper()
	values := make([][]byte, count)
	for row := range values {
		if width == 7 {
			values[row] = compactPackedBenchmarkLabel(row)
		} else {
			values[row] = compactPackedBenchmarkNumber(row)
		}
	}
	encoded := encodeCompactScalarStream(values)
	if int(encoded.width) != width || encoded.kind != kind {
		t.Fatalf(
			"benchmark stream kind=%d width=%d, want kind=%d width=%d",
			encoded.kind, encoded.width, kind, width,
		)
	}
	raw, err := encoded.appendBinary(nil)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := openCompactStream(raw)
	if err != nil {
		t.Fatal(err)
	}
	return stream
}

// The wide fixtures are kept separate from compactPackedBenchmarkStream so
// the established 7/10 corpus and benchmark setup remain byte-for-byte stable
// for before/after comparisons. Both values below are the exact durable query
// corpus formulas used by query/file_packed_count_bench_test.go.
func compactPackedBenchmarkLabel8(row int) []byte {
	id := uint64((row * 73) & 255)
	return fmt.Appendf(nil, `"c%016x"`, (id+1)*compactPackedBenchmarkLabelSalt)
}

func compactPackedBenchmarkNumber16(row int) []byte {
	return fmt.Appendf(nil, "%d", ((row*32749)&65535)-32768)
}

func compactPackedBenchmarkNumber16Value(row int) int64 {
	return int64(((row * 32749) & 65535) - 32768)
}

func compactPackedBenchmarkStreamValues(
	t testing.TB, values [][]byte, width int, kind uint8,
) compactStreamView {
	t.Helper()
	encoded := encodeCompactScalarStream(values)
	if int(encoded.width) != width || encoded.kind != kind {
		t.Fatalf(
			"benchmark stream kind=%d width=%d, want kind=%d width=%d",
			encoded.kind, encoded.width, kind, width,
		)
	}
	raw, err := encoded.appendBinary(nil)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := openCompactStream(raw)
	if err != nil {
		t.Fatal(err)
	}
	return stream
}

func compactPackedBenchmarkStream8(t testing.TB, count int) compactStreamView {
	t.Helper()
	values := make([][]byte, count)
	for row := range count {
		values[row] = compactPackedBenchmarkLabel8(row)
	}
	stream := compactPackedBenchmarkStreamValues(
		t, values, 8, compactStreamDictionary,
	)
	if stream.dictCount != 256 {
		t.Fatalf("dictionary8 stream dictionary=%d, want 256", stream.dictCount)
	}
	return stream
}

func compactPackedBenchmarkStream16(t testing.TB, count int) compactStreamView {
	t.Helper()
	values := make([][]byte, count)
	for row := range count {
		values[row] = compactPackedBenchmarkNumber16(row)
	}
	stream := compactPackedBenchmarkStreamValues(t, values, 16, compactStreamFOR)
	if base := int64(binary.LittleEndian.Uint64(stream.data[:8])); base != -32768 {
		t.Fatalf("FOR16 stream base=%d, want -32768", base)
	}
	return stream
}

func compactPackedBenchmarkNumber16Needle() int64 {
	return compactPackedBenchmarkNumber16Value(17)
}

func compactPackedBenchmarkNumber16Expected(count int) int {
	needle := compactPackedBenchmarkNumber16Needle()
	want := 0
	for row := range count {
		if compactPackedBenchmarkNumber16Value(row) == needle {
			want++
		}
	}
	return want
}

func compactPackedBenchmarkDictionaryID(t testing.TB, stream compactStreamView, needle []byte) uint64 {
	t.Helper()
	for id := 0; id < stream.dictCount; id++ {
		value, _ := stream.dictionaryEntry(id)
		if string(value) == string(needle) {
			return uint64(id)
		}
	}
	t.Fatalf("dictionary needle %q missing", needle)
	return 0
}

func BenchmarkCompactStreamPackedEquality7(b *testing.B) {
	stream := compactPackedBenchmarkStream(b, 4096, 7, compactStreamDictionary)
	needle := compactPackedBenchmarkLabel(17)
	want := compactPackedBenchmarkDictionaryID(b, stream, needle)
	const expected = 32
	b.ReportAllocs()
	b.ReportMetric(float64(stream.count), "rows")
	b.ResetTimer()
	for range b.N {
		matched := countCompactPackedEqual(stream.data, stream.count, 7, want)
		if matched != expected {
			b.Fatalf("dictionary7 count=%d expected=%d", matched, expected)
		}
	}
}

func BenchmarkCompactStreamSpellingEquality7(b *testing.B) {
	stream := compactPackedBenchmarkStream(b, 4096, 7, compactStreamDictionary)
	needle := compactPackedBenchmarkLabel(17)
	const expected = 32
	b.ReportAllocs()
	b.ReportMetric(float64(stream.count), "rows")
	b.ResetTimer()
	for range b.N {
		matched, _, ok := stream.countSpellingEqual(needle, nil)
		if !ok || matched != expected {
			b.Fatalf("dictionary7 count=%d expected=%d ok=%v", matched, expected, ok)
		}
	}
}

func BenchmarkCompactStreamPackedEquality10(b *testing.B) {
	stream := compactPackedBenchmarkStream(b, 4096, 10, compactStreamFOR)
	const (
		want     = uint64(17 - (-512))
		expected = 4
	)
	b.ReportAllocs()
	b.ReportMetric(float64(stream.count), "rows")
	b.ResetTimer()
	for range b.N {
		matched := countCompactPackedEqual(stream.data[8:], stream.count, 10, want)
		if matched != expected {
			b.Fatalf("FOR10 count=%d expected=%d", matched, expected)
		}
	}
}

func BenchmarkCompactStreamIntegerEquality10(b *testing.B) {
	stream := compactPackedBenchmarkStream(b, 4096, 10, compactStreamFOR)
	const (
		needle   = int64(17)
		expected = 4
	)
	b.ReportAllocs()
	b.ReportMetric(float64(stream.count), "rows")
	b.ResetTimer()
	for range b.N {
		matched, ok := stream.countIntegerEqual(needle)
		if !ok || matched != expected {
			b.Fatalf("FOR10 count=%d expected=%d ok=%v", matched, expected, ok)
		}
	}
}

func BenchmarkCompactStreamPackedEquality8(b *testing.B) {
	stream := compactPackedBenchmarkStream8(b, 4096)
	needle := compactPackedBenchmarkLabel8(17)
	want := compactPackedBenchmarkDictionaryID(b, stream, needle)
	expected := 0
	for row := range stream.count {
		if (row*73)&255 == (17*73)&255 {
			expected++
		}
	}
	matched := countCompactPackedEqual(stream.data, stream.count, 8, want)
	if matched != expected {
		b.Fatalf("dictionary8 warm count=%d expected=%d", matched, expected)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		matched = countCompactPackedEqual(stream.data, stream.count, 8, want)
	}
	b.StopTimer()
	if matched != expected {
		b.Fatalf("dictionary8 count=%d expected=%d", matched, expected)
	}
	b.ReportMetric(float64(stream.count), "rows")
}

func BenchmarkCompactStreamSpellingEquality8(b *testing.B) {
	stream := compactPackedBenchmarkStream8(b, 4096)
	needle := compactPackedBenchmarkLabel8(17)
	expected := 0
	for row := range stream.count {
		if (row*73)&255 == (17*73)&255 {
			expected++
		}
	}
	matched, scratch, ok := stream.countSpellingEqual(needle, nil)
	if !ok || matched != expected {
		b.Fatalf("dictionary8 warm count=%d expected=%d ok=%v", matched, expected, ok)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		matched, scratch, ok = stream.countSpellingEqual(needle, scratch)
	}
	b.StopTimer()
	if !ok || matched != expected {
		b.Fatalf("dictionary8 count=%d expected=%d ok=%v", matched, expected, ok)
	}
	b.ReportMetric(float64(stream.count), "rows")
}

func BenchmarkCompactStreamPackedEquality16(b *testing.B) {
	stream := compactPackedBenchmarkStream16(b, 4096)
	needle := compactPackedBenchmarkNumber16Needle()
	base := int64(binary.LittleEndian.Uint64(stream.data[:8]))
	want := uint64(needle - base)
	expected := compactPackedBenchmarkNumber16Expected(stream.count)
	matched := countCompactPackedEqual(stream.data[8:], stream.count, 16, want)
	if matched != expected {
		b.Fatalf("FOR16 warm count=%d expected=%d", matched, expected)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		matched = countCompactPackedEqual(stream.data[8:], stream.count, 16, want)
	}
	b.StopTimer()
	if matched != expected {
		b.Fatalf("FOR16 count=%d expected=%d", matched, expected)
	}
	b.ReportMetric(float64(stream.count), "rows")
}

func BenchmarkCompactStreamIntegerEquality16(b *testing.B) {
	stream := compactPackedBenchmarkStream16(b, 4096)
	needle := compactPackedBenchmarkNumber16Needle()
	expected := compactPackedBenchmarkNumber16Expected(stream.count)
	matched, ok := stream.countIntegerEqual(needle)
	if !ok || matched != expected {
		b.Fatalf("FOR16 warm count=%d expected=%d ok=%v", matched, expected, ok)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		matched, ok = stream.countIntegerEqual(needle)
	}
	b.StopTimer()
	if !ok || matched != expected {
		b.Fatalf("FOR16 count=%d expected=%d ok=%v", matched, expected, ok)
	}
	b.ReportMetric(float64(stream.count), "rows")
}

// TestCompactPackedBenchmarkWidths is deliberately a Test rather than only a
// benchmark. CI must fail if the production encoder stops selecting the exact
// dictionary8 or FOR16 representation that the kernel measurements describe.
func TestCompactPackedBenchmarkWidths(t *testing.T) {
	const count = 4096
	t.Run("dictionary8", func(t *testing.T) {
		stream := compactPackedBenchmarkStream8(t, count)
		needle := compactPackedBenchmarkLabel8(17)
		wantID := compactPackedBenchmarkDictionaryID(t, stream, needle)
		want := 0
		for row := range count {
			if (row*73)&255 == (17*73)&255 {
				want++
			}
		}
		if got := countCompactPackedEqual(stream.data, stream.count, 8, wantID); got != want {
			t.Fatalf("dictionary8 count=%d want=%d", got, want)
		}
		if got, _, ok := stream.countSpellingEqual(needle, nil); !ok || got != want {
			t.Fatalf("dictionary8 spelling count=%d want=%d ok=%v", got, want, ok)
		}
	})
	t.Run("FOR16", func(t *testing.T) {
		stream := compactPackedBenchmarkStream16(t, count)
		needle := compactPackedBenchmarkNumber16Needle()
		want := compactPackedBenchmarkNumber16Expected(count)
		base := int64(binary.LittleEndian.Uint64(stream.data[:8]))
		if got := countCompactPackedEqual(
			stream.data[8:], stream.count, 16, uint64(needle-base),
		); got != want {
			t.Fatalf("FOR16 packed count=%d want=%d", got, want)
		}
		if got, ok := stream.countIntegerEqual(needle); !ok || got != want {
			t.Fatalf("FOR16 integer count=%d want=%d ok=%v", got, want, ok)
		}
	})
}

func BenchmarkCompactStreamPackedEqualityThresholds(b *testing.B) {
	// Widths 6 and 32 keep the generic scalar path in the same short-input
	// comparison as the specialized kernels.
	for _, width := range []int{6, 7, 8, 10, 16, 32} {
		for _, count := range []int{0, 1, 8, 16, 31, 32, 64} {
			name := fmt.Sprintf("width%d-count%d", width, count)
			b.Run(name, func(b *testing.B) {
				data := make([]byte, (count*width+7)/8)
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					if matched := countCompactPackedEqual(data, count, width, 0); matched != count {
						b.Fatalf("count=%d matched=%d", count, matched)
					}
				}
				b.StopTimer()
				b.ReportMetric(float64(count), "rows")
			})
		}
	}
}

func compactPackedBenchmarkStripe(
	t testing.TB,
) (CompactPrimaryStripeView, UnifiedHoleResolver, UnifiedHoleResolver, []byte, int) {
	t.Helper()
	const count = 4096
	records := make([]CommonPrimaryLeafRecord, count)
	for row := range records {
		records[row] = CommonPrimaryLeafRecord{
			Key: fmt.Appendf(nil, "row-%07d", row),
			Value: CommonPrimaryLeafValue{Inline: fmt.Appendf(
				nil,
				`{"label":"c%016x","n":%d}`,
				(uint64((row*73)&127)+1)*compactPackedBenchmarkLabelSalt,
				((row*73)&1023)-512,
			)},
		}
	}
	builder := NewUnifiedPrimaryLeafBuilder()
	payload, err := BuildCompactPrimaryStripePayload(records, builder)
	if err != nil {
		t.Fatal(err)
	}
	extent := int(physicalPageQuantum)
	for extent < PageHeaderSize+len(payload)+PageTrailerSize {
		extent <<= 1
	}
	storeID := unifiedTestStoreID()
	page, err := EncodeCompactPrimaryStripe(
		make([]byte, extent),
		CommonPrimaryLeafHeader{
			StoreID: storeID, Generation: 1, Bucket: 0,
			PageSize: uint32(extent),
		}, records, builder,
	)
	if err != nil {
		t.Fatal(err)
	}
	logicalID, ok := CommonPrimaryLeafLogicalID(0)
	if !ok {
		t.Fatal("benchmark stripe logical id")
	}
	view, err := OpenCompactPrimaryStripe(
		page, storeID, 0,
		PageRef{
			Offset: 4096, Length: uint32(extent),
			LogicalID: logicalID, Generation: 1,
			Kind: PagePrimaryLeaf,
		},
		1, unifiedTestBounds(),
	)
	if err != nil {
		t.Fatal(err)
	}
	var labelResolver UnifiedHoleResolver
	if err := labelResolver.SetPath([]byte("/label")); err != nil {
		t.Fatal(err)
	}
	var numberResolver UnifiedHoleResolver
	if err := numberResolver.SetPath([]byte("/n")); err != nil {
		t.Fatal(err)
	}
	labelStream := compactPackedBenchmarkResolvedStream(t, view, &labelResolver)
	if labelStream.kind != compactStreamDictionary || labelStream.width != 7 ||
		labelStream.count != count {
		t.Fatalf(
			"stripe label stream kind=%d width=%d count=%d, want dictionary7/%d",
			labelStream.kind, labelStream.width, labelStream.count, count,
		)
	}
	numberStream := compactPackedBenchmarkResolvedStream(t, view, &numberResolver)
	if numberStream.kind != compactStreamFOR || numberStream.width != 10 ||
		numberStream.count != count {
		t.Fatalf(
			"stripe number stream kind=%d width=%d count=%d, want FOR10/%d",
			numberStream.kind, numberStream.width, numberStream.count, count,
		)
	}
	needle := compactPackedBenchmarkLabel(17)
	expected := 0
	for row := range records {
		if string(compactPackedBenchmarkLabel(row)) == string(needle) {
			expected++
		}
	}
	return view, labelResolver, numberResolver, needle, expected
}

func compactPackedBenchmarkResolvedStream(
	t testing.TB,
	view CompactPrimaryStripeView,
	resolver *UnifiedHoleResolver,
) compactStreamView {
	t.Helper()
	if view.shapeCount != 1 {
		t.Fatalf("benchmark stripe shape count=%d, want 1", view.shapeCount)
	}
	entry, ok := view.shapeEntry(0)
	if !ok {
		t.Fatal("benchmark stripe shape")
	}
	hole := resolver.resolveCompactTemplate(entry.template)
	if hole < 0 || hole >= entry.template.holes {
		t.Fatalf("benchmark stripe hole=%d", hole)
	}
	streamRaw := entry.streamRaw
	for at := 0; at <= hole; at++ {
		stream, admitted := admittedCompactStream(streamRaw)
		if !admitted {
			t.Fatal("benchmark stripe stream admission")
		}
		if at == hole {
			return stream
		}
		streamRaw = streamRaw[stream.encoded:]
	}
	panic("unreachable")
}

func compactPackedBenchmarkStripeWide(
	t testing.TB,
) (CompactPrimaryStripeView, UnifiedHoleResolver, UnifiedHoleResolver, []byte, int, int64, int) {
	t.Helper()
	const count = 4096
	records := make([]CommonPrimaryLeafRecord, count)
	for row := range records {
		records[row] = CommonPrimaryLeafRecord{
			Key: fmt.Appendf(nil, "row-%07d", row),
			Value: CommonPrimaryLeafValue{Inline: fmt.Appendf(
				nil,
				`{"label":"c%016x","n":%d}`,
				(uint64((row*73)&255)+1)*compactPackedBenchmarkLabelSalt,
				compactPackedBenchmarkNumber16Value(row),
			)},
		}
	}
	builder := NewUnifiedPrimaryLeafBuilder()
	payload, err := BuildCompactPrimaryStripePayload(records, builder)
	if err != nil {
		t.Fatal(err)
	}
	extent := int(physicalPageQuantum)
	for extent < PageHeaderSize+len(payload)+PageTrailerSize {
		extent <<= 1
	}
	storeID := unifiedTestStoreID()
	page, err := EncodeCompactPrimaryStripe(
		make([]byte, extent),
		CommonPrimaryLeafHeader{
			StoreID: storeID, Generation: 1, Bucket: 0,
			PageSize: uint32(extent),
		}, records, builder,
	)
	if err != nil {
		t.Fatal(err)
	}
	logicalID, ok := CommonPrimaryLeafLogicalID(0)
	if !ok {
		t.Fatal("benchmark stripe logical id")
	}
	view, err := OpenCompactPrimaryStripe(
		page, storeID, 0,
		PageRef{
			Offset: 4096, Length: uint32(extent),
			LogicalID: logicalID, Generation: 1,
			Kind: PagePrimaryLeaf,
		},
		1, unifiedTestBounds(),
	)
	if err != nil {
		t.Fatal(err)
	}
	var labelResolver UnifiedHoleResolver
	if err := labelResolver.SetPath([]byte("/label")); err != nil {
		t.Fatal(err)
	}
	var numberResolver UnifiedHoleResolver
	if err := numberResolver.SetPath([]byte("/n")); err != nil {
		t.Fatal(err)
	}
	labelStream := compactPackedBenchmarkResolvedStream(t, view, &labelResolver)
	if labelStream.kind != compactStreamDictionary || labelStream.width != 8 ||
		labelStream.count != count || labelStream.dictCount != 256 {
		t.Fatalf(
			"stripe label stream kind=%d width=%d dict=%d count=%d, want dictionary8/256/%d",
			labelStream.kind, labelStream.width, labelStream.dictCount,
			labelStream.count, count,
		)
	}
	numberStream := compactPackedBenchmarkResolvedStream(t, view, &numberResolver)
	if numberStream.kind != compactStreamFOR || numberStream.width != 16 ||
		numberStream.count != count {
		t.Fatalf(
			"stripe number stream kind=%d width=%d count=%d, want FOR16/%d",
			numberStream.kind, numberStream.width, numberStream.count, count,
		)
	}
	labelNeedle := compactPackedBenchmarkLabel8(17)
	labelExpected := 0
	for row := range records {
		if (row*73)&255 == (17*73)&255 {
			labelExpected++
		}
	}
	numberNeedle := compactPackedBenchmarkNumber16Needle()
	numberExpected := compactPackedBenchmarkNumber16Expected(count)
	return view, labelResolver, numberResolver,
		labelNeedle, labelExpected, numberNeedle, numberExpected
}

func TestCompactPackedBenchmarkStripeWidths(t *testing.T) {
	view, labelResolver, numberResolver, labelNeedle, labelExpected,
		numberNeedle, numberExpected := compactPackedBenchmarkStripeWide(t)
	if got, _, ok := view.CountResolvedSpellingEqual(
		&labelResolver, labelNeedle, nil,
	); !ok || got != labelExpected {
		t.Fatalf("stripe dictionary8 count=%d want=%d ok=%v", got, labelExpected, ok)
	}
	if got, ok := view.CountResolvedIntegerEqual(&numberResolver, numberNeedle); !ok || got != numberExpected {
		t.Fatalf("stripe FOR16 count=%d want=%d ok=%v", got, numberExpected, ok)
	}
}

func BenchmarkCompactPrimaryStripePackedEquality7(b *testing.B) {
	view, labelResolver, _, needle, expected := compactPackedBenchmarkStripe(b)
	scratch := make([]byte, 0, len(needle))
	matched, scratch, ok := view.CountResolvedSpellingEqual(&labelResolver, needle, scratch)
	if !ok || matched != expected {
		b.Fatalf("stripe dictionary7 warm count=%d expected=%d ok=%v", matched, expected, ok)
	}
	b.ReportAllocs()
	b.ReportMetric(float64(view.Len()), "rows")
	b.ResetTimer()
	for range b.N {
		matched, scratch, ok = view.CountResolvedSpellingEqual(&labelResolver, needle, scratch)
		if !ok || matched != expected {
			b.Fatalf("stripe dictionary7 count=%d expected=%d ok=%v", matched, expected, ok)
		}
	}
}

func BenchmarkCompactPrimaryStripePackedEquality10(b *testing.B) {
	view, _, numberResolver, _, _ := compactPackedBenchmarkStripe(b)
	const needle = int64(17)
	want := 0
	for row := 0; row < view.Len(); row++ {
		if int64(((row*73)&1023)-512) == needle {
			want++
		}
	}
	matched, ok := view.CountResolvedIntegerEqual(&numberResolver, needle)
	if !ok || matched != want {
		b.Fatalf("stripe FOR10 warm count=%d expected=%d ok=%v", matched, want, ok)
	}
	b.ReportAllocs()
	b.ReportMetric(float64(view.Len()), "rows")
	b.ResetTimer()
	for range b.N {
		matched, ok = view.CountResolvedIntegerEqual(&numberResolver, needle)
		if !ok || matched != want {
			b.Fatalf("stripe FOR10 count=%d expected=%d ok=%v", matched, want, ok)
		}
	}
}

func BenchmarkCompactPrimaryStripePackedEquality8(b *testing.B) {
	view, labelResolver, _, needle, expected, _, _ := compactPackedBenchmarkStripeWide(b)
	scratch := make([]byte, 0, len(needle))
	matched, scratch, ok := view.CountResolvedSpellingEqual(&labelResolver, needle, scratch)
	if !ok || matched != expected {
		b.Fatalf("stripe dictionary8 warm count=%d expected=%d ok=%v", matched, expected, ok)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		matched, scratch, ok = view.CountResolvedSpellingEqual(&labelResolver, needle, scratch)
	}
	b.StopTimer()
	if !ok || matched != expected {
		b.Fatalf("stripe dictionary8 count=%d expected=%d ok=%v", matched, expected, ok)
	}
	b.ReportMetric(float64(view.Len()), "rows")
}

func BenchmarkCompactPrimaryStripePackedEquality16(b *testing.B) {
	view, _, numberResolver, _, _, needle, expected := compactPackedBenchmarkStripeWide(b)
	matched, ok := view.CountResolvedIntegerEqual(&numberResolver, needle)
	if !ok || matched != expected {
		b.Fatalf("stripe FOR16 warm count=%d expected=%d ok=%v", matched, expected, ok)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		matched, ok = view.CountResolvedIntegerEqual(&numberResolver, needle)
	}
	b.StopTimer()
	if !ok || matched != expected {
		b.Fatalf("stripe FOR16 count=%d expected=%d ok=%v", matched, expected, ok)
	}
	b.ReportMetric(float64(view.Len()), "rows")
}
