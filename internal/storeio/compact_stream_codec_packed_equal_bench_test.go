package storeio

import (
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

func BenchmarkCompactStreamPackedEqualityThresholds(b *testing.B) {
	for _, width := range []int{7, 10} {
		for _, count := range []int{0, 1, 8, 16, 31, 32, 64} {
			name := fmt.Sprintf("width%d-count%d", width, count)
			b.Run(name, func(b *testing.B) {
				data := make([]byte, (count*width+7)/8)
				b.ReportAllocs()
				b.ReportMetric(float64(count), "rows")
				b.ResetTimer()
				for range b.N {
					if matched := countCompactPackedEqual(data, count, width, 0); matched != count {
						b.Fatalf("count=%d matched=%d", count, matched)
					}
				}
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
