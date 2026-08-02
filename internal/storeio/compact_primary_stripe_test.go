package storeio

import (
	"bytes"
	"testing"

	"github.com/thesyncim/vibedb/internal/benchcorpus"
	"github.com/thesyncim/vibejson"
)

func compactPrimaryTestPage(t testing.TB, count int, high bool) ([]byte, CompactPrimaryStripeView, []CommonPrimaryLeafRecord) {
	t.Helper()
	corpus := benchcorpus.Corpus(count, high)
	records := make([]CommonPrimaryLeafRecord, count)
	for i := range corpus {
		records[i] = CommonPrimaryLeafRecord{
			Key: []byte(corpus[i].Key), Value: CommonPrimaryLeafValue{Inline: corpus[i].JSON},
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
			StoreID: storeID, Generation: 1, Bucket: 0, PageSize: uint32(extent),
		},
		records, builder,
	)
	if err != nil {
		t.Fatal(err)
	}
	logicalID, _ := CommonPrimaryLeafLogicalID(0)
	view, err := OpenCompactPrimaryStripe(
		page, storeID, 0,
		PageRef{
			Offset: 4096, Length: uint32(extent), LogicalID: logicalID,
			Generation: 1, Kind: PagePrimaryLeaf,
		},
		1, unifiedTestBounds(),
	)
	if err != nil {
		t.Fatal(err)
	}
	return page, view, records
}

func TestCompactPrimaryStripeRoundTrip(t *testing.T) {
	_, view, records := compactPrimaryTestPage(t, 1000, false)
	if view.Len() != len(records) {
		t.Fatalf("rows=%d want=%d", view.Len(), len(records))
	}
	keyBuf := make([]byte, 0, 32)
	valueBuf := make([]byte, 0, 512)
	canonical := make([]byte, 0, 512)
	for row := range records {
		key, ok := view.AppendKey(keyBuf[:0], row)
		if !ok || !bytes.Equal(key, records[row].Key) {
			t.Fatalf("row %d key=%q ok=%v want=%q", row, key, ok, records[row].Key)
		}
		value, ok := view.AppendValue(valueBuf[:0], row)
		if !ok {
			t.Fatalf("row %d value decode", row)
		}
		var err error
		canonical, err = vibejson.AppendCanonicalize(canonical[:0], records[row].Value.Inline)
		if err != nil || !bytes.Equal(value, canonical) {
			t.Fatalf("row %d value mismatch err=%v\ngot  %s\nwant %s", row, err, value, canonical)
		}
		found, ok := view.FindKey(records[row].Key)
		if !ok || found != row {
			t.Fatalf("key row %d found=%d ok=%v", row, found, ok)
		}
		keyBuf, valueBuf = key, value
	}
	if _, ok := view.FindKey([]byte("doc:missing")); ok {
		t.Fatal("missing key found")
	}
	var resolver UnifiedHoleResolver
	if err := resolver.SetPath([]byte("/country")); err != nil {
		t.Fatal(err)
	}
	holes, ok := view.ResolveHoles(nil, &resolver)
	if !ok {
		t.Fatal("resolve compact country holes")
	}
	matched, ok := view.CountDictionaryHoleEqual(holes, []byte(`"PT"`))
	if !ok {
		t.Fatal("compact country stream is not dictionary encoded")
	}
	want := 0
	filter, err := NewUnifiedEqFilter([]byte("/country"), []byte(`"PT"`))
	if err != nil {
		t.Fatal(err)
	}
	for row := range records {
		value, _ := view.AppendValue(valueBuf[:0], row)
		equal, err := filter.EvalRendered(value)
		if err != nil {
			t.Fatal(err)
		}
		if equal {
			want++
		}
	}
	if matched != want {
		t.Fatalf("compact country count=%d want=%d", matched, want)
	}
}

func TestCompactPrimaryStripeOverflowRoundTrip(t *testing.T) {
	storeID := unifiedTestStoreID()
	overflow := PageRef{
		Offset: 128 << 10, LogicalID: PrimaryFirstDynamicLogicalID + 7,
		Generation: 3, Length: 4096, Kind: PageOverflow,
	}
	records := []CommonPrimaryLeafRecord{
		{Key: []byte("a"), Value: CommonPrimaryLeafValue{Inline: []byte(`{"v":1}`)}},
		{Key: []byte("b"), Value: CommonPrimaryLeafValue{Overflow: overflow}},
		{Key: []byte("c"), Value: CommonPrimaryLeafValue{Inline: []byte(`{"v":3}`)}},
	}
	builder := NewUnifiedPrimaryLeafBuilder()
	payload, err := BuildCompactPrimaryStripePayload(records, builder)
	if err != nil {
		t.Fatal(err)
	}
	extent := (PageHeaderSize + len(payload) + PageTrailerSize + 4095) &^ 4095
	page, err := EncodeCompactPrimaryStripe(
		make([]byte, extent), CommonPrimaryLeafHeader{
			StoreID: storeID, Generation: 3, Bucket: 0, PageSize: uint32(extent),
		}, records, builder,
	)
	if err != nil {
		t.Fatal(err)
	}
	view, err := OpenCompactPrimaryStripe(
		page, storeID, 0, PageRef{
			Offset: 256 << 10, LogicalID: PrimaryLeafLogicalIDBase,
			Generation: 3, Length: uint32(extent), Kind: PagePrimaryLeaf,
		}, 3, unifiedTestBounds(),
	)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := view.OverflowRef(1)
	if !ok || got != overflow {
		t.Fatalf("overflow ref = %+v,%v, want %+v,true", got, ok, overflow)
	}
	rows, err := view.RenderRecordsWithScratch(NewPrimaryLeafMutationScratch(4096))
	if err != nil || rows[1].Value.Overflow != overflow {
		t.Fatalf("rendered overflow = %+v, %v", rows[1].Value.Overflow, err)
	}
}

func TestCompactPrimaryStripeHighCardinalityRoundTrip(t *testing.T) {
	_, view, records := compactPrimaryTestPage(t, 256, true)
	for _, row := range []int{0, 63, 64, 127, 128, 191, 192, 255} {
		value, ok := view.AppendValue(make([]byte, 0, 512), row)
		canonical, err := vibejson.AppendCanonicalize(nil, records[row].Value.Inline)
		if err != nil || !ok || !bytes.Equal(value, canonical) {
			t.Fatalf("row %d mismatch ok=%v err=%v", row, ok, err)
		}
	}
}

func TestCompactPrimaryStripeDeterministic(t *testing.T) {
	first, _, _ := compactPrimaryTestPage(t, 1000, false)
	second, _, _ := compactPrimaryTestPage(t, 1000, false)
	if !bytes.Equal(first, second) {
		t.Fatal("compact primary stripe is not deterministic")
	}
}

func TestCompactPrimaryStripeCorruptionRejected(t *testing.T) {
	page, _, _ := compactPrimaryTestPage(t, 1000, false)
	corrupt := append([]byte(nil), page...)
	corrupt[PageHeaderSize+20] ^= 1
	storeID := unifiedTestStoreID()
	logicalID, _ := CommonPrimaryLeafLogicalID(0)
	if _, err := OpenCompactPrimaryStripe(
		corrupt, storeID, 0,
		PageRef{Offset: 4096, Length: uint32(len(page)), LogicalID: logicalID, Generation: 1, Kind: PagePrimaryLeaf},
		1, unifiedTestBounds(),
	); err == nil {
		t.Fatal("corrupt compact stripe admitted")
	}
}

func TestAdmittedCachedCompactPrimaryStripe(t *testing.T) {
	page, want, records := compactPrimaryTestPage(t, 1000, false)
	header, payload, err := OpenPage(page)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := AdmittedCachedCompactPrimaryStripe(
		header, payload, unifiedTestStoreID(), 0,
	)
	if !ok || got.Len() != want.Len() {
		t.Fatalf("cached compact admission = rows %d, %v; want rows %d, true", got.Len(), ok, want.Len())
	}
	row, ok := got.FindKey(records[777].Key)
	if !ok || row != 777 {
		t.Fatalf("cached compact point lookup = %d, %v; want 777, true", row, ok)
	}

	badIdentity := header
	badIdentity.StoreID[0] ^= 1
	if _, ok := AdmittedCachedCompactPrimaryStripe(
		badIdentity, payload, unifiedTestStoreID(), 0,
	); ok {
		t.Fatal("cached compact admission accepted a mismatched store identity")
	}
	badLength := header
	badLength.PayloadLength--
	if _, ok := AdmittedCachedCompactPrimaryStripe(
		badLength, payload, unifiedTestStoreID(), 0,
	); ok {
		t.Fatal("cached compact admission accepted a mismatched payload length")
	}
	badPayload := bytes.Clone(payload)
	badPayload[0] ^= 1
	if _, ok := AdmittedCachedCompactPrimaryStripe(
		header, badPayload, unifiedTestStoreID(), 0,
	); ok {
		t.Fatal("cached compact admission accepted a mismatched payload grammar")
	}
}

func TestCompactPrimaryStripeWarmPointAllocations(t *testing.T) {
	_, view, records := compactPrimaryTestPage(t, 1000, false)
	key := records[777].Key
	buf := make([]byte, 0, 512)
	allocs := testing.AllocsPerRun(1000, func() {
		row, ok := view.FindKey(key)
		if !ok {
			panic("compact key")
		}
		out, ok := view.AppendValue(buf[:0], row)
		if !ok {
			panic("compact value")
		}
		buf = out
	})
	if allocs != 0 {
		t.Fatalf("point allocations=%v want 0", allocs)
	}
}

type compactPrimaryScanFixture struct {
	view  CompactPrimaryStripeView
	holes []int
}

func compactPrimaryCompetitiveScanFixtures(t testing.TB) []compactPrimaryScanFixture {
	t.Helper()
	corpus := benchcorpus.Corpus(100_000, false)
	storeID := unifiedTestStoreID()
	bounds := CommonPrimaryLeafBounds{
		FileEnd: 1 << 30, NextLogicalID: PrimaryFirstDynamicLogicalID + 100_000,
		AllocationQuantum: physicalPageQuantum,
	}
	var resolver UnifiedHoleResolver
	if err := resolver.SetPath([]byte("/country")); err != nil {
		t.Fatal(err)
	}
	fixtures := make([]compactPrimaryScanFixture, 0, 25)
	builder := NewUnifiedPrimaryLeafBuilder()
	for first := 0; first < len(corpus); first += CompactPrimaryStripeMaxRows {
		last := min(first+CompactPrimaryStripeMaxRows, len(corpus))
		records := make([]CommonPrimaryLeafRecord, last-first)
		for i := range records {
			records[i] = CommonPrimaryLeafRecord{
				Key:   []byte(corpus[first+i].Key),
				Value: CommonPrimaryLeafValue{Inline: corpus[first+i].JSON},
			}
		}
		payload, err := BuildCompactPrimaryStripePayload(records, builder)
		if err != nil {
			t.Fatal(err)
		}
		extent := int(physicalPageQuantum)
		for extent < PageHeaderSize+len(payload)+PageTrailerSize {
			extent <<= 1
		}
		tablet := uint32(len(fixtures) / TabletLocalIdentityLocalCount)
		local := uint32(len(fixtures) % TabletLocalIdentityLocalCount)
		bucket, ok := MakeTabletLocalIdentityBucket(tablet, local)
		if !ok {
			t.Fatal("compact fixture bucket")
		}
		page, err := EncodeCompactPrimaryStripe(
			make([]byte, extent),
			CommonPrimaryLeafHeader{
				StoreID: storeID, Generation: 1, Bucket: BucketID(bucket),
				PageSize: uint32(extent),
			},
			records, builder,
		)
		if err != nil {
			t.Fatal(err)
		}
		logicalID, _ := CommonPrimaryLeafLogicalID(BucketID(bucket))
		view, err := OpenCompactPrimaryStripe(
			page, storeID, BucketID(bucket),
			PageRef{
				Offset: uint64(len(fixtures)+1) * uint64(extent), Length: uint32(extent),
				LogicalID: logicalID, Generation: 1, Kind: PagePrimaryLeaf,
			},
			1, bounds,
		)
		if err != nil {
			t.Fatal(err)
		}
		holes, ok := view.ResolveHoles(nil, &resolver)
		if !ok {
			t.Fatal("compact fixture holes")
		}
		fixtures = append(fixtures, compactPrimaryScanFixture{view: view, holes: holes})
	}
	return fixtures
}

func BenchmarkCompactPrimaryStripeCountryScan(b *testing.B) {
	fixtures := compactPrimaryCompetitiveScanFixtures(b)
	needle := []byte(`"PT"`)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		matched := 0
		for i := range fixtures {
			count, ok := fixtures[i].view.CountDictionaryHoleEqual(fixtures[i].holes, needle)
			if !ok {
				b.Fatal("compact dictionary scan")
			}
			matched += count
		}
		if matched != 945 {
			b.Fatalf("matched=%d want=945", matched)
		}
	}
}

func BenchmarkCompactPrimaryStripePointRead(b *testing.B) {
	_, view, records := compactPrimaryTestPage(b, 4096, false)
	key := records[3077].Key
	buf := make([]byte, 0, 512)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		row, ok := view.FindKey(key)
		if !ok {
			b.Fatal("compact point key")
		}
		buf, ok = view.AppendValue(buf[:0], row)
		if !ok {
			b.Fatal("compact point value")
		}
	}
}
