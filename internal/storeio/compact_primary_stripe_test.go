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
	_, ok := view.ResolveHoles(nil, &resolver)
	if !ok {
		t.Fatal("resolve compact country holes")
	}
	matched, _, ok := view.CountResolvedSpellingEqual(&resolver, []byte(`"PT"`), nil)
	if !ok {
		t.Fatal("compact country spelling scan")
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

func TestCompactPreparedPrefixesMatchOneShot(t *testing.T) {
	for _, high := range []bool{false, true} {
		corpus := benchcorpus.Corpus(512, high)
		records := make([]CommonPrimaryLeafRecord, len(corpus))
		for row := range corpus {
			records[row] = CommonPrimaryLeafRecord{
				Key:   []byte(corpus[row].Key),
				Value: CommonPrimaryLeafValue{Inline: corpus[row].JSON},
			}
		}
		prepared := NewUnifiedPrimaryLeafBuilder()
		if err := prepareCompactPrimaryStripe(records, prepared); err != nil {
			t.Fatal(err)
		}
		for _, count := range []int{512, 1, 257, 64, 511, 65, 2, 256, 127} {
			got, err := buildPreparedCompactPrimaryStripePayload(records[:count], prepared)
			if err != nil {
				t.Fatalf("high=%t count=%d prepared: %v", high, count, err)
			}
			want, err := BuildCompactPrimaryStripePayload(
				records[:count], NewUnifiedPrimaryLeafBuilder(),
			)
			if err != nil {
				t.Fatalf("high=%t count=%d one-shot: %v", high, count, err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("high=%t count=%d prepared bytes differ", high, count)
			}
		}
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

func TestCompactPrimaryStripeScalarReplacementMatchesFullPlanner(t *testing.T) {
	page, view, records := compactPrimaryTestPage(t, 200, false)
	const rank = 73
	updated, err := vibejson.AppendCanonicalize(nil, records[rank].Value.Inline)
	if err != nil {
		t.Fatal(err)
	}
	score := bytes.Index(updated, []byte(`"score":`))
	if score < 0 {
		t.Fatal("score field is missing")
	}
	score += len(`"score":`)
	if updated[score] == '9' {
		updated[score] = '8'
	} else {
		updated[score]++
	}
	fast, ok, err := view.PatchCompactPrimaryStripeReplacements(
		make([]byte, CommonPrimaryLeafMaxExtentBytes), 2,
		[]CommonPrimaryUnifiedReplacement{{
			Key: records[rank].Key, Value: updated, Slot: rank,
		}},
		NewUnifiedPrimaryLeafBuilder(),
	)
	if err != nil || !ok {
		t.Fatalf("compact scalar patch = ok %v, err %v", ok, err)
	}
	fullRecords := append([]CommonPrimaryLeafRecord(nil), records...)
	fullRecords[rank].Value.Inline = updated
	want, err := EncodeBestCompactPrimaryStripe(
		make([]byte, CommonPrimaryLeafMaxExtentBytes),
		CommonPrimaryLeafHeader{
			StoreID: unifiedTestStoreID(), Generation: 2, Bucket: 0,
		},
		unifiedTestStoreID(), fullRecords, NewUnifiedPrimaryLeafBuilder(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(fast, want) {
		t.Fatalf(
			"compact scalar patch differs from full planner (fast=%d full=%d source=%d)",
			len(fast), len(want), len(page),
		)
	}
}

func TestCompactPrimaryStripeMultiShapeReplacementsMatchFullPlanner(t *testing.T) {
	page, view, records := compactPrimaryTestPage(t, 200, false)
	selected := make(map[int]int)
	for rank := range records {
		shape := view.rowShape(rank)
		if _, exists := selected[shape]; !exists {
			selected[shape] = rank
		}
	}
	if len(selected) < 2 {
		t.Fatalf("fixture produced only %d compact shape", len(selected))
	}
	replacements := make([]CommonPrimaryUnifiedReplacement, 0, len(selected))
	fullRecords := append([]CommonPrimaryLeafRecord(nil), records...)
	for _, rank := range selected {
		updated, err := vibejson.AppendCanonicalize(nil, records[rank].Value.Inline)
		if err != nil {
			t.Fatal(err)
		}
		score := bytes.Index(updated, []byte(`"score":`))
		if score < 0 {
			t.Fatal("score field is missing")
		}
		score += len(`"score":`)
		if updated[score] == '9' {
			updated[score] = '8'
		} else {
			updated[score]++
		}
		replacements = append(replacements, CommonPrimaryUnifiedReplacement{
			Key: records[rank].Key, Value: updated, Slot: uint8(rank),
		})
		fullRecords[rank].Value.Inline = updated
	}
	fast, ok, err := view.PatchCompactPrimaryStripeReplacements(
		make([]byte, CommonPrimaryLeafMaxExtentBytes), 2,
		replacements, NewUnifiedPrimaryLeafBuilder(),
	)
	if err != nil || !ok {
		t.Fatalf("compact multi-shape patch = ok %v, err %v", ok, err)
	}
	want, err := EncodeBestCompactPrimaryStripe(
		make([]byte, CommonPrimaryLeafMaxExtentBytes),
		CommonPrimaryLeafHeader{
			StoreID: unifiedTestStoreID(), Generation: 2, Bucket: 0,
		},
		unifiedTestStoreID(), fullRecords, NewUnifiedPrimaryLeafBuilder(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(fast, want) {
		t.Fatalf(
			"compact multi-shape patch differs from full planner (fast=%d full=%d source=%d shapes=%d)",
			len(fast), len(want), len(page), len(selected),
		)
	}
}

func TestCompactPrimaryStripeNetZeroMultiShapeDeltaMatchesFullPlanner(t *testing.T) {
	const rowsPerShape = 64
	records := make([]CommonPrimaryLeafRecord, 2*rowsPerShape)
	for rank := range records {
		value := []byte(`{"a":0}`)
		if rank >= rowsPerShape {
			value = []byte(`{"b":0}`)
		}
		records[rank] = CommonPrimaryLeafRecord{
			Key: []byte(benchcorpus.Key(rank)),
			Value: CommonPrimaryLeafValue{
				Inline: value,
			},
		}
	}
	// The second shape begins with the same outlier the first shape gains below.
	// Replanning the two independent streams therefore produces exact opposite
	// nonzero byte deltas while leaving the complete payload length unchanged.
	records[rowsPerShape].Value.Inline = []byte(`{"b":2147483647}`)
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
	_, sourcePayload, err := OpenPage(page)
	if err != nil {
		t.Fatal(err)
	}
	if view.rowShape(0) == view.rowShape(rowsPerShape) {
		t.Fatal("fixture did not produce two compact shapes")
	}
	replacements := []CommonPrimaryUnifiedReplacement{
		{Key: records[0].Key, Value: []byte(`{"a":2147483647}`), Slot: 0},
		{
			Key: records[rowsPerShape].Key, Value: []byte(`{"b":0}`),
			Slot: rowsPerShape,
		},
	}
	var deltas [2]int
	for index := range replacements {
		patched, ok, patchErr := view.PatchCompactPrimaryStripeReplacements(
			make([]byte, CommonPrimaryLeafMaxExtentBytes), 2,
			replacements[index:index+1], NewUnifiedPrimaryLeafBuilder(),
		)
		if patchErr != nil || !ok {
			t.Fatalf("individual compact patch %d = ok %v, err %v", index, ok, patchErr)
		}
		_, individualPayload, openErr := OpenPage(patched)
		if openErr != nil {
			t.Fatal(openErr)
		}
		deltas[index] = len(individualPayload) - len(sourcePayload)
	}
	if deltas[0] == 0 || deltas[0]+deltas[1] != 0 {
		t.Fatalf("fixture deltas are not nonzero opposites: %v", deltas)
	}
	fast, ok, err := view.PatchCompactPrimaryStripeReplacements(
		make([]byte, CommonPrimaryLeafMaxExtentBytes), 2, replacements,
		NewUnifiedPrimaryLeafBuilder(),
	)
	if err != nil || !ok {
		t.Fatalf("compact net-zero patch = ok %v, err %v", ok, err)
	}
	fullRecords := append([]CommonPrimaryLeafRecord(nil), records...)
	fullRecords[0].Value.Inline = replacements[0].Value
	fullRecords[rowsPerShape].Value.Inline = replacements[1].Value
	want, err := EncodeBestCompactPrimaryStripe(
		make([]byte, CommonPrimaryLeafMaxExtentBytes),
		CommonPrimaryLeafHeader{
			StoreID: unifiedTestStoreID(), Generation: 2, Bucket: 0,
		},
		unifiedTestStoreID(), fullRecords, NewUnifiedPrimaryLeafBuilder(),
	)
	if err != nil {
		t.Fatal(err)
	}
	_, finalPayload, err := OpenPage(fast)
	if err != nil {
		t.Fatal(err)
	}
	if len(finalPayload) != len(sourcePayload) {
		t.Fatalf(
			"net-zero payload length changed: source=%d final=%d deltas=%d,%d",
			len(sourcePayload), len(finalPayload), deltas[0], deltas[1],
		)
	}
	if !bytes.Equal(fast, want) {
		t.Fatalf(
			"net-zero compact patch differs from full planner (source=%d final=%d deltas=%d,%d shapes=%d,%d)",
			len(sourcePayload), len(finalPayload), deltas[0], deltas[1],
			view.rowShape(0), view.rowShape(rowsPerShape),
		)
	}
}

func TestCompactPrimaryStripeMultiColumnReplacementsMatchFullPlanner(t *testing.T) {
	page, view, records := compactPrimaryTestPage(t, 200, false)
	ranks := [...]int{11, 73, 129}
	replacements := make([]CommonPrimaryUnifiedReplacement, 0, len(ranks))
	fullRecords := append([]CommonPrimaryLeafRecord(nil), records...)
	for index, rank := range ranks {
		updated, err := vibejson.AppendCanonicalize(nil, records[rank].Value.Inline)
		if err != nil {
			t.Fatal(err)
		}
		switch index {
		case 0:
			at := bytes.Index(updated, []byte(`"score":`)) + len(`"score":`)
			if at < len(`"score":`) || at >= len(updated) {
				t.Fatal("score field is missing")
			}
			if updated[at] == '9' {
				updated[at] = '8'
			} else {
				updated[at]++
			}
		case 1:
			if at := bytes.Index(updated, []byte(`"active":true`)); at >= 0 {
				updated = append(updated[:at], append([]byte(`"active":false`), updated[at+len(`"active":true`):]...)...)
			} else if at := bytes.Index(updated, []byte(`"active":false`)); at >= 0 {
				updated = append(updated[:at], append([]byte(`"active":true`), updated[at+len(`"active":false`):]...)...)
			} else {
				t.Fatal("active field is missing")
			}
		case 2:
			at := bytes.Index(updated, []byte(`"country":"`)) + len(`"country":"`)
			if at < len(`"country":"`) || at+2 > len(updated) {
				t.Fatal("country field is missing")
			}
			copy(updated[at:at+2], "ZZ")
		}
		replacements = append(replacements, CommonPrimaryUnifiedReplacement{
			Key: records[rank].Key, Value: updated, Slot: uint8(rank),
		})
		fullRecords[rank].Value.Inline = updated
	}
	fast, ok, err := view.PatchCompactPrimaryStripeReplacements(
		make([]byte, CommonPrimaryLeafMaxExtentBytes), 2,
		replacements, NewUnifiedPrimaryLeafBuilder(),
	)
	if err != nil || !ok {
		t.Fatalf("compact multi-column patch = ok %v, err %v", ok, err)
	}
	want, err := EncodeBestCompactPrimaryStripe(
		make([]byte, CommonPrimaryLeafMaxExtentBytes),
		CommonPrimaryLeafHeader{
			StoreID: unifiedTestStoreID(), Generation: 2, Bucket: 0,
		},
		unifiedTestStoreID(), fullRecords, NewUnifiedPrimaryLeafBuilder(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(fast, want) {
		t.Fatalf(
			"compact multi-column patch differs from full planner (fast=%d full=%d source=%d)",
			len(fast), len(want), len(page),
		)
	}
}

func TestCompactPrimaryStripeDenseReplacementBatchMatchesFullPlanner(t *testing.T) {
	for _, high := range []bool{false, true} {
		page, view, records := compactPrimaryTestPage(t, 256, high)
		replacements := make([]CommonPrimaryUnifiedReplacement, 0, 52)
		fullRecords := append([]CommonPrimaryLeafRecord(nil), records...)
		for rank := 0; rank < len(records); rank += 5 {
			updated, err := vibejson.AppendCanonicalize(nil, records[rank].Value.Inline)
			if err != nil {
				t.Fatal(err)
			}
			score := bytes.Index(updated, []byte(`"score":`)) + len(`"score":`)
			if score < len(`"score":`) || score >= len(updated) {
				t.Fatal("score field is missing")
			}
			if updated[score] == '9' {
				updated[score] = '8'
			} else {
				updated[score]++
			}
			replacements = append(replacements, CommonPrimaryUnifiedReplacement{
				Key: records[rank].Key, Value: updated, Slot: uint8(rank),
			})
			fullRecords[rank].Value.Inline = updated
		}
		fast, ok, err := view.PatchCompactPrimaryStripeReplacements(
			make([]byte, CommonPrimaryLeafMaxExtentBytes), 2,
			replacements, NewUnifiedPrimaryLeafBuilder(),
		)
		if err != nil || !ok {
			t.Fatalf("high=%t dense compact patch = ok %v, err %v", high, ok, err)
		}
		want, err := EncodeBestCompactPrimaryStripe(
			make([]byte, CommonPrimaryLeafMaxExtentBytes),
			CommonPrimaryLeafHeader{
				StoreID: unifiedTestStoreID(), Generation: 2, Bucket: 0,
			},
			unifiedTestStoreID(), fullRecords, NewUnifiedPrimaryLeafBuilder(),
		)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(fast, want) {
			t.Fatalf(
				"high=%t dense compact patch differs from full planner (fast=%d full=%d source=%d)",
				high, len(fast), len(want), len(page),
			)
		}
	}
}

func TestCompactPrimaryStripePatchRejectsDuplicateKey(t *testing.T) {
	_, view, records := compactPrimaryTestPage(t, 200, false)
	const rank = 73
	first, err := vibejson.AppendCanonicalize(nil, records[rank].Value.Inline)
	if err != nil {
		t.Fatal(err)
	}
	second := append([]byte(nil), first...)
	score := bytes.Index(first, []byte(`"score":`)) + len(`"score":`)
	country := bytes.Index(second, []byte(`"country":"`)) + len(`"country":"`)
	if score < len(`"score":`) || country < len(`"country":"`) {
		t.Fatal("fixture fields are missing")
	}
	first[score] = '7'
	copy(second[country:country+2], "ZZ")
	_, ok, err := view.PatchCompactPrimaryStripeReplacements(
		make([]byte, CommonPrimaryLeafMaxExtentBytes), 2,
		[]CommonPrimaryUnifiedReplacement{
			{Key: records[rank].Key, Value: first, Slot: rank},
			{Key: records[rank].Key, Value: second, Slot: rank},
		},
		NewUnifiedPrimaryLeafBuilder(),
	)
	if err != nil || ok {
		t.Fatalf("duplicate-key patch = ok %v, err %v; want safe decline", ok, err)
	}
}

func TestCompactPrimaryStripePatchClipsLargerBackingBuffer(t *testing.T) {
	page, _, records := compactPrimaryTestPage(t, 200, false)
	larger := append(append([]byte(nil), page...), make([]byte, 4096)...)
	view, ok := AdmittedCompactPrimaryStripe(larger, unifiedTestStoreID(), 0)
	if !ok {
		t.Fatal("larger backing buffer was not admitted")
	}
	const rank = 73
	updated, err := vibejson.AppendCanonicalize(nil, records[rank].Value.Inline)
	if err != nil {
		t.Fatal(err)
	}
	score := bytes.Index(updated, []byte(`"score":`)) + len(`"score":`)
	updated[score]++
	patched, accepted, err := view.PatchCompactPrimaryStripeReplacements(
		make([]byte, CommonPrimaryLeafMaxExtentBytes), 2,
		[]CommonPrimaryUnifiedReplacement{{
			Key: records[rank].Key, Value: updated, Slot: rank,
		}},
		NewUnifiedPrimaryLeafBuilder(),
	)
	if err != nil || !accepted || len(patched) > len(page) {
		t.Fatalf(
			"larger-buffer patch = bytes %d, accepted %v, err %v; source page %d",
			len(patched), accepted, err, len(page),
		)
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

func TestCompactPrimaryScanDecoderMatchesRandomAccess(t *testing.T) {
	_, view, records := compactPrimaryTestPage(t, 1000, false)
	var decoder CompactPrimaryScanDecoder
	ordinals := make([]int, view.ShapeCount())
	want := make([]byte, 0, 512)
	got := make([]byte, 0, 512)
	for row := range records {
		shape := view.rowShape(row)
		ordinal := ordinals[shape]
		ordinals[shape]++
		var wantOK, gotOK bool
		want, wantOK = view.AppendValue(want[:0], row)
		got, gotOK = decoder.appendValue(
			got[:0], &view, view.header.Bucket, row, shape, ordinal,
		)
		if !wantOK || !gotOK || !bytes.Equal(got, want) {
			t.Fatalf("row %d sequential mismatch wantOK=%v gotOK=%v", row, wantOK, gotOK)
		}
	}
	// Reusing the caller-owned decoder for the same immutable leaf must reset
	// its scalar cursors when lexical iteration starts over.
	clear(ordinals)
	for row := range records {
		shape := view.rowShape(row)
		ordinal := ordinals[shape]
		ordinals[shape]++
		want, _ = view.AppendValue(want[:0], row)
		got, gotOK := decoder.appendValue(
			got[:0], &view, view.header.Bucket, row, shape, ordinal,
		)
		if !gotOK || !bytes.Equal(got, want) {
			t.Fatalf("reused decoder row %d mismatch", row)
		}
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
