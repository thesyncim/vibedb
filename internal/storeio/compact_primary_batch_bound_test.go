package storeio

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/thesyncim/vibedb/internal/benchcorpus"
)

func compactPrimaryBatchBoundPage(
	t testing.TB, records []CommonPrimaryLeafRecord,
) ([]byte, CompactPrimaryStripeView) {
	t.Helper()
	storeID := unifiedTestStoreID()
	builder := NewUnifiedPrimaryLeafBuilder()
	payload, err := BuildCompactPrimaryStripePayload(records, builder)
	if err != nil {
		t.Fatalf("build compact bound fixture: %v", err)
	}
	extent := int(physicalPageQuantum)
	for extent < PageHeaderSize+len(payload)+PageTrailerSize {
		extent <<= 1
	}
	page, err := EncodeCompactPrimaryStripe(
		make([]byte, extent), CommonPrimaryLeafHeader{
			StoreID: storeID, Generation: 1, Bucket: 0,
			PageSize: uint32(extent),
		}, records, builder,
	)
	if err != nil {
		t.Fatalf("encode compact bound fixture: %v", err)
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
		t.Fatalf("open compact bound fixture: %v", err)
	}
	return page, view
}

func compactPrimaryBatchBoundReplace(
	t testing.TB,
	view *CompactPrimaryStripeView,
	records []CommonPrimaryLeafRecord,
	rank int,
	marker, replacement string,
) CommonPrimaryUnifiedReplacement {
	t.Helper()
	base, ok := view.AppendValue(nil, rank)
	if !ok {
		t.Fatalf("decode compact bound row %d", rank)
	}
	markerAt := bytes.Index(base, []byte(marker))
	if markerAt < 0 {
		t.Fatalf("compact bound row %d missing %q", rank, marker)
	}
	start := markerAt + len(marker)
	end := start
	for end < len(base) && base[end] != ',' && base[end] != '}' && base[end] != ']' {
		end++
	}
	if end <= start {
		t.Fatalf("compact bound row %d has empty %q", rank, marker)
	}
	updated := make([]byte, 0, len(base)-end+start+len(replacement))
	updated = append(updated, base[:start]...)
	updated = append(updated, replacement...)
	updated = append(updated, base[end:]...)

	certificate := unifiedScalarCanonicalIndex(t, updated)
	patch, _, resolved, err := view.PatchStableCanonicalReplacementScalarPatch(
		records[rank].Key, uint8(rank), certificate, nil,
	)
	if err != nil || !resolved || !patch.valid() || patch.exact() {
		t.Fatalf(
			"compact bound scalar certificate rank=%d valid=%v exact=%v resolved=%v err=%v",
			rank, patch.valid(), patch.exact(), resolved, err,
		)
	}
	return CommonPrimaryUnifiedReplacement{
		Key: records[rank].Key, Value: updated,
		ScalarPatch: patch, Slot: uint8(rank),
	}
}

func compactPrimaryBatchBoundReencodedPayload(
	t testing.TB, records []CommonPrimaryLeafRecord, rank int,
	replacement CommonPrimaryUnifiedReplacement,
) []byte {
	t.Helper()
	full := append([]CommonPrimaryLeafRecord(nil), records...)
	full[rank].Value.Inline = replacement.Value
	payload, err := BuildCompactPrimaryStripePayload(
		full, NewUnifiedPrimaryLeafBuilder(),
	)
	if err != nil {
		t.Fatalf("recompress compact bound fixture: %v", err)
	}
	return payload
}

func compactPrimaryBatchBoundStream(
	t testing.TB, view *CompactPrimaryStripeView, rank, hole int,
) compactStreamView {
	t.Helper()
	shape := view.rowShape(rank)
	entry, ok := view.shapeEntry(shape)
	if !ok {
		t.Fatalf("compact bound row %d shape %d", rank, shape)
	}
	streamRaw := entry.streamRaw
	for at := 0; at <= hole; at++ {
		stream, admitted := admittedCompactStream(streamRaw)
		if !admitted {
			t.Fatalf("compact bound shape %d stream %d", shape, at)
		}
		if at == hole {
			return stream
		}
		streamRaw = streamRaw[stream.encoded:]
	}
	t.Fatalf("compact bound row %d hole %d not found", rank, hole)
	return compactStreamView{}
}

func TestConservativeScalarReplacementBatchPayloadCoversExtremeScoreRecompression(t *testing.T) {
	page, view, records := compactPrimaryTestPage(t, CompactPrimaryStripeMaxRows, false)
	if len(page) == 0 || view.Len() != CompactPrimaryStripeMaxRows {
		t.Fatalf("compact SQL-shaped fixture page=%d rows=%d", len(page), view.Len())
	}
	const rank = 777
	for _, test := range []struct {
		name, value string
	}{
		{name: "wide-negative", value: "-999999999999999999"},
		{name: "wide-positive", value: "999999999999999999"},
	} {
		t.Run(test.name, func(t *testing.T) {
			replacement := compactPrimaryBatchBoundReplace(
				t, &view, records, rank, `"score":`, test.value,
			)
			actual := compactPrimaryBatchBoundReencodedPayload(
				t, records, rank, replacement,
			)
			bound, admitted, err := view.ConservativeScalarReplacementBatchPayload(
				[]CommonPrimaryUnifiedReplacement{replacement},
			)
			if err != nil || !admitted {
				t.Fatalf("score update was declined: bound=%d admitted=%v err=%v", bound, admitted, err)
			}
			if bound < len(actual) {
				t.Fatalf("admitted bound=%d is below recompressed payload=%d", bound, len(actual))
			}

			fast, ok, err := view.PatchCompactPrimaryStripeReplacements(
				make([]byte, CommonPrimaryLeafMaxExtentBytes), 2,
				[]CommonPrimaryUnifiedReplacement{replacement},
				NewUnifiedPrimaryLeafBuilder(),
			)
			if err != nil || !ok {
				t.Fatalf("certified score patch: ok=%v err=%v", ok, err)
			}
			_, fastPayload, err := OpenPage(fast)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(fastPayload, actual) {
				t.Fatalf("fast score patch differs from actual recompression: fast=%d actual=%d", len(fastPayload), len(actual))
			}
		})
	}
}

func TestConservativeScalarReplacementBatchPayloadRejectsUnboundedMixedScalarStream(t *testing.T) {
	const rows = 768
	records := make([]CommonPrimaryLeafRecord, rows)
	for row := range records {
		var value string
		switch row % 3 {
		case 0:
			value = fmt.Sprintf(`{"v":%d}`, row%17)
		case 1:
			value = `{"v":true}`
		default:
			value = `{"v":null}`
		}
		records[row] = CommonPrimaryLeafRecord{
			Key:   []byte(benchcorpus.Key(row)),
			Value: CommonPrimaryLeafValue{Inline: []byte(value)},
		}
	}
	_, view := compactPrimaryBatchBoundPage(t, records)
	const rank = 1
	replacement := compactPrimaryBatchBoundReplace(
		t, &view, records, rank, `"v":`, "999999999999999999",
	)
	stream := compactPrimaryBatchBoundStream(t, &view, rank, int(replacement.ScalarPatch.bodyOffset))
	var ints, trues, nulls int
	shape := view.rowShape(rank)
	entry, ok := view.shapeEntry(shape)
	if !ok {
		t.Fatal("mixed stream shape")
	}
	for ordinal := 0; ordinal < entry.rows; ordinal++ {
		value, decoded := stream.appendValue(nil, ordinal)
		if !decoded {
			t.Fatalf("mixed stream row %d did not decode", ordinal)
		}
		switch string(value) {
		case "true":
			trues++
		case "null":
			nulls++
		default:
			if _, integer := CanonicalIntValue(value); !integer {
				t.Fatalf("mixed stream row %d has unexpected scalar %q", ordinal, value)
			}
			ints++
		}
	}
	if ints == 0 || trues == 0 || nulls == 0 {
		t.Fatalf("target stream is not mixed: ints=%d true=%d null=%d kind=%d", ints, trues, nulls, stream.kind)
	}
	bound, admitted, err := view.ConservativeScalarReplacementBatchPayload(
		[]CommonPrimaryUnifiedReplacement{replacement},
	)
	if err != nil {
		t.Fatal(err)
	}
	if admitted || bound != 0 {
		t.Fatalf("mixed stream bound=%d admitted=%v, want 0/false", bound, admitted)
	}
}

func TestCompactIntegerStreamRejectsTargetOnlyPrefixIntegerProof(t *testing.T) {
	encoded, ok := encodeCompactPrefixInt([][]byte{[]byte("-10"), []byte("-0")})
	if !ok || encoded.kind != compactStreamPrefixInt {
		t.Fatalf("prefix-integer fixture = kind %d admitted %v", encoded.kind, ok)
	}
	raw, err := encoded.appendBinary(nil)
	if err != nil {
		t.Fatal(err)
	}
	stream, admitted := admittedCompactStream(raw)
	if !admitted {
		t.Fatal("prefix-integer fixture did not reopen")
	}
	if value, decoded := stream.appendValue(nil, 0); !decoded ||
		string(value) != "-10" {
		t.Fatalf("target prefix integer = %q/%v, want -10/true", value, decoded)
	}
	if compactIntegerStream(stream) {
		t.Fatal("target-only PrefixInt proof admitted a stream containing negative zero")
	}
}

func TestConservativeScalarReplacementBatchPayloadChargesChangedIntegerColumnOnly(t *testing.T) {
	const rows = 1024
	records := make([]CommonPrimaryLeafRecord, rows)
	for row := range records {
		records[row] = CommonPrimaryLeafRecord{
			Key: []byte(benchcorpus.Key(row)),
			Value: CommonPrimaryLeafValue{Inline: []byte(fmt.Sprintf(
				`{"a":%d,"b":%d,"score":%d}`,
				row%1000, (row*3)%1000, row%1000,
			))},
		}
	}
	_, view := compactPrimaryBatchBoundPage(t, records)
	if view.shapeCount != 1 {
		t.Fatalf("integer-column fixture produced %d shapes", view.shapeCount)
	}
	entry, ok := view.shapeEntry(0)
	if !ok || entry.template.holes != 3 {
		t.Fatalf("integer-column fixture holes=%d ok=%v", entry.template.holes, ok)
	}
	streamRaw := entry.streamRaw
	for hole := 0; hole < entry.template.holes; hole++ {
		stream, admitted := admittedCompactStream(streamRaw)
		if !admitted {
			t.Fatalf("integer-column hole %d is not an admitted integer stream", hole)
		}
		for ordinal := 0; ordinal < stream.count; ordinal++ {
			value, decoded := stream.appendValue(nil, ordinal)
			if !decoded {
				t.Fatalf("integer-column hole %d row %d did not decode", hole, ordinal)
			}
			if _, integer := CanonicalIntValue(value); !integer {
				t.Fatalf("integer-column hole %d row %d decoded noninteger %q", hole, ordinal, value)
			}
		}
		streamRaw = streamRaw[stream.encoded:]
	}
	const rank = 73
	replacement := compactPrimaryBatchBoundReplace(
		t, &view, records, rank, `"score":`, "999999999999999999",
	)
	actual := compactPrimaryBatchBoundReencodedPayload(t, records, rank, replacement)
	bound, admitted, err := view.ConservativeScalarReplacementBatchPayload(
		[]CommonPrimaryUnifiedReplacement{replacement},
	)
	if err != nil || !admitted {
		t.Fatalf("score column update was declined: bound=%d admitted=%v err=%v", bound, admitted, err)
	}
	if bound < len(actual) {
		t.Fatalf("integer-column bound=%d is below recompressed payload=%d", bound, len(actual))
	}

	stream := compactPrimaryBatchBoundStream(t, &view, rank, int(replacement.ScalarPatch.bodyOffset))
	streamBound, ok := conservativeIntegerStreamBytes(stream.count)
	if !ok {
		t.Fatal("integer stream bound overflow")
	}
	if delta := bound - len(view.payload); delta > streamBound {
		t.Fatalf("bound charged unchanged integer columns: delta=%d one-stream-bound=%d", delta, streamBound)
	}
}
