package storeio

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/bits"
	"strings"
	"testing"
	"unsafe"

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

func TestCompactPrimaryStripeScalarSummary(t *testing.T) {
	pointer, err := vibejson.CompilePointer("/score")
	if err != nil {
		t.Fatal(err)
	}
	records := []CommonPrimaryLeafRecord{
		{Key: []byte("a"), Value: CommonPrimaryLeafValue{Inline: []byte(`{"score":10}`)}},
		{Key: []byte("b"), Value: CommonPrimaryLeafValue{Inline: []byte(`{"score":20}`)}},
	}
	builder := NewUnifiedPrimaryLeafBuilder()
	if err := builder.SetCompactPrimarySummaries([]vibejson.CompiledPointer{pointer}); err != nil {
		t.Fatal(err)
	}
	payload, err := BuildCompactPrimaryStripePayload(records, builder)
	if err != nil {
		t.Fatal(err)
	}
	if allocs := testing.AllocsPerRun(100, func() {
		if _, buildErr := BuildCompactPrimaryStripePayload(records, builder); buildErr != nil {
			panic(buildErr)
		}
	}); allocs != 0 {
		t.Fatalf("warm summarized compact build allocations = %v, want 0", allocs)
	}
	page, err := EncodeCompactPrimaryStripe(
		make([]byte, 4096),
		CommonPrimaryLeafHeader{
			StoreID: unifiedTestStoreID(), Generation: 1,
			Bucket: 0, PageSize: 4096,
		},
		records, builder,
	)
	if err != nil {
		t.Fatal(err)
	}
	_ = payload
	view, ok := AdmittedCompactPrimaryStripe(page, unifiedTestStoreID(), 0)
	if !ok || view.SummaryCount() != 1 {
		t.Fatalf("summary view = (%v, %d)", ok, view.SummaryCount())
	}
	minimum, maximum, valid := view.Summary(0)
	if !valid || bytes.Compare(minimum, maximum) >= 0 {
		t.Fatalf("summary = (%x, %x, %v)", minimum, maximum, valid)
	}
	header, encoded, err := OpenPage(page)
	if err != nil {
		t.Fatal(err)
	}
	summaryBytes := int(binary.LittleEndian.Uint32(encoded[36:40]))
	corrupt := bytes.Clone(encoded)
	corrupt[len(corrupt)-summaryBytes] |= 0x80
	if _, admitted := AdmittedCachedCompactPrimaryStripe(
		header, corrupt, unifiedTestStoreID(), 0,
	); admitted {
		t.Fatal("cached compact admission accepted unknown summary flags")
	}
}

func TestCompactPrimaryStripeSummaryDeclinesUnboundedValues(t *testing.T) {
	pointer, err := vibejson.CompilePointer("/score")
	if err != nil {
		t.Fatal(err)
	}
	overflow := PageRef{
		Offset: 128 << 10, LogicalID: PrimaryFirstDynamicLogicalID + 7,
		Generation: 1, Length: 4096, Kind: PageOverflow,
	}
	tests := []struct {
		name  string
		value CommonPrimaryLeafValue
		valid bool
	}{
		{name: "missing-is-null", value: CommonPrimaryLeafValue{Inline: []byte(`{"other":1}`)}, valid: true},
		{name: "container", value: CommonPrimaryLeafValue{Inline: []byte(`{"score":{"nested":1}}`)}},
		{name: "oversized", value: CommonPrimaryLeafValue{Inline: fmt.Appendf(nil, `{"score":%q}`, strings.Repeat("x", CompactPrimarySummaryMaxKeyBytes+1))}},
		{name: "overflow", value: CommonPrimaryLeafValue{Overflow: overflow}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			builder := NewUnifiedPrimaryLeafBuilder()
			if err := builder.SetCompactPrimarySummaries(
				[]vibejson.CompiledPointer{pointer},
			); err != nil {
				t.Fatal(err)
			}
			records := []CommonPrimaryLeafRecord{{
				Key: []byte("a"), Value: test.value,
			}}
			payload, err := BuildCompactPrimaryStripePayload(records, builder)
			if err != nil {
				t.Fatal(err)
			}
			view, err := openCompactPrimaryStripePayload(
				payload,
				PageHeader{
					StoreID: unifiedTestStoreID(), Generation: 1,
					Kind: PagePrimaryLeaf, PayloadLength: uint32(len(payload)),
				},
				unifiedTestStoreID(), 0, CommonPrimaryLeafBounds{}, false, false,
			)
			if err != nil || view.SummaryCount() != 1 {
				t.Fatalf("open summary = count %d, err %v", view.SummaryCount(), err)
			}
			minimum, maximum, valid := view.Summary(0)
			if valid != test.valid {
				t.Fatalf("summary valid = %v, want %v", valid, test.valid)
			}
			if valid && !bytes.Equal(minimum, maximum) {
				t.Fatalf("single-row extrema differ: %x / %x", minimum, maximum)
			}
		})
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
	certificate := unifiedScalarCanonicalIndex(t, updated)
	scalarPatch, _, resolved, err :=
		view.PatchStableCanonicalReplacementScalarPatch(
			records[rank].Key, rank, certificate,
			make([]byte, 0, len(updated)),
		)
	if err != nil || !resolved || !scalarPatch.valid() || scalarPatch.exact() {
		t.Fatalf(
			"compact admission patch = valid %v exact %v resolved %v err %v",
			scalarPatch.valid(), scalarPatch.exact(), resolved, err,
		)
	}
	builder := NewUnifiedPrimaryLeafBuilder()
	fast, ok, err := view.PatchCompactPrimaryStripeReplacements(
		make([]byte, CommonPrimaryLeafMaxExtentBytes), 2,
		[]CommonPrimaryUnifiedReplacement{{
			Key: records[rank].Key, Value: updated,
			ScalarPatch: scalarPatch, Slot: rank,
		}},
		builder,
	)
	if err != nil || !ok {
		t.Fatalf("compact scalar patch = ok %v, err %v", ok, err)
	}
	if len(builder.rows) != 0 {
		t.Fatalf("certified compact patch rebuilt %d replacement rows", len(builder.rows))
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

func TestCompactPrimaryStripeScalarCertificateDamageDeclines(t *testing.T) {
	_, view, records := compactPrimaryTestPage(t, 200, false)
	const rank = 73
	base, err := vibejson.AppendCanonicalize(nil, records[rank].Value.Inline)
	if err != nil {
		t.Fatal(err)
	}
	updated := append([]byte(nil), base...)
	score := bytes.Index(updated, []byte(`"score":`)) + len(`"score":`)
	if score < len(`"score":`) || score >= len(updated) {
		t.Fatal("score field is missing")
	}
	if updated[score] == '9' {
		updated[score] = '8'
	} else {
		updated[score]++
	}
	certificate := unifiedScalarCanonicalIndex(t, updated)
	patch, _, resolved, err := view.PatchStableCanonicalReplacementScalarPatch(
		records[rank].Key, rank, certificate, make([]byte, 0, len(updated)),
	)
	if err != nil || !resolved || !patch.valid() || patch.exact() {
		t.Fatalf("scalar certificate = %#v resolved=%v err=%v", patch, resolved, err)
	}

	tests := []struct {
		name string
		edit func(*CommonPrimaryUnifiedScalarPatch)
	}{
		{name: "hole", edit: func(p *CommonPrimaryUnifiedScalarPatch) { p.bodyOffset++ }},
		{name: "canonical-offset", edit: func(p *CommonPrimaryUnifiedScalarPatch) { p.canonicalOffset++ }},
		{name: "old-length", edit: func(p *CommonPrimaryUnifiedScalarPatch) { p.bodyLength++ }},
		{name: "new-length", edit: func(p *CommonPrimaryUnifiedScalarPatch) { p.canonicalLength++ }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			damaged := patch
			test.edit(&damaged)
			_, ok, patchErr := view.PatchCompactPrimaryStripeReplacements(
				make([]byte, CommonPrimaryLeafMaxExtentBytes), 2,
				[]CommonPrimaryUnifiedReplacement{{
					Key: records[rank].Key, Value: updated,
					ScalarPatch: damaged, Slot: rank,
				}},
				NewUnifiedPrimaryLeafBuilder(),
			)
			if patchErr != nil || ok {
				t.Fatalf("damaged patch = ok %v err %v", ok, patchErr)
			}
		})
	}

	exact, _, resolved, err := view.PatchStableCanonicalReplacementScalarPatch(
		records[rank].Key, rank, unifiedScalarCanonicalIndex(t, base), nil,
	)
	if err != nil || !resolved || !exact.exact() {
		t.Fatalf("exact certificate = %#v resolved=%v err=%v", exact, resolved, err)
	}
	_, ok, err := view.PatchCompactPrimaryStripeReplacements(
		make([]byte, CommonPrimaryLeafMaxExtentBytes), 2,
		[]CommonPrimaryUnifiedReplacement{{
			Key: records[rank].Key, Value: updated,
			ScalarPatch: exact, Slot: rank,
		}},
		NewUnifiedPrimaryLeafBuilder(),
	)
	if err != nil || ok {
		t.Fatalf("misbound exact patch = ok %v err %v", ok, err)
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

func TestCompactPrimaryStripeReplacementPatchAllocatesZeroWhenWarm(t *testing.T) {
	_, view, records := compactPrimaryTestPage(t, 200, false)
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
	replacements := []CommonPrimaryUnifiedReplacement{{
		Key: records[rank].Key, Value: updated, Slot: rank,
	}}
	dst := make([]byte, CommonPrimaryLeafMaxExtentBytes)
	builder := NewUnifiedPrimaryLeafBuilder()
	patch := func() {
		if _, ok, patchErr := view.PatchCompactPrimaryStripeReplacements(
			dst, 2, replacements, builder,
		); patchErr != nil || !ok {
			panic("compact replacement patch")
		}
	}
	patch()
	if allocs := testing.AllocsPerRun(1_000, patch); allocs != 0 {
		t.Fatalf("warm compact replacement allocations=%v want 0", allocs)
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

func TestCompactPrimaryShapeOrdinalPackedTwoBit(t *testing.T) {
	const rows = 257
	for _, shapes := range []int{3, 4} {
		t.Run(fmt.Sprintf("shapes-%d", shapes), func(t *testing.T) {
			view := CompactPrimaryStripeView{
				rows: rows, shapeCount: shapes, shapeWidth: 2,
				shapeCodes: make([]byte, (rows*2+7)/8),
				rankTable: make([]byte,
					((rows+compactStreamRestart-1)/compactStreamRestart)*shapes*2),
			}
			ranks := make([]uint16, shapes)
			for row := range rows {
				if row%compactStreamRestart == 0 {
					checkpoint := row / compactStreamRestart
					for shape := range ranks {
						binary.LittleEndian.PutUint16(
							view.rankTable[(checkpoint*shapes+shape)*2:], ranks[shape],
						)
					}
				}
				shape := (row*7 + row/5) % shapes
				compactPutBits(view.shapeCodes, row*2, 2, uint64(shape))
				ranks[shape]++
			}
			clear(ranks)
			for row := range rows {
				shape := view.rowShape(row)
				if got, want := view.shapeOrdinal(row, shape), int(ranks[shape]); got != want {
					t.Fatalf("row %d shape %d ordinal = %d, want %d", row, shape, got, want)
				}
				ranks[shape]++
			}
		})
	}
}

func TestCompactPrimaryScanDecoderMatchesRandomAccess(t *testing.T) {
	_, view, records := compactPrimaryTestPage(t, 1000, false)
	var decoder CompactPrimaryScanDecoder
	ordinals := make([]int, view.ShapeCount())
	keyWant := make([]byte, 0, 64)
	keyGot := make([]byte, 0, 64)
	want := make([]byte, 0, 512)
	got := make([]byte, 0, 512)
	for row := range records {
		var keyWantOK, keyGotOK bool
		keyWant, keyWantOK = view.AppendKey(keyWant[:0], row)
		keyGot, keyGotOK = decoder.appendKey(
			keyGot[:0], &view, view.header.Bucket, row,
		)
		if !keyWantOK || !keyGotOK || !bytes.Equal(keyGot, keyWant) {
			t.Fatalf("row %d sequential key mismatch wantOK=%v gotOK=%v", row, keyWantOK, keyGotOK)
		}
		// Borrowed callback data is not retained by the cursor. Mutating it must
		// not poison the decoder's private prefix for the next front-coded key.
		for at := range keyGot {
			keyGot[at] = 0xff
		}
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
		keyWant, _ = view.AppendKey(keyWant[:0], row)
		keyGot, keyGotOK := decoder.appendKey(
			keyGot[:0], &view, view.header.Bucket, row,
		)
		if !keyGotOK || !bytes.Equal(keyGot, keyWant) {
			t.Fatalf("reused decoder key row %d mismatch", row)
		}
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

func compactPrimaryFixedPackedScanView(
	t testing.TB,
	kind uint8,
	width, count int,
) compactStreamView {
	t.Helper()
	prefix := 8
	if kind == compactStreamDate {
		prefix = 4
	}
	packedBytes := (count*width + 7) / 8
	backing := make([]byte, prefix+packedBytes+17)
	for at := range backing {
		backing[at] = 0xff
	}
	data := backing[:prefix+packedBytes]
	clear(data)
	switch kind {
	case compactStreamFOR:
		// Keep signed wraparound in the exercised grammar. The sequential path
		// must perform the same uint64 -> int64 addition as random-rank decode.
		base := int64(^uint64(0)>>1) - 7
		binary.LittleEndian.PutUint64(data, uint64(base))
	case compactStreamDate:
		base, ok := compactDateOrdinal([]byte(`"2018-01-01"`))
		if !ok {
			t.Fatal("date base")
		}
		binary.LittleEndian.PutUint32(data, uint32(base))
	default:
		t.Fatalf("fixed packed kind=%d", kind)
	}
	mask := ^uint64(0)
	if width < 64 {
		mask = uint64(1)<<uint(width) - 1
	}
	for row := range count {
		offset := (uint64(row)*0x9e3779b97f4a7c15 + uint64(row*row+17)) & mask
		compactPutBits(data[prefix:], row*width, width, offset)
	}
	// Padding and the unused backing capacity remain dirty. Sequential decode
	// must consume exactly count*width bits and never observe either region.
	if used := count * width; used&7 != 0 {
		data[len(data)-1] |= ^byte((uint16(1) << uint(used&7)) - 1)
	}
	view := compactStreamView{
		kind: kind, width: uint8(width), count: count, data: data,
	}
	if err := view.validate(); err != nil {
		t.Fatalf("kind=%d width=%d fixture: %v", kind, width, err)
	}
	return view
}

func TestCompactPrimaryScanFixedPackedSequentialParityAllWidths(t *testing.T) {
	const rows = 2*compactStreamRestart + 1
	tests := []struct {
		name     string
		kind     uint8
		maxWidth int
	}{
		{
			name: "FOR", kind: compactStreamFOR,
			maxWidth: 64,
		},
		{
			name: "date", kind: compactStreamDate,
			// Wider admitted date streams cannot fit the 0000..9999 ordinal
			// range even though the framing field itself is an uint8.
			maxWidth: 21,
		},
	}
	for _, test := range tests {
		for width := range test.maxWidth + 1 {
			t.Run(fmt.Sprintf("%s/width=%d", test.name, width), func(t *testing.T) {
				view := compactPrimaryFixedPackedScanView(t, test.kind, width, rows)
				checkCompactSequentialSeeks(t, view)
				var state compactStreamSequentialState
				want := make([]byte, 0, 32)
				got := make([]byte, 0, 32)
				for row := range rows {
					var wantOK, gotOK bool
					want, wantOK = view.appendValue(want[:0], row)
					got, gotOK = state.appendValue(got[:0], &view, row)
					if !wantOK || !gotOK || !bytes.Equal(got, want) {
						t.Fatalf(
							"row=%d got=%q,%v want=%q,%v state=%+v",
							row, got, gotOK, want, wantOK, state,
						)
					}
					// Callback-owned output is not decoder state.
					for at := range got {
						got[at] = 0xff
					}
				}
				if test.kind == compactStreamFOR && width > 56 {
					if state != (compactStreamSequentialState{}) {
						t.Fatalf("wide FOR advanced sequential state: %+v", state)
					}
				} else if state.next != rows {
					t.Fatalf("sequential rows=%d want=%d", state.next, rows)
				}
			})
		}
	}
}

func TestCompactPrimaryScanFixedPackedFallbackAndRollback(t *testing.T) {
	const rows = 2*compactStreamRestart + 1
	for _, kind := range []uint8{compactStreamFOR, compactStreamDate} {
		name := "FOR"
		prefix := 8
		if kind == compactStreamDate {
			name = "date"
			prefix = 4
		}
		t.Run(name, func(t *testing.T) {
			view := compactPrimaryFixedPackedScanView(t, kind, 9, rows)
			var state compactStreamSequentialState
			dst := make([]byte, 0, 32)
			for row := 0; row < 5; row++ {
				var ok bool
				dst, ok = state.appendValue(dst[:0], &view, row)
				if !ok {
					t.Fatalf("prefix row=%d", row)
				}
			}
			snapshot := state
			randomRow := compactStreamRestart + 3
			got, gotOK := state.appendValue(dst[:0], &view, randomRow)
			want, wantOK := view.appendValue(nil, randomRow)
			if !gotOK || !wantOK || !bytes.Equal(got, want) || state != snapshot {
				t.Fatalf(
					"random fallback got=%q,%v want=%q,%v state=%+v snapshot=%+v",
					got, gotOK, want, wantOK, state, snapshot,
				)
			}
			if _, ok := state.appendValue(dst[:0], &view, 5); !ok {
				t.Fatal("sequential decode did not resume after random fallback")
			}

			state = compactStreamSequentialState{}
			if _, ok := state.appendValue(dst[:0], &view, 0); !ok ||
				state.bit == 0 || state.cursor == 0 {
				t.Fatalf("packed prefix state=%+v ok=%v", state, ok)
			}
			snapshot = state
			truncated := view
			truncated.data = truncated.data[:prefix+state.cursor]
			marker := []byte{0xde, 0xad}
			got, gotOK = state.appendValue(marker, &truncated, 1)
			if gotOK || !bytes.Equal(got, marker) || state != snapshot {
				t.Fatalf(
					"truncated refill got=%x,%v state=%+v snapshot=%+v",
					got, gotOK, state, snapshot,
				)
			}
			if _, ok := state.appendValue(dst[:0], &view, 1); !ok {
				t.Fatal("sequential decode did not resume after truncated refill")
			}

			short := view
			short.data = short.data[:prefix-1]
			state = compactStreamSequentialState{}
			got, gotOK = state.appendValue(marker, &short, 0)
			if gotOK || !bytes.Equal(got, marker) ||
				state != (compactStreamSequentialState{}) {
				t.Fatalf("short base got=%x,%v state=%+v", got, gotOK, state)
			}
		})
	}
}

func TestCompactPrimaryScanFixedPackedZeroAlloc(t *testing.T) {
	const rows = 2*compactStreamRestart + 1
	for _, kind := range []uint8{compactStreamFOR, compactStreamDate} {
		name := "FOR"
		if kind == compactStreamDate {
			name = "date"
		}
		t.Run(name, func(t *testing.T) {
			view := compactPrimaryFixedPackedScanView(t, kind, 10, rows)
			dst := make([]byte, 0, 32)
			decode := func() {
				var state compactStreamSequentialState
				for row := range rows {
					var ok bool
					dst, ok = state.appendValue(dst[:0], &view, row)
					if !ok {
						panic("fixed packed sequential decode")
					}
				}
			}
			decode()
			if allocs := testing.AllocsPerRun(1_000, decode); allocs != 0 {
				t.Fatalf("allocations=%v want 0", allocs)
			}
		})
	}
}

func TestPrimaryGraphCursorDecodedConsumePreservesScanState(t *testing.T) {
	_, view, records := compactPrimaryTestPage(t, 1000, false)
	// Exercise the stateful front-key decoder rather than this corpus's legal
	// linear prefix-integer choice. Values retain the real three-shape compact
	// corpus and all of its adaptive scalar codecs.
	keys := make([][]byte, len(records))
	for row := range records {
		keys[row] = records[row].Key
	}
	view.key = compactCodecRoundTrip(t, encodeCompactFront(keys), keys)
	if view.key.kind != compactStreamFront || view.shapeCount < 2 {
		t.Fatalf("fixture key kind=%d shapes=%d", view.key.kind, view.shapeCount)
	}
	wantValues := make([][]byte, len(records))
	for row := range records {
		var err error
		wantValues[row], err = vibejson.AppendCanonicalize(
			nil, records[row].Value.Inline,
		)
		if err != nil {
			t.Fatal(err)
		}
	}

	skipped := make([]bool, len(records))
	seenShape := make([]bool, view.shapeCount)
	for row := range records {
		shape := view.rowShape(row)
		if shape >= 0 && shape < len(seenShape) && !seenShape[shape] {
			skipped[row] = true
			seenShape[shape] = true
		}
	}
	if !skipped[0] {
		t.Fatal("fixture does not consume the first base row")
	}
	for shape, seen := range seenShape {
		if !seen {
			t.Fatalf("fixture did not consume shape %d", shape)
		}
	}

	type finalState struct {
		lastRow    int
		key        compactStreamSequentialState
		keyPrior   [CommonPrimaryLeafMaxKeyBytes]byte
		streams    [compactPrimaryScanStreams]compactStreamSequentialState
		row        int
		shapeBlock int
		shapeSeen  [primaryGraphSequentialShapeSlots]uint8
	}
	run := func(consume []bool) finalState {
		cursor := PrimaryGraphCursor{
			leafBucket: view.header.Bucket,
			leaf:       view, shapeBlock: -1,
			spliceScratch: make([]byte, 0, 512),
		}
		var decoder CompactPrimaryScanDecoder
		wantRow := -1
		calls := 0
		visit := func(key, value []byte) error {
			calls++
			if wantRow < 0 || !bytes.Equal(key, records[wantRow].Key) ||
				!bytes.Equal(value, wantValues[wantRow]) {
				return ErrCommonPrimaryLeafCorrupt
			}
			// Callback output is borrowed only for this call. Poisoning it must
			// not alter the decoder's private front-key prefix or scalar state.
			for at := range key {
				key[at] = 0xff
			}
			for at := range value {
				value[at] = 0xff
			}
			return nil
		}
		for cursor.row < len(records) {
			row := cursor.row
			if consume != nil && consume[row] {
				if err := cursor.ConsumeCurrentLeafBaseDecoded(
					&decoder, records[row].Key,
				); err != nil {
					t.Fatalf("consume row=%d: %v", row, err)
				}
				continue
			}
			wantRow, calls = row, 0
			key, ref, err := cursor.visitCurrentLeafInlineUntil(
				&decoder, row+1, visit,
			)
			if err != nil || key != nil || ref != (PageRef{}) || calls != 1 {
				t.Fatalf(
					"visit row=%d key=%q ref=%+v calls=%d err=%v",
					row, key, ref, calls, err,
				)
			}
		}
		return finalState{
			lastRow: decoder.lastRow,
			key:     decoder.keyState, keyPrior: decoder.keyPrior,
			streams: decoder.streams,
			row:     cursor.row, shapeBlock: cursor.shapeBlock,
			shapeSeen: cursor.shapeSeen,
		}
	}
	control := run(nil)
	if got := run(skipped); got != control {
		t.Fatalf("decoded consume state drift\n got  %+v\n want %+v", got, control)
	}

	// A stale overlay certificate must fail before either cursor or decoder
	// progress changes, so a caller cannot accidentally resume from split state.
	cursor := PrimaryGraphCursor{
		leafBucket: view.header.Bucket,
		leaf:       view, shapeBlock: -1,
		spliceScratch: []byte{0xa5, 0x5a},
	}
	var decoder CompactPrimaryScanDecoder
	if err := cursor.ConsumeCurrentLeafBaseDecoded(
		&decoder, []byte("stale-key"),
	); err != ErrCommonPrimaryLeafCorrupt {
		t.Fatalf("stale consume error=%v", err)
	}
	var zeroStreams [compactPrimaryScanStreams]compactStreamSequentialState
	if cursor.row != 0 || cursor.shapeBlock != -1 ||
		decoder.prepared || decoder.supported || decoder.bucket != 0 ||
		decoder.generation != 0 || decoder.lastRow != 0 ||
		decoder.keyState != (compactStreamSequentialState{}) ||
		decoder.streams != zeroStreams ||
		!bytes.Equal(cursor.spliceScratch, []byte{0xa5, 0x5a}) {
		t.Fatalf(
			"stale consume advanced cursor/decoder: row=%d block=%d decoder=%+v scratch=%x",
			cursor.row, cursor.shapeBlock, decoder, cursor.spliceScratch,
		)
	}
}

// A zero-hole prepared shape is a constant canonical document. Compact pages
// currently choose another legal encoding for that input, but the scan plan's
// complete executor must still preserve this representation case rather than
// rejecting a template that needs no scalar stream.
func TestCompactPrimaryScanDecoderZeroHoleShape(t *testing.T) {
	static := []byte(`{"constant":true}`)
	view := CompactPrimaryStripeView{
		header: CommonPrimaryLeafHeader{Generation: 1, Bucket: 7},
		rows:   1, shapeCount: 1,
	}
	decoder := CompactPrimaryScanDecoder{
		bucket: 7, generation: 1, lastRow: -1,
		prepared: true, supported: true,
	}
	decoder.shapes[0].ends[0] = uint32(len(static))
	decoder.shapes[0].static = static
	got, ok := decoder.appendValue(nil, &view, 7, 0, 0, 0)
	if !ok || !bytes.Equal(got, static) {
		t.Fatalf("zero-hole scan render = %q,%v want %q", got, ok, static)
	}
}

func TestCompactPrimaryScanDecoderDictionaryPlanBounds(t *testing.T) {
	size := unsafe.Sizeof(CompactPrimaryScanDecoder{})
	if unsafe.Sizeof(uintptr(0)) == 8 {
		for _, pin := range []struct {
			name string
			got  uintptr
			want uintptr
		}{
			{"shape metadata", unsafe.Sizeof(compactPrimaryScanShape{}), 96},
			{"stream view", unsafe.Sizeof(compactStreamView{}), 104},
			{"stream plan", unsafe.Sizeof(compactPrimaryScanStream{}), 4},
			{"scan decoder", size, 30_672},
		} {
			if pin.got != pin.want {
				t.Fatalf("64-bit %s bytes=%d, want %d", pin.name, pin.got, pin.want)
			}
		}
	}
	if size > 31<<10 {
		t.Fatalf("scan decoder bytes=%d exceed bounded 31 KiB footprint", size)
	}
	for _, high := range []bool{false, true} {
		rows := 4096
		if high {
			// High-cardinality plans average roughly 960 rows per bounded leaf.
			rows = 900
		}
		_, view, _ := compactPrimaryTestPage(t, rows, high)
		bounds, largest := 0, 0
		for shape := 0; shape < view.shapeCount; shape++ {
			entry, ok := view.shapeEntry(shape)
			if !ok {
				t.Fatalf("high=%t shape=%d", high, shape)
			}
			streams := entry.streamRaw
			for range entry.template.holes {
				stream, ok := admittedCompactStream(streams)
				if !ok {
					t.Fatalf("high=%t shape=%d stream", high, shape)
				}
				if stream.kind == compactStreamDictionary {
					largest = max(largest, stream.dictCount)
					bounds += stream.dictCount + 1
				}
				streams = streams[stream.encoded:]
			}
		}
		t.Logf(
			"high=%t dictionary bounds=%d largest=%d decoder-bytes=%d",
			high, bounds, largest, size,
		)
		if bounds > compactPrimaryScanDictionaryBounds {
			t.Fatalf(
				"high=%t dictionary bounds=%d exceed plan=%d",
				high, bounds, compactPrimaryScanDictionaryBounds,
			)
		}
	}
}

func TestCompactPrimaryScanDictionarySequentialParityAndBounds(t *testing.T) {
	values := make([][]byte, 257)
	for row := range values {
		values[row] = []byte([]string{`"PT"`, `"US"`, `"DE"`}[row%3])
	}
	stream := compactCodecRoundTrip(t, encodeCompactDictionary(values), values)
	bounds := make([]uint16, stream.dictCount+1)
	for id := 0; id < stream.dictCount; id++ {
		bounds[id+1] = binary.LittleEndian.Uint16(stream.dictDir[id*2:])
	}

	var state compactStreamSequentialState
	want := make([]byte, 0, 16)
	got := make([]byte, 0, 16)
	for row := range values {
		var wantOK, gotOK bool
		want, wantOK = stream.appendValue(want[:0], row)
		got, gotOK = state.appendDictionary(got[:0], &stream, row, bounds)
		if !wantOK || !gotOK || !bytes.Equal(got, want) {
			t.Fatalf("row %d dictionary scan = %q,%v want %q,%v", row, got, gotOK, want, wantOK)
		}
	}

	// A non-sequential call keeps the complete admitted random-rank fallback.
	state = compactStreamSequentialState{}
	got, gotOK := state.appendDictionary(got[:0], &stream, 73, bounds)
	want, wantOK := stream.appendValue(want[:0], 73)
	if !wantOK || !gotOK || !bytes.Equal(got, want) || state.next != 0 {
		t.Fatalf("random fallback = %q,%v next=%d want %q,%v", got, gotOK, state.next, want, wantOK)
	}

	// Prepared bounds are trusted only after their local range is rechecked.
	bad := append([]uint16(nil), bounds...)
	bad[1] = uint16(len(stream.dictData) + 1)
	if _, ok := state.appendDictionary(got[:0], &stream, 0, bad); ok {
		t.Fatal("dictionary scan accepted an out-of-range prepared boundary")
	}
}

func compactPrimarySequentialDictionaryFixture(
	cardinality, rows int,
) (compactStreamView, []uint16, []int) {
	width := bits.Len(uint(cardinality - 1))
	ids := make([]int, rows)
	exercised := make([]bool, cardinality)
	for row := range rows {
		ids[row] = (row*251 + row*row*17 + cardinality - 1) % cardinality
		exercised[ids[row]] = true
	}
	directoryBacking := make([]byte, 2*cardinality+17)
	for at := range directoryBacking {
		directoryBacking[at] = 0xa5
	}
	directory := directoryBacking[:2*cardinality]
	dictionaryBacking := make([]byte, 2*min(rows, cardinality)+17)
	for at := range dictionaryBacking {
		dictionaryBacking[at] = 0x5a
	}
	dictionary := dictionaryBacking[:0]
	bounds := make([]uint16, cardinality+1)
	for id := range cardinality {
		if exercised[id] {
			dictionary = append(dictionary, byte(id), byte(id>>8))
		}
		bounds[id+1] = uint16(len(dictionary))
		binary.LittleEndian.PutUint16(directory[id*2:], uint16(len(dictionary)))
	}

	dataBytes := (rows*width + 7) / 8
	dataBacking := make([]byte, dataBytes+17)
	for at := range dataBacking {
		dataBacking[at] = 0xff
	}
	data := dataBacking[:dataBytes]
	clear(data)
	for row := range rows {
		compactPutBits(data, row*width, width, uint64(ids[row]))
	}
	// Keep the unused high bits and the backing capacity dirty. A sequential
	// decoder must consume exactly count*width bits and never observe either.
	if used := rows * width; used&7 != 0 {
		data[len(data)-1] |= ^byte(uint16(1)<<uint(used&7) - 1)
	}
	return compactStreamView{
		kind: compactStreamDictionary, width: uint8(width), count: rows,
		dictCount: cardinality, dictDir: directory,
		dictData: dictionary, data: data,
	}, bounds, ids
}

func TestCompactPrimaryScanDictionaryReservoirAllWidths(t *testing.T) {
	const rows = 2*compactStreamRestart + 1
	for width := 0; width <= 16; width++ {
		minimum := 1
		maximum := 1
		if width != 0 {
			minimum = 1<<(width-1) + 1
			maximum = 1 << width
			if maximum > int(^uint16(0)) {
				maximum = int(^uint16(0))
			}
		}
		for _, cardinality := range []int{minimum, maximum} {
			name := fmt.Sprintf("width=%d/cardinality=%d", width, cardinality)
			t.Run(name, func(t *testing.T) {
				stream, bounds, ids := compactPrimarySequentialDictionaryFixture(
					cardinality, rows,
				)
				if err := stream.validate(); err != nil {
					t.Fatalf("fixture is outside compact dictionary grammar: %v", err)
				}
				if got := bits.Len(uint(stream.dictCount - 1)); got != width {
					t.Fatalf("fixture width=%d want=%d", got, width)
				}
				var state compactStreamSequentialState
				var seenOffset [8]bool
				dirty := make([]byte, 32)
				for row, id := range ids {
					seenOffset[row*width&7] = true
					for at := range dirty {
						dirty[at] = 0xcc
					}
					got, ok := state.appendDictionary(dirty[:0], &stream, row, bounds)
					want, wantOK := stream.appendValue(nil, row)
					if !ok || !wantOK || !bytes.Equal(got, want) ||
						len(got) != 2 || int(binary.LittleEndian.Uint16(got)) != id {
						t.Fatalf(
							"row=%d offset=%d got=%x,%v want=%x,%v id=%d",
							row, row*width&7, got, ok, want, wantOK, id,
						)
					}
				}
				for offset := range 8 {
					mathematicallyReachable := false
					for row := range 8 {
						mathematicallyReachable = mathematicallyReachable ||
							row*width&7 == offset
					}
					if seenOffset[offset] != mathematicallyReachable {
						t.Fatalf(
							"offset=%d seen=%v reachable=%v",
							offset, seenOffset[offset], mathematicallyReachable,
						)
					}
				}

				// A retained state can serve random access without disturbing the
				// next sequential ID, then a zero value restarts the traversal.
				state = compactStreamSequentialState{}
				for row := 0; row < 5; row++ {
					if _, ok := state.appendDictionary(dirty[:0], &stream, row, bounds); !ok {
						t.Fatalf("prefix row=%d", row)
					}
				}
				snapshot := state
				randomRow := compactStreamRestart + 3
				got, ok := state.appendDictionary(dirty[:0], &stream, randomRow, bounds)
				want, wantOK := stream.appendValue(nil, randomRow)
				if !ok || !wantOK || !bytes.Equal(got, want) || state != snapshot {
					t.Fatalf("random fallback changed sequential state: got=%+v want=%+v", state, snapshot)
				}
				if _, ok := state.appendDictionary(dirty[:0], &stream, 5, bounds); !ok {
					t.Fatal("sequential decode did not resume after random fallback")
				}
				state = compactStreamSequentialState{}
				for row := range rows {
					got, ok := state.appendDictionary(dirty[:0], &stream, row, bounds)
					if !ok || len(got) != 2 ||
						int(binary.LittleEndian.Uint16(got)) != ids[row] {
						t.Fatalf("restart row=%d got=%x ok=%v", row, got, ok)
					}
				}
			})
		}
	}
}

func TestCompactPrimaryScanDictionaryReservoirRejectsMalformedState(t *testing.T) {
	stream, bounds, _ := compactPrimarySequentialDictionaryFixture(3, 3)
	assertRejectedWithoutAdvance := func(
		name string,
		state compactStreamSequentialState,
		view compactStreamView,
		prepared []uint16,
	) {
		t.Helper()
		dst := []byte{0xde, 0xad}
		before := state
		got, ok := state.appendDictionary(dst, &view, 0, prepared)
		if ok || !bytes.Equal(got, dst) || state != before {
			t.Fatalf("%s: got=%x ok=%v state=%+v want-state=%+v", name, got, ok, state, before)
		}
	}

	invalidID := stream
	invalidID.data = append([]byte(nil), stream.data...)
	clear(invalidID.data)
	compactPutBits(invalidID.data, 0, int(invalidID.width), uint64(invalidID.dictCount))
	assertRejectedWithoutAdvance("dictionary id", compactStreamSequentialState{}, invalidID, bounds)

	truncated := stream
	truncated.data = truncated.data[:0]
	assertRejectedWithoutAdvance("truncated data", compactStreamSequentialState{}, truncated, bounds)

	tooWide := stream
	tooWide.width = 17
	assertRejectedWithoutAdvance("width", compactStreamSequentialState{}, tooWide, bounds)

	decreasing := append([]uint16(nil), bounds...)
	firstID := int(compactReadBits(stream.data, 0, int(stream.width)))
	decreasing[firstID], decreasing[firstID+1] = 3, 2
	assertRejectedWithoutAdvance("decreasing bounds", compactStreamSequentialState{}, stream, decreasing)

	beyond := append([]uint16(nil), bounds...)
	beyond[firstID+1] = uint16(len(stream.dictData) + 1)
	assertRejectedWithoutAdvance("out-of-range bounds", compactStreamSequentialState{}, stream, beyond)

	// A bounds failure after one width-2 ID must preserve the six carried
	// bits, byte cursor, and next row so the same state can resume immediately.
	width2, width2Bounds, _ := compactPrimarySequentialDictionaryFixture(3, 4)
	var width2State compactStreamSequentialState
	if _, ok := width2State.appendDictionary(nil, &width2, 0, width2Bounds); !ok ||
		width2State.bit == 0 || width2State.value == 0 {
		t.Fatalf("width-2 prefix state=%+v ok=%v", width2State, ok)
	}
	width2Snapshot := width2State
	width2ID := int(compactReadBits(width2.data, int(width2.width), int(width2.width)))
	badWidth2Bounds := append([]uint16(nil), width2Bounds...)
	badWidth2Bounds[width2ID] = uint16(len(width2.dictData) + 1)
	if _, ok := width2State.appendDictionary(nil, &width2, 1, badWidth2Bounds); ok ||
		width2State != width2Snapshot {
		t.Fatalf("width-2 bad bounds advanced state: got=%+v want=%+v", width2State, width2Snapshot)
	}
	if _, ok := width2State.appendDictionary(nil, &width2, 1, width2Bounds); !ok {
		t.Fatal("width-2 state did not resume after bad bounds")
	}

	// Width 9 leaves seven bits of the next ID in the reservoir after row 0.
	// Truncating before its refill byte must not discard that partial ID.
	width9, width9Bounds, _ := compactPrimarySequentialDictionaryFixture(257, 3)
	var width9State compactStreamSequentialState
	if _, ok := width9State.appendDictionary(nil, &width9, 0, width9Bounds); !ok ||
		width9State.bit != 7 || width9State.value == 0 {
		t.Fatalf("width-9 prefix state=%+v ok=%v", width9State, ok)
	}
	width9Snapshot := width9State
	truncatedWidth9 := width9
	truncatedWidth9.data = truncatedWidth9.data[:width9State.cursor]
	if _, ok := width9State.appendDictionary(nil, &truncatedWidth9, 1, width9Bounds); ok ||
		width9State != width9Snapshot {
		t.Fatalf("width-9 truncation advanced state: got=%+v want=%+v", width9State, width9Snapshot)
	}
	if _, ok := width9State.appendDictionary(nil, &width9, 1, width9Bounds); !ok {
		t.Fatal("width-9 state did not resume after truncation")
	}
}

func compactPrimaryScanSingleStreamView(
	t *testing.T,
	encoded compactStreamEncoding,
	generation uint64,
) CompactPrimaryStripeView {
	t.Helper()
	stream, err := encoded.appendBinary(nil)
	if err != nil {
		t.Fatal(err)
	}
	static := []byte(`{"value":}`)
	const holes = 1
	templateBytes := 8 + (holes+1)*4 + len(static)
	shapeData := make([]byte, compactPrimaryShapeHeader+templateBytes, compactPrimaryShapeHeader+templateBytes+len(stream))
	binary.LittleEndian.PutUint32(shapeData, uint32(encoded.count))
	binary.LittleEndian.PutUint16(shapeData[4:], holes)
	binary.LittleEndian.PutUint32(shapeData[8:], uint32(templateBytes))
	binary.LittleEndian.PutUint32(shapeData[12:], uint32(len(stream)))
	template := shapeData[compactPrimaryShapeHeader:]
	binary.LittleEndian.PutUint16(template, holes)
	binary.LittleEndian.PutUint32(template[4:], uint32(len(static)))
	binary.LittleEndian.PutUint32(template[8:], uint32(len(static)-1))
	binary.LittleEndian.PutUint32(template[12:], uint32(len(static)))
	copy(template[16:], static)
	shapeData = append(shapeData, stream...)
	shapeDir := make([]byte, 4)
	binary.LittleEndian.PutUint32(shapeDir, uint32(len(shapeData)))
	return CompactPrimaryStripeView{
		header: CommonPrimaryLeafHeader{Bucket: 7, Generation: generation},
		rows:   encoded.count, shapeCount: 1,
		shapeDir: shapeDir, shapeData: shapeData,
	}
}

func TestCompactPrimaryScanDictionaryReservoirReprepareSameSlot(t *testing.T) {
	values := make([][]byte, 17)
	spellings := [][]byte{[]byte(`"DE"`), []byte(`"PT"`), []byte(`"US"`)}
	for row := range values {
		values[row] = spellings[row%len(spellings)]
	}
	dictionary := compactPrimaryScanSingleStreamView(
		t, encodeCompactDictionary(values), 1,
	)
	front := compactPrimaryScanSingleStreamView(t, encodeCompactFront(values), 2)
	dictionaryAgain := compactPrimaryScanSingleStreamView(
		t, encodeCompactDictionary(values), 3,
	)

	var decoder CompactPrimaryScanDecoder
	decoder.prepare(&dictionary, 7)
	plan := decoder.streamPlan[0]
	if !decoder.supported || plan.dictionaryCount == 0 {
		t.Fatal("initial dictionary stream was not planned")
	}
	first := int(plan.dictionaryFirst)
	last := first + int(plan.dictionaryCount) + 1
	for row := 0; row < 2; row++ {
		if _, ok := decoder.streams[0].appendDictionary(
			nil, &decoder.streamView[0], row, decoder.dictionary[first:last],
		); !ok {
			t.Fatalf("initial dictionary row=%d", row)
		}
	}
	if decoder.streams[0].next != 2 {
		t.Fatalf("initial dictionary state=%+v", decoder.streams[0])
	}

	decoder.prepare(&front, 7)
	if !decoder.supported || decoder.streamView[0].kind != compactStreamFront ||
		decoder.streamPlan[0] != (compactPrimaryScanStream{}) ||
		decoder.streams[0] != (compactStreamSequentialState{}) {
		t.Fatalf(
			"front reprepare left dictionary state: plan=%+v state=%+v kind=%d",
			decoder.streamPlan[0], decoder.streams[0], decoder.streamView[0].kind,
		)
	}

	decoder.prepare(&dictionaryAgain, 7)
	plan = decoder.streamPlan[0]
	if !decoder.supported || plan.dictionaryCount == 0 ||
		decoder.streams[0] != (compactStreamSequentialState{}) {
		t.Fatalf("dictionary reprepare plan=%+v state=%+v", plan, decoder.streams[0])
	}
	first = int(plan.dictionaryFirst)
	last = first + int(plan.dictionaryCount) + 1
	got, ok := decoder.streams[0].appendDictionary(
		nil, &decoder.streamView[0], 0, decoder.dictionary[first:last],
	)
	want, wantOK := decoder.streamView[0].appendValue(nil, 0)
	if !ok || !wantOK || !bytes.Equal(got, want) {
		t.Fatalf("dictionary after reprepare=%q,%v want=%q,%v", got, ok, want, wantOK)
	}
}

func TestCompactPrimaryScanDictionaryReservoirZeroAlloc(t *testing.T) {
	stream, bounds, ids := compactPrimarySequentialDictionaryFixture(65, 257)
	dst := make([]byte, 0, 8)
	decode := func() {
		var state compactStreamSequentialState
		for row, id := range ids {
			got, ok := state.appendDictionary(dst[:0], &stream, row, bounds)
			if !ok || len(got) != 2 ||
				int(binary.LittleEndian.Uint16(got)) != id {
				panic("dictionary reservoir parity")
			}
		}
	}
	decode()
	if allocs := testing.AllocsPerRun(1_000, decode); allocs != 0 {
		t.Fatalf("dictionary reservoir allocations=%v want 0", allocs)
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
