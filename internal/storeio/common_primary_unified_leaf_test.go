package storeio

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/thesyncim/vibejson"
)

func unifiedTestBounds() CommonPrimaryLeafBounds {
	return CommonPrimaryLeafBounds{
		FileEnd:           1 << 20,
		NextLogicalID:     PrimaryFirstDynamicLogicalID + 1000,
		AllocationQuantum: 4096,
	}
}

func unifiedTestStoreID() [16]byte {
	var storeID [16]byte
	for i := range storeID {
		storeID[i] = byte(i + 1)
	}
	return storeID
}

// unifiedTestCorpus builds a mixed corpus that exercises every token class and
// both row forms: corpus-shaped documents with varying tags arity (several
// templates per leaf), non-canonical spellings (unsorted keys, interstitial
// whitespace, over-escaping), typed values (ints of both signs, bools, null,
// floats that must stay literals), dictionary-worthy repeats, a >120-byte
// string (long literal), and occasional structurally unique documents that
// must degrade to trivial rows.
func unifiedTestCorpus(n int) ([]CommonPrimaryLeafRecord, [][]byte) {
	records := make([]CommonPrimaryLeafRecord, n)
	want := make([][]byte, n)
	longNote := bytes.Repeat([]byte("steady state with a very long note "), 4)
	for i := range n {
		key := fmt.Appendf(nil, "doc:%08d", i)
		var doc []byte
		switch {
		case i%17 == 0:
			// Structurally unique nested shape: unique member names force a
			// singleton skeleton, which the amortization predicate makes trivial.
			doc = fmt.Appendf(nil,
				`{"unique-%d":{"deep":[%d,{"x-%d":null}]},"f":%d.5,"e":1e%d}`,
				i, i, i, i, 1+i%7)
		case i%11 == 0:
			// Non-canonical spelling: unsorted keys, whitespace, over-escaping.
			doc = fmt.Appendf(nil,
				"{ \"zz\" : \"\\u0041-%d\" ,\n \"aa\" : [ true , false , null ] , \"mm\" : -%d }",
				i, i%100000)
		default:
			nTags := 2 + i%3
			var b bytes.Buffer
			fmt.Fprintf(&b,
				`{"active":%t,"country":"%s","id":%d,"name":"user-%d","note":"%s","profile":{"joined":"20%02d-01-02","region":"eu-west-1","tier":"%s"},"score":%d,"tags":[`,
				i%3 == 0, []string{"PT", "US", "DE", "FR"}[i%4], i, i,
				longNote, i%25, []string{"free", "pro", "team"}[i%3], i%1000)
			for t := range nTags {
				if t > 0 {
					b.WriteByte(',')
				}
				fmt.Fprintf(&b, `"%s"`, []string{"alpha", "beta", "gamma", "delta"}[(i+t)%4])
			}
			b.WriteString(`]}`)
			doc = append([]byte(nil), b.Bytes()...)
		}
		records[i] = CommonPrimaryLeafRecord{
			Key: key, Value: CommonPrimaryLeafValue{Inline: doc},
		}
		canonical, err := vibejson.AppendCanonicalize(nil, doc)
		if err != nil {
			panic(err)
		}
		want[i] = canonical
	}
	return records, want
}

func encodeUnifiedTestLeaf(
	t testing.TB, records []CommonPrimaryLeafRecord,
) ([]byte, int) {
	t.Helper()
	storeID := unifiedTestStoreID()
	bounds := unifiedTestBounds()
	builder := NewUnifiedPrimaryLeafBuilder()
	count, extent, err := planUnifiedLeaf(builder, storeID, records)
	if err != nil {
		t.Fatalf("planUnifiedLeaf: %v", err)
	}
	dst := make([]byte, extent)
	page, err := EncodeCommonPrimaryUnifiedLeaf(
		dst,
		CommonPrimaryLeafHeader{
			StoreID: storeID, Generation: 1, Bucket: 0, PageSize: uint32(extent),
		},
		storeID, records[:count], bounds, builder,
	)
	if err != nil {
		t.Fatalf("EncodeCommonPrimaryUnifiedLeaf: %v", err)
	}
	return page, count
}

func openUnifiedTestLeaf(
	t testing.TB, page []byte,
) CommonPrimaryUnifiedLeafView {
	t.Helper()
	view, err := openUnifiedTestLeafErr(page)
	if err != nil {
		t.Fatalf("OpenCommonPrimaryUnifiedLeaf: %v", err)
	}
	return view
}

func openUnifiedTestLeafErr(page []byte) (CommonPrimaryUnifiedLeafView, error) {
	logicalID, _ := CommonPrimaryLeafLogicalID(0)
	expected := PageRef{
		Offset: 4096, Length: uint32(len(page)), LogicalID: logicalID,
		Generation: 1, Kind: PagePrimaryLeaf,
	}
	return OpenCommonPrimaryUnifiedLeaf(
		page, unifiedTestStoreID(), 0, expected, 1, unifiedTestBounds(),
	)
}

// TestUnifiedLeafRoundTrip pins the codec end to end: every row of a mixed
// corpus opens, renders its exact canonical spelling by rank and by hashed
// key lookup, unique shapes degrade to trivial rows, and the encode is
// byte-for-byte deterministic.
func TestUnifiedLeafRoundTrip(t *testing.T) {
	records, want := unifiedTestCorpus(220)
	page, count := encodeUnifiedTestLeaf(t, records)
	if PrimaryLeafClass(page) != CommonPrimaryLeafUnified {
		t.Fatalf("class = %d", PrimaryLeafClass(page))
	}
	view := openUnifiedTestLeaf(t, page)
	if view.Len() != count {
		t.Fatalf("len = %d want %d", view.Len(), count)
	}
	if view.TemplateCount() == 0 {
		t.Fatal("expected at least one template")
	}
	if view.TrivialRowCount() == 0 {
		t.Fatal("expected trivial rows for the unique shapes")
	}
	if view.DictionaryCount() == 0 {
		t.Fatal("expected dictionary entries for the categorical repeats")
	}
	storeID := unifiedTestStoreID()
	for rank := 0; rank < count; rank++ {
		key, ok := view.RowAt(rank)
		if !ok || !bytes.Equal(key, records[rank].Key) {
			t.Fatalf("rank %d key mismatch", rank)
		}
		got, ok := view.AppendRawRank(nil, rank)
		if !ok || !bytes.Equal(got, want[rank]) {
			t.Fatalf("rank %d render: ok=%v\n got %q\nwant %q", rank, ok, got, want[rank])
		}
		body, overflow, found := view.LookupBodyHashed(
			KeyHashBytes(storeID, records[rank].Key), records[rank].Key,
		)
		if !found || overflow {
			t.Fatalf("rank %d lookup found=%v overflow=%v", rank, found, overflow)
		}
		byKey, ok := view.AppendRowBody(nil, body)
		if !ok || !bytes.Equal(byKey, want[rank]) {
			t.Fatalf("rank %d lookup render mismatch", rank)
		}
	}
	if got := view.FirstRankFrom(records[3].Key); got != 3 {
		t.Fatalf("FirstRankFrom = %d want 3", got)
	}
	// Admitted view agrees with the fully validated one.
	admitted, ok := AdmittedCommonPrimaryUnifiedLeaf(
		page, storeID, 0, unifiedTestBounds(),
	)
	if !ok || admitted.Len() != count {
		t.Fatalf("admitted view: ok=%v len=%d", ok, admitted.Len())
	}
	var renderer unifiedPrimaryRowRenderer
	renderer.Reset(admitted)
	scratch := make([]byte, 0, 256)
	for rank := 0; rank < count; rank++ {
		_, body, overflow, rowOK := admitted.RowRawAt(rank)
		if !rowOK || overflow {
			t.Fatalf(
				"admitted rank %d: ok=%v overflow=%v",
				rank, rowOK, overflow,
			)
		}
		scratch = renderer.Append(scratch[:0], body)
		if !bytes.Equal(scratch, want[rank]) {
			t.Fatalf(
				"admitted renderer rank %d:\n got %q\nwant %q",
				rank, scratch, want[rank],
			)
		}
	}
	// Deterministic re-encode.
	again, count2 := encodeUnifiedTestLeaf(t, records)
	if count2 != count || !bytes.Equal(page, again) {
		t.Fatalf("encode is not deterministic: count %d vs %d", count, count2)
	}
	// RenderRecords reproduces the canonical rows for the mutation path.
	rendered, _, err := view.RenderRecords(nil, nil)
	if err != nil {
		t.Fatalf("RenderRecords: %v", err)
	}
	for rank := range rendered {
		if !bytes.Equal(rendered[rank].Value.Inline, want[rank]) {
			t.Fatalf("RenderRecords rank %d mismatch", rank)
		}
	}
}

func unifiedRendererPlanCorpus(n int) ([]CommonPrimaryLeafRecord, [][]byte) {
	// Ten recurring schemas deliberately exceed the eight-entry direct-mapped
	// cache. The 3/9-16-hole shapes exercise the bounded planner; 17 holes
	// proves the arbitrary-width fallback. Cycling the schemas by key also
	// forces template IDs 0/8 and 1/9 to collide repeatedly.
	holes := [...]int{3, 9, 10, 11, 12, 13, 14, 15, 16, 17}
	records := make([]CommonPrimaryLeafRecord, n)
	want := make([][]byte, n)
	for i := range n {
		holeCount := holes[i%len(holes)]
		doc := make([]byte, 0, 256)
		if holeCount == 3 {
			doc = fmt.Appendf(
				doc, `{"left":%d,"middle":%d,"right":"row-%d"}`,
				i, i*3, i,
			)
		} else {
			doc = append(doc, `{"values":[`...)
			for hole := range holeCount {
				if hole != 0 {
					doc = append(doc, ',')
				}
				doc = fmt.Appendf(doc, "%d", i+hole)
			}
			doc = append(doc, `]}`...)
		}
		records[i] = CommonPrimaryLeafRecord{
			Key:   fmt.Appendf(nil, "plan:%08d", i),
			Value: CommonPrimaryLeafValue{Inline: doc},
		}
		canonical, err := vibejson.AppendCanonicalize(nil, doc)
		if err != nil {
			panic(err)
		}
		want[i] = canonical
	}
	return records, want
}

func TestUnifiedPrimaryRowRendererPlanCacheDifferential(t *testing.T) {
	records, want := unifiedRendererPlanCorpus(120)
	page, count := encodeUnifiedTestLeaf(t, records)
	// The encoder deliberately selects the largest prefix that fits one bounded
	// leaf. The synthetic wide-shape corpus need not fit in full; its admitted
	// prefix must still span every bounded/fallback width and repeat the direct
	// cache collisions several times.
	if count < 24 {
		t.Fatalf("planned fixture encoded only %d/%d rows", count, len(records))
	}
	view := openUnifiedTestLeaf(t, page)
	if view.TemplateCount() < 10 {
		t.Fatalf("template count = %d, want at least 10", view.TemplateCount())
	}

	admitted, ok := AdmittedCommonPrimaryUnifiedLeaf(
		page, unifiedTestStoreID(), 0, unifiedTestBounds(),
	)
	if !ok {
		t.Fatal("admitted unified fixture")
	}
	seenPlanned := false
	seenFallback := false
	var slotTemplates [unifiedRendererPlanCache][256]bool
	var renderer unifiedPrimaryRowRenderer
	renderer.Reset(admitted)
	scratch := make([]byte, 0, 512)
	for pass := range 3 {
		for rank := 0; rank < count; rank++ {
			_, body, overflow, rowOK := admitted.RowRawAt(rank)
			if !rowOK || overflow || body[0] == unifiedRowTrivial {
				t.Fatalf(
					"pass %d rank %d: ok=%v overflow=%v body=%x",
					pass, rank, rowOK, overflow, body,
				)
			}
			scratch = renderer.Append(scratch[:0], body)
			slot := body[0] & (unifiedRendererPlanCache - 1)
			slotTemplates[slot][body[0]] = true
			plan := &renderer.plans[slot]
			if plan.epoch != renderer.epoch || plan.id != body[0] {
				t.Fatalf("pass %d rank %d cache did not retain template", pass, rank)
			}
			seenPlanned = seenPlanned || plan.fast
			seenFallback = seenFallback || !plan.fast
			if !bytes.Equal(scratch, want[rank]) {
				t.Fatalf(
					"pass %d rank %d render mismatch:\n got %q\nwant %q",
					pass, rank, scratch, want[rank],
				)
			}
			checked, checkedOK := view.AppendRowBody(nil, body)
			if !checkedOK || !bytes.Equal(scratch, checked) {
				t.Fatalf("pass %d rank %d checked renderer mismatch", pass, rank)
			}
		}
		renderer.Reset(admitted)
	}
	if !seenFallback {
		t.Fatal("low-locality cache fixture never exercised generic rendering")
	}
	if !seenPlanned {
		t.Fatal("low-locality cache fixture never exercised planned rendering")
	}
	collided := false
	for slot := range unifiedRendererPlanCache {
		distinct := 0
		for id := range 256 {
			if slotTemplates[slot][id] {
				distinct++
			}
		}
		if distinct > 1 {
			collided = true
		}
	}
	if !collided {
		t.Fatal("fixture did not exercise a direct-map collision")
	}

	var allocationErr error
	allocs := testing.AllocsPerRun(20, func() {
		renderer.Reset(admitted)
		for rank := 0; rank < count; rank++ {
			_, body, overflow, rowOK := admitted.RowRawAt(rank)
			if !rowOK || overflow {
				allocationErr = ErrCommonPrimaryLeafCorrupt
				return
			}
			scratch = renderer.Append(scratch[:0], body)
		}
	})
	if allocationErr != nil {
		t.Fatal(allocationErr)
	}
	if allocs != 0 {
		t.Fatalf("warmed renderer allocated %.2f times, want 0", allocs)
	}
}

func TestUnifiedPrimaryRowRendererOrderedShapeLearnsAndFallsBack(t *testing.T) {
	documents := [][]byte{
		[]byte(`{"a":true,"b":"b0","c":1,"d":"d0","e":"e0","f":"f0","g":"g0","h":"h0","i":2,"j":"j0","k":"k0"}`),
		[]byte(`{"a":false,"b":"b1","c":3,"d":"d1","e":"e1","f":"f1","g":"g1","h":"h1","i":4,"j":"j1","k":"k1"}`),
		// The same template with null in the first scalar must reject the
		// boolean/string/integer scan signature without changing its result.
		[]byte(`{"a":null,"b":"b2","c":5,"d":"d2","e":"e2","f":"f2","g":"g2","h":"h2","i":6,"j":"j2","k":"k2"}`),
		[]byte(`{"a":false,"b":"b3","c":7,"d":"d3","e":"e3","f":"f3","g":"g3","h":"h3","i":8,"j":"j3","k":"k3"}`),
	}
	records := make([]CommonPrimaryLeafRecord, len(documents))
	want := make([][]byte, len(documents))
	for i := range documents {
		records[i] = CommonPrimaryLeafRecord{
			Key:   fmt.Appendf(nil, "shape:%02d", i),
			Value: CommonPrimaryLeafValue{Inline: documents[i]},
		}
		var err error
		want[i], err = vibejson.AppendCanonicalize(nil, documents[i])
		if err != nil {
			t.Fatal(err)
		}
	}
	page, count := encodeUnifiedTestLeaf(t, records)
	if count != len(records) {
		t.Fatalf("shape fixture encoded %d/%d rows", count, len(records))
	}
	view, ok := AdmittedCommonPrimaryUnifiedLeaf(
		page, unifiedTestStoreID(), 0, unifiedTestBounds(),
	)
	if !ok {
		t.Fatal("admitted ordered-shape fixture")
	}
	var renderer unifiedPrimaryRowRenderer
	renderer.Reset(view)
	scratch := make([]byte, 0, 256)
	var templateID uint8
	for rank := range count {
		_, body, overflow, rowOK := view.RowRawAt(rank)
		if !rowOK || overflow || body[0] == unifiedRowTrivial {
			t.Fatalf("rank %d raw row", rank)
		}
		if rank == 0 {
			templateID = body[0]
		} else if body[0] != templateID {
			t.Fatalf("rank %d template %d, want %d", rank, body[0], templateID)
		}
		scratch = renderer.Append(scratch[:0], body)
		if !bytes.Equal(scratch, want[rank]) {
			t.Fatalf("rank %d render = %q, want %q", rank, scratch, want[rank])
		}
		plan := &renderer.plans[body[0]&(unifiedRendererPlanCache-1)]
		wantModes := [...]uint8{0, 1, 2, 2}
		wantMode := wantModes[rank]
		if plan.scanShape != wantMode {
			t.Fatalf("rank %d scan shape = %d, want %d", rank, plan.scanShape, wantMode)
		}
		wantFast := rank != 0
		if plan.fast != wantFast {
			t.Fatalf("rank %d planned = %v, want %v", rank, plan.fast, wantFast)
		}
	}
}

var unifiedRendererBenchmarkSink byte

func BenchmarkUnifiedPrimaryRowRendererPlanCache(b *testing.B) {
	records, _ := unifiedRendererPlanCorpus(120)
	page, count := encodeUnifiedTestLeaf(b, records)
	view, ok := AdmittedCommonPrimaryUnifiedLeaf(
		page, unifiedTestStoreID(), 0, unifiedTestBounds(),
	)
	if !ok {
		b.Fatal("admitted unified fixture")
	}
	var renderer unifiedPrimaryRowRenderer
	renderer.Reset(view)
	scratch := make([]byte, 0, 512)
	var sink byte
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		for rank := 0; rank < count; rank++ {
			_, body, overflow, rowOK := view.RowRawAt(rank)
			if !rowOK || overflow {
				b.Fatal("raw row")
			}
			scratch = renderer.Append(scratch[:0], body)
			sink ^= scratch[0] ^ scratch[len(scratch)-1]
		}
	}
	b.StopTimer()
	b.ReportMetric(
		float64(b.Elapsed().Nanoseconds())/float64(b.N*count),
		"ns/doc",
	)
	unifiedRendererBenchmarkSink = sink
}

func TestUnifiedLeafPlanStableIntegerPatchMatchesFullPlanner(t *testing.T) {
	records, _ := unifiedTestCorpus(220)
	page, count := encodeUnifiedTestLeaf(t, records)
	view := openUnifiedTestLeaf(t, page)
	slots, slotsOK := view.PostingSlots()
	if !slotsOK {
		t.Fatal("posting slots")
	}

	var replacement CommonPrimaryUnifiedReplacement
	replacementRank := -1
	for rank := 0; rank < count; rank++ {
		_, body, overflow, ok := view.RowRawAt(rank)
		if !ok || overflow || body[0] == unifiedRowTrivial {
			continue
		}
		raw, rendered := view.AppendRawRank(nil, rank)
		if !rendered {
			t.Fatalf("render rank %d", rank)
		}
		score := bytes.Index(raw, []byte(`"score":`))
		if score < 0 {
			continue
		}
		score += len(`"score":`)
		updated := append([]byte(nil), raw...)
		// Keep both canonical byte length and the integer's zigzag-varint cost
		// fixed while choosing a spelling absent from this small leaf.
		if updated[score] == '8' {
			updated[score] = '7'
		} else {
			updated[score] = '8'
		}
		replacement = CommonPrimaryUnifiedReplacement{
			Key:   append([]byte(nil), records[rank].Key...),
			Value: updated, Slot: slots[rank],
		}
		replacementRank = rank
		break
	}
	if replacementRank < 0 {
		t.Fatal("fixture has no templated score row")
	}
	indexStorage := make([]vibejson.IndexEntry, 0, 512)
	spanStorage := make([]UnifiedTokenSpan, 0, 1024)
	workspace := NewCanonicalWorkspace(512, len(replacement.Value)*2)
	stableExtent, err := view.PatchStableReplacementKeepsExtent(
		replacement, indexStorage, &workspace, spanStorage,
	)
	if err != nil || !stableExtent {
		t.Fatalf("integer extent certificate = %v,%v, want true,nil",
			stableExtent, err)
	}
	stringChange := replacement
	stringChange.Value = append([]byte(nil), replacement.Value...)
	name := bytes.Index(stringChange.Value, []byte(`"name":"user-`))
	if name < 0 {
		t.Fatal("fixture replacement has no name string")
	}
	name += len(`"name":"`)
	stringChange.Value[name] = 'v'
	stableExtent, err = view.PatchStableReplacementKeepsExtent(
		stringChange, indexStorage, &workspace, spanStorage,
	)
	if err != nil || stableExtent {
		t.Fatalf("dictionary-eligible extent certificate = %v,%v, want false,nil",
			stableExtent, err)
	}

	header := view.Header()
	header.Generation = 2
	fast, accepted, err := view.PatchPlanStableReplacements(
		make([]byte, len(page)), header,
		[]CommonPrimaryUnifiedReplacement{replacement},
		NewUnifiedPrimaryLeafBuilder(),
	)
	if err != nil || !accepted {
		t.Fatalf("native patch = accepted %v, err %v", accepted, err)
	}

	scratch := NewPrimaryLeafMutationScratch(CommonPrimaryLeafMaxExtentBytes)
	fullRecords, err := view.RenderRecordsWithScratch(scratch)
	if err != nil {
		t.Fatal(err)
	}
	fullRecords[replacementRank].Value.Inline = replacement.Value
	full, err := EncodeBestCommonPrimaryUnifiedLeaf(
		make([]byte, CommonPrimaryLeafMaxExtentBytes),
		header, unifiedTestStoreID(), fullRecords, unifiedTestBounds(),
		NewUnifiedPrimaryLeafBuilder(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(fast, full) {
		for i := range min(len(fast), len(full)) {
			if fast[i] != full[i] {
				t.Fatalf(
					"native/full differ at byte %d: %02x != %02x",
					i, fast[i], full[i],
				)
			}
		}
		t.Fatalf("native/full lengths differ: %d != %d", len(fast), len(full))
	}

	logicalID, _ := CommonPrimaryLeafLogicalID(0)
	opened, err := OpenCommonPrimaryUnifiedLeaf(
		fast, unifiedTestStoreID(), 0,
		PageRef{
			Offset: 4096, Length: uint32(len(fast)), LogicalID: logicalID,
			Generation: 2, Kind: PagePrimaryLeaf,
		},
		2, unifiedTestBounds(),
	)
	if err != nil {
		t.Fatal(err)
	}
	got, found := opened.AppendRawRank(nil, replacementRank)
	if !found || !bytes.Equal(got, replacement.Value) {
		t.Fatalf("patched row = %q,%v, want %q", got, found, replacement.Value)
	}
}

func TestUnifiedLeafPlanVariableIntegerWidthPatchRebuildsHeap(t *testing.T) {
	records, want := unifiedTestCorpus(220)
	page, count := encodeUnifiedTestLeaf(t, records)
	source := append([]byte(nil), page...)
	view := openUnifiedTestLeaf(t, page)
	slots, slotsOK := view.PostingSlots()
	if !slotsOK {
		t.Fatal("posting slots")
	}

	replacementRank := -1
	var replacement CommonPrimaryUnifiedReplacement
	for rank := 0; rank < count; rank++ {
		raw, rendered := view.AppendRawRank(nil, rank)
		if !rendered {
			t.Fatalf("render rank %d", rank)
		}
		at := bytes.Index(raw, []byte(`"score":63`))
		if at < 0 {
			continue
		}
		_, body, overflow, rowOK := view.RowRawAt(rank)
		if !rowOK || overflow || body[0] == unifiedRowTrivial {
			continue
		}
		updated := append([]byte(nil), raw...)
		updated[at+len(`"score":6`)] = '4'
		replacement = CommonPrimaryUnifiedReplacement{
			Key: records[rank].Key, Value: updated, Slot: slots[rank],
		}
		replacementRank = rank
		break
	}
	if replacementRank < 0 {
		t.Fatal("fixture has no templated score 63 row")
	}

	header := view.Header()
	header.Generation = 2
	dst := make([]byte, len(page))
	builder := NewUnifiedPrimaryLeafBuilder()
	patched, accepted, err := view.PatchPlanStableReplacements(
		dst, header, []CommonPrimaryUnifiedReplacement{replacement}, builder,
	)
	if err != nil || !accepted {
		t.Fatalf("variable-width patch = %v,%v", accepted, err)
	}
	if !bytes.Equal(page, source) {
		t.Fatal("patch mutated source page")
	}
	oldHeader, _ := decodePageHeader(page)
	newHeader, _ := decodePageHeader(patched)
	if len(patched) != len(page) ||
		newHeader.PayloadLength != oldHeader.PayloadLength+1 {
		t.Fatalf(
			"extent/payload = %d/%d, want %d/%d",
			len(patched), newHeader.PayloadLength,
			len(page), oldHeader.PayloadLength+1,
		)
	}

	logicalID, _ := CommonPrimaryLeafLogicalID(0)
	opened, err := OpenCommonPrimaryUnifiedLeaf(
		patched, unifiedTestStoreID(), 0,
		PageRef{
			Offset: 4096, Length: uint32(len(patched)), LogicalID: logicalID,
			Generation: 2, Kind: PagePrimaryLeaf,
		},
		2, unifiedTestBounds(),
	)
	if err != nil {
		t.Fatalf("open patched leaf: %v", err)
	}
	if !bytes.Equal(
		view.env.payload[view.templateDir:view.heapStart],
		opened.env.payload[opened.templateDir:opened.heapStart],
	) {
		t.Fatal("variable-width patch changed template/dictionary sections")
	}
	for rank := 0; rank < count; rank++ {
		got, ok := opened.AppendRawRank(nil, rank)
		expected := want[rank]
		if rank == replacementRank {
			expected = replacement.Value
		}
		if !ok || !bytes.Equal(got, expected) {
			t.Fatalf("rank %d render = %q,%v want %q", rank, got, ok, expected)
		}
	}

	// All scratch growth is paid by the warm call above. The common integer
	// width-change path must not add GC pressure to a checkpoint fold.
	allocs := testing.AllocsPerRun(200, func() {
		if _, ok, patchErr := view.PatchPlanStableReplacements(
			dst, header,
			[]CommonPrimaryUnifiedReplacement{replacement}, builder,
		); patchErr != nil || !ok {
			panic("warm variable-width patch declined")
		}
	})
	if allocs != 0 {
		t.Fatalf("warmed variable-width patch allocates %.1f/op", allocs)
	}
}

func TestUnifiedLeafPlanVariableWidthRejectsFullExtentAndCorruptBody(t *testing.T) {
	makeRecords := func(member string) []CommonPrimaryLeafRecord {
		records := make([]CommonPrimaryLeafRecord, 8)
		for i := range records {
			records[i] = CommonPrimaryLeafRecord{
				Key: fmt.Appendf(nil, "doc:%08d", i),
				Value: CommonPrimaryLeafValue{Inline: fmt.Appendf(
					nil, `{"%s":"steady","score":63}`, member,
				)},
			}
		}
		return records
	}
	baseRecords := makeRecords("pad")
	base, baseCount := encodeUnifiedTestLeaf(t, baseRecords)
	if baseCount != len(baseRecords) {
		t.Fatalf("base count = %d", baseCount)
	}
	baseHeader, _ := decodePageHeader(base)
	slack := len(base) - PageHeaderSize - PageTrailerSize -
		int(baseHeader.PayloadLength)
	if slack <= 0 {
		t.Fatalf("base slack = %d", slack)
	}
	records := makeRecords(strings.Repeat("p", len("pad")+slack))
	for rank := range records {
		records[rank].Slot = baseRecords[rank].Slot
	}
	page, err := EncodeBestCommonPrimaryUnifiedLeaf(
		make([]byte, CommonPrimaryLeafMaxExtentBytes),
		CommonPrimaryLeafHeader{
			StoreID: unifiedTestStoreID(), Generation: 1, Bucket: 0,
		},
		unifiedTestStoreID(), records, unifiedTestBounds(),
		NewUnifiedPrimaryLeafBuilder(),
	)
	if err != nil {
		t.Fatalf("encode full-extent fixture: %v", err)
	}
	headerOnDisk, _ := decodePageHeader(page)
	if len(page) != len(base) ||
		int(headerOnDisk.PayloadLength) !=
			len(page)-PageHeaderSize-PageTrailerSize {
		t.Fatalf(
			"full-extent fixture extent/payload = %d/%d",
			len(page), headerOnDisk.PayloadLength,
		)
	}
	view := openUnifiedTestLeaf(t, page)
	slots, ok := view.PostingSlots()
	if !ok {
		t.Fatal("posting slots")
	}
	raw, ok := view.AppendRawRank(nil, 0)
	if !ok {
		t.Fatal("render rank zero")
	}
	at := bytes.Index(raw, []byte(`"score":63`))
	if at < 0 {
		t.Fatal("score token")
	}
	updated := append([]byte(nil), raw...)
	updated[at+len(`"score":6`)] = '4'
	replacement := CommonPrimaryUnifiedReplacement{
		Key: records[0].Key, Value: updated, Slot: slots[0],
	}
	header := view.Header()
	header.Generation = 2
	if _, accepted, err := view.PatchPlanStableReplacements(
		make([]byte, len(page)), header,
		[]CommonPrimaryUnifiedReplacement{replacement},
		NewUnifiedPrimaryLeafBuilder(),
	); err != nil || accepted {
		t.Fatalf("full-extent growth = %v,%v, want false,nil", accepted, err)
	}

	corrupted := append([]byte(nil), base...)
	corruptView := openUnifiedTestLeaf(t, corrupted)
	corruptSlots, _ := corruptView.PostingSlots()
	corruptRaw, _ := corruptView.AppendRawRank(nil, 0)
	corruptAt := bytes.Index(corruptRaw, []byte(`"score":63`))
	corruptUpdated := append([]byte(nil), corruptRaw...)
	corruptUpdated[corruptAt+len(`"score":6`)] = '4'
	_, valueStart, _, boundsOK := corruptView.env.keyBounds(0)
	if !boundsOK || corruptView.env.payload[valueStart] == unifiedRowTrivial {
		t.Fatal("corrupt fixture row is not templated")
	}
	corruptView.env.payload[valueStart+1] = 0xFD
	corruptHeader := corruptView.Header()
	corruptHeader.Generation = 2
	_, accepted, err := corruptView.PatchPlanStableReplacements(
		make([]byte, len(corrupted)), corruptHeader,
		[]CommonPrimaryUnifiedReplacement{{
			Key: records[0].Key, Value: corruptUpdated, Slot: corruptSlots[0],
		}},
		NewUnifiedPrimaryLeafBuilder(),
	)
	if accepted || !errors.Is(err, ErrCommonPrimaryLeafCorrupt) {
		t.Fatalf("corrupt body patch = %v,%v", accepted, err)
	}
}

func TestUnifiedLeafPlanStablePatchRejectsNonIncreasingGeneration(t *testing.T) {
	records, _ := unifiedTestCorpus(32)
	page, count := encodeUnifiedTestLeaf(t, records)
	view := openUnifiedTestLeaf(t, page)
	slots, slotsOK := view.PostingSlots()
	if !slotsOK || count == 0 {
		t.Fatal("posting slots")
	}
	value, rendered := view.AppendRawRank(nil, 0)
	if !rendered {
		t.Fatal("render rank zero")
	}
	replacement := CommonPrimaryUnifiedReplacement{
		Key: records[0].Key, Value: value, Slot: slots[0],
	}
	for _, generation := range []uint64{0, view.Header().Generation} {
		header := view.Header()
		header.Generation = generation
		_, accepted, err := view.PatchPlanStableReplacements(
			make([]byte, len(page)), header,
			[]CommonPrimaryUnifiedReplacement{replacement},
			NewUnifiedPrimaryLeafBuilder(),
		)
		if accepted || !errors.Is(err, ErrInvalidWrite) {
			t.Fatalf("generation %d patch = %v,%v", generation, accepted, err)
		}
	}
}

func TestUnifiedLeafPlanVariableWidthRejectsTrivialContentOverflow(t *testing.T) {
	const count = 8
	keys := make([][]byte, count)
	fixedContent := commonPrimaryUnifiedHeaderBytes
	for rank := range count {
		keys[rank] = fmt.Appendf(nil, "doc:%08d", rank)
		// One trivial row tag plus the canonical {"":9} framing; the shared
		// member spelling is added below and repeated in every logical row.
		fixedContent += len(keys[rank]) + 1 + len(`{"":9}`)
	}
	capacity := CommonPrimaryLeafMaxExtentBytes - PageHeaderSize - PageTrailerSize
	maxTrivialContent := capacity - commonPrimaryLeafLayoutFor(
		CommonPrimaryLeafWide, count, CommonPrimaryLeafMaxExtentBytes,
	).heapStart
	memberBytes := (maxTrivialContent - fixedContent) / count
	remainder := (maxTrivialContent - fixedContent) % count
	if memberBytes <= 0 || remainder < 0 {
		t.Fatalf("invalid fixture budget %d/%d", memberBytes, remainder)
	}
	keys[count-1] = append(keys[count-1], strings.Repeat("x", remainder)...)
	member := strings.Repeat("m", memberBytes)
	records := make([]CommonPrimaryLeafRecord, count)
	for rank := range records {
		records[rank] = CommonPrimaryLeafRecord{
			Key: keys[rank],
			Value: CommonPrimaryLeafValue{Inline: fmt.Appendf(
				nil, `{"%s":9}`, member,
			)},
		}
	}
	if err := PlaceCommonPrimaryLeafRecords(
		CommonPrimaryLeafWide, unifiedTestStoreID(), records,
	); err != nil {
		t.Fatalf("place fixture: %v", err)
	}
	page, err := EncodeBestCommonPrimaryUnifiedLeaf(
		make([]byte, CommonPrimaryLeafMaxExtentBytes),
		CommonPrimaryLeafHeader{
			StoreID: unifiedTestStoreID(), Generation: 1, Bucket: 0,
		},
		unifiedTestStoreID(), records, unifiedTestBounds(),
		NewUnifiedPrimaryLeafBuilder(),
	)
	if err != nil {
		t.Fatalf("encode fixture: %v", err)
	}
	view := openUnifiedTestLeaf(t, page)
	if view.TemplateCount() == 0 || view.TrivialRowCount() != 0 {
		t.Fatalf(
			"fixture templates/trivial rows = %d/%d",
			view.TemplateCount(), view.TrivialRowCount(),
		)
	}
	if got := view.TrivialContentBytes(); got != maxTrivialContent ||
		!CommonPrimaryUnifiedTrivialFits(count, got) ||
		CommonPrimaryUnifiedTrivialFits(count, got+1) {
		t.Fatalf("trivial certificate = %d cap %d", got, maxTrivialContent)
	}
	pageHeader, _ := decodePageHeader(page)
	physicalCapacity := len(page) - PageHeaderSize - PageTrailerSize
	if int(pageHeader.PayloadLength)+1 > physicalCapacity {
		t.Fatalf(
			"compressed fixture has no growth room: payload/capacity %d/%d",
			pageHeader.PayloadLength, physicalCapacity,
		)
	}

	slots, slotsOK := view.PostingSlots()
	if !slotsOK {
		t.Fatal("posting slots")
	}
	raw, rendered := view.AppendRawRank(nil, 0)
	if !rendered {
		t.Fatal("render rank zero")
	}
	at := bytes.LastIndex(raw, []byte(`:9}`))
	if at < 0 {
		t.Fatal("integer token")
	}
	updated := make([]byte, 0, len(raw)+1)
	updated = append(updated, raw[:at+1]...)
	updated = append(updated, '6', '4')
	updated = append(updated, raw[at+2:]...)
	_, valueStart, valueEnd, boundsOK := view.env.keyBounds(0)
	if !boundsOK {
		t.Fatal("row bounds")
	}
	oldBody := view.env.payload[valueStart:valueEnd:valueEnd]
	planBuilder := NewUnifiedPrimaryLeafBuilder()
	_, newBodyBytes, oldCanonicalBytes, stable, planErr :=
		view.planStableReplacementBody(planBuilder, oldBody, updated)
	if planErr != nil || !stable || newBodyBytes != len(oldBody)+1 ||
		len(updated) != oldCanonicalBytes+1 {
		t.Fatalf(
			"local width certificate = stable %v err %v body %d/%d canonical %d/%d",
			stable, planErr, newBodyBytes, len(oldBody),
			len(updated), oldCanonicalBytes,
		)
	}
	header := view.Header()
	header.Generation = 2
	_, accepted, err := view.PatchPlanStableReplacements(
		make([]byte, len(page)), header,
		[]CommonPrimaryUnifiedReplacement{{
			Key: records[0].Key, Value: updated, Slot: slots[0],
		}},
		NewUnifiedPrimaryLeafBuilder(),
	)
	if err != nil || accepted {
		t.Fatalf("trivial-overflow patch = %v,%v, want false,nil", accepted, err)
	}
}

// TestUnifiedLeafStableExtentSpannedCertificateDifferential proves that a
// scalar-span certificate emitted while canonicalizing has exactly the same
// stable-extent admission result as the original parse-the-canonical-value
// certificate. The mixed leaf corpus covers templated and trivial rows; the
// deterministic generated values exercise arbitrary shape mismatches and
// canonical rewrites without relying on handpicked accept-only cases.
func TestUnifiedLeafStableExtentSpannedCertificateDifferential(t *testing.T) {
	records, _ := unifiedTestCorpus(220)
	page, count := encodeUnifiedTestLeaf(t, records)
	view := openUnifiedTestLeaf(t, page)
	slots, slotsOK := view.PostingSlots()
	if !slotsOK {
		t.Fatal("posting slots")
	}

	const scratchEntries = 2048
	sourceStorage := make([]vibejson.IndexEntry, scratchEntries)
	newIndexStorage := make([]vibejson.IndexEntry, scratchEntries)
	oldIndexStorage := make([]vibejson.IndexEntry, scratchEntries)
	newSpanStorage := make([]UnifiedTokenSpan, 0, 2*scratchEntries)
	oldSpanStorage := make([]UnifiedTokenSpan, 0, 2*scratchEntries)
	newWorkspace := NewCanonicalWorkspace(scratchEntries, 64<<10)
	oldWorkspace := NewCanonicalWorkspace(scratchEntries, 64<<10)
	canonicalStorage := make([]byte, 0, 64<<10)
	accepted, declined := 0, 0
	compare := func(label string, key []byte, slot uint8, src []byte) {
		t.Helper()
		sourceIndex, err := vibejson.BuildIndex(src, sourceStorage)
		if err != nil {
			t.Fatalf("%s source index: %v", label, err)
		}
		canonical := src
		certificate, alreadyCanonical := CanonicalSpanIndexOf(
			sourceIndex, &newWorkspace, newSpanStorage[:0],
		)
		if !alreadyCanonical {
			canonical, certificate, err = AppendCanonicalIndexedSpans(
				canonicalStorage[:0], sourceIndex,
				&newWorkspace, newSpanStorage[:0],
			)
			if err != nil {
				t.Fatalf("%s spanned canonical render: %v", label, err)
			}
		}
		newOK, newErr := view.PatchStableCanonicalReplacementKeepsExtent(
			key, slot, certificate,
			newIndexStorage, &newWorkspace,
		)

		oldOK, oldErr := view.PatchStableReplacementKeepsExtent(
			CommonPrimaryUnifiedReplacement{
				Key: key, Value: canonical, Slot: slot,
			},
			oldIndexStorage,
			&oldWorkspace,
			oldSpanStorage[:0],
		)
		if oldOK != newOK || (oldErr == nil) != (newErr == nil) ||
			oldErr != nil && oldErr.Error() != newErr.Error() {
			t.Fatalf(
				"%s certificate divergence: old=%v,%v new=%v,%v\nsource=%q\ncanonical=%q",
				label, oldOK, oldErr, newOK, newErr, src, canonical,
			)
		}
		if oldOK {
			accepted++
		} else {
			declined++
		}
	}

	for rank := 0; rank < count; rank++ {
		compare(
			fmt.Sprintf("corpus/%d", rank),
			records[rank].Key, slots[rank], records[rank].Value.Inline,
		)
	}

	rng := rand.New(rand.NewPCG(0xC3A7, 0x5EED))
	for i := range 1_000 {
		src := appendRandomCanonicalTestValue(nil, rng, 4)
		rank := i % count
		compare(
			fmt.Sprintf("generated/%d", i),
			records[rank].Key, slots[rank], src,
		)
	}
	if accepted == 0 || declined == 0 {
		t.Fatalf(
			"differential coverage accepted=%d declined=%d, want both paths",
			accepted, declined,
		)
	}
}

func TestUnifiedLeafPlannerDictionaryExcludesCanonicalIntegers(t *testing.T) {
	records := make([]CommonPrimaryLeafRecord, 96)
	for i := range records {
		records[i] = CommonPrimaryLeafRecord{
			Key: fmt.Appendf(nil, "doc:%08d", i),
			Value: CommonPrimaryLeafValue{Inline: []byte(
				`{"category":"repeated-category","number":123456789}`,
			)},
		}
	}
	page, count := encodeUnifiedTestLeaf(t, records)
	if count != len(records) {
		t.Fatalf("encoded %d rows, want %d", count, len(records))
	}
	view := openUnifiedTestLeaf(t, page)
	if view.DictionaryCount() == 0 {
		t.Fatal("fixture did not produce a string dictionary entry")
	}
	for id := 0; id < view.DictionaryCount(); id++ {
		value := view.admittedDictionaryEntry(id)
		if integer, ok := CanonicalIntValue(value); ok {
			t.Fatalf("dictionary entry %d is canonical integer %d", id, integer)
		}
	}
}

func TestUnifiedLeafPlanStablePatchRandomizedMatchesFullPlanner(t *testing.T) {
	records, _ := unifiedTestCorpus(220)
	page, count := encodeUnifiedTestLeaf(t, records)
	view := openUnifiedTestLeaf(t, page)
	slots, slotsOK := view.PostingSlots()
	if !slotsOK {
		t.Fatal("posting slots")
	}

	eligible := make([]int, 0, count)
	for rank := 0; rank < count; rank++ {
		raw, rendered := view.AppendRawRank(nil, rank)
		if !rendered {
			t.Fatalf("render rank %d", rank)
		}
		score := bytes.Index(raw, []byte(`"score":`))
		name := bytes.Index(raw, []byte(`"name":"user-`))
		if score < 0 || name < 0 {
			continue
		}
		score += len(`"score":`)
		scoreEnd := score
		for scoreEnd < len(raw) &&
			raw[scoreEnd] >= '0' && raw[scoreEnd] <= '9' {
			scoreEnd++
		}
		if scoreEnd-score == 3 {
			eligible = append(eligible, rank)
		}
	}
	if len(eligible) < 16 {
		t.Fatalf("only %d patchable fixture rows", len(eligible))
	}

	rng := rand.New(rand.NewPCG(0xC0FFEE, 0x51A81E))
	header := view.Header()
	header.Generation = 2
	patchBuilder := NewUnifiedPrimaryLeafBuilder()
	fullBuilder := NewUnifiedPrimaryLeafBuilder()
	mutationScratch := NewPrimaryLeafMutationScratch(
		CommonPrimaryLeafMaxExtentBytes,
	)
	accepted, acceptedNonInteger := 0, 0
	for iteration := range 200 {
		replacementCount := 1 + rng.IntN(8)
		selected := make(map[int]struct{}, replacementCount)
		replacements := make(
			[]CommonPrimaryUnifiedReplacement, 0, replacementCount,
		)
		byRank := make(map[int][]byte, replacementCount)
		changeString := iteration%2 != 0
		for len(replacements) < replacementCount {
			rank := eligible[rng.IntN(len(eligible))]
			if _, duplicate := selected[rank]; duplicate {
				continue
			}
			selected[rank] = struct{}{}
			raw, _ := view.AppendRawRank(nil, rank)
			updated := append([]byte(nil), raw...)
			score := bytes.Index(updated, []byte(`"score":`)) +
				len(`"score":`)
			if updated[score] == '1' {
				updated[score] = '2'
			} else {
				updated[score] = '1'
			}
			if changeString {
				name := bytes.Index(updated, []byte(`"name":"user-`)) +
					len(`"name":"`)
				updated[name] = 'v'
			}
			replacements = append(
				replacements,
				CommonPrimaryUnifiedReplacement{
					Key: records[rank].Key, Value: updated,
					Slot: slots[rank],
				},
			)
			byRank[rank] = updated
		}

		fast, ok, err := view.PatchPlanStableReplacements(
			make([]byte, len(page)), header, replacements, patchBuilder,
		)
		if err != nil {
			t.Fatalf("iteration %d patch: %v", iteration, err)
		}
		if !ok {
			continue
		}
		accepted++
		if changeString {
			acceptedNonInteger++
		}
		fullRecords, err := view.RenderRecordsWithScratch(mutationScratch)
		if err != nil {
			t.Fatalf("iteration %d render: %v", iteration, err)
		}
		for rank, value := range byRank {
			fullRecords[rank].Value.Inline = value
		}
		full, err := EncodeBestCommonPrimaryUnifiedLeaf(
			make([]byte, CommonPrimaryLeafMaxExtentBytes),
			header, unifiedTestStoreID(), fullRecords, unifiedTestBounds(),
			fullBuilder,
		)
		if err != nil {
			t.Fatalf("iteration %d full planner: %v", iteration, err)
		}
		if !bytes.Equal(fast, full) {
			for at := range min(len(fast), len(full)) {
				if fast[at] != full[at] {
					t.Fatalf(
						"iteration %d differs at byte %d: %02x != %02x",
						iteration, at, fast[at], full[at],
					)
				}
			}
			t.Fatalf(
				"iteration %d lengths differ: %d != %d",
				iteration, len(fast), len(full),
			)
		}
	}
	if accepted < 150 || acceptedNonInteger < 50 {
		t.Fatalf(
			"accepted %d patches (%d with non-integer changes), want >=150/50",
			accepted, acceptedNonInteger,
		)
	}
}

func BenchmarkUnifiedLeafPlanStableCheckpointFold(b *testing.B) {
	records, _ := unifiedTestCorpus(220)
	storeID := unifiedTestStoreID()
	bounds := unifiedTestBounds()
	encodeBuilder := NewUnifiedPrimaryLeafBuilder()
	count, extent, err := planUnifiedLeaf(encodeBuilder, storeID, records)
	if err != nil {
		b.Fatal(err)
	}
	page, err := EncodeCommonPrimaryUnifiedLeaf(
		make([]byte, extent),
		CommonPrimaryLeafHeader{
			StoreID: storeID, Generation: 1, Bucket: 0,
			PageSize: uint32(extent),
		},
		storeID, records[:count], bounds, encodeBuilder,
	)
	if err != nil {
		b.Fatal(err)
	}
	logicalID, _ := CommonPrimaryLeafLogicalID(0)
	view, err := OpenCommonPrimaryUnifiedLeaf(
		page, storeID, 0,
		PageRef{
			Offset: 4096, Length: uint32(len(page)), LogicalID: logicalID,
			Generation: 1, Kind: PagePrimaryLeaf,
		},
		1, bounds,
	)
	if err != nil {
		b.Fatal(err)
	}
	slots, _ := view.PostingSlots()
	replacementRank := -1
	var replacement CommonPrimaryUnifiedReplacement
	for rank := 0; rank < count; rank++ {
		_, body, overflow, ok := view.RowRawAt(rank)
		if !ok || overflow || body[0] == unifiedRowTrivial {
			continue
		}
		raw, _ := view.AppendRawRank(nil, rank)
		score := bytes.Index(raw, []byte(`"score":`))
		if score < 0 {
			continue
		}
		score += len(`"score":`)
		updated := append([]byte(nil), raw...)
		if updated[score] == '8' {
			updated[score] = '7'
		} else {
			updated[score] = '8'
		}
		replacementRank = rank
		replacement = CommonPrimaryUnifiedReplacement{
			Key: records[rank].Key, Value: updated, Slot: slots[rank],
		}
		break
	}
	if replacementRank < 0 {
		b.Fatal("no patchable row")
	}
	variableRank := -1
	var variableReplacement CommonPrimaryUnifiedReplacement
	for rank := 0; rank < count; rank++ {
		raw, rendered := view.AppendRawRank(nil, rank)
		if !rendered {
			b.Fatal("render variable-width row")
		}
		at := bytes.Index(raw, []byte(`"score":63`))
		if at < 0 {
			continue
		}
		_, body, overflow, rowOK := view.RowRawAt(rank)
		if !rowOK || overflow || body[0] == unifiedRowTrivial {
			continue
		}
		updated := append([]byte(nil), raw...)
		updated[at+len(`"score":6`)] = '4'
		variableRank = rank
		variableReplacement = CommonPrimaryUnifiedReplacement{
			Key: records[rank].Key, Value: updated, Slot: slots[rank],
		}
		break
	}
	if variableRank < 0 {
		b.Fatal("no variable-width row")
	}
	header := view.Header()
	header.Generation = 2
	replacements := []CommonPrimaryUnifiedReplacement{replacement}
	b.ReportMetric(float64(count), "rows/fold")
	b.ReportMetric(float64(extent), "extent-B")

	b.Run("native-plan-certified", func(b *testing.B) {
		dst := make([]byte, CommonPrimaryLeafMaxExtentBytes)
		builder := NewUnifiedPrimaryLeafBuilder()
		if _, ok, err := view.PatchPlanStableReplacements(
			dst, header, replacements, builder,
		); err != nil || !ok {
			b.Fatalf("warm patch = %v,%v", ok, err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if _, ok, err := view.PatchPlanStableReplacements(
				dst, header, replacements, builder,
			); err != nil || !ok {
				b.Fatalf("patch = %v,%v", ok, err)
			}
		}
	})

	b.Run("generic-render-plan-encode", func(b *testing.B) {
		dst := make([]byte, CommonPrimaryLeafMaxExtentBytes)
		scratch := NewPrimaryLeafMutationScratch(
			CommonPrimaryLeafMaxExtentBytes,
		)
		builder := NewUnifiedPrimaryLeafBuilder()
		run := func() {
			rows, renderErr := view.RenderRecordsWithScratch(scratch)
			if renderErr != nil {
				b.Fatal(renderErr)
			}
			rows[replacementRank].Value.Inline = replacement.Value
			if _, encodeErr := EncodeBestCommonPrimaryUnifiedLeaf(
				dst, header, storeID, rows, bounds, builder,
			); encodeErr != nil {
				b.Fatal(encodeErr)
			}
		}
		run()
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			run()
		}
	})

	variableReplacements := []CommonPrimaryUnifiedReplacement{
		variableReplacement,
	}
	b.Run("native-plan-width-growth", func(b *testing.B) {
		dst := make([]byte, CommonPrimaryLeafMaxExtentBytes)
		builder := NewUnifiedPrimaryLeafBuilder()
		if _, ok, err := view.PatchPlanStableReplacements(
			dst, header, variableReplacements, builder,
		); err != nil || !ok {
			b.Fatalf("warm variable patch = %v,%v", ok, err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if _, ok, err := view.PatchPlanStableReplacements(
				dst, header, variableReplacements, builder,
			); err != nil || !ok {
				b.Fatalf("variable patch = %v,%v", ok, err)
			}
		}
	})

	b.Run("generic-render-plan-encode-width-growth", func(b *testing.B) {
		dst := make([]byte, CommonPrimaryLeafMaxExtentBytes)
		scratch := NewPrimaryLeafMutationScratch(
			CommonPrimaryLeafMaxExtentBytes,
		)
		builder := NewUnifiedPrimaryLeafBuilder()
		run := func() {
			rows, renderErr := view.RenderRecordsWithScratch(scratch)
			if renderErr != nil {
				b.Fatal(renderErr)
			}
			rows[variableRank].Value.Inline = variableReplacement.Value
			if _, encodeErr := EncodeBestCommonPrimaryUnifiedLeaf(
				dst, header, storeID, rows, bounds, builder,
			); encodeErr != nil {
				b.Fatal(encodeErr)
			}
		}
		run()
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			run()
		}
	})
}

// TestUnifiedLeafSingleRow pins that a single-row unified leaf
// is legal (one trivial or templated row): the planner has one output shape
// and no raw fallback.
func TestUnifiedLeafSingleRow(t *testing.T) {
	records := []CommonPrimaryLeafRecord{{
		Key:   []byte("only"),
		Value: CommonPrimaryLeafValue{Inline: []byte(`{"b":2,"a":1}`)},
	}}
	page, count := encodeUnifiedTestLeaf(t, records)
	if count != 1 {
		t.Fatalf("count = %d", count)
	}
	view := openUnifiedTestLeaf(t, page)
	got, ok := view.AppendRawRank(nil, 0)
	if !ok || string(got) != `{"a":1,"b":2}` {
		t.Fatalf("render = %q ok=%v", got, ok)
	}
	if view.TrivialRowCount() != 1 || view.TemplateCount() != 0 {
		t.Fatalf("singleton should be trivial: trivial=%d templates=%d",
			view.TrivialRowCount(), view.TemplateCount())
	}
}

func TestUnifiedLeafEmpty(t *testing.T) {
	storeID := unifiedTestStoreID()
	page, err := EncodeBestCommonPrimaryUnifiedLeaf(
		make([]byte, CommonPrimaryLeafMaxExtentBytes),
		CommonPrimaryLeafHeader{
			StoreID: storeID, Generation: 1, Bucket: 0,
		},
		storeID, nil, unifiedTestBounds(), NewUnifiedPrimaryLeafBuilder(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(page) != CommonPrimaryLeafNarrowBytes ||
		PrimaryLeafClass(page) != CommonPrimaryLeafUnified {
		t.Fatalf("empty leaf extent/class = %d/%d", len(page), PrimaryLeafClass(page))
	}
	view := openUnifiedTestLeaf(t, page)
	if view.Len() != 0 || view.TrivialContentBytes() != commonPrimaryUnifiedHeaderBytes {
		t.Fatalf(
			"empty leaf len/trivial bytes = %d/%d",
			view.Len(), view.TrivialContentBytes(),
		)
	}
	if _, _, found := view.LookupBodyHashed(
		KeyHashBytes(storeID, []byte("missing")), []byte("missing"),
	); found {
		t.Fatal("empty leaf lookup found a row")
	}
}

// mutateSealedUnified copies page, applies mutate to its payload, reseals the
// checksum (so corruption reaches the structural validators rather than the
// CRC), and returns the corrupted page.
func mutateSealedUnified(t *testing.T, page []byte, mutate func(payload []byte)) []byte {
	t.Helper()
	corrupted := append([]byte(nil), page...)
	header, _ := decodePageHeader(corrupted)
	payload := corrupted[PageHeaderSize : PageHeaderSize+int(header.PayloadLength)]
	mutate(payload)
	if _, err := sealPage(corrupted, false); err != nil {
		t.Fatalf("reseal: %v", err)
	}
	return corrupted
}

// TestUnifiedLeafCorruptionFailsClosed proves every unified section is
// independently fail-closed: targeted structural violations in the header,
// template directory and entries, dictionary directory, and row bodies must
// each reject the leaf at open.
func TestUnifiedLeafCorruptionFailsClosed(t *testing.T) {
	records, _ := unifiedTestCorpus(180)
	page, _ := encodeUnifiedTestLeaf(t, records)
	view := openUnifiedTestLeaf(t, page)
	unifiedStart := view.templateDir - commonPrimaryUnifiedHeaderBytes

	// Find one templated row's body offset for the row-level cases.
	templatedRank, trivialRank := -1, -1
	var templatedValueStart, trivialValueStart int
	for rank := 0; rank < view.Len(); rank++ {
		_, valueStart, _, ok := view.env.keyBounds(rank)
		if !ok {
			t.Fatalf("row bounds %d", rank)
		}
		if view.env.payload[valueStart] == unifiedRowTrivial {
			if trivialRank < 0 {
				trivialRank, trivialValueStart = rank, valueStart
			}
		} else if templatedRank < 0 {
			templatedRank, templatedValueStart = rank, valueStart
		}
	}
	if templatedRank < 0 || trivialRank < 0 {
		t.Fatal("corpus must produce both row forms")
	}
	_ = trivialValueStart

	cases := []struct {
		name   string
		mutate func(payload []byte)
	}{
		{"template count overflow", func(p []byte) {
			binary.LittleEndian.PutUint16(p[unifiedStart:], uint16(view.templateCount+7))
		}},
		{"dictionary count overflow", func(p []byte) {
			binary.LittleEndian.PutUint16(p[unifiedStart+2:], 200)
		}},
		{"template section bytes drift", func(p []byte) {
			v := binary.LittleEndian.Uint32(p[unifiedStart+4:])
			binary.LittleEndian.PutUint32(p[unifiedStart+4:], v+1)
		}},
		{"dictionary section bytes drift", func(p []byte) {
			v := binary.LittleEndian.Uint32(p[unifiedStart+8:])
			binary.LittleEndian.PutUint32(p[unifiedStart+8:], v+4)
		}},
		{"template directory nonmonotone", func(p []byte) {
			binary.LittleEndian.PutUint32(p[view.templateDir:], 0)
		}},
		{"template entry reserved nonzero", func(p []byte) {
			p[view.templateData+2] = 1
		}},
		{"template entry hole count drift", func(p []byte) {
			v := binary.LittleEndian.Uint16(p[view.templateData:])
			binary.LittleEndian.PutUint16(p[view.templateData:], v+1)
		}},
		{"template segment table decreasing", func(p []byte) {
			binary.LittleEndian.PutUint32(p[view.templateData+8:], ^uint32(0))
		}},
		{"dictionary directory nonmonotone", func(p []byte) {
			binary.LittleEndian.PutUint32(p[view.dictionaryDir:], 0)
		}},
		{"row template id out of range", func(p []byte) {
			p[templatedValueStart] = 0xFE
		}},
		{"row token tag unassigned", func(p []byte) {
			p[templatedValueStart+1] = 0xFD
		}},
		{"empty leaf flag", func(p []byte) {
			p[2] = byte(CommonPrimaryLeafUnified) | 0x80
		}},
		{"class byte flip", func(p []byte) {
			p[2] = byte(CommonPrimaryLeafWide)
		}},
	}
	for _, tc := range cases {
		corrupted := mutateSealedUnified(t, page, tc.mutate)
		if _, err := openUnifiedTestLeafErr(corrupted); err == nil {
			t.Fatalf("%s: corruption not detected", tc.name)
		}
	}

	// Randomized sweep: flipping any single payload byte and resealing must
	// never panic or read out of bounds, and every leaf that still opens must
	// render every row without failure (value-byte flips are legitimately
	// undetectable once the checksum is forged; structure must stay sound).
	rng := rand.New(rand.NewPCG(7, 9))
	header, _ := decodePageHeader(page)
	for range 600 {
		at := rng.IntN(int(header.PayloadLength))
		bit := byte(1) << rng.IntN(8)
		corrupted := mutateSealedUnified(t, page, func(p []byte) {
			p[at] ^= bit
		})
		opened, err := openUnifiedTestLeafErr(corrupted)
		if err != nil {
			continue
		}
		for rank := 0; rank < opened.Len(); rank++ {
			_, body, overflow, ok := opened.RowRawAt(rank)
			if !ok || overflow {
				continue
			}
			_, _ = opened.AppendRowBody(nil, body)
		}
	}
}

// TestUnifiedLeafZeroAllocRead pins the zero-GC directive at the codec level:
// a hashed lookup plus a full row splice into pre-grown scratch allocates
// nothing.
func TestUnifiedLeafZeroAllocRead(t *testing.T) {
	records, _ := unifiedTestCorpus(200)
	page, count := encodeUnifiedTestLeaf(t, records)
	view := openUnifiedTestLeaf(t, page)
	storeID := unifiedTestStoreID()
	dst := make([]byte, 0, 4096)
	key := records[count/2].Key
	hash := KeyHashBytes(storeID, key)
	allocs := testing.AllocsPerRun(200, func() {
		body, _, ok := view.LookupBodyHashed(hash, key)
		if !ok {
			t.Fatal("lookup miss")
		}
		out := view.AppendAdmittedRowBody(dst[:0], body)
		if len(out) == 0 {
			t.Fatal("render failed")
		}
	})
	if allocs != 0 {
		t.Fatalf("point read allocates %.1f/op", allocs)
	}
}

// TestUnifiedPlannerDeterministicCoverage runs the planner over a larger
// ordered corpus and pins: plans are contiguous and exhaustive, every plan
// re-encodes into its chosen extent, and two runs agree exactly.
func TestUnifiedPlannerDeterministicCoverage(t *testing.T) {
	const n = 1500
	records := make([]PrimaryGraphRecord, n)
	for i := range n {
		key := fmt.Appendf(nil, "doc:%08d", i)
		note := make([]byte, 32)
		state := uint64(i + 1)
		for at := range note {
			state = state*6364136223846793005 + 1442695040888963407
			note[at] = byte('a' + state%26)
		}
		doc := fmt.Appendf(nil,
			`{"id":%d,"name":"user-%d","country":"%s","score":%d,"active":%t,"profile":{"tier":"%s","region":"eu-west-1","joined":"2020-01-02"},"tags":["alpha","beta"],"note":"%s"}`,
			i, i, []string{"PT", "US", "DE", "FR"}[i%4], i%1000, i%3 == 0,
			[]string{"free", "pro", "team"}[i%3], note)
		records[i] = BorrowPrimaryGraphRecord(key, doc)
	}
	layout, err := MutableStoreLayout(physicalPageQuantum)
	if err != nil {
		t.Fatal(err)
	}
	plan := func() []primaryLeafPlan {
		tx := &WriteTransaction{
			options: WriteTransactionOptions{
				StoreID: unifiedTestStoreID(), Generation: 1,
				PageSize: physicalPageQuantum,
			},
			fileEnd: layout.DataStart,
			nextID:  PrimaryFirstDynamicLogicalID,
		}
		plans, err := planCompactPrimaryLeaves(
			tx.options.StoreID, records, CompactPrimaryStripeMaxRows,
			CommonPrimaryLeafWideBytes,
		)
		if err != nil {
			t.Fatalf("planUnifiedPrimaryLeaves: %v", err)
		}
		return plans
	}
	plans := plan()
	again := plan()
	if len(plans) != len(again) {
		t.Fatalf("plan count differs: %d vs %d", len(plans), len(again))
	}
	next := 0
	retainedPayloads := 0
	for at := range plans {
		if plans[at].first != next || plans[at].last <= plans[at].first {
			t.Fatalf("plan %d not contiguous: [%d,%d) after %d",
				at, plans[at].first, plans[at].last, next)
		}
		if plans[at].class != CommonPrimaryLeafCompact {
			t.Fatalf("plan %d class %d", at, plans[at].class)
		}
		if again[at].first != plans[at].first || again[at].last != plans[at].last ||
			again[at].extent != plans[at].extent ||
			!bytes.Equal(again[at].payload, plans[at].payload) {
			t.Fatalf("plan %d differs across runs", at)
		}
		next = plans[at].last
		window := make([]CommonPrimaryLeafRecord, plans[at].last-plans[at].first)
		for row := range window {
			record := records[plans[at].first+row]
			window[row] = CommonPrimaryLeafRecord{
				Key:   record.keyBytes(),
				Value: CommonPrimaryLeafValue{Inline: record.valueBytes()},
			}
		}
		fresh, err := BuildCompactPrimaryStripePayload(
			window, NewUnifiedPrimaryLeafBuilder(),
		)
		if err != nil {
			t.Fatalf("plan %d retained payload drift: %v", at, err)
		}
		if plans[at].last < len(records) {
			next := records[plans[at].last]
			larger := append(window, CommonPrimaryLeafRecord{
				Key:   next.keyBytes(),
				Value: CommonPrimaryLeafValue{Inline: next.valueBytes()},
			})
			largerPayload, largerErr := BuildCompactPrimaryStripePayload(
				larger, NewUnifiedPrimaryLeafBuilder(),
			)
			if largerErr == nil {
				need := PageHeaderSize + len(largerPayload) + PageTrailerSize
				largerExtent := (need + int(physicalPageQuantum) - 1) &^
					(int(physicalPageQuantum) - 1)
				if largerExtent <= CommonPrimaryLeafWideBytes {
					t.Fatalf(
						"plan %d stopped at %d rows but %d rows fit in %d bytes",
						at, len(window), len(larger), largerExtent,
					)
				}
			} else if largerErr != ErrCommonPrimaryLeafFull {
				t.Fatalf("plan %d larger-prefix probe: %v", at, largerErr)
			}
		}
		graphBuilder := NewUnifiedPrimaryLeafBuilder()
		graphWindow := records[plans[at].first:plans[at].last]
		if err := prepareCompactPrimaryGraphStripe(graphWindow, false, graphBuilder); err != nil {
			t.Fatalf("plan %d prepare graph payload: %v", at, err)
		}
		graphPayload, err := buildPreparedCompactPrimaryGraphStripePayload(
			graphWindow, graphBuilder,
		)
		if err != nil {
			t.Fatalf("plan %d graph payload: %v", at, err)
		}
		if !bytes.Equal(graphPayload, fresh) {
			t.Fatalf("plan %d graph payload differs from mutation payload", at)
		}
		dst := make([]byte, plans[at].extent)
		header := CommonPrimaryLeafHeader{
			StoreID: unifiedTestStoreID(), Generation: 1, Bucket: 0,
			PageSize: uint32(plans[at].extent),
		}
		var encodeErr error
		if len(plans[at].payload) != 0 {
			retainedPayloads++
			if !bytes.Equal(fresh, plans[at].payload) {
				t.Fatalf("plan %d retained payload differs from fresh encoding", at)
			}
			_, encodeErr = encodeCompactPrimaryStripePayload(
				dst, header, plans[at].payload,
			)
		} else {
			_, encodeErr = encodeCompactPrimaryGraphStripe(
				dst, header, graphWindow, false, NewUnifiedPrimaryLeafBuilder(),
			)
		}
		if encodeErr != nil {
			t.Fatalf("plan %d does not encode into %d: %v",
				at, plans[at].extent, encodeErr)
		}
	}
	if next != n {
		t.Fatalf("plans cover %d of %d records", next, n)
	}
	if retainedPayloads == 0 {
		t.Fatal("planner retained no selected payloads")
	}
	t.Logf("planner: %d leaves for %d records; first extent %d rows %d",
		len(plans), n, plans[0].extent, plans[0].last-plans[0].first)
}

func TestUnifiedPlannerExactAcrossDensityPhaseChanges(t *testing.T) {
	const (
		rows    = 640
		maxRows = 128
	)
	records := make([]PrimaryGraphRecord, rows)
	alphabet := []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
	for i := range records {
		key := fmt.Appendf(nil, "phase:%08d", i)
		if i < 160 || i >= 400 {
			records[i] = BorrowPrimaryGraphRecord(
				key, fmt.Appendf(nil, `{"id":%d,"ok":true}`, i),
			)
			continue
		}
		note := make([]byte, 256)
		state := uint64(i + 1)
		for at := range note {
			state = state*6364136223846793005 + 1442695040888963407
			note[at] = alphabet[state%uint64(len(alphabet))]
		}
		records[i] = BorrowPrimaryGraphRecord(
			key,
			fmt.Appendf(nil, `{"id":%d,"note":"%s","ok":true}`, i, note),
		)
	}

	plans, err := planCompactPrimaryLeaves(
		unifiedTestStoreID(), records, maxRows, CommonPrimaryLeafWideBytes,
	)
	if err != nil {
		t.Fatal(err)
	}
	sawDrop, sawRise := false, false
	previousCount := 0
	for rank, plan := range plans {
		count := plan.last - plan.first
		if rank != 0 {
			sawDrop = sawDrop || count < previousCount
			sawRise = sawRise || count > previousCount
		}
		previousCount = count

		hi := min(maxRows, len(records)-plan.first)
		largest := 0
		for candidate := 1; candidate <= hi; candidate++ {
			window := make([]CommonPrimaryLeafRecord, candidate)
			for row := range window {
				record := records[plan.first+row]
				window[row] = CommonPrimaryLeafRecord{
					Key:   record.keyBytes(),
					Value: CommonPrimaryLeafValue{Inline: record.valueBytes()},
				}
			}
			payload, buildErr := BuildCompactPrimaryStripePayload(
				window, NewUnifiedPrimaryLeafBuilder(),
			)
			if buildErr == ErrCommonPrimaryLeafFull {
				continue
			}
			if buildErr != nil {
				t.Fatalf("plan %d candidate %d: %v", rank, candidate, buildErr)
			}
			need := PageHeaderSize + len(payload) + PageTrailerSize
			extent := (need + int(physicalPageQuantum) - 1) &^
				(int(physicalPageQuantum) - 1)
			if extent <= CommonPrimaryLeafWideBytes {
				largest = candidate
			}
		}
		if count != largest {
			t.Fatalf("plan %d selected %d rows; exhaustive maximum is %d",
				rank, count, largest)
		}
	}
	if !sawDrop || !sawRise {
		t.Fatalf("phase corpus did not exercise both hint directions: drop=%t rise=%t",
			sawDrop, sawRise)
	}
}

func TestUnifiedPlannerBoundsLargePayloadRetention(t *testing.T) {
	const rows = 64
	records := make([]PrimaryGraphRecord, rows)
	for row := range records {
		value := []byte(`{"payload":"`)
		for at := 0; at < 2048; at++ {
			position := (row*37 + at*17 + (at>>5)*13) % 93
			b := byte(' ' + position)
			if b >= '"' {
				b++
			}
			if b >= '\\' {
				b++
			}
			value = append(value, b)
		}
		value = append(value, `"}`...)
		records[row] = BorrowPrimaryGraphRecord(
			fmt.Appendf(nil, "large:%08d", row), value,
		)
	}
	plans, err := planCompactPrimaryLeaves(
		unifiedTestStoreID(), records, CompactPrimaryStripeMaxRows,
		CommonPrimaryLeafWideBytes,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(plans) < 2 {
		t.Fatalf("large payload fixture produced %d plan, want multiple", len(plans))
	}
	plannedRows := 0
	for at := range plans {
		if len(plans[at].payload) != 0 {
			t.Fatalf(
				"plan %d retained payload=%d, want source-backed rebuild",
				at, len(plans[at].payload),
			)
		}
		plannedRows += plans[at].last - plans[at].first
	}
	if plannedRows != rows {
		t.Fatalf("large payload planned rows=%d want=%d", plannedRows, rows)
	}
}

// BenchmarkUnifiedAppendRawByKey measures the point-read splice: hashed slot
// lookup plus full canonical render of a ~250 B competitive-shape document.
func BenchmarkUnifiedAppendRawByKey(b *testing.B) {
	records := make([]CommonPrimaryLeafRecord, 200)
	for i := range records {
		key := fmt.Appendf(nil, "doc:%08d", i)
		doc := fmt.Appendf(nil,
			`{"active":%t,"country":"PT","id":%d,"name":"user-%d","note":"steady state, no anomalies observed in the last reporting window","profile":{"joined":"2020-01-02","region":"eu-west-1","tier":"pro"},"score":%d,"tags":["alpha","beta","gamma"]}`,
			i%3 == 0, i, i, i%1000)
		records[i] = CommonPrimaryLeafRecord{
			Key: key, Value: CommonPrimaryLeafValue{Inline: doc},
		}
	}
	storeID := unifiedTestStoreID()
	builder := NewUnifiedPrimaryLeafBuilder()
	count, extent, err := planUnifiedLeaf(builder, storeID, records)
	if err != nil {
		b.Fatal(err)
	}
	dst := make([]byte, extent)
	page, err := EncodeCommonPrimaryUnifiedLeaf(
		dst,
		CommonPrimaryLeafHeader{
			StoreID: storeID, Generation: 1, Bucket: 0, PageSize: uint32(extent),
		},
		storeID, records[:count], unifiedTestBounds(), builder,
	)
	if err != nil {
		b.Fatal(err)
	}
	view, ok := AdmittedCommonPrimaryUnifiedLeaf(page, storeID, 0, unifiedTestBounds())
	if !ok {
		b.Fatal("admit")
	}
	key := records[count/2].Key
	hash := KeyHashBytes(storeID, key)
	out := make([]byte, 0, 4096)
	docBytes := 0
	b.ResetTimer()
	for range b.N {
		body, _, found := view.LookupBodyHashed(hash, key)
		if !found {
			b.Fatal("miss")
		}
		out = view.AppendAdmittedRowBody(out[:0], body)
		docBytes = len(out)
	}
	b.SetBytes(int64(docBytes))
}

// BenchmarkUnifiedAdmitForMutation isolates the structural mutation bridge
// paid when a class-5 leaf cannot use the row overlay: canonical row
// reconstruction followed by one raw mutable envelope. This exposes the
// temporary heap and page allocation instead of hiding the cold-leaf cost
// inside an end-to-end workload.
func BenchmarkUnifiedAdmitForMutation(b *testing.B) {
	records := unifiedCompetitiveShapeRecords(200)
	page, _ := encodeUnifiedTestBench(b, records)
	b.Run("allocating", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			view, err := AdmittedPrimaryLeafForMutation(
				page, unifiedTestStoreID(), 0, unifiedTestBounds(),
			)
			if err != nil || view.Len() == 0 {
				b.Fatal("mutation workspace", err)
			}
		}
	})
	b.Run("retained-scratch", func(b *testing.B) {
		scratch := NewPrimaryLeafMutationScratch(CommonPrimaryLeafMaxExtentBytes)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			view, err := AdmittedPrimaryLeafForMutationWithScratch(
				page, unifiedTestStoreID(), 0, unifiedTestBounds(), scratch,
			)
			if err != nil || view.Len() == 0 {
				b.Fatal("mutation workspace", err)
			}
		}
	})
}
