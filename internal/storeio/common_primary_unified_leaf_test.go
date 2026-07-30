package storeio

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
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
			// singleton skeleton, which the §3.5 predicate makes trivial.
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
	t *testing.T, records []CommonPrimaryLeafRecord,
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
	t *testing.T, page []byte,
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
}

// TestUnifiedLeafSingleRow pins the §3.6 rule that a single-row unified leaf
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
// each reject the leaf at open (the U1 corruption gate).
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

// TestUnifiedPlannerDeterministicCoverage runs the §3.6 planner over a larger
// ordered corpus and pins: plans are contiguous and exhaustive, every plan
// re-encodes into its chosen extent, and two runs agree exactly.
func TestUnifiedPlannerDeterministicCoverage(t *testing.T) {
	const n = 1500
	records := make([]PrimaryGraphRecord, n)
	for i := range n {
		key := fmt.Appendf(nil, "doc:%08d", i)
		doc := fmt.Appendf(nil,
			`{"id":%d,"name":"user-%d","country":"%s","score":%d,"active":%t,"profile":{"tier":"%s","region":"eu-west-1","joined":"2020-01-02"},"tags":["alpha","beta"],"note":"steady state, no anomalies observed"}`,
			i, i, []string{"PT", "US", "DE", "FR"}[i%4], i%1000, i%3 == 0,
			[]string{"free", "pro", "team"}[i%3])
		records[i] = PrimaryGraphRecord{Key: key, Value: doc}
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
		plans, err := planUnifiedPrimaryLeaves(tx, records)
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
	builder := NewUnifiedPrimaryLeafBuilder()
	for at := range plans {
		if plans[at].first != next || plans[at].last <= plans[at].first {
			t.Fatalf("plan %d not contiguous: [%d,%d) after %d",
				at, plans[at].first, plans[at].last, next)
		}
		if plans[at].class != CommonPrimaryLeafUnified {
			t.Fatalf("plan %d class %d", at, plans[at].class)
		}
		if again[at].first != plans[at].first || again[at].last != plans[at].last ||
			again[at].extent != plans[at].extent {
			t.Fatalf("plan %d differs across runs", at)
		}
		next = plans[at].last
		dst := make([]byte, plans[at].extent)
		if _, err := EncodeCommonPrimaryUnifiedLeaf(
			dst,
			CommonPrimaryLeafHeader{
				StoreID: unifiedTestStoreID(), Generation: 1, Bucket: 0,
				PageSize: uint32(plans[at].extent),
			},
			unifiedTestStoreID(), plans[at].records, unifiedTestBounds(), builder,
		); err != nil {
			t.Fatalf("plan %d does not encode into %d: %v",
				at, plans[at].extent, err)
		}
	}
	if next != n {
		t.Fatalf("plans cover %d of %d records", next, n)
	}
	t.Logf("planner: %d leaves for %d records; first extent %d rows %d",
		len(plans), n, plans[0].extent, plans[0].last-plans[0].first)
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

// BenchmarkUnifiedAdmitForMutation isolates the U1-to-U2 bridge paid on the
// first mutation of a class-5 leaf: canonical row reconstruction followed by
// one raw mutable envelope. It remains useful while the row overlay is being
// landed because it makes temporary heap/page allocation visible rather than
// hiding the cold-leaf cliff inside an end-to-end workload.
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
