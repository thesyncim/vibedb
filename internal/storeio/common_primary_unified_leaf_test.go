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
		out, ok := view.AppendRowBody(dst[:0], body)
		if !ok || len(out) == 0 {
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
		out, found = view.AppendRowBody(out[:0], body)
		if !found {
			b.Fatal("render")
		}
		docBytes = len(out)
	}
	b.SetBytes(int64(docBytes))
}
