package storeio

import (
	"bytes"
	"math/rand/v2"
	"strconv"
	"testing"
	"unsafe"

	"github.com/thesyncim/vibejson"
)

type unifiedScalarPatchFixture struct {
	page   []byte
	view   CommonPrimaryUnifiedLeafView
	key    []byte
	slot   uint8
	rank   int
	base   []byte
	start  int
	end    int
	header CommonPrimaryLeafHeader
}

func newUnifiedScalarPatchFixture(t testing.TB) unifiedScalarPatchFixture {
	t.Helper()
	records, _ := unifiedTestCorpus(220)
	page, count := encodeUnifiedTestLeaf(t, records)
	view := openUnifiedTestLeaf(t, page)
	slots, slotsOK := view.PostingSlots()
	if !slotsOK {
		t.Fatal("posting slots")
	}
	for rank := 0; rank < count; rank++ {
		key, body, overflow, rowOK := view.RowRawAt(rank)
		if !rowOK || overflow || len(body) == 0 ||
			body[0] == unifiedRowTrivial {
			continue
		}
		base, rendered := view.AppendRawRank(nil, rank)
		if !rendered {
			continue
		}
		start := bytes.Index(base, []byte(`"score":`))
		if start < 0 {
			continue
		}
		start += len(`"score":`)
		end := start
		for end < len(base) && base[end] >= '0' && base[end] <= '9' {
			end++
		}
		if end == start {
			continue
		}
		header := view.Header()
		header.Generation++
		return unifiedScalarPatchFixture{
			page: page, view: view,
			key: append([]byte(nil), key...), slot: slots[rank], rank: rank,
			base: base, start: start, end: end, header: header,
		}
	}
	t.Fatal("no templated score row")
	return unifiedScalarPatchFixture{}
}

func (f unifiedScalarPatchFixture) replace(token []byte) []byte {
	value := make([]byte, 0, len(f.base)-(f.end-f.start)+len(token))
	value = append(value, f.base[:f.start]...)
	value = append(value, token...)
	return append(value, f.base[f.end:]...)
}

func unifiedScalarCanonicalIndex(
	t testing.TB, value []byte,
) CanonicalSpanIndex {
	t.Helper()
	entries := make([]vibejson.IndexEntry, 2048)
	index, err := vibejson.BuildIndex(value, entries)
	if err != nil {
		t.Fatalf("BuildIndex: %v", err)
	}
	workspace := NewCanonicalWorkspace(2048, 64<<10)
	certificate, canonical := CanonicalSpanIndexOf(
		index, &workspace, make([]UnifiedTokenSpan, 0, 2048),
	)
	if !canonical {
		t.Fatalf("fixture value is not canonical: %q", value)
	}
	return certificate
}

func openUnifiedScalarPatched(
	t testing.TB, page []byte, generation uint64,
) CommonPrimaryUnifiedLeafView {
	t.Helper()
	logicalID, _ := CommonPrimaryLeafLogicalID(0)
	view, err := OpenCommonPrimaryUnifiedLeaf(
		page, unifiedTestStoreID(), 0,
		PageRef{
			Offset: 4096, Length: uint32(len(page)), LogicalID: logicalID,
			Generation: generation, Kind: PagePrimaryLeaf,
		},
		generation, unifiedTestBounds(),
	)
	if err != nil {
		t.Fatalf("open patched leaf: %v", err)
	}
	return view
}

func TestUnifiedScalarPatchCertificateWidthChangeAndExact(t *testing.T) {
	if got := unsafe.Sizeof(CommonPrimaryUnifiedScalarPatch{}); got != 6 {
		t.Fatalf("certificate size = %d, want 6", got)
	}
	if got := unsafe.Sizeof(CommonPrimaryUnifiedReplacement{}); got != 56 {
		t.Fatalf("replacement size = %d, want 56", got)
	}
	f := newUnifiedScalarPatchFixture(t)

	for _, tc := range []struct {
		name  string
		value []byte
		exact bool
	}{
		{name: "exact-base", value: f.base, exact: true},
		{name: "wider-zigzag", value: f.replace([]byte("999999999999999999"))},
		{name: "typed-bool", value: f.replace([]byte("false"))},
		{name: "typed-null", value: f.replace([]byte("null"))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			canonical := unifiedScalarCanonicalIndex(t, tc.value)
			patch, _, certified, err :=
				f.view.PatchStableCanonicalReplacementScalarPatch(
					f.key, f.slot, canonical,
				)
			if err != nil || !certified || !patch.valid() || patch.exact() != tc.exact {
				t.Fatalf(
					"certificate = %+v exact=%v certified=%v err=%v",
					patch, patch.exact(), certified, err,
				)
			}
			builder := NewUnifiedPrimaryScalarPatchBuilder()
			patched, ok, err := f.view.PatchPlanScalarReplacements(
				make([]byte, len(f.page)), f.header,
				[]CommonPrimaryUnifiedReplacement{{
					Key: f.key, Value: tc.value,
					ScalarPatch: patch, Slot: f.slot,
				}},
				builder,
			)
			if err != nil || !ok {
				t.Fatalf("patch = %v,%v", ok, err)
			}
			if len(builder.indexStore) != 0 {
				t.Fatalf("compact path built %d tape entries", len(builder.indexStore))
			}
			opened := openUnifiedScalarPatched(t, patched, f.header.Generation)
			got, rendered := opened.AppendRawRank(nil, f.rank)
			if !rendered || !bytes.Equal(got, tc.value) {
				t.Fatalf("rendered = %q,%v want %q", got, rendered, tc.value)
			}
		})
	}
}

func TestUnifiedScalarPatchCertificateAdmissionAllocations(t *testing.T) {
	f := newUnifiedScalarPatchFixture(t)
	value := f.replace([]byte("999999999999999999"))
	canonical := unifiedScalarCanonicalIndex(t, value)
	var patch CommonPrimaryUnifiedScalarPatch
	var fixed, certified bool
	var err error
	allocs := testing.AllocsPerRun(1_000, func() {
		patch, fixed, certified, err =
			f.view.PatchStableCanonicalReplacementScalarPatch(
				f.key, f.slot, canonical,
			)
	})
	if allocs != 0 || err != nil || !certified || !patch.valid() {
		t.Fatalf(
			"admission = %.1f allocs patch=%+v fixed=%v certified=%v err=%v",
			allocs, patch, fixed, certified, err,
		)
	}
}

func TestUnifiedScalarPatchAdmissionResolvesNonScalarOnce(t *testing.T) {
	f := newUnifiedScalarPatchFixture(t)
	value := append([]byte(nil), f.base...)
	at := bytes.Index(value, []byte(`"name":"`))
	if at < 0 {
		t.Fatalf("name field missing from %q", value)
	}
	at += len(`"name":"`)
	if value[at] == 'x' {
		value[at] = 'y'
	} else {
		value[at] = 'x'
	}
	canonical := unifiedScalarCanonicalIndex(t, value)
	patch, fixed, resolved, err :=
		f.view.PatchStableCanonicalReplacementScalarPatch(
			f.key, f.slot, canonical,
		)
	if err != nil || !resolved || patch.valid() {
		t.Fatalf(
			"non-scalar admission = patch %+v fixed=%v resolved=%v err=%v",
			patch, fixed, resolved, err,
		)
	}
	workspace := NewCanonicalWorkspace(2048, 64<<10)
	genericFixed, genericErr := f.view.PatchStableCanonicalReplacementKeepsExtent(
		f.key, f.slot, canonical,
		make([]vibejson.IndexEntry, 2048),
		&workspace,
	)
	if genericErr != nil || fixed != genericFixed {
		t.Fatalf(
			"resolved fixed extent = %v; generic = %v,%v",
			fixed, genericFixed, genericErr,
		)
	}
}

func TestUnifiedScalarPatchAdmissionResolvedDifferential(t *testing.T) {
	f := newUnifiedScalarPatchFixture(t)
	base := unifiedScalarCanonicalIndex(t, f.base)
	if len(base.spans) < 2 {
		t.Fatalf("fixture has only %d scalar spans", len(base.spans))
	}
	tokens := [][]byte{
		[]byte("true"), []byte("false"), []byte("null"),
		[]byte("0"), []byte("63"), []byte("64"),
		[]byte("-65"), []byte("999999999999999999"),
		[]byte(`"x"`), []byte(`"dictionary-candidate"`),
		[]byte("[]"), []byte("{}"),
	}
	rng := rand.New(rand.NewPCG(0x4d455247, 0x45445052))
	for iteration := range 1_000 {
		value := append([]byte(nil), f.base...)
		selected := make(map[int][]byte, 3)
		changes := 1 + rng.IntN(min(3, len(base.spans)))
		for len(selected) < changes {
			span := rng.IntN(len(base.spans))
			selected[span] = tokens[rng.IntN(len(tokens))]
		}
		// Rewrite from right to left so the borrowed base offsets stay valid.
		for span := len(base.spans) - 1; span >= 0; span-- {
			token, ok := selected[span]
			if !ok {
				continue
			}
			start, end := int(base.spans[span].Start), int(base.spans[span].End)
			next := make([]byte, 0, len(value)-(end-start)+len(token))
			next = append(next, value[:start]...)
			next = append(next, token...)
			value = append(next, value[end:]...)
		}

		canonical := unifiedScalarCanonicalIndex(t, value)
		patch, fixed, resolved, err :=
			f.view.PatchStableCanonicalReplacementScalarPatch(
				f.key, f.slot, canonical,
			)
		if err != nil || !resolved {
			t.Fatalf(
				"iteration %d resolved admission = patch %+v fixed=%v resolved=%v err=%v value=%q",
				iteration, patch, fixed, resolved, err, value,
			)
		}
		workspace := NewCanonicalWorkspace(2048, 64<<10)
		genericFixed, genericErr :=
			f.view.PatchStableCanonicalReplacementKeepsExtent(
				f.key, f.slot, canonical,
				make([]vibejson.IndexEntry, 2048), &workspace,
			)
		if genericErr != nil || fixed != genericFixed {
			t.Fatalf(
				"iteration %d fused fixed=%v generic=%v,%v patch=%+v value=%q",
				iteration, fixed, genericFixed, genericErr, patch, value,
			)
		}
	}
}

func TestUnifiedScalarPatchPlannerDeclinesUndersizedScratchWithoutGrowth(t *testing.T) {
	f := newUnifiedScalarPatchFixture(t)
	value := f.replace([]byte("999999999999999999"))
	patch, _, certified, err :=
		f.view.PatchStableCanonicalReplacementScalarPatch(
			f.key, f.slot, unifiedScalarCanonicalIndex(t, value),
		)
	if err != nil || !certified {
		t.Fatalf("certificate = %+v,%v,%v", patch, certified, err)
	}
	replacement := []CommonPrimaryUnifiedReplacement{{
		Key: f.key, Value: value, ScalarPatch: patch, Slot: f.slot,
	}}
	dst := make([]byte, len(f.page))
	builder := NewUnifiedPrimaryLeafBuilder()
	var ok bool
	allocs := testing.AllocsPerRun(1_000, func() {
		_, ok, err = f.view.PatchPlanScalarReplacements(
			dst, f.header, replacement, builder,
		)
	})
	if err != nil || ok || allocs != 0 || cap(builder.heap) != 0 ||
		len(builder.rows) != 0 {
		t.Fatalf(
			"undersized strict plan = ok=%v err=%v allocs=%.1f heap-cap=%d rows=%d",
			ok, err, allocs, cap(builder.heap), len(builder.rows),
		)
	}

	strictBuilder := NewUnifiedPrimaryScalarPatchBuilder()
	if _, warmOK, warmErr := f.view.PatchPlanScalarReplacements(
		make([]byte, len(f.page)), f.header, replacement, strictBuilder,
	); warmErr != nil || !warmOK {
		t.Fatalf("warm strict patch = %v,%v", warmOK, warmErr)
	}
	invalidHeader := f.header
	invalidHeader.Generation = f.view.Header().Generation
	for _, tc := range []struct {
		name   string
		dst    []byte
		header CommonPrimaryLeafHeader
	}{
		{name: "short-destination", dst: make([]byte, len(f.page)-1), header: f.header},
		{name: "stale-generation", dst: make([]byte, len(f.page)), header: invalidHeader},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, accepted, invalidErr := f.view.PatchPlanScalarReplacements(
				tc.dst, tc.header, replacement, strictBuilder,
			); accepted || invalidErr == nil {
				t.Fatalf("invalid strict patch = %v,%v", accepted, invalidErr)
			}
			if len(strictBuilder.heap) != 0 || len(strictBuilder.rows) != 0 ||
				len(strictBuilder.spans) != 0 || len(strictBuilder.patchValues) != 0 {
				t.Fatalf(
					"invalid strict patch retained heap=%d rows=%d spans=%d deltas=%d",
					len(strictBuilder.heap), len(strictBuilder.rows),
					len(strictBuilder.spans), len(strictBuilder.patchValues),
				)
			}
		})
	}
}

func TestUnifiedScalarPatchCertificateCorruptionFallsBack(t *testing.T) {
	f := newUnifiedScalarPatchFixture(t)
	value := f.replace([]byte("999999999999999999"))
	patch, _, certified, err :=
		f.view.PatchStableCanonicalReplacementScalarPatch(
			f.key, f.slot, unifiedScalarCanonicalIndex(t, value),
		)
	if err != nil || !certified || patch.exact() {
		t.Fatalf("seed certificate = %+v,%v,%v", patch, certified, err)
	}

	corruptions := []struct {
		name string
		edit func(*CommonPrimaryUnifiedScalarPatch)
	}{
		{name: "body-offset", edit: func(p *CommonPrimaryUnifiedScalarPatch) { p.bodyOffset++ }},
		{name: "canonical-offset", edit: func(p *CommonPrimaryUnifiedScalarPatch) { p.canonicalOffset++ }},
		{name: "body-length", edit: func(p *CommonPrimaryUnifiedScalarPatch) { p.bodyLength++ }},
		{name: "canonical-length", edit: func(p *CommonPrimaryUnifiedScalarPatch) { p.canonicalLength++ }},
		{name: "valid-bit", edit: func(p *CommonPrimaryUnifiedScalarPatch) {
			p.bodyLength &^= commonPrimaryUnifiedScalarPatchValid
		}},
	}
	for _, tc := range corruptions {
		t.Run(tc.name, func(t *testing.T) {
			damaged := patch
			tc.edit(&damaged)
			strictBuilder := NewUnifiedPrimaryScalarPatchBuilder()
			if _, warmOK, warmErr := f.view.PatchPlanScalarReplacements(
				make([]byte, len(f.page)), f.header,
				[]CommonPrimaryUnifiedReplacement{{
					Key: f.key, Value: value,
					ScalarPatch: patch, Slot: f.slot,
				}},
				strictBuilder,
			); warmErr != nil || !warmOK {
				t.Fatalf("strict warm patch = %v,%v", warmOK, warmErr)
			}
			if _, strictOK, strictErr := f.view.PatchPlanScalarReplacements(
				make([]byte, len(f.page)), f.header,
				[]CommonPrimaryUnifiedReplacement{{
					Key: f.key, Value: value,
					ScalarPatch: damaged, Slot: f.slot,
				}},
				strictBuilder,
			); strictErr != nil || strictOK {
				t.Fatalf("strict damaged patch = %v,%v, want false,nil", strictOK, strictErr)
			}
			if len(strictBuilder.indexStore) != 0 || len(strictBuilder.heap) != 0 ||
				len(strictBuilder.spans) != 0 || len(strictBuilder.rows) != 0 ||
				len(strictBuilder.patchValues) != 0 {
				t.Fatalf(
					"strict decline retained scratch: index=%d heap=%d spans=%d rows=%d deltas=%d",
					len(strictBuilder.indexStore), len(strictBuilder.heap),
					len(strictBuilder.spans), len(strictBuilder.rows),
					len(strictBuilder.patchValues),
				)
			}
			builder := NewUnifiedPrimaryLeafBuilder()
			patched, ok, err := f.view.PatchPlanStableReplacements(
				make([]byte, len(f.page)), f.header,
				[]CommonPrimaryUnifiedReplacement{{
					Key: f.key, Value: value,
					ScalarPatch: damaged, Slot: f.slot,
				}},
				builder,
			)
			if err != nil || !ok {
				t.Fatalf("fallback patch = %v,%v", ok, err)
			}
			if len(builder.indexStore) == 0 {
				t.Fatal("damaged certificate did not enter generic planner")
			}
			opened := openUnifiedScalarPatched(t, patched, f.header.Generation)
			got, rendered := opened.AppendRawRank(nil, f.rank)
			if !rendered || !bytes.Equal(got, value) {
				t.Fatalf("fallback rendered = %q,%v want %q", got, rendered, value)
			}
		})
	}
}

func TestUnifiedScalarPatchCertificateDifferential(t *testing.T) {
	f := newUnifiedScalarPatchFixture(t)
	rng := rand.New(rand.NewPCG(0x5ca1a2, 0xc3a71f1e))
	tokens := [][]byte{
		[]byte("true"), []byte("false"), []byte("null"),
		[]byte("0"), []byte("63"), []byte("64"),
		[]byte("-64"), []byte("-65"),
		[]byte("999999999999999999"), []byte("-999999999999999999"),
	}
	for range 1_000 {
		var token []byte
		if rng.IntN(4) == 0 {
			token = tokens[rng.IntN(len(tokens))]
		} else {
			value := int64(rng.Uint64() % 1_000_000_000_000_000_000)
			if rng.IntN(2) != 0 {
				value = -value
			}
			token = strconv.AppendInt(nil, value, 10)
		}
		value := f.replace(token)
		patch, _, certified, err :=
			f.view.PatchStableCanonicalReplacementScalarPatch(
				f.key, f.slot, unifiedScalarCanonicalIndex(t, value),
			)
		if err != nil || !certified {
			t.Fatalf("token %q certificate = %+v,%v,%v", token, patch, certified, err)
		}
		patched, ok, err := f.view.PatchPlanStableReplacements(
			make([]byte, len(f.page)), f.header,
			[]CommonPrimaryUnifiedReplacement{{
				Key: f.key, Value: value, ScalarPatch: patch, Slot: f.slot,
			}},
			NewUnifiedPrimaryLeafBuilder(),
		)
		if err != nil || !ok {
			t.Fatalf("token %q patch = %v,%v", token, ok, err)
		}
		opened := openUnifiedScalarPatched(t, patched, f.header.Generation)
		got, rendered := opened.AppendRawRank(nil, f.rank)
		if !rendered || !bytes.Equal(got, value) {
			t.Fatalf("token %q rendered = %q,%v", token, got, rendered)
		}
	}
}

func TestUnifiedScalarPatchCertificateArbitraryReplacementOrder(t *testing.T) {
	f := newUnifiedScalarPatchFixture(t)
	slots, slotsOK := f.view.PostingSlots()
	if !slotsOK {
		t.Fatal("posting slots")
	}
	replacements := make([]CommonPrimaryUnifiedReplacement, 0, 4)
	for rank := 0; rank < f.view.Len() && len(replacements) < cap(replacements); rank++ {
		key, body, overflow, rowOK := f.view.RowRawAt(rank)
		if !rowOK || overflow || len(body) == 0 || body[0] == unifiedRowTrivial {
			continue
		}
		value, rendered := f.view.AppendRawRank(nil, rank)
		if !rendered {
			continue
		}
		start := bytes.Index(value, []byte(`"active":`))
		if start < 0 {
			continue
		}
		start += len(`"active":`)
		var oldToken, newToken []byte
		if bytes.HasPrefix(value[start:], []byte("true")) {
			oldToken, newToken = []byte("true"), []byte("false")
		} else if bytes.HasPrefix(value[start:], []byte("false")) {
			oldToken, newToken = []byte("false"), []byte("true")
		} else {
			continue
		}
		updated := make([]byte, 0, len(value)-len(oldToken)+len(newToken))
		updated = append(updated, value[:start]...)
		updated = append(updated, newToken...)
		updated = append(updated, value[start+len(oldToken):]...)
		patch, _, certified, err :=
			f.view.PatchStableCanonicalReplacementScalarPatch(
				key, slots[rank], unifiedScalarCanonicalIndex(t, updated),
			)
		if err != nil {
			t.Fatal(err)
		}
		if certified {
			replacements = append(replacements, CommonPrimaryUnifiedReplacement{
				Key: key, Value: updated, ScalarPatch: patch, Slot: slots[rank],
			})
		}
	}
	if len(replacements) != cap(replacements) {
		t.Fatalf("only %d arbitrary-order replacements", len(replacements))
	}
	for left, right := 0, len(replacements)-1; left < right; left, right = left+1, right-1 {
		replacements[left], replacements[right] = replacements[right], replacements[left]
	}
	if bytes.Compare(replacements[0].Key, replacements[len(replacements)-1].Key) <= 0 {
		t.Fatal("fixture reversal did not produce non-lexical input")
	}
	patched, ok, err := f.view.PatchPlanScalarReplacements(
		make([]byte, len(f.page)), f.header, replacements,
		NewUnifiedPrimaryScalarPatchBuilder(),
	)
	if err != nil || !ok {
		t.Fatalf("arbitrary-order patch = %v,%v", ok, err)
	}
	opened := openUnifiedScalarPatched(t, patched, f.header.Generation)
	for _, replacement := range replacements {
		rank := opened.env.LowerBound(replacement.Key)
		got, rendered := opened.AppendRawRank(nil, rank)
		if !rendered || !bytes.Equal(got, replacement.Value) {
			t.Fatalf("key %q rendered = %q,%v want %q",
				replacement.Key, got, rendered, replacement.Value)
		}
	}
}

func TestUnifiedScalarPatchMixedExactAndWidthChange(t *testing.T) {
	f := newUnifiedScalarPatchFixture(t)
	slots, slotsOK := f.view.PostingSlots()
	if !slotsOK {
		t.Fatal("posting slots")
	}
	exactPatch, _, exactResolved, err :=
		f.view.PatchStableCanonicalReplacementScalarPatch(
			f.key, f.slot, unifiedScalarCanonicalIndex(t, f.base),
		)
	if err != nil || !exactResolved || !exactPatch.exact() {
		t.Fatalf("exact certificate = %+v,%v,%v", exactPatch, exactResolved, err)
	}
	replacements := []CommonPrimaryUnifiedReplacement{{
		Key: f.key, Value: f.base, ScalarPatch: exactPatch, Slot: f.slot,
	}}
	changedRank := -1
	for rank := 0; rank < f.view.Len(); rank++ {
		if rank == f.rank {
			continue
		}
		key, body, overflow, rowOK := f.view.RowRawAt(rank)
		if !rowOK || overflow || len(body) == 0 || body[0] == unifiedRowTrivial {
			continue
		}
		value, rendered := f.view.AppendRawRank(nil, rank)
		if !rendered {
			continue
		}
		start := bytes.Index(value, []byte(`"score":`))
		if start < 0 {
			continue
		}
		start += len(`"score":`)
		end := start
		for end < len(value) && value[end] >= '0' && value[end] <= '9' {
			end++
		}
		if end == start {
			continue
		}
		updated := make([]byte, 0, len(value)+20)
		updated = append(updated, value[:start]...)
		updated = append(updated, "999999999999999999"...)
		updated = append(updated, value[end:]...)
		patch, _, resolved, patchErr :=
			f.view.PatchStableCanonicalReplacementScalarPatch(
				key, slots[rank], unifiedScalarCanonicalIndex(t, updated),
			)
		if patchErr != nil {
			t.Fatal(patchErr)
		}
		if !resolved || !patch.valid() || patch.exact() {
			continue
		}
		replacements = append(replacements, CommonPrimaryUnifiedReplacement{
			Key: key, Value: updated, ScalarPatch: patch, Slot: slots[rank],
		})
		changedRank = rank
		break
	}
	if changedRank < 0 {
		t.Fatal("no second width-changing templated row")
	}

	builder := NewUnifiedPrimaryScalarPatchBuilder()
	patched, ok, err := f.view.PatchPlanScalarReplacements(
		make([]byte, len(f.page)), f.header, replacements, builder,
	)
	if err != nil || !ok {
		t.Fatalf("mixed strict patch = %v,%v", ok, err)
	}
	if len(builder.rows) != 1 || len(builder.heap) == 0 {
		t.Fatalf("mixed strict scratch rows/heap = %d/%d, want 1/nonzero",
			len(builder.rows), len(builder.heap))
	}
	opened := openUnifiedScalarPatched(t, patched, f.header.Generation)
	for _, replacement := range replacements {
		rank := opened.env.LowerBound(replacement.Key)
		got, rendered := opened.AppendRawRank(nil, rank)
		if !rendered || !bytes.Equal(got, replacement.Value) {
			t.Fatalf("rank %d rendered = %q,%v want %q",
				rank, got, rendered, replacement.Value)
		}
	}
}

func BenchmarkUnifiedScalarPatchCertificate(b *testing.B) {
	f := newUnifiedScalarPatchFixture(b)
	value := f.replace([]byte("999999999999999999"))
	patch, _, certified, err :=
		f.view.PatchStableCanonicalReplacementScalarPatch(
			f.key, f.slot, unifiedScalarCanonicalIndex(b, value),
		)
	if err != nil || !certified {
		b.Fatalf("certificate = %+v,%v,%v", patch, certified, err)
	}

	for _, tc := range []struct {
		name  string
		patch CommonPrimaryUnifiedScalarPatch
	}{
		{name: "certified", patch: patch},
		{name: "generic", patch: CommonPrimaryUnifiedScalarPatch{}},
	} {
		b.Run(tc.name, func(b *testing.B) {
			builder := NewUnifiedPrimaryLeafBuilder()
			if tc.name == "certified" {
				builder = NewUnifiedPrimaryScalarPatchBuilder()
			}
			dst := make([]byte, len(f.page))
			replacement := []CommonPrimaryUnifiedReplacement{{
				Key: f.key, Value: value, ScalarPatch: tc.patch, Slot: f.slot,
			}}
			if _, ok, err := f.view.PatchPlanStableReplacements(
				dst, f.header, replacement, builder,
			); err != nil || !ok {
				b.Fatalf("warm patch = %v,%v", ok, err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, ok, err := f.view.PatchPlanStableReplacements(
					dst, f.header, replacement, builder,
				); err != nil || !ok {
					b.Fatalf("patch = %v,%v", ok, err)
				}
			}
		})
	}
}

func BenchmarkUnifiedScalarPatchCertificateBatch(b *testing.B) {
	f := newUnifiedScalarPatchFixture(b)
	slots, slotsOK := f.view.PostingSlots()
	if !slotsOK {
		b.Fatal("posting slots")
	}
	replacements := make([]CommonPrimaryUnifiedReplacement, 0, 128)
	for rank := 0; rank < f.view.Len() && len(replacements) < cap(replacements); rank++ {
		key, body, overflow, rowOK := f.view.RowRawAt(rank)
		if !rowOK || overflow || len(body) == 0 ||
			body[0] == unifiedRowTrivial {
			continue
		}
		value, rendered := f.view.AppendRawRank(nil, rank)
		if !rendered {
			continue
		}
		start := bytes.Index(value, []byte(`"active":`))
		if start < 0 {
			continue
		}
		start += len(`"active":`)
		var oldToken, newToken []byte
		if bytes.HasPrefix(value[start:], []byte("true")) {
			oldToken, newToken = []byte("true"), []byte("false")
		} else if bytes.HasPrefix(value[start:], []byte("false")) {
			oldToken, newToken = []byte("false"), []byte("true")
		} else {
			continue
		}
		updated := make([]byte, 0, len(value)-len(oldToken)+len(newToken))
		updated = append(updated, value[:start]...)
		updated = append(updated, newToken...)
		updated = append(updated, value[start+len(oldToken):]...)
		patch, _, certified, err :=
			f.view.PatchStableCanonicalReplacementScalarPatch(
				key, slots[rank], unifiedScalarCanonicalIndex(b, updated),
			)
		if err != nil {
			b.Fatal(err)
		}
		if !certified {
			continue
		}
		replacements = append(replacements, CommonPrimaryUnifiedReplacement{
			Key: key, Value: updated, ScalarPatch: patch, Slot: slots[rank],
		})
	}
	if len(replacements) < 64 {
		b.Fatalf("only %d certified batch rows", len(replacements))
	}
	b.ReportMetric(float64(len(replacements)), "rows")

	for _, tc := range []struct {
		name   string
		strict bool
	}{
		{name: "certified", strict: true},
		{name: "generic"},
	} {
		b.Run(tc.name, func(b *testing.B) {
			candidate := append([]CommonPrimaryUnifiedReplacement(nil), replacements...)
			if !tc.strict {
				for i := range candidate {
					candidate[i].ScalarPatch = CommonPrimaryUnifiedScalarPatch{}
				}
			}
			builder := NewUnifiedPrimaryLeafBuilder()
			if tc.strict {
				builder = NewUnifiedPrimaryScalarPatchBuilder()
			}
			dst := make([]byte, len(f.page))
			patch := f.view.PatchPlanStableReplacements
			if tc.strict {
				patch = f.view.PatchPlanScalarReplacements
			}
			if _, ok, err := patch(dst, f.header, candidate, builder); err != nil || !ok {
				b.Fatalf("warm batch patch = %v,%v", ok, err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, ok, err := patch(
					dst, f.header, candidate, builder,
				); err != nil || !ok {
					b.Fatalf("batch patch = %v,%v", ok, err)
				}
			}
			b.ReportMetric(float64(len(candidate)), "rows/op")
		})
	}
}
