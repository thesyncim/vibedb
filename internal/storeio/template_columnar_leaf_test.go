package storeio

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"testing"
	"unsafe"

	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/document"
)

func templateColumnarLeafRows(t testing.TB, count int, uniqueShape bool) []TemplateColumnarLeafRow {
	t.Helper()
	rows := make([]TemplateColumnarLeafRow, count)
	for i := range rows {
		var doc []byte
		if uniqueShape {
			doc = fmt.Appendf(nil, `{"id":%d,"shape_%03d":"value-%03d","nested":{"ok":%t}}`,
				i, i, i, i&1 == 0)
		} else {
			doc = fmt.Appendf(nil, `{"id":%d,"name":"user-%03d","score":%d,"active":%t,`+
				`"nested":{"note":"exact spelling \u263a","nil":null}}`,
				i, i, i%1000, i&1 == 0)
		}
		rows[i] = TemplateColumnarLeafRow{
			Slot: uint8((i*197 + 17) & 255),
			Key:  []byte(fmt.Sprintf("doc:%08d", i)),
			JSON: doc,
		}
	}
	return rows
}

func TestTemplateColumnarLeafExtractionExact(t *testing.T) {
	docs := [][]byte{
		[]byte(`{"a":1,"b":"x","c":null,"d":true}`),
		[]byte(" \n { \"escaped\\u006b\" : -0.00e+2, \"a\" : [false,{\"z\":\"\\\\u263a\"}] } \t"),
		[]byte(`[1,2,3,{"x":[],"y":{}}]`),
		[]byte(`"root scalar"`),
	}
	rng := rand.New(rand.NewSource(0x501))
	for i := 0; i < 250; i++ {
		docs = append(docs, fmt.Appendf(nil,
			`{"id":%d,"s":"v-%08x","n":%d.%02d,"b":%t,"a":[%d,null,"%c"]}`,
			i, rng.Uint32(), rng.Intn(10000), rng.Intn(100), i&1 == 0,
			rng.Intn(50), 'a'+rune(rng.Intn(26))))
	}
	for i, src := range docs {
		storage := make([]vibejson.IndexEntry, len(src)+2)
		got, err := ExtractTemplateColumnarLeaf(src, storage)
		if err != nil {
			t.Fatalf("doc %d: %v", i, err)
		}
		dst, ok := got.Append(make([]byte, 0, len(src)))
		if !ok || !bytes.Equal(dst, src) {
			t.Fatalf("doc %d exact splice mismatch:\n got %q\nwant %q", i, dst, src)
		}
		again, err := ExtractTemplateColumnarLeaf(src, storage)
		if err != nil || got.ID != again.ID {
			t.Fatalf("doc %d unstable fused fingerprint", i)
		}
	}
}

func TestTemplateColumnarLeafRoundTripStableSlotsAndFields(t *testing.T) {
	rows := templateColumnarLeafRows(t, 190, false)
	image, err := EncodeTemplateColumnarLeaf(rows)
	if err != nil {
		t.Fatal(err)
	}
	view, err := OpenTemplateColumnarLeaf(image)
	if err != nil {
		t.Fatal(err)
	}
	if int(view.count) != len(rows) || view.templates != 1 {
		t.Fatalf("count/templates=%d/%d", view.count, view.templates)
	}
	zone, ok := view.Zone(0, 2)
	if !ok || zone.Kind != document.Number || string(zone.Min) != "0" ||
		string(zone.Max) != "99" {
		t.Fatalf("score zone=%+v ok=%v", zone, ok)
	}
	for rank, row := range rows {
		if view.rankSlots[rank] != row.Slot || view.slotRanks[row.Slot] != byte(rank) {
			t.Fatalf("rank/slot mismatch at %d", rank)
		}
		dst, ok := view.AppendRaw(make([]byte, 0, len(row.JSON)), row.Slot, row.Key)
		if !ok || !bytes.Equal(dst, row.JSON) {
			t.Fatalf("row %d splice mismatch", rank)
		}
		_, ti, ok := view.row(rank)
		if !ok {
			t.Fatalf("row %d lookup", rank)
		}
		batched, before, after := view.appendRawBatched(dst[:0], rank, ti, true)
		if !bytes.Equal(batched, row.JSON) || after > before {
			t.Fatalf("row %d batched splice mismatch; segments %d -> %d", rank, before, after)
		}
		overlay := view.appendRawSkeletonFirst(dst[:0], rank, ti)
		if !bytes.Equal(overlay, row.JSON) {
			t.Fatalf("row %d skeleton-first splice mismatch", rank)
		}
		field, kind, ok := view.Field(row.Slot, 2)
		if !ok || kind != document.Number ||
			!bytes.Equal(field, []byte(fmt.Sprintf("%d", rank%1000))) {
			t.Fatalf("row %d field=%q kind=%v ok=%v", rank, field, kind, ok)
		}
	}
	if _, ok := view.AppendRaw(nil, rows[0].Slot, []byte("wrong")); ok {
		t.Fatal("key confirmation accepted wrong key")
	}
}

func TestTemplateColumnarLeafRegionCorruptionFailClosed(t *testing.T) {
	rows := templateColumnarLeafRows(t, 32, false)
	image, err := EncodeTemplateColumnarLeaf(rows)
	if err != nil {
		t.Fatal(err)
	}
	view, err := OpenTemplateColumnarLeaf(image)
	if err != nil {
		t.Fatal(err)
	}
	regions := [][2]int{
		{0, int(view.metadataEnd)},
		{int(view.metadataEnd), int(view.dictEnd)},
		{int(view.dictEnd), int(view.dataEnd)},
		{int(view.dataEnd), len(image)},
	}
	for i, region := range regions {
		if region[1] <= region[0] {
			continue
		}
		bad := append([]byte(nil), image...)
		bad[region[0]+(region[1]-region[0])/2] ^= 0x40
		if _, err := OpenTemplateColumnarLeaf(bad); !errors.Is(err, ErrTemplateColumnarLeafCorrupt) {
			t.Fatalf("region %d accepted: %v", i, err)
		}
	}
}

func TestTemplateColumnarLeafRejectsGraftedDictionary(t *testing.T) {
	a, err := EncodeTemplateColumnarLeaf(templateColumnarLeafRows(t, 16, false))
	if err != nil {
		t.Fatal(err)
	}
	otherRows := templateColumnarLeafRows(t, 16, false)
	for i := range otherRows {
		// Same byte length, different invariant key spelling.
		otherRows[i].JSON = bytes.Replace(otherRows[i].JSON, []byte(`"name"`), []byte(`"nAme"`), 1)
	}
	b, err := EncodeTemplateColumnarLeaf(otherRows)
	if err != nil {
		t.Fatal(err)
	}
	av, _ := OpenTemplateColumnarLeaf(a)
	bv, _ := OpenTemplateColumnarLeaf(b)
	if av.dictEnd-av.metadataEnd != bv.dictEnd-bv.metadataEnd {
		t.Fatal("graft fixture dictionary sizes differ")
	}
	graft := append([]byte(nil), a...)
	copy(graft[av.metadataEnd:av.dictEnd], b[bv.metadataEnd:bv.dictEnd])
	// Also graft the dictionary checksum. The checksum root still binds the
	// checksum vector to this leaf, and must reject the pair.
	copy(graft[av.dataEnd+4:av.dataEnd+8], b[bv.dataEnd+4:bv.dataEnd+8])
	if _, err := OpenTemplateColumnarLeaf(graft); !errors.Is(err, ErrTemplateColumnarLeafCorrupt) {
		t.Fatalf("grafted dictionary accepted: %v", err)
	}
}

func TestTemplateColumnarLeafRejectsProgramBoundsAndLengthOverflow(t *testing.T) {
	rows := templateColumnarLeafRows(t, 16, false)
	image, err := EncodeTemplateColumnarLeaf(rows)
	if err != nil {
		t.Fatal(err)
	}
	view, _ := OpenTemplateColumnarLeaf(image)

	badProgram := append([]byte(nil), image...)
	tplAt := int(uintptr(unsafe.Pointer(&view.tplDir[0])) - uintptr(unsafe.Pointer(&view.image[0])))
	binary.LittleEndian.PutUint32(badProgram[tplAt+52:tplAt+56], math.MaxUint32)
	if err := ResealTemplateColumnarLeaf(badProgram); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenTemplateColumnarLeaf(badProgram); !errors.Is(err, ErrTemplateColumnarLeafCorrupt) {
		t.Fatalf("program bounds accepted: %v", err)
	}

	badLength := append([]byte(nil), image...)
	rowAt := 0
	lengthStart := int(binary.LittleEndian.Uint32(view.rowDir[rowAt+12:]))
	badLength[lengthStart] = 0xfe
	if err := ResealTemplateColumnarLeaf(badLength); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenTemplateColumnarLeaf(badLength); !errors.Is(err, ErrTemplateColumnarLeafCorrupt) {
		t.Fatalf("length overflow accepted: %v", err)
	}
}

func TestTemplateColumnarLeafPredicateSplicesSurvivorsOnly(t *testing.T) {
	rows := templateColumnarLeafRows(t, 32, false)
	image, err := EncodeTemplateColumnarLeaf(rows)
	if err != nil {
		t.Fatal(err)
	}
	view, err := OpenTemplateColumnarLeaf(image)
	if err != nil {
		t.Fatal(err)
	}
	dst, survivors := view.AppendEqualRaw(nil, 0, 3, []byte("true"))
	if survivors != 16 || len(dst) == 0 {
		t.Fatalf("survivors=%d bytes=%d", survivors, len(dst))
	}
}

func TestTemplateColumnarLeafPatchAndReseal(t *testing.T) {
	rows := templateColumnarLeafRows(t, 64, false)
	image, err := EncodeTemplateColumnarLeaf(rows)
	if err != nil {
		t.Fatal(err)
	}
	slot := rows[10].Slot
	if err := PatchTemplateColumnarLeafFieldFixed(image, slot, 2, []byte("11")); err != nil {
		t.Fatal(err)
	}
	view, err := OpenTemplateColumnarLeaf(image)
	if err != nil {
		t.Fatal(err)
	}
	field, _, ok := view.Field(slot, 2)
	if !ok || string(field) != "11" {
		t.Fatalf("patched field=%q ok=%v", field, ok)
	}
	if err := PatchTemplateColumnarLeafFieldFixed(image, slot, 2, []byte("longer")); !errors.Is(err, ErrTemplateColumnarLeafShape) {
		t.Fatalf("width-changing patch err=%v", err)
	}
}

func TestTemplateColumnarLeafMeasuredRawFallback(t *testing.T) {
	plan, _, err := PlanTemplateColumnarLeaf(
		templateColumnarLeafRows(t, 64, true),
	)
	if err != nil {
		t.Fatal(err)
	}
	if plan.UseTemplate || plan.SelectedBytes != plan.RawBytes ||
		plan.TemplateBytes <= plan.RawBytes {
		t.Fatalf("unique-shape plan=%+v", plan)
	}
}

func TestTemplateColumnarLeafHotPathsZeroAlloc(t *testing.T) {
	rows := templateColumnarLeafRows(t, 190, false)
	image, err := EncodeTemplateColumnarLeaf(rows)
	if err != nil {
		t.Fatal(err)
	}
	view, err := OpenTemplateColumnarLeaf(image)
	if err != nil {
		t.Fatal(err)
	}
	if allocs := testing.AllocsPerRun(1000, func() {
		_, openErr := OpenTemplateColumnarLeaf(image)
		if openErr != nil {
			panic(openErr)
		}
	}); allocs != 0 {
		t.Fatalf("Open allocations=%f", allocs)
	}
	target := rows[95]
	dst := make([]byte, 0, len(target.JSON))
	rank := int(view.slotRanks[target.Slot])
	_, ti, ok := view.row(rank)
	if !ok {
		t.Fatal("target row lookup")
	}
	if allocs := testing.AllocsPerRun(1000, func() {
		out, ok := view.AppendRaw(dst[:0], target.Slot, target.Key)
		if !ok || len(out) == 0 {
			panic("splice")
		}
	}); allocs != 0 {
		t.Fatalf("AppendRaw allocations=%f", allocs)
	}
	if allocs := testing.AllocsPerRun(1000, func() {
		out, _, _ := view.appendRawBatched(dst[:0], rank, ti, true)
		if !bytes.Equal(out, target.JSON) {
			panic("batched splice")
		}
	}); allocs != 0 {
		t.Fatalf("batched AppendRaw allocations=%f", allocs)
	}
	if allocs := testing.AllocsPerRun(1000, func() {
		out := view.appendRawSkeletonFirst(dst[:0], rank, ti)
		if !bytes.Equal(out, target.JSON) {
			panic("skeleton-first splice")
		}
	}); allocs != 0 {
		t.Fatalf("skeleton-first AppendRaw allocations=%f", allocs)
	}
	if allocs := testing.AllocsPerRun(1000, func() {
		field, _, ok := view.Field(target.Slot, 2)
		if !ok || len(field) == 0 {
			panic("field")
		}
	}); allocs != 0 {
		t.Fatalf("Field allocations=%f", allocs)
	}
}

func FuzzTemplateColumnarLeafLengthVectors(f *testing.F) {
	rows := templateColumnarLeafRows(f, 8, false)
	image, err := EncodeTemplateColumnarLeaf(rows)
	if err != nil {
		f.Fatal(err)
	}
	view, _ := OpenTemplateColumnarLeaf(image)
	at := 0 * templateColumnarLeafRowBytes
	start := int(binary.LittleEndian.Uint32(view.rowDir[at+12:]))
	f.Add(uint16(0), uint32(0))
	f.Add(uint16(5), uint32(len(image)))
	f.Fuzz(func(t *testing.T, ordinal uint16, value uint32) {
		bad := append([]byte(nil), image...)
		lengthEnd := int(binary.LittleEndian.Uint32(view.rowDir[at+16:]))
		index := int(ordinal) % max(1, lengthEnd-start)
		if start+index < int(view.dataEnd) {
			bad[start+index] = byte(value)
		}
		// Deliberately reseal to force length-vector validation rather than
		// letting the data-region checksum reject first.
		if err := ResealTemplateColumnarLeaf(bad); err != nil {
			return
		}
		opened, err := OpenTemplateColumnarLeaf(bad)
		if err == nil {
			dst := make([]byte, 0, 256)
			_, _ = opened.AppendRaw(dst, rows[0].Slot, rows[0].Key)
			_, _, _ = opened.Field(rows[0].Slot, 0)
		}
	})
}
