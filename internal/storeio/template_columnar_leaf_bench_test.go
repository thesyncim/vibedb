package storeio

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"

	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/document"
)

// Qualification command:
//   go test ./internal/storeio -run '^$' -bench '^BenchmarkTemplateColumnarLeaf' -benchmem -benchtime=250ms -count=5
//
// Gates are metrics, not test assertions: low/high saving >=40%/>=25%;
// selected adversarial overhead <=2%; splice <=60ns for competitive reads
// (<=30ns reopens default-path use); all-byte scan <=60ns/doc; field <=25ns;
// fused extraction increment <=15%. Results belong in the phase report rather
// than a stale checked-in verdict.

var (
	templateColumnarLeafBenchBytes []byte
	templateColumnarLeafBenchKind  uint8
	templateColumnarLeafBenchBool  bool
	templateColumnarLeafBenchExt   TemplateColumnarLeafExtraction
)

func templateColumnarLeafCompetitiveRows(
	count int, high, uniqueShape bool,
) []TemplateColumnarLeafRow {
	rng := rand.New(rand.NewSource(0x5deece66d))
	rows := make([]TemplateColumnarLeafRow, count)
	notes := []string{
		"steady state, no anomalies observed in the last reporting window",
		"migrated from the legacy pipeline during the maintenance window",
		"flagged for review after a threshold breach on the ingest path",
		"nominal; retention policy applied and checkpoint acknowledged",
	}
	randomText := func(n int) string {
		buf := make([]byte, n)
		for i := range buf {
			buf[i] = byte('a' + rng.Intn(26))
		}
		return string(buf)
	}
	for i := range rows {
		tier, region, note := "team", "eu-west-1", notes[i%len(notes)]
		if high {
			tier, region, note = randomText(len(tier)), randomText(len(region)), randomText(len(note))
		}
		shape := ""
		if uniqueShape {
			shape = fmt.Sprintf(`,"shape_%03d":%d`, i, i)
		}
		json := fmt.Appendf(nil,
			`{"id":%d,"name":"user-%d","country":"%s","score":%d,"active":%t,`+
				`"profile":{"tier":"%s","region":"%s","joined":"2024-07-%02d"},`+
				`"tags":["alpha","beta","gamma"],"note":"%s"%s}`,
			i, i, []string{"PT", "ES", "FR", "DE"}[i&3], i%1000, i%3 != 0,
			tier, region, 1+i%28, note, shape)
		rows[i] = TemplateColumnarLeafRow{
			Slot: uint8((i*197 + 17) & 255),
			Key:  []byte(fmt.Sprintf("doc:%08d", i)),
			JSON: json,
		}
	}
	return rows
}

func BenchmarkTemplateColumnarLeafSpace(b *testing.B) {
	for _, tc := range []struct {
		name         string
		high, unique bool
	}{
		{name: "CompetitiveLow"},
		{name: "CompetitiveHigh", high: true},
		{name: "AdversarialUniqueShape", high: true, unique: true},
	} {
		rows := templateColumnarLeafCompetitiveRows(190, tc.high, tc.unique)
		image, err := EncodeTemplateColumnarLeaf(rows)
		if err != nil {
			b.Fatal(err)
		}
		raw := TemplateColumnarLeafRawBytes(rows)
		selected := min(raw, len(image))
		saving := float64(raw-selected) / float64(raw) * 100
		overhead := float64(selected-raw) / float64(raw) * 100
		if overhead < 0 {
			overhead = 0
		}
		b.Run(tc.name, func(b *testing.B) {
			b.ReportMetric(float64(raw)/float64(len(rows)), "raw-B/doc")
			b.ReportMetric(float64(len(image))/float64(len(rows)), "template-B/doc")
			b.ReportMetric(float64(selected)/float64(len(rows)), "selected-B/doc")
			b.ReportMetric(saving, "selected-saving-%")
			b.ReportMetric(overhead, "selected-overhead-%")
		})
	}
}

func BenchmarkTemplateColumnarLeafAppendRaw(b *testing.B) {
	rows := templateColumnarLeafCompetitiveRows(190, true, false)
	image, err := EncodeTemplateColumnarLeaf(rows)
	if err != nil {
		b.Fatal(err)
	}
	view, err := OpenTemplateColumnarLeaf(image)
	if err != nil {
		b.Fatal(err)
	}
	row := rows[95]
	dst := make([]byte, 0, len(row.JSON))
	rank := int(view.slotRanks[row.Slot])
	_, ti, ok := view.row(rank)
	if !ok {
		b.Fatal("benchmark row")
	}
	b.Run("TemplateSplice", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			templateColumnarLeafBenchBytes,
				templateColumnarLeafBenchBool = view.AppendRaw(dst[:0], row.Slot, row.Key)
		}
	})
	b.Run("BatchedResolution", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			templateColumnarLeafBenchBytes, _, _ =
				view.appendRawBatched(dst[:0], rank, ti, false)
		}
	})
	b.Run("BatchedResolutionRunCoalescing", func(b *testing.B) {
		out, before, after := view.appendRawBatched(dst[:0], rank, ti, true)
		if !bytes.Equal(out, row.JSON) {
			b.Fatal("coalesced splice mismatch")
		}
		b.ReportAllocs()
		for b.Loop() {
			templateColumnarLeafBenchBytes, _, _ =
				view.appendRawBatched(dst[:0], rank, ti, true)
		}
		b.ReportMetric(float64(before), "segments-before/splice")
		b.ReportMetric(float64(after), "segments-after/splice")
	})
	b.Run("SkeletonFirstOverlay", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			templateColumnarLeafBenchBytes =
				view.appendRawSkeletonFirst(dst[:0], rank, ti)
		}
	})
	b.Run("RawLeafCopy", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			templateColumnarLeafBenchBytes = append(dst[:0], row.JSON...)
		}
	})
}

func BenchmarkTemplateColumnarLeafField(b *testing.B) {
	rows := templateColumnarLeafCompetitiveRows(190, true, false)
	image, err := EncodeTemplateColumnarLeaf(rows)
	if err != nil {
		b.Fatal(err)
	}
	view, err := OpenTemplateColumnarLeaf(image)
	if err != nil {
		b.Fatal(err)
	}
	slot := rows[95].Slot
	b.ReportAllocs()
	for b.Loop() {
		var kind document.Kind
		templateColumnarLeafBenchBytes, kind,
			templateColumnarLeafBenchBool = view.Field(slot, 3)
		templateColumnarLeafBenchKind = uint8(kind)
	}
}

func BenchmarkTemplateColumnarLeafExtraction(b *testing.B) {
	row := templateColumnarLeafCompetitiveRows(1, true, false)[0]
	storage := make([]vibejson.IndexEntry, len(row.JSON)+2)
	scratch := TemplateColumnarLeafExtraction{
		Skeleton: make([]byte, 0, len(row.JSON)),
		Holes:    make([]TemplateColumnarLeafHole, 0, 32),
		Fields:   make([][]byte, 0, 32),
		Runs:     make([]templateColumnarLeafRun, 0, 33),
	}
	b.Run("ValidationOnly", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, err := vibejson.BuildIndex(row.JSON, storage); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("FusedExtraction", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			var err error
			templateColumnarLeafBenchExt, err =
				ExtractTemplateColumnarLeafInto(row.JSON, storage, &scratch)
			if err != nil {
				b.Fatal(err)
			}
			scratch = templateColumnarLeafBenchExt
		}
	})
}

func BenchmarkTemplateColumnarLeafScan(b *testing.B) {
	rows := templateColumnarLeafCompetitiveRows(190, true, false)
	image, err := EncodeTemplateColumnarLeaf(rows)
	if err != nil {
		b.Fatal(err)
	}
	view, err := OpenTemplateColumnarLeaf(image)
	if err != nil {
		b.Fatal(err)
	}
	dst := make([]byte, 0, 64<<10)
	b.Run("AllBytes", func(b *testing.B) {
		b.ReportAllocs()
		b.ReportMetric(float64(len(rows)), "docs/op")
		for b.Loop() {
			out := dst[:0]
			for rank := range rows {
				key, _, _ := view.row(rank)
				out, templateColumnarLeafBenchBool =
					view.AppendRaw(out, view.rankSlots[rank], key)
			}
			templateColumnarLeafBenchBytes = out
		}
	})
	b.Run("PredicateSurvivors", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			var survivors int
			templateColumnarLeafBenchBytes, survivors =
				view.AppendEqualRaw(dst[:0], 0, 4, []byte("true"))
			templateColumnarLeafBenchKind = uint8(survivors)
		}
	})
}

func BenchmarkTemplateColumnarLeafLengthMetadata(b *testing.B) {
	rows := templateColumnarLeafCompetitiveRows(190, true, false)
	image, err := EncodeTemplateColumnarLeaf(rows)
	if err != nil {
		b.Fatal(err)
	}
	view, err := OpenTemplateColumnarLeaf(image)
	if err != nil {
		b.Fatal(err)
	}
	before, after := view.MetadataBytesPerDocument()
	b.ReportMetric(before, "offset-dir-B/doc")
	b.ReportMetric(after, "length-vector-B/doc")
}

func BenchmarkTemplateColumnarLeafReseal(b *testing.B) {
	rows := templateColumnarLeafCompetitiveRows(190, false, false)
	original, err := EncodeTemplateColumnarLeaf(rows)
	if err != nil {
		b.Fatal(err)
	}
	slot := rows[10].Slot // score is two bytes and replacement preserves width.
	b.Run("FieldPatchRegionAndRoot", func(b *testing.B) {
		image := append([]byte(nil), original...)
		view, err := OpenTemplateColumnarLeaf(image)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.SetBytes(2)
		for b.Loop() {
			if err := patchTemplateColumnarLeafFieldFixedAdmitted(
				view, slot, 3, []byte("11"),
			); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("WholeLeafReseal", func(b *testing.B) {
		image := append([]byte(nil), original...)
		b.ReportAllocs()
		b.SetBytes(int64(len(image)))
		for b.Loop() {
			if err := ResealTemplateColumnarLeaf(image); err != nil {
				b.Fatal(err)
			}
		}
	})
}
