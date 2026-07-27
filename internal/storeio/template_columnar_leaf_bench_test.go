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
//   go test ./internal/storeio -run '^$' -bench '^BenchmarkTemplateColumnarLeafLab' -benchmem -benchtime=250ms -count=5
//
// Gates are metrics, not test assertions: low/high saving >=40%/>=25%;
// selected adversarial overhead <=2%; splice <=60ns for competitive reads
// (<=30ns reopens default-path use); all-byte scan <=60ns/doc; field <=25ns;
// fused extraction increment <=15%. Results belong in the phase report rather
// than a stale checked-in verdict.

var (
	templateColumnarLeafLabBenchBytes []byte
	templateColumnarLeafLabBenchKind  uint8
	templateColumnarLeafLabBenchBool  bool
	templateColumnarLeafLabBenchView  TemplateColumnarLeafLabView
	templateColumnarLeafLabBenchExt   TemplateColumnarLeafLabExtraction
)

func templateColumnarLeafLabCompetitiveRows(
	count int, high, uniqueShape bool,
) []TemplateColumnarLeafLabRow {
	rng := rand.New(rand.NewSource(0x5deece66d))
	rows := make([]TemplateColumnarLeafLabRow, count)
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
		rows[i] = TemplateColumnarLeafLabRow{
			Slot: uint8((i*197 + 17) & 255),
			Key:  []byte(fmt.Sprintf("doc:%08d", i)),
			JSON: json,
		}
	}
	return rows
}

func BenchmarkTemplateColumnarLeafLabSpace(b *testing.B) {
	for _, tc := range []struct {
		name         string
		high, unique bool
	}{
		{name: "CompetitiveLow"},
		{name: "CompetitiveHigh", high: true},
		{name: "AdversarialUniqueShape", high: true, unique: true},
	} {
		rows := templateColumnarLeafLabCompetitiveRows(190, tc.high, tc.unique)
		image, err := EncodeTemplateColumnarLeafLab(rows)
		if err != nil {
			b.Fatal(err)
		}
		raw := TemplateColumnarLeafLabRawBytes(rows)
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

func BenchmarkTemplateColumnarLeafLabAppendRaw(b *testing.B) {
	rows := templateColumnarLeafLabCompetitiveRows(190, true, false)
	image, err := EncodeTemplateColumnarLeafLab(rows)
	if err != nil {
		b.Fatal(err)
	}
	view, err := OpenTemplateColumnarLeafLab(image)
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
			templateColumnarLeafLabBenchBytes,
				templateColumnarLeafLabBenchBool = view.AppendRaw(dst[:0], row.Slot, row.Key)
		}
	})
	b.Run("BatchedResolution", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			templateColumnarLeafLabBenchBytes, _, _ =
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
			templateColumnarLeafLabBenchBytes, _, _ =
				view.appendRawBatched(dst[:0], rank, ti, true)
		}
		b.ReportMetric(float64(before), "segments-before/splice")
		b.ReportMetric(float64(after), "segments-after/splice")
	})
	b.Run("SkeletonFirstOverlay", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			templateColumnarLeafLabBenchBytes =
				view.appendRawSkeletonFirst(dst[:0], rank, ti)
		}
	})
	b.Run("RawLeafCopy", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			templateColumnarLeafLabBenchBytes = append(dst[:0], row.JSON...)
		}
	})
}

func BenchmarkTemplateColumnarLeafLabField(b *testing.B) {
	rows := templateColumnarLeafLabCompetitiveRows(190, true, false)
	image, err := EncodeTemplateColumnarLeafLab(rows)
	if err != nil {
		b.Fatal(err)
	}
	view, err := OpenTemplateColumnarLeafLab(image)
	if err != nil {
		b.Fatal(err)
	}
	slot := rows[95].Slot
	b.ReportAllocs()
	for b.Loop() {
		var kind document.Kind
		templateColumnarLeafLabBenchBytes, kind,
			templateColumnarLeafLabBenchBool = view.Field(slot, 3)
		templateColumnarLeafLabBenchKind = uint8(kind)
	}
}

func BenchmarkTemplateColumnarLeafLabExtraction(b *testing.B) {
	row := templateColumnarLeafLabCompetitiveRows(1, true, false)[0]
	storage := make([]vibejson.IndexEntry, len(row.JSON)+2)
	scratch := TemplateColumnarLeafLabExtraction{
		Skeleton: make([]byte, 0, len(row.JSON)),
		Holes:    make([]TemplateColumnarLeafLabHole, 0, 32),
		Fields:   make([][]byte, 0, 32),
		Runs:     make([]templateColumnarLeafLabRun, 0, 33),
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
			templateColumnarLeafLabBenchExt, err =
				ExtractTemplateColumnarLeafLabInto(row.JSON, storage, &scratch)
			if err != nil {
				b.Fatal(err)
			}
			scratch = templateColumnarLeafLabBenchExt
		}
	})
}

func BenchmarkTemplateColumnarLeafLabScan(b *testing.B) {
	rows := templateColumnarLeafLabCompetitiveRows(190, true, false)
	image, err := EncodeTemplateColumnarLeafLab(rows)
	if err != nil {
		b.Fatal(err)
	}
	view, err := OpenTemplateColumnarLeafLab(image)
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
				out, templateColumnarLeafLabBenchBool =
					view.AppendRaw(out, view.rankSlots[rank], key)
			}
			templateColumnarLeafLabBenchBytes = out
		}
	})
	b.Run("PredicateSurvivors", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			var survivors int
			templateColumnarLeafLabBenchBytes, survivors =
				view.AppendEqualRaw(dst[:0], 0, 4, []byte("true"))
			templateColumnarLeafLabBenchKind = uint8(survivors)
		}
	})
}

func BenchmarkTemplateColumnarLeafLabLengthMetadata(b *testing.B) {
	rows := templateColumnarLeafLabCompetitiveRows(190, true, false)
	image, err := EncodeTemplateColumnarLeafLab(rows)
	if err != nil {
		b.Fatal(err)
	}
	view, err := OpenTemplateColumnarLeafLab(image)
	if err != nil {
		b.Fatal(err)
	}
	before, after := view.MetadataBytesPerDocument()
	b.ReportMetric(before, "offset-dir-B/doc")
	b.ReportMetric(after, "length-vector-B/doc")
}

func BenchmarkTemplateColumnarLeafLabReseal(b *testing.B) {
	rows := templateColumnarLeafLabCompetitiveRows(190, false, false)
	original, err := EncodeTemplateColumnarLeafLab(rows)
	if err != nil {
		b.Fatal(err)
	}
	slot := rows[10].Slot // score is two bytes and replacement preserves width.
	b.Run("FieldPatchRegionAndRoot", func(b *testing.B) {
		image := append([]byte(nil), original...)
		view, err := OpenTemplateColumnarLeafLab(image)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.SetBytes(2)
		for b.Loop() {
			if err := patchTemplateColumnarLeafLabFieldFixedAdmitted(
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
			if err := ResealTemplateColumnarLeafLab(image); err != nil {
				b.Fatal(err)
			}
		}
	})
}
