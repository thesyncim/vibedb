package storeio

import (
	"bytes"
	"fmt"
	"strconv"
	"testing"

	"github.com/thesyncim/vibedb/internal/benchcorpus"
	"github.com/thesyncim/vibejson"
)

// This harness uses only pre-existing APIs so CI can compile the identical
// source against both revisions. Inputs and replacements are made off-clock.
func compactRankBenchRecords(name string) []CommonPrimaryLeafRecord {
	count := 4096
	if name == "high" || name == "unrelated" {
		count = 512
	}
	records := make([]CommonPrimaryLeafRecord, count)
	if name == "low" || name == "high" {
		for i, doc := range benchcorpus.Corpus(count, name == "high") {
			records[i] = CommonPrimaryLeafRecord{Key: []byte(doc.Key), Value: CommonPrimaryLeafValue{Inline: doc.JSON}}
		}
		return records
	}
	for row := range records {
		number := int64(10000 + row*7)
		if name == "late-miss" && row >= count-11 {
			number += 17
		}
		if name == "negative" {
			number = -number
		}
		if name == "unrelated" {
			number = int64(((uint64(row+17) * 0x9e3779b97f4a7c15) >> 1) % 1_000_000)
		}
		raw := fmt.Appendf(nil, `{"id":%d,"name":"item-%d","score":%d`, number, number, row%71)
		if name != "single" && row%7 < 3 {
			raw = append(raw, `,"extra":true`...)
		}
		records[row] = CommonPrimaryLeafRecord{Key: fmt.Appendf(nil, "row-%04d", row), Value: CommonPrimaryLeafValue{Inline: append(raw, '}')}}
	}
	return records
}

func BenchmarkCompactRankFormat(b *testing.B) {
	for _, name := range []string{"low", "high", "late-miss", "negative", "unrelated", "single"} {
		b.Run(name, func(b *testing.B) {
			records := compactRankBenchRecords(name)
			view := compactProjectionTestView(b, records)
			b.Run("Build", func(b *testing.B) {
				builder := NewUnifiedPrimaryLeafBuilder()
				payload, err := BuildCompactPrimaryStripePayload(records, builder)
				if err != nil {
					b.Fatal(err)
				}
				payloadBytes := len(payload)
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					payload, err = BuildCompactPrimaryStripePayload(records, builder)
					if err != nil {
						b.Fatal(err)
					}
				}
				b.ReportMetric(float64(payloadBytes), "payload-B")
				b.ReportMetric(float64((payloadBytes+PageHeaderSize+PageTrailerSize+4095)&^4095), "leaf-B")
			})
			b.Run("Point", func(b *testing.B) {
				scratch := make([]byte, 0, 512)
				row := len(records)*3/4 + 37
				_, _ = view.AppendValue(scratch[:0], row)
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					var ok bool
					scratch, ok = view.AppendValue(scratch[:0], row)
					if !ok {
						b.Fatal("point")
					}
				}
			})
			b.Run("Scan", func(b *testing.B) {
				scratch := make([]byte, 0, 512)
				ordinals := make([]int, view.shapeCount)
				var decoder CompactPrimaryScanDecoder
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					clear(ordinals)
					for row := range records {
						shape := view.rowShape(row)
						var ok bool
						scratch, ok = decoder.appendValue(scratch[:0], &view, view.header.Bucket, row, shape, ordinals[shape])
						if !ok {
							b.Fatal("scan")
						}
						ordinals[shape]++
					}
				}
			})
			for _, field := range []string{"id", "score"} {
				b.Run("Patch-"+field, func(b *testing.B) {
					row := len(records) / 3
					canonical, err := vibejson.AppendCanonicalize(nil, records[row].Value.Inline)
					if err != nil {
						b.Fatal(err)
					}
					prefix := []byte(`"` + field + `":`)
					start := bytes.Index(canonical, prefix) + len(prefix)
					end := start
					for end < len(canonical) && (canonical[end] == '-' || canonical[end] >= '0' && canonical[end] <= '9') {
						end++
					}
					value, err := strconv.ParseInt(string(canonical[start:end]), 10, 64)
					if err != nil {
						b.Fatal(err)
					}
					replacement := append([]byte(nil), canonical[:start]...)
					replacement = strconv.AppendInt(replacement, value+1, 10)
					replacement = append(replacement, canonical[end:]...)
					certificate := unifiedScalarCanonicalIndex(b, replacement)
					scalar, _, supported, err := view.PatchStableCanonicalReplacementScalarPatch(records[row].Key, 0, certificate, make([]byte, 0, 512))
					if err != nil || !supported || !scalar.valid() {
						b.Fatalf("patch admission: %v %v", supported, err)
					}
					replacements := []CommonPrimaryUnifiedReplacement{{Key: records[row].Key, Value: replacement, ScalarPatch: scalar}}
					builder := NewUnifiedPrimaryLeafBuilder()
					dst := make([]byte, CommonPrimaryLeafMaxExtentBytes)
					if _, ok, err := view.PatchCompactPrimaryStripeReplacements(dst, 2, replacements, builder); err != nil || !ok {
						b.Fatalf("patch warm: %v %v", ok, err)
					}
					b.ReportAllocs()
					b.ResetTimer()
					for b.Loop() {
						if _, ok, err := view.PatchCompactPrimaryStripeReplacements(dst, 2, replacements, builder); err != nil || !ok {
							b.Fatalf("patch: %v %v", ok, err)
						}
					}
				})
			}
		})
	}
}
