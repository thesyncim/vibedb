package storeio

import (
	"bytes"
	"compress/flate"
	"testing"

	"github.com/thesyncim/vibedb/internal/benchcorpus"
)

// TestUnifiedCompetitiveSpaceBreakdown makes the class-5 space target
// actionable. The end-to-end durable census reports physical bytes per row;
// this companion splits the leaf extents into the exact categories a format
// change can affect, so page-envelope overhead or extent slack cannot be
// mistaken for token payload and vice versa.
func TestUnifiedCompetitiveSpaceBreakdown(t *testing.T) {
	if testing.Short() {
		t.Skip("space breakdown plans the 100k competitive corpus")
	}
	const rows = 100_000
	corpus := benchcorpus.Corpus(rows, false)
	records := make([]CommonPrimaryLeafRecord, len(corpus))
	for i := range corpus {
		records[i] = CommonPrimaryLeafRecord{
			Key: []byte(corpus[i].Key),
			Value: CommonPrimaryLeafValue{
				Inline: corpus[i].JSON,
			},
		}
	}

	type totals struct {
		extents, structural int
		keys, rows          int
		templates, dict     int
		slack, leaves       int
		flateFast           int
		flateDense          int
	}
	var total totals
	var tokenBytes [32]int
	var tokenValues [32]int
	rowTags := 0
	builder := NewUnifiedPrimaryLeafBuilder()
	seed := unifiedTestStoreID()
	for len(records) != 0 {
		window := records
		if len(window) > CommonPrimaryLeafWideSlots {
			window = window[:CommonPrimaryLeafWideSlots]
		}
		count, extent, err := planUnifiedLeaf(builder, seed, window)
		if err != nil {
			t.Fatal(err)
		}
		plan, err := builder.planPrefix(count)
		if err != nil {
			t.Fatal(err)
		}
		keyBytes := 0
		for i := 0; i < count; i++ {
			keyBytes += len(window[i].Key)
			if len(window[i].Key) >= commonPrimaryLeafEscapeLength {
				keyBytes++
			}
		}
		structural := CommonPrimaryLeafStructuralBytes(
			CommonPrimaryLeafUnified, count, extent,
		)
		used := structural + plan.templateBytes + plan.dictionaryBytes + plan.heapBytes
		if used > extent || keyBytes > plan.heapBytes {
			t.Fatalf("invalid breakdown: extent=%d used=%d keys=%d heap=%d",
				extent, used, keyBytes, plan.heapBytes)
		}
		for i := 0; i < count; i++ {
			row := &builder.rows[i]
			if row.shape < 0 || plan.templated[row.shape] < 0 {
				continue
			}
			rowTags++
			canonical := builder.canonicalOf(i)
			for hole, span := range builder.spans[row.spanStart:row.spanEnd] {
				value := canonical[span.Start:span.End]
				cost := unifiedTypedTokenCost(value)
				if _, found := builder.dictionaryID[string(value)]; found {
					cost = 1
				}
				tokenBytes[hole] += cost
				tokenValues[hole]++
			}
		}
		page, err := EncodeCommonPrimaryUnifiedLeaf(
			make([]byte, extent),
			CommonPrimaryLeafHeader{
				StoreID: seed, Generation: 1, Bucket: 0,
				PageSize: uint32(extent),
			},
			seed, window[:count], unifiedTestBounds(), builder,
		)
		if err != nil {
			t.Fatal(err)
		}
		total.flateFast += flateSize(t, page, flate.BestSpeed)
		total.flateDense += flateSize(t, page, flate.BestCompression)
		total.extents += extent
		total.structural += structural
		total.keys += keyBytes
		total.rows += plan.heapBytes - keyBytes
		total.templates += plan.templateBytes
		total.dict += plan.dictionaryBytes
		total.slack += extent - used
		total.leaves++
		records = records[count:]
	}

	perRow := func(n int) float64 { return float64(n) / rows }
	t.Logf("leaves=%d physical=%.2f B/doc structural=%.2f keys=%.2f rows=%.2f templates=%.2f dictionary=%.2f slack=%.2f",
		total.leaves, perRow(total.extents), perRow(total.structural),
		perRow(total.keys), perRow(total.rows), perRow(total.templates),
		perRow(total.dict), perRow(total.slack))
	t.Logf("independent-leaf DEFLATE feasibility bound: fastest=%.2f B/doc densest=%.2f B/doc",
		perRow(total.flateFast), perRow(total.flateDense))
	t.Logf("templated row tags=%.2f B/doc", perRow(rowTags))
	for hole := range tokenBytes {
		if tokenValues[hole] != 0 {
			t.Logf("hole[%d] values=%d bytes=%.2f B/doc average=%.2f B/value",
				hole, tokenValues[hole], perRow(tokenBytes[hole]),
				float64(tokenBytes[hole])/float64(tokenValues[hole]))
		}
	}
	if total.structural+total.keys+total.rows+total.templates+total.dict+total.slack != total.extents {
		t.Fatal("space categories do not sum to physical leaf extents")
	}
}

func flateSize(t testing.TB, src []byte, level int) int {
	t.Helper()
	var dst bytes.Buffer
	w, err := flate.NewWriter(&dst, level)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(src); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return dst.Len()
}
