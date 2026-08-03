package storeio

import (
	"bytes"
	"compress/flate"
	"encoding/binary"
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

func TestCompactCompetitiveSpaceBreakdown(t *testing.T) {
	if testing.Short() {
		t.Skip("space breakdown plans both 100k competitive corpora")
	}
	const rows = 100_000
	kindName := func(kind uint8) string {
		switch kind {
		case compactStreamDictionary:
			return "dictionary"
		case compactStreamFront:
			return "front"
		case compactStreamFOR:
			return "for"
		case compactStreamDelta:
			return "delta"
		case compactStreamDate:
			return "date"
		case compactStreamPrefixInt:
			return "prefix-int"
		case compactStreamDeltaPack:
			return "delta-pack"
		case compactStreamAlphabet:
			return "alphabet"
		default:
			return "unknown"
		}
	}
	for _, high := range []bool{false, true} {
		name := "low"
		if high {
			name = "high"
		}
		t.Run(name, func(t *testing.T) {
			corpus := benchcorpus.Corpus(rows, high)
			graph := make([]PrimaryGraphRecord, len(corpus))
			for i := range corpus {
				graph[i] = PrimaryGraphRecord{
					Key: []byte(corpus[i].Key), Value: corpus[i].JSON,
				}
			}
			seed := unifiedTestStoreID()
			plans, err := planCompactPrimaryLeaves(
				seed, graph, CompactPrimaryStripeMaxRows,
				CommonPrimaryLeafMaxExtentBytes,
			)
			if err != nil {
				t.Fatal(err)
			}
			builder := NewUnifiedPrimaryLeafBuilder()
			window := make([]CommonPrimaryLeafRecord, 0, CompactPrimaryStripeMaxRows)
			type totals struct {
				extent, payload, pageFrame, slack int
				primaryHeader, shapeDir, keys     int
				slots, shapeCodes, ranks          int
				shapeHeaders, templates, streams  int
				streamHeaders, dictionary         int
				streamData                        int
				kinds                             [compactStreamKindLimit]int
			}
			var total totals
			for _, plan := range plans {
				window = window[:0]
				for at := plan.first; at < plan.last; at++ {
					window = append(window, CommonPrimaryLeafRecord{
						Key:   graph[at].Key,
						Value: CommonPrimaryLeafValue{Inline: graph[at].Value},
					})
				}
				payload, err := BuildCompactPrimaryStripePayload(window, builder)
				if err != nil {
					t.Fatal(err)
				}
				need := PageHeaderSize + len(payload) + PageTrailerSize
				extent := (need + int(physicalPageQuantum) - 1) &^
					(int(physicalPageQuantum) - 1)
				shapeCount := int(binary.LittleEndian.Uint16(payload[8:]))
				keyBytes := int(binary.LittleEndian.Uint32(payload[12:]))
				shapeCodeBytes := int(binary.LittleEndian.Uint32(payload[16:]))
				rankBytes := int(binary.LittleEndian.Uint32(payload[20:]))
				shapeBytes := int(binary.LittleEndian.Uint32(payload[24:]))
				slotBytes := int(binary.LittleEndian.Uint32(payload[28:]))
				shapeDirBytes := 4 * shapeCount

				total.extent += extent
				total.payload += len(payload)
				total.pageFrame += PageHeaderSize + PageTrailerSize
				total.slack += extent - need
				total.primaryHeader += compactPrimaryHeaderBytes
				total.shapeDir += shapeDirBytes
				total.keys += keyBytes
				total.slots += slotBytes
				total.shapeCodes += shapeCodeBytes
				total.ranks += rankBytes

				shapeStart := compactPrimaryHeaderBytes + shapeDirBytes +
					keyBytes + slotBytes + shapeCodeBytes + rankBytes
				if shapeStart+shapeBytes != len(payload) {
					t.Fatalf("shape geometry = %d+%d, payload %d",
						shapeStart, shapeBytes, len(payload))
				}
				for shape := range shapeCount {
					rel := 0
					if shape != 0 {
						rel = int(binary.LittleEndian.Uint32(
							payload[compactPrimaryHeaderBytes+(shape-1)*4:],
						))
					}
					entry := shapeStart + rel
					templateBytes := int(binary.LittleEndian.Uint32(payload[entry+8:]))
					streamBytes := int(binary.LittleEndian.Uint32(payload[entry+12:]))
					holes := int(binary.LittleEndian.Uint16(payload[entry+4:]))
					total.shapeHeaders += compactPrimaryShapeHeader
					total.templates += templateBytes
					total.streams += streamBytes
					cursor := entry + compactPrimaryShapeHeader + templateBytes
					for range holes {
						stream, err := openCompactStream(payload[cursor:])
						if err != nil {
							t.Fatal(err)
						}
						total.streamHeaders += compactStreamHeader + len(stream.dictDir)
						total.dictionary += len(stream.dictData)
						total.streamData += len(stream.data)
						total.kinds[stream.kind] += stream.encoded
						cursor += stream.encoded
					}
					if cursor != entry+compactPrimaryShapeHeader+
						templateBytes+streamBytes {
						t.Fatal("stream geometry drift")
					}
				}
			}
			perRow := func(value int) float64 { return float64(value) / rows }
			t.Logf("leaves=%d extent=%.3f payload=%.3f page-frame=%.3f slack=%.3f header=%.3f shape-dir=%.3f keys=%.3f slots=%.3f shape-codes=%.3f ranks=%.3f shape-headers=%.3f templates=%.3f streams=%.3f stream-headers=%.3f dictionaries=%.3f stream-data=%.3f B/doc",
				len(plans), perRow(total.extent), perRow(total.payload),
				perRow(total.pageFrame), perRow(total.slack),
				perRow(total.primaryHeader), perRow(total.shapeDir),
				perRow(total.keys), perRow(total.slots),
				perRow(total.shapeCodes), perRow(total.ranks),
				perRow(total.shapeHeaders), perRow(total.templates),
				perRow(total.streams), perRow(total.streamHeaders),
				perRow(total.dictionary), perRow(total.streamData))
			for kind, bytes := range total.kinds {
				if bytes != 0 {
					t.Logf("codec[%s]=%.3f B/doc", kindName(uint8(kind)), perRow(bytes))
				}
			}
		})
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
