package storeio

import (
	"fmt"
	"strconv"
	"testing"
)

const (
	compactAdmissionRows    = 300
	compactAdmissionPayload = 256
)

// compactAdmissionVariedPayload mirrors the frozen varied-v1 SQL client. The
// quote framing is the JSON scalar representation handed to the leaf builder.
func compactAdmissionVariedPayload(dst []byte, row uint64) []byte {
	const alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz-_"
	var payload [compactAdmissionPayload]byte
	x := compactAdmissionMix(row + 0x1d2b79f5aa33cc77)
	for at := range payload {
		x ^= x >> 12
		x ^= x << 25
		x ^= x >> 27
		x *= 0x2545f4914f6cdd1d
		payload[at] = alphabet[(x>>58)&63]
	}
	dst = append(dst, '"')
	dst = append(dst, payload[:]...)
	return append(dst, '"')
}

func compactAdmissionMix(value uint64) uint64 {
	value += 0x9e3779b97f4a7c15
	value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9
	value = (value ^ (value >> 27)) * 0x94d049bb133111eb
	return value ^ (value >> 31)
}

func compactPrimaryFullLeafAdmissionFixture(t testing.TB) (
	page []byte,
	storeID [16]byte,
	expected PageRef,
	bounds CommonPrimaryLeafBounds,
	rows int,
) {
	t.Helper()
	records := make([]CommonPrimaryLeafRecord, compactAdmissionRows)
	for row := range records {
		key := fmt.Appendf(nil, "row-%020d", row)
		document := make([]byte, 0, compactAdmissionPayload+64)
		document = append(document, `{"id":`...)
		document = strconv.AppendUint(document, uint64(row), 10)
		document = append(document, `,"value":`...)
		document = compactAdmissionVariedPayload(document, uint64(row))
		document = append(document, '}')
		records[row] = CommonPrimaryLeafRecord{
			Key: key, Value: CommonPrimaryLeafValue{Inline: document},
		}
	}

	builder := NewUnifiedPrimaryLeafBuilder()
	payload, err := BuildCompactPrimaryStripePayload(records, builder)
	if err != nil {
		t.Fatal(err)
	}
	extent := int(physicalPageQuantum)
	for extent < PageHeaderSize+len(payload)+PageTrailerSize {
		extent <<= 1
	}
	if extent != CommonPrimaryLeafMaxExtentBytes {
		t.Fatalf("admission fixture extent=%d payload=%d, want full %d-byte leaf",
			extent, len(payload), CommonPrimaryLeafMaxExtentBytes)
	}
	storeID = unifiedTestStoreID()
	page, err = EncodeCompactPrimaryStripe(
		make([]byte, extent),
		CommonPrimaryLeafHeader{
			StoreID: storeID, Generation: 1, Bucket: 0, PageSize: uint32(extent),
		}, records, builder,
	)
	if err != nil {
		t.Fatal(err)
	}
	logicalID, ok := CommonPrimaryLeafLogicalID(0)
	if !ok {
		t.Fatal("admission fixture logical id")
	}
	expected = PageRef{
		Offset: 4096, Length: uint32(extent), LogicalID: logicalID,
		Generation: 1, Kind: PagePrimaryLeaf,
	}
	bounds = unifiedTestBounds()
	view, err := OpenCompactPrimaryStripe(page, storeID, 0, expected, 1, bounds)
	if err != nil {
		t.Fatal(err)
	}
	if view.Len() != compactAdmissionRows {
		t.Fatalf("admission fixture rows=%d, want %d", view.Len(), compactAdmissionRows)
	}
	entry, ok := view.shapeEntry(0)
	if !ok {
		t.Fatal("admission fixture shape")
	}
	streamRaw := entry.streamRaw
	var foundAlphabet bool
	for hole := 0; hole < entry.template.holes; hole++ {
		stream, streamErr := openCompactStream(streamRaw)
		if streamErr != nil {
			t.Fatal(streamErr)
		}
		if stream.kind == compactStreamAlphabet {
			alphabet, _, _, partsOK := stream.alphabetParts()
			if !partsOK || stream.width != 6 || len(alphabet) != 64 {
				t.Fatalf("admission alphabet kind=%d width=%d cardinality=%d",
					stream.kind, stream.width, len(alphabet))
			}
			foundAlphabet = true
		}
		streamRaw = streamRaw[stream.encoded:]
	}
	if !foundAlphabet {
		t.Fatal("admission fixture did not select the full-domain alphabet stream")
	}
	return page, storeID, expected, bounds, compactAdmissionRows
}

// BenchmarkCompactPrimaryFullLeafAdmission measures complete validation of a
// realistic full physical leaf. Setup and the first admission stay outside the
// timer; each timed iteration repeats OpenCompactPrimaryStripe on the same
// sealed bytes and therefore includes page identity, checksum, shape, key, and
// compact stream grammar validation.
func BenchmarkCompactPrimaryFullLeafAdmission(b *testing.B) {
	page, storeID, expected, bounds, rows := compactPrimaryFullLeafAdmissionFixture(b)
	b.ReportAllocs()
	b.ReportMetric(float64(rows), "rows/admission")
	b.ReportMetric(float64(len(page)), "leaf-B")
	b.ResetTimer()
	for range b.N {
		view, err := OpenCompactPrimaryStripe(page, storeID, 0, expected, 1, bounds)
		if err != nil || view.Len() != rows {
			b.Fatalf("full-leaf admission rows=%d err=%v", view.Len(), err)
		}
	}
}
