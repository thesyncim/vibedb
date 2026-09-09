package durable

import (
	"bytes"
	"encoding/binary"
	"path/filepath"
	"testing"

	"github.com/pierrec/lz4/v4"
	"github.com/thesyncim/vibedb/internal/storeio"
)

const (
	exactMixedPackDecodedLimit = 64 << 10
	exactMixedPackHeaderBytes  = 32
	exactMixedPackEntryBytes   = 16
)

type exactMixedPackProjection struct {
	frames              int
	encodedBytes        int64
	rawStoredBytes      int64
	compressedBytes     int64
	rawExtentBytes      int64
	compressedExtents   int64
	compressed4KExtents int64
	rawSlackBytes       int64
	compressedSlack     int64
	compressed4KSlack   int64
	compressedFrameWins int
}

// exactMixedPackOrder interleaves leaves by ordinal across physical indexes.
// This is the deterministic Stage 0 policy being sized: nearby frames contain
// overlapping tuple fields without changing the canonical leaf images.
func exactMixedPackOrder(collection *Collection) []struct {
	index int
	leaf  *primaryExactLeaf
} {
	maximum := 0
	for i := range collection.primaryEpoch.exact {
		maximum = max(maximum, len(collection.primaryEpoch.exact[i].leaves))
	}
	ordered := make([]struct {
		index int
		leaf  *primaryExactLeaf
	}, 0)
	for ordinal := 0; ordinal < maximum; ordinal++ {
		for index := range collection.primaryEpoch.exact {
			if ordinal < len(collection.primaryEpoch.exact[index].leaves) {
				ordered = append(ordered, struct {
					index int
					leaf  *primaryExactLeaf
				}{index: index, leaf: &collection.primaryEpoch.exact[index].leaves[ordinal]})
			}
		}
	}
	return ordered
}

func exactLocalPackOrder(collection *Collection) []struct {
	index int
	leaf  *primaryExactLeaf
} {
	ordered := make([]struct {
		index int
		leaf  *primaryExactLeaf
	}, 0)
	for index := range collection.primaryEpoch.exact {
		for ordinal := range collection.primaryEpoch.exact[index].leaves {
			ordered = append(ordered, struct {
				index int
				leaf  *primaryExactLeaf
			}{index: index, leaf: &collection.primaryEpoch.exact[index].leaves[ordinal]})
		}
	}
	return ordered
}

func exactMixedPackProject(tb testing.TB, collection *Collection, mixIndexes bool) exactMixedPackProjection {
	tb.Helper()
	if collection.primaryEpoch == nil {
		tb.Fatal("mixed-pack projection missing exact epoch")
	}
	ordered := exactLocalPackOrder(collection)
	if mixIndexes {
		ordered = exactMixedPackOrder(collection)
	}
	var projection exactMixedPackProjection
	compressor := new(lz4.Compressor)
	for at := 0; at < len(ordered); {
		end := at
		bodyBytes := 0
		for end < len(ordered) {
			if !mixIndexes && end > at && ordered[end].index != ordered[at].index {
				break
			}
			encodedBytes := len(ordered[end].leaf.encoded)
			if encodedBytes == 0 {
				tb.Fatalf("index %d projected leaf has no canonical image", ordered[end].index)
			}
			candidate := exactMixedPackHeaderBytes +
				(end-at+1)*exactMixedPackEntryBytes + bodyBytes + encodedBytes
			if candidate+storeio.PageHeaderSize+storeio.PageTrailerSize > exactMixedPackDecodedLimit {
				break
			}
			bodyBytes += encodedBytes
			end++
		}
		if end == at {
			tb.Fatalf("index %d leaf bytes=%d cannot fit decoded frame cap=%d",
				ordered[at].index, len(ordered[at].leaf.encoded), exactMixedPackDecodedLimit)
		}
		directoryBytes := (end - at) * exactMixedPackEntryBytes
		body := make([]byte, directoryBytes, directoryBytes+bodyBytes)
		offset := 0
		for row := at; row < end; row++ {
			entry := (row - at) * exactMixedPackEntryBytes
			binary.LittleEndian.PutUint16(body[entry:], uint16(ordered[row].index))
			binary.LittleEndian.PutUint32(body[entry+4:], uint32(offset))
			binary.LittleEndian.PutUint32(body[entry+8:], uint32(len(ordered[row].leaf.encoded)))
			body = append(body, ordered[row].leaf.encoded...)
			offset += len(ordered[row].leaf.encoded)
		}
		rawStored := exactMixedPackHeaderBytes + len(body)
		compressed := make([]byte, lz4.CompressBlockBound(len(body)))
		compressedN, err := compressor.CompressBlock(body, compressed)
		if err != nil {
			tb.Fatal(err)
		}
		compressedStored := rawStored
		if compressedN > 0 && exactMixedPackHeaderBytes+compressedN < rawStored {
			compressedStored = exactMixedPackHeaderBytes + compressedN
			projection.compressedFrameWins++
			decoded := make([]byte, len(body))
			n, decodeErr := lz4.UncompressBlock(compressed[:compressedN], decoded)
			if decodeErr != nil || n != len(body) || !bytes.Equal(decoded, body) {
				tb.Fatalf("projected pack LZ4 roundtrip bytes=%d/%d err=%v", n, len(body), decodeErr)
			}
		}
		// Validate the concrete directory contract independently of compression:
		// every member must identify and reproduce its original canonical leaf.
		for row := at; row < end; row++ {
			entry := (row - at) * exactMixedPackEntryBytes
			index := int(binary.LittleEndian.Uint16(body[entry:]))
			offset := int(binary.LittleEndian.Uint32(body[entry+4:]))
			length := int(binary.LittleEndian.Uint32(body[entry+8:]))
			start := directoryBytes + offset
			if index != ordered[row].index || start < directoryBytes ||
				length != len(ordered[row].leaf.encoded) || start+length > len(body) ||
				!bytes.Equal(body[start:start+length], ordered[row].leaf.encoded) {
				tb.Fatalf("projected pack directory member %d does not match canonical leaf", row-at)
			}
		}
		rawExtent, ok := primaryExactExtent(
			rawStored+storeio.PageHeaderSize+storeio.PageTrailerSize,
			uint32(collection.options.PageSize), uint32(collection.options.MaxPageSize),
		)
		if !ok {
			tb.Fatalf("raw mixed pack stored bytes=%d has no exact extent", rawStored)
		}
		compressedExtent, ok := primaryExactExtent(
			compressedStored+storeio.PageHeaderSize+storeio.PageTrailerSize,
			uint32(collection.options.PageSize), uint32(collection.options.MaxPageSize),
		)
		if !ok {
			tb.Fatalf("compressed mixed pack stored bytes=%d has no exact extent", compressedStored)
		}
		projection.frames++
		projection.encodedBytes += int64(bodyBytes)
		projection.rawStoredBytes += int64(rawStored)
		projection.compressedBytes += int64(compressedStored)
		projection.rawExtentBytes += int64(rawExtent)
		projection.compressedExtents += int64(compressedExtent)
		compressedNeed := compressedStored + storeio.PageHeaderSize + storeio.PageTrailerSize
		compressed4KExtent := (compressedNeed + (4 << 10) - 1) &^ ((4 << 10) - 1)
		projection.compressed4KExtents += int64(compressed4KExtent)
		projection.rawSlackBytes += int64(rawExtent) - int64(rawStored+storeio.PageHeaderSize+storeio.PageTrailerSize)
		projection.compressedSlack += int64(compressedExtent) - int64(compressedStored+storeio.PageHeaderSize+storeio.PageTrailerSize)
		projection.compressed4KSlack += int64(compressed4KExtent - compressedNeed)
		at = end
	}
	return projection
}

// BenchmarkExactMixedPackCheckpointProjection is a storage-only Stage 0
// projection. It does not write packed pages and makes no runtime claim. It
// replaces only current exact-leaf extents in the measured checkpoint size;
// roots, catalogs, primary pages, and fixed metadata remain unchanged.
func BenchmarkExactMixedPackCheckpointProjection(b *testing.B) {
	fixtures := []struct {
		name        string
		cardinality int
		sharedBytes int
	}{
		{name: "long/card=8", cardinality: 8, sharedBytes: 256},
		{name: "long/card=1024", cardinality: 1024, sharedBytes: 256},
		{name: "short-id/card=64", cardinality: 64, sharedBytes: 16},
	}
	for _, fixture := range fixtures {
		keys, documents := exactOverlapCorpus(
			b, exactOverlapBenchRows, fixture.cardinality, fixture.sharedBytes,
		)
		for _, test := range exactOverlapBenchCases() {
			if len(test.indexes) == 0 {
				continue
			}
			b.Run(fixture.name+"/"+test.name, func(b *testing.B) {
				path := filepath.Join(b.TempDir(), "mixed-pack.vibe")
				fileBytes := exactOverlapBuild(b, path, keys, documents, test.indexes)
				collection, file := exactOverlapOpen(b, path, test.indexes)
				defer func() {
					_ = collection.Close()
					_ = file.Close()
				}()
				existing := exactOverlapExactCensus(b, collection)
				var mixed, local exactMixedPackProjection
				b.ResetTimer()
				for b.Loop() {
					mixed = exactMixedPackProject(b, collection, true)
					local = exactMixedPackProject(b, collection, false)
				}
				b.StopTimer()
				projectedRaw := fileBytes - existing.extent + mixed.rawExtentBytes
				projectedMixed := fileBytes - existing.extent + mixed.compressedExtents
				projectedLocal := fileBytes - existing.extent + local.compressedExtents
				projectedMixed4K := fileBytes - existing.extent + mixed.compressed4KExtents
				projectedLocal4K := fileBytes - existing.extent + local.compressed4KExtents
				b.ReportMetric(float64(fileBytes)/exactOverlapBenchRows, "existing-fileB/doc")
				b.ReportMetric(float64(existing.extent)/exactOverlapBenchRows, "existing-exactLeafB/doc")
				b.ReportMetric(float64(projectedRaw)/exactOverlapBenchRows, "projected-raw-fileB/doc")
				b.ReportMetric(float64(projectedMixed)/exactOverlapBenchRows, "projected-mixed-lz4-pow2-fileB/doc")
				b.ReportMetric(float64(projectedLocal)/exactOverlapBenchRows, "projected-local-lz4-pow2-fileB/doc")
				b.ReportMetric(float64(projectedMixed4K)/exactOverlapBenchRows, "projected-mixed-lz4-4K-fileB/doc")
				b.ReportMetric(float64(projectedLocal4K)/exactOverlapBenchRows, "projected-local-lz4-4K-fileB/doc")
				b.ReportMetric(float64(mixed.frames), "mixed-packs")
				b.ReportMetric(float64(local.frames), "local-packs")
				b.ReportMetric(float64(mixed.compressedFrameWins), "mixed-lz4-wins")
				b.ReportMetric(float64(local.compressedFrameWins), "local-lz4-wins")
				b.ReportMetric(float64(mixed.encodedBytes), "canonical-leaf-bytes")
				b.ReportMetric(float64(mixed.rawStoredBytes), "mixed-raw-pack-bytes")
				b.ReportMetric(float64(mixed.compressedBytes), "mixed-lz4-pack-bytes")
				b.ReportMetric(float64(local.compressedBytes), "local-lz4-pack-bytes")
				b.ReportMetric(float64(mixed.rawSlackBytes), "mixed-raw-pow2-slack")
				b.ReportMetric(float64(mixed.compressedSlack), "mixed-lz4-pow2-slack")
				b.ReportMetric(float64(mixed.compressed4KSlack), "mixed-lz4-4K-slack")
			})
		}
	}
}
