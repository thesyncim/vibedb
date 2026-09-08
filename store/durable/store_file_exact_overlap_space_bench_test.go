package durable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibejson"
)

const exactOverlapBenchRows = 20_000

type exactOverlapBenchCase struct {
	name    string
	indexes []store.IndexDefinition
}

func exactOverlapBenchCases() []exactOverlapBenchCase {
	return []exactOverlapBenchCase{
		{name: "indexes=0"},
		{name: "shared-leading/indexes=1", indexes: []store.IndexDefinition{
			{Name: "tenant_shared_a", Paths: []string{"/tenant", "/shared", "/a"}},
		}},
		{name: "shared-nonleading/indexes=1", indexes: []store.IndexDefinition{
			{Name: "tenant_a_shared", Paths: []string{"/tenant", "/a", "/shared"}},
		}},
		{name: "shared-leading/indexes=2", indexes: []store.IndexDefinition{
			{Name: "tenant_shared_a", Paths: []string{"/tenant", "/shared", "/a"}},
			{Name: "tenant_shared_b", Paths: []string{"/tenant", "/shared", "/b"}},
		}},
		{name: "shared-nonleading/indexes=2", indexes: []store.IndexDefinition{
			{Name: "tenant_a_shared", Paths: []string{"/tenant", "/a", "/shared"}},
			{Name: "tenant_b_shared", Paths: []string{"/tenant", "/b", "/shared"}},
		}},
	}
}

func exactOverlapMix(value uint64) uint64 {
	value += 0x9e3779b97f4a7c15
	value = (value ^ value>>30) * 0xbf58476d1ce4e5b9
	value = (value ^ value>>27) * 0x94d049bb133111eb
	return value ^ value>>31
}

func exactOverlapSharedValues(cardinality, length int) []string {
	const alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz-_"
	values := make([]string, cardinality)
	for id := range values {
		value := make([]byte, length)
		state := exactOverlapMix(uint64(id) + 71)
		for i := range value {
			state = exactOverlapMix(state + uint64(i))
			value[i] = alphabet[state%uint64(len(alphabet))]
		}
		values[id] = string(value)
	}
	return values
}

func exactOverlapCorpus(tb testing.TB, rows, cardinality, sharedBytes int) ([]string, [][]byte) {
	tb.Helper()
	shared := exactOverlapSharedValues(cardinality, sharedBytes)
	keys := make([]string, rows)
	documents := make([][]byte, rows)
	for row := range rows {
		tenant := exactOverlapMix(uint64(row)+1) % 16
		sharedID := exactOverlapMix(uint64(row)+1009) % uint64(cardinality)
		keys[row] = fmt.Sprintf("overlap-%09d", row)
		document := fmt.Appendf(nil,
			`{"tenant":"t%02d","shared":"%s","a":"a%04d","b":"b%04d","id":%d}`,
			tenant, shared[sharedID], row%997, row%991, row,
		)
		var err error
		documents[row], err = vibejson.AppendCanonicalize(nil, document)
		if err != nil {
			tb.Fatal(err)
		}
	}
	return keys, documents
}

func exactOverlapOptions(indexes []store.IndexDefinition) Options {
	return Options{
		Collection:    store.Options{ChunkDocuments: 64},
		ResidentBytes: 256 << 20,
		Backend:       BackendPortable,
		Durability:    DurabilityBufferedVisible,
		Indexes:       indexes,
	}
}

func exactOverlapBuild(
	tb testing.TB, path string, keys []string, documents [][]byte,
	indexes []store.IndexDefinition,
) int64 {
	tb.Helper()
	builder, err := store.NewBuilder(store.Options{ChunkDocuments: 64})
	if err != nil {
		tb.Fatal(err)
	}
	for i := range keys {
		if err := builder.Append(keys[i], documents[i]); err != nil {
			tb.Fatal(err)
		}
	}
	source, err := builder.Build()
	if err != nil {
		tb.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		tb.Fatal(err)
	}
	fileBytes, err := CreateFromPrimary(source, file, exactOverlapOptions(indexes))
	if err != nil {
		_ = file.Close()
		tb.Fatal(err)
	}
	if err := file.Close(); err != nil {
		tb.Fatal(err)
	}
	return fileBytes
}

func exactOverlapOpen(tb testing.TB, path string, indexes []store.IndexDefinition) (*Collection, *os.File) {
	tb.Helper()
	file, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		tb.Fatal(err)
	}
	collection, err := Open(file, exactOverlapOptions(indexes))
	if err != nil {
		_ = file.Close()
		tb.Fatal(err)
	}
	return collection, file
}

type exactOverlapCensus struct {
	extent, catalog, encoded, key, posting, dictionary, metadata int64
}

func exactOverlapPrimaryCensus(tb testing.TB, collection *Collection) (payload, extent int64) {
	tb.Helper()
	router := collection.primaryRouter.Load()
	if router == nil {
		tb.Fatal("missing primary router")
	}
	for rank := 0; rank < router.Len(); rank++ {
		route, ok := router.RouteAtRank(rank)
		if !ok {
			tb.Fatalf("primary route at rank %d", rank)
		}
		lease, err := router.AcquireLeaf(collection.cache, route)
		if err != nil {
			tb.Fatal(err)
		}
		view, ok := storeio.AdmittedCompactPrimaryStripe(
			lease.Page(), collection.state.Load().root.StoreID, route.Bucket,
		)
		if !ok {
			lease.Release()
			tb.Fatalf("primary leaf %d is not compact", rank)
		}
		payload += int64(view.EncodedPayloadBytes())
		extent += int64(len(lease.Page()))
		lease.Release()
	}
	return payload, extent
}

func exactOverlapExactCensus(tb testing.TB, collection *Collection) exactOverlapCensus {
	tb.Helper()
	var census exactOverlapCensus
	if collection.primaryEpoch == nil {
		tb.Fatal("missing primary exact epoch")
	}
	physicalOffsets := make(map[uint64]string)
	countRef := func(ref storeio.PageRef, label string) {
		if ref.Length == 0 {
			tb.Fatalf("%s has empty physical ref", label)
		}
		if previous, exists := physicalOffsets[ref.Offset]; exists {
			tb.Fatalf("%s duplicates physical offset %d already counted by %s", label, ref.Offset, previous)
		}
		physicalOffsets[ref.Offset] = label
	}
	for i := range collection.primaryEpoch.exact {
		for j, ref := range collection.primaryEpoch.exact[i].catalog {
			countRef(ref, fmt.Sprintf("index %d catalog %d", i, j))
			census.catalog += int64(ref.Length)
		}
		for j := range collection.primaryEpoch.exact[i].leaves {
			leaf := &collection.primaryEpoch.exact[i].leaves[j]
			countRef(leaf.ref, fmt.Sprintf("index %d leaf %d", i, j))
			census.extent += int64(leaf.ref.Length)
			if len(leaf.encoded) < 28 {
				tb.Fatalf("index %d leaf %d encoded bytes=%d want at least 28", i, j, len(leaf.encoded))
			}
			keyAt := int(binary.LittleEndian.Uint16(leaf.encoded[18:20]))
			postingAt := int(binary.LittleEndian.Uint16(leaf.encoded[20:22]))
			dictionaryAt := int(binary.LittleEndian.Uint16(leaf.encoded[22:24]))
			dictionaryDataAt := int(binary.LittleEndian.Uint16(leaf.encoded[26:28]))
			if keyAt > postingAt || postingAt > dictionaryAt ||
				dictionaryAt > dictionaryDataAt || dictionaryDataAt > len(leaf.encoded) {
				tb.Fatalf(
					"index %d leaf %d malformed regions key=%d posting=%d dictionary=%d dictionaryData=%d encoded=%d",
					i, j, keyAt, postingAt, dictionaryAt, dictionaryDataAt, len(leaf.encoded),
				)
			}
			metadata := keyAt
			keyBytes := postingAt - keyAt
			postingBytes := dictionaryAt - postingAt
			dictionaryTailBytes := len(leaf.encoded) - dictionaryAt
			if metadata+keyBytes+postingBytes+dictionaryTailBytes != len(leaf.encoded) {
				tb.Fatalf("index %d leaf %d region accounting drift", i, j)
			}
			census.encoded += int64(len(leaf.encoded))
			census.key += int64(keyBytes)
			census.posting += int64(postingBytes)
			census.dictionary += int64(dictionaryTailBytes)
			census.metadata += int64(metadata)
		}
	}
	return census
}

func TestExactOverlapSpaceFixtureReopens(t *testing.T) {
	keys, documents := exactOverlapCorpus(t, 1_024, 8, 192)
	for _, test := range exactOverlapBenchCases() {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "overlap.vibe")
			exactOverlapBuild(t, path, keys, documents, test.indexes)
			collection, file := exactOverlapOpen(t, path, test.indexes)
			defer func() {
				_ = collection.Close()
				_ = file.Close()
			}()
			for _, row := range []int{0, 511, 1023} {
				got, found, err := collection.AppendRaw(nil, []byte(keys[row]))
				if err != nil || !found || !bytes.Equal(got, documents[row]) {
					t.Fatalf("row=%d found=%v err=%v", row, found, err)
				}
			}
			for _, definition := range test.indexes {
				needles := make([]vibejson.Index, len(definition.Paths))
				needleRaw := make([][]byte, len(definition.Paths))
				expected := make([]string, 0, 2)
				for pathIndex, path := range definition.Paths {
					var resolver storeio.UnifiedHoleResolver
					if err := resolver.SetPath([]byte(path)); err != nil {
						t.Fatal(err)
					}
					start, end, found, err := resolver.PathSpanOf(documents[0])
					if err != nil || !found {
						t.Fatalf("resolve %s: found=%v err=%v", path, found, err)
					}
					needleRaw[pathIndex] = bytes.Clone(documents[0][start:end])
					needles[pathIndex] = primaryExactTestNeedle(t, string(needleRaw[pathIndex]))
				}
				for row := range documents {
					matched := true
					for pathIndex, path := range definition.Paths {
						var resolver storeio.UnifiedHoleResolver
						if err := resolver.SetPath([]byte(path)); err != nil {
							t.Fatal(err)
						}
						start, end, found, err := resolver.PathSpanOf(documents[row])
						if err != nil || !found || !bytes.Equal(documents[row][start:end], needleRaw[pathIndex]) {
							matched = false
							break
						}
					}
					if matched {
						expected = append(expected, keys[row])
					}
				}
				got := primaryExactTestKeys(t, collection, definition.Name, needles...)
				if !slices.Equal(got, expected) {
					t.Fatalf("index %s keys=%v want=%v", definition.Name, got, expected)
				}
			}
		})
	}
}

// BenchmarkExactOverlapCheckpointSpace isolates bytes retained when the same
// long scalar participates in overlapping exact tuples. primary* metrics come
// from the primary leaf census; residualB/doc is the complete checkpoint minus
// primary leaf extents and therefore includes exact indexes and fixed metadata.
func BenchmarkExactOverlapCheckpointSpace(b *testing.B) {
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
			b.Run(fixture.name+"/"+test.name, func(b *testing.B) {
				var totalFileBytes int64
				var lastPath string
				b.ResetTimer()
				for i := 0; b.Loop(); i++ {
					lastPath = filepath.Join(b.TempDir(), fmt.Sprintf("overlap-%d.vibe", i))
					totalFileBytes += exactOverlapBuild(b, lastPath, keys, documents, test.indexes)
				}
				b.StopTimer()
				collection, file := exactOverlapOpen(b, lastPath, test.indexes)
				primaryPayload, primaryExtent := exactOverlapPrimaryCensus(b, collection)
				exact := exactOverlapExactCensus(b, collection)
				if err := collection.Close(); err != nil {
					b.Fatal(err)
				}
				if err := file.Close(); err != nil {
					b.Fatal(err)
				}
				meanFileBytes := float64(totalFileBytes) / float64(b.N)
				b.ReportMetric(meanFileBytes/exactOverlapBenchRows, "fileB/doc")
				b.ReportMetric(float64(primaryPayload)/exactOverlapBenchRows, "primaryPayloadB/doc")
				b.ReportMetric(float64(primaryExtent)/exactOverlapBenchRows, "primaryExtentB/doc")
				b.ReportMetric(float64(exact.extent)/exactOverlapBenchRows, "exactLeafExtentB/doc")
				b.ReportMetric(float64(exact.catalog)/exactOverlapBenchRows, "exactCatalogB/doc")
				b.ReportMetric(float64(exact.key)/exactOverlapBenchRows, "exactKeyB/doc")
				b.ReportMetric(float64(exact.posting)/exactOverlapBenchRows, "exactPostingB/doc")
				b.ReportMetric(float64(exact.dictionary)/exactOverlapBenchRows, "exactDictionaryTailB/doc")
				b.ReportMetric(float64(exact.metadata)/exactOverlapBenchRows, "exactMetadataB/doc")
				b.ReportMetric((meanFileBytes-float64(primaryExtent))/exactOverlapBenchRows, "residualB/doc")
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*exactOverlapBenchRows), "ns/doc")
			})
		}
	}
}
