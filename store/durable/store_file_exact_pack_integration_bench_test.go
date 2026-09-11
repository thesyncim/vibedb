package durable

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibejson"
)

const exactPackIntegrationBatch = 64

func exactPackIntegrationIndexes() []store.IndexDefinition {
	return []store.IndexDefinition{
		{Name: "tenant_a_shared", Paths: []string{"/tenant", "/a", "/shared"}},
		{Name: "tenant_b_shared", Paths: []string{"/tenant", "/b", "/shared"}},
	}
}

func exactPackIntegrationNeedles(
	tb testing.TB, document []byte, definition store.IndexDefinition,
) []vibejson.Index {
	tb.Helper()
	needles := make([]vibejson.Index, len(definition.Paths))
	for i, path := range definition.Paths {
		var resolver storeio.UnifiedHoleResolver
		if err := resolver.SetPath([]byte(path)); err != nil {
			tb.Fatal(err)
		}
		start, end, found, err := resolver.PathSpanOf(document)
		if err != nil || !found {
			tb.Fatalf("resolve %s: found=%v err=%v", path, found, err)
		}
		needles[i] = primaryExactTestNeedle(tb, string(document[start:end]))
	}
	return needles
}

func BenchmarkExactPackResidentEquality(b *testing.B) {
	for _, cardinality := range []int{8, 1024} {
		b.Run(fmt.Sprintf("long/card=%d", cardinality), func(b *testing.B) {
			keys, documents := exactOverlapCorpus(b, exactOverlapBenchRows, cardinality, 256)
			indexes := exactPackIntegrationIndexes()
			path := filepath.Join(b.TempDir(), "exact-pack-probe.vibe")
			exactOverlapBuild(b, path, keys, documents, indexes)
			collection, file := exactOverlapOpen(b, path, indexes)
			defer func() { _ = collection.Close(); _ = file.Close() }()
			snapshot, err := collection.Snapshot()
			if err != nil {
				b.Fatal(err)
			}
			defer snapshot.Close()
			needles := [][]vibejson.Index{
				exactPackIntegrationNeedles(b, documents[0], indexes[0]),
				exactPackIntegrationNeedles(b, documents[0], indexes[1]),
			}
			workspaces := [2]IndexWorkspace{}
			defer workspaces[0].Release()
			defer workspaces[1].Release()
			masks := make([]store.Mask, 0, 256)
			want := [2]int{}
			for index := range indexes {
				masks, err = snapshot.AppendIndexMasksInto(
					masks[:0], &workspaces[index], indexes[index].Name, needles[index]...,
				)
				if err != nil {
					b.Fatal(err)
				}
				want[index] = primaryExactMaskRows(masks)
				if want[index] == 0 {
					b.Fatalf("index %s warm probe matched no rows", indexes[index].Name)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; b.Loop(); i++ {
				index := i & 1
				masks, err = snapshot.AppendIndexMasksInto(
					masks[:0], &workspaces[index], indexes[index].Name, needles[index]...,
				)
				if err != nil || primaryExactMaskRows(masks) != want[index] {
					b.Fatalf("index=%s rows=%d want=%d err=%v",
						indexes[index].Name, primaryExactMaskRows(masks), want[index], err)
				}
			}
		})
	}
}

type exactPackPreparedProbe struct {
	index    int
	needles  []vibejson.Index
	expected int
}

func exactPackTupleRaw(
	tb testing.TB, document []byte, definition store.IndexDefinition,
) ([]string, string) {
	tb.Helper()
	values := make([]string, len(definition.Paths))
	var signature []byte
	for i, path := range definition.Paths {
		var resolver storeio.UnifiedHoleResolver
		if err := resolver.SetPath([]byte(path)); err != nil {
			tb.Fatal(err)
		}
		start, end, found, err := resolver.PathSpanOf(document)
		if err != nil || !found {
			tb.Fatalf("resolve %s: found=%v err=%v", path, found, err)
		}
		values[i] = string(document[start:end])
		signature = append(signature, document[start:end]...)
		signature = append(signature, 0)
	}
	return values, string(signature)
}

func BenchmarkExactPackResidentEqualityRotating(b *testing.B) {
	const probesPerIndex = 256
	for _, cardinality := range []int{8, 1024} {
		b.Run(fmt.Sprintf("long/card=%d", cardinality), func(b *testing.B) {
			keys, documents := exactOverlapCorpus(b, exactOverlapBenchRows, cardinality, 256)
			indexes := exactPackIntegrationIndexes()
			path := filepath.Join(b.TempDir(), "exact-pack-rotating-probe.vibe")
			exactOverlapBuild(b, path, keys, documents, indexes)
			collection, file := exactOverlapOpen(b, path, indexes)
			defer func() { _ = collection.Close(); _ = file.Close() }()
			snapshot, err := collection.Snapshot()
			if err != nil {
				b.Fatal(err)
			}
			defer snapshot.Close()

			counts := make([]map[string]int, len(indexes))
			for index, definition := range indexes {
				counts[index] = make(map[string]int, len(documents))
				for _, document := range documents {
					_, signature := exactPackTupleRaw(b, document, definition)
					counts[index][signature]++
				}
			}
			prepared := make([]exactPackPreparedProbe, 0, probesPerIndex*len(indexes))
			for probe := range probesPerIndex {
				for index, definition := range indexes {
					row := (probe*7919 + index*3571) % len(documents)
					raw, signature := exactPackTupleRaw(b, documents[row], definition)
					expected := counts[index][signature]
					if probe&1 != 0 {
						raw[0] = fmt.Sprintf(`"absent-tenant-%03d"`, probe)
						expected = 0
					}
					needles := make([]vibejson.Index, len(raw))
					for i := range raw {
						needles[i] = primaryExactTestNeedle(b, raw[i])
					}
					prepared = append(prepared, exactPackPreparedProbe{
						index: index, needles: needles, expected: expected,
					})
				}
			}
			workspaces := make([]IndexWorkspace, len(indexes))
			defer func() {
				for i := range workspaces {
					workspaces[i].Release()
				}
			}()
			masks := make([]store.Mask, 0, 256)
			for _, probe := range prepared {
				masks, err = snapshot.AppendIndexMasksInto(
					masks[:0], &workspaces[probe.index], indexes[probe.index].Name,
					probe.needles...,
				)
				if err != nil || primaryExactMaskRows(masks) != probe.expected {
					b.Fatalf("warm index=%s rows=%d want=%d err=%v",
						indexes[probe.index].Name, primaryExactMaskRows(masks), probe.expected, err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; b.Loop(); i++ {
				probe := &prepared[i%len(prepared)]
				masks, err = snapshot.AppendIndexMasksInto(
					masks[:0], &workspaces[probe.index], indexes[probe.index].Name,
					probe.needles...,
				)
				if err != nil || primaryExactMaskRows(masks) != probe.expected {
					b.Fatalf("index=%s rows=%d want=%d err=%v",
						indexes[probe.index].Name, primaryExactMaskRows(masks), probe.expected, err)
				}
			}
		})
	}
}

func exactPackMutationDocument(
	tb testing.TB, row, cardinality int, shared []string, kind string, generation int,
) []byte {
	tb.Helper()
	tenant := exactOverlapMix(uint64(row)+1) % 16
	sharedID := exactOverlapMix(uint64(row)+1009) % uint64(cardinality)
	a, bv, id := row%997, row%991, row
	if kind == "shared" {
		sharedID = (sharedID + uint64(generation) + 1) % uint64(cardinality)
	} else if kind == "indexed-a" {
		a = 1000 + (row+generation)%997
	} else if kind == "unrelated" {
		id = row + (generation+1)*exactOverlapBenchRows
	}
	raw := fmt.Appendf(nil,
		`{"tenant":"t%02d","shared":"%s","a":"a%04d","b":"b%04d","id":%d}`,
		tenant, shared[sharedID], a, bv, id,
	)
	document, err := vibejson.AppendCanonicalize(nil, raw)
	if err != nil {
		tb.Fatal(err)
	}
	return document
}

func exactPackMutationVariants(
	tb testing.TB, rows, cardinality int, shared []string, kind string,
) [2][][]byte {
	tb.Helper()
	variants := [2][][]byte{make([][]byte, rows), make([][]byte, rows)}
	for row := range rows {
		variants[0][row] = exactPackMutationDocument(tb, row, cardinality, shared, kind, 0)
		variants[1][row] = exactPackMutationDocument(tb, row, cardinality, shared, kind, 1)
		if bytes.Equal(variants[0][row], variants[1][row]) {
			tb.Fatalf("%s row %d mutation variants are identical", kind, row)
		}
	}
	return variants
}

func TestExactPackIntegrationMutationCorrectness(t *testing.T) {
	const rows = 1_024
	for _, cardinality := range []int{8, 1024} {
		for _, kind := range []string{"shared", "indexed-a", "unrelated"} {
			t.Run(fmt.Sprintf("card=%d/%s", cardinality, kind), func(t *testing.T) {
				keys, documents := exactOverlapCorpus(t, rows, cardinality, 256)
				indexes := exactPackIntegrationIndexes()
				path := filepath.Join(t.TempDir(), "exact-pack-correctness.vibe")
				exactOverlapBuild(t, path, keys, documents, indexes)
				collection, file := exactOverlapOpen(t, path, indexes)
				defer func() { _ = collection.Close(); _ = file.Close() }()
				shared := exactOverlapSharedValues(cardinality, 256)
				const row = 17
				want := exactPackMutationDocument(t, row, cardinality, shared, kind, 0)
				if _, err := collection.Put([]byte(keys[row]), want); err != nil {
					t.Fatal(err)
				}
				if err := collection.Flush(); err != nil {
					t.Fatal(err)
				}
				got, found, err := collection.AppendRaw(nil, []byte(keys[row]))
				if err != nil || !found || !bytes.Equal(got, want) {
					t.Fatalf("row=%d found=%v err=%v", row, found, err)
				}
				for _, definition := range indexes {
					needles := exactPackIntegrationNeedles(t, want, definition)
					gotKeys := primaryExactTestKeys(t, collection, definition.Name, needles...)
					if !slices.Contains(gotKeys, keys[row]) {
						t.Fatalf("index %s lost updated key %s", definition.Name, keys[row])
					}
				}
			})
		}
	}
}

func BenchmarkExactPackExistingUpdate(b *testing.B) {
	for _, cardinality := range []int{8, 1024} {
		for _, kind := range []string{"shared", "indexed-a", "unrelated"} {
			b.Run(fmt.Sprintf("long/card=%d/%s", cardinality, kind), func(b *testing.B) {
				keys, documents := exactOverlapCorpus(b, exactOverlapBenchRows, cardinality, 256)
				indexes := exactPackIntegrationIndexes()
				path := filepath.Join(b.TempDir(), "exact-pack-update.vibe")
				exactOverlapBuild(b, path, keys, documents, indexes)
				collection, file := exactOverlapOpen(b, path, indexes)
				defer func() { _ = collection.Close(); _ = file.Close() }()
				keyBytes := make([][]byte, len(keys))
				for i := range keys {
					keyBytes[i] = []byte(keys[i])
				}
				shared := exactOverlapSharedValues(cardinality, 256)
				variants := exactPackMutationVariants(
					b, exactOverlapBenchRows, cardinality, shared, kind,
				)
				versions := make([]uint8, exactOverlapBenchRows)
				base := collection.Stats()
				updated := 0
				lastRow, lastVersion := 0, uint8(0)
				b.ResetTimer()
				for generation := 0; b.Loop(); generation++ {
					start := generation * exactPackIntegrationBatch
					if err := collection.Update(func(batch *WriteBatch) error {
						for offset := range exactPackIntegrationBatch {
							row := (start + offset*8191) % exactOverlapBenchRows
							versions[row] ^= 1
							document := variants[versions[row]][row]
							if err := batch.Put(keyBytes[row], document); err != nil {
								return err
							}
							lastRow, lastVersion = row, versions[row]
						}
						return nil
					}); err != nil {
						b.Fatal(err)
					}
					updated += exactPackIntegrationBatch
				}
				b.StartTimer()
				flushStart := time.Now()
				if err := collection.Flush(); err != nil {
					b.Fatal(err)
				}
				flushElapsed := time.Since(flushStart)
				b.StopTimer()
				after := collection.Stats()
				durableElapsed := b.Elapsed()
				b.ReportMetric(float64(durableElapsed.Nanoseconds())/float64(updated), "e2e-ns/doc")
				b.ReportMetric(float64(after.DeviceBytes-base.DeviceBytes)/float64(updated), "devB/doc")
				b.ReportMetric(float64(flushElapsed.Nanoseconds()), "flush-ns")

				// Verify the final acknowledged generation independently through a
				// fully materialized snapshot, a full document read, and both exact
				// indexes. Keep this checkpoint separate from the Flush acknowledgement
				// metrics above.
				checkpointStart := time.Now()
				snapshot, err := collection.Snapshot()
				if err != nil {
					b.Fatal(err)
				}
				if err := collection.Flush(); err != nil {
					b.Fatal(err)
				}
				checkpointElapsed := time.Since(checkpointStart)
				afterPhysical := collection.Stats()
				b.ReportMetric(float64((durableElapsed+checkpointElapsed).Nanoseconds())/float64(updated), "physical-e2e-ns/doc")
				b.ReportMetric(float64(afterPhysical.DeviceBytes-base.DeviceBytes)/float64(updated), "physical-devB/doc")
				b.ReportMetric(float64(checkpointElapsed.Nanoseconds()), "checkpoint-ns")

				probeRow := lastRow
				want := variants[lastVersion][probeRow]
				got, found, err := collection.AppendRaw(nil, keyBytes[probeRow])
				if err != nil || !found || !bytes.Equal(got, want) {
					b.Fatalf("acknowledged row=%d found=%v err=%v", probeRow, found, err)
				}
				defer snapshot.Close()
				for _, definition := range indexes {
					needles := exactPackIntegrationNeedles(b, want, definition)
					masks, err := snapshot.AppendIndexMasks(nil, definition.Name, needles...)
					if err != nil {
						b.Fatal(err)
					}
					matched := false
					if err := snapshot.RangeMasksRaw(masks, func(key, _ []byte) error {
						matched = matched || bytes.Equal(key, keyBytes[probeRow])
						return nil
					}); err != nil {
						b.Fatal(err)
					}
					if !matched {
						b.Fatalf("index %s lost acknowledged row %d", definition.Name, probeRow)
					}
				}
			})
		}
	}
}

func BenchmarkExactPackBatchInsert(b *testing.B) {
	for _, cardinality := range []int{8, 1024} {
		b.Run(fmt.Sprintf("long/card=%d", cardinality), func(b *testing.B) {
			indexes := exactPackIntegrationIndexes()
			seedKeys, seedDocuments := exactOverlapCorpus(b, 1, cardinality, 256)
			path := filepath.Join(b.TempDir(), "exact-pack-insert.vibe")
			exactOverlapBuild(b, path, seedKeys, seedDocuments, indexes)
			collection, file := exactOverlapOpen(b, path, indexes)
			defer func() { _ = collection.Close(); _ = file.Close() }()
			total := b.N * exactPackIntegrationBatch
			keys, documents := exactOverlapCorpus(b, total+1, cardinality, 256)
			keyBytes := make([][]byte, total)
			for i := range total {
				keyBytes[i] = []byte(keys[i+1])
			}
			base := collection.Stats()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				start := iteration * exactPackIntegrationBatch
				if err := collection.Update(func(batch *WriteBatch) error {
					for offset := range exactPackIntegrationBatch {
						at := start + offset
						if err := batch.Put(keyBytes[at], documents[at+1]); err != nil {
							return err
						}
					}
					return nil
				}); err != nil {
					b.Fatal(err)
				}
			}
			flushStart := time.Now()
			if err := collection.Flush(); err != nil {
				b.Fatal(err)
			}
			flushElapsed := time.Since(flushStart)
			b.StopTimer()
			after := collection.Stats()
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(total), "e2e-ns/doc")
			b.ReportMetric(float64(after.DeviceBytes-base.DeviceBytes)/float64(total), "devB/doc")
			b.ReportMetric(float64(flushElapsed.Nanoseconds()), "flush-ns")
			got, found, err := collection.AppendRaw(nil, keyBytes[total-1])
			if err != nil || !found || !bytes.Equal(got, documents[total]) {
				b.Fatalf("last inserted row found=%v err=%v", found, err)
			}
		})
	}
}

func BenchmarkExactPackOpen(b *testing.B) {
	for _, cardinality := range []int{8, 1024} {
		b.Run(fmt.Sprintf("long/card=%d", cardinality), func(b *testing.B) {
			keys, documents := exactOverlapCorpus(b, exactOverlapBenchRows, cardinality, 256)
			indexes := exactPackIntegrationIndexes()
			path := filepath.Join(b.TempDir(), "exact-pack-open.vibe")
			exactOverlapBuild(b, path, keys, documents, indexes)
			files := make([]*os.File, b.N)
			for i := range files {
				var err error
				files[i], err = os.OpenFile(path, os.O_RDWR, 0o600)
				if err != nil {
					b.Fatal(err)
				}
			}
			options := exactOverlapOptions(indexes)
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				collection, err := Open(files[i], options)
				if err != nil {
					b.Fatal(err)
				}
				b.StopTimer()
				if err := collection.Close(); err != nil {
					b.Fatal(err)
				}
				if err := files[i].Close(); err != nil {
					b.Fatal(err)
				}
				if i+1 < b.N {
					b.StartTimer()
				}
			}
		})
	}
}

func TestExactPackBatchInsertOverlays(t *testing.T) {
	indexes := exactPackIntegrationIndexes()
	seedKeys, seedDocuments := exactOverlapCorpus(t, 1, 8, 256)
	path := filepath.Join(t.TempDir(), "exact-pack-insert-overlay.vibe")
	exactOverlapBuild(t, path, seedKeys, seedDocuments, indexes)
	collection, file := exactOverlapOpen(t, path, indexes)
	defer func() { _ = collection.Close(); _ = file.Close() }()
	router := collection.primaryRouter.Load()
	if router == nil || router.Len() == 0 {
		t.Fatal("missing seed leaf")
	}
	route, ok := router.RouteAtRank(0)
	if !ok {
		t.Fatal("missing seed route")
	}
	t.Logf("seed leaves=%d ref.len=%d max=%d",
		router.Len(), route.Ref.Length, storeio.CommonPrimaryLeafMaxExtentBytes)
	const insertCount = 8
	keys, documents := exactOverlapCorpus(t, insertCount+1, 8, 256)
	if err := collection.Update(func(batch *WriteBatch) error {
		for i := range insertCount {
			if err := batch.Put([]byte(keys[i+1]), documents[i+1]); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	t.Logf("after pending=%v parents=%d splits=%d len=%d",
		collection.primaryUnifiedOverlay.hasPending(),
		len(collection.primaryPendingParents),
		collection.Stats().PrimaryLeafSplits,
		collection.Len(),
	)
	if !collection.primaryUnifiedOverlay.hasPending() {
		t.Fatal("small pack insert did not stay on row overlay")
	}
	if len(collection.primaryPendingParents) != 0 {
		t.Fatal("small pack insert dirtied compact leaves")
	}
	got, found, err := collection.AppendRaw(nil, []byte(keys[insertCount]))
	if err != nil || !found || !bytes.Equal(got, documents[insertCount]) {
		t.Fatalf("last inserted row found=%v err=%v", found, err)
	}
}
