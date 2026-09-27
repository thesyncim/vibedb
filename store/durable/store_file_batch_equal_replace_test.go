package durable

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/store"
)

type batchEqualReplaceReader interface {
	Len() uint64
	AppendRaw(dst []byte, key []byte) ([]byte, bool, error)
}

type batchEqualReplaceMutation struct {
	key    string
	value  string
	remove bool
}

func batchEqualReplaceDocument(number int, status, padding string) string {
	return fmt.Sprintf(`{"n":%d,"pad":%q,"status":%q}`, number, padding, status)
}

func assertBatchEqualReplaceOracle(
	t testing.TB, label string, reader batchEqualReplaceReader,
	keys []string, want map[string]string,
) {
	t.Helper()
	if got := reader.Len(); got != uint64(len(want)) {
		t.Fatalf("%s length = %d, want %d", label, got, len(want))
	}
	for _, key := range keys {
		wantRaw, wantPresent := want[key]
		gotRaw, gotPresent, err := reader.AppendRaw(nil, []byte(key))
		if err != nil {
			t.Fatalf("%s AppendRaw(%q): %v", label, key, err)
		}
		if gotPresent != wantPresent || (gotPresent && string(gotRaw) != wantRaw) {
			t.Fatalf("%s %q = (%q,%v), want (%q,%v)",
				label, key, gotRaw, gotPresent, wantRaw, wantPresent)
		}
	}
}

func batchEqualReplaceIndexOracle(t testing.TB, want map[string]string) map[string][]string {
	t.Helper()
	byStatus := make(map[string][]string)
	for key, raw := range want {
		var document struct {
			Status string `json:"status"`
		}
		if err := json.Unmarshal([]byte(raw), &document); err != nil {
			t.Fatalf("decode oracle document for %q: %v", key, err)
		}
		byStatus[document.Status] = append(byStatus[document.Status], key)
	}
	for status := range byStatus {
		slices.Sort(byStatus[status])
	}
	return byStatus
}

func assertBatchEqualReplaceIndexes(
	t *testing.T, collection *Collection, snapshot *Snapshot,
	statuses []string, want map[string]string,
) {
	t.Helper()
	byStatus := batchEqualReplaceIndexOracle(t, want)
	for _, status := range statuses {
		wantKeys := byStatus[status]
		needle := primaryExactTestNeedle(t, fmt.Sprintf("%q", status))
		var got []string
		if snapshot != nil {
			got = primaryExactSnapshotKeys(t, snapshot, "status", needle)
		} else {
			got = primaryExactTestKeys(t, collection, "status", needle)
			slices.Sort(got)
		}
		if !slices.Equal(got, wantKeys) {
			t.Fatalf("status index %q = %v, want %v", status, got, wantKeys)
		}
	}
}

func TestCollectionUpdateEqualLengthReplacementInterleavedMutations(t *testing.T) {
	for _, indexed := range []bool{false, true} {
		name := "plain"
		if indexed {
			name = "indexed"
		}
		t.Run(name, func(t *testing.T) {
			options := testBatchOptions(16)
			options.MaxBatchBytes = 1 << 20
			if indexed {
				options.Indexes = []store.IndexDefinition{{
					Name: "status", Paths: []string{"/status"},
				}}
			}
			path := filepath.Join(t.TempDir(), "equal-replacement.vibe")
			file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
			if err != nil {
				t.Fatal(err)
			}
			collection, err := Create(file, options)
			if err != nil {
				_ = file.Close()
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if collection != nil {
					_ = collection.Close()
				}
				_ = file.Close()
			})

			keys := []string{"a", "b", "c", "d", "e", "gone", "keep", "new"}
			initial := map[string]string{
				"a":    batchEqualReplaceDocument(1, "same", "a"),
				"b":    batchEqualReplaceDocument(2, "same", "neighbor-b"),
				"c":    batchEqualReplaceDocument(3, "grow", "c"),
				"d":    batchEqualReplaceDocument(4, "shrink", strings.Repeat("d", 64)),
				"e":    batchEqualReplaceDocument(5, "return", "e"),
				"gone": batchEqualReplaceDocument(6, "gone", "delete-me"),
				"keep": batchEqualReplaceDocument(7, "stable", strings.Repeat("k", 32)),
			}
			for _, key := range keys {
				if raw, ok := initial[key]; ok {
					if _, err := collection.Put([]byte(key), []byte(raw)); err != nil {
						t.Fatalf("seed %q: %v", key, err)
					}
				}
			}
			before, err := collection.Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = before.Close() })
			statuses := []string{"same", "grow", "expanded", "shrink", "return", "gone", "large", "back", "added", "stable"}
			if indexed {
				assertBatchEqualReplaceIndexes(t, collection, before, statuses, initial)
			}

			mutations := []batchEqualReplaceMutation{
				{key: "a", value: batchEqualReplaceDocument(8, "same", "a")},
				{key: "b", value: batchEqualReplaceDocument(9, "same", "neighbor-b")},
				{key: "c", value: batchEqualReplaceDocument(10, "grow", "c")},
				{key: "d", value: batchEqualReplaceDocument(11, "shrink", strings.Repeat("d", 64))},
				{key: "e", value: batchEqualReplaceDocument(12, "return", "e")},
				{key: "b", value: batchEqualReplaceDocument(13, "same", "neighbor-b")},
				{key: "c", value: batchEqualReplaceDocument(14, "expanded", strings.Repeat("x", 96))},
				{key: "a", value: batchEqualReplaceDocument(15, "same", "a")},
				{key: "d", value: batchEqualReplaceDocument(16, "shrink", "d")},
				{key: "gone", remove: true},
				{key: "e", remove: true},
				{key: "b", value: batchEqualReplaceDocument(17, "large", strings.Repeat("b", 48))},
				{key: "c", value: batchEqualReplaceDocument(18, "expanded", strings.Repeat("x", 96))},
				{key: "e", value: batchEqualReplaceDocument(19, "return", "e-reinserted")},
				{key: "d", remove: true},
				{key: "d", value: batchEqualReplaceDocument(20, "back", "d-back")},
				{key: "a", value: batchEqualReplaceDocument(21, "same", "a")},
				{key: "new", value: batchEqualReplaceDocument(22, "added", "new")},
				{key: "new", value: batchEqualReplaceDocument(23, "added", "new")},
			}
			want := make(map[string]string, len(initial))
			for key, raw := range initial {
				want[key] = raw
			}
			if err := collection.Update(func(batch *WriteBatch) error {
				for _, mutation := range mutations {
					if mutation.remove {
						if err := batch.Delete([]byte(mutation.key)); err != nil {
							return err
						}
						delete(want, mutation.key)
						continue
					}
					if err := batch.Put([]byte(mutation.key), []byte(mutation.value)); err != nil {
						return err
					}
					want[mutation.key] = mutation.value
				}
				return nil
			}); err != nil {
				t.Fatalf("Update: %v", err)
			}
			assertBatchEqualReplaceOracle(t, "live", collection, keys, want)
			assertBatchEqualReplaceOracle(t, "held snapshot", before, keys, initial)
			if indexed {
				assertBatchEqualReplaceIndexes(t, collection, nil, statuses, want)
				assertBatchEqualReplaceIndexes(t, nil, before, statuses, initial)
			}
			if err := before.Close(); err != nil {
				t.Fatalf("close old snapshot: %v", err)
			}
			if err := collection.Flush(); err != nil {
				t.Fatalf("flush: %v", err)
			}
			if err := collection.Close(); err != nil {
				t.Fatalf("close before reopen: %v", err)
			}
			collection = nil

			reopened, err := Open(file, options)
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			collection = reopened
			assertBatchEqualReplaceOracle(t, "reopened", reopened, keys, want)
			if indexed {
				assertBatchEqualReplaceIndexes(t, reopened, nil, statuses, want)
			}
		})
	}
}

// BenchmarkCollectionUpdateEqualLengthReplacement measures public batch
// publication in the DurabilityBufferedVisible lane. Its Update time includes
// bounded batch bookkeeping and publication; the final Flush is outside the
// timer so deferred device work does not accumulate past the benchmark.
func BenchmarkCollectionUpdateEqualLengthReplacement(b *testing.B) {
	for _, keyCount := range []int{64, 512} {
		for _, scenario := range []struct {
			name   string
			passes int
		}{
			{name: "no-duplicate", passes: 1},
			{name: "four-writes-per-key", passes: 4},
		} {
			b.Run(fmt.Sprintf("keys=%d/%s", keyCount, scenario.name), func(b *testing.B) {
				options := benchBatchOptions(keyCount)
				corpus := newMutationBenchCorpus(keyCount)
				collection, done := openMutationBenchCollection(b, options, corpus)
				defer done()
				startGeneration := collection.Generation()
				b.ReportAllocs()
				b.ResetTimer()
				state := 0
				for iteration := 0; iteration < b.N; iteration++ {
					target := state ^ 1
					if err := collection.Update(func(batch *WriteBatch) error {
						for pass := 0; pass < scenario.passes; pass++ {
							for keyIndex := range keyCount {
								if err := batch.Put(
									corpus.keys[keyIndex], corpus.documents[target][keyIndex],
								); err != nil {
									return err
								}
							}
						}
						return nil
					}); err != nil {
						b.Fatal(err)
					}
					state = target
				}
				b.StopTimer()
				if err := collection.Flush(); err != nil {
					b.Fatalf("flush: %v", err)
				}
				if b.N == 0 || collection.Generation() <= startGeneration {
					b.Fatal("benchmark did not publish an update")
				}
				for _, keyIndex := range []int{0, keyCount - 1} {
					got, ok, err := collection.AppendRaw(nil, corpus.keys[keyIndex])
					if err != nil || !ok || !bytes.Equal(got, corpus.documents[state][keyIndex]) {
						b.Fatalf("final row %d = (%q,%v,%v), want %q",
							keyIndex, got, ok, err, corpus.documents[state][keyIndex])
					}
				}
			})
		}
	}
}
