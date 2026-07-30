package competitive

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"slices"
	"testing"
)

func TestSameSizeUpdatedJSON(t *testing.T) {
	for i := range min(1000, len(docs)) {
		got := SameSizeUpdatedJSON(docs, i)
		if !json.Valid(got) {
			t.Fatalf("document %d update is invalid JSON: %s", i, got)
		}
		if len(got) != len(docs[i].JSON) {
			t.Fatalf("document %d update length = %d, want %d", i, len(got), len(docs[i].JSON))
		}
		if bytes.Equal(got, docs[i].JSON) {
			t.Fatalf("document %d update did not change bytes", i)
		}
	}
}

// BenchmarkPointWriteSameSize isolates a replacement that changes bytes but
// not document length, key routing, or the indexed country. It belongs beside
// BenchmarkPointWrite, whose appended revision field grows the value.
func BenchmarkPointWriteSameSize(b *testing.B) {
	for _, factory := range Factories() {
		for _, durability := range BenchmarkDurabilityModes(factory.Name) {
			b.Run(fmt.Sprintf(
				"%s/durability=%s", factory.Name, durability,
			), func(b *testing.B) {
				closeForeignFixtures("")
				e, _, cleanup := newLoaded(b, factory, Config{
					Durability: durability,
				})
				defer cleanup()
				i := 0
				replacement := make([]byte, 0, 512)
				updated := make([]bool, len(docs))
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					idx := probeIdx[i]
					if updated[idx] {
						replacement = append(replacement[:0], docs[idx].JSON...)
					} else {
						replacement = AppendSameSizeUpdatedJSON(replacement[:0], docs, idx)
					}
					if err := e.Put(docs[idx].Key, replacement); err != nil {
						b.Fatal(err)
					}
					updated[idx] = !updated[idx]
					i++
					if i == len(probeIdx) {
						i = 0
					}
				}
			})
		}
	}
}

// BenchmarkDeleteRestore measures steady delete churn without shrinking the
// fixture away: every deletion is followed by an upsert of the exact original
// value. One benchmark operation is therefore two durable mutations, reported
// explicitly as ns/mutation as well as the standard ns/op cycle.
func BenchmarkDeleteRestore(b *testing.B) {
	for _, factory := range Factories() {
		for _, durability := range BenchmarkDurabilityModes(factory.Name) {
			b.Run(fmt.Sprintf(
				"%s/durability=%s", factory.Name, durability,
			), func(b *testing.B) {
				closeForeignFixtures("")
				e, _, cleanup := newLoaded(b, factory, Config{
					Durability: durability,
				})
				defer cleanup()
				i := 0
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					idx := probeIdx[i]
					if err := e.Delete(docs[idx].Key); err != nil {
						b.Fatal(err)
					}
					if err := e.Upsert(docs[idx].Key, docs[idx].JSON); err != nil {
						b.Fatal(err)
					}
					i++
					if i == len(probeIdx) {
						i = 0
					}
				}
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(2*b.N), "ns/mutation")
				b.ReportMetric(2, "mutations/op")
			})
		}
	}
}

type mixedScenario struct {
	name                         string
	reads, updates, readModifies int
	churns, scans                int
	indexed                      bool
}

var mixedScenarios = []mixedScenario{
	// The first three mirror YCSB B, A, and F operation ratios. Unlike YCSB's
	// default 1-KiB field records, this harness keeps the shared exact JSON
	// corpus and every engine's native document API.
	{name: "ycsb-b-read95-update5", reads: 950, updates: 50},
	{name: "ycsb-a-read50-update50", reads: 500, updates: 500},
	{name: "ycsb-f-read50-rmw50", reads: 500, readModifies: 500},
	{name: "read70-update25-churn5", reads: 700, updates: 250, churns: 50},
	{name: "read80-update15-churn5-indexed", reads: 800, updates: 150, churns: 50, indexed: true},
	// One full ordered scan per 1,000 foreground choices. The scan is one
	// operation in the mix, but its cost includes touching every value byte.
	{name: "read80-update15-churn5-scan", reads: 799, updates: 150, churns: 50, scans: 1},
}

func (s mixedScenario) total() int {
	return s.reads + s.updates + s.readModifies + s.churns + s.scans
}

type mixedOperation uint8

const (
	mixedRead mixedOperation = iota
	mixedUpdate
	mixedReadModifyWrite
	mixedChurn
	mixedScan
)

// mixedOperationTrace returns one deterministic, exactly proportioned,
// shuffled cycle. Grouping all reads before all writes is not a mixed
// workload: it changes transaction-cache invalidation, write backpressure, and
// LSM background behavior enough to reverse comparative findings.
func mixedOperationTrace(s mixedScenario) []mixedOperation {
	cycle := make([]mixedOperation, 0, s.total())
	appendN := func(op mixedOperation, n int) {
		for range n {
			cycle = append(cycle, op)
		}
	}
	appendN(mixedRead, s.reads)
	appendN(mixedUpdate, s.updates)
	appendN(mixedReadModifyWrite, s.readModifies)
	appendN(mixedChurn, s.churns)
	appendN(mixedScan, s.scans)
	rng := rand.New(rand.NewSource(0xA11CE))
	trace := make([]mixedOperation, 0, 64*len(cycle))
	for range 64 {
		rng.Shuffle(len(cycle), func(i, j int) {
			cycle[i], cycle[j] = cycle[j], cycle[i]
		})
		trace = append(trace, cycle...)
	}
	return trace
}

func TestMixedOperationTrace(t *testing.T) {
	s := mixedScenarios[3]
	trace := mixedOperationTrace(s)
	if len(trace) != 64*s.total() {
		t.Fatalf("trace length = %d, want %d", len(trace), 64*s.total())
	}
	counts := [5]int{}
	for _, op := range trace {
		if int(op) >= len(counts) {
			t.Fatalf("invalid operation %d", op)
		}
		counts[op]++
	}
	want := [5]int{
		64 * s.reads,
		64 * s.updates,
		64 * s.readModifies,
		64 * s.churns,
		64 * s.scans,
	}
	if counts != want {
		t.Fatalf("operation counts = %v, want %v", counts, want)
	}
	grouped := true
	for _, op := range trace[:s.reads] {
		if op != mixedRead {
			grouped = false
			break
		}
	}
	if grouped {
		t.Fatal("trace begins with an unshuffled read phase")
	}
	if slices.Equal(trace[:s.total()], trace[s.total():2*s.total()]) {
		t.Fatal("operation cycle repeats instead of being independently shuffled")
	}
}

// BenchmarkMixedWorkload drives deterministic, cache-unfriendly point probes
// with same-size updates, delete/reinsert churn, and optional ordered scans.
// The indexed arm keeps the predicate value stable during updates but still
// pays full index removal/reinsertion for churn.
//
// A churn choice performs Delete+Upsert and is counted as one user operation
// and two storage mutations. Results report the exact physical call mix so the
// number cannot be mistaken for an all-read or all-write throughput figure.
func BenchmarkMixedWorkload(b *testing.B) {
	for _, scenario := range mixedScenarios {
		if scenario.total() != 1000 {
			b.Fatalf("scenario %s has %d choices, want 1000", scenario.name, scenario.total())
		}
		operationTrace := mixedOperationTrace(scenario)
		for _, factory := range Factories() {
			for _, durability := range BenchmarkDurabilityModes(factory.Name) {
				if scenario.indexed && !IndexCapable(factory.Name) {
					continue
				}
				name := fmt.Sprintf(
					"%s/%s/durability=%s",
					scenario.name, factory.Name, durability,
				)
				b.Run(name, func(b *testing.B) {
					closeForeignFixtures("")
					e, _, cleanup := newLoaded(b, factory, Config{
						Durability: durability,
						Indexed:    scenario.indexed,
					})
					defer cleanup()

					// YCSB's standard mixes use a Zipfian request
					// distribution. Precomputing the trace keeps generator
					// cost out of the storage number while retaining stable
					// hot-key locality across engines and repetitions.
					const traceSize = 1 << 16
					trace := make([]int, traceSize)
					rng := rand.New(rand.NewSource(0x51C5B))
					zipf := rand.NewZipf(rng, 1.01, 1, uint64(len(docs)-1))
					for i := range trace {
						trace[i] = probeIdx[int(zipf.Uint64())]
					}

					var reads, updates, readModifies, churns, scans uint64
					var readScratch, replacement []byte
					updated := make([]bool, len(docs))
					probe := 0
					b.ReportAllocs()
					b.ResetTimer()
					for operation := 0; b.Loop(); operation++ {
						choice := operationTrace[operation%len(operationTrace)]
						idx := trace[probe]
						probe++
						if probe == len(trace) {
							probe = 0
						}
						switch choice {
						case mixedRead:
							out, err := e.Get(readScratch[:0], docs[idx].Key)
							if err != nil || len(out) == 0 {
								b.Fatalf("Get(%q) = %d bytes, %v", docs[idx].Key, len(out), err)
							}
							readScratch = out
							reads++
						case mixedUpdate:
							if updated[idx] {
								replacement = append(replacement[:0], docs[idx].JSON...)
							} else {
								replacement = AppendSameSizeUpdatedJSON(replacement[:0], docs, idx)
							}
							if err := e.Put(docs[idx].Key, replacement); err != nil {
								b.Fatal(err)
							}
							updated[idx] = !updated[idx]
							updates++
						case mixedReadModifyWrite:
							out, err := e.Get(readScratch[:0], docs[idx].Key)
							if err != nil || len(out) == 0 {
								b.Fatalf("RMW Get(%q) = %d bytes, %v", docs[idx].Key, len(out), err)
							}
							readScratch = out
							if updated[idx] {
								replacement = append(replacement[:0], docs[idx].JSON...)
							} else {
								replacement = AppendSameSizeUpdatedJSON(replacement[:0], docs, idx)
							}
							if err := e.Put(docs[idx].Key, replacement); err != nil {
								b.Fatal(err)
							}
							updated[idx] = !updated[idx]
							readModifies++
						case mixedChurn:
							if err := e.Delete(docs[idx].Key); err != nil {
								b.Fatal(err)
							}
							if err := e.Upsert(docs[idx].Key, docs[idx].JSON); err != nil {
								b.Fatal(err)
							}
							updated[idx] = false
							churns++
						case mixedScan:
							n, err := e.ScanAllBytes()
							if err != nil || n != len(docs) {
								b.Fatalf("ScanAllBytes = %d, %v", n, err)
							}
							scans++
						default:
							b.Fatalf("unknown mixed operation %d", choice)
						}
					}
					b.StopTimer()
					diskBytes, err := e.DiskBytes()
					if err != nil {
						b.Fatal(err)
					}
					b.ReportMetric(float64(reads)/float64(b.N), "reads/op")
					b.ReportMetric(float64(updates)/float64(b.N), "updates/op")
					b.ReportMetric(float64(readModifies)/float64(b.N), "rmw/op")
					b.ReportMetric(float64(churns)/float64(b.N), "churns/op")
					b.ReportMetric(float64(scans)/float64(b.N), "scans/op")
					b.ReportMetric(float64(reads+updates+2*readModifies+2*churns+scans)/float64(b.N), "engine-calls/op")
					if diskBytes != 0 {
						b.ReportMetric(float64(diskBytes)/float64(len(docs)), "disk-B/doc")
					}
				})
			}
		}
	}
}

func TestMixedMutationEquivalence(t *testing.T) {
	fixture := Corpus(512)
	for _, factory := range Factories() {
		t.Run(factory.Name, func(t *testing.T) {
			dir := t.TempDir()
			indexed := IndexCapable(factory.Name)
			e, err := factory.New(Config{
				Dir: dir, Indexed: indexed, CacheBytes: DefaultCacheBytes,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer e.Close()
			if err := e.Load(fixture); err != nil {
				t.Fatal(err)
			}

			want := make(map[string][]byte, len(fixture))
			var expectedScratch []byte
			for i := range fixture {
				expectedScratch, err = AppendExpectedStoredJSON(expectedScratch[:0], factory.Name, fixture[i].JSON)
				if err != nil {
					t.Fatal(err)
				}
				want[fixture[i].Key] = append([]byte(nil), expectedScratch...)
			}

			updateAt := 17
			updated := SameSizeUpdatedJSON(fixture, updateAt)
			if err := e.Put(fixture[updateAt].Key, updated); err != nil {
				t.Fatal(err)
			}
			expectedScratch, err = AppendExpectedStoredJSON(expectedScratch[:0], factory.Name, updated)
			if err != nil {
				t.Fatal(err)
			}
			want[fixture[updateAt].Key] = append(want[fixture[updateAt].Key][:0], expectedScratch...)
			got, err := e.Get(nil, fixture[updateAt].Key)
			if err != nil || !bytes.Equal(got, want[fixture[updateAt].Key]) {
				t.Fatalf("updated Get = %q, %v; want %q", got, err, want[fixture[updateAt].Key])
			}

			deleteAt := 23
			if indexed {
				for i := range fixture {
					if bytes.Contains(fixture[i].JSON, []byte(`"country":"`+FilterValue+`"`)) {
						deleteAt = i
						break
					}
				}
			}
			if err := e.Delete(fixture[deleteAt].Key); err != nil {
				t.Fatal(err)
			}
			delete(want, fixture[deleteAt].Key)
			if _, err := e.Get(nil, fixture[deleteAt].Key); err == nil {
				t.Fatalf("Get(%q) succeeded after Delete", fixture[deleteAt].Key)
			}
			if indexed {
				got, err := e.IndexedCount(FilterValue)
				if err != nil {
					t.Fatal(err)
				}
				_, _, _, baseline := CorpusStats(fixture)
				if got != baseline-1 {
					t.Fatalf("IndexedCount after delete = %d, want %d", got, baseline-1)
				}
			}
			if err := e.Upsert(fixture[deleteAt].Key, fixture[deleteAt].JSON); err != nil {
				t.Fatal(err)
			}
			expectedScratch, err = AppendExpectedStoredJSON(expectedScratch[:0], factory.Name, fixture[deleteAt].JSON)
			if err != nil {
				t.Fatal(err)
			}
			want[fixture[deleteAt].Key] = append([]byte(nil), expectedScratch...)

			seen := make(map[string]bool, len(want))
			if err := e.Visit(func(key string, value []byte) error {
				expected, ok := want[key]
				if !ok {
					return fmt.Errorf("unexpected key %q", key)
				}
				if seen[key] {
					return fmt.Errorf("duplicate key %q", key)
				}
				if !bytes.Equal(value, expected) {
					return fmt.Errorf("value mismatch for %q", key)
				}
				seen[key] = true
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if len(seen) != len(want) {
				t.Fatalf("Visit saw %d documents, want %d", len(seen), len(want))
			}
			if indexed {
				got, err := e.IndexedCount(FilterValue)
				if err != nil {
					t.Fatal(err)
				}
				_, _, _, baseline := CorpusStats(fixture)
				if got != baseline {
					t.Fatalf("IndexedCount after restore = %d, want %d", got, baseline)
				}
			}
		})
	}
}
