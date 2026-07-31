package durable

// This file is the store-level gate for the cost of mutating a collection that
// maintains an exact secondary index. It exists because that cost regressed by
// three orders of magnitude and no benchmark noticed: on the competitive
// harness's buffered-visible tuning a same-size replacement Put measured about
// 4.6us unindexed and about 4,954us with one exact index over a single JSON
// field, because preparePrimaryExactLeaf re-encodes every term of the resident
// term leaf on every mutation rather than only the mutated bucket's
// contribution. The harness surfaced the damage only as an aggregate
// mixed-workload throughput figure (the 567 ops/s finding), which is too far
// from the cause to bisect; nothing at store level measured indexed mutation
// cost at all, so the cliff shipped silently.
//
// BenchmarkPrimaryExactMutation makes the regression a first-class number: the
// indexed=1 arms against their indexed=0 twins ARE the metric, and the fix for
// the whole-leaf re-encode must move the indexed arms from the millisecond
// class into the microsecond class without disturbing the unindexed baseline.

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"os"
	"testing"

	"github.com/thesyncim/vibedb/store"
)

// primaryExactMutationCheckpointEvery mirrors the competitive harness's mixed
// and diagnostic workloads, which call Collection.Flush after every 64
// mutations (bench/competitive/churn_diag_test.go and cmd/mixed). Keeping the
// cadence identical is what lets a per-mutation number here map onto the
// harness's mixed-workload throughput findings instead of measuring a
// checkpoint schedule nobody runs.
const primaryExactMutationCheckpointEvery = 64

// primaryExactBenchDoc pairs each key with two same-length bodies that differ
// in exactly one score digit. The timed loop alternates a key between them, so
// every Put changes stored bytes (no engine may elide the write) while the
// document's length and its indexed country value never change: the benchmark
// isolates the fixed-size replacement path — the harness's point-write lane —
// from growth-driven page reshaping and from index-selectivity churn.
type primaryExactBenchDoc struct {
	key      []byte
	original []byte
	updated  []byte
}

// primaryExactBenchCorpus builds a deterministic corpus in the competitive
// harness's size class (roughly 230 bytes per document, well under the tuned
// 1 KiB MaxDocumentBytes bound) with the indexed scalar at the same top-level
// "country" field the harness predicates on. Every field derives from the
// document ordinal by fixed arithmetic — no random source at all — so the same
// (documents, values) pair always yields byte-identical documents and the two
// cardinality arms differ only in how many distinct country values exist.
//
// values is the cardinality of the indexed field. Three values is the
// worst case for per-term posting volume: one term's posting list is a third of
// the corpus. A thousand values is the opposite extreme — about nine postings
// per term at that arm's 9,000-document ceiling, with a thousand term keys of
// overhead instead — so the pair brackets what real data can do to the term
// leaf from both directions.
func primaryExactBenchCorpus(b *testing.B, documents, values int) []primaryExactBenchDoc {
	b.Helper()
	tiers := []string{"free", "pro", "team", "enterprise"}
	regions := []string{"eu-west-1", "eu-central-1", "us-east-1", "us-west-2", "ap-south-1"}
	notes := []string{
		"steady state, no anomalies observed in the last reporting window",
		"processed during the scheduled maintenance window",
		"flagged for review after a threshold breach on the ingest path",
		"nominal; retention policy applied and checkpoint acknowledged",
	}
	docs := make([]primaryExactBenchDoc, documents)
	for i := range documents {
		// Scores stay three digits (100..999) so the same-size replacement
		// below can bump one digit without ever changing document length.
		score := 100 + (i*7)%900
		body := fmt.Appendf(nil,
			`{"id":%d,"name":"user-%08d","country":"v%03d","score":%d,"active":%t,`+
				`"profile":{"tier":"%s","region":"%s","joined":"%04d-%02d-%02d"},"note":"%s"}`,
			i, i, i%values, score, i%3 != 0,
			tiers[i%len(tiers)], regions[i%len(regions)],
			2018+i%7, 1+i%12, 1+i%28, notes[i%len(notes)],
		)
		updated := append([]byte(nil), body...)
		at := bytes.Index(updated, []byte(`"score":`))
		if at < 0 {
			b.Fatalf("benchmark corpus lost its score field: %s", body)
		}
		at += len(`"score":`)
		if updated[at] == '9' {
			updated[at] = '8'
		} else {
			updated[at]++
		}
		docs[i] = primaryExactBenchDoc{
			key:      fmt.Appendf(nil, "doc:%08d", i),
			original: body,
			updated:  updated,
		}
	}
	return docs
}

// primaryExactBenchOptions is the competitive harness's tuned buffered-visible
// configuration (bench/competitive/engine_vibejson_durable.go, options), copied
// field for field — same MaxDocumentBytes, BufferCount, QueueSlots, and
// GroupLimit — because a per-mutation number measured under different staging
// bounds would not be comparable with the harness's published findings, which
// is this file's whole purpose.
func primaryExactBenchOptions(indexed bool) Options {
	options := Options{
		ResidentBytes:      64 << 20,
		Durability:         DurabilityBufferedVisible,
		Backend:            BackendPortable,
		CheckpointStrength: CheckpointFilesystem,
		MaxBatchDocuments:  1,
		MaxDocumentBytes:   1 << 10,
		BufferCount:        1024,
		QueueSlots:         1024,
		GroupLimit:         64,
	}
	if indexed {
		options.Indexes = []store.IndexDefinition{{
			Name: "country", Paths: []string{"/country"},
		}}
	}
	return options
}

// primaryExactBenchStore bulk-builds the corpus into a durable collection via
// CreateFromPrimary — the same cutover the harness's loadBulk uses — and opens
// it with the tuned mutation options. Any build failure is fatal rather than a
// skip: a benchmark that silently dropped an indexed arm would recreate the
// exact blind spot this file exists to close.
// The deterministic content-defined cutter spans term-boundary runs, giant-term
// stripe pieces, and hard-cap sub-cuts, so both cardinalities build, probe, and
// churn at 100,000 documents.
func primaryExactBenchStore(
	b *testing.B, docs []primaryExactBenchDoc, indexed bool,
) *Collection {
	b.Helper()
	builder, err := store.NewBuilder(store.Options{})
	if err != nil {
		b.Fatalf("resident builder: %v", err)
	}
	for i := range docs {
		if err := builder.Append(string(docs[i].key), docs[i].original); err != nil {
			b.Fatalf("append %s: %v", docs[i].key, err)
		}
	}
	source, err := builder.Build()
	if err != nil {
		b.Fatalf("resident build: %v", err)
	}
	file, err := os.CreateTemp(b.TempDir(), "primary-exact-mutation-*")
	if err != nil {
		b.Fatalf("temp store file: %v", err)
	}
	// The bulk transaction stages the whole graph at once and sizes its own
	// commit pool, so BufferCount/QueueSlots are cleared for the cutover only —
	// exactly the harness's primaryBulkOptions split. Open below restores the
	// tuned pool, which is the staging geometry the mutation loop measures.
	bulk := primaryExactBenchOptions(indexed)
	bulk.BufferCount, bulk.QueueSlots = 0, 0
	if _, err := CreateFromPrimary(source, file, bulk); err != nil {
		b.Fatalf("indexed=%t bulk build must succeed, not skip: %v", indexed, err)
	}
	collection, err := Open(file, primaryExactBenchOptions(indexed))
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	b.Cleanup(func() {
		if err := collection.Close(); err != nil {
			b.Errorf("close: %v", err)
		}
		file.Close()
	})
	return collection
}

// BenchmarkPrimaryExactMutation measures a same-size replacement Put on the
// buffered-visible lane, checkpointing every 64 mutations, across the three
// axes that decide indexed mutation cost: whether an exact index exists at all
// (the headline pair — indexed=1 over indexed=0 at the same corpus is the
// regression ratio), how many documents are resident (1k and the largest large
// corpus each cardinality can build, because the whole-leaf re-encode scales
// with total postings rather than with the touched bucket), and the indexed
// field's cardinality (three values concentrate the postings into few terms; a
// thousand spread them thin and pay per-term overhead instead).
//
// The headline pair is docs=10000/card=low, indexed=0 against indexed=1. Red
// baseline measured when this gate was added (Apple M4 Max, -benchtime=1s):
// indexed=0 at 12.3us per mutation with 1 alloc, indexed=1 at 1,694us with
// 3,132 allocs — a 137x cliff, and the docs=9000/card=high arm lands at
// 5.1ms, squarely the decomposition's 4,954us figure. The pair's ratio is
// smaller than the raw 4.6us -> 4,954us Put-only cliff because both arms here
// also carry the amortized scheduled checkpoint (about 8us per mutation on the
// unindexed side), which is deliberate: the harness cadence is what production
// pays. The fix for the whole-leaf re-encode must collapse the pair's ratio to
// a small constant without moving the indexed=0 side.
func BenchmarkPrimaryExactMutation(b *testing.B) {
	cardinalities := []struct {
		name      string
		values    int
		documents []int
	}{
		// Low cardinality concentrates large posting runs and exercises the
		// spanned cutter.
		{name: "low", values: 3, documents: []int{1_000, 10_000, 100_000}},
		// High cardinality measures per-term routing overhead.
		{name: "high", values: 1000, documents: []int{1_000, 10_000, 100_000}},
	}
	for _, cardinality := range cardinalities {
		for _, documents := range cardinality.documents {
			for indexed := range 2 {
				name := fmt.Sprintf(
					"docs=%d/card=%s/indexed=%d",
					documents, cardinality.name, indexed,
				)
				b.Run(name, func(b *testing.B) {
					runPrimaryExactMutation(
						b, documents, cardinality.values, indexed == 1,
					)
				})
			}
		}
	}
}

func runPrimaryExactMutation(
	b *testing.B, documents, values int, indexed bool,
) {
	docs := primaryExactBenchCorpus(b, documents, values)
	collection := primaryExactBenchStore(b, docs, indexed)
	// Settle the freshly opened store on a clean checkpoint so the timed loop
	// starts from the same durable root regardless of corpus size.
	if err := collection.Flush(); err != nil {
		b.Fatalf("initial checkpoint: %v", err)
	}
	// The key stream is uniform over the resident corpus, from a fixed-seed
	// generator salted only by the benchmark's own axes: every run of one arm
	// replays the same keys in the same order, and no arm shares another's
	// stream. Uniform rather than Zipfian on purpose — the regression under
	// test is a per-mutation whole-leaf re-encode that a hot-key distribution
	// neither amplifies nor hides, and uniform also exercises every primary
	// leaf class the bulk build produced.
	var indexedBit uint64
	if indexed {
		indexedBit = 1
	}
	rng := rand.New(rand.NewPCG(
		0x9E3779B97F4A7C15,
		uint64(documents)<<16|uint64(values)<<1|indexedBit,
	))
	updated := make([]bool, len(docs))
	forcedBefore := collection.Stats().AutomaticCheckpoints
	b.ReportAllocs()
	b.ResetTimer()
	pending := 0
	for range b.N {
		at := rng.IntN(len(docs))
		body := docs[at].updated
		if updated[at] {
			body = docs[at].original
		}
		updated[at] = !updated[at]
		if _, err := collection.Put(docs[at].key, body); err != nil {
			b.Fatalf("put %s: %v", docs[at].key, err)
		}
		pending++
		if pending >= primaryExactMutationCheckpointEvery {
			if err := collection.Flush(); err != nil {
				b.Fatalf("scheduled checkpoint: %v", err)
			}
			pending = 0
		}
	}
	b.StopTimer()
	b.ReportMetric(
		float64(b.Elapsed().Nanoseconds())/float64(b.N), "ns/mutation",
	)
	// Staging pressure can force a checkpoint outside the 64-mutation
	// schedule, which would silently change what a mutation costs here; the
	// metric proves the cadence held (it must report zero) the same way the
	// mixed harness audits its own runs.
	forced := collection.Stats().AutomaticCheckpoints - forcedBefore
	b.ReportMetric(float64(forced)/float64(b.N), "forced-cp/op")
}

// primaryExactProbeBenchRows keeps the probe loop's result observable so the
// compiler cannot discard the lookups being measured.
var primaryExactProbeBenchRows int

// BenchmarkPrimaryExactProbe complements BenchmarkPrimaryExactLookupIteration
// (store_file_primary_exact_test.go), which already measures probe and
// probe-plus-iteration on a bulk-built 100-value corpus and is deliberately
// left untouched. The one probe axis it does not cover is term-leaf shape at
// the cardinality extremes this file's mutation arms introduce: at three
// values a single term's posting list is a third of the corpus — at 100,000
// documents a giant term spanning dozens of stripe-piece leaves, the largest
// streaming probe the spanned format can meet — while at a thousand values a
// term carries roughly documents/1000 postings routed by one binary search.
// The 100,000-document high-cardinality arm targets ≤ 2 µs and
// 0 allocations; the low arms measure giant-term streaming throughput, which
// scales with the postings returned, not with a bound. Each arm also reports
// exact-B/posting so the 100k space baseline stays a pinned number.
func BenchmarkPrimaryExactProbe(b *testing.B) {
	cardinalities := []struct {
		name      string
		values    int
		documents int
	}{
		{name: "low", values: 3, documents: 10_000},
		{name: "low", values: 3, documents: 100_000},
		{name: "high", values: 1000, documents: 10_000},
		{name: "high", values: 1000, documents: 100_000},
	}
	for _, cardinality := range cardinalities {
		name := fmt.Sprintf(
			"docs=%d/card=%s", cardinality.documents, cardinality.name,
		)
		b.Run(name, func(b *testing.B) {
			docs := primaryExactBenchCorpus(
				b, cardinality.documents, cardinality.values,
			)
			collection := primaryExactBenchStore(b, docs, true)
			byteCount, postings := 0, 0
			for _, resident := range collection.primaryEpoch.exact {
				for l := range resident.leaves {
					byteCount += len(resident.leaves[l].encoded)
					postings += resident.leaves[l].view.PostingLen()
				}
			}
			b.ReportMetric(
				float64(byteCount)/float64(postings), "exact-B/posting",
			)
			snapshot, err := collection.Snapshot()
			if err != nil {
				b.Fatalf("snapshot: %v", err)
			}
			defer snapshot.Close()
			// v001 exists at every cardinality this benchmark uses, so the
			// two arms probe the same term key and differ only in how many
			// postings that term resolves to.
			needle := primaryExactTestNeedle(b, `"v001"`)
			var workspace IndexWorkspace
			masks := make([]store.Mask, 0, 256)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				masks, err = snapshot.AppendIndexMasksInto(
					masks[:0], &workspace, "country", needle,
				)
				if err != nil {
					b.Fatalf("probe: %v", err)
				}
			}
			b.StopTimer()
			primaryExactProbeBenchRows = primaryExactMaskRows(masks)
		})
	}
}
