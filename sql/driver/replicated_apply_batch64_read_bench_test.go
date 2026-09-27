package driver

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/rf3bench"
	"github.com/thesyncim/vibedb/store/durable"
	"github.com/thesyncim/vibejson"
)

const replicatedApplyBatch64RuntimeReadRows = batch64Rows * batch64MeasurementBatches

type replicatedApplyBatch64ReadCorpus struct {
	database    *Database
	identity    ReplicatedShardStoreIdentity
	collection  *durable.Collection
	keys        [][]byte
	expected    [][]byte
	permutation []int
	maxRowBytes int
	setupStats  durable.Stats
	footprint   rf3bench.Footprint
}

// BenchmarkReplicatedApplyBatch64RuntimeWarmPointRead measures direct durable
// point reads from rows published through the real three-member ReplicatedApply
// path. Setup builds and checkpoints a fixed 131,072-row corpus before the
// timer. Each operation reads every row once into one reused destination, so
// -benchtime=1x measures one full pass and larger fixed counts such as 50x can
// be used for stable profiling. Sequential and deterministic permutation probes
// share each immutable corpus but warm and validate their complete access order
// independently before timing.
func BenchmarkReplicatedApplyBatch64RuntimeWarmPointRead(b *testing.B) {
	for _, corpusCase := range []struct {
		name     string
		fillRows replicatedApplyBatch64RowsFunc
	}{
		{name: "varied", fillRows: fillReplicatedApplyBatch64Rows},
		{name: "shared", fillRows: fillReplicatedApplyBatch64SharedRows},
	} {
		corpusCase := corpusCase
		b.Run(corpusCase.name, func(b *testing.B) {
			b.StopTimer()
			corpus := newReplicatedApplyBatch64ReadCorpus(
				b, replicatedApplyBatch64RuntimeReadRows, corpusCase.fillRows,
			)
			b.Run("sequential", func(b *testing.B) {
				runReplicatedApplyBatch64WarmPointRead(b, corpus, nil)
			})
			b.Run("permuted", func(b *testing.B) {
				runReplicatedApplyBatch64WarmPointRead(
					b, corpus, corpus.permutation,
				)
			})
		})
	}
}

func newReplicatedApplyBatch64ReadCorpus(
	tb testing.TB,
	rows int,
	fillRows replicatedApplyBatch64RowsFunc,
) *replicatedApplyBatch64ReadCorpus {
	tb.Helper()
	if rows <= 0 || rows%batch64Rows != 0 || fillRows == nil {
		tb.Fatalf("invalid runtime-read corpus rows=%d fillRows=%t", rows, fillRows != nil)
	}
	database, claim, identity, group := newReplicatedApplyBatch64Fixture(tb)
	core := database.connector.db
	core.mu.RLock()
	table := core.tables[identity.UserTable]
	if table == nil || table.collection == nil {
		core.mu.RUnlock()
		tb.Fatalf("replicated user table %q is unavailable", identity.UserTable)
	}
	collection := table.collection
	core.mu.RUnlock()

	corpus := &replicatedApplyBatch64ReadCorpus{
		database: database, identity: identity, collection: collection,
		keys: make([][]byte, rows), expected: make([][]byte, rows),
		permutation: make([]int, rows),
	}
	lanes := replicatedApplyBatch64Lanes()
	batchKeys := make([][]byte, batch64Rows)
	batchValues := make([][]byte, batch64Rows)
	mutations := make([]replication.Mutation, batch64Rows)
	entries := make([]raftmodel.NormalApply, 1)
	witnesses := make([][32]byte, 1)
	var completions raftmodel.NormalApplyBatchCompletions
	command := make([]byte, 0, 128<<10)
	for batch := 0; batch < rows/batch64Rows; batch++ {
		start := batch * batch64Rows
		if err := fillRows(database, start, batchKeys, batchValues); err != nil {
			tb.Fatalf("fill runtime-read batch %d: %v", batch, err)
		}
		for row := range mutations {
			ordinal := start + row
			corpus.keys[ordinal] = bytes.Clone(batchKeys[row])
			canonical, err := vibejson.AppendCanonicalize(nil, batchValues[row])
			if err != nil {
				tb.Fatalf("canonicalize runtime-read row %d: %v", ordinal, err)
			}
			corpus.expected[ordinal] = canonical
			if len(canonical) > corpus.maxRowBytes {
				corpus.maxRowBytes = len(canonical)
			}
			mutations[row] = replication.Mutation{
				Kind: replication.MutationPutAbsent,
				Key:  batchKeys[row], Value: batchValues[row],
			}
		}
		batches := []replication.RelationMutationBatch{{
			Relation: 1, Mutations: mutations,
		}}
		nextCommand, err := appendReplicatedApplyBatch64Command(
			command[:0], identity, lanes[batch%len(lanes)],
			uint64(batch/len(lanes))+1, batches,
		)
		if err != nil {
			tb.Fatalf("encode runtime-read batch %d: %v", batch, err)
		}
		command = nextCommand
		index := uint64(batch + 2)
		entries[0] = raftmodel.NormalApply{
			Meta: replicatedApplyBatch64Meta(index), Data: command,
		}
		applied, publication, applyErr := claim.ApplyNormalBatchWithCompletions(
			entries, witnesses, &completions,
		)
		if applyErr != nil || applied != 1 || publication.Applied != index ||
			witnesses[0] == ([32]byte{}) || publication.DataChainDigest != witnesses[0] {
			tb.Fatalf("runtime-read batch %d apply=%d publication=%+v witness=%x err=%v",
				batch, applied, publication, witnesses[0], applyErr)
		}
		completion, present := completions.Completion(0)
		if !present || len(completion) == 0 {
			tb.Fatalf("runtime-read batch %d returned no completion", batch)
		}
		view, completionErr := replication.OpenCompletion(completion)
		if completionErr != nil {
			tb.Fatalf("open runtime-read completion %d: %v", batch, completionErr)
		}
		result, resultErr := replicatedstate.OpenTransactionCompletionResult(
			view.ResultCode, view.InlineResult,
		)
		if resultErr != nil || view.ResultCode != replicatedstate.ResultApplied ||
			view.AppliedSequence != index || !result.AffectedRowsValid ||
			result.AffectedRows != batch64Rows {
			tb.Fatalf("runtime-read completion %d = %+v result=%+v err=%v",
				batch, view, result, resultErr)
		}
	}
	if err := group.Checkpoint(); err != nil {
		tb.Fatalf("checkpoint runtime-read corpus: %v", err)
	}
	if collection.Len() != uint64(rows) {
		tb.Fatalf("runtime-read corpus rows = %d, want %d", collection.Len(), rows)
	}
	groupStats := group.Stats()
	if groupStats.AppliedIndex != uint64(rows/batch64Rows+1) ||
		groupStats.CheckpointAppliedIndex != groupStats.AppliedIndex {
		tb.Fatalf("runtime-read corpus group cut = %+v", groupStats)
	}
	corpus.setupStats = collection.Stats()
	footprint, err := rf3bench.MeasureFootprint(database.connector.db.dataDir)
	if err != nil {
		tb.Fatalf("measure runtime-read corpus footprint: %v", err)
	}
	corpus.footprint = footprint
	for ordinal := range corpus.permutation {
		corpus.permutation[ordinal] = ordinal
	}
	state := uint64(0x726561642d626174)
	for ordinal := len(corpus.permutation) - 1; ordinal > 0; ordinal-- {
		state = replicatedApplyBatch64Mix(state + uint64(ordinal))
		other := int(state % uint64(ordinal+1))
		corpus.permutation[ordinal], corpus.permutation[other] =
			corpus.permutation[other], corpus.permutation[ordinal]
	}
	return corpus
}

func validateReplicatedApplyBatch64ReadOrder(
	tb testing.TB,
	corpus *replicatedApplyBatch64ReadCorpus,
	order []int,
) {
	tb.Helper()
	if corpus == nil || corpus.collection == nil || len(corpus.keys) == 0 ||
		len(corpus.keys) != len(corpus.expected) ||
		corpus.maxRowBytes <= 0 ||
		(order != nil && len(order) != len(corpus.keys)) {
		tb.Fatal("runtime-read corpus shape is invalid")
	}
	seen := make([]bool, len(corpus.keys))
	dst := make([]byte, 0, corpus.maxRowBytes)
	for probe := range corpus.keys {
		ordinal := probe
		if order != nil {
			ordinal = order[probe]
		}
		if ordinal < 0 || ordinal >= len(corpus.keys) || seen[ordinal] {
			tb.Fatalf("runtime-read order repeats or misses ordinal %d at probe %d", ordinal, probe)
		}
		seen[ordinal] = true
		got, found, err := corpus.collection.AppendRaw(dst[:0], corpus.keys[ordinal])
		if err != nil || !found || !bytes.Equal(got, corpus.expected[ordinal]) {
			tb.Fatalf("runtime-read row %d = %q/%v/%v, want %q",
				ordinal, got, found, err, corpus.expected[ordinal])
		}
		dst = got[:0]
	}
}

func runReplicatedApplyBatch64PointReadPass(
	corpus *replicatedApplyBatch64ReadCorpus,
	order []int,
	dst []byte,
) ([]byte, error) {
	for probe := range corpus.keys {
		ordinal := probe
		if order != nil {
			ordinal = order[probe]
		}
		got, found, err := corpus.collection.AppendRaw(dst[:0], corpus.keys[ordinal])
		if err != nil {
			return dst, fmt.Errorf("point read ordinal %d: %w", ordinal, err)
		}
		if !found {
			return dst, fmt.Errorf("point read ordinal %d was absent", ordinal)
		}
		dst = got[:0]
	}
	return dst, nil
}

func runReplicatedApplyBatch64WarmPointRead(
	b *testing.B,
	corpus *replicatedApplyBatch64ReadCorpus,
	order []int,
) {
	b.Helper()
	b.StopTimer()
	validateReplicatedApplyBatch64ReadOrder(b, corpus, order)

	dst := make([]byte, 0, corpus.maxRowBytes)
	allocsPerPass := testing.AllocsPerRun(1, func() {
		var err error
		dst, err = runReplicatedApplyBatch64PointReadPass(corpus, order, dst)
		if err != nil {
			panic(err)
		}
	})
	if allocsPerPass != 0 {
		b.Fatalf("warmed all-row point pass allocated %.2f objects, want zero", allocsPerPass)
	}
	validateReplicatedApplyBatch64ReadOrder(b, corpus, order)

	before := corpus.collection.Stats()
	b.ReportAllocs()
	b.ResetTimer()
	b.StartTimer()
	started := time.Now()
	for range b.N {
		nextDst, readErr := runReplicatedApplyBatch64PointReadPass(corpus, order, dst)
		if readErr != nil {
			b.Fatal(readErr)
		}
		dst = nextDst
	}
	elapsed := time.Since(started)
	b.StopTimer()
	after := corpus.collection.Stats()
	validateReplicatedApplyBatch64ReadOrder(b, corpus, order)

	reads := float64(len(corpus.keys) * b.N)
	// The independent full-pass allocation check above found zero allocations;
	// b.ReportAllocs also records the timed pass's standard B/op and allocs/op.
	b.ReportMetric(float64(len(corpus.keys)), "rows/read-pass")
	b.ReportMetric(float64(elapsed.Nanoseconds())/reads, "ns/read")
	b.ReportMetric(0, "B/read")
	b.ReportMetric(allocsPerPass/float64(len(corpus.keys)), "allocs/read")
	b.ReportMetric(float64(after.PageReads-before.PageReads)/reads, "page-reads/read")
	b.ReportMetric(float64(after.ReadBytes-before.ReadBytes)/reads, "read-bytes/read")
	b.ReportMetric(float64(after.CacheHits-before.CacheHits)/reads, "cache-hits/read")
	b.ReportMetric(float64(after.CacheMisses-before.CacheMisses)/reads, "cache-misses/read")
	b.ReportMetric(float64(after.Evictions-before.Evictions)/reads, "evictions/read")
	b.ReportMetric(float64(after.ResidentBytes), "resident-bytes")
	b.ReportMetric(float64(after.PinnedPages), "pinned-pages")
	b.ReportMetric(float64(corpus.setupStats.PrimaryLeafSplits), "leaf-splits")
	b.ReportMetric(float64(corpus.setupStats.PrimaryEmptyLeaves), "empty-leaves")
	b.ReportMetric(float64(corpus.footprint.ApparentBytes), "final-apparent-file-B")
	b.ReportMetric(float64(corpus.footprint.AllocatedBytes), "final-allocated-file-B")
	b.ReportMetric(float64(corpus.footprint.Files), "final-files")
	b.Logf("reads=%d rows=%d leaf_splits=%d resident_bytes=%d cache_misses=%d page_reads=%d read_bytes=%d apparent_file_bytes=%d allocated_file_bytes=%d",
		int(reads), len(corpus.keys), corpus.setupStats.PrimaryLeafSplits,
		after.ResidentBytes, after.CacheMisses-before.CacheMisses,
		after.PageReads-before.PageReads, after.ReadBytes-before.ReadBytes,
		corpus.footprint.ApparentBytes, corpus.footprint.AllocatedBytes)
}

func runtimeReadCorpusMissingKey(
	tb testing.TB,
	corpus *replicatedApplyBatch64ReadCorpus,
	id string,
) []byte {
	tb.Helper()
	core := corpus.database.connector.db
	core.mu.RLock()
	table := core.tables[corpus.identity.UserTable]
	if table == nil || table.collection == nil {
		core.mu.RUnlock()
		tb.Fatal("runtime-read user table is unavailable")
	}
	raw := fmt.Appendf(nil,
		`{"id":%q,"bucket":0,"score":0,"payload":""}`, id)
	key, err := documentKey(
		raw, table.meta.PrimaryKey, table.primary,
		table.collection.MaxKeyBytes(),
	)
	core.mu.RUnlock()
	if err != nil {
		tb.Fatalf("encode missing runtime-read key %q: %v", id, err)
	}
	return []byte(key)
}

func TestReplicatedApplyBatch64RuntimePointReadBoundaries(t *testing.T) {
	const rows = batch64Rows * 2
	corpus := newReplicatedApplyBatch64ReadCorpus(
		t, rows, fillReplicatedApplyBatch64Rows,
	)
	validateReplicatedApplyBatch64ReadOrder(t, corpus, nil)
	validateReplicatedApplyBatch64ReadOrder(t, corpus, corpus.permutation)

	dst := make([]byte, 0, corpus.maxRowBytes)
	for _, ordinal := range []int{0, batch64Rows - 1, batch64Rows, rows - 1} {
		got, found, err := corpus.collection.AppendRaw(dst[:0], corpus.keys[ordinal])
		if err != nil || !found || !bytes.Equal(got, corpus.expected[ordinal]) {
			t.Fatalf("boundary row %d = %q/%v/%v, want %q",
				ordinal, got, found, err, corpus.expected[ordinal])
		}
		dst = got[:0]
	}
	for _, id := range []string{
		"aaa-before", "key-00000064-missing", "missing-after",
	} {
		key := runtimeReadCorpusMissingKey(t, corpus, id)
		got, found, err := corpus.collection.AppendRaw(dst[:0], key)
		if err != nil || found || len(got) != 0 {
			t.Fatalf("missing id %q = %q/%v/%v, want absent", id, got, found, err)
		}
	}
}
