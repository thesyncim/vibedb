package driver

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/query"
)

// BenchmarkReplicatedReadExecutor measures the SQL boundary used by the
// replicated shard read caller. The immutable cut and all durable setup are
// built before the sub-benchmarks start; fresh runs still construct, prepare,
// execute, and close one session per operation. The warm runs are an explicit
// upper bound for a future bounded session/statement cache.
const (
	replicatedReadExecutorPointSQL = `SELECT id,bucket,score,payload FROM docs WHERE id = ?`
)

func replicatedReadExecutorRangeSQL(rows int) string {
	return fmt.Sprintf(
		"SELECT id,score FROM docs WHERE id >= ? ORDER BY id LIMIT %d", rows,
	)
}

type replicatedReadExecutorExpectedRow struct {
	cells   [4][]byte
	columns int
}

type replicatedReadExecutorBenchFixture struct {
	claim       *ReplicatedApply
	base        ReplicatedShardStoreIdentity
	cut         replicatedstate.DataReadCut
	ctx         context.Context
	options     query.ExecOptions
	primaryPath []byte

	pointHitIDs   []string
	pointHitKeys  [][]byte
	pointHitRaws  [][]byte
	pointMissIDs  []string
	pointMissKeys [][]byte

	expected      []replicatedReadExecutorExpectedRow
	rangeExpected []replicatedReadExecutorExpectedRow
	rangeIDs      []string
}

func newReplicatedReadExecutorBenchFixture(b *testing.B) *replicatedReadExecutorBenchFixture {
	b.Helper()
	_, database, binding, _ := prepareReplicatedTestRoot(
		b, "sql-replicated-read-executor-bench", false,
	)
	b.Cleanup(func() {
		if err := database.Close(); err != nil {
			b.Errorf("close database: %v", err)
		}
	})
	base := requireReplicatedShardStoreBind(b, database, binding, "docs")
	bootstrap := testReplicatedApplyBootstrap()
	claim, _, err := database.OpenReplicatedApply(
		base, bootstrap, testReplicatedApplyOptions(),
	)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := claim.Close(); err != nil {
			b.Errorf("close replicated apply: %v", err)
		}
	})
	if _, err := claim.InstallSnapshot(bootstrap); err != nil {
		b.Fatal(err)
	}
	epoch := benchmarkReplicatedApplySessionOpen(b, claim, base, 2)
	want := replicatedProjectedFieldRowsWant()
	lastApplied := benchmarkApplyReplicatedProjectedFieldRows(
		b, database, claim, base, epoch, 2, want,
	)
	var cut replicatedstate.DataReadCut
	if err := claim.DataReadCutInto(nil, lastApplied, &cut); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := cut.Close(); err != nil {
			b.Errorf("close data read cut: %v", err)
		}
	})

	hitKey := benchmarkReplicatedApplyKey(b, database, want[0].doc)
	pointRead, err := claim.PointReadInto(
		1, hitKey, lastApplied, base.UserLimits.MaxDocumentBytes, nil,
	)
	if err != nil || !pointRead.Found {
		b.Fatalf("point fixture read = %+v, %v", pointRead, err)
	}

	expected := make([]replicatedReadExecutorExpectedRow, len(want))
	rangeExpected := make([]replicatedReadExecutorExpectedRow, len(want))
	rangeIDs := make([]string, len(want))
	for row := range want {
		expected[row].columns = 4
		for column, value := range want[row].values {
			expected[row].cells[column] = []byte(value)
		}
		rangeIDs[row] = want[row].id
		rangeExpected[row].columns = 2
		rangeExpected[row].cells[0] = []byte(want[row].values[0])
		rangeExpected[row].cells[1] = []byte(want[row].values[2])
	}
	const pointVariants = 64
	pointHitIDs := make([]string, pointVariants)
	pointHitKeys := make([][]byte, pointVariants)
	pointHitRaws := make([][]byte, pointVariants)
	pointMissIDs := make([]string, pointVariants)
	pointMissKeys := make([][]byte, pointVariants)
	for variant := range pointVariants {
		row := (variant * 127) % len(want)
		pointHitIDs[variant] = want[row].id
		pointHitKeys[variant] = benchmarkReplicatedApplyKey(b, database, want[row].doc)
		pointHitRaws[variant] = append([]byte(nil), want[row].doc...)
		pointMissIDs[variant] = fmt.Sprintf("row-%08d", len(want)+variant)
		pointMissKeys[variant] = benchmarkReplicatedApplyKey(
			b, database, []byte(fmt.Sprintf(`{"id":%q}`, pointMissIDs[variant])),
		)
	}
	var cancel query.CancelFlag
	return &replicatedReadExecutorBenchFixture{
		claim: claim,
		base:  base,
		cut:   cut,
		ctx:   context.Background(),
		options: query.ExecOptions{
			Workers:           1,
			BatchRows:         256,
			BatchBytes:        128 << 10,
			MemoryBytes:       32 << 20,
			Cancel:            &cancel,
			ResultRows:        512,
			ResultBytes:       512 << 10,
			IntermediateBytes: 8 << 20,
		},
		primaryPath:   []byte(base.UserPrimaryKey),
		pointHitIDs:   pointHitIDs,
		pointHitKeys:  pointHitKeys,
		pointHitRaws:  pointHitRaws,
		pointMissIDs:  pointMissIDs,
		pointMissKeys: pointMissKeys,
		expected:      expected,
		rangeExpected: rangeExpected,
		rangeIDs:      rangeIDs,
	}
}

func benchmarkReplicatedApplySessionOpen(
	t testing.TB,
	claim *ReplicatedApply,
	identity ReplicatedShardStoreIdentity,
	index uint64,
) uint64 {
	t.Helper()
	command := testReplicatedApplySessionOpen(identity)
	if err := claim.AdmitCommand(command); err != nil {
		t.Fatalf("AdmitCommand session open at %d: %v", index, err)
	}
	publication, err := claim.ApplyNormal(testReplicatedApplyMeta(index), command)
	if err != nil || publication.Applied != index {
		t.Fatalf("ApplyNormal session open at %d = %+v, %v", index, publication, err)
	}
	lookup, err := claim.LookupCompletion(command)
	if err != nil {
		t.Fatalf("LookupCompletion session open at %d: %v", index, err)
	}
	completion, err := replication.OpenCompletion(lookup.Bytes)
	if err != nil || completion.ResultCode != replicatedstate.ResultSessionOpened ||
		completion.ClientEpoch != index || completion.ClientSequence != 1 ||
		completion.AppliedSequence != index {
		t.Fatalf("session open completion at %d = %+v, %v", index, completion, err)
	}
	return completion.ClientEpoch
}

func benchmarkReplicatedApplyKey(
	t testing.TB,
	database *Database,
	document []byte,
) []byte {
	t.Helper()
	core := database.connector.db
	core.mu.RLock()
	table := core.tables["docs"]
	key, err := documentKey(
		document, table.meta.PrimaryKey, table.primary, table.collection.MaxKeyBytes(),
	)
	core.mu.RUnlock()
	if err != nil {
		t.Fatalf("documentKey(%s): %v", document, err)
	}
	return []byte(key)
}

func benchmarkApplyReplicatedProjectedFieldRows(
	t testing.TB,
	database *Database,
	claim *ReplicatedApply,
	base ReplicatedShardStoreIdentity,
	epoch, startIndex uint64,
	want []replicatedProjectedFieldRow,
) uint64 {
	t.Helper()
	const batchSize = 64
	last := startIndex
	for start := 0; start < len(want); start += batchSize {
		end := min(start+batchSize, len(want))
		mutations := make([]replication.Mutation, 0, end-start)
		for row := start; row < end; row++ {
			document := want[row].doc
			mutations = append(mutations, replication.Mutation{
				Kind:  replication.MutationPut,
				Key:   benchmarkReplicatedApplyKey(t, database, document),
				Value: document,
			})
		}
		index := startIndex + uint64(start/batchSize) + 1
		publication, err := claim.ApplyNormal(
			testReplicatedApplyMeta(index),
			testReplicatedApplyCommand(
				base, epoch, uint64(start/batchSize)+2, mutations...,
			),
		)
		if err != nil || publication.Applied != index {
			t.Fatalf("apply projection batch %d = %+v, %v; want applied %d",
				start/batchSize, publication, err, index)
		}
		last = index
	}
	return last
}

type replicatedReadExecutorPhaseTotals struct {
	construct time.Duration
	prepare   time.Duration
	execute   time.Duration
	encode    time.Duration
	close     time.Duration
}

func (p replicatedReadExecutorPhaseTotals) total() time.Duration {
	return p.construct + p.prepare + p.execute + p.encode + p.close
}

func reportReplicatedReadExecutorPhases(
	b *testing.B,
	phases replicatedReadExecutorPhaseTotals,
	includeConstructPrepare bool,
) {
	b.StopTimer()
	n := float64(b.N)
	if n == 0 {
		return
	}
	total := phases.total()
	b.ReportMetric(float64(total.Nanoseconds())/n, "phases-ns/op")
	b.ReportMetric(float64(phases.execute.Nanoseconds())/n, "execute-ns/op")
	b.ReportMetric(float64(phases.encode.Nanoseconds())/n, "encode-ns/op")
	b.ReportMetric(float64(phases.close.Nanoseconds())/n, "close-ns/op")
	if includeConstructPrepare {
		b.ReportMetric(float64(phases.construct.Nanoseconds())/n, "construct-ns/op")
		b.ReportMetric(float64(phases.prepare.Nanoseconds())/n, "prepare-ns/op")
	}
	if total > 0 {
		b.ReportMetric(100*float64(phases.execute)/float64(total), "execute-pct")
		b.ReportMetric(100*float64(phases.encode)/float64(total), "encode-pct")
		b.ReportMetric(100*float64(phases.close)/float64(total), "close-pct")
		if includeConstructPrepare {
			b.ReportMetric(100*float64(phases.construct)/float64(total), "construct-pct")
			b.ReportMetric(100*float64(phases.prepare)/float64(total), "prepare-pct")
		}
	}
}

func validateReplicatedReadExecutorCursor(
	t testing.TB,
	cursor *Cursor,
	want []replicatedReadExecutorExpectedRow,
) {
	t.Helper()
	row := 0
	for cursor.Next() {
		if row >= len(want) {
			t.Fatalf("returned more than %d rows", len(want))
		}
		for column, expected := range want[row].cells[:want[row].columns] {
			if got := cursor.Cell(column).JSON(); !bytes.Equal(got, expected) {
				t.Fatalf("row %d column %d = %s, want %s", row, column, got, expected)
			}
		}
		row++
	}
	if row != len(want) {
		t.Fatalf("returned %d rows, want %d", row, len(want))
	}
}

func validateReplicatedReadExecutorRangeStats(
	t testing.TB,
	reader *ReplicatedReadSession,
	rows int,
) {
	t.Helper()
	stats := reader.conn.exec.Stats
	if !stats.PrimaryRangeBounded || stats.ProjectedRows != uint64(rows) {
		t.Fatalf("range stats = %+v, want bounded projected %d-row read", stats, rows)
	}
}

func (f *replicatedReadExecutorBenchFixture) rangeArgs() []any {
	return []any{f.rangeIDs[0]}
}

func (f *replicatedReadExecutorBenchFixture) pointAt(
	iteration int,
	hit bool,
) (string, []byte, []byte, bool, []replicatedReadExecutorExpectedRow) {
	if iteration < 0 {
		iteration = 0
	}
	if hit {
		variant := iteration % len(f.pointHitIDs)
		row := (variant * 127) % len(f.expected)
		return f.pointHitIDs[variant], f.pointHitKeys[variant],
			f.pointHitRaws[variant], true, f.expected[row : row+1]
	}
	variant := iteration % len(f.pointMissIDs)
	return f.pointMissIDs[variant], f.pointMissKeys[variant], nil, false, nil
}

func runReplicatedReadExecutorFreshRange(
	b *testing.B,
	f *replicatedReadExecutorBenchFixture,
	rows int,
) {
	b.Helper()
	b.ReportAllocs()
	args := f.rangeArgs()
	var phases replicatedReadExecutorPhaseTotals
	rangeSQL := replicatedReadExecutorRangeSQL(rows)
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		startRow := (iteration % (len(f.rangeExpected) / rows)) * rows
		args[0] = f.rangeIDs[startRow]
		want := f.rangeExpected[startRow : startRow+rows]
		started := time.Now()
		reader, err := f.claim.NewDataReadSession(f.ctx, &f.cut, f.options)
		phases.construct += time.Since(started)
		if err != nil {
			b.Fatal(err)
		}

		started = time.Now()
		prepared, err := reader.Prepare(f.ctx, rangeSQL)
		phases.prepare += time.Since(started)
		if err != nil {
			_ = reader.Close()
			b.Fatal(err)
		}

		started = time.Now()
		var cursor Cursor
		err = prepared.QueryInto(f.ctx, args, &cursor)
		phases.execute += time.Since(started)
		b.StopTimer()
		if err == nil {
			started = time.Now()
			validateReplicatedReadExecutorCursor(b, &cursor, want)
			validateReplicatedReadExecutorRangeStats(b, reader, rows)
			phases.encode += time.Since(started)
		}
		if err != nil {
			_ = prepared.Close()
			_ = reader.Close()
			b.Fatal(err)
		}

		b.StartTimer()
		started = time.Now()
		if err := cursor.Close(); err != nil {
			_ = prepared.Close()
			_ = reader.Close()
			b.Fatal(err)
		}
		if err := prepared.Close(); err != nil {
			_ = reader.Close()
			b.Fatal(err)
		}
		if err := reader.Close(); err != nil {
			b.Fatal(err)
		}
		phases.close += time.Since(started)
	}
	reportReplicatedReadExecutorPhases(b, phases, true)
}

func runReplicatedReadExecutorFreshPoint(
	b *testing.B,
	f *replicatedReadExecutorBenchFixture,
	hit bool,
) {
	b.Helper()
	b.ReportAllocs()
	args := []any{nil}
	keys := [][]byte{nil}
	var phases replicatedReadExecutorPhaseTotals
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		id, key, raw, found, want := f.pointAt(iteration, hit)
		args[0] = id
		keys[0] = key
		started := time.Now()
		reader, err := f.claim.NewPointReadSession(
			f.ctx, 1, key, found, raw, f.primaryPath, f.options,
		)
		phases.construct += time.Since(started)
		if err != nil {
			b.Fatal(err)
		}

		started = time.Now()
		prepared, err := reader.Prepare(f.ctx, replicatedReadExecutorPointSQL)
		phases.prepare += time.Since(started)
		if err != nil {
			_ = reader.Close()
			b.Fatal(err)
		}

		started = time.Now()
		var cursor Cursor
		err = prepared.QueryCandidateKeysInto(
			f.ctx, args, f.primaryPath, keys, &cursor,
		)
		phases.execute += time.Since(started)
		b.StopTimer()
		if err == nil {
			started = time.Now()
			validateReplicatedReadExecutorCursor(b, &cursor, want)
			phases.encode += time.Since(started)
		}
		if err != nil {
			_ = prepared.Close()
			_ = reader.Close()
			b.Fatal(err)
		}

		b.StartTimer()
		started = time.Now()
		if err := cursor.Close(); err != nil {
			_ = prepared.Close()
			_ = reader.Close()
			b.Fatal(err)
		}
		if err := prepared.Close(); err != nil {
			_ = reader.Close()
			b.Fatal(err)
		}
		if err := reader.Close(); err != nil {
			b.Fatal(err)
		}
		phases.close += time.Since(started)
	}
	reportReplicatedReadExecutorPhases(b, phases, true)
}

func runReplicatedReadExecutorWarmRange(
	b *testing.B,
	f *replicatedReadExecutorBenchFixture,
	rows int,
) {
	b.Helper()
	reader, err := f.claim.NewDataReadSession(f.ctx, &f.cut, f.options)
	if err != nil {
		b.Fatal(err)
	}
	rangeSQL := replicatedReadExecutorRangeSQL(rows)
	prepared, err := reader.Prepare(f.ctx, rangeSQL)
	if err != nil {
		_ = reader.Close()
		b.Fatal(err)
	}
	args := f.rangeArgs()
	want := f.rangeExpected[:rows]
	primeReplicatedReadExecutorRange(b, reader, prepared, args, want, rows)

	b.ReportAllocs()
	var phases replicatedReadExecutorPhaseTotals
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		startRow := (iteration % (len(f.rangeExpected) / rows)) * rows
		args[0] = f.rangeIDs[startRow]
		want := f.rangeExpected[startRow : startRow+rows]
		started := time.Now()
		var cursor Cursor
		err := prepared.QueryInto(f.ctx, args, &cursor)
		phases.execute += time.Since(started)
		b.StopTimer()
		if err == nil {
			started = time.Now()
			validateReplicatedReadExecutorCursor(b, &cursor, want)
			validateReplicatedReadExecutorRangeStats(b, reader, rows)
			phases.encode += time.Since(started)
		}
		if err != nil {
			_ = prepared.Close()
			_ = reader.Close()
			b.Fatal(err)
		}
		b.StartTimer()
		started = time.Now()
		if err := cursor.Close(); err != nil {
			_ = prepared.Close()
			_ = reader.Close()
			b.Fatal(err)
		}
		phases.close += time.Since(started)
	}
	b.StopTimer()
	if err := prepared.Close(); err != nil {
		_ = reader.Close()
		b.Fatal(err)
	}
	if err := reader.Close(); err != nil {
		b.Fatal(err)
	}
	reportReplicatedReadExecutorPhases(b, phases, false)
}

func runReplicatedReadExecutorWarmPoint(
	b *testing.B,
	f *replicatedReadExecutorBenchFixture,
	hit bool,
) {
	b.Helper()
	id, key, raw, found, want := f.pointAt(0, hit)
	args := []any{id}
	keys := [][]byte{key}
	reader, err := f.claim.NewPointReadSession(
		f.ctx, 1, key, found, raw, f.primaryPath, f.options,
	)
	if err != nil {
		b.Fatal(err)
	}
	prepared, err := reader.Prepare(f.ctx, replicatedReadExecutorPointSQL)
	if err != nil {
		_ = reader.Close()
		b.Fatal(err)
	}
	primeReplicatedReadExecutorPoint(b, reader, prepared, f.primaryPath, args, keys, want)

	b.ReportAllocs()
	var phases replicatedReadExecutorPhaseTotals
	b.ResetTimer()
	for range b.N {
		started := time.Now()
		var cursor Cursor
		err := prepared.QueryCandidateKeysInto(
			f.ctx, args, f.primaryPath, keys, &cursor,
		)
		phases.execute += time.Since(started)
		b.StopTimer()
		if err == nil {
			started = time.Now()
			validateReplicatedReadExecutorCursor(b, &cursor, want)
			phases.encode += time.Since(started)
		}
		if err != nil {
			_ = prepared.Close()
			_ = reader.Close()
			b.Fatal(err)
		}
		b.StartTimer()
		started = time.Now()
		if err := cursor.Close(); err != nil {
			_ = prepared.Close()
			_ = reader.Close()
			b.Fatal(err)
		}
		phases.close += time.Since(started)
	}
	b.StopTimer()
	if err := prepared.Close(); err != nil {
		_ = reader.Close()
		b.Fatal(err)
	}
	if err := reader.Close(); err != nil {
		b.Fatal(err)
	}
	reportReplicatedReadExecutorPhases(b, phases, false)
}

func primeReplicatedReadExecutorRange(
	b testing.TB,
	reader *ReplicatedReadSession,
	prepared *Prepared,
	args []any,
	want []replicatedReadExecutorExpectedRow,
	rows int,
) {
	b.Helper()
	var cursor Cursor
	if err := prepared.QueryInto(context.Background(), args, &cursor); err != nil {
		b.Fatal(err)
	}
	validateReplicatedReadExecutorCursor(b, &cursor, want)
	validateReplicatedReadExecutorRangeStats(b, reader, rows)
	if err := cursor.Close(); err != nil {
		b.Fatal(err)
	}
}

func primeReplicatedReadExecutorPoint(
	b testing.TB,
	reader *ReplicatedReadSession,
	prepared *Prepared,
	primaryPath []byte,
	args []any,
	keys [][]byte,
	want []replicatedReadExecutorExpectedRow,
) {
	b.Helper()
	var cursor Cursor
	if err := prepared.QueryCandidateKeysInto(
		context.Background(), args, primaryPath, keys, &cursor,
	); err != nil {
		b.Fatal(err)
	}
	validateReplicatedReadExecutorCursor(b, &cursor, want)
	if err := cursor.Close(); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkReplicatedReadExecutor(b *testing.B) {
	f := newReplicatedReadExecutorBenchFixture(b)
	b.Run("point_hit/fresh", func(b *testing.B) {
		runReplicatedReadExecutorFreshPoint(b, f, true)
	})
	b.Run("point_hit/warm_prepared_session_upper_bound", func(b *testing.B) {
		runReplicatedReadExecutorWarmPoint(b, f, true)
	})
	b.Run("point_miss/fresh", func(b *testing.B) {
		runReplicatedReadExecutorFreshPoint(b, f, false)
	})
	b.Run("point_miss/warm_prepared_session_upper_bound", func(b *testing.B) {
		runReplicatedReadExecutorWarmPoint(b, f, false)
	})
	for _, rows := range []int{32, 64, 256} {
		rows := rows
		b.Run(fmt.Sprintf("range_%d/fresh", rows), func(b *testing.B) {
			runReplicatedReadExecutorFreshRange(b, f, rows)
		})
		b.Run(fmt.Sprintf("range_%d/warm_prepared_session_upper_bound", rows), func(b *testing.B) {
			runReplicatedReadExecutorWarmRange(b, f, rows)
		})
	}
}
