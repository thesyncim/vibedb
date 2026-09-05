package driver

import (
	"fmt"
	"testing"
	"time"
)

// This benchmark measures the candidate prepared-only reuse API. Its fixture,
// SQL text, typed changing bindings, and row checks are shared with
// replicated_read_executor_bench_test.go; the fresh constructor/prepare arms
// remain in that frozen baseline benchmark. The first request for each arm is
// outside the timed region and proves that the next request acquires the same
// cached reader/prepared pair.
type replicatedReadExecutorReusePhaseTotals struct {
	acquire time.Duration
	execute time.Duration
	verify  time.Duration
	finish  time.Duration
}

// Keep the three identities captured before setup release. A cache slot can
// be reused for a miss, so checking the mutable slot fields against the slot
// itself would not prove that the retained reader and Prepared survived.
type replicatedReadExecutorReuseIdentity struct {
	slot     *replicatedReadReuseSlot
	reader   *ReplicatedReadSession
	prepared *Prepared
}

func (p replicatedReadExecutorReusePhaseTotals) total() time.Duration {
	return p.acquire + p.execute + p.verify + p.finish
}

func reportReplicatedReadExecutorReusePhases(
	b *testing.B,
	phases replicatedReadExecutorReusePhaseTotals,
) {
	b.StopTimer()
	if b.N == 0 {
		return
	}
	total := phases.total()
	n := float64(b.N)
	b.ReportMetric(float64(total.Nanoseconds())/n, "reuse-phases-ns/op")
	b.ReportMetric(float64(phases.acquire.Nanoseconds())/n, "reuse-acquire-ns/op")
	b.ReportMetric(float64(phases.execute.Nanoseconds())/n, "reuse-execute-ns/op")
	b.ReportMetric(float64(phases.verify.Nanoseconds())/n, "reuse-cursor-verify-ns/op")
	b.ReportMetric(float64(phases.finish.Nanoseconds())/n, "reuse-finish-ns/op")
	if total > 0 {
		b.ReportMetric(100*float64(phases.acquire)/float64(total), "reuse-acquire-pct")
		b.ReportMetric(100*float64(phases.execute)/float64(total), "reuse-execute-pct")
		b.ReportMetric(100*float64(phases.verify)/float64(total), "reuse-cursor-verify-pct")
		b.ReportMetric(100*float64(phases.finish)/float64(total), "reuse-finish-pct")
	}
}

func assertReplicatedReadReuseIdleBound(
	b testing.TB,
	f *replicatedReadExecutorBenchFixture,
	wantSlot *replicatedReadReuseSlot,
) {
	b.Helper()
	cache := f.claim.replicatedReadReuseCache()
	if cache == nil {
		b.Fatal("prepared-read reuse cache is nil")
	}
	cache.mu.Lock()
	retained := cache.retained
	idle := 0
	var found *replicatedReadReuseSlot
	for i := range cache.slots {
		slot := &cache.slots[i]
		if !slot.active && slot.reader != nil {
			idle++
			if slot == wantSlot {
				found = slot
			}
		}
	}
	cache.mu.Unlock()
	if retained <= 0 || retained > replicatedReadReuseMaxBytes {
		b.Fatalf("idle reuse retained bytes = %d, want (0,%d]", retained, replicatedReadReuseMaxBytes)
	}
	if idle == 0 || found == nil || found.retainedByte <= 0 {
		b.Fatalf("reuse cache has no retained idle slot: idle=%d found=%p slot=%p", idle, found, wantSlot)
	}
}

func finishReplicatedReadReuseSetup(
	b testing.TB,
	lease *ReplicatedReadLease,
	cursor *Cursor,
) replicatedReadExecutorReuseIdentity {
	b.Helper()
	if lease == nil || lease.slot == nil {
		b.Fatal("prepared-read setup returned no lease slot")
	}
	slot := lease.slot
	identity := replicatedReadExecutorReuseIdentity{
		slot: slot, reader: slot.reader, prepared: slot.prepared,
	}
	if cursor != nil {
		if err := cursor.Close(); err != nil {
			_ = lease.Abort(err)
			b.Fatalf("close setup cursor: %v", err)
		}
	}
	if err := lease.Finish(nil); err != nil {
		b.Fatalf("finish prepared-read setup: %v", err)
	}
	if !slot.cacheable || slot.reader == nil || slot.prepared == nil ||
		slot.retainedByte <= 0 || slot.active {
		b.Fatalf("setup did not leave a cached idle slot: cacheable=%v reader=%p prepared=%p retained=%d active=%v",
			slot.cacheable, slot.reader, slot.prepared, slot.retainedByte, slot.active)
	}
	return identity
}

func probeReplicatedReadReusePoint(
	b testing.TB,
	f *replicatedReadExecutorBenchFixture,
	hit bool,
	identity replicatedReadExecutorReuseIdentity,
) {
	b.Helper()
	id, key, raw, found, want := f.pointAt(0, hit)
	lease, err := f.claim.AcquireReplicatedPointRead(
		f.ctx, 1, key, found, raw, f.primaryPath,
		replicatedReadExecutorPointSQL, nil, false, f.options,
	)
	if err != nil {
		b.Fatalf("prepared point cache hit acquire: %v", err)
	}
	if lease == nil {
		b.Fatal("prepared point cache hit returned a nil lease")
	}
	if lease.slot != identity.slot || lease.slot.reader != identity.reader ||
		lease.slot.prepared != identity.prepared {
		_ = lease.Abort(ErrReplicatedReadLeaseClosed)
		b.Fatal("prepared point acquire did not hit the retained reader/prepared slot")
	}
	args := []any{id}
	keys := [][]byte{key}
	var cursor Cursor
	if err := lease.QueryCandidateKeysInto(
		f.ctx, args, f.primaryPath, keys, &cursor,
	); err != nil {
		_ = lease.Abort(err)
		b.Fatalf("prepared point cache hit query: %v", err)
	}
	validateReplicatedReadExecutorCursor(b, &cursor, want)
	if err := cursor.Close(); err != nil {
		_ = lease.Abort(err)
		b.Fatalf("close prepared point cache hit cursor: %v", err)
	}
	if err := lease.Finish(nil); err != nil {
		b.Fatalf("finish prepared point cache hit: %v", err)
	}
}

func probeReplicatedReadReuseRange(
	b testing.TB,
	f *replicatedReadExecutorBenchFixture,
	rows int,
	want replicatedReadExecutorReuseIdentity,
) {
	b.Helper()
	rangeSQL := replicatedReadExecutorRangeSQL(rows)
	args := []any{f.rangeIDs[0]}
	lease, err := f.claim.AcquireReplicatedDataRead(
		f.ctx, &f.cut, rangeSQL, nil, false, f.options,
	)
	if err != nil {
		b.Fatalf("prepared range cache hit acquire: %v", err)
	}
	if lease == nil {
		b.Fatal("prepared range cache hit returned a nil lease")
	}
	if lease.slot != want.slot || lease.slot.reader != want.reader ||
		lease.slot.prepared != want.prepared {
		_ = lease.Abort(ErrReplicatedReadLeaseClosed)
		b.Fatal("prepared range acquire did not hit the retained reader/prepared slot")
	}
	var cursor Cursor
	if err := lease.QueryInto(f.ctx, args, &cursor); err != nil {
		_ = lease.Abort(err)
		b.Fatalf("prepared range cache hit query: %v", err)
	}
	validateReplicatedReadExecutorCursor(b, &cursor, f.rangeExpected[:rows])
	validateReplicatedReadExecutorRangeStats(b, lease.slot.reader, rows)
	if err := cursor.Close(); err != nil {
		_ = lease.Abort(err)
		b.Fatalf("close prepared range cache hit cursor: %v", err)
	}
	if err := lease.Finish(nil); err != nil {
		b.Fatalf("finish prepared range cache hit: %v", err)
	}
}

func runReplicatedReadExecutorPreparedReusePoint(
	b *testing.B,
	f *replicatedReadExecutorBenchFixture,
	hit bool,
) {
	b.Helper()
	id, key, raw, found, want := f.pointAt(0, hit)
	lease, err := f.claim.AcquireReplicatedPointRead(
		f.ctx, 1, key, found, raw, f.primaryPath,
		replicatedReadExecutorPointSQL, nil, false, f.options,
	)
	if err != nil {
		b.Fatal(err)
	}
	args := []any{id}
	keys := [][]byte{key}
	var cursor Cursor
	if err := lease.QueryCandidateKeysInto(
		f.ctx, args, f.primaryPath, keys, &cursor,
	); err != nil {
		_ = lease.Abort(err)
		b.Fatal(err)
	}
	validateReplicatedReadExecutorCursor(b, &cursor, want)
	identity := finishReplicatedReadReuseSetup(b, lease, &cursor)
	probeReplicatedReadReusePoint(b, f, hit, identity)
	assertReplicatedReadReuseIdleBound(b, f, identity.slot)

	b.ReportAllocs()
	var phases replicatedReadExecutorReusePhaseTotals
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		id, key, raw, found, want := f.pointAt(iteration+1, hit)
		args[0] = id
		keys[0] = key
		started := time.Now()
		lease, err := f.claim.AcquireReplicatedPointRead(
			f.ctx, 1, key, found, raw, f.primaryPath,
			replicatedReadExecutorPointSQL, nil, false, f.options,
		)
		phases.acquire += time.Since(started)
		if err != nil {
			b.Fatal(err)
		}
		if lease == nil {
			b.Fatal("prepared point candidate returned a nil lease")
		}
		if lease.slot != identity.slot || lease.slot.reader != identity.reader ||
			lease.slot.prepared != identity.prepared {
			_ = lease.Abort(ErrReplicatedReadLeaseClosed)
			b.Fatal("prepared point candidate changed retained slot, reader, or prepared identity")
		}

		started = time.Now()
		var cursor Cursor
		err = lease.QueryCandidateKeysInto(
			f.ctx, args, f.primaryPath, keys, &cursor,
		)
		phases.execute += time.Since(started)
		b.StopTimer()
		if err == nil {
			started = time.Now()
			validateReplicatedReadExecutorCursor(b, &cursor, want)
			phases.verify += time.Since(started)
		}
		if err != nil {
			_ = lease.Abort(err)
			b.Fatal(err)
		}

		b.StartTimer()
		started = time.Now()
		if err := cursor.Close(); err != nil {
			_ = lease.Abort(err)
			b.Fatal(err)
		}
		if err := lease.Finish(nil); err != nil {
			b.Fatal(err)
		}
		phases.finish += time.Since(started)
	}
	b.StopTimer()
	assertReplicatedReadReuseIdleBound(b, f, identity.slot)
	reportReplicatedReadExecutorReusePhases(b, phases)
}

func runReplicatedReadExecutorPreparedReuseRange(
	b *testing.B,
	f *replicatedReadExecutorBenchFixture,
	rows int,
) {
	b.Helper()
	rangeSQL := replicatedReadExecutorRangeSQL(rows)
	args := []any{f.rangeIDs[0]}
	lease, err := f.claim.AcquireReplicatedDataRead(
		f.ctx, &f.cut, rangeSQL, nil, false, f.options,
	)
	if err != nil {
		b.Fatal(err)
	}
	var cursor Cursor
	if err := lease.QueryInto(f.ctx, args, &cursor); err != nil {
		_ = lease.Abort(err)
		b.Fatal(err)
	}
	validateReplicatedReadExecutorCursor(b, &cursor, f.rangeExpected[:rows])
	validateReplicatedReadExecutorRangeStats(b, lease.slot.reader, rows)
	identity := finishReplicatedReadReuseSetup(b, lease, &cursor)
	probeReplicatedReadReuseRange(b, f, rows, identity)
	assertReplicatedReadReuseIdleBound(b, f, identity.slot)

	b.ReportAllocs()
	var phases replicatedReadExecutorReusePhaseTotals
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		startRow := (iteration + 1) % (len(f.rangeExpected) / rows) * rows
		args[0] = f.rangeIDs[startRow]
		want := f.rangeExpected[startRow : startRow+rows]
		started := time.Now()
		lease, err := f.claim.AcquireReplicatedDataRead(
			f.ctx, &f.cut, rangeSQL, nil, false, f.options,
		)
		phases.acquire += time.Since(started)
		if err != nil {
			b.Fatal(err)
		}
		if lease == nil {
			b.Fatal("prepared range candidate returned a nil lease")
		}
		if lease.slot != identity.slot || lease.slot.reader != identity.reader ||
			lease.slot.prepared != identity.prepared {
			_ = lease.Abort(ErrReplicatedReadLeaseClosed)
			b.Fatal("prepared range candidate changed retained slot, reader, or prepared identity")
		}

		started = time.Now()
		var cursor Cursor
		err = lease.QueryInto(f.ctx, args, &cursor)
		phases.execute += time.Since(started)
		b.StopTimer()
		if err == nil {
			started = time.Now()
			validateReplicatedReadExecutorCursor(b, &cursor, want)
			validateReplicatedReadExecutorRangeStats(b, lease.slot.reader, rows)
			phases.verify += time.Since(started)
		}
		if err != nil {
			_ = lease.Abort(err)
			b.Fatal(err)
		}

		b.StartTimer()
		started = time.Now()
		if err := cursor.Close(); err != nil {
			_ = lease.Abort(err)
			b.Fatal(err)
		}
		if err := lease.Finish(nil); err != nil {
			b.Fatal(err)
		}
		phases.finish += time.Since(started)
	}
	b.StopTimer()
	assertReplicatedReadReuseIdleBound(b, f, identity.slot)
	reportReplicatedReadExecutorReusePhases(b, phases)
}

// BenchmarkReplicatedReadExecutorPreparedReuse measures cache-hit prepared
// reuse. Acquire includes the fresh cut/session attachment and cache lookup;
// after the one-time setup it does not prepare SQL. The cursor verification
// phase consumes and checks every returned JSON cell (and range stats), with
// the Go benchmark timer stopped; it is not a pure encoding measurement or
// PostgreSQL frame encoding. These calls intentionally pass nil explicit
// parameter descriptors: Go string bindings provide typed values, but this
// benchmark makes no wire-OID/descriptor-identity claim (that is covered by
// the separate regression/RF3 workload).
func BenchmarkReplicatedReadExecutorPreparedReuse(b *testing.B) {
	f := newReplicatedReadExecutorBenchFixture(b)
	b.Run("point_hit/prepared_reuse", func(b *testing.B) {
		runReplicatedReadExecutorPreparedReusePoint(b, f, true)
	})
	b.Run("point_miss/prepared_reuse", func(b *testing.B) {
		runReplicatedReadExecutorPreparedReusePoint(b, f, false)
	})
	for _, rows := range []int{32, 64, 256} {
		rows := rows
		b.Run(fmt.Sprintf("range_%d/prepared_reuse", rows), func(b *testing.B) {
			runReplicatedReadExecutorPreparedReuseRange(b, f, rows)
		})
	}
}
