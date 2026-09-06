package driver

import (
	"context"
	"github.com/thesyncim/vibedb/query"
	"testing"
)

// This benchmark is a diagnostic CPU loop for the replicated point-read
// caller. Every timed iteration includes cache Acquire, candidate-key SQL
// execution, complete cursor JSON verification, cursor Close, and lease
// Finish. Verification is intentionally inside the timer: the loop measures
// the local driver API's end-to-end work, not a wire encoder or a competitive
// distributed throughput result.
type replicatedPointCPUIdentity struct {
	slot     *replicatedReadReuseSlot
	reader   *ReplicatedReadSession
	prepared *Prepared
}

func runReplicatedPointCPU(
	b *testing.B,
	f *replicatedReadExecutorBenchFixture,
	hit bool,
) {
	b.Helper()

	// Install and verify one cached prepared slot before timing. The probe uses
	// the next changing key/value, so the timed loop starts with a proven cache
	// hit while still exercising alternating point documents and misses.
	id, key, raw, found, want := f.pointAt(0, hit)
	lease, err := f.claim.AcquireReplicatedPointRead(
		f.ctx, 1, key, found, raw, f.primaryPath,
		replicatedReadExecutorPointSQL, nil, false, f.options,
	)
	if err != nil {
		b.Fatal(err)
	}
	if lease == nil || lease.slot == nil {
		b.Fatal("point CPU setup returned no lease slot")
	}
	identity := replicatedPointCPUIdentity{
		slot: lease.slot, reader: lease.slot.reader, prepared: lease.slot.prepared,
	}
	var cursor Cursor
	if err := lease.QueryCandidateKeysInto(
		f.ctx, []any{id}, f.primaryPath, [][]byte{key}, &cursor,
	); err != nil {
		_ = lease.Abort(err)
		b.Fatal(err)
	}
	validateReplicatedReadExecutorCursor(b, &cursor, want)
	if err := cursor.Close(); err != nil {
		_ = lease.Abort(err)
		b.Fatal(err)
	}
	if err := lease.Finish(nil); err != nil {
		b.Fatal(err)
	}
	if identity.reader == nil || identity.prepared == nil {
		b.Fatal("point CPU setup did not retain reader/prepared")
	}

	id, key, raw, found, want = f.pointAt(1, hit)
	lease, err = f.claim.AcquireReplicatedPointRead(
		f.ctx, 1, key, found, raw, f.primaryPath,
		replicatedReadExecutorPointSQL, nil, false, f.options,
	)
	if err != nil {
		b.Fatal(err)
	}
	if lease == nil || lease.slot != identity.slot ||
		lease.slot.reader != identity.reader || lease.slot.prepared != identity.prepared {
		if lease != nil {
			_ = lease.Abort(ErrReplicatedReadLeaseClosed)
		}
		b.Fatal("point CPU probe did not reuse the prepared reader identity")
	}
	cursor = Cursor{}
	if err := lease.QueryCandidateKeysInto(
		f.ctx, []any{id}, f.primaryPath, [][]byte{key}, &cursor,
	); err != nil {
		_ = lease.Abort(err)
		b.Fatal(err)
	}
	validateReplicatedReadExecutorCursor(b, &cursor, want)
	if err := cursor.Close(); err != nil {
		_ = lease.Abort(err)
		b.Fatal(err)
	}
	if err := lease.Finish(nil); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		id, key, raw, found, want = f.pointAt(iteration+2, hit)
		lease, err := f.claim.AcquireReplicatedPointRead(
			f.ctx, 1, key, found, raw, f.primaryPath,
			replicatedReadExecutorPointSQL, nil, false, f.options,
		)
		if err != nil {
			b.Fatal(err)
		}
		if lease == nil || lease.slot != identity.slot ||
			lease.slot.reader != identity.reader || lease.slot.prepared != identity.prepared {
			if lease != nil {
				_ = lease.Abort(ErrReplicatedReadLeaseClosed)
			}
			b.Fatal("point CPU acquire changed the retained reader identity")
		}

		var cursor Cursor
		err = lease.QueryCandidateKeysInto(
			f.ctx, []any{id}, f.primaryPath, [][]byte{key}, &cursor,
		)
		if err != nil {
			_ = lease.Abort(err)
			b.Fatal(err)
		}
		validateReplicatedReadExecutorCursor(b, &cursor, want)
		if err := cursor.Close(); err != nil {
			_ = lease.Abort(err)
			b.Fatal(err)
		}
		if err := lease.Finish(nil); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	stats := f.claim.replicatedReadReuseStats()
	if stats.RetainedSlots == 0 || stats.RetainedBytes <= 0 {
		b.Fatalf("point CPU loop left no retained prepared slot: %+v", stats)
	}
}

func BenchmarkReplicatedPointReadCPU(b *testing.B) {
	f := newReplicatedReadExecutorBenchFixture(b)
	b.Run("point_hit", func(b *testing.B) {
		runReplicatedPointCPU(b, f, true)
	})
	b.Run("point_miss", func(b *testing.B) {
		runReplicatedPointCPU(b, f, false)
	})
}

// This uses reusable caller-owned handles and preboxed arguments, as a server
// request workspace can. Every operation still acquires a fresh point, binds
// its arguments, executes, checks every returned cell, closes, and finishes.
func BenchmarkReplicatedPointReadInto(b *testing.B) {
	f := newReplicatedReadExecutorBenchFixture(b)
	for _, hit := range []bool{true, false} {
		name := "point_miss"
		if hit {
			name = "point_hit"
		}
		b.Run(name, func(b *testing.B) {
			run := replicatedPointIntoLoop(b, f, hit)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				run(i)
			}
		})
	}
}

func replicatedPointIntoLoop(t testing.TB, f *replicatedReadExecutorBenchFixture, hit bool) func(int) {
	t.Helper()
	type input struct {
		args []any
		keys [][]byte
		raw  []byte
		want []replicatedReadExecutorExpectedRow
	}
	inputs := make([]input, len(f.pointHitIDs))
	if !hit {
		inputs = make([]input, len(f.pointMissIDs))
	}
	for i := range inputs {
		id, key, raw, _, want := f.pointAt(i, hit)
		inputs[i] = input{[]any{id}, [][]byte{key}, raw, want}
	}
	var lease ReplicatedReadLease
	var cursor Cursor
	run := func(i int) {
		in := &inputs[i%len(inputs)]
		if err := f.claim.AcquireReplicatedPointReadInto(f.ctx, 1, in.keys[0], hit, in.raw,
			f.primaryPath, replicatedReadExecutorPointSQL, nil, false, f.options, &lease); err != nil {
			t.Fatal(err)
		}
		if err := lease.QueryCandidateKeysInto(f.ctx, in.args, f.primaryPath, in.keys, &cursor); err != nil {
			t.Fatal(err)
		}
		validateReplicatedReadExecutorCursor(t, &cursor, in.want)
		if err := cursor.Close(); err != nil {
			t.Fatal(err)
		}
		if err := lease.Finish(nil); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < len(inputs)*2; i++ {
		run(i)
	}
	return run
}

// Compare the original callback plus driver watcher with direct observation
// of the request context channel at existing executor checkpoints.
func BenchmarkReplicatedPointReadCancellation(b *testing.B) {
	f := newReplicatedReadExecutorBenchFixture(b)
	for _, shared := range []bool{false, true} {
		name := "duplicate"
		if shared {
			name = "direct"
		}
		b.Run(name, func(b *testing.B) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var flag query.CancelFlag
			f.ctx, f.options.Cancel = ctx, &flag
			run := replicatedPointIntoLoop(b, f, true)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if shared {
					flag.BindDone(ctx.Done())
					run(i)
				} else {
					stop := context.AfterFunc(ctx, flag.Cancel)
					run(i)
					stop()
				}
			}
		})
	}
}
