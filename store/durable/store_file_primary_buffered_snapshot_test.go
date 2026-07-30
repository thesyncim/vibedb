package durable

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
)

// primaryBufferedFixedValue is a fixed-width value: every id and revision is
// zero-padded so a replacement is always exactly the same byte length as the
// bulk-built value. That keeps leaves from growing into a structural split, so
// the workload exercises the buffered COW/materialize retirement path in
// isolation -- the first touch of each key COWs (accumulating a pending parent),
// exactly as the same-size update workload does.
func primaryBufferedFixedValue(id, revision int) []byte {
	return fmt.Appendf(nil, `{"id":"%09d","rev":"%040d"}`, id, revision)
}

// buildPrimaryBufferedFixedCorpus builds a primary-graph seed whose values are
// fixed-width (see primaryBufferedFixedValue).
func buildPrimaryBufferedFixedCorpus(
	t testing.TB, count int,
) (*store.Collection, []string) {
	t.Helper()
	builder, err := store.NewBuilder(store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	keys := make([]string, count)
	for at := range count {
		keys[at] = fmt.Sprintf("primary-key-%09d", at)
		if err := builder.Append(keys[at], primaryBufferedFixedValue(at, 0)); err != nil {
			t.Fatal(err)
		}
	}
	built, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	return built, keys
}

// TestFilePrimaryBufferedSnapshotCheckpointRetire reproduces the buffered
// primary-graph "overlapping retired extent" defect and proves the accounting
// fix.
//
// The trigger is two buffered materializes derived from the same durable base
// with no flush between them. A buffered materialize re-checkpoints the primary
// parent graph and retires the base root; checkpointBufferedLocked keeps the
// durable base current by flushing after it, but Snapshot() -- and any
// snapshot-contended mutation -- materialize with no flush, leaving durableState
// behind. The next materialize then re-derives from that stale base and retires
// the base root a second time, which the extent reclaimer correctly rejects as
// an overlap.
//
// Each round accumulates volatile mutations, opens a snapshot (forcing a
// flush-less materialize), holds it across further mutations and a checkpoint,
// then closes it -- with no Flush across rounds, so every round's materialize
// shares the same durable base. Before the fix the second round's Snapshot fails
// with "overlapping retired extent"; after it, every acknowledged and durable
// view is intact.
func TestFilePrimaryBufferedSnapshotCheckpointRetire(t *testing.T) {
	const count = 2000
	built, keys := buildPrimaryBufferedFixedCorpus(t, count)
	options := Options{
		Backend: BackendPortable, ResidentBytes: 128 << 20,
		Durability: DurabilityBufferedVisible,
	}
	file := createPrimaryPointFile(t, built, options, "buffered-snapshot-retire.vibe")
	collection, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()

	oracle := make(map[string][]byte, count)
	for i := range keys {
		oracle[keys[i]] = primaryBufferedFixedValue(i, 0)
	}
	revision := 0
	put := func(id int) {
		revision++
		value := primaryBufferedFixedValue(id, revision)
		if _, err := collection.Put([]byte(keys[id]), value); err != nil {
			t.Fatalf("put %s: %v", keys[id], err)
		}
		oracle[keys[id]] = value
	}
	verify := func(tag string) {
		t.Helper()
		var scratch []byte
		for key, want := range oracle {
			got, found, err := collection.AppendRaw(scratch[:0], []byte(key))
			scratch = got
			if err != nil || !found || !bytes.Equal(got, want) {
				t.Fatalf("%s: read %s = (%q, found=%v, err=%v), want %q",
					tag, key, got, found, err, want)
			}
		}
	}

	// scatter touches a fixed stride of leaves so each round accumulates several
	// distinct pending parents across tablets, not just one.
	const stride = 47
	scatter := func(base int) {
		for k := 0; k < count/stride; k++ {
			put((base + k*stride) % count)
		}
	}

	for round := 0; round < 5; round++ {
		// Volatile mutations with no snapshot: the fast COW path accumulates
		// pending parents that the Snapshot below must materialize.
		scatter(round)
		snap, err := collection.Snapshot()
		if err != nil {
			t.Fatalf("round %d snapshot: %v", round, err)
		}
		// Mutate while the snapshot lease is held open across the checkpoint, then
		// checkpoint underneath it. The acknowledged immutable view must survive.
		scatter(round + stride/2)
		seen := 0
		if err := snap.RangeRaw(func(_, _ []byte) error {
			seen++
			return nil
		}); err != nil {
			t.Fatalf("round %d snapshot scan: %v", round, err)
		}
		if seen != count {
			t.Fatalf("round %d snapshot visited %d documents, want %d", round, seen, count)
		}
		if err := snap.Close(); err != nil {
			t.Fatalf("round %d snapshot close: %v", round, err)
		}
		verify(fmt.Sprintf("round %d live", round))
	}

	// A checkpoint now makes the whole accumulated sequence durable in one shot.
	if err := collection.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	verify("after flush")

	// The flushed image must recover the full content on reopen.
	image := captureJournalImage(t, file.Name())
	reopened := openBufferedImage(t, image, options)
	var scratch []byte
	for key, want := range oracle {
		got, found, err := reopened.AppendRaw(scratch[:0], []byte(key))
		scratch = got
		if err != nil || !found || !bytes.Equal(got, want) {
			t.Fatalf("reopen read %s = (%q, found=%v, err=%v), want %q",
				key, got, found, err, want)
		}
	}
}

// runPrimarySnapshotCheckpointFaultPass drives the double-materialize workload
// under a fault-injecting device. Each op accumulates volatile mutations, opens
// a snapshot that materializes them with no flush, accumulates a second batch,
// opens a second snapshot (deriving its checkpoint from the first, un-flushed
// one), and flushes with that snapshot held open across the persisted
// checkpoint. Volatile mutations and the two buffered materializes issue no
// device commit; only the terminal Flush does, so each op's commit window is its
// checkpoint. It records the commit boundaries, content oracle, and per-commit
// write plan of a clean pass, and the on-disk image at whatever crash the plan
// induced.
func runPrimarySnapshotCheckpointFaultPass(
	t *testing.T,
	built *store.Collection,
	keys []string,
	options Options,
	fc *faultController,
	plan storeio.FaultPlan,
	ops int,
) (
	boundaries []int,
	contents []map[string]string,
	records []storeio.FaultCommitRecord,
	image journalCrashImage,
	faulted bool,
) {
	t.Helper()
	prev := storeCommitterFactory
	storeCommitterFactory = fc.factory()
	defer func() { storeCommitterFactory = prev }()
	fc.plan = plan
	fc.device = nil

	path := filepath.Join(t.TempDir(), "primary-snapshot-checkpoint-crash.vibe")
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := CreateFromPrimary(built, file, options); err != nil {
		_ = file.Close()
		t.Fatalf("seed CreateFromPrimary: %v", err)
	}
	coll, err := Open(file, options)
	if err != nil {
		_ = file.Close()
		t.Fatalf("open seed: %v", err)
	}
	dev := fc.device
	if dev == nil {
		_ = coll.Close()
		_ = file.Close()
		t.Fatal("fault device was not installed by the committer factory")
	}

	count := len(keys)
	const stride = 31
	revision := 0
	scatter := func(base int) error {
		for k := 0; k < count/stride; k++ {
			revision++
			id := (base + k*stride) % count
			if _, err := coll.Put([]byte(keys[id]), primaryBufferedFixedValue(id, revision)); err != nil {
				return err
			}
		}
		return nil
	}

	boundaries = []int{dev.Commits()}
	contents = []map[string]string{snapshotCollectionContent(t, coll)}
	for i := 0; i < ops; i++ {
		opErr := scatter(i)
		var first *Snapshot
		if opErr == nil {
			// First flush-less materialize; closed before the second batch so those
			// mutations take the fast COW path and accumulate fresh pending parents.
			first, opErr = coll.Snapshot()
		}
		if opErr == nil {
			opErr = first.Close()
			first = nil
		}
		if opErr == nil {
			opErr = scatter(i + stride/2)
		}
		var second *Snapshot
		if opErr == nil {
			// Second flush-less materialize; its base is the first, un-flushed cut.
			second, opErr = coll.Snapshot()
		}
		if opErr == nil {
			// Persist both materializes in one checkpoint with a snapshot open across
			// its commit window.
			opErr = coll.Flush()
		}
		if second != nil {
			_ = second.Close()
		}
		if dev.Faulted() {
			break
		}
		if opErr != nil {
			_ = coll.Close()
			_ = file.Close()
			t.Fatalf("op %d: unexpected error before any fault: %v", i, opErr)
		}
		boundaries = append(boundaries, dev.Commits())
		contents = append(contents, snapshotCollectionContent(t, coll))
	}
	faulted = dev.Faulted()
	records = dev.Records()
	image = captureJournalImage(t, path)
	_ = coll.Close()
	_ = file.Close()
	return boundaries, contents, records, image, faulted
}

// TestFilePrimaryBufferedSnapshotCheckpointCrashBoundary is the crash cover for
// the buffered snapshot-checkpoint retirement fix. The existing split sweep
// (TestFilePrimaryLeafSplitCrashBoundary) flushes after every op and never holds
// a snapshot across a checkpoint, so it does not reach the path where two
// flush-less materializes share one durable base and a single later checkpoint
// persists both.
//
// The clean probe pass already exercises the fix: without it the second
// Snapshot per op fails with an overlapping retired extent before any fault
// fires. The sweep then crashes that persisting checkpoint at every recorded
// write point -- each data-page write, the data barrier, the alternate root, the
// final sync, and a torn root -- and reopens each induced image. Every reopen
// must fail closed or recover to exactly the pre- or post-checkpoint content
// (the reclaimer must neither double-free nor leak the twice-touched base root),
// and the offline Verify walker must accept every recovered graph.
func TestFilePrimaryBufferedSnapshotCheckpointCrashBoundary(t *testing.T) {
	const count = 600
	built, keys := buildPrimaryBufferedFixedCorpus(t, count)
	options := Options{
		Backend: BackendPortable, ResidentBytes: 32 << 20,
		Durability: DurabilityBufferedVisible,
		// One generation per device commit and no coalescing so the Nth commit is
		// the same logical write on every replay.
		GroupLimit: 1, CommitCoalesce: 0,
	}

	const maxOps = 3
	fc := &faultController{}
	boundaries, contents, records, _, faulted := runPrimarySnapshotCheckpointFaultPass(
		t, built, keys, options, fc, storeio.FaultPlan{Phase: storeio.FaultNone}, maxOps,
	)
	if faulted {
		t.Fatal("clean probe pass unexpectedly faulted")
	}
	if len(boundaries) < maxOps+1 {
		t.Fatalf("probe recorded %d boundaries, want %d", len(boundaries), maxOps+1)
	}
	if boundaries[len(boundaries)-1] != len(records) {
		t.Fatalf("commit boundary %d disagrees with %d recorded commits",
			boundaries[len(boundaries)-1], len(records))
	}

	// Sweep the last op's persisting-checkpoint window, which has a durable prior
	// boundary to recover to.
	op := maxOps - 1
	windowLo, windowHi := boundaries[op], boundaries[op+1]
	if windowHi <= windowLo {
		t.Fatalf("op %d issued no device commit: window [%d,%d)", op, windowLo, windowHi)
	}
	t.Logf("checkpoint window [%d,%d) of %d commits; pre=%d keys post=%d keys",
		windowLo, windowHi, boundaries[len(boundaries)-1],
		len(contents[op]), len(contents[op+1]))

	tally := map[string]int{}
	exercised := 0
	for commit := windowLo; commit < windowHi; commit++ {
		rec := records[commit]
		var plans []storeio.FaultPlan
		for i := range rec.DataPages {
			plans = append(plans, storeio.FaultPlan{
				Commit: commit, Phase: storeio.FaultAfterDataWrite, DataIndex: i,
			})
		}
		plans = append(plans,
			storeio.FaultPlan{Commit: commit, Phase: storeio.FaultAfterBarrier},
			storeio.FaultPlan{Commit: commit, Phase: storeio.FaultAfterRootWrite},
			storeio.FaultPlan{Commit: commit, Phase: storeio.FaultAfterFinalSync},
			storeio.FaultPlan{Commit: commit, Phase: storeio.FaultTornRoot},
		)
		for _, plan := range plans {
			_, _, _, image, didFault := runPrimarySnapshotCheckpointFaultPass(
				t, built, keys, options, fc, plan, op+1,
			)
			if !didFault {
				continue
			}
			label := fmt.Sprintf("commit=%d phase=%d data=%d",
				plan.Commit, plan.Phase, plan.DataIndex)
			legal := expectedStates(commit, plan.Phase, boundaries, contents)
			outcome := verifyJournalCrashImage(
				t, options, image, legal, label,
			)
			tally[outcome]++
			exercised++

			imagePath := filepath.Join(t.TempDir(), "verify.vibe")
			if err := os.WriteFile(imagePath, image.store, 0o600); err != nil {
				t.Fatal(err)
			}
			vf, vErr := os.OpenFile(imagePath, os.O_RDWR, 0o600)
			if vErr != nil {
				t.Fatal(vErr)
			}
			report, verifyErr := Verify(vf)
			_ = vf.Close()
			if verifyErr != nil {
				t.Fatalf("%s: Verify returned I/O error: %v", label, verifyErr)
			}
			if outcome == "recovered" && !report.OK() {
				t.Fatalf("%s: recovered image failed verify walker: %v",
					label, report.Findings)
			}
		}
	}
	if exercised == 0 {
		t.Fatal("no crash points were exercised in the checkpoint window")
	}
	t.Logf("snapshot-checkpoint crash sweep: %d points exercised; outcomes %v",
		exercised, tally)
}
