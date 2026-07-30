package durable

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
)

// primaryBatchLane names one of the three durability lanes an ordered-primary
// Update supports.
type primaryBatchLane struct {
	name    string
	options Options
}

func primaryBatchLanes() []primaryBatchLane {
	buffered := journalTestOptions(CheckpointPowerSafe)
	buffered.RecoveryJournal = false
	return []primaryBatchLane{
		{"buffered", buffered},
		{"buffered-journal", journalTestOptions(CheckpointPowerSafe)},
		{"sync", syncPrimaryJournalTestOptions()},
	}
}

// openPrimaryBatchStore builds a seeded ordered-primary store and opens it.
func openPrimaryBatchStore(t *testing.T, options Options) (*Collection, *os.File, string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "store.vibe")
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := CreateFromPrimary(seedPrimaryCollection(t), file, options); err != nil {
		t.Fatalf("CreateFromPrimary: %v", err)
	}
	_ = file.Close()
	file, err = os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	coll, err := Open(file, options)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return coll, file, path
}

func primaryStoreContent(t *testing.T, coll *Collection) map[string]string {
	t.Helper()
	snap, err := coll.Snapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	defer snap.Close()
	content := map[string]string{}
	if err := snap.RangeRaw(func(key, value []byte) error {
		content[string(key)] = string(value)
		return nil
	}); err != nil {
		t.Fatalf("range: %v", err)
	}
	return content
}

type batchOp struct {
	key    string
	value  []byte
	remove bool
}

// primaryBatchWorkload builds a deterministic op stream that overwrites keys,
// deletes present and absent keys, and packs several distinct keys into the same
// leaf, so the batch path exercises crowded-leaf folding, dedup, and split retry.
func primaryBatchWorkload(seed int64, count, keyspace int) []batchOp {
	rng := rand.New(rand.NewSource(seed))
	ops := make([]batchOp, 0, count)
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("k%03d", rng.Intn(keyspace))
		if rng.Intn(4) == 0 {
			ops = append(ops, batchOp{key: key, remove: true})
			continue
		}
		ops = append(ops, batchOp{
			key:   key,
			value: []byte(fmt.Sprintf(`{"i":%d,"k":%q}`, i, key)),
		})
	}
	return ops
}

// TestFilePrimaryBatchMatchesSequential proves a batch reaches the same end state
// as the identical operations applied one at a time, on every supported lane. It
// is the differential the SQL Tx surface stands on: batch(ops) == sequential(ops).
func TestFilePrimaryBatchMatchesSequential(t *testing.T) {
	for _, lane := range primaryBatchLanes() {
		t.Run(lane.name, func(t *testing.T) {
			ops := primaryBatchWorkload(0x5eed, 600, 48)

			sequential, seqFile, _ := openPrimaryBatchStore(t, lane.options)
			defer sequential.Close()
			defer seqFile.Close()
			for _, op := range ops {
				if op.remove {
					if _, err := sequential.Delete([]byte(op.key)); err != nil {
						t.Fatalf("sequential delete %s: %v", op.key, err)
					}
					continue
				}
				if _, err := sequential.Put([]byte(op.key), op.value); err != nil {
					t.Fatalf("sequential put %s: %v", op.key, err)
				}
			}

			batched, batFile, _ := openPrimaryBatchStore(t, lane.options)
			defer batched.Close()
			defer batFile.Close()
			rng := rand.New(rand.NewSource(0x1337))
			for i := 0; i < len(ops); {
				group := 1 + rng.Intn(12)
				if i+group > len(ops) {
					group = len(ops) - i
				}
				run := ops[i : i+group]
				if err := batched.Update(func(b *WriteBatch) error {
					for _, op := range run {
						if op.remove {
							if err := b.Delete([]byte(op.key)); err != nil {
								return err
							}
							continue
						}
						if err := b.Put([]byte(op.key), op.value); err != nil {
							return err
						}
					}
					return nil
				}); err != nil {
					t.Fatalf("batch update at %d: %v", i, err)
				}
				i += group
			}

			seq := primaryStoreContent(t, sequential)
			bat := primaryStoreContent(t, batched)
			if !mapsEqual(seq, bat) {
				t.Fatalf("batched content diverged from sequential: seq=%d keys, bat=%d keys",
					len(seq), len(bat))
			}
			if sequential.Len() != batched.Len() {
				t.Fatalf("Len diverged: sequential=%d batched=%d",
					sequential.Len(), batched.Len())
			}
		})
	}
}

// TestFilePrimaryBatchAtomicOnPrepareFailure proves a batch that fails during
// prepare — because the callback returns an error, or because a document does not
// parse — publishes nothing at all, on every lane. It is the all-or-nothing
// half of the transactional contract.
func TestFilePrimaryBatchAtomicOnPrepareFailure(t *testing.T) {
	for _, lane := range primaryBatchLanes() {
		t.Run(lane.name, func(t *testing.T) {
			coll, file, _ := openPrimaryBatchStore(t, lane.options)
			defer coll.Close()
			defer file.Close()
			for i := 0; i < 8; i++ {
				if _, err := coll.Put([]byte(fmt.Sprintf("base%02d", i)), journalValue(i)); err != nil {
					t.Fatalf("seed put: %v", err)
				}
			}
			before := primaryStoreContent(t, coll)

			boom := errors.New("caller aborted the batch")
			if err := coll.Update(func(b *WriteBatch) error {
				if err := b.Put([]byte("late"), []byte(`{"ok":1}`)); err != nil {
					return err
				}
				return boom
			}); !errors.Is(err, boom) {
				t.Fatalf("callback-error Update = %v, want %v", err, boom)
			}
			if got := primaryStoreContent(t, coll); !mapsEqual(got, before) {
				t.Fatal("a callback-aborted batch changed collection state")
			}

			// A malformed document is a prepare-time rejection: the whole batch,
			// including the well-formed sibling, must publish nothing.
			if err := coll.Update(func(b *WriteBatch) error {
				if err := b.Put([]byte("good"), []byte(`{"ok":2}`)); err != nil {
					return err
				}
				return b.Put([]byte("bad"), []byte(`{"broken":`))
			}); err == nil {
				t.Fatal("batch with a malformed document succeeded, want an error")
			}
			after := primaryStoreContent(t, coll)
			if !mapsEqual(after, before) {
				t.Fatal("a rejected malformed batch changed collection state")
			}
			if _, ok, _ := coll.AppendRaw(nil, []byte("good")); ok {
				t.Fatal("the well-formed sibling of a rejected batch became visible")
			}
			if coll.PersistenceError() != nil {
				t.Fatalf("an ordinary batch rejection poisoned the collection: %v",
					coll.PersistenceError())
			}
		})
	}
}

// TestFilePrimaryBatchDeterministic proves batch apply is byte-deterministic: two
// stores that start from identical bytes and receive the identical batch commit
// to identical bytes. Each newly created buffered store has an independent
// random journal identity, so the fixture creates one base and clones the paired
// store+journal image before applying either batch.
func TestFilePrimaryBatchDeterministic(t *testing.T) {
	options := journalTestOptions(CheckpointPowerSafe)
	options.RecoveryJournal = false

	baseDir := t.TempDir()
	basePath := filepath.Join(baseDir, "determinism-base.vibe")
	baseFile, err := os.OpenFile(
		basePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := CreateFromPrimary(
		seedPrimaryCollection(t), baseFile, options,
	); err != nil {
		t.Fatalf("CreateFromPrimary: %v", err)
	}
	if err := baseFile.Close(); err != nil {
		t.Fatal(err)
	}
	baseStore, err := os.ReadFile(basePath)
	if err != nil {
		t.Fatal(err)
	}
	baseJournal, err := os.ReadFile(RecoveryJournalPath(basePath))
	if err != nil {
		t.Fatal(err)
	}

	build := func(label string) []byte {
		dir := t.TempDir()
		path := filepath.Join(dir, label+".vibe")
		if err := os.WriteFile(path, baseStore, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(
			RecoveryJournalPath(path), baseJournal, 0o600,
		); err != nil {
			t.Fatal(err)
		}
		file, err := os.OpenFile(path, os.O_RDWR, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		coll, err := Open(file, options)
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		ops := primaryBatchWorkload(0xABCD, 400, 40)
		if err := coll.Update(func(b *WriteBatch) error {
			for _, op := range ops {
				if op.remove {
					if err := b.Delete([]byte(op.key)); err != nil {
						return err
					}
					continue
				}
				if err := b.Put([]byte(op.key), op.value); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			t.Fatalf("batch update: %v", err)
		}
		if err := coll.Flush(); err != nil {
			t.Fatalf("flush: %v", err)
		}
		if err := coll.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
		_ = file.Close()
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		return data
	}

	first := build("determinism-first")
	second := build("determinism-second")
	if !bytes.Equal(first, second) {
		t.Fatalf("batch apply is not byte-deterministic: %d vs %d bytes",
			len(first), len(second))
	}
}

// TestFilePrimaryBatchSnapshotIsolation proves the snapshot/lease rule: a batch
// committed while a snapshot is open is invisible to that snapshot (its
// superseded frames are retained, not freed) and fully visible to the live
// collection, on every lane. It exercises the canonical path under an active
// lease, which the sync lane always takes and the buffered lanes take here too.
func TestFilePrimaryBatchSnapshotIsolation(t *testing.T) {
	for _, lane := range primaryBatchLanes() {
		t.Run(lane.name, func(t *testing.T) {
			coll, file, _ := openPrimaryBatchStore(t, lane.options)
			defer coll.Close()
			defer file.Close()
			for i := 0; i < 24; i++ {
				if _, err := coll.Put([]byte(fmt.Sprintf("base%02d", i)), journalValue(i)); err != nil {
					t.Fatalf("seed put: %v", err)
				}
			}
			before := primaryStoreContent(t, coll)

			snap, err := coll.Snapshot()
			if err != nil {
				t.Fatalf("snapshot: %v", err)
			}

			if err := coll.Update(func(b *WriteBatch) error {
				if err := b.Put([]byte("fresh"), []byte(`{"new":1}`)); err != nil {
					return err
				}
				if err := b.Put([]byte("base00"), []byte(`{"overwritten":1}`)); err != nil {
					return err
				}
				return b.Delete([]byte("base01"))
			}); err != nil {
				t.Fatalf("batch under snapshot: %v", err)
			}

			// The open snapshot must still observe the exact pre-batch content.
			snapContent := map[string]string{}
			if err := snap.RangeRaw(func(key, value []byte) error {
				snapContent[string(key)] = string(value)
				return nil
			}); err != nil {
				t.Fatalf("snapshot range: %v", err)
			}
			if !mapsEqual(snapContent, before) {
				t.Fatal("open snapshot observed a batch committed after it was taken")
			}

			// The live collection observes the whole batch.
			if v, ok, _ := coll.AppendRaw(nil, []byte("fresh")); !ok || string(v) != `{"new":1}` {
				t.Fatalf("live fresh = %q,%v, want the batch put", v, ok)
			}
			if v, ok, _ := coll.AppendRaw(nil, []byte("base00")); !ok || string(v) != `{"overwritten":1}` {
				t.Fatalf("live base00 = %q,%v, want the batch overwrite", v, ok)
			}
			if _, ok, _ := coll.AppendRaw(nil, []byte("base01")); ok {
				t.Fatal("live base01 still present after batch delete")
			}
			if err := snap.Close(); err != nil {
				t.Fatalf("snapshot close: %v", err)
			}

			// After the lease closes, a second batch commits and reads back.
			if err := coll.Update(func(b *WriteBatch) error {
				return b.Put([]byte("after"), []byte(`{"post":1}`))
			}); err != nil {
				t.Fatalf("post-snapshot batch: %v", err)
			}
			if v, ok, _ := coll.AppendRaw(nil, []byte("after")); !ok || string(v) != `{"post":1}` {
				t.Fatalf("live after = %q,%v, want the second batch put", v, ok)
			}
			if coll.PersistenceError() != nil {
				t.Fatalf("snapshot-spanning batches poisoned the collection: %v",
					coll.PersistenceError())
			}
		})
	}
}

func batchCrashKey(batch, slot int) string {
	return fmt.Sprintf("b%03d-%d", batch, slot)
}

// batchContentOracle computes the content after each batch directly from the
// deterministic workload: contents[i] is the seed plus every key of batches
// 0..i-1. Each batch is atomic, so a crash on batch i's record recovers to
// contents[i] or contents[i+1] — never a torn half.
func batchContentOracle(n int) []map[string]string {
	contents := make([]map[string]string, 0, n+1)
	for i := 0; i <= n; i++ {
		m := map[string]string{"seed": `{"v":0}`}
		for g := 0; g < i; g++ {
			m[batchCrashKey(g, 0)] = string(journalValue(2 * g))
			m[batchCrashKey(g, 1)] = string(journalValue(2*g + 1))
		}
		contents = append(contents, m)
	}
	return contents
}

func applyCrashBatch(coll *Collection, i int) error {
	return coll.Update(func(b *WriteBatch) error {
		if err := b.Put([]byte(batchCrashKey(i, 0)), journalValue(2*i)); err != nil {
			return err
		}
		return b.Put([]byte(batchCrashKey(i, 1)), journalValue(2*i+1))
	})
}

// TestFilePrimaryBatchCommitCrashMatrix crashes the batch's single journal record
// append at each fault phase and several batch ordinals, then reopens the captured
// store+journal image. Because one batch is one record framed by one CRC, a torn,
// dropped, or ENOSPC'd append drops the whole batch, and recovery must land on the
// content just before or just after it — never a batch with only one of its two
// documents.
func TestFilePrimaryBatchCommitCrashMatrix(t *testing.T) {
	lanes := []primaryBatchLane{
		{"sync", syncPrimaryJournalTestOptions()},
		{"buffered-journal", journalTestOptions(CheckpointPowerSafe)},
	}
	const n = 8
	contents := batchContentOracle(n)
	phases := []struct {
		name  string
		phase storeio.JournalFaultPhase
	}{
		{"torn-append", storeio.JournalFaultTornAppend},
		{"drop-append", storeio.JournalFaultDropAppend},
		{"enospc-append", storeio.JournalFaultENOSPCAppend},
	}
	indices := []int{0, 1, 3, 6}
	exercised := 0
	for _, lane := range lanes {
		for _, ph := range phases {
			for _, idx := range indices {
				label := fmt.Sprintf("%s/%s@%d", lane.name, ph.name, idx)
				get, restore := installJournalFaultSeam(t)
				coll, file, path := openPrimaryBatchStore(t, lane.options)
				fj := get()
				fj.Program(storeio.JournalFaultPlan{Phase: ph.phase, AppendIndex: idx})
				for i := 0; i < n; i++ {
					err := applyCrashBatch(coll, i)
					if fj.Faulted() {
						break
					}
					if err != nil {
						t.Fatalf("%s: batch %d before fault: %v", label, i, err)
					}
				}
				image := captureJournalImage(t, path)
				_ = coll.Close()
				_ = file.Close()
				restore()
				if !fj.Faulted() {
					t.Fatalf("%s: programmed fault never fired", label)
				}
				legal := []map[string]string{contents[idx]}
				if idx+1 <= n {
					legal = append(legal, contents[idx+1])
				}
				outcome := verifyJournalCrashImage(t, lane.options, image, legal, label)
				if outcome == "hole" || outcome == "panic" {
					t.Fatalf("%s: crash recovery outcome %s", label, outcome)
				}
				exercised++
			}
		}
	}
	if exercised == 0 {
		t.Fatal("no batch crash points exercised")
	}
	t.Logf("primary batch commit crash matrix: %d points", exercised)
}

// TestFilePrimaryBatchSyncedUnpublishedRecovers proves the other side of the
// window: on the sync lane the batch record is appended and synced BEFORE the
// batch is published, so a crash captured at that exact instant — record durable,
// store root still ahead of it, no in-memory publish — recovers the WHOLE batch
// on reopen, and a second reopen replays nothing new (idempotence).
func TestFilePrimaryBatchSyncedUnpublishedRecovers(t *testing.T) {
	options := syncPrimaryJournalTestOptions()
	const n = 6
	const target = 3
	contents := batchContentOracle(n)

	get, restore := installJournalFaultSeam(t)
	defer restore()
	coll, file, path := openPrimaryBatchStore(t, options)
	_ = get() // installs a JournalFaultNone seam so Open's own replay/recycle stay clean.

	var captured *journalCrashImage
	syncCount := 0
	prevHook := recoveryJournalPostSyncHook
	recoveryJournalPostSyncHook = func() {
		syncCount++
		// Capture right after the target batch's record is synced durable but before
		// its publish. syncCount reaches target+1 on the target batch's append,
		// because the seeded store performed no earlier acknowledgements.
		if syncCount == target+1 && captured == nil {
			image := captureJournalImage(t, path)
			captured = &image
		}
	}
	defer func() { recoveryJournalPostSyncHook = prevHook }()

	for i := 0; i < n; i++ {
		if err := applyCrashBatch(coll, i); err != nil {
			t.Fatalf("batch %d: %v", i, err)
		}
	}
	_ = coll.Close()
	_ = file.Close()
	restore()
	recoveryJournalPostSyncHook = prevHook
	if captured == nil {
		t.Fatal("post-sync hook never captured the synced-but-unpublished window")
	}

	// The record for batch `target` is durable but its publish never happened, so
	// recovery replays it: the whole batch (both documents) must be present.
	want := contents[target+1]
	dir := t.TempDir()
	recoveredPath := filepath.Join(dir, "recovered.vibe")
	if err := os.WriteFile(recoveredPath, captured.store, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(recoveredPath+".rjournal", captured.journal, 0o600); err != nil {
		t.Fatal(err)
	}

	firstContent := reopenPrimaryContent(t, recoveredPath, options)
	if !mapsEqual(firstContent, want) {
		t.Fatalf("first reopen recovered %v, want the whole synced batch %v", firstContent, want)
	}
	// Reopen the now-recovered store a second time: a clean close recycled the
	// journal, so this replays nothing new and yields the identical content.
	secondContent := reopenPrimaryContent(t, recoveredPath, options)
	if !mapsEqual(secondContent, firstContent) {
		t.Fatalf("second reopen diverged: %v vs %v", secondContent, firstContent)
	}
}

// runPrimaryBatchCheckpointFaultPass drives `ops` batches, each followed by a
// Flush that checkpoints the batch's buffered frames to disk, with the store
// device faulted at `plan`. It records the device commit boundaries and the
// content after each op so a sweep can crash the persisting checkpoint at every
// write point and check all-or-nothing recovery.
func runPrimaryBatchCheckpointFaultPass(
	t *testing.T, options Options, fc *faultController,
	plan storeio.FaultPlan, ops int,
) (boundaries []int, contents []map[string]string, records []storeio.FaultCommitRecord, image []byte, faulted bool) {
	t.Helper()
	prev := storeCommitterFactory
	storeCommitterFactory = fc.factory()
	defer func() { storeCommitterFactory = prev }()
	fc.plan = plan
	fc.device = nil

	path := filepath.Join(t.TempDir(), "primary-batch-checkpoint-crash.vibe")
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := CreateFromPrimary(seedPrimaryCollection(t), file, options); err != nil {
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

	boundaries = []int{dev.Commits()}
	contents = []map[string]string{snapshotCollectionContent(t, coll)}
	for i := 0; i < ops; i++ {
		opErr := coll.Update(func(b *WriteBatch) error {
			for j := 0; j < 8; j++ {
				if err := b.Put([]byte(fmt.Sprintf("bc%02d-%02d", i, j)), journalValue(i*8+j)); err != nil {
					return err
				}
			}
			return nil
		})
		if opErr == nil {
			opErr = coll.Flush()
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
	image, _ = os.ReadFile(path)
	_ = coll.Close()
	_ = file.Close()
	return boundaries, contents, records, image, faulted
}

// TestFilePrimaryBatchCheckpointCrashBoundary is the FaultDevice cover for the
// other durability boundary a batch crosses: the checkpoint that materializes its
// buffered frames into a durable root. It crashes that persisting checkpoint at
// every recorded write point — each data-page write, the data barrier, the
// alternate root, the final sync, and a torn root — and requires every reopen to
// fail closed or recover to exactly the pre- or post-checkpoint content, with the
// structural verify walker clean. A batch reaching disk is thus all-or-nothing at
// both the journal record and the checkpoint root.
func TestFilePrimaryBatchCheckpointCrashBoundary(t *testing.T) {
	options := Options{
		Backend: BackendPortable, ResidentBytes: 32 << 20,
		Durability: DurabilityBufferedVisible,
		GroupLimit: 1, CommitCoalesce: 0,
	}
	const maxOps = 3
	fc := &faultController{}
	boundaries, contents, records, _, faulted := runPrimaryBatchCheckpointFaultPass(
		t, options, fc, storeio.FaultPlan{Phase: storeio.FaultNone}, maxOps,
	)
	if faulted {
		t.Fatal("clean probe pass unexpectedly faulted")
	}
	if len(boundaries) < maxOps+1 {
		t.Fatalf("probe recorded %d boundaries, want %d", len(boundaries), maxOps+1)
	}

	op := maxOps - 1
	windowLo, windowHi := boundaries[op], boundaries[op+1]
	if windowHi <= windowLo {
		t.Fatalf("op %d issued no device commit: window [%d,%d)", op, windowLo, windowHi)
	}
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
			_, _, _, image, didFault := runPrimaryBatchCheckpointFaultPass(
				t, options, fc, plan, op+1,
			)
			if !didFault {
				continue
			}
			label := fmt.Sprintf("commit=%d phase=%d data=%d",
				plan.Commit, plan.Phase, plan.DataIndex)
			legal := expectedStates(commit, plan.Phase, boundaries, contents)
			outcome := verifyCrashImage(t, options, image, legal, label)
			tally[outcome]++
			exercised++

			imagePath := filepath.Join(t.TempDir(), "verify.vibe")
			if err := os.WriteFile(imagePath, image, 0o600); err != nil {
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
				t.Fatalf("%s: recovered image failed verify walker: %v", label, report.Findings)
			}
		}
	}
	if exercised == 0 {
		t.Fatal("no crash points were exercised in the batch checkpoint window")
	}
	t.Logf("primary batch checkpoint crash sweep: %d points exercised; outcomes %v",
		exercised, tally)
}

func reopenPrimaryContent(t *testing.T, path string, options Options) map[string]string {
	t.Helper()
	file, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	coll, err := Open(file, options)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	content := primaryStoreContent(t, coll)
	if err := coll.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	_ = file.Close()
	return content
}
