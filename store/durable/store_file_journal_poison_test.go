package durable

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/storeio"
)

// openFaultJournalCollection creates and opens a journaled primary collection
// with the FaultJournal seam installed over the journal Open pairs, returning
// the collection, its store path, and the wrapper to program. It is the shared
// fixture for the poison tests below; the seam restore is registered on t.
func openFaultJournalCollection(
	t *testing.T, options Options,
) (*Collection, string, *storeio.FaultJournal) {
	t.Helper()
	get, restore := installJournalFaultSeam(t)
	t.Cleanup(restore)
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
	t.Cleanup(func() {
		_ = coll.Close()
		_ = file.Close()
	})
	fj := get()
	if fj == nil {
		t.Fatal("fault seam not installed")
	}
	return coll, path, fj
}

// reopenJournalImage writes a captured store+journal pair into a fresh
// directory and opens it, failing the test on any Open error: every image the
// poison tests capture is a clean prefix, never a torn one.
func reopenJournalImage(
	t *testing.T, options Options, image journalCrashImage,
) *Collection {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "recovered.vibe")
	if err := os.WriteFile(path, image.store, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path+".rjournal", image.journal, 0o600); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	coll, err := Open(file, options)
	if err != nil {
		_ = file.Close()
		t.Fatalf("reopen recovered image: %v", err)
	}
	t.Cleanup(func() {
		_ = coll.Close()
		_ = file.Close()
	})
	return coll
}

func requireJournalKey(t *testing.T, coll *Collection, key, want string) {
	t.Helper()
	got, ok, err := coll.AppendRaw(nil, []byte(key))
	if err != nil || !ok || string(got) != want {
		t.Fatalf("recovered key %q = (%q,%v,%v), want %q", key, got, ok, err, want)
	}
}

func requireUnknownJournalOutcome(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("journal sync failure reported success")
	}
	if !errors.Is(err, ErrCommitOutcomeUnknown) {
		t.Fatalf("journal sync failure = %v, want ErrCommitOutcomeUnknown", err)
	}
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("journal sync failure = %v, want root cause EIO", err)
	}
}

// TestSyncPrimaryJournalSyncFailureOutcomeUnknownMayReplay fixes the exact
// synchronous single-record ambiguity boundary. Append has completed, the
// durability barrier reports EIO, and the mutation has not been published to
// the live reader; nevertheless the complete redo record may survive a crash
// and replay. The caller and every sticky poison surface must therefore retain
// both ErrCommitOutcomeUnknown and the device's root cause.
func TestSyncPrimaryJournalSyncFailureOutcomeUnknownMayReplay(t *testing.T) {
	options := syncPrimaryJournalTestOptions()
	coll, path, fj := openFaultJournalCollection(t, options)

	appendAt := fj.Appends()
	syncAt := fj.Syncs()
	fj.Program(storeio.JournalFaultPlan{
		Phase:     storeio.JournalFaultSyncError,
		SyncIndex: syncAt,
	})
	key := []byte("sync-unknown-single")
	value := journalValue(71)
	_, commitErr := coll.Put(key, value)
	requireUnknownJournalOutcome(t, commitErr)
	if !fj.Faulted() {
		t.Fatal("programmed journal sync fault never fired")
	}
	if got := fj.Appends(); got != appendAt+1 {
		t.Fatalf("journal appends = %d, want %d", got, appendAt+1)
	}
	if got := fj.Syncs(); got != syncAt+1 {
		t.Fatalf("journal syncs = %d, want %d", got, syncAt+1)
	}
	if got, found, readErr := coll.AppendRaw(nil, key); readErr != nil || found {
		t.Fatalf("failed sync mutation visible before reopen: got=%q found=%t err=%v",
			got, found, readErr)
	}
	requireUnknownJournalOutcome(t, coll.PersistenceError())
	if _, afterErr := coll.Put([]byte("after-single-poison"), journalValue(72)); !errors.Is(afterErr, commitErr) {
		t.Fatalf("later mutation error = %v, want sticky poison %v", afterErr, commitErr)
	}

	// Capture the exact post-append/post-failed-sync image. Reading the image is
	// intentionally not another durability fence: it models the legal device
	// outcome in which the complete record reached stable media despite EIO.
	image := captureJournalImage(t, path)
	_ = coll.Close()
	recovered := reopenJournalImage(t, options, image)
	requireJournalKey(t, recovered, string(key), string(value))
}

// TestSyncPrimaryBatchJournalSyncFailureOutcomeUnknownMayReplay proves the
// same boundary for the atomic batch record. The failed caller sees neither row
// live, while reopen may replay both rows from the one checksummed record; a
// partial batch is never an admissible outcome.
func TestSyncPrimaryBatchJournalSyncFailureOutcomeUnknownMayReplay(t *testing.T) {
	options := syncPrimaryJournalTestOptions()
	coll, path, fj := openFaultJournalCollection(t, options)

	appendAt := fj.Appends()
	syncAt := fj.Syncs()
	fj.Program(storeio.JournalFaultPlan{
		Phase:     storeio.JournalFaultSyncError,
		SyncIndex: syncAt,
	})
	keys := [2][]byte{[]byte("sync-unknown-batch-a"), []byte("sync-unknown-batch-b")}
	values := [2][]byte{journalValue(81), journalValue(82)}
	commitErr := coll.Update(func(batch *WriteBatch) error {
		for i := range keys {
			if err := batch.Put(keys[i], values[i]); err != nil {
				return err
			}
		}
		return nil
	})
	requireUnknownJournalOutcome(t, commitErr)
	if !fj.Faulted() {
		t.Fatal("programmed batch journal sync fault never fired")
	}
	if got := fj.Appends(); got != appendAt+1 {
		t.Fatalf("batch journal appends = %d, want one atomic record (%d)",
			got, appendAt+1)
	}
	if got := fj.Syncs(); got != syncAt+1 {
		t.Fatalf("batch journal syncs = %d, want %d", got, syncAt+1)
	}
	for i := range keys {
		if got, found, readErr := coll.AppendRaw(nil, keys[i]); readErr != nil || found {
			t.Fatalf("failed batch row %d visible before reopen: got=%q found=%t err=%v",
				i, got, found, readErr)
		}
	}
	requireUnknownJournalOutcome(t, coll.PersistenceError())
	if _, afterErr := coll.Put([]byte("after-batch-poison"), journalValue(83)); !errors.Is(afterErr, commitErr) {
		t.Fatalf("later mutation error = %v, want sticky poison %v", afterErr, commitErr)
	}

	image := captureJournalImage(t, path)
	_ = coll.Close()
	recovered := reopenJournalImage(t, options, image)
	for i := range keys {
		requireJournalKey(t, recovered, string(keys[i]), string(values[i]))
	}
}

// TestSyncPrimaryJournalAppendFailureOutcomeUnknown covers the positional-write
// ambiguity for both synchronous record shapes. Even before an explicit sync,
// an append error cannot prove rejection: the complete checksummed body may
// have landed before unwritten padding. The lane must therefore retain both
// ErrCommitOutcomeUnknown and the device error. This fault seam rejects the
// write completely, so the captured image exercises the admissible no-replay
// side of that unknown outcome.
func TestSyncPrimaryJournalAppendFailureOutcomeUnknown(t *testing.T) {
	for _, tc := range []struct {
		name string
		run  func(*Collection, [2][]byte, [2][]byte) error
		rows int
	}{
		{
			name: "single",
			rows: 1,
			run: func(coll *Collection, keys [2][]byte, values [2][]byte) error {
				_, err := coll.Put(keys[0], values[0])
				return err
			},
		},
		{
			name: "atomic-batch",
			rows: 2,
			run: func(coll *Collection, keys [2][]byte, values [2][]byte) error {
				return coll.Update(func(batch *WriteBatch) error {
					for i := range keys {
						if err := batch.Put(keys[i], values[i]); err != nil {
							return err
						}
					}
					return nil
				})
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			options := syncPrimaryJournalTestOptions()
			coll, path, fj := openFaultJournalCollection(t, options)
			keys := [2][]byte{[]byte("append-failed-a"), []byte("append-failed-b")}
			values := [2][]byte{journalValue(91), journalValue(92)}
			appendAt := fj.Appends()
			fj.Program(storeio.JournalFaultPlan{
				Phase:       storeio.JournalFaultENOSPCAppend,
				AppendIndex: appendAt,
			})

			commitErr := tc.run(coll, keys, values)
			if commitErr == nil {
				t.Fatal("failed journal append reported success")
			}
			if !errors.Is(commitErr, syscall.ENOSPC) {
				t.Fatalf("append failure = %v, want root cause ENOSPC", commitErr)
			}
			if !errors.Is(commitErr, ErrCommitOutcomeUnknown) {
				t.Fatalf("append failure = %v, want ErrCommitOutcomeUnknown", commitErr)
			}
			if persistenceErr := coll.PersistenceError(); !errors.Is(persistenceErr, syscall.ENOSPC) ||
				!errors.Is(persistenceErr, ErrCommitOutcomeUnknown) {
				t.Fatalf("sticky append poison = %v, want unknown+ENOSPC", persistenceErr)
			}
			for i := 0; i < tc.rows; i++ {
				if got, found, readErr := coll.AppendRaw(nil, keys[i]); readErr != nil || found {
					t.Fatalf("failed append row %d visible: got=%q found=%t err=%v",
						i, got, found, readErr)
				}
			}

			image := captureJournalImage(t, path)
			_ = coll.Close()
			recovered := reopenJournalImage(t, options, image)
			for i := 0; i < tc.rows; i++ {
				if got, found, readErr := recovered.AppendRaw(nil, keys[i]); readErr != nil || found {
					t.Fatalf("failed append row %d replayed: got=%q found=%t err=%v",
						i, got, found, readErr)
				}
			}
		})
	}
}

// TestRecoveryJournalOverflowMutationErrorLeavesNoDirtyResidue drives an
// out-of-line Put on the journal-backed sync lane into a device append failure
// at its point-of-no-return journal fence — after the overflow chain and the
// rewritten leaf were already admitted as buffered-dirty frames — and asserts
// the cache's dirty accounting returns exactly to its pre-Put level. Before the
// unadmit fix those frames were stranded: not in the pending-parent set, never
// retired, invisible to every checkpoint, so each failed attempt leaked up to a
// whole document of dirty capacity until Close.
func TestRecoveryJournalOverflowMutationErrorLeavesNoDirtyResidue(t *testing.T) {
	options := syncPrimaryOverflowJournalTestOptions()
	coll, path, fj := openFaultJournalCollection(t, options)

	// One inline acknowledged write settles the steady state the failed Put must
	// restore, and is the acknowledged key the reopen below must recover.
	if _, err := coll.Put([]byte("warm"), journalValue(0)); err != nil {
		t.Fatalf("warm put: %v", err)
	}
	// Settle the warm overlay before taking the dirty baseline. An out-of-line
	// mutation cannot itself use the inline row overlay, so it first folds any
	// pending class-5 rows; those reachable checkpoint frames are not residue
	// from the later failed mutation.
	if err := coll.Flush(); err != nil {
		t.Fatalf("warm flush: %v", err)
	}
	dirtyBefore := coll.cache.Stats().DirtyBytes

	fj.Program(storeio.JournalFaultPlan{
		Phase:       storeio.JournalFaultENOSPCAppend,
		AppendIndex: fj.Appends(),
	})
	if _, err := coll.Put([]byte("spill"), journalOverflowValue(1)); err == nil {
		t.Fatal("overflow put with failed journal append reported success")
	}
	if dirtyAfter := coll.cache.Stats().DirtyBytes; dirtyAfter != dirtyBefore {
		t.Fatalf("failed overflow put leaked dirty frames: dirty bytes %d -> %d",
			dirtyBefore, dirtyAfter)
	}
	if coll.PersistenceError() == nil {
		t.Fatal("journal append device failure left PersistenceError nil")
	}
	if _, err := coll.Put([]byte("after"), journalValue(2)); err == nil {
		t.Fatal("mutation accepted after journal poison")
	}

	// The acknowledged prefix must survive reopen; the failed Put must not.
	image := captureJournalImage(t, path)
	_ = coll.Close()
	recovered := reopenJournalImage(t, options, image)
	requireJournalKey(t, recovered, "seed", `{"v":0}`)
	requireJournalKey(t, recovered, "warm", string(journalValue(0)))
	if _, ok, err := recovered.AppendRaw(nil, []byte("spill")); err != nil || ok {
		t.Fatalf("unacknowledged failed put resurfaced: ok=%t err=%v", ok, err)
	}
}

// TestRecoveryJournalRecycleDeviceFailurePoisons proves a Recycle device
// failure is sticky. The recycle header write is the journal half of a
// checkpoint's root publication; before the fix its failure surfaced as a plain
// error with PersistenceError still nil while the mutation stream kept being
// accepted and re-failing, violating the die-don't-retry posture every
// neighboring journal device error already has.
func TestRecoveryJournalRecycleDeviceFailurePoisons(t *testing.T) {
	options := journalTestOptions(CheckpointFilesystem)
	coll, path, fj := openFaultJournalCollection(t, options)

	acked := map[string]string{"seed": `{"v":0}`}
	for i := range 3 {
		key := journalCrashKey(i)
		if _, err := coll.Put([]byte(key), journalValue(i)); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
		acked[key] = string(journalValue(i))
	}

	fj.Program(storeio.JournalFaultPlan{Phase: storeio.JournalFaultENOSPCRecycle})
	if err := coll.Flush(); err == nil {
		t.Fatal("checkpoint with failed journal recycle reported success")
	}
	if coll.PersistenceError() == nil {
		t.Fatal("recycle device failure left PersistenceError nil")
	}
	if _, err := coll.Put([]byte("after"), journalValue(9)); err == nil {
		t.Fatal("mutation accepted after recycle poison")
	}

	// Every acknowledged key must survive: the store root the checkpoint made
	// durable and the untouched journal records cover the whole prefix.
	image := captureJournalImage(t, path)
	_ = coll.Close()
	recovered := reopenJournalImage(t, options, image)
	for key, want := range acked {
		requireJournalKey(t, recovered, key, want)
	}
}

// TestRecoveryJournalGroupLeaderSyncFailurePoisonsWaiters parks concurrent
// buffered-journal depositors behind one lingering group-commit leader, fails
// the leader's sync, and asserts the whole fence dies: every parked waiter
// returns the poison instead of hanging, no second leader retries the sync (the
// fault is one-shot, so an illegal retry would succeed and acknowledge a waiter
// the assertion would catch), the store rejects later mutations, and every key
// acknowledged before the failure survives reopen. The sync barrier had no
// fault coverage at all before this seam.
func TestRecoveryJournalGroupLeaderSyncFailurePoisonsWaiters(t *testing.T) {
	options := journalTestOptions(CheckpointFilesystem)
	options.CommitCoalesce = 25 * time.Millisecond
	coll, path, fj := openFaultJournalCollection(t, options)

	acked := map[string]string{"seed": `{"v":0}`}
	for i := range 2 {
		key := journalCrashKey(i)
		if _, err := coll.Put([]byte(key), journalValue(i)); err != nil {
			t.Fatalf("warm put %s: %v", key, err)
		}
		acked[key] = string(journalValue(i))
	}

	faultAt := fj.Syncs()
	fj.Program(storeio.JournalFaultPlan{
		Phase:     storeio.JournalFaultSyncError,
		SyncIndex: faultAt,
	})

	const parked = 4
	errs := make([]error, parked)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for w := range parked {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			<-start
			_, errs[w] = coll.Put([]byte(journalCrashKey(100+w)), journalValue(100+w))
		}(w)
	}
	close(start)

	// Waitgroup accounting doubles as the leak check: a waiter stuck on the
	// group fence keeps wg from draining and fails here instead of hanging the
	// suite forever.
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("parked group-commit waiters hung after leader sync failure")
	}

	for w, err := range errs {
		if err == nil {
			t.Fatalf("parked waiter %d was acknowledged across a failed sync", w)
		}
		requireUnknownJournalOutcome(t, err)
	}
	if !fj.Faulted() {
		t.Fatal("programmed sync fault never fired")
	}
	if got := fj.Syncs(); got != faultAt+1 {
		t.Fatalf("journal syncs after poison = %d, want %d (a second leader retried)",
			got, faultAt+1)
	}
	requireUnknownJournalOutcome(t, coll.PersistenceError())
	if _, err := coll.Put([]byte("after"), journalValue(9)); err == nil {
		t.Fatal("mutation accepted after group fence poison")
	} else {
		requireUnknownJournalOutcome(t, err)
	}

	// Reopen: every key acknowledged before the failure is durable through the
	// journal; a parked key may legally appear (its record was appended before
	// the failed sync) but never with torn bytes.
	image := captureJournalImage(t, path)
	_ = coll.Close()
	recovered := reopenJournalImage(t, options, image)
	for key, want := range acked {
		requireJournalKey(t, recovered, key, want)
	}
	for w := range parked {
		key := journalCrashKey(100 + w)
		got, ok, err := recovered.AppendRaw(nil, []byte(key))
		if err != nil {
			t.Fatalf("read parked key %q: %v", key, err)
		}
		if ok && string(got) != string(journalValue(100+w)) {
			t.Fatalf("parked key %q recovered torn value %q", key, got)
		}
	}
}
