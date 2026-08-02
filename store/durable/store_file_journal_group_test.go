package durable

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/storeio"
)

func blockNextRecoveryJournalGroupSync(
	t *testing.T,
) (entered <-chan uint64, release func()) {
	t.Helper()
	enteredCh := make(chan uint64, 1)
	releaseCh := make(chan struct{})
	var releaseOnce sync.Once
	previous := recoveryJournalGroupBeforeSyncHook
	recoveryJournalGroupBeforeSyncHook = func(through uint64) {
		enteredCh <- through
		<-releaseCh
	}
	release = func() { releaseOnce.Do(func() { close(releaseCh) }) }
	t.Cleanup(func() {
		release()
		recoveryJournalGroupBeforeSyncHook = previous
	})
	return enteredCh, release
}

// openJournalGroupCollection creates and opens a buffered-visible primary
// collection with the recovery journal enabled, returning the collection, its
// store path, and a close that also closes the file. It is the shared fixture
// for the group-commit concurrency and crash-window tests.
func openJournalGroupCollection(t *testing.T, options Options) (*Collection, string) {
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
	t.Cleanup(func() {
		_ = coll.Close()
		_ = file.Close()
	})
	return coll, path
}

// TestRecoveryJournalReaderForcedTransactionalAck covers the buffered-journal
// acknowledgement when an active reader diverts Put from the canonical-frame
// lane into the transactional COW path. A successful return must still have one
// durable acknowledgement and must recover the mutation from an immediate
// crash image without relying on Flush or Close.
func TestRecoveryJournalReaderForcedTransactionalAck(t *testing.T) {
	options := journalTestOptions(CheckpointFilesystem)
	coll, path := openJournalGroupCollection(t, options)

	snapshot, err := coll.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	defer snapshot.Close()

	before := coll.Stats()
	key := []byte("reader-fallback")
	value := []byte(`{"v":1}`)
	if _, err := coll.Put(key, value); err != nil {
		t.Fatalf("Put: %v", err)
	}
	after := coll.Stats()
	acks := after.JournalAcks - before.JournalAcks +
		after.ChainAcks - before.ChainAcks
	if acks != 1 {
		t.Fatalf("durable acknowledgements=%d, want 1", acks)
	}

	image := captureJournalImage(t, path)
	result := verifyJournalCrashImage(
		t, options, image,
		[]map[string]string{{
			"seed":            `{"v":0}`,
			"reader-fallback": `{"v":1}`,
		}},
		"reader-forced transactional acknowledgement",
	)
	if result != "recovered" {
		t.Fatalf("crash-image result=%s, want recovered", result)
	}
}

// TestRecoveryJournalGroupCommitSharesSync proves the phase-1 amortization: when
// concurrent buffered-journal callers deposit under the writer and share one
// journal sync, one fence covers many records instead of one each — and every
// acknowledged record is still durable across a reopen. The linger variant makes
// the sharing deterministic (a leader waits CommitCoalesce for the group to
// gather); the default variant proves durability with no artificial window.
func TestRecoveryJournalGroupCommitSharesSync(t *testing.T) {
	const writers = 8
	const putsPer = 4

	for _, tc := range []struct {
		name            string
		linger          time.Duration
		requireGrouping bool
	}{
		{"linger", 25 * time.Millisecond, true},
		{"natural", 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			options := journalTestOptions(CheckpointPowerSafe)
			options.CommitCoalesce = tc.linger
			coll, path := openJournalGroupCollection(t, options)

			want := map[string]string{"seed": `{"v":0}`}
			var mu sync.Mutex
			var wg sync.WaitGroup
			start := make(chan struct{})
			for w := 0; w < writers; w++ {
				wg.Add(1)
				go func(w int) {
					defer wg.Done()
					<-start
					for p := 0; p < putsPer; p++ {
						key := fmt.Sprintf("w%02d-k%02d", w, p)
						// Class 5 exposes canonical JSON bytes. Keep this
						// journal test's oracle canonical so it measures
						// acknowledgement durability rather than source
						// object-field order.
						val := fmt.Sprintf(`{"p":%d,"w":%d}`, p, w)
						if _, err := coll.Put([]byte(key), []byte(val)); err != nil {
							t.Errorf("put %s: %v", key, err)
							return
						}
						mu.Lock()
						want[key] = val
						mu.Unlock()
					}
				}(w)
			}
			close(start)
			wg.Wait()

			stats := coll.Stats()
			t.Logf("%s: journalAcks=%d journalSyncs=%d largestGroup=%d",
				tc.name, stats.JournalAcks, stats.JournalSyncs, stats.JournalLargestGroup)
			if stats.JournalAcks != uint64(writers*putsPer) {
				t.Fatalf("journalAcks=%d, want %d", stats.JournalAcks, writers*putsPer)
			}
			if stats.JournalSyncs > stats.JournalAcks {
				t.Fatalf("journalSyncs=%d exceeds journalAcks=%d",
					stats.JournalSyncs, stats.JournalAcks)
			}
			if tc.requireGrouping && stats.JournalLargestGroup < 2 {
				t.Fatalf("no group commit occurred: largestGroup=%d (acks=%d syncs=%d)",
					stats.JournalLargestGroup, stats.JournalAcks, stats.JournalSyncs)
			}

			// Every acknowledged record survives a reopen: fold to a durable root,
			// close, and replay from a fresh Open.
			if err := coll.Flush(); err != nil {
				t.Fatalf("flush: %v", err)
			}
			if err := coll.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}
			reopened, err := os.OpenFile(path, os.O_RDWR, 0o600)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			rc, err := Open(reopened, options)
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			defer rc.Close()
			got := map[string]string{}
			snap, err := rc.Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			if err := snap.RangeRaw(func(k, v []byte) error {
				got[string(k)] = string(v)
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			snap.Close()
			if len(got) != len(want) {
				t.Fatalf("reopen has %d keys, want %d", len(got), len(want))
			}
			for k, v := range want {
				if got[k] != v {
					t.Fatalf("reopen key %s = %q, want %q", k, got[k], v)
				}
			}
		})
	}
}

// TestRecoveryJournalGroupTicketCoverageBeatsConcurrentAppendPoison pins both
// races between an out-of-writer group fence and a later append failure. The
// append's ENOSPC remains the collection's first, definite sticky diagnostic.
// A ticket already captured by the fence waits for and receives that fence's
// independent result: success if Sync succeeds, unknown+EIO if Sync fails.
func TestRecoveryJournalGroupTicketCoverageBeatsConcurrentAppendPoison(t *testing.T) {
	for _, tc := range []struct {
		name     string
		batch    bool
		failSync bool
	}{
		{name: "put/successful coverage wins", failSync: false},
		{name: "put/failed coverage keeps sync cause", failSync: true},
		{name: "update/successful coverage wins", batch: true, failSync: false},
		{name: "update/failed coverage keeps sync cause", batch: true, failSync: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			options := journalTestOptions(CheckpointFilesystem)
			coll, path, fault := openFaultJournalCollection(t, options)
			entered, release := blockNextRecoveryJournalGroupSync(t)

			coveredKey := []byte("covered-ticket")
			coveredValue := journalValue(301)
			coveredDone := make(chan error, 1)
			go func() {
				var err error
				if tc.batch {
					err = coll.Update(func(batch *WriteBatch) error {
						return batch.Put(coveredKey, coveredValue)
					})
				} else {
					_, err = coll.Put(coveredKey, coveredValue)
				}
				coveredDone <- err
			}()

			var through uint64
			select {
			case through = <-entered:
			case <-time.After(10 * time.Second):
				t.Fatal("group leader did not reach the blocked Sync")
			}
			if through != 1 {
				t.Fatalf("blocked Sync covers ticket %d, want 1", through)
			}

			// The later mutation reaches Append while the captured fence is held
			// outside c.writer, then fails before depositing its own ticket.
			appendAt := fault.Appends()
			fault.Program(storeio.JournalFaultPlan{
				Phase:       storeio.JournalFaultENOSPCAppend,
				AppendIndex: appendAt,
			})
			failedKey := []byte("definite-append-failure")
			_, appendErr := coll.Put(failedKey, journalValue(302))
			if appendErr == nil || !errors.Is(appendErr, syscall.ENOSPC) {
				t.Fatalf("later append error = %v, want ENOSPC", appendErr)
			}
			if errors.Is(appendErr, ErrCommitOutcomeUnknown) {
				t.Fatalf("definite append error misclassified as unknown: %v", appendErr)
			}
			if persistenceErr := coll.PersistenceError(); !errors.Is(persistenceErr, syscall.ENOSPC) ||
				errors.Is(persistenceErr, ErrCommitOutcomeUnknown) {
				t.Fatalf("first sticky poison = %v, want definite ENOSPC", persistenceErr)
			}

			// g.fail broadcasts, but a captured ticket must not consume that
			// fallback while its device fence is unresolved.
			select {
			case err := <-coveredDone:
				t.Fatalf("covered waiter returned before its Sync resolved: %v", err)
			default:
			}

			syncAt := fault.Syncs()
			if tc.failSync {
				fault.Program(storeio.JournalFaultPlan{
					Phase:     storeio.JournalFaultSyncError,
					SyncIndex: syncAt,
				})
			}
			release()

			var coveredErr error
			select {
			case coveredErr = <-coveredDone:
			case <-time.After(10 * time.Second):
				t.Fatal("covered waiter did not resolve after Sync")
			}
			if tc.failSync {
				if !errors.Is(coveredErr, ErrCommitOutcomeUnknown) ||
					!errors.Is(coveredErr, syscall.EIO) {
					t.Fatalf("failed covered ticket = %v, want unknown+EIO", coveredErr)
				}
				if errors.Is(coveredErr, syscall.ENOSPC) {
					t.Fatalf("failed covered ticket inherited unrelated append poison: %v",
						coveredErr)
				}
			} else if coveredErr != nil {
				t.Fatalf("successfully covered ticket returned later poison: %v", coveredErr)
			}

			// The collection-wide diagnostic remains first-failure-wins even when
			// the covered waiter carries the independent sync failure.
			persistenceErr := coll.PersistenceError()
			if !errors.Is(persistenceErr, syscall.ENOSPC) ||
				errors.Is(persistenceErr, ErrCommitOutcomeUnknown) ||
				errors.Is(persistenceErr, syscall.EIO) {
				t.Fatalf("sticky poison after fence = %v, want only definite ENOSPC",
					persistenceErr)
			}
			if _, laterErr := coll.Put([]byte("after-poison"), journalValue(303)); !errors.Is(laterErr, appendErr) {
				t.Fatalf("later operation = %v, want sticky append poison %v",
					laterErr, appendErr)
			}
			if got := fault.Syncs(); got != syncAt+1 {
				t.Fatalf("terminal poison allowed %d group Syncs, want exactly 1",
					got-syncAt)
			}

			// The covered complete record is recoverable in both device outcomes;
			// the ENOSPC'd append has no valid record and must not replay.
			image := captureJournalImage(t, path)
			_ = coll.Close()
			recovered := reopenJournalImage(t, options, image)
			requireJournalKey(t, recovered, string(coveredKey), string(coveredValue))
			if got, found, readErr := recovered.AppendRaw(nil, failedKey); readErr != nil || found {
				t.Fatalf("definite append failure replayed: got=%q found=%t err=%v",
					got, found, readErr)
			}
		})
	}
}

// TestRecoveryJournalGroupWindowCrashMatrix drives N concurrent buffered-journal
// depositors into one shared-sync window, then tears the journal at each fault
// phase and several append ordinals. Because option (b) appends every record
// under the writer and only shares the fsync, a crash mid-window leaves the same
// on-disk record stream a per-caller crash would: a legal contiguous prefix.
// The sweep asserts every reopen recovers to a subset of the intended workload
// with only correct (never torn) values and never panics, and — for the clean
// ENOSPC poison, which returns an error to the depositor — that every Put which
// returned success is still present (no acknowledged loss across the group
// fence).
func TestRecoveryJournalGroupWindowCrashMatrix(t *testing.T) {
	const writers = 6

	phases := []struct {
		name  string
		phase storeio.JournalFaultPhase
		// errs reports whether the fault returns an error to the faulting append
		// (ENOSPC) versus silently losing bytes a crash would (torn/drop).
		errs bool
	}{
		{"torn-append", storeio.JournalFaultTornAppend, false},
		{"drop-append", storeio.JournalFaultDropAppend, false},
		{"enospc-append", storeio.JournalFaultENOSPCAppend, true},
	}
	ordinals := []int{2, 5, 9}

	for _, ph := range phases {
		for _, ord := range ordinals {
			label := fmt.Sprintf("%s@%d", ph.name, ord)
			t.Run(label, func(t *testing.T) {
				options := journalTestOptions(CheckpointFilesystem)
				// A short linger widens the window so several depositors are in
				// flight when the fault ordinal is reached.
				options.CommitCoalesce = 5 * time.Millisecond
				get, restore := installJournalFaultSeam(t)
				defer restore()

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
				fj := get()
				if fj == nil {
					t.Fatalf("%s: fault seam not installed", label)
				}
				fj.Program(storeio.JournalFaultPlan{Phase: ph.phase, AppendIndex: ord})

				// The intended values, so recovery can be checked for torn bytes.
				intended := map[string]string{"seed": `{"v":0}`}
				for w := 0; w < writers; w++ {
					key := journalCrashKey(w)
					intended[key] = string(journalValue(w))
				}

				var mu sync.Mutex
				acked := map[string]string{}
				var wg sync.WaitGroup
				start := make(chan struct{})
				for w := 0; w < writers; w++ {
					wg.Add(1)
					go func(w int) {
						defer wg.Done()
						<-start
						key := journalCrashKey(w)
						if _, err := coll.Put([]byte(key), journalValue(w)); err == nil {
							mu.Lock()
							acked[key] = string(journalValue(w))
							mu.Unlock()
						}
					}(w)
				}
				close(start)
				wg.Wait()

				image := captureJournalImage(t, path)
				_ = coll.Close()
				_ = file.Close()

				// Reopen and classify. Reuse the append-matrix verifier's discipline:
				// a fresh Open, a full range read, no panic.
				rdir := t.TempDir()
				rpath := filepath.Join(rdir, "recovered.vibe")
				if err := os.WriteFile(rpath, image.store, 0o600); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(rpath+".rjournal", image.journal, 0o600); err != nil {
					t.Fatal(err)
				}
				rfile, err := os.OpenFile(rpath, os.O_RDWR, 0o600)
				if err != nil {
					t.Fatal(err)
				}
				defer rfile.Close()

				var (
					content  = map[string]string{}
					panicked bool
					openErr  error
				)
				func() {
					defer func() {
						if r := recover(); r != nil {
							panicked = true
							t.Errorf("%s: PANIC during recovery: %v", label, r)
						}
					}()
					rc, oerr := Open(rfile, options)
					if oerr != nil {
						openErr = oerr
						return
					}
					defer rc.Close()
					snap, serr := rc.Snapshot()
					if serr != nil {
						openErr = serr
						return
					}
					_ = snap.RangeRaw(func(k, v []byte) error {
						content[string(k)] = string(v)
						return nil
					})
					snap.Close()
				}()
				if panicked {
					return
				}
				if openErr != nil {
					if ph.errs {
						// The ENOSPC fault fails the append cleanly without writing a
						// byte, so the on-disk record stream is a valid contiguous
						// prefix and reopen must succeed. Skipping the acked-subset
						// check below on a failed Open would make this phase's only
						// meaningful assertion vacuous.
						t.Fatalf("%s: reopen failed on a clean-prefix image (%d acked): %v",
							label, len(acked), openErr)
					}
					// Failing closed is a legal outcome for a torn or dropped tail.
					t.Logf("%s: fail-closed: %v", label, openErr)
					return
				}
				// Every recovered value must be a correct intended value (never torn)
				// and belong to the workload.
				for k, v := range content {
					want, ok := intended[k]
					if !ok {
						t.Fatalf("%s: recovered unexpected key %q=%q", label, k, v)
					}
					if v != want {
						t.Fatalf("%s: recovered torn value for %q: %q (want %q)",
							label, k, v, want)
					}
				}
				// Seed must always survive (it predates the journal).
				if content["seed"] != `{"v":0}` {
					t.Fatalf("%s: seed lost or torn: %q", label, content["seed"])
				}
				if ph.errs {
					// ENOSPC returns an error to the faulting depositor, so every Put
					// that returned success followed a completed covering sync and must
					// be durable: no acknowledged loss across the shared fence.
					mu.Lock()
					for k, v := range acked {
						if content[k] != v {
							mu.Unlock()
							t.Fatalf("%s: acknowledged key %q lost across group fence (got %q want %q)",
								label, k, content[k], v)
						}
					}
					mu.Unlock()
				}
				t.Logf("%s: recovered %d/%d keys, %d acked",
					label, len(content), len(intended), len(acked))
			})
		}
	}
}

// TestRecoveryJournalGroupConcurrentRaceSmoke hammers the acknowledgement path
// with concurrent writers on distinct and colliding keys under -race, then
// verifies the folded root replays every value. It runs both the buffered-journal
// lane — whose shared fence (deposit under the writer, sync outside it,
// checkpoint recycles racing the group) is the phase-1 change — and the
// DurabilitySync lane, which is unchanged by phase 1 (per-caller fence before
// publish) and must stay race-clean under concurrent callers.
func TestRecoveryJournalGroupConcurrentRaceSmoke(t *testing.T) {
	for _, lane := range []struct {
		name    string
		options Options
	}{
		{"buffered-journal", journalTestOptions(CheckpointFilesystem)},
		{"sync", syncPrimaryJournalTestOptions()},
	} {
		t.Run(lane.name, func(t *testing.T) {
			coll, path := openJournalGroupCollection(t, lane.options)

			const writers = 8
			const putsPer = 32
			var wg sync.WaitGroup
			start := make(chan struct{})
			for w := 0; w < writers; w++ {
				wg.Add(1)
				go func(w int) {
					defer wg.Done()
					<-start
					for p := 0; p < putsPer; p++ {
						// Distinct keys per writer, plus periodic collisions on a shared
						// key so same-key serialization through the writer is exercised.
						key := fmt.Sprintf("w%02d-k%03d", w, p)
						if _, err := coll.Put([]byte(key), []byte(fmt.Sprintf(`{"p":%d,"w":%d}`, p, w))); err != nil {
							t.Errorf("put %s: %v", key, err)
							return
						}
						if p%8 == 0 {
							shared := []byte(fmt.Sprintf(`{"last":%d}`, w))
							if _, err := coll.Put([]byte("shared"), shared); err != nil {
								t.Errorf("put shared: %v", err)
								return
							}
						}
					}
				}(w)
			}
			close(start)
			wg.Wait()

			// Every distinct-key write is present with its exact value.
			for w := 0; w < writers; w++ {
				for p := 0; p < putsPer; p++ {
					key := fmt.Sprintf("w%02d-k%03d", w, p)
					got, ok, err := coll.AppendRaw(nil, []byte(key))
					want := fmt.Sprintf(`{"p":%d,"w":%d}`, p, w)
					if err != nil || !ok || string(got) != want {
						t.Fatalf("live key %s = (%q,%v,%v), want %q", key, got, ok, err, want)
					}
				}
			}
			if err := coll.Flush(); err != nil {
				t.Fatalf("flush: %v", err)
			}
			if err := coll.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}
			reopened, err := os.OpenFile(path, os.O_RDWR, 0o600)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			rc, err := Open(reopened, lane.options)
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			defer rc.Close()
			for w := 0; w < writers; w++ {
				for p := 0; p < putsPer; p++ {
					key := fmt.Sprintf("w%02d-k%03d", w, p)
					got, ok, err := rc.AppendRaw(nil, []byte(key))
					want := fmt.Sprintf(`{"p":%d,"w":%d}`, p, w)
					if err != nil || !ok || string(got) != want {
						t.Fatalf("reopened key %s = (%q,%v,%v), want %q", key, got, ok, err, want)
					}
				}
			}
		})
	}
}
