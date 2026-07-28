package durable

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
)

// Store-level recovery-journal crash matrix.
//
// The journal is a separate file with its own write seam (FaultJournal), so its
// crash-relevant boundaries are the record append and the recycle header write,
// each followed by a sync. This sweep drives a real buffered-visible workload
// through a journal whose raw writes are torn, dropped, or ENOSPC'd at an exact
// point, snapshots the store and journal images a crash there would leave, and
// asserts every reopen recovers to a legal prefix state or fails closed — never
// a torn third value, never a panic. It is the store-level cover for the
// append/sync ordering and the checkpoint recycle.

// installJournalFaultSeam points recoveryJournalFaultHook at a fresh FaultJournal
// for the next journal this collection opens and returns a getter plus a restore
// func. The seam defaults to JournalFaultNone so Open's own replay and recycle
// run clean; the caller programs the real plan after Open returns.
func installJournalFaultSeam(t *testing.T) (get func() *storeio.FaultJournal, restore func()) {
	t.Helper()
	prev := recoveryJournalFaultHook
	var fj *storeio.FaultJournal
	recoveryJournalFaultHook = func(rj *storeio.RecoveryJournal) {
		fj = storeio.NewFaultJournal(rj)
	}
	return func() *storeio.FaultJournal { return fj },
		func() { recoveryJournalFaultHook = prev }
}

// journalCrashImage is a store image plus its sibling journal captured at a crash.
type journalCrashImage struct {
	store   []byte
	journal []byte
}

// captureJournalImage reads the store file and its sibling journal.
func captureJournalImage(t *testing.T, storePath string) journalCrashImage {
	t.Helper()
	storeBytes, err := os.ReadFile(storePath)
	if err != nil {
		t.Fatalf("read store: %v", err)
	}
	journalBytes, err := os.ReadFile(storePath + ".rjournal")
	if err != nil {
		t.Fatalf("read journal: %v", err)
	}
	return journalCrashImage{store: storeBytes, journal: journalBytes}
}

// verifyJournalCrashImage reopens a captured store+journal pair and classifies
// the outcome against the legal prefix states, exactly the journal analogue of
// the device sweep's verifyCrashImage.
func verifyJournalCrashImage(
	t *testing.T, options Options, image journalCrashImage,
	legal []map[string]string, label string,
) string {
	t.Helper()
	dir := t.TempDir()
	storePath := filepath.Join(dir, "recovered.vibe")
	if err := os.WriteFile(storePath, image.store, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(storePath+".rjournal", image.journal, 0o600); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(storePath, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	var (
		coll     *Collection
		openErr  error
		content  = map[string]string{}
		readErr  error
		panicked bool
	)
	func() {
		defer func() {
			if r := recover(); r != nil {
				panicked = true
				t.Errorf("%s: PANIC during recovery: %v", label, r)
			}
		}()
		coll, openErr = Open(file, options)
		if openErr != nil {
			return
		}
		snap, snapErr := coll.Snapshot()
		if snapErr != nil {
			readErr = snapErr
			return
		}
		readErr = snap.RangeRaw(func(key, value []byte) error {
			content[string(key)] = string(value)
			return nil
		})
		snap.Close()
	}()
	if coll != nil {
		_ = coll.Close()
	}
	switch {
	case panicked:
		return "panic"
	case openErr != nil:
		return "fail-closed"
	case readErr != nil:
		return "read-error"
	}
	for _, want := range legal {
		if mapsEqual(content, want) {
			return "recovered"
		}
	}
	t.Errorf("%s: reopen recovered content %v is not a legal prefix state %v",
		label, content, legal)
	return "hole"
}

func journalCrashKey(i int) string { return fmt.Sprintf("key-%04d", i) }

// journalContentOracle computes the content after each put directly from the
// deterministic workload: contents[i] is the seed plus puts 0..i-1. The oracle
// is built from the known workload rather than by snapshotting the live buffered
// collection, whose volatile pending mutations a mid-workload Snapshot is not
// obligated to materialize.
func journalContentOracle(n int) []map[string]string {
	contents := make([]map[string]string, 0, n+1)
	for i := 0; i <= n; i++ {
		m := map[string]string{"seed": `{"v":0}`}
		for j := 0; j < i; j++ {
			m[journalCrashKey(j)] = string(journalValue(j))
		}
		contents = append(contents, m)
	}
	return contents
}

// TestRecoveryJournalAppendCrashMatrix crashes the journal append at each of the
// three fault phases and several append ordinals, then reopens each captured
// store+journal image. A torn, dropped, or ENOSPC'd append is the truncation
// point of the tail: recovery must recover to the content just before or just
// after that acknowledgement, and never a torn third value.
func TestRecoveryJournalAppendCrashMatrix(t *testing.T) {
	options := journalTestOptions(CheckpointPowerSafe)
	const n = 8
	contents := journalContentOracle(n)

	phases := []struct {
		name  string
		phase storeio.JournalFaultPhase
	}{
		{"torn-append", storeio.JournalFaultTornAppend},
		{"drop-append", storeio.JournalFaultDropAppend},
		{"enospc-append", storeio.JournalFaultENOSPCAppend},
	}
	indices := []int{0, 1, 3, 6}

	tally := map[string]int{}
	exercised := 0
	for _, ph := range phases {
		for _, idx := range indices {
			label := fmt.Sprintf("%s@%d", ph.name, idx)
			get, restore := installJournalFaultSeam(t)

			dir := t.TempDir()
			path := filepath.Join(dir, "store.vibe")
			file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := CreateFromPrimary(seedPrimaryCollection(t), file, options); err != nil {
				t.Fatalf("%s: CreateFromPrimary: %v", label, err)
			}
			_ = file.Close()
			file, err = os.OpenFile(path, os.O_RDWR, 0o600)
			if err != nil {
				t.Fatal(err)
			}
			coll, err := Open(file, options)
			if err != nil {
				t.Fatalf("%s: open: %v", label, err)
			}
			fj := get()
			if fj == nil {
				t.Fatalf("%s: fault seam not installed", label)
			}
			fj.Program(storeio.JournalFaultPlan{Phase: ph.phase, AppendIndex: idx})

			landed := 0
			for i := 0; i < n; i++ {
				_, putErr := coll.Put(journalCrashKey(i), journalValue(i))
				if fj.Faulted() {
					if putErr == nil {
						landed = i + 1
					} else {
						landed = i
					}
					break
				}
				if putErr != nil {
					t.Fatalf("%s: put %d unexpected error before fault: %v", label, i, putErr)
				}
				landed = i + 1
			}
			image := captureJournalImage(t, path)
			_ = coll.Close()
			_ = file.Close()
			restore()

			if !fj.Faulted() {
				// The append ordinal was never reached (too few puts). Skip.
				continue
			}
			// Legal: the acknowledgement at the faulted ordinal may be present or
			// absent; every earlier one is durable.
			legal := []map[string]string{contents[idx]}
			if idx+1 <= n {
				legal = append(legal, contents[idx+1])
			}
			_ = landed
			outcome := verifyJournalCrashImage(t, options, image, legal, label)
			tally[outcome]++
			exercised++
		}
	}
	if exercised == 0 {
		t.Fatal("no append crash points exercised")
	}
	t.Logf("journal append crash matrix: %d points; outcomes %v", exercised, tally)
}

// TestRecoveryJournalRecycleCrashMatrix crashes the checkpoint's journal recycle
// header write. The store root was already fenced durable before the recycle, so
// a torn or ENOSPC'd recycle must leave a reopen that recovers to the fully
// checkpointed state through the surviving previous header — never a lost
// acknowledgement, never a panic.
func TestRecoveryJournalRecycleCrashMatrix(t *testing.T) {
	options := journalTestOptions(CheckpointPowerSafe)
	const putsBeforeFlush = 5
	contents := journalContentOracle(putsBeforeFlush)
	checkpointed := contents[putsBeforeFlush]

	phases := []struct {
		name  string
		phase storeio.JournalFaultPhase
	}{
		{"torn-recycle", storeio.JournalFaultTornRecycle},
		{"enospc-recycle", storeio.JournalFaultENOSPCRecycle},
	}

	tally := map[string]int{}
	for _, ph := range phases {
		label := ph.name
		get, restore := installJournalFaultSeam(t)

		dir := t.TempDir()
		path := filepath.Join(dir, "store.vibe")
		file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := CreateFromPrimary(seedPrimaryCollection(t), file, options); err != nil {
			t.Fatalf("%s: CreateFromPrimary: %v", label, err)
		}
		_ = file.Close()
		file, err = os.OpenFile(path, os.O_RDWR, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		coll, err := Open(file, options)
		if err != nil {
			t.Fatalf("%s: open: %v", label, err)
		}
		for i := 0; i < putsBeforeFlush; i++ {
			if _, err := coll.Put(journalCrashKey(i), journalValue(i)); err != nil {
				t.Fatalf("%s: put %d: %v", label, i, err)
			}
		}
		fj := get()
		fj.Program(storeio.JournalFaultPlan{Phase: ph.phase})
		// Flush checkpoints (root durable) then recycles the journal, where the
		// fault fires.
		flushErr := coll.Flush()
		if !fj.Faulted() {
			t.Fatalf("%s: recycle fault never fired (flushErr=%v)", label, flushErr)
		}
		image := captureJournalImage(t, path)
		_ = coll.Close()
		_ = file.Close()
		restore()

		outcome := verifyJournalCrashImage(
			t, options, image, []map[string]string{checkpointed}, label,
		)
		tally[outcome]++
	}
	t.Logf("journal recycle crash matrix: outcomes %v", tally)
}
