package durable

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
)

// journalTestOptions is a buffered-visible primary configuration with the
// recovery journal enabled. Small pages and one generation per commit keep the
// crash sweeps fast and deterministic.
func journalTestOptions(strength CheckpointStrength) Options {
	return Options{
		Collection:         store.Options{ChunkDocuments: 1},
		Backend:            BackendPortable,
		ResidentBytes:      32 << 20,
		Durability:         DurabilityBufferedVisible,
		CheckpointStrength: strength,
		RecoveryJournal:    true,
		PageSize:           4096,
		MaxPageSize:        64 << 10,
		InlineValueBytes:   2048,
		MaxDocumentBytes:   2048,
		GroupLimit:         1,
		CommitCoalesce:     0,
	}
}

// syncPrimaryJournalTestOptions is a DurabilitySync primary configuration. The
// journal is unconditional on this lane (RecoveryJournal is neither required nor
// consulted), and sync always uses the power-safe barrier, so unlike the
// buffered helper there is no filesystem-strength variant.
func syncPrimaryJournalTestOptions() Options {
	o := journalTestOptions(CheckpointPowerSafe)
	o.Durability = DurabilitySync
	o.RecoveryJournal = false
	return o
}

// journalOverflowOptions is journalTestOptions with MaxDocumentBytes raised above
// InlineValueBytes, so a value between the two stores an out-of-line overflow
// chain rather than fitting inline. The journal and its replay must then carry
// the acknowledgement by reference to that chain, which is the seam the inline
// helper never exercises.
func journalOverflowOptions(strength CheckpointStrength) Options {
	o := journalTestOptions(strength)
	o.InlineValueBytes = 256
	o.MaxDocumentBytes = 8 << 10
	return o
}

// syncPrimaryOverflowJournalTestOptions is the DurabilitySync counterpart of
// journalOverflowOptions, matching syncPrimaryJournalTestOptions' lane choices.
func syncPrimaryOverflowJournalTestOptions() Options {
	o := journalOverflowOptions(CheckpointPowerSafe)
	o.Durability = DurabilitySync
	o.RecoveryJournal = false
	return o
}

// journalOverflowValue is journalValue past InlineValueBytes=256, so every write
// spills out of line. The pad is distinct per index so a value replayed from its
// overflow chain is checked byte for byte rather than merely for presence.
func journalOverflowValue(i int) []byte {
	return []byte(fmt.Sprintf(`{"i":%d,"pad":%q}`, i, strings.Repeat("p", 1024)))
}

// seedPrimaryCollection builds a one-document ordered primary graph so a plain
// CreateFromPrimary produces a mutable primary store.
func seedPrimaryCollection(t testing.TB) *store.Collection {
	t.Helper()
	builder, err := store.NewBuilder(store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := builder.Append("seed", []byte(`{"v":0}`)); err != nil {
		t.Fatal(err)
	}
	built, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	return built
}

func journalValue(i int) []byte {
	return []byte(fmt.Sprintf(`{"i":%d,"v":"payload"}`, i))
}

func copyFileForCrash(t *testing.T, src, dst string) {
	t.Helper()
	data, err := os.ReadFile(src)
	if err != nil {
		t.Fatalf("read %s: %v", src, err)
	}
	if err := os.WriteFile(dst, data, 0o600); err != nil {
		t.Fatalf("write %s: %v", dst, err)
	}
}

// TestRecoveryJournalRoundTrip proves a journaled buffered store mints a sibling
// journal, acknowledges frame-deferred mutations through it, and reopens with
// every acknowledgement intact after a clean checkpointing Close.
func TestRecoveryJournalRoundTrip(t *testing.T) {
	options := journalTestOptions(CheckpointPowerSafe)
	built := seedPrimaryCollection(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "store.vibe")

	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := CreateFromPrimary(built, file, options); err != nil {
		t.Fatalf("CreateFromPrimary: %v", err)
	}
	_ = file.Close()

	if _, err := os.Stat(path + ".rjournal"); err != nil {
		t.Fatalf("sibling journal not created: %v", err)
	}

	file, err = os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	coll, err := Open(file, options)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	const n = 200
	for i := 0; i < n; i++ {
		if _, err := coll.Put([]byte(fmt.Sprintf("key-%04d", i)), journalValue(i)); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	stats := coll.Stats()
	if stats.JournalAcks == 0 {
		t.Fatalf("expected journal acknowledgements, got %+v acks (journal=%d chain=%d)",
			stats.JournalAcks, stats.JournalAcks, stats.ChainAcks)
	}
	t.Logf("round-trip acks: journal=%d chain=%d", stats.JournalAcks, stats.ChainAcks)
	if err := coll.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	_ = file.Close()

	// A clean close checkpointed and recycled the journal; reopen must find every
	// key and replay nothing.
	file, err = os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	coll, err = Open(file, options)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer coll.Close()
	got := snapshotCollectionContent(t, coll)
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%04d", i)
		if got[key] != string(journalValue(i)) {
			t.Fatalf("key %s: got %q want %q", key, got[key], journalValue(i))
		}
	}
}

// TestSyncPrimaryJournalRoundTrip proves the unified durability mechanism: a
// DurabilitySync primary store mints a sibling journal unconditionally,
// acknowledges every mutation through a journal append+sync BEFORE publishing it
// (so JournalAcks accrues and no chain fence runs), makes each acknowledged value
// immediately visible, and reopens with every acknowledgement intact after a
// clean checkpointing Close.
func TestSyncPrimaryJournalRoundTrip(t *testing.T) {
	options := syncPrimaryJournalTestOptions()
	built := seedPrimaryCollection(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "store.vibe")

	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := CreateFromPrimary(built, file, options); err != nil {
		t.Fatalf("CreateFromPrimary: %v", err)
	}
	_ = file.Close()

	if _, err := os.Stat(path + ".rjournal"); err != nil {
		t.Fatalf("sync primary store minted no sibling journal: %v", err)
	}

	file, err = os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	coll, err := Open(file, options)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	const n = 40
	want := map[string]string{"seed": `{"v":0}`}
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%04d", i)
		val := journalValue(i)
		if _, err := coll.Put([]byte(key), val); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
		want[key] = string(val)
		// Visibility follows durability: the value is readable on the live reader
		// immediately after the acknowledging Put returns (a snapshot pins the
		// durable checkpoint root and would not yet see the un-checkpointed ack).
		if got, found, err := coll.AppendRaw(nil, []byte(key)); err != nil ||
			!found || string(got) != string(val) {
			t.Fatalf("key %s not visible after sync ack: got %q found=%v err=%v want %q",
				key, got, found, err, val)
		}
	}
	stats := coll.Stats()
	if stats.JournalAcks == 0 {
		t.Fatalf("sync primary Put took no journal acknowledgements (journal=%d chain=%d)",
			stats.JournalAcks, stats.ChainAcks)
	}
	if stats.ChainAcks != 0 {
		t.Fatalf("sync primary lane must not take the retired chain fence, got chain=%d",
			stats.ChainAcks)
	}
	t.Logf("sync round-trip acks: journal=%d chain=%d", stats.JournalAcks, stats.ChainAcks)
	if err := coll.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	_ = file.Close()

	file, err = os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	coll, err = Open(file, options)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer coll.Close()
	if got := snapshotCollectionContent(t, coll); !mapsEqual(got, want) {
		t.Fatalf("reopen after clean close mismatched: got %d keys want %d", len(got), len(want))
	}
}

// TestSyncPrimaryJournaledRecordReplayedAfterCrashWindow exercises the exact
// window the reversed ordering opens: the redo record is appended and synced
// durable, but the mutation is NOT yet applied to memory or published. The
// post-sync fault seam captures the on-disk store and journal at precisely that
// instant — the record is durable, the store root still predates it — and a
// reopen of that image must replay the record. This proves a crash between the
// journal sync and the in-memory publish loses nothing.
func TestSyncPrimaryJournaledRecordReplayedAfterCrashWindow(t *testing.T) {
	options := syncPrimaryJournalTestOptions()
	built := seedPrimaryCollection(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "store.vibe")

	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := CreateFromPrimary(built, file, options); err != nil {
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

	want := map[string]string{"seed": `{"v":0}`}
	const baseline = 8
	for i := 0; i < baseline; i++ {
		key := fmt.Sprintf("key-%04d", i)
		val := journalValue(i)
		if _, err := coll.Put([]byte(key), val); err != nil {
			t.Fatalf("baseline put %d: %v", i, err)
		}
		want[key] = string(val)
	}

	// Arm the seam to snapshot the files exactly once, at the sync-but-unpublished
	// instant of the next Put. At that point its record is durable on disk while
	// the store root still predates it and no in-memory publish has happened.
	crashStore := filepath.Join(dir, "crash.vibe")
	crashKey := "crash-key"
	crashVal := journalValue(999)
	var captured bool
	recoveryJournalPostSyncHook = func() {
		if captured {
			return
		}
		captured = true
		copyFileForCrash(t, path, crashStore)
		copyFileForCrash(t, path+".rjournal", crashStore+".rjournal")
	}
	defer func() { recoveryJournalPostSyncHook = nil }()

	if _, err := coll.Put([]byte(crashKey), crashVal); err != nil {
		t.Fatalf("crash-window put: %v", err)
	}
	if !captured {
		t.Fatal("post-sync seam never fired: the sync lane did not journal before publishing")
	}
	want[crashKey] = string(crashVal)
	// The live writer published normally after the seam; the value is visible on
	// the live reader.
	if got, found, err := coll.AppendRaw(nil, []byte(crashKey)); err != nil ||
		!found || string(got) != string(crashVal) {
		t.Fatalf("crash key not visible on live store after publish: got %q found=%v err=%v",
			got, found, err)
	}
	if err := coll.Close(); err != nil {
		t.Fatalf("close live: %v", err)
	}
	_ = file.Close()

	// Reopen the crash image captured mid-Put. Replay must reconstruct every
	// baseline acknowledgement AND the record whose in-memory publish never ran.
	verify := func(label string) {
		cf, err := os.OpenFile(crashStore, os.O_RDWR, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		rc, err := Open(cf, options)
		if err != nil {
			t.Fatalf("%s: reopen crash image: %v", label, err)
		}
		got := snapshotCollectionContent(t, rc)
		if got[crashKey] != string(crashVal) {
			t.Fatalf("%s: synced-but-unpublished record was not replayed: got %q want %q",
				label, got[crashKey], crashVal)
		}
		if !mapsEqual(got, want) {
			t.Fatalf("%s: recovered %d keys, want %d", label, len(got), len(want))
		}
		if err := rc.Close(); err != nil {
			t.Fatalf("%s: close: %v", label, err)
		}
		_ = cf.Close()
	}
	verify("first-open-replay")
	verify("second-open-idempotent")
}

// TestSyncPrimaryJournaledOverflowReplayIsDeterministic is the overflow
// counterpart of TestSyncPrimaryJournaledRecordReplayedAfterCrashWindow. The
// crash value is stored out of line, so the redo record captured at the
// sync-but-unpublished instant names an overflow chain rather than carrying an
// inline value. Replay must reconstruct the referenced value exactly, and a
// second reopen — now checkpointed, the chain folded into the durable root —
// must recover the identical content: an overflow chain rebuilt on replay is
// deterministic, not a fresh allocation whose bytes could drift.
func TestSyncPrimaryJournaledOverflowReplayIsDeterministic(t *testing.T) {
	options := syncPrimaryOverflowJournalTestOptions()
	built := seedPrimaryCollection(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "store.vibe")

	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := CreateFromPrimary(built, file, options); err != nil {
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

	want := map[string]string{"seed": `{"v":0}`}
	const baseline = 6
	for i := 0; i < baseline; i++ {
		key := fmt.Sprintf("key-%04d", i)
		val := journalOverflowValue(i)
		if _, err := coll.Put([]byte(key), val); err != nil {
			t.Fatalf("baseline overflow put %d: %v", i, err)
		}
		want[key] = string(val)
	}

	// Snapshot the store and journal exactly once, at the sync-but-unpublished
	// instant of the next out-of-line Put: its record and overflow chain are
	// durable on disk while the store root still predates them.
	crashStore := filepath.Join(dir, "crash.vibe")
	crashKey := "crash-key"
	crashVal := journalOverflowValue(999)
	var captured bool
	recoveryJournalPostSyncHook = func() {
		if captured {
			return
		}
		captured = true
		copyFileForCrash(t, path, crashStore)
		copyFileForCrash(t, path+".rjournal", crashStore+".rjournal")
	}
	defer func() { recoveryJournalPostSyncHook = nil }()

	if _, err := coll.Put([]byte(crashKey), crashVal); err != nil {
		t.Fatalf("crash-window overflow put: %v", err)
	}
	if !captured {
		t.Fatal("post-sync seam never fired: the sync lane did not journal before publishing")
	}
	want[crashKey] = string(crashVal)
	if got, found, err := coll.AppendRaw(nil, []byte(crashKey)); err != nil ||
		!found || string(got) != string(crashVal) {
		t.Fatalf("overflow crash key not visible on live store after publish: found=%v err=%v", found, err)
	}
	if err := coll.Close(); err != nil {
		t.Fatalf("close live: %v", err)
	}
	_ = file.Close()

	// Reopen the crash image mid-Put. Both opens must reconstruct every baseline
	// overflow value AND the crash record whose in-memory publish never ran, and
	// the two must be byte-for-byte identical.
	verify := func(label string) {
		cf, err := os.OpenFile(crashStore, os.O_RDWR, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		rc, err := Open(cf, options)
		if err != nil {
			t.Fatalf("%s: reopen crash image: %v", label, err)
		}
		got := snapshotCollectionContent(t, rc)
		if got[crashKey] != string(crashVal) {
			t.Fatalf("%s: synced-but-unpublished overflow record was not replayed: got %q want overflow value",
				label, got[crashKey])
		}
		if !mapsEqual(got, want) {
			t.Fatalf("%s: recovered %d keys, want %d", label, len(got), len(want))
		}
		if err := rc.Close(); err != nil {
			t.Fatalf("%s: close: %v", label, err)
		}
		_ = cf.Close()
	}
	verify("first-open-replay")
	verify("second-open-idempotent")
}

// TestRecoveryJournalReplayReconstructsAcks copies the store and journal at a
// point where acknowledgements have been journaled but no checkpoint has folded
// them into the root, then reopens the copy. Replay must reconstruct every
// journaled acknowledgement, and a second reopen (now checkpointed) must be
// identical: replay is deterministic.
func TestRecoveryJournalReplayReconstructsAcks(t *testing.T) {
	for _, strength := range []struct {
		name     string
		strength CheckpointStrength
	}{
		{"PowerSafe", CheckpointPowerSafe},
		{"Filesystem", CheckpointFilesystem},
	} {
		t.Run(strength.name, func(t *testing.T) {
			options := journalTestOptions(strength.strength)
			built := seedPrimaryCollection(t)
			dir := t.TempDir()
			path := filepath.Join(dir, "store.vibe")

			file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := CreateFromPrimary(built, file, options); err != nil {
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
			const n = 50
			want := map[string]string{"seed": `{"v":0}`}
			for i := 0; i < n; i++ {
				key := fmt.Sprintf("key-%04d", i)
				val := journalValue(i)
				if _, err := coll.Put([]byte(key), val); err != nil {
					t.Fatalf("put %d: %v", i, err)
				}
				want[key] = string(val)
			}
			acks := coll.Stats().JournalAcks
			if acks == 0 {
				t.Fatal("no journal acknowledgements before crash copy")
			}

			// Simulate a crash: copy the store and its journal with no clean close,
			// so the root still predates the journaled acknowledgements.
			crashStore := filepath.Join(dir, "crash.vibe")
			copyFileForCrash(t, path, crashStore)
			copyFileForCrash(t, path+".rjournal", crashStore+".rjournal")

			// The writer is still live; close it after capturing the image.
			if err := coll.Close(); err != nil {
				t.Fatalf("close live: %v", err)
			}
			_ = file.Close()

			// Reopen the crash image: replay must reconstruct every ack.
			verifyReplay := func(label string) {
				cf, err := os.OpenFile(crashStore, os.O_RDWR, 0o600)
				if err != nil {
					t.Fatal(err)
				}
				rc, err := Open(cf, options)
				if err != nil {
					t.Fatalf("%s: reopen crash image: %v", label, err)
				}
				got := snapshotCollectionContent(t, rc)
				if !mapsEqual(got, want) {
					t.Fatalf("%s: recovered %d keys, want %d (mismatch)", label, len(got), len(want))
				}
				if err := rc.Close(); err != nil {
					t.Fatalf("%s: close: %v", label, err)
				}
				_ = cf.Close()
			}
			verifyReplay("first-open-replay")
			// The first reopen checkpointed and recycled; the second must recover the
			// identical state with nothing left to replay.
			verifyReplay("second-open-idempotent")
		})
	}
}

// TestRecoveryJournalFullForcesCheckpoint drives more same-size in-place updates
// than the preallocated journal holds, so the journal fills and forces a
// checkpoint exactly like staging pressure. The forced checkpoint folds the
// acknowledgements into a durable root and recycles the journal, counting a
// chain acknowledgement; every value must still survive a reopen.
func TestRecoveryJournalFullForcesCheckpoint(t *testing.T) {
	if testing.Short() {
		t.Skip("journal-full needs thousands of individually synced acknowledgements")
	}
	// Filesystem strength keeps the thousands of per-ack syncs off the F_FULLFSYNC
	// drive-drain path; the full-forced checkpoint it exercises is strength-blind.
	options := journalTestOptions(CheckpointFilesystem)
	built := seedPrimaryCollection(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "store.vibe")

	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := CreateFromPrimary(built, file, options); err != nil {
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
	// A handful of keys, repeatedly overwritten with a fixed-width value so each
	// update is a same-size in-place patch that appends exactly one record. More
	// than recoveryJournalCheckpointRecords updates guarantees at least one full.
	const keys = 4
	const rounds = recoveryJournalCheckpointRecords + 600
	pad := make([]byte, 1500)
	for i := range pad {
		pad[i] = 'a'
	}
	want := map[string]string{"seed": `{"v":0}`}
	for r := 0; r < rounds; r++ {
		k := fmt.Sprintf("key-%d", r%keys)
		v := fmt.Sprintf(`{"r":"%08d","pad":"%s"}`, r, pad)
		if _, err := coll.Put([]byte(k), []byte(v)); err != nil {
			t.Fatalf("put round %d: %v", r, err)
		}
		want[k] = v
	}
	stats := coll.Stats()
	if stats.ChainAcks == 0 {
		t.Fatalf("expected a full-forced checkpoint (chain ack), got journal=%d chain=%d",
			stats.JournalAcks, stats.ChainAcks)
	}
	t.Logf("full-forced: journal=%d chain=%d auto-checkpoints=%d",
		stats.JournalAcks, stats.ChainAcks, stats.AutomaticCheckpoints)
	if err := coll.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	_ = file.Close()

	file, err = os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	coll, err = Open(file, options)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer coll.Close()
	got := snapshotCollectionContent(t, coll)
	if !mapsEqual(got, want) {
		t.Fatalf("reopen after full-forced checkpoint mismatched: got %d keys want %d",
			len(got), len(want))
	}
}

// TestRecoveryJournalRequiresPrimaryLayout proves the primary journal lane is
// live: a CreateFromPrimary store opened with Options.RecoveryJournal actually
// acknowledges a Put through the journal. The former chunk-image cases (Create
// and Open of a chunk-layout store failing closed with
// ErrRecoveryJournalRequiresPrimary) were deleted with the chunk layout, which
// can no longer be constructed. The production guard that fails an Open closed
// when a root names a journal but carries no primary graph (store_file.go,
// root.PrimaryRoot == zero && journal named) remains and is exercised by the
// recovery/fuzz sweeps.
func TestRecoveryJournalRequiresPrimaryLayout(t *testing.T) {
	options := journalTestOptions(CheckpointFilesystem)
	dir := t.TempDir()

	// The primary path succeeds and journals: a Put must record at least one
	// journal acknowledgement.
	built := seedPrimaryCollection(t)
	primaryPath := filepath.Join(dir, "primary.vibe")
	pf, err := os.OpenFile(primaryPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := CreateFromPrimary(built, pf, options); err != nil {
		t.Fatalf("CreateFromPrimary with RecoveryJournal: %v", err)
	}
	_ = pf.Close()
	pf, err = os.OpenFile(primaryPath, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer pf.Close()
	primary, err := Open(pf, options)
	if err != nil {
		t.Fatalf("Open primary journaled store: %v", err)
	}
	defer primary.Close()
	if _, err := primary.Put([]byte("key-0001"), journalValue(1)); err != nil {
		t.Fatalf("primary put: %v", err)
	}
	if acks := primary.Stats().JournalAcks; acks == 0 {
		t.Fatalf("primary Put recorded no journal acknowledgement (journal=%d chain=%d)",
			acks, primary.Stats().ChainAcks)
	}
}

// The former TestChunkSyncMintsNoJournal was deleted with the chunk layout: it
// asserted that a DurabilitySync chunk-layout store minted no sibling journal.
// Every store is now an ordered primary graph, and the synchronous lane journals
// by design (the sync-journal lane), so the biconditional it guarded no longer
// exists.

// TestRecoveryJournalMissingFailsClosed proves a root that references a journal
// whose file is absent fails closed rather than silently dropping acknowledged
// mutations.
func TestRecoveryJournalMissingFailsClosed(t *testing.T) {
	options := journalTestOptions(CheckpointPowerSafe)
	built := seedPrimaryCollection(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "store.vibe")

	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := CreateFromPrimary(built, file, options); err != nil {
		t.Fatalf("CreateFromPrimary: %v", err)
	}
	_ = file.Close()

	if err := os.Remove(path + ".rjournal"); err != nil {
		t.Fatalf("remove journal: %v", err)
	}
	file, err = os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if _, err := Open(file, options); err == nil {
		t.Fatal("Open succeeded despite a referenced-but-missing journal")
	} else if !errors.Is(err, storeio.ErrRecoveryJournalMissing) {
		t.Fatalf("expected ErrRecoveryJournalMissing, got %v", err)
	}
}
