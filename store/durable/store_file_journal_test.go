package durable

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
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
		if _, err := coll.Put(fmt.Sprintf("key-%04d", i), journalValue(i)); err != nil {
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
				if _, err := coll.Put(key, val); err != nil {
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
		if _, err := coll.Put(k, []byte(v)); err != nil {
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
